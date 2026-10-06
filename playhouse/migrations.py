"""
Migration runner built on the playhouse.migrate helpers.

Migrations are plain python scripts in a directory, applied in numeric
order and recorded by name in a history table. Each script defines
``up(migrator, db)`` and (optionally) ``down(migrator, db)``:

    # migrations/0002_add_karma.py
    from peewee import *

    def up(migrator, db):
        migrator.migrate(
            migrator.add_column('user', 'karma', IntegerField(default=0)))

    def down(migrator, db):
        migrator.migrate(migrator.drop_column('user', 'karma'))

Nothing is introspected at runtime or auto-detected. New tables need no special
support: declare the model inline (a frozen copy, deliberately independent of
the application models) and call ``db.create_tables()`` from ``up()``.

Set ``atomic = False`` at module scope to disable transaction wrapping.

Cross-process locking and resumable runs are opt-in via ``lock=True``.
When enabled, each migration is claimed by exactly one process at a time
(PostgreSQL advisory locks, MySQL named locks, or a committed claim row
elsewhere); a "running" marker is written before the migration body runs
and the history row is settled in the same transaction as the migration
on success. A process killed mid-migration leaves the marker behind, so
the next run refuses to re-execute that migration unless ``force=True``
is passed explicitly. ``Runner.plan()`` (and the ``plan`` CLI command)
report what would run, what would be skipped and what is in progress
without writing anything.
"""
import argparse
import contextlib
import datetime
import decimal
import enum
import glob
import hashlib
import importlib.util
import logging
import os
import re
import socket
import sys
import time
import traceback
import uuid
from collections import namedtuple

from peewee import *
from peewee import is_model
from peewee import sort_models
from playhouse.fields import EnumFieldMixin
from playhouse.migrate import SchemaMigrator
from playhouse.migrate import make_index_name

try:
    from playhouse.cockroachdb import CockroachDatabase
except ImportError:
    CockroachDatabase = None

__all__ = ['Migration', 'MigrationError', 'MigrationLocked', 'PlanItem',
           'Runner', 'run', 'template']

logger = logging.getLogger('peewee.migrations')

# History row states (the columns only exist when locking is enabled).
APPLIED = 'applied'
RUNNING = 'running'
REVERTING = 'reverting'

# Plan actions.
PLAN_APPLY = 'apply'
PLAN_SKIP = 'skip'
PLAN_REVERT = 'revert'
PLAN_IN_PROGRESS = 'running'
PLAN_MISSING = 'missing'

# How often a lock waiter re-checks a claim.
_LOCK_POLL = 0.2

TEMPLATE = '''\
"""%s"""
from peewee import *


def up(migrator, db):
    # migrator.migrate(
    #     migrator.add_column('tbl', 'col', TextField(default='')))
    pass


# def down(migrator, db):
#     pass
'''

class MigrationError(Exception): pass

class MigrationLocked(MigrationError):
    """A migration is held by another runner or was interrupted."""
    pass

def _index(name):
    match = re.match(r'\d+', name)
    return int(match.group()) if match is not None else -1

class Migration(namedtuple('Migration', ('idx', 'name', 'path', 'applied',
                                         'state'),
                           defaults=('pending',))):
    __slots__ = ()

    @classmethod
    def load_directory(cls, directory):
        accum = []
        for path in glob.glob(os.path.join(directory, '[0-9]*.py')):
            name = os.path.basename(path)[:-3]
            accum.append(Migration(_index(name), name, path, None))
        return sorted(accum)

    def load(self):
        # Execute the file at the exact path, bypassing sys.path and the
        # sys.modules cache (migration names collide across projects).
        spec = importlib.util.spec_from_file_location(self.name, self.path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


# A read-only plan entry: a Migration tuple plus the action a run would
# take for it ('apply', 'skip', 'revert', 'running', 'missing').
PlanItem = namedtuple('PlanItem', ('migration', 'action'))

# History row as read by the runner. ``state``/``started_at``/``holder``
# are None on tables written before locking existed.
HistorySnapshot = namedtuple('HistorySnapshot',
                             ('name', 'applied', 'state',
                              'started_at', 'holder'))


class Runner(object):
    def __init__(self, database, directory='migrations',
                 table_name='schema_migration', schema=None,
                 lock=False, wait_timeout=10.0):
        self.database = database
        self.directory = directory
        self.table_name = table_name
        self.schema = schema
        self.locking = bool(lock)
        self.wait_timeout = wait_timeout
        self.migrator = SchemaMigrator.from_database(database, schema=schema)

        # History lives in the schema it describes, so each schema tracks
        # its own applied set.
        if self.locking:
            # state/started_at/holder back the in-progress marker and the
            # cross-process claim. All nullable: a row without a state is
            # an applied row written by an older release.
            class History(database.Model):
                name = CharField(unique=True)
                applied = DateTimeField(default=datetime.datetime.now,
                                        null=True)
                state = CharField(null=True)
                started_at = DateTimeField(null=True)
                holder = CharField(null=True)
                class Meta:
                    legacy_table_names = False
                    table_name = self.table_name
                    schema = self.schema
        else:
            class History(database.Model):
                name = CharField(unique=True)
                applied = DateTimeField(default=datetime.datetime.now)
                class Meta:
                    legacy_table_names = False
                    table_name = self.table_name
                    schema = self.schema

        self.History = History
        # Upgraded tables may keep the original NOT NULL "applied" column
        # (sqlite cannot alter it in place); markers then use a sentinel.
        self._applied_notnull = False

        resolved = database
        if isinstance(database, Proxy) and database.obj is not None:
            resolved = database.obj
        if not self.locking:
            self._lock_kind = None
        elif (isinstance(resolved, PostgresqlDatabase) and
                not (CockroachDatabase and
                     isinstance(resolved, CockroachDatabase))):
            self._lock_kind = 'postgres'
        elif isinstance(resolved, MySQLDatabase):
            self._lock_kind = 'mysql'
        else:
            # SQLite, CockroachDB and anything else: the committed claim
            # row itself is the lock, polled until wait_timeout.
            self._lock_kind = 'table'

    def migrations(self):
        return Migration.load_directory(self.directory)

    def _history_rows(self):
        if not self.History.table_exists():
            return {}
        H = self.History
        if self.locking:
            # Select only the columns that exist: a read-only status/plan
            # must work against a table before _ensure_history() adds the
            # in-progress columns.
            columns = {c.name for c in
                       self.database.get_columns(self.table_name,
                                                 self.schema)}
            if {'state', 'started_at', 'holder'} <= columns:
                fields = (H.name, H.applied, H.state, H.started_at,
                          H.holder)
                values = (lambda t: t)
            else:
                fields = (H.name, H.applied)
                values = (lambda t: (t[0], t[1], None, None, None))
            return {t[0]: HistorySnapshot(*values(t))
                    for t in H.select(*fields).tuples()}
        return {h.name: h for h in H.select()}

    @staticmethod
    def _row_state(row):
        state = row.state
        if state == RUNNING or state == REVERTING:
            return state
        return APPLIED

    def applied(self):
        if not self.locking:
            if not self.History.table_exists():
                return {}
            return {h.name: h.applied for h in self.History.select()}

        return {name: row.applied
                for name, row in self._history_rows().items()
                if self._row_state(row) == APPLIED and row.applied is not None}

    def status(self):
        """
        Files and history merged as Migration tuples, in numeric order.
        ``applied`` is None for pending/in-progress files, ``path`` is
        None for rows whose files are gone, and ``state`` is one of
        'pending', 'applied', 'running' or 'reverting'.
        """
        rows = self._history_rows() if self.locking else self.applied()
        accum = []
        for m in self.migrations():
            row = rows.get(m.name)
            if row is None:
                accum.append(m)
            elif self.locking:
                state = self._row_state(row)
                applied = row.applied if state == APPLIED else None
                accum.append(m._replace(applied=applied, state=state))
            else:
                accum.append(m._replace(applied=row, state=APPLIED))
        gone = set(rows) - {m.name for m in accum}
        if self.locking:
            accum.extend(Migration(_index(n), n, None, rows[n].applied,
                                   self._row_state(rows[n]))
                         for n in gone)
        else:
            accum.extend(Migration(_index(n), n, None, rows[n], APPLIED)
                         for n in gone)
        return sorted(accum)

    def plan(self, target=None, revert=False):
        """
        Merge files and history into PlanItems without reading modules
        or writing anything. Up plans classify each file as 'apply',
        'skip' or 'running'; down plans as 'revert', 'running' or
        'missing' (an applied row whose file is gone).
        """
        rows = self.status()
        if not revert:
            known = {m.name for m in rows if m.path is not None}
            if target is not None and target not in known:
                raise MigrationError('unknown migration "%s".' % target)

            items = []
            for m in rows:
                if m.state in (RUNNING, REVERTING):
                    action = PLAN_IN_PROGRESS
                elif m.applied or m.path is None:
                    action = PLAN_SKIP
                else:
                    action = PLAN_APPLY
                items.append(PlanItem(m, action))
                if m.name == target:
                    break
            return items

        active = [m for m in rows
                  if m.applied or
                  (self.locking and m.state == REVERTING)]
        if target is not None and target not in {m.name for m in active}:
            raise MigrationError('"%s" is not an applied migration.' % target)

        items = []
        for m in reversed(active):
            if self.locking and m.state == REVERTING:
                action = PLAN_IN_PROGRESS
            elif m.path is None:
                action = PLAN_MISSING
            else:
                action = PLAN_REVERT
            items.append(PlanItem(m, action))
            if target is None or m.name == target:
                break
        return items

    def up(self, target=None, fake=False, force=False):
        """
        Apply all pending migrations in order, or, given a target, stop
        after applying it. Returns the applied names.

        With locking enabled, each migration is claimed exclusively; a
        migration left in a running state by a dead process is skipped
        with an error unless ``force`` is True.
        """
        rows = [m for m in self.status() if m.path is not None]
        if target is not None and target not in {m.name for m in rows}:
            raise MigrationError('unknown migration "%s".' % target)

        plan = []
        for migration in rows:
            if not migration.applied:
                module = migration.load()
                if getattr(module, 'up', None) is None:
                    raise MigrationError('%s does not define up().'
                                         % migration.name)
                plan.append((migration, module))
            if migration.name == target:
                break

        if not self.locking:
            self.History.create_table()
            accum = []
            for migration, module in plan:
                self._run(migration.name, module, 'up', fake=fake)
                accum.append(migration.name)
            return accum

        self._ensure_history()
        if fake:
            return self._fake_locked(plan)

        accum = []
        for migration, module in plan:
            with self._claim(migration, 'up', force) as action:
                if action == 'run':
                    self._execute(migration.name, module, 'up')
                    accum.append(migration.name)
        return accum

    def down(self, target=None, force=False):
        """
        Revert the most recently-applied migration, or, given a target,
        every applied migration back through the target.

        With locking enabled the revert is claimed exclusively and a
        migration interrupted mid-revert is only retried when ``force``
        is True. A missing migration file always aborts the plan.
        """
        rows = [m for m in self.status()
                if m.applied or
                (self.locking and m.state == REVERTING)]
        if target is not None and target not in {m.name for m in rows}:
            raise MigrationError('"%s" is not an applied migration.' % target)

        plan = []
        for migration in reversed(rows):
            if migration.path is None:
                raise MigrationError('cannot revert "%s": migration file '
                                     'is missing.' % migration.name)
            module = migration.load()
            if getattr(module, 'down', None) is None:
                raise MigrationError('%s does not define down().'
                                     % migration.name)

            plan.append((migration, module))
            if target is None or migration.name == target:
                break

        if not self.locking:
            accum = []
            for migration, module in plan:
                self._run(migration.name, module, 'down')
                accum.append(migration.name)
            return accum

        self._ensure_history()
        accum = []
        for migration, module in plan:
            with self._claim(migration, 'down', force) as action:
                if action == 'run':
                    self._execute(migration.name, module, 'down')
                    accum.append(migration.name)
        return accum

    def fake(self, target=None):
        """
        Record pending migrations as applied without running them,
        stopping after the target if given. Returns the recorded names.
        """
        return self.up(target, fake=True)

    def create(self, name='', body=None):
        """Write a numbered skeleton migration file. Returns its path."""
        os.makedirs(self.directory, exist_ok=True)
        idx = max((m.idx for m in self.migrations()), default=0) + 1
        slug = re.sub(r'[^\w]+', '_', name.strip()).strip('_').lower()
        path = os.path.join(self.directory,
                            '%04d_%s.py' % (idx, slug or 'migration'))
        with open(path, 'w') as fh:
            fh.write(body or (TEMPLATE % name))
        return path

    # -- locking ---------------------------------------------------------

    def _ensure_history(self):
        # Create the table, then backfill the in-progress columns onto a
        # table created by an older release.
        self.History.create_table()
        columns = {c.name: c for c in
                   self.database.get_columns(self.table_name, self.schema)}
        for column in ('state', 'started_at', 'holder'):
            if column not in columns:
                field = self.History._meta.fields[column]
                self.migrator.add_column(self.table_name, column,
                                         field).run()
        applied = columns.get('applied')
        self._applied_notnull = bool(applied is not None and not applied.null)

    def _marker_applied(self):
        # Markers mean "not finished": NULL when the column allows it, an
        # epoch sentinel on upgraded tables whose column is NOT NULL. The
        # state column is what actually classifies the row.
        if self._applied_notnull:
            return datetime.datetime(1970, 1, 1)
        return None

    @staticmethod
    def _holder_token():
        return '%s@%s/%s' % (os.getpid(), socket.gethostname(),
                            uuid.uuid4().hex[:8])

    def _lock_key(self, name):
        raw = '\0'.join((self.schema or '', self.table_name, name))
        digest = hashlib.sha1(raw.encode('utf-8')).hexdigest()
        if self._lock_kind == 'postgres':
            # Positive int4-range value, stable across processes.
            return int(digest[:15], 16)
        return 'pwmig_' + digest

    def _get_row(self, name):
        return self.History.get_or_none(self.History.name == name)

    def _raise_stale(self, migration, state, row):
        holder = row.holder if row is not None else '?'
        since = row.started_at if row is not None else None
        raise MigrationError(
            '%s was left in a "%s" state (holder=%s, since=%s). Its schema '
            'changes may be partially applied; inspect the schema and '
            're-run with force=True to execute it again.'
            % (migration.name, state, holder, since))

    def _locked_message(self, name, row, waited):
        if row is not None and row.state in (RUNNING, REVERTING):
            detail = 'state=%s, holder=%s, since=%s' % (
                row.state, row.holder, row.started_at)
        else:
            detail = 'held by another process'
        return ('migration "%s" is locked (%s); waited %.2fs. If no other '
                'migration is running, re-run with force=True.'
                % (name, detail, waited))

    def _advisory_acquire(self, name):
        deadline = time.monotonic() + max(0.0, self.wait_timeout)
        key = self._lock_key(name)
        while True:
            if self._lock_kind == 'postgres':
                cursor = self.database.execute_sql(
                    'SELECT pg_try_advisory_lock(%s)', (key,))
                acquired = cursor.fetchone()[0]
            else:
                cursor = self.database.execute_sql(
                    'SELECT GET_LOCK(%s, 0)', (key,))
                acquired = (cursor.fetchone() or [None])[0] == 1
            if acquired:
                return
            if time.monotonic() >= deadline:
                waited = max(0.0, self.wait_timeout)
                raise MigrationLocked(
                    self._locked_message(name, self._get_row(name), waited))
            time.sleep(_LOCK_POLL)

    def _advisory_release(self, name):
        key = self._lock_key(name)
        try:
            if self._lock_kind == 'postgres':
                self.database.execute_sql(
                    'SELECT pg_advisory_unlock(%s)', (key,)).fetchall()
            else:
                self.database.execute_sql(
                    'SELECT RELEASE_LOCK(%s)', (key,)).fetchall()
        except Exception:
            # A dead session releases the lock server-side automatically.
            logger.debug('failed to release lock for %s', name,
                         exc_info=True)

    def _stamp(self, name, state, insert, token):
        now = datetime.datetime.now()
        if insert:
            self.History.insert(
                name=name, applied=self._marker_applied(), state=state,
                started_at=now, holder=token).execute()
        else:
            (self.History.update(
                {self.History.state: state,
                 self.History.started_at: now,
                 self.History.holder: token})
             .where(self.History.name == name)
             .execute())

    @contextlib.contextmanager
    def _claim(self, migration, direction, force):
        """
        Exclusive claim on one migration. Yields 'skip' when another
        runner already settled it, otherwise 'run' with the in-progress
        marker committed before control returns.
        """
        name = migration.name
        if self._lock_kind in ('postgres', 'mysql'):
            self._advisory_acquire(name)
            try:
                row = self._get_row(name)
                state = self._row_state(row) if row is not None else None
                if direction == 'up':
                    if state == APPLIED:
                        yield 'skip'
                        return
                    if state in (RUNNING, REVERTING):
                        if not force:
                            self._raise_stale(migration, state, row)
                        self._stamp(name, RUNNING, False,
                                    self._holder_token())
                    else:
                        self._stamp(name, RUNNING, True,
                                    self._holder_token())
                else:
                    if state == APPLIED:
                        self._stamp(name, REVERTING, False,
                                    self._holder_token())
                    elif state == REVERTING:
                        if not force:
                            self._raise_stale(migration, state, row)
                        self._stamp(name, REVERTING, False,
                                    self._holder_token())
                    elif row is None:
                        # Another runner reverted it first.
                        yield 'skip'
                        return
                    else:
                        raise MigrationError(
                            '"%s" is not an applied migration.' % name)
                yield 'run'
            finally:
                self._advisory_release(name)
        else:
            action = self._claim_table(migration, direction, force)
            if action == 'skip':
                yield 'skip'
            else:
                yield 'run'

    def _claim_table(self, migration, direction, force):
        # Polling claim for backends without session locks: an inserted
        # marker row is the lock, with the unique name column resolving
        # the race. wait_timeout bounds how long a live holder is waited
        # for; force overwrites a marker whose holder never came back.
        name = migration.name
        new_state = RUNNING if direction == 'up' else REVERTING
        deadline = time.monotonic() + max(0.0, self.wait_timeout)
        while True:
            row = self._get_row(name)
            state = self._row_state(row) if row is not None else None
            won = False
            if direction == 'up':
                if state == APPLIED:
                    return 'skip'
                if state in (RUNNING, REVERTING):
                    if force:
                        token = self._holder_token()
                        self._stamp(name, RUNNING, False, token)
                        won = self._get_row(name).holder == token
                else:
                    token = self._holder_token()
                    (self.History
                     .insert(name=name,
                             applied=self._marker_applied(),
                             state=RUNNING,
                             started_at=datetime.datetime.now(),
                             holder=token)
                     .on_conflict_ignore()
                     .execute())
                    claimed = self._get_row(name)
                    won = claimed is not None and claimed.holder == token
            else:
                if row is None:
                    # Another runner reverted it first.
                    return 'skip'
                if state == APPLIED or (state == REVERTING and force):
                    token = self._holder_token()
                    (self.History.update(
                        {self.History.state: new_state,
                         self.History.started_at: datetime.datetime.now(),
                         self.History.holder: token})
                     .where(self.History.name == name,
                            self.History.state == state)
                     .execute())
                    claimed = self._get_row(name)
                    won = claimed is not None and claimed.holder == token
                elif state in (RUNNING, REVERTING):
                    won = False  # Occupied (or stale without force).
                else:
                    raise MigrationError(
                        '"%s" is not an applied migration.' % name)

            if won:
                return 'run'
            if time.monotonic() >= deadline:
                waited = max(0.0, self.wait_timeout)
                raise MigrationLocked(
                    self._locked_message(name, self._get_row(name), waited))
            time.sleep(_LOCK_POLL)

    def _fake_locked(self, plan):
        accum = []
        for migration, _ in plan:
            row = self._get_row(migration.name)
            if row is not None and self._row_state(row) == APPLIED:
                continue
            if row is not None:
                raise MigrationError(
                    'cannot fake "%s": migration is in state "%s"; settle '
                    'it with up(force=True) first.'
                    % (migration.name, row.state))
            self.History.insert(
                name=migration.name, state=APPLIED,
                applied=datetime.datetime.now()).execute()
            accum.append(migration.name)
        return accum

    # -- execution -------------------------------------------------------

    def _run(self, name, module, direction, fake=False):
        # Legacy path (locking disabled). The plan was validated as it
        # was built. Run it blind.
        logger.info('%s: %s%s', direction, name, ' [FAKE]' if fake else '')
        if fake:
            self.History.create(name=name)
            return

        with self.migrator.migration_context(getattr(module, 'atomic', True)):
            getattr(module, direction)(self.migrator, self.database)
            if direction == 'up':
                self.History.create(name=name)
            else:
                (self.History.delete()
                 .where(self.History.name == name)
                 .execute())

    def _execute(self, name, module, direction):
        # The in-progress marker was committed by _claim(). Run the
        # migration body and settle the history row in the same
        # transaction so a committed schema change is always recorded.
        logger.info('%s: %s', direction, name)
        with self.migrator.migration_context(getattr(module, 'atomic', True)):
            getattr(module, direction)(self.migrator, self.database)
            if direction == 'up':
                (self.History.update(
                    {self.History.state: APPLIED,
                     self.History.applied: datetime.datetime.now()})
                 .where(self.History.name == name)
                 .execute())
            else:
                (self.History.delete()
                 .where(self.History.name == name)
                 .execute())


def run(database, directory='migrations', **kwargs):
    return Runner(database, directory, **kwargs).up()


_COMMON_DEFAULTS = (
    (datetime.datetime.now, 'datetime.datetime.now', 'import datetime'),
    (datetime.datetime.utcnow, 'datetime.datetime.utcnow', 'import datetime'),
    (datetime.date.today, 'datetime.date.today', 'import datetime'),
    (time.time, 'time.time', 'import time'),
    (time.time_ns, 'time.time_ns', 'import time'),
    (uuid.uuid4, 'uuid.uuid4', 'import uuid'),
    (dict, 'dict', None),
    (list, 'list', None))

def _handle_common_default(default, args, imports):
    for value, source, imp in _COMMON_DEFAULTS:
        if default == value:
            args.append('default=%s' % source)
            if imp:
                imports.add(imp)
            return True
    if isinstance(default, decimal.Decimal):
        args.append('default=decimal.Decimal(%r)' % str(default))
        imports.add('import decimal')
        return True
    return False

def _build_field_args(field, imports, todos, targets=None, unbound=False):
    args = []

    # Populate FK-only attrs first.
    if isinstance(field, ForeignKeyField):
        rel_model = field.rel_model
        # 'self' only means something inside a class body.
        if not unbound and rel_model is field.model:
            rel_name = "'self'"
        else:
            rel_name = targets[rel_model]
        args.append(rel_name)
        if unbound or field.rel_field is not rel_model._meta.primary_key:
            # Destination field needs to be explicitly specified when the
            # field is unbound (add_column calls) or if FK is not to the
            # rel's PK.
            args.append('field=%s.%s' % (rel_name, field.rel_field.name))

    if field.primary_key and not isinstance(field, AutoField):
        args.append('primary_key=True')
    if field.unique:
        args.append('unique=True')
    elif field.index and not isinstance(field, ForeignKeyField):
        args.append('index=True')
    if field.null:
        args.append('null=True')

    if isinstance(field, CharField) and field.max_length != 255:
        args.append('max_length=%r' % field.max_length)
    elif isinstance(field, DecimalField):
        if field.max_digits != 10:
            args.append('max_digits=%r' % field.max_digits)
        if field.decimal_places != 5:
            args.append('decimal_places=%r' % field.decimal_places)

    # Basic `default=` handling.
    if field.default is not None:
        default = field.default
        if isinstance(default, enum.Enum):
            # IntEnum passes the isinstance() below but %r is not valid source.
            default = default.value
        if isinstance(default, (str, int, float, bool)):
            args.append('default=%r' % default)
        elif not _handle_common_default(default, args, imports):
            todos.append('%s: default %r must be added by hand' %
                         (field.name, field.default))

    # Specify column-name if necessary.
    if isinstance(field, ForeignKeyField):
        default_colname = (field.name if field.name.endswith('_id')
                           else field.name + '_id')
        if field.column_name != default_colname:
            args.append('column_name=%r' % field.column_name)
        if field.on_delete: args.append('on_delete=%r' % field.on_delete)
        if field.on_update: args.append('on_update=%r' % field.on_update)
    elif field.column_name != field.name:
        args.append('column_name=%r' % field.column_name)

    return args

def _build_field(field, imports, todos, targets=None, unbound=False):
    cls = type(field)
    if isinstance(field, EnumFieldMixin):
        # Emit the storage type so the file does not depend on the enum.
        cls = next(c for c in cls.__mro__ if c.__module__ == 'peewee')
    if cls.__module__ != 'peewee':
        imports.add('from %s import %s' % (cls.__module__, cls.__name__))

    args = _build_field_args(field, imports, todos, targets, unbound)
    return '%s(%s)' % (cls.__name__, ', '.join(args))

def _is_implicit_id(field):
    return (type(field) is AutoField and field.name == 'id' and
            field.column_name == 'id')

def _build_model(model, imports, todos, targets):
    meta = model._meta
    lines = ['class %s(Model):' % model.__name__]
    for field in meta.sorted_fields:
        if _is_implicit_id(field):
            continue
        lines.append('    %s = %s' % (
            field.name,
            _build_field(field, imports, todos, targets)))

    lines.append('    class Meta:')
    lines.append('        database = db')
    lines.append('        table_name = %r' % meta.table_name)
    if meta.schema:
        lines.append('        schema = %r' % meta.schema)
    pk = meta.primary_key
    if isinstance(pk, CompositeKey):
        lines.append('        primary_key = CompositeKey(%s)' %
                     ', '.join(repr(f) for f in pk.field_names))
    elif not pk:
        lines.append('        primary_key = False')

    indexes = []
    for index in (meta.indexes or ()):
        if isinstance(index, (list, tuple)):
            columns, unique = index
            indexes.append((tuple(columns), unique))
        else:
            idx_name = getattr(index, '_name', None) or repr(index)
            todos.append('%s: index %s must be added by hand' %
                         (meta.table_name, idx_name))
    if indexes:
        lines.append('        indexes = %r' % (tuple(indexes),))

    return lines

def _build_stub(model, fields, imports):
    # A frozen stand-in for an existing table: its name plus whichever
    # referenced columns are not the implicit id.
    meta = model._meta
    lines = ['class %s(Model):' % model.__name__]
    for field in fields:
        if isinstance(field, ForeignKeyField):
            # A pk that is itself a fk stores a plain integer.
            source = 'IntegerField(primary_key=True, column_name=%r)' % (
                field.column_name)
        else:
            source = _build_field(field, imports, [])
        lines.append('    %s = %s' % (field.name, source))
    lines.append('    class Meta:')
    lines.append('        database = db')
    lines.append('        table_name = %r' % meta.table_name)
    if meta.schema:
        lines.append('        schema = %r' % meta.schema)
    return lines

def _build_add_index(idx):
    extra = ', unique=True' if idx.unique else ''
    if idx.name and idx.name != make_index_name(idx.table, idx.columns):
        extra += ', name=%r' % idx.name
    return 'migrator.migrate(migrator.add_index(%r, %r%s))' % (
        idx.table,
        idx.columns,
        extra)

def _block(fn, lines):
    while lines and not lines[-1]:
        lines.pop()
    if not lines:
        return 'def %s(migrator, db):\n    pass' % fn

    indented = ['    %s' % line if line else '' for line in lines]
    return 'def %s(migrator, db):\n%s' % (fn, '\n'.join(indented))


def template(diff):
    """
    Render a playhouse.schema_diff.SchemaDiff as a migration-file body.
    Fully-determined changes render as runnable code. Foreign keys
    reference a model created in the file, 'self', or a frozen stub of
    the target table. Anything else needing a definition is flagged
    with a TODO comment.
    """
    todos, imports = [], set()

    # Ensure all FK reference targets exist, either as classes or stubs.
    fks = []
    for model in diff.create_tables:
        fks.extend(f for f in model._meta.sorted_fields
                   if isinstance(f, ForeignKeyField) and
                   f.rel_model is not model)
    fks.extend(f for f in diff.add_columns
               if isinstance(f, ForeignKeyField))

    created = set(diff.create_tables)
    stubs = {}  # Existing fk target -> referenced fields, by name.
    for field in fks:
        rel = field.rel_model
        if rel in created:
            continue
        if rel not in stubs:
            stubs[rel] = {}  # A stub renders even when only its name is used.
        if not _is_implicit_id(field.rel_field):
            stubs[rel][field.rel_field.name] = field.rel_field
    targets = {model: model.__name__ for model in created | set(stubs)}

    # Build any stub models needed for FKs to resolve properly.
    plan = []
    for model in sorted(stubs, key=lambda m: m.__name__):
        fields = sorted(stubs[model].values(), key=lambda f: f._sort_key)
        up = _build_stub(model, fields, imports)
        up.append('')
        plan.append((up, []))

    # Create tables for models.
    for model in diff.create_tables:
        up = _build_model(model, imports, todos, targets)
        up.extend(['db.create_tables([%s])' % model.__name__, ''])

        meta = model._meta
        down = 'migrator.migrate(migrator.drop_table(%r%s))' % (
            meta.table_name,
            ', schema=%r' % meta.schema if meta.schema else '')
        plan.append((up, [down]))

    # Index drops here to avoid issues dropping indexed columns w/sqlite.
    dropped = set(diff.drop_columns)
    for idx in diff.drop_indexes:
        up = ['migrator.migrate(migrator.drop_index(%r, %r))' %
              (idx.table, idx.name)]
        down = []
        if idx.columns is None:
            todos.append('%s: dropped index %s (partial/expression) cannot '
                         'be restored by down()' % (idx.table, idx.name))
        elif any((idx.table, column) in dropped for column in idx.columns):
            todos.append('%s: dropped index %s cannot be restored by '
                         'down() (column dropped)' % (idx.table, idx.name))
        else:
            down.append(_build_add_index(idx))
        plan.append((up, down))

    for field in diff.add_columns:
        meta = field.model._meta
        source = _build_field(field, imports, todos, targets, unbound=True)
        if not field.null and field.default is None:
            todos.append('%s.%s: not-null column needs a default or '
                         'allow_not_null backfill' %
                         (meta.table_name, field.column_name))
        plan.append((
            ['migrator.migrate(migrator.add_column(%r, %r, %s))' %
             (meta.table_name, field.column_name, source)],
            ['migrator.migrate(migrator.drop_column(%r, %r))' %
             (meta.table_name, field.column_name)]))

    # add_column() creates the index for an index=True/unique=True field
    # itself, while `down()` drops the index before the column.
    auto_indexed = set((f.model._meta.table_name, (f.column_name,),
                        bool(f.unique))
                       for f in diff.add_columns if f.index or f.unique)
    for idx in diff.add_indexes:
        if idx.columns is None:
            todos.append('%s: create index %s (partial/expression, details '
                         'not detected)' % (idx.table, idx.name))
            continue
        up = []
        if (idx.table, idx.columns, idx.unique) not in auto_indexed:
            up.append(_build_add_index(idx))
        down = ['migrator.migrate(migrator.drop_index(%r, %r))' %
                (idx.table, make_index_name(idx.table, idx.columns))]
        plan.append((up, down))

    for table, name in diff.drop_columns:
        plan.append((['migrator.migrate(migrator.drop_column(%r, %r))' %
                      (table, name)], []))
        todos.append('%s.%s: dropped column cannot be restored by down()' %
                     (table, name))

    qualified = set(f.model._meta.table_name
                    for f in diff.add_columns if f.model._meta.schema)
    if qualified:
        todos.append('schema-qualified tables (%s): column/index '
                     'operations are emitted unqualified. Run with --schema' %
                     ', '.join(sorted(qualified)))

    up, down = [], []
    for u, _ in plan:
        up.extend(u)
    for _, d in reversed(plan):
        down.extend(d)

    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    out = ['# Generated from a schema diff on %s.' % ts,
           'from peewee import *']

    out.extend(sorted(imports))
    out.append('')
    out.extend('# TODO: %s' % todo for todo in todos)
    if todos:
        out.append('')
    out.append(_block('up', up))
    out.append('')
    out.append('')
    out.append(_block('down', down))
    return '\n'.join(out) + '\n'


def _cwd_import(path):
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    return importlib.import_module(path)


def _resolve_models(spec):
    attr = None
    if ':' in spec:
        spec, attr = spec.split(':', 1)
    module = _cwd_import(spec)
    if attr:
        obj = getattr(module, attr, None)
        if obj is None:
            raise MigrationError('"%s" not found in module "%s".'
                                 % (attr, spec))
        if is_model(obj):
            return [obj], []
        try:
            models = list(obj)
        except TypeError:
            raise MigrationError('"%s:%s" must name a Model or a list '
                                 'of Models.' % (spec, attr))
        for model in models:
            if not is_model(model):
                raise MigrationError('"%s:%s" must name a Model or a list '
                                     'of Models.' % (spec, attr))
        return models, []
    models, skipped = [], []
    for value in vars(module).values():
        if is_model(value) and value is not Model and \
           value.__module__ == module.__name__:
            fields = value._meta.sorted_fields
            if len(fields) == 1 and _is_implicit_id(fields[0]) and \
               not value._meta.indexes:
                skipped.append(value.__name__)
            else:
                models.append(value)
    if not models:
        raise MigrationError('no models found in module "%s".' % spec)
    return models, skipped


def _resolve_database(spec):
    if '://' in spec:
        from playhouse.db_url import connect
        try:
            database = connect(spec)
        except (RuntimeError, ValueError) as exc:  # Malformed url.
            raise MigrationError(str(exc))
        try:
            database.connect()
            database.close()
        except Exception as exc:
            raise MigrationError('cannot connect to "%s": %s' % (spec, exc))
        return database
    if os.path.isfile(spec):
        return SqliteDatabase(spec)
    if '.' not in spec:
        raise MigrationError('database must be given as a db url, a sqlite '
                             'filename, or a dotted path to a Database '
                             'instance.')
    module_path, attr = spec.rsplit('.', 1)
    try:
        module = _cwd_import(module_path)
    except ModuleNotFoundError as exc:
        raise MigrationError('cannot import "%s": %s' % (module_path, exc))
    obj = getattr(module, attr, None)
    if obj is None:
        raise MigrationError('"%s" not found in "%s".' % (attr, module_path))
    if isinstance(obj, Proxy):
        if obj.obj is None:
            raise MigrationError('"%s" proxy is uninitialized.' % spec)
        obj = obj.obj
    if not isinstance(obj, Database):
        raise MigrationError('"%s" did not resolve to a Database.' % spec)
    if not obj.database:
        raise MigrationError('"%s" is a deferred database. Initialize it, '
                             'or point at one that is.' % spec)
    return obj


def _resolve_diff(database, spec):
    from playhouse.schema_diff import SchemaDiff, diff_models
    models, skipped = _resolve_models(spec)
    for name in skipped:
        sys.stderr.write('skipped: %s (no fields)\n' % name)
    if database is None:  # initial: assume an empty database.
        # Virtual tables are out of scope, matching diff_models.
        accum = []
        for model in models:
            if getattr(model._meta, 'extension_module', None):
                sys.stderr.write('skipped: %s (virtual table)\n'
                                 % model.__name__)
            else:
                accum.append(model)
        return SchemaDiff(sort_models(accum), [], [], [], [])
    try:
        return diff_models(database, models)
    except ValueError as exc:
        raise MigrationError(str(exc))


def _report(verb, names):
    for name in names:
        print('%s: %s' % (verb, name))
    if not names:
        print('nothing to do.')


def _cmd_status(runner, args):
    rows = runner.status()
    history = runner._history_rows() if runner.locking else {}
    for m in rows:
        if m.state in (RUNNING, REVERTING):
            marker = '~'
        elif not m.applied:
            marker = ' '
        elif m.path is not None:
            marker = 'x'
        else:
            marker = '?'
        line = '[%s] %s' % (marker, m.name)
        if m.applied:
            line += '  ' + m.applied.strftime('%Y-%m-%d %H:%M:%S')
        elif m.state in (RUNNING, REVERTING):
            detail = [m.state]
            row = history.get(m.name)
            if row is not None and row.holder:
                detail.append(row.holder)
            line += '  ' + ' '.join(detail)
        print(line)
    # Exit 1 when pending, so status can gate a deploy.
    if any(not m.applied for m in rows):
        return 1


def _cmd_up(runner, args):
    _report('applied', runner.up(args.target, force=args.force))


def _cmd_down(runner, args):
    _report('reverted', runner.down(args.target, force=args.force))


def _cmd_fake(runner, args):
    _report('faked', runner.fake(args.target))


_PLAN_LABELS = {
    PLAN_APPLY: 'will apply',
    PLAN_SKIP: 'will skip',
    PLAN_REVERT: 'will revert',
    PLAN_IN_PROGRESS: 'in progress',
    PLAN_MISSING: 'file missing',
}


def _cmd_plan(runner, args):
    # Read-only: the plan is computed from files and history, nothing is
    # created or executed.
    items = runner.plan(args.target, revert=args.down)
    rows = runner._history_rows()
    blocked = False
    for item in items:
        m = item.migration
        line = '%s: %s' % (_PLAN_LABELS[item.action], m.name)
        if item.action == PLAN_IN_PROGRESS:
            blocked = True
            row = rows.get(m.name)
            if row is not None and row.holder:
                line += ' (%s)' % row.holder
        elif item.action == PLAN_MISSING:
            blocked = True
        print(line)
    if not items:
        print('nothing to do.')
    # Nonzero when the run could not proceed cleanly, like `status`.
    return 1 if blocked else None


def _cmd_initial(runner, args):
    if not args.models:
        raise MigrationError('initial requires a models module.')
    if runner.migrations():
        raise MigrationError('migrations already exist in "%s".'
                             % args.directory)
    diff = _resolve_diff(None, args.models)
    print(runner.create('initial', body=template(diff)))


def _cmd_create(runner, args):
    print(runner.create(args.name))


def _cmd_generate(runner, args):
    if not args.models:
        raise MigrationError('generate requires a models module.')
    diff = _resolve_diff(runner.database, args.models)
    if not diff:
        print('schema matches models. Nothing to generate.')
        return
    print(runner.create(args.name, body=template(diff)))


def _cmd_diff(runner, args):
    if not args.models:
        raise MigrationError('diff requires a models module.')
    diff = _resolve_diff(runner.database, args.models)
    print(diff if diff else 'schema matches models.')


# Mirrored by the subparser declarations in _parser().
_COMMANDS = ('status', 'plan', 'up', 'down', 'initial', 'create',
             'generate', 'fake', 'diff')


def _as_bool(value):
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _read_config(path):
    # The default dotfile is optional. A file named with -c must exist
    # (the caller checks).
    if path is None:
        path = '.pwmigrate'
        if not os.path.exists(path):
            return {}
    config = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, _, value = line.partition('=')
            config[key.strip()] = value.strip()
    for key in sorted(set(config) - {'database', 'directory', 'models',
                                     'schema', 'table', 'lock', 'wait'}):
        sys.stderr.write('warning: unknown key "%s" in %s\n' % (key, path))
    return config


def _parser(config):
    prog = os.path.basename(sys.argv[0] or '')
    if not prog.startswith('pwmigrate'):
        prog = 'python -m playhouse.migrations'
    parser = argparse.ArgumentParser(
        prog=prog, description='peewee migration runner')
    parser.add_argument('database', help='dotted path to a Database '
                        'instance (e.g. myapp.settings.db), db url (e.g. '
                        'postgres:///app), or path to a sqlite file')
    parser.add_argument('-c', '--config', metavar='FILE',
                        help='config file supplying argument defaults '
                        '(default: .pwmigrate)')

    try:
        wait = float(config.get('wait', '10'))
    except ValueError:
        sys.stderr.write('warning: invalid wait=%r in config, using 10s\n'
                         % config.get('wait'))
        wait = 10.0
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('-d', '--directory',
                        default=config.get('directory', 'migrations'),
                        help='migrations directory (default: migrations)')
    common.add_argument('-s', '--schema',
                        default=config.get('schema'),
                        help='schema containing the tables to be migrated')
    common.add_argument('-t', '--table',
                        default=config.get('table', 'schema_migration'),
                        help='history table name')
    common.add_argument('--lock', action='store_true',
                        default=_as_bool(config.get('lock')),
                        help='serialize each migration across processes '
                        '(advisory lock plus in-progress marker)')
    common.add_argument('--wait', metavar='SECONDS', type=float,
                        default=wait,
                        help='seconds to wait for a locked migration '
                        'before failing (default: 10)')
    common.add_argument('-v', '--verbose', action='store_true',
                        help='echo SQL as it executes')

    sub = parser.add_subparsers(dest='command', metavar='command',
                                required=True)
    p = sub.add_parser('status', parents=[common],
                       help='list migrations and applied timestamps')
    p.set_defaults(func=_cmd_status)

    p = sub.add_parser('plan', parents=[common],
                       help='print what up/down would do without touching '
                       'the database')
    p.add_argument('target', nargs='?', help='stop after this migration')
    p.add_argument('--down', action='store_true',
                   help='plan a rollback instead of an upgrade')
    p.set_defaults(func=_cmd_plan)

    p = sub.add_parser('up', parents=[common],
                       help='apply pending migrations in order')
    p.add_argument('target', nargs='?', help='stop after this migration')
    p.add_argument('-f', '--force', action='store_true',
                   help='re-run a migration left in an in-progress state')
    p.set_defaults(func=_cmd_up)

    p = sub.add_parser('down', parents=[common],
                       help='revert the most recent migration')
    p.add_argument('target', nargs='?',
                   help='revert back through this migration')
    p.add_argument('-f', '--force', action='store_true',
                   help='retry a migration interrupted mid-revert')
    p.set_defaults(func=_cmd_down)

    p = sub.add_parser('initial', parents=[common],
                       help='generate the first migration, assuming an '
                       'empty database')
    p.add_argument('models', nargs='?', default=config.get('models'),
                   help='models module ("app.models" or '
                   '"app.models:MODELS")')
    p.set_defaults(func=_cmd_initial)

    p = sub.add_parser('create', parents=[common],
                       help='write a skeleton migration file')
    p.add_argument('name', help='migration name')
    p.set_defaults(func=_cmd_create)

    p = sub.add_parser('generate', parents=[common],
                       help='generate a migration from the schema diff')
    p.add_argument('name', help='migration name')
    p.add_argument('models', nargs='?', default=config.get('models'),
                   help='models module ("app.models" or '
                   '"app.models:MODELS")')
    p.set_defaults(func=_cmd_generate)

    p = sub.add_parser('fake', parents=[common],
                       help='record pending migrations without running them')
    p.add_argument('target', nargs='?', help='record through this migration')
    p.set_defaults(func=_cmd_fake)

    p = sub.add_parser('diff', parents=[common],
                       help='print schema drift against the models')
    p.add_argument('models', nargs='?', default=config.get('models'),
                   help='models module ("app.models" or '
                   '"app.models:MODELS")')
    p.set_defaults(func=_cmd_diff)
    return parser


def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    # -c must be known before the parser is built: the config file
    # supplies argument defaults.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument('-c', '--config')
    known, argv = pre.parse_known_args(argv)
    if known.config and not os.path.exists(known.config):
        sys.stderr.write('error: config file "%s" not found.\n'
                         % known.config)
        return 2
    config = _read_config(known.config)

    if argv and argv[0] in _COMMANDS and not config.get('database'):
        sys.stderr.write('error: no database given and no .pwmigrate '
                         'config found.\n')
        return 2
    if config.get('database') and (not argv or argv[0] in _COMMANDS):
        argv = [config['database']] + argv

    args = _parser(config).parse_args(argv)
    if args.verbose:
        peewee_logger = logging.getLogger('peewee')
        peewee_logger.addHandler(logging.StreamHandler())
        peewee_logger.setLevel(logging.DEBUG)

    database = None
    try:
        database = _resolve_database(args.database)
        runner = Runner(database, args.directory, args.table, args.schema,
                        lock=args.lock, wait_timeout=args.wait)
        return args.func(runner, args) or 0
    except (MigrationError, DatabaseError, InterfaceError,
            ImproperlyConfigured, ValueError) as exc:
        if args.verbose:
            traceback.print_exc()
        sys.stderr.write('error: %s\n' % exc)
        return 2
    finally:
        if database is not None:
            database.close()


if __name__ == '__main__':
    sys.exit(main())
