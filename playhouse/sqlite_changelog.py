import uuid

from peewee import *
from playhouse.sqlite_ext import JSONField


class _Missing(object):
    # Sentinel value used to distinguish "column is not present in the change
    # payload" (the column was not touched / does not exist on the tracked
    # table) from "column is present with an explicit NULL value".
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(_Missing, cls).__new__(cls)
        return cls._instance

    def __repr__(self):
        return '<MISSING>'

    def __bool__(self):
        return False

    def __reduce__(self):
        return (_Missing, ())


MISSING = _Missing()


class ChangeLogError(Exception):
    """Base class for errors raised by the changelog consumer channel."""


class LeaseBusyError(ChangeLogError):
    """Another consumer owns an active (non-expired) lease on the channel."""
    def __init__(self, name, owner, expires):
        self.name = name
        self.owner = owner
        self.expires = expires
        super(LeaseBusyError, self).__init__(
            'consumer "%s" is leased by %r until %s' % (
                name, owner, expires))


class LeaseLostError(ChangeLogError):
    """The lease was lost (taken over after expiry or never acquired).

    Raised when acknowledging, renewing or releasing a lease that is no
    longer owned by this consumer, so a stale producer can never commit
    progress that a new owner is already re-delivering.
    """


class ChangeLogGapError(ChangeLogError):
    """The changelog was purged past the consumer checkpoint.

    :param expected: first log id the consumer needs, which no longer exists.
    :param available: earliest surviving log id (``None`` when the log table
                      is empty).
    """
    def __init__(self, expected, available, name=None):
        self.expected = expected
        self.available = available
        self.name = name
        if available is None:
            msg = ('changelog gap for consumer %r: log id %s and all later '
                   'rows are gone (log table purged)' % (name, expected))
        else:
            msg = ('changelog gap for consumer %r: expected log id %s, '
                   'earliest available id is %s' % (
                       name, expected, available))
        super(ChangeLogGapError, self).__init__(msg)


class BaseChangeLog(Model):
    timestamp = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')])
    action = TextField()
    table = TextField()
    primary_key = IntegerField()
    changes = JSONField()


class BaseConsumerState(Model):
    # Persistent, recoverable checkpoint + lease for one named consumer
    # (consumer group). A separate table is used so that enabling the
    # consumption channel never alters the changelog table or triggers.
    name = TextField(primary_key=True)

    # Highest changelog id that has been confirmed (acked). The next fetch
    # starts at ``position + 1``: a stable, monotonically increasing cursor
    # over the append-only log.
    position = BigIntegerField(default=0)

    # Opaque fencing token identifying the consumer instance that currently
    # holds the lease on the unacknowledged range, along with its expiry.
    lease_owner = TextField(null=True)
    lease_expires = DateTimeField(null=True)

    # Last id covered by the current lease (the fetched, unacked range is
    # ``[position + 1, lease_to]``).
    lease_to = BigIntegerField(null=True)


class ChangeEntry(object):
    """A single changelog row as delivered by the consumer channel.

    The ``changes`` mapping only contains the columns that actually changed,
    mapped to ``[old_value, new_value]`` pairs. Use :meth:`old_value` /
    :meth:`new_value` to distinguish a missing column (returns the
    :data:`MISSING` sentinel) from a column that changed to/from ``NULL``
    (returns ``None``).
    """
    __slots__ = ('id', 'timestamp', 'action', 'table', 'primary_key',
                 'changes')

    def __init__(self, row):
        self.id = row.id
        self.timestamp = row.timestamp
        self.action = row.action
        self.table = row.table
        self.primary_key = row.primary_key
        self.changes = row.changes

    @property
    def columns(self):
        """Names of the columns actually present in this change payload."""
        return list(self.changes)

    def has_column(self, column):
        return column in self.changes

    def old_value(self, column):
        """Return the old value for ``column``.

        Returns :data:`MISSING` when the column is not present in this
        change; returns ``None`` when it is present with a SQL NULL value.
        """
        return self._value(column, 0)

    def new_value(self, column):
        """Return the new value for ``column``.

        Returns :data:`MISSING` when the column is not present in this
        change; returns ``None`` when it is present with a SQL NULL value.
        """
        return self._value(column, 1)

    def _value(self, column, index):
        values = self.changes.get(column, MISSING)
        if values is MISSING:
            return MISSING
        return values[index]

    def __repr__(self):
        return ('<ChangeEntry: id=%s %s %s pk=%s>' % (
            self.id, self.action, self.table, self.primary_key))


class ChangeLog(object):
    # Model class that will serve as the base for the changelog. This model
    # will be subclassed and mapped to your application database.
    base_model = BaseChangeLog

    # Model class used for consumer checkpoints / leases. Like the changelog
    # model it is subclassed and mapped to your application database.
    consumer_base_model = BaseConsumerState

    # Template for the triggers that handle updating the changelog table.
    # table: table name
    # action: insert / update / delete
    # new_old: NEW or OLD (OLD is for DELETE)
    # primary_key: table primary key column name
    # column_array: output of build_column_array()
    # change_table: changelog table name
    template = """CREATE TRIGGER IF NOT EXISTS %(table)s_changes_%(action)s
    AFTER %(action)s ON %(table)s
    BEGIN
        INSERT INTO %(change_table)s
            ("action", "table", "primary_key", "changes")
        SELECT
            '%(action)s', '%(table)s', %(new_old)s."%(primary_key)s", "changes"
        FROM (
            SELECT json_group_object(
                col,
                json_array(
                    case when json_valid("oldval") then json("oldval")
                        else "oldval" end,
                    case when json_valid("newval") then json("newval")
                        else "newval" end)
                ) AS "changes"
            FROM (
                SELECT json_extract(value, '$[0]') as "col",
                       json_extract(value, '$[1]') as "oldval",
                       json_extract(value, '$[2]') as "newval"
                FROM json_each(json_array(%(column_array)s))
                WHERE "oldval" IS NOT "newval"
            )
        );
    END;"""

    drop_template = 'DROP TRIGGER IF EXISTS %(table)s_changes_%(action)s'

    _actions = ('INSERT', 'UPDATE', 'DELETE')

    def __init__(self, db, table_name='changelog', consumer_table_name=None):
        self.db = db
        self.table_name = table_name
        # The consumption channel is opt-in: this table is only created when
        # consumer() is called, so the changelog table and triggers are
        # completely unaffected when the channel is not used.
        self.consumer_table_name = (consumer_table_name
                                    if consumer_table_name is not None
                                    else '%s_consumer' % table_name)

    def _build_column_array(self, model, use_old, use_new, skip_fields=None):
        # Builds a list of SQL expressions for each field we are tracking. This
        # is used as the data source for change tracking in our trigger.
        col_array = []
        for field in model._meta.sorted_fields:
            if field.primary_key:
                continue

            if skip_fields is not None and field.name in skip_fields:
                continue

            column = field.column_name
            new = 'NULL' if not use_new else 'NEW."%s"' % column
            old = 'NULL' if not use_old else 'OLD."%s"' % column

            if isinstance(field, JSONField):
                # Ensure that values are cast to JSON so that the serialization
                # is preserved when calculating the old / new.
                if use_old: old = 'json(%s)' % old
                if use_new: new = 'json(%s)' % new

            col_array.append("json_array('%s', %s, %s)" % (column, old, new))

        return ', '.join(col_array)

    def trigger_sql(self, model, action, skip_fields=None):
        assert action in self._actions
        use_old = action != 'INSERT'
        use_new = action != 'DELETE'
        cols = self._build_column_array(model, use_old, use_new, skip_fields)
        return self.template % {
            'table': model._meta.table_name,
            'action': action,
            'new_old': 'NEW' if action != 'DELETE' else 'OLD',
            'primary_key': model._meta.primary_key.column_name,
            'column_array': cols,
            'change_table': self.table_name}

    def drop_trigger_sql(self, model, action):
        assert action in self._actions
        return self.drop_template % {
            'table': model._meta.table_name,
            'action': action}

    @property
    def model(self):
        if not hasattr(self, '_changelog_model'):
            class ChangeLog(self.base_model):
                class Meta:
                    database = self.db
                    table_name = self.table_name
            self._changelog_model = ChangeLog

        return self._changelog_model

    @property
    def consumer_model(self):
        if not hasattr(self, '_consumer_model'):
            class ConsumerState(self.consumer_base_model):
                class Meta:
                    database = self.db
                    table_name = self.consumer_table_name
            self._consumer_model = ConsumerState

        return self._consumer_model

    def consumer(self, name, lease_duration=30, start=None, owner=None):
        """Open an incremental consumption channel for this changelog.

        :param name: consumer / consumer-group name. Checkpoints and leases
            are keyed by this name: distinct names consume independently,
            while processes sharing a name compete for one lease.
        :param lease_duration: lease lifetime in seconds. While a lease is
            active no other owner may fetch; once it expires another consumer
            instance may take the unacknowledged range over.
        :param start: starting point, only honored when ``name`` is registered
            for the first time. ``None`` or ``0`` start at the oldest row;
            ``'latest'`` starts after the current tail (new rows only); an
            int ``N`` starts at log id ``N``.
        :param owner: explicit fencing token (a random one is generated by
            default). A new token is generated per process / instance, which
            is what makes crash recovery safe.
        """
        # Ensure the backing tables exist. This is the only point at which
        # the consumer-state table is created -- nothing changes when the
        # channel is never enabled.
        self.model.create_table(safe=True)
        self.consumer_model.create_table(safe=True)
        return ChangeLogConsumer(self, name, lease_duration=lease_duration,
                                 start=start, owner=owner)

    def install(self, model, skip_fields=None, drop=True, insert=True,
                update=True, delete=True, create_table=True):
        ChangeLog = self.model
        if create_table:
            ChangeLog.create_table()

        actions = list(zip((insert, update, delete), self._actions))
        if drop:
            for _, action in actions:
                self.db.execute_sql(self.drop_trigger_sql(model, action))

        for enabled, action in actions:
            if enabled:
                sql = self.trigger_sql(model, action, skip_fields)
                self.db.execute_sql(sql)


class ChangeLogConsumer(object):
    """Incremental, checkpointed, lease-protected changelog consumer.

    Typical usage::

        consumer = changelog.consumer('search-index', lease_duration=30)
        while True:
            for entry in consumer.fetch(limit=100):
                handle(entry)
            consumer.ack()  # persist the checkpoint

    Delivery is at-least-once within one consumer name: acked changes are
    never delivered again, while an unacknowledged batch whose owner crashed
    is re-delivered to whoever takes the expired lease over. The owner token
    fences off the stale owner, so a range can never be committed by two
    owners at once.
    """
    def __init__(self, changelog, name, lease_duration=30, start=None,
                 owner=None):
        self.changelog = changelog
        self.db = changelog.db
        self.name = name
        if not isinstance(lease_duration, int) or lease_duration <= 0:
            raise ValueError('lease_duration must be a positive integer '
                             'number of seconds')
        self.lease_duration = lease_duration
        self.token = owner if owner is not None else uuid.uuid4().hex
        self._register(start)

    # -- helpers -----------------------------------------------------------

    @property
    def log_model(self):
        return self.changelog.model

    @property
    def state_model(self):
        return self.changelog.consumer_model

    def _lease_expiry(self, seconds):
        # Use the database clock for both setting and comparing the expiry
        # so different consumer processes do not depend on synchronized
        # wall clocks.
        return fn.datetime('now', '+%d seconds' % int(seconds))

    def _register(self, start):
        # Create the checkpoint row if this is the first time this consumer
        # name is seen; an existing row always wins, so ``start`` is only a
        # registration-time parameter and restarts resume from the persisted
        # checkpoint.
        CS = self.state_model
        CL = self.log_model
        with self.db.atomic('IMMEDIATE'):
            if CS.get_or_none(CS.name == self.name) is not None:
                return

            if start is None or start == 0:
                position = 0
            elif isinstance(start, str) and start in ('latest', 'tail',
                                                      'end'):
                position = CL.select(fn.MAX(CL.id)).scalar() or 0
            elif (isinstance(start, int) and not isinstance(start, bool)
                  and start >= 1):
                position = start - 1
            else:
                raise ValueError(
                    'start must be None, 0, "latest", or a positive log id, '
                    'got %r' % (start,))
            CS.create(name=self.name, position=position)

    def _lease_active(self):
        CS = self.state_model
        value = (CS
                 .select(CS.lease_expires > fn.datetime('now'))
                 .where(CS.name == self.name)
                 .scalar())
        return value == 1

    def _read(self, start, limit, stop=None):
        # Read up to ``limit`` log rows beginning at ``start`` (in write
        # order) and verify that the stream is still contiguous. Returns a
        # ``(rows, earliest_id_at_or_after_start)`` tuple; a missing start or
        # a hole raises ChangeLogGapError instead of silently replaying from
        # the new head.
        CL = self.log_model
        available = (CL
                     .select(fn.MIN(CL.id))
                     .where(CL.id >= start)
                     .scalar())
        if available is None:
            return [], None
        if available != start:
            raise ChangeLogGapError(start, available, self.name)

        query = CL.select().where(CL.id >= start)
        if stop is not None:
            query = query.where(CL.id <= stop)
        rows = list(query.order_by(CL.id).limit(limit))
        for index, row in enumerate(rows):
            if row.id != start + index:
                raise ChangeLogGapError(start + index, row.id, self.name)
        return rows, available

    #-- public API --------------------------------------------------------

    @property
    def position(self):
        """Last confirmed log id (the persisted checkpoint)."""
        CS = self.state_model
        return CS.select(CS.position).where(CS.name == self.name).scalar()

    def fetch(self, limit=100):
        """Lease and return the next batch of changes in write order.

        :param limit: maximum number of rows returned in one pull. The query
            always uses LIMIT, so the whole log is never loaded.
        :returns: a list of :class:`ChangeEntry`; an empty list means there
            are no new changes yet.
        :raises LeaseBusyError: another owner currently holds an active
            lease on this consumer name.
        :raises ChangeLogGapError: the checkpoint falls outside the retained
            log window (rows were purged).
        """
        if not isinstance(limit, int) or limit <= 0:
            raise ValueError('limit must be a positive integer')
        CS = self.state_model
        with self.db.atomic('IMMEDIATE'):
            state = CS.get(CS.name == self.name)

            if state.lease_owner is not None and self._lease_active():
                if state.lease_owner == self.token:
                    # We still own an unacked batch: re-deliver exactly that
                    # range (at-least-once) instead of leasing new rows.
                    rows, _ = self._read(state.position + 1, limit,
                                         stop=state.lease_to)
                    return [ChangeEntry(row) for row in rows]
                raise LeaseBusyError(self.name, state.lease_owner,
                                     state.lease_expires)

            start = state.position + 1
            rows, available = self._read(start, limit)
            if not rows:
                # No row at or after the checkpoint. Tell "caught up" apart
                # from "the log was purged": the latter is reported loudly
                # instead of silently replaying from the new head.
                max_id = (self.log_model
                          .select(fn.MAX(self.log_model.id))
                          .scalar())
                if max_id is not None and max_id < start:
                    # Caught up (also covers an explicit start id beyond the
                    # current tail).
                    return []
                if max_id is None and state.position == 0:
                    # Fresh consumer and nothing has ever been logged.
                    return []
                raise ChangeLogGapError(start, None, self.name)

            # Conditional claim: free lease, expired lease, or our own
            # (expired) lease may be claimed; a concurrently acquired valid
            # lease makes the UPDATE match zero rows. IMMEDIATE transactions
            # plus this predicate guarantee one owner per range.
            claimed = (CS
                       .update(lease_owner=self.token,
                               lease_expires=self._lease_expiry(
                                   self.lease_duration),
                               lease_to=rows[-1].id)
                       .where((CS.name == self.name)
                              & (CS.lease_owner.is_null()
                                 | (CS.lease_expires <= fn.datetime('now'))
                                 | (CS.lease_owner == self.token)))
                       .execute())
            if not claimed:
                raise LeaseBusyError(self.name, state.lease_owner,
                                     state.lease_expires)

            return [ChangeEntry(row) for row in rows]

    def ack(self):
        """Confirm the fetched batch and persist the checkpoint.

        Advances ``position`` to the last leased id and releases the lease.
        Raises :class:`LeaseLostError` if this instance no longer owns the
        lease (it expired and another instance took over), in which case the
        checkpoint is left untouched and no progress is double-committed.
        """
        CS = self.state_model
        with self.db.atomic('IMMEDIATE'):
            updated = (CS
                       .update(position=CS.lease_to,
                               lease_owner=None,
                               lease_expires=None,
                               lease_to=None)
                       .where((CS.name == self.name)
                              & (CS.lease_owner == self.token)
                              & CS.lease_to.is_null(False))
                       .execute())
            if not updated:
                raise LeaseLostError(
                    'consumer %r: lease lost or no pending batch' %
                    self.name)
            return self.position

    def renew(self, lease_duration=None):
        """Extend the current lease (heartbeat). Raises LeaseLostError."""
        if lease_duration is not None:
            if (not isinstance(lease_duration, int)
                    or lease_duration <= 0):
                raise ValueError('lease_duration must be a positive integer '
                                 'number of seconds')
            self.lease_duration = lease_duration
        CS = self.state_model
        updated = (CS
                   .update(lease_expires=self._lease_expiry(
                               self.lease_duration))
                   .where((CS.name == self.name)
                          & (CS.lease_owner == self.token))
                   .execute())
        if not updated:
            raise LeaseLostError(
                'consumer %r: cannot renew, lease lost' % self.name)

    def release(self):
        """Give up the current lease without confirming anything.

        The same range becomes eligible for fetch (and takeover) again.
        Raises :class:`LeaseLostError` if this instance does not own it.
        """
        CS = self.state_model
        updated = (CS
                   .update(lease_owner=None, lease_expires=None,
                           lease_to=None)
                   .where((CS.name == self.name)
                          & (CS.lease_owner == self.token))
                   .execute())
        if not updated:
            raise LeaseLostError(
                'consumer %r: cannot release, lease lost' % self.name)
