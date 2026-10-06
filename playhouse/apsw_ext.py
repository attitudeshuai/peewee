"""
Peewee integration with APSW, "another python sqlite wrapper".

Project page: https://rogerbinns.github.io/apsw/

APSW is a really neat library that provides a thin wrapper on top of SQLite's
C interface.

Here are just a few reasons to use APSW, taken from the documentation:

* APSW gives all functionality of SQLite, including virtual tables, virtual
  file system, blob i/o, backups and file control.
* Connections can be shared across threads without any additional locking.
* Transactions are managed explicitly by your code.
* APSW can handle nested transactions.
* Unicode is handled correctly.
* APSW is faster.
"""
import apsw
from peewee import *
from peewee import EXCEPTIONS
from peewee import __exception_wrapper__
from peewee import BooleanField as _BooleanField
from peewee import DateField as _DateField
from peewee import DateTimeField as _DateTimeField
from peewee import DecimalField as _DecimalField
from peewee import TimeField as _TimeField


# apsw raises one exception per sqlite result code. Map them the way the
# sqlite3 module does, so callers can catch peewee's DB-API exceptions.
EXCEPTIONS.update({
    'SQLError': OperationalError,
    'BusyError': OperationalError,
    'LockedError': OperationalError,
    'ReadOnlyError': OperationalError,
    'IOError': OperationalError,
    'FullError': OperationalError,
    'CantOpenError': OperationalError,
    'SchemaChangeError': OperationalError,
    'InterruptError': OperationalError,
    'AbortError': OperationalError,
    'PermissionsError': OperationalError,
    'ProtocolError': OperationalError,
    'EmptyError': OperationalError,
    'ExtensionLoadingError': OperationalError,
    'CorruptError': DatabaseError,
    'NotADBError': DatabaseError,
    'AuthError': DatabaseError,
    'FormatError': DatabaseError,
    'TooBigError': DataError,
    'MismatchError': IntegrityError,
    'MisuseError': InterfaceError,
    'RangeError': InterfaceError,
    'NotFoundError': InternalError,
    'BindingsError': ProgrammingError,
    'ConnectionClosedError': ProgrammingError,
    'CursorClosedError': ProgrammingError,
    'ThreadingViolationError': ProgrammingError,
})


class APSWDatabase(SqliteDatabase):
    server_version = tuple(int(i) for i in apsw.sqlitelibversion().split('.'))

    def __init__(self, database, **kwargs):
        self._modules = {}
        super(APSWDatabase, self).__init__(database, **kwargs)
        # APSW connections may be shared and modified across threads.
        self._ext.cross_thread = True

    def register_module(self, mod_name, mod_inst, override=False):
        return self._ext.register('module', mod_name, mod_inst,
                                  override=override)

    def unregister_module(self, mod_name, policy=UNREGISTER_PENDING):
        return self._ext.unregister('module', mod_name, policy)

    def _connect(self):
        conn = apsw.Connection(self.database, **self.connect_params)
        if self._timeout is not None:
            conn.setbusytimeout(int(self._timeout * 1000))
        try:
            self._add_conn_hooks(conn)
        except:
            conn.close()
            raise
        return conn

    def _add_conn_hooks(self, conn):
        snapshot = super(APSWDatabase, self)._add_conn_hooks(conn)
        self._load_modules(conn, snapshot.get('module', ()))
        return snapshot

    def _load_modules(self, conn, items):
        for mod_name, mod_inst in items.items():
            conn.createmodule(mod_name, mod_inst)

    def _load_aggregates(self, conn, items):
        for name, (klass, num_params) in items.items():
            def make_aggregate(klass=klass):
                return (klass(), klass.step, klass.finalize)
            conn.createaggregatefunction(name, make_aggregate)

    def _load_collations(self, conn, items):
        for name, fn in items.items():
            conn.createcollation(name, fn)

    def _load_functions(self, conn, items):
        for name, (fn, num_params, deterministic) in items.items():
            args = (deterministic,) if deterministic else ()
            conn.createscalarfunction(name, fn, num_params, *args)

    def _load_window_functions(self, conn, items):
        for name, (klass, num_params) in items.items():
            def make_window(klass=klass):
                return (klass(), klass.step, klass.finalize, klass.value,
                        klass.inverse)
            conn.create_window_function(name, make_window, num_params)

    def _unload_extension(self, kind, conn, name, payload):
        if kind == 'function':
            args = (payload[2],) if payload[2] else ()
            conn.createscalarfunction(name, None, payload[1], *args)
        elif kind == 'aggregate':
            conn.createaggregatefunction(name, None)
        elif kind == 'collation':
            conn.createcollation(name, None)
        elif kind == 'window':
            conn.create_window_function(name, None, payload[1])
        elif kind == 'module':
            conn.createmodule(name, None)

    def _load_extensions(self, conn):
        conn.enableloadextension(True)
        for extension in self._extensions:
            conn.loadextension(extension)

    def load_extension(self, extension):
        self._extensions.add(extension)
        if not self.is_closed():
            conn = self.connection()
            conn.enableloadextension(True)
            conn.loadextension(extension)

    def _last_insert_rowid(self, cursor):
        return cursor.connection.last_insert_rowid()

    def rows_affected(self, cursor):
        try:
            return cursor.connection.changes()
        except AttributeError:
            return cursor.cursor.connection.changes()  # RETURNING query.

    def commit(self):
        if self.is_closed():
            raise InterfaceError('Cannot commit, database connection not '
                                 'open.')
        with __exception_wrapper__:
            curs = self.cursor()
            if curs.connection.getautocommit():
                return False
            curs.execute('commit;')
        return True

    def rollback(self):
        if self.is_closed():
            raise InterfaceError('Cannot rollback, database connection not '
                                 'open.')
        with __exception_wrapper__:
            curs = self.cursor()
            if curs.connection.getautocommit():
                return False
            curs.execute('rollback;')
        return True


def nh(s, v):
    if v is not None:
        return str(v)

class BooleanField(_BooleanField):
    def db_value(self, v):
        v = super(BooleanField, self).db_value(v)
        if v is not None:
            return v and 1 or 0

class DateField(_DateField):
    db_value = nh

class TimeField(_TimeField):
    db_value = nh

class DateTimeField(_DateTimeField):
    db_value = nh

class DecimalField(_DecimalField):
    db_value = nh
