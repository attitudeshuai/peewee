import os
import shutil
import tempfile
import threading
import time
import logging
from functools import partial

try:
    import gevent
    from gevent.event import Event as GreenEvent
except ImportError:
    gevent = None

from peewee import *
from playhouse.sqliteq import ResultTimeout
from playhouse.sqliteq import ShutdownException
from playhouse.sqliteq import Spool
from playhouse.sqliteq import SqliteQueueDatabase
from playhouse.sqliteq import WriterPaused

from .base import BaseTestCase
from .base import TestModel
from .base import db_loader
from .base import get_sqlite_db
from .base import skip_if


get_db = partial(db_loader, 'sqlite', db_class=SqliteQueueDatabase)
db = get_sqlite_db()


class User(TestModel):
    name = TextField(unique=True)

    class Meta:
        table_name = 'threaded_db_test_user'


class BaseTestQueueDatabase(object):
    database_config = {}
    n_rows = 20
    n_threads = 20

    def setUp(self):
        super(BaseTestQueueDatabase, self).setUp()
        User._meta.database = db
        with db:
            db.create_tables([User], safe=True)

        User._meta.database = \
                self.database = get_db(**self.database_config)

        # Sanity check at startup.
        self.assertEqual(self.database.queue_size(), 0)

    def tearDown(self):
        super(BaseTestQueueDatabase, self).tearDown()
        User._meta.database = db
        with db:
            User.drop_table()
        if not self.database.is_closed():
            self.database.close()
        if not db.is_closed():
            db.close()
        filename = db.database
        if os.path.exists(filename):
            os.unlink(filename)

    def test_query_error(self):
        self.database.start()
        curs = self.database.execute_sql('foo bar baz')
        self.assertRaises(OperationalError, curs.fetchone)
        self.database.stop()

    def test_integrity_error(self):
        self.database.start()
        u = User.create(name='u')
        self.assertRaises(IntegrityError, User.create, name='u')

    def test_query_execution(self):
        qr = User.select().execute()
        self.assertEqual(self.database.queue_size(), 0)

        self.database.start()

        try:
            users = list(qr)
            huey = User.create(name='huey')
            mickey = User.create(name='mickey')

            self.assertTrue(huey.id is not None)
            self.assertTrue(mickey.id is not None)
            self.assertEqual(self.database.queue_size(), 0)

        finally:
            self.database.stop()

    def create_thread(self, fn, *args):
        raise NotImplementedError

    def create_event(self):
        raise NotImplementedError

    def test_multiple_threads(self):
        def create_rows(idx, nrows):
            for i in range(idx, idx + nrows):
                User.create(name='u-%s' % i)

        total = self.n_threads * self.n_rows
        self.database.start()
        threads = [self.create_thread(create_rows, i, self.n_rows)
                   for i in range(0, total, self.n_rows)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        self.assertEqual(User.select().count(), total)
        self.database.stop()

    def test_pause(self):
        event_a = self.create_event()
        event_b = self.create_event()

        def create_user(name, event, expect_paused):
            event.wait()
            if expect_paused:
                self.assertRaises(WriterPaused, lambda: User.create(name=name))
            else:
                User.create(name=name)

        self.database.start()

        t_a = self.create_thread(create_user, 'a', event_a, True)
        t_a.start()
        t_b = self.create_thread(create_user, 'b', event_b, False)
        t_b.start()

        User.create(name='c')
        self.assertEqual(User.select().count(), 1)

        # Pause operations but preserve the writer thread/connection.
        self.database.pause()

        event_a.set()
        self.assertEqual(User.select().count(), 1)
        t_a.join()

        self.database.unpause()
        self.assertEqual(User.select().count(), 1)

        event_b.set()
        t_b.join()
        self.assertEqual(User.select().count(), 2)

        self.database.stop()

    def test_stop_returns_true(self):
        self.database.start()
        User.create(name='a')
        self.assertTrue(self.database.stop())

    def test_restart(self):
        self.database.start()
        User.create(name='a')
        self.database.stop()
        self.database._results_timeout = 0.0001

        self.assertRaises(ResultTimeout, User.create, name='b')
        self.assertEqual(User.select().count(), 1)

        self.database.start()  # Will execute the pending "b" INSERT.
        self.database._results_timeout = None

        User.create(name='c')
        self.assertEqual(User.select().count(), 3)
        self.assertEqual(sorted(u.name for u in User.select()),
                         ['a', 'b', 'c'])

    def test_waiting(self):
        D = {}

        def create_user(name):
            D[name] = User.create(name=name).id

        threads = [self.create_thread(create_user, name)
                   for name in ('huey', 'charlie', 'zaizee')]
        [t.start() for t in threads]

        def get_users():
            D['users'] = [(user.id, user.name) for user in User.select()]

        tg = self.create_thread(get_users)
        tg.start()
        threads.append(tg)

        self.database.start()
        [t.join() for t in threads]
        self.database.stop()

        self.assertEqual(sorted(D), ['charlie', 'huey', 'users', 'zaizee'])

    def test_next_method(self):
        self.database.start()

        User.create(name='mickey')
        User.create(name='huey')
        query = iter(User.select().order_by(User.name))
        self.assertEqual(next(query).name, 'huey')
        self.assertEqual(next(query).name, 'mickey')
        self.assertRaises(StopIteration, lambda: next(query))

        self.assertEqual(
            next(self.database.execute_sql('PRAGMA journal_mode'))[0],
            'wal')

        self.database.stop()


class TestThreadedDatabaseThreads(BaseTestQueueDatabase, BaseTestCase):
    database_config = {'use_gevent': False}

    def tearDown(self):
        self.database._results_timeout = None
        super(TestThreadedDatabaseThreads, self).tearDown()

    def create_thread(self, fn, *args):
        t = threading.Thread(target=fn, args=args)
        t.daemon = True
        return t

    def create_event(self):
        return threading.Event()

    def test_timeout(self):
        @self.database.func()
        def slow(n):
            time.sleep(n)
            return 'slept %0.2f' % n

        self.database.start()

        # Make the result timeout very small, then call our function which
        # will cause the query results to time-out.
        self.database._results_timeout = 0.001
        def do_query():
            # Prepend a space so that we can force it through the threaded
            # pipeline, otherwise it would execute normally.
            cursor = self.database.execute_sql(' select slow(?)', (0.01,))
            self.assertEqual(cursor.fetchone()[0], 'slept 0.01')

        self.assertRaises(ResultTimeout, do_query)
        self.database.stop()


@skip_if(gevent is None, 'gevent not installed')
class TestThreadedDatabaseGreenlets(BaseTestQueueDatabase, BaseTestCase):
    database_config = {'use_gevent': True}
    n_rows = 10
    n_threads = 40

    def create_thread(self, fn, *args):
        return gevent.Greenlet(fn, *args)

    def create_event(self):
        return GreenEvent()


class TestPersistentSpool(BaseTestCase):
    """Tests for disk-backed spooling and restart replay."""

    def setUp(self):
        super(TestPersistentSpool, self).setUp()
        self.tmp = tempfile.mkdtemp()
        self.db_file = os.path.join(self.tmp, 'spool_test.db')
        self.spool_dir = os.path.join(self.tmp, 'spool')
        self.instances = []

    def tearDown(self):
        for instance in self.instances:
            try:
                instance.stop()
            except Exception:
                pass
            try:
                instance.close()
            except Exception:
                pass
            spool = getattr(instance, '_spool', None)
            if spool is not None:
                spool.close()
        shutil.rmtree(self.tmp, ignore_errors=True)
        super(TestPersistentSpool, self).tearDown()

    def new_database(self, **config):
        config.setdefault('persistent', True)
        config.setdefault('autostart', False)
        database = SqliteQueueDatabase(self.db_file,
                                       spool_dir=self.spool_dir, **config)
        self.instances.append(database)
        return database

    def create_table(self, database):
        cursor = database.execute_sql(
            'create table if not exists spool_user ('
            'id integer primary key, name text)')
        cursor.fetchone()

    def get_names(self, database):
        return [row[0] for row in database.execute_sql(
            'select name from spool_user order by name').fetchall()]

    def stage_backlog(self):
        # Commit "a", stop cleanly, then leave "b" and "c" only in the spool
        # (process "killed" before they could run).
        database = self.new_database()
        database.start()
        self.create_table(database)
        database.execute_sql(
            "insert into spool_user (name) values ('a')").fetchone()
        database.stop()
        database.close()
        database.execute_sql("insert into spool_user (name) values ('b')")
        database.execute_sql("insert into spool_user (name) values ('c')")
        return database

    def test_disabled_by_default(self):
        database = SqliteQueueDatabase(self.db_file, autostart=False)
        self.instances.append(database)
        self.assertTrue(database._spool is None)

    def test_spool_replays_backlog_in_order(self):
        first = self.stage_backlog()
        self.assertEqual(first.spool_discarded(), 0)

        # A fresh database instance replays the spool before new writes.
        database = self.new_database()
        database.start()
        self.assertEqual(self.get_names(database), ['a', 'b', 'c'])

        # Replayed entries are acknowledged and do not replay again.
        self.assertEqual(database._spool.pending_entries(), [])
        database.stop()
        database.close()

        restarted = self.new_database()
        restarted.start()
        self.assertEqual(self.get_names(restarted), ['a', 'b', 'c'])

    def test_replay_deduplicates_by_request_id(self):
        self.stage_backlog()

        database = self.new_database()
        database.start()
        self.assertEqual(self.get_names(database), ['a', 'b', 'c'])
        # Restarting any number of times never executes an entry twice.
        for _ in range(3):
            database.stop()
            database.close()
            database = self.new_database()
            database.start()
            self.assertEqual(self.get_names(database), ['a', 'b', 'c'])

    def test_dropped_oldest_is_counted(self):
        database = self.new_database(spool_max_size=3)
        database.start()
        self.create_table(database)
        database.stop()
        database.close()

        # Seven submissions into a cap-3 spool never raise or block.
        for i in range(7):
            cursor = database.execute_sql(
                "insert into spool_user (name) values ('n%d')" % i)
            self.assertTrue(cursor is not None)
        self.assertEqual(database.spool_discarded(), 4)

        restarted = self.new_database(spool_max_size=3)
        restarted.start()
        # Only the three newest entries survive and are replayed.
        self.assertEqual(self.get_names(restarted),
                         ['n4', 'n5', 'n6'])
        # Cumulative discard count survives the restart.
        self.assertEqual(restarted.spool_discarded(), 4)

    def test_unwritable_directory_warns_once(self):
        blocker = os.path.join(self.tmp, 'not-a-directory')
        with open(blocker, 'w') as fh:
            fh.write('x')

        records = []

        class ListHandler(logging.Handler):
            def emit(self, record):
                records.append(record)

        handler = ListHandler()
        spool_logger = logging.getLogger('peewee.sqliteq')
        spool_logger.addHandler(handler)
        try:
            database = SqliteQueueDatabase(
                self.db_file, persistent=True, spool_dir=blocker,
                autostart=False)
            self.instances.append(database)

            warnings = [r for r in records
                        if 'not writable' in r.getMessage()]
            self.assertEqual(len(warnings), 1)

            # Fallback: plain in-memory queue behavior.
            self.assertTrue(database._spool is None)
            database.start()
            self.create_table(database)
            database.execute_sql(
                "insert into spool_user (name) values ('x')").fetchone()
            self.assertEqual(self.get_names(database), ['x'])

            # No further warnings on subsequent operations.
            self.assertEqual(
                len([r for r in records
                     if 'not writable' in r.getMessage()]), 1)
        finally:
            spool_logger.removeHandler(handler)

    def test_pause_resume_stages_and_replays(self):
        database = self.new_database()
        database.start()
        self.create_table(database)
        database.pause()

        # Writes while paused are staged and remain not-ready; they do not
        # raise WriterPaused.
        cursor_a = database.execute_sql(
            "insert into spool_user (name) values ('p1')")
        cursor_b = database.execute_sql(
            "insert into spool_user (name) values ('p2')")
        self.assertFalse(cursor_a._event.is_set())
        self.assertFalse(cursor_b._event.is_set())

        database.unpause()
        cursor_a.fetchone()
        cursor_b.fetchone()
        self.assertEqual(self.get_names(database), ['p1', 'p2'])

    def test_stop_rejects_new_writes_and_defers(self):
        database = self.new_database()
        entered = threading.Event()
        release = threading.Event()

        @database.func()
        def gate(value):
            entered.set()
            release.wait(5)
            return value

        database.start()
        self.create_table(database)
        database.execute_sql(
            "insert into spool_user (name) values ((select gate('g0')))")
        self.assertTrue(entered.wait(2))

        def shutdown():
            return database.stop(timeout=2)

        stop_thread = threading.Thread(target=shutdown)
        stop_thread.start()

        # While shutting down new writes are refused.
        deadline = time.time() + 2
        while not database._closing and time.time() < deadline:
            time.sleep(0.005)
        self.assertTrue(database._closing)
        self.assertRaises(
            ShutdownException,
            lambda: database.execute_sql(
                "insert into spool_user (name) values ('late')"))

        release.set()
        stop_thread.join(5)
        self.assertFalse(stop_thread.is_alive())
        self.assertEqual(self.get_names(database), ['g0'])

    def test_stop_timeout_leaves_entries_for_next_start(self):
        database = self.new_database()

        @database.func()
        def slow(n):
            time.sleep(n)
            return n

        database.start()
        self.create_table(database)
        for _ in range(5):
            database.execute_sql(
                "insert into spool_user (name) values ((select slow(0.1)))")

        self.assertFalse(database.stop(timeout=0.05))

        # The daemon writer finishes draining; wait, then simulate a new
        # process over the same spool.
        writer = database._writer
        deadline = time.time() + 10
        while database._thread_helper.is_alive(writer) and \
                time.time() < deadline:
            time.sleep(0.02)
        database.close()
        database._spool.close()

        restarted = self.new_database()
        restarted.start()
        count = restarted.execute_sql(
            'select count(*) from spool_user').fetchone()[0]
        self.assertEqual(count, 5)

    def test_torn_spool_tail_is_repaired(self):
        database = self.new_database()
        database.start()
        self.create_table(database)
        database.execute_sql(
            "insert into spool_user (name) values ('a')").fetchone()
        database.stop()
        database.close()
        database._spool.close()

        # Corrupt the tail of the spool log (partial final frame).
        log_path = os.path.join(self.spool_dir, Spool.LOG_NAME)
        with open(log_path, 'ab') as fh:
            fh.write(b'\xff\x00\x10garbage-not-a-frame')

        restarted = self.new_database()
        restarted.start()
        self.assertEqual(self.get_names(restarted), ['a'])
        # The repaired spool accepts new writes normally.
        restarted.execute_sql(
            "insert into spool_user (name) values ('b')").fetchone()
        self.assertEqual(self.get_names(restarted), ['a', 'b'])
