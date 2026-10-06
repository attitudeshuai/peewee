import json
import logging
import os
import pickle
import tempfile
import uuid
from queue import Empty
from queue import Queue
from threading import Event
from threading import Lock
from threading import Thread

try:
    import gevent
    from gevent import Greenlet as GThread
    from gevent.event import Event as GEvent
    from gevent.queue import Empty as GEmpty
    from gevent.queue import Queue as GQueue
except ImportError:
    GThread = GQueue = GEvent = GEmpty = None

from peewee import SqliteDatabase


logger = logging.getLogger('peewee.sqliteq')


class ResultTimeout(Exception):
    pass

class WriterPaused(Exception):
    pass

class ShutdownException(Exception):
    pass


# Internal table used to record request ids that have been committed. The row
# is inserted in the same transaction as the user write, which makes replay
# idempotent even if the process is killed between the commit and the spool
# acknowledgement.
ACK_TABLE = 'peewee_spool_ack'
ACK_CREATE_SQL = (
    'CREATE TABLE IF NOT EXISTS "%s" (request_id TEXT PRIMARY KEY)' %
    ACK_TABLE)
ACK_LOOKUP_SQL = 'SELECT 1 FROM "%s" WHERE request_id = ?' % ACK_TABLE
ACK_INSERT_SQL = 'INSERT OR IGNORE INTO "%s" (request_id) VALUES (?)' % ACK_TABLE


class _EmptyCursor(object):
    """Cursor stand-in returned when a request is skipped as a duplicate."""
    lastrowid = None
    rowcount = -1
    description = None

    def fetchall(self):
        return []

    def close(self):
        pass


class AsyncCursor(object):
    __slots__ = ('sql', 'params', 'timeout', 'request_id',
                 '_event', '_cursor', '_exc', '_idx', '_rows', '_ready')

    def __init__(self, event, sql, params, timeout, request_id=None):
        self._event = event
        self.sql = sql
        self.params = params
        self.timeout = timeout
        self.request_id = request_id
        self._cursor = self._exc = self._idx = self._rows = None
        self._ready = False

    def set_result(self, cursor, exc=None):
        self._cursor = cursor
        self._exc = exc
        self._idx = 0
        self._rows = cursor.fetchall() if exc is None else []
        self._event.set()
        return self

    def set_skipped(self):
        # Request was already committed (e.g. duplicated by restart replay).
        return self.set_result(_EmptyCursor())

    def _wait(self, timeout=None):
        timeout = timeout if timeout is not None else self.timeout
        if not self._event.wait(timeout=timeout) and timeout is not None:
            raise ResultTimeout('results not ready, timed out.')
        if self._exc is not None:
            raise self._exc
        self._ready = True

    def __iter__(self):
        if not self._ready:
            self._wait()
        if self._exc is not None:
            raise self._exc
        return self

    def next(self):
        if not self._ready:
            self._wait()
        try:
            obj = self._rows[self._idx]
        except IndexError:
            raise StopIteration
        else:
            self._idx += 1
            return obj
    __next__ = next

    @property
    def lastrowid(self):
        if not self._ready:
            self._wait()
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        if not self._ready:
            self._wait()
        return self._cursor.rowcount

    @property
    def description(self):
        if not self._ready:
            self._wait()
        return self._cursor.description

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def fetchall(self):
        return list(self)  # Iterating implies waiting until populated.

    def fetchone(self):
        if not self._ready:
            self._wait()
        try:
            return next(self)
        except StopIteration:
            return None

SHUTDOWN = StopIteration
QUERY = object()
PAUSE = object()
UNPAUSE = object()
READY = object()


class ReplayJob(object):
    """A spooled write left behind by a previous process run."""
    __slots__ = ('request_id', 'sql', 'params', '_spool')

    def __init__(self, request_id, sql, params, spool):
        self.request_id = request_id
        self.sql = sql
        self.params = params
        self._spool = spool

    def set_result(self, cursor, exc=None):
        if exc is None:
            self._spool.ack(self.request_id)
        return self

    def set_skipped(self):
        self._spool.ack(self.request_id)
        return self


class Spool(object):
    """Filesystem-backed append-only staging area for write requests.

    The spool directory contains two files:

    * ``spool.log`` - append-only frame log. Frame types record a payload
      (the sql and params), an acknowledgement (the request committed
      successfully) or a drop (the request was discarded because the spool
      was full).
    * ``spool.meta`` - JSON metadata, currently the cumulative number of
      dropped requests, persisted when the log is compacted.

    Frame layout (big-endian)::

        type(1) id_len(2) id(utf-8) payload_len(4) payload

    A partially-written final frame (process killed mid-write) is detected
    and truncated when the spool is opened.
    """

    LOG_NAME = 'spool.log'
    META_NAME = 'spool.meta'
    PREAMBLE = b'SPQL1\n'

    PAYLOAD = 0x01
    ACK = 0x02
    DROP = 0x03

    PICKLE_PROTOCOL = 2  # Widely portable across Python versions.

    # Compact the log once this many acknowledgement/drop frames accumulate.
    CHECKPOINT_STALE_FRAMES = 256

    def __init__(self, directory, max_entries=None):
        self.directory = directory
        self.max_entries = max_entries  # None or 0 -> unbounded.
        self._lock = Lock()
        self._fh = None
        self._path = os.path.join(directory, self.LOG_NAME)
        self._meta_path = os.path.join(directory, self.META_NAME)
        self._data = {}                 # request_id -> (sql, params)
        self._pending = []              # pending request ids, in order.
        self._settled = set()           # acked or dropped ids.
        self._stale = 0                 # ACK/DROP frames since checkpoint.
        self._discarded = 0             # cumulative dropped count.

    def open(self):
        """Open the spool, creating the directory if needed.

        Returns ``True`` when the spool is usable, ``False`` when the
        directory cannot be created or written to.
        """
        try:
            os.makedirs(self.directory, exist_ok=True)
            probe = os.path.join(self.directory, '.write-test')
            with open(probe, 'w'):
                pass
            os.unlink(probe)
        except OSError:
            return False

        data = b''
        if os.path.exists(self._path):
            with open(self._path, 'rb') as fh:
                data = fh.read()
        valid_end, entries = self._parse(data)
        if valid_end < len(data):
            # Repair a torn final frame so future appends remain valid.
            try:
                with open(self._path, 'r+b') as fh:
                    fh.truncate(valid_end)
            except OSError:
                return False

        discarded_base = self._read_meta()
        payload_seen = set()
        for frame_type, rid, payload in entries:
            if frame_type == self.PAYLOAD:
                if rid in payload_seen:
                    # Duplicate payload frame (idempotency guard).
                    continue
                payload_seen.add(rid)
                self._data[rid] = pickle.loads(payload)
                self._pending.append(rid)
            elif frame_type == self.ACK:
                self._settled.add(rid)
                self._stale += 1
            elif frame_type == self.DROP:
                self._settled.add(rid)
                self._stale += 1
                self._discarded += 1

        pending = []
        for rid in self._pending:
            if rid in self._settled:
                self._data.pop(rid, None)
            else:
                pending.append(rid)
        self._pending = pending
        self._discarded += discarded_base

        try:
            self._fh = open(self._path, 'ab')
        except OSError:
            return False
        if not data:
            self._write_bytes(self.PREAMBLE, sync=False)
        return True

    def append(self, sql, params):
        """Stage a write request, returning its unique request id.

        When the spool is full the oldest pending request is dropped and the
        cumulative discard counter is incremented. This method never blocks
        on business logic: only a fast, lock-protected append is performed.
        """
        with self._lock:
            rid = uuid.uuid4().hex
            if self.max_entries:
                while len(self._pending) >= self.max_entries:
                    oldest = self._pending.pop(0)
                    self._settled.add(oldest)
                    self._data.pop(oldest, None)
                    self._write_frame(self.DROP, oldest)
                    self._stale += 1
                    self._discarded += 1
            payload = pickle.dumps((sql, params), self.PICKLE_PROTOCOL)
            self._write_frame(self.PAYLOAD, rid, payload)
            self._data[rid] = (sql, params)
            self._pending.append(rid)
            self._maybe_checkpoint()
            return rid

    def ack(self, rid):
        """Record that the given request committed successfully."""
        with self._lock:
            if rid in self._settled:
                return
            self._write_frame(self.ACK, rid)
            self._settled.add(rid)
            self._data.pop(rid, None)
            try:
                self._pending.remove(rid)
            except ValueError:
                pass
            self._stale += 1
            self._maybe_checkpoint()

    def pending_entries(self):
        """Return a list of ``(request_id, sql, params)`` pending, in order."""
        with self._lock:
            return [(rid,) + self._data[rid] for rid in self._pending]

    @property
    def discarded(self):
        """Cumulative number of dropped requests (across restarts)."""
        return self._discarded

    def close(self):
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.flush()
                except OSError:
                    pass
                self._fh = None

    # -- internals --------------------------------------------------------

    def _parse(self, data):
        """Parse frames, returning ``(valid_end, [(type, id, payload)])``."""
        entries = []
        offset = 0
        if data.startswith(self.PREAMBLE):
            offset = len(self.PREAMBLE)
        header_size = 1 + 2 + 4
        while len(data) - offset >= header_size:
            frame_type = data[offset]
            id_len = int.from_bytes(data[offset + 1:offset + 3], 'big')
            payload_len = int.from_bytes(
                data[offset + 3:offset + 7], 'big')
            end = offset + header_size + id_len + payload_len
            if end > len(data):
                break
            if frame_type not in (self.PAYLOAD, self.ACK, self.DROP):
                break
            try:
                rid = data[offset + 7:offset + 7 + id_len].decode('utf-8')
            except UnicodeDecodeError:
                break
            payload = data[offset + 7 + id_len:end]
            entries.append((frame_type, rid, payload))
            offset = end
        return offset, entries

    def _write_frame(self, frame_type, rid, payload=b''):
        rid_bytes = rid.encode('utf-8')
        frame = (
            bytes((frame_type,)) +
            len(rid_bytes).to_bytes(2, 'big') +
            len(payload).to_bytes(4, 'big') +
            rid_bytes +
            payload)
        self._write_bytes(frame)

    def _write_bytes(self, data, sync=True):
        self._fh.write(data)
        self._fh.flush()
        if sync:
            self._fsync(self._fh)

    def _fsync(self, fh):
        try:
            os.fsync(fh.fileno())
        except OSError:
            # fsync may be unsupported (e.g. some Windows/TempFS setups).
            pass

    def _fsync_dir(self):
        try:
            flags = os.O_RDONLY
            if hasattr(os, 'O_DIRECTORY'):
                flags |= os.O_DIRECTORY
            dir_fd = os.open(self.directory, flags)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def _read_meta(self):
        try:
            with open(self._meta_path, 'r') as fh:
                meta = json.load(fh)
            return int(meta.get('discarded', 0))
        except (OSError, ValueError):
            return 0

    def _write_meta(self):
        meta = {'discarded': self._discarded}
        tmp = self._meta_path + '.tmp'
        try:
            with open(tmp, 'w') as fh:
                json.dump(meta, fh)
            os.replace(tmp, self._meta_path)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def _maybe_checkpoint(self):
        if self._stale < self.CHECKPOINT_STALE_FRAMES:
            return
        pending = list(self._pending)
        tmp = self._path + '.tmp'
        try:
            with open(tmp, 'wb') as fh:
                fh.write(self.PREAMBLE)
                for rid in pending:
                    payload = pickle.dumps(self._data[rid],
                                           self.PICKLE_PROTOCOL)
                    header = (
                        bytes((self.PAYLOAD,)) +
                        len(rid.encode('utf-8')).to_bytes(2, 'big') +
                        len(payload).to_bytes(4, 'big') +
                        rid.encode('utf-8'))
                    fh.write(header)
                    fh.write(payload)
                fh.flush()
                self._fsync(fh)
        except OSError:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return
        try:
            os.replace(tmp, self._path)
        except OSError:
            return
        self._fsync_dir()
        try:
            if self._fh is not None:
                self._fh.close()
        except OSError:
            pass
        try:
            self._fh = open(self._path, 'ab')
        except OSError:
            self._fh = None
        self._write_meta()
        self._stale = 0


class Writer(object):
    __slots__ = ('database', 'queue', 'spool', 'empty')

    def __init__(self, database, queue, spool=None):
        self.database = database
        self.queue = queue
        self.spool = spool
        self.empty = database._thread_helper.empty_cls

    def run(self):
        conn = self.database.connection()
        try:
            if self.spool is not None:
                self.database._execute(ACK_CREATE_SQL)
            while True:
                try:
                    if conn is None:  # Paused.
                        held = []
                        if self.wait_unpause(held):
                            conn = self.database.connection()
                            # Flush writes that arrived while paused, in the
                            # order they arrived, before new queue work.
                            for obj in held:
                                self.execute(obj)
                    else:
                        conn = self.loop(conn)
                except ShutdownException:
                    logger.info('writer received shutdown request, exiting.')
                    return
        finally:
            if conn is not None:
                self.database._close(conn)
                self.database._state.reset()

    def wait_unpause(self, held):
        while True:
            op, obj = self.queue.get()
            if op is UNPAUSE:
                logger.info('writer unpaused - reconnecting to database.')
                obj.set()
                return True
            elif op is SHUTDOWN:
                raise ShutdownException()
            elif op is PAUSE:
                logger.error('writer received pause, but is already paused.')
                obj.set()
            elif op is READY:
                obj.set()
            elif self.spool is not None:
                # Writes during a pause are staged and replayed in order once
                # the writer resumes.
                held.append(obj)
            else:
                obj.set_result(None, WriterPaused())
                logger.warning('writer paused, not handling %s', obj)

    def loop(self, conn):
        op, obj = self.queue.get()
        if op is QUERY:
            self.execute(obj)
        elif op is PAUSE:
            logger.info('writer paused - closing database connection.')
            self.database._close(conn)
            self.database._state.reset()
            obj.set()
            return
        elif op is UNPAUSE:
            logger.error('writer received unpause, but is already running.')
            obj.set()
        elif op is READY:
            obj.set()
        elif op is SHUTDOWN:
            if self.spool is None:
                raise ShutdownException()
            # Stop accepting new work is handled by the caller; finish all
            # writes already queued, then exit.
            self.drain(conn)
        else:
            logger.error('writer received unsupported object: %s', obj)
        return conn

    def drain(self, conn):
        while True:
            try:
                op, obj = self.queue.get_nowait()
            except self.empty:
                raise ShutdownException()
            if op is QUERY:
                self.execute(obj)
            elif op is PAUSE or op is UNPAUSE or op is READY:
                obj.set()
            elif op is SHUTDOWN:
                continue
            else:
                logger.error('writer received unsupported object: %s', obj)

    def execute(self, obj):
        logger.debug('received query %s', obj.sql)
        rid = getattr(obj, 'request_id', None)
        if self.spool is None or rid is None:
            try:
                cursor = self.database._execute(obj.sql, obj.params)
            except Exception as execute_err:
                cursor = None
                exc = execute_err  # python3 is so fucking lame.
            else:
                exc = None
            return obj.set_result(cursor, exc)

        if self._is_committed(rid):
            logger.debug('request %s already committed, skipping.', rid)
            return obj.set_skipped()

        cursor = None
        exc = None
        try:
            self.database._execute('BEGIN IMMEDIATE')
        except Exception as begin_err:
            return obj.set_result(None, begin_err)
        try:
            cursor = self.database._execute(obj.sql, obj.params)
        except Exception as execute_err:
            exc = execute_err
        else:
            try:
                self.database._execute(ACK_INSERT_SQL, (rid,))
            except Exception as ack_err:
                exc = ack_err
        if exc is not None:
            try:
                self.database._execute('ROLLBACK')
            except Exception:
                logger.exception('could not rollback failed write.')
            return obj.set_result(None, exc)
        try:
            self.database._execute('COMMIT')
        except Exception as commit_err:
            try:
                self.database._execute('ROLLBACK')
            except Exception:
                logger.exception('could not rollback failed commit.')
            return obj.set_result(None, commit_err)

        # Commit succeeded: acknowledge the spool entry before publishing the
        # result. A crash between commit and this acknowledgement cannot
        # cause a double-execution, because the request id is already in the
        # durable ACK table.
        self.spool.ack(rid)
        return obj.set_result(cursor, exc)

    def _is_committed(self, rid):
        cursor = self.database._execute(ACK_LOOKUP_SQL, (rid,))
        return cursor.fetchone() is not None


class SqliteQueueDatabase(SqliteDatabase):
    WAL_MODE_ERROR_MESSAGE = ('SQLite must be configured to use the WAL '
                              'journal mode when using this feature. WAL mode '
                              'allows one or more readers to continue reading '
                              'while another connection writes to the '
                              'database.')

    def __init__(self, database, use_gevent=False, autostart=True,
                 queue_max_size=None, results_timeout=None,
                 persistent=False, spool_dir=None, spool_max_size=None,
                 *args, **kwargs):
        kwargs['check_same_thread'] = False

        # Lock around starting and stopping write thread operations.
        self._qlock = Lock()

        # Ensure that journal_mode is WAL. This value is passed to the parent
        # class constructor below.
        pragmas = self._validate_journal_mode(kwargs.pop('pragmas', None))

        # Reference to execute_sql on the parent class. Since we've overridden
        # execute_sql(), this is just a handy way to reference the real
        # implementation.
        Parent = super(SqliteQueueDatabase, self)
        self._execute = Parent.execute_sql

        # Call the parent class constructor with our modified pragmas.
        Parent.__init__(database, pragmas=pragmas, *args, **kwargs)

        self._autostart = autostart
        self._results_timeout = results_timeout
        self._is_stopped = True
        self._closing = False
        self._spool_max_size = spool_max_size
        self._spool_warned = False

        # Get different objects depending on the threading implementation.
        self._thread_helper = self.get_thread_impl(use_gevent)(queue_max_size)

        # Gate opened only after the backlog from a previous run has been
        # replayed, so new writes cannot overtake the replay.
        self._ready = self._thread_helper.event()

        # Create the writer thread, optionally starting it.
        self._create_write_queue()
        self._writer = None

        self._spool = None
        if persistent:
            self._init_spool(database, spool_dir)

        if self._autostart:
            self.start()

    def get_thread_impl(self, use_gevent):
        return GreenletHelper if use_gevent else ThreadHelper

    def _validate_journal_mode(self, pragmas=None):
        if not pragmas:
            return {'journal_mode': 'wal'}

        if not isinstance(pragmas, dict):
            pragmas = dict((k.lower(), v) for (k, v) in pragmas)
        if pragmas.get('journal_mode', 'wal').lower() != 'wal':
            raise ValueError(self.WAL_MODE_ERROR_MESSAGE)

        pragmas['journal_mode'] = 'wal'
        return pragmas

    def _create_write_queue(self):
        self._write_queue = self._thread_helper.queue()

    def _init_spool(self, database, spool_dir):
        if spool_dir is None:
            if not database or database == ':memory:':
                spool_dir = tempfile.mkdtemp(prefix='peewee-spool-')
            else:
                spool_dir = '%s.spool' % database
        spool = Spool(spool_dir, self._spool_max_size)
        if spool.open():
            self._spool = spool
        else:
            # Warning is emitted only here; subsequent behavior is the same
            # as the default in-memory queue.
            logger.warning('spool directory %s is not writable, falling back '
                           'to in-memory queue behavior.', spool_dir)
            self._spool = None

    def queue_size(self):
        return self._write_queue.qsize()

    def spool_discarded(self):
        """Return the cumulative number of requests dropped by the spool."""
        if self._spool is None:
            return 0
        return self._spool.discarded

    def execute_sql(self, sql, params=None, timeout=None):
        if sql.lower().startswith('select'):
            return self._execute(sql, params)

        spool = self._spool
        if spool is not None and self._closing:
            raise ShutdownException(
                'database is shutting down, not accepting new writes.')

        if spool is None:
            cursor = AsyncCursor(
                event=self._thread_helper.event(),
                sql=sql,
                params=params,
                timeout=self._results_timeout if timeout is None else timeout)
            self._write_queue.put((QUERY, cursor))
            return cursor

        if not self._is_stopped:
            # Writer is starting: wait until the previous backlog has been
            # replayed before accepting this write.
            wait_timeout = self._results_timeout if timeout is None else timeout
            if not self._ready.wait(timeout=wait_timeout) and \
                    wait_timeout is not None:
                raise ResultTimeout('spool replay not finished, timed out.')

        try:
            request_id = spool.append(sql, params)
        except Exception:
            # The spool must never break the business call path: warn the
            # first time, then degrade silently to the in-memory behavior.
            if not self._spool_warned:
                logger.warning('spool write failed, falling back to in-memory '
                               'queue behavior.', exc_info=False)
                self._spool_warned = True
            request_id = None

        cursor = AsyncCursor(
            event=self._thread_helper.event(),
            sql=sql,
            params=params,
            timeout=self._results_timeout if timeout is None else timeout,
            request_id=request_id)
        self._write_queue.put((QUERY, cursor))
        return cursor

    def start(self):
        with self._qlock:
            if not self._is_stopped:
                return False

            # A writer that survived a previous shutdown timeout must exit
            # before a new one takes over.
            prior = self._writer
            if prior is not None and \
                    self._thread_helper.is_alive(prior):
                self._thread_helper.join(prior)

            ready = None
            if self._spool is not None:
                # Re-enqueue the backlog ahead of the READY gate so the
                # writer drains it before new submissions are accepted.
                ready = self._ready = self._thread_helper.event()
                for request_id, sql, params in self._spool.pending_entries():
                    job = ReplayJob(request_id, sql, params, self._spool)
                    self._write_queue.put((QUERY, job))
                self._write_queue.put((READY, ready))

            def run():
                writer = Writer(self, self._write_queue, self._spool)
                writer.run()

            self._writer = self._thread_helper.thread(run)
            self._writer.start()
            self._is_stopped = False

        # Block callers of start() until the replay is complete, so no new
        # writes can be accepted before the backlog is flushed.
        if ready is not None:
            ready.wait()

        return True

    def stop(self, timeout=None):
        logger.debug('environment stop requested.')
        with self._qlock:
            if self._is_stopped:
                return False

            # 1. Stop accepting new requests first.
            self._closing = True
            if self._spool is not None:
                self._ready.clear()

            self._write_queue.put((SHUTDOWN, None))
            writer = self._writer
            self._is_stopped = True

        # 2. Wait for in-flight / queued writes to finish.
        stopped = self._thread_helper.join(writer, timeout)

        if not stopped:
            # 3. Timed out: leave the remaining entries staged for the next
            # startup, which will replay them.
            self._closing = False
            logger.warning('shutdown timed out after %s seconds, %s pending '
                           'write(s) deferred to next startup.',
                           timeout, self._write_queue.qsize())
            return False

        # Writer has exited: handle anything left in the queue.
        while not self._write_queue.empty():
            op, obj = self._write_queue.get()
            if op is PAUSE or op is UNPAUSE or op is READY:
                obj.set()
            elif op is QUERY:
                rid = getattr(obj, 'request_id', None)
                if self._spool is not None and rid is not None:
                    # Stay un-ready; the entry remains durable in the spool
                    # and will be replayed on next start.
                    continue
                obj.set_result(None, ShutdownException())

        self._closing = False
        return True

    def is_stopped(self):
        with self._qlock:
            return self._is_stopped

    def pause(self):
        with self._qlock:
            if self._is_stopped:
                return False

            evt = self._thread_helper.event()
            self._write_queue.put((PAUSE, evt))

        evt.wait()

    def unpause(self):
        with self._qlock:
            if self._is_stopped:
                return False

            evt = self._thread_helper.event()
            self._write_queue.put((UNPAUSE, evt))

        evt.wait()

    def __unsupported__(self, *args, **kwargs):
        raise ValueError('This method is not supported by %r.' % type(self))
    atomic = transaction = savepoint = __unsupported__


class ThreadHelper(object):
    __slots__ = ('queue_max_size',)

    empty_cls = Empty

    def __init__(self, queue_max_size=None):
        self.queue_max_size = queue_max_size

    def event(self): return Event()

    def queue(self, max_size=None):
        max_size = max_size if max_size is not None else self.queue_max_size
        return Queue(maxsize=max_size or 0)

    def thread(self, fn, *args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.daemon = True
        return thread

    def join(self, thread, timeout=None):
        """Join, returning ``True`` if the thread has finished."""
        thread.join(timeout)
        return not thread.is_alive()

    def is_alive(self, thread):
        return thread.is_alive()


class GreenletHelper(ThreadHelper):
    __slots__ = ()

    empty_cls = GEmpty if GEmpty is not None else Empty

    def event(self): return GEvent()

    def queue(self, max_size=None):
        max_size = max_size if max_size is not None else self.queue_max_size
        return GQueue(maxsize=max_size or 0)

    def thread(self, fn, *args, **kwargs):
        def wrap(*a, **k):
            gevent.sleep()
            return fn(*a, **k)
        return GThread(wrap, *args, **kwargs)

    def join(self, thread, timeout=None):
        thread.join(timeout=timeout)
        return thread.ready()

    def is_alive(self, thread):
        return not thread.ready()
