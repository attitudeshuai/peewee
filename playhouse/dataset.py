import base64
import csv
import datetime
import hashlib
import io
import json
import operator
import os
import sys
import tempfile
import time
import uuid
from decimal import Decimal
from functools import reduce
from urllib.parse import urlparse

from peewee import *
from peewee import _StringField
from playhouse.db_url import connect
from playhouse.migrate import migrate
from playhouse.migrate import SchemaMigrator
from playhouse.reflection import Introspector


STATE_VERSION = 1
EXPORT_STATE_SUFFIX = '.freeze.state'
IMPORT_STATE_SUFFIX = '.thaw.state'
LOCK_TABLE = '__dataset_transfer_lock'
CHUNK_TABLE = '__dataset_transfer_chunk'


class DataSetError(Exception):
    pass


class TransferConflict(DataSetError):
    pass


class TransferSizeLimit(DataSetError):
    pass


class TransferIntegrityError(DataSetError):
    pass


class TransferDegraded(DataSetError):
    def __init__(self, message, committed_lines=None, **info):
        super(TransferDegraded, self).__init__(message)
        self.committed_lines = list(committed_lines or [])
        self.info = info


class ImportRowError(DataSetError):
    def __init__(self, line_number, row_number, column, value, cause=None):
        self.line_number = line_number
        self.row_number = row_number
        self.column = column
        self.value = value
        self.cause = cause
        message = 'Invalid value in row %s (line %s), column "%s": %r' % (
            row_number, line_number, column, value)
        if cause is not None:
            message = '%s (%s: %s)' % (
                message, type(cause).__name__, cause)
        super(ImportRowError, self).__init__(message)


class TransferResult(object):
    def __init__(self, **kwargs):
        self.info = kwargs

    def __getattr__(self, name):
        try:
            return self.info[name]
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return '<%s: %s>' % (type(self).__name__, self.info)


class ExportResult(TransferResult, int):
    def __new__(cls, rows_exported=0, **kwargs):
        return int.__new__(cls, rows_exported)

    def __init__(self, rows_exported=0, **kwargs):
        TransferResult.__init__(self, rows_exported=rows_exported, **kwargs)


class ImportResult(TransferResult, int):
    def __new__(cls, rows_inserted=0, **kwargs):
        return int.__new__(cls, rows_inserted)

    def __init__(self, rows_inserted=0, **kwargs):
        TransferResult.__init__(self, rows_inserted=rows_inserted, **kwargs)


class TransferStateStore(object):
    def __init__(self, path=None, file_obj=None):
        if not path and file_obj is None:
            raise ValueError('A state path or file-like object is required.')
        self.path = path
        self.file_obj = file_obj

    def load(self):
        if self.path:
            if not os.path.exists(self.path):
                return None
            with open(self.path, 'r', encoding='utf8') as fh:
                return self._load(fh)
        self.file_obj.seek(0)
        return self._load(self.file_obj)

    def _load(self, fh):
        content = fh.read()
        if not content:
            return None
        return json.loads(content)

    def save(self, state):
        if self.path:
            directory = os.path.dirname(self.path) or '.'
            fd, tmp_path = tempfile.mkstemp(
                prefix='.dataset-state-', dir=directory)
            try:
                with os.fdopen(fd, 'w', encoding='utf8') as fh:
                    json.dump(state, fh, indent=2, sort_keys=True)
                    fh.flush()
                    os.fsync(fh.fileno())
                os.replace(tmp_path, self.path)
            except Exception:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        else:
            fh = self.file_obj
            fh.seek(0)
            fh.truncate()
            json.dump(state, fh, indent=2, sort_keys=True)
            fh.flush()


class _HashTextWriter(object):
    def __init__(self, raw, binary=False):
        self.raw = raw
        self.binary = binary
        self.position = 0
        self.hash = hashlib.sha256()

    def write(self, value):
        data = value.encode('utf8') if isinstance(value, str) else value
        self.hash.update(data)
        self.position += len(data)
        if self.binary:
            return self.raw.write(data)
        return self.raw.write(value)

    def flush(self):
        self.raw.flush()

    def tell(self):
        return self.position

    def seek(self, offset):
        self.raw.seek(offset)
        self.position = offset

    def truncate(self):
        self.raw.truncate()

    def read(self, size=-1):
        return self.raw.read(size)

    def seek_to_start(self):
        self.seek(0)


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


def canonical_checksum(rows):
    content = json.dumps(
        list(rows), sort_keys=True, separators=(',', ':'), default=str)
    return hashlib.sha256(content.encode('utf8')).hexdigest()


def new_transfer_state(kind, format, resource, options, columns=None):
    return {
        'version': STATE_VERSION,
        'kind': kind,
        'format': format,
        'resource': resource,
        'transfer_id': str(uuid.uuid4()),
        'options': options,
        'columns': list(columns or []),
        'chunks': [],
        'complete': False,
        'total_rows': 0,
        'file_position': 0,
        'file_sha256': None,
    }


class _FileLock(object):
    def __init__(self, path, timeout=0):
        self.path = path
        self.timeout = timeout
        self.fh = None

    def __enter__(self):
        directory = os.path.dirname(self.path) or '.'
        if not os.path.exists(directory):
            os.makedirs(directory)
        self.fh = open(self.path, 'a+b')
        deadline = time.time() + self.timeout
        while True:
            try:
                self._lock()
                return self
            except OSError:
                if time.time() >= deadline:
                    self.fh.close()
                    self.fh = None
                    raise TransferConflict('Transfer state file is locked.')
                time.sleep(0.05)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.fh is not None:
            try:
                self._unlock()
            finally:
                self.fh.close()

    def _lock(self):
        self.fh.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(self.fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(self):
        self.fh.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(self.fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.fh.fileno(), fcntl.LOCK_UN)


class DataSet(object):
    def __init__(self, url, include_views=False, **kwargs):
        if isinstance(url, Database):
            self._database = url
            self._database_path = self._database.database
        else:
            parse_result = urlparse(url)
            self._database_path = parse_result.path[1:]

            # Connect to the database.
            self._database = connect(url)

        # Open a connection if one does not already exist.
        self._database.connect(reuse_if_open=True)

        # Introspect the database and generate models.
        self._introspector = Introspector.from_database(self._database)
        self._include_views = include_views
        self._model_kwargs = kwargs
        self._models = self._introspector.generate_models(
            skip_invalid=True,
            literal_column_names=True,
            include_views=self._include_views,
            **self._model_kwargs)
        self._migrator = SchemaMigrator.from_database(self._database)

        class BaseModel(Model):
            class Meta:
                database = self._database
        self._base_model = BaseModel
        self._transfer_models = None
        self._transfer_locks = {}
        self._export_formats = self.get_export_formats()
        self._import_formats = self.get_import_formats()

    def __repr__(self):
        return '<DataSet: %s>' % self._database_path

    def get_export_formats(self):
        return {
            'csv': CSVExporter,
            'json': JSONExporter,
            'tsv': TSVExporter}

    def get_import_formats(self):
        return {
            'csv': CSVImporter,
            'json': JSONImporter,
            'tsv': TSVImporter}

    def __getitem__(self, table):
        if table not in self._models and table in self.tables:
            self.update_cache(table)
        return Table(self, table, self._models.get(table))

    @property
    def tables(self):
        tables = self._database.get_tables()
        if self._include_views:
            tables += self.views
        return tables

    @property
    def views(self):
        return [v.name for v in self._database.get_views()]

    def __contains__(self, table):
        return table in self.tables

    def connect(self, reuse_if_open=False):
        self._database.connect(reuse_if_open=reuse_if_open)

    def close(self):
        self._database.close()

    def update_cache(self, table=None):
        if table:
            dependencies = [table]
            if table in self._models:
                model_class = self._models[table]
                dependencies.extend([
                    related._meta.table_name for _, related, _ in
                    model_class._meta.model_graph()])
            else:
                dependencies.extend(self.get_table_dependencies(table))
        else:
            dependencies = None  # Update all tables.
            self._models = {}
        updated = self._introspector.generate_models(
            skip_invalid=True,
            table_names=dependencies,
            literal_column_names=True,
            include_views=self._include_views,
            **self._model_kwargs)
        self._models.update(updated)

    def get_table_dependencies(self, table):
        stack = [table]
        accum = []
        seen = set()
        while stack:
            table = stack.pop()
            for fk_meta in self._database.get_foreign_keys(table):
                dest = fk_meta.dest_table
                if dest not in seen:
                    seen.add(dest)
                    stack.append(dest)
                    accum.append(dest)
        return accum

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._database.is_closed():
            self.close()

    def query(self, sql, params=None):
        return self._database.execute_sql(sql, params)

    def transaction(self):
        return self._database.atomic()

    def _ensure_transfer_models(self):
        if self._transfer_models is not None:
            return self._transfer_models

        class TransferLock(self._base_model):
            resource = TextField(primary_key=True)
            operation = TextField()
            transfer_id = TextField()
            heartbeat = TextField()

            class Meta:
                table_name = LOCK_TABLE

        class TransferChunk(self._base_model):
            transfer_id = TextField()
            chunk_index = IntegerField()
            operation = TextField()
            resource = TextField()
            start_row = IntegerField()
            end_row = IntegerField()
            line_start = IntegerField()
            line_end = IntegerField()
            checksum = TextField()

            class Meta:
                table_name = CHUNK_TABLE
                primary_key = CompositeKey('transfer_id', 'chunk_index')

        TransferLock.create_table()
        TransferChunk.create_table()
        self._transfer_models = (TransferLock, TransferChunk)
        return self._transfer_models

    def _transfer_lock(self, resource, operation, transfer_id,
                       enabled=True, timeout=0, ttl=60):
        if not enabled or resource is None:
            class _NoLock(object):
                def pulse(self):
                    pass

                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    pass
            return _NoLock()

        active = self._transfer_locks.get(resource)
        if active is not None:
            if active[0] != transfer_id:
                raise TransferConflict(
                    'Transfer for "%s" is already active.' % resource)
            active[1] += 1

            class _NestedLock(object):
                def __init__(self, outer):
                    self.outer = outer

                def pulse(self):
                    pass

                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    self.outer._release_lock(resource)
            return _NestedLock(self)

        deadline = time.time() + timeout
        while True:
            try:
                TransferLock, _ = self._ensure_transfer_models()
                cutoff = utc_now() - datetime.timedelta(seconds=ttl)
                TransferLock.delete().where(
                    TransferLock.heartbeat < cutoff.isoformat()).execute()
                TransferLock.create(
                    resource=resource,
                    operation=operation,
                    transfer_id=transfer_id,
                    heartbeat=utc_now().isoformat())
                self._transfer_locks[resource] = [transfer_id, 1]
                break
            except IntegrityError:
                if time.time() >= deadline:
                    raise TransferConflict(
                        'Transfer for "%s" is already active.' % resource)
                time.sleep(0.05)
            except OperationalError:
                if time.time() >= deadline:
                    raise TransferConflict(
                        'Unable to acquire transfer lock for "%s".' % resource)
                time.sleep(0.05)

        outer = self

        class _Lock(object):
            def pulse(self):
                TransferLock.update(
                    heartbeat=utc_now().isoformat()).where(
                    TransferLock.resource == resource,
                    TransferLock.transfer_id == transfer_id).execute()

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                outer._release_lock(resource)
        return _Lock()

    def _release_lock(self, resource):
        active = self._transfer_locks.get(resource)
        if active is None:
            return
        active[1] -= 1
        if active[1] > 0:
            return
        TransferLock, _ = self._ensure_transfer_models()
        TransferLock.delete().where(
            TransferLock.resource == resource,
            TransferLock.transfer_id == active[0]).execute()
        self._transfer_locks.pop(resource, None)

    def _completed_chunks(self, transfer_id):
        _, TransferChunk = self._ensure_transfer_models()
        return {
            chunk.chunk_index: chunk.checksum for chunk in
            TransferChunk.select().where(
                TransferChunk.transfer_id == transfer_id)}

    def _record_chunk(self, transfer_id, chunk, operation, resource):
        _, TransferChunk = self._ensure_transfer_models()
        TransferChunk.create(
            transfer_id=transfer_id,
            chunk_index=chunk['index'],
            operation=operation,
            resource=resource,
            start_row=chunk['start_row'],
            end_row=chunk['end_row'],
            line_start=chunk.get('line_start'),
            line_end=chunk.get('line_end'),
            checksum=chunk['checksum'])

    def _check_arguments(self, filename, file_obj, format, format_dict):
        if filename and file_obj:
            raise ValueError('file is over-specified. Please use either '
                             'filename or file_obj, but not both.')
        if not filename and not file_obj:
            raise ValueError('A filename or file-like object must be '
                             'specified.')
        if format not in format_dict:
            valid_formats = ', '.join(sorted(format_dict.keys()))
            raise ValueError('Unsupported format "%s". Use one of %s.' % (
                format, valid_formats))

    def _state_store(self, filename, state_filename, state_file, suffix):
        if state_file is not None or state_filename is not None:
            return TransferStateStore(state_filename, state_file)
        if filename is not None:
            return TransferStateStore(filename + suffix)
        return TransferStateStore(file_obj=io.StringIO())

    def _query_resource(self, query):
        model = getattr(query, 'model_class', None)
        if model is not None:
            return model._meta.table_name
        return None

    def _fingerprint_file(self, file_obj):
        if not file_obj.seekable():
            return None
        file_obj.seek(0)
        digest = hashlib.sha256()
        while True:
            chunk = file_obj.read(65536)
            if not chunk:
                break
            if isinstance(chunk, str):
                chunk = chunk.encode('utf8')
            digest.update(chunk)
        file_obj.seek(0)
        return digest.hexdigest()

    def freeze(self, query, format='csv', filename=None, file_obj=None,
               encoding='utf8', iso8601_datetimes=False, base64_bytes=False,
               chunksize=None, max_rows=None, resume=True,
               state_filename=None, state_file=None, lock=True,
               lock_timeout=0, lock_ttl=60, **kwargs):
        self._check_arguments(filename, file_obj, format, self._export_formats)
        stateful = (chunksize is not None or state_file is not None or
                    state_filename is not None)
        if not stateful:
            if filename:
                file_obj = open(filename, 'w', encoding=encoding)
            try:
                exporter = self._export_formats[format](
                    query,
                    iso8601_datetimes=iso8601_datetimes,
                    base64_bytes=base64_bytes)
                exporter.export(file_obj, **kwargs)
            finally:
                if filename:
                    file_obj.close()
            return

        if chunksize is None:
            chunksize = 1000
        if int(chunksize) <= 0:
            raise ValueError('chunksize must be greater than zero.')
        if file_obj is not None and state_file is None and state_filename is None:
            raise ValueError(
                'state_file or state_filename is required for resumable '
                'export from a file-like object.')
        store = self._state_store(
            filename, state_filename, state_file, EXPORT_STATE_SUFFIX)
        state = store.load()
        options = {
            'chunksize': int(chunksize),
            'max_rows': max_rows,
            'format': format,
            'encoding': encoding,
            'iso8601_datetimes': iso8601_datetimes,
            'base64_bytes': base64_bytes,
            'writer_options': kwargs,
        }
        resource = self._query_resource(query)
        if state is None or not resume:
            state = new_transfer_state(
                'export', format, resource, options=options)
        self._validate_transfer_state(state, 'export', format, resource)
        if (state['chunks'] and not state['complete'] and
                state.get('options') != options):
            raise TransferConflict(
                'Cannot resume an export with different options; the '
                'original options were %r.' % state['options'])

        file_lock = (_FileLock(store.path + '.lock', lock_timeout)
                     if store.path else None)
        try:
            if file_lock is not None:
                file_lock.__enter__()
            binary_data = False
            if filename:
                mode = 'r+b' if os.path.exists(filename) else 'w+b'
                raw = open(filename, mode)
                file_obj = io.TextIOWrapper(
                    raw, encoding=encoding, newline='')
                binary_data = True
            elif file_obj.seekable():
                file_obj.seek(0)

            db_lock = self._transfer_lock(
                resource, 'export', state['transfer_id'], lock,
                lock_timeout, lock_ttl)
            with db_lock:
                if not state['chunks']:
                    file_obj.seek(0)
                    file_obj.truncate()
                exporter = self._export_formats[format](
                    query,
                    iso8601_datetimes=iso8601_datetimes,
                    base64_bytes=base64_bytes)
                result = exporter.export_stateful(
                    file_obj, store, state, int(chunksize), max_rows,
                    resume, binary_data, db_lock.pulse, **kwargs)
        finally:
            if file_lock is not None:
                file_lock.__exit__(None, None, None)
            if filename:
                file_obj.close()
        return result

    def thaw(self, table, format='csv', filename=None, file_obj=None,
             strict=False, encoding='utf8', iso8601_datetimes=False,
             base64_bytes=False, chunksize=None, max_rows=None, resume=True,
             state_filename=None, state_file=None, lock=True,
             lock_timeout=0, lock_ttl=60, add_columns=None, on_unknown=None,
             on_missing='default', on_type_error='raise',
             empty_as_null=False, use_transaction=True, use_batch=True,
             **kwargs):
        self._check_arguments(filename, file_obj, format, self._import_formats)
        stateful = (chunksize is not None or state_file is not None or
                    state_filename is not None)
        if not stateful:
            if filename:
                file_obj = open(filename, 'r', encoding=encoding)
            try:
                importer = self._import_formats[format](
                    self[table],
                    strict=strict,
                    iso8601_datetimes=iso8601_datetimes,
                    base64_bytes=base64_bytes)
                count = importer.load(file_obj, **kwargs)
            finally:
                if filename:
                    file_obj.close()
            return count

        if chunksize is None:
            chunksize = 1000
        if int(chunksize) <= 0:
            raise ValueError('chunksize must be greater than zero.')
        if on_unknown is None:
            if add_columns is not None:
                on_unknown = 'add' if add_columns else 'ignore'
            else:
                on_unknown = 'ignore' if strict else 'add'
        if on_unknown not in ('add', 'ignore', 'error'):
            raise ValueError('on_unknown must be add, ignore or error.')
        if on_missing not in ('default', 'null', 'error'):
            raise ValueError('on_missing must be default, null or error.')
        if on_type_error not in ('raise', 'null', 'skip'):
            raise ValueError(
                'on_type_error must be raise, null or skip.')
        if file_obj is not None and state_file is None and state_filename is None:
            raise ValueError(
                'state_file or state_filename is required for resumable '
                'import from a file-like object.')

        store = self._state_store(
            filename, state_filename, state_file, IMPORT_STATE_SUFFIX)
        state = store.load()
        existing_state = state is not None
        options = {
            'chunksize': int(chunksize),
            'max_rows': max_rows,
            'format': format,
            'encoding': encoding,
            'strict': strict,
            'iso8601_datetimes': iso8601_datetimes,
            'base64_bytes': base64_bytes,
            'reader_options': kwargs,
            'on_unknown': on_unknown,
            'on_missing': on_missing,
            'on_type_error': on_type_error,
            'empty_as_null': empty_as_null,
        }
        table_obj = self[table]
        resource = table_obj.name
        if state is None or not resume:
            state = new_transfer_state(
                'import', format, resource, options=options)
        self._validate_transfer_state(state, 'import', format, resource)
        if (state['chunks'] and not state['complete'] and
                state.get('options') != options):
            raise TransferConflict(
                'Cannot resume an import with different options; the '
                'original options were %r.' % state['options'])

        file_lock = (_FileLock(store.path + '.lock', lock_timeout)
                     if store.path else None)
        if file_lock is not None:
            file_lock.__enter__()
        binary_data = False
        if filename:
            raw = open(filename, 'rb')
            file_obj = io.TextIOWrapper(raw, encoding=encoding, newline='')
            binary_data = True
        elif file_obj.seekable():
            file_obj.seek(0)
        fingerprint = self._fingerprint_file(file_obj)
        if fingerprint is not None:
            state['source_sha256'] = fingerprint
            if not existing_state or not resume:
                identity = json.dumps([
                    resource, format, fingerprint, options], sort_keys=True,
                    default=str)
                state['transfer_id'] = hashlib.sha256(
                    identity.encode('utf8')).hexdigest()

        importer = self._import_formats[format](
            table_obj,
            strict=strict,
            iso8601_datetimes=iso8601_datetimes,
            base64_bytes=base64_bytes)
        try:
            db_lock = self._transfer_lock(
                resource, 'import', state['transfer_id'], lock,
                lock_timeout, lock_ttl)
            with db_lock:
                count = importer.load_stateful(
                    file_obj, store, state, int(chunksize), max_rows,
                    resume, on_unknown, on_missing, on_type_error,
                    empty_as_null, use_transaction, use_batch,
                    db_lock.pulse, **kwargs)
        finally:
            if file_lock is not None:
                file_lock.__exit__(None, None, None)
            if filename:
                file_obj.close()
        return count

    def _validate_transfer_state(self, state, kind, format, resource):
        if state.get('version') != STATE_VERSION:
            raise TransferIntegrityError('Unsupported transfer state version.')
        if state.get('kind') != kind or state.get('format') != format:
            raise TransferConflict('Transfer state does not match this job.')
        if resource is not None and state.get('resource') != resource:
            raise TransferConflict(
                'Transfer state was created for a different table.')


class Table(object):
    def __init__(self, dataset, name, model_class):
        self.dataset = dataset
        self.name = name
        if model_class is None:
            model_class = self._create_model()
            model_class.create_table()
            self.dataset._models[name] = model_class

    @property
    def model_class(self):
        return self.dataset._models[self.name]

    def __repr__(self):
        return '<Table: %s>' % self.name

    def __len__(self):
        return self.find().count()

    def __iter__(self):
        return iter(self.find().iterator())

    def _create_model(self):
        class Meta:
            table_name = self.name
        return type(
            str(self.name),
            (self.dataset._base_model,),
            {'Meta': Meta})

    def create_index(self, columns, unique=False):
        index = ModelIndex(self.model_class, columns, unique=unique)
        self.model_class.add_index(index)
        self.dataset._database.execute(index)

    def _guess_field_type(self, value):
        if isinstance(value, str):
            return TextField
        if isinstance(value, (datetime.date, datetime.datetime)):
            return DateTimeField
        elif value is True or value is False:
            return BooleanField
        elif isinstance(value, int):
            return IntegerField
        elif isinstance(value, float):
            return FloatField
        elif isinstance(value, Decimal):
            return DecimalField
        return TextField

    @property
    def columns(self):
        return [f.name for f in self.model_class._meta.sorted_fields]

    def _migrate_new_columns(self, data):
        new_keys = set(data) - set(self.model_class._meta.fields)
        new_keys -= set(self.model_class._meta.columns)
        if new_keys:
            operations = []
            for key in new_keys:
                field_class = self._guess_field_type(data[key])
                field = field_class(null=True)
                operations.append(
                    self.dataset._migrator.add_column(self.name, key, field))
                field.bind(self.model_class, key)

            migrate(*operations)

            self.dataset.update_cache(self.name)

    def ensure_columns(self, rows):
        samples = {}
        for row in rows:
            for key, value in row.items():
                if key not in samples:
                    samples[key] = value
                elif isinstance(value, str):
                    samples[key] = ''
                elif value is not None and samples[key] is None:
                    samples[key] = value
                elif (isinstance(value, float) and
                      isinstance(samples[key], int) and
                      not isinstance(samples[key], bool)):
                    samples[key] = value
        new_keys = set(samples) - set(self.model_class._meta.fields)
        new_keys -= set(self.model_class._meta.columns)
        if not new_keys:
            return []
        operations = []
        added = []
        for key in sorted(new_keys):
            field_class = self._guess_field_type(samples[key])
            field = field_class(null=True)
            operations.append(
                self.dataset._migrator.add_column(self.name, key, field))
            field.bind(self.model_class, key)
            added.append(key)
        migrate(*operations)
        self.dataset.update_cache(self.name)
        return added

    def __getitem__(self, item):
        try:
            return self.model_class[item]
        except self.model_class.DoesNotExist:
            pass

    def __setitem__(self, item, value):
        if not isinstance(value, dict):
            raise ValueError('Table.__setitem__() value must be a dict')

        pk = self.model_class._meta.primary_key
        value[pk.name] = item

        try:
            with self.dataset.transaction() as txn:
                self.insert(**value)
        except IntegrityError:
            self.dataset.update_cache(self.name)
            self.update(columns=[pk.name], **value)

    def __delitem__(self, item):
        del self.model_class[item]

    def insert(self, **data):
        self._migrate_new_columns(data)
        return self.model_class.insert(**data).execute()

    def _apply_where(self, query, filters, conjunction=None):
        conjunction = conjunction or operator.and_
        if filters:
            expressions = [
                (self.model_class._meta.fields[column] == value)
                for column, value in filters.items()]
            query = query.where(reduce(conjunction, expressions))
        return query

    def update(self, columns=None, conjunction=None, **data):
        self._migrate_new_columns(data)
        filters = {}
        if columns:
            for column in columns:
                filters[column] = data.pop(column)

        return self._apply_where(
            self.model_class.update(**data),
            filters,
            conjunction).execute()

    def _query(self, **query):
        return self._apply_where(self.model_class.select(), query)

    def find(self, **query):
        return self._query(**query).dicts()

    def find_one(self, **query):
        try:
            return self.find(**query).get()
        except self.model_class.DoesNotExist:
            return None

    def all(self):
        return self.find()

    def delete(self, **query):
        return self._apply_where(self.model_class.delete(), query).execute()

    def freeze(self, *args, **kwargs):
        return self.dataset.freeze(self.all(), *args, **kwargs)

    def thaw(self, *args, **kwargs):
        return self.dataset.thaw(self.name, *args, **kwargs)


class Exporter(object):
    def __init__(self, query, iso8601_datetimes=False, base64_bytes=False):
        self.query = query
        self.iso8601_datetimes = iso8601_datetimes
        self.base64_bytes = base64_bytes

    def _export_value(self, value):
        if isinstance(value, _datetime_types):
            return value.isoformat() if self.iso8601_datetimes else str(value)
        elif isinstance(value, (Decimal, uuid.UUID)):
            return str(value)
        elif isinstance(value, bytes):
            if self.base64_bytes:
                return base64.urlsafe_b64encode(value).decode('utf8')
            return value.hex()
        return value

    def export(self, file_obj):
        raise NotImplementedError

    def _ordered_query(self):
        query = self.query
        model = getattr(query, 'model_class', None)
        if model is not None and not getattr(query, '_order_by', None):
            primary_keys = model._meta.get_primary_keys()
            if primary_keys:
                query = query.order_by(*primary_keys)
        return query

    def _fetch_chunk(self, query, offset, limit):
        chunk_query = query.limit(limit).offset(offset)
        tuples = chunk_query.tuples().execute()
        tuples.initialize()
        columns = list(getattr(tuples, 'columns', None) or [])
        rows = [tuple(self._export_value(value) for value in row)
                for row in tuples]
        return columns, rows

    def _write_export_header(self, output, columns, **kwargs):
        raise NotImplementedError

    def _write_export_rows(self, writer, columns, rows, first_row, **kwargs):
        raise NotImplementedError

    def _write_export_footer(self, output, **kwargs):
        pass

    def export_stateful(self, file_obj, store, state, chunksize, max_rows,
                        resume, binary_data=False, pulse=None, **kwargs):
        raw = file_obj.buffer if binary_data else file_obj
        output = _HashTextWriter(raw, binary=binary_data)
        columns = state.get('columns') or []
        start_row = 0
        resumed_chunks = 0
        if state['chunks']:
            start_row = state['chunks'][-1]['end_row']
            expected_position = state['file_position']
            output.seek(0)
            prefix = output.read(expected_position)
            checksum = hashlib.sha256(
                prefix.encode('utf8') if isinstance(prefix, str) else prefix
            ).hexdigest()
            if checksum != state['file_sha256']:
                raise TransferIntegrityError(
                    'Exported prefix does not match transfer state.')
            output.seek(expected_position)
            output.truncate()
            output.hash = hashlib.sha256(prefix.encode(
                'utf8') if isinstance(prefix, str) else prefix)
            output.position = expected_position
            resumed_chunks = len(state['chunks'])
            columns = state['columns']
            if state['complete']:
                return ExportResult(
                    0,
                    total_rows=state['total_rows'],
                    chunks_completed=len(state['chunks']),
                    resumed=True,
                    columns=columns,
                    complete=True,
                    checksum=state['file_sha256'])

        query = self._ordered_query()
        total_rows = query.count()
        if max_rows is not None and total_rows > max_rows:
            raise TransferSizeLimit(
                'Export contains %s rows, exceeding max_rows=%s.' % (
                    total_rows, max_rows))
        if total_rows < start_row:
            raise TransferIntegrityError(
                'Query has fewer rows than the recorded export position.')

        writer = None
        rows_written = 0
        while start_row < total_rows:
            fetched_columns, rows = self._fetch_chunk(
                query, start_row, chunksize)
            if not fetched_columns and not rows:
                break
            if not columns:
                columns = fetched_columns
                state['columns'] = columns
                self._write_export_header(output, columns, **kwargs)
            elif fetched_columns and columns != fetched_columns:
                raise TransferIntegrityError(
                    'Export columns changed during a resumed export.')
            if writer is None:
                writer = self._make_writer(output, **kwargs)
            first_row = start_row == 0
            self._write_export_rows(
                writer, columns, rows, first_row, output=output, **kwargs)
            output.flush()
            end_row = start_row + len(rows)
            chunk = {
                'index': len(state['chunks']),
                'start_row': start_row,
                'end_row': end_row,
                'row_count': len(rows),
                'checksum': canonical_checksum(
                    [dict(zip(columns, row)) for row in rows]),
                'file_position': output.tell(),
                'file_sha256': output.hash.copy().hexdigest(),
            }
            state['chunks'].append(chunk)
            state['file_position'] = chunk['file_position']
            state['file_sha256'] = chunk['file_sha256']
            rows_written += len(rows)
            start_row = end_row
            if pulse is not None:
                pulse()
            store.save(state)
            if not rows:
                break

        if not columns:
            empty_columns = list(getattr(self.query, '_select', []) or [])
            columns = [
                col.column_name if hasattr(col, 'column_name') else str(col)
                for col in empty_columns]
            state['columns'] = columns
        if not state['chunks']:
            self._write_export_header(output, columns, **kwargs)
        self._write_export_footer(output, **kwargs)
        output.flush()
        state['complete'] = True
        state['total_rows'] = total_rows
        state['file_position'] = output.tell()
        state['file_sha256'] = output.hash.copy().hexdigest()
        store.save(state)
        return ExportResult(
            rows_written,
            total_rows=total_rows,
            chunks_completed=len(state['chunks']),
            resumed=bool(resumed_chunks),
            resumed_chunks=resumed_chunks,
            columns=columns,
            complete=True,
            checksum=state['file_sha256'])

    def _make_writer(self, output, **kwargs):
        return output


_datetime_types = (datetime.datetime, datetime.date, datetime.time)


class JSONExporter(Exporter):
    def _make_default(self):
        def default(o):
            value = self._export_value(o)
            if value is o:
                raise TypeError('Unable to serialize %r as JSON' % o)
            return value
        return default

    def export(self, file_obj, **kwargs):
        json.dump(
            list(self.query),
            file_obj,
            default=self._make_default(),
            **kwargs)

    def _write_export_header(self, output, columns, **kwargs):
        output.write('[')

    def _write_export_rows(self, writer, columns, rows, first_row,
                           output=None, **kwargs):
        target = output or writer
        for offset, row in enumerate(rows):
            if not (first_row and offset == 0):
                target.write(',')
            value = dict(zip(columns, row))
            target.write(json.dumps(value, default=self._make_default()))

    def _write_export_footer(self, output, **kwargs):
        output.write(']')


class CSVExporter(Exporter):
    def export(self, file_obj, header=True, **kwargs):
        writer = csv.writer(file_obj, **kwargs)
        tuples = self.query.tuples().execute()
        tuples.initialize()
        if header and getattr(tuples, 'columns', None):
            writer.writerow([column for column in tuples.columns])
        for row in tuples:
            writer.writerow([self._export_value(value) for value in row])

    def _csv_kwargs(self, kwargs):
        return {key: value for key, value in kwargs.items()
                if key != 'header'}

    def _make_writer(self, output, **kwargs):
        return csv.writer(output, **self._csv_kwargs(kwargs))

    def _write_export_header(self, output, columns, header=True, **kwargs):
        if header:
            csv.writer(output, **self._csv_kwargs(kwargs)).writerow(columns)

    def _write_export_rows(self, writer, columns, rows, first_row,
                           output=None, **kwargs):
        for row in rows:
            writer.writerow(list(row))


class TSVExporter(CSVExporter):
    def export(self, file_obj, header=True, **kwargs):
        kwargs.setdefault('delimiter', '\t')
        return super(TSVExporter, self).export(file_obj, header, **kwargs)

    def _csv_kwargs(self, kwargs):
        csv_kwargs = super(TSVExporter, self)._csv_kwargs(kwargs)
        csv_kwargs.setdefault('delimiter', '\t')
        return csv_kwargs


class Importer(object):
    def __init__(self, table, strict=False, iso8601_datetimes=False,
                 base64_bytes=False):
        self.table = table
        self.strict = strict
        self.iso8601_datetimes = iso8601_datetimes
        self.base64_bytes = base64_bytes

        model = self.table.model_class
        self.columns = dict(model._meta.columns)
        self.columns.update(model._meta.fields)

    def _import_value(self, field, value):
        if value is None:
            return value
        if isinstance(field, DateTimeField) and self.iso8601_datetimes:
            value = datetime.datetime.fromisoformat(value)
        elif isinstance(field, DateField) and self.iso8601_datetimes:
            value = datetime.date.fromisoformat(value)
        elif isinstance(field, BlobField):
            if self.base64_bytes:
                value = base64.urlsafe_b64decode(value.encode('utf8'))
            else:
                value = bytes.fromhex(value)
        return field.python_value(value)

    def refresh_columns(self):
        model = self.table.model_class
        self.columns = dict(model._meta.columns)
        self.columns.update(model._meta.fields)

    def load(self, file_obj):
        raise NotImplementedError

    def _iter_source_rows(self, file_obj, **kwargs):
        raise NotImplementedError

    def _scan_source(self, file_obj, **kwargs):
        columns = set()
        samples = {}
        count = 0
        for _, row in self._iter_source_rows(file_obj, **kwargs):
            count += 1
            columns.update(row)
            for key, value in row.items():
                if key not in samples:
                    samples[key] = value
                elif isinstance(value, str):
                    samples[key] = ''
                elif value is not None and samples[key] is None:
                    samples[key] = value
                elif (isinstance(value, float) and
                      isinstance(samples[key], int) and
                      not isinstance(samples[key], bool)):
                    samples[key] = value
        return count, columns, samples

    def _rewind_source(self, file_obj):
        if not file_obj.seekable():
            raise TransferConflict('A seekable input is required.')
        file_obj.seek(0)

    def _missing_fields(self, source_columns):
        model = self.table.model_class
        names = set(model._meta.fields) | set(model._meta.columns)
        return [
            field for field in model._meta.sorted_fields
            if field.name not in source_columns and field.name in names]

    def _is_required_missing(self, field):
        meta = self.table.model_class._meta
        return (not field.null and
                field.default is None and
                not (field.primary_key and meta.auto_increment))

    def _coerce_value(self, field, value, line_number, row_number, column):
        try:
            if isinstance(field, BooleanField):
                if isinstance(value, bool):
                    return value
                if isinstance(value, int) and value in (0, 1):
                    return bool(value)
                if isinstance(value, str):
                    normalized = value.strip().lower()
                    if normalized in ('true', '1', 'yes'):
                        return True
                    if normalized in ('false', '0', 'no'):
                        return False
                raise ValueError('expected a boolean value')
            if isinstance(field, IntegerField):
                if isinstance(value, bool):
                    return int(value)
                if isinstance(value, int):
                    return value
                if isinstance(value, float) and value.is_integer():
                    return int(value)
                if isinstance(value, str):
                    return int(value.strip())
                if isinstance(value, Decimal) and value == value.to_integral_value():
                    return int(value)
                raise ValueError('expected an integer value')
            if isinstance(field, FloatField):
                converted = float(value)
                return converted
            if isinstance(field, DecimalField):
                converted = Decimal(str(value))
                if field.max_digits is not None:
                    sign, digits, exponent = converted.as_tuple()
                    integer_digits = max(len(digits) + exponent, 0)
                    if integer_digits > (field.max_digits - field.decimal_places):
                        raise ValueError('decimal value exceeds field precision')
                return converted
            if isinstance(field, (DateField, DateTimeField)) and isinstance(
                    value, str):
                converted = self._import_value(field, value)
                if isinstance(converted, str):
                    raise ValueError('expected a date/time value')
                return converted
            if isinstance(field, _StringField) and not isinstance(value, str):
                raise ValueError('expected a string value')
            return self._import_value(field, value)
        except (TypeError, ValueError) as exc:
            raise ImportRowError(
                line_number, row_number, column, value, exc)

    def _normalize_chunk(self, chunk, source_columns, start_row, on_unknown,
                         on_missing, on_type_error, empty_as_null):
        if on_unknown == 'error':
            unknown = sorted(
                set(source_columns) -
                (set(self.table.model_class._meta.fields) |
                 set(self.table.model_class._meta.columns)))
            if unknown:
                raise TransferConflict(
                    'Unknown column(s): %s' % ', '.join(unknown))
        if on_unknown == 'add':
            self.table.ensure_columns([row for _, row in chunk])
            self.refresh_columns()

        missing_fields = self._missing_fields(source_columns)
        required_missing = [
            field.name for field in missing_fields
            if self._is_required_missing(field)]
        if on_missing == 'error' and required_missing:
            raise TransferConflict(
                'Required column(s) missing: %s' %
                ', '.join(sorted(required_missing)))

        rows = []
        skipped = []
        explicit_nulls = set()
        model = self.table.model_class
        for offset, (line_number, raw) in enumerate(chunk):
            row_number = start_row + offset + 1
            obj = {}
            unknown_keys = set(raw) - set(self.columns)
            for name, field in self.columns.items():
                if name not in raw:
                    field = model._meta.fields.get(name)
                    if field is None:
                        continue
                    if name in [f.name for f in missing_fields]:
                        if on_missing == 'null':
                            if field.null:
                                obj[name] = None
                            elif self._is_required_missing(field):
                                raise ImportRowError(
                                    line_number, row_number, name, None,
                                    ValueError('required column is missing'))
                        continue
                    continue
                value = raw.get(name)
                if value is None:
                    explicit_nulls.add(name)
                    field = model._meta.fields.get(name, field)
                    if not getattr(field, 'null', True):
                        raise ImportRowError(
                            line_number, row_number, name, value,
                            ValueError('column is not nullable'))
                    obj[name] = None
                    continue
                if (value == '' and
                        (empty_as_null or not isinstance(field, _StringField))):
                    value = None
                    explicit_nulls.add(name)
                try:
                    converted = self._coerce_value(
                        field, value, line_number, row_number, name)
                except ImportRowError:
                    if on_type_error == 'null':
                        field = model._meta.fields.get(name, field)
                        if not getattr(field, 'null', True):
                            raise
                        converted = None
                        explicit_nulls.add(name)
                    elif on_type_error == 'skip':
                        skipped.append({
                            'row_number': row_number,
                            'line_number': line_number,
                        })
                        obj = None
                        break
                    else:
                        raise
                obj[name] = converted
            if obj is None:
                continue
            if on_unknown == 'ignore':
                unknown_present = [key for key in raw if key in unknown_keys]
                if not obj and unknown_present:
                    skipped.append({
                        'row_number': row_number,
                        'line_number': line_number,
                        'reason': 'no_known_columns',
                    })
                    continue
            elif on_unknown == 'add':
                for key in unknown_keys:
                    obj[key] = raw[key]
            if obj:
                rows.append(obj)
        return {
            'rows': rows,
            'skipped': skipped,
            'explicit_nulls': sorted(explicit_nulls),
            'missing_columns': [field.name for field in missing_fields],
            'added_columns': sorted(
                set(source_columns) &
                (set(model._meta.fields) | set(model._meta.columns))),
        }

    def load_stateful(self, file_obj, store, state, chunksize, max_rows,
                      resume, on_unknown, on_missing, on_type_error,
                      empty_as_null, use_transaction, use_batch, pulse=None,
                      **kwargs):
        self._rewind_source(file_obj)
        total_rows, source_columns, samples = self._scan_source(
            file_obj, **kwargs)
        if max_rows is not None and total_rows > max_rows:
            raise TransferSizeLimit(
                'Import contains %s rows, exceeding max_rows=%s.' % (
                    total_rows, max_rows))
        if on_unknown == 'error':
            model = self.table.model_class
            unknown = source_columns - (
                set(model._meta.fields) | set(model._meta.columns))
            if unknown:
                raise TransferConflict(
                    'Unknown column(s): %s' % ', '.join(sorted(unknown)))
        if on_unknown == 'add':
            self.table.ensure_columns([samples])
            self.refresh_columns()
        self._rewind_source(file_obj)

        dataset = self.table.dataset
        if use_transaction or use_batch:
            dataset._ensure_transfer_models()
        completed_state = {
            chunk['index']: chunk['checksum']
            for chunk in state.get('chunks', [])}
        completed_db = dataset._completed_chunks(state['transfer_id'])
        if completed_state and completed_db and completed_state != completed_db:
            raise TransferIntegrityError(
                'File state and database chunk state do not match.')
        completed = completed_db or completed_state

        inserted = 0
        skipped_chunks = 0
        skipped_rows = 0
        batched = bool(use_batch)
        transactional = bool(use_transaction)
        state['columns'] = sorted(source_columns)

        def rows():
            accum = []
            for line_number, row in self._iter_source_rows(
                    file_obj, **kwargs):
                accum.append((line_number, row))
                if len(accum) == chunksize:
                    yield accum
                    accum = []
            if accum:
                yield accum

        store.save(state)
        for chunk_index, chunk in enumerate(rows()):
            line_start = chunk[0][0]
            line_end = chunk[-1][0]
            start_row = chunk_index * chunksize
            end_row = start_row + len(chunk)
            raw_rows = [row for _, row in chunk]
            checksum = canonical_checksum(raw_rows)
            chunk_state = {
                'index': chunk_index,
                'start_row': start_row,
                'end_row': end_row,
                'line_start': line_start,
                'line_end': line_end,
                'checksum': checksum,
            }
            if chunk_index in completed:
                if completed[chunk_index] != checksum:
                    raise TransferIntegrityError(
                        'Chunk %s checksum does not match recorded state.' %
                        chunk_index)
                skipped_chunks += 1
                skipped_rows += len(chunk)
                if chunk_index not in completed_state:
                    state['chunks'].append(chunk_state)
                    store.save(state)
                continue

            normalized = self._normalize_chunk(
                chunk, source_columns, start_row, on_unknown, on_missing,
                on_type_error, empty_as_null)
            result_info = self._insert_chunk(
                chunk_state, normalized['rows'], state, use_transaction,
                use_batch)
            use_transaction = use_transaction and result_info['transactional']
            use_batch = use_batch and result_info['batched']
            transactional = transactional and result_info['transactional']
            batched = batched and result_info['batched']
            chunk_state.update({
                'columns': sorted(source_columns),
                'rows_inserted': len(normalized['rows']),
                'skipped_rows': normalized['skipped'],
                'explicit_nulls': normalized['explicit_nulls'],
                'missing_columns': normalized['missing_columns'],
                'transactional': result_info['transactional'],
                'batched': result_info['batched'],
            })
            state['columns'] = sorted(source_columns)
            state['chunks'].append(chunk_state)
            store.save(state)
            inserted += len(normalized['rows'])
            skipped_rows += len(normalized['skipped'])
            if pulse is not None:
                pulse()

        state['complete'] = True
        state['total_rows'] = total_rows
        store.save(state)
        return ImportResult(
            inserted,
            total_rows=total_rows,
            chunks_completed=len(state['chunks']),
            skipped_chunks=skipped_chunks,
            skipped_rows=skipped_rows,
            columns=sorted(source_columns),
            transactional=transactional,
            batched=batched,
            complete=True,
            transfer_id=state['transfer_id'])

    def _insert_without_transaction(self, rows, chunk, record):
        committed_lines = []
        try:
            for offset, row in enumerate(rows):
                self.table.model_class.insert(row).execute()
                committed_lines.append(chunk['line_start'] + offset)
            record()
        except Exception as exc:
            raise TransferDegraded(
                'Chunk was partially written because transactions are '
                'unavailable.',
                committed_lines,
                chunk=chunk['index']) from exc
        return {'transactional': False, 'batched': False}

    def _enter_transaction(self, dataset):
        context = dataset.transaction()
        try:
            context.__enter__()
        except Exception:
            return None
        return context

    def _insert_chunk(self, chunk, rows, state, use_transaction, use_batch):
        dataset = self.table.dataset
        resource = self.table.name

        def record():
            dataset._record_chunk(
                state['transfer_id'], chunk, 'import', resource)

        if not use_transaction:
            if not rows:
                record()
                return {'transactional': False, 'batched': False}
            return self._insert_without_transaction(rows, chunk, record)

        transaction = self._enter_transaction(dataset)
        if transaction is None:
            if not rows:
                record()
                return {'transactional': False, 'batched': False}
            return self._insert_without_transaction(rows, chunk, record)

        if not rows:
            try:
                record()
                transaction.__exit__(None, None, None)
            except Exception:
                transaction.__exit__(*sys.exc_info())
                raise
            return {'transactional': True, 'batched': True}

        if use_batch:
            try:
                self.table.model_class.insert_many(rows).execute()
                record()
                transaction.__exit__(None, None, None)
                return {'transactional': True, 'batched': True}
            except ImportRowError:
                transaction.__exit__(*sys.exc_info())
                raise
            except Exception:
                transaction.__exit__(*sys.exc_info())

        transaction = self._enter_transaction(dataset)
        if transaction is None:
            return self._insert_without_transaction(rows, chunk, record)

        # The bulk statement failed. Re-run row-by-row in a transaction so
        # that the exact failing line is reported while preserving atomicity.
        try:
            for offset, row in enumerate(rows):
                try:
                    self.table.model_class.insert(row).execute()
                except Exception as exc:
                    raise ImportRowError(
                        chunk['line_start'] + offset,
                        chunk['start_row'] + offset + 1,
                        None, row, exc)
            record()
            transaction.__exit__(None, None, None)
        except BaseException:
            transaction.__exit__(*sys.exc_info())
            raise
        return {'transactional': True, 'batched': False}


class JSONImporter(Importer):
    def load(self, file_obj, **kwargs):
        data = json.load(file_obj, **kwargs)
        count = 0

        for row in data:
            obj = {}
            for key in row:
                field = self.columns.get(key)
                if field is not None:
                    obj[key] = self._import_value(field, row[key])
                elif not self.strict:
                    obj[key] = row[key]

            if obj:
                self.table.insert(**obj)
                count += 1

        return count

    def _iter_source_rows(self, file_obj, **kwargs):
        decoder = json.JSONDecoder(**kwargs)
        buffer = ''
        index = 0
        line_number = 1
        started = False

        def fill():
            chunk = file_obj.read(65536)
            if chunk:
                return chunk
            return None

        while True:
            while True:
                while index < len(buffer) and buffer[index].isspace():
                    if buffer[index] == '\n':
                        line_number += 1
                    index += 1
                if index < len(buffer):
                    break
                chunk = fill()
                if not chunk:
                    if not started:
                        return
                    raise json.JSONDecodeError(
                        'Unexpected end of JSON input', buffer, index)
                buffer = buffer[index:] + chunk
                index = 0

            if not started:
                if buffer[index] == '[':
                    started = True
                    index += 1
                    continue
                started = True

            if buffer[index] == ']':
                return
            try:
                row, end = decoder.raw_decode(buffer, index)
            except json.JSONDecodeError:
                consumed = buffer[index:]
                chunk = fill()
                if not chunk:
                    raise
                buffer = consumed + chunk
                index = 0
                continue

            consumed = buffer[index:end]
            start_line = line_number
            line_number += consumed.count('\n')
            buffer = buffer[end:]
            index = 0
            if not isinstance(row, dict):
                raise TransferConflict('JSON import rows must be objects.')
            yield start_line, row

            while True:
                while index < len(buffer) and buffer[index].isspace():
                    if buffer[index] == '\n':
                        line_number += 1
                    index += 1
                if index < len(buffer):
                    break
                chunk = fill()
                if not chunk:
                    return
                buffer = buffer[index:] + chunk
                index = 0
            if buffer[index] == ',':
                index += 1
            elif buffer[index] == ']':
                return


class CSVImporter(Importer):
    def load(self, file_obj, header=True, **kwargs):
        count = 0
        reader = csv.reader(file_obj, **kwargs)

        header_fields = []
        if header:
            try:
                header_keys = next(reader)
            except StopIteration:
                return count

            for idx, key in enumerate(header_keys):
                if key in self.columns or not self.strict:
                    header_fields.append((idx, key, self.columns.get(key)))
        else:
            fields = self.table.model_class._meta.sorted_fields
            for idx, field in enumerate(fields):
                header_fields.append((idx, field.name, field))

        if not header_fields:
            return count

        for row in reader:
            obj = {}
            for idx, name, field in header_fields:
                value = row[idx]
                if field is None:
                    obj[name] = value
                    continue

                # CSV has no null, treat empty as NULL for non-text fields.
                if value == '' and not isinstance(field, _StringField):
                    value = None
                obj[field.name] = self._import_value(field, value)

            self.table.insert(**obj)
            count += 1

        return count

    def _csv_kwargs(self, kwargs):
        return {key: value for key, value in kwargs.items()
                if key != 'header'}

    def _csv_header(self, file_obj, header, kwargs):
        reader = csv.reader(file_obj, **self._csv_kwargs(kwargs))
        if header:
            try:
                return reader, next(reader)
            except StopIteration:
                return reader, []
        return reader, [field.name for field in
                        self.table.model_class._meta.sorted_fields]

    def _iter_source_rows(self, file_obj, header=True, **kwargs):
        reader, header_keys = self._csv_header(file_obj, header, kwargs)
        if not header_keys:
            return
        for row in reader:
            data = {}
            for idx, key in enumerate(header_keys):
                if idx < len(row):
                    data[key] = row[idx]
            yield reader.line_num, data


class TSVImporter(CSVImporter):
    def load(self, file_obj, header=True, **kwargs):
        kwargs.setdefault('delimiter', '\t')
        return super(TSVImporter, self).load(file_obj, header, **kwargs)

    def _csv_kwargs(self, kwargs):
        csv_kwargs = super(TSVImporter, self)._csv_kwargs(kwargs)
        csv_kwargs.setdefault('delimiter', '\t')
        return csv_kwargs
