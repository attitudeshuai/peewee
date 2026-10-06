import datetime

from peewee import *
from playhouse.sqlite_changelog import ChangeLog
from playhouse.sqlite_changelog import ChangeEntry
from playhouse.sqlite_changelog import ChangeLogGapError
from playhouse.sqlite_changelog import LeaseBusyError
from playhouse.sqlite_changelog import LeaseLostError
from playhouse.sqlite_changelog import MISSING

from .base import ModelTestCase
from .base import TestModel
from .base import requires_models
from .base import skip_unless


database = SqliteDatabase(':memory:', pragmas={'foreign_keys': 1})


class Person(TestModel):
    name = TextField()
    dob = DateField()


class Note(TestModel):
    person = ForeignKeyField(Person, on_delete='CASCADE')
    content = TextField()
    timestamp = TimestampField()
    status = IntegerField(default=0)


class CT1(TestModel):
    f1 = TextField()
    f2 = IntegerField(null=True)
    f3 = FloatField()
    fi = IntegerField()


class CT2(TestModel):
    data = JSONField()  # Diff of json?


changelog = ChangeLog(database)
CL = changelog.model


class TestChangeLog(ModelTestCase):
    database = database
    requires = [Person, Note]

    def setUp(self):
        super(TestChangeLog, self).setUp()
        changelog.install(Person)
        changelog.install(Note, skip_fields=['timestamp'])
        self.last_index = 0

    def assertChanges(self, changes, last_index=None):
        last_index = last_index or self.last_index
        query = (CL
                 .select(CL.action, CL.table, CL.changes)
                 .order_by(CL.id)
                 .offset(last_index))
        accum = list(query.tuples())
        self.last_index += len(accum)
        self.assertEqual(accum, changes)

    def test_changelog(self):
        huey = Person.create(name='huey', dob=datetime.date(2010, 5, 1))
        zaizee = Person.create(name='zaizee', dob=datetime.date(2013, 1, 1))
        self.assertChanges([
            ('INSERT', 'person', {'name': [None, 'huey'],
                                  'dob': [None, '2010-05-01']}),
            ('INSERT', 'person', {'name': [None, 'zaizee'],
                                  'dob': [None, '2013-01-01']})])

        zaizee.dob = datetime.date(2013, 2, 2)
        zaizee.save()
        self.assertChanges([
            ('UPDATE', 'person', {'dob': ['2013-01-01', '2013-02-02']})])

        zaizee.name = 'zaizee-x'
        zaizee.dob = datetime.date(2013, 3, 3)
        zaizee.save()

        huey.save()  # No changes.

        self.assertChanges([
            ('UPDATE', 'person', {'name': ['zaizee', 'zaizee-x'],
                                  'dob': ['2013-02-02', '2013-03-03']}),
            ('UPDATE', 'person', {})])

        zaizee.delete_instance()
        self.assertChanges([
            ('DELETE', 'person', {'name': ['zaizee-x', None],
                                  'dob': ['2013-03-03', None]})])

        nh1 = Note.create(person=huey, content='huey1', status=1)
        nh2 = Note.create(person=huey, content='huey2', status=2)
        self.assertChanges([
            ('INSERT', 'note', {'person_id': [None, huey.id],
                                'content': [None, 'huey1'],
                                'status': [None, 1]}),
            ('INSERT', 'note', {'person_id': [None, huey.id],
                                'content': [None, 'huey2'],
                                'status': [None, 2]})])

        nh1.content = 'huey1-x'
        nh1.status = 0
        nh1.save()

        mickey = Person.create(name='mickey', dob=datetime.date(2009, 8, 1))
        nh2.person = mickey
        nh2.save()

        self.assertChanges([
            ('UPDATE', 'note', {'content': ['huey1', 'huey1-x'],
                                'status': [1, 0]}),
            ('INSERT', 'person', {'name': [None, 'mickey'],
                                  'dob': [None, '2009-08-01']}),
            ('UPDATE', 'note', {'person_id': [huey.id, mickey.id]})])

        mickey.delete_instance()
        self.assertChanges([
            ('DELETE', 'note', {'person_id': [mickey.id, None],
                                'content': ['huey2', None],
                                'status': [2, None]}),
            ('DELETE', 'person', {'name': ['mickey', None],
                                  'dob': ['2009-08-01', None]})])

    @requires_models(CT1)
    def test_changelog_details(self):
        changelog.install(CT1, skip_fields=['fi'], insert=False, delete=False)

        c1 = CT1.create(f1='v1', f2=1, f3=1.5, fi=0)
        self.assertChanges([])

        CT1.update(f1='v1-x', f2=2, f3=2.5, fi=1).execute()
        self.assertChanges([
            ('UPDATE', 'ct1', {
                'f1': ['v1', 'v1-x'],
                'f2': [1, 2],
                'f3': [1.5, 2.5]})])

        c1.f2 = None
        c1.save()  # Overwrites previously-changed fields.
        self.assertChanges([('UPDATE', 'ct1', {
            'f1': ['v1-x', 'v1'],
            'f2': [2, None],
            'f3': [2.5, 1.5]})])

        c1.delete_instance()
        self.assertChanges([])

    @requires_models(CT2)
    def test_changelog_jsonfield(self):
        changelog.install(CT2)

        ca = CT2.create(data={'k1': 'v1'})
        cb = CT2.create(data=['i0', 'i1', 'i2'])
        cc = CT2.create(data='hello')

        self.assertChanges([
            ('INSERT', 'ct2', {'data': [None, {'k1': 'v1'}]}),
            ('INSERT', 'ct2', {'data': [None, ['i0', 'i1', 'i2']]}),
            ('INSERT', 'ct2', {'data': [None, 'hello']})])

        ca.data['k1'] = 'v1-x'
        cb.data.append('i3')
        cc.data = 'world'

        ca.save()
        cb.save()
        cc.save()

        self.assertChanges([
            ('UPDATE', 'ct2', {'data': [{'k1': 'v1'}, {'k1': 'v1-x'}]}),
            ('UPDATE', 'ct2', {'data': [['i0', 'i1', 'i2'],
                                        ['i0', 'i1', 'i2', 'i3']]}),
            ('UPDATE', 'ct2', {'data': ['hello', 'world']})])

        cc.data = 13.37
        cc.save()
        self.assertChanges([('UPDATE', 'ct2', {'data': ['world', 13.37]})])

        ca.delete_instance()
        self.assertChanges([
            ('DELETE', 'ct2', {'data': [{'k1': 'v1-x'}, None]})])


CS = changelog.consumer_model


def expire_lease(name):
    # Simulate a crashed worker whose lease has aged out (database clock).
    database.execute_sql(
        'UPDATE "%s" SET lease_expires = datetime(\'now\', \'-10 seconds\') '
        'WHERE name = ?' % changelog.consumer_table_name, (name,))


class TestChangeLogConsumer(ModelTestCase):
    database = database
    requires = [Person]

    def setUp(self):
        super(TestChangeLogConsumer, self).setUp()
        changelog.install(Person)

    def createPeople(self, *names):
        for i, name in enumerate(names):
            Person.create(name=name, dob=datetime.date(2000, 1, i + 1))

    def ids(self, entries):
        return [entry.id for entry in entries]

    def test_channel_disabled_by_default(self):
        # The consumer-state table is created lazily: triggers and the
        # changelog table are untouched until consumer() is called.
        tables = database.get_tables()
        self.assertFalse(changelog.consumer_table_name in tables)

        self.createPeople('huey')
        self.assertEqual(CL.select().count(), 1)

        consumer = changelog.consumer('c1')
        self.assertTrue(changelog.consumer_table_name in
                        database.get_tables())
        self.assertEqual(consumer.position, 0)

    def test_fetch_order_limit_redeliver_and_ack(self):
        self.createPeople('huey', 'zaizee', 'mickey')
        consumer = changelog.consumer('c1', lease_duration=300)

        batch = consumer.fetch(limit=2)
        self.assertEqual(self.ids(batch), [1, 2])
        self.assertTrue(all(isinstance(e, ChangeEntry) for e in batch))
        entry = batch[0]
        self.assertEqual(entry.action, 'INSERT')
        self.assertEqual(entry.table, 'person')
        self.assertEqual(entry.primary_key, 1)
        self.assertEqual(entry.changes['name'], [None, 'huey'])

        # An unacked batch is re-delivered to its owner in full, rather than
        # leasing rows past it, and the checkpoint has not moved.
        self.assertEqual(self.ids(consumer.fetch(limit=2)), [1, 2])
        self.assertEqual(consumer.position, 0)

        self.assertEqual(consumer.ack(), 2)
        self.assertEqual(self.ids(consumer.fetch(limit=2)), [3])
        self.assertEqual(consumer.ack(), 3)

        # Caught up: empty list, not an error.
        self.assertEqual(consumer.fetch(limit=2), [])
        self.assertEqual(consumer.position, 3)

        self.assertRaises(ValueError, consumer.fetch, 0)

    def test_batch_is_limited(self):
        self.createPeople('p1', 'p2', 'p3', 'p4', 'p5')
        consumer = changelog.consumer('c1', lease_duration=300)

        batch = consumer.fetch(limit=3)
        self.assertEqual(len(batch), 3)
        self.assertEqual(self.ids(batch), [1, 2, 3])
        consumer.ack()

        batch = consumer.fetch(limit=3)
        self.assertEqual(len(batch), 2)
        self.assertEqual(self.ids(batch), [4, 5])
        consumer.ack()

        self.assertEqual(consumer.fetch(limit=3), [])

    def test_restart_resumes_from_checkpoint(self):
        self.createPeople('p1', 'p2', 'p3', 'p4')
        worker = changelog.consumer('grp', lease_duration=300)
        self.assertEqual(self.ids(worker.fetch(limit=2)), [1, 2])
        # "Crash" without acking: a new process creates a new instance with
        # a fresh owner token and resumes the same consumer name.
        restarted = changelog.consumer('grp', lease_duration=300)
        self.assertRaises(LeaseBusyError, restarted.fetch, 5)

        # Once the old lease expires, the new owner takes the unconfirmed
        # range over (no skip, no silent jump) and confirms it.
        expire_lease('grp')
        self.assertEqual(self.ids(restarted.fetch(limit=2)), [1, 2])
        self.assertEqual(restarted.ack(), 2)

        # The stale owner is fenced off: it cannot ack nor renew after the
        # lease was taken over.
        self.assertRaises(LeaseLostError, worker.ack)
        self.assertRaises(LeaseLostError, worker.renew)

        self.assertEqual(self.ids(restarted.fetch(limit=5)), [3, 4])
        self.assertEqual(restarted.ack(), 4)
        self.assertEqual(restarted.fetch(limit=5), [])

    def test_takeover_does_not_double_commit(self):
        self.createPeople('p1', 'p2', 'p3')
        a = changelog.consumer('grp', lease_duration=300, owner='worker-a')
        b = changelog.consumer('grp', lease_duration=300, owner='worker-b')

        self.assertEqual(self.ids(a.fetch(limit=3)), [1, 2, 3])

        # While A's lease is valid B can neither fetch nor ack.
        self.assertRaises(LeaseBusyError, b.fetch, 3)
        self.assertRaises(LeaseLostError, b.ack)

        expire_lease('grp')
        self.assertEqual(self.ids(b.fetch(limit=3)), [1, 2, 3])

        state = CS.get(CS.name == 'grp')
        self.assertEqual(state.lease_owner, 'worker-b')
        self.assertEqual(state.lease_to, 3)

        # A coming back from the dead cannot commit the same range.
        self.assertRaises(LeaseLostError, a.ack)
        self.assertRaises(LeaseLostError, a.renew)
        self.assertRaises(LeaseLostError, a.release)

        self.assertEqual(b.ack(), 3)
        state = CS.get(CS.name == 'grp')
        self.assertEqual(state.position, 3)
        self.assertIsNone(state.lease_owner)
        self.assertIsNone(state.lease_to)

    def test_lease_expiry_is_configurable_and_renewable(self):
        self.createPeople('p1')
        consumer = changelog.consumer('grp', lease_duration=120)
        consumer.fetch(limit=5)

        active = (CS
                  .select(CS.lease_expires
                          > fn.datetime('now', '+60 seconds'))
                  .where(CS.name == 'grp')
                  .scalar())
        self.assertEqual(active, 1)

        consumer.renew(300)
        active = (CS
                  .select(CS.lease_expires
                          > fn.datetime('now', '+240 seconds'))
                  .where(CS.name == 'grp')
                  .scalar())
        self.assertEqual(active, 1)

        self.assertRaises(ValueError, changelog.consumer, 'bad',
                          lease_duration=0)

    def test_release_redelivers(self):
        self.createPeople('p1', 'p2', 'p3')
        a = changelog.consumer('grp', lease_duration=300, owner='worker-a')
        b = changelog.consumer('grp', lease_duration=300, owner='worker-b')

        self.assertEqual(self.ids(a.fetch(limit=2)), [1, 2])
        # A stranger cannot release someone else's lease.
        self.assertRaises(LeaseLostError, b.release)

        a.release()
        # Position unchanged: the previously leased rows are fetched again
        # from position + 1 rather than being skipped.
        self.assertEqual(a.position, 0)
        self.assertEqual(self.ids(a.fetch(limit=2)), [1, 2])

    def test_gap_reported_when_purged_behind_checkpoint(self):
        self.createPeople('p1', 'p2', 'p3', 'p4')
        consumer = changelog.consumer('grp', lease_duration=300)
        self.assertEqual(self.ids(consumer.fetch(limit=10)), [1, 2, 3, 4])
        consumer.ack()
        self.assertEqual(consumer.position, 4)

        # Rows 5 and 6 arrive, then retention deletes everything up to 5.
        self.createPeople('p5', 'p6')
        CL.delete().where(CL.id <= 5).execute()

        with self.assertRaises(ChangeLogGapError) as ctx:
            consumer.fetch(limit=10)
        self.assertEqual(ctx.exception.expected, 5)
        self.assertEqual(ctx.exception.available, 6)

        # The checkpoint is left untouched and the stream is not silently
        # replayed from the new head.
        self.assertEqual(consumer.position, 4)

        # Whole log purged while behind: gap with available=None.
        CL.delete().execute()
        with self.assertRaises(ChangeLogGapError) as ctx:
            consumer.fetch(limit=10)
        self.assertEqual(ctx.exception.expected, 5)
        self.assertIsNone(ctx.exception.available)

    def test_gap_reported_on_hole(self):
        self.createPeople('p1', 'p2', 'p3', 'p4')
        CL.delete().where(CL.id == 2).execute()  # Mid-stream purge.

        consumer = changelog.consumer('grp', lease_duration=300)
        with self.assertRaises(ChangeLogGapError) as ctx:
            consumer.fetch(limit=10)
        self.assertEqual(ctx.exception.expected, 2)
        self.assertEqual(ctx.exception.available, 3)
        self.assertEqual(consumer.position, 0)

    def test_start_latest_explicit_and_default(self):
        self.createPeople('p1', 'p2')

        tail = changelog.consumer('tail', start='latest')
        self.assertEqual(tail.fetch(limit=10), [])  # No backlog delivered.

        from_two = changelog.consumer('from2', start=2)
        beginning = changelog.consumer('beginning')  # start=None default.

        self.createPeople('p3')
        self.assertEqual(self.ids(tail.fetch(limit=10)), [3])
        self.assertEqual(self.ids(from_two.fetch(limit=10)), [2, 3])
        self.assertEqual(self.ids(beginning.fetch(limit=10)), [1, 2, 3])

        # start is only honored at registration: restarting "tail" resumes.
        tail.ack()
        restarted = changelog.consumer('tail', start=0)
        self.assertEqual(restarted.position, 3)
        self.assertEqual(restarted.fetch(limit=10), [])

        self.assertRaises(ValueError, changelog.consumer, 'bad1',
                          start='nope')
        self.assertRaises(ValueError, changelog.consumer, 'bad2', start=-3)

    @requires_models(CT1)
    def test_missing_column_distinct_from_null(self):
        changelog.install(CT1, skip_fields=['fi'])
        row = CT1.create(f1='v1', f2=1, f3=1.5, fi=0)
        row.f2 = None
        row.save()  # Only f2 appears in this change.

        consumer = changelog.consumer('ct', lease_duration=300, start=1)
        update = [e for e in consumer.fetch(limit=10)
                  if e.action == 'UPDATE'][0]

        # Present with an explicit NULL vs absent from the payload:
        self.assertIsNone(update.new_value('f2'))
        self.assertEqual(update.old_value('f2'), 1)
        self.assertTrue(update.new_value('f1') is MISSING)
        self.assertTrue(update.old_value('f1') is MISSING)
        self.assertTrue(update.has_column('f2'))
        self.assertFalse(update.has_column('f1'))
        self.assertEqual(update.columns, ['f2'])

    @requires_models(CT1)
    def test_payload_drives_columns_after_schema_change(self):
        # Simulate a trigger rebuilt after the tracked table gained/lost
        # columns: the consumer reads exactly the keys present in the
        # payload and never validates them against a fixed field list.
        changelog.install(CT1, skip_fields=['fi'])
        CT1.create(f1='v1', f2=1, f3=1.5, fi=0)
        consumer = changelog.consumer('ct', lease_duration=300)
        self.assertEqual(len(consumer.fetch(limit=10)), 1)
        consumer.ack()

        # New column "f_new" appears (NULL old value); f2 absent entirely.
        CL.insert(action='UPDATE', table='ct1', primary_key=1,
                  changes={'f1': ['v1', 'v2'],
                           'f_new': [None, 99]}).execute()
        entry = consumer.fetch(limit=10)[0]
        self.assertEqual(set(entry.columns), {'f1', 'f_new'})
        self.assertEqual(entry.new_value('f_new'), 99)
        self.assertIsNone(entry.old_value('f_new'))  # Present NULL.
        self.assertFalse(entry.has_column('f2'))
        self.assertTrue(entry.new_value('f2') is MISSING)
        self.assertEqual(entry.new_value('f1'), 'v2')
        consumer.ack()

        # A dropped column still reads correctly from historical payloads.
        CL.insert(action='UPDATE', table='ct1', primary_key=1,
                  changes={'f3': [1.5, 2.5]}).execute()
        entry = consumer.fetch(limit=10)[0]
        self.assertEqual(entry.columns, ['f3'])
        self.assertEqual(entry.old_value('f3'), 1.5)
        self.assertTrue(entry.new_value('f1') is MISSING)

    def test_independent_consumers(self):
        self.createPeople('p1', 'p2', 'p3')
        a = changelog.consumer('indexer', lease_duration=300)
        b = changelog.consumer('cache', lease_duration=300)

        # Each named consumer gets the full stream and its own lease.
        self.assertEqual(self.ids(a.fetch(limit=2)), [1, 2])
        self.assertEqual(self.ids(b.fetch(limit=10)), [1, 2, 3])
        self.assertEqual(a.ack(), 2)
        self.assertEqual(b.ack(), 3)

        self.assertEqual(self.ids(a.fetch(limit=10)), [3])
        self.assertEqual(b.fetch(limit=10), [])
        self.assertEqual(a.position, 2)
        self.assertEqual(b.position, 3)
