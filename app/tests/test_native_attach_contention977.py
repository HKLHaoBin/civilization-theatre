"""977 regressions. Compile-only until independent parent review authorizes run."""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest

from app.actor_execution import EntryBudget, ExecutionPolicy
from app.world import _AttemptConnection, WorldError, _readonly_attach_retry
from app.coordinator_scheduler import _c0_admission_record

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class NativeAttachContention977Tests(unittest.TestCase):
    @contextmanager
    def held(self, which='actor', begin='BEGIN EXCLUSIVE'):
        with tempfile.TemporaryDirectory(prefix='parent977-tests-', dir=SCRATCH) as folder:
            paths = {name: Path(folder) / (name+'.sqlite') for name in ('main', 'actor')}
            for path in paths.values():
                db = sqlite3.connect(path)
                try:
                    db.execute('CREATE TABLE payload(value INTEGER)').close()
                    db.execute('INSERT INTO payload VALUES(1)').close()
                    db.commit()
                finally:
                    db.close()
            ready, release = threading.Event(), threading.Event()
            errors = []
            def holder():
                db = None
                try:
                    db = sqlite3.connect(paths[which], timeout=0)
                    db.execute(begin).close()
                    db.execute('UPDATE payload SET value=2').close()
                    ready.set()
                    if not release.wait(4):
                        raise RuntimeError('bounded fixture holder timeout')
                    db.commit()
                except BaseException as error:
                    errors.append(error)
                    ready.set()
                finally:
                    if db is not None:
                        db.close()
            thread = threading.Thread(target=holder, name='parent977-test-holder', daemon=False)
            thread.start()
            try:
                self.assertTrue(ready.wait(2))
                self.assertEqual(errors, [])
                yield paths, release
            finally:
                release.set()
                thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(errors, [])

    @contextmanager
    def reader(self, paths, budget):
        db = sqlite3.connect(paths['main'].as_uri()+'?mode=ro', uri=True,
                             timeout=0, factory=_AttemptConnection)
        db._attempt = budget
        try:
            yield db
        finally:
            db.close()
            with self.assertRaises(sqlite3.ProgrammingError):
                sqlite3.Connection.in_transaction.__get__(db, sqlite3.Connection)

    def attach(self, db, paths):
        db.execute('ATTACH DATABASE ? AS actor', (paths['actor'].as_uri()+'?mode=ro',)).close()

    def test_transient_actor_and_main_attach_recover_without_new_budget(self):
        for which in ('actor', 'main'):
            with self.subTest(which=which), self.held(which) as (paths, release):
                budget = EntryBudget(ExecutionPolicy(), threading.Event())
                bounds = (budget.deadline, budget.busy_deadline)
                attempted = threading.Event()
                failures = []
                def releaser():
                    try:
                        if not attempted.wait(2):
                            raise RuntimeError('reader start timeout')
                        # Beyond three native attempts on observed Windows handler.
                        release.wait(.55)
                    except BaseException as error:
                        failures.append(error)
                    finally:
                        release.set()
                thread = threading.Thread(target=releaser, daemon=False)
                thread.start()
                try:
                    with self.reader(paths, budget) as db:
                        attempted.set()
                        self.attach(db, paths)
                        self.assertIs(db._attempt, budget)
                        self.assertFalse(db.in_transaction)
                        self.assertEqual(db.execute('SELECT value FROM actor.payload').fetchone(), (2 if which == 'actor' else 1,))
                        self.assertEqual(db.execute('SELECT value FROM main.payload').fetchone(), (2 if which == 'main' else 1,))
                        self.assertEqual((budget.deadline, budget.busy_deadline), bounds)
                        budget.check()
                finally:
                    attempted.set(); release.set(); thread.join(2)
                    self.assertFalse(thread.is_alive())
                    self.assertEqual(failures, [])

    def test_persistent_busy_keeps_two_second_entry_limit(self):
        with self.held() as (paths, release):
            budget = EntryBudget(ExecutionPolicy(), threading.Event())
            bounds = budget.deadline, budget.busy_deadline
            with self.reader(paths, budget) as db:
                began = time.monotonic()
                with self.assertRaises(WorldError) as caught:
                    self.attach(db, paths)
                elapsed = time.monotonic()-began
                self.assertEqual(caught.exception.code, 'EXECUTION_STORAGE_BUSY')
                self.assertGreaterEqual(elapsed, 1.7)
                self.assertLess(elapsed, 2.8)  # Native calls are not preemptible.
                self.assertEqual((budget.deadline, budget.busy_deadline), bounds)
                self.assertFalse(release.is_set())

    def test_captured_shorter_boundary_is_not_refreshed(self):
        with self.held() as (paths, release):
            parent = EntryBudget(ExecutionPolicy(busy_budget_seconds=.15), threading.Event())
            budget = EntryBudget(ExecutionPolicy(), threading.Event(), (parent,))
            self.assertEqual(budget.busy_deadline, parent.busy_deadline)
            with self.reader(paths, budget) as db:
                began = time.monotonic()
                with self.assertRaises(WorldError) as caught:
                    self.attach(db, paths)
                self.assertEqual(caught.exception.code, 'EXECUTION_STORAGE_BUSY')
                self.assertLess(time.monotonic()-began, .8)
                self.assertEqual(budget.busy_deadline, parent.busy_deadline)

    def test_parent_total_deadline_wins(self):
        with self.held() as (paths, release):
            parent = EntryBudget(ExecutionPolicy(act_deadline_seconds=.15), threading.Event())
            budget = EntryBudget(ExecutionPolicy(), threading.Event(), (parent,))
            with self.reader(paths, budget) as db:
                with self.assertRaises(WorldError) as caught:
                    self.attach(db, paths)
                self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_DEADLINE')
                self.assertEqual(budget.deadline, parent.deadline)

    def test_stop_during_busy_wait_wins_and_threads_close(self):
        with self.held() as (paths, release):
            stop = threading.Event()
            budget = EntryBudget(ExecutionPolicy(), stop)
            started = threading.Event()
            def cancel():
                if started.wait(2):
                    stop.wait(.08); stop.set()
            thread = threading.Thread(target=cancel, daemon=False)
            thread.start()
            try:
                with self.reader(paths, budget) as db:
                    started.set()
                    with self.assertRaises(WorldError) as caught:
                        self.attach(db, paths)
                    self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_CANCELLED')
            finally:
                started.set(); stop.set(); thread.join(2)
                self.assertFalse(thread.is_alive())

    def test_reserved_actor_writer_allows_readonly_attach(self):
        with self.held(begin='BEGIN IMMEDIATE') as (paths, release):
            budget = EntryBudget(ExecutionPolicy(), threading.Event())
            with self.reader(paths, budget) as db:
                self.attach(db, paths)
                self.assertFalse(release.is_set())
                self.assertEqual(db.execute('SELECT value FROM actor.payload').fetchone(), (1,))

    def test_retry_eligibility_is_not_an_authority_or_transaction_bypass(self):
        with self.held(begin='BEGIN IMMEDIATE') as (paths, release):
            budget = EntryBudget(ExecutionPolicy(), threading.Event())
            with self.reader(paths, budget) as db:
                native = sqlite3.Connection.execute.__get__(db)
                args = ('ATTACH DATABASE ? AS actor', (paths['actor'].as_uri()+'?mode=ro',))
                self.assertTrue(_readonly_attach_retry(db, budget, native, args, {}))
                for call, positional, named in ((lambda *a: None, args, {}), (native, ('SELECT 1',), {}),
                                                (native, (args[0], (str(paths['actor']),)), {}),
                                                (native, args, {'unexpected': True})):
                    self.assertFalse(_readonly_attach_retry(db, budget, call, positional, named))
                self.assertFalse(_readonly_attach_retry(db, object(), native, args, {}))
                db.execute('BEGIN').close()
                self.assertFalse(_readonly_attach_retry(db, budget, native, args, {}))
                db.rollback()

    def test_nonbusy_native_error_is_not_normalized_or_retried(self):
        budget = EntryBudget(ExecutionPolicy(), threading.Event())
        db = sqlite3.connect(':memory:', factory=_AttemptConnection)
        db._attempt = budget
        try:
            with self.assertRaises(sqlite3.OperationalError) as caught:
                db.execute('INVALID SQL')
            self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_ERROR)
        finally:
            db.close()

    def test_c0_admission_negative_shape_stays_fail_closed(self):
        world, budget, db = object(), object(), object()
        path = SCRATCH / 'parent977-never-created.sqlite'
        local = dict(self=world, budget=budget, db=db, actor_path=path, failure=[], old_length=1048576)
        native = dict(self=db, attempt=budget, index=2,
                      args=('ATTACH DATABASE ? AS actor', (path.as_uri()+'?mode=ro',)), kwargs={})
        self.assertTrue(_c0_admission_record(local, native, world, budget, path, 256, 256))
        for key, value in (('index', 3), ('self', object()), ('attempt', object()),
                           ('args', ('SELECT 1',)), ('kwargs', {'x': 1})):
            altered = dict(native); altered[key] = value
            self.assertFalse(_c0_admission_record(local, altered, world, budget, path, 256, 256))
        local['failure'] = [RuntimeError('progress')]
        self.assertFalse(_c0_admission_record(local, native, world, budget, path, 256, 256))
        # This is the pure shape predicate, NOT loaded-native authority approval.


if __name__ == '__main__':
    unittest.main()
