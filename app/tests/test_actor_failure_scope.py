"""R1 failure-path regressions, real SQLite, offline provider seam only."""
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest

from app.actor_execution import ActorRunError, Attempt, ExecutionPolicy
from app.actors import ActorHost
from app.world import WorldStore
from app.tests import test_actor_execution as existing

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
PERSON = 'person-country-1-finance'


class ActorFailureScopeTests(unittest.TestCase):
    @contextmanager
    def pair(self, **policy):
        with tempfile.TemporaryDirectory(prefix='p022-reviewfix-', dir=SCRATCH) as temp:
            root = Path(temp)
            world = WorldStore(root/'world.sqlite')
            world.initialize(); world.control('resume')
            host = ActorHost(world, root/'actors.sqlite', execution_policy=ExecutionPolicy(**policy))
            try:
                yield world, host
            finally:
                host.close()

    @contextmanager
    def lock(self, world):
        entered = threading.Event(); release = threading.Event(); errors = []
        def hold():
            try:
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute('BEGIN IMMEDIATE'); entered.set()
                    release.wait(1); db.rollback()
            except BaseException as error:
                errors.append(error); entered.set()
        thread = threading.Thread(target=hold, name='p022-reviewfix-lock')
        thread.start()
        try:
            self.assertTrue(entered.wait(2)); self.assertFalse(errors)
            yield
        finally:
            release.set(); thread.join(2)
            self.assertFalse(thread.is_alive()); self.assertFalse(errors)

    def assert_prepared_only(self, world, host):
        with closing(sqlite3.connect(Path(host.db_path).resolve().as_uri()+'?mode=ro', uri=True)) as db:
            rows = db.execute('SELECT intent_id,state,candidate,metrics,receipt FROM actor_intents').fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][1:], ('prepared', None, None, None))
            self.assertEqual(db.execute('SELECT count(*) FROM actor_calls').fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT messages,seen FROM actor_sessions').fetchone(), ('[]', '[]'))
        self.assertIsNone(world.receipt(PERSON, rows[0][0]))

    def test_failure_pause_retains_original_attempt_and_deadline(self):
        with self.pair(act_deadline_seconds=.15) as (world, host):
            before = world.snapshot(); failure = RuntimeError('CONTROLLED_PROVIDER_FAILURE')
            scopes = []; contexts = []
            original = world.technical_pause
            def request(payload):
                context = self.lock(world); context.__enter__(); contexts.append(context)
                raise failure
            def pause(*args, **kwargs):
                scopes.append(world._scoped_attempt())
                self.assertEqual(kwargs['execution'], host._attempt.permit)
                return original(*args, **kwargs)
            host._request = request; world.technical_pause = pause
            start = time.monotonic()
            try:
                with self.assertRaises(RuntimeError) as caught:
                    host.act(PERSON, pause_on_failure=True)
                elapsed = time.monotonic()-start
            finally:
                for context in reversed(contexts): context.__exit__(None, None, None)
            self.assertIs(caught.exception, failure)
            self.assertEqual(world.snapshot(), before, 'expired attempt must not commit a failure pause')
            self.assertLess(elapsed, .7)
            self.assertEqual(len(scopes), 1)
            self.assertIsNotNone(scopes[0])
            self.assertIsNone(world._scoped_attempt())
            self.assert_prepared_only(world, host)
            print('failure_pause_deadline_elapsed', elapsed)

    def test_private_candidate_lock_wait_reports_elapsed_deadline(self):
        with self.pair(act_deadline_seconds=.15) as (world, host):
            before = world.snapshot(); contexts = []
            def request(payload):
                context = self.lock(world); context.__enter__(); contexts.append(context)
                return existing.ActorExecutionTests.result(payload)
            host._request = request
            start = time.monotonic()
            try:
                with self.assertRaises(Exception) as caught:
                    host.act(PERSON)
                elapsed = time.monotonic()-start
            finally:
                for context in reversed(contexts): context.__exit__(None, None, None)
            self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_DEADLINE')
            self.assertLess(elapsed, .7)
            self.assertEqual(world.snapshot(), before)
            self.assert_prepared_only(world, host)
            self.assertIsNone(world._scoped_attempt())
            print('private_deadline_elapsed', elapsed)

    def test_private_retry_backoff_rechecks_original_attempt_before_busy_exit(self):
        for code in ('ACTOR_EXECUTION_CANCELLED', 'ACTOR_EXECUTION_DEADLINE'):
            with self.subTest(code=code), self.pair() as (world, host):
                permit = world.acquire_actor_execution(PERSON, 'backoff-control')
                attempt = Attempt(permit, host.execution_policy)
                waits = []
                class CancelAtBackoff(threading.Event):
                    def wait(self, timeout=None):
                        waits.append(timeout)
                        if code.endswith('CANCELLED'):
                            attempt.failure = ActorRunError(code)
                        else:
                            attempt.deadline = 0
                        self.set()
                        return super().wait(timeout)
                attempt.stop = CancelAtBackoff()
                before = world.snapshot()
                try:
                    with self.lock(world):
                        with self.assertRaises(Exception) as caught:
                            with host._storage.transaction(permit, PERSON, attempt=attempt):
                                self.fail('busy transaction body must not run')
                    self.assertEqual(waits, [.05])
                    self.assertEqual(caught.exception.code, code)
                    self.assertEqual(world.snapshot(), before)
                finally:
                    world.release_actor_execution(permit)

    def test_characterization_ordinary_failure_can_pause_with_same_permit(self):
        with self.pair() as (world, host):
            failure = RuntimeError('CONTROLLED_PROVIDER_FAILURE'); captured = []; pauses = []
            original = world.technical_pause
            def request(payload):
                captured.append(host._attempt)
                raise failure
            def pause(code, *, execution):
                pauses.append((world._scoped_attempt(), execution, code))
                return original(code, execution=execution)
            host._request = request; world.technical_pause = pause
            with self.assertRaises(RuntimeError) as caught:
                host.act(PERSON, pause_on_failure=True)
            self.assertIs(caught.exception, failure)
            self.assertEqual(len(captured), 1)
            self.assertEqual(pauses, [(captured[0], captured[0].permit, 'HOST_FAILURE')])
            self.assertEqual(world.snapshot()['status'], 'technical_paused')
            self.assert_prepared_only(world, host)
            self.assertIsNone(world._scoped_attempt())
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT epoch,owner_token FROM actor_execution_leases WHERE person_id=?', (PERSON,)).fetchone(), (1, None))

    def test_characterization_default_failure_does_not_pause(self):
        with self.pair() as (world, host):
            before = world.snapshot(); failure = RuntimeError('CONTROLLED_PROVIDER_FAILURE')
            def request(payload):
                raise failure
            host._request = request
            with self.assertRaises(RuntimeError) as caught:
                host.act(PERSON)
            self.assertIs(caught.exception, failure)
            self.assertEqual(world.snapshot(), before)
            self.assert_prepared_only(world, host)

    def test_characterization_pause_cancel_or_expiry_preserves_provider_error_and_outer_scope(self):
        for code in ('ACTOR_EXECUTION_CANCELLED', 'ACTOR_EXECUTION_DEADLINE'):
            with self.subTest(code=code), self.pair() as (world, host):
                outer_permit = world.acquire_actor_execution('person-country-1-king', 'outer-control')
                outer = Attempt(outer_permit, host.execution_policy)
                before = world.snapshot(); failure = RuntimeError('CONTROLLED_PROVIDER_FAILURE')
                captured = []; original = world.technical_pause
                def request(payload):
                    captured.append(host._attempt)
                    raise failure
                def pause(*args, **kwargs):
                    self.assertIs(world._scoped_attempt(), captured[0])
                    self.assertEqual(kwargs['execution'], captured[0].permit)
                    if code.endswith('CANCELLED'):
                        captured[0].failure = ActorRunError(code); captured[0].stop.set()
                    else:
                        captured[0].deadline = 0
                    return original(*args, **kwargs)
                host._request = request; world.technical_pause = pause
                try:
                    with world._actor_attempt(outer):
                        with self.assertRaises(RuntimeError) as caught:
                            host.act(PERSON, pause_on_failure=True)
                        self.assertIs(world._scoped_attempt(), outer)
                    self.assertIs(caught.exception, failure)
                    self.assertEqual(world.snapshot(), before)
                    self.assert_prepared_only(world, host)
                    self.assertIsNone(world._scoped_attempt())
                finally:
                    world.release_actor_execution(outer_permit)

    def test_characterization_private_native_wait_prioritizes_original_cancellation_or_fencing(self):
        for code in ('ACTOR_EXECUTION_CANCELLED', 'ACTOR_EXECUTION_FENCED'):
            with self.subTest(code=code), self.pair() as (world, host):
                permit = world.acquire_actor_execution(PERSON, 'native-control')
                attempt = Attempt(permit, host.execution_policy)
                before = world.snapshot(); entered = threading.Event(); cancel_done = threading.Event()
                failure = ActorRunError(code); original = attempt.check
                def check():
                    original(); entered.set()
                attempt.check = check
                def cancel():
                    if entered.wait(2):
                        cancel_done.wait(.03)
                        attempt.failure = failure; attempt.stop.set()
                    cancel_done.set()
                thread = threading.Thread(target=cancel, name='p022-reviewfix-cancel')
                thread.start()
                try:
                    with self.lock(world):
                        start = time.monotonic()
                        with self.assertRaises(ActorRunError) as caught:
                            with host._storage.transaction(permit, PERSON, attempt=attempt):
                                self.fail('cancelled native wait must not enter body')
                        elapsed = time.monotonic()-start
                    self.assertTrue(cancel_done.is_set())
                    self.assertIs(caught.exception, failure)
                    self.assertLess(elapsed, .7)
                    self.assertEqual(world.snapshot(), before)
                finally:
                    cancel_done.set(); thread.join(2)
                    self.assertFalse(thread.is_alive())
                    world.release_actor_execution(permit)

    def test_characterization_private_valid_busy_and_unscoped_initialization_stay_busy(self):
        with self.pair(busy_budget_seconds=.05) as (world, host):
            permit = world.acquire_actor_execution(PERSON, 'busy-control')
            attempt = Attempt(permit, host.execution_policy)
            before = world.snapshot()
            try:
                for scoped in (True, False):
                    with self.subTest(scoped=scoped), self.lock(world):
                        with self.assertRaises(ActorRunError) as caught:
                            with host._storage.transaction(permit, PERSON, attempt=attempt if scoped else None, initialize=not scoped):
                                self.fail('busy transaction must not enter body')
                        self.assertEqual(caught.exception.code, 'EXECUTION_STORAGE_BUSY')
                        attempt.check()
                self.assertEqual(world.snapshot(), before)
            finally:
                world.release_actor_execution(permit)

    def test_characterization_private_nonbusy_sql_error_is_not_reclassified(self):
        with self.pair() as (world, host):
            permit = world.acquire_actor_execution(PERSON, 'sql-control')
            attempt = Attempt(permit, host.execution_policy)
            try:
                with self.assertRaises(sqlite3.OperationalError) as caught:
                    with host._storage.transaction(permit, PERSON, attempt=attempt) as db:
                        db.execute('CREATE TABLE actor.reviewfix_rollback_control (value)')
                        db.execute('SELECT * FROM deliberately_missing_table')
                self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_ERROR)
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='reviewfix_rollback_control'").fetchone())
            finally:
                world.release_actor_execution(permit)
