"""Trusted attempt regressions using original World/ADJ and real SQLite.
All stores are isolated under Hermes scratch. No model, Node or service needed.
"""
from contextlib import closing, contextmanager
from pathlib import Path
import json
import sqlite3
import tempfile
import threading
import time
import unittest

from app.actor_execution import Attempt, ActorRunError, ExecutionPolicy
from app.actors import ActorHost
from app.world import WorldStore
from app.adjudication import Adjudicator
from app.tests import test_actor_execution as existing

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
PERSON = 'person-country-1-finance'


class ActorDeadlineTests(unittest.TestCase):
    @contextmanager
    def pair(self, **options):
        with tempfile.TemporaryDirectory(prefix='p022-deadline-', dir=SCRATCH) as temp:
            root = Path(temp)
            world = WorldStore(root/'world.sqlite')
            world.initialize(); world.control('resume')
            host = ActorHost(world, root/'actors.sqlite', **options)
            host._request = existing.ActorExecutionTests.result
            try:
                yield world, host
            finally:
                host.close()

    @contextmanager
    def locked(self, world, seconds=1):
        entered = threading.Event(); release = threading.Event(); errors = []
        def hold():
            try:
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute('BEGIN EXCLUSIVE'); entered.set()
                    release.wait(seconds); db.rollback()
            except BaseException as error:
                errors.append(error); entered.set()
        thread = threading.Thread(target=hold, name='p022-deadline-lock')
        thread.start(); self.assertTrue(entered.wait(2)); self.assertFalse(errors)
        try:
            yield
        finally:
            release.set(); thread.join(2)
            self.assertFalse(thread.is_alive()); self.assertFalse(errors)

    def test_scope_cannot_substitute_another_principals_valid_permit(self):
        with self.pair() as (world, host):
            other = 'person-country-1-king'
            first = world.acquire_actor_execution(PERSON, 'scope-first')
            second = world.acquire_actor_execution(other, 'scope-second')
            attempt = Attempt(first, ExecutionPolicy())
            before = world.snapshot()
            try:
                with world._actor_attempt(attempt):
                    with self.assertRaises(Exception) as caught:
                        world.submit(other, 'wrong-scope', {'action':'wait'}, execution=second)
                    self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_FENCED')
                self.assertEqual(world.snapshot(), before)
                self.assertIsNone(world.receipt(other, 'wrong-scope'))
            finally:
                world.release_actor_execution(first); world.release_actor_execution(second)

    def test_characterization_submit_first_transaction_rolls_back_on_final_cancel(self):
        with self.pair() as (world, host):
            original = world._check_actor_execution; hits = []
            def guard(db, identity, person, permit, **kwargs):
                original(db, identity, person, permit, **kwargs)
                if 'actor' not in [r[1] for r in db.execute('PRAGMA database_list')]:
                    if db.execute('SELECT 1 FROM intents WHERE person=?', (PERSON,)).fetchone():
                        hits.append(True)
                        host._attempt.failure = ActorRunError('ACTOR_EXECUTION_CANCELLED')
            world._check_actor_execution = guard
            with self.assertRaisesRegex(Exception, 'ACTOR_EXECUTION_CANCELLED'):
                host.act(PERSON)
            self.assertEqual(hits, [True])
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 0)
            world._check_actor_execution = original
            with host._connect() as db:
                intent = db.execute('SELECT intent_id FROM actor_intents').fetchone()[0]
            recovered = host.act(PERSON)
            self.assertEqual(recovered['intent_id'], intent)

    def test_characterization_dm_compound_final_cancel_rolls_back_children_and_recovers(self):
        from app.scripted_dm import ScriptedDM
        with self.pair() as (world, host):
            adj = Adjudicator(world)
            host.adjudicator = adj
            def provider(payload):
                decision = ScriptedDM(world)(payload)
                decision['effects'] = [{'action':'investigate'}, {'action':'wait'}]
                return decision
            host.dm_provider = provider
            host._choose = lambda view, seen: ({'action':'propose','text':'本轮暂候消息，不采取新的行动。'}, seen)
            before = world.snapshot(); original = world._check_actor_execution; hits = []
            def guard(db, identity, person, permit, **kwargs):
                original(db, identity, person, permit, **kwargs)
                if 'actor' not in [r[1] for r in db.execute('PRAGMA database_list')]:
                    count = db.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%' AND receipt IS NOT NULL").fetchone()[0]
                    if count:
                        hits.append(count)
                        host._attempt.failure = ActorRunError('ACTOR_EXECUTION_CANCELLED')
                        host._attempt.stop.set()
            world._check_actor_execution = guard
            with self.assertRaisesRegex(Exception, 'ACTOR_EXECUTION_CANCELLED'):
                host.act(PERSON)
            self.assertEqual(hits, [2])
            self.assertEqual(world.snapshot(), before)
            with host._connect() as db:
                intent, state = db.execute('SELECT intent_id,state FROM actor_intents').fetchone()
                self.assertEqual(state, 'candidate')
            self.assertIsNone(world.receipt(PERSON, intent))
            with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%'").fetchone()[0], 0)
                self.assertEqual(db.execute("SELECT count(*) FROM dm_timeline WHERE kind='commit'").fetchone()[0], 0)
            world._check_actor_execution = original
            host._request = lambda payload: self.fail('candidate must not rerun SDK')
            host.dm_provider = lambda payload: self.fail('durable decision must not rerun DM')
            result = host.act(PERSON)
            self.assertEqual(result['intent_id'], intent)
            self.assertEqual(world.receipt(PERSON, intent), result['receipt'])
            with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone()[0], 3)

    def test_characterization_close_during_actual_submit_lock_wait(self):
        with self.pair() as (world, host):
            entered = threading.Event(); errors = []; original = world.submit
            def submit(*args, **kwargs):
                with self.locked(world):
                    entered.set()
                    return original(*args, **kwargs)
            world.submit = submit
            def run():
                try:
                    host.act(PERSON)
                except Exception as error:
                    errors.append(error)
            thread = threading.Thread(target=run, name='p022-deadline-host')
            thread.start(); self.assertTrue(entered.wait(2)); start = time.monotonic()
            host.close(); thread.join(2)
            self.assertLess(time.monotonic()-start, .7)
            self.assertFalse(thread.is_alive())
            self.assertEqual([error.code for error in errors], ['ACTOR_EXECUTION_CANCELLED'])
            with host._connect() as db:
                intent = db.execute('SELECT intent_id FROM actor_intents').fetchone()[0]
            self.assertIsNone(world.receipt(PERSON, intent))
            self.assertFalse([t for t in threading.enumerate() if t.name.startswith('actor-heartbeat-')])

    def test_characterization_nested_scopes_restore_and_concurrent_hosts_do_not_cross(self):
        with self.pair() as (world, host):
            other = 'person-country-1-king'
            first = world.acquire_actor_execution(PERSON, 'first')
            second = world.acquire_actor_execution(other, 'second')
            a, b = Attempt(first, ExecutionPolicy()), Attempt(second, ExecutionPolicy())
            try:
                with world._actor_attempt(a):
                    with self.assertRaisesRegex(ValueError, 'nested'):
                        with world._actor_attempt(b):
                            self.assertIs(world._scoped_attempt(), b)
                            raise ValueError('nested')
                    self.assertIs(world._scoped_attempt(), a)
                self.assertIsNone(world._scoped_attempt())
            finally:
                world.release_actor_execution(first); world.release_actor_execution(second)
            other_host = ActorHost(world, Path(host.db_path).with_name('other-actors.sqlite'))
            other_host._request = existing.ActorExecutionTests.result
            barrier = threading.Barrier(2); original = world.submit; results = []; errors = []
            def submit(person, intent, body, **kwargs):
                captured = world._scoped_attempt()
                self.assertEqual(captured.permit, kwargs['execution'])
                barrier.wait(2)
                self.assertIs(world._scoped_attempt(), captured)
                if person == PERSON:
                    captured.deadline = 0
                return original(person, intent, body, **kwargs)
            world.submit = submit
            def run(actor, person):
                try:
                    results.append(actor.act(person))
                except Exception as error:
                    errors.append(error)
            threads = [threading.Thread(target=run, args=(actor, person)) for actor, person in ((host, PERSON), (other_host, other))]
            try:
                for thread in threads: thread.start()
                for thread in threads: thread.join(4); self.assertFalse(thread.is_alive())
                self.assertEqual([error.code for error in errors], ['ACTOR_EXECUTION_DEADLINE'])
                self.assertEqual([row['person_id'] for row in results], [other])
                self.assertIsNone(world._scoped_attempt())
            finally:
                other_host.close()

    def test_characterization_unscoped_timeout_readonly_and_single_argument_mutate(self):
        with self.pair() as (world, host):
            with world._connection() as db:
                self.assertEqual(db.execute('PRAGMA busy_timeout').fetchone()[0], 30000)
            with world._connection(readonly=True) as db:
                self.assertEqual(db.execute('PRAGMA busy_timeout').fetchone()[0], 30000)
                with self.assertRaises(sqlite3.OperationalError):
                    db.execute('CREATE TABLE forbidden (x)')
            original = world._mutate; calls = []
            def mutate(operation):
                calls.append(True)
                return original(operation)
            world._mutate = mutate
            result = world.submit(PERSON, 'legacy-single', {'action':'wait'})
            self.assertEqual(calls, [True])
            self.assertEqual(world.receipt(PERSON, 'legacy-single'), result)
            # The public receipt remains readable even with a dead scoped attempt.
            permit = world.acquire_actor_execution(PERSON, 'expired-scope')
            attempt = Attempt(permit, ExecutionPolicy()); attempt.deadline = 0
            with self.assertRaisesRegex(Exception, 'ACTOR_EXECUTION_DEADLINE'):
                with world._actor_attempt(attempt):
                    world.receipt(PERSON, 'legacy-single')
            self.assertEqual(world.receipt(PERSON, 'legacy-single'), result)
            world.release_actor_execution(permit)

    def test_dm_provider_thread_inherits_original_scope_and_exits_bounded_read_wait(self):
        from app.scripted_dm import ScriptedDM
        with self.pair(execution_policy=ExecutionPolicy(act_deadline_seconds=.2)) as (world, host):
            host.adjudicator = Adjudicator(world)
            observed = []; provider_threads = []; done = threading.Event()
            def provider(payload):
                observed.append(world._scoped_attempt())
                provider_threads.append(threading.current_thread())
                try:
                    with self.locked(world):
                        return ScriptedDM(world)(payload)
                finally:
                    done.set()
            host.dm_provider = provider
            host._choose = lambda view, seen: ({'action':'propose','text':'本轮暂候消息，不采取新的行动。'}, seen)
            start = time.monotonic()
            try:
                with self.assertRaisesRegex(Exception, 'ACTOR_EXECUTION_DEADLINE'):
                    host.act(PERSON)
                self.assertLess(time.monotonic()-start, .7)
                self.assertTrue(done.wait(.1), 'cooperative real World reader must exit, not linger behind an unscoped 30s wait')
                self.assertEqual(len(observed), 1)
                self.assertIsNotNone(observed[0], 'ContextVar does not automatically propagate into a provider thread')
                original = observed[0]
                self.assertEqual(original.permit.person_id, PERSON)
                self.assertEqual(len(provider_threads), 1)
                self.assertIn(provider_threads[0], original.calls)
                # The reader's finally event precedes native thread exit. Prove
                # actual exit before admission, not an eagerly emptied registry.
                for thread in original.calls:
                    thread.join(.1)
                    self.assertFalse(thread.is_alive())
                self.assertFalse(original.thread.is_alive())
                self.assertEqual(getattr(original.failure, 'code', None), 'ACTOR_EXECUTION_DEADLINE')
                self.assertTrue(all(not thread.is_alive() for thread in host._pending_threads))
                for issuer, permit, attempt in host._lease_cleanups:
                    self.assertIs(issuer, world); self.assertIs(permit, original.permit); self.assertIs(attempt, original)
                with host._connect() as db:
                    intent = db.execute('SELECT intent_id FROM actor_intents').fetchone()[0]
                self.assertIsNone(world.receipt(PERSON, intent))
                # Ordinary admission must retire the original issuer's actual
                # cleanup before acquisition; no close/manual registry clearing.
                acquire = world.acquire_actor_execution; admissions = []
                def admitted(*args, **kwargs):
                    with closing(sqlite3.connect(world.db_path)) as db:
                        old = db.execute('SELECT epoch,owner_token FROM actor_execution_leases WHERE person_id=?', (PERSON,)).fetchone()
                    admissions.append((old, len(host._pending_threads), len(host._lease_cleanups), host.cleanup_pending))
                    return acquire(*args, **kwargs)
                world.acquire_actor_execution = admitted
                host.dm_provider = ScriptedDM(world)
                try:
                    result = host.act(PERSON)
                finally:
                    world.acquire_actor_execution = acquire
                self.assertEqual(admissions, [((original.permit.epoch, None), 0, 0, False)])
                self.assertEqual(result['intent_id'], intent)
                self.assertIsNotNone(world.receipt(PERSON, intent))
                self.assertEqual(len(observed), 1)
                self.assertFalse(host._pending_threads)
                self.assertFalse(host._lease_cleanups)
                self.assertFalse(host.cleanup_pending)
                self.assertFalse(host._closed)
            finally:
                self.assertTrue(done.wait(2))
                for thread in provider_threads + list(host._pending_threads):
                    thread.join(2)
                self.assertFalse([t for t in threading.enumerate() if t.name.startswith('actor-provider-')])

    def test_characterization_busy_is_bounded_and_other_sql_errors_are_not_masked(self):
        with self.pair() as (world, host):
            permit = world.acquire_actor_execution(PERSON, 'busy-control')
            attempt = Attempt(permit, ExecutionPolicy())
            try:
                with self.locked(world), world._actor_attempt(attempt):
                    start = time.monotonic()
                    with self.assertRaises(Exception) as caught:
                        world.submit(PERSON, 'busy-control', {'action':'wait'}, execution=permit)
                    self.assertEqual(caught.exception.code, 'EXECUTION_STORAGE_BUSY')
                    self.assertLess(time.monotonic()-start, .7)
                self.assertIsNone(world.receipt(PERSON, 'busy-control'))
                with world._actor_attempt(attempt):
                    with self.assertRaises(sqlite3.OperationalError) as caught:
                        world._mutate(lambda state, db: db.execute('SELECT * FROM deliberately_missing_table'), execution=permit, person_id=PERSON)
                    self.assertEqual(caught.exception.sqlite_errorcode, sqlite3.SQLITE_ERROR)
            finally:
                world.release_actor_execution(permit)

    def test_world_and_adjudication_read_waits_share_remaining_budget(self):
        for kind in ('snapshot', 'observe', 'receipt', 'preview', 'adjudication'):
            with self.subTest(kind=kind), self.pair() as (world, host):
                adj = Adjudicator(world)
                permit = world.acquire_actor_execution(PERSON, 'read-scope')
                attempt = Attempt(permit, ExecutionPolicy(act_deadline_seconds=.15))
                operation = {
                    'snapshot': world.snapshot,
                    'observe': lambda: world.observe(PERSON),
                    'receipt': lambda: world.receipt(PERSON, 'read-wait'),
                    'preview': lambda: world.preview(PERSON, 'read-wait', {'action':'wait'}, execution=permit),
                    'adjudication': lambda: adj.history(PERSON, 'read-wait'),
                }[kind]
                try:
                    with self.locked(world), world._actor_attempt(attempt):
                        start = time.monotonic()
                        with self.assertRaisesRegex(Exception, 'ACTOR_EXECUTION_DEADLINE'):
                            operation()
                        self.assertLess(time.monotonic()-start, .7)
                finally:
                    world.release_actor_execution(permit)
                self.assertIsNone(world.receipt(PERSON, 'read-wait'))

    def test_submit_effect_precommit_deadline_and_cancel_rollback_and_same_id_recovery(self):
        for reason in ('ACTOR_EXECUTION_DEADLINE', 'ACTOR_EXECUTION_CANCELLED'):
            with self.subTest(reason=reason), self.pair() as (world, host):
                before = world.snapshot()
                original = world._check_actor_execution
                hits = []
                def guard(db, identity, person, permit, **kwargs):
                    original(db, identity, person, permit, **kwargs)
                    if 'actor' not in [r[1] for r in db.execute('PRAGMA database_list')]:
                        row = db.execute('SELECT receipt FROM intents WHERE person=? AND receipt IS NOT NULL', (PERSON,)).fetchone()
                        if row:
                            hits.append(row[0])
                            if reason.endswith('DEADLINE'):
                                host._attempt.deadline = 0  # trusted monotonic budget, not machine clock
                            else:
                                host._attempt.failure = ActorRunError(reason)
                                host._attempt.stop.set()
                world._check_actor_execution = guard
                with self.assertRaisesRegex(Exception, reason):
                    host.act(PERSON)
                self.assertEqual(len(hits), 1)
                with host._connect() as db:
                    intent, state, candidate = db.execute('SELECT intent_id,state,candidate FROM actor_intents').fetchone()
                    self.assertEqual(state, 'candidate'); self.assertIsNotNone(candidate)
                self.assertIsNone(world.receipt(PERSON, intent), 'deadline/cancel must roll back the actual receipt and effect')
                self.assertEqual(world.snapshot(), before)
                world._check_actor_execution = original
                host._request = lambda payload: self.fail('durable candidate recovery must not call SDK')
                recovered = host.act(PERSON)
                self.assertEqual(recovered['intent_id'], intent)
                self.assertEqual(world.receipt(PERSON, intent), recovered['receipt'])
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone()[0], 1)
                with host._connect() as db:
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0], 1)
                self.assertFalse([t for t in threading.enumerate() if t.name.startswith('actor-heartbeat-')])
