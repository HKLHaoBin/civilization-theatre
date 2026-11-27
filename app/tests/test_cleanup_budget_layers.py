"""R3 budget-layer regression; SDK-free explicit fixtures, no Case imports."""
from pathlib import Path
import json
import threading
import time
import unittest
from app.actor_execution import EntryBudget, ExecutionPolicy
from app.tests import test_persistent_close_cleanup as fixtures


class CleanupBudgetLayersTests(unittest.TestCase):
    def test_no_cleanup_outer_io_does_not_shorten_ordinary_child(self):
        with fixtures.fixture() as (world, host, path):
            outer = EntryBudget(ExecutionPolicy(act_deadline_seconds=.8,
                busy_budget_seconds=.7), threading.Event())
            attempts = []
            entries = []
            acquire = world.acquire_actor_execution
            def take(*args, **kwargs):
                entries.append(world._scoped_entry_budget())
                return acquire(*args, **kwargs)
            def act(*args, **kwargs):
                attempts.append(host._attempt)
                return {'intent_id': 'R3-ordinary-no-domain-write'}
            world.acquire_actor_execution = take
            host._act = act
            self.assertEqual(host._lease_cleanups, [])
            with world._entry_budget(outer):
                host.act(fixtures.PERSON)
                self.assertIs(world._scoped_entry_budget(), outer)
            self.assertEqual(len(entries), 1)
            self.assertEqual(attempts[0].deadline, entries[0].deadline)
            remaining = attempts[0].deadline - time.monotonic()
            print(json.dumps({'R3_case': 'no_cleanup_no_capture',
                'entry_deadline': entries[0].deadline,
                'outer_deadline': outer.deadline,
                'attempt_deadline': attempts[0].deadline,
                'child_remaining': remaining,
                'sql_lease': fixtures.lease(world)}))
            self.assertGreater(remaining, 118)
            self.assertNotIn(outer, entries[0].captured)
            self.assertFalse(host.cleanup_pending)
            self.assertIsNone(fixtures.lease(world)[1])

    def test_plain_no_outer_no_capture_characterization(self):
        with fixtures.fixture() as (world, host, path):
            attempts = []
            host._act = lambda *a, **k: attempts.append(host._attempt) or {'intent_id': 'R3-plain'}
            self.assertIsNone(world._scoped_entry_budget())
            host.act(fixtures.PERSON)
            self.assertGreater(attempts[0].deadline - time.monotonic(), 118)
            self.assertIsNone(world._scoped_entry_budget())
            self.assertFalse(host.cleanup_pending)
            self.assertIsNone(fixtures.lease(world)[1])

    def test_quick_true_cas_tombstone_successor_keep_predrain_outer_characterization(self):
        for mode in ('true_cas', 'tombstone', 'successor'):
            with self.subTest(mode=mode), fixtures.fixture() as (world, host, path):
                old = fixtures.run_pending(world, host)
                successor = None
                if mode != 'true_cas':
                    self.assertTrue(world.release_actor_execution(old))
                if mode == 'successor':
                    successor = world.acquire_actor_execution(fixtures.PERSON, 'R3-real-successor')
                original_row = fixtures.lease(world)
                actual_release = world.release_actor_execution
                actual_acquire = world.acquire_actor_execution
                entries, observations, attempts = [], [], []
                outer = EntryBudget(ExecutionPolicy(act_deadline_seconds=.8,
                    busy_budget_seconds=.7), threading.Event())
                def release(value):
                    if value == old:
                        entries.append(world._scoped_entry_budget())
                    return actual_release(value)
                def acquire(*a, **k):
                    entries.append(world._scoped_entry_budget())
                    observations.append((len(host._lease_cleanups), fixtures.lease(world)))
                    return actual_acquire(*a, **k)
                host._act = lambda *a, **k: attempts.append(host._attempt) or {'intent_id': 'R3-quick'}
                world.release_actor_execution = release
                world.acquire_actor_execution = acquire
                try:
                    with world._entry_budget(outer):
                        host.act('person-country-1-marshal')
                    self.assertEqual(len(entries), 2)
                    self.assertIs(entries[0], entries[1])
                    self.assertIn(outer, entries[0].captured)
                    self.assertEqual(entries[0].deadline, outer.deadline)
                    self.assertEqual(entries[0].busy_deadline, outer.busy_deadline)
                    self.assertEqual(attempts[0].deadline, outer.deadline)
                    self.assertEqual(observations[0][0], 0)
                    expected_row = (old.epoch, None) if mode == 'true_cas' else original_row
                    self.assertEqual(observations[0][1], expected_row)
                    self.assertEqual(fixtures.lease(world), expected_row)
                    self.assertFalse(host.cleanup_pending)
                    print(json.dumps({'R3_case': 'quick_predrain_' + mode,
                        'same_entry': entries[0] is entries[1],
                        'outer_in_captured': outer in entries[0].captured,
                        'empty_at_acquire': observations[0][0] == 0,
                        'entry_deadline': entries[0].deadline,
                        'attempt_deadline': attempts[0].deadline,
                        'outer_deadline': outer.deadline,
                        'busy_deadline': entries[0].busy_deadline,
                        'old_lease_after': fixtures.lease(world)}))
                finally:
                    world.release_actor_execution = actual_release
                    if successor is not None:
                        self.assertTrue(actual_release(successor))

    def test_current_outer_actor_stop_and_expiry_reject_before_cleanup_characterization(self):
        from app.actor_execution import ActorRunError, Attempt
        for mode in ('cancel', 'deadline'):
            with self.subTest(mode=mode), fixtures.fixture() as (world, host, path):
                old = fixtures.run_pending(world, host)
                saved = host._lease_cleanups[0]
                outer_permit = world.acquire_actor_execution('person-country-1-marshal', 'R3-outer-actor')
                outer = Attempt(outer_permit, ExecutionPolicy(act_deadline_seconds=.035))
                actual_release, actual_acquire = world.release_actor_execution, world.acquire_actor_execution
                releases, acquires, executions = [], [], []
                def release(value):
                    releases.append(value)
                    return actual_release(value)
                def acquire(*a, **k):
                    acquires.append(True)
                    return actual_acquire(*a, **k)
                world.release_actor_execution, world.acquire_actor_execution = release, acquire
                host._act = lambda *a, **k: executions.append(True) or {}
                try:
                    with world._actor_attempt(outer):
                        if mode == 'cancel':
                            outer.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'))
                        else:
                            threading.Event().wait(max(0, outer.deadline - time.monotonic()) + .005)
                        with self.assertRaises(ActorRunError) as caught:
                            host.act(fixtures.PERSON)
                    self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_CANCELLED' if mode == 'cancel' else 'ACTOR_EXECUTION_DEADLINE')
                    self.assertEqual((releases, acquires, executions), ([], [], []))
                    self.assertIs(host._lease_cleanups[0], saved)
                    self.assertEqual(fixtures.lease(world), (old.epoch, old.owner_token))
                    print(json.dumps({'R3_case': 'current_outer_actor_' + mode,
                        'error': caught.exception.code, 'CAS': 0, 'acquire': 0, 'execute': 0}))
                finally:
                    world.release_actor_execution, world.acquire_actor_execution = actual_release, actual_acquire
                    actual_release(outer_permit)

    def test_maintenance_only_live_obligation_blocks_admission_until_join_characterization(self):
        from app.actor_execution import ActorRunError
        with fixtures.fixture() as (world, host, path):
            entered, unblock = threading.Event(), threading.Event()
            releases, acquires, attempts = [], [], []
            actual_release, actual_acquire, actual_renew = world.release_actor_execution, world.acquire_actor_execution, world.renew_actor_execution
            def renew(*a, **k):
                entered.set()
                if not unblock.wait(8):
                    raise RuntimeError('R3 maintenance observation barrier expired')
                raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
            def release(value):
                releases.append(value)
                return actual_release(value)
            def acquire(*a, **k):
                acquires.append(True)
                return actual_acquire(*a, **k)
            def act(*a, **k):
                attempts.append(host._attempt)
                if not entered.wait(3):
                    raise RuntimeError('R3 maintenance did not enter')
                return {'intent_id': 'R3-maintenance-only'}
            host.execution_policy = ExecutionPolicy(renew_interval_seconds=.02)
            world.renew_actor_execution = renew
            world.release_actor_execution, world.acquire_actor_execution = release, acquire
            host._act = act
            try:
                host.act(fixtures.PERSON)
                original = attempts[0]
                self.assertTrue(original.thread.is_alive())
                self.assertEqual(original.calls, [])
                self.assertEqual(releases, [])
                self.assertEqual(len(acquires), 1)
                with self.assertRaises(ActorRunError) as caught:
                    host.act(fixtures.PERSON)
                self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_CLEANUP_PENDING')
                self.assertEqual(releases, [])
                self.assertEqual(len(acquires), 1)
                self.assertEqual(len(attempts), 1)
                unblock.set()
                original.thread.join(3)
                self.assertFalse(original.thread.is_alive())
                world.renew_actor_execution = actual_renew
                host._act = lambda *a, **k: {'intent_id': 'R3-maintenance-joined'}
                host.act(fixtures.PERSON)
                self.assertEqual(len(acquires), 2)
                self.assertEqual(releases[0], original.permit)
                self.assertFalse(host.cleanup_pending)
                self.assertIsNone(fixtures.lease(world)[1])
                print(json.dumps({'R3_case': 'maintenance_only_admission',
                    'CAS_while_live': 0, 'acquire_while_live': 0,
                    'original_maintenance_joined': not original.thread.is_alive(),
                    'cleanup_pending_after_join_and_act': host.cleanup_pending}))
            finally:
                unblock.set()
                for attempt in attempts:
                    attempt.thread.join(4)
                world.release_actor_execution, world.acquire_actor_execution, world.renew_actor_execution = actual_release, actual_acquire, actual_renew

    def test_later_act_cleanup_cannot_upgrade_original_unsafe_capture_characterization(self):
        from contextvars import Context
        from app.tests import test_dispatch_ack_continued as durable
        from app.actor_execution import ActorRunError, dispatch_capture_scope
        from app.coordinator_scheduler import prove_dispatch_ack
        with durable.prepared() as (world, host, owner, requests, root, ref, index):
            actual_release = world.release_actor_execution
            world.release_actor_execution = lambda permit: False
            with dispatch_capture_scope(world, host, ref, root, roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                first = host.act(ref.person_id, pause_on_failure=True, coordinator=root.permit, activation=ref)
                cap = slot.capture()
                self.assertFalse(cap.lease_released)
                self.assertTrue(cap.cleanup_pending)
                world.release_actor_execution = actual_release
                errors, results = [], []
                def later_caller():
                    try:
                        with owner._owner_scope(root):
                            results.append(host.act(ref.person_id, pause_on_failure=True,
                                coordinator=root.permit, activation=ref))
                    except BaseException as error:
                        errors.append(error)
                # A distinct legitimate caller, without inheriting the old dispatch slot.
                worker = threading.Thread(target=lambda: Context().run(later_caller), name='R3-owned-later-act')
                worker.start()
                worker.join(5)
                self.assertFalse(worker.is_alive())
                self.assertEqual(errors, [])
                self.assertEqual(results[0]['intent_id'], first['intent_id'])
                self.assertEqual(len(requests), 1)
                self.assertFalse(host.cleanup_pending)
                self.assertFalse(cap.lease_released)
                self.assertTrue(cap.cleanup_pending)
                before = (durable.logical(world.db_path), durable.logical(host.db_path))
                with self.assertRaises(ActorRunError):
                    prove_dispatch_ack(world, slot=slot, capture=cap)
                self.assertEqual((durable.logical(world.db_path), durable.logical(host.db_path)), before)
                print(json.dumps({'R3_case': 'late_act_original_cap_unsafe',
                    'same_intent': results[0]['intent_id'] == first['intent_id'],
                    'requests': len(requests), 'host_pending': host.cleanup_pending,
                    'original_cap_lease_released': cap.lease_released,
                    'original_cap_pending': cap.cleanup_pending}))

    def test_root_sql_ttl_expiry_is_readonly_gate_despite_live_object_characterization(self):
        from contextlib import closing
        import sqlite3
        from app.tests import test_dispatch_ack_continued as durable
        from app.actor_execution import dispatch_capture_scope
        from app.coordinator_scheduler import prove_dispatch_ack
        from app.world import WorldError
        with durable.prepared() as (world, host, owner, requests, root, ref, index):
            with dispatch_capture_scope(world, host, ref, root, roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id, pause_on_failure=True, coordinator=root.permit, activation=ref)
                cap = slot.capture()
                with closing(sqlite3.connect(world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    expiry = db.execute('SELECT expires_wall_ns FROM coordinator_execution_leases').fetchone()[0]
                clock = world._execution_clock_ns
                world._execution_clock_ns = lambda: expiry
                before = (durable.logical(world.db_path), durable.logical(host.db_path))
                raw = (world.db_path.read_bytes(), Path(host.db_path).read_bytes())
                try:
                    self.assertIsNone(root.failure)
                    self.assertTrue(root.thread.is_alive())
                    with self.assertRaises(WorldError) as caught:
                        prove_dispatch_ack(world, slot=slot, capture=cap)
                    self.assertEqual(caught.exception.code, 'COORDINATOR_EXECUTION_EXPIRED')
                    self.assertIsNone(root.failure)
                    self.assertEqual((durable.logical(world.db_path), durable.logical(host.db_path)), before)
                    self.assertEqual((world.db_path.read_bytes(), Path(host.db_path).read_bytes()), raw)
                    self.assertEqual(len(requests), 1)
                    print(json.dumps({'R3_case': 'root_SQL_TTL', 'trusted_now': expiry,
                        'error': caught.exception.code, 'root_python_failure': None,
                        'both_db_bytes_unchanged': True}))
                finally:
                    world._execution_clock_ns = clock

    def test_actual_actor_wall_ttl_late_reply_has_no_candidate_effect_or_pause_characterization(self):
        from contextlib import closing
        import sqlite3
        from app.tests import test_actor_execution as request_fixtures
        from app.world import WorldError
        with fixtures.fixture() as (world, host, path):
            clock = world._execution_clock_ns
            observations = []
            def request(payload):
                with closing(sqlite3.connect(world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    expiry = db.execute('SELECT expires_wall_ns FROM actor_execution_leases WHERE person_id=?', (fixtures.PERSON,)).fetchone()[0]
                observations.append(expiry)
                world._execution_clock_ns = lambda: expiry
                return request_fixtures.ActorExecutionTests.result(payload)
            host._request = request
            host._choose = lambda view, seen: ({'action': 'wait'}, seen)
            before_world = world.snapshot()
            try:
                with self.assertRaises(WorldError) as caught:
                    host.act(fixtures.PERSON, pause_on_failure=True)
                self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_EXPIRED')
                self.assertEqual(len(observations), 1)
                with closing(sqlite3.connect(Path(host.db_path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    self.assertEqual(db.execute('SELECT state,candidate,metrics,receipt FROM actor_intents').fetchall(), [('prepared', None, None, None)])
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_calls').fetchone()[0], 0)
                self.assertEqual(world.snapshot(), before_world)
                self.assertIsNone(fixtures.lease(world)[1])
                self.assertFalse(host.cleanup_pending)
                print(json.dumps({'R3_case': 'actor_actual_wall_TTL_late_reply',
                    'error': caught.exception.code, 'candidate': None,
                    'actor_calls': 0, 'world_unchanged': True, 'original_CAS_released': True}))
            finally:
                world._execution_clock_ns = clock
