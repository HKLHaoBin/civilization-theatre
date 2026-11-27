"""P023b first vertical behavior: actual World/private SQLite, no model."""
from contextlib import closing, contextmanager
from unittest.mock import patch
import hashlib
import os
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.world import DAY, WorldStore

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class CoordinatorWindowTests(unittest.TestCase):
    @contextmanager
    def fixture(self, *, registered=True):
        with tempfile.TemporaryDirectory(prefix='p023b-ledger-', dir=SCRATCH) as directory:
            root = Path(directory)
            world = WorldStore(root/'world.sqlite')
            world.initialize()
            host = ActorHost(world, root/'actors.sqlite')
            owner = world.acquire_coordinator_execution('ledger-owner', ttl_seconds=3600)
            try:
                world.control('resume', coordinator=owner)
                snapshot = world.snapshot()
                version = {k:snapshot[k] for k in ('world_id','revision')}
                if registered:
                    world.register_scheduler(coordinator=owner, expected_version=version,
                                             boundary=dict(kind='pristine', **version), actor_db_path=host.db_path)
                yield world, host, owner, version, root
            finally:
                host.close()
                world.release_coordinator_execution(owner)

    def assertCode(self, code, function, *args, **kwargs):
        from app.world import WorldError
        with self.assertRaises(WorldError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_next_is_readonly_and_returns_first_pending_without_claim(self):
        with self.fixture() as (world, host, owner, version, root):
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            next_activation = getattr(world, 'next_activation', None)
            self.assertTrue(callable(next_activation), 'read-only scheduler next is missing')
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            first = next_activation(window['window_id'], coordinator=owner)
            self.assertEqual(first['person_id'], window['roster'][0])
            self.assertEqual(first['roster_index'], 0)
            self.assertEqual(first['phase'], 'pending')
            self.assertIsNone(first['intent_id'])
            self.assertEqual(first['private_closed'], 0)
            self.assertEqual(next_activation(window['window_id'], coordinator=owner), first)
            self.assertEqual({p:p.read_bytes() for p in before}, before)

    def test_complete_requires_every_terminal_private_closed_and_preserves_queue(self):
        with self.fixture() as (world, host, owner, version, root):
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            complete = getattr(world, 'complete_window', None)
            self.assertTrue(callable(complete), 'terminal window completion is missing')
            before = world.snapshot()
            self.assertCode('WINDOW_INCOMPLETE', complete, window['window_id'], coordinator=owner)
            self.assertEqual(world.snapshot(), before)
            from app.tests.activation_fixtures import close_activations
            results = close_activations(world, host, owner, window)
            self.assertEqual(len(results), len(window['roster']))
            self.assertEqual(len({r['intent_id'] for r in results}), len(results))
            with closing(sqlite3.connect(world.db_path)) as db:
                actual = db.execute('SELECT a.intent_id,i.receipt FROM coordinator_activations a JOIN intents i ON i.id=a.intent_id WHERE a.window_id=?', (window['window_id'],)).fetchall()
                self.assertEqual({r[0] for r in actual}, {r['intent_id'] for r in results})
                self.assertTrue(all(r[1] for r in actual))
                db.execute('UPDATE coordinator_activations SET private_closed=0')
                db.commit()
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM actor_session_heads WHERE version=1').fetchone()[0],len(results))
            after_actors = world.snapshot()
            self.assertEqual(after_actors['queue'], before['queue'])
            before = after_actors
            self.assertCode('WINDOW_INCOMPLETE', complete, window['window_id'], coordinator=owner)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('UPDATE coordinator_activations SET private_closed=1')
                db.commit()
            self.assertIsNone(world.next_activation(window['window_id'], coordinator=owner))
            closed = complete(window['window_id'], coordinator=owner)
            self.assertEqual(closed['state'], 'completed')
            self.assertEqual(closed['completed_revision'], before['revision'])
            self.assertEqual(world.snapshot(), before)
            self.assertEqual(complete(window['window_id'], coordinator=owner), closed)

    def test_registration_permanently_disables_public_advance(self):
        with self.fixture() as (world, host, owner, version, root):
            before = world.snapshot()
            self.assertCode('ACTIVATION_REQUIRED', world.advance, DAY, coordinator=owner)
            self.assertEqual(world.snapshot(), before)
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            after = world.snapshot()
            self.assertCode('WINDOW_INCOMPLETE', world.advance, 2*DAY, coordinator=owner)
            self.assertEqual(world.snapshot(), after)
            world.release_coordinator_execution(owner)
            successor = world.acquire_coordinator_execution('successor')
            try:
                self.assertCode('WINDOW_INCOMPLETE', world.advance, 2*DAY, coordinator=successor)
            finally:
                world.release_coordinator_execution(successor)

    def test_registered_root_cannot_apply_role_effect_through_generic_mutate(self):
        with self.fixture() as (world, host, owner, version, root):
            before = world.snapshot()
            person = before['persons'][0]['id']
            def bypass(state, db):
                world._apply_action(state, state['persons'][0], 'generic-backdoor', {'action':'investigate'}, '成功')
            with world._coordinator_scope(owner):
                self.assertCode('ACTIVATION_REQUIRED', world._mutate, bypass)
            self.assertEqual(world.snapshot(), before)
            self.assertCode('ACTIVATION_REQUIRED', world.acquire_actor_execution,
                            person, 'ledger-actor', coordinator=owner)
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            row = world.next_activation(window['window_id'], coordinator=owner)
            from app.coordinator_scheduler import ActivationRef
            ref = ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
            permit = world.acquire_actor_execution(person, 'ledger-actor', coordinator=owner, activation=ref)
            try:
                with world._coordinator_scope(owner):
                    self.assertCode('ACTIVATION_REQUIRED', world.submit, person, 'not-an-activation', {'action':'wait'}, execution=permit)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 0)
            finally:
                world.release_actor_execution(permit)
            world.edit_card(person, {'mission':'合法前向卡'}, coordinator=owner)
            world.control('pause', coordinator=owner)
            self.assertEqual(world.snapshot()['status'], 'user_paused')

    def test_recovery_rejects_corrupted_frozen_roster_and_activation_links(self):
        corruptions = [
            "UPDATE coordinator_windows SET roster_sha256='wrong'",
            'DELETE FROM coordinator_activations WHERE roster_index=0',
            "UPDATE coordinator_activations SET person_id='unknown-person' WHERE roster_index=0",
            'UPDATE coordinator_windows SET advanced_revision=base_revision',
            'UPDATE coordinator_windows SET target_time=target_time+1',
            "UPDATE coordinator_activations SET intent_id='unprepared' WHERE roster_index=0",
        ]
        for corruption in corruptions:
            with self.subTest(corruption=corruption), self.fixture() as (world, host, owner, version, root):
                window = world.open_or_recover_window(coordinator=owner, expected_version=version)
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute(corruption)
                    db.commit()
                before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window, coordinator=owner, expected_version=version)
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.next_activation, window['window_id'], coordinator=owner)
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.complete_window, window['window_id'], coordinator=owner)
                self.assertEqual({p:p.read_bytes() for p in before}, before)

    def test_new_worldstore_and_deferred_host_can_recover_only_original_private_file(self):
        with self.fixture() as (world, host, owner, version, root):
            opened = world.open_or_recover_window(coordinator=owner, expected_version=version)
            host.close()
            world.release_coordinator_execution(owner)
            reopened = WorldStore(world.db_path)
            reopened.initialize()
            replacement = ActorHost(reopened, root/'actors.sqlite')
            successor = reopened.acquire_coordinator_execution('reopened-owner')
            try:
                before = reopened.snapshot()
                recovered = reopened.open_or_recover_window(coordinator=successor, expected_version=version)
                self.assertEqual(recovered['window_id'], opened['window_id'])
                self.assertEqual(reopened.snapshot(), before)
                replacement.close()
                substituted = ActorHost(reopened, root/'empty-substitute.sqlite')
                try:
                    with reopened._coordinator_scope(successor):
                        substituted._initialize_private()
                    self.assertCode('WINDOW_RECOVERY_UNPROVEN', reopened.open_or_recover_window,
                                    coordinator=successor, expected_version=version)
                    self.assertEqual(reopened.snapshot(), before)
                finally:
                    substituted.close()
            finally:
                replacement.close()
                reopened.release_coordinator_execution(successor)

    def test_registered_operations_recheck_actual_private_binding_even_initialize_transaction(self):
        with self.fixture() as (world, host, owner, version, root):
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            with closing(sqlite3.connect(host.db_path)) as db:
                db.execute("UPDATE actor_world_binding SET world_id='other-world'")
                db.commit()
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window, coordinator=owner, expected_version=version)
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.next_activation, window['window_id'], coordinator=owner)
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.complete_window, window['window_id'], coordinator=owner)
            current = {k:world.snapshot()[k] for k in ('world_id','revision')}
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.register_scheduler, coordinator=owner,
                            expected_version=current, boundary=dict(kind='pristine', **current), actor_db_path=host.db_path)
            self.assertEqual({p:p.read_bytes() for p in before}, before)

    def test_inspection_requires_actual_source_and_never_creates_missing_private(self):
        with self.fixture(registered=False) as (world, host, owner, version, root):
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            inspected = world.inspect_scheduler_boundary(actor_db_path=host.db_path)
            self.assertTrue(inspected['eligible'])
            self.assertEqual(inspected['boundary'], dict(kind='pristine', **version))
            self.assertEqual({p:p.read_bytes() for p in before}, before)
            isolated = WorldStore(world.db_path)
            self.assertFalse(isolated.inspect_scheduler_boundary(actor_db_path=host.db_path)['eligible'])
            missing = root/'deferred-missing.sqlite'
            deferred = ActorHost(world, missing)
            try:
                self.assertTrue(deferred._bootstrap_pending)
                self.assertFalse(missing.exists())
                self.assertFalse(world.inspect_scheduler_boundary(actor_db_path=missing)['eligible'])
                self.assertFalse(world.inspect_scheduler_boundary(actor_db_path=host.db_path)['eligible'])
                self.assertFalse(missing.exists())
                self.assertEqual({p:p.read_bytes() for p in before}, before)
            finally:
                deferred.close()

    def test_conflicting_actual_hosts_fail_closed_not_last_wins_or_caller_filtered(self):
        with self.fixture(registered=False) as (world, host, owner, version, root):
            other = ActorHost(world, root/'other-empty.sqlite')
            try:
                with world._coordinator_scope(owner):
                    other._initialize_private()
                before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path), Path(other.db_path))}
                for path in (host.db_path, other.db_path):
                    self.assertFalse(world.inspect_scheduler_boundary(actor_db_path=path)['eligible'])
                    self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.register_scheduler, coordinator=owner,
                                    expected_version=version, boundary=dict(kind='pristine', **version), actor_db_path=path)
                self.assertEqual({p:p.read_bytes() for p in before}, before)
            finally:
                other.close()
            self.assertTrue(world.inspect_scheduler_boundary(actor_db_path=host.db_path)['eligible'])
            self.assertFalse(world.inspect_scheduler_boundary(actor_db_path=other.db_path)['eligible'])
            duplicate = ActorHost(world, host.db_path)
            try:
                self.assertTrue(world.inspect_scheduler_boundary(actor_db_path=host.db_path)['eligible'])
            finally:
                duplicate.close()

    def test_source_path_and_file_replacement_are_readonly_rejections(self):
        for change in ('path', 'file', 'authority', 'storage'):
            with self.subTest(change=change), self.fixture(registered=False) as (world, host, owner, version, root):
                original_path = host.db_path
                original_authority = host._authority
                original_storage = host._storage
                path = Path(host.db_path)
                backup = root/'original-actors.sqlite'
                try:
                    if change == 'path':
                        host.db_path = str(root/'different.sqlite')
                    elif change == 'file':
                        path.rename(backup)
                        path.write_bytes(backup.read_bytes())
                    elif change == 'authority':
                        host._authority = WorldStore(world.db_path)
                    else:
                        host._storage = None
                    before = world.db_path.read_bytes()
                    self.assertFalse(world.inspect_scheduler_boundary(actor_db_path=original_path)['eligible'])
                    # Preserve the original P022 path/file authority diagnostic.
                    expected = 'EXECUTION_WORLD_MISMATCH' if change in ('path','file') else 'WINDOW_RECOVERY_UNPROVEN'
                    self.assertCode(expected, world.register_scheduler, coordinator=owner,
                                    expected_version=version, boundary=dict(kind='pristine', **version), actor_db_path=original_path)
                    self.assertEqual(world.db_path.read_bytes(), before)
                    self.assertFalse((root/'different.sqlite').exists())
                finally:
                    host.db_path = original_path
                    host._authority = original_authority
                    host._storage = original_storage
                    if backup.exists():
                        path.unlink()
                        backup.rename(path)

    def test_genesis_does_not_backfill_real_legacy_time_zero_world(self):
        import importlib.util
        frozen = Path(__file__).resolve().parents[1]/'evidence/candidates/p02-3a/app/world.py'
        spec = importlib.util.spec_from_file_location('app._p023b_test_legacy_world', frozen)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory(prefix='p023b-legacy-genesis-', dir=SCRATCH) as directory:
            root = Path(directory)
            legacy = module.WorldStore(root/'world.sqlite')
            legacy.initialize()
            before = legacy.db_path.read_bytes()
            world = WorldStore(legacy.db_path)
            world.initialize()
            self.assertEqual(world.db_path.read_bytes(), before)
            host = ActorHost(world, root/'actors.sqlite')
            try:
                pair = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
                result = world.inspect_scheduler_boundary(actor_db_path=host.db_path)
                self.assertFalse(result['eligible'])
                self.assertEqual(result['reason'], 'WINDOW_RECOVERY_UNPROVEN')
                self.assertEqual({p:p.read_bytes() for p in pair}, pair)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='scheduler_genesis'").fetchone())
            finally:
                host.close()

    def test_open_atomically_rolls_back_snapshot_queue_window_and_all_activations(self):
        with self.fixture() as (world, host, owner, version, root):
            encode = world._encode_state
            injected = []
            def inject(db, state):
                db.execute("CREATE TEMP TRIGGER p023b_injected_failure BEFORE INSERT ON main.coordinator_activations WHEN NEW.roster_index=1 BEGIN SELECT RAISE(ABORT, 'injected-ledger-failure'); END")
                injected.append((
                    db.execute("SELECT count(*) FROM temp.sqlite_master WHERE type='trigger' AND name='p023b_injected_failure'").fetchone()[0],
                    db.execute("SELECT count(*) FROM main.sqlite_master WHERE type='trigger' AND name='p023b_injected_failure'").fetchone()[0]))
                return encode(db, state)
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            snapshot = world.snapshot()
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'injected-ledger-failure'):
                with patch.object(world, '_encode_state', inject):
                    world.open_or_recover_window(coordinator=owner, expected_version=version)
            self.assertEqual(injected, [(1, 0)])
            self.assertEqual(world.snapshot(), snapshot)
            self.assertEqual({p:p.read_bytes() for p in before}, before)
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM coordinator_windows').fetchone()[0], 0)
                self.assertEqual(db.execute('SELECT count(*) FROM coordinator_activations').fetchone()[0], 0)
                self.assertIsNone(db.execute("SELECT 1 FROM main.sqlite_master WHERE type='trigger' AND name='p023b_injected_failure'").fetchone())
                self.assertIsNone(db.execute("SELECT 1 FROM temp.sqlite_master WHERE type='trigger' AND name='p023b_injected_failure'").fetchone())
            opened = world.open_or_recover_window(coordinator=owner, expected_version=version)
            self.assertEqual(opened['ordinal'], 1)
            self.assertEqual(world.snapshot()['time'], DAY)

    def test_version_shape_world_and_new_window_revision_gates_leave_no_window(self):
        with self.fixture() as (world, host, owner, version, root):
            before = world.db_path.read_bytes()
            for malformed in (None, {}, dict(version, extra=1), dict(version, revision=True),
                              dict(version, revision=0), dict(version, revision=2**63), dict(version, revision=1.0)):
                self.assertCode('INVALID_BASE_VERSION', world.open_or_recover_window,
                                coordinator=owner, expected_version=malformed)
            self.assertCode('WORLD_MISMATCH', world.open_or_recover_window,
                            coordinator=owner, expected_version=dict(version, world_id='other'))
            self.assertCode('WINDOW_VERSION_CONFLICT', world.open_or_recover_window,
                            coordinator=owner, expected_version=dict(version, revision=1))
            self.assertEqual(world.db_path.read_bytes(), before)
            opened = world.open_or_recover_window(coordinator=owner, expected_version=version)
            for malformed in (dict(version, revision=True), dict(version, extra=1)):
                self.assertCode('INVALID_BASE_VERSION', world.open_or_recover_window,
                                coordinator=owner, expected_version=malformed)
            self.assertCode('WORLD_MISMATCH', world.open_or_recover_window,
                            coordinator=owner, expected_version=dict(version, world_id='other'))
            self.assertEqual(world.open_or_recover_window(coordinator=owner, expected_version=version)['window_id'], opened['window_id'])

    def test_durable_registration_revalidates_schema_evidence_and_integer_identity(self):
        corruptions = [
            "UPDATE coordinator_scheduler_registration SET evidence_sha256='wrong'",
            'UPDATE coordinator_scheduler_registration SET boundary_revision=boundary_revision+1',
            'UPDATE coordinator_scheduler_registration SET created_parent_epoch=1.5',
            "UPDATE coordinator_scheduler_registration SET private_identity_json='not-json'",
            'ALTER TABLE coordinator_scheduler_registration ADD COLUMN unsupported TEXT',
        ]
        for corruption in corruptions:
            with self.subTest(corruption=corruption), self.fixture() as (world, host, owner, version, root):
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute(corruption)
                    db.commit()
                before = world.db_path.read_bytes()
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window,
                                coordinator=owner, expected_version=version)
                self.assertEqual(world.db_path.read_bytes(), before)

    def test_window_revision_exhaustion_fails_closed_before_sql_overflow(self):
        with self.fixture() as (world, host, owner, version, root):
            def exhausted(state, db):
                state['revision'] = 2**63-2
            with world._coordinator_scope(owner):
                world._mutate(exhausted)
            current = {k:world.snapshot()[k] for k in ('world_id','revision')}
            self.assertEqual(current['revision'], 2**63-1)
            before = world.db_path.read_bytes()
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window,
                            coordinator=owner, expected_version=current)
            self.assertEqual(world.db_path.read_bytes(), before)

    def test_registration_rechecks_source_ambiguity_at_original_transaction_final_gate(self):
        import app.coordinator_scheduler as scheduler
        with self.fixture(registered=False) as (world, host, owner, version, root):
            real_schema = scheduler.schema
            newcomers = []
            def interleave(db):
                from contextvars import Context
                real_schema(db)
                # An independently constructed observer does not inherit this
                # transaction's trusted root scope or bootstrap another writer.
                newcomers.append(Context().run(ActorHost, world, root/'new-conflicting.sqlite'))
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            try:
                with patch.object(scheduler, 'schema', interleave):
                    self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.register_scheduler,
                                    coordinator=owner, expected_version=version,
                                    boundary=dict(kind='pristine', **version), actor_db_path=host.db_path)
                self.assertEqual({p:p.read_bytes() for p in before}, before)
                self.assertFalse((root/'new-conflicting.sqlite').exists())
            finally:
                for newcomer in newcomers:
                    newcomer.close()

    def test_horizon_window_is_empty_completed_and_keeps_each_future_task(self):
        with self.fixture() as (world, host, owner, version, root):
            # Explicit ledger fixture shortens horizon AFTER pristine registration;
            # no legacy boundary inference and no Host completion simulation.
            def horizon(state, db):
                state['horizon'] = DAY
            with world._coordinator_scope(owner):
                world._mutate(horizon)
            before = world.snapshot()
            version = {k:before[k] for k in ('world_id','revision')}
            future = {q['id']:q for q in before['queue'] if q['at'] > DAY}
            self.assertTrue(future)
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            self.assertEqual(window['state'], 'completed')
            self.assertEqual(window['boundary_kind'], 'horizon')
            self.assertEqual(window['roster'], [])
            self.assertIsNone(world.next_activation(window['window_id'], coordinator=owner))
            stopped = world.snapshot()
            self.assertEqual(stopped['status'], 'horizon_paused')
            self.assertEqual(stopped['time'], DAY)
            queued = {q['id']:q for q in stopped['queue']}
            for task, payload in future.items():
                self.assertEqual(queued[task], payload)
            world.control('resume', coordinator=owner)
            resumed = world.snapshot()
            self.assertEqual(resumed['time'], DAY)
            self.assertEqual(resumed['horizon'], 91*DAY)
            self.assertEqual(resumed['queue'], stopped['queue'])

    def test_cancelled_captured_root_rolls_back_window_and_snapshot_at_final_gate(self):
        from app.coordinator_execution import CoordinatorAttempt
        from app.actor_execution import EntryBudget
        import threading
        with self.fixture() as (world, host, owner, version, root):
            attempt = CoordinatorAttempt(world, owner, host.execution_policy)
            entry = EntryBudget(host.execution_policy, threading.Event(), (attempt,))
            encode = world._encode_state
            def cancel(db, state):
                result = encode(db, state)
                attempt.stop.set()
                return result
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            with world._coordinator_scope(owner, attempt), world._entry_budget(entry), patch.object(world, '_encode_state', cancel):
                self.assertCode('COORDINATOR_EXECUTION_CANCELLED', world.open_or_recover_window,
                                coordinator=owner, expected_version=version)
            self.assertEqual({p:p.read_bytes() for p in before}, before)

    def test_scheduler_metadata_stays_out_of_snapshots_roles_and_receipts(self):
        with self.fixture() as (world, host, owner, version, root):
            world.open_or_recover_window(coordinator=owner, expected_version=version)
            person = world.snapshot()['persons'][0]['id']
            public = json.dumps([world.snapshot(), world.snapshot(1), world.observe(person)], ensure_ascii=False)
            for secret in (owner.owner_token, str(root), 'coordinator_windows', 'activation_id',
                           'private_identity_json', 'scheduler_genesis', 'owner_parent_epoch'):
                self.assertNotIn(secret, public)
            self.assertIsNone(world.receipt(person, 'no-business-intent'))

    def test_pristine_refuses_all_actual_attempt_traces_and_unknown_boundary_tags(self):
        for trace in ('private_session','main_attempt','advanced'):
            with self.subTest(trace=trace), self.fixture(registered=False) as (world, host, owner, version, root):
                if trace == 'private_session':
                    with closing(sqlite3.connect(host.db_path)) as db:
                        db.execute("INSERT INTO actor_sessions VALUES (?,'[{}]','[]')", (world.snapshot()['persons'][0]['id'],))
                        db.commit()
                elif trace == 'main_attempt':
                    permit = world.acquire_actor_execution(world.snapshot()['persons'][0]['id'], 'attempted', coordinator=owner)
                    world.release_actor_execution(permit)
                else:
                    world.advance(DAY, coordinator=owner)
                current = {k:world.snapshot()[k] for k in ('world_id','revision')}
                pair = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
                self.assertFalse(world.inspect_scheduler_boundary(actor_db_path=host.db_path)['eligible'])
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.register_scheduler, coordinator=owner,
                                expected_version=current, boundary=dict(kind='pristine', **current), actor_db_path=host.db_path)
                for kind in ('verified_boundary','authorized_boundary','anything'):
                    self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.register_scheduler, coordinator=owner,
                                    expected_version=current, boundary=dict(kind=kind, **current), actor_db_path=host.db_path)
                self.assertEqual({p:p.read_bytes() for p in pair}, pair)

    def test_card_history_is_allowed_but_never_rewritten_by_registration(self):
        with self.fixture(registered=False) as (world, host, owner, version, root):
            person = world.snapshot()['persons'][0]['id']
            world.edit_card(person, {'mission':'仅向前的新使命'}, coordinator=owner)
            before = world.snapshot()
            current = {k:before[k] for k in ('world_id','revision')}
            inspection = world.inspect_scheduler_boundary(actor_db_path=host.db_path)
            self.assertTrue(inspection['eligible'])
            world.register_scheduler(coordinator=owner, expected_version=current,
                                     boundary=inspection['boundary'], actor_db_path=host.db_path)
            self.assertEqual(world.snapshot(), before)

    def test_genesis_and_initial_snapshot_rollback_together(self):
        with tempfile.TemporaryDirectory(prefix='p023b-atomic-genesis-', dir=SCRATCH) as directory:
            world = WorldStore(Path(directory)/'world.sqlite')
            encode = world._encode_state
            def fail(db, state):
                encode(db, state)
                raise RuntimeError('genesis-injected')
            with patch.object(world, '_encode_state', fail), self.assertRaisesRegex(RuntimeError, 'genesis-injected'):
                world.initialize()
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])
            initial = world.initialize()
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT world_id FROM scheduler_genesis').fetchone()[0], initial['world_id'])
                self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 1)

    def test_existing_registration_rejects_same_path_replaced_file_after_new_host(self):
        with self.fixture() as (world, host, owner, version, root):
            world.open_or_recover_window(coordinator=owner, expected_version=version)
            host.close()
            path = Path(host.db_path)
            backup = root/'previous-actors.sqlite'
            path.rename(backup)
            path.write_bytes(backup.read_bytes())
            reopened = WorldStore(world.db_path)
            observer = ActorHost(reopened, path)
            try:
                before = {p:p.read_bytes() for p in (world.db_path, path)}
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', reopened.open_or_recover_window,
                                coordinator=owner, expected_version=version)
                self.assertEqual({p:p.read_bytes() for p in before}, before)
            finally:
                observer.close()
                path.unlink()
                backup.rename(path)

    def test_incompatible_ledger_schema_is_sanitized_and_never_opened(self):
        for corruption in ('DROP TABLE coordinator_activations',
                           'ALTER TABLE coordinator_windows ADD COLUMN incompatible TEXT',
                           'DROP INDEX coordinator_one_open_window'):
            with self.subTest(corruption=corruption), self.fixture() as (world, host, owner, version, root):
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute(corruption)
                    db.commit()
                before = world.db_path.read_bytes()
                self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window,
                                coordinator=owner, expected_version=version)
                self.assertEqual(world.db_path.read_bytes(), before)

    def test_source_query_during_actual_host_constructor_fails_closed_until_wired(self):
        with self.fixture(registered=False) as (world, host, owner, version, root):
            register = world._register_private_source
            observed = []
            def during_constructor(storage):
                register(storage)
                observed.append(world.inspect_scheduler_boundary(actor_db_path=host.db_path))
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            duplicate = None
            try:
                with patch.object(world, '_register_private_source', during_constructor):
                    duplicate = ActorHost(world, host.db_path)
                self.assertFalse(observed[0]['eligible'])
                self.assertTrue(world.inspect_scheduler_boundary(actor_db_path=host.db_path)['eligible'])
                self.assertEqual({p:p.read_bytes() for p in before}, before)
            finally:
                if duplicate is not None:
                    duplicate.close()

    def test_window_internal_advance_preserves_final_frozen_pathology_gate(self):
        with self.fixture() as (world, host, owner, version, root):
            process = world._process_event
            def corrupt(state, event):
                process(state, event)
                state['pathology']['contacts_per_infection'] += 1
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            with patch.object(world, '_process_event', corrupt):
                self.assertCode('FROZEN_PATHOLOGY', world.open_or_recover_window,
                                coordinator=owner, expected_version=version)
            self.assertEqual({p:p.read_bytes() for p in before}, before)

    def test_open_persists_one_advanced_window_and_recovery_never_advances_again(self):
        with tempfile.TemporaryDirectory(prefix='p023b-window-first-', dir=SCRATCH) as directory:
            root = Path(directory)
            world = WorldStore(root / 'world.sqlite')
            initial = world.initialize()
            host = ActorHost(world, root / 'actors.sqlite')
            owner = world.acquire_coordinator_execution('window-first-owner')
            try:
                register = getattr(world, 'register_scheduler', None)
                self.assertTrue(callable(register), 'Persistent scheduler registration is missing')
                world.control('resume', coordinator=owner)
                before = world.snapshot()
                version = {'world_id': before['world_id'], 'revision': before['revision']}
                boundary = {'kind': 'pristine', **version}
                register(coordinator=owner, expected_version=version, boundary=boundary,
                         actor_db_path=host.db_path)
                self.assertEqual(world.snapshot(), before, 'registration is host metadata, not world time/revision')
                opened = world.open_or_recover_window(coordinator=owner, expected_version=version)
                after = world.snapshot()
                expected_roster = [person['id'] for person in after['persons']]
                self.assertEqual(after['time'], DAY)
                self.assertEqual(opened['world_id'], initial['world_id'])
                self.assertEqual(opened['target_time'], DAY)
                self.assertEqual(opened['state'], 'open')
                self.assertEqual(opened['roster'], expected_roster)
                recovered = world.open_or_recover_window(coordinator=owner, expected_version=version)
                self.assertEqual(recovered['window_id'], opened['window_id'])
                self.assertEqual(world.snapshot(), after, 'an old caller revision must recover, not skip a day')
                with closing(sqlite3.connect(world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM coordinator_windows').fetchone()[0], 1)
                    row = db.execute('SELECT target_time,advanced_revision,roster_json FROM coordinator_windows').fetchone()
                    self.assertEqual(row[0], DAY)
                    self.assertEqual(row[1], after['revision'])
                    self.assertEqual(json.loads(row[2]), expected_roster)
                    activations = db.execute('SELECT person_id,phase,intent_id FROM coordinator_activations ORDER BY roster_index').fetchall()
                    self.assertEqual(activations, [(person, 'pending', None) for person in expected_roster])
                    self.assertEqual(db.execute('PRAGMA integrity_check').fetchone(), ('ok',))
            finally:
                host.close()
                world.release_coordinator_execution(owner)
