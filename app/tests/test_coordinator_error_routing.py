"""Real Host/Coordinator/SQL routing; only the inner action is fault-injected."""
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.server import Coordinator
from app.world import WorldError, WorldStore

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class CoordinatorErrorRoutingTests(unittest.TestCase):
    def check_routing(self, boundary, expected_code, expected_status):
        with tempfile.TemporaryDirectory(prefix='p023a-routing-', dir=SCRATCH) as temp:
            world = WorldStore(Path(temp) / 'world.sqlite')
            world.initialize()
            host = ActorHost(world, Path(temp) / 'actors.sqlite')
            owner = Coordinator(world, host)
            original = host._act
            attempts = []

            def fault(person, *, execution=None, attempt_id=None):
                attempts.append(host._attempt)
                if boundary == 'invalid_execution':
                    # The real World validator must reject bool, not a mocked code.
                    world.renew_actor_execution(replace(execution, epoch=True))
                elif boundary == 'invalid_coordinator':
                    world._validate_coordinator_execution(object())
                elif boundary == 'fenced_actor':
                    raise WorldError('ACTOR_EXECUTION_FENCED', 'sanitized')
                elif boundary == 'coded_failure':
                    raise WorldError('UNEXPECTED_TECHNICAL_FAILURE', 'sanitized')
                raise RuntimeError('ordinary failure')

            host._act = fault
            try:
                owner.start()
                owner.worker.join(5)
                self.assertFalse(owner.worker.is_alive())
                self.assertEqual(len(attempts), 1)
                self.assertEqual(owner.error, expected_code)
                self.assertTrue(owner.stop.is_set())
                self.assertFalse(owner.busy)
                self.assertEqual(owner.snapshot()['runtime']['error'], expected_code)
                self.assertIsNone(host.worker_pid)
                after = world.snapshot()
                with closing(sqlite3.connect(world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    persisted = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                    self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 0)
                    statuses = [json.loads(row[0])['status'] for row in db.execute('SELECT state FROM snapshots')]
                for field in ('status', 'revision', 'time'):
                    self.assertEqual(after[field], persisted[field])
                self.assertEqual(after.get('diagnostic'), persisted.get('diagnostic'))
                self.assertEqual(after['status'], expected_status)
                if expected_status == 'running':
                    self.assertIsNone(after.get('diagnostic'))
                    self.assertNotIn('technical_paused', statuses)
                else:
                    self.assertEqual(after['diagnostic']['code'], expected_code)
            finally:
                host._act = original
                owner.close()
                self.assertFalse(owner.worker and owner.worker.is_alive())
                self.assertFalse(owner._owner_attempt.thread.is_alive())
                self.assertIsNone(host.worker_pid)
                self.assertFalse(host.cleanup_pending)
                for attempt in attempts:
                    self.assertFalse(attempt.thread.is_alive())
                with closing(sqlite3.connect(world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    self.assertEqual(db.execute('SELECT owner_token FROM coordinator_execution_leases').fetchone(), (None,))
                    self.assertTrue(all(row == (None,) for row in db.execute('SELECT owner_token FROM actor_execution_leases')))

    def test_invalid_execution_stops_locally_without_world_pause(self):
        self.check_routing('invalid_execution', 'INVALID_EXECUTION', 'running')

    # Unchanged-behavior characterization, not additional historical REDs.
    def test_invalid_coordinator_stops_locally(self):
        self.check_routing('invalid_coordinator', 'INVALID_COORDINATOR_EXECUTION', 'running')

    def test_fenced_actor_stops_locally(self):
        self.check_routing('fenced_actor', 'ACTOR_EXECUTION_FENCED', 'running')

    def test_ordinary_runtime_failure_still_pauses(self):
        self.check_routing('ordinary_failure', 'HOST_FAILURE', 'technical_paused')

    def test_unrecognized_technical_code_still_pauses(self):
        self.check_routing('coded_failure', 'UNEXPECTED_TECHNICAL_FAILURE', 'technical_paused')

    def test_coordinator_failure_can_pause_with_root_only(self):
        with tempfile.TemporaryDirectory(prefix='p023a-root-pause-', dir=SCRATCH) as temp:
            world = WorldStore(Path(temp) / 'world.sqlite')
            world.initialize()
            host = ActorHost(world, Path(temp) / 'actors.sqlite')
            owner = Coordinator(world, host)

            def fail_before_actor(continuous):
                raise RuntimeError('coordinator failure before actor acquisition')

            owner._run_step = fail_before_actor
            try:
                owner.start()
                owner.worker.join(5)
                self.assertFalse(owner.worker.is_alive())
                self.assertEqual(owner.error, 'HOST_FAILURE')
                self.assertTrue(owner.stop.is_set())
                self.assertEqual(world.snapshot()['status'], 'technical_paused')
                with closing(sqlite3.connect(world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                    persisted = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                    self.assertEqual(persisted['status'], 'technical_paused')
                    self.assertEqual(persisted['diagnostic']['code'], 'HOST_FAILURE')
                    self.assertEqual(db.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name='actor_execution_leases'").fetchone()[0], 0)
                    self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 0)
            finally:
                owner.close()
                self.assertFalse(owner._owner_attempt.thread.is_alive())
                self.assertFalse(owner.worker.is_alive())
                self.assertIsNone(host.worker_pid)

