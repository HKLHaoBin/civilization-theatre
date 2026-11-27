"""Actor DB ownership tests; identity protocol doubles, real SQLite (no LLM)."""
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.tests.test_actors import FakeWorld


class ActorBindingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path(os.environ['LOCALAPPDATA'])/'hermes/cache/scratch')
        self.path = Path(self.tmp.name)/'actors.sqlite'

    def tearDown(self):
        self.tmp.cleanup()

    def world(self, identity):
        world = FakeWorld()
        world.world_id = identity
        return world

    def test_01_fresh_actor_store_cannot_be_opened_by_another_world(self):
        host = ActorHost(self.world('WORLD_A'), self.path)
        host.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("INSERT INTO actor_sessions VALUES ('king', '[\"WORLD_A_PRIVATE\"]', '[]')")
            db.commit()
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(RuntimeError, '^ACTOR_WORLD_MISMATCH$'):
            other = ActorHost(self.world('WORLD_B'), self.path)
            other.close()
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
        self.assertIsNone(host.worker_pid)
    def legacy_host(self, world):
        import importlib.util
        import json
        root = Path(__file__).resolve().parents[2]
        archive = root/'app/evidence/candidates/p07-1'
        source = archive/'app/actors.py'
        manifest = json.loads((archive/'manifest.json').read_text(encoding='utf-8'))
        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), manifest['sha256']['app/actors.py'])
        spec = importlib.util.spec_from_file_location('p011_frozen_actor_producer', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.ActorHost(world, self.path)

    def test_02_unproven_legacy_private_pending_is_refused_without_any_write(self):
        world = self.world('WORLD_A')
        old = self.legacy_host(world)
        old.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("INSERT INTO actor_sessions VALUES ('king', '[\"PRIVATE_UNPROVEN\"]', '[]')")
            db.execute("INSERT INTO actor_intents(intent_id,person_id,body,state) VALUES ('pending','king','{}','prepared')")
            db.commit()
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(RuntimeError, '^ACTOR_WORLD_UNPROVEN$'):
            host = ActorHost(world, self.path)
            host.close()
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)
    def test_03_legacy_actual_producer_with_verified_world_receipt_is_adopted(self):
        from unittest.mock import patch
        world = self.world('WORLD_A')
        view = world.observe('king')
        view['observations'] = [{'text': 'LEGACY_PRIVATE_A'}]
        world.views['king'] = view
        old = self.legacy_host(world)
        root = Path(__file__).resolve().parents[2]
        try:
            with patch.dict(os.environ, {'PI_MODULE_ROOT': str(root/'spikes/002-pi-isolation/npm/node_modules')}):
                result = old.act('king')  # Real frozen producer + Node/Pi, protocol-double world.
        finally:
            old.close()
        self.assertEqual(old.worker_exit_code, 0)
        with closing(sqlite3.connect(self.path)) as db:
            before = db.execute('SELECT * FROM actor_sessions').fetchall()
        host = ActorHost(world, self.path)
        try:
            self.assertIsNone(host.worker_pid)
            with closing(sqlite3.connect(self.path)) as db:
                self.assertEqual(db.execute('SELECT * FROM actor_sessions').fetchall(), before)
                self.assertEqual(db.execute('SELECT world_id FROM actor_world_binding').fetchone()[0], 'WORLD_A')
            self.assertIn(('king', result['intent_id']), world.lookups)
        finally:
            host.close()
    def test_04_foreign_world_receipt_cannot_promote_private_candidate(self):
        host = ActorHost(self.world('WORLD_A'), self.path)
        try:
            with closing(sqlite3.connect(self.path)) as db:
                db.execute("INSERT INTO actor_sessions VALUES ('king','[]','[]')")
                db.execute("INSERT INTO actor_intents(intent_id,person_id,body,state) VALUES ('candidate','king','{}','candidate')")
                db.commit()
                before = list(db.iterdump())
            receipt = {'world_id':'WORLD_B','person_id':'king','intent_id':'candidate'}
            with self.assertRaisesRegex(RuntimeError, '^RECEIPT_WORLD_MISMATCH$'):
                host._finish('candidate', 'king', '[\"FOREIGN_PRIVATE_CANDIDATE\"]', receipt)
            with closing(sqlite3.connect(self.path)) as db:
                self.assertEqual(list(db.iterdump()), before)
        finally:
            host.close()
    # Additional characterization of ownership rules already introduced above;
    # these are NOT claimed as extra RED/GREEN cycles.
    def test_c_missing_world_identity_is_rejected_before_creating_actor_db(self):
        world = FakeWorld()
        del world.world_id
        with self.assertRaisesRegex(RuntimeError, '^ACTOR_WORLD_ID_REQUIRED$'):
            ActorHost(world, self.path)
        self.assertFalse(self.path.exists())

    def test_c_same_world_reopen_preserves_nonempty_session_and_database_bytes(self):
        world = self.world('WORLD_A')
        host = ActorHost(world, self.path)
        host.close()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("INSERT INTO actor_sessions VALUES ('king','[\"PRIVATE_A\"]','[\"seen\"]')")
            db.commit()
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        host = ActorHost(world, self.path)
        host.close()
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)

    def seed_receipt_fixture(self, world):
        import json
        host = self.legacy_host(world)
        host.close()
        receipt = world.submit('king', 'original', {'action':'wait'})
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("INSERT INTO actor_intents(intent_id,person_id,body,state,receipt) VALUES ('original','king','{}','committed',?)", (json.dumps(receipt),))
            db.execute("INSERT INTO actor_sessions VALUES ('king','[\"LEGACY_A_PRIVATE\"]','[]')")
            db.commit()

    def assert_unchanged_refusal(self, world, error):
        before = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaisesRegex(RuntimeError, '^'+error+'$'):
            ActorHost(world, self.path)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), before)

    def test_c_legacy_foreign_world_receipt_is_not_relabelled(self):
        self.seed_receipt_fixture(self.world('WORLD_A'))
        self.assert_unchanged_refusal(self.world('WORLD_B'), 'ACTOR_WORLD_MISMATCH')

    def test_c_self_claimed_receipt_without_actual_world_fact_cannot_prove_ownership(self):
        world = self.world('WORLD_A')
        self.seed_receipt_fixture(world)
        world.receipts.clear()
        self.assert_unchanged_refusal(world, 'ACTOR_WORLD_UNPROVEN')

    def test_c_one_verified_person_cannot_claim_another_private_session(self):
        world = self.world('WORLD_A')
        self.seed_receipt_fixture(world)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("INSERT INTO actor_sessions VALUES ('unproven_person','[\"FOREIGN_PRIVATE\"]','[]')")
            db.commit()
        self.assert_unchanged_refusal(world, 'ACTOR_WORLD_UNPROVEN')

    def test_c_actual_archived_world_and_actor_producers_unknown_ack_adopt_and_reconcile(self):
        import importlib.util
        import json
        from unittest.mock import patch
        root = Path(__file__).resolve().parents[2]
        archive = root/'app/evidence/candidates/p07-1'
        source = archive/'app/world.py'
        manifest = json.loads((archive/'manifest.json').read_text(encoding='utf-8'))
        self.assertEqual(hashlib.sha256(source.read_bytes()).hexdigest(), manifest['sha256']['app/world.py'])
        spec = importlib.util.spec_from_file_location('app.p011_archived_world', source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        world = module.WorldStore(Path(self.tmp.name)/'world.sqlite')
        initial = world.initialize()
        world.control('resume')
        old = self.legacy_host(world)
        original = {}
        def lose_finish(intent, *args):
            original['intent'] = intent
            raise RuntimeError('injected loss before actor receipt checkpoint')
        old._finish = lose_finish
        try:
            with patch.dict(os.environ, {'PI_MODULE_ROOT': str(root/'spikes/002-pi-isolation/npm/node_modules')}):
                with self.assertRaisesRegex(RuntimeError, 'injected loss'):
                    old.act('person-country-1-king')
        finally:
            old.close()
        self.assertEqual(old.worker_exit_code, 0)
        before_revision = world.snapshot()['revision']
        host = ActorHost(world, self.path)
        try:
            recovered = host.act('person-country-1-king')
            self.assertTrue(recovered['reconciled'])
            self.assertEqual(recovered['intent_id'], original['intent'])
            self.assertEqual(recovered['receipt']['world_id'], initial['world_id'])
            self.assertEqual(world.snapshot()['revision'], before_revision)
            self.assertIsNone(host.worker_pid)
        finally:
            host.close()


if __name__ == '__main__':
    unittest.main()
