"""P06.1: real SQLite rule preview, not an LLM or open-action compiler."""
from contextlib import closing
import hashlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app.world import WorldError, WorldStore


class PreviewTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(os.environ['LOCALAPPDATA']) / 'hermes/cache/scratch'
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'world.sqlite'
        self.world = WorldStore(self.path)
        self.world.initialize()
        self.world.control('resume')
        self.person = 'person-country-1-finance'
        self.body = {'action': 'ship', 'to': 'place-1', 'quantity': 10}

    def test_preview_uses_readonly_connection_and_does_not_change_world(self):
        self.assertTrue(callable(getattr(self.world, 'preview', None)), '缺少真实只读试算接口')
        before = self.world.snapshot()
        knowledge = self.world.observe(self.person)
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        connect = sqlite3.connect
        def readonly_only(database, *args, **kwargs):
            self.assertTrue(str(database).endswith('?mode=ro'))
            self.assertTrue(kwargs.get('uri'))
            return connect(database, *args, **kwargs)
        with patch('app.world.sqlite3.connect', side_effect=readonly_only):
            draft = self.world.preview(self.person, 'preview-ship', self.body)
        self.assertEqual(draft['status'], 'preview')
        self.assertEqual(draft['world_id'], before['world_id'])
        self.assertEqual(draft['base_revision'], before['revision'])
        self.assertEqual(draft['projected']['places'][0]['grain'], before['places'][0]['grain']-10)
        self.assertEqual(len(draft['projected']['in_transit']), 1)
        self.assertEqual(self.world.snapshot(), before)
        self.assertEqual(self.world.observe(self.person), knowledge)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)
        self.assertIsNone(self.world.receipt(self.person, 'preview-ship'))
        receipt = self.world.submit(self.person, 'preview-ship', self.body)
        self.assertEqual(receipt['effect'], draft['effect'])
        self.assertEqual(self.world.snapshot(), draft['projected'])


    def test_stale_preview_is_rejected_without_effect_or_durable_intent(self):
        import inspect
        self.assertIn('expected_version', inspect.signature(self.world.submit).parameters)
        draft = self.world.preview(self.person, 'stale-ship', self.body)
        self.world.edit_card(self.person, {'risk': '愿意承担风险'})
        before = self.world.snapshot()
        with self.assertRaises(WorldError) as failure:
            self.world.submit(self.person, 'stale-ship', self.body,
                              expected_version={'world_id': draft['world_id'], 'revision': draft['base_revision']})
        self.assertEqual(failure.exception.code, 'STALE_PREVIEW')
        self.assertEqual(self.world.snapshot(), before)
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 0)


    def test_version_is_rechecked_after_intent_preparation_and_retry_keeps_id(self):
        draft = self.world.preview(self.person, 'racy-ship', self.body)
        version = {'world_id': draft['world_id'], 'revision': draft['base_revision']}
        original_mutate = self.world._mutate
        def intervening_commit(operation):
            WorldStore(self.path).edit_card(self.person, {'risk': '另一个提交发生'})
            return original_mutate(operation)
        with patch.object(self.world, '_mutate', side_effect=intervening_commit):
            with self.assertRaises(WorldError) as failure:
                self.world.submit(self.person, 'racy-ship', self.body, expected_version=version)
        self.assertEqual(failure.exception.code, 'STALE_PREVIEW')
        self.assertIsNone(self.world.receipt(self.person, 'racy-ship'))
        self.assertEqual(self.world.snapshot()['in_transit'], [])
        fresh = self.world.preview(self.person, 'racy-ship', self.body)
        latest_version = {'world_id': fresh['world_id'], 'revision': fresh['base_revision']}
        receipt = self.world.submit(self.person, 'racy-ship', self.body, expected_version=latest_version)
        after = self.world.snapshot()
        self.world.control('pause')
        paused = self.world.snapshot()
        self.assertEqual(self.world.submit(self.person, 'racy-ship', self.body, expected_version=latest_version), receipt)
        self.assertEqual(self.world.snapshot(), paused)
        self.assertEqual(len(after['in_transit']), 1)


    def test_committed_effect_uses_frozen_durable_payload(self):
        original_mutate = self.world._mutate
        def caller_mutates_body(operation):
            self.body['quantity'] = 100
            return original_mutate(operation)
        with patch.object(self.world, '_mutate', side_effect=caller_mutates_body):
            self.world.submit(self.person, 'immutable-intent', self.body)
        self.assertEqual(self.world.snapshot()['in_transit'][0]['quantity'], 10)


    def test_preview_respects_existing_business_identity(self):
        self.world.submit(self.person, 'already-sent', self.body)
        for person, body, code in [
            (self.person, self.body, 'ALREADY_COMMITTED'),
            (self.person, self.body | {'quantity': 11}, 'ID_PAYLOAD_CONFLICT'),
            ('person-country-1-king', self.body, 'ID_PAYLOAD_CONFLICT'),
        ]:
            with self.subTest(code=code):
                with self.assertRaises(WorldError) as failure:
                    self.world.preview(person, 'already-sent', body)
                self.assertEqual(failure.exception.code, code)
        before = self.world.snapshot()
        with self.assertRaises(WorldError) as failure:
            self.world.submit(self.person, 'unsupported-intent', {'action': 'propose', 'text': '发展未支持的工艺'})
        self.assertEqual(failure.exception.code, 'UNSUPPORTED')
        with self.assertRaises(WorldError) as failure:
            self.world.preview(self.person, 'unsupported-intent', self.body)
        self.assertEqual(failure.exception.code, 'ID_PAYLOAD_CONFLICT')
        self.assertEqual(self.world.snapshot(), before)


    def test_invalid_versions_and_rule_errors_never_write(self):
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        revision = self.world.snapshot()['revision']
        for version in [True, {}, {'world_id': 'x', 'revision': True},
                        {'world_id': 'x', 'revision': 0}, {'world_id': None, 'revision': 1},
                        {'world_id': 'x', 'revision': 1, 'extra': 1}]:
            with self.subTest(version=version):
                with self.assertRaises(WorldError) as failure:
                    self.world.submit(self.person, 'bad-version', self.body, expected_version=version)
                self.assertEqual(failure.exception.code, 'INVALID_BASE_VERSION')
        with self.assertRaises(WorldError) as failure:
            self.world.submit(self.person, 'wrong-world', self.body,
                              expected_version={'world_id': 'another-world', 'revision': revision})
        self.assertEqual(failure.exception.code, 'WORLD_MISMATCH')
        for person, body, code in [
            ('person-country-1-king', self.body, 'NOT_AUTHORIZED'),
            (self.person, {'action': 'propose', 'text': '未支持行动'}, 'UNSUPPORTED'),
            (self.person, self.body | {'quantity': True}, 'INVALID_BODY'),
        ]:
            with self.subTest(code=code):
                with self.assertRaises(WorldError) as failure:
                    self.world.preview(person, 'failed-draft', body)
                self.assertEqual(failure.exception.code, code)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)

    def test_communications_travel_and_four_rulings_stay_prospective(self):
        before = self.world.snapshot()
        king = 'person-country-1-king'
        knowledge = self.world.observe(king)
        for ruling in ['大失败', '失败', '成功', '大成功']:
            for body in [{'action': 'send', 'to': king, 'text': '准备赈济，但尚未发出'},
                         {'action': 'travel', 'to': 'place-1'}, {'action': 'investigate'}]:
                with self.subTest(ruling=ruling, body=body):
                    draft = self.world.preview(self.person, 'prospective', body, ruling=ruling)
                    self.assertEqual(draft['status'], 'preview')
                    self.assertEqual(draft['projected']['time'], before['time'])
                    self.assertEqual(draft['projected']['pathology'], before['pathology'])
                    self.assertEqual(self.world.snapshot(), before)
                    self.assertEqual(self.world.observe(king), knowledge)
                    self.assertIsNone(self.world.receipt(self.person, 'prospective'))
        self.world.control('pause')
        with self.assertRaises(WorldError) as failure:
            self.world.preview(self.person, 'paused', self.body)
        self.assertEqual(failure.exception.code, 'WORLD_PAUSED')


if __name__ == '__main__':
    unittest.main()
