"""Parent regression discoveries, real SQLite; no model calls."""
import os
from pathlib import Path
import tempfile
import unittest

from app.adjudication import Adjudicator
from app.world import WorldError, WorldStore


class AdjudicationParentRegressions(unittest.TestCase):
    def setUp(self):
        scratch = Path(os.environ['LOCALAPPDATA'])/'hermes/cache/scratch'
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.tmp.cleanup)
        self.world = WorldStore(Path(self.tmp.name)/'world.sqlite')
        self.world.initialize()
        self.world.control('resume')
        self.dm = Adjudicator(self.world)
        self.person = 'person-country-1-finance'

    def test_unsupported_has_explicit_safe_diagnostic_not_a_provider_failure(self):
        state = self.world.snapshot()
        def provider(context):
            return {'world_id': state['world_id'], 'base_revision': state['revision'],
                    'ruling': '成功', 'reason': '脚本测试', 'goal': '未支持的行动',
                    'costs': [], 'benefits': [], 'effects': [{'action': 'teleport', 'to': 'place-1'}]}
        with self.assertRaises(WorldError) as failure:
            self.dm.resolve(self.person, 'unsupported-diagnostic', '尝试尚未实现的行动', provider)
        self.assertEqual(failure.exception.code, 'ADJUDICATION_UNSUPPORTED')
        result = self.world.snapshot()
        self.assertEqual(result['status'], 'technical_paused')
        self.assertEqual(result['diagnostic']['code'], 'ADJUDICATION_UNSUPPORTED')
        self.assertEqual(result['events'], state['events'])
        self.assertEqual(result['queue'], state['queue'])
        self.assertEqual(self.dm.history(self.person, 'unsupported-diagnostic')[0]['decision']['effects'], [{'action':'teleport','to':'place-1'}])


    def test_paused_world_does_not_call_provider_or_replace_pause_reason(self):
        self.world.control('pause')
        before = self.world.snapshot()
        calls = []
        def provider(context):
            calls.append(1)
            state = context['world']
            return {'world_id': state['world_id'], 'base_revision': state['revision'],
                    'ruling': '成功', 'reason': '脚本测试', 'goal': '等待',
                    'costs': [], 'benefits': [], 'effects': [{'action': 'wait'}]}
        with self.assertRaises(WorldError) as failure:
            self.dm.resolve(self.person, 'paused-proposal', '暂停时不得调用DM', provider)
        self.assertEqual(failure.exception.code, 'WORLD_PAUSED')
        self.assertEqual(calls, [])
        self.assertEqual(self.world.snapshot(), before)


if __name__ == '__main__':
    unittest.main()
