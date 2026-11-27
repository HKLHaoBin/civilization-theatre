"""Dispatcher tests: fake adjudicator/world, real Pi worker; not DM rule acceptance."""
from contextlib import closing
import inspect
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.tests.test_actors import FakeWorld


TEXT = '若库存允许，请调粮赈济并向君主报信，随后核查仓情。'


class ProposalActor(ActorHost):
    @staticmethod
    def _choose(view, seen):
        return {'action': 'propose', 'text': TEXT}, seen


class DMHostTests(unittest.TestCase):
    def test_optional_adjudicator_receives_only_after_real_pi_candidate_is_durable(self):
        self.assertIn('adjudicator', inspect.signature(ActorHost).parameters)
        world = FakeWorld()
        scratch = Path(os.environ['LOCALAPPDATA']) / 'hermes/cache/scratch'
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            path = Path(directory) / 'actors.sqlite'
            calls = []
            provider = lambda context: {'reason': 'DM_ONLY_SECRET'}
            class DispatcherProbe:
                def __init__(self):
                    self.world = world
                def resolve(self, person, intent, text, dm):
                    with closing(sqlite3.connect(path)) as db:
                        row = db.execute('SELECT state,body,candidate FROM actor_intents WHERE intent_id=?', (intent,)).fetchone()
                    self_test.assertEqual(row[0], 'candidate')
                    self_test.assertEqual(json.loads(row[1]), {'action': 'propose', 'text': TEXT})
                    self_test.assertIsNotNone(row[2])
                    self_test.assertIs(dm, provider)
                    calls.append((person, intent, text))
                    return world.submit(person, intent, {'action': 'propose', 'text': text})
            self_test = self
            host = ProposalActor(world, path, adjudicator=DispatcherProbe(), dm_provider=provider)
            try:
                result = host.act('finance')
                self.assertEqual(result['provider'], 'scripted-pi')
                self.assertEqual(calls, [('finance', result['intent_id'], TEXT)])
                with closing(sqlite3.connect(path)) as db:
                    request, candidate = db.execute('SELECT request,candidate FROM actor_intents').fetchone()
                    messages = db.execute('SELECT messages FROM actor_sessions').fetchone()[0]
                for text in [request, candidate, messages]:
                    self.assertNotIn('DM_ONLY_SECRET', text)
                self.assertEqual(result['body']['text'], TEXT)
            finally:
                host.close()
            self.assertEqual(host.worker_exit_code, 0)


    def test_invalid_dm_binding_is_rejected_before_creating_actor_database(self):
        from types import SimpleNamespace
        world = FakeWorld()
        valid = SimpleNamespace(world=world, resolve=lambda *args: None)
        foreign = SimpleNamespace(world=FakeWorld(), resolve=lambda *args: None)
        provider = lambda context: {}
        cases = [dict(adjudicator=valid), dict(dm_provider=provider),
                 dict(adjudicator=foreign, dm_provider=provider),
                 dict(adjudicator=valid, dm_provider='not-callable')]
        scratch = Path(os.environ['LOCALAPPDATA']) / 'hermes/cache/scratch'
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            for index, options in enumerate(cases):
                with self.subTest(index=index):
                    path = Path(directory) / f'bad-{index}.sqlite'
                    with self.assertRaises(ValueError):
                        ActorHost(world, path, **options)
                    self.assertFalse(path.exists())


    def test_pending_proposal_reuses_original_sdk_candidate_and_business_id(self):
        self._exercise_unknown_result(False)

    def test_committed_proposal_reconciles_without_another_dm_or_sdk_call(self):
        self._exercise_unknown_result(True)

    def _exercise_unknown_result(self, committed):
        world = FakeWorld()
        calls = []
        provider = lambda context: {}
        class LostReplyProbe:
            def __init__(self):
                self.world = world
            def resolve(self, person, intent, text, dm):
                calls.append((person, intent, text))
                if len(calls) == 1:
                    if committed:
                        world.submit(person, intent, {'action': 'propose', 'text': text})
                    raise RuntimeError('TEST_UNKNOWN_DM_RESULT')
                return world.submit(person, intent, {'action': 'propose', 'text': text})
        adjudicator = LostReplyProbe()
        scratch = Path(os.environ['LOCALAPPDATA']) / 'hermes/cache/scratch'
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            path = Path(directory) / 'actors.sqlite'
            host = ProposalActor(world, path, adjudicator=adjudicator, dm_provider=provider)
            try:
                with self.assertRaisesRegex(RuntimeError, 'TEST_UNKNOWN_DM_RESULT'):
                    host.act('finance')
            finally:
                host.close()
            restored = ProposalActor(world, path, adjudicator=adjudicator, dm_provider=provider)
            try:
                recovered = restored.act('finance')
                self.assertEqual(recovered['intent_id'], calls[0][1])
                self.assertEqual(recovered['reconciled'], committed)
                self.assertIsNone(restored.worker_pid)
                self.assertEqual(len(world.calls), 1)
                self.assertEqual(len(calls), 1 if committed else 2)
                self.assertTrue(all(call == calls[0] for call in calls))
            finally:
                restored.close()


if __name__ == '__main__':
    unittest.main()
