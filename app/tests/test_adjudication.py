"""P06.2 real SQLite tests; scripted decisions, no model acceptance."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from app.world import WorldStore, WorldError
try:
    from app.adjudication import Adjudicator
except ImportError:
    Adjudicator = None


class AdjudicationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='C:/Users/We3q/AppData/Local/hermes/cache/scratch')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'world.sqlite'
        self.world = WorldStore(self.path)
        self.world.initialize()
        self.world.control('resume')
        self.person = 'person-country-1-finance'

    def engine(self):
        self.assertIsNotNone(Adjudicator, 'missing adjudication engine')
        return Adjudicator(self.world)

    def decision(self, effects=None, **changes):
        state = self.world.snapshot()
        return dict(world_id=state['world_id'], base_revision=state['revision'], ruling='成功',
                    reason='脚本按现况裁定', goal='核账', costs=[], benefits=[],
                    effects=effects or [{'action':'wait'}]) | changes

    def test_history_rejects_rebound_world_identity(self):
        engine = self.engine()
        engine.propose(self.person, 'binding', '核账')
        engine.record(self.person, 'binding', self.decision())
        def replace_world(state, db):
            state['world_id'] = 'different-world-id'
        self.world._mutate(replace_world)
        with self.assertRaises(WorldError) as error:
            engine.history(self.person, 'binding')
        self.assertEqual(error.exception.code, 'WORLD_MISMATCH')

    def test_atomic_fault_rolls_back_children_outbox_and_grain(self):
        engine = self.engine()
        engine.propose(self.person, 'fault', '调粮并发信')
        engine.record(self.person, 'fault', self.decision([
            {'action':'ship','to':'place-1','quantity':10},
            {'action':'send','to':'person-country-1-king','text':'报告'}]))
        before = self.world.snapshot()
        knowledge = self.world.observe(self.person)
        apply = self.world._apply_action
        calls = []
        def fail_midway(*args):
            calls.append(1)
            if len(calls) == 2:
                raise RuntimeError('injected fault')
            return apply(*args)
        with patch.object(self.world, '_apply_action', side_effect=fail_midway):
            with self.assertRaises(RuntimeError):
                engine.commit(self.person, 'fault', 1)
        self.assertEqual(self.world.snapshot(), before)
        self.assertEqual(self.world.observe(self.person), knowledge)
        self.assertIsNone(self.world.receipt(self.person, 'fault'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 1)
        self.assertEqual(engine.commit(self.person, 'fault', 1)['status'], 'committed')

    def test_stale_superseded_redecision_commits_same_original_id(self):
        engine = self.engine()
        engine.propose(self.person, 'stale', '核账')
        engine.record(self.person, 'stale', self.decision())
        self.world.edit_card(self.person, {'risk':'new'})
        before = self.world.snapshot()
        with self.assertRaises(WorldError) as error:
            engine.commit(self.person, 'stale', 1)
        self.assertEqual(error.exception.code, 'STALE_PREVIEW')
        self.assertEqual(self.world.snapshot(), before)
        engine.record(self.person, 'stale', self.decision([{'action':'investigate'}], ruling='失败', reason='新理由'))
        with self.assertRaises(WorldError) as error:
            engine.preview(self.person, 'stale', 1)
        self.assertEqual(error.exception.code, 'SUPERSEDED_DECISION')
        self.assertEqual(engine.commit(self.person, 'stale', 2)['intent_id'], 'stale')

    def test_shared_namespace_collision_is_not_overwritten(self):
        engine = self.engine()
        self.world.submit(self.person, 'legacy', {'action':'wait'})
        with self.assertRaises(WorldError):
            engine.propose(self.person, 'legacy', '原话')
        engine.propose(self.person, 'parent', '运粮')
        child = engine._child_id('parent', 1, 0)
        self.world.submit(self.person, child, {'action':'wait'})
        original = self.world.receipt(self.person, child)
        engine.record(self.person, 'parent', self.decision([{'action':'ship','to':'place-1','quantity':10}]))
        before = self.world.snapshot()
        with self.assertRaises(WorldError):
            engine.commit(self.person, 'parent', 1)
        self.assertEqual(self.world.snapshot(), before)
        self.assertEqual(self.world.receipt(self.person, child), original)

    def test_resolve_recovery_uses_recorded_decision_without_provider(self):
        engine = self.engine()
        engine.propose(self.person, 'recover', '核账')
        engine.record(self.person, 'recover', self.decision([{'action':'investigate'}]))
        recovered = Adjudicator(WorldStore(self.path))
        receipt = recovered.resolve(self.person, 'recover', '核账', lambda _: self.fail('provider should not run'))
        self.assertEqual(receipt['decision_version'], 1)

    def test_resolve_unknown_and_provider_failure_are_sanitized_no_story(self):
        engine = self.engine()
        before = self.world.snapshot()
        def provider(_):
            raise RuntimeError('SECRET provider detail')
        with self.assertRaises(WorldError) as error:
            engine.resolve(self.person, 'failed-provider', '核账', provider)
        self.assertNotIn('SECRET', str(error.exception))
        after = self.world.snapshot()
        self.assertEqual(after['status'], 'technical_paused')
        self.assertEqual(after['events'], before['events'])
        self.assertEqual(after['queue'], before['queue'])

    def test_resolve_exhausts_three_stale_attempts(self):
        engine = self.engine()
        calls = []
        def provider(_):
            calls.append(1)
            decision = self.decision()
            self.world.edit_card(self.person, {'risk':str(len(calls))})
            return decision
        with self.assertRaises(WorldError):
            engine.resolve(self.person, 'exhausted', '核账', provider)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(engine.history(self.person, 'exhausted')), 3)
        self.assertEqual(self.world.snapshot()['status'], 'technical_paused')
        self.assertIsNone(self.world.receipt(self.person, 'exhausted'))

    def test_authorization_not_bypassed_and_unknown_effect_resolve_pauses(self):
        engine = self.engine()
        engine.propose(self.person, 'authority', '代国王命令')
        engine.record(self.person, 'authority', self.decision([{'action':'command','to':'person-country-1-marshal','text':'限制聚集'}]))
        before = self.world.snapshot()
        with self.assertRaises(WorldError) as error:
            engine.preview(self.person, 'authority', 1)
        self.assertEqual(error.exception.code, 'NOT_AUTHORIZED')
        self.assertEqual(self.world.snapshot(), before)
        with self.assertRaises(WorldError):
            engine.resolve(self.person, 'unsupported', '改世界', lambda _:self.decision([{'action':'patch'}]))
        self.assertEqual(self.world.snapshot()['status'], 'technical_paused')
        self.assertEqual(self.world.snapshot()['events'], before['events'])
        self.assertEqual(engine.history(self.person, 'unsupported')[0]['decision']['effects'], [{'action':'patch'}])

    def test_unknown_effect_preview_readonly_commit_technical_pause(self):
        engine = self.engine()
        engine.propose(self.person, 'unknown', '任意原话')
        decision = self.decision([{'action':'invented','patch':{'grain':999}}])
        engine.record(self.person, 'unknown', decision)
        before = self.world.snapshot()
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        with self.assertRaises(WorldError):
            engine.preview(self.person, 'unknown', 1)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)
        with self.assertRaises(WorldError):
            engine.commit(self.person, 'unknown', 1)
        after = self.world.snapshot()
        self.assertEqual(after['status'], 'technical_paused')
        self.assertEqual(after['events'], before['events'])
        self.assertEqual(after['places'], before['places'])
        self.assertEqual(engine.history(self.person, 'unknown')[0]['decision'], decision)
        self.assertIsNone(self.world.receipt(self.person, 'unknown'))

    def test_resolve_retries_stale_original_id_and_reuses_recovery(self):
        engine = self.engine()
        engine.propose(self.person, 'retry', '核账')
        engine.record(self.person, 'retry', self.decision())
        self.world.edit_card(self.person, {'risk':'changed'})
        calls = []
        def provider(request):
            calls.append(request)
            self.assertEqual(set(request), {'proposal','world'})
            self.assertEqual(request['proposal']['intent_id'], 'retry')
            if len(calls) == 1:
                base = self.decision(ruling='失败', reason='过期裁定')
                self.world.edit_card(self.person, {'risk':'interleaving'})
                return base
            return self.decision([{'action':'investigate'}], ruling='大成功', reason='重新核账')
        receipt = engine.resolve(self.person, 'retry', '核账', provider)
        self.assertEqual(len(calls), 2)
        self.assertEqual(receipt['decision_version'], 3)
        self.assertEqual(receipt['ruling'], '大成功')
        self.world.control('pause')
        before = self.world.snapshot()
        self.assertEqual(engine.resolve(self.person, 'retry', '核账', lambda _: self.fail('unneeded provider')), receipt)
        self.assertEqual(self.world.snapshot(), before)
        with self.assertRaises(WorldError):
            engine.commit(self.person, 'retry', 1)

    def test_compound_readonly_preview_and_atomic_commit_real_time(self):
        engine = self.engine()
        engine.propose(self.person, 'compound', '运十粮，报告并核账')
        effects = [{'action':'ship','to':'place-1','quantity':10},
                   {'action':'send','to':'person-country-1-king','text':'已发粮，尚未到达'},
                   {'action':'investigate'}]
        version = engine.record(self.person, 'compound', self.decision(effects))
        before = self.world.snapshot()
        knowledge = self.world.observe(self.person)
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        connect = sqlite3.connect
        def ro(database, *args, **kwargs):
            self.assertTrue(str(database).endswith('?mode=ro'))
            self.assertTrue(kwargs.get('uri'))
            return connect(database, *args, **kwargs)
        with patch('app.adjudication.sqlite3.connect', side_effect=ro):
            preview = engine.preview(self.person, 'compound', version)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)
        self.assertEqual(self.world.observe(self.person), knowledge)
        self.assertEqual(self.world.snapshot(), before)
        self.assertEqual(preview['projected']['places'][0]['grain'], before['places'][0]['grain']-10)
        receipt = engine.commit(self.person, 'compound', version)
        self.assertEqual(receipt['effect']['effects'], preview['effects'])
        self.assertEqual(self.world.snapshot(), preview['projected'])
        self.assertEqual(self.world.receipt(self.person, 'compound'), receipt)
        self.assertEqual(len(receipt['effect']['effects']), 3)
        for effect in receipt['effect']['effects']:
            self.assertGreater(effect['due'], before['time'])
        self.assertEqual(self.world.observe('person-country-1-king')['messages'], [])
        self.world.advance(max(e['due'] for e in receipt['effect']['effects']))
        self.assertEqual(len(self.world.observe('person-country-1-king')['messages']), 1)
        self.assertTrue(any('核账' in o['text'] for o in self.world.observe(self.person)['observations']))
        self.world.control('pause')
        paused = self.world.snapshot()
        self.assertEqual(engine.commit(self.person, 'compound', version), receipt)
        self.assertEqual(self.world.snapshot(), paused)
        with self.assertRaises(WorldError):
            engine.record(self.person, 'compound', self.decision())

    def test_decisions_append_only_frozen_and_original_id(self):
        engine = self.engine()
        engine.propose(self.person, 'versions', '核账')
        first = self.decision()
        self.assertEqual(engine.record(self.person, 'versions', first), 1)
        self.assertEqual(engine.record(self.person, 'versions', first), 1)
        first['effects'][0]['action'] = 'invented'
        self.assertEqual(engine.history(self.person, 'versions')[0]['decision']['effects'], [{'action':'wait'}])
        second = self.decision(ruling='失败', reason='不同裁定', effects=[{'action':'unknown'}])
        self.assertEqual(engine.record(self.person, 'versions', second), 2)
        history = Adjudicator(WorldStore(self.path)).history(self.person, 'versions')
        self.assertEqual([r['version'] for r in history], [1, 2])
        self.assertEqual(history[1]['decision'], second)
        for invalid in [self.decision(base_revision=True), self.decision(effects=[{'action':'wait','x':float('nan')}]), self.decision(extra=1)]:
            with self.assertRaises(WorldError):
                engine.record(self.person, 'versions', invalid)
        with closing(sqlite3.connect(self.path)) as db:
            for sql in ['UPDATE dm_decisions SET version=99', 'DELETE FROM dm_decisions']:
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute(sql)

    def test_proposal_persisted_identity_world_and_immutable(self):
        engine = self.engine()
        before = self.world.snapshot()
        result = engine.propose(self.person, 'original', ' 原话：运粮\n')
        self.assertEqual(result, dict(world_id=before['world_id'], person_id=self.person,
                                     intent_id='original', text=' 原话：运粮\n'))
        self.assertEqual(Adjudicator(WorldStore(self.path)).proposal(self.person, 'original'), result)
        self.assertEqual(engine.propose(self.person, 'original', result['text']), result)
        for person, text in [(self.person, 'changed'), ('person-country-1-king', result['text'])]:
            with self.assertRaises(WorldError):
                engine.propose(person, 'original', text)
        self.assertEqual(self.world.snapshot(), before)
        with closing(sqlite3.connect(self.path)) as db:
            for sql in ['UPDATE dm_proposals SET text=\'changed\'', 'DELETE FROM dm_proposals']:
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute(sql)
