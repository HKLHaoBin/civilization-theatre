"""P06.3 SQLite causality: scripted fixtures, no model/network acceptance."""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from app.world import WorldStore, WorldError
from app.adjudication import Adjudicator


class CausalityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='C:/Users/We3q/AppData/Local/hermes/cache/scratch')
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'world.sqlite'
        self.world = WorldStore(self.path)
        self.world.initialize()
        self.world.control('resume')
        self.person = 'person-country-1-finance'
        self.engine = Adjudicator(self.world)

    def decision(self, effects=None, **changes):
        state = self.world.snapshot()
        return dict(world_id=state['world_id'], base_revision=state['revision'], ruling='成功',
                    reason='脚本理由', goal='核账', costs=[], benefits=[],
                    effects=effects or [{'action':'wait'}]) | changes

    def test_readers_are_ro_begin_and_unknown_never_writes(self):
        self.engine.propose(self.person, 'read', '核账')
        self.engine.record(self.person, 'read', self.decision())
        self.world.edit_card(self.person, {'risk':'visible'})
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        connect = sqlite3.connect
        statements = []
        def readonly(database, *args, **kwargs):
            self.assertTrue(str(database).endswith('?mode=ro'))
            self.assertTrue(kwargs.get('uri'))
            db = connect(database, *args, **kwargs)
            db.set_trace_callback(statements.append)
            return db
        with patch('app.adjudication.sqlite3.connect', side_effect=readonly):
            self.engine.trace()
            self.world.observe(self.person, 1)
            for invalid in [True, 0, -1, '1']:
                with self.assertRaises(WorldError) as error:
                    self.engine.trace(invalid)
                self.assertEqual(error.exception.code, 'INVALID_SEQUENCE')
            with self.assertRaises(WorldError) as error:
                self.engine.trace(999)
            self.assertEqual(error.exception.code, 'UNKNOWN_SNAPSHOT')
        self.assertEqual(sum(s == 'BEGIN' for s in statements), 3)
        self.assertFalse(any(s.startswith(('INSERT','UPDATE','DELETE','CREATE','PRAGMA')) for s in statements))
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)

    def test_legacy_no_dm_tables_or_anchors_is_not_fabricated(self):
        legacy = WorldStore(Path(self.tmp.name) / 'legacy.sqlite')
        legacy.initialize()
        engine = Adjudicator.__new__(Adjudicator)
        engine.world = legacy
        digest = hashlib.sha256(legacy.db_path.read_bytes()).hexdigest()
        self.assertEqual(engine.trace()['proposals'], [])
        self.assertEqual(hashlib.sha256(legacy.db_path.read_bytes()).hexdigest(), digest)
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('INSERT INTO dm_proposals VALUES (?,?,?,?)', ('legacy',self.world.snapshot()['world_id'],self.person,'无锚旧原话'))
            db.execute('INSERT INTO dm_decisions VALUES (?,?,?)', ('legacy',1,json.dumps(self.decision())))
            db.commit()
        self.world.edit_card(self.person, {'risk':'later'})
        self.assertEqual(self.engine.trace()['proposals'], [])

    def test_anchor_failure_rolls_back_effects_receipts_and_snapshot(self):
        self.engine.propose(self.person, 'atomic', '调粮')
        self.engine.record(self.person, 'atomic', self.decision([{'action':'ship','to':'place-1','quantity':10}]))
        before = self.world.snapshot()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("CREATE TRIGGER fail_commit_anchor BEFORE INSERT ON dm_timeline WHEN NEW.kind='commit' BEGIN SELECT RAISE(ABORT, 'injected anchor failure'); END")
            db.commit()
        with self.assertRaises(sqlite3.IntegrityError):
            self.engine.commit(self.person, 'atomic', 1)
        self.assertEqual(self.world.snapshot(), before)
        self.assertIsNone(self.world.receipt(self.person, 'atomic'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM dm_timeline WHERE kind='commit'").fetchone()[0], 0)
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 1)
            db.execute('DROP TRIGGER fail_commit_anchor')
            db.commit()
        self.engine.commit(self.person, 'atomic', 1)
        self.assertEqual(self.engine.trace()['proposals'][0]['committed']['resource_changes'][0]['delta'], -10)

    def test_undelivered_arrival_and_investigation_completion_are_real(self):
        king = 'person-country-1-king'
        self.engine.propose(self.person, 'proof', '调查发信')
        self.engine.record(self.person, 'proof', self.decision([{'action':'investigate'}, {'action':'send','to':king,'text':'报告'}]))
        receipt = self.engine.commit(self.person, 'proof', 1)
        self.engine.propose(king, 'move', '离开')
        self.engine.record(king, 'move', self.decision([{'action':'travel','to':'place-1'}]))
        move = self.engine.commit(king, 'move', 1)
        self.world.advance(max([e['due'] for e in receipt['effect']['effects']] + [move['effect']['effects'][0]['due']]))
        proposals = {p['intent_id']:p for p in self.engine.trace()['proposals']}
        self.assertEqual([t['status'] for t in proposals['proof']['committed']['tasks']], ['completed','undelivered'])
        self.assertEqual(proposals['move']['committed']['tasks'][0]['status'], 'completed')
        self.assertEqual(self.world.observe(king)['messages'], [])

    def test_due_without_terminal_is_never_completed(self):
        self.engine.propose(self.person, 'lost', '调查')
        self.engine.record(self.person, 'lost', self.decision([{'action':'investigate'}]))
        receipt = self.engine.commit(self.person, 'lost', 1)
        def lost_queue(state, db):
            state['queue'] = [q for q in state['queue'] if not q.get('cause_id')]
            state['time'] = receipt['effect']['effects'][0]['due']+1
        self.world._mutate(lost_queue)
        task = self.engine.trace()['proposals'][0]['committed']['tasks'][0]
        self.assertEqual(task['status'], 'untracked')
        self.assertNotIn('event_id', task)

    def test_old_event_structure_and_historical_snapshot_bytes_unchanged(self):
        with closing(sqlite3.connect(self.path)) as db:
            before = db.execute('SELECT seq,state FROM snapshots').fetchall()
        state = {'events':[], 'time':0}
        event = self.world._event(state, 'old', 'old')
        self.assertEqual(set(event), {'id','time','kind','text','person_id'})
        self.engine.propose(self.person, 'new', '核账')
        self.engine.record(self.person, 'new', self.decision([{'action':'investigate'}]))
        self.engine.commit(self.person, 'new', 1)
        with closing(sqlite3.connect(self.path)) as db:
            after = dict(db.execute('SELECT seq,state FROM snapshots'))
        for seq, data in before:
            self.assertEqual(after[seq], data)

    def test_historical_observe_uses_own_messages_card_and_location(self):
        king = 'person-country-1-king'
        self.engine.propose(self.person, 'letter', '只给君主报告')
        self.engine.record(self.person, 'letter', self.decision([{'action':'send','to':king,'text':'私密报告'}]))
        receipt = self.engine.commit(self.person, 'letter', 1)
        early = self.world.snapshot()['revision']
        old_card = self.world.observe(king)['person']['card']
        self.world.edit_card(king, {'risk':'后来人格'})
        self.world.advance(receipt['effect']['effects'][0]['due'])
        digest = hashlib.sha256(self.path.read_bytes()).hexdigest()
        historical = self.world.observe(king, seq=early)
        self.assertEqual(historical['revision'], early)
        self.assertEqual(historical['messages'], [])
        self.assertEqual(historical['person']['card'], old_card)
        self.assertEqual(len(self.world.observe(king)['messages']), 1)
        self.assertEqual(self.world.observe('person-country-2-king')['messages'], [])
        self.assertEqual(set(historical), {'person','time','observations','messages','contacts','revision'})
        self.assertNotIn('causality', historical)
        for invalid in [True, 0, -1, '2']:
            with self.assertRaises(WorldError) as error:
                self.world.observe(king, invalid)
            self.assertEqual(error.exception.code, 'INVALID_SEQUENCE')
        with self.assertRaises(WorldError) as error:
            self.world.observe(king, 999)
        self.assertEqual(error.exception.code, 'UNKNOWN_SNAPSHOT')
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)

    def test_tasks_use_terminal_evidence_and_historical_pending(self):
        self.engine.propose(self.person, 'tasks', '调查并调粮报告')
        bodies = [{'action':'investigate'}, {'action':'ship','to':'place-1','quantity':10},
                  {'action':'send','to':'person-country-1-king','text':'报告'}]
        self.engine.record(self.person, 'tasks', self.decision(bodies))
        receipt = self.engine.commit(self.person, 'tasks', 1)
        committed = self.world.snapshot()['revision']
        initial = self.engine.trace()['proposals'][0]['committed']
        self.assertEqual(len(initial['tasks']), 3)
        self.assertEqual([t['status'] for t in initial['tasks']], ['pending']*3)
        self.assertEqual(initial['resource_changes'][0]['delta'], -10)
        self.world.submit(self.person, 'leave', {'action':'travel','to':'place-1'})
        self.world.advance(max(e['due'] for e in receipt['effect']['effects']))
        final = self.engine.trace()['proposals'][0]['committed']
        self.assertEqual([t['status'] for t in final['tasks']], ['interrupted','completed','completed'])
        events = {e['id']:e for e in self.world.snapshot()['events']}
        for task in final['tasks']:
            self.assertEqual(events[task['event_id']]['cause_id'], task['cause_id'])
        self.assertEqual(self.engine.trace(committed)['proposals'][0]['committed'], initial)
        self.assertEqual(final['resource_changes'], initial['resource_changes'])

    def test_anchors_hide_later_same_base_decisions_and_commit(self):
        base = self.world.snapshot()['revision']
        self.engine.propose(self.person, 'original', '原话')
        decision = self.decision()
        self.engine.record(self.person, 'original', decision)
        self.assertEqual(self.world.snapshot()['revision'], base)
        self.assertEqual(self.engine.trace(base)['proposals'], [])
        self.world.edit_card(self.person, {'risk':'first snapshot'})
        early = self.world.snapshot()['revision']
        self.engine.record(self.person, 'original', decision | {'reason':'后来同基准'})
        first = self.engine.trace(early)['proposals'][0]
        self.assertEqual(first['recorded_revision'], early)
        self.assertEqual([d['version'] for d in first['decisions']], [1])
        self.assertEqual(first['decisions'][0]['status'], 'stale')
        self.assertIsNone(first['committed'])
        version = self.engine.record(self.person, 'original', self.decision())
        self.engine.commit(self.person, 'original', version)
        self.assertEqual(self.engine.trace(early)['proposals'][0], first)
        latest = self.engine.trace()['proposals'][0]
        self.assertEqual([d['status'] for d in latest['decisions']], ['superseded','superseded','committed'])
        self.assertEqual(latest['committed']['decision_version'], 3)
        self.assertEqual(set(latest), {'intent_id','person_id','person_name','text','recorded_revision','decisions','committed'})
        with closing(sqlite3.connect(self.path)) as db:
            anchors = db.execute('SELECT kind,version,visible_from FROM dm_timeline ORDER BY kind,version').fetchall()
            self.assertEqual(len(anchors), 5)
            for operation in ['UPDATE dm_timeline SET visible_from=1', 'DELETE FROM dm_timeline']:
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute(operation)
