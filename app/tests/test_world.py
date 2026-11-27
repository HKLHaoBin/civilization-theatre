"""World rules only: scripted, fictional, uncalibrated; no model calls."""
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
DAY = 86400


class WorldTests(unittest.TestCase):
    def setUp(self):
        from app.world import WorldStore, WorldError
        self.Error = WorldError
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'world.sqlite'
        self.w = WorldStore(self.db)

    def test_01_initialization_real_geography_and_reopen(self):
        s = self.w.initialize()
        self.assertEqual((len(s['countries']), len(s['persons']), len(s['places'])), (4, 12, 8))
        self.assertEqual((s['time'], s['horizon'], s['status'], s['mode']), (0, 90*DAY, 'user_paused', 'scripted'))
        g = json.loads((ROOT / 'data/geography.json').read_text(encoding='utf-8'))
        regions = {r['id']: r for r in g['regions']}
        settlements = {r['source_settlement_id']: r for r in g['settlements']}
        for country in s['countries']:
            places = [p for p in s['places'] if p['country_id'] == country['id']]
            a, b = places
            self.assertIn(b['id'], [n['to'] for n in regions[a['id']]['neighbors']])
            for p in places:
                self.assertEqual(settlements[p['source_settlement_id']]['place_id'], p['id'])
                self.assertEqual(p['source_region_id'], regions[p['id']]['source_region_id'])
                self.assertIn('disease', p)
        self.assertEqual(self.w.initialize(), s)
        s['places'][0]['grain'] = -999
        self.assertGreater(self.w.snapshot()['places'][0]['grain'], 0)

    def test_02_time_control_and_history_are_readonly(self):
        initial = self.w.initialize()
        with self.assertRaises(self.Error):
            self.w.advance(DAY)
        self.w.control('resume')
        self.assertEqual(self.w.advance(DAY)['time'], DAY)
        self.w.control('pause')
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        self.assertEqual(self.w.snapshot(1), initial)
        self.assertEqual(self.w.snapshot()['status'], 'user_paused')
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before)
        for value in (True, 1.0, -1, '1'):
            with self.assertRaises(self.Error):
                self.w.advance(value)
        with self.assertRaises(self.Error):
            self.w.snapshot(True)
        with self.assertRaises(self.Error):
            self.w.snapshot(99999)

    def test_03_ninety_days_food_conservation_and_extension(self):
        initial = self.w.initialize()
        self.w.control('resume')
        s = self.w.advance(90*DAY)
        self.assertEqual((s['time'], s['status']), (90*DAY, 'horizon_paused'))
        ledger = s['ledger']
        self.assertEqual(sum(p['grain'] for p in s['places']) + sum(x['quantity'] for x in s['in_transit']) + ledger['consumed'], ledger['initial'] + ledger['harvested'])
        self.assertEqual(ledger['consumed'], 8*90*10)
        self.assertEqual(ledger['harvested'], 8*3*400)
        self.assertTrue(any(e['at'] > s['time'] for e in s['queue']))
        resumed = self.w.control('resume')
        self.assertEqual((resumed['status'], resumed['horizon']), ('running', 180*DAY))
        self.assertTrue(any(e['kind'] == 'horizon_extended' for e in resumed['events']))
        self.assertEqual(self.w.snapshot(1), initial)

    def test_04_fixed_pathology_projects_only_actual_local_symptoms(self):
        s = self.w.initialize()
        pid = 'person-country-1-marshal'
        king = 'person-country-1-king'
        self.assertEqual(self.w.observe(pid)['observations'], [])
        self.w.control('resume')
        self.w.advance(DAY-1)
        self.assertEqual(self.w.observe(pid)['observations'], [])
        day = self.w.advance(DAY)
        view = self.w.observe(pid)
        self.assertTrue(any('咳嗽' in o['text'] for o in view['observations']))
        self.assertEqual(self.w.observe(king)['observations'], [])
        self.assertEqual(set(view), {'person', 'time', 'observations', 'messages', 'contacts', 'revision'})
        self.assertEqual(view['revision'], day['revision'])
        self.assertNotIn('NON_MEDICAL', json.dumps(view))
        self.assertNotIn('disease', json.dumps(view))
        self.assertEqual(len(view['contacts']), 3)
        pathology = day['pathology']
        final = self.w.advance(5*DAY)
        self.assertEqual(final['pathology'], pathology)
        cohort = final['places'][1]['disease']['cohorts'][0]
        self.assertEqual((cohort['infected_at'], cohort['stage']), (0, 'recovered'))
        self.assertEqual(self.w.snapshot(1), s)

    def test_05_persistent_business_identity_and_strict_wait(self):
        from app.world import WorldStore
        self.w.initialize()
        self.w.control('resume')
        pid = 'person-country-1-king'
        self.assertIsNone(self.w.receipt(pid, 'w1'))
        receipt = self.w.submit(pid, 'w1', {'action':'wait'})
        self.assertEqual(set(receipt), {'world_id','person_id','intent_id','status','ruling','time','effect'})
        before = self.w.snapshot()
        self.assertEqual(WorldStore(self.db).submit(pid, 'w1', {'action':'wait'}), receipt)
        self.assertEqual(self.w.snapshot(), before)
        self.assertEqual(self.w.receipt(pid, 'w1'), receipt)
        for person, body in [(pid, {'action':'investigate'}), ('person-country-1-finance', {'action':'wait'})]:
            with self.assertRaises(self.Error):
                self.w.submit(person, 'w1', body)
        with self.assertRaises(self.Error):
            self.w.receipt('person-country-1-finance', 'w1')
        for body in ({'action':'wait','patch':{}}, {'action':'wait','person_id':pid}, {'action':True}, [], {'action':'sql'}):
            with self.assertRaises(self.Error):
                self.w.submit(pid, 'bad', body)
        with self.assertRaises(self.Error):
            self.w.submit(pid, 'bad', {'action':'wait'}, ruling='unknown')
        self.assertEqual(self.w.snapshot(), before)

    def test_06_messages_and_travel_respect_actual_arrival(self):
        self.w.initialize()
        self.w.control('resume')
        marshal, king = 'person-country-1-marshal', 'person-country-1-king'
        r = self.w.submit(marshal, 'report', {'action':'send','to':king,'text':'当地观察；并非全知真相'})
        self.assertEqual(self.w.observe(king)['messages'], [])
        self.w.advance(r['effect']['due']-1)
        self.assertEqual(self.w.observe(king)['messages'], [])
        self.w.advance(r['effect']['due'])
        self.assertEqual(self.w.observe(king)['messages'][0]['text'], '当地观察；并非全知真相')
        reply = self.w.submit(king, 'reply', {'action':'send','to':marshal,'text':'请回报'})
        journey = self.w.submit(marshal, 'move', {'action':'travel','to':'place-0'})
        self.assertIsNone(next(p for p in self.w.snapshot()['persons'] if p['id'] == marshal)['location'])
        with self.assertRaises(self.Error):
            self.w.submit(marshal, 'bad-move', {'action':'send','to':king,'text':'旅途不能瞬移信使'})
        self.w.advance(max(reply['effect']['due'], journey['effect']['due']))
        self.assertEqual(self.w.observe(marshal)['messages'], [])
        self.assertEqual(next(p for p in self.w.snapshot()['persons'] if p['id'] == marshal)['location'], 'place-0')
        self.assertTrue(any(e['kind'] == 'undeliverable' for e in self.w.snapshot()['events']))

    def test_07_technical_pause_is_persistent_not_a_character_penalty(self):
        self.w.initialize()
        pid = 'person-country-1-king'
        view = self.w.observe(pid)
        before = self.w.snapshot()
        s = self.w.technical_pause('SDK_UNAVAILABLE')
        self.assertEqual(s['status'], 'technical_paused')
        self.assertEqual(s['diagnostic']['code'], 'SDK_UNAVAILABLE')
        after_view = self.w.observe(pid)
        self.assertEqual(after_view['revision'], s['revision'])
        self.assertEqual(view['revision'], before['revision'])
        self.assertEqual({k:v for k,v in after_view.items() if k != 'revision'},
                         {k:v for k,v in view.items() if k != 'revision'})
        self.assertEqual(s['events'], before['events'])
        with self.assertRaises(self.Error):
            self.w.control('resume')
        with self.assertRaises(self.Error):
            self.w.advance(DAY)
        from app.world import WorldStore
        self.assertEqual(WorldStore(self.db).initialize(), s)

    def test_08_shipping_authority_four_rulings_and_in_transit(self):
        self.w.initialize()
        self.w.control('resume')
        finance = 'person-country-1-finance'
        initial = self.w.snapshot()
        for pid, to, quantity in [('person-country-1-king','place-1',1), (finance,'place-78',1), (finance,'place-0',1), (finance,'place-1',99999), (finance,'place-1',True), (finance,'place-1',1.0)]:
            with self.assertRaises(self.Error):
                self.w.submit(pid, 'bad-'+str(to)+str(quantity)+pid, {'action':'ship','to':to,'quantity':quantity})
        self.assertEqual(self.w.snapshot(), initial)
        receipts = []
        for i, ruling in enumerate(('大失败','失败','成功','大成功')):
            r = self.w.submit(finance, 'ship-'+str(i), {'action':'ship','to':'place-1','quantity':70}, ruling=ruling)
            self.assertEqual(r['effect']['implemented'], i >= 2)
            if i >= 2:
                receipts.append(r)
        s = self.w.snapshot()
        self.assertEqual(s['places'][0]['grain'], initial['places'][0]['grain']-140)
        self.assertEqual(s['places'][1]['grain'], initial['places'][1]['grain'])
        self.assertEqual(sum(x['quantity'] for x in s['in_transit']), 140)
        self.w.advance(max(r['effect']['due'] for r in receipts))
        end = self.w.snapshot()
        self.assertEqual(end['in_transit'], [])
        self.assertEqual(end['places'][1]['grain'], initial['places'][1]['grain']+140)
        self.assertEqual(sum(p['grain'] for p in end['places']), end['ledger']['initial'])
        self.assertEqual(self.w.receipt(finance, 'ship-2'), receipts[0])

    def test_09_command_arrival_does_not_choose_obedience(self):
        self.w.initialize()
        self.w.control('resume')
        king, marshal = 'person-country-1-king', 'person-country-1-marshal'
        self.w.advance(2*DAY)
        original = json.loads(json.dumps(self.w.snapshot()['places'][1]['disease']['cohorts']))
        r = self.w.submit(king, 'order', {'action':'command','to':marshal,'text':'请限制聚集'})
        command_id = r['effect']['command_id']
        with self.assertRaises(self.Error):
            self.w.submit(marshal, 'early', {'action':'respond','command_id':command_id,'accept':True})
        self.w.advance(r['effect']['due'])
        self.assertFalse(self.w.snapshot()['places'][1]['disease']['restricted'])
        message = self.w.observe(marshal)['messages'][-1]
        self.assertEqual((message['from'], message['command_id']), (king, command_id))
        self.w.submit(marshal, 'refuse', {'action':'respond','command_id':command_id,'accept':False})
        self.assertFalse(self.w.snapshot()['places'][1]['disease']['restricted'])
        r2 = self.w.submit(king, 'order2', {'action':'command','to':marshal,'text':'请限制聚集'})
        self.w.advance(r2['effect']['due'])
        self.w.submit(marshal, 'accept', {'action':'respond','command_id':r2['effect']['command_id'],'accept':True})
        after = self.w.snapshot()['places'][1]['disease']
        self.assertTrue(after['restricted'])
        self.assertEqual(after['cohorts'], original)
        with self.assertRaises(self.Error):
            self.w.submit(marshal, 'twice', {'action':'respond','command_id':command_id,'accept':True})
        with self.assertRaises(self.Error):
            self.w.submit('person-country-1-finance', 'usurp', {'action':'command','to':marshal,'text':'假冒命令'})
        with self.assertRaises(self.Error):
            self.w.submit(king, 'foreign', {'action':'command','to':'person-country-2-marshal','text':'越权命令'})

    def test_10_investigation_is_delayed_local_and_can_be_wrong(self):
        self.w.initialize()
        self.w.control('resume')
        self.w.advance(DAY)
        pid = 'person-country-1-marshal'
        before = len(self.w.observe(pid)['observations'])
        receipts = [self.w.submit(pid, 'investigate-'+str(i), {'action':'investigate'}, ruling=r) for i,r in enumerate(('大失败','失败','成功','大成功'))]
        self.assertEqual(len(self.w.observe(pid)['observations']), before)
        due = max(r['effect']['due'] for r in receipts)
        self.w.advance(due)
        texts = [o['text'] for o in self.w.observe(pid)['observations'][before:]]
        self.assertEqual(len(texts), 4)
        self.assertIn('未经证实', texts[0])
        self.assertIn('证据不足', texts[1])
        self.assertIn('咳嗽', texts[2])
        self.assertNotIn('NON_MEDICAL', ''.join(texts))
        r = self.w.submit(pid, 'interrupted', {'action':'investigate'})
        self.w.submit(pid, 'leave', {'action':'travel','to':'place-0'})
        self.w.advance(r['effect']['due'])
        self.assertTrue(any(e['kind']=='investigation_interrupted' for e in self.w.snapshot()['events']))
        f = self.w.submit('person-country-1-finance', 'audit', {'action':'investigate'})
        self.w.advance(f['effect']['due'])
        self.assertTrue(any('核账' in o['text'] and '粮' in o['text'] for o in self.w.observe('person-country-1-finance')['observations']))

    def test_11_card_edits_are_forward_only_and_reject_authority_fields(self):
        initial = self.w.initialize()
        pid = 'person-country-1-king'
        s = self.w.edit_card(pid, {'values':'不服从强权', 'risk':'审慎冒险'})
        person = next(p for p in s['persons'] if p['id'] == pid)
        self.assertEqual(person['card']['values'], '不服从强权')
        self.assertEqual((person['card_version'], person['card_author']), (2, 'user'))
        self.assertEqual(self.w.snapshot(1), initial)
        self.assertEqual(self.w.observe(pid)['person']['card'], person['card'])
        for changes in ({'location':'place-1'}, {'grain':100}, {'office':'king'}, {'risk':True}, {'risk':1}, {}, []):
            with self.assertRaises(self.Error):
                self.w.edit_card(pid, changes)
        self.assertEqual(self.w.snapshot(), s)
        self.assertTrue(any(e['kind']=='card_edited' for e in s['events']))


    def test_12_static_geography_is_not_rewritten_for_each_intent(self):
        initial=self.w.initialize()
        self.w.control('resume')
        before=self.db.stat().st_size
        for i in range(10):
            self.w.submit('person-country-1-king',f'bounded-growth-{i}',{'action':'wait'})
        growth=self.db.stat().st_size-before
        geography_bytes=(ROOT/'data/geography.json').stat().st_size
        self.assertLess(growth,geography_bytes*2,
                        'static geography is being duplicated into every durable snapshot')
        from app.world import WorldStore
        reopened=WorldStore(self.db)
        self.assertEqual(reopened.snapshot(1),initial)
        result=reopened.submit('person-country-1-marshal','route-after-reopen',{'action':'travel','to':'place-0'})
        self.assertGreater(result['effect']['due'],0)

    def test_13_original_full_geography_saves_remain_readable(self):
        from contextlib import closing
        from app.world import WorldStore
        initial=self.w.initialize()
        with closing(sqlite3.connect(self.db)) as db:
            raw=json.loads(db.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0])
            raw['_geography']=json.loads(db.execute("SELECT value FROM world_constants WHERE key='geography'").fetchone()[0])
        legacy=self.db.with_name('original-format.sqlite')
        legacy_json=json.dumps(raw,ensure_ascii=False,sort_keys=True)
        with closing(sqlite3.connect(legacy)) as db:
            db.executescript('CREATE TABLE snapshots(seq INTEGER PRIMARY KEY,state TEXT NOT NULL); CREATE TABLE intents(id TEXT PRIMARY KEY,person TEXT NOT NULL,payload TEXT NOT NULL,receipt TEXT);')
            db.execute('INSERT INTO snapshots VALUES(1,?)',(legacy_json,)); db.commit()
        reopened=WorldStore(legacy)
        self.assertEqual(reopened.initialize(),initial)
        reopened.control('resume')
        moved=reopened.submit('person-country-1-marshal','legacy-route',{'action':'travel','to':'place-0'})
        self.assertGreater(moved['effect']['due'],0)
        self.assertEqual(reopened.snapshot(1),initial)
        with closing(sqlite3.connect(legacy)) as db:
            self.assertEqual(db.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0],legacy_json)
            self.assertNotIn('_geography',json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0]))

if __name__ == '__main__':
    unittest.main()
