"""P07.1 fictional mechanics: real SQLite, no models or medical claims."""
from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from app.world import WorldStore, WorldError

SCRATCH = 'C:/Users/We3q/AppData/Local/hermes/cache/scratch'
ROOT = Path(__file__).resolve().parents[1]
KING = 'person-country-1-king'
MARSHAL = 'person-country-1-marshal'


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class DiseaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'world.sqlite'
        self.world = WorldStore(self.path)
        self.rules = json.loads((ROOT / 'data/scenario.json').read_text(encoding='utf-8'))['pathology']

    def test_canonical_rules_validate_types_ranges_and_preserve_all_fields(self):
        self.assertIsNotNone(importlib.util.find_spec('app.disease'), 'pure disease helper is missing')
        from app.disease import pathology_digest
        rules = self.rules | {'extension': {'note':'虚构', 'items':[1, True, None]}}
        expected = hashlib.sha256(canonical(rules).encode('utf-8')).hexdigest()
        self.assertEqual(pathology_digest(rules), expected)
        self.assertEqual(pathology_digest(dict(reversed(list(rules.items())))), expected)
        self.assertEqual(rules['extension']['items'], [1, True, None])
        invalid = [None, [], {}, rules | {'version':''}, rules | {'version':'  '}, rules | {'version':1}]
        integers = ('symptom_seconds','severe_seconds','recovery_seconds','contact_offset',
                    'open_contacts','restricted_contacts','contacts_per_infection')
        for field in integers:
            for value in (True, False, 1.0, '1', None, -1):
                invalid.append(rules | {field:value})
            missing = dict(rules)
            del missing[field]
            invalid.append(missing)
        for field in ('symptom_seconds','severe_seconds','recovery_seconds','contacts_per_infection'):
            invalid.append(rules | {field:0})
        invalid.extend([rules | {'severe_seconds':rules['symptom_seconds']},
                        rules | {'recovery_seconds':rules['severe_seconds']},
                        rules | {'restricted_contacts':rules['open_contacts']+1},
                        rules | {'extension':float('nan')}, rules | {'extension':object()}])
        for value in invalid:
            with self.subTest(value=repr(value)):
                with self.assertRaises(ValueError):
                    pathology_digest(value)
        for value in (0, 1):
            allowed = rules | {'contact_offset':value, 'open_contacts':value, 'restricted_contacts':value}
            self.assertEqual(len(pathology_digest(allowed)), 64)

    def test_initial_digest_exists_before_first_case_and_invalid_rules_are_atomic(self):
        infect = self.world._infect
        seen = []
        def first_case(state, place, number):
            self.assertIn('pathology_sha256', state, 'identity must precede any case')
            self.assertEqual(state['pathology_sha256'], hashlib.sha256(canonical(self.rules).encode()).hexdigest())
            self.assertEqual(state['pathology_frozen_at'], 0)
            seen.append(True)
            return infect(state, place, number)
        with patch.object(self.world, '_infect', side_effect=first_case):
            initial = self.world.initialize()
        self.assertEqual(len(seen), 1)
        self.assertEqual(initial['revision'], 1)
        self.assertEqual(self.world.snapshot(1), initial)
        config = json.loads((ROOT / 'data/scenario.json').read_text(encoding='utf-8'))
        for index, value in enumerate((None, {}, self.rules | {'symptom_seconds':True}, self.rules | {'contact_offset':-1})):
            with self.subTest(value=value):
                scenario = Path(self.tmp.name) / f'invalid-{index}.json'
                scenario.write_text(canonical(config | {'pathology':value}), encoding='utf-8')
                bad = WorldStore(Path(self.tmp.name) / f'invalid-{index}.sqlite', scenario)
                with patch.object(bad, '_infect') as seed:
                    with self.assertRaises(WorldError) as error:
                        bad.initialize()
                    self.assertEqual(error.exception.code, 'INVALID_PATHOLOGY')
                    seed.assert_not_called()
                with closing(sqlite3.connect(bad.db_path)) as db:
                    self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])
        for person in initial['persons']:
            encoded = canonical(self.world.observe(person['id']))
            self.assertNotIn(initial['pathology_sha256'], encoded)
            self.assertNotIn('pathology', encoded)
            self.assertNotIn(self.rules['version'], encoded)

    def rows(self, world=None):
        with closing(sqlite3.connect((world or self.world).db_path)) as db:
            return (db.execute('SELECT seq,state FROM snapshots ORDER BY seq').fetchall(),
                    db.execute('SELECT * FROM intents ORDER BY id').fetchall())

    def test_freeze_post_guard_rolls_back_rules_config_time_digest_and_sql(self):
        self.world.initialize()
        self.world.control('resume')
        before = self.rows()
        def rule(state):
            state['pathology']['open_contacts'] += 1
        def config(state):
            state['_config']['pathology']['open_contacts'] += 1
        def time(state):
            state['pathology_frozen_at'] = 1
        def boolean_time(state):
            state['pathology_frozen_at'] = False
        def digest(state):
            state['pathology_sha256'] = '0'*64
        def remove(state):
            del state['pathology_sha256']
        def coordinated(state):
            state['pathology']['open_contacts'] += 1
            state['_config']['pathology'] = dict(state['pathology'])
            state['pathology_sha256'] = hashlib.sha256(canonical(state['pathology']).encode()).hexdigest()
        def extension(state):
            state['pathology']['unknown-extra'] = 'not in original identity'
        for tamper in (rule, config, time, boolean_time, digest, remove, coordinated, extension):
            with self.subTest(tamper=tamper.__name__):
                self.world = WorldStore(Path(self.tmp.name) / (tamper.__name__+'.sqlite'))
                self.world.initialize()
                self.world.control('resume')
                before = self.rows()
                def operation(state, db):
                    tamper(state)
                    state['places'][0]['grain'] -= 10
                    state['queue'].clear()
                    db.execute('INSERT INTO intents VALUES (?,?,?,?)', ('injected',KING,'{}','{}'))
                with self.assertRaises(WorldError) as error:
                    self.world._mutate(operation)
                self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
                self.assertEqual(self.rows(), before)
        paused = self.world.technical_pause('FROZEN_PATHOLOGY')
        self.assertEqual(paused['status'], 'technical_paused')
        self.assertEqual(paused['events'], self.world.snapshot(before[0][-1][0])['events'])
        self.assertEqual(self.rows()[0][:len(before[0])], before[0])

    def append_corrupt(self, mutate):
        with closing(sqlite3.connect(self.world.db_path)) as db:
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            state['revision'] += 1
            mutate(state)
            db.execute('INSERT INTO snapshots VALUES (?,?)', (state['revision'], canonical(state)))
            db.commit()

    def test_pre_guard_rejects_latest_corruption_before_operation_runs(self):
        self.world.initialize()
        self.world.control('resume')
        self.append_corrupt(lambda state: state['pathology'].update(open_contacts=21))
        before = self.rows()
        operation = unittest.mock.Mock()
        with self.assertRaises(WorldError) as error:
            self.world._mutate(operation)
        self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
        operation.assert_not_called()
        self.assertEqual(self.rows(), before)

    def test_submit_checks_freeze_before_durable_intent_reservation(self):
        self.world.initialize()
        self.world.control('resume')
        self.append_corrupt(lambda state: state['_config']['pathology'].update(open_contacts=21))
        before = self.rows()
        with self.assertRaises(WorldError) as error:
            self.world.submit(KING, 'must-not-reserve', {'action':'wait'})
        self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
        self.assertEqual(self.rows(), before)

    def order(self, suffix, text='请限制聚集', target=MARSHAL):
        receipt = self.world.submit(KING, 'order-'+suffix, {'action':'command','to':target,'text':text})
        self.assertNotIn('contact_restriction', receipt['effect'])
        self.world.advance(receipt['effect']['due'])
        return receipt['effect']['command_id']

    def test_actual_accepted_response_receipt_identifies_frozen_mechanism(self):
        initial = self.world.initialize()
        self.world.control('resume')
        for index, ruling in enumerate(('成功','大成功')):
            command = self.order(str(index))
            before = self.world.snapshot()
            body = {'action':'respond','command_id':command,'accept':True}
            receipt = self.world.submit(MARSHAL, 'accept-'+str(index), body, ruling=ruling)
            self.assertIn('contact_restriction', receipt['effect'])
            self.assertEqual(receipt['effect']['contact_restriction'], {
                'place_id':'place-1', 'before':bool(index), 'after':True,
                'mechanism_sha256':initial['pathology_sha256']})
            after = self.world.snapshot()
            self.assertTrue(after['places'][1]['disease']['restricted'])
            self.assertEqual(after['places'][1]['disease']['cohorts'], before['places'][1]['disease']['cohorts'])
            self.assertEqual(after['queue'], before['queue'])
            self.assertEqual(after['ledger'], before['ledger'])
            rows = self.rows()
            self.assertEqual(WorldStore(self.path).submit(MARSHAL, 'accept-'+str(index), body, ruling=ruling), receipt)
            self.assertEqual(self.rows(), rows)

    def test_first_transition_emits_causal_event_and_only_local_natural_notice(self):
        initial = self.world.initialize()
        self.world.control('resume')
        finance = 'person-country-1-finance'
        journey = self.world.submit(finance, 'onsite', {'action':'travel','to':'place-1'})
        self.world.advance(journey['effect']['due'])
        command = self.order('causal')
        before = {p['id']:self.world.observe(p['id'])['observations'] for p in initial['persons']}
        body = {'action':'respond','command_id':command,'accept':True}
        receipt = self.world.submit(MARSHAL, 'accept-causal', body)
        applied = [e for e in self.world.snapshot()['events'] if e['kind'] == 'contact_restriction_applied']
        self.assertEqual(len(applied), 1, 'actual first intervention must have an event')
        self.assertEqual(applied[0]['cause_id'], 'accept-causal')
        self.assertEqual(applied[0]['person_id'], MARSHAL)
        for person in initial['persons']:
            pid = person['id']
            added = self.world.observe(pid)['observations'][len(before[pid]):]
            if pid in (MARSHAL, finance):
                local = [o for o in added if o['source'].startswith('亲见：')]
                self.assertEqual(len(local), 1)
                self.assertIn('限制聚集', local[0]['text'])
                self.assertNotIn(initial['pathology_sha256'], canonical(added))
                self.assertNotIn(self.rules['version'], canonical(added))
                self.assertNotIn('susceptible', canonical(added))
            else:
                self.assertEqual(added, [])
        rows = self.rows()
        self.assertEqual(self.world.submit(MARSHAL, 'accept-causal', body), receipt)
        self.assertEqual(self.rows(), rows)
        second = self.order('maintain')
        self.world.submit(MARSHAL, 'accept-maintain', {'action':'respond','command_id':second,'accept':True})
        self.assertEqual([e for e in self.world.snapshot()['events'] if e['kind'] == 'contact_restriction_applied'], applied)

    def test_legacy_without_digest_keeps_original_format_and_metadata_revision(self):
        from app.adjudication import Adjudicator
        self.world.initialize()
        with closing(sqlite3.connect(self.path)) as db:
            raw = json.loads(db.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0])
            raw['_geography'] = json.loads(db.execute("SELECT value FROM world_constants WHERE key='geography'").fetchone()[0])
        del raw['pathology_sha256']
        raw['future_snapshot_field'] = {'keep':['unknown', True]}
        raw['pathology']['future_rule_field'] = {'keep':'旧机制扩展'}
        raw['_config']['pathology']['future_rule_field'] = {'keep':'旧机制扩展'}
        legacy = WorldStore(Path(self.tmp.name) / 'legacy.sqlite')
        original = canonical(raw)
        with closing(sqlite3.connect(legacy.db_path)) as db:
            db.executescript('CREATE TABLE snapshots(seq INTEGER PRIMARY KEY,state TEXT NOT NULL); CREATE TABLE intents(id TEXT PRIMARY KEY,person TEXT NOT NULL,payload TEXT NOT NULL,receipt TEXT);')
            db.execute('INSERT INTO snapshots VALUES (1,?)', (original,))
            db.commit()
        fingerprint = hashlib.sha256(legacy.db_path.read_bytes()).hexdigest()
        initial = legacy.initialize()
        self.assertNotIn('pathology_sha256', initial)
        self.assertEqual(initial, legacy.snapshot(1))
        legacy.observe(MARSHAL, 1)
        self.assertEqual(hashlib.sha256(legacy.db_path.read_bytes()).hexdigest(), fingerprint)
        engine = Adjudicator(legacy)
        self.assertEqual(legacy.snapshot(), initial)
        legacy.control('resume')
        base = legacy.snapshot()
        engine.propose(MARSHAL, 'meta', '等待现场消息')
        decision = dict(world_id=base['world_id'], base_revision=base['revision'], ruling='成功',
                        reason='元数据不自过期', goal='等待', costs=[], benefits=[], effects=[{'action':'wait'}])
        version = engine.record(MARSHAL, 'meta', decision)
        self.assertEqual(legacy.snapshot(), base)
        self.assertEqual(engine.preview(MARSHAL, 'meta', version)['base_revision'], base['revision'])
        engine.commit(MARSHAL, 'meta', version)
        self.world = legacy
        command = self.order('legacy')
        receipt = legacy.submit(MARSHAL, 'accept-legacy', {'action':'respond','command_id':command,'accept':True})
        self.assertEqual(receipt['effect']['contact_restriction']['mechanism_sha256'], hashlib.sha256(canonical(raw['pathology']).encode()).hexdigest())
        self.assertNotIn('pathology_sha256', legacy.snapshot())
        self.assertEqual(legacy.snapshot()['future_snapshot_field'], raw['future_snapshot_field'])
        self.assertEqual(legacy.snapshot(1), initial)
        self.assertEqual(self.rows(legacy)[0][0], (1, original))
        before = self.rows()
        with self.assertRaises(WorldError) as error:
            legacy._mutate(lambda state, db: state.update(pathology_sha256=receipt['effect']['contact_restriction']['mechanism_sha256']))
        self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
        self.assertEqual(self.rows(), before)

    def test_characterization_only_future_contacts_change_not_cohorts_or_grain(self):
        outcomes = []
        for restricted in (False, True):
            world = WorldStore(Path(self.tmp.name) / f'contact-{restricted}.sqlite')
            initial = world.initialize()
            world.control('resume')
            world.advance(86400)
            self.world = world
            command = self.order('counterfactual')
            before = world.snapshot()
            self.assertEqual(before['places'][1]['disease']['cohorts'][0]['stage'], 'symptomatic')
            world.submit(MARSHAL, 'choice', {'action':'respond','command_id':command,'accept':restricted})
            after = world.snapshot()
            self.assertEqual(after['places'][1]['disease']['cohorts'], before['places'][1]['disease']['cohorts'])
            self.assertEqual(after['queue'], before['queue'])
            future = world.advance(2*86400)
            ledger = future['ledger']
            self.assertEqual(sum(p['grain'] for p in future['places']) + sum(s['quantity'] for s in future['in_transit']) + ledger['consumed'], ledger['initial'] + ledger['harvested'])
            self.assertEqual(future['pathology'], initial['pathology'])
            self.assertEqual(world.snapshot(1), initial)
            outcomes.append(future)
        open_world, restricted_world = outcomes
        open_cases = sum(c['count'] for c in open_world['places'][1]['disease']['cohorts'])
        restricted_cases = sum(c['count'] for c in restricted_world['places'][1]['disease']['cohorts'])
        self.assertGreater(open_cases, restricted_cases)
        self.assertEqual(open_world['ledger'], restricted_world['ledger'])
        self.assertEqual([p['grain'] for p in open_world['places']], [p['grain'] for p in restricted_world['places']])
        self.assertEqual(open_world['places'][1]['disease']['cohorts'][0], restricted_world['places'][1]['disease']['cohorts'][0])

    def test_characterization_delivery_authority_location_rulings_and_noninterventions(self):
        self.world.initialize()
        self.world.control('resume')
        order = self.world.submit(KING, 'pending', {'action':'command','to':MARSHAL,'text':'限制聚集'})
        command = order['effect']['command_id']
        body = {'action':'respond','command_id':command,'accept':True}
        for pid in (MARSHAL, KING, 'person-country-1-finance'):
            with self.assertRaises(WorldError) as error:
                self.world.submit(pid, 'premature-'+pid, body)
            self.assertEqual(error.exception.code, 'COMMAND_NOT_DELIVERED')
        self.world.advance(order['effect']['due'])
        for index, ruling in enumerate(('大失败','失败')):
            receipt = self.world.submit(MARSHAL, 'failed-response-'+str(index), body, ruling=ruling)
            self.assertEqual(receipt['effect'], {'implemented':False})
        self.assertFalse(self.world.snapshot()['places'][1]['disease']['restricted'])
        refusal = self.world.submit(MARSHAL, 'refuse', body | {'accept':False})
        self.assertEqual(set(refusal['effect']), {'implemented','accepted','command_id'})
        with self.assertRaises(WorldError) as error:
            self.world.submit(MARSHAL, 'respond-again', body)
        self.assertEqual(error.exception.code, 'ALREADY_RESPONDED')
        for suffix, text, target in [('ordinary','请巡视现场',MARSHAL), ('finance','限制聚集','person-country-1-finance')]:
            command = self.order(suffix, text, target)
            receipt = self.world.submit(target, 'accept-'+suffix, {'action':'respond','command_id':command,'accept':True})
            self.assertEqual(set(receipt['effect']), {'implemented','accepted','command_id'})
        for pid, target in [('person-country-1-finance',MARSHAL), (KING,'person-country-2-marshal')]:
            with self.assertRaises(WorldError) as error:
                self.world.submit(pid, 'usurp-'+target, {'action':'command','to':target,'text':'限制聚集'})
            self.assertEqual(error.exception.code, 'NOT_AUTHORIZED')
        command = self.order('leave')
        self.world.submit(MARSHAL, 'leave', {'action':'travel','to':'place-0'})
        with self.assertRaises(WorldError) as error:
            self.world.submit(MARSHAL, 'away', {'action':'respond','command_id':command,'accept':True})
        self.assertEqual(error.exception.code, 'NOT_AT_LOCATION')
        self.assertFalse(any(p['disease']['restricted'] for p in self.world.snapshot()['places']))
        self.assertFalse(any(e['kind']=='contact_restriction_applied' for e in self.world.snapshot()['events']))
        self.assertFalse(any('限制聚集' in o['text'] for person in self.world.snapshot()['persons'] for o in self.world.observe(person['id'])['observations']))

    def test_characterization_new_chain_trace_exposes_only_committed_actual_effect(self):
        from app.adjudication import Adjudicator
        self.world.initialize()
        self.world.control('resume')
        command = self.order('trace')
        engine = Adjudicator(self.world)
        base = self.world.snapshot()
        engine.propose(MARSHAL, 'respond-raw', '我接受来信，开始限制聚集')
        decision = dict(world_id=base['world_id'], base_revision=base['revision'], ruling='成功',
                        reason='已送达且本人在场', goal='执行', costs=[], benefits=[],
                        effects=[{'action':'respond','command_id':command,'accept':True}])
        version = engine.record(MARSHAL, 'respond-raw', decision)
        self.assertEqual(self.world.snapshot(), base)
        projected = engine.preview(MARSHAL, 'respond-raw', version)
        self.assertFalse(self.world.snapshot()['places'][1]['disease']['restricted'])
        receipt = engine.commit(MARSHAL, 'respond-raw', version)
        self.assertEqual(projected['effects'], receipt['effect']['effects'])
        proposal = engine.trace()['proposals'][0]
        self.assertEqual(proposal['committed']['effects'], receipt['effect']['effects'])
        self.assertTrue(proposal['committed']['effects'][0]['contact_restriction']['after'])
        event = next(e for e in self.world.snapshot()['events'] if e['kind']=='contact_restriction_applied')
        self.assertEqual(event['cause_id'], engine._child_id('respond-raw', version, 0))
        rows = self.rows()
        self.assertEqual(engine.commit(MARSHAL, 'respond-raw', version), receipt)
        self.assertEqual(self.rows(), rows)
        self.assertEqual(self.world.snapshot(base['revision']), base)
        self.assertNotIn('pathology_sha256', canonical(self.world.observe(MARSHAL)))

    def test_characterization_pre_guard_uses_first_snapshot_for_every_identity_field(self):
        variants = (
            lambda s: s['pathology'].update(version='REPLACED'),
            lambda s: s['_config']['pathology'].update(contact_offset=0),
            lambda s: s.update(pathology_frozen_at=False),
            lambda s: s.update(pathology_sha256='0'*64),
            lambda s: s.pop('pathology_sha256'),
        )
        for index, corrupt in enumerate(variants):
            with self.subTest(index=index):
                self.world = WorldStore(Path(self.tmp.name) / f'pre-{index}.sqlite')
                self.world.initialize()
                self.world.control('resume')
                self.append_corrupt(corrupt)
                before = self.rows()
                operation = unittest.mock.Mock()
                with self.assertRaises(WorldError) as error:
                    self.world._mutate(operation)
                self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
                operation.assert_not_called()
                self.assertEqual(self.rows(), before)
                with self.assertRaises(WorldError) as error:
                    self.world.submit(KING, 'pre-no-reservation', {'action':'wait'})
                self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
                self.assertEqual(self.rows(), before)

    def test_characterization_freeze_is_rechecked_in_second_submit_transaction(self):
        self.world.initialize()
        self.world.control('resume')
        mutate = self.world._mutate
        before = self.rows()[0]
        def between(operation):
            def corrupt(state):
                state['pathology']['open_contacts'] += 1
                state['_config']['pathology'] = dict(state['pathology'])
                state['pathology_sha256'] = hashlib.sha256(canonical(state['pathology']).encode()).hexdigest()
            self.append_corrupt(corrupt)
            return mutate(operation)
        with patch.object(self.world, '_mutate', side_effect=between):
            with self.assertRaises(WorldError) as error:
                self.world.submit(KING, 'interleaved', {'action':'wait'})
        self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
        snapshots, intents = self.rows()
        self.assertEqual(snapshots[:len(before)], before)
        # One synthetic competing append, no effect snapshot from the rejected submit.
        self.assertEqual(len(snapshots), len(before)+1)
        self.assertEqual(len(intents), 1)
        self.assertIsNone(intents[0][3])  # Existing durable prepare semantics retained.
        self.assertEqual(self.world.snapshot()['events'], self.world.snapshot(before[-1][0])['events'])

    def test_characterization_undeliverable_command_cannot_apply_intervention(self):
        self.world.initialize()
        self.world.control('resume')
        receipt = self.world.submit(KING, 'will-miss', {'action':'command','to':MARSHAL,'text':'限制聚集'})
        self.world.submit(MARSHAL, 'move-first', {'action':'travel','to':'place-0'})
        self.world.advance(receipt['effect']['due'])
        self.assertTrue(any(e['kind']=='undeliverable' for e in self.world.snapshot()['events']))
        with self.assertRaises(WorldError) as error:
            self.world.submit(MARSHAL, 'never-received', {'action':'respond','command_id':receipt['effect']['command_id'],'accept':True})
        self.assertEqual(error.exception.code, 'COMMAND_NOT_DELIVERED')
        self.assertFalse(self.world.snapshot()['places'][1]['disease']['restricted'])
        self.assertFalse(any(e['kind']=='contact_restriction_applied' for e in self.world.snapshot()['events']))


if __name__ == '__main__':
    unittest.main()
