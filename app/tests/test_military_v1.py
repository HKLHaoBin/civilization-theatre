"""966 prospective actual-source military regressions. NOT RUN in source-only preparation.
No fake WorldStore/SQL authority; all positives commit via original Adjudicator.
Fictional parameters; fixtures create fresh owned files and retain them for audit.
"""
import copy
import json
import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from app.world import WorldStore, WorldError, DAY
from app.adjudication import Adjudicator
from app.scenario import validate_scenario, ScenarioError
from app import world as WM

DATA = Path(WM.__file__).with_name('data')
SCRATCH = Path.home()/'AppData/Local/hermes/cache/scratch'
MARSHAL = 'person-country-1-marshal'
KING2 = 'person-country-2-king'
FINANCE2 = 'person-country-2-finance'


def configuration(economic=False):
    c = json.loads((DATA/'scenarios/military-v1-iron-4.json').read_bytes())
    if economic:
        c['schema_version'] = 3
        del c['harvest_day']; del c['harvest_quantity']
        places = [p for country in c['countries'] for p in country['places']]
        c['economy'] = dict(version='p08-production-economy-v1', assumptions='Uncalibrated game units.',
            units=dict(grain='grain_unit', iron='iron_unit', tool='tool_unit'),
            initial_materials={p['id']: dict(iron=10, tool=0) for p in places},
            labor_hours_per_day={p['id']: 8 for p in places},
            recipes=dict(forge=dict(inputs=dict(iron=1), outputs=dict(tool=1), labor_hours=8, duration_seconds=DAY)))
    return c


def read_sql(path):
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        names = [r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {n: db.execute('SELECT * FROM "'+n.replace('"', '""')+'"').fetchall() for n in names}


def army(s, aid='army-1'):
    return next(a for a in s['military']['armies'] if a['id'] == aid)


def place(s, pid):
    return next(p for p in s['places'] if p['id'] == pid)


class MilitaryVerticalTests(unittest.TestCase):
    def setUp(self):
        self.root = SCRATCH/('parent966-military-'+uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.counter = 0

    def own(self, c=None):
        scenario = self.root/'scenario.json'
        scenario.write_text(json.dumps(configuration() if c is None else c, ensure_ascii=False), encoding='utf-8')
        w = WorldStore(self.root/'world.sqlite', scenario); w.initialize(); w.control('resume')
        a = Adjudicator(w)
        return w, a

    def prepare(self, w, a, effects, person=MARSHAL, ruling='成功'):
        self.counter += 1; intent = 'military-intent-'+str(self.counter)
        a.propose(person, intent, '我选择指挥自己的在场部队；客观结果交世界裁定。')
        s = w.snapshot()
        v = a.record(person, intent, dict(world_id=s['world_id'], base_revision=s['revision'], ruling=ruling,
            reason='DM依据实际在场兵粮及显式防御条件裁定', goal='军事行动', costs=[], benefits=[], effects=effects))
        return person, intent, v

    def apply(self, w, a, effects, person=MARSHAL, ruling='成功'):
        args = self.prepare(w, a, effects, person, ruling)
        a.preview(*args)
        return a.commit(*args)

    def denied(self, w, a, effect, code, person=MARSHAL):
        args = self.prepare(w, a, [effect], person)
        before = w.snapshot(); sql = read_sql(w.db_path)
        with self.assertRaises(WorldError) as cm:
            a.commit(*args)
        self.assertEqual(cm.exception.code, code)
        self.assertEqual(w.snapshot(), before)
        self.assertEqual(read_sql(w.db_path), sql)
        self.assertIsNone(w.receipt(args[0], args[1]))

    def march(self, w, a, target='place-10'):
        receipt = self.apply(w, a, [dict(action='military_move', army_id='army-1', to=target)])
        due = receipt['effect']['effects'][0]['due']
        w.submit(MARSHAL, 'person-travel-'+str(self.counter), dict(action='travel', to=target))
        return due

    def balance(self, s):
        held = sum(a['grain'] for a in s['military']['armies'])
        total = sum(p['grain'] for p in s['places']) + sum(t['quantity'] for t in s['in_transit']) + held
        self.assertEqual(total + s['ledger']['consumed'], s['ledger']['initial'] + s['ledger']['harvested'])
        for initial in configuration()['military']['armies']:
            current = army(s, initial['id'])
            lost = sum(r['troops'] for r in s['military']['records'] if r['army_id'] == initial['id'])
            self.assertEqual(initial['strength'], current['strength'] + lost)

    def capture(self, w, a):
        due = self.march(w, a); w.advance(due)
        self.denied(w, a, dict(action='military_garrison', army_id='army-1'), 'HOSTILE_PRESENT')
        self.apply(w, a, [dict(action='military_engage', army_id='army-1', losses={'army-1': 1, 'army-2': 4})])
        self.apply(w, a, [dict(action='military_garrison', army_id='army-1')])

    def test_genesis_conserves_grain_and_does_not_double_count_civilian_labor(self):
        c = configuration(economic=True); w, a = self.own(c); s = w.snapshot()
        civilians = [p for country in c['countries'] for p in country['places']]
        self.assertEqual(sum(p['population'] for p in s['places']), sum(p['population'] for p in civilians))
        self.assertEqual(s['economy']['labor_capacity_hours_per_day'], c['economy']['labor_hours_per_day'])
        self.assertEqual(place(s, 'place-1')['grain'], next(p['grain'] for p in civilians if p['id'] == 'place-1') - 2000)
        self.balance(s)

    def test_preview_supply_no_side_effect_and_original_receipt_replay(self):
        w, a = self.own(); args = self.prepare(w, a, [dict(action='military_supply', army_id='army-1', quantity=7)])
        before = w.snapshot(); sql = read_sql(w.db_path); stat = w.db_path.stat()
        projected = a.preview(*args)['projected']
        self.assertEqual(army(projected)['grain'], army(before)['grain'] + 7)
        self.assertEqual(w.snapshot(), before); self.assertEqual(read_sql(w.db_path), sql)
        self.assertEqual(w.db_path.stat().st_mtime_ns, stat.st_mtime_ns)
        receipt = a.commit(*args); after = w.snapshot(); rows = read_sql(w.db_path)
        self.assertEqual(a.commit(*args), receipt)
        self.assertEqual(w.receipt(args[0], args[1]), receipt)
        self.assertEqual(w.snapshot(), after); self.assertEqual(read_sql(w.db_path), rows)
        self.balance(after)

    def test_rollback_later_effect_preserves_earlier_supply_and_child_rows(self):
        w, a = self.own()
        args = self.prepare(w, a, [dict(action='military_supply', army_id='army-1', quantity=9),
                                  dict(action='military_supply', army_id='army-2', quantity=1)])
        before = w.snapshot(); rows = read_sql(w.db_path)
        with self.assertRaises(WorldError) as cm:
            a.commit(*args)
        self.assertEqual(cm.exception.code, 'NOT_AUTHORIZED')
        self.assertEqual(w.snapshot(), before); self.assertEqual(read_sql(w.db_path), rows)
        for index in (0, 1):
            self.assertIsNone(w.receipt(MARSHAL, a._child_id(args[1], args[2], index)))

    def test_march_exact_arrival_same_time_independent_person_and_no_double_defense(self):
        w, a = self.own()
        self.apply(w, a, [dict(action='military_garrison', army_id='army-1')])
        before = w.snapshot(); due = self.march(w, a)
        moving = army(w.snapshot())
        self.assertIsNone(moving['location']); self.assertEqual(moving['stance'], 'marching')
        self.assertEqual(moving['journey']['from'], 'place-1')
        self.assertTrue(all(not (x['id'] == 'army-1' and x['location'] in ('place-1', 'place-10')) for x in w.snapshot()['military']['armies']))
        w.advance(due - 1); self.assertIsNone(army(w.snapshot())['location'])
        self.assertEqual(w.snapshot()['military']['control']['place-10'], 'country-2')
        w.advance(due); s = w.snapshot()
        self.assertEqual(army(s)['location'], 'place-10'); self.assertEqual(army(s)['stance'], 'field')
        self.assertEqual(next(p for p in s['persons'] if p['id'] == MARSHAL)['location'], 'place-10')
        self.assertEqual(s['military']['control']['place-10'], 'country-2')
        arrival = next(e for e in s['events'] if e['kind'] == 'military_arrived')
        self.assertEqual(arrival['time'], due)
        self.balance(s); self.assertEqual(w.snapshot(before['revision']), before)

    def test_not_present_no_remote_command_or_officer_teleport(self):
        w, a = self.own()
        self.apply(w, a, [dict(action='military_move', army_id='army-1', to='place-10')])
        self.denied(w, a, dict(action='military_supply', army_id='army-1', quantity=1), 'NOT_AT_LOCATION')
        due = army(w.snapshot())['journey']['due']; w.advance(due)
        self.assertEqual(next(p for p in w.snapshot()['persons'] if p['id'] == MARSHAL)['location'], 'place-1')
        self.denied(w, a, dict(action='military_garrison', army_id='army-1'), 'NOT_AT_LOCATION')

    def test_hostile_access_and_finite_supply_and_boolean_rejected(self):
        w, a = self.own()
        self.denied(w, a, dict(action='military_supply', army_id='army-2', quantity=1), 'NOT_AUTHORIZED')
        self.denied(w, a, dict(action='military_supply', army_id='army-1', quantity=2**62), 'INSUFFICIENT_GRAIN')
        self.denied(w, a, dict(action='military_supply', army_id='army-1', quantity=True), 'INVALID_BODY')
        self.denied(w, a, dict(action='military_garrison', army_id='army-1'), 'NOT_AUTHORIZED', person='person-country-1-finance')

    def test_no_grain_blocks_action_then_clock_causes_real_attrition(self):
        c = configuration(); c['military']['armies'][0]['grain'] = 5
        w, a = self.own(c)
        self.denied(w, a, dict(action='military_move', army_id='army-1', to='place-10'), 'INSUFFICIENT_GRAIN')
        w.advance(DAY); s = w.snapshot()
        self.assertEqual((army(s)['strength'], army(s)['grain']), (5, 0))
        self.assertEqual(next(r for r in s['military']['records'] if r['army_id']=='army-1')['troops'], 7)
        w.advance(DAY*2); self.assertEqual(army(w.snapshot())['strength'], 0)
        self.denied(w, a, dict(action='military_garrison', army_id='army-1'), 'NO_TROOPS')
        self.balance(w.snapshot())

    def test_long_advance_does_not_skip_arrival_or_ration_nodes_and_reopens(self):
        w, a = self.own(); due = self.march(w, a)
        target = max(due + DAY, DAY*8); w.advance(target)
        s = w.snapshot(); self.assertEqual(army(s)['location'], 'place-10')
        self.assertEqual(len([e for e in s['events'] if e['kind']=='military_arrived']), 1)
        self.assertEqual(len([r for r in s['military']['records'] if r['kind']=='rations']), (target//DAY)*2)
        self.balance(s)
        reopened = WorldStore(w.db_path, w.scenario_path); self.assertEqual(reopened.initialize(), s)
        reopened.advance(target); self.assertEqual(reopened.snapshot(), s)

    def test_legal_battle_controls_real_warehouses_and_denies_old_owner(self):
        w, a = self.own(configuration(economic=True)); self.capture(w, a); s = w.snapshot()
        self.assertEqual(s['military']['control']['place-10'], 'country-1')
        self.assertEqual(place(s, 'place-10')['country_id'], 'country-2', 'occupation is not annexation')
        self.assertEqual(army(s, 'army-2')['strength'], 0)
        self.assertEqual(army(s)['strength'], 11)
        before = place(s, 'place-10')['grain']
        self.apply(w, a, [dict(action='military_supply', army_id='army-1', quantity=13)])
        self.assertEqual(place(w.snapshot(), 'place-10')['grain'], before - 13)
        for effect in (dict(action='ship', to='place-78', quantity=1),
                       dict(action='start_production', recipe_id='forge', quantity=1),
                       dict(action='ship_material', to='place-78', resource_id='iron', quantity=1)):
            self.denied(w, a, effect, 'NOT_AUTHORIZED', person=FINANCE2)
        self.balance(w.snapshot())

    def test_battle_cannot_ignore_defenders_overkill_or_choose_active_person_surrender(self):
        w, a = self.own(); due = self.march(w, a); w.advance(due)
        people = copy.deepcopy(w.snapshot()['persons'])
        for losses in ({'army-1': 0, 'army-2': 5}, {'army-1': 0, 'army-3': 1}, {'army-1': 0, 'army-2': 0}):
            self.denied(w, a, dict(action='military_engage', army_id='army-1', losses=losses), 'INVALID_BATTLE')
        self.apply(w, a, [dict(action='military_engage', army_id='army-1', losses={'army-1': 1, 'army-2': 4})])
        self.assertEqual(w.snapshot()['persons'], people)
        self.assertEqual(w.snapshot()['military']['control']['place-10'], 'country-2', 'battle alone is not occupation')
        self.denied(w, a, dict(action='military_engage', army_id='army-1', losses={'army-1': 0, 'army-2': 1}), 'NO_ENEMY')

    def test_explicit_terrain_and_all_defending_armies_constrain_dm_losses(self):
        c = configuration(); c['military']['defense_multiplier']['place-10'] = 4
        c['military']['armies'].append(dict(id='army-3',name='第三军',country_id='country-2',location='place-10',strength=2,grain=2000))
        w, a = self.own(c); due = self.march(w,a); w.advance(due)
        self.denied(w,a,dict(action='military_engage',army_id='army-1',losses={'army-1':0,'army-2':1}),'INVALID_BATTLE')
        self.denied(w,a,dict(action='military_engage',army_id='army-1',losses={'army-1':1,'army-2':4,'army-3':0}),'INVALID_BATTLE')
        self.apply(w,a,[dict(action='military_engage',army_id='army-1',losses={'army-1':1,'army-2':2,'army-3':1})])
        self.assertEqual(army(w.snapshot(),'army-2')['strength'],2)
        self.assertEqual(army(w.snapshot(),'army-3')['strength'],1)
        self.denied(w,a,dict(action='military_garrison',army_id='army-1'),'HOSTILE_PRESENT')

    def test_observation_local_only_and_own_exact_strength_not_enemy_or_remote(self):
        w, a = self.own()
        local = w.observe(MARSHAL); remote = w.observe('person-country-1-king')
        text = json.dumps(local, ensure_ascii=False); hidden = json.dumps(remote, ensure_ascii=False)
        self.assertIn('当场点验兵力12人', text)
        self.assertNotIn('当场点验兵力12人', hidden)
        self.assertNotIn('第二军', text)
        due = self.march(w, a); w.advance(due)
        enemy_text = json.dumps(w.observe(KING2), ensure_ascii=False)
        self.assertIn('第一军', enemy_text)
        self.assertNotIn('当场点验兵力12人', enemy_text)
        self.assertNotIn('defense_multiplier', enemy_text)
        self.assertNotIn('military_record', enemy_text)

    def test_optional_extension_legacy_has_no_armies_and_invalid_genesis_rejected(self):
        geography = json.loads((DATA/'geography.json').read_bytes())
        legacy = json.loads((DATA/'scenarios/iron-4.json').read_bytes())
        self.assertNotIn('military', validate_scenario(legacy, geography))
        for mutate in (lambda c: c['military']['armies'][0].update(strength=True),
                       lambda c: c['military']['armies'][0].update(grain=10**12),
                       lambda c: c['military']['armies'][0].update(id=MARSHAL),
                       lambda c: c['military'].update(population_basis='civilian-double-count')):
            c = configuration(); mutate(c)
            with self.assertRaises(ScenarioError): validate_scenario(c, geography)
        w, a = self.own(legacy); self.assertNotIn('military', w.snapshot())

    def test_frozen_rules_and_forged_arrival_or_control_fail_without_world_write(self):
        w, a = self.own(); before = w.snapshot()
        for change in (lambda s: s['_config']['military'].update(grain_per_soldier_day=2),
                       lambda s: s['military']['control'].update({'place-10':'country-1'}),
                       lambda s: army(s).update(strength=13)):
            with self.assertRaises(WorldError):
                w._mutate(lambda s, db: change(s))
            self.assertEqual(w.snapshot(), before)
        self.apply(w, a, [dict(action='military_move', army_id='army-1', to='place-10')])
        before = w.snapshot()
        with self.assertRaises(WorldError):
            w._mutate(lambda s, db: s['queue'].__setitem__(slice(None), [q for q in s['queue'] if q['kind']!='military_arrival']))
        self.assertEqual(w.snapshot(), before)

    def test_military_effects_cannot_be_direct_actor_chosen_battle_outcomes(self):
        w, a = self.own(); before = w.snapshot()
        with self.assertRaises(WorldError) as cm:
            w.submit(MARSHAL, 'bypass-dm', dict(action='military_engage', army_id='army-1', losses={'army-1':0,'army-2':4}))
        self.assertEqual(cm.exception.code, 'INVALID_BODY'); self.assertEqual(w.snapshot(), before)


if __name__ == '__main__':
    unittest.main()
