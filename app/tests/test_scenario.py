"""P01.1 actual source imports and durable worlds; no model claims."""
from contextlib import closing
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.world import WorldError, WorldStore

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = 'C:/Users/We3q/AppData/Local/hermes/cache/scratch'


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class ScenarioTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.geo = json.loads((ROOT / 'data/geography.json').read_text(encoding='utf-8'))

    def pack(self, count=4):
        return json.loads((ROOT / f'evidence/p011-prepared-scenarios/iron-{count}.json').read_text(encoding='utf-8'))

    def world(self, config, name='candidate'):
        path = self.base / f'{name}.json'
        path.write_text(json.dumps(config, ensure_ascii=False), encoding='utf-8')
        return WorldStore(self.base / f'{name}.sqlite', path)

    def test_actual_456_worlds_import_explicit_people_offices_and_resources(self):
        ids = []
        for count in (4, 5, 6):
            with self.subTest(count=count):
                config = self.pack(count)
                world = self.world(config, f'world-{count}')
                initial = world.initialize()
                ids.append(initial['world_id'])
                self.assertEqual(len(initial['countries']), count)
                self.assertEqual(len(initial['persons']), count * 3)
                self.assertEqual(len({p['id'] for p in initial['persons']}), count * 3)
                self.assertEqual(len({p['office_id'] for p in initial['persons']}), count * 3)
                self.assertEqual(initial['scenario_id'], config['scenario_id'])
                self.assertEqual(initial['scenario_version'], config['version'])
                self.assertEqual(initial['scenario_sha256'], hashlib.sha256(canonical(config).encode()).hexdigest())
                self.assertEqual(initial['ledger']['initial'], sum(p['grain'] for c in config['countries'] for p in c['places']))
                for country in config['countries']:
                    actual = next(c for c in initial['countries'] if c['id'] == country['id'])
                    self.assertEqual(actual['institution'], country['institution'])
                    for place in country['places']:
                        actual = next(p for p in initial['places'] if p['id'] == place['id'])
                        for key in ('name','population','grain','source_settlement_id','source_region_id','source_cell'):
                            self.assertEqual(actual[key], place[key])
                        self.assertEqual(actual['disease']['susceptible'] + sum(c['count'] for c in actual['disease']['cohorts']), place['population'])
                self.assertEqual(world.snapshot(1), initial)
        self.assertEqual(len(set(ids)), 3)

    def test_reordering_country_or_place_lists_preserves_seed_and_forward_disease_by_id(self):
        config = self.pack()
        baseline = self.world(config, 'seed-baseline')
        initial = baseline.initialize()
        expected = {'place-1': [{'id':'cohort-place-1-1', 'infected_at':0,
                                  'count':1, 'stage':'latent'}]}
        def infected(snapshot):
            return {p['id']:p['disease']['cohorts'] for p in snapshot['places'] if p['disease']['cohorts']}
        self.assertEqual(infected(initial), expected)
        rules = config['pathology']
        first_active_contact = rules['contact_offset']
        while first_active_contact < rules['symptom_seconds']:
            first_active_contact += 86400
        checkpoints = sorted({rules['symptom_seconds'], first_active_contact,
                              rules['severe_seconds'], rules['recovery_seconds']})
        baseline.control('resume')
        forward = {at:baseline.advance(at) for at in checkpoints}
        self.assertGreater(len(infected(forward[first_active_contact])['place-1']), 1)
        for order in ('countries', 'first-country-places'):
            with self.subTest(order=order):
                reordered = copy.deepcopy(config)
                if order == 'countries':
                    reordered['countries'].reverse()
                else:
                    reordered['countries'][0]['places'].reverse()
                world = self.world(reordered, 'seed-' + order)
                actual = world.initialize()
                self.assertEqual(infected(actual), expected)
                self.assertEqual(actual['queue'], initial['queue'])
                self.assertEqual(actual['pathology_sha256'], initial['pathology_sha256'])
                world.control('resume')
                for at in checkpoints:
                    actual = world.advance(at)
                    self.assertEqual({p['id']:p['disease'] for p in actual['places']},
                                     {p['id']:p['disease'] for p in forward[at]['places']})
                    self.assertEqual(actual['queue'], forward[at]['queue'])
                    for person in actual['persons']:
                        self.assertEqual(world.observe(person['id']), baseline.observe(person['id'], forward[at]['revision']))

    def test_missing_fixed_seed_place_is_invalid_without_partial_database(self):
        from app.scenario import validate_scenario, ScenarioError
        config = self.pack()
        # An otherwise valid authored polity with another real, reachable settlement.
        country = config['countries'][0]
        replacement = copy.deepcopy(self.pack(5)['countries'][4]['places'][0])
        country.update(source_country_id=None, authored_political_entity=True)
        country['places'][1] = replacement
        for position in country['positions']:
            if position['location'] == 'place-1':
                position['location'] = replacement['id']
        with self.subTest(boundary='validator'):
            with self.assertRaises(ScenarioError) as error:
                validate_scenario(config, self.geo)
            self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
        with self.subTest(boundary='durable-world'):
            self.assert_invalid(config, 'missing-fixed-seed')

    def assert_invalid(self, config, name='bad', code='INVALID_SCENARIO'):
        world = self.world(config, name)
        with self.assertRaises(WorldError) as error:
            world.initialize()
        self.assertEqual(error.exception.code, code)
        with closing(sqlite3.connect(world.db_path)) as db:
            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])

    def test_rejects_invalid_scalar_schema_and_count_without_partial_world(self):
        cases = []
        for field in ('country_count','horizon_days','daily_consumption','harvest_day',
                      'harvest_quantity','investigation_seconds','local_message_seconds','schema_version'):
            for value in (True, 1.0, None, -1, 0, '1'):
                cases.append(self.pack() | {field:value})
            missing = self.pack()
            del missing[field]
            cases.append(missing)
        for count in (3, 5, 7):
            cases.append(self.pack() | {'country_count':count})
        for field in ('scenario_id','version','assumptions'):
            for value in ('', '  ', None, 1):
                cases.append(self.pack() | {field:value})
        for field, values in (('population',(True, 1.0, 0, -1, None)), ('grain',(True, 1.0, -1, None))):
            for value in values:
                config = self.pack()
                config['countries'][0]['places'][0][field] = value
                cases.append(config)
        for value in ('url(https://invalid/)', 'red', '#abcxyz', '', None):
            config = self.pack()
            config['countries'][0]['color'] = value
            cases.append(config)
        cases += [[], None, self.pack() | {'extension':float('nan')}]
        for i, config in enumerate(cases):
            with self.subTest(case=i):
                self.assert_invalid(config, f'scalar-{i}')
    def test_rejects_missing_wrong_duplicate_sources_and_disconnected_core(self):
        cases = []
        for field in ('id','source_region_id','source_settlement_id','source_cell','name'):
            for value in (None, '', True, 999999):
                config = self.pack()
                config['countries'][0]['places'][0][field] = value
                cases.append(config)
            config = self.pack()
            del config['countries'][0]['places'][0][field]
            cases.append(config)
        config = self.pack()
        config['countries'][1]['places'][0] = copy.deepcopy(config['countries'][0]['places'][0])
        cases.append(config)
        config = self.pack()
        config['countries'][0]['places'][1] = copy.deepcopy(config['countries'][0]['places'][0])
        cases.append(config)
        config = self.pack()
        config['countries'][0]['source_country_id'] = 'country-does-not-exist'
        cases.append(config)
        config = self.pack(5)
        config['countries'][4]['authored_political_entity'] = False
        cases.append(config)
        for i, config in enumerate(cases):
            with self.subTest(case=i):
                self.assert_invalid(config, f'source-{i}')
        from app.scenario import validate_scenario, ScenarioError
        for change in ('missing-target','asymmetric','seconds-bool','seconds-zero','duplicate-region','isolate-core'):
            geo = copy.deepcopy(self.geo)
            if change == 'missing-target':
                geo['regions'][0]['neighbors'][0]['to'] = 'missing'
            elif change == 'asymmetric':
                geo['regions'][0]['neighbors'].pop()
            elif change.startswith('seconds'):
                geo['regions'][0]['neighbors'][0]['seconds'] = True if change == 'seconds-bool' else 0
            elif change == 'duplicate-region':
                geo['regions'].append(copy.deepcopy(geo['regions'][0]))
            else:
                for region in geo['regions']:
                    if region['id'] == 'place-0':
                        region['neighbors'] = []
                    else:
                        region['neighbors'] = [edge for edge in region['neighbors'] if edge['to'] != 'place-0']
            with self.subTest(change=change), self.assertRaises(ScenarioError):
                validate_scenario(self.pack(), geo)

    def test_full_source_graph_retained_and_ambiguous_settlement_is_explicit(self):
        from app.scenario import validate_scenario
        config = self.pack()
        original = copy.deepcopy(config)
        result = validate_scenario(config, self.geo)
        result['countries'][0]['places'][0]['grain'] += 1
        self.assertEqual(config, original)
        world = self.world(config)
        world.initialize()
        with closing(sqlite3.connect(world.db_path)) as db:
            regions = json.loads(db.execute("SELECT value FROM world_constants WHERE key='geography'").fetchone()[0])
        self.assertEqual(len(regions), 629)
        self.assertEqual(regions, {r['id']:r for r in self.geo['regions']})
        self.assertGreater(world._route_seconds({'_geography':regions, '_config':config}, 'place-0','place-10'), 0)
        # The two records at place-38 must be selectable by ID, never next().
        for settlement in [s for s in self.geo['settlements'] if s['place_id'] == 'place-38']:
            variant = self.pack()
            region = next(r for r in self.geo['regions'] if r['id'] == 'place-38')
            variant['countries'][3]['places'][0].update(id='place-38', source_region_id=region['source_region_id'],
                source_settlement_id=settlement['source_settlement_id'], source_cell=settlement['source_cell'], name=settlement['source_name'])
            for position in variant['countries'][3]['positions']:
                if position['location'] == 'place-37':
                    position['location'] = 'place-38'
            actual = self.world(variant, f"ambiguous-{settlement['source_settlement_id']}").initialize()
            self.assertEqual(next(p for p in actual['places'] if p['id']=='place-38')['source_settlement_id'], settlement['source_settlement_id'])

    def test_rejects_duplicate_or_missing_country_office_person_and_card_bindings(self):
        cases = []
        for field in ('id','name','institution'):
            for value in ('', None, True):
                config = self.pack()
                config['countries'][0][field] = value
                cases.append(config)
        for field in ('id','office','person_id','person_name','location','card'):
            config = self.pack()
            del config['countries'][0]['positions'][0][field]
            cases.append(config)
        for field in ('id','person_id','person_name','office'):
            config = self.pack()
            config['countries'][0]['positions'][1][field] = config['countries'][0]['positions'][0][field]
            cases.append(config)
        config = self.pack()
        config['countries'][1]['id'] = config['countries'][0]['id']
        cases.append(config)
        config = self.pack()
        config['countries'][1]['positions'][0]['person_id'] = config['countries'][0]['positions'][0]['person_id']
        cases.append(config)
        config = self.pack()
        config['countries'][0]['positions'].pop()
        cases.append(config)
        config = self.pack()
        config['countries'][0]['positions'][0]['location'] = 'place-10'
        cases.append(config)
        config = self.pack()
        config['countries'][0]['positions'][0]['card']['mission'] = ''
        cases.append(config)
        config = self.pack()
        config['countries'][0]['positions'][0]['person_id'] = config['countries'][0]['positions'][0]['id']
        cases.append(config)
        for i, config in enumerate(cases):
            with self.subTest(case=i):
                self.assert_invalid(config, f'identity-{i}')

    def test_legacy_input_only_accepts_original_four_country_pairs(self):
        original = json.loads((ROOT / 'data/scenario.json').read_text(encoding='utf-8'))
        initial = self.world(original, 'legacy-valid').initialize()
        self.assertEqual(len(initial['countries']), 4)
        self.assertEqual(len(initial['persons']), 12)
        for i, change in enumerate(({'country_count':5}, {'country_count':3}, {'pairs':original['pairs'][:3]},
                {'pairs':original['pairs'] + [['place-114','place-348']]},
                {'pairs':list(reversed(original['pairs']))}, {'population':True}, {'grain':-1})):
            self.assert_invalid(original | change, f'legacy-invalid-{i}')

    def test_invalid_json_file_and_source_hash_have_stable_diagnostics(self):
        for i, raw in enumerate(('{broken', '{"a":1,"a":2}', '[]', 'null',
                                 canonical(self.pack()).replace('"grain":1200','"grain":NaN',1))):
            path = self.base / f'raw-{i}.json'
            path.write_text(raw, encoding='utf-8')
            world = WorldStore(self.base / f'raw-{i}.sqlite', path)
            with self.subTest(raw=i):
                with self.assertRaises(WorldError) as error:
                    world.initialize()
                self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
        for i, change in enumerate(({'geography_sha256':None}, {'geography_sha256':'not-a-hash'}, {'geography_sha256':'0'*64})):
            self.assert_invalid(self.pack() | change, f'hash-{i}', 'SOURCE_DRIFT' if i == 2 else 'INVALID_SCENARIO')
        missing = self.pack()
        del missing['geography_sha256']
        self.assert_invalid(missing, 'hash-missing')
        world = WorldStore(self.base / 'absent.sqlite', self.base / 'absent.json')
        with self.assertRaises(WorldError) as error:
            world.initialize()
        self.assertEqual(error.exception.code, 'INVALID_SCENARIO')

    def test_default_uses_formal_four_pack_and_formal_packs_match_preparation(self):
        for count in (4, 5, 6):
            formal = ROOT / f'data/scenarios/iron-{count}.json'
            prepared = ROOT / f'evidence/p011-prepared-scenarios/iron-{count}.json'
            self.assertTrue(formal.exists(), f'缺正式{count}国包')
            self.assertEqual(formal.read_bytes(), prepared.read_bytes())
            actual = WorldStore(self.base / f'formal-{count}.sqlite', formal).initialize()
            self.assertEqual(len(actual['countries']), count)
        initial = WorldStore(self.base / 'default.sqlite').initialize()
        self.assertEqual(initial['scenario_id'], self.pack()['scenario_id'])
        self.assertEqual(initial['places'][0]['population'], 100)
        self.assertEqual(initial['places'][0]['grain'], 1200)
        self.assertGreater(len({p['population'] for p in initial['places']}), 1)
        self.assertGreater(len({p['grain'] for p in initial['places']}), 1)
        legacy = self.world(json.loads((ROOT / 'data/scenario.json').read_text(encoding='utf-8')), 'legacy-identity').initialize()
        self.assertEqual(legacy['scenario_id'], 'legacy-four-country')
        self.assertEqual(legacy['scenario_version'], 'uncalibrated-fictional-iron-v1')
        self.assertEqual(len(legacy['scenario_sha256']), 64)

    def test_nonfinite_json_numbers_are_invalid_on_create_and_explicit_restore(self):
        from app.scenario import load_scenario, ScenarioError
        world = self.world(self.pack(), 'finite-existing')
        initial = world.initialize()
        world.control('resume')
        world.advance(90000)
        current = world.snapshot()
        before = world.db_path.read_bytes()
        for index, extra in enumerate(('1e999', '-1e999',
                                       '{"nested":[0,{"value":1e999}]}',
                                       '{"nested":[0,{"value":-1e999}]}',
                                       'NaN', 'Infinity', '-Infinity')):
            path = self.base / f'nonfinite-{index}.json'
            path.write_text(canonical(self.pack())[:-1] + ',"extra":' + extra + '}', encoding='utf-8')
            with self.subTest(number=extra, boundary='loader'):
                with self.assertRaises(ScenarioError) as error:
                    load_scenario(path)
                self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
            with self.subTest(number=extra, boundary='existing-world'):
                try:
                    with self.assertRaises(WorldError) as error:
                        WorldStore(world.db_path, path).initialize()
                    self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
                finally:
                    self.assertEqual(world.db_path.read_bytes(), before)
                    self.assertEqual(world.snapshot(), current)
                    self.assertEqual(world.snapshot(1), initial)
            with self.subTest(number=extra, boundary='new-world'):
                new = WorldStore(self.base / f'nonfinite-{index}.sqlite', path)
                try:
                    with self.assertRaises(WorldError) as error:
                        new.initialize()
                    self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
                finally:
                    with closing(sqlite3.connect(new.db_path)) as db:
                        self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])

    def test_escaped_lone_surrogate_is_invalid_at_all_scenario_boundaries(self):
        from app.scenario import load_scenario, validate_scenario, ScenarioError
        import importlib
        import sys
        import types
        package = types.ModuleType('p011_unicodefix_p071')
        package.__path__ = [str(ROOT / 'evidence/candidates/p07-1/app')]
        sys.modules[package.__name__] = package
        oldworld = importlib.import_module(package.__name__ + '.world')
        current = self.world(self.pack(), 'unicode-existing')
        legacy = oldworld.WorldStore(self.base / 'unicode-p071.sqlite')
        saved = []
        for world in (current, legacy):
            initial = world.initialize()
            world.control('resume')
            world.advance(90000)
            saved.append((world, initial, world.snapshot(), world.db_path.read_bytes()))
        config = self.pack() | {'extra': 'bad\ud800'}
        path = self.base / 'escaped-surrogate.json'
        path.write_text(json.dumps(config, ensure_ascii=True), encoding='utf-8')
        self.assertIn(b'\\ud800', path.read_bytes())
        with self.subTest(boundary='loader'):
            with self.assertRaises(ScenarioError) as error:
                load_scenario(path)
            self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
        with self.subTest(boundary='direct-validator'):
            with self.assertRaises(ScenarioError) as error:
                validate_scenario(config, self.geo)
            self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
        with self.subTest(boundary='new-world'):
            new = WorldStore(self.base / 'unicode-new.sqlite', path)
            try:
                with self.assertRaises(WorldError) as error:
                    new.initialize()
                self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
            finally:
                with closing(sqlite3.connect(new.db_path)) as db:
                    self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])
        for world, initial, latest, before in saved:
            with self.subTest(boundary='explicit-restore', producer=type(world).__module__):
                try:
                    with self.assertRaises(WorldError) as error:
                        WorldStore(world.db_path, path).initialize()
                    self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
                finally:
                    self.assertEqual(world.db_path.read_bytes(), before)
                    self.assertEqual(world.snapshot(1), initial)
                    self.assertEqual(world.snapshot(), latest)

    def test_characterization_surrogate_positions_preserve_stable_refusal(self):
        from app.scenario import canonical as scene_canonical, load_scenario, scenario_digest, validate_scenario, ScenarioError
        world = self.world(self.pack(), 'unicode-matrix-existing')
        initial = world.initialize()
        world.control('resume')
        world.advance(90000)
        latest, before = world.snapshot(), world.db_path.read_bytes()
        for half in ('\ud800', '\udfff'):
            for field in ('extra', 'nested-value', 'nested-key', 'scenario_id', 'country-id', 'country-name', 'card'):
                config = self.pack()
                bad = 'bad' + half
                if field == 'nested-value':
                    config['extra'] = {'nested': [0, {'value': bad}]}
                elif field == 'nested-key':
                    config['extra'] = {'nested': [0, {bad: 'value'}]}
                elif field == 'country-id':
                    config['countries'][0]['id'] = bad
                elif field == 'country-name':
                    config['countries'][0]['name'] = bad
                elif field == 'card':
                    config['countries'][0]['positions'][0]['card']['mission'] = bad
                else:
                    config[field] = bad
                label = f'{ord(half):x}-{field}'
                path = self.base / f'{label}.json'
                path.write_text(json.dumps(config, ensure_ascii=True), encoding='utf-8')
                for boundary, operation in (
                    ('loader', lambda: load_scenario(path)),
                    ('validator', lambda: validate_scenario(config, self.geo)),
                    ('canonical', lambda: scene_canonical(config)),
                    ('digest', lambda: scenario_digest(config)),
                ):
                    with self.subTest(case=label, boundary=boundary):
                        with self.assertRaises(ScenarioError) as error:
                            operation()
                        self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
                with self.subTest(case=label, boundary='new-world'):
                    new = WorldStore(self.base / f'{label}.sqlite', path)
                    try:
                        with self.assertRaises(WorldError) as error:
                            new.initialize()
                        self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
                    finally:
                        with closing(sqlite3.connect(new.db_path)) as db:
                            self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), [])
                with self.subTest(case=label, boundary='explicit-restore'):
                    try:
                        with self.assertRaises(WorldError) as error:
                            WorldStore(world.db_path, path).initialize()
                        self.assertEqual(error.exception.code, 'INVALID_SCENARIO')
                    finally:
                        self.assertEqual(world.db_path.read_bytes(), before)
                        self.assertEqual(world.snapshot(1), initial)
                        self.assertEqual(world.snapshot(), latest)

    def test_characterization_valid_chinese_nonbmp_and_escaped_pair_preserve_bytes(self):
        from app.scenario import canonical as scene_canonical, load_scenario, scenario_digest, validate_scenario
        config = self.pack()
        text = '中文😀𠮷e\u0301'
        config['scenario_id'] += text
        config['countries'][0]['name'] += text
        config['countries'][0]['positions'][0]['card']['mission'] += text
        config['extra'] = {text: ['中文', '😀', '𠮷', 'e\u0301']}
        original = copy.deepcopy(config)
        expected = canonical(config).encode('utf-8')
        expected_digest = hashlib.sha256(expected).hexdigest()
        for escaped in (False, True):
            with self.subTest(escaped=escaped):
                path = self.base / f'valid-unicode-{escaped}.json'
                path.write_text(json.dumps(config, ensure_ascii=escaped), encoding='utf-8')
                if escaped:
                    self.assertIn(b'\\ud83d\\ude00', path.read_bytes())
                else:
                    self.assertIn('😀'.encode('utf-8'), path.read_bytes())
                loaded = load_scenario(path)
                validated = validate_scenario(loaded, self.geo)
                self.assertEqual(loaded, original)
                self.assertEqual(validated, original)
                self.assertEqual(scene_canonical(validated).encode('utf-8'), expected)
                self.assertEqual(scenario_digest(validated), expected_digest)
                world = WorldStore(self.base / f'valid-unicode-{escaped}.sqlite', path)
                initial = world.initialize()
                self.assertEqual(initial['scenario_sha256'], expected_digest)
                self.assertEqual(initial['scenario_id'], original['scenario_id'])
                self.assertEqual(initial['countries'][0]['name'], original['countries'][0]['name'])
                self.assertEqual(initial['persons'][0]['card'], original['countries'][0]['positions'][0]['card'])
                before = world.db_path.read_bytes()
                self.assertEqual(WorldStore(world.db_path, path).initialize(), initial)
                self.assertEqual(world.db_path.read_bytes(), before)
        self.assertEqual(config, original)

    def test_characterization_invalid_pathology_keeps_its_diagnostic(self):
        from app.scenario import validate_scenario, ScenarioError
        for change in (None, {'symptom_seconds': True}, {'extra': '\ud800'}):
            config = self.pack()
            if change is None:
                config['pathology'] = None
            else:
                config['pathology'].update(change)
            with self.subTest(change=repr(change)):
                with self.assertRaises(ScenarioError) as error:
                    validate_scenario(config, self.geo)
                self.assertEqual(error.exception.code, 'INVALID_PATHOLOGY')
                if change is None or 'symptom_seconds' in change:
                    self.assert_invalid(config, 'invalid-pathology-' + str(change is None), 'INVALID_PATHOLOGY')

    def test_explicit_mismatch_rejected_but_same_config_and_implicit_restore_are_readonly(self):
        world = self.world(self.pack(5), 'restore')
        initial = world.initialize()
        world.control('resume')
        world.advance(90000)
        before = world.db_path.read_bytes()
        current = world.snapshot()
        other = self.base / 'other.json'
        other.write_text(canonical(self.pack(6)), encoding='utf-8')
        with self.assertRaises(WorldError) as error:
            WorldStore(world.db_path, other).initialize()
        self.assertEqual(error.exception.code, 'SCENARIO_MISMATCH')
        self.assertEqual(world.db_path.read_bytes(), before)
        changed = self.pack(5)
        changed['countries'][0]['places'][0]['grain'] += 1
        other.write_text(canonical(changed), encoding='utf-8')
        with self.assertRaises(WorldError) as error:
            WorldStore(world.db_path, other).initialize()
        self.assertEqual(error.exception.code, 'SCENARIO_MISMATCH')
        self.assertEqual(world.db_path.read_bytes(), before)
        same = self.base / 'same.json'
        same.write_text(json.dumps(dict(reversed(list(self.pack(5).items())))), encoding='utf-8')
        self.assertEqual(WorldStore(world.db_path, same).initialize(), current)
        implicit = WorldStore(world.db_path)
        implicit.scenario_path = self.base / 'nonexistent-new-default.json'
        self.assertEqual(implicit.initialize(), current)
        self.assertEqual(world.db_path.read_bytes(), before)
        self.assertEqual(world.snapshot(1), initial)

    def test_frozen_p071_database_is_not_migrated_or_given_scenario_identity(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location('app._p011_legacy_world', ROOT / 'evidence/candidates/p07-1/app/world.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        path = self.base / 'frozen-legacy.sqlite'
        old = module.WorldStore(path, ROOT / 'data/scenario.json')
        initial = old.initialize()
        old.control('resume')
        old.advance(90000)
        current = old.snapshot()
        self.assertNotIn('scenario_id', current)
        before = path.read_bytes()
        restored = WorldStore(path)
        self.assertEqual(restored.initialize(), current)
        self.assertEqual(restored.snapshot(1), initial)
        self.assertEqual(WorldStore(path, ROOT / 'data/scenario.json').initialize(), current)
        self.assertEqual(path.read_bytes(), before)
        with self.assertRaises(WorldError) as error:
            WorldStore(path, ROOT / 'data/scenarios/iron-4.json').initialize()
        self.assertEqual(error.exception.code, 'SCENARIO_MISMATCH')
        self.assertEqual(path.read_bytes(), before)

    def test_rejects_malformed_or_nonfinite_source_records_with_stable_error(self):
        from app.scenario import validate_scenario, ScenarioError
        for change in ('infinite-length','nan-extra','bad-settlement-place','bad-country-regions','nonobject-region'):
            geo = copy.deepcopy(self.geo)
            if change == 'infinite-length':
                geo['regions'][0]['neighbors'][0]['source_length'] = float('inf')
                geo['regions'][1]['neighbors'][0]['source_length'] = float('inf')
            elif change == 'nan-extra':
                geo['extension'] = float('nan')
            elif change == 'bad-settlement-place':
                geo['settlements'][0]['place_id'] = []
            elif change == 'bad-country-regions':
                geo['countries'][0]['regions'] = None
            else:
                geo['regions'][0] = []
            with self.subTest(change=change):
                with self.assertRaises(ScenarioError) as error:
                    validate_scenario(self.pack(), geo)
                self.assertEqual(error.exception.code, 'INVALID_SCENARIO')

    def test_missing_or_unreadable_geography_is_source_drift(self):
        from unittest.mock import patch
        data = self.base / 'source-data'
        data.mkdir()
        for i, raw in enumerate((None, b'{broken')):
            if raw is not None:
                (data / 'geography.json').write_bytes(raw)
            with patch('app.world.DATA', data):
                self.assert_invalid(self.pack(), f'geo-file-{i}', 'SOURCE_DRIFT')

    def test_characterization_inventory_and_observations_remain_independent_across_worlds_and_countries(self):
        first = self.world(self.pack(6), 'isolation-first')
        second = self.world(self.pack(6), 'isolation-second')
        a, b = first.initialize(), second.initialize()
        self.assertNotEqual(a['world_id'], b['world_id'])
        first.control('resume')
        before = first.snapshot()
        with self.assertRaises(WorldError) as error:
            first.submit('person-country-1-finance', 'cross-country-ship', {'action':'ship','to':'place-114','quantity':10})
        self.assertEqual(error.exception.code, 'NOT_AUTHORIZED')
        self.assertEqual(first.snapshot(), before)
        with self.assertRaises(WorldError) as error:
            first.submit('person-country-1-king', 'cross-country-order', {'action':'command','to':'person-country-5-marshal','text':'限制聚集'})
        self.assertEqual(error.exception.code, 'NOT_AUTHORIZED')
        receipt = first.submit('person-country-5-finance', 'local-ship', {'action':'ship','to':'place-348','quantity':17})
        self.assertTrue(receipt['effect']['implemented'])
        latest = first.snapshot()
        self.assertEqual(next(p for p in latest['places'] if p['id']=='place-114')['grain'], 1033)
        self.assertEqual(next(p for p in latest['places'] if p['id']=='place-0')['grain'], 1200)
        self.assertEqual(second.snapshot(), b)
        self.assertNotEqual(first.observe('person-country-5-finance')['observations'], second.observe('person-country-5-finance')['observations'])
        self.assertEqual(first.observe('person-country-6-finance')['observations'], [])
        first.advance(receipt['effect']['due'])
        final = first.snapshot()
        self.assertEqual(next(p for p in final['places'] if p['id']=='place-348')['grain'], 1267)
        self.assertEqual(sum(p['grain'] for p in final['places']) + sum(p['quantity'] for p in final['in_transit']), final['ledger']['initial'])

    def test_characterization_pure_90_day_no_action_budget_uses_actual_asymmetric_stocks(self):
        # Arithmetic budget only, not a model run or another 90-day WorldStore observation.
        for count in (4,5,6):
            config = self.pack(count)
            initial = self.world(config, f'budget-{count}').initialize()
            populations, stocks, institutions = set(), set(), set()
            for country in config['countries']:
                institutions.add(country['institution'])
                for place in country['places']:
                    populations.add(place['population'])
                    stocks.add(place['grain'])
                    balance, consumed, harvested, minimum = place['grain'], 0, 0, place['grain']
                    for day in range(1, 91):
                        balance -= config['daily_consumption']
                        consumed += config['daily_consumption']
                        minimum = min(minimum, balance)
                        self.assertGreaterEqual(balance, 0, place['id'])
                        if day % config['harvest_day'] == 0:
                            balance += config['harvest_quantity']
                            harvested += config['harvest_quantity']
                    self.assertEqual(balance, place['grain'] + harvested - consumed)
                    self.assertGreaterEqual(minimum, 0)
            self.assertGreater(len(populations), 1)
            self.assertGreater(len(stocks), 1)
            self.assertEqual(len(institutions), count)
            self.assertTrue(any(q['kind']=='harvest' and q['at'] == config['harvest_day']*86400 for q in initial['queue']))


if __name__ == '__main__':
    unittest.main()
