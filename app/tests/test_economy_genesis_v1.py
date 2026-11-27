"""Self-contained real public-world SQLite tests; no evidence-tree fixtures.

Persisted corrupt latest inputs below are explicit fault injection, not legal
writers. Pre-connect copies are deterministic interleavings, not thread races.
"""
import copy
import hashlib
import json
import sqlite3
import sys
import uuid
import shutil
import unittest
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app import world as MOD
from app import scenario as SC

DATA = Path(MOD.__file__).with_name('data')
GEO = json.loads((DATA / 'geography.json').read_bytes())
SCRATCH = Path.home() / 'AppData/Local/hermes/cache/scratch'


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')


def sql_contents(path):
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
        master = db.execute('SELECT type,name,tbl_name,rootpage,sql FROM sqlite_master ORDER BY type,name').fetchall()
        tables = {}
        for kind, name, _, _, _ in master:
            if kind == 'table':
                quoted = '"' + name.replace('"', '""') + '"'
                tables[name] = sorted(db.execute('SELECT * FROM ' + quoted).fetchall(), key=repr)
        return {'master': master, 'tables': tables}


def file_identity(path):
    s = path.stat()
    return {'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), 'size': s.st_size,
            'mtime_ns': s.st_mtime_ns, 'inode': s.st_ino}


def capture_world(world):
    public = world.snapshot()
    return {'sql': sql_contents(world.db_path), 'public': public,
            'views': {p['id']: world.observe(p['id']) for p in public['persons']},
            'file': file_identity(world.db_path)}


def v3_config(count=4):
    c = json.loads((DATA / f'scenarios/iron-{count}.json').read_bytes())
    c.update(schema_version=3, scenario_id=f'p08-owned-economy-{count}-v1', version='p08-owned-genesis-v1')
    del c['harvest_day']; del c['harvest_quantity']
    places = [p for country in c['countries'] for p in country['places']]
    c['economy'] = {'version': 'p08-production-economy-v1',
        'assumptions': 'Uncalibrated fictional game assumptions; no scientific or historical calibration.',
        'units': {'grain': 'grain_unit', 'timber': 'timber_unit', 'iron': 'iron_unit', 'charcoal': 'charcoal_unit', 'iron_tool': 'tool_unit'},
        'initial_materials': {p['id']: {'timber': 20, 'iron': 10, 'charcoal': 10, 'iron_tool': 2} for p in places},
        'labor_hours_per_day': {p['id']: p['population'] * 8 for p in places},
        'recipes': {'forge_agricultural_tool': {'inputs': {'iron': 2, 'charcoal': 1, 'timber': 1},
            'outputs': {'iron_tool': 1}, 'labor_hours': 8, 'duration_seconds': 86400}}}
    return c


def owned_world(mod, c, label):
    # Even repeated labels cannot reuse an earlier corrupt or rollback DB.
    stem = label + '-' + uuid.uuid4().hex
    path = S / (stem + '.json')
    write_json(path, c)
    return mod.WorldStore(S / (stem + '.sqlite'), path)


def persisted_fault(world):
    """Append-only, parameterized fault-injected persisted input; genesis intact."""
    with closing(sqlite3.connect(world.db_path)) as db:
        seq, encoded = db.execute('SELECT seq,state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()
        bad = json.loads(encoded)
        bad['revision'] = seq + 1
        bad['economy']['frozen_at'] = False
        db.execute('INSERT INTO snapshots VALUES (?,?)', (seq + 1, SC.canonical(bad)))
        db.commit()
    return bad


def reorder(value):
    if isinstance(value, dict):
        return {k: reorder(v) for k, v in reversed(list(value.items()))}
    if isinstance(value, list):
        return [reorder(v) for v in value]
    return value


class EconomyGenesisTests(unittest.TestCase):
    def setUp(self):
        global S, E, LABEL
        LABEL = self._testMethodName + '-' + uuid.uuid4().hex
        S = SCRATCH / ('p08-F-test-' + LABEL)
        S.mkdir(parents=True, exist_ok=False)
        E = S / 'observations'
        E.mkdir()
        self.owned_scratch = S

    def test_F_initialize_rejects_fault_injected_latest_readonly(self):
        world = owned_world(MOD, v3_config(), LABEL + '-persisted-corruption')
        initial = world.initialize()
        bad = persisted_fault(world)
        before = sql_contents(world.db_path)
        identity = file_identity(world.db_path)
        write_json(E / 'fault-input.json', {'classification': 'fault-injected persisted input',
            'path': str(world.db_path), 'initial': initial, 'bad_latest': bad, 'sql': before, 'file': identity})
        error = None
        try:
            world.initialize()
        except MOD.WorldError as caught:
            error = caught.code
        after = sql_contents(world.db_path)
        write_json(E / 'fault-result.json', {'error': error, 'sql': after, 'file': file_identity(world.db_path)})
        self.assertEqual(error, 'FROZEN_ECONOMY', 'real initialize incorrectly accepted fault-injected latest')
        self.assertEqual(SC.canonical(after), SC.canonical(before))
        self.assertEqual(file_identity(world.db_path), identity)

    def test_I_456_typed_sql_frames_and_defensive_copy(self):
        for count in (4, 5, 6):
            with self.subTest(count=count):
                config = v3_config(count)
                world = owned_world(MOD, config, LABEL + '-typed-' + str(count))
                initial = world.initialize()
                economy = initial['economy']
                declaration = config['economy']
                digest = hashlib.sha256(SC.canonical(declaration).encode('utf-8')).hexdigest()
                expected = {'rules_version': declaration['version'], 'rules_sha256': digest, 'frozen_at':0,
                    'units':declaration['units'], 'recipes':declaration['recipes'],
                    'labor_capacity_hours_per_day':declaration['labor_hours_per_day'],
                    'materials':declaration['initial_materials'],
                    'material_ledger':{'initial':declaration['initial_materials'],
                        **{key:{pid:{rid:0 for rid in values} for pid,values in declaration['initial_materials'].items()}
                           for key in ('produced','consumed','lost')}}, 'tasks':[]}
                self.assertEqual(SC.canonical(economy), SC.canonical(expected))
                self.assertEqual(len(initial['countries']),count)
                self.assertEqual(len(initial['persons']),count*3)
                self.assertEqual(initial['pathology'],config['pathology'])
                self.assertEqual(initial['pathology_sha256'], __import__('hashlib').sha256(SC.canonical(config['pathology']).encode()).hexdigest())
                self.assertEqual(initial['horizon'],90*86400)
                self.assertEqual(initial['time'],0)
                self.assertEqual(initial['ledger']['initial'],sum(p['grain'] for c in config['countries'] for p in c['places']))
                for country in config['countries']:
                    for place in country['places']:
                        actual = next(p for p in initial['places'] if p['id']==place['id'])
                        for key,value in place.items():
                            self.assertEqual(actual[key],value)
                before = sql_contents(world.db_path)
                identity = file_identity(world.db_path)
                self.assertEqual(SC.canonical(world.initialize()),SC.canonical(initial))
                self.assertEqual(sql_contents(world.db_path),before)
                self.assertEqual(file_identity(world.db_path),identity)
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    raw = json.loads(db.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0])
                self.assertEqual(SC.canonical(MOD.WorldStore._public(raw)),SC.canonical(initial))
                validated = SC.validate_scenario(config,GEO)
                config['economy']['initial_materials']['place-0']['iron'] += 100
                self.assertEqual(validated['economy']['initial_materials']['place-0']['iron'],10)
                initial['economy']['materials']['place-0']['iron'] += 100
                self.assertEqual(world.snapshot()['economy']['materials']['place-0']['iron'],10)

    def test_I_strict_economy_boundaries_and_mixed_versions(self):
        base = v3_config()
        cases = []
        def add(label, change):
            c = copy.deepcopy(base); change(c); cases.append((label,c))
        for key in base['economy']:
            add('missing-'+key, lambda c,k=key:c['economy'].pop(k))
        add('unknown',lambda c:c['economy'].update(unknown=0))
        for value in (None, [], True, 'v1'):
            add('block-'+repr(value),lambda c,v=value:c.update(economy=v))
        for key in ('harvest_day','harvest_quantity'):
            add('old-'+key,lambda c,k=key:c.update({k:1}))
        for version in (2,1,True,3.0):
            add('mixed-'+repr(version),lambda c,v=version:c.update(schema_version=v,harvest_day=30,harvest_quantity=400))
        for value in (True,1.0,-1,None,2**63):
            add('initial-'+repr(value),lambda c,v=value:c['economy']['initial_materials']['place-0'].update(iron=v))
            add('labor-'+repr(value),lambda c,v=value:c['economy']['labor_hours_per_day'].update({'place-0':v}))
        for section in ('initial_materials','labor_hours_per_day'):
            add('place-missing-'+section,lambda c,s=section:c['economy'][s].pop('place-0'))
            add('place-unknown-'+section,lambda c,s=section:c['economy'][s].update(unknown=0))
        for key in ('grain','unknown'):
            add('initial-'+key,lambda c,k=key:c['economy']['initial_materials']['place-0'].update({k:1}))
        add('material-missing',lambda c:c['economy']['initial_materials']['place-0'].pop('iron'))
        add('labor-overpopulation',lambda c:c['economy']['labor_hours_per_day'].update({'place-0':2401}))
        for value in ('', ' ', 'x'*2001, True):
            add('assumptions-'+str(value)[:15],lambda c,v=value:c['economy'].update(assumptions=v))
        for value in ({}, {'grain':'wrong','iron':'iron_unit'}, {'grain':'grain_unit'},
                      {'grain':'grain_unit','Iron':'iron_unit'}, {'grain':'grain_unit','iron':True},
                      {'grain':'grain_unit','iron':'x'*65}):
            add('units-'+repr(value),lambda c,v=value:c['economy'].update(units=v))
        for value in ({}, [], None):
            add('recipes-'+repr(value),lambda c,v=value:c['economy'].update(recipes=v))
        recipe='forge_agricultural_tool'
        for key in ('inputs','outputs','labor_hours','duration_seconds'):
            add('recipe-missing-'+key,lambda c,k=key:c['economy']['recipes'][recipe].pop(k))
        add('recipe-extra',lambda c:c['economy']['recipes'][recipe].update(extra=1))
        for side in ('inputs','outputs'):
            for value in ({}, {'grain':1}, {'unknown':1}, {'iron':True}, {'iron':0}, {'iron':-1}, {'iron':1.0}, {'iron':2**63}):
                add(side+repr(value),lambda c,s=side,v=value:c['economy']['recipes'][recipe].update({s:v}))
        add('self-generation',lambda c:c['economy']['recipes'][recipe]['outputs'].update(iron=1))
        for key in ('labor_hours','duration_seconds'):
            for value in (True,0,-1,1.0,2**63):
                add(key+repr(value),lambda c,k=key,v=value:c['economy']['recipes'][recipe].update({k:v}))
        add('wrong-version',lambda c:c['economy'].update(version='other'))
        add('schema3-missing-economy',lambda c:c.pop('economy'))
        add('bad-source',lambda c:c['countries'][0]['places'][0].update(source_cell=-1))
        add('bad-person',lambda c:c['countries'][0]['positions'][0].update(location='unknown'))
        add('bad-pathology',lambda c:c['pathology'].update(symptom_seconds=True))
        for index,(label,c) in enumerate(cases):
            with self.subTest(case=index,label=label):
                with self.assertRaises(SC.ScenarioError):
                    SC.validate_scenario(c,GEO)
                w=owned_world(MOD,c,LABEL+'-invalid-'+str(index))
                with self.assertRaises(MOD.WorldError):
                    w.initialize()
                self.assertEqual(sql_contents(w.db_path)['tables'],{})
        for value in (float('nan'),float('inf'),float('-inf')):
            c=copy.deepcopy(base);c['economy']['initial_materials']['place-0']['iron']=value
            with self.subTest(nonfinite=repr(value)), self.assertRaises(SC.ScenarioError):
                SC.validate_scenario(c,GEO)
        for value in (0,2**63-1):
            c=copy.deepcopy(base);c['economy']['initial_materials']['place-0']['iron']=value
            self.assertEqual(SC.validate_scenario(c,GEO),c)
        c=copy.deepcopy(base);c['economy']['labor_hours_per_day']['place-0']=2400
        self.assertEqual(SC.validate_scenario(c,GEO),c)

    def test_F_first_snapshot_identity_rejects_coordinated_restatement(self):
        config=v3_config()
        world=owned_world(MOD,config,LABEL+'-F-target')
        initial=world.initialize()
        before=capture_world(world)
        write_json(E/(LABEL+'-F-before.json'),before)
        def operation(state,db):
            declaration=state['_config']['economy']
            declaration['assumptions'] += ' Changed after genesis.'
            declaration['units']['iron']='changed_iron_unit'
            declaration['labor_hours_per_day']['place-0'] += 1
            declaration['initial_materials']['place-0']['iron'] += 1
            frame=state['economy']
            frame.update(rules_version=declaration['version'],rules_sha256=SC.economy_digest(declaration),
                         units=copy.deepcopy(declaration['units']),recipes=copy.deepcopy(declaration['recipes']),
                         labor_capacity_hours_per_day=copy.deepcopy(declaration['labor_hours_per_day']))
            frame['material_ledger']['initial']=copy.deepcopy(declaration['initial_materials'])
            state['events'].append({'id':'owned-malicious-test-event','time':0,'kind':'test-rollback'})
            world._schedule(state,86400,'contact')
            db.execute('INSERT INTO intents VALUES (?,?,?,?)',('owned-malicious-test-intent',state['persons'][0]['id'],'{}','{}'))
        error=None
        try:
            world._mutate(operation)
        except MOD.WorldError as caught:
            error=caught.code
        after=capture_world(world)
        write_json(E/(LABEL+'-F-after.json'),after)
        write_json(E/(LABEL+'-F-operation-result.json'),{'error':error,'revision_before':initial['revision'],'revision_after':after['public']['revision']})
        self.assertEqual(error,'FROZEN_ECONOMY','coordinated current declarations were incorrectly committed')
        self.assertEqual(SC.canonical(after['sql']),SC.canonical(before['sql']))
        self.assertEqual(SC.canonical(after['public']),SC.canonical(before['public']))
        self.assertEqual(SC.canonical(after['views']),SC.canonical(before['views']))

    def test_I_explicit_schema3_initializes_before_first_infection(self):
        config = v3_config(4)
        world = owned_world(MOD, config, LABEL + '-I-target')
        try:
            loaded = SC.load_scenario(world.scenario_path)
            validated = SC.validate_scenario(loaded, GEO)
            actual = world.initialize()
        except (MOD.WorldError, SC.ScenarioError) as error:
            # initialize is the real durable boundary; validator failure is
            # independently confirmed there, not an import/setup RED.
            try:
                world.initialize()
            except MOD.WorldError as durable:
                self.assertEqual(durable.code, 'INVALID_SCENARIO')
                self.fail('valid explicit schema3 economy rejected at real initialize: ' + str(durable))
            raise AssertionError('validator rejected but initialize accepted') from error
        self.assertEqual(validated, config)
        self.assertEqual(actual['economy']['rules_version'], config['economy']['version'])
        self.assertEqual(actual['economy']['frozen_at'], 0)
        self.assertIs(type(actual['economy']['frozen_at']), int)
        self.assertEqual(actual['economy']['materials'], config['economy']['initial_materials'])
        self.assertEqual(world.snapshot(1), actual)
        self.assertNotIn('harvest', [q['kind'] for q in actual['queue']])
        self.assertEqual(sum(len(p['disease']['cohorts']) for p in actual['places']), 1)


    def test_F_initialize_transaction_baseline_native_preconnect_interleaving(self):
        for corrupt in (False, True):
            with self.subTest(fault_injected=corrupt, branch='transaction-baseline'):
                seed = owned_world(MOD, v3_config(), LABEL + '-closed-source')
                initial = seed.initialize()
                if corrupt:
                    persisted_fault(seed)
                before = sql_contents(seed.db_path)
                source_identity = file_identity(seed.db_path)
                target = S / ('absent-target-' + uuid.uuid4().hex + '.sqlite')
                world = MOD.WorldStore(target, seed.scenario_path)
                self.assertFalse(target.exists())
                original_sqlite = MOD.sqlite3
                records = []
                copied_identity = []

                class Preconnect:
                    def __getattr__(self, name):
                        return getattr(original_sqlite, name)

                    def connect(observer, *args, **kwargs):
                        if str(args[0]) == str(target):
                            records.append({'before_exists': target.exists(), 'source': str(seed.db_path), 'target': str(target)})
                            if target.exists():
                                raise AssertionError('preconnect boundary target was not absent')
                            shutil.copyfile(seed.db_path, target)
                            copied_identity.append(file_identity(target))
                            records[-1]['copied_sha256'] = copied_identity[-1]['sha256']
                            records[-1]['delegate_native'] = True
                        return original_sqlite.connect(*args, **kwargs)

                error = None
                actual = None
                MOD.sqlite3 = Preconnect()
                try:
                    try:
                        actual = world.initialize()
                    except MOD.WorldError as caught:
                        error = caught.code
                finally:
                    MOD.sqlite3 = original_sqlite
                self.assertEqual(len(records), 1)
                self.assertFalse(records[0]['before_exists'])
                self.assertEqual(records[0]['copied_sha256'], source_identity['sha256'])
                self.assertEqual(error, 'FROZEN_ECONOMY' if corrupt else None)
                if not corrupt:
                    self.assertEqual(SC.canonical(actual), SC.canonical(initial))
                self.assertEqual(SC.canonical(sql_contents(target)), SC.canonical(before))
                self.assertEqual(file_identity(target), copied_identity[0])
                self.assertEqual(file_identity(seed.db_path), source_identity)
                write_json(E / ('preconnect-' + str(corrupt) + '.json'), {
                    'classification': 'deterministic pre-connect interleaving; native SQLite, no thread race',
                    'fault_injected_persisted_input': corrupt, 'records': records, 'error': error,
                    'source_file': source_identity, 'target_before_native': copied_identity[0],
                    'target_after': file_identity(target), 'sql': before})

    def test_F_same_explicit_reopen_canonical_reorder_and_scenario_mismatch(self):
        config = v3_config()
        world = owned_world(MOD, config, LABEL + '-reopen')
        initial = world.initialize()
        before = capture_world(world)
        for label, requested in [('identical', config), ('canonical-reorder', reorder(config))]:
            with self.subTest(control=label, branch='readonly-baseline'):
                path = S / (label + '-' + uuid.uuid4().hex + '.json')
                write_json(path, requested)
                actual = MOD.WorldStore(world.db_path, path).initialize()
                self.assertEqual(SC.canonical(actual), SC.canonical(initial))
                self.assertEqual(SC.canonical(capture_world(world)), SC.canonical(before))
        changed = copy.deepcopy(config)
        changed['economy']['assumptions'] += ' Different explicit economic world.'
        mismatch = S / ('mismatch-' + uuid.uuid4().hex + '.json')
        write_json(mismatch, changed)
        with self.subTest(control='different-economy-scenario'):
            with self.assertRaises(MOD.WorldError) as caught:
                MOD.WorldStore(world.db_path, mismatch).initialize()
            self.assertEqual(caught.exception.code, 'SCENARIO_MISMATCH')
            self.assertEqual(SC.canonical(capture_world(world)), SC.canonical(before))
        write_json(E / 'reopen-result.json', {'before': before, 'after': capture_world(world)})

    def test_F_real_mutate_strict_frozen_fields_and_pathology_rollback(self):
        cases = [
            ('missing-frame', lambda s: s.pop('economy'), 'FROZEN_ECONOMY'),
            ('frozen-at-bool', lambda s: s['economy'].update(frozen_at=False), 'FROZEN_ECONOMY'),
            ('frozen-at-float', lambda s: s['economy'].update(frozen_at=0.0), 'FROZEN_ECONOMY'),
            ('rules-hash', lambda s: s['economy'].update(rules_sha256='0'*64), 'FROZEN_ECONOMY'),
            ('rules-version', lambda s: s['economy'].update(rules_version='other'), 'FROZEN_ECONOMY'),
            ('units', lambda s: s['economy']['units'].update(iron='changed'), 'FROZEN_ECONOMY'),
            ('recipes', lambda s: s['economy']['recipes']['forge_agricultural_tool'].update(labor_hours=9), 'FROZEN_ECONOMY'),
            ('base-labor', lambda s: s['economy']['labor_capacity_hours_per_day'].update({'place-0': 1}), 'FROZEN_ECONOMY'),
            ('initial-ledger', lambda s: s['economy']['material_ledger']['initial']['place-0'].update(iron=11), 'FROZEN_ECONOMY'),
            ('declaration-and-hash', lambda s: coordinated_declaration(s), 'FROZEN_ECONOMY'),
            ('pathology', lambda s: s['pathology'].update(symptom_seconds=s['pathology']['symptom_seconds']+1), 'FROZEN_PATHOLOGY'),
        ]
        for label, change, code in cases:
            with self.subTest(case=label):
                world = owned_world(MOD, v3_config(), LABEL + '-' + label)
                world.initialize()
                before = capture_world(world)
                entered = []
                def operation(state, db):
                    entered.append(True)
                    change(state)
                    state['events'].append({'id': 'internal-fault', 'kind': 'test-rollback', 'time': 0})
                    world._schedule(state, 86400, 'contact')
                    db.execute('INSERT INTO intents VALUES (?,?,?,?)', ('internal-fault', state['persons'][0]['id'], '{}', '{}'))
                with self.assertRaises(MOD.WorldError) as caught:
                    world._mutate(operation)
                self.assertEqual(entered, [True])
                self.assertEqual(caught.exception.code, code)
                after = capture_world(world)
                self.assertEqual(SC.canonical(after['sql']), SC.canonical(before['sql']))
                self.assertEqual(SC.canonical(after['public']), SC.canonical(before['public']))
                self.assertEqual(SC.canonical(after['views']), SC.canonical(before['views']))
                write_json(E / (label + '.json'), {'code': caught.exception.code,
                    'before_file': before['file'], 'after_file': after['file'],
                    'sql_unchanged': True, 'public_unchanged': True, 'views_unchanged': True,
                    'database': str(world.db_path)})

    def test_F_current_inventory_tasks_and_change_ledgers_are_not_frozen(self):
        world = owned_world(MOD, v3_config(), LABEL + '-mutable-current')
        initial = world.initialize()
        def internal_synthetic_probe(state, db):
            # Internal synthetic mutation, NOT a production/task execution API.
            state['economy']['materials']['place-0']['iron'] -= 1
            state['economy']['tasks'].append({'synthetic_internal_probe': True})
            for key in ('produced', 'consumed', 'lost'):
                state['economy']['material_ledger'][key]['place-0']['iron'] += 1
        actual = world._mutate(internal_synthetic_probe)
        self.assertEqual(actual['revision'], initial['revision'] + 1)
        self.assertEqual(actual['economy']['materials']['place-0']['iron'], 9)
        self.assertEqual(actual['economy']['tasks'], [{'synthetic_internal_probe': True}])
        for key in ('produced', 'consumed', 'lost'):
            with self.subTest(mutable_ledger=key):
                self.assertEqual(actual['economy']['material_ledger'][key]['place-0']['iron'], 1)
        for key in ('rules_version', 'rules_sha256', 'frozen_at', 'units', 'recipes', 'labor_capacity_hours_per_day'):
            with self.subTest(frozen_field=key):
                self.assertEqual(SC.canonical(actual['economy'][key]), SC.canonical(initial['economy'][key]))
        self.assertEqual(actual['economy']['material_ledger']['initial'], initial['economy']['material_ledger']['initial'])
        self.assertEqual(SC.canonical(world.initialize()), SC.canonical(actual))
        before = capture_world(world)
        def reorder_only(state, db):
            state['_config']['economy'] = reorder(state['_config']['economy'])
            for key in ('units', 'recipes', 'labor_capacity_hours_per_day'):
                state['economy'][key] = reorder(state['economy'][key])
        world._mutate(reorder_only)
        self.assertEqual(SC.canonical(capture_world(world)), SC.canonical(before))
        write_json(E / 'internal-synthetic-probe.json', {'classification': 'internal synthetic mutation, no task/production API',
            'initial': initial, 'actual': actual, 'file': file_identity(world.db_path), 'database': str(world.db_path)})

    def test_F_legacy_reopen_no_migration_and_midway_optin_rejected(self):
        for kind in ('no-schema', 'schema2', 'schema2-opaque'):
            with self.subTest(legacy=kind):
                config = json.loads((DATA / ('scenario.json' if kind == 'no-schema' else 'scenarios/iron-4.json')).read_bytes())
                world = owned_world(MOD, config, LABEL + '-' + kind)
                initial = world.initialize()
                self.assertNotIn('economy', initial)
                self.assertIn('harvest', [q['kind'] for q in initial['queue']])
                if kind == 'schema2-opaque':
                    # Trusted internal probe: a private opaque config field is not opt-in.
                    world._mutate(lambda s, db: s['_config'].update(economy={'opaque': 'not economic opt-in'}))
                before = capture_world(world)
                reopened = MOD.WorldStore(world.db_path).initialize()
                self.assertNotIn('economy', reopened)
                self.assertEqual(SC.canonical(capture_world(world)), SC.canonical(before))
                for trigger in ('schema3', 'public-economy'):
                    with self.subTest(legacy=kind, midway=trigger):
                        entered = []
                        def operation(state, db):
                            entered.append(True)
                            if trigger == 'schema3':
                                state['_config']['schema_version'] = 3
                            else:
                                state['economy'] = {}
                            state['events'].append({'id':'legacy-fault', 'kind':'test-rollback', 'time':0})
                            world._schedule(state, 86400, 'contact')
                            db.execute('INSERT INTO intents VALUES (?,?,?,?)', ('legacy-fault', state['persons'][0]['id'], '{}', '{}'))
                        with self.assertRaises(MOD.WorldError) as caught:
                            world._mutate(operation)
                        self.assertEqual(entered, [True])
                        self.assertEqual(caught.exception.code, 'FROZEN_ECONOMY')
                        self.assertEqual(SC.canonical(capture_world(world)['sql']), SC.canonical(before['sql']))
                        self.assertEqual(SC.canonical(capture_world(world)['public']), SC.canonical(before['public']))
                        self.assertEqual(SC.canonical(capture_world(world)['views']), SC.canonical(before['views']))
                write_json(E / ('legacy-' + kind + '.json'), {'before': before, 'after': capture_world(world), 'database':str(world.db_path)})


def coordinated_declaration(state):
    state['_config']['economy']['assumptions'] += ' Co-mutated declaration.'
    state['economy']['rules_sha256'] = SC.economy_digest(state['_config']['economy'])
