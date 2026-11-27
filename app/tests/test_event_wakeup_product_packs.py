"""1063 shipped-pack product-route definitions; source-only, executed=0.

Future wholemethods use fresh retained owned worlds, real registered classes,
original submit/CAS/root leases and component-scripted's original callback.
Only each Host INSTANCE chooser is controlled; this is not autonomous/model/SDK
coverage. No mocking, subclassing, clock replacement or budget adjustment.
"""
import copy
import hashlib
import io
import json
import sqlite3
import unittest
import uuid
from collections import Counter
from contextlib import closing, redirect_stdout
from pathlib import Path

from app import product_cli
from app.actors import ActorHost
from app.server import Coordinator
from app.world import WorldStore, WorldError, DAY
from app.scenario import load_scenario, scenario_digest

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
KING = 'person-country-1-king'
TEXT = '地方信使，请只依实际收到的这封信核查现场。'
BASE_PINS = {
    4: '47f509b652ccf86b990e86efebf9efd923fa5a23fe252f0ab568f7ffd0e62be8',
    5: '4133c51199a87d3f5812e40d43c753526b21895ad9f7a0274bdcc6856fff7b8b',
    6: 'dab7d4d8f669c5ea01d279fb2a4aec1528849a7612f4045030a010d550e14502',
}


def typed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


class EventWakeupProductPackTests(unittest.TestCase):
    def setUp(self):
        self.root = SCRATCH / ('parent1063-product-owned-' + uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.session = None
        self.views = []
        self.windows = []
        self.evidence = {'definitions_only_until_admitted': True, 'real_model': False, 'sdk': False,
                         'controlled_instance_chooser': True, 'scratch': str(self.root)}

    def save(self, name, value):
        with (self.root / (name + '.json')).open('w', encoding='utf-8', newline='\n') as stream:
            stream.write(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n')

    def rows(self, path, table):
        self.assertIn(table, {'intents', 'actor_intents', 'actor_session_heads',
                              'actor_execution_leases', 'coordinator_execution_leases'})
        with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM ' + table)]

    def private_state(self):
        with closing(sqlite3.connect(self.session.world.db_path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
            return json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])

    def pack(self, count):
        # Actual shipped file, never a helper-created positive configuration.
        relative = 'app/data/scenarios/event-wakeup-v1-iron-' + str(count) + '.json'
        manifest = json.loads(product_cli.MANIFEST.read_bytes())
        self.assertIn(relative, manifest['files'])
        self.assertIn('app/tests/test_event_wakeup_product_packs.py', manifest['files'])
        path = product_cli.ROOT / relative
        data = path.read_bytes()
        self.assertEqual({'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}, manifest['files'][relative])
        baseline = product_cli.SCENARIOS / ('iron-' + str(count) + '.json')
        original = baseline.read_bytes()
        self.assertEqual(hashlib.sha256(original).hexdigest(), BASE_PINS[count])
        base, config = json.loads(original), load_scenario(path)
        self.assertEqual(set(config) - set(base), {'event_wakeup'})
        self.assertEqual(set(base) - set(config), set())
        changed = {key for key in base if typed(base[key]) != typed(config[key])}
        self.assertEqual(changed, {'scenario_id', 'version', 'assumptions'})
        self.assertTrue(config['assumptions'].startswith(base['assumptions']))
        self.assertEqual(config['scenario_id'], 'event-wakeup-v1-iron-' + str(count))
        self.assertEqual(config['version'], 'authored-event-wakeup-iron-pack-v1')
        rules = config['event_wakeup']
        self.assertEqual(set(rules), {'version', 'government_interval_seconds', 'temporary_steps', 'temporary_persons'})
        self.assertEqual((rules['version'], rules['government_interval_seconds'], rules['temporary_steps']),
                         ('p04-event-wakeup-v1', DAY, 3))
        self.assertIs(type(rules['government_interval_seconds']), int)
        self.assertIs(type(rules['temporary_steps']), int)
        self.assertEqual(len(rules['temporary_persons']), 1)
        temp = rules['temporary_persons'][0]
        self.assertEqual(set(temp), {'id', 'name', 'country_id', 'office', 'location', 'card'})
        self.assertEqual(set(temp['card']), {'values', 'interests', 'risk', 'mission'})
        self.assertEqual((temp['country_id'], temp['location'], temp['office']), ('country-1', 'place-0', '地方信使'))
        self.evidence.update(pack_path=str(path), pack_sha256=hashlib.sha256(data).hexdigest(),
                             expected_country_count=count, expected_scenario_sha256=scenario_digest(config))
        return path, config

    def step(self):
        # One original complete ProductSession step(60), never a split/renewal.
        state = self.session.step(60)
        ledger = self.session.ledger()
        window = max(ledger['coordinator_windows'], key=lambda row: row['ordinal'])
        self.assertEqual(window['state'], 'completed')
        self.assertEqual(len(ledger['coordinator_windows']), len(self.windows) + 1)
        self.windows.append(copy.deepcopy(window))
        self.save('windows', self.windows)
        self.save('ledger', ledger)
        return window, state

    def closure(self):
        session = self.session
        actual = session.ledger()['coordinator_activations']
        leaves = self.rows(session.directory / 'actors.sqlite', 'actor_intents')
        intents = self.rows(session.directory / 'world.sqlite', 'intents')
        self.assertEqual(len(actual), len(leaves))
        self.assertEqual(len(actual), len(intents))
        self.assertEqual({row['intent_id'] for row in actual}, {row['intent_id'] for row in leaves})
        self.assertEqual({row['intent_id'] for row in actual}, {row['id'] for row in intents})
        for activation in actual:
            self.assertEqual((activation['phase'], activation['private_closed']), ('completed', 1))
            leaf = next(row for row in leaves if row['intent_id'] == activation['intent_id'])
            public = next(row for row in intents if row['id'] == activation['intent_id'])
            self.assertEqual(activation['promoted_version'], activation['base_session_version'] + 1)
            self.assertEqual(json.loads(leaf['receipt']), json.loads(public['receipt']))
            self.assertEqual(json.loads(public['receipt'])['status'], 'committed')
            request, metrics = json.loads(leaf['request']), json.loads(leaf['metrics'])
            self.assertEqual(request['person_id'], activation['person_id'])
            self.assertEqual(request['intent_id'], activation['intent_id'])
            self.assertEqual(request['body'], json.loads(leaf['body']))
            self.assertEqual(request['messages'], [])
            self.assertEqual(json.loads(leaf['candidate']), request['messages'] + [dict(role='user', content=request['prompt'])])
            self.assertEqual((metrics['provider'], metrics['sdk_events'], metrics['sdk_requests']), ('component-scripted', 0, 0))
            self.assertEqual(metrics['module_root'], str(product_cli.ROOT))
        self.save('actor-intents', leaves)
        self.save('public-intents', intents)
        self.save('heads', self.rows(session.directory / 'actors.sqlite', 'actor_session_heads'))
        self.assertIsNone(session.host._process)
        self.assertFalse(session.host.cleanup_pending)
        self.assertFalse(session.coordinator.cleanup_pending)
        return leaves

    def exercise_pack(self, count):
        path, config = self.pack(count)
        owned = self.root / 'world'
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            self.assertEqual(product_cli.main(['--data-dir', str(owned), 'init', '--scenario', str(path)]), 0)
        report = json.loads(stdout.getvalue())
        self.assertFalse(report['real_model'])
        self.assertIsNone(report['provider_mode'])
        self.assertEqual((owned / 'scenario.json').read_bytes(), path.read_bytes())
        marker = json.loads((owned / 'owner.json').read_bytes())
        self.assertEqual(marker['scenario_sha256'], hashlib.sha256(path.read_bytes()).hexdigest())
        self.assertEqual(marker['generation_sha256'], product_cli.verify_generation())
        initial = product_cli.read_owned(owned)
        self.assertEqual(initial['scenario_sha256'], scenario_digest(config))
        self.assertEqual((initial['time'], initial['horizon']), (0, 90 * DAY))
        self.assertEqual(initial['scenario_id'], config['scenario_id'])
        self.assertEqual(len(initial['countries']), count)
        self.assertEqual(len(initial['persons']), 3 * count + 1)
        residents = [position['person_id'] for country in config['countries'] for position in country['positions']]
        self.assertEqual(len(residents), len(set(residents)))
        self.assertEqual(len({p['name'] for p in initial['persons']}), 3 * count + 1)
        for country in config['countries']:
            people = [p for p in initial['persons'] if p['country_id'] == country['id'] and p['id'] in residents]
            self.assertEqual({p['office'] for p in people}, {'king', 'finance', 'marshal'})
            for position in country['positions']:
                person = next(p for p in people if p['id'] == position['person_id'])
                for actual_key, source_key in [('office_id', 'id'), ('name', 'person_name'),
                                               ('office', 'office'), ('location', 'location'), ('card', 'card')]:
                    self.assertEqual(typed(person[actual_key]), typed(position[source_key]))
        temporary = config['event_wakeup']['temporary_persons'][0]
        temp = temporary['id']
        person = next(p for p in initial['persons'] if p['id'] == temp)
        self.assertEqual(typed(person), typed(dict(temporary, card_version=1, card_author='scenario')))
        self.session = product_cli.ProductSession(owned, 'component-scripted')
        session = self.session
        self.assertIs(type(session), product_cli.ProductSession)
        self.assertIs(type(session.host), ActorHost)
        self.assertIs(type(session.coordinator), Coordinator)
        self.assertIs(type(session.world), WorldStore)
        self.assertIs(session.host._request, product_cli._component_request)
        self.assertIs(session.coordinator.actors, session.host)
        self.assertIs(session.host._authority, session.world)
        self.assertTrue(session.host.execution_fenced)
        for policy in (session.host.execution_policy, session.coordinator.execution_policy):
            self.assertEqual((policy.busy_budget_seconds, policy.act_deadline_seconds), (2, 120))
        source_before = product_cli.source_identity(session.world, session.host, session.coordinator)
        genesis = self.private_state()
        self.assertEqual(genesis['_config'], config)
        self.assertEqual(genesis['_p04']['remaining'], {temp: 0})
        self.assertNotIn(temp, genesis['_p04']['pending'])
        self.assertEqual(session.world.observe(temp)['messages'], [])
        self.assertEqual(session.world.observe(temp)['observations'], [])

        def controlled_choose(view, seen):
            self.views.append(copy.deepcopy(view))
            # Actual component request callback remains untouched. The proposal
            # is consumed by original _act -> candidate -> _submit -> finish/CAS.
            if view['person']['id'] == KING and view['time'] == 0:
                return dict(action='send', to=temp, text=TEXT), seen
            return dict(action='wait'), seen

        session.host._choose = controlled_choose
        self.assertIs(session.host._request, product_cli._component_request)
        self.assertEqual(product_cli.source_identity(session.world, session.host, session.coordinator), source_before)
        first, before = self.step()
        self.assertEqual((first['target_time'], json.loads(first['roster_json'])), (0, residents))
        self.assertEqual(Counter(v['person']['id'] for v in self.views), Counter(residents))
        self.assertEqual(session.world.observe(temp)['messages'], [])
        deliveries = [q for q in before['queue'] if q['kind'] == 'deliver']
        self.assertEqual(len(deliveries), 1)
        delivery = deliveries[0]
        due = config['local_message_seconds']
        self.assertGreater(due, 0)
        self.assertEqual((delivery['at'], delivery['sender'], delivery['recipient']), (due, KING, temp))
        self.assertEqual(self.private_state()['_p04']['remaining'][temp], 0)
        second, after = self.step()
        self.assertEqual((second['target_time'], after['time'], json.loads(second['roster_json'])), (due, due, [temp]))
        messages = session.world.observe(temp)['messages']
        self.assertEqual([(m['id'], m['time'], m['from'], m['text']) for m in messages],
                         [(delivery['message_id'], due, KING, TEXT)])
        received = [v for v in self.views if v['person']['id'] == temp]
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]['messages'], messages)
        asleep = self.private_state()['_p04']
        self.assertEqual(asleep['last_grant'][temp], due)
        self.assertEqual(asleep['remaining'][temp], 0)
        self.assertNotIn(temp, asleep['pending'])
        third, later = self.step()
        self.assertEqual((third['target_time'], json.loads(third['roster_json'])), (config['pathology']['contact_offset'], []))
        self.assertEqual(Counter(v['person']['id'] for v in self.views), Counter(residents + [temp]))
        self.assertEqual(self.private_state()['_p04']['remaining'][temp], 0)
        for resident in residents:
            self.assertFalse(any(m['text'] == TEXT for m in session.world.observe(resident)['messages']))
        self.assertEqual(later['horizon'], 90 * DAY)
        self.assertEqual(product_cli.source_identity(session.world, session.host, session.coordinator), source_before)
        leaves = self.closure()
        temp_leaf = next(row for row in leaves if row['person_id'] == temp)
        self.assertEqual(json.loads(temp_leaf['body']), dict(action='wait'))
        self.assertIn(TEXT, json.loads(temp_leaf['request'])['prompt'])
        self.evidence.update(source_identity=source_before, cli_report=report, marker=marker,
                             native_definition_result='completed-only-if-this-method-returns')

    def test_iron4_shipped_pack_cli_product_route(self):
        self.exercise_pack(4)

    def test_iron5_shipped_pack_cli_product_route(self):
        self.exercise_pack(5)

    def test_iron6_shipped_pack_cli_product_route(self):
        self.exercise_pack(6)

    def test_product_initialization_rejects_event_semantics_without_fallback(self):
        path, config = self.pack(4)
        # Strict-JSON positive reaches the SAME actual initialization first.
        positive = product_cli.initialize_owned(self.root / 'semantic-positive', path)
        self.assertEqual(positive['scenario_sha256'], scenario_digest(config))
        self.assertEqual(len(positive['persons']), 13)
        cases = []
        bad = copy.deepcopy(config)
        bad['event_wakeup']['temporary_steps'] = True
        cases.append(('schema-bool-step', bad))
        bad = copy.deepcopy(config)
        bad['event_wakeup']['temporary_persons'][0]['location'] = config['countries'][1]['places'][0]['id']
        cases.append(('foreign-valid-place', bad))
        bad = copy.deepcopy(config)
        bad['event_wakeup']['temporary_persons'][0]['id'] = KING
        cases.append(('resident-identity-conflict', bad))
        results = []
        for label, bad in cases:
            with self.subTest(label=label):
                input_path = self.root / (label + '.json')
                with input_path.open('x', encoding='utf-8', newline='\n') as stream:
                    stream.write(json.dumps(bad, ensure_ascii=False, allow_nan=False) + '\n')
                self.assertEqual(typed(load_scenario(input_path)), typed(bad))
                self.assertEqual(typed(bad['countries']), typed(config['countries']))
                target = self.root / (label + '-owned')
                with self.assertRaises(WorldError) as failure:
                    product_cli.initialize_owned(target, input_path)
                self.assertEqual(failure.exception.code, 'INVALID_EVENT_WAKEUP')
                self.assertEqual((target / 'scenario.json').read_bytes(), input_path.read_bytes())
                self.assertFalse((target / 'owner.json').exists())
                self.assertFalse((target / 'actors.sqlite').exists())
                with self.assertRaisesRegex(ValueError, '^OWNED_WORLD_REQUIRED$'):
                    product_cli.read_owned(target)
                results.append(dict(case=label, code=failure.exception.code, target=str(target),
                                    strict_json=True, gate='WorldStore._p04_initial', fallback=False))
        self.save('semantic-negatives', results)

    def tearDown(self):
        try:
            if self.session is not None:
                self.session.close()
                self.assertFalse(self.session.host.cleanup_pending)
                self.assertFalse(self.session.coordinator.cleanup_pending)
                self.save('final', self.session.world.snapshot())
                self.save('ledger-final', self.session.ledger())
        finally:
            self.save('views', self.views)
            self.save('evidence', self.evidence)


if __name__ == '__main__':
    unittest.main()
