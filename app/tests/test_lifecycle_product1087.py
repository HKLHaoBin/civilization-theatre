"""1099 acceptance definitions; real registered product, synthetic transport only.

SOURCE ONLY: this module has not been imported or run by its author. Each test
owns a fresh CLI world and one cumulative 120 second deadline, including cold
reopen. No SOURCE_PASS, runtime pass, live model or release claim follows.

The finite observer below is deliberately NOT a profiler or an authority hook.
It observes exact owned World release returns and Host.act's original exit
(join/CAS results plus its own Attempt threads), and, once, the original Host's
results.append(result) statement AFTER the real commit. It calls the original
completed-job APIs inside that still-live original caller/Work/Entry/pipeline;
it never changes frame locals, phases, code, classes, registries or writers.
"""
import ast
import copy
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time
import unittest
import uuid
from contextlib import closing, contextmanager, ExitStack, redirect_stdout
from unittest.mock import patch

from app import product_cli
from app import coordinator_scheduler as scheduler
from app.actor_execution import PrivateStore, PreparationBudget, LifecycleCallBudget
from app.actors import ActorHost
from app.adjudication import Adjudicator
from app.coordinator_execution import CoordinatorAttempt
from app.real_provider import RealGateway
from app.real_settings import SettingsStore
from app.server import Coordinator
from app.world import WorldStore, WorldError

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
KING = 'person-country-1-king'
OLD = 'person-country-1-finance'
OFFICE = 'office-country-1-finance'
MEANS = 'personal-iron-country-1-king'
PUBLIC_DOC = 'PUBLIC1099：机构核定本旬粮账，公开余额应以现场清点为准。'
PRIVATE_SECRET = 'PRIVATE1099：我尚未告诉任何人的账簿夹层暗记紫杉七。'
CHECK = '本人请于真实信使到达后核查机构公文。'
OLD_CHECK = '本人财政官请君主稍后到场核查所写公文。'
WAIT = '本人选择等待实际后续见闻，不替他人决定。'
CLAIM = '我只是声称财政官已经死了；这不是我的物理行动。'

MAIN_TABLES = ('intents', 'dm_proposals', 'dm_decisions', 'dm_timeline',
               'coordinator_scheduler_registration', 'coordinator_windows',
               'coordinator_activations', 'coordinator_activation_outcomes',
               'actor_coordinator_delegations', 'actor_activation_delegations')
PRIVATE_TABLES = ('actor_world_binding', 'actor_intents', 'actor_sessions',
                  'actor_session_heads', 'actor_intent_execution', 'actor_calls')


def wire(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def sha(value):
    return hashlib.sha256(wire(value).encode('utf-8')).hexdigest()


class LifecycleProductTests(unittest.TestCase):
    def setUp(self):
        self.started = time.monotonic()
        self.deadline = self.started + 120
        self.root = SCRATCH / ('parent1099-owned-' + uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.session = None
        self.sessions = []
        self.calls = []
        self.violations = []
        self.windows = []
        self.cas = []
        self.observer_errors = []
        self.replays = []
        self.replay_enabled = False
        self.replay_fired = False
        self.mode = 'mortal'
        self.institution = 'appointed'
        self.new = None
        self.first_new = None
        self.root_call = None
        self.threads = []
        self.actor_exits = []
        self.observe_exits = True
        self.act_code = ActorHost.act.__code__
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'socket.create_connection', 'subprocess.Popen',
                       'http.client.HTTPConnection.request',
                       'app.real_settings.default_credential_factory',
                       'app.real_provider.default_credential_factory'):
            self.stack.enter_context(patch(target, self.forbidden))
        self.host_code = ActorHost.resolve_lifecycle_jobs.__code__
        self.host_space = ActorHost.resolve_lifecycle_jobs.__globals__
        host_file = Path(self.host_code.co_filename).resolve()
        self.assertEqual(host_file, product_cli.ROOT / 'app/actors.py')
        tree = ast.parse(host_file.read_bytes())
        owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'ActorHost')
        method = next(n for n in owner.body if isinstance(n, ast.FunctionDef)
                      and n.name == 'resolve_lifecycle_jobs')
        markers = [n for n in ast.walk(method) if isinstance(n, ast.Expr)
                   and ast.dump(n.value, include_attributes=False) ==
                   "Call(func=Attribute(value=Name(id='results', ctx=Load()), attr='append', ctx=Load()), args=[Name(id='result', ctx=Load())], keywords=[])"]
        self.assertEqual(len(markers), 1, 'source-bound observer must match exactly one original statement')
        self.replay_line = markers[0].lineno
        self.release_codes = {WorldStore.release_actor_execution.__code__: 'actor',
                              WorldStore.release_coordinator_execution.__code__: 'root'}
        self.previous_trace = sys.gettrace()
        self.previous_thread_trace = threading.gettrace()
        self.assertIsNone(self.previous_trace, 'use a non-instrumented future runner')
        self.assertIsNone(self.previous_thread_trace)
        # Definition only now. A future runner restores BOTH hooks in tearDown.
        sys.settrace(self.owned_observer)
        threading.settrace(self.owned_observer)
        self.addCleanup(self.restore_observer)

    def restore_observer(self):
        sys.settrace(self.previous_trace)
        threading.settrace(self.previous_thread_trace)

    def forbidden(self, *args, **kwargs):
        self.violations.append('external-or-scripted-consumer')
        raise AssertionError('No network/SDK/subprocess/vault/account/scripted chooser')

    def budget(self):
        self.assertLess(time.monotonic(), self.deadline,
                        'one cumulative whole120 including setup, proof, replay and cold reopen')

    def owned_observer(self, frame, event, arg):
        code = frame.f_code
        if code is self.act_code:
            if (frame.f_globals is not ActorHost.act.__globals__ or self.session is None
                    or frame.f_locals.get('self') is not self.session.host):
                return None
            if event == 'return':
                owned = frame.f_locals
                attempt = owned.get('attempt')
                self.actor_exits.append(dict(attempt=attempt,
                    maintenance_joined=owned.get('maintenance_joined'),
                    providers_joined=owned.get('providers_joined'),
                    lease_released=owned.get('lease_released')))
            return self.owned_observer
        if code in self.release_codes:
            if (frame.f_globals is not WorldStore.release_actor_execution.__globals__
                    or self.session is None or frame.f_locals.get('self') is not self.session.world):
                return None
            if event == 'return':
                permit = frame.f_locals.get('permit')
                self.cas.append(dict(kind=self.release_codes[code], result=arg,
                    world_id=getattr(permit, 'world_id', None),
                    person_id=getattr(permit, 'person_id', None),
                    epoch=getattr(permit, 'epoch', None)))
            return self.owned_observer
        if code is not self.host_code or frame.f_globals is not self.host_space:
            return None
        if self.session is None or frame.f_locals.get('self') is not self.session.host:
            return None
        if (event == 'line' and frame.f_lineno == self.replay_line
                and self.replay_enabled and not self.replay_fired):
            self.replay_fired = True
            # Only locals from this exact owned, original Host business frame.
            fields = ('captured_root', 'entry', 'io', 'job_id', 'service_version',
                      'request_sha256', 'carrier', 'result')
            owned = {key: frame.f_locals[key] for key in fields}
            try:
                self.replay_completed_job(owned)
            except BaseException as error:
                # Product catches callback exceptions. Assert outside it after join.
                self.observer_errors.append((type(error).__name__, str(error)))
        return self.owned_observer

    def rows(self, table, private=False):
        allowed = PRIVATE_TABLES if private else MAIN_TABLES + (
            'actor_execution_leases', 'coordinator_execution_leases', 'execution_clock')
        self.assertIn(table, allowed)
        path = self.owned / ('actors.sqlite' if private else 'world.sqlite')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]

    def raw(self, seq=None):
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            row = db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1' if seq is None
                             else 'SELECT state FROM snapshots WHERE seq=?',
                             () if seq is None else (seq,)).fetchone()
            self.assertIsNotNone(row)
            return json.loads(row[0])

    def business(self):
        # Fully typed original rows, never SELECT count or a fabricated Row.
        return dict(raw=self.raw(), main={t:self.rows(t) for t in MAIN_TABLES},
                    private={t:self.rows(t, True) for t in PRIVATE_TABLES})

    def role_calls(self, pid):
        return [c for c in self.calls if c['kind'] == 'roles' and c['view']['person']['id'] == pid]

    def root_calls(self):
        return [c for c in self.calls if c['kind'] == 'succession']

    def build(self, *, institution='appointed', mode='mortal'):
        self.budget()
        self.institution, self.mode = institution, mode
        pack = product_cli.SCENARIOS / 'lifecycle-v1-iron-4.json'
        data = pack.read_bytes()
        manifest = json.loads(product_cli.MANIFEST.read_bytes())
        self.assertEqual(manifest['files']['app/data/scenarios/lifecycle-v1-iron-4.json'],
                         dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
        config = json.loads(data)
        self.published = copy.deepcopy(config)
        # Legal new-genesis derivation ONLY. Never mutate runtime locations.
        for country in config['countries']:
            for position in country['positions']:
                if position['person_id'] not in (KING, OLD):
                    position['location'] = country['places'][-1]['id']
        country = next(c for c in config['countries'] if c['id'] == 'country-1')
        self.destination = country['places'][-1]['id']
        self.assertNotEqual(self.destination, 'place-0')
        rules = config['lifecycle']
        if institution == 'vacant':
            country['institution'] = '河谷君主制；财政职位空缺后的补任办法尚未成立，现无自动代理惯例。'
            rules['institutional_facts'] = [f for f in rules['institutional_facts'] if f['office_id'] != OFFICE]
        elif institution == 'disputed':
            country['institution'] = '河谷君主制；财政机构资深书吏与资深仓吏各有并行自动代理习惯，未成立优先序。'
            fact = next(f for f in rules['institutional_facts'] if f['office_id'] == OFFICE)
            fact['institution_text'] = country['institution']
            fact['public_text'] = '本机构资深书吏有既成自动代理习惯，但与仓吏习惯无优先序。'
            second = copy.deepcopy(fact)
            second.update(id='institution-country-1-finance-parallel-acting',
                          background_role='该财政机构资深仓吏',
                          public_text='本机构资深仓吏另有并行自动代理习惯，但与书吏习惯无优先序。')
            rules['institutional_facts'].append(second)
        else:
            self.assertEqual(institution, 'appointed')
        self.assertEqual(config['pathology'], self.published['pathology'])
        self.assertEqual((config['horizon_days'], config['local_message_seconds'],
                          config['event_wakeup']['government_interval_seconds']), (90, 60, 86400))
        self.assertEqual(config['event_wakeup']['temporary_persons'], [])
        for key in ('personal_objects', 'relationship_facts'):
            self.assertEqual(rules[key], self.published['lifecycle'][key])
        if hasattr(self, 'object_case_id'):
            # Legal new genesis, after the original published-pack assertions.
            rules['personal_objects'][0]['id'] = self.object_case_id
            rules['personal_objects'].append(dict(id='private-old-object-1106-main',
                kind='held_iron_weapon', holder_person_id=OLD,
                source_text='仅旧财政官本人实持的独立私人铁器1106-main，不随职位交付。'))
        self.fixture_means_id = next(v['id'] for v in rules['personal_objects']
                                    if v['holder_person_id'] == KING)
        fixture = self.root / 'derived-genesis.json'
        fixture.write_text(wire(config), encoding='utf-8')
        self.owned = self.root / 'world'
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(product_cli.main(['--data-dir', str(self.owned), 'init',
                                               '--scenario', str(fixture)]), 0)
        self.assertFalse(json.loads(out.getvalue())['real_model'])
        self.assertEqual((self.owned / 'scenario.json').read_bytes(), fixture.read_bytes())
        self.genesis = self.raw(1)
        self.assertEqual(self.genesis['_lifecycle']['deaths'], [])
        self.assertEqual(self.genesis['_lifecycle']['jobs'], [])
        self.services = {kind:dict(provider='openai-chat-loopback',
            base_url='http://127.0.0.1:' + port + '/v1', model=kind + '-1099-synthetic',
            credential_ref=None, timeout_seconds=2, max_retries=0)
            for kind, port in (('roles', '18181'), ('dm', '18182'))}
        store = SettingsStore(self.owned / 'services.json', credential_factory=self.forbidden)
        store.save(dict(services=self.services, secrets={}))
        self.assertEqual(store.snapshot(), self.services)
        self.session = product_cli.ProductSession(self.owned, 'real')
        self.sessions.append(self.session)
        self.bind()
        self.source = product_cli.source_identity(self.session.world, self.session.host, self.session.coordinator)
        self.generation = product_cli.verify_generation()
        self.residents = [p['id'] for p in self.genesis['persons']]
        self.assertEqual(len(self.residents), 12)
        self.assertEqual([p['id'] for p in self.genesis['persons'] if p['location'] == 'place-0'], [KING, OLD])
        self.initial_resources = {k:copy.deepcopy(self.raw()[k]) for k in ('places', 'ledger', 'in_transit')}
        self.budget()

    def bind(self):
        s = self.session
        for value, cls in ((s, product_cli.ProductSession), (s.world, WorldStore),
                           (s.host, ActorHost), (s.coordinator, Coordinator),
                           (s.host.adjudicator, Adjudicator), (s.host._storage, PrivateStore),
                           (s.host.real_gateway, RealGateway), (s.host.real_settings, SettingsStore)):
            self.assertIs(type(value), cls)
        self.assertIs(s.host.adjudicator.world, s.world)
        self.assertIs(s.coordinator.actors, s.host)
        self.assertIs(s.host._authority, s.world)
        self.assertIs(s.host._storage.host, s.host)
        self.assertIs(s.host._storage.world, s.world)
        self.assertIs(s.host.resolve_lifecycle_jobs.__func__, ActorHost.resolve_lifecycle_jobs)
        self.assertIs(s.host._request.__func__, ActorHost._request)
        for name in ('_act', '_submit', '_finish', 'resolve_lifecycle_jobs'):
            self.assertNotIn(name, s.host.__dict__)
        for policy in (s.host.execution_policy, s.coordinator.execution_policy):
            self.assertEqual((policy.busy_budget_seconds, policy.act_deadline_seconds), (2, 120))
        s.host._choose = self.forbidden  # Deny sentinel, never supplies behavior.
        s.host.real_gateway.transport = self.transport  # The only response seam.

    def old_text(self):
        if self.institution == 'appointed':
            return '请只将这一段写入本机构公文：' + PUBLIC_DOC + '。另记私人考虑：' + PRIVATE_SECRET + '。并向' + KING + '发送：' + OLD_CHECK
        return '我保留未对外传出的私人考虑：' + PRIVATE_SECRET + '。并向' + KING + '发送：' + OLD_CHECK

    def assert_personal_object_input(self, payload, messages):
        pid = payload['person']['id']
        objects = payload['personal_objects']  # Unknown/missing wire format FAILS.
        self.assertIs(type(objects), list)
        for item in objects:
            self.assertIs(type(item), dict)
            self.assertEqual(set(item), {'id', 'kind', 'description'})
            for value in item.values():
                self.assertIs(type(value), str)
                self.assertTrue(value.strip())
        rules = self.genesis['_lifecycle']['rules']['personal_objects']
        expected = [dict(id=item['id'], kind=item['kind'], description=item['source_text'])
                    for item in rules if item['holder_person_id'] == pid]
        self.assertEqual(objects, expected)  # Independent genesis oracle only.
        for key in ('_lifecycle', '_config', 'rules', 'jobs', 'relationship_facts', 'session'):
            self.assertNotIn(key, payload)
        self.assertEqual(messages[-1]['role'], 'user')
        self.assertEqual(json.loads(messages[-1]['content']), payload)
        self.assertEqual(messages[0]['role'], 'system')
        for item in rules:
            self.assertNotIn(item['id'], messages[0]['content'])
            self.assertNotIn(item['source_text'], messages[0]['content'])
            if item['holder_person_id'] != pid:
                self.assertNotIn(item['id'], wire(messages))
                self.assertNotIn(item['source_text'], wire(messages))
        prepared = [r for r in self.rows('actor_intents', True)
                    if r['person_id'] == pid and r['state'] == 'prepared']
        self.assertEqual(len(prepared), 1)
        request = json.loads(prepared[0]['request'])
        self.assertEqual(request['schema_version'], 3)
        self.assertEqual(json.loads(request['prompt']), payload)
        self.assertEqual(request['identity_binding']['person_id'], pid)
        self.assertEqual(request['identity_binding']['observation_revision'], payload['revision'])
        self.assertEqual(messages[1:-1], [dict(role=v['role'], content=v['content'])
                                       for v in request['messages']])

    def role_response(self, view):
        pid, at = view['person']['id'], view['time']
        prior = self.role_calls(pid)
        response = dict(text=WAIT + pid, physical_attempt=None)
        if pid == KING and not prior:
            response['text'] = '本人向自己' + KING + '发送信使：' + CHECK
        elif pid == OLD and not prior:
            response['text'] = self.old_text()
        elif pid == KING and at == 60:
            if self.mode in ('mortal', 'nonfatal', 'no_contact'):
                held = [v for v in view['personal_objects'] if v['kind'] == 'held_iron_weapon']
                self.assertEqual(len(held), 1, 'choose only a real role-input object')
                response.update(text='本人选择以本人实持铁器近身击向' + OLD + '。随后本人离开，前往' + self.destination + '。',
                    physical_attempt=dict(mode='strike_with_held_weapon', target_person_id=OLD, means_id=held[0]['id']))
                if self.mode != 'mortal':
                    response['text'] = '本人选择以本人实持铁器近身击向' + OLD + '。随后本人留在现场，并向自己发出复核信使。'
            elif self.mode == 'claim':
                response['text'] = CLAIM + '我仍只选择等待，并向自己发出复核信使。'
            elif self.mode == 'travel':
                response['text'] = '本人选择离开前向自己发信，然后前往' + self.destination + '。'
            else:
                raise AssertionError('unexpected fixture mode')
        elif self.new is not None and pid == self.new and not prior:
            docs = view['office_documents']
            self.assertEqual(len(docs), 1)
            response['text'] = '本人到场，主动请求实际阅读机构文书' + docs[0]['id'] + '。'
        return response

    def effects(self, payload):
        proposal = payload['proposal']
        pid, text = proposal['person_id'], proposal['text']
        protocol = payload['opt_in_effect_protocol']
        self.assertEqual(protocol['version'], 'p09-lifecycle-v1')
        self.assertEqual(protocol['base_revision'], payload['world']['revision'])
        if pid == KING and text == '本人向自己' + KING + '发送信使：' + CHECK:
            return [dict(action='send', to=KING, text=CHECK)]
        if pid == OLD and text == self.old_text():
            result = []
            if self.institution == 'appointed':
                start = text.index(PUBLIC_DOC)
                result.append(dict(action='write_office_document', text=PUBLIC_DOC,
                                   quote_start=start, quote_end=start + len(PUBLIC_DOC)))
            # Real source excludes self-sent messages from event wake. A second,
            # independently authored OLD->KING message is the actual t60 wake.
            # KING's original self-message still has a real receipt and node.
            result.append(dict(action='send', to=KING, text=OLD_CHECK))
            return result
        if pid == KING and proposal['physical_attempt'] is not None:
            anchor = proposal['physical_attempt']
            role = self.role_calls(KING)[-1]
            self.assertEqual(anchor, role['response']['physical_attempt'])
            self.assertEqual(anchor['mode'], 'strike_with_held_weapon')
            self.assertEqual(anchor['target_person_id'], OLD)
            self.assertIn(anchor['means_id'], [v['id'] for v in role['view']['personal_objects']])
            self.assertEqual(protocol['physical_attempt'], anchor)
            target = next(p for p in protocol['scene_persons'] if p['id'] == anchor['target_person_id'])
            actor = next(p for p in protocol['scene_persons'] if p['id'] == proposal['person_id'])
            means = next(m for m in protocol['personal_objects'] if m['id'] == anchor['means_id'])
            self.assertEqual((actor['location'], target['location'], actor['alive'], target['alive'], means['holder_person_id']),
                             ('place-0', 'place-0', True, True, KING))
            revision = payload['world']['revision']
            result = [dict(action='resolve_physical_attempt', outcome=self.mode,
                basis_refs=[dict(kind='personal_object', id=means['id'], at_revision=revision),
                            dict(kind='scene_person', id=target['id'], at_revision=revision)],
                reason='按本人明确原锚及实际同地、实持手段裁有限客观接触后果，不替目标作选择。')]
            if self.mode == 'mortal':
                self.assertIn(self.destination, text)
                result.append(dict(action='travel', to=self.destination))
            else:
                result.append(dict(action='send', to=KING, text='本人另行复核非致命后果。'))
            return result
        if pid == KING and text.startswith(CLAIM):
            self.assertIsNone(proposal['physical_attempt'])
            return [dict(action='send', to=KING, text='本人复核未经证实的声明。')]
        if pid == KING and self.mode == 'travel' and self.destination in text:
            return [dict(action='send', to=KING, text='本人离开前发出的复核信使。'),
                    dict(action='travel', to=self.destination)]
        if self.new is not None and pid == self.new and text.startswith('本人到场，主动请求实际阅读机构文书'):
            document_id = next(d['id'] for d in self.raw()['_lifecycle']['documents'])
            self.assertIn(document_id, text)
            return [dict(action='read_office_document', document_id=document_id)]
        self.assertTrue(text.startswith(WAIT), 'DM must compile an actual captured authored intention')
        return [dict(action='wait')]

    def succession_response(self, payload):
        revision = payload['base_revision']
        if self.institution == 'vacant':
            self.assertEqual(payload['unresolved_background_slots'], [])
            self.assertEqual(payload['conflicts'], [])
            self.assertFalse(any(f['kind'] == 'institution' for f in payload['established_facts']))
            return dict(status='vacant', reason='本职位没有既成自动代理制度，保持正常空缺，不代君主任命。',
                        basis_refs=[dict(kind='vacancy', id=payload['vacancy_id'], at_revision=revision)],
                        successor=None, unresolved=['实际补任办法尚未成立。'])
        if self.institution == 'disputed':
            self.assertEqual(len(payload['unresolved_background_slots']), 2)
            self.assertEqual(len(payload['conflicts']), 2)
            return dict(status='disputed', reason='两项真实并行习惯无既成优先序，不强造共识或代君主任命。',
                        basis_refs=copy.deepcopy(payload['conflicts']), successor=None,
                        unresolved=['实际制度优先序未成立。'])
        self.assertEqual(len(payload['unresolved_background_slots']), 1)
        self.assertEqual(payload['conflicts'], [])
        slot = payload['unresolved_background_slots'][0]
        fact = next(f for f in payload['established_facts'] if 'slot-' + f['record']['id'] == slot['id'])
        return dict(status='appointed', reason='依据真实既成自动代理习惯，仅补未建模机构人员的身份与角色卡。',
                    basis_refs=[dict(kind='institution', id=fact['record']['id'], at_revision=revision)],
                    successor=dict(name='许衡书吏', background_role=slot['background_role'], location=slot['place_id'],
                        card=dict(values='守实账与职责边界', interests='维护机构账册', risk='谨慎', mission='依实际所知核账')),
                    unresolved=[])

    def transport(self, service, messages, token, budget):
        self.budget()
        budget.check()
        self.assertIsNone(token)
        payload = json.loads(messages[-1]['content'])
        if service['model'] == self.services['roles']['model']:
            self.assert_personal_object_input(payload, messages)
            response = self.role_response(payload)
            record = dict(kind='roles', view=copy.deepcopy(payload), messages=copy.deepcopy(messages),
                          response=response, token=token)
            pid = payload['person']['id']
            if self.new is not None and pid == self.new and not self.role_calls(pid):
                # Original Host already durably prepared a genuine empty v3
                # request. Read back original rows, not a synthetic empty input.
                self.first_new = dict(private={t:self.rows(t, True) for t in PRIVATE_TABLES},
                                      view=copy.deepcopy(payload), messages=copy.deepcopy(messages))
        else:
            self.assertEqual(service['model'], self.services['dm']['model'])
            if payload.get('kind') == 'succession':
                world, host, coordinator = self.session.world, self.session.host, self.session.coordinator
                root = world._scoped_coordinator_attempt()
                entry = world._scoped_entry_budget()
                active = host._lifecycle_active
                self.assertIs(type(root), CoordinatorAttempt)
                self.assertIs(root, coordinator._owner_attempt)
                self.assertIs(type(entry), PreparationBudget)
                self.assertIsNone(world._scoped_attempt())
                self.assertIsNone(host._attempt)
                self.assertIsNotNone(active)
                self.assertIs(type(active[1]), LifecycleCallBudget)
                self.assertIs(active[0], root)
                self.assertIs(active[1].entry, entry)
                self.assertIs(active[1].captured_root, root)
                self.assertIs(active[2], host.real_gateway)
                self.assertEqual(active[1].deadline, entry.deadline)
                self.assertEqual(active[1].calls, [])
                self.assertIs(threading.current_thread(), coordinator.worker)
                self.assertFalse(host._lock._is_owned(), '1098 root DM must not hold pipeline lock')
                raw = self.raw()
                windows = self.rows('coordinator_windows')
                latest = max(windows, key=lambda w:w['ordinal'])
                self.assertEqual((raw['time'], latest['target_time'], json.loads(latest['roster_json'])), (61, 61, []))
                self.assertEqual(latest['state'], 'completed')
                self.assertEqual([d['person_id'] for d in raw['_lifecycle']['deaths']], [OLD])
                self.assertEqual(raw['_lifecycle']['deliveries'], [])
                self.assertNotIn(payload['proposed_person_id'], {p['id'] for p in raw['persons']})
                job = next(j for j in raw['_lifecycle']['jobs'] if j['id'] == payload['job_id'])
                self.assertIsNone(job['result'])
                self.assertEqual(job['attempts'][-1]['request'], payload)
                self.assertEqual(job['attempts'][-1]['status'], 'pending')
                self.assertEqual(job['attempts'][-1]['request_sha256'], sha(payload))
                self.root_call = dict(raw=raw, windows=windows,
                    actor_leases=self.rows('actor_execution_leases'), private={t:self.rows(t, True) for t in PRIVATE_TABLES},
                    root_is_original=True, entry_id=id(entry), io_id=id(active[1]), root_id=id(root),
                    root_epoch=root.permit.epoch, worker_name=threading.current_thread().name)
                response = self.succession_response(payload)
                if response['status'] == 'appointed':
                    self.new = payload['proposed_person_id']
                record = dict(kind='succession', payload=copy.deepcopy(payload), messages=copy.deepcopy(messages),
                              response=response, token=token)
            else:
                effects = self.effects(payload)
                response = dict(ruling='成功', reason='只编译原提案者明确行为与具证客观后果。',
                                goal='有限真实行为', costs=[], benefits=[], effects=effects)
                record = dict(kind='dm', payload=copy.deepcopy(payload), messages=copy.deepcopy(messages),
                              response=response, token=token)
        record['response_raw'] = wire(response)
        self.calls.append(record)
        return dict(choices=[dict(message=dict(content=record['response_raw']))],
                    usage=dict(prompt_tokens=17, completion_tokens=9))

    def step(self, at, roster):
        self.budget()
        result = self.session.step(60)
        self.budget()
        worker = self.session.coordinator.worker
        self.assertIsNotNone(worker)
        worker.join(0)
        self.assertFalse(worker.is_alive())
        self.threads.append(worker)
        self.assertIsNone(self.session.coordinator.error)
        self.assertEqual(result['status'], 'user_paused')
        current = self.rows('coordinator_windows')
        self.assertEqual(len(current), len(self.windows) + 1)
        window = max(current, key=lambda w:w['ordinal'])
        self.assertEqual((window['state'], window['target_time'], json.loads(window['roster_json'])),
                         ('completed', at, roster))
        self.assertEqual(current[:-1], self.windows)
        self.windows = copy.deepcopy(current)
        self.assertEqual(self.observer_errors, [])
        self.assertFalse(self.session.host.cleanup_pending)
        self.assertFalse(self.session.coordinator.cleanup_pending)
        if self.observe_exits:
            self.original_actor_exits()
        return self.raw()

    def original_actor_exits(self):
        active = self.rows('coordinator_activations')
        if not active:
            return
        self.assertEqual(len(self.actor_exits), len(active))
        mapping = {a['activation_id']:a for a in active}
        seen = set()
        for record in self.actor_exits:
            attempt = record['attempt']
            self.assertIsNotNone(attempt)
            reference = attempt.activation
            self.assertIsNotNone(reference)
            self.assertNotIn(reference.activation_id, seen)
            seen.add(reference.activation_id)
            activation = mapping[reference.activation_id]
            self.assertEqual((reference.world_id, reference.window_id, reference.person_id),
                (activation['world_id'], activation['window_id'], activation['person_id']))
            self.assertEqual((attempt.permit.person_id, attempt.permit.epoch),
                             (activation['person_id'], activation['owner_actor_epoch']))
            for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                self.assertIs(record[key], True, 'original act-finally result, before any later close')
            self.assertIsNotNone(attempt.thread)
            attempt.thread.join(0)
            self.assertFalse(attempt.thread.is_alive())
            self.assertEqual(len(attempt.calls), 2)
            self.assertIsNot(attempt.calls[0], attempt.calls[1])
            for thread in attempt.calls:
                thread.join(0)
                self.assertFalse(thread.is_alive())
            actual_cas = [r for r in self.cas if r['kind'] == 'actor'
                and r['person_id'] == attempt.permit.person_id and r['epoch'] == attempt.permit.epoch]
            self.assertEqual([r['result'] for r in actual_cas], [True])
        self.assertEqual(seen, set(mapping))

    def arrange_t60(self):
        self.build(institution=self.institution, mode=self.mode)
        self.step(0, self.residents)
        initial = self.raw()
        self.assertEqual(initial['_lifecycle']['deaths'], [])
        self.assertEqual(initial['_lifecycle']['jobs'], [])
        messages = [q for q in initial['queue'] if q['kind'] == 'deliver' and q['recipient'] == KING]
        self.assertEqual({(q['sender'], q['at'], q['text']) for q in messages},
                         {(KING, 60, CHECK), (OLD, 60, OLD_CHECK)})
        self.assertEqual(len(messages), 2)
        self.old_rows = {t:[r for r in self.rows(t, True) if r.get('person_id') == OLD] for t in PRIVATE_TABLES}
        old_session = self.old_rows['actor_sessions']
        self.assertEqual(len(old_session), 1)
        self.assertTrue(json.loads(old_session[0]['messages']))
        self.assertIn(PRIVATE_SECRET, old_session[0]['messages'])
        self.before_injury_revision = initial['revision']
        self.before_injury_observation = self.session.world.observe(OLD, seq=initial['revision'])
        after = self.step(60, [KING])
        king = self.role_calls(KING)
        self.assertEqual([c['view']['time'] for c in king], [0, 60])
        self.assertIn(OLD_CHECK, wire(king[-1]['view']))
        self.assertEqual(after['_lifecycle']['deaths'], [])
        self.assertEqual(after['_lifecycle']['jobs'], [])
        return after

    def mortal_t60(self, *, institution='appointed', replay=False):
        self.mode, self.institution = 'mortal', institution
        self.replay_enabled = replay
        state = self.arrange_t60()
        injuries = [t for t in state['_lifecycle']['transitions'] if t['kind'] == 'mortal_injury']
        self.assertEqual(len(injuries), 1)
        injury = injuries[0]
        self.assertEqual((injury['person_id'], injury['actor_person_id'], injury['means_id'], injury['occurred_at']),
                         (OLD, KING, self.fixture_means_id, 60))
        due = [q for q in state['queue'] if q['kind'] == 'person_death_due']
        self.assertEqual(due, [injury['node']])
        self.assertEqual(due[0]['at'], 61)
        self.assertEqual(due[0]['cause_id'], injury['cause_ref']['child_intent_id'])
        arrivals = [q for q in state['queue'] if q['kind'] == 'arrival' and q['person_id'] == KING]
        self.assertEqual(len(arrivals), 1)
        self.assertGreater(arrivals[0]['at'], 61)
        self.assertEqual(arrivals[0]['destination'], self.destination)
        self.assertIsNone(next(p for p in state['persons'] if p['id'] == KING)['location'])
        self.injury = copy.deepcopy(injury)
        self.travel_node = copy.deepcopy(arrivals[0])
        self.old_predeath = {t:[r for r in self.rows(t, True) if r.get('person_id') == OLD] for t in PRIVATE_TABLES}
        self.assertEqual(self.old_predeath, self.old_rows)
        return state

    def replay_completed_job(self, owned):
        root, entry, io_budget = owned['captured_root'], owned['entry'], owned['io']
        self.assertIs(root, self.session.coordinator._owner_attempt)
        self.assertIs(entry, self.session.world._scoped_entry_budget())
        self.assertIs(io_budget.entry, entry)
        self.assertEqual((id(entry), id(io_budget), id(root)),
                         (self.root_call['entry_id'], self.root_call['io_id'], self.root_call['root_id']))
        self.assertTrue(self.session.host._lock._is_owned())
        before = self.business()
        count = len(self.calls)
        a = self.session.host.adjudicator
        job, request_sha, carrier = owned['job_id'], owned['request_sha256'], owned['carrier']
        original = owned['result']
        prepared = a.prepare_lifecycle(job, owned['service_version'])
        preview = a.preview_lifecycle(job, request_sha, carrier['response_raw'], carrier['response'])
        replayed = a.commit_lifecycle(job, request_sha, carrier['response_raw'], carrier['response'],
                                     decision_sha256=original['decision_sha256'])
        self.assertEqual(prepared, dict(status='completed', result=original))
        self.assertEqual(preview, dict(status='completed', result=original))
        self.assertEqual(replayed, original)
        self.assertEqual(self.business(), before)
        self.assertEqual(len(self.calls), count)
        io_budget.check()
        self.replays.append(dict(job_id=job, result=copy.deepcopy(original), provider_calls=count,
                                 before=before, after=self.business()))

    def death_and_result(self, status):
        state = self.step(61, [])
        self.assertIsNotNone(self.root_call)
        roots = self.root_calls()
        self.assertEqual(len(roots), 1)
        call, life = roots[0], state['_lifecycle']
        self.assertEqual(len(life['deaths']), 1)
        death = life['deaths'][0]
        self.assertEqual(death, dict(id='death-' + self.injury['id'], person_id=OLD,
            injury_id=self.injury['id'], occurred_at=61, committed_revision=self.root_call['raw']['_lifecycle']['deaths'][0]['committed_revision'],
            cause_ref=self.injury['cause_ref'], node_id=self.injury['node']['id'],
            source_snapshot_revision=self.injury['committed_revision']))
        self.assertEqual(len(life['vacancies']), 1)
        vacancy = life['vacancies'][0]
        self.assertEqual((vacancy['office_id'], vacancy['former_person_id'], vacancy['death_id'], vacancy['status']),
                         (OFFICE, OLD, death['id'], status))
        self.assertEqual(vacancy['opened_revision'], death['committed_revision'])
        self.assertEqual(len(life['jobs']), 1)
        job = life['jobs'][0]
        self.assertEqual(job['id'], 'vacancy:' + OFFICE + ':' + death['id'])
        self.assertEqual(job['vacancy_id'], vacancy['id'])
        self.assertEqual(job['generation'], 1)
        self.assertEqual(len(job['attempts']), 1)
        attempt = job['attempts'][0]
        self.assertEqual(attempt['request'], call['payload'])
        self.assertEqual(attempt['request_id'], call['payload']['request_id'])
        self.assertEqual(attempt['prepared_revision'], call['payload']['base_revision'])
        self.assertEqual(attempt['response'], call['response'])
        self.assertEqual(attempt['response_raw'], call['response_raw'])
        self.assertEqual(attempt['status'], 'committed')
        self.assertIsNone(attempt['failure_code'])
        result = job['result']
        self.assertEqual(result, dict(job_id=job['id'], vacancy_id=vacancy['id'], generation=1,
            status=status, person_id=self.new if status == 'appointed' else None,
            committed_revision=result['committed_revision'], request_sha256=sha(call['payload']),
            decision_sha256=sha(call['response'])))
        self.assertGreater(result['committed_revision'], attempt['prepared_revision'])
        old_tenure = next(t for t in life['tenures'] if t['person_id'] == OLD)
        self.assertEqual(old_tenure['to_revision'], death['committed_revision'])
        self.assertEqual({t:[r for r in self.rows(t, True) if r.get('person_id') == OLD] for t in PRIVATE_TABLES}, self.old_predeath)
        self.assertEqual([c['view']['time'] for c in self.role_calls(OLD)], [0])
        self.assertEqual(life['deliveries'], [])
        self.assertEqual(len([e for e in state['events'] if e['kind'] == 'person_death']), 1)
        self.assertEqual([q for q in state['queue'] if q['kind'] == 'person_death_due'], [])
        self.assertIn(self.travel_node, state['queue'])
        self.assertEqual(self.session.world.pending_lifecycle_jobs(), [])
        return state, job, death

    def exact_lineage(self):
        self.budget()
        public_rows, leaf_rows = self.rows('intents'), self.rows('actor_intents', True)
        active = self.rows('coordinator_activations')
        self.assertTrue(public_rows)
        self.assertTrue(leaf_rows)
        self.assertTrue(active)
        public = {r['id']:r for r in public_rows}
        leaves = {r['intent_id']:r for r in leaf_rows}
        self.assertEqual(len(public), len(public_rows))
        self.assertEqual(len(leaves), len(leaf_rows))
        self.assertEqual({a['intent_id'] for a in active}, set(leaves))
        metas = {r['intent_id']:r for r in self.rows('actor_intent_execution', True)}
        outcomes = {r['activation_id']:r for r in self.rows('coordinator_activation_outcomes')}
        proposals = {r['intent_id']:r for r in self.rows('dm_proposals')}
        decisions = self.rows('dm_decisions')
        timeline = self.rows('dm_timeline')
        self.assertEqual(set(metas), set(leaves))
        self.assertEqual(set(proposals), set(leaves))
        self.assertEqual(set(outcomes), {a['activation_id'] for a in active})
        self.assertEqual(len(decisions), len(active))
        self.assertEqual(len(timeline), 3 * len(active))
        expected = set(leaves)
        for activation in active:
            intent, pid = activation['intent_id'], activation['person_id']
            leaf, meta = leaves[intent], metas[intent]
            q, candidate, body = (json.loads(leaf[k]) for k in ('request', 'candidate', 'body'))
            self.assertEqual(q['schema_version'], 3)
            self.assertIsNone(q['body'])
            self.assertEqual((q['person_id'], q['intent_id']), (pid, intent))
            self.assertEqual(q['identity_binding'], self.session.world.lifecycle_identity(pid, revision=q['identity_binding']['observation_revision']))
            self.assertEqual(q['identity_binding']['observation_revision'], activation['observation_revision'])
            self.assertEqual(candidate[:-2], q['messages'])
            self.assertEqual(candidate[-2], dict(role='user', content=q['prompt']))
            self.assertEqual(candidate[-1]['content'], body['text'])
            self.assertEqual(candidate[-1]['intent_binding']['body'], body)
            self.assertEqual(candidate[-1]['intent_binding']['request_sha256'], sha(q))
            self.assertEqual(candidate[-1]['intent_binding']['schema_version'], 3)
            self.assertEqual((activation['request_sha256'], activation['candidate_sha256']), (sha(q), sha(candidate)))
            matching = [c for c in self.role_calls(pid) if c['view']['revision'] == activation['observation_revision']]
            self.assertEqual(len(matching), 1)
            call = matching[0]
            self.assertEqual(call['response'], {k:body[k] for k in ('text', 'physical_attempt')})
            self.assertEqual(call['messages'][1:-1], [dict(role=m['role'], content=m['content']) for m in q['messages']])
            self.assertEqual(json.loads(call['messages'][-1]['content']), json.loads(q['prompt']))
            self.assertEqual(json.loads(public[intent]['payload']), dict(body=body))
            dm = [c for c in self.calls if c['kind'] == 'dm' and c['payload']['proposal']['intent_id'] == intent]
            self.assertEqual(len(dm), 1)
            selected = [d for d in decisions if d['intent_id'] == intent]
            self.assertEqual(len(selected), 1)
            decision = json.loads(selected[0]['data'])
            self.assertEqual(decision['effects'], dm[0]['response']['effects'])
            self.assertEqual(dm[0]['payload']['proposal']['physical_attempt'], body['physical_attempt'])
            self.assertEqual((activation['phase'], activation['private_closed'], leaf['state']), ('completed', 1, 'committed'))
            self.assertEqual((meta['base_version'], meta['promoted_version']),
                             (activation['base_session_version'], activation['base_session_version'] + 1))
            receipt = json.loads(public[intent]['receipt'])
            self.assertEqual(json.loads(leaf['receipt']), receipt)
            self.assertEqual(self.session.world.receipt(pid, intent), receipt)
            self.assertEqual((receipt['person_id'], receipt['intent_id'], receipt['status']), (pid, intent, 'committed'))
            for index, effect in enumerate(decision['effects']):
                child = Adjudicator._child_id(intent, selected[0]['version'], index)
                expected.add(child)
                self.assertEqual(public[child]['person'], pid)
                self.assertEqual(json.loads(public[child]['payload']), dict(parent=intent, version=selected[0]['version'], body=effect))
                child_receipt = json.loads(public[child]['receipt'])
                self.assertEqual((child_receipt['person_id'], child_receipt['intent_id']), (pid, child))
                self.assertEqual(receipt['effect']['effects'][index], child_receipt['effect'])
            self.assertEqual(json.loads(outcomes[activation['activation_id']]['outcome_json'])['intent_id'], intent)
            self.assertEqual([r['call_id'] for r in self.rows('actor_calls', True) if r['intent_id'] == intent], ['real-' + intent])
            metrics = json.loads(leaf['metrics'])
            self.assertEqual((metrics['provider'], metrics['sdk_requests'], metrics['sdk_events']),
                             ('real-chat-v1', 0, {'http_requests':1}))
        self.assertEqual(set(public), expected)
        for head in self.rows('actor_session_heads', True):
            spine = sorted([a for a in active if a['person_id'] == head['person_id']], key=lambda a:a['promoted_version'])
            self.assertTrue(spine)
            self.assertEqual([a['promoted_version'] for a in spine], list(range(1, len(spine) + 1)))
            messages, seen = [], []
            for a in spine:
                leaf = leaves[a['intent_id']]
                self.assertEqual(json.loads(leaf['request'])['messages'], messages)
                self.assertEqual(metas[a['intent_id']]['base_messages_sha256'], sha(messages))
                self.assertEqual(metas[a['intent_id']]['base_seen_sha256'], sha(seen))
                messages, seen = json.loads(leaf['candidate']), json.loads(leaf['consumed'])
            session = next(r for r in self.rows('actor_sessions', True) if r['person_id'] == head['person_id'])
            self.assertEqual(json.loads(session['messages']), messages)
            self.assertEqual(json.loads(session['seen']), seen)
            self.assertEqual((head['version'], head['last_promoted_intent_id'], head['messages_sha256'], head['seen_sha256']),
                             (len(spine), spine[-1]['intent_id'], sha(messages), sha(seen)))
        self.assertEqual(self.violations, [])
        self.assertEqual(self.observer_errors, [])
        self.assertEqual(product_cli.verify_generation(), self.generation)
        self.assertEqual(product_cli.source_identity(self.session.world, self.session.host, self.session.coordinator), self.source)
        for call in self.calls:
            self.assertIsNone(call['token'])
        self.budget()

    def both_consumers(self):
        before, provider_count = self.business(), len(self.calls)
        c, w = self.session.coordinator, self.session.world
        with c._owner_scope(), c._preparation_scope(c._owner_attempt):
            for window in self.windows:
                self.budget()
                with scheduler.source_lock(w), w._connection(readonly=True) as db:
                    db.row_factory = sqlite3.Row
                    db.execute('ATTACH DATABASE ? AS actor', ((self.owned / 'actors.sqlite').as_uri() + '?mode=ro',))
                    db.execute('BEGIN')
                    native = scheduler.window_record(db, self.genesis['world_id'], window['window_id'])
                    scheduler.window_phase_proof(w, db, window['window_id'])
                self.assertEqual(native['roster'], json.loads(window['roster_json']))
                self.assertEqual(native['state'], 'completed')
                # Real next_activation: original collect -> offlock fullproof ->
                # fresh native seal. Completed historic windows have no next row.
                self.assertIsNone(w.next_activation(window['window_id'], coordinator=c._owner_attempt.permit))
        self.assertEqual(self.business(), before)
        self.assertEqual(len(self.calls), provider_count)
        self.budget()

    def assert_no_death(self, state):
        life = state['_lifecycle']
        for key in ('deaths', 'vacancies', 'jobs', 'deliveries'):
            self.assertEqual(life[key], [])
        self.assertEqual([t for t in life['transitions'] if t['kind'] in ('mortal_injury', 'person_death', 'person_introduced')], [])
        self.assertEqual([q for q in state['queue'] if q['kind'] == 'person_death_due'], [])
        self.assertEqual(self.root_calls(), [])
        self.assertEqual({p['id'] for p in state['persons']}, set(self.residents))
        self.assertTrue(self.session.world.observe(OLD)['person']['alive'])
        self.assertTrue(self.session.world.observe(OLD)['person']['current_office'])
        self.assertIsNone(next(t for t in life['tenures'] if t['person_id'] == OLD)['to_revision'])
        self.assertEqual(state['status'], 'user_paused')

    def close_checked(self):
        self.budget()
        s = self.session
        root = s.coordinator._owner_attempt
        workers = tuple(self.threads)
        before_cas = len(self.cas)
        s.close()
        self.assertFalse(s.host.cleanup_pending)
        self.assertFalse(s.coordinator.cleanup_pending)
        self.assertTrue(s.host._closed)
        self.assertIsNone(s.host._process)
        self.assertIsNone(s.host._attempt)
        self.assertIsNone(s.host._lifecycle_active)
        self.assertEqual(s.host._lease_cleanups, [])
        self.assertFalse(s.host._lease_cleanup_pending)
        for worker in workers:
            worker.join(0)
            self.assertFalse(worker.is_alive())
        if s.coordinator.worker is not None:
            s.coordinator.worker.join(0)
            self.assertFalse(s.coordinator.worker.is_alive())
        self.assertIsNotNone(root)
        self.assertIsNotNone(root.thread)
        root.thread.join(0)
        self.assertFalse(root.thread.is_alive())
        for thread in s.host._pending_threads:
            thread.join(0)
            self.assertFalse(thread.is_alive())
        releases = self.cas[before_cas:]
        if self.observe_exits:
            self.original_actor_exits()
            self.assertEqual([r['result'] for r in releases if r['kind'] == 'root' and r['epoch'] == root.permit.epoch], [True])
            self.assertTrue(self.cas)
            # Original CAS results individually observed, never inferred from close.
            for release in self.cas:
                self.assertIs(release['result'], True)
        else:
            self.assertEqual(self.cas, [])
            self.assertEqual(self.actor_exits, [])
        for table in ('actor_execution_leases', 'coordinator_execution_leases'):
            rows = self.rows(table)
            self.assertTrue(rows)
            for row in rows:
                self.assertIsNone(row['owner_token'])
        self.budget()

    def test_product_mortal_result_root_succession_empty_session_document_and_historical_reopen(self):
        self.mortal_t60(replay=True)
        life = self.raw()['_lifecycle']
        self.assertEqual(len(life['documents']), 1)
        document = life['documents'][0]
        self.assertEqual((document['author_person_id'], document['office_id'], document['place_id'], document['text']),
                         (OLD, OFFICE, 'place-0', PUBLIC_DOC))
        self.assertNotIn(PRIVATE_SECRET, wire(document))
        state, job, death = self.death_and_result('appointed')
        self.assertTrue(self.replay_fired)
        self.assertEqual(len(self.replays), 1)
        self.assertEqual(self.observer_errors, [])
        self.assertRegex(self.new, r'^person-[0-9a-f]{32}$')
        self.assertNotIn(self.new, self.residents)
        self.assertNotEqual(self.new, OLD)
        self.assertEqual(self.replays[0]['result'], job['result'])
        origin = state['_lifecycle']['person_origins'][self.new]
        self.assertEqual(origin['kind'], 'dm_introduction')
        self.assertEqual(origin['source_ref'], dict(kind='lifecycle_job', job_id=job['id'], generation=1,
            request_id=job['attempts'][0]['request_id'], committed_revision=job['result']['committed_revision']))
        self.assertEqual(origin['identity']['office_id'], OFFICE)
        new_tenure = next(t for t in state['_lifecycle']['tenures'] if t['person_id'] == self.new)
        self.assertEqual((new_tenure['from_revision'], new_tenure['to_revision']), (job['result']['committed_revision'], None))
        self.assertEqual(state['_observations'][self.new], [])
        self.assertEqual(state['_messages'][self.new], [])
        self.assertEqual(state['_p04']['resident_last_dispatch'][self.new], -1)
        self.assertIn(self.new, state['_p04']['pending'])
        self.assertEqual(state['_p04']['dispatch'], [])
        for table in ('actor_sessions', 'actor_session_heads', 'actor_intents'):
            self.assertEqual([r for r in self.rows(table, True) if r['person_id'] == self.new], [])
        self.assertNotIn(PRIVATE_SECRET, wire(self.root_calls()[0]['payload']))
        self.assertNotIn(PUBLIC_DOC, wire(self.root_calls()[0]['payload']))
        self.assertEqual(len(self.root_calls()[0]['messages']), 2)
        # Original same-time opening after genuine new identity (1096-F1).
        self.step(61, [self.new])
        self.assertIsNotNone(self.first_new)
        first = self.first_new
        qrow = next(r for r in first['private']['actor_intents'] if r['person_id'] == self.new)
        request = json.loads(qrow['request'])
        self.assertEqual(request['messages'], [])
        self.assertEqual(request['schema_version'], 3)
        self.assertEqual(request['identity_binding']['person_id'], self.new)
        self.assertEqual(request['identity_binding']['tenure_ref'], new_tenure['id'])
        head = next(r for r in first['private']['actor_session_heads'] if r['person_id'] == self.new)
        session = next(r for r in first['private']['actor_sessions'] if r['person_id'] == self.new)
        self.assertEqual((head['version'], json.loads(session['messages']), json.loads(session['seen'])), (0, [], []))
        self.assertEqual([r for r in first['private']['actor_intent_execution'] if r['person_id'] == self.new][0]['promoted_version'], None)
        self.assertNotIn(PUBLIC_DOC, wire(request))
        self.assertNotIn(PRIVATE_SECRET, wire(request))
        self.assertEqual([d['id'] for d in first['view']['office_documents']], [document['id']])
        after_read = self.raw()
        deliveries = after_read['_lifecycle']['deliveries']
        self.assertEqual(len(deliveries), 1)
        delivery = deliveries[0]
        self.assertEqual((delivery['document_id'], delivery['recipient_person_id'], delivery['channel'], delivery['text_sha256']),
                         (document['id'], self.new, 'onsite_document_read', hashlib.sha256(PUBLIC_DOC.encode('utf-8')).hexdigest()))
        self.assertIn(PUBLIC_DOC, wire(self.session.world.observe(self.new)))
        self.assertNotIn(PRIVATE_SECRET, wire(self.session.world.observe(self.new)))
        deferred = [q for q in after_read['queue'] if q['kind'] == 'p04_deferred_wake' and q['person_id'] == self.new]
        self.assertEqual(len(deferred), 1)
        self.assertEqual((deferred[0]['at'], deferred[0]['requested_at'], deferred[0]['cause_id']),
                         (121, 61, delivery['cause_ref']['child_intent_id']))
        self.step(121, [self.new])
        new_calls = self.role_calls(self.new)
        self.assertEqual([c['view']['time'] for c in new_calls], [61, 121])
        self.assertIn(PUBLIC_DOC, wire(new_calls[-1]['view']))
        self.assertNotIn(PRIVATE_SECRET, wire(new_calls[-1]['messages']))
        self.assertEqual(self.raw()['_lifecycle']['deliveries'], deliveries)
        self.assertEqual(len(self.root_calls()), 1)
        for pid in self.residents:
            if pid != OLD:
                self.assertNotIn(PUBLIC_DOC, wire(self.session.world.observe(pid)))
                self.assertNotIn(PRIVATE_SECRET, wire(self.session.world.observe(pid)))
        for key, value in self.initial_resources.items():
            self.assertEqual(self.raw()[key], value)
        self.assertEqual({t:[r for r in self.rows(t, True) if r.get('person_id') == OLD] for t in PRIVATE_TABLES}, self.old_predeath)
        self.assertEqual(self.session.world.observe(OLD, seq=self.before_injury_revision), self.before_injury_observation)
        self.assertNotIn(self.new, {p['id'] for p in self.session.world.snapshot(seq=death['committed_revision'])['persons']})
        self.assertFalse(self.session.world.observe(OLD, seq=death['committed_revision'])['person']['alive'])
        self.assertFalse(self.session.world.observe(OLD)['person']['current_office'])
        self.assertIn(self.new, {p['id'] for p in self.session.world.snapshot(seq=job['result']['committed_revision'])['persons']})
        self.exact_lineage()
        self.both_consumers()
        at = self.raw()['revision']
        historical = self.session.coordinator.snapshot(seq=at)
        observations = {pid:self.session.world.observe(pid, seq=at) for pid in (OLD, KING, self.new)}
        trace = self.session.host.adjudicator.trace(seq=at)
        receipts = {r['id']:self.session.world.receipt(r['person'], r['id']) for r in self.rows('intents')}
        saved = self.business()
        self.close_checked()
        # Cold original native initialize BEFORE creating a new Host, with the
        # exact owned persisted genesis and registration/actor inode read path.
        cold = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        self.assertEqual(cold.initialize(), self.session.world.snapshot())
        self.assertEqual(self.business(), saved)
        self.session = product_cli.ProductSession(self.owned, 'real')
        self.sessions.append(self.session)
        self.bind()
        count = len(self.calls)
        self.assertEqual(self.business(), saved)
        self.assertEqual(self.session.coordinator.snapshot(seq=at), historical)
        self.assertEqual(self.session.host.adjudicator.trace(seq=at), trace)
        self.assertEqual({pid:self.session.world.observe(pid, seq=at) for pid in observations}, observations)
        self.assertEqual({r['id']:self.session.world.receipt(r['person'], r['id']) for r in self.rows('intents')}, receipts)
        self.assertEqual(product_cli.read_owned(self.owned, seq=at)['revision'], at)
        self.exact_lineage()
        self.both_consumers()
        self.assertEqual(len(self.calls), count)
        self.close_checked()

    def test_product_no_established_automatic_institution_keeps_normal_vacancy(self):
        self.mortal_t60(institution='vacant', replay=True)
        state, job, death = self.death_and_result('vacant')
        self.assertIsNone(job['result']['person_id'])
        self.assertEqual(len(state['persons']), len(self.residents))
        self.assertEqual(state['_lifecycle']['documents'], [])
        self.assertEqual([o for o in state['_lifecycle']['person_origins'].values() if o['kind'] == 'dm_introduction'], [])
        self.assertTrue(self.replay_fired)
        self.assertEqual(len(self.replays), 1)
        self.assertEqual(job['attempts'][0]['response']['basis_refs'],
                         [dict(kind='vacancy', id=job['vacancy_id'], at_revision=job['attempts'][0]['prepared_revision'])])
        self.normal_succession_continues(job, death)

    def test_product_parallel_established_institutions_keep_normal_dispute(self):
        self.mortal_t60(institution='disputed', replay=True)
        state, job, death = self.death_and_result('disputed')
        request, response = job['attempts'][0]['request'], job['attempts'][0]['response']
        self.assertEqual(response['basis_refs'], request['conflicts'])
        self.assertEqual(len(response['basis_refs']), 2)
        facts = {f['record']['id']:f for f in request['established_facts'] if f['kind'] == 'institution'}
        self.assertEqual(set(facts), {r['id'] for r in response['basis_refs']})
        self.assertEqual(len({f['record']['background_role'] for f in facts.values()}), 2)
        self.assertIsNone(response['successor'])
        self.assertEqual(len(state['persons']), len(self.residents))
        self.assertTrue(self.replay_fired)
        self.assertEqual(len(self.replays), 1)
        self.normal_succession_continues(job, death)

    def normal_succession_continues(self, job, death):
        # A real subsequent queued event, not a direct advance or a new budget.
        next_at = self.raw()['queue'][0]['at']
        self.assertGreater(next_at, 61)
        expected = WorldStore._p04_target(self.raw())
        self.assertEqual(next_at, expected)
        # The next ordinary window is driven by original product code; its roster
        # depends on the real earliest event (travel/contact), not a guessed clock.
        self.budget()
        self.session.step(60)
        self.budget()
        worker = self.session.coordinator.worker
        worker.join(0)
        self.assertFalse(worker.is_alive())
        self.threads.append(worker)
        self.assertIsNone(self.session.coordinator.error)
        current = self.rows('coordinator_windows')
        self.assertEqual(len(current), len(self.windows) + 1)
        self.assertEqual(current[:-1], self.windows)
        self.windows = current
        self.assertEqual(self.raw()['time'], expected)
        self.assertEqual(self.raw()['status'], 'user_paused')
        self.assertEqual(self.raw()['_lifecycle']['jobs'], [job])
        self.assertEqual(self.raw()['_lifecycle']['deaths'], [death])
        self.assertEqual(len(self.root_calls()), 1)
        self.assertEqual([c['view']['time'] for c in self.role_calls(OLD)], [0])
        self.assertEqual({p['id'] for p in self.raw()['persons']}, set(self.residents))
        self.assertEqual({r['person_id'] for r in self.rows('actor_sessions', True)}, set(self.residents))
        self.exact_lineage()
        self.both_consumers()
        self.close_checked()

    def nonfatal_case(self, outcome):
        self.mode = outcome
        state = self.arrange_t60()
        events = [e for e in state['events'] if e['kind'] == 'physical_attempt_resolved']
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual((event['outcome'], event['person_id'], event['target_person_id'], event['means_id']),
                         (outcome, KING, OLD, self.fixture_means_id))
        self.assertTrue(event['basis_refs'])
        child = next(r for r in self.rows('intents') if r['id'] == event['cause_id'])
        self.assertEqual(json.loads(child['payload'])['body']['outcome'], outcome)
        self.assertEqual(json.loads(child['receipt'])['effect'], dict(implemented=True, outcome=outcome))
        self.assert_no_death(state)
        # Actual self-message changes no wake; the t120 empty opening still
        # crosses death_delay while material/native consumers see a living OLD.
        self.step(120, [])
        self.assert_no_death(self.raw())
        self.assertEqual([e for e in self.raw()['events'] if e['kind'] == 'physical_attempt_resolved'], events)
        self.exact_lineage()
        self.both_consumers()
        self.close_checked()

    def test_product_no_contact_real_physical_resolution_never_schedules_death(self):
        self.nonfatal_case('no_contact')

    def test_product_nonfatal_real_physical_resolution_never_schedules_death(self):
        self.nonfatal_case('nonfatal')

    def test_product_unanchored_ordinary_death_claim_does_not_kill(self):
        self.mode = 'claim'
        state = self.arrange_t60()
        claim = self.role_calls(KING)[-1]
        self.assertIn(CLAIM, claim['response']['text'])
        self.assertIsNone(claim['response']['physical_attempt'])
        physical = [e for e in state['events'] if e['kind'] == 'physical_attempt_resolved']
        self.assertEqual(physical, [])
        self.assert_no_death(state)
        self.step(120, [])
        self.assert_no_death(self.raw())
        self.exact_lineage()
        self.both_consumers()
        self.close_checked()

    def test_optin_real_travel_then_opening_uses_actual_genesis_in_all_consumers(self):
        # Separate uninstrumented positive: no observer is mounted for any CLI,
        # model, World, scheduler, writer, close or cold operation in this case.
        self.restore_observer()
        self.observe_exits = False
        self.assertIsNone(sys.gettrace())
        self.assertIsNone(threading.gettrace())
        self.mode = 'travel'
        state = self.arrange_t60()
        arrivals = [q for q in state['queue'] if q['kind'] == 'arrival' and q['person_id'] == KING]
        self.assertEqual(len(arrivals), 1)
        self.assertGreater(arrivals[0]['at'], 120)
        self.assertIsNone(next(p for p in state['persons'] if p['id'] == KING)['location'])
        self.step(120, [])
        window = self.windows[-1]
        base, advanced = self.raw(window['base_revision']), self.raw(window['advanced_revision'])
        self.assertGreater(base['revision'], 1)
        self.assertIsNone(next(p for p in base['persons'] if p['id'] == KING)['location'])
        before, count = self.business(), len(self.calls)
        self.assertEqual(WorldStore._p04_expected(base, advanced, genesis=self.genesis), [])
        self.assert_no_death(self.raw())
        self.exact_lineage()
        self.both_consumers()
        self.assertEqual(self.business(), before)
        self.assertEqual(len(self.calls), count)
        self.close_checked()

    def test_optin_real_travel_base_without_actual_genesis_is_deeply_rejected(self):
        self.mode = 'travel'
        self.arrange_t60()
        self.step(120, [])
        window = self.windows[-1]
        base, advanced = self.raw(window['base_revision']), self.raw(window['advanced_revision'])
        self.assertGreater(base['revision'], 1)
        self.assertIsNone(next(p for p in base['persons'] if p['id'] == KING)['location'])
        self.assertEqual(WorldStore._p04_expected(base, advanced, genesis=self.genesis), [])
        self.both_consumers()  # Both original consumers first pass real producers.
        before, count = self.business(), len(self.calls)
        with self.assertRaises(WorldError) as missing:
            WorldStore._p04_expected(base, advanced)
        self.assertEqual(missing.exception.code, 'ACTOR_RECOVERY_UNPROVEN')
        self.assertIn('actual immutable genesis', str(missing.exception))
        false_genesis = copy.deepcopy(base)
        false_genesis['revision'] = 1
        # Same world and correctly typed revision cannot disguise a moved base
        # as genesis; original strict seed identity/location guard must reject.
        with self.assertRaises(WorldError) as forged:
            WorldStore._p04_expected(base, advanced, genesis=false_genesis)
        self.assertEqual(forged.exception.code, 'ACTOR_RECOVERY_UNPROVEN')
        self.assertIn('首人物与声明不符', str(forged.exception))
        self.assertEqual(self.business(), before)
        self.assertEqual(len(self.calls), count)
        self.exact_lineage()
        self.close_checked()

    def test_role_object_alpha_full_original_lifecycle_and_privacy(self):
        self.object_case_id = 'owned-independent-alpha-1106-main'
        self.test_product_mortal_result_root_succession_empty_session_document_and_historical_reopen()
        self.assertEqual(self.fixture_means_id, self.object_case_id)
        self.assertEqual(self.first_new['view']['personal_objects'], [])
        self.assertEqual(self.session.world.observe(OLD, seq=1)['personal_objects'], [
            dict(id='private-old-object-1106-main', kind='held_iron_weapon',
                 description='仅旧财政官本人实持的独立私人铁器1106-main，不随职位交付。')])
        self.assertEqual(self.session.world.observe(self.new)['personal_objects'], [])

    def test_role_object_beta_full_original_lifecycle_and_privacy(self):
        self.object_case_id = 'owned-independent-beta-1106-main'
        self.test_product_mortal_result_root_succession_empty_session_document_and_historical_reopen()
        self.assertEqual(self.fixture_means_id, self.object_case_id)
        self.assertEqual(self.first_new['view']['personal_objects'], [])
        self.assertEqual(self.session.world.observe(OLD, seq=1)['personal_objects'], [
            dict(id='private-old-object-1106-main', kind='held_iron_weapon',
                 description='仅旧财政官本人实持的独立私人铁器1106-main，不随职位交付。')])
        self.assertEqual(self.session.world.observe(self.new)['personal_objects'], [])

    def tearDown(self):
        try:
            if self.session is not None and not self.session.host._closed:
                self.close_checked()
            self.assertEqual(self.observer_errors, [])
            self.assertEqual(self.violations, [])
            self.budget()
        finally:
            self.restore_observer()
            # Diagnostics only after joins/outside proof, never in a hot gate.
            (self.root / 'synthetic-calls.json').write_text(wire(self.calls), encoding='utf-8')
            (self.root / 'source-definition-runtime-metadata.json').write_text(wire(dict(
                synthetic_transport=True, live_model=False, source_only_authoring=True,
                cas=self.cas, observer_errors=self.observer_errors,
                elapsed_seconds=time.monotonic() - self.started,
                claim='runtime evidence exists only if a future complete method returns')),
                encoding='utf-8')
            self.stack.close()
