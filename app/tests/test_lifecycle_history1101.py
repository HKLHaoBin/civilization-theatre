"""1101/W4 acceptance definitions; SOURCE ONLY, never executed by the author.

Real CLI/registered World, Host, Coordinator, PrivateStore, Gateway and C0
collector. Only Gateway.transport supplies synthetic external model outputs.
Every test owns a fresh world and one cumulative wholemethod 120s. Corruptions
are ordinary SQL against that owned fixture AFTER the real operation exits.
Immutable snapshot UPDATE/DELETE triggers are never removed: state faults are
explicit damaged append-only snapshots, not claimed legitimate world actions.
No live model/SDK/network/account evidence; no SOURCE_PASS or runtime claim.
"""
import copy
import hashlib
import io
import json
from contextlib import closing, contextmanager, ExitStack, redirect_stdout
from pathlib import Path
import sqlite3
import time
import unittest
import uuid
from unittest.mock import patch

from app import product_cli
from app import coordinator_scheduler as scheduler
from app import world as world_module
from app.actors import ActorHost
from app.actor_execution import PrivateStore, Attempt
from app.adjudication import Adjudicator
from app.real_provider import RealGateway
from app.real_settings import SettingsStore
from app.server import Coordinator
from app.world import WorldStore, WorldError

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
KING = 'person-country-1-king'
OLD = 'person-country-1-finance'
MARSHAL = 'person-country-1-marshal'
WEAPON = 'personal-iron-country-1-king'
DOCUMENT = '机构公文1101：本季账目仅以此指定正文交阅。'
SECRET = '前任未传出的私密1101：封缄账簿与个人疑虑，绝不随职位复制。'
WRITE = '我本人指定写入公文：“' + DOCUMENT + '”。另记私人思考：' + SECRET + '。我本人另向君主寄信，请稍后核查1101。'
WAKE = '我本人选择向自己寄送稍后核查消息。'
STRIKE = '我本人现在选择用实持铁器近身攻击原财政官，不替对方作任何决定。'
WAIT = '我本人选择等待真实后续消息。'
MEETING = '我只向现场访客说明：请自行判断这份建议。'

# Fixed explicit domains, not arbitrary table-name discovery or COUNT oracles.
MAIN = ('snapshots', 'world_constants', 'scheduler_genesis', 'intents',
        'coordinator_scheduler_registration', 'coordinator_windows',
        'coordinator_activations', 'actor_coordinator_delegations',
        'actor_activation_delegations', 'coordinator_activation_outcomes',
        'coordinator_activation_blocks', 'coordinator_fault_issuers',
        'coordinator_technical_maintenance', 'coordinator_technical_window_releases',
        'dm_proposals', 'dm_decisions', 'dm_timeline', 'execution_clock',
        'actor_execution_leases', 'coordinator_execution_leases',
        'history_proof_epochs', 'history_proof_coverage')
PRIVATE = ('actor_world_binding', 'actor_sessions', 'actor_intents', 'actor_calls',
           'actor_session_heads', 'actor_intent_execution',
           'history_proof_epochs', 'history_proof_coverage')


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False)


def sha(value):
    return hashlib.sha256(encoded(value).encode('utf-8')).hexdigest()


class LifecycleHistoryProductTests(unittest.TestCase):
    """Independent fixtures; no imports from sibling in-flight test modules."""

    def setUp(self):
        self.started = time.monotonic()
        self.deadline = self.started + 120
        self.root = SCRATCH / ('parent1101-history-owned-' + uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.session = None
        self.generation = None
        self.extra_hosts = []
        self.calls = []
        self.evidence = {}
        self.violations = []
        self.guards = ExitStack()
        self.addCleanup(self.guards.close)
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'socket.create_connection', 'subprocess.Popen',
                       'http.client.HTTPConnection.request',
                       'app.real_settings.default_credential_factory',
                       'app.real_provider.default_credential_factory'):
            self.guards.enter_context(patch(target, self.forbidden))

    def forbidden(self, *args, **kwargs):
        self.violations.append('forbidden-external-or-scripted-operation')
        raise AssertionError('only synthetic Gateway.transport may supply outputs')

    def budget(self):
        self.assertLess(time.monotonic(), self.deadline,
                        'one wholemethod 120s; do not split consumers or renew')

    def build(self, pack='lifecycle-v1-iron-4.json'):
        self.budget()
        self.pack_name = pack
        path = product_cli.SCENARIOS / pack
        manifest = json.loads(product_cli.MANIFEST.read_bytes())
        raw = path.read_bytes()
        self.assertEqual(manifest['files']['app/data/scenarios/' + pack],
                         dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        self.config = json.loads(raw)
        if hasattr(self, 'object_case_id'):
            self.assertEqual(pack, 'lifecycle-v1-iron-4.json')
            objects = self.config['lifecycle']['personal_objects']
            objects[0]['id'] = self.object_case_id
            objects.append(dict(id='private-old-object-1106-history', kind='held_iron_weapon',
                holder_person_id=OLD,
                source_text='仅旧财政官本人实持的独立私人铁器1106-history，不随职位交付。'))
            raw = (encoded(self.config) + '\n').encode('utf-8')
            path = self.root / 'owned-object-genesis.json'
            path.write_bytes(raw)
        self.owned = self.root / 'world'
        stream = io.StringIO()
        with redirect_stdout(stream):
            result = product_cli.main(['--data-dir', str(self.owned), 'init', '--scenario', str(path)])
        self.assertEqual(result, 0)
        self.assertFalse(json.loads(stream.getvalue())['real_model'])
        self.assertEqual((self.owned / 'scenario.json').read_bytes(), raw)
        self.services = {kind:dict(provider='openai-chat-loopback',
            base_url='http://127.0.0.1:' + port + '/v1',
            model=kind + '-history1101-synthetic', credential_ref=None,
            timeout_seconds=2, max_retries=0)
            for kind, port in (('roles', '18181'), ('dm', '18182'))}
        settings = SettingsStore(self.owned / 'services.json', credential_factory=self.forbidden)
        settings.save(dict(services=self.services, secrets={}))
        self.assertEqual(settings.snapshot(), self.services)
        self.session = product_cli.ProductSession(self.owned, 'real')
        s = self.session
        for actual, expected in ((s, product_cli.ProductSession), (s.world, WorldStore),
                (s.host, ActorHost), (s.coordinator, Coordinator),
                (s.host._storage, PrivateStore), (s.host.adjudicator, Adjudicator),
                (s.host.real_gateway, RealGateway), (s.host.real_settings, SettingsStore)):
            self.assertIs(type(actual), expected)
        self.assertIs(s.host.world, s.world)
        self.assertIs(s.host._storage.world, s.world)
        self.assertIs(s.coordinator.actors, s.host)
        self.assertIs(s.host.adjudicator.world, s.world)
        self.assertIs(s.host._request.__func__, ActorHost._request)
        self.assertNotIn('_submit', s.host.__dict__)
        self.assertTrue(s.host.execution_fenced)
        for policy in (s.host.execution_policy, s.coordinator.execution_policy):
            self.assertEqual((policy.busy_budget_seconds, policy.act_deadline_seconds), (2, 120))
        s.host._choose = self.forbidden  # Deny sentinel; never selects a body.
        s.host.real_gateway.transport = self.transport
        self.generation = product_cli.verify_generation()
        self.genesis = self.raw(1)
        self.world_id = self.genesis['world_id']
        self.temporary = (self.config.get('event_wakeup', {}).get('temporary_persons') or [{}])[0].get('id')
        self.budget()

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
                self.assertNotIn(item['id'], encoded(messages))
                self.assertNotIn(item['source_text'], encoded(messages))
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

    def transport(self, service, messages, token, budget):
        """Sole external-output seam; actual consumers parse and persist replies."""
        budget.check()
        self.assertIsNone(token)
        payload = json.loads(messages[-1]['content'])
        if service['model'] == self.services['roles']['model']:
            if self.pack_name == 'lifecycle-v1-iron-4.json':
                self.assert_personal_object_input(payload, messages)
            else:
                self.assertNotIn('personal_objects', payload, 'legacy view shape must not grow')
            person = payload['person']['id']
            previous = [c for c in self.calls if c['kind'] == 'role' and c['person'] == person]
            attempt = None
            if self.pack_name == 'lifecycle-v1-iron-4.json':
                if person == OLD and not previous:
                    text = WRITE
                elif person == KING and not previous:
                    text = WAKE
                elif person == KING and payload['time'] == 60:
                    text = STRIKE
                    held = [v for v in payload['personal_objects'] if v['kind'] == 'held_iron_weapon']
                    self.assertEqual(len(held), 1, 'lawful role must choose from actual input')
                    attempt = dict(mode='strike_with_held_weapon', target_person_id=OLD, means_id=held[0]['id'])
                else:
                    text = WAIT + person
                content = encoded(dict(text=text, physical_attempt=attempt))
            else:
                text = MEETING if self.temporary and person == KING and not previous else WAIT + person
                content = text
            self.calls.append(dict(kind='role', person=person, text=text,
                payload=copy.deepcopy(payload), messages=copy.deepcopy(messages), content=content))
        elif payload.get('kind') == 'succession':
            slots = payload['unresolved_background_slots']
            self.assertEqual(len(slots), 1)
            self.assertEqual(payload['conflicts'], [])
            slot = slots[0]
            institution = next(f['record'] for f in payload['established_facts']
                if f['kind'] == 'institution' and 'slot-' + f['record']['id'] == slot['id'])
            response = dict(status='appointed', reason='依本次真实空缺及既成唯一代理惯例补背景，不复制私密。',
                basis_refs=[dict(kind='institution', id=institution['id'], at_revision=payload['base_revision'])],
                successor=dict(name='岚序1101', background_role=slot['background_role'], location=slot['place_id'],
                    card=dict(values='守持账目', interests='依机构惯例履职', risk='谨慎', mission='核清可实际查阅的材料')),
                unresolved=[])
            content = encoded(response)
            # Only limited fixture-owned references, no foreign frames or fake work.
            active = self.session.host._lifecycle_active
            self.assertIsNotNone(active)
            self.assertIs(active[0], self.session.coordinator._owner_attempt)
            self.assertIsNone(self.session.world._scoped_attempt())
            self.assertEqual(active[1].calls, [])
            self.calls.append(dict(kind='succession', payload=copy.deepcopy(payload),
                messages=copy.deepcopy(messages), response=copy.deepcopy(response)))
        else:
            proposal = payload['proposal']
            text = proposal['text']
            if text == WRITE:
                start = text.index(DOCUMENT)
                effects = [dict(action='write_office_document', text=DOCUMENT,
                                quote_start=start, quote_end=start + len(DOCUMENT)),
                           dict(action='send', to=KING, text='请稍后核查1101。')]
            elif text == WAKE:
                effects = [dict(action='send', to=KING, text='稍后由本人核查1101。')]
            elif text == STRIKE:
                protocol = payload['opt_in_effect_protocol']
                anchor = proposal['physical_attempt']
                role = [c for c in self.calls if c['kind'] == 'role' and c['person'] == KING][-1]
                self.assertEqual(protocol['physical_attempt'], anchor)
                self.assertEqual(anchor, json.loads(role['content'])['physical_attempt'])
                self.assertEqual(anchor['mode'], 'strike_with_held_weapon')
                self.assertEqual(anchor['target_person_id'], OLD)
                means = next(v for v in protocol['personal_objects'] if v['id'] == anchor['means_id'])
                self.assertEqual(means['holder_person_id'], proposal['person_id'])
                self.assertIn(means['id'], [v['id'] for v in role['payload']['personal_objects']])
                target = next(v for v in protocol['scene_persons'] if v['id'] == anchor['target_person_id'])
                self.assertEqual(target['id'], OLD)
                revision = payload['world']['revision']
                self.assertEqual(protocol['base_revision'], revision)
                effects = [dict(action='resolve_physical_attempt', outcome='mortal',
                    basis_refs=[dict(kind='personal_object', id=means['id'], at_revision=revision),
                                dict(kind='scene_person', id=target['id'], at_revision=revision)],
                    reason='只裁原本人近身锚的有限客观后果。')]
            elif text == MEETING:
                effects = [dict(action='meet', to=self.temporary, text='请自行判断这份建议。')]
            else:
                self.assertTrue(text.startswith(WAIT))
                effects = [dict(action='wait')]
            content = encoded(dict(ruling='成功', reason='只兑现原人物有限意图。',
                goal='原具证行动', costs=[], benefits=[], effects=effects))
            self.calls.append(dict(kind='dm', payload=copy.deepcopy(payload),
                messages=copy.deepcopy(messages), effects=copy.deepcopy(effects)))
        budget.check()
        return dict(choices=[dict(message=dict(content=content))],
                    usage=dict(prompt_tokens=17, completion_tokens=9))

    @contextmanager
    def readonly(self):
        """Actual native connection and original source gate; no Row adapter."""
        w = self.session.world
        with scheduler.source_lock(w), w._connection(readonly=True) as db:
            db.execute('ATTACH DATABASE ? AS actor', ((self.owned / 'actors.sqlite').as_uri() + '?mode=ro',))
            db.execute('BEGIN')
            db.row_factory = sqlite3.Row
            yield db

    def raw(self, seq=None):
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            sql = ('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1' if seq is None
                   else 'SELECT state FROM snapshots WHERE seq=?')
            row = db.execute(sql, () if seq is None else (seq,)).fetchone()
            self.assertIsNotNone(row)
            return json.loads(row[0])

    def rows(self, table, private=False):
        self.assertIn(table, PRIVATE if private else MAIN)
        path = self.owned / ('actors.sqlite' if private else 'world.sqlite')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            present = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
            if present is None:
                return None  # Explicit absence, not fabricated empty proof.
            return [dict(r) for r in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]

    def oracle(self):
        result = {}
        for label, names, private in (('main', MAIN, False), ('actor', PRIVATE, True)):
            path = self.owned / ('actors.sqlite' if private else 'world.sqlite')
            with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
                db.row_factory = sqlite3.Row
                db.execute('BEGIN')
                catalog = [dict(r) for r in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
                present = {r['name'] for r in catalog if r['type'] == 'table'}
                tables = {name:([dict(r) for r in db.execute('SELECT * FROM ' + name + ' ORDER BY rowid')]
                               if name in present else None) for name in names}
                result[label] = dict(catalog=catalog, tables=tables)
        result['provider_calls'] = copy.deepcopy(self.calls)
        return result

    def unchanged(self, before):
        """All typed rows exact, except original live root heartbeat time fields."""
        after = self.oracle()
        expected = copy.deepcopy(before)
        for table, fields in (('execution_clock', ('high_water_wall_ns',)),
                ('coordinator_execution_leases', ('last_wall_ns', 'expires_wall_ns'))):
            old = expected['main']['tables'][table]
            new = after['main']['tables'][table]
            self.assertEqual(len(old), len(new))
            for a, b in zip(old, new):
                for field in fields:
                    self.assertGreaterEqual(b[field], a[field])
                    a[field] = b[field]
        self.assertEqual(after, expected)
        self.budget()

    def step(self):
        self.budget()
        state = self.session.step(60)
        self.assertIsNone(self.session.coordinator.error)
        self.assertEqual(state['status'], 'user_paused')
        self.assertFalse(self.session.coordinator.worker.is_alive())
        self.assertFalse(self.session.host.cleanup_pending)
        self.budget()
        return state

    @contextmanager
    def proof_entry(self):
        """One real preparation Entry; nested materials cannot renew its owner."""
        c, w = self.session.coordinator, self.session.world
        captured = c._owner_attempt
        with c._owner_scope(captured), c._preparation_scope(captured, check_exit=False):
            entry = w._scoped_entry_budget()
            with scheduler._lifecycle_operation_scope(entry=entry) as operation:
                self.assertIs(entry._lifecycle_value_owner, operation)
                yield entry

    @contextmanager
    def material(self, *, entry=None, proof=None):
        """Real V3 owner/borrow/result scopes; legacy keeps its original V2 wire."""
        c, w = self.session.coordinator, self.session.world
        with ExitStack() as stack:
            if entry is None:
                entry = stack.enter_context(self.proof_entry())
            else:
                self.assertIs(w._scoped_entry_budget(), entry)
                self.assertIs(w._scoped_coordinator_attempt(), c._owner_attempt)
            operation = (stack.enter_context(scheduler._lifecycle_operation_scope(entry=entry))
                         if proof is None else proof.owner)
            self.assertIs(entry._lifecycle_value_owner, operation if proof is None else proof.parent)
            if proof is not None:
                self.assertIs(scheduler._lc_ro1127_context.get(), proof)
            if proof is None:
                with scheduler.source_lock(w):
                    capsule = scheduler._C0Selection(w, c._owner_attempt.permit, entry,
                                                     _host=self.session.host)
            else:
                capsule = proof.capsule
            data = scheduler._c0_collect(capsule)
            owner = scheduler._c0_v3_material(data)
            with scheduler._c0_v3_material_scope(data):
                if owner is None:
                    self.assertNotIn('_lifecycle', self.genesis)
                    self.assertTrue(data['pages'])
                    for rows in data['tables'].values():
                        self.assertIs(type(rows), tuple)
                        for row in rows:
                            self.assertIs(type(row), sqlite3.Row)
                else:
                    self.assertIn('_lifecycle', self.genesis)
                    self.assertIs(owner.memory, operation.memory)
                    self.assertIs(owner.material, data)
                    self.assertIs(owner.capsule, capsule)
                    self.assertIs(owner.entry, entry)
                    self.assertIs(owner.root, c._owner_attempt)
                    self.assertIs(owner.source_generation, capsule.source_generation)
                    self.assertIs(owner.vector, capsule.vector)
                    self.assert_material_census(capsule, data)
                yield capsule, data
                self.budget()
            if owner is not None:
                self.assertTrue(owner.closed)
                self.assertFalse(owner.failed)
                self.assertEqual(owner.exports, [])  # transferred, never manually cleared
                self.assertEqual(owner.leases, [])
                self.assertEqual(owner.tables, {})
                self.assertEqual(owner.coverage, {})
                self.assertIsNone(owner.capsule)
                self.assertIsNone(owner.entry)
                self.assertIsNone(owner.root)
                self.assertIsNone(owner.vector)
                self.assertFalse(operation.closed)
                self.assertFalse(operation.failed)
                self.assertIs(operation.memory, entry._lifecycle_value_owner.memory)
                if proof is not None:
                    self.assertIs(operation, proof.owner)

    def assert_borrowed_rows(self, data, table, *, nonempty=False):
        owner = scheduler._c0_v3_material(data)
        iterator = scheduler._c0_iter_borrows(data, table)
        count = 0
        try:
            for loan in iterator:
                try:
                    self.assertIs(type(loan.value), sqlite3.Row)
                    original = loan.value
                    self.assertIs(loan.value, original)  # within THIS genuine borrow
                    if owner is not None:
                        self.assertIs(loan.owner, owner)
                        self.assertIs(loan.owner.material, data)
                    original = None  # no Row alias survives a successful close
                    count += 1
                except BaseException:
                    loan.fail()
                    raise
                loan.close()
                self.assertTrue(loan.closed)
                self.assertIsNone(loan.value)
        finally:
            if not hasattr(iterator, 'previous') or iterator.previous is None or iterator.previous.closed:
                iterator.close()
        if nonempty:
            self.assertGreater(count, 0, 'actual producer dependency must be nonempty: ' + table)
        return count

    def assert_material_census(self, capsule, data):
        owner = scheduler._c0_v3_material(data)
        self.assertIs(type(owner), scheduler._C0V3MaterialOwner)
        self.assertEqual(data['version'], 'C0-V3-bounded-native-row')
        expected = dict(scheduler._C0_KEYS)
        expected.update({'main.sqlite_master': ('type', 'name'),
                         'actor.sqlite_master': ('type', 'name')})
        self.assertEqual(set(data['coverage']), set(expected))
        present = {schema + '.' + name
                   for schema, part in zip(('main', 'actor'), capsule.vector)
                   for name, ddl in part[2] if ddl is not None}
        for table, keys in expected.items():
            covered = data['coverage'][table]
            self.assertIs(type(covered), tuple)
            self.assertEqual(len(covered), 2)
            self.assertIs(type(covered[1]), int)
            if table not in present and not table.endswith('.sqlite_master'):
                self.assertEqual(covered, ('absent', 0))
                self.assertNotIn(table, data['tables'])
                continue
            self.assertIn(table, data['tables'])
            directory = data['tables'][table]
            self.assertIs(type(directory), scheduler._C0V3Directory)
            self.assertIs(directory.owner, owner)
            self.assertEqual(directory.table, table)
            self.assertEqual(directory.keys, keys)
            self.assertTrue(directory.terminal)
            self.assertFalse(directory.absent)
            self.assertEqual(covered, ('terminal-empty-page', len(directory.records)))
            # Re-read every existing original Row through the genuine provenance
            # consumer, not a descriptor -> dict or an invented Row constructor.
            self.assertEqual(self.assert_borrowed_rows(data, table), covered[1])
        self.assertEqual(set(data['tables']),
                         {table for table, (kind, _) in data['coverage'].items()
                          if kind == 'terminal-empty-page'})
        self.assertIs(owner.vector, capsule.vector)
        self.assertIs(owner.source_generation, capsule.source_generation)
        self.assertFalse(owner.closed)
        self.assertFalse(owner.failed)

    def seal(self, capsule):
        # Same original terminal native generation/source gate used by C0.
        with scheduler._c0_segment(capsule, scheduler._C0Fresh(capsule)) as db:
            capsule.pin(db)
            capsule.check_source()

    def assert_recovery(self, operation, codes, required_code, leaf_code=None):
        """Keep the actual primary tuple before any fallible deep-leaf assertion."""
        try:
            operation()
        except WorldError as error:
            first_tb = BaseException.__traceback__.__get__(error)
            first_context = BaseException.__context__.__get__(error)
            first_cause = BaseException.__cause__.__get__(error)
            self.__dict__.setdefault('_retained_recovery_errors', []).append(
                (error, first_tb, first_context, first_cause))
            self.assertIn(error.code, codes)
            frames = []
            tb = first_tb
            while tb is not None:
                frames.append(tb.tb_frame.f_code)
                tb = tb.tb_next
            self.assertIn(required_code, frames)
            if leaf_code is not None:
                self.assertIs(frames[-1], leaf_code)
            self.evidence.setdefault('deep_rejections', []).append(dict(code=error.code,
                required=required_code.co_qualname, leaf=frames[-1].co_qualname))
            return error
        self.fail('damaged owned fixture was accepted by the original deep consumer')

    def positive(self):
        windows = self.rows('coordinator_windows')
        self.assertTrue(windows)
        self.assertTrue(self.rows('intents'))
        self.assertTrue(self.rows('actor_intents', True))
        with self.readonly() as db:
            scheduler.registration(db, self.world_id, scheduler.source(self.session.world)[1])
            native = {}
            for win in windows:
                native[win['window_id']] = scheduler.window_record(db, self.world_id, win['window_id'])
                for row in db.execute('SELECT * FROM coordinator_activations WHERE window_id=?', (win['window_id'],)):
                    scheduler.native_audit(self.session.host._storage, db, row)
        with self.material() as (capsule, data):
            def consume():
                for win in windows:
                    wid = win['window_id']
                    self.assertEqual(scheduler._c0_window_record_uncached(data, self.world_id, wid), native[wid])
                    self.assertTrue(scheduler._c0_fullproof(self.session.world, capsule.storage, data, wid))
                self.seal(capsule)
            # The test comparison is a real consumer frame. Its temporaries end
            # before invoke retires local loans; all actual product proofs above
            # are unchanged. No synthetic authority/read/Row replacement.
            scheduler._c0_v3_invoke(data, consume)
        self.evidence['positive_window_ids'] = [w['window_id'] for w in windows]
        self.budget()

    def lifecycle(self, pending_new=False):
        self.build()
        self.assertEqual(self.genesis['_config']['horizon_days'], 90)
        self.assertEqual(self.genesis['_p04']['resident_tick_seconds'], 60)
        self.assertEqual(self.step()['time'], 0)
        initial_windows = self.rows('coordinator_windows')
        self.assertEqual(len(initial_windows), 1)
        old_rows = [r for r in self.rows('actor_intents', True) if r['person_id'] == OLD]
        self.assertEqual(len(old_rows), 1)
        old = old_rows[0]
        self.assertEqual(old['state'], 'committed')
        self.assertIn(SECRET, old['candidate'])
        doc = self.raw()['_lifecycle']['documents']
        self.assertEqual(len(doc), 1)
        self.assertEqual(doc[0]['text'], DOCUMENT)
        self.assertNotIn(SECRET, encoded(doc))
        self.assertEqual(doc[0]['cause_ref']['parent_intent_id'], old['intent_id'])
        self.assertEqual(self.step()['time'], 60)
        injured = self.raw()
        self.injury = copy.deepcopy(next(t for t in injured['_lifecycle']['transitions'] if t['kind'] == 'mortal_injury'))
        self.assertEqual(injured['_lifecycle']['deaths'], [])
        self.assertEqual(self.injury['person_id'], OLD)
        self.assertEqual(self.injury['node']['at'], 61)
        self.assertIn(self.injury['node'], injured['queue'])
        self.assertTrue(self.injury['basis_refs'])
        public = {r['id']:r for r in self.rows('intents')}
        cause = self.injury['cause_ref']
        self.assertEqual(json.loads(public[cause['parent_intent_id']]['receipt'])['decision_version'], cause['decision_version'])
        self.assertEqual(json.loads(public[cause['child_intent_id']]['receipt'])['effect']['injury_id'], self.injury['id'])
        self.assertEqual(self.step()['time'], 61)
        state = self.raw()
        life = state['_lifecycle']
        self.assertEqual(len(life['deaths']), 1)
        self.death = copy.deepcopy(life['deaths'][0])
        self.assertEqual((self.death['person_id'], self.death['injury_id'], self.death['node_id']),
                         (OLD, self.injury['id'], self.injury['node']['id']))
        self.assertEqual(self.death['source_snapshot_revision'], self.injury['committed_revision'])
        jobs = life['jobs']
        self.assertEqual(len(jobs), 1)
        job = jobs[0]
        self.assertEqual(job['result']['status'], 'appointed')
        self.assertEqual(len(job['attempts']), 1)
        attempt = job['attempts'][0]
        self.assertEqual(attempt['status'], 'committed')
        self.assertEqual(attempt['request_sha256'], sha(attempt['request']))
        self.new = job['result']['person_id']
        self.assertNotEqual(self.new, OLD)
        self.assertRegex(self.new, r'^person-[0-9a-f]{32}$')
        self.assertEqual(self.new, attempt['request']['proposed_person_id'])
        self.assertEqual(life['person_origins'][self.new]['source_ref']['job_id'], job['id'])
        tenures = {t['person_id']:t for t in life['tenures']}
        self.assertEqual(tenures[OLD]['to_revision'], self.death['committed_revision'])
        self.assertEqual(tenures[self.new]['office_id'], tenures[OLD]['office_id'])
        self.assertIsNone(tenures[self.new]['to_revision'])
        self.assertEqual([c['payload']['request_id'] for c in self.calls if c['kind'] == 'succession'], [attempt['request_id']])
        self.assertFalse([c for c in self.calls if c['kind'] == 'role' and c['person'] == self.new])
        self.assertEqual(state['_observations'][self.new], [])
        self.assertEqual(state['_messages'][self.new], [])
        self.assertFalse([r for r in self.rows('actor_intents', True) if r['person_id'] == self.new])
        heads = [r for r in self.rows('actor_session_heads', True) if r['person_id'] == self.new]
        self.assertTrue(not heads or (len(heads) == 1 and heads[0]['version'] == 0))
        self.old_window = initial_windows[0]['window_id']
        self.evidence['chain'] = dict(document=doc[0], injury=self.injury, death=self.death,
            job=copy.deepcopy(job), new=self.new, old_intent=old['intent_id'])
        if pending_new:
            c, w = self.session.coordinator, self.session.world
            with c._owner_scope(), c._preparation_scope(c._owner_attempt):
                w.control('resume')
                current = w.snapshot()
                opened = w.open_or_recover_window(coordinator=c._owner_attempt.permit,
                    expected_version={k:current[k] for k in ('world_id', 'revision')})
            self.assertIn(self.new, opened['roster'])
            self.new_window = opened['window_id']
            active = self.rows('coordinator_activations')
            new_rows = [r for r in active if r['window_id'] == self.new_window and r['person_id'] == self.new]
            self.assertEqual(len(new_rows), 1)
            self.assertEqual(new_rows[0]['phase'], 'pending')
            self.assertIsNone(new_rows[0]['intent_id'])
            self.new_activation = new_rows[0]
        self.positive()  # Never inject a fault unless both complete consumers succeed.

    def idle(self):
        self.assertFalse(self.session.coordinator.busy)
        worker = self.session.coordinator.worker
        self.assertTrue(worker is None or not worker.is_alive())
        self.assertIsNone(self.session.host._attempt)
        self.assertIsNone(self.session.host._lifecycle_active)
        self.assertFalse(self.session.host.cleanup_pending)

    def append_fault(self, mutate):
        self.idle()
        before = self.raw()
        damaged = copy.deepcopy(before)
        damaged['revision'] += 1
        mutate(damaged)
        # Essential separation: fault MUST pass the original typed/shape gate.
        WorldStore._check_frozen_p04(damaged, self.genesis)
        self.assertIsNotNone(world_module._lifecycle_shape(damaged, self.genesis))
        self.assertNotEqual(damaged['_lifecycle'], before['_lifecycle'])
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            db.execute('INSERT INTO snapshots(seq,state) VALUES (?,?)', (damaged['revision'], encoded(damaged)))
            db.commit()
        self.assertEqual(self.raw(), damaged)
        self.evidence['post_injection'] = self.oracle()
        return damaged

    def dual_state_reject(self):
        baseline = self.oracle()
        calls = copy.deepcopy(self.calls)
        with self.proof_entry() as entry:
            self.ro1127_reject(entry,
                lambda proof:scheduler._lc_ro1132_invoke(proof, 'current'),
                {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_project.__code__)
            self.unchanged(baseline)
            self.ro1127_reject(entry,
                lambda proof:scheduler._lc_ro1132_invoke(proof, 'window', self.old_window),
                {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_project.__code__)
            self.unchanged(baseline)
            self.ro1127_reject(entry,
                lambda capsule,data:scheduler._p04_material_current(data, scheduler._c0_latest(data)),
                {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_project.__code__, material=True)
            self.unchanged(baseline)
            self.ro1127_reject(entry,
                lambda capsule,data:scheduler._c0_window_record_uncached(data, self.world_id, self.old_window),
                {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_project.__code__, material=True)
            self.unchanged(baseline)
        self.assertEqual(self.calls, calls)
    def test_head_zero_copied_nonempty_old_session_rejected_by_both_spines(self):
        self.lifecycle(pending_new=True)
        store = self.session.host._storage
        w, c = self.session.world, self.session.coordinator
        root = c._owner_attempt
        ref = scheduler.ActivationRef(**{k:self.new_activation[k] for k in
            ('world_id', 'window_id', 'activation_id', 'person_id')})
        # Real original acquisition/adopt creates NEW's empty version-zero head
        # WITHOUT producing a role request or any invented committed history.
        with c._owner_scope(root), c._preparation_scope(root):
            permit = w.acquire_actor_execution(self.new, 'history1101-positive-empty-adoption',
                coordinator=root.permit, activation=ref)
            try:
                attempt = Attempt(permit, self.session.host.execution_policy,
                    coordinator=root.permit, parent_attempt=root, activation=ref)
                with w._actor_attempt(attempt), self.session.host._with_entry_pipeline():
                    with store.transaction(permit, self.new, attempt=attempt) as db:
                        store.adopt(db, self.new, permit, attempt.id)
                        self.assertEqual(store.head(db, self.new)['version'], 0)
                        session = db.execute('SELECT messages,seen FROM actor.actor_sessions WHERE person_id=?',
                                             (self.new,)).fetchone()
                        self.assertEqual(tuple(session), ('[]', '[]'))
            finally:
                released = w.release_actor_execution(permit)
                self.evidence['positive_NEW_release_CAS'] = released
                self.assertIs(released, True)
        # Empty NEW is independently accepted by the two complete consumers.
        with self.readonly() as db:
            row = db.execute('SELECT * FROM coordinator_activations WHERE activation_id=?',
                             (self.new_activation['activation_id'],)).fetchone()
            scheduler.native_audit(store, db, row)
        with self.material() as (capsule, data):
            row = scheduler._c0_read(data, 'main.coordinator_activations',
                (('activation_id', self.new_activation['activation_id']),), first=True)
            scheduler._c0_native_audit(store, data, row)
        self.idle()
        old_session = next(r for r in self.rows('actor_sessions', True) if r['person_id'] == OLD)
        self.assertIn(SECRET, old_session['messages'])
        old_head = next(r for r in self.rows('actor_session_heads', True) if r['person_id'] == OLD)
        self.assertGreater(old_head['version'], 0)
        # Deliberate valid-typed SQL attack: NEW head0 with synchronized hashes,
        # but no NEW actor_intents/meta/calls and therefore no committed spine.
        with closing(sqlite3.connect(self.owned / 'actors.sqlite')) as db:
            self.assertEqual(db.execute('UPDATE actor_sessions SET messages=?,seen=? WHERE person_id=?',
                (old_session['messages'], old_session['seen'], self.new)).rowcount, 1)
            self.assertEqual(db.execute('UPDATE actor_session_heads SET messages_sha256=?,seen_sha256=? '
                'WHERE person_id=? AND version=0 AND last_promoted_intent_id IS NULL',
                (sha(json.loads(old_session['messages'])), sha(json.loads(old_session['seen'])), self.new)).rowcount, 1)
            db.commit()
        post = self.oracle()
        self.evidence['post_injection'] = post
        calls_before = copy.deepcopy(self.calls)
        with c._owner_scope(root), c._preparation_scope(root):
            self.assert_recovery(lambda:w.acquire_actor_execution(self.new,
                'history1101-corrupt-acquisition', coordinator=root.permit, activation=ref),
                {'ACTOR_RECOVERY_UNPROVEN'}, PrivateStore._audit.__code__, PrivateStore._audit.__code__)
        self.unchanged(post)
        with self.proof_entry() as entry:
            def native_direct(proof):
                # The real head hash check and native empty-row query are now
                # before-return paid consumers, not unowned test DB allocations.
                self.assertEqual(scheduler._lc_ro1132_invoke(proof, 'head0_empty', self.new), (0, True))
                scheduler._lc_ro1132_invoke(proof, 'audit', self.new)
            self.ro1127_reject(entry, native_direct, {'ACTOR_RECOVERY_UNPROVEN'},
                PrivateStore._audit.__code__, PrivateStore._audit.__code__)
            self.unchanged(post)
            def native_activation(proof):
                scheduler._lc_ro1132_invoke(proof, 'activation', self.new_activation['activation_id'])
            self.ro1127_reject(entry, native_activation, {'ACTOR_RECOVERY_UNPROVEN'},
                PrivateStore._audit.__code__, PrivateStore._audit.__code__)
            self.unchanged(post)
            def direct(capsule, data):
                self.assertEqual(store._material_head(data, self.new)['version'], 0)
                store._material_audit(data, self.new)
            self.ro1127_reject(entry, direct, {'ACTOR_RECOVERY_UNPROVEN'},
                PrivateStore._material_audit_rules_v3.__code__, PrivateStore._material_audit_rules_v3.__code__, material=True)
            self.unchanged(post)
            self.ro1127_reject(entry,
                lambda capsule,data:scheduler._c0_fullproof(self.session.world, store, data, self.new_window),
                {'ACTOR_RECOVERY_UNPROVEN'}, PrivateStore._material_audit_rules_v3.__code__, material=True)
        self.unchanged(post)
        self.assertEqual(self.calls, calls_before)

    def test_synchronized_person_origin_tenure_without_real_job_is_rejected(self):
        self.lifecycle()
        forged = 'person-' + uuid.uuid4().hex
        def mutate(s):
            life = s['_lifecycle']
            origin = copy.deepcopy(life['person_origins'][self.new])
            person = copy.deepcopy(next(p for p in s['persons'] if p['id'] == self.new))
            person.update(id=forged, name='无真实原job的冒名者1101')
            origin['identity'].update(id=forged, name=person['name'])
            origin['introduced_revision'] = s['revision']
            origin['source_ref'].update(request_id=uuid.uuid4().hex, committed_revision=s['revision'])
            life['person_origins'][forged] = origin
            s['persons'].append(person)
            s['_observations'][forged] = []
            s['_messages'][forged] = []
            s['_p04']['resident_last_dispatch'][forged] = -1
            s['_p04']['pending'].append(forged)
            predecessor = next(t for t in life['tenures'] if t['person_id'] == self.new)
            predecessor['to_revision'] = s['revision']
            tenure = copy.deepcopy(predecessor)
            tenure.update(id='tenure-' + person['office_id'] + ':' + forged + ':' + str(s['revision']),
                person_id=forged, from_revision=s['revision'], to_revision=None,
                source_ref=copy.deepcopy(origin['source_ref']))
            life['tenures'].append(tenure)
            self.assertFalse([j for j in life['jobs'] if j['result'] and j['result']['person_id'] == forged])
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_historical_activation_cannot_be_bound_to_current_new_person(self):
        self.lifecycle()
        original = next(r for r in self.rows('coordinator_activations')
                        if r['window_id'] == self.old_window and r['person_id'] == MARSHAL)
        self.assertEqual(original['phase'], 'completed')
        self.assertIsNotNone(original['observation_revision'])
        self.assertNotIn(self.new, self.raw(original['observation_revision'])['_lifecycle']['person_origins'])
        self.idle()
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            self.assertEqual(db.execute('UPDATE coordinator_activations SET person_id=? WHERE activation_id=?',
                (self.new, original['activation_id'])).rowcount, 1)
            db.commit()
        baseline = self.oracle()
        self.evidence['post_injection'] = baseline
        with self.proof_entry() as entry:
            self.ro1127_reject(entry,
                lambda proof:scheduler._lc_ro1132_invoke(proof, 'window', self.old_window),
                {'WINDOW_RECOVERY_UNPROVEN'}, scheduler._lifecycle_activation_identity.__code__)
            self.unchanged(baseline)
            self.ro1127_reject(entry,
                lambda capsule,data:scheduler._c0_window_record_uncached(data, self.world_id, self.old_window),
                {'WINDOW_RECOVERY_UNPROVEN'}, scheduler._lifecycle_activation_identity.__code__, material=True)
        self.unchanged(baseline)

    def test_historical_roster_cannot_import_new_person_with_matching_hash(self):
        self.lifecycle()
        win = next(w for w in self.rows('coordinator_windows') if w['window_id'] == self.old_window)
        roster = json.loads(win['roster_json'])
        self.assertIn(MARSHAL, roster)
        self.assertNotIn(self.new, roster)
        roster[roster.index(MARSHAL)] = self.new
        self.idle()
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            db.execute('UPDATE coordinator_windows SET roster_json=?,roster_sha256=? WHERE window_id=?',
                       (encoded(roster), sha(roster), self.old_window))
            db.commit()
        baseline = self.oracle()
        self.evidence['post_injection'] = baseline
        self.assertEqual(sha(roster), self.rows('coordinator_windows')[0]['roster_sha256'])
        with self.proof_entry() as entry:
            self.ro1127_reject(entry,
                lambda proof:scheduler._lc_ro1132_invoke(proof, 'window', self.old_window),
                {'INVALID_EVENT_WAKEUP'}, scheduler._p04_current_projection.__code__)
            self.unchanged(baseline)
            self.ro1127_reject(entry,
                lambda capsule,data:scheduler._c0_window_record_uncached(data, self.world_id, self.old_window),
                {'INVALID_EVENT_WAKEUP'}, scheduler._p04_current_projection.__code__, material=True)
        self.unchanged(baseline)

    def test_deleting_actual_dead_person_and_synchronized_maps_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            life = s['_lifecycle']
            s['persons'] = [p for p in s['persons'] if p['id'] != OLD]
            del life['person_origins'][OLD]
            del s['_observations'][OLD]
            del s['_messages'][OLD]
            del s['_p04']['resident_last_dispatch'][OLD]
            for key in ('pending', 'dispatch'):
                s['_p04'][key] = [p for p in s['_p04'][key] if p != OLD]
            life['tenures'] = [t for t in life['tenures'] if t['person_id'] != OLD]
            life['deaths'] = []  # synchronized shape, but the actual history cannot be erased.
            self.assertTrue(life['jobs'])
            self.assertTrue(life['transitions'])
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_original_country_drift_with_synchronized_origin_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            next(p for p in s['persons'] if p['id'] == OLD)['country_id'] = 'country-2'
            s['_lifecycle']['person_origins'][OLD]['identity']['country_id'] = 'country-2'
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_original_office_drift_with_synchronized_origin_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            next(p for p in s['persons'] if p['id'] == OLD)['office'] = 'marshal'
            s['_lifecycle']['person_origins'][OLD]['identity']['office'] = 'marshal'
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_removing_real_death_to_resurrect_person_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            self.assertEqual(s['_lifecycle']['deaths'], [self.death])
            s['_lifecycle']['deaths'] = []
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_reopening_real_closed_tenure_is_rejected_without_parallel_shape_error(self):
        self.lifecycle()
        def mutate(s):
            old = next(t for t in s['_lifecycle']['tenures'] if t['person_id'] == OLD)
            new = next(t for t in s['_lifecycle']['tenures'] if t['person_id'] == self.new)
            self.assertEqual(old['to_revision'], self.death['committed_revision'])
            new['to_revision'] = s['revision']
            old['to_revision'] = None  # exactly one open office, so no early overlap guard.
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_real_death_node_tamper_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            injury = next(t for t in s['_lifecycle']['transitions'] if t['id'] == self.injury['id'])
            injury['node']['at'] += 1
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_real_death_source_snapshot_tamper_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            death = s['_lifecycle']['deaths'][0]
            death['source_snapshot_revision'] += 1
            transition = next(t for t in s['_lifecycle']['transitions'] if t['kind'] == 'person_death')
            transition['source_snapshot_revision'] = death['source_snapshot_revision']
        self.append_fault(mutate)
        self.dual_state_reject()

    def test_real_mortal_basis_tamper_with_valid_existing_ref_is_rejected(self):
        self.lifecycle()
        def mutate(s):
            injury = next(t for t in s['_lifecycle']['transitions'] if t['id'] == self.injury['id'])
            target = next(r for r in injury['basis_refs'] if r['kind'] == 'scene_person')
            self.assertEqual(target['id'], OLD)
            target['id'] = MARSHAL  # existing genesis person, typed revision unchanged.
        self.append_fault(mutate)
        self.dual_state_reject()

    def assert_memo_dependencies(self, data):
        # V3 has no p04_rows/global Row-is memo. Check the SAME fifteen real
        # dependency producers and every genuine Row through active loans.
        names = ('main.snapshots', 'main.coordinator_windows', 'main.coordinator_activations',
                 'main.intents', 'main.dm_decisions', 'main.dm_proposals', 'main.dm_timeline',
                 'main.coordinator_activation_outcomes', 'main.coordinator_scheduler_registration',
                 'actor.actor_intents', 'actor.actor_intent_execution', 'actor.actor_calls',
                 'actor.actor_sessions', 'actor.actor_session_heads', 'actor.actor_world_binding')
        owner = scheduler._c0_v3_material(data)
        self.assertIs(type(owner), scheduler._C0V3MaterialOwner)
        for name in names:
            directory = data['tables'][name]
            self.assertIs(directory.owner, owner)
            self.assertTrue(directory.terminal)
            self.assertFalse(directory.absent)
            self.assertGreater(len(directory.records), 0)
            self.assertEqual(self.assert_borrowed_rows(data, name, nonempty=True),
                             data['coverage'][name][1])

    def memo_positive(self, data):
        owner = scheduler._c0_v3_material(data)
        generation, source_generation = owner.generation, owner.source_generation
        entry, root, vector = owner.entry, owner.root, owner.vector
        self.assertIsNone(scheduler._c0_query_context.get())
        with scheduler._c0_query_scope(data) as owned:
            self.assertIs(owned, data)
            self.assertIsNone(scheduler._c0_query_context.get())
            self.assert_memo_dependencies(owned)
            def repeat():
                state = scheduler._c0_latest(owned)
                before = owner.acquisitions
                scheduler._p04_material_current(owned, state)
                after = owner.acquisitions
                self.assertGreater(after, before)
                scheduler._p04_material_current(owned, state)
                self.assertGreater(owner.acquisitions, after,
                                   'V3 repeat must reacquire real dependencies, not reuse proof success')
            scheduler._c0_v3_invoke(owned, repeat)
            self.assertIs(owner.generation, generation)
            self.assertIs(owner.source_generation, source_generation)
            self.assertIs(owner.entry, entry)
            self.assertIs(owner.root, root)
            self.assertIs(owner.vector, vector)
            self.assertFalse(owner.closed)
            self.assertFalse(owner.failed)
        self.assertIsNone(scheduler._c0_query_context.get())
        self.assertIsNone(scheduler._c0_v3_frame_context.get())
        self.assertEqual(owner.exports, [])

    def test_material_memo_rechecks_cancel_and_clears_scope(self):
        self.lifecycle()
        observed = {}
        def reject():
            with self.material() as (capsule, data):
                self.memo_positive(data)
                observed['owner'] = scheduler._c0_v3_material(data)
                observed['baseline'] = self.oracle()
                # The original root Event, never a fake check or a cleared stop.
                def cancelled_consumer():
                    state = scheduler._c0_latest(data)
                    capsule.root.stop.set()
                    scheduler._p04_material_current(data, state)
                # The original exception crosses BOTH real scopes before catch;
                # poison is retained, not falsely normal-closed or reused.
                scheduler._c0_v3_invoke(data, cancelled_consumer)
        self.assert_recovery(reject, {'COORDINATOR_EXECUTION_CANCELLED'},
                             scheduler._C0Selection.check.__code__)
        self.unchanged(observed['baseline'])
        self.assertTrue(observed['owner'].failed)
        self.assertTrue(observed['owner'].memory.failed)
        self.assertFalse(observed['owner'].closed)
        self.assertIsNone(scheduler._c0_query_context.get())
        self.assertIsNone(scheduler._c0_v3_frame_context.get())
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.assertIsNone(scheduler._lifecycle_live_context.get())

    def test_material_memo_new_source_is_rejected_by_original_source_seal(self):
        self.lifecycle()
        observed = {}
        def reject():
            with self.material() as (capsule, data):
                self.memo_positive(data)
                owner = scheduler._c0_v3_material(data)
                observed.update(owner=owner, capsule=capsule)
                # New REAL registered same-file Host, not a fake source/dict.
                extra = ActorHost(self.session.world, self.owned / 'actors.sqlite')
                self.extra_hosts.append(extra)
                observed['baseline'] = self.oracle()
                self.seal(capsule)
        self.assert_recovery(reject, {'WINDOW_RECOVERY_UNPROVEN'},
                             scheduler._C0Selection.check_source.__code__)
        self.unchanged(observed['baseline'])
        owner = observed['owner']
        self.assertIs(owner.capsule, observed['capsule'])
        self.assertTrue(owner.failed)
        self.assertTrue(owner.memory.failed)
        self.assertFalse(owner.closed)
        self.assertIsNone(scheduler._c0_query_context.get())
        self.assertIsNone(scheduler._c0_v3_frame_context.get())

    def test_material_memo_actual_new_dependency_generation_cannot_reuse_success(self):
        self.lifecycle()
        # One captured Entry and one operation ledger for this same-fault chain.
        # Independent read-only siblings preserve both original deep oracles;
        # the original Entry/ledger and all failed-owner debt remain shared.
        with self.proof_entry() as entry:
            with self.material(entry=entry) as (capsule, data):
                self.memo_positive(data)
                owner = scheduler._c0_v3_material(data)
                generation = owner.generation
                parent = self.injury['cause_ref']['parent_intent_id']
                self.idle()
                with closing(sqlite3.connect(self.owned / 'actors.sqlite')) as db:
                    row = db.execute('SELECT person_id,call_id FROM actor_calls WHERE intent_id=?', (parent,)).fetchone()
                    self.assertEqual(row, (KING, 'real-' + parent))
                    db.execute('UPDATE actor_calls SET person_id=? WHERE intent_id=?', (MARSHAL, parent))
                    db.commit()
                baseline = self.oracle()
                self.evidence['post_injection'] = baseline
                self.assert_recovery(lambda:self.seal(capsule), {'WINDOW_RECOVERY_UNPROVEN'},
                                     scheduler._C0Selection.pin.__code__)
                self.unchanged(baseline)
            self.assertTrue(owner.closed)
            def changed_consumer(fresh, changed):
                changed_owner = scheduler._c0_v3_material(changed)
                self.assertIsNot(fresh, capsule)
                self.assertIsNot(changed, data)
                self.assertIs(fresh.budget, entry)
                self.assertIs(changed_owner.memory, owner.memory)
                self.assertIsNot(changed_owner.generation, generation)
                scheduler._c0_fullproof(self.session.world, fresh.storage, changed, self.old_window)
            self.ro1127_reject(entry, changed_consumer, {'ACTOR_RECOVERY_UNPROVEN'},
                world_module._lc_child.__code__, material=True)
            self.unchanged(baseline)
            self.ro1127_reject(entry,
                lambda proof:scheduler._lc_ro1132_invoke(proof, 'current'),
                {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lc_child.__code__)
            self.unchanged(baseline)
        self.assertIsNone(scheduler._c0_query_context.get())

    def test_material_memo_other_entry_and_material_build_independent_real_proof(self):
        self.lifecycle()
        with self.material() as (first, one):
            self.memo_positive(one)
            first_owner = scheduler._c0_v3_material(one)
            first_entry = first.budget
            first_generation = first_owner.generation
            first_memory = first_owner.memory
        self.assertTrue(first_owner.closed)
        with self.material() as (second, two):
            second_owner = scheduler._c0_v3_material(two)
            self.assertIsNot(first_entry, second.budget)
            self.assertIsNot(one, two)
            self.assertIsNot(first_generation, second_owner.generation)
            self.assertIsNot(first_memory, second_owner.memory)
            self.assertFalse(second_owner.closed)
            self.assertFalse(second_owner.failed)
            self.memo_positive(two)
            self.assertTrue(scheduler._c0_fullproof(self.session.world, second.storage, two, self.old_window))
            self.seal(second)
        self.assertTrue(second_owner.closed)
        self.assertIsNone(scheduler._c0_query_context.get())
        self.assertIsNone(scheduler._c0_v3_frame_context.get())

    def legacy(self, no_p04=False):
        self.build('iron-4.json' if no_p04 else 'event-wakeup-v1-iron-4.json')
        self.assertNotIn('_lifecycle', self.genesis)
        self.step()
        if not no_p04:
            first = [c for c in self.calls if c['kind'] == 'role' and c['person'] == KING]
            self.assertEqual(first[0]['text'], MEETING)
            self.assertIn(self.temporary, self.raw()['_p04']['pending'])
            self.step()
            rows = [r for r in self.rows('actor_intents', True) if r['person_id'] == self.temporary]
            self.assertTrue(rows)
            q = json.loads(rows[0]['request'])
            self.assertEqual(q['schema_version'], 2)
            self.assertEqual(q['messages'], [])
            self.assertNotIn('identity_binding', q)
            self.assertIn('请自行判断这份建议。', q['prompt'])
            self.assertTrue([e for e in self.raw()['events'] if e['kind'] == 'meeting'])
            self.assertEqual(self.raw()['_p04']['resident_tick_seconds'], 60)
        for r in self.rows('actor_intents', True):
            self.assertEqual(json.loads(r['request'])['schema_version'], 2)
        self.assertEqual(self.session.world.pending_lifecycle_jobs(), [])
        self.assertIsNone(self.session.world.lifecycle_identity(KING))
        self.old_window = self.rows('coordinator_windows')[0]['window_id']
        self.positive()

    def legacy_fault(self, kind):
        self.legacy()
        self.idle()
        damaged = copy.deepcopy(self.raw())
        damaged['revision'] += 1
        if kind == 'person':
            person = copy.deepcopy(damaged['persons'][0])
            person.update(id='person-' + uuid.uuid4().hex, name='旧局无源新增人物1101')
            damaged['persons'].append(person)
            damaged['_observations'][person['id']] = []
            damaged['_messages'][person['id']] = []
            damaged['_p04']['resident_last_dispatch'][person['id']] = -1
        elif kind in ('country_id', 'office'):
            person = next(p for p in damaged['persons'] if p['id'] == OLD)
            person[kind] = 'country-2' if kind == 'country_id' else 'marshal'
        elif kind == 'lifecycle':
            declared = json.loads((product_cli.SCENARIOS / 'lifecycle-v1-iron-4.json').read_bytes())['lifecycle']
            damaged['_config']['lifecycle'] = declared
        elif kind == 'v2':
            damaged['_config']['event_wakeup']['version'] = 'p04-event-wakeup-v2-lifecycle'
            damaged['_p04']['rules']['version'] = 'p04-event-wakeup-v2-lifecycle'
        else:
            self.fail('unknown fault')
        # Original complete v1 state, valid original UUIDs and synchronized roster
        # ledger domains. The expected failure is precisely genesis, not stale hash.
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            db.execute('INSERT INTO snapshots VALUES (?,?)', (damaged['revision'], encoded(damaged)))
            db.commit()
        baseline = self.oracle()
        self.evidence['post_injection'] = baseline
        if kind in ('person', 'country_id', 'office'):
            self.assert_recovery(lambda:WorldStore._check_frozen_p04(damaged, self.genesis),
                {'FROZEN_EVENT_WAKEUP'}, WorldStore._check_frozen_p04.__code__)
            with self.readonly() as db:
                self.assert_recovery(lambda:scheduler.window_record(db, self.world_id, self.old_window),
                    {'WINDOW_RECOVERY_UNPROVEN'}, scheduler.window_record.__code__)
            with self.material() as (capsule, data):
                self.assert_recovery(lambda:scheduler._c0_window_record_uncached(data, self.world_id, self.old_window),
                    {'WINDOW_RECOVERY_UNPROVEN'}, scheduler._c0_window_record_uncached.__code__)
        else:
            self.assert_recovery(lambda:WorldStore._check_frozen_p04(damaged, self.genesis),
                {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_shape.__code__)
            with self.readonly() as db:
                self.assert_recovery(lambda:scheduler._p04_native_current(db, scheduler.latest(db)),
                    {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_shape.__code__)
            with self.material() as (capsule, data):
                self.assert_recovery(lambda:scheduler._p04_material_current(data, scheduler._c0_latest(data)),
                    {'ACTOR_RECOVERY_UNPROVEN'}, world_module._lifecycle_shape.__code__)
        self.unchanged(baseline)

    def test_v1_genesis_rejects_new_person_after_real_meeting_empty_session_controls(self):
        self.legacy_fault('person')

    def test_v1_genesis_rejects_original_country_drift_after_real_controls(self):
        self.legacy_fault('country_id')

    def test_v1_genesis_rejects_original_office_drift_after_real_controls(self):
        self.legacy_fault('office')

    def test_v1_genesis_rejects_late_lifecycle_declaration_after_real_controls(self):
        self.legacy_fault('lifecycle')

    def test_v1_genesis_rejects_late_v2_declaration_after_real_controls(self):
        self.legacy_fault('v2')

    def test_existing_v1_world_other_valid_scenario_is_mismatch_without_writes(self):
        self.legacy()
        self.idle()
        baseline = self.oracle()
        world = WorldStore(self.owned / 'world.sqlite',
            scenario_path=product_cli.SCENARIOS / 'lifecycle-v1-iron-4.json')
        self.assert_recovery(world.initialize, {'SCENARIO_MISMATCH'}, WorldStore.initialize.__code__)
        self.unchanged(baseline)
        self.assertEqual(self.session.world.initialize()['world_id'], self.world_id)
        self.unchanged(baseline)

    def test_no_p04_original_pack_does_not_acquire_lifecycle_capabilities(self):
        self.legacy(no_p04=True)
        baseline = self.oracle()
        self.assertNotIn('_p04', self.raw())
        self.assertNotIn('event_wakeup', self.raw()['_config'])
        for call in self.calls:
            if call['kind'] == 'dm':
                self.assertNotIn('opt_in_effect_protocol', call['payload'])
                self.assertIn('人物死亡或继任能力', call['messages'][0]['content'])
        effects = [dict(action='write_office_document', text=DOCUMENT, quote_start=0, quote_end=len(DOCUMENT)),
                   dict(action='read_office_document', document_id='office-document-unavailable1101'),
                   dict(action='resolve_physical_attempt', outcome='mortal',
                        basis_refs=[dict(kind='scene_person', id=OLD, at_revision=self.raw()['revision'])],
                        reason='旧版本无能力')]
        for effect in effects:
            self.assert_recovery(lambda:self.session.host.adjudicator._compile(dict(effects=[effect])),
                {'UNSUPPORTED'}, Adjudicator._compile.__code__)
        self.assertEqual(self.session.world.pending_lifecycle_jobs(), [])
        self.assertIsNone(self.session.world.lifecycle_identity(OLD))
        self.unchanged(baseline)

    def object_input_complete_case(self, object_id):
        self.object_case_id = object_id
        self.lifecycle()  # Complete original causal chain + both real consumers.
        self.assertEqual(self.injury['means_id'], object_id)
        self.assertEqual(self.genesis['_lifecycle']['rules']['personal_objects'][0]['id'], object_id)
        self.assertEqual(self.step()['time'], 61)
        calls = [c for c in self.calls if c['kind'] == 'role' and c['person'] == self.new]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['payload']['personal_objects'], [])
        self.assertNotIn('private-old-object-1106-history', encoded(calls[0]['messages']))
        self.assertEqual(self.session.world.observe(OLD, seq=1)['personal_objects'], [
            dict(id='private-old-object-1106-history', kind='held_iron_weapon',
                 description='仅旧财政官本人实持的独立私人铁器1106-history，不随职位交付。')])
        self.assertEqual(self.session.world.observe(self.new)['personal_objects'], [])
        self.positive()
        self.budget()

    def test_role_object_alpha_complete_history_and_new_privacy(self):
        self.object_input_complete_case('owned-independent-alpha-1106-history')

    def test_role_object_beta_complete_history_and_new_privacy(self):
        self.object_input_complete_case('owned-independent-beta-1106-history')

    def ro1127_reject(self, entry, consume, codes, required, leaf=None, *, material=False):
        """Catch AFTER the failed proof scope, while retaining the original Entry."""
        observed = {}
        original = entry._lifecycle_value_owner
        memory = original.memory
        prior = tuple((receipt, receipt.first, receipt.terminal)
                      for receipt in original.control_errors
                      if type(receipt) is scheduler._lc_ro1127_Proof and receipt.failed)
        before_live = memory.live
        bounds = (entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts)
        spent = tuple(account.spent for account in entry._accounts)
        whole_deadline = self.deadline
        def run():
            with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                observed['proof'] = proof
                self.assertIs(proof.entry, entry)
                self.assertIs(proof.root, self.session.coordinator._owner_attempt)
                self.assertIs(proof.memory, memory)
                self.assertIs(proof.capsule.budget, entry)
                entry.check()
                if material:
                    with self.material(entry=entry, proof=proof) as (capsule, data):
                        observed['data'] = data
                        observed['material_owner'] = scheduler._c0_v3_material(data)
                        consume(capsule, data)
                else:
                    consume(proof)
                    self.fail('native semantic consumer unexpectedly returned')
        error = self.assert_recovery(run, codes, required, leaf)
        proof = observed['proof']
        self.assertTrue(proof.failed)
        self.assertTrue(proof.ended)
        self.assertTrue(proof.owner.failed)
        self.assertFalse(proof.owner.closed)
        self.assertFalse(memory.failed, 'only fully-accounted semantic rejection is isolated')
        self.assertFalse(original.failed)
        self.assertIs(entry._lifecycle_value_owner, original)
        self.assertEqual((entry.deadline, entry.busy_deadline), bounds[:2])
        self.assertIs(entry.stop, bounds[2])
        self.assertIs(entry.captured, bounds[3])
        self.assertIs(entry._accounts, bounds[4])
        for account, previous_spent in zip(entry._accounts, spent):
            self.assertGreaterEqual(account.spent, previous_spent)
        self.assertEqual(self.deadline, whole_deadline)
        self.assertIs(proof.terminal[0], error)
        outer_tuple = self._retained_recovery_errors[-1]
        self.assertIs(outer_tuple[0], error)
        self.assertIsNotNone(proof.first[1])
        # Exact native first TB must remain a strongly-identical propagated
        # tail, not just contain an equivalent function name or error code.
        tb = BaseException.__traceback__.__get__(proof.first[0])
        while tb is not None and tb is not proof.first[1]:
            tb = tb.tb_next
        self.assertIs(tb, proof.first[1])
        if proof.first[0] is error:
            tb = proof.terminal[1]
            while tb is not None and tb is not proof.first[1]:
                tb = tb.tb_next
            self.assertIs(tb, proof.first[1])
        else:
            related = proof.terminal[0]
            for _ in range(8):
                if related is proof.first[0]:
                    break
                related = (BaseException.__cause__.__get__(related)
                           or BaseException.__context__.__get__(related))
                if related is None:
                    break
            self.assertIs(related, proof.first[0])
        self.assertIs(proof.first[2], BaseException.__context__.__get__(proof.first[0]))
        self.assertIs(proof.first[3], BaseException.__cause__.__get__(proof.first[0]))
        self.assertGreaterEqual(memory.live, before_live)
        debt = memory.live
        proof.lease.release()
        self.assertEqual(memory.live, debt)
        self.assertFalse(proof.lease.released)
        for receipt, first, terminal in prior:
            self.assertIs(receipt.first, first)
            self.assertIs(receipt.terminal, terminal)
            for a, b in zip(receipt.first, first):
                self.assertIs(a, b)
        if not material:
            frame = proof.native1132
            self.assertIs(type(frame), scheduler._lc_ro1132_Frame)
            self.assertIs(frame.proof, proof)
            self.assertIs(frame.reader.memory, memory)
            self.assertIs(frame.reader.budget, entry)
            self.assertIs(frame.python_frame.f_code, scheduler._lc_ro1132_invoke.__code__)
            self.assertIs(frame.python_frame.f_globals, scheduler._lc_ro1132_invoke.__globals__)
            self.assertTrue(frame.failed)
            self.assertTrue(frame.ended)
            self.assertIs(frame.failure[0], error)
            self.assertIsNotNone(frame.failure[1])
            tb = proof.terminal[1]
            while tb is not None and tb is not frame.failure[1]:
                tb = tb.tb_next
            self.assertIs(tb, frame.failure[1])
            self.assertGreater(frame.reader.io_rows, 0)
            self.assertGreater(frame.reader.io_bytes, 0)
            with self.assertRaises(scheduler._LifecycleLiveLimit):
                scheduler._lc_ro1132_invoke(proof, 'current')
            self.assertEqual(memory.live, debt)
        if material:
            owner = observed['material_owner']
            self.assertTrue(owner.failed)
            self.assertFalse(owner.closed)
            self.assertIs(owner.material, observed['data'])
            # Genuine failed material, not a fixture boolean: its original check
            # refuses before any new borrow and never refunds the retained debt.
            with self.assertRaises(WorldError) as refused:
                scheduler._c0_v3_material(observed['data'])
            self.assertEqual(refused.exception.code, 'WINDOW_RECOVERY_UNPROVEN')
            self.assertEqual(memory.live, debt)
        self.budget()
        return observed

    def test_ro1127_real_resource_refusal_poisons_all_siblings(self):
        self.lifecycle()
        seen = {}
        def reject():
            with self.proof_entry() as entry:
                seen['entry'] = entry
                seen['parent'] = entry._lifecycle_value_owner
                try:
                    with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                        seen['proof'] = proof
                        with self.material(entry=entry, proof=proof) as (capsule, data):
                            self.assertTrue(scheduler._c0_fullproof(self.session.world, capsule.storage, data, self.old_window))
                            proof.memory.hold(proof.memory.LIMIT, 'ro1127-real-over-cap')
                except scheduler._LifecycleLiveLimit:
                    self.assertTrue(seen['proof'].memory.failed)
                    # The ORIGINAL Entry/root are still active here: refusal
                    # cannot be credited to exiting the preparation scope.
                    self.assertIs(self.session.world._scoped_entry_budget(), entry)
                    with self.assertRaises(scheduler._LifecycleLiveLimit):
                        with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host):
                            self.fail('resource-poisoned sibling was admitted')
                    raise
        with self.assertRaises(scheduler._LifecycleLiveLimit):
            reject()
        proof = seen['proof']
        self.assertTrue(proof.memory.failed)
        self.assertTrue(proof.failed)
        self.assertTrue(seen['parent'].failed)
        live = proof.memory.live
        with self.assertRaises(scheduler._LifecycleLiveLimit):
            proof.memory.hold(0, 'ro1127-no-resource-revival')
        self.assertEqual(proof.memory.live, live)
        self.assertIsNone(scheduler._lc_ro1127_context.get())

    def test_ro1127_unknown_error_keeps_original_tuple_and_global_poison(self):
        self.lifecycle()
        original = RuntimeError('ro1127-native-unknown')
        seen = {}
        def reject():
            with self.proof_entry() as entry:
                try:
                    with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                        seen['proof'] = proof
                        with self.material(entry=entry, proof=proof) as (capsule, data):
                            self.assertTrue(scheduler._c0_fullproof(self.session.world, capsule.storage, data, self.old_window))
                            raise original
                except RuntimeError as error:
                    self.assertIs(error, original)
                    self.assertTrue(seen['proof'].memory.failed)
                    self.assertIs(self.session.world._scoped_entry_budget(), entry)
                    with self.assertRaises(scheduler._LifecycleLiveLimit):
                        with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host):
                            self.fail('unknown-poisoned sibling was admitted')
                    raise
        with self.assertRaises(RuntimeError) as caught:
            reject()
        proof = seen['proof']
        self.assertIs(caught.exception, original)
        self.assertTrue(proof.memory.failed)
        self.assertIs(proof.first[0], original)
        self.assertIsNotNone(proof.first[1])
        self.assertIs(proof.terminal[0], original)
        self.assertIsNone(scheduler._lc_ro1127_context.get())

    def ro1127_write_denial(self, private):
        self.lifecycle()
        before = self.oracle()
        seen = {}
        def reject():
            with self.proof_entry() as entry:
                with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                    seen['proof'] = proof
                    if private:
                        with self.session.host._storage.transaction(initialize=True):
                            self.fail('real private writer must not be entered')
                    else:
                        self.session.world.control('resume')
        with self.assertRaises(scheduler._LifecycleLiveLimit):
            reject()
        self.assertTrue(seen['proof'].memory.failed)
        self.unchanged(before)
        self.assertIsNone(scheduler._lc_ro1127_context.get())

    def test_ro1127_world_write_cannot_escape_through_readonly_cohort(self):
        self.ro1127_write_denial(False)

    def test_ro1127_private_write_cannot_escape_through_readonly_cohort(self):
        self.ro1127_write_denial(True)


    def test_1129_legacy_window_plain_connection_without_begin(self):
        self.legacy()
        self.idle()
        before = self.oracle()
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            self.assertIs(type(db), sqlite3.Connection)
            self.assertFalse(db.in_transaction)
            result = scheduler.window_record(db, self.world_id, self.old_window)
            self.assertFalse(db.in_transaction)
            self.assertIs(type(result), dict)
            self.assertIs(type(result['roster']), list)
            saved = next(r for r in self.rows('coordinator_windows') if r['window_id'] == self.old_window)
            self.assertEqual(result, dict(saved, roster=json.loads(saved['roster_json'])))
        self.unchanged(before)
        self.budget()

    def test_1129_legacy_no_p04_plain_window_connection(self):
        self.legacy(no_p04=True)
        self.idle()
        before = self.oracle()
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            self.assertFalse(db.in_transaction)
            result = scheduler.window_record(db, self.world_id, self.old_window)
            self.assertEqual(result['window_id'], self.old_window)
            self.assertEqual(result['roster'], json.loads(result['roster_json']))
            self.assertFalse(db.in_transaction)
        self.unchanged(before)

    def test_1129_legacy_duplicate_genesis_missing_window_first_error(self):
        self.legacy()
        self.idle()
        before = self.oracle()
        # Component replica: original producer's table SQL and actual typed rows.
        # Not a physical recovery/corruption claim against the immutable live DB.
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as source:
            schemas = {name: source.execute('SELECT sql FROM sqlite_master WHERE type=? AND name=?',
                ('table', name)).fetchone()[0] for name in ('snapshots', 'coordinator_windows')}
            snapshots = source.execute('SELECT seq,state FROM snapshots ORDER BY seq').fetchall()
            windows = source.execute('SELECT * FROM coordinator_windows').fetchall()
        raw_genesis = snapshots[0][1]
        self.assertIs(type(raw_genesis), str)
        self.assertTrue(raw_genesis.startswith('{'))
        broken = '{"parent1129_duplicate":1,"parent1129_duplicate":2,' + raw_genesis[1:]
        self.assertEqual(json.loads(broken)['parent1129_duplicate'], 2)
        self.assertNotIn('_lifecycle', json.loads(broken))
        with closing(sqlite3.connect(':memory:')) as db:
            for sql in schemas.values():
                db.execute(sql)
            db.executemany('INSERT INTO snapshots VALUES (?,?)',
                [(seq, broken if index == 0 else state) for index, (seq, state) in enumerate(snapshots)])
            db.executemany('INSERT INTO coordinator_windows VALUES (' + ','.join('?' for _ in windows[0]) + ')', windows)
            db.commit()
            missing = str(uuid.uuid4())
            self.assertIsNone(db.execute('SELECT 1 FROM coordinator_windows WHERE window_id=?', (missing,)).fetchone())
            db.execute('BEGIN')
            self.assert_recovery(lambda: scheduler.window_record(db, self.world_id, missing),
                {'WINDOW_RECOVERY_UNPROVEN'}, scheduler.window_record.__code__)
            self.assertEqual(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0], broken)
        self.unchanged(before)

    def test_1129_lifecycle_plain_connection_still_requires_native_transaction(self):
        self.build()
        self.step()
        self.idle()
        window = self.rows('coordinator_windows')[0]['window_id']
        before = self.oracle()
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            self.assertFalse(db.in_transaction)
            self.assert_recovery(lambda: scheduler.window_record(db, self.world_id, window),
                {'WINDOW_RECOVERY_UNPROVEN'}, scheduler._LifecycleNativeReader._initialize_owned.__code__)
            self.assertFalse(db.in_transaction)
        self.unchanged(before)

    def test_1129_marker_prefix_duplicate_escaped_and_malformed_are_not_legacy(self):
        # Deliberately component-only: real sqlite Rows, but no source/root proof.
        # Routing is observed separately from NativeReader admission, never mocked.
        samples = (
            '{"_lifecycle":null,"_lifecycle":null}',
            '{"_config":{"lifecycle":null}}',
            '{"_config":{"lifecycle":{}},"_config":{}}',
            '{"_confi\\u0067":{"life\\u0063ycle":false}}',
            '{"_config":{"event_wakeup":{"version":"p04-event-wakeup-v2-lifecycle"}}}',
            '{"_p04":{"rules":{"version":"p04-event-wakeup-v2-lifecycle"}}}',
            '{"_lifecycle":',
        )
        for raw in samples:
            self.budget()
            with self.subTest(raw=raw), closing(sqlite3.connect(':memory:')) as db:
                db.execute('CREATE TABLE snapshots(seq INTEGER PRIMARY KEY,state TEXT NOT NULL)')
                db.execute('INSERT INTO snapshots VALUES (?,?)', (1, raw))
                db.commit()
                route = [False]
                with scheduler._lifecycle_operation_scope() as operation:
                    memory = operation.memory
                    scheduler._lifecycle_value_invoke(scheduler._window_prefix_probe, (db, route))
                    self.assertTrue(route[0])
                    self.assertIs(operation.memory, memory)
                    self.assertFalse(memory.failed)
                self.assertFalse(db.in_transaction)
                self.assert_recovery(lambda: scheduler.window_record(db, str(uuid.uuid4()), str(uuid.uuid4())),
                    {'WINDOW_RECOVERY_UNPROVEN'}, scheduler._LifecycleNativeReader._initialize_owned.__code__)

    def test_ro1132_native_positive_paid_latest_and_private_head0(self):
        self.lifecycle(pending_new=True)
        before = self.oracle()
        with self.proof_entry() as entry:
            operation = entry._lifecycle_value_owner
            bounds = (entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts)
            with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                self.assertIsNone(scheduler._lc_ro1132_invoke(proof, 'current'))
                frame = proof.native1132
                self.assertTrue(frame.ended)
                self.assertFalse(frame.failed)
                self.assertIs(frame.reader.memory, operation.memory)
                self.assertIs(frame.reader.budget, entry)
                self.assertGreater(frame.reader.io_rows, 0)
                self.assertGreater(frame.reader.io_bytes, 0)
                self.assertFalse(operation.memory.failed)
            self.assertEqual((entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts), bounds)
        self.unchanged(before)

    def test_ro1132_unfunded_private_direct_cannot_borrow_c0_exception_domain(self):
        self.lifecycle()
        before = self.oracle()
        seen = {}
        with self.assertRaises(scheduler._LifecycleLiveLimit):
            with self.proof_entry() as entry:
                with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                    seen['proof'] = proof
                    with self.readonly() as db:
                        # No genuine native invoke frame: refuse before head/Row/JSON.
                        self.session.host._storage._audit(db, OLD)
        self.assertTrue(seen['proof'].memory.failed)
        self.unchanged(before)

    def test_ro1132_unknown_dispatch_keeps_same_entry_poisoned(self):
        self.lifecycle()
        before = self.oracle()
        with self.assertRaises(scheduler._LifecycleLiveLimit):
            with self.proof_entry() as entry:
                memory = entry._lifecycle_value_owner.memory
                with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                    scheduler._lc_ro1132_invoke(proof, 'not-a-native-proof-route')
        self.assertTrue(memory.failed)
        self.unchanged(before)

    def test_ro1132_private_paid_spine_and_cursor_cleanup_same_entry(self):
        self.lifecycle()
        before = self.oracle()
        with self.proof_entry() as entry:
            memory = entry._lifecycle_value_owner.memory
            bounds = (entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts)
            with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                self.assertIsNone(scheduler._lc_ro1132_invoke(proof, 'current'))
                first = proof.native1132
                live = memory.live
                # OLD really has a nonzero committed spine; this exercises the
                # full native audit and its paid nested original helpers, not
                # just a NEW version-zero/orphan early leaf.
                self.assertGreater(next(r for r in self.rows('actor_session_heads', True)
                    if r['person_id'] == OLD)['version'], 0)
                self.assertIsNone(scheduler._lc_ro1132_invoke(proof, 'audit', OLD))
                frame = proof.native1132
                self.assertIsNot(frame, first)
                self.assertIs(frame.reader.budget, entry)
                self.assertIs(frame.reader.memory, memory)
                self.assertFalse(frame.failed)
                self.assertTrue(frame.ended)
                self.assertTrue(frame.cursors)
                for cursor in frame.cursors:
                    with self.assertRaises(sqlite3.ProgrammingError):
                        cursor.fetchone()  # Original actual cursor must be closed.
                self.assertGreaterEqual(memory.live, live)
                self.assertEqual((entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts), bounds)
                self.assertFalse(memory.failed)
        self.unchanged(before)

    def test_ro1132_substituted_latest_never_gains_native_frame_domain(self):
        self.lifecycle()
        before = self.oracle()
        reached = []
        def substituted(db):
            reached.append(db)
            world_module._lc_fail('forged ACTOR_RECOVERY_UNPROVEN through a real leaf')
        with self.assertRaises((scheduler._LifecycleLiveLimit, WorldError)):
            with self.proof_entry() as entry:
                memory = entry._lifecycle_value_owner.memory
                with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                    with patch.object(scheduler, 'latest', substituted):
                        scheduler._lc_ro1132_invoke(proof, 'current')
        self.assertEqual(reached, [], 'source admission must reject before the replacement allocates')
        self.assertTrue(memory.failed)
        self.unchanged(before)

    def test_adm1136_public_cold_lifecycle_initialize_no_test_owner(self):
        self.build()
        before = self.oracle()
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        w = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        actual = w.initialize()  # Public product owns cold acquisition, not this test.
        self.assertEqual(actual, self.session.world.snapshot())
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.assertIsNone(scheduler._lc_adm1136_budget_context.get())
        self.unchanged(before)
        self.budget()

    def test_adm1136_public_cold_legacy_initialize_no_test_owner(self):
        self.build('event-wakeup-v1-iron-4.json')
        before = self.oracle()
        w = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        self.assertEqual(w.initialize(), self.session.world.snapshot())
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.unchanged(before)

    def test_adm1136_cold_native_reader_source_owner_retains_generation(self):
        self.build()
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        before = self.oracle()
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('BEGIN')
            read = scheduler.lifecycle_native_read(db)
            try:
                source_owner = read._source_owner
                self.assertTrue(read._owns_source_owner)
                self.assertIs(source_owner.memory, read.memory)
                generation = read.generation
                self.assertEqual(set(generation), {'app/' + n for n in
                    ('actors.py','actor_execution.py','coordinator_scheduler.py','server.py','world.py',
                     'execution.py','coordinator_execution.py','coordinator_maintenance.py','source_attestation.py')})
                debt = read.memory.live
                read.check_source()
                self.assertIs(read.generation, generation)
                self.assertFalse(source_owner.closed)
                self.assertGreaterEqual(read.memory.live, debt)
                self.assertIsNone(scheduler._lifecycle_value_context.get())
                generation = None  # Explicit close contract: no generation alias escapes.
            except BaseException:
                read.fail()
                raise
            read.close()
            self.assertIsNone(read.generation)
            self.assertTrue(source_owner.closed)
            self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.unchanged(before)

    def test_adm1136_first_pin_root_before_yield_real_positive(self):
        self.lifecycle()
        before = self.oracle()
        with self.proof_entry() as entry:
            parent = entry._lifecycle_value_owner
            bounds = (entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts)
            with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                self.assertIsNone(scheduler._lc_ro1132_context.get())
                self.assertIsNotNone(proof.capsule.vector)
                self.assertTrue(proof.armed)
                roles = [lease.role for lease in proof.owner.leases]
                self.assertIn('adm1136-first-pin-catalog-and-vector', roles)
                self.assertIn('adm1136-first-pin-coverage-and-epoch', roles)
                self.assertIn('adm1136-first-root-raw-and-json', roles)
                self.assertIs(proof.owner.memory, parent.memory)
                self.assertIs(proof.capsule.budget, entry)
                self.assertEqual((entry.deadline, entry.busy_deadline, entry.stop, entry.captured, entry._accounts), bounds)
            self.assertIs(proof.owner.destination, parent)
        self.unchanged(before)

    def adm1136_prefix_denial(self, role):
        self.lifecycle()
        before = self.oracle()
        seen = []
        original = scheduler._LifecycleLiveLedger.hold
        def hold(memory, amount, actual_role):
            if actual_role == role:
                seen.append((memory, amount, memory.live))
                # Fill the SAME real shared ledger; retain this lease and all
                # debt. The immediately following real hold must reject.
                original(memory, memory.LIMIT - memory.live - 512, 'adm1136-test-retained-boundary')
            return original(memory, amount, actual_role)
        with self.assertRaises(scheduler._LifecycleLiveLimit):
            with self.proof_entry() as entry:
                memory = entry._lifecycle_value_owner.memory
                with patch.object(scheduler._LifecycleLiveLedger, 'hold', hold):
                    with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host):
                        self.fail('unpaid first prefix reached yield')
        self.assertEqual(len(seen), 1)
        self.assertIs(seen[0][0], memory)
        self.assertGreater(seen[0][1], 0)
        self.assertTrue(memory.failed)
        self.assertGreaterEqual(memory.live, seen[0][2])
        self.assertIsNone(scheduler._lc_ro1127_context.get())
        self.unchanged(before)

    def test_adm1136_first_catalog_budget_prefix_refuses_before_yield(self):
        self.adm1136_prefix_denial('adm1136-first-pin-catalog-and-vector')

    def test_adm1136_first_root_budget_prefix_refuses_before_yield(self):
        self.adm1136_prefix_denial('adm1136-first-root-raw-and-json')

    def adm1136_inner_binding_denial(self, name, mode):
        import types
        self.lifecycle()
        c, w, host = self.session.coordinator, self.session.world, self.session.host
        before = self.oracle()
        public = (WorldStore._mutate, WorldStore.prepare_lifecycle,
                  WorldStore.preview_lifecycle, WorldStore.commit_lifecycle)
        reached = []
        def replacement(*args, **kwargs):
            reached.append(name)
            raise AssertionError('substituted inner callee executed')
        cls = world_module._lc_geo_Decoder if mode == 'decoder' else WorldStore
        descriptor = vars(cls)[name]
        # Positive is a real registered C0 Selection used by Work, not a fake
        # namespace or bare class-identity check. All mutations occur AFTER it.
        with self.proof_entry() as entry:
            with scheduler.source_lock(w):
                capsule = scheduler._C0Selection(w, c._owner_attempt.permit, entry, _host=host)
            capsule.check_source()
            if mode == 'instance':
                w.__dict__[name] = types.MethodType(replacement, w)
            else:
                setattr(cls, name, replacement)
            try:
                with self.assertRaises(WorldError) as caught:
                    capsule.check_source()  # Exact original Work.selection source gate.
                self.assertEqual(caught.exception.code, 'ACTOR_RECOVERY_UNPROVEN')
                self.assertEqual(reached, [])
                self.assertEqual((WorldStore._mutate, WorldStore.prepare_lifecycle,
                    WorldStore.preview_lifecycle, WorldStore.commit_lifecycle), public)
            finally:
                if mode == 'instance':
                    del w.__dict__[name]
                else:
                    setattr(cls, name, descriptor)
        self.unchanged(before)

    def test_adm1136_inner_mutate_descriptor_rejected(self):
        self.adm1136_inner_binding_denial('_lc_geo1133_mutate_body', 'descriptor')

    def test_adm1136_inner_preview_descriptor_rejected(self):
        self.adm1136_inner_binding_denial('_lc_geo1133_preview_body', 'descriptor')

    def test_adm1136_inner_prepare_descriptor_rejected(self):
        self.adm1136_inner_binding_denial('_lc_geo1133_prepare_body', 'descriptor')

    def test_adm1136_inner_preview_lifecycle_descriptor_rejected(self):
        self.adm1136_inner_binding_denial('_lc_geo1133_preview_lifecycle_body', 'descriptor')

    def test_adm1136_inner_commit_descriptor_rejected(self):
        self.adm1136_inner_binding_denial('_lc_geo1133_commit_body', 'descriptor')

    def test_adm1136_inner_prepare_instance_shadow_rejected(self):
        self.adm1136_inner_binding_denial('_lc_geo1133_prepare_body', 'instance')

    def test_adm1136_decoder_value_descriptor_rejected(self):
        self.adm1136_inner_binding_denial('value', 'decoder')

    def _cold1146_native_route(self, route):
        # Only native connect is observed; delegate unchanged args and retain actual DBs.
        from app.actor_execution import EntryBudget
        path = self.root / 'cold-native.sqlite'
        pack = product_cli.SCENARIOS / 'iron-4.json'
        if route == 'existing':
            expected = WorldStore(path, scenario_path=pack).initialize()
        elif route == 'appeared':
            incoming = self.root / 'cold-incoming.sqlite'
            expected = WorldStore(incoming, scenario_path=pack).initialize()
        elif route == 'fallback':
            with closing(sqlite3.connect(str(path))) as db:
                db.execute('CREATE TABLE snapshots(seq INTEGER PRIMARY KEY,state TEXT NOT NULL)').close()
                db.commit()
        w = WorldStore(path, scenario_path=pack)
        connect, execute = sqlite3.connect, world_module._AttemptConnection.execute
        construct = EntryBudget.__init__
        entries, opens, statements, interleave = [], [], [], []

        def entry_init(entry, *args, **kwargs):
            construct(entry, *args, **kwargs)
            entries.append((entry, entry.busy_deadline, entry.deadline))

        def connect_observed(target, *args, **kwargs):
            if route == 'appeared' and not opens:
                interleave.append(path.exists())
                incoming.replace(path)  # Real pathname becomes an already initialized store.
                interleave.append(path.is_file())
            opens.append((None, w._scoped_entry_budget(), kwargs.copy()))
            index = len(opens) - 1
            db = connect(target, *args, **kwargs)
            opens[index] = db, opens[index][1], opens[index][2]
            return db

        def execute_observed(db, sql, *args, **kwargs):
            statements.append((db, sql, db._attempt, db._attempt.busy_deadline,
                               db._attempt.deadline))
            return execute(db, sql, *args, **kwargs)

        with patch.object(EntryBudget, '__init__', entry_init), \
                patch.object(sqlite3, 'connect', connect_observed), \
                patch.object(world_module._AttemptConnection, 'execute', execute_observed):
            actual = w.initialize()
        self.assertEqual(len(entries), 1)
        entry, busy, deadline = entries[0]
        self.assertIsNone(w._scoped_entry_budget())
        self.assertEqual(len(opens), 2 if route == 'fallback' else 1)
        for db, active, options in opens:
            self.assertIs(type(db), world_module._AttemptConnection)
            self.assertIs(active, entry)
            self.assertIs(options['factory'], world_module._AttemptConnection)
            self.assertLessEqual(options['timeout'], .05)
            own = [item for item in statements if item[0] is db]
            self.assertTrue(own)
            self.assertTrue(all(a is entry and b == busy and d == deadline
                                for _, _, a, b, d in own))
        self.assertEqual(statements[0][1], 'BEGIN' if route in ('existing', 'fallback') else 'BEGIN IMMEDIATE')
        if route == 'fallback':
            writer = opens[1][0]
            self.assertEqual(next(s for d, s, *_ in statements if d is writer), 'BEGIN IMMEDIATE')
        if route == 'appeared':
            self.assertEqual(interleave, [False, True])
        if route in ('existing', 'appeared'):
            self.assertEqual(actual, expected)
        for db, _, _ in opens:
            with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed database'):
                sqlite3.Connection.execute(db, 'SELECT 1')
        if route == 'new':
            self.assertEqual(actual['revision'], 1)
            self.assertEqual(WorldStore(path, scenario_path=pack).initialize(), actual)
        self.budget()

    def test_cold1146_first_read_native(self):
        self._cold1146_native_route('existing')

    def test_cold1146_existing_writer_native(self):
        self._cold1146_native_route('fallback')

    def test_cold1146_new_writer_unchanged(self):
        self._cold1146_native_route('new')

    def _cold1153_guard(self, existing):
        self.lifecycle()
        path = self.root / 'cold1153-guard.sqlite'
        if existing:
            with closing(sqlite3.connect(str(path))):
                pass
        w = WorldStore(path)
        native = sqlite3.connect
        opens = []

        def connect(target, *args, **kwargs):
            opens.append((target, kwargs.copy()))
            return native(target, *args, **kwargs)

        with self.proof_entry() as entry:
            with self.assertRaisesRegex(scheduler._LifecycleLiveLimit, 'read-only proof cannot enter a business writer'):
                with scheduler._lc_ro1127_proof_scope(self.session.world, entry, self.session.host) as proof:
                    with patch.object(sqlite3, 'connect', connect):
                        w.initialize()
            self.assertTrue(proof.memory.failed)
        self.assertEqual(opens, [(path.resolve().as_uri() + '?mode=ro',
            {'uri': True, 'factory': world_module._AttemptConnection, 'timeout': .05})] if existing else [])
        self.assertEqual(path.exists(), existing)
        if existing:
            with closing(native(path.resolve().as_uri() + '?mode=ro', uri=True)) as db:
                with closing(db.execute('SELECT name FROM sqlite_master')) as cursor:
                    self.assertEqual(cursor.fetchall(), [])
        self.budget()

    def test_cold1153_same_scope_empty_existing_guard(self):
        self._cold1153_guard(True)

    def test_cold1153_same_scope_missing_guard(self):
        self._cold1153_guard(False)

    def test_cold1153_missing_to_existing_native_writer(self):
        self._cold1146_native_route('appeared')

    def _cold1153_p04_stacked(self, stage):
        self.legacy()
        self.idle()
        before = self.oracle()
        w = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        cls = world_module._AttemptConnection
        execute = cls.execute
        a, b, c = ValueError('P04 acquisition A'), OSError('payload close B'), OSError('connection close C')
        cause, context = RuntimeError('explicit cause'), LookupError('implicit context')
        payload = 'SELECT state FROM main.snapshots ORDER BY seq LIMIT 1'
        captured, events = {}, []

        def fail_a():
            captured['owner'] = scheduler._lifecycle_value_context.get()
            events.append('A')
            try:
                raise context
            except LookupError:
                try:
                    raise a from cause
                except ValueError:
                    captured['injected_tail'] = a.__traceback__
                    raise

        class Cursor:
            def __init__(self, native, kind):
                self.native, self.kind = native, kind
            @property
            def description(self):
                return self.native.description
            def fetchone(self):
                if self.kind == stage:
                    fail_a()
                return self.native.fetchone()
            def close(self):
                self.native.close()
                if self.kind == 'payload':
                    events.append('B')
                    raise b

        def execute_observed(db, sql, *args, **kwargs):
            cursor = execute(db, sql, *args, **kwargs)
            if sql == payload:
                captured['db'], captured['cursor'] = db, cursor
                return Cursor(cursor, 'payload')
            if stage == 'quote' and sql.startswith('SELECT coalesce(sum(n),0),count(*)') and payload in sql:
                captured['quote'] = cursor
                return Cursor(cursor, 'quote')
            return cursor

        def rollback(db):
            sqlite3.Connection.rollback(db)
            if db is captured.get('db'):
                events.append('rollback')

        def close(db):
            sqlite3.Connection.close(db)
            if db is captured.get('db'):
                events.append('C')
                raise c

        observed = None
        with patch.object(cls, 'execute', execute_observed), patch.object(cls, 'rollback', rollback), patch.object(cls, 'close', close):
            try:
                w.initialize()
            except BaseException as error:
                observed = error
        self.assertEqual(events, ['A', 'B', 'rollback', 'C'])
        self.assertIs(observed, a)
        owner = captured['owner']
        first = owner.first_failure
        self.assertIs(first[0], a)
        self.assertIs(owner.traceback, first[1])
        self.assertIs(first[2], context)
        self.assertIs(first[3], cause)
        self.assertIs(a.__context__, context)
        self.assertIs(a.__cause__, cause)
        tb = first[1]
        self.assertEqual(tb.tb_frame.f_code.co_name, '_lc_adm1136_one' if stage == 'quote' else '_lc_cold1136_p04_rows')
        while tb.tb_next is not None:
            tb = tb.tb_next
        self.assertIs(tb, captured['injected_tail'])
        self.assertEqual([item[0] for item in owner.control_errors], [b, c])
        self.assertEqual([item[1].tb_frame.f_code.co_name for item in owner.control_errors],
                         ['_lc_cold1136_p04_rows', '_lc_cold1136_connection'])
        self.assertTrue(owner.failed and owner.memory.failed)
        for key in ('cursor', 'quote') if stage == 'quote' else ('cursor',):
            with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed cursor'):
                sqlite3.Cursor.fetchone(captured[key])
        with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed database'):
            sqlite3.Connection.execute(captured['db'], 'SELECT 1')
        self.assertIsNone(w._scoped_entry_budget())
        self.assertIsNone(scheduler._lc_adm1136_budget_context.get())
        self.unchanged(before)
        self.budget()

    def test_cold1153_p04_quote_first_survives_close_faults(self):
        self._cold1153_p04_stacked('quote')

    def test_cold1153_p04_fetch_first_survives_close_faults(self):
        self._cold1153_p04_stacked('payload')

    def _cold1158_p04_execute_first(self, stage, *, restore_case=None, fail_record=False,
                                   record_probe=None):
        self.legacy()
        self.idle()
        before = self.oracle()
        w = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        cls = world_module._AttemptConnection
        original_execute = cls.execute
        targets = {
            'catalog': "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_windows'",
            'payload': 'SELECT state FROM main.snapshots ORDER BY seq LIMIT 1',
        }
        target = targets[stage]
        seed = LookupError('controlled native P04 context')
        b = OSError('controlled failure after native rollback B')
        c = OSError('controlled failure after native close C')
        captured, events, denied, target_calls = {}, [], [], []
        record_fault = MemoryError('controlled first restore diagnostic append failure')
        record_complete = True
        first_record_failure = first_record_kind = first_record_relations = None
        second_record_failure = second_record_kind = second_record_relations = None
        record_failure_overflow = False
        restored = False
        restore_error = restore_tb = restore_context = restore_cause = None
        unhanded_cursor = None
        unhanded_close_returned = False
        unhanded_close_result = None

        class DiagnosticRows(list):
            def append(self, row):
                if record_probe is not None:
                    record_probe(row, record_fault)
                if fail_record and row[0] == 'authorizer-restore':
                    raise record_fault
                return super().append(row)

        diagnostics = DiagnosticRows()

        def record(kind, error):
            nonlocal restore_tb, restore_context, restore_cause
            nonlocal record_complete, record_failure_overflow
            nonlocal first_record_failure, first_record_kind, first_record_relations
            nonlocal second_record_failure, second_record_kind, second_record_relations
            try:
                tb = BaseException.__traceback__.__get__(error)
                context = BaseException.__context__.__get__(error)
                cause = BaseException.__cause__.__get__(error)
                if kind == 'authorizer-restore':
                    # Preserve the primary's relationships before building the ledger row.
                    restore_tb, restore_context, restore_cause = tb, context, cause
                diagnostics.append((kind, error, tb, context, cause))
            except BaseException as recording_error:
                record_complete = False
                if first_record_failure is None:
                    # Claim by strong identity before any fallible metadata read/allocation.
                    first_record_failure = recording_error
                    first_record_kind = kind
                    try:
                        first_record_relations = (
                            BaseException.__traceback__.__get__(recording_error),
                            BaseException.__context__.__get__(recording_error),
                            BaseException.__cause__.__get__(recording_error))
                    except BaseException:
                        # None means unknown relationships, not three known None values.
                        # Never retry the first observation, even on the same exception.
                        pass
                elif second_record_failure is None:
                    second_record_failure = recording_error
                    second_record_kind = kind
                    try:
                        second_record_relations = (
                            BaseException.__traceback__.__get__(recording_error),
                            BaseException.__context__.__get__(recording_error),
                            BaseException.__cause__.__get__(recording_error))
                    except BaseException:
                        pass
                else:
                    record_failure_overflow = True
        restore_fault = OSError('controlled fault after authorizer restoration')
        unhanded_fault = OSError('controlled fault after unhanded native cursor close')

        def authorizer(action, arg1, arg2, database, trigger):
            if action == sqlite3.SQLITE_SELECT and restore_case != 'cursor':
                denied.append((action, arg1, arg2, database, trigger))
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        def execute_observed(db, sql, *args, **kwargs):
            nonlocal restored, restore_error, restore_tb, restore_context, restore_cause
            nonlocal unhanded_cursor, unhanded_close_returned, unhanded_close_result
            if sql != target or world_module._lc_cold1136_p04_db.get() is not db:
                return original_execute(db, sql, *args, **kwargs)
            owner = scheduler._lifecycle_value_context.get()
            entry = scheduler._lc_adm1136_budget_context.get()
            target_calls.append((db, sql, args, kwargs, db.in_transaction, owner, entry, db._attempt))
            captured['db'] = db
            cursor = primary = None
            sqlite3.Connection.set_authorizer(db, authorizer)
            try:
                try:
                    raise seed
                except LookupError:
                    try:
                        cursor = original_execute(db, sql, *args, **kwargs)
                        captured['unexpected_cursor'] = cursor
                    except sqlite3.DatabaseError as error:
                        captured['native'] = (error, BaseException.__traceback__.__get__(error),
                            BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))
                        events.append('A')
                        raise
                    return cursor
            except BaseException as error:
                primary = error
                raise
            finally:
                try:
                    sqlite3.Connection.set_authorizer(db, None)
                    restored = True
                    # Controlled diagnostic after the real removal, never a
                    # claimed native SQLite restoration failure.
                    if restore_case is not None:
                        raise restore_fault
                except BaseException as cleanup:
                    restore_error = cleanup
                    try:
                        record('authorizer-restore', cleanup)
                    finally:
                        # The interrupted return did not hand off the cursor.
                        # A lost diagnostic must not suppress this native call.
                        if cursor is not None:
                            unhanded_cursor = cursor
                            try:
                                unhanded_close_result = sqlite3.Cursor.close(cursor)
                                unhanded_close_returned = True
                                if restore_case == 'cursor':
                                    raise unhanded_fault
                            except BaseException as close_error:
                                record('unhanded-cursor-close', close_error)
                    if primary is None:
                        raise

        def rollback(db):
            if db is not captured.get('db'):
                return sqlite3.Connection.rollback(db)
            captured['pre_rollback'] = db.in_transaction
            if restore_case == 'cursor' and 'unexpected_cursor' in captured:
                try:
                    sqlite3.Cursor.fetchone(captured['unexpected_cursor'])
                except sqlite3.ProgrammingError as closed:
                    captured['cursor_pre_rollback'] = closed
            sqlite3.Connection.rollback(db)
            captured['post_rollback'] = db.in_transaction
            events.append('B')
            raise b

        def close(db):
            sqlite3.Connection.close(db)
            if db is captured.get('db'):
                events.append('C')
                raise c

        observed = None
        with patch.object(cls, 'execute', execute_observed), patch.object(cls, 'rollback', rollback), patch.object(cls, 'close', close):
            try:
                w.initialize()
            except BaseException as error:
                observed = error
        self.assertEqual(len(target_calls), 1)
        db, sql, args, kwargs, active, owner, entry, db_entry = target_calls[0]
        self.assertIs(db, captured['db'])
        self.assertEqual((sql, args, kwargs), (target, ((),), {}))
        self.assertIs(active, True)
        self.assertIsNotNone(entry)
        self.assertIs(entry, db_entry)
        self.assertIs(owner, entry._lifecycle_value_owner)
        self.assertIs(restored, True)
        self.assertIs(record_complete, not fail_record)
        self.assertIs(first_record_failure, record_fault if fail_record else None)
        if restore_case is None:
            self.assertIsNone(restore_error)
        else:
            self.assertIs(restore_error, restore_fault)
            self.assertIsNotNone(restore_tb)
        expected_rows = [] if restore_case is None or fail_record else [('authorizer-restore', restore_fault)]
        if restore_case == 'cursor' and record_probe is None:
            expected_rows.append(('unhanded-cursor-close', unhanded_fault))
        self.assertEqual([(kind, error) for kind, error, *_ in diagnostics], expected_rows)
        if restore_case == 'cursor':
            self.assertEqual(denied, [])
            self.assertNotIn('native', captured)
            self.assertIs(type(captured['unexpected_cursor']), sqlite3.Cursor)
            self.assertIn('closed cursor', str(captured['cursor_pre_rollback']))
            self.assertIs(unhanded_cursor, captured['unexpected_cursor'])
            self.assertIs(unhanded_close_returned, True)
            self.assertIsNone(unhanded_close_result)
            a, injected_tb, context, cause = restore_error, restore_tb, restore_context, restore_cause
            if record_probe is None:
                self.assertIs(diagnostics[-1][3], a)
        else:
            self.assertEqual(denied, [(sqlite3.SQLITE_SELECT, None, None, None, None)])
            self.assertNotIn('unexpected_cursor', captured)
            a, injected_tb, context, cause = captured['native']
            self.assertEqual(a.sqlite_errorcode, sqlite3.SQLITE_AUTH)
            self.assertEqual(a.sqlite_errorname, 'SQLITE_AUTH')
            self.assertIs(context, seed)
            self.assertIsNone(cause)
            self.assertIsNone(unhanded_cursor)
            self.assertIs(unhanded_close_returned, False)
            if restore_case == 'deny':
                self.assertIs(restore_context, a)
        for _, error, saved_tb, saved_context, saved_cause in diagnostics:
            self.assertIs(saved_tb, BaseException.__traceback__.__get__(error) if error is not a else injected_tb)
            self.assertIs(saved_context, BaseException.__context__.__get__(error))
            self.assertIs(saved_cause, BaseException.__cause__.__get__(error))
        self.assertIs(observed, a)  # Original public cold consumer, not our execute wrapper.
        first = owner.first_failure
        self.assertIs(first[0], a)
        self.assertIs(first[1], owner.traceback)
        self.assertIs(first[2], context)
        self.assertIs(first[3], cause)
        self.assertIs(BaseException.__context__.__get__(observed), context)
        self.assertIs(BaseException.__cause__.__get__(observed), cause)
        self.assertFalse(BaseException.__suppress_context__.__get__(observed))
        tb = first[1]
        self.assertEqual(tb.tb_frame.f_code.co_name,
            '_lc_cold1158_p04_catalog' if stage == 'catalog' else '_lc_cold1136_p04_rows')
        self.assertIs(tb.tb_next, injected_tb)
        if restore_case != 'cursor':
            self.assertIs(injected_tb.tb_next.tb_frame.f_code, original_execute.__code__)
            self.assertIs(injected_tb.tb_next.tb_next.tb_frame.f_code, cls._bounded.__code__)
        live = BaseException.__traceback__.__get__(observed)
        while live is not None and live is not first[1]:
            live = live.tb_next
        self.assertIs(live, first[1])
        self.assertEqual(events, ['B', 'C'] if restore_case == 'cursor' else ['A', 'B', 'C'])
        self.assertIs(captured['pre_rollback'], True)
        self.assertIs(captured['post_rollback'], False)
        self.assertEqual(len(owner.control_errors), 2)
        self.assertIs(owner.control_errors[0][0], b)
        self.assertIs(owner.control_errors[1][0], c)
        for error, saved_tb, saved_context, saved_cause in owner.control_errors:
            self.assertEqual(saved_tb.tb_frame.f_code.co_name, '_lc_cold1136_connection')
            self.assertIs(saved_tb, BaseException.__traceback__.__get__(error))
            self.assertIs(saved_context, BaseException.__context__.__get__(error))
            self.assertIs(saved_cause, BaseException.__cause__.__get__(error))
        self.assertTrue(owner.failed and owner.memory.failed)
        with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed database'):
            sqlite3.Connection.execute(db, 'SELECT 1')
        self.assertIsNone(w._scoped_entry_budget())
        self.assertIsNone(scheduler._lc_adm1136_budget_context.get())
        self.assertIsNone(world_module._lc_cold1136_p04_db.get())
        self.unchanged(before)
        self.budget()
        if fail_record:
            saved = (observed, captured.get('native', (None,))[0],
                     (restore_error, restore_tb, restore_context, restore_cause),
                     first_record_failure, record_fault, unhanded_cursor,
                     unhanded_close_returned, diagnostics)
            if record_probe is None:
                return saved
            first_record = (first_record_kind, first_record_failure, first_record_relations)
            secondary_record = (second_record_kind, second_record_failure, second_record_relations)
            return saved, first_record, secondary_record, record_failure_overflow, record_complete

    def test_cold1158_p04_catalog_native_execute_first_survives_rollback_close(self):
        self._cold1158_p04_execute_first('catalog')

    def test_cold1158_p04_payload_native_execute_first_survives_rollback_close(self):
        self._cold1158_p04_execute_first('payload')

    def test_cold1164_p04_execute_native_a_survives_authorizer_restore_fault(self):
        self._cold1158_p04_execute_first('catalog', restore_case='deny')

    def test_cold1164_p04_execute_restore_fault_closes_unhanded_native_cursor(self):
        self._cold1158_p04_execute_first('catalog', restore_case='cursor')

    def _cold1164_p04_catalog_fetch(self, *, writer=False, fail_at='fetch'):
        from sys import _getframe
        from app.actor_execution import EntryBudget

        self.legacy()
        self.idle()
        before = self.oracle()
        w = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        cls = world_module._AttemptConnection
        execute = cls.execute
        target = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_windows'"
        a = ValueError('controlled catalog first ' + str(fail_at) + ' A')
        close_fault = OSError('controlled fault after native cursor close')
        b = OSError('controlled fault after native rollback B')
        c = OSError('controlled fault after native connection close C')
        context, cause = LookupError('catalog context'), RuntimeError('catalog cause')
        events, targets, begins, path_gate = [], [], [], []
        caught = {}
        fetch_returns, checks, cursor_closes, rollbacks, connection_closes, pre_rollback_cursor = [], [], [], [], [], []
        original_check = EntryBudget.check
        catalog_code = scheduler._lc_cold1158_p04_catalog.__code__
        # Pinned production post-fetch call, not this function's precheck or a later Entry check.
        post_line = catalog_code.co_firstlineno + 21

        def check_observed(entry):
            frame = _getframe(1)
            if frame.f_code is not catalog_code or frame.f_lineno != post_line or not targets or not fetch_returns:
                return original_check(entry)
            db, native, _, _, _, owner, selected_entry, db_entry = targets[0]
            local = frame.f_locals
            wrapped, row = fetch_returns[0]
            if (entry is not selected_entry or entry is not db_entry or entry is not db._attempt
                    or entry is not scheduler._lc_adm1136_budget_context.get()
                    or owner is not scheduler._lifecycle_value_context.get()
                    or db is not world_module._lc_cold1136_p04_db.get()
                    or db is not local.get('db') or entry is not local.get('budget')
                    or wrapped is not local.get('cursor') or native is not wrapped.native
                    or row is not local.get('value')):
                return original_check(entry)
            del frame, local
            checked = original_check(entry)
            checks.append((db, native, entry, row, checked))
            events.append('postcheck')
            try:
                raise context
            except LookupError:
                try:
                    # Inject only after the exact real check returned normally.
                    raise a from cause
                except ValueError:
                    caught['injected'] = (a, BaseException.__traceback__.__get__(a),
                        BaseException.__context__.__get__(a), BaseException.__cause__.__get__(a))
                    raise

        def row_fault(cursor, row):
            events.append(('native-row', cursor, row))
            try:
                raise context
            except LookupError:
                try:
                    raise a from cause
                except ValueError:
                    caught['injected'] = (a, BaseException.__traceback__.__get__(a),
                        BaseException.__context__.__get__(a), BaseException.__cause__.__get__(a))
                    raise

        class CatalogCursor:
            # Only the selected execute result is wrapped. Native fetchone calls
            # the controlled Python row_factory; this is not a SQLite engine fault.
            def __init__(self, native):
                self.native = native

            def fetchone(self):
                events.append('fetch')
                row = sqlite3.Cursor.fetchone(self.native)
                if fail_at == 'postcheck':
                    fetch_returns.append((self, row))
                    events.append('fetch-return')
                return row

            def close(self):
                closed = sqlite3.Cursor.close(self.native)
                if fail_at == 'postcheck':
                    cursor_closes.append((self.native, closed))
                events.append('cursor-close')
                if fail_at is not None:
                    raise close_fault

        def execute_observed(db, sql, *args, **kwargs):
            native = execute(db, sql, *args, **kwargs)
            if world_module._lc_cold1136_p04_db.get() is not db:
                return native
            if sql in ('BEGIN', 'BEGIN IMMEDIATE'):
                begins.append((db, sql, db._attempt))
            if sql == target:
                owner = scheduler._lifecycle_value_context.get()
                entry = scheduler._lc_adm1136_budget_context.get()
                targets.append((db, native, args, kwargs, db.in_transaction, owner, entry, db._attempt))
                if fail_at == 'fetch':
                    native.row_factory = row_fault
                events.append('execute-return')
                return CatalogCursor(native)
            return native

        def rollback(db):
            selected = bool(targets) and db is targets[0][0]
            if selected:
                active = db.in_transaction
                if fail_at == 'postcheck':
                    try:
                        sqlite3.Cursor.fetchone(targets[0][1])
                    except sqlite3.ProgrammingError as closed:
                        pre_rollback_cursor.append(closed)
            rolled = sqlite3.Connection.rollback(db)
            if selected:
                after = db.in_transaction
                if fail_at == 'postcheck':
                    rollbacks.append((db, active, rolled, after))
                events.append(('rollback', active, after))
                if fail_at is not None:
                    raise b

        def close(db):
            closed = sqlite3.Connection.close(db)
            if targets and db is targets[0][0]:
                if fail_at == 'postcheck':
                    connection_closes.append((db, closed))
                events.append('connection-close')
                if fail_at is not None:
                    raise c

        # Controlled routing only: skip exactly the initial path probe for this
        # store, without altering the file or replacing the real writer/Entry.
        path_type = type(w.db_path)
        original_exists = path_type.exists
        def exists(path):
            actual = original_exists(path)
            if path is w.db_path and not path_gate:
                path_gate.append(actual)
                return False
            return actual

        observed = None
        with ExitStack() as stack:
            if writer:
                stack.enter_context(patch.object(path_type, 'exists', exists))
            if fail_at == 'postcheck':
                stack.enter_context(patch.object(EntryBudget, 'check', check_observed))
            stack.enter_context(patch.object(cls, 'execute', execute_observed))
            stack.enter_context(patch.object(cls, 'rollback', rollback))
            stack.enter_context(patch.object(cls, 'close', close))
            try:
                result = w.initialize()
            except BaseException as error:
                observed = error
        self.assertEqual(path_gate, [True] if writer else [])
        self.assertEqual(len(targets), 1)
        db, native, args, kwargs, active, owner, entry, db_entry = targets[0]
        self.assertIs(type(native), sqlite3.Cursor)
        self.assertEqual((args, kwargs, active), (((),), {}, True))
        self.assertIsNotNone(entry)
        self.assertIs(db_entry, entry)
        self.assertIs(owner, entry._lifecycle_value_owner)
        self.assertEqual(begins, [(db, 'BEGIN IMMEDIATE' if writer else 'BEGIN', entry)])
        prefix = ['execute-return', 'fetch']
        if fail_at == 'fetch':
            prefix.append(('native-row', native, (1,)))
        if fail_at == 'postcheck':
            prefix.extend(['fetch-return', 'postcheck'])
        prefix.append('cursor-close')
        if fail_at is None:
            self.assertIsNone(observed)
            self.assertEqual(result['revision'], self.raw()['revision'])
            self.assertIsNone(owner.first_failure)
            self.assertEqual(owner.control_errors, [])
            self.assertEqual(events, prefix + ['connection-close'])
        else:
            primary = a if fail_at in ('fetch', 'postcheck') else close_fault
            self.assertIs(observed, primary)
            first = owner.first_failure
            self.assertIs(first[0], primary)
            self.assertIs(first[1], owner.traceback)
            self.assertIs(first[1].tb_frame.f_code, scheduler._lc_cold1158_p04_catalog.__code__)
            self.assertEqual(events, prefix + [('rollback', True, False), 'connection-close'])
            if fail_at in ('fetch', 'postcheck'):
                injected_error, injected_tb, saved_context, saved_cause = caught['injected']
                self.assertIs(injected_error, a)
                self.assertIs(first[1].tb_next.tb_next if fail_at == 'fetch' else first[1].tb_next, injected_tb)
                if fail_at == 'postcheck':
                    self.assertEqual(first[1].tb_lineno, post_line)
                    self.assertIs(injected_tb.tb_frame.f_code, check_observed.__code__)
                self.assertIs(first[2], saved_context)
                self.assertIs(first[3], saved_cause)
                self.assertIs(saved_context, context)
                self.assertIs(saved_cause, cause)
                self.assertTrue(BaseException.__suppress_context__.__get__(observed))
            self.assertIs(BaseException.__context__.__get__(observed), first[2])
            self.assertIs(BaseException.__cause__.__get__(observed), first[3])
            live = BaseException.__traceback__.__get__(observed)
            while live is not None and live is not first[1]:
                live = live.tb_next
            self.assertIs(live, first[1])
            expected = [(close_fault, '_lc_cold1158_p04_catalog')] if fail_at in ('fetch', 'postcheck') else []
            expected.extend([(b, '_lc_cold1136_connection'), (c, '_lc_cold1136_connection')])
            self.assertEqual(len(owner.control_errors), len(expected))
            for (error, tb, ctx, why), (wanted, frame) in zip(owner.control_errors, expected):
                self.assertIs(error, wanted)
                self.assertEqual(tb.tb_frame.f_code.co_name, frame)
                self.assertIs(tb, BaseException.__traceback__.__get__(error))
                self.assertIs(ctx, BaseException.__context__.__get__(error))
                self.assertIs(why, BaseException.__cause__.__get__(error))
            self.assertTrue(owner.failed and owner.memory.failed)
        self.assertEqual(events.count('cursor-close'), 1)
        with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed database'):
            sqlite3.Connection.execute(db, 'SELECT 1')
        self.assertIsNone(w._scoped_entry_budget())
        self.assertIsNone(scheduler._lc_adm1136_budget_context.get())
        self.assertIsNone(world_module._lc_cold1136_p04_db.get())
        self.unchanged(before)
        self.budget()
        if fail_at == 'postcheck':
            for records in (fetch_returns, checks, cursor_closes, rollbacks, connection_closes, pre_rollback_cursor):
                self.assertEqual(len(records), 1)
            self.assertIn('closed cursor', str(pre_rollback_cursor[0]))
            return (db, native, entry, checks[0], first, caught['injected'],
                    cursor_closes[0], rollbacks[0], connection_closes[0])

    def test_cold1164_p04_catalog_native_cursor_fetch_first_survives_cleanup(self):
        self._cold1164_p04_catalog_fetch()

    def test_cold1164_p04_catalog_native_cursor_fetch_first_survives_writer_cleanup(self):
        self._cold1164_p04_catalog_fetch(writer=True)

    def test_cold1164_p04_catalog_cursor_close_as_primary_survives_cleanup(self):
        self._cold1164_p04_catalog_fetch(fail_at='close')

    def test_cold1164_p04_catalog_cursor_normal_close(self):
        self._cold1164_p04_catalog_fetch(fail_at=None)

    def test_cold1169_p04_catalog_postcheck_first_survives_cleanup(self):
        expected = self._cold1164_p04_catalog_fetch(fail_at='postcheck')
        db, cursor, entry, check, first, injected, close_result, rollback_result, connection_result = expected
        self.assertIs(check[0], db)
        self.assertIs(check[1], cursor)
        self.assertIs(check[2], entry)
        self.assertEqual((check[3], check[4]), ((1,), None))
        for actual, saved in zip((first[0], first[1].tb_next, first[2], first[3]), injected, strict=True):
            self.assertIs(actual, saved)
        self.assertEqual(close_result, (cursor, None))
        self.assertEqual(rollback_result, (db, True, None, False))
        self.assertEqual(connection_result, (db, None))

    def test_cold1169_p04_native_execute_a_survives_restore_record_failure(self):
        saved = self._cold1158_p04_execute_first('catalog', restore_case='deny', fail_record=True)
        primary, native, restore, missing, record_fault, closed, close_returned, saved_tail = saved
        self.assertIs(primary, native)
        self.assertIsNot(primary, restore[0])
        self.assertIs(restore[2], native)
        self.assertIs(missing, record_fault)
        self.assertIsNone(closed)
        self.assertFalse(close_returned)
        self.assertEqual(saved_tail, [])

    def test_cold1169_p04_first_restore_survives_record_failure_and_cursor_closes(self):
        saved = self._cold1158_p04_execute_first('catalog', restore_case='cursor', fail_record=True)
        primary, native, restore, missing, record_fault, closed, close_returned, saved_tail = saved
        self.assertIsNone(native)
        self.assertIs(primary, restore[0])
        self.assertIs(missing, record_fault)
        self.assertIs(type(closed), sqlite3.Cursor)
        self.assertTrue(close_returned)
        self.assertEqual([row[0] for row in saved_tail], ['unhanded-cursor-close'])
        self.assertIs(saved_tail[0][3], primary)

    def test_cold1174_p04_same_recorder_error_keeps_first_relationships(self):
        causes = (LookupError('controlled first append cause'),
                  LookupError('controlled second append cause'))
        attempts = []

        def reject(row, recorder_error):
            # Both failures are synthetic Python sink failures, not SQLite faults.
            try:
                raise recorder_error from causes[len(attempts)]
            except MemoryError as error:
                attempts.append((row, error, BaseException.__traceback__.__get__(error),
                                 BaseException.__context__.__get__(error),
                                 BaseException.__cause__.__get__(error)))
                raise

        saved, first, secondary, overflow, complete = self._cold1158_p04_execute_first(
            'catalog', restore_case='cursor', fail_record=True, record_probe=reject)
        primary, native, restore, missing, recorder_error, cursor, closed, ledger = saved
        # The real fixture already checks the same native cursor is closed before
        # sqlite3.Connection.rollback, the transaction goes True -> False, and the
        # original connection closes. Neither sink rejection can substitute for them.
        self.assertIsNone(native)
        self.assertIs(primary, restore[0])
        self.assertIs(type(cursor), sqlite3.Cursor)
        self.assertIs(closed, True)
        self.assertEqual(ledger, [])
        self.assertIs(complete, False)
        self.assertIs(overflow, False)
        self.assertEqual([attempt[0][0] for attempt in attempts],
                         ['authorizer-restore', 'unhanded-cursor-close'])
        self.assertIs(attempts[0][0][1], primary)
        self.assertIs(attempts[1][0][3], primary)
        self.assertIsNot(attempts[1][0][1], primary)
        self.assertIs(missing, recorder_error)

        for index, (kind, error, relations) in enumerate((first, secondary)):
            row, at_sink, sink_tb, sink_context, sink_cause = attempts[index]
            self.assertEqual(kind, row[0])
            self.assertIs(error, recorder_error)
            self.assertIs(error, at_sink)
            self.assertIsNotNone(relations)
            tb, context, cause = relations
            self.assertEqual(tb.tb_frame.f_code.co_name, 'record')
            self.assertEqual(tb.tb_next.tb_frame.f_code.co_name, 'append')
            self.assertIs(tb.tb_next.tb_next, sink_tb)
            self.assertIs(context, sink_context)
            self.assertIs(context, row[1])
            self.assertIs(cause, sink_cause)
            self.assertIs(cause, causes[index])

        for first_value, secondary_value in zip(first[2], secondary[2], strict=True):
            self.assertIsNot(first_value, secondary_value)
        for descriptor, first_value, second_value in zip(
                (BaseException.__traceback__, BaseException.__context__, BaseException.__cause__),
                first[2], secondary[2], strict=True):
            live = descriptor.__get__(recorder_error)
            self.assertIs(live, second_value)
            self.assertIsNot(live, first_value)

    def _cold1146_json_case(self, mode):
        pack = product_cli.SCENARIOS / 'iron-4.json'
        original = self.root / 'cold-json-producer.sqlite'
        expected = WorldStore(original, scenario_path=pack).initialize()
        with closing(sqlite3.connect(str(original))) as source:
            raw = source.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0]
        path = self.root / 'cold-json-consumer.sqlite'
        bad = '{' if mode.startswith('first') else raw + ' \n extra'
        if mode == 'ordinary':
            # Private unknown extension: ordinary stdlib last-wins/NaN/escape domain.
            edited = '{"_cold1146_extra":null,"_cold1146_extra":[NaN,Infinity,-Infinity,"\\ud800"],' + raw[1:]
            rows = ((1, edited),)
        elif mode.startswith('first'):
            rows = ((1, bad), (2, raw))
        else:
            rows = ((1, raw), (2, bad))
        # Deliberate separately owned replica; no edits to producer or triggers.
        with closing(sqlite3.connect(str(path))) as db:
            db.execute('CREATE TABLE snapshots(seq INTEGER PRIMARY KEY,state TEXT NOT NULL)').close()
            db.executemany('INSERT INTO snapshots VALUES(?,?)', rows).close()
            db.commit()
        # Successful cold open with military requires the real Geo storage from producer.
        # For ordinary success instead append a new snapshot to the complete producer.
        if mode == 'ordinary':
            with closing(sqlite3.connect(str(original))) as db:
                db.execute('INSERT INTO snapshots VALUES(?,?)', (2, edited)).close()
                db.commit()
            self.assertEqual(WorldStore(original, scenario_path=pack).initialize(), expected)
        else:
            with self.assertRaises(json.JSONDecodeError) as reference:
                json.loads(bad)
            explicit = None if mode == 'first_implicit' else pack
            with self.assertRaises(json.JSONDecodeError) as observed:
                WorldStore(path, scenario_path=explicit).initialize()
            a, b = observed.exception, reference.exception
            self.assertIs(type(a), type(b))
            self.assertEqual((a.args, a.msg, a.doc, a.pos, a.lineno, a.colno),
                             (b.args, b.msg, b.doc, b.pos, b.lineno, b.colno))
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.assertIsNone(scheduler._lc_adm1136_budget_context.get())
        self.budget()

    def test_cold1146_bad_first_explicit_json(self):
        self._cold1146_json_case('first_explicit')

    def test_cold1146_bad_first_implicit_json(self):
        self._cold1146_json_case('first_implicit')

    def test_cold1146_bad_latest_json(self):
        self._cold1146_json_case('latest')

    def test_cold1146_ordinary_legacy_json(self):
        self._cold1146_json_case('ordinary')

    def test_cold1146_legacy_p04_paid_before_fetch_and_decode(self):
        self.legacy()
        self.idle()
        before = self.oracle()
        w = WorldStore(self.owned / 'world.sqlite', scenario_path=self.owned / 'scenario.json')
        expected = self.session.world.snapshot()
        execute, decode = world_module._AttemptConnection.execute, json.loads
        fetched, parsed = [], []

        class ObserveCursor:
            def __init__(observer, cursor, role):
                observer.cursor, observer.role = cursor, role

            def __getattr__(observer, name):
                return getattr(observer.cursor, name)

            def fetchone(observer):
                record(observer.role, 'one')
                return observer.cursor.fetchone()

            def fetchall(observer):
                record(observer.role, 'all')
                return observer.cursor.fetchall()

        def roles():
            active = scheduler._lifecycle_value_context.get()
            return () if active is None else tuple(lease.role for lease in active.leases)

        def record(role, kind):
            fetched.append((role, kind, roles().count('cold1146-p04-raw:' + role)))

        def execute_observed(db, sql, *args, **kwargs):
            cursor = execute(db, sql, *args, **kwargs)
            for table in ('coordinator_windows', 'snapshots', 'intents', 'dm_decisions'):
                if sql.startswith('SELECT ') and ' FROM main.' + table in sql and ' FROM (' not in sql:
                    return ObserveCursor(cursor, table)
            return cursor

        def decode_observed(raw, *args, **kwargs):
            active = roles()
            if active and type(raw) is str:
                parsed.append((raw, active.count('cold1146-plain-json-value')))
            return decode(raw, *args, **kwargs)

        with patch.object(world_module._AttemptConnection, 'execute', execute_observed), \
                patch.object(json, 'loads', decode_observed):
            actual = w.initialize()
        self.assertEqual(actual, expected)
        window_fetches = [x for x in fetched if x[0] == 'coordinator_windows']
        self.assertEqual(window_fetches, [('coordinator_windows', 'all', n)
                         for n in range(1, 3 if 'military' in expected else 2)])
        self.assertGreaterEqual(len([x for x in fetched if x[0] == 'snapshots']), 3)
        for table in {t for t, _, _ in fetched}:
            self.assertEqual([n for t, _, n in fetched if t == table],
                             list(range(1, 1 + sum(t == table for t, _, _ in fetched))))
        self.assertTrue(any(table == 'intents' for table, _, _ in fetched))
        for row in self.rows('coordinator_windows'):
            self.assertTrue(any(raw == row['roster_json'] and paid > (parsed[i-1][1] if i else 0)
                                for i, (raw, paid) in enumerate(parsed)))
        self.unchanged(before)
        self.budget()

    def test_cold1146_legacy_opening_order_before_historical_json(self):
        self.legacy()
        self.idle()
        before = self.oracle()
        with closing(sqlite3.connect((self.owned / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            schemas = [db.execute('SELECT sql FROM sqlite_master WHERE name=?', (name,)).fetchone()[0]
                       for name in ('snapshots', 'coordinator_windows')]
            snapshots = db.execute('SELECT seq,state FROM snapshots ORDER BY seq').fetchall()
            cursor = db.execute('SELECT * FROM coordinator_windows ORDER BY ordinal')
            names = [item[0] for item in cursor.description]
            windows = cursor.fetchall()
            cursor.close()
        row = list(windows[0])
        row[names.index('ordinal')] = 99  # first opening-order check must win
        row[names.index('roster_json')] = '['
        damaged_seq = row[names.index('advanced_revision')]
        self.assertNotIn(damaged_seq, (snapshots[0][0], snapshots[-1][0]))
        path = self.root / 'cold-opening-order.sqlite'
        with closing(sqlite3.connect(str(path))) as db:
            for sql in schemas:
                db.execute(sql).close()
            db.executemany('INSERT INTO snapshots VALUES(?,?)',
                           [(seq, '{' if seq == damaged_seq else raw) for seq, raw in snapshots]).close()
            db.execute('INSERT INTO coordinator_windows VALUES (' + ','.join('?' for _ in row) + ')', row).close()
            db.commit()
        decode = json.loads
        parsed = []

        def observed(raw, *args, **kwargs):
            parsed.append(raw)
            return decode(raw, *args, **kwargs)

        with patch.object(json, 'loads', observed):
            with self.assertRaises(WorldError) as failure:
                WorldStore(path).initialize()
        self.assertEqual(failure.exception.code, 'INVALID_EVENT_WAKEUP')
        self.assertEqual(str(failure.exception.__context__), 'opening order')
        self.assertNotIn('{', parsed)
        self.assertNotIn('[', parsed)
        self.unchanged(before)
        self.budget()

    def adm1148_first_root_floor_case(self, *, fenced):
        from app.coordinator_execution import CoordinatorAttempt

        # Use the real producer and permit; omit only background renewals so
        # the deliberately damaged fixture cannot race the heartbeat.
        with patch.object(CoordinatorAttempt, 'start', autospec=True) as start:
            self.build()
        self.assertEqual(self.step()['time'], 0)
        w = self.session.world
        root = self.session.coordinator._owner_attempt
        permit = root.permit
        start.assert_called_once()
        self.assertIs(start.call_args.args[0], root)
        self.assertIsNone(root.thread)
        self.assertEqual(permit.world_id, self.world_id)
        root.check()
        before = self.oracle()
        floor = 'x' * 600_000  # TEXT, not a digit string that affinity can coerce.
        key = (permit.world_id, permit.epoch)
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            with db:
                self.assertEqual(db.execute('UPDATE execution_clock SET high_water_wall_ns=? '
                    'WHERE singleton=1', (floor,)).rowcount, 1)
                if fenced:
                    self.assertEqual(db.execute('UPDATE coordinator_execution_leases SET owner_token=NULL '
                        'WHERE world_id=? AND epoch=? AND owner_token=?', (*key, permit.owner_token)).rowcount, 1)
            try:
                damaged = self.oracle()
                tables = damaged['main']['tables']
                self.assertEqual(tables['execution_clock'][0]['high_water_wall_ns'], floor)
                row = next(r for r in tables['coordinator_execution_leases'] if r['world_id'] == self.world_id)
                self.assertEqual(row['epoch'], permit.epoch)
                if fenced:
                    self.assertIsNone(row['owner_token'])
                else:
                    self.assertTrue(row['owner_token'] == permit.owner_token)
                expected = WorldError if fenced else scheduler._LifecycleLiveLimit
                with patch.object(w, '_execution_clock_ns', wraps=w._execution_clock_ns) as clock:
                    with self.assertRaises(expected) as caught:
                        with self.proof_entry() as entry:
                            parent = entry._lifecycle_value_owner
                            with scheduler._lc_ro1127_proof_scope(w, entry, self.session.host):
                                self.fail('first readonly prefix unexpectedly yielded')
                proof = parent.control_errors[0]
                self.assertIs(proof.root, root)
                self.assertIs(proof.capsule.budget, entry)
                self.assertIsNotNone(proof.capsule.vector)  # genuine catalog/coverage pin reached
                self.assertIs(proof.terminal[0], caught.exception)
                self.assertFalse(proof.armed)
                self.assertTrue(proof.ended)
                self.assertIsNone(scheduler._lc_ro1127_context.get())
                if fenced:
                    self.assertEqual(caught.exception.code, 'COORDINATOR_EXECUTION_FENCED')
                    clock.assert_not_called()
                else:
                    clock.assert_called_once_with()  # floor quote reached only after clock input
                    self.assertTrue(parent.memory.failed)
                self.assertEqual(self.oracle(), damaged)
            finally:
                # Restore only the injected bytes before standard first close/CAS.
                with db:
                    original = before['main']['tables']['execution_clock'][0]['high_water_wall_ns']
                    self.assertEqual(db.execute('UPDATE execution_clock SET high_water_wall_ns=? '
                        'WHERE singleton=1', (original,)).rowcount, 1)
                    if fenced:
                        self.assertEqual(db.execute('UPDATE coordinator_execution_leases SET owner_token=? '
                            'WHERE world_id=? AND epoch=? AND owner_token IS NULL', (permit.owner_token, *key)).rowcount, 1)
        self.assertEqual(self.oracle(), before)
        self.budget()

    def test_adm1148_first_root_fenced_skips_oversized_floor(self):
        self.adm1148_first_root_floor_case(fenced=True)

    def test_adm1148_first_root_live_prepays_visited_oversized_floor(self):
        self.adm1148_first_root_floor_case(fenced=False)

    def adm1154_first_root_failure(self, expected, *, patch_edge=None):
        w, host = self.session.world, self.session.host
        root = self.session.coordinator._owner_attempt
        try:
            with self.proof_entry() as entry:
                parent = entry._lifecycle_value_owner
                memory = parent.memory
                try:
                    with ExitStack() as stack:
                        if patch_edge is not None:
                            stack.enter_context(patch_edge)
                        with scheduler._lc_ro1127_proof_scope(w, entry, host):
                            self.fail('first readonly Root unexpectedly yielded')
                finally:
                    # Observe before Entry exits; neither catch nor clear A here.
                    poisoned_before_entry_exit = memory.failed
        except expected as observed:
            # assertRaises would clear the live traceback on the same object.
            error = observed
            outer_tb = BaseException.__traceback__.__get__(observed)
        else:
            self.fail('first readonly Root did not propagate through its Entry')

        self.assertEqual(len(parent.control_errors), 1)
        proof = parent.control_errors[0]
        self.assertIs(proof.root, root)
        self.assertIs(proof.entry, entry)
        self.assertIs(proof.capsule.budget, entry)
        self.assertIs(entry._lifecycle_value_owner, parent)
        self.assertIs(proof.parent, parent)
        self.assertIs(proof.memory, memory)
        self.assertIs(proof.owner.memory, memory)
        self.assertIs(parent.memory, memory)
        self.assertIsNotNone(proof.capsule.vector)
        first, first_tb, first_context, first_cause = proof.first
        terminal, terminal_tb, terminal_context, terminal_cause = proof.terminal
        self.assertIs(error, first)
        self.assertIs(error, terminal)
        self.assertIs(error, proof.owner.error)
        self.assertIs(error, parent.error)
        self.assertIs(terminal_context, first_context)
        self.assertIs(terminal_cause, first_cause)
        self.assertIs(BaseException.__context__.__get__(error), first_context)
        self.assertIs(BaseException.__cause__.__get__(error), first_cause)
        self.assertIs(parent.error_context, first_context)
        self.assertIs(parent.error_cause, first_cause)
        self.assertIsNotNone(first_tb)
        self.assertIsNotNone(terminal_tb)
        self.assertIsNotNone(outer_tb)
        for head in (outer_tb, parent.traceback):
            tail = head
            while tail is not None and tail is not terminal_tb:
                self.assertIsNot(tail.tb_frame.f_code, scheduler._LifecycleValueOwner.handoff.__code__)
                tail = tail.tb_next
            self.assertIs(tail, terminal_tb)
        codes = []
        tb = terminal_tb
        saw_first = False
        while tb is not None:
            saw_first |= tb is first_tb
            codes.append(tb.tb_frame.f_code)
            tb = tb.tb_next
        self.assertTrue(saw_first)
        self.assertNotIn(scheduler._LifecycleValueOwner.handoff.__code__, codes)
        self.assertTrue(poisoned_before_entry_exit)
        self.assertTrue(memory.failed)
        self.assertTrue(proof.failed)
        self.assertTrue(proof.owner.failed)
        self.assertTrue(parent.failed)
        self.assertFalse(parent.closed)
        self.assertIsNone(parent.destination)
        self.assertIsNone(proof.owner.destination)
        self.assertFalse(proof.armed)
        self.assertTrue(proof.ended)
        self.assertIsNone(w._scoped_entry_budget())
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.assertIsNone(scheduler._lifecycle_live_context.get())
        self.assertIsNone(scheduler._lc_ro1127_context.get())
        return error, codes

    def adm1154_boot_root(self):
        from app.coordinator_execution import CoordinatorAttempt
        with patch.object(CoordinatorAttempt, 'start', autospec=True) as start:
            self.build()
        self.assertEqual(self.step()['time'], 0)
        root = self.session.coordinator._owner_attempt
        start.assert_called_once()
        args, kwargs = start.call_args
        self.assertEqual(kwargs, {})
        self.assertEqual(len(args), 2)
        self.assertIs(args[0], root)
        self.assertTrue(callable(args[1]))  # The original cancel-children callback; do not invoke it.
        self.assertIsNone(root.thread)
        root.check()
        return root

    def adm1154_genesis_case(self, *, malformed):
        root = self.adm1154_boot_root()
        permit = root.permit
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            seq, original = db.execute('SELECT seq,state FROM snapshots ORDER BY seq LIMIT 1').fetchone()
            key = (permit.world_id, permit.epoch, permit.owner_token)
            prefix = '{"world_id":' + json.dumps(self.world_id) + ',"ignored":"汉",'
            raw = prefix + ('"extra":[0,]}' if malformed else '"extra":1e999}')
            if not malformed:
                self.assertEqual(json.loads(raw)['extra'], float('inf'))
            with db:
                db.execute('INSERT INTO snapshots VALUES (?,?)', (seq - 1, raw)).close()
                self.assertEqual(db.execute('UPDATE coordinator_execution_leases SET owner_token=NULL '
                    'WHERE world_id=? AND epoch=? AND owner_token=?', key).rowcount, 1)
            try:
                before = self.oracle()
                with patch.object(self.session.world, '_execution_clock_ns',
                                  wraps=self.session.world._execution_clock_ns) as clock:
                    if malformed:
                        with self.assertRaises(json.JSONDecodeError) as original_error:
                            json.loads(raw)
                        error, codes = self.adm1154_first_root_failure(json.JSONDecodeError)
                        expected = original_error.exception
                        self.assertIs(type(error), type(expected))
                        self.assertEqual(error.args, expected.args)
                        self.assertEqual((error.msg, error.doc, error.pos, error.lineno, error.colno),
                            (expected.msg, expected.doc, expected.pos, expected.lineno, expected.colno))
                        self.assertNotIn(scheduler._lc_adm1148_root_json.__code__, codes)
                        self.assertIn(json.loads.__code__, codes)
                    else:
                        error, codes = self.adm1154_first_root_failure(WorldError)
                        self.assertEqual(error.code, 'COORDINATOR_EXECUTION_FENCED')
                        self.assertIn(WorldStore._check_coordinator_execution.__code__, codes)
                    clock.assert_not_called()
                self.assertEqual(self.oracle(), before)
            finally:
                # The owned fixture remains append-only. An earlier good copy
                # restores the original first-state consumer for teardown; it
                # does not delete the damaged row or claim epoch restoration.
                with db:
                    db.execute('INSERT INTO snapshots VALUES (?,?)', (seq - 2, original)).close()
                    self.assertEqual(db.execute('UPDATE coordinator_execution_leases SET owner_token=? '
                        'WHERE world_id=? AND epoch=? AND owner_token IS NULL',
                        (permit.owner_token, permit.world_id, permit.epoch)).rowcount, 1)
        self.budget()

    def test_adm1154_first_root_ignored_nonfinite_keeps_fence(self):
        self.adm1154_genesis_case(malformed=False)

    def test_adm1154_first_root_malformed_uses_original_json_error(self):
        self.adm1154_genesis_case(malformed=True)

    def adm1154_bad_binding(self, target, name, edge, *, static=False):
        self.adm1154_boot_root()
        before = self.oracle()
        called = []
        def replacement(*args, **kwargs):
            called.append(name)
            raise AssertionError('substituted first Root callee executed')
        if target == 'instance':
            from types import MethodType
            fault = patch.object(self.session.world, name, MethodType(replacement, self.session.world))
        elif target == 'descriptor':
            fault = patch.object(WorldStore, name, staticmethod(replacement) if static else replacement)
        else:
            fault = patch.object(scheduler, name, replacement)
        error, codes = self.adm1154_first_root_failure(WorldError, patch_edge=fault)
        self.assertEqual(error.code, 'WINDOW_RECOVERY_UNPROVEN')
        self.assertIn('first readonly Root: ' + edge, str(error))
        self.assertIn(WorldStore._check_coordinator_execution.__code__, codes)
        if edge == 'genesis quote':
            self.assertIn(WorldStore._execution_identity.__code__, codes)
        self.assertEqual(called, [])
        self.assertEqual(self.oracle(), before)
        self.budget()

    def test_adm1154_first_root_identity_descriptor_source_mismatch(self):
        self.adm1154_bad_binding('descriptor', '_execution_identity', '_execution_identity', static=True)

    def test_adm1154_first_root_clock_instance_source_mismatch(self):
        self.adm1154_bad_binding('instance', '_execution_now', '_execution_now')

    def test_adm1154_first_root_pay_source_mismatch(self):
        self.adm1154_bad_binding('helper', '_lc_adm1148_root_pay', 'identity quote')

    def test_adm1154_first_root_json_quote_source_mismatch(self):
        self.adm1154_bad_binding('helper', '_lc_adm1148_root_json', 'genesis quote')

    def test_adm1159r_first_root_genesis_reserve_descriptor_source_mismatch(self):
        root = self.adm1154_boot_root()
        before = self.oracle()
        connection = world_module._AttemptConnection
        owner_type = scheduler._LifecycleValueOwner
        execute = vars(connection)['execute']
        reserve = vars(owner_type)['reserve']
        quote = scheduler._lc_adm1148_root_json
        gate = scheduler._lc_adm1154_root_gate
        raw_role = 'adm1136-first-root-raw-and-json'
        json_role = 'adm1154-first-root-original-json'
        landed, called = [], []

        def replacement(owner, amount, role):
            called.append(role)
            raise AssertionError('substituted genesis reserve executed')

        @contextmanager
        def inject_at_genesis():
            # Both patches unwind inside the original Entry; neither handles A.
            with ExitStack() as fault:
                def execute_then_inject(db, *args, **kwargs):
                    cursor = execute(db, *args, **kwargs)
                    proof = scheduler._lc_ro1127_context.get()
                    if (proof is not None and not proof.armed and not proof.ended
                            and proof.capsule.vector is not None and db._attempt is proof.entry
                            and db.in_transaction and not kwargs
                            and args == ('SELECT state FROM snapshots ORDER BY seq LIMIT 1',)):
                        owner, memory = proof.owner, proof.memory
                        prefix = tuple(lease for lease in owner.leases if lease.role == raw_role)
                        landed.append((proof, owner, memory, db._attempt, proof.capsule.vector, prefix))
                        fault.enter_context(patch.object(owner_type, 'reserve', replacement))
                    return cursor
                fault.enter_context(patch.object(connection, 'execute', execute_then_inject))
                yield

        error, codes = self.adm1154_first_root_failure(WorldError, patch_edge=inject_at_genesis())
        self.assertIs(vars(owner_type)['reserve'], reserve)
        self.assertIs(vars(connection)['execute'], execute)
        self.assertEqual(len(landed), 1)
        proof, owner, memory, entry, vector, prefix = landed[0]
        self.assertIs(proof.root, root)
        self.assertIs(proof.terminal[0], error)
        self.assertIs(type(owner), owner_type)
        self.assertIs(proof.owner, owner)
        self.assertIs(proof.memory, memory)
        self.assertIs(owner.memory, memory)
        self.assertIs(proof.entry, entry)
        self.assertIs(proof.capsule.vector, vector)
        self.assertEqual(error.code, 'WINDOW_RECOVERY_UNPROVEN')
        self.assertIn('first readonly Root: genesis reserve', str(error))
        self.assertIn(WorldStore._check_coordinator_execution.__code__, codes)
        self.assertIn(WorldStore._execution_identity.__code__, codes)
        self.assertEqual(codes.count(quote.__code__), 1)
        self.assertEqual(codes[-2:], [quote.__code__, gate.__code__])
        leaf = proof.terminal[1]
        while leaf.tb_next is not None:
            leaf = leaf.tb_next
        edge = leaf.tb_frame.f_locals
        self.assertEqual(edge['edge'], 'genesis reserve')
        self.assertEqual(edge['name'], '_LifecycleValueOwner.reserve')
        self.assertIs(edge['actual'], owner)
        self.assertIs(edge['descriptor'], replacement)
        # The prefix and native-row quote already paid on this ledger. The
        # actual genesis JSON reserve call and decode are both beyond the gate.
        self.assertEqual(len(prefix), 2)
        fixed, raw = prefix
        self.assertEqual(fixed.charge, 32768 + 512)
        self.assertGreaterEqual(raw.charge, 8192 + 512)
        self.assertLess(fixed.ticket, raw.ticket)
        for lease in prefix:
            self.assertIs(lease.ledger, memory)
            self.assertTrue(any(candidate is lease for candidate in owner.leases))
            self.assertFalse(lease.released)
            self.assertEqual(memory.leases[lease.ticket], lease.charge)
        self.assertNotIn(json_role, [lease.role for lease in owner.leases])
        self.assertNotIn(reserve.__code__, codes)
        self.assertNotIn(json.loads.__code__, codes)
        self.assertEqual(called, [])
        self.assertEqual(self.oracle(), before)
        self.budget()

    def test_adm1170_first_root_genesis_json_exact_debit_before_original_loads(self):
        import re
        import sys

        root = self.adm1154_boot_root()
        w, host = self.session.world, self.session.host
        before = self.oracle()
        genesis = before['main']['tables']['snapshots'][0]
        self.assertEqual(genesis['seq'], 1)
        native_raw = genesis['state']
        self.assertIs(type(native_raw), str)
        self.assertEqual(json.loads(native_raw)['world_id'], root.permit.world_id)

        # Lex complete JSON string tokens atomically (including escapes), then
        # classify only unquoted delimiters. Quote decoded TEXT by code points;
        # the earlier native Row allowance instead uses storage UTF-8 bytes.
        tokens = re.compile(r'"(?:\\[\s\S]|[^"\\])*"|[,:{\[]')
        entries = opens = 0
        for match in tokens.finditer(native_raw):
            if native_raw[match.start()] in '{[':
                opens += 1
            else:
                entries += 1
        payload = 8192 + 16 * len(native_raw) + 512 * entries + 1536 * opens
        overhead = 512
        expected_charge = payload + overhead
        raw_role = 'adm1136-first-root-raw-and-json'
        json_role = 'adm1154-first-root-original-json'
        sql = 'SELECT state FROM snapshots ORDER BY seq LIMIT 1'

        identity = WorldStore._execution_identity
        execute = world_module._AttemptConnection.execute
        quote = scheduler._lc_adm1148_root_json
        reserve = scheduler._LifecycleValueOwner.reserve
        hold = scheduler._LifecycleLiveLedger.hold
        loads, loads_code, loads_globals = world_module._ROOT1154_LOADS
        self.assertIs(loads, json.loads)
        self.assertIs(loads_code, loads.__code__)
        self.assertIs(loads_globals, loads.__globals__)
        events = []
        cursor = None

        def at(frame, function):
            return (frame is not None and frame.f_code is function.__code__
                    and frame.f_globals is function.__globals__)

        def sample(proof, db):
            owner, memory = proof.owner, proof.memory
            return dict(proof=proof, owner=owner, memory=memory, db=db,
                        entry=db._attempt, vector=proof.capsule.vector,
                        transaction=db.in_transaction, unarmed=not proof.armed and not proof.ended,
                        current_owner=scheduler._lifecycle_value_context.get(),
                        current_memory=scheduler._lifecycle_live_context.get(),
                        leases=tuple(owner.leases), ticket=memory.next_ticket, live=memory.live,
                        held=tuple((lease, lease.released, memory.leases.get(lease.ticket))
                                   for lease in owner.leases if lease.role in (raw_role, json_role)))

        def observe(frame, event, arg):
            nonlocal cursor
            caller = frame.f_back
            if event == 'return' and at(frame, execute) and at(caller, identity):
                local = frame.f_locals
                if local['args'] == (sql,) and not local['kwargs']:
                    proof = scheduler._lc_ro1127_context.get()
                    if proof is not None and caller.f_locals['_first_root_gate'] is not None:
                        cursor = arg   # Real return, not a cursor adapter.
                        events.append(('select', sample(proof, local['self'])))
            elif (event == 'c_return' and cursor is not None and at(frame, identity)
                    and getattr(arg, '__self__', None) is cursor and arg.__name__ == 'fetchone'):
                events.append(('fetchone', type(cursor), cursor.connection))
                cursor = None
            elif (event == 'return' and at(frame, hold) and at(caller, reserve)
                  and at(caller.f_back, quote) and at(caller.f_back.f_back, identity)):
                local = frame.f_locals
                events.append(('hold', caller.f_locals['self'], local['self'],
                               local['amount'], local['role'], arg))
            elif (event == 'call' and frame.f_code is loads_code and frame.f_globals is loads_globals
                  and at(caller, identity) and caller.f_locals['_first_root_gate'] is not None):
                local = caller.f_locals
                proof = scheduler._lc_ro1127_context.get()
                events.append(('loads', sample(proof, local['db']), local['row'],
                               local['loads'], frame.f_locals['s']))

        with self.proof_entry() as entry:
            parent = entry._lifecycle_value_owner
            memory = parent.memory
            with ExitStack() as stack:
                previous_profile = sys.getprofile()
                self.assertIsNone(previous_profile, 'Root debit observer requires a free thread profile slot')
                sys.setprofile(observe)
                try:
                    proof = stack.enter_context(scheduler._lc_ro1127_proof_scope(w, entry, host))
                finally:
                    sys.setprofile(previous_profile)
                    cursor = None
                self.assertTrue(proof.armed)
                self.assertIsNone(sys.getprofile())
                starts = [item for item in events if item[0] == 'select']
                decodes = [item for item in events if item[0] == 'loads']
                self.assertEqual(len(starts), 1)
                self.assertEqual(len(decodes), 1)
                _, start = starts[0]
                _, end, row, consumer, raw = decodes[0]
                self.assertIs(type(raw), str)
                self.assertIs(raw, row[0])
                self.assertEqual(raw, native_raw)
                self.assertIs(consumer, loads)
                self.assertIs(proof.root, root)
                self.assertIs(proof.entry, entry)
                self.assertIs(proof.capsule.budget, entry)
                self.assertIs(proof.parent, parent)
                self.assertIs(proof.memory, memory)
                self.assertIs(proof.owner.memory, memory)
                self.assertIsNot(proof.owner, parent)
                self.assertIsNotNone(proof.capsule.vector)
                for seen in (start, end):
                    self.assertIs(seen['proof'], proof)
                    self.assertIs(seen['owner'], proof.owner)
                    self.assertIs(seen['current_owner'], proof.owner)
                    self.assertIs(seen['memory'], memory)
                    self.assertIs(seen['current_memory'], memory)
                    self.assertIs(seen['entry'], entry)
                    self.assertIs(seen['vector'], proof.capsule.vector)
                    self.assertTrue(seen['transaction'] and seen['unarmed'])
                self.assertIs(start['db'], end['db'])
                self.assertIs(type(start['db']), world_module._AttemptConnection)

                prefix = [lease for lease in start['leases'] if lease.role == raw_role]
                self.assertEqual(len(prefix), 2)
                fixed, native = prefix
                self.assertEqual(fixed.charge, 32768 + overhead)
                self.assertEqual(native.charge, 8192 + 32 * len(native_raw.encode('utf-8')) + overhead)
                self.assertLess(fixed.ticket, native.ticket)
                self.assertEqual(native.ticket, start['ticket'])
                self.assertNotIn(json_role, [lease.role for lease in start['leases']])
                self.assertEqual(start['held'], ((fixed, False, fixed.charge), (native, False, native.charge)))
                charged = [lease for lease in end['leases'] if lease.role == json_role]
                self.assertEqual(len(charged), 1, 'missing or duplicate genesis JSON debit at original loads')
                paid, = charged
                self.assertEqual([item[0] for item in events], ['select', 'fetchone', 'hold', 'loads'])
                _, cursor_type, fetch_db = events[1]
                self.assertIs(cursor_type, sqlite3.Cursor)
                self.assertIs(fetch_db, start['db'])
                _, debit_owner, debit_memory, requested, role, receipt = events[2]
                self.assertIs(debit_owner, proof.owner)
                self.assertIs(debit_memory, memory)
                self.assertIs(receipt, paid)
                self.assertEqual(role, json_role)
                self.assertEqual(requested, payload, 'incorrect genesis JSON payload debit')
                self.assertEqual(paid.charge, expected_charge, 'incorrect genesis JSON debit including overhead')
                self.assertEqual(end['live'] - start['live'], expected_charge)
                self.assertEqual(end['ticket'], start['ticket'] + 1)
                self.assertEqual(paid.ticket, end['ticket'])
                self.assertEqual(len(end['leases']), len(start['leases']) + 1)
                for old, kept in zip(start['leases'], end['leases']):
                    self.assertIs(kept, old)
                self.assertIs(end['leases'][-1], paid)
                self.assertEqual(end['held'], (*start['held'], (paid, False, expected_charge)))
                for lease in (fixed, native, paid):
                    self.assertIs(lease.ledger, memory)
                    self.assertIs(lease.ro1127, proof)
                    self.assertFalse(lease.released)
                self.assertFalse(memory.failed)
            self.assertTrue(proof.ended)
            self.assertIs(proof.owner.destination, parent)
            self.assertTrue(any(lease is paid for lease in parent.leases))
            self.assertEqual(memory.leases[paid.ticket], expected_charge)
            self.assertFalse(paid.released)
        self.assertFalse(memory.failed)
        self.assertIsNone(w._scoped_entry_budget())
        self.assertIsNone(scheduler._lc_ro1127_context.get())
        self.assertIsNone(scheduler._lifecycle_value_context.get())
        self.assertIsNone(scheduler._lifecycle_live_context.get())
        self.assertEqual(self.oracle(), before)  # Heartbeat suppressed; no clock exception.
        self.budget()

    def tearDown(self):
        try:
            for host in self.extra_hosts:
                host.close()
                self.assertFalse(host.cleanup_pending)
            if self.session is not None:
                host, coordinator = self.session.host, self.session.coordinator
                owner = coordinator._owner_attempt
                worker = coordinator.worker
                self.session.close()
                self.assertFalse(host.cleanup_pending)
                self.assertFalse(coordinator.cleanup_pending)
                self.assertTrue(worker is None or not worker.is_alive())
                self.assertTrue(owner is None or owner.thread is None or not owner.thread.is_alive())
                self.assertFalse([t for t in host._pending_threads if t.is_alive()])
                self.assertIsNone(host._process)
                if self.generation is not None:
                    self.assertEqual(product_cli.verify_generation(), self.generation)
            self.assertEqual(self.violations, [])
            self.budget()
        finally:
            # Diagnostics only after original worker/maintenance close, not inside proof.
            (self.root / 'history1101-evidence.json').write_text(encoded(dict(
                method=self.id(), evidence=self.evidence, calls=self.calls,
                synthetic_transport=True, live_model=False,
                elapsed_seconds=time.monotonic() - self.started,
                runtime_claim='only if the entire method including cleanup returns')) + '\n', encoding='utf-8')
            self.guards.close()


class LifecycleHistoryColdBootstrap1183(unittest.TestCase):
    """Public product cold startup; no legacy() fixture, patches, or model calls."""

    def _material(self, path, names):
        # Observe only fresh, owned files. No connection spans a product call.
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('BEGIN')
            present = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            return {name: (sorted(db.execute('SELECT * FROM ' + name).fetchall(), key=repr)
                           if name in present else None) for name in names}

    def _cold_p04(self, edit):
        end = time.monotonic() + 120
        directory = (SCRATCH / ('parent1183-history-owned-' + uuid.uuid4().hex) / 'world').absolute()
        pack = product_cli.SCENARIOS / 'event-wakeup-v1-iron-4.json'
        raw = pack.read_bytes()
        manifest = json.loads(product_cli.MANIFEST.read_bytes())
        self.assertEqual(manifest['files']['app/data/scenarios/' + pack.name],
                         dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        geo_raw = (product_cli.ROOT / 'app/data/geography.json').read_bytes()
        geo_sha = hashlib.sha256(geo_raw).hexdigest()
        self.assertEqual(manifest['files']['app/data/geography.json'],
                         dict(bytes=len(geo_raw), sha256=geo_sha))
        self.assertEqual(geo_sha, 'c57b051cd37e8d866418b1b67ec7398c56997843498490676112c004270a687f')
        stream = io.StringIO()
        with redirect_stdout(stream):
            result = product_cli.main(['--data-dir', str(directory), 'init', '--scenario', str(pack)])
        self.assertEqual(result, 0)
        self.assertEqual((directory / 'scenario.json').read_bytes(), raw)
        public = product_cli.read_owned(directory)
        receipt = json.loads(stream.getvalue())
        self.assertEqual({k: receipt[k] for k in ('world_id', 'revision', 'status', 'time')},
                         {k: public[k] for k in ('world_id', 'revision', 'status', 'time')})
        self.assertEqual((encoded(public['revision']), encoded(public['time']), public['status']),
                         ('1', '0', 'user_paused'))
        self.assertIs(receipt['real_call_verified'], False)
        self.assertIs(receipt['real_model'], False)
        world_id = public['world_id']
        main_path, actor_path = (directory / name for name in ('world.sqlite', 'actors.sqlite'))
        lease = 'coordinator_execution_leases'
        registration = 'coordinator_scheduler_registration'
        metadata = {'execution_clock', lease, 'history_proof_epochs', 'history_proof_coverage'}
        business = tuple(name for name in MAIN if name not in metadata)
        private = tuple(name for name in PRIVATE if name not in metadata)
        world = WorldStore(main_path, scenario_path=directory / 'scenario.json')
        if edit:
            # The original pre-registration public producer, without a root row.
            self.assertIsNone(self._material(main_path, (lease,))[lease])
            original = next(p for p in public['persons'] if p['id'] == KING)
            changes = {'mission': '1183：在时间零核对正式账簿。'}
            public = world.edit_card(KING, changes)
            card = next(p for p in public['persons'] if p['id'] == KING)
            self.assertEqual(card['card'], {**original['card'], **changes})
            self.assertEqual(card['card_version'], original.get('card_version', 1) + 1)
            self.assertEqual(card['card_author'], 'user')
            self.assertEqual((public['events'][-1]['kind'], public['events'][-1]['person_id']),
                             ('card_edited', KING))
            self.assertEqual(encoded(public), encoded(product_cli.read_owned(directory)))
        revision = 2 if edit else 1
        self.assertEqual((encoded(public['revision']), encoded(public['time']), public['world_id']),
                         (str(revision), '0', world_id))
        self.assertFalse(actor_path.exists())
        before = self._material(main_path, business + (lease,))
        self.assertIsNone(before.pop(lease))
        self.assertIsNone(before[registration])
        self.assertEqual([r[0] for r in before['snapshots']], list(range(1, revision + 1)))
        for seq, raw_state in before['snapshots']:
            state = json.loads(raw_state)
            self.assertEqual((encoded(state['revision']), encoded(state['time'])), (str(seq), '0'))
            self.assertNotIn('_geography', state)
            self.assertNotIn('_lifecycle', state)
            self.assertEqual(state['_p04']['resident_tick_seconds'], 60)
        # Independent storage oracle, NOT a replacement for in-transaction restore.
        values = [row for row in before['world_constants'] if row[0] == 'geography']
        self.assertEqual(len(values), 1)
        _, value, digest = values[0]
        regions = json.loads(geo_raw)['regions']
        self.assertEqual(len(regions), 629)
        self.assertEqual(value, encoded({row['id']: row for row in regions}))
        self.assertEqual(digest, 'cc5b59d3e183b01c6587eb65a1057f90f52944065d77e1d0457e096d4a22aab9')
        self.assertEqual(hashlib.sha256(value.encode('utf-8')).hexdigest(), digest)

        self.assertLess(time.monotonic(), end)
        session = product_cli.open_session(directory, 'real')
        primary = None
        try:
            self.assertIs(type(session), product_cli.ProductSession)
            coordinator, host = session.coordinator, session.host
            owner = coordinator._owner_attempt
            permit, heartbeat = owner.permit, owner.thread
            worker = coordinator.worker
            self.assertIs(owner.world, session.world)
            self.assertIs(coordinator.actors, host)
            self.assertIs(host.world, session.world)
            self.assertIsNone(worker)
            self.assertIsNotNone(heartbeat)
            self.assertTrue(heartbeat.is_alive())
            self.assertIsNone(host._attempt)
            self.assertIsNone(host._process)
            self.assertTrue(actor_path.is_file())
            opened = self._material(main_path, business + (lease,))
            active = opened.pop(lease)
            self.assertEqual(len(active), 1)
            self.assertTrue(active[0][:3] == (world_id, permit.epoch, permit.owner_token),
                            'registration must use the original live root, not a successor')
            self.assertTrue(permit.world_id == world_id and bool(permit.owner_token))
            self.assertEqual(len(opened[registration]), 1)
            wid, schema, kind, rev, at, evidence, evidence_sha, epoch, identity = opened[registration][0]
            self.assertEqual((wid, schema, kind, rev, at, epoch),
                             (world_id, 1, 'pristine', revision, 0, permit.epoch))
            boundary = dict(kind='pristine', world_id=world_id, revision=revision)
            self.assertEqual(evidence, encoded(boundary))
            self.assertEqual(evidence_sha, sha(boundary))
            info = actor_path.stat()
            self.assertEqual(json.loads(identity), [str(actor_path.resolve()), info.st_dev, info.st_ino])
            self.assertEqual(encoded(session.world.snapshot()), encoded(public))
            for name in ('dm_proposals', 'dm_decisions', 'dm_timeline'):
                self.assertIsNone(before[name], name)
                self.assertEqual(opened[name], [], name)
            for name in business:
                if name in (registration, 'coordinator_windows', 'coordinator_activations',
                            'dm_proposals', 'dm_decisions', 'dm_timeline'):
                    continue
                self.assertEqual(encoded(opened[name]), encoded(before[name]), name)
            self.assertEqual(opened['coordinator_windows'], [])
            self.assertEqual(opened['coordinator_activations'], [])
            self.assertEqual(opened['intents'], [])
            bound = self._material(actor_path, private)
            self.assertEqual(bound['actor_world_binding'], [(1, world_id)])
            for name in ('actor_sessions', 'actor_calls', 'actor_intents'):
                self.assertEqual(bound[name], [], name)
            for name in ('actor_session_heads', 'actor_intent_execution'):
                self.assertIn(bound[name], (None, []), name)
            # Real public mutation, deliberately without the genuine root scope.
            # Catch locally: unittest.assertRaises strips the original traceback.
            try:
                world.edit_card(KING, {'mission': '1183：无权变更不得落库。'})
            except WorldError as denied:
                self.assertEqual(denied.code, 'COORDINATOR_EXECUTION_REQUIRED')
            else:
                self.fail('unpermitted time-zero mutation returned')
            rejected = self._material(main_path, business)
            self.assertEqual(encoded(rejected), encoded(opened))
            self.assertEqual(encoded(self._material(actor_path, private)), encoded(bound))
            self.assertLess(time.monotonic(), end)
        except BaseException as error:
            primary = error
            raise
        finally:
            # First and only ProductSession.close; never issue a substitute CAS.
            try:
                first_close = session.close()
            except BaseException as cleanup:
                if primary is None:
                    raise
                primary.add_note('First ProductSession.close also raised ' + type(cleanup).__name__)
        self.assertIsNone(first_close)
        self.assertIs(coordinator._owner_attempt, owner)
        self.assertIs(owner.permit, permit)
        self.assertIs(owner.thread, heartbeat)
        self.assertTrue(owner.stop.is_set())
        self.assertFalse(heartbeat.is_alive())
        self.assertIsNone(owner.failure)
        self.assertFalse(coordinator.cleanup_pending)
        self.assertTrue(host._closed)
        self.assertFalse(host.cleanup_pending)
        self.assertFalse(host._pending_threads)
        self.assertIsNone(host._process)
        closed = self._material(main_path, business + (lease,))
        tombstone = closed.pop(lease)
        self.assertEqual(len(tombstone), 1)
        self.assertEqual(tombstone[0][:3], (world_id, permit.epoch, None))
        self.assertEqual(encoded(closed), encoded(opened))
        self.assertEqual(encoded(self._material(actor_path, private)), encoded(bound))
        self.assertFalse((directory / 'services.json').exists())
        self.assertLess(time.monotonic(), end)

    def test_p04_fresh_cli_init_first_registered_session_and_close(self):
        self._cold_p04(False)

    def test_p04_fresh_legal_second_time_zero_snapshot_first_session_and_close(self):
        self._cold_p04(True)


if __name__ == '__main__':
    unittest.main()
