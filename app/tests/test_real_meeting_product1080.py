"""1080 definitions only: real product code, synthetic external transport; actual=0.

No live provider, SDK, account or credential evidence. Run only after assembly
and independent source review. Each wholemethod owns one fresh shipped-pack
world and one cumulative 120s budget; original step(60) and 2/120 stay intact.
"""
import copy
import hashlib
import io
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import time
import unittest
import uuid
from contextlib import closing, ExitStack, redirect_stdout
from unittest.mock import patch

from app import product_cli
from app.actors import ActorHost
from app.adjudication import Adjudicator
from app.coordinator_scheduler import ActivationRef
from app.real_provider import RealGateway
from app.real_settings import SettingsStore
from app.server import Coordinator
from app.world import WorldStore

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
KING = 'person-country-1-king'
FINANCE = 'person-country-1-finance'
MARSHAL = 'person-country-1-marshal'
WORDS = '请与我共同支持这项建议。'
REFUSAL = '我不同意此建议；我选择拒绝。'
PRIVATE = '君主未对外传出的私人考虑：暂不透露储备计划。'
FINANCE_PRIVATE = '财政官独立私人考虑：暂不透露账簿。'
REMOTE_WORDS = '异地绝不应送达的会面原话。'
WAIT = '本人选择等待实际后续消息。'


def typed(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest_text(value):
    return hashlib.sha256(typed(json.loads(value)).encode('utf-8')).hexdigest()


class RealMeetingProductTests(unittest.TestCase):
    def setUp(self):
        self.started = time.monotonic()
        self.deadline = self.started + 120
        self.root = SCRATCH / ('parent1080-real-meeting-owned-' + uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.session = None
        self.calls = []
        self.violations = []
        self.windows = []
        self.remote = False
        self.remote_before = None
        self.guards = ExitStack()
        self.addCleanup(self.guards.close)
        # Deny actual external consumers, not Host/DM/World/Adjudicator logic.
        for target in ('socket.socket.connect', 'socket.socket.connect_ex',
                       'socket.create_connection', 'subprocess.Popen',
                       'http.client.HTTPConnection.request',
                       'app.real_settings.default_credential_factory',
                       'app.real_provider.default_credential_factory'):
            self.guards.enter_context(patch(target, self.forbidden))

    def forbidden(self, *args, **kwargs):
        self.violations.append('external-or-scripted-call')
        raise AssertionError('No HTTP/SDK/vault/account/scripted consumer permitted')

    def budget(self):
        self.assertLess(time.monotonic(), self.deadline, 'wholemethod cumulative 120s exhausted; no renewal')

    def save(self, name, value):
        (self.root / (name + '.json')).write_text(
            json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + '\n',
            encoding='utf-8', newline='\n')

    def rows(self, table, private=False):
        allowed = {'intents', 'dm_proposals', 'dm_decisions', 'dm_timeline',
                   'coordinator_windows', 'coordinator_activations', 'coordinator_activation_outcomes',
                   'actor_execution_leases', 'coordinator_execution_leases', 'execution_clock',
                   'actor_coordinator_delegations', 'actor_activation_delegations',
                   'actor_intents', 'actor_sessions', 'actor_session_heads', 'actor_intent_execution', 'actor_calls'}
        self.assertIn(table, allowed)
        path = self.session.directory / ('actors.sqlite' if private else 'world.sqlite')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY rowid')]

    def raw(self):
        path = self.session.directory / 'world.sqlite'
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            return json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])

    def business(self):
        return dict(raw=self.raw(), public={name:self.rows(name) for name in
                    ('intents', 'dm_proposals', 'dm_decisions', 'dm_timeline', 'coordinator_activation_outcomes')},
                    private={name:self.rows(name, True) for name in
                    ('actor_intents', 'actor_sessions', 'actor_session_heads', 'actor_intent_execution', 'actor_calls')})

    def build(self):
        self.budget()
        self.pack = product_cli.SCENARIOS / 'event-wakeup-v1-iron-4.json'
        manifest = json.loads(product_cli.MANIFEST.read_bytes())
        relative = 'app/data/scenarios/event-wakeup-v1-iron-4.json'
        data = self.pack.read_bytes()
        self.assertEqual(manifest['files'][relative], dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest()))
        config = json.loads(data)
        self.temp = config['event_wakeup']['temporary_persons'][0]['id']
        self.assertEqual(config['event_wakeup']['temporary_steps'], 3)
        self.owned = self.root / 'world'
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(product_cli.main(['--data-dir', str(self.owned), 'init', '--scenario', str(self.pack)]), 0)
        self.assertFalse(json.loads(output.getvalue())['real_model'])
        self.assertEqual((self.owned / 'scenario.json').read_bytes(), data)
        self.services = {kind:dict(provider='openai-chat-loopback',
            base_url='http://127.0.0.1:' + port + '/v1', model=kind + '-meeting1080-synthetic',
            credential_ref=None, timeout_seconds=2, max_retries=0)
            for kind, port in (('roles', '18181'), ('dm', '18182'))}
        store = SettingsStore(self.owned / 'services.json', credential_factory=self.forbidden)
        store.save(dict(services=self.services, secrets={}))
        self.assertEqual(store.snapshot(), self.services)
        self.session = product_cli.ProductSession(self.owned, 'real')
        self.bind()
        state = self.session.world.snapshot()
        self.initial = copy.deepcopy(state)
        people = {p['id']:p for p in state['persons']}
        self.assertEqual((people[KING]['location'], people[self.temp]['location'],
                          people[FINANCE]['location'], people[MARSHAL]['location']),
                         ('place-0', 'place-0', 'place-0', 'place-1'))
        self.assertEqual(len(people), 13)
        self.residents = [p['id'] for p in state['persons'] if p['id'] != self.temp]
        self.assertEqual(self.raw()['_p04']['remaining'], {self.temp:0})
        self.assertNotIn(self.temp, self.raw()['_p04']['pending'])
        self.assertEqual(self.session.world.observe(self.temp)['observations'], [])
        self.assertEqual(self.session.world.observe(self.temp)['messages'], [])
        self.source = product_cli.source_identity(self.session.world, self.session.host, self.session.coordinator)
        self.generation = product_cli.verify_generation()
        self.budget()

    def bind(self):
        s = self.session
        self.assertIs(type(s), product_cli.ProductSession)
        self.assertIs(type(s.host), ActorHost)
        self.assertIs(type(s.world), WorldStore)
        self.assertIs(type(s.coordinator), Coordinator)
        self.assertIs(type(s.host.adjudicator), Adjudicator)
        self.assertIs(s.host.adjudicator.world, s.world)
        self.assertTrue(callable(s.host.dm_provider))
        self.assertIs(type(s.host.real_gateway), RealGateway)
        self.assertIs(type(s.host.real_settings), SettingsStore)
        self.assertIs(s.host._request.__func__, ActorHost._request)
        self.assertNotIn('_submit', s.host.__dict__)
        self.assertIs(s.coordinator.actors, s.host)
        self.assertTrue(s.host.execution_fenced)
        for policy in (s.host.execution_policy, s.coordinator.execution_policy):
            self.assertEqual((policy.busy_budget_seconds, policy.act_deadline_seconds), (2, 120))
        s.host._choose = self.forbidden  # Detection only; never supplies an intent.
        s.host.real_gateway.transport = self.transport  # Sole provider-output seam.

    def effect(self, proposal):
        if proposal == self.king_proposal:
            return dict(action='meet', to=self.temp, text=WORDS)
        if proposal == self.temp_proposal:
            return dict(action='meet', to=KING, text=REFUSAL)
        if proposal == self.remote_proposal:
            return dict(action='meet', to=MARSHAL, text=REMOTE_WORDS)
        if proposal.startswith(WAIT) or proposal == FINANCE_PRIVATE:
            return dict(action='wait')
        raise AssertionError('unexpected synthetic proposal')

    @property
    def king_proposal(self):
        return PRIVATE + ' 我只向' + self.temp + '发言：“' + WORDS + '”'

    @property
    def temp_proposal(self):
        return '我本人只向' + KING + '发言：“' + REFUSAL + '”'

    @property
    def remote_proposal(self):
        return '我本人尝试会面' + MARSHAL + '，只说：“' + REMOTE_WORDS + '”'

    def transport(self, service, messages, token, budget):
        budget.check()
        view = None
        if service['model'] == self.services['roles']['model']:
            view = json.loads(messages[-1]['content'])
            pid = view['person']['id']
            previous = [c for c in self.calls if c['kind'] == 'roles' and c['view']['person']['id'] == pid]
            if pid == KING and not previous:
                text = self.king_proposal
            elif pid == KING and self.remote:
                text = self.remote_proposal
            elif pid == self.temp and not previous:
                text = self.temp_proposal
            elif pid == FINANCE:
                text = FINANCE_PRIVATE
            else:
                text = WAIT + pid
            record = dict(kind='roles', view=copy.deepcopy(view), messages=copy.deepcopy(messages), text=text, token=token)
        else:
            payload = json.loads(messages[-1]['content'])
            proposal = payload['proposal']
            effect = self.effect(proposal['text'])
            record = dict(kind='dm', payload=copy.deepcopy(payload), messages=copy.deepcopy(messages), effect=effect, token=token)
            if proposal['text'] == self.remote_proposal:
                # This is the actual external seam AFTER durable parent creation,
                # BEFORE original record/preview/commit/effect_or_reject.
                self.remote_before = dict(business=self.business(), activations=self.rows('coordinator_activations'),
                                          windows=self.rows('coordinator_windows'), control=self.control())
            text = typed(dict(ruling='成功', reason='仅兑现本提案者原话或等待，目标自主决定',
                              goal='有限会面或等候', costs=[], benefits=[], effects=[effect]))
        self.calls.append(record)
        return {'choices':[{'message':{'content':text}}], 'usage':{'prompt_tokens':17, 'completion_tokens':9}}

    def step(self, at, roster):
        self.budget()
        state = self.session.step(60)
        self.budget()
        self.assertIsNone(self.session.coordinator.error)
        self.assertEqual(state['status'], 'user_paused')  # Normal single-step control, not technical failure.
        windows = self.rows('coordinator_windows')
        self.assertEqual(len(windows), len(self.windows) + 1)
        window = max(windows, key=lambda w:w['ordinal'])
        self.assertEqual((window['state'], window['target_time'], json.loads(window['roster_json'])), ('completed', at, roster))
        self.windows.append(window)
        self.save('windows', windows)
        return state

    def local_exchange(self):
        self.step(0, self.residents)
        self.assertFalse(any(c['kind']=='roles' and c['view']['person']['id']==self.temp for c in self.calls))
        self.assertIn(self.temp, self.raw()['_p04']['pending'])
        self.assertEqual(self.raw()['_p04']['remaining'][self.temp], 3)
        self.step(0, [self.temp])
        self.assertEqual(self.raw()['_p04']['remaining'][self.temp], 2)
        deferred = [q for q in self.raw()['queue'] if q['kind']=='p04_deferred_wake']
        self.assertEqual([(q['person_id'], q['at'], q['requested_at']) for q in deferred], [(KING, 60, 0)])
        self.step(0, [self.temp])
        self.assertEqual(self.raw()['_p04']['remaining'][self.temp], 0)
        self.assertNotIn(self.temp, self.raw()['_p04']['pending'])
        self.assertEqual([o['text'] for o in self.session.world.observe(self.temp)['observations'] if o['source']=='实际会面'], [WORDS])
        self.assertEqual([o['text'] for o in self.session.world.observe(KING)['observations'] if o['source']=='实际会面'], [REFUSAL])
        self.assertEqual(len([e for e in self.raw()['events'] if e['kind']=='meeting']), 2)

    def closure(self):
        public = {r['id']:r for r in self.rows('intents')}
        leaves = {r['intent_id']:r for r in self.rows('actor_intents', True)}
        active = self.rows('coordinator_activations')
        decisions = self.rows('dm_decisions')
        proposals = {r['intent_id']:r for r in self.rows('dm_proposals')}
        metas = {r['intent_id']:r for r in self.rows('actor_intent_execution', True)}
        outcomes = {r['activation_id']:r for r in self.rows('coordinator_activation_outcomes')}
        self.assertEqual({a['intent_id'] for a in active}, set(leaves))
        self.assertEqual(set(proposals), set(leaves))
        self.assertEqual(len(decisions), len(active))
        self.assertEqual(len(outcomes), len(active))
        expected_ids = set(leaves)
        for a in active:
            intent, person = a['intent_id'], a['person_id']
            leaf, meta = leaves[intent], metas[intent]
            request, candidate = json.loads(leaf['request']), json.loads(leaf['candidate'])
            body = json.loads(leaf['body'])
            self.assertEqual((request['person_id'], request['intent_id'], request['body']), (person, intent, None))
            self.assertEqual(body, dict(action='propose', text=candidate[-1]['content']))
            self.assertEqual(candidate[:-2], request['messages'])
            self.assertEqual(candidate[-2], dict(role='user', content=request['prompt']))
            binding = candidate[-1]['intent_binding']
            self.assertEqual((binding['person_id'], binding['intent_id'], binding['body']), (person, intent, body))
            self.assertEqual(binding['request_sha256'], digest_text(leaf['request']))
            role_calls = [c for c in self.calls if c['kind']=='roles' and typed(c['view'])==request['prompt']]
            self.assertEqual(len(role_calls), 1)
            role_call = role_calls[0]
            self.assertEqual(role_call['text'], body['text'])
            self.assertEqual(role_call['messages'][0]['content'], request['system_prompt'] +
                '\n你自主决定下一步意图；可以等待、拒绝或提出任何自然语言计划。只返回意图原文，不捏造未收到的世界事实。')
            self.assertEqual(role_call['messages'][1:-1],
                [dict(role=m['role'], content=m['content']) for m in request['messages']])
            self.assertEqual(role_call['messages'][-1], dict(role='user', content=request['prompt']))
            self.assertEqual((a['request_sha256'], a['candidate_sha256']), (digest_text(leaf['request']), digest_text(leaf['candidate'])))
            self.assertEqual(proposals[intent]['person_id'], person)
            self.assertEqual(proposals[intent]['text'], body['text'])
            self.assertEqual(json.loads(public[intent]['payload']), {'body':body})
            dm = next(c for c in self.calls if c['kind']=='dm' and c['payload']['proposal']['intent_id']==intent)
            decision_row = next(d for d in decisions if d['intent_id']==intent)
            decision = json.loads(decision_row['data'])
            self.assertEqual(decision['effects'], [dm['effect']])
            self.assertEqual(dm['payload']['proposal']['person_id'], person)
            system = dm['messages'][0]['content']
            for literal in ('meet(to,text)', '可选效果', 'optin新局', '两名不同人物', '实际同地', '不在途',
                            '本次提案者', '真实指定发言', '不得替对方答应', '撒谎', '决定忠诚',
                            '失败或大失败不投递', '独立会话', '回答、拒绝或等待', '无共识', '真实规则'):
                self.assertIn(literal, system)  # Actual independent DM request, not source-only grep.
            self.assertNotIn('_p04', typed(dm['payload']['world']))
            metrics = json.loads(leaf['metrics'])
            self.assertEqual((metrics['provider'], metrics['sdk_requests']), ('real-chat-v1', 0))
            self.assertEqual(metrics['sdk_events'], {'http_requests':1})
            self.assertEqual(a['private_closed'], 1)
            self.assertEqual(meta['base_version'], a['base_session_version'])
            child = Adjudicator._child_id(intent, decision_row['version'], 0)
            if a['phase']=='rejected':
                self.assertEqual((leaf['state'], a['terminal_reason']), ('rejected', 'NOT_AT_LOCATION'))
                self.assertIsNone(a['promoted_version']); self.assertIsNone(meta['promoted_version'])
                self.assertIsNone(leaf['receipt']); self.assertIsNone(public[intent]['receipt'])
                self.assertNotIn(child, public)
            else:
                self.assertEqual((a['phase'], leaf['state']), ('completed', 'committed'))
                self.assertEqual(a['promoted_version'], a['base_session_version']+1)
                self.assertEqual(meta['promoted_version'], a['promoted_version'])
                expected_ids.add(child)
                self.assertEqual(public[child]['person'], person)
                self.assertEqual(json.loads(public[child]['payload']), dict(parent=intent, version=decision_row['version'], body=dm['effect']))
                receipt = json.loads(public[intent]['receipt'])
                child_receipt = json.loads(public[child]['receipt'])
                self.assertEqual(json.loads(leaf['receipt']), receipt)
                self.assertEqual((receipt['person_id'], receipt['intent_id'], receipt['status']), (person, intent, 'committed'))
                self.assertEqual(receipt['effect']['effects'], [child_receipt['effect']])
                if dm['effect']['action']=='meet':
                    self.assertEqual(child_receipt['effect'], dict(implemented=True, participant=dm['effect']['to']))
                    self.assertEqual([e['cause_id'] for e in self.raw()['events'] if e['kind']=='meeting' and e['cause_id']==child], [child])
            self.assertEqual(json.loads(outcomes[a['activation_id']]['outcome_json'])['intent_id'], intent)
        self.assertEqual(set(public), expected_ids)
        for head in self.rows('actor_session_heads', True):
            spine = sorted((a for a in active if a['person_id']==head['person_id'] and a['phase']=='completed'), key=lambda a:a['promoted_version'])
            self.assertEqual([a['promoted_version'] for a in spine], list(range(1, len(spine)+1)))
            self.assertEqual(head['version'], len(spine))
            prior = []
            for a in spine:
                self.assertEqual(json.loads(leaves[a['intent_id']]['request'])['messages'], prior)
                prior = json.loads(leaves[a['intent_id']]['candidate'])
            session = next(r for r in self.rows('actor_sessions', True) if r['person_id']==head['person_id'])
            self.assertEqual(json.loads(session['messages']), prior)
            if spine:self.assertEqual(head['last_promoted_intent_id'], spine[-1]['intent_id'])
        temp_first = next(r for r in leaves.values() if r['person_id']==self.temp and json.loads(r['request'])['messages']==[])
        q = json.loads(temp_first['request'])
        self.assertIn(WORDS, q['prompt'])
        self.assertNotIn(PRIVATE, typed(q)); self.assertNotIn(FINANCE_PRIVATE, typed(q))
        for call in self.calls:
            self.assertIsNone(call['token'])
        for pid in self.residents:
            if pid != KING:
                view = self.session.world.observe(pid)
                for words in (WORDS, REFUSAL, PRIVATE, REMOTE_WORDS):self.assertNotIn(words, typed(view))
        self.assertIsNone(self.session.host._process)
        self.assertEqual(self.violations, [])
        self.assertEqual(product_cli.verify_generation(), self.generation)
        self.assertEqual(product_cli.source_identity(self.session.world, self.session.host, self.session.coordinator), self.source)
        self.save('calls', self.calls)
        self.save('closure', dict(activations=active, private=list(leaves.values()), public=list(public.values()), decisions=decisions))
        return active

    def test_real_product_local_meeting_independent_refusal_replay_and_reopen(self):
        self.build()
        self.local_exchange()
        self.step(60, [KING])
        king_calls = [c for c in self.calls if c['kind']=='roles' and c['view']['person']['id']==KING]
        self.assertEqual([c['text'] for c in king_calls], [self.king_proposal, WAIT+KING])
        self.assertIn(REFUSAL, typed(king_calls[-1]['view']))
        self.assertEqual(len([e for e in self.raw()['events'] if e['kind']=='meeting']), 2)
        for key in ('places', 'ledger', 'in_transit'):
            self.assertEqual(self.session.world.snapshot()[key], self.initial[key])
        active = self.closure()
        original = next(a for a in active if a['person_id']==KING and a['base_session_version']==0)
        ref = ActivationRef(**{k:original[k] for k in ('world_id','window_id','activation_id','person_id')})
        # Single-step paused normally; resume by original root control before replay.
        before_resume = self.raw()
        with self.session.coordinator._owner_scope():self.session.world.control('resume')
        resumed = self.raw()
        expected = copy.deepcopy(before_resume); expected.update(status='running', revision=before_resume['revision']+1)
        self.assertEqual(resumed, expected)
        before, control_before = self.business(), self.control()
        calls = len(self.calls)
        replay = self.session.host.act(KING, coordinator=self.session.coordinator._owner_attempt.permit, activation=ref)
        self.assertEqual(replay['intent_id'], original['intent_id'])
        self.assertTrue(replay['reconciled'])
        self.assertEqual(len(self.calls), calls)
        self.assertEqual(self.business(), before)
        self.replay_control(control_before, self.control(), original)
        with self.session.coordinator._owner_scope():self.session.world.control('pause')
        at = self.raw()['revision']
        historical = self.session.coordinator.snapshot(seq=at)
        historical_proposal = next(p for p in historical['causality']['proposals'] if p['intent_id']==original['intent_id'])
        self.assertEqual(historical_proposal['text'], self.king_proposal)
        self.assertIsNotNone(historical_proposal['committed'])
        self.assertEqual(historical_proposal['decisions'][0]['effects'], [dict(action='meet', to=self.temp, text=WORDS)])
        observed = {p:self.session.world.observe(p, seq=at) for p in (KING, self.temp, FINANCE, MARSHAL)}
        saved = self.business()
        self.session.close()
        self.assertFalse(self.session.host.cleanup_pending); self.assertFalse(self.session.coordinator.cleanup_pending)
        self.session = product_cli.ProductSession(self.owned, 'real')
        self.bind()
        self.assertEqual(self.business(), saved)
        self.assertEqual(self.session.coordinator.snapshot(seq=at), historical)
        self.assertEqual({p:self.session.world.observe(p, seq=at) for p in observed}, observed)
        self.assertEqual(product_cli.read_owned(self.owned, seq=at)['revision'], at)
        self.assertEqual(len(self.calls), calls)
        self.closure()
        self.budget()

    def control(self):
        return {name:self.rows(name) for name in ('execution_clock', 'actor_execution_leases',
            'coordinator_execution_leases', 'actor_coordinator_delegations', 'actor_activation_delegations',
            'coordinator_activations', 'coordinator_windows')}

    def replay_control(self, before, after, original):
        # Exact legitimate accounting, NOT a blanket exclusion of control tables.
        old = next(r for r in before['actor_execution_leases'] if r['person_id']==KING)
        new = next(r for r in after['actor_execution_leases'] if r['person_id']==KING)
        self.assertEqual(new['epoch'], old['epoch']+1)
        self.assertIsNone(new['owner_token'])
        self.assertEqual(new['owner_instance'], old['owner_instance'])
        for field in ('acquired_wall_ns', 'expires_wall_ns', 'last_wall_ns'):
            self.assertGreaterEqual(new[field], old[field])
        for name in ('actor_coordinator_delegations', 'actor_activation_delegations'):
            self.assertEqual(after[name][:-1], before[name])
            self.assertEqual(len(after[name]), len(before[name])+1)
            row = after[name][-1]
            self.assertEqual((row['world_id'], row['person_id'], row['actor_epoch']), (original['world_id'], KING, new['epoch']))
            if name=='actor_activation_delegations':
                self.assertEqual((row['window_id'], row['activation_id']), (original['window_id'], original['activation_id']))
            else:
                owner = self.session.coordinator._owner_attempt.permit
                self.assertEqual((row['parent_epoch'], row['parent_owner_token']), (owner.epoch, owner.owner_token))
        expected = copy.deepcopy(before['coordinator_activations'])
        row = next(r for r in expected if r['activation_id']==original['activation_id'])
        row.update(owner_actor_epoch=new['epoch'], owner_parent_epoch=self.session.coordinator._owner_attempt.permit.epoch)
        self.assertEqual(after['coordinator_activations'], expected)
        self.assertEqual(after['coordinator_windows'], before['coordinator_windows'])
        self.assertEqual([r for r in after['actor_execution_leases'] if r['person_id']!=KING],
                         [r for r in before['actor_execution_leases'] if r['person_id']!=KING])
        self.assertGreaterEqual(after['execution_clock'][0]['high_water_wall_ns'], before['execution_clock'][0]['high_water_wall_ns'])
        old_owner, new_owner = before['coordinator_execution_leases'][0], after['coordinator_execution_leases'][0]
        for key in old_owner:
            if key in ('expires_wall_ns', 'last_wall_ns'):self.assertGreaterEqual(new_owner[key], old_owner[key])
            else:self.assertEqual(new_owner[key], old_owner[key])
        self.save('replay-control-delta', dict(before=before, after=after))

    def test_real_product_remote_meeting_denies_without_words_or_forced_response(self):
        self.build()
        self.local_exchange()  # Legal producer positive first; no fixture/DB location mutation.
        self.remote = True
        self.step(60, [KING])
        self.assertIsNotNone(self.remote_before)
        captured = self.remote_before['business']
        after = self.business()
        # At actual DM transport entry, the original parent already exists. Only
        # decision audit + rejection leaf + normal pause may follow, never words.
        expected_raw = copy.deepcopy(captured['raw'])
        expected_raw.update(status='user_paused', revision=expected_raw['revision']+1)
        self.assertEqual(after['raw'], expected_raw)
        self.assertEqual(after['public']['intents'], captured['public']['intents'])
        self.assertEqual(after['public']['dm_proposals'], captured['public']['dm_proposals'])
        added = after['public']['dm_decisions'][len(captured['public']['dm_decisions']):]
        self.assertEqual(after['public']['dm_decisions'][:-1], captured['public']['dm_decisions'])
        self.assertEqual(len(added), 1)
        intent = added[0]['intent_id']
        a = next(a for a in self.rows('coordinator_activations') if a['intent_id']==intent)
        self.assertEqual((a['person_id'], a['phase'], a['terminal_reason'], a['private_closed']), (KING, 'rejected', 'NOT_AT_LOCATION', 1))
        outcome_row = next(r for r in after['public']['coordinator_activation_outcomes'] if r['intent_id']==intent)
        outcome = json.loads(outcome_row['outcome_json'])
        self.assertEqual((outcome_row['kind'], outcome['status'], outcome['code'], outcome['origin'], outcome['action'], outcome['effect_index']),
                         ('domain_rejected', 'domain_rejected', 'NOT_AT_LOCATION', 'adjudication.execute', 'meet', 0))
        self.assertEqual(outcome['effects'], [])
        self.assertEqual(outcome['decision_version'], added[0]['version'])
        self.assertEqual(outcome['decision_sha256'], digest_text(added[0]['data']))
        self.assertEqual(after['public']['coordinator_activation_outcomes'][:-1], captured['public']['coordinator_activation_outcomes'])
        timeline = after['public']['dm_timeline']
        self.assertEqual(timeline[:-1], captured['public']['dm_timeline'])
        self.assertEqual((timeline[-1]['intent_id'], timeline[-1]['kind'], timeline[-1]['version']), (intent, 'decision', added[0]['version']))
        self.assertFalse(any(r['intent_id']==intent and r['kind']=='commit' for r in timeline))
        for table in ('actor_sessions', 'actor_session_heads', 'actor_intent_execution', 'actor_calls'):
            self.assertEqual(after['private'][table], captured['private'][table])
        expected_private = copy.deepcopy(captured['private']['actor_intents'])
        next(r for r in expected_private if r['intent_id']==intent)['state'] = 'rejected'
        self.assertEqual(after['private']['actor_intents'], expected_private)
        expected_active = copy.deepcopy(self.remote_before['activations'])
        next(r for r in expected_active if r['intent_id']==intent).update(phase='rejected', terminal_reason='NOT_AT_LOCATION', private_closed=1)
        self.assertEqual(self.rows('coordinator_activations'), expected_active)
        expected_windows = copy.deepcopy(self.remote_before['windows'])
        expected_windows[-1].update(state='completed', completed_revision=captured['raw']['revision'])
        self.assertEqual(self.rows('coordinator_windows'), expected_windows)
        before_control, after_control = self.remote_before['control'], self.control()
        for table in ('actor_coordinator_delegations', 'actor_activation_delegations'):
            self.assertEqual(after_control[table], before_control[table])
        for table in ('actor_execution_leases', 'coordinator_execution_leases'):
            self.assertEqual(len(after_control[table]), len(before_control[table]))
            for old, new in zip(before_control[table], after_control[table]):
                for key in old:
                    if key in ('expires_wall_ns', 'last_wall_ns'):
                        self.assertGreaterEqual(new[key], old[key])  # Original maintenance only.
                    elif table=='actor_execution_leases' and old['person_id']==KING and key=='owner_token':
                        self.assertIsNone(new[key])  # Original attempt release, not new authority.
                    else:
                        self.assertEqual(new[key], old[key])
        self.assertEqual(len(after_control['execution_clock']), 1)
        self.assertEqual(after_control['execution_clock'][0]['singleton'], 1)
        self.assertGreaterEqual(after_control['execution_clock'][0]['high_water_wall_ns'],
                                before_control['execution_clock'][0]['high_water_wall_ns'])
        self.assertNotIn(MARSHAL, after['raw']['_p04']['pending'])
        self.assertFalse(any(q['kind']=='p04_deferred_wake' and q['person_id']==MARSHAL for q in after['raw']['queue']))
        remote_calls = [c for c in self.calls if c['kind']=='roles' and c['view']['person']['id']==MARSHAL]
        self.assertEqual(len(remote_calls), 1)  # Original t0 government, never a forced response.
        self.assertEqual(remote_calls[0]['view']['time'], 0)
        self.assertNotIn(REMOTE_WORDS, typed(self.session.world.observe(MARSHAL)))
        trace = self.session.host.adjudicator.trace()
        proposal = next(p for p in trace['proposals'] if p['intent_id']==intent)
        self.assertEqual(proposal['text'], self.remote_proposal)
        self.assertIsNone(proposal['committed'])
        self.assertEqual(len(proposal['decisions']), 1)
        self.closure()
        self.save('remote-deep-leaf', dict(before=self.remote_before, after=after, outcome=outcome))
        self.budget()

    def tearDown(self):
        try:
            if self.session is not None:
                self.session.close()
                self.assertFalse(self.session.host.cleanup_pending)
                self.assertFalse(self.session.coordinator.cleanup_pending)
            self.budget()
        finally:
            self.save('calls-final', self.calls)
            self.save('definition-execution-metadata', dict(synthetic_transport=True, live_model=False,
                sdk=False, owned=str(self.root), violations=self.violations,
                elapsed_seconds=time.monotonic()-self.started, runtime_claim='only if this complete method returns'))
            self.guards.close()


if __name__ == '__main__':
    unittest.main()
