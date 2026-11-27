"""W3 real-product regression DEFINITIONS ONLY; no runtime result is claimed.

Only RealGateway.transport supplies provider output. Observers select exact real
code/AST statements, never inspect frame locals or replace production methods.
Every method owns one fresh world and one cumulative whole120; step60, original
2s contention and actor/preparation120 remain unchanged. No live model/SDK.
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
from contextlib import closing, ExitStack, redirect_stdout
from unittest.mock import patch

from app import product_cli
from app import coordinator_scheduler as scheduler
from app.actor_execution import PreparationBudget, Attempt, LifecycleCallBudget
from app.actors import ActorHost
from app.adjudication import Adjudicator
from app.real_provider import RealGateway, ProviderFault
from app.real_settings import SettingsStore
from app.server import Coordinator
from app.world import WorldStore, WorldError

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
KING = 'person-country-1-king'
OLD = 'person-country-1-finance'
REMOTE = 'person-country-1-marshal'
MEANS = 'personal-iron-country-1-king'
SECRET = '旧财政官自己没有交出的非空私人账簿考虑。'
WAKE = '我本人向person-country-1-king发信：六十秒后核查。'
STRIKE = '我本人选择以所指原物近身行动，不替目标决定回应。'
PUBLIC = ('snapshots', 'intents', 'dm_proposals', 'dm_decisions', 'dm_timeline',
          'coordinator_windows', 'coordinator_activations', 'coordinator_activation_outcomes')
PRIVATE = ('actor_intents', 'actor_sessions', 'actor_session_heads',
           'actor_intent_execution', 'actor_calls')
CONTROL = ('actor_execution_leases', 'coordinator_execution_leases', 'execution_clock',
           'actor_coordinator_delegations', 'actor_activation_delegations')


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def sha(value):
    return hashlib.sha256(canonical(value).encode('utf-8')).hexdigest()


class SyntheticTransport:
    """Existing external-response seam, including a genuinely blocking close."""
    def __init__(self, owner):
        self.owner = owner
        self.entered = threading.Event()
        self.cancelled = threading.Event()
        self.exited = threading.Event()
        self.close_threads = []
        self.root_thread = None
        self.block_root = False

    def close(self):
        self.close_threads.append(threading.current_thread())
        self.cancelled.set()

    def __call__(self, service, messages, token, budget):
        t = self.owner
        t.budget()
        budget.check()
        t.assertIsNone(token)
        if type(budget.attempt) is Attempt and not any(budget.attempt is a for a in t.attempts):
            t.attempts.append(budget.attempt)
        elif type(budget.attempt) is LifecycleCallBudget:
            t.assertEqual(budget.attempt.calls, [])
        payload = json.loads(messages[-1]['content'])
        record = dict(messages=copy.deepcopy(messages), payload=copy.deepcopy(payload))
        if service['model'] == t.services['roles']['model']:
            record['kind'] = 'roles'
            t.assert_personal_object_input(payload, messages)
            pid = payload['person']['id']
            body = dict(text=SECRET if pid == OLD else '我本人选择等待。', physical_attempt=None)
            if pid == OLD and payload['time'] == 0:
                body['text'] = SECRET + WAKE
            if pid == KING and payload['time'] == 60:
                if t.unheld:
                    # DELIBERATE illegal adversarial fixture: foreign ID is NOT
                    # claimed role knowledge. Preserve the original unheld deny.
                    t.assertEqual(payload['personal_objects'], [])
                    means_id = MEANS
                    record['illegal_foreign_means_fixture'] = True
                else:
                    held = [v for v in payload['personal_objects'] if v['kind'] == 'held_iron_weapon']
                    t.assertEqual(len(held), 1, 'no omniscient fallback for lawful roles')
                    means_id = held[0]['id']
                body = dict(text=STRIKE, physical_attempt=dict(mode='strike_with_held_weapon',
                    target_person_id=t.target, means_id=means_id))
            text = canonical(body)
        elif payload.get('kind') == 'succession':
            record['kind'] = 'succession'
            self.root_thread = threading.current_thread()
            t.assertIs(self.root_thread, t.session.coordinator.worker)
            w, h = t.session.world, t.session.host
            root = t.session.coordinator._owner_attempt
            t.assertIs(w._scoped_coordinator_attempt(), root)
            t.assertIsNone(w._scoped_attempt())
            t.assertIs(type(w._scoped_entry_budget()), PreparationBudget)
            t.assertFalse(h._lock._is_owned(), 'root provider must not hold pipeline lock')
            t.assertFalse(w._private_sources_lock._is_owned(), 'root provider must not hold source lock')
            t.assertIs(h._lifecycle_active[0], root)
            t.assertIs(h._lifecycle_active[2], h.real_gateway)
            t.root_entry = w._scoped_entry_budget()
            t.root_request = copy.deepcopy(payload)
            t.prepared = t.business()
            t.assertIsNone(t.raw()['_lifecycle']['jobs'][0]['result'])
            slot = payload['unresolved_background_slots'][0]
            fact = next(f['record'] for f in payload['established_facts'] if f['kind'] == 'institution'
                        and f['record']['office_id'] == slot['office_id'])
            response = dict(status='appointed', reason='依本次真实空缺与已成立自动代理惯例。',
                basis_refs=[dict(kind='institution', id=fact['id'], at_revision=payload['base_revision'])],
                successor=dict(name='本次独立裁定的新书吏', background_role=slot['background_role'],
                    location=slot['place_id'], card=dict(values='守约', interests='账簿', risk='谨慎', mission='履行本职')),
                unresolved=[])
            t.root_response = copy.deepcopy(response)
            text = canonical(response)
            t.final_armed = True
            t.calls.append(record)
            self.entered.set()
            if self.block_root:
                try:
                    while not self.cancelled.wait(.01):
                        budget.check()
                        t.budget()
                    # Return a syntactically valid response AFTER close. Original
                    # Gateway/Host checks, not this seam, must prevent its commit.
                finally:
                    self.exited.set()
            return {'choices': [{'message': {'content': text}}],
                    'usage': {'prompt_tokens': 17, 'completion_tokens': 9}}
        else:
            record['kind'] = 'dm'
            proposal = payload['proposal']
            if proposal['text'] == STRIKE:
                t.attack_before = t.business()
                t.attack_intent = proposal['intent_id']
                t.attack_armed = True
                if t.provider_code:
                    t.calls.append(record)
                    raise ProviderFault(t.provider_code)
                anchor = proposal['physical_attempt']
                protocol = payload['opt_in_effect_protocol']
                role = [c for c in t.calls if c['kind'] == 'roles'
                        and c['payload']['person']['id'] == KING][-1]
                t.assertEqual(anchor, json.loads(role['response_raw'])['physical_attempt'])
                t.assertEqual(anchor['mode'], 'strike_with_held_weapon')
                t.assertEqual(anchor['target_person_id'], t.target)
                if t.unheld:
                    # Malicious DM fixture also knowingly references a foreign
                    # object; it is a negative, not a legitimate DM projection.
                    t.assertEqual(protocol['personal_objects'], [])
                    t.assertEqual(anchor['means_id'], MEANS)
                    t.assertTrue(role['illegal_foreign_means_fixture'])
                else:
                    means = next(v for v in protocol['personal_objects'] if v['id'] == anchor['means_id'])
                    t.assertEqual(means['holder_person_id'], proposal['person_id'])
                    t.assertIn(means['id'], [v['id'] for v in role['payload']['personal_objects']])
                target = next(v for v in protocol['scene_persons'] if v['id'] == anchor['target_person_id'])
                t.assertEqual(target['id'], t.target)
                revision = payload['world']['revision']
                t.assertEqual(protocol['base_revision'], revision)
                effects = [dict(action='resolve_physical_attempt', outcome='mortal',
                    reason='只裁原锚的有限物理后果。', basis_refs=[
                        dict(kind='personal_object', id=anchor['means_id'], at_revision=revision),
                        dict(kind='scene_person', id=target['id'], at_revision=revision)])]
                t.assertEqual(payload['opt_in_effect_protocol']['physical_attempt'], proposal['physical_attempt'])
            elif proposal['text'] == SECRET + WAKE:
                effects = [dict(action='send', to=KING, text=WAKE)]
            else:
                effects = [dict(action='wait')]
            text = canonical(dict(ruling='成功', reason='只执行本提案者原意图。', goal='有限行动',
                                  costs=[], benefits=[], effects=effects))
        record['response_raw'] = text
        t.calls.append(record)
        return {'choices': [{'message': {'content': text}}],
                'usage': {'prompt_tokens': 17, 'completion_tokens': 9}}


class LifecycleAuthorityProductTests(unittest.TestCase):
    def assert_close_oracle(self, end=None):
        events = self.close_events if end is None else self.close_events[:end]
        self.assertTrue(events)
        host = [e for e in events if e['operation'] == 'host_exit' and e['event'] == 'return']
        self.assertEqual(len(host), len(self.attempts))
        for attempt in self.attempts:
            ordinal = next(i for i,p in enumerate(self.close_permits) if attempt.permit is p)
            exits = [e for e in host if e['permit'] == ordinal]
            self.assertEqual(len(exits), 1)
            for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                self.assertIs(exits[0][key], True, 'first Host exit boolean cannot be washed')
            releases = [e for e in events if e['operation'] == 'actor_release' and e['permit'] == ordinal]
            self.assertEqual([e['event'] for e in releases], ['call', 'return'])
            self.assertIs(releases[-1]['value'], True)
            self.assertLess(releases[-1]['order'], exits[0]['order'])
            finishes = [e for e in events if e['operation'] == 'actor_finish' and e['permit'] == ordinal]
            self.assertEqual([e['event'] for e in finishes], ['call', 'return'])
            self.assertIs(finishes[-1]['value'], True)
            self.assertLess(finishes[-1]['order'], releases[0]['order'])
        root = [e for e in events if e['operation'] == 'root_release']
        returns = [e for e in root if e['event'] == 'return']
        self.assertTrue(returns)
        self.assertEqual([e['event'] for e in root], ['call', 'return'] * len(returns))
        for i, event in enumerate(returns):
            self.assertIs(self.close_permits[event['permit']], self.original_root.permit)
            self.assertEqual(event['phase'], 'first_close' if i == 0 else 'repeat_cleanup')
            self.assertIs(event['value'], i == 0, 'only explicitly repeated root cleanup returns False')
        finishes = [e for e in events if e['operation'] == 'root_finish']
        finished = [e for e in finishes if e['event'] == 'return']
        self.assertEqual(len(finished), len(returns))
        self.assertEqual([e['event'] for e in finishes], ['call', 'return'] * len(finished))
        for finish, release in zip(finished, returns):
            self.assertIs(finish['value'], True)
            self.assertLess(finish['order'], release['order'])
        self.assertEqual(self.close_frames, {})

    def check_source_binding(self, binding, expected_raw=None):
        """Dynamic check only at a selected callback, not every line."""
        b = binding
        self.assertIs(sys.modules[b['module'].__name__], b['module'])
        self.assertTrue(vars(b['module']) is b['namespace'], 'loaded module dictionary mismatch')
        self.assertIs(b['namespace'][b['cls'].__name__], b['cls'])
        self.assertIs(getattr(b['cls'], b['method']), b['function'])
        self.assertIs(b['function'].__code__, b['code'])
        self.assertTrue(b['function'].__globals__ is b['namespace'], 'loaded function globals mismatch')
        self.assertEqual(product_cli.ROOT.resolve(strict=True), b['root'])
        self.assertEqual(product_cli.MANIFEST.read_bytes(), b['manifest_raw'])
        self.assertEqual(Path(b['module'].__file__).resolve(strict=True), b['path'])
        self.assertEqual(Path(b['code'].co_filename).resolve(strict=True), b['path'])
        self.assertEqual(b['path'].read_bytes(), b['raw'] if expected_raw is None else expected_raw)

    def close_record(self):
        self.close_phase = 'first_close'
        record = dict(events_start=len(self.close_events))
        try:
            record['return'] = self.session.close()
        except BaseException as error:
            record['error'] = error  # strong original, never str/args in diagnostics
        record['events_end'] = len(self.close_events)
        self.first_close_record = record  # latch before fallible resource observation
        record['resources'] = self.resource_record()
        self.assertTrue('error' not in record, 'first close raised; original retained privately')
        self.assert_close_oracle(record['events_end'])
        return record

    def install_close_observer(self, *, owned=None):
        """Owned return ledger from BEFORE build; only named owned locals read.

        Strong permit/Attempt references stay internal; no token, exception text,
        args, foreign locals or traceback serialization. Missing values fail.
        """
        from app import actor_execution as execution
        from app import coordinator_execution as root_execution
        import dis

        def save_error(arg):
            error = arg[1]
            # arg[2] is the native traceback at this very traced propagation edge.
            return (error, arg[2], BaseException.__context__.__get__(error),
                    BaseException.__cause__.__get__(error))

        def result_opcode(frame):
            offset = frame.f_lasti
            code = frame.f_code.co_code
            return dis.opname[code[offset]] if 0 <= offset < len(code) else None

        specs = ((ActorHost, 'act', 'host_exit'),
            (WorldStore, 'release_actor_execution', 'actor_release'),
            (WorldStore, 'release_coordinator_execution', 'root_release'),
            (execution.Attempt, 'finish', 'actor_finish'),
            (root_execution.CoordinatorAttempt, 'finish', 'root_finish'))
        bindings = [(self.source_binding(cls, method), label) for cls, method, label in specs]
        self.close_events, self.close_permits, self.close_frames = [], [], {}
        self.close_phase = 'real_work'
        prior_thread, prior_main = threading.gettrace(), sys.gettrace()
        self.assertIsNone(prior_thread)
        self.assertIsNone(prior_main)
        def emit(item):
            self.assertLess(len(self.close_events), 512, 'finite fixture ledger overflow is failure')
            item['order'] = len(self.close_events)
            item['phase'] = self.close_phase
            self.close_events.append(item)
        # 1163 opt-in: extend the existing exact-code observer, never replace a
        # constructor, factory, close or CAS. These are running source frames,
        # not locals recovered from an exception traceback.
        if owned is not None:
            session_b = self.source_binding(product_cli.ProductSession, '__init__')
            coordinator_b = self.source_binding(Coordinator, '__init__')
            host_b = self.source_binding(ActorHost, '__init__')
            root_b = self.source_binding(root_execution.CoordinatorAttempt, 'start')
            worker_b = self.source_binding(Coordinator, '_start_bounded')
            coord_close_b = self.source_binding(Coordinator, 'close')
            host_close_b = self.source_binding(ActorHost, 'close')
            def before_start(binding, attr):
                lines = [n.lineno for n in ast.walk(binding['node']) if isinstance(n, ast.Expr)
                         and ast.unparse(n.value) == 'self.' + attr + '.start()']
                self.assertEqual(len(lines), 1)
                return lines[0]
            root_line = before_start(root_b, 'thread')
            worker_line = before_start(worker_b, 'worker')
            construction_frames = {}
            constructing = False

            def save_thread(thread):
                if not any(thread is old for old in owned['threads']):
                    owned['threads'].append(thread)

            def collect(frame, event, arg):
                nonlocal constructing
                if event != 'call':
                    return None
                binding = next((b for b in (session_b, coordinator_b, host_b, root_b, worker_b,
                                                coord_close_b, host_close_b) if frame.f_code is b['code']), None)
                if binding is None:
                    return None
                obj = frame.f_locals['self']
                if binding is session_b:
                    if self.session is not None or frame.f_locals['data_dir'] != self.owned:
                        return None
                    self.check_source_binding(binding)
                    owned['session'] = obj
                    constructing = True
                    self.close_phase = 'construction'
                    def exit_init(frame, event, arg):
                        nonlocal constructing
                        if event == 'return':
                            constructing = False
                            self.close_phase = 'real_work'
                        return exit_init
                    return exit_init
                if binding is coordinator_b and constructing and frame.f_locals['world'] is owned['session'].world:
                    self.check_source_binding(binding)
                    owned['coordinator'] = obj
                elif binding is host_b and constructing and frame.f_locals['world'] is owned['session'].world:
                    self.check_source_binding(binding)
                    owned['hosts'].append(obj)
                elif binding is root_b and owned['coordinator'] is not None and obj is owned['coordinator']._owner_attempt:
                    self.check_source_binding(binding)
                    owned['root'] = obj
                    self.original_root = obj
                    def root_start(frame, event, arg):
                        if event == 'line' and frame.f_lineno == root_line:
                            save_thread(obj.thread)
                        return root_start
                    return root_start
                elif binding is worker_b and obj is owned['coordinator']:
                    self.check_source_binding(binding)
                    def worker_start(frame, event, arg):
                        if event == 'line' and frame.f_lineno == worker_line:
                            save_thread(obj.worker)
                        return worker_start
                    return worker_start
                elif constructing and ((binding is coord_close_b and obj is owned['coordinator']) or
                                       (binding is host_close_b and any(obj is h for h in owned['hosts']))):
                    self.check_source_binding(binding)
                    row = dict(target=obj, status='entered', events_start=len(self.close_events), exceptions=[])
                    owned['construction_closes'].append(row)
                    construction_frames[frame] = row
                    def close_result(frame, event, arg):
                        if event == 'exception':
                            saved = save_error(arg)
                            prior = next((old for old in row['exceptions'] if old[0] is saved[0]), None)
                            if prior is None:
                                row['exceptions'].append(saved)
                            row['last_exception'] = saved if prior is None else prior
                        elif event == 'return':
                            # Exceptional trace return also carries None. Only a
                            # genuine return opcode establishes a return value.
                            op = dis.opname[frame.f_code.co_code[frame.f_lasti]]
                            if op in ('RETURN_VALUE', 'RETURN_CONST'):
                                row.update(status='returned', **{'return': arg})
                            elif 'last_exception' in row:
                                row.update(status='raised', error=row['last_exception'])
                            else:
                                row['status'] = 'completion_unknown'
                            row['events_end'] = len(self.close_events)
                            del construction_frames[frame]
                        return close_result
                    return close_result
                return None

        def trace(frame, event, arg):
            if owned is not None:
                local = collect(frame, event, arg)
                if local is not None:
                    return local
            selected = next(((b, label) for b, label in bindings if frame.f_code is b['code']), None)
            if selected is None:
                return None
            b, label = selected
            self.assertTrue(frame.f_globals is b['namespace'], 'owned return globals mismatch')
            # Exact source-owned self/permit and three booleans only; never dump.
            if event == 'call':
                s = self.session if owned is None else owned['session']
                if s is None:
                    return None
                obj = frame.f_locals.get('self')
                if label == 'host_exit':
                    mine = obj is s.host if owned is None else any(obj is h for h in owned['hosts'])
                elif label in ('actor_release', 'root_release'):
                    mine = obj is getattr(s, 'world', None)
                elif label == 'root_finish':
                    mine = obj is (s.coordinator._owner_attempt if owned is None else owned['root'])
                else:
                    mine = any(obj is a for a in self.attempts)
                if not mine:
                    return None
                permit = frame.f_locals.get('permit') if label.endswith('release') else getattr(obj, 'permit', None)
                ordinal = None
                if permit is not None:
                    if not any(permit is p for p in self.close_permits):
                        self.close_permits.append(permit)
                    ordinal = next(i for i,p in enumerate(self.close_permits) if permit is p)
                self.assertLess(len(self.close_frames), 64)
                self.close_frames[frame] = (label, ordinal, {}, [None])
                emit(dict(operation=label, event='call', permit=ordinal))
            elif frame in self.close_frames:
                label, ordinal, first_errors, latest = self.close_frames[frame]
                if event == 'exception':
                    error = arg[1]
                    if label == 'host_exit':
                        self.check_source_binding(b)
                        emit(dict(operation=label, event='exception', permit=ordinal,
                            error_type=type(error).__name__, code=getattr(error, 'code', None)))
                    else:
                        # One private first tuple per strong identity in this real call.
                        # The event cap also bounds this per-call cache.
                        saved = first_errors.get(id(error))
                        if saved is None:
                            saved = save_error(arg)
                        self.check_source_binding(b)
                        emit(dict(operation=label, event='exception', permit=ordinal, error=saved))
                        first_errors[id(error)] = saved
                        latest[0] = (frame.f_lasti, saved)
                elif event == 'return':
                    self.check_source_binding(b)
                    op = result_opcode(frame)
                    if label != 'host_exit' and op not in ('RETURN_VALUE', 'RETURN_CONST'):
                        seen = latest[0]
                        # CPython also emits a trace 'return' on unwind. Require
                        # a corresponding exception edge or the original re-raise.
                        if (arg is None and seen is not None
                                and (frame.f_lasti == seen[0] or op == 'RERAISE')):
                            item = dict(operation=label, event='raised', permit=ordinal, error=seen[1])
                        else:
                            item = dict(operation=label, event='unknown', permit=ordinal)
                    else:
                        item = dict(operation=label, event='return', permit=ordinal,
                            value=arg if label != 'host_exit' or type(arg) is bool or arg is None else 'UNKNOWN_NONBOOL')
                    if label == 'host_exit':
                        attempt = frame.f_locals.get('attempt')
                        self.assertTrue(any(attempt is a for a in self.attempts))
                        self.assertTrue(any(attempt.permit is p for p in self.close_permits))
                        item['permit'] = next(i for i,p in enumerate(self.close_permits) if attempt.permit is p)
                        for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                            value = frame.f_locals.get(key)
                            item[key] = value if type(value) is bool else 'UNKNOWN'
                    emit(item)
                    del self.close_frames[frame]
            return trace
        def restore():
            threading.settrace(prior_thread)
            sys.settrace(prior_main)
        self.close_observer_restore = restore
        threading.settrace(trace)
        sys.settrace(trace)
        self.addCleanup(restore)

    def observe_binding(self, binding, line, callback, armed):
        prior = threading.gettrace()
        self.assertIsNone(prior, 'do not displace a foreign observer')
        fired = False
        def trace(frame, event, arg):
            nonlocal fired
            if frame.f_code is not binding['code']:
                return None
            self.assertTrue(frame.f_globals is binding['namespace'], 'observer globals mismatch')
            if event == 'line' and frame.f_lineno == line and armed() and not fired:
                fired = True
                try:
                    self.check_source_binding(binding)
                    callback()
                except BaseException as error:
                    self.trace_error = error
                    raise
            if event == 'exception' and self.hits:
                self.final_errors.append(getattr(arg[1], 'code', type(arg[1]).__name__))
            return trace
        threading.settrace(trace)
        self.addCleanup(threading.settrace, prior)
        return lambda: threading.settrace(prior)

    def resource_record(self):
        s = self.session
        threads = [s.coordinator.worker, self.original_root.thread, *s.host._pending_threads]
        for attempt in self.attempts:
            threads.extend([attempt.thread, *attempt.calls])
        actual = []
        for thread in threads:
            if thread is not None and not any(thread is old for old in actual):
                actual.append(thread)
        for thread in actual:
            thread.join(0)
        return dict(live=[thread.name for thread in actual if thread.is_alive()],
            host_pending=s.host.cleanup_pending, coordinator_pending=s.coordinator.cleanup_pending,
            lease_cleanups=len(s.host._lease_cleanups), lifecycle_active=s.host._lifecycle_active is not None)

    def source_binding(self, cls, method):
        """Finite full-module compile-only reference; never exec extracted code."""
        function = getattr(cls, method)
        module = sys.modules[function.__module__]
        namespace = vars(module)
        self.assertTrue(function.__globals__ is namespace, 'loaded function globals mismatch')
        self.assertIs(namespace[cls.__name__], cls)
        self.assertIs(getattr(cls, method), function)
        root = product_cli.ROOT.resolve(strict=True)
        path = Path(function.__code__.co_filename).resolve(strict=True)
        self.assertTrue(path.is_relative_to(root))
        self.assertEqual(Path(module.__file__).resolve(strict=True), path)
        relative = path.relative_to(root).as_posix()
        manifest_raw = product_cli.MANIFEST.read_bytes()
        manifest = json.loads(manifest_raw)
        self.assertEqual(Path(manifest['root']).resolve(strict=True), root)
        raw = path.read_bytes()
        pin = dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        self.assertEqual(manifest['files'][relative], pin)
        tree = ast.parse(raw, filename=function.__code__.co_filename)
        owners = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls.__name__]
        self.assertEqual(len(owners), 1)
        nodes = [n for n in owners[0].body if isinstance(n, ast.FunctionDef) and n.name == method]
        self.assertEqual(len(nodes), 1)
        compiled = compile(raw, function.__code__.co_filename, 'exec', dont_inherit=True)
        codes = []
        def collect(code):
            if code.co_qualname == function.__qualname__:
                codes.append(code)
            for value in code.co_consts:
                if isinstance(value, type(code)):
                    collect(value)
        collect(compiled)
        self.assertEqual(len(codes), 1)
        self.assertEqual(function.__code__, codes[0])
        self.assertEqual(function.__code__.co_filename, codes[0].co_filename)
        return dict(cls=cls, method=method, function=function, code=function.__code__,
            module=module, namespace=namespace, root=root, path=path, raw=raw,
            pin=pin, manifest_raw=manifest_raw, node=nodes[0])

    def setUp(self):
        self.deadline = time.monotonic() + 120
        self.root = SCRATCH / ('parent1100-authority-owned-' + uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.session = None
        self.calls = []
        self.violations = []
        self.target = OLD
        self.provider_code = None
        self.attack_armed = False
        self.final_armed = False
        self.hits = []
        self.attempts = []
        self.final_errors = []
        self.trace_error = None
        self.root_entry = None
        self.transport = SyntheticTransport(self)
        self.guards = ExitStack()
        self.addCleanup(self.guards.close)
        for name in ('socket.socket.connect', 'socket.socket.connect_ex', 'socket.create_connection',
                     'subprocess.Popen', 'http.client.HTTPConnection.request',
                     'app.real_settings.default_credential_factory', 'app.real_provider.default_credential_factory'):
            self.guards.enter_context(patch(name, self.forbidden))

    def forbidden(self, *args, **kwargs):
        self.violations.append('external-or-scripted')
        raise AssertionError('No network/SDK/account/vault/scripted consumer')

    def budget(self):
        self.assertLess(time.monotonic(), self.deadline, 'whole120 exhausted; no renewal')

    def rows(self, name, private=False):
        self.assertIn(name, PRIVATE if private else PUBLIC + CONTROL)
        path = self.session.directory / ('actors.sqlite' if private else 'world.sqlite')
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute('SELECT * FROM ' + name + ' ORDER BY rowid')]

    def raw(self):
        return json.loads(self.rows('snapshots')[-1]['state'])

    def business(self):
        return dict(public={n: self.rows(n) for n in PUBLIC}, private={n: self.rows(n, True) for n in PRIVATE})

    def controls(self):
        return {name: self.rows(name) for name in CONTROL}

    def unchanged_controls(self, before):
        after = self.controls()
        for name in ('actor_execution_leases', 'actor_coordinator_delegations', 'actor_activation_delegations'):
            self.assertEqual(after[name], before[name])
        self.assertEqual(len(after['coordinator_execution_leases']), 1)
        for key, value in before['coordinator_execution_leases'][0].items():
            if key in ('expires_wall_ns', 'last_wall_ns'):
                self.assertGreaterEqual(after['coordinator_execution_leases'][0][key], value)
            else:
                self.assertEqual(after['coordinator_execution_leases'][0][key], value)
        self.assertEqual(len(after['execution_clock']), 1)
        self.assertEqual(after['execution_clock'][0]['singleton'], 1)
        self.assertGreaterEqual(after['execution_clock'][0]['high_water_wall_ns'],
                                before['execution_clock'][0]['high_water_wall_ns'])

    def build(self, *, remote=False, unheld=False):
        self.budget()
        pack = product_cli.SCENARIOS / 'lifecycle-v1-iron-4.json'
        raw = pack.read_bytes()
        manifest = json.loads(product_cli.MANIFEST.read_bytes())
        self.assertEqual(manifest['files']['app/data/scenarios/lifecycle-v1-iron-4.json'],
                         dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest()))
        config = json.loads(raw)
        self.assertEqual(config['event_wakeup']['version'], 'p04-event-wakeup-v2-lifecycle')
        self.assertEqual(config['lifecycle']['death_delay_seconds'], 1)
        self.assertEqual(config['horizon_days'], 90)
        self.unheld = unheld
        if remote:
            self.target = REMOTE  # Actual original place-1, no runtime teleport.
        if hasattr(self, 'object_case_id'):
            self.assertFalse(unheld, 'legal-ID variants must not launder the negative fixture')
            config['lifecycle']['personal_objects'][0]['id'] = self.object_case_id
            config['lifecycle']['personal_objects'].append(dict(id='private-old-object-1106-authority',
                kind='held_iron_weapon', holder_person_id=OLD,
                source_text='仅旧财政官本人实持的独立私人铁器1106-authority，不随职位交付。'))
            pack = self.root / 'owned-object-genesis.json'
            pack.write_text(canonical(config) + '\n', encoding='utf-8')
        self.fixture_means_id = config['lifecycle']['personal_objects'][0]['id']
        if unheld:
            config['lifecycle']['personal_objects'][0]['holder_person_id'] = OLD
            pack = self.root / 'owned-unheld-genesis.json'
            pack.write_text(canonical(config) + '\n', encoding='utf-8')
        self.owned = self.root / 'world'
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(product_cli.main(['--data-dir', str(self.owned), 'init', '--scenario', str(pack)]), 0)
        self.assertFalse(json.loads(output.getvalue())['real_model'])
        self.services = {kind: dict(provider='openai-chat-loopback', base_url='http://127.0.0.1:' + port + '/v1',
            model=kind + '-authority1100-synthetic', credential_ref=None, timeout_seconds=2, max_retries=0)
            for kind, port in (('roles', '18181'), ('dm', '18182'))}
        settings = SettingsStore(self.owned / 'services.json', credential_factory=self.forbidden)
        settings.save(dict(services=self.services, secrets={}))
        self.assertEqual(settings.snapshot(), self.services)
        self.session = product_cli.ProductSession(self.owned, 'real')
        s = self.session
        for obj, cls in ((s, product_cli.ProductSession), (s.host, ActorHost), (s.world, WorldStore),
                         (s.coordinator, Coordinator), (s.host.adjudicator, Adjudicator),
                         (s.host.real_gateway, RealGateway), (s.host.real_settings, SettingsStore)):
            self.assertIs(type(obj), cls)
        self.assertIs(s.host.world, s.world)
        self.assertIs(s.coordinator.actors, s.host)
        self.assertIs(s.host.adjudicator.world, s.world)
        self.assertIs(s.host._request.__func__, ActorHost._request)
        self.assertTrue(s.host.execution_fenced)
        for policy in (s.host.execution_policy, s.coordinator.execution_policy):
            self.assertEqual((policy.busy_budget_seconds, policy.act_deadline_seconds), (2, 120))
        s.host._choose = self.forbidden  # Detection sentinel, never returns behavior.
        s.host.real_gateway.transport = self.transport
        self.source_identity = product_cli.source_identity(s.world, s.host, s.coordinator)
        self.generation = product_cli.verify_generation()
        self.original_root = s.coordinator._owner_attempt
        self.initial = self.raw()
        self.genesis = copy.deepcopy(self.initial)
        people = {p['id']: p for p in self.initial['persons']}
        self.assertEqual((people[KING]['location'], people[OLD]['location'], people[REMOTE]['location']),
                         ('place-0', 'place-0', 'place-1'))

    def step(self, at):
        self.budget()
        state = self.session.step(60)
        self.budget()
        self.assertEqual((state['time'], state['status']), (at, 'user_paused'))
        self.assertIsNone(self.session.coordinator.error)
        self.assertFalse(self.session.coordinator.worker.is_alive())
        self.assertTrue(self.rows('coordinator_windows'))
        self.assertEqual(self.rows('coordinator_windows')[-1]['state'], 'completed')
        return state

    def start_join(self):
        self.budget()
        self.session.coordinator.start(continuous=False)
        worker = self.session.coordinator.worker
        worker.join(min(60, max(0, self.deadline - time.monotonic())))
        self.assertFalse(worker.is_alive(), 'actual coordinator worker must exit')
        self.budget()
        if self.trace_error is not None:
            raise self.trace_error
        return self.session.coordinator.error

    def injury(self):
        self.step(0)
        old = next(a for a in self.rows('coordinator_activations') if a['person_id'] == OLD)
        self.assertEqual((old['phase'], old['private_closed']), ('completed', 1))
        self.old_activation = copy.deepcopy(old)
        self.old_ref = scheduler.ActivationRef(**{k: old[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id')})
        self.old_receipt = self.session.world.receipt(OLD, old['intent_id'])
        self.assertIsNotNone(self.old_receipt)
        self.old_seq = next(r['completed_revision'] for r in self.rows('coordinator_windows')
                            if r['window_id'] == old['window_id'])
        old_private = next(r for r in self.rows('actor_intents', True) if r['intent_id'] == old['intent_id'])
        self.assertIn(SECRET, old_private['candidate'])
        self.step(60)
        raw = self.raw()
        transitions = [x for x in raw['_lifecycle']['transitions'] if x['kind'] == 'mortal_injury']
        self.assertEqual(len(transitions), 1)
        injury = transitions[0]
        self.assertEqual((injury['actor_person_id'], injury['person_id'], injury['occurred_at'], injury['means_id']),
                         (KING, OLD, 60, self.fixture_means_id))
        nodes = [q for q in raw['queue'] if q['kind'] == 'person_death_due']
        self.assertEqual(nodes, [injury['node']])
        self.assertEqual(nodes[0]['at'], 61)
        self.assertEqual(raw['_lifecycle']['deaths'], [])
        self.assertEqual(raw['_lifecycle']['jobs'], [])
        self.injury_record = copy.deepcopy(injury)

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
                self.assertNotIn(item['id'], canonical(messages))
                self.assertNotIn(item['source_text'], canonical(messages))
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

    def source_site(self, cls, method, statement):
        binding = self.source_binding(cls, method)
        candidates = sorted((n for n in ast.walk(binding['node']) if isinstance(n, ast.stmt)
            and ast.get_source_segment(binding['raw'].decode('utf-8'), n) == statement), key=lambda n: n.lineno)
        self.assertEqual(len(candidates), 1, 'exact current source/AST site must remain unique')
        return binding['code'], candidates[0].lineno

    def observe(self, cls, method, statement, callback, armed):
        binding = self.source_binding(cls, method)
        candidates = sorted((n for n in ast.walk(binding['node']) if isinstance(n, ast.stmt)
            and ast.get_source_segment(binding['raw'].decode('utf-8'), n) == statement), key=lambda n:n.lineno)
        self.assertEqual(len(candidates), 1)
        return self.observe_binding(binding, candidates[0].lineno, callback, armed)

    def test_world_committed_private_open_blocks_due_and_original_finish_cas(self):
        self.build()
        def before_finish():
            self.attack_armed = False
            before = self.business()
            raw = self.raw()
            self.assertEqual(raw['time'], 60)
            injury = [x for x in raw['_lifecycle']['transitions'] if x['kind'] == 'mortal_injury']
            self.assertEqual(len(injury), 1)
            active = next(a for a in self.rows('coordinator_activations') if a['intent_id'] == self.attack_intent)
            self.assertEqual((active['phase'], active['private_closed']), ('world_committed', 0))
            leaf = next(r for r in self.rows('actor_intents', True) if r['intent_id'] == self.attack_intent)
            self.assertEqual(leaf['state'], 'candidate')
            self.assertIsNone(leaf['receipt'])
            with self.assertRaises(WorldError) as caught:
                self.session.world.advance(61, coordinator=self.original_root.permit)
            self.assertEqual(caught.exception.code, 'WINDOW_INCOMPLETE')
            self.assertEqual(self.business(), before)
            self.hits.append(dict(activation=active, leaf=leaf, before=before))
        restore = self.observe(ActorHost, '_act',
            'self._finish(intent_id, person_id, _json(result[\'messages\']), receipt, execution=execution, attempt_id=attempt_id)',
            before_finish, lambda: self.attack_armed)
        try:
            self.injury()
        finally:
            restore()
        if self.trace_error is not None:
            raise self.trace_error
        self.assertEqual(len(self.hits), 1)
        prior = self.hits[0]
        a = next(a for a in self.rows('coordinator_activations') if a['activation_id'] == prior['activation']['activation_id'])
        leaf = next(r for r in self.rows('actor_intents', True) if r['intent_id'] == a['intent_id'])
        self.assertEqual((a['phase'], a['private_closed'], leaf['state']), ('completed', 1, 'committed'))
        for key in ('intent_id', 'body', 'candidate', 'request', 'consumed', 'metrics'):
            self.assertEqual(leaf[key], prior['leaf'][key])
        head = next(r for r in self.rows('actor_session_heads', True) if r['person_id'] == KING)
        self.assertEqual((head['version'], head['last_promoted_intent_id']),
                         (prior['activation']['base_session_version'] + 1, a['intent_id']))
        self.assertEqual(json.loads(leaf['receipt']), self.session.world.receipt(KING, a['intent_id']))
        self.step(61)
        self.assertEqual([d['person_id'] for d in self.raw()['_lifecycle']['deaths']], [OLD])

    def test_live_old_permit_person_dead_expired_fenced_release_cas_and_receipt_readonly(self):
        self.build()
        self.injury()
        w, c = self.session.world, self.session.coordinator
        with c._owner_scope():
            permit = w.acquire_actor_execution(OLD, 'authority1100-owned', ttl_seconds=120,
                coordinator=self.original_root.permit, activation=self.old_ref)
        lease = next(r for r in self.rows('actor_execution_leases') if r['person_id'] == OLD)
        self.assertEqual((lease['epoch'], lease['owner_token']), (permit.epoch, permit.owner_token))
        self.step(61)
        after = next(r for r in self.rows('actor_execution_leases') if r['person_id'] == OLD)
        self.assertEqual(after, lease, 'death must not revoke or silently replace a valid SQL lease')
        self.assertLess(time.time_ns(), after['expires_wall_ns'])
        self.assertIs(c._owner_attempt, self.original_root)
        frozen, calls = self.business(), len(self.calls)
        controls = self.controls()
        with c._owner_scope():
            with self.assertRaises(WorldError) as caught:
                w.submit(OLD, self.old_activation['intent_id'], dict(action='wait'), execution=permit, activation=self.old_ref)
            self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_PERSON_DEAD')
        self.assertEqual(self.business(), frozen)
        self.unchanged_controls(controls)
        # Preserve original expiration priority using only the actual owned lease
        # row, after the positive still-valid PERSON_DEAD case, not a fake permit.
        with closing(sqlite3.connect(self.owned / 'world.sqlite')) as db:
            changed = db.execute('UPDATE actor_execution_leases SET expires_wall_ns=last_wall_ns WHERE world_id=? AND person_id=? AND epoch=? AND owner_token=?',
                                 (permit.world_id, OLD, permit.epoch, permit.owner_token)).rowcount
            self.assertEqual(changed, 1)
            db.commit()
        with c._owner_scope():
            with self.assertRaises(WorldError) as caught:
                w.submit(OLD, self.old_activation['intent_id'], dict(action='wait'), execution=permit, activation=self.old_ref)
            self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_EXPIRED')
        self.assertIs(w.release_actor_execution(permit), True)
        self.assertIs(w.release_actor_execution(permit), False)
        with c._owner_scope():
            with self.assertRaises(WorldError) as caught:
                w.submit(OLD, self.old_activation['intent_id'], dict(action='wait'), execution=permit, activation=self.old_ref)
            self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_FENCED')
            with self.assertRaises(WorldError) as caught:
                self.session.host.act(OLD, coordinator=self.original_root.permit, activation=self.old_ref)
            self.assertEqual(caught.exception.code, 'ACTOR_EXECUTION_PERSON_DEAD')
        read_before = self.business()
        self.assertEqual(w.receipt(OLD, self.old_activation['intent_id']), self.old_receipt)
        self.assertEqual(w.receipt(OLD, self.old_activation['intent_id']), self.old_receipt)
        self.assertEqual(w.observe(OLD, seq=self.old_seq)['person']['id'], OLD)
        self.assertTrue(self.session.host.adjudicator.trace()['proposals'])
        self.assertEqual(self.business(), read_before)
        self.assertEqual(self.business(), frozen)
        self.assertEqual(len(self.calls), calls)

    def domain_case(self, code, *, remote=False, unheld=False, provider=False):
        self.build(remote=remote, unheld=unheld)
        self.step(0)
        if provider:
            self.provider_code = code
            error = self.start_join()
            self.assertEqual(error, 'ADJUDICATION_FAILED')
            self.assertEqual(self.raw()['status'], 'technical_paused')
            outcomes = self.rows('coordinator_activation_outcomes')
            self.assertFalse(any(r['intent_id'] == self.attack_intent and r['kind'] == 'domain_rejected' for r in outcomes))
            a = next(a for a in self.rows('coordinator_activations') if a['intent_id'] == self.attack_intent)
            self.assertEqual(a['phase'], 'technical_blocked')
        else:
            self.step(60)
            a = next(a for a in self.rows('coordinator_activations') if a['intent_id'] == self.attack_intent)
            self.assertEqual((a['phase'], a['terminal_reason'], a['private_closed']), ('rejected', code, 1))
            outcome = next(r for r in self.rows('coordinator_activation_outcomes') if r['intent_id'] == self.attack_intent)
            data = json.loads(outcome['outcome_json'])
            self.assertEqual((outcome['kind'], data['status'], data['code'], data['origin'], data['action'], data['effect_index']),
                ('domain_rejected', 'domain_rejected', code, 'adjudication.execute', 'resolve_physical_attempt', 0))
            self.assertEqual(data['effects'], [])
            decisions = [r for r in self.rows('dm_decisions') if r['intent_id'] == self.attack_intent]
            self.assertEqual(len(decisions), 1)
            self.assertEqual(data['decision_sha256'], sha(json.loads(decisions[0]['data'])))
            self.assertEqual(self.rows('intents'), self.attack_before['public']['intents'])
            for name in ('actor_sessions', 'actor_session_heads', 'actor_intent_execution', 'actor_calls'):
                self.assertEqual(self.rows(name, True), self.attack_before['private'][name])
            expected = copy.deepcopy(self.attack_before['private']['actor_intents'])
            next(r for r in expected if r['intent_id'] == self.attack_intent)['state'] = 'rejected'
            self.assertEqual(self.rows('actor_intents', True), expected)
        self.assertTrue(self.attack_before['private']['actor_intents'])
        self.assertTrue(any(c['kind'] == 'dm' and c['payload']['proposal']['intent_id'] == self.attack_intent for c in self.calls))
        life = self.raw()['_lifecycle']
        self.assertEqual(life['deaths'], [])
        self.assertEqual(life['jobs'], [])
        self.assertEqual([x for x in life['transitions'] if x['kind'] == 'mortal_injury'], [])
        self.assertEqual([q for q in self.raw()['queue'] if q['kind'] == 'person_death_due'], [])

    def test_actual_remote_physical_attempt_domain_rejection(self):
        self.domain_case('NOT_AT_LOCATION', remote=True)

    def test_actual_unheld_means_domain_rejection(self):
        self.domain_case('PERSONAL_MEANS_REQUIRED', unheld=True)

    def test_transport_not_at_location_code_is_technical_not_domain(self):
        self.domain_case('NOT_AT_LOCATION', remote=True, provider=True)

    def test_transport_personal_means_code_is_technical_not_domain(self):
        self.domain_case('PERSONAL_MEANS_REQUIRED', unheld=True, provider=True)

    def final_case(self, fault):
        self.build()
        self.injury()
        w, h = self.session.world, self.session.host
        other_world = WorldStore(w.db_path, scenario_path=self.owned / 'scenario.json')
        other_settings = SettingsStore(self.owned / 'services.json', credential_factory=self.forbidden)
        self.assertIsNot(other_world, w)
        self.assertIsNot(other_settings, h.real_settings)
        old_settings = h.real_settings
        def inject():
            self.final_armed = False
            # This is the genuine scoped Work captured by the original Host,
            # not a frame local, fake row, substitute connection or authority.
            work = scheduler.lifecycle_work(w, self.root_request['job_id'])
            self.assertIs(work.root, self.original_root)
            self.assertIs(work.entry, self.root_entry)
            self.assertIs(work.db._attempt, self.root_entry)
            self.assertTrue(work.db.in_transaction)
            tentative = copy.deepcopy(work.state)
            self.assertIsNotNone(next(j for j in tentative['_lifecycle']['jobs'] if j['id'] == self.root_request['job_id'])['result'])
            self.assertIn(self.root_request['proposed_person_id'], [p['id'] for p in tentative['persons']])
            self.assertEqual(self.business(), self.prepared, 'external reader sees only durable prepared baseline')
            self.hits.append(dict(site=fault, tentative=tentative))
            if fault == 'root_sql':
                changed = work.db.execute('UPDATE coordinator_execution_leases SET owner_token=NULL WHERE world_id=? AND epoch=? AND owner_token=?',
                    (self.original_root.permit.world_id, self.original_root.permit.epoch, self.original_root.permit.owner_token)).rowcount
                self.assertEqual(changed, 1)
            elif fault == 'cancel':
                self.original_root.stop.set()
            elif fault == 'entry':
                self.root_entry.deadline = time.monotonic() - 1
            elif fault == 'root_object':
                self.original_root.world = other_world
            elif fault == 'source_object':
                h.real_settings = other_settings
            else:
                raise AssertionError('unknown finite fault')
        statement = "self._check_coordinator_execution(db, state['world_id'], None)" if fault == 'root_sql' else 'if _precommit is not None:_precommit(db)'
        # Root SQL statement occurs twice in _lc_geo1133_mutate_body. Select the final statement
        # by its AST adjacency to the exact real _precommit, without fuzzy lines.
        if fault == 'root_sql':
            statement = 'if _precommit is not None:_precommit(db)'
            # SQL lease injection at Work final would be TOO LATE: final Work
            # intentionally performs no SQL. Use dedicated final-SQL observer.
            restore = self.observe_final_sql(inject)
        else:
            restore = self.observe(WorldStore, '_lc_geo1133_mutate_body', statement, inject, lambda: self.final_armed)
        try:
            error = self.start_join()
        finally:
            restore()
            self.original_root.world = w
            h.real_settings = old_settings
        expected = dict(root_sql='COORDINATOR_EXECUTION_FENCED', cancel='COORDINATOR_EXECUTION_CANCELLED',
                        entry='ACTOR_EXECUTION_DEADLINE', root_object='ACTIVATION_BINDING_CONFLICT',
                        source_object='WINDOW_RECOVERY_UNPROVEN')
        self.assertEqual(error, expected[fault])
        self.assertEqual(len(self.hits), 1, 'no earlier gate is credited as final rollback')
        self.assertIn('WINDOW_RECOVERY_UNPROVEN' if fault in ('root_object', 'source_object') else expected[fault],
                      self.final_errors, 'actual original transaction must raise at its final gate')
        self.assertEqual(self.business(), self.prepared)
        current = self.raw()
        job = next(j for j in current['_lifecycle']['jobs'] if j['id'] == self.root_request['job_id'])
        self.assertIsNone(job['result'])
        self.assertEqual(job['attempts'][-1]['status'], 'pending')
        self.assertNotIn(self.root_request['proposed_person_id'], [p['id'] for p in current['persons']])
        self.assertEqual([c['kind'] for c in self.calls].count('succession'), 1)
        self.assertEqual(current['status'], 'running', 'authority stop must not fabricate vacant/technical pause')
        # SQL fault itself rolled back, not merely the proposed person.
        root_lease = self.rows('coordinator_execution_leases')[0]
        self.assertEqual((root_lease['epoch'], root_lease['owner_token']),
                         (self.original_root.permit.epoch, self.original_root.permit.owner_token))

    def observe_final_sql(self, callback):
        binding = self.source_binding(WorldStore, '_lc_geo1133_mutate_body')
        text = binding['raw'].decode('utf-8')
        calls = sorted((n for n in ast.walk(binding['node']) if isinstance(n, ast.Expr) and
            ast.get_source_segment(text, n) == "self._check_coordinator_execution(db, state['world_id'], None)"), key=lambda n:n.lineno)
        self.assertEqual(len(calls), 2)
        pre = [n for n in ast.walk(binding['node']) if isinstance(n, ast.If) and
            ast.get_source_segment(text, n) == 'if _precommit is not None:_precommit(db)']
        self.assertEqual(len(pre), 1)
        self.assertEqual(pre[0].lineno, calls[-1].end_lineno + 1)
        return self.observe_binding(binding, calls[-1].lineno, callback, lambda: self.final_armed)

    def test_last_root_sql_loss_rolls_back_tentative_successor(self):
        self.final_case('root_sql')

    def test_last_work_cancel_rolls_back_tentative_successor(self):
        self.final_case('cancel')

    def test_last_work_entry_deadline_rolls_back_tentative_successor(self):
        self.final_case('entry')

    def test_last_work_root_world_object_drift_rolls_back(self):
        self.final_case('root_object')

    def test_last_work_real_settings_source_object_drift_rolls_back(self):
        self.final_case('source_object')

    def test_original_root_commit_positive_and_same_scope_completed_job_replay(self):
        self.build()
        self.injury()
        def replay():
            self.final_armed = False
            a = self.session.host.adjudicator
            q, response = self.root_request, self.root_response
            request_sha = sha(q)
            raw = canonical(response)
            before, calls = self.business(), len(self.calls)
            result = next(j for j in self.raw()['_lifecycle']['jobs'] if j['id'] == q['job_id'])['result']
            self.assertIsNotNone(result)
            self.assertEqual(a.prepare_lifecycle(q['job_id'], q['service_version']), dict(status='completed', result=result))
            self.assertEqual(a.preview_lifecycle(q['job_id'], request_sha, raw, response), dict(status='completed', result=result))
            self.assertEqual(a.commit_lifecycle(q['job_id'], request_sha, raw, response, decision_sha256=sha(response)), result)
            self.assertEqual(self.business(), before)
            self.assertEqual(len(self.calls), calls)
            self.hits.append(copy.deepcopy(result))
        restore = self.observe(ActorHost, 'resolve_lifecycle_jobs', 'results.append(result)', replay, lambda: self.final_armed)
        try:
            self.step(61)
        finally:
            restore()
        if self.trace_error is not None:
            raise self.trace_error
        self.assertEqual(len(self.hits), 1)
        result = self.hits[0]
        self.assertEqual(result['status'], 'appointed')
        self.assertEqual(result['person_id'], self.root_request['proposed_person_id'])
        self.assertNotEqual(result['person_id'], OLD)
        self.assertEqual([c['kind'] for c in self.calls].count('succession'), 1)
        self.assertEqual(len([p for p in self.raw()['persons'] if p['id'] == result['person_id']]), 1)
        self.assertEqual(self.session.world.receipt(OLD, self.old_activation['intent_id']), self.old_receipt)

    def test_blocking_root_transport_gateway_close_worker_exit_no_ghost_commit(self):
        self.install_close_observer()  # before original root/Host work
        self.build()
        self.original_root.deadline = min(self.original_root.deadline, self.deadline)
        self.injury()
        self.transport.block_root = True
        self.session.coordinator.start(continuous=False)
        worker = self.session.coordinator.worker
        self.assertTrue(self.transport.entered.wait(min(60, max(0, self.deadline - time.monotonic()))))
        self.assertTrue(worker.is_alive())
        self.assertIs(self.transport.root_thread, worker)
        self.assertIsNone(self.session.host._attempt, 'root DM must not masquerade as actor')
        original_gateway = self.session.host.real_gateway
        # Native Coordinator.close -> Host._cancel_lifecycle -> ORIGINAL
        # RealGateway.close -> this transport.close. Never signal it ourselves.
        self.close_record()
        worker.join(0)
        self.assertFalse(worker.is_alive())
        self.assertTrue(self.transport.cancelled.is_set())
        self.assertTrue(self.transport.exited.is_set())
        self.assertTrue(self.transport.close_threads)
        self.assertIs(self.session.host.real_gateway, original_gateway)
        self.assertFalse(self.session.host.cleanup_pending)
        self.assertFalse(self.session.coordinator.cleanup_pending)
        self.assertFalse(self.original_root.thread.is_alive())
        self.assertIsNone(self.session.host._lifecycle_active)
        self.assertFalse(any(t.is_alive() for t in self.session.host._pending_threads))
        self.assertEqual(self.business(), self.prepared, 'valid late response after close must not ghost commit')
        self.assertIsNone(self.raw()['_lifecycle']['jobs'][0]['result'])
        self.assertNotIn(self.root_request['proposed_person_id'], [p['id'] for p in self.raw()['persons']])
        self.assertEqual(self.session.coordinator.error, 'COORDINATOR_EXECUTION_CANCELLED')
        calls = len(self.calls)
        self.assertEqual(self.business(), self.prepared)
        self.assertEqual(len(self.calls), calls)

    def object_input_complete_case(self, object_id):
        self.object_case_id = object_id
        # Same entire original positive/replay case, same setUp deadline.
        self.test_original_root_commit_positive_and_same_scope_completed_job_replay()
        new = self.root_request['proposed_person_id']
        self.assertEqual(self.fixture_means_id, object_id)
        self.assertEqual(self.raw()['_lifecycle']['rules']['personal_objects'][0]['id'], object_id)
        self.step(61)  # Genuine NEW role request, not a handcrafted view.
        calls = [c for c in self.calls if c['kind'] == 'roles' and c['payload']['person']['id'] == new]
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]['payload']['personal_objects'], [])
        self.assertNotIn('private-old-object-1106-authority', canonical(calls[0]['messages']))
        self.assertEqual(self.session.world.observe(OLD, seq=1)['personal_objects'], [
            dict(id='private-old-object-1106-authority', kind='held_iron_weapon',
                 description='仅旧财政官本人实持的独立私人铁器1106-authority，不随职位交付。')])
        self.assertEqual(self.session.world.observe(new)['personal_objects'], [])
        self.budget()

    def test_role_object_alpha_complete_root_replay_and_new_privacy(self):
        self.object_input_complete_case('owned-independent-alpha-1106-authority')

    def test_role_object_beta_complete_root_replay_and_new_privacy(self):
        self.object_input_complete_case('owned-independent-beta-1106-authority')

    def tearDown(self):
        try:
            if self.session is not None:
                if hasattr(self, 'close_events'):
                    self.close_phase = 'repeat_cleanup' if hasattr(self, 'first_close_record') else 'first_close'
                self.session.close()
                self.assertFalse(self.session.host.cleanup_pending)
                self.assertFalse(self.session.coordinator.cleanup_pending)
                worker = self.session.coordinator.worker
                self.assertFalse(worker is not None and worker.is_alive())
                self.assertFalse(self.original_root.thread.is_alive())
                self.assertTrue(self.attempts, 'real role producer must have run')
                for attempt in self.attempts:
                    attempt.thread.join(0)
                    self.assertFalse(attempt.thread.is_alive())
                    self.assertTrue(attempt.calls)
                    for provider_thread in attempt.calls:
                        provider_thread.join(0)
                        self.assertFalse(provider_thread.is_alive())
                if hasattr(self, 'close_events'):
                    self.assert_close_oracle()
                self.assertEqual(self.session.host._lease_cleanups, [])
                self.assertIsNone(self.session.host._process)
                self.assertEqual(product_cli.verify_generation(), self.generation)
                self.assertEqual(product_cli.source_identity(self.session.world, self.session.host, self.session.coordinator), self.source_identity)
            self.assertEqual(self.violations, [])
            self.budget()
        finally:
            if hasattr(self, 'close_observer_restore'):
                self.close_observer_restore()
            self.guards.close()


if __name__ == '__main__':
    unittest.main()
