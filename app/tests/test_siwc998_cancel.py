"""998 prospective cross-thread regressions. NOT EXECUTED during preparation.

TEST_WAIT is a test harness hang detector, never a replacement product budget.
No real network: actual PublicHTTP.request plus controlled connection boundaries.
Only the test owner releases the gates, always in finally, after control assertions.
"""
from contextlib import contextmanager, nullcontext
import json
import queue
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from app.real_provider import ProviderFault, RequestBudget, RealGateway
from app.siwc_responses import API, PublicHTTP
from app.tests.siwc979_synthetic import Harness

TEST_WAIT = 4


class Socket:
    def __init__(self):self.closed = False;self.shutdowns = 0
    def shutdown(self, how):self.shutdowns += 1
    def close(self):self.closed = True
    def settimeout(self, seconds):pass


class Reply:
    status = 200
    def __init__(self):self.parts = [b'{"models":[]}', b''];self.closed = False
    def getheader(self, name, default=''):return 'application/json'
    def read1(self, n):return self.parts.pop(0)
    def close(self):self.closed = True


class Gate:
    def __init__(self):self.entered = threading.Event();self.release = threading.Event()
    def wait(self):
        self.entered.set()
        if not self.release.wait(TEST_WAIT * 3):raise AssertionError('owner failed to release gate')


class Connection:
    """Deliberately reconnects like http.client unless auto_open was disabled.

    request hangs BEFORE calling send: cancelling a previously admitted request
    must still prevent that later send and any reconnect after close.
    """
    def __init__(self, gate, stage='request', context=None):
        self.gate = gate;self.stage = stage;self._context = context
        self.sock = None;self.auto_open = 1;self.sent = [];self.sockets = []
        self.request_entries = 0;self.connect_entries = 0;self.closed = False
        self.response = Reply();self._create_connection = self.create_socket
    def create_socket(self, *args, **kwargs):
        if self.stage == 'dns':self.gate.wait()
        result = Socket();self.sockets.append(result);return result
    def connect(self):
        self.connect_entries += 1
        self.sock = self._create_connection(('synthetic', 443))
        self.sock = self._context.wrap_socket(self.sock)
        if self.stage == 'connect':self.gate.wait()
    def send(self, data):
        if self.sock is None:
            if not self.auto_open:raise OSError('auto reconnect forbidden')
            self.connect()
        if self.sock.closed:raise OSError('closed socket')
        self.sent.append(data)
    def request(self, *args, **kwargs):
        self.request_entries += 1
        if self.stage == 'request':self.gate.wait()
        self.send(b'SYNTHETIC_WIRE')
        if self.stage == 'sent':self.gate.wait()
    def getresponse(self):return self.response
    def close(self):
        self.closed = True
        if self.sock is not None:self.sock.close()
        self.sock = None


class TLS:
    def __init__(self, gate, stage):self.gate = gate;self.stage = stage;self.results = []
    def load_default_certs(self):pass
    def wrap_socket(self, sock, **kwargs):
        if self.stage == 'tls':self.gate.wait()
        self.results.append(sock);return sock


class Workers:
    def __init__(self, case):self.case = case;self.items = []
    def start(self, function):
        done = threading.Event();outcome = []
        def run():
            try:outcome.append((True, function()))
            except BaseException as error:outcome.append((False, error))
            finally:done.set()
        worker = threading.Thread(target=run, daemon=True)
        self.items.append(worker);worker.start()
        return done, outcome
    def join(self):
        for worker in self.items:worker.join(TEST_WAIT)
        for worker in self.items:self.case.assertFalse(worker.is_alive(), 'leaked test worker')


class CancellationTests(unittest.TestCase):
    @contextmanager
    def fixture(self, stage='request'):
        with tempfile.TemporaryDirectory() as directory:
            h = Harness(directory);key = h.authorize()
            transport = PublicHTTP();h.session.http = transport
            h.session.lifecycle_lock = transport.lock
            gate = Gate();tls = TLS(gate, stage);connections = []
            def factory(*args, **kwargs):
                connection = Connection(gate, stage, kwargs['context']);connections.append(connection)
                if stage == 'constructor':gate.wait()
                return connection
            workers = Workers(self)
            with patch('app.siwc_responses.ssl.SSLContext', return_value=tls), patch(
                    'app.siwc_responses.http.client.HTTPSConnection', side_effect=factory):
                try:yield h, key, transport, gate, connections, workers, tls
                finally:
                    gate.release.set()
                    workers.join()
                    self.assertEqual(transport.active, set())
                    for connection in connections:
                        self.assertTrue(connection.closed)
                        self.assertTrue(all(s.closed for s in connection.sockets))

    def wait(self, event):self.assertTrue(event.wait(TEST_WAIT), 'control blocked behind native send')
    def rejected(self, done, outcome):
        self.wait(done);self.assertFalse(outcome[0][0]);self.assertIsInstance(outcome[0][1], ProviderFault)
        self.assertIn('CANCEL', outcome[0][1].code)

    def host(self, gateway):
        # Only the worker-free Host fields used by its ACTUAL _stop/_stop_worker.
        from app.actors import ActorHost
        host = object.__new__(ActorHost)
        host.real_gateway = gateway;host._worker_lock = threading.RLock()
        host._process = None;host._responses = queue.Queue();host._readers = []
        return host

    def test_actual_gateway_and_host_stop_return_before_request_release(self):
        for action in ('cancel', 'close', 'gateway', 'host'):
            with self.subTest(action=action), self.fixture() as f:
                h, key, http, gate, connections, workers, tls = f
                budget = RequestBudget(None, 30);deadline = budget.deadline
                done, outcome = workers.start(lambda:h.session.models(key, budget))
                self.wait(gate.entered)
                gateway = RealGateway(SimpleNamespace(siwc=h.session))
                host = self.host(gateway);original_queue = host._responses
                control = {'cancel':h.session.cancel, 'close':h.session.close,
                           'gateway':gateway.close, 'host':host._stop}[action]
                cancelled, result = workers.start(control)
                self.wait(cancelled)
                self.assertTrue(result[0][0], result)
                self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
                self.assertFalse(h.session.closed if action != 'close' else not h.session.closed)
                if action == 'host':self.assertIsNone(original_queue.get_nowait())
                self.assertEqual(budget.deadline, deadline);self.assertEqual(budget.requests, 0)
                gate.release.set();self.rejected(done, outcome)
                self.assertFalse(connections[0].sent)
                self.assertEqual(connections[0].connect_entries, 1)
                if action != 'close':
                    self.assertEqual(h.session.models(key, RequestBudget(None, 30)), [])
                else:
                    h.session.cancel()
                    with self.assertRaisesRegex(ProviderFault, 'UNCONFIGURED'):
                        h.session.models(key, RequestBudget(None, 30))

    def test_select_logout_reach_original_budgeted_store_before_send_release(self):
        class StoreReached(Exception):pass
        for action, seconds in (('select', 10), ('logout', 15)):
            with self.subTest(action=action), self.fixture() as f:
                h, key, http, gate, connections, workers, tls = f
                done, outcome = workers.start(lambda:h.session.models(key, RequestBudget(None, 30)))
                self.wait(gate.entered);original = h.session.store.locked
                store_entered = threading.Event();seen = [];control_id = []
                @contextmanager
                def store(budget):
                    if threading.get_ident() == control_id[0]:
                        seen.append(budget);store_entered.set();raise StoreReached()
                    with original(budget):yield
                def control():
                    control_id.append(threading.get_ident())
                    return h.session.select(key) if action == 'select' else h.session.logout()
                epoch = h.session.epoch
                # Stop only the real store prefix, NOT HTTP/cancel/lock methods.
                # A session access owns the store until its worker unwinds, so
                # successful select/logout need not complete before send release.
                with patch.object(h.session.store, 'locked', store):
                    cancelled, result = workers.start(control)
                    self.wait(store_entered);self.wait(cancelled)
                    self.assertIsInstance(result[0][1], StoreReached)
                    self.assertGreater(h.session.epoch, epoch)
                    self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
                    self.assertIsNone(seen[0].attempt);self.assertEqual(seen[0].requests, 0)
                    self.assertLessEqual(seen[0].remaining(), seconds)
                gate.release.set();self.rejected(done, outcome)
                self.assertFalse(connections[0].sent)

    def test_constructor_dns_tls_connect_late_results_closed_without_send(self):
        for stage in ('constructor', 'dns', 'tls', 'connect'):
            with self.subTest(stage=stage), self.fixture(stage) as f:
                h, key, http, gate, connections, workers, tls = f
                done, outcome = workers.start(lambda:h.session.models(key, RequestBudget(None, 30)))
                self.wait(gate.entered)
                cancelled, result = workers.start(h.session.cancel)
                self.wait(cancelled);self.assertTrue(result[0][0], result)
                self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
                gate.release.set();self.rejected(done, outcome)
                self.assertFalse(connections[0].sent)
                if stage != 'constructor':self.assertEqual(connections[0].request_entries, 0)
                self.assertTrue(all(s.closed for s in connections[0].sockets))

    def test_transport_close_revokes_registered_request_without_session_guard(self):
        with self.fixture() as f:
            h, key, http, gate, connections, workers, tls = f
            done, outcome = workers.start(lambda:http.request(API+'/models','GET',None,{},RequestBudget(None,30)))
            self.wait(gate.entered)
            cancelled, result = workers.start(http.close)
            self.wait(cancelled);self.assertTrue(result[0][0], result)
            self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
            gate.release.set();self.rejected(done, outcome)
            self.assertFalse(connections[0].sent)

    def test_admitted_before_cancel_bytes_are_not_claimed_unsent(self):
        with self.fixture('sent') as f:
            h, key, http, gate, connections, workers, tls = f
            done, outcome = workers.start(lambda:h.session.models(key, RequestBudget(None,30)))
            self.wait(gate.entered);self.assertEqual(connections[0].sent, [b'SYNTHETIC_WIRE'])
            cancelled, result = workers.start(h.session.cancel)
            self.wait(cancelled);self.assertTrue(result[0][0], result)
            self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
            gate.release.set();self.rejected(done, outcome)
            self.assertEqual(connections[0].sent, [b'SYNTHETIC_WIRE'])

    def test_real_gateway_preserves_original_attempt_budget_and_request_count(self):
        from pathlib import Path
        from app.real_settings import SettingsStore
        from app.tests.test_siwc979_consumers import services
        from app.actor_execution import Attempt, ExecutionPolicy
        with self.fixture() as f:
            h, key, http, gate, connections, workers, tls = f
            settings = SettingsStore(h.session.store.path.parent/'services.json', credential_factory=h.vault)
            settings.siwc = h.session
            descriptors = services()
            for service in descriptors.values():service['timeout_seconds'] = 30
            settings.save({'services':descriptors, 'secrets':{}})
            gateway = RealGateway(settings);attempt = Attempt(object(), ExecutionPolicy())
            budget = gateway._budget('dm', settings.snapshot()['dm'], attempt)
            deadline = budget.deadline
            done, outcome = workers.start(lambda:gateway.dm({}, attempt))
            self.wait(gate.entered)
            cancelled, result = workers.start(gateway.close)
            self.wait(cancelled);self.assertTrue(result[0][0], result)
            self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
            self.assertIs(gateway.budgets[(attempt,'dm')], budget)
            self.assertIs(budget.attempt, attempt);self.assertEqual(budget.deadline, deadline)
            self.assertEqual(budget.requests, 1)
            gate.release.set();self.rejected(done, outcome)
            self.assertEqual(budget.requests, 1);self.assertEqual(budget.deadline, deadline)
            self.assertFalse(connections[0].sent)

    def test_original_host_act_attempt_reaches_cleanup_pending_before_send_release(self):
        from app.actor_execution import ExecutionPolicy, ActorRunError
        class Authority:
            def _scoped_coordinator(self, value):return value
            def _scoped_entry_budget(self):return None
            def _scoped_attempt(self):return None
            def _scoped_coordinator_attempt(self):return None
            def _entry_budget(self, entry):return nullcontext()
            def _actor_attempt(self, attempt):return nullcontext()
            def acquire_actor_execution(self, *args, **kwargs):return object()
            def release_actor_execution(self, permit):raise AssertionError('provider still alive: no CAS')
        with self.fixture() as f:
            h, key, http, gate, connections, workers, tls = f
            gateway = RealGateway(SimpleNamespace(siwc=h.session))
            host = self.host(gateway);authority = Authority()
            host._authority = authority;host._owner_instance = 'synthetic-owner'
            host._lock = threading.RLock();host._local = threading.local()
            host._closed = False;host._closing = threading.Event();host._attempt = None
            host._pending_threads = [];host._lease_cleanups = []
            host._lease_cleanup_pending = False;host.cleanup_pending = False
            host.execution_policy = ExecutionPolicy()
            budgets = [];attempts = []
            def domain(*args, **kwargs):
                attempt = host._attempt;attempts.append(attempt)
                budget = RequestBudget(attempt, 30);budgets.append((budget, budget.deadline))
                return attempt.call(host, lambda:h.session.models(key, budget))
            # Only the domain body/authority are synthetic. act, Attempt.call,
            # _stop, Gateway.close, finish and original cleanup finally are real.
            host._act = domain
            done, outcome = workers.start(lambda:host.act('synthetic-person'))
            try:
                self.wait(gate.entered)
                attempt = attempts[0]
                cancelled, control = workers.start(lambda:attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED')))
                self.wait(cancelled);self.assertTrue(control[0][0], control)
                self.wait(done)  # Requires real _stop to get past responses.close.
                self.assertFalse(outcome[0][0]);self.assertEqual(outcome[0][1].code, 'ACTOR_EXECUTION_CANCELLED')
                self.assertFalse(gate.release.is_set())
                self.assertTrue(host.cleanup_pending);self.assertTrue(host._lease_cleanup_pending)
                self.assertEqual(host._lease_cleanups, [(authority, attempt.permit, attempt)])
                self.assertEqual(host._pending_threads, attempt.calls)
                self.assertTrue(any(t.is_alive() for t in attempt.calls))
                self.assertFalse(attempt.thread.is_alive())
                self.assertIs(budgets[0][0].attempt, attempt)
                self.assertEqual(budgets[0][0].deadline, budgets[0][1])
                self.assertEqual(budgets[0][0].requests, 0)
            finally:
                gate.release.set()
                if host._attempt is not None:
                    host._attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'))
                for attempt in attempts:
                    for thread in attempt.calls:thread.join(TEST_WAIT)
                    if attempt.thread is not None:attempt.thread.join(TEST_WAIT)
                    self.assertFalse(any(t.is_alive() for t in attempt.calls))
                    self.assertFalse(attempt.thread is not None and attempt.thread.is_alive())

    def test_refresh_send_cancellation_retains_durable_rotation_marker(self):
        with self.fixture() as f:
            h, key, http, gate, connections, workers, tls = f
            h.now += 3601
            old = h.session.store.secret(h.session.store.load()['accounts'][key])
            budget = RequestBudget(None, 30);deadline = budget.deadline
            done, outcome = workers.start(lambda:h.session.models(key, budget))
            self.wait(gate.entered)
            cancelled, result = workers.start(h.session.cancel)
            self.wait(cancelled);self.assertTrue(result[0][0], result)
            self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
            self.assertTrue(h.session.store.load()['accounts'][key]['rotation_pending'])
            gate.release.set();self.rejected(done, outcome)
            account = h.session.store.load()['accounts'][key]
            self.assertTrue(account['rotation_pending'])
            self.assertEqual(h.session.store.secret(account), old)
            before = len(connections)
            with self.assertRaisesRegex(ProviderFault, 'REAUTHORIZATION_REQUIRED'):
                h.session.models(key, RequestBudget(None, 30))
            self.assertEqual(len(connections), before)
            self.assertEqual(budget.deadline, deadline);self.assertEqual(budget.requests, 0)

    def test_begin_discovery_does_not_hold_pending_lock_against_close(self):
        from app.tests.siwc979_synthetic import CALLBACK
        with self.fixture() as f:
            h, key, http, gate, connections, workers, tls = f
            before = len(h.urls)
            done, outcome = workers.start(lambda:h.session.begin(CALLBACK))
            self.wait(gate.entered)
            cancelled, result = workers.start(h.session.close)
            self.wait(cancelled);self.assertTrue(result[0][0], result)
            self.assertFalse(gate.release.is_set());self.assertFalse(done.is_set())
            self.assertIsNone(h.session.pending)
            gate.release.set();self.rejected(done, outcome)
            self.assertEqual(len(h.urls), before);self.assertTrue(h.session.closed)

    def test_lexical_nested_guards_restore_after_error_and_do_not_leak_threads(self):
        http = PublicHTTP();seen = []
        with http.operation(lambda:seen.append('outer')):
            outer = http.local.check
            with self.assertRaisesRegex(RuntimeError, 'synthetic'):
                with http.operation(lambda:seen.append('inner')):
                    http._check(RequestBudget(None,30));raise RuntimeError('synthetic')
            self.assertIs(http.local.check, outer)
            workers = Workers(self)
            try:
                done, result = workers.start(lambda:http._check(RequestBudget(None,30)))
                self.wait(done);self.assertTrue(result[0][0], result)
            finally:workers.join()
            self.assertEqual(seen, ['outer','inner'])
        self.assertIsNone(http.local.check)


if __name__ == '__main__':unittest.main()
