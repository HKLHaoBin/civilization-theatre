"""Prospective regressions, NOT RUN in 992. Ordinary unittest entrypoint.
Actual Session/Gateway/PublicHTTP bodies; synthetic vault/TLS/read1 only.
No live OAuth/model/WinCred or signature evidence. No sleeps or worker threads.
"""
from contextlib import contextmanager
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from app.tests.siwc979_synthetic import Harness, frame, completed
from app.real_provider import ProviderFault, RequestBudget, RealGateway
from app.siwc_responses import API, PublicHTTP


class FakeResponse:
    status = 200
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.reads = 0
        self.closed = False
    def getheader(self, name, default=''):
        return 'text/event-stream' if name == 'Content-Type' else default
    def read1(self, n):
        self.reads += 1
        if not self.chunks:
            raise AssertionError('must not wait for keepalive EOF')
        value = self.chunks.pop(0)
        if isinstance(value, BaseException):
            raise value
        assert len(value) <= n
        return value
    def close(self):
        self.closed = True


class FakeConnection:
    sock = None
    def __init__(self, response):
        self.response = response
        self.sent = []
        self.closed = False
    def request(self, *args, **kwargs):
        self.sent.append((args, kwargs))
    def getresponse(self):
        return self.response
    def close(self):
        self.closed = True


class HTTPBoundaryTests(unittest.TestCase):
    def run_wire(self, chunks, budget=None, transport=None):
        response = FakeResponse(chunks)
        connection = FakeConnection(response)
        transport = transport or PublicHTTP()
        self.addCleanup(self.assertTrue, response.closed)
        self.addCleanup(self.assertTrue, connection.closed)
        self.addCleanup(self.assertEqual, transport.active, set())
        with patch('app.siwc_responses.ssl.SSLContext'), patch(
                'app.siwc_responses.http.client.HTTPSConnection', return_value=connection):
            result = transport.request(API+'/responses', 'POST', b'{}', {},
                                       budget or RequestBudget(None, 10), True)
        return result, response, connection

    def test_real_read1_every_terminal_trailer_split_and_bytewise(self):
        terminal = frame(completed(usage={'input_tokens':31, 'output_tokens':7}))
        for trailer in (b'data: [DONE]\n\n', b': keepalive comment\n\n',
                        b': \xe5\x90\x88\xe6\x88\x90\r\n\r\n', b'event: done\n\n'):
            wire = terminal+trailer
            for split in range(1, len(wire)):
                with self.subTest(trailer=trailer, split=split):
                    result, response, connection = self.run_wire([wire[:split], wire[split:]])
                    self.assertEqual(result['choices'][0]['message']['content'], '合成文本')
                    self.assertEqual(result['usage'], {'prompt_tokens':31, 'completion_tokens':7})
                    self.assertEqual(len(connection.sent), 1)
            result, _, _ = self.run_wire([bytes([x]) for x in wire])
            self.assertEqual(result['usage']['completion_tokens'], 7)

    def test_partial_trailer_already_received_requires_continuation_not_eof(self):
        terminal = frame(completed())
        for first, rest in ((b'data: [DO', b'NE]\n\n'), (b': keep', b'alive\n\n'),
                            (b': \xe5', b'\x90\x88\n\n')):
            result, response, _ = self.run_wire([terminal+first, rest])
            self.assertEqual(response.reads, 2)
            self.assertIsNone(result['usage']['prompt_tokens'])
        _, response, _ = self.run_wire([terminal])
        self.assertEqual(response.reads, 1)

    def test_received_trailer_failure_duplicate_tool_and_invalid_bytes_rejected(self):
        terminal = frame(completed())
        bad = (frame(completed()), frame({'type':'response.failed'}),
               frame({'type':'response.output_item.added','item':{'type':'function_call'}}),
               b'data: not-json\n\n', b'garbage\n', b': \xff\n')
        for suffix in bad:
            for split in range(1, len(suffix)):
                with self.subTest(suffix=suffix, split=split), self.assertRaises(ProviderFault):
                    self.run_wire([terminal+suffix[:split], suffix[split:], b''])
        for wire in (b'', frame({'type':'response.output_text.delta','delta':'x'}),
                     terminal[:-1], terminal+b'data: [DO', terminal+b': \xe5'):
            with self.subTest(wire=wire), self.assertRaises(ProviderFault):
                self.run_wire([wire, b''] if wire else [b''])

    def test_trailer_continuation_uses_original_deadline_and_attempt(self):
        class Attempt:
            deadline = float('inf')
            cancelled = False
            def check(self):
                if self.cancelled:raise ProviderFault('SYNTHETIC_ATTEMPT_CANCELLED')
        attempt = Attempt()
        budget = RequestBudget(attempt, 10)
        deadline = budget.deadline
        original = FakeResponse.read1
        def read(response, n):
            if response.reads == 1:attempt.cancelled = True
            return original(response, n)
        with patch.object(FakeResponse, 'read1', read):
            with self.assertRaisesRegex(ProviderFault, 'SYNTHETIC_ATTEMPT_CANCELLED'):
                self.run_wire([frame(completed())+b'data: [DO', b'NE]\n\n'], budget)
        self.assertIs(budget.attempt, attempt)
        self.assertEqual(budget.deadline, deadline)
        self.assertEqual(budget.requests, 0)


class CancellationTests(unittest.TestCase):
    def fresh(self, h, key):
        return h.session.models(key, RequestBudget(None, 10))

    def test_epoch_frozen_before_store_wait_and_fresh_cancel_recovery(self):
        with tempfile.TemporaryDirectory() as d:
            h = Harness(d); key = h.authorize()
            original = h.session.store.locked
            @contextmanager
            def waiting(budget):
                h.session.cancel()  # deterministic cancellation before lock acquired
                with original(budget):yield
            before = len(h.http.calls)
            with patch.object(h.session.store, 'locked', waiting):
                with self.assertRaisesRegex(ProviderFault, 'AUTH_CANCELLED'):
                    self.fresh(h, key)
            self.assertEqual(len(h.http.calls), before)
            self.assertTrue(self.fresh(h, key))

    def test_refresh_return_cancellation_retains_marker_never_reuses_old_token(self):
        from app.siwc_oauth import TOKEN
        with tempfile.TemporaryDirectory() as d:
            h = Harness(d); key = h.authorize(); h.now += 3601
            h.http.tokens.pop('id_token')
            old = copy.deepcopy(h.session.store.secret(h.session.store.load()['accounts'][key]))
            h.http.hook = lambda url: h.session.cancel() if url == TOKEN else None
            budget = RequestBudget(None, 10); deadline = budget.deadline
            with self.assertRaisesRegex(ProviderFault, 'AUTH_CANCELLED'):
                h.session.models(key, budget)
            h.http.hook = None
            account = h.session.store.load()['accounts'][key]
            self.assertTrue(account['rotation_pending'])
            self.assertEqual(h.session.store.secret(account), old)
            before = len(h.http.calls)
            with self.assertRaisesRegex(ProviderFault, 'REAUTHORIZATION_REQUIRED'):
                self.fresh(h, key)
            self.assertEqual(len(h.http.calls), before)
            self.assertEqual(budget.deadline, deadline)

    def test_refresh_save_window_cancel_select_logout_close_never_yields_old_operation(self):
        for action in ('cancel', 'select', 'logout', 'close'):
            for when in ('before_save', 'after_save'):
                with self.subTest(action=action, when=when), tempfile.TemporaryDirectory() as d:
                    h = Harness(d); key = h.authorize()
                    other = h.authorize('oaiapp_SECOND', 'SYNTHETIC_SECOND')
                    h.session.select(key); h.now += 3601
                    h.http.tokens.pop('id_token')
                    h.http.tokens.update(access_token='SYNTHETIC_NEW_ACCESS',refresh_token='SYNTHETIC_NEW_REFRESH')
                    original = h.session.store.replace_secret
                    # Invoke the real action through its invalidation prefix;
                    # deterministically suspend its store phase without threads.
                    class DeferredStore(Exception):pass
                    @contextmanager
                    def deferred_store(budget):
                        raise DeferredStore()
                        yield
                    def invalidate():
                        if action in ('cancel', 'close'):
                            getattr(h.session, action)()
                        else:
                            with patch.object(h.session.store, 'locked', deferred_store):
                                with self.assertRaises(DeferredStore):
                                    if action == 'select':h.session.select(other)
                                    else:h.session.logout()
                    def save(*args):
                        if when == 'before_save':invalidate()
                        original(*args)
                        if when == 'after_save':invalidate()
                    before = len([c for c in h.http.calls if c[0].endswith('/models')])
                    with patch.object(h.session.store, 'replace_secret', save):
                        with self.assertRaisesRegex(ProviderFault, 'AUTH_CANCELLED'):
                            self.fresh(h, key)
                    self.assertEqual(len([c for c in h.http.calls if c[0].endswith('/models')]), before)
                    account = h.session.store.load()['accounts'][key]
                    self.assertFalse(account['rotation_pending'])
                    self.assertEqual(h.session.store.secret(account)['refresh_token'], 'SYNTHETIC_NEW_REFRESH')
                    if action == 'select':
                        h.session.select(other)
                        with self.assertRaisesRegex(ProviderFault, 'UNCONFIGURED'):self.fresh(h, key)
                    elif action == 'logout':
                        h.session.logout()
                        self.assertFalse(h.session.store.load()['accounts'][key]['slots'])
                    elif action == 'close':
                        h.session.cancel()
                        with self.assertRaisesRegex(ProviderFault, 'UNCONFIGURED'):self.fresh(h, key)
                    else:self.assertTrue(self.fresh(h, key))

    def test_real_gateway_late_cancel_at_http_constructor_denies_send_not_early_attempt(self):
        from app.real_settings import SettingsStore
        from app.tests.test_siwc979_consumers import services
        for action in ('cancel', 'close'):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as d:
                h = Harness(d); h.authorize()
                transport = PublicHTTP(); h.session.http = transport
                settings = SettingsStore(Path(d)/'services.json', credential_factory=h.vault)
                settings.siwc = h.session
                settings.save({'services':services(),'secrets':{}})
                gateway = RealGateway(settings)
                budget = gateway._budget('dm', settings.snapshot()['dm'], None)
                deadline = budget.deadline
                response = FakeResponse([frame(completed())]); connection = FakeConnection(response)
                def construct(*args, **kwargs):
                    getattr(h.session, action)()
                    return connection
                # Session uses its transport lock at construction time, so share
                # it explicitly when replacing only the test transport seam.
                h.session.lifecycle_lock = transport.lock
                with patch.object(gateway, '_budget', return_value=budget), patch(
                        'app.siwc_responses.ssl.SSLContext'), patch(
                        'app.siwc_responses.http.client.HTTPSConnection', side_effect=construct):
                    with self.assertRaisesRegex(ProviderFault, 'AUTH_CANCELLED'):gateway.dm({}, None)
                self.assertFalse(connection.sent)
                self.assertTrue(connection.closed)
                self.assertEqual(transport.active, set())
                self.assertEqual(budget.requests, 1)
                self.assertEqual(budget.deadline, deadline)
                if action == 'cancel':
                    self.assertFalse(h.session.closed)
                    with h.session.access(h.session.binding(), RequestBudget(None, 10)) as token:
                        self.assertEqual(token, 'SYNTHETIC_ACCESS_NEVER_REAL')
                else:self.assertTrue(h.session.closed)

    def test_actual_select_logout_invalidation_precedes_store_lock(self):
        for action in ('select','logout'):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as d:
                h = Harness(d); key = h.authorize()
                with self.assertRaisesRegex(ProviderFault, 'AUTH_CANCELLED'):
                    with h.session.access(key, RequestBudget(None, 10)):
                        getattr(h.session, action)(key) if action == 'select' else h.session.logout()


if __name__ == '__main__':
    unittest.main()
