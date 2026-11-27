"""Issue #7 pure contract checks; no DB, network, browser, model, or vault.

All provider output and credential values here are synthetic sentinels. These
checks exercise the routing, redaction, and bounded journal seams only; they do
not establish real-service or billing acceptance.
"""
import json
from pathlib import Path
import tempfile
import threading
from typing import Any, cast
import unittest

from app.actors import ActorHost
from app.credential_adapter import InMemoryCredentialAdapter
from app.provider_adapter import (PiProviderAdapter, ProviderRouter,
                                   RealProviderAdapter)
from app.real_provider import ProviderFault
from app.real_settings import SettingsStore
from app.redaction import exception_code, redact, safe_code


SYNTHETIC_SECRET = 'SYNTHETIC_SECRET_SENTINEL'
REF = 'civilization-theatre/service/' + 'a' * 32


def service_config():
    return {
        'roles': dict(provider='openai-chat-https',
                      base_url='https://service.example.invalid/v1',
                      model='synthetic-role-model', credential_ref=None,
                      timeout_seconds=2, max_retries=1),
        'dm': dict(provider='openai-chat-https',
                   base_url='https://service.example.invalid/v1',
                   model='synthetic-dm-model', credential_ref=None,
                   timeout_seconds=3, max_retries=2),
    }


class _Gateway:
    def __init__(self):
        self.role_calls = []
        self.dm_calls = []
        self.closed = 0

    def role(self, request, attempt):
        self.role_calls.append((request, attempt))
        return {'route': 'roles'}

    def dm(self, payload, attempt, expected_version):
        self.dm_calls.append((payload, attempt, expected_version))
        return {'route': 'dm'}

    def close(self):
        self.closed += 1


class ProviderRoutingTests(unittest.TestCase):
    def test_real_routes_are_separate_and_never_fall_back_to_pi(self):
        gateway = _Gateway()
        adapter = RealProviderAdapter(gateway)
        router = ProviderRouter('real', roles=adapter, dm=adapter)
        self.assertEqual(router.snapshot(),
                         {'mode': 'real', 'roles': 'real-gateway', 'dm': 'real-gateway'})
        self.assertEqual(router.role({'private': 'role'}, None), {'route': 'roles'})
        self.assertEqual(router.dm({'private': 'dm'}, None, 'v'), {'route': 'dm'})
        self.assertEqual(len(gateway.role_calls), 1)
        self.assertEqual(len(gateway.dm_calls), 1)
        router.close()
        self.assertEqual(gateway.closed, 1)

        missing_dm = ProviderRouter('real', roles=adapter)
        with self.assertRaisesRegex(ProviderFault, 'REAL_UNCONFIGURED'):
            missing_dm.dm({}, None, None)
        with self.assertRaisesRegex(ValueError, 'REAL_PI_FALLBACK_FORBIDDEN'):
            ProviderRouter('real', roles=adapter, dm=adapter,
                           pi=object())

    def test_scripted_pi_is_a_distinct_explicit_mode(self):
        calls = []
        pi = PiProviderAdapter(
            lambda request, attempt: calls.append((request, attempt)) or {'route': 'pi'})
        router = ProviderRouter('scripted-pi', pi=pi)
        self.assertEqual(router.role({'fake': True}, 'attempt'), {'route': 'pi'})
        self.assertEqual(router.snapshot()['mode'], 'scripted-pi')
        self.assertEqual(len(calls), 1)
        with self.assertRaisesRegex(ProviderFault, 'PI_DM_UNAVAILABLE'):
            router.dm({}, None, None)

    def test_unconfigured_real_router_fails_before_any_adapter_call(self):
        router = ProviderRouter('real')
        with self.assertRaisesRegex(ProviderFault, 'REAL_UNCONFIGURED'):
            router.role({}, None)
        with self.assertRaisesRegex(ProviderFault, 'REAL_UNCONFIGURED'):
            router.dm({}, None, None)

    def test_bound_real_router_blocks_legacy_pi_start_fallback(self):
        host = cast(Any, object.__new__(ActorHost))
        host.real_gateway = object()
        host.provider_router = None
        host._local = threading.local()
        host._start = lambda: self.fail('real route must not start Pi')
        host.bind_provider_router(ProviderRouter('real'))
        with self.assertRaisesRegex(ProviderFault, 'REAL_UNCONFIGURED'):
            host._request({'synthetic': True})


class CredentialAndSettingsTests(unittest.TestCase):
    def test_synthetic_credential_is_opaque_and_role_dm_refs_are_independent(self):
        vault = InMemoryCredentialAdapter()
        vault.write(REF, SYNTHETIC_SECRET.encode('utf-8'))
        secret = vault.read(REF)
        self.assertIsNotNone(secret)
        assert secret is not None
        self.assertNotIn(SYNTHETIC_SECRET, repr(secret))
        self.assertEqual(secret.reveal_bytes(), SYNTHETIC_SECRET.encode('utf-8'))
        secret.close()
        with self.assertRaisesRegex(ValueError, 'SECRET_CLOSED'):
            secret.reveal_bytes()

        with tempfile.TemporaryDirectory() as directory:
            store = SettingsStore(Path(directory) / 'services.json',
                                  credential_factory=lambda: vault)
            saved = store.save({'services': service_config(),
                                'secrets': {'roles': SYNTHETIC_SECRET,
                                            'dm': 'SYNTHETIC_DM_KEY'}})
            config = store.snapshot()
            self.assertIsNotNone(config)
            assert config is not None
            self.assertNotEqual(config['roles']['credential_ref'],
                                config['dm']['credential_ref'])
            self.assertIn('"credential_reference_present": true',
                          json.dumps(saved, ensure_ascii=False))
            self.assertNotIn(SYNTHETIC_SECRET,
                             (Path(directory) / 'services.json').read_text())
            self.assertNotIn('SYNTHETIC_DM_KEY',
                             json.dumps(saved, ensure_ascii=False))

    def test_usage_journal_is_idempotent_and_conflicts_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            store = SettingsStore(Path(directory) / 'services.json',
                                  credential_factory=lambda: InMemoryCredentialAdapter())
            event = dict(event='request_started', kind='roles',
                         request_id='b' * 32, request_index=1,
                         input_tokens=None, output_tokens=None, cost=None,
                         billing_status='UNKNOWN')
            first = store.usage(event)
            duplicate = store.usage(dict(event))
            self.assertTrue(first['written'])
            self.assertTrue(duplicate['duplicate'])
            self.assertEqual(len(store.usage_path.read_text().splitlines()), 1)
            conflicting = dict(event, input_tokens=7)
            with self.assertRaisesRegex(ValueError, 'USAGE_EVENT_CONFLICT'):
                store.usage(conflicting)
            self.assertNotIn(SYNTHETIC_SECRET, store.usage_path.read_text())


class RedactionAndPauseContractTests(unittest.TestCase):
    def test_secret_values_and_untrusted_error_codes_are_redacted(self):
        payload = {'secret': SYNTHETIC_SECRET,
                   'nested': [SYNTHETIC_SECRET, {'token': SYNTHETIC_SECRET}],
                   'public': 'SYNTHETIC_PUBLIC'}
        safe = redact(payload, [SYNTHETIC_SECRET])
        serialized = json.dumps(safe, ensure_ascii=False)
        self.assertNotIn(SYNTHETIC_SECRET, serialized)
        self.assertIn('SYNTHETIC_PUBLIC', serialized)
        self.assertEqual(safe_code(SYNTHETIC_SECRET), 'PROVIDER_ERROR')
        class UntrustedFault:
            code = SYNTHETIC_SECRET
        self.assertEqual(exception_code(UntrustedFault()), 'PROVIDER_ERROR')

    def test_host_has_fail_closed_pause_boundary(self):
        source = (Path(__file__).resolve().parents[1] / 'actors.py').read_text()
        self.assertIn('pause_on_failure', source)
        self.assertIn('technical_pause(code', source)
        self.assertIn("ACTOR_PROVIDER_MODE_MISMATCH", source)


if __name__ == '__main__':
    unittest.main()
