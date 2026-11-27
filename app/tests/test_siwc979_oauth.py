"""SOURCE ONLY, NOT RUN. All OAuth/HTTP/credential values are synthetic.
Fake crypto tests are seam tests, NOT signature evidence. Native-crypto cases
live in test_siwc979_crypto and also remain unexecuted.
"""
from contextlib import nullcontext
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs,urlencode
from app.tests.siwc979_synthetic import Harness,CALLBACK,NOW,SyntheticVerifier,synthetic_file_lock

class OAuthTests(unittest.TestCase):
    def test_dynamic_registration_pkce_hostid_and_issued_client_exchange(self):
        import base64,hashlib
        from app.siwc_oauth import TOKEN
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);p=h.begin()
            self.assertEqual(p['client_id'],'dynamic_agent_client');self.assertEqual(p['agent_name_hint'],'Civilization Theatre')
            verifier=h.session.pending['pkce']
            self.assertEqual(p['code_challenge'],base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b'=').decode())
            h.tokens(p);h.callback(p)
            fields=parse_qs([c[2] for c in h.http.calls if c[0]==TOKEN][0].decode())
            self.assertEqual(fields['client_id'],['oaiapp_SYNTHETIC']);self.assertEqual(fields['redirect_uri'],[CALLBACK])
            self.assertEqual(fields['code_verifier'],[verifier]);key=h.session.binding()
            h.session.welcome();p2=h.begin(key)
            self.assertEqual(p2['ext_agent_host_id'],p['ext_agent_host_id']);self.assertEqual(p2['client_id'],'oaiapp_SYNTHETIC')
            self.assertNotIn('agent_name_hint',p2);self.assertIn('id_token_hint',p2)
            self.assertNotEqual(p2['state'],p['state']);self.assertNotEqual(p2['nonce'],p['nonce'])
            self.assertIsNone(h.session.status()['real_call_verified'] or None)
            self.assertTrue(all(s.closed for s in h.vault.reads))
            disk=h.session.store.path.read_text();self.assertNotIn('SYNTHETIC_ACCESS',disk);self.assertNotIn('SYNTHETIC_REFRESH',disk)
            self.assertNotIn(verifier,disk);self.assertNotIn(p['state'],disk)

    def test_exchange_invalid_grant_retains_issued_client_for_fresh_authorization(self):
        from app.siwc_oauth import TOKEN
        from app.siwc_responses import upstream_failure
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);p=h.begin();h.tokens(p)
            def hook(url):
                if url==TOKEN:raise upstream_failure({'error':'invalid_grant'},400)
            h.http.hook=hook
            with self.assertRaisesRegex(Exception,'INVALID_GRANT'):h.callback(p)
            self.assertIsNone(h.session.binding())
            h.http.hook=None;p2=h.begin()
            self.assertEqual(p2['client_id'],'oaiapp_SYNTHETIC');self.assertNotIn('agent_name_hint',p2)
            self.assertNotEqual(p2['state'],p['state']);h.tokens(p2);h.callback(p2)
            self.assertIsNotNone(h.session.binding())

    def test_state_duplicate_client_conflict_denial_expiry_and_replay(self):
        for mode in ('state','duplicate','client-missing','bootstrap','denied','expired','wrong-uri'):
            with tempfile.TemporaryDirectory() as d:
                h=Harness(d);p=h.begin();h.tokens(p)
                values=dict(state=p['state'],code='SYNTHETIC_CODE',client_id='oaiapp_SYNTHETIC')
                uri=CALLBACK
                if mode=='state':values['state']='SYNTHETIC_WRONG_STATE'
                if mode=='client-missing':del values['client_id']
                if mode=='bootstrap':values['client_id']='dynamic_agent_client'
                if mode=='denied':values={'state':p['state'],'error':'access_denied'}
                if mode=='expired':h.now+=301
                if mode=='wrong-uri':uri=CALLBACK.replace('32123','32124')
                query=urlencode(values)
                if mode=='duplicate':query+='&state=duplicate'
                before=len(h.http.calls)
                with self.subTest(mode=mode),self.assertRaises(Exception):h.session.callback(query,uri)
                self.assertEqual(len(h.http.calls),before);self.assertIsNone(h.session.binding())
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);p=h.begin();h.tokens(p);h.callback(p)
            with self.assertRaisesRegex(Exception,'STATE_INVALID'):h.callback(p)

    def test_nonce_issuer_audience_expired_scope_and_subject_bound(self):
        for field,value in [('nonce','WRONG_SYNTHETIC'),('iss','https://evil.invalid'),('aud','oaiapp_OTHER'),('exp',NOW-100),('iat',NOW+100),('sub','')]:
            with tempfile.TemporaryDirectory() as d:
                h=Harness(d);p=h.begin();h.tokens(p);claims=json.loads(h.http.tokens['id_token']);claims[field]=value;h.http.tokens['id_token']=json.dumps(claims)
                with self.subTest(field=field),self.assertRaises(Exception):h.callback(p)
                self.assertIsNone(h.session.binding());self.assertFalse(h.vault.writes)
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);p=h.begin();h.tokens(p,scope='openid profile email')
            with self.assertRaisesRegex(Exception,'SCOPE_REQUIRED'):h.callback(p)
            self.assertIsNone(h.session.binding())

    def test_returning_identity_swap_or_client_replacement_never_overwrites(self):
        for mode in ('identity','client'):
            with tempfile.TemporaryDirectory() as d:
                h=Harness(d);key=h.authorize();before=h.session.store.path.read_bytes();vault=copy.deepcopy(h.vault.data)
                p=h.begin(key);h.tokens(p,subject='SYNTHETIC_OTHER' if mode=='identity' else 'SYNTHETIC_SUBJECT')
                with self.assertRaises(Exception):h.callback(p,client='oaiapp_OTHER' if mode=='client' else 'oaiapp_SYNTHETIC')
                self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)

    def test_duplicate_registration_client_and_wrong_selected_account_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);h.authorize();p=h.begin();h.tokens(p)
            with self.assertRaisesRegex(Exception,'CLIENT_CONFLICT'):h.callback(p)
            self.assertEqual(len(h.session.store.load()['accounts']),1)
            h.session.pending=None
            with self.assertRaisesRegex(Exception,'ACCOUNT_INVALID'):h.begin('civilization-theatre/service/'+'f'*32)

    def test_separate_accounts_restart_select_and_logout_keep_mapping(self):
        from app.siwc_oauth import SIWCSession
        from app.real_provider import RequestBudget
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);first=h.authorize();second=h.authorize('oaiapp_SECOND','SYNTHETIC_SUBJECT')
            self.assertNotEqual(first,second);m=h.session.store.load()
            self.assertTrue(set(m['accounts'][first]['slots']).isdisjoint(m['accounts'][second]['slots']))
            restarted=SIWCSession(h.session.store.path,h.vault,http=h.http,verifier=SyntheticVerifier(),clock=lambda:h.now,opener=h.urls.append,file_lock=synthetic_file_lock)
            self.assertEqual(restarted.binding(),second);restarted.select(first)
            with restarted.access(first,RequestBudget(None,1)) as token:self.assertEqual(token,'SYNTHETIC_ACCESS_NEVER_REAL')
            out=restarted.logout();self.assertTrue(out['remote_revocation_confirmed']);m=restarted.store.load()
            self.assertEqual(m['accounts'][first]['slots'],[]);self.assertTrue(m['accounts'][second]['slots'])
            self.assertEqual(m['accounts'][first]['client_id'],'oaiapp_SYNTHETIC')
            with self.assertRaises(Exception):
                with restarted.access(second,RequestBudget(None,1)):self.fail('other account must not be silently active')

    def test_refresh_rotates_once_omits_scope_and_preserves_identity(self):
        from app.real_provider import RequestBudget
        from app.siwc_oauth import TOKEN
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();h.now+=3601
            h.http.tokens.update(access_token='SYNTHETIC_REPLACEMENT_ACCESS',refresh_token='SYNTHETIC_REPLACEMENT_REFRESH')
            h.http.tokens.pop('id_token');h.http.tokens.pop('scope')
            before=len([c for c in h.http.calls if c[0]==TOKEN])
            for _ in range(2):
                with h.session.access(key,RequestBudget(None,1)) as token:self.assertEqual(token,'SYNTHETIC_REPLACEMENT_ACCESS')
            calls=[c for c in h.http.calls if c[0]==TOKEN]
            self.assertEqual(len(calls),before+1);form=parse_qs(calls[-1][2].decode())
            self.assertNotIn('scope',form);self.assertEqual(form['client_id'],['oaiapp_SYNTHETIC'])
            self.assertFalse(h.session.store.load()['accounts'][key]['rotation_pending'])
            self.assertTrue(all(s.closed for s in h.vault.reads))

    def test_interrupted_refresh_durable_marker_prevents_reuse_after_restart(self):
        from app.real_provider import RequestBudget
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();before=h.session.store.secret(h.session.store.load()['accounts'][key]);h.now+=3601
            h.vault.fail_at=len(h.vault.writes)+1;h.http.tokens.pop('id_token')
            with self.assertRaises(Exception):
                with h.session.access(key,RequestBudget(None,1)):self.fail('refresh save failed')
            a=h.session.store.load()['accounts'][key];self.assertTrue(a['rotation_pending'])
            self.assertEqual(h.session.store.secret(a),before)
            count=len(h.http.calls)
            with self.assertRaisesRegex(Exception,'REAUTHORIZATION_REQUIRED'):
                with h.session.access(key,RequestBudget(None,1)):self.fail('must not reuse rotating token')
            self.assertEqual(len(h.http.calls),count)

    def test_chunk_slot_failure_or_atomic_pointer_failure_preserves_old(self):
        for mode in ('slot','replace'):
            with tempfile.TemporaryDirectory() as d:
                h=Harness(d);key=h.authorize();old=h.session.store.path.read_bytes();oldvault=copy.deepcopy(h.vault.data)
                p=h.begin(key);h.tokens(p,access_token='SYNTHETIC_'+('A'*8000),refresh_token='SYNTHETIC_'+('R'*8000))
                if mode=='slot':h.vault.fail_at=len(h.vault.writes)+2
                with patch('app.siwc_storage.os.replace',side_effect=OSError('SYNTHETIC_ATOMIC_FAILURE')) if mode=='replace' else nullcontext():
                    with self.assertRaises(Exception):h.callback(p)
                self.assertEqual(h.session.store.path.read_bytes(),old);self.assertEqual(h.vault.data,oldvault)
                self.assertTrue(all(s.closed for s in h.vault.reads))

    def test_large_tokens_chunked_under_native_ceiling_and_corrupt_slot_rejected(self):
        from app.real_provider import RequestBudget
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);p=h.begin();h.tokens(p,access_token='SYNTHETIC_'+('A'*10000),refresh_token='SYNTHETIC_'+('R'*10000));h.callback(p)
            self.assertGreater(len(h.vault.data),2);self.assertTrue(all(0<len(v)<=2400 for v in h.vault.data.values()))
            a=h.session.store.load()['accounts'][h.session.binding()];h.vault.data[a['slots'][0]]=b'SYNTHETIC_CORRUPTION'
            with self.assertRaisesRegex(Exception,'CREDENTIAL_INVALID'):
                with h.session.access(h.session.binding(),RequestBudget(None,1)):self.fail('corrupt credential')
            self.assertTrue(all(s.closed for s in h.vault.reads))

    def test_status_missing_crypto_and_browser_failure_have_no_native_or_token_calls(self):
        from app.siwc_oauth import IdentityVerifier
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d)
            with patch.object(h.session.store,'factory',side_effect=AssertionError('native construction')):
                self.assertEqual(h.session.status()['state'],'unconfigured')
            h.session.verifier=IdentityVerifier()
            with patch.dict('sys.modules',{'jwt':None}):
                with self.assertRaisesRegex(Exception,'CRYPTO_UNCONFIGURED'):h.begin()
            self.assertFalse(h.urls);self.assertFalse(h.http.calls)
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);h.session.opener=lambda _:(_ for _ in ()).throw(RuntimeError('SYNTHETIC_URL_MUST_NOT_LEAK'))
            with self.assertRaises(Exception) as caught:h.begin()
            self.assertNotIn('SYNTHETIC_URL',str(caught.exception));self.assertIsNone(h.session.pending)

    def test_logout_during_callback_and_generation_change_cancel_commit(self):
        from app.siwc_oauth import TOKEN
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);p=h.begin();h.tokens(p)
            def hook(url):
                if url==TOKEN:h.session.logout()
            h.http.hook=hook
            with self.assertRaisesRegex(Exception,'AUTH_CANCELLED'):h.callback(p)
            self.assertIsNone(h.session.binding());self.assertFalse(h.vault.data)

    def test_attempt_cancel_is_not_permanent_logout_or_host_close(self):
        from app.real_provider import RequestBudget
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize()
            h.session.cancel()
            with h.session.access(key,RequestBudget(None,1)) as token:self.assertEqual(token,'SYNTHETIC_ACCESS_NEVER_REAL')
            self.assertFalse(h.session.closed)
            h.session.close()
            with self.assertRaisesRegex(Exception,'UNCONFIGURED'):
                with h.session.access(key,RequestBudget(None,1)):self.fail('closed host')
            with self.assertRaisesRegex(Exception,'CLOSED'):h.begin(key)

    def test_models_only_list_visible_order_and_no_cached_fake_catalog(self):
        from app.real_provider import RequestBudget
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();h.http.models_value={'models':[
                {'slug':'b','display_name':'B','visibility':'list'},{'slug':'hidden','display_name':'Hidden','visibility':'hidden'},
                {'slug':'a','display_name':'A','visibility':'list'}]}
            self.assertEqual(h.session.models(key,RequestBudget(None,1)),[{'slug':'b','display_name':'B'},{'slug':'a','display_name':'A'}])
            h.http.models_value={'data':[{'id':'not the SIWC catalog'}]}
            with self.assertRaisesRegex(Exception,'MODELS_INVALID'):h.session.models(key,RequestBudget(None,1))

    def test_concurrent_refresh_serialized_and_logout_invalidates_active_lease(self):
        # Future SYNTHETIC threads only. No real HTTP/crypto/WinCred.
        from concurrent.futures import ThreadPoolExecutor
        from app.real_provider import RequestBudget
        from app.siwc_oauth import TOKEN
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();h.now+=3601;h.http.tokens.pop('id_token')
            def read():
                with h.session.access(key,RequestBudget(None,5)) as token:return token
            before=len([c for c in h.http.calls if c[0]==TOKEN])
            with ThreadPoolExecutor(max_workers=2) as pool:self.assertEqual(list(pool.map(lambda _:read(),range(2))),['SYNTHETIC_ACCESS_NEVER_REAL']*2)
            self.assertEqual(len([c for c in h.http.calls if c[0]==TOKEN]),before+1)
            with self.assertRaisesRegex(Exception,'AUTH_CANCELLED'):
                with h.session.access(key,RequestBudget(None,5)):h.session.logout()
            self.assertGreater(h.http.closed,0)
