"""Prospective SOURCE ONLY. Actual Gateway + actual Handler methods with
synthetic OAuth/HTTP/vault and in-memory request/response streams. NOT RUN.
No real listener/browser/model/native credential access in these methods.
"""
from contextlib import redirect_stdout,redirect_stderr
from email.message import Message
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch
from app.tests.siwc979_synthetic import Harness,completed,frame
from app.tests.test_siwc979_responses import Budget


def services():
    return {kind:dict(provider='openai-siwc-responses',base_url='https://api.openai.com/v1',
        model='SYNTHETIC_'+kind,credential_ref=None,timeout_seconds=2,max_retries=0) for kind in ('roles','dm')}

class ConsumerTests(unittest.TestCase):
    def test_actual_role_candidate_and_independent_dm_use_responses_parser(self):
        from app.real_settings import SettingsStore
        from app.real_provider import RealGateway
        from app.real_intent import make_request,body_matches
        from app.siwc_responses import PublicHTTP,ResponsesDecoder
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);h.authorize();settings=SettingsStore(Path(d)/'services.json',credential_factory=h.vault);settings.siwc=h.session
            settings.save({'services':services(),'secrets':{}});gateway=RealGateway(settings)
            actual=PublicHTTP();calls=[]
            dm=dict(ruling='成功',reason='合成裁定',goal='合成等待',costs=[],benefits=[],effects=[{'action':'wait'}])
            def wire(url,method,data,headers,budget,stream=False):
                body=json.loads(data);calls.append(body)
                text='自主合成意图，不是脚本预备body' if body['model']=='SYNTHETIC_roles' else json.dumps(dm,ensure_ascii=False)
                decoder=ResponsesDecoder(budget);wire=frame(completed(text,{'input_tokens':9,'output_tokens':5}))
                for start in range(0,len(wire),3):decoder.feed(wire[start:start+3])
                return decoder.finish()
            actual.request=wire;gateway.responses=actual
            q=make_request('SYNTHETIC_PERSON','SYNTHETIC_INTENT',[{'role':'assistant','content':'SYNTHETIC_OWN_HISTORY'}],
                           'SYNTHETIC_ROLE_PRIVATE_INSTRUCTIONS','SYNTHETIC_OWN_OBSERVATION',gateway.version())
            r=gateway.role(q,None);self.assertEqual(r['body'],{'action':'propose','text':'自主合成意图，不是脚本预备body'})
            self.assertIsNone(q['body']);self.assertTrue(body_matches(q,r['body'],json.dumps(r['messages'])))
            ruling=gateway.dm({'proposal':{'text':r['body']['text']},'world':{'world_id':'SYNTHETIC_WORLD','revision':2}},None,gateway.version())
            self.assertEqual(ruling['effects'],[{'action':'wait'}]);self.assertEqual(ruling['base_revision'],2)
            self.assertEqual([c['model'] for c in calls],['SYNTHETIC_roles','SYNTHETIC_dm'])
            self.assertNotIn('SYNTHETIC_ROLE_PRIVATE',json.dumps(calls[1]));self.assertNotIn('SYNTHETIC_OWN_HISTORY',json.dumps(calls[1]))
            usage=r['messages'][-1]['intent_binding']['usage'];self.assertEqual(usage['input_tokens'],9);self.assertIsNone(usage['cost'])
            self.assertNotIn('SYNTHETIC_ACCESS',settings.usage_path.read_text());self.assertEqual(r['sdk_requests'],0)

    def test_no_configuration_no_crypto_no_vault_no_paid_or_scripted_fallback(self):
        from app.real_settings import SettingsStore
        from app.real_provider import RealGateway
        with tempfile.TemporaryDirectory() as d:
            def forbidden():raise AssertionError('SYNTHETIC_FORBIDDEN_VAULT')
            settings=SettingsStore(Path(d)/'services.json',credential_factory=forbidden)
            settings.save({'services':services(),'secrets':{}});gateway=RealGateway(settings)
            gateway.transport=lambda *a:self.fail('ordinary paid provider fallback')
            with patch.dict('sys.modules',{'jwt':None}):
                with self.assertRaisesRegex(Exception,'CRYPTO_UNCONFIGURED'):gateway.dm({},None)
            self.assertFalse(settings.usage_path.exists());self.assertEqual(settings.status()['siwc']['state'],'unconfigured')

    def test_existing_attempt_budget_cancel_and_version_bind_actual_siwc_consumer(self):
        from app.real_settings import SettingsStore
        from app.real_provider import RealGateway
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);h.authorize();settings=SettingsStore(Path(d)/'services.json',credential_factory=h.vault);settings.siwc=h.session
            settings.save({'services':services(),'secrets':{}});g=RealGateway(settings)
            calls=[]
            g.responses=SimpleNamespace(responses=lambda *args:calls.append(args))
            class Cancelled:
                deadline=float('inf')
                calls=[]
                def check(self):raise RuntimeError('SYNTHETIC_ORIGINAL_ATTEMPT_CANCEL')
            with self.assertRaisesRegex(Exception,'ORIGINAL_ATTEMPT_CANCEL'):g.dm({},Cancelled())
            with self.assertRaisesRegex(Exception,'CONFIGURATION_CHANGED'):g.dm({},None,'0'*64)
            self.assertFalse(calls)

    def test_original_protected_source_and_25_tests_are_not_rewritten(self):
        # This source-only assertion consumes the preparation-generated pin map.
        # It does NOT claim any inherited method was executed.
        import hashlib
        root=Path(__file__).resolve().parents[2]
        m=json.loads((root/'generation-manifest.json').read_text())
        self.assertEqual(len(m['preserved_real967_selectors']),25)
        for name,pin in m['preserved_contract_files'].items():
            raw=(root/name).read_bytes();self.assertEqual(len(raw),pin['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),pin['sha256'])

class SyntheticServer:
    def __init__(self,address,handler):self.server_address=(address[0],32123);self.handler=handler

class RouteTests(unittest.TestCase):
    def setup_handler(self,session):
        from app.server import make_server
        world=object();actors=SimpleNamespace(real_settings=SimpleNamespace(siwc=session,status=lambda:{'siwc':session.status()}))
        coordinator=SimpleNamespace(world=world,actors=actors,busy=False)
        with patch('app.server.TheatreServer',SyntheticServer):server=make_server(world,actors,0,coordinator=coordinator)
        handler=object.__new__(server.handler);handler.server=server;handler.out=[]
        handler.send_payload=lambda code,payload,mime:handler.out.append((code,payload,mime))
        return handler
    def request(self,handler,method,path,body=None,*,origin=True,csrf=None,host='127.0.0.1:32123'):
        handler.path=path;handler.headers=Message();handler.headers['Host']=host
        if origin:handler.headers['Origin']='http://127.0.0.1:32123'
        if csrf is not None:handler.headers['X-Theatre-CSRF']=csrf
        raw=json.dumps(body or {}).encode();handler.headers['Content-Type']='application/json';handler.headers['Content-Length']=str(len(raw))
        handler.rfile=io.BytesIO(raw);handler.wfile=io.BytesIO();getattr(handler,'do_'+method)()
        return handler.out[-1]
    def csrf(self,h):return json.loads(self.request(h,'GET','/api/meta')[1])['csrf']

    def test_settings_begin_models_select_logout_require_real_csrf_and_single_host(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);handler=self.setup_handler(h.session);csrf=self.csrf(handler)
            for action in ('begin','models','select','logout','welcome'):
                body={'account':None} if action in ('begin','select') else {}
                self.assertEqual(self.request(handler,'POST','/api/siwc/'+action,body,csrf='WRONG')[0],403)
                self.assertEqual(self.request(handler,'POST','/api/siwc/'+action,body,csrf=csrf,origin=False)[0],403)
            self.assertFalse(h.urls);self.assertFalse(h.vault.reads)
            self.assertEqual(self.request(handler,'POST','/api/siwc/begin',{'account':None},csrf=csrf)[0],200)
            self.assertEqual(len(h.urls),1)
            self.assertEqual(self.request(handler,'POST','/api/siwc/models',{'token':'SYNTHETIC_FORBIDDEN'},csrf=csrf)[0],400)

    def test_native_callback_without_origin_uses_actual_manager_once_and_never_logs(self):
        from urllib.parse import urlencode
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);handler=self.setup_handler(h.session);csrf=self.csrf(handler)
            self.request(handler,'POST','/api/siwc/begin',{'account':None},csrf=csrf)
            p={k:v[0] for k,v in __import__('urllib.parse',fromlist=['parse_qs']).parse_qs(__import__('urllib.parse',fromlist=['urlsplit']).urlsplit(h.urls[-1]).query).items()}
            h.tokens(p);path='/auth/callback?'+urlencode(dict(code='SYNTHETIC_CODE_NEVER_LOG',state=p['state'],client_id='oaiapp_SYNTHETIC'))
            output=io.StringIO()
            with redirect_stdout(output),redirect_stderr(output):
                self.assertEqual(self.request(handler,'GET',path,origin=False,host='localhost:32123')[0],403)
                self.assertIsNotNone(h.session.pending)
                self.assertEqual(self.request(handler,'GET',path,origin=False)[0],200)
                handler.log_request(200);handler.log_message('SYNTHETIC_CODE_NEVER_LOG')
                self.assertEqual(self.request(handler,'GET',path,origin=False)[0],400)
            self.assertEqual(output.getvalue(),'');self.assertIsNotNone(h.session.binding())
            self.assertNotIn(p['state'],str(handler.out));self.assertNotIn('SYNTHETIC_CODE_NEVER_LOG',str(handler.out))
            self.assertEqual(self.request(handler,'POST','/api/siwc/models',{},csrf=csrf)[0],200)
            self.assertEqual(json.loads(handler.out[-1][1])['models'][0]['slug'],'synthetic-role')

    def test_status_is_nonsecret_and_busy_routes_do_not_open_browser(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);handler=self.setup_handler(h.session);csrf=self.csrf(handler)
            with patch.object(h.session.store,'factory',side_effect=AssertionError('no native status')):
                status=json.loads(self.request(handler,'GET','/api/settings')[1]);self.assertFalse(status['siwc']['connection_verified'])
            handler.server.coordinator.busy=True
            self.assertEqual(self.request(handler,'POST','/api/siwc/begin',{'account':None},csrf=csrf)[0],409)
            self.assertFalse(h.urls);self.assertFalse(h.vault.reads)
