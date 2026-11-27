"""Future synthetic component/owned-world tests. NOT executed in source-only 967.
No test here constitutes a real-service, native-credential or billing acceptance.
"""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


def config():
    return {kind:dict(provider='openai-chat-loopback',base_url='http://127.0.0.1:18181/v1',
                     model=kind+'-synthetic',credential_ref=None,timeout_seconds=2,max_retries=1)
            for kind in ('roles','dm')}


class BindingTests(unittest.TestCase):
    def request(self):
        return dict(schema_version=2,person_id='p',intent_id='i',body=None,messages=[],
                    system_prompt='own knowledge only',prompt='own observation',service_version='a'*64)

    def test_model_text_not_prepared_body_is_consumed(self):
        from app.real_intent import make_candidate, body_matches
        q=self.request()
        result=make_candidate(q,'我拒绝命令，先研究水轮。',dict(requests=1,input_tokens=None,output_tokens=None,cost=None))
        self.assertEqual(result['body'],{'action':'propose','text':'我拒绝命令，先研究水轮。'})
        self.assertIsNone(q['body'])
        self.assertTrue(body_matches(q,result['body'],json.dumps(result['messages'])))
        self.assertFalse(body_matches(q,{'action':'wait'},json.dumps(result['messages'])))

    def test_prepared_null_only_and_legacy_equality_preserved(self):
        from app.real_intent import body_matches
        q=self.request()
        self.assertTrue(body_matches(q,None,None))
        self.assertFalse(body_matches(q,{'action':'wait'},None))
        self.assertTrue(body_matches({'body':{'action':'wait'}},{'action':'wait'},None))
        self.assertFalse(body_matches({'body':{'action':'wait'}},{'action':'propose'},None))
        q['schema_version']=3
        self.assertFalse(body_matches(q,None,None))

    def test_recovery_binding_rejects_identity_request_prefix_and_usage_corruption(self):
        from app.real_intent import make_candidate, body_matches
        q=self.request(); r=make_candidate(q,'等待观察',dict(requests=1,input_tokens=None,output_tokens=None,cost=None))
        for key,value in [('person_id','other'),('intent_id','other'),('prompt','changed'),('messages',[{'role':'assistant','content':'foreign secret'}]),('service_version','b'*64)]:
            changed=copy.deepcopy(q);changed[key]=value
            self.assertFalse(body_matches(changed,r['body'],json.dumps(r['messages'])),key)
        messages=copy.deepcopy(r['messages']);messages[-1]['intent_binding']['body']['text']='tampered'
        self.assertFalse(body_matches(q,r['body'],json.dumps(messages)))


class SettingsTests(unittest.TestCase):
    def test_status_and_save_do_not_read_vault_or_call_provider(self):
        from app.real_settings import SettingsStore
        with tempfile.TemporaryDirectory() as d:
            def forbidden(): raise AssertionError('vault construction')
            s=SettingsStore(Path(d)/'services.json',credential_factory=forbidden)
            self.assertEqual(s.status()['state'],'unconfigured')
            s.save({'services':config(),'secrets':{}})
            self.assertEqual(s.status()['state'],'metadata_configured')
            self.assertEqual(s.status()['credential_status'],'unknown')
            self.assertNotIn('18181',json.dumps(s.status()))
            self.assertEqual(SettingsStore(Path(d)/'services.json').snapshot(),config())

    def test_bad_urls_refs_extra_secret_fields_and_boolean_budgets_rejected(self):
        from app.real_settings import SettingsStore
        with tempfile.TemporaryDirectory() as d:
            s=SettingsStore(Path(d)/'services.json')
            for url in ['https://u:p@example.invalid/v1','https://example.invalid/v1?q=secret',
                        'https://example.invalid/#secret','http://example.invalid/v1','http://localhost.evil/v1',
                        'https://example.invalid/v1?','https://example.invalid/v1#']:
                c=config();c['roles']['base_url']=url
                with self.subTest(url=url),self.assertRaises(ValueError):s.save({'services':c,'secrets':{}})
            for field,value in [('credential_ref','civilization-theatre/service/'+'f'*32),('api_key','SYNTHETIC'),('timeout_seconds',True)]:
                c=config();c['roles'][field]=value
                with self.subTest(field=field),self.assertRaises(ValueError):s.save({'services':c,'secrets':{}})
            with self.assertRaises(ValueError):s.save({'services':config(),'secrets':{},'token':'SYNTHETIC'})
            self.assertFalse((Path(d)/'services.json').exists())

    def test_secret_is_new_game_ref_never_metadata_and_no_old_delete(self):
        from app.real_settings import SettingsStore
        writes=[]
        class Vault:
            def write(self,ref,value): writes.append((ref,bytes(value)))
            def read(self,*a): raise AssertionError('save must not read')
            def delete(self,*a): raise AssertionError('never delete old credentials')
        with tempfile.TemporaryDirectory() as d:
            s=SettingsStore(Path(d)/'services.json',credential_factory=Vault)
            c=config()
            for service in c.values():service.update(provider='openai-chat-https',base_url='https://service.example.invalid/v1')
            s.save({'services':c,'secrets':{'roles':'SYNTHETIC_ROLE','dm':'SYNTHETIC_DM'}})
            first=s.snapshot();self.assertNotEqual(first['roles']['credential_ref'],first['dm']['credential_ref'])
            self.assertEqual(len(writes),2)
            self.assertNotIn('SYNTHETIC_ROLE',(Path(d)/'services.json').read_text())
            s.save({'services':c,'secrets':{'roles':'SYNTHETIC_REPLACEMENT'}})
            self.assertNotEqual(first['roles']['credential_ref'],s.snapshot()['roles']['credential_ref'])
            self.assertEqual(first['dm']['credential_ref'],s.snapshot()['dm']['credential_ref'])


class GatewayTests(unittest.TestCase):
    def setup_gateway(self,d,*,transport=None,vault=None,config_value=None):
        from app.real_settings import SettingsStore
        from app.real_provider import RealGateway
        s=SettingsStore(Path(d)/'services.json',credential_factory=vault)
        s.save({'services':config_value or config(),'secrets':{}})
        return s,RealGateway(s,transport=transport,credential_factory=vault)

    def test_independent_roles_dm_no_conversation_crossfeed_unknown_usage(self):
        from app.real_intent import make_request
        calls=[]
        def transport(service,messages,token,budget):
            calls.append((service['model'],copy.deepcopy(messages),token))
            text='自由研究冶金' if service['model'].startswith('roles') else json.dumps(dict(ruling='成功',reason='有限行动',goal='观察',costs=[],benefits=[],effects=[{'action':'wait'}]),ensure_ascii=False)
            return {'choices':[{'message':{'content':text}}]}
        with tempfile.TemporaryDirectory() as d:
            s,g=self.setup_gateway(d,transport=transport)
            q=make_request('p','i',[], 'private role prompt','private facts',g.version())
            r=g.role(q,None)
            dm=g.dm({'proposal':{'person_id':'p','intent_id':'i','text':r['body']['text']},'world':{'world_id':'w','revision':2}},None)
            self.assertEqual(dm['world_id'],'w');self.assertEqual(dm['base_revision'],2)
            self.assertEqual([c[0] for c in calls],['roles-synthetic','dm-synthetic'])
            self.assertNotIn('private role prompt',json.dumps(calls[1]))
            self.assertIsNone(r['messages'][-1]['intent_binding']['usage']['input_tokens'])
            self.assertIsNone(r['messages'][-1]['intent_binding']['usage']['cost'])
            self.assertEqual(r['sdk_requests'],0)

    def test_configuration_incomplete_no_transport_and_no_fallback(self):
        from app.real_settings import SettingsStore
        from app.real_provider import RealGateway
        with tempfile.TemporaryDirectory() as d:
            g=RealGateway(SettingsStore(Path(d)/'services.json'),transport=lambda *a:self.fail('network'))
            with self.assertRaisesRegex(Exception,'REAL_UNCONFIGURED'):g.role({},None)
            with self.assertRaisesRegex(Exception,'REAL_UNCONFIGURED'):g.dm({},None)
            g.settings.save({'services':config(),'secrets':{}})
            with self.assertRaisesRegex(Exception,'REAL_CONFIGURATION_CHANGED'):g.dm({},None,'0'*64)

    def test_retries_finite_and_attempt_checked_before_every_retry(self):
        from app.real_provider import ProviderFault
        from app.real_intent import make_request
        calls=[]
        def transport(*args):calls.append(1);raise ProviderFault('REAL_TRANSPORT',retryable=True)
        with tempfile.TemporaryDirectory() as d:
            s,g=self.setup_gateway(d,transport=transport)
            q=make_request('p','i',[],'s','p',g.version())
            with self.assertRaisesRegex(Exception,'REAL_TRANSPORT'):g.role(q,None)
            self.assertEqual(len(calls),2)
            journal=[json.loads(line) for line in s.usage_path.read_text().splitlines()]
            self.assertEqual(sum(x['event']=='request_started' for x in journal),2)
            self.assertTrue(all(x['cost'] is None for x in journal))

    def test_cancelled_attempt_never_calls_transport(self):
        class Cancelled:
            deadline=float('inf')
            def check(self):raise RuntimeError('ACTOR_EXECUTION_CANCELLED')
        with tempfile.TemporaryDirectory() as d:
            s,g=self.setup_gateway(d,transport=lambda *a:self.fail('network after cancel'))
            with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_CANCELLED'):g.dm({},Cancelled())

    def test_secret_closed_for_transport_parse_echo_and_adapter_failures(self):
        from app.real_provider import RealGateway
        from app.real_settings import SettingsStore
        from app.real_intent import make_request
        for mode in ('throw','echo','bad-json','ok'):
            owned=[]
            class Secret:
                def __init__(self):self.closed=False;owned.append(self)
                def reveal_bytes(self):return b'SYNTHETIC_SECRET_SENTINEL'
                def close(self):self.closed=True
            class Vault:
                def write(self,*a):pass
                def read(self,*a):return Secret()
            def transport(*a):
                if mode=='throw':raise RuntimeError('SYNTHETIC_SECRET_SENTINEL')
                if mode=='echo':return {'choices':[{'message':{'content':'SYNTHETIC_SECRET_SENTINEL'}}]}
                if mode=='bad-json':return {'choices':[]}
                return {'choices':[{'message':{'content':'等待'}}]}
            with tempfile.TemporaryDirectory() as d:
                s=SettingsStore(Path(d)/'services.json',credential_factory=Vault)
                c=config()
                for item in c.values():item.update(provider='openai-chat-https',base_url='https://service.example.invalid/v1',max_retries=0)
                s.save({'services':c,'secrets':{'roles':'SYNTHETIC_SECRET_SENTINEL','dm':'SYNTHETIC_SECRET_SENTINEL'}})
                g=RealGateway(s,transport=transport,credential_factory=Vault)
                q=make_request('p','i',[],'s','p',g.version())
                if mode=='ok':g.role(q,None)
                else:
                    with self.assertRaises(Exception) as caught:g.role(q,None)
                    self.assertNotIn('SYNTHETIC_SECRET_SENTINEL',str(caught.exception))
                self.assertTrue(owned and all(x.closed for x in owned))
                self.assertNotIn('SYNTHETIC_SECRET_SENTINEL',s.usage_path.read_text())

    def test_redirect_error_body_oversize_and_close_boundary(self):
        from app.real_provider import HTTPTransport,RequestBudget,ProviderFault
        class Response:
            status=302
            def read(self,*a):raise AssertionError('must not read error body')
            def close(self):pass
        class Connection:
            def __init__(self,*a,**k):self.closed=False
            def request(self,*a,**k):pass
            def getresponse(self):return Response()
            def close(self):self.closed=True
        transport=HTTPTransport();connections=[]
        def factory(*a,**k):c=Connection();connections.append(c);return c
        with patch('app.real_provider.http.client.HTTPConnection',factory):
            with self.assertRaises(ProviderFault):transport(config()['roles'],[],None,RequestBudget(None,2))
        self.assertTrue(all(c.closed for c in connections))
        transport.close();self.assertFalse(transport.active)


class OwnedIntegrationTests(unittest.TestCase):
    """Requires later owned runtime authorization; uses fake provider ONLY."""
    def test_real_model_candidate_recovery_same_ids_not_chooser_echo(self):
        from app.product_cli import initialize_owned,open_session,SCENARIOS
        from app.real_settings import SettingsStore
        with tempfile.TemporaryDirectory() as d:
            owned=Path(d)/'owned';initialize_owned(owned,SCENARIOS/'iron-4.json')
            SettingsStore(owned/'services.json').save({'services':config(),'secrets':{}})
            calls=[]
            def transport(service,messages,*a):
                calls.append(service['model'])
                text='我决定等待并观察，不采纳脚本。' if service['model'].startswith('roles') else json.dumps(dict(ruling='成功',reason='自主等待',goal='等待观察',costs=[],benefits=[],effects=[{'action':'wait'}]),ensure_ascii=False)
                return {'choices':[{'message':{'content':text}}]}
            with open_session(owned,'real') as session:
                session.host.real_gateway.transport=transport
                session.host._choose=lambda *a:self.fail('scripted chooser called in real mode')
                original=session.host._submit; captured=[]
                def cut(person,intent,body,**kw):
                    captured.append((person,intent,copy.deepcopy(body)))
                    if len(captured)==1:raise RuntimeError('synthetic candidate cut')
                    return original(person,intent,body,**kw)
                session.host._submit=cut
                # Direct act executes original captured lease path; no registered
                # activation may be fabricated. Coordinator test below owns windows.
                session.coordinator.start(continuous=False)
                session.coordinator.worker.join(120)
                self.assertFalse(session.coordinator.worker.is_alive())
                self.assertTrue(captured)
                self.assertEqual(captured[0][2]['action'],'propose')
                self.assertEqual(captured[0][2]['text'],'我决定等待并观察，不采纳脚本。')
                # Inspect retained immutable request and candidate at crash boundary.
                with session.host._connect() as db:
                    row=db.execute('SELECT * FROM actor_intents WHERE intent_id=?',(captured[0][1],)).fetchone()
                self.assertEqual(row['state'],'candidate')
                self.assertIsNone(json.loads(row['request'])['body'])
                self.assertEqual(json.loads(row['body']),captured[0][2])
                self.assertIsNone(session.world.receipt(captured[0][0],captured[0][1]))
                # A technical-blocked activation must NOT silently rerun. Retain
                # original ID/material for the separate authorized maintenance seam.
                before=len(calls)
                import sqlite3
                from contextlib import closing
                from app.coordinator_scheduler import ActivationRef
                from app.world import WorldError
                with closing(sqlite3.connect(session.world.db_path)) as db:
                    active=db.execute('SELECT world_id,window_id,activation_id,person_id FROM coordinator_activations WHERE intent_id=?',(captured[0][1],)).fetchone()
                ref=ActivationRef(*active)
                with self.assertRaises(WorldError) as caught:
                    session.host.act(ref.person_id,coordinator=session.coordinator._owner_attempt.permit,activation=ref)
                self.assertEqual(caught.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
                self.assertEqual(len(calls),before)


if __name__=='__main__':unittest.main()
