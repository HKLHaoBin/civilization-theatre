"""Future real-path synthetic-provider integration tests, unexecuted in 967.
Uses actual registered Host/World/SQLite guards; only external transport is fake.
No SDK, real-service, machine reboot or billing exactly-once claim.
"""
from contextlib import closing,contextmanager
import copy
import http.client
import json
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from app.tests.test_real_provider967 import config


class RealActivationTests(unittest.TestCase):
    @contextmanager
    def registered(self):
        from app.tests.test_actor_activations import ActorActivationTests
        from app.adjudication import Adjudicator
        from app.real_provider import RealGateway
        from app.real_settings import SettingsStore
        # real_pi=True only disables the OLD scripted _request replacement.
        # Installing real_gateway below means no Node/Pi subprocess is started.
        with ActorActivationTests.registered(self,real_pi=True) as (world,host,owner,ref,unused):
            settings=SettingsStore(Path(host.db_path).with_name('services.json'))
            settings.save({'services':config(),'secrets':{}})
            calls=[]
            def transport(service,messages,token,budget):
                calls.append((service['model'],copy.deepcopy(messages)))
                text='我拒绝既定脚本，先等待亲见消息。' if service['model'].startswith('roles') else json.dumps(dict(ruling='成功',reason='等待不创造物资',goal='等候',costs=[],benefits=[],effects=[{'action':'wait'}]),ensure_ascii=False)
                return {'choices':[{'message':{'content':text}}],'usage':{'prompt_tokens':17,'completion_tokens':9}}
            with world._coordinator_scope(owner):host.adjudicator=Adjudicator(world)
            host.dm_provider=lambda p:self.fail('unbound DM bypass')
            host.real_gateway=RealGateway(settings,transport=transport)
            host.real_settings=settings
            host._choose=lambda *a:self.fail('scripted chooser called')
            yield world,host,owner,ref,calls,transport

    def test_prepared_candidate_and_worldcommit_reopen_reuse_original_ids(self):
        from app.product_cli import _create_product_actors
        for cut in ('prepared','candidate','world_committed'):
            with self.subTest(cut=cut),self.registered() as (world,host,owner,ref,calls,transport):
                if cut=='prepared':
                    host._request=lambda q:(_ for _ in ()).throw(RuntimeError('synthetic prepared cut'))
                elif cut=='candidate':
                    host._submit=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('synthetic candidate cut'))
                else:
                    host._finish=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('synthetic world committed cut'))
                with self.assertRaisesRegex(RuntimeError,'synthetic'):
                    host.act(ref.person_id,coordinator=owner,activation=ref)
                with closing(sqlite3.connect(host.db_path)) as db:
                    original=db.execute('SELECT intent_id,request,body,candidate FROM actor_intents').fetchone()
                self.assertIsNone(json.loads(original[1])['body'])
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()[0],cut)
                host.close()
                with world._coordinator_scope(owner):fresh=_create_product_actors(world,host.db_path,'real')
                try:
                    fresh.real_gateway.transport=transport
                    fresh._choose=lambda *a:self.fail('scripted recovery fallback')
                    result=fresh.act(ref.person_id,coordinator=owner,activation=ref)
                    again=fresh.act(ref.person_id,coordinator=owner,activation=ref)
                    self.assertEqual(result['intent_id'],original[0])
                    self.assertEqual(result['receipt'],again['receipt'])
                    self.assertEqual(result['body'],{'action':'propose','text':'我拒绝既定脚本，先等待亲见消息。'})
                    self.assertEqual([m for m,_ in calls],['roles-synthetic','dm-synthetic'])
                    with closing(sqlite3.connect(world.db_path)) as db:
                        self.assertEqual(db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('completed',1))
                        self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0],2)
                        self.assertEqual(db.execute('SELECT count(*) FROM coordinator_activation_outcomes').fetchone()[0],1)
                    with closing(sqlite3.connect(host.db_path)) as db:
                        self.assertEqual(db.execute('SELECT count(*),min(intent_id) FROM actor_intents').fetchone(),(1,original[0]))
                        self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(1,))
                        self.assertEqual(db.execute('SELECT request FROM actor_intents').fetchone()[0],original[1])
                finally:fresh.close()

    def test_body_column_tamper_cannot_launder_v2_candidate(self):
        from app.world import WorldError
        with self.registered() as (world,host,owner,ref,calls,transport):
            submit=host._submit;observed=[]
            def tamper(person,intent,body,*,execution=None):
                replacement={'action':'propose','text':'篡改模型真实意图'}
                with closing(sqlite3.connect(host.db_path)) as db:
                    original=db.execute('SELECT body FROM actor_intents WHERE intent_id=?',(intent,)).fetchone()[0]
                    db.execute('UPDATE actor_intents SET body=? WHERE intent_id=?',(json.dumps(replacement),intent));db.commit()
                try:
                    try:submit(person,intent,replacement,execution=execution)
                    except WorldError as error:observed.append(error.code)
                    else:observed.append('WRONGLY_ACCEPTED')
                finally:
                    with closing(sqlite3.connect(host.db_path)) as db:
                        db.execute('UPDATE actor_intents SET body=? WHERE intent_id=?',(original,intent));db.commit()
                return submit(person,intent,body,execution=execution)
            host._submit=tamper
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(observed,['WINDOW_RECOVERY_UNPROVEN'])
            self.assertEqual(result['receipt']['status'],'committed')
            self.assertEqual(len(calls),2)

    def test_candidate_transition_rollback_keeps_original_null_request_and_session_cas(self):
        with self.registered() as (world,host,owner,ref,calls,transport):
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute("CREATE TRIGGER injected_v2_candidate_abort BEFORE UPDATE OF phase ON coordinator_activations WHEN NEW.phase='candidate' BEGIN SELECT RAISE(ABORT,'synthetic candidate transaction abort'); END");db.commit()
            before=world.snapshot()
            with self.assertRaisesRegex(sqlite3.IntegrityError,'synthetic candidate transaction abort'):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(host.db_path)) as db:
                row=db.execute('SELECT state,body,candidate,metrics,request FROM actor_intents').fetchone()
                self.assertEqual(row[:4],('prepared','null',None,None))
                self.assertEqual(db.execute('SELECT count(*) FROM actor_calls').fetchone(),(0,))
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0,))
            self.assertEqual(world.snapshot(),before)
            self.assertEqual(len(calls),1)

    def test_dm_configuration_change_fails_without_replacing_role_candidate(self):
        from app.real_provider import ProviderFault
        with self.registered() as (world,host,owner,ref,calls,transport):
            original=host._submit
            captured=[]
            def change(person,intent,body,**kw):
                captured.append(copy.deepcopy(body))
                value=config();value['dm']['model']='new-independent-dm'
                host.real_settings.save({'services':value,'secrets':{}})
                return original(person,intent,body,**kw)
            host._submit=change
            from app.world import WorldError
            with self.assertRaises(WorldError) as caught:
                host.act(ref.person_id,coordinator=owner,activation=ref)
            # The real Adjudicator deliberately wraps provider failures; assert
            # its actual technical-pause envelope, not a bypassed fake seam.
            self.assertEqual(caught.exception.code,'ADJUDICATION_FAILED')
            self.assertEqual(world.snapshot()['status'],'technical_paused')
            self.assertEqual([m for m,_ in calls],['roles-synthetic'])
            with closing(sqlite3.connect(host.db_path)) as db:
                row=db.execute('SELECT body,state,receipt FROM actor_intents').fetchone()
                self.assertEqual(json.loads(row[0]),captured[0]);self.assertEqual(row[1:],('candidate',None))


class SettingsHTTPTests(unittest.TestCase):
    """Future real loopback HTTP, world/actor doubles, no model request."""
    def setUp(self):
        from app.tests.test_server import WorldDouble,ActorsDouble
        from app.server import make_server
        from app.real_settings import SettingsStore
        self.temporary=tempfile.TemporaryDirectory()
        self.actors=ActorsDouble()
        self.actors.real_settings=SettingsStore(Path(self.temporary.name)/'services.json',credential_factory=lambda:self.fail('status/save loopback must not construct vault'))
        self.actors.real_gateway=object()
        self.server=make_server(WorldDouble(),self.actors,0)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.port=self.server.server_address[1]
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.server.shutdown();self.server.server_close();self.thread.join(5)
        self.temporary.cleanup()

    def request(self,path,method='GET',body=None,headers=None):
        c=http.client.HTTPConnection('127.0.0.1',self.port,timeout=5)
        try:
            c.request(method,path,body=body,headers=headers or {})
            r=c.getresponse();return r.status,r.read()
        finally:c.close()

    def test_status_without_vault_and_origin_csrf_matrix_then_persist_readback(self):
        code,raw=self.request('/api/meta');self.assertEqual(code,200)
        meta=json.loads(raw);self.assertEqual(meta['mode'],'real')
        origin='http://127.0.0.1:'+str(self.port)
        valid={'Origin':origin,'X-Theatre-CSRF':meta['csrf'],'Content-Type':'application/json'}
        payload=json.dumps({'services':config(),'secrets':{}})
        for h in ({},{'Origin':origin},{**valid,'Origin':'https://evil.invalid'},
                  {**valid,'X-Theatre-CSRF':'wrong'},{**valid,'Host':'evil.invalid'}):
            code,_=self.request('/api/settings','POST',payload,h)
            self.assertEqual(code,403)
            self.assertFalse(self.actors.real_settings.path.exists())
        code,raw=self.request('/api/settings');self.assertEqual(code,200)
        self.assertEqual(json.loads(raw)['state'],'unconfigured')
        code,raw=self.request('/api/settings','POST',payload,valid);self.assertEqual(code,200,raw)
        code,raw=self.request('/api/settings');self.assertEqual(code,200)
        self.assertEqual(json.loads(raw)['state'],'metadata_configured')
        self.assertEqual(self.actors.real_settings.snapshot(),config())
        self.assertNotIn(b'18181',raw)
        self.assertEqual(self.actors.calls,[])
        code,raw=self.request('/settings.html');self.assertEqual(code,200)
        self.assertIn(b'/settings.js',raw)

    def test_extra_secret_field_and_query_route_no_write_no_echo(self):
        _,raw=self.request('/api/meta');csrf=json.loads(raw)['csrf']
        h={'Origin':'http://127.0.0.1:'+str(self.port),'X-Theatre-CSRF':csrf,'Content-Type':'application/json'}
        payload={'services':config(),'secrets':{},'api_key':'SYNTHETIC_MUST_NOT_ECHO'}
        code,raw=self.request('/api/settings','POST',json.dumps(payload),h)
        self.assertEqual(code,400);self.assertNotIn(b'SYNTHETIC_MUST_NOT_ECHO',raw)
        code,_=self.request('/api/settings?key=SYNTHETIC','POST','{}',h);self.assertEqual(code,404)
        self.assertFalse(self.actors.real_settings.path.exists())
        self.assertEqual(self.actors.calls,[])


if __name__=='__main__':unittest.main()
