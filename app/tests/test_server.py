"""Real loopback HTTP contract tests; the world/actor objects are unit doubles."""
from pathlib import Path
import http.client
import importlib
import json
import threading
import time
import unittest

class WorldDouble:
    def __init__(self):
        self.state={'world_id':'test','revision':1,'time':0,'horizon':7776000,
                    'status':'user_paused','mode':'scripted','countries':[],
                    'places':[],'persons':[],'events':[],'queue':[],'in_transit':[]}
    def snapshot(self,seq=None): return dict(self.state)
    def control(self,action):
        self.state['status']='running' if action=='resume' else 'user_paused'
        return self.snapshot()
    def advance(self,target):
        self.state['time']=target
        if target>=self.state['horizon']: self.state['status']='horizon_paused'
        return self.snapshot()
    def technical_pause(self,code):
        self.state['status']='technical_paused'
        return self.snapshot()
    def observe(self,person): return {'person':{'id':person},'observations':[],'messages':[]}
    def edit_card(self,person,changes): return self.snapshot()

class ActorsDouble:
    def __init__(self): self.calls=[]
    def act(self,person):
        self.calls.append(person)
        return {'provider':'scripted-pi'}
    def close(self): pass

class ServerTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue((Path(__file__).parents[1]/'server.py').exists(), 'loopback server is not implemented')
        module=importlib.import_module('app.server')
        self.world=WorldDouble()
        self.actors=ActorsDouble()
        self.server=module.make_server(self.world,self.actors,port=0)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.port=self.server.server_address[1]
        self.addCleanup(self.cleanup_server)
    def cleanup_server(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(5)
    def request(self,path,method='GET',body=None,headers=None):
        connection=http.client.HTTPConnection('127.0.0.1',self.port,timeout=5)
        try:
            connection.request(method,path,body=body,headers=headers or {})
            response=connection.getresponse(); raw=response.read()
            return response.status,raw,dict(response.getheaders())
        finally: connection.close()
    def test_metadata_reports_scripted_provider_and_ephemeral_csrf(self):
        code,raw,headers=self.request('/api/meta')
        self.assertEqual(code,200)
        meta=json.loads(raw)
        self.assertEqual(meta['mode'],'scripted-pi')
        self.assertGreaterEqual(len(meta['csrf']),32)
        self.assertIn('no-store',headers['Cache-Control'])

    def test_metadata_has_no_hardcoded_four_countries_or_current_world_payload(self):
        self.world.state['countries'] = [{'id':f'country-{n}','name':'CURRENT_WORLD_ONLY'} for n in range(1,7)]
        code,raw,_ = self.request('/api/meta')
        self.assertEqual(code,200)
        meta = json.loads(raw)
        self.assertNotIn('四国',' '.join(meta['limitations']))
        self.assertNotIn('CURRENT_WORLD_ONLY',raw.decode('utf-8'))
        self.assertNotIn('country_count',meta)

    def test_state_and_history_reads_do_not_change_world(self):
        before=self.world.snapshot()
        for path in ('/api/state','/api/state?seq=1'):
            code,raw,_=self.request(path)
            self.assertEqual(code,200)
            self.assertEqual(json.loads(raw)['revision'],1)
        self.assertEqual(before,self.world.snapshot())

    def test_foreign_host_and_unauthenticated_writes_are_rejected(self):
        code,_,_=self.request('/api/meta',headers={'Host':f'evil.invalid:{self.port}'})
        self.assertEqual(code,403)
        for headers in ({},{'Origin':'https://evil.invalid'},
                        {'Origin':f'http://127.0.0.1:{self.port}','X-Theatre-CSRF':'wrong'}):
            with self.subTest(headers=headers):
                code,_,_=self.request('/api/control','POST','{}',headers)
                self.assertEqual(code,403)
        self.assertEqual(self.world.snapshot()['status'],'user_paused')

    def authorized_headers(self):
        _,raw,_=self.request('/api/meta')
        return {'Origin':f'http://127.0.0.1:{self.port}',
                'X-Theatre-CSRF':json.loads(raw)['csrf'],'Content-Type':'application/json'}

    def test_authorized_pause_and_forward_card_edit(self):
        for route,body in (('/api/control',{'action':'pause'}),
                           ('/api/card',{'person_id':'p1','changes':{'values':'谨慎'}})):
            code,raw,_=self.request(route,'POST',json.dumps(body),self.authorized_headers())
            self.assertEqual(code,200,raw)
            self.assertEqual(json.loads(raw)['status'],'user_paused')

    def test_invalid_mutation_payloads_cannot_reach_world(self):
        headers=self.authorized_headers()
        wrong_type={**headers,'Content-Type':'text/plain'}
        code,_,_=self.request('/api/control','POST','{"action":"pause"}',wrong_type)
        self.assertEqual(code,415)
        for body in ('[]','{','{"action":5}','{"action":"pause","resource":99}'):
            with self.subTest(body=body):
                code,_,_=self.request('/api/control','POST',body,headers)
                self.assertEqual(code,400)
        code,_,_=self.request('/api/control','POST',' '*65537,headers)
        self.assertEqual(code,413)
        code,_,_=self.request('/api/card','POST',json.dumps({'person_id':'p1','changes':{'grain':999}}),headers)
        self.assertEqual(code,400)

    def test_step_runs_a_bounded_actor_window_then_pauses(self):
        self.world.state['persons']=[{'id':'p1'},{'id':'p2'}]
        code,raw,_=self.request('/api/control','POST','{"action":"step"}',self.authorized_headers())
        self.assertEqual(code,202,raw)
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            code,raw,_=self.request('/api/state')
            state=json.loads(raw)
            if state['time']==86400 and state['status']=='user_paused': break
            time.sleep(.01)
        self.assertEqual(state['time'],86400)
        self.assertEqual(state['status'],'user_paused')
        self.assertEqual(self.actors.calls,['p1','p2'])

    def test_continuous_run_stops_at_horizon(self):
        self.world.state['horizon']=86400*3
        self.world.state['persons']=[{'id':'p1'}]
        code,raw,_=self.request('/api/control','POST','{"action":"run"}',self.authorized_headers())
        self.assertEqual(code,202,raw)
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            _,raw,_=self.request('/api/state'); state=json.loads(raw)
            if state['status']=='horizon_paused' and not state['runtime']['busy']: break
            time.sleep(.01)
        self.assertEqual(state['time'],86400*3)
        self.assertEqual(state['status'],'horizon_paused')
        self.assertEqual(self.actors.calls,['p1','p1'])

    def test_browser_entry_assets_and_person_projection_are_served(self):
        for path,mime in (('/','text/html'),('/app.js','javascript'),('/app.css','text/css')):
            code,raw,headers=self.request(path)
            self.assertEqual(code,200,raw)
            self.assertIn(mime,headers['Content-Type'])
            self.assertGreater(len(raw),100)
        code,raw,_=self.request('/api/person?id=p1')
        self.assertEqual(code,200,raw)
        self.assertEqual(json.loads(raw)['person']['id'],'p1')
        for path in ('/../data/scenario.json','/data/scenario.json','/server.py'):
            self.assertEqual(self.request(path)[0],404)

    def test_cli_help_is_available_without_creating_a_world(self):
        import subprocess,sys
        process=subprocess.run([sys.executable,'-B','-m','app.server','--help'],
                               capture_output=True,text=True,encoding='utf-8',timeout=10)
        self.assertEqual(process.returncode,0,process.stderr)
        self.assertIn('--data-dir',process.stdout)
        self.assertIn('--port',process.stdout)

if __name__=='__main__': unittest.main()
