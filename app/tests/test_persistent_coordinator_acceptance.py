"""A-only additional acceptance of implemented paths; not historical RED.

Real SQLite/Host/HTTP, scripted request seam (zero new SDK/model calls).
Whole-process kill and caller ACK continuation deliberately deferred to B/C.
"""
from contextlib import closing
from pathlib import Path
from unittest.mock import patch
import contextvars
import http.client
import json
import sqlite3
import tempfile
import threading
import unittest
from app import server
from app.actors import ActorHost
from app.server import Coordinator
from app.world import WorldStore,WorldError,DAY
from app.tests import test_actor_execution as request_fixtures

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
ROOT=Path(__file__).resolve().parents[2]


def scripted(host,requests):
    def request(payload):
        requests.append(payload['intent_id'])
        return request_fixtures.ActorExecutionTests.result(payload)
    host._request=request
    host._choose=lambda view,seen:({'action':'wait'},seen)


class PersistentCoordinatorAcceptanceTests(unittest.TestCase):
    def test_actual_main_factory_fresh_and_registered_reopen_no_extra_pause(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-main-positive-',dir=SCRATCH) as directory:
            instances=[]
            class Lifecycle:
                server_address=('127.0.0.1',0)
                def serve_forever(self):pass
                def server_close(self):self.coordinator.close()
            def build(world,actors,port,*,coordinator):
                from app.scripted_dm import DefaultActors,ScriptedDM
                self.assertIsInstance(actors,DefaultActors)
                self.assertIsInstance(actors.dm_provider,ScriptedDM)
                self.assertIs(actors.adjudicator.world,world)
                self.assertEqual(world.snapshot()['status'],'user_paused')
                self.assertIsNotNone(coordinator._prepare_scheduler(coordinator._owner_attempt))
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM coordinator_windows').fetchone(),(0,))
                value=Lifecycle();value.coordinator=coordinator;instances.append(value);return value
            with patch.object(server,'make_server',build):
                server.main(['--data-dir',directory,'--port','0'])
                before=WorldStore(Path(directory)/'world.sqlite').snapshot()
                server.main(['--data-dir',directory,'--port','0'])
                self.assertEqual(WorldStore(Path(directory)/'world.sqlite').snapshot(),before)
            for instance in instances:
                self.assertTrue(instance.coordinator.actors._closed)
                self.assertFalse(instance.coordinator.cleanup_pending)
                self.assertFalse(instance.coordinator._owner_attempt.thread.is_alive())

    def test_missing_private_unknown_main_and_old_unbound_private_are_not_bootstrapped(self):
        for mode in ('unknown_missing','old_unbound','old_nonempty'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory(prefix='p023b4-source-negative-',dir=SCRATCH) as directory:
                root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
                if mode=='unknown_missing':world.control('resume');world.advance(DAY)
                owner=Coordinator(world,None);owner.acquire_owner();private=root/'actors.sqlite'
                if mode!='unknown_missing':
                    with closing(sqlite3.connect(private)) as db:
                        db.execute('CREATE TABLE old_content(value TEXT)')
                        if mode=='old_nonempty':db.execute("INSERT INTO old_content VALUES ('preserve')")
                        db.commit()
                old=private.read_bytes() if private.exists() else None;before=world.snapshot()
                host=ActorHost(world,private);owner.actors=host
                try:
                    with self.assertRaises(WorldError) as caught:owner.start()
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    self.assertEqual(world.snapshot(),before);self.assertFalse(owner.busy);self.assertIsNone(owner.worker)
                    self.assertEqual(private.read_bytes() if private.exists() else None,old)
                finally:owner.close()

    def test_bootstrap_actual_transaction_rechecks_main_and_final_source(self):
        for mode in ('main_change','conflicting_source'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory(prefix='p023b4-bootstrap-final-',dir=SCRATCH) as directory:
                root=Path(directory);world=WorldStore(root/'world.sqlite');before=world.initialize()
                owner=Coordinator(world,None);owner.acquire_owner();host=ActorHost(world,root/'actors.sqlite');owner.actors=host;other=[]
                original=host._prove_legacy_ownership
                def interleave(db,identity):
                    original(db,identity)
                    if mode=='main_change':
                        state=json.loads(db.execute('SELECT state FROM main.snapshots').fetchone()[0]);state['time']=DAY
                        state['revision']=2
                        db.execute('INSERT INTO main.snapshots VALUES (?,?)',(2,json.dumps(state)))
                    else:
                        other.append(contextvars.Context().run(lambda:ActorHost(world,root/'different.sqlite')))
                host._prove_legacy_ownership=interleave
                try:
                    with owner._owner_scope(owner._owner_attempt):
                        with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    self.assertEqual(world.snapshot(),before)
                    with closing(sqlite3.connect(host.db_path)) as db:
                        self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),[])
                finally:
                    for item in other:item.close()
                    owner.close()

    def test_same_file_sources_are_allowed_but_conflicting_live_source_refused(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-source-registry-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite')
            owner=Coordinator(world,host);owner.acquire_owner();same=ActorHost(world,root/'actors.sqlite');other=None
            try:
                with owner._owner_scope(owner._owner_attempt):owner._prepare_scheduler(owner._owner_attempt)
                before=world.snapshot();other=ActorHost(world,root/'different.sqlite')
                with self.assertRaises(WorldError):owner.start()
                self.assertEqual(world.snapshot(),before);self.assertFalse((root/'different.sqlite').exists())
            finally:
                if other:other.close()
                same.close();owner.close()

    def test_real_domain_closed_one_continues_original_roster(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-domain-one-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');state=world.initialize()
            host=ActorHost(world,root/'actors.sqlite');requests=[];scripted(host,requests)
            first=state['persons'][0];host._choose=lambda view,seen:({'action':'travel','to':first['location']} if view['person']['id']==first['id'] else {'action':'wait'},seen)
            owner=Coordinator(world,host)
            try:
                owner.start();owner.worker.join(15);self.assertFalse(owner.worker.is_alive());self.assertIsNone(owner.error)
                self.assertEqual((world.snapshot()['time'],world.snapshot()['status']),(DAY,'user_paused'))
                self.assertEqual(len(requests),12)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE roster_index=0').fetchone(),('rejected',1))
                    self.assertEqual(db.execute('SELECT state FROM coordinator_windows').fetchone(),('completed',))
                    self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone(),(11,))
            finally:owner.close()

    def test_real_domain_closed_zero_recovers_same_id_without_new_provider(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-domain-zero-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');state=world.initialize()
            host=ActorHost(world,root/'actors.sqlite');requests=[];scripted(host,requests);first=state['persons'][0]
            host._choose=lambda view,seen:({'action':'travel','to':first['location']} if view['person']['id']==first['id'] else {'action':'wait'},seen)
            original=host._reject_result
            def interrupt(*args,**kwargs):raise WorldError('ACTIVATION_BINDING_CONFLICT','controlled private-close interruption')
            host._reject_result=interrupt;owner=Coordinator(world,host)
            try:
                owner.start();owner.worker.join(5)
                self.assertEqual(owner.error,'ACTIVATION_BINDING_CONFLICT');self.assertEqual(len(requests),1)
                with closing(sqlite3.connect(world.db_path)) as db:
                    original_id=db.execute('SELECT activation_id,intent_id,phase,private_closed FROM coordinator_activations WHERE roster_index=0').fetchone()
                self.assertEqual(original_id[2:],('rejected',0))
                host._reject_result=original;owner.start();owner.worker.join(15)
                self.assertIsNone(owner.error);self.assertFalse(owner.worker.is_alive());self.assertEqual(len(requests),12)
                with closing(sqlite3.connect(world.db_path)) as db:
                    row=db.execute('SELECT activation_id,intent_id,phase,private_closed FROM coordinator_activations WHERE roster_index=0').fetchone()
                    self.assertEqual(row,(*original_id[:2],'rejected',1))
                    self.assertEqual(db.execute('SELECT state FROM coordinator_windows').fetchone(),('completed',))
            finally:owner.close()

    def test_real_host_technical_block_is_preserved_and_start_cannot_resume(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-blocked-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite')
            host._request=lambda payload:(_ for _ in ()).throw(RuntimeError('controlled provider failure'))
            owner=Coordinator(world,host)
            try:
                owner.start();owner.worker.join(5);self.assertEqual(owner.error,'HOST_FAILURE')
                before=world.snapshot();self.assertEqual(before['status'],'technical_paused')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,resume_phase FROM coordinator_activations WHERE roster_index=0').fetchone(),('technical_blocked','prepared'))
                    self.assertEqual(db.execute('SELECT state FROM coordinator_windows').fetchone(),('open',))
                with self.assertRaises(WorldError) as caught:owner.start()
                self.assertEqual(caught.exception.code,'WORLD_PAUSED');self.assertEqual(world.snapshot(),before)
            finally:owner.close()

    def test_horizon_empty_window_and_explicit_extension_once_keeps_queue(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-horizon-',dir=SCRATCH) as directory:
            root=Path(directory);config=json.loads((ROOT/'app/data/scenario.json').read_text(encoding='utf8'));config['horizon_days']=1
            scenario=root/'scenario.json';scenario.write_text(json.dumps(config),encoding='utf8')
            world=WorldStore(root/'world.sqlite',scenario_path=scenario);world.initialize();host=ActorHost(world,root/'actors.sqlite');requests=[];scripted(host,requests)
            owner=Coordinator(world,host)
            try:
                owner.start(continuous=True);owner.worker.join(5);self.assertIsNone(owner.error)
                at=world.snapshot();self.assertEqual((at['time'],at['status']),(DAY,'horizon_paused'));self.assertEqual(requests,[])
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT state,roster_json FROM coordinator_windows').fetchall(),[('completed','[]')])
                original=world.control;resumes=[]
                def control(action):
                    result=original(action)
                    if action=='resume':resumes.append(result)
                    return result
                world.control=control
                owner.start();owner.worker.join(15);self.assertIsNone(owner.error)
                self.assertEqual(len(resumes),1);self.assertEqual(resumes[0]['time'],at['time']);self.assertEqual(resumes[0]['queue'],at['queue'])
                self.assertEqual(world.snapshot()['horizon'],at['horizon']+90*DAY)
                self.assertEqual(world.snapshot()['time'],2*DAY);self.assertEqual(len(requests),12)
            finally:owner.close()

    def test_http_observer_busy_csrf_and_actual_day_boundary_pause(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-http-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host);owner.acquire_owner()
            requests=[];scripted(host,requests);entered=threading.Event();release=threading.Event();original=host._request
            def request(payload):
                if not requests:entered.set();self.assertTrue(release.wait(5))
                return original(payload)
            host._request=request
            otherworld=WorldStore(world.db_path);otherhost=ActorHost(otherworld,root/'actors.sqlite')
            services=[server.make_server(world,host,0,coordinator=owner),server.make_server(otherworld,otherhost,0)]
            threads=[threading.Thread(target=s.serve_forever) for s in services]
            for thread in threads:thread.start()
            def request_http(index,method,path,body=None,token=None):
                service=services[index];port=service.server_address[1];connection=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                headers={'Origin':f'http://127.0.0.1:{port}','Content-Type':'application/json'}
                if token:headers['X-Theatre-CSRF']=token
                try:
                    connection.request(method,path,None if body is None else json.dumps(body),headers);response=connection.getresponse();return response.status,json.loads(response.read())
                finally:connection.close()
            try:
                before=world.snapshot();token=request_http(0,'GET','/api/meta')[1]['csrf'];other=request_http(1,'GET','/api/meta')[1]['csrf']
                self.assertEqual(request_http(1,'GET','/api/state')[0],200);self.assertEqual(world.snapshot(),before)
                self.assertEqual(request_http(0,'POST','/api/control',{'action':'step'})[0],403)
                self.assertEqual(request_http(1,'POST','/api/control',{'action':'step'},other)[1]['error']['code'],'COORDINATOR_EXECUTION_BUSY')
                self.assertEqual(request_http(0,'POST','/api/control',{'action':'run'},token),(202,{'accepted':True}));self.assertTrue(entered.wait(3))
                self.assertEqual(request_http(0,'POST','/api/control',{'action':'step'},token)[1]['error']['code'],'BUSY')
                status,payload=request_http(0,'POST','/api/control',{'action':'pause'},token)
                self.assertEqual(status,200);self.assertTrue(payload['runtime']['pause_requested']);self.assertEqual(payload['status'],'running')
                release.set();owner.worker.join(15);self.assertFalse(owner.worker.is_alive());self.assertIsNone(owner.error)
                self.assertEqual(len(requests),12);self.assertEqual((world.snapshot()['time'],world.snapshot()['status']),(DAY,'user_paused'))
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT state FROM coordinator_windows').fetchall(),[('completed',)])
            finally:
                release.set()
                for service in services:service.shutdown();service.server_close()
                for thread in threads:thread.join(5);self.assertFalse(thread.is_alive())
                self.assertFalse(owner.cleanup_pending);self.assertFalse(owner._owner_attempt.thread.is_alive())
