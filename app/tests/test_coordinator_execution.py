"""P023a first vertical authority behavior; actual World SQLite only."""
from pathlib import Path
import tempfile
import unittest
from app.world import WorldStore,WorldError

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class CoordinatorExecutionTests(unittest.TestCase):
    def test_first_root_acquisition_and_live_owner_contention_do_not_change_world(self):
        with tempfile.TemporaryDirectory(prefix='p023a-first-',dir=SCRATCH) as temp:
            path=Path(temp)/'world.sqlite';first=WorldStore(path);before=first.initialize()
            acquire=getattr(first,'acquire_coordinator_execution',None)
            self.assertTrue(callable(acquire),'World-backed coordinator execution acquisition is missing')
            permit=acquire('root-owner-first',ttl_seconds=60)
            self.assertEqual(permit.world_id,before['world_id'])
            self.assertEqual(permit.epoch,1)
            self.assertEqual(first.snapshot(),before,'root authority metadata is not a world event/revision')
            second=WorldStore(path);self.assertEqual(second.initialize(),before)
            with self.assertRaises(WorldError) as error:
                second.acquire_coordinator_execution('root-owner-second',ttl_seconds=60)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_BUSY')
            self.assertEqual(second.snapshot(),before)

    def test_root_renew_expiry_takeover_and_clock_independent_release(self):
        from dataclasses import FrozenInstanceError
        with tempfile.TemporaryDirectory(prefix='p023a-life-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);world.initialize()
            first=world.acquire_coordinator_execution('a',ttl_seconds=1)
            self.assertNotIn(first.owner_token,repr(first))
            with self.assertRaises(FrozenInstanceError):first.epoch=2
            self.assertTrue(callable(getattr(world,'renew_coordinator_execution',None)),'root renewal missing')
            now[0]+=500000000
            self.assertEqual(world.renew_coordinator_execution(first,ttl_seconds=1),first)
            now[0]+=1000000000
            with self.assertRaises(WorldError) as error:world.renew_coordinator_execution(first)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_EXPIRED')
            second=world.acquire_coordinator_execution('b')
            self.assertEqual(second.epoch,2)
            now[0]=1
            self.assertFalse(world.release_coordinator_execution(first))
            self.assertTrue(world.release_coordinator_execution(second))
            now[0]=102000000000
            third=world.acquire_coordinator_execution('c');self.assertEqual(third.epoch,3)

    def test_registered_root_permanently_fences_generic_control_writes_and_commit(self):
        import sqlite3
        from contextlib import closing
        with tempfile.TemporaryDirectory(prefix='p023a-gate-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();person=state['persons'][0]['id']
            root=world.acquire_coordinator_execution('owner',ttl_seconds=1)
            before=world.snapshot()
            with self.assertRaises(WorldError) as error:world.control('resume')
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_REQUIRED')
            world.control('resume',coordinator=root)
            world.advance(1,coordinator=root)
            world.edit_card(person,{'risk':'测试'},coordinator=root)
            before=world.snapshot()
            def late(state,db):
                state['time']=2;now[0]+=1000000000
            with self.assertRaises(WorldError) as error:
                with world._coordinator_scope(root):world._mutate(late)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_EXPIRED')
            self.assertEqual(world.snapshot(),before)
            now[0]+=1;new=world.acquire_coordinator_execution('new')
            with self.assertRaises(WorldError) as error:world.technical_pause('OLD',coordinator=root)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED')
            world.technical_pause('VALID',coordinator=new)
            world.release_coordinator_execution(new)
            with self.assertRaises(WorldError) as error:world._mutate(lambda s,d:None)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_REQUIRED')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM coordinator_execution_leases').fetchone()[0],1)
                self.assertFalse(any(root.owner_token in r[0] for r in db.execute('SELECT state FROM snapshots')))

    def test_actor_original_parent_is_required_and_takeover_invalidates_live_child(self):
        with tempfile.TemporaryDirectory(prefix='p023a-child-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();person=state['persons'][0]['id'];world.control('resume')
            orphan=world.acquire_actor_execution(person,'orphan')
            root=world.acquire_coordinator_execution('a',ttl_seconds=1)
            with self.assertRaises(WorldError) as error:world.submit(person,'orphan-id',{'action':'wait'},execution=orphan)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_REQUIRED')
            with world._coordinator_scope(root):
                with self.assertRaises(WorldError) as error:world.submit(person,'orphan-id',{'action':'wait'},execution=orphan)
                self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED')
            actor=world.acquire_actor_execution(person,'child',coordinator=root)
            with world._coordinator_scope(root):
                receipt=world.submit(person,'same-id',{'action':'wait'},execution=actor)
            now[0]+=1000000000;new=world.acquire_coordinator_execution('b')
            with self.assertRaises(WorldError) as error:world.renew_actor_execution(actor,coordinator=root)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED')
            with world._coordinator_scope(new):
                with self.assertRaises(WorldError) as error:world.submit(person,'same-id',{'action':'wait'},execution=actor)
                self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED')
            successor=world.acquire_actor_execution(person,'new-child',coordinator=new)
            self.assertEqual(successor.epoch,actor.epoch+1)
            self.assertFalse(world.release_actor_execution(actor))
            with world._coordinator_scope(new):self.assertEqual(world.submit(person,'same-id',{'action':'wait'},execution=successor),receipt)
            with self.assertRaises(WorldError) as error:world.acquire_actor_execution(person,'busy',coordinator=new)
            self.assertEqual(error.exception.code,'ACTOR_EXECUTION_BUSY')

    def test_actual_host_captures_parent_for_provider_private_finish_and_recovery(self):
        import os,sqlite3,json
        from contextlib import closing
        from unittest.mock import patch
        from app.actors import ActorHost
        with tempfile.TemporaryDirectory(prefix='p023a-host-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();person=state['persons'][0]['id']
            host=ActorHost(world,Path(temp)/'actor.sqlite');root=world.acquire_coordinator_execution('old',ttl_seconds=1);world.control('resume',coordinator=root)
            original=host._finish;captured=[];new=[]
            def late(*args,**kwargs):
                captured.append(args[0]);now[0]+=1000000000;new.append(world.acquire_coordinator_execution('new'))
                return original(*args,**kwargs)
            host._finish=late
            try:
                with patch.dict(os.environ,{'PI_MODULE_ROOT':str(Path('spikes/002-pi-isolation/npm/node_modules').resolve())}):
                    with self.assertRaises(WorldError) as error:host.act(person,coordinator=root,pause_on_failure=True)
                    self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED')
                    self.assertEqual(world.snapshot()['status'],'running')
                    host._finish=original
                    result=host.act(person,coordinator=new[0]);self.assertEqual(result['intent_id'],captured[0]);self.assertTrue(result['reconciled'])
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],1)
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone()[0],1)
                    request=db.execute('SELECT request FROM actor_intents').fetchone()[0]
                    self.assertNotIn(root.owner_token,request);self.assertNotIn(new[0].owner_token,request)
            finally:host.close()
            self.assertFalse(host.cleanup_pending);self.assertIsNone(host._process);self.assertIsNotNone(host.worker_exit_code)

    def test_coordinator_owns_real_world_with_noactors_and_observer_cannot_write(self):
        import hashlib,threading
        from app.server import Coordinator
        class NoActors:
            def act(self,person):pass
            def close(self):pass
        with tempfile.TemporaryDirectory(prefix='p023a-service-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');world.initialize()
            from app.tests.legacy_control_coordinator import LegacyControlCoordinator
            owner=LegacyControlCoordinator(world,NoActors());observer=Coordinator(WorldStore(world.db_path),NoActors())
            before=hashlib.sha256(world.db_path.read_bytes()).hexdigest()
            observer.snapshot();self.assertEqual(hashlib.sha256(world.db_path.read_bytes()).hexdigest(),before)
            self.assertTrue(callable(getattr(owner,'acquire_owner',None)),'explicit coordinator ownership missing')
            root=owner.acquire_owner()
            self.assertEqual(owner.acquire_owner(),root)
            with self.assertRaises(WorldError) as error:observer.start()
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_BUSY')
            owner.start();owner.worker.join(5);self.assertFalse(owner.worker.is_alive());self.assertIsNone(owner.error)
            self.assertEqual(world.snapshot()['time'],86400)
            self.assertEqual(world.snapshot()['status'],'user_paused')
            owner.edit_card(world.snapshot()['persons'][0]['id'],{'risk':'合法'})
            owner.close();observer.close()
            self.assertFalse(owner._owner_attempt.thread.is_alive())
            self.assertTrue(world.acquire_coordinator_execution('successor').epoch>root.epoch)

    def test_http_card_owner_and_observer_readonly(self):
        import threading,http.client,json,hashlib
        from app.server import make_server
        class NoActors:
            def act(self,person):pass
            def close(self):pass
        with tempfile.TemporaryDirectory(prefix='p023a-http-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize()
            server=make_server(world,NoActors(),0);observer=make_server(WorldStore(world.db_path),NoActors(),0)
            threads=[threading.Thread(target=s.serve_forever) for s in (server,observer)]
            for t in threads:t.start()
            def request(s,method,path,body=None,csrf=None):
                port=s.server_address[1];c=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                headers={'Origin':f'http://127.0.0.1:{port}','Content-Type':'application/json'}
                if csrf:headers['X-Theatre-CSRF']=csrf
                try:
                    c.request(method,path,None if body is None else json.dumps(body),headers);r=c.getresponse();return r.status,json.loads(r.read())
                finally:c.close()
            try:
                token=request(server,'GET','/api/meta')[1]['csrf']
                server.coordinator.acquire_owner()
                code,result=request(server,'POST','/api/card',{'person_id':state['persons'][0]['id'],'changes':{'risk':'受权'}},token)
                self.assertEqual(code,200,result)
                before=hashlib.sha256(world.db_path.read_bytes()).hexdigest()
                other=request(observer,'GET','/api/meta')[1]['csrf']
                self.assertEqual(request(observer,'GET','/api/state')[0],200)
                self.assertEqual(request(observer,'GET','/api/person?id='+state['persons'][0]['id'])[0],200)
                code,result=request(observer,'POST','/api/card',{'person_id':state['persons'][0]['id'],'changes':{'risk':'无权'}},other)
                self.assertEqual(code,409,result);self.assertEqual(result['error']['code'],'COORDINATOR_EXECUTION_BUSY')
                self.assertEqual(hashlib.sha256(world.db_path.read_bytes()).hexdigest(),before)
                self.assertEqual(request(observer,'POST','/api/control',{'action':'step','coordinator':'forged'},other)[0],400)
            finally:
                for s in (server,observer):s.shutdown();s.server_close()
                for t in threads:t.join(5);self.assertFalse(t.is_alive())

    def test_startup_running_archive_must_claim_before_pause_and_private_construction(self):
        from unittest.mock import patch
        from app import server as service
        from app.server import Coordinator
        with tempfile.TemporaryDirectory(prefix='p023a-startup-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');world.initialize();world.control('resume')
            blocker=world.acquire_coordinator_execution('another')
            before=world.db_path.read_bytes();constructed=[]
            with patch.object(service,'create_default_actors',side_effect=lambda *a:constructed.append(True)):
                with self.assertRaises(WorldError) as error:service.main(['--data-dir',temp,'--port','0'])
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_BUSY')
            self.assertEqual(constructed,[]);self.assertEqual(world.db_path.read_bytes(),before)
            world.release_coordinator_execution(blocker)
            class FakeServer:
                server_address=('127.0.0.1',0)
                def serve_forever(self):pass
                def server_close(self):self.coordinator.close()
            def build(w,a,p,**kwargs):
                self.assertEqual(w.snapshot()['status'],'user_paused')
                self.assertIsNotNone(kwargs.get('coordinator'))
                s=FakeServer();s.coordinator=kwargs['coordinator'];return s
            with patch.object(service,'make_server',side_effect=build):service.main(['--data-dir',temp,'--port','0'])
            self.assertEqual(world.snapshot()['status'],'user_paused')

    def test_nested_control_scope_preserves_original_root_cancellation_at_commit(self):
        from app.server import Coordinator
        class NoActors:
            def close(self):pass
        with tempfile.TemporaryDirectory(prefix='p023a-cancel-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();owner=Coordinator(world,NoActors());root=owner.acquire_owner();before=world.snapshot()
            original=world._event
            def cancel(*a,**k):
                result=original(*a,**k);owner._owner_attempt.stop.set();return result
            world._event=cancel
            try:
                with self.assertRaises(WorldError) as error:owner.edit_card(state['persons'][0]['id'],{'risk':'不得提交'})
                self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_CANCELLED');self.assertEqual(world.snapshot(),before)
            finally:world._event=original;owner.close()

    def test_root_only_sql_wait_is_cancelled_without_full_connection_timeout(self):
        import sqlite3,threading,time
        from app.server import Coordinator
        class NoActors:
            def close(self):pass
        with tempfile.TemporaryDirectory(prefix='p023a-rootwait-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();owner=Coordinator(world,NoActors());owner.acquire_owner();before=world.snapshot()
            db=sqlite3.connect(world.db_path);db.execute('BEGIN IMMEDIATE');errors=[]
            def write():
                try:owner.edit_card(state['persons'][0]['id'],{'risk':'晚写'})
                except Exception as error:errors.append(error)
            thread=threading.Thread(target=write);start=time.monotonic();thread.start();time.sleep(.05);owner._owner_attempt.stop.set();thread.join(.5)
            bounded=not thread.is_alive();db.rollback();db.close();thread.join(2);owner.close()
            self.assertTrue(bounded,'root SQL wait ignored captured cancellation')
            self.assertEqual(getattr(errors[0],'code',None),'COORDINATOR_EXECUTION_CANCELLED');self.assertEqual(world.snapshot(),before)

    def test_actual_pi_provider_inherits_original_root_cancellation(self):
        import os,sqlite3
        from contextlib import closing
        from unittest.mock import patch
        from app.actors import ActorHost
        from app.server import Coordinator
        with tempfile.TemporaryDirectory(prefix='p023a-parentcancel-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();host=ActorHost(world,Path(temp)/'actor.sqlite');owner=Coordinator(world,host);root=owner.acquire_owner();world.control('resume',coordinator=root)
            request=host._request
            def cancel(payload):
                result=request(payload);owner._owner_attempt.stop.set();return result
            host._request=cancel
            try:
                with patch.dict(os.environ,{'PI_MODULE_ROOT':str(Path('spikes/002-pi-isolation/npm/node_modules').resolve())}):
                    with self.assertRaises(WorldError) as error:
                        with owner._owner_scope():host.act(state['persons'][0]['id'],pause_on_failure=True)
                self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_CANCELLED')
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT state FROM actor_intents').fetchone()[0],'prepared')
                self.assertEqual(world.snapshot()['status'],'running')
            finally:host._request=request;owner.close()

    def test_adjudicator_propagates_root_authority_error_without_failure_pause(self):
        from app.adjudication import Adjudicator
        with tempfile.TemporaryDirectory(prefix='p023a-adj-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();adj=Adjudicator(world);root=world.acquire_coordinator_execution('root');world.control('resume',coordinator=root);person=state['persons'][0]['id'];actor=world.acquire_actor_execution(person,'child',coordinator=root)
            def provider(payload):raise WorldError('COORDINATOR_EXECUTION_FENCED','sanitized')
            with world._coordinator_scope(root):
                with self.assertRaises(WorldError) as error:adj.resolve(person,'proposal','原话',provider,execution=actor)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED');self.assertEqual(world.snapshot()['status'],'running')

    def test_existing_world_initialize_is_readonly_even_under_writer_lock(self):
        import sqlite3,threading
        with tempfile.TemporaryDirectory(prefix='p023a-open-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();world.acquire_coordinator_execution('root')
            before=world.db_path.read_bytes();db=sqlite3.connect(world.db_path);db.execute('BEGIN IMMEDIATE');out=[]
            thread=threading.Thread(target=lambda:out.append(WorldStore(world.db_path).initialize()));thread.start();thread.join(.4)
            bounded=not thread.is_alive();db.rollback();db.close();thread.join(2)
            self.assertTrue(bounded,'opening an existing world took the write lock before ownership')
            self.assertEqual(out,[state]);self.assertEqual(before,world.db_path.read_bytes())

    def test_known_receipt_retry_checks_parent_again_before_transaction_commit(self):
        with tempfile.TemporaryDirectory(prefix='p023a-receipt-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();root=world.acquire_coordinator_execution('root',ttl_seconds=1);world.control('resume',coordinator=root);person=state['persons'][0]['id'];actor=world.acquire_actor_execution(person,'child',coordinator=root)
            with world._coordinator_scope(root):world.submit(person,'receipt',{'action':'wait'},execution=actor)
            guard=world._check_actor_execution
            def expire(*a,**k):
                guard(*a,**k);now[0]+=1000000000
            world._check_actor_execution=expire
            try:
                with world._coordinator_scope(root):
                    with self.assertRaises(WorldError) as error:world.submit(person,'receipt',{'action':'wait'},execution=actor)
                self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_EXPIRED')
            finally:world._check_actor_execution=guard
            self.assertIsNotNone(world.receipt(person,'receipt'))

    def test_captured_actor_cannot_drop_child_authority_for_root_only_pause(self):
        from app.actor_execution import Attempt,ExecutionPolicy
        with tempfile.TemporaryDirectory(prefix='p023a-dropchild-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();root=world.acquire_coordinator_execution('root');world.control('resume',coordinator=root);person=state['persons'][0]['id'];old=world.acquire_actor_execution(person,'child',coordinator=root)
            attempt=Attempt(old,ExecutionPolicy(),coordinator=root);world.release_actor_execution(old);new=world.acquire_actor_execution(person,'newchild',coordinator=root)
            with world._actor_attempt(attempt):
                with self.assertRaises(WorldError) as error:world.technical_pause('LATE')
            self.assertEqual(error.exception.code,'ACTOR_EXECUTION_FENCED');self.assertEqual(world.snapshot()['status'],'running')
            world.technical_pause('ROOT_VALID',coordinator=root)

    def test_private_bootstrap_cannot_drop_captured_actor_authority(self):
        from app.actor_execution import Attempt,ExecutionPolicy
        from app.actors import ActorHost
        with tempfile.TemporaryDirectory(prefix='p023a-initgate-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');state=world.initialize();host=ActorHost(world,Path(temp)/'actor.sqlite');root=world.acquire_coordinator_execution('root');person=state['persons'][0]['id'];old=world.acquire_actor_execution(person,'child',coordinator=root)
            attempt=Attempt(old,ExecutionPolicy(),coordinator=root);world.release_actor_execution(old);world.acquire_actor_execution(person,'newchild',coordinator=root)
            try:
                with world._actor_attempt(attempt):
                    with self.assertRaises(WorldError) as error:
                        with host._private(initialize=True) as db:db.execute('INSERT INTO actor.actor_sessions(person_id) VALUES (?)',(person,))
                self.assertEqual(error.exception.code,'ACTOR_EXECUTION_FENCED')
            finally:host.close()

    def test_close_during_root_acquisition_cannot_start_maintenance_or_resurrect_owner(self):
        from app.server import Coordinator
        from contextlib import closing
        import sqlite3
        with tempfile.TemporaryDirectory(prefix='p023a-closeclaim-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');world.initialize();owner=Coordinator(world,None);acquire=world.acquire_coordinator_execution
            def closing_acquire(*a,**k):
                result=acquire(*a,**k);owner._closed.set();return result
            world.acquire_coordinator_execution=closing_acquire
            try:
                with self.assertRaises(WorldError) as error:owner.acquire_owner()
                self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_CANCELLED')
                self.assertIsNone(owner._owner_attempt)
                with closing(sqlite3.connect(world.db_path)) as db:self.assertIsNone(db.execute('SELECT owner_token FROM coordinator_execution_leases').fetchone()[0])
            finally:owner.close()

    def test_late_dm_provider_exception_is_not_replaced_by_failure_pause_authority_error(self):
        from app.adjudication import Adjudicator
        with tempfile.TemporaryDirectory(prefix='p023a-dmerror-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();adj=Adjudicator(world);root=world.acquire_coordinator_execution('old',ttl_seconds=1);world.control('resume',coordinator=root);person=state['persons'][0]['id'];actor=world.acquire_actor_execution(person,'child',coordinator=root)
            original=RuntimeError('original-provider-error')
            def provider(payload):
                now[0]+=1000000000;world.acquire_coordinator_execution('new');raise original
            with world._coordinator_scope(root):
                with self.assertRaises(Exception) as error:adj.resolve(person,'failed','原文',provider,execution=actor)
            self.assertIs(error.exception,original);self.assertEqual(world.snapshot()['status'],'running')


if __name__=='__main__':unittest.main()
