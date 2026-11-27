"""Additional real cleanup and final root-gate controls, no fake permits."""
from contextlib import closing
from pathlib import Path
import sqlite3,threading,unittest
from app.tests import test_dispatch_ack_continued as fixtures
from app.actor_execution import dispatch_capture_scope, ActorRunError
from app.coordinator_scheduler import prove_dispatch_ack

class ContinuedCleanupTests(unittest.TestCase):
    def test_independent_next_complete_and_control_faults_keep_legal_root_pause(self):
        for mode in ('next','complete','control'):
            with self.subTest(mode=mode),fixtures.fixture() as (world,host,owner,requests):
                def fault(*a,**k):raise RuntimeError('ordinary independent control failure')
                if mode=='next':world.next_activation=fault
                elif mode=='complete':world.complete_window=fault
                else:
                    control=world.control
                    def controlled(action,*a,**k):
                        if action=='pause':return fault()
                        return control(action,*a,**k)
                    world.control=controlled
                owner.start();owner.worker.join(20)
                self.assertFalse(owner.worker.is_alive());self.assertEqual(owner.error,'HOST_FAILURE')
                self.assertEqual(world.snapshot()['status'],'technical_paused')
                self.assertEqual(len(requests),0 if mode=='next' else 12)


    def test_preexisting_stop_without_failure_is_not_finish_only(self):
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            original=host._finish
            def stopping(*a,**k):
                result=original(*a,**k);host._attempt.stop.set();return result
            host._finish=stopping
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();self.assertTrue(cap.stop_before_finish);self.assertIsNone(cap.failure_before_finish)
                with self.assertRaises(ActorRunError):cap.check()
                self.assertEqual(len(requests),1)


    def test_connection_exit_and_server_continue_still_check_original_live_capture(self):
        from app import coordinator_scheduler as scheduler
        from contextlib import contextmanager
        from unittest.mock import patch
        from app.world import WorldError
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();actual=world._connection;exited=[]
                @contextmanager
                def exiting(*a,**k):
                    with actual(*a,**k) as db:yield db
                    exited.append(True);host._closing.set()
                before=(fixtures.logical(world.db_path),fixtures.logical(host.db_path))
                with patch.object(world,'_connection',exiting):
                    with self.assertRaises(ActorRunError):prove_dispatch_ack(world,slot=slot,capture=cap)
                self.assertEqual(exited,[True])
                self.assertEqual((fixtures.logical(world.db_path),fixtures.logical(host.db_path)),before)
        with fixtures.fixture() as (world,host,owner,requests):
            actual=host.act;original_proof=scheduler.prove_dispatch_ack;proved=[]
            def caller(*a,**k):actual(*a,**k);raise WorldError('ORIGINAL_POST_RETURN','caller text')
            def closing_after_proof(*a,**k):
                value=original_proof(*a,**k);proved.append(value);host._closing.set();return value
            host.act=caller
            with patch.object(scheduler,'prove_dispatch_ack',closing_after_proof):
                owner.start();owner.worker.join(10)
            self.assertFalse(owner.worker.is_alive());self.assertEqual(owner.error,'ORIGINAL_POST_RETURN')
            self.assertEqual(len(proved),1);self.assertEqual(len(requests),1)
            self.assertEqual(world.snapshot()['status'],'running')


    def test_actual_maintenance_join_timeout_publishes_incomplete_cleanup(self):
        import tempfile
        from app.actor_execution import ExecutionPolicy
        from app.coordinator_scheduler import ActivationRef
        with tempfile.TemporaryDirectory(prefix='p023b4-B-pending-maintenance-',dir=fixtures.SCRATCH) as directory:
            path=Path(directory);world=fixtures.WorldStore(path/'world.sqlite');world.initialize()
            host=fixtures.ActorHost(world,path/'actors.sqlite');owner=fixtures.Coordinator(world,host)
            entered=threading.Event();unblock=threading.Event();captured=[];original_release=world.release_actor_execution;releases=[]
            def release(permit):releases.append(True);return original_release(permit)
            world.release_actor_execution=release
            try:
                owner.acquire_owner();root=owner._owner_attempt
                with owner._owner_scope(root):
                    owner._prepare_scheduler(root);world.control('resume');state=world.snapshot()
                    window=world.open_or_recover_window(coordinator=root.permit,expected_version={k:state[k] for k in ('world_id','revision')})
                    row=world.next_activation(window['window_id'],coordinator=root.permit)
                    ref=ActivationRef(row['world_id'],row['window_id'],row['activation_id'],row['person_id'])
                    host.execution_policy=ExecutionPolicy(renew_interval_seconds=.02)
                    def renew(*a,**k):
                        entered.set()
                        if not unblock.wait(6):raise RuntimeError('maintenance test barrier timeout')
                        raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
                    world.renew_actor_execution=renew
                    def request(payload):
                        if not entered.wait(3):raise AssertionError('maintenance never entered')
                        return fixtures.request_fixtures.ActorExecutionTests.result(payload)
                    host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
                    with dispatch_capture_scope(world,host,ref,root,roster_index=row['roster_index']) as slot:
                        slot._bind_coordinator_closing(owner._closed)
                        host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                        cap=slot.capture();captured.append(cap)
                        self.assertFalse(cap.maintenance_joined);self.assertFalse(cap.lease_released)
                        self.assertTrue(cap.cleanup_pending);self.assertEqual(releases,[])
                        with self.assertRaises(ActorRunError):cap.check()
                        unblock.set();cap._attempt.thread.join(3);self.assertFalse(cap._attempt.thread.is_alive())
                        host.close()
                        with self.assertRaises(ActorRunError):cap.check()
                        self.assertTrue(cap.cleanup_pending);self.assertFalse(cap.maintenance_joined)
                        self.assertFalse(cap.lease_released)
                        self.assertFalse(host.cleanup_pending);self.assertEqual(releases,[True])
                        with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                            self.assertEqual(db.execute('SELECT owner_token FROM actor_execution_leases WHERE person_id=?',(ref.person_id,)).fetchone(),(None,))
            finally:
                unblock.set()
                for cap in captured:
                    cap._attempt.thread.join(3);self.assertFalse(cap._attempt.thread.is_alive())
                    original_release(cap._actor_identity)
                owner.close()
                # Current cleanup completes; the original unsafe capture stays unsafe.
                self.assertFalse(host.cleanup_pending)
                for cap in captured:
                    self.assertTrue(cap.cleanup_pending);self.assertFalse(cap.maintenance_joined)
                    self.assertFalse(cap.lease_released)
                    with self.assertRaises(ActorRunError):cap.check()
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute('SELECT owner_token FROM coordinator_execution_leases').fetchone(),(None,))


    def test_scope_wrong_host_reference_and_reused_capture_are_preexecution_denials(self):
        from dataclasses import replace
        from app.coordinator_scheduler import ActivationRef
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            other=fixtures.ActorHost(world,host.db_path);acquired=[];acquire=world.acquire_actor_execution
            def counted(*a,**k):acquired.append(True);return acquire(*a,**k)
            world.acquire_actor_execution=counted
            try:
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    before=(fixtures.logical(world.db_path),fixtures.logical(host.db_path))
                    for target,person,reference in (
                        (other,ref.person_id,ref),
                        (host,'person-country-1-finance',ref),
                        (host,ref.person_id,replace(ref,activation_id='0'*32)),
                        (host,ref.person_id,replace(ref,world_id='0'*32)),
                    ):
                        with self.assertRaises(Exception):target.act(person,coordinator=root.permit,activation=reference)
                    self.assertEqual(acquired,[]);self.assertEqual(requests,[])
                    self.assertFalse(slot.entered)
                    self.assertEqual((fixtures.logical(world.db_path),fixtures.logical(host.db_path)),before)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    old=slot.capture();old.check()
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as next_slot:
                    next_slot._bind_coordinator_closing(owner._closed)
                    self.assertIsNone(next_slot.capture())
                    before=(fixtures.logical(world.db_path),fixtures.logical(host.db_path))
                    with self.assertRaises(ActorRunError):prove_dispatch_ack(world,slot=next_slot,capture=old)
                    self.assertEqual((fixtures.logical(world.db_path),fixtures.logical(host.db_path)),before)
                self.assertEqual(len(acquired),1);self.assertEqual(len(requests),1)
            finally:other.close()

    def test_actual_source_conflict_after_exact_material_copy_hits_final_gate(self):
        from app import coordinator_scheduler as scheduler
        from unittest.mock import patch
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();actual=scheduler.acquisition_proof;others=[];hits=[]
                def interleave(*a,**k):
                    result=actual(*a,**k)
                    if a[-1] is ref:
                        hits.append(True);others.append(fixtures.ActorHost(world,Path(host.db_path).with_name('final-conflict.sqlite')))
                    return result
                before=(fixtures.logical(world.db_path),fixtures.logical(host.db_path))
                try:
                    with patch.object(scheduler,'acquisition_proof',interleave):
                        with self.assertRaises(Exception):prove_dispatch_ack(world,slot=slot,capture=cap)
                    self.assertEqual(hits,[True]);self.assertEqual(len(requests),1)
                    self.assertEqual((fixtures.logical(world.db_path),fixtures.logical(host.db_path)),before)
                finally:
                    for other in others:other.close()

    def test_caller_error_survives_new_proof_error_and_missing_capture(self):
        from app import coordinator_scheduler as scheduler
        from app.world import WorldError
        from unittest.mock import patch
        for mode in ('proof_error','missing_capture'):
            with self.subTest(mode=mode),fixtures.fixture() as (world,host,owner,requests):
                actual=host.act;proofs=[];fault=WorldError('ORIGINAL_CALLER_FAILURE','private caller text')
                def caller(*a,**k):
                    if mode=='proof_error':actual(*a,**k)
                    raise fault
                def broken_proof(*a,**k):proofs.append(True);raise WorldError('NEW_PROOF_FAILURE','not original')
                host.act=caller
                with patch.object(scheduler,'prove_dispatch_ack',broken_proof):
                    owner.start();owner.worker.join(10)
                self.assertFalse(owner.worker.is_alive());self.assertEqual(owner.error,'ORIGINAL_CALLER_FAILURE')
                self.assertEqual(world.snapshot()['status'],'running')
                self.assertEqual(len(requests),1 if mode=='proof_error' else 0)
                self.assertEqual(proofs,[True] if mode=='proof_error' else [])
                import json
                text=json.dumps(owner.snapshot())
                self.assertNotIn('private caller text',text);self.assertNotIn('NEW_PROOF_FAILURE',text)


    def test_actual_pending_provider_cannot_be_upgraded_by_later_cleanup(self):
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            entered=threading.Event();release=threading.Event();helpers=[];errors=[];original=host._finish
            def lingering():
                entered.set();release.wait(4)
            def finish(*a,**k):
                result=original(*a,**k);attempt=host._attempt
                def call():
                    try:attempt.call(host,lingering)
                    except BaseException as error:errors.append(error)
                helper=threading.Thread(target=call,name='owned-extra-provider-invocation');helpers.append(helper);helper.start()
                if not entered.wait(3):raise AssertionError('provider did not enter')
                return result
            host._finish=finish
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                try:
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture()
                    self.assertTrue(cap.cleanup_pending);self.assertFalse(cap.providers_joined)
                    self.assertFalse(cap.lease_released);self.assertTrue(cap.maintenance_joined)
                    self.assertTrue(any(t.is_alive() for t in cap._attempt.calls))
                    with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                        self.assertEqual(db.execute('SELECT owner_token FROM actor_execution_leases WHERE person_id=?',(ref.person_id,)).fetchone(),(cap._actor_identity.owner_token,))
                    with self.assertRaises(ActorRunError):cap.check()
                    release.set()
                    for helper in helpers:helper.join(3);self.assertFalse(helper.is_alive())
                    self.assertFalse(errors)
                    private_before=fixtures.logical(host.db_path);world_before=world.snapshot()
                    host.close();host.close();self.assertFalse(host.cleanup_pending)
                    self.assertEqual(fixtures.logical(host.db_path),private_before)
                    self.assertEqual(world.snapshot(),world_before)
                    with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                        self.assertEqual(db.execute('SELECT owner_token FROM actor_execution_leases WHERE person_id=?',(ref.person_id,)).fetchone(),(None,))
                        self.assertEqual(db.execute('SELECT owner_token FROM coordinator_execution_leases').fetchone(),(root.permit.owner_token,))
                    with self.assertRaises(ActorRunError):cap.check()
                    self.assertTrue(cap.cleanup_pending);self.assertFalse(cap.providers_joined)
                    self.assertFalse(cap.lease_released)
                finally:
                    release.set()
                    for helper in helpers:helper.join(3)
            self.assertEqual(len(requests),1)

    def test_original_root_sql_lease_and_clock_are_readonly_final_gates(self):
        from app import coordinator_scheduler as scheduler
        from unittest.mock import patch
        from app.world import WorldError
        for mode in ('released','rollback','invalid','final_rollback'):
            with self.subTest(mode=mode),fixtures.prepared() as (world,host,owner,requests,root,ref,index):
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture();clock=world._execution_clock_ns;hits=[];actual=scheduler.acquisition_proof
                    def interleave(*a,**k):
                        result=actual(*a,**k)
                        if a[-1] is ref:
                            world._execution_clock_ns=lambda:1;hits.append(True)
                        return result
                    if mode=='released':self.assertTrue(world.release_coordinator_execution(root.permit))
                    elif mode=='rollback':world._execution_clock_ns=lambda:1
                    elif mode=='invalid':world._execution_clock_ns=lambda:True
                    before=(fixtures.logical(world.db_path),fixtures.logical(host.db_path))
                    raw=(Path(world.db_path).read_bytes(),Path(host.db_path).read_bytes())
                    try:
                        with patch.object(scheduler,'acquisition_proof',interleave if mode=='final_rollback' else actual):
                            with self.assertRaises(WorldError) as caught:prove_dispatch_ack(world,slot=slot,capture=cap)
                        self.assertEqual(caught.exception.code,{'released':'COORDINATOR_EXECUTION_FENCED','rollback':'EXECUTION_CLOCK_UNSAFE','invalid':'EXECUTION_CLOCK_UNSAFE','final_rollback':'EXECUTION_CLOCK_UNSAFE'}[mode])
                        if mode=='final_rollback':self.assertEqual(hits,[True])
                        self.assertEqual((fixtures.logical(world.db_path),fixtures.logical(host.db_path)),before)
                        self.assertEqual((Path(world.db_path).read_bytes(),Path(host.db_path).read_bytes()),raw)
                        self.assertEqual(len(requests),1)
                    finally:world._execution_clock_ns=clock
