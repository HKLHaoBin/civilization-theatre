"""Real durable Host controls for B; old tests remain frozen."""
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from app.world import WorldStore
from app.actors import ActorHost
from app.server import Coordinator
from app.actor_execution import (EntryBudget, ActorRunError, Attempt, DispatchExit,
    dispatch_capture_scope, _dispatch_scope, _dispatch_read_scope)
from app.coordinator_scheduler import ActivationRef, prove_dispatch_ack
from dataclasses import replace
from app.tests import test_actor_execution as request_fixtures

SCRATCH = 'C:/Users/We3q/AppData/Local/hermes/cache/scratch'

def logical(path):
    with closing(sqlite3.connect(Path(path).as_uri()+'?mode=ro',uri=True)) as db:
        return tuple(db.iterdump())

@contextmanager
def fixture():
    with tempfile.TemporaryDirectory(prefix='p023b4-B-continued-',dir=SCRATCH) as directory:
        path=Path(directory); world=WorldStore(path/'world.sqlite');world.initialize()
        host=ActorHost(world,path/'actors.sqlite');requests=[]
        def request(payload):
            requests.append(payload['intent_id'])
            return request_fixtures.ActorExecutionTests.result(payload)
        host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
        owner=Coordinator(world,host)
        try:
            yield world,host,owner,requests
        finally:
            owner.close()
            assert not owner.cleanup_pending and not host.cleanup_pending

@contextmanager
def prepared():
    with fixture() as (world,host,owner,requests):
        owner.acquire_owner();root=owner._owner_attempt
        with owner._owner_scope(root):
            owner._prepare_scheduler(root)
            world.control('resume')
            state=world.snapshot()
            window=world.open_or_recover_window(coordinator=root.permit,
                expected_version={k:state[k] for k in ('world_id','revision')})
            row=world.next_activation(window['window_id'],coordinator=root.permit)
            ref=ActivationRef(row['world_id'],row['window_id'],row['activation_id'],row['person_id'])
            yield world,host,owner,requests,root,ref,row['roster_index']

class ContinuedDispatchTests(unittest.TestCase):
    def test_actual_host_origin_dispositions_and_terminal_rejection_characterization(self):
        from app.world import WorldError
        cases=(('normal',DispatchExit.RETURNED,True),
            ('reconciled',DispatchExit.RETURNED_RECONCILED,True),
            ('rejected',DispatchExit.RETURNED,True),
            ('proof_failed',DispatchExit.PROOF_FAILED,False),
            ('ordinary_nonterminal',DispatchExit.HOST_NONTERMINAL_ERROR,False),
            ('authority',DispatchExit.AUTHORITY_STOP,False),
            ('control_flow',DispatchExit.CONTROL_FLOW,False),
            ('false',DispatchExit.UNCLASSIFIED_ERROR,False),
            ('rejected_open_cancel',DispatchExit.AUTHORITY_STOP,False))
        for mode,disposition,safe in cases:
            with self.subTest(mode=mode),prepared() as (world,host,owner,requests,root,ref,index):
                original_error=KeyboardInterrupt('control original') if mode=='control_flow' else RuntimeError('original private text must not leak')
                if mode in ('reconciled','proof_failed','false'):
                    finish=host._finish
                    def after_finish(*a,**k):
                        finish(*a,**k);raise original_error
                    host._finish=after_finish
                if mode=='proof_failed':
                    def bad_proof(*a,**k):raise ValueError('new proof error must not mask original')
                    host._terminal_ack_result=bad_proof
                if mode in ('rejected','rejected_open_cancel'):
                    location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                    host._choose=lambda view,seen:({'action':'travel','to':location},seen)
                    if mode=='rejected_open_cancel':
                        def before_close(*a,**k):
                            host._attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'));raise original_error
                        host._reject_result=before_close
                if mode in ('ordinary_nonterminal','authority','control_flow'):
                    if mode=='authority':original_error=ActorRunError('ACTOR_EXECUTION_CANCELLED')
                    def fault(payload):raise original_error
                    host._request=fault
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    if safe:
                        result=host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    else:
                        with self.assertRaises(type(original_error)) as caught:
                            host.act(ref.person_id,pause_on_failure=mode!='false',coordinator=root.permit,activation=ref)
                        self.assertIs(caught.exception,original_error)
                    cap=slot.capture();self.assertEqual(cap.exit_disposition,disposition)
                    before=(logical(world.db_path),logical(host.db_path))
                    if safe:
                        proof=prove_dispatch_ack(world,slot=slot,capture=cap)
                        self.assertTrue(proof.terminal);self.assertEqual(proof.intent_id,result['intent_id'])
                    else:
                        with self.assertRaises(Exception):prove_dispatch_ack(world,slot=slot,capture=cap)
                    self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                    with closing(sqlite3.connect(world.db_path)) as db:
                        row=db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                    if mode=='ordinary_nonterminal':
                        self.assertEqual(row,('technical_blocked',0));self.assertEqual(world.snapshot()['status'],'technical_paused')
                    elif mode=='rejected_open_cancel':self.assertEqual(row,('rejected',0))
                    elif mode=='rejected':self.assertEqual(row,('rejected',1))
                    elif mode in ('normal','reconciled','proof_failed','false'):self.assertEqual(row,('completed',1))
                    if mode!='ordinary_nonterminal':self.assertEqual(world.snapshot()['status'],'running')


    def test_exact_original_ref_native_material_and_history_corruption_characterization(self):
        cases=(
            ('main','UPDATE coordinator_activations SET roster_index=99 WHERE activation_id=?'),
            ('main','UPDATE coordinator_activations SET owner_parent_epoch=owner_parent_epoch+1 WHERE activation_id=?'),
            ('main','UPDATE coordinator_activations SET owner_actor_epoch=owner_actor_epoch+1 WHERE activation_id=?'),
            ('main','DELETE FROM actor_activation_delegations WHERE activation_id=?'),
            ('main','DELETE FROM actor_coordinator_delegations WHERE person_id=?'),
            ('private','DELETE FROM actor_session_heads WHERE person_id=?'),
            ('private','DELETE FROM actor_calls WHERE person_id=?'),
            ('private',"UPDATE actor_intents SET candidate='[]' WHERE person_id=?"),
        )
        for target,sql in cases:
            with self.subTest(sql=sql),prepared() as (world,host,owner,requests,root,ref,index):
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture();self.assertTrue(prove_dispatch_ack(world,slot=slot,capture=cap).terminal)
                    with closing(sqlite3.connect(world.db_path if target=='main' else host.db_path)) as db:
                        db.execute(sql,(ref.activation_id if 'activation_id=?' in sql else ref.person_id,));db.commit()
                    before=(logical(world.db_path),logical(host.db_path));raw=(Path(world.db_path).read_bytes(),Path(host.db_path).read_bytes())
                    with self.assertRaises(Exception):prove_dispatch_ack(world,slot=slot,capture=cap)
                    self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                    self.assertEqual((Path(world.db_path).read_bytes(),Path(host.db_path).read_bytes()),raw)
                    self.assertEqual(len(requests),1)

    def test_proof_never_reenters_released_actor_and_samefile_live_host_characterization(self):
        with prepared() as (world,host,owner,requests,root,ref,index):
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();other=ActorHost(world,host.db_path)
                forbidden=[]
                def deny(*a,**k):forbidden.append(True);raise AssertionError('released actor must not be used')
                world._check_actor_execution=deny;world.acquire_actor_execution=deny;world.renew_actor_execution=deny
                host._storage.transaction=deny;host.act=deny
                before=(logical(world.db_path),logical(host.db_path))
                try:
                    proof=prove_dispatch_ack(world,slot=slot,capture=cap)
                    self.assertTrue(proof.terminal);self.assertEqual(proof.reference,ref)
                    self.assertEqual(forbidden,[])
                    self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                finally:other.close()

    def test_source_conflict_and_live_closing_at_final_proof_gate_characterization(self):
        from app import coordinator_scheduler as scheduler
        from unittest.mock import patch
        for mode in ('conflict','path','host_closing','coordinator_closing','child_cancel','root_cancel'):
            with self.subTest(mode=mode),prepared() as (world,host,owner,requests,root,ref,index):
                others=[]
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture();actual=scheduler.acquisition_proof;hits=[]
                    def interleave(*a,**k):
                        value=actual(*a,**k);hits.append(True)
                        if mode=='conflict':others.append(ActorHost(world,Path(host.db_path).with_name('conflicting.sqlite')))
                        elif mode=='path':host.db_path=Path(host.db_path).with_name('replaced.sqlite')
                        elif mode=='host_closing':host._closing.set()
                        elif mode=='coordinator_closing':owner._closed.set()
                        elif mode=='child_cancel':cap._attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'))
                        elif mode=='root_cancel':root.stop.set()
                        return value
                    original_path=host.db_path;before=(logical(world.db_path),logical(original_path))
                    try:
                        with patch.object(scheduler,'acquisition_proof',interleave):
                            with self.assertRaises(Exception):prove_dispatch_ack(world,slot=slot,capture=cap)
                        self.assertEqual(hits,[True]);self.assertEqual(len(requests),1)
                        self.assertEqual((logical(world.db_path),logical(original_path)),before)
                    finally:
                        host.db_path=original_path
                        for other in others:other.close()


    def test_original_outer_budget_expires_with_live_119s_child_characterization(self):
        import time
        from app.actor_execution import ExecutionPolicy
        with prepared() as (world,host,owner,requests,root,ref,index):
            outer=EntryBudget(ExecutionPolicy(act_deadline_seconds=.8,busy_budget_seconds=.7),threading.Event())
            nested=EntryBudget(host.execution_policy,threading.Event(),(outer,))
            with world._entry_budget(nested):
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture()
                    self.assertEqual(cap.deadline,min(cap._attempt.deadline,outer.deadline,nested.deadline))
                    self.assertEqual(cap.busy_deadline,outer.busy_deadline)
                    self.assertIs(cap._outer_entry,nested)
                    before=(logical(world.db_path),logical(host.db_path))
                    threading.Event().wait(max(0,outer.deadline-time.monotonic())+.01)
                    self.assertGreater(cap._attempt.deadline-time.monotonic(),118)
                    failure=cap._attempt.failure
                    with self.assertRaises(ActorRunError):prove_dispatch_ack(world,slot=slot,capture=cap)
                    self.assertIs(cap._attempt.failure,failure)
                    self.assertIs(world._scoped_entry_budget(),nested)
                    self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
            self.assertIsNone(world._scoped_entry_budget());self.assertIs(world._scoped_coordinator_attempt(),root)

    def test_main_and_private_native_backoff_use_original_remainder_characterization(self):
        import time
        from app.actor_execution import ExecutionPolicy, _DispatchWait
        from app.world import WorldError
        from unittest.mock import patch
        for target in ('main','private'):
            with self.subTest(target=target),prepared() as (world,host,owner,requests,root,ref,index):
                outer=EntryBudget(ExecutionPolicy(act_deadline_seconds=1.5,busy_budget_seconds=1.2),threading.Event())
                with world._entry_budget(outer),dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture();waits=[];original_wait=_DispatchWait.wait
                    threading.Event().wait(max(0,outer.busy_deadline-time.monotonic()-.45))
                    def wait(adapter,seconds):
                        waits.append((seconds,outer.busy_deadline-time.monotonic()))
                        return original_wait(adapter,seconds)
                    before=(logical(world.db_path),logical(host.db_path))
                    with closing(sqlite3.connect(world.db_path if target=='main' else host.db_path,timeout=0)) as lock:
                        lock.execute('BEGIN EXCLUSIVE')
                        try:
                            with patch.object(_DispatchWait,'wait',wait):
                                with self.assertRaises(WorldError) as caught:prove_dispatch_ack(world,slot=slot,capture=cap)
                        finally:lock.rollback()
                    self.assertTrue(waits,(target,getattr(caught.exception,'code',None),str(caught.exception),outer.busy_deadline-time.monotonic()))
                    self.assertTrue(all(0<=seconds<=max(0,remaining)+.005 for seconds,remaining in waits),waits)
                    self.assertIs(world._scoped_entry_budget(),outer)
                    self.assertIs(world._scoped_coordinator_attempt(),root)
                    self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                    self.assertIsNone(cap._attempt.failure)

    def test_read_adapter_has_no_authority_and_restores_scopes_characterization(self):
        import time
        with prepared() as (world,host,owner,requests,root,ref,index):
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();cap.check();self.assertTrue(cap._attempt.stop.is_set())
                before=(logical(world.db_path),logical(host.db_path));t=time.monotonic()
                with self.assertRaisesRegex(RuntimeError,'controlled read body'):
                    with _dispatch_read_scope(world,slot,cap):
                        adapter=world._scoped_entry_budget()
                        self.assertEqual(adapter.deadline,cap.deadline)
                        self.assertEqual(adapter.busy_deadline,cap.busy_deadline)
                        self.assertTrue(all(not hasattr(adapter,n) for n in ('permit','renew','call','finish','claim')))
                        self.assertFalse(adapter.stop.wait(.04))
                        self.assertGreaterEqual(time.monotonic()-t,.03)
                        raise RuntimeError('controlled read body')
                self.assertIsNone(world._scoped_entry_budget());self.assertIsNone(world._scoped_attempt())
                self.assertIs(world._scoped_coordinator_attempt(),root)
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)


    def test_finish_join_observes_actual_inflight_maintenance_failure_characterization(self):
        from app.actor_execution import ExecutionPolicy
        from unittest.mock import patch
        with prepared() as (world,host,owner,requests,root,ref,index):
            host.execution_policy=ExecutionPolicy(renew_interval_seconds=.02)
            entered=threading.Event();unblock=threading.Event();failure=RuntimeError('controlled maintenance failure');finished=[]
            original_request=host._request
            def renewing(*a,**k):
                entered.set()
                if not unblock.wait(3):raise AssertionError('maintenance barrier not released')
                raise failure
            def request(payload):
                self.assertTrue(entered.wait(3))
                return original_request(payload)
            host._request=request;world.renew_actor_execution=renewing
            original_finish=Attempt.finish
            def finish(attempt):
                def after_stop():
                    if attempt.stop.wait(3):unblock.set()
                helper=threading.Thread(target=after_stop,name='owned-finish-maintenance-interleave');helper.start()
                try:return original_finish(attempt)
                finally:helper.join(3);finished.append(not helper.is_alive())
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                with patch.object(Attempt,'finish',finish):
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture()
                self.assertIsNone(cap.failure_before_finish)
                self.assertFalse(cap.stop_before_finish);self.assertTrue(cap.maintenance_joined)
                self.assertIs(cap._attempt.failure,failure)
                self.assertGreater(cap._attempt._cancel_generation,cap._cancel_generation)
                with self.assertRaises(RuntimeError) as caught:cap.check()
                self.assertIs(caught.exception,failure)
                before=(logical(world.db_path),logical(host.db_path))
                with self.assertRaises(RuntimeError):prove_dispatch_ack(world,slot=slot,capture=cap)
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
            self.assertEqual(finished,[True]);self.assertEqual(len(requests),1)

    def test_real_double_release_cas_false_never_becomes_safe_after_close_characterization(self):
        with prepared() as (world,host,owner,requests,root,ref,index):
            release=world.release_actor_execution;results=[]
            def twice(permit):
                results.append(release(permit));results.append(release(permit))
                return results[-1]
            world.release_actor_execution=twice
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();self.assertEqual(results,[True,False])
                self.assertFalse(cap.lease_released);self.assertTrue(cap.cleanup_pending)
                self.assertTrue(host.cleanup_pending)
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute('SELECT owner_token FROM actor_execution_leases WHERE person_id=?',(ref.person_id,)).fetchone(),(None,))
                before=(logical(world.db_path),logical(host.db_path))
                with self.assertRaises(ActorRunError):prove_dispatch_ack(world,slot=slot,capture=cap)
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                host.close()
                self.assertFalse(host.cleanup_pending)
                self.assertEqual(results,[True,False,False,False])
                with self.assertRaises(ActorRunError):cap.check()
                self.assertFalse(cap.lease_released);self.assertTrue(cap.cleanup_pending)
                with self.assertRaises(ActorRunError):prove_dispatch_ack(world,slot=slot,capture=cap)
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
            self.assertEqual(len(requests),1)

    def test_original_failure_cancel_generation_and_live_closing_characterization(self):
        for mode in ('before_finish','after_publish','host_closing','root_closing'):
            with self.subTest(mode=mode),prepared() as (world,host,owner,requests,root,ref,index):
                failure=ActorRunError('ACTOR_EXECUTION_CANCELLED');original=host._finish
                def finish(*a,**k):
                    result=original(*a,**k)
                    host._attempt.cancel(failure)
                    host._attempt.cancel(RuntimeError('later ordinary failure must not replace original'))
                    return result
                if mode=='before_finish':host._finish=finish
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture()
                    if mode=='after_publish':cap._attempt.cancel(failure)
                    elif mode=='host_closing':host._closing.set()
                    elif mode=='root_closing':owner._closed.set()
                    if mode=='before_finish':
                        self.assertIs(cap.failure_before_finish,failure);self.assertIs(cap._attempt.failure,failure)
                        self.assertTrue(cap.stop_before_finish)
                    with self.assertRaises(ActorRunError):cap.check()
                    self.assertEqual(len(requests),1)

    def test_scope_identity_single_assignment_and_closed_capture_characterization(self):
        with prepared() as (world,host,owner,requests,root,ref,index):
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                with self.assertRaises(ActorRunError):
                    with dispatch_capture_scope(world,host,ref,root,roster_index=index):pass
                result=host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();self.assertEqual(cap.intent_id,result['intent_id']);cap.check()
                before=(logical(world.db_path),logical(host.db_path));original_bytes=(Path(world.db_path).read_bytes(),Path(host.db_path).read_bytes())
                acquired=[];original=world.acquire_actor_execution
                def counted(*a,**k):acquired.append(True);return original(*a,**k)
                world.acquire_actor_execution=counted
                for operation in (
                    lambda:host.act(ref.person_id,coordinator=root.permit,activation=ref),
                    lambda:slot.bind(cap._attempt),
                    lambda:slot.publish(DispatchExit.RETURNED,cap.intent_id,maintenance_joined=True,providers_joined=True,lease_released=True),
                    lambda:slot.validate(replace(cap)),
                    lambda:prove_dispatch_ack(world,slot=slot,capture=replace(cap)),
                ):
                    with self.assertRaises(ActorRunError):operation()
                proof=prove_dispatch_ack(world,slot=slot,capture=cap)
                self.assertTrue(proof.terminal);self.assertEqual(proof.reference,ref)
                self.assertEqual(acquired,[]);self.assertEqual(len(requests),1)
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                self.assertEqual((Path(world.db_path).read_bytes(),Path(host.db_path).read_bytes()),original_bytes)
            self.assertIsNone(_dispatch_scope.get())
            for operation in (slot.capture,cap.check,lambda:slot.bind(cap._attempt),
                lambda:prove_dispatch_ack(world,slot=slot,capture=cap)):
                with self.assertRaises(ActorRunError):operation()

    def test_preentry_actor_and_root_object_replacements_characterization(self):
        from app.coordinator_execution import CoordinatorAttempt
        with prepared() as (world,host,owner,requests,root,ref,index):
            permit=world.acquire_actor_execution(ref.person_id,'controlled-outer',coordinator=root.permit,activation=ref)
            outer=Attempt(permit,host.execution_policy,coordinator=root.permit,parent_attempt=root,activation=ref)
            acquired=[];original=world.acquire_actor_execution
            def counted(*a,**k):acquired.append(True);return original(*a,**k)
            world.acquire_actor_execution=counted
            try:
                before=(logical(world.db_path),logical(host.db_path))
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    with world._actor_attempt(outer):
                        with self.assertRaises(ActorRunError):host.act(ref.person_id,coordinator=root.permit,activation=ref)
                    clone=CoordinatorAttempt(world,root.permit,owner.execution_policy)
                    from app.world import _coordinator_attempt_scope
                    token=_coordinator_attempt_scope.set((world,root.permit,clone))
                    try:
                        with self.assertRaises(ActorRunError):host.act(ref.person_id,coordinator=root.permit,activation=ref)
                    finally:_coordinator_attempt_scope.reset(token)
                    self.assertIsNone(slot.capture());self.assertFalse(slot.entered)
                self.assertEqual(acquired,[]);self.assertEqual(requests,[])
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
            finally:world.release_actor_execution(permit)


    def test_storage_replacement_before_host_is_rejected_before_acquire(self):
        with fixture() as (world,host,owner,requests):
            other=ActorHost(world,host.db_path)
            actual=host.act;acquire=world.acquire_actor_execution;acquisitions=[];observed=[];unchanged=[]
            def counted(*args,**kwargs):
                acquisitions.append(True)
                return acquire(*args,**kwargs)
            world.acquire_actor_execution=counted
            def caller(*args,**kwargs):
                before=(logical(world.db_path),logical(host.db_path));original=host._storage
                host._storage=other._storage
                try:
                    try:actual(*args,**kwargs)
                    except Exception as error:observed.append(getattr(error,'code',type(error).__name__))
                finally:host._storage=original
                unchanged.append((logical(world.db_path),logical(host.db_path))==before)
                raise RuntimeError('ordinary caller fault')
            host.act=caller
            try:
                owner.start();owner.worker.join(10)
                self.assertFalse(owner.worker.is_alive())
                self.assertEqual(acquisitions,[])
                self.assertEqual(requests,[])
                self.assertEqual(observed,['ACTIVATION_BINDING_CONFLICT'])
                self.assertEqual(unchanged,[True])
            finally:other.close()

    def test_entry_replacement_before_host_is_rejected_before_acquire(self):
        with fixture() as (world,host,owner,requests):
            actual=host.act;acquire=world.acquire_actor_execution;acquisitions=[];observed=[]
            def counted(*args,**kwargs):
                acquisitions.append(True)
                return acquire(*args,**kwargs)
            world.acquire_actor_execution=counted
            def caller(*args,**kwargs):
                before=(logical(world.db_path),logical(host.db_path))
                replacement=EntryBudget(host.execution_policy,threading.Event())
                with world._entry_budget(replacement):
                    try:
                        actual(*args,**kwargs)
                    except ActorRunError as error:
                        observed.append(error.code)
                self.assertEqual((logical(world.db_path),logical(host.db_path)),before)
                raise RuntimeError('original caller error remains local')
            host.act=caller
            owner.start();owner.worker.join(10)
            self.assertFalse(owner.worker.is_alive())
            self.assertEqual(acquisitions,[])
            self.assertEqual(requests,[])
            self.assertEqual(observed,['ACTIVATION_BINDING_CONFLICT'])
            self.assertEqual(owner.error,'HOST_FAILURE')
            self.assertEqual(world.snapshot()['status'],'running')
