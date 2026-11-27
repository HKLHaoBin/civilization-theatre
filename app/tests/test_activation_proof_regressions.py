"""R1 proof gates. Actual World/Host; synthetic request seam, no SDK."""
from contextlib import closing
from pathlib import Path
import json
import sqlite3
import unittest
from app.tests import test_actor_activations as fixtures
from app.coordinator_scheduler import ActivationRef

class ActivationProofRegressions(unittest.TestCase):
    registered = fixtures.ActorActivationTests.registered

    def test_acquisition_missing_head_rejects_completed_and_pending_history(self):
        for pending in (False, True):
            with self.subTest(pending=pending), self.registered() as (world,host,owner,ref,requests):
                host.act(ref.person_id, coordinator=owner, activation=ref)
                if pending:
                    # Finish the entire real window, then select same person in the next.
                    from app.tests.activation_fixtures import close_activations
                    with closing(sqlite3.connect(world.db_path)) as db:
                        window=dict(window_id=ref.window_id)
                    close_activations(world,host,owner,window)
                    world.complete_window(ref.window_id,coordinator=owner)
                    version={k:world.snapshot()[k] for k in ('world_id','revision')}
                    window=world.open_or_recover_window(coordinator=owner,expected_version=version)
                    row=world.next_activation(window['window_id'],coordinator=owner)
                    ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('DELETE FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).rowcount,1)
                    db.commit()
                before={p:Path(p).read_bytes() for p in (world.db_path,host.db_path)}
                permit=None
                try:
                    with self.assertRaises(Exception) as caught:
                        permit=world.acquire_actor_execution(ref.person_id,'r1-missing-head',coordinator=owner,activation=ref)
                    self.assertEqual(caught.exception.code,'ACTOR_RECOVERY_UNPROVEN')
                    self.assertEqual({p:Path(p).read_bytes() for p in before},before)
                finally:
                    if permit is not None:world.release_actor_execution(permit)

    def configure_effect(self, world, host, owner, mode):
        if mode == 'adj':
            from app.adjudication import Adjudicator
            with world._coordinator_scope(owner):host.adjudicator=Adjudicator(world)
            host._choose=lambda view,seen:({'action':'propose','text':'r1 original'},seen)
            def dm(payload):
                state=payload['world']
                return dict(world_id=state['world_id'],base_revision=state['revision'],ruling='成功',reason='r1 seam',goal='investigate',costs=[],benefits=[],effects=[{'action':'investigate'}])
            host.dm_provider=dm
        else:host._choose=lambda view,seen:({'action':'investigate'},seen)

    def main_facts(self, world):
        with closing(sqlite3.connect(world.db_path.as_uri()+'?mode=ro',uri=True)) as db:
            tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            return {table:db.execute('SELECT * FROM '+table).fetchall() for table in
                    ('snapshots','intents','coordinator_activation_outcomes') if table in tables}

    def test_actual_effect_rechecks_complete_lineage(self):
        for mode in ('world','adj'):
            for fault in ('calls','consumed','base'):
                with self.subTest(mode=mode,fault=fault), self.registered() as (world,host,owner,ref,requests):
                    self.configure_effect(world,host,owner,mode)
                    mutate=world._mutate;observed={}
                    def interleave(operation, **kwargs):
                        # World durable prepare / ADJ proposal+decision are already real.
                        if operation.__name__=='apply' and not observed:
                            with closing(sqlite3.connect(host.db_path)) as db:
                                if fault=='calls':
                                    self.assertEqual(db.execute('DELETE FROM actor_calls').rowcount,1)
                                elif fault=='consumed':db.execute("UPDATE actor_intents SET consumed='[1]'")
                                else:db.execute("UPDATE actor_intent_execution SET base_seen_sha256='bad'")
                                db.commit()
                            observed['facts']=self.main_facts(world)
                            observed['snapshot']=world.snapshot()
                        return mutate(operation,**kwargs)
                    world._mutate=interleave
                    with self.assertRaises(Exception) as caught:
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                    self.assertEqual(caught.exception.code,'ACTOR_RECOVERY_UNPROVEN')
                    self.assertTrue(observed)
                    self.assertEqual(self.main_facts(world),observed['facts'])
                    self.assertEqual(world.snapshot(),observed['snapshot'])
                    with closing(sqlite3.connect(world.db_path)) as db:
                        self.assertEqual(db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()[0],'candidate')

    def test_full_candidate_and_synthetic_zero_work_remain_legal(self):
        for mode in ('world','adj'):
            for zero in (False,True):
                with self.subTest(mode=mode,zero=zero), self.registered() as (world,host,owner,ref,requests):
                    self.configure_effect(world,host,owner,mode)
                    if zero:
                        host._request=lambda payload:dict(messages=payload['messages'],calls=[],sdk_requests=0,sdk_events=0,provider='offline-test',pid=0,module_root='none')
                    receipt=host.act(ref.person_id,coordinator=owner,activation=ref)
                    self.assertEqual(receipt['receipt']['status'],'committed')
                    with closing(sqlite3.connect(world.db_path)) as db:
                        self.assertEqual(db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()[0],'completed')

    def test_final_effect_source_conflict_rolls_back_actual_transaction(self):
        from contextvars import Context
        from app.actors import ActorHost
        from app.world import _world_mutation_scope
        for mode in ('world','adj'):
            with self.subTest(mode=mode), self.registered() as (world,host,owner,ref,requests):
                self.configure_effect(world,host,owner,mode)
                actual=world._apply_action;others=[];before={}
                def interleave(*args,**kwargs):
                    scope=_world_mutation_scope.get()
                    if scope is not None and scope[0] is world and not others:
                        before['facts']=self.main_facts(world);before['snapshot']=world.snapshot()
                        result=actual(*args,**kwargs)
                        others.append(Context().run(ActorHost,world,Path(host.db_path).with_name('r1-conflict.sqlite')))
                        return result
                    return actual(*args,**kwargs)
                world._apply_action=interleave
                try:
                    with self.assertRaises(Exception) as caught:
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    self.assertEqual(len(others),1)
                    self.assertFalse(Path(others[0].db_path).exists())
                    self.assertEqual(self.main_facts(world),before['facts'])
                    self.assertEqual(world.snapshot(),before['snapshot'])
                    with closing(sqlite3.connect(world.db_path)) as db:
                        self.assertEqual(db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()[0],'candidate')
                finally:
                    world._apply_action=actual
                    for other in others:other.close()

    def test_final_private_source_conflict_preserves_world_commit_not_promotion(self):
        from contextvars import Context
        from app.actors import ActorHost
        for mode in ('world','adj'):
            with self.subTest(mode=mode), self.registered() as (world,host,owner,ref,requests):
                self.configure_effect(world,host,owner,mode)
                actual=host._storage.finish;others=[];before={}
                def interleave(*args,**kwargs):
                    before['facts']=self.main_facts(world)
                    with closing(sqlite3.connect(host.db_path)) as db:
                        before['private']={t:db.execute('SELECT * FROM '+t).fetchall() for t in
                                           ('actor_sessions','actor_session_heads','actor_intents','actor_intent_execution','actor_calls')}
                    result=actual(*args,**kwargs)
                    others.append(Context().run(ActorHost,world,Path(host.db_path).with_name('r1-private-conflict.sqlite')))
                    return result
                host._storage.finish=interleave
                try:
                    with self.assertRaises(Exception) as caught:
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    self.assertEqual(len(others),1)
                    self.assertFalse(Path(others[0].db_path).exists())
                    self.assertEqual(self.main_facts(world),before['facts'])
                    with closing(sqlite3.connect(host.db_path)) as db:
                        self.assertEqual({t:db.execute('SELECT * FROM '+t).fetchall() for t in before['private']},before['private'])
                    with closing(sqlite3.connect(world.db_path)) as db:
                        self.assertEqual(db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()[0],'world_committed')
                        self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone()[0],1 if mode=='world' else 2)
                finally:
                    host._storage.finish=actual
                    for other in others:other.close()

    def test_final_same_file_multiple_hosts_remain_legal(self):
        from contextvars import Context
        from app.actors import ActorHost
        for mode in ('world','adj'):
            with self.subTest(mode=mode), self.registered() as (world,host,owner,ref,requests):
                self.configure_effect(world,host,owner,mode)
                before={p:Path(p).read_bytes() for p in (world.db_path,host.db_path)}
                other=Context().run(ActorHost,world,host.db_path)
                try:
                    self.assertEqual({p:Path(p).read_bytes() for p in before},before)
                    result=host.act(ref.person_id,coordinator=owner,activation=ref)
                    self.assertEqual(result['receipt']['status'],'committed')
                    self.assertIsNone(other._attempt)
                finally:other.close()

    def test_known_receipt_apis_recheck_lineage_without_writes(self):
        from app.coordinator_scheduler import activation_scope
        for mode in ('world','adj'):
            with self.subTest(mode=mode), self.registered() as (world,host,owner,ref,requests):
                self.configure_effect(world,host,owner,mode)
                result=host.act(ref.person_id,coordinator=owner,activation=ref)
                permit=world.acquire_actor_execution(ref.person_id,'r1-receipt-proof',coordinator=owner,activation=ref)
                try:
                    with closing(sqlite3.connect(host.db_path)) as db:
                        self.assertEqual(db.execute('DELETE FROM actor_calls').rowcount,1);db.commit()
                    before={p:Path(p).read_bytes() for p in (world.db_path,host.db_path)}
                    intent=result['intent_id'];body=result['body']
                    if mode=='world':
                        callbacks=[lambda:world.submit(ref.person_id,intent,body,execution=permit,activation=ref)]
                    else:
                        adj=host.adjudicator;version=result['receipt']['decision_version']
                        decision=adj.history(ref.person_id,intent)[-1]['decision']
                        callbacks=[lambda:adj.propose(ref.person_id,intent,body['text'],execution=permit),
                                   lambda:adj.record(ref.person_id,intent,decision,execution=permit),
                                   lambda:adj.preview(ref.person_id,intent,version,execution=permit),
                                   lambda:adj.commit(ref.person_id,intent,version,execution=permit),
                                   lambda:adj.resolve(ref.person_id,intent,body['text'],lambda payload:self.fail('receipt replay must not request DM'),execution=permit),
                                   lambda:adj._execution_receipt(ref.person_id,intent,permit)]
                    with world._coordinator_scope(owner),activation_scope(world,ref):
                        for index,callback in enumerate(callbacks):
                            with self.subTest(api=index),self.assertRaises(Exception) as caught:callback()
                            self.assertEqual(caught.exception.code,'ACTOR_RECOVERY_UNPROVEN')
                            self.assertEqual({p:Path(p).read_bytes() for p in before},before)
                finally:world.release_actor_execution(permit)

    def test_provider_is_outside_source_lock_and_maintenance_does_not_wait_for_it(self):
        import threading
        from app.actor_execution import ExecutionPolicy
        with self.registered() as (world,host,owner,ref,requests):
            host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03)
            request=host._request;renew=world.renew_actor_execution
            config_held=threading.Event();release=threading.Event();renewed=threading.Event();threads=[]
            def maintain(*args,**kwargs):
                result=renew(*args,**kwargs)
                if config_held.is_set():renewed.set()
                return result
            world.renew_actor_execution=maintain
            def provider(payload):
                def config_owner():
                    with world._private_sources_lock:
                        config_held.set();release.wait(2)
                t=threading.Thread(target=config_owner,name='r1-config-control');threads.append(t);t.start()
                try:
                    self.assertTrue(config_held.wait(.5),'provider must not hold source lock')
                    self.assertTrue(renewed.wait(.5),'actual actor heartbeat must not wait for source lock')
                finally:release.set();t.join(2)
                return request(payload)
            host._request=provider
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref)
                self.assertEqual(result['receipt']['status'],'committed')
            finally:
                release.set()
                for thread in threads:thread.join(2)
                self.assertFalse(any(t.is_alive() for t in threads))

    def test_source_lock_precedes_actual_main_connections(self):
        from contextlib import contextmanager
        import threading
        from app.coordinator_scheduler import activation_scope
        with self.registered() as (world,host,owner,ref,requests):
            permit=world.acquire_actor_execution(ref.person_id,'r1-lock-order',coordinator=owner,activation=ref)
            try:
                for kind in ('acquire','mutate','private','adj-read'):
                    with self.subTest(kind=kind):
                        entered=threading.Event();started=threading.Event();errors=[]
                        target=host._storage if kind=='private' else world
                        attribute='_open' if kind=='private' else '_execution_connection' if kind=='acquire' else '_connection'
                        actual=getattr(target,attribute)
                        @contextmanager
                        def spy(*args,**kwargs):
                            entered.set()
                            with actual(*args,**kwargs) as db:yield db
                        def private_spy(*args,**kwargs):
                            entered.set()
                            return actual(*args,**kwargs)
                        setattr(target,attribute,private_spy if kind=='private' else spy)
                        def operation():
                            try:
                                started.set()
                                with world._coordinator_scope(owner),activation_scope(world,ref):
                                    if kind=='acquire':
                                        try:world.acquire_actor_execution(ref.person_id,'r1-busy',coordinator=owner,activation=ref)
                                        except Exception as error:
                                            if error.code!='ACTOR_EXECUTION_BUSY':raise
                                    elif kind=='mutate':world._mutate(lambda state,db:None,execution=permit,person_id=ref.person_id)
                                    elif kind=='private':
                                        with host._storage.transaction(permit,ref.person_id) as db:host._storage.adopt(db,ref.person_id,permit,'r1-lock')
                                    else:
                                        from app.adjudication import Adjudicator
                                        # Use the real read context, without constructor schema writes.
                                        adj=object.__new__(Adjudicator);adj.world=world
                                        with adj._read() as db:db.execute('SELECT count(*) FROM snapshots').fetchone()
                            except BaseException as error:errors.append(error)
                        with world._private_sources_lock:
                            thread=threading.Thread(target=operation,name='r1-lock-order');thread.start()
                            self.assertTrue(started.wait(1))
                            blocked=not entered.wait(.08)
                        thread.join(3)
                        setattr(target,attribute,actual)
                        self.assertFalse(thread.is_alive())
                        self.assertTrue(entered.is_set())
                        self.assertTrue(blocked,'must not open/acquire main before source lock')
                        self.assertFalse(errors,repr(errors))
            finally:world.release_actor_execution(permit)

    def test_source_lock_wait_preserves_original_attempt_deadline(self):
        import threading
        from app.actor_execution import Attempt,ExecutionPolicy
        from app.coordinator_scheduler import activation_scope
        with self.registered() as (world,host,owner,ref,requests):
            permit=world.acquire_actor_execution(ref.person_id,'r1-lock-deadline',coordinator=owner,activation=ref)
            try:
                for kind in ('world','private'):
                    with self.subTest(kind=kind):
                        held=threading.Event();release=threading.Event();done=threading.Event();errors=[]
                        attempt=Attempt(permit,ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.02,act_deadline_seconds=.12),coordinator=owner,activation=ref)
                        def holder():
                            with world._private_sources_lock:held.set();release.wait(2)
                        def writer():
                            try:
                                with world._actor_attempt(attempt):
                                    if kind=='world':world._mutate(lambda state,db:None,execution=permit,person_id=ref.person_id)
                                    else:
                                        with host._storage.transaction(permit,ref.person_id,attempt=attempt):pass
                            except BaseException as error:errors.append(error)
                            finally:done.set()
                        owner_thread=threading.Thread(target=holder,name='r1-deadline-holder');owner_thread.start()
                        self.assertTrue(held.wait(1))
                        worker=threading.Thread(target=writer,name='r1-deadline-writer');worker.start()
                        try:
                            timely=done.wait(.35)
                        finally:
                            release.set();owner_thread.join(2);worker.join(2)
                        self.assertFalse(worker.is_alive());self.assertFalse(owner_thread.is_alive())
                        self.assertTrue(timely,'source contention must not hide original attempt deadline until config release')
                        self.assertEqual([error.code for error in errors],['ACTOR_EXECUTION_DEADLINE'])
            finally:world.release_actor_execution(permit)

    def test_first_pending_without_head_and_completed_with_head_are_legal(self):
        with self.registered() as (world,host,owner,ref,requests):
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            permit=world.acquire_actor_execution(ref.person_id,'r1-positive',coordinator=owner,activation=ref)
            try:
                from app.coordinator_scheduler import activation_scope
                with world._coordinator_scope(owner), activation_scope(world,ref):
                    with host._storage.transaction(permit,ref.person_id) as db:
                        host._storage.adopt(db,ref.person_id,permit,'r1-control')
                        self.assertEqual(db.execute('SELECT intent_id FROM actor.actor_intents WHERE person_id=?',(ref.person_id,)).fetchone()[0],result['intent_id'])
            finally:world.release_actor_execution(permit)
