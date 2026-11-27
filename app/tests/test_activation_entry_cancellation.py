"""Real Host pre-permit entry lifecycle; scripted request seam, no SDK claims."""
from contextlib import closing
from pathlib import Path
import sqlite3
import threading
import time
import unittest
from app.tests import test_actor_activations as fixtures
from app.actor_execution import ExecutionPolicy


class ActivationEntryCancellationTests(unittest.TestCase):
    registered = fixtures.ActorActivationTests.registered

    @staticmethod
    def baseline(world,host):
        # Read actual schema/data through fresh read-only handles, not assertions
        # about method calls. Independent clock high-water is not business data.
        result={}
        for path in (world.db_path,host.db_path):
            with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                result[str(path)]=[line for line in db.iterdump() if 'execution_clock' not in line]
        return result

    def test_actual_close_cancels_before_permit_while_source_still_held(self):
        with self.registered() as (world,host,owner,ref,requests):
            host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=.1,act_deadline_seconds=.12)
            before=self.baseline(world,host)
            entered=threading.Event();done=threading.Event();closed=threading.Event();errors=[]
            acquire=world.acquire_actor_execution
            def observed(*args,**kwargs):
                entered.set();return acquire(*args,**kwargs)
            world.acquire_actor_execution=observed
            def actor():
                try:host.act(ref.person_id,coordinator=owner,activation=ref)
                except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
                finally:done.set()
            def closer():
                try:host.close()
                finally:closed.set()
            a=threading.Thread(target=actor,name='entry-cancel-actor')
            c=threading.Thread(target=closer,name='entry-cancel-close')
            try:
                with world._private_sources_lock:
                    a.start();self.assertTrue(entered.wait(1))
                    self.assertIsNone(host._attempt,'no fake Attempt before the real permit')
                    c.start()
                    timely=closed.wait(.6)
                    act_timely=done.is_set()
                    untouched=self.baseline(world,host)==before
            finally:
                a.join(3)
                if c.ident is not None:c.join(3)
            self.assertFalse(a.is_alive());self.assertFalse(c.is_alive())
            self.assertTrue(timely,'close must complete without releasing the external source lock')
            self.assertTrue(act_timely)
            self.assertEqual(errors,['ACTOR_EXECUTION_CANCELLED'])
            self.assertTrue(untouched)
            self.assertEqual(self.baseline(world,host),before)
            self.assertEqual(requests,[])
            self.assertIsNone(world._scoped_attempt())
            self.assertIsNone(world._scoped_coordinator_attempt())

    def test_host_entry_contention_is_bounded_by_busy_or_deadline_without_close(self):
        for busy,deadline,expected in ((.08,2,'EXECUTION_STORAGE_BUSY'),(1,.08,'ACTOR_EXECUTION_DEADLINE')):
            with self.subTest(busy=busy,deadline=deadline),self.registered() as (world,host,owner,ref,requests):
                host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=busy,act_deadline_seconds=deadline)
                before=self.baseline(world,host);started=threading.Event();done=threading.Event();errors=[]
                def actor():
                    try:
                        started.set();host.act(ref.person_id,coordinator=owner,activation=ref)
                    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
                    finally:done.set()
                worker=threading.Thread(target=actor,name='entry-budget-actor')
                try:
                    with world._private_sources_lock:
                        worker.start();self.assertTrue(started.wait(1))
                        timely=done.wait(.5)
                        untouched=self.baseline(world,host)==before
                finally:worker.join(3)
                self.assertFalse(worker.is_alive())
                self.assertTrue(timely,'uncancelled pre-acquire must consume a finite original budget')
                self.assertEqual(errors,[expected]);self.assertEqual(requests,[])
                self.assertTrue(untouched)
                self.assertEqual(self.baseline(world,host),before)

    def test_close_during_acquire_retry_backoff_keeps_cancel_diagnostic(self):
        with self.registered() as (world,host,owner,ref,requests):
            host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=1,act_deadline_seconds=2)
            before=self.baseline(world,host);backoff=threading.Event();done=threading.Event();errors=[]
            wait=host._closing.wait;count=[]
            def observed_wait(timeout=None):
                if timeout==.05:
                    count.append(timeout)
                    if len(count)==2:backoff.set()
                return wait(timeout)
            host._closing.wait=observed_wait
            def actor():
                try:host.act(ref.person_id,coordinator=owner,activation=ref)
                except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
                finally:done.set()
            worker=threading.Thread(target=actor,name='entry-backoff-cancel')
            try:
                with closing(sqlite3.connect(world.db_path)) as blocker:
                    blocker.execute('BEGIN IMMEDIATE');worker.start()
                    self.assertTrue(backoff.wait(1),'must enter the actual acquire retry backoff')
                    host.close()
                    self.assertTrue(done.wait(.5));blocker.rollback()
            finally:worker.join(3)
            self.assertFalse(worker.is_alive());self.assertEqual(errors,['ACTOR_EXECUTION_CANCELLED'])
            self.assertEqual(self.baseline(world,host),before);self.assertEqual(requests,[])

    def test_entry_budget_continues_into_actual_acquire_sql_lock_wait(self):
        from contextlib import contextmanager
        for cancel in (False,True):
            with self.subTest(cancel=cancel),self.registered() as (world,host,owner,ref,requests):
                host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=1,act_deadline_seconds=.07 if not cancel else 2)
                before=self.baseline(world,host);entered=threading.Event();done=threading.Event();closed=threading.Event();errors=[]
                actual=world._execution_connection
                @contextmanager
                def connection():
                    entered.set()
                    with actual() as db:yield db
                world._execution_connection=connection
                def actor():
                    try:host.act(ref.person_id,coordinator=owner,activation=ref)
                    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
                    finally:done.set()
                def closer():
                    try:host.close()
                    finally:closed.set()
                a=threading.Thread(target=actor,name='entry-sql-actor');c=threading.Thread(target=closer,name='entry-sql-close')
                try:
                    with closing(sqlite3.connect(world.db_path)) as blocker:
                        blocker.execute('BEGIN IMMEDIATE')
                        a.start();self.assertTrue(entered.wait(1))
                        if cancel:c.start()
                        timely=done.wait(.18)
                        close_timely=closed.is_set() if cancel else True
                        blocker.rollback()
                finally:
                    a.join(3)
                    if c.ident is not None:c.join(3)
                self.assertFalse(a.is_alive());self.assertFalse(c.is_alive())
                self.assertTrue(timely,'the same entry budget must bound the real acquire SQL wait')
                self.assertTrue(close_timely)
                self.assertEqual(errors,['ACTOR_EXECUTION_CANCELLED' if cancel else 'ACTOR_EXECUTION_DEADLINE'])
                self.assertEqual(self.baseline(world,host),before);self.assertEqual(requests,[])

    def test_preacquire_spends_original_deadline_instead_of_restarting_attempt(self):
        for captured_root in (False,True):
            with self.subTest(captured_root=captured_root),self.registered() as (world,host,owner,ref,requests):
                from contextlib import nullcontext
                from app.coordinator_execution import CoordinatorAttempt
                host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=1,act_deadline_seconds=1.5)
                parent=CoordinatorAttempt(world,owner,host.execution_policy) if captured_root else None
                started=threading.Event();done=threading.Event();errors=[];outcomes=[];observed=[]
                request=host._request
                def provider(payload):
                    observed.append(host._attempt.deadline)
                    return request(payload)
                host._request=provider
                def actor():
                    try:
                        with world._coordinator_scope(owner,parent) if parent else nullcontext():
                            started.set();outcomes.append(host.act(ref.person_id,coordinator=owner,activation=ref))
                    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
                    finally:done.set()
                worker=threading.Thread(target=actor,name='entry-remainder')
                start=time.monotonic()
                if parent:parent.deadline=start+1.2
                expected=parent.deadline if parent else start+host.execution_policy.act_deadline_seconds
                try:
                    with world._private_sources_lock:
                        worker.start();self.assertTrue(started.wait(1))
                        self.assertFalse(done.wait(.2))
                    self.assertTrue(done.wait(3))
                finally:worker.join(3)
                self.assertFalse(worker.is_alive());self.assertEqual(errors,[])
                self.assertEqual(len(observed),1);self.assertEqual(len(requests),1)
                self.assertLessEqual(observed[0],expected+.035,'real Attempt must retain pre-acquire/root remaining deadline')
                self.assertEqual(outcomes[0]['receipt']['status'],'committed')

    def test_characterization_busy_exit_restores_scope_and_retry_executes_once(self):
        with self.registered() as (world,host,owner,ref,requests):
            host.execution_policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=.08,act_deadline_seconds=2)
            held=threading.Event();release=threading.Event()
            def holder():
                with world._private_sources_lock:held.set();release.wait(3)
            worker=threading.Thread(target=holder,name='entry-retry-holder');worker.start()
            try:
                self.assertTrue(held.wait(1))
                with self.assertRaises(Exception) as caught:host.act(ref.person_id,coordinator=owner,activation=ref)
                self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY')
                self.assertIsNone(world._scoped_entry_budget())
                self.assertIsNone(world._scoped_attempt());self.assertIsNone(world._scoped_coordinator_attempt())
            finally:release.set();worker.join(3)
            self.assertFalse(worker.is_alive())
            host.execution_policy=ExecutionPolicy()
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            replay=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['intent_id'],replay['intent_id']);self.assertEqual(len(requests),1)
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(1,))
            self.assertIsNone(world._scoped_entry_budget())

    def test_characterization_entry_precommit_cancel_rolls_back_real_claim(self):
        from app import coordinator_scheduler as scheduler
        with self.registered() as (world,host,owner,ref,requests):
            before=self.baseline(world,host);entered=threading.Event();close_done=threading.Event();proofs=[]
            actual=scheduler.acquisition_proof
            def proof(*args,**kwargs):
                result=actual(*args,**kwargs);proofs.append(1)
                if len(proofs)==2:
                    entered.set();self.assertTrue(host._closing.wait(1))
                return result
            def closer():
                if entered.wait(2):host.close()
                close_done.set()
            worker=threading.Thread(target=closer,name='entry-precommit-close')
            scheduler.acquisition_proof=proof;worker.start()
            try:
                with self.assertRaises(Exception) as caught:host.act(ref.person_id,coordinator=owner,activation=ref)
                self.assertEqual(caught.exception.code,'ACTOR_EXECUTION_CANCELLED')
            finally:
                scheduler.acquisition_proof=actual;worker.join(3)
            self.assertFalse(worker.is_alive());self.assertTrue(close_done.is_set());self.assertEqual(len(proofs),2)
            self.assertEqual(self.baseline(world,host),before);self.assertEqual(requests,[])
            self.assertIsNone(world._scoped_entry_budget())

    def test_characterization_entry_scope_is_world_bound_nested_and_restored(self):
        from app.actor_execution import EntryBudget
        with self.registered() as (world,host,owner,ref,requests):
            outer=EntryBudget(ExecutionPolicy(),threading.Event());inner=EntryBudget(ExecutionPolicy(),threading.Event())
            with world._entry_budget(outer):
                self.assertIs(world._scoped_entry_budget(),outer)
                with self.assertRaisesRegex(RuntimeError,'controlled exit'):
                    with world._entry_budget(inner):
                        self.assertIs(world._scoped_entry_budget(),inner)
                        raise RuntimeError('controlled exit')
                self.assertIs(world._scoped_entry_budget(),outer)
                from app.world import WorldStore
                unrelated=WorldStore(world.db_path.parent/'not-created.sqlite')
                self.assertIsNone(unrelated._scoped_entry_budget())
                self.assertFalse(unrelated.db_path.exists())
            self.assertIsNone(world._scoped_entry_budget())

    def test_raw_world_acquire_without_host_cancel_event_has_finite_busy_wait(self):
        with self.registered() as (world,host,owner,ref,requests):
            before=self.baseline(world,host);done=threading.Event();errors=[]
            def actor():
                try:world.acquire_actor_execution(ref.person_id,'raw-entry-test',coordinator=owner,activation=ref)
                except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
                finally:done.set()
            worker=threading.Thread(target=actor,name='entry-raw-world')
            try:
                with world._private_sources_lock:
                    worker.start();timely=done.wait(2.5)
                    untouched=self.baseline(world,host)==before
            finally:worker.join(3)
            self.assertFalse(worker.is_alive())
            self.assertTrue(timely,'ordinary legal World entry must not have unbounded RLock.acquire')
            self.assertEqual(errors,['EXECUTION_STORAGE_BUSY'])
            self.assertTrue(untouched);self.assertEqual(self.baseline(world,host),before)
            self.assertEqual(requests,[])
