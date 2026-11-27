"""Cleanup-only regressions: real leases, no SDK or subprocesses."""
from contextlib import closing,contextmanager
from pathlib import Path
import sqlite3,tempfile,threading,time,unittest
from app.actors import ActorHost
from app.world import WorldStore
SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
PERSON='person-country-1-finance'
@contextmanager
def fixture():
    with tempfile.TemporaryDirectory(prefix='p023b4-cleanup-',dir=SCRATCH) as directory:
        path=Path(directory);world=WorldStore(path/'world.sqlite');world.initialize();world.control('resume')
        host=ActorHost(world,path/'actors.sqlite')
        try:yield world,host,path
        finally:host.close()
def lease(world,person=PERSON):
    with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True,timeout=0)) as db:
        return db.execute('SELECT epoch,owner_token FROM actor_execution_leases WHERE person_id=?',(person,)).fetchone()
def run_pending(world,host,person=PERSON):
    if host._lease_cleanups:
        # Aggregate independent ended lifecycles without relying on the old
        # admission bug (which formerly let a pending Host acquire again).
        return append_independent_pending(world,host,person)
    original=world.release_actor_execution;captured=[]
    def fail(permit):captured.append(permit);raise RuntimeError('test-only-release-failure')
    world.release_actor_execution=fail
    host._act=lambda *a,**k:{'intent_id':'cleanup-unit-no-domain-write'}
    try:host.act(person)
    finally:world.release_actor_execution=original
    return captured[0]
def append_independent_pending(world,host,person):
    other=ActorHost(world,host.db_path)
    try:
        permit=run_pending(world,other,person)
        host._lease_cleanups.extend(other._lease_cleanups)
        host._lease_cleanup_pending=host.cleanup_pending=True
        other._lease_cleanups=[];other._lease_cleanup_pending=other.cleanup_pending=False
        return permit
    finally:other.close()
class PersistentCloseCleanupTests(unittest.TestCase):
    def test_R2_admission_unknown_false_and_exception_do_not_acquire(self):
        from app.actor_execution import ActorRunError
        for mode in ('false_live','exception','false_unknown'):
            with self.subTest(mode=mode),fixture() as (world,host,path):
                permit=run_pending(world,host);saved=host._lease_cleanups[0];hits=[];acquires=[];executions=[]
                actual=world.release_actor_execution
                if mode=='false_unknown':
                    with closing(sqlite3.connect(world.db_path)) as db:
                        db.execute('DELETE FROM actor_execution_leases');db.commit()
                def release(value):
                    hits.append(value)
                    if mode=='exception':raise RuntimeError('R2 injected CAS error')
                    return False
                def acquire(*a,**k):acquires.append(True);raise AssertionError('unproved cleanup must not acquire')
                def act(*a,**k):executions.append(True);return {}
                world.release_actor_execution=release;world.acquire_actor_execution=acquire;host._act=act
                with self.assertRaisesRegex(ActorRunError,'ACTOR_EXECUTION_CLEANUP_PENDING'):host.act(PERSON)
                self.assertEqual(hits,[permit]);self.assertEqual(acquires,[]);self.assertEqual(executions,[])
                self.assertIs(host._lease_cleanups[0],saved);self.assertTrue(host.cleanup_pending)
                if mode!='false_unknown':self.assertEqual(lease(world),(permit.epoch,permit.owner_token))
                world.release_actor_execution=actual

    def test_R2_multiple_obligations_share_one_native_lock_budget(self):
        from app.actor_execution import ExecutionPolicy,ActorRunError
        import json
        with fixture() as (world,host,path):
            first=run_pending(world,host);second=run_pending(world,host,'person-country-1-marshal')
            saved=list(host._lease_cleanups);actual=world.release_actor_execution;hits=[];entries=[];acquires=[]
            host.execution_policy=ExecutionPolicy(busy_budget_seconds=.035,act_deadline_seconds=.1)
            def release(value):hits.append(value);entries.append(world._scoped_entry_budget());return actual(value)
            def acquire(*a,**k):acquires.append(True);raise AssertionError('must not acquire after cleanup busy')
            world.release_actor_execution=release;world.acquire_actor_execution=acquire
            with closing(sqlite3.connect(world.db_path,timeout=0)) as writer:
                writer.execute('BEGIN IMMEDIATE');started=time.monotonic()
                with self.assertRaisesRegex(ActorRunError,'EXECUTION_STORAGE_BUSY'):host.act(PERSON)
                elapsed=time.monotonic()-started;writer.rollback()
            self.assertGreaterEqual(elapsed,.025);self.assertLess(elapsed,.16)
            self.assertEqual(hits,[first]);self.assertIsNotNone(entries[0]);self.assertEqual(acquires,[])
            self.assertEqual(host._lease_cleanups,saved);self.assertTrue(host.cleanup_pending)
            self.assertEqual(lease(world),(first.epoch,first.owner_token));self.assertEqual(lease(world,second.person_id),(second.epoch,second.owner_token))
            print(json.dumps({'R2_case':'multiple_native_BUSY','elapsed':elapsed,'busy_budget':.035,'CAS_calls':len(hits),'obligations_retained':len(saved)}))
            host.close();self.assertFalse(host.cleanup_pending)

    def test_R2_multiple_successful_cleanups_and_acquire_share_original_entry(self):
        from app.actor_execution import ExecutionPolicy
        import json
        with fixture() as (world,host,path):
            first=run_pending(world,host);second=run_pending(world,host,'person-country-1-marshal')
            actual=world.release_actor_execution;acquire=world.acquire_actor_execution;entries=[];attempts=[]
            host.execution_policy=ExecutionPolicy(act_deadline_seconds=.1)
            def release(value):
                if value in (first,second):entries.append(world._scoped_entry_budget());threading.Event().wait(.015)
                return actual(value)
            def take(*a,**k):entries.append(world._scoped_entry_budget());return acquire(*a,**k)
            def act(*a,**k):attempts.append(host._attempt);return {'intent_id':'R2-multiple'}
            world.release_actor_execution=release;world.acquire_actor_execution=take;host._act=act
            started=time.monotonic();host.act(PERSON);elapsed=time.monotonic()-started
            self.assertEqual(len(entries),3);self.assertIsNotNone(entries[0]);self.assertTrue(all(e is entries[0] for e in entries))
            self.assertEqual(attempts[0].deadline,entries[0].deadline);self.assertLess(attempts[0].deadline-started,.101)
            self.assertFalse(host.cleanup_pending);self.assertEqual(lease(world,second.person_id),(second.epoch,None))
            print(json.dumps({'R2_case':'multiple_shared_entry','elapsed':elapsed,'remaining_at_return':attempts[0].deadline-time.monotonic(),'entry_deadline':entries[0].deadline,'attempt_deadline':attempts[0].deadline}))

    def test_R2_original_issuer_not_later_host_pointer_supplies_cleanup(self):
        with fixture() as (world,host,path):
            permit=run_pending(world,host);actual=world.release_actor_execution;hits=[];later_hits=[]
            other=WorldStore(path/'other-world.sqlite');other.initialize();other.control('resume')
            def release(value):hits.append((value,world._scoped_entry_budget()));return actual(value)
            def later_release(value):later_hits.append(value);raise AssertionError('not original cleanup issuer')
            def stop_acquire(*a,**k):raise RuntimeError('R2-new-admission-reached')
            world.release_actor_execution=release;other.release_actor_execution=later_release;other.acquire_actor_execution=stop_acquire
            host._authority=other
            with self.assertRaisesRegex(RuntimeError,'R2-new-admission-reached'):host.act(PERSON)
            self.assertEqual([h[0] for h in hits],[permit]);self.assertIsNotNone(hits[0][1]);self.assertEqual(later_hits,[])
            self.assertFalse(host.cleanup_pending);self.assertEqual(lease(world),(permit.epoch,None))

    def test_R2_false_proof_obeys_remaining_deadline_and_original_cancel(self):
        from app.actor_execution import EntryBudget,ExecutionPolicy,ActorRunError
        import json
        for mode in ('deadline','cancel'):
            with self.subTest(mode=mode),fixture() as (world,host,path):
                permit=run_pending(world,host);actual=world.release_actor_execution;identity=world._execution_identity;observations=[];acquires=[]
                outer=EntryBudget(ExecutionPolicy(act_deadline_seconds=.035),threading.Event())
                def expensive(db,world_id):
                    identity(db,world_id);observations.append('actual_readonly_identity')
                    if mode=='cancel':outer.stop.set()
                    db.execute('WITH RECURSIVE work(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM work WHERE n<10000000) SELECT sum(n) FROM work').fetchone()
                def acquire(*a,**k):acquires.append(True);raise AssertionError('proof failure cannot acquire')
                world.release_actor_execution=lambda p:False;world._execution_identity=expensive;world.acquire_actor_execution=acquire
                started=time.monotonic()
                with world._entry_budget(outer):
                    with self.assertRaisesRegex(ActorRunError,'ACTOR_EXECUTION_DEADLINE' if mode=='deadline' else 'ACTOR_EXECUTION_CANCELLED'):
                        host.act(PERSON)
                elapsed=time.monotonic()-started
                self.assertEqual(observations,['actual_readonly_identity']);self.assertEqual(acquires,[]);self.assertLess(elapsed,.13)
                self.assertTrue(host.cleanup_pending);self.assertEqual(lease(world),(permit.epoch,permit.owner_token))
                world._execution_identity=identity;world.release_actor_execution=actual
                print(json.dumps({'R2_case':'readonly_VM_'+mode,'elapsed':elapsed,'outer_deadline':outer.deadline,'retained':len(host._lease_cleanups)}))

    def test_R2_cleanup_exception_preserves_root_failure_and_cancel_priority(self):
        from app.tests import test_dispatch_ack_continued as fixtures
        from app.actor_execution import ActorRunError
        for mode in ('root_loss','parent_cancel','host_cancel'):
            with self.subTest(mode=mode),fixtures.prepared() as (world,host,owner,requests,root,ref,index):
                actual=world.release_actor_execution;world.release_actor_execution=lambda permit:False
                host.act(ref.person_id,coordinator=root.permit,activation=ref)
                original=host._lease_cleanups[0];count=len(requests);acquires=[]
                expected=ActorRunError('COORDINATOR_EXECUTION_FENCED')
                def release(value):
                    if mode=='root_loss':root.failure=expected
                    elif mode=='parent_cancel':root.stop.set()
                    else:host._closing.set()
                    raise ActorRunError('EXECUTION_STORAGE_BUSY')
                def acquire(*a,**k):acquires.append(True);raise AssertionError('cancelled admission cannot acquire')
                world.release_actor_execution=release;world.acquire_actor_execution=acquire
                code={'root_loss':'COORDINATOR_EXECUTION_FENCED','parent_cancel':'COORDINATOR_EXECUTION_CANCELLED','host_cancel':'ACTOR_EXECUTION_CANCELLED'}[mode]
                try:
                    with self.assertRaises(Exception) as caught:host.act(ref.person_id,coordinator=root.permit,activation=ref)
                    self.assertEqual(getattr(caught.exception,'code',None),code)
                    if mode=='root_loss':self.assertIs(caught.exception,expected)
                    self.assertEqual(acquires,[]);self.assertEqual(len(requests),count);self.assertIs(host._lease_cleanups[0],original)
                    self.assertTrue(host.cleanup_pending)
                finally:world.release_actor_execution=actual

    def test_R2_original_outer_busy_boundary_is_not_reset_by_cleanup(self):
        from app.actor_execution import EntryBudget,ExecutionPolicy,ActorRunError
        with fixture() as (world,host,path):
            permit=run_pending(world,host);actual=world.release_actor_execution;acquisitions=[];entries=[]
            outer=EntryBudget(ExecutionPolicy(busy_budget_seconds=.02),threading.Event())
            def release(value):
                if value==permit:
                    entries.append(world._scoped_entry_budget());threading.Event().wait(.03)
                return actual(value)
            def forbidden(*a,**k):acquisitions.append(True);raise AssertionError('no new acquire after outer busy boundary')
            world.release_actor_execution=release;world.acquire_actor_execution=forbidden
            with world._entry_budget(outer):
                with self.assertRaisesRegex(ActorRunError,'EXECUTION_STORAGE_BUSY'):
                    host.act(PERSON)
            self.assertEqual(acquisitions,[]);self.assertEqual(len(entries),1)
            self.assertLessEqual(entries[0].busy_deadline,outer.busy_deadline)
            self.assertTrue(host.cleanup_pending);self.assertEqual(lease(world),(permit.epoch,permit.owner_token))
            world.release_actor_execution=actual

    def test_R2_ended_obligation_reconciles_before_acquire_with_one_entry(self):
        from app.actor_execution import ExecutionPolicy
        with fixture() as (world,host,path):
            permit=run_pending(world,host)
            host.execution_policy=ExecutionPolicy(act_deadline_seconds=.1)
            actual_release=world.release_actor_execution;actual_acquire=world.acquire_actor_execution
            entries=[];attempts=[];elapsed=[]
            def release(value):
                if value==permit:
                    entries.append(world._scoped_entry_budget())
                    started=time.monotonic();threading.Event().wait(.025);elapsed.append(time.monotonic()-started)
                return actual_release(value)
            def acquire(*a,**k):
                entries.append(world._scoped_entry_budget());return actual_acquire(*a,**k)
            def act(*a,**k):
                attempts.append(host._attempt);return {'intent_id':'R2-no-sdk-no-domain-write'}
            world.release_actor_execution=release;world.acquire_actor_execution=acquire;host._act=act
            result=host.act(PERSON)
            self.assertEqual(result['intent_id'],'R2-no-sdk-no-domain-write')
            self.assertEqual(len(entries),2);self.assertIsNotNone(entries[0]);self.assertIs(entries[0],entries[1])
            self.assertEqual(attempts[0].deadline,entries[0].deadline)
            self.assertGreaterEqual(elapsed[0],.02)
            self.assertLess(attempts[0].deadline-time.monotonic(),.08,'cleanup time is not refunded')
            self.assertFalse(host.cleanup_pending);self.assertEqual(host._lease_cleanups,[])
            self.assertEqual(lease(world),(permit.epoch+1,None))

    def test_original_cleanup_under_real_root_successor_never_releases_successor(self):
        from app.tests import test_dispatch_ack_continued as fixtures
        from app.server import Coordinator
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            actual=world.release_actor_execution;world.release_actor_execution=lambda permit:False
            host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
            world.release_actor_execution=actual
            self.assertTrue(host.cleanup_pending);self.assertTrue(root.finish())
            self.assertTrue(world.release_coordinator_execution(root.permit))
            successor_world=WorldStore(world.db_path)
            observer=ActorHost(successor_world,host.db_path);successor=Coordinator(successor_world,observer)
            try:
                successor.acquire_owner();new=successor._owner_attempt.permit
                self.assertEqual(new.epoch,root.permit.epoch+1)
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    before=db.execute('SELECT * FROM coordinator_execution_leases').fetchall()
                self.assertFalse(world.release_coordinator_execution(root.permit))
                host.close();owner.close();self.assertFalse(host.cleanup_pending);self.assertFalse(owner.cleanup_pending)
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute('SELECT * FROM coordinator_execution_leases').fetchall(),before)
                self.assertIsNone(lease(world,ref.person_id)[1])
            finally:successor.close()

    def test_completed_return_with_live_provider_stays_unsafe_after_late_cleanup(self):
        from app.tests import test_dispatch_ack_continued as fixtures
        from app.actor_execution import ActorRunError,dispatch_capture_scope
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            entered=threading.Event();release=threading.Event();helpers=[];errors=[];attempts=[];actual=host._finish
            def lingering():entered.set();release.wait(8)
            def finish(*a,**k):
                result=actual(*a,**k);attempt=host._attempt;attempts.append(attempt)
                def call():
                    try:attempt.call(host,lingering)
                    except BaseException as error:errors.append(error)
                helper=threading.Thread(target=call,name='cleanup-test-extra-provider');helpers.append(helper);helper.start()
                if not entered.wait(3):raise RuntimeError('provider barrier missed')
                return result
            host._finish=finish
            try:
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture();self.assertFalse(cap.providers_joined);self.assertFalse(cap.lease_released)
                    self.assertTrue(cap.cleanup_pending);host.close();self.assertTrue(host.cleanup_pending)
                    self.assertEqual(lease(world,ref.person_id),(cap._actor_identity.epoch,cap._actor_identity.owner_token))
                    release.set()
                    for helper in helpers:helper.join(3);self.assertFalse(helper.is_alive())
                    self.assertEqual(errors,[]);host.close();self.assertFalse(host.cleanup_pending)
                    self.assertFalse(cap.providers_joined);self.assertFalse(cap.lease_released);self.assertTrue(cap.cleanup_pending)
                    with self.assertRaises(ActorRunError):cap.check()
            finally:
                release.set()
                for helper in helpers:helper.join(4)
                for attempt in attempts:
                    for thread in attempt.calls:thread.join(4)

    def test_false_initial_cas_capture_stays_unsafe_after_true_late_cas(self):
        from app.tests import test_dispatch_ack_continued as fixtures
        from app.actor_execution import ActorRunError,dispatch_capture_scope
        from app.coordinator_scheduler import prove_dispatch_ack
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            actual=world.release_actor_execution;world.release_actor_execution=lambda permit:False
            with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                slot._bind_coordinator_closing(owner._closed)
                host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                cap=slot.capture();self.assertFalse(cap.lease_released);self.assertTrue(cap.cleanup_pending)
                world.release_actor_execution=actual;host.close()
                self.assertFalse(host.cleanup_pending);self.assertIsNone(lease(world,ref.person_id)[1])
                self.assertFalse(cap.lease_released);self.assertTrue(cap.cleanup_pending)
                with self.assertRaises(ActorRunError):prove_dispatch_ack(world,slot=slot,capture=cap)

    def test_live_maintenance_blocks_cas_and_late_cleanup_never_rewrites_B_capture(self):
        from app.tests import test_dispatch_ack_continued as fixtures
        from app.actor_execution import ExecutionPolicy,ActorRunError,dispatch_capture_scope
        with fixtures.prepared() as (world,host,owner,requests,root,ref,index):
            entered=threading.Event();release=threading.Event();hits=[];actual=world.release_actor_execution
            def renew(*a,**k):
                entered.set();release.wait(8);raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
            def counted(value):hits.append(value);return actual(value)
            original_request=host._request
            def request(payload):
                if not entered.wait(3):raise RuntimeError('maintenance barrier not reached')
                return original_request(payload)
            world.renew_actor_execution=renew;world.release_actor_execution=counted
            host.execution_policy=ExecutionPolicy(renew_interval_seconds=.02);host._request=request
            cap=None
            try:
                with dispatch_capture_scope(world,host,ref,root,roster_index=index) as slot:
                    slot._bind_coordinator_closing(owner._closed)
                    host.act(ref.person_id,pause_on_failure=True,coordinator=root.permit,activation=ref)
                    cap=slot.capture();self.assertTrue(entered.is_set());self.assertTrue(cap._attempt.thread.is_alive())
                    self.assertFalse(cap.maintenance_joined);self.assertFalse(cap.lease_released)
                    host.close();self.assertTrue(host.cleanup_pending);self.assertEqual(hits,[])
                    self.assertEqual(lease(world,ref.person_id),(cap._actor_identity.epoch,cap._actor_identity.owner_token))
                    release.set();cap._attempt.thread.join(3);self.assertFalse(cap._attempt.thread.is_alive())
                    host.close();self.assertFalse(host.cleanup_pending);self.assertEqual(hits,[cap._actor_identity])
                    self.assertTrue(cap.cleanup_pending);self.assertFalse(cap.maintenance_joined);self.assertFalse(cap.lease_released)
                    with self.assertRaises(ActorRunError):cap.check()
            finally:
                release.set()
                if cap is not None:cap._attempt.thread.join(4)

    def test_multiple_original_obligations_survive_partial_cleanup_without_overwrite(self):
        with fixture() as (world,host,path):
            first=run_pending(world,host);second=run_pending(world,host,'person-country-1-marshal')
            self.assertEqual(len(host._lease_cleanups),2)
            actual=world.release_actor_execution;hits=[]
            def partial(value):
                hits.append(value)
                if value==first:return False
                return actual(value)
            world.release_actor_execution=partial
            started=time.monotonic();host.close()
            self.assertLess(time.monotonic()-started,1.5);self.assertEqual(hits,[first,second])
            self.assertTrue(host.cleanup_pending);self.assertEqual(len(host._lease_cleanups),1)
            self.assertEqual(lease(world),(first.epoch,first.owner_token))
            self.assertEqual(lease(world,second.person_id),(second.epoch,None))
            world.release_actor_execution=actual;host.close();self.assertFalse(host.cleanup_pending)
            self.assertEqual(lease(world),(first.epoch,None));host.close()

    def test_saved_issuer_isolated_from_later_host_authority_and_actor_successor(self):
        for successor_mode in (False,True):
            with self.subTest(successor=successor_mode),fixture() as (world,host,path):
                permit=run_pending(world,host);successor=None
                if successor_mode:
                    self.assertTrue(world.release_actor_execution(permit))
                    successor=world.acquire_actor_execution(PERSON,'legitimate-successor')
                    self.assertEqual(successor.epoch,permit.epoch+1)
                    self.assertFalse(world.release_actor_execution(permit))
                other=WorldStore(path/'other-world.sqlite');other.initialize()
                def forbidden(value):raise AssertionError('later host authority must not be used for original cleanup')
                other.release_actor_execution=forbidden;host._authority=other
                host.close();host.close()
                self.assertFalse(host.cleanup_pending)
                self.assertEqual(lease(world),(successor.epoch,successor.owner_token) if successor else (permit.epoch,None))
                with closing(sqlite3.connect(other.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='actor_execution_leases'").fetchone())
                if successor:self.assertTrue(world.release_actor_execution(successor))

    def test_unknown_or_malformed_readonly_identity_remains_pending(self):
        import json
        for mode in ('missing_table','missing_row','wrong_world','same_epoch_other_token','noninteger_epoch','CAS_exception'):
            with self.subTest(mode=mode),fixture() as (world,host,path):
                permit=run_pending(world,host)
                with closing(sqlite3.connect(world.db_path)) as db:
                    if mode=='missing_table':db.execute('DROP TABLE actor_execution_leases')
                    elif mode=='missing_row':db.execute('DELETE FROM actor_execution_leases')
                    elif mode=='same_epoch_other_token':db.execute('UPDATE actor_execution_leases SET owner_token=?',('other-token',))
                    elif mode=='noninteger_epoch':db.execute('UPDATE actor_execution_leases SET epoch=1.5,owner_token=NULL')
                    elif mode=='wrong_world':
                        state=world.snapshot();state['world_id']='wrong-latest-world'
                        db.execute('INSERT INTO snapshots SELECT max(seq)+1,? FROM snapshots',(json.dumps(state),))
                    db.commit()
                def fail(value):raise RuntimeError('native-like-release-fault')
                world.release_actor_execution=fail if mode=='CAS_exception' else lambda value:False
                started=time.monotonic();host.close()
                self.assertLess(time.monotonic()-started,1);self.assertTrue(host.cleanup_pending)
                self.assertEqual(len(host._lease_cleanups),1)

    def test_second_real_writer_lock_keeps_pending_one_native_cas_per_close(self):
        with fixture() as (world,host,path):
            permit=run_pending(world,host);actual=world.release_actor_execution;hits=[]
            def release(value):hits.append(value);return actual(value)
            world.release_actor_execution=release
            with closing(sqlite3.connect(world.db_path,timeout=0)) as writer:
                writer.execute('BEGIN IMMEDIATE')
                started=time.monotonic();host.close();elapsed=time.monotonic()-started
                self.assertTrue(host.cleanup_pending);self.assertEqual(hits,[permit])
                self.assertLess(elapsed,1.0,'must not reset full storage retry')
                self.assertEqual(lease(world),(permit.epoch,permit.owner_token));writer.rollback()
            host.close();self.assertFalse(host.cleanup_pending);self.assertEqual(hits,[permit,permit])

    def test_false_cas_cannot_retire_original_live_token(self):
        with fixture() as (world,host,path):
            permit=run_pending(world,host);actual=world.release_actor_execution;hits=[]
            def false(value):hits.append(value);return False
            world.release_actor_execution=false
            try:
                host.close();host.close()
                self.assertEqual(hits,[permit,permit]);self.assertTrue(host.cleanup_pending)
                self.assertEqual(lease(world),(permit.epoch,permit.owner_token))
            finally:world.release_actor_execution=actual
            host.close();self.assertFalse(host.cleanup_pending)

    def test_live_provider_prevents_original_cas_until_true_join(self):
        with fixture() as (world,host,path):
            entered=threading.Event();release=threading.Event();attempts=[];errors=[]
            def provider():entered.set();release.wait(8)
            def act(*a,**k):
                attempt=host._attempt;attempts.append(attempt)
                return attempt.call(host,provider)
            def worker():
                try:host.act(PERSON)
                except BaseException as error:errors.append(error)
            host._act=act;thread=threading.Thread(target=worker,name='cleanup-test-worker');thread.start()
            try:
                self.assertTrue(entered.wait(3));original=attempts[0]
                host.close();thread.join(3)
                self.assertFalse(thread.is_alive());self.assertTrue(original.calls[0].is_alive())
                self.assertTrue(host.cleanup_pending)
                self.assertEqual(lease(world),(original.permit.epoch,original.permit.owner_token),'live original provider forbids even successful early CAS')
                release.set();original.calls[0].join(3);self.assertFalse(original.calls[0].is_alive())
                host.close();self.assertFalse(host.cleanup_pending)
                self.assertEqual(lease(world),(original.permit.epoch,None))
                self.assertEqual([getattr(e,'code',None) for e in errors],['ACTOR_EXECUTION_CANCELLED'])
            finally:
                release.set();thread.join(4)
                for attempt in attempts:
                    for call in attempt.calls:call.join(4)

    def test_close_retries_original_failed_release_after_real_join(self):
        with fixture() as (world,host,path):
            permit=run_pending(world,host)
            self.assertTrue(host.cleanup_pending);self.assertEqual(lease(world),(permit.epoch,permit.owner_token))
            host.close();host.close()
            self.assertFalse(host.cleanup_pending,'completed original lifecycle must not leave sticky pending')
            self.assertEqual(lease(world),(permit.epoch,None))

    def test_false_cas_true_tombstone_retires_original_obligation(self):
        with fixture() as (world,host,path):
            permit=run_pending(world,host)
            self.assertTrue(world.release_actor_execution(permit))
            self.assertFalse(world.release_actor_execution(permit))
            host.close()
            self.assertFalse(host.cleanup_pending,'a proven released tombstone is safe retirement, not reacquisition')
            self.assertEqual(lease(world),(permit.epoch,None))
