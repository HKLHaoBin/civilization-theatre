"""P022 first vertical behavior: real Host owns authority before SDK.
No worker/provider is started; the actual request seam stops deliberately.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.world import WorldStore

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class ActorExecutionTests(unittest.TestCase):
    @staticmethod
    def result(payload):
        return dict(messages=payload['messages']+[{'role':'user','content':payload['prompt']}],
                    calls=[{'call_id':'call-'+payload['intent_id']}],sdk_events=1,sdk_requests=1,
                    provider='offline-test',pid=0,module_root='none')

    def test_private_candidate_and_calls_rollback_under_final_real_guard(self):
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p022-private-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');person='person-country-1-finance'
            original=world._check_actor_execution;hits=[]
            def guard(db,identity,principal,permit,**kwargs):
                original(db,identity,principal,permit,**kwargs)
                attached=[r[1] for r in db.execute('PRAGMA database_list')]
                if 'actor' in attached:
                    row=db.execute('SELECT candidate FROM actor.actor_intents WHERE person_id=?',(person,)).fetchone()
                    if row and row[0]:
                        hits.append(True)
                        self.assertEqual(db.execute('SELECT count(*) FROM actor.actor_calls').fetchone()[0],1)
                        raise WorldError('ACTOR_EXECUTION_EXPIRED','injected final guard')
            world._check_actor_execution=guard;host._request=self.result
            try:
                with self.assertRaises(WorldError) as caught:host.act(person)
                self.assertEqual(caught.exception.code,'ACTOR_EXECUTION_EXPIRED');self.assertEqual(hits,[True])
                with host._connect() as db:
                    row=db.execute('SELECT state,candidate,metrics FROM actor_intents').fetchone()
                    self.assertEqual(tuple(row),('prepared',None,None))
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_calls').fetchone()[0],0)
                self.assertIsNone(world.receipt(person,db.execute if False else 'no-receipt'))
            finally:host.close()

    def test_old_finish_never_rewinds_newer_session_or_seen(self):
        with tempfile.TemporaryDirectory(prefix='p022-cas-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            person='person-country-1-finance';host=ActorHost(world,root/'actors.sqlite');host._request=self.result
            try:
                first=host.act(person);host.act(person)
                with host._connect() as db:
                    before=tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone())
                    old=db.execute('SELECT candidate FROM actor_intents WHERE intent_id=?',(first['intent_id'],)).fetchone()[0]
                permit=world.acquire_actor_execution(person,'explicit-replay-test')
                try:host._finish(first['intent_id'],person,old,first['receipt'],execution=permit)
                finally:world.release_actor_execution(permit)
                with host._connect() as db:
                    self.assertEqual(tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone()),before,
                                     'a valid newer permit is NOT permission to rewind an already promoted old candidate')
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],2)
            finally:host.close()

    def test_execution_policy_is_strict_immutable_and_internal(self):
        import app.actors as module
        policy_type=getattr(module,'ExecutionPolicy',None)
        self.assertIsNotNone(policy_type,'Host needs a validated immutable execution policy')
        default=policy_type()
        self.assertEqual((default.ttl_seconds,default.renew_interval_seconds,default.busy_budget_seconds,default.act_deadline_seconds),(60,15,2,120))
        for fields in ({'ttl_seconds':True},{'ttl_seconds':0},{'renew_interval_seconds':20},
                       {'busy_budget_seconds':2.1},{'act_deadline_seconds':121},{'act_deadline_seconds':float('inf')},
                       {'renew_interval_seconds':False}):
            with self.assertRaises(ValueError):policy_type(**fields)
        with self.assertRaises(AttributeError):default.ttl_seconds=1

    def test_heartbeat_renews_real_lease_while_request_holds_pipeline_lock(self):
        import time,threading
        from app.actors import ExecutionPolicy
        with tempfile.TemporaryDirectory(prefix='p022-heartbeat-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            person='person-country-1-finance';policy=ExecutionPolicy(ttl_seconds=1,renew_interval_seconds=.1)
            host=ActorHost(world,root/'actors.sqlite',execution_policy=policy);renewals=[]
            original=world.renew_actor_execution
            def renew(*args,**kwargs):
                value=original(*args,**kwargs);renewals.append(threading.current_thread().name);return value
            world.renew_actor_execution=renew
            def request(payload):
                time.sleep(1.3)
                self.assertGreater(len(renewals),1,'heartbeat must renew independently during actual request')
                return self.result(payload)
            host._request=request
            try:
                host.act(person)
                self.assertTrue(all(name!='MainThread' for name in renewals))
                count=len(renewals);time.sleep(.2);self.assertEqual(len(renewals),count)
                self.assertFalse([t for t in threading.enumerate() if t.name.startswith('actor-heartbeat-')])
            finally:host.close()

    def test_heartbeat_failure_abandons_late_provider_without_private_writes(self):
        import threading
        from app.actors import ExecutionPolicy
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p022-heartbeat-error-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite',execution_policy=ExecutionPolicy(ttl_seconds=1,renew_interval_seconds=.05))
            failed=threading.Event()
            def renew(*args,**kwargs):
                failed.set();raise WorldError('EXECUTION_CLOCK_UNSAFE','injected clock failure')
            world.renew_actor_execution=renew
            def request(payload):
                self.assertTrue(failed.wait(2));return self.result(payload)
            host._request=request
            try:
                with self.assertRaises(WorldError) as caught:host.act('person-country-1-finance')
                self.assertEqual(caught.exception.code,'EXECUTION_CLOCK_UNSAFE')
                with host._connect() as db:
                    self.assertIsNone(db.execute('SELECT candidate FROM actor_intents').fetchone()[0])
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_calls').fetchone()[0],0)
            finally:host.close()

    def test_deadline_returns_before_noncooperative_request_and_discards_late_reply(self):
        import threading,time
        from app.actors import ExecutionPolicy
        with tempfile.TemporaryDirectory(prefix='p022-deadline-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite',execution_policy=ExecutionPolicy(ttl_seconds=1,renew_interval_seconds=.05,act_deadline_seconds=.15))
            release=threading.Event();done=threading.Event()
            def request(payload):
                release.wait(1);done.set();return self.result(payload)
            host._request=request;start=time.monotonic()
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_DEADLINE'):host.act('person-country-1-finance')
                self.assertLess(time.monotonic()-start,.8)
                self.assertTrue(host.cleanup_pending,'an arbitrary Python thread cannot be forcibly killed')
                release.set();self.assertTrue(done.wait(1))
                with host._connect() as db:self.assertIsNone(db.execute('SELECT candidate FROM actor_intents').fetchone()[0])
            finally:release.set();host.close()

    def test_close_cancels_before_waiting_for_pipeline_lock(self):
        import threading,time
        with tempfile.TemporaryDirectory(prefix='p022-close-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');entered=threading.Event();release=threading.Event();errors=[]
            def request(payload):entered.set();release.wait(1);return self.result(payload)
            host._request=request
            def run():
                try:host.act('person-country-1-finance')
                except Exception as error:errors.append(error)
            thread=threading.Thread(target=run);thread.start();self.assertTrue(entered.wait(2));start=time.monotonic()
            try:
                host.close();self.assertLess(time.monotonic()-start,.7,'close must cancel without acquiring the blocked pipeline lock first')
                thread.join(1);self.assertFalse(thread.is_alive());self.assertTrue(errors)
                self.assertEqual(getattr(errors[0],'code',None),'ACTOR_EXECUTION_CANCELLED')
                with host._connect() as db:self.assertIsNone(db.execute('SELECT candidate FROM actor_intents').fetchone()[0])
            finally:release.set();thread.join(2);host.close()

    def test_late_dm_provider_cannot_enter_adjudicator_or_pause_after_deadline(self):
        import threading
        from app.adjudication import Adjudicator
        from app.actors import ExecutionPolicy
        from app.scripted_dm import ScriptedDM
        with tempfile.TemporaryDirectory(prefix='p022-dm-deadline-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            release=threading.Event();dm=ScriptedDM(world)
            def provider(payload):release.wait(.5);return dm(payload)
            host=ActorHost(world,root/'actors.sqlite',adjudicator=Adjudicator(world),dm_provider=provider,
                           execution_policy=ExecutionPolicy(act_deadline_seconds=.15))
            host._request=self.result;host._choose=lambda view,seen:({'action':'propose','text':'本轮暂候消息，不采取新的行动。'},seen)
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_DEADLINE'):host.act('person-country-1-finance')
                release.set()
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM dm_decisions').fetchone()[0],0,
                                     'late provider may not record a decision or effect')
                self.assertEqual(world.snapshot()['status'],'running','deadline must not be recast as a normal Adjudicator failure pause')
            finally:release.set();host.close()

    @staticmethod
    def copy_pair(source,root):
        for name in ('world.sqlite','actors.sqlite'):
            with closing(sqlite3.connect((source/name).resolve().as_uri()+'?mode=ro',uri=True)) as original, closing(sqlite3.connect(root/name)) as copy:
                original.backup(copy)

    def test_actual_legacy_regressed_5_is_proven_and_repaired_to_9_before_request(self):
        import json
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-regressed-session-fixture'
        with tempfile.TemporaryDirectory(prefix='p022-repair-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root);world=WorldStore(root/'world.sqlite');before=world.snapshot()
            person='person-country-1-finance';host=ActorHost(world,root/'actors.sqlite');seen=[]
            def request(payload):
                self.assertEqual(len(payload['messages']),9,'prove original causal chain before assigning its version')
                with host._connect() as db:
                    head=db.execute('SELECT version,migration_kind,original_messages FROM actor_session_heads').fetchone()
                    self.assertEqual(tuple(head[:2]),(2,'legacy_proven_chain_repair'))
                    self.assertEqual(len(json.loads(head[2])),5)
                seen.append(True);raise RuntimeError('PROVEN_REPAIR_BOUNDARY')
            host._request=request
            try:
                with self.assertRaisesRegex(RuntimeError,'PROVEN_REPAIR_BOUNDARY'):host.act(person)
                self.assertEqual(seen,[True]);self.assertEqual(world.snapshot(),before)
            finally:host.close()

    def test_actual_legacy_prepared_reuses_exact_request_and_business_id(self):
        import json
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/prepared_without_candidate'
        with tempfile.TemporaryDirectory(prefix='p022-prepared-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root);world=WorldStore(root/'world.sqlite')
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:old=db.execute('SELECT intent_id,request FROM actor_intents').fetchone()
            host=ActorHost(world,root/'actors.sqlite');requests=[]
            def request(payload):requests.append(payload);return self.result(payload)
            host._request=request
            try:
                result=host.act('person-country-1-finance')
                self.assertEqual(result['intent_id'],old[0]);self.assertEqual(requests,[json.loads(old[1])])
                with host._connect() as db:self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],1)
            finally:host.close()

    def test_actual_legacy_candidate_recovers_original_id_without_sdk(self):
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/candidate_without_world_receipt'
        with tempfile.TemporaryDirectory(prefix='p022-candidate-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root);world=WorldStore(root/'world.sqlite')
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:old=db.execute('SELECT intent_id,candidate,consumed FROM actor_intents').fetchone()
            host=ActorHost(world,root/'actors.sqlite')
            host._request=lambda payload:self.fail('durable candidate may not be regenerated')
            try:
                result=host.act('person-country-1-finance');self.assertEqual(result['intent_id'],old[0])
                with host._connect() as db:
                    self.assertEqual(tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone()),old[1:])
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],1)
                self.assertEqual(world.receipt('person-country-1-finance',old[0]),result['receipt'])
                self.assertIsNone(host.worker_pid)
            finally:host.close()

    def test_c_actual_legacy_receipt_reconciles_without_sdk_dm_or_world_resubmit(self):
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/world_receipt_without_private_promotion'
        with tempfile.TemporaryDirectory(prefix='p022-receipt-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root);world=WorldStore(root/'world.sqlite');before=world.snapshot()
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:old=db.execute('SELECT intent_id,candidate,consumed FROM actor_intents').fetchone()
            host=ActorHost(world,root/'actors.sqlite');host._request=lambda payload:self.fail('must not resend SDK')
            world.submit=lambda *a,**k:self.fail('must not resubmit an already committed effect')
            try:
                result=host.act('person-country-1-finance');self.assertTrue(result['reconciled']);self.assertEqual(result['intent_id'],old[0])
                self.assertEqual(world.snapshot(),before)
                with host._connect() as db:self.assertEqual(tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone()),old[1:])
            finally:host.close()

    def test_c_actual_legacy_committed_endpoint_preserves_raw_and_is_next_request_base(self):
        import json
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/committed_with_newer_private_session'
        with tempfile.TemporaryDirectory(prefix='p022-committed-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root);world=WorldStore(root/'world.sqlite')
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:old=tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone())
            host=ActorHost(world,root/'actors.sqlite')
            def request(payload):self.assertEqual(payload['messages'],json.loads(old[0]));raise RuntimeError('COMMITTED_ENDPOINT')
            host._request=request
            try:
                with self.assertRaisesRegex(RuntimeError,'COMMITTED_ENDPOINT'):host.act('person-country-1-finance')
                with host._connect() as db:
                    self.assertEqual(tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone()),old)
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],2)
            finally:host.close()

    def test_c_legacy_corrupt_seen_missing_candidate_branch_and_multiple_pending_fail_closed(self):
        import json
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/committed_with_newer_private_session'
        for corrupt in ('seen','missing_candidate','branch','multi_pending'):
            with self.subTest(corrupt=corrupt),tempfile.TemporaryDirectory(prefix='p022-refusal-',dir=SCRATCH) as temp:
                root=Path(temp);self.copy_pair(source,root)
                with closing(sqlite3.connect(root/'actors.sqlite')) as db:
                    if corrupt=='seen':db.execute("UPDATE actor_sessions SET seen='[\"invented_seen\"]'")
                    elif corrupt=='missing_candidate':db.execute('UPDATE actor_intents SET candidate=NULL')
                    elif corrupt=='multi_pending':db.execute("UPDATE actor_intents SET state='candidate'")
                    else:
                        rows=db.execute('SELECT intent_id,request FROM actor_intents').fetchall()
                        for intent,raw in rows:
                            request=json.loads(raw);request['messages']=[]
                            db.execute('UPDATE actor_intents SET request=? WHERE intent_id=?',(json.dumps(request),intent))
                    db.commit();before=list(db.iterdump())
                world=WorldStore(root/'world.sqlite');host=ActorHost(world,root/'actors.sqlite')
                host._request=lambda payload:self.fail('unproven history must not call SDK')
                try:
                    with self.assertRaisesRegex(RuntimeError,'ACTOR_RECOVERY_UNPROVEN|ACTOR_PENDING_CONFLICT'):host.act('person-country-1-finance')
                    with host._connect() as db:self.assertEqual(list(db.iterdump()),before,'adoption DDL and all private effects roll back')
                finally:host.close()

    def test_known_archived_world_is_explicitly_adapted_not_unfenced(self):
        import importlib.util
        source=Path(__file__).resolve().parents[1]/'evidence/candidates/p07-1/app/world.py'
        spec=importlib.util.spec_from_file_location('app.p022_known_archived_world',source)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory(prefix='p022-adapter-',dir=SCRATCH) as temp:
            root=Path(temp);world=module.WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');host._request=self.result
            try:
                self.assertTrue(host.execution_fenced,'persistent legacy storage must use explicit current-World authority adapter')
                self.assertIs(host.world,world)
                world.submit=lambda *a,**k:self.fail('adapter may not call old unguarded submit')
                result=host.act('person-country-1-finance')
                self.assertEqual(result['receipt'],world.receipt('person-country-1-finance',result['intent_id']))
                with closing(sqlite3.connect(root/'world.sqlite')) as db:self.assertEqual(db.execute('SELECT epoch,owner_token FROM actor_execution_leases').fetchone(),(1,None))
            finally:host.close()

    def test_acquire_retries_only_bounded_storage_busy_not_live_owner(self):
        import threading,time
        with tempfile.TemporaryDirectory(prefix='p022-busy-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');host._request=self.result
            original=world.acquire_actor_execution;calls=[]
            def acquire(*a,**k):calls.append(True);return original(*a,**k)
            world.acquire_actor_execution=acquire
            with closing(sqlite3.connect(world.db_path,check_same_thread=False)) as lock:
                lock.execute('BEGIN IMMEDIATE');timer=threading.Timer(.7,lock.commit);timer.start();start=time.monotonic()
                try:host.act('person-country-1-finance')
                finally:timer.join(2);lock.rollback();host.close()
            self.assertGreaterEqual(len(calls),2);self.assertLessEqual(len(calls),3);self.assertLess(time.monotonic()-start,2)

    def test_release_busy_is_bounded_preserves_original_error_and_reports_cleanup(self):
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p022-release-busy-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');calls=[]
            def release(permit):calls.append(True);raise WorldError('EXECUTION_STORAGE_BUSY','injected busy')
            world.release_actor_execution=release
            def request(payload):raise RuntimeError('ORIGINAL_PROVIDER_ERROR')
            host._request=request
            try:
                with self.assertRaisesRegex(RuntimeError,'ORIGINAL_PROVIDER_ERROR'):host.act('person-country-1-finance')
                self.assertEqual(len(calls),3);self.assertTrue(host.cleanup_pending)
                with closing(sqlite3.connect(world.db_path)) as db:self.assertIsNotNone(db.execute('SELECT owner_token FROM actor_execution_leases').fetchone()[0])
            finally:host.close()

    def test_renew_busy_retries_original_permit_without_new_acquisition(self):
        import threading
        from app.actors import ExecutionPolicy
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p022-renew-busy-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite',execution_policy=ExecutionPolicy(ttl_seconds=1,renew_interval_seconds=.05))
            original=world.renew_actor_execution;calls=[];renewed=threading.Event()
            def renew(permit,**kwargs):
                calls.append(permit)
                if len(calls)<3:raise WorldError('EXECUTION_STORAGE_BUSY','injected busy')
                result=original(permit,**kwargs);renewed.set();return result
            world.renew_actor_execution=renew
            def request(payload):self.assertTrue(renewed.wait(2));return self.result(payload)
            host._request=request
            try:
                host.act('person-country-1-finance');self.assertGreaterEqual(len(calls),3);self.assertEqual(len(set(calls)),1)
                with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(db.execute('SELECT epoch FROM actor_execution_leases').fetchone()[0],1)
            finally:host.close()

    def test_private_begin_retries_busy_with_closed_connections(self):
        import threading
        with tempfile.TemporaryDirectory(prefix='p022-private-busy-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');resources=[]
            def request(payload):
                lock=sqlite3.connect(world.db_path,check_same_thread=False);lock.execute('BEGIN IMMEDIATE')
                timer=threading.Timer(.7,lock.commit);timer.start();resources.append((lock,timer))
                return self.result(payload)
            host._request=request
            try:host.act('person-country-1-finance')
            finally:
                for lock,timer in resources:timer.join(2);lock.close()
                host.close()

    def test_private_precommit_checks_cancellation_after_actual_final_guard(self):
        from app.actors import ActorRunError
        with tempfile.TemporaryDirectory(prefix='p022-final-cancel-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');host._request=self.result;original=world._check_actor_execution
            def guard(db,*args,**kwargs):
                result=original(db,*args,**kwargs)
                if 'actor' in [r[1] for r in db.execute('PRAGMA database_list')]:
                    row=db.execute('SELECT candidate FROM actor.actor_intents').fetchone()
                    if row and row[0]:host._attempt.failure=ActorRunError('ACTOR_EXECUTION_CANCELLED')
                return result
            world._check_actor_execution=guard
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_CANCELLED'):host.act('person-country-1-finance')
                with host._connect() as db:self.assertIsNone(db.execute('SELECT candidate FROM actor_intents').fetchone()[0])
            finally:host.close()

    def test_real_submit_without_permit_is_required_even_before_registration(self):
        with tempfile.TemporaryDirectory(prefix='p022-required-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume');before=world.snapshot()
            host=ActorHost(world,root/'actors.sqlite')
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_REQUIRED'):
                    host._submit('person-country-1-finance','unregistered-bypass',{'action':'wait'})
                self.assertEqual(world.snapshot(),before)
            finally:host.close()

    def test_existing_head_never_skips_revalidation_of_original_history(self):
        with tempfile.TemporaryDirectory(prefix='p022-head-revalidate-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');host._request=self.result
            try:
                first=host.act('person-country-1-finance');host.act('person-country-1-finance')
                with host._connect() as db:db.execute('UPDATE actor_intents SET candidate=NULL WHERE intent_id=?',(first['intent_id'],))
                before=world.snapshot();requests=[]
                def request(payload):requests.append(True);return self.result(payload)
                host._request=request
                with self.assertRaisesRegex(RuntimeError,'ACTOR_RECOVERY_UNPROVEN'):host.act('person-country-1-finance')
                self.assertEqual(requests,[]);self.assertEqual(world.snapshot(),before)
            finally:host.close()

    def test_constructor_rejects_foreign_stored_receipt_before_any_adoption(self):
        import json
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/committed_with_newer_private_session'
        with tempfile.TemporaryDirectory(prefix='p022-foreign-receipt-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root)
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:
                row=db.execute('SELECT intent_id,receipt FROM actor_intents LIMIT 1').fetchone();receipt=json.loads(row[1]);receipt['world_id']='foreign-world'
                db.execute('UPDATE actor_intents SET receipt=? WHERE intent_id=?',(json.dumps(receipt),row[0]));db.commit();before=list(db.iterdump())
            with self.assertRaisesRegex(RuntimeError,'ACTOR_WORLD_MISMATCH'):ActorHost(WorldStore(root/'world.sqlite'),root/'actors.sqlite')
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:self.assertEqual(list(db.iterdump()),before)

    def test_private_queries_never_resolve_a_same_named_world_main_table(self):
        with tempfile.TemporaryDirectory(prefix='p022-namespace-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('CREATE TABLE actor_sessions(person_id TEXT PRIMARY KEY,messages TEXT,seen TEXT)')
                db.execute("INSERT INTO actor_sessions VALUES ('person-country-1-finance','[\"MAIN_NOT_PRIVATE\"]','[]')");db.commit()
            host=ActorHost(world,root/'actors.sqlite')
            def request(payload):self.assertEqual(payload['messages'],[],'private storage must be explicitly namespaced to ATTACH actor');return self.result(payload)
            host._request=request
            try:
                host.act('person-country-1-finance')
                with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(db.execute('SELECT messages FROM actor_sessions').fetchone()[0],'[\"MAIN_NOT_PRIVATE\"]')
            finally:host.close()

    def test_repeated_finish_requires_proven_promotion_marker(self):
        with tempfile.TemporaryDirectory(prefix='p022-marker-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');host._request=self.result;person='person-country-1-finance'
            try:
                first=host.act(person);host.act(person)
                with host._connect() as db:
                    candidate=db.execute('SELECT candidate FROM actor_intents WHERE intent_id=?',(first['intent_id'],)).fetchone()[0]
                    db.execute('UPDATE actor_intent_execution SET promoted_version=2 WHERE intent_id=?',(first['intent_id'],))
                permit=world.acquire_actor_execution(person,'marker-replay')
                try:
                    with self.assertRaisesRegex(RuntimeError,'ACTOR_RECOVERY_UNPROVEN'):host._finish(first['intent_id'],person,candidate,first['receipt'],execution=permit)
                finally:world.release_actor_execution(permit)
            finally:host.close()

    def test_noncooperative_old_request_blocks_worker_reuse_until_actual_exit(self):
        import threading
        from app.actors import ExecutionPolicy
        with tempfile.TemporaryDirectory(prefix='p022-cleanup-boundary-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite',execution_policy=ExecutionPolicy(act_deadline_seconds=.1))
            release=threading.Event();done=threading.Event()
            def request(payload):release.wait(1);done.set();return self.result(payload)
            host._request=request
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_DEADLINE'):host.act('person-country-1-finance')
                host._request=self.result
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_CLEANUP_PENDING'):host.act('person-country-1-finance')
                release.set();self.assertTrue(done.wait(1))
                for thread in host._pending_threads:thread.join(1)
                host.act('person-country-1-finance');self.assertFalse(host.cleanup_pending)
            finally:release.set();host.close()

    def test_valid_new_permit_does_not_bypass_stale_session_base_without_promotion_mark(self):
        with tempfile.TemporaryDirectory(prefix='p022-stale-base-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');host._request=self.result;person='person-country-1-finance'
            try:
                first=host.act(person);host.act(person)
                with host._connect() as db:
                    candidate=db.execute('SELECT candidate FROM actor_intents WHERE intent_id=?',(first['intent_id'],)).fetchone()[0]
                    before=tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone())
                    db.execute('UPDATE actor_intent_execution SET promoted_version=NULL WHERE intent_id=?',(first['intent_id'],))
                permit=world.acquire_actor_execution(person,'stale-cas')
                try:
                    with self.assertRaisesRegex(RuntimeError,'ACTOR_SESSION_STALE'):
                        host._finish(first['intent_id'],person,candidate,first['receipt'],execution=permit)
                finally:world.release_actor_execution(permit)
                with host._connect() as db:self.assertEqual(tuple(db.execute('SELECT messages,seen FROM actor_sessions').fetchone()),before)
            finally:host.close()

    def test_legacy_malformed_message_shape_has_stable_unproven_code(self):
        source=Path(__file__).resolve().parents[1]/'evidence/p021-parent-actor-recovery-fixtures/candidate_without_world_receipt'
        with tempfile.TemporaryDirectory(prefix='p022-malformed-',dir=SCRATCH) as temp:
            root=Path(temp);self.copy_pair(source,root)
            with closing(sqlite3.connect(root/'actors.sqlite')) as db:db.execute("UPDATE actor_intents SET candidate='[1]'");db.commit();before=list(db.iterdump())
            host=ActorHost(WorldStore(root/'world.sqlite'),root/'actors.sqlite')
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_RECOVERY_UNPROVEN'):host.act('person-country-1-finance')
                with host._connect() as db:self.assertEqual(list(db.iterdump()),before)
            finally:host.close()

    def test_contract_deadline_bounds_actual_original_world_submit_lock_wait(self):
        """Next vertical RED: actual World SQL wait exceeds Host deadline.
        Do not weaken this assertion or patch the forbidden World implementation.
        """
        import threading,time,json,uuid
        from app.actors import ExecutionPolicy
        with tempfile.TemporaryDirectory(prefix='p022-world-deadline-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite',execution_policy=ExecutionPolicy(act_deadline_seconds=.15))
            host._request=self.result;person='person-country-1-finance';resources=[];original=world.submit
            def submit(*args,**kwargs):
                lock=sqlite3.connect(world.db_path,check_same_thread=False);lock.execute('BEGIN IMMEDIATE')
                timer=threading.Timer(1.0,lock.commit);timer.start();resources.append((lock,timer))
                return original(*args,**kwargs)
            world.submit=submit;start=time.monotonic();before=world.snapshot()
            try:
                with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_DEADLINE'):host.act(person)
                elapsed=time.monotonic()-start
                with host._connect() as db:
                    intent=db.execute('SELECT intent_id,state FROM actor_intents').fetchone()
                receipt=world.receipt(person,intent[0])
                report=dict(boundary='actual original WorldStore.submit, independent SQLite write lock',
                            policy_deadline_seconds=.15,actual_elapsed_seconds=elapsed,world_receipt=receipt,
                            actor_intent_state=intent[1],world_changed=world.snapshot()!=before,
                            forbidden_source_change_required='app/world.py connection timeout / trusted precommit cancellation seam')
                path=Path(__file__).resolve().parents[1]/f'evidence/p022-backend-world-deadline-repro-{uuid.uuid4().hex}.json'
                path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
                print('ACTUAL_WORLD_DEADLINE_REPRO',json.dumps(report))
                self.assertLess(elapsed,.7,'Host act deadline must bound the original World SQL wait, not merely reject private promotion afterwards')
                self.assertIsNone(receipt,'a World effect may not be newly committed after the Host execution deadline')
            finally:
                for lock,timer in resources:timer.join(2);lock.close()
                host.close()

    def test_real_submit_uses_original_world_and_captured_permit(self):
        with tempfile.TemporaryDirectory(prefix='p022-submit-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            person='person-country-1-finance';host=ActorHost(world,root/'actors.sqlite')
            original=world.submit;calls=[]
            def submit(*args,**kwargs):
                calls.append(kwargs.get('execution'))
                return original(*args,**kwargs)
            world.submit=submit;host._request=self.result
            try:
                result=host.act(person)
                self.assertEqual(len(calls),1);self.assertIsNotNone(calls[0])
                self.assertEqual(world.receipt(person,result['intent_id']),result['receipt'])
            finally:host.close()

    def test_real_act_owns_lease_before_request_and_releases_on_error(self):
        with tempfile.TemporaryDirectory(prefix='p022-first-',dir=SCRATCH) as temp:
            root=Path(temp)
            world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            before=world.snapshot();person='person-country-1-finance'
            host=ActorHost(world,root/'actors.sqlite')
            seen=[]
            def request_boundary(payload):
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_execution_leases'").fetchone()
                    self.assertTrue(exists,'actual ActorHost.act must acquire World authority BEFORE the provider request')
                    row=db.execute('SELECT world_id,person_id,epoch,owner_token FROM actor_execution_leases WHERE person_id=?',(person,)).fetchone()
                self.assertIsNotNone(row,'actual request cannot run without a persistent live lease')
                self.assertEqual(row[:3],(before['world_id'],person,1))
                self.assertIsInstance(row[3],str);self.assertTrue(row[3]);seen.append(True)
                raise RuntimeError('P022_PROVIDER_BOUNDARY')
            host._request=request_boundary
            try:
                with self.assertRaisesRegex(RuntimeError,'P022_PROVIDER_BOUNDARY'):
                    host.act(person)
                self.assertEqual(seen,[True])
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    row=db.execute('SELECT epoch,owner_token FROM actor_execution_leases WHERE person_id=?',(person,)).fetchone()
                self.assertEqual(row,(1,None),'act finally releases its own lease before close, retaining epoch tombstone')
                self.assertEqual(world.snapshot(),before,'Host metadata/failed request does not alter World facts')
                self.assertIsNone(host.worker_pid)
            finally:
                host.close()


if __name__=='__main__':unittest.main()
