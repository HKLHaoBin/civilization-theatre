"""Q50-M1 first vertical behavior: real committed/block producer, SDK0 seam."""
from pathlib import Path
from contextlib import closing
import hashlib,json,sqlite3,tempfile,unittest,uuid
from app.world import WorldStore,WorldError
from app.actors import ActorHost
from app.server import Coordinator
from app.coordinator_scheduler import ActivationRef
from app.tests import test_actor_execution as provider_fixture

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
EVIDENCE=Path(__file__).resolve().parents[1]/'evidence'
TRACE=[]
def digest(obj):return hashlib.sha256(json.dumps(obj,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
def tables(path,names):
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
        return {n:[list(x) for x in db.execute('SELECT * FROM "'+n+'" ORDER BY rowid')] for n in names}

class TechnicalMaintenanceTests(unittest.TestCase):
    def test_world_committed_recovery_promotes_original_private_once_and_keeps_global_pause(self):
        with tempfile.TemporaryDirectory(prefix='q50-m1-first-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite');coordinator=Coordinator(world,host)
            calls=[]
            def request(payload):
                calls.append(payload)
                return provider_fixture.ActorExecutionTests.result(payload)
            host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
            try:
                owner=coordinator.acquire_owner()
                world.control('resume',coordinator=owner)
                version={k:world.snapshot()[k] for k in ('world_id','revision')}
                boundary=world.inspect_scheduler_boundary(actor_db_path=host.db_path)
                world.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=host.db_path)
                window=world.open_or_recover_window(coordinator=owner,expected_version=version)
                row=world.next_activation(window['window_id'],coordinator=owner)
                ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                original_finish=host._finish
                def fault(*args,**kwargs):raise RuntimeError('q50-m1 controlled original private finish fault')
                host._finish=fault
                try:
                    with self.assertRaisesRegex(RuntimeError,'q50-m1 controlled original private finish fault'):
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                finally:host._finish=original_finish
                paused=world.snapshot();self.assertEqual(paused['status'],'technical_paused');self.assertEqual(len(calls),1)
                with closing(sqlite3.connect(world.db_path)) as db:
                    phase=db.execute('SELECT phase,resume_phase,private_closed,intent_id,receipt_sha256 FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                    block=db.execute('SELECT * FROM coordinator_activation_blocks WHERE activation_id=?',(ref.activation_id,)).fetchone()
                    receipt=db.execute('SELECT receipt FROM intents WHERE id=?',(phase[3],)).fetchone()[0]
                self.assertEqual(phase[:3],('technical_blocked','world_committed',0));self.assertIsNotNone(receipt);self.assertIsNotNone(phase[4])
                with closing(sqlite3.connect(host.db_path)) as db:
                    private_before=db.execute('SELECT body,request,candidate FROM actor_intents WHERE intent_id=?',(phase[3],)).fetchone()
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(0,))
                facts=tables(world.db_path,['snapshots','intents','coordinator_activation_outcomes','coordinator_windows'])
                with self.assertRaises(WorldError) as old_start:coordinator.start()
                self.assertEqual(old_start.exception.code,'WORLD_PAUSED')
                with self.assertRaises(WorldError) as old_host:host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(old_host.exception.code,'ACTIVATION_TECHNICAL_BLOCKED');self.assertEqual(len(calls),1)
                def forbid(*args,**kwargs):raise AssertionError('M1 must not reissue provider/DM/business submit')
                host._request=forbid;host._submit=forbid;host.dm_provider=forbid
                evidence_file=root/'repair-validation.json';evidence_file.write_text(json.dumps({'kind':'controlled_original_finish_fault_removed','activation_id':ref.activation_id,'intent_id':phase[3],'original_finish_restored':getattr(host._finish,'__func__',None) is ActorHost._finish,'pause_revision':paused['revision'],'prior_request_seam_calls':1}),encoding='utf-8')
                sources=['app/actors.py','app/actor_execution.py','app/coordinator_scheduler.py','app/server.py','app/world.py']
                project=Path(__file__).resolve().parents[2]
                repair={'repair_id':uuid.uuid4().hex,'reason':'controlled finish fault removed; original ActorHost._finish restored; immutable committed material retained','source_sha256':{n:hashlib.sha256((project/n).read_bytes()).hexdigest() for n in sources},'verification_refs':[{'path':str(evidence_file),'sha256':hashlib.sha256(evidence_file.read_bytes()).hexdigest()}]}
                operation=uuid.uuid4().hex
                TRACE.append({'case':'real_world_committed_blocked_precondition','phase':list(phase[:3]),'activation_id':ref.activation_id,'intent_id':phase[3],'head_version':0,'request_seam_calls':len(calls),'actual_SDK_requests':0,'legacy_start_code':old_start.exception.code,'legacy_host_code':old_host.exception.code,'domain_projection_sha256':digest(facts),'block_fields_sha256':digest(block),'original_finish_restored':True})
                recover=getattr(coordinator,'recover_technical',None)
                self.assertTrue(callable(recover),'Q50-M1 missing trusted recovery: real committed activation remains technical_blocked/head0 after fault removal; expected original private closure with global pause retained')
                result=recover(ref,operation_id=operation,repair_evidence=repair)
                self.assertEqual(result['operation_id'],operation);self.assertEqual(result['activation_id'],ref.activation_id);self.assertEqual(result['intent_id'],phase[3]);self.assertEqual(result['phase'],'completed');self.assertEqual(result['world_status'],'technical_paused');self.assertIs(result['replayed'],False)
                self.assertEqual(world.snapshot(),paused);self.assertEqual(tables(world.db_path,list(facts)),facts);self.assertEqual(len(calls),1)
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(1,))
                    self.assertEqual(db.execute('SELECT state,body,request,candidate FROM actor_intents WHERE intent_id=?',(phase[3],)).fetchone(),('committed',*private_before))
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,private_closed,intent_id FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('completed',1,phase[3]))
                    self.assertEqual(db.execute('SELECT receipt FROM intents WHERE id=?',(phase[3],)).fetchone()[0],receipt)
                TRACE.append({'case':'maintenance_private_closure','passed':True,'phase':'completed','head_version':1,'world_status':'technical_paused','request_seam_calls':len(calls),'actual_SDK_requests':0,'domain_projection_unchanged':True})
            finally:
                coordinator.close();host.close()


# New tracers append below the byte-preserved parent method.
from contextlib import contextmanager

@contextmanager
def blocked_case(label):
    root=Path(tempfile.mkdtemp(prefix='q50-m1-'+label+'-',dir=SCRATCH))
    world=WorldStore(root/'world.sqlite');world.initialize()
    host=ActorHost(world,root/'actors.sqlite');coordinator=Coordinator(world,host)
    calls=[]
    def request(payload):
        calls.append(payload)
        return provider_fixture.ActorExecutionTests.result(payload)
    host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
    try:
        owner=coordinator.acquire_owner();world.control('resume',coordinator=owner)
        version={k:world.snapshot()[k] for k in ('world_id','revision')}
        boundary=world.inspect_scheduler_boundary(actor_db_path=host.db_path)
        world.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=host.db_path)
        window=world.open_or_recover_window(coordinator=owner,expected_version=version)
        row=world.next_activation(window['window_id'],coordinator=owner)
        ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
        finish=host._finish
        def fault(*a,**k):raise RuntimeError('controlled original private finish fault')
        host._finish=fault
        try:
            try:host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except RuntimeError as error:
                assert str(error)=='controlled original private finish fault',repr(error)
            else:raise AssertionError('fault must be reached')
        finally:host._finish=finish
        assert len(calls)==1
        def forbid(*a,**k):raise AssertionError('no SDK/provider/submit/DM allowed')
        host._request=forbid;host._submit=forbid;host.dm_provider=forbid
        record=root/'verification.json';record.write_text(json.dumps({'kind':'controlled_fault_removed','activation_id':ref.activation_id}),encoding='utf-8')
        project=Path(__file__).resolve().parents[2]
        repair={'repair_id':uuid.uuid4().hex,'reason':'controlled original finish fault removed; native proof required','source_sha256':{n:hashlib.sha256((project/n).read_bytes()).hexdigest() for n in ['app/actors.py','app/actor_execution.py','app/coordinator_scheduler.py','app/server.py','app/world.py']},'verification_refs':[{'path':str(record),'sha256':hashlib.sha256(record.read_bytes()).hexdigest()}]}
        case={'world':world,'host':host,'coordinator':coordinator,'ref':ref,'root':root,'repair':repair,'operation':uuid.uuid4().hex,'paused':world.snapshot(),'calls':calls}
        yield case
    finally:
        coordinator.close();host.close()
        TRACE.append({'case':label,'db_root':str(root),'activation_id':ref.activation_id if 'ref' in locals() else None,'actual_SDK_requests':0,'synthetic_request_seam_calls':len(calls),'cleanup_pending':host.cleanup_pending})


def perform(case,**changes):
    args={'operation_id':case['operation'],'repair_evidence':case['repair']};args.update(changes)
    return case['coordinator'].recover_technical(case['ref'],**args)


class MaintenanceAdditionalTests(unittest.TestCase):
    def test_same_key_replay_and_changed_payload_rejected(self):
        with blocked_case('replay') as c:
            first=perform(c)
            before=tables(Path(c['host'].db_path),['actor_sessions','actor_intents','actor_session_heads','actor_intent_execution'])
            second=perform(c)
            self.assertIs(second['replayed'],True)
            self.assertEqual({k:v for k,v in first.items() if k!='replayed'},{k:v for k,v in second.items() if k!='replayed'})
            changed={**c['repair'],'reason':'different operator attestation'}
            with self.assertRaises(WorldError) as caught:perform(c,repair_evidence=changed)
            self.assertEqual(caught.exception.code,'TECHNICAL_MAINTENANCE_OPERATION_CONFLICT')
            self.assertEqual(tables(Path(c['host'].db_path),list(before)),before)
            self.assertEqual(c['world'].snapshot(),c['paused'])
            self.assertEqual(len(tables(c['world'].db_path,['coordinator_technical_maintenance'])['coordinator_technical_maintenance']),1)

    def test_characterization_forged_grants_pending_phase_and_unsupported_records_are_no_write(self):
        from app import coordinator_maintenance as maintenance
        with blocked_case('denials') as c:
            w=c['world'];h=c['host'];ref=c['ref']
            before=(logical(w.db_path),logical(h.db_path))
            for fake in ({'authorized':True},None,{'fixed':True}):
                with self.subTest(fake=fake),self.assertRaises(WorldError) as caught:maintenance.promote(fake)
                self.assertEqual(caught.exception.code,'TECHNICAL_MAINTENANCE_REQUIRED')
            for repair in ({**c['repair'],'fixed':True},{**c['repair'],'verification_refs':[]},{**c['repair'],'source_sha256':{}},{**c['repair'],'verification_refs':[{'path':str(Path(__file__).resolve()),'sha256':'0'*64}]}):
                with self.subTest(repair=repair),self.assertRaises(WorldError):perform(c,repair_evidence=repair)
            with closing(sqlite3.connect(w.db_path)) as db:
                row=db.execute("SELECT world_id,window_id,activation_id,person_id FROM coordinator_activations WHERE phase='pending' ORDER BY roster_index LIMIT 1").fetchone()
            pending=ActivationRef(*row)
            with self.assertRaises(WorldError) as caught:c['coordinator'].recover_technical(pending,operation_id=c['operation'],repair_evidence=c['repair'])
            self.assertEqual(caught.exception.code,'TECHNICAL_MAINTENANCE_UNSUPPORTED_PHASE')
            self.assertEqual((logical(w.db_path),logical(h.db_path)),before)
            source=(Path(__file__).resolve().parents[1]/'server.py').read_text(encoding='utf-8')
            self.assertNotIn('recover_technical',source[source.index('def make_server('):])
            self.assertEqual(len(c['calls']),1)

    def test_characterization_real_private_trigger_abort_rolls_back_and_retry_promotes_once(self):
        with blocked_case('rollback') as c:
            w=c['world'];h=c['host']
            with closing(sqlite3.connect(h.db_path)) as db:
                db.execute("CREATE TRIGGER q50_fault BEFORE UPDATE ON actor_sessions BEGIN SELECT RAISE(ABORT,'original maintenance transaction fault'); END");db.commit()
            before=logical(h.db_path);facts=tables(w.db_path,['snapshots','intents','coordinator_activation_blocks','coordinator_activation_outcomes','coordinator_activations','coordinator_windows'])
            with self.assertRaisesRegex(sqlite3.IntegrityError,'original maintenance transaction fault'):perform(c)
            self.assertEqual(logical(h.db_path),before);self.assertEqual(tables(w.db_path,list(facts)),facts)
            with closing(sqlite3.connect(w.db_path)) as db:self.assertIsNone(db.execute("SELECT name FROM sqlite_master WHERE name='coordinator_technical_maintenance'").fetchone())
            TRACE.append({'case':'rollback_pre_retry','db_root':str(c['root']),'activation_id':c['ref'].activation_id,'head_version':0,'phase':'technical_blocked','domain_unchanged':True})
            # Retain independent SQL-observer input before the positive retry.
            saved=c['root']/'rollback';saved.mkdir()
            import shutil
            shutil.copyfile(w.db_path,saved/'world.sqlite');shutil.copyfile(h.db_path,saved/'actors.sqlite')
            with closing(sqlite3.connect(h.db_path)) as db:db.execute('DROP TRIGGER q50_fault');db.commit()
            result=perform(c);self.assertFalse(result['replayed']);self.assertTrue(perform(c)['replayed'])
            self.assertEqual(c['world'].snapshot(),c['paused']);self.assertEqual(len(c['calls']),1)


    def test_characterization_material_and_final_authority_identity_fail_closed(self):
        from app import coordinator_maintenance as maintenance
        from unittest.mock import patch
        from dataclasses import replace
        for mode,code in [('request','WINDOW_RECOVERY_UNPROVEN'),('root','COORDINATOR_EXECUTION_FENCED'),('host','ACTIVATION_BINDING_CONFLICT'),('ref','ACTIVATION_BINDING_CONFLICT'),('actor','ACTOR_EXECUTION_FENCED'),('verification','TECHNICAL_MAINTENANCE_SOURCE_CHANGED'),('source','WINDOW_RECOVERY_UNPROVEN')]:
            with self.subTest(mode=mode),blocked_case('identity-'+mode) as c:
                w=c['world'];h=c['host'];co=c['coordinator'];successor=None;other=None
                original=maintenance.promote
                if mode=='request':
                    with closing(sqlite3.connect(h.db_path)) as db:
                        db.execute("UPDATE actor_intents SET request='{}'");db.commit()
                before=logical(h.db_path);facts=tables(w.db_path,['snapshots','intents','coordinator_activations','coordinator_activation_blocks','coordinator_activation_outcomes'])
                def interleave(grant):
                    nonlocal successor,other
                    if mode=='root':
                        self.assertTrue(w.release_coordinator_execution(co._owner_attempt.permit));successor=w.acquire_coordinator_execution('maintenance-successor')
                    if mode=='host':co.actors=object()
                    if mode=='ref':grant.ref=replace(grant.ref,activation_id=uuid.uuid4().hex)
                    if mode=='actor':
                        with closing(sqlite3.connect(w.db_path)) as db:
                            db.execute('UPDATE actor_execution_leases SET owner_token=? WHERE person_id=?',(uuid.uuid4().hex,c['ref'].person_id));db.commit()
                    if mode=='verification':Path(c['repair']['verification_refs'][0]['path']).write_text('changed after attestation',encoding='utf-8')
                    if mode=='source':other=ActorHost(w,c['root']/'conflicting.sqlite')
                    return original(grant)
                try:
                    with patch.object(maintenance,'promote',interleave),self.assertRaises(WorldError) as caught:perform(c)
                    self.assertEqual(caught.exception.code,code)
                    self.assertEqual(logical(h.db_path),before);self.assertEqual(tables(w.db_path,list(facts)),facts)
                    TRACE.append({'case':'identity-rejection','mode':mode,'code':caught.exception.code,'original_activation_id':c['ref'].activation_id,'world_private_material_unchanged':True})
                finally:
                    co.actors=h
                    if other is not None:other.close()
                    if successor is not None:w.release_coordinator_execution(successor)


    def test_characterization_concurrent_maintenance_same_key_serializes_one_native_promotion(self):
        import threading
        with blocked_case('concurrent') as c:
            barrier=threading.Barrier(3,timeout=4);results=[];errors=[]
            def run():
                try:barrier.wait();results.append(perform(c))
                except BaseException as error:errors.append(error)
            threads=[threading.Thread(target=run,name='q50-m1-contender-'+str(n)) for n in range(2)]
            for t in threads:t.start()
            barrier.wait()
            for t in threads:t.join(6)
            self.assertFalse(any(t.is_alive() for t in threads));self.assertEqual(errors,[])
            self.assertEqual(sorted(r['replayed'] for r in results),[False,True])
            self.assertEqual(tables(Path(c['host'].db_path),['actor_session_heads'])['actor_session_heads'][0][2],1)
            rows=tables(c['world'].db_path,['coordinator_technical_maintenance'])['coordinator_technical_maintenance'];self.assertEqual(len(rows),1)
            self.assertEqual(c['world'].snapshot(),c['paused']);self.assertEqual(len(c['calls']),1)
            TRACE.append({'case':'two_actual_API_threads','threads_joined':True,'native_promotion_count':1,'replay_count':1,'locking':'pipeline admission serialization plus final SQLite CAS; not overlapping promotion transactions'})


    def test_committed_cleanup_pending_retries_original_obligation_then_readonly_replay(self):
        with blocked_case('cleanup-retry') as c:
            w=c['world'];h=c['host'];release=w.release_actor_execution;hits=[]
            def fail(permit):hits.append(permit);raise RuntimeError('controlled release failure after private commit')
            w.release_actor_execution=fail
            try:first=perform(c)
            finally:w.release_actor_execution=release
            self.assertFalse(first['replayed']);self.assertTrue(h.cleanup_pending);self.assertEqual(len(h._lease_cleanups),1)
            issuer,permit,attempt=h._lease_cleanups[0]
            self.assertIs(issuer,w);self.assertEqual(permit,hits[0]);self.assertFalse(attempt.thread.is_alive())
            before=tables(Path(h.db_path),['actor_sessions','actor_intents','actor_session_heads','actor_intent_execution'])
            replay=perform(c)
            self.assertTrue(replay['replayed']);self.assertFalse(h.cleanup_pending);self.assertEqual(h._lease_cleanups,[])
            self.assertEqual(tables(Path(h.db_path),list(before)),before)
            self.assertEqual(w.snapshot(),c['paused']);self.assertEqual(len(c['calls']),1)


    def test_characterization_final_cancel_deadline_rollback_and_source_wait_budget(self):
        import threading,time
        from app.actor_execution import ExecutionPolicy
        for mode,code in [('cancel','COORDINATOR_EXECUTION_CANCELLED'),('deadline','ACTOR_EXECUTION_DEADLINE')]:
            with self.subTest(mode=mode),blocked_case('final-'+mode) as c:
                w=c['world'];h=c['host'];check=w._check_actor_execution
                before=logical(h.db_path);facts=tables(w.db_path,['snapshots','intents','coordinator_activations','coordinator_activation_blocks','coordinator_activation_outcomes'])
                reached=[]
                def final(db,*a,**k):
                    phase=db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,)).fetchone()[0]
                    if phase=='completed':
                        reached.append(phase)
                        if mode=='cancel':c['coordinator']._owner_attempt.failure=WorldError(code,'controlled final root cancellation')
                        else:w._scoped_attempt().deadline=time.monotonic()-1
                    return check(db,*a,**k)
                w._check_actor_execution=final
                try:
                    with self.assertRaises(WorldError) as caught:perform(c)
                finally:w._check_actor_execution=check
                self.assertEqual(caught.exception.code,code);self.assertTrue(reached)
                self.assertEqual(logical(h.db_path),before);self.assertEqual(tables(w.db_path,list(facts)),facts)
        with blocked_case('finite-source-budget') as c:
            c['coordinator'].execution_policy=ExecutionPolicy(act_deadline_seconds=1,busy_budget_seconds=.2)
            entered=threading.Event();release=threading.Event()
            def hold():
                with c['world']._private_sources_lock:
                    entered.set();release.wait(4)
            t=threading.Thread(target=hold,name='q50-m1-source-holder');t.start();self.assertTrue(entered.wait(2))
            before=logical(c['host'].db_path);started=time.monotonic()
            try:
                with self.assertRaises(WorldError) as caught:perform(c)
                self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY');self.assertFalse(release.is_set());self.assertLess(time.monotonic()-started,.8)
            finally:release.set();t.join(3)
            self.assertFalse(t.is_alive());self.assertEqual(logical(c['host'].db_path),before);self.assertEqual(c['world'].snapshot(),c['paused'])

    def test_characterization_new_root_replay_preserves_original_fault_issuer_and_observer_is_readonly(self):
        with blocked_case('new-root') as c:
            first=perform(c);co=c['coordinator'];w=c['world'];oldroot=co._owner_attempt.permit.epoch
            before=tables(w.db_path,['coordinator_activation_blocks','coordinator_activations','coordinator_technical_maintenance'])
            co.close()
            h2=ActorHost(w,c['host'].db_path);co2=Coordinator(w,h2)
            try:
                self.assertEqual(tables(w.db_path,list(before)),before,'observer constructors must not upgrade schema')
                co2.acquire_owner();self.assertGreater(co2._owner_attempt.permit.epoch,oldroot)
                replay=co2.recover_technical(c['ref'],operation_id=c['operation'],repair_evidence=c['repair'])
                self.assertTrue(replay['replayed']);self.assertEqual(first['intent_id'],replay['intent_id']);self.assertEqual(tables(w.db_path,list(before)),before)
                self.assertEqual(w.snapshot(),c['paused']);self.assertEqual(set(replay),{'operation_id','activation_id','intent_id','phase','world_status','replayed'})
                TRACE.append({'case':'new-root-independent-authorization','original_parent_epoch':oldroot,'recovery_parent_epoch':co2._owner_attempt.permit.epoch,'historical_issuer_preserved':True,'readonly_replay':True})
            finally:co2.close()


    def test_loaded_maintenance_source_drift_is_rejected_before_actor_issuance(self):
        from app import coordinator_maintenance as maintenance
        with blocked_case('loaded-source-drift') as c:
            path=Path(maintenance.__file__);raw=path.read_bytes()
            before=logical(c['host'].db_path);facts=tables(c['world'].db_path,['snapshots','intents','coordinator_activations','coordinator_activation_blocks'])
            try:
                path.write_bytes(raw+b'\n# controlled on-disk drift after actual module import\n')
                with self.assertRaises(WorldError) as caught:perform(c)
                self.assertEqual(caught.exception.code,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
            finally:path.write_bytes(raw)
            self.assertEqual(logical(c['host'].db_path),before);self.assertEqual(tables(c['world'].db_path,list(facts)),facts)


    def test_original_completion_fault_still_present_must_abort_real_maintenance_transaction(self):
        for mode in ('host-finish','native-finish'):
            with self.subTest(mode=mode),blocked_case('still-failing-'+mode) as c:
                target=c['host'] if mode=='host-finish' else c['host']._storage
                name='_finish' if mode=='host-finish' else 'finish'
                original=getattr(target,name)
                def fault(*a,**k):raise RuntimeError('original completion remains broken')
                before=logical(c['host'].db_path);facts=tables(c['world'].db_path,['snapshots','intents','coordinator_activations','coordinator_activation_blocks','coordinator_activation_outcomes'])
                setattr(target,name,fault)
                try:
                    with self.assertRaisesRegex(RuntimeError,'original completion remains broken'):perform(c)
                finally:setattr(target,name,original)
                self.assertEqual(logical(c['host'].db_path),before);self.assertEqual(tables(c['world'].db_path,list(facts)),facts)
                self.assertFalse(perform(c)['replayed']);self.assertTrue(perform(c)['replayed'])


    def test_operator_verification_io_uses_original_finite_entry_from_first_hash(self):
        from app import coordinator_maintenance as maintenance
        from unittest.mock import patch
        with blocked_case('verification-entry') as c:
            original=maintenance.file_sha;entries=[]
            def observed(path):
                entries.append(c['world']._scoped_entry_budget())
                return original(path)
            with patch.object(maintenance,'file_sha',observed):result=perform(c)
            self.assertEqual(result['phase'],'completed');self.assertTrue(entries)
            self.assertTrue(all(entry is not None for entry in entries),'operator verification must not read files before its finite entry')
            self.assertTrue(all(entry is entries[0] for entry in entries),'verification must not renew its entry budget')


    def test_postcommit_cleanup_budget_failure_returns_committed_fact_not_rollback(self):
        import time
        with blocked_case('postcommit-budget') as c:
            w=c['world'];release=w.release_actor_execution
            def expired(permit):
                w._scoped_entry_budget().busy_deadline=time.monotonic()-1
                raise RuntimeError('controlled cleanup budget exhausted after commit')
            w.release_actor_execution=expired
            try:result=perform(c)
            finally:w.release_actor_execution=release
            self.assertEqual(result['phase'],'completed');self.assertFalse(result['replayed']);self.assertTrue(c['host'].cleanup_pending)
            self.assertEqual(tables(Path(c['host'].db_path),['actor_session_heads'])['actor_session_heads'][0][2],1)
            self.assertEqual(w.snapshot(),c['paused']);self.assertTrue(perform(c)['replayed']);self.assertFalse(c['host'].cleanup_pending)


def logical(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
        return list(db.iterdump())

