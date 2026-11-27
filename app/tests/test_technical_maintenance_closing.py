"""CLOSE tracers: actual bounded producers and original native consumer, SDK0."""
from contextlib import closing,contextmanager
from pathlib import Path
import hashlib,json,tempfile,threading,time,uuid
import sqlite3,unittest
from unittest.mock import patch
from app.tests import test_technical_maintenance as fixture
from app.tests import test_technical_maintenance_arch_a as prior
from app import coordinator_maintenance as maintenance
from app.world import WorldStore
PROOFS=[]

@contextmanager
def registered(label):
    from app.actors import ActorHost
    from app.server import Coordinator
    from app import coordinator_scheduler as scheduler
    root=Path(tempfile.mkdtemp(prefix='q50-close-'+label+'-',dir=fixture.SCRATCH))
    w=WorldStore(root/'world.sqlite');w.initialize();h=ActorHost(w,root/'actors.sqlite');co=Coordinator(w,h);calls=[]
    h._request=lambda payload:(calls.append(payload) or fixture.provider_fixture.ActorExecutionTests.result(payload))
    h._choose=lambda view,seen:({'action':'wait'},seen)
    try:
        owner=co.acquire_owner();w.control('resume',coordinator=owner);version={k:w.snapshot()[k] for k in ('world_id','revision')}
        boundary=w.inspect_scheduler_boundary(actor_db_path=h.db_path);w.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=h.db_path)
        window=w.open_or_recover_window(coordinator=owner,expected_version=version);row=w.next_activation(window['window_id'],coordinator=owner)
        ref=scheduler.ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
        proof=root/'verification.json';proof.write_text('{}',encoding='utf-8');project=Path(__file__).resolve().parents[2]
        repair=dict(repair_id=uuid.uuid4().hex,reason='controlled producer original fault removed',source_sha256={n:hashlib.sha256((project/n).read_bytes()).hexdigest() for n in maintenance.CORE},verification_refs=[dict(path=str(proof),sha256=hashlib.sha256(proof.read_bytes()).hexdigest())])
        yield dict(world=w,host=h,coordinator=co,ref=ref,root=root,operation=uuid.uuid4().hex,repair=repair,calls=calls)
    finally:co.close();h.close()

def state(c):
    w,h=c['world'],c['host']
    with closing(sqlite3.connect(w.db_path)) as db:
        row=db.execute('SELECT phase,private_closed,intent_id FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,)).fetchone()
        exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_technical_maintenance'").fetchone()
        history=db.execute('SELECT COUNT(*) FROM coordinator_technical_maintenance').fetchone()[0] if exists else 0
    with closing(sqlite3.connect(h.db_path)) as db:
        head=db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(c['ref'].person_id,)).fetchone()[0]
        consumed=db.execute('SELECT COUNT(*) FROM actor_intent_execution WHERE promoted_version IS NOT NULL').fetchone()[0]
    return dict(phase=row[0],private_closed=row[1],intent_id=row[2],head=head,history=history,consumed=consumed)

class RootWorldTests(unittest.TestCase):
    def drive(self,gate,swap=True):
        with fixture.blocked_case('close-root-world-'+gate+'-'+str(swap)) as c:
            prior.precondition(self,c);w,h,co=c['world'],c['host'],c['coordinator'];root=co._owner_attempt
            other=WorldStore(w.db_path,execution_clock_ns=w._execution_clock_ns)
            self.assertIs(root.world,w);self.assertIsNot(other,w);self.assertEqual(other.db_path,w.db_path)
            promote=maintenance.promote;finish=h._finish;native_finish=h._storage.finish;check=w._check_actor_execution;floor=w._execution_floor
            hits=[];native=[];active=[];error=None;result=None
            def mutate():hits.append(dict(gate=gate,same_root=co._owner_attempt is root));root.world=other if swap else w
            def observed_finish(*a,**k):native.append('actual-Host._finish');return finish(*a,**k)
            def before_promote(grant):
                self.assertIsNotNone(grant.actor)
                if gate=='after-issuance':mutate()
                return promote(grant)
            def issue_floor(db,now):
                floor(db,now)
                if gate=='issuance-precommit' and active and not hits:mutate()
            acquire=maintenance.acquire_actor
            def issue(grant):
                active.append(True)
                try:return acquire(grant)
                finally:active.clear()
            def before_native(db,*a,**k):
                if gate=='native':mutate()
                return native_finish(db,*a,**k)
            def final(db,*a,**k):
                phase=db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,)).fetchone()[0]
                if gate=='precommit' and phase=='completed' and not hits:mutate()
                return check(db,*a,**k)
            if gate=='initial':mutate()
            h._finish=observed_finish;h._storage.finish=before_native;w._check_actor_execution=final;w._execution_floor=issue_floor
            before_private=fixture.logical(h.db_path)
            try:
                with patch.object(maintenance,'promote',before_promote),patch.object(maintenance,'acquire_actor',issue):
                    try:result=fixture.perform(c)
                    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
            finally:root.world=w;h._finish=finish;h._storage.finish=native_finish;w._check_actor_execution=check;w._execution_floor=floor
            actual=state(c)
            PROOFS.append(dict(case='effect-root-world',gate=gate,swap=swap,root=str(c['root']),activation_id=c['ref'].activation_id,hits=hits,native=native,error=error,result=result,state=actual))
            self.assertTrue(hits,'mutation seam must actually execute')
            if swap:
                self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT')
                self.assertEqual({k:actual[k] for k in ('phase','private_closed','head','history','consumed')},dict(phase='technical_blocked',private_closed=0,head=0,history=0,consumed=0))
                self.assertEqual(fixture.logical(h.db_path),before_private)
            else:
                self.assertIsNone(error);self.assertTrue(native);self.assertEqual((actual['phase'],actual['head'],actual['history'],actual['consumed']),('completed',1,1,1))
                self.assertTrue(fixture.perform(c)['replayed'])
            self.assertEqual(w.snapshot(),c['paused'])
    def test_effect_root_world_binding_rejects_after_issuance(self):self.drive('after-issuance')
    def test_characterization_root_world_actual_issuance_native_and_precommit(self):
        for gate in ('initial','issuance-precommit','native','precommit'):
            with self.subTest(gate=gate):self.drive(gate)
    def test_characterization_original_host_native_same_world_positive(self):self.drive('after-issuance',False)
    def test_initial_root_world_rejects_before_material_or_issuance(self):
        with fixture.blocked_case('close-root-initial') as c:
            prior.precondition(self,c);w,co=c['world'],c['coordinator'];root=co._owner_attempt
            other=WorldStore(w.db_path,execution_clock_ns=w._execution_clock_ns);hits=[];read=maintenance.material;error=None
            def observed(*a,**k):hits.append('material');return read(*a,**k)
            before=fixture.logical(c['host'].db_path);root.world=other
            try:
                with patch.object(maintenance,'material',observed):
                    try:fixture.perform(c)
                    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
            finally:root.world=w
            PROOFS.append(dict(case='initial-root-world',root=str(c['root']),activation_id=c['ref'].activation_id,mutation_hit=True,material_hits=hits,error=error,state=state(c)))
            self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT');self.assertEqual(hits,[],'backend must reject object mismatch before material processing')
            self.assertEqual(fixture.logical(c['host'].db_path),before)

class AppendOnlyTests(unittest.TestCase):
    def probe(self,c,table):
        with closing(sqlite3.connect(c['world'].db_path)) as db:
            db.row_factory=sqlite3.Row;original=dict(db.execute('SELECT * FROM '+table).fetchone());columns=list(original)
            variants={'same':dict(original),'changed':dict(original)}
            content='witness_json' if table=='coordinator_fault_issuers' else 'record_json'
            variants['changed'][content]='changed replacement content'
            if table=='coordinator_technical_maintenance':
                variants['activation-unique']={**original,'operation_id':'1'*32,content:'different operation same activation'}
                variants['operation-primary']={**original,'activation_id':'2'*32,content:'same operation different activation'}
            for recursive in (0,1):
                db.execute('PRAGMA recursive_triggers='+str(recursive));self.assertEqual(db.execute('PRAGMA recursive_triggers').fetchone()[0],recursive)
                for label,row in variants.items():
                    db.execute('BEGIN');error=None
                    try:db.execute('INSERT OR REPLACE INTO '+table+' ('+','.join(columns)+') VALUES ('+','.join('?' for _ in columns)+')',tuple(row[k] for k in columns))
                    except sqlite3.IntegrityError as exc:error=str(exc)
                    visible=[dict(r) for r in db.execute('SELECT * FROM '+table)];db.rollback()
                    PROOFS.append(dict(case='appendonly-replace',table=table,variant=label,recursive=recursive,root=str(c['root']),activation_id=c['ref'].activation_id,rejected=error is not None,error=error,visible_preserved=visible==[original],rollback=True))
                    with self.subTest(table=table,recursive=recursive,variant=label):
                        self.assertIsNotNone(error,'BEFORE INSERT conflict gate must reject REPLACE with recursive triggers disabled or enabled')
                        self.assertEqual(visible,[original])
    def test_fault_issuer_conflict_replace_rejected(self):
        with fixture.blocked_case('close-fault-replace') as c:prior.precondition(self,c);self.probe(c,'coordinator_fault_issuers')
    def test_maintenance_history_conflict_replace_rejected(self):
        with fixture.blocked_case('close-history-replace') as c:
            prior.precondition(self,c);self.assertFalse(fixture.perform(c)['replayed']);self.probe(c,'coordinator_technical_maintenance')
    def test_characterization_update_delete_duplicate_and_plain_first_insert_replay(self):
        with fixture.blocked_case('close-all-sql') as c:
            prior.precondition(self,c);self.assertFalse(fixture.perform(c)['replayed']);self.assertTrue(fixture.perform(c)['replayed'])
            with closing(sqlite3.connect(c['world'].db_path)) as db:
                for table,content in (('coordinator_fault_issuers','witness_json'),('coordinator_technical_maintenance','record_json')):
                    original=db.execute('SELECT * FROM '+table).fetchall();self.assertEqual(len(original),1)
                    for recursive in (0,1):
                        db.execute('PRAGMA recursive_triggers='+str(recursive))
                        for sql in ('UPDATE '+table+" SET "+content+"='changed'",'DELETE FROM '+table,'INSERT INTO '+table+' SELECT * FROM '+table):
                            db.execute('BEGIN');error=None
                            try:db.execute(sql)
                            except sqlite3.IntegrityError as exc:error=str(exc)
                            self.assertIsNotNone(error);self.assertEqual(db.execute('SELECT * FROM '+table).fetchall(),original);db.rollback()
                            PROOFS.append(dict(case='appendonly-other-sql',root=str(c['root']),table=table,recursive=recursive,sql=sql,rejected=True,original_preserved=True,rollback=True))

class CharacterizationTests(unittest.TestCase):
    def test_characterization_two_trigger_fault_history_is_diagnostic_not_effect_authority(self):
        from app.world import WorldError
        with fixture.blocked_case('close-legacy-two-triggers') as c:
            prior.precondition(self,c);w,h,co=c['world'],c['host'],c['coordinator']
            with closing(sqlite3.connect(w.db_path)) as db:db.execute('DROP TRIGGER fault_issuer_no_replace');db.commit()
            with self.assertRaises(WorldError) as caught:h.act(c['ref'].person_id,coordinator=co._owner_attempt.permit,activation=c['ref'],pause_on_failure=True)
            self.assertEqual(caught.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
            before=(fixture.logical(w.db_path),fixture.logical(h.db_path))
            with self.assertRaises(WorldError) as caught:fixture.perform(c)
            self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN');self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before)
            self.assertEqual(state(c)['head'],0)
            PROOFS.append(dict(case='two-trigger-diagnostic-only',root=str(c['root']),activation_id=c['ref'].activation_id,ordinary_code='ACTIVATION_TECHNICAL_BLOCKED',effect_code=caught.exception.code,no_upgrade=True))
    def test_characterization_schema_missing_wrong_named_triggers_and_observers_never_upgrade(self):
        from app import coordinator_scheduler as scheduler
        from app.actors import ActorHost
        from app.server import Coordinator
        from app.world import WorldError
        for table,prefix,code in (('coordinator_fault_issuers','fault_issuer','WINDOW_RECOVERY_UNPROVEN'),('coordinator_technical_maintenance','technical_maintenance','TECHNICAL_MAINTENANCE_UNPROVEN')):
            for event in ('update','delete','replace'):
                for mode in ('missing','wrong-same-name'):
                    with self.subTest(table=table,event=event,mode=mode),fixture.blocked_case('close-schema-'+prefix+'-'+event+'-'+mode) as c:
                        prior.precondition(self,c)
                        if table=='coordinator_technical_maintenance':self.assertFalse(fixture.perform(c)['replayed'])
                        name=prefix+'_no_'+event;w,h=c['world'],c['host']
                        with closing(sqlite3.connect(w.db_path)) as db:
                            db.execute('DROP TRIGGER '+name)
                            if mode=='wrong-same-name':db.execute('CREATE TRIGGER '+name+' BEFORE INSERT ON '+table+' BEGIN SELECT 1; END')
                            db.commit()
                        before=(fixture.logical(w.db_path),fixture.logical(h.db_path));ww=WorldStore(w.db_path);observer=ActorHost(ww,h.db_path);cc=Coordinator(ww,observer)
                        try:self.assertEqual(ww.snapshot(),c['paused'])
                        finally:cc.close();observer.close()
                        self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before,'constructors cannot repair triggers')
                        with self.assertRaises(WorldError) as caught:fixture.perform(c)
                        self.assertEqual(caught.exception.code,code);self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before,'maintenance/replay cannot upgrade malformed history')
                        PROOFS.append(dict(case='strict-schema-observer-no-DDL',root=str(c['root']),activation_id=c['ref'].activation_id,table=table,event=event,mode=mode,error=caught.exception.code,source_trigger_missing_or_wrong_preserved=True))
    def test_characterization_captured_parent_scope_loss_cannot_downgrade_to_permit(self):
        from app import coordinator_scheduler as scheduler
        from app import world as worldmod
        for lost in (False,True):
            with self.subTest(lost=lost),registered('parent-scope-'+str(lost)) as c:
                w,h,co=c['world'],c['host'],c['coordinator'];root=co._owner_attempt;capture=scheduler.capture_fault_issuer;hits=[]
                def intercepted(world,db,row,execution,revision,proof):
                    attempt=w._scoped_attempt();observation=dict(captured_parent_is_root=attempt.parent_attempt is root,scope_is_root=w._scoped_coordinator_attempt() is root,lost=lost)
                    token=worldmod._coordinator_attempt_scope.set((w,root.permit,None)) if lost else None
                    observation['actual_scope_after_mutation_is_none']=w._scoped_coordinator_attempt() is None
                    try:return capture(world,db,row,execution,revision,proof)
                    except Exception as exc:observation['error']=getattr(exc,'code',type(exc).__name__);raise
                    finally:
                        hits.append(observation)
                        if token is not None:worldmod._coordinator_attempt_scope.reset(token)
                finish=h._finish
                def fault(*a,**k):raise RuntimeError('captured parent original native fault')
                h._finish=fault
                try:
                    with co._owner_scope(root),patch.object(scheduler,'capture_fault_issuer',intercepted),self.assertRaisesRegex(RuntimeError,'captured parent original native fault'):
                        h.act(c['ref'].person_id,coordinator=root.permit,activation=c['ref'],pause_on_failure=True)
                finally:h._finish=finish
                self.assertTrue(hits);self.assertTrue(all(x['captured_parent_is_root'] and x['scope_is_root'] for x in hits))
                if lost:
                    self.assertTrue(all(x['actual_scope_after_mutation_is_none'] and x.get('error')=='WINDOW_RECOVERY_UNPROVEN' for x in hits))
                    self.assertEqual(w.snapshot()['status'],'running')
                    with closing(sqlite3.connect(w.db_path)) as db:
                        self.assertEqual(db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,)).fetchone()[0],'world_committed')
                        self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_fault_issuers'").fetchone())
                else:
                    prior.precondition(self,c);c['paused']=w.snapshot();self.assertFalse(fixture.perform(c)['replayed'])
                PROOFS.append(dict(case='captured-parent-scope',root=str(c['root']),activation_id=c['ref'].activation_id,lost=lost,hits=hits))
    def test_characterization_unsupported_phases_have_actual_current_material(self):
        from app.world import WorldError
        for mode in ('pending','prepared','candidate','rejected','world_committed','completed'):
            with self.subTest(mode=mode),registered('unsupported-'+mode) as c:
                w,h,co=c['world'],c['host'],c['coordinator'];target={'pending':'_act','prepared':'_request','candidate':'_submit','rejected':'_reject_result','world_committed':'_finish'}.get(mode)
                if mode=='rejected':
                    location=next(p['location'] for p in w.snapshot()['persons'] if p['id']==c['ref'].person_id)
                    h._choose=lambda view,seen:({'action':'travel','to':location},seen)
                original=getattr(h,target) if target else None
                def fault(*a,**k):raise RuntimeError('actual unsupported material '+mode)
                if target:setattr(h,target,fault)
                try:
                    if target:
                        with self.assertRaisesRegex(RuntimeError,'actual unsupported material '+mode):h.act(c['ref'].person_id,coordinator=co._owner_attempt.permit,activation=c['ref'],pause_on_failure=mode!='world_committed')
                    else:h.act(c['ref'].person_id,coordinator=co._owner_attempt.permit,activation=c['ref'],pause_on_failure=True)
                finally:
                    if target:setattr(h,target,original)
                row=prior.read(w.db_path,'SELECT * FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,))[0]
                if mode in ('world_committed','completed'):self.assertEqual(row['phase'],mode)
                else:self.assertEqual((row['phase'],row['resume_phase']),('technical_blocked',mode))
                before=(fixture.logical(w.db_path),fixture.logical(h.db_path));paused=w.snapshot()
                with self.assertRaises(WorldError) as caught:fixture.perform(c)
                self.assertEqual(caught.exception.code,'TECHNICAL_MAINTENANCE_UNSUPPORTED_PHASE')
                self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before);self.assertEqual(w.snapshot(),paused)
                PROOFS.append(dict(case='actual-unsupported-phase',mode=mode,root=str(c['root']),activation_id=c['ref'].activation_id,actual_row=row,error=caught.exception.code,source='real Host producer; phase/witness never hand-seeded'))
    def test_characterization_actual_close_interrupts_entry_and_source_wait_without_effect(self):
        for gate in ('entry','source'):
            with self.subTest(gate=gate),fixture.blocked_case('close-wait-'+gate) as c:
                prior.precondition(self,c);w,h,co=c['world'],c['host'],c['coordinator'];observed=threading.Event();errors=[];entries=[]
                original=co.lock if gate=='entry' else w._private_sources_lock
                class LockWitness:
                    def acquire(inner,*a,**k):
                        taken=original.acquire(*a,**k)
                        if not taken:entries.append(w._scoped_entry_budget());observed.set()
                        return taken
                    def release(inner):return original.release()
                if gate=='entry':co.lock=LockWitness()
                else:w._private_sources_lock=LockWitness()
                before=fixture.logical(h.db_path);facts=fixture.tables(w.db_path,['snapshots','intents','coordinator_activations','coordinator_activation_blocks','coordinator_fault_issuers'])
                def recover():
                    try:fixture.perform(c)
                    except Exception as exc:errors.append(getattr(exc,'code',type(exc).__name__))
                worker=threading.Thread(target=recover,name='q50-close-actual-'+gate+'-wait');original.acquire();worker.start()
                try:
                    self.assertTrue(observed.wait(2),'real acquire(timeout) must have returned false while original lock held')
                    started=time.monotonic();co.close();elapsed=time.monotonic()-started;worker.join(3)
                    self.assertFalse(worker.is_alive());self.assertEqual(errors,['ACTOR_EXECUTION_CANCELLED'])
                    self.assertTrue(entries);self.assertTrue(all(e is entries[0] and e is not None for e in entries));self.assertLess(elapsed,2)
                finally:
                    original.release();worker.join(3)
                    if gate=='entry':co.lock=original
                    else:w._private_sources_lock=original
                self.assertEqual(fixture.logical(h.db_path),before);self.assertEqual(fixture.tables(w.db_path,list(facts)),facts);self.assertEqual(w.snapshot(),c['paused'])
                PROOFS.append(dict(case='actual-close-during-wait',gate=gate,root=str(c['root']),activation_id=c['ref'].activation_id,actual_lock_failed_acquires=len(entries),actual_close=True,same_entry=True,error=errors,worker_joined=not worker.is_alive(),seconds=elapsed))
