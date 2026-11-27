"""ARCH-A only: genuine producers, four distinct actor roles, SDK0."""
from contextlib import closing
from pathlib import Path
import hashlib,json,sqlite3,unittest,uuid
from app.tests import test_technical_maintenance as fixture
from app.world import WorldError
from app import coordinator_scheduler as scheduler
from app.actor_execution import canonical,digest
from contextlib import contextmanager
import importlib,shutil,sys,types
PROOFS=[]

@contextmanager
def isolated_runtime(label):
    """Full current scope copied and sealed BEFORE importing any alias core."""
    project=Path(__file__).resolve().parents[2]
    mp=project/'manifest.json'
    if not mp.exists():mp=project/'app/evidence/q50-m1-arch-a-parent-baseline-source/manifest.json'
    source=json.loads(mp.read_text(encoding='utf-8'));paths=set(source['sha256'])
    for n in ('app/source_attestation.py','app/tests/test_technical_maintenance_arch_a.py','app/evidence/q50-m1-arch-a-cycle.py'):
        if (project/n).exists():paths.add(n)
    destination=project/'app/evidence'/('q50-m1-arch-a-isolation-'+label+'-'+uuid.uuid4().hex)
    destination.mkdir();hashes={}
    for n in sorted(paths):
        target=destination/n;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(project/n,target)
        hashes[n]=hashlib.sha256(target.read_bytes()).hexdigest()
        assert hashes[n]==hashlib.sha256((project/n).read_bytes()).hexdigest()
    frozen=dict(sha256=hashes,source_files=len(hashes),inherited_manifest=dict(path=str(mp),sha256=hashlib.sha256(mp.read_bytes()).hexdigest(),scope='immutable references and historical copies inherited exactly; no full historical rescan'),stage_accepted=False)
    manifest=destination/'manifest.json';manifest.write_text(json.dumps(frozen,ensure_ascii=False,indent=2),encoding='utf-8')
    name='arch_a_'+uuid.uuid4().hex;package=types.ModuleType(name);package.__path__=[str(destination/'app')];package.__package__=name;sys.modules[name]=package
    PROOFS.append(dict(case='isolated-runtime-source',label=label,root=str(destination),manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),source_files=len(hashes)))
    try:yield name,destination
    finally:
        for key in list(sys.modules):
            if key==name or key.startswith(name+'.'):sys.modules.pop(key,None)

@contextmanager
def alias_blocked(name,destination,label,coordinator_class=None):
    """Actual aliased classes, real producer, no fabricated phase/witness."""
    import tempfile
    worldmod=importlib.import_module(name+'.world');actorsmod=importlib.import_module(name+'.actors');servermod=importlib.import_module(name+'.server');sched=importlib.import_module(name+'.coordinator_scheduler')
    root=Path(tempfile.mkdtemp(prefix='q50-arch-'+label+'-',dir=fixture.SCRATCH))
    w=worldmod.WorldStore(root/'world.sqlite');w.initialize();h=actorsmod.ActorHost(w,root/'actors.sqlite');co=(coordinator_class or servermod.Coordinator)(w,h);calls=[]
    h._request=lambda payload:(calls.append(payload) or fixture.provider_fixture.ActorExecutionTests.result(payload));h._choose=lambda view,seen:({'action':'wait'},seen)
    try:
        owner=co.acquire_owner();w.control('resume',coordinator=owner);version={k:w.snapshot()[k] for k in ('world_id','revision')}
        boundary=w.inspect_scheduler_boundary(actor_db_path=h.db_path);w.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=h.db_path)
        window=w.open_or_recover_window(coordinator=owner,expected_version=version);row=w.next_activation(window['window_id'],coordinator=owner)
        ref=sched.ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')});finish=h._finish
        def fault(*a,**k):raise RuntimeError('alias original finish fault')
        h._finish=fault
        try:
            try:h.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except RuntimeError as error:assert str(error)=='alias original finish fault'
            else:raise AssertionError('actual fault must be reached')
        finally:h._finish=finish
        proof=root/'verified.json';proof.write_text('{}',encoding='utf-8')
        repair=dict(repair_id=uuid.uuid4().hex,reason='aliased original consumer restored',source_sha256={n:hashlib.sha256((destination/n).read_bytes()).hexdigest() for n in ('app/actors.py','app/actor_execution.py','app/coordinator_scheduler.py','app/server.py','app/world.py')},verification_refs=[dict(path=str(proof),sha256=hashlib.sha256(proof.read_bytes()).hexdigest())])
        yield dict(world=w,host=h,coordinator=co,ref=ref,root=root,paused=w.snapshot(),operation=uuid.uuid4().hex,repair=repair,calls=calls)
    finally:co.close();h.close()

class LoadedSourceTests(unittest.TestCase):
    def test_characterization_same_source_alias_and_real_main_class_are_authorized(self):
        from unittest.mock import patch
        import argparse,runpy
        for mode in ('alias','__main__'):
            with self.subTest(mode=mode),isolated_runtime(mode) as (name,copy):
                cls=None
                if mode=='__main__':
                    capture=[]
                    class StopBeforeStartup(Exception):pass
                    def stop_parser(*a,**k):
                        import inspect
                        capture.append(inspect.currentframe().f_back.f_globals['Coordinator'])
                        raise StopBeforeStartup()
                    with patch.object(argparse.ArgumentParser,'parse_args',stop_parser),self.assertRaises(StopBeforeStartup):runpy.run_module(name+'.server',run_name='__main__')
                    cls=capture[0];self.assertEqual(cls.__module__,'__main__')
                with alias_blocked(name,copy,mode,coordinator_class=cls) as c:
                    precondition(self,c);self.assertFalse(fixture.perform(c)['replayed']);self.assertTrue(fixture.perform(c)['replayed'])
                    self.assertEqual(c['world'].snapshot(),c['paused']);self.assertEqual(len(c['calls']),1)
                    PROOFS.append(dict(case='same-source-'+mode,root=str(c['root']),activation_id=c['ref'].activation_id,server_class=type(c['coordinator']).__module__))

    def test_characterization_late_module_name_replacement_does_not_rebind_old_class_generation(self):
        import importlib.util
        with isolated_runtime('old-object') as (name,copy):
            original=importlib.import_module(name+'.server');oldclass=original.Coordinator
            path=copy/'app/server.py';raw=path.read_bytes();path.write_bytes(raw+b'\n# distinct later source generation\n')
            try:
                spec=importlib.util.spec_from_file_location(name+'.server_successor',path);later=importlib.util.module_from_spec(spec);sys.modules[spec.name]=later;spec.loader.exec_module(later)
                sys.modules[name+'.server']=later
                with alias_blocked(name,copy,'old-object',coordinator_class=oldclass) as c:
                    precondition(self,c);before=(fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path))
                    error=None
                    try:fixture.perform(c)
                    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
                    self.assertEqual(error,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
                    self.assertEqual((fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path)),before)
                    PROOFS.append(dict(case='actual-old-class-not-sys-modules',root=str(c['root']),activation_id=c['ref'].activation_id,rejected=error))
            finally:path.write_bytes(raw)

    def test_characterization_loaded_bytes_sampling_gap_and_internal_dependency_drift_reject(self):
        import importlib.util
        for mode in ('load-sample-gap','internal-dependency'):
            with self.subTest(mode=mode),isolated_runtime(mode) as (name,copy):
                path=copy/('app/server.py' if mode=='load-sample-gap' else 'app/execution.py');raw=path.read_bytes()
                if mode=='load-sample-gap':
                    # Execute original module bytes while register samples a newer
                    # disk body. No loader hook, reload or running service.
                    mod=types.ModuleType(name+'.server');mod.__file__=str(path);mod.__package__=name;sys.modules[mod.__name__]=mod
                    old=b'return recover(self, ref, operation_id=operation_id, repair_evidence=repair_evidence)'
                    path.write_bytes(raw.replace(old,b"raise RuntimeError('unloaded generation')"))
                    exec(compile(raw,str(path),'exec'),vars(mod))
                else:
                    importlib.import_module(name+'.actors');path.write_bytes(raw+b'\n# drift in internally bound permit dependency\n')
                try:
                    with alias_blocked(name,copy,mode) as c:
                        precondition(self,c);before=(fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path));error=None
                        try:fixture.perform(c)
                        except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
                        self.assertEqual(error,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
                        self.assertEqual((fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path)),before)
                        PROOFS.append(dict(case=mode,root=str(c['root']),activation_id=c['ref'].activation_id,rejected=error))
                finally:path.write_bytes(raw)

    def test_loaded_core_before_first_maintenance_import_rejects_later_disk_bytes(self):
        with isolated_runtime('late-maintenance') as (name,copy):
            server=importlib.import_module(name+'.server');importlib.import_module(name+'.actors');importlib.import_module(name+'.world')
            self.assertNotIn(name+'.coordinator_maintenance',sys.modules)
            loaded=server.Coordinator.recover_technical.__code__;path=copy/'app/server.py';raw=path.read_bytes()
            old=b'return recover(self, ref, operation_id=operation_id, repair_evidence=repair_evidence)'
            self.assertEqual(raw.count(old),1)
            path.write_bytes(raw.replace(old,b"raise RuntimeError('unloaded server disk generation')"))
            try:
                with alias_blocked(name,copy,'late-import') as c:
                    precondition(self,c)
                    self.assertIs(server.Coordinator.recover_technical.__code__,loaded)
                    before=(fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path))
                    error=None;result=None
                    try:result=fixture.perform(c)
                    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
                    PROOFS.append(dict(case='late-maintenance-loaded-core',root=str(c['root']),activation_id=c['ref'].activation_id,rejected=error,result=result,loaded_source_sha256=hashlib.sha256(raw).hexdigest(),disk_sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
                    self.assertEqual(error,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED','old loaded method must not inherit first lazy maintenance import disk hash')
                    self.assertEqual((fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path)),before)
            finally:path.write_bytes(raw)

def read(path,sql,args=()):
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        return [dict(r) for r in db.execute(sql,args)]

def precondition(test,c):
    test.assertEqual(c['world'].snapshot()['status'],'technical_paused')
    row=read(c['world'].db_path,'SELECT * FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,))[0]
    test.assertEqual((row['phase'],row['resume_phase'],row['private_closed']),('technical_blocked','world_committed',0))
    test.assertEqual(read(c['host'].db_path,'SELECT version FROM actor_session_heads WHERE person_id=?',(c['ref'].person_id,))[0]['version'],0)
    return row

def replace_witness(c,edit,*,noncanonical=False):
    with closing(sqlite3.connect(c['world'].db_path)) as db:
        row=db.execute('SELECT witness_json FROM coordinator_fault_issuers').fetchone()
        witness=json.loads(row[0]);edit(witness)
        raw=json.dumps(witness,ensure_ascii=False,sort_keys=True,indent=2) if noncanonical else canonical(witness)
        db.execute('DROP TRIGGER fault_issuer_no_update')
        db.execute('UPDATE coordinator_fault_issuers SET witness_json=?,witness_sha256=?',(raw,digest(raw)))
        db.execute("CREATE TRIGGER fault_issuer_no_update BEFORE UPDATE ON coordinator_fault_issuers BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END")
        db.commit()

class ReplayIdentityTests(unittest.TestCase):
    def test_characterization_postaudit_actual_source_drift_is_rejected_not_preempted(self):
        from unittest.mock import patch
        with isolated_runtime('replay-source') as (name,copy):
            with alias_blocked(name,copy,'replay-source') as c:
                precondition(self,c);self.assertFalse(fixture.perform(c)['replayed'])
                sched=importlib.import_module(name+'.coordinator_scheduler');audit=sched.native_audit
                path=copy/'app/server.py';raw=path.read_bytes();hits=[]
                before=(fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path))
                def post_audit(storage,db,row):
                    result=audit(storage,db,row)
                    if row['phase']=='completed' and not hits:
                        hits.append('real-native-audit-returned');path.write_bytes(raw+b'\n# source drift strictly after native replay audit\n')
                    return result
                error=None
                try:
                    with patch.object(sched,'native_audit',post_audit):
                        try:fixture.perform(c)
                        except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
                finally:path.write_bytes(raw)
                self.assertTrue(hits);self.assertEqual(error,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
                self.assertEqual((fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path)),before)
                PROOFS.append(dict(case='post-native-source-drift',root=str(c['root']),activation_id=c['ref'].activation_id,hits=hits,error=error))

    def test_characterization_postaudit_storage_world_root_source_and_remaining_entry(self):
        from app.actors import ActorHost
        from app.world import WorldStore
        from app.coordinator_execution import CoordinatorAttempt
        from app import coordinator_maintenance as maintenance
        from unittest.mock import patch
        import contextvars,time
        codes={'storage':'ACTIVATION_BINDING_CONFLICT','world':'ACTIVATION_BINDING_CONFLICT','root':'ACTIVATION_BINDING_CONFLICT','root-world':'ACTIVATION_BINDING_CONFLICT','file-capture':'ACTIVATION_BINDING_CONFLICT','source':'WINDOW_RECOVERY_UNPROVEN','cancel':'COORDINATOR_EXECUTION_CANCELLED','entry':'EXECUTION_STORAGE_BUSY','same-file-positive':None}
        for mode,expected in codes.items():
            with self.subTest(mode=mode),fixture.blocked_case('arch-replay-'+mode) as c:
                precondition(self,c);self.assertFalse(fixture.perform(c)['replayed'])
                w,h,co=c['world'],c['host'],c['coordinator'];root=co._owner_attempt;storage=h._storage
                other=ActorHost(w,h.db_path);otherworld=WorldStore(w.db_path);otherroot=CoordinatorAttempt(w,root.permit,co.execution_policy)
                captured_file=storage.actor_file;audit=scheduler.native_audit;hits=[];conflicts=[]
                before=(fixture.logical(w.db_path),fixture.logical(h.db_path))
                def post_audit(s,db,row):
                    result=audit(s,db,row)
                    if row['phase']=='completed' and not hits:
                        hits.append('real-native-audit-returned')
                        if mode=='storage':h._storage=other._storage
                        elif mode=='world':co.world=otherworld
                        elif mode=='root':co._owner_attempt=otherroot
                        elif mode=='root-world':root.world=otherworld
                        elif mode=='file-capture':storage.actor_file=(captured_file[0],captured_file[1]+1)
                        elif mode=='source':
                            # A genuine observer constructor in its independent
                            # empty context, same thread/source lock, no SQL/root.
                            conflicts.append(contextvars.Context().run(ActorHost,w,c['root']/'conflicting.sqlite'))
                        elif mode=='cancel':root.failure=WorldError(expected,'post-audit root cancellation')
                        elif mode=='entry':w._scoped_entry_budget().busy_deadline=time.monotonic()-1
                    return result
                def forbidden(*a,**k):raise AssertionError('no replay lease/grant/promotion')
                error=None;result=None
                try:
                    with patch.object(scheduler,'native_audit',post_audit),patch.object(maintenance,'acquire_actor',forbidden),patch.object(maintenance,'promote',forbidden):
                        try:result=fixture.perform(c)
                        except WorldError as exc:error=exc.code
                finally:
                    co.world=w;co._owner_attempt=root;root.world=w;root.failure=None;h._storage=storage;storage.actor_file=captured_file
                    for conflict in conflicts:conflict.close()
                    other.close()
                PROOFS.append(dict(case='post-native-replay',mode=mode,root=str(c['root']),activation_id=c['ref'].activation_id,hits=hits,error=error,result=result))
                self.assertTrue(hits);self.assertEqual(error,expected)
                if expected is None:self.assertTrue(result['replayed'])
                self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before)

    def test_replay_rejects_actual_host_replacement_after_real_native_audit(self):
        from app import coordinator_maintenance as maintenance
        from app.actors import ActorHost
        from unittest.mock import patch
        with fixture.blocked_case('arch-replay-host') as c:
            precondition(self,c);self.assertFalse(fixture.perform(c)['replayed'])
            w,h,co=c['world'],c['host'],c['coordinator'];audit=scheduler.native_audit;hits=[]
            # Prepare the real same-file Host outside the readonly transaction.
            # Constructing it inside root scope/native audit may legitimately try
            # initialization under the held read lock: that BUSY is not this gate.
            others=[ActorHost(w,h.db_path)]
            before=(fixture.logical(w.db_path),fixture.logical(h.db_path))
            def post_audit(storage,db,row):
                result=audit(storage,db,row)
                if row['phase']=='completed' and not hits:
                    co.actors=others[0];hits.append('real-native-audit-completed-before-Host-swap')
                return result
            def forbidden(*a,**k):raise AssertionError('readonly replay has no effect capability or promotion')
            error=None;result=None
            try:
                with patch.object(scheduler,'native_audit',post_audit),patch.object(maintenance,'acquire_actor',forbidden),patch.object(maintenance,'promote',forbidden):
                    try:result=fixture.perform(c)
                    except WorldError as exc:error=exc.code
            finally:
                co.actors=h
                for other in others:other.close()
            PROOFS.append(dict(case='post-native-replay-host',root=str(c['root']),activation_id=c['ref'].activation_id,hits=hits,error=error,result=result))
            self.assertTrue(hits);self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT','actual Host identity must hold at the real readonly return boundary')
            self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before)

class IssuerModelTests(unittest.TestCase):
    def test_v1_fault_witness_requires_exact_shape_version_and_strict_types(self):
        edits={
            'extra_top':lambda w:w.update(extra=True),
            'bool_version':lambda w:w.update(schema_version=True),
            'extra_binding':lambda w:w['binding'].update(extra=True),
            'bool_binding_epoch':lambda w:w['binding'].update(owner_actor_epoch=True),
            'bool_parent_epoch':lambda w:w['parent'].__setitem__(0,True),
            'bool_meta_base':lambda w:w['meta'].update(base_version=False),
            'bool_meta_owner':lambda w:w['meta'].update(owner_epoch=True),
            'unknown_version':lambda w:w.update(schema_version=2),
            'noncanonical':lambda w:None,
        }
        for mode,edit in edits.items():
            with self.subTest(mode=mode),fixture.blocked_case('arch-shape-'+mode) as c:
                precondition(self,c);replace_witness(c,edit,noncanonical=mode=='noncanonical')
                before=(fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path))
                error=None
                try:fixture.perform(c)
                except WorldError as exc:error=exc.code
                PROOFS.append(dict(case='strict-v1',mode=mode,error=error,root=str(c['root']),activation_id=c['ref'].activation_id))
                self.assertEqual(error,'WINDOW_RECOVERY_UNPROVEN')
                self.assertEqual((fixture.logical(c['world'].db_path),fixture.logical(c['host'].db_path)),before)

    def test_business_A_adopted_fault_B_claim_C_and_maintenance_D_have_distinct_recorded_identity(self):
        from app.actors import ActorHost
        from app.server import Coordinator
        from app.world import WorldStore
        import tempfile
        root=Path(tempfile.mkdtemp(prefix='q50-arch-four-actors-',dir=fixture.SCRATCH))
        w=WorldStore(root/'world.sqlite');w.initialize();h=ActorHost(w,root/'actors.sqlite');co=Coordinator(w,h)
        calls=[];observed=[]
        h._request=lambda payload:(calls.append(payload) or fixture.provider_fixture.ActorExecutionTests.result(payload))
        h._choose=lambda view,seen:({'action':'wait'},seen)
        finish=h._finish
        def fault(*args,**kwargs):
            observed.append(dict(epoch=h._attempt.permit.epoch,token=h._attempt.permit.owner_token,attempt=h._attempt.id,parent=h._attempt.parent_attempt is co._owner_attempt))
            raise RuntimeError('four-actor finish fault')
        try:
            owner=co.acquire_owner();w.control('resume',coordinator=owner)
            version={k:w.snapshot()[k] for k in ('world_id','revision')}
            boundary=w.inspect_scheduler_boundary(actor_db_path=h.db_path)
            w.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=h.db_path)
            window=w.open_or_recover_window(coordinator=owner,expected_version=version)
            row=w.next_activation(window['window_id'],coordinator=owner)
            ref=scheduler.ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
            h._finish=fault
            with co._owner_scope(co._owner_attempt):
                with self.assertRaisesRegex(RuntimeError,'four-actor finish fault'):h.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=False)
            self.assertEqual(w.snapshot()['status'],'running')
            business=read(w.db_path,'SELECT * FROM intents')
            with co._owner_scope(co._owner_attempt):
                with self.assertRaisesRegex(RuntimeError,'four-actor finish fault'):h.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            h._finish=finish
            c=dict(world=w,host=h,coordinator=co,ref=ref,root=root,paused=w.snapshot(),operation=uuid.uuid4().hex)
            row=precondition(self,c);witness=read(w.db_path,'SELECT * FROM coordinator_fault_issuers')[0]
            old=json.loads(witness['witness_json']);self.assertEqual(len(calls),1);self.assertEqual(len(observed),2)
            self.assertTrue(all(x['parent'] for x in observed));self.assertLess(observed[0]['epoch'],observed[1]['epoch'])
            self.assertEqual(old['meta']['owner_epoch'],observed[1]['epoch']);self.assertEqual(old['meta']['owner_token'],observed[1]['token']);self.assertEqual(old['meta']['attempt_id'],observed[1]['attempt'])
            permit=w.acquire_actor_execution(ref.person_id,'four-actor-C',coordinator=owner,activation=ref)
            w.release_actor_execution(permit)
            before=read(w.db_path,'SELECT * FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,))[0]
            proof=root/'verified.json';proof.write_text('{}',encoding='utf-8');project=Path(__file__).resolve().parents[2]
            c['repair']=dict(repair_id=uuid.uuid4().hex,reason='original finish restored',source_sha256={n:hashlib.sha256((project/n).read_bytes()).hexdigest() for n in ('app/actors.py','app/actor_execution.py','app/coordinator_scheduler.py','app/server.py','app/world.py')},verification_refs=[dict(path=str(proof),sha256=hashlib.sha256(proof.read_bytes()).hexdigest())])
            self.assertFalse(fixture.perform(c)['replayed'])
            record=json.loads(read(w.db_path,'SELECT * FROM coordinator_technical_maintenance')[0]['record_json'])
            PROOFS.append(dict(case='four-actors',root=str(root),activation_id=ref.activation_id,observed=observed,claim_C=permit.epoch,record=record))
            self.assertEqual(record['original_row'],before)
            self.assertIn('original_fault_issuer',record,'maintenance history must separately retain fault B and pre-maintenance current owner C')
            self.assertEqual(record['original_fault_issuer'],old)
            self.assertGreater(record['recovery_actor_epoch'],permit.epoch)
            self.assertEqual(read(w.db_path,'SELECT * FROM intents'),business)
            self.assertEqual(read(w.db_path,'SELECT * FROM coordinator_fault_issuers')[0],witness)
            self.assertEqual(w.snapshot(),c['paused'])
        finally:h._finish=finish;co.close();h.close()

    def test_maintenance_actor_issuance_commit_rechecks_fresh_current_row(self):
        from app import coordinator_maintenance as maintenance
        from unittest.mock import patch
        with fixture.blocked_case('arch-issuance-final') as c:
            precondition(self,c);w=c['world'];original=w._execution_floor;acquire=maintenance.acquire_actor;inside=[]
            before=(fixture.logical(w.db_path),fixture.logical(c['host'].db_path))
            def floor(db,now):
                original(db,now)
                if inside:
                    inside.append('fault-in-real-issuance-transaction')
                    db.execute('UPDATE coordinator_activations SET owner_actor_epoch=999 WHERE activation_id=?',(c['ref'].activation_id,))
            def observed(grant):
                inside.append('maintenance-acquire')
                try:return acquire(grant)
                finally:inside.clear()
            w._execution_floor=floor
            try:
                with patch.object(maintenance,'acquire_actor',observed),self.assertRaises(WorldError) as error:fixture.perform(c)
            finally:w._execution_floor=original
            self.assertEqual(error.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            self.assertEqual((fixture.logical(w.db_path),fixture.logical(c['host'].db_path)),before,'fresh material rejection must rollback issuance, not merely fail later native work')
            PROOFS.append(dict(case='issuance-final-row',root=str(c['root']),activation_id=c['ref'].activation_id,code=error.exception.code))

    def test_characterization_native_and_precommit_reread_fault_metadata_and_rollback(self):
        for gate in ('native','precommit'):
            with self.subTest(gate=gate),fixture.blocked_case('arch-'+gate) as c:
                precondition(self,c);w,h=c['world'],c['host'];hits=[]
                before=fixture.logical(h.db_path)
                facts=fixture.tables(w.db_path,['snapshots','intents','coordinator_activations','coordinator_activation_blocks','coordinator_fault_issuers','coordinator_windows'])
                original=h._storage.finish if gate=='native' else w._check_actor_execution
                def corrupt(db):
                    hits.append(gate)
                    db.execute('UPDATE actor.actor_intent_execution SET owner_token=?',(uuid.uuid4().hex,))
                def native(db,*a,**k):corrupt(db);return original(db,*a,**k)
                def final(db,*a,**k):
                    phase=db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,)).fetchone()[0]
                    if phase=='completed':corrupt(db)
                    return original(db,*a,**k)
                if gate=='native':h._storage.finish=native
                else:w._check_actor_execution=final
                try:
                    with self.assertRaises(WorldError) as error:fixture.perform(c)
                finally:
                    if gate=='native':h._storage.finish=original
                    else:w._check_actor_execution=original
                self.assertTrue(hits);self.assertEqual(error.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                self.assertEqual(fixture.logical(h.db_path),before);self.assertEqual(fixture.tables(w.db_path,list(facts)),facts)
                self.assertEqual(w.snapshot(),c['paused'])
                PROOFS.append(dict(case='native-final-rollback',gate=gate,hits=hits,root=str(c['root']),activation_id=c['ref'].activation_id,code=error.exception.code))

    def test_characterization_legacy_missing_issuer_preserves_ordinary_blocked_but_never_mints_maintenance(self):
        with fixture.blocked_case('arch-legacy') as c:
            precondition(self,c);w,h=c['world'],c['host']
            with closing(sqlite3.connect(w.db_path)) as db:db.execute('DROP TABLE coordinator_fault_issuers');db.commit()
            with self.assertRaises(WorldError) as error:h.act(c['ref'].person_id,coordinator=c['coordinator']._owner_attempt.permit,activation=c['ref'],pause_on_failure=True)
            self.assertEqual(error.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
            before=(fixture.logical(w.db_path),fixture.logical(h.db_path))
            with self.assertRaises(WorldError) as error:fixture.perform(c)
            self.assertEqual(error.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before)
            PROOFS.append(dict(case='unknown-legacy',root=str(c['root']),activation_id=c['ref'].activation_id))

    def test_fault_witness_replace_cannot_bypass_appendonly_triggers(self):
        with fixture.blocked_case('arch-replace-appendonly') as c:
            precondition(self,c);w=c['world'];attempts=[]
            before=fixture.logical(w.db_path)
            with closing(sqlite3.connect(w.db_path)) as db:
                recursive=db.execute('PRAGMA recursive_triggers').fetchone()[0]
                try:
                    db.execute('INSERT OR REPLACE INTO coordinator_fault_issuers SELECT * FROM coordinator_fault_issuers')
                except sqlite3.IntegrityError as error:attempts.append(str(error))
                finally:db.rollback()
            PROOFS.append(dict(case='appendonly-replace',root=str(c['root']),activation_id=c['ref'].activation_id,recursive_triggers=recursive,rejected=bool(attempts),errors=attempts))
            self.assertTrue(attempts,'fault witness append-only SQL must reject REPLACE; trusted publisher uses INSERT, but the declared trigger invariant must not silently depend on recursive_triggers')
            self.assertEqual(fixture.logical(w.db_path),before)

    def test_characterization_fault_schema_and_appendonly_enforcement(self):
        with fixture.blocked_case('arch-schema') as c:
            precondition(self,c);w=c['world'];before=fixture.logical(w.db_path)
            with closing(sqlite3.connect(w.db_path)) as db:
                for sql in ('UPDATE coordinator_fault_issuers SET witness_sha256=witness_sha256','DELETE FROM coordinator_fault_issuers','INSERT INTO coordinator_fault_issuers SELECT * FROM coordinator_fault_issuers'):
                    with self.subTest(sql=sql),self.assertRaises(sqlite3.IntegrityError):db.execute(sql)
                    db.rollback()
            self.assertEqual(fixture.logical(w.db_path),before)
            with closing(sqlite3.connect(w.db_path)) as db:db.execute('DROP TRIGGER fault_issuer_no_delete');db.commit()
            before=(fixture.logical(w.db_path),fixture.logical(c['host'].db_path))
            with self.assertRaises(WorldError) as error:fixture.perform(c)
            self.assertEqual(error.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            self.assertEqual((fixture.logical(w.db_path),fixture.logical(c['host'].db_path)),before)

    def test_characterization_world_committed_preclaim_bad_owner_is_not_laundered(self):
        with fixture.blocked_case('arch-preclaim') as c:
            precondition(self,c);w=c['world'];ref=c['ref']
            with closing(sqlite3.connect(w.db_path)) as db:db.execute('UPDATE coordinator_activations SET owner_actor_epoch=999 WHERE activation_id=?',(ref.activation_id,));db.commit()
            before=(fixture.logical(w.db_path),fixture.logical(c['host'].db_path))
            with self.assertRaises(WorldError) as error:w.acquire_actor_execution(ref.person_id,'no-launder',coordinator=c['coordinator']._owner_attempt.permit,activation=ref)
            self.assertEqual(error.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            self.assertEqual((fixture.logical(w.db_path),fixture.logical(c['host'].db_path)),before)

    def test_characterization_claim_changes_only_current_owner_not_fault_or_private_meta(self):
        with fixture.blocked_case('arch-claim') as c:
            row=precondition(self,c);w,h=c['world'],c['host'];ref=c['ref']
            witness=read(w.db_path,'SELECT * FROM coordinator_fault_issuers')
            meta=read(h.db_path,'SELECT * FROM actor_intent_execution')
            permit=w.acquire_actor_execution(ref.person_id,'arch-ordinary-C',coordinator=c['coordinator']._owner_attempt.permit,activation=ref)
            try:
                changed=read(w.db_path,'SELECT * FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,))[0]
                self.assertGreater(changed['owner_actor_epoch'],row['owner_actor_epoch'])
                self.assertEqual(read(w.db_path,'SELECT * FROM coordinator_fault_issuers'),witness)
                self.assertEqual(read(h.db_path,'SELECT * FROM actor_intent_execution'),meta)
                with w._coordinator_scope(c['coordinator']._owner_attempt.permit),scheduler.activation_scope(w,ref):
                    with self.assertRaises(WorldError) as error:
                        with h._private(permit,ref.person_id) as db:h._storage.adopt(db,ref.person_id,permit,uuid.uuid4().hex)
                    self.assertEqual(error.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
            finally:w.release_actor_execution(permit)
            result=fixture.perform(c);self.assertFalse(result['replayed'])
            finalmeta=read(h.db_path,'SELECT * FROM actor_intent_execution')[0]
            self.assertEqual({k:v for k,v in finalmeta.items() if k!='promoted_version'},{k:v for k,v in meta[0].items() if k!='promoted_version'})
            self.assertEqual(read(w.db_path,'SELECT * FROM coordinator_fault_issuers'),witness)
            PROOFS.append(dict(case='claim-C-maintenance-D',root=str(c['root']),activation_id=ref.activation_id,fault_epoch=row['owner_actor_epoch'],claim_epoch=changed['owner_actor_epoch'],maintenance=read(w.db_path,'SELECT * FROM coordinator_technical_maintenance')))
