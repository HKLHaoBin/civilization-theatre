"""M2 first vertical: real M1 anchor, trusted release, actual original dispatcher.
SDK metrics returned by the controlled provider fixture are synthetic, not SDK calls.
"""
from collections import Counter
from contextlib import closing
from pathlib import Path
import hashlib,json,sqlite3,tempfile,threading,unittest,uuid
from app.world import WorldStore,WorldError
from app.actors import ActorHost
from app.server import Coordinator
from app.coordinator_scheduler import ActivationRef
from app.tests import test_actor_execution as provider_fixture
from app.tests import test_technical_maintenance as m1_fixture
SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch');TRACE=[]

class TechnicalWindowReleaseTests(unittest.TestCase):
 def test_real_M1_closed_first_activation_releases_to_user_pause_then_step_completes_same_window_once(self):
  root=Path(tempfile.mkdtemp(prefix='q50-m2-first-',dir=SCRATCH));w=WorldStore(root/'world.sqlite');w.initialize();h=ActorHost(w,root/'actors.sqlite');co=Coordinator(w,h);calls=[]
  def synthetic_request(payload):calls.append(dict(person_id=payload['person_id'],intent_id=payload['intent_id']));return provider_fixture.ActorExecutionTests.result(payload)
  h._request=synthetic_request;h._choose=lambda view,seen:({'action':'wait'},seen)
  submit=h._submit;dm=getattr(h,'dm_provider',None)
  project=Path(__file__).resolve().parents[2]
  def repair(kind,ref):
   p=root/(kind+'.json');p.write_text(json.dumps({'kind':kind,'activation_id':ref.activation_id,'original_consumer_restored':True}),encoding='utf-8')
   return {'repair_id':uuid.uuid4().hex,'reason':kind,'source_sha256':{n:hashlib.sha256((project/n).read_bytes()).hexdigest() for n in ('app/actors.py','app/actor_execution.py','app/coordinator_scheduler.py','app/server.py','app/world.py')},'verification_refs':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}]}
  try:
   owner=co.acquire_owner();w.control('resume',coordinator=owner);version={k:w.snapshot()[k] for k in ('world_id','revision')};boundary=w.inspect_scheduler_boundary(actor_db_path=h.db_path);w.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=h.db_path);window=w.open_or_recover_window(coordinator=owner,expected_version=version);row=w.next_activation(window['window_id'],coordinator=owner);ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
   finish=h._finish
   def original_fault(*a,**k):raise RuntimeError('M2 parent original world-committed private fault')
   h._finish=original_fault
   try:
    with self.assertRaisesRegex(RuntimeError,'M2 parent original world-committed private fault'):h.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
   finally:h._finish=finish
   self.assertEqual(w.snapshot()['status'],'technical_paused');self.assertEqual(len(calls),1)
   def forbidden(*a,**k):raise AssertionError('release and M1 must not issue provider/submit/DM')
   h._request=forbidden;h._submit=forbidden;h.dm_provider=forbidden
   first=co.recover_technical(ref,operation_id=uuid.uuid4().hex,repair_evidence=repair('actual_original_finish_restored',ref));self.assertEqual(first['phase'],'completed');self.assertIs(first['replayed'],False)
   paused=w.snapshot();self.assertEqual(paused['status'],'technical_paused')
   with closing(sqlite3.connect(w.db_path)) as db:
    db.row_factory=sqlite3.Row;original_rows=[dict(x) for x in db.execute('SELECT * FROM coordinator_activations WHERE window_id=? ORDER BY roster_index',(ref.window_id,))];original_receipt=db.execute('SELECT receipt FROM intents WHERE id=?',(first['intent_id'],)).fetchone()[0]
   self.assertEqual(original_rows[0]['activation_id'],ref.activation_id);self.assertEqual((original_rows[0]['phase'],original_rows[0]['private_closed']),('completed',1));self.assertTrue(all(x['phase']=='pending' for x in original_rows[1:]));self.assertEqual(len(original_rows),12)
   with closing(sqlite3.connect(h.db_path)) as db:self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone()[0],1)
   with self.assertRaises(WorldError) as old_start:co.start()
   self.assertEqual(old_start.exception.code,'WORLD_PAUSED');self.assertIsNone(co.worker)
   protected_names=['intents','coordinator_windows','coordinator_activations','coordinator_activation_outcomes','coordinator_activation_blocks','coordinator_fault_issuers','coordinator_technical_maintenance'];facts=m1_fixture.tables(w.db_path,protected_names);private=m1_fixture.logical(h.db_path);operation=uuid.uuid4().hex
   TRACE.append({'case':'actual_M1_closed_original_window_before_new_release','root':str(root),'world_id':ref.world_id,'window_id':ref.window_id,'activation_id':ref.activation_id,'original_intent_id':first['intent_id'],'roster_activation_ids':[x['activation_id'] for x in original_rows],'completed':1,'pending':len(original_rows)-1,'head':1,'status':paused['status'],'time':paused['time'],'revision':paused['revision'],'ordinary_start_error':old_start.exception.code,'SDK':0,'synthetic_request_seam_calls':len(calls)})
   release=getattr(co,'release_technical_window',None)
   self.assertTrue(callable(release),'M2 trusted original-window release is missing after actual M1 completed/head1 and remaining original roster pending; ordinary start correctly still WORLD_PAUSED')
   result=release(ref,operation_id=operation,repair_evidence=repair('trusted_original_window_release_after_M1',ref))
   self.assertEqual(result['operation_id'],operation);self.assertEqual(result['window_id'],ref.window_id);self.assertEqual(result['activation_id'],ref.activation_id);self.assertEqual(result['phase'],'released');self.assertEqual(result['target_status'],'user_paused');self.assertEqual(result['observed_status'],'user_paused');self.assertEqual(result['release_revision'],paused['revision']+1);self.assertIs(result['replayed'],False)
   released=w.snapshot();self.assertEqual(released['status'],'user_paused');self.assertEqual(released['revision'],paused['revision']+1);self.assertNotIn('diagnostic',released)
   self.assertEqual({k:v for k,v in released.items() if k not in ('status','revision','diagnostic')},{k:v for k,v in paused.items() if k not in ('status','revision','diagnostic')});self.assertEqual(m1_fixture.tables(w.db_path,protected_names),facts);self.assertEqual(m1_fixture.logical(h.db_path),private);self.assertEqual(len(calls),1);self.assertIsNone(co.worker)
   h._request=synthetic_request;h._submit=submit;h.dm_provider=dm
   started=co.start(continuous=False);self.assertIs(started['accepted'],True);co.worker.join(20);self.assertFalse(co.worker.is_alive());self.assertIsNone(co.error);self.assertFalse(co.cleanup_pending);self.assertFalse(h.cleanup_pending)
   final=w.snapshot();self.assertEqual(final['status'],'user_paused');self.assertEqual(final['time'],paused['time']);self.assertEqual(final['horizon'],paused['horizon'])
   with closing(sqlite3.connect(w.db_path)) as db:
    db.row_factory=sqlite3.Row;rows=[dict(x) for x in db.execute('SELECT * FROM coordinator_activations WHERE window_id=? ORDER BY roster_index',(ref.window_id,))];completed_window=dict(db.execute('SELECT * FROM coordinator_windows WHERE window_id=?',(ref.window_id,)).fetchone());self.assertEqual(db.execute('SELECT COUNT(*) FROM coordinator_windows').fetchone()[0],1);self.assertEqual(db.execute('SELECT receipt FROM intents WHERE id=?',(first['intent_id'],)).fetchone()[0],original_receipt)
   self.assertEqual([x['activation_id'] for x in rows],[x['activation_id'] for x in original_rows]);self.assertTrue(all(x['phase']=='completed' and x['private_closed']==1 for x in rows));self.assertEqual(completed_window['state'],'completed');self.assertEqual(len({x['intent_id'] for x in rows}),len(rows));self.assertEqual(rows[0]['intent_id'],first['intent_id']);self.assertEqual(len(calls),len(rows));self.assertEqual(Counter(x['person_id'] for x in calls),Counter({x['person_id']:1 for x in rows}));self.assertEqual(sum(x['intent_id']==first['intent_id'] for x in calls),1)
   with closing(sqlite3.connect(h.db_path)) as db:
    heads=db.execute('SELECT person_id,version FROM actor_session_heads').fetchall();self.assertEqual({p:v for p,v in heads},{x['person_id']:1 for x in rows})
   TRACE.append({'case':'trusted_release_and_actual_original_dispatcher_complete','root':str(root),'release':result,'same_window_id':ref.window_id,'first_original_intent_id':first['intent_id'],'original_roster_ids_preserved':True,'first_not_reissued':True,'all_original_activations_once':True,'no_next_day':True,'synthetic_request_seam_calls':len(calls),'actual_SDK':0})
  finally:
   co.close();h.close();TRACE.append({'case':'first_fixture_cleanup','root':str(root),'cleanup_pending':h.cleanup_pending,'actual_SDK':0})

# Independent tracers append after the byte-preserved parent method.
from contextlib import contextmanager
from app import coordinator_maintenance as maintenance
from unittest.mock import patch

class WindowFinalGateTests(unittest.TestCase):
 def test_final_root_sql_cannot_hide_material_change_after_prior_digest(self):
  with released_case('last-material-sql') as c:
   connect=sqlite3.connect;before=release_facts(c);hits=[];phase={'snapshot':False};error=None
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_INSERT and arg1=='snapshots':phase['snapshot']=True
     if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and phase['snapshot'] and not hits:
      hits.append('pending-mutated-after-prior-digest');db.execute('UPDATE coordinator_activations SET card_version=1 WHERE roster_index=1')
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   with patch.object(sqlite3,'connect',observed):
    try:release_case(c)
    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
   self.assertTrue(hits);self.assertEqual(error,'TECHNICAL_WINDOW_NOT_PRISTINE');self.assertEqual(release_facts(c),before)
   with closing(sqlite3.connect(c['world'].db_path)) as db:self.assertIsNone(db.execute('SELECT card_version FROM coordinator_activations WHERE roster_index=1').fetchone()[0])
   TRACE.append({'case':'final-SQL-fresh-material-rollback','root':str(c['root']),'hits':hits,'error':error})

 def test_last_root_sql_rechecks_actual_repair_file_bytes_before_commit(self):
  with released_case('last-evidence-sql') as c:
   connect=sqlite3.connect;before=release_facts(c);hits=[];phase={'snapshot':False};error=None
   p=Path(c['repair']['verification_refs'][0]['path']);original=p.read_bytes()
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_INSERT and arg1=='snapshots':phase['snapshot']=True
     if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and phase['snapshot'] and not hits:
      hits.append('repair-file-changed-after-writes');p.write_bytes(b'changed after earlier proof')
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   try:
    with patch.object(sqlite3,'connect',observed):
     try:release_case(c)
     except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
   finally:p.write_bytes(original)
   self.assertTrue(hits);self.assertEqual(error,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED');self.assertEqual(release_facts(c),before)
   TRACE.append({'case':'last-SQL-fresh-repair-proof-rollback','root':str(c['root']),'hits':hits,'error':error})

 def test_last_root_sql_cannot_commit_after_actual_root_world_substitution(self):
  with released_case('last-root-sql') as c:
   w=c['world'];co=c['coordinator'];root=co._owner_attempt;other=WorldStore(w.db_path);before=release_facts(c)
   connect=sqlite3.connect;hits=[];phase={'snapshot':False};error=None
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_INSERT and arg1=='snapshots':phase['snapshot']=True
     if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and phase['snapshot'] and not hits:
      hits.append('actual-root-SQL-after-release-history-and-snapshot');root.world=other
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   try:
    with patch.object(sqlite3,'connect',observed):
     try:release_case(c)
     except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
   finally:root.world=w
   self.assertTrue(hits,'actual last transaction gate must be reached')
   self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT','root object must remain bound through the last SQL gate')
   self.assertEqual(release_facts(c),before)
   TRACE.append({'case':'last-root-sql-binding-rollback','root':str(c['root']),'hits':hits,'error':error})

@contextmanager
def released_case(label, *, release=False):
 with m1_fixture.blocked_case('M2-'+label) as c:
  first=m1_fixture.perform(c)
  c['m1']=first;c['operation']=uuid.uuid4().hex
  c['repair']=json.loads(json.dumps(c['repair']));c['repair']['repair_id']=uuid.uuid4().hex
  assert c['world'].snapshot()['status']=='technical_paused'
  assert first['phase']=='completed'
  if release:c['released']=release_case(c)
  try:yield c
  finally:TRACE.append({'case':label,'root':str(c['root']),'world_id':c['ref'].world_id,'window_id':c['ref'].window_id,'activation_id':c['ref'].activation_id,'SDK':0})

def release_case(c,**kw):
 args=dict(operation_id=c['operation'],repair_evidence=c['repair']);args.update(kw)
 return c['coordinator'].release_technical_window(c['ref'],**args)

def release_facts(c):
 with closing(sqlite3.connect(c['world'].db_path)) as db:
  schema=db.execute("SELECT sql FROM sqlite_master WHERE name='coordinator_technical_window_releases'").fetchone()
  records=db.execute('SELECT * FROM coordinator_technical_window_releases').fetchall() if schema else []
 return dict(snapshot=c['world'].snapshot(),records=records,private=m1_fixture.logical(c['host'].db_path))

def run_original_step(c,test):
 h=c['host'];co=c['coordinator']
 h._submit=ActorHost._submit.__get__(h,ActorHost);h.dm_provider=None
 h._request=lambda payload:(c['calls'].append(payload) or provider_fixture.ActorExecutionTests.result(payload))
 test.assertTrue(co.start(continuous=False)['accepted']);co.worker.join(20)
 test.assertFalse(co.worker.is_alive());test.assertIsNone(co.error)

class WindowReplayTests(unittest.TestCase):
 def test_strict_history_rejects_boolean_epoch_and_unknown_keys(self):
  with released_case('strict-record',release=True) as c:
   with closing(sqlite3.connect(c['world'].db_path)) as db:
    db.row_factory=sqlite3.Row;saved=dict(db.execute('SELECT * FROM coordinator_technical_window_releases').fetchone())
    original=json.loads(saved['record_json'])
    for name,value in (('parent_epoch',True),('parent_epoch',2**63),('release_revision',True),('operation_id','z'*32),('target_status','running'),('unexpected',1)):
     with self.subTest(field=name,value=value):
      record={**original,name:value};row={**saved,'record_json':maintenance.canonical(record)};row['record_sha256']=maintenance.digest(row['record_json'])
      if name=='operation_id':row['operation_id']=value
      error=None
      try:maintenance.window_record_proof(db,row)
      except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
      self.assertEqual(error,'TECHNICAL_MAINTENANCE_UNPROVEN','digest/canonical alone must not authorize a malformed host-only record')

 def test_same_key_readonly_ack_then_actual_step_and_replay(self):
  with released_case('replay',release=True) as c:
   first=c['released'];before=release_facts(c);error=None;second=None
   try:second=release_case(c)
   except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
   self.assertIsNone(error,'same committed window operation must reconcile read-only')
   self.assertEqual(second,{**first,'replayed':True});self.assertEqual(release_facts(c),before)
   for kw in ({'operation_id':uuid.uuid4().hex},{'repair_evidence':{**c['repair'],'reason':'changed payload'}}):
    with self.assertRaises(WorldError) as denied:release_case(c,**kw)
    self.assertEqual(denied.exception.code,'TECHNICAL_MAINTENANCE_OPERATION_CONFLICT')
   run_original_step(c,self);after=release_facts(c)
   replay=release_case(c);self.assertTrue(replay['replayed']);self.assertEqual(replay['release_revision'],first['release_revision']);self.assertEqual(replay['observed_status'],'user_paused');self.assertEqual(release_facts(c),after)
   self.assertEqual(len(c['calls']),12)
   TRACE.append({'case':'read_only_replay_before_after_actual_dispatch','root':str(c['root']),'first':first,'replay':replay,'calls':len(c['calls'])})

class WindowBoundaryCharacterizationTests(unittest.TestCase):
 def test_characterization_no_root_fake_capsule_and_binding_ref(self):
  with released_case('authority') as c:
   co=c['coordinator'];w=c['world'];root=co._owner_attempt;before=release_facts(c)
   co._owner_attempt=None
   try:
    with self.assertRaises(WorldError) as error:release_case(c)
    self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_REQUIRED')
   finally:co._owner_attempt=root
   with self.assertRaises(WorldError) as error:maintenance._WindowCapsule({},co,root,c['ref'],c['repair'],c['operation'],None)
   self.assertEqual(error.exception.code,'TECHNICAL_MAINTENANCE_REQUIRED')
   other=WorldStore(w.db_path);root.world=other
   try:
    with self.assertRaises(WorldError) as error:release_case(c)
    self.assertEqual(error.exception.code,'ACTIVATION_BINDING_CONFLICT')
   finally:root.world=w
   with self.assertRaises(WorldError):co.release_technical_window({'activation_id':c['ref'].activation_id},operation_id=c['operation'],repair_evidence=c['repair'])
   self.assertEqual(release_facts(c),before);self.assertEqual(len(c['calls']),1)

 def test_characterization_wrong_schema_and_claimed_pending_are_not_repaired(self):
  for variant in ('schema','pending-material','orphan-call'):
   with self.subTest(variant=variant),released_case(variant) as c:
    if variant=='orphan-call':
     with closing(sqlite3.connect(c['host'].db_path)) as db:
      db.execute('INSERT INTO actor_calls VALUES (?,?,?)',(uuid.uuid4().hex,uuid.uuid4().hex,'p02'));db.commit()
    else:
     with closing(sqlite3.connect(c['world'].db_path)) as db:
      if variant=='schema':db.execute('CREATE TABLE coordinator_technical_window_releases (operation_id TEXT)')
      else:db.execute('UPDATE coordinator_activations SET observation_revision=3 WHERE roster_index=1')
      db.commit()
    before=release_facts(c);error=None
    try:release_case(c)
    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
    self.assertIsNotNone(error);self.assertEqual(release_facts(c),before);self.assertEqual(len(c['calls']),1)
    TRACE.append({'case':'pristine-and-schema-rejection','variant':variant,'error':error,'root':str(c['root'])})

 def test_characterization_atomic_rollback_at_history_snapshot_and_real_commit(self):
  for gate in ('history','snapshot','commit'):
   with self.subTest(gate=gate),released_case('rollback-'+gate) as c:
    connect=sqlite3.connect;hits=[];before=release_facts(c);staged={'history':False};error=None
    def observed(*a,**kw):
     kw['cached_statements']=0;db=connect(*a,**kw)
     def auth(action,arg1,arg2,dbname,trigger):
      if action==sqlite3.SQLITE_INSERT and arg1=='coordinator_technical_window_releases':staged['history']=True
      deny=((gate=='history' and action==sqlite3.SQLITE_INSERT and arg1=='coordinator_technical_window_releases')
        or (gate=='snapshot' and staged['history'] and action==sqlite3.SQLITE_INSERT and arg1=='snapshots')
        or (gate=='commit' and staged['history'] and action==sqlite3.SQLITE_TRANSACTION and arg1=='COMMIT'))
      if deny:hits.append(gate);return sqlite3.SQLITE_DENY
      return sqlite3.SQLITE_OK
     db.set_authorizer(auth);return db
    with patch.object(sqlite3,'connect',observed):
     try:release_case(c)
     except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
    self.assertTrue(hits);self.assertIsNotNone(error);self.assertEqual(release_facts(c),before)
    TRACE.append({'case':'atomic-window-release-rollback','gate':gate,'hits':hits,'error':error,'root':str(c['root'])})

 def test_characterization_append_only_all_independent_conflicts_recursive_zero_one(self):
  with released_case('append-only',release=True) as c:
   with closing(sqlite3.connect(c['world'].db_path)) as db:
    saved=db.execute('SELECT * FROM coordinator_technical_window_releases').fetchone()
    for recursive in (0,1):
     db.execute('PRAGMA recursive_triggers='+str(recursive))
     for verb in ('INSERT','INSERT OR REPLACE'):
      for key in ('operation','window','both'):
       row=list(saved)
       if key=='operation':row[2]=uuid.uuid4().hex
       if key=='window':row[0]=uuid.uuid4().hex
       row[3]='changed payload'
       with self.assertRaises(sqlite3.IntegrityError):db.execute(verb+' INTO coordinator_technical_window_releases VALUES (?,?,?,?,?,?)',row)
       db.rollback();self.assertEqual(db.execute('SELECT * FROM coordinator_technical_window_releases').fetchall(),[saved])
     for statement in ("UPDATE coordinator_technical_window_releases SET record_json='changed'",'DELETE FROM coordinator_technical_window_releases'):
      with self.assertRaises(sqlite3.IntegrityError):db.execute(statement)
      db.rollback();self.assertEqual(db.execute('SELECT * FROM coordinator_technical_window_releases').fetchall(),[saved])
   TRACE.append({'case':'append-only-0-1-independent-unique-conflicts','root':str(c['root']),'recursive_modes':[0,1],'statements_per_mode':8})

 def test_characterization_old_release_replay_observes_new_real_fault_without_unlock(self):
  with released_case('later-fault',release=True) as c:
   h=c['host'];co=c['coordinator'];finish=h._finish
   h._submit=ActorHost._submit.__get__(h,ActorHost);h.dm_provider=None
   h._request=lambda payload:(c['calls'].append(payload) or provider_fixture.ActorExecutionTests.result(payload))
   def fault(*a,**kw):raise RuntimeError('second actual activation private fault')
   h._finish=fault
   try:
    self.assertTrue(co.start(continuous=False)['accepted']);co.worker.join(20);self.assertFalse(co.worker.is_alive())
   finally:h._finish=finish
   self.assertIsNotNone(co.error);self.assertEqual(c['world'].snapshot()['status'],'technical_paused');self.assertEqual(len(c['calls']),2)
   before=release_facts(c);result=release_case(c)
   self.assertTrue(result['replayed']);self.assertEqual(result['observed_status'],'technical_paused');self.assertEqual(result['target_status'],'user_paused');self.assertEqual(release_facts(c),before)
   with self.assertRaises(WorldError):release_case(c,operation_id=uuid.uuid4().hex)
   TRACE.append({'case':'old-key-cannot-unlock-second-real-fault','root':str(c['root']),'result':result,'calls':len(c['calls'])})

 def test_characterization_new_world_host_coordinator_root_readonly_replay(self):
  with released_case('cold-new-root',release=True) as c:
   run_original_step(c,self);before=release_facts(c);old_epoch=c['coordinator']._owner_attempt.permit.epoch
   c['coordinator'].close();c['host'].close()
   w=WorldStore(c['world'].db_path);h=ActorHost(w,c['host'].db_path);co=Coordinator(w,h)
   def forbid(*a,**kw):raise AssertionError('cold replay cannot dispatch')
   h._request=forbid;h._submit=forbid;h.dm_provider=forbid
   try:
    co.acquire_owner();new={**c,'world':w,'host':h,'coordinator':co}
    result=release_case(new);self.assertTrue(result['replayed']);self.assertGreater(co._owner_attempt.permit.epoch,old_epoch)
    self.assertEqual(release_facts(new),before);self.assertIsNone(co.worker)
    TRACE.append({'case':'cold-new-World-Host-Co-root-readonly-replay','root':str(c['root']),'old_epoch':old_epoch,'new_epoch':co._owner_attempt.permit.epoch,'result':result})
   finally:co.close();h.close()

 def test_characterization_original_entry_deadline_and_cleanup_are_not_replaced(self):
  import time
  from app.actor_execution import EntryBudget,ActorRunError
  with released_case('entry-budget') as c:
   co=c['coordinator'];w=c['world'];root=co._owner_attempt;before=release_facts(c)
   with co._owner_scope(root),co._preparation_scope(root):
    entry=w._scoped_entry_budget();deadline=entry.deadline;busy=entry.busy_deadline
    entry.deadline=time.monotonic()-1
    try:
     with self.assertRaises(ActorRunError) as error:release_case(c)
     self.assertEqual(error.exception.code,'ACTOR_EXECUTION_DEADLINE')
    finally:entry.deadline=deadline
    entry.busy_deadline=time.monotonic()-1
    try:
     with self.assertRaises(ActorRunError) as error:release_case(c)
     self.assertEqual(error.exception.code,'EXECUTION_STORAGE_BUSY')
    finally:entry.busy_deadline=busy
   co.cleanup_pending=True
   try:
    with self.assertRaises(WorldError) as error:release_case(c)
    self.assertEqual(error.exception.code,'ACTOR_EXECUTION_CLEANUP_PENDING')
   finally:co.cleanup_pending=False
   self.assertEqual(release_facts(c),before)

 def test_characterization_readonly_replay_has_no_DDL_or_new_actor_authority(self):
  with released_case('replay-no-ddl',release=True) as c:
   before=release_facts(c);connect=sqlite3.connect;ddl=[]
   with closing(connect(c['world'].db_path)) as db:
    lease_before=db.execute('SELECT * FROM actor_execution_leases').fetchall()
   def observed(*a,**kw):
    db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action in (sqlite3.SQLITE_CREATE_TABLE,sqlite3.SQLITE_CREATE_TRIGGER,sqlite3.SQLITE_DROP_TABLE,sqlite3.SQLITE_DROP_TRIGGER):ddl.append([action,arg1]);return sqlite3.SQLITE_DENY
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   with patch.object(sqlite3,'connect',observed):self.assertTrue(release_case(c)['replayed'])
   self.assertEqual(ddl,[]);self.assertEqual(release_facts(c),before)
   with closing(connect(c['world'].db_path)) as db:self.assertEqual(db.execute('SELECT * FROM actor_execution_leases').fetchall(),lease_before)

class WindowHistoricalGenerationChecks(unittest.TestCase):
 # Explicit-only actual retained producer check, not a hermetic discovery test.
 # Uses an earlier frozen GREEN's real scratch DB; no reconstructed history.
 def check_prior_generation_release_replay(self):
  project=Path(__file__).resolve().parents[2]
  old=json.loads((project/'app/evidence/q50-m2-r-final-root-green-execution.json').read_text(encoding='utf-8'))
  trace=next(x for x in old['traces']['test_technical_window_release']['TRACE'] if x.get('case')=='read_only_replay_before_after_actual_dispatch')
  root=Path(trace['root']);w=WorldStore(root/'world.sqlite');h=ActorHost(w,root/'actors.sqlite');co=Coordinator(w,h)
  try:
   co.acquire_owner()
   with closing(sqlite3.connect(w.db_path)) as db:
    saved=json.loads(db.execute('SELECT record_json FROM coordinator_technical_window_releases WHERE operation_id=?',(trace['first']['operation_id'],)).fetchone()[0])
    row=db.execute('SELECT person_id FROM coordinator_activations WHERE activation_id=?',(saved['activation_id'],)).fetchone()
   current=hashlib.sha256((project/'app/coordinator_maintenance.py').read_bytes()).hexdigest()
   self.assertNotEqual(saved['source_sha256']['app/coordinator_maintenance.py'],current,'must really be a distinct earlier loaded source generation')
   ref=ActivationRef(saved['world_id'],saved['window_id'],saved['activation_id'],row[0]);error=None;result=None
   before=dict(snapshot=w.snapshot(),private=m1_fixture.logical(h.db_path))
   try:result=co.release_technical_window(ref,operation_id=saved['operation_id'],repair_evidence=saved['repair'])
   except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
   self.assertIsNone(error,'immutable historical repair/source must not be rebased to new loaded generation on read-only replay')
   self.assertTrue(result['replayed']);self.assertEqual(dict(snapshot=w.snapshot(),private=m1_fixture.logical(h.db_path)),before)
   TRACE.append({'case':'actual-prior-loaded-generation-replay','root':str(root),'prior_manifest':old['manifest_sha256'],'historical_source':saved['source_sha256'],'current_maintenance_sha256':current,'result':result})
  finally:co.close();h.close()

class WindowLifecycleCharacterizationTests(unittest.TestCase):
 def test_characterization_concurrent_same_key_serializes_one_release(self):
  with released_case('concurrent') as c:
   barrier=threading.Barrier(3);results=[];errors=[];before=c['world'].snapshot()
   def call():
    try:barrier.wait(3);results.append(release_case(c))
    except Exception as exc:errors.append(getattr(exc,'code',type(exc).__name__))
   workers=[threading.Thread(target=call,name='M2-owned-release-'+str(i)) for i in range(2)]
   for worker in workers:worker.start()
   barrier.wait(3)
   for worker in workers:worker.join(10);self.assertFalse(worker.is_alive())
   self.assertEqual(errors,[]);self.assertEqual(sorted(x['replayed'] for x in results),[False,True]);self.assertEqual(c['world'].snapshot()['revision'],before['revision']+1)
   self.assertEqual(len(release_facts(c)['records']),1);self.assertEqual(len(c['calls']),1)
   TRACE.append({'case':'concurrent-samekey-single-release','root':str(c['root']),'results':results,'errors':errors})

 def test_characterization_actual_new_entry_and_callee_substitution_fail_source(self):
  for variant in ('entry','callee'):
   with self.subTest(variant=variant),released_case('source-'+variant) as c:
    before=release_facts(c);co=c['coordinator'];entry=co.release_technical_window
    if variant=='entry':
     def counterfeit(*a,**kw):return entry(*a,**kw)
     co.release_technical_window=counterfeit
     try:
      with self.assertRaises(WorldError) as error:release_case(c)
     finally:del co.release_technical_window
    else:
     original=maintenance.window_material
     def substituted(*a,**kw):return original(*a,**kw)
     with patch.object(maintenance,'window_material',substituted):
      with self.assertRaises(WorldError) as error:release_case(c)
    self.assertEqual(error.exception.code,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED');self.assertEqual(release_facts(c),before)

 def test_characterization_final_cancel_deadline_and_permit_swap_rollback(self):
  import time
  from app.coordinator_execution import CoordinatorPermit
  for variant in ('cancel','deadline','permit'):
   with self.subTest(variant=variant),released_case('final-'+variant) as c:
    co=c['coordinator'];w=c['world'];root=co._owner_attempt;permit=root.permit;connect=sqlite3.connect;hits=[];phase={'snapshot':False};error=None;before=release_facts(c)
    def observed(*a,**kw):
     kw['cached_statements']=0;db=connect(*a,**kw)
     def auth(action,arg1,arg2,dbname,trigger):
      if action==sqlite3.SQLITE_INSERT and arg1=='snapshots':phase['snapshot']=True
      if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and phase['snapshot'] and not hits:
       hits.append(variant)
       if variant=='cancel':root.stop.set()
       elif variant=='deadline':w._scoped_entry_budget().deadline=time.monotonic()-1
       else:root.permit=CoordinatorPermit(permit.world_id,permit.owner_token,permit.epoch)
      return sqlite3.SQLITE_OK
     db.set_authorizer(auth);return db
    try:
     with patch.object(sqlite3,'connect',observed):
      try:release_case(c)
      except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
    finally:root.stop.clear();root.permit=permit
    self.assertTrue(hits);self.assertEqual(error,{'cancel':'COORDINATOR_EXECUTION_CANCELLED','deadline':'ACTOR_EXECUTION_DEADLINE','permit':'ACTIVATION_BINDING_CONFLICT'}[variant]);self.assertEqual(release_facts(c),before)
    TRACE.append({'case':'final-original-lifetime-rollback','variant':variant,'hits':hits,'error':error,'root':str(c['root'])})

 def test_characterization_expiry_after_real_commit_is_committed_fact(self):
  import time
  with released_case('postcommit') as c:
   w=c['world'];connect=sqlite3.connect;hits=[];staged={'history':False}
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_INSERT and arg1=='coordinator_technical_window_releases':staged['history']=True
     if action==sqlite3.SQLITE_TRANSACTION and arg1=='COMMIT' and staged['history']:
      hits.append('commit-authorized-then-entry-expired');w._scoped_entry_budget().deadline=time.monotonic()-1
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   with patch.object(sqlite3,'connect',observed):result=release_case(c)
   self.assertTrue(hits);self.assertFalse(result['replayed']);self.assertEqual(w.snapshot()['status'],'user_paused')
   before=release_facts(c);self.assertTrue(release_case(c)['replayed']);self.assertEqual(release_facts(c),before)
   TRACE.append({'case':'postcommit-expired-entry-honest-result','root':str(c['root']),'hits':hits,'result':result})

 def test_characterization_main_writer_contention_is_finite_and_no_effect(self):
  import time
  with released_case('writer-lock') as c:
   before=release_facts(c);error=None
   with closing(sqlite3.connect(c['world'].db_path)) as writer:
    writer.execute('BEGIN IMMEDIATE');start=time.monotonic()
    try:release_case(c)
    except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
    elapsed=time.monotonic()-start;writer.rollback()
   self.assertEqual(error,'EXECUTION_STORAGE_BUSY');self.assertLess(elapsed,3);self.assertEqual(release_facts(c),before)
   TRACE.append({'case':'finite-real-main-writer-lock','root':str(c['root']),'error':error,'elapsed':elapsed})



class TechnicalWindowR1HostBindingTests(unittest.TestCase):
 def test_R1_last_actual_root_SQL_current_Host_swap_is_local_rejection_and_rollback(self):
  with released_case('R1-parent-last-current-Host') as c:
   co=c['coordinator'];host=c['host'];before=release_facts(c);connect=sqlite3.connect;hits=[];phase={'snapshot':False};replacement=object();error=None
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_INSERT and arg1=='snapshots':phase['snapshot']=True
     if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and phase['snapshot'] and not hits:
      hits.append('actual-last-root-SQL-current-Host-swap');co.actors=replacement
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   try:
    with patch.object(sqlite3,'connect',observed):
     try:release_case(c)
     except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
    replaced_at_return=co.actors is replacement
   finally:co.actors=host
   after=release_facts(c)
   TRACE.append({'case':'R1-parent-current-Host-last-SQL-regression','root':str(c['root']),'hits':hits,'error':error,'replaced_at_return':replaced_at_return,'before_status':before['snapshot']['status'],'after_status':after['snapshot']['status'],'before_revision':before['snapshot']['revision'],'after_revision':after['snapshot']['revision'],'before_release_count':len(before['records']),'after_release_count':len(after['records'])})
   self.assertTrue(hits,'must reach actual final SQL after staged history and snapshot')
   self.assertTrue(replaced_at_return,'the injected association loss must persist to the real return boundary')
   self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT','M2 final gate must validate current Coordinator.actors against captured actual Host')
   self.assertEqual(after,before,'association loss must not commit a release or forward snapshot and must preserve all private facts')


class TechnicalWindowR1HostControls(unittest.TestCase):
 def test_R1_actual_Host_identity_at_effect_and_readonly_SQL(self):
  for replay in (False,True):
   for variant in ('original','same-file-other-Host'):
    with self.subTest(replay=replay,variant=variant),released_case('R1-host-control',release=replay) as c:
     co=c['coordinator'];host=c['host'];before=release_facts(c);connect=sqlite3.connect;hits=[];phase={'ready':False};error=None;result=None
     other=ActorHost(WorldStore(c['world'].db_path),host.db_path)
     self.assertIsNot(other,host);self.assertEqual(other.db_path,host.db_path);self.assertEqual(other.world.db_path,host.world.db_path)
     def observed(*a,**kw):
      kw['cached_statements']=0;db=connect(*a,**kw)
      def auth(action,arg1,arg2,dbname,trigger):
       if (not replay and action==sqlite3.SQLITE_INSERT and arg1=='snapshots') or (replay and action==sqlite3.SQLITE_READ and arg1=='coordinator_technical_window_releases' and arg2=='record_json'):phase['ready']=True
       if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and phase['ready'] and not hits:
        hits.append('actual-root-SQL');co.actors=host if variant=='original' else other
       return sqlite3.SQLITE_OK
      db.set_authorizer(auth);return db
     try:
      with patch.object(sqlite3,'connect',observed):
       try:result=release_case(c)
       except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
     finally:co.actors=host;other.close()
     self.assertTrue(hits)
     if variant=='original':
      self.assertIsNone(error);self.assertEqual(result['replayed'],replay)
      if replay:self.assertEqual(release_facts(c),before)
      else:self.assertEqual(c['world'].snapshot()['revision'],before['snapshot']['revision']+1)
     else:self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT');self.assertEqual(release_facts(c),before)
     TRACE.append({'case':'R1-actual-Host-object-control','replay':replay,'variant':variant,'hits':hits,'error':error,'root':str(c['root'])})


class TechnicalWindowR1CoreUpgradeTests(unittest.TestCase):
 def test_R1_actual_copied_CORE_server_upgrade_reconciles_original_release_readonly(self):
  import runpy
  project=Path(__file__).resolve().parents[2]
  helper=runpy.run_path(str(project/'app/evidence/q50-m2-r1-core-upgrade.py'))
  proof=helper['run'](project);TRACE.append(proof)
  self.assertIsNone(proof['error'],'a real new server/CORE generation must replay the original immutable repair without rebasing hashes')
  self.assertTrue(proof['result']['replayed']);self.assertEqual(proof['result']['operation_id'],proof['first']['operation_id'])
  self.assertEqual(proof['all_SQL_logical_before_sha256'],proof['all_SQL_logical_after_sha256'])


class TechnicalWindowR1SourceBoundaryControls(unittest.TestCase):
 def test_R1_first_effect_and_M1_default_still_require_current_CORE_and_strict_payload(self):
  with released_case('R1-source-boundaries') as c:
   before=release_facts(c);stale=json.loads(json.dumps(c['repair']));stale['source_sha256']['app/server.py']='0'*64
   for action in (lambda:release_case(c,repair_evidence=stale),lambda:m1_fixture.perform(c,repair_evidence=stale)):
    with self.assertRaises(WorldError) as error:action()
    self.assertEqual(error.exception.code,'TECHNICAL_MAINTENANCE_SOURCE_CHANGED');self.assertEqual(release_facts(c),before)
   for field in ('historical_source','_historical_source'):
    with self.assertRaises(WorldError) as error:release_case(c,repair_evidence={**c['repair'],field:c['repair']['source_sha256']})
    self.assertEqual(error.exception.code,'INVALID_TECHNICAL_MAINTENANCE');self.assertEqual(release_facts(c),before)
   self.assertEqual(len(c['calls']),1);self.assertIsNone(c['coordinator'].worker)


class TechnicalWindowR1FinalReadonlyHostTests(unittest.TestCase):
 def test_R1_last_readonly_root_SQL_rechecks_current_actual_Host(self):
  with released_case('R1-last-readonly-Host',release=True) as c:
   co=c['coordinator'];host=c['host'];connect=sqlite3.connect;before=release_facts(c);reads=[];target=None;replacement=host;hits=[]
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and arg2=='epoch':
      reads.append(len(reads)+1)
      if target is not None and len(reads)==target:hits.append(target);co.actors=replacement
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   with patch.object(sqlite3,'connect',observed):self.assertTrue(release_case(c)['replayed'])
   target=len(reads);self.assertGreaterEqual(target,2);self.assertEqual(release_facts(c),before)
   other=ActorHost(WorldStore(c['world'].db_path),host.db_path)
   try:
    for replacement in (object(),other):
     reads.clear();hits.clear();error=None
     try:
      with patch.object(sqlite3,'connect',observed):
       try:release_case(c)
       except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
      current_is_replacement=co.actors is replacement
     finally:co.actors=host
     self.assertEqual(hits,[target]);self.assertEqual(len(reads),target);self.assertTrue(current_is_replacement)
     self.assertEqual(error,'ACTIVATION_BINDING_CONFLICT');self.assertEqual(release_facts(c),before)
     TRACE.append({'case':'R1-final-readonly-root-SQL-current-Host','target_last_root_SQL':target,'actual_root_SQL_reads':len(reads),'replacement_type':type(replacement).__name__,'error':error,'root':str(c['root'])})
   finally:other.close()


class TechnicalWindowCloseEntryHostTests(unittest.TestCase):
 def test_CLOSE_original_entry_Host_cannot_be_recaptured_before_capsule(self):
  with released_case('CLOSE-entry-Host') as c:
   co=c['coordinator'];world=c['world'];host=c['host'];before=release_facts(c)
   other=ActorHost(world,host.db_path);connect=sqlite3.connect;hits=[];error=None;result=None
   def observed(*a,**kw):
    kw['cached_statements']=0;db=connect(*a,**kw)
    def auth(action,arg1,arg2,dbname,trigger):
     if action==sqlite3.SQLITE_READ and arg1=='sqlite_master' and not hits:
      hits.append({'entry_host_original':co.actors is host,'entry_lock_owned':host._lock._is_owned()})
      host.close();co.actors=other
     return sqlite3.SQLITE_OK
    db.set_authorizer(auth);return db
   try:
    with patch.object(sqlite3,'connect',observed):
     try:result=release_case(c)
     except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
    replacement_observed=co.actors is other;original_closed=host._closed
   finally:co.actors=host;other.close()
   self.assertEqual(hits,[{'entry_host_original':True,'entry_lock_owned':True}])
   self.assertTrue(replacement_observed);self.assertTrue(original_closed)
   TRACE.append({'case':'CLOSE-entry-Host','root':str(c['root']),'hits':hits,'error':error,'result':result,'original_closed':original_closed})
   self.assertIn(error,('ACTIVATION_BINDING_CONFLICT','COORDINATOR_EXECUTION_CANCELLED'))
   self.assertIsNone(result);self.assertEqual(release_facts(c),before)


class TechnicalWindowCloseIdentityPairs(unittest.TestCase):
 def test_CLOSE_entry_and_final_native_boundaries_preserve_original_identity_chain(self):
  # Supplemental characterization of the already sealed entry fix, not a new RED.
  from dataclasses import replace
  from app import world as world_module
  from app.actor_execution import EntryBudget,ExecutionPolicy
  for replay in (False,True):
   for seam in ('entry','final'):
    for variant in ('original','live_host','closed_host','storage','host_lock','coordinator_lock','world','root_world','permit','entry'):
     with self.subTest(replay=replay,seam=seam,variant=variant),released_case('CLOSE-identity-pair',release=replay) as c:
      co=c['coordinator'];w=c['world'];h=c['host'];root=co._owner_attempt
      other_world=WorldStore(w.db_path);other=ActorHost(other_world,h.db_path)
      originals=(h._storage,h._lock,co.lock,root.permit,world_module._actor_entry_scope.get())
      connect=sqlite3.connect;reads=[];hits=[];armed=False;target=None;phase={'writes':False}
      before=release_facts(c);result=None;error=None
      def observed(*a,**kw):
       kw['cached_statements']=0;db=connect(*a,**kw)
       def auth(action,arg1,arg2,dbname,trigger):
        nonlocal other
        if action==sqlite3.SQLITE_INSERT and arg1=='snapshots':phase['writes']=True
        if action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and arg2=='epoch':reads.append(len(reads)+1)
        match=(seam=='entry' and action==sqlite3.SQLITE_READ and arg1=='sqlite_master' or
               seam=='final' and action==sqlite3.SQLITE_READ and arg1=='coordinator_execution_leases' and arg2=='epoch' and len(reads)==target)
        if armed and match and not hits:
         hits.append({'original_Host':co.actors is h,'original_lock_owned':originals[1]._is_owned(),'snapshot_insert_reached':phase['writes']})
         if variant in ('live_host','closed_host'):
          # Genuine observer construction in a fresh context, not a new root
          # scope: avoids unauthorized bootstrap SQL in this native callback.
          from contextvars import Context
          other.close();other=Context().run(ActorHost,w,h.db_path)
          if variant=='closed_host':h.close()
          co.actors=other
         elif variant=='storage':h._storage=other._storage
         elif variant=='host_lock':h._lock=threading.RLock()
         elif variant=='coordinator_lock':co.lock=threading.RLock()
         elif variant=='world':co._authority=other_world
         elif variant=='root_world':root.world=other_world
         elif variant=='permit':root.permit=replace(originals[3])
         elif variant=='entry':world_module._actor_entry_scope.set((w,EntryBudget(ExecutionPolicy(),co._closed,(root,))))
        return sqlite3.SQLITE_OK
       db.set_authorizer(auth);return db
      try:
       if seam=='final':
        # Readonly calibration is nonmutating. Effect calibration uses another
        # real fixture and commits its positive; it never changes this case.
        if replay:
         with patch.object(sqlite3,'connect',observed):self.assertTrue(release_case(c)['replayed'])
        else:
         # Observe the actual final root read, not an assumed ordinal.
         with released_case('CLOSE-effect-final-calibration') as calibration:
          with patch.object(sqlite3,'connect',observed):self.assertFalse(release_case(calibration)['replayed'])
        target=len(reads);self.assertGreater(target,0);reads.clear();phase['writes']=False
       armed=True
       with patch.object(sqlite3,'connect',observed):
        try:result=release_case(c)
        except Exception as exc:error=getattr(exc,'code',type(exc).__name__)
      finally:
       co.actors=h;co._authority=w;root.world=w;root.permit=originals[3]
       h._storage=originals[0];h._lock=originals[1];co.lock=originals[2]
       world_module._actor_entry_scope.set(originals[4]);other.close()
      self.assertEqual(len(hits),1);self.assertTrue(hits[0]['original_Host']);self.assertTrue(hits[0]['original_lock_owned'])
      if seam=='final' and not replay:self.assertTrue(hits[0]['snapshot_insert_reached'])
      if variant=='original':
       self.assertIsNone(error);self.assertEqual(result['replayed'],replay)
       if replay:self.assertEqual(release_facts(c),before)
       else:self.assertEqual(c['world'].snapshot()['revision'],before['snapshot']['revision']+1)
      else:
       self.assertIn(error,('ACTIVATION_BINDING_CONFLICT','COORDINATOR_EXECUTION_CANCELLED'))
       self.assertIsNone(result);self.assertEqual(release_facts(c),before)
      self.assertEqual(len(c['calls']),1);self.assertIsNone(co.worker)
      TRACE.append({'case':'CLOSE-original-identity-pair','root':str(c['root']),'replay':replay,'seam':seam,'variant':variant,'hits':hits,'error':error,'result':result})


class TechnicalWindowCloseDeadlineLifecyclePairs(unittest.TestCase):
 def test_CLOSE_real_deadline_reader_done_tail_exit_cleanup_and_normal_reuse_are_distinct(self):
  from app.tests import test_actor_deadline as deadline_fixture
  from app.actor_execution import ExecutionPolicy
  from app.adjudication import Adjudicator
  from app.scripted_dm import ScriptedDM
  import time
  fixture=deadline_fixture.ActorDeadlineTests();person=deadline_fixture.PERSON
  for mode in ('normal_exit','held_tail','held_tail_successor'):
   with self.subTest(mode=mode),fixture.pair(execution_policy=ExecutionPolicy(act_deadline_seconds=.2)) as (w,h):
    h.adjudicator=Adjudicator(w);done=threading.Event();tail=threading.Event();observed=[];events=[];successor=None
    def provider(payload):
     observed.append((w._scoped_attempt(),threading.current_thread()))
     try:
      with fixture.locked(w):return ScriptedDM(w)(payload)
     finally:
      events.append(('real_reader_finally',time.monotonic()));done.set()
      if mode!='normal_exit':tail.wait(2)
      events.append(('provider_function_tail',time.monotonic()))
    h.dm_provider=provider;h._choose=lambda view,seen:({'action':'propose','text':'本轮暂候消息，不采取新的行动。'},seen)
    def lease():
     with closing(sqlite3.connect(w.db_path)) as db:
      return db.execute('SELECT epoch,owner_token FROM actor_execution_leases WHERE person_id=?',(person,)).fetchone()
    def facts():return (m1_fixture.logical(w.db_path),m1_fixture.logical(h.db_path))
    try:
     start=time.monotonic()
     with self.assertRaisesRegex(Exception,'ACTOR_EXECUTION_DEADLINE'):h.act(person)
     elapsed=time.monotonic()-start;self.assertLess(elapsed,.7)
     self.assertTrue(done.wait(.1));self.assertEqual(len(observed),1)
     attempt,thread=observed[0];self.assertIsNotNone(attempt);self.assertIn(thread,attempt.calls)
     self.assertEqual(attempt.permit.person_id,person);self.assertEqual(attempt.permit.world_id,w.snapshot()['world_id'])
     self.assertFalse(attempt.thread.is_alive());self.assertEqual(getattr(attempt.failure,'code',None),'ACTOR_EXECUTION_DEADLINE')
     with h._connect() as db:intent=db.execute('SELECT intent_id FROM actor_intents').fetchone()[0]
     self.assertIsNone(w.receipt(person,intent));before=facts()
     if mode!='normal_exit':
      self.assertTrue(thread.is_alive());self.assertIn(thread,h._pending_threads)
      self.assertEqual(len(h._lease_cleanups),1)
      issuer,permit,original=h._lease_cleanups[0]
      self.assertIs(issuer,w);self.assertIs(permit,attempt.permit);self.assertIs(original,attempt)
      self.assertEqual(lease(),(permit.epoch,permit.owner_token))
      with self.assertRaisesRegex(Exception,'ACTOR_EXECUTION_CLEANUP_PENDING'):h.act(person)
      self.assertEqual(facts(),before);self.assertEqual(len(observed),1)
      self.assertTrue(thread.is_alive());self.assertTrue(h.cleanup_pending)
     tail.set();thread.join(.1);self.assertFalse(thread.is_alive())
     self.assertTrue(all(not t.is_alive() for t in attempt.calls))
     # Registration may still contain an exited thread. Only a real admission
     # retires it; done.set and thread exit do not mutate Host registries.
     if mode!='normal_exit':self.assertIn(thread,h._pending_threads)
     if mode=='held_tail_successor':
      self.assertIs(w.release_actor_execution(attempt.permit),True)
      successor=w.acquire_actor_execution(person,'CLOSE-real-successor')
      held=lease();self.assertEqual(held,(successor.epoch,successor.owner_token))
      with self.assertRaises(WorldError) as denied:h.act(person)
      self.assertEqual(denied.exception.code,'ACTOR_EXECUTION_BUSY')
      self.assertEqual(lease(),held);self.assertFalse(h._lease_cleanups);self.assertFalse(h._pending_threads)
      self.assertEqual(len(observed),1);self.assertIsNone(w.receipt(person,intent))
      self.assertIs(w.release_actor_execution(successor),True);successor=None
     h.dm_provider=ScriptedDM(w)
     result=h.act(person)
     self.assertEqual(result['intent_id'],intent);self.assertIsNotNone(w.receipt(person,intent))
     self.assertFalse(h._pending_threads);self.assertFalse(h._lease_cleanups);self.assertFalse(h.cleanup_pending)
     self.assertIsNone(lease()[1]);self.assertFalse(h._closed)
     TRACE.append({'case':'CLOSE-deadline-lifecycle-pair','mode':mode,'elapsed':elapsed,'done_wait_bound':.1,'join_bound':.1,'original_epoch':attempt.permit.epoch,'real_reader_scope':True,'events':events,'original_intent_reused':result['intent_id']==intent,'cleanup_retired_by_normal_admission':True,'provider_thread_exited':not thread.is_alive()})
    finally:
     tail.set()
     for attempt,thread in observed:thread.join(2)
     if successor is not None:w.release_actor_execution(successor)


class TechnicalWindowCloseEntryCompatibility(unittest.TestCase):
 def test_CLOSE_unsupported_entry_Host_keeps_original_backend_diagnostic(self):
  with released_case('CLOSE-invalid-entry-backend') as c:
   co=c['coordinator'];h=c['host'];before=release_facts(c)
   try:
    co.actors=object()
    with self.assertRaises(WorldError) as error:release_case(c)
    self.assertEqual(error.exception.code,'SCHEDULER_BACKEND_UNSUPPORTED')
   finally:co.actors=h
   self.assertEqual(release_facts(c),before)
   self.assertFalse(release_case(c)['replayed']);self.assertEqual(len(c['calls']),1)

