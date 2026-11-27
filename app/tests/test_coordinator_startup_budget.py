"""Production startup budgets: real source/SQLite waits, no provider or model."""
from contextlib import closing, contextmanager
from pathlib import Path
import sqlite3,tempfile,threading,time,unittest
from app.world import WorldStore,WorldError
from app.server import Coordinator
from app.actors import ActorHost
from app.actor_execution import ExecutionPolicy,EntryBudget

class CoordinatorStartupBudgetTests(unittest.TestCase):
 @contextmanager
 def setup_owner(self, *, missing=False, busy=.025, policy=None):
  with tempfile.TemporaryDirectory(prefix='p023b4-A-R2-',dir='C:/Users/We3q/AppData/Local/hermes/cache/scratch') as directory:
   root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
   owner=Coordinator(world,None,execution_policy=policy or ExecutionPolicy(busy_budget_seconds=busy))
   if missing:
    owner.acquire_owner();host=ActorHost(world,root/'actors.sqlite')
   else:
    host=ActorHost(world,root/'actors.sqlite');owner.acquire_owner()
   owner.actors=host
   try:yield world,host,owner
   finally:
    if owner.worker:owner.worker.join(3)
    owner.close()
    self.assertFalse(owner.cleanup_pending)
 @staticmethod
 def facts(world):
  with closing(sqlite3.connect(world.db_path)) as db:
   return list(db.execute('SELECT seq,state FROM snapshots')),list(db.execute('SELECT * FROM intents'))
 def held_sql(self,world,owner,path,operation, *, wait=.10):
  entered=threading.Event();done=threading.Event();errors=[];values=[]
  def run():
   entered.set()
   try:values.append(operation())
   except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
   finally:done.set()
  worker=threading.Thread(target=run,name='startup-sql-budget')
  try:
   with closing(sqlite3.connect(path)) as blocker:
    blocker.execute('BEGIN EXCLUSIVE');worker.start();self.assertTrue(entered.wait(1));timely=done.wait(wait);blocker.rollback()
  finally:worker.join(3)
  self.assertFalse(worker.is_alive());return timely,errors,values
 def test_preflight_sql_contention_short_policy_characterization(self):
  with self.setup_owner() as (world,host,owner):
   before=self.facts(world)
   timely,errors,values=self.held_sql(world,owner,world.db_path,lambda:owner._prepare_scheduler(owner._owner_attempt))
   self.assertTrue(timely,'actual preflight main SELECT must honor .025s entry, not root infinity')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertEqual(values,[])
   self.assertEqual(self.facts(world),before);self.assertFalse(owner.busy)
   self.assertEqual(owner._owner_attempt.deadline,float('inf'))
   self.assertIsNone(world._scoped_entry_budget());self.assertIsNone(world._scoped_coordinator_attempt())
 def test_preflight_main_sql_keeps_budget_consumed_before_connection(self):
  with self.setup_owner(busy=.2) as (world,host,owner):
   before=self.facts(world);actual=world._connection;seen=[]
   @contextmanager
   def delayed(**kwargs):
    if not seen:
     seen.append(1);threading.Event().wait(.16)
    with actual(**kwargs) as db:yield db
   world._connection=delayed
   try:timely,errors,values=self.held_sql(world,owner,world.db_path,lambda:owner._prepare_scheduler(owner._owner_attempt),wait=.26)
   finally:world._connection=actual
   self.assertTrue(timely,'main SQL cannot restart .2s after .16s of preparation')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertEqual(values,[]);self.assertEqual(self.facts(world),before)
 def test_native_sql_backoff_never_restarts_consumed_entry_busy_budget(self):
  with self.setup_owner(busy=.6) as (world,host,owner):
   actual_connection=world._connection;original_wait=owner._closed.wait;seen=[];delayed=[]
   @contextmanager
   def connection(**kwargs):
    if not delayed:delayed.append(1);original_wait(.3)
    with actual_connection(**kwargs) as db:yield db
   def wait(timeout=None):
    entry=world._scoped_entry_budget()
    if entry is not None:seen.append((timeout,max(0,entry.busy_deadline-time.monotonic())))
    return original_wait(timeout)
   world._connection=connection;owner._closed.wait=wait
   try:
    timely,errors,values=self.held_sql(world,owner,world.db_path,lambda:owner._prepare_scheduler(owner._owner_attempt),wait=.8)
   finally:world._connection=actual_connection;owner._closed.wait=original_wait
   print('ACTUAL_SQL_BACKOFF_REMAINING',seen)
   self.assertTrue(timely);self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertTrue(seen)
   self.assertTrue(all(timeout<=remaining+.003 for timeout,remaining in seen),seen)

 def test_newly_acquired_original_root_failure_wins_during_admission_sql(self):
  with tempfile.TemporaryDirectory(prefix='p023b4-new-root-R2-',dir='C:/Users/We3q/AppData/Local/hermes/cache/scratch') as directory:
   root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite')
   owner=Coordinator(world,host,execution_policy=ExecutionPolicy(busy_budget_seconds=.3));self.assertIsNone(owner._owner_attempt)
   actual=owner._prepare_scheduler;blockers=[];original_error=WorldError('EXECUTION_CLOCK_UNSAFE','captured newly acquired root failed')
   def prepare(captured):
    result=actual(captured)
    blocker=sqlite3.connect(world.db_path);blocker.execute('BEGIN EXCLUSIVE');blockers.append(blocker)
    captured.failure=original_error
    return result
   owner._prepare_scheduler=prepare
   try:
    with self.assertRaises(WorldError) as caught:owner.start()
    self.assertIs(caught.exception,original_error,'new root authority failure must not be hidden by outer pre-root budget SQL BUSY')
    self.assertIsNone(owner.worker);self.assertFalse(owner.busy)
    self.assertIsNone(world._scoped_entry_budget());self.assertIsNone(world._scoped_coordinator_attempt())
   finally:
    for blocker in blockers:blocker.rollback();blocker.close()
    if owner._owner_attempt:owner._owner_attempt.failure=None
    owner.close();self.assertFalse(owner.cleanup_pending)

 def test_characterization_owned_empty_retry_never_accepts_replacement_data_or_failed_root(self):
  from unittest.mock import patch
  import os
  for mutation in ('replace','data','root'):
   with self.subTest(mutation=mutation),self.setup_owner(missing=True,busy=.1) as (world,host,owner):
    original=host._scheduler_bootstrap_guard
    def expire(db,**kwargs):
     value=original(db,**kwargs)
     if kwargs.get('final'):threading.Event().wait(.14)
     return value
    with patch.object(host,'_scheduler_bootstrap_guard',expire),self.assertRaises(WorldError):owner._prepare_scheduler(owner._owner_attempt)
    marker=host._storage._bootstrap_file;self.assertIsNotNone(marker);owner.execution_policy=ExecutionPolicy()
    if mutation=='replace':
     replacement=host._storage.actor_path.with_suffix('.replacement');replacement.write_bytes(b'');os.replace(replacement,host._storage.actor_path)
    elif mutation=='data':
     with closing(sqlite3.connect(host.db_path)) as db:db.execute('CREATE TABLE foreign_fact (id INTEGER)');db.commit()
    else:owner._owner_attempt.failure=WorldError('EXECUTION_CLOCK_UNSAFE','original root failure')
    before=self.facts(world)
    with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
    self.assertEqual(caught.exception.code,'EXECUTION_CLOCK_UNSAFE' if mutation=='root' else 'WINDOW_RECOVERY_UNPROVEN')
    self.assertEqual(self.facts(world),before)
    owner._owner_attempt.failure=None

 def test_same_host_retry_after_rolled_back_bootstrap_uses_only_own_file(self):
  from unittest.mock import patch
  with self.setup_owner(missing=True,busy=.1) as (world,host,owner):
   original=host._scheduler_bootstrap_guard
   def expire(db,**kwargs):
    result=original(db,**kwargs)
    if kwargs.get('final'):threading.Event().wait(.14)
    return result
   with patch.object(host,'_scheduler_bootstrap_guard',expire),self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
   self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY')
   self.assertTrue(host._storage.actor_path.exists());owner.execution_policy=ExecutionPolicy()
   result=owner._prepare_scheduler(owner._owner_attempt)
   self.assertEqual(result['world_id'],world.snapshot()['world_id']);self.assertEqual(self.facts(world)[1],[])
  # A caller-created empty file carries no same-host creation provenance.
  with self.setup_owner(missing=True,busy=2) as (world,host,owner):
   with closing(sqlite3.connect(host.db_path)):pass
   with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
   self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')

 def test_characterization_register_actual_private_begin_contention(self):
  with self.setup_owner(busy=.08) as (world,host,owner):
   before=self.facts(world);opens=[];actual=host._storage._open
   def observed(attempt):opens.append(attempt);return actual(attempt)
   host._storage._open=observed
   with closing(sqlite3.connect(host.db_path)) as blocker:
    blocker.execute('BEGIN IMMEDIATE');at=time.monotonic()
    with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
    elapsed=time.monotonic()-at;blocker.rollback()
   self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY');self.assertLess(elapsed,.25)
   self.assertTrue(opens);self.assertTrue(all(isinstance(x,EntryBudget) for x in opens));self.assertEqual(self.facts(world),before)

 def test_characterization_bootstrap_and_register_final_sql_gates_roll_back(self):
  from unittest.mock import patch
  from app import coordinator_scheduler as scheduler
  for missing in (False,True):
   with self.subTest(missing=missing),self.setup_owner(missing=missing,busy=.1) as (world,host,owner):
    before=self.facts(world)
    if missing:
     original=host._scheduler_bootstrap_guard
     def guard(db,**kwargs):
      result=original(db,**kwargs)
      if kwargs.get('final'):threading.Event().wait(.14)
      return result
     context=patch.object(host,'_scheduler_bootstrap_guard',guard)
    else:
     original=scheduler.schema
     def schema(db):original(db);threading.Event().wait(.14)
     context=patch.object(scheduler,'schema',schema)
    with context,self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
    self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY');self.assertEqual(self.facts(world),before)
    with closing(sqlite3.connect(world.db_path)) as db:
     self.assertEqual(list(db.execute("SELECT name FROM sqlite_master WHERE name='coordinator_scheduler_registration'")),[])
    if missing:
     with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(list(db.execute('SELECT name FROM sqlite_master')),[])

 def test_characterization_original_actor_and_root_scopes_are_not_replaced(self):
  from app.actor_execution import Attempt
  from app.coordinator_scheduler import ActivationRef
  with self.setup_owner(busy=2) as (world,host,owner):
   root=owner._owner_attempt;owner._prepare_scheduler(root)
   with owner._owner_scope(root):
    world.control('resume')
    state=world.snapshot();window=world.open_or_recover_window(coordinator=root.permit,expected_version={k:state[k] for k in ('world_id','revision')})
    row=world.next_activation(window['window_id'],coordinator=root.permit)
    ref=ActivationRef(row['world_id'],row['window_id'],row['activation_id'],row['person_id'])
    permit=world.acquire_actor_execution(ref.person_id,'startup-original-actor',coordinator=root.permit,activation=ref)
    actor=Attempt(permit,ExecutionPolicy(act_deadline_seconds=.04),coordinator=root.permit,parent_attempt=root,activation=ref)
    try:
     with world._actor_attempt(actor):
      threading.Event().wait(.05)
      with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(root)
      self.assertIs(caught.exception,actor.failure);self.assertEqual(caught.exception.code,'ACTOR_EXECUTION_DEADLINE')
      self.assertIs(world._scoped_attempt(),actor);self.assertIs(world._scoped_coordinator_attempt(),root);self.assertIsNone(world._scoped_entry_budget())
    finally:world.release_actor_execution(permit)
   self.assertIsNone(world._scoped_attempt());self.assertIsNone(world._scoped_coordinator_attempt());self.assertEqual(root.deadline,float('inf'))

 def test_characterization_root_heartbeat_survives_entry_budget_failure(self):
  with self.setup_owner(policy=ExecutionPolicy(ttl_seconds=3,renew_interval_seconds=.03,busy_budget_seconds=.08)) as (world,host,owner):
   root=owner._owner_attempt
   def expiry():
    with closing(sqlite3.connect(world.db_path)) as db:return db.execute('SELECT expires_wall_ns FROM coordinator_execution_leases').fetchone()[0]
   # Observe renewals of this fixture's original three-second root lease.
   threading.Event().wait(.07);before=expiry();done=threading.Event();errors=[]
   def prepare():
    try:owner._prepare_scheduler(root)
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
    finally:done.set()
   worker=threading.Thread(target=prepare,name='startup-root-maintenance')
   try:
    with world._private_sources_lock:
     worker.start();self.assertTrue(done.wait(.3));threading.Event().wait(.1);after=expiry()
   finally:worker.join(3)
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertGreater(after,before);self.assertIs(owner._owner_attempt,root)
   self.assertTrue(root.thread.is_alive());self.assertFalse(root.stop.is_set());self.assertEqual(root.deadline,float('inf'))

 def test_characterization_source_budget_both_existing_and_deferred_no_close(self):
  for missing in (False,True):
   with self.subTest(missing=missing),self.setup_owner(missing=missing,busy=.08) as (world,host,owner):
    before=self.facts(world);done=threading.Event();errors=[]
    def prepare():
     try:owner._prepare_scheduler(owner._owner_attempt)
     except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
     finally:done.set()
    worker=threading.Thread(target=prepare,name='startup-source-budget')
    try:
     with world._private_sources_lock:
      worker.start();timely=done.wait(.22)
    finally:worker.join(3)
    self.assertFalse(worker.is_alive());self.assertTrue(timely);self.assertEqual(errors,['EXECUTION_STORAGE_BUSY'])
    self.assertEqual(self.facts(world),before);self.assertFalse(owner.busy);self.assertIsNone(owner.worker)
    self.assertEqual(owner._owner_attempt.deadline,float('inf'))
    if missing:self.assertFalse(host._storage.actor_path.exists())
    owner.execution_policy=ExecutionPolicy();owner._prepare_scheduler(owner._owner_attempt)
    self.assertEqual(self.facts(world),before)

 def test_characterization_source_release_in_time_succeeds(self):
  with self.setup_owner(missing=True,busy=2) as (world,host,owner):
   entered=threading.Event();errors=[];results=[]
   def prepare():
    entered.set()
    try:results.append(owner._prepare_scheduler(owner._owner_attempt))
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
   worker=threading.Thread(target=prepare,name='startup-source-release')
   try:
    with world._private_sources_lock:
     worker.start();self.assertTrue(entered.wait(1));threading.Event().wait(.04)
   finally:worker.join(3)
   self.assertFalse(worker.is_alive());self.assertEqual(errors,[]);self.assertEqual(len(results),1)
   self.assertEqual(results[0]['world_id'],world.snapshot()['world_id']);self.assertEqual(self.facts(world)[1],[])

 def test_characterization_close_cancels_while_source_stays_held(self):
  for missing in (False,True):
   with self.subTest(missing=missing),self.setup_owner(missing=missing,busy=2) as (world,host,owner):
    entered=threading.Event();done=threading.Event();closed=threading.Event();errors=[]
    actual=owner._prepare_scheduler_bounded
    def observed(captured):entered.set();return actual(captured)
    owner._prepare_scheduler_bounded=observed
    def prepare():
     try:owner._prepare_scheduler(owner._owner_attempt)
     except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
     finally:done.set()
    def close():owner.close();closed.set()
    worker=threading.Thread(target=prepare,name='startup-source-close');closer=threading.Thread(target=close,name='startup-owned-close')
    try:
     with world._private_sources_lock:
      worker.start();self.assertTrue(entered.wait(1));closer.start();timely=closed.wait(.5);returned=done.is_set()
    finally:
     worker.join(3)
     if closer.ident:closer.join(3)
    self.assertTrue(timely);self.assertTrue(returned);self.assertFalse(worker.is_alive());self.assertFalse(closer.is_alive())
    self.assertEqual(errors,['ACTOR_EXECUTION_CANCELLED']);self.assertFalse(owner.cleanup_pending)

 def test_characterization_registration_native_private_and_main_contention(self):
  for private in (False,True):
   with self.subTest(private=private),self.setup_owner(busy=.08) as (world,host,owner):
    before=self.facts(world)
    if private:
     timely,errors,values=self.held_sql(world,owner,host.db_path,lambda:owner._prepare_scheduler(owner._owner_attempt),wait=.25)
    else:
     with closing(sqlite3.connect(world.db_path)) as blocker:
      blocker.execute('BEGIN IMMEDIATE')
      with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
      errors=[caught.exception.code];timely=True;values=[];blocker.rollback()
    self.assertTrue(timely);self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertEqual(values,[]);self.assertEqual(self.facts(world),before)
    owner.execution_policy=ExecutionPolicy();owner._prepare_scheduler(owner._owner_attempt)

 def test_characterization_outer_entry_root_restore_and_original_failure_priority(self):
  with self.setup_owner(busy=2) as (world,host,owner):
   owner._prepare_scheduler(owner._owner_attempt);captured=owner._owner_attempt
   outer=EntryBudget(ExecutionPolicy(busy_budget_seconds=.04),threading.Event())
   with world._coordinator_scope(captured.permit,captured),world._entry_budget(outer):
    threading.Event().wait(.05)
    with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(captured)
    self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY');self.assertIs(world._scoped_entry_budget(),outer)
    self.assertIs(world._scoped_coordinator_attempt(),captured)
   self.assertIsNone(world._scoped_entry_budget());self.assertIsNone(world._scoped_coordinator_attempt())
   captured.failure=WorldError('EXECUTION_CLOCK_UNSAFE','original unsafe root')
   with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(captured)
   self.assertIs(caught.exception,captured.failure);captured.failure=None
   owner._prepare_scheduler(captured);self.assertEqual(captured.deadline,float('inf'));self.assertFalse(captured.stop.is_set())

 def test_actual_main_factory_private_attach_has_same_finite_startup_budget(self):
  from unittest.mock import patch
  from app import server
  with tempfile.TemporaryDirectory(prefix='p023b4-main-R2-',dir='C:/Users/We3q/AppData/Local/hermes/cache/scratch') as directory:
   at_factory=threading.Event();locked=threading.Event();done=threading.Event();errors=[];owners=[]
   actual_factory=server.create_default_actors
   class ShortCoordinator(Coordinator):
    def __init__(self,*args,**kwargs):
     super().__init__(*args,execution_policy=ExecutionPolicy(busy_budget_seconds=.3),**kwargs);owners.append(self)
   def factory(world,path):
    at_factory.set();self.assertTrue(locked.wait(2));return actual_factory(world,path)
   def main():
    try:server.main(['--port','0','--data-dir',directory])
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
    finally:done.set()
   worker=threading.Thread(target=main,name='startup-real-main-factory')
   with patch.object(server,'Coordinator',ShortCoordinator),patch.object(server,'create_default_actors',factory),patch.object(server,'make_server',side_effect=AssertionError('no listener allowed')):
    try:
     worker.start();self.assertTrue(at_factory.wait(3))
     with closing(sqlite3.connect(Path(directory)/'actors.sqlite')) as blocker:
      blocker.execute('BEGIN EXCLUSIVE');locked.set();timely=done.wait(.4);blocker.rollback()
    finally:locked.set();worker.join(4)
   self.assertFalse(worker.is_alive());self.assertTrue(timely,'main factory actual private ATTACH must not regain native .25s/root infinity')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertEqual(len(owners),1);self.assertFalse(owners[0].cleanup_pending)

 def test_expired_admission_never_publishes_worker_after_control_returns(self):
  with self.setup_owner(busy=2) as (world,host,owner):
   owner._prepare_scheduler(owner._owner_attempt);owner.execution_policy=ExecutionPolicy(busy_budget_seconds=.1)
   actual=world.control
   def delayed(action):
    result=actual(action);threading.Event().wait(.14);return result
   world.control=delayed
   def no_open(**kwargs):raise WorldError('WINDOW_VERSION_CONFLICT','controlled worker stop')
   world.open_or_recover_window=no_open
   with self.assertRaises(WorldError) as caught:owner.start()
   self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY')
   self.assertIsNone(owner.worker,'expired synchronous preparation cannot launch asynchronous work')
   self.assertFalse(owner.busy)

 def test_start_original_owner_lock_wait_keeps_entry_budget(self):
  with self.setup_owner() as (world,host,owner):
   done=threading.Event();errors=[]
   def start():
    try:owner.start()
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
    finally:done.set()
   worker=threading.Thread(target=start,name='startup-owner-lock')
   try:
    with owner._owner_lock:
     worker.start();timely=done.wait(.1)
   finally:worker.join(3)
   self.assertFalse(worker.is_alive());self.assertTrue(timely,'captured-root admission cannot wait indefinitely for owner mutex')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertIsNone(owner.worker);self.assertFalse(owner.busy)

 def test_start_admission_lock_is_bounded_without_close(self):
  with self.setup_owner() as (world,host,owner):
   done=threading.Event();errors=[]
   def start():
    try:owner.start()
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
    finally:done.set()
   worker=threading.Thread(target=start,name='startup-admission-budget')
   try:
    with owner.lock:
     worker.start();timely=done.wait(.1)
   finally:worker.join(3)
   self.assertFalse(worker.is_alive());self.assertTrue(timely,'synchronous admission cannot wait outside entry')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertIsNone(owner.worker);self.assertFalse(owner.busy)

 def test_bootstrap_pipeline_lock_wait_is_finite_without_close(self):
  with self.setup_owner(missing=True) as (world,host,owner):
   done=threading.Event();errors=[]
   def prepare():
    try:owner._prepare_scheduler(owner._owner_attempt)
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
    finally:done.set()
   worker=threading.Thread(target=prepare,name='startup-pipeline-budget')
   try:
    with host._lock:
     worker.start();timely=done.wait(.1)
   finally:worker.join(3)
   self.assertFalse(worker.is_alive());self.assertTrue(timely,'pipeline must not block outside finite entry')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertFalse(host._storage.actor_path.exists())

 def test_failed_missing_bootstrap_retries_same_configured_source(self):
  with self.setup_owner(missing=True) as (world,host,owner):
   with closing(sqlite3.connect(world.db_path)) as blocker:
    blocker.execute('BEGIN IMMEDIATE')
    with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
    self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY');blocker.rollback()
   owner.execution_policy=ExecutionPolicy()
   result=owner._prepare_scheduler(owner._owner_attempt)
   self.assertEqual(result['world_id'],world.snapshot()['world_id'])
   self.assertEqual(self.facts(world)[1],[])

 def test_missing_bootstrap_actual_main_begin_is_bounded(self):
  with self.setup_owner(missing=True) as (world,host,owner):
   before=self.facts(world)
   # RESERVED main lock permits the pristine preflight but blocks actual BEGIN.
   done=threading.Event();errors=[]
   def prepare():
    try:owner._prepare_scheduler(owner._owner_attempt)
    except BaseException as error:errors.append(getattr(error,'code',type(error).__name__))
    finally:done.set()
   worker=threading.Thread(target=prepare,name='startup-bootstrap-main')
   try:
    with closing(sqlite3.connect(world.db_path)) as blocker:
     blocker.execute('BEGIN IMMEDIATE');worker.start();timely=done.wait(.1);blocker.rollback()
   finally:worker.join(3)
   self.assertFalse(worker.is_alive());self.assertTrue(timely,'real main+ATTACH bootstrap cannot spend native .25s outside entry')
   self.assertEqual(errors,['EXECUTION_STORAGE_BUSY']);self.assertEqual(self.facts(world),before)
   if host._storage.actor_path.exists():
    with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(list(db.execute('SELECT name FROM sqlite_master')),[])
