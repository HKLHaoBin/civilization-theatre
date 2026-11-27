"""R3 regressions and post-fix characterization; no SDK or fabricated permits."""
from contextlib import closing,contextmanager
from pathlib import Path
from unittest.mock import patch
import os,sqlite3,tempfile,threading,time,unittest
from app.world import WorldStore,WorldError
from app.server import Coordinator
from app.actors import ActorHost
from app.actor_execution import ExecutionPolicy,EntryBudget
SCRATCH='C:/Users/We3q/AppData/Local/hermes/cache/scratch'

class CoordinatorStartupR3Tests(unittest.TestCase):
 @contextmanager
 def fixture(self,missing=False,busy=2):
  with tempfile.TemporaryDirectory(prefix='p023b4-A-R3-',dir=SCRATCH) as directory:
   path=Path(directory);world=WorldStore(path/'world.sqlite');world.initialize()
   owner=Coordinator(world,None,execution_policy=ExecutionPolicy(busy_budget_seconds=busy))
   if missing:owner.acquire_owner()
   host=ActorHost(world,path/'actors.sqlite');owner.actors=host
   if not missing:owner.acquire_owner()
   try:yield world,host,owner
   finally:
    if owner.worker:owner.worker.join(12)
    owner.close();self.assertFalse(owner.cleanup_pending)
 @staticmethod
 def domain(world):
  with closing(sqlite3.connect(world.db_path)) as db:
   return list(db.execute('SELECT seq,state FROM snapshots')),list(db.execute('SELECT * FROM intents'))
 def rollback(self,world,host,owner):
  owner.execution_policy=ExecutionPolicy(busy_budget_seconds=.1);original=host._scheduler_bootstrap_guard
  def delayed(db,**kw):
   value=original(db,**kw)
   if kw.get('final'):threading.Event().wait(.14)
   return value
  with patch.object(host,'_scheduler_bootstrap_guard',delayed),self.assertRaises(WorldError) as caught:
   owner._prepare_scheduler(owner._owner_attempt)
  self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY')
  self.assertIsNotNone(host._storage._bootstrap_file)
  with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(list(db.execute('SELECT name FROM sqlite_master')),[])
  owner.execution_policy=ExecutionPolicy()
 def test_actual_start_main_and_private_waits_keep_original_absolute_remaining(self):
  # Integrated equivalent of the independently observed RED; all actual SQL.
  for private in (False,True):
   with self.subTest(private=private),self.fixture(busy=.6) as (world,host,owner):
    before=self.domain(world);wait=owner._closed.wait;acquire=owner.acquire_owner;seen=[]
    def delayed():
     result=acquire();wait(.3);return result
    def observed(timeout=None):
     entry=world._scoped_entry_budget()
     if entry is not None:
      stack=[entry];deadlines=[]
      while stack:
       item=stack.pop();deadlines.append(item.busy_deadline)
       stack.extend(x for x in item.captured if isinstance(x,EntryBudget))
      seen.append((timeout,min(deadlines)-time.monotonic(),entry.busy_deadline==min(deadlines)))
     return wait(timeout)
    with patch.object(owner,'acquire_owner',delayed),patch.object(owner._closed,'wait',observed):
     with closing(sqlite3.connect(host.db_path if private else world.db_path)) as blocker:
      blocker.execute('BEGIN EXCLUSIVE')
      with self.assertRaises(WorldError) as caught:owner.start()
      blocker.rollback()
    self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY');self.assertTrue(seen)
    self.assertTrue(all(timeout<=remaining+.003 and equal for timeout,remaining,equal in seen),seen)
    self.assertEqual(self.domain(world),before);self.assertIsNone(owner.worker);self.assertFalse(owner.busy)
    self.assertEqual(owner._owner_attempt.deadline,float('inf'));self.assertIsNone(world._scoped_entry_budget())
 def test_numeric_nested_busy_and_overall_deadlines_intersect_without_changing_root(self):
  with self.fixture() as (world,host,owner):
   root=owner._owner_attempt
   outer=EntryBudget(ExecutionPolicy(busy_budget_seconds=.08),threading.Event(),(root,))
   threading.Event().wait(.02)
   middle=EntryBudget(ExecutionPolicy(busy_budget_seconds=.5),threading.Event(),(root,outer))
   child=EntryBudget(ExecutionPolicy(busy_budget_seconds=2),threading.Event(),(root,middle,outer))
   self.assertEqual(middle.busy_deadline,outer.busy_deadline);self.assertEqual(child.busy_deadline,outer.busy_deadline)
   brief=EntryBudget(ExecutionPolicy(act_deadline_seconds=.01),threading.Event(),(child,))
   self.assertEqual(brief.busy_deadline,brief.deadline);self.assertLess(brief.busy_deadline,outer.busy_deadline)
   fault=WorldError('EXECUTION_CLOCK_UNSAFE','original root error');root.failure=fault
   try:
    with self.assertRaises(WorldError) as caught:child.check()
    self.assertIs(caught.exception,fault)
   finally:root.failure=None
   self.assertEqual(root.deadline,float('inf'));self.assertFalse(root.stop.is_set())
 def test_successor_released_root_cannot_consume_marker_or_register_or_create_tables(self):
  with self.fixture(missing=True) as (world,host,old):
   self.rollback(world,host,old);root=old._owner_attempt;origin=host._storage._bootstrap_origin;marker=host._storage._bootstrap_file
   self.assertTrue(root.finish());world.release_coordinator_execution(root.permit)
   new=Coordinator(world,host)
   try:
    new.acquire_owner();self.assertEqual(new._owner_attempt.permit.epoch,root.permit.epoch+1)
    self.assertNotEqual(root.permit,new._owner_attempt.permit);before=self.domain(world)
    with self.assertRaises(WorldError) as caught:new._prepare_scheduler(new._owner_attempt)
    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
    self.assertEqual(self.domain(world),before);self.assertIs(host._storage._bootstrap_origin,origin)
    self.assertEqual(host._storage._bootstrap_file,marker)
    # Independent native readback, not a registration API's return value.
    with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(list(db.execute('SELECT name FROM sqlite_master')),[])
    with closing(sqlite3.connect(world.db_path)) as db:
     self.assertEqual(list(db.execute("SELECT name FROM sqlite_master WHERE name IN ('coordinator_scheduler_registration','coordinator_windows','coordinator_activations')")),[])
    # The actual ATTACH transaction guard also refuses, independently of preflight.
    with new._owner_scope(new._owner_attempt),new._preparation_scope(new._owner_attempt):
     with self.assertRaises(WorldError) as caught:host._initialize_private(_scheduler_bootstrap=True)
    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
    with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(list(db.execute('SELECT name FROM sqlite_master')),[])
   finally:new.close();self.assertFalse(new.cleanup_pending)
 def test_same_original_root_retry_clears_provenance_only_after_real_commit(self):
  with self.fixture(missing=True) as (world,host,owner):
   self.rollback(world,host,owner);root=owner._owner_attempt;origin=host._storage._bootstrap_origin;marker=host._storage._bootstrap_file
   self.assertIs(origin[0],root);self.assertEqual(origin[1],root.permit);before=self.domain(world)
   result=owner._prepare_scheduler(root);self.assertEqual(result['world_id'],root.permit.world_id)
   self.assertIsNone(host._storage._bootstrap_origin);self.assertIsNone(host._storage._bootstrap_file)
   self.assertEqual(host._storage.actor_file,marker);self.assertEqual(self.domain(world),before)
   with closing(sqlite3.connect(host.db_path)) as db:
    self.assertEqual({row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")},{'actor_world_binding','actor_sessions','actor_intents','actor_calls'})
   with closing(sqlite3.connect(world.db_path)) as db:
    self.assertEqual(db.execute('SELECT count(*) FROM coordinator_scheduler_registration').fetchone()[0],1)
 def test_characterization_origin_never_adopts_foreign_changed_or_removed_empty_file(self):
  for change in ('foreign','replace','remove','table','failed_root','reconstruct'):
   with self.subTest(change=change),self.fixture(missing=True) as (world,host,owner):
    if change=='foreign':
     with closing(sqlite3.connect(host.db_path)):pass
    else:self.rollback(world,host,owner)
    if change=='replace':
     other=host._storage.actor_path.with_suffix('.other');other.write_bytes(b'');os.replace(other,host._storage.actor_path)
    elif change=='remove':host._storage.actor_path.unlink()
    elif change=='table':
     with closing(sqlite3.connect(host.db_path)) as db:db.execute('CREATE TABLE foreign_table (id INTEGER)');db.commit()
    elif change=='failed_root':owner._owner_attempt.failure=WorldError('EXECUTION_CLOCK_UNSAFE','original root error')
    elif change=='reconstruct':
     host.close();owner.actors=ActorHost(world,host.db_path)
    before=self.domain(world)
    try:
     with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
     self.assertEqual(caught.exception.code,'EXECUTION_CLOCK_UNSAFE' if change=='failed_root' else 'WINDOW_RECOVERY_UNPROVEN')
     self.assertEqual(self.domain(world),before)
    finally:owner._owner_attempt.failure=None
