"""Actual SQLite rollback controls (unchanged behavior characterization)."""
from contextlib import closing
import sqlite3,unittest
from app.tests import test_actor_activations as fixtures

class ActivationTransactionTests(unittest.TestCase):
 registered=fixtures.ActorActivationTests.registered
 def test_characterization_adjudicated_effect_abort_rolls_back_children_queue_and_phase(self):
  from app.adjudication import Adjudicator
  from app.world import WorldError
  with self.registered() as (world,host,owner,ref,requests):
   with world._coordinator_scope(owner):host.adjudicator=Adjudicator(world)
   host._choose=lambda view,seen:({'action':'propose','text':'original compound rollback'},seen)
   def dm(payload):
    s=payload['world']
    return dict(world_id=s['world_id'],base_revision=s['revision'],ruling='成功',reason='script',goal='queue two',costs=[],benefits=[],effects=[{'action':'investigate'},{'action':'investigate'}])
   host.dm_provider=dm
   before=world.snapshot()
   with closing(sqlite3.connect(world.db_path)) as db:
    db.execute("CREATE TRIGGER injected_adjudication_abort BEFORE UPDATE OF phase ON coordinator_activations WHEN NEW.phase='world_committed' BEGIN SELECT RAISE(ABORT,'injected adjudication commit failure'); END");db.commit()
   with self.assertRaises(WorldError) as caught:host.act(ref.person_id,coordinator=owner,activation=ref)
   self.assertEqual(caught.exception.code,'ADJUDICATION_FAILED')
   self.assertEqual(world.snapshot()['queue'],before['queue'])
   self.assertEqual(world.snapshot()['places'],before['places'])
   self.assertEqual(world.snapshot()['status'],'technical_paused')
   with closing(sqlite3.connect(world.db_path)) as db:
    self.assertEqual(db.execute('SELECT phase,resume_phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('technical_blocked','candidate',0))
    rows=db.execute('SELECT id,receipt FROM intents').fetchall()
    self.assertEqual(len(rows),1);self.assertIsNone(rows[0][1]);self.assertFalse(rows[0][0].startswith('dm:'))
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    self.assertNotIn('coordinator_activation_outcomes',tables)
   with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchall(),[(0,)])

 def test_characterization_prepare_candidate_effect_finish_rollback(self):
  for target,prior,private_state in [('prepared','pending',None),('candidate','prepared','prepared'),('world_committed','candidate','candidate'),('completed','world_committed','candidate')]:
   with self.subTest(target=target),self.registered() as (world,host,owner,ref,requests):
    before=world.snapshot()
    with closing(sqlite3.connect(world.db_path)) as db:
     db.execute("CREATE TRIGGER injected_phase_abort BEFORE UPDATE OF phase ON coordinator_activations WHEN NEW.phase='"+target+"' BEGIN SELECT RAISE(ABORT,'injected actual transaction phase failure'); END")
     db.commit()
    with self.assertRaisesRegex(sqlite3.IntegrityError,'injected actual transaction'):
     host.act(ref.person_id,coordinator=owner,activation=ref)
    with closing(sqlite3.connect(world.db_path)) as db:
     phase=db.execute('SELECT phase,intent_id,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
     self.assertEqual(phase[0],prior);self.assertEqual(phase[2],0)
     tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
     outcomes=db.execute('SELECT count(*) FROM coordinator_activation_outcomes').fetchone()[0] if 'coordinator_activation_outcomes' in tables else 0
     self.assertEqual(outcomes,1 if target=='completed' else 0)
     self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone()[0],1 if target=='completed' else 0)
     db.execute('DROP TRIGGER injected_phase_abort');db.commit()
    with closing(sqlite3.connect(host.db_path)) as db:
     rows=db.execute('SELECT intent_id,state,candidate,receipt FROM actor_intents').fetchall()
     self.assertEqual(len(rows),int(private_state is not None))
     if rows:
      self.assertEqual(rows[0][1],private_state);self.assertIsNone(rows[0][3])
      self.assertEqual(rows[0][2] is not None,private_state=='candidate')
     self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchall(),[(0,)])
    if target!='completed':self.assertEqual(world.snapshot(),before)
    result=host.act(ref.person_id,coordinator=owner,activation=ref)
    if phase[1] is not None:self.assertEqual(result['intent_id'],phase[1])
    with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchall(),[(1,)])
