"""Phase proof must precede any new actor ownership write."""
from contextlib import closing
import sqlite3
import unittest
from app.tests import test_actor_activations as fixtures
from app.world import WorldError

class ActivationPhaseProofTests(unittest.TestCase):
    registered=fixtures.ActorActivationTests.registered
    def test_acquire_final_gate_rejects_new_conflicting_actual_host(self):
        from app.actors import ActorHost
        from pathlib import Path
        with self.registered() as (world,host,owner,ref,requests):
            before={p:p.read_bytes() for p in (world.db_path,host._storage.actor_path)}
            floor=world._execution_floor;others=[]
            def interleave(db,now):
                floor(db,now)
                others.append(ActorHost(world,Path(host.db_path).with_name('conflicting.sqlite')))
            world._execution_floor=interleave
            try:
                with self.assertRaises(WorldError) as caught:
                    world.acquire_actor_execution(ref.person_id,'final-proof-owner',coordinator=owner,activation=ref)
                self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                self.assertEqual({p:p.read_bytes() for p in before},before)
                self.assertFalse(Path(host.db_path).with_name('conflicting.sqlite').exists())
            finally:
                world._execution_floor=floor
                for other in others:other.close()

    def test_malformed_phase_payload_is_recovery_local_stop_not_technical_pause(self):
        with self.registered() as (world,host,owner,ref,requests):
            submit=host._submit
            def interleave(person,intent,body,*,execution=None):
                with closing(sqlite3.connect(host.db_path)) as db:
                    db.execute("UPDATE actor_intents SET body='{' WHERE intent_id=?",(intent,));db.commit()
                return submit(person,intent,body,execution=execution)
            host._submit=interleave
            with self.assertRaises(WorldError) as caught:
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            self.assertEqual(world.snapshot()['status'],'running')

    def test_effect_binds_body_to_original_hashed_request_not_mutable_body_column(self):
        import json
        with self.registered() as (world,host,owner,ref,requests):
            submit=host._submit
            def interleave(person,intent,body,*,execution=None):
                replacement={'action':'investigate'}
                with closing(sqlite3.connect(host.db_path)) as db:
                    db.execute('UPDATE actor_intents SET body=? WHERE intent_id=?',(json.dumps(replacement),intent));db.commit()
                try:
                    with self.assertRaises(WorldError) as caught:world.submit(person,intent,replacement,execution=execution)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(0,))
                finally:
                    with closing(sqlite3.connect(host.db_path)) as db:
                        db.execute('UPDATE actor_intents SET body=? WHERE intent_id=?',(json.dumps(body),intent));db.commit()
                return submit(person,intent,body,execution=execution)
            host._submit=interleave
            self.assertEqual(host.act(ref.person_id,coordinator=owner,activation=ref)['body'],{'action':'wait'})

    def test_completed_receipt_identity_cannot_be_laundered_by_matching_hashes(self):
        import json
        from app.actor_execution import digest,canonical
        from app.coordinator_scheduler import activation_scope
        with self.registered() as (world,host,owner,ref,requests):
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            execution=world.acquire_actor_execution(ref.person_id,'receipt-proof',coordinator=owner,activation=ref)
            receipt=dict(result['receipt'],intent_id='f'*32)
            raw=canonical(receipt)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('UPDATE intents SET receipt=? WHERE id=?',(raw,result['intent_id']))
                db.execute('UPDATE coordinator_activations SET receipt_sha256=? WHERE activation_id=?',(digest(raw),ref.activation_id))
                db.execute('UPDATE coordinator_activation_outcomes SET outcome_json=?,outcome_sha256=? WHERE activation_id=?',(raw,digest(raw),ref.activation_id));db.commit()
            with closing(sqlite3.connect(host.db_path)) as db:
                db.execute('UPDATE actor_intents SET receipt=? WHERE intent_id=?',(raw,result['intent_id']));db.commit()
            try:
                with world._coordinator_scope(owner),activation_scope(world,ref):
                    with self.assertRaises(WorldError) as caught:
                        world.submit(ref.person_id,result['intent_id'],result['body'],execution=execution,activation=ref)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            finally:world.release_actor_execution(execution)

    def test_candidate_phase_requires_actual_candidate_state_at_effect(self):
        with self.registered() as (world,host,owner,ref,requests):
            submit=host._submit
            def interleave(person,intent,body,*,execution=None):
                with closing(sqlite3.connect(host.db_path)) as db:
                    db.execute("UPDATE actor_intents SET state='prepared' WHERE intent_id=?",(intent,));db.commit()
                try:
                    with self.assertRaises(WorldError) as caught:submit(person,intent,body,execution=execution)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(0,))
                finally:
                    with closing(sqlite3.connect(host.db_path)) as db:
                        db.execute("UPDATE actor_intents SET state='candidate' WHERE intent_id=?",(intent,));db.commit()
                return submit(person,intent,body,execution=execution)
            host._submit=interleave
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['receipt']['status'],'committed')

    def test_terminal_bits_cannot_launder_missing_receipt_or_private_history(self):
        for completed_window in (False,True):
            with self.subTest(completed_window=completed_window), self.registered() as (world,host,owner,ref,requests):
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute("UPDATE coordinator_activations SET phase='completed',private_closed=1")
                    if completed_window:
                        db.execute("UPDATE coordinator_windows SET state='completed',completed_revision=advanced_revision")
                    db.commit()
                before={p:p.read_bytes() for p in (world.db_path,host._storage.actor_path)}
                version={k:world.snapshot()[k] for k in ('world_id','revision')}
                calls=[lambda:world.next_activation(ref.window_id,coordinator=owner),
                       lambda:world.complete_window(ref.window_id,coordinator=owner),
                       lambda:world.open_or_recover_window(coordinator=owner,expected_version=version)]
                for call in calls:
                    with self.assertRaises(WorldError) as caught:call()
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    self.assertEqual({p:p.read_bytes() for p in before},before)

    def test_corrupt_phase_is_rejected_before_lease_or_owner_sql(self):
        for phase in ('prepared','candidate','world_committed','completed'):
            with self.subTest(phase=phase), self.registered() as (world,host,owner,ref,requests):
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute('UPDATE coordinator_activations SET phase=?,intent_id=?,private_closed=? WHERE activation_id=?',(phase,'e'*32,int(phase=='completed'),ref.activation_id));db.commit()
                before={p:p.read_bytes() for p in (world.db_path,host._storage.actor_path)}
                statements=[]
                connection=world._execution_connection
                from contextlib import contextmanager
                @contextmanager
                def traced(*args,**kwargs):
                    with connection(*args,**kwargs) as db:
                        db.set_trace_callback(statements.append)
                        yield db
                world._execution_connection=traced
                try:
                    with self.assertRaises(WorldError) as caught:
                        world.acquire_actor_execution(ref.person_id,'must-not-write',coordinator=owner,activation=ref)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                    self.assertFalse(any(s.lstrip().upper().startswith(('INSERT','UPDATE','CREATE')) for s in statements),statements)
                    self.assertEqual({p:p.read_bytes() for p in before},before)
                finally:
                    world._execution_connection=connection
