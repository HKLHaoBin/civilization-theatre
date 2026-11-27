"""P023b3 B: registered activation technical failure and atomic pause witness."""
from contextlib import closing
import json
import sqlite3
import unittest

from app.tests import test_actor_activations as activation_fixtures


class ActivationTechnicalBlockingTests(unittest.TestCase):
    def test_blocked_prepared_requires_real_observation_and_card_version(self):
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            host._request=lambda payload: (_ for _ in ()).throw(RuntimeError('prepared stop'))
            with self.assertRaisesRegex(RuntimeError,'prepared stop'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('UPDATE coordinator_activations SET observation_revision=NULL,card_version=NULL WHERE activation_id=?',(ref.activation_id,));db.commit();before=list(db.iterdump())
            with self.assertRaises(WorldError) as caught:world.next_activation(ref.window_id,coordinator=owner)
            self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)

    def test_blocked_candidate_cannot_claim_private_promotion_without_closed_fact(self):
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            host._submit=lambda *args,**kwargs: (_ for _ in ()).throw(RuntimeError('candidate stop'))
            with self.assertRaisesRegex(RuntimeError,'candidate stop'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('UPDATE coordinator_activations SET promoted_version=1 WHERE activation_id=?',(ref.activation_id,));db.commit();before=list(db.iterdump())
            with self.assertRaises(WorldError) as caught:world.next_activation(ref.window_id,coordinator=owner)
            self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)

    def test_blocked_pending_cannot_claim_unpersisted_base_or_observation(self):
        from app.actor_execution import canonical,digest
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            host._act=lambda *args,**kwargs: (_ for _ in ()).throw(RuntimeError('pending stop'))
            with self.assertRaisesRegex(RuntimeError,'pending stop'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            with closing(sqlite3.connect(world.db_path)) as db:
                proof=json.loads(db.execute('SELECT proof_json FROM coordinator_activation_blocks').fetchone()[0]);proof['base_session_version']=0;raw=canonical(proof)
                db.execute('UPDATE coordinator_activations SET base_session_version=0 WHERE activation_id=?',(ref.activation_id,))
                db.execute('UPDATE coordinator_activation_blocks SET proof_json=?,proof_sha256=?',(raw,digest(raw)));db.commit();before=list(db.iterdump())
            with self.assertRaises(WorldError) as caught:world.next_activation(ref.window_id,coordinator=owner)
            self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)

    def test_blocked_owner_binding_corruption_cannot_be_washed_by_acquire(self):
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            host._request=lambda payload: (_ for _ in ()).throw(RuntimeError('prepared block'))
            with self.assertRaisesRegex(RuntimeError,'prepared block'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('UPDATE coordinator_activations SET owner_actor_epoch=999 WHERE activation_id=?',(ref.activation_id,));db.commit();before=list(db.iterdump())
            permit=None
            try:
                with self.assertRaises(WorldError) as caught:
                    permit=world.acquire_actor_execution(ref.person_id,'must-not-wash',coordinator=owner,activation=ref)
                self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)
            finally:
                if permit is not None:world.release_actor_execution(permit)

    def test_blocked_private_adoption_is_not_an_implicit_resume(self):
        from app.coordinator_scheduler import activation_scope
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            host._request=lambda payload: (_ for _ in ()).throw(RuntimeError('prepared block'))
            with self.assertRaisesRegex(RuntimeError,'prepared block'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            permit=world.acquire_actor_execution(ref.person_id,'blocked-adoption',coordinator=owner,activation=ref)
            try:
                with closing(sqlite3.connect(host.db_path)) as db:before=list(db.iterdump())
                with world._coordinator_scope(owner),activation_scope(world,ref):
                    with self.assertRaises(WorldError) as caught:
                        with host._private(permit,ref.person_id) as db:
                            host._storage.adopt(db,ref.person_id,permit,'must-not-adopt')
                    self.assertEqual(caught.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
                with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(list(db.iterdump()),before)
            finally:world.release_actor_execution(permit)

    def test_blocked_prepared_cannot_launder_candidate_hash_in_rehashed_witness(self):
        from app.actor_execution import canonical,digest
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            host._request=lambda payload: (_ for _ in ()).throw(RuntimeError('prepared block'))
            with self.assertRaisesRegex(RuntimeError,'prepared block'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            with closing(sqlite3.connect(world.db_path)) as db:
                proof=json.loads(db.execute('SELECT proof_json FROM coordinator_activation_blocks').fetchone()[0])
                proof['candidate_sha256']='f'*64;raw=canonical(proof)
                db.execute('UPDATE coordinator_activations SET candidate_sha256=? WHERE activation_id=?',('f'*64,ref.activation_id))
                db.execute('UPDATE coordinator_activation_blocks SET proof_json=?,proof_sha256=?',(raw,digest(raw)));db.commit()
                before=list(db.iterdump())
            with self.assertRaises(WorldError) as caught:world.next_activation(ref.window_id,coordinator=owner)
            self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
            with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)

    def test_blocked_direct_submit_cannot_even_persist_parent_intent(self):
        from app.coordinator_scheduler import activation_scope
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            host._request=lambda payload: (_ for _ in ()).throw(RuntimeError('prepared block'))
            with self.assertRaisesRegex(RuntimeError,'prepared block'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            permit=world.acquire_actor_execution(ref.person_id,'blocked-submit',coordinator=owner,activation=ref)
            try:
                with closing(sqlite3.connect(host.db_path)) as db:
                    intent,body=db.execute('SELECT intent_id,body FROM actor_intents').fetchone()
                with closing(sqlite3.connect(world.db_path)) as db:before=list(db.iterdump())
                with world._coordinator_scope(owner), activation_scope(world,ref):
                    with self.assertRaises(WorldError) as caught:
                        world.submit(ref.person_id,intent,json.loads(body),execution=permit)
                    self.assertEqual(caught.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
                with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)
            finally:world.release_actor_execution(permit)

    def test_blocked_pending_missing_head_cannot_hide_unproved_private_history(self):
        from app.coordinator_scheduler import activation_scope
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            host._act=lambda *args,**kwargs: (_ for _ in ()).throw(RuntimeError('pending stop'))
            with self.assertRaisesRegex(RuntimeError,'pending stop'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            permit=world.acquire_actor_execution(ref.person_id,'pending-maintenance',coordinator=owner,activation=ref)
            try:
                with closing(sqlite3.connect(host.db_path)) as db:
                    db.execute('INSERT OR REPLACE INTO actor_sessions VALUES (?,?,?)',(ref.person_id,'[{"role":"assistant","content":"unproved"}]','[]'));db.commit()
                with closing(sqlite3.connect(world.db_path)) as db:before=list(db.iterdump())
                with world._coordinator_scope(owner),activation_scope(world,ref):
                    with self.assertRaises(Exception) as caught:world.renew_actor_execution(permit)
                    self.assertIn(getattr(caught.exception,'code',None),('ACTOR_RECOVERY_UNPROVEN','WINDOW_RECOVERY_UNPROVEN'))
                with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(list(db.iterdump()),before)
            finally:world.release_actor_execution(permit)

    def test_corrupt_block_witness_cannot_renew_or_write_clock_floor(self):
        from app.coordinator_scheduler import activation_scope
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            host._request=lambda payload: (_ for _ in ()).throw(RuntimeError('block for renewal'))
            with self.assertRaisesRegex(RuntimeError,'block for renewal'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            permit=world.acquire_actor_execution(ref.person_id,'blocked-maintenance',coordinator=owner,activation=ref)
            try:
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute("UPDATE coordinator_activation_blocks SET proof_sha256=?",('0'*64,));db.commit()
                    before=list(db.iterdump())
                with world._coordinator_scope(owner), activation_scope(world,ref):
                    with self.assertRaises(WorldError) as caught:
                        world.renew_actor_execution(permit)
                    self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(list(db.iterdump()),before)
            finally:
                world.release_actor_execution(permit)

    def test_rejected_private_close_fault_blocks_without_losing_noeffect(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            def fail(*args, **kwargs):
                raise RuntimeError('private rejection close fault')
            host._reject_result=fail
            with self.assertRaisesRegex(RuntimeError,'private rejection close fault'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(world.snapshot()['status'],'technical_paused')
            with closing(sqlite3.connect(world.db_path)) as db:
                row=db.execute('SELECT phase,resume_phase,terminal_reason,private_closed,receipt_sha256,promoted_version FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                self.assertEqual(row,('technical_blocked','rejected','ALREADY_THERE',0,None,None))
                saved=db.execute('SELECT kind,outcome_sha256 FROM coordinator_activation_outcomes').fetchone()
                self.assertEqual(saved[0],'domain_rejected')
                proof=json.loads(db.execute('SELECT proof_json FROM coordinator_activation_blocks').fetchone()[0])
                self.assertEqual(proof['outcome_sha256'],saved[1])
                self.assertEqual(db.execute('SELECT receipt FROM intents').fetchone(),(None,))
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT state,receipt FROM actor_intents').fetchone(),('candidate',None))
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0,))
            self.assertEqual(world.next_activation(ref.window_id,coordinator=owner)['resume_phase'],'rejected')
            self.assertEqual(len(requests),1)

    def test_pending_fault_after_permit_before_prepare_has_no_fabricated_private_intent(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            def fail(person, *, execution=None, attempt_id=None):
                self.assertIsNotNone(execution)
                raise RuntimeError('pending preprepare fault')
            host._act=fail
            with self.assertRaisesRegex(RuntimeError,'pending preprepare fault'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(world.snapshot()['status'],'technical_paused')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,resume_phase,intent_id,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('technical_blocked','pending',None,0))
                proof=json.loads(db.execute('SELECT proof_json FROM coordinator_activation_blocks').fetchone()[0])
                self.assertIsNone(proof['base_session_version']);self.assertIsNone(proof['request_sha256'])
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone(),(0,))
            self.assertEqual(world.next_activation(ref.window_id,coordinator=owner)['phase'],'technical_blocked')
            self.assertEqual(requests,[])

    def test_world_committed_private_fault_retains_actual_receipt_and_head(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            def fail(*args, **kwargs):
                raise RuntimeError('private promotion fault')
            host._finish = fail
            with self.assertRaisesRegex(RuntimeError, 'private promotion fault'):
                host.act(ref.person_id, coordinator=owner, activation=ref, pause_on_failure=True)
            self.assertEqual(world.snapshot()['status'],'technical_paused')
            with closing(sqlite3.connect(world.db_path)) as db:
                row=db.execute('SELECT phase,resume_phase,receipt_sha256,intent_id FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                self.assertEqual(row[:2],('technical_blocked','world_committed'))
                self.assertIsNotNone(row[2])
                actual=db.execute('SELECT receipt FROM intents WHERE id=?',(row[3],)).fetchone()[0]
                self.assertIsNotNone(actual)
                self.assertEqual(db.execute('SELECT kind,outcome_json FROM coordinator_activation_outcomes').fetchone(),('committed',actual))
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT state,receipt FROM actor_intents').fetchone(),('candidate',None))
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0,))
            self.assertEqual(world.next_activation(ref.window_id,coordinator=owner)['resume_phase'],'world_committed')
            self.assertEqual(len(requests),1)

    def test_candidate_submit_fault_preserves_calls_candidate_and_blocks(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            def fail(*args, **kwargs):
                raise RuntimeError('candidate submit fault')
            host._submit = fail
            with self.assertRaisesRegex(RuntimeError, 'candidate submit fault'):
                host.act(ref.person_id, coordinator=owner, activation=ref, pause_on_failure=True)
            self.assertEqual(world.snapshot()['status'], 'technical_paused')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,resume_phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('technical_blocked','candidate',0))
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(0,))
            with closing(sqlite3.connect(host.db_path)) as db:
                row=db.execute('SELECT state,candidate,metrics,receipt FROM actor_intents').fetchone()
                self.assertEqual(row[0], 'candidate'); self.assertIsNotNone(row[1]); self.assertIsNotNone(row[2]); self.assertIsNone(row[3])
                self.assertGreater(db.execute('SELECT count(*) FROM actor_calls').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(0,))
            self.assertEqual(world.next_activation(ref.window_id,coordinator=owner)['phase'],'technical_blocked')
            self.assertEqual(len(requests),1)

    def test_blocked_host_reentry_stops_before_provider_or_private_adoption(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world, host, owner, ref, requests):
            def fail(payload):
                raise RuntimeError('original provider fault')
            host._request = fail
            with self.assertRaisesRegex(RuntimeError, 'original provider fault'):
                host.act(ref.person_id, coordinator=owner, activation=ref, pause_on_failure=True)
            with closing(sqlite3.connect(host.db_path)) as db:
                before = list(db.iterdump())
            calls = []
            def retry_request(payload):
                calls.append(payload)
                return activation_fixtures.ActorExecutionTests.result(payload)
            host._request = retry_request
            from app.world import WorldError
            with self.assertRaises(WorldError) as caught:
                host.act(ref.person_id, coordinator=owner, activation=ref, pause_on_failure=True)
            self.assertEqual(caught.exception.code, 'ACTIVATION_TECHNICAL_BLOCKED')
            self.assertEqual(calls, [], 'blocked reentry must not reissue the original prepared request')
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(list(db.iterdump()), before)
            row = world.next_activation(ref.window_id, coordinator=owner)
            self.assertEqual((row['activation_id'],row['phase']), (ref.activation_id,'technical_blocked'))
            with self.assertRaises(WorldError) as caught:
                world.complete_window(ref.window_id, coordinator=owner)
            self.assertEqual(caught.exception.code, 'WINDOW_INCOMPLETE')

    def test_prepared_provider_fault_blocks_with_same_pause_snapshot(self):
        registered = activation_fixtures.ActorActivationTests.registered
        with registered(self) as (world, host, owner, ref, requests):
            def fail_request(payload):
                raise RuntimeError('controlled prepared provider failure')
            host._request = fail_request
            with self.assertRaisesRegex(RuntimeError, 'controlled prepared provider failure'):
                host.act(ref.person_id, coordinator=owner, activation=ref,
                         pause_on_failure=True)
            snapshot = world.snapshot()
            self.assertEqual(snapshot['status'], 'technical_paused')
            self.assertEqual(snapshot['diagnostic']['code'], 'HOST_FAILURE')
            with closing(sqlite3.connect(world.db_path)) as db:
                phase, resume, code, closed, intent = db.execute(
                    'SELECT phase,resume_phase,diagnostic_code,private_closed,intent_id '
                    'FROM coordinator_activations WHERE activation_id=?',
                    (ref.activation_id,)).fetchone()
                self.assertEqual((phase, resume, code, closed),
                                 ('technical_blocked', 'prepared', 'HOST_FAILURE', 0),
                                 'A legal captured provider failure must durably block the original '
                                 'activation in the same transaction as its technical pause')
                row = db.execute(
                    'SELECT resume_phase,diagnostic_code,pause_revision,proof_json,proof_sha256 '
                    'FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',
                    (ref.world_id, ref.activation_id)).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row[:3], ('prepared', 'HOST_FAILURE', snapshot['revision']))
                proof = json.loads(row[3])
                self.assertEqual(proof['intent_id'], intent)
                self.assertEqual(proof['activation_id'], ref.activation_id)
                self.assertEqual(proof['resume_phase'], 'prepared')
                self.assertIsNone(proof['candidate_sha256'])
                self.assertIsNone(proof['receipt_sha256'])
                self.assertIsNone(proof['outcome_sha256'])
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 0)
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute(
                    'SELECT state,candidate,receipt FROM actor_intents WHERE intent_id=?',
                    (intent,)).fetchone(), ('prepared', None, None))
                self.assertEqual(db.execute(
                    'SELECT version,last_promoted_intent_id FROM actor_session_heads '
                    'WHERE person_id=?', (ref.person_id,)).fetchone(), (0, None))
            self.assertEqual(requests, [])
