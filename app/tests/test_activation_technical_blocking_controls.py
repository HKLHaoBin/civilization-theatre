"""Additional B acceptance/characterization; scripted request seam, not SDK.
These cover implemented behavior and are not retroactive test-first REDs.
"""
from contextlib import closing
import json
import sqlite3
import threading
import time
import unittest
from app.tests import test_actor_activations as fixtures
from app.coordinator_scheduler import activation_scope
from app.world import WorldError


def dump(path):
    with closing(sqlite3.connect(path)) as db:
        return list(db.iterdump())


def fail_provider(host, message='controlled original error'):
    def fail(payload):
        raise RuntimeError(message)
    host._request=fail


class ActivationBlockingControls(unittest.TestCase):
    registered=fixtures.ActorActivationTests.registered

    def test_characterization_witness_corruption_is_zero_write_at_recovery_gates(self):
        mutations=[
            "DELETE FROM coordinator_activation_blocks",
            "UPDATE coordinator_activation_blocks SET proof_sha256='bad'",
            "UPDATE coordinator_activation_blocks SET proof_json='{}'",
            "UPDATE coordinator_activation_blocks SET pause_revision=1",
            "UPDATE coordinator_activation_blocks SET pause_revision=999999",
            "UPDATE coordinator_activation_blocks SET diagnostic_code='OTHER_FAILURE'",
            "UPDATE coordinator_activations SET diagnostic_code='unsafe diagnostic' WHERE phase='technical_blocked'",
            "UPDATE coordinator_activations SET private_closed=1 WHERE phase='technical_blocked'",
            "UPDATE coordinator_activations SET resume_phase='completed' WHERE phase='technical_blocked'",
            "UPDATE coordinator_activations SET phase='unknown' WHERE phase='technical_blocked'",
            "UPDATE actor_activation_delegations SET activation_id='ffffffffffffffffffffffffffffffff'",
            "UPDATE actor_coordinator_delegations SET parent_epoch=999",
        ]
        for sql in mutations:
            with self.subTest(sql=sql),self.registered() as (world,host,owner,ref,requests):
                fail_provider(host)
                with self.assertRaisesRegex(RuntimeError,'controlled original error'):
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                with closing(sqlite3.connect(world.db_path)) as db:
                    if "phase='unknown'" in sql:db.execute('PRAGMA ignore_check_constraints=ON')
                    db.execute(sql);db.commit()
                before=(dump(world.db_path),dump(host.db_path))
                gates=[lambda:world.next_activation(ref.window_id,coordinator=owner),
                       lambda:world.open_or_recover_window(coordinator=owner,expected_version={k:world.snapshot()[k] for k in ('world_id','revision')}),
                       lambda:world.acquire_actor_execution(ref.person_id,'no-wash',coordinator=owner,activation=ref),
                       lambda:host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)]
                for gate in gates:
                    with self.assertRaises(Exception) as caught:gate()
                    self.assertEqual(getattr(caught.exception,'code',None),'WINDOW_RECOVERY_UNPROVEN')
                    self.assertEqual((dump(world.db_path),dump(host.db_path)),before)

    def test_characterization_repeat_keeps_original_witness_after_root_diagnostic_changes(self):
        with self.registered() as (world,host,owner,ref,requests):
            fail_provider(host)
            with self.assertRaises(RuntimeError):host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            with closing(sqlite3.connect(world.db_path)) as db:
                witness=db.execute('SELECT * FROM coordinator_activation_blocks').fetchone()
                original=db.execute('SELECT state FROM snapshots WHERE seq=?',(witness[4],)).fetchone()
            world.technical_pause('INDEPENDENT_ROOT_FAILURE',coordinator=owner)
            self.assertEqual(world.next_activation(ref.window_id,coordinator=owner)['diagnostic_code'],'HOST_FAILURE')
            permit=world.acquire_actor_execution(ref.person_id,'repeat-block',coordinator=owner,activation=ref)
            try:
                with world._coordinator_scope(owner),activation_scope(world,ref):
                    world.renew_actor_execution(permit)
                    world.technical_pause('HOST_FAILURE',execution=permit)
                    revision=world.snapshot()['revision']
                    world.technical_pause('HOST_FAILURE',execution=permit)
                    self.assertEqual(world.snapshot()['revision'],revision)
                    with self.assertRaises(WorldError):world.technical_pause('DIFFERENT',execution=permit)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT * FROM coordinator_activation_blocks').fetchone(),witness)
                    self.assertEqual(db.execute('SELECT state FROM snapshots WHERE seq=?',(witness[4],)).fetchone(),original)
            finally:world.release_actor_execution(permit)

    def test_characterization_snapshot_abort_rolls_back_block_and_pause_and_preserves_error(self):
        with self.registered() as (world,host,owner,ref,requests):
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute("CREATE TRIGGER block_pause_abort BEFORE INSERT ON snapshots WHEN json_extract(NEW.state,'$.status')='technical_paused' BEGIN SELECT RAISE(ABORT,'controlled snapshot abort'); END");db.commit()
            fail_provider(host)
            with self.assertRaisesRegex(RuntimeError,'controlled original error'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(world.snapshot()['status'],'running')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,resume_phase,diagnostic_code FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('prepared',None,None))
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_blocks'").fetchone())

    def test_characterization_final_gate_rejects_wrong_actual_snapshot_atomically(self):
        with self.registered() as (world,host,owner,ref,requests):
            encode=world._encode_state
            def corrupt(db,state):
                raw=encode(db,state)
                if state['status']=='technical_paused':
                    saved=json.loads(raw);saved['diagnostic']['time']+=1
                    return json.dumps(saved,ensure_ascii=False)
                return raw
            world._encode_state=corrupt
            fail_provider(host)
            with self.assertRaisesRegex(RuntimeError,'controlled original error'):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(world.snapshot()['status'],'running')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,resume_phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('prepared',None))
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_blocks'").fetchone())

    def test_characterization_original_attempt_cancel_or_deadline_localstop_keeps_provider_error(self):
        for reason in ('cancel','deadline'):
            with self.subTest(reason=reason),self.registered() as (world,host,owner,ref,requests):
                original=world.technical_pause;captured=[]
                def request(payload):
                    captured.append(world._scoped_attempt())
                    raise RuntimeError('original error before cleanup')
                def pause(code,*,execution=None,coordinator=None):
                    attempt=world._scoped_attempt()
                    self.assertIs(attempt,captured[0]);self.assertEqual(execution,attempt.permit)
                    if reason=='cancel':
                        from app.actor_execution import ActorRunError
                        attempt.failure=ActorRunError('ACTOR_EXECUTION_CANCELLED');attempt.stop.set()
                    else:attempt.deadline=time.monotonic()-1
                    return original(code,execution=execution,coordinator=coordinator)
                host._request=request;world.technical_pause=pause
                with self.assertRaisesRegex(RuntimeError,'original error before cleanup'):
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(world.snapshot()['status'],'running')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,resume_phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('prepared',None))
                    self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_blocks'").fetchone())

    def test_characterization_false_mode_has_no_extra_host_pause(self):
        with self.registered() as (world,host,owner,ref,requests):
            fail_provider(host)
            with self.assertRaises(RuntimeError):host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=False)
            self.assertEqual(world.snapshot()['status'],'running')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,resume_phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('prepared',None))

    def test_characterization_terminal_ack_facts_never_block_B_does_not_implement_C(self):
        for rejected in (False,True):
            with self.subTest(rejected=rejected),self.registered() as (world,host,owner,ref,requests):
                if rejected:
                    location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                    host._choose=lambda view,seen:({'action':'travel','to':location},seen)
                    original=host._reject_result
                    def lose(*args,**kwargs):
                        original(*args,**kwargs);raise RuntimeError('terminal ACK loss')
                    host._reject_result=lose
                else:
                    original=host._finish
                    def lose(*args,**kwargs):
                        original(*args,**kwargs);raise RuntimeError('terminal ACK loss')
                    host._finish=lose
                # C addendum: historical B-only fixture now returns original terminal.
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertTrue(result['reconciled'])
                self.assertEqual(result['intent_id'],requests[0]['intent_id'])
                self.assertEqual(world.snapshot()['status'],'running')
                if rejected:
                    self.assertIsNone(result['receipt'])
                    self.assertEqual(result['result_kind'],'domain_rejected')
                    self.assertEqual(result['outcome']['code'],'ALREADY_THERE')
                else:
                    self.assertEqual(result['receipt']['status'],'committed')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,private_closed,resume_phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('rejected' if rejected else 'completed',1,None))
                    self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_blocks'").fetchone())
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0 if rejected else 1,))
                self.assertEqual(len(requests),1)

    def test_characterization_blocked_heartbeat_source_contention_does_not_wait_under_main(self):
        with self.registered() as (world,host,owner,ref,requests):
            fail_provider(host)
            with self.assertRaises(RuntimeError):host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            permit=world.acquire_actor_execution(ref.person_id,'blocked-maintenance',coordinator=owner,activation=ref)
            held=threading.Event();release=threading.Event()
            def holder():
                with world._private_sources_lock:
                    held.set();release.wait(4)
            thread=threading.Thread(target=holder)
            thread.start()
            try:
                self.assertTrue(held.wait(2));before=dump(world.db_path)
                started=time.monotonic()
                with world._coordinator_scope(owner),activation_scope(world,ref):
                    with self.assertRaises(WorldError) as caught:world.renew_actor_execution(permit)
                    self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY')
                self.assertLess(time.monotonic()-started,.5)
                self.assertEqual(dump(world.db_path),before)
            finally:
                release.set();thread.join(3);world.release_actor_execution(permit)
            self.assertFalse(thread.is_alive())

    def test_characterization_blocked_candidate_still_requires_full_calls_and_head(self):
        for table in ('actor_calls','actor_session_heads'):
            with self.subTest(table=table),self.registered() as (world,host,owner,ref,requests):
                host._submit=lambda *args,**kwargs: (_ for _ in ()).throw(RuntimeError('candidate fault'))
                with self.assertRaises(RuntimeError):host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                with closing(sqlite3.connect(host.db_path)) as db:db.execute('DELETE FROM '+table);db.commit()
                before=(dump(world.db_path),dump(host.db_path))
                with self.assertRaises(Exception) as caught:world.next_activation(ref.window_id,coordinator=owner)
                self.assertIn(getattr(caught.exception,'code',None),('ACTOR_RECOVERY_UNPROVEN','WINDOW_RECOVERY_UNPROVEN'))
                self.assertEqual((dump(world.db_path),dump(host.db_path)),before)
