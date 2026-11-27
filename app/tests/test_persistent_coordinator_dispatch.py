"""P023b4 first vertical production Coordinator dispatch, request seam only."""
from contextlib import closing
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.server import Coordinator
from app.world import WorldStore, DAY
from app.tests import test_actor_execution as request_fixtures

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class PersistentCoordinatorDispatchTests(unittest.TestCase):
    def test_deferred_missing_private_prepares_without_control_or_window(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-new-private-',dir=SCRATCH) as directory:
            root=Path(directory); world=WorldStore(root/'world.sqlite'); before=world.initialize()
            owner=Coordinator(world,None); owner.acquire_owner()
            host=ActorHost(world,root/'actors.sqlite'); owner.actors=host
            self.assertFalse(Path(host.db_path).exists())
            try:
                with owner._owner_scope(owner._owner_attempt):
                    registration=owner._prepare_scheduler(owner._owner_attempt)
                self.assertEqual(registration['world_id'],before['world_id'])
                self.assertEqual(world.snapshot(),before)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM coordinator_windows').fetchone(),(0,))
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT world_id FROM actor_world_binding').fetchall(),[(before['world_id'],)])
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone(),(0,))
            finally:owner.close()

    def test_main_unknown_running_refuses_before_factory_pause_or_private_write(self):
        from unittest.mock import patch
        from app import server
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p023b4-main-unknown-',dir=SCRATCH) as directory:
            root=Path(directory); world=WorldStore(root/'world.sqlite');world.initialize()
            world.control('resume'); world.advance(DAY); before=world.snapshot()
            private=root/'actors.sqlite'
            with closing(sqlite3.connect(private)) as db:
                db.execute('CREATE TABLE sentinel (value TEXT)');db.execute("INSERT INTO sentinel VALUES ('old')");db.commit()
            original=private.read_bytes()
            with patch.object(server,'create_default_actors') as factory, patch.object(server,'make_server') as listener:
                with self.assertRaises(WorldError) as caught:
                    server.main(['--data-dir',directory,'--port','0'])
                self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                factory.assert_not_called();listener.assert_not_called()
            self.assertEqual(world.snapshot(),before)
            self.assertEqual(private.read_bytes(),original)

    def test_caller_after_terminal_host_exception_only_stops_A_without_root_pause(self):
        # B supersedes the old A-only stop, preserving every real caller fault.
        import json
        with tempfile.TemporaryDirectory(prefix='p023b4-caller-stop-',dir=SCRATCH) as directory:
            root=Path(directory); world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite');requests=[];returned=[];terminal_rows=[]
            def request(payload):
                requests.append(payload['intent_id'])
                return request_fixtures.ActorExecutionTests.result(payload)
            host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
            original=host.act
            def caller(*args,**kwargs):
                result=original(*args,**kwargs);reference=kwargs['activation']
                returned.append(reference)
                with closing(sqlite3.connect(world.db_path)) as db:
                    terminal_rows.append(db.execute('SELECT activation_id,intent_id,phase,private_closed FROM coordinator_activations WHERE activation_id=?',(reference.activation_id,)).fetchone())
                raise RuntimeError('caller after actual Host terminal return; B readonly ACK')
            host.act=caller;owner=Coordinator(world,host)
            try:
                owner.start();owner.worker.join(20)
                self.assertFalse(owner.worker.is_alive());self.assertIsNone(owner.error)
                self.assertEqual((world.snapshot()['time'],world.snapshot()['status']),(DAY,'user_paused'))
                self.assertEqual(len(requests),12);self.assertEqual(len(set(requests)),12)
                self.assertEqual(len(returned),12);self.assertEqual(len({ref.activation_id for ref in returned}),12)
                self.assertEqual(len(terminal_rows),12)
                for reference,intent,row in zip(returned,requests,terminal_rows):
                    self.assertEqual(row,(reference.activation_id,intent,'completed',1))
                    self.assertEqual(requests.count(intent),1)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM coordinator_activations WHERE phase='completed' AND private_closed=1").fetchone(),(12,))
                    self.assertEqual(db.execute('SELECT window_id,state FROM coordinator_windows').fetchall(),[(returned[0].window_id,'completed')])
                    self.assertEqual(db.execute("SELECT count(*) FROM intents").fetchone(),(12,))
                    self.assertNotIn('technical_paused',[json.loads(row[0])['status'] for row in db.execute('SELECT state FROM snapshots')])
                    self.assertEqual(db.execute('SELECT activation_id,intent_id FROM coordinator_activations WHERE roster_index=0').fetchone(),terminal_rows[0][:2])
            finally:
                owner.close();self.assertFalse(owner.cleanup_pending);self.assertFalse(host.cleanup_pending)

    def test_independent_open_untrusted_codes_localstop_but_ordinary_fault_pauses(self):
        from app.world import WorldError
        codes=('ACTIVATION_BINDING_CONFLICT','INVALID_ACTIVATION','WINDOW_RECOVERY_UNPROVEN',
               'ACTOR_RECOVERY_UNPROVEN','ACTOR_WORLD_UNPROVEN','ACTOR_SESSION_STALE',
               'WINDOW_VERSION_CONFLICT','ORDINARY_STORAGE_FAULT')
        for code in codes:
            with self.subTest(code=code), tempfile.TemporaryDirectory(prefix='p023b4-origin-',dir=SCRATCH) as directory:
                root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
                host=ActorHost(world,root/'actors.sqlite'); owner=Coordinator(world,host)
                def fault(**kwargs):raise WorldError(code,'sanitized controlled open fault')
                world.open_or_recover_window=fault
                try:
                    owner.start();owner.worker.join(5)
                    self.assertFalse(owner.worker.is_alive());self.assertEqual(owner.error,code)
                    self.assertEqual(world.snapshot()['status'],'technical_paused' if code=='ORDINARY_STORAGE_FAULT' else 'running')
                    self.assertEqual(world.snapshot()['time'],0)
                finally:owner.close()

    def test_outer_catch_never_borrows_replaced_owner_attempt(self):
        from app.coordinator_execution import CoordinatorAttempt
        with tempfile.TemporaryDirectory(prefix='p023b4-captured-',dir=SCRATCH) as directory:
            root=Path(directory);clock=[100_000_000_000]
            world=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:clock[0]);world.initialize()
            host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host);old=[]
            def fault(**kwargs):
                old.append(owner._owner_attempt);old[0].finish()
                clock[0]+=61_000_000_000
                permit=world.acquire_coordinator_execution('controlled-successor')
                owner._owner_attempt=CoordinatorAttempt(world,permit,owner.execution_policy)
                owner._owner_attempt.start(owner._cancel_children)
                raise RuntimeError('original captured root is no longer valid')
            world.open_or_recover_window=fault
            try:
                owner.start();owner.worker.join(5)
                self.assertFalse(owner.worker.is_alive());self.assertEqual(owner.error,'HOST_FAILURE')
                self.assertEqual(world.snapshot()['status'],'running')
                self.assertEqual(world.snapshot()['time'],0)
            finally:
                owner.close()
                for attempt in old:attempt.finish()

    def test_running_open_step_finishes_original_roster_without_tomorrow_or_duplicate(self):
        # A genuine Host PROOF_FAILED remains a midroster stop under B.
        import json
        with tempfile.TemporaryDirectory(prefix='p023b4-open-recover-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite');requests=[];proof_calls=[]
            def request(payload):requests.append(payload['intent_id']);return request_fixtures.ActorExecutionTests.result(payload)
            host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
            original_finish=host._finish;original_proof=host._terminal_ack_result
            def finish_then_fault(*args,**kwargs):
                original_finish(*args,**kwargs)
                raise RuntimeError('controlled real Host post-finish failure')
            def proof_fault(*args,**kwargs):
                proof_calls.append(True)
                raise RuntimeError('controlled Host internal terminal proof failure')
            host._finish=finish_then_fault;host._terminal_ack_result=proof_fault;owner=Coordinator(world,host)
            try:
                owner.start();owner.worker.join(5)
                self.assertFalse(owner.worker.is_alive());self.assertEqual(owner.error,'HOST_FAILURE')
                self.assertEqual(len(proof_calls),1);self.assertEqual(len(requests),1)
                self.assertEqual((world.snapshot()['time'],world.snapshot()['status']),(DAY,'running'))
                self.assertFalse(owner._owner_attempt.stop.is_set());owner._owner_attempt.check()
                with closing(sqlite3.connect(world.db_path)) as db:
                    window=db.execute('SELECT window_id FROM coordinator_windows').fetchone()[0]
                    first=db.execute('SELECT activation_id,intent_id FROM coordinator_activations WHERE roster_index=0').fetchone()
                    self.assertEqual(db.execute('SELECT state FROM coordinator_windows').fetchone(),('open',))
                    self.assertEqual(db.execute("SELECT count(*) FROM coordinator_activations WHERE phase='completed' AND private_closed=1").fetchone(),(1,))
                    self.assertEqual(db.execute("SELECT count(*) FROM coordinator_activations WHERE phase='pending'").fetchone(),(11,))
                    self.assertEqual(first[1],requests[0])
                    self.assertNotIn('technical_paused',[json.loads(row[0])['status'] for row in db.execute('SELECT state FROM snapshots')])
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads WHERE person_id=?',('person-country-1-king',)).fetchone(),(1,first[1]))
                host._finish=original_finish;host._terminal_ack_result=original_proof
                before_recovery=len(requests)
                self.assertEqual(owner.start(),{'accepted':True});owner.worker.join(15)
                self.assertFalse(owner.worker.is_alive());self.assertIsNone(owner.error)
                self.assertEqual((world.snapshot()['time'],world.snapshot()['status']),(DAY,'user_paused'))
                self.assertEqual(len(requests),12);self.assertEqual(len(set(requests)),12)
                self.assertEqual(len(requests)-before_recovery,11);self.assertEqual(requests.count(first[1]),1)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT window_id,state FROM coordinator_windows').fetchall(),[(window,'completed')])
                    self.assertEqual(db.execute('SELECT activation_id,intent_id FROM coordinator_activations WHERE roster_index=0').fetchone(),first)
                    self.assertEqual(db.execute("SELECT count(*) FROM coordinator_activations WHERE phase='completed' AND private_closed=1").fetchone(),(12,))
                    self.assertNotIn('technical_paused',[json.loads(row[0])['status'] for row in db.execute('SELECT state FROM snapshots')])
            finally:
                host._finish=original_finish;host._terminal_ack_result=original_proof
                owner.close();self.assertFalse(owner.cleanup_pending);self.assertFalse(host.cleanup_pending)

    def test_start_rejects_pending_cleanup_and_live_old_worker_before_control(self):
        import threading
        from app.server import RequestError
        for mode in ('cleanup','live_worker'):
            with self.subTest(mode=mode),tempfile.TemporaryDirectory(prefix='p023b4-admission-',dir=SCRATCH) as directory:
                root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
                host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host);release=threading.Event()
                before=world.snapshot()
                if mode=='cleanup':owner.cleanup_pending=True
                else:
                    owner.worker=threading.Thread(target=lambda:release.wait(2));owner.worker.start()
                try:
                    with self.assertRaises(RequestError) as caught:owner.start()
                    self.assertEqual(caught.exception.code,'BUSY')
                    self.assertFalse(owner.busy);self.assertEqual(world.snapshot(),before)
                    self.assertIsNone(owner._owner_attempt)
                finally:release.set();owner.close()

    def test_failed_worker_start_restores_busy(self):
        import threading
        from unittest.mock import patch
        with tempfile.TemporaryDirectory(prefix='p023b4-thread-start-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host);owner.acquire_owner()
            original=threading.Thread.start
            def fail(thread):
                if thread.name=='theatre-coordinator':raise RuntimeError('finite injected start failure')
                return original(thread)
            try:
                with patch.object(threading.Thread,'start',fail):
                    with self.assertRaises(RuntimeError):owner.start()
                self.assertFalse(owner.busy);self.assertFalse(owner.worker and owner.worker.is_alive())
                self.assertEqual(owner.error,'HOST_FAILURE')
            finally:owner.close()

    def test_close_retains_root_until_actual_pending_provider_writer_exits(self):
        import threading,time
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p023b4-close-pending-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host)
            entered=threading.Event();release=threading.Event()
            def request(payload):
                entered.set();release.wait(3);return request_fixtures.ActorExecutionTests.result(payload)
            host._request=request;host._choose=lambda view,seen:({'action':'wait'},seen)
            try:
                owner.start();self.assertTrue(entered.wait(3));permit=owner._owner_attempt.permit
                started=time.monotonic();owner.close();self.assertLess(time.monotonic()-started,1.5)
                self.assertTrue(host._pending_threads and any(t.is_alive() for t in host._pending_threads))
                self.assertTrue(owner.cleanup_pending)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT owner_token FROM coordinator_execution_leases').fetchone(),(permit.owner_token,))
                with self.assertRaises(WorldError) as caught:owner.start()
                self.assertEqual(caught.exception.code,'COORDINATOR_EXECUTION_CANCELLED')
            finally:
                release.set()
                for thread in host._pending_threads:thread.join(3)
                owner.close()
            self.assertFalse(owner.cleanup_pending)
            self.assertFalse(owner.worker.is_alive());self.assertFalse(owner._owner_attempt.thread.is_alive())
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertIsNone(db.execute('SELECT owner_token FROM coordinator_execution_leases').fetchone()[0])
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(0,))

    def test_internal_step_without_captured_owner_never_legacy_after_root_registration(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-no-downgrade-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
            permit=world.acquire_coordinator_execution('former-owner');world.release_coordinator_execution(permit)
            host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host);calls=[]
            world.advance=lambda *args:calls.append(args)
            try:
                owner._step()
                self.assertEqual(calls,[])
                self.assertEqual(owner.error,'COORDINATOR_EXECUTION_REQUIRED')
                self.assertEqual(world.snapshot()['time'],0)
            finally:owner.close()

    def test_old_root_heartbeat_failure_does_not_cancel_successor_child(self):
        import threading
        from app.actor_execution import ExecutionPolicy,Attempt
        from app.coordinator_execution import CoordinatorAttempt
        with tempfile.TemporaryDirectory(prefix='p023b4-maintenance-capture-',dir=SCRATCH) as directory:
            root=Path(directory);clock=[100_000_000_000];world=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:clock[0]);state=world.initialize()
            host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host,execution_policy=ExecutionPolicy(renew_interval_seconds=.02))
            entered=threading.Event();release=threading.Event();original=world.renew_coordinator_execution;old=None;child=None
            def fail(*args,**kwargs):
                entered.set();release.wait(3);raise RuntimeError('controlled old heartbeat failure')
            world.renew_coordinator_execution=fail
            try:
                owner.acquire_owner();old=owner._owner_attempt;self.assertTrue(entered.wait(2))
                clock[0]+=61_000_000_000
                successor=world.acquire_coordinator_execution('new-root')
                new=CoordinatorAttempt(world,successor,owner.execution_policy);owner._owner_attempt=new
                with world._coordinator_scope(successor,new):
                    permit=world.acquire_actor_execution(state['persons'][0]['id'],'new-child',coordinator=successor)
                child=Attempt(permit,host.execution_policy,coordinator=successor,parent_attempt=new);host._attempt=child
                release.set();old.thread.join(3)
                self.assertFalse(old.thread.is_alive());self.assertFalse(child.stop.is_set())
                self.assertFalse(owner.stop.is_set())
            finally:
                release.set();world.renew_coordinator_execution=original
                if old:old.finish()
                owner.close()
                if child:world.release_actor_execution(child.permit)

    def test_prepare_seam_keeps_original_cancel_scope_through_registration(self):
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p023b4-prepare-cancel-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite')
            owner=Coordinator(world,host);owner.acquire_owner();original=world.inspect_scheduler_boundary
            def cancel(**kwargs):
                result=original(**kwargs);owner._owner_attempt.stop.set();return result
            world.inspect_scheduler_boundary=cancel
            try:
                with self.assertRaises(WorldError) as caught:owner._prepare_scheduler(owner._owner_attempt)
                self.assertEqual(caught.exception.code,'COORDINATOR_EXECUTION_CANCELLED')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_scheduler_registration'").fetchone())
            finally:owner.close()

    def test_registered_empty_world_missing_private_is_not_recreated_as_pristine(self):
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p023b4-registered-missing-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite')
            first=Coordinator(world,host);first.acquire_owner();first._prepare_scheduler(first._owner_attempt);first.close()
            Path(host.db_path).unlink()
            reopened=WorldStore(world.db_path);owner=Coordinator(reopened,None);owner.acquire_owner()
            deferred=ActorHost(reopened,root/'actors.sqlite');owner.actors=deferred;before=reopened.snapshot()
            try:
                with self.assertRaises(WorldError) as caught:owner.start()
                self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                self.assertFalse(Path(deferred.db_path).exists())
                self.assertEqual(reopened.snapshot(),before)
                self.assertFalse(owner.busy);self.assertIsNone(owner.worker)
            finally:owner.close()

    def test_persistent_scheduler_protocol_is_checked_synchronously(self):
        from app.world import WorldError
        with tempfile.TemporaryDirectory(prefix='p023b4-api-admission-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize();host=ActorHost(world,root/'actors.sqlite');owner=Coordinator(world,host)
            world.open_or_recover_window=None;before=world.snapshot()
            try:
                with self.assertRaises(WorldError) as caught:owner.start()
                self.assertEqual(caught.exception.code,'SCHEDULER_BACKEND_UNSUPPORTED')
                self.assertFalse(owner.busy);self.assertIsNone(owner.worker);self.assertEqual(world.snapshot(),before)
            finally:owner.close()

    def test_pristine_step_registers_and_completes_one_actual_window(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-parent-first-',dir=SCRATCH) as directory:
            root=Path(directory)
            world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite')
            requests=[]
            def request(payload):
                requests.append(payload)
                return request_fixtures.ActorExecutionTests.result(payload)
            host._request=request
            host._choose=lambda view,seen:({'action':'wait'},seen)
            owner=Coordinator(world,host)
            try:
                self.assertEqual(owner.start(),{'accepted':True})
                owner.worker.join(15)
                self.assertFalse(owner.worker.is_alive())
                self.assertIsNone(owner.error)
                self.assertFalse(owner.busy)
                self.assertEqual(world.snapshot()['time'],DAY)
                self.assertEqual(world.snapshot()['status'],'user_paused')
                with closing(sqlite3.connect(world.db_path)) as db:
                    exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_scheduler_registration'").fetchone()
                    self.assertIsNotNone(exists,'Production Coordinator must register the persistent scheduler before dispatch, not use the old public-advance/person loop')
                    self.assertEqual(db.execute('SELECT count(*) FROM coordinator_scheduler_registration').fetchone(),(1,))
                    windows=db.execute('SELECT window_id,target_time,state,roster_json FROM coordinator_windows').fetchall()
                    self.assertEqual(len(windows),1)
                    window,target,state,raw_roster=windows[0]
                    self.assertEqual((target,state),(DAY,'completed'))
                    roster=json.loads(raw_roster)
                    self.assertEqual(len(roster),12)
                    activations=db.execute('SELECT person_id,phase,private_closed,intent_id FROM coordinator_activations WHERE window_id=? ORDER BY roster_index',(window,)).fetchall()
                    self.assertEqual([x[0] for x in activations],roster)
                    self.assertEqual(len({x[3] for x in activations}),12)
                    self.assertTrue(all(x[1:3]==('completed',1) for x in activations))
                    self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone(),(12,))
                    self.assertEqual(db.execute("SELECT count(*) FROM coordinator_activation_outcomes WHERE kind='committed'").fetchone(),(12,))
                self.assertEqual(len(requests),12)
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_intents WHERE state=?',('committed',)).fetchone(),(12,))
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_session_heads WHERE version=1').fetchone(),(12,))
            finally:
                owner.close()
                self.assertFalse(owner.worker and owner.worker.is_alive())
                self.assertFalse(owner._owner_attempt and owner._owner_attempt.thread.is_alive())
                self.assertIsNone(host.worker_pid)
                self.assertFalse(host.cleanup_pending)
