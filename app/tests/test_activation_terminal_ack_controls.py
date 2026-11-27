"""C acceptance of existing gates; request seam metrics are synthetic, not SDK.
No retrospective RED claim: terminal proof failures and old protocols are controls.
"""
from contextlib import closing
from pathlib import Path
import json
import sqlite3
import threading
import time
import unittest
from app.tests import test_actor_activations as fixtures
from app.actor_execution import ActorRunError
from app.actors import ActorHost
from app.coordinator_scheduler import ActivationRef, scoped
from app.world import WorldError


def rows(path):
    """Durable domain/private facts, excluding legitimate lease/CAS cleanup."""
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
        names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {n:db.execute('SELECT * FROM '+n+' ORDER BY rowid').fetchall() for n in names
                if n not in ('actor_execution_leases','coordinator_execution_leases','execution_clock')}


def terminal_hook(test,world,host,rejected,callback):
    if rejected:
        location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==test.ref.person_id)
        host._choose=lambda view,seen:({'action':'travel','to':location},seen)
        original=host._reject_result
        def hook(*args,**kwargs):
            result=original(*args,**kwargs)
            return callback(result)
        host._reject_result=hook
    else:
        original=host._finish
        def hook(*args,**kwargs):
            original(*args,**kwargs)
            return callback(None)
        host._finish=hook


class ActivationTerminalAckControls(unittest.TestCase):
    registered=fixtures.ActorActivationTests.registered

    def test_unregistered_errors_keep_legacy_pause_protocol_without_terminal_inspection(self):
        from app.tests import test_actor_failure_scope as legacy
        for kind in (RuntimeError,KeyboardInterrupt):
            with self.subTest(kind=kind.__name__),legacy.ActorFailureScopeTests.pair(self) as (world,host):
                error=kind('legacy unregistered failure')
                def request(payload):raise error
                host._request=request
                host._terminal_ack_result=lambda *a:self.fail('no activation must not use C proof')
                with self.assertRaises(kind) as caught:
                    host.act('person-country-1-finance',pause_on_failure=True)
                self.assertIs(caught.exception,error)
                self.assertEqual(world.snapshot()['status'],'technical_paused')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activations'").fetchone())

    def test_characterization_false_keeps_original_terminal_error_both_phases(self):
        for rejected in (False,True):
            with self.subTest(rejected=rejected),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;error=RuntimeError('False original');saved=[]
                def loss(result):
                    saved.append((world.snapshot(),rows(world.db_path),rows(host.db_path)));raise error
                terminal_hook(self,world,host,rejected,loss)
                host._terminal_ack_result=lambda *a: self.fail('False must not inspect terminal')
                with self.assertRaises(RuntimeError) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=False)
                self.assertIs(caught.exception,error)
                self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),saved[0])
                self.assertEqual(len(requests),1)

    def test_characterization_localstop_codes_do_not_try_terminal_or_pause(self):
        codes=('ACTOR_EXECUTION_EXPIRED','EXECUTION_CLOCK_ROLLBACK','INVALID_EXECUTION',
               'COORDINATOR_EXECUTION_FENCED','INVALID_COORDINATOR_EXECUTION',
               'ACTIVATION_BINDING_CONFLICT','INVALID_ACTIVATION','WINDOW_RECOVERY_UNPROVEN',
               'WINDOW_VERSION_CONFLICT','ACTOR_RECOVERY_UNPROVEN','ACTOR_WORLD_MISMATCH',
               'ACTOR_SESSION_STALE','ACTOR_EXECUTION_CANCELLED','ACTOR_EXECUTION_DEADLINE')
        for code in codes:
            with self.subTest(code=code),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;error=WorldError(code,'original localstop');saved=[]
                # Preserve the existing three-try observation conflict protocol;
                # fault every actual returned result, including terminal replays.
                act=host._act
                def loss(*args,**kwargs):
                    act(*args,**kwargs)
                    saved.append((world.snapshot(),rows(world.db_path),rows(host.db_path)));raise error
                host._act=loss
                host._terminal_ack_result=lambda *a:self.fail('original localstop may not reconcile')
                world.technical_pause=lambda *a,**kw:self.fail('original localstop may not pause')
                with self.assertRaises(WorldError) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertIs(caught.exception,error)
                self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),saved[0])

    def test_characterization_bad_terminal_material_preserves_error_no_writes_no_pause(self):
        corruptions=[('private','DELETE FROM actor_calls'),
                     ('private','DELETE FROM actor_session_heads'),
                     ('private',"UPDATE actor_intents SET body='{}'"),
                     ('private',"UPDATE actor_intents SET request='{}'"),
                     ('main',"UPDATE coordinator_activation_outcomes SET outcome_sha256='bad'"),
                     ('main',"UPDATE intents SET payload='{}'"),
                     ('main','DELETE FROM coordinator_scheduler_registration')]
        for rejected in (False,True):
            for store,sql in corruptions:
                with self.subTest(rejected=rejected,sql=sql),self.registered() as (world,host,owner,ref,requests):
                    self.ref=ref;error=RuntimeError('original terminal ACK');saved=[];proof=[]
                    reconcile=host._terminal_ack_result
                    def inspect(*args):
                        proof.append(world._scoped_attempt());return reconcile(*args)
                    host._terminal_ack_result=inspect
                    def loss(result):
                        with closing(sqlite3.connect(host.db_path if store=='private' else world.db_path)) as db:
                            db.execute(sql);db.commit()
                        saved.append((world.snapshot(),rows(world.db_path),rows(host.db_path)));raise error
                    terminal_hook(self,world,host,rejected,loss)
                    world.technical_pause=lambda *a,**kw:self.fail('bad proof may not fallback pause')
                    with self.assertRaises(RuntimeError) as caught:
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                    self.assertIs(caught.exception,error)
                    self.assertEqual(len(proof),1)
                    self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),saved[0])
                    self.assertEqual(len(requests),1)
                    self.assertIsNone(world._scoped_attempt());self.assertIsNone(scoped(world))

    def test_characterization_authority_budget_and_source_loss_after_terminal_are_localstop(self):
        for reason in ('cancel','deadline','clock_rollback','clock_invalid','actor_expired','root_expired',
                       'actor_fenced','root_fenced','source_file','source_conflict'):
            with self.subTest(reason=reason),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;error=RuntimeError('captured original ACK');saved=[];extras=[]
                clock=world._execution_clock_ns;path=host.db_path
                def loss(result):
                    snapshot=world.snapshot()
                    attempt=world._scoped_attempt()
                    if reason=='cancel':attempt.failure=ActorRunError('ACTOR_EXECUTION_CANCELLED');attempt.stop.set()
                    elif reason=='deadline':attempt.deadline=time.monotonic()-1
                    elif reason=='clock_rollback':world._execution_clock_ns=lambda:1
                    elif reason=='clock_invalid':world._execution_clock_ns=lambda:True
                    elif reason=='source_file':host.db_path=str(Path(path).with_name('wrong.sqlite'))
                    elif reason=='source_conflict':extras.append(ActorHost(world,Path(path).with_name('conflict.sqlite')))
                    else:
                        with closing(sqlite3.connect(world.db_path)) as db:
                            if reason=='actor_expired':db.execute('UPDATE actor_execution_leases SET expires_wall_ns=1')
                            elif reason=='root_expired':db.execute('UPDATE coordinator_execution_leases SET expires_wall_ns=1')
                            elif reason=='actor_fenced':db.execute('UPDATE actor_execution_leases SET owner_token=?',('successor-token',))
                            else:db.execute('UPDATE coordinator_execution_leases SET owner_token=?',('successor-token',))
                            db.commit()
                    saved.append((snapshot,rows(world.db_path),rows(path)));raise error
                terminal_hook(self,world,host,False,loss)
                world.technical_pause=lambda *a,**kw:self.fail('failed terminal proof may not pause')
                try:
                    with self.assertRaises(RuntimeError) as caught:
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                    self.assertIs(caught.exception,error)
                    self.assertEqual((world.snapshot(),rows(world.db_path),rows(path)),saved[0])
                    self.assertIsNone(world._scoped_attempt());self.assertIsNone(scoped(world))
                    self.assertEqual(len(requests),1)
                finally:
                    world._execution_clock_ns=clock;host.db_path=path
                    for extra in extras:extra.close()

    def test_characterization_source_wait_uses_same_remaining_attempt_and_close_cancel(self):
        for cancel in (False,True):
            with self.subTest(cancel=cancel),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;error=RuntimeError('ACK before bounded source wait');held=threading.Event();release=threading.Event()
                threads=[];saved=[];captured=[]
                def loss(result):
                    attempt=world._scoped_attempt();captured.append(attempt)
                    if not cancel:attempt.deadline=min(attempt.deadline,time.monotonic()+.18)
                    def holder():
                        with world._private_sources_lock:held.set();release.wait(3)
                    thread=threading.Thread(target=holder);threads.append(thread);thread.start()
                    self.assertTrue(held.wait(1));saved.append(world.snapshot())
                    if cancel:
                        def closer():
                            time.sleep(.05);host.close()
                        thread=threading.Thread(target=closer);threads.append(thread);thread.start()
                    raise error
                terminal_hook(self,world,host,False,loss)
                world.technical_pause=lambda *a,**kw:self.fail('exhausted proof may not pause')
                started=time.monotonic()
                try:
                    with self.assertRaises(RuntimeError) as caught:
                        host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                    self.assertIs(caught.exception,error)
                    self.assertLess(time.monotonic()-started,1.5)
                    self.assertFalse(release.is_set())
                    self.assertEqual(world.snapshot(),saved[0])
                    self.assertIsNone(world._scoped_attempt());self.assertIsNone(scoped(world))
                finally:
                    release.set()
                    for thread in threads:thread.join(2);self.assertFalse(thread.is_alive())

    def test_characterization_terminal_check_is_same_scope_read_only_and_metrics_exact(self):
        from contextlib import contextmanager
        for rejected in (False,True):
            with self.subTest(rejected=rejected),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;error=RuntimeError('ACK');captured=[];checks=[];traces=[];saved=[];opened=[]
                transaction=host._storage.transaction
                check=world._check_actor_execution
                def guard(db,identity,person,permit,**kwargs):
                    if opened:
                        checks.append((world._scoped_attempt(),permit,world._scoped_coordinator(),scoped(world)))
                    return check(db,identity,person,permit,**kwargs)
                world._check_actor_execution=guard
                @contextmanager
                def inspect(*args,**kwargs):
                    opened.append(True)
                    self.assertIs(kwargs['attempt'],captured[0])
                    with transaction(*args,**kwargs) as db:
                        db.set_trace_callback(traces.append)
                        yield db
                def loss(result):
                    captured.append(world._scoped_attempt())
                    saved.append((world.snapshot(),rows(world.db_path),rows(host.db_path)))
                    host._storage.transaction=inspect
                    host._storage.adopt=lambda *a:self.fail('terminal must not adopt')
                    raise error
                terminal_hook(self,world,host,rejected,loss)
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertTrue(result['reconciled']);self.assertEqual(len(opened),1)
                self.assertGreaterEqual(len(checks),2)
                for attempt,permit,parent,activation in checks:
                    self.assertIs(attempt,captured[0]);self.assertEqual(permit,captured[0].permit)
                    self.assertEqual(parent,owner);self.assertEqual(activation,ref)
                self.assertFalse(any(sql.lstrip().upper().startswith(('INSERT','UPDATE','DELETE','CREATE','REPLACE')) for sql in traces))
                self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),saved[0])
                with closing(sqlite3.connect(host.db_path)) as db:
                    metrics=json.loads(db.execute('SELECT metrics FROM actor_intents').fetchone()[0])
                self.assertEqual({k:result[k] for k in metrics},metrics)
                self.assertEqual(len(requests),1)
                print('C_SCOPE_READ_ONLY',json.dumps(dict(rejected=rejected,transactions=len(opened),authority_checks=len(checks),sql_statements=len(traces))))

    def test_characterization_final_authority_failure_cannot_return_prebuilt_success(self):
        for rejected in (False,True):
            with self.subTest(rejected=rejected),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;error=RuntimeError('original before final proof failure');saved=[];checks=[]
                guard=world._check_actor_execution
                def loss(result):
                    saved.append((world.snapshot(),rows(world.db_path),rows(host.db_path)))
                    def fail_final(*args,**kwargs):
                        guard(*args,**kwargs);checks.append(world._scoped_attempt())
                        if len(checks)==2:raise WorldError('ACTOR_EXECUTION_FENCED','controlled final gate')
                    world._check_actor_execution=fail_final
                    raise error
                terminal_hook(self,world,host,rejected,loss)
                world.technical_pause=lambda *a,**kw:self.fail('failed final gate may not pause')
                with self.assertRaises(RuntimeError) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertIs(caught.exception,error);self.assertEqual(len(checks),2)
                self.assertIs(checks[0],checks[1])
                self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),saved[0])

    def test_characterization_native_sql_wait_cannot_restart_expired_attempt(self):
        with self.registered() as (world,host,owner,ref,requests):
            self.ref=ref;error=RuntimeError('original ACK before main SQL busy');held=threading.Event();release=threading.Event();threads=[];saved=[]
            def loss(result):
                saved.append(world.snapshot());world._scoped_attempt().deadline=min(world._scoped_attempt().deadline,time.monotonic()+.15)
                def holder():
                    with closing(sqlite3.connect(world.db_path)) as db:
                        db.execute('BEGIN IMMEDIATE');held.set();release.wait(3);db.rollback()
                thread=threading.Thread(target=holder);threads.append(thread);thread.start()
                self.assertTrue(held.wait(1));raise error
            terminal_hook(self,world,host,False,loss)
            world.technical_pause=lambda *a,**kw:self.fail('expired SQL proof may not pause')
            started=time.monotonic()
            try:
                with self.assertRaises(RuntimeError) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertIs(caught.exception,error)
                self.assertLess(time.monotonic()-started,1.5)
                self.assertFalse(release.is_set());self.assertEqual(world.snapshot(),saved[0])
            finally:
                release.set()
                for thread in threads:thread.join(2);self.assertFalse(thread.is_alive())

    def test_characterization_user_pause_and_card_after_terminal_are_not_overwritten(self):
        for rejected in (False,True):
            with self.subTest(rejected=rejected),self.registered() as (world,host,owner,ref,requests):
                self.ref=ref;saved=[]
                def loss(result):
                    world.edit_card(ref.person_id,{'mission':'legitimate forward card'},coordinator=owner)
                    world.control('pause',coordinator=owner)
                    saved.append((world.snapshot(),rows(world.db_path),rows(host.db_path)))
                    raise RuntimeError('ACK following lawful user control')
                terminal_hook(self,world,host,rejected,loss)
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertTrue(result['reconciled'])
                self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),saved[0])
                self.assertEqual(world.snapshot()['status'],'user_paused')

    def test_characterization_old_terminal_error_never_rewinds_later_head(self):
        for rejected in (False,True):
            with self.subTest(rejected=rejected),self.registered() as (world,host,owner,ref,requests):
                if rejected:
                    location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                    host._choose=lambda view,seen:({'action':'travel','to':location},seen)
                first=host.act(ref.person_id,coordinator=owner,activation=ref)
                host._choose=lambda view,seen:({'action':'wait'},seen)
                while True:
                    row=world.next_activation(ref.window_id,coordinator=owner)
                    if row is None:break
                    current=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                    host.act(current.person_id,coordinator=owner,activation=current)
                world.complete_window(ref.window_id,coordinator=owner)
                version={k:world.snapshot()[k] for k in ('world_id','revision')}
                window=world.open_or_recover_window(coordinator=owner,expected_version=version)
                row=world.next_activation(window['window_id'],coordinator=owner)
                current=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                host.act(current.person_id,coordinator=owner,activation=current)
                original_act=host._act;before=[];count=len(requests)
                def loss(*args,**kwargs):
                    result=original_act(*args,**kwargs)
                    self.assertEqual(result['intent_id'],first['intent_id'])
                    before.append((world.snapshot(),rows(world.db_path),rows(host.db_path)))
                    raise RuntimeError('old terminal result ACK')
                host._act=loss
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(result,dict(first,reconciled=True))
                self.assertEqual(len(requests),count)
                self.assertEqual((world.snapshot(),rows(world.db_path),rows(host.db_path)),before[0])
