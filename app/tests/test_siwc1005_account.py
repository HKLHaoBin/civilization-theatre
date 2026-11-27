"""1005 prospective definitions, NOT EXECUTED. Owned synthetic values only.
Thread gates are ordering evidence, timeout is only a harness hang detector.
Windows process tests use the real byte-range lock and a file-backed fake vault;
ordinary terminate/kill is NOT a power-loss or machine-reboot test.
"""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from app.real_provider import ProviderFault, RequestBudget
from app.siwc_oauth import SIWCSession
from app.siwc_storage import SessionStore
from app.tests.siwc979_synthetic import Harness, CALLBACK, SyntheticVerifier, synthetic_file_lock

WAIT=5

class Gate:
    def __init__(self):self.entered=threading.Event();self.release=threading.Event()
    def hold(self):
        self.entered.set()
        if not self.release.wait(WAIT*4):raise AssertionError('test owner did not release gate')

class Workers:
    def __init__(self,case):self.case=case;self.threads=[]
    def start(self,fn,name='1005-worker'):
        done=threading.Event();result=[]
        def run():
            try:result.append((True,fn()))
            except BaseException as e:result.append((False,e))
            finally:done.set()
        t=threading.Thread(target=run,name=name,daemon=True);self.threads.append(t);t.start()
        return done,result
    def join(self):
        for t in self.threads:t.join(WAIT)
        for t in self.threads:self.case.assertFalse(t.is_alive(),t.name)

class NonFIFO:
    def __init__(self,real):self.real=real;self.waiting=threading.Event();self.allow=threading.Event();self.failed=0
    def acquire(self,timeout):
        stale=threading.current_thread().name=='1005-stale'
        if stale and self.failed and not self.allow.wait(timeout):return False
        ok=self.real.acquire(timeout=timeout)
        if stale and not ok:self.failed+=1;self.waiting.set()
        return ok
    def release(self):self.real.release()

class AccountTests(unittest.TestCase):
    def wait(self,event):self.assertTrue(event.wait(WAIT),'missing ordering event')
    def fresh(self,h):
        return SIWCSession(h.session.store.path,h.vault,http=h.http,verifier=SyntheticVerifier(),
            clock=lambda:h.now,opener=h.urls.append,file_lock=synthetic_file_lock)
    def edge_case(self,edge,after_f=False,action='cancel'):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();old=copy.deepcopy(h.session.store.load());refs=set(h.vault.data)
            p=h.begin(key);h.tokens(p,access_token='SYNTHETIC_NEW_ACCESS')
            gate=Gate();workers=Workers(self);armed=[True]
            def hook(label):
                if label==edge and armed[0]:armed[0]=False;gate.hold()
            with patch.object(h.session.store,'_edge',side_effect=hook):
                try:
                    done,out=workers.start(lambda:h.callback(p))
                    self.wait(gate.entered)
                    cd,co=workers.start(getattr(h.session,action),'1005-control');self.wait(cd)
                    self.assertTrue(co[0][0],co)
                    ack=co[0][1];self.assertTrue(ack['request_ack'])
                    self.assertFalse(ack['cancellation_complete']);self.assertFalse(ack['durable_ack'])
                    self.assertEqual(ack['outcome'],'commit_won_pending' if after_f else 'cancel_won_pending')
                    self.assertFalse(done.is_set());self.assertFalse(gate.release.is_set())
                    self.assertTrue(refs.issubset(h.vault.data) or after_f)
                    gate.release.set();self.wait(done)
                    if after_f:
                        self.assertTrue(out[0][0],out)
                        self.assertTrue(out[0][1]['durable_ack'])
                    else:
                        self.assertFalse(out[0][0],out)
                        self.assertIsInstance(out[0][1],ProviderFault)
                        self.assertIn('CANCEL',out[0][1].code)
                        final=h.session.transaction_status()
                        self.assertEqual(final['outcome'],'cancelled_complete');self.assertTrue(final['durable_ack'])
                    fresh=self.fresh(h);self.assertEqual(fresh.binding(),key)
                    with fresh.access(key,RequestBudget(None,2)) as token:
                        self.assertEqual(token,'SYNTHETIC_NEW_ACCESS' if after_f else 'SYNTHETIC_ACCESS_NEVER_REAL')
                    if not after_f:self.assertEqual(h.vault.data.keys(),refs)
                    self.assertEqual(fresh.store.load()['accounts'][key]['subject'],old['accounts'][key]['subject'])
                finally:gate.release.set();workers.join()

    def test_cancel_during_authoritative_load(self):self.edge_case('loaded')
    def test_cancel_during_prepared_temp_fsync(self):self.edge_case('prepared_fsync')
    def test_cancel_after_prepared_replace_before_return(self):self.edge_case('prepared_replaced')
    def test_cancel_during_vault_factory(self):self.edge_case('vault_factory')
    def test_cancel_after_vault_write(self):self.edge_case('vault_written')
    def test_cancel_after_vault_readback(self):self.edge_case('vault_readback')
    def test_cancel_after_final_bytes_fsync_before_F(self):self.edge_case('committed_fsync')
    def test_commit_won_before_native_replace(self):self.edge_case('finalizing',True)
    def test_commit_won_replace_effective_not_returned(self):self.edge_case('committed_replaced',True)
    def test_commit_won_durability_flush(self):self.edge_case('committed_flush',True)
    def test_commit_won_durability_readback(self):self.edge_case('committed_readback',True)

    def test_actual_native_replace_applied_before_wrapper_returns(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p,access_token='SYNTHETIC_NATIVE_NEW')
            gate=Gate();workers=Workers(self);original=os.replace;armed=[True]
            def replace(source,target):
                value=json.loads(Path(source).read_text());result=original(source,target)
                if value.get('state')=='COMMITTED' and armed[0]:armed[0]=False;gate.hold()
                return result
            with patch('app.siwc_storage.os.replace',side_effect=replace):
                try:
                    done,out=workers.start(lambda:h.callback(p));self.wait(gate.entered)
                    cd,co=workers.start(h.session.cancel);self.wait(cd);self.assertTrue(co[0][0],co)
                    self.assertEqual(co[0][1]['outcome'],'commit_won_pending');self.assertFalse(co[0][1]['durable_ack'])
                    cold=self.fresh(h);cold.store.thread_lock=h.session.store.thread_lock
                    with self.assertRaises(ProviderFault):cold.store.load(RequestBudget(None,.15))
                    self.assertFalse(done.is_set());gate.release.set();self.wait(done);self.assertTrue(out[0][0],out)
                    self.assertTrue(out[0][1]['durable_ack'])
                    with self.fresh(h).access(key,RequestBudget(None,2)) as token:self.assertEqual(token,'SYNTHETIC_NATIVE_NEW')
                finally:gate.release.set();workers.join()

    def test_server_callback_pending_and_serverclose_ack_are_consumed(self):
        from app.tests.test_siwc979_consumers import RouteTests
        from app.server import TheatreServer
        from types import SimpleNamespace
        route=RouteTests()
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);handler=route.setup_handler(h.session)
            pending=dict(state='account_mutation_pending',durable_ack=False,account_mutation_pending=True,outcome='commit_unknown')
            with patch.object(h.session,'callback',return_value=pending):
                code,body,mime=route.request(handler,'GET','/auth/callback?state=SYNTHETIC',origin=False)
                self.assertEqual(code,202);self.assertIn('待确认',body.decode());self.assertNotIn('授权已保存',body.decode())
            server=object.__new__(TheatreServer)
            server.coordinator=SimpleNamespace(close=lambda:None,actors=SimpleNamespace(real_settings=SimpleNamespace(siwc=h.session)))
            with patch.object(h.session,'close',return_value=pending),patch('http.server.ThreadingHTTPServer.server_close'):
                self.assertEqual(server.server_close(),pending);self.assertTrue(server.account_cleanup_pending)

    def test_logout_waits_refresh_save_then_completes_once_without_retarget(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();h.now+=3601;h.http.tokens.pop('id_token')
            h.http.tokens.update(access_token='SYNTHETIC_ROTATED',refresh_token='SYNTHETIC_ROTATED_REFRESH')
            gate=Gate();workers=Workers(self);waiting=threading.Event();locked=h.session.store.locked;armed=[True]
            def edge(label):
                if label=='vault_written' and armed[0]:armed[0]=False;gate.hold()
            @contextmanager
            def observe(budget):
                if threading.current_thread().name=='1005-logout':waiting.set()
                with locked(budget):yield
            with patch.object(h.session.store,'_edge',side_effect=edge),patch.object(h.session.store,'locked',observe):
                try:
                    rd,ro=workers.start(lambda:h.session.models(key,RequestBudget(None,10)));self.wait(gate.entered)
                    ld,lo=workers.start(h.session.logout,'1005-logout');self.wait(waiting);self.assertFalse(ld.is_set())
                    gate.release.set();self.wait(rd);self.wait(ld)
                    self.assertFalse(ro[0][0]);self.assertIsInstance(ro[0][1],ProviderFault)
                    self.assertIn('AUTH_CANCELLED',ro[0][1].code)
                    self.assertTrue(lo[0][0],lo);self.assertTrue(lo[0][1]['local_logout_complete'])
                    m=h.session.store.load();self.assertFalse(m['accounts'][key]['slots']);self.assertFalse(m['accounts'][key]['rotation_pending'])
                    self.assertEqual(m['accounts'][key]['client_id'],'oaiapp_SYNTHETIC')
                    self.assertEqual(m['accounts'][key]['subject'],'SYNTHETIC_SUBJECT')
                finally:gate.release.set();workers.join()
    def test_commit_won_last_native_unlock(self):self.edge_case('unlock',True)
    def test_commit_won_gc_does_not_authorize(self):self.edge_case('gc',True)
    def test_close_pending_and_success_survives_fresh_instance(self):self.edge_case('committed_replaced',True,'close')

    def test_normal_first_login_reauth_select_and_cold_models(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);a=h.authorize();p=h.begin(a);h.tokens(p,access_token='SYNTHETIC_REAUTH');h.callback(p)
            b=h.authorize('oaiapp_SECOND','SYNTHETIC_SECOND');fresh=self.fresh(h)
            self.assertEqual(fresh.binding(),b);fresh.select(a);fresh.welcome()
            self.assertFalse(fresh.status()['welcome_required'])
            self.assertTrue(fresh.models(a,RequestBudget(None,2)))
            with self.fresh(h).access(a,RequestBudget(None,2)) as token:self.assertEqual(token,'SYNTHETIC_REAUTH')

    def test_cold_prepared_chooses_old_and_committed_chooses_new(self):
        for edge,new in (('vault_written',False),('committed_replaced',True)):
            with self.subTest(edge=edge),tempfile.TemporaryDirectory() as d:
                h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p,access_token='SYNTHETIC_COLD_NEW')
                captured=[]
                def snap(label):
                    if label==edge:captured.append((h.session.store.path.read_bytes(),copy.deepcopy(h.vault.data)))
                with patch.object(h.session.store,'_edge',side_effect=snap):h.callback(p)
                self.assertEqual(len(captured),1)
                h.session.store.path.write_bytes(captured[0][0]);h.vault.data=captured[0][1]
                fresh=self.fresh(h)
                with fresh.access(key,RequestBudget(None,2)) as token:
                    self.assertEqual(token,'SYNTHETIC_COLD_NEW' if new else 'SYNTHETIC_ACCESS_NEVER_REAL')
                self.assertEqual(json.loads(fresh.store.path.read_text())['state'],'COMMITTED')

    def test_corrupt_unknown_envelope_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);h.authorize();raw=json.loads(h.session.store.path.read_text());raw['state']='UNKNOWN'
            h.session.store.path.write_text(json.dumps(raw))
            with self.assertRaises(ProviderFault):self.fresh(h).binding()
            self.assertEqual(self.fresh(h).status()['state'],'account_mutation_pending')

    def test_schema1_migration_retains_original_refs_and_rejects_legacy_reader(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();legacy=dict(h.session.store.load());refs=set(h.vault.data)
            h.session.store.path.write_text(json.dumps(legacy));fresh=self.fresh(h)
            self.assertEqual(fresh.binding(),key);self.assertEqual(set(h.vault.data),refs)
            envelope=json.loads(h.session.store.path.read_text());self.assertEqual(envelope['schema'],1005)
            self.assertNotEqual(set(envelope),{'schema','host_id','active','accounts'})
            with fresh.access(key,RequestBudget(None,2)) as token:self.assertTrue(token)

    def test_stale_select_logout_welcome_real_wait_B_commits_before_S(self):
        for action in ('select','logout','welcome'):
            with self.subTest(action=action),tempfile.TemporaryDirectory() as d:
                h=Harness(d);a=h.authorize();h.authorize('oaiapp_SECOND','SYNTHETIC_SECOND')
                real=h.session.store.thread_lock;lock=NonFIFO(real);h.session.store.thread_lock=lock
                workers=Workers(self);real.acquire();held=True
                try:
                    fn=(lambda:h.session.select(a)) if action=='select' else getattr(h.session,action)
                    done,out=workers.start(fn,'1005-stale');self.wait(lock.waiting);self.assertGreater(lock.failed,0)
                    real.release();held=False
                    p=h.begin();h.tokens(p,'oaiapp_THIRD','SYNTHETIC_THIRD');h.callback(p,'oaiapp_THIRD')
                    before=h.session.store.path.read_bytes();vault=copy.deepcopy(h.vault.data);calls=len(h.http.calls)
                    self.assertFalse(done.is_set());lock.allow.set();self.wait(done)
                    self.assertFalse(out[0][0]);self.assertIsInstance(out[0][1],ProviderFault)
                    self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)
                    self.assertEqual(len(h.http.calls),calls)
                finally:
                    if held:real.release()
                    lock.allow.set();workers.join()

    def test_new_begin_queued_does_not_take_over_finalizing_owner(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p)
            gate=Gate();workers=Workers(self);entered=threading.Event();locked=h.session.store.locked
            @contextmanager
            def observe(budget):
                if threading.current_thread().name=='1005-begin':entered.set()
                with locked(budget):yield
            with patch.object(h.session.store,'_edge',side_effect=lambda label:gate.hold() if label=='finalizing' else None),patch.object(h.session.store,'locked',observe):
                try:
                    ad,ao=workers.start(lambda:h.callback(p));self.wait(gate.entered)
                    bd,bo=workers.start(lambda:h.session.begin(CALLBACK),'1005-begin');self.wait(entered)
                    self.assertFalse(bd.is_set());self.assertEqual(h.session.transaction_status()['outcome'],'commit_won_pending')
                    gate.release.set();self.wait(ad);self.wait(bd);self.assertTrue(ao[0][0],ao);self.assertTrue(bo[0][0],bo)
                finally:gate.release.set();workers.join()

    def test_refresh_consumed_unknown_is_never_reused(self):
        from app.siwc_oauth import TOKEN
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();h.now+=3601;h.http.tokens.pop('id_token')
            h.http.hook=lambda url:h.session.cancel() if url==TOKEN else None
            with self.assertRaises(ProviderFault):h.session.models(key,RequestBudget(None,2))
            h.http.hook=None;before=len(h.http.calls);fresh=self.fresh(h)
            with self.assertRaisesRegex(ProviderFault,'REAUTHORIZATION_REQUIRED'):fresh.models(key,RequestBudget(None,2))
            self.assertEqual(len(h.http.calls),before);self.assertTrue(fresh.store.load()['accounts'][key]['rotation_pending'])

    def test_refresh_safe_save_cancel_then_logout_fixed_target(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();h.now+=3601;h.http.tokens.pop('id_token')
            h.http.tokens.update(access_token='SYNTHETIC_ROTATED',refresh_token='SYNTHETIC_ROTATED_REFRESH')
            original=h.session.store.replace_secret;write=h.vault.write
            def save(*args,**kwargs):
                h.session.cancel()
                def reentrant_write(ref,raw):
                    with self.assertRaisesRegex(ProviderFault,'MUTATION_PENDING'):h.session.logout()
                    return write(ref,raw)
                with patch.object(h.vault,'write',side_effect=reentrant_write):return original(*args,**kwargs)
            with patch.object(h.session.store,'replace_secret',side_effect=save):
                with self.assertRaisesRegex(ProviderFault,'AUTH_CANCELLED'):h.session.models(key,RequestBudget(None,2))
            a=h.session.store.load()['accounts'][key];self.assertFalse(a['rotation_pending'])
            self.assertEqual(h.session.store.secret(a)['refresh_token'],'SYNTHETIC_ROTATED_REFRESH')
            out=h.session.logout();self.assertTrue(out['local_logout_complete'])
            self.assertTrue(out['remote_revocation_confirmed'])
            with self.assertRaises(ProviderFault):self.fresh(h).models(key,RequestBudget(None,2))

    def test_local_logout_durable_before_remote_failure_and_gc(self):
        from app.siwc_oauth import DISCOVERY
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();seen=[]
            def hook(url):
                if url==DISCOVERY:
                    seen.append(self.fresh(h).store.load()['accounts'][key]['slots'])
                    raise OSError('synthetic remote failure')
            h.http.hook=hook;out=h.session.logout()
            self.assertEqual(seen,[[]]);self.assertTrue(out['local_logout_complete'])
            self.assertFalse(out['remote_revocation_confirmed'])
            with self.assertRaises(ProviderFault):self.fresh(h).models(key,RequestBudget(None,2))

    def test_failed_staging_preserves_old_and_owned_gc_only(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();old=copy.deepcopy(h.vault.data);foreign='not-application-owned'
            h.vault.data[foreign]=b'FOREIGN';p=h.begin(key);h.tokens(p);h.vault.fail_at=len(h.vault.writes)+1
            with self.assertRaises(ProviderFault):h.callback(p)
            with self.fresh(h).access(key,RequestBudget(None,2)) as token:self.assertEqual(token,'SYNTHETIC_ACCESS_NEVER_REAL')
            self.assertEqual(h.vault.data,{**old,foreign:b'FOREIGN'})

    def test_settings_save_status_models_welcome_consume_locked_view(self):
        from app.real_settings import SettingsStore
        from app.tests.test_siwc979_consumers import services
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();settings=SettingsStore(Path(d)/'settings.json',credential_factory=h.vault);settings.siwc=h.session
            settings.save({'services':services(),'secrets':{}})
            self.assertEqual(settings.snapshot()['roles']['credential_ref'],key)
            self.assertEqual(settings.snapshot()['dm']['credential_ref'],key)
            with patch.object(h.session.store,'factory',side_effect=AssertionError('status vault read')):
                self.assertEqual(settings.status()['siwc']['active'],key)
            self.assertTrue(h.session.models(key,RequestBudget(None,2)));h.session.welcome()
            self.assertFalse(settings.status()['siwc']['welcome_required'])

    @unittest.skipUnless(sys.platform=='win32','production msvcrt lock requires Windows')
    def test_real_windows_process_kill_recovery_and_reader_lock(self):
        # No shell, no inherited secrets, no WinCred. Only this owned tempfile tree.
        with tempfile.TemporaryDirectory(prefix='siwc1005-owned-') as d:
            root=Path(d);marker=root/'OWNED-SYNTHETIC-1005';marker.write_text('synthetic only')
            for stage in ('prepared_replaced','vault_written','committed_replaced','committed_readback','unlock','gc'):
                with self.subTest(stage=stage):
                    target=root/stage;target.mkdir();(target/'OWNED-SYNTHETIC-1005').write_text('synthetic only')
                    cmd=[sys.executable,'-m','app.tests.siwc1005_crashworker',str(target),stage]
                    process=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env={})
                    try:
                        # Dedicated reader avoids blocking the test owner on a broken child.
                        lines=[];workers=Workers(self)
                        done,out=workers.start(lambda:lines.append(process.stdout.readline()))
                        self.wait(done);self.assertTrue(out[0][0],out);self.assertEqual(lines,['GATE '+stage+'\n'])
                        vault_module=__import__('app.tests.siwc1005_crashworker',fromlist=['FileVault'])
                        store=SessionStore(target/'session.json',vault_module.FileVault(target))
                        # The child holds the real OS lock; a distinct instance cannot load.
                        with self.assertRaises(ProviderFault):store.load(RequestBudget(None,.15))
                        process.kill();process.wait(WAIT)
                        m=store.load(RequestBudget(None,2));self.assertEqual(m['active'],vault_module.KEY)
                        record=store.secret(m['accounts'][m['active']])
                        self.assertEqual(record['access_token'],'SYNTHETIC_NEW' if stage in ('committed_replaced','committed_readback','unlock','gc') else 'SYNTHETIC_OLD')
                    finally:
                        if process.poll() is None:process.kill();process.wait(WAIT)
                        workers.join()
                        for stream in (process.stdin,process.stdout,process.stderr):stream.close()

if __name__=='__main__':unittest.main()
