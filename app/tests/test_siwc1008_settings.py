"""1008 prospective Settings regressions. NOT RUN; synthetic authority/vault/wire.
Actual save, Gateway, access, locked and usage bodies; Events, never sleep.
Thread RLock tests do not claim Windows cross-process lock or real OAuth proof.
"""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from app.actor_execution import Attempt, ExecutionPolicy
from app.real_provider import RealGateway
from app.real_settings import SettingsStore
from app.siwc_responses import PublicHTTP, ResponsesDecoder
from app.tests.siwc979_synthetic import Harness, completed, frame
from app.tests.test_siwc979_consumers import services
from app.tests.test_siwc998_cancel import Workers, TEST_WAIT


class SettingsLockOrderTests(unittest.TestCase):
    def wait(self,event):self.assertTrue(event.wait(TEST_WAIT),'owner-controlled boundary stalled')

    def success(self,done,result):
        self.wait(done);self.assertTrue(result[0][0],result)
        return result[0][1]

    def fixture(self,directory):
        h=Harness(directory);key=h.authorize()
        settings=SettingsStore(Path(directory)/'services.json',credential_factory=h.vault)
        settings.siwc=h.session
        settings.save({'services':services(),'secrets':{}})
        return h,key,settings

    def test_real_save_waiting_on_access_allows_usage_then_succeeds_original_budgets(self):
        with tempfile.TemporaryDirectory() as directory:
            h,key,settings=self.fixture(directory)
            # Keep the original finite descriptor timeout; no test-only extension.
            attempt=Attempt(object(),ExecutionPolicy());gateway=RealGateway(settings)
            budget=gateway._budget('dm',settings.snapshot()['dm'],attempt)
            deadline=budget.deadline;workers=Workers(self)
            at_usage=threading.Event();allow_usage=threading.Event();usage_done=threading.Event()
            save_waiting=threading.Event();save_identity=[];save_budgets=[];observations=[]
            native_lock=h.session.store.thread_lock;native_locked=h.session.store.locked
            native_usage=settings.usage
            class ObservedLock:
                def acquire(proxy,*args,**kwargs):
                    if save_identity and threading.get_ident()==save_identity[0]:
                        if native_lock.acquire(blocking=False):return True
                        save_waiting.set()
                    return native_lock.acquire(*args,**kwargs)
                def release(proxy):return native_lock.release()
            @contextmanager
            def locked(original_budget):
                if (save_identity and threading.get_ident()==save_identity[0]
                        and not getattr(h.session.store.local,'depth',0)):
                    save_budgets.append((original_budget,original_budget.deadline))
                with native_locked(original_budget):yield
            def usage(event):
                if event['event']=='request_started':
                    observations.append(getattr(h.session.store.local,'depth',0))
                    at_usage.set()
                    if not allow_usage.wait(TEST_WAIT):raise AssertionError('usage release missing')
                native_usage(event)
                if event['event']=='request_started':usage_done.set()
            wire=PublicHTTP()
            def request(url,method,data,headers,original_budget,stream=False):
                self.assertIs(original_budget,budget)
                decoder=ResponsesDecoder(original_budget)
                decoder.feed(frame(completed(json.dumps({'synthetic':'settings-lock'}),
                                             {'input_tokens':3,'output_tokens':2})))
                return decoder.finish()
            wire.request=request;gateway.responses=wire
            def save():
                save_identity.append(threading.get_ident())
                return settings.save({'services':services(),'secrets':{}})
            try:
                with patch.object(h.session.store,'thread_lock',ObservedLock()),patch.object(
                        h.session.store,'locked',locked),patch.object(settings,'usage',usage):
                    done,result=workers.start(lambda:gateway._generate('dm',[],attempt))
                    self.wait(at_usage)
                    saved,save_result=workers.start(save);self.wait(save_waiting)
                    self.assertFalse(saved.is_set());self.assertFalse(done.is_set())
                    # Decisive old-source failure: S is waiting on actual L while
                    # A holds it; an unrelated thread must still obtain settings.
                    available=settings.lock.acquire(blocking=False)
                    try:self.assertTrue(available,'save held settings while waiting for account L')
                    finally:
                        if available:settings.lock.release()
                    self.assertEqual(observations,[1]);self.assertFalse(usage_done.is_set())
                    allow_usage.set();self.wait(usage_done)
                    text,usage_value=self.success(done,result)
                    reply=self.success(saved,save_result)
                    budget.check()
                    self.assertEqual(json.loads(text),{'synthetic':'settings-lock'})
                    self.assertEqual(usage_value['requests'],1)
                    self.assertEqual(reply['state'],'metadata_configured')
                    self.assertIs(gateway.budgets[(attempt,'dm')],budget)
                    self.assertEqual((budget.deadline,budget.requests),(deadline,1))
                    self.assertTrue(save_budgets)
                    for original_budget,original_deadline in save_budgets:
                        self.assertIs(original_budget,save_budgets[0][0])
                        self.assertEqual(original_budget.deadline,original_deadline)
                        original_budget.check()
            finally:allow_usage.set();workers.join()
            config=settings.snapshot()
            self.assertEqual([config[k]['credential_ref'] for k in ('roles','dm')],[key,key])
            rows=[json.loads(line) for line in settings.usage_path.read_text().splitlines()]
            self.assertEqual([row['event'] for row in rows],['request_started','request_returned'])
            self.assertNotIn('SYNTHETIC_ACCESS',settings.usage_path.read_text())

    def test_both_siwc_descriptors_share_one_real_locked_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            h,key,settings=self.fixture(directory);native=h.session.binding_snapshot;entries=[]
            @contextmanager
            def binding(budget=None):
                with native(budget) as value:
                    entries.append((value,getattr(h.session.store.local,'depth',0)))
                    yield value
            before=h.session.store.path.read_bytes();vault=copy.deepcopy(h.vault.data)
            with patch.object(h.session,'binding_snapshot',binding):
                settings.save({'services':services(),'secrets':{}})
            self.assertEqual(entries,[(key,1)])
            self.assertEqual([settings.snapshot()[k]['credential_ref'] for k in ('roles','dm')],[key,key])
            self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)

    def test_mixed_chat_secret_replace_failure_preserves_old_and_cleans_owned_buffer(self):
        with tempfile.TemporaryDirectory() as directory:
            h,key,settings=self.fixture(directory);old=settings.path.read_bytes();vault=copy.deepcopy(h.vault.data)
            mixed=services();mixed['roles'].update(provider='openai-chat-https',model='synthetic-chat')
            native=h.vault.write;buffers=[]
            def write(ref,data):buffers.append(data);native(ref,data)
            with patch.object(h.vault,'write',write),patch('app.real_settings.os.replace',side_effect=OSError('owned replace failure')):
                with self.assertRaisesRegex(OSError,'owned replace failure'):
                    settings.save({'services':mixed,'secrets':{'roles':'SYNTHETIC_CHAT_KEY'}})
            self.assertTrue(buffers);self.assertTrue(all(not any(b) for b in buffers))
            self.assertEqual(settings.path.read_bytes(),old)
            for ref,value in vault.items():self.assertEqual(h.vault.data[ref],value)
            self.assertEqual(list(settings.path.parent.glob(settings.path.name+'.*.pending')),[])
            settings.save({'services':mixed,'secrets':{'roles':'SYNTHETIC_CHAT_KEY'}})
            config=settings.snapshot();self.assertEqual(config['dm']['credential_ref'],key)
            self.assertEqual(h.vault.data[config['roles']['credential_ref']],b'SYNTHETIC_CHAT_KEY')
            retained=config['roles']['credential_ref']
            settings.save({'services':mixed,'secrets':{}})
            self.assertEqual(settings.snapshot()['roles']['credential_ref'],retained)
            self.assertNotIn('SYNTHETIC_CHAT_KEY',settings.path.read_text())
            with self.assertRaisesRegex(ValueError,'SIWC_SECRET_INPUT_FORBIDDEN'):
                settings.save({'services':services(),'secrets':{'dm':'forbidden'}})

    def test_snapshot_cannot_observe_half_save_and_readback_failure_is_not_false_success(self):
        with tempfile.TemporaryDirectory() as directory:
            h,key,settings=self.fixture(directory);workers=Workers(self)
            entered=threading.Event();release=threading.Event();reader_entered=threading.Event()
            import app.real_settings as module
            replace=module.os.replace
            def publish(source,target):
                entered.set()
                if not release.wait(TEST_WAIT):raise AssertionError('publish release missing')
                return replace(source,target)
            payload=services();payload['dm']['model']='SYNTHETIC_NEW_DM'
            def read():reader_entered.set();return settings.snapshot()
            try:
                with patch('app.real_settings.os.replace',side_effect=publish):
                    done,result=workers.start(lambda:settings.save({'services':payload,'secrets':{}}))
                    self.wait(entered);rd,rr=workers.start(read);self.wait(reader_entered)
                    self.assertFalse(rd.is_set());release.set();self.success(done,result)
                    config=self.success(rd,rr);self.assertEqual(config['dm']['model'],'SYNTHETIC_NEW_DM')
            finally:release.set();workers.join()
            original=settings.snapshot;calls=[]
            def snapshot():
                calls.append(None)
                return original() if len(calls)==1 else None
            with patch.object(settings,'snapshot',side_effect=snapshot):
                with self.assertRaisesRegex(ValueError,'SETTINGS_READBACK_FAILED'):
                    settings.save({'services':services(),'secrets':{}})
            self.assertEqual(list(settings.path.parent.glob(settings.path.name+'.*.pending')),[])
            self.assertEqual(original()['dm']['model'],'SYNTHETIC_dm')

    def test_non_siwc_save_status_never_holds_settings_while_waiting_for_account(self):
        with tempfile.TemporaryDirectory() as directory:
            h,key,settings=self.fixture(directory);native=h.session.status;observed=[]
            def status():
                observed.append(settings.lock._is_owned())
                return native()
            loopback=services()
            for s in loopback.values():s.update(provider='openai-chat-loopback',base_url='http://127.0.0.1:32124/v1')
            with patch.object(h.session,'status',side_effect=status):
                settings.save({'services':loopback,'secrets':{}})
            self.assertEqual(observed,[False])


if __name__=='__main__':unittest.main()
