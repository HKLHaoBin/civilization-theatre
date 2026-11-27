"""1008 prospective regressions. NOT RUN; no real OAuth/vault/network.
All concurrency owner gates have finally release/join; no timing sleeps.
"""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from app.real_provider import ProviderFault, RequestBudget, RealGateway
from app.siwc_oauth import SIWCSession, TOKEN
from app.tests.siwc979_synthetic import Harness, SyntheticVerifier, synthetic_file_lock
from app.tests.test_siwc1005_account import Gate, Workers, NonFIFO, WAIT

class Account1008Tests(unittest.TestCase):
    def wait(self,event):self.assertTrue(event.wait(WAIT),'missing owned ordering event')
    def fresh(self,h):
        s=SIWCSession(h.session.store.path,h.vault,http=h.http,verifier=SyntheticVerifier(),
            clock=lambda:h.now,opener=h.urls.append,file_lock=synthetic_file_lock)
        s.store.thread_lock=h.session.store.thread_lock
        return s
    def expire(self,h):
        h.now+=3601;h.http.tokens.pop('id_token',None)
        h.http.tokens.update(access_token='SYNTHETIC_ROTATED_1008',refresh_token='SYNTHETIC_REFRESH_1008')
    def read(self,s,key,budget):
        with s.access(key,budget) as token:return token

    def test_two_same_identity_readers_succeed_one_refresh_original_budgets(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();self.expire(h)
            gate=Gate();workers=Workers(self);waiting=threading.Event();locked=h.session.store.locked
            first=RequestBudget(None,10);second=RequestBudget(None,10);deadlines=(first.deadline,second.deadline)
            @contextmanager
            def observe(budget):
                if threading.current_thread().name=='1008-reader':waiting.set()
                with locked(budget):yield
            h.http.hook=lambda url:gate.hold() if url==TOKEN else None
            before=len(h.http.calls)
            with patch.object(h.session.store,'locked',observe):
                try:
                    ad,ao=workers.start(lambda:self.read(h.session,key,first));self.wait(gate.entered)
                    bd,bo=workers.start(lambda:self.read(h.session,key,second),'1008-reader');self.wait(waiting)
                    self.assertFalse(bd.is_set());gate.release.set();self.wait(ad);self.wait(bd)
                    self.assertEqual(ao,[(True,'SYNTHETIC_ROTATED_1008')]);self.assertEqual(bo,ao)
                    calls=[c for c in h.http.calls[before:] if c[0]==TOKEN]
                    self.assertEqual(len(calls),1);self.assertIs(calls[0][4],first)
                    self.assertEqual((first.deadline,second.deadline),deadlines)
                finally:gate.release.set();h.http.hook=None;workers.join()

    def test_cross_instance_fresh_access_and_logout_without_status(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();s=self.fresh(h);self.assertEqual(s.binding(),key)
            self.expire(h);self.assertEqual(self.read(h.session,key,RequestBudget(None,2)),'SYNTHETIC_ROTATED_1008')
            calls=len(h.http.calls)
            with patch.object(s,'status',side_effect=AssertionError('access must not need status')):
                self.assertEqual(self.read(s,key,RequestBudget(None,2)),'SYNTHETIC_ROTATED_1008')
            self.assertEqual(len(h.http.calls),calls)
            out=s.logout();self.assertTrue(out['local_logout_complete'])
            self.assertFalse(h.session.store.load()['accounts'][key]['slots'])

    def test_real_gateway_nonretryable_path_accepts_rotated_cached_session(self):
        from app.real_settings import SettingsStore
        from app.tests.test_siwc979_consumers import services
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();cached=self.fresh(h);cached.binding()
            settings=SettingsStore(Path(d)/'settings.json',credential_factory=h.vault);settings.siwc=cached
            settings.save({'services':services(),'secrets':{}})
            self.expire(h);self.read(h.session,key,RequestBudget(None,2))
            gateway=RealGateway(settings,credential_factory=h.vault)
            class Responses:
                def responses(self,service,messages,token,budget):
                    return {'choices':[{'message':{'content':'synthetic response'}}], 'usage':{'prompt_tokens':1,'completion_tokens':1}}
            gateway.responses=Responses()
            with patch.object(cached,'status',side_effect=AssertionError('no status workaround')):
                text,usage=gateway._generate('roles',[{'role':'user','content':'synthetic'}],None)
            self.assertEqual(text,'synthetic response');self.assertEqual(usage['requests'],1)

    def test_true_new_owner_reauth_or_revocation_never_borrows_identity(self):
        for change in ('other','reauth','revoke'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as d:
                h=Harness(d);key=h.authorize();s=self.fresh(h);s.binding()
                if change=='other':h.authorize('oaiapp_SECOND','SYNTHETIC_SECOND')
                elif change=='reauth':
                    p=h.begin(key);h.tokens(p);h.callback(p)
                else:h.session.logout()
                before=h.session.store.path.read_bytes();vault=copy.deepcopy(h.vault.data);calls=len(h.http.calls)
                for op in (lambda:self.read(s,key,RequestBudget(None,2)),s.logout):
                    with self.assertRaises(ProviderFault):op()
                self.assertEqual(h.session.store.path.read_bytes(),before)
                self.assertEqual(h.vault.data,vault);self.assertEqual(len(h.http.calls),calls)

    def failed_callback(self,h,key,edge):
        p=h.begin(key);h.tokens(p,access_token='SYNTHETIC_RECOVERED_1008');armed=[True]
        def fault(label):
            if label==edge and armed[0]:armed[0]=False;raise OSError('owned one-time '+edge)
        with patch.object(h.session.store,'_edge',side_effect=fault):
            try:result=h.callback(p)
            except ProviderFault:result=None
        self.assertFalse(armed[0]);return result

    def test_same_session_queries_reconcile_once_flush_readback_and_gc(self):
        for edge in ('committed_flush','committed_readback','gc'):
            for consumer in ('status','binding','binding_snapshot','transaction_status'):
                with self.subTest(edge=edge,consumer=consumer),tempfile.TemporaryDirectory() as d:
                    h=Harness(d);key=h.authorize();self.failed_callback(h,key,edge)
                    tx=h.session._tx;self.assertFalse(tx.status()['durable_ack'])
                    seen=[];original=h.session.store._read
                    def read():
                        seen.append(getattr(h.session.store.local,'depth',0));return original()
                    with patch.object(h.session.store,'_read',side_effect=read):
                        if consumer=='binding_snapshot':
                            with h.session.binding_snapshot() as binding:self.assertEqual(binding,key)
                        else:
                            value=getattr(h.session,consumer)()
                            if consumer=='binding':self.assertEqual(value,key)
                    self.assertTrue(seen);self.assertTrue(all(seen))
                    ack=h.session.transaction_status();self.assertEqual(ack['txid'],tx.id)
                    self.assertEqual(ack['outcome'],'committed_complete');self.assertTrue(ack['durable_ack'])
                    self.assertEqual(self.read(h.session,key,RequestBudget(None,2)),'SYNTHETIC_RECOVERED_1008')
                    self.assertFalse(json.loads(h.session.store.path.read_text())['gc'])

    def test_prepared_unknown_recovers_old_with_original_txid(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p)
            def fault(label):
                if label in ('prepared_readback','recovery_flush'):raise OSError('owned pre-F/recovery interruption')
            with patch.object(h.session.store,'_edge',side_effect=fault):
                result=h.callback(p)
            self.assertEqual(result['outcome'],'commit_unknown');self.assertFalse(result['durable_ack'])
            # Recovery replace may have won; no tx-specific evidence remains:
            # safe current view may resume but must not invent old ACK.
            ack=h.session.transaction_status();self.assertEqual(ack['outcome'],'commit_unknown')
            self.assertFalse(ack['durable_ack']);self.assertTrue(ack['current_view_confirmed'])
            self.assertEqual(h.session.binding(),key)
            self.assertEqual(self.read(h.session,key,RequestBudget(None,2)),'SYNTHETIC_ACCESS_NEVER_REAL')

    def test_matching_prepared_unknown_recovers_cancelled_complete(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p)
            original=h.session.store._recover;armed=[False]
            def edge(label):
                if label=='prepared_readback':armed[0]=True;raise OSError('owned prepared interruption')
            def recover():
                if armed[0]:raise ProviderFault('SIWC_RECOVERY_PENDING')
                return original()
            with patch.object(h.session.store,'_edge',side_effect=edge),patch.object(h.session.store,'_recover',side_effect=recover):
                result=h.callback(p)
            self.assertEqual(result['outcome'],'commit_unknown');self.assertFalse(result['durable_ack'])
            self.assertEqual(json.loads(h.session.store.path.read_text())['state'],'PREPARED')
            txid=h.session._tx.id;ack=h.session.transaction_status()
            self.assertEqual(ack['txid'],txid);self.assertEqual(ack['outcome'],'cancelled_complete')
            self.assertTrue(ack['durable_ack']);self.assertEqual(h.session.binding(),key)

    def test_live_writer_status_never_enters_recovery(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p)
            gate=Gate();workers=Workers(self)
            def edge(label):
                if label=='committed_flush':raise OSError('owned unknown before native unlock')
                if label=='unlock':gate.hold()
            with patch.object(h.session.store,'_edge',side_effect=edge):
                try:
                    done,out=workers.start(lambda:h.callback(p));self.wait(gate.entered)
                    with patch.object(h.session.store,'_recover',side_effect=AssertionError('live writer recovery forbidden')) as recovery:
                        ack=h.session.transaction_status();status=h.session.status()
                        self.assertEqual(ack['outcome'],'commit_unknown')
                        self.assertFalse(ack['durable_ack']);self.assertEqual(status['state'],'account_mutation_pending')
                    recovery.assert_not_called()
                    self.assertFalse(done.is_set());gate.release.set();self.wait(done)
                finally:gate.release.set();workers.join()
            self.assertTrue(h.session.transaction_status()['durable_ack'])

    def test_corruption_wrong_txid_and_successor_never_fake_old_ack(self):
        for change in ('corrupt','wrong_txid','successor'):
            with self.subTest(change=change),tempfile.TemporaryDirectory() as d:
                h=Harness(d);key=h.authorize();self.failed_callback(h,key,'committed_flush');tx=h.session._tx
                before=json.loads(h.session.store.path.read_text())
                self.assertEqual(before['state'],'COMMITTED');self.assertIsNone(before['candidate'])
                self.assertEqual(before['planned'],[]);self.assertIsNone(before['txid'])
                retired=set(before['gc']);live={r for a in before['stable']['value']['accounts'].values() for r in a['slots']}
                old,candidate=tx.plan
                self.assertEqual(before['stable'],candidate)
                oldrefs={r for a in old['value']['accounts'].values() for r in a['slots']}
                self.assertEqual(retired,oldrefs-live);self.assertTrue(retired);self.assertFalse(retired & live)
                for ref in retired:self.assertRegex(ref,r'^civilization-theatre/service/[0-9a-f]{32}$')
                # Both external and owned-but-unlisted refs are outside GC authority.
                h.vault.data['not-application-owned']=b'SYNTHETIC_EXTERNAL_GUARD'
                guard='civilization-theatre/service/'+'f'*32
                self.assertNotIn(guard,h.vault.data);self.assertNotIn(guard,retired|live)
                h.vault.data[guard]=b'SYNTHETIC_UNLISTED_GUARD'
                vault_before=copy.deepcopy(h.vault.data);deletes=len(h.vault.deleted);writes=list(h.vault.writes)
                expected=copy.deepcopy(before);expected_vault=copy.deepcopy(vault_before)
                if change=='successor':
                    s=self.fresh(h);result=s.welcome()
                    successor=s._tx;self.assertTrue(result['durable_ack']);self.assertNotEqual(successor.id,tx.id)
                    # The real welcome producer changes exactly these stable fields, plus GC.
                    expected['stable']['revision']+=1
                    expected['stable']['last_txid']=successor.id
                    expected['stable']['value']['accounts'][key]['welcome_seen']=True
                    expected['gc']=[]
                    for ref in retired:del expected_vault[ref]
                    self.assertEqual(json.loads(s.store.path.read_text()),expected)
                    self.assertEqual(h.vault.data,expected_vault)
                    self.assertEqual(h.vault.deleted[deletes:],sorted(retired))
                else:
                    e=copy.deepcopy(before)
                    if change=='corrupt':e['state']='UNKNOWN'
                    else:
                        e['stable']['last_txid']='a'*32 if tx.id!='a'*32 else 'b'*32
                        self.assertNotEqual(e['stable']['last_txid'],tx.id)
                    h.session.store.path.write_text(json.dumps(e));expected=copy.deepcopy(e)
                    if change=='wrong_txid':
                        expected['gc']=[]
                        for ref in retired:del expected_vault[ref]
                raw=h.session.store.path.read_bytes();stable_before_query=copy.deepcopy(json.loads(raw)['stable'])
                query_deletes=len(h.vault.deleted);ack=h.session.transaction_status()
                self.assertEqual(ack['txid'],tx.id);self.assertEqual(ack['outcome'],'commit_unknown');self.assertFalse(ack['durable_ack'])
                self.assertEqual(tx.status()['txid'],tx.id);self.assertTrue(ack['historical_outcome_unknown'])
                if change=='corrupt':
                    self.assertFalse(ack['current_view_confirmed']);self.assertTrue(ack['account_mutation_pending'])
                    self.assertEqual(h.session.status()['state'],'account_mutation_pending')
                    with self.assertRaises(ProviderFault):h.session.binding()
                    self.assertEqual(h.session.store.path.read_bytes(),raw)
                    self.assertEqual(h.vault.deleted[query_deletes:],[])
                else:
                    self.assertTrue(ack['current_view_confirmed']);self.assertFalse(ack['account_mutation_pending'])
                    self.assertEqual(h.session.binding(),key)
                    self.assertEqual(h.session.status()['state'],'authorized_not_verified')
                    actual=json.loads(h.session.store.path.read_text())
                    self.assertEqual(actual['stable'],stable_before_query)  # ALL current authorization, refs, owner, gen, pointer
                    self.assertEqual(actual,expected)  # full envelope: only precise GC compression allowed
                    self.assertEqual(h.vault.deleted[query_deletes:],sorted(retired) if change=='wrong_txid' else [])
                    if change=='successor':self.assertEqual(h.session.store.path.read_bytes(),raw)
                self.assertEqual(h.vault.data,expected_vault if change!='corrupt' else vault_before)
                self.assertEqual(h.vault.writes,writes)
                for ref in retired:
                    if change=='corrupt':self.assertEqual(h.vault.data[ref],vault_before[ref])
                    else:self.assertNotIn(ref,h.vault.data)
                for ref in live|{guard,'not-application-owned'}:
                    self.assertEqual(h.vault.data[ref],vault_before[ref])

    def test_reconciliation_uses_supplied_absolute_budget_and_no_retry(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();self.failed_callback(h,key,'committed_flush')
            budget=RequestBudget(None,2);deadline=budget.deadline;seen=[];locked=h.session.store.locked
            @contextmanager
            def observe(value):
                seen.append(value)
                with locked(value):yield
            with patch.object(h.session.store,'locked',observe):h.session.transaction_status(budget)
            self.assertEqual(seen,[budget]);self.assertEqual(budget.deadline,deadline)
            self.failed_callback(h,key,'committed_flush');expired=RequestBudget(None,0)
            ack=h.session.transaction_status(expired);self.assertFalse(ack['durable_ack'])
            self.assertEqual(ack['outcome'],'commit_unknown')

    def test_F_won_before_replace_failure_recovers_matching_prepared_not_commit(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p)
            def fault(label):
                if label=='finalizing':raise OSError('owned F won before final replace')
            with patch.object(h.session.store,'_edge',side_effect=fault):result=h.callback(p)
            self.assertEqual(result['outcome'],'commit_unknown');self.assertFalse(result['durable_ack'])
            self.assertEqual(json.loads(h.session.store.path.read_text())['state'],'PREPARED')
            ack=h.session.transaction_status();self.assertEqual(ack['outcome'],'cancelled_complete')
            self.assertTrue(ack['durable_ack']);self.assertEqual(self.read(h.session,key,RequestBudget(None,2)),'SYNTHETIC_ACCESS_NEVER_REAL')

    def test_unlock_failure_never_ack_until_query_reacquires_and_releases_L(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p)
            original=h.session.store.file_lock;armed=[True];entered=[]
            @contextmanager
            def fail_once(budget):
                with original(budget):yield
                if armed[0]:armed[0]=False;raise OSError('owned native unlock returned unknown')
            with patch.object(h.session.store,'file_lock',fail_once):
                with self.assertRaisesRegex(ProviderFault,'SIWC_AUTH_ERROR'):h.callback(p)
                result=h.session._transaction_status()
                self.assertFalse(result['durable_ack']);self.assertEqual(result['outcome'],'commit_unknown')
                @contextmanager
                def observe(budget):
                    entered.append('acquired')
                    with original(budget):yield
                    entered.append('released')
                with patch.object(h.session.store,'file_lock',observe):ack=h.session.transaction_status()
            self.assertEqual(entered,['acquired','released']);self.assertTrue(ack['durable_ack'])
            self.assertEqual(ack['outcome'],'committed_complete')

    def test_true_owner_fault_stays_nonretryable_on_actual_gateway(self):
        from app.real_settings import SettingsStore
        from app.tests.test_siwc979_consumers import services
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();s=self.fresh(h);s.binding()
            settings=SettingsStore(Path(d)/'settings.json',credential_factory=h.vault);settings.siwc=s
            settings.save({'services':services(),'secrets':{}})
            p=h.begin(key);h.tokens(p);h.callback(p)
            gateway=RealGateway(settings,credential_factory=h.vault)
            calls=[]
            class Responses:
                def responses(self,*args):calls.append(args);raise AssertionError('must not use new owner')
            gateway.responses=Responses()
            before=h.session.store.path.read_bytes();vault=copy.deepcopy(h.vault.data)
            with self.assertRaises(ProviderFault) as caught:gateway._generate('roles',[],None)
            self.assertEqual(caught.exception.code,'SIWC_STALE_OWNER_CANCELLED');self.assertFalse(caught.exception.retryable)
            self.assertEqual(calls,[]);self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)
            self.assertFalse(settings.usage_path.exists())

if __name__=='__main__':unittest.main()
