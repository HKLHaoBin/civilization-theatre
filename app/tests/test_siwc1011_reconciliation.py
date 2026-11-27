"""1011 prospective definitions ONLY: never executed during source preparation.
The gate wraps the REAL outer store.locked return (including native unlock),
not its pre-native 'unlock' edge. Synthetic IO is not Windows qualification.
"""
from contextlib import contextmanager
import copy
import json
import tempfile
import threading
import unittest
from unittest.mock import patch
from app.real_provider import RequestBudget
from app.tests.siwc979_synthetic import Harness
from app.tests.test_siwc1005_account import Gate, Workers, WAIT
from app.tests import test_siwc1008_account as account1008


class Reconciliation1011Tests(unittest.TestCase):
    def wait(self,event):self.assertTrue(event.wait(WAIT),'missing owned ordering event')

    def view(self,m):
        return (copy.deepcopy(dict(m)),m.revision,m.owner,dict(m.generations),m.last_txid)

    def unknown(self,h,key):
        account1008.Account1008Tests().failed_callback(h,key,'committed_flush')
        tx=h.session._tx
        self.assertTrue(tx.recovery_ready());self.assertFalse(tx.status()['durable_ack'])
        return tx

    @contextmanager
    def after_query_unlock(self,s,budget,gate,observations):
        locked=s.store.locked;native=s.store.file_lock;armed=[True];native_returns=[]
        @contextmanager
        def native_observe(value):
            with native(value):yield
            if threading.current_thread().name=='1011-query':native_returns.append(value)
        @contextmanager
        def observe(value):
            outer=not getattr(s.store.local,'depth',0)
            with locked(value):yield
            if outer and threading.current_thread().name=='1011-query' and armed[0]:
                armed[0]=False
                observations.append((value,getattr(s.store.local,'depth',0),list(native_returns),s._tx.status()))
                gate.hold()
        with patch.object(s.store,'file_lock',native_observe),patch.object(s.store,'locked',observe):yield

    def fresh_access_logout(self,h,key,expected):
        s=h.session;access=RequestBudget(None,2);deadline=access.deadline;seen=[];locked=s.store.locked
        @contextmanager
        def observe(value):
            if not getattr(s.store.local,'depth',0):seen.append(value)
            with locked(value):yield
        # Neither a status workaround nor somebody else's refreshed snapshot is supplied.
        with patch.object(s.store,'locked',observe),patch.object(s,'status',side_effect=AssertionError('no status before access')):
            with s.access(key,access) as token:self.assertEqual(token,expected)
        self.assertEqual(seen,[access,access]);self.assertEqual(access.deadline,deadline)
        created=[];seen.clear()
        def budget_factory(cancel,seconds):
            value=RequestBudget(cancel,seconds);created.append((value,seconds,value.deadline));return value
        with patch('app.siwc_oauth.RequestBudget',side_effect=budget_factory),patch.object(s.store,'locked',observe):
            result=s.logout()
        self.assertTrue(result['local_logout_complete']);self.assertTrue(result['durable_ack'])
        self.assertEqual([seconds for _,seconds,_ in created],[15,2])
        self.assertEqual(seen,[created[0][0],created[1][0]])
        for value,_,original in created:self.assertEqual(value.deadline,original)
        self.assertFalse(s.store.load()['accounts'][key]['slots'])

    def successor_case(self,kind):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();s=h.session;old=self.unknown(h,key)
            gate=Gate();workers=Workers(self);observations=[];budget=RequestBudget(None,2);deadline=budget.deadline
            with self.after_query_unlock(s,budget,gate,observations):
                try:
                    qd,qo=workers.start(lambda:s.transaction_status(budget),'1011-query');self.wait(gate.entered)
                    self.assertEqual(len(observations),1)
                    value,depth,native_returns,ack=observations[0]
                    self.assertIs(value,budget);self.assertEqual(depth,0);self.assertEqual(native_returns,[budget])
                    self.assertFalse(ack['durable_ack']);self.assertIs(s._tx,old)
                    def successor():
                        if kind=='welcome':return s.welcome()
                        p=h.begin(key);h.tokens(p,access_token='SYNTHETIC_SUCCESSOR_1011');h.callback(p)
                        return s.status()
                    bd,bo=workers.start(successor,'1011-successor');self.wait(bd);self.assertTrue(bo[0][0],bo)
                    self.assertFalse(qd.is_set());self.assertFalse(gate.release.is_set())
                    current=s._tx;current_ack=current.status();current_view=self.view(s._view)
                    self.assertIsNot(current,old);self.assertTrue(current_ack['durable_ack'])
                    self.assertEqual(bo[0][1]['txid'],current.id)
                    raw=s.store.path.read_bytes();vault=copy.deepcopy(h.vault.data);epoch=s.epoch
                    candidate=old.plan[1]
                    if kind=='welcome':
                        self.assertEqual(s._view.revision,candidate['revision']+1)
                        self.assertEqual(s._view.generations,candidate['generations'])
                    else:self.assertNotEqual(s._view.owner,candidate['owner'])
                    gate.release.set();self.wait(qd);self.assertTrue(qo[0][0],qo)
                    self.assertIs(s._tx,current);self.assertEqual(current.status(),current_ack)
                    self.assertEqual(qo[0][1],current_ack);self.assertEqual(self.view(s._view),current_view)
                    self.assertEqual(s.epoch,epoch);self.assertEqual(s.store.path.read_bytes(),raw)
                    self.assertEqual(h.vault.data,vault);self.assertEqual(budget.deadline,deadline)
                    self.assertEqual(old.status()['txid'],old.id)
                    self.assertEqual(old.status()['outcome'],'committed_complete');self.assertTrue(old.status()['durable_ack'])
                    self.fresh_access_logout(h,key,'SYNTHETIC_RECOVERED_1008' if kind=='welcome' else 'SYNTHETIC_SUCCESSOR_1011')
                finally:gate.release.set();workers.join()

    def test_same_session_welcome_after_query_native_unlock_preserves_successor(self):
        self.successor_case('welcome')

    def test_same_session_reauth_after_query_native_unlock_preserves_successor(self):
        self.successor_case('reauth')

    def test_no_successor_query_publishes_real_recovered_view(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();s=h.session;tx=self.unknown(h,key)
            old_revision=s._view.revision;budget=RequestBudget(None,2);deadline=budget.deadline
            result=s.transaction_status(budget)
            self.assertEqual(result['txid'],tx.id);self.assertTrue(result['durable_ack'])
            self.assertGreater(s._view.revision,old_revision)
            self.assertEqual(self.view(s._view),self.view(s.store.load()))
            self.assertEqual(budget.deadline,deadline)
            self.fresh_access_logout(h,key,'SYNTHETIC_RECOVERED_1008')

    def test_same_transaction_newer_binding_view_not_overwritten_by_old_query(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();s=h.session;tx=self.unknown(h,key)
            gate=Gate();workers=Workers(self);observations=[];budget=RequestBudget(None,2)
            with self.after_query_unlock(s,budget,gate,observations):
                try:
                    qd,qo=workers.start(lambda:s.transaction_status(budget),'1011-query');self.wait(gate.entered)
                    other=account1008.Account1008Tests().fresh(h)
                    bd,bo=workers.start(other.welcome,'1011-other');self.wait(bd);self.assertTrue(bo[0][0],bo)
                    self.assertIs(s._tx,tx)
                    self.assertEqual(s.binding(),key)  # second query, same historical tx, newer current view
                    newer=self.view(s._view);raw=s.store.path.read_bytes();vault=copy.deepcopy(h.vault.data)
                    self.assertGreater(s._view.revision,tx.plan[1]['revision'])
                    self.assertFalse(qd.is_set());self.assertFalse(gate.release.is_set())
                    gate.release.set();self.wait(qd);self.assertTrue(qo[0][0],qo)
                    self.assertIs(s._tx,tx);self.assertEqual(self.view(s._view),newer)
                    self.assertEqual(s.store.path.read_bytes(),raw);self.assertEqual(h.vault.data,vault)
                    self.assertTrue(tx.status()['durable_ack'])  # Q's genuine earlier proof of A remains historical truth
                    self.fresh_access_logout(h,key,'SYNTHETIC_RECOVERED_1008')
                finally:gate.release.set();workers.join()

    def test_late_status_load_binding_and_intent_never_publish_lower_revision_or_borrow(self):
        for consumer in ('status','binding','intent'):
            with self.subTest(consumer=consumer),tempfile.TemporaryDirectory() as d:
                h=Harness(d);key=h.authorize();s=h.session;gate=Gate();workers=Workers(self);load=s.store.load;armed=[True]
                old=self.view(s._view);captured=[]
                def delayed(*args,**kwargs):
                    outer=not getattr(s.store.local,'depth',0)
                    m=load(*args,**kwargs)
                    if outer and threading.current_thread().name=='1011-late' and armed[0]:
                        armed[0]=False;captured.append(self.view(m));gate.hold()
                    return m
                def consume():
                    if consumer=='intent':return s._intent(RequestBudget(None,2))
                    return getattr(s,consumer)()
                with patch.object(s.store,'load',side_effect=delayed):
                    try:
                        qd,qo=workers.start(consume,'1011-late');self.wait(gate.entered)
                        bd,bo=workers.start(s.welcome,'1011-successor');self.wait(bd);self.assertTrue(bo[0][0],bo)
                        newer=self.view(s._view);current=s._tx
                        self.assertFalse(gate.release.is_set());self.assertFalse(qd.is_set())
                        gate.release.set();self.wait(qd);self.assertTrue(qo[0][0],qo)
                        self.assertEqual(captured,[old]);self.assertEqual(self.view(s._view),newer);self.assertIs(s._tx,current)
                        if consumer=='intent':self.assertEqual(qo[0][1][0],old[1])  # return OLD authority, not newer welcome
                        self.fresh_access_logout(h,key,'SYNTHETIC_ACCESS_NEVER_REAL')
                    finally:gate.release.set();workers.join()

    def test_live_writer_does_not_recover_and_cancel_does_not_wait_for_L(self):
        with tempfile.TemporaryDirectory() as d:
            h=Harness(d);key=h.authorize();p=h.begin(key);h.tokens(p);s=h.session
            gate=Gate();workers=Workers(self)
            def edge(label):
                if label=='committed_flush':raise OSError('owned UNKNOWN before unlock')
                if label=='unlock':gate.hold()
            with patch.object(s.store,'_edge',side_effect=edge):
                try:
                    wd,wo=workers.start(lambda:h.callback(p),'1011-writer');self.wait(gate.entered)
                    with patch.object(s.store,'reconcile',side_effect=AssertionError('live recovery forbidden')) as reconcile:
                        self.assertFalse(s.transaction_status()['durable_ack'])
                        cd,co=workers.start(s.cancel,'1011-cancel');self.wait(cd);self.assertTrue(co[0][0],co)
                        self.assertFalse(co[0][1]['durable_ack'])
                    reconcile.assert_not_called();self.assertFalse(wd.is_set());self.assertFalse(gate.release.is_set())
                    gate.release.set();self.wait(wd);self.assertTrue(wo[0][0],wo)
                finally:gate.release.set();workers.join()
            self.assertTrue(s.transaction_status()['durable_ack'])


if __name__=='__main__':unittest.main()
