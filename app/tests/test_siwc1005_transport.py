"""1005 prospective owner-scope regressions: NOT RUN during preparation.
Real PublicHTTP/Gateway/Host/Attempt, synthetic HTTP/TLS/vault/authority only.
TEST_WAIT is a hang detector, not a renewed product budget. No live network.
Run only from the parent's complete, reviewed, combined 1005 generation.
"""
from contextlib import contextmanager, nullcontext
import gc
import json
import queue
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import weakref
from app.real_provider import ProviderFault, RequestBudget, RealGateway
from app.siwc_responses import API, PublicHTTP
from app.tests.siwc979_synthetic import Harness, CALLBACK, completed, frame
from app.tests.test_siwc979_consumers import services
from app.tests.test_siwc998_cancel import Gate, Connection, TLS, Workers, TEST_WAIT


class OwnerTransportTests(unittest.TestCase):
    def wait(self, event):self.assertTrue(event.wait(TEST_WAIT), 'control stalled behind native boundary')

    def success(self, done, outcome):
        self.wait(done);self.assertTrue(outcome[0][0], outcome)
        return outcome[0][1]

    def cancelled(self, done, outcome):
        self.wait(done);self.assertFalse(outcome[0][0], outcome)
        self.assertIsInstance(outcome[0][1], ProviderFault)
        self.assertIn('CANCEL', outcome[0][1].code)

    @contextmanager
    def lanes(self, stage='request'):
        """Each real worker selects one synthetic native lane, never a fake cancel."""
        http=PublicHTTP();gates={name:Gate() for name in ('auth','host','other')}
        connections={name:[] for name in gates};tls=TLS(gates['host'],stage)
        workers=Workers(self)
        def factory(*args, **kwargs):
            lane=http.local.test_lane
            c=Connection(gates[lane],stage,kwargs['context']);connections[lane].append(c)
            if lane=='auth':
                from app.siwc_oauth import ISSUER, AUTHORIZE, TOKEN, JWKS
                c.response.parts=[json.dumps(dict(issuer=ISSUER,authorization_endpoint=AUTHORIZE,
                    token_endpoint=TOKEN,jwks_uri=JWKS)).encode(),b'']
                native_request=c.request
                def auth_request(method,path,*a,**k):
                    payloads=getattr(http,'test_auth_payloads',{})
                    if path in payloads:c.response.parts=[json.dumps(payloads[path]).encode(),b'']
                    return native_request(method,path,*a,**k)
                c.request=auth_request
            elif lane=='host':
                c.response.parts=[frame(completed(json.dumps({'synthetic':'owned response'}))),b'']
                c.response.getheader=lambda *a:'text/event-stream'
            if stage=='constructor':gates[lane].wait()
            return c
        def on_lane(lane, function):
            http.local.test_lane=lane
            try:return function()
            finally:del http.local.test_lane
        with patch('app.siwc_responses.ssl.SSLContext',return_value=tls), patch(
                'app.siwc_responses.http.client.HTTPSConnection',side_effect=factory):
            try:yield http,gates,connections,workers,on_lane
            finally:
                for gate in gates.values():gate.release.set()
                workers.join()
                self.assertEqual(http.active,set())
                for group in connections.values():
                    for c in group:
                        self.assertTrue(c.closed)
                        self.assertTrue(all(sock.closed for sock in c.sockets))

    def direct(self, http, owner, lane, run, budget=None):
        budget=budget or RequestBudget(None,30)
        def request():
            with http.operation(None,owner=owner):
                return http.request(API+'/models','GET',None,{},budget)
        return run(lane,request)

    def settings(self, h):
        from app.real_settings import SettingsStore
        settings=SettingsStore(h.session.store.path.parent/'services.json',credential_factory=h.vault)
        settings.siwc=h.session;descriptors=services()
        for descriptor in descriptors.values():descriptor['timeout_seconds']=30
        settings.save({'services':descriptors,'secrets':{}})
        return settings

    def host(self, gateway):
        from app.actors import ActorHost
        host=object.__new__(ActorHost);host.real_gateway=gateway
        host._worker_lock=threading.RLock();host._process=None
        host._responses=queue.Queue();host._readers=[]
        return host

    def installed_owner_baseline(self,h,before_begin,vault):
        # Discovery gate is AFTER the real begin owner transaction has committed,
        # before ANY test mutator. Compare the full envelope with ONLY these
        # three exact bookkeeping substitutions; never exempt account fields.
        import copy
        before=json.loads(before_begin)
        baseline=h.session.store.path.read_bytes();after=json.loads(baseline)
        self.assertEqual(before['state'],'COMMITTED');self.assertEqual(after['state'],'COMMITTED')
        old=before['stable'];new=after['stable']
        self.assertEqual(new['revision'],old['revision']+1)
        self.assertRegex(new['owner'],r'^[0-9a-f]{32}$');self.assertNotEqual(new['owner'],old['owner'])
        self.assertRegex(new['last_txid'],r'^[0-9a-f]{32}$')
        self.assertNotEqual(new['last_txid'],old['last_txid'])
        ack=h.session.transaction_status()
        self.assertTrue(ack['durable_ack']);self.assertEqual(new['last_txid'],ack['txid'])
        expected=copy.deepcopy(before)
        for name in ('revision','owner','last_txid'):expected['stable'][name]=new[name]
        self.assertEqual(after,expected)  # includes active, ALL accounts/refs/hash/slots, GC and generations
        self.assertEqual(h.vault.data,vault)
        return baseline

    def test_actual_host_stop_cancels_gateway_not_pending_oauth_discovery(self):
        from app.actor_execution import Attempt, ExecutionPolicy
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);h.authorize();settings=self.settings(h)
            before_begin=h.session.store.path.read_bytes();vault=dict(h.vault.data)
            with self.lanes() as (http,gates,connections,workers,run):
                h.session.http=http;h.session.lifecycle_lock=http.lock
                gateway=RealGateway(settings);host=self.host(gateway)
                auth_done,auth_result=workers.start(lambda:run('auth',lambda:h.session.begin(CALLBACK)))
                self.wait(gates['auth'].entered)
                before=self.installed_owner_baseline(h,before_begin,vault)
                pending=h.session.pending;epoch=h.session.epoch
                attempt=Attempt(object(),ExecutionPolicy())
                budget=gateway._budget('dm',settings.snapshot()['dm'],attempt);deadline=budget.deadline
                done,outcome=workers.start(lambda:run('host',lambda:gateway._generate('dm',[],attempt)))
                self.wait(gates['host'].entered)
                stopped,control=workers.start(host._stop)
                self.success(stopped,control)
                self.assertIsNone(host._responses.get_nowait())
                self.assertIs(h.session.pending,pending);self.assertEqual(h.session.epoch,epoch)
                self.assertFalse(h.session.closed);self.assertFalse(auth_done.is_set())
                self.assertFalse(gates['auth'].release.is_set());self.assertFalse(done.is_set())
                auth_latches=[latch for latch in http.active if latch.scope is http._default_owner]
                self.assertEqual(len(auth_latches),1);self.assertFalse(auth_latches[0].cancelled)
                self.assertIs(gateway.budgets[(attempt,'dm')],budget)
                self.assertEqual((budget.deadline,budget.requests),(deadline,1))
                gates['host'].release.set();self.cancelled(done,outcome)
                self.assertFalse(connections['host'][0].sent)
                self.assertFalse(auth_done.is_set());self.assertFalse(connections['auth'][0].sockets[0].closed)
                gates['auth'].release.set();self.success(auth_done,auth_result)
                self.assertIsNotNone(h.session.pending)
                self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)
                self.assertEqual((budget.deadline,budget.requests),(deadline,1))

    def test_actual_oauth_callback_survives_host_stop_and_can_commit_after_host_unwinds(self):
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);key=h.authorize();settings=self.settings(h)
            params=h.begin(key);h.tokens(params)
            with self.lanes() as (http,gates,connections,workers,run):
                http.test_auth_payloads={'/api/accounts/oauth/token':h.http.tokens,
                    '/.well-known/jwks.json':{'keys':['SYNTHETIC_NOT_A_JWK']}}
                h.session.http=http;h.session.lifecycle_lock=http.lock;gateway=RealGateway(settings)
                host=self.host(gateway);epoch=h.session.epoch
                ad,ar=workers.start(lambda:run('auth',lambda:h.callback(params)))
                self.wait(gates['auth'].entered)
                done,outcome=workers.start(lambda:run('host',lambda:gateway._generate('dm',[],None)))
                self.wait(gates['host'].entered)
                stopped,control=workers.start(host._stop);self.success(stopped,control)
                self.assertEqual(h.session.epoch,epoch);self.assertFalse(ad.is_set())
                self.assertFalse(connections['auth'][0].sockets[0].closed)
                gates['host'].release.set();self.cancelled(done,outcome)
                self.assertFalse(ad.is_set());self.assertFalse(gates['auth'].release.is_set())
                gates['auth'].release.set();reply=self.success(ad,ar)
                self.assertEqual(reply['state'],'authorized_not_verified')
                self.assertEqual(h.session.binding(),key)
                with h.session.access(key,RequestBudget(None,30)) as token:
                    self.assertEqual(token,h.http.tokens['access_token'])
                self.assertFalse(h.session.closed)

    def test_default_oauth_cancel_does_not_cancel_gateway_owner_or_logout(self):
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);h.authorize();settings=self.settings(h)
            before_begin=h.session.store.path.read_bytes();vault=dict(h.vault.data)
            with self.lanes() as (http,gates,connections,workers,run):
                h.session.http=http;h.session.lifecycle_lock=http.lock;gateway=RealGateway(settings)
                auth_done,auth_result=workers.start(lambda:run('auth',lambda:h.session.begin(CALLBACK)))
                self.wait(gates['auth'].entered)
                before=self.installed_owner_baseline(h,before_begin,vault)
                done,outcome=workers.start(lambda:run('host',lambda:gateway._generate('dm',[],None)))
                self.wait(gates['host'].entered)
                stopped,control=workers.start(lambda:http.cancel_owner(None));self.success(stopped,control)
                self.assertFalse(done.is_set());self.assertFalse(auth_done.is_set())
                host_latches=[latch for latch in http.active if latch.scope is gateway._responses_owner]
                self.assertEqual(len(host_latches),1);self.assertFalse(host_latches[0].cancelled)
                gates['auth'].release.set();self.cancelled(auth_done,auth_result)
                gates['host'].release.set();self.success(done,outcome)
                self.assertEqual(connections['host'][0].sent,[b'SYNTHETIC_WIRE'])
                self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)

    def test_session_cancel_is_not_persistent_logout(self):
        # Session epoch may reject an in-flight credential consumer. HTTP scope
        # isolation does not exempt account identity/rotation guards.
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);key=h.authorize();before_begin=h.session.store.path.read_bytes();vault=dict(h.vault.data)
            with self.lanes() as (http,gates,connections,workers,run):
                h.session.http=http;h.session.lifecycle_lock=http.lock
                done,outcome=workers.start(lambda:run('auth',lambda:h.session.begin(CALLBACK)))
                self.wait(gates['auth'].entered)
                before=self.installed_owner_baseline(h,before_begin,vault)
                stopped,control=workers.start(h.session.cancel);self.success(stopped,control)
                self.assertFalse(h.session.closed);self.assertIsNone(h.session.pending)
                gates['auth'].release.set();self.cancelled(done,outcome)
                self.assertEqual(h.session.store.path.read_bytes(),before);self.assertEqual(h.vault.data,vault)
                self.assertEqual(h.session.binding(),key)

                with h.session.access(key,RequestBudget(None,2)) as token:
                    self.assertEqual(token,h.http.tokens['access_token'])

    def test_two_gateway_owners_close_only_the_selected_gateway(self):
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);h.authorize();settings=self.settings(h)
            with self.lanes() as (http,gates,connections,workers,run):
                h.session.http=http;h.session.lifecycle_lock=http.lock
                a=RealGateway(settings);b=RealGateway(settings)
                self.assertIsNot(a._responses_owner,b._responses_owner)
                # Raw second-owner request avoids serializing on the real store
                # while still using the actual Gateway-created owner capability.
                da,ra=workers.start(lambda:self.direct(http,a._responses_owner,'other',run))
                self.wait(gates['other'].entered)
                db,rb=workers.start(lambda:self.direct(http,b._responses_owner,'auth',run))
                self.wait(gates['auth'].entered)
                stopped,control=workers.start(a.close);self.success(stopped,control)
                gates['other'].release.set();self.cancelled(da,ra)
                self.assertFalse(db.is_set());self.assertFalse(connections['auth'][0].sockets[0].closed)
                gates['auth'].release.set();self.success(db,rb)

    def test_nested_owner_guard_restore_explicit_none_and_thread_local(self):
        http=PublicHTTP();a=http.new_owner();b=http.new_owner();seen=[];workers=Workers(self)
        with http.operation(lambda:seen.append('outer'),owner=a):
            outer=http.local.check
            with http.operation(lambda:seen.append('inner')):
                self.assertIs(http.local.owner,a);http._check(RequestBudget(None,30))
            self.assertIs(http.local.check,outer)
            with self.assertRaisesRegex(RuntimeError,'synthetic'):
                with http.operation(None,owner=b):
                    self.assertIs(http.local.owner,b);raise RuntimeError('synthetic')
            self.assertIs(http.local.owner,a);self.assertIs(http.local.check,outer)
            with http.operation(None,owner=None):self.assertIs(http.local.owner,http._default_owner)
            self.assertIs(http.local.owner,a)
            def other():
                with http.operation(None):return http.local.owner
            try:
                done,outcome=workers.start(other)
                self.assertIs(self.success(done,outcome),http._default_owner)
            finally:workers.join()
        self.assertIsNone(http.local.check);self.assertIsNone(http.local.owner)
        self.assertEqual(seen,['outer','inner'])

    def test_cancelled_outer_operation_never_recaptures_scope_for_later_request(self):
        http=PublicHTTP();owner=http.new_owner();budget=RequestBudget(None,30)
        with http.operation(None,owner=owner):
            http.cancel_owner(owner)
            with patch('app.siwc_responses.ssl.SSLContext',side_effect=AssertionError('no native entry')):
                with self.assertRaisesRegex(ProviderFault,'CANCEL'):http.request(API+'/models','GET',None,{},budget)
            with http.operation(None):
                with self.assertRaisesRegex(ProviderFault,'CANCEL'):http._check(budget)
        # A separate explicit operation is a new baseline, not resurrection.
        with http.operation(None,owner=owner):http._check(budget)

    def test_foreign_and_noncapability_owner_rejected_without_state_change(self):
        http=PublicHTTP();foreign=PublicHTTP().new_owner();owner=http.new_owner()
        generation=http.generation
        with http.operation(None,owner=owner):
            guard=http.local.check
            for value in (foreign,object(),0,'owner'):
                with self.subTest(value=type(value).__name__):
                    with self.assertRaisesRegex(ProviderFault,'OWNER'):http.cancel_owner(value)
                    with self.assertRaisesRegex(ProviderFault,'OWNER'):
                        with http.operation(None,owner=value):self.fail('foreign entry')
                    self.assertIs(http.local.check,guard);self.assertIs(http.local.owner,owner)
            http._check(RequestBudget(None,30))
        self.assertEqual(http.generation,generation)

    def test_owner_tokens_not_retained_by_history_registry(self):
        http=PublicHTTP();owner=http.new_owner();reference=weakref.ref(owner)
        with http.operation(None,owner=owner):pass
        http.cancel_owner(owner);del owner;gc.collect()
        self.assertIsNone(reference());self.assertEqual(http.active,set())

    def test_scope_cancel_before_request_registration_lock_fences_entry(self):
        with self.lanes() as (http,gates,connections,workers,run):
            owner=http.new_owner();entered=threading.Event();release=threading.Event()
            # urlsplit is after generation capture but before registration lock.
            from urllib.parse import urlsplit
            def delayed(url):
                entered.set()
                if not release.wait(TEST_WAIT*3):raise AssertionError('entry release')
                return urlsplit(url)
            try:
                with patch('urllib.parse.urlsplit',side_effect=delayed):
                    done,outcome=workers.start(lambda:self.direct(http,owner,'other',run))
                    self.wait(entered);http.cancel_owner(owner);release.set();self.cancelled(done,outcome)
                self.assertFalse(connections['other'])
            finally:release.set()

    def test_all_native_late_boundaries_keep_scope_latch_and_no_reconnect(self):
        for stage in ('constructor','dns','tls','connect','request'):
            with self.subTest(stage=stage),self.lanes(stage) as (http,gates,connections,workers,run):
                owner=http.new_owner();budget=RequestBudget(None,30);deadline=budget.deadline
                done,outcome=workers.start(lambda:self.direct(http,owner,'host',run,budget))
                self.wait(gates['host'].entered)
                stopped,control=workers.start(lambda:http.cancel_owner(owner));self.success(stopped,control)
                self.assertFalse(done.is_set());self.assertFalse(gates['host'].release.is_set())
                gates['host'].release.set();self.cancelled(done,outcome)
                self.assertFalse(connections['host'][0].sent)
                self.assertEqual((budget.deadline,budget.requests),(deadline,0))
                self.assertEqual(connections['host'][0].auto_open,0 if stage!='constructor' else 1)

    def test_already_admitted_send_is_not_reported_unsent(self):
        with self.lanes('sent') as (http,gates,connections,workers,run):
            owner=http.new_owner();done,outcome=workers.start(lambda:self.direct(http,owner,'host',run))
            self.wait(gates['host'].entered);self.assertEqual(connections['host'][0].sent,[b'SYNTHETIC_WIRE'])
            http.cancel_owner(owner);gates['host'].release.set();self.cancelled(done,outcome)
            self.assertEqual(connections['host'][0].sent,[b'SYNTHETIC_WIRE'])

    def test_global_close_revokes_all_registered_and_pre_request_operation_scopes(self):
        with self.lanes() as (http,gates,connections,workers,run):
            owner=http.new_owner();waiting=http.new_owner()
            da,ra=workers.start(lambda:self.direct(http,None,'auth',run));self.wait(gates['auth'].entered)
            db,rb=workers.start(lambda:self.direct(http,owner,'other',run));self.wait(gates['other'].entered)
            with http.operation(None,owner=waiting):
                stopped,control=workers.start(http.close);self.success(stopped,control)
                with self.assertRaisesRegex(ProviderFault,'CANCEL'):http._check(RequestBudget(None,30))
            for lane in ('auth','other'):gates[lane].release.set()
            self.cancelled(da,ra);self.cancelled(db,rb)
            with http.operation(None,owner=waiting):http._check(RequestBudget(None,30))

    def test_cancel_socket_cleanup_does_not_hold_control_lock(self):
        with self.lanes() as (http,gates,connections,workers,run):
            owner=http.new_owner();done,outcome=workers.start(lambda:self.direct(http,owner,'other',run))
            self.wait(gates['other'].entered);cleanup=Gate();sock=connections['other'][0].sock
            original=sock.shutdown
            def shutdown(how):cleanup.wait();original(how)
            sock.shutdown=shutdown
            try:
                stopped,control=workers.start(lambda:http.cancel_owner(owner));self.wait(cleanup.entered)
                def lock_probe():
                    with http.lock:return http.new_owner()
                lock_done,lock_result=workers.start(lock_probe);self.success(lock_done,lock_result)
                self.assertFalse(stopped.is_set())
            finally:cleanup.release.set()
            self.success(stopped,control);gates['other'].release.set();self.cancelled(done,outcome)

    def test_synthetic_transport_compatibility_never_closes_shared_http_or_session(self):
        # Fake-only compatibility, NOT evidence of real PublicHTTP isolation.
        def forbidden():raise AssertionError('shared close/account cancel forbidden')
        fake=SimpleNamespace(close=forbidden,responses=lambda *a:None)
        session=SimpleNamespace(http=fake,cancel=forbidden)
        chat=SimpleNamespace(close=lambda:closed.append('chat'));closed=[]
        gateway=RealGateway(SimpleNamespace(siwc=session),transport=chat)
        gateway.close();self.assertEqual(closed,['chat'])
        with gateway._response_operation():pass

    def test_gateway_final_result_fence_rejects_close_after_response_cleanup(self):
        http=PublicHTTP();gateway=RealGateway(SimpleNamespace(siwc=SimpleNamespace(http=http)))
        def returned(*args):
            gateway.close()
            return ('synthetic',{'requests':1})
        with patch.object(gateway,'_generate_owned',side_effect=returned):
            with self.assertRaisesRegex(ProviderFault,'CANCEL'):gateway._generate('dm',[],None)
        self.assertIsNone(http.local.check);self.assertIsNone(http.local.owner)

    def test_cancel_keeps_original_attempt_budget_and_explicit_new_attempt_only(self):
        from app.actor_execution import Attempt, ExecutionPolicy
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);h.authorize();settings=self.settings(h)
            with self.lanes() as (http,gates,connections,workers,run):
                h.session.http=http;h.session.lifecycle_lock=http.lock;gateway=RealGateway(settings)
                attempt=Attempt(object(),ExecutionPolicy());budget=gateway._budget('dm',settings.snapshot()['dm'],attempt)
                deadline=budget.deadline
                done,outcome=workers.start(lambda:run('host',lambda:gateway._generate('dm',[],attempt)))
                self.wait(gates['host'].entered);gateway.close();gates['host'].release.set();self.cancelled(done,outcome)
                self.assertIs(gateway._budget('dm',settings.snapshot()['dm'],attempt),budget)
                with self.assertRaisesRegex(ProviderFault,'REQUEST_LIMIT'):
                    run('host',lambda:gateway._generate('dm',[],attempt))
                self.assertEqual(len(connections['host']),1)
                self.assertEqual((budget.deadline,budget.requests),(deadline,1))
                fresh=Attempt(object(),ExecutionPolicy())
                text,usage=run('host',lambda:gateway._generate('dm',[],fresh))
                self.assertEqual(json.loads(text),{'synthetic':'owned response'})
                self.assertEqual(usage['requests'],1);self.assertEqual(len(connections['host']),2)
                self.assertIs(gateway.budgets[(fresh,'dm')].attempt,fresh)
                self.assertEqual((budget.deadline,budget.requests),(deadline,1))

    def test_real_attempt_cleanup_pending_while_oauth_remains_suspended(self):
        from app.actor_execution import ExecutionPolicy, ActorRunError
        class Authority:
            def _scoped_coordinator(self,value):return value
            def _scoped_entry_budget(self):return None
            def _scoped_attempt(self):return None
            def _scoped_coordinator_attempt(self):return None
            def _entry_budget(self,entry):return nullcontext()
            def _actor_attempt(self,attempt):return nullcontext()
            def acquire_actor_execution(self,*args,**kwargs):return object()
            def release_actor_execution(self,permit):raise AssertionError('no release before provider join')
        with tempfile.TemporaryDirectory() as directory:
            h=Harness(directory);h.authorize();settings=self.settings(h)
            with self.lanes() as (http,gates,connections,workers,run):
                h.session.http=http;h.session.lifecycle_lock=http.lock;gateway=RealGateway(settings)
                ad,ar=workers.start(lambda:run('auth',lambda:h.session.begin(CALLBACK)));self.wait(gates['auth'].entered)
                pending=h.session.pending;epoch=h.session.epoch;host=self.host(gateway);authority=Authority()
                host._authority=authority;host._owner_instance='synthetic-1005'
                host._lock=threading.RLock();host._local=threading.local();host._closed=False
                host._closing=threading.Event();host._attempt=None;host._pending_threads=[]
                host._lease_cleanups=[];host._lease_cleanup_pending=False;host.cleanup_pending=False
                host.execution_policy=ExecutionPolicy();attempts=[];budgets=[]
                def domain(*args,**kwargs):
                    attempt=host._attempt;attempts.append(attempt)
                    budget=gateway._budget('dm',settings.snapshot()['dm'],attempt);budgets.append((budget,budget.deadline))
                    return attempt.call(host,lambda:run('host',lambda:gateway._generate('dm',[],attempt)))
                host._act=domain;done,outcome=workers.start(lambda:host.act('synthetic-person'))
                try:
                    self.wait(gates['host'].entered);attempt=attempts[0]
                    stopped,control=workers.start(lambda:attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED')))
                    self.success(stopped,control);self.wait(done)
                    self.assertFalse(outcome[0][0]);self.assertEqual(outcome[0][1].code,'ACTOR_EXECUTION_CANCELLED')
                    self.assertTrue(host.cleanup_pending);self.assertTrue(host._lease_cleanup_pending)
                    self.assertEqual(host._lease_cleanups,[(authority,attempt.permit,attempt)])
                    self.assertEqual(host._pending_threads,attempt.calls)
                    self.assertTrue(any(t.is_alive() for t in attempt.calls));self.assertFalse(attempt.thread.is_alive())
                    self.assertIs(budgets[0][0].attempt,attempt)
                    self.assertEqual((budgets[0][0].deadline,budgets[0][0].requests),(budgets[0][1],1))
                    self.assertIs(h.session.pending,pending);self.assertEqual(h.session.epoch,epoch)
                    self.assertFalse(ad.is_set());self.assertFalse(gates['auth'].release.is_set())
                finally:
                    gates['host'].release.set();gates['auth'].release.set()
                    if host._attempt is not None:host._attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'))
                    for attempt in attempts:
                        for thread in attempt.calls:thread.join(TEST_WAIT)
                        if attempt.thread is not None:attempt.thread.join(TEST_WAIT)
                        self.assertFalse(any(t.is_alive() for t in attempt.calls))
                self.success(ad,ar);self.assertIsNotNone(h.session.pending)


if __name__=='__main__':unittest.main()
