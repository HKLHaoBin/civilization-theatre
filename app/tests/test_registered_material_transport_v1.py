"""T1/T2 actual registered windows; script only model seams, native dual SQL."""
import copy
import json
import unittest
from app.tests import test_registered_production_runtime_v1 as runtime
from app.world import DAY
from app.adjudication import Adjudicator

PID = runtime.PID
TEXT = ' 原话：请将现有铁料三单位调往本国另一聚落，出仓后据实登记在途。\n'


def shipment(to='place-1', resource='iron', quantity=3):
    return dict(action='ship_material', to=to, resource_id=resource, quantity=quantity)


class RegisteredMaterialTransportTests(unittest.TestCase):
    def setUp(self):
        self.fixture = runtime.RegisteredProductionRuntimeTests('runTest')
        self.fixture.setUp()
        self.root = self.fixture.root
        self.views = []
        self.requests = []

    def tearDown(self):
        self.fixture.save('T-views', self.views)
        self.fixture.save('T-requests', self.requests)
        self.fixture.tearDown()

    def build(self, effects=None, principal=PID, provider_error=None, c=None):
        f = self.fixture
        f.build(c=c, effects=[shipment()] if effects is None else effects,
                principal=principal, provider_error=provider_error)
        turns = 0
        def choose(view, seen):
            nonlocal turns
            f.counts['choose'] += 1
            self.views.append(copy.deepcopy(view))
            body = dict(action='wait')
            if view['person']['id'] == principal:
                turns += 1
                if turns == 1:
                    body = dict(action='propose', text=TEXT)
            return body, seen
        request = f.host._request
        def record_request(payload):
            self.requests.append(copy.deepcopy(payload))
            return request(payload)
        f.host._choose = choose
        f.host._request = record_request

    def window(self):
        f = self.fixture
        f.dispatch()
        self.assertIsNone(f.owner.error, 'registered material transfer must commit; actual='+str(f.owner.error))
        return f.world.snapshot()

    def assert_dispatch(self):
        f = self.fixture
        state = self.window()
        row = f.activation()
        public = runtime.read_sql(f.world.db_path)
        private = runtime.read_sql(f.host.db_path)
        self.assertEqual((row['phase'], row['private_closed']), ('completed', 1))
        self.assertTrue(all(r['phase']=='completed' and r['private_closed']==1 for r in public['coordinator_activations']))
        self.assertEqual(next(r['text'] for r in public['dm_proposals'] if r['intent_id']==row['intent_id']), TEXT)
        child = Adjudicator._child_id(row['intent_id'], 1, 0)
        transfer = state['economy']['transport']['transfers'][0]
        self.assertEqual((transfer['id'], transfer['cause_id'], transfer['owner_id'], transfer['status']),
                         ('material-shipment-'+child, child, PID, 'pending'))
        self.assertEqual((transfer['from'], transfer['to'], transfer['resource_id'], transfer['quantity']),
                         ('place-0', 'place-1', 'iron', 3))
        self.assertEqual(state['economy']['materials']['place-0']['iron'], 17)
        self.assertEqual(state['economy']['materials']['place-1']['iron'], 20)
        self.assertEqual(transfer['departed_at'], DAY)
        with f.world._connection(readonly=True) as db:
            raw = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            f.world._restore_geography(db, raw)
        self.assertEqual(transfer['due'], DAY+f.world._route_seconds(raw, 'place-0', 'place-1'))
        self.assertGreater(transfer['due'], DAY)
        nodes = [q for q in state['queue'] if q['kind']=='material_shipment']
        self.assertEqual(nodes, [transfer['node']])
        self.assertEqual(state['in_transit'], [])
        self.assertEqual(state['economy']['tasks'], [])
        for book in ('produced', 'consumed', 'lost'):
            self.assertTrue(all(n==0 for row in state['economy']['material_ledger'][book].values() for n in row.values()))
        self.assertEqual(f.world._production_accounts(raw), raw['economy'])
        parent = f.world.receipt(PID, row['intent_id'])
        self.assertEqual(f.world.receipt(PID, child)['effect'], parent['effect']['effects'][0])
        meta = next(r for r in private['actor_intent_execution'] if r['intent_id']==row['intent_id'])
        head = next(r for r in private['actor_session_heads'] if r['person_id']==PID)
        self.assertEqual(meta['promoted_version'], meta['base_version']+1)
        self.assertEqual(head['version'], meta['promoted_version'])
        self.assertEqual(json.loads(next(r['request'] for r in private['actor_intents'] if r['intent_id']==row['intent_id']))['body'],
                         dict(action='propose', text=TEXT))
        notices = [o for o in f.world.observe(PID)['observations'] if 'iron' in o['text'] and '在途' in o['text']]
        self.assertEqual(len(notices), 1)
        self.assertIn('iron_unit', notices[0]['text'])
        self.assertEqual(f.replay()['receipt'], parent)
        f.save('T1-asserted', dict(state=state, transfer=transfer, parent=parent, public=public, private=private))
        return state, transfer

    def test_registered_propose_material_dispatch_debits_source_only(self):
        self.build()
        self.assert_dispatch()

    def test_registered_material_arrival_settles_once_and_is_locally_known(self):
        self.build()
        f = self.fixture
        first, transfer = self.assert_dispatch()
        marshal = next(p for p in first['persons'] if p['id']=='person-country-1-marshal')
        self.assertEqual((marshal['country_id'], marshal['location']), ('country-1', transfer['to']))
        final = first
        for _ in range(4):
            if final['time'] >= transfer['due']:break
            final = self.window()
        f.save('T2-before-decisive-assertion', dict(first=first, final=final, transfer=transfer,
               views=self.views, requests=self.requests, public=runtime.read_sql(f.world.db_path),private=runtime.read_sql(f.host.db_path)))
        self.assertGreaterEqual(final['time'], transfer['due'])
        self.assertEqual(final['economy']['materials'][transfer['to']]['iron'], 23,
                         'MISSING_ACTUAL_MATERIAL_ARRIVAL: original due clock popped node but destination must receive')
        arrived = final['economy']['transport']['transfers'][0]
        self.assertEqual((arrived['status'], arrived['arrived_at']), ('arrived', transfer['due']))
        self.assertEqual(final['economy']['materials'][transfer['from']]['iron'], 17)
        self.assertFalse(any(q['kind']=='material_shipment' for q in final['queue']))
        self.assertEqual(len([e for e in final['events'] if e['kind']=='material_shipment_arrived']), 1)
        self.assertEqual(f.counts['dm'], 1)
        receiver = f.world.observe(marshal['id'])
        notices = [o for o in receiver['observations'] if o['time']==transfer['due'] and 'iron_unit' in o['text'] and '实际到达' in o['text']]
        self.assertEqual(len(notices), 1)
        notice = notices[0]
        place = next(p for p in final['places'] if p['id']==transfer['to'])
        self.assertEqual(notice['source'], '亲见：'+place['name'])
        self.assertIn('3', notice['text'])
        for pid in (PID, 'person-country-1-king'):
            self.assertFalse(any('iron_unit' in o['text'] and '实际到达' in o['text'] for o in f.world.observe(pid)['observations']))
        request = [r for r in self.requests if r['person_id']==marshal['id']][-1]
        self.assertIn(notice['text'], request['prompt'])
        private = runtime.read_sql(f.host.db_path)
        leaf = next(r for r in private['actor_intents'] if r['intent_id']==request['intent_id'])
        self.assertIn(notice['text'], json.loads(leaf['request'])['prompt'])
        self.assertIn(notice['text'], json.dumps(json.loads(leaf['candidate']), ensure_ascii=False))
        for key in ('contact_packet', 'contact_units', 'pathology', 'material_ledger'):
            self.assertNotIn(key, json.dumps(request, ensure_ascii=False))
        trace = f.adjudicator.trace()
        task = next(p for p in trace['proposals'] if p['person_id']==PID)['committed']['tasks'][0]
        self.assertEqual((task['kind'], task['status']), ('material_shipment', 'completed'))
        before = f.world.snapshot()
        self.assertEqual(f.replay()['receipt'], f.world.receipt(PID, f.activation()['intent_id']))
        self.assertEqual(f.world.snapshot(), before)
        f.save('T2-asserted', dict(final=final, notice=notice, request=request, leaf=leaf, trace=trace))

    def assert_rejection(self, code, action='ship_material', index=0):
        f = self.fixture
        f.observe_execution()
        self.window()
        row=f.activation();public=runtime.read_sql(f.world.db_path);private=runtime.read_sql(f.host.db_path)
        self.assertEqual((row['phase'],row['private_closed']),('rejected',1))
        outcome=json.loads(next(r['outcome_json'] for r in public['coordinator_activation_outcomes'] if r['intent_id']==row['intent_id']))
        self.assertEqual((outcome['status'],outcome['origin'],outcome['code'],outcome['action'],outcome['effect_index']),
                         ('domain_rejected','adjudication.execute',code,action,index))
        self.assertEqual(outcome['effects'],[])
        self.assertIsNone(row['promoted_version']);self.assertIsNone(row['receipt_sha256'])
        self.assertIsNone(f.world.receipt(f.principal,row['intent_id']))
        leaf=next(r for r in private['actor_intents'] if r['intent_id']==row['intent_id'])
        self.assertEqual(leaf['state'],'rejected');self.assertIsNotNone(leaf['candidate'])
        head=next(r for r in private['actor_session_heads'] if r['person_id']==f.principal)
        self.assertEqual(head['version'],0)
        real=[r for r in f.observations if r['kind']=='execute' and r['persist']]
        self.assertEqual(len(real),1)
        self.assertEqual((real[0]['error_type'],real[0]['code']),('DomainActionError',code))
        state=f.world.snapshot();before=real[0]['before']
        for key in ('economy','queue','in_transit','ledger'):
            self.assertEqual(state[key],before[key])
        for p in state['persons']:
            self.assertEqual(p,next(x for x in before['persons'] if x['id']==p['id']))
        children={Adjudicator._child_id(row['intent_id'],outcome['decision_version'],i) for i in range(len(real[0]['decision']['effects']))}
        self.assertFalse(any(r['id'] in children for r in public['intents']))
        self.assertFalse(any(e.get('cause_id') in children for e in state['events']))
        self.assertEqual(f.replay()['outcome'],outcome)
        f.save('T-rejection-asserted',dict(outcome=outcome,real=real,public=public,private=private))
        return real[0]

    def test_registered_king_material_rejection(self):
        self.build(principal='person-country-1-king')
        self.assert_rejection('NOT_AUTHORIZED')

    def test_registered_marshal_material_rejection(self):
        self.build(principal='person-country-1-marshal')
        self.assert_rejection('NOT_AUTHORIZED')

    def test_registered_foreign_destination_material_rejection(self):
        self.build(effects=[shipment(to='place-10')])
        self.assert_rejection('NOT_AUTHORIZED')

    def test_registered_shortage_original_id_rejection(self):
        self.build(effects=[shipment(quantity=21)])
        self.assert_rejection('INSUFFICIENT_MATERIALS')

    def test_registered_actual_inflight_rejection_rolls_back_tentative_travel(self):
        self.build(effects=[dict(action='travel',to='place-1'),shipment()])
        real=self.assert_rejection('NOT_AT_LOCATION',index=1)
        person=next(p for p in real['after']['persons'] if p['id']==PID)
        self.assertIsNone(person['location'])
        self.assertEqual(len([q for q in real['after']['queue'] if q['kind']=='arrival' and q['person_id']==PID]),1)
        self.assertEqual(sum(r['receipt'] is not None for r in real['child_rows_before_savepoint_exit']),1)

    def test_registered_two_shipments_true_savepoint_rollback(self):
        self.build(effects=[shipment(quantity=12),shipment(quantity=12)])
        real=self.assert_rejection('INSUFFICIENT_MATERIALS',index=1)
        self.assertEqual(real['after']['economy']['materials']['place-0']['iron'],8)
        self.assertEqual(len(real['after']['economy']['transport']['transfers']),1)
        self.assertEqual(sum(r['receipt'] is not None for r in real['child_rows_before_savepoint_exit']),1)
        self.assertTrue(any(e['kind']=='material_shipment_departed' for e in real['after']['events']))

    def test_registered_production_then_shipping_cannot_spend_reserved(self):
        self.build(effects=[runtime.production(),shipment(quantity=19)])
        real=self.assert_rejection('INSUFFICIENT_MATERIALS',index=1)
        self.assertEqual(real['after']['economy']['tasks'][0]['reserved_inputs']['iron'],2)
        self.assertEqual(sum(r['receipt'] is not None for r in real['child_rows_before_savepoint_exit']),1)

    def test_registered_shipping_then_production_cannot_spend_cargo(self):
        self.build(effects=[shipment(quantity=19),runtime.production()])
        real=self.assert_rejection('INSUFFICIENT_MATERIALS',action='start_production',index=1)
        self.assertEqual(real['after']['economy']['materials']['place-0']['iron'],1)
        self.assertEqual(len(real['after']['economy']['transport']['transfers']),1)
        self.assertEqual(sum(r['receipt'] is not None for r in real['child_rows_before_savepoint_exit']),1)

    def provider_technical(self, code):
        from app.world import WorldError
        self.build(provider_error=WorldError(code,'synthetic provider error, not business producer'))
        f=self.fixture;f.dispatch()
        self.assertIsNotNone(f.owner.error)
        row=f.activation();public=runtime.read_sql(f.world.db_path);private=runtime.read_sql(f.host.db_path)
        self.assertEqual(row['phase'],'technical_blocked')
        self.assertFalse(any(r['intent_id']==row['intent_id'] for r in public.get('coordinator_activation_outcomes',[])))
        self.assertNotIn('transport',f.world.snapshot()['economy'])
        self.assertFalse(any(r.get('promoted_version') is not None for r in private['actor_intent_execution'] if r['person_id']==PID))
        f.save('T-provider-technical',dict(code=code,error=f.owner.error,public=public,private=private))

    def test_registered_provider_material_code_stays_technical(self):
        self.provider_technical('INSUFFICIENT_MATERIALS')

    def test_registered_provider_authority_code_stays_technical(self):
        self.provider_technical('NOT_AUTHORIZED')

    def test_registered_provider_location_code_stays_technical(self):
        self.provider_technical('NOT_AT_LOCATION')

    def test_registered_raw_and_forged_ref_cannot_reach_child(self):
        from app.world import WorldError
        from app.coordinator_scheduler import ActivationRef
        self.build();self.assert_dispatch();f=self.fixture
        before=f.world.snapshot();public=runtime.read_sql(f.world.db_path);private=runtime.read_sql(f.host.db_path)
        with f.owner._owner_scope(),self.assertRaises(WorldError) as caught:
            f.world.submit(PID,'raw-material',shipment())
        self.assertEqual(caught.exception.code,'INVALID_BODY')
        row=f.activation()
        bad=ActivationRef(row['world_id'],row['window_id'],'0'*32,row['person_id'])
        with self.assertRaises(WorldError):
            f.host.act(PID,coordinator=f.owner._owner_attempt.permit,activation=bad)
        self.assertEqual(f.world.snapshot(),before)
        self.assertEqual(runtime.read_sql(f.world.db_path),public)
        self.assertEqual(runtime.read_sql(f.host.db_path),private)

    def test_registered_notice_failure_original_clock_rollback(self):
        self.build();first,transfer=self.assert_dispatch();f=self.fixture
        delegate=f.world._notice;attempts=[]
        def broken(state,pid,text,source):
            if 'iron_unit' in text and '实际到达' in text:
                attempts.append(dict(pid=pid,time=state['time'],text=text))
                delegate(state,pid,text,source)
                raise RuntimeError('injected material receiver notice persistence fault')
            return delegate(state,pid,text,source)
        f.world._notice=broken
        try:f.dispatch()
        finally:f.world._notice=delegate
        after=f.world.snapshot()
        self.assertEqual(f.owner.error,'HOST_FAILURE')
        self.assertEqual(len(attempts),1)
        self.assertEqual((attempts[0]['pid'],attempts[0]['time']),('person-country-1-marshal',transfer['due']))
        self.assertEqual(after['time'],first['time'])
        for key in ('economy','queue','in_transit'):
            self.assertEqual(after[key],first[key])
        self.assertFalse(any(e['kind']=='material_shipment_arrived' for e in after['events']))
        self.assertFalse(any('实际到达' in o['text'] and 'iron_unit' in o['text'] for o in f.world.observe('person-country-1-marshal')['observations']))
        f.save('T-clock-rollback',dict(before=first,after=after,attempts=attempts,public=runtime.read_sql(f.world.db_path),private=runtime.read_sql(f.host.db_path)))

    def test_registered_committed_journey_then_next_window_real_inflight_rejection(self):
        self.build();f=self.fixture;turns=0
        def choose(view,seen):
            nonlocal turns
            f.counts['choose']+=1;self.views.append(copy.deepcopy(view))
            if view['person']['id']!=PID:return dict(action='wait'),seen
            turns+=1
            if turns==1:return dict(action='travel',to='place-3'),seen
            return dict(action='propose',text=TEXT),seen
        f.host._choose=choose
        first=self.window()
        person=next(p for p in first['persons'] if p['id']==PID)
        self.assertIsNone(person['location'])
        node=next(q for q in first['queue'] if q['kind']=='arrival' and q['person_id']==PID)
        self.assertGreater(node['at'],2*DAY)
        f.observe_execution();second=self.window()
        self.assertEqual(second['time'],2*DAY)
        self.assertIsNone(next(p for p in second['persons'] if p['id']==PID)['location'])
        public=runtime.read_sql(f.world.db_path);private=runtime.read_sql(f.host.db_path)
        rejected=[r for r in public['coordinator_activations'] if r['person_id']==PID and r['phase']=='rejected']
        self.assertEqual(len(rejected),1);row=rejected[0]
        outcome=json.loads(next(r['outcome_json'] for r in public['coordinator_activation_outcomes'] if r['intent_id']==row['intent_id']))
        self.assertEqual((outcome['code'],outcome['action']),('NOT_AT_LOCATION','ship_material'))
        self.assertEqual(row['private_closed'],1);self.assertIsNone(row['promoted_version'])
        self.assertNotIn('transport',second['economy'])
        head=next(r for r in private['actor_session_heads'] if r['person_id']==PID)
        self.assertEqual(head['version'],1)
        self.assertTrue(any(r['kind']=='action_location' and r.get('code')=='NOT_AT_LOCATION' and r['person']['location'] is None for r in f.observations))
        self.assertEqual([q for q in second['queue'] if q['kind']=='arrival' and q['person_id']==PID],[node])
        f.save('T-committed-journey-rejection',dict(first=first,second=second,node=node,outcome=outcome,public=public,private=private))

    def test_registered_same_physical_dual_db_reopen_replay_next_window_once(self):
        from pathlib import Path
        from app.world import WorldStore
        from app.actors import ActorHost
        from app.server import Coordinator
        self.build();first,transfer=self.assert_dispatch();f=self.fixture
        self.window();arrived=f.world.snapshot()
        self.assertEqual(arrived['economy']['transport']['transfers'][0]['status'],'arrived')
        original=f.activation();receipt=f.world.receipt(PID,original['intent_id'])
        private=runtime.read_sql(f.host.db_path);counts=dict(f.counts)
        identities={str(p):Path(p).stat().st_ino for p in (f.world.db_path,f.host.db_path)}
        choose,request,dm=f.host._choose,f.host._request,f.host.dm_provider
        f.owner.close();self.assertFalse(f.owner.cleanup_pending);self.assertFalse(f.host.cleanup_pending)
        f.world=WorldStore(f.root/'public.sqlite',f.root/'scenario.json')
        f.host=ActorHost(f.world,f.root/'private.sqlite')
        def forbidden_worker():raise AssertionError('SDK forbidden')
        f.host._start_worker=forbidden_worker
        f.host._choose,f.host._request=choose,request
        f.owner=Coordinator(f.world,f.host);f.owner.acquire_owner()
        with f.owner._owner_scope():f.adjudicator=Adjudicator(f.world)
        f.host.adjudicator,f.host.dm_provider=f.adjudicator,dm
        f.owner._prepare_scheduler(f.owner._owner_attempt)
        self.assertEqual({str(p):Path(p).stat().st_ino for p in (f.world.db_path,f.host.db_path)},identities)
        self.assertEqual(f.replay()['receipt'],receipt);self.assertEqual(f.counts,counts)
        self.assertEqual(f.world.snapshot(),arrived)
        self.assertEqual(runtime.read_sql(f.host.db_path)['actor_session_heads'],private['actor_session_heads'])
        final=self.window()
        self.assertEqual(final['economy'],arrived['economy'])
        self.assertEqual(len([e for e in final['events'] if e['kind']=='material_shipment_arrived']),1)
        self.assertEqual(f.counts['dm'],1)
        f.save('T-object-reopen-asserted',dict(original=original,receipt=receipt,identities=identities,public=runtime.read_sql(f.world.db_path),private=runtime.read_sql(f.host.db_path)))
