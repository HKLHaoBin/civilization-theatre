"""Unregistered rule boundaries: actual SQLite/clock, never registered advance."""
import copy
import json
import tempfile
import unittest
from pathlib import Path
from app.tests import test_registered_production_runtime_v1 as runtime
from app.world import WorldStore, WorldError, DomainActionError, DAY
from app.adjudication import Adjudicator
from app.tests.test_registered_material_transport_v1 import shipment

PID = runtime.PID
SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class MaterialTransportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.serial = 0

    def own(self, config=None):
        self.serial += 1
        path = self.root/(str(self.serial)+'.json')
        path.write_text(json.dumps(runtime.configuration() if config is None else config, ensure_ascii=False),encoding='utf-8')
        w = WorldStore(path.with_suffix('.sqlite'), path)
        initial = w.initialize()
        w.control('resume')
        return w, Adjudicator(w), initial

    def raw(self, w):
        with w._connection(readonly=True) as db:
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            w._restore_geography(db, state)
        return state

    def prepare(self, w, a, effects=None, pid=PID, ruling='成功'):
        intent = 'material-rule-'+str(self.serial)+'-'+str(len(a.trace()['proposals']))
        a.propose(pid, intent, ' 原话：按真实库存调运。\n')
        state = w.snapshot()
        version = a.record(pid,intent,dict(world_id=state['world_id'],base_revision=state['revision'],
            ruling=ruling,reason='本地真实条件',goal='运送',costs=[],benefits=[],effects=[shipment()] if effects is None else effects))
        return intent,version

    def commit(self, w, a, effects=None, pid=PID, ruling='成功'):
        i,v = self.prepare(w,a,effects,pid,ruling)
        return a.commit(pid,i,v)

    def assert_accounts(self, w):
        state = self.raw(w)
        before = copy.deepcopy(state)
        w._production_accounts(state)
        self.assertEqual(state,before, 'accounts proof must be read-only')
        e=state['economy'];l=e['material_ledger']
        for rid in set(e['units'])-{'grain'}:
            pending=sum(t['quantity'] for t in e.get('transport',{}).get('transfers',[]) if t['status']=='pending' and t['resource_id']==rid)
            held=sum(t['reserved_inputs'].get(rid,0) for t in e['tasks'] if t['status']=='pending')
            self.assertEqual(sum(l['initial'][p][rid]+l['produced'][p][rid]-l['consumed'][p][rid]-l['lost'][p][rid] for p in e['materials']),
                             sum(m[rid] for m in e['materials'].values())+held+pending)
        return state

    def test_strict_body_raw_legacy_and_lazy_initial_shape(self):
        w,a,initial=self.own()
        self.assertNotIn('transport',initial['economy'])
        before=runtime.read_sql(w.db_path)
        self.assert_accounts(w)
        self.assertNotIn('transport',w.snapshot()['economy'])
        self.assertEqual(WorldStore(w.db_path).initialize(),w.snapshot())
        self.assertEqual(runtime.read_sql(w.db_path),before)
        bad=[dict(shipment(),quantity=x) for x in (True,False,0,-1,1.0,2**63,None,'3')]
        bad += [dict(shipment(),resource_id=x) for x in ('grain','',None,'IRON')]
        bad += [dict(shipment(),extra=1),{k:v for k,v in shipment().items() if k!='to'}]
        for body in bad:
            with self.subTest(body=body),self.assertRaises(WorldError):w._material_transport_body(body)
        for method in (w.submit,w.preview):
            with self.assertRaises(WorldError) as caught:method(PID,'raw-material',shipment())
            self.assertEqual(caught.exception.code,'INVALID_BODY')
        self.assertEqual(runtime.read_sql(w.db_path),before)
        for ruling in ('大失败','失败','成功','大成功'):
            ww,aa,_=self.own()
            self.commit(ww,aa,ruling=ruling)
            self.assertEqual('transport' in ww.snapshot()['economy'],ruling in ('成功','大成功'))
            self.assert_accounts(ww)
        legacy=json.loads((runtime.DATA/'scenarios/iron-4.json').read_bytes())
        ww,aa,s0=self.own(legacy)
        i,v=self.prepare(ww,aa)
        with self.assertRaises(WorldError) as caught:aa.preview(PID,i,v)
        self.assertEqual(caught.exception.code,'UNSUPPORTED')
        self.assertNotIn('economy',ww.snapshot())
        self.assertNotIn('economy',WorldStore(ww.db_path).initialize())

    def test_technical_targets_unknown_resource_null_and_no_route(self):
        w,a,_=self.own()
        for body in (shipment(to='place-0'),shipment(to='not-a-place'),shipment(resource='unobtainium')):
            with self.subTest(body=body):
                i,v=self.prepare(w,a,[body]);before=runtime.read_sql(w.db_path)
                with self.assertRaises(WorldError) as caught:a.preview(PID,i,v)
                self.assertNotIsInstance(caught.exception,DomainActionError)
                self.assertEqual(runtime.read_sql(w.db_path),before)
        for value in (None,{},dict(version='bad',transfers=[]),dict(version='p08-material-transport-v1',transfers=None)):
            raw=self.raw(w);raw['economy']['transport']=value
            with self.subTest(transport=value),self.assertRaises(WorldError):w._production_accounts(raw)
        raw=self.raw(w)
        for node in raw['_geography'].values():node['neighbors']=[]
        before=copy.deepcopy(raw)
        with self.assertRaises(WorldError) as caught:w._ship_material(raw,next(p for p in raw['persons'] if p['id']==PID),'no-route',shipment(),True)
        self.assertEqual(caught.exception.code,'NO_ROUTE');self.assertEqual(raw,before)

    def test_material_production_reserved_compete_both_directions(self):
        for effects in ([runtime.production(),shipment(quantity=19)],[shipment(quantity=19),runtime.production()]):
            w,a,_=self.own();i,v=self.prepare(w,a,effects);before=runtime.read_sql(w.db_path)
            with self.assertRaises(DomainActionError) as caught:a.commit(PID,i,v)
            self.assertEqual(caught.exception.code,'INSUFFICIENT_MATERIALS')
            self.assertEqual(runtime.read_sql(w.db_path),before)
        w,a,_=self.own()
        self.commit(w,a,[runtime.production()])
        self.assertEqual(w.snapshot()['economy']['tasks'][0]['reserved_inputs']['iron'],2)
        i,v=self.prepare(w,a,[shipment(quantity=19)]);before=runtime.read_sql(w.db_path)
        with self.assertRaises(DomainActionError):a.commit(PID,i,v)
        self.assertEqual(runtime.read_sql(w.db_path),before);self.assert_accounts(w)

    def test_actual_produced_output_transport_and_destination_production(self):
        c=runtime.configuration()
        for values in c['economy']['initial_materials'].values():values['iron_tool']=0
        c['economy']['recipes']['reuse_tool']=dict(inputs=dict(iron_tool=1),outputs=dict(iron=1),labor_hours=8,duration_seconds=DAY)
        w,a,_=self.own(c)
        self.commit(w,a,[runtime.production()])
        w.advance(DAY)
        original=copy.deepcopy(w.snapshot()['economy']['tasks'][0])
        self.commit(w,a,[shipment(resource='iron_tool',quantity=1)])
        pending=w.snapshot()['economy']['transport']['transfers'][0]
        self.assertEqual(w.snapshot()['economy']['materials']['place-0']['iron_tool'],0)
        self.assertEqual(w.snapshot()['economy']['materials']['place-1']['iron_tool'],0)
        self.assert_accounts(w)
        w.advance(pending['due']);self.assert_accounts(w)
        trip=w.submit(PID,'finance-to-actual-receiver',dict(action='travel',to='place-1'))
        w.advance(trip['effect']['due'])
        self.commit(w,a,[dict(action='start_production',recipe_id='reuse_tool',quantity=1)])
        due=w.snapshot()['economy']['tasks'][-1]['due'];w.advance(due)
        final=self.assert_accounts(w)
        self.assertEqual(final['economy']['tasks'][0],original)
        self.assertEqual(final['economy']['tasks'][-1]['status'],'completed')
        self.assertEqual(final['economy']['material_ledger']['produced']['place-0']['iron_tool'],1)
        self.assertEqual(final['economy']['material_ledger']['consumed']['place-1']['iron_tool'],1)
        self.assertTrue(all(n==0 for row in final['economy']['material_ledger']['lost'].values() for n in row.values()))

    def test_original_due_full_node_retries_and_tampering_fail_closed(self):
        w,a,_=self.own();self.commit(w,a)
        pending=w.snapshot();t=pending['economy']['transport']['transfers'][0];node=copy.deepcopy(t['node'])
        self.assertEqual(w.advance(t['due']-1)['economy']['materials']['place-1']['iron'],20)
        w.advance(t['due']);self.assert_accounts(w)
        w.advance(t['due']+1)
        before=runtime.read_sql(w.db_path)
        w._mutate(lambda s,db:w._finish_material_transport(s,copy.deepcopy(node)))
        self.assertEqual(runtime.read_sql(w.db_path),before)
        for key,value in dict(id='q-bad',seq=True,at=t['due']+1,cause_id='other',quantity=4,destination='place-0',source='place-1',generation=2,unit='bad',resource_id='timber',transfer_id='bad').items():
            with self.subTest(node_field=key),self.assertRaises(WorldError):
                w._mutate(lambda s,db,k=key,v=value:w._finish_material_transport(s,dict(node,**{k:v})))
            self.assertEqual(runtime.read_sql(w.db_path),before)
        for mutate in (lambda s:s['economy']['transport']['transfers'].append(copy.deepcopy(s['economy']['transport']['transfers'][0])),
                       lambda s:s['economy']['transport']['transfers'][0].update(quantity=4),
                       lambda s:s['economy']['transport']['transfers'][0].update(arrived_at=t['due']+1),
                       lambda s:s['economy']['transport']['transfers'][0].update(cause_id='bad'),
                       lambda s:s['economy']['transport']['transfers'][0].update(generation=True)):
            state=self.raw(w);mutate(state)
            with self.subTest(record=str(mutate)),self.assertRaises(WorldError):w._production_accounts(state)
        self.assertEqual(w.snapshot(pending['revision']),pending)
        self.assertEqual(a.trace(pending['revision'])['proposals'][0]['committed']['tasks'][0]['status'],'pending')
        self.assertEqual(a.trace()['proposals'][0]['committed']['tasks'][0]['status'],'completed')
        self.assertEqual(runtime.read_sql(w.db_path),before)

    def test_pending_node_missing_duplicate_and_corrupt_records(self):
        w,a,_=self.own();self.commit(w,a)
        for field,value in dict(quantity=True,due=1,duration=1,unit='bad',rules_sha256='bad',departure_event_id='bad',owner_id='person-country-1-king',status='arrived').items():
            state=self.raw(w);state['economy']['transport']['transfers'][0][field]=value
            with self.subTest(record_field=field),self.assertRaises(WorldError):w._production_accounts(state)
        for way in ('missing','duplicate','bad-cause'):
            state=self.raw(w);node=next(q for q in state['queue'] if q['kind']=='material_shipment')
            if way=='missing':state['queue'].remove(node)
            elif way=='duplicate':state['queue'].append(copy.deepcopy(node))
            else:node['cause_id']='bad'
            with self.subTest(queue=way),self.assertRaises(WorldError):w._production_accounts(state)
        self.assert_accounts(w)

    def test_notice_and_contact_faults_rollback_entire_original_clock(self):
        for seam in ('_notice','_arrive_contact'):
            w,a,_=self.own();self.commit(w,a);t=w.snapshot()['economy']['transport']['transfers'][0]
            before=runtime.read_sql(w.db_path);delegate=getattr(w,seam);attempts=[]
            def broken(*args):
                attempts.append(1)
                delegate(*args)
                raise RuntimeError('actual arrival fault after native domain mutation')
            setattr(w,seam,broken)
            with self.assertRaisesRegex(RuntimeError,'actual arrival fault'):w.advance(t['due'])
            setattr(w,seam,delegate)
            self.assertEqual(attempts,[1]);self.assertEqual(runtime.read_sql(w.db_path),before)
            w.advance(t['due']);self.assert_accounts(w)

    def contact_world(self, optin=True):
        c=runtime.configuration()
        if optin:
            actual=json.loads((runtime.DATA/'scenarios/contact-v2-iron-4.json').read_bytes())
            c['pathology_runtime']=actual['pathology_runtime'];c['pathology']=actual['pathology']
        w,a,_=self.own(c)
        trip=w.submit(PID,'onsite-symptom-source',dict(action='travel',to='place-1'))
        w.advance(trip['effect']['due']);w.advance(DAY)
        return w,a

    def test_contact_original_ship_symptoms_arrival_once_and_optout(self):
        for optin in (False,True):
            w,a=self.contact_world(optin)
            before=w.snapshot();source=next(p for p in before['places'] if p['id']=='place-1')
            self.assertGreater(sum(c['count'] for c in source['disease']['cohorts'] if c['stage'] in ('symptomatic','severe')),0)
            self.commit(w,a,[shipment(to='place-0')])
            pending=w.snapshot();t=pending['economy']['transport']['transfers'][0]
            if optin:
                packet=next(p for p in pending['contact_packets'] if p['cause_id']==t['cause_id'])
                self.assertEqual((packet['channel'],packet['source_place_id'],packet['destination_place_id']),('ship','place-1','place-0'))
                self.assertEqual(t['contact_packet_id'],packet['id'])
            else:self.assertNotIn('contact_packets',pending)
            self.assertEqual(next(p for p in w.advance(t['due']-1)['places'] if p['id']=='place-0')['disease']['cohorts'],[])
            arrived=w.advance(t['due']);node=t['node'];self.assert_accounts(w)
            if optin:
                apps=[x for x in arrived['contact_applications'] if x['packet_id']==packet['id']]
                self.assertEqual(len(apps),1);self.assertEqual(apps[0]['new_infections'],2)
                self.assertEqual(apps[0]['arrived_at'],t['due'])
            else:self.assertNotIn('contact_applications',arrived)
            rows=runtime.read_sql(w.db_path)
            w._mutate(lambda state,db:w._finish_material_transport(state,node))
            self.assertEqual(runtime.read_sql(w.db_path),rows)
            for secret in ('contact_units','contact_packets','pathology','material_ledger'):
                self.assertNotIn(secret,json.dumps(w.observe(PID),ensure_ascii=False))

    def test_material_marker_refuses_before_any_child(self):
        from contextlib import closing
        import sqlite3
        w,a,_=self.own();i,v=self.prepare(w,a,[dict(action='wait'),shipment()])
        with closing(sqlite3.connect(w.db_path)) as db:
            db.execute('CREATE TABLE coordinator_scheduler_registration (invalid_marker TEXT)');db.commit()
        before=runtime.read_sql(w.db_path);state=w.snapshot();original=copy.deepcopy(state)
        with closing(sqlite3.connect(w.db_path)) as db:
            db.execute('BEGIN IMMEDIATE')
            decision=a.history(PID,i)[-1]['decision']
            with self.assertRaises(WorldError) as caught:a._execute(state,db,PID,i,v,decision,True)
            self.assertEqual(caught.exception.code,'UNSUPPORTED')
            self.assertEqual(state,original)
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0],1)
            db.rollback()
        self.assertEqual(runtime.read_sql(w.db_path),before)

    def test_true_foreign_location_and_foreign_onsite_receiver_privacy(self):
        w,a,_=self.own()
        foreign='person-country-2-king'
        move=w.submit(foreign,'foreign-witness-travel',dict(action='travel',to='place-1'))
        w.advance(move['effect']['due'])
        self.assertEqual(next(p for p in w.snapshot()['persons'] if p['id']==foreign)['location'],'place-1')
        self.commit(w,a);t=w.snapshot()['economy']['transport']['transfers'][0]
        w.advance(t['due'])
        self.assertFalse(any('iron_unit' in o['text'] and '实际到达' in o['text'] for o in w.observe(foreign)['observations']))
        self.assertTrue(any('iron_unit' in o['text'] and '实际到达' in o['text'] for o in w.observe('person-country-1-marshal')['observations']))
        move=w.submit(PID,'finance-foreign-travel',dict(action='travel',to='place-10'))
        w.advance(move['effect']['due'])
        i,v=self.prepare(w,a,[shipment(to='place-1')]);before=runtime.read_sql(w.db_path)
        with self.assertRaises(DomainActionError) as caught:a.commit(PID,i,v)
        self.assertEqual((caught.exception.code,caught.exception.action),('NOT_AUTHORIZED','ship_material'))
        self.assertEqual(runtime.read_sql(w.db_path),before)

    def test_healthy_latent_material_departure_never_creates_packet(self):
        actual=json.loads((runtime.DATA/'scenarios/contact-v2-iron-4.json').read_bytes())
        for latent in (False,True):
            c=runtime.configuration();c['pathology_runtime']=actual['pathology_runtime'];c['pathology']=actual['pathology']
            w,a,_=self.own(c)
            if latent:
                trip=w.submit(PID,'latent-onsite',dict(action='travel',to='place-1'))
                w.advance(trip['effect']['due'])
                self.assertLess(w.snapshot()['time'],DAY)
            self.commit(w,a,[shipment(to='place-0' if latent else 'place-1')])
            t=w.snapshot()['economy']['transport']['transfers'][0]
            self.assertNotIn('contact_packet_id',t);self.assertNotIn('contact_packets',w.snapshot())
            final=w.advance(t['due']);self.assertNotIn('contact_applications',final)
            self.assert_accounts(w)

    def test_material_real_contact_fault_rolls_back_cohort_and_clock(self):
        w,a=self.contact_world();self.commit(w,a,[shipment(to='place-0')])
        t=w.snapshot()['economy']['transport']['transfers'][0]
        self.assertIn('contact_packet_id',t)
        before=runtime.read_sql(w.db_path);delegate=w._arrive_contact;attempts=[]
        def broken(state,node,arrival):
            delegate(state,node,arrival)
            app=next(x for x in state['contact_applications'] if x['packet_id']==t['contact_packet_id'])
            attempts.append(copy.deepcopy(app))
            raise RuntimeError('injected after real material contact application')
        w._arrive_contact=broken
        try:
            with self.assertRaisesRegex(RuntimeError,'after real material contact'):w.advance(t['due'])
        finally:w._arrive_contact=delegate
        self.assertEqual(len(attempts),1);self.assertEqual(attempts[0]['new_infections'],2)
        self.assertEqual(runtime.read_sql(w.db_path),before)
        final=w.advance(t['due']);self.assert_accounts(w)
        self.assertEqual(len([a for a in final['contact_applications'] if a['packet_id']==t['contact_packet_id']]),1)

    def test_contact_expired_and_actual_destination_restriction(self):
        # Original frozen TTL is one day; actual graph path chooses a farther
        # domestic destination where available, never a fake _route_seconds.
        w,a=self.contact_world()
        raw=self.raw(w)
        domestic=[p['id'] for p in raw['places'] if p['country_id']=='country-1' and p['id']!='place-1']
        target=max(domestic,key=lambda p:w._route_seconds(raw,'place-1',p))
        duration=w._route_seconds(raw,'place-1',target)
        self.commit(w,a,[shipment(to=target)])
        t=w.snapshot()['economy']['transport']['transfers'][0]
        arrived=w.advance(t['due'])
        app=next(x for x in arrived['contact_applications'] if x['packet_id']==t['contact_packet_id'])
        self.assertEqual(app['status'],'expired' if duration>=DAY else 'applied')
        self.assert_accounts(w)
        # Destination place-0: its real marshal travels there and accepts a
        # real local command before dispatch, without moving any fixture person.
        w,a=self.contact_world()
        marshal='person-country-1-marshal';king='person-country-1-king'
        trip=w.submit(marshal,'defender-travel',dict(action='travel',to='place-0'));w.advance(trip['effect']['due'])
        command=w.submit(king,'restrict-destination',dict(action='command',to=marshal,text='限制聚集'))
        w.advance(command['effect']['due'])
        w.submit(marshal,'accept-restrict',dict(action='respond',command_id=command['effect']['command_id'],accept=True))
        self.commit(w,a,[shipment(to='place-0')]);t=w.snapshot()['economy']['transport']['transfers'][0]
        final=w.advance(t['due']);app=next(x for x in final['contact_applications'] if x['packet_id']==t['contact_packet_id'])
        self.assertEqual((app['effective_contact_units'],app['new_infections']),(2,0));self.assert_accounts(w)
