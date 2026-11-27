"""P07.2a fictional contact: real SQLite; no models/medical claims."""
from contextlib import closing
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from app.world import WorldStore, WorldError
from app.scenario import validate_scenario, ScenarioError

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = 'C:/Users/We3q/AppData/Local/hermes/cache/scratch'
RUNTIME = 'CONTACT_EXPOSURE_V2'
KING = 'person-country-1-king'
MARSHAL = 'person-country-1-marshal'
FINANCE = 'person-country-1-finance'
DAY = 86400


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def mechanism(rules):
    return hashlib.sha256(canonical({'runtime':RUNTIME, 'pathology':rules}).encode()).hexdigest()


class TransmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.geo = json.loads((ROOT/'data/geography.json').read_text(encoding='utf-8'))
        self.config = json.loads((ROOT/'data/scenarios/iron-4.json').read_text(encoding='utf-8'))
        self.config['pathology_runtime'] = RUNTIME
        self.config['pathology'].update(version='NON_MEDICAL_FICTIONAL_CONTACT_V2', exposure_lifetime_seconds=DAY)

    def world(self, config=None, name='world'):
        path = self.base/(name+'.json')
        path.write_text(canonical(self.config if config is None else config), encoding='utf-8')
        world = WorldStore(self.base/(name+'.sqlite'), path)
        world.initialize()
        return world

    def rows(self, world):
        with closing(sqlite3.connect(world.db_path)) as db:
            return (db.execute('SELECT seq,state FROM snapshots ORDER BY seq').fetchall(),
                    db.execute('SELECT * FROM intents ORDER BY id').fetchall())

    def place(self, state, pid):
        return next(p for p in state['places'] if p['id']==pid)

    def test_explicit_runtime_validation_and_joint_identity(self):
        from app import disease
        self.assertTrue(callable(getattr(disease, 'contact_mechanism_digest', None)), 'missing explicit v2 validator')
        rules = self.config['pathology'] | {'extension': {'中文':['😀', True, None]}}
        self.assertEqual(disease.contact_mechanism_digest(RUNTIME, rules), mechanism(rules))
        self.assertEqual(validate_scenario(self.config, self.geo), self.config)
        invalid = []
        for runtime in (None, True, 2, '', 'CONTACT_EXPOSURE_V3', {}, []):
            invalid.append(self.config | {'pathology_runtime':runtime})
        for ttl in (None, True, False, 0, -1, 1.0, '86400'):
            invalid.append(self.config | {'pathology':rules | {'exposure_lifetime_seconds':ttl}})
        invalid.append(self.config | {'pathology':{k:v for k,v in rules.items() if k!='exposure_lifetime_seconds'}})
        invalid.append(self.config | {'pathology':rules | {'version':'NON_MEDICAL_FICTIONAL_CONTACT_V1'}})
        for config in invalid:
            with self.subTest(config=config):
                with self.assertRaises(ScenarioError) as error:
                    validate_scenario(config, self.geo)
                self.assertEqual(error.exception.code, 'INVALID_PATHOLOGY')
        # The old digest keeps unknown keys identity-only, including a bad v2 TTL.
        from app.disease import pathology_digest
        old = rules | {'exposure_lifetime_seconds':True}
        self.assertEqual(pathology_digest(old), hashlib.sha256(canonical(old).encode()).hexdigest())
        legacy_config = copy.deepcopy(self.config)
        del legacy_config['pathology_runtime']
        legacy_config['pathology'] = old
        self.assertEqual(validate_scenario(legacy_config, self.geo), legacy_config)

    def test_initializer_freezes_runtime_before_original_seed_and_three_packs(self):
        for count in (4,5,6):
            path = ROOT/f'data/scenarios/contact-v2-iron-{count}.json'
            self.assertTrue(path.exists(), 'missing explicit v2 scenario')
            config = json.loads(path.read_text(encoding='utf-8'))
            original = json.loads((ROOT/f'data/scenarios/iron-{count}.json').read_text(encoding='utf-8'))
            expected = copy.deepcopy(original)
            expected.update(pathology_runtime=RUNTIME, scenario_id=f'fictional-iron-{count}-contact-v2', version='authored-iron-contact-v2')
            expected['pathology'].update(version='NON_MEDICAL_FICTIONAL_CONTACT_V2', exposure_lifetime_seconds=DAY)
            self.assertEqual(config, expected)
            world = WorldStore(self.base/f'pack-{count}.sqlite', path)
            infect = world._infect
            calls = []
            def seed(state, place, number):
                self.assertEqual(state['pathology_runtime'], RUNTIME)
                self.assertEqual(state['pathology_mechanism_sha256'], mechanism(config['pathology']))
                self.assertEqual(state['pathology_sha256'], hashlib.sha256(canonical(config['pathology']).encode()).hexdigest())
                self.assertEqual((state['time'],place['id'],number), (0,'place-1',1))
                calls.append(1)
                return infect(state, place, number)
            with patch.object(world, '_infect', side_effect=seed):
                initial = world.initialize()
            self.assertEqual(calls, [1])
            self.assertEqual(initial['pathology_frozen_at'], 0)
            self.assertEqual(world.snapshot(1), initial)
        legacy = WorldStore(self.base/'default.sqlite').initialize()
        self.assertNotIn('pathology_runtime', legacy)
        self.assertNotIn('pathology_mechanism_sha256', legacy)

    def test_runtime_freeze_pre_and_post_guards_reject_downgrade_and_injection(self):
        variants = [lambda s:s.pop('pathology_runtime'),
                    lambda s:s.update(pathology_runtime='OTHER'),
                    lambda s:s['_config'].pop('pathology_runtime'),
                    lambda s:s['_config'].update(pathology_runtime=None),
                    lambda s:s.pop('pathology_mechanism_sha256'),
                    lambda s:s.update(pathology_mechanism_sha256='0'*64)]
        for index, tamper in enumerate(variants):
            for pre in (False, True):
                world = self.world(name=f'freeze-{index}-{pre}')
                world.control('resume')
                if pre:
                    with closing(sqlite3.connect(world.db_path)) as db:
                        state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                        state['revision'] += 1
                        tamper(state)
                        db.execute('INSERT INTO snapshots VALUES (?,?)',(state['revision'],canonical(state)))
                        db.commit()
                before = self.rows(world)
                def operation(state, db):
                    if pre:
                        self.fail('pre-guard must run before mutation')
                    tamper(state)
                    state['places'][0]['grain'] -= 10
                    db.execute('INSERT INTO intents VALUES (?,?,?,?)',('bad',KING,'{}','{}'))
                with self.assertRaises(WorldError) as error:
                    world._mutate(operation)
                self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
                self.assertEqual(self.rows(world), before)
                if pre:
                    with self.assertRaises(WorldError) as error:
                        world.submit(KING,'no-reservation',{'action':'wait'})
                    self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
                    self.assertEqual(self.rows(world), before)
        config = copy.deepcopy(self.config)
        del config['pathology_runtime']
        old = self.world(config, 'old-injection')
        before = self.rows(old)
        with self.assertRaises(WorldError) as error:
            old._mutate(lambda state,db:state.update(pathology_runtime=RUNTIME, pathology_mechanism_sha256=mechanism(state['pathology'])))
        self.assertEqual(error.exception.code, 'FROZEN_PATHOLOGY')
        self.assertEqual(self.rows(old), before)

    def test_cross_country_travel_freezes_one_host_packet_only_at_departure(self):
        world = self.world()
        world.control('resume')
        world.advance(2*DAY+43200)  # multiple active source cases, one opportunity
        source = self.place(world.snapshot(),'place-1')['disease']
        self.assertGreater(sum(c['count'] for c in source['cohorts'] if c['stage'] in ('symptomatic','severe')), 1)
        depart = world.snapshot()
        receipt = world.submit(MARSHAL,'foreign-visit',{'action':'travel','to':'place-10'})
        state = world.snapshot()
        self.assertEqual(len(state.get('contact_packets',[])), 1, 'departure must persist host-derived exposure')
        packet = state['contact_packets'][0]
        self.assertEqual({k:packet[k] for k in ('channel','source_place_id','destination_place_id','departed_at','expires_at','contact_units','cause_id','mechanism_sha256')},
                         dict(channel='travel',source_place_id='place-1',destination_place_id='place-10',departed_at=depart['time'],expires_at=depart['time']+DAY,contact_units=20,cause_id='foreign-visit',mechanism_sha256=mechanism(self.config['pathology'])))
        self.assertEqual(packet['source_activity']['restricted'], False)
        self.assertGreater(packet['source_activity']['active_count'], 0)
        self.assertEqual(packet['source_activity']['cohorts'], [c for c in source['cohorts'] if c['stage'] in ('symptomatic','severe')])
        self.assertEqual(self.place(state,'place-10')['disease'], self.place(depart,'place-10')['disease'])
        queued = next(q for q in state['queue'] if q['kind']=='arrival')
        self.assertEqual(queued['contact_packet_id'], packet['id'])
        self.assertEqual(queued['cause_id'], 'foreign-visit')
        self.assertEqual(self.place(state,'place-1')['disease'],source)
        self.assertNotIn('contact_packets',world.observe(MARSHAL))
        self.assertNotIn('infected', canonical(next(p for p in state['persons'] if p['id']==MARSHAL)))

    def test_actual_cross_country_arrival_creates_latent_cohort_with_causal_stages(self):
        world = self.world()
        world.control('resume')
        world.advance(DAY)
        before = world.snapshot()
        receipt = world.submit(MARSHAL,'arrival-proof',{'action':'travel','to':'place-10'})
        due = receipt['effect']['due']
        self.assertLess(due, DAY+DAY, 'real route must fall inside TTL for positive fixture')
        pending = world.advance(due-1)
        self.assertEqual(self.place(pending,'place-10')['disease']['cohorts'],[])
        state = world.advance(due)
        cohorts = self.place(state,'place-10')['disease']['cohorts']
        self.assertEqual(len(cohorts),1,'only actual arrival can seed destination')
        cohort = cohorts[0]
        self.assertEqual((cohort['count'],cohort['infected_at'],cohort['stage']), (2,due,'latent'))
        packet = state['contact_packets'][0]
        self.assertEqual(cohort['contact_source'],dict(packet_id=packet['id'], source_place_id='place-1',arrived_at=due,cause_id='arrival-proof'))
        stages = [q for q in state['queue'] if q.get('cohort_id')==cohort['id']]
        self.assertEqual([q['at'] for q in stages], [due+self.config['pathology'][key] for key in ('symptom_seconds','severe_seconds','recovery_seconds')])
        resolution = state['contact_applications'][0]
        self.assertEqual((resolution['packet_id'],resolution['new_infections'],resolution['arrived_at']), (packet['id'],2,due))
        arrival = next(e for e in state['events'] if e['kind']=='arrival')
        exposure = next(e for e in state['events'] if e['kind']=='contact_exposure_applied')
        self.assertEqual(exposure['arrival_event_id'], arrival['id'])
        self.assertEqual(exposure['cause_id'], 'arrival-proof')
        self.assertEqual(self.place(state,'place-1')['disease']['cohorts'][0], self.place(before,'place-1')['disease']['cohorts'][0])
        self.assertEqual(world.snapshot(pending['revision']),pending)
        view = canonical(world.observe(MARSHAL))
        for secret in ('packet','contact_units','cohort','mechanism_sha256','传染','susceptible'):
            self.assertNotIn(secret,view)

    def test_shipping_contacts_only_when_grain_really_enters_domestic_warehouse(self):
        world = self.world()
        world.control('resume')
        move = world.submit(FINANCE,'finance-onsite',{'action':'travel','to':'place-1'})
        world.advance(move['effect']['due'])
        world.advance(DAY)
        before = world.snapshot()
        receipt = world.submit(FINANCE,'grain-proof',{'action':'ship','to':'place-0','quantity':17})
        pending = world.snapshot()
        self.assertEqual(len(pending.get('contact_packets',[])),1,'shipment departure requires a packet')
        self.assertEqual(pending['contact_packets'][0]['channel'],'ship')
        self.assertEqual(self.place(pending,'place-1')['grain'], self.place(before,'place-1')['grain']-17)
        self.assertEqual(self.place(pending,'place-0')['grain'], self.place(before,'place-0')['grain'])
        due = receipt['effect']['due']
        self.assertEqual(self.place(world.advance(due-1),'place-0')['disease']['cohorts'],[])
        final = world.advance(due)
        self.assertEqual([c['count'] for c in self.place(final,'place-0')['disease']['cohorts']], [2])
        self.assertEqual(self.place(final,'place-0')['grain'], self.place(before,'place-0')['grain']+17)
        self.assertEqual(final['in_transit'],[])
        ledger = final['ledger']
        self.assertEqual(sum(p['grain'] for p in final['places'])+ledger['consumed'],ledger['initial']+ledger['harvested'])
        rows = self.rows(world)
        self.assertEqual(WorldStore(world.db_path).submit(FINANCE,'grain-proof',{'action':'ship','to':'place-0','quantity':17}),receipt)
        self.assertEqual(self.rows(world),rows)

    def test_successfully_delivered_send_and_command_messengers_carry_host_contacts(self):
        for action in ('send','command'):
            world = self.world(name='messenger-'+action)
            world.control('resume')
            sender, recipient = MARSHAL, KING
            if action=='command':
                sender, recipient = KING, FINANCE
                move = world.submit(KING,'king-onsite',{'action':'travel','to':'place-1'})
                world.advance(move['effect']['due'])
            world.advance(DAY)
            receipt = world.submit(sender,'letter',{'action':action,'to':recipient,'text':'普通来信；不声明传播风险'})
            state = world.snapshot()
            self.assertEqual(len(state.get('contact_packets',[])),1, 'actual messenger departure must carry exposure')
            self.assertEqual(state['contact_packets'][0]['channel'],'messenger')
            self.assertEqual(world.observe(recipient)['messages'],[])
            due = receipt['effect']['due']
            self.assertEqual(self.place(world.advance(due-1),'place-0')['disease']['cohorts'],[])
            final = world.advance(due)
            self.assertEqual([c['count'] for c in self.place(final,'place-0')['disease']['cohorts']], [2])
            self.assertEqual(len(world.observe(recipient)['messages']),1)
            self.assertEqual(final['contact_applications'][0]['arrived_at'], due)
            self.assertTrue(any(e['kind']=='message_delivered' for e in final['events']))
            self.assertNotIn('contact_units',canonical(world.observe(recipient)))

    def test_half_open_ttl_uses_actual_arrival_without_changing_pathology_values(self):
        for duration, expected in ((DAY-1,2),(DAY,0),(DAY+1,0)):
            world = self.world(name=f'ttl-{duration}')
            world.control('resume')
            world.advance(DAY)
            with patch.object(world,'_route_seconds',return_value=duration):
                receipt = world.submit(MARSHAL,'ttl-trip',{'action':'travel','to':'place-10'})
            final = world.advance(receipt['effect']['due'])
            disease = self.place(final,'place-10')['disease']
            self.assertEqual(sum(c['count'] for c in disease['cohorts']),expected, 'arrival at TTL equality must not infect')
            self.assertEqual(final['contact_applications'][0]['new_infections'],expected)
            self.assertEqual(final['pathology'], self.config['pathology'])
            self.assertTrue(any(e['kind']=='arrival' for e in final['events']))

    def order(self, world, issuer, recipient, suffix, accept=True):
        receipt = world.submit(issuer,'order-'+suffix,{'action':'command','to':recipient,'text':'请限制聚集'})
        world.advance(receipt['effect']['due'])
        return world.submit(recipient,'response-'+suffix,{'action':'respond','command_id':receipt['effect']['command_id'],'accept':accept})

    def test_destination_restriction_is_sampled_at_actual_arrival_not_retroactive(self):
        outcomes=[]
        for when in ('before','after'):
            world = self.world(name='dest-'+when)
            world.control('resume')
            marshal2, king2 = 'person-country-2-marshal', 'person-country-2-king'
            move = world.submit(marshal2,'defender-onsite',{'action':'travel','to':'place-10'})
            world.advance(move['effect']['due'])
            world.advance(DAY)
            receipt = world.submit(MARSHAL,'dest-restrict-trip',{'action':'travel','to':'place-10'})
            due = receipt['effect']['due']
            packet = copy.deepcopy(world.snapshot()['contact_packets'][0])
            if when=='before':
                self.order(world,king2,marshal2,'before')
                self.assertLess(world.snapshot()['time'],due)
            arrived = world.advance(due)
            cases = copy.deepcopy(self.place(arrived,'place-10')['disease']['cohorts'])
            if when=='after':
                self.order(world,king2,marshal2,'after')
                self.assertEqual(self.place(world.snapshot(),'place-10')['disease']['cohorts'], cases)
            final=world.snapshot()
            self.assertEqual(final['contact_packets'][0],packet)
            self.assertEqual(final['contact_applications'][0]['effective_contact_units'],2 if when=='before' else 20)
            outcomes.append(sum(c['count'] for c in cases))
        self.assertEqual(outcomes,[0,2])

    def test_v2_accepted_intervention_identifies_joint_mechanism(self):
        world = self.world()
        initial = world.snapshot()
        world.control('resume')
        receipt = self.order(world,KING,MARSHAL,'joint')
        effect = receipt['effect']['contact_restriction']
        self.assertEqual(effect['mechanism_sha256'],initial['pathology_mechanism_sha256'])
        self.assertNotEqual(effect['mechanism_sha256'],initial['pathology_sha256'])

    def test_duplicate_actual_tasks_and_packet_ids_are_durable_once(self):
        for channel in ('travel','ship','send'):
            world = self.world(name='once-'+channel)
            world.control('resume')
            if channel=='ship':
                move=world.submit(FINANCE,'onsite',{'action':'travel','to':'place-1'})
                world.advance(move['effect']['due'])
            world.advance(DAY)
            sender = FINANCE if channel=='ship' else MARSHAL
            body = {'travel':{'action':'travel','to':'place-0'},
                    'ship':{'action':'ship','to':'place-0','quantity':9},
                    'send':{'action':'send','to':KING,'text':'实际来信'}}[channel]
            receipt=world.submit(sender,'once',body)
            queued=copy.deepcopy(next(q for q in world.snapshot()['queue'] if q.get('contact_packet_id')))
            world.advance(receipt['effect']['due'])
            before=self.rows(world)
            def duplicate(state,db):
                world._process_event(state,copy.deepcopy(queued))
                # Replayed same packet under another task ID must not duplicate effects.
                world._process_event(state,dict(queued,id='duplicate-task'))
            world._mutate(duplicate)
            self.assertEqual(self.rows(world),before,'duplicate arrival must be a no-op, including grain/messages')
            final=world.snapshot()
            self.assertEqual(len(final['contact_applications']),1)
            self.assertEqual(sum(c['count'] for c in self.place(final,'place-0')['disease']['cohorts']),2)

    def test_both_preview_paths_reject_latest_runtime_corruption_against_first_snapshot(self):
        from app.adjudication import Adjudicator
        for new in (False,True):
            config=copy.deepcopy(self.config)
            if not new:
                del config['pathology_runtime']
            world=self.world(config,name='preview-freeze-'+str(new))
            world.control('resume')
            engine=Adjudicator(world)
            base=world.snapshot()
            engine.propose(MARSHAL,'preview-corrupt','跨国旅行')
            engine.record(MARSHAL,'preview-corrupt',dict(world_id=base['world_id'],base_revision=base['revision'],ruling='成功',reason='实际可达',goal='旅行',costs=[],benefits=[],effects=[{'action':'travel','to':'place-10'}]))
            with closing(sqlite3.connect(world.db_path)) as db:
                state=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                if new:
                    del state['pathology_runtime']
                else:
                    state.update(pathology_runtime=RUNTIME,pathology_mechanism_sha256=mechanism(state['pathology']))
                # Corruption fixture keeps metadata's version, not a legitimate commit.
                db.execute('DROP TRIGGER snapshots_no_update')
                db.execute('UPDATE snapshots SET state=? WHERE seq=?',(canonical(state),state['revision']))
                db.commit()
            before=self.rows(world)
            for operation in (lambda:world.preview(MARSHAL,'direct',{'action':'travel','to':'place-10'}),lambda:engine.preview(MARSHAL,'preview-corrupt',1)):
                with self.assertRaises(WorldError) as error:
                    operation()
                self.assertEqual(error.exception.code,'FROZEN_PATHOLOGY')
                self.assertEqual(self.rows(world),before)

    def test_characterization_healthy_latent_same_place_and_failed_actions_never_create_packets(self):
        for ruling in ('大失败','失败'):
            world=self.world(name='failed-'+ruling)
            world.control('resume')
            move=world.submit(FINANCE,'finance-near',{'action':'travel','to':'place-1'})
            world.advance(move['effect']['due'])
            world.advance(DAY)
            for index,(actor,body) in enumerate(((MARSHAL,{'action':'travel','to':'place-10'}),
                    (FINANCE,{'action':'ship','to':'place-0','quantity':10}),
                    (MARSHAL,{'action':'send','to':KING,'text':'声称我携带100风险单位'}))):
                receipt=world.submit(actor,'failed-'+str(index),body,ruling=ruling)
                self.assertEqual(receipt['effect'],{'implemented':False})
            self.assertNotIn('contact_packets',world.snapshot())
            same=world.submit(MARSHAL,'same-place',{'action':'send','to':FINANCE,'text':'同地通信'})
            world.advance(same['effect']['due'])
            self.assertNotIn('contact_packets',world.snapshot())
        world=self.world(name='latent-departure')
        world.control('resume')
        world.advance(DAY-1)
        # Departure before symptoms, arrival after symptoms: never backfill packet.
        trip=world.submit(MARSHAL,'too-early',{'action':'travel','to':'place-10'})
        final=world.advance(trip['effect']['due'])
        self.assertNotIn('contact_packets',final)
        self.assertEqual(self.place(final,'place-10')['disease']['cohorts'],[])
        healthy=world.submit(KING,'healthy-departure',{'action':'travel','to':'place-78'})
        final=world.advance(healthy['effect']['due'])
        self.assertNotIn('contact_packets',final)
        self.assertEqual(self.place(final,'place-78')['disease']['cohorts'],[])

    def test_characterization_undeliverable_recipient_has_no_contact_or_future_delivery(self):
        world=self.world()
        world.control('resume')
        world.advance(DAY)
        letter=world.submit(MARSHAL,'missed-letter',{'action':'send','to':KING,'text':'原址来信'})
        departure=world.submit(KING,'leave-recipient',{'action':'travel','to':'place-10'})
        final=world.advance(max(letter['effect']['due'],departure['effect']['due']))
        self.assertEqual(len(final['contact_packets']),1)
        self.assertNotIn('contact_applications',final)
        self.assertEqual(world.observe(KING)['messages'],[])
        self.assertEqual(self.place(final,'place-0')['disease']['cohorts'],[])
        self.assertTrue(any(e['kind']=='undeliverable' for e in final['events']))
        self.assertEqual(world.observe(KING,1)['messages'],[])

    def test_characterization_source_restriction_before_after_and_refusal_preserve_departure_facts(self):
        results=[]
        for when in ('before','after','refuse'):
            world=self.world(name='source-'+when)
            world.control('resume')
            move=world.submit(FINANCE,'onsite',{'action':'travel','to':'place-1'})
            world.advance(move['effect']['due'])
            world.advance(DAY)
            order=world.submit(KING,'source-order',{'action':'command','to':MARSHAL,'text':'限制聚集'})
            if when=='after':
                world.advance(DAY+1)
                trip=world.submit(FINANCE,'source-trip',{'action':'travel','to':'place-0'})
            world.advance(order['effect']['due'])
            response=world.submit(MARSHAL,'source-response',{'action':'respond','command_id':order['effect']['command_id'],'accept':when!='refuse'})
            if when!='after':
                trip=world.submit(FINANCE,'source-trip',{'action':'travel','to':'place-0'})
            packet=copy.deepcopy(world.snapshot()['contact_packets'][0])
            self.assertEqual(packet['contact_units'],2 if when=='before' else 20)
            final=world.advance(trip['effect']['due'])
            self.assertEqual(final['contact_packets'][0],packet)
            cases=sum(c['count'] for c in self.place(final,'place-0')['disease']['cohorts'])
            results.append(cases)
            if when=='refuse':
                self.assertNotIn('contact_restriction',response['effect'])
                self.assertFalse(self.place(final,'place-1')['disease']['restricted'])
        self.assertEqual(results,[0,2,2])

    def test_characterization_destination_exhaustion_caps_without_moving_source_population(self):
        for remaining in (0,1):
            world=self.world(name='susceptible-'+str(remaining))
            world.control('resume')
            world.advance(DAY)
            def exhausted(state,db):
                place=self.place(state,'place-10')
                place['disease']['susceptible']=remaining
                place['disease']['cohorts']=[dict(id='fixture-recovered',infected_at=0,count=place['population']-remaining,stage='recovered')]
            world._mutate(exhausted)
            source=copy.deepcopy(self.place(world.snapshot(),'place-1')['disease'])
            trip=world.submit(MARSHAL,'capped',{'action':'travel','to':'place-10'})
            final=world.advance(trip['effect']['due'])
            disease=self.place(final,'place-10')['disease']
            self.assertEqual(disease['susceptible'],0)
            self.assertEqual(final['contact_applications'][0]['new_infections'],remaining)
            self.assertEqual(sum(c['count'] for c in disease['cohorts']),self.place(final,'place-10')['population'])
            self.assertEqual(self.place(final,'place-1')['disease'],source)

    def test_characterization_packet_injection_rejected_for_actor_and_dm_without_world_writes(self):
        from app.adjudication import Adjudicator
        world=self.world()
        world.control('resume')
        world.advance(DAY)
        body={'action':'travel','to':'place-10','contact_packet':{'contact_units':9999}}
        before=self.rows(world)
        with self.assertRaises(WorldError) as error:
            world.submit(MARSHAL,'injected',body)
        self.assertEqual(error.exception.code,'INVALID_BODY')
        self.assertEqual(self.rows(world),before)
        engine=Adjudicator(world)
        state=world.snapshot()
        engine.propose(MARSHAL,'dm-injected','我能直接写风险')
        engine.record(MARSHAL,'dm-injected',dict(world_id=state['world_id'],base_revision=state['revision'],ruling='成功',reason='不可信扩展',goal='旅行',costs=[],benefits=[],effects=[body]))
        before=self.rows(world)
        for op in (lambda:engine.preview(MARSHAL,'dm-injected',1),lambda:engine.commit(MARSHAL,'dm-injected',1)):
            with self.assertRaises(WorldError) as error:
                op()
            self.assertEqual(error.exception.code,'INVALID_BODY')
            self.assertEqual(self.rows(world),before)

    def test_undeliverable_packet_cannot_be_repurposed_by_later_task_retry(self):
        world=self.world(name='missed-once')
        world.control('resume')
        world.advance(DAY)
        letter=world.submit(MARSHAL,'missed',{'action':'send','to':KING,'text':'原址'})
        queued=copy.deepcopy(next(q for q in world.snapshot()['queue'] if q.get('message_id')=='message-missed'))
        move=world.submit(KING,'away',{'action':'travel','to':'place-1'})
        world.advance(max(move['effect']['due'],letter['effect']['due']))
        back=world.submit(KING,'back',{'action':'travel','to':'place-0'})
        world.advance(back['effect']['due'])
        before=self.rows(world)
        world._mutate(lambda state,db:world._process_event(state,dict(queued,id='retry-missed')))
        self.assertEqual(self.rows(world),before,'failed delivery is terminal, not a fresh contact opportunity')

    def test_characterization_arrival_fault_rolls_back_inventory_messages_location_and_contacts(self):
        for channel in ('travel','ship','send'):
            world=self.world(name='rollback-'+channel)
            world.control('resume')
            if channel=='ship':
                move=world.submit(FINANCE,'rollback-onsite',{'action':'travel','to':'place-1'})
                world.advance(move['effect']['due'])
            world.advance(DAY)
            sender=FINANCE if channel=='ship' else MARSHAL
            body={'travel':{'action':'travel','to':'place-0'},'ship':{'action':'ship','to':'place-0','quantity':11},'send':{'action':'send','to':KING,'text':'实际报告'}}[channel]
            receipt=world.submit(sender,'fault-arrival',body)
            due=receipt['effect']['due']
            before=self.rows(world)
            infect=world._infect
            def fail_after_cohort(state,place,number):
                infect(state,place,number)
                raise RuntimeError('injected after latent cohort and stages')
            with patch.object(world,'_infect',side_effect=fail_after_cohort):
                with self.assertRaisesRegex(RuntimeError,'injected'):
                    world.advance(due)
            self.assertEqual(self.rows(world),before)
            self.assertEqual(world.observe(KING)['messages'],[])
            final=world.advance(due)
            self.assertEqual(len(final['contact_applications']),1)
            self.assertEqual(final['contact_applications'][0]['new_infections'],2)

    def compound(self, world, actor, intent, bodies):
        from app.adjudication import Adjudicator
        engine=Adjudicator(world)
        base=world.snapshot()
        engine.propose(actor,intent,'按实际条件调粮、通信和旅行')
        version=engine.record(actor,intent,dict(world_id=base['world_id'],base_revision=base['revision'],ruling='成功',reason='有限脚本裁定',goal='联系',costs=[],benefits=[],effects=bodies))
        return engine,version

    def test_characterization_compound_preview_is_readonly_and_failure_leaves_no_packets(self):
        world=self.world()
        world.control('resume')
        move=world.submit(FINANCE,'compound-onsite',{'action':'travel','to':'place-1'})
        world.advance(move['effect']['due'])
        world.advance(DAY)
        bodies=[{'action':'ship','to':'place-0','quantity':12},{'action':'send','to':KING,'text':'已发粮'},{'action':'travel','to':'place-10'}]
        engine,version=self.compound(world,FINANCE,'compound',bodies)
        before=self.rows(world)
        digest=hashlib.sha256(world.db_path.read_bytes()).hexdigest()
        knowledge={p['id']:world.observe(p['id']) for p in world.snapshot()['persons']}
        draft=engine.preview(FINANCE,'compound',version)
        self.assertEqual(len(draft['projected']['contact_packets']),3)
        self.assertEqual(self.rows(world),before)
        self.assertEqual(hashlib.sha256(world.db_path.read_bytes()).hexdigest(),digest)
        for pid,view in knowledge.items():
            self.assertEqual(world.observe(pid),view)
        apply=world._apply_action
        calls=[]
        def fail_second(*args):
            calls.append(1)
            result=apply(*args)
            if len(calls)==2:
                raise RuntimeError('injected after second packet')
            return result
        with patch.object(world,'_apply_action',side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError,'second packet'):
                engine.commit(FINANCE,'compound',version)
        self.assertEqual(self.rows(world),before)
        receipt=engine.commit(FINANCE,'compound',version)
        self.assertEqual(receipt['effect']['effects'],draft['effects'])
        self.assertEqual(world.snapshot(),draft['projected'])
        before=self.rows(world)
        self.assertEqual(engine.commit(FINANCE,'compound',version),receipt)
        self.assertEqual(self.rows(world),before)

    def test_characterization_compound_terminal_and_historical_privacy_survive_epidemic_events(self):
        for channel in ('travel','ship','send'):
            world=self.world(name='causality-'+channel)
            world.control('resume')
            if channel=='ship':
                move=world.submit(FINANCE,'causal-onsite',{'action':'travel','to':'place-1'})
                world.advance(move['effect']['due'])
            world.advance(DAY)
            actor=FINANCE if channel=='ship' else MARSHAL
            body={'travel':{'action':'travel','to':'place-10'},'ship':{'action':'ship','to':'place-0','quantity':7},'send':{'action':'send','to':KING,'text':'真实私信'}}[channel]
            engine,version=self.compound(world,actor,'causal-'+channel,[body])
            receipt=engine.commit(actor,'causal-'+channel,version)
            pending=world.snapshot()
            historical={p['id']:world.observe(p['id'],pending['revision']) for p in pending['persons']}
            world.advance(receipt['effect']['effects'][0]['due'])
            trace=engine.trace()['proposals'][0]['committed']
            task=trace['tasks'][0]
            self.assertEqual(task['status'],'completed')
            final=world.snapshot()
            event=next(e for e in final['events'] if e['id']==task['event_id'])
            expected={'travel':'arrival','ship':'shipment_arrived','send':'message_delivered'}[channel]
            self.assertEqual(event['kind'],expected)
            self.assertEqual(final['contact_packets'][0]['cause_id'],engine._child_id('causal-'+channel,version,0))
            exposure=next(e for e in final['events'] if e['kind']=='contact_exposure_applied')
            self.assertEqual(exposure['cause_id'],task['cause_id'])
            self.assertEqual(engine.trace(pending['revision'])['proposals'][0]['committed']['tasks'][0]['status'],'pending')
            self.assertEqual(world.snapshot(pending['revision']),pending)
            for pid,view in historical.items():
                self.assertEqual(world.observe(pid,pending['revision']),view)
                encoded=canonical(world.observe(pid))
                for secret in ('contact_units','contact_packets','contact_source','cohort','pathology','mechanism_sha256'):
                    self.assertNotIn(secret,encoded)
            self.assertEqual(world.observe('person-country-4-king')['observations'],[])
            self.assertEqual(world.observe('person-country-4-king')['messages'],[])

    def test_characterization_real_process_reopen_reconciles_original_receipt_without_duplicate_arrival(self):
        world=self.world(name='process')
        world.control('resume')
        move=world.submit(FINANCE,'process-onsite',{'action':'travel','to':'place-1'})
        world.advance(move['effect']['due'])
        world.advance(DAY)
        body={'action':'ship','to':'place-0','quantity':13}
        receipt=world.submit(FINANCE,'process-shipment',body)
        code="from app.world import WorldStore; import sys; w=WorldStore(sys.argv[1]); w.initialize(); w.advance(int(sys.argv[2])); print('actual-arrival-committed')"
        first=subprocess.run([sys.executable,'-B','-c',code,str(world.db_path),str(receipt['effect']['due'])],cwd=ROOT.parent,capture_output=True,text=True)
        self.assertEqual(first.returncode,0,first.stderr)
        self.assertIn('actual-arrival-committed',first.stdout)
        before=self.rows(world)
        replay="from app.world import WorldStore; import sys,json; w=WorldStore(sys.argv[1]); w.initialize(); print(json.dumps(w.submit(sys.argv[2],sys.argv[3],json.loads(sys.argv[4])),sort_keys=True))"
        second=subprocess.run([sys.executable,'-B','-c',replay,str(world.db_path),FINANCE,'process-shipment',canonical(body)],cwd=ROOT.parent,capture_output=True,text=True)
        self.assertEqual(second.returncode,0,second.stderr)
        self.assertEqual(json.loads(second.stdout),receipt)
        self.assertEqual(self.rows(world),before)
        final=world.snapshot()
        self.assertEqual(len(final['contact_applications']),1)
        self.assertEqual(final['contact_applications'][0]['new_infections'],2)

    def test_characterization_original_full_snapshot_without_digest_keeps_unknown_keys_inert(self):
        # Old-format first frame: unknown keys collide but never opt into v2.
        seed=WorldStore(self.base/'legacy-seed.sqlite')
        seed.initialize()
        with closing(sqlite3.connect(seed.db_path)) as db:
            state=json.loads(db.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0])
            state['_geography']=json.loads(db.execute("SELECT value FROM world_constants WHERE key='geography'").fetchone()[0])
        del state['pathology_sha256']
        state['pathology'].update(version='NON_MEDICAL_FICTIONAL_CONTACT_V2',exposure_lifetime_seconds=True,pathology_runtime=RUNTIME)
        state['_config']['pathology']=copy.deepcopy(state['pathology'])
        state['_config']['pathology_runtime']=RUNTIME
        original=canonical(state)
        world=WorldStore(self.base/'legacy-no-digest.sqlite')
        with closing(sqlite3.connect(world.db_path)) as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('CREATE TABLE snapshots(seq INTEGER PRIMARY KEY,state TEXT NOT NULL)')
            db.execute('CREATE TABLE intents(id TEXT PRIMARY KEY,person TEXT NOT NULL,payload TEXT NOT NULL,receipt TEXT)')
            db.execute('INSERT INTO snapshots VALUES (1,?)',(original,))
            db.commit()
        before=world.db_path.read_bytes()
        initial=world.initialize()
        world.observe(MARSHAL,1)
        self.assertEqual(world.db_path.read_bytes(),before)
        world.control('resume')
        world.advance(DAY)
        trip=world.submit(MARSHAL,'old-trip',{'action':'travel','to':'place-10'})
        final=world.advance(trip['effect']['due'])
        self.assertEqual(self.place(final,'place-10')['disease']['cohorts'],[])
        for field in ('contact_packets','contact_applications','pathology_runtime','pathology_mechanism_sha256','pathology_sha256'):
            self.assertNotIn(field,final)
        self.assertEqual(world.snapshot(1),initial)
        self.assertEqual(self.rows(world)[0][0],(1,original))

    def test_characterization_source_recovery_cannot_rewrite_existing_packet(self):
        world=self.world(name='recovered-source')
        world.control('resume')
        world.advance(DAY)
        trip=world.submit(MARSHAL,'depart-before-recovery',{'action':'travel','to':'place-10'})
        packet=copy.deepcopy(world.snapshot()['contact_packets'][0])
        def recover_fixture(state,db):
            # Injected stage fixture, not a claim of natural full recovery.
            for cohort in self.place(state,'place-1')['disease']['cohorts']:
                cohort['stage']='recovered'
        world._mutate(recover_fixture)
        final=world.advance(trip['effect']['due'])
        self.assertEqual(final['contact_packets'][0],packet)
        self.assertEqual(final['contact_applications'][0]['new_infections'],2)
        self.assertEqual(packet['source_activity']['active_count'],1)

    def test_characterization_invalid_v2_initialization_is_atomic_before_any_seed(self):
        cases=[self.config|{'pathology_runtime':True},self.config|{'pathology_runtime':'UNKNOWN'}]
        for value in (True,0,None,1.0,'86400'):
            cases.append(self.config|{'pathology':self.config['pathology']|{'exposure_lifetime_seconds':value}})
        missing=copy.deepcopy(self.config)
        del missing['pathology']['exposure_lifetime_seconds']
        cases.append(missing)
        for index,config in enumerate(cases):
            path=self.base/f'invalid-v2-{index}.json'
            path.write_text(canonical(config),encoding='utf-8')
            world=WorldStore(self.base/f'invalid-v2-{index}.sqlite',path)
            with patch.object(world,'_infect') as seed:
                with self.assertRaises(WorldError) as error:
                    world.initialize()
                self.assertEqual(error.exception.code,'INVALID_PATHOLOGY')
                seed.assert_not_called()
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),[])

    def test_characterization_runtime_is_rechecked_in_second_submit_transaction(self):
        world=self.world(name='runtime-interleave')
        world.control('resume')
        mutate=world._mutate
        initial=world.snapshot()
        def interleave(operation):
            with closing(sqlite3.connect(world.db_path)) as db:
                raw=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                raw['revision']+=1
                del raw['pathology_runtime']
                db.execute('INSERT INTO snapshots VALUES (?,?)',(raw['revision'],canonical(raw)))
                db.commit()
            return mutate(operation)
        with patch.object(world,'_mutate',side_effect=interleave):
            with self.assertRaises(WorldError) as error:
                world.submit(MARSHAL,'runtime-interleave',{'action':'travel','to':'place-10'})
        self.assertEqual(error.exception.code,'FROZEN_PATHOLOGY')
        snapshots,intents=self.rows(world)
        self.assertEqual(len(intents),1)
        self.assertIsNone(intents[0][3])
        final=world.snapshot()
        self.assertEqual(final['events'],initial['events'])
        self.assertEqual(final['queue'],initial['queue'])
        self.assertNotIn('contact_packets',final)
