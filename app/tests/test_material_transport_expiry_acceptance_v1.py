"""261 supplemental existing-behavior acceptance, not feature RED or medicine.
New unregistered worlds declare a short positive TTL before initialize/seed.
Original map, routes, symptom timings, queue and host sampling remain real.
This does not prove expiry in the original standard TTL=86400 world.
"""
import copy
import json
import unittest
from app.tests import test_material_transport_v1 as rules
from app.tests import test_registered_production_runtime_v1 as runtime
from app.tests.test_registered_material_transport_v1 import shipment
from app.disease import CONTACT_RUNTIME, contact_mechanism_digest, pathology_digest
from app.world import DAY


class MaterialTransportExpiryAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.fixture = rules.MaterialTransportTests('runTest')
        self.fixture.setUp()
        self.root = self.fixture.root
        self.addCleanup(self.fixture.doCleanups)

    def exercise_new_world(self, ttl, expected_status, expected_infections):
        c = runtime.configuration()
        c['scenario_id'] = 'p08-controlled-short-ttl-new-world-'+str(ttl)
        c['version'] = 'p08-expiry-acceptance-v1'
        original = json.loads((runtime.DATA/'scenarios/contact-v2-iron-4.json').read_bytes())
        c['pathology_runtime'] = CONTACT_RUNTIME
        c['pathology'] = copy.deepcopy(original['pathology'])
        c['pathology']['exposure_lifetime_seconds'] = ttl
        declared = copy.deepcopy(c['pathology'])
        mechanism = contact_mechanism_digest(CONTACT_RUNTIME, declared)
        w, a, initial = self.fixture.own(c)
        self.assertEqual(initial['time'], 0)
        self.assertEqual(initial['pathology'], declared)
        self.assertEqual(initial['pathology_sha256'], pathology_digest(declared))
        self.assertEqual(initial['pathology_mechanism_sha256'], mechanism)
        self.assertNotIn('contact_packets', initial)
        trip = w.submit(runtime.PID, 'controlled-ttl-travel-to-original-source', dict(action='travel', to='place-1'))
        self.assertEqual(trip['effect']['due'], 3600)
        w.advance(trip['effect']['due'])
        w.advance(DAY)
        before = w.snapshot()
        source = next(p for p in before['places'] if p['id']=='place-1')
        active = [x for x in source['disease']['cohorts'] if x['stage'] in ('symptomatic','severe')]
        self.assertGreater(sum(x['count'] for x in active), 0)
        self.assertEqual(next(p for p in before['persons'] if p['id']==runtime.PID)['location'], 'place-1')
        self.assertEqual(next(p for p in before['places'] if p['id']=='place-0')['disease']['cohorts'], [])
        parent = self.fixture.commit(w, a, [shipment(to='place-0')])
        pending = w.snapshot()
        t = pending['economy']['transport']['transfers'][0]
        self.assertEqual((t['duration'],t['due']-t['departed_at']), (3600,3600))
        self.assertEqual(t['node'], next(q for q in pending['queue'] if q['kind']=='material_shipment'))
        packet = next(p for p in pending['contact_packets'] if p['id']==t['contact_packet_id'])
        self.assertEqual(packet['channel'], 'ship')
        self.assertEqual((packet['source_place_id'],packet['destination_place_id']), ('place-1','place-0'))
        self.assertEqual(packet['source_activity']['cohorts'], active)
        self.assertEqual(packet['source_activity']['active_count'], sum(x['count'] for x in active))
        self.assertEqual(packet['mechanism_sha256'], mechanism)
        self.assertEqual(packet['expires_at'], t['departed_at']+ttl)
        self.assertEqual(packet['cause_id'], t['cause_id'])
        pre_due = w.advance(t['due']-1)
        self.assertEqual(pre_due['economy']['materials']['place-0']['iron'], 20)
        self.assertNotIn('contact_applications', pre_due)
        final = w.advance(t['due'])
        transfer = final['economy']['transport']['transfers'][0]
        application = next(x for x in final['contact_applications'] if x['packet_id']==packet['id'])
        self.assertEqual((application['status'],application['new_infections']), (expected_status,expected_infections))
        self.assertEqual(application['arrived_at'],t['due'])
        self.assertEqual((transfer['status'],transfer['arrived_at']), ('arrived',t['due']))
        self.assertEqual(final['economy']['materials']['place-0']['iron'], 23)
        self.assertEqual(final['economy']['materials']['place-1']['iron'], 17)
        target = next(p for p in final['places'] if p['id']=='place-0')
        self.assertEqual(sum(x['count'] for x in target['disease']['cohorts'] if x['stage']=='latent'), expected_infections)
        if expected_status=='expired':
            self.assertEqual(t['due'],packet['expires_at'])
            self.assertIsNone(application['cohort_id'])
            self.assertEqual(target['disease']['cohorts'], [])
        else:
            self.assertLess(t['due'],packet['expires_at'])
            cohort = next(x for x in target['disease']['cohorts'] if x['id']==application['cohort_id'])
            self.assertEqual(cohort['contact_source']['packet_id'],packet['id'])
        events = [x for x in final['events'] if x['kind']=='material_shipment_arrived' and x.get('cause_id')==t['cause_id']]
        self.assertEqual(len(events),1)
        self.assertEqual(application['arrival_event_id'], events[0]['id'])
        notices = [x for x in w.observe('person-country-1-king')['observations'] if x['time']==t['due'] and '实际到达' in x['text'] and 'iron_unit' in x['text']]
        self.assertEqual(len(notices),1)
        self.assertEqual(notices[0]['source'],'亲见：'+target['name'])
        self.fixture.assert_accounts(w)
        rows = runtime.read_sql(w.db_path)
        self.assertTrue(all(json.loads(x['state'])['pathology']==declared and json.loads(x['state'])['pathology_mechanism_sha256']==mechanism for x in rows['snapshots']))
        w._mutate(lambda state,db:w._finish_material_transport(state,copy.deepcopy(t['node'])))
        self.assertEqual(runtime.read_sql(w.db_path),rows)
        (self.root/'expiry-asserted.json').write_text(json.dumps(dict(layer='unregistered real World.advance',controlled_new_world=True,ttl=ttl,initial=initial,before=before,packet=packet,transfer=transfer,application=application,parent=parent,notice=notices[0],final=final,public=rows),ensure_ascii=False,indent=2),encoding='utf-8')

    def test_controlled_new_world_expired_at_original_route_boundary(self):
        self.exercise_new_world(3600,'expired',0)

    def test_paired_new_world_unexpired_one_second_inside_original_route(self):
        self.exercise_new_world(3601,'applied',2)
