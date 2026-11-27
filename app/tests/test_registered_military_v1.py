"""966 registered route: real Host/Coordinator/SQL, only provider seams scripted.
Prospective, unexecuted. No SDK/network claim; cleanup is owned by each test.
"""
import copy
import json
import unittest
import uuid
from pathlib import Path
from app.world import WorldStore
from app.actors import ActorHost
from app.adjudication import Adjudicator
from app.server import Coordinator
from app.coordinator_scheduler import ActivationRef
from app.tests.test_military_v1 import configuration, army, MARSHAL, SCRATCH
from app.tests.test_registered_production_runtime_v1 import read_sql


class RegisteredMilitaryTests(unittest.TestCase):
    def setUp(self):
        self.root = SCRATCH/('parent966-registered-military-'+uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.owner = self.host = None
        self.calls = dict(choose=0, request=0, dm=0)
        self.views = []

    def build(self, effects):
        path = self.root/'scenario.json'
        path.write_text(json.dumps(configuration(), ensure_ascii=False), encoding='utf-8')
        self.world = WorldStore(self.root/'world.sqlite', path); self.world.initialize()
        self.adjudicator = Adjudicator(self.world)
        def dm(context):
            self.calls['dm'] += 1; state = context['world']
            return dict(world_id=state['world_id'], base_revision=state['revision'], ruling='成功',
                reason='现场兵粮限制', goal='驻防补给', costs=[], benefits=[], effects=copy.deepcopy(effects))
        self.host = ActorHost(self.world, self.root/'private.sqlite', adjudicator=self.adjudicator, dm_provider=dm)
        def choose(view, seen):
            self.calls['choose'] += 1; self.views.append(copy.deepcopy(view))
            return (dict(action='propose', text='我选择以自己的在场部队执行军事行动。')
                    if view['person']['id']==MARSHAL else dict(action='wait')), seen
        def request(payload):
            self.calls['request'] += 1
            return dict(messages=payload['messages'] + [dict(role='user', content=payload['prompt'])],
                calls=[dict(call_id='call-'+payload['intent_id'])], sdk_events=0, sdk_requests=0,
                provider='scripted_callback', pid=0, module_root='no-sdk')
        def no_sdk(): raise AssertionError('No SDK authorized by this test')
        self.host._choose = choose; self.host._request = request; self.host._start_worker = no_sdk
        self.owner = Coordinator(self.world, self.host)
        self.before = self.world.snapshot()

    def dispatch(self):
        self.owner.start(); self.owner.worker.join(90)
        self.assertFalse(self.owner.worker.is_alive())
        self.assertIsNone(self.owner.error, repr(self.owner.error))
        rows = read_sql(self.world.db_path)
        self.row = next(r for r in rows['coordinator_activations'] if r['person_id']==MARSHAL)
        self.assertTrue(all(r['private_closed']==1 for r in rows['coordinator_activations']))
        self.assertEqual(len(rows['coordinator_scheduler_registration']), 1)
        self.assertEqual([w['state'] for w in rows['coordinator_windows']], ['completed'])
        return rows

    def assert_replay_databases(self, before, after, private_before, private_after):
        """Business bytes stay exact; prove, not ignore, each lease transition.

        Host.act acquires/releases an actor capability even for terminal replay.
        claim inserts two lineage rows and updates activation/window ownership;
        BEFORE triggers advance main generation four times. adopt's existing
        INSERT OR IGNORE session advances private generation once. No business
        row is exempt. A live coordinator may renew, never change its identity.
        """
        expected = copy.deepcopy(before)
        self.assertEqual(set(after), set(before))
        person = self.row['person_id']; world_id = self.row['world_id']
        def one(rows, **keys):
            found = [r for r in rows if all(r[k] == v for k,v in keys.items())]
            self.assertEqual(len(found), 1)
            return found[0]
        old = one(before['actor_execution_leases'], world_id=world_id, person_id=person)
        new = one(after['actor_execution_leases'], world_id=world_id, person_id=person)
        self.assertIsNone(old['owner_token']); self.assertIsNone(new['owner_token'])
        self.assertEqual(new['epoch'], old['epoch'] + 1)
        self.assertEqual(new['owner_instance'], self.host._owner_instance)
        self.assertGreaterEqual(new['acquired_wall_ns'], old['last_wall_ns'])
        self.assertGreaterEqual(new['last_wall_ns'], new['acquired_wall_ns'])
        self.assertEqual(new['expires_wall_ns'] - new['last_wall_ns'],
                         self.host.execution_policy.ttl_seconds * 1_000_000_000)
        wanted = one(expected['actor_execution_leases'], world_id=world_id, person_id=person)
        for k in ('epoch','owner_instance','acquired_wall_ns','last_wall_ns','expires_wall_ns'):
            wanted[k] = new[k]
        permit = self.owner._owner_attempt.permit
        parent_old = one(before['coordinator_execution_leases'], world_id=world_id)
        parent_new = one(after['coordinator_execution_leases'], world_id=world_id)
        self.assertEqual((parent_new['epoch'],parent_new['owner_token']), (permit.epoch,permit.owner_token))
        self.assertGreaterEqual(parent_new['last_wall_ns'], parent_old['last_wall_ns'])
        self.assertEqual(parent_new['expires_wall_ns']-parent_new['last_wall_ns'],
                         self.owner.execution_policy.ttl_seconds * 1_000_000_000)
        parent_expected = one(expected['coordinator_execution_leases'], world_id=world_id)
        for k in ('last_wall_ns','expires_wall_ns'): parent_expected[k] = parent_new[k]
        expected['actor_coordinator_delegations'].append(dict(world_id=world_id,person_id=person,
            actor_epoch=new['epoch'],parent_epoch=permit.epoch,parent_owner_token=permit.owner_token))
        expected['actor_activation_delegations'].append(dict(world_id=world_id,person_id=person,
            actor_epoch=new['epoch'],window_id=self.row['window_id'],activation_id=self.row['activation_id']))
        active = one(expected['coordinator_activations'], world_id=world_id,activation_id=self.row['activation_id'])
        active['owner_actor_epoch'] = new['epoch']; active['owner_parent_epoch'] = permit.epoch
        window = one(expected['coordinator_windows'], world_id=world_id,window_id=self.row['window_id'])
        window['owner_parent_epoch'] = permit.epoch
        floor = after['execution_clock'][0]['high_water_wall_ns']
        self.assertIs(type(floor), int)
        self.assertGreaterEqual(floor, before['execution_clock'][0]['high_water_wall_ns'])
        self.assertGreaterEqual(floor, max(new['last_wall_ns'],parent_new['last_wall_ns']))
        expected['execution_clock'][0]['high_water_wall_ns'] = floor
        expected['history_proof_epochs'][0]['generation'] += 4
        self.assertEqual(after, expected)
        private_expected = copy.deepcopy(private_before)
        private_expected['history_proof_epochs'][0]['generation'] += 1
        self.assertEqual(private_after, private_expected)
        self.assertFalse(self.host.cleanup_pending)
        self.assertIsNone(self.host._attempt)

    def replay(self):
        state = self.world.snapshot(); rows = read_sql(self.world.db_path)
        private = read_sql(self.host.db_path); calls = dict(self.calls)
        ref = ActivationRef(**{k:self.row[k] for k in ('world_id','window_id','activation_id','person_id')})
        result = self.host.act(MARSHAL, coordinator=self.owner._owner_attempt.permit, activation=ref)
        self.assertEqual(self.calls, calls); self.assertEqual(self.world.snapshot(), state)
        self.assert_replay_databases(rows, read_sql(self.world.db_path),
                                     private, read_sql(self.host.db_path))
        return result

    def tearDown(self):
        if self.owner is not None:
            self.owner.close(); self.assertFalse(self.owner.cleanup_pending)
        elif self.host is not None: self.host.close()
        if self.host is not None: self.assertFalse(self.host.cleanup_pending)

    def test_registered_real_military_proposal_commit_original_receipt_and_local_projection(self):
        self.build([dict(action='military_supply', army_id='army-1', quantity=7),
                    dict(action='military_garrison', army_id='army-1')])
        rows = self.dispatch(); state = self.world.snapshot()
        self.assertEqual((self.row['phase'],self.row['private_closed']), ('completed',1))
        self.assertEqual(state['time'], 86400)
        self.assertEqual((army(state)['grain'],army(state)['stance']), (2000 - 12 + 7,'garrison'))
        parent = self.world.receipt(MARSHAL,self.row['intent_id'])
        self.assertEqual(len(parent['effect']['effects']), 2)
        for index,effect in enumerate(parent['effect']['effects']):
            child = self.adjudicator._child_id(self.row['intent_id'],1,index)
            self.assertEqual(self.world.receipt(MARSHAL,child)['effect'],effect)
        self.assertEqual(self.replay()['receipt'],parent)
        local = next(v for v in self.views if v['person']['id']==MARSHAL)
        remote = next(v for v in self.views if v['person']['id']=='person-country-1-king')
        self.assertIn('当场点验兵力12人',json.dumps(local,ensure_ascii=False))
        self.assertNotIn('当场点验兵力12人',json.dumps(remote,ensure_ascii=False))
        self.assertEqual(self.calls['dm'],1)

    def test_registered_compound_supply_then_foreign_army_denies_and_rolls_back_all(self):
        self.build([dict(action='military_supply', army_id='army-1', quantity=7),
                    dict(action='military_supply', army_id='army-2', quantity=1)])
        rows = self.dispatch()
        self.assertEqual((self.row['phase'],self.row['private_closed']),('rejected',1))
        outcome = next(r for r in rows['coordinator_activation_outcomes'] if r['intent_id']==self.row['intent_id'])
        result = json.loads(outcome['outcome_json'])
        self.assertEqual((result['code'],result['action'],result['effect_index']),('NOT_AUTHORIZED','military_supply',1))
        self.assertEqual(result['effects'],[])
        advanced = rows['coordinator_windows'][0]['advanced_revision']
        self.assertEqual(self.world.snapshot()['military'],self.world.snapshot(advanced)['military'])
        self.assertIsNone(self.world.receipt(MARSHAL,self.row['intent_id']))
        self.assertEqual(self.replay()['outcome'],result)
        for index in (0,1):
            self.assertFalse(any(r['id']==self.adjudicator._child_id(self.row['intent_id'],1,index) for r in rows['intents']))

    def test_registered_original_military_receipt_recovers_with_new_owned_objects(self):
        self.build([dict(action='military_supply', army_id='army-1', quantity=3)])
        self.dispatch(); receipt = self.world.receipt(MARSHAL,self.row['intent_id'])
        state = self.world.snapshot(); calls = dict(self.calls)
        choose,request,dm = self.host._choose,self.host._request,self.host.dm_provider
        self.owner.close(); self.assertFalse(self.owner.cleanup_pending)
        self.world = WorldStore(self.root/'world.sqlite',self.root/'scenario.json')
        self.host = ActorHost(self.world,self.root/'private.sqlite')
        def no_sdk(): raise AssertionError('No SDK authorized by this test')
        self.host._start_worker = no_sdk; self.host._choose,self.host._request = choose,request
        self.owner = Coordinator(self.world,self.host); self.owner.acquire_owner()
        with self.owner._owner_scope(): self.adjudicator = Adjudicator(self.world)
        self.host.adjudicator,self.host.dm_provider = self.adjudicator,dm
        self.owner._prepare_scheduler(self.owner._owner_attempt)
        self.assertEqual(self.replay()['receipt'],receipt)
        self.assertEqual(self.world.snapshot(),state); self.assertEqual(self.calls,calls)

    def test_registered_march_preserves_real_queue_node_for_window_clock(self):
        self.build([dict(action='military_move', army_id='army-1', to='place-10')])
        self.dispatch(); state = self.world.snapshot(); moving = army(state)
        self.assertIsNone(moving['location']); self.assertEqual(moving['stance'],'marching')
        trip = moving['journey']; nodes = [q for q in state['queue'] if q['kind']=='military_arrival']
        self.assertEqual(nodes,[trip['node']]); self.assertGreater(nodes[0]['at'],state['time'])
        self.assertEqual(state['military']['control']['place-10'],'country-2')
        self.assertEqual(next(p['location'] for p in state['persons'] if p['id']==MARSHAL),'place-1')
        self.replay()


if __name__ == '__main__': unittest.main()
