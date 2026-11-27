"""R: real registered native SQLite/Host/Coordinator; only model seams scripted.
No SDK, process, listener or simulated authority. Fictional game rates only.
"""
import copy
import json
import sqlite3
import unittest
import uuid
from pathlib import Path
from contextlib import closing
from app.world import WorldStore, WorldError, DAY
from app.adjudication import Adjudicator
from app.actors import ActorHost
from app.server import Coordinator
from app.coordinator_scheduler import ActivationRef
from app import world as world_module

SCRATCH = Path.home()/'AppData/Local/hermes/cache/scratch'
DATA = Path(world_module.__file__).with_name('data')
PID = 'person-country-1-finance'
RID = 'forge_agricultural_tool'
TEXT = ' 原话：请组织本地工班用现有铁炭木锻造农具。\n'


def configuration(capacity=8, iron=20):
    c=json.loads((DATA/'scenarios/iron-4.json').read_bytes())
    c.update(schema_version=3,scenario_id='p08-registered-production-v2',version='p08-registered-production-v2')
    del c['harvest_day'];del c['harvest_quantity']
    places=[p for country in c['countries'] for p in country['places']]
    c['economy']=dict(version='p08-production-economy-v1',assumptions='Fictional uncalibrated game rates.',
        units=dict(grain='grain_unit',iron='iron_unit',charcoal='charcoal_unit',timber='timber_unit',iron_tool='tool_unit'),
        initial_materials={p['id']:dict(iron=iron,charcoal=10,timber=20,iron_tool=2) for p in places},
        labor_hours_per_day={p['id']:capacity for p in places},
        recipes={RID:dict(inputs=dict(iron=2,charcoal=1,timber=1),outputs=dict(iron_tool=1),labor_hours=8,duration_seconds=DAY)})
    return c


def production():
    return dict(action='start_production',recipe_id=RID,quantity=1)


def read_sql(path):
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
        db.row_factory=sqlite3.Row
        names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {n:[dict(r) for r in db.execute('SELECT * FROM "'+n.replace('"','""')+'"')] for n in names}


class RegisteredProductionRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.root=SCRATCH/('p08-registered-owned-'+uuid.uuid4().hex)
        self.root.mkdir(parents=True,exist_ok=False)
        self.owner=None;self.host=None;self.world=None
        self.counts=dict(choose=0,request=0,dm=0,actual_sdk_requests=0)
        self.observations=[]

    def save(self,label,data):
        (self.root/(label+'.json')).write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')

    def build(self,c=None,effects=None,principal=PID,travel=False,provider_error=None):
        scenario=self.root/'scenario.json'
        scenario.write_text(json.dumps(configuration() if c is None else c,ensure_ascii=False),encoding='utf-8')
        self.world=WorldStore(self.root/'public.sqlite',scenario);self.world.initialize()
        self.adjudicator=Adjudicator(self.world)
        def dm(payload):
            self.counts['dm']+=1
            if provider_error is not None: raise provider_error
            state=payload['world']
            return dict(world_id=state['world_id'],base_revision=state['revision'],ruling='成功',reason='真实本地条件，脚本裁定',goal='制造',costs=[],benefits=[],effects=copy.deepcopy([production()] if effects is None else effects))
        self.host=ActorHost(self.world,self.root/'private.sqlite',adjudicator=self.adjudicator,dm_provider=dm)
        def forbidden_worker():raise AssertionError('SDK worker forbidden')
        self.host._start_worker=forbidden_worker
        def choose(view,seen):
            self.counts['choose']+=1
            body=dict(action='propose',text=TEXT) if view['person']['id']==principal else dict(action='wait')
            return body,seen
        def request(payload):
            self.counts['request']+=1
            return dict(messages=payload['messages']+[dict(role='user',content=payload['prompt'])],
                calls=[dict(call_id='call-'+payload['intent_id'])],sdk_events=0,sdk_requests=0,provider='scripted_callback',pid=0,module_root='no-sdk')
        self.host._choose=choose;self.host._request=request
        self.owner=Coordinator(self.world,self.host)
        self.principal=principal
        self.save('before',dict(public=read_sql(self.world.db_path),private=read_sql(self.host.db_path),counts=self.counts))

    def dispatch(self):
        self.owner.start();self.owner.worker.join(90)
        self.assertFalse(self.owner.worker.is_alive(),'owned coordinator must genuinely exit')
        self.save('dispatch',dict(error=self.owner.error,state=self.world.snapshot(),public=read_sql(self.world.db_path),private=read_sql(self.host.db_path),counts=self.counts))

    def activation(self):
        return next(r for r in read_sql(self.world.db_path)['coordinator_activations'] if r['person_id']==self.principal)

    def replay(self):
        row=self.activation();ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
        before=self.world.snapshot();counts=dict(self.counts)
        private=read_sql(self.host.db_path);heads=private['actor_session_heads']
        result=self.host.act(self.principal,coordinator=self.owner._owner_attempt.permit,activation=ref)
        self.assertEqual(self.counts,counts);self.assertEqual(self.world.snapshot(),before)
        self.assertEqual(read_sql(self.host.db_path)['actor_session_heads'],heads)
        self.assertEqual(result['intent_id'],row['intent_id'])
        self.save('replay',dict(result=result,counts=self.counts,public=read_sql(self.world.db_path),private=read_sql(self.host.db_path)))
        return result

    def tearDown(self):
        if self.owner is not None:self.owner.close()
        elif self.host is not None:self.host.close()
        if self.world is not None:
            private=read_sql(self.host.db_path) if self.host is not None else None
            self.save('closed',dict(owner_cleanup_pending=getattr(self.owner,'cleanup_pending',None),host_cleanup_pending=getattr(self.host,'cleanup_pending',None),public=read_sql(self.world.db_path),private=private,counts=self.counts))
        if self.owner is not None:self.assertFalse(self.owner.cleanup_pending)
        if self.host is not None:self.assertFalse(self.host.cleanup_pending)

    def test_registered_propose_production_closes_and_replays(self):
        self.build();self.dispatch()
        self.assertIsNone(self.owner.error,'registered typed production must commit; actual error='+str(self.owner.error))
        row=self.activation();self.assertEqual((row['phase'],row['private_closed']),('completed',1))
        public=read_sql(self.world.db_path);private=read_sql(self.host.db_path);state=self.world.snapshot()
        self.assertEqual(len(public['coordinator_scheduler_registration']),1)
        self.assertEqual([r['state'] for r in public['coordinator_windows']],['completed'])
        self.assertTrue(all(r['phase']=='completed' and r['private_closed']==1 for r in public['coordinator_activations']))
        self.assertEqual(len(public['coordinator_activations']),len(state['persons']))
        self.assertEqual(next(r['text'] for r in public['dm_proposals'] if r['intent_id']==row['intent_id']),TEXT)
        child=Adjudicator._child_id(row['intent_id'],1,0)
        task=state['economy']['tasks'][0]
        self.assertEqual((task['cause_id'],task['id'],task['owner_id'],task['status']),(child,'production-'+child,PID,'pending'))
        self.assertEqual(task['reserved_inputs'],dict(iron=2,charcoal=1,timber=1))
        self.assertEqual(task['labor_hours'],8)
        self.assertEqual(state['economy']['materials']['place-0'],dict(iron=18,charcoal=9,timber=19,iron_tool=2))
        self.assertTrue(all(v==0 for place in state['economy']['material_ledger']['consumed'].values() for v in place.values()))
        self.assertEqual(len([q for q in state['queue'] if q['kind']=='production' and q['cause_id']==child]),1)
        parent=self.world.receipt(PID,row['intent_id']);self.assertIsNotNone(parent)
        self.assertEqual(self.world.receipt(PID,child)['effect'],parent['effect']['effects'][0])
        meta=next(r for r in private['actor_intent_execution'] if r['intent_id']==row['intent_id'])
        self.assertEqual(meta['promoted_version'],meta['base_version']+1)
        self.assertEqual(self.replay()['receipt'],parent)

    def observe_execution(self):
        """Record actual delegated execution; never synthesize an effect or error."""
        execute = self.adjudicator._execute
        location = self.world._action_location
        def observed_location(state, person, action):
            item = dict(kind='action_location',action=action,person=copy.deepcopy(person),
                        time=state['time'],queue=copy.deepcopy(state['queue']))
            try:
                value = location(state, person, action)
                item['returned_place'] = value['id']
                return value
            except BaseException as error:
                item.update(error_type=type(error).__name__,code=getattr(error,'code',None))
                raise
            finally:
                self.observations.append(item)
        def observed_execute(state, db, person, intent, version, decision, persist):
            item = dict(kind='execute',persist=persist,person_id=person,intent_id=intent,
                        version=version,decision=copy.deepcopy(decision),before=copy.deepcopy(state))
            try:
                value = execute(state,db,person,intent,version,decision,persist)
                item['returned'] = copy.deepcopy(value)
                return value
            except BaseException as error:
                item.update(error_type=type(error).__name__,code=getattr(error,'code',None),
                            action=getattr(error,'action',None),effect_index=getattr(error,'effect_index',None))
                raise
            finally:
                item['after'] = copy.deepcopy(state)
                item['child_rows_before_savepoint_exit'] = [dict(r) for r in db.execute(
                    'SELECT * FROM intents WHERE id IN ('+','.join('?' for _ in decision['effects'])+')',
                    tuple(Adjudicator._child_id(intent,version,i) for i in range(len(decision['effects']))))]
                self.observations.append(item)
                self.save('effect-observations',self.observations)
        self.world._action_location = observed_location
        self.adjudicator._execute = observed_execute

    def assert_rejected(self, code, index=0):
        from app.actor_execution import digest
        from app.world import DomainActionError
        self.assertIsNone(self.owner.error)
        row = self.activation();intent = row['intent_id']
        public = read_sql(self.world.db_path);private = read_sql(self.host.db_path)
        self.assertEqual((row['phase'],row['private_closed']),('rejected',1))
        self.assertIsNone(row['receipt_sha256']);self.assertIsNone(row['promoted_version'])
        self.assertEqual([w['state'] for w in public['coordinator_windows']],['completed'])
        self.assertTrue(all(a['private_closed']==1 and a['phase'] in ('completed','rejected') for a in public['coordinator_activations']))
        self.assertEqual(len(public['coordinator_activations']),len(self.world.snapshot()['persons']))
        saved = next(r for r in public['coordinator_activation_outcomes'] if r['intent_id']==intent)
        outcome = json.loads(saved['outcome_json'])
        self.assertEqual(digest(saved['outcome_json']),saved['outcome_sha256'])
        self.assertEqual((outcome['status'],outcome['code'],outcome['origin'],outcome['action'],outcome['effect_index']),
                         ('domain_rejected',code,'adjudication.execute','start_production',index))
        self.assertEqual(outcome['effects'],[])
        decision = next(r for r in public['dm_decisions'] if r['intent_id']==intent)
        self.assertEqual(outcome['decision_version'],decision['version'])
        self.assertEqual(outcome['decision_sha256'],digest(decision['data']))
        self.assertEqual(next(r['text'] for r in public['dm_proposals'] if r['intent_id']==intent),TEXT)
        main = next(r for r in public['intents'] if r['id']==intent)
        leaf = next(r for r in private['actor_intents'] if r['intent_id']==intent)
        meta = next(r for r in private['actor_intent_execution'] if r['intent_id']==intent)
        head = next(r for r in private['actor_session_heads'] if r['person_id']==self.principal)
        self.assertEqual((leaf['state'],leaf['receipt'],main['receipt']),('rejected',None,None))
        self.assertIsNotNone(leaf['candidate']);self.assertIsNone(meta['promoted_version'])
        self.assertEqual((head['version'],head['last_promoted_intent_id']),(meta['base_version'],None))
        self.assertEqual(head['version'],0)
        self.assertEqual((meta['base_messages_sha256'],meta['base_seen_sha256']),
                         (head['messages_sha256'],head['seen_sha256']))
        self.assertEqual(json.loads(leaf['request'])['body'],dict(action='propose',text=TEXT))
        self.assertEqual(digest(leaf['request']),outcome['request_sha256'])
        self.assertEqual(digest(leaf['candidate']),outcome['candidate_sha256'])
        self.assertEqual(digest(main['payload']),outcome['payload_sha256'])
        self.assertEqual(len([r for r in private['actor_calls'] if r['intent_id']==intent]),1)
        self.assertFalse(any(r['intent_id']==intent and r['kind']=='commit' for r in public['dm_timeline']))
        for i in range(len(json.loads(decision['data'])['effects'])):
            self.assertFalse(any(r['id']==Adjudicator._child_id(intent,decision['version'],i) for r in public['intents']))
        self.assertIsNone(self.world.receipt(self.principal,intent))
        real = [r for r in self.observations if r['kind']=='execute' and r['persist']]
        self.assertEqual(len(real),1)
        self.assertEqual((real[0]['error_type'],real[0]['code'],real[0]['effect_index']),
                         (DomainActionError.__name__,code,index))
        initial = real[0]['before'];state = self.world.snapshot()
        self.assertEqual(state['economy'],initial['economy'])
        self.assertEqual(state['queue'],initial['queue'])
        self.assertEqual(next(p for p in state['persons'] if p['id']==self.principal)['location'],
                         next(p for p in initial['persons'] if p['id']==self.principal)['location'])
        children = {Adjudicator._child_id(intent,decision['version'],i) for i in range(len(json.loads(decision['data'])['effects']))}
        self.assertFalse(any(e.get('cause_id') in children for e in state['events']))
        replay = self.replay()
        self.assertEqual(replay['outcome'],outcome);self.assertIsNone(replay['receipt'])
        self.assertTrue(replay['reconciled'])
        self.save('rejection-assertions',dict(outcome=outcome,activation=row,head=head,execution=meta,
                  real_producer=real[0],synthetic_metrics=dict(self.counts),actual_sdk_requests=0))
        return outcome,real[0]

    def rejection_case(self, code, c=None, effects=None, principal=PID, index=0):
        self.build(c=c,effects=effects,principal=principal)
        self.observe_execution();self.dispatch()
        return self.assert_rejected(code,index)

    def test_registered_material_shortage_is_real_rejection(self):
        self.rejection_case('INSUFFICIENT_MATERIALS',c=configuration(iron=1))

    def test_registered_low_base_capacity_is_real_rejection(self):
        self.rejection_case('INSUFFICIENT_LABOR',c=configuration(capacity=7))

    def test_registered_king_is_not_authorized(self):
        self.rejection_case('NOT_AUTHORIZED',principal='person-country-1-king')

    def test_registered_marshal_is_not_authorized(self):
        self.rejection_case('NOT_AUTHORIZED',principal='person-country-1-marshal')

    def test_registered_tentative_travel_then_production_rolls_back(self):
        # A real transient in-flight state in a failed compound transaction;
        # deliberately NOT a committed multi-day journey/recovery experiment.
        c=configuration()
        target=next(p['id'] for p in c['countries'][0]['places'] if p['id']!='place-0')
        outcome,observed=self.rejection_case('NOT_AT_LOCATION',c=c,
            effects=[dict(action='travel',to=target),production()],index=1)
        location = [r for r in self.observations if r['kind']=='action_location'
                    and r['action']=='start_production' and r.get('code')=='NOT_AT_LOCATION']
        self.assertEqual(len(location),2)  # readonly preview and the actual write region
        for r in location:
            self.assertIsNone(r['person']['location']);self.assertGreater(r['person']['journey_version'],0)
            nodes=[q for q in r['queue'] if q['kind']=='arrival' and q.get('person_id')==self.principal]
            self.assertEqual(len(nodes),1);self.assertEqual(nodes[0]['destination'],target)
            self.assertGreater(nodes[0]['at'],r['time'])
        self.assertTrue(any(r['receipt'] is not None for r in observed['child_rows_before_savepoint_exit']))

    def test_registered_second_production_rolls_back_first_reservation(self):
        outcome,observed=self.rejection_case('INSUFFICIENT_MATERIALS',c=configuration(capacity=16,iron=3),
                                             effects=[production(),production()],index=1)
        before=observed['before'];tentative=observed['after']
        self.assertEqual(len(before['economy']['tasks']),0)
        self.assertEqual(len(tentative['economy']['tasks']),1)
        task=tentative['economy']['tasks'][0]
        self.assertEqual(task['reserved_inputs'],dict(iron=2,charcoal=1,timber=1))
        self.assertEqual(task['labor_hours'],8)
        self.assertEqual(tentative['economy']['materials']['place-0']['iron'],1)
        self.assertEqual(len([q for q in tentative['queue'] if q['kind']=='production']),1)
        self.assertEqual(len(observed['child_rows_before_savepoint_exit']),2)
        self.assertEqual(sum(r['receipt'] is not None for r in observed['child_rows_before_savepoint_exit']),1)
        self.assertEqual(len([e for e in tentative['events'] if e['kind']=='production_started']),1)

    def test_registered_rejected_replay_after_real_object_reopen(self):
        outcome,observed=self.rejection_case('INSUFFICIENT_MATERIALS',c=configuration(iron=1))
        original_row=self.activation();original_state=self.world.snapshot()
        original_private=read_sql(self.host.db_path);counts=dict(self.counts)
        choose,request,dm=self.host._choose,self.host._request,self.host.dm_provider
        self.owner.close();self.assertFalse(self.owner.cleanup_pending);self.assertFalse(self.host.cleanup_pending)
        self.world=WorldStore(self.root/'public.sqlite',self.root/'scenario.json')
        self.host=ActorHost(self.world,self.root/'private.sqlite')
        def forbidden_worker():raise AssertionError('SDK worker forbidden')
        self.host._start_worker=forbidden_worker
        self.host._choose,self.host._request=choose,request
        self.owner=Coordinator(self.world,self.host)
        # Genuine new owner and original prepare, without start(): start would
        # legitimately open the next day's new window. Constructor DDL also
        # requires the real new root once registration already exists.
        self.owner.acquire_owner()
        with self.owner._owner_scope():
            self.adjudicator=Adjudicator(self.world)
        self.host.adjudicator,self.host.dm_provider=self.adjudicator,dm
        self.owner._prepare_scheduler(self.owner._owner_attempt)
        result=self.replay()
        self.assertEqual(result['outcome'],outcome);self.assertEqual(self.counts,counts)
        business_fields=('world_id','window_id','activation_id','person_id','intent_id','phase','private_closed',
                         'request_sha256','candidate_sha256','receipt_sha256','promoted_version','terminal_reason')
        self.assertEqual({k:self.activation()[k] for k in business_fields},{k:original_row[k] for k in business_fields})
        self.assertEqual(self.world.snapshot(),original_state)
        self.assertEqual(read_sql(self.host.db_path)['actor_session_heads'],original_private['actor_session_heads'])
        self.save('reopened-replay',dict(result=result,public=read_sql(self.world.db_path),private=read_sql(self.host.db_path)))

    def technical_case(self, effects=None, c=None, provider_error=None, raw=False):
        self.build(c=c,effects=effects,provider_error=provider_error)
        if raw:
            self.host._choose=lambda view,seen: (production() if view['person']['id']==PID else dict(action='wait'),seen)
        self.observe_execution();self.dispatch()
        row=self.activation();public=read_sql(self.world.db_path);private=read_sql(self.host.db_path)
        self.assertIsNotNone(self.owner.error)
        self.assertEqual(row['phase'],'technical_blocked')
        self.assertEqual(self.world.snapshot()['status'],'technical_paused')
        self.assertFalse(any(r['intent_id']==row['intent_id'] for r in public.get('coordinator_activation_outcomes',[])))
        self.assertFalse(any(r['phase']=='rejected' for r in public['coordinator_activations']))
        self.assertFalse(self.world.snapshot().get('economy',{}).get('tasks',[]))
        self.assertIsNone(self.world.receipt(PID,row['intent_id']))
        self.assertFalse(any(r.get('promoted_version') is not None for r in private['actor_intent_execution'] if r['person_id']==PID))
        self.assertFalse(any(r.get('error_type')=='DomainActionError' for r in self.observations))
        self.save('technical-assertions',dict(error=self.owner.error,activation=row,observations=self.observations))

    def test_registered_bad_typed_keys_remain_technical(self):
        self.technical_case(effects=[dict(production(),unexpected=1)])

    def test_registered_bool_quantity_remains_technical(self):
        self.technical_case(effects=[dict(production(),quantity=True)])

    def test_registered_unknown_recipe_remains_technical(self):
        self.technical_case(effects=[dict(production(),recipe_id='unknown_recipe')])

    def test_registered_schema2_production_remains_technical(self):
        self.technical_case(c=json.loads((DATA/'scenarios/iron-4.json').read_bytes()))

    def test_registered_provider_material_code_is_not_business_rejection(self):
        self.technical_case(provider_error=WorldError('INSUFFICIENT_MATERIALS','provider synthetic error, not a producer'))

    def test_registered_provider_labor_code_is_not_business_rejection(self):
        self.technical_case(provider_error=WorldError('INSUFFICIENT_LABOR','provider synthetic error, not a producer'))

    def test_registered_provider_authorization_code_is_not_business_rejection(self):
        self.technical_case(provider_error=WorldError('NOT_AUTHORIZED','provider synthetic error, not a producer'))

    def test_registered_provider_location_code_is_not_business_rejection(self):
        self.technical_case(provider_error=WorldError('NOT_AT_LOCATION','provider synthetic error, not a producer'))

    def test_registered_raw_actor_production_stays_invalid(self):
        self.rejection_case('INSUFFICIENT_MATERIALS',c=configuration(iron=1))
        before=self.world.snapshot();public=read_sql(self.world.db_path);private=read_sql(self.host.db_path)
        with self.owner._owner_scope(),self.assertRaises(WorldError) as caught:
            self.world.submit(PID,'raw-production-'+uuid.uuid4().hex,production())
        self.assertEqual(caught.exception.code,'INVALID_BODY')
        self.assertEqual(self.world.snapshot(),before)
        self.assertEqual(read_sql(self.world.db_path)['intents'],public['intents'])
        self.assertEqual(read_sql(self.host.db_path),private)
        self.save('raw-api-denied',dict(code=caught.exception.code,public=read_sql(self.world.db_path)))
