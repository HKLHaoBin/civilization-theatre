"""P04 source-only regression definitions. NOT RUN; no SDK or real model claim.
Run as a complete module only in an explicitly admitted candidate root.
Each method owns an exclusive retained scratch directory; never opens old DBs.
"""
import copy
import json
import sqlite3
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from app import world as world_module
from app.world import WorldStore, WorldError, DAY
from app.actors import ActorHost
from app.server import Coordinator
from app.coordinator_scheduler import ActivationRef

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
KING = 'person-country-1-king'
FINANCE = 'person-country-1-finance'
TEMP = 'p04-temporary-envoy'
TEXT = '只告知被实际联系者：请核查现场。'
PRIVATE = '常驻君主从未传出的私人计划：只保留在本人会话中。'


def configuration():
    c = json.loads((Path(world_module.__file__).with_name('data')/'scenarios/iron-4.json').read_bytes())
    c['scenario_id'] = 'parent1043-event-wakeup-v1'
    c['version'] = 'parent1043-event-wakeup-v1'
    c['local_message_seconds'] = 60
    c['investigation_seconds'] = 30
    c['event_wakeup'] = dict(version='p04-event-wakeup-v1', government_interval_seconds=DAY,
        temporary_steps=3, temporary_persons=[dict(id=TEMP, name='本地临时使者',
        country_id='country-1', office='使者', location='place-0',
        card=dict(values='重视亲见',interests='维护本人声誉',risk='谨慎',mission='条件允许时推进文明'))])
    return c


class EventWakeupTests(unittest.TestCase):
    def setUp(self):
        self.root = SCRATCH/('parent1043-owned-'+uuid.uuid4().hex)
        self.root.mkdir(parents=True, exist_ok=False)
        self.world = self.host = self.owner = None
        self.views = []; self.requests = []; self.windows = []
        self.policy = lambda view, seen: (dict(action='wait'), seen)

    def save(self, name, value):
        (self.root/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')

    def rows(self, path, table):
        self.assertIn(table, {'coordinator_windows','coordinator_activations','intents','actor_intents','actor_session_heads','actor_execution_leases','coordinator_execution_leases'})
        with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory = sqlite3.Row
            return [dict(r) for r in db.execute('SELECT * FROM '+table)]

    def raw(self):
        with closing(sqlite3.connect(self.world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            return json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])

    def build(self, c=None, dm=None):
        c = configuration() if c is None else c
        (self.root/'scenario.json').write_text(json.dumps(c,ensure_ascii=False),encoding='utf-8')
        self.world = WorldStore(self.root/'public.sqlite', self.root/'scenario.json')
        self.world.initialize()
        if dm is None:
            self.host = ActorHost(self.world, self.root/'private.sqlite')
        else:
            from app.adjudication import Adjudicator
            self.host = ActorHost(self.world, self.root/'private.sqlite',
                adjudicator=Adjudicator(self.world), dm_provider=dm)
        def forbidden_worker():
            raise AssertionError('P04 regression forbids SDK worker')
        def choose(view, seen):
            self.views.append(copy.deepcopy(view))
            return self.policy(view, seen)
        def request(payload):
            self.requests.append(copy.deepcopy(payload))
            tail = [dict(role='user',content=payload['prompt']),
                    dict(role='assistant',content=PRIVATE if payload['person_id']==KING else '本人的有限回应')]
            return dict(messages=payload['messages']+tail,calls=[dict(call_id='p04-'+payload['intent_id'])],
                sdk_events=0,sdk_requests=0,provider='p04-controlled-callback',pid=0,module_root='no-sdk')
        self.host._start_worker = forbidden_worker
        self.host._choose = choose
        self.host._request = request
        self.owner = Coordinator(self.world,self.host)

    def step(self):
        self.owner.start()
        self.owner.worker.join(60)
        self.assertFalse(self.owner.worker.is_alive(), 'original whole step(60) did not finish; no renewal')
        self.assertIsNone(self.owner.error)
        windows = self.rows(self.world.db_path,'coordinator_windows')
        w = max(windows,key=lambda x:x['ordinal'])
        self.assertEqual(w['state'],'completed')
        self.windows.append(copy.deepcopy(w))
        self.save('windows',self.windows)
        return w, self.world.snapshot()

    def roster(self, window):
        return json.loads(window['roster_json'])

    def assert_closure(self):
        actual = self.rows(self.world.db_path,'coordinator_activations')
        leaves = self.rows(self.host.db_path,'actor_intents')
        intents = self.rows(self.world.db_path,'intents')
        self.assertEqual({a['intent_id'] for a in actual},{r['intent_id'] for r in leaves})
        for a in actual:
            self.assertEqual((a['phase'],a['private_closed']),('completed',1))
            leaf = next(r for r in leaves if r['intent_id']==a['intent_id'])
            receipt = next(r for r in intents if r['id']==a['intent_id'])
            self.assertEqual(json.loads(leaf['receipt']),json.loads(receipt['receipt']))
            self.assertEqual(a['promoted_version'],a['base_session_version']+1)
        self.assertFalse(self.host.cleanup_pending)

    def tearDown(self):
        if self.owner is not None:
            self.owner.close()
            self.assertFalse(self.owner.cleanup_pending)
        elif self.host is not None:
            self.host.close()
        if self.host is not None:
            self.assertFalse(self.host.cleanup_pending)
        self.save('views',self.views); self.save('requests',self.requests)
        if self.world is not None:
            self.save('final',self.world.snapshot())

    def test_delivered_message_wakes_only_recipient_at_actual_subday_time(self):
        self.build()
        def policy(v, seen):
            return (dict(action='send',to=TEMP,text=TEXT) if v['person']['id']==KING and v['time']==0 else dict(action='wait')),seen
        self.policy = policy
        first, before = self.step()
        self.assertEqual((first['target_time'],len(self.roster(first))),(0,12))
        self.assertNotIn(TEMP,self.roster(first))
        self.assertEqual(self.world.observe(TEMP)['messages'],[])
        self.assertFalse(any(r['person_id']==TEMP for r in self.requests))
        second, after = self.step()
        self.assertEqual(second['target_time'],60)
        self.assertEqual(self.roster(second),[TEMP])
        message = self.world.observe(TEMP)['messages']
        self.assertEqual([(m['time'],m['text'],m['from']) for m in message],[(60,TEXT,KING)])
        received = next(v for v in self.views if v['person']['id']==TEMP)
        self.assertEqual(received['messages'],message)
        self.assertIn(TEXT,next(r['prompt'] for r in self.requests if r['person_id']==TEMP))
        for p in after['persons']:
            if p['id']!=TEMP:
                self.assertFalse(any(m['text']==TEXT for m in self.world.observe(p['id'])['messages']))
        self.assertEqual(after['time'],60)
        self.assert_closure()

    def test_temporary_observe_contact_then_wait_sleeps_and_real_task_reawakens(self):
        self.build()
        turns = []
        def policy(v, seen):
            if v['person']['id']==KING and v['time']==0:
                return dict(action='send',to=TEMP,text=TEXT),seen
            if v['person']['id']==TEMP:
                turns.append(v['time'])
                body = [dict(action='investigate'),dict(action='send',to=KING,text='本人主动回信'),dict(action='wait')]
                return (body[len(turns)-1] if len(turns)<=3 else dict(action='wait')),seen
            return dict(action='wait'),seen
        self.policy = policy
        self.step()
        for _ in range(3):
            w,state = self.step()
            self.assertEqual((w['target_time'],self.roster(w)),(60,[TEMP]))
        asleep = self.raw()['_p04']
        self.assertEqual(asleep['remaining'][TEMP],0)
        self.assertNotIn(TEMP,asleep['pending'])
        w,state = self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(90,[TEMP]))
        self.assertEqual(turns,[60,60,60,90])
        obs = self.world.observe(TEMP)['observations']
        self.assertEqual(len([o for o in obs if o['time']==90 and o['source'].startswith('现场调查：')]),1)
        self.assertEqual(len([e for e in state['events'] if e['kind']=='investigation_completed' and e['person_id']==TEMP]),1)
        w,state = self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(120,[KING]))
        self.assertTrue(any(m['text']=='本人主动回信' for m in self.world.observe(KING)['messages']))
        self.assertEqual(len([r for r in self.requests if r['person_id']==TEMP]),4)
        self.assert_closure()

    def test_no_wait_choice_still_exhausts_exact_finite_window(self):
        self.build()
        self.policy = lambda v,s: ((dict(action='send',to=TEMP,text=TEXT) if v['person']['id']==KING and v['time']==0 else
                                  dict(action='send',to=KING,text='有限主动联系') if v['person']['id']==TEMP else dict(action='wait')),s)
        self.step()
        for _ in range(3):
            w,state = self.step()
            self.assertEqual((w['target_time'],self.roster(w)),(60,[TEMP]))
        self.assertEqual(self.raw()['_p04']['remaining'][TEMP],0)
        self.assertNotIn(TEMP,self.raw()['_p04']['pending'])
        w,state = self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(120,[KING]))
        self.assertEqual(len([r for r in self.requests if r['person_id']==TEMP]),3)
        self.assertEqual(len([m for m in self.world.observe(KING)['messages'] if m['text']=='有限主动联系']),3)
        self.assert_closure()

    def test_actual_local_meeting_wakes_target_without_giving_bystanders_words(self):
        self.build()
        self.policy = lambda v,s: ((dict(action='meet',to=TEMP,text=TEXT) if v['person']['id']==KING and v['time']==0 else dict(action='wait')),s)
        self.step()
        self.assertFalse(any(r['person_id']==TEMP for r in self.requests))
        w,state = self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(0,[TEMP]))
        meeting = [o for o in self.world.observe(TEMP)['observations'] if o['source']=='实际会面']
        self.assertEqual([o['text'] for o in meeting],[TEXT])
        self.assertIn(TEXT,next(r['prompt'] for r in self.requests if r['person_id']==TEMP))
        self.assertNotIn(TEXT,json.dumps(self.world.observe(FINANCE),ensure_ascii=False))
        self.assertEqual(len([e for e in state['events'] if e['kind']=='meeting']),1)
        self.assert_closure()

    def test_remote_meeting_rejected_by_real_producer_and_no_knowledge_or_intent_override(self):
        c=configuration();c['event_wakeup']['temporary_persons'][0]['location']='place-1'
        self.build(c)
        self.policy=lambda v,s: ((dict(action='meet',to=TEMP,text=TEXT) if v['person']['id']==KING else dict(action='wait')),s)
        w,state=self.step()
        row=next(a for a in self.rows(self.world.db_path,'coordinator_activations') if a['person_id']==KING)
        self.assertEqual((row['phase'],row['private_closed'],row['terminal_reason']),('rejected',1,'NOT_AT_LOCATION'))
        self.assertFalse(any(e['kind']=='meeting' for e in state['events']))
        self.assertNotIn(TEXT,json.dumps(self.world.observe(TEMP),ensure_ascii=False))
        self.assertFalse(any(r['person_id']==TEMP for r in self.requests))
        self.assertEqual(self.world.observe(TEMP)['person']['card']['interests'],'维护本人声誉')
        self.assertNotIn(TEMP,self.raw()['_p04']['pending'])

    def test_periodic_government_is_real_queue_event_not_daily_all_person_roster(self):
        self.build()
        self.step()
        reached = False
        for _ in range(8):
            w,state=self.step()
            if state['time']==DAY:
                reached=True
                self.assertEqual(set(self.roster(w)),{p['id'] for p in state['persons'] if p['id']!=TEMP})
                self.assertNotIn(TEMP,self.roster(w))
                self.assertEqual(len([e for e in state['events'] if e['kind']=='government' and e['time']==DAY]),1)
                self.assertTrue(any(q['kind']=='p04_government' and q['at']==2*DAY for q in state['queue']))
                break
            self.assertEqual(self.roster(w),[])
            self.assertEqual(w['boundary_kind'],'normal')
        self.assertTrue(reached,'finite frozen queue did not reach first government')
        self.assertFalse(any(r['person_id']==TEMP for r in self.requests))
        self.assert_closure()

    def test_new_person_private_session_starts_empty_same_person_resume_retains_only_own_history(self):
        self.build()
        self.policy=lambda v,s: ((dict(action='send',to=TEMP,text=TEXT) if v['person']['id']==KING and v['time']==0 else
                                 dict(action='investigate') if v['person']['id']==TEMP and v['time']==60 else dict(action='wait')),s)
        self.step();self.step()
        first=next(r for r in self.requests if r['person_id']==TEMP)
        self.assertEqual(first['messages'],[])
        self.assertNotIn(PRIVATE,json.dumps(first,ensure_ascii=False))
        old_world,old_host,old_owner=self.world,self.host,self.owner
        identities={Path(p).name:Path(p).stat().st_ino for p in (old_world.db_path,old_host.db_path)}
        old_owner.close();self.assertFalse(old_owner.cleanup_pending);self.assertFalse(old_host.cleanup_pending)
        self.world=WorldStore(old_world.db_path,self.root/'scenario.json')
        self.world.initialize()
        self.host=ActorHost(self.world,old_host.db_path)
        self.host._choose=old_host._choose;self.host._request=old_host._request
        self.host._start_worker=old_host._start_worker
        self.owner=Coordinator(self.world,self.host)
        w,state=self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(60,[TEMP]))
        current=[r for r in self.requests if r['person_id']==TEMP][-1]
        self.assertEqual(len(current['messages']),2)
        self.assertNotIn(PRIVATE,json.dumps(current,ensure_ascii=False))
        self.assertEqual({Path(p).name:Path(p).stat().st_ino for p in (self.world.db_path,self.host.db_path)},identities)
        self.assert_closure()

    def test_open_rollback_does_not_consume_message_or_wake_or_duplicate_on_retry(self):
        self.build()
        self.policy=lambda v,s: ((dict(action='send',to=TEMP,text=TEXT) if v['person']['id']==KING and v['time']==0 else dict(action='wait')),s)
        self.step()
        before=self.raw();world=self.world;owner=self.owner
        owner.acquire_owner()
        with owner._owner_scope():world.control('resume')
        before=self.raw()
        encode=world._encode_state
        attempted=[]
        def fail(db,state):
            attempted.append(copy.deepcopy(state))
            raise RuntimeError('parent1043 fault before wake snapshot insert')
        world._encode_state=fail
        try:
            with owner._owner_scope(),self.assertRaisesRegex(RuntimeError,'parent1043 fault'):
                world.open_or_recover_window(coordinator=owner._owner_attempt.permit,
                    expected_version={k:before[k] for k in ('world_id','revision')})
        finally:
            world._encode_state=encode
        self.assertEqual(len(attempted),1)
        self.assertEqual(self.raw(),before)
        self.assertEqual(world.observe(TEMP)['messages'],[])
        w,state=self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(60,[TEMP]))
        self.assertEqual(len(world.observe(TEMP)['messages']),1)
        self.assertEqual(len([e for e in state['events'] if e['kind']=='message_delivered']),1)
        self.assert_closure()

    def test_completed_activation_replay_does_not_refill_autonomy_or_make_second_meeting(self):
        self.build()
        self.policy=lambda v,s: ((dict(action='meet',to=TEMP,text=TEXT) if v['person']['id']==KING else dict(action='wait')),s)
        w,state=self.step()
        a=next(a for a in self.rows(self.world.db_path,'coordinator_activations') if a['person_id']==KING)
        ref=ActivationRef(**{k:a[k] for k in ('world_id','window_id','activation_id','person_id')})
        before=self.raw();n=len(self.requests)
        result=self.host.act(KING,coordinator=self.owner._owner_attempt.permit,activation=ref)
        self.assertEqual(result['intent_id'],a['intent_id'])
        self.assertEqual(self.raw(),before)
        self.assertEqual(len(self.requests),n)
        self.assertEqual(len([e for e in state['events'] if e['kind']=='meeting']),1)

    def test_proposed_meeting_and_proposed_wait_use_original_adjudicator_and_sleep(self):
        calls=[]
        def dm(payload):
            calls.append(copy.deepcopy(payload['proposal']))
            p=payload['proposal']['person_id'];s=payload['world']
            return dict(world_id=s['world_id'],base_revision=s['revision'],ruling='成功',reason='已存在的同地条件',
                goal='兑现本人原意图',costs=[],benefits=[],effects=[dict(action='meet',to=TEMP,text=TEXT) if p==KING else dict(action='wait')])
        self.build(dm=dm)
        self.policy=lambda v,s: ((dict(action='propose',text='请求同地与使者面谈') if v['person']['id']==KING else
                                 dict(action='propose',text='本人选择等待真实消息') if v['person']['id']==TEMP else dict(action='wait')),s)
        self.step();w,state=self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(0,[TEMP]))
        self.assertEqual([p['person_id'] for p in calls],[KING,TEMP])
        self.assertEqual(self.raw()['_p04']['remaining'][TEMP],0)
        self.assertNotIn(TEMP,self.raw()['_p04']['pending'])
        self.assertEqual(len([e for e in state['events'] if e['kind']=='meeting']),1)
        self.assertEqual([o['text'] for o in self.world.observe(TEMP)['observations'] if o['source']=='实际会面'],[TEXT])
        w,state=self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(43200,[]))
        self.assertEqual(len(calls),2)
        self.assert_closure()

    def test_production_completion_wakes_only_actual_local_owner_at_exact_due(self):
        from app.tests import test_registered_production_runtime_v1 as production_fixture
        c=production_fixture.configuration(capacity=24)
        c['scenario_id']='parent1043-production-event-wakeup-v1'
        c['event_wakeup']=configuration()['event_wakeup']
        c['economy']['recipes'][production_fixture.RID]['duration_seconds']=3600
        c['economy']['recipes'][production_fixture.RID]['labor_hours']=1
        calls=[]
        def dm(payload):
            calls.append(copy.deepcopy(payload['proposal']))
            s=payload['world']
            return dict(world_id=s['world_id'],base_revision=s['revision'],ruling='成功',reason='实际投入与现场工班',
                goal='制造',costs=[],benefits=[],effects=[production_fixture.production()])
        self.build(c,dm=dm)
        self.policy=lambda v,s: ((dict(action='propose',text='请使用现场实际投入制造一件农具') if v['person']['id']==FINANCE and v['time']==0 else dict(action='wait')),s)
        self.step()
        self.assertFalse(any('iron_tool' in o['text'] for o in self.world.observe(FINANCE)['observations']))
        w,state=self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(3600,[FINANCE]))
        task=state['economy']['tasks'][0]
        self.assertEqual((task['status'],task['completed_at'],task['actual_outputs']),('completed',3600,dict(iron_tool=1)))
        self.assertEqual(state['economy']['materials']['place-0']['iron_tool'],3)
        actual=[o for o in self.world.observe(FINANCE)['observations'] if o['time']==3600 and 'iron_tool' in o['text']]
        self.assertEqual(len(actual),1)
        self.assertIn(actual[0]['text'],[r for r in self.requests if r['person_id']==FINANCE][-1]['prompt'])
        self.assertEqual(len(calls),1)
        self.assertFalse(any(r['person_id']==TEMP for r in self.requests))
        self.assert_closure()

    def test_invalid_new_world_temporary_declarations_are_atomic_rejections(self):
        variants=[]
        for key,value in [('temporary_steps',True),('temporary_steps',0),('government_interval_seconds',0)]:
            c=configuration();c['event_wakeup'][key]=value;variants.append(c)
        for key,value in [('id',KING),('office','king'),('location','unknown'),('private_messages',[PRIVATE])]:
            c=configuration();c['event_wakeup']['temporary_persons'][0][key]=value;variants.append(c)
        for index,c in enumerate(variants):
            with self.subTest(index=index):
                path=self.root/('invalid-'+str(index)+'.json')
                path.write_text(json.dumps(c,ensure_ascii=False),encoding='utf-8')
                world=WorldStore(self.root/('invalid-'+str(index)+'.sqlite'),path)
                with self.assertRaises(WorldError) as caught:world.initialize()
                self.assertEqual(caught.exception.code,'INVALID_EVENT_WAKEUP')
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(),[])

    def test_event_rules_cannot_change_or_leak_host_metadata(self):
        self.build();self.step()
        before=self.raw()
        def corrupt(state,db):state['_p04']['rules']['temporary_steps']+=1
        with self.owner._owner_scope(),self.assertRaises(WorldError) as caught:self.world._mutate(corrupt)
        self.assertEqual(caught.exception.code,'FROZEN_EVENT_WAKEUP')
        self.assertEqual(self.raw(),before)
        self.assertNotIn('_p04',self.world.snapshot())
        self.assertNotIn('_p04',self.world.observe(TEMP))
        self.assertNotIn('last_grant',json.dumps(self.requests,ensure_ascii=False))

    def test_self_mail_does_not_regrant_an_exhausted_temporary_window(self):
        self.build()
        self.policy=lambda v,s: ((dict(action='send',to=TEMP,text=TEXT) if v['person']['id']==KING and v['time']==0 else
                                 dict(action='send',to=TEMP,text='本人给自己的信') if v['person']['id']==TEMP else dict(action='wait')),s)
        self.step()
        for _ in range(3):self.step()
        w,state=self.step()
        self.assertEqual((w['target_time'],self.roster(w)),(120,[]))
        self.assertEqual(len([m for m in self.world.observe(TEMP)['messages'] if m['from']==TEMP]),3)
        self.assertEqual(self.raw()['_p04']['remaining'][TEMP],0)
        self.assertEqual(len([r for r in self.requests if r['person_id']==TEMP]),3)
        self.assert_closure()

    def test_legacy_world_keeps_original_daily_roster_without_late_optin(self):
        c=configuration();del c['event_wakeup']
        self.build(c)
        w,state=self.step()
        self.assertEqual(w['target_time'],DAY)
        self.assertEqual(self.roster(w),[p['id'] for p in state['persons']])
        self.assertNotIn(TEMP,self.roster(w))
        self.assertNotIn('_p04',self.raw())
        self.assert_closure()

    def test_resident_mutual_meet_advances_real_nodes_and_reopen_keeps_dispatch(self):
        self.build()
        def policy(v, seen):
            pid = v['person']['id']
            if pid in (KING, FINANCE):
                return dict(action='meet', to=FINANCE if pid==KING else KING,
                            text='1045-resident-'+pid+'-'+str(v['time'])), seen
            return dict(action='wait'), seen
        self.policy = policy
        w, state = self.step()
        self.assertEqual(w['target_time'], 0)
        before = self.raw()
        self.assertEqual({before['_p04']['resident_last_dispatch'][p] for p in (KING,FINANCE)}, {0})
        nodes = [q for q in before['queue'] if q['kind']=='p04_deferred_wake']
        self.assertEqual({(q['person_id'],q['at'],q['requested_at']) for q in nodes}, {(KING,60,0),(FINANCE,60,0)})
        self.assertEqual(len(nodes), 2)
        a = next(a for a in self.rows(self.world.db_path,'coordinator_activations') if a['person_id']==KING)
        ref = ActivationRef(**{k:a[k] for k in ('world_id','window_id','activation_id','person_id')})
        request_count = len(self.requests)
        replay = self.host.act(KING,coordinator=self.owner._owner_attempt.permit,activation=ref)
        self.assertEqual(replay['intent_id'], a['intent_id'])
        self.assertEqual(self.raw(), before)
        self.assertEqual(len(self.requests), request_count)
        old_world, old_host, old_owner = self.world, self.host, self.owner
        old_owner.close()
        self.assertFalse(old_owner.cleanup_pending)
        self.assertFalse(old_host.cleanup_pending)
        self.world = WorldStore(old_world.db_path,self.root/'scenario.json')
        self.world.initialize()
        self.host = ActorHost(self.world,old_host.db_path)
        self.host._choose = old_host._choose
        self.host._request = old_host._request
        self.host._start_worker = old_host._start_worker
        self.owner = Coordinator(self.world,self.host)
        self.assertEqual(self.raw(), before)
        for at in (60,120,180):
            w, state = self.step()
            self.assertEqual((w['target_time'],self.roster(w)), (at,[KING,FINANCE]))
            self.assertEqual({self.raw()['_p04']['resident_last_dispatch'][p] for p in (KING,FINANCE)}, {at})
            self.assertFalse(any(q['id'] in {n['id'] for n in nodes} for q in state['queue']))
            nodes = [q for q in state['queue'] if q['kind']=='p04_deferred_wake']
            self.assertEqual({(q['person_id'],q['at'],q['requested_at']) for q in nodes}, {(KING,at+60,at),(FINANCE,at+60,at)})
            self.assertEqual(len(nodes), 2)
        meetings = [e for e in state['events'] if e['kind']=='meeting']
        self.assertEqual(len(meetings), 8)
        self.assertEqual(len({e['cause_id'] for e in meetings}), 8)
        for recipient,sender in ((KING,FINANCE),(FINANCE,KING)):
            words = [o['text'] for o in self.world.observe(recipient)['observations'] if o['source']=='实际会面']
            self.assertEqual(words, ['1045-resident-'+sender+'-'+str(at) for at in (0,60,120,180)])
        self.assertEqual(len([r for r in self.requests if r['person_id']==KING]), 4)
        self.assertEqual(len([r for r in self.requests if r['person_id']==FINANCE]), 4)
        self.assertTrue(any(q['kind']=='p04_government' and q['at']==DAY for q in state['queue']))
        self.assertNotIn(PRIVATE,json.dumps(self.world.observe(FINANCE),ensure_ascii=False))
        self.assert_closure()

    def test_temporary_triggered_resident_meet_chain_reaches_preexisting_delivery(self):
        self.build()
        def policy(v, seen):
            pid = v['person']['id']
            if pid==KING and v['time']==0:
                return dict(action='meet',to=TEMP,text=TEXT),seen
            if pid==TEMP and v['time']==0 and not seen:
                return dict(action='send',to=FINANCE,text='1045-future-node'),['1045-sent']
            if pid==TEMP and v['time']==0:
                return dict(action='meet',to=KING,text='1045-temporary-trigger'),seen
            if pid in (KING,FINANCE) and v['time']>0:
                return dict(action='meet',to=FINANCE if pid==KING else KING,text='1045-mixed-'+pid+'-'+str(v['time'])),seen
            return dict(action='wait'),seen
        self.policy = policy
        self.step()
        for _ in range(3):
            w,state = self.step()
            self.assertEqual((w['target_time'],self.roster(w)),(0,[TEMP]))
        self.assertEqual(self.raw()['_p04']['remaining'][TEMP],0)
        nodes = [q for q in state['queue'] if q['kind']=='p04_deferred_wake']
        self.assertEqual([(q['person_id'],q['at'],q['requested_at']) for q in nodes],[(KING,60,0)])
        future = [q for q in state['queue'] if q['kind']=='deliver' and q['text']=='1045-future-node']
        self.assertEqual(len(future),1)
        self.assertEqual(future[0]['at'],60)
        self.assertFalse(any(m['text']=='1045-future-node' for m in self.world.observe(FINANCE)['messages']))
        for at in (60,120,180):
            w,state = self.step()
            self.assertEqual((w['target_time'],self.roster(w)),(at,[KING,FINANCE]))
            self.assertEqual(len([m for m in self.world.observe(FINANCE)['messages'] if m['text']=='1045-future-node']),1)
            self.assertFalse(any(q['id']==future[0]['id'] for q in state['queue']))
            self.assertEqual(self.raw()['_p04']['remaining'][TEMP],0)
        self.assertEqual([v['time'] for v in self.views if v['person']['id']==TEMP],[0,0,0])
        self.assertEqual(len([e for e in state['events'] if e['kind']=='message_delivered' and e['person_id']==FINANCE]),1)
        words = [o['text'] for o in self.world.observe(KING)['observations'] if o['source']=='实际会面']
        self.assertEqual(words.count('1045-temporary-trigger'),2)
        self.assertEqual(len([e for e in state['events'] if e['kind']=='meeting']),9)
        self.assertEqual(len([r for r in self.requests if r['person_id']==TEMP]),3)
        self.assertNotIn(PRIVATE,json.dumps(self.world.observe(TEMP),ensure_ascii=False))
        self.assertNotIn('1045-future-node',json.dumps(self.world.observe(TEMP)['messages'],ensure_ascii=False))
        self.assert_closure()

    def _1047_resident_fixture(self, dm_mode=False, failed=False):
        if dm_mode:
            def dm(payload):
                s=payload['world'];p=payload['proposal']['person_id']
                return dict(world_id=s['world_id'],base_revision=s['revision'],ruling='失败' if failed else '成功',
                    reason='真实同地producer材料',goal='会面',costs=[],benefits=[],
                    effects=[dict(action='meet',to=FINANCE if p==KING else KING,text='1047-child')])
            self.build(dm=dm)
        else:
            self.build()
        def policy(v,seen):
            p=v['person']['id']
            if p in (KING,FINANCE):
                return (dict(action='propose',text='1047会面') if dm_mode else
                        dict(action='meet',to=FINANCE if p==KING else KING,text='1047-direct')),seen
            return dict(action='wait'),seen
        self.policy=policy
        w,_=self.step()
        return w

    def _1047_consumers(self, window, changes):
        # Authenticated collector first; substitutions change only current's
        # serialized state after collection. Original material/check/tables remain
        # bound; every other source row stays the collector's actual sqlite3.Row.
        from app import coordinator_scheduler as scheduler
        from app.actor_execution import EntryBudget
        from contextlib import contextmanager
        from types import MappingProxyType
        before=self.raw(); request_count=len(self.requests); view_count=len(self.views)
        root=self.owner._owner_attempt
        with self.owner._owner_scope(),self.host._with_entry_pipeline():
            budget=EntryBudget(root.policy,root.stop,(root,None))
            with self.world._entry_budget(budget):
                with scheduler.source_lock(self.world):
                    capsule=scheduler._C0Selection(self.world,root.permit,budget)
                material=scheduler._c0_collect(capsule)
                good=scheduler._c0_window_record_uncached(material,before['world_id'],window['window_id'])
                self.assertEqual(good['window_id'],window['window_id'])
                for name,change,code in changes:
                    bad=copy.deepcopy(before);change(bad)
                    tables=dict(material['tables'])
                    tables['main.snapshots']=tuple(dict(row,state=json.dumps(bad,ensure_ascii=False))
                        if row['seq']==before['revision'] else row for row in tables['main.snapshots'])
                    controlled=MappingProxyType(dict(material,tables=MappingProxyType(tables)))
                    with self.subTest(consumer='offlock-material',fault=name):
                        with self.assertRaises(WorldError) as caught:
                            scheduler._c0_window_record_uncached(controlled,before['world_id'],window['window_id'])
                        self.assertEqual(caught.exception.code,code)
                material['check']()
        self.assertEqual(self.raw(),before)
        self.assertEqual((len(self.requests),len(self.views)),(request_count,view_count))
        # Stop legitimate lease heartbeat before exact whole-file no-write oracle.
        self.owner.close()
        self.assertFalse(self.owner.cleanup_pending)
        self.assertFalse(self.host.cleanup_pending)
        paths=[Path(self.world.db_path),Path(self.host.db_path)]
        paths += [Path(str(p)+suffix) for p in list(paths) for suffix in ('-wal','-shm')]
        whole={str(p):(p.exists(),p.read_bytes() if p.exists() else None) for p in paths}
        class Row(dict):
            def __getitem__(self,key):
                return list(self.values())[key] if type(key) is int else super().__getitem__(key)
        class Cursor:
            def __init__(self,cursor,change,snapshot):self.cursor=cursor;self.change=change;self.snapshot=snapshot
            def convert(self,row):
                if row is None or not self.snapshot or 'state' not in row.keys():return row
                state=json.loads(row['state'])
                if state['revision']!=before['revision']:return row
                self.change(state)
                return Row(dict(row,state=json.dumps(state,ensure_ascii=False)))
            def fetchone(self):return self.convert(self.cursor.fetchone())
            def fetchall(self):return [self.convert(r) for r in self.cursor.fetchall()]
            def __iter__(self):return iter(self.fetchall())
        class Reader:
            def __init__(self,db,change):self.db=db;self.change=change
            @property
            def row_factory(self):return self.db.row_factory
            @row_factory.setter
            def row_factory(self,value):self.db.row_factory=value
            def execute(self,*a,**kw):
                # Only the frozen producers' snapshots SELECT with state may inject JSON.
                # Window phase strings and snapshot COUNT/seq projections pass unchanged.
                tokens=a[0].split();upper=[token.upper() for token in tokens]
                table=tokens[upper.index('FROM')+1] if upper and upper[0]=='SELECT' and 'FROM' in upper else None
                snapshot=table in ('snapshots','main.snapshots')
                return Cursor(self.db.execute(*a,**kw),self.change,snapshot)
        with closing(sqlite3.connect(paths[0].resolve().as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory=sqlite3.Row
            self.assertEqual(scheduler.window_record(db,before['world_id'],window['window_id'])['window_id'],window['window_id'])
            for name,change,code in changes:
                with self.subTest(consumer='native-material',fault=name):
                    with self.assertRaises(WorldError) as caught:
                        scheduler.window_record(Reader(db,change),before['world_id'],window['window_id'])
                    self.assertEqual(caught.exception.code,code)
        # Real initialize restore, original connection/readonly transaction; only
        # returned row material changes. This is not a physically damaged DB test.
        healthy=WorldStore(paths[0],self.root/'scenario.json')
        self.assertEqual(healthy.initialize()['revision'],before['revision'])
        for name,change,code in changes:
            restored=WorldStore(paths[0],self.root/'scenario.json')
            original=restored._connection
            @contextmanager
            def connection(*args,**kwargs):
                with original(*args,**kwargs) as db:
                    db.row_factory=sqlite3.Row
                    yield Reader(db,change)
            restored._connection=connection
            with self.subTest(consumer='initialize-restore-material',fault=name):
                with self.assertRaises(WorldError) as caught:restored.initialize()
                self.assertEqual(caught.exception.code,code)
        self.assertEqual({str(p):(p.exists(),p.read_bytes() if p.exists() else None) for p in paths},whole)
        self.assertEqual(self.raw(),before)
        self.assertEqual((len(self.requests),len(self.views)),(request_count,view_count))

    def test_1047_current_missing_meet_defer_rejected_by_both_readers_and_restore(self):
        w=self._1047_resident_fixture()
        self.assertEqual(len([q for q in self.raw()['queue'] if q['kind']=='p04_deferred_wake']),2)
        def missing(s):s['queue']=[q for q in s['queue'] if q['kind']!='p04_deferred_wake']
        self._1047_consumers(w,[('delete-two-obligations',missing,'INVALID_EVENT_WAKEUP')])

    def test_1047_current_ledger_rollback_after_temporary_send_rejected(self):
        self.build()
        def policy(v,seen):
            if v['person']['id']==KING:return dict(action='meet',to=TEMP,text=TEXT),seen
            if v['person']['id']==TEMP:return dict(action='send',to=FINANCE,text='1047-future'),seen
            return dict(action='wait'),seen
        self.policy=policy
        first,_=self.step();w,_=self.step()
        self.assertEqual(self.raw()['_p04']['dispatch'],[TEMP])
        self.assertFalse(any(q['kind']=='p04_deferred_wake' for q in self.raw()['queue']))
        def rollback(s):s['_p04']['resident_last_dispatch'][KING]=-1
        self._1047_consumers(first,[('KING-0-to-minus1',rollback,'INVALID_EVENT_WAKEUP')])

    def test_1047_entire_queue_identity_and_exact_counter_rejection(self):
        w=self._1047_resident_fixture()
        def collision(s):
            a,b=[q for q in s['queue'] if q['kind']=='p04_deferred_wake']
            b['id']=a['id'];b['seq']=a['seq']
        def otherkind(s):
            a=next(q for q in s['queue'] if q['kind']=='p04_deferred_wake')
            b=next(q for q in s['queue'] if q['kind']!='p04_deferred_wake')
            a['id']=b['id'];a['seq']=b['seq']
        def boolean(s):s['_queue_seq']=True
        self._1047_consumers(w,[('cross-person',collision,'FROZEN_EVENT_WAKEUP'),
            ('cross-kind',otherkind,'FROZEN_EVENT_WAKEUP'),('bool-counter',boolean,'FROZEN_EVENT_WAKEUP')])

    def test_1047_legal_later_windows_keep_earliest_and_historical_readers(self):
        first=self._1047_resident_fixture()
        original=[q for q in self.raw()['queue'] if q['kind']=='p04_deferred_wake']
        self.assertEqual({q['at'] for q in original},{60})
        for at in (60,120):
            w,_=self.step()
            self.assertEqual(w['target_time'],at)
            self.assertEqual({self.raw()['_p04']['resident_last_dispatch'][p] for p in (KING,FINANCE)},{at})
        self._1047_consumers(first,[])

    def test_1047_dm_child_meet_obligations_and_failed_meet_have_distinct_facts(self):
        first=self._1047_resident_fixture(dm_mode=True)
        nodes=[q for q in self.raw()['queue'] if q['kind']=='p04_deferred_wake']
        self.assertEqual(len(nodes),2)
        self.assertTrue(all(q['cause_id'].startswith('dm:') for q in nodes))
        def missing(s):s['queue']=[q for q in s['queue'] if q['kind']!='p04_deferred_wake']
        self._1047_consumers(first,[('missing-DM-child-obligation',missing,'INVALID_EVENT_WAKEUP')])

    def test_1047_failed_dm_meet_does_not_invent_defer(self):
        w=self._1047_resident_fixture(dm_mode=True,failed=True)
        self.assertFalse(any(q['kind']=='p04_deferred_wake' for q in self.raw()['queue']))
        self.assertFalse(any(e['kind']=='meeting' for e in self.raw()['events']))
        self._1047_consumers(w,[])

    # Source-only additions for EventWakeupTests. No execution authorized.
    # Function wrappers count real consumer calls; no class/source registry rebinding.
    def _1049_collect_scope(self):
        from app import coordinator_scheduler as scheduler
        from app.actor_execution import EntryBudget
        from contextlib import contextmanager
        @contextmanager
        def collected():
            root=self.owner._owner_attempt
            with self.owner._owner_scope(),self.host._with_entry_pipeline():
                budget=EntryBudget(root.policy,root.stop,(root,None))
                with self.world._entry_budget(budget):
                    with scheduler.source_lock(self.world):
                        capsule=scheduler._C0Selection(self.world,root.permit,budget)
                    material=scheduler._c0_collect(capsule)
                    yield scheduler,capsule,material
                    material['check']()
        return collected()

    def test_1049_outer_history_consumers_share_projection_not_authority(self):
        from unittest.mock import patch
        self._1047_resident_fixture()
        self.step();self.step()
        with self._1049_collect_scope() as (scheduler,capsule,material):
            # Exact builtin tuples/native Rows, original bound check: independent
            # uninstrumented positive, not a measured subclass admission branch.
            self.assertTrue(all(type(rows) is tuple for rows in material['tables'].values()))
            result=scheduler._open_history_proof(capsule,material)
            self.assertEqual(result[2],3)
            counts=dict(projection=0,projection_reads=0,meeting=0,windows=0,phases=0,checks=0)
            original_projection=scheduler._p04_current_projection
            original_meeting=scheduler._p04_committed_meeting
            original_window=scheduler._c0_window_record
            original_phase=scheduler._c0_phase_proof
            def projection(read,state):
                counts['projection']+=1
                def counted(*args,**kwargs):
                    counts['projection_reads']+=1
                    return read(*args,**kwargs)
                return original_projection(counted,state)
            def meeting(*args,**kwargs):
                counts['meeting']+=1
                return original_meeting(*args,**kwargs)
            def window(*args,**kwargs):
                counts['windows']+=1
                return original_window(*args,**kwargs)
            def phase(*args,**kwargs):
                counts['phases']+=1
                return original_phase(*args,**kwargs)
            def check():
                counts['checks']+=1
                material['check']()
            from types import MappingProxyType
            measured=MappingProxyType(dict(material,check=check))
            with patch.object(scheduler,'_p04_current_projection',projection), \
                 patch.object(scheduler,'_p04_committed_meeting',meeting), \
                 patch.object(scheduler,'_c0_window_record',window), \
                 patch.object(scheduler,'_c0_phase_proof',phase):
                with scheduler._c0_query_scope(measured) as owned:
                    plan=scheduler._c0_query_context.get()[3]
                    actual=scheduler._open_history_proof(capsule,owned)
                    self.assertEqual(actual,result)
                    self.assertEqual(counts['projection'],1)
                    self.assertGreater(counts['projection_reads'],0)
                    self.assertGreater(counts['meeting'],0)
                    # Count actual per-activation/private-leaf readers, not a
                    # synthetic helper loop standing in for the real audit.
                    activations=material['tables']['main.coordinator_activations']
                    self.assertGreater(counts['windows'],len(activations)+3)
                    self.assertGreater(counts['phases'],len(activations))
                    self.assertIn('main.snapshots',plan.indexes)
                    saved_index=plan.indexes['main.snapshots']
                    saved=counts.copy()
                    scheduler._open_history_proof(capsule,owned)
                    self.assertEqual(counts['projection'],saved['projection'])
                    self.assertEqual(counts['projection_reads'],saved['projection_reads'])
                    self.assertEqual(counts['meeting'],saved['meeting'])
                    self.assertGreater(counts['checks'],saved['checks'])
                    self.assertIs(plan.indexes['main.snapshots'],saved_index)
                self.assertFalse(plan.active)
                self.assertIsNone(plan.p04)
                self.assertEqual(plan.indexes,{})
                # New outer proof / Entry lifetime never inherits derived success.
                scheduler._open_history_proof(capsule,measured)
                self.assertEqual(counts['projection'],2)
            self.assertIsNone(scheduler._c0_query_context.get())

    def test_1049_nested_material_identity_restore_and_exception_cleanup(self):
        from types import MappingProxyType
        from unittest.mock import patch
        self._1047_resident_fixture();self.step();self.step()
        with self._1049_collect_scope() as (scheduler,capsule,material):
            projections=[]
            original=scheduler._p04_current_projection
            def projection(read,state):
                projections.append(state['revision'])
                return original(read,state)
            with patch.object(scheduler,'_p04_current_projection',projection):
                with scheduler._c0_query_scope(material) as owned:
                    outer=scheduler._c0_query_context.get()
                    scheduler._open_history_proof(capsule,owned)
                    other=MappingProxyType(dict(material,tables=MappingProxyType(dict(material['tables']))))
                    inner=None
                    with self.assertRaisesRegex(RuntimeError,'1049 inner unwind'):
                        with scheduler._c0_query_scope(other) as foreign:
                            inner=scheduler._c0_query_context.get()[3]
                            self.assertIsNot(inner,outer[3])
                            scheduler._open_history_proof(capsule,foreign)
                            raise RuntimeError('1049 inner unwind')
                    self.assertFalse(inner.active)
                    self.assertIsNone(inner.p04)
                    self.assertIs(scheduler._c0_query_context.get(),outer)
                    scheduler._open_history_proof(capsule,owned)
                    self.assertEqual(len(projections),2)
                # A new actual EntryBudget/collector is exercised below, not just
                # a different dictionary with a guessed Entry tag.
                self.assertIsNone(scheduler._c0_query_context.get())
        with self._1049_collect_scope() as (scheduler,capsule,material):
            with scheduler._c0_query_scope(material) as owned:
                new=scheduler._c0_query_context.get()[3]
                self.assertIsNot(new,outer[3])
                scheduler._open_history_proof(capsule,owned)
                self.assertIsNotNone(new.p04)
        self.assertFalse(new.active)

    def test_1049_controlled_bad_material_reaches_original_semantic_gates(self):
        from types import MappingProxyType
        from unittest.mock import patch
        first=self._1047_resident_fixture();self.step();self.step()
        def missing(state):state['queue']=[q for q in state['queue'] if q['kind']!='p04_deferred_wake']
        def rollback(state):
            # Preserve frozen shape; the opening ledger/dispatch still disagree.
            state['_p04']['dispatch']=[p for p in state['_p04']['dispatch'] if p!=KING]
            state['queue']=[q for q in state['queue']
                if not (q['kind']=='p04_deferred_wake' and q['person_id']==KING)]
            state['_p04']['resident_last_dispatch'][KING]=-1
        def collision(state):
            a,b=[q for q in state['queue'] if q['kind']=='p04_deferred_wake']
            b['id']=a['id'];b['seq']=a['seq']
        with self._1049_collect_scope() as (scheduler,capsule,material):
            before=self.raw();original=scheduler._p04_current_projection
            calls=[]
            def projection(read,state):
                calls.append(state['revision'])
                return original(read,state)
            with scheduler._c0_query_scope(material) as healthy:
                outer=scheduler._c0_query_context.get()
                scheduler._open_history_proof(capsule,healthy)
                for mutate,code in ((missing,'INVALID_EVENT_WAKEUP'),(rollback,'INVALID_EVENT_WAKEUP'),(collision,'FROZEN_EVENT_WAKEUP')):
                    bad=copy.deepcopy(before);mutate(bad)
                    if mutate is not collision:
                        self.assertIsNone(WorldStore._check_frozen_p04(bad,before),mutate.__name__)
                    tables=dict(material['tables'])
                    tables['main.snapshots']=tuple(dict(row,state=json.dumps(bad,ensure_ascii=False))
                        if row['seq']==before['revision'] else row for row in tables['main.snapshots'])
                    controlled=dict(material,tables=tables)
                    with patch.object(scheduler,'_p04_current_projection',projection):
                        with scheduler._c0_query_scope(controlled) as owned:
                            plan=scheduler._c0_query_context.get()[3]
                            with self.assertRaises(WorldError) as caught:
                                # cache-hit material current gate, without an
                                # early new index-admission rejection.
                                scheduler._open_history_proof(capsule,owned)
                            self.assertEqual(caught.exception.code,code)
                            self.assertIsNone(plan.p04)
                    self.assertIs(scheduler._c0_query_context.get(),outer)
                    scheduler._c0_window_record(healthy,before['world_id'],first['window_id'])
                self.assertEqual(len(calls),3)
                # Replacing rows/check on the SAME mutable source cannot reuse
                # an earlier owned plan; the malformed payload still reaches the
                # original projection rather than being rejected on admission.
                mutable=dict(material,tables=dict(material['tables']))
                with scheduler._c0_query_scope(mutable):
                    held=scheduler._c0_query_context.get()[3]
                    mutable['tables']=tables
                    with scheduler._c0_query_scope(mutable) as changed:
                        self.assertIsNot(scheduler._c0_query_context.get()[3],held)
                        with self.assertRaises(WorldError) as caught:
                            scheduler._p04_material_current(changed,scheduler._c0_latest(changed))
                        self.assertEqual(caught.exception.code,'FROZEN_EVENT_WAKEUP')

    def test_1049_hit_dynamic_denial_invalidates_projection_and_unwinds(self):
        from types import MappingProxyType
        self._1047_resident_fixture();self.step();self.step()
        with self._1049_collect_scope() as (scheduler,capsule,material):
            enabled=[False];checks=[];error=RuntimeError('1049 dynamic cancellation sentinel')
            def check():
                checks.append(1)
                material['check']()
                if enabled[0]:raise error
            controlled=MappingProxyType(dict(material,check=check))
            plan=None
            with self.assertRaises(RuntimeError) as caught:
                with scheduler._c0_query_scope(controlled) as owned:
                    plan=scheduler._c0_query_context.get()[3]
                    scheduler._open_history_proof(capsule,owned)
                    self.assertIsNotNone(plan.p04)
                    count=len(checks);enabled[0]=True
                    scheduler._open_history_proof(capsule,owned)
            self.assertIs(caught.exception,error)
            self.assertGreater(len(checks),count)
            self.assertFalse(plan.active)
            self.assertIsNone(plan.p04)
            self.assertIsNone(scheduler._c0_query_context.get())
            enabled[0]=False
            scheduler._open_history_proof(capsule,controlled)


if __name__=='__main__':
    unittest.main()
