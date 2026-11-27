"""1067 whole-method definitions only: never imported/run during preparation.

Actual WorldStore + registered ActorHost + Coordinator + native dual SQLite.
Only model callbacks/chooser are controlled; no SDK/model or forged authority.
Each method retains a fresh exclusive owned world and one cumulative 120s bound.
Original actor120, busy2, prep120, root/phase/source/CAS gates remain unchanged.
"""
import copy
import json
import sqlite3
import time
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from app import world as WM
from app.world import WorldStore, WorldError, DAY
from app.adjudication import Adjudicator
from app.actors import ActorHost
from app.server import Coordinator
from app.coordinator_scheduler import ActivationRef

PID='person-country-1-finance'
KING='person-country-1-king'
PROJECT='local_tool_trial'
KNOWLEDGE='local_verified_tool_trial'
PRIVATE='组织者未向任何人传达的私人动机标记1067'
TEXT='请依本地实际条件推进分阶段试验，不凭概念领取成品。'
CONFIG=Path(WM.__file__).with_name('data')/'scenarios/staged-projects-v1-iron-4.json'
SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
TABLES=('snapshots','intents','dm_proposals','dm_decisions','dm_timeline',
        'coordinator_windows','coordinator_activations','coordinator_activation_outcomes',
        'actor_intents','actor_session_heads','actor_intent_execution','actor_calls')


def configuration():
    return json.loads(CONFIG.read_bytes())


def start():
    return dict(action='start_project',project_id=PROJECT)


class StagedProjects1067Tests(unittest.TestCase):
    def setUp(self):
        self.root=SCRATCH/('parent1067-owned-'+uuid.uuid4().hex)
        self.root.mkdir(parents=True,exist_ok=False)
        self.world=self.host=self.owner=None
        self.views=[];self.requests=[];self.previews=[];self.windows=[];self.dm_calls=[]
        self.deadline=time.monotonic()+120
        self.policy=lambda v,s:(dict(action='wait'),s)
        self.effects=lambda p,s:[start()]
        self.ruling='成功'

    def rows(self,path):
        with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory=sqlite3.Row
            names={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            result={n:[dict(r) for r in db.execute('SELECT * FROM '+n)] for n in TABLES if n in names and n!='snapshots'}
            if 'snapshots' in names:
                result['snapshots']=[dict(r) for r in db.execute('SELECT * FROM snapshots ORDER BY seq DESC LIMIT 1')]
                result['snapshot_count']=db.execute('SELECT count(*) FROM snapshots').fetchone()[0]
            return result

    def save(self,name,data):
        (self.root/(name+'.json')).write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')

    def _dm1070(self,payload):
        self.dm_calls.append(copy.deepcopy(payload['proposal']))
        s=payload['world']
        return dict(world_id=s['world_id'],base_revision=s['revision'],ruling=self.ruling,
            reason='明确虚构游戏参数与实际现场条件；无历史工艺效度声称',goal='本地阶段项目',
            costs=[],benefits=[],effects=copy.deepcopy(self.effects(payload['proposal'],s)))

    def bind(self, initialize_dm=False):
        # Reopen constructs the real adjudicator only under the new root owner.
        if initialize_dm:
            self.owner.acquire_owner()
            with self.owner._owner_scope():self.adjudicator=Adjudicator(self.world)
        self.host.adjudicator=self.adjudicator;self.host.dm_provider=self._dm1070
        def forbidden():raise AssertionError('1067 forbids SDK worker')
        def choose(view,seen):
            self.views.append(copy.deepcopy(view));return self.policy(view,seen)
        def request(payload):
            self.requests.append(copy.deepcopy(payload))
            return dict(messages=payload['messages']+[dict(role='user',content=payload['prompt']),
                dict(role='assistant',content=PRIVATE if payload['person_id']==PID else '本人的有限回应')],
                calls=[dict(call_id='1067-'+payload['intent_id'])],sdk_events=0,sdk_requests=0,
                provider='controlled-component-callback',pid=0,module_root='no-sdk')
        self.host._start_worker=forbidden;self.host._choose=choose;self.host._request=request

    def build(self,c=None):
        (self.root/'scenario.json').write_text(json.dumps(configuration() if c is None else c,ensure_ascii=False),encoding='utf-8')
        self.world=WorldStore(self.root/'public.sqlite',self.root/'scenario.json');self.world.initialize()
        self.adjudicator=Adjudicator(self.world)
        self.host=ActorHost(self.world,self.root/'private.sqlite',adjudicator=self.adjudicator,dm_provider=self._dm1070)
        self.owner=Coordinator(self.world,self.host)
        self.bind()
        # This wrapper only observes the original read-only preview, not effects,
        # SQLite authority, source attestations or permit objects.
        original=self.adjudicator.preview
        def preview(*a,**kw):
            before=dict(public=self.rows(self.world.db_path),private=self.rows(self.host.db_path))
            try:return original(*a,**kw)
            finally:
                after=dict(public=self.rows(self.world.db_path),private=self.rows(self.host.db_path))
                self.assertEqual(after,before,'preview may not change either real database')
                self.previews.append(dict(person_id=a[0],intent_id=a[1],decision_version=a[2],readonly=True))
        self.adjudicator.preview=preview

    def step(self):
        remaining=self.deadline-time.monotonic()
        self.assertGreater(remaining,0,'wholemethod cumulative120 exceeded; no renewal')
        self.owner.start(continuous=False)
        self.owner.worker.join(min(60,remaining))
        self.assertFalse(self.owner.worker.is_alive(),'original finite step60 must close')
        self.assertLess(time.monotonic(),self.deadline,'wholemethod cumulative120 exceeded')
        self.assertIsNone(self.owner.error,str(getattr(self.owner,'failure_projection',None)))
        s=self.world.snapshot();self.windows.append(dict(time=s['time'],status=s['status']))
        self.save('checkpoint',dict(state=s,public=self.rows(self.world.db_path),private=self.rows(self.host.db_path),
            windows=self.windows,previews=self.previews))
        return s

    def until(self,target,limit=256):
        s=self.world.snapshot()
        for _ in range(limit):
            if s['time']>=target:return s
            s=self.step()
        self.fail('finite real window limit reached without target')

    def propose_once(self,person=PID):
        issued=set()
        def policy(v,s):
            pid=v['person']['id']
            if pid==person and pid not in issued:
                issued.add(pid);return dict(action='propose',text=TEXT),s
            return dict(action='wait'),s
        self.policy=policy

    def replay(self,person=PID):
        row=next(r for r in self.rows(self.world.db_path)['coordinator_activations'] if r['person_id']==person)
        ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
        before=self.world.snapshot();private=self.rows(self.host.db_path);counts=(len(self.requests),len(self.dm_calls))
        result=self.host.act(person,coordinator=self.owner._owner_attempt.permit,activation=ref)
        self.assertEqual(self.world.snapshot(),before)
        self.assertEqual(self.rows(self.host.db_path)['actor_session_heads'],private['actor_session_heads'])
        self.assertEqual((len(self.requests),len(self.dm_calls)),counts)
        self.assertEqual(result['intent_id'],row['intent_id'])
        return result

    def closure(self):
        public=self.rows(self.world.db_path);private=self.rows(self.host.db_path)
        self.assertTrue(public['coordinator_windows'])
        self.assertTrue(all(w['state']=='completed' for w in public['coordinator_windows']))
        for row in public['coordinator_activations']:
            self.assertEqual(row['private_closed'],1)
            self.assertIn(row['phase'],('completed','rejected'))
            leaf=next(r for r in private['actor_intents'] if r['intent_id']==row['intent_id'])
            meta=next(r for r in private['actor_intent_execution'] if r['intent_id']==row['intent_id'])
            if row['phase']=='completed':
                self.assertEqual(meta['promoted_version'],meta['base_version']+1)
                self.assertEqual(json.loads(leaf['receipt']),self.world.receipt(row['person_id'],row['intent_id']))
            else:
                self.assertIsNone(meta['promoted_version']);self.assertIsNone(leaf['receipt'])
        for request in self.requests:
            text=json.dumps(request,ensure_ascii=False)
            self.assertNotIn('knowledge_sources',text);self.assertNotIn('project_knowledge_source',text);self.assertNotIn('material_ledger',text)
            if request['person_id']!=PID:self.assertNotIn(PRIVATE,text)

    def rejection(self,code,person=PID,index=0):
        self.step();s=self.world.snapshot()
        row=next(r for r in self.rows(self.world.db_path)['coordinator_activations'] if r['person_id']==person)
        self.assertEqual((row['phase'],row['private_closed']),('rejected',1))
        outcome=json.loads(next(r['outcome_json'] for r in self.rows(self.world.db_path)['coordinator_activation_outcomes'] if r['intent_id']==row['intent_id']))
        self.assertEqual((outcome['code'],outcome['effect_index'],outcome['origin']),(code,index,'adjudication.execute'))
        self.assertEqual(outcome['effects'],[]);self.assertEqual(s['projects']['tasks'],[])
        self.assertEqual(s['economy']['tasks'],[])
        self.assertEqual(s['economy']['materials'],configuration()['economy']['initial_materials'] if not hasattr(self,'initial') else self.initial)
        self.assertEqual(self.replay(person)['outcome'],outcome)
        self.closure()

    def _raw1070(self):
        with closing(sqlite3.connect(self.world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            baseline=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
            state=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            return state,baseline

    def _business1070(self):
        # Full schema and every row, not rows()'s latest-snapshot/table subset.
        # Retain maintenance separately: the live root heartbeat may renew it.
        result={}
        for label,path in (('public',self.world.db_path),('private',self.host.db_path)):
            with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                db.execute('BEGIN')
                schema=db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
                tables={}
                for kind,name,table,sql in schema:
                    if kind!='table':continue
                    quoted='"'+name.replace('"','""')+'"'
                    rows=db.execute('SELECT * FROM '+quoted).fetchall()
                    rows=[[dict(sqlite_blob_hex=v.hex()) if type(v) is bytes else v for v in row] for row in rows]
                    tables[name]=sorted(rows,key=lambda row:json.dumps(row,ensure_ascii=False,allow_nan=False))
                maintenance={k:tables.pop(k) for k in ('execution_clock','coordinator_execution_leases') if k in tables}
                result[label]=dict(business=dict(schema=schema,tables=tables),maintenance=maintenance)
        return result

    def _shape1070(self,state,baseline):
        # Structural/frozen controls intentionally do NOT claim semantic validity.
        self.world._check_frozen_pathology(state,baseline)
        self.world._check_frozen_economy(state,baseline)
        self.world._check_frozen_military(state,baseline)
        self.world._check_frozen_p04(state,baseline)
        project=state['projects']
        tasks=project['tasks'];knowledge=project['knowledge']
        task_keys={'id','project_id','owner_id','country_id','place_id','runs','status'}
        run_keys={'index','cause_id','started_at','due','ruling','journey_version','knowledge_sources','node','status','start_event_id'}
        end_keys={'ended_at','reason','end_event_id','labor_elapsed_seconds','actual_labor_hours'}
        node_keys={'id','seq','at','kind','project_task_id','stage_index','cause_id','rules_sha256'}
        structure=(type(tasks) is list and type(knowledge) is list and type(state['queue']) is list
            and all(type(t) is dict and set(t)==task_keys and type(t['runs']) is list
                and all(type(r) is dict and set(r)==run_keys|(end_keys if r['status']!='pending' else set())
                    and type(r['node']) is dict and set(r['node'])==node_keys
                    and all(type(r[k]) is int for k in ('index','started_at','due','journey_version'))
                    and type(r['knowledge_sources']) is dict for r in t['runs']) for t in tasks)
            and all(type(g) is dict and set(g)=={'id','person_id','knowledge_id','time','kind','source'}
                and type(g['time']) is int and all(type(g[k]) is str for k in ('id','person_id','knowledge_id','kind','source')) for g in knowledge)
            and all(type(q) is dict and (q.get('kind')!='project_stage' or set(q)==node_keys) for q in state['queue']))
        return dict(frozen=True,structure=structure,json_roundtrip=json.loads(json.dumps(state,allow_nan=False))==state)

    def _corrupt1070(self,state,case):
        # Only owned, SQL-derived mutable facts; no frozen rule/source/permit edit.
        task=state['projects']['tasks'][0];run=task['runs'][0]
        if case=='missing-node':
            state['queue']=[q for q in state['queue'] if q['id']!=run['node']['id']]
        elif case=='duplicate-node':
            # Duplicate causal stage with a fresh queue identity: the global
            # P04 ID/seq shape gate must pass before the project leaf refuses it.
            state['_queue_seq']+=1
            duplicate=copy.deepcopy(run['node'])
            duplicate.update(seq=state['_queue_seq'],id='q-'+str(state['_queue_seq']))
            state['queue'].append(duplicate)
            state['queue'].sort(key=lambda q:(q['at'],q['seq']))
        elif case=='node-at':run['node']['at']+=1
        elif case=='node-cause':run['node']['cause_id']='1070-wrong-cause'
        elif case=='tool-stock-and-held':state['economy']['materials'][task['place_id']]['anvil']+=1
        elif case=='consumed-book':state['economy']['material_ledger']['consumed'][task['place_id']]['iron']+=1
        elif case=='forged-source':state['projects']['knowledge'][0]['source']='1070-no-stage-event'
        elif case=='undelivered-knowledge':
            node=next(q for q in state['queue'] if q['kind']=='deliver' and 'project_knowledge_source' in q)
            source=next(g for g in state['projects']['knowledge'] if g['id']==node['project_knowledge_source'])
            state['projects']['knowledge'].append(dict(id='project-message-'+node['message_id'],person_id=KING,
                knowledge_id=source['knowledge_id'],time=node['at'],kind='message',source=node['message_id']))
        elif case=='wrong-due':run['due']+=1
        elif case=='terminal-time':run['ended_at']-=1
        elif case=='terminal-status':task['status']='completed'
        else:raise AssertionError('unknown owned 1070 case')

    def _safe_error1070(self,observation,error):
        # Exact known WorldError.code only. Never str/args/repr/TB frames/locals.
        observation['error_type']=type(error).__module__+'.'+type(error).__qualname__
        observation['exact_world_error']=type(error) is WorldError
        observation['domain_error']=type(error) is WM.DomainActionError
        observation['code']=error.code if type(error) is WorldError else None

    def _committed1070(self,state):
        task_id=state['projects']['tasks'][0]['id']
        for row in self.rows(self.world.db_path)['coordinator_activations']:
            if row['person_id']!=PID or row['phase']!='completed':continue
            receipt=self.world.receipt(PID,row['intent_id'])
            if receipt and receipt['status']=='committed' and any(
                effect.get('project_task_id')==task_id for effect in receipt['effect']['effects']):
                return bool(self.dm_calls)
        return False

    def _material_cases1070(self,cases):
        state,baseline=self._raw1070()
        for case,code in cases:
            obs=dict(case=case,mode='SQL-derived-controlled-material-NOT-cold-recovery',phase='positive',
                positive=False,entered=False,project_pass=False,code=None,exact_world_error=False,domain_error=False)
            bad=copy.deepcopy(state);before=after=None
            try:
                before=self._business1070()
                obs['committed']=self._committed1070(state)
                obs['host_pair']=type(self.host) is ActorHost and self.host.adjudicator is self.adjudicator and self.host.dm_provider==self._dm1070
                obs['positive_shape']=self._shape1070(state,baseline)
                obs['positive_project']=len(self.world._project_accounts(state))==3
                obs['positive']=self.world._production_accounts(state) is state['economy']
                self.assertTrue(obs['committed'] and obs['host_pair'] and obs['positive'] and obs['positive_project'] and all(obs['positive_shape'].values()))
                self._corrupt1070(bad,case);obs['phase']='negative'
                obs['negative_shape']=self._shape1070(bad,baseline)
                self.assertTrue(all(obs['negative_shape'].values()))
                if code=='INVALID_PRODUCTION_STATE':
                    obs['project_pass']=len(self.world._project_accounts(bad))==3
                obs['entered']=True
                obs['source_gate']='WorldStore._production_accounts -> _project_accounts / completion-derived books / material balance'
                self.world._production_accounts(bad)
            except BaseException as error:self._safe_error1070(obs,error)
            finally:
                try:after=self._business1070()
                finally:self.save('1070-'+case,dict(observation=obs,material=bad,before=before,after=after))
            self.assertTrue(obs['committed']);self.assertTrue(obs['host_pair']);self.assertTrue(obs['positive']);self.assertTrue(obs['positive_project'])
            self.assertTrue(all(obs['positive_shape'].values()));self.assertTrue(all(obs['negative_shape'].values()))
            self.assertEqual(obs['phase'],'negative');self.assertTrue(obs['entered'])
            self.assertTrue(obs['exact_world_error']);self.assertFalse(obs['domain_error']);self.assertEqual(obs['code'],code)
            if code=='INVALID_PRODUCTION_STATE':self.assertTrue(obs['project_pass'])
            self.assertEqual({k:v['business'] for k,v in after.items()},{k:v['business'] for k,v in before.items()})
            self.assertLess(time.monotonic(),self.deadline,'wholemethod cumulative120 exceeded; no renewal')

    def test_owned_persistent_missing_node_reaches_real_advance_consumer1070(self):
        self.build();self.propose_once();self.step()
        self.assertEqual(self.world.snapshot()['status'],'user_paused')
        with self.owner._owner_scope():self.world.control('resume')
        self.assertEqual(self.world.snapshot()['status'],'running')
        positive,baseline=self._raw1070()
        obs=dict(mode='owned-public-SQLite-root-mutate-fault-seam',phase='positive',positive=False,
            injected=False,entered=False,loaded_owned_bad=False,code=None,exact_world_error=False,domain_error=False)
        expected=copy.deepcopy(positive);before=after=bad=None
        try:
            obs['committed']=self._committed1070(positive)
            obs['host_pair']=type(self.host) is ActorHost and self.host.adjudicator is self.adjudicator and self.host.dm_provider==self._dm1070
            obs['positive_shape']=self._shape1070(positive,baseline)
            obs['positive_project']=len(self.world._project_accounts(positive))==3
            obs['positive']=self.world._production_accounts(positive) is positive['economy']
            def positive_advance(state,db):
                self.world._advance_state(state,db,state['time'])
            with self.owner._owner_scope():self.world._mutate(positive_advance)
            obs['positive_advance']=self._raw1070()[0]==positive
            self.assertTrue(obs['committed'] and obs['host_pair'] and obs['positive'] and obs['positive_project'] and obs['positive_advance'] and all(obs['positive_shape'].values()))
            self._corrupt1070(expected,'missing-node');expected['revision']+=1
            obs['phase']='injection'
            def inject(state,db):
                self._corrupt1070(state,'missing-node');obs['injected']=True
            # Real existing trusted-root callback seam; all original frozen,
            # source/connection/history triggers and lease checks stay installed.
            with self.owner._owner_scope():self.world._mutate(inject)
            bad,_=self._raw1070();obs['persisted_exact']=bad==expected
            obs['negative_shape']=self._shape1070(bad,baseline)
            self.assertTrue(obs['persisted_exact'] and all(obs['negative_shape'].values()))
            before=self._business1070();obs['phase']='negative'
            def advance_bad(state,db):
                obs['entered']=True
                obs['loaded_owned_bad']=self.world._dynamic_state(state)==bad
                obs['source_gate']='WorldStore._mutate SQL latest -> _advance_state -> _production_accounts -> _project_accounts'
                self.world._advance_state(state,db,state['time'])
            with self.owner._owner_scope():self.world._mutate(advance_bad)
        except BaseException as error:self._safe_error1070(obs,error)
        finally:
            try:after=self._business1070()
            finally:self.save('1070-owned-persistent-missing-node',dict(observation=obs,positive=positive,
                expected_bad=expected,persisted_bad=bad,before=before,after=after))
        self.assertTrue(obs['committed']);self.assertTrue(obs['host_pair']);self.assertTrue(obs['positive']);self.assertTrue(obs['positive_project']);self.assertTrue(obs['positive_advance'])
        self.assertTrue(all(obs['positive_shape'].values()));self.assertTrue(all(obs['negative_shape'].values()))
        self.assertEqual(obs['phase'],'negative');self.assertTrue(obs['injected']);self.assertTrue(obs['persisted_exact'])
        self.assertTrue(obs['entered']);self.assertTrue(obs['loaded_owned_bad'])
        self.assertTrue(obs['exact_world_error']);self.assertFalse(obs['domain_error']);self.assertEqual(obs['code'],'INVALID_PROJECT_STATE')
        self.assertEqual({k:v['business'] for k,v in after.items()},{k:v['business'] for k,v in before.items()})
        self.assertLess(time.monotonic(),self.deadline,'wholemethod cumulative120 exceeded; no renewal')

    def test_sql_material_original_node_identity_negatives1070(self):
        self.build();self.propose_once();self.step()
        self._material_cases1070([(case,'INVALID_PROJECT_STATE') for case in ('duplicate-node','node-at','node-cause')])

    def test_sql_material_tool_ownership_and_derived_books_negatives1070(self):
        self.build();self.propose_once();self.step()
        self._material_cases1070([(case,'INVALID_PRODUCTION_STATE') for case in ('tool-stock-and-held','consumed-book')])

    def test_sql_material_forged_and_not_delivered_knowledge_negatives1070(self):
        c=configuration();c['local_message_seconds']=60
        self.build(c)
        self.policy=lambda v,s:((dict(action='propose',text=TEXT) if v['person']['id']==PID and v['time'] in (0,3600) else dict(action='wait')),s)
        self.effects=lambda p,s:([start()] if s['time']==0 else [dict(action='share_project_knowledge',to=KING,knowledge_id=KNOWLEDGE)])
        self.step();self.step()
        self._material_cases1070([(case,'INVALID_PROJECT_STATE') for case in ('forged-source','undelivered-knowledge')])

    def test_sql_material_stage_time_and_terminal_negatives1070(self):
        self.build();self.propose_once();self.step();self.step()
        self._material_cases1070([(case,'INVALID_PROJECT_STATE') for case in ('wrong-due','terminal-time','terminal-status')])

    def test_two_stage_due_knowledge_then_actual_manufacture_and_cas_replay(self):
        self.build()
        def policy(v,s):
            if v['person']['id']==KING and v['time']==0:return dict(action='send',to=PID,text='请于真实送达时检查阶段尚未到期'),s
            if v['person']['id']==PID and v['time'] in (0,3600):return dict(action='propose',text=TEXT),s
            return dict(action='wait'),s
        self.policy=policy
        def effects(p,s):
            if s['time']==0:return [start()]
            task=s['projects']['tasks'][0]
            return [dict(action='continue_project',project_task_id=task['id']),dict(action='send',to=KING,text='仍须等待制造到期')]
        self.effects=effects
        first=self.step();t=first['projects']['tasks'][0]
        self.assertEqual((first['time'],t['status']),(0,'pending'))
        self.assertEqual(first['economy']['materials']['place-0']['anvil'],0)
        self.assertEqual(first['economy']['materials']['place-0']['iron_tool'],2)
        self.assertEqual(first['economy']['material_ledger']['consumed']['place-0']['iron'],1)
        self.assertEqual(first['projects']['knowledge'],[])
        self.replay()
        before=self.step();self.assertEqual(before['time'],3599)
        self.assertEqual(before['projects']['tasks'][0]['runs'][0]['status'],'pending')
        research=self.step();self.assertEqual(research['time'],3600)
        task=research['projects']['tasks'][0]
        self.assertEqual([r['status'] for r in task['runs']],['completed','pending'])
        self.assertEqual(research['economy']['materials']['place-0']['iron_tool'],2)
        self.assertEqual(research['projects']['knowledge'][0]['person_id'],PID)
        self.assertFalse(any(g['person_id']==KING for g in research['projects']['knowledge']))
        before=self.step();self.assertEqual(before['time'],7199)
        self.assertEqual(before['economy']['materials']['place-0']['iron_tool'],2)
        final=self.step();self.assertEqual(final['time'],7200)
        self.assertEqual(final['projects']['tasks'][0]['status'],'completed')
        self.assertEqual(final['economy']['materials']['place-0']['iron_tool'],3)
        self.assertEqual(final['economy']['materials']['place-0']['anvil'],1)
        self.assertEqual(len([e for e in final['events'] if e['kind']=='project_stage_completed']),2)
        self.assertTrue(self.previews);self.closure()

    def test_king_cannot_organize_project(self):
        self.build();self.propose_once(KING);self.rejection('NOT_AUTHORIZED',KING)

    def test_unacquired_knowledge_cannot_be_shared(self):
        self.build();self.effects=lambda p,s:[dict(action='share_project_knowledge',to=KING,knowledge_id=KNOWLEDGE)]
        self.propose_once();self.rejection('KNOWLEDGE_REQUIRED')
        self.assertEqual(self.world.snapshot()['projects']['knowledge'],[])

    def test_tool_contention_rolls_back_entire_compound(self):
        self.build();self.effects=lambda p,s:[start(),start()]
        self.propose_once();self.rejection('INSUFFICIENT_TOOLS',index=1)
        self.assertFalse(any(q['kind']=='project_stage' for q in self.world.snapshot()['queue']))

    def test_actual_material_shortage_has_no_business_side_effects(self):
        self.build()
        self.effects=lambda p,s:[dict(action='ship_material',to='place-1',resource_id='iron',quantity=20),start()]
        self.propose_once();self.rejection('INSUFFICIENT_MATERIALS',index=1)
        self.assertNotIn('transport',self.world.snapshot()['economy'])

    def test_labor_contention_with_existing_production_rolls_back_both(self):
        c=configuration();c['economy']['labor_hours_per_day']['place-0']=24
        self.build(c)
        self.effects=lambda p,s:[dict(action='start_production',recipe_id='forge_agricultural_tool',quantity=1),start()]
        self.propose_once();self.rejection('INSUFFICIENT_LABOR',index=1)

    def test_dm_failure_consumes_real_inputs_but_no_early_or_final_reward(self):
        self.build();self.ruling='失败';self.propose_once()
        first=self.step();self.assertEqual(first['projects']['tasks'][0]['status'],'pending')
        self.assertEqual(first['projects']['knowledge'],[])
        final=self.until(3600)
        task=final['projects']['tasks'][0]
        self.assertEqual(task['status'],'failed');self.assertEqual(task['runs'][0]['labor_elapsed_seconds'],3600)
        self.assertEqual(final['economy']['materials']['place-0']['anvil'],1)
        self.assertEqual(final['economy']['materials']['place-0']['iron'],19)
        self.assertEqual(final['economy']['materials']['place-0']['iron_tool'],2)
        self.assertEqual(final['projects']['knowledge'],[]);self.replay();self.closure()

    def test_real_travel_interrupts_spent_stage_without_refund_or_reward(self):
        self.build()
        def policy(v,s):
            if v['person']['id']==KING and v['time']==0:return dict(action='send',to=PID,text='真实信使到达后可选择离开'),s
            if v['person']['id']==PID and v['time']==0:return dict(action='propose',text=TEXT),s
            if v['person']['id']==PID and v['time']==3599:return dict(action='travel',to='place-1'),s
            return dict(action='wait'),s
        self.policy=policy;self.step();s=self.step()
        task=s['projects']['tasks'][0]
        self.assertEqual((task['status'],task['runs'][0]['labor_elapsed_seconds']),('interrupted',3599))
        self.assertEqual(task['runs'][0]['actual_labor_hours'],dict(numerator=3599,denominator=3600))
        self.assertEqual(s['economy']['materials']['place-0']['iron'],19)
        self.assertEqual(s['economy']['materials']['place-0']['anvil'],1)
        self.assertFalse(any(q['kind']=='project_stage' for q in s['queue']))
        self.assertEqual(s['projects']['knowledge'],[])
        s=self.until(3600)
        self.assertFalse(any(e['kind']=='project_stage_completed' for e in s['events']));self.closure()

    def test_explicit_interrupt_releases_only_tools_and_future_commitments(self):
        self.build()
        self.effects=lambda p,s:[start(),dict(action='interrupt_project',project_task_id='project-'+Adjudicator._child_id(p['intent_id'],1,0))]
        self.propose_once();s=self.step();task=s['projects']['tasks'][0]
        self.assertEqual(task['status'],'interrupted')
        self.assertEqual(s['economy']['material_ledger']['consumed']['place-0']['iron'],1)
        self.assertEqual(s['economy']['materials']['place-0']['timber'],20)
        self.assertEqual(s['economy']['materials']['place-0']['anvil'],1)
        self.assertEqual(s['projects']['knowledge'],[]);self.replay();self.closure()

    def test_completion_p04_wake_and_legal_courier_do_not_grant_global_secrets(self):
        c=configuration();c['local_message_seconds']=60
        self.build(c)
        def policy(v,s):
            if v['person']['id']==PID and v['time'] in (0,3600):return dict(action='propose',text=TEXT),s
            return dict(action='wait'),s
        self.policy=policy
        self.effects=lambda p,s:([start()] if s['time']==0 else [dict(action='share_project_knowledge',to=KING,knowledge_id=KNOWLEDGE)])
        self.step();research=self.step();self.assertEqual(research['time'],3600)
        recent=max(self.rows(self.world.db_path)['coordinator_windows'],key=lambda w:w['ordinal'])
        self.assertEqual(json.loads(recent['roster_json']),[PID])
        self.assertFalse(any(g['person_id']==KING for g in research['projects']['knowledge']))
        self.assertFalse(any(m.get('project_knowledge_source') for m in self.world.observe(KING)['messages']))
        delivered=self.step();self.assertEqual(delivered['time'],3660)
        grants=delivered['projects']['knowledge'];self.assertEqual({g['person_id'] for g in grants},{PID,KING})
        self.assertEqual(next(g for g in grants if g['person_id']==KING)['kind'],'message')
        recent=max(self.rows(self.world.db_path)['coordinator_windows'],key=lambda w:w['ordinal'])
        self.assertEqual(json.loads(recent['roster_json']),[KING])
        for person in delivered['persons']:
            if person['id'] not in (PID,KING):
                self.assertNotIn(configuration()['staged_projects']['knowledge'][KNOWLEDGE],json.dumps(self.world.observe(person['id']),ensure_ascii=False))
        self.closure()

    def test_actual_delivered_command_and_voluntary_response_interrupts_local_work(self):
        c=configuration()
        marshal=next(p for p in c['countries'][0]['positions'] if p['office']=='marshal')
        marshal['location']='place-0';mid=marshal['person_id']
        self.build(c)
        def policy(v,s):
            pid=v['person']['id']
            if pid==KING and v['time']==0:return dict(action='command',to=mid,text='请自行决定是否限制聚集'),s
            if pid==PID and v['time']==0:return dict(action='propose',text=TEXT),s
            if pid==mid and v['time']==3599:
                message=next(m for m in v['messages'] if m.get('command_id'))
                return dict(action='respond',command_id=message['command_id'],accept=True),s
            return dict(action='wait'),s
        self.policy=policy;first=self.step()
        self.assertFalse(next(p for p in first['places'] if p['id']=='place-0')['disease']['restricted'])
        self.assertEqual(first['projects']['tasks'][0]['status'],'pending')
        s=self.step();self.assertEqual(s['time'],3599)
        self.assertTrue(next(p for p in s['places'] if p['id']=='place-0')['disease']['restricted'])
        self.assertEqual(s['projects']['tasks'][0]['status'],'interrupted')
        self.assertEqual(s['projects']['tasks'][0]['runs'][0]['actual_labor_hours'],dict(numerator=3599,denominator=3600))
        self.assertEqual(s['economy']['materials']['place-0']['anvil'],1)
        self.assertEqual(s['economy']['materials']['place-0']['iron'],19)
        self.assertEqual(s['projects']['knowledge'],[])
        self.assertFalse(any(q['kind']=='project_stage' for q in s['queue']))
        self.closure()

    def test_future_90_day_horizon_reopen_same_node_and_continue_original_world(self):
        c=configuration();c['staged_projects']['projects'][PROJECT]['stages'][0]['duration_seconds']=91*DAY
        # No parent/actor/busy budget is extended for this long simulated task.
        self.build(c);self.propose_once();first=self.step()
        task=copy.deepcopy(first['projects']['tasks'][0]);world_id=first['world_id']
        paused=self.until(90*DAY)
        self.assertEqual(paused['status'],'horizon_paused')
        self.assertEqual(paused['projects']['tasks'][0],task)
        self.assertIn(task['runs'][0]['node'],paused['queue']);self.assertEqual(paused['projects']['knowledge'],[])
        heads=self.rows(self.host.db_path)['actor_session_heads']
        self.owner.close();self.assertFalse(self.owner.cleanup_pending);self.assertFalse(self.host.cleanup_pending)
        self.world=WorldStore(self.root/'public.sqlite',self.root/'scenario.json');self.world.initialize()
        self.assertEqual(self.world.snapshot(),paused)
        self.host=ActorHost(self.world,self.root/'private.sqlite');self.owner=Coordinator(self.world,self.host)
        self.bind(initialize_dm=True)
        self.assertEqual(self.rows(self.host.db_path)['actor_session_heads'],heads)
        finished=self.until(91*DAY)
        self.assertEqual(finished['world_id'],world_id)
        self.assertEqual(finished['projects']['tasks'][0]['runs'][0]['node'],task['runs'][0]['node'])
        self.assertEqual(finished['projects']['tasks'][0]['status'],'ready')
        self.assertEqual(len(finished['projects']['tasks'][0]['runs']),1)
        self.assertEqual(len([e for e in finished['events'] if e['kind']=='project_stage_completed']),1)
        self.assertEqual(finished['economy']['materials']['place-0']['iron_tool'],2)
        self.assertEqual(finished['economy']['materials']['place-0']['anvil'],1)
        self.closure()

    def test_before_research_due_cannot_continue_or_receive_manufactured_goods(self):
        self.build()
        self.effects=lambda p,s:[start(),dict(action='continue_project',project_task_id='project-'+Adjudicator._child_id(p['intent_id'],1,0))]
        self.propose_once();self.rejection('PROJECT_NOT_READY',index=1)
        self.assertEqual(self.world.snapshot()['projects']['knowledge'],[])

    def test_manufacturing_failure_keeps_research_but_never_produces_goods(self):
        self.build()
        self.policy=lambda v,s:((dict(action='propose',text=TEXT) if v['person']['id']==PID and v['time'] in (0,3600) else dict(action='wait')),s)
        def effects(p,s):
            if s['time']==0:return [start()]
            return [dict(action='continue_project',project_task_id=s['projects']['tasks'][0]['id'])]
        self.effects=effects
        self.step();self.ruling='失败';research=self.step()
        self.assertEqual(research['time'],3600)
        self.assertEqual([r['status'] for r in research['projects']['tasks'][0]['runs']],['completed','pending'])
        self.assertEqual(len(research['projects']['knowledge']),1)
        final=self.step();self.assertEqual(final['time'],7200)
        self.assertEqual(final['projects']['tasks'][0]['status'],'failed')
        self.assertEqual(len(final['projects']['knowledge']),1)
        self.assertEqual(final['economy']['materials']['place-0']['iron_tool'],2)
        self.assertEqual(final['economy']['material_ledger']['consumed']['place-0']['iron'],3)
        self.assertEqual(final['economy']['materials']['place-0']['anvil'],1)
        self.closure()

    def test_optin_exact_schema_and_raw_actor_effect_denied(self):
        c=configuration();c['staged_projects']['version']='unknown'
        (self.root/'invalid.json').write_text(json.dumps(c),encoding='utf-8')
        w=WorldStore(self.root/'invalid.sqlite',self.root/'invalid.json')
        with self.assertRaises(WorldError) as caught:w.initialize()
        self.assertEqual(caught.exception.code,'INVALID_SCENARIO')
        self.build();self.propose_once();self.step();before=self.world.snapshot()
        with self.owner._owner_scope(),self.assertRaises(WorldError) as caught:
            self.world.submit(PID,'raw-staged-effect',start())
        self.assertEqual(caught.exception.code,'INVALID_BODY');self.assertEqual(self.world.snapshot(),before)
        self.closure()

    def tearDown(self):
        if self.owner is not None:
            self.owner.close();self.assertFalse(self.owner.cleanup_pending)
        elif self.host is not None:self.host.close()
        if self.host is not None:self.assertFalse(self.host.cleanup_pending)
        self.save('final-source-defined-evidence',dict(state=self.world.snapshot() if self.world is not None else None,
            windows=self.windows,views=self.views,requests=self.requests,previews=self.previews,
            dm_calls=self.dm_calls,model_layer='controlled-callback-no-sdk-no-model'))
