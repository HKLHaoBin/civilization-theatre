"""P08 public backend only: native SQLite, scripted DM, no registered runtime.

Fictional forge inputs/rates are game assumptions, not calibrated manufacture.
Corrupt-state/marker fixtures are fault injection, never authority or permission.
"""
import copy
import hashlib
import json
import sqlite3
import sys
import unittest
import uuid
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from app.world import WorldStore, WorldError, DomainActionError, DAY
from app.adjudication import Adjudicator
from app import world as WM

DATA = Path(WM.__file__).with_name('data')
SCRATCH = Path.home() / 'AppData/Local/hermes/cache/scratch'
RID = 'forge_agricultural_tool'
PID = 'person-country-1-finance'
TEXT = ' 原话：请组织本地工班用现有铁炭木锻造农具。\n'
LIMIT = 2**63-1


def config(quantity=1, capacity=8):
    c = json.loads((DATA/'scenarios/iron-4.json').read_bytes())
    c.update(schema_version=3, scenario_id='p08-production-owned-v1', version='p08-owned-production-v1')
    del c['harvest_day']; del c['harvest_quantity']
    places = [p for country in c['countries'] for p in country['places']]
    c['economy'] = dict(version='p08-production-economy-v1',
        assumptions='Fictional uncalibrated game manufacture; sequential batches, fixed crews.',
        units=dict(grain='grain_unit',iron='iron_unit',charcoal='charcoal_unit',timber='timber_unit',iron_tool='tool_unit'),
        initial_materials={p['id']:dict(iron=quantity*20,charcoal=quantity*10,timber=quantity*10,iron_tool=2) for p in places},
        labor_hours_per_day={p['id']:capacity for p in places},
        recipes={RID:dict(inputs=dict(iron=2,charcoal=1,timber=1),outputs=dict(iron_tool=1),labor_hours=8,duration_seconds=DAY)})
    return c


def effect(q=1):
    return dict(action='start_production', recipe_id=RID, quantity=q)


def decision(w, effects, ruling='成功'):
    s = w.snapshot()
    return dict(world_id=s['world_id'],base_revision=s['revision'],ruling=ruling,
                reason='纯脚本DM按真实本地条件裁定',goal='制造农具',costs=[],benefits=[],effects=effects)


def sql(path):
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
        master=db.execute('SELECT type,name,tbl_name,rootpage,sql FROM sqlite_master ORDER BY type,name').fetchall()
        tables={}
        for kind,name,*_ in master:
            if kind=='table':
                tables[name]=sorted(db.execute('SELECT * FROM "'+name.replace('"','""')+'"').fetchall(),key=repr)
        return dict(master=master,tables=tables)


def capture(w, a):
    s=w.snapshot(); st=w.db_path.stat()
    return dict(sql=sql(w.db_path),public=s,trace=a.trace(),
        views={p['id']:w.observe(p['id']) for p in s['persons']},
        file=dict(sha256=hashlib.sha256(w.db_path.read_bytes()).hexdigest(),size=st.st_size,mtime_ns=st.st_mtime_ns,inode=st.st_ino))


class ProductionLoopTests(unittest.TestCase):
    def setUp(self):
        self.root=SCRATCH/('p08-production-test-'+uuid.uuid4().hex)
        self.root.mkdir(parents=True,exist_ok=False)
        self.worlds=[]

    def own(self,c=None):
        n=str(len(self.worlds)); p=self.root/(n+'.json')
        p.write_text(json.dumps(config() if c is None else c,ensure_ascii=False,allow_nan=False),encoding='utf-8')
        w=WorldStore(self.root/(n+'.sqlite'),p); w.initialize(); w.control('resume')
        a=Adjudicator(w); self.worlds.append((w,a)); return w,a

    def save(self,label,value):
        (self.root/(label+'.json')).write_text(json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False),encoding='utf-8')

    def prepared(self,w,a,i='start',effects=None,pid=PID,ruling='成功'):
        before=w.snapshot()
        a.propose(pid,i,TEXT)
        v=a.record(pid,i,decision(w,[effect()] if effects is None else effects,ruling))
        self.assertEqual(w.snapshot(),before,'metadata must not advance revision')
        return v

    def accounts(self,s):
        e=s['economy']; l=e['material_ledger']
        for rid in set(e['units'])-{'grain'}:
            with self.subTest(account=rid):
                lhs=sum(l['initial'][p][rid]+l['produced'][p][rid]-l['consumed'][p][rid]-l['lost'][p][rid] for p in e['materials'])
                held=sum(t['reserved_inputs'].get(rid,0) for t in e['tasks'] if t['status']=='pending')
                self.assertEqual(lhs,sum(v[rid] for v in e['materials'].values())+held)
        self.assertEqual(sum(p['grain'] for p in s['places'])+sum(x['quantity'] for x in s['in_transit'])+s['ledger']['consumed'],s['ledger']['initial']+s['ledger']['harvested'])

    def test_A_real_preview_atomic_reservation_replay(self):
        w,a=self.own(); v=self.prepared(w,a)
        before=capture(w,a); self.save('before',before)
        try:
            p=a.preview(PID,'start',v)
        except WorldError as err:
            self.save('red-observation',dict(code=err.code,after=capture(w,a)))
            self.fail('valid typed production must preview through original API, rejected '+err.code)
        self.assertEqual(capture(w,a),before,'preview must leave ALL SQL, public, trace, views and physical identity unchanged')
        self.assertEqual(p['projected']['time'],before['public']['time'])
        r=a.commit(PID,'start',v); after=capture(w,a); self.save('after',after)
        self.assertEqual(r['effect']['effects'],p['effects'])
        self.assertEqual(after['public'],p['projected'])
        t=after['public']['economy']['tasks'][0]; child=a._child_id('start',v,0)
        self.assertEqual(t['id'],'production-'+child)
        self.assertEqual(t['cause_id'],child)
        self.assertEqual(t['reserved_inputs'],dict(iron=2,charcoal=1,timber=1))
        self.assertEqual(t['expected_outputs'],dict(iron_tool=1))
        self.assertEqual((t['owner_id'],t['place_id'],t['country_id'],t['quantity'],t['generation'],t['status']), (PID,'place-0','country-1',1,1,'pending'))
        self.assertEqual((t['started_at'],t['due'],t['labor_hours']), (0,DAY,8))
        self.assertEqual(t['rules_sha256'],after['public']['economy']['rules_sha256'])
        self.assertEqual(after['public']['economy']['materials']['place-0'],dict(iron=18,charcoal=9,timber=9,iron_tool=2))
        self.assertTrue(all(v==0 for row in after['public']['economy']['material_ledger']['consumed'].values() for v in row.values()))
        q=[q for q in after['public']['queue'] if q['kind']=='production']; self.assertEqual(len(q),1)
        self.assertEqual(q[0]['cause_id'],child); self.assertEqual(q[0]['task_id'],t['id'])
        self.assertEqual(w.receipt(PID,child)['effect'],r['effect']['effects'][0])
        self.assertEqual(a.proposal(PID,'start')['text'],TEXT)
        self.assertEqual(a.commit(PID,'start',v),r); self.assertEqual(capture(w,a),after)
        with self.assertRaises(WorldError): a.propose(PID,'start','changed')
        with self.assertRaises(WorldError): a.propose('person-country-1-king','start',TEXT)
        changes=after['trace']['proposals'][0]['committed']['resource_changes']
        iron=next(x for x in changes if x['resource']=='iron')
        self.assertEqual((iron['delta'],iron['reserved_delta'],iron['unit']),(-2,2,'iron_unit'))
        self.assertEqual(after['trace']['proposals'][0]['committed']['tasks'][0]['status'],'pending')
        self.assertNotIn('economy',after['views'][PID])
        self.accounts(after['public'])

    def test_A_scripted_resolve_once(self):
        w,a=self.own(); calls=[]
        def provider(req):
            calls.append(copy.deepcopy(req)); return decision(w,[effect()])
        r=a.resolve(PID,'scripted',TEXT,provider); before=capture(w,a)
        self.assertEqual(a.resolve(PID,'scripted',TEXT,provider),r)
        self.assertEqual(len(calls),1); self.assertEqual(calls[0]['proposal']['text'],TEXT)
        self.assertEqual(capture(w,a),before); self.accounts(w.snapshot())
        self.save('scripted-provider-not-model',dict(calls=calls,receipt=r,state=before))

    def test_A_business_negatives_and_four_rulings(self):
        for label,pid,change,expected in [
            ('king','person-country-1-king',None,'NOT_AUTHORIZED'),
            ('marshal','person-country-1-marshal',None,'NOT_AUTHORIZED'),
            ('foreign',PID,'foreign','NOT_AUTHORIZED'),
            ('travel',PID,'travel','NOT_AT_LOCATION'),
            ('stock',PID,'stock','INSUFFICIENT_MATERIALS'),
            ('labor',PID,'labor','INSUFFICIENT_LABOR')]:
            with self.subTest(case=label):
                c=config(capacity=7 if change=='labor' else 8)
                if change=='stock': c['economy']['initial_materials']['place-0']['iron']=0
                w,a=self.own(c)
                if change=='foreign':
                    journey=w.submit(PID,'foreign-travel',dict(action='travel',to='place-10'))
                    w.advance(journey['effect']['due'])
                if change=='travel': w.submit(PID,'travel',dict(action='travel',to='place-1'))
                v=self.prepared(w,a,pid=pid); before=capture(w,a)
                for fn in (a.preview,a.commit):
                    with self.assertRaises(DomainActionError) as err: fn(pid,'start',v)
                    self.assertEqual(err.exception.code,expected); self.assertEqual(capture(w,a),before)
                self.save(label,dict(before=before,after=capture(w,a),code=expected))
        for ruling in ('大失败','失败','成功','大成功'):
            with self.subTest(ruling=ruling):
                w,a=self.own();v=self.prepared(w,a,ruling=ruling)
                r=a.commit(PID,'start',v); self.assertEqual(r['effect']['effects'][0]['implemented'],ruling in ('成功','大成功'))
                self.assertEqual(len(w.snapshot()['economy']['tasks']),int(ruling in ('成功','大成功')));self.accounts(w.snapshot())

    def test_A_compound_labor_rollback(self):
        w,a=self.own();v=self.prepared(w,a,effects=[effect(),effect()]); before=capture(w,a)
        for fn in (a.preview,a.commit):
            with self.assertRaises(DomainActionError) as err: fn(PID,'start',v)
            self.assertEqual(err.exception.code,'INSUFFICIENT_LABOR'); self.assertEqual(err.exception.effect_index,1)
            self.assertEqual(capture(w,a),before,'entire SQL/task/queue/receipts/timeline must roll back')
        self.save('compound-rollback',dict(before=before,after=capture(w,a)))

    def test_A_strict_fields_sequential_batch_and_old_mode(self):
        bad=[{},dict(effect(),extra=1),dict(effect(),recipe_id='unknown'),dict(effect(),recipe_id=True)]
        bad += [dict(effect(),quantity=q) for q in (True,1.0,0,-1,LIMIT+1,None)]
        for index,b in enumerate(bad):
            with self.subTest(invalid=index):
                w,a=self.own();v=self.prepared(w,a,effects=[b]);before=capture(w,a)
                with self.assertRaises(WorldError) as err:a.preview(PID,'start',v)
                self.assertNotIsInstance(err.exception,DomainActionError);self.assertEqual(capture(w,a),before)
        w,a=self.own(config(quantity=2));v=self.prepared(w,a,effects=[effect(2)]);a.commit(PID,'start',v)
        t=w.snapshot()['economy']['tasks'][0]
        self.assertEqual((t['due'],t['labor_hours'],t['reserved_inputs'],t['expected_outputs']), (2*DAY,16,dict(iron=4,charcoal=2,timber=2),dict(iron_tool=2)));self.accounts(w.snapshot())
        for q in (LIMIT,LIMIT//DAY+1):
            with self.subTest(overflow=q):
                w,a=self.own();v=self.prepared(w,a,effects=[effect(q)]);before=capture(w,a)
                with self.assertRaises(WorldError) as err:a.preview(PID,'start',v)
                self.assertNotIsInstance(err.exception,DomainActionError);self.assertEqual(capture(w,a),before)
        legacy=json.loads((DATA/'scenarios/iron-4.json').read_bytes())
        w,a=self.own(legacy);w._mutate(lambda s,db:s['_config'].update(economy={'opaque':True}))
        v=self.prepared(w,a);before=capture(w,a)
        with self.assertRaises(WorldError) as err:a.preview(PID,'start',v)
        self.assertEqual(err.exception.code,'UNSUPPORTED');self.assertEqual(capture(w,a),before)
        w,a=self.own()
        with self.assertRaises(WorldError):w.submit(PID,'actor-direct',effect())
        self.assertEqual(w.snapshot()['economy']['tasks'],[])

    def test_A_invalid_marker_failclosed_before_any_child(self):
        w,a=self.own();v=self.prepared(w,a)
        with closing(sqlite3.connect(w.db_path)) as db:
            db.execute('CREATE TABLE coordinator_scheduler_registration (invalid_marker TEXT)');db.commit()
        # Direct internal negative only: marker is not a valid registration or permit.
        before=sql(w.db_path);s=w.snapshot();original=copy.deepcopy(s)
        with closing(sqlite3.connect(w.db_path)) as db:
            db.execute('BEGIN IMMEDIATE')
            with self.assertRaises(WorldError) as err:
                a._execute(s,db,PID,'start',v,decision(w,[dict(action='wait'),effect()]),True)
            self.assertEqual(err.exception.code,'UNSUPPORTED')
            self.assertEqual(s,original)
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0],1)
            db.rollback()
        self.assertEqual(sql(w.db_path),before)
        self.save('illegal-marker-only',dict(sql_before=before,sql_after=sql(w.db_path),not_registered_runtime=True))

    def test_A_corrupt_account_fails_technical_and_rolls_back(self):
        w,a=self.own();w._mutate(lambda s,db:s['economy']['materials']['place-0'].update(iron=19))
        v=self.prepared(w,a);before=capture(w,a)
        for fn in (a.preview,a.commit):
            with self.assertRaises(WorldError) as err:fn(PID,'start',v)
            self.assertNotIsInstance(err.exception,DomainActionError);self.assertEqual(err.exception.code,'INVALID_PRODUCTION_STATE')
            self.assertEqual(capture(w,a),before)
        self.save('fault-account',dict(before=before,after=capture(w,a)))

    def test_B_due_once_reopen_replay_and_labor_release(self):
        w,a=self.own();v=self.prepared(w,a); receipt=a.commit(PID,'start',v)
        pending=w.snapshot();task=pending['economy']['tasks'][0]
        node=next(copy.deepcopy(q) for q in pending['queue'] if q['kind']=='production')
        self.save('B-real-start',dict(frame=capture(w,a),receipt=receipt,node=node))
        w.advance(task['due']-1)
        before=capture(w,a);self.save('B-before-due',before)
        self.assertEqual(before['public']['economy']['materials']['place-0']['iron_tool'],2)
        self.assertEqual(before['public']['economy']['tasks'][0]['status'],'pending')
        # New objects, same physical DB, no manual repair of the pending node.
        w=WorldStore(w.db_path);a=Adjudicator(w)
        self.assertEqual(w.snapshot()['economy'],before['public']['economy'])
        w.advance(task['due']);after=capture(w,a);self.save('B-at-due',after)
        done=after['public']['economy']['tasks'][0]
        self.assertEqual(done['status'],'completed','original clock must actually finish production at due')
        self.assertEqual(after['public']['economy']['materials']['place-0'],dict(iron=18,charcoal=9,timber=9,iron_tool=3))
        self.assertEqual(done['inputs'],task['inputs']);self.assertEqual(done['reserved_inputs'],{})
        self.assertEqual(done['actual_outputs'],task['expected_outputs'])
        self.assertEqual((done['completed_at'],done['actual_labor_hours']), (task['due'],8))
        events=[e for e in after['public']['events'] if e['kind']=='production_completed']
        self.assertEqual(len(events),1);self.assertEqual(events[0]['cause_id'],task['cause_id'])
        self.assertEqual((events[0]['id'],events[0]['time']),(done['completion_event_id'],task['due']))
        self.assertEqual(after['public']['economy']['material_ledger']['consumed']['place-0'],dict(iron=2,charcoal=1,timber=1,iron_tool=0))
        self.assertEqual(after['public']['economy']['material_ledger']['produced']['place-0'],dict(iron=0,charcoal=0,timber=0,iron_tool=1))
        self.accounts(after['public'])
        self.assertEqual(a.trace(pending['revision'])['proposals'][0]['committed']['tasks'][0]['status'],'pending')
        self.assertEqual(a.trace()['proposals'][0]['committed']['tasks'][0]['status'],'completed')
        self.assertEqual(a.commit(PID,'start',v),receipt);self.assertEqual(capture(w,a),after)
        self.assertEqual(w.receipt(PID,task['cause_id'])['effect'],receipt['effect']['effects'][0])
        def duplicate(s,db):
            w._schedule(s,node['at'],node['kind'],**{k:x for k,x in node.items() if k not in ('id','seq','at','kind')})
        w._mutate(duplicate);preduplicate=w.snapshot()
        w.advance(task['due']);postduplicate=w.snapshot()
        self.assertEqual(postduplicate['economy'],preduplicate['economy'])
        self.assertEqual(postduplicate['events'],preduplicate['events'])
        v2=self.prepared(w,a,i='second');a.commit(PID,'second',v2)
        self.assertEqual([t['status'] for t in w.snapshot()['economy']['tasks']],['completed','pending'])
        self.accounts(w.snapshot());self.save('B-replay-labor-release',capture(w,a))

    def test_B_real_ninety_day_hold_resume_once(self):
        w,a=self.own(config(quantity=91));v=self.prepared(w,a,effects=[effect(91)])
        r=a.commit(PID,'start',v);start=w.snapshot();t=start['economy']['tasks'][0]
        self.assertGreater(t['due'],90*DAY)
        horizon=w.advance(90*DAY);self.save('B-90-day-pending',capture(w,a))
        self.assertEqual((horizon['time'],horizon['status']),(90*DAY,'horizon_paused'))
        self.assertEqual(horizon['economy'],start['economy'])
        self.assertEqual(len([q for q in horizon['queue'] if q['kind']=='production']),1)
        self.assertFalse(any(e['kind']=='production_completed' for e in horizon['events']))
        self.accounts(horizon)
        w=WorldStore(w.db_path);a=Adjudicator(w);w.control('resume')
        self.assertEqual(w.snapshot()['horizon'],180*DAY)
        w.advance(t['due']-1);self.assertEqual(w.snapshot()['economy'],start['economy'])
        end=w.advance(t['due']);self.accounts(end)
        self.assertEqual(end['economy']['tasks'][0]['status'],'completed')
        self.assertEqual(end['economy']['materials']['place-0']['iron_tool'],93)
        self.assertEqual(end['economy']['tasks'][0]['actual_labor_hours'],728)
        self.assertEqual(len([e for e in end['events'] if e['kind']=='production_completed']),1)
        self.assertEqual(a.commit(PID,'start',v),r)
        w.advance(t['due']+1)
        self.assertEqual(w.snapshot()['economy'],end['economy'])
        self.save('B-91-day-completed',capture(w,a))

    def test_B_bad_nodes_reservation_and_technical_fault_rollback(self):
        changes=[('generation',lambda s,q:q.update(generation=2)),
                 ('bool-generation',lambda s,q:q.update(generation=True)),
                 ('cause',lambda s,q:q.update(cause_id='unrelated')),
                 ('rules',lambda s,q:q.update(rules_sha256='0'*64)),
                 ('place',lambda s,q:q.update(place_id='place-1')),
                 ('task',lambda s,q:q.update(task_id='missing')),
                 ('early',lambda s,q:q.update(at=q['at']-1)),
                 ('late',lambda s,q:q.update(at=q['at']+1)),
                 ('reservation',lambda s,q:s['economy']['tasks'][0]['reserved_inputs'].update(iron=1)),
                 ('inputs',lambda s,q:s['economy']['tasks'][0]['inputs'].update(iron=1)),
                 ('quantity-bool',lambda s,q:s['economy']['tasks'][0].update(quantity=True)),
                 ('start-event',lambda s,q:s['economy']['tasks'][0].update(start_event_id='missing')),
                 ('fabricated-books',lambda s,q:(s['economy']['material_ledger']['produced']['place-0'].update(iron=1),s['economy']['materials']['place-0'].update(iron=19))),
                 ('fabricated-loss',lambda s,q:(s['economy']['material_ledger']['lost']['place-0'].update(iron=1),s['economy']['materials']['place-0'].update(iron=17)))]
        for label,change in changes:
            with self.subTest(fault=label):
                w,a=self.own();v=self.prepared(w,a);a.commit(PID,'start',v)
                def corrupt(s,db):
                    q=next(q for q in s['queue'] if q['kind']=='production');change(s,q)
                    s['queue'].sort(key=lambda q:(q['at'],q['seq']))
                w._mutate(corrupt);before=capture(w,a)
                with self.assertRaises(WorldError) as error:w.advance(DAY+1)
                self.assertNotIsInstance(error.exception,DomainActionError)
                self.assertEqual(capture(w,a),before,'native SQL/queue/time/events/materials/receipts must all rollback')
                self.save('B-bad-'+label,dict(before=before,after=capture(w,a),code=error.exception.code))
        from unittest.mock import patch
        w,a=self.own();v=self.prepared(w,a);a.commit(PID,'start',v);before=capture(w,a)
        original=w._event;observed=[]
        def fail_completion(state,kind,*args,**kwargs):
            if kind=='production_completed':
                observed.append(copy.deepcopy(state['economy']))
                raise WorldError('INJECTED_STORAGE_FAULT','synthetic technical fault, not story')
            return original(state,kind,*args,**kwargs)
        with patch.object(w,'_event',side_effect=fail_completion):
            with self.assertRaises(WorldError) as error:w.advance(DAY)
        self.assertEqual(error.exception.code,'INJECTED_STORAGE_FAULT');self.assertEqual(len(observed),1)
        self.assertEqual(observed[0]['materials']['place-0']['iron_tool'],3)
        self.assertEqual(capture(w,a),before)
        self.save('B-technical-rollback',dict(before=before,after=capture(w,a),inside_fault=observed))
        w.advance(DAY);done=capture(w,a)
        def wrong_completed(s,db):
            t=s['economy']['tasks'][0]
            w._schedule(s,t['due'],'production',task_id=t['id'],generation=2,place_id=t['place_id'],rules_sha256=t['rules_sha256'],cause_id=t['cause_id'])
        w._mutate(wrong_completed);before=capture(w,a)
        with self.assertRaises(WorldError):w.advance(DAY)
        self.assertEqual(capture(w,a),before)
        self.save('B-completed-wrong-generation',dict(healthy=done,before=before,after=capture(w,a)))

