"""P023b3 first vertical behavior; real Host/World/private, no SDK.

The controlled request seam's numeric SDK metrics are synthetic only.
"""
from contextlib import closing
import json
import sqlite3
import unittest

from app.coordinator_scheduler import ActivationRef
from app.tests import test_actor_activations as activation_fixtures


def database_rows(path):
    from pathlib import Path
    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
        names=[r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        return {name:db.execute('SELECT * FROM "'+name+'" ORDER BY rowid').fetchall() for name in names}


class ActivationDomainRejectionTests(unittest.TestCase):
    def test_characterization_rejection_actual_and_private_final_gates_roll_back(self):
        from app.world import WorldError
        for boundary in ('actual','private'):
            with self.subTest(boundary=boundary), activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
                location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
                original=world._check_actor_execution;hits=[];before=world.snapshot()
                def guard(db,identity,person,permit,**kwargs):
                    original(db,identity,person,permit,**kwargs)
                    row=db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                    if row and row[0]=='rejected' and row[1]==(1 if boundary=='private' else 0):
                        hits.append(True)
                        raise WorldError('ACTOR_EXECUTION_DEADLINE','injected-original-final-gate')
                world._check_actor_execution=guard
                with self.assertRaises(WorldError) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(caught.exception.code,'ACTOR_EXECUTION_DEADLINE')
                self.assertEqual(hits,[True])
                self.assertEqual(world.snapshot(),before)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('candidate',0) if boundary=='actual' else ('rejected',0))
                    self.assertIsNone(db.execute('SELECT receipt FROM intents').fetchone()[0])
                    has=db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_outcomes'").fetchone()
                    if boundary=='actual' and has:
                        self.assertEqual(db.execute('SELECT count(*) FROM coordinator_activation_outcomes').fetchone(),(0,))
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT state,receipt FROM actor_intents').fetchone(),('candidate',None))
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0,))

    def test_characterization_rejected_proof_corruption_is_zero_write_at_all_read_gates(self):
        from app.actor_execution import canonical,digest
        faults=('missing_outcome','hash','outcome_person','outcome_intent','outcome_action','outcome_effects','outcome_index',
                'private_state','private_receipt','main_receipt','candidate','request','calls','missing_head','closed_bit','promoted','base')
        for fault in faults:
            with self.subTest(fault=fault), activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
                location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
                result=host.act(ref.person_id,coordinator=owner,activation=ref)
                with closing(sqlite3.connect(world.db_path)) as db:
                    if fault=='missing_outcome': db.execute('DELETE FROM coordinator_activation_outcomes')
                    elif fault=='hash': db.execute("UPDATE coordinator_activation_outcomes SET outcome_sha256='bad'")
                    elif fault.startswith('outcome_'):
                        outcome=dict(result['outcome']);key=fault[8:]
                        key={'person':'person_id','intent':'intent_id','index':'effect_index'}.get(key,key)
                        outcome[key]=[{'invented':True}] if key=='effects' else (0 if key=='effect_index' else 'fake')
                        raw=canonical(outcome);db.execute('UPDATE coordinator_activation_outcomes SET outcome_json=?,outcome_sha256=?',(raw,digest(raw)))
                    elif fault=='main_receipt': db.execute("UPDATE intents SET receipt='{}'")
                    elif fault=='closed_bit': db.execute('UPDATE coordinator_activations SET private_closed=0')
                    elif fault=='promoted': db.execute('UPDATE coordinator_activations SET promoted_version=1')
                    db.commit()
                with closing(sqlite3.connect(host.db_path)) as db:
                    if fault=='private_state': db.execute("UPDATE actor_intents SET state='unknown'")
                    elif fault=='private_receipt': db.execute("UPDATE actor_intents SET receipt='{}'")
                    elif fault=='candidate': db.execute("UPDATE actor_intents SET candidate='[]'")
                    elif fault=='request': db.execute("UPDATE actor_intents SET request='{}'")
                    elif fault=='calls': db.execute('DELETE FROM actor_calls')
                    elif fault=='missing_head': db.execute('DELETE FROM actor_session_heads')
                    elif fault=='base': db.execute('UPDATE actor_intent_execution SET base_version=1')
                    db.commit()
                before=(database_rows(world.db_path),database_rows(host.db_path))
                for operation in (lambda: world.next_activation(ref.window_id,coordinator=owner),
                                  lambda: host.act(ref.person_id,coordinator=owner,activation=ref),
                                  lambda: world.open_or_recover_window(coordinator=owner,expected_version={k:world.snapshot()[k] for k in ('world_id','revision')}),
                                  lambda: world.complete_window(ref.window_id,coordinator=owner)):
                    with self.assertRaises(Exception): operation()
                    self.assertEqual((database_rows(world.db_path),database_rows(host.db_path)),before)
                self.assertEqual(len(requests),1)

    def test_characterization_provider_same_code_never_creates_domain_outcome(self):
        from app.world import WorldError
        for code in ('INSUFFICIENT_GRAIN','NOT_AUTHORIZED','ALREADY_THERE','NOT_AT_LOCATION'):
            with self.subTest(code=code), activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
                def bad(payload): raise WorldError(code,'provider spoof, not actual origin')
                host._request=bad
                with self.assertRaises(WorldError) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(caught.exception.code,code)
                self.assertEqual(world.snapshot()['status'],'technical_paused')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,resume_phase,diagnostic_code,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('technical_blocked','prepared',code,0))
                    exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_outcomes'").fetchone()
                    if exists: self.assertEqual(db.execute('SELECT count(*) FROM coordinator_activation_outcomes').fetchone(),(0,))
                    self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(0,))
                    self.assertEqual(db.execute('SELECT resume_phase,diagnostic_code,pause_revision FROM coordinator_activation_blocks WHERE activation_id=?',(ref.activation_id,)).fetchone(),('prepared',code,world.snapshot()['revision']))
                self.assertEqual(world.snapshot()['diagnostic']['code'],code)
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT state,candidate,receipt FROM actor_intents WHERE person_id=?',(ref.person_id,)).fetchall(),[('prepared',None,None)])
                    self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(0,None))

    def test_characterization_private_reject_trigger_rolls_back_both_sides_then_reconciles(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            close=host._reject_result
            def reject(*args,**kwargs):
                with closing(sqlite3.connect(host.db_path)) as db:
                    db.execute("CREATE TRIGGER fail_reject BEFORE UPDATE ON actor_intents WHEN NEW.state='rejected' BEGIN SELECT RAISE(ABORT,'private-reject-fault'); END");db.commit()
                return close(*args,**kwargs)
            host._reject_result=reject
            with self.assertRaisesRegex(sqlite3.IntegrityError,'private-reject-fault'):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(world.db_path)) as db:
                row=db.execute('SELECT phase,private_closed,intent_id FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                self.assertEqual(row[:2],('rejected',0))
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT state,receipt FROM actor_intents').fetchone(),('candidate',None))
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0,))
                db.execute('DROP TRIGGER fail_reject');db.commit()
            host._reject_result=close
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['intent_id'],row[2])
            self.assertEqual(len(requests),1)

    def test_unclosed_rejection_needs_full_proof_before_next_and_then_closes_original(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            close=host._reject_result
            def stop(*args,**kwargs):
                raise RuntimeError('injected-before-private-close')
            host._reject_result=stop
            with self.assertRaisesRegex(RuntimeError,'injected-before-private-close'):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            row=world.next_activation(ref.window_id,coordinator=owner)
            self.assertEqual((row['phase'],row['private_closed']),('rejected',0))
            with closing(sqlite3.connect(world.db_path)) as db:
                saved=db.execute('SELECT * FROM coordinator_activation_outcomes').fetchone()
                db.execute('DELETE FROM coordinator_activation_outcomes');db.commit()
            before=(database_rows(world.db_path),database_rows(host.db_path))
            with self.assertRaises(Exception):
                world.next_activation(ref.window_id,coordinator=owner)
            self.assertEqual((database_rows(world.db_path),database_rows(host.db_path)),before)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute('INSERT INTO coordinator_activation_outcomes VALUES (?,?,?,?,?,?)',saved);db.commit()
            host._reject_result=close
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['intent_id'],row['intent_id'])
            self.assertEqual(result['outcome']['activation_id'],ref.activation_id)
            self.assertEqual(len(requests),1)
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT state FROM actor_intents WHERE intent_id=?',(row['intent_id'],)).fetchone(),('rejected',))

    def test_rejected_leaf_invalid_metrics_cannot_acquire_or_replay(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            original=host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(host.db_path)) as db:
                metrics=json.loads(db.execute('SELECT metrics FROM actor_intents WHERE intent_id=?',(original['intent_id'],)).fetchone()[0])
                metrics['sdk_requests']=-1
                db.execute('UPDATE actor_intents SET metrics=? WHERE intent_id=?',(json.dumps(metrics),original['intent_id']));db.commit()
            before=(database_rows(world.db_path),database_rows(host.db_path))
            with self.assertRaises(Exception):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual((database_rows(world.db_path),database_rows(host.db_path)),before)

    def test_hidden_rejection_cannot_acquire_as_candidate(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            original=host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(world.db_path)) as db:
                db.execute("UPDATE coordinator_activations SET phase='candidate',private_closed=0,terminal_reason=NULL WHERE activation_id=?",(ref.activation_id,));db.commit()
            with closing(sqlite3.connect(host.db_path)) as db:
                db.execute("UPDATE actor_intents SET state='candidate' WHERE intent_id=?",(original['intent_id'],));db.commit()
            before=(database_rows(world.db_path),database_rows(host.db_path))
            with self.assertRaises(Exception):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual((database_rows(world.db_path),database_rows(host.db_path)),before,
                             'bad outcome/phase must reject before any lease/delegation write')
            self.assertEqual(len(requests),1)

    def test_delivered_command_respond_after_real_departure_is_domain_rejection(self):
        from app.adjudication import Adjudicator
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            state=world.snapshot();king=next(p for p in state['persons'] if p['id']==ref.person_id)
            recipient=next(p for p in state['persons'] if p['country_id']==king['country_id'] and p['office']=='finance')
            host._choose=lambda view,seen: ({'action':'command','to':recipient['id'],'text':'合法受令：清点库存'},seen)
            command=host.act(ref.person_id,coordinator=owner,activation=ref)['receipt']['effect']['command_id']
            host._choose=lambda view,seen: ({'action':'wait'},seen)
            while (row:=world.next_activation(ref.window_id,coordinator=owner)) is not None:
                other=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                host.act(other.person_id,coordinator=owner,activation=other)
            world.complete_window(ref.window_id,coordinator=owner)
            state=world.snapshot();window=world.open_or_recover_window(coordinator=owner,expected_version={k:state[k] for k in ('world_id','revision')})
            while True:
                row=world.next_activation(window['window_id'],coordinator=owner)
                ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                if ref.person_id==recipient['id']:
                    break
                host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(world.db_path)) as db:
                raw=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self.assertTrue(next(c for c in raw['_commands'] if c['id']==command)['delivered'])
            with world._coordinator_scope(owner):
                host.adjudicator=Adjudicator(world)
            target=next(p['id'] for p in world.snapshot()['places'] if p['id']!=recipient['location'])
            host._choose=lambda view,seen: ({'action':'propose','text':'先出发，再执行收到的原命令'},seen)
            host.dm_provider=lambda payload: dict(world_id=payload['world']['world_id'],base_revision=payload['world']['revision'],ruling='成功',reason='真实执行地点',goal='回应',costs=[],benefits=[],effects=[{'action':'travel','to':target},{'action':'respond','command_id':command,'accept':True}])
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref)
            except Exception as error:
                self.fail('legitimate delivered command location refusal must close: '+str(getattr(error,'code',error)))
            self.assertEqual(result['outcome']['code'],'NOT_AT_LOCATION')
            self.assertEqual(result['outcome']['action'],'respond')
            self.assertEqual(result['outcome']['effect_index'],1)

    def test_no_effect_only_window_can_complete_without_fabricated_revision(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            revision=world.snapshot()['revision'];count=0
            try:
                while ref is not None:
                    location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                    host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
                    host.act(ref.person_id,coordinator=owner,activation=ref)
                    count+=1
                    row=world.next_activation(ref.window_id,coordinator=owner)
                    window_id=ref.window_id
                    ref=None if row is None else ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                completed=world.complete_window(window_id,coordinator=owner)
                self.assertEqual(completed['completed_revision'],revision)
                version={k:world.snapshot()[k] for k in ('world_id','revision')}
                world.open_or_recover_window(coordinator=owner,expected_version=version)
            except Exception as error:
                self.fail('all-rejected window must have a real zero-revision terminal: '+str(getattr(error,'code',error)))
            self.assertEqual(len(requests),count)
            self.assertEqual(world.snapshot()['time'],2*86400)

    def test_adjudication_rejected_parent_is_immutable_and_all_entrypoints_stay_bound(self):
        from app.adjudication import Adjudicator
        from app.world import WorldError
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):
                adj=Adjudicator(world);host.adjudicator=adj
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            text='固定原文';body={'action':'propose','text':text}
            host._choose=lambda view,seen: (body,seen)
            host.dm_provider=lambda payload: dict(world_id=payload['world']['world_id'],base_revision=payload['world']['revision'],ruling='成功',reason='原裁定',goal='原地',costs=[],benefits=[],effects=[{'action':'travel','to':location}])
            close=host._reject_result;checked=[]
            def reject(person,intent,execution,attempt_id,**kwargs):
                result=close(person,intent,execution,attempt_id,**kwargs)
                original=adj.history(person,intent)[0]['decision']
                self.assertEqual(adj.propose(person,intent,text,execution=execution)['text'],text)
                with self.assertRaises(WorldError) as mismatch:
                    adj.propose(person,intent,text+'替换',execution=execution)
                self.assertEqual(mismatch.exception.code,'ACTIVATION_BINDING_CONFLICT')
                with self.assertRaises(WorldError) as frozen:
                    adj.record(person,intent,dict(original,reason='不可替换'),execution=execution)
                self.assertEqual(frozen.exception.code,'ALREADY_REJECTED')
                with self.assertRaises(WorldError) as preview:
                    adj.preview(person,intent,1,execution=execution)
                self.assertEqual(preview.exception.code,'ALREADY_REJECTED')
                self.assertEqual(adj.commit(person,intent,1,execution=execution),result['outcome'])
                self.assertEqual(adj.resolve(person,intent,text,lambda payload: self.fail('must not call DM'),execution=execution),result['outcome'])
                self.assertEqual(len(adj.history(person,intent)),1)
                checked.append(True)
                return result
            host._reject_result=reject
            host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(checked,[True])
            self.assertEqual(len(requests),1)

    def test_actual_world_null_receipt_retry_returns_saved_rejection(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            body={'action':'travel','to':location}
            host._choose=lambda view,seen: (body,seen)
            original=host._reject_result;replayed=[];applications=[];apply=world._apply_action
            def track(*args):
                applications.append(args[2])
                return apply(*args)
            world._apply_action=track
            def close(person,intent,execution,attempt_id,**kwargs):
                result=original(person,intent,execution,attempt_id,**kwargs)
                replayed.append(world.submit(person,intent,body,execution=execution))
                return result
            host._reject_result=close
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref)
            except Exception as error:
                self.fail('NULL receipt retry must load proven outcome: '+str(getattr(error,'code',error)))
            self.assertEqual(replayed,[result['outcome']])
            self.assertEqual(applications,[result['intent_id']])

    def test_in_transit_actual_route_and_ship_origins_reject_as_business(self):
        from app.adjudication import Adjudicator
        for action in ('send','command','travel','ship'):
            with self.subTest(action=action), activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
                with world._coordinator_scope(owner):
                    host.adjudicator=Adjudicator(world)
                state=world.snapshot();person=next(p for p in state['persons'] if p['id']==ref.person_id)
                place=next(p['id'] for p in state['places'] if p['id']!=person['location'])
                other=next(p['id'] for p in state['persons'] if p['country_id']==person['country_id'] and p['id']!=person['id'])
                second=({'action':action,'to':other,'text':'在途发送'} if action in ('send','command') else
                        {'action':action,'to':place,**({'quantity':1} if action=='ship' else {})})
                host._choose=lambda view,seen: ({'action':'propose','text':'真实旅途后的后续动作'},seen)
                host.dm_provider=lambda payload: dict(world_id=payload['world']['world_id'],base_revision=payload['world']['revision'],
                    ruling='成功',reason='原合法目标',goal='旅途',costs=[],benefits=[],effects=[{'action':'travel','to':place},second])
                try:
                    result=host.act(ref.person_id,coordinator=owner,activation=ref)
                except Exception as error:
                    self.fail('actual lawful transit origin must reject: '+str(getattr(error,'code',error)))
                self.assertEqual(result['outcome']['code'],'NOT_AT_LOCATION')
                self.assertEqual(result['outcome']['action'],action)
                self.assertEqual(result['outcome']['effect_index'],1)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%'").fetchone(),(0,))

    def test_normal_resolve_rechecks_preview_refusal_in_actual_original_commit(self):
        from app.adjudication import Adjudicator
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):
                host.adjudicator=Adjudicator(world)
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            target=next(p['id'] for p in world.snapshot()['places'] if p['id']!=location)
            host._choose=lambda view,seen: ({'action':'propose','text':'原文不换：出发再调查'},seen)
            dm_calls=[]
            def dm(payload):
                dm_calls.append(payload)
                return dict(world_id=payload['world']['world_id'],base_revision=payload['world']['revision'],ruling='成功',reason='原裁定',goal='调查',costs=[],benefits=[],effects=[{'action':'travel','to':target},{'action':'investigate'}])
            host.dm_provider=dm
            actual_execute=host.adjudicator._execute;executions=[];before=[]
            def execute(state,db,person,intent,version,decision,persist):
                executions.append((intent,version,persist))
                if persist:
                    before.append(world.snapshot())
                try:
                    return actual_execute(state,db,person,intent,version,decision,persist)
                finally:
                    if not persist:
                        self.assertEqual(world.snapshot()['revision'],state['revision'])
                        with closing(sqlite3.connect(world.db_path)) as check:
                            self.assertEqual(check.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%'").fetchone(),(0,))
                            self.assertEqual(check.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('candidate',))
            host.adjudicator._execute=execute
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except Exception as error:
                self.fail('normal preview refusal needs actual adjudication: '+str(getattr(error,'code',error)))
            self.assertEqual(executions,[(result['intent_id'],1,False),(result['intent_id'],1,True)])
            self.assertEqual(len(dm_calls),1)
            self.assertEqual(len(requests),1)
            self.assertEqual(world.snapshot(),before[0])
            self.assertEqual(result['outcome']['effect_index'],1)
            self.assertEqual(result['outcome']['code'],'NOT_AT_LOCATION')
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%'").fetchone(),(0,))
                self.assertEqual(db.execute('SELECT count(*) FROM dm_decisions').fetchone(),(1,))

    def test_actual_adjudicated_second_child_refusal_rolls_back_entire_effect_region(self):
        from app.adjudication import Adjudicator
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):
                host.adjudicator=Adjudicator(world)
            state=world.snapshot()
            location=next(p['location'] for p in state['persons'] if p['id']==ref.person_id)
            target=next(p['id'] for p in state['places'] if p['id']!=location)
            host._choose=lambda view,seen: ({'action':'propose','text':'先出发，再在现场调查'},seen)
            host.dm_provider=lambda payload: dict(world_id=payload['world']['world_id'],base_revision=payload['world']['revision'],
                ruling='成功',reason='真实复合',goal='出发调查',costs=[],benefits=[],effects=[{'action':'travel','to':target},{'action':'investigate'}])
            # Explicit actual-only orchestration seam, not normal resolve evidence.
            host.adjudicator.preview=lambda *args,**kwargs: None
            actual_commit=host.adjudicator.commit;before=[]
            def commit(*args,**kwargs):
                before.append(world.snapshot())
                return actual_commit(*args,**kwargs)
            host.adjudicator.commit=commit
            actual_apply=world._apply_action;tentative=[]
            def observe(state,person,intent,body,ruling):
                if body['action']=='investigate':
                    tentative.append((person['location'],len(state['queue'])))
                return actual_apply(state,person,intent,body,ruling)
            world._apply_action=observe
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except Exception as error:
                self.fail('actual compound refusal must close: '+str(getattr(error,'code',error)))
            self.assertEqual(result['outcome']['code'],'NOT_AT_LOCATION')
            self.assertEqual(result['outcome']['origin'],'adjudication.execute')
            self.assertEqual(result['outcome']['action'],'investigate')
            self.assertEqual(result['outcome']['effect_index'],1)
            self.assertEqual(result['outcome']['decision_version'],1)
            self.assertEqual(world.snapshot(),before[0])
            self.assertEqual(tentative,[(None,len(before[0]['queue'])+1)])
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%'").fetchone(),(0,))
                self.assertEqual(db.execute("SELECT count(*) FROM dm_timeline WHERE kind='commit'").fetchone(),(0,))
                self.assertEqual(db.execute('SELECT count(*) FROM dm_decisions').fetchone(),(1,))
                self.assertIsNone(db.execute('SELECT receipt FROM intents WHERE id=?',(result['intent_id'],)).fetchone()[0])
            self.assertEqual(len(requests),1)

    def test_multiple_rejected_leaves_allow_successor_spine_and_historical_replay(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            old_refs=[]; old_results=[]
            for window_index in range(3):
                location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
                host._choose=lambda view,seen: ({'action':'travel','to':location} if window_index<2 else {'action':'wait'},seen)
                try:
                    result=host.act(ref.person_id,coordinator=owner,activation=ref)
                    if window_index<2:
                        old_refs.append(ref);old_results.append(result)
                    host._choose=lambda view,seen: ({'action':'wait'},seen)
                    while True:
                        row=world.next_activation(ref.window_id,coordinator=owner)
                        if row is None:
                            break
                        other=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                        host.act(other.person_id,coordinator=owner,activation=other)
                    world.complete_window(ref.window_id,coordinator=owner)
                    if window_index<2:
                        state=world.snapshot()
                        window=world.open_or_recover_window(coordinator=owner,expected_version={k:state[k] for k in ('world_id','revision')})
                        row=world.next_activation(window['window_id'],coordinator=owner)
                        ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                except Exception as error:
                    self.fail('rejected leaves must permit proven successor chain: '+str(getattr(error,'code',error)))
            count=len(requests);before=world.snapshot()
            for old,original in zip(old_refs,old_results):
                replay=host.act(old.person_id,coordinator=owner,activation=old)
                self.assertEqual(replay['outcome'],original['outcome'])
            self.assertEqual(len(requests),count)
            self.assertEqual(world.snapshot(),before)
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(1,result['intent_id']))
                self.assertEqual(db.execute('SELECT base_version,promoted_version FROM actor_intent_execution WHERE person_id=? ORDER BY base_version',(ref.person_id,)).fetchall(),[(0,None),(0,None),(0,1)])

    def test_closed_rejection_replays_original_without_provider_or_promotion(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            original=host.act(ref.person_id,coordinator=owner,activation=ref)
            before=world.snapshot()
            try:
                replay=host.act(ref.person_id,coordinator=owner,activation=ref)
            except Exception as error:
                self.fail('closed reject replay must reconcile: '+str(getattr(error,'code',error)))
            self.assertEqual(replay['outcome'],original['outcome'])
            self.assertEqual(replay['intent_id'],original['intent_id'])
            self.assertTrue(replay['reconciled'])
            self.assertEqual(len(requests),1)
            self.assertEqual(world.snapshot(),before)
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(0,))

    def test_travel_to_actual_location_is_a_closed_rejection(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            person=next(p for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            body=dict(action='travel',to=person['location'])
            host._choose=lambda view,seen: (body,seen)
            before=world.snapshot()
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except Exception as error:
                self.fail('actual already-there must close: '+str(getattr(error,'code',error)))
            self.assertEqual(result['outcome']['code'],'ALREADY_THERE')
            self.assertEqual(world.snapshot(),before)
            self.assertIsNone(result['receipt'])

    def test_actual_office_authorization_rejects_without_pausing(self):
        registered = activation_fixtures.ActorActivationTests.registered
        for action in ('ship','command'):
            with self.subTest(action=action), registered(self) as (world,host,owner,ref,requests):
                state=world.snapshot()
                person=next(p for p in state['persons'] if p['id']==ref.person_id)
                if action=='ship':
                    target=next(p for p in state['places'] if p['country_id']==person['country_id'] and p['id']!=person['location'])
                    body=dict(action='ship',to=target['id'],quantity=1)
                else:
                    target=next(p for p in state['persons'] if p['country_id']!=person['country_id'])
                    body=dict(action='command',to=target['id'],text='合法目标，非本国受令者')
                host._choose=lambda view,seen: (body,seen)
                before=world.snapshot()
                try:
                    result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                except Exception as error:
                    self.fail('actual office rejection must close: '+str(getattr(error,'code',error)))
                self.assertEqual(result['outcome']['code'],'NOT_AUTHORIZED')
                self.assertEqual(result['outcome']['action'],action)
                self.assertEqual(world.snapshot(),before)
                self.assertIsNone(result['receipt'])
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT state FROM actor_intents WHERE intent_id=?',(result['intent_id'],)).fetchone(),('rejected',))

    def test_insufficient_grain_closes_rejected_without_effect_or_promotion(self):
        registered = activation_fixtures.ActorActivationTests.registered
        with registered(self) as (world, host, owner, ref, requests):
            # Reach the finance officer through actual prior activations.
            while True:
                state = world.snapshot()
                person = next(p for p in state['persons'] if p['id'] == ref.person_id)
                if person['office'] == 'finance':
                    break
                host.act(ref.person_id, coordinator=owner, activation=ref)
                row = world.next_activation(ref.window_id, coordinator=owner)
                self.assertIsNotNone(row)
                ref = ActivationRef(**{k: row[k] for k in
                                       ('world_id', 'window_id', 'activation_id', 'person_id')})
            target = next(p for p in state['places']
                          if p['country_id'] == person['country_id']
                          and p['id'] != person['location'])
            body = {'action': 'ship', 'to': target['id'], 'quantity': 1000000}
            host._choose = lambda view, seen: (dict(body), seen)
            before = world.snapshot()
            prior_requests = len(requests)
            try:
                result = host.act(ref.person_id, coordinator=owner, activation=ref,
                                  pause_on_failure=True)
            except Exception as error:
                self.fail('Actual insufficient-grain action must return proven domain rejection, '
                          'not propagate/pause as a technical fault: '
                          + str(getattr(error, 'code', type(error).__name__)))
            self.assertEqual(result['result_kind'], 'domain_rejected')
            self.assertIsNone(result['receipt'])
            self.assertEqual(result['body'], body)
            self.assertEqual(result['outcome']['code'], 'INSUFFICIENT_GRAIN')
            self.assertEqual(result['outcome']['effects'], [])
            self.assertEqual(len(requests), prior_requests + 1)
            self.assertEqual(world.snapshot(), before)
            self.assertIsNone(world.receipt(ref.person_id, result['intent_id']))
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute(
                    'SELECT phase,private_closed,receipt_sha256,promoted_version '
                    'FROM coordinator_activations WHERE activation_id=?',
                    (ref.activation_id,)).fetchone(), ('rejected', 1, None, None))
                payload, receipt = db.execute(
                    'SELECT payload,receipt FROM intents WHERE id=?',
                    (result['intent_id'],)).fetchone()
                self.assertEqual(json.loads(payload)['body'], body)
                self.assertIsNone(receipt)
                kind, raw = db.execute(
                    'SELECT kind,outcome_json FROM coordinator_activation_outcomes '
                    'WHERE activation_id=?', (ref.activation_id,)).fetchone()
                self.assertEqual(kind, 'domain_rejected')
                self.assertEqual(json.loads(raw), result['outcome'])
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute(
                    'SELECT state,receipt FROM actor_intents WHERE intent_id=?',
                    (result['intent_id'],)).fetchone(), ('rejected', None))
                self.assertEqual(db.execute(
                    'SELECT version,last_promoted_intent_id FROM actor_session_heads '
                    'WHERE person_id=?', (ref.person_id,)).fetchone(), (0, None))
