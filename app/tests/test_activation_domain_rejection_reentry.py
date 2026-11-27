"""Original candidate retries must take the domain result branch too."""
from contextlib import closing
import sqlite3,unittest
from app.tests import test_actor_activations as fixtures

class DomainRejectionReentryTests(unittest.TestCase):
    def test_characterization_direct_send_to_real_in_transit_target_rejects_without_losing_journey(self):
        from app.coordinator_scheduler import ActivationRef
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            first=ref;person=next(p for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            target=next(p['id'] for p in world.snapshot()['places'] if p['id']!=person['location'])
            host._choose=lambda view,seen: ({'action':'travel','to':target},seen)
            journey=host.act(ref.person_id,coordinator=owner,activation=ref)
            row=world.next_activation(ref.window_id,coordinator=owner)
            ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
            host._choose=lambda view,seen: ({'action':'send','to':first.person_id,'text':'发给真实在途的国君'},seen)
            before=world.snapshot();result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(result['outcome']['code'],'NOT_AT_LOCATION')
            self.assertEqual(result['outcome']['origin'],'world.apply_action')
            self.assertIsNone(result['outcome']['effect_index'])
            self.assertEqual(world.snapshot(),before)
            self.assertEqual(world.receipt(first.person_id,journey['intent_id']),journey['receipt'])
            self.assertEqual(len(requests),2)

    def test_characterization_adjudicated_shipping_refusal_rolls_back_grain_sql_queue_and_notices(self):
        from app.adjudication import Adjudicator
        from app.coordinator_scheduler import ActivationRef
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            while next(p['office'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)!='finance':
                host.act(ref.person_id,coordinator=owner,activation=ref)
                row=world.next_activation(ref.window_id,coordinator=owner)
                ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
            person=next(p for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            target=next(p['id'] for p in world.snapshot()['places'] if p['country_id']==person['country_id'] and p['id']!=person['location'])
            with world._coordinator_scope(owner):host.adjudicator=Adjudicator(world)
            host._choose=lambda view,seen: ({'action':'propose','text':'先发七份粮，再发超库存粮'},seen)
            host.dm_provider=lambda payload: dict(world_id=payload['world']['world_id'],base_revision=payload['world']['revision'],ruling='成功',reason='真实粮账拒绝',goal='两次发粮',costs=[],benefits=[],effects=[{'action':'ship','to':target,'quantity':7},{'action':'ship','to':target,'quantity':1000000}])
            commit=host.adjudicator.commit;before=[]
            def track(*args,**kwargs):
                before.append(world.snapshot())
                return commit(*args,**kwargs)
            host.adjudicator.commit=track
            result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(result['outcome']['code'],'INSUFFICIENT_GRAIN')
            self.assertEqual(result['outcome']['effect_index'],1)
            self.assertEqual(world.snapshot(),before[0])
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM intents WHERE id LIKE 'dm:%'").fetchone(),(0,))
                self.assertEqual(db.execute("SELECT count(*) FROM dm_timeline WHERE kind='commit'").fetchone(),(0,))

    def test_rehashed_invalid_main_payload_does_not_launder_rejected_proof(self):
        import json
        from app.actor_execution import canonical,digest
        from app.tests import test_activation_domain_rejections as domain_helpers
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(world.db_path)) as db:
                payload=json.loads(db.execute('SELECT payload FROM intents WHERE id=?',(result['intent_id'],)).fetchone()[0])
                payload['invented_effect']=True
                raw=canonical(payload)
                outcome=dict(result['outcome'],payload_sha256=digest(raw));outcome_raw=canonical(outcome)
                db.execute('UPDATE intents SET payload=? WHERE id=?',(raw,result['intent_id']))
                db.execute('UPDATE coordinator_activation_outcomes SET outcome_json=?,outcome_sha256=?',(outcome_raw,digest(outcome_raw)))
                db.commit()
            before=(domain_helpers.database_rows(world.db_path),domain_helpers.database_rows(host.db_path))
            with self.assertRaises(Exception):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual((domain_helpers.database_rows(world.db_path),domain_helpers.database_rows(host.db_path)),before)

    def test_durable_candidate_retry_closes_original_rejection_without_new_request(self):
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            submit=host._submit
            def stop(*args,**kwargs):raise RuntimeError('stop-after-durable-candidate')
            host._submit=stop
            with self.assertRaisesRegex(RuntimeError,'stop-after-durable-candidate'):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            with closing(sqlite3.connect(host.db_path)) as db:
                row=db.execute('SELECT intent_id,state,request,candidate,metrics,consumed FROM actor_intents').fetchone()
                self.assertEqual(row[1],'candidate')
            host._submit=submit
            host._choose=lambda view,seen: self.fail('must preserve original candidate/body')
            before=world.snapshot()
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except Exception as error:
                self.fail('candidate replay must branch to proven rejection close: '+str(getattr(error,'code',error)))
            self.assertEqual(result['intent_id'],row[0])
            self.assertEqual(result['result_kind'],'domain_rejected')
            self.assertEqual(result['outcome']['code'],'ALREADY_THERE')
            self.assertIsNone(result['receipt'])
            self.assertEqual(len(requests),1)
            self.assertEqual(world.snapshot(),before)
            with closing(sqlite3.connect(host.db_path)) as db:
                after=db.execute('SELECT intent_id,state,request,candidate,metrics,consumed FROM actor_intents').fetchone()
                self.assertEqual(after,(row[0],'rejected',*row[2:]))
                self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone(),(0,))
