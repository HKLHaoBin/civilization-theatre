"""P023b2 first vertical behavior: actual Host/private/World, request seam only.
The reused test provider's numeric SDK metrics are synthetic, not SDK calls.
"""
from contextlib import closing, contextmanager
import inspect
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.actors import ActorHost
from app.coordinator_scheduler import ActivationRef
from app.tests import test_actor_execution as execution_fixtures
from types import SimpleNamespace
ActorExecutionTests = SimpleNamespace(result=execution_fixtures.ActorExecutionTests.result)
from app.world import WorldStore, DAY

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')


class ActorActivationTests(unittest.TestCase):
    @contextmanager
    def registered(self, *, real_pi=False):
        with tempfile.TemporaryDirectory(prefix='p023b2-cycle-',dir=SCRATCH) as directory:
            root=Path(directory)
            world=WorldStore(root/'world.sqlite')
            world.initialize()
            host=ActorHost(world,root/'actors.sqlite')
            owner=world.acquire_coordinator_execution('activation-cycle-owner')
            requests=[]
            if not real_pi:
                def request(payload):
                    requests.append(payload)
                    return ActorExecutionTests.result(payload)
                host._request=request
            host._choose=lambda view,seen: ({'action':'wait'},seen)
            try:
                world.control('resume',coordinator=owner)
                version={k:world.snapshot()[k] for k in ('world_id','revision')}
                boundary=world.inspect_scheduler_boundary(actor_db_path=host.db_path)
                world.register_scheduler(coordinator=owner,expected_version=version,
                                         boundary=boundary['boundary'],actor_db_path=host.db_path)
                window=world.open_or_recover_window(coordinator=owner,expected_version=version)
                row=world.next_activation(window['window_id'],coordinator=owner)
                ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                yield world,host,owner,ref,requests
            finally:
                host.close()
                world.release_coordinator_execution(owner)

    def test_observation_conflict_reloads_same_activation_before_request(self):
        with self.registered() as (world,host,owner,ref,requests):
            self.assertTrue(callable(getattr(host,'_observation_envelope',None)),
                            'Host must pin observation and card version before preparing')
            original=host._observation_envelope
            envelopes=[]
            def interleave(person):
                envelope=original(person)
                envelopes.append(envelope)
                if len(envelopes)==1:
                    world.edit_card(person,{'mission':'new version mission'},coordinator=owner)
                return envelope
            host._observation_envelope=interleave
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(len(envelopes),2)
            self.assertEqual(len(requests),1)
            self.assertIn('new version mission',requests[0]['system_prompt'])
            self.assertEqual(requests[0]['intent_id'],result['intent_id'])
            self.assertEqual(set(envelopes[1]),{'view','world_id','revision','card_version'})
            self.assertNotIn('card_version',envelopes[1]['view']['person'])
            with closing(sqlite3.connect(world.db_path)) as db:
                row=db.execute('SELECT observation_revision,card_version,intent_id FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                self.assertEqual(row,(envelopes[1]['revision'],envelopes[1]['card_version'],result['intent_id']))
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone(),(1,))

    def test_adjudicated_activation_commits_parent_children_and_phase_together(self):
        from app.adjudication import Adjudicator
        with self.registered() as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):
                host.adjudicator=Adjudicator(world)
            dm_calls=[]
            def dm(payload):
                dm_calls.append(payload['proposal']['intent_id'])
                state=payload['world']
                return dict(world_id=state['world_id'],base_revision=state['revision'],
                            ruling='成功',reason='activation scripted decision',goal='wait twice',
                            costs=[],benefits=[],effects=[{'action':'wait'},{'action':'wait'}])
            host.dm_provider=dm
            host._choose=lambda view,seen: ({'action':'propose','text':'original activation proposal'},seen)
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            replay=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['receipt'],replay['receipt'])
            self.assertEqual(dm_calls,[result['intent_id']])
            self.assertEqual(len(requests),1)
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('completed',1))
                rows=db.execute('SELECT id,receipt FROM intents WHERE person=?',(ref.person_id,)).fetchall()
                self.assertEqual(len(rows),3)
                self.assertTrue(all(receipt for _,receipt in rows))
                self.assertEqual(db.execute('SELECT intent_id,kind FROM coordinator_activation_outcomes').fetchall(),[(result['intent_id'],'committed')])

    def test_activation_cannot_replace_durable_body_at_world_boundary(self):
        from app.world import WorldError
        with self.registered() as (world,host,owner,ref,requests):
            submit=host._submit
            def wrong_body(person,intent,body,*,execution=None):
                with self.assertRaises(WorldError) as caught:
                    world.submit(person,intent,{'action':'investigate'},execution=execution)
                self.assertEqual(caught.exception.code,'ACTIVATION_BINDING_CONFLICT')
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(0,))
                return submit(person,intent,body,execution=execution)
            host._submit=wrong_body
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['body'],{'action':'wait'})
            self.assertEqual(len(requests),1)

    def test_explicit_world_reference_must_match_original_scope(self):
        from app.world import WorldError
        with self.registered() as (world,host,owner,ref,requests):
            original=host._submit
            def submit(person,intent,body,*,execution=None):
                other=ActivationRef(ref.world_id,ref.window_id,'f'*32,person)
                with self.assertRaises(WorldError) as error:
                    world.submit(person,intent,body,execution=execution,activation=other)
                self.assertEqual(error.exception.code,'ACTIVATION_BINDING_CONFLICT')
                return world.submit(person,intent,body,execution=execution,activation=ref)
            host._submit=submit
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(len(requests),1)
            self.assertEqual(result['receipt']['status'],'committed')

    def test_activation_binding_failure_is_local_stop_not_successor_pause(self):
        from app.actor_execution import ActorRunError
        with self.registered() as (world,host,owner,ref,requests):
            def reject(person,*,execution=None,attempt_id=None):
                raise ActorRunError('ACTIVATION_BINDING_CONFLICT')
            host._act=reject
            with self.assertRaises(ActorRunError) as error:
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(error.exception.code,'ACTIVATION_BINDING_CONFLICT')
            self.assertEqual(world.snapshot()['status'],'running')
            self.assertEqual(requests,[])

    def test_root_and_reference_cannot_bypass_actor_gate_in_generic_mutate(self):
        from app.world import WorldError
        from app.coordinator_scheduler import activation_scope
        with self.registered() as (world,host,owner,ref,requests):
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            before=world.snapshot()
            with world._coordinator_scope(owner), activation_scope(world,ref):
                with self.assertRaises(WorldError) as caught:
                    world._mutate(lambda state,db: world._apply_action(state,world._find(state['persons'],ref.person_id,'UNKNOWN_PERSON'),result['intent_id'],{'action':'wait'},'成功'))
            self.assertEqual(caught.exception.code,'ACTOR_EXECUTION_REQUIRED')
            self.assertEqual(world.snapshot(),before)

    def test_characterization_durable_phase_recovery_preserves_original_payload(self):
        import json
        for stop in ('prepared','candidate','world_committed'):
            with self.subTest(stop=stop), self.registered() as (world,host,owner,ref,requests):
                request=host._request
                def interrupt(*args,**kwargs):
                    raise RuntimeError('injected durable breakpoint')
                if stop=='prepared': host._request=interrupt
                elif stop=='candidate': host._submit=interrupt
                else: host._finish=interrupt
                with self.assertRaisesRegex(RuntimeError,'injected durable breakpoint'):
                    host.act(ref.person_id,coordinator=owner,activation=ref)
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    before=db.execute('SELECT phase,intent_id,created_parent_epoch,request_sha256 FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                    self.assertEqual(before[0],stop)
                path=host.db_path
                with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    original=db.execute('SELECT intent_id,body,request,consumed FROM actor_intents').fetchone()
                world.edit_card(ref.person_id,{'mission':'future card must not rewrite durable request'},coordinator=owner)
                host.close()
                self.assertTrue(world.release_coordinator_execution(owner))
                successor=world.acquire_coordinator_execution('recovery-successor')
                recovered=ActorHost(world,path)
                recovered._request=request
                recovered._choose=lambda *args: self.fail('durable activation must not choose again')
                try:
                    result=recovered.act(ref.person_id,coordinator=successor,activation=ref)
                    replay=recovered.act(ref.person_id,coordinator=successor,activation=ref)
                    self.assertEqual(result['intent_id'],original[0])
                    self.assertEqual(replay['receipt'],result['receipt'])
                    self.assertEqual(len(requests),1)
                    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                        self.assertEqual(db.execute('SELECT intent_id,body,request,consumed FROM actor_intents').fetchone(),original)
                        self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads').fetchall(),[(1,original[0])])
                    with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                        after=db.execute('SELECT phase,intent_id,private_closed,created_parent_epoch,owner_parent_epoch,request_sha256 FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                        self.assertEqual(after,('completed',original[0],1,1,2,before[3]))
                        self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone(),(1,))
                    print('ACTIVATION_RECOVERY_SQL',json.dumps(dict(stop=stop,before=before,after=after,request_seam_calls=len(requests),actual_sdk_requests=0)))
                finally:
                    recovered.close()
                    world.release_coordinator_execution(successor)

    def test_characterization_real_pi_finish_ack_loss_replays_without_request(self):
        import json
        with self.registered(real_pi=True) as (world,host,owner,ref,requests):
            metrics=[]
            actual=host._request
            def measured(payload):
                result=actual(payload)
                if payload.get('op')!='close':
                    metrics.append({k:result[k] for k in ('sdk_requests','sdk_events','pid','provider')})
                return result
            host._request=measured
            finish=host._finish
            def lose_ack(*args,**kwargs):
                finish(*args,**kwargs)
                raise RuntimeError('injected ACK loss after actual private commit')
            host._finish=lose_ack
            with self.assertRaisesRegex(RuntimeError,'injected ACK loss'):
                host.act(ref.person_id,coordinator=owner,activation=ref)
            path=host.db_path
            with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                original=db.execute('SELECT phase,intent_id,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                self.assertEqual((original[0],original[2]),('completed',1))
            host.close()
            self.assertEqual(host.worker_exit_code,0)
            self.assertTrue(world.release_coordinator_execution(owner))
            successor=world.acquire_coordinator_execution('real-pi-ack-successor')
            recovered=ActorHost(world,path)
            recovered._request=lambda payload:self.fail('completed replay cannot call worker')
            try:
                result=recovered.act(ref.person_id,coordinator=successor,activation=ref)
                self.assertEqual(result['intent_id'],original[1])
                self.assertEqual(len(metrics),1)
                self.assertEqual(metrics[0]['sdk_requests'],2)
                with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads').fetchall(),[(1,original[1])])
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone(),(1,))
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone(),(1,))
                    self.assertEqual(db.execute('SELECT intent_id,kind FROM coordinator_activation_outcomes').fetchall(),[(original[1],'committed')])
                print('ACTIVATION_REAL_PI_ACK_SQL',json.dumps(dict(activation=ref.activation_id,intent=original[1],metrics=metrics,node_exit=host.worker_exit_code,head=1,receipts=1,request_seam_calls=0)))
            finally:
                recovered.close()
                world.release_coordinator_execution(successor)

    def test_characterization_old_completed_replay_never_rewinds_newer_head(self):
        with self.registered() as (world,host,owner,ref,requests):
            first=host.act(ref.person_id,coordinator=owner,activation=ref)
            while True:
                row=world.next_activation(ref.window_id,coordinator=owner)
                if row is None:break
                current=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                host.act(current.person_id,coordinator=owner,activation=current)
            world.complete_window(ref.window_id,coordinator=owner)
            version={k:world.snapshot()[k] for k in ('world_id','revision')}
            window=world.open_or_recover_window(coordinator=owner,expected_version=version)
            row=world.next_activation(window['window_id'],coordinator=owner)
            current=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
            self.assertEqual(current.person_id,ref.person_id)
            later=host.act(current.person_id,coordinator=owner,activation=current)
            with closing(sqlite3.connect(Path(host.db_path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                head=db.execute('SELECT version,last_promoted_intent_id,messages_sha256,seen_sha256 FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone()
                session=db.execute('SELECT messages,seen FROM actor_sessions WHERE person_id=?',(ref.person_id,)).fetchone()
                ids=db.execute('SELECT intent_id FROM actor_intents ORDER BY intent_id').fetchall()
                self.assertEqual(head[:2],(2,later['intent_id']))
            calls=len(requests)
            replay=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(replay['intent_id'],first['intent_id'])
            self.assertEqual(replay['receipt'],first['receipt'])
            self.assertEqual(len(requests),calls)
            with closing(sqlite3.connect(Path(host.db_path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                self.assertEqual(db.execute('SELECT version,last_promoted_intent_id,messages_sha256,seen_sha256 FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),head)
                self.assertEqual(db.execute('SELECT messages,seen FROM actor_sessions WHERE person_id=?',(ref.person_id,)).fetchone(),session)
                self.assertEqual(db.execute('SELECT intent_id FROM actor_intents ORDER BY intent_id').fetchall(),ids)

    def test_completed_activation_replay_keeps_original_intent_and_private_head(self):
        with tempfile.TemporaryDirectory(prefix='p023b2-first-',dir=SCRATCH) as directory:
            root=Path(directory)
            world=WorldStore(root/'world.sqlite')
            world.initialize()
            host=ActorHost(world,root/'actors.sqlite')
            owner=world.acquire_coordinator_execution('activation-first-owner')
            requests=[]
            def request(payload):
                requests.append(payload['intent_id'])
                return ActorExecutionTests.result(payload)
            host._request=request
            host._choose=lambda view,seen: ({'action':'wait'},seen)
            try:
                self.assertIn('activation',inspect.signature(host.act).parameters,
                              'Actual ActorHost.act must bind a durable activation before choosing an intent')
                world.control('resume',coordinator=owner)
                version={k:world.snapshot()[k] for k in ('world_id','revision')}
                boundary=world.inspect_scheduler_boundary(actor_db_path=host.db_path)
                self.assertTrue(boundary['eligible'])
                world.register_scheduler(coordinator=owner,expected_version=version,
                                         boundary=boundary['boundary'],actor_db_path=host.db_path)
                window=world.open_or_recover_window(coordinator=owner,expected_version=version)
                pending=world.next_activation(window['window_id'],coordinator=owner)
                reference=ActivationRef(**{key:pending[key] for key in ('world_id','window_id','activation_id','person_id')})
                first=host.act(reference.person_id,coordinator=owner,activation=reference)
                replay=host.act(reference.person_id,coordinator=owner,activation=reference)
                self.assertEqual(replay['intent_id'],first['intent_id'])
                self.assertEqual(replay['receipt'],first['receipt'])
                self.assertEqual(requests,[first['intent_id']])
                self.assertEqual(world.snapshot()['time'],DAY)
                with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    row=db.execute('SELECT phase,intent_id,private_closed FROM coordinator_activations WHERE activation_id=?',
                                   (reference.activation_id,)).fetchone()
                    self.assertEqual(row,('completed',first['intent_id'],1))
                    self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone(),(1,))
                with closing(sqlite3.connect(Path(host.db_path).resolve().as_uri()+'?mode=ro',uri=True)) as db:
                    self.assertEqual(db.execute('SELECT version FROM actor_session_heads WHERE person_id=?',
                                                (reference.person_id,)).fetchone(),(1,))
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone(),(1,))
            finally:
                host.close()
                world.release_coordinator_execution(owner)
