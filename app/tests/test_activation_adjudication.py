"""Direct ADJ API activation gates, actual SQLite and Host pipeline."""
from contextlib import closing
import inspect
import sqlite3
import unittest
from app.tests import test_actor_activations as fixtures
from app.adjudication import Adjudicator
from app.coordinator_scheduler import ActivationRef
from app.world import WorldError

class DirectAdjudicationActivationTests(unittest.TestCase):
    registered=fixtures.ActorActivationTests.registered

    def test_actor_recovery_exception_through_dm_is_local_stop(self):
        from app.actor_execution import ActorRunError
        with self.registered() as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):host.adjudicator=Adjudicator(world)
            host._choose=lambda view,seen:({'action':'propose','text':'original'},seen)
            def provider(payload):raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            host.dm_provider=provider
            with self.assertRaises(ActorRunError) as caught:
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(caught.exception.code,'ACTOR_RECOVERY_UNPROVEN')
            self.assertEqual(world.snapshot()['status'],'running')

    def test_direct_authorized_receipt_replay_retains_original_id(self):
        from app.coordinator_scheduler import activation_scope
        with self.registered() as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):adj=Adjudicator(world)
            host.adjudicator=adj
            host._choose=lambda view,seen:({'action':'propose','text':'original replay text'},seen)
            def dm(payload):
                s=payload['world']
                return dict(world_id=s['world_id'],base_revision=s['revision'],ruling='成功',reason='script',goal='wait',costs=[],benefits=[],effects=[{'action':'wait'}])
            host.dm_provider=dm
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            execution=world.acquire_actor_execution(ref.person_id,'direct-replay',coordinator=owner,activation=ref)
            try:
                with world._coordinator_scope(owner),activation_scope(world,ref):
                    def forbidden(payload):self.fail('receipt replay must never call DM')
                    receipt=adj.resolve(ref.person_id,result['intent_id'],'original replay text',forbidden,execution=execution,activation=ref)
                    self.assertEqual(receipt,result['receipt'])
                    self.assertEqual(adj.commit(ref.person_id,result['intent_id'],receipt['decision_version'],execution=execution,activation=ref),receipt)
                    for field in ('world_id','window_id','activation_id','person_id'):
                        bad=dict(ref.__dict__);bad[field]='f'*32
                        with self.assertRaises(WorldError) as caught:
                            adj.resolve(ref.person_id,result['intent_id'],'original replay text',forbidden,execution=execution,activation=ActivationRef(**bad))
                        self.assertEqual(caught.exception.code,'ACTIVATION_BINDING_CONFLICT')
                self.assertEqual(len(requests),1)
            finally:world.release_actor_execution(execution)

    def test_adjudication_recovery_errors_remain_local_stop(self):
        for code in ('ACTIVATION_BINDING_CONFLICT','INVALID_ACTIVATION',
                     'WINDOW_RECOVERY_UNPROVEN','ACTOR_RECOVERY_UNPROVEN'):
            with self.subTest(code=code), self.registered() as (world,host,owner,ref,requests):
                with world._coordinator_scope(owner):
                    host.adjudicator=Adjudicator(world)
                host._choose=lambda view,seen:({'action':'propose','text':'original'},seen)
                def provider(payload):
                    raise WorldError(code,'injected permission diagnostic')
                host.dm_provider=provider
                with self.assertRaises(Exception) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(caught.exception.code,code)
                self.assertEqual(world.snapshot()['status'],'running')

    def test_every_direct_api_binds_explicit_ref_and_original_text(self):
        with self.registered() as (world,host,owner,ref,requests):
            with world._coordinator_scope(owner):
                adj=Adjudicator(world)
            host.adjudicator=adj
            host._choose=lambda view,seen:({'action':'propose','text':'durable original text'},seen)
            def dm(payload):
                state=payload['world']
                return dict(world_id=state['world_id'],base_revision=state['revision'],ruling='成功',reason='script',goal='wait',costs=[],benefits=[],effects=[{'action':'wait'}])
            host.dm_provider=dm
            for name in ('propose','record','preview','commit','resolve'):
                self.assertIn('activation',inspect.signature(getattr(adj,name)).parameters,name+' direct API must accept activation')
            for name in ('propose','record','preview','commit','resolve'):
                original=getattr(adj,name)
                def probe(*args,_name=name,_original=original,**kwargs):
                    if (args[0] != ref.person_id or args[1] != requests[0]['intent_id']
                            or (_name in ('propose','resolve') and args[2] != 'durable original text')):
                        return _original(*args,**kwargs)
                    for field in ('world_id','window_id','activation_id','person_id'):
                        bad=dict(ref.__dict__);bad[field]='f'*32
                        with self.assertRaises(WorldError) as caught:
                            _original(*args,**dict(kwargs,activation=ActivationRef(**bad)))
                        self.assertEqual(caught.exception.code,'ACTIVATION_BINDING_CONFLICT',(_name,field))
                    for index,code in ((0,'EXECUTION_PERSON_MISMATCH'),(1,None)):
                        foreign=list(args);foreign[index]='e'*32
                        snapshot=world.snapshot()
                        with self.assertRaises(WorldError) as caught:
                            _original(*foreign,**dict(kwargs,activation=ref))
                        if code:self.assertEqual(caught.exception.code,code,_name)
                        else:self.assertIn(caught.exception.code,('ACTIVATION_BINDING_CONFLICT','UNKNOWN_PROPOSAL'),_name)
                        self.assertEqual(world.snapshot(),snapshot)
                    if _name in ('propose','resolve'):
                        wrong=list(args);wrong[2]='replaced original text'
                        with self.assertRaises(WorldError) as caught:
                            _original(*wrong,**dict(kwargs,activation=ref))
                        self.assertEqual(caught.exception.code,'ACTIVATION_BINDING_CONFLICT',_name)
                    return _original(*args,**dict(kwargs,activation=ref))
                setattr(adj,name,probe)
            result=host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(result['receipt']['status'],'committed')
            self.assertEqual(len(requests),1)
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT text FROM dm_proposals').fetchall(),[('durable original text',)])
                self.assertEqual(db.execute('SELECT phase,private_closed FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('completed',1))
