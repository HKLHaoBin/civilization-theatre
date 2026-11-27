"""P023b3 C first behavior: True-mode terminal ACK loss, real durable stores."""
from contextlib import closing
import json
import sqlite3
import unittest

from app.tests import test_actor_activations as activation_fixtures


class ActivationTerminalAckTests(unittest.TestCase):
    def test_true_mode_completed_ack_loss_returns_original_without_pause(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            finish=host._finish
            completed=[]
            def lose_ack(*args,**kwargs):
                finish(*args,**kwargs)
                completed.append(dict(intent=args[0],snapshot=world.snapshot()))
                raise RuntimeError('controlled ACK loss after actual completed private commit')
            host._finish=lose_ack
            try:
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            except Exception as error:
                self.fail('True-mode completed ACK loss must prove and return the original terminal '
                          'result without a technical pause: '+str(error))
            self.assertEqual(len(completed),1)
            self.assertEqual(result['intent_id'],completed[0]['intent'])
            self.assertTrue(result['reconciled'])
            self.assertEqual(result['receipt']['status'],'committed')
            self.assertEqual(world.snapshot(),completed[0]['snapshot'])
            self.assertEqual(world.snapshot()['status'],'running')
            self.assertEqual(len(requests),1)
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,private_closed,resume_phase,diagnostic_code FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('completed',1,None,None))
                actual=db.execute('SELECT receipt FROM intents WHERE id=?',(result['intent_id'],)).fetchone()[0]
                self.assertEqual(json.loads(actual),result['receipt'])
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(1,))
                exists=db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_blocks'").fetchone()
                if exists:self.assertEqual(db.execute('SELECT count(*) FROM coordinator_activation_blocks').fetchone(),(0,))
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(1,result['intent_id']))
                self.assertEqual(db.execute('SELECT count(*) FROM actor_intents').fetchone(),(1,))

    def test_ack_diagnostic_is_sanitized_host_local_only(self):
        from app.world import WorldError
        for code in ('ACK_LOST','unsafe/path?token=secret'):
            with self.subTest(code=code),activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
                finish=host._finish;captured=[]
                def lose(*args,**kwargs):
                    finish(*args,**kwargs);captured.append(world.snapshot())
                    raise WorldError(code,'PRIVATE_REQUEST_SESSION_TOKEN_PATH_NEVER_EXPORT')
                host._finish=lose
                result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertEqual(getattr(host,'_terminal_ack_diagnostic',None),
                                 {'code':'ACK_LOST' if code=='ACK_LOST' else 'HOST_FAILURE','phase':'completed'})
                self.assertEqual(world.snapshot(),captured[0])
                self.assertNotIn('PRIVATE_REQUEST_SESSION_TOKEN_PATH_NEVER_EXPORT',json.dumps(result))
                self.assertNotIn('ACK_LOST',json.dumps(world.observe(ref.person_id)))
                self.assertNotIn('ACK_LOST',json.dumps(result))
                for path in (world.db_path,host.db_path):
                    with closing(sqlite3.connect(path)) as db:
                        text='\n'.join(db.iterdump())
                        self.assertNotIn('PRIVATE_REQUEST_SESSION_TOKEN_PATH_NEVER_EXPORT',text)
                        self.assertNotIn('ACK_LOST',text)

    def test_completed_ack_missing_metrics_fails_closed_without_fallback_pause(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            finish=host._finish;original=RuntimeError('original ACK must survive proof failure');snapshots=[]
            def lose(*args,**kwargs):
                finish(*args,**kwargs)
                with closing(sqlite3.connect(host.db_path)) as db:
                    db.execute("UPDATE actor_intents SET metrics='{}'");db.commit()
                snapshots.append(world.snapshot());raise original
            host._finish=lose
            with self.assertRaises(RuntimeError) as caught:
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertIs(caught.exception,original)
            self.assertEqual(world.snapshot(),snapshots[0])
            self.assertEqual(len(requests),1)

    def test_control_flow_exceptions_after_terminal_are_not_reconciled_or_paused(self):
        for kind in (KeyboardInterrupt,SystemExit,GeneratorExit):
            with self.subTest(kind=kind.__name__),activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
                finish=host._finish;captured=[];error=kind('controlled control flow')
                def lose(*args,**kwargs):
                    finish(*args,**kwargs);captured.append(world.snapshot());raise error
                host._finish=lose
                with self.assertRaises(kind) as caught:
                    host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
                self.assertIs(caught.exception,error)
                self.assertEqual(world.snapshot(),captured[0])
                self.assertEqual(len(requests),1)

    def test_true_mode_closed_rejection_ack_loss_returns_original_without_reclose(self):
        with activation_fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen:({'action':'travel','to':location},seen)
            reject=host._reject_result
            originals=[]
            def lose(*args,**kwargs):
                self.assertEqual(originals,[], 'closed reject hook may not be called again')
                result=reject(*args,**kwargs)
                originals.append((result,world.snapshot()))
                raise RuntimeError('controlled closed rejection ACK loss')
            host._reject_result=lose
            result=host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual(len(originals),1)
            self.assertEqual(result,dict(originals[0][0],reconciled=True))
            self.assertEqual(world.snapshot(),originals[0][1])
            self.assertIsNone(result['receipt'])
            self.assertEqual(result['outcome']['code'],'ALREADY_THERE')
            self.assertEqual(len(requests),1)
            with closing(sqlite3.connect(world.db_path)) as db:
                self.assertEqual(db.execute('SELECT phase,private_closed,resume_phase,diagnostic_code FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone(),('rejected',1,None,None))
                self.assertEqual(db.execute('SELECT receipt FROM intents WHERE id=?',(result['intent_id'],)).fetchone(),(None,))
                self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='coordinator_activation_blocks'").fetchone())
            with closing(sqlite3.connect(host.db_path)) as db:
                self.assertEqual(db.execute('SELECT version,last_promoted_intent_id FROM actor_session_heads WHERE person_id=?',(ref.person_id,)).fetchone(),(0,None))
                self.assertEqual(db.execute('SELECT state,receipt FROM actor_intents').fetchone(),('rejected',None))

