"""Additional vertical rejection guard regressions, no imported TestCase."""
from contextlib import closing
import json,sqlite3,unittest
from app.tests import test_actor_activations as fixtures
from app.tests import test_activation_domain_rejections as domain_helpers

class DomainRejectionGuardTests(unittest.TestCase):
    def test_characterization_rejected_heartbeat_never_inverts_source_main_lock_order(self):
        import threading,time
        from app.world import WorldError
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            close=host._reject_result;checked=[]
            def reject(person,intent,execution,attempt_id,**kwargs):
                result=close(person,intent,execution,attempt_id,**kwargs)
                acquired=threading.Event();release=threading.Event()
                def hold():
                    with world._private_sources_lock:
                        acquired.set();release.wait(3)
                thread=threading.Thread(target=hold);thread.start()
                try:
                    self.assertTrue(acquired.wait(1))
                    before=domain_helpers.database_rows(world.db_path);start=time.monotonic()
                    with self.assertRaises(WorldError) as caught:
                        world.renew_actor_execution(execution)
                    self.assertEqual(caught.exception.code,'EXECUTION_STORAGE_BUSY')
                    self.assertLess(time.monotonic()-start,.25)
                    self.assertEqual(domain_helpers.database_rows(world.db_path),before)
                finally:
                    release.set();thread.join(2)
                self.assertFalse(thread.is_alive())
                self.assertEqual(world.renew_actor_execution(execution),execution)
                checked.append(True);return result
            host._reject_result=reject
            host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(checked,[True])

    def test_corrupt_principal_office_is_not_a_business_authorization_refusal(self):
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            with closing(sqlite3.connect(world.db_path)) as db:
                seq,raw=db.execute('SELECT seq,state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()
                state=json.loads(raw);person=next(p for p in state['persons'] if p['id']==ref.person_id)
                person['office']='corrupt-roster-office'
                target=next(p['id'] for p in state['persons'] if p['country_id']==person['country_id'] and p['id']!=person['id'])
                state['revision']=seq+1
                db.execute('INSERT INTO snapshots VALUES (?,?)',(seq+1,json.dumps(state)));db.commit()
            host._choose=lambda view,seen: ({'action':'command','to':target,'text':'合法目标不证明损坏的主人物'},seen)
            before=(domain_helpers.database_rows(world.db_path),domain_helpers.database_rows(host.db_path))
            with self.assertRaises(Exception):
                host.act(ref.person_id,coordinator=owner,activation=ref,pause_on_failure=True)
            self.assertEqual((domain_helpers.database_rows(world.db_path),domain_helpers.database_rows(host.db_path)),before)
            self.assertEqual(requests,[])

    def test_rejected_activation_native_authority_gate_requires_real_proof(self):
        with fixtures.ActorActivationTests.registered(self) as (world,host,owner,ref,requests):
            location=next(p['location'] for p in world.snapshot()['persons'] if p['id']==ref.person_id)
            host._choose=lambda view,seen: ({'action':'travel','to':location},seen)
            close=host._reject_result;checked=[]
            def reject(person,intent,execution,attempt_id,**kwargs):
                result=close(person,intent,execution,attempt_id,**kwargs)
                with closing(sqlite3.connect(world.db_path)) as db:
                    db.execute("UPDATE coordinator_activation_outcomes SET outcome_sha256='corrupt'");db.commit()
                before=domain_helpers.database_rows(world.db_path)
                with self.assertRaises(Exception):
                    world.renew_actor_execution(execution)
                self.assertEqual(domain_helpers.database_rows(world.db_path),before,
                                 'native rejected authority gate cannot renew a corrupt terminal')
                checked.append(True)
                return result
            host._reject_result=reject
            host.act(ref.person_id,coordinator=owner,activation=ref)
            self.assertEqual(checked,[True])
