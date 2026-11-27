"""Additional current-behavior acceptance, NOT retroactive TDD REDs."""
from contextlib import closing
from pathlib import Path
import unittest,tempfile,sqlite3,json,time,subprocess,sys,threading,os
from dataclasses import replace
from unittest.mock import patch
from app.world import WorldStore,WorldError
from app.server import Coordinator
from app.actors import ActorHost
from app.actor_execution import ExecutionPolicy
SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
ROOT=Path(__file__).resolve().parents[2]
class NoActors:
    def act(self,p):pass
    def close(self):pass
class ParentAcceptance(unittest.TestCase):
    def test_independent_coordinators_compete_kill_ttl_takeover_and_os_cleanup(self):
        with tempfile.TemporaryDirectory(prefix='p023a-process-',dir=SCRATCH) as temp:
            path=Path(temp)/'world.sqlite';world=WorldStore(path);world.initialize();children=[]
            def spawn():
                p=subprocess.Popen([sys.executable,'-B',str(ROOT/'app/evidence/p023a-backend-process.py'),str(path)],cwd=ROOT,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8');children.append(p);return p
            try:
                a,b=spawn(),spawn();ra,rb=json.loads(a.stdout.readline()),json.loads(b.stdout.readline())
                self.assertEqual(sorted([ra['result'],rb['result']]),['COORDINATOR_EXECUTION_BUSY','owner'])
                winner=a if ra['result']=='owner' else b;loser=b if winner is a else a
                loser.stdin.write('close\n');loser.stdin.flush();lo,le=loser.communicate(timeout=5);self.assertEqual(loser.returncode,0,le)
                winner.terminate();winner.communicate(timeout=5);time.sleep(1.3)
                c=spawn();rc=json.loads(c.stdout.readline());self.assertEqual(rc['result'],'owner');self.assertEqual(rc['epoch'],2)
                c.stdin.write('close\n');c.stdin.flush();out,err=c.communicate(timeout=5);self.assertEqual(c.returncode,0,err);self.assertFalse(json.loads(out)['heartbeat_alive'])
                with closing(sqlite3.connect(path.as_uri()+'?mode=ro',uri=True)) as db:
                    row=db.execute('SELECT epoch,owner_token,schema_version FROM coordinator_execution_leases').fetchone();self.assertEqual(row,(2,None,1));self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0],1)
                print('ROOT_PROCESS_REAL',json.dumps(dict(pids=[p.pid for p in children],exits=[p.returncode for p in children],epoch=2,all_waited=True)))
            finally:
                for p in children:
                    if p.poll() is None:p.kill();p.communicate(timeout=5)
                    for stream in (p.stdin,p.stdout,p.stderr):
                        if stream and not stream.closed:stream.close()

    def test_actor_effect_lock_linearizes_root_takeover(self):
        with tempfile.TemporaryDirectory(prefix='p023a-rootlock-',dir=SCRATCH) as temp:
            now=[100000000000];path=Path(temp)/'world.sqlite';world=WorldStore(path,execution_clock_ns=lambda:now[0]);state=world.initialize();root=world.acquire_coordinator_execution('a',ttl_seconds=1);world.control('resume',coordinator=root);person=state['persons'][0]['id'];actor=world.acquire_actor_execution(person,'child',coordinator=root)
            original=world._apply_action;out=[]
            def apply(*a):
                result=original(*a)
                # The independent successor observes expiry but cannot acquire
                # main's write lock while the actual actor effect owns it.
                process=subprocess.run([sys.executable,'-B',str(ROOT/'app/evidence/p023a-backend-process.py'),str(path),'claim-only'],cwd=ROOT,capture_output=True,text=True,timeout=5)
                self.assertEqual(process.returncode,0,process.stderr)
                out.append(json.loads(process.stdout.splitlines()[0])['result'])
                return result
            world._apply_action=apply
            with world._coordinator_scope(root):world.submit(person,'locked',{'action':'wait'},execution=actor)
            self.assertEqual(out,['EXECUTION_STORAGE_BUSY']);now[0]+=1000000000;new=world.acquire_coordinator_execution('b');self.assertEqual(new.epoch,2)
            self.assertIsNotNone(world.receipt(person,'locked'))

    def test_validation_clock_shared_floor_and_final_renew_sample(self):
        with tempfile.TemporaryDirectory(prefix='p023a-clock-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize()
            for ttl in (True,0,3601,1.0):
                with self.assertRaises(WorldError):world.acquire_coordinator_execution('root',ttl_seconds=ttl)
            root=world.acquire_coordinator_execution('root',ttl_seconds=1)
            for value in (True,0,2**63,1.0):
                with self.assertRaises(WorldError) as error:world.renew_coordinator_execution(replace(root,epoch=value))
                self.assertEqual(error.exception.code,'INVALID_COORDINATOR_EXECUTION')
            samples=iter([100500000000,101000000000]);world._execution_clock_ns=lambda:next(samples)
            with self.assertRaises(WorldError) as error:world.renew_coordinator_execution(root)
            self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_EXPIRED')
            world._execution_clock_ns=lambda:now[0];now[0]=99999999999
            with self.assertRaises(WorldError) as error:world.control('resume',coordinator=root)
            self.assertEqual(error.exception.code,'EXECUTION_CLOCK_UNSAFE')
            self.assertTrue(world.release_coordinator_execution(root))
            with closing(sqlite3.connect(world.db_path)) as db:self.assertEqual(db.execute('SELECT high_water_wall_ns FROM execution_clock').fetchone()[0],100000000000)

    def test_actual_pi_late_candidate_failure_and_heartbeat_reject_original_parent(self):
        for boundary in ('candidate','failure','heartbeat'):
            with self.subTest(boundary=boundary),tempfile.TemporaryDirectory(prefix='p023a-late-'+boundary+'-',dir=SCRATCH) as temp:
                now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();policy=ExecutionPolicy(renew_interval_seconds=.05 if boundary=='heartbeat' else 15)
                host=ActorHost(world,Path(temp)/'actor.sqlite',execution_policy=policy);root=world.acquire_coordinator_execution('old',ttl_seconds=1);world.control('resume',coordinator=root);person=state['persons'][0]['id'];request=host._request;successor=[];late_errors=[]
                def provider(payload):
                    result=request(payload);permit=host._attempt.permit
                    now[0]+=1000000000;successor.append(world.acquire_coordinator_execution('new'))
                    try:world.renew_actor_execution(permit,coordinator=root)
                    except WorldError as e:late_errors.append(e.code)
                    if boundary=='failure':raise RuntimeError('original-provider-failure')
                    if boundary=='heartbeat':time.sleep(.12)
                    return result
                host._request=provider
                try:
                    with patch.dict(os.environ,{'PI_MODULE_ROOT':str(ROOT/'spikes/002-pi-isolation/npm/node_modules')}):
                        with self.assertRaises(Exception) as error:host.act(person,coordinator=root,pause_on_failure=True)
                    if boundary=='failure':self.assertEqual(str(error.exception),'original-provider-failure')
                    else:self.assertIn(getattr(error.exception,'code',None),('COORDINATOR_EXECUTION_FENCED','COORDINATOR_EXECUTION_EXPIRED'))
                    self.assertEqual(late_errors,['COORDINATOR_EXECUTION_FENCED']);self.assertEqual(world.snapshot()['status'],'running');self.assertIsNone(world.receipt(person,'unused'))
                    with closing(sqlite3.connect(host.db_path)) as db:
                        self.assertEqual(db.execute('SELECT state FROM actor_intents').fetchone()[0],'prepared')
                        self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],0)
                finally:
                    host._request=request;host.close()
                    for pending in host._pending_threads:pending.join(.5)
                    host.close()  # Re-read truthful pending cleanup after bounded join.
                self.assertFalse(host.cleanup_pending);self.assertIsNone(host._process)

    def test_root_maintenance_independent_of_pipeline_lock_and_readonly_host(self):
        with tempfile.TemporaryDirectory(prefix='p023a-maintain-',dir=SCRATCH) as temp:
            world=WorldStore(Path(temp)/'world.sqlite');world.initialize();owner=Coordinator(world,NoActors(),execution_policy=ExecutionPolicy(ttl_seconds=1,renew_interval_seconds=.1));owner.acquire_owner()
            try:
                with closing(sqlite3.connect(world.db_path)) as db:before=db.execute('SELECT last_wall_ns FROM coordinator_execution_leases').fetchone()[0]
                with owner.lock:time.sleep(.3)
                with closing(sqlite3.connect(world.db_path)) as db:self.assertGreater(db.execute('SELECT last_wall_ns FROM coordinator_execution_leases').fetchone()[0],before)
                host_path=Path(temp)/'observer.sqlite';host=ActorHost(world,host_path);self.assertFalse(host_path.exists());host.close()
            finally:owner.close()
            self.assertFalse(owner._owner_attempt.thread.is_alive())

    def test_actual_pi_adjudication_callback_takeover_recovers_original_id(self):
        from app.adjudication import Adjudicator
        from app.scripted_dm import DefaultActors,ScriptedDM
        with tempfile.TemporaryDirectory(prefix='p023a-adjlate-',dir=SCRATCH) as temp:
            now=[100000000000];world=WorldStore(Path(temp)/'world.sqlite',execution_clock_ns=lambda:now[0]);state=world.initialize();adj=Adjudicator(world);dm=ScriptedDM(world);host=DefaultActors(world,Path(temp)/'actor.sqlite',adjudicator=adj,dm_provider=dm);root=world.acquire_coordinator_execution('old',ttl_seconds=1);world.control('resume',coordinator=root);successors=[];ids=[]
            def late(payload):
                decision=dm(payload);ids.append(payload['proposal']['intent_id']);now[0]+=1000000000;successors.append(world.acquire_coordinator_execution('new'));return decision
            host.dm_provider=late;person=state['persons'][0]['id']
            try:
                with patch.dict(os.environ,{'PI_MODULE_ROOT':str(ROOT/'spikes/002-pi-isolation/npm/node_modules')}):
                    with self.assertRaises(WorldError) as error:host.act(person,coordinator=root,pause_on_failure=True)
                    self.assertEqual(error.exception.code,'COORDINATOR_EXECUTION_FENCED');self.assertEqual(world.snapshot()['status'],'running')
                    host.dm_provider=dm;result=host.act(person,coordinator=successors[0]);self.assertEqual(result['intent_id'],ids[0])
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM dm_proposals').fetchone()[0],1)
                    self.assertEqual(db.execute('SELECT count(*) FROM dm_decisions').fetchone()[0],1)
                    self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone()[0],2)
                with closing(sqlite3.connect(host.db_path)) as db:self.assertEqual(db.execute('SELECT version FROM actor_session_heads').fetchone()[0],1)
            finally:host.close()

if __name__=='__main__':unittest.main()
