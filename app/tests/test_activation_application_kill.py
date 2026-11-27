"""Whole owned Host application kill after actual resource commit (not reboot)."""
from contextlib import closing
import json,os,sqlite3,subprocess,sys,tempfile,time,unittest
from pathlib import Path
from app.tests import test_actor_activations as fixtures
from app.tests import test_actor_execution as provider_fixture
from app.coordinator_scheduler import ActivationRef
from app.actors import ActorHost
from app.world import WorldStore
ROOT=Path(__file__).resolve().parents[2]
SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
CHILD=r'''
import json,sys,time
from pathlib import Path
from app.actors import ActorHost
from app.world import WorldStore
from app.coordinator_scheduler import ActivationRef
from app.adjudication import Adjudicator
root=Path(sys.argv[1]);mode=sys.argv[2]
w=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()))
h=ActorHost(w,root/'actors.sqlite');owner=w.acquire_coordinator_execution('kill-original')
ref=ActivationRef(**json.loads((root/'ref.json').read_text()))
body=json.loads((root/'body.json').read_text())
if mode=='adj':
 with w._coordinator_scope(owner):h.adjudicator=Adjudicator(w)
 def dm(payload):
  state=payload['world']
  return dict(world_id=state['world_id'],base_revision=state['revision'],ruling='成功',reason='offline resource acceptance',goal='ship seven',costs=[],benefits=[],effects=[dict(body,quantity=3),dict(body,quantity=4)])
 h.dm_provider=dm
h._choose=lambda view,seen:(({'action':'propose','text':'ship seven original text'} if mode=='adj' else body),seen)
actual=h._request;metrics=[]
def measured(payload):
 result=actual(payload)
 if payload.get('op')!='close':metrics.append({k:result[k] for k in ('sdk_requests','pid','provider')})
 return result
h._request=measured
def durable_gate(*args,**kwargs):
 node=h._process
 h._request({'op':'close'})
 node.wait(timeout=5)
 (root/'ready.json').write_text(json.dumps(dict(pid=__import__('os').getpid(),node_pid=node.pid,node_exit=node.returncode,metrics=metrics)))
 end=time.monotonic()+30
 while time.monotonic()<end:time.sleep(.02)
 raise RuntimeError('bounded kill gate expired')
h._finish=durable_gate
try:h.act(ref.person_id,coordinator=owner,activation=ref)
finally:h.close();w.release_coordinator_execution(owner)
'''
READ=r'''
import sqlite3,json,sys
from pathlib import Path
from contextlib import closing
root=Path(sys.argv[1]);out={}
for file,tables in [('world.sqlite',['coordinator_activations','coordinator_activation_outcomes','intents']),('actors.sqlite',['actor_intents','actor_session_heads','actor_sessions'])]:
 with closing(sqlite3.connect((root/file).as_uri()+'?mode=ro',uri=True)) as db:
  out[file]={t:db.execute('SELECT * FROM '+t+' ORDER BY 1').fetchall() for t in tables}
  out[file]['integrity']=db.execute('PRAGMA integrity_check').fetchone()[0]
  if file=='world.sqlite':out['snapshot']=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
print(json.dumps(out))
'''

class ActivationApplicationKillTests(unittest.TestCase):
 def test_characterization_resource_committed_application_kill_and_reopen(self):
  import psutil
  for mode in ('world','adj'):
   with self.subTest(mode=mode),tempfile.TemporaryDirectory(prefix='p023b2-application-kill-',dir=SCRATCH) as directory:
    root=Path(directory);(root/'clock').write_text('100000000000')
    world=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()));world.initialize()
    h=ActorHost(world,root/'actors.sqlite');owner=world.acquire_coordinator_execution('kill-fixture')
    world.control('resume',coordinator=owner);version={k:world.snapshot()[k] for k in ('world_id','revision')}
    boundary=world.inspect_scheduler_boundary(actor_db_path=h.db_path)
    world.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=h.db_path)
    window=world.open_or_recover_window(coordinator=owner,expected_version=version)
    h._request=provider_fixture.ActorExecutionTests.result;h._choose=lambda view,seen:({'action':'wait'},seen)
    row=world.next_activation(window['window_id'],coordinator=owner)
    ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
    h.act(ref.person_id,coordinator=owner,activation=ref)
    row=world.next_activation(window['window_id'],coordinator=owner)
    ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
    person=next(p for p in world.snapshot()['persons'] if p['id']==ref.person_id)
    self.assertEqual(person['office'],'finance')
    target=next(p for p in world.snapshot()['places'] if p['country_id']==person['country_id'] and p['id']!=person['location'])
    body=dict(action='ship',to=target['id'],quantity=7)
    baseline=world.snapshot();grain=next(p['grain'] for p in baseline['places'] if p['id']==person['location'])
    (root/'ref.json').write_text(json.dumps(ref.__dict__));(root/'body.json').write_text(json.dumps(body))
    h.close();world.release_coordinator_execution(owner)
    child=subprocess.Popen([sys.executable,'-B','-c',CHILD,str(root),mode],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    successor=None;replacement=None;application=None
    def readback():
     p=subprocess.run([sys.executable,'-B','-c',READ,str(root)],cwd=ROOT,capture_output=True,text=True,timeout=8)
     self.assertEqual(p.returncode,0,p.stderr);return json.loads(p.stdout)
    try:
     end=time.monotonic()+20
     while not (root/'ready.json').exists() and child.poll() is None and time.monotonic()<end:time.sleep(.02)
     if not (root/'ready.json').exists():
      if child.poll() is None:child.kill()
      output=child.communicate(timeout=5)[0];self.fail(output)
     ready=json.loads((root/'ready.json').read_text())
     descendants={p.pid for p in psutil.Process(child.pid).children(recursive=True)}
     self.assertIn(ready['pid'],descendants|{child.pid})
     application=psutil.Process(ready['pid'])
     self.assertEqual(ready['node_exit'],0);self.assertFalse(psutil.pid_exists(ready['node_pid']))
     before=readback();activation=next(a for a in before['world.sqlite']['coordinator_activations'] if a[2]==ref.activation_id)
     self.assertEqual(activation[5],'world_committed');original=activation[6]
     self.assertEqual(next(p['grain'] for p in before['snapshot']['places'] if p['id']==person['location']),grain-7)
     self.assertEqual(len(before['snapshot']['in_transit'])-len(baseline['in_transit']),2 if mode=='adj' else 1)
     application.kill();application.wait(timeout=5)
     child.communicate(timeout=5)
     self.assertFalse(psutil.pid_exists(ready['pid']));self.assertFalse(psutil.pid_exists(child.pid))
     self.assertNotEqual(child.returncode,0)
     (root/'clock').write_text('161000000000')
     reopened=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()))
     successor=reopened.acquire_coordinator_execution('kill-successor');replacement=ActorHost(reopened,root/'actors.sqlite')
     replacement._request=lambda payload:self.fail('world committed recovery cannot call Pi')
     result=replacement.act(ref.person_id,coordinator=successor,activation=ref)
     self.assertEqual(result['intent_id'],original)
     after=readback();self.assertEqual(after['snapshot'],before['snapshot'])
     self.assertEqual(after['world.sqlite']['intents'],before['world.sqlite']['intents'])
     self.assertEqual(after['world.sqlite']['coordinator_activation_outcomes'],before['world.sqlite']['coordinator_activation_outcomes'])
     heads=after['actors.sqlite']['actor_session_heads'];self.assertTrue(any(r[0]==ref.person_id and r[2]==1 and r[5]==original for r in heads))
     self.assertEqual(sum(m['sdk_requests'] for m in ready['metrics']),2)
     print('ACTIVATION_APPLICATION_KILL_SQL',json.dumps(dict(mode=mode,original_intent=original,grain_before=grain,grain_after=grain-7,ready=ready,application_exit=child.returncode,recovery_sdk_requests=0,before=before,after=after)))
    finally:
     if application is not None and application.is_running():
      application.kill();application.wait(timeout=5)
     if child.poll() is None:child.kill()
     child.communicate(timeout=5)
     if replacement is not None:replacement.close()
     if successor is not None:reopened.release_coordinator_execution(successor)
