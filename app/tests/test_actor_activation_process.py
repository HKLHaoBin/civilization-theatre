"""Additional characterization: actual independent Host processes, no SDK.
Application/process recovery is not machine-reboot or provider billing evidence.
"""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from app.actors import ActorHost
from app.coordinator_scheduler import ActivationRef
from app.world import WorldStore
from app.tests import test_actor_execution as provider_fixture

ROOT=Path(__file__).resolve().parents[2]
SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
CHILD=r'''
import json,sys,time,tempfile
from pathlib import Path
from app.actors import ActorHost
from app.world import WorldStore
from app.coordinator_scheduler import ActivationRef
from app.tests.test_actor_execution import ActorExecutionTests
tempfile.tempdir='C:/Users/We3q/AppData/Local/hermes/cache/scratch'
root=Path(sys.argv[1]);mode=sys.argv[2]
w=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()))
p=w.acquire_coordinator_execution('independent-original-host')
h=ActorHost(w,root/'actors.sqlite')
ref=ActivationRef(**json.loads((root/'reference.json').read_text()))
calls=[]
def gate():
 (root/'ready').write_text('durable '+mode)
 end=time.monotonic()+20
 while not (root/'continue').exists():
  if time.monotonic()>end:raise RuntimeError('bounded process gate timed out')
  time.sleep(.01)
def request(payload):
 calls.append(payload['intent_id'])
 if mode=='prepared':gate()
 return ActorExecutionTests.result(payload)
h._request=request
h._choose=lambda view,seen:({'action':'wait'},seen)
if mode=='world_committed':
 finish=h._finish
 def delayed(*args,**kwargs):gate();return finish(*args,**kwargs)
 h._finish=delayed
try:
 result=h.act(ref.person_id,coordinator=p,activation=ref)
 outcome=dict(ok=True,intent=result['intent_id'])
except Exception as e:
 outcome=dict(ok=False,code=getattr(e,'code',type(e).__name__))
finally:
 h.close();w.release_coordinator_execution(p)
outcome.update(request_seam_calls=len(calls),actual_sdk_requests=0,cleanup_pending=h.cleanup_pending,node_exit=h.worker_exit_code)
(root/'old-result.json').write_text(json.dumps(outcome))
print(json.dumps(outcome),flush=True)
'''
READBACK=r'''
import json,sqlite3,sys
from contextlib import closing
from pathlib import Path
r=Path(sys.argv[1]);out={}
for file,tables in [('world.sqlite',['coordinator_activations','coordinator_activation_outcomes','intents','actor_execution_leases','actor_activation_delegations','actor_coordinator_delegations','coordinator_execution_leases']),('actors.sqlite',['actor_intents','actor_calls','actor_sessions','actor_session_heads','actor_intent_execution'])]:
 with closing(sqlite3.connect((r/file).resolve().as_uri()+'?mode=ro',uri=True)) as db:
  out[file]={t:db.execute('SELECT * FROM '+t+' ORDER BY 1,2').fetchall() for t in tables}
  out[file]['integrity']=db.execute('PRAGMA integrity_check').fetchone()[0]
print(json.dumps(out,sort_keys=True))
'''


class IndependentActivationHostTests(unittest.TestCase):
    def test_characterization_old_process_callback_cannot_write_after_root_takeover(self):
        for mode in ('prepared','world_committed'):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory(prefix='p023b2-process-',dir=SCRATCH) as directory:
                root=Path(directory)
                (root/'clock').write_text('100000000000')
                world=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()))
                world.initialize()
                host=ActorHost(world,root/'actors.sqlite')
                owner=world.acquire_coordinator_execution('process-fixture')
                try:
                    world.control('resume',coordinator=owner)
                    version={k:world.snapshot()[k] for k in ('world_id','revision')}
                    boundary=world.inspect_scheduler_boundary(actor_db_path=host.db_path)
                    world.register_scheduler(coordinator=owner,expected_version=version,boundary=boundary['boundary'],actor_db_path=host.db_path)
                    window=world.open_or_recover_window(coordinator=owner,expected_version=version)
                    row=world.next_activation(window['window_id'],coordinator=owner)
                    ref=ActivationRef(**{k:row[k] for k in ('world_id','window_id','activation_id','person_id')})
                    (root/'reference.json').write_text(json.dumps(ref.__dict__))
                finally:
                    host.close();world.release_coordinator_execution(owner)
                child=subprocess.Popen([sys.executable,'-B','-c',CHILD,str(root),mode],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
                successor=None;new=None
                try:
                    end=time.monotonic()+12
                    while not (root/'ready').exists() and time.monotonic()<end:
                        self.assertIsNone(child.poll(),'original process failed before durable seam')
                        time.sleep(.02)
                    self.assertTrue((root/'ready').exists())
                    with closing(sqlite3.connect(world.db_path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
                        original=db.execute('SELECT phase,intent_id,request_sha256,created_parent_epoch FROM coordinator_activations WHERE activation_id=?',(ref.activation_id,)).fetchone()
                    self.assertEqual(original[0],mode)
                    (root/'clock').write_text('161000000000')
                    successor=world.acquire_coordinator_execution('independent-successor')
                    new=ActorHost(world,root/'actors.sqlite')
                    requests=[]
                    def request(payload):
                        requests.append(payload['intent_id']);return provider_fixture.ActorExecutionTests.result(payload)
                    new._request=request
                    new._choose=lambda *args:self.fail('original durable intent must survive process takeover')
                    result=new.act(ref.person_id,coordinator=successor,activation=ref)
                    self.assertEqual(result['intent_id'],original[1])
                    self.assertEqual(len(requests),1 if mode=='prepared' else 0)
                    def readback():
                        p=subprocess.run([sys.executable,'-B','-c',READBACK,str(root)],cwd=ROOT,capture_output=True,text=True,timeout=8)
                        self.assertEqual(p.returncode,0,p.stderr)
                        return json.loads(p.stdout)
                    before=readback()
                    (root/'continue').write_text('release original callback')
                    output,_=child.communicate(timeout=10)
                    self.assertEqual(child.returncode,0,output)
                    stale=json.loads((root/'old-result.json').read_text())
                    self.assertFalse(stale['ok'])
                    self.assertEqual(stale['code'],'COORDINATOR_EXECUTION_FENCED')
                    self.assertFalse(stale['cleanup_pending'])
                    self.assertEqual(readback(),before,'late original callback must make zero SQL writes')
                    self.assertEqual(before['actors.sqlite']['actor_session_heads'][0][2],1)
                    self.assertEqual(len(before['world.sqlite']['intents']),1)
                    activation=next(r for r in before['world.sqlite']['coordinator_activations'] if r[2]==ref.activation_id)
                    self.assertEqual((activation[5],activation[6],activation[15],activation[18],activation[19]),('completed',original[1],1,1,3))
                    self.assertEqual(activation[10],original[2])
                    print('ACTIVATION_PROCESS_SQL',json.dumps(dict(mode=mode,original=original,activation=activation,stale=stale,successor_requests=len(requests),actual_sdk_requests=0,python_exit=child.returncode,private_head=before['actors.sqlite']['actor_session_heads'],receipts=before['world.sqlite']['intents'],integrity=[before[f]['integrity'] for f in ('world.sqlite','actors.sqlite')]),sort_keys=True))
                finally:
                    (root/'continue').write_text('cleanup')
                    if child.poll() is None:child.kill()
                    child.communicate(timeout=5)
                    if new is not None:new.close()
                    if successor is not None:world.release_coordinator_execution(successor)
