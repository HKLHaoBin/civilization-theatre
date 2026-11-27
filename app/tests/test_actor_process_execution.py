"""Additional actual-process acceptance for implemented Host fencing.
Real Pi is offline scripted. Application hard kill is NOT power/reboot evidence.
"""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from app.world import WorldStore
from app.actors import ActorHost
from app.tests.test_actor_execution import SCRATCH

ROOT=Path(__file__).resolve().parents[2]
PERSON='person-country-1-finance'
CHILD=r'''
import json,sys,time
from pathlib import Path
from app.world import WorldStore
from app.actors import ActorHost
root=Path(sys.argv[1]);mode=sys.argv[2]
def clock():return int((root/'clock').read_text())
w=WorldStore(root/'world.sqlite',execution_clock_ns=clock)
h=ActorHost(w,root/'actors.sqlite')
h._choose=lambda view,seen:({'action':'ship','to':'place-1','quantity':10},seen+['seen-'+str(len(seen)+1)])
def ready():
    (root/'ready').write_text(mode)
    end=time.monotonic()+15
    while not (root/'continue').exists():
        if time.monotonic()>end:raise RuntimeError('test gate timeout')
        time.sleep(.01)
if mode=='late_candidate':
    request=h._request
    def delayed(payload):
        value=request(payload);ready();return value
    h._request=delayed
elif mode=='late_finish':
    finish=h._finish
    def delayed(*args,**kwargs):ready();return finish(*args,**kwargs)
    h._finish=delayed
elif mode=='hardkill':
    finish=h._finish
    def close_node_then_finish(*args,**kwargs):
        h._request({'op':'close'});h._process.wait(5);h._stop()
        (root/'hardkill-node-exit.json').write_text(json.dumps({'exit':h.worker_exit_code}))
        return finish(*args,**kwargs)
    h._finish=close_node_then_finish
    guard=w._check_actor_execution
    def gate(db,*args,**kwargs):
        value=guard(db,*args,**kwargs)
        if 'actor' in [r[1] for r in db.execute('PRAGMA database_list')]:
            row=db.execute('SELECT state FROM actor.actor_intents').fetchone()
            if row and row[0]=='committed':ready()
        return value
    w._check_actor_execution=gate
try:
    result=h.act('person-country-1-finance')
    if mode=='successor':
        recovered=result['intent_id'];h.act('person-country-1-finance')
    else:recovered=result['intent_id']
    outcome={'ok':True,'original_id':recovered}
except Exception as error:
    outcome={'ok':False,'code':getattr(error,'code',type(error).__name__)}
finally:
    h.close()
outcome.update(node_exit=h.worker_exit_code,cleanup_pending=h.cleanup_pending)
(root/(mode+'.json')).write_text(json.dumps(outcome))
print(json.dumps(outcome),flush=True)
'''


class ActualHostProcessTests(unittest.TestCase):
    def backup(self,root,folder):
        folder.mkdir(exist_ok=True)
        for name in ('world.sqlite','actors.sqlite'):
            with closing(sqlite3.connect((root/name).resolve().as_uri()+'?mode=ro',uri=True)) as source, closing(sqlite3.connect(folder/name)) as target:
                source.backup(target)

    def start_child(self,root,mode,folder):
        env=dict(os.environ,PI_MODULE_ROOT=str(ROOT/'spikes/002-pi-isolation/npm/node_modules'))
        log=open(folder/(mode+'.log'),'wb')
        p=subprocess.Popen([sys.executable,'-B','-c',CHILD,str(root),mode],cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
        return p,log

    def wait_ready(self,root,p):
        end=time.monotonic()+12
        while not (root/'ready').exists() and time.monotonic()<end:
            self.assertIsNone(p.poll(),'child failed before production seam');time.sleep(.02)
        self.assertTrue((root/'ready').exists(),'actual Host did not reach gated seam')

    def inventory(self,root):
        with closing(sqlite3.connect(root/'actors.sqlite')) as db:
            session=db.execute('SELECT messages,seen FROM actor_sessions').fetchone()
            rows=db.execute('SELECT intent_id,state,candidate,receipt FROM actor_intents ORDER BY intent_id').fetchall()
            heads=db.execute('SELECT version,messages_sha256,seen_sha256 FROM actor_session_heads').fetchall()
            meta=db.execute('SELECT intent_id,base_version,owner_epoch,promoted_version FROM actor_intent_execution ORDER BY base_version').fetchall()
            calls=db.execute('SELECT call_id,intent_id,person_id FROM actor_calls ORDER BY call_id').fetchall()
            check=db.execute('PRAGMA integrity_check').fetchone()[0]
        with closing(sqlite3.connect(root/'world.sqlite')) as db:
            leases=db.execute('SELECT epoch,owner_token IS NULL FROM actor_execution_leases').fetchall()
            receipts=db.execute('SELECT id,receipt FROM intents WHERE person=? AND receipt IS NOT NULL ORDER BY id',(PERSON,)).fetchall()
        return dict(session=session,intents=rows,heads=heads,meta=meta,calls=calls,integrity=check,leases=leases,receipts=receipts)

    def run_takeover(self,mode):
        folder=ROOT/'app/evidence'/('p022-backend-process-'+mode+'-'+uuid.uuid4().hex);folder.mkdir()
        with tempfile.TemporaryDirectory(prefix='p022-process-',dir=SCRATCH) as temp:
            root=Path(temp);(root/'clock').write_text('100000000000')
            w=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()));w.initialize();w.control('resume')
            a,alog=self.start_child(root,mode,folder);b=None;blog=None
            try:
                self.wait_ready(root,a)
                before=self.inventory(root);original=before['intents'][0][0]
                b,blog=self.start_child(root,'contender',folder);self.assertEqual(b.wait(8),0);blog.close()
                contender_exit=b.returncode
                contender=json.loads((root/'contender.json').read_text());self.assertFalse(contender['ok']);self.assertEqual(contender['code'],'ACTOR_EXECUTION_BUSY')
                self.assertEqual(contender['node_exit'],None);self.assertEqual(self.inventory(root),before)
                (root/'clock').write_text('161000000000')
                b,blog=self.start_child(root,'successor',folder);self.assertEqual(b.wait(12),0);blog.close()
                successor=json.loads((root/'successor.json').read_text());self.assertTrue(successor['ok']);self.assertEqual(successor['original_id'],original)
                after_successor=self.inventory(root)
                (root/'continue').write_text('release old callback');self.assertEqual(a.wait(12),0);alog.close()
                stale=json.loads((root/(mode+'.json')).read_text());self.assertFalse(stale['ok']);self.assertEqual(stale['code'],'ACTOR_EXECUTION_FENCED')
                after=self.inventory(root);self.assertEqual(after,after_successor,'late candidate/finish must make ZERO private or World writes')
                self.assertEqual(len(json.loads(after['session'][0])),9);self.assertEqual(json.loads(after['session'][1]),['seen-1','seen-2'])
                self.assertEqual(after['heads'][0][0],2);self.assertEqual(after['leases'],[(3,1)]);self.assertEqual(after['integrity'],'ok')
                self.assertEqual(len(after['receipts']),2);self.assertTrue(all(r[1]=='committed' for r in after['intents']))
                self.assertFalse(stale['cleanup_pending']);self.assertFalse(successor['cleanup_pending']);self.assertEqual(successor['node_exit'],0)
                self.backup(root,folder/'final')
                result=dict(mode=mode,source_sha256={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'app/actors.py',ROOT/'app/actor_execution.py')},
                            before=before,after=after,old=stale,successor=successor,contender=contender,python_exits=[a.returncode,b.returncode,contender_exit],scratch=str(root))
                (folder/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
                print('ACTUAL_HOST_TAKEOVER',mode,folder)
            finally:
                (root/'continue').write_text('cleanup')
                for p,log in ((a,alog),(b,blog)):
                    if p is not None:
                        if p.poll() is None:p.kill();p.wait(5)
                        if log and not log.closed:log.close()

    def test_c_independent_hosts_discard_late_candidate_at_epoch_takeover(self):self.run_takeover('late_candidate')
    def test_c_independent_hosts_recover_world_committed_then_reject_old_finish_9_to_5(self):self.run_takeover('late_finish')

    def test_c_actual_private_promotion_hardkill_rolls_back_journal_then_recovers_original_id(self):
        folder=ROOT/'app/evidence'/('p022-backend-process-hardkill-'+uuid.uuid4().hex);folder.mkdir()
        with tempfile.TemporaryDirectory(prefix='p022-kill-',dir=SCRATCH) as temp:
            root=Path(temp);(root/'clock').write_text('100000000000')
            world=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:int((root/'clock').read_text()));world.initialize();world.control('resume')
            p,log=self.start_child(root,'hardkill',folder)
            try:
                self.wait_ready(root,p)
                (root/'clock').write_text('161000000000')
                challenger=r'''import sys,json;from pathlib import Path;from app.world import WorldStore
r=Path(sys.argv[1]);w=WorldStore(r/'world.sqlite',execution_clock_ns=lambda:int((r/'clock').read_text()))
try:w.acquire_actor_execution('person-country-1-finance','competing-real-process');print('unexpected owner');sys.exit(2)
except Exception as e:print(getattr(e,'code',type(e).__name__));sys.exit(0 if getattr(e,'code',None)=='EXECUTION_STORAGE_BUSY' else 3)
'''
                competition=subprocess.run([sys.executable,'-B','-c',challenger,str(root)],cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=5)
                (folder/'same-lock-challenger.log').write_bytes(competition.stdout);self.assertEqual(competition.returncode,0)
                p.kill();p.wait(5);log.close();self.assertNotEqual(p.returncode,0)
                journal=root/'actors.sqlite-journal';self.assertTrue(journal.exists());self.assertGreater(journal.stat().st_size,0)
                shutil.copy2(journal,folder/'actual-actors.sqlite-journal')
                raw=folder/'kill-before-reopen';raw.mkdir()
                for name in ('world.sqlite','actors.sqlite','actors.sqlite-journal'):shutil.copy2(root/name,raw/name)
                before=self.inventory(root);self.assertEqual(before['heads'][0][0],0)
                self.assertEqual(json.loads(before['session'][0]),[]);self.assertEqual(before['intents'][0][1],'candidate')
                intent=before['intents'][0][0];receipt=world.receipt(PERSON,intent);self.assertIsNotNone(receipt);facts=world.snapshot()
                (root/'clock').write_text('161000000000')
                host=ActorHost(world,root/'actors.sqlite');host._request=lambda payload:self.fail('World committed candidate must not resend SDK')
                try:result=host.act(PERSON);self.assertEqual(result['intent_id'],intent);self.assertEqual(result['receipt'],receipt)
                finally:host.close()
                after=self.inventory(root);self.assertEqual(world.snapshot(),facts);self.assertEqual(after['heads'][0][0],1)
                self.assertEqual(len(json.loads(after['session'][0])),5);self.assertEqual(after['leases'],[(2,1)])
                self.assertEqual(after['integrity'],'ok');self.assertEqual(len(after['receipts']),1)
                self.backup(root,folder/'final')
                node_exit=json.loads((root/'hardkill-node-exit.json').read_text());self.assertEqual(node_exit['exit'],0)
                (folder/'result.json').write_text(json.dumps(dict(before=before,after=after,python_kill_exit=p.returncode,scratch=str(root),node_cleanup=node_exit),ensure_ascii=False,indent=2),encoding='utf-8')
                print('ACTUAL_PRIVATE_PROMOTION_HARDKILL',folder)
            finally:
                if p.poll() is None:p.kill();p.wait(5)
                if not log.closed:log.close()
