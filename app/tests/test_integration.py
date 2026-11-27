"""End-to-end checks: real SQLite world, real Pi worker and real loopback HTTP."""
from contextlib import closing
from pathlib import Path
import http.client
import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from app.world import WorldStore
from app.actors import ActorHost
from app.server import make_server

class IntegrationTests(unittest.TestCase):
    def test_four_days_replay_and_restart_use_actual_components(self):
        scratch=Path(os.environ['LOCALAPPDATA'])/'hermes/cache/scratch'
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root=Path(directory)
            world=WorldStore(root/'world.sqlite'); initial=world.initialize()
            host=ActorHost(world,root/'actors.sqlite')
            server=make_server(world,host,0)
            thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
            port=server.server_address[1]
            def request(route,body=None):
                connection=http.client.HTTPConnection('127.0.0.1',port,timeout=10)
                try:
                    headers={}
                    if body is not None:
                        headers={'Origin':f'http://127.0.0.1:{port}','X-Theatre-CSRF':token,'Content-Type':'application/json'}
                    connection.request('GET' if body is None else 'POST',route,
                                       body=None if body is None else json.dumps(body),headers=headers)
                    response=connection.getresponse(); data=json.loads(response.read())
                    self.assertLess(response.status,400,data)
                    return data
                finally: connection.close()
            try:
                token=request('/api/meta')['csrf']
                self.assertEqual(len(initial['persons']),12)
                for day in range(1,5):
                    request('/api/control',{'action':'step'})
                    deadline=time.monotonic()+60
                    while time.monotonic()<deadline:
                        state=request('/api/state')
                        if not state['runtime']['busy']: break
                        time.sleep(.05)
                    self.assertEqual(state['status'],'user_paused',state['runtime'])
                    self.assertEqual(state['time'],day*86400)
                before=world.snapshot()
                old=request('/api/state?seq=1')
                self.assertEqual(old['time'],0)
                self.assertEqual(world.snapshot(),before)
                king=request('/api/person?id=person-country-1-king')
                self.assertNotIn('pathology',king)
                self.assertNotIn('grain',json.dumps(king))
                self.assertTrue(king['messages'],'king must learn from actually delivered report')
                self.assertEqual(sum(p['grain'] for p in before['places'])+sum(x['quantity'] for x in before['in_transit'])+before['ledger']['consumed'],before['ledger']['initial']+before['ledger']['harvested'])
                with closing(sqlite3.connect(root/'actors.sqlite')) as db:
                    sessions=db.execute('SELECT COUNT(*) FROM actor_sessions').fetchone()[0]
                    metrics=[json.loads(row[0]) for row in db.execute("SELECT metrics FROM actor_intents WHERE state='committed'")]
                self.assertEqual(sessions,12)
                self.assertEqual(len(metrics),48)
                self.assertTrue(all(m['provider']=='scripted-pi' and m['sdk_requests']==2 for m in metrics))
            finally:
                server.shutdown(); server.server_close(); thread.join(5)
            self.assertEqual(host.worker_exit_code,0)
            restored=WorldStore(root/'world.sqlite'); self.assertEqual(restored.initialize(),before)
            reopened_host=ActorHost(restored,root/'actors.sqlite'); reopened_host.close()
            print('REAL_COMPONENTS',json.dumps({'days':4,'countries':len(before['countries']),'sessions':sessions,'committed_intents':len(metrics),'sdk_provider_requests':sum(m['sdk_requests'] for m in metrics),'node_exit':host.worker_exit_code,'replay_unchanged':True,'reopen_unchanged':True}))

    def test_real_node_kill_recovers_a_committed_shipment_once(self):
        from unittest.mock import patch
        scratch=Path(os.environ['LOCALAPPDATA'])/'hermes/cache/scratch'
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root=Path(directory); world=WorldStore(root/'world.sqlite'); world.initialize(); world.control('resume')
            finance='person-country-1-finance'
            world.submit('person-country-1-king','known-order',{'action':'command','to':finance,'text':'目的地：place-1 请核账后考虑赈粮。'})
            world.advance(60)
            host=ActorHost(world,root/'actors.sqlite')
            try:
                self.assertEqual(host.act(finance)['body']['action'],'respond')
                investigation=host.act(finance)
                self.assertEqual(investigation['body']['action'],'investigate')
                world.advance(investigation['receipt']['effect']['due'])
                before=next(p['grain'] for p in world.snapshot()['places'] if p['id']=='place-0')
                original_submit=world.submit
                def lose_ack(person,intent,body,*args,**kwargs):
                    original_submit(person,intent,body,*args,**kwargs)
                    raise RuntimeError('TEST_LOST_ACK_AFTER_REAL_COMMIT')
                with patch.object(world,'submit',lose_ack):
                    with self.assertRaisesRegex(RuntimeError,'TEST_LOST_ACK'):
                        host.act(finance)
                killed_pid=host.worker_pid
                host._process.kill(); killed_code=host._process.wait(5)
                recovered=host.act(finance)
                self.assertTrue(recovered['reconciled'])
                self.assertEqual(recovered['body'],{'action':'ship','to':'place-1','quantity':10})
                snapshot=world.snapshot()
                self.assertEqual(next(p['grain'] for p in snapshot['places'] if p['id']=='place-0'),before-10)
                self.assertEqual(len(snapshot['in_transit']),1)
                with closing(sqlite3.connect(root/'actors.sqlite')) as db:
                    self.assertEqual(db.execute('SELECT state FROM actor_intents WHERE intent_id=?',(recovered['intent_id'],)).fetchone()[0],'committed')
                host.act(finance)
                replacement_pid=host.worker_pid
                self.assertNotEqual(killed_pid,replacement_pid)
                print('REAL_NODE_KILL',json.dumps({'killed_pid':killed_pid,'kill_exit_code':killed_code,'replacement_pid':replacement_pid,'original_intent':recovered['intent_id'],'shipment_count':len(snapshot['in_transit'])}))
            finally:
                host.close()
            self.assertEqual(host.worker_exit_code,0)

if __name__=='__main__': unittest.main()
