"""Review regressions: real SQLite/HTTP, local workers; no models/network."""
import http.client
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from app.actors import ActorHost
from app.server import Coordinator, DAY, make_server
from app.world import WorldStore

SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')

class NoActors:
    def act(self, person): pass
    def close(self): pass

class ReviewRegressions(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.temp.cleanup)
        self.world = WorldStore(Path(self.temp.name) / 'world.sqlite')
        self.world.initialize()

    def near_horizon(self):
        self.world.control('resume')
        self.world.advance(self.world.snapshot()['horizon'] - DAY)
        self.world.control('pause')

    def test_horizon_boundary_preserves_normal_pause(self):
        for continuous in (False, True):
            with self.subTest(continuous=continuous):
                self.world = WorldStore(Path(self.temp.name) / f'boundary-{continuous}.sqlite')
                self.world.initialize()
                self.near_horizon()
                from app.tests.legacy_control_coordinator import LegacyControlCoordinator
                coordinator = LegacyControlCoordinator(self.world, NoActors())
                advance = self.world.advance
                def at_boundary(target):
                    result = advance(target)
                    coordinator.stop.set()
                    return result
                with patch.object(self.world, 'advance', at_boundary):
                    coordinator.start(continuous=continuous)
                    coordinator.worker.join(5)
                self.assertFalse(coordinator.worker.is_alive())
                self.assertEqual(self.world.snapshot()['status'], 'horizon_paused')
                self.assertIsNone(coordinator.error)
                coordinator.pause()
                self.assertEqual(self.world.snapshot()['status'], 'horizon_paused')

    def test_http_resume_runs_until_extended_horizon(self):
        self.world.control('resume')
        self.world.advance(self.world.snapshot()['horizon'])
        before = self.world.snapshot()['horizon']
        from app.tests.legacy_control_coordinator import LegacyControlCoordinator
        actors=NoActors()
        fixture=LegacyControlCoordinator(self.world,actors)
        server = make_server(self.world, actors, port=0, coordinator=fixture)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        connection = http.client.HTTPConnection(*server.server_address, timeout=5)
        try:
            connection.request('GET', '/api/meta')
            meta = json.loads(connection.getresponse().read())
            connection.request('POST', '/api/control', json.dumps({'action':'resume'}),
                {'Origin':f'http://127.0.0.1:{server.server_address[1]}',
                 'X-Theatre-CSRF':meta['csrf'], 'Content-Type':'application/json'})
            response = connection.getresponse()
            self.assertEqual(response.status, 200, response.read())
            server.coordinator.worker.join(20)
            self.assertFalse(server.coordinator.worker.is_alive())
            state = self.world.snapshot()
            self.assertEqual(state['horizon'], before + 90 * DAY)
            self.assertEqual(state['time'], state['horizon'])
            self.assertEqual(state['status'], 'horizon_paused')
            self.assertIsNone(server.coordinator.error)
        finally:
            connection.close()
            server.shutdown()
            server.server_close()
            thread.join(5)

    def local_worker(self, mode, original_popen):
        if mode == 'sleep':
            code = 'import time; time.sleep(30)'
        elif mode == 'exit':
            code = 'raise SystemExit(7)'
        elif mode == 'invalid':
            code = "import sys; sys.stdin.readline(); print('not-json', flush=True)"
        elif mode == 'mismatch':
            code = "import sys; sys.stdin.readline(); print('{\"id\":\"wrong\",\"ok\":true}', flush=True)"
        else:
            code = '''import sys,json,os
for line in sys.stdin:
 p=json.loads(line)
 result={'messages':[], 'calls':[], 'sdk_events':0, 'sdk_requests':0,
         'provider':'local-test-worker', 'pid':os.getpid(), 'module_root':'test',
         'intent_id':p.get('intent_id')}
 print(json.dumps({'id':p['id'],'ok':True,'result':result}), flush=True)
 if p.get('op') == 'close': break
'''
        return original_popen([sys.executable, '-u', '-c', code], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', bufsize=1)

    def test_worker_timeout_retries_prepared_identity(self):
        self.world.control('resume')
        person = self.world.snapshot()['persons'][0]['id']
        host = ActorHost(self.world, Path(self.temp.name) / 'actors.sqlite')
        host.timeout = .1
        original = subprocess.Popen
        modes = iter(('sleep', 'echo'))
        with patch('app.actors.subprocess.Popen', side_effect=lambda *a, **k: self.local_worker(next(modes), original)):
            try:
                with self.assertRaisesRegex(RuntimeError, 'PI_WORKER_TIMEOUT_OR_IO'):
                    host.act(person)
                with host._connect() as db:
                    pending = dict(db.execute('SELECT * FROM actor_intents').fetchone())
                self.assertEqual(pending['state'], 'prepared')
                self.assertIsNone(host._process, 'timeout must clear the terminated process')
                host.timeout = 2
                result = host.act(person)
                self.assertEqual(result['intent_id'], pending['intent_id'])
                self.assertEqual(result['body'], json.loads(pending['body']))
                with host._connect() as db:
                    rows = db.execute('SELECT intent_id,state FROM actor_intents').fetchall()
                self.assertEqual([(r['intent_id'],r['state']) for r in rows], [(pending['intent_id'],'committed')])
            finally:
                host.close()

    def test_worker_failures_clear_handles_before_retry(self):
        original = subprocess.Popen
        for mode in ('exit', 'invalid', 'mismatch', 'closed-stdin'):
            with self.subTest(mode=mode):
                host = ActorHost(self.world, Path(self.temp.name) / f'{mode}.sqlite')
                modes = iter(('sleep' if mode == 'closed-stdin' else mode, 'echo'))
                with patch('app.actors.subprocess.Popen', side_effect=lambda *a, **k: self.local_worker(next(modes), original)):
                    try:
                        host.timeout = 2
                        if mode == 'closed-stdin':
                            host._start()
                            host._process.stdin.close()
                        with self.assertRaisesRegex(RuntimeError, 'PI_WORKER_'):
                            host._request({'op':'probe'})
                        self.assertIsNone(host._process)
                        self.assertTrue(host._responses.empty())
                        self.assertEqual(host._request({'intent_id':'durable-original'})['intent_id'], 'durable-original')
                    finally:
                        host.close()

    def test_old_stdout_eof_does_not_enter_replacement_queue(self):
        host = ActorHost(self.world, Path(self.temp.name) / 'queues.sqlite')
        original = subprocess.Popen
        with patch('app.actors.subprocess.Popen', side_effect=lambda *a, **k: self.local_worker('sleep', original)):
            host._start()
            old_queue = host._responses
            replacement = queue.Queue()
            host._responses = replacement
            process = host._process
            try:
                process.terminate()
                process.wait(5)
                for reader in host._readers:
                    reader.join(2)
                self.assertTrue(replacement.empty(), 'old reader EOF polluted the next worker queue')
                self.assertIsNone(old_queue.get(timeout=1))
            finally:
                host.close()

if __name__ == '__main__': unittest.main()
