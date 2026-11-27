"""First required replay entry regression; absence is not proof of database effects."""
import importlib.util
import unittest
class ReadOnlyReplayEntryTests(unittest.TestCase):
    def test_explicit_readonly_service_entry_exists(self):
        self.assertIsNotNone(importlib.util.find_spec('app.replay'), 'explicit readonly replay service entry does not exist')
import hashlib
import http.client
import json
from pathlib import Path
import shutil
import tempfile
import threading
import uuid

ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT/'evidence/p11-readonly-replay-v1'

class ReplayFactoryTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((EVIDENCE/'fixture-config.json').read_text(encoding='utf-8'))
        self.temp = tempfile.TemporaryDirectory(dir=self.config['scratch'])
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name)
        shutil.copyfile(self.config['fixture'], self.data/'world.sqlite')
        self.manifest = dict(world_id=self.config['world_id'], files={'world.sqlite':self.config['fixture_sha256'], **{name:hashlib.sha256((ROOT/'app/web'/name).read_bytes()).hexdigest() for name in ('index.html','app.js','app.css')}})
        self.manifest_path = self.data/'manifest.json'
        self.manifest_path.write_text(json.dumps(self.manifest), encoding='utf-8')
        self.kwargs = dict(source_manifest=self.manifest_path, data_dir=self.data, web_root=ROOT/'app/web', nonce=uuid.uuid4().hex, port=0)

    def factory(self, **overrides):
        from app import replay
        factory = getattr(replay, 'make_replay_server', None)
        self.assertTrue(callable(factory), 'read-only factory missing')
        return factory(**(self.kwargs | overrides))

    def start(self):
        server = self.factory()
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval':0.01})
        thread.start()
        def close():
            server.shutdown(); server.server_close(); thread.join(3)
            self.assertFalse(thread.is_alive())
        self.addCleanup(close)
        self.server = server
        return server

    def request(self, path, method='GET', headers=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        try:
            conn.request(method,path,headers=headers or {})
            response = conn.getresponse()
            raw = response.read()
            return response.status, dict(response.getheaders()), (json.loads(raw) if response.getheader('Content-Type','').startswith('application/json') else raw) if raw else None
        finally:
            conn.close()

    def test_factory_serves_explicit_observation_meta_without_csrf(self):
        self.start()
        status, headers, body = self.request('/api/meta')
        self.assertEqual(status,200)
        self.assertEqual(body['mode'],'readonly-replay')
        self.assertIs(body['read_only'],True)
        self.assertNotIn('csrf',body)
        self.assertEqual(set(body['capabilities']),{'readonly-replay','history-person','causal-trace','map-topology'})
        self.assertEqual(headers['Cache-Control'],'no-store')
        self.assertEqual(headers['X-Content-Type-Options'],'nosniff')
        self.assertIn("object-src 'none'",headers['Content-Security-Policy'])

    def test_factory_rejects_unbound_inputs_without_creating_or_opening_sqlite(self):
        from unittest.mock import patch
        import sqlite3
        attempts = []
        original = sqlite3.connect
        def record(*args, **kwargs):
            attempts.append(str(args[0])); return original(*args, **kwargs)
        missing = self.data/'missing'
        with patch('sqlite3.connect', record):
            for bad in [dict(port=True),dict(port=-1),dict(port=65536),dict(nonce=''),dict(nonce='bad\nnonce'),dict(data_dir=missing),dict(web_root=missing),dict(source_manifest=missing)]:
                with self.subTest(bad=bad):
                    server = None
                    try:
                        with self.assertRaises((ValueError, OverflowError)): server = self.factory(**bad)
                    finally:
                        if server: server.server_close()
        self.assertEqual(attempts,[])
        self.assertFalse(missing.exists())
        for change in ['world','sha','web','shape','sidecar','missing-db']:
            manifest = json.loads(json.dumps(self.manifest))
            if change=='world': manifest['world_id']='not-a-world'
            elif change=='sha': manifest['files']['world.sqlite']='0'*64
            elif change=='web': manifest['files']['app.js']='0'*64
            elif change=='shape': manifest['extra']=True
            elif change=='sidecar': (self.data/'world.sqlite-wal').write_bytes(b'unsafe')
            elif change=='missing-db': (self.data/'world.sqlite').unlink()
            self.manifest_path.write_text(json.dumps(manifest),encoding='utf-8')
            server = None
            try:
                with self.subTest(change=change), self.assertRaises(ValueError): server = self.factory()
            finally:
                if server: server.server_close()
                if change=='sidecar': (self.data/'world.sqlite-wal').unlink()
                if change=='missing-db': shutil.copyfile(self.config['fixture'],self.data/'world.sqlite')
        self.manifest_path.write_text(json.dumps(self.manifest),encoding='utf-8')
        server=self.factory(); server.server_close()

    def test_http_rejects_all_non_get_without_body_or_writer_and_checks_trust(self):
        self.start()
        for method in ['POST','PUT','PATCH','DELETE','HEAD','OPTIONS','TRACE','CONNECT','BREW']:
            with self.subTest(method=method):
                status, headers, body = self.request('/api/control',method,{'Content-Length':'999999999','X-Theatre-CSRF':'forged'})
                self.assertEqual(status,405)
                if method=='HEAD': self.assertIsNone(body)
                else: self.assertEqual(body['error']['code'],'READ_ONLY')
        for method in ['GET','POST','HEAD','BREW']:
            for headers in [{'Host':'evil.example'},{'Origin':'http://evil.example'}]:
                with self.subTest(method=method,headers=headers):
                    self.assertEqual(self.request('/api/meta',method,headers)[0],403)
        status, _, body = self.request('/not-an-api')
        self.assertEqual(status,404)
        self.assertEqual(body['error']['code'],'NOT_FOUND')

    def test_real_state_person_trace_are_revision_pinned_through_only_closed_ro_sql(self):
        from unittest.mock import patch
        import sqlite3
        from contextlib import ExitStack
        from app.world import WorldStore
        from app.adjudication import Adjudicator
        connections, sql, writes = [], [], []
        original = sqlite3.connect
        class Recorded(sqlite3.Connection):
            def close(db):
                connections[db.record_index]['closed'] = True
                super().close()
        def connect(target, *args, **kwargs):
            row = dict(target=str(target), uri=kwargs.get('uri'), closed=False)
            connections.append(row)
            db = original(target,*args,**(kwargs | {'factory':Recorded}))
            db.record_index = len(connections)-1
            db.set_trace_callback(sql.append)
            return db
        def forbidden(*args, **kwargs):
            writes.append('writer'); raise RuntimeError('writer forbidden')
        expected = json.loads((EVIDENCE/'original-reader-characterization.json').read_text(encoding='utf-8'))
        before = hashlib.sha256((self.data/'world.sqlite').read_bytes()).hexdigest()
        with ExitStack() as stack:
            stack.enter_context(patch('sqlite3.connect',connect))
            for owner, name in [(WorldStore,'initialize'),(WorldStore,'_mutate'),(Adjudicator,'__init__')]:
                stack.enter_context(patch.object(owner,name,forbidden))
            self.start()
            for case, events, grain, proposals in zip(expected['cases'],[0,7,14],[9200,9120,9120],[0,5,12]):
                seq=case['seq']
                status, _, state = self.request(f'/api/state?seq={seq}')
                self.assertEqual(status,200)
                self.assertIn('revision',state,'state route must serve original world projection')
                public = {k:v for k,v in state.items() if k not in ('runtime','causality')}
                self.assertEqual(public,case['public_state'])
                self.assertEqual(len(state['events']),events)
                self.assertEqual(sum(p['grain'] for p in state['places']),grain)
                self.assertEqual(len(state['causality']['proposals']),proposals)
                self.assertEqual(state['causality']['revision'],seq)
                self.assertEqual(state['causality']['world_id'],self.config['world_id'])
                self.assertEqual(state['runtime'],dict(read_only=True,busy=False,pause_requested=False,error=None,historical=True))
                self.assertFalse(any(k.startswith('_') for k in state))
                for person_id, person in case['persons'].items():
                    status, _, body = self.request(f'/api/person?id={person_id}&seq={seq}')
                    self.assertEqual(status,200)
                    self.assertEqual(body,person)
            status, _, latest = self.request('/api/state')
            self.assertEqual(status,200)
            self.assertEqual(latest['revision'],16)
            self.assertIs(latest['runtime']['read_only'],True)
            self.assertNotIn('historical',latest['runtime'])
            from app.replay import ReadOnlyTrace
            reader=ReadOnlyTrace(WorldStore(self.data/'world.sqlite'))
            self.assertIs(reader.trace.__func__,Adjudicator.trace)
            self.assertIs(reader._read.__func__,Adjudicator._read)
            self.assertIs(reader._child_id,Adjudicator._child_id)
            self.assertFalse(isinstance(reader,Adjudicator))
            self.assertFalse(hasattr(reader,'resolve'))
        self.assertEqual(writes,[])
        self.assertTrue(connections)
        self.assertTrue(all(x['uri'] is True and x['target'].endswith('/world.sqlite?mode=ro') and x['closed'] for x in connections))
        self.assertTrue(all(s.lstrip().split()[0].upper() in ('SELECT','BEGIN') for s in sql))
        self.assertEqual(hashlib.sha256((self.data/'world.sqlite').read_bytes()).hexdigest(),before)
        self.assertFalse(any((self.data/('world.sqlite'+s)).exists() for s in ('-wal','-shm','-journal')))
        print(json.dumps(dict(proof='real-readonly-boundary',connections=connections,sql=sql,writer_calls=writes,sha256=before)))

    def test_query_validation_never_falls_back_to_latest(self):
        self.start()
        for value in ['0','-1','','1.5','%EF%BC%91','1&seq=8','9'*100]:
            for route in ['/api/state?', '/api/person?id=person-country-1-king&']:
                with self.subTest(value=value,route=route):
                    status, _, body = self.request(route+'seq='+value)
                    self.assertEqual(status,400)
                    self.assertEqual(body['error']['code'],'INVALID_SEQUENCE')
        for path, code in [('/api/state?wat=1','INVALID_SEQUENCE'),('/api/person','INVALID_PERSON'),('/api/person?id=','INVALID_PERSON'),('/api/person?id=x&id=y','INVALID_PERSON'),('/api/person?id=x&wat=1','INVALID_PERSON')]:
            with self.subTest(path=path):
                status, _, body = self.request(path)
                self.assertEqual(status,400)
                self.assertEqual(body['error']['code'],code)
        for path, code in [('/api/state?seq=17','UNKNOWN_SNAPSHOT'),('/api/person?id=x&seq=17','UNKNOWN_SNAPSHOT'),('/api/person?id=x&seq=1','UNKNOWN_PERSON')]:
            with self.subTest(path=path):
                status, _, body = self.request(path)
                self.assertEqual(status,409)
                self.assertEqual(body['error']['code'],code)
        self.assertEqual(self.request('/api/state?seq=8')[2]['revision'],8)

    def test_static_allowlist_serves_exact_pinned_bytes_without_path_fallback(self):
        self.start()
        for route, name in [('/','index.html'),('/index.html','index.html'),('/app.js','app.js'),('/app.css','app.css')]:
            status, headers, body = self.request(route)
            self.assertEqual(status,200)
            self.assertEqual(body,(ROOT/'app/web'/name).read_bytes())
            self.assertEqual(headers['Cache-Control'],'no-store')
        for path in ['/../world.sqlite','/world.sqlite','/api/meta?extra=1','/app.js?extra=1','/%2e%2e/world.sqlite']:
            self.assertEqual(self.request(path)[0],404)

    def test_cli_parses_five_required_arguments_before_core_or_storage(self):
        import subprocess
        import sys
        from app import replay
        self.assertTrue(callable(getattr(replay,'main',None)), 'explicit CLI main missing')
        code = '''import builtins,runpy,sys,sqlite3,http.server
original=builtins.__import__
def guard(name,*args,**kwargs):
    if name.startswith(('app.world','app.actors','app.server','app.adjudication')) or name in ('world','actors','server','adjudication'):
        raise AssertionError('core import before argument validation')
    return original(name,*args,**kwargs)
def forbidden(*args,**kwargs): raise AssertionError('storage or HTTP initialized')
builtins.__import__=guard
sqlite3.connect=forbidden
http.server.HTTPServer.__init__=forbidden
sys.argv=['app.replay']+sys.argv[1:]
runpy.run_module('app.replay',run_name='__main__')
'''
        cases=[(['--help'],0),([],2),(['--source-manifest','missing','--data-dir','missing','--web-root','missing','--nonce','x','--port','-1'],2)]
        for args, expected in cases:
            completed=subprocess.run([sys.executable,'-B','-c',code,*args],cwd=ROOT,capture_output=True,text=True,timeout=5)
            self.assertEqual(completed.returncode,expected,completed.stderr)
            self.assertNotIn('AssertionError',completed.stderr)
            if args==['--help']:
                for flag in ['--source-manifest','--data-dir','--web-root','--nonce','--port']:
                    self.assertIn(flag,completed.stdout)
            print(json.dumps(dict(proof='CLI-parse-only',argv=[sys.executable,'-B','-c',code,*args],cwd=str(ROOT),exit=completed.returncode,stdout=completed.stdout,stderr=completed.stderr)))

    def test_storage_ro_connection_rejects_deliberate_sql_write_negative(self):
        import sqlite3
        from app.world import WorldStore
        before = (self.data/'world.sqlite').read_bytes()
        statements=[]
        with WorldStore(self.data/'world.sqlite')._connection(readonly=True) as db:
            db.set_trace_callback(statements.append)
            with self.assertRaises(sqlite3.OperationalError) as caught:
                db.execute('CREATE TABLE p11_forbidden_write (id INTEGER)')
            self.assertIn('readonly',str(caught.exception).lower())
        self.assertEqual((self.data/'world.sqlite').read_bytes(),before)
        self.assertFalse((self.data/'world.sqlite-journal').exists())
        print(json.dumps(dict(proof='deliberate-SQL-write-negative-not-reader-SQL',statements=statements,error=str(caught.exception),unchanged=True)))

    def test_additional_manifest_negatives_are_fail_closed(self):
        for suffix in ['-shm','-journal']:
            sidecar=self.data/('world.sqlite'+suffix)
            sidecar.write_bytes(b'unsafe')
            try:
                with self.assertRaises(ValueError): self.factory()
            finally: sidecar.unlink()
        self.manifest['world_id']=str(uuid.uuid4())
        self.manifest_path.write_text(json.dumps(self.manifest),encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'WORLD_MISMATCH'): self.factory()
        for raw in ['null','[]','{','{}']:
            self.manifest_path.write_text(raw,encoding='utf-8')
            with self.assertRaises(ValueError): self.factory()

    def test_idle_browser_preconnection_cannot_block_observation_requests(self):
        import socket
        self.start()
        accepted=threading.Event()
        handler=self.server.RequestHandlerClass
        original=handler.setup
        def setup(instance):
            original(instance); accepted.set()
        handler.setup=setup
        idle=socket.create_connection(self.server.server_address,timeout=2)
        done=threading.Event(); outcome=[]
        def read():
            try: outcome.append(self.request('/api/meta'))
            except Exception as error: outcome.append(str(error))
            finally: done.set()
        worker=threading.Thread(target=read)
        try:
            self.assertTrue(accepted.wait(1))
            worker.start()
            completed_while_idle=done.wait(0.5)
        finally:
            idle.close()
            if worker.ident is not None: worker.join(4)
            handler.setup=original
        self.assertFalse(worker.is_alive())
        self.assertTrue(completed_while_idle,'browser preconnection blocked GET')
        self.assertEqual(outcome[0][0],200)

if __name__ == '__main__':
    unittest.main()
