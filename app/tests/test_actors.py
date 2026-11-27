"""Actor tests use a minimal fake-world, but an actual installed Pi SDK/Node.
These are NOT actor/world integration tests and do not call a real model.
"""
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest

from app.actors import ActorHost


class FakeWorld:
    def __init__(self):
        self.world_id = 'fake-world'  # Explicit ownership protocol for this test double.
        self.views = {}
        self.receipts = {}
        self.calls = []
        self.lookups = []
        self.fail_after_commit = False

    def observe(self, person_id):
        return self.views.get(person_id, {
            'person': {'id': person_id, 'name': person_id, 'office': '君主',
                       'location': '都城', 'card': {}},
            'time': 0, 'observations': [], 'messages': [], 'contacts': []})

    def receipt(self, person_id, intent_id):
        self.lookups.append((person_id, intent_id))
        return self.receipts.get((person_id, intent_id))

    def submit(self, person_id, intent_id, body):
        if (person_id, intent_id) not in self.receipts:
            self.calls.append((person_id, intent_id, body))
            self.receipts[person_id, intent_id] = {
                'world_id': self.world_id, 'person_id': person_id,
                'intent_id': intent_id, 'status': 'committed',
                'ruling': '成功', 'time': 0, 'effect': body}
        if self.fail_after_commit:
            self.fail_after_commit = False
            raise RuntimeError('fake lost acknowledgement after durable world commit')
        return self.receipts[person_id, intent_id]


class ActorTests(unittest.TestCase):
    def setUp(self):
        scratch = Path(os.environ['LOCALAPPDATA']) / 'hermes/cache/scratch'
        scratch.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=scratch)
        self.world = FakeWorld()
        self.db = Path(self.tmp.name) / 'actors.sqlite'
        self.host = ActorHost(self.world, self.db)

    def tearDown(self):
        self.host.close()
        self.tmp.cleanup()

    def test_lazy_start_real_pi_wait_long_lived_and_clean_exit(self):
        self.assertIsNone(self.host.worker_pid)
        first = self.host.act('king')
        pid = self.host.worker_pid
        second = self.host.act('general')
        self.assertEqual(pid, self.host.worker_pid)
        self.assertEqual(first['provider'], 'scripted-pi')
        self.assertEqual(first['body'], {'action': 'wait'})
        self.assertGreater(first['sdk_events']['tool_execution_start'], 0)
        self.assertEqual(first['sdk_requests'], 2)
        self.assertEqual(len(self.world.calls), 2)
        self.assertEqual(second['body'], {'action': 'wait'})
        self.host.close()
        self.assertEqual(self.host.worker_exit_code, 0)


    def test_unknown_commit_reconciles_original_intent_without_second_execution(self):
        self.world.fail_after_commit = True
        with self.assertRaisesRegex(RuntimeError, 'lost acknowledgement'):
            self.host.act('king')
        original = self.world.calls[0][1]
        self.host.close()
        self.host = ActorHost(self.world, self.db)
        recovered = self.host.act('king')
        self.assertEqual(recovered['intent_id'], original)
        self.assertEqual(len(self.world.calls), 1)
        self.assertIn(('king', original), self.world.lookups)
        self.assertIsNone(self.host.worker_pid)
        self.assertTrue(recovered['reconciled'])
        with closing(sqlite3.connect(self.db)) as db:
            principal, intent = db.execute('SELECT person_id,intent_id FROM actor_calls').fetchone()
        self.assertEqual((principal, intent), ('king', original))

    def test_independent_durable_sessions_and_principal_binding(self):
        self.world.views['king'] = self.world.observe('king')
        self.world.views['king']['observations'] = [
            {'id':'private-1','time':0,'source':'亲见','text':'私记：ONLY_KING_KNOWS'}]
        self.host.act('king')
        self.host.act('general')
        with closing(sqlite3.connect(self.db)) as db:
            sessions = dict(db.execute('SELECT person_id,messages FROM actor_sessions'))
        self.assertIn('ONLY_KING_KNOWS', sessions['king'])
        self.assertNotIn('ONLY_KING_KNOWS', sessions['general'])
        self.world.views['intruder'] = self.world.observe('king')
        with self.assertRaisesRegex(RuntimeError, 'PRINCIPAL_MISMATCH'):
            self.host.act('intruder')


    def test_local_symptoms_report_once_and_monarch_waits_without_mail(self):
        self.world.views['marshal'] = {
            'person': {'id':'marshal','name':'武官','office':'marshal','location':'边城','card':{}},
            'time':100,'observations':[{'id':'fever','time':100,'source':'亲见：边城','text':'当地有人咳嗽发热。'}],
            'messages':[], 'contacts':[{'id':'king','name':'君主','office':'king','country_id':'a'}]}
        report = self.host.act('marshal')
        self.assertEqual(report['body']['action'], 'send')
        self.assertEqual(report['body']['to'], 'king')
        self.assertIn('边城', report['body']['text'])
        self.assertEqual(self.host.act('marshal')['body'], {'action':'wait'})
        self.assertEqual(self.host.act('king')['body'], {'action':'wait'})


    def test_monarch_commands_only_from_delivered_report(self):
        view = self.world.observe('king')
        view['person']['office'] = 'king'
        view['contacts'] = [{'id':'finance','name':'财政官','office':'finance','country_id':'a'}]
        self.world.views['king'] = view
        self.assertEqual(self.host.act('king')['body'], {'action':'wait'})
        view['messages'] = [{'id':'report-1','time':0,'from':'marshal',
                             'text':'边城有人咳嗽发热，请核账赈粮。目的地：place-1'}]
        command = self.host.act('king')['body']
        self.assertEqual(command['action'], 'command')
        self.assertEqual(command['to'], 'finance')
        self.assertIn('place-1', command['text'])
        self.assertEqual(self.host.act('king')['body'], {'action':'wait'})


    def test_finance_replies_independently_then_checks_ledger_before_shipping(self):
        view = self.world.observe('finance')
        view['person']['office'] = 'finance'
        self.world.views['finance'] = view
        self.assertEqual(self.host.act('finance')['body'], {'action':'wait'})
        view['messages'] = [{'id':'letter','command_id':'order-1','time':0,'from':'king',
                             'text':'请核账赈粮。目的地：place-1'}]
        self.assertEqual(self.host.act('finance')['body'],
                         {'action':'respond','command_id':'order-1','accept':True})
        self.assertEqual(self.host.act('finance')['body'], {'action':'investigate'})
        self.assertEqual(self.host.act('finance')['body'], {'action':'wait'})
        view['observations'] = [{'id':'ledger-1','time':10,'source':'本人核账','text':'核账：本地可用粮食100份。'}]
        self.assertEqual(self.host.act('finance')['body'], {'action':'ship','to':'place-1','quantity':10})
        self.assertEqual(self.host.act('finance')['body'], {'action':'wait'})
        view['messages'].append({'id':'letter-2','command_id':'order-2','time':20,'from':'king',
                                 'text':'再赈粮。目的地：place-1'})
        view['person']['card'] = {'values':'拒绝服从此类征粮命令'}
        self.assertEqual(self.host.act('finance')['body'],
                         {'action':'respond','command_id':'order-2','accept':False})


    def test_crash_after_sdk_before_candidate_replays_original_id_with_new_call_id(self):
        original_request = self.host._request
        lost = {}

        def lose_result(payload):
            with closing(sqlite3.connect(self.db)) as db:
                lost['intent'] = db.execute("SELECT intent_id FROM actor_intents WHERE state='prepared'").fetchone()[0]
            result = original_request(payload)
            lost['call'] = result['calls'][0]['call_id']
            raise RuntimeError('simulated crash before candidate checkpoint')

        self.host._request = lose_result
        with self.assertRaisesRegex(RuntimeError, 'simulated crash'):
            self.host.act('king')
        self.assertEqual(self.world.calls, [])
        self.host._process.kill()
        self.host._process.wait(timeout=5)
        self.assertIsNotNone(self.host._process.returncode)
        self.host.close()
        self.host = ActorHost(self.world, self.db)
        result = self.host.act('king')
        self.assertEqual(result['intent_id'], lost['intent'])
        self.assertIn(('king', lost['intent']), self.world.lookups)
        with closing(sqlite3.connect(self.db)) as db:
            call, intent = db.execute('SELECT call_id,intent_id FROM actor_calls').fetchone()
        self.assertNotEqual(call, lost['call'])
        self.assertEqual(intent, lost['intent'])
        self.assertEqual(len(self.world.calls), 1)


    def test_standalone_real_node_sdk_blocks_network_and_exits(self):
        node = os.environ.get('PI_NODE_EXE') or __import__('shutil').which('node')
        request = {'id':'probe','op':'network_probe'}
        env = {k:v for k,v in os.environ.items() if k.upper() in {'PATH','SYSTEMROOT','WINDIR','PI_MODULE_ROOT'}}
        process = subprocess.run([node, str(Path(__file__).parents[1] / 'pi_worker.mjs')],
                                 input=json.dumps(request)+'\n', text=True, encoding='utf-8',
                                 capture_output=True, env=env, timeout=30)
        self.assertEqual(process.returncode, 0, process.stderr)
        response = json.loads(process.stdout)
        self.assertTrue(response['ok'])
        self.assertEqual(response['result']['network_blocked'],
                         ['fetch','http','https','socket','tls','dns','udp'])
        print('REAL_NODE_NETWORK_PROBE', process.stdout.strip(), 'NODE_EXIT_CODE=', process.returncode)


    def test_natural_identity_projection_excludes_backend_pathology(self):
        view = self.world.observe('king')
        view['person']['name'] = '昭王'
        view['person']['office'] = 'king'
        view['person']['card'] = {'mission':'保全民生'}
        view['pathology'] = {'latent_seconds':'FORBIDDEN_BACKEND_MARKER'}
        view['countries'] = ['FORBIDDEN_BACKEND_MARKER']
        self.world.views['king'] = view
        self.host.act('king')
        with closing(sqlite3.connect(self.db)) as db:
            request = json.loads(db.execute('SELECT request FROM actor_intents').fetchone()[0])
        self.assertIn('君主', request['system_prompt'])
        self.assertIn('昭王', request['system_prompt'])
        self.assertIn('保全民生', request['system_prompt'])
        self.assertNotIn('king', request['system_prompt'])
        self.assertNotIn('AI', request['system_prompt'])
        self.assertNotIn('FORBIDDEN_BACKEND_MARKER', json.dumps(request))


if __name__ == '__main__':
    unittest.main()
