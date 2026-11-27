"""HTTP routing doubles prove revision binding, not world rule semantics."""
import http.client
import json
import threading
import unittest
from types import SimpleNamespace

from app.server import make_server
from app.tests.test_server import WorldDouble, ActorsDouble


class TraceHTTPTests(unittest.TestCase):
    def setUp(self):
        self.world = WorldDouble()
        self.bound = []
        def observe(person, seq=None):
            self.bound.append(('person', person, seq))
            return {'person': {'id': person}, 'revision': seq or self.world.state['revision'],
                    'observations': [], 'messages': [], 'contacts': []}
        self.world.observe = observe
        def trace(seq=None):
            self.bound.append(('trace', seq))
            return {'world_id': 'test', 'revision': seq, 'proposals': []}
        self.actors = ActorsDouble()
        self.actors.adjudicator = SimpleNamespace(world=self.world, trace=trace)
        self.server = make_server(self.world, self.actors, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(5)

    def get(self, path):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_address[1], timeout=5)
        try:
            connection.request('GET', path)
            response = connection.getresponse()
            return response.status, json.loads(response.read())
        finally:
            connection.close()

    def test_state_attaches_causality_for_exact_displayed_revision(self):
        status, data = self.get('/api/state?seq=1')
        self.assertEqual(status, 200)
        self.assertIn('causality', data)
        self.assertEqual(data['causality']['revision'], data['revision'])
        self.assertEqual(self.bound, [('trace', 1)])


    def test_person_history_is_revision_bound_and_malformed_sequences_are_rejected(self):
        status, data = self.get('/api/person?id=person&seq=1')
        self.assertEqual(status, 200)
        self.assertEqual(data['revision'], 1)
        self.assertEqual(self.bound, [('person', 'person', 1)])
        for query in ['0', '-1', '1.0', 'true', '1&seq=2', '']:
            with self.subTest(query=query):
                status, _ = self.get('/api/person?id=person&seq='+query)
                self.assertEqual(status, 400)
        status, _ = self.get('/api/person?id=person&extra=1')
        self.assertEqual(status, 400)


    def test_meta_labels_scripted_dm_and_readonly_causal_capabilities(self):
        status, data = self.get('/api/meta')
        self.assertEqual(status, 200)
        self.assertEqual(data.get('dm_mode'), 'scripted-dm')
        self.assertIn('causal-trace', data['capabilities'])
        self.assertIn('history-person', data['capabilities'])


    def test_historical_state_does_not_include_current_runtime_diagnostic(self):
        self.server.coordinator.error = 'FUTURE_DIAGNOSTIC'
        self.server.coordinator.stop.set()
        self.server.coordinator.busy = True
        status, data = self.get('/api/state?seq=1')
        self.assertEqual(status, 200)
        self.assertIsNone(data['runtime']['error'])
        self.assertFalse(data['runtime']['busy'])
        self.assertFalse(data['runtime']['pause_requested'])
        self.assertTrue(data['runtime']['historical'])
        self.assertNotIn('FUTURE_DIAGNOSTIC', json.dumps(data))
        status, latest = self.get('/api/state')
        self.assertEqual(latest['runtime']['error'], 'FUTURE_DIAGNOSTIC')


if __name__ == '__main__':
    unittest.main()
