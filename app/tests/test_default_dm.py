"""P06.3 default real-Pi wiring; no real model. HTTP doubles are explicit."""
from contextlib import closing
import importlib
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from app.world import WorldStore


class DefaultDMTests(unittest.TestCase):
    def test_default_factory_runs_natural_proposal_through_real_pi_and_dm(self):
        server = importlib.import_module('app.server')
        self.assertTrue(callable(getattr(server, 'create_default_actors', None)), '默认运行尚未接入新链路')
        scratch = Path(os.environ['LOCALAPPDATA'])/'hermes/cache/scratch'
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            world = WorldStore(root/'world.sqlite')
            world.initialize()
            world.control('resume')
            host = server.create_default_actors(world, root/'actors.sqlite')
            try:
                result = host.act('person-country-1-king')
                self.assertEqual(result['provider'], 'scripted-pi')
                self.assertEqual(result['body']['action'], 'propose')
                self.assertEqual(result['receipt']['decision_version'], 1)
                self.assertIs(host.adjudicator.world, world)
                with closing(sqlite3.connect(root/'world.sqlite')) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM dm_proposals').fetchone()[0], 1)
                    self.assertEqual(db.execute('SELECT count(*) FROM dm_decisions').fetchone()[0], 1)
            finally:
                host.close()
            self.assertEqual(host.worker_exit_code, 0)


    def test_default_policy_keeps_send_command_response_and_investigation_as_proposals(self):
        from app.scripted_dm import DefaultActors, ScriptedDM
        from app.actors import ActorHost
        contacts = [{'id': 'king', 'name': '清河君主', 'office': 'king'},
                    {'id': 'finance', 'name': '清河财政官', 'office': 'finance'}]
        cases = [
            ({'person': {'id':'marshal','office':'marshal','name':'清河军事官','location':'松川','card':{}},
              'observations':[{'id':'symptom','text':'有人咳嗽发热'}], 'messages':[], 'contacts':contacts}, []),
            ({'person': {'id':'king','office':'king','name':'清河君主','location':'都城','card':{}},
              'observations':[], 'messages':[{'id':'report','from':'finance','text':'已亲见症状'}], 'contacts':contacts}, []),
            ({'person': {'id':'finance','office':'finance','name':'清河财政官','location':'都城','card':{}},
              'observations':[], 'messages':[{'id':'order','from':'king','command_id':'command-order','text':'请核账'}], 'contacts':contacts}, []),
            ({'person': {'id':'finance','office':'finance','name':'清河财政官','location':'都城','card':{}},
              'observations':[], 'messages':[{'id':'order','from':'king','command_id':'command-order','text':'请核账'}], 'contacts':contacts}, ['respond:order']),
        ]
        for view, seen in cases:
            with self.subTest(office=view['person']['office'], seen=seen):
                expected, _ = ActorHost._choose(view, seen)
                body, consumed = DefaultActors._choose(view, seen)
                self.assertEqual(body['action'], 'propose')
                class KnownWorld:
                    def observe(self, person):
                        return view
                decision = ScriptedDM(KnownWorld())({'proposal': {'person_id':view['person']['id'],'text':body['text']},
                    'world': {'world_id':'fixture','revision':1}})
                self.assertEqual(decision['effects'], [expected])
                self.assertEqual(consumed, ActorHost._choose(view, seen)[1])
                self.assertNotIn('command-order', body['text'])


    def test_finance_compound_uses_only_received_place_name_and_measured_ledger(self):
        from app.scripted_dm import DefaultActors, ScriptedDM
        view = {'person': {'id':'finance','office':'finance','name':'财政官','country_id':'country-1','location':'都城','card':{}},
            'observations':[{'id':'ledger','text':'核账：此刻本地粮100份；数字来自本地账簿清点。'}],
            'messages':[{'id':'order','from':'king','command_id':'order-id','text':'请核账赈济；原报告：我在松川亲见：有人咳嗽。'}],
            'contacts':[{'id':'king','name':'清河君主','office':'king'}]}
        seen = ['respond:order','ledger:order']
        body, consumed = DefaultActors._choose(view, seen)
        self.assertIn('调运10份', body['text'])
        self.assertIn('message:order', consumed)
        class KnownWorld:
            def observe(self, person):
                return view
        context = {'proposal': {'person_id':'finance','text':body['text']}, 'world':{'world_id':'fixture','revision':1,
            'places':[{'id':'place-1','name':'松川','country_id':'country-1'}]}}
        effects = ScriptedDM(KnownWorld())(context)['effects']
        self.assertEqual(effects[0], {'action':'ship','to':'place-1','quantity':10})
        self.assertEqual(effects[1]['action'], 'send')
        self.assertEqual(effects[1]['to'], 'king')
        self.assertNotIn('place-1', body['text'])
        view['observations'] = []
        self.assertNotIn('调运10份', DefaultActors._choose(view, seen)[0]['text'])


    def test_legacy_destination_reference_also_uses_new_compound_chain(self):
        from app.scripted_dm import DefaultActors, ScriptedDM
        view = {'person': {'id':'finance','office':'finance','country_id':'country-1','location':'都城','card':{}},
            'observations':[{'id':'ledger','text':'核账：本地粮100份'}],
            'messages':[{'id':'order','from':'king','command_id':'command-old','text':'请调粮，目的地：place-1'}],
            'contacts':[{'id':'king','name':'清河君主','office':'king'}]}
        body, consumed = DefaultActors._choose(view, ['respond:order','ledger:order'])
        self.assertEqual(body['action'], 'propose')
        self.assertNotIn('place-1', body['text'])
        class KnownWorld:
            def observe(self, person): return view
        data = ScriptedDM(KnownWorld())({'proposal':{'person_id':'finance','text':body['text']},
            'world':{'world_id':'fixture','revision':1,'places':[{'id':'place-1','name':'松川','country_id':'country-1'}]}})
        self.assertEqual([item['action'] for item in data['effects']], ['ship','send'])
        self.assertEqual(data['effects'][0]['quantity'], 10)


if __name__ == '__main__':
    unittest.main()
