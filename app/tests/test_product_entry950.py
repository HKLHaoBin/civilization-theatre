"""Additional new acceptance source; NOT executed in source-only scope.
No protected historical test method or historical fixture is invoked.
"""
import json
from pathlib import Path
import unittest
import uuid
from app import product_cli as cli


class ProductEntryTests(unittest.TestCase):
    def owned(self):
        # Do not use TemporaryDirectory: preserve failed owned worlds.
        return cli.ROOT / ('acceptance-owned-' + uuid.uuid4().hex)

    def test_g1_actual_generation_initialize_readback_and_reopen(self):
        root = self.owned()
        result = cli.initialize_owned(root, cli.SCENARIOS / 'iron-4.json')
        self.assertEqual(result['time'], 0)
        self.assertEqual(len(result['countries']), 4)
        self.assertEqual(len(result['persons']), 12)
        self.assertEqual(cli.read_owned(root), result)
        self.assertEqual(cli.read_owned(root, seq=1), result)
        self.assertEqual(len(cli.source_identity()), 9)
        with self.assertRaises(FileExistsError):
            cli.initialize_owned(root, cli.SCENARIOS / 'iron-4.json')
        self.assertEqual(cli.read_owned(root), result)

    def test_g3_registered_production_due_notice_and_recovery(self):
        root = self.owned()
        config = json.loads((cli.SCENARIOS / 'iron-4.json').read_text(encoding='utf-8'))
        config.update(schema_version=3, scenario_id='parent950-component', version='parent950-component')
        del config['harvest_day']
        del config['harvest_quantity']
        places = [p for country in config['countries'] for p in country['places']]
        config['economy'] = dict(
            version='p08-production-economy-v1', assumptions='Fictional game rates, not historical calibration.',
            units=dict(grain='grain_unit', iron='iron_unit', charcoal='charcoal_unit',
                       timber='timber_unit', iron_tool='tool_unit'),
            initial_materials={p['id']: dict(iron=20, charcoal=10, timber=20, iron_tool=2) for p in places},
            labor_hours_per_day={p['id']: 8 for p in places},
            recipes={'forge_tool': dict(inputs=dict(iron=2, charcoal=1, timber=1),
                                       outputs=dict(iron_tool=1), labor_hours=8,
                                       duration_seconds=86401)})
        # Own scenario is a new ordinary file, never a historical database.
        scenario = cli.ROOT / ('acceptance-scenario-' + uuid.uuid4().hex + '.json')
        scenario.write_text(json.dumps(config, ensure_ascii=False), encoding='utf-8')
        cli.initialize_owned(root, scenario)
        pid = 'person-country-1-finance'
        views = []
        proposed = False
        with cli.open_session(root, 'component-scripted') as session:
            from app.actors import ActorHost
            from app.scripted_dm import DefaultActors
            from app.source_attestation import SourceMismatch
            self.assertIs(type(session.host), ActorHost)
            self.assertIs(session.host._choose, DefaultActors._choose)
            self.assertEqual(cli.source_identity(session.host), cli.source_identity())
            with self.assertRaises(SourceMismatch):
                cli.source_identity(DefaultActors)
            def choose(view, seen):
                nonlocal proposed
                views.append(json.loads(json.dumps(view)))
                if view['person']['id'] == pid and not proposed:
                    proposed = True
                    return dict(action='propose', text='组织真实工班制造农具。'), seen
                return dict(action='wait'), seen
            def dm(context):
                state = context['world']
                return dict(world_id=state['world_id'], base_revision=state['revision'],
                            ruling='成功', reason='component scripted only', goal='制造',
                            costs=[], benefits=[], effects=[dict(action='start_production',
                                                               recipe_id='forge_tool', quantity=1)])
            session.host._choose = choose
            session.host.dm_provider = dm
            first = session.step(60)
            task = first['economy']['tasks'][0]
            self.assertEqual((first['time'], task['due'], task['status']), (86400, 172801, 'pending'))
            session.step(60)
            third = session.step(60)
            self.assertEqual(third['time'], 259200)
            self.assertEqual(third['economy']['tasks'][0]['status'], 'completed')
            self.assertEqual(third['economy']['tasks'][0]['actual_outputs'], {'iron_tool': 1})
            self.assertEqual(third['economy']['materials']['place-0']['iron_tool'], 3)
            self.assertEqual(len([e for e in third['events'] if e['kind'] == 'production_completed']), 1)
            owner_views = [v for v in views if v['person']['id'] == pid]
            notice = [o for o in owner_views[-1]['observations'] if o['time'] == task['due'] and 'iron_tool' in o['text']]
            self.assertEqual(len(notice), 1)
            for view in views:
                if view['person']['id'] != pid:
                    self.assertFalse(any(o['time'] == task['due'] and 'iron_tool' in o['text']
                                         for o in view['observations']))
            from app.world import WorldError
            with self.assertRaises(WorldError) as failure:
                session.world.advance(third['time'] + 1)
            self.assertEqual(failure.exception.code, 'COORDINATOR_EXECUTION_REQUIRED')
            tables = session.ledger()
            self.assertEqual(len(tables['coordinator_scheduler_registration']), 1)
            self.assertEqual(len(tables['coordinator_windows']), 3)
            self.assertTrue(all(a['phase'] == 'completed' and a['private_closed'] == 1
                                for a in tables['coordinator_activations']))
            self.assertEqual(len(tables['coordinator_activations']), 36)
            before = session.world.snapshot()
            historical = session.world.snapshot(1)
            self.assertEqual(historical['time'], 0)
            self.assertEqual(session.world.snapshot(), before)
        self.assertEqual(cli.read_owned(root), third)
        with cli.open_session(root, 'component-scripted') as resumed:
            self.assertEqual(resumed.world.snapshot(), third)
            fourth = resumed.step(60)
            self.assertEqual(fourth['time'], 345600)
            self.assertEqual(len(fourth['economy']['tasks']), 1)
            self.assertEqual(len([e for e in fourth['events'] if e['kind'] == 'production_completed']), 1)
        self.assertEqual(cli.read_owned(root), fourth)

    def test_unowned_path_rejected_without_database_creation(self):
        path = self.owned()
        with self.assertRaises(ValueError):
            cli.read_owned(path)
        self.assertFalse(path.exists())

    def test_explicit_provider_required(self):
        with self.assertRaises(ValueError):
            cli.open_session(self.owned(), 'real-model-without-account')


if __name__ == '__main__':
    unittest.main(verbosity=2)
