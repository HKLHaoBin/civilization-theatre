"""P02.1 first vertical authority behavior; real SQLite, no model."""
from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import unittest
from app.world import WorldStore, WorldError

SCRATCH = 'C:/Users/We3q/AppData/Local/hermes/cache/scratch'


class ExecutionContractTests(unittest.TestCase):
    def test_first_authority_claim_blocks_second_host_without_world_facts(self):
        with tempfile.TemporaryDirectory(dir=SCRATCH) as temp:
            path = Path(temp) / 'world.sqlite'
            first = WorldStore(path)
            state = first.initialize()
            with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                snapshots = db.execute('SELECT seq,state FROM snapshots ORDER BY seq').fetchall()
            self.assertTrue(callable(getattr(first, 'acquire_actor_execution', None)),
                            'World-backed actor execution acquisition is missing')
            permit = first.acquire_actor_execution('person-country-1-finance', 'owner-first', ttl_seconds=60)
            self.assertEqual(permit.world_id, state['world_id'])
            self.assertEqual(permit.person_id, 'person-country-1-finance')
            self.assertEqual(type(permit.epoch), int)
            self.assertEqual(permit.epoch, 1)
            self.assertTrue(permit.owner_token)
            second = WorldStore(path)
            second.initialize()
            with self.assertRaises(WorldError) as error:
                second.acquire_actor_execution('person-country-1-finance', 'owner-second', ttl_seconds=60)
            self.assertEqual(error.exception.code, 'ACTOR_EXECUTION_BUSY')
            self.assertEqual(first.snapshot(), state)
            with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                self.assertEqual(db.execute('SELECT seq,state FROM snapshots ORDER BY seq').fetchall(), snapshots)
            for hidden in ('owner_token', 'actor_execution_leases', 'execution_clock'):
                self.assertNotIn(hidden, str(first.observe('person-country-1-finance')))


if __name__ == '__main__':
    unittest.main()
