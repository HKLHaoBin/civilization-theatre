"""R1 corrupt-ledger fault injection, not normal API or arbitrary SQL defense.

Terminal seeds are ledger fixtures only: no Host phase/receipt is invented.
"""
from contextlib import closing
from pathlib import Path
import sqlite3
import unittest

from app.tests import test_coordinator_windows as original


class LedgerRecoveryTests(unittest.TestCase):
    fixture = original.CoordinatorWindowTests.fixture
    assertCode = original.CoordinatorWindowTests.assertCode

    def inject(self, world, *statements):
        with closing(sqlite3.connect(world.db_path)) as db:
            for sql, parameters in statements:
                db.execute(sql, parameters)
            db.commit()

    def assertRejectedUnchanged(self, world, host, owner, window):
        before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
        snapshot = world.snapshot()
        version = {k:snapshot[k] for k in ('world_id', 'revision')}
        self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window,
                        coordinator=owner, expected_version=version)
        self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.next_activation,
                        window['window_id'], coordinator=owner)
        self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.complete_window,
                        window['window_id'], coordinator=owner)
        self.assertEqual(world.snapshot(), snapshot)
        self.assertEqual({p:p.read_bytes() for p in before}, before)

    def terminal_seed(self, world, host, owner, window):
        # Real Host/private promotion; no hand-written receipt or done bits.
        from app.tests.activation_fixtures import close_activations
        results = close_activations(world, host, owner, window)
        self.assertEqual(len(results), len(window['roster']))
        self.assertEqual(len({r['intent_id'] for r in results}), len(results))
        with closing(sqlite3.connect(host.db_path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM actor_session_heads WHERE version=1').fetchone()[0], len(results))
        return results

    def test_completed_boundary_rejects_pending_unclosed_and_bad_revision(self):
        for corruption in ('pending', 'unclosed', 'missing_revision', 'future_revision', 'wrong_snapshot'):
            with self.subTest(corruption=corruption), self.fixture() as (world, host, owner, version, root):
                window = world.open_or_recover_window(coordinator=owner, expected_version=version)
                if corruption != 'pending':
                    self.terminal_seed(world, host, owner, window)
                self.inject(world, ("UPDATE coordinator_windows SET state='completed',completed_revision=advanced_revision", ()))
                if corruption == 'unclosed':
                    self.inject(world, ('UPDATE coordinator_activations SET private_closed=0 WHERE roster_index=0', ()))
                elif corruption == 'missing_revision':
                    self.inject(world, ('UPDATE coordinator_windows SET completed_revision=NULL', ()))
                elif corruption == 'future_revision':
                    self.inject(world, ('UPDATE coordinator_windows SET completed_revision=advanced_revision+1', ()))
                elif corruption == 'wrong_snapshot':
                    self.inject(world, ('UPDATE coordinator_windows SET completed_revision=base_revision', ()))
                self.assertRejectedUnchanged(world, host, owner, window)

    def test_open_completion_revision_is_contradictory(self):
        with self.fixture() as (world, host, owner, version, root):
            window = world.open_or_recover_window(coordinator=owner, expected_version=version)
            self.inject(world, ('UPDATE coordinator_windows SET completed_revision=advanced_revision', ()))
            self.assertRejectedUnchanged(world, host, owner, window)

    def test_prior_completed_history_is_validated_before_recovering_later_open(self):
        with self.fixture() as (world, host, owner, version, root):
            first = world.open_or_recover_window(coordinator=owner, expected_version=version)
            results = self.terminal_seed(world, host, owner, first)
            world.complete_window(first['window_id'], coordinator=owner)
            current = {k:world.snapshot()[k] for k in ('world_id', 'revision')}
            second = world.open_or_recover_window(coordinator=owner, expected_version=current)
            self.assertEqual(second['ordinal'], 2)
            self.inject(world, ("UPDATE coordinator_activations SET phase='pending',private_closed=0 WHERE window_id=? AND roster_index=0", (first['window_id'],)))
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.open_or_recover_window,
                            coordinator=owner, expected_version=current)
            self.assertEqual({p:p.read_bytes() for p in before}, before)
            self.assertCode('WINDOW_RECOVERY_UNPROVEN', world.next_activation, first['window_id'], coordinator=owner)

    def test_strict_persisted_ids_and_integer_metadata_at_recovery_and_dispatch(self):
        corruptions = [
            ("UPDATE coordinator_activations SET activation_id=? WHERE roster_index=0", value)
            for value in ('', 'not-a-uuid', 'f'*31, 'g'*32)
        ]
        corruptions += [
            (f'UPDATE coordinator_activations SET {column}=? WHERE roster_index=0', value)
            for column, value in (
                ('created_parent_epoch', 1.5), ('owner_parent_epoch', 1.5),
                ('roster_index', 0.5), ('observation_revision', 1.5),
                ('observation_revision', -1), ('card_version', 0),
                ('card_version', 1.5), ('base_session_version', -1),
                ('base_session_version', 0.5), ('promoted_version', 0),
                ('promoted_version', 1.5), ('owner_actor_epoch', 0),
                ('owner_actor_epoch', 1.5), ('owner_actor_epoch', float(2**63)),
                ('created_parent_epoch', 2),
            )
        ]
        corruptions += [
            (f'UPDATE coordinator_windows SET {column}=?', value)
            for column, value in (
                ('created_parent_epoch', 1.5), ('owner_parent_epoch', 1.5),
                ('created_parent_epoch', 2), ('ordinal', 1.5),
                ('base_revision', 1.5), ('completed_revision', 1.5),
            )
        ]
        for sql, value in corruptions:
            with self.subTest(sql=sql, value=value), self.fixture() as (world, host, owner, version, root):
                window = world.open_or_recover_window(coordinator=owner, expected_version=version)
                self.inject(world, (sql, (value,)))
                self.assertRejectedUnchanged(world, host, owner, window)

    def test_bad_window_uuid_even_with_matching_activation_links_is_rejected(self):
        for value in ('', 'bad-window-id'):
            with self.subTest(value=value), self.fixture() as (world, host, owner, version, root):
                window = world.open_or_recover_window(coordinator=owner, expected_version=version)
                self.inject(world, ('UPDATE coordinator_windows SET window_id=?', (value,)),
                            ('UPDATE coordinator_activations SET window_id=?', (value,)))
                self.assertRejectedUnchanged(world, host, owner, dict(window, window_id=value))

    def test_window_reference_inputs_reject_nonstring_shapes_without_sql_binding_errors(self):
        with self.fixture() as (world, host, owner, version, root):
            world.open_or_recover_window(coordinator=owner, expected_version=version)
            before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
            for value in (True, 1, 1.5, None, [], {}, '', 'not-a-uuid'):
                for operation in (world.next_activation, world.complete_window):
                    self.assertCode('WINDOW_RECOVERY_UNPROVEN', operation, value, coordinator=owner)
            self.assertEqual({p:p.read_bytes() for p in before}, before)

    def replace_index(self, world, predicate):
        # Only fixed fixture statements; never caller SQL or an evaluator.
        self.inject(world, ('DROP INDEX coordinator_one_open_window', ()),
                    ('CREATE UNIQUE INDEX coordinator_one_open_window ON coordinator_windows(world_id) WHERE '+predicate, ()))

    def test_wrong_one_open_index_predicate_fails_closed_without_repair(self):
        for predicate in ("state='completed'", "state IN ('open','completed')", "state='open' OR state='completed'", "state!='open'"):
            with self.subTest(predicate=predicate), self.fixture() as (world, host, owner, version, root):
                window = world.open_or_recover_window(coordinator=owner, expected_version=version)
                self.replace_index(world, predicate)
                self.assertRejectedUnchanged(world, host, owner, window)

    def test_equivalent_one_open_index_sql_formatting_is_accepted_readonly(self):
        for predicate in (''' ( "state" = 'open' ) ''',
                          "((state) = ('open'))", "[state] == 'open'",
                          "'open' = `state`", "(((STATE = 'open'))) ",
                          "/* fixture formatting */ (state\n = 'open')"):
            with self.subTest(predicate=predicate), self.fixture() as (world, host, owner, version, root):
                window = world.open_or_recover_window(coordinator=owner, expected_version=version)
                self.replace_index(world, predicate)
                before = {p:p.read_bytes() for p in (world.db_path, Path(host.db_path))}
                self.assertEqual(world.open_or_recover_window(coordinator=owner, expected_version=version)['window_id'], window['window_id'])
                self.assertEqual(world.next_activation(window['window_id'], coordinator=owner)['phase'], 'pending')
                self.assertCode('WINDOW_INCOMPLETE', world.complete_window, window['window_id'], coordinator=owner)
                self.assertEqual({p:p.read_bytes() for p in before}, before)

    def test_terminal_fixture_completion_can_open_successor_without_fake_receipts(self):
        with self.fixture() as (world, host, owner, version, root):
            first = world.open_or_recover_window(coordinator=owner, expected_version=version)
            results = self.terminal_seed(world, host, owner, first)
            world.complete_window(first['window_id'], coordinator=owner)
            current = {k:world.snapshot()[k] for k in ('world_id', 'revision')}
            second = world.open_or_recover_window(coordinator=owner, expected_version=current)
            self.assertEqual(second['ordinal'], 2)
            self.assertEqual(second['target_time'], 172800)
            with closing(sqlite3.connect(world.db_path.as_uri()+'?mode=ro', uri=True)) as db:
                rows = db.execute('SELECT a.intent_id,i.person,i.receipt FROM coordinator_activations a JOIN intents i ON i.id=a.intent_id WHERE a.window_id=?',(first['window_id'],)).fetchall()
                self.assertEqual({r[0] for r in rows}, {r['intent_id'] for r in results})
                self.assertEqual({r[1] for r in rows}, set(first['roster']))
                self.assertTrue(all(r[2] for r in rows))
                self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0],len(first['roster']))
