"""Pending append-only test module: app/tests/test_lifecycle_rollback1152.py.

Unexecuted. Mount only after review on the exact source generation being tested.
The only transport is Authority1100's existing synthetic transport. The rollback
and close faults below are Python injections, NOT SQLite-engine errors.
"""
import ast
from contextvars import Context
import sqlite3
import threading
import time
from types import SimpleNamespace
import unittest

from app import actors, coordinator_scheduler as sc, server, world as wm
from app.actor_execution import LifecycleCallBudget, PreparationBudget
from app.tests import test_lifecycle_authority1100 as authority
from app.tests import test_lifecycle_source_drift1103 as drift


# Keep the exact owning fixture reachable if cleanup cannot be certified.
_STOPPED_1157 = None


def _tuple(error):
    return (error, BaseException.__traceback__.__get__(error),
            BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))


class IssuedRollbackDelivery1152(unittest.TestCase):
    _geo1145_assert_first_resources = drift.LifecycleSourceDriftTests._geo1145_assert_first_resources

    def run(self, result=None):
        try:
            return super().run(result)
        finally:
            if result is not None and _STOPPED_1157 is not None:
                result.stop()  # All later unittest cases, not merely this class.

    def test_issued_commit_rollback_A_live_check(self):
        self._run('sqlite-shaped', False)

    def test_issued_commit_rollback_ABC_then_real_root_cancel(self):
        self._run('sqlite-shaped', True)

    def test_issued_commit_rollback_WorldError_then_real_root_cancel(self):
        self._run('world', True)

    def _run(self, kind, stacked):
        global _STOPPED_1157
        if _STOPPED_1157 is not None:
            self.fail('STOP: prior rollback fixture resources not proven quiescent; guards retained')
        whole = time.monotonic() + 120
        fixture = authority.LifecycleAuthorityProductTests(methodName='runTest')
        observed = self.rollback1152 = {'physical': [], 'checks': [], 'capture': []}
        a = (sqlite3.OperationalError('injected-before-native-rollback-A') if kind == 'sqlite-shaped'
             else wm.WorldError('ACTOR_RECOVERY_UNPROVEN', 'injected-before-native-rollback-A'))
        b = RuntimeError('injected-after-native-rollback-B')
        c = ValueError('injected-after-native-close-C')
        a_context, a_cause = LookupError('injected-A-context'), TypeError('injected-A-cause')
        original_cleanups = fixture.doCleanups

        def restore_after_native_close():
            if 'armed' in observed:
                db, _, _, _, entry, _, _ = observed['armed']
                self.assertIs(drift.LifecycleSourceDriftTests._geo1145_actual_closed(self, db, entry),
                              True, 'STOP: issued native connection closure unproven')
            return original_cleanups()

        fixture.doCleanups = restore_after_native_close

        def exercise():
            s = fixture.session
            host, world, root = s.host, s.world, fixture.original_root
            hb = fixture.source_binding(actors.ActorHost, 'resolve_lifecycle_jobs')
            mb = fixture.source_binding(wm.WorldStore, '_lc_geo1133_mutate_body')
            cb = fixture.source_binding(server.Coordinator, '_capture_failure')
            ib = fixture.source_binding(LifecycleCallBudget, 'check')
            prior = threading.gettrace()

            def unique(binding, predicate):
                hits = [n.lineno for n in ast.walk(binding['node']) if predicate(n)]
                self.assertEqual(len(hits), 1)
                return hits[0]

            host_line = unique(hb, lambda n: isinstance(n, ast.Assign) and
                               isinstance(n.value, ast.Call) and isinstance(n.value.func, ast.Attribute)
                               and n.value.func.attr == 'commit_lifecycle')
            rollback_line, _ = drift.LifecycleSourceDriftTests._geo1151_rollback_sites(self, mb)

            def edit(f, work):
                entry, outer = work.entry, f['parent']
                owner = entry._lifecycle_value_owner
                before = (owner.memory.live, len(owner.leases), len(owner.control_errors), entry.deadline, entry.busy_deadline)
                def in_original_root():
                    self.assertIsNone(sc._lifecycle_scope.get())
                    separate = PreparationBudget(entry.policy, entry.stop, (root, entry))
                    with world._coordinator_scope(work.permit, root), world._entry_budget(separate):
                        self.assertIsNot(sc._lifecycle_value_context.get(), owner)
                        world.edit_card(authority.KING, {'mission': '1152独立提交'}, coordinator=work.permit)
                        separate.check()
                Context().run(in_original_root)
                self.assertIs(sc._lifecycle_value_context.get(), owner)
                self.assertIs(world._scoped_entry_budget(), entry)
                self.assertIs(outer._lifecycle_value_owner, owner)
                self.assertEqual((owner.memory.live, len(owner.leases), len(owner.control_errors),
                                  entry.deadline, entry.busy_deadline), before)
                state = fixture.raw()
                job = next(j for j in state['_lifecycle']['jobs'] if j['id'] == work.job_id)
                card = next(p for p in state['persons'] if p['id'] == authority.KING)
                self.assertEqual(state['revision'], f['request']['base_revision'] + 1)
                self.assertEqual((card['card_author'], card['card']['mission']), ('user', '1152独立提交'))
                self.assertEqual((state['status'], job['result'], job['attempts'][-1]['status']), ('running', None, 'pending'))
                self.assertEqual(job['attempts'][-1]['request_id'], f['request']['request_id'])
                observed['after_edit'] = (fixture.business(), fixture.controls(), work, owner, f['io'], entry)

            rollback_here_code = []

            def arm(f, work):
                db = f['db']
                issued, owner, entry = f['result'], f['owner'], work.entry
                parent = entry._lifecycle_value_owner
                self.assertIs(type(db), wm._AttemptConnection)
                self.assertIs(db._attempt, entry)
                self.assertIs(db, work.db)
                self.assertIs(f['state'], work.state)
                self.assertIs(f['_precommit'], work)
                self.assertIs(type(issued), sc._lc_stale1141_Control)
                self.assertIs(issued, work.stale1141)
                self.assertEqual((issued.phase, issued.retry, issued.ready, issued.accepted), ('commit', 0, False, False))
                self.assertIsNone(issued.first)
                self.assertIs(sc._lifecycle_value_context.get(), owner)
                self.assertIsNot(owner, parent)
                self.assertIs(owner.memory, parent.memory)
                self.assertIs(db.rollback.__self__, db)
                self.assertIs(db.close.__self__, db)
                self.assertNotIn('rollback', vars(db))
                self.assertNotIn('close', vars(db))
                self.assertTrue(sqlite3.Connection.in_transaction.__get__(db))
                rollback, close = db.rollback, db.close
                observed['armed'] = (db, issued, owner, parent, entry, (entry.deadline, entry.busy_deadline), owner.memory.live)
                events = observed['physical']

                def rollback_here():
                    if not events:
                        events.append(('injected-A', sqlite3.Connection.in_transaction.__get__(db)))
                        try:
                            raise a_context
                        except LookupError:
                            raise a from a_cause
                    # Real original bound rollback, on the same transaction.
                    events.append(('before-native-rollback', sqlite3.Connection.in_transaction.__get__(db)))
                    try:
                        result = rollback()
                    except BaseException as error:
                        events.append(('native-rollback-raised', _tuple(error)))
                        raise
                    events.append(('native-rollback-returned', result, sqlite3.Connection.in_transaction.__get__(db)))
                    if stacked:
                        raise b

                def close_here():
                    # Restore only the exact instance attributes; no replacement
                    # connection and no global class, factory or method patch.
                    try:
                        result = close()
                    except BaseException as error:
                        events.append(('native-close-raised', _tuple(error)))
                        raise
                    finally:
                        del db.rollback, db.close
                    events.append(('native-close-returned', result))
                    if stacked:
                        root.stop.set()  # actual original root.check now rejects; no fake io.check or deadline
                        raise c

                rollback_here_code.append(rollback_here.__code__)
                db.rollback = rollback_here
                db.close = close_here

            def trace(frame, event, arg):
                binding = next((x for x in (hb, mb, cb, ib) if frame.f_code is x['code']), None)
                if binding is None:
                    return prior(frame, event, arg)
                try:
                    self.assertIs(frame.f_globals, binding['namespace'])
                    f = frame.f_locals
                    work = sc._lifecycle_scope.get()
                    if binding is hb and f.get('self') is host and event == 'line' and frame.f_lineno == host_line and 'after_edit' not in observed:
                        fixture.check_source_binding(hb)
                        self.assertIs(work.root, root)
                        edit(f, work)
                    elif binding is mb and f.get('self') is world and work is not None:
                        if event == 'line' and frame.f_lineno == rollback_line:
                            fixture.check_source_binding(mb)
                            self.assertNotIn('armed', observed)
                            self.assertIs(work, observed['after_edit'][2])
                            arm(f, work)
                        elif event == 'exception' and arg[1] is a:
                            self.assertNotIn('at_mutate', observed)
                            observed['at_mutate'] = (arg[1], arg[2], *_tuple(a)[2:])
                    elif binding is ib and 'armed' in observed and f.get('self') is observed['after_edit'][4]:
                        if event == 'exception':
                            observed['checks'].append((arg[1], arg[2], *_tuple(arg[1])[2:]))
                        if event == 'call':
                            observed.setdefault('late_check_calls', []).append((f['self'], f['self'].entry))
                    elif binding is cb and f.get('self') is s.coordinator and event == 'call':
                        observed['capture'].append((f['error'], f.get('rollback')))
                    return trace
                except BaseException as error:
                    if fixture.trace_error is None:
                        fixture.trace_error = error
                    raise

            fixture.addCleanup(threading.settrace, prior)
            threading.settrace(trace)
            fixture.start_join()
            for binding in (hb, mb, cb, ib):
                fixture.check_source_binding(binding)
            expected_code = 'HOST_FAILURE' if kind == 'sqlite-shaped' else 'ACTOR_RECOVERY_UNPROVEN'
            self.assertEqual(s.coordinator.error, expected_code)
            self.assertTrue(s.coordinator.stop.is_set())
            self.assertFalse(s.coordinator.busy)
            baseline, controls, work, parent, io, entry = observed['after_edit']
            db, issued, child, same_parent, same_entry, bounds, live = observed['armed']
            self.assertIs(parent, same_parent)
            self.assertIs(entry, same_entry)
            self.assertEqual(observed['physical'], [('injected-A', True), ('before-native-rollback', True),
                ('native-rollback-returned', None, False), ('native-close-returned', None)])
            with self.assertRaisesRegex(sqlite3.ProgrammingError, 'closed database'):
                sqlite3.Connection.in_transaction.__get__(db)
            self.assertIs(db._attempt, entry)
            self.assertEqual(len(observed['late_check_calls']), 1)
            self.assertIs(observed['late_check_calls'][0][0], io)
            self.assertIs(observed['late_check_calls'][0][1], entry)
            self.assertEqual((entry.deadline, entry.busy_deadline), bounds)
            self.assertTrue(child.failed and parent.failed and parent.memory.failed)
            self.assertEqual(parent.memory.live, live)  # no later debit or refund for this terminal lane
            self.assertFalse(issued.ready or issued.accepted)
            self.assertIsNone(issued.first)
            self.assertEqual(parent.control_errors, [])
            self.assertIsNone(host._lifecycle_active)
            self.assertIsNone(work.db)
            self.assertIsNone(work.state)
            self.assertIs(child.first_failure, parent.first_failure)
            self.assertIs(child.rollback_cleanup, parent.rollback_cleanup)
            for got, want in zip(parent.first_failure, observed['at_mutate'], strict=True):
                self.assertIs(got, want)
            self.assertIs(parent.first_failure[0], a)
            self.assertIs(parent.first_failure[2], a_context)
            self.assertIs(parent.first_failure[3], a_cause)
            self.assertEqual(len(observed['capture']), 1)
            escaped, rollback = observed['capture'][0]
            self.assertIs(escaped, a)
            first, lane, check = rollback
            self.assertIs(first, parent.first_failure)
            self.assertIs(lane, parent.rollback_cleanup)
            self.assertIsNone(lane[0])
            if stacked:
                self.assertEqual(len(observed['checks']), 1)
                self.assertEqual(observed['checks'][0][0].code, 'COORDINATOR_EXECUTION_CANCELLED')
                seen_error, seen_tb, seen_context, seen_cause = observed['checks'][0]
                self.assertIs(check[0], seen_error)
                self.assertIs(check[2], seen_context)
                self.assertIs(check[3], seen_cause)
                self.assertIs(check[1].tb_frame.f_code, hb['code'])
                self.assertIs(check[1].tb_next, seen_tb)
                self.assertIs(lane[1][0], b)
                self.assertIs(lane[2][0], c)
                self.assertIsNotNone(lane[1][1])
                self.assertIsNotNone(lane[2][1])
            else:
                self.assertEqual(observed['checks'], [])
                self.assertIsNone(check)
                self.assertEqual(lane[1:], [None, None])
            diagnostic = s.coordinator.failure_projection
            self.assertEqual(diagnostic['chain'][0]['type'], 'sqlite3.OperationalError' if kind == 'sqlite-shaped' else 'WorldError')
            self.assertEqual([f['line'] for f in diagnostic['chain'][0]['reported_frames']
                              if f['file'] == 'app/world.py' and f['function'] == '_lc_geo1133_mutate_body'], [rollback_line])
            self.assertFalse(any(f['file'] == 'app/actors.py' for f in diagnostic['chain'][0]['reported_frames']))
            self.assertEqual([r['stage'] for r in diagnostic['rollback_followups']],
                             ['rollback_retry', 'connection_close', 'host_io_check'])
            self.assertEqual([r['failure'] is not None for r in diagnostic['rollback_followups']], [stacked] * 3)
            if stacked:
                # Independently bind both projected direct edges to the exact A.
                self.assertIs(lane[1][2], a)
                self.assertIsNone(lane[1][3])
                self.assertIs(check[2], a)
                self.assertIsNone(check[3])
                saved_tb = observed['at_mutate'][1]
                self.assertIs(saved_tb.tb_frame.f_code, mb['code'])
                self.assertEqual(saved_tb.tb_lineno, rollback_line)
                self.assertIs(saved_tb.tb_next.tb_frame.f_code, rollback_here_code[0])
                self.assertIsNone(saved_tb.tb_next.tb_next)
                expected = [('app/world.py', mb['code'].co_name, saved_tb.tb_lineno)]
                # Negative control: live A differs by the real upward propagation.
                live_tb = BaseException.__traceback__.__get__(a)
                live_codes = []
                while live_tb is not None:
                    live_codes.append(live_tb.tb_frame.f_code)
                    live_tb = live_tb.tb_next
                self.assertIn(hb['code'], live_codes)
                for index, source_type in ((0, 'RuntimeError'), (2, 'WorldError')):
                    chain = diagnostic['rollback_followups'][index]['failure']['chain']
                    self.assertEqual((chain[0]['relation'], chain[0]['type']), ('primary', source_type))
                    nested = chain[1]
                    self.assertEqual((nested['relation'], nested['type']),
                                     ('context', diagnostic['chain'][0]['type']))
                    self.assertEqual([(f['file'], f['function'], f['line'])
                                      for f in nested['reported_frames']], expected)
                    self.assertIs(nested['foreign_frames_omitted'], True)
                    self.assertIs(nested['frames_truncated'], False)
            if kind == 'sqlite-shaped':
                self.assertIsNone(diagnostic['chain'][0]['sqlite_errorcode'])  # no invented engine metadata
            self.assertEqual([call['kind'] for call in fixture.calls].count('succession'), 1)
            self.assertEqual(fixture.business(), baseline)
            fixture.unchanged_controls(controls)
            self.assertEqual(world.receipt(authority.OLD, fixture.old_activation['intent_id']), fixture.old_receipt)
            self.assertEqual(fixture.violations, [])
        try:
            drift.LifecycleSourceDriftTests._geo1145_first(self, fixture, whole, exercise)
        finally:
            record = self._geo1145_first_record
            observed['first_close'] = record['close']
            observed['test_failure'] = record['primary']
            observed['cleanup_failures'] = record['secondary']
            observed['close_envelope'] = record
            if not record['scopes_restored']:
                _STOPPED_1157 = (fixture, record)
                observed['stop'] = 'STOP: owned rollback fixture quiescence/restoration unproven'


class RollbackFirstTupleProjection1157(unittest.TestCase):
    def test_every_rollback_chain_uses_first_tuples_for_known_nodes(self):
        def first(error):
            return (error, BaseException.__traceback__.__get__(error),
                    BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))

        a = RuntimeError('private-A')
        b = ValueError('private-B')
        c = TypeError('private-C')
        check = TimeoutError('private-check')
        unknown = OSError('private-live-unknown')
        a.__cause__, a.__context__ = unknown, b
        b.__cause__ = c
        c.__context__ = check
        check.__cause__ = b
        saved = [first(error) for error in (a, b, c, check)]

        # Change all the known objects AFTER their first observations. A later
        # catch adds a foreign frame; neither it nor the new links are first.
        for error in (a, b, c, check):
            try:
                raise error
            except BaseException as live:
                live.__context__ = live.__cause__ = AssertionError('private-not-first')
        try:
            raise unknown
        except OSError as live:
            live.__cause__ = check  # Unknown must still use its live descriptors.

        target = SimpleNamespace(_dispatch_local=SimpleNamespace(), failure_projection=None)
        actual = server.Coordinator._capture_failure(target, a,
            rollback=(saved[0], [None, saved[1], saved[2]], saved[3]))
        self.assertIs(actual, target.failure_projection)
        followups = actual['rollback_followups']
        self.assertEqual([row['stage'] for row in followups],
                         ['rollback_retry', 'connection_close', 'host_io_check'])
        cases = (
            (actual, [('primary', 'RuntimeError'), ('cause', 'OSError'),
                      ('context', 'ValueError'), ('cause', 'TimeoutError')]),
            (followups[0]['failure'], [('primary', 'ValueError'), ('cause', 'TypeError'),
                                      ('context', 'TimeoutError')]),
            (followups[1]['failure'], [('primary', 'TypeError'), ('context', 'TimeoutError'),
                                      ('cause', 'ValueError')]),
            (followups[2]['failure'], [('primary', 'TimeoutError'), ('cause', 'ValueError'),
                                      ('cause', 'TypeError')]),
        )
        for projection, expected in cases:
            self.assertEqual([(row['relation'], row['type']) for row in projection['chain']], expected)
            self.assertIs(projection['chain_truncated'], True)
            self.assertEqual([row['foreign_frames_omitted'] for row in projection['chain']],
                             [kind == 'OSError' for _, kind in expected])
            self.assertTrue(all(row['reported_frames'] == [] for row in projection['chain']))
        self.assertNotIn('private-', str(actual))
        self.assertIs(a.__cause__, a.__context__)
        self.assertIs(type(a.__cause__), AssertionError)

        ordinary = server.Coordinator._capture_failure(target, a)
        self.assertNotIn('rollback_followups', ordinary)
        self.assertEqual([(row['relation'], row['type']) for row in ordinary['chain']],
                         [('primary', 'RuntimeError'), ('cause', 'AssertionError')])
        self.assertIs(ordinary['chain'][0]['foreign_frames_omitted'], True)
