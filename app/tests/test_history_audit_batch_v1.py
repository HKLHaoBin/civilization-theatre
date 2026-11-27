"""History-only full-lineage audit count; real registered Host/World stores."""
from collections import Counter
from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from app.actor_execution import PrivateStore
from app.actors import ActorHost
from app.server import Coordinator
from app.world import WorldStore
from app.tests.activation_fixtures import close_activations

SCRATCH=Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
OBSERVATIONS=[]

@contextmanager
def history_fixture():
    with tempfile.TemporaryDirectory(prefix='history-audit-count-',dir=SCRATCH) as directory:
        root=Path(directory)
        world=WorldStore(root/'world.sqlite');world.initialize()
        host=ActorHost(world,root/'actors.sqlite')
        owner=Coordinator(world,host)
        try:
            owner.acquire_owner();attempt=owner._owner_attempt
            with owner._owner_scope(attempt):
                owner._prepare_scheduler(attempt)
                world.control('resume')
                windows=[]
                for _ in range(2):
                    state=world.snapshot()
                    window=world.open_or_recover_window(coordinator=attempt.permit,
                        expected_version={k:state[k] for k in ('world_id','revision')})
                    results=close_activations(world,host,attempt.permit,window)
                    assert len(results)==len(window['roster']) and len(results)>1
                    completed=world.complete_window(window['window_id'],coordinator=attempt.permit)
                    assert completed['state']=='completed'
                    windows.append(completed)
                yield world,host,owner,attempt,windows
        finally:
            owner.close()
            assert not owner.cleanup_pending and not host.cleanup_pending

class HistoryAuditBatchTests(unittest.TestCase):
    def test_history_reuses_one_successful_complete_audit_per_person(self):
        with history_fixture() as (world,host,owner,attempt,windows):
            counts=Counter();calls=[];original=PrivateStore._audit
            def counted(storage,db,person):
                value=original(storage,db,person)
                counts[person]+=1
                calls.append(dict(person=person,storage=id(storage),db=id(db),success=True))
                return value
            state=world.snapshot()
            with owner._owner_scope(attempt),patch.object(PrivateStore,'_audit',counted):
                opened=world.open_or_recover_window(coordinator=attempt.permit,
                    expected_version={k:state[k] for k in ('world_id','revision')})
            OBSERVATIONS.append(dict(counts=dict(counts),calls=calls,opened=opened,
                historical=windows,root=str(Path(world.db_path).parent),original_open_succeeded=True))
            self.assertEqual(opened['ordinal'],3)
            self.assertEqual(opened['target_time'],state['time']+86400)
            self.assertEqual(set(counts),set(windows[0]['roster']))
            self.assertEqual(dict(counts),{person:1 for person in windows[0]['roster']},
                'one successful complete PrivateStore._audit per history batch/person')

if __name__=='__main__':unittest.main()


# Append-only candidate characterization; frozen fixture/oracle above unchanged.
import contextvars
import sqlite3
import time
from app import coordinator_scheduler as batch_scheduler
from app.actor_execution import ActorRunError
from app.world import WorldError

NEGATIVE_OBSERVATIONS=[]


@contextmanager
def batch_case(world,host,owner,attempt):
    with owner._owner_scope(attempt),host._storage.transaction(initialize=True,attempt=attempt) as db:
        with batch_scheduler._history_audit_scope(world,host._storage,db):
            batch=batch_scheduler._history_audit_batch.get()
            assert batch is not None
            rows=[dict(r) for r in db.execute('SELECT * FROM coordinator_activations ORDER BY window_id,roster_index')]
            yield db,batch,rows


@contextmanager
def real_audit_counts():
    counts=Counter();entered=Counter();original=PrivateStore._audit
    def counted(storage,db,person):
        entered[(id(storage),person)]+=1
        value=original(storage,db,person)
        counts[(id(storage),person)]+=1
        return value
    with patch.object(PrivateStore,'_audit',counted):yield counts,entered


class HistoryAuditInvalidationTests(unittest.TestCase):
    def test_sql_controls_and_real_writes_permanently_taint_but_exact_busy_one_does_not(self):
        controls={
            'pure-savepoint':['SAVEPOINT history_probe','RELEASE history_probe'],
            'write-rollback':['SAVEPOINT history_probe','UPDATE actor.actor_sessions SET messages=messages','ROLLBACK TO history_probe','RELEASE history_probe'],
            'commit-restart':['COMMIT','BEGIN IMMEDIATE'],
            'rollback-restart':['ROLLBACK','BEGIN IMMEDIATE'],
            'main-DML':['UPDATE main.intents SET payload=payload'],
            'actor-DML':['UPDATE actor.actor_sessions SET messages=messages'],
            'DDL':['CREATE TABLE main.history_probe (v INTEGER)','DROP TABLE main.history_probe'],
            'attach-detach':["ATTACH DATABASE ':memory:' AS history_extra",'DETACH DATABASE history_extra'],
            'unknown-read':['WITH history_probe(v) AS (SELECT 1) SELECT v FROM history_probe'],
            'other-pragma':['PRAGMA busy_timeout=2'],
            'readonly':['SELECT 1','PRAGMA database_list','PRAGMA actor.table_info(actor_calls)','  pragma BUSY_TIMEOUT = 1 ; '],
        }
        with history_fixture() as (w,h,co,attempt,windows):
            for mode,sqls in controls.items():
                with self.subTest(mode=mode),real_audit_counts() as (counts,entered),batch_case(w,h,co,attempt) as (db,batch,rows):
                    row=rows[0];key=(id(h._storage),row['person_id']);audit=lambda:batch_scheduler.native_audit(h._storage,db,row)
                    audit();audit();self.assertEqual(counts[key],1);before=db.total_changes;actual=[]
                    for sql in sqls:
                        db.execute(sql).fetchall();actual.append(sql)
                    audit();audit();expected=1 if mode=='readonly' else 3
                    self.assertEqual(counts[key],expected);self.assertEqual(batch.tainted,mode!='readonly')
                    if mode in ('pure-savepoint','commit-restart','rollback-restart','attach-detach','unknown-read','other-pragma','readonly'):
                        self.assertEqual(db.total_changes,before)
                    NEGATIVE_OBSERVATIONS.append(dict(case='sql-invalidation',mode=mode,actual_sql=actual,successes=counts[key],tainted=batch.tainted))

    def test_actual_actor_and_main_corruption_reaudits_and_failure_is_never_cached(self):
        with history_fixture() as (w,h,co,attempt,windows):
            for mode in ('actor-call','main-receipt','first-audit'):
                with self.subTest(mode=mode),real_audit_counts() as (counts,entered),batch_case(w,h,co,attempt) as (db,batch,rows):
                    row=rows[0];person=row['person_id'];key=(id(h._storage),person)
                    if mode!='first-audit':batch_scheduler.native_audit(h._storage,db,row)
                    if mode=='main-receipt':
                        old=db.execute('SELECT receipt FROM main.intents WHERE id=?',(row['intent_id'],)).fetchone()[0]
                        db.execute("UPDATE main.intents SET receipt='{}' WHERE id=?",(row['intent_id'],))
                    else:
                        old=db.execute('SELECT call_id,intent_id FROM actor.actor_calls WHERE person_id=? LIMIT 1',(person,)).fetchone()
                        self.assertIsNotNone(old)
                        db.execute("UPDATE actor.actor_calls SET intent_id='broken-real-link' WHERE call_id=?",(old[0],))
                    with self.assertRaises((ActorRunError,WorldError)) as caught:batch_scheduler.native_audit(h._storage,db,row)
                    self.assertIn(caught.exception.code,('ACTOR_RECOVERY_UNPROVEN','WINDOW_RECOVERY_UNPROVEN'))
                    self.assertTrue(batch.tainted);self.assertEqual(batch.successes,set())
                    if mode=='main-receipt':db.execute('UPDATE main.intents SET receipt=? WHERE id=?',(old,row['intent_id']))
                    else:db.execute('UPDATE actor.actor_calls SET intent_id=? WHERE call_id=?',(old[1],old[0]))
                    batch_scheduler.native_audit(h._storage,db,row)
                    self.assertEqual(entered[key],2 if mode=='first-audit' else 3)
                    self.assertEqual(counts[key],1 if mode=='first-audit' else 2)
                    NEGATIVE_OBSERVATIONS.append(dict(case='real-corrupt-and-repair',mode=mode,error=caught.exception.code,body_entries=entered[key],successes=counts[key]))

    def test_person_connection_batch_and_real_same_file_storage_are_isolated(self):
        with history_fixture() as (w,h,co,attempt,windows):
            alias=contextvars.Context().run(ActorHost,w,h.db_path)
            try:
                with real_audit_counts() as (counts,entered):
                    ids=[];person=None
                    for i in range(2):
                        with batch_case(w,h,co,attempt) as (db,batch,rows):
                            ids.append(id(db));row=rows[0];person=row['person_id'];other=next(r for r in rows if r['person_id']!=person)
                            for _ in range(2):batch_scheduler.native_audit(h._storage,db,row)
                            batch_scheduler.native_audit(h._storage,db,other)
                            if i==0:
                                batch_scheduler.native_audit(alias._storage,db,row)
                                self.assertTrue(batch.tainted)
                                batch_scheduler.native_audit(h._storage,db,row)
                    self.assertNotEqual(*ids);self.assertEqual(counts[(id(h._storage),person)],3)
                    self.assertEqual(counts[(id(alias._storage),person)],1)
                    self.assertEqual(counts[(id(h._storage),other['person_id'])],2)
                    NEGATIVE_OBSERVATIONS.append(dict(case='principal-storage-connection-isolation',connections=ids,original_person_successes=3,alias_successes=1))
            finally:alias.close()

    def test_original_path_source_object_and_remaining_authority_gates_survive_hits(self):
        from contextlib import closing, nullcontext
        from app.actor_execution import EntryBudget
        import threading
        import traceback
        with history_fixture() as (w,h,co,attempt,windows):
            storage=h._storage;otherworld=WorldStore(w.db_path)
            alias=contextvars.Context().run(ActorHost,w,h.db_path)
            closed_conflicts=[]  # Keep genuine closed WeakSet members alive through ALL modes.
            def domain_rows():
                result=[]
                for path,names in ((w.db_path,('snapshots','intents','coordinator_windows','coordinator_activations','coordinator_activation_outcomes')),
                                   (h.db_path,('actor_sessions','actor_session_heads','actor_intents','actor_intent_execution','actor_calls'))):
                    with closing(sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True)) as reader:
                        result.append({name:reader.execute('SELECT * FROM '+name+' ORDER BY rowid').fetchall() for name in names})
                return result
            try:
                for mode in ('storage-object','root-world','path','source-conflict','cancel','entry'):
                    conflict=None;entry=None;target_entered=[];target_error=None;final_error=None
                    before=domain_rows()
                    if mode=='entry':
                        entry=EntryBudget(h.execution_policy,threading.Event())
                        # Wait outside every DB transaction; never alter the original bound.
                        entry.stop.wait(max(0,entry.busy_deadline-time.monotonic()-.25))
                    with self.subTest(mode=mode),real_audit_counts() as (counts,entered):
                        try:
                            with (w._entry_budget(entry) if entry is not None else nullcontext()):
                                if entry is not None:self.assertIs(w._scoped_entry_budget(),entry)
                                with batch_case(w,h,co,attempt) as (db,batch,rows):
                                    row=rows[0];person=row['person_id'];key=(id(storage),person)
                                    priming_started=time.monotonic()
                                    batch_scheduler.native_audit(storage,db,row)
                                    self.assertEqual(counts[key],1)
                                    self.assertIn(person,batch.successes);self.assertFalse(batch.tainted)
                                    batch_scheduler.native_audit(storage,db,row)
                                    self.assertEqual(counts[key],1);self.assertEqual(entered[key],1)
                                    self.assertIn(person,batch.successes);self.assertFalse(batch.tainted)
                                    priming_finished=time.monotonic()
                                    if mode=='storage-object':
                                        h._storage=alias._storage
                                        batch_scheduler.native_audit(storage,db,row);self.assertTrue(batch.tainted)
                                        with self.assertRaises(WorldError):batch_scheduler.source(w)
                                    elif mode=='root-world':
                                        attempt.world=otherworld
                                        batch_scheduler.native_audit(storage,db,row);self.assertTrue(batch.tainted)
                                        self.assertEqual(counts[key],2)
                                    elif mode=='path':
                                        h.db_path=str(Path(h.db_path).with_name('different.sqlite'))
                                        with self.assertRaises(ActorRunError) as caught:batch_scheduler.native_audit(storage,db,row)
                                        self.assertEqual(caught.exception.code,'EXECUTION_WORLD_MISMATCH')
                                    elif mode=='source-conflict':
                                        conflict=contextvars.Context().run(ActorHost,w,Path(h.db_path).with_name('conflicting.sqlite'))
                                        closed_conflicts.append(conflict)
                                        with self.assertRaises(WorldError) as caught:batch_scheduler.source(w)
                                        self.assertEqual(caught.exception.code,'WINDOW_RECOVERY_UNPROVEN')
                                        batch_scheduler.native_audit(storage,db,row);self.assertTrue(batch.tainted)
                                    elif mode=='cancel':
                                        attempt.failure=WorldError('COORDINATOR_EXECUTION_CANCELLED','actual captured failure')
                                        with self.assertRaises(WorldError) as caught:batch_scheduler.native_audit(storage,db,row)
                                        self.assertIs(caught.exception,attempt.failure)
                                        self.assertEqual(counts[key],1)
                                    else:
                                        self.assertLess(priming_finished,entry.busy_deadline)
                                        original_audit=batch_scheduler.native_audit
                                        def delegated(*args,**kwargs):
                                            target_entered.append(dict(entry=id(w._scoped_entry_budget()),now=time.monotonic()))
                                            return original_audit(*args,**kwargs)
                                        entry.stop.wait(max(0,entry.busy_deadline-time.monotonic()))
                                        with patch.object(batch_scheduler,'native_audit',delegated):
                                            try:batch_scheduler.native_audit(storage,db,row)
                                            except WorldError as error:
                                                target_error=error
                                                trace=''.join(traceback.format_exception(error))
                                                self.assertIn('native_audit',trace)
                                                self.assertEqual(error.code,'EXECUTION_STORAGE_BUSY')
                                            else:self.fail('original expired entry must reject at actual native audit')
                                        self.assertEqual(target_entered[0]['entry'],id(entry));self.assertEqual(len(target_entered),1)
                                        self.assertEqual(counts[key],1)
                                    NEGATIVE_OBSERVATIONS.append(dict(case='actual-identity-authority',mode=mode,successes=sum(counts.values()),
                                        hit_body_entries=1,priming_seconds=priming_finished-priming_started,
                                        entry_id=id(entry) if entry else None,absolute_busy_deadline=entry.busy_deadline if entry else None,
                                        priming_finished=priming_finished,target_entered=target_entered,
                                        target_exception=trace if entry else None,closed_conflicts_retained=len(closed_conflicts)))
                                    # Restore before the original transaction/owner final gates.
                                    h._storage=storage;h.db_path=str(storage.actor_path);attempt.world=w;attempt.failure=None
                                    if conflict is not None:conflict.close();conflict=None
                        except WorldError as error:
                            if entry is None:raise
                            final_error=error
                            self.assertIsNotNone(target_error,'scope entry/final failure is NOT target coverage')
                            self.assertEqual(error.code,'EXECUTION_STORAGE_BUSY')
                            NEGATIVE_OBSERVATIONS.append(dict(case='original-expired-entry-final-gate',code=error.code,
                                traceback=''.join(traceback.format_exception(error)),target_exception_preserved=target_error.code))
                        finally:
                            h._storage=storage;h.db_path=str(storage.actor_path);attempt.world=w;attempt.failure=None
                            if conflict is not None:conflict.close()
                        if entry is not None:
                            self.assertTrue(target_entered);self.assertIsNotNone(target_error)
                            self.assertEqual(target_error.code,'EXECUTION_STORAGE_BUSY')
                        self.assertEqual(domain_rows(),before,'no domain writes through original authority negatives')
            finally:alias.close()

    def test_unknown_connection_lifecycle_keeps_existing_trace_without_overwriting(self):
        with history_fixture() as (w,h,co,attempt,windows):
            original=PrivateStore._open;seen=[]
            def instrumented(storage,attempt):
                db=original(storage,attempt);db.set_trace_callback(seen.append);return db
            with patch.object(PrivateStore,'_open',instrumented),co._owner_scope(attempt):
                with h._storage.transaction(initialize=True,attempt=attempt) as db:
                    with batch_scheduler._history_audit_scope(w,h._storage,db):
                        self.assertIsNone(batch_scheduler._history_audit_batch.get());db.execute('SELECT 731')
                    db.execute('SELECT 732')
            self.assertIn('SELECT 731',seen);self.assertIn('SELECT 732',seen)
            NEGATIVE_OBSERVATIONS.append(dict(case='unknown-existing-callback-preserved',actual_selects=[s for s in seen if s in ('SELECT 731','SELECT 732')]))

    def test_real_missing_historical_head_never_inherits_cached_success(self):
        with history_fixture() as (w,h,co,attempt,windows),batch_case(w,h,co,attempt) as (db,batch,rows):
            row=rows[0];person=row['person_id'];batch_scheduler.native_audit(h._storage,db,row)
            saved=db.execute('SELECT * FROM actor.actor_session_heads WHERE person_id=?',(person,)).fetchone()
            db.execute('DELETE FROM actor.actor_session_heads WHERE person_id=?',(person,))
            with self.assertRaises(ActorRunError) as caught:batch_scheduler.native_audit(h._storage,db,row)
            self.assertEqual(caught.exception.code,'ACTOR_RECOVERY_UNPROVEN')
            db.execute('INSERT INTO actor.actor_session_heads VALUES (?,?,?,?,?,?,?,?,?,?)',tuple(saved))
            batch_scheduler.native_audit(h._storage,db,row);self.assertTrue(batch.tainted)
            # A genuinely new, untouched principal may have an empty head.
            pending=dict(row,person_id='history-new-untouched',phase='pending')
            batch_scheduler.native_audit(h._storage,db,pending)
            with self.assertRaises(ActorRunError):batch_scheduler.native_audit(h._storage,db,dict(pending,phase='completed'))
            NEGATIVE_OBSERVATIONS.append(dict(case='missing-head-and-legitimate-empty',error=caught.exception.code))

    def test_real_rejected_leaves_then_successor_preserve_full_lineage(self):
        from app.tests import test_activation_domain_rejections as domain
        domain.ActivationDomainRejectionTests('test_multiple_rejected_leaves_allow_successor_spine_and_historical_replay').test_multiple_rejected_leaves_allow_successor_spine_and_historical_replay()
        NEGATIVE_OBSERVATIONS.append(dict(case='real-two-rejected-leaves-followed-by-success-and-replay',original_assertions_completed=True))

    def test_real_blocked_material_and_post_audit_final_identity_remain_enforced(self):
        from app.tests import test_technical_maintenance_arch_a as original
        from app.tests import test_technical_maintenance as fixture
        from app import coordinator_maintenance as maintenance
        with fixture.blocked_case('history-batch-block-final') as c:
            original.precondition(self,c)
            w,h,co=c['world'],c['host'],c['coordinator']
            with self.assertRaises(WorldError) as caught:h.act(c['ref'].person_id,coordinator=co._owner_attempt.permit,activation=c['ref'])
            self.assertEqual(caught.exception.code,'ACTIVATION_TECHNICAL_BLOCKED')
            self.assertFalse(fixture.perform(c)['replayed'])
            other=contextvars.Context().run(ActorHost,w,h.db_path);audit=batch_scheduler.native_audit;hits=[]
            before=(fixture.logical(w.db_path),fixture.logical(h.db_path))
            def post(storage,db,row):
                value=audit(storage,db,row)
                if row['phase']=='completed' and not hits:co.actors=other;hits.append('real-audit-completed-then-original-Host-replaced')
                return value
            def forbidden(*a,**k):raise AssertionError('readonly final gate cannot issue or promote')
            try:
                with patch.object(batch_scheduler,'native_audit',post),patch.object(maintenance,'acquire_actor',forbidden),patch.object(maintenance,'promote',forbidden),self.assertRaises(WorldError) as caught:fixture.perform(c)
            finally:co.actors=h;other.close()
            self.assertTrue(hits);self.assertEqual(caught.exception.code,'ACTIVATION_BINDING_CONFLICT')
            self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before)
            NEGATIVE_OBSERVATIONS.append(dict(case='blocked-and-real-post-audit-final-Host',hits=hits,error=caught.exception.code,root=str(c['root'])))


# Pure regressions call the actually imported canonical classifier.
CLASSIFIER_PARENT_CASES = (('known_constant', 'SELECT 1', True), ('known_head_exists', "SELECT 1 FROM actor.sqlite_master WHERE type='table' AND name='actor_session_heads'", True), ('known_session', "SELECT messages,seen FROM actor.actor_sessions WHERE person_id='person-country-1-king'", True), ('busy_one_exact', 'PRAGMA busy_timeout=1', True), ('busy_one_format', '  pragma BUSY_TIMEOUT = 1 ; ', True), ('readonly_database_list', 'PRAGMA database_list', True), ('readonly_table_info', 'PRAGMA actor.table_info(actor_calls)', True), ('unknown_udf', 'SELECT history_unclassified_udf(1)', False), ('incomplete_select', 'SELECT 1 FROM', False), ('write', 'UPDATE actor.actor_sessions SET messages=messages', False), ('DDL', 'CREATE TABLE history_probe(v INTEGER)', False), ('control', 'COMMIT', False), ('savepoint', 'SAVEPOINT history_probe', False), ('attach', "ATTACH DATABASE ':memory:' AS history_extra", False), ('multiple_statements', 'SELECT 1; DELETE FROM actor.actor_calls', False), ('unknown_with_read', 'WITH history_probe(v) AS (SELECT 1) SELECT v FROM history_probe', False), ('other_busy_setting', 'PRAGMA busy_timeout=2', False), ('unknown_pragma', 'PRAGMA journal_mode', False), ('comment', 'SELECT 1 -- history probe', False))

class HistoryClassifierContractTests(unittest.TestCase):
    def test_same_nineteen_parent_contract_inputs(self):
        for name,sql,expected in CLASSIFIER_PARENT_CASES:
            with self.subTest(case=name):
                self.assertIs(batch_scheduler._history_read_sql(sql),expected)

    def test_complete_forms_preserve_scalar_literals_and_reject_unknown_combinations(self):
        positives = [
            "  sElEcT messages , seen FROM actor.actor_sessions WHERE person_id = 'O''Reilly; -- /* SELECT bad() */' ;  ",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id='Alice \"quoted\"'",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id=-2.5e+3",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id=NULL",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id=x'00ff'",
            "SELECT state FROM snapshots WHERE seq=17",
            "SELECT * FROM actor.actor_intents WHERE intent_id='a' AND person_id='b'",
            "SELECT coalesce(max(ordinal),0) FROM coordinator_windows WHERE world_id='w'",
        ]
        negatives = [
            'SELECT', 'SELECT 1 FROM', 'SELECT 1 garbage', 'SELECT 1; SELECT 1',
            'SELECT 1 -- comment', 'SELECT 1 /* comment */',
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id=unknown_udf(1)",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id='a' OR 1=1",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id='a' UNION SELECT 1,2",
            "SELECT messages,seen FROM unknown_table WHERE person_id='a'",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id='unterminated",
            "SELECT messages,seen FROM actor.actor_sessions WHERE person_id=x'0'",
            'SELECT unknown_udf(1)', 'SELECT unknown_udf(SELECT 1)',
            'SELECT 1;;', 'PRAGMA busy_timeout=01', 'PRAGMA busy_timeout=2',
        ]
        for sql in positives:
            with self.subTest(positive=sql):self.assertIs(batch_scheduler._history_read_sql(sql),True)
        for sql in negatives:
            with self.subTest(negative=sql):self.assertIs(batch_scheduler._history_read_sql(sql),False)
