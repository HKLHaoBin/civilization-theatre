"""P02.1 real SQLite authority and deterministic transaction interleavings."""
from contextlib import closing
from dataclasses import replace, FrozenInstanceError
from pathlib import Path
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from app.world import WorldStore, WorldError
from app.execution import ExecutionPermit
from app.adjudication import Adjudicator

SCRATCH = 'C:/Users/We3q/AppData/Local/hermes/cache/scratch'
PERSON = 'person-country-1-finance'


class ExecutionFencingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=SCRATCH)
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'world.sqlite'
        self.now = 1000000000000
        self.world = WorldStore(self.path)
        self.world.initialize()
        self.world.control('resume')

    def error(self, code, call):
        with self.assertRaises(WorldError) as caught:
            call()
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def clock_world(self):
        self.assertIn('execution_clock_ns', __import__('inspect').signature(WorldStore).parameters,
                      'trusted execution clock injection is missing')
        self.world = WorldStore(self.path, execution_clock_ns=lambda: self.now)
        return self.world

    def decision(self, effects):
        state = self.world.snapshot()
        return dict(world_id=state['world_id'], base_revision=state['revision'], ruling='成功',
                    reason='可信脚本裁定', goal='测试', costs=[], benefits=[], effects=effects)

    def child_acquire(self, now, owner='child'):
        code = """import sys,json
from dataclasses import asdict
from app.world import WorldStore,WorldError
w=WorldStore(sys.argv[1],execution_clock_ns=lambda:int(sys.argv[2]))
try:
 p=w.acquire_actor_execution(sys.argv[3],sys.argv[4])
 print(json.dumps({'permit':asdict(p)}))
except WorldError as e:
 print(json.dumps({'error':e.code}))
"""
        result = subprocess.run([sys.executable,'-B','-c',code,str(self.path),str(now),PERSON,owner],
                                cwd=Path(__file__).resolve().parents[2], capture_output=True,text=True,timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_characterization_independent_process_race_has_one_authority_owner(self):
        """Additional acceptance for implemented acquisition, not a new RED."""
        code = """import sys,json
from dataclasses import asdict
from app.world import WorldStore,WorldError
w=WorldStore(sys.argv[1],execution_clock_ns=lambda:1000000000000)
print('ready',flush=True)
sys.stdin.readline()
try:
 p=w.acquire_actor_execution(sys.argv[2],sys.argv[3])
 print(json.dumps({'permit':asdict(p)}),flush=True)
except WorldError as e:
 print(json.dumps({'error':e.code}),flush=True)
"""
        processes = []
        try:
            for owner in ('process-a','process-b'):
                child = subprocess.Popen([sys.executable,'-B','-c',code,str(self.path),PERSON,owner],
                    cwd=Path(__file__).resolve().parents[2],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                processes.append(child)
            for child in processes:
                self.assertEqual(child.stdout.readline().strip(), 'ready')
            for child in processes:
                child.stdin.write('go\n')
                child.stdin.flush()
            results = []
            for child in processes:
                out, err = child.communicate(timeout=15)
                self.assertEqual(child.returncode, 0, err)
                results.append(json.loads(out))
            self.assertEqual(sum('permit' in item for item in results), 1)
            self.assertEqual([item['error'] for item in results if 'error' in item], ['ACTOR_EXECUTION_BUSY'])
            winner = next(item['permit'] for item in results if 'permit' in item)
            with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
                self.assertEqual(db.execute('SELECT epoch,owner_token FROM actor_execution_leases').fetchone(), (1,winner['owner_token']))
                self.assertEqual(db.execute('SELECT count(*) FROM snapshots').fetchone()[0], 2)
            print('independent_process_race: children=2 exits=[0,0] owners=1 busy=1 snapshots=2')
        finally:
            for child in processes:
                if child.poll() is None:
                    child.kill()
                child.communicate(timeout=15)

    def test_characterization_process_takeover_between_submit_transactions_recovers_original_id(self):
        """Real child-process takeover at the durable prepare/effect seam."""
        w = self.clock_world()
        dm = Adjudicator(w)
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        dm.propose(PERSON, 'compound-process', '调粮并等待', execution=p)
        version = dm.record(PERSON, 'compound-process', self.decision([{'action':'ship','to':'place-1','quantity':5},{'action':'wait'}]), execution=p)
        original = w._mutate
        captured = {}
        def takeover(operation, **kwargs):
            self.now += 1000000000
            response = self.child_acquire(self.now)
            captured['permit'] = ExecutionPermit(**response['permit'])
            return original(operation, **kwargs)
        before = w.snapshot()
        w._mutate = takeover
        try:
            self.error('ACTOR_EXECUTION_FENCED', lambda: w.submit(PERSON,'process-original',{'action':'ship','to':'place-1','quantity':10},execution=p))
        finally:
            w._mutate = original
        self.assertEqual(w.snapshot(), before)
        self.assertIsNone(w.receipt(PERSON,'process-original'))
        self.error('ACTOR_EXECUTION_FENCED', lambda: dm.commit(PERSON,'compound-process',version,execution=p))
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NULL').fetchone()[0], 2)
            self.assertEqual(db.execute('SELECT epoch FROM actor_execution_leases').fetchone()[0], 2)
        q = captured['permit']
        receipt = w.submit(PERSON,'process-original',{'action':'ship','to':'place-1','quantity':10},execution=q)
        self.assertEqual(w.submit(PERSON,'process-original',{'action':'ship','to':'place-1','quantity':10},execution=q),receipt)
        self.assertEqual(w.snapshot()['places'][0]['grain'],before['places'][0]['grain']-10)
        version = dm.record(PERSON,'compound-process',self.decision([{'action':'ship','to':'place-1','quantity':5},{'action':'wait'}]),execution=q)
        compound = dm.commit(PERSON,'compound-process',version,execution=q)
        self.assertEqual(dm.commit(PERSON,'compound-process',version,execution=q),compound)
        self.assertEqual(w.snapshot()['places'][0]['grain'],before['places'][0]['grain']-15)
        with closing(sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro',uri=True)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents WHERE receipt IS NOT NULL').fetchone()[0],4)
        print('process_takeover: child_exit=0 epoch=2 stale_submit=fenced stale_compound=fenced original_ship=10 compound_ship=5 receipts=4')

    def test_characterization_effect_guard_and_takeover_use_same_sqlite_write_lock(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one')
        apply = w._apply_action
        def competing_writer(*args):
            self.assertEqual(self.child_acquire(self.now+61000000000)['error'], 'EXECUTION_STORAGE_BUSY')
            return apply(*args)
        w._apply_action = competing_writer
        w.submit(PERSON,'locked',{'action':'wait'},execution=p)
        self.assertIsNotNone(w.receipt(PERSON,'locked'))
        print('effect_lock: independent_child_takeover=EXECUTION_STORAGE_BUSY while World effect transaction owns write lock')

    def test_characterization_version_and_epoch_gates_are_independent(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON,'one')
        preview = w.preview(PERSON,'version',{'action':'wait'},execution=p)
        old = {'world_id':preview['world_id'],'revision':preview['base_revision']}
        w.submit('person-country-1-king','advance-version',{'action':'wait'})
        self.error('STALE_PREVIEW',lambda: w.submit(PERSON,'version',{'action':'wait'},expected_version=old,execution=p))
        w.release_actor_execution(p)
        q = w.acquire_actor_execution(PERSON,'two')
        current = {'world_id':q.world_id,'revision':w.snapshot()['revision']}
        self.error('ACTOR_EXECUTION_FENCED',lambda: w.submit(PERSON,'version',{'action':'wait'},expected_version=current,execution=p))
        w.submit(PERSON,'version',{'action':'wait'},expected_version=current,execution=q)
        self.now += 61000000000
        self.error('ACTOR_EXECUTION_EXPIRED',lambda: w.submit(PERSON,'version',{'action':'wait'},execution=q))

    def test_characterization_compound_expiry_rolls_back_all_children_and_metadata(self):
        w = self.clock_world()
        dm = Adjudicator(w)
        p = w.acquire_actor_execution(PERSON,'one',ttl_seconds=1)
        dm.propose(PERSON,'compound-expiry','调粮两次',execution=p)
        version = dm.record(PERSON,'compound-expiry',self.decision([{'action':'ship','to':'place-1','quantity':10},{'action':'ship','to':'place-1','quantity':5}]),execution=p)
        before = self.path.read_bytes()
        original = w._apply_action
        def expire(*args):
            effect = original(*args)
            self.now += 500000000
            return effect
        w._apply_action = expire
        self.error('ACTOR_EXECUTION_EXPIRED',lambda: dm.commit(PERSON,'compound-expiry',version,execution=p))
        self.assertEqual(self.path.read_bytes(),before)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0],1)
            self.assertEqual(db.execute("SELECT count(*) FROM dm_timeline WHERE kind='commit'").fetchone()[0],0)

    def test_characterization_legacy_prepare_cannot_bypass_new_registration(self):
        w = self.clock_world()
        original = w._mutate
        def register_at_seam(operation):
            w.acquire_actor_execution(PERSON,'new-owner')
            return original(operation)
        w._mutate = register_at_seam
        before = w.snapshot()
        self.error('ACTOR_EXECUTION_REQUIRED',lambda: w.submit(PERSON,'legacy-seam',{'action':'ship','to':'place-1','quantity':10}))
        self.assertEqual(w.snapshot(),before)
        self.assertIsNone(w.receipt(PERSON,'legacy-seam'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0],1)

    def test_characterization_unsupported_pause_rechecks_in_its_own_transaction(self):
        w = self.clock_world()
        dm = Adjudicator(w)
        p = w.acquire_actor_execution(PERSON,'one')
        dm.propose(PERSON,'unsupported-seam','未知效果',execution=p)
        version = dm.record(PERSON,'unsupported-seam',self.decision([{'action':'teleport'}]),execution=p)
        original = w.technical_pause
        def lose_before_pause(code, *, execution=None):
            w.release_actor_execution(p)
            w.acquire_actor_execution(PERSON,'new-owner')
            return original(code,execution=execution)
        w.technical_pause = lose_before_pause
        before = w.snapshot()
        self.error('ACTOR_EXECUTION_FENCED',lambda: dm.commit(PERSON,'unsupported-seam',version,execution=p))
        self.assertEqual(w.snapshot(),before)
        self.assertIsNone(w.receipt(PERSON,'unsupported-seam'))

    def test_characterization_provider_failure_with_live_permit_pauses_without_false_receipt(self):
        w = self.clock_world()
        dm = Adjudicator(w)
        p = w.acquire_actor_execution(PERSON,'one')
        def failed(context):
            raise RuntimeError('scripted provider error')
        self.error('ADJUDICATION_FAILED',lambda: dm.resolve(PERSON,'normal-failure','等待',failed,execution=p))
        self.assertEqual(w.snapshot()['diagnostic']['code'],'ADJUDICATION_FAILED')
        self.assertIsNone(w.receipt(PERSON,'normal-failure'))
        self.assertEqual(dm.history(PERSON,'normal-failure'),[])

    def test_characterization_ttl_defaults_maximum_and_simulated_time_are_separate(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON,'one')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT expires_wall_ns-acquired_wall_ns FROM actor_execution_leases WHERE person_id=?',(PERSON,)).fetchone()[0],60000000000)
        w.advance(3600)
        w.submit(PERSON,'simulation-does-not-expire',{'action':'wait'},execution=p)
        self.assertEqual(w.renew_actor_execution(p,ttl_seconds=3600),p)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT expires_wall_ns-last_wall_ns FROM actor_execution_leases WHERE person_id=?',(PERSON,)).fetchone()[0],3600000000000)
        self.now += 3600000000000
        self.assertTrue(w.release_actor_execution(p))  # expired owner can release its own tombstone
        self.error('ACTOR_EXECUTION_REQUIRED',lambda: w.preview(PERSON,'no-bypass',{'action':'wait'}))
        q = w.acquire_actor_execution(PERSON,'two')
        self.assertEqual(q.epoch,2)
        other_path = Path(self.temp.name)/'other-world.sqlite'
        other = WorldStore(other_path,execution_clock_ns=lambda:self.now)
        other.initialize()
        other.control('resume')
        foreign = other.acquire_actor_execution(PERSON,'other')
        self.error('EXECUTION_WORLD_MISMATCH',lambda: w.submit(PERSON,'other-world',{'action':'wait'},execution=foreign))
        self.error('EXECUTION_WORLD_MISMATCH',lambda: w.renew_actor_execution(foreign))
        self.error('EXECUTION_WORLD_MISMATCH',lambda: w.release_actor_execution(foreign))

    def test_characterization_nonbusy_sql_failure_is_not_mislabeled(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON,'one')
        with closing(sqlite3.connect(self.path)) as db:
            db.execute('DROP TABLE execution_clock')
            db.execute('CREATE TABLE execution_clock (wrong_column INTEGER)')
            db.commit()
        with self.assertRaises(sqlite3.OperationalError) as caught:
            w.renew_actor_execution(p)
        self.assertEqual(caught.exception.sqlite_errorcode,sqlite3.SQLITE_ERROR)

    def test_reacquire_updates_permanent_lease_row_in_place(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one')
        # A second person ensures SQLite cannot coincidentally reuse the deleted rowid.
        w.acquire_actor_execution('person-country-1-king', 'other')
        with closing(sqlite3.connect(self.path)) as db:
            rowid = db.execute('SELECT rowid FROM actor_execution_leases WHERE person_id=?',(PERSON,)).fetchone()[0]
        w.release_actor_execution(p)
        q = w.acquire_actor_execution(PERSON, 'two')
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT rowid FROM actor_execution_leases WHERE person_id=?',(PERSON,)).fetchone()[0],rowid)
        self.assertEqual(q.epoch,2)

    def test_renew_does_not_resurrect_if_clock_expires_inside_transaction(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        calls = iter([self.now+999999999, self.now+1000000000])
        w._execution_clock_ns = lambda: next(calls)
        self.error('ACTOR_EXECUTION_EXPIRED', lambda: w.renew_actor_execution(p))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT expires_wall_ns FROM actor_execution_leases').fetchone()[0], self.now+1000000000)

    def test_prepare_transaction_rechecks_before_committing_pending(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        original = w._check_version
        def expiry_during_prepare(state, version):
            original(state, version)
            self.now += 1000000000
        w._check_version = expiry_during_prepare
        self.error('ACTOR_EXECUTION_EXPIRED', lambda: w.submit(PERSON, 'prepare-expiry', {'action':'wait'}, execution=p))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM intents WHERE id='prepare-expiry'").fetchone())

    def test_resolve_provider_late_return_cannot_write_or_pause_new_owner(self):
        w = self.clock_world()
        dm = Adjudicator(w)
        p = w.acquire_actor_execution(PERSON, 'one')
        self.assertIn('execution', __import__('inspect').signature(dm.resolve).parameters,
                      'resolve must carry its original execution permit')
        captured = {}
        def provider(context):
            self.assertNotIn(p.owner_token, json.dumps(context))
            decision = self.decision([{'action':'wait'}])
            w.release_actor_execution(p)
            captured['permit'] = w.acquire_actor_execution(PERSON, 'two')
            return decision
        self.error('ACTOR_EXECUTION_FENCED', lambda: dm.resolve(PERSON, 'provider-late', '等待', provider, execution=p))
        self.assertEqual(w.snapshot()['status'], 'running')
        self.assertEqual(dm.history(PERSON, 'provider-late'), [])
        self.assertIsNone(w.receipt(PERSON, 'provider-late'))
        q = captured['permit']
        receipt = dm.resolve(PERSON, 'provider-late', '等待', lambda context: self.decision([{'action':'wait'}]), execution=q)
        def never(context):
            self.fail('committed original ID must not recall provider')
        self.assertEqual(dm.resolve(PERSON, 'provider-late', '等待', never, execution=q), receipt)
        self.error('ACTOR_EXECUTION_FENCED', lambda: dm.resolve(PERSON, 'provider-late', '等待', never, execution=p))
        def failing(context):
            w.release_actor_execution(q)
            captured['permit'] = w.acquire_actor_execution(PERSON, 'three')
            raise RuntimeError('provider failed after losing ownership')
        before = w.snapshot()
        self.error('ACTOR_EXECUTION_FENCED', lambda: dm.resolve(PERSON, 'failure-late', '等待', failing, execution=q))
        self.assertEqual(w.snapshot(), before)
        self.assertEqual(dm.history(PERSON, 'failure-late'), [])
        r = captured['permit']
        self.error('ADJUDICATION_UNSUPPORTED', lambda: dm.resolve(PERSON, 'unsupported', '未知', lambda context: self.decision([{'action':'teleport'}]), execution=r))
        self.assertEqual(w.snapshot()['diagnostic']['code'], 'ADJUDICATION_UNSUPPORTED')

    def test_compound_dm_uses_parent_permit_for_every_real_transaction(self):
        w = self.clock_world()
        dm = Adjudicator(w)
        p = w.acquire_actor_execution(PERSON, 'one')
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: dm.propose(PERSON, 'compound', '调粮并等待'))
        dm.propose(PERSON, 'compound', '调粮并等待', execution=p)
        decision = self.decision([{'action':'ship','to':'place-1','quantity':10}, {'action':'wait'}])
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: dm.record(PERSON, 'compound', decision))
        version = dm.record(PERSON, 'compound', decision, execution=p)
        before = self.path.read_bytes()
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: dm.preview(PERSON, 'compound', version))
        self.assertEqual(dm.preview(PERSON, 'compound', version, execution=p)['effects'][0]['implemented'], True)
        self.assertEqual(self.path.read_bytes(), before)
        w.release_actor_execution(p)
        q = w.acquire_actor_execution(PERSON, 'two')
        state = w.snapshot()
        self.error('ACTOR_EXECUTION_FENCED', lambda: dm.commit(PERSON, 'compound', version, execution=p))
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: dm.commit(PERSON, 'compound', version))
        self.assertEqual(w.snapshot(), state)
        self.assertIsNone(w.receipt(PERSON, 'compound'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 1)
        receipt = dm.commit(PERSON, 'compound', version, execution=q)
        after = w.snapshot()
        self.assertEqual(after['places'][0]['grain'], state['places'][0]['grain']-10)
        self.assertEqual(dm.commit(PERSON, 'compound', version, execution=q), receipt)
        self.error('ACTOR_EXECUTION_FENCED', lambda: dm.commit(PERSON, 'compound', version, execution=p))
        self.assertEqual(w.snapshot(), after)
        self.assertNotIn(q.owner_token, json.dumps(receipt)+json.dumps(dm.trace())+json.dumps(dm.history(PERSON,'compound')))

    def test_technical_pause_requires_world_protection_even_after_release(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        before = w.snapshot()
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: w.technical_pause('LATE'))
        w.release_actor_execution(p)
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: w.technical_pause('LATE'))
        self.error('ACTOR_EXECUTION_FENCED', lambda: w.technical_pause('LATE', execution=p))
        q = w.acquire_actor_execution(PERSON, 'two', ttl_seconds=1)
        self.now += 1000000000
        self.error('ACTOR_EXECUTION_EXPIRED', lambda: w.technical_pause('LATE', execution=q))
        self.assertEqual(w.snapshot(), before)
        r = w.acquire_actor_execution(PERSON, 'three')
        self.assertEqual(w.technical_pause('VALID', execution=r)['status'], 'technical_paused')

    def test_preview_is_readonly_but_requires_live_permit(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        before = self.path.read_bytes()
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: w.preview(PERSON, 'preview', {'action':'wait'}))
        preview = w.preview(PERSON, 'preview', {'action':'wait'}, execution=p)
        self.assertEqual(preview['status'], 'preview')
        self.assertEqual(self.path.read_bytes(), before)
        self.now += 1000000000
        self.error('ACTOR_EXECUTION_EXPIRED', lambda: w.preview(PERSON, 'preview', {'action':'wait'}, execution=p))
        self.assertEqual(self.path.read_bytes(), before)
        self.now -= 1000000001
        self.error('EXECUTION_CLOCK_UNSAFE', lambda: w.preview(PERSON, 'preview', {'action':'wait'}, execution=p))
        self.assertEqual(self.path.read_bytes(), before)

    def test_effect_transaction_rechecks_clock_before_commit_and_rolls_back(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        before = w.snapshot()
        apply = w._apply_action
        def expire_after_effect(*args):
            result = apply(*args)
            self.now += 1000000000
            return result
        w._apply_action = expire_after_effect
        self.error('ACTOR_EXECUTION_EXPIRED', lambda: w.submit(PERSON, 'late', {'action':'ship','to':'place-1','quantity':10}, execution=p))
        self.assertEqual(w.snapshot(), before)
        self.assertIsNone(w.receipt(PERSON, 'late'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone()[0], 1)
        w._apply_action = apply
        q = w.acquire_actor_execution(PERSON, 'two')
        receipt = w.submit(PERSON, 'late', {'action':'ship','to':'place-1','quantity':10}, execution=q)
        self.assertEqual(w.submit(PERSON, 'late', {'action':'ship','to':'place-1','quantity':10}, execution=q), receipt)
        self.assertEqual(w.snapshot()['places'][0]['grain'], before['places'][0]['grain']-10)

    def test_submit_requires_registered_actor_and_checks_receipt_retry(self):
        w = self.clock_world()
        p = w.acquire_actor_execution(PERSON, 'one')
        before = w.snapshot()
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: w.submit(PERSON, 'missing', {'action':'wait'}))
        self.assertEqual(w.snapshot(), before)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM intents WHERE id='missing'").fetchone())
        self.error('EXECUTION_PERSON_MISMATCH', lambda: w.submit('person-country-1-king', 'wrong', {'action':'wait'}, execution=p))
        self.error('EXECUTION_WORLD_MISMATCH', lambda: w.submit(PERSON, 'foreign', {'action':'wait'}, execution=replace(p, world_id='other')))
        self.error('INVALID_EXECUTION', lambda: w.submit(PERSON, 'bad', {'action':'wait'}, execution=replace(p, epoch=True)))
        receipt = w.submit(PERSON, 'once', {'action':'ship', 'to':'place-1', 'quantity':10}, execution=p)
        w.release_actor_execution(p)
        self.error('ACTOR_EXECUTION_REQUIRED', lambda: w.submit(PERSON, 'released', {'action':'wait'}))
        self.error('ACTOR_EXECUTION_FENCED', lambda: w.submit(PERSON, 'once', {'action':'ship', 'to':'place-1', 'quantity':10}, execution=p))
        q = w.acquire_actor_execution(PERSON, 'two')
        committed = w.snapshot()
        self.assertEqual(w.submit(PERSON, 'once', {'action':'ship', 'to':'place-1', 'quantity':10}, expected_version={'world_id':q.world_id,'revision':1}, execution=q), receipt)
        self.assertEqual(w.receipt(PERSON, 'once'), receipt)
        self.assertEqual(w.snapshot(), committed)
        # Unregistered people keep trusted legacy APIs, but a fabricated permit never does.
        w.submit('person-country-1-king', 'legacy', {'action':'wait'})
        self.error('ACTOR_EXECUTION_FENCED', lambda: w.submit('person-country-1-king', 'fabricated', {'action':'wait'}, execution=replace(q, person_id='person-country-1-king')))
        for hidden in ('owner_token','owner_instance','epoch','expires_wall_ns'):
            self.assertNotIn(hidden, json.dumps(w.snapshot())+json.dumps(receipt)+json.dumps(w.observe(PERSON)))

    def test_acquire_identity_epoch_overflow_and_bounded_sql_busy(self):
        w = self.clock_world()
        for owner in ('', ' ', True, 42, None):
            self.error('INVALID_EXECUTION', lambda: w.acquire_actor_execution(PERSON, owner))
        self.error('UNKNOWN_PERSON', lambda: w.acquire_actor_execution('country-1', 'one'))
        p = w.acquire_actor_execution(PERSON, 'one')
        with closing(sqlite3.connect(self.path, timeout=0.25)) as db:
            db.execute('BEGIN IMMEDIATE')
            for call in (lambda: w.acquire_actor_execution(PERSON, 'two'),
                         lambda: w.renew_actor_execution(p), lambda: w.release_actor_execution(p)):
                self.error('EXECUTION_STORAGE_BUSY', call)
            db.rollback()
            db.execute('UPDATE actor_execution_leases SET epoch=?,owner_token=NULL', (2**63-1,))
            db.commit()
        self.error('ACTOR_EXECUTION_FENCED', lambda: w.acquire_actor_execution(PERSON, 'two'))
        # A forged current identity cannot replace first-snapshot identity.
        with closing(sqlite3.connect(self.path)) as db:
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            state['revision'] += 1
            state['world_id'] = 'forged-world'
            db.execute('INSERT INTO snapshots VALUES (?,?)', (state['revision'], json.dumps(state)))
            db.commit()
        self.error('EXECUTION_WORLD_MISMATCH', lambda: w.acquire_actor_execution('person-country-1-king', 'two'))

    def test_renew_expiry_release_are_persistent_owner_cas(self):
        w = self.clock_world()
        self.assertTrue(callable(getattr(w, 'renew_actor_execution', None)), 'renew is missing')
        self.assertTrue(callable(getattr(w, 'release_actor_execution', None)), 'release is missing')
        p = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        self.error('ACTOR_EXECUTION_BUSY', lambda: w.acquire_actor_execution(PERSON, 'one'))
        self.now += 500000000
        self.assertEqual(w.renew_actor_execution(p, ttl_seconds=1), p)
        self.now += 1000000000
        self.error('ACTOR_EXECUTION_EXPIRED', lambda: w.renew_actor_execution(p))
        q = w.acquire_actor_execution(PERSON, 'two')
        self.assertEqual(q.epoch, p.epoch+1)
        self.assertNotEqual(q.owner_token, p.owner_token)
        self.assertFalse(w.release_actor_execution(p))
        self.error('ACTOR_EXECUTION_FENCED', lambda: w.renew_actor_execution(p))
        self.now = 0  # release is intentionally clock-independent CAS
        self.assertTrue(w.release_actor_execution(q))
        self.assertFalse(w.release_actor_execution(q))
        self.error('EXECUTION_CLOCK_UNSAFE', lambda: w.acquire_actor_execution(PERSON, 'three'))
        self.now = 2000000000000
        r = w.acquire_actor_execution(PERSON, 'three')
        self.assertEqual(r.epoch, 3)
        for bad in (None, {}, replace(r, epoch=True), replace(r, owner_token='')):
            self.error('INVALID_EXECUTION', lambda: w.renew_actor_execution(bad))
        self.error('ACTOR_EXECUTION_FENCED', lambda: w.renew_actor_execution(replace(r, person_id='person-country-1-king')))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM actor_execution_leases').fetchone()[0], 1)

    def test_ttl_and_clock_are_strict_bounded_and_persist_floor(self):
        w = self.clock_world()
        for ttl in (True, False, 0, -1, 3601, 1.0, '60'):
            self.error('INVALID_EXECUTION_TTL', lambda: w.acquire_actor_execution(PERSON, 'one', ttl_seconds=ttl))
        for now in (True, 0, -1, 1.5, '1', 2**63, 2**63-1):
            self.now = now
            self.error('EXECUTION_CLOCK_UNSAFE', lambda: w.acquire_actor_execution(PERSON, 'one'))
        self.now = 1000000000000
        permit = w.acquire_actor_execution(PERSON, 'one', ttl_seconds=1)
        self.assertNotIn(permit.owner_token, repr(permit))
        with self.assertRaises(FrozenInstanceError):
            permit.epoch = 2
        self.now -= 1
        self.error('EXECUTION_CLOCK_UNSAFE', lambda: w.acquire_actor_execution('person-country-1-king', 'two'))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute('SELECT high_water_wall_ns FROM execution_clock').fetchone()[0], 1000000000000)
