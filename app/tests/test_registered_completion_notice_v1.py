"""N1: real registered day windows; script only model responses, no SDK.
Fictional economy rates. Composition avoids rediscovering R's frozen 17 methods.
"""
import copy
import json
import unittest
from pathlib import Path
from app.tests import test_registered_production_runtime_v1 as runtime
from app.world import DAY


class RegisteredCompletionNoticeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = runtime.RegisteredProductionRuntimeTests('runTest')
        self.fixture.setUp()
        self.root = self.fixture.root
        self.views = []
        self.requests = []
        self.states = []

    def tearDown(self):
        self.fixture.save('N-views', self.views)
        self.fixture.save('N-requests', self.requests)
        self.fixture.save('N-states', self.states)
        self.fixture.tearDown()

    def build(self, travel=False):
        f = self.fixture
        c = runtime.configuration()
        c['economy']['recipes'][runtime.RID]['duration_seconds'] = DAY + 1
        effects = None
        if travel:
            self.destination = next(p['id'] for p in c['countries'][0]['places'] if p['id']!='place-0')
            effects = [runtime.production(), dict(action='travel', to=self.destination)]
        f.build(c=c, effects=effects)
        principal_turns = 0
        def choose(view, seen):
            nonlocal principal_turns
            f.counts['choose'] += 1
            self.views.append(copy.deepcopy(view))
            body = dict(action='wait')
            if view['person']['id'] == runtime.PID:
                principal_turns += 1
                if principal_turns == 1:
                    body = dict(action='propose', text=runtime.TEXT)
            return body, seen
        request = f.host._request
        def record_request(payload):
            self.requests.append(copy.deepcopy(payload))
            return request(payload)
        f.host._choose = choose
        f.host._request = record_request

    def window(self):
        f = self.fixture
        f.dispatch()
        self.assertIsNone(f.owner.error, 'real registered window must succeed')
        state = f.world.snapshot()
        self.states.append(copy.deepcopy(state))
        f.save('N-states', self.states)
        return state

    @staticmethod
    def completion_observations(view, due):
        return [o for o in view['observations'] if o['time'] == due
                and 'iron_tool' in o['text'] and 'tool_unit' in o['text']]

    def test_registered_three_windows_owner_learns_actual_completion(self):
        self.build()
        f = self.fixture
        first = self.window()
        task = first['economy']['tasks'][0]
        self.assertEqual((first['time'], task['started_at'], task['due']), (DAY, DAY, 2*DAY+1))
        self.assertEqual((task['status'], first['economy']['materials']['place-0']['iron_tool']), ('pending', 2))
        self.assertEqual(len(first['economy']['tasks']), 1)
        second = self.window()
        self.assertEqual(second['time'], 2*DAY)
        self.assertEqual(second['economy']['tasks'][0]['status'], 'pending')
        self.assertEqual(second['economy']['materials']['place-0']['iron_tool'], 2)
        self.assertFalse(self.completion_observations(f.world.observe(runtime.PID), task['due']))
        third = self.window()
        completed = third['economy']['tasks'][0]
        self.assertEqual(third['time'], 3*DAY)
        self.assertEqual((completed['status'], completed['completed_at']), ('completed', task['due']))
        self.assertEqual(completed['actual_outputs'], dict(iron_tool=1))
        self.assertEqual(completed['reserved_inputs'], {})
        self.assertEqual(completed['actual_labor_hours'], 8)
        self.assertEqual(third['economy']['materials']['place-0'], dict(iron=18,charcoal=9,timber=19,iron_tool=3))
        self.assertEqual(third['economy']['material_ledger']['consumed']['place-0'], dict(iron=2,charcoal=1,timber=1,iron_tool=0))
        self.assertEqual(third['economy']['material_ledger']['produced']['place-0']['iron_tool'], 1)
        self.assertEqual(len(third['economy']['tasks']), 1)
        self.assertFalse(any(q['kind']=='production' for q in third['queue']))
        self.assertEqual(len([e for e in third['events'] if e['kind']=='production_completed']), 1)
        self.assertEqual(f.counts['dm'], 1)
        owner_views = [v for v in self.views if v['person']['id']==runtime.PID]
        self.assertEqual([v['time'] for v in owner_views], [DAY, 2*DAY, 3*DAY])
        actual = self.completion_observations(owner_views[-1], task['due'])
        f.save('N-before-decisive-assertion', dict(task=completed, actual_view=owner_views[-1], actual_requests=self.requests, public=runtime.read_sql(f.world.db_path), private=runtime.read_sql(f.host.db_path)))
        self.assertEqual(len(actual), 1, 'MISSING_LOCAL_COMPLETION_NOTICE: actual completed output absent from owner normal _choose view')
        notice = actual[0]
        self.assertEqual(set(notice), {'id','time','text','source'})
        place = next(p for p in third['places'] if p['id']==task['place_id'])
        self.assertEqual(notice['source'], '亲见：'+place['name'])
        self.assertIn('1', notice['text'])
        for view in self.views:
            if view['person']['id']!=runtime.PID:
                self.assertNotIn(notice, view['observations'])
        request = [r for r in self.requests if r['person_id']==runtime.PID][-1]
        self.assertIn(notice['text'], request['prompt'])
        private = runtime.read_sql(f.host.db_path)
        leaf = next(r for r in private['actor_intents'] if r['intent_id']==request['intent_id'])
        self.assertIn(notice['text'], json.loads(leaf['request'])['prompt'])
        self.assertIn(notice['text'], json.dumps(json.loads(leaf['candidate']), ensure_ascii=False))
        public = runtime.read_sql(f.world.db_path)
        self.assertTrue(all(r['phase']=='completed' and r['private_closed']==1 for r in public['coordinator_activations']))
        self.assertEqual(len(public['coordinator_windows']), 3)
        head = next(r for r in private['actor_session_heads'] if r['person_id']==runtime.PID)
        self.assertEqual(head['version'], 3)

    def test_coverage_typed_departure_arrival_no_remote_completion(self):
        self.build(travel=True)
        f = self.fixture
        first = self.window()
        task = first['economy']['tasks'][0]
        person = lambda s: next(p for p in s['persons'] if p['id']==runtime.PID)
        self.assertIsNone(person(first)['location'])
        arrivals = [q for q in first['queue'] if q['kind']=='arrival' and q.get('person_id')==runtime.PID]
        self.assertEqual(len(arrivals), 1)
        self.assertEqual(arrivals[0]['destination'], self.destination)
        self.assertLess(arrivals[0]['at'], 2*DAY)
        second = self.window()
        self.assertEqual(person(second)['location'], self.destination)
        self.assertEqual(second['economy']['tasks'][0]['status'], 'pending')
        third = self.window()
        self.assertEqual(person(third)['location'], self.destination)
        self.assertNotEqual(self.destination, task['place_id'])
        self.assertEqual(third['economy']['tasks'][0]['status'], 'completed')
        self.assertEqual(third['economy']['tasks'][0]['actual_outputs'], dict(iron_tool=1))
        self.assertEqual(third['economy']['materials']['place-0']['iron_tool'], 3)
        self.assertEqual(len([e for e in third['events'] if e['kind']=='production_completed']), 1)
        self.assertFalse(any(q['kind']=='production' for q in third['queue']))
        for v in self.views:
            self.assertFalse(self.completion_observations(v, task['due']))
        for p in third['persons']:
            self.assertFalse(self.completion_observations(f.world.observe(p['id']), task['due']))
        self.assertEqual(f.counts['dm'], 1)
        f.save('N-remote-assertions', dict(task=task,arrival=arrivals[0],owner_after=person(third),public=runtime.read_sql(f.world.db_path),private=runtime.read_sql(f.host.db_path)))

    def test_coverage_same_physical_dual_db_reopen_replay_next_window_once(self):
        from app.world import WorldStore
        from app.actors import ActorHost
        from app.adjudication import Adjudicator
        from app.server import Coordinator
        self.build()
        f = self.fixture
        self.window(); self.window(); third = self.window()
        task = third['economy']['tasks'][0]
        notice = self.completion_observations(f.world.observe(runtime.PID), task['due'])
        self.assertEqual(len(notice), 1)
        original = f.activation()
        original_receipt = f.world.receipt(runtime.PID, original['intent_id'])
        counts = dict(f.counts)
        heads = runtime.read_sql(f.host.db_path)['actor_session_heads']
        identity = {Path(p).name: Path(p).stat().st_ino for p in (f.world.db_path, f.host.db_path)}
        choose, request, dm = f.host._choose, f.host._request, f.host.dm_provider
        old_world, old_host, old_owner = f.world, f.host, f.owner
        f.owner.close()
        self.assertFalse(f.owner.cleanup_pending); self.assertFalse(f.host.cleanup_pending)
        f.world = WorldStore(f.root/'public.sqlite', f.root/'scenario.json')
        f.host = ActorHost(f.world, f.root/'private.sqlite')
        def forbidden_worker():
            raise AssertionError('SDK worker forbidden')
        f.host._start_worker = forbidden_worker
        f.host._choose, f.host._request = choose, request
        f.owner = Coordinator(f.world, f.host)
        self.assertIsNot(f.world, old_world); self.assertIsNot(f.host, old_host); self.assertIsNot(f.owner, old_owner)
        f.owner.acquire_owner()
        with f.owner._owner_scope():
            f.adjudicator = Adjudicator(f.world)
        f.host.adjudicator, f.host.dm_provider = f.adjudicator, dm
        f.owner._prepare_scheduler(f.owner._owner_attempt)
        self.assertEqual({Path(p).name:Path(p).stat().st_ino for p in (f.world.db_path,f.host.db_path)}, identity)
        self.assertEqual(f.replay()['receipt'], original_receipt)
        self.assertEqual(f.counts, counts)
        self.assertEqual(f.world.snapshot(), third)
        self.assertEqual(runtime.read_sql(f.host.db_path)['actor_session_heads'], heads)
        fourth = self.window()
        self.assertEqual(fourth['time'], 4*DAY)
        self.assertEqual(fourth['economy'], third['economy'])
        self.assertEqual(self.completion_observations(f.world.observe(runtime.PID), task['due']), notice)
        self.assertEqual(len([e for e in fourth['events'] if e['kind']=='production_completed']), 1)
        self.assertEqual(f.counts['dm'], 1)
        self.assertEqual(f.replay()['receipt'], original_receipt)
        self.assertEqual(self.completion_observations(f.world.observe(runtime.PID), task['due']), notice)
        f.save('N-reopen-assertions', dict(original_activation=original,original_receipt=original_receipt,identities=identity,public=runtime.read_sql(f.world.db_path),private=runtime.read_sql(f.host.db_path)))

    def test_coverage_notice_failure_rolls_back_original_clock_transaction(self):
        self.build()
        f = self.fixture
        self.window(); before = self.window()
        task = before['economy']['tasks'][0]
        delegated = f.world._notice
        attempted = []
        def fail_completion(state, person_id, text, source):
            if 'iron_tool' in text and 'tool_unit' in text:
                attempted.append(dict(time=state['time'],person_id=person_id,text=text,source=source))
                raise RuntimeError('N1 injected notice persistence failure')
            return delegated(state, person_id, text, source)
        f.world._notice = fail_completion
        try:
            f.dispatch()
        finally:
            f.world._notice = delegated
        after = f.world.snapshot()
        self.assertEqual(len(attempted), 1)
        self.assertEqual((attempted[0]['time'],attempted[0]['person_id']), (task['due'],runtime.PID))
        self.assertEqual(f.owner.error, 'HOST_FAILURE')
        self.assertEqual(after['status'], 'technical_paused')
        self.assertEqual(after['time'], before['time'])
        self.assertEqual(after['economy'], before['economy'])
        self.assertEqual(after['queue'], before['queue'])
        self.assertFalse(any(e['kind']=='production_completed' for e in after['events']))
        self.assertFalse(self.completion_observations(f.world.observe(runtime.PID), task['due']))
        f.save('N-rollback-assertions', dict(attempted=attempted,before=before,after=after,public=runtime.read_sql(f.world.db_path),private=runtime.read_sql(f.host.db_path)))
