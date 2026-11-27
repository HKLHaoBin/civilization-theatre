"""P022 server/real Host authority error routing, no HTTP/model."""
from pathlib import Path
import tempfile
import unittest
from app.actors import ActorHost
from app.world import WorldStore, WorldError
from app.server import Coordinator
from app.tests.test_actor_execution import SCRATCH


class ProtectedServerTests(unittest.TestCase):
    def test_c_fenced_host_error_never_pauses_or_controls_successor(self):
        with tempfile.TemporaryDirectory(prefix='p022-server-fenced-',dir=SCRATCH) as temp:
            root=Path(temp);clock=[100000000000];world=WorldStore(root/'world.sqlite',execution_clock_ns=lambda:clock[0]);world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');calls=[];successor=[];facts=[]
            world.technical_pause=lambda *a,**k:calls.append(('pause',k))
            control=world.control
            def control_spy(*a,**k):calls.append(('control',None));return control(*a,**k)
            world.control=control_spy
            def request(payload):
                clock[0]=161000000000;successor.append(world.acquire_actor_execution(payload['person_id'],'new-owner'))
                facts.append(world.snapshot());raise WorldError('ACTOR_EXECUTION_FENCED','old callback')
            host._request=request;coordinator=Coordinator(world,host)
            try:
                coordinator._step();self.assertEqual(calls,[]);self.assertEqual(coordinator.error,'ACTOR_EXECUTION_FENCED')
                self.assertTrue(coordinator.stop.is_set());self.assertEqual(world.snapshot(),facts[0])
            finally:
                for permit in successor:world.release_actor_execution(permit)
                coordinator.close()

    def test_c_outer_protected_coordinator_failure_has_no_authority_to_pause(self):
        with tempfile.TemporaryDirectory(prefix='p022-server-outer-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');calls=[];before=world.snapshot()
            def fail(*a):raise RuntimeError('outer coordinator failure')
            world.advance=fail;world.technical_pause=lambda *a,**k:calls.append(True)
            coordinator=Coordinator(world,host)
            try:
                coordinator._step();self.assertEqual(calls,[]);self.assertEqual(world.snapshot(),before);self.assertEqual(coordinator.error,'HOST_FAILURE')
            finally:coordinator.close()

    def test_coordinator_close_cancels_protected_host_before_joining_pipeline(self):
        import threading,time
        with tempfile.TemporaryDirectory(prefix='p022-coordinator-close-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');entered=threading.Event();release=threading.Event()
            from app.tests.test_actor_execution import ActorExecutionTests
            def request(payload):entered.set();release.wait(1);return ActorExecutionTests.result(payload)
            host._request=request;coordinator=Coordinator(world,host)
            coordinator.worker=threading.Thread(target=coordinator._step);coordinator.worker.start();self.assertTrue(entered.wait(2));start=time.monotonic()
            try:
                coordinator.close();self.assertLess(time.monotonic()-start,.7,'server close must cancel Host before waiting on its work')
                self.assertFalse(coordinator.worker.is_alive());self.assertEqual(coordinator.error,'ACTOR_EXECUTION_CANCELLED')
            finally:release.set();coordinator.worker.join(2);host.close()

    def test_protected_host_pauses_once_with_original_live_permit(self):
        with tempfile.TemporaryDirectory(prefix='p022-server-',dir=SCRATCH) as temp:
            root=Path(temp);world=WorldStore(root/'world.sqlite');world.initialize();world.control('resume')
            host=ActorHost(world,root/'actors.sqlite');calls=[];original=world.technical_pause
            def pause(code,**kwargs):calls.append(kwargs.get('execution'));return original(code,**kwargs)
            world.technical_pause=pause
            def request(payload):raise RuntimeError('injected provider failure (not public text)')
            host._request=request;coordinator=Coordinator(world,host)
            try:
                coordinator._step()
                self.assertEqual(len(calls),1);self.assertIsNotNone(calls[0],'Host must pause BEFORE releasing its original permit')
                self.assertEqual(world.snapshot()['status'],'technical_paused')
                self.assertTrue(coordinator.stop.is_set());self.assertEqual(coordinator.error,'HOST_FAILURE')
            finally:coordinator.close()
