"""Parent first B regression: a caller error after an actual terminal return."""
from contextlib import closing
from pathlib import Path
import json
import sqlite3
import tempfile
import unittest
from app.world import WorldStore
from app.actors import ActorHost
from app.server import Coordinator
from app.tests import test_actor_execution as request_fixtures

SCRATCH='C:/Users/We3q/AppData/Local/hermes/cache/scratch'

class PersistentDispatchACKTests(unittest.TestCase):
    def test_caller_after_actual_terminal_return_finishes_original_window_once(self):
        with tempfile.TemporaryDirectory(prefix='p023b4-parent-B-first-',dir=SCRATCH) as directory:
            root=Path(directory);world=WorldStore(root/'world.sqlite');world.initialize()
            host=ActorHost(world,root/'actors.sqlite');requests=[];returned=[];first=[]
            def request(payload):
                requests.append(payload['intent_id'])
                return request_fixtures.ActorExecutionTests.result(payload)
            host._request=request
            host._choose=lambda view,seen:({'action':'wait'},seen)
            actual=host.act
            def caller(*args,**kwargs):
                result=actual(*args,**kwargs)
                returned.append(kwargs['activation'])
                if len(returned)==1:
                    with closing(sqlite3.connect(world.db_path)) as db:
                        row=db.execute('SELECT activation_id,intent_id,phase,private_closed FROM coordinator_activations WHERE roster_index=0').fetchone()
                    self.assertEqual(row[2:],('completed',1))
                    self.assertEqual(row[1],requests[0])
                    self.assertEqual(row[0],returned[0].activation_id)
                    first.append(row)
                    raise RuntimeError('parent ordinary caller ACK lost after real Host terminal return')
                return result
            host.act=caller;owner=Coordinator(world,host)
            try:
                self.assertEqual(owner.start(),{'accepted':True})
                owner.worker.join(20)
                self.assertFalse(owner.worker.is_alive())
                self.assertIsNone(owner.error)
                self.assertEqual((world.snapshot()['time'],world.snapshot()['status']),(86400,'user_paused'))
                self.assertEqual(len(requests),12)
                self.assertEqual(len(set(requests)),12)
                self.assertEqual(requests.count(first[0][1]),1)
                self.assertEqual(len(returned),12)
                self.assertEqual(len({r.activation_id for r in returned}),12)
                with closing(sqlite3.connect(world.db_path)) as db:
                    self.assertEqual(db.execute('SELECT window_id,state FROM coordinator_windows').fetchall(),[(returned[0].window_id,'completed')])
                    self.assertEqual(db.execute('SELECT count(*) FROM coordinator_activations WHERE phase=? AND private_closed=1',('completed',)).fetchone(),(12,))
                    self.assertEqual(db.execute('SELECT activation_id,intent_id,phase,private_closed FROM coordinator_activations WHERE roster_index=0').fetchone(),first[0])
                    self.assertEqual(db.execute('SELECT count(*) FROM intents').fetchone(),(12,))
                with closing(sqlite3.connect(host.db_path)) as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM actor_intents WHERE state=?',('committed',)).fetchone(),(12,))
            finally:
                owner.close()
                self.assertFalse(owner.cleanup_pending)
                self.assertFalse(host.cleanup_pending)
                print('P023B4_B_PARENT_FIRST_FACTS '+json.dumps({'request_seam_entries':len(requests),'actual_host_returns':len(returned),'first_actual_terminal':first,'SDK':0,'coordinator_error':owner.error}))
