"""Independent R1 regressions. Real Host producers and scratch SQLite only."""
from contextlib import closing
from pathlib import Path
import sqlite3
import unittest
import uuid
from unittest.mock import patch
from app.tests import test_technical_maintenance as fixture
from app.world import WorldError

PROOFS=[]

class IssuerTests(unittest.TestCase):
    def test_original_epoch_token_attempt_corruption_rejected_before_issuance(self):
        for fields in (('owner_epoch',),('owner_token',),('attempt_id',),('owner_epoch','owner_token','attempt_id')):
            with self.subTest(fields=fields),fixture.blocked_case('r1-issuer-'+ '-'.join(fields)) as c:
                w,h=c['world'],c['host']
                self.assertEqual(w.snapshot()['status'],'technical_paused','real blocked producer precondition; an earlier pause failure is not issuer GREEN')
                with closing(sqlite3.connect(w.db_path)) as db:
                    self.assertEqual(db.execute('SELECT phase,resume_phase,private_closed FROM coordinator_activations WHERE activation_id=?',(c['ref'].activation_id,)).fetchone(),('technical_blocked','world_committed',0))
                with closing(sqlite3.connect(h.db_path)) as db:
                    db.row_factory=sqlite3.Row
                    original=dict(db.execute('SELECT * FROM actor_intent_execution').fetchone())
                    changed={k:(original[k]+1000 if k=='owner_epoch' else uuid.uuid4().hex) for k in fields}
                    for k,v in changed.items():
                        db.execute('UPDATE actor_intent_execution SET '+k+'=? WHERE intent_id=?',(v,original['intent_id']))
                    db.commit()
                before=(fixture.logical(w.db_path),fixture.logical(h.db_path))
                error=None;result=None
                try:result=fixture.perform(c)
                except WorldError as exc:error=exc.code
                PROOFS.append(dict(case='issuer-corruption',fields=fields,code=error,result=result,db_root=str(c['root']),activation_id=c['ref'].activation_id,intent_id=original['intent_id']))
                self.assertIsNotNone(error,'corrupt original issuer must not be signed as maintenance material')
                self.assertEqual((fixture.logical(w.db_path),fixture.logical(h.db_path)),before)
