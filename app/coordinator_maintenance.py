"""Trusted operator-only private closure. No HTTP, provider or business dispatch.

Repair hashes bind an operator attestation to local bytes; they are not a proof
that arbitrary bugs were fixed. The actual native transaction must still pass.
"""
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
import hashlib
import json
import sqlite3
import uuid

from . import coordinator_scheduler as scheduler
from .actor_execution import Attempt, ActorRunError, PrivateStore, canonical, digest
from .execution import ExecutionPermit
from .world import WorldError, _history_cover

ROOT = Path(__file__).resolve().parents[1]
SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch').resolve()
CORE = ('app/actors.py','app/actor_execution.py','app/coordinator_scheduler.py','app/server.py','app/world.py')
_SCOPE = ContextVar('trusted_technical_maintenance', default=None)
_VERIFY_ENTRY = ContextVar('technical_maintenance_verification_entry', default=None)
_KEY = object()
_TABLE = 'coordinator_technical_maintenance'
_DDL = '''CREATE TABLE coordinator_technical_maintenance (
    operation_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, activation_id TEXT NOT NULL UNIQUE,
    record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version=1))'''
_INSERT = "CREATE TRIGGER technical_maintenance_no_replace BEFORE INSERT ON coordinator_technical_maintenance WHEN EXISTS (SELECT 1 FROM coordinator_technical_maintenance WHERE operation_id=NEW.operation_id OR activation_id=NEW.activation_id) BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"


def fail(code='TECHNICAL_MAINTENANCE_UNPROVEN'):
    raise WorldError(code, '受信维护材料或授权不可证明')


def file_sha(path):
    entry=_VERIFY_ENTRY.get()
    result=hashlib.sha256()
    if entry is not None:entry.check()
    with path.open('rb') as stream:
        while True:
            if entry is not None:entry.check()
            chunk=stream.read(65536)
            if not chunk:break
            result.update(chunk)
    if entry is not None:entry.check()
    return result.hexdigest()


@contextmanager
def verification_scope(world):
    entry=world._scoped_entry_budget()
    if entry is None:fail('TECHNICAL_MAINTENANCE_REQUIRED')
    token=_VERIFY_ENTRY.set(entry)
    try:
        entry.check()
        yield
    finally:
        _VERIFY_ENTRY.reset(token)


def uuid_hex(value):
    if type(value) is not str or len(value) != 32 or any(c not in '0123456789abcdef' for c in value):
        fail('INVALID_TECHNICAL_MAINTENANCE')


def evidence(value, *, _historical_source=None):
    if _IMPORTED_SHA != {n:file_sha(ROOT/n) for n in _IMPORTED_SHA}:
        fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
    if type(value) is not dict or set(value) != {'repair_id','reason','source_sha256','verification_refs'}:
        fail('INVALID_TECHNICAL_MAINTENANCE')
    # Serialize first: never retain caller-owned nested dictionaries.
    try:
        value = json.loads(canonical(value))
    except (TypeError, ValueError):
        fail('INVALID_TECHNICAL_MAINTENANCE')
    uuid_hex(value['repair_id'])
    if type(value['reason']) is not str or not value['reason'].strip():
        fail('INVALID_TECHNICAL_MAINTENANCE')
    sources=value['source_sha256']
    if type(sources) is not dict or not set(CORE) <= set(sources) or set(sources)-set((*CORE,'app/coordinator_maintenance.py')):
        fail('INVALID_TECHNICAL_MAINTENANCE')
    for name, expected in sources.items():
        # Only M2's already-proven immutable release supplies historical hashes.
        # Default M1 and every first effect still require current source bytes.
        if type(expected) is not str or (file_sha(ROOT/name) if _historical_source is None else _historical_source.get(name)) != expected:
            fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
    refs=value['verification_refs']
    if type(refs) is not list or not refs:
        fail('INVALID_TECHNICAL_MAINTENANCE')
    for ref in refs:
        if type(ref) is not dict or set(ref) != {'path','sha256'} or type(ref['path']) is not str or type(ref['sha256']) is not str:
            fail('INVALID_TECHNICAL_MAINTENANCE')
        path=Path(ref['path']).resolve()
        if not any(path.is_relative_to(base) for base in (ROOT/'app/evidence',SCRATCH)) or not path.is_file():
            fail('INVALID_TECHNICAL_MAINTENANCE')
        if file_sha(path) != ref['sha256']:
            fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
    return value


def schema(db, *, create=False):
    if not scheduler.exists(db,_TABLE):
        if not create:
            return False
        db.execute(_DDL)
        for event in ('UPDATE','DELETE'):
            db.execute('CREATE TRIGGER technical_maintenance_no_'+event.lower()+' BEFORE '+event+' ON '+_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END")
        db.execute(_INSERT)
        _history_cover(db)
    actual=db.execute("SELECT sql FROM sqlite_master WHERE name=?",(_TABLE,)).fetchone()[0]
    if actual != _DDL:
        fail()
    for event in ('UPDATE','DELETE'):
        expected='CREATE TRIGGER technical_maintenance_no_'+event.lower()+' BEFORE '+event+' ON '+_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"
        found=db.execute('SELECT sql FROM sqlite_master WHERE name=?',('technical_maintenance_no_'+event.lower(),)).fetchone()
        if found is None or found[0] != expected:
            fail()
    found=db.execute("SELECT sql FROM sqlite_master WHERE name='technical_maintenance_no_replace'").fetchone()
    if found is None or found[0] != _INSERT:
        fail()
    return True


def completed_proof(world, db, row):
    """Re-prove original witness/issuer without pretending private is unpromoted."""
    if not schema(db):
        fail()
    saved=db.execute('SELECT * FROM '+_TABLE+' WHERE world_id=? AND activation_id=?',(row['world_id'],row['activation_id'])).fetchone()
    if saved is None or saved['schema_version'] != 1 or digest(saved['record_json']) != saved['record_sha256']:
        fail()
    record=json.loads(saved['record_json']);old=record['original_row']
    if old['phase'] != 'technical_blocked' or old['resume_phase'] != 'world_committed':
        fail()
    changed={'phase','private_closed','promoted_version','resume_phase','diagnostic_code'}
    if set(old) != set(row.keys()) or any(old[k] != row[k] for k in old if k not in changed):
        fail()
    scheduler.block_witness_proof(world,db,old)
    if scheduler.fault_issuer_proof(world,db,old) != record.get('original_fault_issuer'):
        fail()
    block=dict(db.execute('SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',(row['world_id'],row['activation_id'])).fetchone())
    if block != record['original_block'] or record['operation_id'] != saved['operation_id']:
        fail()
    scheduler.material_proof(world,db,row,'completed')
    return record


def runtime_sources(coordinator, root, host, storage, entry, ref):
    from .source_attestation import attest, SourceMismatch
    try:
        return attest(ROOT,coordinator,root,root.permit,host,storage,coordinator._authority,entry,ref,
                      Attempt,ExecutionPermit,scheduler.scoped,recover,check=entry.check)
    except (SourceMismatch,OSError,ValueError):
        fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')


class _Grant:
    __slots__=('coordinator','world','host','storage','root','ref','entry','material','repair','operation','source_sha','actor','attempt','used','finishing')
    def __init__(self, key, coordinator, root, ref, entry, material, repair, operation):
        if key is not _KEY:
            fail('TECHNICAL_MAINTENANCE_REQUIRED')
        self.coordinator=coordinator;self.world=coordinator._authority;self.host=coordinator.actors
        self.storage=self.host._storage;self.root=root;self.ref=ref;self.entry=entry
        self.material=material;self.repair=canonical(repair);self.operation=operation
        self.source_sha=runtime_sources(coordinator,root,self.host,self.storage,entry,ref)
        self.actor=None;self.attempt=None;self.used=False;self.finishing=False

    def check(self):
        self.entry.check();self.root.check()
        c=self.coordinator;h=self.host;w=self.world
        if (c._closed.is_set() or h._closing.is_set() or h._closed):
            fail('COORDINATOR_EXECUTION_CANCELLED')
        if (c._authority is not w or c.world is not w or c.actors is not h or c._owner_attempt is not self.root
                or h._storage is not self.storage or self.storage.world is not w or self.storage.host is not h
                or self.root.world is not w or h.world is not w or h._authority is not w or w._scoped_coordinator_attempt() is not self.root
                or w._scoped_entry_budget() is not self.entry or scheduler.scoped(w) != self.ref):
            fail('ACTIVATION_BINDING_CONFLICT')
        self.storage.paths()
        if self.source_sha != runtime_sources(c,self.root,h,self.storage,self.entry,self.ref):
            fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
        if canonical(evidence(json.loads(self.repair))) != self.repair:
            fail()


@contextmanager
def grant_scope(grant):
    if type(grant) is not _Grant:
        fail('TECHNICAL_MAINTENANCE_REQUIRED')
    token=_SCOPE.set(grant)
    try:
        grant.check()
        yield grant
    finally:
        _SCOPE.reset(token)


def actor_authorized(world, reference, execution):
    grant=_SCOPE.get()
    if grant is None:
        return False
    grant.check()
    if grant.world is not world or grant.ref != reference or execution != grant.actor or grant.attempt is not world._scoped_attempt():
        fail('ACTIVATION_BINDING_CONFLICT')
    return True


class _ReplayIdentity:
    """Read-only return capsule. No effect grant, permit, lease or new budget."""
    __slots__=('coordinator','world','host','storage','root','permit','entry','ref','row','state','paths','identity','source_sha','registration')
    def __init__(self,coordinator,world,host,storage,root,entry,ref,row,state,identity,db):
        self.coordinator=coordinator;self.world=world;self.host=host;self.storage=storage
        self.root=root;self.permit=root.permit;self.entry=entry;self.ref=ref
        self.row=canonical(dict(row));self.state=canonical(state)
        self.paths=(storage.world_path,storage.world_file,storage.actor_path,storage.actor_file)
        self.identity=tuple(identity)
        self.source_sha=runtime_sources(coordinator,root,host,storage,entry,ref)
        self.registration=canonical([dict(r) for r in db.execute('SELECT * FROM coordinator_scheduler_registration')])

    def check(self,db,row):
        c=self.coordinator;w=self.world;h=self.host;s=self.storage;root=self.root;entry=self.entry
        entry.check();root.check()
        if c._closed.is_set() or h._closing.is_set() or h._closed:
            fail('COORDINATOR_EXECUTION_CANCELLED')
        if (c._authority is not w or c.world is not w or c.actors is not h or c._owner_attempt is not root
                or root.world is not w or root.permit is not self.permit or h._authority is not w or h.world is not w
                or h._storage is not s or s.world is not w or s.host is not h
                or h._bound_world_id != self.ref.world_id or w._scoped_coordinator_attempt() is not root
                or w._scoped_entry_budget() is not entry or scheduler.scoped(w) != self.ref
                or (s.world_path,s.world_file,s.actor_path,s.actor_file) != self.paths):
            fail('ACTIVATION_BINDING_CONFLICT')
        s.paths()
        actual,identity=scheduler.source(w)
        if tuple(identity) != self.identity or actual.actor_path != s.actor_path:
            fail('ACTIVATION_BINDING_CONFLICT')
        scheduler.registration(db,self.ref.world_id,identity);scheduler.private_binding(db,self.ref.world_id)
        if self.registration != canonical([dict(r) for r in db.execute('SELECT * FROM coordinator_scheduler_registration')]):
            fail('ACTIVATION_BINDING_CONFLICT')
        if self.source_sha != runtime_sources(c,root,h,s,entry,self.ref):
            fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
        state=scheduler.latest(db)
        fresh=db.execute('SELECT * FROM coordinator_activations WHERE world_id=? AND activation_id=?',(self.ref.world_id,self.ref.activation_id)).fetchone()
        if (fresh is None or self.row != canonical(dict(fresh)) or self.row != canonical(dict(row))
                or self.state != canonical(state) or state['status'] != 'technical_paused'):
            fail('ACTIVATION_BINDING_CONFLICT')
        # Preserve the original actual root/entry remainder through the LAST gate.
        w._check_coordinator_execution(db,self.ref.world_id,self.permit,readonly=True)
        entry.check();root.check()


def backend(coordinator, root, host, storage):
    from .actors import ActorHost
    if root.world is not coordinator._authority:
        fail('ACTIVATION_BINDING_CONFLICT')
    if (type(host) is not ActorHost or type(storage) is not PrivateStore or coordinator.backend_kind != 'current'
            or coordinator.world is not coordinator._authority or host.world is not coordinator.world
            or host._authority is not coordinator._authority or storage.world is not coordinator._authority
            or storage.host is not host or host._storage is not storage or coordinator._owner_attempt is not root):
        fail('SCHEDULER_BACKEND_UNSUPPORTED')


def material(world, db, host, storage, ref):
    state=scheduler.latest(db)
    world._check_coordinator_execution(db,state['world_id'],None,readonly=True)
    actual,identity=scheduler.source(world)
    if identity != [str(storage.actor_path),*storage.actor_file] or actual.actor_path != storage.actor_path:
        fail('ACTIVATION_BINDING_CONFLICT')
    scheduler.registration(db,state['world_id'],identity);scheduler.private_binding(db,state['world_id'])
    row=scheduler.activation_row(world,db,state,ref.person_id,ref)
    if state['status'] != 'technical_paused' or row['phase'] != 'technical_blocked' or row['resume_phase'] != 'world_committed' or row['private_closed'] != 0:
        fail('TECHNICAL_MAINTENANCE_UNSUPPORTED_PHASE')
    scheduler.phase_proof(world,db,row);scheduler.native_audit(storage,db,row)
    storage.check_base(db,row['intent_id'],ref.person_id)
    block=dict(db.execute('SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',(ref.world_id,ref.activation_id)).fetchone())
    private=dict(db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=?',(row['intent_id'],)).fetchone())
    meta=dict(db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?',(row['intent_id'],)).fetchone())
    issuer=scheduler.fault_issuer_proof(world,db,row)
    head=dict(storage.head(db,ref.person_id))
    payload=dict(row=row,block=block,private=private,meta=meta,issuer=issuer,head=head,state_sha=scheduler.sha(state),identity=identity)
    return payload


def acquire_actor(grant):
    """Issue metadata only; never claim/adopt or rewrite the historical owner."""
    grant.check();w=grant.world;h=grant.host;ref=grant.ref
    duration=w._execution_ttl(h.execution_policy.ttl_seconds)
    with w._connection() as db:
        db.row_factory=sqlite3.Row
        db.execute('ATTACH DATABASE ? AS actor',(grant.storage.actor_path.as_uri()+'?mode=ro',))
        db.execute('BEGIN IMMEDIATE')
        try:
            if material(w,db,h,grant.storage,ref) != grant.material:
                fail()
            now=w._execution_now(db,duration)
            old=db.execute('SELECT epoch,owner_token,expires_wall_ns FROM actor_execution_leases WHERE world_id=? AND person_id=?',(ref.world_id,ref.person_id)).fetchone()
            # No takeover of a live child, even if it belonged to an older root.
            if old is None or type(old['epoch']) is not int or old['epoch'] >= 2**63-1:
                fail()
            if old['owner_token'] is not None and now < old['expires_wall_ns']:
                fail('ACTOR_EXECUTION_BUSY')
            permit=ExecutionPermit(ref.world_id,ref.person_id,uuid.uuid4().hex,old['epoch']+1)
            changed=db.execute('UPDATE actor_execution_leases SET epoch=?,owner_token=?,owner_instance=?,acquired_wall_ns=?,expires_wall_ns=?,last_wall_ns=? WHERE world_id=? AND person_id=? AND epoch=? AND owner_token IS ?',
                (permit.epoch,permit.owner_token,h._owner_instance,now,now+duration,now,ref.world_id,ref.person_id,old['epoch'],old['owner_token'])).rowcount
            if changed != 1:fail('ACTOR_EXECUTION_FENCED')
            db.execute('INSERT INTO actor_coordinator_delegations VALUES (?,?,?,?,?)',(ref.world_id,ref.person_id,permit.epoch,grant.root.permit.epoch,grant.root.permit.owner_token))
            db.execute('INSERT INTO actor_activation_delegations VALUES (?,?,?,?,?)',(ref.world_id,ref.person_id,permit.epoch,ref.window_id,ref.activation_id))
            w._execution_floor(db,now);grant.check()
            # Re-read after all issuance work, in this same write transaction.
            # A preflight/material cache must not commit a laundered claim row.
            if material(w,db,h,grant.storage,ref) != grant.material:
                fail()
            grant.check()
            w._check_coordinator_execution(db,ref.world_id,grant.root.permit)
            db.commit()
            return permit
        except BaseException:
            db.rollback();raise


def original_base(storage, db, intent, person):
    grant=_SCOPE.get()
    if type(grant) is not _Grant or grant.storage is not storage or not grant.finishing:
        fail('TECHNICAL_MAINTENANCE_REQUIRED')
    grant.check()
    issuer=scheduler.fault_issuer_proof(grant.world,db,grant.material['row'])
    if issuer != grant.material['issuer']:
        fail()
    meta=issuer['meta']
    original=ExecutionPermit(meta['world_id'],meta['person_id'],meta['owner_token'],meta['owner_epoch'])
    return storage.check_base(db,intent,person,original,meta['attempt_id'])


def begin_finish(storage, db, intent, person, execution, attempt_id):
    """Only the real Host -> PrivateStore.finish consumer may use this grant."""
    grant=_SCOPE.get()
    if type(grant) is not _Grant:
        return False
    grant.check()
    if (not grant.finishing or grant.used or grant.storage is not storage or grant.actor != execution
            or grant.attempt is not storage.world._scoped_attempt() or grant.attempt.id != attempt_id
            or grant.material['row']['intent_id'] != intent or grant.ref.person_id != person):
        fail('TECHNICAL_MAINTENANCE_REQUIRED')
    if material(grant.world,db,grant.host,storage,grant.ref) != grant.material:
        fail()
    schema(db,create=True)
    old=grant.material['row'];version=grant.material['head']['version']+1
    record=dict(operation_id=grant.operation,repair=json.loads(grant.repair),source_sha256=grant.source_sha,original_row=old,original_block=grant.material['block'],original_fault_issuer=grant.material['issuer'],material_sha256=scheduler.sha(grant.material),recovery_parent_epoch=grant.root.permit.epoch,recovery_actor_epoch=grant.actor.epoch,promoted_version=version)
    raw=canonical(record)
    db.execute('INSERT INTO '+_TABLE+' VALUES (?,?,?,?,?,1)',(grant.operation,grant.ref.world_id,grant.ref.activation_id,raw,digest(raw)))
    return True


def promote(grant):
    if type(grant) is not _Grant or _SCOPE.get() is not grant or grant.used:
        fail('TECHNICAL_MAINTENANCE_REQUIRED')
    grant.check();old=grant.material['row'];private=grant.material['private']
    # Exercise the original Host completion path. A still-broken finish must
    # fail here; never replace it with a separate SQL success implementation.
    grant.finishing=True
    try:
        with grant.world._connection(readonly=True) as db:
            receipt=json.loads(db.execute('SELECT receipt FROM intents WHERE id=?',(old['intent_id'],)).fetchone()[0])
        grant.host._finish(old['intent_id'],grant.ref.person_id,private['candidate'],receipt,
            execution=grant.actor,base_session_version=old['base_session_version'],attempt_id=grant.attempt.id)
    finally:
        grant.finishing=False
    grant.used=True
    return {'operation_id':grant.operation,'activation_id':grant.ref.activation_id,'intent_id':old['intent_id'],'phase':'completed','world_status':'technical_paused','replayed':False}


def recover(coordinator, ref, *, operation_id, repair_evidence):
    uuid_hex(operation_id)
    w=coordinator._authority;host=coordinator.actors;storage=getattr(host,'_storage',None)
    root=coordinator._owner_attempt
    if root is None:fail('COORDINATOR_EXECUTION_REQUIRED')
    backend(coordinator,root,host,storage)
    ref=scheduler.scoped(w,ref)
    if ref is None:fail('INVALID_ACTIVATION')
    # Actual SQL final gates own precommit checks. Do not turn a committed
    # closure into a reported rollback when cleanup exhausts the old entry.
    with coordinator._owner_scope(root),coordinator._preparation_scope(root,check_exit=False),scheduler.activation_scope(w,ref),verification_scope(w):
        repair=evidence(repair_evidence)
        entry=w._scoped_entry_budget()
        runtime_sources(coordinator,root,host,storage,entry,ref)
        with coordinator._entry_lock(coordinator.lock),coordinator._entry_lock(host._lock),scheduler.source_lock(w,entry):
            backend(coordinator,root,host,storage)
            with w._connection(readonly=True) as preflight:
                current=scheduler.latest(preflight)
                w._check_coordinator_execution(preflight,current['world_id'],root.permit,readonly=True)
            # Cleanup is IO over immutable original issuer obligations, not adoption
            # or a fresh actor/root lifetime. It consumes this same entry remainder.
            host._drain_lease_cleanups(entry)
            entry.check()
            if coordinator.busy or coordinator.cleanup_pending or host._attempt is not None or host._pending_threads or host._lease_cleanups or host.cleanup_pending:
                fail('ACTOR_EXECUTION_CLEANUP_PENDING')
            with w._connection(readonly=True) as db:
                db.row_factory=sqlite3.Row;db.execute('ATTACH DATABASE ? AS actor',(storage.actor_path.as_uri()+'?mode=ro',));db.execute('BEGIN')
                state=scheduler.latest(db)
                w._check_coordinator_execution(db,state['world_id'],root.permit,readonly=True)
                actual,identity=scheduler.source(w)
                if identity != [str(storage.actor_path),*storage.actor_file]:fail('ACTIVATION_BINDING_CONFLICT')
                scheduler.registration(db,state['world_id'],identity);scheduler.private_binding(db,state['world_id'])
                if schema(db):
                    saved=db.execute('SELECT * FROM '+_TABLE+' WHERE operation_id=?',(operation_id,)).fetchone()
                    if saved is not None:
                        if saved['world_id'] != ref.world_id or saved['activation_id'] != ref.activation_id:
                            fail('TECHNICAL_MAINTENANCE_OPERATION_CONFLICT')
                        row=scheduler.activation_row(w,db,state,ref.person_id,ref)
                        record=completed_proof(w,db,row)
                        if canonical(record['repair']) != canonical(repair):fail('TECHNICAL_MAINTENANCE_OPERATION_CONFLICT')
                        identity_check=_ReplayIdentity(coordinator,w,host,storage,root,entry,ref,row,state,identity,db)
                        scheduler.native_audit(storage,db,row)
                        identity_check.check(db,row)
                        return {'operation_id':operation_id,'activation_id':ref.activation_id,'intent_id':row['intent_id'],'phase':'completed','world_status':'technical_paused','replayed':True}
                original=material(w,db,host,storage,ref)
            grant=_Grant(_KEY,coordinator,root,ref,entry,original,repair,operation_id)
            with grant_scope(grant):
                permit=acquire_actor(grant);grant.actor=permit
                attempt=Attempt(permit,host.execution_policy,coordinator=root.permit,parent_attempt=root,activation=ref,deadline=entry.deadline)
                grant.attempt=attempt
                # Synchronous finite maintenance: no provider or heartbeat needed.
                attempt.thread=None
                host._attempt=attempt
                try:
                    with w._actor_attempt(attempt):
                        return promote(grant)
                finally:
                    host._attempt=None
                    # Preserve original issuer obligation even after a successful commit.
                    released=False
                    try:
                        released=w.release_actor_execution(permit) is True
                    except Exception:
                        pass
                    if not released:
                        # Existing cleanup expects a joined maintenance thread handle.
                        import threading
                        done=threading.Thread(target=lambda:None,name='technical-maintenance-cleanup-marker')
                        done.start();done.join();attempt.thread=done
                        host._lease_cleanups.append((w,permit,attempt));host._lease_cleanup_pending=True;host.cleanup_pending=True


# Independent root-only window release. No actor capability is constructed.
_WINDOW_TABLE = 'coordinator_technical_window_releases'
_WINDOW_DDL = '''CREATE TABLE coordinator_technical_window_releases (
    operation_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, window_id TEXT NOT NULL,
    record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version=1), UNIQUE(world_id,window_id))'''
_WINDOW_TRIGGERS = {
    'technical_window_release_no_'+event.lower():
    'CREATE TRIGGER technical_window_release_no_'+event.lower()+' BEFORE '+event+' ON '+_WINDOW_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable technical window release'); END"
    for event in ('UPDATE','DELETE')}
_WINDOW_TRIGGERS['technical_window_release_no_replace'] = "CREATE TRIGGER technical_window_release_no_replace BEFORE INSERT ON coordinator_technical_window_releases WHEN EXISTS (SELECT 1 FROM coordinator_technical_window_releases WHERE operation_id=NEW.operation_id OR (world_id=NEW.world_id AND window_id=NEW.window_id)) BEGIN SELECT RAISE(ABORT, 'immutable technical window release'); END"


def window_schema(db, *, create=False):
    found=db.execute('SELECT type,sql FROM sqlite_master WHERE name=?',(_WINDOW_TABLE,)).fetchone()
    if found is None:
        # A partial schema is not bootstrap permission.
        if any(db.execute('SELECT 1 FROM sqlite_master WHERE name=?',(n,)).fetchone() for n in _WINDOW_TRIGGERS):fail()
        if not create:return False
        db.execute(_WINDOW_DDL)
        for sql in _WINDOW_TRIGGERS.values():db.execute(sql)
        _history_cover(db)
        found=db.execute('SELECT type,sql FROM sqlite_master WHERE name=?',(_WINDOW_TABLE,)).fetchone()
    if tuple(found)!=('table',_WINDOW_DDL):fail()
    actual={r[0]:r[1] for r in db.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(_WINDOW_TABLE,))}
    epoch_names={'history_proof_'+_WINDOW_TABLE+'_'+e for e in ('insert','update','delete')}
    if any(n in actual for n in epoch_names):
        from .world import _history_check_triggers
        _history_check_triggers(db,'main',_WINDOW_TABLE)
    if {k:v for k,v in actual.items() if k not in epoch_names}!=_WINDOW_TRIGGERS:fail()
    return True


def window_sources(c, root, host, storage, entry, ref):
    from .source_attestation import attest, SourceMismatch
    import types
    try:
        result=attest(ROOT,c,root,root.permit,host,storage,c._authority,entry,ref,
                      Attempt,ExecutionPermit,scheduler.scoped,release_window,_WindowCapsule,check=entry.check)
        # Attest the actual new entry and every internal callee, not just names.
        stamps=(_SOURCE_GENERATION,c.__class__.__dict__['release_technical_window'].__globals__['_SOURCE_GENERATION'])
        codes={}
        def walk(code):
            codes[code.co_qualname]=code
            for child in code.co_consts:
                if isinstance(child,types.CodeType):walk(child)
        for stamp in stamps:walk(stamp['code'])
        for name,original in _WINDOW_DEFINITIONS.items():
            actual=globals().get(name)
            if actual is not original:raise SourceMismatch('replaced window callable')
            funcs=([actual] if isinstance(actual,types.FunctionType) else
                   [v for v in vars(actual).values() if isinstance(v,types.FunctionType)])
            for f in funcs:
                if f.__code__!=codes.get(f.__qualname__):raise SourceMismatch('changed window code')
        bound=c.release_technical_window
        if (getattr(bound,'__self__',None) is not c or
                bound.__func__.__code__!=codes.get('Coordinator.release_technical_window')):
            raise SourceMismatch('changed actual window entry')
        return result
    except (SourceMismatch,OSError,ValueError,KeyError,AttributeError):
        fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')


class _WindowCapsule:
    """Scope-local root identity only; never an M1 promotion grant."""
    def __init__(self,key,c,root,ref,repair,operation,db, *, w=None,h=None,s=None,host_lock=None,coordinator_lock=None,permit=None,entry=None,historical_source=None):
        if key is not _KEY:fail('TECHNICAL_MAINTENANCE_REQUIRED')
        if any(item is None for item in (w,h,s,host_lock,coordinator_lock,permit,entry)):fail('TECHNICAL_MAINTENANCE_REQUIRED')
        self.c=c;self.w=w;self.h=h;self.s=s
        self.host_lock=host_lock;self.coordinator_lock=coordinator_lock
        self.root=root;self.permit=permit;self.ref=ref
        self.entry=entry;self.repair=canonical(repair);self.operation=operation
        self.historical_source=canonical(historical_source)
        self.paths=(self.s.world_path,self.s.world_file,self.s.actor_path,self.s.actor_file)
        self.live()
        self.registration=canonical([dict(r) for r in db.execute('SELECT * FROM coordinator_scheduler_registration')])
        self.source=window_sources(c,root,self.h,self.s,self.entry,ref)

    def live(self):
        c=self.c;w=self.w;h=self.h;s=self.s;root=self.root;ref=self.ref
        self.entry.check();root.check()
        if (c._closed.is_set() or h._closing.is_set() or h._closed):fail('COORDINATOR_EXECUTION_CANCELLED')
        if (c._authority is not w or c.actors is not h or h._storage is not s
                or h._lock is not self.host_lock or c.lock is not self.coordinator_lock
                or c._owner_attempt is not root or root.world is not w or root.permit is not self.permit
                or w._scoped_coordinator_attempt() is not root or w._scoped_entry_budget() is not self.entry
                or w._scoped_attempt() is not None or scheduler.scoped(w)!=ref or h._bound_world_id!=ref.world_id
                or (s.world_path,s.world_file,s.actor_path,s.actor_file)!=self.paths):fail('ACTIVATION_BINDING_CONFLICT')
        backend(c,root,h,s)

    def check(self,db):
        self.live()
        c=self.c;w=self.w;h=self.h;s=self.s;root=self.root;ref=self.ref
        s.paths();actual,identity=scheduler.source(w)
        if actual is not s or identity!=[str(s.actor_path),*s.actor_file]:fail('ACTIVATION_BINDING_CONFLICT')
        scheduler.registration(db,ref.world_id,identity);scheduler.private_binding(db,ref.world_id)
        if self.registration!=canonical([dict(r) for r in db.execute('SELECT * FROM coordinator_scheduler_registration')]):fail('ACTIVATION_BINDING_CONFLICT')
        if self.source!=window_sources(c,root,h,s,self.entry,ref):fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
        if self.repair!=canonical(evidence(json.loads(self.repair),_historical_source=json.loads(self.historical_source))):fail()
        if (c.busy or c.cleanup_pending or h._attempt is not None or h._pending_threads or h._lease_cleanups or h.cleanup_pending):fail('ACTOR_EXECUTION_CLEANUP_PENDING')
        w._check_coordinator_execution(db,ref.world_id,self.permit,readonly=True)
        self.seal()

    def seal(self):
        # No SQL after this final source/file/live gate. It is deliberately
        # distinct from material reads and root SQL, which may invoke adapters.
        self.s.paths()
        if self.source!=window_sources(self.c,self.root,self.h,self.s,self.entry,self.ref):fail('TECHNICAL_MAINTENANCE_SOURCE_CHANGED')
        if self.repair!=canonical(evidence(json.loads(self.repair),_historical_source=json.loads(self.historical_source))):fail()
        self.live()


def window_chain(capsule,db):
    w=capsule.w;ref=capsule.ref;s=capsule.s
    state=scheduler.latest(db)
    if state['world_id']!=ref.world_id:fail('ACTIVATION_BINDING_CONFLICT')
    last=scheduler.window_history(db,ref.world_id,state)
    windows=[scheduler.window_record(db,ref.world_id,r[0]) for r in db.execute('SELECT window_id FROM coordinator_windows ORDER BY ordinal')]
    # Keep the anchor scope intact. acquisition_proof is a single-activation
    # capability consumer and deliberately rejects another roster reference.
    # window_record above validates roster identities; these are read-only
    # non-authorizing phase/native proofs for every persisted row.
    for window in windows:
        for r in db.execute('SELECT * FROM coordinator_activations WHERE window_id=?',(window['window_id'],)).fetchall():
            scheduler.phase_proof(w,db,r);scheduler.native_audit(s,db,r)
    # Audit *all* native rows, including orphans not reachable from a roster.
    people={p['id'] for p in state['persons']}
    for table in ('actor_intents','actor_intent_execution','actor_calls','actor_sessions','actor_session_heads'):
        if any(r[0] not in people for r in db.execute('SELECT DISTINCT person_id FROM actor.'+table)):fail()
    if db.execute('SELECT 1 FROM actor.actor_intent_execution m LEFT JOIN actor.actor_intents i ON i.intent_id=m.intent_id WHERE i.intent_id IS NULL').fetchone():fail()
    if db.execute('SELECT 1 FROM main.intents i LEFT JOIN coordinator_activations a ON a.intent_id=i.id WHERE a.activation_id IS NULL').fetchone():fail()
    row=scheduler.activation_row(w,db,state,ref.person_id,ref)
    if row['phase']!='completed' or row['private_closed']!=1 or row['roster_index']!=0:fail()
    m1=completed_proof(w,db,row)
    return state,last,windows,dict(row),m1


def window_material(capsule,db, *, paused_state=None):
    state,last,windows,row,m1=window_chain(capsule,db);ref=capsule.ref
    window=scheduler.window_record(db,ref.world_id,ref.window_id)
    if (last is None or last['window_id']!=ref.window_id or window['state']!='open' or window['boundary_kind']!='normal'
            or window['target_time']>=state['horizon']):fail('TECHNICAL_WINDOW_UNSUPPORTED')
    block=m1['original_block']
    original=db.execute('SELECT state FROM snapshots WHERE seq=?',(block['pause_revision'],)).fetchone()
    if original is None:fail()
    pause=json.loads(original[0])
    current=pause if paused_state is not None else state
    if (current!=pause or (paused_state is not None and paused_state!=pause) or pause['status']!='technical_paused'
            or pause['revision']!=block['pause_revision'] or pause.get('diagnostic')!={'code':block['diagnostic_code'],'time':window['target_time']}):fail()
    rows=[dict(r) for r in db.execute('SELECT * FROM coordinator_activations WHERE window_id=? ORDER BY roster_index',(ref.window_id,))]
    empty=('intent_id','request_sha256','candidate_sha256','receipt_sha256','promoted_version','observation_revision','card_version','base_session_version','owner_actor_epoch','resume_phase','diagnostic_code','terminal_reason')
    for r in rows[1:]:
        if (r['phase']!='pending' or r['private_closed']!=0 or any(r[k] is not None for k in empty)
                or r['owner_parent_epoch']!=r['created_parent_epoch']):fail('TECHNICAL_WINDOW_NOT_PRISTINE')
    # No unclosed native leaf, even when the generic lineage audit permits one.
    if db.execute("SELECT 1 FROM actor.actor_intents WHERE state NOT IN ('committed','rejected')").fetchone():fail('TECHNICAL_WINDOW_NOT_PRISTINE')
    # Every block must be this anchor or a terminal historical window with its
    # own independently proven release; old nonempty history is not a fault.
    for b in db.execute('SELECT * FROM coordinator_activation_blocks'):
        if b['activation_id']==ref.activation_id and b['world_id']==ref.world_id:continue
        a=db.execute('SELECT * FROM coordinator_activations WHERE activation_id=?',(b['activation_id'],)).fetchone()
        if a is None or a['window_id']==ref.window_id:fail()
        old=scheduler.window_record(db,ref.world_id,a['window_id'])
        if old['state']!='completed' or not window_schema(db):fail()
        saved=db.execute('SELECT * FROM '+_WINDOW_TABLE+' WHERE world_id=? AND window_id=?',(ref.world_id,a['window_id'])).fetchone()
        if saved is None:fail()
        window_record_proof(db,saved)
    facts={}
    for table in ('intents','coordinator_windows','coordinator_activations','coordinator_activation_outcomes','coordinator_activation_blocks','coordinator_fault_issuers','coordinator_technical_maintenance'):
        facts[table]=[dict(r) for r in db.execute('SELECT * FROM '+table+' ORDER BY rowid')]
    private={table:[dict(r) for r in db.execute('SELECT * FROM actor.'+table+' ORDER BY rowid')]
             for table in ('actor_world_binding','actor_sessions','actor_intents','actor_intent_execution','actor_calls','actor_session_heads')}
    return dict(state=pause,window=window,rows=rows,m1=m1,facts=facts,private=private)


def window_record_proof(db,saved):
    """Validate frozen history, never reinterpret its source as today's code."""
    try:
        raw=saved['record_json']
        if type(saved['schema_version']) is not int or saved['schema_version']!=1 or digest(raw)!=saved['record_sha256']:fail()
        r=json.loads(raw)
        keys={'operation_id','world_id','window_id','activation_id','repair','source_sha256','parent_epoch','m1_operation_id','pause_revision','pause_sha256','diagnostic','material_sha256','material','target_status','release_revision'}
        if type(r) is not dict or set(r)!=keys or canonical(r)!=raw:fail()
        for k in ('operation_id','window_id','activation_id','m1_operation_id'):
            if type(r[k]) is not str or len(r[k])!=32 or any(c not in '0123456789abcdef' for c in r[k]):fail()
        # World IDs follow the persisted world's UUID form, not operation hex.
        if type(r['world_id']) is not str or str(uuid.UUID(r['world_id']))!=r['world_id']:fail()
        for k in ('parent_epoch','pause_revision','release_revision'):
            if type(r[k]) is not int or not 1<=r[k]<=2**63-1:fail()
        if r['release_revision']!=r['pause_revision']+1 or r['target_status']!='user_paused':fail()
        if any(r[k]!=saved[k] for k in ('operation_id','world_id','window_id')):fail()
        from .source_attestation import FILES
        if type(r['source_sha256']) is not dict or set(r['source_sha256'])!={'app/'+n for n in FILES}:fail()
        hashes=[r[k] for k in ('pause_sha256','material_sha256')]+list(r['source_sha256'].values())
        if any(type(v) is not str or len(v)!=64 or any(c not in '0123456789abcdef' for c in v) for v in hashes):fail()
        repair=r['repair']
        if (type(repair) is not dict or set(repair)!={'repair_id','reason','source_sha256','verification_refs'}
                or type(repair['reason']) is not str or not repair['reason'].strip()):fail()
        uuid_hex(repair['repair_id'])
        if (type(repair['source_sha256']) is not dict or not set(CORE)<=set(repair['source_sha256'])
                or set(repair['source_sha256'])-set((*CORE,'app/coordinator_maintenance.py'))):fail()
        if any(v!=r['source_sha256'].get(k) for k,v in repair['source_sha256'].items()):fail()
        if type(repair['verification_refs']) is not list or not repair['verification_refs']:fail()
        for v in repair['verification_refs']:
            if (type(v) is not dict or set(v)!={'path','sha256'} or type(v['path']) is not str
                    or type(v['sha256']) is not str or len(v['sha256'])!=64
                    or any(c not in '0123456789abcdef' for c in v['sha256'])):fail()
        m=r['material']
        if type(m) is not dict or set(m)!={'state','window','rows','m1','facts','private'} or scheduler.sha(m)!=r['material_sha256']:fail()
        pause=json.loads(db.execute('SELECT state FROM snapshots WHERE seq=?',(r['pause_revision'],)).fetchone()[0])
        released=json.loads(db.execute('SELECT state FROM snapshots WHERE seq=?',(r['release_revision'],)).fetchone()[0])
        expected={**pause,'status':'user_paused','revision':r['release_revision']};expected.pop('diagnostic',None)
        if (released!=expected or m['state']!=pause or scheduler.sha(pause)!=r['pause_sha256']
                or pause['world_id']!=r['world_id'] or pause['status']!='technical_paused'
                or r['diagnostic']!=pause['diagnostic'] or r['m1_operation_id']!=m['m1']['operation_id']
                or m['window']['window_id']!=r['window_id'] or m['window']['state']!='open'
                or m['rows'][0]['activation_id']!=r['activation_id']):fail()
        # Immutable historical producers must still be present unchanged. The
        # roster/window/head/session are allowed to evolve through native APIs.
        for table in ('intents','coordinator_activation_outcomes','coordinator_activation_blocks','coordinator_fault_issuers','coordinator_technical_maintenance'):
            current=[dict(x) for x in db.execute('SELECT * FROM '+table)]
            if any(old not in current for old in m['facts'][table]):fail()
        for table in ('actor_world_binding','actor_intents','actor_intent_execution','actor_calls'):
            current=[dict(x) for x in db.execute('SELECT * FROM actor.'+table)]
            if any(old not in current for old in m['private'][table]):fail()
        return r
    except (TypeError,ValueError,KeyError,IndexError,AttributeError):
        fail()


def window_result(record,state,replayed):
    return {**{k:record[k] for k in ('operation_id','world_id','window_id','activation_id','release_revision')},
            'phase':'released','target_status':'user_paused','observed_status':state['status'],'replayed':replayed}


def window_effect(capsule,db,material):
    w=capsule.w;ref=capsule.ref
    if canonical(window_material(capsule,db))!=canonical(material):fail()
    state=json.loads(canonical(material['state']));w._restore_geography(db,state)
    baseline=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
    w._check_frozen_pathology(state,baseline)
    state['status']='user_paused';state.pop('diagnostic',None);state['revision']+=1
    w._check_frozen_pathology(state,baseline)
    record=dict(operation_id=capsule.operation,world_id=ref.world_id,window_id=ref.window_id,activation_id=ref.activation_id,
                repair=json.loads(capsule.repair),source_sha256=capsule.source,parent_epoch=capsule.permit.epoch,
                m1_operation_id=material['m1']['operation_id'],pause_revision=material['state']['revision'],
                pause_sha256=scheduler.sha(material['state']),diagnostic=material['state']['diagnostic'],
                material_sha256=scheduler.sha(material),material=material,target_status='user_paused',release_revision=state['revision'])
    window_schema(db,create=True)
    raw=canonical(record)
    db.execute('INSERT INTO '+_WINDOW_TABLE+' VALUES (?,?,?,?,?,1)',(capsule.operation,ref.world_id,ref.window_id,raw,digest(raw)))
    encoded=w._encode_state(db,state)
    if scheduler.latest(db)!=material['state']:fail()
    db.execute('INSERT INTO snapshots VALUES (?,?)',(state['revision'],encoded))
    # Own the actual COMMIT: all mutations precede these fresh final proofs.
    # Root SQL comes before the LAST material/snapshot/history reads. Neither
    # a cached digest nor earlier validation may cover later adapter activity.
    capsule.check(db)
    w._check_coordinator_execution(db,ref.world_id,capsule.permit)
    if canonical(window_material(capsule,db,paused_state=material['state']))!=canonical(material):fail()
    saved=db.execute('SELECT * FROM '+_WINDOW_TABLE+' WHERE operation_id=?',(capsule.operation,)).fetchone()
    if window_record_proof(db,saved)!=record or scheduler.latest(db)!=json.loads(encoded):fail()
    capsule.seal()
    db.commit()
    return window_result(record,state,False)


def release_window(coordinator,ref, *, operation_id,repair_evidence):
    uuid_hex(operation_id)
    w=coordinator._authority;host=coordinator.actors;storage=getattr(host,'_storage',None);root=coordinator._owner_attempt
    if root is None:fail('COORDINATOR_EXECUTION_REQUIRED')
    permit=root.permit
    backend(coordinator,root,host,storage)
    host_lock=host._lock;coordinator_lock=coordinator.lock
    ref=scheduler.scoped(w,ref)
    if ref is None:fail('INVALID_ACTIVATION')
    with coordinator._owner_scope(root),coordinator._preparation_scope(root,check_exit=False),scheduler.activation_scope(w,ref),verification_scope(w):
        entry=w._scoped_entry_budget()
        window_sources(coordinator,root,host,storage,entry,ref)
        with coordinator._entry_lock(coordinator_lock),coordinator._entry_lock(host_lock),scheduler.source_lock(w,entry):
            backend(coordinator,root,host,storage)
            host._drain_lease_cleanups(entry);entry.check()
            with w._connection(readonly=True) as pre:
                pre.row_factory=sqlite3.Row;pre.execute('ATTACH DATABASE ? AS actor',(storage.actor_path.as_uri()+'?mode=ro',));pre.execute('BEGIN')
                if window_schema(pre):
                    saved=pre.execute('SELECT * FROM '+_WINDOW_TABLE+' WHERE operation_id=? OR (world_id=? AND window_id=?)',(operation_id,ref.world_id,ref.window_id)).fetchall()
                    if saved:
                        if len(saved)!=1 or saved[0]['operation_id']!=operation_id:fail('TECHNICAL_MAINTENANCE_OPERATION_CONFLICT')
                        record=window_record_proof(pre,saved[0])
                        repair=evidence(repair_evidence,_historical_source=record['source_sha256'])
                        capsule=_WindowCapsule(_KEY,coordinator,root,ref,repair,operation_id,pre,w=w,h=host,s=storage,host_lock=host_lock,coordinator_lock=coordinator_lock,permit=permit,entry=entry,historical_source=record['source_sha256'])
                        if (record['world_id']!=ref.world_id or record['window_id']!=ref.window_id or record['activation_id']!=ref.activation_id
                                or canonical(record['repair'])!=capsule.repair):fail('TECHNICAL_MAINTENANCE_OPERATION_CONFLICT')
                        capsule.check(pre)
                        state,last,windows,row,m1=window_chain(capsule,pre)
                        if m1['operation_id']!=record['m1_operation_id']:fail()
                        capsule.check(pre)
                        return window_result(record,state,True)
                repair=evidence(repair_evidence)
                capsule=_WindowCapsule(_KEY,coordinator,root,ref,repair,operation_id,pre,w=w,h=host,s=storage,host_lock=host_lock,coordinator_lock=coordinator_lock,permit=permit,entry=entry);capsule.check(pre)
                material=window_material(capsule,pre)
            with w._connection() as db:
                db.row_factory=sqlite3.Row;db.execute('ATTACH DATABASE ? AS actor',(storage.actor_path.as_uri()+'?mode=ro',));db.execute('BEGIN IMMEDIATE')
                try:
                    capsule.check(db)
                    return window_effect(capsule,db,material)
                except BaseException:
                    db.rollback();raise


_WINDOW_DEFINITIONS={name:value for name,value in tuple(globals().items())
                     if name.startswith('window_') or name in ('release_window','_WindowCapsule')}


# Captured at actual import, not retroactively rebound at operator entry.
_IMPORTED_SHA = {n:file_sha(ROOT/n) for n in (*CORE,'app/coordinator_maintenance.py')}

from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())
