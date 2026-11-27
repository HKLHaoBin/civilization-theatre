"""Host-only scheduler ledger. No provider or actor execution lives here."""
from __future__ import annotations
from .actor_execution import body_matches
from typing import TYPE_CHECKING, Literal
if TYPE_CHECKING:
    from .world import WorldStore
    from .actor_execution import _DispatchSlot, DispatchCapture
from contextlib import contextmanager
from dataclasses import dataclass
from contextvars import ContextVar
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import threading
import uuid
import weakref

from .world import WorldError, DAY, _json, _history_cover, _bootstrap_baseline, _bootstrap_derived

MAX_INT = 2**63-1


def unproven():
    raise WorldError('WINDOW_RECOVERY_UNPROVEN', '窗口来源或持久状态不可证明')


def integer(value, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_INT:
        unproven()


def internal_uuid(value):
    if type(value) is not str or len(value) != 32 or any(c not in '0123456789abcdef' for c in value):
        unproven()


def sha(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def exists(db, table, schema='main'):
    return db.execute(f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def version_shape(version):
    if (type(version) is not dict or set(version) != {'world_id', 'revision'}
            or type(version['world_id']) is not str or not version['world_id'].strip()
            or type(version['revision']) is not int or not 1 <= version['revision'] <= MAX_INT):
        raise WorldError('INVALID_BASE_VERSION', '必须指定精确世界ID及正整数版本')
    return dict(version)


def check_version(state, version, *, revision=True):
    if state['world_id'] != version['world_id']:
        raise WorldError('WORLD_MISMATCH', '窗口不属于此世界')
    if revision and state['revision'] != version['revision']:
        raise WorldError('WINDOW_VERSION_CONFLICT', '开窗所据世界已变化')


@dataclass(frozen=True)
class WindowRef:
    world_id: str
    window_id: str


@dataclass(frozen=True)
class ActivationRef:
    world_id: str
    window_id: str
    activation_id: str
    person_id: str


_activation_scope = ContextVar('scheduler_activation', default=None)
_child_effect_scope = ContextVar('scheduler_child_effect', default=None)


@contextmanager
def child_effect(world, db, person, parent, child):
    # Only the adjudicator's compiled effect loop creates this exact SQL scope.
    token = _child_effect_scope.set((world, db, person, parent, child))
    try:
        yield
    finally:
        _child_effect_scope.reset(token)


def parent_intent(world, db, person, intent):
    child = _child_effect_scope.get()
    if child is not None and child[0] is world and child[1] is db and child[2] == person and child[4] == intent:
        return child[3]
    return intent


def scoped(world, reference=None):
    outer = _activation_scope.get()
    if outer is not None and outer[0] is world:
        if reference is not None and reference != outer[1]:
            raise WorldError('ACTIVATION_BINDING_CONFLICT', '不可替换原激活')
        reference = outer[1]
    if reference is not None:
        if (type(reference) is not ActivationRef or any(type(v) is not str or not v for v in
                (reference.world_id, reference.window_id, reference.activation_id, reference.person_id))):
            raise WorldError('INVALID_ACTIVATION', '激活引用格式不合法')
        for value in (reference.window_id, reference.activation_id):
            if len(value) != 32 or any(c not in '0123456789abcdef' for c in value):
                raise WorldError('INVALID_ACTIVATION', '激活引用格式不合法')
    return reference


@contextmanager
def activation_scope(world, reference):
    reference = scoped(world, reference)
    token = _activation_scope.set((world, reference))
    try:
        yield reference
    finally:
        _activation_scope.reset(token)


def activation_row(world, db, state, person, reference=None, execution=None, intent=None):
    reference = scoped(world, reference)
    registered = exists(db, 'coordinator_scheduler_registration')
    if not registered:
        if reference is not None:
            raise WorldError('ACTIVATION_BINDING_CONFLICT', '激活不属于登记世界')
        return None
    if reference is None:
        raise WorldError('ACTIVATION_REQUIRED', '登记世界必须指定原激活')
    if reference.world_id != state['world_id'] or reference.person_id != person:
        raise WorldError('ACTIVATION_BINDING_CONFLICT', '激活人物或世界不符')
    window = window_record(db, state['world_id'], reference.window_id)
    db.row_factory = sqlite3.Row
    raw = db.execute('SELECT * FROM coordinator_activations WHERE world_id=? AND activation_id=?',
                     (reference.world_id, reference.activation_id)).fetchone()
    if raw is None or raw['window_id'] != reference.window_id or raw['person_id'] != person:
        raise WorldError('ACTIVATION_BINDING_CONFLICT', '激活绑定不符')
    row = dict(raw)
    if row['phase'] not in ('pending','prepared','candidate','world_committed','completed','rejected','technical_blocked'):
        unproven()
    if intent is not None and row['intent_id'] != intent:
        raise WorldError('ACTIVATION_BINDING_CONFLICT', '原意图不符')
    if execution is not None:
        parent = world._scoped_coordinator()
        delegation = db.execute('SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                                (execution.world_id,person,execution.epoch)).fetchone() if exists(db,'actor_activation_delegations') else None
        from .coordinator_maintenance import actor_authorized
        maintenance = actor_authorized(world, reference, execution)
        if (not maintenance and (parent is None or row['owner_parent_epoch'] != parent.epoch or row['owner_actor_epoch'] != execution.epoch
                or delegation is None or tuple(delegation) != (reference.window_id,reference.activation_id))):
            raise WorldError('ACTIVATION_BINDING_CONFLICT', '原激活授权已失权')
    if row['phase'] in ('rejected','technical_blocked'):
        storage,identity=source(world)
        if 'actor' not in {r[1] for r in db.execute('PRAGMA database_list')}:
            db.execute('ATTACH DATABASE ? AS actor',(storage.actor_path.as_uri()+'?mode=ro',))
        registration(db,state['world_id'],identity)
        try:
            phase_proof(world,db,row)
            native_audit(storage,db,row)
        except (sqlite3.DatabaseError,ValueError,TypeError,KeyError,IndexError):
            unproven()
        storage.paths()
    return row


def claim(world, db, state, person, reference, execution, parent):
    row = activation_row(world,db,state,person,reference)
    if row is None:
        return
    if parent is None:
        raise WorldError('COORDINATOR_EXECUTION_REQUIRED', '激活必须持协调许可')
    db.execute('''CREATE TABLE IF NOT EXISTS actor_activation_delegations (
        world_id TEXT NOT NULL,person_id TEXT NOT NULL,actor_epoch INTEGER NOT NULL,
        window_id TEXT NOT NULL,activation_id TEXT NOT NULL,PRIMARY KEY(world_id,person_id,actor_epoch))''')
    _history_cover(db)
    db.execute('INSERT INTO actor_activation_delegations VALUES (?,?,?,?,?)',
               (state['world_id'],person,execution.epoch,reference.window_id,reference.activation_id))
    db.execute('UPDATE coordinator_activations SET owner_parent_epoch=?,owner_actor_epoch=? WHERE world_id=? AND activation_id=?',
               (parent.epoch,execution.epoch,state['world_id'],reference.activation_id))
    db.execute('UPDATE coordinator_windows SET owner_parent_epoch=? WHERE world_id=? AND window_id=?',
               (parent.epoch,state['world_id'],reference.window_id))


def private_proof(world, db, person, execution, *, intent=None):
    row = activation_row(world,db,latest(db),person,execution=execution,intent=intent)
    if row is None:
        return None
    acquisition_proof(world, db, latest(db), person, scoped(world))
    return row


def phase_proof(world, db, row):
    try:
        return _phase_proof(world, db, row)
    except (ValueError, TypeError, KeyError, IndexError):
        unproven()


def _phase_proof(world, db, row):
    if row['phase'] == 'technical_blocked':
        block_proof(world, db, row)
        return row
    if (row['phase'] == 'completed' and exists(db, 'coordinator_activation_blocks')
            and db.execute('SELECT 1 FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',
                           (row['world_id'], row['activation_id'])).fetchone()):
        from .coordinator_maintenance import completed_proof
        completed_proof(world, db, row)
        return row
    return material_proof(world, db, row, row['phase'])


def material_proof(world, db, row, phase):
    """Non-recursive original material proof; never grants execution authority."""
    from .actor_execution import digest
    person = row['person_id']
    if phase not in ('pending','prepared','candidate','world_committed','completed','rejected'):
        unproven()
    if phase != 'rejected':
        fields = ('terminal_reason',) if row['phase']=='technical_blocked' else ('terminal_reason','resume_phase','diagnostic_code')
        if any(row[k] is not None for k in fields):
            unproven()
        saved=db.execute('SELECT kind FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
                         (row['world_id'],row['activation_id'])).fetchone() if exists(db,'coordinator_activation_outcomes') else None
        if phase not in ('world_committed','completed') and (saved is not None or row['receipt_sha256'] is not None):
            unproven()
    if phase == 'rejected':
        rejection_proof(world, db, row)
        return row
    if phase == 'pending':
        if any(row[k] is not None for k in ('intent_id','request_sha256','candidate_sha256','receipt_sha256','promoted_version')):
            unproven()
        return row
    private = db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?', (row['intent_id'],person)).fetchone()
    meta = db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (row['intent_id'],)).fetchone()
    if (private is None or meta is None or digest(private['request']) != row['request_sha256']
            or meta['base_version'] != row['base_session_version']):
        unproven()
    request = json.loads(private['request'])
    if (type(request) is not dict or request.get('person_id') != person
            or request.get('intent_id') != row['intent_id'] or not body_matches(request, json.loads(private['body']), private['candidate'])
            or meta['person_id'] != person or meta['world_id'] != row['world_id']):
        unproven()
    if phase == 'prepared':
        if private['state'] != 'prepared' or private['candidate'] is not None:
            unproven()
    else:
        if (private['candidate'] is None or digest(private['candidate']) != row['candidate_sha256']
                or private['state'] != ('committed' if phase=='completed' else 'candidate')):
            unproven()
    actual = db.execute('SELECT person,receipt FROM main.intents WHERE id=?',(row['intent_id'],)).fetchone()
    if phase in ('world_committed','completed'):
        outcome = db.execute('SELECT * FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
                             (row['world_id'],row['activation_id'])).fetchone() if exists(db,'coordinator_activation_outcomes') else None
        if (actual is None or actual['person'] != person or actual['receipt'] is None
                or digest(actual['receipt']) != row['receipt_sha256'] or outcome is None
                or outcome['kind'] != 'committed' or outcome['intent_id'] != row['intent_id']
                or digest(outcome['outcome_json']) != outcome['outcome_sha256']
                or json.loads(outcome['outcome_json']) != json.loads(actual['receipt'])):
            unproven()
        receipt = json.loads(actual['receipt'])
        if (type(receipt) is not dict or receipt.get('status') != 'committed'
                or receipt.get('world_id') != row['world_id'] or receipt.get('person_id') != person
                or receipt.get('intent_id') != row['intent_id']):
            unproven()
    elif actual is not None and actual['receipt'] is not None:
        unproven()
    if phase == 'completed':
        if (row['private_closed'] != 1 or private['state'] != 'committed' or private['receipt'] is None
                or digest(private['receipt']) != row['receipt_sha256'] or meta['promoted_version'] != row['promoted_version']
                or row['promoted_version'] != meta['base_version']+1):
            unproven()
    elif row['private_closed'] != 0 or meta['promoted_version'] is not None:
        unproven()
    return row


def block_payload(db, row, resume):
    saved = db.execute('SELECT outcome_sha256 FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
                       (row['world_id'], row['activation_id'])).fetchone() if exists(db,'coordinator_activation_outcomes') else None
    return dict(schema_version=1, **{k:row[k] for k in
                ('world_id','window_id','activation_id','person_id','intent_id','request_sha256',
                 'candidate_sha256','receipt_sha256','base_session_version')},
                resume_phase=resume, outcome_sha256=saved[0] if saved else None, private_closed=0)


def block_proof(world, db, row):
    block_witness_proof(world, db, row)
    material_proof(world, db, row, row['resume_phase'])
    return row


def block_witness_proof(world, db, row):
    from .actor_execution import canonical, digest
    if (row['resume_phase'] not in ('pending','prepared','candidate','world_committed','rejected')
            or row['private_closed'] != 0 or row['promoted_version'] is not None
            or type(row['diagnostic_code']) is not str
            or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', row['diagnostic_code']) is None
            or not exists(db, 'coordinator_activation_blocks')):
        unproven()
    if (not exists(db,'actor_activation_delegations') or not exists(db,'actor_coordinator_delegations')):
        unproven()
    binding=db.execute('SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                       (row['world_id'],row['person_id'],row['owner_actor_epoch'])).fetchone()
    parent=db.execute('SELECT parent_epoch FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                      (row['world_id'],row['person_id'],row['owner_actor_epoch'])).fetchone()
    if binding is None or tuple(binding)!=(row['window_id'],row['activation_id']) or parent is None or parent[0]!=row['owner_parent_epoch']:
        unproven()
    if row['resume_phase']!='pending':
        integer(row['observation_revision'],1)
        integer(row['card_version'],1)
        integer(row['base_session_version'])
    if row['resume_phase']=='pending' and any(row[k] is not None for k in
            ('intent_id','request_sha256','candidate_sha256','receipt_sha256','promoted_version',
             'observation_revision','card_version','base_session_version')):
        unproven()
    if row['resume_phase']=='prepared' and any(row[k] is not None for k in ('candidate_sha256','receipt_sha256','promoted_version')):
        unproven()
    saved = db.execute('SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',
                       (row['world_id'],row['activation_id'])).fetchone()
    if (saved is None or saved['resume_phase'] != row['resume_phase']
            or saved['diagnostic_code'] != row['diagnostic_code']
            or type(saved['pause_revision']) is not int or saved['pause_revision'] < 1
            or saved['proof_json'] != canonical(block_payload(db,row,row['resume_phase']))
            or saved['proof_sha256'] != digest(saved['proof_json'])):
        unproven()
    pause = db.execute('SELECT state FROM snapshots WHERE seq=?',(saved['pause_revision'],)).fetchone()
    if pause is None:
        unproven()
    pause = json.loads(pause[0])
    window = window_record(db,row['world_id'],row['window_id'])
    if (pause['world_id'] != row['world_id'] or pause['revision'] != saved['pause_revision']
            or pause['time'] != window['target_time'] or pause['status'] != 'technical_paused'
            or pause.get('diagnostic') != {'code':row['diagnostic_code'],'time':window['target_time']}
            or saved['pause_revision'] < (row['observation_revision'] or window['advanced_revision'])):
        unproven()
    fault_issuer_proof(world, db, row, required=False)
    return row


_ISSUER_TABLE = 'coordinator_fault_issuers'
_ISSUER_DDL = '''CREATE TABLE coordinator_fault_issuers (
    world_id TEXT NOT NULL, activation_id TEXT NOT NULL,
    witness_json TEXT NOT NULL, witness_sha256 TEXT NOT NULL,
    PRIMARY KEY(world_id,activation_id))'''


_ISSUER_INSERT = "CREATE TRIGGER fault_issuer_no_replace BEFORE INSERT ON coordinator_fault_issuers WHEN EXISTS (SELECT 1 FROM coordinator_fault_issuers WHERE world_id=NEW.world_id AND activation_id=NEW.activation_id) BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"


def fault_issuer_schema(db, *, create=False, authorize=False):
    if not exists(db, _ISSUER_TABLE):
        if not create:
            return False
        db.execute(_ISSUER_DDL)
        for event in ('UPDATE','DELETE'):
            db.execute('CREATE TRIGGER fault_issuer_no_'+event.lower()+' BEFORE '+event+' ON '+_ISSUER_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END")
        db.execute(_ISSUER_INSERT)
        _history_cover(db)
    if db.execute('SELECT sql FROM sqlite_master WHERE name=?',(_ISSUER_TABLE,)).fetchone()[0] != _ISSUER_DDL:
        unproven()
    for event in ('UPDATE','DELETE'):
        expected='CREATE TRIGGER fault_issuer_no_'+event.lower()+' BEFORE '+event+' ON '+_ISSUER_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"
        found=db.execute('SELECT sql FROM sqlite_master WHERE name=?',('fault_issuer_no_'+event.lower(),)).fetchone()
        if found is None or found[0] != expected:
            unproven()
    found=db.execute("SELECT sql FROM sqlite_master WHERE name='fault_issuer_no_replace'").fetchone()
    # A legacy two-trigger witness remains diagnostic material, never a new
    # effect capability. No online upgrade or historical re-signing here.
    if (found is None and (create or authorize)) or (found is not None and found[0] != _ISSUER_INSERT):
        unproven()
    return True


def decode_fault_issuer(raw):
    """Strict v1 history: owner fields describe the publishing fault, not a claim.

    Canonical JSON and append-only SQL are integrity checks within the trusted
    publisher/storage boundary, not authentication against a DB administrator.
    """
    from .actor_execution import canonical
    try:
        value=json.loads(raw)
    except (ValueError,TypeError):
        unproven()
    def exact(obj, keys):
        if type(obj) is not dict or set(obj) != set(keys):unproven()
    def text(value):
        if type(value) is not str or not value.strip():unproven()
    def hash_text(value):
        if type(value) is not str or re.fullmatch('[0-9a-f]{64}',value) is None:unproven()
    exact(value,('schema_version','binding','parent','pause_revision','block_sha256','meta'))
    if type(value['schema_version']) is not int or value['schema_version'] != 1:unproven()
    binding=value['binding'];meta=value['meta'];parent=value['parent']
    exact(binding,('world_id','window_id','activation_id','person_id','intent_id','owner_parent_epoch','owner_actor_epoch'))
    exact(meta,('intent_id','person_id','world_id','base_version','base_messages_sha256','base_seen_sha256','owner_epoch','owner_token','attempt_id'))
    for k in ('world_id','person_id'):
        text(binding[k]);text(meta[k])
    for k in ('window_id','activation_id','intent_id'):internal_uuid(binding[k])
    for k in ('intent_id','owner_token','attempt_id'):internal_uuid(meta[k])
    for k in ('owner_actor_epoch','owner_parent_epoch'):integer(binding[k],1)
    integer(meta['owner_epoch'],1);integer(meta['base_version']);integer(value['pause_revision'],1)
    for k in ('base_messages_sha256','base_seen_sha256'):hash_text(meta[k])
    hash_text(value['block_sha256'])
    if type(parent) is not list or len(parent) != 2:unproven()
    integer(parent[0],1);internal_uuid(parent[1])
    if (parent[0] != binding['owner_parent_epoch'] or meta['owner_epoch'] != binding['owner_actor_epoch']
            or any(meta[k] != binding[k] for k in ('world_id','person_id','intent_id'))
            or canonical(value) != raw):unproven()
    return value


def fault_issuer_proof(world, db, row, *, required=True):
    """Original pause-time authority, never minted from maintenance-time metadata."""
    from .actor_execution import canonical, digest
    found = db.execute('SELECT * FROM '+_ISSUER_TABLE+' WHERE world_id=? AND activation_id=?',
                       (row['world_id'],row['activation_id'])).fetchone() if fault_issuer_schema(db,authorize=required) else None
    if found is None:
        if required:
            unproven()
        return None  # Legacy ordinary blocked diagnostics retain their old proof.
    witness=decode_fault_issuer(found['witness_json'])
    if (found['world_id'] != row['world_id'] or found['activation_id'] != row['activation_id']
            or found['witness_sha256'] != digest(found['witness_json'])):
        unproven()
    block=db.execute('SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',
                     (row['world_id'],row['activation_id'])).fetchone()
    # v1 binding is the immutable fault-time snapshot, NOT the current
    # admission owner. claim() may legitimately change the row's two epochs;
    # block_witness_proof separately checks that current delegation chain.
    fault=witness['binding']
    integer(fault['owner_actor_epoch'],1);integer(fault['owner_parent_epoch'],1)
    parent=db.execute('SELECT parent_epoch,parent_owner_token FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                      (row['world_id'],row['person_id'],fault['owner_actor_epoch'])).fetchone()
    delegation=db.execute('SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                          (row['world_id'],row['person_id'],fault['owner_actor_epoch'])).fetchone()
    meta=db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?',(row['intent_id'],)).fetchone()
    if (block is None or parent is None or meta is None or delegation is None
            or any(fault[k] != row[k] for k in ('world_id','window_id','activation_id','person_id','intent_id'))
            or tuple(delegation) != (row['window_id'],row['activation_id'])
            or parent['parent_epoch'] != fault['owner_parent_epoch']
            or witness['pause_revision'] != block['pause_revision'] or witness['block_sha256'] != block['proof_sha256']
            or witness['parent'] != list(parent)
            or witness['meta'] != {k:meta[k] for k in meta.keys() if k != 'promoted_version'}
            or meta['owner_epoch'] != fault['owner_actor_epoch']):
        unproven()
    # At publication/final pause gate, bind to the actual captured live issuer.
    # A later legitimate actor (including maintenance) never substitutes for it.
    attempt=world._scoped_attempt()
    if attempt is not None and attempt.permit.epoch == fault['owner_actor_epoch']:
        if (attempt.permit.world_id != row['world_id'] or attempt.permit.person_id != row['person_id']
                or attempt.permit.owner_token != meta['owner_token'] or attempt.id != meta['attempt_id']
                or attempt.activation != scoped(world) or attempt.coordinator is None
                or [attempt.coordinator.epoch,attempt.coordinator.owner_token] != witness['parent']):
            unproven()
        fault_issuer_scope(world, row, attempt.permit)
    return witness


def fault_issuer_scope(world, row, execution):
    """Preserve the actual captured authority shape, including permit-only entry."""
    attempt=world._scoped_attempt()
    if (attempt is None or attempt.permit != execution or attempt.coordinator is None
            or attempt.activation != scoped(world) or attempt.activation is None
            or any(getattr(attempt.activation,k) != row[k] for k in ('world_id','window_id','activation_id','person_id'))
            or world._scoped_coordinator(attempt.coordinator) != attempt.coordinator):
        unproven()
    parent=attempt.parent_attempt
    # A lost captured parent scope is never permission to downgrade to a permit.
    if world._scoped_coordinator_attempt() is not parent:
        unproven()
    if parent is not None:
        if parent.permit != attempt.coordinator:
            unproven()
        parent.check()
    attempt.check()
    return attempt


def capture_fault_issuer(world, db, row, execution, revision, proof):
    from .actor_execution import canonical, digest
    attempt=fault_issuer_scope(world,row,execution)
    world._check_actor_execution(db,row['world_id'],row['person_id'],execution,coordinator=attempt.coordinator)
    world._check_coordinator_execution(db,row['world_id'],attempt.coordinator)
    storage,identity=source(world)
    registration(db,row['world_id'],identity);storage.paths()
    _,meta=storage.check_base(db,row['intent_id'],row['person_id'],execution,attempt.id)
    witness=dict(schema_version=1,binding={k:row[k] for k in ('world_id','window_id','activation_id','person_id','intent_id','owner_parent_epoch','owner_actor_epoch')},
                 parent=[attempt.coordinator.epoch,attempt.coordinator.owner_token],pause_revision=revision,block_sha256=digest(proof),
                 meta={k:meta[k] for k in meta.keys() if k != 'promoted_version'})
    fault_issuer_schema(db,create=True)
    raw=canonical(witness)
    db.execute('INSERT INTO '+_ISSUER_TABLE+' VALUES (?,?,?,?)',(row['world_id'],row['activation_id'],raw,digest(raw)))


def block_activation(world, db, state, execution, code):
    """Called only in the original actual pause transaction, before its snapshot."""
    from .actor_execution import canonical, digest
    reference = scoped(world)
    if reference is None or execution is None:
        return
    row = activation_row(world,db,state,execution.person_id,reference,execution)
    acquisition_proof(world,db,state,execution.person_id,reference)
    if row['phase']=='completed' or (row['phase']=='rejected' and row['private_closed']==1):
        return  # Terminal ACK classification is a separate delivery slice.
    if row['phase']=='technical_blocked':
        block_proof(world,db,row)
        if row['diagnostic_code'] != code:
            unproven()
        return
    if row['phase'] not in ('pending','prepared','candidate','world_committed','rejected') or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',code) is None:
        unproven()
    db.execute('''CREATE TABLE IF NOT EXISTS coordinator_activation_blocks (
        world_id TEXT NOT NULL, activation_id TEXT NOT NULL,
        resume_phase TEXT NOT NULL CHECK(resume_phase IN ('pending','prepared','candidate','world_committed','rejected')),
        diagnostic_code TEXT NOT NULL, pause_revision INTEGER NOT NULL CHECK(pause_revision>0),
        proof_json TEXT NOT NULL, proof_sha256 TEXT NOT NULL, PRIMARY KEY(world_id,activation_id))''')
    _history_cover(db)
    proof = canonical(block_payload(db,row,row['phase']))
    changed = state['status'] != 'technical_paused' or state.get('diagnostic') != {'code':code,'time':state['time']}
    revision = state['revision']+int(changed)
    if row['phase']=='world_committed':
        capture_fault_issuer(world,db,row,execution,revision,proof)
    db.execute('INSERT INTO coordinator_activation_blocks VALUES (?,?,?,?,?,?,?)',
               (row['world_id'],row['activation_id'],row['phase'],code,revision,proof,digest(proof)))
    db.execute("UPDATE coordinator_activations SET phase='technical_blocked',resume_phase=?,diagnostic_code=? WHERE world_id=? AND activation_id=?",
               (row['phase'],code,row['world_id'],row['activation_id']))


_history_audit_batch = ContextVar('scheduler_history_audit_batch', default=None)


# Fixed complete native query shapes. '?' denotes exactly one SQLite scalar,
# never an identifier, function, expression, clause or statement. The list is
# audited source at implementation time, not discovered/extended at runtime.
_HISTORY_SELECT_FORMS = (
    'SELECT * FROM actor.actor_calls WHERE person_id=?',
    'SELECT * FROM actor.actor_intent_execution WHERE intent_id=?',
    'SELECT * FROM actor.actor_intent_execution WHERE person_id=? ORDER BY base_version',
    'SELECT * FROM actor.actor_intents WHERE intent_id=?',
    'SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?',
    'SELECT * FROM actor.actor_intents WHERE person_id=?',
    "SELECT * FROM actor.actor_intents WHERE person_id=? AND state IN ('prepared','candidate')",
    'SELECT * FROM actor.actor_session_heads WHERE person_id=?',
    'SELECT * FROM actor.actor_sessions',
    'SELECT * FROM actor.actor_sessions WHERE person_id=?',
    'SELECT * FROM coordinator_activation_blocks',
    'SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',
    'SELECT * FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
    'SELECT * FROM coordinator_activations ORDER BY window_id,roster_index',
    'SELECT * FROM coordinator_activations WHERE activation_id=?',
    'SELECT * FROM coordinator_activations WHERE intent_id=?',
    'SELECT * FROM coordinator_activations WHERE window_id=?',
    'SELECT * FROM coordinator_activations WHERE window_id=? ORDER BY roster_index',
    'SELECT * FROM coordinator_activations WHERE world_id=? AND activation_id=?',
    "SELECT * FROM coordinator_activations WHERE world_id=? AND window_id=? AND NOT (phase IN ('completed','rejected','skipped') AND private_closed=1) ORDER BY roster_index LIMIT 1",
    'SELECT * FROM coordinator_activations WHERE world_id=? AND window_id=? ORDER BY roster_index',
    'SELECT * FROM coordinator_fault_issuers WHERE world_id=? AND activation_id=?',
    'SELECT * FROM coordinator_scheduler_registration',
    'SELECT * FROM coordinator_technical_maintenance WHERE world_id=? AND activation_id=?',
    'SELECT * FROM coordinator_windows WHERE world_id=? AND window_id=?',
    'SELECT * FROM main.intents WHERE id=?',
    'SELECT * FROM scheduler_genesis',
    'SELECT 1',
    'SELECT 1 FROM actor.actor_calls WHERE person_id=?',
    'SELECT 1 FROM actor.actor_intent_execution WHERE person_id=?',
    'SELECT 1 FROM actor.actor_intent_execution m LEFT JOIN actor.actor_intents i ON i.intent_id=m.intent_id WHERE i.intent_id IS NULL',
    'SELECT 1 FROM actor.actor_intents WHERE person_id=?',
    "SELECT 1 FROM actor.actor_intents WHERE state NOT IN ('committed','rejected')",
    'SELECT 1 FROM actor.actor_session_heads WHERE person_id=?',
    "SELECT 1 FROM actor.sqlite_master WHERE type='table' AND name=?",
    'SELECT 1 FROM actor_execution_leases WHERE world_id=? LIMIT 1',
    'SELECT 1 FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?',
    "SELECT 1 FROM coordinator_activations WHERE world_id=? AND window_id=? AND NOT (phase IN ('completed','rejected','skipped') AND private_closed=1) LIMIT 1",
    "SELECT 1 FROM coordinator_windows WHERE state='open'",
    "SELECT 1 FROM dm_timeline WHERE intent_id=? AND kind='commit'",
    'SELECT 1 FROM intents WHERE id=?',
    'SELECT 1 FROM main.intents i LEFT JOIN coordinator_activations a ON a.intent_id=i.id WHERE a.activation_id IS NULL',
    "SELECT 1 FROM main.sqlite_master WHERE type='table' AND name=?",
    'SELECT 1 FROM sqlite_master WHERE name=?',
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_coordinator_delegations'",
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_execution_leases'",
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_execution_leases'",
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_clock'",
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='snapshots'",
    "SELECT 1 FROM world_constants WHERE key='geography'",
    'SELECT body FROM actor.actor_intents WHERE intent_id=? AND person_id=?',
    'SELECT coalesce(max(ordinal),0) FROM coordinator_windows WHERE world_id=?',
    'SELECT epoch,owner_token,expires_wall_ns FROM actor_execution_leases WHERE world_id=? AND person_id=?',
    'SELECT epoch,owner_token,expires_wall_ns FROM coordinator_execution_leases WHERE world_id=?',
    'SELECT high_water_wall_ns FROM execution_clock WHERE singleton=1',
    'SELECT kind FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
    'SELECT messages,seen FROM actor.actor_sessions WHERE person_id=?',
    "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
    'SELECT outcome_sha256 FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
    'SELECT parent_epoch FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
    'SELECT parent_epoch,parent_owner_token FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
    'SELECT person,payload,receipt FROM intents WHERE id=?',
    'SELECT person,payload,receipt FROM main.intents WHERE id=?',
    'SELECT person,receipt FROM intents WHERE id=?',
    'SELECT person,receipt FROM main.intents WHERE id=?',
    'SELECT phase FROM coordinator_activations WHERE activation_id=?',
    'SELECT receipt FROM intents WHERE id=?',
    'SELECT seq,state FROM snapshots ORDER BY seq',
    'SELECT seq,state FROM snapshots ORDER BY seq LIMIT 1',
    'SELECT singleton,world_id FROM actor.actor_world_binding',
    "SELECT sql FROM sqlite_master WHERE name='fault_issuer_no_replace'",
    "SELECT sql FROM sqlite_master WHERE name='technical_maintenance_no_replace'",
    'SELECT sql FROM sqlite_master WHERE name=?',
    "SELECT sql FROM sqlite_master WHERE type='index' AND name='coordinator_one_open_window'",
    'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1',
    'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1',
    'SELECT state FROM snapshots ORDER BY seq LIMIT 1',
    'SELECT state FROM snapshots WHERE seq=?',
    'SELECT type,sql FROM sqlite_master WHERE name=?',
    "SELECT value,sha256 FROM world_constants WHERE key='geography'",
    'SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1',
    'SELECT window_id FROM coordinator_windows ORDER BY ordinal',
    'SELECT window_id FROM coordinator_windows WHERE world_id=?',
    'SELECT window_id FROM coordinator_windows WHERE world_id=? ORDER BY ordinal',
    'SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
    'SELECT world_id FROM actor.actor_world_binding WHERE singleton=1',
    'SELECT world_id,person_id,text FROM dm_proposals WHERE intent_id=?',
)


def _history_sql_tokens(sql):
    """Tokenize without executing SQL; reject comments and unknown syntax.

    Preserve quoted literal contents (including escaped quotes, comment markers
    and semicolons). Keywords/identifiers alone are case-insensitive. Native
    trace-expanded bind values are single scalar tokens, not SQL fragments.
    """
    token = re.compile(
        r"\s+|[xX]'[0-9a-fA-F]*'|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|"
        r"[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?)(?![A-Za-z_0-9])|"
        r"[A-Za-z_][A-Za-z_0-9]*|<=|>=|!=|<>|==|[?=(),.*;<>]"
    )
    values = []
    position = 0
    while position < len(sql):
        match = token.match(sql, position)
        if match is None:
            return None
        value = match.group()
        position = match.end()
        if value.isspace():
            continue
        if value.startswith("'"):
            values.append(('scalar', value[1:-1].replace("''", "'")))
        elif value[:2].lower() == "x'":
            if (len(value)-3) % 2:
                return None
            values.append(('blob', value[2:-1].lower()))
        elif value.startswith('"'):
            values.append(('quoted', value[1:-1].replace('""', '"')))
        elif re.fullmatch(r'[+-]?(?:[0-9].*|\.[0-9].*)', value):
            values.append(('number', value))
        else:
            values.append(('syntax', value.lower()))
    if values and values[-1] == ('syntax', ';'):
        values.pop()
    if ('syntax', ';') in values:
        return None
    return tuple(values)


_HISTORY_SELECT_TOKENS = tuple(_history_sql_tokens(sql) for sql in _HISTORY_SELECT_FORMS)


def _history_read_sql(sql):
    """Observation only: complete trusted forms, otherwise permanent taint."""
    text = sql.strip()
    if text.endswith(';'):
        text = text[:-1].rstrip()
    # Preserve the exact prior PRAGMA grammar and the sole setting exception.
    if re.fullmatch(r'PRAGMA\s+busy_timeout\s*=\s*1', text, re.I):
        return True
    if re.fullmatch(
        r'PRAGMA\s+(?:(?:main|actor)\.)?(?:database_list|'
        r'(?:table_info|index_list|index_info)\(\s*[A-Za-z_][A-Za-z_0-9]*\s*\))',
        text, re.I) is not None:
        return True
    actual = _history_sql_tokens(sql)
    if actual is None:
        return False
    for expected in _HISTORY_SELECT_TOKENS:
        if len(actual) != len(expected):
            continue
        if all(a == e or (e == ('syntax', '?') and
               (a[0] in ('scalar', 'number', 'blob') or a in
                (('syntax', 'null'), ('syntax', '?'))))
               for a, e in zip(actual, expected)):
            return True
    return False


def _history_original_method(function, stamp):
    """Reject substituted connection lifecycle code; no trace getter exists."""
    function = getattr(function, '__wrapped__', function)
    code = getattr(function, '__code__', None)
    if code is None:
        return False
    pending = [stamp['code']]
    while pending:
        original = pending.pop()
        if original.co_qualname == code.co_qualname and original == code:
            return True
        pending.extend(v for v in original.co_consts if isinstance(v, type(original)))
    return False


class _HistoryAuditBatch:
    """Success-only memo for the sole fresh ledger connection's history loop.

    PrivateStore._open constructs this connection, ATTACHes and begins it;
    _transaction/ledger_transaction do not expose it or install a callback
    before open_window's loop. Only that reviewed lifecycle owns the trace.
    Neither a setter's None return nor a connection type proves an old trace
    absent. Unknown/substituted lifecycle code disables this optimization.
    Arbitrary Python/native instrumentation is outside the trusted-source model.
    """
    def __init__(self, world, storage, db):
        self.world, self.storage, self.db = world, storage, db
        self.host = storage.host
        self.tainted = False
        self.successes = set()
        self.window_records = {}
        self.running = False
        self.initial = self.fingerprint()

    def invalidate(self):
        self.tainted = True
        self.successes.clear()
        self.window_records.clear()

    def observe(self, sql):
        try:
            if not _history_read_sql(sql):
                self.invalidate()
        except BaseException:
            self.invalidate()  # Callback exceptions must not become a gate.

    def fingerprint(self):
        world, storage, host, db = self.world, self.storage, self.host, self.db
        root = world._scoped_coordinator_attempt()
        actor = world._scoped_attempt()
        entry = world._scoped_entry_budget()
        for captured in (root, actor, entry):
            if captured is not None:
                captured.check()
        storage.paths()
        # Keep strong object references as well as value identities. Changes
        # disable this whole batch, even if a later caller restores the object.
        objects = (storage, host, world, host._storage, host.world, host._authority,
                   root, getattr(root, 'world', None), getattr(root, 'permit', None),
                   actor, getattr(actor, 'permit', None), entry,
                   world._scoped_coordinator(), _activation_scope.get())
        sources = tuple(sorted((id(s), id(s.host), id(s.world), id(s.host._storage),
                                id(s.host.world), id(s.host._authority),
                                str(s.actor_path), s.actor_file, s.host._closed,
                                s.host._closing.is_set()) for s in world._private_sources))
        files = tuple((str(p), p.stat().st_dev, p.stat().st_ino)
                      for p in (storage.world_path, storage.actor_path))
        from .source_attestation import FILES, source_digest
        def source_check():
            for captured in (root, actor, entry):
                if captured is not None:captured.check()
        source_bytes = tuple((name, bytes.fromhex(source_digest(Path(__file__).with_name(name), source_check)))
                             for name in sorted(FILES))
        values = (db.total_changes, db.in_transaction,
                  tuple(tuple(r) for r in db.execute('PRAGMA database_list')),
                  storage.world_path, storage.world_file, storage.actor_path,
                  storage.actor_file, str(world.db_path), str(host.db_path),
                  host._bound_world_id, host._closed, host._closing.is_set(),
                  files, sources, source_bytes,
                  tuple((getattr(x, 'deadline', None), getattr(x, 'busy_deadline', None))
                        for x in (root, actor, entry)))
        return objects, values

    def unchanged(self, storage, db):
        current = self.fingerprint()
        same = (storage is self.storage and db is self.db and db.in_transaction
                and len(current[0]) == len(self.initial[0])
                and all(a is b for a, b in zip(current[0], self.initial[0]))
                and current[1] == self.initial[1])
        if not same:
            self.invalidate()
        return same and not self.tainted

    def audit(self, storage, db, person):
        try:
            valid = self.unchanged(storage, db)
            if valid and not self.running and person in self.successes:
                return
            if self.running:
                self.invalidate()
            self.running = True
            storage.audit(db, person)
            if self.unchanged(storage, db):
                self.successes.add(person)
        except BaseException:
            self.invalidate()
            raise
        finally:
            self.running = False


@contextmanager
def _history_audit_scope(world, storage, db):
    from .actor_execution import PrivateStore
    from .world import _AttemptConnection
    import types
    stamp = PrivateStore._open.__globals__.get('_SOURCE_GENERATION') if isinstance(PrivateStore._open, types.FunctionType) else None
    # No nesting may overwrite the outer batch's callback. Exact native types
    # and original source-defined lifecycle are prerequisites, not a getter.
    eligible = (_history_audit_batch.get() is None and type(storage) is PrivateStore
                and type(db) in (sqlite3.Connection, _AttemptConnection)
                and isinstance(sqlite3.connect, types.BuiltinFunctionType)
                and stamp is not None and stamp['matched']
                and all(_history_original_method(f, stamp) for f in
                        (storage._open, storage.transaction, storage._transaction)))
    if not eligible:
        yield
        return
    batch = _HistoryAuditBatch(world, storage, db)
    db.set_trace_callback(batch.observe)
    token = _history_audit_batch.set(batch)
    try:
        yield
    finally:
        batch.invalidate()
        _history_audit_batch.reset(token)
        db.set_trace_callback(None)  # Restore the proved original no-callback state.


def native_audit(storage, db, row):
    """Full private lineage, separate from the non-recursive row material proof."""
    from .world import WorldStore
    WorldStore._lifecycle_check(db,latest(db))
    person=row['person_id']
    if exists(db,'actor_session_heads','actor') and db.execute('SELECT 1 FROM actor.actor_session_heads WHERE person_id=?',(person,)).fetchone():
        batch = _history_audit_batch.get()
        if batch is not None and batch.db is db:
            batch.audit(storage, db, person)
        else:
            storage.audit(db,person)
        return
    from .actor_execution import ActorRunError
    untouched = row['phase']=='pending' or (row['phase']=='technical_blocked' and row['resume_phase']=='pending')
    if not untouched or any(exists(db,table,'actor') and db.execute(f'SELECT 1 FROM actor.{table} WHERE person_id=?',(person,)).fetchone()
                            for table in ('actor_intents','actor_calls','actor_intent_execution')):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
    session=db.execute('SELECT messages,seen FROM actor.actor_sessions WHERE person_id=?',(person,)).fetchone()
    if session is not None and any(json.loads(raw)!=[] for raw in session):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')


def acquisition_proof(world, db, state, person, reference):
    row = activation_row(world, db, state, person, reference)
    if row is None:
        return
    storage, identity = source(world)
    if 'actor' not in {r[1] for r in db.execute('PRAGMA database_list')}:
        db.execute('ATTACH DATABASE ? AS actor', (storage.actor_path.as_uri()+'?mode=ro',))
    registration(db, state['world_id'], identity)
    try:
        phase_proof(world, db, row)
        native_audit(storage,db,row)
    except (sqlite3.DatabaseError, ValueError, TypeError, KeyError, IndexError):
        unproven()
    storage.paths()


def durable_body(world, db, state, person, intent, body, execution):
    row = activation_row(world,db,state,person,execution=execution,intent=intent)
    if row is None:
        return
    storage, identity = source(world)
    if 'actor' not in {r[1] for r in db.execute('PRAGMA database_list')}:
        db.execute('ATTACH DATABASE ? AS actor',(storage.actor_path.as_uri()+'?mode=ro',))
    registration(db,state['world_id'],identity)
    private_proof(world,db,person,execution,intent=intent)
    if row['phase']=='technical_blocked':
        raise WorldError('ACTIVATION_TECHNICAL_BLOCKED', '原激活已技术阻塞，禁止兑现或提交')
    # Use the storage owner's complete P022 request/consumed/base/call proof
    # under this actual main transaction, including known-receipt retries.
    storage.audit(db, person)
    original = db.execute('SELECT body FROM actor.actor_intents WHERE intent_id=? AND person_id=?',(intent,person)).fetchone()
    if original is None or json.loads(original['body']) != body:
        raise WorldError('ACTIVATION_BINDING_CONFLICT', '不得替换原意图载荷')
    storage.paths()


# Only actual business producers added by a demonstrated vertical behavior.
DOMAIN_ACTIONS = {
    'INSUFFICIENT_GRAIN': {'ship','military_supply','military_move','military_garrison','military_engage'},
    'NOT_AUTHORIZED': {'ship','command','respond','start_production','ship_material','military_supply','military_move','military_garrison','military_engage'},
    'ALREADY_THERE': {'travel','military_move'},
    'NOT_AT_LOCATION': {'investigate','send','command','ship','travel','respond','start_production','ship_material','military_supply','military_move','military_garrison','military_engage','meet'},
    'INSUFFICIENT_MATERIALS': {'start_production','ship_material'}, 'INSUFFICIENT_LABOR': {'start_production'},
    'NO_TROOPS': {'military_supply','military_move','military_garrison','military_engage'},
    'HOSTILE_PRESENT': {'military_garrison'}, 'NO_ENEMY': {'military_engage'}}
# Narrow producers only; original guards, receipt schema and budgets unchanged.
DOMAIN_ACTIONS['NOT_AUTHORIZED'] |= {'start_project','continue_project','interrupt_project'}
DOMAIN_ACTIONS['NOT_AT_LOCATION'] |= {'start_project','continue_project','interrupt_project','share_project_knowledge'}
DOMAIN_ACTIONS['INSUFFICIENT_MATERIALS'] |= {'start_project','continue_project'}
DOMAIN_ACTIONS['INSUFFICIENT_LABOR'] |= {'start_project','continue_project'}
DOMAIN_ACTIONS['INSUFFICIENT_TOOLS'] = {'start_project','continue_project'}
DOMAIN_ACTIONS['KNOWLEDGE_REQUIRED'] = {'start_project','continue_project','share_project_knowledge'}
DOMAIN_ACTIONS['PROJECT_NOT_READY'] = {'continue_project','interrupt_project'}
# Only actual World lifecycle DomainActionError producers, never protocol faults.
DOMAIN_ACTIONS['NOT_AT_LOCATION'] |= {'resolve_physical_attempt','write_office_document','read_office_document'}
DOMAIN_ACTIONS['NOT_AUTHORIZED'] |= {'write_office_document','read_office_document'}
DOMAIN_ACTIONS['PERSONAL_MEANS_REQUIRED'] = {'resolve_physical_attempt'}
DOMAIN_ACTIONS['TARGET_NOT_ALIVE'] = {'resolve_physical_attempt'}
DOMAIN_ACTIONS['MORTAL_INJURY_PENDING'] = {'resolve_physical_attempt'}


def rejection_proof(world, db, row):
    """Row-level proof only; callers separately run the complete native audit.

    Keeping this non-recursive lets the audit prove historical rejected leaves.
    """
    from .actor_execution import canonical, digest
    blocked = row['phase']=='technical_blocked' and row['resume_phase']=='rejected' and row['private_closed']==0
    nulls = ('receipt_sha256','promoted_version') if blocked else ('receipt_sha256','promoted_version','resume_phase','diagnostic_code')
    if ((row['phase'] != 'rejected' and not blocked) or row['private_closed'] not in (0, 1)
            or any(row[k] is not None for k in nulls)):
        unproven()
    private = db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?',
                         (row['intent_id'],row['person_id'])).fetchone()
    meta = db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (row['intent_id'],)).fetchone()
    actual = db.execute('SELECT * FROM main.intents WHERE id=?', (row['intent_id'],)).fetchone()
    saved = db.execute('SELECT * FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?',
                       (row['world_id'],row['activation_id'])).fetchone() if exists(db,'coordinator_activation_outcomes') else None
    if (private is None or meta is None or actual is None or saved is None
            or private['state'] != ('rejected' if row['private_closed'] else 'candidate')
            or private['receipt'] is not None or actual['receipt'] is not None
            or actual['person'] != row['person_id'] or meta['person_id'] != row['person_id']
            or meta['world_id'] != row['world_id'] or meta['base_version'] != row['base_session_version']
            or meta['promoted_version'] is not None or private['candidate'] is None
            or digest(private['request']) != row['request_sha256']
            or digest(private['candidate']) != row['candidate_sha256']
            or saved['kind'] != 'domain_rejected' or saved['intent_id'] != row['intent_id']
            or digest(saved['outcome_json']) != saved['outcome_sha256']):
        unproven()
    result = json.loads(saved['outcome_json'])
    metrics=json.loads(private['metrics'])
    if (type(metrics) is not dict or set(metrics)!={'sdk_events','sdk_requests','provider','pid','module_root'}
            or type(metrics['sdk_requests']) is not int or metrics['sdk_requests']<0
            or type(metrics['pid']) is not int or metrics['pid']<0
            or any(type(metrics[k]) is not str or not metrics[k] for k in ('provider','module_root'))):
        unproven()
    events=metrics['sdk_events']
    if not ((type(events) is int and events>=0) or
            (type(events) is dict and all(type(k) is str and type(v) is int and v>=0 for k,v in events.items()))):
        unproven()
    fields = {'status','world_id','window_id','activation_id','person_id','intent_id','code','origin',
              'action','decision_version','effect_index','request_sha256','candidate_sha256',
              'payload_sha256','decision_sha256','effects'}
    request = json.loads(private['request']); payload = json.loads(actual['payload']); body = json.loads(private['body'])
    if (type(result) is not dict or set(result) != fields or saved['outcome_json'] != canonical(result)
            or request.get('person_id') != row['person_id'] or request.get('intent_id') != row['intent_id']
            or not body_matches(request, body, private['candidate']) or payload.get('body') != body
            or any(result[k] != row[k] for k in ('world_id','window_id','activation_id','person_id','intent_id','request_sha256','candidate_sha256'))
            or result['status'] != 'domain_rejected' or result['effects'] != []
            or result['payload_sha256'] != digest(actual['payload'])
            or result['code'] != row['terminal_reason']
            or result['action'] not in DOMAIN_ACTIONS.get(result['code'], set())
            or result['origin'] not in ('world.apply_action','adjudication.execute')):
        unproven()
    world._validate_body(body)
    if result['origin']=='world.apply_action':
        if (set(payload)!={'body','ruling','reason'} or payload['ruling'] not in ('大失败','失败','成功','大成功')
                or type(payload['reason']) is not str or not payload['reason'].strip() or len(payload['reason'])>8000
                or result['action'] != body['action'] or any(result[k] is not None for k in ('decision_version','effect_index','decision_sha256'))):
            unproven()
    else:
        if set(payload)!={'body'}:
            unproven()
        version=result['decision_version'];index=result['effect_index']
        if type(version) is not int or version<1 or type(index) is not int or index<0 or body['action']!='propose':
            unproven()
        proposal=db.execute('SELECT world_id,person_id,text FROM dm_proposals WHERE intent_id=?',(row['intent_id'],)).fetchone()
        decision=db.execute('SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1',(row['intent_id'],)).fetchone()
        if proposal is None or tuple(proposal)!=(row['world_id'],row['person_id'],body['text']) or decision is None or decision['version']!=version or digest(decision['data'])!=result['decision_sha256']:
            unproven()
        data=json.loads(decision['data'])
        if data['world_id']!=row['world_id'] or type(data['effects']) is not list or not 0<=index<len(data['effects']) or data['effects'][index]['action']!=result['action']:
            unproven()
        from .adjudication import Adjudicator
        from types import SimpleNamespace
        if any(body.get('action') in _LIFECYCLE_ACTIONS for body in data['effects']):
            read=lifecycle_native_read(db)
            try:
                compiled = _lifecycle_compile(world,data,read)
                _lifecycle_rejection(read,world,row,result,data)
            except BaseException:
                read.fail()
                raise
            read.close()
        else:
            compiled = Adjudicator._compile(SimpleNamespace(world=world), data)
        for i, effect in enumerate(compiled):
            if db.execute('SELECT 1 FROM intents WHERE id=?',(Adjudicator._child_id(row['intent_id'],version,i),)).fetchone():
                unproven()
        if db.execute("SELECT 1 FROM dm_timeline WHERE intent_id=? AND kind='commit'",(row['intent_id'],)).fetchone():
            unproven()
    return result


def rejected_result(world, db, person, intent, execution=None):
    row = activation_row(world, db, latest(db), person, execution=execution, intent=intent)
    if row is not None and row['phase'] == 'rejected':
        return rejection_proof(world, db, row)
    return None


def effect_or_reject(world, db, state, person, intent, execution, effect, *, decision=None, version=None):
    """One actual effect region, one savepoint, no tentative state escapes."""
    from copy import deepcopy
    from .world import DomainActionError
    from .actor_execution import canonical, digest
    row = activation_row(world, db, state, person, execution=execution, intent=intent)
    if row is None:
        return effect(state)
    if row['phase'] != 'candidate':
        unproven()
    tentative = deepcopy(state)
    db.execute('SAVEPOINT domain_effect')
    try:
        result = effect(tentative)
    except DomainActionError as error:
        db.execute('ROLLBACK TO domain_effect')
        db.execute('RELEASE domain_effect')
        if type(error) is not DomainActionError or error.action not in DOMAIN_ACTIONS.get(error.code, set()):
            raise
        if error.action in _LIFECYCLE_ACTIONS and not _lifecycle_domain_source(error):
            raise
        actual = db.execute('SELECT person,payload,receipt FROM intents WHERE id=?', (intent,)).fetchone()
        if actual is None or actual['person'] != person or actual['receipt'] is not None:
            unproven()
        result = dict(status='domain_rejected', world_id=state['world_id'],window_id=row['window_id'],
                      activation_id=row['activation_id'],person_id=person,intent_id=intent,
                      code=error.code,origin='world.apply_action' if decision is None else 'adjudication.execute',action=error.action,
                      decision_version=version,effect_index=None if decision is None else error.effect_index,request_sha256=row['request_sha256'],
                      candidate_sha256=row['candidate_sha256'],payload_sha256=digest(actual['payload']),
                      decision_sha256=None if decision is None else digest(canonical(decision)),effects=[])
        outcome_schema(db)
        raw=canonical(result)
        db.execute('INSERT INTO coordinator_activation_outcomes VALUES (?,?,?,?,?,?)',
                   (state['world_id'],row['activation_id'],intent,'domain_rejected',raw,digest(raw)))
        db.execute("UPDATE coordinator_activations SET phase='rejected',private_closed=0,terminal_reason=? WHERE world_id=? AND activation_id=? AND phase='candidate'",
                   (error.code,state['world_id'],row['activation_id']))
        return result
    except BaseException:
        db.execute('ROLLBACK TO domain_effect')
        db.execute('RELEASE domain_effect')
        raise
    db.execute('RELEASE domain_effect')
    state.clear();state.update(tentative)
    return result


def outcome_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS coordinator_activation_outcomes (
        world_id TEXT NOT NULL,activation_id TEXT NOT NULL,intent_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('committed','domain_rejected')),outcome_json TEXT NOT NULL,
        outcome_sha256 TEXT NOT NULL,PRIMARY KEY(world_id,activation_id),UNIQUE(world_id,intent_id))''')
    _history_cover(db)


def committed(world, db, state, person, intent, receipt, execution):
    from .actor_execution import digest, canonical
    row = activation_row(world,db,state,person,execution=execution,intent=intent)
    if row is None:
        return
    if row['phase'] != 'candidate':
        unproven()
    original = db.execute('SELECT body FROM actor.actor_intents WHERE intent_id=? AND person_id=?',(intent,person)).fetchone()
    if original is None:
        unproven()
    body = json.loads(original['body'])
    if '_p04' in state and body['action']=='propose':
        decision = db.execute('SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1',(intent,)).fetchone()
        if decision is None or decision['version']!=receipt.get('decision_version'):
            unproven()
        effects = json.loads(decision['data'])['effects']
        if effects and all(e['action']=='wait' for e in effects):
            body = {'action':'wait'}
    world._p04_committed(state,person,body)
    db.execute('''CREATE TABLE IF NOT EXISTS coordinator_activation_outcomes (
        world_id TEXT NOT NULL,activation_id TEXT NOT NULL,intent_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('committed','domain_rejected')),outcome_json TEXT NOT NULL,
        outcome_sha256 TEXT NOT NULL,PRIMARY KEY(world_id,activation_id),UNIQUE(world_id,intent_id))''')
    _history_cover(db)
    raw = canonical(receipt)
    db.execute('INSERT INTO coordinator_activation_outcomes VALUES (?,?,?,?,?,?)',
               (state['world_id'],row['activation_id'],intent,'committed',raw,digest(raw)))
    db.execute("UPDATE coordinator_activations SET phase='world_committed',receipt_sha256=? WHERE world_id=? AND activation_id=?",
               (digest(raw),state['world_id'],row['activation_id']))


@contextmanager
def maintenance_source(world):
    # Heartbeats must never wait behind a provider/configuration holder. A
    # rejected leaf may renew only when this same nonblocking source capture
    # can stay held through its full proof and the native lease commit.
    lock=world._private_sources_lock
    captured=lock.acquire(blocking=False)
    try:
        yield captured
    finally:
        if captured:
            lock.release()


@contextmanager
def source_lock(world, attempt=None):
    # One order for all ledger/effect/private paths: pipeline -> config -> SQL.
    # Waiting for configuration must retain, not restart, the original budget.
    captured = world._scoped_entry_budget() or world._scoped_attempt() or world._scoped_coordinator_attempt() or attempt
    lock = world._private_sources_lock
    if captured is None:
        from .actor_execution import EntryBudget, ExecutionPolicy
        import threading
        # Raw trusted World callers have no Host cancellation event, but may
        # not bypass the ordinary finite configuration-contention budget.
        captured = EntryBudget(ExecutionPolicy(), threading.Event())
    from .actor_execution import PreparationBudget
    if type(captured) is PreparationBudget:
        with captured.locked(lock):
            yield
        return
    while True:
        captured.check()
        if lock.acquire(timeout=.025):
            break
    try:
        captured.check()
        yield
    finally:
        lock.release()


def register_source(world, source):
    # Called only by the actual PrivateStore constructor; never touches storage.
    with source_lock(world):
        world._private_sources.add(source)


def source(world, actor_db_path=None):
    from .actor_execution import PrivateStore
    with source_lock(world):
        sources = list(world._private_sources)
    active = []
    identities = set()
    for item in sources:
        host = item.host
        if getattr(host, '_closed', False):
            continue
        if (type(item) is not PrivateStore or item.world is not world
                or host._authority is not world or host.world is not world
                or getattr(host, '_storage', None) is not item or host._closing.is_set()):
            unproven()
        try:
            item.paths()
            if not item.actor_path.is_file() or item.actor_file is None:
                unproven()
            identity = (str(item.actor_path), *item.actor_file)
        except OSError:
            unproven()
        identities.add(identity)
        active.append(item)
    if len(identities) != 1:
        unproven()
    actual = active[0]
    if actor_db_path is not None and Path(actor_db_path).resolve() != actual.actor_path:
        unproven()
    return actual, list(next(iter(identities)))


def latest(db):
    if _lc_ro1127_context.get() is not None:
        read = _lc_ro1132_reader_for(db)
        value = _lifecycle_native_sql_json(read, 'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1')
        if value is None:unproven()
        return value
    row = db.execute('SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1').fetchone()
    if row is None:
        unproven()
    return json.loads(row[0])


def private_binding(db, world_id):
    try:
        binding = db.execute('SELECT singleton,world_id FROM actor.actor_world_binding').fetchall()
        if [tuple(r) for r in binding] != [(1, world_id)]:
            unproven()
        for table, columns in (
                ('actor_world_binding', ['singleton', 'world_id']),
                ('actor_sessions', ['person_id', 'messages', 'seen']),
                ('actor_intents', ['intent_id', 'person_id', 'body', 'state', 'candidate', 'metrics', 'receipt', 'consumed', 'request']),
                ('actor_calls', ['call_id', 'intent_id', 'person_id'])):
            if [r[1] for r in db.execute('PRAGMA actor.table_info('+table+')')] != columns:
                unproven()
    except sqlite3.DatabaseError:
        unproven()


@contextmanager
def ledger_transaction(world, storage, identity, *, _precommit=None):
    # Registry configuration cannot change between the final proof and commit.
    # RLock permits same-thread fault/interleaving probes, caught by final proof.
    with source_lock(world):
        options={} if _precommit is None else dict(_precommit=_precommit)
        with storage.transaction(initialize=True, attempt=world._scoped_coordinator_attempt(), **options) as db:
            if source(world)[1] != identity:
                unproven()
            private_binding(db, storage.host._bound_world_id)
            yield db
            if source(world)[1] != identity:
                unproven()
            private_binding(db, storage.host._bound_world_id)
            # Persisted snapshot proof precedes storage original final root SQL.
            world._lifecycle_check(db, latest(db))


# Import only already loaded source owners. The anchors are comparisons, never
# substitutes for the live restore, execute, or any of their consumers.
from . import world as _pristine1193_world, actor_execution as _pristine1193_actor
from types import CodeType as _Pristine1193Code, FunctionType as _Pristine1193Function


def _pristine1193_image(module, owner_name, names):
    space = vars(module)
    source = space['_SOURCE_GENERATION']
    owner = space if owner_name is None else space.get(owner_name)
    if (source['namespace'] is not space or not source['matched']
            or owner_name is not None and not any(owner is item for item in source['objects'])):
        return ()
    code = source['code']
    if owner_name is not None:
        code = next((c for c in code.co_consts if type(c) is _Pristine1193Code
                     and c.co_name == owner_name), None)
        if code is None:
            return ()
    namespace = owner if owner_name is None else vars(owner)
    records = []
    for name in names:
        descriptor = namespace.get(name)
        function = descriptor.__func__ if type(descriptor) is staticmethod else descriptor
        original = next((c for c in code.co_consts if type(c) is _Pristine1193Code
                         and c.co_name == name), None)
        if (type(function) is not _Pristine1193Function or function.__code__ is not original
                or function.__globals__ is not space):
            return ()
        records.append((namespace, name, descriptor, function, original))
    return tuple(records)


_PRISTINE1193_WORLD = _pristine1193_world.WorldStore
_PRISTINE1193_DB = _pristine1193_world._AttemptConnection
_PRISTINE1193_ENTRY = _pristine1193_actor.PreparationBudget
_PRISTINE1193_ENTRY_SCOPE = _pristine1193_world._actor_entry_scope
_PRISTINE1193_WORLD_METHODS = _pristine1193_image(_pristine1193_world, 'WorldStore', (
    '_restore_geography', '_check_frozen_pathology', '_check_frozen_economy',
    '_check_frozen_military', '_check_frozen_p04', '_lifecycle_check', '_military_accounts'))
_PRISTINE1193_DB_METHODS = _pristine1193_image(_pristine1193_world, '_AttemptConnection', (
    'execute', '_bounded'))
_PRISTINE1193_ENTRY_METHODS = _pristine1193_image(_pristine1193_actor, 'PreparationBudget', (
    'check', 'native_call', '_busy_elapsed', 'wait', 'remaining'))
_PRISTINE1193_WORLD_FUNCTIONS = _pristine1193_image(_pristine1193_world, None, (
    '_lc_root_json', '_lc_root_row', '_lc_json', '_lc_canonical', '_lc_geo_borrow',
    '_lc_geo_fetch_decode', '_lc_geo_query', '_lc_geo1137_close', '_lc_optin',
    '_lifecycle_shape', '_lc_value_memory', '_lc_history_scope', '_lc_history_owner'))


def _pristine1193_lane(world, db, restore, owner, memory):
    # No getters on unknown world/DB/Entry implementations. This is a
    # refund predicate, not a new business admission or an alternate SQL path.
    if (type(world) is not _PRISTINE1193_WORLD or type(db) is not _PRISTINE1193_DB
            or type(owner) is not _LifecycleValueOwner or type(memory) is not _LifecycleLiveLedger
            or owner.memory is not memory or _lifecycle_value_context.get() is not owner
            or _lifecycle_live_context.get() is not memory or _lc_ro1127_context.get() is not None
            or owner.ro1127 is not None or owner.closed or owner.failed or memory.failed
            or owner.control_errors):
        return None
    scope = _PRISTINE1193_ENTRY_SCOPE.get()
    if (type(scope) is not tuple or len(scope) != 2 or scope[0] is not world or type(scope[1]) is not _PRISTINE1193_ENTRY
            or vars(db).get('_attempt') is not scope[1]
            or vars(scope[1]).get('_lifecycle_value_owner') is not owner):
        return None
    if (not _PRISTINE1193_WORLD_METHODS or not _PRISTINE1193_DB_METHODS
            or not _PRISTINE1193_ENTRY_METHODS or not _PRISTINE1193_WORLD_FUNCTIONS
            or not _pristine1193_retry_wrapper(scope[1])
            or _pristine1193_world.WorldStore is not _PRISTINE1193_WORLD
            or _pristine1193_world._AttemptConnection is not _PRISTINE1193_DB
            or _pristine1193_actor.PreparationBudget is not _PRISTINE1193_ENTRY
            or _pristine1193_world._actor_entry_scope is not _PRISTINE1193_ENTRY_SCOPE):
        return None
    if ('_restore_geography' in vars(world) or
            not _pristine1193_static_match(_PRISTINE1193_WORLD_METHODS[0], restore)):
        return None
    world_class = vars(_PRISTINE1193_WORLD)
    db_class = vars(_PRISTINE1193_DB)
    entry_class = vars(_PRISTINE1193_ENTRY)
    if (any(name in world_class or name in db_class or name in entry_class
            for name in ('__getattribute__', '__getattr__'))
            or any(name in db_class or name in vars(db)
                   for name in ('row_factory', 'text_factory', 'in_transaction'))
            or 'execute' in vars(db) or '_bounded' in vars(db)
            or any(item[1] in vars(scope[1]) for item in _PRISTINE1193_ENTRY_METHODS)):
        return None
    # Exact native Connection subtype, default attribute resolution, actual
    # instance unshadowed, and both active class dispatch edges unchanged.
    for image in (_PRISTINE1193_WORLD_METHODS, _PRISTINE1193_DB_METHODS,
                  _PRISTINE1193_ENTRY_METHODS, _PRISTINE1193_WORLD_FUNCTIONS):
        for namespace, name, descriptor, function, code in image:
            if namespace.get(name) is not descriptor or function.__code__ is not code:
                return None
    for function, code in _PRISTINE1193_P04:
        if globals().get(code.co_name) is not function or function.__code__ is not code:
            return None
    if not _pristine1193_world._lc1196_consumer_lane(world, 'restore', 'proof', 'attempt'):
        return None
    if ((db.row_factory is not None and db.row_factory is not sqlite3.Row)
            or db.text_factory is not str):
        return None
    return scope


def _pristine1193_retry_wrapper(entry):
    # @contextmanager stores a stdlib wrapper over this exact source function.
    wrapper = _PRISTINE1193_CONTENTION
    source = _PRISTINE1193_CONTENTION_SOURCE
    return (type(wrapper) is _Pristine1193Function
            and vars(_PRISTINE1193_ENTRY).get('_contention') is wrapper
            and '_contention' not in vars(entry)
            and wrapper.__code__ is _PRISTINE1193_CONTEXTMANAGER_CODE
            and wrapper.__globals__ is contextmanager.__globals__
            and wrapper.__closure__ is not None and len(wrapper.__closure__) == 1
            and getattr(wrapper.__closure__[0], 'cell_contents', None) is source
            and type(source) is _Pristine1193Function
            and source.__globals__ is vars(_pristine1193_actor)
            and source.__code__ is _PRISTINE1193_CONTENTION_CODE)


def _pristine1193_static_match(record, actual):
    # Compare the actual function resolved for the original call, not a class
    # lookup alone. A modified default could select the cold/material branch.
    return (type(record[2]) is staticmethod and actual is record[3]
            and actual.__defaults__ is None
            and type(actual.__kwdefaults__) is dict
            and len(actual.__kwdefaults__) == 2
            and 'cold_decode' in actual.__kwdefaults__
            and actual.__kwdefaults__['cold_decode'] is None
            and actual.__kwdefaults__.get('_pristine_loan') is False)


def _pristine1193_live_ticket(lease, memory, ticket_start):
    return (type(lease) is _LifecycleLiveLease and lease.ledger is memory
            and lease.ro1127 is None and lease.ticket > ticket_start
            and not lease.released and memory.leases.get(lease.ticket) == lease.charge)


def _pristine1193_geo_ticket(db, loan, owner, loan_start, ticket_start):
    return (len(owner.loans) > loan_start and type(loan) is _LifecycleValueLoan
            and _pristine1193_world._lc1196_geo_paid(db, owner, loan, loan.value, ticket_start))


def pristine_main(world, db, state):
    world._execution_identity(db, state['world_id'])
    if not exists(db, 'scheduler_genesis'):
        unproven()
    rows = db.execute('SELECT * FROM scheduler_genesis').fetchall()
    first = db.execute('SELECT seq,state FROM snapshots ORDER BY seq LIMIT 1').fetchone()
    if (len(rows) != 1 or tuple(rows[0]) != (state['world_id'], 1, first[0], _bootstrap_derived(db, 'genesis-sha', first[1],
                    lambda: sha(_bootstrap_baseline(db, first[1]))))
            or first[0] != 1):
        unproven()
    baseline = _bootstrap_baseline(db, first[1])
    def normalized(snapshot):
        value = json.loads(_json(snapshot))
        if (type(value['time']) is not int or value['time'] != 0
                or value['status'] not in ('user_paused', 'running')
                or any(e['kind'] != 'card_edited' for e in value['events'])):
            unproven()
        value.pop('revision'); value.pop('status'); value.pop('events')
        for person in value['persons']:
            for key in ('card', 'card_version', 'card_author'):
                person.pop(key, None)
        return value
    original = _bootstrap_derived(db, 'normalized', first[1], lambda: normalized(baseline))
    def validate_snapshot(seq, raw):
        snapshot = json.loads(raw)
        if (snapshot['revision'] != seq or _bootstrap_derived(
                db, 'snapshot-normalized', raw, lambda: normalized(snapshot)) != original):
            unproven()
        # Resolve exactly the callable used below, once, after normalization.
        # A diagnostic override still runs with its original arguments/result
        # semantics, but cannot qualify either success refund.
        restore = world._restore_geography
        owner = _lifecycle_value_context.get()
        memory = owner.memory if type(owner) is _LifecycleValueOwner else None
        scope = _pristine1193_lane(world, db, restore, owner, memory)
        mark = None
        if scope is not None:
            mark = (restore, scope, owner, memory, owner.loans, owner.leases,
                    len(owner.loans), len(owner.leases), memory.next_ticket,
                    '_lifecycle' not in baseline and '_lifecycle' not in snapshot)
        loan = restore(db, snapshot, _pristine_loan=True)
        if mark is not None and loan is not None and (
                type(loan) is not _LifecycleValueLoan or '_geography' not in snapshot
                or loan.value is not snapshot['_geography']):
            mark = None
        return loan, mark

    for seq, raw in db.execute('SELECT seq,state FROM snapshots ORDER BY seq'):
        loan, mark = validate_snapshot(seq, raw)
        # Only this successful return crosses the alias lifetime boundary.
        # No catch/finally settlement: retain any debt reachable from failure TBs.
        if mark is not None:
            restore, scope, owner, memory, loans, leases, loan_start, lease_start, ticket_start, legacy = mark
            if (_pristine1193_lane(world, db, restore, owner, memory) is scope
                    and owner.loans is loans and owner.leases is leases
                    and len(loans) >= loan_start and len(leases) >= lease_start
                    and memory.next_ticket >= ticket_start):
                if loan is not None:
                    if not _pristine1193_geo_ticket(db, loan, owner, loan_start, ticket_start):
                        continue
                    _release_pristine_geography(loan)
                if legacy:
                    _release_pristine_legacy_work(owner, loan_start, lease_start, ticket_start)
    for table in ('intents', 'actor_execution_leases', 'actor_coordinator_delegations'):
        if exists(db, table) and db.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone():
            unproven()


def _release_pristine_legacy_work(owner, loan_start, lease_start, ticket_start):
    # Select only tickets from this completed snapshot. The original raw and
    # four cache slots were never backed by these restore-only reservations.
    # Keep all other tickets, including any unclassified loan, in place.
    memory = owner.memory
    if (_lifecycle_value_context.get() is not owner
            or _lifecycle_live_context.get() is not memory
            or _lc_ro1127_context.get() is not None
            or owner.closed or owner.failed or memory.failed or owner.control_errors):
        return
    for i in range(len(owner.loans) - 1, loan_start - 1, -1):
        loan = owner.loans[i]
        if (type(loan) is not _LifecycleValueLoan or loan.owner is not owner
                or loan.memory is not memory or loan.closed or loan.failed
                or len(loan.leases) != 1):
            continue
        lease = loan.leases[0]
        if (type(lease) is _LifecycleLiveLease
                and lease.role in ('world-json:value', 'world-canonical:text')
                and _pristine1193_live_ticket(lease, memory, ticket_start)):
            loan.close()
            owner.loans.pop(i)
    for i in range(len(owner.leases) - 1, lease_start - 1, -1):
        lease = owner.leases[i]
        if (type(lease) is _LifecycleLiveLease
                and lease.role in ('root-query-metadata', 'root-native-row', 'hash-digest',
                           'geo1137-query-first-secondary-recording-fallback')
                and _pristine1193_live_ticket(lease, memory, ticket_start)):
            lease.release()
            owner.leases.pop(i)


def pristine(world, db, state):
    pristine_main(world, db, state)
    private_binding(db, state['world_id'])
    people = {p['id'] for p in state['persons']}
    for person, messages, seen in db.execute('SELECT * FROM actor.actor_sessions'):
        if person not in people or json.loads(messages) != [] or json.loads(seen) != []:
            unproven()
    for table in ('actor_intents', 'actor_calls', 'actor_intent_execution', 'actor_session_heads'):
        if exists(db, table, 'actor') and db.execute(f'SELECT 1 FROM actor.{table} LIMIT 1').fetchone():
            unproven()


def schema(db):
    for sql in (
        '''CREATE TABLE coordinator_scheduler_registration (
        world_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version=1),
        boundary_kind TEXT NOT NULL CHECK(boundary_kind='pristine'), boundary_revision INTEGER NOT NULL CHECK(boundary_revision>0),
        boundary_time INTEGER NOT NULL CHECK(boundary_time=0), evidence_json TEXT NOT NULL,
        evidence_sha256 TEXT NOT NULL, created_parent_epoch INTEGER NOT NULL CHECK(created_parent_epoch>0),
        private_identity_json TEXT NOT NULL)''',
        '''CREATE TABLE coordinator_windows (
        world_id TEXT NOT NULL, window_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>0),
        base_revision INTEGER NOT NULL CHECK(base_revision>0), target_time INTEGER NOT NULL CHECK(target_time>=0),
        horizon_at_open INTEGER NOT NULL CHECK(horizon_at_open>=target_time), advanced_revision INTEGER NOT NULL CHECK(advanced_revision>0),
        state TEXT NOT NULL CHECK(state IN ('open','completed')),
        boundary_kind TEXT NOT NULL CHECK(boundary_kind IN ('normal','horizon')),
        roster_json TEXT NOT NULL, roster_sha256 TEXT NOT NULL,
        created_parent_epoch INTEGER NOT NULL CHECK(created_parent_epoch>0), owner_parent_epoch INTEGER NOT NULL CHECK(owner_parent_epoch>0),
        completed_revision INTEGER,
        PRIMARY KEY(world_id,window_id), UNIQUE(world_id,ordinal))''',
        "CREATE UNIQUE INDEX coordinator_one_open_window ON coordinator_windows(world_id) WHERE state='open'",
        '''CREATE TABLE coordinator_activations (
        world_id TEXT NOT NULL, window_id TEXT NOT NULL, activation_id TEXT NOT NULL,
        roster_index INTEGER NOT NULL CHECK(roster_index>=0), person_id TEXT NOT NULL,
        phase TEXT NOT NULL CHECK(phase IN ('pending','prepared','candidate','world_committed','completed','technical_blocked','rejected','skipped')),
        intent_id TEXT, observation_revision INTEGER, card_version INTEGER,
        base_session_version INTEGER, request_sha256 TEXT, candidate_sha256 TEXT,
        receipt_sha256 TEXT, promoted_version INTEGER, terminal_reason TEXT,
        private_closed INTEGER NOT NULL DEFAULT 0 CHECK(private_closed IN (0,1)),
        resume_phase TEXT, diagnostic_code TEXT,
        created_parent_epoch INTEGER NOT NULL CHECK(created_parent_epoch>0), owner_parent_epoch INTEGER NOT NULL CHECK(owner_parent_epoch>0),
        owner_actor_epoch INTEGER,
        PRIMARY KEY(world_id,activation_id), UNIQUE(world_id,window_id,person_id),
        UNIQUE(world_id,window_id,roster_index), UNIQUE(world_id,intent_id))'''):
        db.execute(sql)
    _history_cover(db)


def one_open_predicate(sql):
    # Recognize only the contract's single equality, not arbitrary SQL semantics.
    # sqlite_master contains SQLite-parsed DDL. With exactly one equality,
    # grouping parentheses cannot alter its meaning. Quoted tokens stay intact.
    if type(sql) is not str:
        return False
    token = re.compile(r'''\s+|--[^\n]*(?:\n|$)|/\*[\s\S]*?\*/|'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`|\[[^\]]*\]|[A-Za-z_][A-Za-z_0-9]*|==|[=(),.;]''')
    tokens = []
    position = 0
    while position < len(sql):
        match = token.match(sql, position)
        if match is None:
            return False
        value = match.group()
        position = match.end()
        if not value.isspace() and not value.startswith(('--', '/*')):
            tokens.append(value)
    where = [i for i, value in enumerate(tokens) if value.upper() == 'WHERE']
    if len(where) != 1:
        return False
    predicate = tokens[where[0]+1:]
    if predicate and predicate[-1] == ';':
        predicate.pop()
    predicate = [value for value in predicate if value not in ('(', ')')]
    if len(predicate) != 3 or predicate[1] not in ('=', '=='):
        return False
    def state_column(value):
        return value.lower() in ('state', '"state"', '`state`', '[state]')
    left, _, right = predicate
    return ((state_column(left) and right == "'open'")
            or (left == "'open'" and state_column(right)))


def registration(db, world_id, identity):
    private_binding(db, world_id)
    try:
        columns = ['world_id', 'schema_version', 'boundary_kind', 'boundary_revision',
                   'boundary_time', 'evidence_json', 'evidence_sha256', 'created_parent_epoch', 'private_identity_json']
        if [r[1] for r in db.execute('PRAGMA table_info(coordinator_scheduler_registration)')] != columns:
            unproven()
        for table, names in (
                ('coordinator_windows', 'world_id window_id ordinal base_revision target_time horizon_at_open advanced_revision state boundary_kind roster_json roster_sha256 created_parent_epoch owner_parent_epoch completed_revision'),
                ('coordinator_activations', 'world_id window_id activation_id roster_index person_id phase intent_id observation_revision card_version base_session_version request_sha256 candidate_sha256 receipt_sha256 promoted_version terminal_reason private_closed resume_phase diagnostic_code created_parent_epoch owner_parent_epoch owner_actor_epoch')):
            if [r[1] for r in db.execute('PRAGMA table_info('+table+')')] != names.split():
                unproven()
        indexes = {r[1]:r for r in db.execute('PRAGMA index_list(coordinator_windows)')}
        index = indexes.get('coordinator_one_open_window')
        if index is None or index[2] != 1 or index[4] != 1:
            unproven()
        if [r[2] for r in db.execute('PRAGMA index_info(coordinator_one_open_window)')] != ['world_id']:
            unproven()
        index_sql = db.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='coordinator_one_open_window'").fetchone()
        if index_sql is None or not one_open_predicate(index_sql[0]):
            unproven()
        db.row_factory = sqlite3.Row
        rows = db.execute('SELECT * FROM coordinator_scheduler_registration').fetchall()
        if len(rows) != 1:
            unproven()
        row = dict(rows[0])
        if (row['world_id'] != world_id or type(row['schema_version']) is not int or row['schema_version'] != 1
                or row['boundary_kind'] != 'pristine' or type(row['boundary_time']) is not int or row['boundary_time'] != 0
                or json.loads(row['private_identity_json']) != identity):
            unproven()
        for key in ('boundary_revision','created_parent_epoch'):
            if type(row[key]) is not int or not 1 <= row[key] <= MAX_INT:
                unproven()
        evidence = json.loads(row['evidence_json'])
        if (evidence != dict(kind='pristine', world_id=world_id, revision=row['boundary_revision'])
                or type(evidence.get('revision')) is not int or sha(evidence) != row['evidence_sha256']):
            unproven()
        return row
    except (ValueError, KeyError, TypeError, sqlite3.DatabaseError):
        unproven()


def inspect(world, actor_db_path):
    with source_lock(world), world._connection(readonly=True) as db:
        db.execute('BEGIN')
        state = latest(db)
        result = dict(world_id=state['world_id'], revision=state['revision'], time=state['time'],
                      eligible=False, boundary=None, reason='WINDOW_RECOVERY_UNPROVEN')
        try:
            storage, identity = source(world, actor_db_path)
            # URI read-only attach cannot create a missing private file.
            db.execute('ATTACH DATABASE ? AS actor', (storage.actor_path.as_uri()+'?mode=ro',))
            pristine(world, db, state)
            if exists(db, 'coordinator_scheduler_registration'):
                unproven()
            storage.paths()
        except (WorldError, sqlite3.Error, OSError, ValueError, KeyError, TypeError):
            return result
        result.update(eligible=True, reason=None, boundary=dict(kind='pristine', world_id=state['world_id'], revision=state['revision']))
        return result


def register(world, *, coordinator, expected_version, boundary, actor_db_path):
    version = version_shape(expected_version)
    if type(boundary) is not dict or set(boundary) != {'kind', 'world_id', 'revision'} or boundary != dict(kind='pristine', **version):
        unproven()
    storage, identity = source(world, actor_db_path)
    with storage.host._with_entry_pipeline(), world._coordinator_scope(coordinator):
        with ledger_transaction(world, storage, identity) as db:
            state = latest(db)
            check_version(state, version)
            # initialize=True is only the original shared-lock/root transaction,
            # NOT permission to initialize or silently adopt a private binding.
            if exists(db, 'coordinator_scheduler_registration'):
                return registration(db, state['world_id'], identity)
            pristine(world, db, state)
            schema(db)
            evidence = _json(boundary)
            db.execute('INSERT INTO coordinator_scheduler_registration VALUES (?,?,?,?,?,?,?,?,?)',
                       (state['world_id'], 1, 'pristine', state['revision'], 0, evidence, sha(boundary), coordinator.epoch, _json(identity)))
            return registration(db, state['world_id'], identity)


def _p04_committed_meeting_legacy(read, world_id, at, cause, *, cold_decode=None):
    """Join a real meeting cause to direct submit or committed DM child rows.

    Preview decisions, unsuccessful ruling, domain rejection and uncommitted
    children cannot create an obligation. No new attestation table or witness.
    """
    decode = json.loads if cold_decode is None else cold_decode
    row=read('main.intents',(('id',cause),),first=True)
    if row is None or row['receipt'] is None:raise ValueError('missing meet receipt')
    payload=decode(row['payload']);receipt=decode(row['receipt'])
    body=payload['body']
    if (body.get('action')!='meet' or receipt['world_id']!=world_id
            or receipt['person_id']!=row['person'] or receipt['intent_id']!=cause
            or receipt['status']!='committed' or receipt['time']!=at
            or receipt['ruling'] not in ('成功','大成功')
            or receipt['effect'].get('implemented') is not True
            or receipt['effect'].get('participant')!=body['to']):
        raise ValueError('nonimplemented meeting')
    child='parent' in payload
    if child:
        parent=payload['parent'];version=payload['version']
        if type(version) is not int or version<1:raise ValueError('child version')
        source=read('main.intents',(('id',parent),),first=True)
        decision=read('main.dm_decisions',(('intent_id',parent),('version',version)),first=True)
        if source is None or source['receipt'] is None or decision is None:raise ValueError('child lineage')
        source_payload=decode(source['payload']);parent_receipt=decode(source['receipt'])
        decision=decode(decision['data'])
        matches=[i for i,b in enumerate(decision['effects']) if cause==f'dm:{len(parent)}:{parent}:{version}:{i}' and b==body]
        if (len(matches)!=1 or source['person']!=row['person']
                or source_payload['body']['action']!='propose'
                or parent_receipt['world_id']!=world_id or parent_receipt['person_id']!=row['person']
                or parent_receipt['intent_id']!=parent or parent_receipt['status']!='committed'
                or parent_receipt['time']!=at or parent_receipt['decision_version']!=version
                or receipt['decision_version']!=version or decision['world_id']!=world_id
                or parent_receipt['ruling']!=decision['ruling'] or receipt['ruling']!=decision['ruling']
                or parent_receipt['effect']['effects'][matches[0]]!=receipt['effect']):
            raise ValueError('uncommitted child effect')
    elif payload['ruling']!=receipt['ruling']:
        raise ValueError('direct ruling')
    return row['person'],body['to'],child


def _p04_current_projection_legacy(read, state, *, cold_decode=None):
    """Only P04 opening/meeting projection over existing bound materials.

    Each historical base is joined to the preceding proven opening; latest is
    joined only to its OWN latest opening, never to every old advanced snapshot.
    Read callbacks are native or offlock originals, not a self-reported hash.
    """
    if '_p04' not in state:return
    from .world import WorldStore, _lc_history_scope
    try:
        row=read('main.snapshots',columns=('state',),order=(('seq',False),),limit=1,first=True)
        if row is None:raise ValueError('genesis')
        decode = json.loads if cold_decode is None else cold_decode
        genesis=decode(row[0]);anchor=genesis;previous=None;ordinal=0
        with _lc_history_scope(read,genesis):
            WorldStore._check_frozen_p04(state,genesis)
            _lifecycle_snapshot_proof(read,state,genesis)
            def snapshot(revision):
                row=read('main.snapshots',(('seq',revision),),('state',),first=True)
                if row is None:raise ValueError('opening snapshot')
                value=decode(row[0])
                if value['world_id']!=state['world_id'] or value['revision']!=revision:raise ValueError('snapshot identity')
                WorldStore._check_frozen_p04(value,genesis)
                _lifecycle_snapshot_proof(read,value,genesis)
                return value
            def continuation(start,end):
                def meeting(cause):
                    if cold_decode is None:
                        return _p04_committed_meeting(read,state['world_id'],end['time'],cause)
                    return _p04_committed_meeting_legacy(read,state['world_id'],end['time'],cause,
                                                        cold_decode=cold_decode)
                WorldStore._p04_continuity(start,end,meeting)
            for window in read('main.coordinator_windows',(('world_id',state['world_id']),),order=(('ordinal',False),)):
                if window['advanced_revision']>state['revision']:continue
                ordinal+=1
                if (window['ordinal']!=ordinal or window['advanced_revision']!=window['base_revision']+1
                        or (previous is not None and (previous['state']!='completed'
                            or window['base_revision']<previous['completed_revision']))):
                    raise ValueError('opening order')
                base=snapshot(window['base_revision']);advanced=snapshot(window['advanced_revision'])
                continuation(anchor,base)
                expected=WorldStore._p04_expected(base,advanced,genesis=genesis)
                if (window['target_time']!=WorldStore._p04_target(base) or advanced['time']!=window['target_time']
                        or advanced['horizon']!=window['horizon_at_open']
                        or decode(window['roster_json'])!=expected or advanced['_p04']['dispatch']!=expected):
                    raise ValueError('unproven opening')
                anchor=advanced;previous=window
            continuation(anchor,state)
    except (KeyError,TypeError,ValueError,IndexError,RecursionError):
        raise WorldError('INVALID_EVENT_WAKEUP','常驻当前状态未接回原窗口及已提交会面材料') from None


def _p04_current_projection_owned(read, state):
    """Original ordered opening joins with one retained anchor allocation region."""
    if '_p04' not in state:return
    from .world import WorldStore, _lc_history_scope, _lc_json, _lc_read, _lc_value_memory, _lc_iter_loans, _lc_copy, _lc_close_iterator
    memory = _lc_value_memory(read)
    held = region = None
    anchor = previous = window = raw_loan = None
    iterator = None
    try:
        row = _lc_read(read, 'main.snapshots', columns=('state',), order=(('seq',False),), limit=1, first=True)
        if row is None:raise ValueError('genesis')
        genesis = _lc_json(row[0])
        anchor = genesis
        ordinal = 0
        with _lc_history_scope(read, genesis):
            WorldStore._check_frozen_p04(state, genesis)
            _lifecycle_snapshot_proof(read, state, genesis)
            def snapshot(revision):
                row = _lc_read(read, 'main.snapshots', (('seq',revision),), ('state',), first=True)
                if row is None:raise ValueError('opening snapshot')
                value = _lc_json(row[0])
                if value['world_id'] != state['world_id'] or value['revision'] != revision:raise ValueError('snapshot identity')
                WorldStore._check_frozen_p04(value, genesis)
                _lifecycle_snapshot_proof(read, value, genesis)
                return value
            def continuation(start, end):
                WorldStore._p04_continuity(start, end, lambda cause:
                    _p04_committed_meeting(read, state['world_id'], end['time'], cause))
            def opening(window, ordinal, previous, anchor):
                if (window['ordinal'] != ordinal or window['advanced_revision'] != window['base_revision']+1
                        or (previous is not None and (previous['state'] != 'completed'
                            or window['base_revision'] < previous['completed_revision']))):
                    raise ValueError('opening order')
                base = snapshot(window['base_revision'])
                advanced = snapshot(window['advanced_revision'])
                continuation(anchor, base)
                expected = WorldStore._p04_expected(base, advanced, genesis=genesis)
                if (window['target_time'] != WorldStore._p04_target(base) or advanced['time'] != window['target_time']
                        or advanced['horizon'] != window['horizon_at_open']
                        or _lc_json(window['roster_json']) != expected or advanced['_p04']['dispatch'] != expected):
                    raise ValueError('unproven opening')
                return advanced
            iterator = _lc_iter_loans(read,'main.coordinator_windows',(('world_id',state['world_id']),),order=(('ordinal',False),))
            for raw_loan in iterator:
                window=raw_loan.value
                if window['advanced_revision'] > state['revision']:
                    window=None;raw_loan.close();raw_loan=None
                    continue
                ordinal += 1
                region = _LifecycleValueOwner(memory) if memory is not None else None
                token = _lifecycle_value_context.set(region) if region is not None else None
                if region is not None:region.loans.append(raw_loan)
                current_loan=raw_loan
                raw_loan=None
                try:
                    new_anchor = opening(window, ordinal, previous, anchor)
                except BaseException:
                    if region is not None:region.fail()
                    raise
                finally:
                    if token is not None:_lifecycle_value_context.reset(token)
                anchor = new_anchor
                new_anchor = None
                region.reserve(2048,'opening-scalar-frontier')
                previous = region.take(_lifecycle_copy_borrow(dict(state=window['state'],completed_revision=window['completed_revision']),memory=memory,role='opening-frontier'))
                window=None
                current_loan.close();current_loan=None
                # The inner opening frame has returned; prior anchor aliases
                # are gone before its actual allocation region can be closed.
                if held is not None:held.close()
                held = region
                region = None
            continuation(anchor, state)
    except (KeyError, TypeError, ValueError, IndexError, RecursionError):
        memory.failed=True
        if raw_loan is not None:raw_loan.fail()
        if region is not None:region.fail()
        if held is not None:held.fail()
        raise WorldError('INVALID_EVENT_WAKEUP','常驻当前状态未接回原窗口及已提交会面材料') from None
    except BaseException:
        memory.failed=True
        if raw_loan is not None:raw_loan.fail()
        if region is not None:region.fail()
        if held is not None:held.fail()
        raise
    finally:
        anchor = previous = window = None
        if raw_loan is not None:
            if memory.failed:raw_loan.fail()
            else:raw_loan.close()
        if iterator is not None:_lc_close_iterator(iterator)
        if held is not None and not held.failed:held.close()


def _p04_current_projection(read, state):
    from .world import _lc_value_memory
    memory = _lc_value_memory(read)
    if memory is None:
        return _p04_current_projection_legacy(read, state)
    return _lifecycle_value_invoke(_p04_current_projection_owned, (read, state),
        memory=memory, inputs=(state,), direct_factor=2)


def _lc_cold1136_p04_rows(db, sql, params, *, table, first):
    """Fund the original cursor's real Rows before its original fetch.

    Only the cold adapter supplies SQL. The scalar aggregate is over the same
    transaction, predicate and projection. A first-only consumer never pays
    for the tail. No window payload is decoded by the quote.
    """
    owner = _lifecycle_source_owner()
    entry = _lc_adm1136_budget_context.get()
    if entry is None or not db.in_transaction:
        raise _LifecycleLiveLimit('cold P04 requires the original Entry transaction')
    entry.check()
    owner.reserve(4096, 'cold1146-p04-cursor')
    from .world import _lc_cold1136_P04Failure, _lc_cold1136_p04_db, _lc_geo1137_tuple
    if _lc_cold1136_p04_db.get() is not db:
        raise _LifecycleLiveLimit('cold P04 requires the original cold connection')
    # Keep original execute/prepare error and real description/Row semantics.
    cursor = None
    start = len(owner.control_errors)
    failed = None

    def latch(error):
        if owner.error is error:
            # The nested scalar quote may already have observed A before its close.
            first = (error, owner.traceback, owner.error_context, owner.error_cause)
            if owner.first_failure is None:
                owner.first_failure = first
            else:
                first = owner.first_failure
        else:
            first = _lc_geo1137_tuple(error)
            owner._remember_failure(first)
        if len(owner.control_errors) == start + 1 and owner.control_errors[start][0] is error:
            owner.control_errors[start] = first
        return _lc_cold1136_P04Failure(db, owner, first, start)

    try:
        try:
            cursor = db.execute(sql, params)
            # Price actual columns, not a new schema/required-field gate.
            owner.reserve(1024 * len(cursor.description) +
                          16 * sum(len(field[0]) for field in cursor.description),
                          'cold1146-p04-quote-metadata')
            keys = ['"' + field[0].replace('"', '""') + '"' for field in cursor.description]
            widths = '+'.join("CASE typeof(" + key + ") WHEN 'text' THEN length(CAST(" + key +
                              " AS BLOB)) WHEN 'blob' THEN length(" + key + ") ELSE 8 END" for key in keys)
            source = 'SELECT * FROM (' + sql + ') LIMIT 1' if first else sql
            total, count, maximum = _lc_adm1136_one(db,
                'SELECT coalesce(sum(n),0),count(*),coalesce(max(n),0) FROM '
                '(SELECT ' + widths + ' AS n FROM (' + source + '))', params)
            fields = len(keys)
            _lifecycle_raw_charge(maximum, fields)
            owner.reserve(4 * total + count * (_lifecycle_raw_charge(0, fields) + 32),
                          'cold1146-p04-raw:' + table)
            entry.check()
            value = cursor.fetchone() if first else cursor.fetchall()
            entry.check()
        except BaseException as error:
            failed = latch(error)
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except BaseException as cleanup:
                if failed is None:
                    failed = latch(cleanup)
                else:
                    failed.secondary(cleanup)
    if failed is not None:
        raise failed from None
    return value


def _lc_cold1158_p04_catalog(db, sql, params=()):
    """Original scalar read on the caller-selected exact cold DB."""
    owner = _lifecycle_source_owner()
    budget = getattr(db, '_attempt', None) or _lc_adm1136_budget_context.get()
    if budget is not None:
        budget.check()
    from .world import _lc_cold1136_P04Failure, _lc_geo1137_tuple
    start = len(owner.control_errors)
    cursor = None
    failed = None

    def latch(error):
        first = _lc_geo1137_tuple(error)
        owner._remember_failure(first)
        return _lc_cold1136_P04Failure(db, owner, first, start)

    try:
        try:
            cursor = db.execute(sql, params)
            value = cursor.fetchone()
            if budget is not None:
                budget.check()
        except BaseException as error:
            failed = latch(error)
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except BaseException as cleanup:
                if failed is None:
                    failed = latch(cleanup)
                else:
                    failed.secondary(cleanup)
    if failed is not None:
        raise failed from None
    return value


def _p04_native_current(db, state, *, cold_decode=None):
    if '_p04' not in state:return
    from .world import _lc_optin
    if _lc_optin(state):
        reader = lifecycle_native_read(db)
        try:
            _p04_current_projection(reader, state)
        except BaseException:
            reader.fail()
            raise
        reader.close()
        return
    # initialize may precede scheduler registration. Existing opt-in worlds with
    # no opening still have genesis ledger/dispatch; no schema migration/write.
    catalog = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_windows'"
    if cold_decode is None:
        present=db.execute(catalog).fetchone() is not None
    else:
        _lifecycle_source_owner().reserve(4096, 'cold1146-p04-catalog')
        from .world import _lc_cold1136_p04_db
        if _lc_cold1136_p04_db.get() is db:
            present=_lc_cold1158_p04_catalog(db, catalog) is not None
        else:
            present=_lc_adm1136_one(db, catalog) is not None
    allowed={'main.snapshots':{'seq','state'},'main.coordinator_windows':{'world_id','ordinal'},
             'main.intents':{'id'},'main.dm_decisions':{'intent_id','version'}}
    def read(table,equality=(),columns=None,order=(),limit=None,first=False):
        if table=='main.coordinator_windows' and not present:return []
        if (table not in allowed or any(k not in allowed[table] for k,v in equality)
                or any(k not in allowed[table] for k,desc in order)
                or (columns is not None and any(k not in allowed[table] for k in columns))):
            unproven()
        sql='SELECT '+('*' if columns is None else ','.join(columns))+' FROM '+table
        if equality:sql+=' WHERE '+' AND '.join(k+'=?' for k,v in equality)
        if order:sql+=' ORDER BY '+','.join(k+(' DESC' if desc else '') for k,desc in order)
        if limit is not None:
            if type(limit) is not int or limit!=1:unproven()
            sql+=' LIMIT 1'
        params=tuple(v for k,v in equality)
        if cold_decode is not None:
            return _lc_cold1136_p04_rows(db, sql, params, table=table[5:], first=first)
        cursor=db.execute(sql,params)
        return cursor.fetchone() if first else cursor.fetchall()
    old_factory=db.row_factory
    try:
        db.row_factory=sqlite3.Row
        if cold_decode is None:
            _p04_current_projection(read,state)
        else:
            _p04_current_projection_legacy(read,state,cold_decode=cold_decode)
    finally:
        db.row_factory=old_factory


def _p04_material_current(material,state):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _p04_material_current_v3, material, state)
    if '_p04' not in state:return
    scope=_c0_query_context.get()
    plan=scope[3] if scope is not None and material is scope[1] else None
    if type(plan) is _C0QueryPlan:
        return plan.p04_project(material,state)
    _p04_current_projection(_lifecycle_material_read(material),state)


def window_record(db, world_id, window_id):
    # Prefix and result remain on the active operation's same shared ledger.
    return _lifecycle_value_invoke(_window_record_owned, (db, world_id, window_id),
        inputs=(world_id, window_id))


def window_phase_proof(world, db, window_id):
    storage, identity = source(world)
    registration(db, latest(db)['world_id'], identity)
    rows = db.execute('SELECT * FROM coordinator_activations WHERE window_id=?', (window_id,)).fetchall()
    for row in rows:
        # Every persisted phase is checked before SQL may exclude a terminal;
        # unclosed rejected rows must be proven too, before returning them.
        if row['phase'] == 'skipped':
            unproven()  # No legitimate skipping rule.
        reference=ActivationRef(row['world_id'],row['window_id'],row['activation_id'],row['person_id'])
        acquisition_proof(world,db,latest(db),row['person_id'],reference)
    storage.paths()


def window_history(db, world_id, state):
    # A completed bit is not a boundary proof. Validate the ordered chain under
    # the same main/ATTACH transaction that may create its successor.
    rows = db.execute('SELECT window_id FROM coordinator_windows WHERE world_id=? ORDER BY ordinal', (world_id,)).fetchall()
    previous = None
    for ordinal, row in enumerate(rows, 1):
        window = window_record(db, world_id, row[0])
        base = json.loads(db.execute('SELECT state FROM snapshots WHERE seq=?', (window['base_revision'],)).fetchone()[0])
        if (window['ordinal'] != ordinal
                or (previous is not None and (previous['state'] != 'completed'
                    or window['base_revision'] < previous['completed_revision']))
                or base['time'] != (previous['target_time'] if previous else 0)):
            unproven()
        previous = window
    if state['time'] != (previous['target_time'] if previous else 0):
        unproven()
    return previous


@dataclass(frozen=True, repr=False)
class DispatchAckProof:
    reference: ActivationRef
    roster_index: int
    intent_id: str | None
    phase: Literal['pending', 'prepared', 'candidate', 'world_committed', 'completed', 'rejected', 'technical_blocked']
    private_closed: Literal[0, 1]
    terminal: bool


def prove_dispatch_ack(world: WorldStore, *, slot: _DispatchSlot, capture: DispatchCapture) -> DispatchAckProof:
    """Original root authorizes reads; a released child is historical data only."""
    from .actor_execution import _dispatch_read_scope
    slot.validate(capture)
    capture.check()
    reference = scoped(world, capture.reference)
    internal_uuid(capture.attempt_id)
    internal_uuid(capture.intent_id)
    world._validate_execution(capture._actor_identity)
    world._validate_coordinator_execution(capture._root_permit)

    def source_gate(db, state):
        storage, identity = source(world)
        # source() permits multiple live Hosts sharing the same legitimate file.
        original = capture._storage
        original.paths()
        if ((original.world_path, original.world_file, original.actor_path, original.actor_file)
                != capture._source_identity or storage.world_path != original.world_path
                or storage.world_file != original.world_file or storage.actor_path != original.actor_path
                or storage.actor_file != original.actor_file
                or identity != [str(original.actor_path), *original.actor_file]):
            unproven()
        registration(db, state['world_id'], identity)
        return storage

    with _dispatch_read_scope(world, slot, capture):
        with source_lock(world), world._connection(readonly=True) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            state = latest(db)
            if state['world_id'] != reference.world_id:
                unproven()
            world._check_coordinator_execution(db, state['world_id'], capture._root_permit, readonly=True)
            storage, identity = source(world)
            capture._storage.paths()
            db.execute('ATTACH DATABASE ? AS actor', (storage.actor_path.as_uri()+'?mode=ro',))
            source_gate(db, state)
            window_record(db, reference.world_id, reference.window_id)
            window_phase_proof(world, db, reference.window_id)
            row = activation_row(world, db, state, reference.person_id, reference)
            acquisition_proof(world, db, state, reference.person_id, reference)
            actor, root = capture._actor_identity, capture._root_permit
            delegated = db.execute('SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                (actor.world_id,actor.person_id,actor.epoch)).fetchone()
            parent = db.execute('SELECT parent_epoch,parent_owner_token FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?',
                (actor.world_id,actor.person_id,actor.epoch)).fetchone()
            if (row['roster_index'] != capture.roster_index or row['intent_id'] != capture.intent_id
                    or row['owner_parent_epoch'] != root.epoch or row['owner_actor_epoch'] != actor.epoch
                    or delegated is None or tuple(delegated) != (reference.window_id,reference.activation_id)
                    or parent is None or tuple(parent) != (root.epoch,root.owner_token)):
                unproven()
            proof = DispatchAckProof(reference,capture.roster_index,row['intent_id'],row['phase'],row['private_closed'],
                row['phase'] in ('completed','rejected') and row['private_closed'] == 1)
            source_gate(db, state)
            world._check_coordinator_execution(db, state['world_id'], capture._root_permit, readonly=True)
            capture.check()
        capture.check()
    capture.check()
    return proof


@contextmanager
def _history_ro_audit_scope(world, storage, db):
    """Sole trace ownership for next_activation's fresh WorldStore RO reader.

    Unlike the ledger scope, admission is bound to WorldStore._connection and
    the actual original next_activation frame. That source-owned path creates
    a fresh native connection, performs BEGIN/ATTACH/registration, and installs
    no callback before this point. Unknown lifecycles never touch their trace.
    Neither set_trace_callback's return nor PrivateStore's writer lifecycle is
    evidence about this reader. Arbitrary native instrumentation is not trusted.
    """
    import inspect
    import types
    import _sqlite3
    from .world import WorldStore, _AttemptConnection
    from .actor_execution import PrivateStore
    from .coordinator_execution import CoordinatorAttempt
    from .server import Coordinator
    from .coordinator_maintenance import recover
    from .source_attestation import attest
    method = getattr(WorldStore._connection, '__wrapped__', None)
    stamp = method.__globals__.get('_SOURCE_GENERATION') if isinstance(method, types.FunctionType) else None
    own = globals().get('_SOURCE_GENERATION')
    frame = inspect.currentframe()
    try:
        caller = frame.f_back.f_back
        original_call = (caller.f_code is next_activation.__code__
                         and caller.f_locals.get('world') is world
                         and caller.f_locals.get('storage') is storage
                         and caller.f_locals.get('db') is db)
    finally:
        del frame, caller
    bound = world._connection
    eligible = (original_call and _history_audit_batch.get() is None
                and type(world) is WorldStore and type(storage) is PrivateStore
                and storage.world is world
                and type(db) in (sqlite3.Connection, _AttemptConnection)
                and db.in_transaction
                and sqlite3.connect is _sqlite3.connect
                and isinstance(sqlite3.connect, types.BuiltinFunctionType)
                and getattr(bound, '__self__', None) is world
                and getattr(bound, '__func__', None) is WorldStore._connection
                and stamp is not None and stamp['matched']
                and any(obj is WorldStore for obj in stamp['objects'])
                and _history_original_method(bound, stamp)
                and own is not None and own['matched']
                and own['namespace'] is globals()
                and _history_original_method(next_activation, own))
    if not eligible:
        yield
        return
    host = storage.host
    root = world._scoped_coordinator_attempt()
    if (host.world is not world or host._authority is not world
            or host._storage is not storage
            or (root is not None and root.world is not world)):
        unproven()
    definitions = (world, storage, host, CoordinatorAttempt, recover, Coordinator)
    source_root = Path(__file__).resolve().parent.parent
    generation = attest(source_root, *definitions)
    batch = _HistoryAuditBatch(world, storage, db)
    db.set_trace_callback(batch.observe)
    token = _history_audit_batch.set(batch)
    try:
        yield
        # All ordinary full proofs and the original SQL final guard completed.
        # Recheck captured instance edges and source before leaving this same
        # transaction; cancellation/failure is never mapped to a cache miss.
        current = batch.fingerprint()
        if (len(current[0]) != len(batch.initial[0])
                or any(a is not b for a, b in zip(current[0], batch.initial[0]))
                or current[1] != batch.initial[1]
                or host.world is not world or host._authority is not world
                or host._storage is not storage
                or (root is not None and root.world is not world)):
            unproven()
        if attest(source_root, *definitions) != generation:
            unproven()
    finally:
        batch.invalidate()
        _history_audit_batch.reset(token)
        db.set_trace_callback(None)  # The source-proved initial callback is absent.


"""C0 material primitives: native immutable Rows, no SQL/Connection/Row proxy."""
from types import MappingProxyType

_C0_KEYS = {
 'main.snapshots':('seq',),'main.world_constants':('key',),'main.scheduler_genesis':('world_id',),
 'main.intents':('id',),'main.coordinator_scheduler_registration':('world_id',),
 'main.coordinator_windows':('world_id','window_id'),'main.coordinator_activations':('world_id','activation_id'),
 'main.actor_coordinator_delegations':('world_id','person_id','actor_epoch'),
 'main.actor_activation_delegations':('world_id','person_id','actor_epoch'),
 'main.coordinator_activation_outcomes':('world_id','activation_id'),
 'main.coordinator_activation_blocks':('world_id','activation_id'),'main.coordinator_fault_issuers':('world_id','activation_id'),
 'main.coordinator_technical_maintenance':('operation_id',),'main.coordinator_technical_window_releases':('operation_id',),
 'main.dm_proposals':('intent_id',),'main.dm_decisions':('intent_id','version'),'main.dm_timeline':('intent_id','kind','version'),
 'actor.actor_world_binding':('singleton',),'actor.actor_sessions':('person_id',),'actor.actor_intents':('intent_id',),
 'actor.actor_calls':('call_id',),'actor.actor_session_heads':('person_id',),'actor.actor_intent_execution':('intent_id',)}


class _C0QueryMeasuredTuple(tuple):
    """Fixture-only counter; never wraps a Row or changes native SQL."""
    def __new__(cls, rows, counts):
        obj = tuple.__new__(cls, rows)
        obj.counts = counts
        return obj

    def __iter__(self):
        self.counts['iterations'] += 1
        for row in tuple.__iter__(self):
            self.counts['yields'] += 1
            yield row


class _C0QueryOwnedMaterial:
    __slots__ = ('_material', '_alive', '_plans', 'preparation', '_source', '_tables', '_check')

    @property
    def material(self):
        return self._material

    @property
    def alive(self):
        return self._alive

    def __init__(self, source):
        if _c0_v3_material(source) is not None:
            unproven()
        # New backings never returned; caller's backing aliases cannot mutate us.
        self._source=source;self._tables=source['tables'];self._check=source['check']
        self._material = MappingProxyType(dict(source, tables=MappingProxyType(dict(source['tables']))))
        self._alive = True
        self._plans = []
        self.preparation = {'table_reference_copies':len(self.material['tables']),
                            'row_copies':0,'index_builds':0,'proof_calls':0}

    def matches(self, source):
        if not self.alive:return False
        if source is self.material:return True
        if (source is not self._source or source['tables'] is not self._tables
                or source['check'] is not self._check):return False
        return (len(source['tables'])==len(self.material['tables'])
                and all(source['tables'].get(name) is rows
                        for name,rows in self.material['tables'].items()))

    def plan(self):
        plan = _C0QueryPlan(self.material, owner=self)
        self._plans.append(plan)
        return plan

    def close(self):
        self._alive = False
        for plan in self._plans:
            plan.close()
        self._plans.clear()
        self._source=self._tables=self._check=None


class _C0QueryPlan:
    def __init__(self, material, *, owner=None):
        if _c0_v3_material(material) is not None:
            unproven()
        self.material = material
        self.tables = material['tables']
        self.check = material['check']
        self.owner = owner
        self.native = (type(owner) is _C0QueryOwnedMaterial and owner.alive
                       and owner.material is material)
        self.active = (type(material) is dict and type(self.tables) is dict) or self.native
        self.indexes = {}
        self.p04=self.p04_rows=self.p04_edges=self.p04_latest=None

    def close(self):
        self.active = False
        self.indexes.clear()
        self.p04=self.p04_rows=self.p04_edges=self.p04_latest=None
        self.material = self.tables = self.check = None
        self.owner = None

    def p04_bound(self, material):
        """Strong immutable input binding, not a material/authority admission gate.

        Unknown/controlled containers always use the unchanged original consumer.
        The private owner backing is created by _C0QueryOwnedMaterial, not inferred
        from a MappingProxyType appearance. No copied or impersonated native Row.
        """
        if (not self.active or not self.native or self.owner is None
                or not self.owner.alive or material is not self.material
                or self.owner.material is not material
                or material['tables'] is not self.tables
                or material['check'] is not self.check):
            self.p04 = None
            return False
        names=('main.snapshots','main.coordinator_windows','main.coordinator_activations',
               'main.intents','main.dm_decisions','main.dm_proposals','main.dm_timeline',
               'main.coordinator_activation_outcomes','main.coordinator_scheduler_registration',
               'actor.actor_intents','actor.actor_intent_execution','actor.actor_calls',
               'actor.actor_sessions','actor.actor_session_heads','actor.actor_world_binding')
        held=self.p04_rows
        if held is False:return False
        if held is not None:
            if any(self.tables.get(name,()) is not rows for name,rows in held):
                self.close()
                return False
            return True
        held=[]
        for name in names:
            rows=self.tables.get(name,())
            if (type(rows) is not tuple or any(type(row) is not sqlite3.Row
                    or any(type(value) not in (str,int,float,bytes,type(None)) for value in row)
                    for row in rows)):
                self.p04_rows=False
                return False
            held.append((name,rows))
        self.p04_rows=tuple(held)
        return True

    def p04_project(self, material, state):
        from copy import deepcopy
        try:
            material['check']()
            if not self.p04_bound(material):
                _p04_current_projection(_lifecycle_material_read(material),state)
                material['check']()
                return
            derived=self.p04
            if derived is not None and state==derived[0]:
                # Same exact immutable snapshots/windows/receipt dependencies.
                # Replay EVERY original read's pre/post check; no cancellation,
                # root/actor/Entry, source, authorization or budget success memo.
                for _ in range(derived[1]):
                    material['check']()
                    material['check']()
            else:
                # Record only the original projection's real dependency reads.
                # No partial/failed projection is published. A changed target
                # is checked by the original consumer, never by stale success.
                # All events have the same two dynamic endpoints. Keeping
                # their exact cardinality preserves order and multiplicity;
                # original table tuples/Rows remain strongly held by p04_bound.
                read_pairs=0
                def read(*args,**kwargs):
                    nonlocal read_pairs
                    result=_lifecycle_material_read(material)(*args,**kwargs)
                    read_pairs+=1
                    return result
                _p04_current_projection(read,state)
                material['check']()
                self.p04=(deepcopy(state),read_pairs)
            material['check']()
        except BaseException:
            self.close()
            raise

    def snapshot_edge(self, material, descending):
        # _c0_latest's real pre/post gates stay outside this pure selection.
        if not self.p04_bound(material):return None
        entry=self.indexes.get('main.snapshots')
        if entry is None:
            rows=self.tables['main.snapshots'];groups={}
            for row in rows:
                value=row['seq']
                if type(value) is not int:return None
                groups.setdefault(value,[]).append(row)
            entry=(rows,{key:tuple(group) for key,group in groups.items()})
            self.indexes['main.snapshots']=entry
        groups=entry[1]
        if not groups:return None
        if self.p04_edges is None:
            self.p04_edges=(groups[min(groups)][0],groups[max(groups)][0])
        # Keep equal-seq first-row semantics; do not repair duplicates.
        return self.p04_edges[1 if descending else 0]

    def select(self, material, table, equality):
        # Called ONLY after the dynamic original precheck, never as proof memo.
        if not self.active:
            return None
        if self.native and (not self.owner.alive or self.owner.material is not material):
            self.close()
            return None
        if (material is not self.material or material['tables'] is not self.tables
                or material.get('check') is not self.check):
            self.close()
            return None
        if type(table) is not str:
            return None
        if table == 'main.coordinator_activations':
            field = 'intent_id'
        elif table == 'main.intents':
            field = 'id'
        elif table == 'main.snapshots':
            field = 'seq'
        else:
            return None
        if not (type(equality) is tuple and len(equality) == 1
                and type(equality[0]) is tuple and len(equality[0]) == 2
                and type(equality[0][0]) is str and equality[0][0] == field
                and type(equality[0][1]) is (int if field == 'seq' else str)):
            return None
        value = equality[0][1]
        rows = material['tables'][table]
        entry = self.indexes.get(table)
        if entry is not None:
            held_rows, groups = entry
            if rows is not held_rows:
                self.close()
                return None
            return list(groups.get(value, ()))
        # Tuple subclass is permitted for fixture-only iteration measurement.
        # Exact builtin tuple + genuine Row is exercised separately, unmeasured.
        if not isinstance(rows, tuple):
            return None
        if self.native and type(rows) not in (tuple, _C0QueryMeasuredTuple):
            return None
        selected, groups, safe = [], {}, True
        for row in rows:
            # Original comparison/None short circuit precedes key validation.
            if all(row[k] == v and row[k] is not None and v is not None for k,v in equality):
                selected.append(row)
            if type(row) is not (sqlite3.Row if self.native else dict):
                safe = False
                continue
            key_value = row[field]
            if type(key_value) is (int if field == 'seq' else str):
                groups.setdefault(key_value, []).append(row)
            elif field == 'seq':
                # Numeric equality may match floats/bools; keep the original
                # scan result but never publish an incomplete integer index.
                safe = False
            elif key_value is not None and type(key_value) not in (bool, int, float):
                safe = False
        if safe:
            self.indexes[table] = (rows, {key:tuple(group) for key,group in groups.items()})
        return selected


_c0_query_context = ContextVar('c0_query_context', default=None)


@contextmanager
def _c0_query_scope(material):
    if _c0_v3_material(material) is not None:
        yield material
        return
    """Own one lazy selection plan for this proof's complete collector material.

    No authority/proof/check success is stored. Same-material inner proofs share
    the plan; different-material nesting restores the outer context. Owner and
    its private material backings have no product registry or cross-Entry root.
    Closing clears plan references; the owner's material lives until scope exit,
    not a constant-space promise for an externally retained closed owner.
    """
    current = _c0_query_context.get()
    if current is not None and current[2].matches(material):
        try:
            yield current[1]
        except BaseException:
            current[3].close()
            raise
        return
    owner = _C0QueryOwnedMaterial(material)
    try:
        plan = owner.plan()
        token = _c0_query_context.set((material, owner.material, owner, plan))
        try:
            yield owner.material
        finally:
            _c0_query_context.reset(token)
    finally:
        owner.close()


def _c0_read(material, table, equality=(), columns=None, order=(), limit=None, first=False):
    if _c0_v3_material(material) is not None:
        return _c0_v3_adopt(material, _c0_borrow(material, table, equality, columns, order, limit, first))
    """Fixed generated relational inputs, NOT a SQL interpreter or adapter.

    Full rows are the original immutable sqlite3.Row from native SELECT *. A
    projection is an ordinary built-in tuple; there is no Row impersonation.
    """
    material['check']()
    query_scope = _c0_query_context.get()
    plan = query_scope[3] if query_scope is not None and material is query_scope[1] else None
    rows = plan.select(material, table, equality) if type(plan) is _C0QueryPlan else None
    if rows is None:
        rows=[r for r in material['tables'][table] if all(r[k]==v and r[k] is not None and v is not None for k,v in equality)]
    for key,reverse in reversed(order):rows.sort(key=lambda row:row[key],reverse=reverse)
    if limit is not None:rows=rows[:limit]
    if columns is not None:rows=[tuple(r[k] for k in columns) for r in rows]
    material['check']()
    return (rows[0] if rows else None) if first else rows


def _c0_exists(material, table, schema='main'):
    return schema+'.'+table in material['tables']


def _c0_latest(material):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_latest_v3, material)
    scope=_c0_query_context.get()
    plan=scope[3] if scope is not None and material is scope[1] else None
    if type(plan) is _C0QueryPlan:
        try:
            material['check']()
            edge=plan.snapshot_edge(material,True)
            if edge is not None:
                if plan.p04_latest is None or plan.p04_latest[0] is not edge:
                    plan.p04_latest=(edge,json.loads(edge['state']))
                from copy import deepcopy
                result=deepcopy(plan.p04_latest[1])
                material['check']()
                return result
        except BaseException:
            plan.close()
            raise
    row=_c0_read(material,'main.snapshots',columns=('state',),order=(('seq',True),),limit=1,first=True)
    if row is None:unproven()
    return json.loads(row[0])


def _c0_fault_schema(material, *, create=False, authorize=False):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_fault_schema_v3, material, create=create, authorize=authorize)
    # Read-only consumer never installs/repairs schema or mint old evidence.
    if create:unproven()
    if not _c0_exists(material,_ISSUER_TABLE):return False
    def sql(name):
        r=_c0_read(material,'main.sqlite_master',(('name',name),),('sql',),first=True)
        return r[0] if r else None
    if sql(_ISSUER_TABLE)!=_ISSUER_DDL:unproven()
    for event in ('UPDATE','DELETE'):
        expected='CREATE TRIGGER fault_issuer_no_'+event.lower()+' BEFORE '+event+' ON '+_ISSUER_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"
        if sql('fault_issuer_no_'+event.lower())!=expected:unproven()
    found=sql('fault_issuer_no_replace')
    if (found is None and authorize) or (found is not None and found!=_ISSUER_INSERT):unproven()
    return True


def _c0_m1_schema(material):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_m1_schema_v3, material)
    from .coordinator_maintenance import _TABLE,_DDL,_INSERT,fail
    if not _c0_exists(material,_TABLE):return False
    def sql(name):
        r=_c0_read(material,'main.sqlite_master',(('name',name),),('sql',),first=True)
        return r[0] if r else None
    if sql(_TABLE)!=_DDL:fail()
    for event in ('UPDATE','DELETE'):
        expected='CREATE TRIGGER technical_maintenance_no_'+event.lower()+' BEFORE '+event+' ON '+_TABLE+" BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"
        if sql('technical_maintenance_no_'+event.lower())!=expected:fail()
    if sql('technical_maintenance_no_replace')!=_INSERT:fail()
    return True


def _c0_native_audit(storage, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_native_audit_v3, storage, material, row)
    from .actor_execution import ActorRunError
    current=_c0_latest(material)
    _p04_material_current(material,current)
    person=row['person_id']
    if _c0_exists(material,'actor_session_heads','actor') and _c0_read(material,'actor.actor_session_heads',(('person_id',person),),first=True):
        storage._material_audit(material,person)
        return
    untouched=row['phase']=='pending' or (row['phase']=='technical_blocked' and row['resume_phase']=='pending')
    if not untouched or any(_c0_exists(material,table,'actor') and _c0_read(material,'actor.'+table,(('person_id',person),),first=True)
                         for table in ('actor_intents','actor_calls','actor_intent_execution')):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
    session=_c0_read(material,'actor.actor_sessions',(('person_id',person),),('messages','seen'),first=True)
    if session is not None and any(json.loads(raw)!=[] for raw in session):raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')


_c0_structure_scope = ContextVar('c0_structure_scope', default=None)


def _c0_fullproof(world, storage, material, window_id):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_fullproof_v3, world, storage, material, window_id)
    """Complete original window + per-person/per-meta proof, with no SQL/lock."""
    with _c0_query_scope(material) as material:
        scope = dict(material=material, tables=material['tables'], check=material['check'], cache={})
        token = _c0_structure_scope.set(scope)
        try:
            material['check']()
            current=_c0_latest(material)
            _c0_window_record(material,current['world_id'],window_id)
            for row in _c0_read(material,'main.coordinator_activations',(('window_id',window_id),)):
                material['check']()
                if row['phase']=='skipped':unproven()
                ref=scoped(world,ActivationRef(row['world_id'],row['window_id'],row['activation_id'],row['person_id']))
                if ref.world_id!=current['world_id']:raise WorldError('ACTIVATION_BINDING_CONFLICT','激活人物或世界不符')
                _c0_window_record(material,ref.world_id,ref.window_id)
                _c0_phase_proof(world,material,row)
                _c0_native_audit(storage,material,row)
            material['check']()
            return True
        finally:
            scope['cache'].clear()
            _c0_structure_scope.reset(token)


def _c0_window_record(material, world_id, window_id):
    if _c0_v3_material(material) is not None:
        return _c0_window_record_uncached(material, world_id, window_id)
    """Memo only a successful immutable window structure within one fullproof."""
    from copy import deepcopy
    scope = _c0_structure_scope.get()
    try:
        if (scope is None or scope['material'] is not material
                or scope['tables'] is not material['tables']
                or scope['check'] is not material['check']
                or type(world_id) is not str or not world_id
                or type(window_id) is not str):
            return _c0_window_record_uncached(material, world_id, window_id)
        internal_uuid(window_id)
        material['check']()
        key = (world_id, window_id)
        hit649 = key in scope['cache']
        if hit649:
            # Do not memoize the newly added current/receipt read gates.
            _p04_material_current(material,_c0_latest(material))
            saved, checkpoints = scope['cache'][key]
            # Replay each successful original _c0_read pre/post dynamic gate;
            # no authority, cancellation or budget success is cached.
            for checkpoint in checkpoints:
                material['check']()
                material['check']()
            result = deepcopy(saved)
        else:
            result = _c0_window_record_uncached(material, world_id, window_id)
            material['check']()
            checkpoints = ('window', 'advanced', 'base', 'latest', 'genesis', 'activations')
            if result['state'] == 'completed':
                checkpoints += ('completed',)
            scope['cache'][key] = (deepcopy(result), checkpoints)
            result = deepcopy(result)
        material['check']()
        return result
    except BaseException:
        if scope is not None:
            scope['cache'].clear()
        raise


def _c0_window_record_uncached(material, world_id, window_id):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_window_record_uncached_v3, material, world_id, window_id)
    from .world import WorldStore, _lc_history_scope
    internal_uuid(window_id)
    from copy import deepcopy
    row = _c0_read(material, 'main.coordinator_windows', (('world_id', (world_id, window_id)[0]), ('window_id', (world_id, window_id)[1])), None, (), None, True)
    if row is None:
        unproven()
    result = dict(row)
    try:
        result['roster'] = json.loads(result['roster_json'])
        roster = result['roster']
        if type(roster) is not list or any((type(p) is not str or not p for p in roster)) or len(set(roster)) != len(roster) or (sha(roster) != result['roster_sha256']) or (result['state'] not in ('open', 'completed')) or (result['boundary_kind'] not in ('normal', 'horizon')):
            unproven()
        for key in ('ordinal', 'base_revision', 'advanced_revision', 'created_parent_epoch', 'owner_parent_epoch'):
            if type(result[key]) is not int or not 1 <= result[key] <= MAX_INT:
                unproven()
        for key in ('target_time', 'horizon_at_open'):
            if type(result[key]) is not int or not 0 <= result[key] <= MAX_INT:
                unproven()
        if result['advanced_revision'] != result['base_revision'] + 1 or result['owner_parent_epoch'] < result['created_parent_epoch'] or result['horizon_at_open'] < result['target_time']:
            unproven()
        if result['completed_revision'] is not None:
            integer(result['completed_revision'], 1)
        advanced = _c0_read(material, 'main.snapshots', (('seq', (result['advanced_revision'],)[0]),), ('state',), (), None, True)
        base = _c0_read(material, 'main.snapshots', (('seq', (result['base_revision'],)[0]),), ('state',), (), None, True)
        if advanced is None or base is None:
            unproven()
        advanced = json.loads(advanced[0])
        base = json.loads(base[0])
        current = _c0_latest(material)
        if advanced['world_id'] != world_id or base['world_id'] != world_id or current['world_id'] != world_id or (advanced['revision'] != result['advanced_revision']) or (advanced['time'] != result['target_time']) or (advanced['horizon'] != result['horizon_at_open']) or (result['target_time'] != WorldStore._p04_target(base)):
            unproven()
        genesis = json.loads(_c0_read(material, 'main.snapshots', (), ('state',), (('seq', False),), 1, True)[0])

        def identities(snapshot):
            return [(p['id'], p['country_id'], p['office']) for p in snapshot['persons']]
        from .world import _lc_optin
        lifecycle_read = _lifecycle_material_read(material) if _lc_optin(genesis) else None
        with _lc_history_scope(lifecycle_read,genesis):
            if lifecycle_read is None:
                if identities(current) != identities(genesis) or identities(advanced) != identities(genesis):
                    unproven()
            else:
                for historical in (base,advanced,current):
                    _lifecycle_snapshot_proof(lifecycle_read,historical,genesis)
            horizon = result['boundary_kind'] == 'horizon'
            WorldStore._check_frozen_p04(advanced,genesis)
            WorldStore._check_frozen_p04(current,genesis)
            _p04_material_current(material,current)
            expected = WorldStore._p04_expected(base,advanced,genesis=genesis)
            if '_p04' in advanced and advanced['_p04']['dispatch']!=expected:
                unproven()
            if roster != expected or horizon != (advanced['time'] == advanced['horizon']) or (horizon and (advanced['status'] != 'horizon_paused' or result['state'] != 'completed')) or (not set(roster) <= {p['id'] for p in current['persons']}):
                unproven()
            activations = _c0_read(material, 'main.coordinator_activations', (('world_id', (world_id, window_id)[0]), ('window_id', (world_id, window_id)[1])), None, (('roster_index', False),), None, False)
            if len(activations) != len(roster):
                unproven()
            for index, activation in enumerate(activations):
                internal_uuid(activation['activation_id'])
                integer(activation['roster_index'])
                integer(activation['private_closed'])
                for key in ('created_parent_epoch', 'owner_parent_epoch'):
                    integer(activation[key], 1)
                for key in ('observation_revision', 'card_version', 'base_session_version', 'promoted_version', 'owner_actor_epoch'):
                    if activation[key] is not None:
                        integer(activation[key], 0 if key == 'base_session_version' else 1)
                if activation['world_id'] != world_id or activation['window_id'] != window_id or activation['created_parent_epoch'] != result['created_parent_epoch'] or (activation['owner_parent_epoch'] < activation['created_parent_epoch']) or (activation['observation_revision'] is not None and (not result['advanced_revision'] <= activation['observation_revision'] <= current['revision'])) or (activation['promoted_version'] is not None and activation['base_session_version'] is not None and (activation['promoted_version'] != activation['base_session_version'] + 1)):
                    unproven()
                if lifecycle_read is not None and activation['observation_revision'] is not None:
                    _lifecycle_activation_identity(lifecycle_read,genesis,activation)
                if activation['intent_id'] is not None:
                    internal_uuid(activation['intent_id'])
                if activation['roster_index'] != index or activation['person_id'] != roster[index] or activation['phase'] not in ('pending', 'prepared', 'candidate', 'world_committed', 'completed', 'technical_blocked', 'rejected', 'skipped') or (activation['private_closed'] not in (0, 1)) or (activation['phase'] == 'pending' and (activation['intent_id'] is not None or activation['private_closed'] != 0)):
                    unproven()
            if result['state'] == 'completed':
                completion = result['completed_revision']
                if type(completion) is not int or not result['advanced_revision'] <= completion <= current['revision'] or any((a['observation_revision'] is not None and (a['observation_revision'] > completion or (a['observation_revision'] == completion and a['phase'] != 'rejected')) for a in activations)) or any((a['phase'] not in ('completed', 'rejected', 'skipped') or a['private_closed'] != 1 for a in activations)):
                    unproven()
                completed = _c0_read(material, 'main.snapshots', (('seq', (completion,)[0]),), ('state',), (), None, True)
                if completed is None:
                    unproven()
                completed = json.loads(completed[0])
                if lifecycle_read is not None:
                    _lifecycle_snapshot_proof(lifecycle_read,completed,genesis)
                if completed['world_id'] != world_id or completed['revision'] != completion or completed['time'] != result['target_time']:
                    unproven()
            elif result['completed_revision'] is not None:
                unproven()
    except (KeyError, ValueError, TypeError, IndexError):
        unproven()
    return deepcopy(result)


def _c0_phase_proof(world, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_phase_proof_v3, world, material, row)
    try:
        return _c0__phase_proof(world, material, row)
    except (ValueError, TypeError, KeyError, IndexError):
        unproven()


def _c0__phase_proof(world, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0__phase_proof_v3, world, material, row)
    if row['phase'] == 'technical_blocked':
        _c0_block_proof(world, material, row)
        return row
    if row['phase'] == 'completed' and _c0_exists(material, 'coordinator_activation_blocks') and _c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True):
        from .coordinator_maintenance import completed_proof
        _c0_completed_proof(world, material, row)
        return row
    return _c0_material_proof(world, material, row, row['phase'])


def _c0_material_proof(world, material, row, phase):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_material_proof_v3, world, material, row, phase)
    """Non-recursive original material proof; never grants execution authority."""
    from .actor_execution import digest
    person = row['person_id']
    if phase not in ('pending', 'prepared', 'candidate', 'world_committed', 'completed', 'rejected'):
        unproven()
    if phase != 'rejected':
        fields = ('terminal_reason',) if row['phase'] == 'technical_blocked' else ('terminal_reason', 'resume_phase', 'diagnostic_code')
        if any((row[k] is not None for k in fields)):
            unproven()
        saved = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), ('kind',), (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
        if phase not in ('world_committed', 'completed') and (saved is not None or row['receipt_sha256'] is not None):
            unproven()
    if phase == 'rejected':
        _c0_rejection_proof(world, material, row)
        return row
    if phase == 'pending':
        if any((row[k] is not None for k in ('intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'promoted_version'))):
            unproven()
        return row
    private = _c0_read(material, 'actor.actor_intents', (('intent_id', (row['intent_id'], person)[0]), ('person_id', (row['intent_id'], person)[1])), None, (), None, True)
    meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
    if private is None or meta is None or digest(private['request']) != row['request_sha256'] or (meta['base_version'] != row['base_session_version']):
        unproven()
    request = json.loads(private['request'])
    if type(request) is not dict or request.get('person_id') != person or request.get('intent_id') != row['intent_id'] or (not body_matches(request, json.loads(private['body']), private['candidate'])) or (meta['person_id'] != person) or (meta['world_id'] != row['world_id']):
        unproven()
    if phase == 'prepared':
        if private['state'] != 'prepared' or private['candidate'] is not None:
            unproven()
    elif private['candidate'] is None or digest(private['candidate']) != row['candidate_sha256'] or private['state'] != ('committed' if phase == 'completed' else 'candidate'):
        unproven()
    actual = _c0_read(material, 'main.intents', (('id', (row['intent_id'],)[0]),), ('person', 'receipt'), (), None, True)
    if phase in ('world_committed', 'completed'):
        outcome = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
        if actual is None or actual[0] != person or actual[1] is None or (digest(actual[1]) != row['receipt_sha256']) or (outcome is None) or (outcome['kind'] != 'committed') or (outcome['intent_id'] != row['intent_id']) or (digest(outcome['outcome_json']) != outcome['outcome_sha256']) or (json.loads(outcome['outcome_json']) != json.loads(actual[1])):
            unproven()
        receipt = json.loads(actual[1])
        if type(receipt) is not dict or receipt.get('status') != 'committed' or receipt.get('world_id') != row['world_id'] or (receipt.get('person_id') != person) or (receipt.get('intent_id') != row['intent_id']):
            unproven()
    elif actual is not None and actual[1] is not None:
        unproven()
    if phase == 'completed':
        if row['private_closed'] != 1 or private['state'] != 'committed' or private['receipt'] is None or (digest(private['receipt']) != row['receipt_sha256']) or (meta['promoted_version'] != row['promoted_version']) or (row['promoted_version'] != meta['base_version'] + 1):
            unproven()
    elif row['private_closed'] != 0 or meta['promoted_version'] is not None:
        unproven()
    return row


def _c0_block_payload(material, row, resume):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_block_payload_v3, material, row, resume)
    saved = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), ('outcome_sha256',), (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
    return dict(schema_version=1, **{k: row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'base_session_version')}, resume_phase=resume, outcome_sha256=saved[0] if saved else None, private_closed=0)


def _c0_block_proof(world, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_block_proof_v3, world, material, row)
    _c0_block_witness_proof(world, material, row)
    _c0_material_proof(world, material, row, row['resume_phase'])
    return row


def _c0_block_witness_proof(world, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_block_witness_proof_v3, world, material, row)
    from .actor_execution import canonical, digest
    if row['resume_phase'] not in ('pending', 'prepared', 'candidate', 'world_committed', 'rejected') or row['private_closed'] != 0 or row['promoted_version'] is not None or (type(row['diagnostic_code']) is not str) or (re.fullmatch('[A-Z][A-Z0-9_]{0,79}', row['diagnostic_code']) is None) or (not _c0_exists(material, 'coordinator_activation_blocks')):
        unproven()
    if not _c0_exists(material, 'actor_activation_delegations') or not _c0_exists(material, 'actor_coordinator_delegations'):
        unproven()
    binding = _c0_read(material, 'main.actor_activation_delegations', (('world_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[2])), ('window_id', 'activation_id'), (), None, True)
    parent = _c0_read(material, 'main.actor_coordinator_delegations', (('world_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[2])), ('parent_epoch',), (), None, True)
    if binding is None or tuple(binding) != (row['window_id'], row['activation_id']) or parent is None or (parent[0] != row['owner_parent_epoch']):
        unproven()
    if row['resume_phase'] != 'pending':
        integer(row['observation_revision'], 1)
        integer(row['card_version'], 1)
        integer(row['base_session_version'])
    if row['resume_phase'] == 'pending' and any((row[k] is not None for k in ('intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'promoted_version', 'observation_revision', 'card_version', 'base_session_version'))):
        unproven()
    if row['resume_phase'] == 'prepared' and any((row[k] is not None for k in ('candidate_sha256', 'receipt_sha256', 'promoted_version'))):
        unproven()
    saved = _c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True)
    if saved is None or saved['resume_phase'] != row['resume_phase'] or saved['diagnostic_code'] != row['diagnostic_code'] or (type(saved['pause_revision']) is not int) or (saved['pause_revision'] < 1) or (saved['proof_json'] != canonical(_c0_block_payload(material, row, row['resume_phase']))) or (saved['proof_sha256'] != digest(saved['proof_json'])):
        unproven()
    pause = _c0_read(material, 'main.snapshots', (('seq', (saved['pause_revision'],)[0]),), ('state',), (), None, True)
    if pause is None:
        unproven()
    pause = json.loads(pause[0])
    window = _c0_window_record(material, row['world_id'], row['window_id'])
    if pause['world_id'] != row['world_id'] or pause['revision'] != saved['pause_revision'] or pause['time'] != window['target_time'] or (pause['status'] != 'technical_paused') or (pause.get('diagnostic') != {'code': row['diagnostic_code'], 'time': window['target_time']}) or (saved['pause_revision'] < (row['observation_revision'] or window['advanced_revision'])):
        unproven()
    _c0_fault_issuer_proof(world, material, row, required=False)
    return row


def _c0_fault_issuer_proof(world, material, row, *, required=True):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_fault_issuer_proof_v3, world, material, row, required=required)
    """Original pause-time authority, never minted from maintenance-time metadata."""
    from .actor_execution import canonical, digest
    found = _c0_read(material, 'main.coordinator_fault_issuers', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True) if _c0_fault_schema(material, authorize=required) else None
    if found is None:
        if required:
            unproven()
        return None
    witness = decode_fault_issuer(found['witness_json'])
    if found['world_id'] != row['world_id'] or found['activation_id'] != row['activation_id'] or found['witness_sha256'] != digest(found['witness_json']):
        unproven()
    block = _c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True)
    fault = witness['binding']
    integer(fault['owner_actor_epoch'], 1)
    integer(fault['owner_parent_epoch'], 1)
    parent = _c0_read(material, 'main.actor_coordinator_delegations', (('world_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[2])), ('parent_epoch', 'parent_owner_token'), (), None, True)
    delegation = _c0_read(material, 'main.actor_activation_delegations', (('world_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[2])), ('window_id', 'activation_id'), (), None, True)
    meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
    if block is None or parent is None or meta is None or (delegation is None) or any((fault[k] != row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id'))) or (tuple(delegation) != (row['window_id'], row['activation_id'])) or (parent[0] != fault['owner_parent_epoch']) or (witness['pause_revision'] != block['pause_revision']) or (witness['block_sha256'] != block['proof_sha256']) or (witness['parent'] != list(parent)) or (witness['meta'] != {k: meta[k] for k in meta.keys() if k != 'promoted_version'}) or (meta['owner_epoch'] != fault['owner_actor_epoch']):
        unproven()
    attempt = world._scoped_attempt()
    if attempt is not None and attempt.permit.epoch == fault['owner_actor_epoch']:
        if attempt.permit.world_id != row['world_id'] or attempt.permit.person_id != row['person_id'] or attempt.permit.owner_token != meta['owner_token'] or (attempt.id != meta['attempt_id']) or (attempt.activation != scoped(world)) or (attempt.coordinator is None) or ([attempt.coordinator.epoch, attempt.coordinator.owner_token] != witness['parent']):
            unproven()
        fault_issuer_scope(world, row, attempt.permit)
    return witness


def _c0_rejection_proof(world, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_rejection_proof_v3, world, material, row)
    """Row-level proof only; callers separately run the complete native audit.

    Keeping this non-recursive lets the audit prove historical rejected leaves.
    """
    from .actor_execution import canonical, digest
    blocked = row['phase'] == 'technical_blocked' and row['resume_phase'] == 'rejected' and (row['private_closed'] == 0)
    nulls = ('receipt_sha256', 'promoted_version') if blocked else ('receipt_sha256', 'promoted_version', 'resume_phase', 'diagnostic_code')
    if row['phase'] != 'rejected' and (not blocked) or row['private_closed'] not in (0, 1) or any((row[k] is not None for k in nulls)):
        unproven()
    private = _c0_read(material, 'actor.actor_intents', (('intent_id', (row['intent_id'], row['person_id'])[0]), ('person_id', (row['intent_id'], row['person_id'])[1])), None, (), None, True)
    meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
    actual = _c0_read(material, 'main.intents', (('id', (row['intent_id'],)[0]),), None, (), None, True)
    saved = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
    if private is None or meta is None or actual is None or (saved is None) or (private['state'] != ('rejected' if row['private_closed'] else 'candidate')) or (private['receipt'] is not None) or (actual['receipt'] is not None) or (actual['person'] != row['person_id']) or (meta['person_id'] != row['person_id']) or (meta['world_id'] != row['world_id']) or (meta['base_version'] != row['base_session_version']) or (meta['promoted_version'] is not None) or (private['candidate'] is None) or (digest(private['request']) != row['request_sha256']) or (digest(private['candidate']) != row['candidate_sha256']) or (saved['kind'] != 'domain_rejected') or (saved['intent_id'] != row['intent_id']) or (digest(saved['outcome_json']) != saved['outcome_sha256']):
        unproven()
    result = json.loads(saved['outcome_json'])
    metrics = json.loads(private['metrics'])
    if type(metrics) is not dict or set(metrics) != {'sdk_events', 'sdk_requests', 'provider', 'pid', 'module_root'} or type(metrics['sdk_requests']) is not int or (metrics['sdk_requests'] < 0) or (type(metrics['pid']) is not int) or (metrics['pid'] < 0) or any((type(metrics[k]) is not str or not metrics[k] for k in ('provider', 'module_root'))):
        unproven()
    events = metrics['sdk_events']
    if not (type(events) is int and events >= 0 or (type(events) is dict and all((type(k) is str and type(v) is int and (v >= 0) for k, v in events.items())))):
        unproven()
    fields = {'status', 'world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'code', 'origin', 'action', 'decision_version', 'effect_index', 'request_sha256', 'candidate_sha256', 'payload_sha256', 'decision_sha256', 'effects'}
    request = json.loads(private['request'])
    payload = json.loads(actual['payload'])
    body = json.loads(private['body'])
    if type(result) is not dict or set(result) != fields or saved['outcome_json'] != canonical(result) or (request.get('person_id') != row['person_id']) or (request.get('intent_id') != row['intent_id']) or (not body_matches(request, body, private['candidate'])) or (payload.get('body') != body) or any((result[k] != row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'request_sha256', 'candidate_sha256'))) or (result['status'] != 'domain_rejected') or (result['effects'] != []) or (result['payload_sha256'] != digest(actual['payload'])) or (result['code'] != row['terminal_reason']) or (result['action'] not in DOMAIN_ACTIONS.get(result['code'], set())) or (result['origin'] not in ('world.apply_action', 'adjudication.execute')):
        unproven()
    world._validate_body(body)
    if result['origin'] == 'world.apply_action':
        if set(payload) != {'body', 'ruling', 'reason'} or payload['ruling'] not in ('大失败', '失败', '成功', '大成功') or type(payload['reason']) is not str or (not payload['reason'].strip()) or (len(payload['reason']) > 8000) or (result['action'] != body['action']) or any((result[k] is not None for k in ('decision_version', 'effect_index', 'decision_sha256'))):
            unproven()
    else:
        if set(payload) != {'body'}:
            unproven()
        version = result['decision_version']
        index = result['effect_index']
        if type(version) is not int or version < 1 or type(index) is not int or (index < 0) or (body['action'] != 'propose'):
            unproven()
        proposal = _c0_read(material, 'main.dm_proposals', (('intent_id', (row['intent_id'],)[0]),), ('world_id', 'person_id', 'text'), (), None, True)
        decision = _c0_read(material, 'main.dm_decisions', (('intent_id', (row['intent_id'],)[0]),), ('version', 'data'), (('version', True),), 1, True)
        if proposal is None or tuple(proposal) != (row['world_id'], row['person_id'], body['text']) or decision is None or (decision[0] != version) or (digest(decision[1]) != result['decision_sha256']):
            unproven()
        data = json.loads(decision[1])
        if data['world_id'] != row['world_id'] or type(data['effects']) is not list or (not 0 <= index < len(data['effects'])) or (data['effects'][index]['action'] != result['action']):
            unproven()
        from .adjudication import Adjudicator
        from types import SimpleNamespace
        if any(body.get('action') in _LIFECYCLE_ACTIONS for body in data['effects']):
            read=_lifecycle_material_read(material)
            compiled = _lifecycle_compile(world,data,read)
            _lifecycle_rejection(read,world,row,result,data)
        else:
            compiled = Adjudicator._compile(SimpleNamespace(world=world), data)
        for i, effect in enumerate(compiled):
            if _c0_read(material, 'main.intents', (('id', (Adjudicator._child_id(row['intent_id'], version, i),)[0]),), None, (), None, True):
                unproven()
        if _c0_read(material, 'main.dm_timeline', (('intent_id', (row['intent_id'],)[0]), ('kind', 'commit')), None, (), None, True):
            unproven()
    return result


def _c0_completed_proof(world, material, row):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_completed_proof_v3, world, material, row)
    from .actor_execution import digest
    from .coordinator_maintenance import fail
    'Re-prove original witness/issuer without pretending private is unpromoted.'
    if not _c0_m1_schema(material):
        fail()
    saved = _c0_read(material, 'main.coordinator_technical_maintenance', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True)
    if saved is None or saved['schema_version'] != 1 or digest(saved['record_json']) != saved['record_sha256']:
        fail()
    record = json.loads(saved['record_json'])
    old = record['original_row']
    if old['phase'] != 'technical_blocked' or old['resume_phase'] != 'world_committed':
        fail()
    changed = {'phase', 'private_closed', 'promoted_version', 'resume_phase', 'diagnostic_code'}
    if set(old) != set(row.keys()) or any((old[k] != row[k] for k in old if k not in changed)):
        fail()
    _c0_block_witness_proof(world, material, old)
    if _c0_fault_issuer_proof(world, material, old) != record.get('original_fault_issuer'):
        fail()
    block = dict(_c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True))
    if block != record['original_block'] or record['operation_id'] != saved['operation_id']:
        fail()
    _c0_material_proof(world, material, row, 'completed')
    return record


def _c0_source_attest(root,definitions,check,*,expected=None):
    from .source_attestation import attest,SourceMismatch
    owner = _lifecycle_source_owner()
    if expected is None:
        owner.reserve(4096, 'source9-retained-generation')
    scratch = owner.memory.hold(196608, 'source9-business-fixedscratch')
    try:
        try:
            result = attest(root,*definitions,check=check)
        except SourceMismatch:
            unproven()
        if expected is not None:
            matches = result == expected
            result = None
        else:
            matches = result
    except BaseException as error:
        owner.fail(error)
        raise
    scratch.release()
    return matches


class _C0Selection:
    """One-call material/authority binding. Never returned or persisted."""
    def __init__(self,world,coordinator,budget,*,_host=None):
        from .coordinator_execution import CoordinatorAttempt
        from .coordinator_maintenance import recover
        from .server import Coordinator
        from .source_attestation import attest
        from .adjudication import Adjudicator
        import types
        self.world=world;self.coordinator=coordinator;self.budget=budget
        self.root=world._scoped_coordinator_attempt();self.actor=world._scoped_attempt()
        self.activation=_activation_scope.get()
        self.storage,self.identity=source(world);self.host=self.storage.host
        if _host is not None:
            from .actors import ActorHost
            from .actor_execution import PrivateStore
            storage=_host._storage
            if (type(_host) is not ActorHost or type(storage) is not PrivateStore
                    or storage not in world._private_sources or storage.host is not _host or storage.world is not world
                    or _host.world is not world or _host._authority is not world
                    or [str(storage.actor_path),*storage.actor_file]!=self.identity):unproven()
            storage.paths();self.storage=storage;self.host=_host
        self.root_permit=self.root.permit if self.root is not None else None
        self.actor_permit=self.actor.permit if self.actor is not None else None
        self.host_world_id=self.host._bound_world_id
        self.raw_paths=(str(world.db_path),str(self.host.db_path))
        self.captured_bounds=tuple((getattr(x,'deadline',None),getattr(x,'busy_deadline',None)) for x in (self.root,self.actor,budget))
        self.files=(self.storage.world_path,self.storage.world_file,self.storage.actor_path,self.storage.actor_file)
        self.sources=tuple(world._private_sources)
        self.source_values=self._source_values()
        self.bounds=(budget.deadline,budget.busy_deadline)
        self.definitions=(world,self.storage,self.host,CoordinatorAttempt,recover,Coordinator,ActivationRef)
        self.source_root=Path(__file__).resolve().parent.parent
        self.source_generation=_c0_source_attest(self.source_root,self.definitions,budget.check)
        # Adjudication is not one of the nine. Bind its actual ordinary-import
        # namespace, class and compiled definitions independently, never claim
        # this extra closure came from FILES or the nine-source registry.
        self.dm_class=Adjudicator
        self.dm_functions=(Adjudicator._compile,Adjudicator._child_id)
        self.dm_namespace=Adjudicator._compile.__globals__
        self.dm_path=Path(self.dm_namespace['__file__']).resolve()
        if self.dm_path!=self.source_root/'app/adjudication.py':unproven()
        self.dm_raw = _lifecycle_source_capture(self.dm_path, budget.check,
            lambda raw: _lifecycle_source_dm_bind(raw, self.dm_path, self.dm_functions))
        self.registered=_LifecycleRegisteredBindings(world,self.host,self.storage)
        self.ordinary=_LifecycleOrdinarySources(self.host,self.source_root,self.check)
        self.vector=None;self.consumed=False
        self.check_source()

    def _source_values(self):
        return tuple(sorted((id(s),id(s.host),id(s.world),id(s.host._storage),id(s.host.world),id(s.host._authority),
            str(s.actor_path),s.actor_file,s.host._closed,s.host._closing.is_set()) for s in self.world._private_sources))

    def check(self):
        if self.root is not None:self.root.check()
        self.budget.check()
        if self.actor is not None:self.actor.check()
        if self.host._closed or self.host._closing.is_set():
            from .actor_execution import ActorRunError
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        if (self.world._scoped_coordinator_attempt() is not self.root or self.world._scoped_attempt() is not self.actor
                or self.world._scoped_entry_budget() is not self.budget or _activation_scope.get() is not self.activation
                or self.world._scoped_coordinator() is not self.coordinator
                or self.host.world is not self.world or self.host._authority is not self.world
                or self.host._storage is not self.storage or self.storage.host is not self.host or self.storage.world is not self.world
                or (self.root is not None and (self.root.world is not self.world or self.root.permit is not self.root_permit))
                or (self.actor is not None and self.actor.permit is not self.actor_permit)
                or self.host._bound_world_id!=self.host_world_id
                or tuple((getattr(x,'deadline',None),getattr(x,'busy_deadline',None)) for x in (self.root,self.actor,self.budget))!=self.captured_bounds
                or (self.budget.deadline,self.budget.busy_deadline)!=self.bounds
                or (self.storage.world_path,self.storage.world_file,self.storage.actor_path,self.storage.actor_file)!=self.files):
            unproven()
        if (str(self.world.db_path),str(self.host.db_path))!=self.raw_paths:
            # Preserve original path diagnostic only after cancellation and
            # captured authority/object checks; lexical aliases remain unproven.
            self.storage.paths()
            unproven()

    def check_source(self):
        from .source_attestation import attest
        self.check();self.storage.paths()
        actual,identity=source(self.world)
        if (identity!=self.identity or actual.actor_path!=self.storage.actor_path
                or self._source_values()!=self.source_values):unproven()
        if not _c0_source_attest(self.source_root,self.definitions,self.budget.check,expected=self.source_generation):unproven()
        if (self.dm_namespace.get('Adjudicator') is not self.dm_class
                or self.dm_class._compile is not self.dm_functions[0]
                or self.dm_class._child_id is not self.dm_functions[1]
                or not _lifecycle_source_matches(self.dm_path,self.dm_raw,self.budget.check)):unproven()
        self.registered.validate()
        self.ordinary.validate()
        self.check()

    def pin(self,db):
        from .world import _history_witness
        # Both actual read snapshots are pinned and coexist before any page.
        vector=(_history_witness(db,'main'),_history_witness(db,'actor'))
        if self.vector is None:self.vector=vector
        elif vector!=self.vector:unproven()
        return vector


def _c0_collect_pin(capsule, db):
    """Fresh catalog per page pin; preserve logical guards, never cache."""
    from .world import (_history_epoch, _HISTORY_MAIN, _HISTORY_PRIVATE,
                        _history_failure, _history_trigger, _history_schema_check)
    parts=[]
    for schema,domain in (('main',_HISTORY_MAIN),('actor',_HISTORY_PRIVATE)):
        epoch=_history_epoch(db,schema)
        coverage=db.execute(f'SELECT table_name,table_sql FROM {schema}.history_proof_coverage ORDER BY table_name').fetchall()
        cover={r[0]:r[1] for r in coverage}
        catalog=tuple(tuple(r) for r in db.execute(f'SELECT type,name,tbl_name,sql FROM {schema}.sqlite_master ORDER BY type,name'))
        objects={(r[0],r[1]):r for r in catalog}
        actual={}
        for table in domain:
            db._attempt.check()
            row=objects.get(('table',table))
            actual[table]=None if row is None else row[3]
            if row is None:
                if table in cover:_history_failure()
            else:
                if cover.get(table)!=row[3]:_history_failure()
                for event in ('INSERT','UPDATE','DELETE'):
                    expected=_history_trigger(table,event)
                    name='history_proof_'+table+'_'+event.lower()
                    db._attempt.check()
                    trigger=objects.get(('trigger',name))
                    if trigger is None or trigger[3]!=expected:_history_failure()
        if set(cover)!={t for t,v in actual.items() if v is not None}:_history_failure()
        cookie=db.execute(f'PRAGMA {schema}.schema_version').fetchone()[0]
        if type(cookie) is not int or cookie<0:_history_failure()
        db._attempt.check()
        _history_schema_check(db,schema,catalog)
        parts.append((epoch,cookie,tuple(sorted(actual.items())),catalog))
    vector=tuple(parts)
    if capsule.vector is None:capsule.vector=vector
    elif vector!=capsule.vector:unproven()
    return vector


def _c0_bounded_rows(db, sql, params, capsule, total_bytes=0, *, _allocation_owner=None):
    """Preflight each raw row before one page-local payload cursor admits it.

    This retains the same snapshot, whole Entry and material thresholds. It
    changes physical statement/error timing, not native-event bisimulation.
    """
    capsule.check()
    cursor=db.execute('SELECT * FROM ('+sql+') LIMIT 0',params)
    try:columns=tuple(part[0] for part in cursor.description)
    finally:cursor.close()
    expressions=[]
    for name in columns:
        column='"'+name.replace('"','""')+'"'
        expressions.append("CASE typeof("+column+") WHEN 'text' THEN length(CAST("+column+" AS BLOB)) WHEN 'blob' THEN length("+column+") ELSE 8 END")
    cursor=db.execute('SELECT '+(' + '.join(expressions))+' FROM ('+sql+')',params)
    rows=[];payload=None
    try:
        while True:
            capsule.check()
            metadata=cursor.fetchone()
            if metadata is None:break
            size=metadata[0]
            if type(size) is not int or size<0 or size>1048576 or total_bytes+size>16777216:unproven()
            if _allocation_owner is not None:
                _allocation_owner.reserve(_lifecycle_raw_charge(size, len(columns)) + 128 * size + 8192,
                    'c0-discovery-original-row-and-native-json-scratch')
            capsule.check()
            if payload is None:
                payload=db.execute('SELECT * FROM ('+sql+')',params)
            else:
                # The removed execute's _AttemptConnection._bounded admission:
                # same captured Entry, check ordering and absolute remainder.
                # Keep fetch outside _bounded: payload errors get zero retries.
                attempt=db._attempt
                end=min(attempt.deadline,getattr(attempt,'busy_deadline',float('inf')),
                        _c0_monotonic()+attempt.policy.busy_budget_seconds)
                attempt.check()
                if end-_c0_monotonic()<=0:
                    attempt.check()
                    raise WorldError('EXECUTION_STORAGE_BUSY','执行存储繁忙')
            row=payload.fetchone()
            if type(row) is not sqlite3.Row:unproven()
            total_bytes+=size
            rows.append(row)
    finally:
        # Attempt both closes, retain the primary object and its first traceback.
        # Cleanup exceptions are diagnostic obligations, never a retry signal.
        import sys
        primary=sys.exception();cleanup=[]
        for owned in (payload,cursor):
            if owned is not None:
                try:owned.close()
                except BaseException as error:cleanup.append((error,error.__traceback__))
        if cleanup:
            if primary is not None:
                primary.__dict__.setdefault('_c0_row_cleanup_failures',[]).extend(cleanup)
            else:
                error,firsttb=cleanup[0]
                error.__dict__.setdefault('_c0_row_cleanup_failures',[]).extend(cleanup[1:])
                raise error.with_traceback(firsttb)
    capsule.check()
    return tuple(rows),total_bytes


from time import monotonic as _c0_monotonic


def _c0_preparation_pin_busy(capsule, db, error, firsttb):
    """Authenticate the new actual native caller, never a lookalike budget."""
    from .actor_execution import PreparationBudget, ActorRunError
    from .world import _AttemptConnection
    from .source_attestation import _REGISTRY
    import types
    try:
        function=PreparationBudget.native_call
        namespace=function.__globals__;stamp=namespace.get('_SOURCE_GENERATION')
        world_function=_AttemptConnection._bounded
        world_namespace=world_function.__globals__;world_stamp=world_namespace.get('_SOURCE_GENERATION')
        for current,space,name in ((stamp,namespace,'actor_execution.py'),(world_stamp,world_namespace,'world.py')):
            if (current is None or _REGISTRY.get(id(space)) is not current
                    or current['namespace'] is not space or not current['matched']
                    or current['path']!=capsule.source_root/'app'/name
                    or current['sha256']!=capsule.source_generation.get('app/'+name)):
                return False
        if (type(capsule.budget) is not PreparationBudget or type(error) is not ActorRunError
                or error.code!='EXECUTION_STORAGE_BUSY' or type(db) is not _AttemptConnection
                or db._attempt is not capsule.budget
                or namespace.get('PreparationBudget') is not PreparationBudget
                or namespace.get('ActorRunError') is not ActorRunError
                or not all(any(obj is old for old in stamp['objects']) for obj in (PreparationBudget,ActorRunError))
                or not any(_AttemptConnection is old for old in world_stamp['objects'])
                or not _history_original_method(function,stamp)
                or not _history_original_method(world_function,world_stamp)
                or not _history_original_method(capsule.world._history_segment,world_stamp)
                or not _history_original_method(capsule.world._connection,world_stamp)):
            return False
        native=error.__context__;number=getattr(native,'sqlite_errorcode',None)
        if (type(native) is not sqlite3.OperationalError or type(number) is not int
                or number & 255 not in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED)):
            return False
        cursor=firsttb;bridges=[]
        if cursor is None:return False
        while cursor is not None:
            if cursor.tb_frame.f_code is world_function.__code__:bridges.append(cursor.tb_frame)
            tail=cursor;cursor=cursor.tb_next
        frame=tail.tb_frame;local=frame.f_locals
        if (frame.f_code is not function.__code__ or frame.f_globals is not namespace
                or tail.tb_lineno!=function.__code__.co_firstlineno+14
                or local.get('self') is not capsule.budget
                or type(local.get('index')) is not int or local['index']!=2
                or len(bridges)!=1 or bridges[0].f_globals is not world_namespace
                or bridges[0].f_locals.get('self') is not db
                or bridges[0].f_locals.get('attempt') is not capsule.budget):
            return False
        call=local.get('function')
        if (not isinstance(call,types.BuiltinMethodType) or getattr(call,'__self__',None) is not db
                or call!=sqlite3.Connection.execute.__get__(db)
                or bridges[0].f_locals.get('function')!=call):return False
        origin=native.__traceback__
        if origin is None:return False
        while origin.tb_next is not None:origin=origin.tb_next
        return origin.tb_frame is frame and origin.tb_lineno==function.__code__.co_firstlineno+6
    except Exception:
        return False


def _c0_first_pin_busy(capsule, db, error, firsttb):
    """Authenticate only this loaded World's terminal native execute BUSY.

    A saved textual traceback, code caption or historical observer is not a
    capability. Unknown/old errors fail closed. This trusts the pinned Python
    runtime, stdlib SQLite and original loaded-generation registry, not an
    attacker with arbitrary in-process mutation authority.
    """
    from .world import _AttemptConnection
    import types
    try:
        from .actor_execution import PreparationBudget
        if type(capsule.budget) is PreparationBudget:
            return _c0_preparation_pin_busy(capsule,db,error,firsttb)
        function=_AttemptConnection._bounded
        namespace=function.__globals__
        stamp=namespace.get('_SOURCE_GENERATION')
        if (stamp is None or not stamp['matched'] or stamp['namespace'] is not namespace
                or not any(_AttemptConnection is obj for obj in stamp['objects'])
                or not any(WorldError is obj for obj in stamp['objects'])
                or namespace.get('WorldError') is not WorldError
                or type(error) is not WorldError or error.code!='EXECUTION_STORAGE_BUSY'
                or type(db) is not _AttemptConnection or db._attempt is not capsule.budget
                or not _history_original_method(function,stamp)
                or not _history_original_method(capsule.world._history_segment,stamp)
                or not _history_original_method(capsule.world._connection,stamp)):
            return False
        native=error.__context__
        number=getattr(native,'sqlite_errorcode',None)
        if (type(native) is not sqlite3.OperationalError or type(number) is not int
                or number & 255 not in (sqlite3.SQLITE_BUSY,sqlite3.SQLITE_LOCKED)):
            return False
        tail=firsttb
        if tail is None:return False
        while tail.tb_next is not None:tail=tail.tb_next
        frame=tail.tb_frame;local=frame.f_locals
        # This exact code was compiled from the unchanged World source. Its
        # Legacy terminal index==2 is +26; +14 is pre-native remaining refusal.
        if (frame.f_code is not function.__code__ or frame.f_globals is not namespace
                or tail.tb_lineno!=function.__code__.co_firstlineno+26
                or local.get('self') is not db or local.get('attempt') is not capsule.budget
                or type(local.get('index')) is not int or local['index']!=2):
            return False
        call=local.get('function')
        if (not isinstance(call,types.BuiltinMethodType) or getattr(call,'__self__',None) is not db
                or call!=sqlite3.Connection.execute.__get__(db)):
            return False
        origin=native.__traceback__
        if origin is None:return False
        while origin.tb_next is not None:origin=origin.tb_next
        # A Python-raised lookalike has a deeper frame; actual native execute
        # raises at this exact legacy _bounded call site (+20).
        return (origin.tb_frame is frame and origin.tb_lineno==function.__code__.co_firstlineno+20)
    except Exception:
        return False


class _C0Fresh:
    """A single logical segment's shared admission/first-pin recovery slot."""
    __slots__=('capsule','budget','stop','bounds','used','first_failure','failed_reader','successor')
    def __init__(self,capsule):
        self.capsule=capsule;self.budget=capsule.budget;self.stop=capsule.budget.stop
        self.bounds=(capsule.budget.deadline,capsule.budget.busy_deadline)
        self.used=False;self.first_failure=None
        self.failed_reader=None;self.successor=None


def _c0_recovery_wait(capsule,fresh,failure):
    """Claim before waiting; never refund, rebuild an Entry or extend its bounds."""
    if (type(fresh) is not _C0Fresh or fresh.capsule is not capsule
            or fresh.budget is not capsule.budget or fresh.used):
        raise failure[0].with_traceback(failure[1])
    fresh.used=True;fresh.first_failure=failure
    try:
        capsule.check_source()
        bounds=(capsule.budget.deadline,capsule.budget.busy_deadline)
        if capsule.budget.stop is not fresh.stop or bounds!=fresh.bounds or any(type(v) not in (int,float) or not -float('inf')<v<float('inf') for v in bounds):
            raise failure[0].with_traceback(failure[1])
        remaining=min(bounds)-_c0_monotonic()
        if not .05<remaining<float('inf'):
            raise failure[0].with_traceback(failure[1])
        from .actor_execution import PreparationBudget
        if type(capsule.budget) is PreparationBudget:
            capsule.budget.wait(.05)
            stopped=False
        else:
            stopped=capsule.budget.stop.wait(.05)
        capsule.check_source()
        if (stopped or (capsule.budget.deadline,capsule.budget.busy_deadline)!=fresh.bounds
                or capsule.budget is not fresh.budget or capsule.budget.stop is not fresh.stop):
            raise failure[0].with_traceback(failure[1])
    except BaseException as error:
        if error is not failure[0]:error.__dict__.setdefault('_c0_first_pin_failure',failure)
        raise


def _c0_admission_record(local,native,world,budget,actor_path,line,attach_line):
    """Pure shape/identity predicate; never grants loaded-native authority alone."""
    from .actor_execution import PreparationBudget
    args=native.get('args');kwargs=native.get('kwargs')
    owner=(native.get('self') is local.get('db') and native.get('attempt') is budget)
    if type(budget) is PreparationBudget:
        owner=(native.get('self') is budget
            and getattr(native.get('function'),'__self__',None) is local.get('db'))
    return (type(line) is int and line==attach_line
        and local.get('self') is world and local.get('budget') is budget
        and local.get('actor_path') is actor_path
        and type(local.get('failure')) is list and not local['failure']
        and type(local.get('old_length')) is int
        and owner and local.get('db') is not None
        and type(native.get('index')) is int and native['index']==2
        and type(args) is tuple and len(args)==2
        and type(args[0]) is str and args[0]=='ATTACH DATABASE ? AS actor'
        and type(args[1]) is tuple and len(args[1])==1
        and type(args[1][0]) is str and args[1][0]==actor_path.as_uri()+'?mode=ro'
        and type(kwargs) is dict and not kwargs)


def _c0_admission_busy(capsule,error,firsttb,history_cm,lock_cm):
    """Only actual loaded ATTACH-before-yield, exhausted CMs and closed reader.

    Connection.in_transaction's original C descriptor only checks the handle;
    it executes no SQL, changes no callback and rejects a closed handle with
    ProgrammingError. Real descriptor behavior remains native-review deferred.
    """
    from .world import WorldStore,_AttemptConnection
    from .source_attestation import _REGISTRY
    from contextlib import _GeneratorContextManager
    import _sqlite3
    import types
    try:
        world=capsule.world
        lock_namespace=source_lock.__wrapped__.__globals__
        lock_stamp=lock_namespace.get('_SOURCE_GENERATION')
        if (lock_stamp is None or _REGISTRY.get(id(lock_namespace)) is not lock_stamp
                or lock_stamp['namespace'] is not lock_namespace or not lock_stamp['matched']
                or lock_stamp['path']!=capsule.source_root/'app/coordinator_scheduler.py'
                or lock_stamp['sha256']!=capsule.source_generation.get('app/coordinator_scheduler.py')
                or not _history_original_method(source_lock,lock_stamp)
                or type(lock_cm) is not _GeneratorContextManager
                or lock_cm.gen.gi_code is not source_lock.__wrapped__.__code__
                or lock_cm.gen.gi_frame is not None):return None
        history=WorldStore._history_segment.__wrapped__
        connection=WorldStore._connection.__wrapped__
        namespace=history.__globals__;stamp=namespace.get('_SOURCE_GENERATION')
        if (type(world) is not WorldStore or stamp is None
                or _REGISTRY.get(id(namespace)) is not stamp
                or stamp['namespace'] is not namespace or not stamp['matched']
                or stamp['path']!=capsule.source_root/'app/world.py'
                or stamp['sha256']!=capsule.source_generation.get('app/world.py')
                or not any(WorldStore is obj for obj in stamp['objects'])
                or namespace.get('WorldStore') is not WorldStore
                or world._history_segment.__self__ is not world
                or world._history_segment.__func__ is not WorldStore._history_segment
                or world._connection.__self__ is not world
                or world._connection.__func__ is not WorldStore._connection
                or not _history_original_method(history,stamp)
                or not _history_original_method(connection,stamp)
                or type(history_cm) is not _GeneratorContextManager
                or history_cm.gen.gi_code is not history.__code__
                or history_cm.gen.gi_frame is not None
                or sqlite3.Connection is not _sqlite3.Connection
                or sqlite3.OperationalError is not _sqlite3.OperationalError
                or sqlite3.ProgrammingError is not _sqlite3.ProgrammingError):
            return None
        matches=[];cursor=firsttb
        while cursor is not None:
            if cursor.tb_frame.f_code is history.__code__:matches.append(cursor)
            cursor=cursor.tb_next
        if len(matches)!=1:return None
        origin=matches[0];local=origin.tb_frame.f_locals
        db=local.get('db')
        if (origin.tb_frame.f_globals is not namespace
                or origin.tb_lineno!=history.__code__.co_firstlineno+32
                or local.get('self') is not world or local.get('budget') is not capsule.budget
                or local.get('actor_path') is not capsule.storage.actor_path
                or type(local.get('failure')) is not list or local['failure']
                or type(local.get('old_length')) is not int
                or type(db) is not _AttemptConnection
                or set(vars(db))!={'_attempt'} or db._attempt is not capsule.budget
                or not _c0_first_pin_busy(capsule,db,error,firsttb)):
            return None
        tail=firsttb
        while tail.tb_next is not None:tail=tail.tb_next
        native=tail.tb_frame.f_locals
        if not _c0_admission_record(local,native,world,capsule.budget,capsule.storage.actor_path,
                origin.tb_lineno,history.__code__.co_firstlineno+32):return None
        # The original C descriptor cannot be shadowed by an instance property.
        # In the original synchronous CM, creator-close follows all cleanup.
        try:sqlite3.Connection.in_transaction.__get__(db,sqlite3.Connection)
        except sqlite3.ProgrammingError:
            return db
        return None
    except Exception:
        return None


@contextmanager
def _c0_segment(capsule,fresh):
    """Admission-only retry. Once yielded, every payload/final-pin error escapes."""
    if type(fresh) is not _C0Fresh or fresh.capsule is not capsule:unproven()
    while True:
        admitted=False;history_cm=None;lock_cm=None
        try:
            lock_cm=source_lock(capsule.world)
            with lock_cm:
                capsule.check_source()
                history_cm=capsule.world._history_segment(capsule.storage.actor_path,capsule.budget)
                with history_cm as db:
                    admitted=True
                    if fresh.used:
                        if db is fresh.failed_reader:unproven()
                        fresh.successor=db
                    yield db
                return
        except BaseException as error:
            failure=(error,error.__traceback__)
            if fresh.first_failure is not None and error is not fresh.first_failure[0]:
                error.__dict__.setdefault('_c0_first_pin_failure',fresh.first_failure)
            if admitted or fresh.used or history_cm is None:raise
            failed_reader=_c0_admission_busy(capsule,error,failure[1],history_cm,lock_cm)
            if failed_reader is None:raise
            fresh.failed_reader=failed_reader
            _c0_recovery_wait(capsule,fresh,failure)


def _c0_collect(capsule):
    """Bounded native keysets; terminal empty pages and optional absence."""
    world=capsule.world;tables={};pages=[];total_bytes=0
    destination = _lifecycle_value_context.get()
    if destination is None:
        # Preserve the original cold legacy path. The real operation wrapper
        # supplies an owner for controlled V3; there is no implicit fake owner.
        # E0 segment captures exact catalogs and explicit optional-table absence.
        with _c0_segment(capsule,_C0Fresh(capsule)) as db:
            capsule.check_source();vector=capsule.pin(db)
            initial_rows,_=_c0_bounded_rows(db,'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1',(),capsule)
            initial_raw=initial_rows[0] if initial_rows else None
            catalogs={schema:tuple(db.execute(f'SELECT type,name,tbl_name,sql FROM {schema}.sqlite_master ORDER BY type,name').fetchall()) for schema in ('main','actor')}
            capsule.pin(db);capsule.check_source()
        # Original next_activation reads latest before its first native root SQL.
        # Decode that raw snapshot after close, not under a material/source lock.
        if initial_raw is None:unproven()
        initial_state=json.loads(initial_raw[0])
        with _c0_segment(capsule,_C0Fresh(capsule)) as db:
            capsule.check_source();capsule.pin(db)
            world._check_coordinator_execution(db,initial_state['world_id'],capsule.coordinator,readonly=True)
            capsule.pin(db);capsule.check_source()
        optin = _c0_v3_candidate(initial_state)
        initial_rows = initial_raw = initial_state = None
    else:
        prefix = _LifecycleValueOwner(destination.memory)
        try:
            optin, catalog_loan = _c0_collect_discovery(capsule, prefix)
            vector = capsule.vector
            if optin:
                catalogs = None
            else:
                # Catalog Rows really survive inside the returned v2 material;
                # transfer that charge to the operation, not to a cleared local.
                catalogs = catalog_loan.value
                catalog_loan.transfer(destination)
            catalog_loan = None
            prefix.close()
        except BaseException:
            prefix.fail()
            destination.fail()
            raise
    if optin:
        owner = _C0V3MaterialOwner(capsule)
        token = _lifecycle_live_context.set(owner.memory)
        try:
            catalogs = vector = None
            return _c0_collect_v3(owner)
        except BaseException:
            owner.fail()
            raise
        finally:
            _lifecycle_live_context.reset(token)
    for schema,rows in catalogs.items():tables[schema+'.sqlite_master']=rows
    present={schema+'.'+name for schema,part in zip(('main','actor'),vector) for name,ddl in part[2] if ddl is not None}
    for table,keys in _C0_KEYS.items():
        if table not in present:
            pages.append((table,'absent'));continue
        collected=[];last=None
        while True:
            capsule.check()
            fresh=_C0Fresh(capsule)
            first_failure=None
            for admission in range(2):
                pin_failure=None;qualified=False
                try:
                    with _c0_segment(capsule,fresh) as db:
                        capsule.check_source()
                        try:
                            _c0_collect_pin(capsule,db)
                        except BaseException as pin_error:
                            pin_failure=(pin_error,pin_error.__traceback__)
                            qualified=_c0_first_pin_busy(capsule,db,*pin_failure)
                            raise
                        keyexpr=','.join(keys);where=''
                        params=()
                        if last is not None:
                            where=' WHERE ('+keyexpr+') > ('+','.join('?' for _ in keys)+')';params=last
                        sql='SELECT * FROM '+table+where+' ORDER BY '+keyexpr+' LIMIT 32'
                        rows,total_bytes=_c0_bounded_rows(db,sql,params,capsule,total_bytes)
                        _c0_collect_pin(capsule,db);capsule.check_source()
                except BaseException as error:
                    if first_failure is None and pin_failure is not None and error is not pin_failure[0]:
                        error.__dict__.setdefault('_c0_first_pin_failure',pin_failure)
                    if first_failure is not None and error is not first_failure[0]:
                        error.__dict__.setdefault('_c0_first_pin_failure',first_failure)
                    # Both original context managers have now unwound. A
                    # cleanup replacement is NEVER the locally captured error.
                    if (admission!=0 or fresh.used or pin_failure is None
                            or error is not pin_failure[0] or not qualified):
                        raise
                    first_failure=pin_failure
                    fresh.failed_reader=db
                    _c0_recovery_wait(capsule,fresh,pin_failure)
                    continue
                break
            capsule.check()
            for row in rows:
                key=tuple(row[k] for k in keys)
                if any(type(v) not in (str,int) for v in key):unproven()
            pages.append((table,last,len(rows)))
            collected.extend(rows)
            if not rows:break
            last=tuple(rows[-1][k] for k in keys)
        tables[table]=tuple(collected)
    capsule.check()
    return MappingProxyType(dict(tables=MappingProxyType(tables),check=capsule.check,pages=tuple(pages),bytes=total_bytes))


def _c0_v3_candidate(state):
    # Representation selection is not a schema/authority verdict. An invalid
    # non-LC configuration must reach its original legacy proof/error site,
    # not raise a new early KeyError before the original table reads.
    from .world import _LC_WAKEUP, _LC_VERSION
    if type(state) is not dict:
        return False
    config = state.get('_config')
    if type(config) is not dict:
        return False
    wake, life = config.get('event_wakeup'), config.get('lifecycle')
    return (type(wake) is dict and wake.get('version') == _LC_WAKEUP
            and type(life) is dict and life.get('version') == _LC_VERSION)


def _c0_collect_discovery(capsule, prefix):
    """Original latest/catalog/root SQL and guard order, with prepaid locals.

    No genesis query, strict JSON decoder or V3 iterator is inserted into the
    legacy prefix. Original source-pin internals retain their inherited owner.
    """
    world = capsule.world
    with _c0_segment(capsule, _C0Fresh(capsule)) as db:
        capsule.check_source(); vector = capsule.pin(db)
        initial_rows, _ = _c0_bounded_rows(db,
            'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1', (), capsule,
            _allocation_owner=prefix)
        initial_raw = initial_rows[0] if initial_rows else None
        # Price the duplicate catalog Rows before the original fetchall. The
        # source-pin vector already contains the exact source-validated scalar
        # bytes; this adds no SQL or dynamic original-read check.
        count = sum(len(part[3]) for part in vector)
        size = sum(_c0_v3_text_size(value) for part in vector for row in part[3]
                   for value in row if type(value) is str)
        catalog_loan = _LifecycleValueLoan(prefix.memory,
            prefix.memory.hold(32 * size + 8192 * count + 65536, 'c0-discovery-catalog-copy'))
        prefix.take(catalog_loan)
        catalog_loan.value = {schema:tuple(db.execute(
            f'SELECT type,name,tbl_name,sql FROM {schema}.sqlite_master ORDER BY type,name').fetchall())
            for schema in ('main', 'actor')}
        capsule.pin(db); capsule.check_source()
    if initial_raw is None:
        unproven()
    initial_state = json.loads(initial_raw[0])
    with _c0_segment(capsule, _C0Fresh(capsule)) as db:
        capsule.check_source(); capsule.pin(db)
        world._check_coordinator_execution(db, initial_state['world_id'], capsule.coordinator, readonly=True)
        capsule.pin(db); capsule.check_source()
    return _c0_v3_candidate(initial_state), catalog_loan


def next_activation(world, window_id, *, coordinator):
    """C0 only: native segments -> offlock fullproof -> fresh native seal."""
    from .actor_execution import EntryBudget,ExecutionPolicy
    world._validate_coordinator_execution(coordinator)
    with world._coordinator_scope(coordinator):
        root=world._scoped_coordinator_attempt();actor=world._scoped_attempt()
        entry=world._scoped_entry_budget()
        if entry is None:
            entry=EntryBudget(root.policy if root is not None else ExecutionPolicy(),
                root.stop if root is not None else threading.Event(),(root,actor))
        with world._entry_budget(entry):
            with source_lock(world):capsule=_C0Selection(world,coordinator,entry)
            material=_c0_collect(capsule)
            with _c0_v3_material_scope(material):
                result = _c0_v3_invoke(material, _c0_next_activation_material,
                    world, capsule, material, window_id, coordinator)
                capsule = None
                return result


def _c0_next_activation_material(world, capsule, material, window_id, coordinator):
    # This boundary has no reader/cursor/source lock. It is the complete
    # original material proof, not an epoch-only or row-count shortcut.
    _c0_fullproof(world,capsule.storage,material,window_id)
    capsule.check()
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _c0_v3_next_activation_seal, world, capsule, material, window_id, coordinator)
    with _c0_segment(capsule,_C0Fresh(capsule)) as db:
        capsule.check_source();capsule.pin(db)
        final_rows,_=_c0_bounded_rows(db,'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1',(),capsule)
        if not final_rows:unproven()
        state=json.loads(final_rows[0][0])
        world._check_coordinator_execution(db,state['world_id'],coordinator,readonly=True)
        registration(db,state['world_id'],capsule.identity)
        final_rows,_=_c0_bounded_rows(db,"SELECT * FROM coordinator_activations WHERE world_id=? AND window_id=? AND NOT (phase IN ('completed','rejected','skipped') AND private_closed=1) ORDER BY roster_index LIMIT 1",(state['world_id'],window_id),capsule)
        row=final_rows[0] if final_rows else None
        capsule.storage.paths()
        world._check_coordinator_execution(db,state['world_id'],coordinator,readonly=True)
        capsule.pin(db)
        # No SQL after this fresh source/actual objects/cancellation gate.
        capsule.check_source()
        if capsule.consumed:unproven()
        capsule.consumed=True
        result=dict(row) if row is not None else None
    capsule.check()
    return result



def complete_window(world, window_id, *, coordinator):
    storage, identity = source(world)
    with storage.host._with_entry_pipeline(), world._coordinator_scope(coordinator):
        with ledger_transaction(world, storage, identity) as db:
            state = latest(db)
            registration(db, state['world_id'], identity)
            window = window_record(db, state['world_id'], window_id)
            incomplete = db.execute("SELECT 1 FROM coordinator_activations WHERE world_id=? AND window_id=? AND NOT (phase IN ('completed','rejected','skipped') AND private_closed=1) LIMIT 1", (state['world_id'], window_id)).fetchone()
            if incomplete:
                raise WorldError('WINDOW_INCOMPLETE', '窗口仍有未闭合人物激活')
            window_phase_proof(world, db, window_id)
            if window['state'] != 'completed':
                db.execute("UPDATE coordinator_windows SET state='completed',completed_revision=? WHERE world_id=? AND window_id=?", (state['revision'], state['world_id'], window_id))
            return window_record(db, state['world_id'], window_id)


def _open_history_proof(capsule, material):
    if _c0_v3_material(material) is not None:
        return _c0_v3_invoke(material, _open_history_proof_v3, capsule, material)
    """All ordered history and complete phase/lineage proof after segment close."""
    with _c0_query_scope(material) as material:
        world=capsule.world
        state=_c0_latest(material)
        _p04_material_current(material,state)
        rows=_c0_read(material,'main.coordinator_windows',(('world_id',state['world_id']),),('window_id',),(('ordinal',False),))
        previous=None
        for ordinal,row in enumerate(rows,1):
            capsule.check()
            window=_c0_window_record(material,state['world_id'],row[0])
            base_row=_c0_read(material,'main.snapshots',(('seq',window['base_revision']),),('state',),first=True)
            if base_row is None:unproven()
            base=json.loads(base_row[0])
            if (window['ordinal']!=ordinal
                    or (previous is not None and (previous['state']!='completed'
                        or window['base_revision']<previous['completed_revision']))
                    or base['time']!=(previous['target_time'] if previous else 0)):
                unproven()
            previous=window
        if state['time']!=(previous['target_time'] if previous else 0):unproven()
        for row in rows:_c0_fullproof(world,capsule.storage,material,row[0])
        records=_c0_read(material,'main.coordinator_scheduler_registration',(('world_id',state['world_id']),))
        if len(records)!=1:unproven()
        capsule.check()
        return state,previous,len(rows),dict(records[0])


class _OpenWindowSeal:
    """One open call, one fresh prewrite match, one actual precommit gate."""
    def __init__(self,capsule,proof):
        self.capsule=capsule
        self.state,self.previous,self.ordinal,self.registration=proof
        self.db=None;self.matched=False;self.sealed=False

    def before_effect(self,db,state,registered):
        capsule=self.capsule
        capsule.check_source()
        if self.matched or self.sealed or not db.in_transaction or db._attempt is not capsule.budget:unproven()
        capsule.pin(db)
        if state!=self.state or registered!=self.registration:unproven()
        row=db.execute('SELECT window_id FROM coordinator_windows WHERE world_id=? ORDER BY ordinal DESC LIMIT 1',(state['world_id'],)).fetchone()
        previous=window_record(db,state['world_id'],row[0]) if row is not None else None
        ordinal=db.execute('SELECT coalesce(max(ordinal),0) FROM coordinator_windows WHERE world_id=?',(state['world_id'],)).fetchone()[0]
        if previous!=self.previous or type(ordinal) is not int or ordinal!=self.ordinal:unproven()
        self.db=db;self.matched=True
        capsule.check()
        return previous

    def __call__(self,db):
        # Original storage paths/Attempt/root SQL ran immediately before this.
        self.capsule.check_source()
        if self.sealed or not self.matched or self.db is not db or not db.in_transaction or db._attempt is not self.capsule.budget:unproven()
        self.sealed=True


def open_window(world, *, coordinator, expected_version):
    from .actor_execution import EntryBudget,ExecutionPolicy
    version=version_shape(expected_version)
    storage,identity=source(world)
    with storage.host._with_entry_pipeline(),world._coordinator_scope(coordinator):
        root=world._scoped_coordinator_attempt();actor=world._scoped_attempt()
        entry=world._scoped_entry_budget()
        if entry is None:
            entry=EntryBudget(root.policy if root is not None else ExecutionPolicy(),
                root.stop if root is not None else threading.Event(),(root,actor))
        with world._entry_budget(entry):
            with source_lock(world):capsule=_C0Selection(world,coordinator,entry)
            material=_c0_collect(capsule)
            with _c0_v3_material_scope(material):
                result = _c0_v3_invoke(material, _c0_open_window_material,
                    world, storage, identity, capsule, material, coordinator, version)
                capsule = None
                return result


def _c0_open_window_material(world, storage, identity, capsule, material, coordinator, version):
    proof=_open_history_proof(capsule,material)
    capsule.check()
    seal=_OpenWindowSeal(capsule,proof)
    with ledger_transaction(world,storage,identity,_precommit=seal) as db:
        state = latest(db)
        check_version(state, version, revision=False)
        registered=registration(db,state['world_id'],identity)
        previous=seal.before_effect(db,state,registered)
        if previous is not None and previous['state'] == 'open':
            return previous
        check_version(state, version)
        if (type(state['revision']) is not int or not 1 <= state['revision'] < MAX_INT
                or type(state['time']) is not int or type(state['horizon']) is not int
                or not 0 <= state['time'] <= state['horizon'] <= MAX_INT):
            unproven()
        previous_ordinal = db.execute('SELECT coalesce(max(ordinal),0) FROM coordinator_windows WHERE world_id=?', (state['world_id'],)).fetchone()[0]
        if type(previous_ordinal) is not int or not 0 <= previous_ordinal < MAX_INT:
            unproven()
        ordinal = previous_ordinal+1
        world._restore_geography(db, state)
        base = state['revision']
        world._advance_state(state, db, world._p04_target(state))
        roster = world._p04_open(state)
        baseline = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
        world._check_frozen_pathology(state, baseline)
        world._check_frozen_p04(state, baseline)
        state['revision'] += 1
        db.execute('INSERT INTO snapshots VALUES (?,?)', (state['revision'], world._encode_state(db, state)))
        window = uuid.uuid4().hex
        done = not roster
        db.execute('INSERT INTO coordinator_windows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (state['world_id'], window, ordinal, base, state['time'], state['horizon'], state['revision'],
                    'completed' if done else 'open', 'horizon' if state['time']==state['horizon'] else 'normal', _json(roster), sha(roster),
                    coordinator.epoch, coordinator.epoch, state['revision'] if done else None))
        for index, person in enumerate(roster):
            db.execute('''INSERT INTO coordinator_activations
                        (world_id,window_id,activation_id,roster_index,person_id,phase,created_parent_epoch,owner_parent_epoch)
                        VALUES (?,?,?,?,?,'pending',?,?)''', (state['world_id'], window, uuid.uuid4().hex, index, person, coordinator.epoch, coordinator.epoch))
        return window_record(db, state['world_id'], window)


# Lifecycle readers and root business scope. No authority is stored in snapshots.
_LIFECYCLE_TABLES = {
    'main.snapshots': ('seq state', 'seq', 'seq'),
    'main.intents': ('id person payload receipt', 'id', ''),
    'main.dm_proposals': ('intent_id world_id person_id text', 'intent_id', ''),
    'main.dm_decisions': ('intent_id version data', 'intent_id version', ''),
    'main.dm_timeline': ('intent_id kind version visible_from', 'intent_id kind version', ''),
    'main.coordinator_windows': ('world_id window_id ordinal base_revision target_time horizon_at_open advanced_revision state boundary_kind roster_json roster_sha256 created_parent_epoch owner_parent_epoch completed_revision', 'advanced_revision world_id', 'ordinal'),
    'main.coordinator_activations': ('world_id window_id activation_id roster_index person_id phase intent_id observation_revision card_version base_session_version request_sha256 candidate_sha256 receipt_sha256 promoted_version terminal_reason private_closed resume_phase diagnostic_code created_parent_epoch owner_parent_epoch owner_actor_epoch', 'intent_id window_id', ''),
    'actor.actor_intents': ('intent_id person_id body state candidate metrics receipt consumed request', 'intent_id person_id', ''),
    'actor.actor_intent_execution': ('intent_id person_id world_id base_version base_messages_sha256 base_seen_sha256 owner_epoch owner_token attempt_id promoted_version', 'intent_id person_id', ''),
    'actor.actor_calls': ('call_id intent_id person_id', 'intent_id person_id', ''),
}


def _lifecycle_world_primitives():
    """Current globals of the registered World projector/condition graph."""
    from .world import WorldStore
    from .source_attestation import _REGISTRY
    import types
    namespace=WorldStore._lifecycle_check.__globals__
    stamp=_REGISTRY.get(id(namespace))
    if (stamp is None or stamp['namespace'] is not namespace or not stamp['matched']
            or namespace.get('WorldStore') is not WorldStore
            or not any(WorldStore is old for old in stamp['objects'])):unproven()
    for function in stamp['objects']:
        if (isinstance(function,types.FunctionType)
                and function.__name__.startswith(('_lc_','_lifecycle_'))):
            if (namespace.get(function.__name__) is not function
                    or not _history_original_method(function,stamp)):unproven()


def _lifecycle_scalar_row(table, row, columns=None):
    # This validates native scalar values; it never copies or creates a Row.
    names=_LIFECYCLE_TABLES[table][0].split() if columns is None else columns
    integer_fields={'seq','version','visible_from','ordinal','base_revision','target_time',
                    'horizon_at_open','advanced_revision','created_parent_epoch','owner_parent_epoch',
                    'completed_revision','roster_index','observation_revision','card_version',
                    'base_session_version','promoted_version','private_closed','owner_actor_epoch',
                    'base_version','owner_epoch'}
    nullable={'main.intents':{'receipt'},'main.coordinator_windows':{'completed_revision'},
              'main.coordinator_activations':{'intent_id','observation_revision','card_version',
                'base_session_version','request_sha256','candidate_sha256','receipt_sha256',
                'promoted_version','terminal_reason','resume_phase','diagnostic_code','owner_actor_epoch'},
              'actor.actor_intents':{'candidate','metrics','receipt'},
              'actor.actor_intent_execution':{'promoted_version'}}
    for index,name in enumerate(names):
        value=row[index] if columns is not None else row[name]
        if value is None and name in nullable.get(table,()):continue
        if type(value) is not (int if name in integer_fields else str):unproven()


def _lifecycle_snapshot_proof(read, state, genesis):
    from .world import _lc_optin, _lifecycle_shape, _lifecycle_project
    _lifecycle_world_primitives()
    _lifecycle_shape(state, genesis)
    if not _lc_optin(genesis):
        return None
    projected = _lifecycle_project(read, genesis, state['revision'])
    if (projected['world_id'] != state['world_id'] or projected['revision'] != state['revision']
            or projected['lifecycle'] != state['_lifecycle']):
        unproven()
    return projected


def _lifecycle_material_read(material):
    if _c0_v3_material(material) is not None:
        return _LifecycleMaterialReader(material)
    # Full-table reads retain the exact collector Rows. The pre-existing C0
    # columns projection remains a builtin scalar tuple, never a forged Row.
    def read(table, equality=(), columns=None, order=(), limit=None, first=False):
        if table not in _LIFECYCLE_TABLES:
            unproven()
        if table not in material['tables']:
            material['check']()
            material['check']()
            return None if first else []
        return _c0_read(material, table, equality, columns, order, limit, first)
    return read


class _LifecycleLiveLimit(MemoryError):
    """Technical material-accounting refusal, never a business-domain verdict."""


# Forward-only read-only proof failure domains (1127).
_lc_ro1127_context = ContextVar('lc_ro1127_proof', default=None)


def _lc_ro1127_write_guard(world=None):
    proof = _lc_ro1127_context.get()
    owner = _lifecycle_value_context.get()
    if proof is not None or owner is not None and owner.readonly_proofs:
        # Any business writer is forbidden, including a writer for another world.
        (proof.memory if proof is not None else owner.memory)._failed = True
        raise _LifecycleLiveLimit('read-only proof cannot enter a business writer')


class _lc_ro1127_Proof:
    def __init__(self, world, entry, parent):
        self.world, self.entry, self.parent = world, entry, parent
        self.memory = parent.memory
        self.root = world._scoped_coordinator_attempt()
        self.capsule = self.owner = None
        self.armed = self.failed = self.ended = False
        self.first = self.terminal = None
        self.c0_owner = None
        self.native1132 = None
        # Fixed error/identity and scope carriers are paid on the ORIGINAL ledger.
        self.lease = self.memory.hold(65536, 'ro1127-proof-first-error-and-domain-carriers')
        self.lease.ro1127 = self

    def semantic(self, error):
        from .world import (_lc_fail, _lifecycle_project, _lifecycle_project_owned,
                            _lc_child, _lc_child_owned, _lc_project_step_owned)
        from .actor_execution import ActorRunError, PrivateStore
        from types import CodeType
        if not self.armed or self.ended or error is None:
            return False
        # Existing paid C0 admission remains intact. The separate actual native
        # frame now supplies its own complete prepayment/source/readonly binding;
        # unframed latest/private calls still cannot claim this exception domain.
        native1132 = _lc_ro1132_authenticated(self, error)
        if self.c0_owner is None and not native1132:
            frame = _c0_v3_frame_context.get()
            if (type(frame) is not dict or type(frame.get('owner')) is not _C0V3MaterialOwner
                    or frame['owner'].ro1127 is not self
                    or frame['owner'].memory is not self.memory):
                return False
            self.c0_owner = frame['owner']
        # A source/root/cancel error cannot qualify merely by having the same code.
        tb = BaseException.__traceback__.__get__(error)
        previous = last = None
        projection = False
        while tb is not None:
            previous, last = last, tb
            if any(tb.tb_frame.f_code is native for native in
                   (_lifecycle_project.__code__, _lifecycle_project_owned.__code__,
                    _lc_child.__code__, _lc_child_owned.__code__, _lc_project_step_owned.__code__)):
                projection = True
            tb = tb.tb_next
        if last is None:
            return False
        code = last.tb_frame.f_code
        if native1132:
            expected_globals = None
            for function in (_lc_fail, _lifecycle_project, _lifecycle_project_owned,
                    _lc_child, _lc_child_owned, _lc_project_step_owned,
                    _lifecycle_activation_identity_owned, unproven,
                    _p04_current_projection_owned, PrivateStore._audit):
                if code is function.__code__:
                    expected_globals = function.__globals__
                    if not _lc_ro1132_original(function):return False
                    break
                if any(type(item) is CodeType and code is item
                       for item in function.__code__.co_consts):
                    expected_globals = function.__globals__
                    if not _lc_ro1132_original(function):return False
                    break
            if expected_globals is None or last.tb_frame.f_globals is not expected_globals:
                return False
        if type(error) is WorldError and error.code == 'ACTOR_RECOVERY_UNPROVEN':
            if code is _lc_fail.__code__ and projection:
                # This old error code also carries a real resource refusal.
                return last.tb_frame.f_locals.get('message') != '结构历史事实超界'
        if type(error) is ActorRunError and error.code == 'ACTOR_RECOVERY_UNPROVEN':
            if code is PrivateStore._material_audit_rules_v3.__code__:
                return self.c0_owner is not None
            return (native1132 and code is PrivateStore._audit.__code__
                and last.tb_frame.f_globals is PrivateStore._audit.__globals__
                and last.tb_frame.f_locals.get('read') is self.native1132.reader
                and last.tb_frame.f_locals.get('self') is self.capsule.storage
                and _lc_ro1132_original(PrivateStore._audit))
        if (type(error) is WorldError and error.code == 'WINDOW_RECOVERY_UNPROVEN'
                and code is unproven.__code__ and previous is not None):
            return previous.tb_frame.f_code is _lifecycle_activation_identity_owned.__code__
        if type(error) is WorldError and error.code == 'INVALID_EVENT_WAKEUP':
            return code is _p04_current_projection_owned.__code__
        # The unchanged projection translates this exact local ValueError into
        # INVALID_EVENT_WAKEUP. Recognize that original native boundary BEFORE
        # its finally blocks can refund a traceback-retained carrier.
        if type(error) is ValueError:
            return any(type(item) is CodeType and item.co_name == 'opening' and code is item
                       for item in _p04_current_projection_owned.__code__.co_consts)
        return False

    def fail(self, error):
        if self.first is None:
            self.first = (error, BaseException.__traceback__.__get__(error),
                          BaseException.__context__.__get__(error),
                          BaseException.__cause__.__get__(error)) if error is not None else None
        self.failed = True
        # No failed flag, debt, alias or first tuple is ever cleared.
        if self.memory._failed or not self.semantic(error):
            self.memory._failed = True
        # Unknown/resource contexts never disappear behind a semantic wrapper.
        related = error
        for _ in range(8):
            if related is None:
                break
            related = (BaseException.__cause__.__get__(related)
                       or BaseException.__context__.__get__(related))
            if related is not None and not self.semantic(related):
                self.memory._failed = True
                break
        else:
            self.memory._failed = True


def _lc_ro1127_account_failure(memory, error=None):
    import sys
    if error is None:
        error = sys.exception()
    proof = _lc_ro1127_context.get()
    if proof is None or proof.memory is not memory:
        memory._failed = True
        return
    proof.fail(error)


@contextmanager
def _lc_ro1127_proof_scope(world, entry, host):
    """Fresh read-only proof, never a fresh Entry, ledger or business permit.

    The caller catches outside THIS boundary but inside its original Entry.
    All dynamic Selection/source/root/generation/stop gates rerun per sibling.
    Physical cursor/rollback/lease cleanup retains its independent old routes.
    """
    parent = _lifecycle_value_context.get()
    if (_lc_ro1127_context.get() is not None or _lifecycle_scope.get() is not None
            or type(parent) is not _LifecycleValueOwner
            or getattr(entry, '_lifecycle_value_owner', None) is not parent
            or world._scoped_entry_budget() is not entry
            or world._scoped_attempt() is not None
            or parent.closed or parent.failed or parent.memory.failed):
        raise _LifecycleLiveLimit('read-only sibling requires the original healthy preparation Entry')
    entry.check()
    parent.readonly_proofs = True
    proof = _lc_ro1127_Proof(world, entry, parent)
    # Parent retains both successful and failed domain receipts; the paid fixed
    # carrier bounds this append. No weakrefs, resets, refunds or GC inference.
    parent.control_errors.append(proof)
    token = _lc_ro1127_context.set(proof)
    value_token = None
    try:
        proof.owner = _LifecycleValueOwner(parent.memory)
        value_token = _lifecycle_value_context.set(proof.owner)
        with source_lock(world):
            proof.capsule = _C0Selection(world, proof.root.permit, entry, _host=host)
        with _c0_segment(proof.capsule, _C0Fresh(proof.capsule)) as db:
            proof.capsule.check_source()
            _lc_adm1136_first_pin(db)
            proof.capsule.pin(db)
            # Native genuine root/permit gates, with readonly=True (no clock write).
            world._check_coordinator_execution(db, proof.root.permit.world_id,
                                               proof.root.permit, readonly=True, _first_readonly=True)
            proof.capsule.check_source()
        proof.armed = True
        yield proof
        if proof.failed:
            raise _LifecycleLiveLimit('failed read-only proof cannot return normally or be reused')
        proof.capsule.check()
        proof.capsule.check_source()
        proof.owner.move_to(parent)
    except BaseException as error:
        proof.fail(error)
        if proof.owner is not None:
            proof.owner.fail(error)
        proof.terminal = (error, BaseException.__traceback__.__get__(error),
                          BaseException.__context__.__get__(error),
                          BaseException.__cause__.__get__(error))
        raise
    finally:
        proof.ended = True
        try:
            if value_token is not None:
                _lifecycle_value_context.reset(value_token)
        finally:
            _lc_ro1127_context.reset(token)


class _LifecycleLiveLedger:
    """Explicit pre-paid ownership, not an RSS/getsizeof/GC estimator.

    This ledger prices only declared allocations. A caller may not release a
    lease while any borrowed Row/value remains in use. Native payload IO counters never debit or refund live material. Legacy
    readers retain the independent cumulative gate. Actual LC genesis plus an
    active same-ledger operation owner selects the prepaid native live route.
    """
    __slots__ = ('live', 'peak', 'leases', 'next_ticket', '_failed')
    LIMIT = 16777216
    ROW_LIMIT = 1048576

    def __init__(self):
        self.live = self.peak = 0
        self.leases = {}
        self.next_ticket = 0
        self._failed = False


    @property
    def failed(self):
        return self._failed

    @failed.setter
    def failed(self, value):
        if value is not True:
            raise _LifecycleLiveLimit('shared lifecycle poison cannot be cleared')
        _lc_ro1127_account_failure(self)
    def hold(self, amount, role):
        proof = _lc_ro1127_context.get()
        if proof is not None and proof.memory is self and proof.failed:
            self._failed = True
            raise _LifecycleLiveLimit('failed read-only proof cannot allocate again')
        if self.failed:
            raise _LifecycleLiveLimit('shared ledger has retained exceptional debt')
        if type(amount) is not int or amount < 0 or type(role) is not str:
            self._failed=True
            raise _LifecycleLiveLimit('invalid live allocation')
        # The charge includes a conservative fixed carrier/ledger-entry allowance.
        charge = amount + 512
        if charge > self.LIMIT - self.live:
            self._failed=True
            raise _LifecycleLiveLimit('live lifecycle material exceeds 16MiB')
        self.next_ticket += 1
        ticket = self.next_ticket
        self.live += charge
        self.peak = max(self.peak, self.live)
        self.leases[ticket] = charge
        try:
            return _LifecycleLiveLease(self, ticket, charge, role)
        except BaseException:
            # The failed constructor's traceback can hold its partial carrier.
            # Keep that admitted fee as permanent poison, never refund on fail.
            self._failed=True
            raise

    def release(self, lease):
        if (type(lease) is not _LifecycleLiveLease or lease.ledger is not self
                or lease.ticket not in self.leases or lease.released):
            raise _LifecycleLiveLimit('foreign or already released live allocation')
        # Poison is permanent accounting debt, not a reason to block physical
        # cursor/connection cleanup or overwrite the original exception.
        proof = lease.ro1127
        if proof is not None:
            import sys
            error = sys.exception()
            if error is not None and type(error) is not GeneratorExit:
                _lc_ro1127_account_failure(self, error)
            if proof.failed:
                return False
        if self.failed:return False
        charge = self.leases.pop(lease.ticket)
        self.live -= charge
        lease.released = True


class _LifecycleLiveLease:
    __slots__ = ('ledger', 'ticket', 'charge', 'role', 'released', 'ro1127')

    def __init__(self, ledger, ticket, charge, role):
        self.ledger, self.ticket, self.charge, self.role = ledger, ticket, charge, role
        self.released = False

        self.ro1127 = _lc_ro1127_context.get()
    def release(self):
        self.ledger.release(self)


class _LifecycleNativeLoan:
    """Caller-visible explicit native Row loan; no weakref/finalizer/refcount.

    The exact Row/list is borrowed, not replaced by a mapping lookalike. The
    consumer must clear its aliases before close. Exiting a lexical block alone
    is intentionally NOT an implicit release API.
    """
    __slots__ = ('reader', 'value', 'leases', 'closed', 'failed')

    def __init__(self, reader):
        self.reader = reader
        self.value = None
        self.leases = []
        self.closed = self.failed = False

    def close(self):
        if self.failed:
            raise _LifecycleLiveLimit('exceptional native debt cannot be refunded')
        if self.closed:
            return
        self.value = None
        # Row/list references are cleared by the producer/consumer BEFORE this
        # operation. Leases themselves contain no Row or decoded-object alias.
        for lease in reversed(self.leases):
            lease.release()
        self.leases.clear()
        self.reader = None
        self.closed = True

    def fail(self):
        self.value = None
        self.failed = True
        if self.reader is not None:
            self.reader._loan_failed = True
            self.reader.memory.failed = True


class _LifecycleValueLoan:
    """Explicit value ownership; fail preserves debt held by traceback aliases.

    close is an assertion by the consumer that *all* its aliases are gone. It
    is never called implicitly by a context manager, finalizer or GC callback.
    transfer moves the SAME loan, not the value or a re-priced allocation.
    """
    __slots__ = ('value', 'memory', 'leases', 'closed', 'failed', 'owner', '_geo1196')

    def __init__(self, memory, lease):
        self._geo1196 = None
        self.value = None
        self.memory = memory
        self.leases = [lease]
        self.closed = self.failed = False
        self.owner = None

    def close(self):
        if self.failed:
            raise _LifecycleLiveLimit('exceptional value debt cannot be refunded')
        if self.closed:
            return
        self.value = None
        for lease in reversed(self.leases):
            lease.release()
        self.leases.clear()
        self._geo1196 = None
        self.owner = None
        self.closed = True

    def fail(self):
        # Must not allocate a replacement exception or hide the active primary.
        self.value = None
        self.failed = True
        self.memory.failed = True

    def transfer(self, owner):
        if (self.closed or self.failed or type(owner) is not _LifecycleValueOwner
                or owner.memory is not self.memory or owner.closed or owner.failed):
            raise _LifecycleLiveLimit('invalid value-loan ownership transfer')
        previous = self.owner
        if previous is owner:
            return self
        if previous is not None:
            previous.loans.remove(self)
        owner.loans.append(self)
        self.owner = owner
        return self


class _LifecycleValueOwner:
    """Paid controlled domain; external handoff is not physical destruction.

    Same-Entry scopes share this owner/ledger. Detached proof results may move
    alone only after the producing frame has returned; otherwise the complete
    region moves, so a builtin alias never causes a speculative refund.
    """
    __slots__ = ('memory', 'loans', 'leases', 'readers', 'closed', 'failed',
                 'error', 'traceback', 'destination', 'exports', 'control_errors',
                 'error_context', 'error_cause', 'readonly_proofs', 'ro1127',
                 'first_failure', 'rollback_cleanup')

    def __init__(self, memory):
        if type(memory) is not _LifecycleLiveLedger:
            raise _LifecycleLiveLimit('foreign lifecycle allocation ledger')
        self.memory = memory
        self.loans, self.readers, self.exports, self.control_errors = [], [], [], []
        self.leases = [memory.hold(1024, 'value-owner')]
        self.closed = self.failed = False
        self.error = self.traceback = self.destination = None

        self.error_context = self.error_cause = None
        self.first_failure = self.rollback_cleanup = None
        self.ro1127 = _lc_ro1127_context.get()
        previous = _lifecycle_value_context.get()
        self.readonly_proofs = (self.ro1127 is not None or previous is not None
            and previous.memory is memory and previous.readonly_proofs)
    def take(self, loan):
        if (self.closed or self.failed or self.memory.failed
                or self.ro1127 is not None and self.ro1127.failed):
            raise _LifecycleLiveLimit('closed lifecycle allocation owner')
        if type(loan) is _LifecycleValueLoan:
            loan.transfer(self)
        elif type(loan) is _LifecycleNativeLoan:
            if loan.reader.memory is not self.memory or loan.closed or loan.failed:
                raise _LifecycleLiveLimit('foreign native allocation owner')
            if not any(loan is old for old in self.loans):
                self.loans.append(loan)
        else:
            raise _LifecycleLiveLimit('foreign allocation loan')
        return loan.value

    def reserve(self, amount, role):
        if (self.closed or self.failed or self.memory.failed
                or self.ro1127 is not None and self.ro1127.failed):
            raise _LifecycleLiveLimit('closed lifecycle allocation owner')
        try:
            self.leases.append(self.memory.hold(amount, role))
        except BaseException as error:
            self.fail(error)
            raise

    def keep_reader(self, reader):
        if (self.closed or self.failed or self.memory.failed
                or reader.memory is not self.memory):
            raise _LifecycleLiveLimit('foreign lifecycle reader owner')
        self.readers.append(reader)
        return reader

    def move_to(self, owner):
        if (type(owner) is not _LifecycleValueOwner or owner is self
                or owner.memory is not self.memory or owner.closed or owner.failed
                or self.closed or self.failed or self.memory.failed):
            raise _LifecycleLiveLimit('invalid lifecycle region transfer')
        # List capacities are bounded by the live lease charges already held.
        for loan in self.loans:
            owner.loans.append(loan)
            if type(loan) is _LifecycleValueLoan:
                loan.owner = owner
        owner.leases.extend(self.leases)
        owner.readers.extend(self.readers)
        owner.exports.extend(self.exports)
        owner.control_errors.extend(self.control_errors)
        self.loans = []; self.leases = []; self.readers = []; self.exports = []; self.control_errors = []
        self.destination = owner
        self.closed = True

    def handoff(self):
        if self.closed or self.failed or self.memory.failed:
            raise _LifecycleLiveLimit('invalid lifecycle external handoff')
        # No close/release/zero assertion: the external caller may retain any
        # builtin result. Its heap lifetime is outside this controlled domain.
        # Entry retains this closed receipt and cannot acquire another allowance.
        self.destination = 'external-caller; physical-reclamation-unknown'
        self.closed = True

    def close(self):
        if self.failed or self.memory.failed or self.control_errors:
            raise _LifecycleLiveLimit('exceptional region debt cannot be refunded')
        if self.closed:
            return
        for loan in reversed(self.loans):
            loan.close()
        self.loans.clear()
        for reader in reversed(self.readers):
            reader.close()
        self.readers.clear()
        for lease in reversed(self.leases):
            lease.release()
        self.leases.clear()
        self.closed = True
        self.destination = 'local-frame-ended; no-escaping-alias'

    def _remember_failure(self, first, rollback_cleanup=None):
        # Fixed owner slots only: no cleanup callback, traversal or refund.
        # Can therefore latch before the connection's independent teardown.
        self.failed = True
        self.memory.failed = True
        if self.error is None:
            self.first_failure = first
            self.error, self.traceback, self.error_context, self.error_cause = first
            self.rollback_cleanup = rollback_cleanup

    def fail(self, error=None, *, first=None, rollback_cleanup=None):
        if error is None:
            import sys
            error = sys.exception()
        self.failed = True
        self.memory.failed = True
        if error is not None and self.error is None:
            if first is not None:
                self._remember_failure(first, rollback_cleanup)
            else:
                self.error = error
                self.traceback = BaseException.__traceback__.__get__(error)
                self.error_context = BaseException.__context__.__get__(error)
                self.error_cause = BaseException.__cause__.__get__(error)
        for loan in self.loans:
            loan.fail()
        for reader in self.readers:
            reader.fail()


_lifecycle_value_context = ContextVar('lifecycle_value_owner', default=None)


def _release_pristine_geography(loan):
    # This is only the loan returned by the ended, successful snapshot frame.
    # It was registered after the restore prefix and no other value loan is
    # registered during its decode or military check. Never search the owner
    # or disturb another allocation if the original live tail is not present.
    owner = _lifecycle_value_context.get()
    if (owner is None or owner is not loan.owner or owner.memory is not loan.memory
            or owner.closed or owner.failed or owner.memory.failed
            or loan.closed or loan.failed or not owner.loans or owner.loans[-1] is not loan):
        return
    loan.close()
    owner.loans.pop()


@contextmanager
def _lifecycle_operation_scope(*, entry=None, memory=None, mutation=False):
    """One actual operation/Entry, never a per-stage 16MiB renewal."""
    parent = _lifecycle_value_context.get()
    previous = getattr(entry, '_lifecycle_value_owner', None) if entry is not None else None
    owner = parent if parent is not None else previous
    if owner is None:
        shared = _lifecycle_live_context.get()
        if memory is not None and shared is not None and memory is not shared:
            raise _LifecycleLiveLimit('foreign lifecycle operation ledger')
        owner = _LifecycleValueOwner(memory if memory is not None else
                                     shared if shared is not None else _LifecycleLiveLedger())
    if (type(owner) is not _LifecycleValueOwner or owner.closed or owner.failed
            or owner.memory.failed or memory is not None and owner.memory is not memory
            or previous is not None and previous is not owner):
        raise _LifecycleLiveLimit('Entry lifecycle ownership cannot be renewed')
    if entry is not None:
        entry._lifecycle_value_owner = owner
    value_token = _lifecycle_value_context.set(owner)
    live_token = _lifecycle_live_context.set(owner.memory)
    try:
        try:
            if mutation:
                owner.reserve(4096, 'original-mutation-exception-carrier')
            yield owner
        except BaseException as error:
            owner.fail(error)
            raise
        else:
            if parent is None:
                owner.handoff()
    finally:
        try:
            _lifecycle_live_context.reset(live_token)
        finally:
            _lifecycle_value_context.reset(value_token)


def _lifecycle_value_invoke(function, args, *, memory=None, inputs=(),
                            direct_factor=1, detached=False):
    """Call an actual body; transfer only after that body's frame has exited.

    detached is reserved for the project/check API whose result is the existing
    independently copied value loan. Other escaping values transfer the complete
    region, including direct-container reservations, rather than guessing aliases.
    """
    parent = _lifecycle_value_context.get()
    shared = _lifecycle_live_context.get()
    if memory is None:
        memory = parent.memory if parent is not None else shared
    if memory is None:
        memory = _LifecycleLiveLedger()
    if parent is not None and parent.memory is not memory:
        raise _LifecycleLiveLimit('foreign lifecycle result destination')
    try:
        owner = _LifecycleValueOwner(memory)
    except BaseException as error:
        memory.failed = True
        if parent is not None:parent.fail(error)
        raise
    value_token = _lifecycle_value_context.set(owner)
    live_token = _lifecycle_live_context.set(memory)
    result = None
    try:
        for value in inputs:
            charge, wire, nodes = _lifecycle_value_price(value, memory)
            owner.reserve(charge * direct_factor + 4096, 'root-input-and-direct-containers')
        result = function(*args)
        if result is None:
            owner.close()
        elif detached:
            selected = None
            for loan in owner.loans:
                if type(loan) is _LifecycleValueLoan and loan.value is result:
                    selected = loan
                    break
            if selected is None:
                raise _LifecycleLiveLimit('detached result has no prepaid value loan')
            if parent is None:
                owner.loans.remove(selected)
                selected.owner = None
                owner.exports.append(selected)
            else:
                selected.transfer(parent)
            owner.close()
            if parent is None:
                owner.destination = 'external-prepaid-result; physical-reclamation-unknown'
        elif parent is not None:
            owner.move_to(parent)
        else:
            owner.handoff()
        return result
    except BaseException as error:
        owner.fail(error)
        if parent is not None:
            first = owner.first_failure
            if first is not None and first[0] is error:
                parent.fail(error, first=first, rollback_cleanup=owner.rollback_cleanup)
            else:
                parent.fail(error)
        raise
    finally:
        try:
            _lifecycle_live_context.reset(live_token)
        finally:
            _lifecycle_value_context.reset(value_token)


def _lifecycle_utf8_size(text):
    if type(text) is not str:
        raise TypeError('lifecycle JSON text must be an exact str')
    count = 0
    for char in text:
        point = ord(char)
        if 0xD800 <= point <= 0xDFFF:
            raise UnicodeError('unpaired surrogate in lifecycle JSON')
        count += 1 if point < 128 else 2 if point < 2048 else 3 if point < 65536 else 4
    return count


class _LifecycleJSONScan:
    """Grammar/UTF8/allocation preflight; never builds the decoded graph.

    Each recursive frame is prepaid before creation. String/key scanning keeps
    only integer offsets for values. Per-object decoded keys are separately
    prepaid and duplicate errors occur at the original completed-object hook. The scanner's price is a declared
    allocation model, not a whole-interpreter heap/RSS claim.
    """
    __slots__ = ('raw', 'memory', 'offset', 'length', 'charge', 'wire', 'nodes', 'strict')

    def __init__(self, raw, memory, strict=True):
        self.raw, self.memory = raw, memory
        self.strict = strict
        self.offset, self.length = 0, len(raw)
        self.charge = self.wire = self.nodes = 0

    def space(self):
        while self.offset < self.length and self.raw[self.offset] in ' \t\r\n':
            self.offset += 1

    def string(self):
        self.offset += 1
        points = 0
        while self.offset < self.length:
            char = self.raw[self.offset]
            self.offset += 1
            if char == '"':
                self.charge += 80 + 4 * points
                self.wire += 2 + 6 * points
                self.nodes += 1
                return
            if ord(char) < 32:
                raise ValueError('control character in lifecycle JSON')
            if char == '\\':
                if self.offset == self.length:
                    raise ValueError('unterminated lifecycle JSON escape')
                char = self.raw[self.offset]
                self.offset += 1
                if char == 'u':
                    point = self.hex4()
                    # Surrogates are accepted by stdlib's string parser.
                    # Canonical UTF8 validation happens after all parser hooks.
                elif char not in '"\\/bfnrt':
                    raise ValueError('invalid lifecycle JSON escape')
            points += 1
        raise ValueError('unterminated lifecycle JSON string')

    def hex4(self):
        if self.offset + 4 > self.length:
            raise ValueError('short lifecycle JSON Unicode escape')
        result = 0
        for unused in range(4):
            char = self.raw[self.offset]
            self.offset += 1
            digit = '0123456789abcdef'.find(char.lower())
            if digit < 0:
                raise ValueError('invalid lifecycle JSON Unicode escape')
            result = result * 16 + digit
        return result

    def value(self):
        # Reserve before descending, not after an arbitrarily deep recursion.
        frame = self.memory.hold(2048, 'json-preflight-frame')
        try:
            self.space()
            if self.offset >= self.length:
                raise ValueError('missing lifecycle JSON value')
            char = self.raw[self.offset]
            if char == '"':
                self.string()
            elif char in '[{':
                mapping = char == '{'
                end = '}' if mapping else ']'
                self.offset += 1
                self.charge += 256 if mapping else 80
                self.nodes += 1
                self.wire += 2
                keys = {} if mapping else None
                key_leases=[]
                duplicate=False
                self.space()
                if self.offset < self.length and self.raw[self.offset] == end:
                    self.offset += 1
                else:
                    while True:
                        self.space()
                        if mapping:
                            if self.offset == self.length or self.raw[self.offset] != '"':
                                raise ValueError('lifecycle JSON object key is not a string')
                            key_start=self.offset
                            self.string()
                            key_lease=self.memory.hold(8*(self.offset-key_start)+512,'json-preflight-key')
                            key_leases.append(key_lease)
                            key=json.loads(self.raw[key_start:self.offset])
                            duplicate = duplicate or key in keys
                            keys[key]=None
                            key=None
                            self.space()
                            if self.offset == self.length or self.raw[self.offset] != ':':
                                raise ValueError('missing lifecycle JSON colon')
                            self.offset += 1
                            self.wire += 1
                        self.value()
                        self.charge += 128 if mapping else 32
                        self.space()
                        if self.offset == self.length:
                            raise ValueError('unterminated lifecycle JSON container')
                        char = self.raw[self.offset]
                        self.offset += 1
                        if char == end:
                            if self.strict and mapping and duplicate:raise ValueError('duplicate lifecycle JSON key')
                            break
                        if char != ',':
                            raise ValueError('missing lifecycle JSON comma')
                        self.wire += 1
                keys=None
                for key_lease in key_leases:key_lease.release()
                key_leases=None
            elif any(self.raw.startswith(literal,self.offset) for literal in ('NaN','Infinity','-Infinity')):
                if self.strict:raise ValueError('non-finite lifecycle JSON constant')
                for literal in ('NaN','Infinity','-Infinity'):
                    if self.raw.startswith(literal,self.offset):
                        self.offset+=len(literal);break
                self.charge+=64;self.wire+=32;self.nodes+=1
            elif char in '-0123456789':
                start = self.offset
                if char == '-':
                    self.offset += 1
                if self.offset == self.length:
                    raise ValueError('incomplete lifecycle JSON number')
                if self.raw[self.offset] == '0':
                    self.offset += 1
                elif self.raw[self.offset] in '123456789':
                    while self.offset < self.length and self.raw[self.offset] in '0123456789':
                        self.offset += 1
                else:
                    raise ValueError('invalid lifecycle JSON number')
                for markers in ('.', 'eE'):
                    if self.offset < self.length and self.raw[self.offset] in markers:
                        self.offset += 1
                        if markers == 'eE' and self.offset < self.length and self.raw[self.offset] in '+-':
                            self.offset += 1
                        digit_start = self.offset
                        while self.offset < self.length and self.raw[self.offset] in '0123456789':
                            self.offset += 1
                        if self.offset == digit_start:
                            raise ValueError('incomplete lifecycle JSON number')
                width = self.offset - start
                numeric_scratch=self.memory.hold(8*width+1024,'json-preflight-number')
                token=self.raw[start:self.offset]
                number=float(token) if any(c in token for c in '.eE') else int(token)
                number=token=None
                numeric_scratch.release()
                self.charge += 64 + width
                self.wire += max(32, width)
                self.nodes += 1
            else:
                for literal in ('true', 'false', 'null'):
                    if self.raw.startswith(literal, self.offset):
                        self.offset += len(literal)
                        self.charge += 16
                        self.wire += len(literal)
                        self.nodes += 1
                        break
                else:
                    raise ValueError('invalid lifecycle JSON token')
        except BaseException:
            self.memory.failed = True
            raise
        else:
            frame.release()


def _lifecycle_json_pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError('duplicate lifecycle JSON key')
        result[key] = value
    return result


def _lifecycle_json_constant(value):
    raise ValueError('non-finite lifecycle JSON constant')


def _lifecycle_json_borrow(raw, *, memory, role):
    """Preflight grammar/UTF8, then prepay graph+decoder+canonical scratch."""
    if type(memory) is not _LifecycleLiveLedger or type(role) is not str:
        raise _LifecycleLiveLimit('foreign lifecycle JSON allocation')
    carrier = memory.hold(4096, role + ':scan')
    loan = None
    try:
        if _lifecycle_utf8_size(raw)>1048576:raise _LifecycleLiveLimit('lifecycle JSON input exceeds 1MiB')
        scan = _LifecycleJSONScan(raw, memory)
        scan.value()
        scan.space()
        if scan.offset != scan.length:
            raise ValueError('extra lifecycle JSON data')
        loan = _LifecycleValueLoan(memory, memory.hold(scan.charge + 256, role + ':value'))
        scratch = memory.hold(4096 + 256 * scan.nodes + 16 * scan.wire, role + ':decode-canonical')
        value = json.loads(raw, object_pairs_hook=_lifecycle_json_pairs,
                           parse_constant=_lifecycle_json_constant)
        canonical = _json(value)
        encoded = canonical.encode('utf-8')
        # No allocation has been allowed to precede its price reservation.
        encoded = canonical = None
        loan.value = value
        value = raw = scan = None
    except BaseException:
        memory.failed = True
        if loan is not None:
            loan.fail()
        raise
    scratch.release()
    carrier.release()
    return loan


def _lifecycle_value_price(value, memory, ancestors=None):
    """Price an exact builtin JSON graph without copy/serialization first.

    Path-only strong ancestors reject cycles but allow ordinary shared acyclic
    inputs. Every occurrence is charged because JSON copying duplicates it.
    """
    frame = memory.hold(2048, 'value-price-frame')
    try:
        kind = type(value)
        if kind is str:
            _lifecycle_utf8_size(value)
            result = (80 + 4 * len(value), 2 + 6 * len(value), 1)
        elif value is None or kind is bool:
            result = (16, 5, 1)
        elif kind is int:
            width = max(1, (value.bit_length() * 30103) // 100000 + 2)
            result = (64 + width, width, 1)
        elif kind is float:
            if value != value or value in (float('inf'), float('-inf')):
                raise ValueError('non-finite lifecycle JSON value')
            result = (64, 32, 1)
        elif kind is list or kind is dict:
            parent = ancestors
            while parent is not None:
                if parent[0] is value:
                    raise ValueError('cyclic lifecycle JSON value')
                parent = parent[1]
            ancestry = (value, ancestors)
            charge, wire, nodes = (256 if kind is dict else 80), 2, 1
            if kind is dict:
                for key, item in value.items():
                    if type(key) is not str:
                        raise TypeError('lifecycle JSON keys must be exact strings')
                    kc, kw, kn = _lifecycle_value_price(key, memory, ancestry)
                    vc, vw, vn = _lifecycle_value_price(item, memory, ancestry)
                    charge += kc + vc + 128
                    wire += kw + vw + 2
                    nodes += kn + vn
            else:
                for item in value:
                    vc, vw, vn = _lifecycle_value_price(item, memory, ancestry)
                    charge += vc + 32
                    wire += vw + 1
                    nodes += vn
            result = (charge, wire, nodes)
        else:
            raise TypeError('lifecycle JSON requires exact builtin values')
    except BaseException:
        memory.failed = True
        raise
    else:
        frame.release()
        return result


def _lifecycle_copy_borrow(value, *, memory, role):
    if type(memory) is not _LifecycleLiveLedger or type(role) is not str:
        raise _LifecycleLiveLimit('foreign lifecycle copy allocation')
    carrier = memory.hold(4096, role + ':scan')
    loan = None
    try:
        charge, wire, nodes = _lifecycle_value_price(value, memory)
        loan = _LifecycleValueLoan(memory, memory.hold(charge + 256, role + ':value'))
        scratch = memory.hold(4096 + 256 * nodes + 16 * wire, role + ':copy-canonical')
        canonical = _json(value)
        loan.value = json.loads(canonical)
        canonical = value = None
    except BaseException:
        memory.failed = True
        if loan is not None:
            loan.fail()
        raise
    scratch.release()
    carrier.release()
    return loan


def _lifecycle_hash_owned(value, *, memory):
    """Only the fixed digest escapes; all canonical/UTF8 scratch is prepaid.

    The digest is charged to the active lexical value owner when present;
    otherwise its fixed receipt lease stays on the supplied ledger. A failed
    helper poisons that same shared ledger and never refunds traceback debt.
    """
    if type(memory) is not _LifecycleLiveLedger:
        raise _LifecycleLiveLimit('foreign lifecycle hash allocation')
    carrier = memory.hold(4096, 'hash-scan')
    try:
        charge, wire, nodes = _lifecycle_value_price(value, memory)
        scratch = memory.hold(4096 + 256 * nodes + 16 * wire, 'hash-canonical-utf8')
        digest_lease = memory.hold(256, 'hash-digest')
        canonical = _json(value)
        encoded = canonical.encode('utf-8')
        digest = hashlib.sha256(encoded).hexdigest()
        encoded = canonical = value = None
        owner = _lifecycle_value_context.get()
        if owner is not None:
            if owner.memory is not memory or owner.closed or owner.failed:
                raise _LifecycleLiveLimit('foreign hash-result allocation owner')
            owner.leases.append(digest_lease)
    except BaseException:
        memory.failed = True
        raise
    scratch.release()
    carrier.release()
    return digest


def _lifecycle_canonical_borrow(value, *, memory, role):
    """Internal canonical string loan for original byte-sensitive comparisons."""
    if type(memory) is not _LifecycleLiveLedger or type(role) is not str:
        raise _LifecycleLiveLimit('foreign lifecycle canonical allocation')
    carrier = memory.hold(4096, role + ':scan')
    loan = None
    try:
        charge, wire, nodes = _lifecycle_value_price(value, memory)
        loan = _LifecycleValueLoan(memory, memory.hold(4 * wire + 256, role + ':text'))
        scratch = memory.hold(4096 + 256 * nodes + 16 * wire, role + ':canonical')
        loan.value = _json(value)
        value = None
    except BaseException:
        memory.failed = True
        if loan is not None:loan.fail()
        raise
    scratch.release()
    carrier.release()
    return loan


def _lifecycle_raw_charge(size, fields):
    """Declared raw Row domain: Unicode payload + Row/tuple/list slot overhead.

    Four bytes per input UTF-8 byte bounds native Unicode scalar storage; eight
    per selected field bounds slots. 256 bytes/field and 1024/row are declared
    CPython-object allowances, NOT a portable or whole-process RSS theorem.
    Decoded JSON, canonical/hash scratch and outer results are separate debts.
    """
    if (type(size) is not int or not 0 <= size <= 1048576
            or type(fields) is not int or fields < 0):
        raise _LifecycleLiveLimit('native row exceeds 1MiB or has invalid width')
    return 4 * size + 264 * fields + 1024


_lifecycle_live_context = ContextVar('lifecycle_live_material', default=None)


class _LifecycleNativeReader:
    """Uses the caller-owned connection; cold ATTACH dies with that owner.

    No new connection, Host, lease, schema, migration or private writer. Fresh
    pre-registration absence is represented as absence, never a missing-row proof.
    """
    def __init__(self, db):
        try:
            self._initialize_owned(db)
        except BaseException as error:
            memory = getattr(self, 'memory', None)
            source_owner = getattr(self, '_source_owner', None)
            if source_owner is not None:
                source_owner.fail(error)
            if memory is not None:
                self._loan_failed = True
                memory.failed = True
                for loan in getattr(self, '_retained_loans', ()):
                    loan.fail()
            raise

    def _initialize_owned(self, db):
        from .world import WorldStore, _AttemptConnection
        from .actor_execution import EntryBudget, ExecutionPolicy, PrivateStore
        from .coordinator_execution import CoordinatorAttempt
        from .coordinator_maintenance import recover
        from .actors import ActorHost
        from .server import Coordinator
        if type(db) not in (sqlite3.Connection, _AttemptConnection) or not db.in_transaction:
            unproven()
        if db.row_factory is not None and db.row_factory is not sqlite3.Row:unproven()
        self.db = db
        self.budget = getattr(db, '_attempt', None) or _lc_adm1136_budget_context.get()
        if self.budget is None:
            # Cold initialize has no Host/root. This is a finite read policy only.
            self.budget = EntryBudget(ExecutionPolicy(), threading.Event())
        self.budget.check()
        self.source_root = Path(__file__).resolve().parent.parent
        self.definitions = (WorldStore, PrivateStore, ActorHost, CoordinatorAttempt, recover, Coordinator, ActivationRef)
        self.memory = _lifecycle_live_context.get()
        if self.memory is None:self.memory = _LifecycleLiveLedger()
        self._loan_failed = False
        self._live_admission = False
        self._initializing = True
        self._retained_loans = []
        self._reader_lease = self.memory.hold(4096,'native-reader-carrier')
        self._source_owner = _lifecycle_value_context.get()
        self._owns_source_owner = self._source_owner is None
        if self._source_owner is None:
            self._source_owner = _LifecycleValueOwner(self.memory)
        elif self._source_owner.memory is not self.memory:
            raise _LifecycleLiveLimit('native reader/source owner has a foreign ledger')
        self.generation = self._attest_owned()
        self.main_path = self.main_file = self.actor_path = self.actor_file = None
        self.identity = self.world_id = None
        self.total_bytes = 0
        self.io_bytes = self.io_rows = 0

        self.present = set()
        self._paths(initial=True)
        first = self._one('SELECT state FROM main.snapshots ORDER BY seq LIMIT 1')
        if first is None:
            unproven()
        genesis = self._initial_json(first[0]);self.world_id = genesis['world_id']
        registered = exists(db, 'coordinator_scheduler_registration')
        if registered:
            rows = self._all('SELECT * FROM coordinator_scheduler_registration')
            if len(rows) != 1 or rows[0]['world_id'] != self.world_id:
                unproven()
            identity = self._initial_json(rows[0]['private_identity_json'])
            if (type(identity) is not list or len(identity) != 3 or type(identity[0]) is not str
                    or any(type(v) is not int or v < 0 for v in identity[1:])):
                unproven()
            path = Path(identity[0])
            if not path.is_absolute() or str(path.resolve()) != identity[0] or path.resolve() == self.main_path:
                unproven()
            self.actor_path = path.resolve();self.actor_file = tuple(identity[1:]);self.identity = identity
            self.check_source()
            attached = {row[1]: row[2] for row in self._all('PRAGMA database_list')}
            if 'actor' not in attached:
                cursor = db.execute('ATTACH DATABASE ? AS actor', (self.actor_path.as_uri()+'?mode=ro',))
                cursor.close()
            # Existing actor is never detached, replaced or granted write access.
            self._paths()
            scratch = self._helper_scratch(True)
            try:
                registration(db,self.world_id,identity)
                from .world import _history_witness
                _history_witness(db,'main');_history_witness(db,'actor')
            except BaseException:
                self.memory.failed=True
                raise
            scratch.release()
        elif exists(db,'coordinator_windows') or exists(db,'coordinator_activations'):
            unproven()  # Orphan scheduler material is not a pristine genesis.
        for table, (columns, filters, orders) in _LIFECYCLE_TABLES.items():
            schema_name, name = table.split('.')
            if schema_name=='actor' and not registered:
                continue  # No registration: actor material cannot prove anything.
            attached = {row[1] for row in self._all('PRAGMA database_list')}
            present = schema_name in attached and exists(db, name, schema_name)
            if not present:
                if registered and table in ('main.snapshots','main.coordinator_windows','main.coordinator_activations',
                                             'actor.actor_intents','actor.actor_calls'):
                    unproven()
                continue
            actual = [row[1] for row in self._all('PRAGMA '+schema_name+'.table_info('+name+')')]
            if actual != columns.split():
                unproven()
            self.present.add(table)
        from .world import _lc_optin
        owner=_lifecycle_value_context.get()
        self._live_admission = (_lc_optin(genesis) and owner is not None and owner.memory is self.memory and not owner.closed and not owner.failed)
        self.check_source()
        self._initializing = False

    def _all(self, sql, params=()):
        """Constructor/source metadata reads, priced before any payload fetch.

        PRAGMA forms are finite original callers. Table-valued PRAGMA metadata
        is used only for pricing; payload still executes the original SQL and
        returns its genuine original Row values, column order and errors.
        """
        self.budget.check()
        if self._loan_failed or self.memory.failed:
            raise _LifecycleLiveLimit('reader has retained exceptional loan debt')
        carrier = self.memory.hold(4096, 'native-initial-query')
        loan = _LifecycleNativeLoan(self)
        loan.leases.append(carrier)
        previous = self.db.row_factory
        cursor = metadata = description = None
        rows = []
        try:
            if previous is not None and previous is not sqlite3.Row:unproven()
            self.db.row_factory = sqlite3.Row
            pricing_sql, pricing_params = sql, params
            if sql == 'PRAGMA database_list':
                pricing_sql = 'SELECT seq,name,file FROM pragma_database_list'
            elif sql.startswith('PRAGMA ') and '.table_info(' in sql and sql.endswith(')'):
                schema, name = sql[7:-1].split('.table_info(', 1)
                if schema not in ('main', 'actor') or not name.replace('_', '').isalnum():unproven()
                pricing_sql = 'SELECT cid,name,type,"notnull",dflt_value,pk FROM pragma_table_info(?,?)'
                pricing_params = (name, schema)
            elif not sql.startswith('SELECT '):
                unproven()
            # execute/description allocates no payload Rows. Its carrier was
            # prepaid above; a zero-row projection recovers exact field names.
            description = self.db.execute('SELECT * FROM (' + pricing_sql + ') LIMIT 0', pricing_params)
            names = tuple(item[0] for item in description.description)
            description.close()
            description = None
            if not names or any('"' in name for name in names):unproven()
            sizes = '+'.join('CASE typeof("'+name+'") WHEN \'text\' THEN length(CAST("'+name+'" AS BLOB)) '
                             'WHEN \'blob\' THEN length("'+name+'") ELSE 8 END' for name in names)
            metadata = self.db.execute('SELECT '+sizes+' FROM ('+pricing_sql+')', pricing_params)
            while True:
                self.budget.check()
                size_row = metadata.fetchone()
                if size_row is None:
                    break
                size = size_row[0]
                lease = self.memory.hold(_lifecycle_raw_charge(size, len(names)), 'native-initial-row')
                loan.leases.append(lease)
                if cursor is None:
                    cursor = self.db.execute(sql, params)
                row = cursor.fetchone()
                if type(row) is not sqlite3.Row or tuple(row.keys()) != names:unproven()
                self.io_rows += 1
                self.io_bytes += size
                rows.append(row)
            self.budget.check()
            loan.value = rows
            owner = _lifecycle_value_context.get()
            if owner is not None and not self._initializing:
                return owner.take(loan)
            self._retained_loans.append(loan)
            return rows
        except BaseException:
            loan.fail()
            raise
        finally:
            try:
                if cursor is not None:cursor.close()
            finally:
                try:
                    if metadata is not None:metadata.close()
                finally:
                    try:
                        if description is not None:description.close()
                    finally:
                        self.db.row_factory = previous

    def _one(self, sql, params=()):
        rows = self._all(sql, params)
        return rows[0] if rows else None

    def _paths(self, initial=False):
        if initial:
            # Initial main_path escapes on self, so its source Row loan belongs
            # to the reader, not a temporary source-check allocation region.
            return self._paths_owned(initial=True)
        owner = _LifecycleValueOwner(self.memory)
        token = _lifecycle_value_context.set(owner)
        try:
            self._paths_owned(initial=False)
        except BaseException:
            owner.fail()
            raise
        finally:
            _lifecycle_value_context.reset(token)
        owner.close()

    def _paths_owned(self, initial=False):
        self.budget.check()
        rows = self._all('PRAGMA database_list')
        amount=32768+sum(32*len(row[2]) for row in rows)
        owner=_lifecycle_value_context.get()
        if owner is not None and not self._initializing:owner.reserve(amount,'native-path-scratch')
        else:self._retained_loans.append(_LifecycleValueLoan(self.memory,self.memory.hold(amount,'native-path-scratch')))
        paths = {row[1]: row[2] for row in rows}
        if not paths.get('main'):
            unproven()
        main = Path(paths['main']).resolve()
        stat = main.stat();identity = (stat.st_dev, stat.st_ino)
        if initial:
            self.main_path, self.main_file = main, identity
        elif main != self.main_path or identity != self.main_file:
            unproven()
        if self.actor_path is not None:
            stat = self.actor_path.stat()
            if (not self.actor_path.is_file() or (stat.st_dev, stat.st_ino) != self.actor_file
                    or self.actor_path == main or self.actor_file == self.main_file):
                unproven()
            if 'actor' in paths and Path(paths['actor']).resolve() != self.actor_path:
                unproven()
        elif not initial and 'actor' in paths and self.identity is not None:
            unproven()
        self.budget.check()

    def check_source(self):
        self.budget.check();self._paths();_lifecycle_world_primitives()
        if self._attest_owned() != self.generation:
            unproven()
        self.budget.check()

    def __call__(self, table, equality=(), columns=None, order=(), limit=None, first=False):
        # Compatibility reads are real loans too. A lexical consumer owner is
        # preferred; otherwise the reader owns them until explicit close().
        loan = self.borrow(table, equality, columns, order, limit, first)
        owner = _lifecycle_value_context.get()
        if owner is not None:
            return owner.take(loan)
        self._retained_loans.append(loan)
        return loan.value

    def borrow(self, table, equality=(), columns=None, order=(), limit=None, first=False):
        """Explicit raw-loan API; legacy cumulative gate and V3 live admission.

        Successful callers must drop ALL Row/value aliases before loan.close().
        Exceptional paths poison this reader and retain charges, since a saved
        traceback can still own raw values. No GC-based refund is performed.
        """
        if self._loan_failed or self.memory.failed:
            raise _LifecycleLiveLimit('reader has retained exceptional loan debt')
        if self._live_admission:
            owner=_lifecycle_value_context.get()
            if owner is None or owner.memory is not self.memory or owner.closed or owner.failed:
                raise _LifecycleLiveLimit('live native acquisition lost its operation owner')
        carrier = self.memory.hold(4096, 'native-query-carrier')
        loan = None
        try:
            loan = _LifecycleNativeLoan(self)
            loan.leases.append(carrier)
            return self._borrow_rows(loan,table,equality,columns,order,limit,first)
        except BaseException:
            self._loan_failed = True
            self.memory.failed = True
            if loan is not None:loan.fail()
            raise

    def _borrow_rows(self, loan, table, equality=(), columns=None, order=(), limit=None, first=False):
        if self._loan_failed:raise _LifecycleLiveLimit('reader has retained exceptional loan debt')
        self.check_source()
        if (table not in _LIFECYCLE_TABLES or type(equality) is not tuple or type(order) is not tuple
                or type(first) is not bool or (limit is not None and (type(limit) is not int or limit != 1))):
            unproven()
        names, filters, orders = _LIFECYCLE_TABLES[table]
        for pair in equality:
            if (type(pair) is not tuple or len(pair) != 2 or pair[0] not in filters.split()
                    or type(pair[1]) is not (int if pair[0] in ('seq','version','advanced_revision') else str)):
                unproven()
        if len({k for k,v in equality}) != len(equality):
            unproven()
        for pair in order:
            if type(pair) is not tuple or len(pair) != 2 or pair[0] not in orders.split() or type(pair[1]) is not bool:
                unproven()
        if columns not in (None, ('state',)) or (columns is not None and table != 'main.snapshots'):
            unproven()
        if table not in self.present:
            self.check_source()
            loan.value = None if first else []
            return loan
        sql = 'SELECT '+('*' if columns is None else ','.join(columns))+' FROM '+table
        if equality:
            sql += ' WHERE '+' AND '.join(k+'=?' for k,v in equality)
        if order:
            sql += ' ORDER BY '+','.join(k+(' DESC' if desc else '') for k,desc in order)
        if limit is not None:
            sql += ' LIMIT 1'
        params = tuple(v for k,v in equality)
        # Price raw payload before fetching it, under this same read snapshot.
        selected = names.split() if columns is None else columns
        sizes = '+'.join("CASE typeof("+k+") WHEN 'text' THEN length(CAST("+k+" AS BLOB)) WHEN 'blob' THEN length("+k+") ELSE 8 END" for k in selected)
        cursor = metadata = None;previous = self.db.row_factory;rows = []
        if previous is not None and previous is not sqlite3.Row:unproven()
        try:
            self.db.row_factory = sqlite3.Row
            metadata = self.db.execute('SELECT '+sizes+' FROM ('+sql+')', params)
            while True:
                self.budget.check()
                size = metadata.fetchone()
                if size is None:
                    break
                size = size[0]
                if (type(size) is not int or not 0 <= size <= 1048576
                        or (not self._live_admission and self.total_bytes+size > 16777216)):
                    unproven()
                lease = self.memory.hold(_lifecycle_raw_charge(size,len(selected)), 'native-row')
                loan.leases.append(lease)
                if cursor is None:
                    cursor = self.db.execute(sql, params)
                row = cursor.fetchone()
                if row is not None:
                    self.io_bytes += size
                    self.io_rows += 1
                if type(row) is not sqlite3.Row or tuple(row.keys()) != tuple(selected):
                    unproven()
                _lifecycle_scalar_row(table,row,columns)
                rows.append(row);self.total_bytes += size
                if first:
                    break
        finally:
            try:
                if cursor is not None:cursor.close()
            finally:
                try:
                    if metadata is not None:metadata.close()
                finally:
                    self.db.row_factory = previous
        self.check_source()
        loan.value = (rows[0] if rows else None) if first else rows
        return loan

    def _initial_json(self, raw):
        loan = _lifecycle_json_borrow(raw, memory=self.memory, role='native-initial-json')
        self._retained_loans.append(loan)
        return loan.value

    def close(self):
        """Caller asserts every implicit __call__/initial Row alias is dead."""
        if self._reader_lease.released:return
        if self._loan_failed or self.memory.failed:
            raise _LifecycleLiveLimit('exceptional reader debt cannot be refunded')
        self.identity = self.world_id = None
        self.main_path = self.actor_path = None
        for loan in reversed(self._retained_loans):
            loan.close()
        self._retained_loans.clear()
        self.generation = None
        if self._owns_source_owner:
            self._source_owner.close()
        self._reader_lease.release()

    def fail(self):
        self._loan_failed = True
        self.memory.failed = True
        if getattr(self, '_owns_source_owner', False):
            self._source_owner.fail()
        for loan in self._retained_loans:
            loan.fail()

    def iter_borrows(self, table, equality=(), columns=None, order=()):
        """Original validated SELECT, streamed as one prepaid genuine Row loan."""
        self.check_source()
        if (table not in _LIFECYCLE_TABLES or type(equality) is not tuple or type(order) is not tuple):unproven()
        names, filters, orders = _LIFECYCLE_TABLES[table]
        for pair in equality:
            if (type(pair) is not tuple or len(pair)!=2 or pair[0] not in filters.split()
                    or type(pair[1]) is not (int if pair[0] in ('seq','version','advanced_revision') else str)):unproven()
        if len({k for k,v in equality})!=len(equality):unproven()
        for pair in order:
            if type(pair) is not tuple or len(pair)!=2 or pair[0] not in orders.split() or type(pair[1]) is not bool:unproven()
        if columns not in (None,('state',)) or (columns is not None and table!='main.snapshots'):unproven()
        if table not in self.present:
            self.check_source()
            return (loan for loan in ())
        sql='SELECT '+('*' if columns is None else ','.join(columns))+' FROM '+table
        if equality:sql+=' WHERE '+' AND '.join(k+'=?' for k,v in equality)
        if order:sql+=' ORDER BY '+','.join(k+(' DESC' if desc else '') for k,desc in order)
        return self._stream_query(sql,tuple(v for k,v in equality),table,columns)

    def _stream_query(self, sql, params=(), table=None, columns=None):
        """Trusted direct caller SQL; no foreign callback or synthetic Row.

        Two same-transaction cursors, scalar metadata before each payload fetch.
        The consumer, not generator resumption, owns/ends every yielded loan.
        """
        if self._loan_failed or self.memory.failed:raise _LifecycleLiveLimit('reader has retained exceptional loan debt')
        if self._live_admission:
            owner=_lifecycle_value_context.get()
            if owner is None or owner.memory is not self.memory or owner.closed or owner.failed:
                raise _LifecycleLiveLimit('native stream lost its operation owner')
        carrier=self.memory.hold(8192,'native-stream-cursors')
        cursor=metadata=description=loan=row=None
        previous=self.db.row_factory
        try:
            self.check_source()
            if previous is not None and previous is not sqlite3.Row:unproven()
            self.db.row_factory=sqlite3.Row
            description=self.db.execute('SELECT * FROM ('+sql+') LIMIT 0',params)
            names=tuple(item[0] for item in description.description)
            description.close();description=None
            if not names or any('"' in name for name in names):unproven()
            sizes='+'.join('CASE typeof("'+name+'") WHEN \'text\' THEN length(CAST("'+name+'" AS BLOB)) WHEN \'blob\' THEN length("'+name+'") ELSE 8 END' for name in names)
            metadata=self.db.execute('SELECT '+sizes+' FROM ('+sql+')',params)
            while True:
                self.budget.check()
                size_row=metadata.fetchone()
                if size_row is None:break
                size=size_row[0];size_row=None
                if (type(size) is not int or not 0<=size<=1048576
                        or (not self._live_admission and self.total_bytes+size>16777216)):unproven()
                loan=_LifecycleNativeLoan(self)
                loan.leases.append(self.memory.hold(_lifecycle_raw_charge(size,len(names)),'native-stream-row'))
                if cursor is None:cursor=self.db.execute(sql,params)
                row=cursor.fetchone()
                if row is not None:
                    self.io_rows+=1;self.io_bytes+=size
                if type(row) is not sqlite3.Row or tuple(row.keys())!=names:unproven()
                if table is not None:_lifecycle_scalar_row(table,row,columns)
                self.total_bytes+=size
                loan.value=row;row=None
                # The original semantic read's source gates remain; each yielded
                # acquisition has an additional source gate before exposure.
                self.check_source()
                yield loan
                loan=None
            self.check_source()
        except GeneratorExit:
            # Successful early-stop callers retain or close their yielded loan.
            row=None
            raise
        except BaseException:
            self._loan_failed=True;self.memory.failed=True
            if loan is not None:loan.fail()
            raise
        finally:
            row=None
            try:
                if cursor is not None:cursor.close()
            finally:
                try:
                    if metadata is not None:metadata.close()
                finally:
                    try:
                        if description is not None:description.close()
                    finally:
                        self.db.row_factory=previous
                        if not self.memory.failed:carrier.release()

    def _attest_owned(self):
        """Price original source-reader work without changing its source9 set."""
        self.budget.check()
        active = _lifecycle_value_context.get()
        owner = self._source_owner if active is None else active
        if (owner.closed or owner.failed or owner.memory is not self.memory or self.memory.failed):
            raise _LifecycleLiveLimit('native source lost its retained reader/operation owner')
        token = _lifecycle_value_context.set(owner)
        try:
            _lc_adm1136_world_members()
            generation = getattr(self, 'generation', None)
            if generation is None:
                return _c0_source_attest(self.source_root,self.definitions,self.budget.check)
            if not _c0_source_attest(self.source_root,self.definitions,self.budget.check,expected=generation):
                unproven()
            return generation
        except BaseException as error:
            owner.fail(error)
            raise
        finally:
            _lifecycle_value_context.reset(token)

    def _helper_scratch(self, registered):
        """Prepay native registration/history helper raw, JSON and token scratch.

        Only scalar length aggregates precede the reservation. No full catalog,
        registration JSON graph or witness tuple is decoded to estimate its size.
        The unchanged original helpers still perform every schema/source check.
        """
        amount=65536
        schemas=('main','actor') if registered else ('main',)
        for schema in schemas:
            row=self._one('SELECT coalesce(sum(coalesce(length(CAST(type AS BLOB)),0)+coalesce(length(CAST(name AS BLOB)),0)+coalesce(length(CAST(tbl_name AS BLOB)),0)+coalesce(length(CAST(sql AS BLOB)),0)),0),count(*) FROM '+schema+'.sqlite_master')
            amount+=32*row[0]+8192*row[1]
            if registered:
                from .world import _HISTORY_COVER_DDL,_HISTORY_EPOCH_DDL
                for name,ddl,expression in (('history_proof_coverage',_HISTORY_COVER_DDL,'length(CAST(table_name AS BLOB))+length(CAST(table_sql AS BLOB))'),('history_proof_epochs',_HISTORY_EPOCH_DDL,'length(CAST(store_nonce AS BLOB))')):
                    shape=self._one('SELECT sql FROM '+schema+'.sqlite_master WHERE type=? AND name=?',('table',name))
                    if shape is None or shape[0]!=ddl:continue
                    row=self._one('SELECT coalesce(sum('+expression+'),0),count(*) FROM '+schema+'.'+name)
                    amount+=32*row[0]+8192*row[1]
        if registered:
            binding_names=[row[1] for row in self._all('PRAGMA actor.table_info(actor_world_binding)')]
            if binding_names==['singleton','world_id']:
                row=self._one('SELECT coalesce(sum(coalesce(length(CAST(world_id AS BLOB)),0)+8),0),count(*),coalesce(max(coalesce(length(CAST(world_id AS BLOB)),0)+8),0) FROM actor.actor_world_binding')
                if row[2]>1048576:raise _LifecycleLiveLimit('native sourcehelper binding row exceeds 1MiB')
                amount+=32*row[0]+8192*row[1]
            names=[row[1] for row in self._all('PRAGMA main.table_info(coordinator_scheduler_registration)')]
            if names==['world_id','schema_version','boundary_kind','boundary_revision','boundary_time','evidence_json','evidence_sha256','created_parent_epoch','private_identity_json']:
                row=self._one('SELECT coalesce(sum(coalesce(length(CAST(evidence_json AS BLOB)),0)+coalesce(length(CAST(private_identity_json AS BLOB)),0)),0),count(*) FROM main.coordinator_scheduler_registration')
                amount+=1024*row[0]+8192*row[1]
                row=self._one('SELECT coalesce(sum(coalesce(length(CAST(world_id AS BLOB)),0)+coalesce(length(CAST(boundary_kind AS BLOB)),0)+coalesce(length(CAST(evidence_json AS BLOB)),0)+coalesce(length(CAST(evidence_sha256 AS BLOB)),0)+coalesce(length(CAST(private_identity_json AS BLOB)),0)+40),0) FROM main.coordinator_scheduler_registration')
                amount+=32*row[0]
            row=self._one('SELECT coalesce(max(length(CAST(sql AS BLOB))),0) FROM main.sqlite_master WHERE type=? AND name=?',('index','coordinator_one_open_window'))
            amount+=256*row[0]
        return self.memory.hold(amount,'native-original-source-helper-scratch')


def lifecycle_native_read(db):
    return _LifecycleNativeReader(db)


def _lifecycle_capture_cells(function):
    """Retain executable closure values, not a decorated function's name only."""
    held=[]
    for name,cell in zip(function.__code__.co_freevars,function.__closure__ or ()):
        try:value=cell.cell_contents
        except ValueError:unproven()
        held.append((name,cell,value))
    wrapped=getattr(function,'__wrapped__',None)
    if wrapped is not None and not any(name in ('func','method') and value is wrapped for name,cell,value in held):
        unproven()
    return function,wrapped,tuple(held)


def _lifecycle_validate_cells(captures):
    for function,wrapped,held in captures:
        if getattr(function,'__wrapped__',None) is not wrapped:unproven()
        for name,cell,value in held:
            try:current=cell.cell_contents
            except ValueError:unproven()
            if current is not value:unproven()


class _LifecycleRegisteredBindings:
    """Actual registered methods, descriptors and bound instances; no SQL."""
    def __init__(self, world, host, storage):
        from .actor_execution import PreparationBudget
        from .coordinator_execution import CoordinatorAttempt
        from .source_attestation import _REGISTRY
        import types
        owner_value = _lifecycle_source_owner()
        owner_value.reserve(4096, 'registered-source-witness-carriers')
        _lc_adm1136_world_members()
        from .world import _lc_geo_Decoder
        self.items = [];self.cells = []
        owners = ((world, ('_mutate','_connection','_scoped_attempt','_scoped_entry_budget',
                    '_scoped_coordinator_attempt','_scoped_coordinator','_check_coordinator_execution',
                    '_lifecycle_check','_p04_expected','_check_frozen_p04','_apply_action','_lifecycle_context',
                    '_lifecycle_effect','prepare_lifecycle','preview_lifecycle','commit_lifecycle','fail_lifecycle',
                    '_lc_geo1133_mutate_body','_lc_geo1133_preview_body','_lc_geo1133_prepare_body',
                    '_lc_geo1133_preview_lifecycle_body','_lc_geo1133_commit_body',
                    '_lifecycle_bound_attempt','_lc_stale1141_fail_body')),
                  (_lc_geo_Decoder, ('__init__','pay','space','error','scalar','descend','value')),
                  (host, ('_with_entry_pipeline',)),
                  (storage, ('paths','_open','transaction','_transaction','audit','_material_audit')),
                  (CoordinatorAttempt, ('check',)),(PreparationBudget, ('check','locked','native_call')))
        if hasattr(type(host),'resolve_lifecycle_jobs'):
            owners += ((host, ('resolve_lifecycle_jobs',)),)
        owner_value.reserve(sum(len(names) for owner,names in owners)*512, 'registered-source-binding-slots')
        for owner,names in owners:
            cls = owner if isinstance(owner,type) else type(owner)
            for name in names:
                if not isinstance(owner,type) and name in vars(owner):unproven()
                descriptor = vars(cls).get(name)
                function = descriptor.__func__ if isinstance(descriptor,(staticmethod,classmethod)) else descriptor
                if not isinstance(function,types.FunctionType):unproven()
                chain = []
                while True:
                    owner_value.reserve(1024+256*len(function.__closure__ or ()), 'registered-source-chain-and-cells')
                    chain.append((function,function.__code__,function.__globals__))
                    inner = getattr(function,'__wrapped__',None)
                    if inner is None:break
                    if not isinstance(inner,types.FunctionType):unproven()
                    function = inner
                import contextlib
                native_helpers=tuple(c for c in contextlib.contextmanager.__code__.co_consts if isinstance(c,types.CodeType))
                space = function.__globals__;stamp = _REGISTRY.get(id(space))
                if (stamp is None or not stamp['matched'] or stamp['namespace'] is not space
                        or space.get(cls.__name__) is not cls
                        or not any(cls is old for old in stamp['objects'])
                        or not _history_original_method(function,stamp)):unproven()
                for wrapped,wrapped_code,wrapper_space in chain:
                    if wrapper_space is space:
                        if not _history_original_method(wrapped,stamp):unproven()
                    elif wrapper_space is not vars(contextlib) or wrapped_code not in native_helpers:unproven()
                self.cells.extend(_lifecycle_capture_cells(item[0]) for item in chain)
                bound = getattr(owner,name)
                if (not isinstance(owner,type) and not isinstance(descriptor,staticmethod)
                        and (getattr(bound,'__self__',None) is not (cls if isinstance(descriptor,classmethod) else owner)
                             or getattr(bound,'__func__',None) is not getattr(getattr(cls,name),'__func__',getattr(cls,name)))):unproven()
                self.items.append((owner,cls,name,descriptor,bound,tuple(chain)))
        self.validate()

    def validate(self):
        _lc_adm1136_world_members()
        _lifecycle_validate_cells(self.cells)
        for owner,cls,name,descriptor,bound,chain in self.items:
            if not isinstance(owner,type) and name in vars(owner):unproven()
            if vars(cls).get(name) is not descriptor:unproven()
            current = getattr(owner,name)
            if (getattr(current,'__self__',None) is not getattr(bound,'__self__',None)
                    or getattr(current,'__func__',current) is not getattr(bound,'__func__',bound)):unproven()
            for function,code,space in chain:
                if function.__code__ is not code or function.__globals__ is not space:unproven()


def _lifecycle_source_owner():
    owner = _lifecycle_value_context.get()
    if (owner is None or owner.closed or owner.failed or owner.memory.failed):
        raise _LifecycleLiveLimit('source witness requires the original operation owner')
    return owner


def _lifecycle_source_capture(path, check, bind):
    """Finite raw/compile/AST admission, priced before the actual read.

    The compiler/AST allowance is a declared source-length allocation model,
    not a portable CPython heap theorem. No code/AST from bind may escape;
    retained raw/bindings have their separate operation-owner reservations.
    """
    owner = _lifecycle_source_owner()
    check()
    size = path.stat().st_size
    # The original 16MiB ledger derives admission; no extra arbitrary file cap.
    owner.reserve(2 * (size + 1) + 4096, 'source-raw-retained-witness')
    scratch = owner.memory.hold(320 * size + 65536, 'source-compile-ast-transient')
    try:
        check()
        with path.open('rb') as stream:
            raw = stream.read(size + 1)
        check()
        if len(raw) != size:
            unproven()
        bind(raw)
        check()
    except BaseException as error:
        owner.fail(error)
        raise
    # bind's compilation/AST frame has returned. raw stays paid at the owner.
    scratch.release()
    return raw


def _lifecycle_source_dm_bind(raw, path, functions):
    import types
    code = compile(raw, str(path), 'exec', dont_inherit=True)
    pending = [code]; codes = {}
    while pending:
        item = pending.pop(); codes[item.co_qualname] = item
        pending.extend(c for c in item.co_consts if isinstance(c, types.CodeType))
    if any(f.__code__ != codes.get(f.__qualname__) for f in functions):
        unproven()


def _lifecycle_source_matches(path, raw, check):
    """Actual current full-byte comparison AND hash, fixed prepaid scratch.

    On growth/mismatch stop within one 64KiB read; do not materialize the tail.
    Healthy files reach EOF and both complete hashes; no stat/verdict cache.
    """
    owner = _lifecycle_source_owner()
    scratch = owner.memory.hold(196608, 'ordinary-source-compare-fixedscratch')
    try:
        check()
        expected = hashlib.sha256(raw)
        current = hashlib.sha256()
        offset = 0
        matches = True
        with path.open('rb') as stream:
            while True:
                check()
                chunk = stream.read(65536)
                check()
                if not chunk:
                    break
                width = len(chunk)
                if width > len(raw) - offset or chunk != raw[offset:offset + width]:
                    matches = False
                    chunk = None
                    break
                current.update(chunk)
                offset += width
                chunk = None
        matches = matches and offset == len(raw) and current.digest() == expected.digest()
        current = expected = None
        check()
    except BaseException as error:
        owner.fail(error)
        raise
    scratch.release()
    return matches


class _LifecycleOrdinarySources:
    """Raw/module namespace/compiled code/current binding, outside source9."""
    def __init__(self, host, root, check):
        import types
        from . import adjudication, real_provider
        self.host, self.root, self.check = host, Path(root).resolve(), check
        _lifecycle_source_owner().reserve(8192, 'ordinary-source-witness-carriers')
        self.modules = [];self.edges = [];self.cells = []
        for module, filename in ((adjudication, 'adjudication.py'), (real_provider, 'real_provider.py')):
            namespace = vars(module);path = Path(namespace['__file__']).resolve()
            if path != self.root/'app'/filename:
                unproven()
            _lifecycle_source_capture(path, self.check,
                lambda raw: self._bind_module(module, namespace, path, raw))
        self.adjudicator = host.adjudicator;self.gateway = host.real_gateway
        self.settings = host.real_settings
        if self.gateway is not None and self.gateway.settings is not self.settings:unproven()
        if self.adjudicator is not None:
            if type(self.adjudicator) is not adjudication.Adjudicator or self.adjudicator.world is not host.world:
                unproven()
            for name in ('_compile','_execute','_child_id','prepare_lifecycle','preview_lifecycle','commit_lifecycle'):
                function = getattr(adjudication.Adjudicator, name, None)
                if function is None:unproven()
                self.edges.append((self.adjudicator, name, getattr(self.adjudicator, name), function))
        if self.gateway is not None:
            if type(self.gateway) is not real_provider.RealGateway:unproven()
            for name in ('dm','_dm_succession','_generate','_generate_owned','_budget','close','version'):
                self.edges.append((self.gateway, name, getattr(self.gateway, name), getattr(real_provider.RealGateway, name)))
        self.validate()

    def _bind_module(self, module, namespace, path, raw):
        import types
        import ast
        owner_value = _lifecycle_source_owner()
        code = compile(raw, str(path), 'exec', dont_inherit=True)
        pending = [code];codes = {}
        while pending:
            item = pending.pop();codes[item.co_qualname] = item
            pending.extend(c for c in item.co_consts if isinstance(c, types.CodeType))
        import ast
        declared=ast.parse(raw)
        count = sum(isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) for node in ast.walk(declared))
        owner_value.reserve(4096+256*len(namespace)+1024*count, 'ordinary-source-binding-slots')
        for declaration in declared.body:
            if isinstance(declaration,ast.FunctionDef):
                candidate=namespace.get(declaration.name)
                if not isinstance(candidate,types.FunctionType):unproven()
            elif isinstance(declaration,ast.ClassDef):
                candidate=namespace.get(declaration.name)
                if not isinstance(candidate,type) or candidate.__module__!=namespace['__name__']:unproven()
                for method in declaration.body:
                    if isinstance(method,ast.FunctionDef):
                        value=vars(candidate).get(method.name)
                        if not isinstance(value,(types.FunctionType,staticmethod,classmethod,property)):unproven()
        bindings = tuple((name, value) for name,value in namespace.items()
                         if isinstance(value, (type, types.FunctionType, types.ModuleType)))
        functions = []
        def bind(owner, name, value):
            original = value
            if isinstance(value, (staticmethod, classmethod)):
                value = value.__func__
            if isinstance(value, property):
                for accessor in (value.fget, value.fset, value.fdel):
                    if accessor is not None:bind(owner, name, accessor)
                return
            if not isinstance(value, types.FunctionType):
                return
            chain = []
            while True:
                self.check()
                owner_value.reserve(1024+256*len(value.__closure__ or ()), 'ordinary-source-chain-and-cells')
                chain.append((value, value.__code__, value.__globals__))
                wrapped = getattr(value, '__wrapped__', None)
                if wrapped is None:break
                if not isinstance(wrapped, types.FunctionType):unproven()
                value = wrapped
            if value.__globals__ is not namespace:
                return
            if value.__code__ != codes.get(value.__qualname__):
                unproven()
            import contextlib
            native_helpers=tuple(c for c in contextlib.contextmanager.__code__.co_consts if isinstance(c,types.CodeType))
            for wrapped,wrapped_code,space in chain:
                if space is namespace:
                    if wrapped_code!=codes.get(wrapped_code.co_qualname):unproven()
                elif space is not vars(contextlib) or wrapped_code not in native_helpers:
                    unproven()
            self.cells.extend(_lifecycle_capture_cells(item[0]) for item in chain)
            functions.append((owner, name, original, tuple(chain)))
        for name,value in tuple(namespace.items()):
            if isinstance(value, type) and value.__module__ == namespace['__name__']:
                for key,item in vars(value).items():bind(value, key, item)
            elif isinstance(value, types.FunctionType):bind(module, name, value)
        self.modules.append((module, namespace, path, raw, bindings, tuple(functions)))

    def validate(self):
        self.check()
        _lifecycle_validate_cells(self.cells)
        if (self.host.adjudicator is not self.adjudicator or self.host.real_gateway is not self.gateway
                or self.host.real_settings is not self.settings
                or (self.adjudicator is not None and self.adjudicator.world is not self.host.world)
                or (self.gateway is not None and self.gateway.settings is not self.settings)):
            unproven()
        for module, namespace, path, raw, bindings, functions in self.modules:
            self.check()
            if vars(module) is not namespace or Path(namespace['__file__']).resolve() != path or not _lifecycle_source_matches(path, raw, self.check):
                unproven()
            if any(namespace.get(name) is not value for name,value in bindings):unproven()
            for owner,name,original,chain in functions:
                current = vars(owner).get(name)
                if isinstance(current, property):
                    if original not in (current.fget,current.fset,current.fdel):unproven()
                elif current is not original:unproven()
                for function,code,space in chain:
                    if function.__code__ is not code or function.__globals__ is not space:unproven()
        for owner,name,old,function in self.edges:
            current = getattr(owner, name)
            if (getattr(current,'__self__',None) is not getattr(old,'__self__',None)
                    or getattr(current,'__func__',current) is not function):unproven()
        self.check()


_lifecycle_scope = ContextVar('scheduler_lifecycle_work', default=None)
_lc_rollback1152_delivery = ContextVar('scheduler_lifecycle_rollback_delivery', default=None)


def _lc_rollback1152_latch(error, host, root, entry):
    """Read a terminal diagnostic only; never a positive authority decision."""
    from sys import _getframe
    work = _lifecycle_scope.get()
    owner = _lifecycle_value_context.get()
    if (type(work) is not _LifecycleWork or work.caller is not _getframe(1)
            or work.host is not host or work.root is not root or work.entry is not entry
            or work.closed or work.db is None or work.state is None
            or type(owner) is not _LifecycleValueOwner or owner is not entry._lifecycle_value_owner
            or not owner.failed or not owner.memory.failed):
        return None
    first, lane, control = owner.first_failure, owner.rollback_cleanup, work.stale1141
    if (lane is None or lane[0] is not None or first is None or first[0] is not error
            or type(control) is not _lc_stale1141_Control or control.phase != 'commit'
            or control.first is not None or control.ready or control.accepted):
        return None
    return first, lane


class _LifecycleWork:
    """A lexical root business operation; never a permit or actor attempt."""
    def __init__(self, host, captured_root, entry, job_id, service_version, caller):
        from .actors import ActorHost
        from .actor_execution import PreparationBudget, PrivateStore
        from .coordinator_execution import CoordinatorAttempt
        from .world import WorldStore
        if (type(host) is not ActorHost or type(captured_root) is not CoordinatorAttempt
                or type(entry) is not PreparationBudget or type(host.world) is not WorldStore
                or type(host._storage) is not PrivateStore):unproven()
        captured_root.check();entry.check()
        self.host, self.world, self.root, self.entry = host, host.world, captured_root, entry
        self.permit = captured_root.permit
        self.lifetime_edges=(captured_root.stop,entry.stop,host._closing,host._lock,host.world._private_sources_lock,
                             captured_root.policy,entry.policy,entry.captured,entry._accounts)
        self.job_id, self.service_version = job_id, service_version
        if (type(job_id) is not str or not job_id or len(job_id)>1024
                or type(service_version) is not str or re.fullmatch('[0-9a-f]{64}',service_version) is None):unproven()
        self.caller = caller
        self.caller_code = ActorHost.resolve_lifecycle_jobs.__code__
        self.caller_namespace = ActorHost.resolve_lifecycle_jobs.__globals__
        bound = host.resolve_lifecycle_jobs
        if (caller.f_code is not self.caller_code or caller.f_globals is not self.caller_namespace
                or caller.f_locals.get('self') is not host or caller.f_locals.get('captured_root') is not captured_root
                or getattr(bound,'__self__',None) is not host
                or getattr(bound,'__func__',None) is not ActorHost.resolve_lifecycle_jobs):unproven()
        self.closed = False;self.db = None;self.state = None
        self.stale1141 = None
        self.selection = _C0Selection(self.world, self.permit, entry, _host=host)
        # source() can choose a sibling Host for the same file; Work cannot.
        if self.selection.host is not host or self.selection.storage is not host._storage:unproven()
        self.ordinary = _LifecycleOrdinarySources(host, self.selection.source_root, self.check)
        if self.ordinary.adjudicator is None or self.ordinary.gateway is None:unproven()
        self.check_source()

    def check(self):
        from .actors import ActorHost
        self.root.check();self.entry.check()
        current_edges=(self.root.stop,self.entry.stop,self.host._closing,self.host._lock,self.world._private_sources_lock,
                       self.root.policy,self.entry.policy,self.entry.captured,self.entry._accounts)
        if any(current is not original for current,original in zip(current_edges,self.lifetime_edges)):unproven()
        if self.closed or self.host._closed or self.host._closing.is_set():
            from .actor_execution import ActorRunError
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        if (self.world._scoped_coordinator_attempt() is not self.root
                or self.world._scoped_entry_budget() is not self.entry
                or self.world._scoped_coordinator() is not self.permit
                or self.world._scoped_attempt() is not None or self.root.world is not self.world
                or self.root.permit is not self.permit or self.host.world is not self.world
                or self.host._authority is not self.world or self.host._storage is not self.selection.storage
                or self.caller is None or self.caller.f_code is not self.caller_code
                or self.caller.f_globals is not self.caller_namespace
                or self.caller.f_locals.get('self') is not self.host
                or self.caller.f_locals.get('captured_root') is not self.root
                or ActorHost.resolve_lifecycle_jobs.__code__ is not self.caller_code
                or ActorHost.resolve_lifecycle_jobs.__globals__ is not self.caller_namespace):unproven()
        import inspect
        frame=inspect.currentframe()
        cursor=frame
        try:
            while cursor is not None and cursor is not self.caller:cursor=cursor.f_back
            if cursor is None:unproven()
        finally:
            del frame,cursor
        self.selection.check()

    def check_source(self):
        self.check();self.selection.check_source()
        self.ordinary.validate()
        self.check()

    def _before(self, db, state, readonly):
        from .world import _AttemptConnection
        if lifecycle_work(self.world,self.job_id) is not self:unproven()
        self.check_source()
        if not self.host._lock._is_owned():unproven()
        if type(db) is not _AttemptConnection or db._attempt is not self.entry or not db.in_transaction:unproven()
        self.world._check_coordinator_execution(db,state['world_id'],self.permit,readonly=readonly)
        storage = self.host._storage
        storage.paths()
        if 'actor' not in {r[1] for r in db.execute('PRAGMA database_list')}:
            db.execute('ATTACH DATABASE ? AS actor',(storage.actor_path.as_uri()+'?mode=ro',)).close()
        registration(db,state['world_id'],self.selection.identity)
        proof = self.world._lifecycle_check(db,state)
        if proof is None or state['world_id'] != self.permit.world_id:unproven()
        jobs = [job for job in proof['lifecycle']['jobs'] if job['id']==self.job_id]
        if len(jobs)!=1:unproven()
        for job in jobs:
            for attempt in job['attempts']:
                if attempt['status']=='pending' and attempt['request']['service_version']!=self.service_version:unproven()
        # Whole old window/private closure remains mandatory. No dead-person skip.
        window_history(db,state['world_id'],state)
        for row in db.execute('SELECT window_id FROM coordinator_windows WHERE world_id=?',(state['world_id'],)).fetchall():
            window_phase_proof(self.world,db,row[0])
        self.world._check_coordinator_execution(db,state['world_id'],self.permit,readonly=readonly)
        self.check_source()
        self.db = None if readonly else db
        self.state = None if readonly else state

    def before_effect(self, db, state):
        return self._before(db,state,False)

    def before_preview(self, db, state):
        return self._before(db,state,True)

    def __call__(self, db):
        # World has just consumed persisted lifecycle proof and original root SQL.
        # Deliberately no SQL or read adapter after the original root SQL gate.
        if (type(_lifecycle_scope.get()) is not _LifecycleWork or _lifecycle_scope.get() is not self
                or self.db is not db or not db.in_transaction or getattr(db,'_attempt',None) is not self.entry):unproven()
        self.check_source()
        if self.state is None:unproven()
        jobs=[job for job in self.state['_lifecycle']['jobs'] if job['id']==self.job_id]
        if len(jobs)!=1:unproven()
        if jobs[0]['attempts'] and jobs[0]['attempts'][-1]['request']['service_version']!=self.service_version:unproven()
        self.db = None;self.state = None


def lifecycle_work(world, job_id):
    work = _lifecycle_scope.get()
    if type(work) is not _LifecycleWork or work.world is not world or work.job_id != job_id:
        unproven()
    work.check_source()
    if not work.host._lock._is_owned():unproven()
    return work


@contextmanager
def lifecycle_scope(host, captured_root, entry, job_id, service_version):
    import inspect
    # contextlib invokes the generator; the real Host frame is exactly two up.
    frame = inspect.currentframe()
    try:
        caller = frame.f_back.f_back
        if _lifecycle_scope.get() is not None:unproven()
        with _lifecycle_operation_scope(entry=entry):
            with host._with_entry_pipeline(), source_lock(host.world):
                work = _LifecycleWork(host,captured_root,entry,job_id,service_version,caller)
            token = _lifecycle_scope.set(work)
            try:
                yield work
                work.check_source()
            finally:
                work.closed = True;work.db = None;work.state = None;work.caller = None
                work.stale1141 = None
                _lifecycle_scope.reset(token)
    finally:
        del frame, caller


_LIFECYCLE_ACTIONS = frozenset(('resolve_physical_attempt','write_office_document','read_office_document'))


def _lifecycle_domain_source(error):
    from .world import DomainActionError, _lc_effect_conditions, _lc_office_place
    _lifecycle_world_primitives()
    if type(error) is not DomainActionError:
        return False
    cursor = error.__traceback__
    if cursor is None:return False
    while cursor.tb_next is not None:cursor = cursor.tb_next
    return any(cursor.tb_frame.f_code is function.__code__ and cursor.tb_frame.f_globals is function.__globals__
               for function in (_lc_effect_conditions, _lc_office_place))


def _lifecycle_rejection_legacy(read, world, row, result, decision):
    """Same primitive historical producer proof for native and offlock leaves.

    Replay only a private state copy. No permit, fake DB, SQL dispatch adapter,
    saved verdict or persisted candidate is made by this proof.
    """
    if result['action'] not in _LIFECYCLE_ACTIONS:
        return
    from .world import (DomainActionError, _lc_optin, _lc_child, _lc_effect_conditions,
                        _world_mutation_scope, _lc_copy, _lifecycle_project, _lc_historical)
    if result['origin'] != 'adjudication.execute':unproven()
    genesis_row = read('main.snapshots', columns=('state',), order=(('seq',False),), limit=1, first=True)
    if genesis_row is None:unproven()
    genesis = json.loads(genesis_row[0])
    if not _lc_optin(genesis):unproven()
    base_revision = decision['base_revision'];integer(base_revision,1)
    # The single causal pass validates every original revision and producer.
    # The state tuple does not retain complete snapshots or authorize anything.
    _lifecycle_project(read,genesis,base_revision)
    states = (read,genesis,base_revision)
    state = _lc_historical(states,base_revision);index = result['effect_index']
    cause = dict(parent_intent_id=row['intent_id'],decision_version=result['decision_version'],
                 effect_index=index,child_intent_id=f"dm:{len(row['intent_id'])}:{row['intent_id']}:{result['decision_version']}:{index}")
    pid,proposal,body,original,_ = _lc_child(read,row['world_id'],cause,base_revision,states,committed=False)
    if original != decision or pid != row['person_id']:unproven()
    person = world._find(state['persons'],pid,'ACTOR_RECOVERY_UNPROVEN')
    token = _world_mutation_scope.set(None)
    try:
        for position,effect in enumerate(decision['effects'][:index+1]):
            child = f"dm:{len(row['intent_id'])}:{row['intent_id']}:{result['decision_version']}:{position}"
            if effect['action'] not in _LIFECYCLE_ACTIONS:
                # Original pure state operation, with no mutation DB/actor scope.
                try:world._apply_action(state,person,child,effect,decision['ruling'])
                except DomainActionError:unproven()  # An earlier error is not this leaf.
                continue
            try:
                condition = _lc_effect_conditions(state,person,proposal,effect,decision['ruling'],base_revision)
            except DomainActionError as error:
                if (position != index or not _lifecycle_domain_source(error)
                        or error.code != result['code'] or error.action != result['action']):unproven()
                return
            if position == index:unproven()  # Claimed rejection had no real producer.
            if decision['ruling'] not in ('成功','大成功'):continue
            # Only condition dependencies of later lifecycle effects are projected.
            # Notices/event log are not authority and do not influence these gates.
            if effect['action']=='write_office_document':
                state['_lifecycle']['documents'].append(dict(id='office-document-'+child,
                    author_person_id=pid,office_id=person['office_id'],place_id=condition['id'],text=effect['text'],
                    created_revision=base_revision+1,cause_ref=dict(parent_intent_id=row['intent_id'],
                        decision_version=result['decision_version'],effect_index=position,child_intent_id=child),
                    access='current_office_at_place'))
            elif effect['action']=='resolve_physical_attempt' and effect['outcome']=='mortal':
                target,means = condition
                state['_lifecycle']['transitions'].append(dict(kind='mortal_injury',person_id=target['id']))
        unproven()
    finally:
        _world_mutation_scope.reset(token)


def _lifecycle_activation_identity_legacy(read, genesis, activation):
    revision=activation['observation_revision']
    saved=read('main.snapshots',(('seq',revision),),('state',),first=True)
    if saved is None:unproven()
    state=json.loads(saved[0]);proof=_lifecycle_snapshot_proof(read,state,genesis)
    pid=activation['person_id']
    if proof is None or pid not in proof['active']:unproven()
    from .world import _lc_tenure
    tenure=_lc_tenure(state,pid,revision)
    private=read('actor.actor_intents',(('intent_id',activation['intent_id']),('person_id',pid)),first=True)
    if private is None:unproven()
    request=json.loads(private['request'])
    expected=dict(world_id=state['world_id'],person_id=pid,observation_revision=revision,
                  origin_ref=pid,tenure_ref=tenure['id'] if tenure is not None else None)
    if request.get('schema_version')!=3 or request.get('identity_binding')!=expected:unproven()


def _lifecycle_compile_legacy(world, decision, read):
    from .adjudication import Adjudicator
    from .world import _lc_optin
    from types import SimpleNamespace
    first=read('main.snapshots',columns=('state',),order=(('seq',False),),limit=1,first=True)
    if first is None:unproven()
    genesis=json.loads(first[0])
    if _lc_optin(genesis):
        return Adjudicator._compile(SimpleNamespace(world=world),decision,lifecycle_version='p09-lifecycle-v1')
    return Adjudicator._compile(SimpleNamespace(world=world),decision)




# Forward-only C0 V3. Insert before the original source registration.
# No helper in the 1112-owned _LifecycleLive*/Native/JSON resource domain is defined here.
_c0_v3_frame_context = ContextVar('c0_v3_consumer_frame', default=None)


def _c0_v3_text_size(value):
    return sum(1 if ord(c) < 128 else 2 if ord(c) < 2048 else 3 if ord(c) < 65536 else 4 for c in value)


def _c0_v3_material(material):
    owner = material.get('_c0_v3_owner')
    if owner is None:
        return None
    if type(owner) is not _C0V3MaterialOwner or owner.material is not material:
        unproven()
    owner.check()
    return owner


class _C0V3Loan:
    __slots__ = ('owner', 'value', 'leases', 'closed', 'failed')

    def __init__(self, owner, value=None, leases=()):
        self.owner = owner
        self.value = value
        self.leases = list(leases)
        self.closed = self.failed = False

    def close(self):
        if self.closed:
            return
        if self.failed or (self.owner is not None and self.owner.memory.failed):
            raise _LifecycleLiveLimit('exceptional C0 debt cannot be refunded')
        self.value = None
        for lease in reversed(self.leases):
            lease.release()
        self.leases.clear()
        self.closed = True
        self.owner = None

    def fail(self):
        # An exception traceback can retain caller aliases. Never refund them.
        self.value = None
        self.failed = True
        if self.owner is not None:
            self.owner.failed = True
            self.owner.memory.failed = True

    def transfer(self, destination):
        # Bridge the actual carrier to the existing value-loan protocol. Move
        # its original leases; never release/reprice while a result is aliased.
        if (self.closed or self.failed or self.owner is None
                or type(destination) is not _LifecycleValueOwner
                or destination.memory is not self.owner.memory or self.owner.memory.failed
                or destination.closed or destination.failed or not self.leases):
            raise _LifecycleLiveLimit('invalid C0 result-loan transfer')
        bridge = _LifecycleValueLoan(self.owner.memory, self.leases[0])
        bridge.leases = self.leases
        bridge.value = self.value
        bridge.transfer(destination)
        self.value = None
        self.leases = []
        self.owner = None
        self.closed = True
        return bridge


class _C0V3PackedRecords:
    """Fixed prepaid byte blocks; no retained per-row Python descriptor graph.

    The pointer stream is fixed-width (offset,length); the payload is typed
    scalars plus raw-size and whole-Row digest. Decoding never constructs a Row.
    Only an iterator's prepaid scratch retains decoded descriptor temporaries.
    """
    __slots__ = ('directory', 'blocks', 'pointers', 'used', 'count', 'maximum')
    BLOCK = 65536
    POINTER_BLOCK = 4096

    def __init__(self, directory):
        self.directory = directory
        self.blocks, self.pointers = [], []
        self.used = self.count = self.maximum = 0

    def __len__(self):
        return self.count

    def append(self, payload):
        owner = self.directory.owner
        end = self.used + len(payload)
        while end > len(self.blocks) * self.BLOCK:
            owner.leases.append(owner.memory.hold(self.BLOCK + 256, 'c0-v3-fixed-provenance-byte-block'))
            self.blocks.append(bytearray(self.BLOCK))
        pointer = self.count * 16
        if pointer == len(self.pointers) * self.POINTER_BLOCK:
            owner.leases.append(owner.memory.hold(self.POINTER_BLOCK + 256, 'c0-v3-fixed-provenance-offset-block'))
            self.pointers.append(bytearray(self.POINTER_BLOCK))
        start = self.used
        view = memoryview(payload)
        while self.used < end:
            block, offset = divmod(self.used, self.BLOCK)
            amount = min(end - self.used, self.BLOCK - offset)
            self.blocks[block][offset:offset+amount] = view[self.used-start:self.used-start+amount]
            self.used += amount
        block, offset = divmod(pointer, self.POINTER_BLOCK)
        self.pointers[block][offset:offset+16] = start.to_bytes(8, 'big') + len(payload).to_bytes(8, 'big')
        self.count += 1
        self.maximum = max(self.maximum, len(payload))

    def __getitem__(self, index):
        if type(index) is not int:
            raise TypeError('provenance index must be an int')
        if index < 0:
            index += self.count
        if not 0 <= index < self.count:
            raise IndexError(index)
        block, offset = divmod(index * 16, self.POINTER_BLOCK)
        pointer = self.pointers[block]
        start = int.from_bytes(pointer[offset:offset+8], 'big')
        length = int.from_bytes(pointer[offset+8:offset+16], 'big')
        payload = bytearray(length)
        copied = 0
        while copied < length:
            block, offset = divmod(start + copied, self.BLOCK)
            amount = min(length - copied, self.BLOCK - offset)
            payload[copied:copied+amount] = memoryview(self.blocks[block])[offset:offset+amount]
            copied += amount
        size = int.from_bytes(payload[:8], 'big')
        digest = bytes(payload[8:40])
        scalars = []
        offset = 40
        for _ in self.directory.fields:
            kind = payload[offset]; offset += 1
            if kind == 0:
                value = None
            elif kind == 1:
                value = int.from_bytes(payload[offset:offset+8], 'big', signed=True)
                offset += 8
            else:
                length = int.from_bytes(payload[offset:offset+4], 'big'); offset += 4
                encoded = bytes(payload[offset:offset+length]); offset += length
                if kind == 2:
                    value = encoded.decode('utf-8')
                elif kind == 3:
                    value = encoded
                elif kind == 4:
                    value = float.fromhex(encoded.decode('ascii'))
                else:
                    unproven()
            scalars.append(value)
        if offset != len(payload):
            unproven()
        scalars = tuple(scalars)
        keys = tuple(scalars[self.directory.fields.index(name)] for name in self.directory.keys)
        return keys, size, digest, scalars

    def __iter__(self):
        for index in range(self.count):
            yield self[index]

    def clear(self):
        self.blocks.clear(); self.pointers.clear()
        self.used = self.count = self.maximum = 0
        self.directory = None


class _C0V3Directory:
    __slots__ = ('owner', 'table', 'keys', 'records', 'fields', 'terminal', 'absent')

    def __init__(self, owner, table, keys):
        self.owner, self.table, self.keys = owner, table, keys
        self.fields = ()
        self.records = _C0V3PackedRecords(self)
        self.terminal = self.absent = False

    def seal(self):
        self.terminal = True


class _C0V3MaterialOwner:
    def __init__(self, capsule):
        if type(capsule) is not _C0Selection:
            unproven()
        self.capsule = capsule
        self.memory = _lifecycle_live_context.get()
        if self.memory is None:
            self.memory = _LifecycleLiveLedger()
        self.generation = object()
        self.source_generation = capsule.source_generation
        self.root = capsule.root
        self.entry = capsule.budget
        self.vector = None
        self.tables = {}
        self.coverage = {}
        self.leases = []
        self.exports = []
        self.descriptor_scratch = 0
        self.failed = self.closed = False
        self.io_rows = self.io_bytes = self.acquisitions = 0
        self.material = MappingProxyType(dict(tables=MappingProxyType(self.tables),
            check=self.check, coverage=MappingProxyType(self.coverage), _c0_v3_owner=self,
            version='C0-V3-bounded-native-row'))
        self.leases.append(self.memory.hold(65536, 'c0-v3-owner-acquisition-and-consumer-carriers'))

        self.ro1127 = _lc_ro1127_context.get()
        self.error_tuple = None
    def check(self):
        if (self.closed or self.failed or self.memory.failed
                or self.ro1127 is not None and self.ro1127.failed
                or self.capsule.budget is not self.entry
                or self.capsule.root is not self.root
                or self.capsule.source_generation is not self.source_generation
                or (self.vector is not None and self.capsule.vector is not self.vector)):
            unproven()
        self.capsule.check()

    def prepay_catalog(self, db):
        # Read only bounded aggregate metadata before _history_witness/catalog allocation.
        size = count = 0
        for schema in ('main', 'actor'):
            cursor = db.execute(f"SELECT count(*),coalesce(sum(length(CAST(type AS BLOB))+length(CAST(name AS BLOB))+length(CAST(tbl_name AS BLOB))+coalesce(length(CAST(sql AS BLOB)),0)),0) FROM {schema}.sqlite_master")
            try:
                row = cursor.fetchone()
                count += row[0]
                size += row[1]
                row = None
            finally:
                cursor.close()
        if type(size) is not int or type(count) is not int or min(size, count) < 0:
            unproven()
        # Retained vector, both native catalog temporaries, coverage/actual maps,
        # native Row backing and sorting/index scratch are simultaneously priced.
        self.leases.append(self.memory.hold(32 * size + 8192 * count + 65536,
            'c0-v3-catalog-vector-and-pin-scratch'))

    def ensure_descriptor_scratch(self, amount):
        # The census retains one frontier; every nested query pays its own
        # decoding scratch. Blocks are charged before increasing that frontier.
        while self.descriptor_scratch < amount:
            self.leases.append(self.memory.hold(65536, 'c0-v3-census-packed-frontier-scratch'))
            self.descriptor_scratch += 65536

    def remember(self, directory, row, size, digest):
        keys = tuple(row[name] for name in directory.keys)
        if any(type(v) not in (str, int) for v in keys):
            unproven()
        fields = tuple(name for name in row.keys() if name in (
            'seq', 'key', 'world_id', 'id', 'intent_id', 'window_id', 'activation_id',
            'person_id', 'actor_epoch', 'operation_id', 'kind', 'version', 'singleton',
            'call_id', 'type', 'name', 'ordinal', 'roster_index', 'base_version', 'promoted_version',
            'advanced_revision', 'visible_from'))
        if directory.fields and directory.fields != fields:
            unproven()
        if any(name not in fields for name in directory.keys):
            unproven()
        directory.fields = fields
        scalar_bytes = sum(_c0_v3_text_size(row[name]) if type(row[name]) is str else len(row[name]) if type(row[name]) is bytes else 64 for name in fields)
        self.ensure_descriptor_scratch(16 * (scalar_bytes + 64 * len(fields) + 64))
        def pack():
            payload = bytearray(size.to_bytes(8, 'big') + digest)
            for name in fields:
                value = row[name]
                if value is None:
                    payload.append(0)
                elif type(value) is int:
                    payload.append(1); payload.extend(value.to_bytes(8, 'big', signed=True))
                else:
                    if type(value) is str:
                        kind, encoded = 2, value.encode('utf-8')
                    elif type(value) is bytes:
                        kind, encoded = 3, value
                    elif type(value) is float:
                        kind, encoded = 4, value.hex().encode('ascii')
                    else:
                        unproven()
                    payload.append(kind); payload.extend(len(encoded).to_bytes(4, 'big')); payload.extend(encoded)
            return payload
        payload = pack()
        if len(directory.records) and directory.records[-1][0] == keys:
            unproven()
        directory.records.append(payload)
        payload = None

    def fail(self):
        # The original exception and first TB remain the active primary. No
        # cleanup is attempted here and no trace-visible charge is refunded.
        import sys
        error = sys.exception()
        if error is not None and self.error_tuple is None:
            self.error_tuple = (error, BaseException.__traceback__.__get__(error),
                BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))
        self.failed = True
        self.memory.failed = True
        for loan in self.exports:
            loan.fail()

    def close(self):
        if self.closed or self.failed or self.memory.failed:
            return
        if self.exports:
            raise _LifecycleLiveLimit('c0-v3 result loans require a real ownership transfer')
        for directory in self.tables.values():
            directory.records.clear()
            directory.owner = None
        self.tables.clear(); self.coverage.clear()
        self.vector = None
        self.capsule = self.root = self.entry = None
        for lease in reversed(self.leases):
            lease.release()
        self.leases.clear()
        self.closed = True


def _c0_v3_fingerprint(row):
    # This digest binds provenance only, NEVER a causal/authority success.
    digest = hashlib.sha256()
    for key in row.keys():
        raw = key.encode('utf-8')
        digest.update(len(raw).to_bytes(8, 'big')); digest.update(raw)
        raw = None
        value = row[key]
        if value is None:
            kind, raw = b'n', b''
        elif type(value) is str:
            kind, raw = b's', value.encode('utf-8')
        elif type(value) is bytes:
            kind, raw = b'b', value
        elif type(value) is int:
            kind, raw = b'i', str(value).encode('ascii')
        elif type(value) is float:
            kind, raw = b'f', value.hex().encode('ascii')
        else:
            unproven()
        digest.update(kind); digest.update(len(raw).to_bytes(8, 'big')); digest.update(raw)
        value = raw = None
    return digest.digest()


def _c0_v3_fetch(db, sql, params, owner):
    # Original bounded_rows still enforces the original per-row gate and payload
    # error/close priority. V3 always supplies LIMIT 1 and prices before fetch.
    cursor = db.execute('SELECT * FROM (' + sql + ') LIMIT 0', params)
    try:
        names = tuple(part[0] for part in cursor.description)
    finally:
        cursor.close()
    expressions = []
    for name in names:
        column = '"' + name.replace('"', '""') + '"'
        expressions.append("CASE typeof(" + column + ") WHEN 'text' THEN length(CAST(" + column + " AS BLOB)) WHEN 'blob' THEN length(" + column + ") ELSE 8 END")
    cursor = db.execute('SELECT ' + ' + '.join(expressions) + ' FROM (' + sql + ')', params)
    try:
        price = cursor.fetchone()
        size = None if price is None else price[0]
        price = None
    finally:
        cursor.close()
    loan = _C0V3Loan(owner)
    if size is not None:
        loan.leases.append(owner.memory.hold(_lifecycle_raw_charge(size, len(names)) + size + 8192,
            'c0-v3-genuine-row-and-fingerprint-scratch'))
    try:
        rows, amount = _c0_bounded_rows(db, sql, params, owner.capsule, 0)
        if len(rows) > 1 or (not rows) != (size is None) or (size is not None and amount != size):
            unproven()
        loan.value = rows[0] if rows else None
        rows = None
        if size is not None:
            owner.io_rows += 1; owner.io_bytes += size
        return loan, size
    except BaseException:
        loan.fail()
        raise


def _c0_v3_acquire(owner, sql, params=(), *, initial=False):
    owner.check()
    capsule = owner.capsule
    fresh = _C0Fresh(capsule)
    first_failure = None
    loan = None
    for admission in range(2):
        pin_failure = None; qualified = False
        try:
            with _c0_segment(capsule, fresh) as db:
                capsule.check_source()
                if initial:
                    owner.prepay_catalog(db)
                try:
                    _c0_collect_pin(capsule, db)
                except BaseException as pin_error:
                    pin_failure = (pin_error, pin_error.__traceback__)
                    qualified = _c0_first_pin_busy(capsule, db, *pin_failure)
                    raise
                loan, size = _c0_v3_fetch(db, sql, params, owner)
                _c0_collect_pin(capsule, db)
                capsule.check_source()
        except BaseException as error:
            if loan is not None:
                loan.fail()
            if first_failure is None and pin_failure is not None and error is not pin_failure[0]:
                error.__dict__.setdefault('_c0_first_pin_failure', pin_failure)
            if first_failure is not None and error is not first_failure[0]:
                error.__dict__.setdefault('_c0_first_pin_failure', first_failure)
            if (admission != 0 or fresh.used or pin_failure is None
                    or error is not pin_failure[0] or not qualified):
                raise
            first_failure = pin_failure
            fresh.failed_reader = db
            _c0_recovery_wait(capsule, fresh, pin_failure)
            continue
        break
    owner.acquisitions += 1
    owner.vector = capsule.vector
    owner.check()
    return loan, size


def _c0_v3_original(owner, directory, record):
    if directory.owner is not owner or not directory.terminal or directory.absent:
        unproven()
    keys, expected_size, expected_digest, scalar_index = record
    where = ' AND '.join('"' + name + '"=?' for name in directory.keys)
    sql = 'SELECT * FROM ' + directory.table + ' WHERE ' + where + ' LIMIT 1'
    loan, size = _c0_v3_acquire(owner, sql, keys)
    try:
        row = loan.value
        if (type(row) is not sqlite3.Row or size != expected_size
                or tuple(row[name] for name in directory.keys) != keys
                or tuple(row[name] for name in directory.fields) != scalar_index
                or _c0_v3_fingerprint(row) != expected_digest):
            unproven()
        row = None
        return loan
    except BaseException:
        loan.fail()
        raise


def _c0_collect_v3(owner):
    capsule = owner.capsule
    # The legal original latest/root prefix selected a possible LC world;
    # only this actual immutable first Row can authorize the V3 representation.
    genesis_loan, _ = _c0_v3_acquire(owner, 'SELECT * FROM main.snapshots ORDER BY seq LIMIT 1', initial=True)
    try:
        def decode_genesis():
            row = genesis_loan.value
            if row is None:
                unproven()
            return _lifecycle_json_borrow(row['state'], memory=owner.memory, role='c0-v3-actual-genesis')
        genesis = decode_genesis()
        from .world import _lc_optin, _lifecycle_shape
        optin = _lc_optin(genesis.value)
        if not optin:
            # A late-added lifecycle marker is never a genesis opt-in.
            unproven()
        latest_loan, _ = _c0_v3_acquire(owner, 'SELECT * FROM main.snapshots ORDER BY seq DESC LIMIT 1')
        latest = _lifecycle_json_borrow(latest_loan.value['state'], memory=owner.memory, role='c0-v3-initial-current')
        _lifecycle_shape(latest.value, genesis.value)
        latest.close(); latest_loan.close()
        genesis.close(); genesis_loan.close()
        present = {schema + '.' + name for schema, part in zip(('main', 'actor'), owner.vector)
                   for name, ddl in part[2] if ddl is not None}
        tables = dict(_C0_KEYS)
        tables.update({'main.sqlite_master': ('type', 'name'), 'actor.sqlite_master': ('type', 'name')})
        for table, keys in tables.items():
            owner.check()
            directory = _C0V3Directory(owner, table, keys)
            owner.tables[table] = directory
            if table not in present and not table.endswith('.sqlite_master'):
                directory.absent = directory.terminal = True
                owner.coverage[table] = ('absent', 0)
                continue
            last = None
            while True:
                keyexpr = ','.join(keys)
                where = '' if last is None else ' WHERE (' + keyexpr + ') > (' + ','.join('?' for _ in keys) + ')'
                sql = 'SELECT * FROM ' + table + where + ' ORDER BY ' + keyexpr + ' LIMIT 1'
                loan, size = _c0_v3_acquire(owner, sql, () if last is None else last)
                try:
                    def census_one():
                        row = loan.value
                        if row is None:
                            return None
                        owner.remember(directory, row, size, _c0_v3_fingerprint(row))
                        return directory.records[-1][0]
                    next_key = census_one()
                except BaseException:
                    loan.fail()
                    raise
                loan.close()
                if next_key is None:
                    directory.seal()
                    owner.coverage[table] = ('terminal-empty-page', len(directory.records))
                    break
                last = next_key
        # Absence is explicit in directory but old table existence semantics remain.
        for table in tuple(owner.tables):
            if owner.tables[table].absent:
                del owner.tables[table]
        owner.check()
        return owner.material
    except BaseException:
        owner.fail()
        genesis_loan.fail()
        if 'genesis' in locals():
            genesis.fail()
        if 'latest' in locals():
            latest.fail()
        if 'latest_loan' in locals():
            latest_loan.fail()
        raise


def _c0_v3_adopt(material, loan):
    frame = _c0_v3_frame_context.get()
    owner = _c0_v3_material(material)
    if frame is None or frame['owner'] is not owner:
        owner.exports.append(loan)
    else:
        frame['loans'].append(loan)
    return loan.value


def _c0_v3_json(material, raw):
    owner = _c0_v3_material(material)
    # Retain scratch for the original consumer's implicit slices, set/dict
    # comparisons and predicate-local canonicalization until its frame is gone.
    amount = _c0_v3_text_size(raw) if type(raw) is str else len(raw)
    scratch = _C0V3Loan(owner, leases=(owner.memory.hold(32 * amount + 8192,
        'c0-v3-consumer-json-derived-slice-set-and-predicate-scratch'),))
    _c0_v3_adopt(material, scratch)
    return _c0_v3_adopt(material, _lifecycle_json_borrow(raw, memory=owner.memory, role='c0-v3-decoded-consumer'))


def _c0_v3_copy(material, value):
    owner = _c0_v3_material(material)
    return _c0_v3_adopt(material, _lifecycle_copy_borrow(value, memory=owner.memory, role='c0-v3-copy-consumer'))


def _c0_v3_hash(material, value):
    owner = _c0_v3_material(material)
    return _lifecycle_hash_owned(value, memory=owner.memory)


def _c0_v3_digest(material, raw):
    owner = _c0_v3_material(material)
    loan = _lifecycle_json_borrow(raw, memory=owner.memory, role='c0-v3-canonical-digest-input')
    try:
        result = _lifecycle_hash_owned(loan.value, memory=owner.memory)
    except BaseException:
        loan.fail(); owner.fail()
        raise
    loan.close()
    return result


def _c0_v3_canonical(material, value):
    owner = _c0_v3_material(material)
    # Allocation-free size walk; overprices escapes, integer/float text and slots.
    def price(item):
        if type(item) is str:
            return 6 * len(item) + 2
        if type(item) in (int, float, bool, type(None)):
            return 64
        if type(item) is list:
            return 2 + sum(1 + price(v) for v in item)
        if type(item) is dict:
            return 2 + sum(2 + price(k) + price(v) for k, v in item.items())
        raise TypeError('not a builtin JSON value')
    estimate = price(value)
    lease = owner.memory.hold(8 * estimate + 8192, 'c0-v3-canonical-text-utf8-and-sort-scratch')
    loan = _C0V3Loan(owner, leases=(lease,))
    try:
        loan.value = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
        return _c0_v3_adopt(material, loan)
    except BaseException:
        loan.fail()
        raise


def _c0_v3_body_matches(material, request, body, candidate):
    return body_matches(request, body, _c0_v3_json(material, candidate) if type(candidate) is str else candidate)


def _c0_v3_invoke(material, function, *args, **kwargs):
    owner = _c0_v3_material(material)
    if owner is None:
        return function(*args, **kwargs)
    parent = _c0_v3_frame_context.get()
    carrier = _C0V3Loan(owner, leases=(owner.memory.hold(16384, 'c0-v3-consumer-frame-and-builtin-temporaries'),))
    # Fund both carriers BEFORE setting any context token. Constructor quota
    # refusal cannot leave a failed C0 frame installed into another Entry.
    values = _LifecycleValueOwner(owner.memory)
    frame = dict(owner=owner, loans=[carrier])
    token = _c0_v3_frame_context.set(frame)
    memory_token = _lifecycle_live_context.set(owner.memory)
    value_token = _lifecycle_value_context.set(values)
    exports = []
    try:
        result = function(*args, **kwargs)
        # The complete normal consumer frame is gone. Clone escaping builtins;
        # a true Row instead carries the exact original native loan.
        def export(value):
            if type(value) is tuple:
                loan = _C0V3Loan(owner, leases=(owner.memory.hold(1024 + 64 * len(value), 'c0-v3-export-tuple'),))
                frame['loans'].append(loan)
                exports.append(loan)
                loan.value = tuple(export(item) for item in value)
                return loan.value
            if type(value) in (dict, list, str):
                loan = _lifecycle_copy_borrow(value, memory=owner.memory, role='c0-v3-independent-export')
                frame['loans'].append(loan); exports.append(loan)
                return loan.value
            if type(value) is bytes:
                loan = _C0V3Loan(owner, leases=(owner.memory.hold(3 * len(value) + 512, 'c0-v3-independent-bytes-export'),))
                frame['loans'].append(loan); exports.append(loan)
                loan.value = bytes(bytearray(value))
                return loan.value
            if type(value) is sqlite3.Row:
                # Input Rows may already belong to the parent frame. They stay
                # there; only locally acquired original Rows need promotion.
                for loan in frame['loans']:
                    if loan.value is value:
                        if loan not in exports:
                            exports.append(loan)
                        break
                return value
            return value
        if owner.memory.failed:
            raise _LifecycleLiveLimit('C0 consumer retained exceptional shared debt')
        output_value = export(result)
        result = None
        destination = parent['loans'] if parent is not None and parent['owner'] is owner else owner.exports
        for loan in exports:
            frame['loans'].remove(loan)
            destination.append(loan)
        exports.clear()
        # Every remaining loan was local to the now-returned inner frame.
        for loan in reversed(frame['loans']):
            loan.close()
        frame['loans'].clear()
        values.close()
        return output_value
    except BaseException:
        owner.fail()
        values.fail()
        for loan in frame['loans']:
            loan.fail()
        raise
    finally:
        _lifecycle_value_context.reset(value_token)
        _lifecycle_live_context.reset(memory_token)
        _c0_v3_frame_context.reset(token)


class _C0V3BorrowIterator:
    def __init__(self, material, table, equality=(), columns=None, order=(), *, prepare=False, limit=None, _owner=None):
        self.material = material
        self.owner = _c0_v3_material(material) if _owner is None else _owner
        self.directory = self.owner.tables[table]
        self.equality, self.columns, self.order = equality, columns, order
        self.position = 0
        self.previous = None
        self.selected = None
        self.leases = []
        self.closed = False
        # Acquisition/descriptor scratch, NOT another original read guard pair.
        self.leases.append(self.owner.memory.hold(16384 + 16 * self.directory.records.maximum,
            'c0-v3-iterator-descriptor-decode-and-scalar-scratch'))
        if order or prepare:
            self.selected = []
            safe = all(type(k) is str and k in self.directory.fields for k, _ in equality)
            safe = safe and all(type(k) is str and k in self.directory.fields for k, _ in order)
            for index in range(len(self.directory.records)):
                record = self.directory.records[index]
                if safe:
                    values = record[3]
                    def scalar(name):
                        return values[self.directory.fields.index(name)]
                    if all(scalar(k) == v and scalar(k) is not None and v is not None for k, v in equality):
                        amount = sum(4 * _c0_v3_text_size(scalar(k)) if type(scalar(k)) is str
                                     else len(scalar(k)) if type(scalar(k)) is bytes else 64 for k, _ in order)
                        self.leases.append(self.owner.memory.hold(512 + amount + 128 * len(order),
                            'c0-v3-compact-selection-position-and-sort-key-scratch'))
                        self.selected.append(index)
                    continue
                loan = _c0_v3_original(self.owner, self.directory, record)
                try:
                    def select_one():
                        row = loan.value
                        if not all(row[k] == v and row[k] is not None and v is not None for k, v in equality):
                            return False
                        amount = sum(4 * _c0_v3_text_size(row[k]) if type(row[k]) is str else len(row[k]) if type(row[k]) is bytes else 64 for k, _ in order)
                        self.leases.append(self.owner.memory.hold(512 + amount + 128 * len(order), 'c0-v3-ordered-query-selection'))
                        return tuple(row[k] for k, _ in order)
                    ordered = select_one()
                    if ordered is not False:
                        # Unknown-column semantics were read from the real Row;
                        # retain only its explicitly prepaid order projection.
                        self.selected.append((index, ordered))
                    ordered = None
                except BaseException:
                    loan.fail(); raise
                loan.close()
            if safe:
                for position in reversed(range(len(order))):
                    field = self.directory.fields.index(order[position][0])
                    self.selected.sort(key=lambda index: self.directory.records[index][3][field], reverse=order[position][1])
            else:
                for position in reversed(range(len(order))):
                    self.selected.sort(key=lambda entry: entry[1][position], reverse=order[position][1])
                self.selected = [entry[0] for entry in self.selected]
            if limit is not None:
                self.selected = self.selected[:limit]

    def __iter__(self):
        return self

    def __next__(self):
        if self.closed:
            raise StopIteration
        self.owner.check()
        if self.previous is not None and not self.previous.closed:
            raise _LifecycleLiveLimit('close the previous original Row loan before advancing')
        count = len(self.directory.records) if self.selected is None else len(self.selected)
        while self.position < count:
            index = self.position if self.selected is None else self.selected[self.position]
            self.position += 1
            record = self.directory.records[index]
            if self.selected is None and all(type(k) is str and k in self.directory.fields for k, _ in self.equality):
                values = record[3]
                if not all(values[self.directory.fields.index(k)] == v
                    and values[self.directory.fields.index(k)] is not None and v is not None for k, v in self.equality):
                    continue
            loan = _c0_v3_original(self.owner, self.directory, record)
            try:
                def project_one():
                    row = loan.value
                    if self.selected is None and not all(row[k] == v and row[k] is not None and v is not None for k, v in self.equality):
                        return False
                    if self.columns is not None:
                        loan.value = tuple(row[k] for k in self.columns)
                    return True
                matched = project_one()
            except BaseException:
                loan.fail(); raise
            if not matched:
                loan.close(); continue
            self.previous = loan
            return loan
        # StopIteration itself can retain this frame. Retire descriptor/raw
        # aliases before closing the iterator's decoding scratch.
        record = values = loan = project_one = None
        self.close()
        raise StopIteration

    def close(self):
        if self.closed:
            return
        if self.previous is not None and not self.previous.closed:
            raise _LifecycleLiveLimit('iterator still has a borrowed Row')
        self.previous = self.selected = self.directory = None
        self.material = None
        for lease in reversed(self.leases):
            lease.release()
        self.leases.clear()
        self.closed = True


def _c0_iter_borrows(material, table, equality=(), columns=None, order=()):
    owner = _c0_v3_material(material)
    if owner is None:
        return _C0V2BorrowIterator(_c0_read(material, table, equality, columns, order))
    # These are the original semantic read pre/post gates. Iterator and
    # native acquisition checks have a separate role and are not replayed here.
    material['check']()
    try:
        iterator = _C0V3BorrowIterator(material, table, equality, columns, order, _owner=owner)
        material['check']()
        return iterator
    except BaseException:
        owner.fail()
        raise


class _C0V2BorrowIterator:
    def __init__(self, rows):
        self.rows = iter(rows)
    def __iter__(self):
        return self
    def __next__(self):
        return _C0V3Loan(None, next(self.rows))
    def close(self):
        self.rows = iter(())


def _c0_borrow(material, table, equality=(), columns=None, order=(), limit=None, first=False):
    owner = _c0_v3_material(material)
    if owner is None:
        return _C0V3Loan(None, _c0_read(material, table, equality, columns, order, limit, first))
    # This API preserves bounded list/first/tuple semantics. Streaming consumers
    # use iter_borrows; an unbounded list is admitted only with all Rows prepaid.
    material['check']()
    try:
        iterator = _C0V3BorrowIterator(material, table, equality, columns, order, prepare=True, limit=limit, _owner=owner)
    except BaseException:
        owner.fail()
        raise
    result = _C0V3Loan(owner, [])
    result.leases.append(owner.memory.hold(4096, 'c0-v3-read-list-carrier'))
    try:
        for loan in iterator:
            result.leases.append(owner.memory.hold(32, 'c0-v3-result-row-slot'))
            result.value.append(loan.value)
            result.leases.extend(loan.leases)
            loan.leases = []
            loan.value = None
            loan.close()
        iterator.close()

        if first:
            result.value = result.value[0] if result.value else None
        material['check']()
        return result
    except BaseException:
        result.fail()
        raise


class _LifecycleMaterialReader:
    def __init__(self, material):
        self.material = material
        self.owner = _c0_v3_material(material)
        if self.owner is None:
            unproven()
        self.memory = self.owner.memory
        self.generation = self.owner.generation

    def check_source(self):
        self.check()
        self.owner.capsule.check_source()
        self.check()

    def check(self):
        if (self.material is not self.owner.material or self.memory is not self.owner.memory
                or self.generation is not self.owner.generation):
            unproven()
        self.owner.check()

    def borrow(self, table, equality=(), columns=None, order=(), limit=None, first=False):
        self.check()
        if table not in _LIFECYCLE_TABLES:
            unproven()
        if table not in self.material['tables']:
            self.material['check'](); self.material['check']()
            return _C0V3Loan(self.owner, None if first else [],
                leases=(self.memory.hold(1024, 'c0-v3-absent-table-result-carrier'),))
        return _c0_borrow(self.material, table, equality, columns, order, limit, first)

    def __call__(self, table, equality=(), columns=None, order=(), limit=None, first=False):
        return _c0_v3_adopt(self.material, self.borrow(table, equality, columns, order, limit, first))


def _c0_v3_each(material, table, equality, columns, order, callback):
    iterator = _c0_iter_borrows(material, table, equality, columns, order)
    try:
        for loan in iterator:
            try:
                _c0_v3_invoke(material, callback, loan.value)
            except BaseException:
                loan.fail(); raise
            loan.close()
    finally:
        # On a failed loan, preserve original primary/traceback, not a close error.
        if iterator.previous is None or iterator.previous.closed:
            iterator.close()


def _p04_material_current_v3(material, state):
    if '_p04' not in state:
        return
    scope = _c0_query_context.get()
    plan = scope[3] if scope is not None and material is scope[1] else None
    if type(plan) is _C0QueryPlan:
        return plan.p04_project(material, state)
    _p04_current_projection(_lifecycle_material_read(material), state)

def _c0_latest_v3(material):
    scope = _c0_query_context.get()
    plan = scope[3] if scope is not None and material is scope[1] else None
    if type(plan) is _C0QueryPlan:
        try:
            material['check']()
            edge = plan.snapshot_edge(material, True)
            if edge is not None:
                if plan.p04_latest is None or plan.p04_latest[0] is not edge:
                    plan.p04_latest = (edge, _c0_v3_json(material, edge['state']))
                from copy import deepcopy
                result = _c0_v3_copy(material, plan.p04_latest[1])
                material['check']()
                return result
        except BaseException:
            plan.close()
            raise
    row = _c0_read(material, 'main.snapshots', columns=('state',), order=(('seq', True),), limit=1, first=True)
    if row is None:
        unproven()
    return _c0_v3_json(material, row[0])

def _c0_fault_schema_v3(material, *, create=False, authorize=False):
    if create:
        unproven()
    if not _c0_exists(material, _ISSUER_TABLE):
        return False

    def sql(name):
        r = _c0_read(material, 'main.sqlite_master', (('name', name),), ('sql',), first=True)
        return r[0] if r else None
    if sql(_ISSUER_TABLE) != _ISSUER_DDL:
        unproven()
    for event in ('UPDATE', 'DELETE'):
        expected = 'CREATE TRIGGER fault_issuer_no_' + event.lower() + ' BEFORE ' + event + ' ON ' + _ISSUER_TABLE + " BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"
        if sql('fault_issuer_no_' + event.lower()) != expected:
            unproven()
    found = sql('fault_issuer_no_replace')
    if found is None and authorize or (found is not None and found != _ISSUER_INSERT):
        unproven()
    return True

def _c0_m1_schema_v3(material):
    from .coordinator_maintenance import _TABLE, _DDL, _INSERT, fail
    if not _c0_exists(material, _TABLE):
        return False

    def sql(name):
        r = _c0_read(material, 'main.sqlite_master', (('name', name),), ('sql',), first=True)
        return r[0] if r else None
    if sql(_TABLE) != _DDL:
        fail()
    for event in ('UPDATE', 'DELETE'):
        expected = 'CREATE TRIGGER technical_maintenance_no_' + event.lower() + ' BEFORE ' + event + ' ON ' + _TABLE + " BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"
        if sql('technical_maintenance_no_' + event.lower()) != expected:
            fail()
    if sql('technical_maintenance_no_replace') != _INSERT:
        fail()
    return True

def _c0_native_audit_v3(storage, material, row):
    from .actor_execution import ActorRunError
    current = _c0_latest(material)
    _p04_material_current(material, current)
    person = row['person_id']
    if _c0_exists(material, 'actor_session_heads', 'actor') and _c0_read(material, 'actor.actor_session_heads', (('person_id', person),), first=True):
        storage._material_audit(material, person)
        return
    untouched = row['phase'] == 'pending' or (row['phase'] == 'technical_blocked' and row['resume_phase'] == 'pending')
    if not untouched or any((_c0_exists(material, table, 'actor') and _c0_read(material, 'actor.' + table, (('person_id', person),), first=True) for table in ('actor_intents', 'actor_calls', 'actor_intent_execution'))):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
    session = _c0_read(material, 'actor.actor_sessions', (('person_id', person),), ('messages', 'seen'), first=True)
    if session is not None and any((_c0_v3_json(material, raw) != [] for raw in session)):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')

def _c0_window_record_uncached_v3(material, world_id, window_id):
    from .world import WorldStore, _lc_history_scope
    internal_uuid(window_id)
    from copy import deepcopy
    row = _c0_read(material, 'main.coordinator_windows', (('world_id', (world_id, window_id)[0]), ('window_id', (world_id, window_id)[1])), None, (), None, True)
    if row is None:
        unproven()
    result = dict(row)
    try:
        result['roster'] = _c0_v3_json(material, result['roster_json'])
        roster = result['roster']
        if type(roster) is not list or any((type(p) is not str or not p for p in roster)) or len(set(roster)) != len(roster) or (_c0_v3_hash(material, roster) != result['roster_sha256']) or (result['state'] not in ('open', 'completed')) or (result['boundary_kind'] not in ('normal', 'horizon')):
            unproven()
        for key in ('ordinal', 'base_revision', 'advanced_revision', 'created_parent_epoch', 'owner_parent_epoch'):
            if type(result[key]) is not int or not 1 <= result[key] <= MAX_INT:
                unproven()
        for key in ('target_time', 'horizon_at_open'):
            if type(result[key]) is not int or not 0 <= result[key] <= MAX_INT:
                unproven()
        if result['advanced_revision'] != result['base_revision'] + 1 or result['owner_parent_epoch'] < result['created_parent_epoch'] or result['horizon_at_open'] < result['target_time']:
            unproven()
        if result['completed_revision'] is not None:
            integer(result['completed_revision'], 1)
        advanced = _c0_read(material, 'main.snapshots', (('seq', (result['advanced_revision'],)[0]),), ('state',), (), None, True)
        base = _c0_read(material, 'main.snapshots', (('seq', (result['base_revision'],)[0]),), ('state',), (), None, True)
        if advanced is None or base is None:
            unproven()
        advanced = _c0_v3_json(material, advanced[0])
        base = _c0_v3_json(material, base[0])
        current = _c0_latest(material)
        if advanced['world_id'] != world_id or base['world_id'] != world_id or current['world_id'] != world_id or (advanced['revision'] != result['advanced_revision']) or (advanced['time'] != result['target_time']) or (advanced['horizon'] != result['horizon_at_open']) or (result['target_time'] != WorldStore._p04_target(base)):
            unproven()
        genesis = _c0_v3_json(material, _c0_read(material, 'main.snapshots', (), ('state',), (('seq', False),), 1, True)[0])

        def identities(snapshot):
            return [(p['id'], p['country_id'], p['office']) for p in snapshot['persons']]
        from .world import _lc_optin
        lifecycle_read = _lifecycle_material_read(material) if _lc_optin(genesis) else None
        with _lc_history_scope(lifecycle_read, genesis):
            if lifecycle_read is None:
                if identities(current) != identities(genesis) or identities(advanced) != identities(genesis):
                    unproven()
            else:
                for historical in (base, advanced, current):
                    _lifecycle_snapshot_proof(lifecycle_read, historical, genesis)
            horizon = result['boundary_kind'] == 'horizon'
            WorldStore._check_frozen_p04(advanced, genesis)
            WorldStore._check_frozen_p04(current, genesis)
            _p04_material_current(material, current)
            expected = WorldStore._p04_expected(base, advanced, genesis=genesis)
            if '_p04' in advanced and advanced['_p04']['dispatch'] != expected:
                unproven()
            if roster != expected or horizon != (advanced['time'] == advanced['horizon']) or (horizon and (advanced['status'] != 'horizon_paused' or result['state'] != 'completed')) or (not set(roster) <= {p['id'] for p in current['persons']}):
                unproven()
            activations = _c0_read(material, 'main.coordinator_activations', (('world_id', (world_id, window_id)[0]), ('window_id', (world_id, window_id)[1])), None, (('roster_index', False),), None, False)
            if len(activations) != len(roster):
                unproven()
            for index, activation in enumerate(activations):
                internal_uuid(activation['activation_id'])
                integer(activation['roster_index'])
                integer(activation['private_closed'])
                for key in ('created_parent_epoch', 'owner_parent_epoch'):
                    integer(activation[key], 1)
                for key in ('observation_revision', 'card_version', 'base_session_version', 'promoted_version', 'owner_actor_epoch'):
                    if activation[key] is not None:
                        integer(activation[key], 0 if key == 'base_session_version' else 1)
                if activation['world_id'] != world_id or activation['window_id'] != window_id or activation['created_parent_epoch'] != result['created_parent_epoch'] or (activation['owner_parent_epoch'] < activation['created_parent_epoch']) or (activation['observation_revision'] is not None and (not result['advanced_revision'] <= activation['observation_revision'] <= current['revision'])) or (activation['promoted_version'] is not None and activation['base_session_version'] is not None and (activation['promoted_version'] != activation['base_session_version'] + 1)):
                    unproven()
                if lifecycle_read is not None and activation['observation_revision'] is not None:
                    _lifecycle_activation_identity(lifecycle_read, genesis, activation)
                if activation['intent_id'] is not None:
                    internal_uuid(activation['intent_id'])
                if activation['roster_index'] != index or activation['person_id'] != roster[index] or activation['phase'] not in ('pending', 'prepared', 'candidate', 'world_committed', 'completed', 'technical_blocked', 'rejected', 'skipped') or (activation['private_closed'] not in (0, 1)) or (activation['phase'] == 'pending' and (activation['intent_id'] is not None or activation['private_closed'] != 0)):
                    unproven()
            if result['state'] == 'completed':
                completion = result['completed_revision']
                if type(completion) is not int or not result['advanced_revision'] <= completion <= current['revision'] or any((a['observation_revision'] is not None and (a['observation_revision'] > completion or (a['observation_revision'] == completion and a['phase'] != 'rejected')) for a in activations)) or any((a['phase'] not in ('completed', 'rejected', 'skipped') or a['private_closed'] != 1 for a in activations)):
                    unproven()
                completed = _c0_read(material, 'main.snapshots', (('seq', (completion,)[0]),), ('state',), (), None, True)
                if completed is None:
                    unproven()
                completed = _c0_v3_json(material, completed[0])
                if lifecycle_read is not None:
                    _lifecycle_snapshot_proof(lifecycle_read, completed, genesis)
                if completed['world_id'] != world_id or completed['revision'] != completion or completed['time'] != result['target_time']:
                    unproven()
            elif result['completed_revision'] is not None:
                unproven()
    except (KeyError, ValueError, TypeError, IndexError):
        unproven()
    return _c0_v3_copy(material, result)

def _c0_phase_proof_v3(world, material, row):
    try:
        return _c0__phase_proof(world, material, row)
    except (ValueError, TypeError, KeyError, IndexError):
        unproven()

def _c0__phase_proof_v3(world, material, row):
    if row['phase'] == 'technical_blocked':
        _c0_block_proof(world, material, row)
        return row
    if row['phase'] == 'completed' and _c0_exists(material, 'coordinator_activation_blocks') and _c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True):
        from .coordinator_maintenance import completed_proof
        _c0_completed_proof(world, material, row)
        return row
    return _c0_material_proof(world, material, row, row['phase'])

def _c0_material_proof_v3(world, material, row, phase):
    """Non-recursive original material proof; never grants execution authority."""
    from .actor_execution import digest
    person = row['person_id']
    if phase not in ('pending', 'prepared', 'candidate', 'world_committed', 'completed', 'rejected'):
        unproven()
    if phase != 'rejected':
        fields = ('terminal_reason',) if row['phase'] == 'technical_blocked' else ('terminal_reason', 'resume_phase', 'diagnostic_code')
        if any((row[k] is not None for k in fields)):
            unproven()
        saved = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), ('kind',), (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
        if phase not in ('world_committed', 'completed') and (saved is not None or row['receipt_sha256'] is not None):
            unproven()
    if phase == 'rejected':
        _c0_rejection_proof(world, material, row)
        return row
    if phase == 'pending':
        if any((row[k] is not None for k in ('intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'promoted_version'))):
            unproven()
        return row
    private = _c0_read(material, 'actor.actor_intents', (('intent_id', (row['intent_id'], person)[0]), ('person_id', (row['intent_id'], person)[1])), None, (), None, True)
    meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
    if private is None or meta is None or _c0_v3_digest(material, private['request']) != row['request_sha256'] or (meta['base_version'] != row['base_session_version']):
        unproven()
    request = _c0_v3_json(material, private['request'])
    if type(request) is not dict or request.get('person_id') != person or request.get('intent_id') != row['intent_id'] or (not _c0_v3_body_matches(material, request, _c0_v3_json(material, private['body']), private['candidate'])) or (meta['person_id'] != person) or (meta['world_id'] != row['world_id']):
        unproven()
    if phase == 'prepared':
        if private['state'] != 'prepared' or private['candidate'] is not None:
            unproven()
    elif private['candidate'] is None or _c0_v3_digest(material, private['candidate']) != row['candidate_sha256'] or private['state'] != ('committed' if phase == 'completed' else 'candidate'):
        unproven()
    actual = _c0_read(material, 'main.intents', (('id', (row['intent_id'],)[0]),), ('person', 'receipt'), (), None, True)
    if phase in ('world_committed', 'completed'):
        outcome = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
        if actual is None or actual[0] != person or actual[1] is None or (_c0_v3_digest(material, actual[1]) != row['receipt_sha256']) or (outcome is None) or (outcome['kind'] != 'committed') or (outcome['intent_id'] != row['intent_id']) or (_c0_v3_digest(material, outcome['outcome_json']) != outcome['outcome_sha256']) or (_c0_v3_json(material, outcome['outcome_json']) != _c0_v3_json(material, actual[1])):
            unproven()
        receipt = _c0_v3_json(material, actual[1])
        if type(receipt) is not dict or receipt.get('status') != 'committed' or receipt.get('world_id') != row['world_id'] or (receipt.get('person_id') != person) or (receipt.get('intent_id') != row['intent_id']):
            unproven()
    elif actual is not None and actual[1] is not None:
        unproven()
    if phase == 'completed':
        if row['private_closed'] != 1 or private['state'] != 'committed' or private['receipt'] is None or (_c0_v3_digest(material, private['receipt']) != row['receipt_sha256']) or (meta['promoted_version'] != row['promoted_version']) or (row['promoted_version'] != meta['base_version'] + 1):
            unproven()
    elif row['private_closed'] != 0 or meta['promoted_version'] is not None:
        unproven()
    return row

def _c0_block_payload_v3(material, row, resume):
    saved = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), ('outcome_sha256',), (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
    return dict(schema_version=1, **{k: row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'base_session_version')}, resume_phase=resume, outcome_sha256=saved[0] if saved else None, private_closed=0)

def _c0_block_proof_v3(world, material, row):
    _c0_block_witness_proof(world, material, row)
    _c0_material_proof(world, material, row, row['resume_phase'])
    return row

def _c0_block_witness_proof_v3(world, material, row):
    from .actor_execution import canonical, digest
    if row['resume_phase'] not in ('pending', 'prepared', 'candidate', 'world_committed', 'rejected') or row['private_closed'] != 0 or row['promoted_version'] is not None or (type(row['diagnostic_code']) is not str) or (re.fullmatch('[A-Z][A-Z0-9_]{0,79}', row['diagnostic_code']) is None) or (not _c0_exists(material, 'coordinator_activation_blocks')):
        unproven()
    if not _c0_exists(material, 'actor_activation_delegations') or not _c0_exists(material, 'actor_coordinator_delegations'):
        unproven()
    binding = _c0_read(material, 'main.actor_activation_delegations', (('world_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[2])), ('window_id', 'activation_id'), (), None, True)
    parent = _c0_read(material, 'main.actor_coordinator_delegations', (('world_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], row['owner_actor_epoch'])[2])), ('parent_epoch',), (), None, True)
    if binding is None or tuple(binding) != (row['window_id'], row['activation_id']) or parent is None or (parent[0] != row['owner_parent_epoch']):
        unproven()
    if row['resume_phase'] != 'pending':
        integer(row['observation_revision'], 1)
        integer(row['card_version'], 1)
        integer(row['base_session_version'])
    if row['resume_phase'] == 'pending' and any((row[k] is not None for k in ('intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'promoted_version', 'observation_revision', 'card_version', 'base_session_version'))):
        unproven()
    if row['resume_phase'] == 'prepared' and any((row[k] is not None for k in ('candidate_sha256', 'receipt_sha256', 'promoted_version'))):
        unproven()
    saved = _c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True)
    if saved is None or saved['resume_phase'] != row['resume_phase'] or saved['diagnostic_code'] != row['diagnostic_code'] or (type(saved['pause_revision']) is not int) or (saved['pause_revision'] < 1) or (saved['proof_json'] != _c0_v3_canonical(material, _c0_block_payload(material, row, row['resume_phase']))) or (saved['proof_sha256'] != _c0_v3_digest(material, saved['proof_json'])):
        unproven()
    pause = _c0_read(material, 'main.snapshots', (('seq', (saved['pause_revision'],)[0]),), ('state',), (), None, True)
    if pause is None:
        unproven()
    pause = _c0_v3_json(material, pause[0])
    window = _c0_window_record(material, row['world_id'], row['window_id'])
    if pause['world_id'] != row['world_id'] or pause['revision'] != saved['pause_revision'] or pause['time'] != window['target_time'] or (pause['status'] != 'technical_paused') or (pause.get('diagnostic') != {'code': row['diagnostic_code'], 'time': window['target_time']}) or (saved['pause_revision'] < (row['observation_revision'] or window['advanced_revision'])):
        unproven()
    _c0_fault_issuer_proof(world, material, row, required=False)
    return row

def _c0_fault_issuer_proof_v3(world, material, row, *, required=True):
    """Original pause-time authority, never minted from maintenance-time metadata."""
    from .actor_execution import canonical, digest
    found = _c0_read(material, 'main.coordinator_fault_issuers', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True) if _c0_fault_schema(material, authorize=required) else None
    if found is None:
        if required:
            unproven()
        return None
    witness = decode_fault_issuer(found['witness_json'])
    if found['world_id'] != row['world_id'] or found['activation_id'] != row['activation_id'] or found['witness_sha256'] != _c0_v3_digest(material, found['witness_json']):
        unproven()
    block = _c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True)
    fault = witness['binding']
    integer(fault['owner_actor_epoch'], 1)
    integer(fault['owner_parent_epoch'], 1)
    parent = _c0_read(material, 'main.actor_coordinator_delegations', (('world_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[2])), ('parent_epoch', 'parent_owner_token'), (), None, True)
    delegation = _c0_read(material, 'main.actor_activation_delegations', (('world_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[0]), ('person_id', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[1]), ('actor_epoch', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])[2])), ('window_id', 'activation_id'), (), None, True)
    meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
    if block is None or parent is None or meta is None or (delegation is None) or any((fault[k] != row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id'))) or (tuple(delegation) != (row['window_id'], row['activation_id'])) or (parent[0] != fault['owner_parent_epoch']) or (witness['pause_revision'] != block['pause_revision']) or (witness['block_sha256'] != block['proof_sha256']) or (witness['parent'] != list(parent)) or (witness['meta'] != {k: meta[k] for k in meta.keys() if k != 'promoted_version'}) or (meta['owner_epoch'] != fault['owner_actor_epoch']):
        unproven()
    attempt = world._scoped_attempt()
    if attempt is not None and attempt.permit.epoch == fault['owner_actor_epoch']:
        if attempt.permit.world_id != row['world_id'] or attempt.permit.person_id != row['person_id'] or attempt.permit.owner_token != meta['owner_token'] or (attempt.id != meta['attempt_id']) or (attempt.activation != scoped(world)) or (attempt.coordinator is None) or ([attempt.coordinator.epoch, attempt.coordinator.owner_token] != witness['parent']):
            unproven()
        fault_issuer_scope(world, row, attempt.permit)
    return witness

def _c0_rejection_proof_v3(world, material, row):
    """Row-level proof only; callers separately run the complete native audit.

    Keeping this non-recursive lets the audit prove historical rejected leaves.
    """
    from .actor_execution import canonical, digest
    blocked = row['phase'] == 'technical_blocked' and row['resume_phase'] == 'rejected' and (row['private_closed'] == 0)
    nulls = ('receipt_sha256', 'promoted_version') if blocked else ('receipt_sha256', 'promoted_version', 'resume_phase', 'diagnostic_code')
    if row['phase'] != 'rejected' and (not blocked) or row['private_closed'] not in (0, 1) or any((row[k] is not None for k in nulls)):
        unproven()
    private = _c0_read(material, 'actor.actor_intents', (('intent_id', (row['intent_id'], row['person_id'])[0]), ('person_id', (row['intent_id'], row['person_id'])[1])), None, (), None, True)
    meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
    actual = _c0_read(material, 'main.intents', (('id', (row['intent_id'],)[0]),), None, (), None, True)
    saved = _c0_read(material, 'main.coordinator_activation_outcomes', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True) if _c0_exists(material, 'coordinator_activation_outcomes') else None
    if private is None or meta is None or actual is None or (saved is None) or (private['state'] != ('rejected' if row['private_closed'] else 'candidate')) or (private['receipt'] is not None) or (actual['receipt'] is not None) or (actual['person'] != row['person_id']) or (meta['person_id'] != row['person_id']) or (meta['world_id'] != row['world_id']) or (meta['base_version'] != row['base_session_version']) or (meta['promoted_version'] is not None) or (private['candidate'] is None) or (_c0_v3_digest(material, private['request']) != row['request_sha256']) or (_c0_v3_digest(material, private['candidate']) != row['candidate_sha256']) or (saved['kind'] != 'domain_rejected') or (saved['intent_id'] != row['intent_id']) or (_c0_v3_digest(material, saved['outcome_json']) != saved['outcome_sha256']):
        unproven()
    result = _c0_v3_json(material, saved['outcome_json'])
    metrics = _c0_v3_json(material, private['metrics'])
    if type(metrics) is not dict or set(metrics) != {'sdk_events', 'sdk_requests', 'provider', 'pid', 'module_root'} or type(metrics['sdk_requests']) is not int or (metrics['sdk_requests'] < 0) or (type(metrics['pid']) is not int) or (metrics['pid'] < 0) or any((type(metrics[k]) is not str or not metrics[k] for k in ('provider', 'module_root'))):
        unproven()
    events = metrics['sdk_events']
    if not (type(events) is int and events >= 0 or (type(events) is dict and all((type(k) is str and type(v) is int and (v >= 0) for k, v in events.items())))):
        unproven()
    fields = {'status', 'world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'code', 'origin', 'action', 'decision_version', 'effect_index', 'request_sha256', 'candidate_sha256', 'payload_sha256', 'decision_sha256', 'effects'}
    request = _c0_v3_json(material, private['request'])
    payload = _c0_v3_json(material, actual['payload'])
    body = _c0_v3_json(material, private['body'])
    if type(result) is not dict or set(result) != fields or saved['outcome_json'] != _c0_v3_canonical(material, result) or (request.get('person_id') != row['person_id']) or (request.get('intent_id') != row['intent_id']) or (not _c0_v3_body_matches(material, request, body, private['candidate'])) or (payload.get('body') != body) or any((result[k] != row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'request_sha256', 'candidate_sha256'))) or (result['status'] != 'domain_rejected') or (result['effects'] != []) or (result['payload_sha256'] != _c0_v3_digest(material, actual['payload'])) or (result['code'] != row['terminal_reason']) or (result['action'] not in DOMAIN_ACTIONS.get(result['code'], set())) or (result['origin'] not in ('world.apply_action', 'adjudication.execute')):
        unproven()
    world._validate_body(body)
    if result['origin'] == 'world.apply_action':
        if set(payload) != {'body', 'ruling', 'reason'} or payload['ruling'] not in ('大失败', '失败', '成功', '大成功') or type(payload['reason']) is not str or (not payload['reason'].strip()) or (len(payload['reason']) > 8000) or (result['action'] != body['action']) or any((result[k] is not None for k in ('decision_version', 'effect_index', 'decision_sha256'))):
            unproven()
    else:
        if set(payload) != {'body'}:
            unproven()
        version = result['decision_version']
        index = result['effect_index']
        if type(version) is not int or version < 1 or type(index) is not int or (index < 0) or (body['action'] != 'propose'):
            unproven()
        proposal = _c0_read(material, 'main.dm_proposals', (('intent_id', (row['intent_id'],)[0]),), ('world_id', 'person_id', 'text'), (), None, True)
        decision = _c0_read(material, 'main.dm_decisions', (('intent_id', (row['intent_id'],)[0]),), ('version', 'data'), (('version', True),), 1, True)
        if proposal is None or tuple(proposal) != (row['world_id'], row['person_id'], body['text']) or decision is None or (decision[0] != version) or (_c0_v3_digest(material, decision[1]) != result['decision_sha256']):
            unproven()
        data = _c0_v3_json(material, decision[1])
        if data['world_id'] != row['world_id'] or type(data['effects']) is not list or (not 0 <= index < len(data['effects'])) or (data['effects'][index]['action'] != result['action']):
            unproven()
        from .adjudication import Adjudicator
        from types import SimpleNamespace
        if any((body.get('action') in _LIFECYCLE_ACTIONS for body in data['effects'])):
            read = _lifecycle_material_read(material)
            compiled = _lifecycle_compile(world, data, read)
            _lifecycle_rejection(read, world, row, result, data)
        else:
            compiled = Adjudicator._compile(SimpleNamespace(world=world), data)
        for i, effect in enumerate(compiled):
            if _c0_read(material, 'main.intents', (('id', (Adjudicator._child_id(row['intent_id'], version, i),)[0]),), None, (), None, True):
                unproven()
        if _c0_read(material, 'main.dm_timeline', (('intent_id', (row['intent_id'],)[0]), ('kind', 'commit')), None, (), None, True):
            unproven()
    return result

def _c0_completed_proof_v3(world, material, row):
    from .actor_execution import digest
    from .coordinator_maintenance import fail
    'Re-prove original witness/issuer without pretending private is unpromoted.'
    if not _c0_m1_schema(material):
        fail()
    saved = _c0_read(material, 'main.coordinator_technical_maintenance', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True)
    if saved is None or saved['schema_version'] != 1 or _c0_v3_digest(material, saved['record_json']) != saved['record_sha256']:
        fail()
    record = _c0_v3_json(material, saved['record_json'])
    old = record['original_row']
    if old['phase'] != 'technical_blocked' or old['resume_phase'] != 'world_committed':
        fail()
    changed = {'phase', 'private_closed', 'promoted_version', 'resume_phase', 'diagnostic_code'}
    if set(old) != set(row.keys()) or any((old[k] != row[k] for k in old if k not in changed)):
        fail()
    _c0_block_witness_proof(world, material, old)
    if _c0_fault_issuer_proof(world, material, old) != record.get('original_fault_issuer'):
        fail()
    block = dict(_c0_read(material, 'main.coordinator_activation_blocks', (('world_id', (row['world_id'], row['activation_id'])[0]), ('activation_id', (row['world_id'], row['activation_id'])[1])), None, (), None, True))
    if block != record['original_block'] or record['operation_id'] != saved['operation_id']:
        fail()
    _c0_material_proof(world, material, row, 'completed')
    return record

@contextmanager
def _c0_v3_material_scope(material):
    owner = _c0_v3_material(material)
    if owner is None:
        yield
        return
    destination = _lifecycle_value_context.get()
    if (type(destination) is not _LifecycleValueOwner or destination.memory is not owner.memory
            or destination.closed or destination.failed):
        owner.fail()
        raise _LifecycleLiveLimit('C0 V3 requires the active same-operation result owner')
    token = _lifecycle_live_context.set(owner.memory)
    try:
        yield
        if owner.memory.failed:
            raise _LifecycleLiveLimit('C0 material has retained exceptional debt')
        # Only the complete material consumer's independent result loans may
        # escape this boundary. No exports.clear() or result-copy refund.
        while owner.exports:
            loan = owner.exports[0]
            loan.transfer(destination)
            del owner.exports[0]
        owner.close()
    except BaseException:
        owner.fail()
        destination.fail()
        raise
    finally:
        _lifecycle_live_context.reset(token)


def _c0_fullproof_v3(world, storage, material, window_id):
    material['check']()
    current = _c0_latest(material)
    _c0_window_record(material, current['world_id'], window_id)
    def activation(row):
        material['check']()
        if row['phase'] == 'skipped':
            unproven()
        ref = scoped(world, ActivationRef(row['world_id'], row['window_id'], row['activation_id'], row['person_id']))
        if ref.world_id != current['world_id']:
            raise WorldError('ACTIVATION_BINDING_CONFLICT', '激活人物或世界不符')
        _c0_window_record(material, ref.world_id, ref.window_id)
        _c0_phase_proof(world, material, row)
        _c0_native_audit(storage, material, row)
    _c0_v3_each(material, 'main.coordinator_activations', (('window_id', window_id),), None, (), activation)
    material['check']()
    return True


def _open_history_proof_v3(capsule, material):
    world = capsule.world
    state = _c0_latest(material)
    _p04_material_current(material, state)
    progress = dict(previous=None, count=0)
    def window_edge(row):
        capsule.check()
        ordinal = progress['count'] + 1
        window = _c0_window_record(material, state['world_id'], row[0])
        base_row = _c0_read(material, 'main.snapshots', (('seq', window['base_revision']),), ('state',), first=True)
        if base_row is None:
            unproven()
        base = _c0_v3_json(material, base_row[0])
        previous = progress['previous']
        if (window['ordinal'] != ordinal
                or (previous is not None and (previous['state'] != 'completed'
                    or window['base_revision'] < previous['completed_revision']))
                or base['time'] != (previous['target_time'] if previous else 0)):
            unproven()
        # Only the small lineage frontier crosses an iteration, not historical roster/material.
        progress['previous'] = dict(state=window['state'], completed_revision=window['completed_revision'],
                                    target_time=window['target_time'])
        progress['count'] = ordinal
    _c0_v3_each(material, 'main.coordinator_windows', (('world_id', state['world_id']),),
        ('window_id',), (('ordinal', False),), window_edge)
    previous = progress['previous']
    if state['time'] != (previous['target_time'] if previous else 0):
        unproven()
    def prove_window(row):
        _c0_fullproof(world, capsule.storage, material, row[0])
    _c0_v3_each(material, 'main.coordinator_windows', (('world_id', state['world_id']),),
        ('window_id',), (('ordinal', False),), prove_window)
    # Preserve the full last-window result required by the actual native seal.
    latest_window = _c0_read(material, 'main.coordinator_windows', (('world_id', state['world_id']),),
                             ('window_id',), (('ordinal', True),), 1, True)
    previous = _c0_window_record(material, state['world_id'], latest_window[0]) if latest_window is not None else None
    records = _c0_read(material, 'main.coordinator_scheduler_registration', (('world_id', state['world_id']),))
    if len(records) != 1:
        unproven()
    capsule.check()
    return state, previous, progress['count'], dict(records[0])


def _c0_v3_next_activation_seal(world, capsule, material, window_id, coordinator):
    owner = _c0_v3_material(material)
    # Decode only outside the acquisition/source lock. The fresh actual native
    # Row must match this very generation's original payload before root SQL.
    expected = _c0_read(material, 'main.snapshots', (), ('state',), (('seq', True),), 1, True)
    if expected is None:
        unproven()
    state = _c0_v3_json(material, expected[0])
    with _c0_segment(capsule, _C0Fresh(capsule)) as db:
        capsule.check_source(); capsule.pin(db)
        current_loan, _ = _c0_v3_fetch(db, 'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1', (), owner)
        current = _c0_v3_adopt(material, current_loan)
        if current is None or current[0] != expected[0]:
            unproven()
        world._check_coordinator_execution(db, state['world_id'], coordinator, readonly=True)
        registration(db, state['world_id'], capsule.identity)
        activation_loan, _ = _c0_v3_fetch(db,
            "SELECT * FROM coordinator_activations WHERE world_id=? AND window_id=? AND NOT (phase IN ('completed','rejected','skipped') AND private_closed=1) ORDER BY roster_index LIMIT 1",
            (state['world_id'], window_id), owner)
        row = _c0_v3_adopt(material, activation_loan)
        capsule.storage.paths()
        world._check_coordinator_execution(db, state['world_id'], coordinator, readonly=True)
        capsule.pin(db)
        # Exact original last fresh source gate: no SQL follows it.
        capsule.check_source()
        if capsule.consumed:
            unproven()
        capsule.consumed = True
        result = dict(row) if row is not None else None
    capsule.check()
    return result


def _lifecycle_native_call(read, function, *args, export=False, **kwargs):
    """Nested native scratch ends after the full consumer frame returns."""
    from .world import _lc_value_memory
    memory=_lc_value_memory(read)
    if memory is None:return function(*args,**kwargs)
    outer=_lifecycle_value_context.get()
    if outer is None or outer.memory is not memory:
        raise _LifecycleLiveLimit('native consumer needs its operation value owner')
    region=_LifecycleValueOwner(memory)
    region.reserve(16384,'native-inner-frame-and-builtin-temporaries')
    token=_lifecycle_value_context.set(region)
    result=loan=None
    try:
        result=function(*args,**kwargs)
        if export:
            loan=_lifecycle_copy_borrow(result,memory=memory,role='native-nested-result')
    except BaseException:
        region.fail()
        raise
    finally:
        _lifecycle_value_context.reset(token)
    result=None
    region.close()
    return outer.take(loan) if export else None


def _window_prefix_probe(db, route):
    """Paid, non-authorizing lexical domain probe; never strict-load genesis.

    Observe all duplicate/escaped marker keys, including overwritten _config.
    True selects the unchanged native admission. None is unknown, not legacy.
    This frame returns None so invoke closes its scratch before native admission.
    """
    owner = _lifecycle_value_context.get()
    if owner is None or owner.closed or owner.failed:
        raise _LifecycleLiveLimit('window prefix lacks operation owner')
    owner.reserve(4 * 1048577 + 32768, 'window-prefix-bounded-text-and-token-scratch')
    attempt = getattr(db, '_attempt', None)
    if attempt is not None:attempt.check()
    cursor = None
    previous_factory = db.row_factory
    db.row_factory = sqlite3.Row
    stack = []
    route[0] = None
    try:
        # SQL itself bounds returned Unicode, even if storage grows after pricing.
        # Both fields are selected by one statement on one snapshot; no BEGIN is
        # imposed on the legacy caller and no connection/Entry/ledger is created.
        cursor = db.execute('SELECT length(CAST(state AS BLOB)),substr(state,1,1048577) FROM snapshots ORDER BY seq LIMIT 1')
        row = cursor.fetchone()
        cursor.close();cursor = None
        if row is None:return
        size, raw = row
        if type(size) is not int or size > 1048576:
            raise _LifecycleLiveLimit('window prefix exceeds 1MiB')
        if type(raw) is not str:return
        i = 0
        while i < len(raw):
            if attempt is not None:attempt.check()
            char = raw[i]
            if char.isspace():i += 1;continue
            if char == '"':
                start = i
                i += 1
                while i < len(raw):
                    if raw[i] == '\\':i += 2;continue
                    if raw[i] == '"':break
                    i += 1
                if i >= len(raw):return
                i += 1
                end = i
                while i < len(raw) and raw[i].isspace():i += 1
                # All recognized tokens have <= 256 raw chars even when every
                # ASCII character is escaped. Longer values cannot be markers.
                token = json.loads(raw[start:end]) if end-start <= 256 else None
                path = stack[-1][1] if stack else None
                if i < len(raw) and raw[i] == ':':
                    if not stack or stack[-1][0] != '{':return
                    stack[-1][2] = token
                    if (path == 'root' and token == '_lifecycle'
                            or path == 'root._config' and token == 'lifecycle'):
                        route[0] = True
                        return
                    i += 1
                elif (stack and stack[-1][2] == 'version'
                        and path in ('root._config.event_wakeup', 'root._p04.rules')
                        and token == 'p04-event-wakeup-v2-lifecycle'):
                    route[0] = True
                    return
                continue
            if char in '{[':
                if not stack:
                    path = 'root' if char == '{' else None
                else:
                    parent, key = stack[-1][1:3]
                    path = (parent + '.' + key if parent in ('root', 'root._config', 'root._p04')
                            and key in ('_config', '_p04', 'event_wakeup', 'rules') else None)
                lease = owner.memory.hold(2048, 'window-prefix-frame')
                stack.append([char, path, None, lease])
            elif char in '}]':
                if not stack or (stack[-1][0] == '{') != (char == '}'):return
                frame = stack.pop()
                frame[3].release()
            elif char == ',':
                if stack:stack[-1][2] = None
            i += 1
        if not stack:route[0] = False
    except (ValueError, TypeError, IndexError):
        # A malformed token is unknown, never a successful legacy determination.
        # Do not publish a strict JSON exception before the old missing-window leaf.
        route[0] = None
    finally:
        try:
            if cursor is not None:cursor.close()
        finally:
            db.row_factory = previous_factory
            for frame in stack:frame[3].release()


def _window_record_legacy(db, world_id, window_id):
    from .world import WorldStore
    batch = _history_audit_batch.get()
    try:
        internal_uuid(window_id)
        db.row_factory = sqlite3.Row
        from copy import deepcopy
        window_key = (world_id, window_id)
        valid_key = type(world_id) is str and bool(world_id) and type(window_id) is str
        if (batch is not None and batch.db is db and valid_key
                and batch.unchanged(batch.storage, db)):
            cached = batch.window_records.get(window_key)
            if cached is not None:
                _p04_native_current(db,latest(db))
                return deepcopy(cached)
        row = db.execute('SELECT * FROM coordinator_windows WHERE world_id=? AND window_id=?', (world_id, window_id)).fetchone()
        if row is None:
            unproven()
        result = dict(row)
        try:
            result['roster'] = json.loads(result['roster_json'])
            roster = result['roster']
            if (type(roster) is not list or any(type(p) is not str or not p for p in roster)
                    or len(set(roster)) != len(roster) or sha(roster) != result['roster_sha256']
                    or result['state'] not in ('open', 'completed') or result['boundary_kind'] not in ('normal', 'horizon')):
                unproven()
            for key in ('ordinal', 'base_revision', 'advanced_revision', 'created_parent_epoch', 'owner_parent_epoch'):
                if type(result[key]) is not int or not 1 <= result[key] <= MAX_INT:
                    unproven()
            for key in ('target_time', 'horizon_at_open'):
                if type(result[key]) is not int or not 0 <= result[key] <= MAX_INT:
                    unproven()
            if (result['advanced_revision'] != result['base_revision']+1
                    or result['owner_parent_epoch'] < result['created_parent_epoch']
                    or result['horizon_at_open'] < result['target_time']):
                unproven()
            if result['completed_revision'] is not None:
                integer(result['completed_revision'], 1)
            advanced = db.execute('SELECT state FROM snapshots WHERE seq=?', (result['advanced_revision'],)).fetchone()
            base = db.execute('SELECT state FROM snapshots WHERE seq=?', (result['base_revision'],)).fetchone()
            if advanced is None or base is None:
                unproven()
            advanced = json.loads(advanced[0]); base = json.loads(base[0]); current = latest(db)
            if (advanced['world_id'] != world_id or base['world_id'] != world_id or current['world_id'] != world_id
                    or advanced['revision'] != result['advanced_revision'] or advanced['time'] != result['target_time']
                    or advanced['horizon'] != result['horizon_at_open']
                    or result['target_time'] != WorldStore._p04_target(base)):
                unproven()
            # This slice has no office/country/roster replacement producer. Card
            # edits and journeys are legal; damaged principal identities are not a
            # business loss of office and cannot authorize a rejection leaf.
            genesis=json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
            def identities(snapshot):
                return [(p['id'],p['country_id'],p['office']) for p in snapshot['persons']]
            from .world import _lc_optin
            lifecycle_read = lifecycle_native_read(db) if _lc_optin(genesis) else None
            if lifecycle_read is None:
                if identities(current)!=identities(genesis) or identities(advanced)!=identities(genesis):
                    unproven()
            else:
                for historical in (base,advanced,current):
                    _lifecycle_snapshot_proof(lifecycle_read,historical,genesis)
            horizon = result['boundary_kind'] == 'horizon'
            WorldStore._check_frozen_p04(advanced,genesis)
            WorldStore._check_frozen_p04(current,genesis)
            _p04_native_current(db,current)
            expected = WorldStore._p04_expected(base,advanced,genesis=genesis)
            if '_p04' in advanced and advanced['_p04']['dispatch']!=expected:
                unproven()
            if (roster != expected or (horizon != (advanced['time'] == advanced['horizon']))
                    or (horizon and (advanced['status'] != 'horizon_paused' or result['state'] != 'completed'))
                    or not set(roster) <= {p['id'] for p in current['persons']}):
                unproven()
            activations = db.execute('SELECT * FROM coordinator_activations WHERE world_id=? AND window_id=? ORDER BY roster_index', (world_id, window_id)).fetchall()
            if len(activations) != len(roster):
                unproven()
            for index, activation in enumerate(activations):
                internal_uuid(activation['activation_id'])
                integer(activation['roster_index'])
                integer(activation['private_closed'])
                for key in ('created_parent_epoch', 'owner_parent_epoch'):
                    integer(activation[key], 1)
                for key in ('observation_revision', 'card_version', 'base_session_version',
                            'promoted_version', 'owner_actor_epoch'):
                    if activation[key] is not None:
                        integer(activation[key], 0 if key == 'base_session_version' else 1)
                if (activation['world_id'] != world_id or activation['window_id'] != window_id
                        or activation['created_parent_epoch'] != result['created_parent_epoch']
                        or activation['owner_parent_epoch'] < activation['created_parent_epoch']
                        or (activation['observation_revision'] is not None and not
                            result['advanced_revision'] <= activation['observation_revision'] <= current['revision'])
                        or (activation['promoted_version'] is not None and activation['base_session_version'] is not None
                            and activation['promoted_version'] != activation['base_session_version']+1)):
                    unproven()
                if lifecycle_read is not None and activation['observation_revision'] is not None:
                    _lifecycle_activation_identity(lifecycle_read,genesis,activation)
                if activation['intent_id'] is not None:
                    internal_uuid(activation['intent_id'])
                if (activation['roster_index'] != index or activation['person_id'] != roster[index]
                        or activation['phase'] not in ('pending','prepared','candidate','world_committed','completed','technical_blocked','rejected','skipped')
                        or activation['private_closed'] not in (0,1)
                        or (activation['phase'] == 'pending' and (activation['intent_id'] is not None or activation['private_closed'] != 0))):
                    unproven()
            if result['state'] == 'completed':
                completion = result['completed_revision']
                if (type(completion) is not int
                        or not result['advanced_revision'] <= completion <= current['revision']
                        or any(a['observation_revision'] is not None and
                               (a['observation_revision'] > completion or
                                (a['observation_revision'] == completion and a['phase'] != 'rejected'))
                               for a in activations)
                        or any(a['phase'] not in ('completed', 'rejected', 'skipped')
                               or a['private_closed'] != 1 for a in activations)):
                    unproven()
                completed = db.execute('SELECT state FROM snapshots WHERE seq=?', (completion,)).fetchone()
                if completed is None:
                    unproven()
                completed = json.loads(completed[0])
                if lifecycle_read is not None:
                    _lifecycle_snapshot_proof(lifecycle_read,completed,genesis)
                if (completed['world_id'] != world_id or completed['revision'] != completion
                        or completed['time'] != result['target_time']):
                    unproven()
            elif result['completed_revision'] is not None:
                unproven()
        except (KeyError, ValueError, TypeError, IndexError):
            unproven()
        if (batch is not None and _history_audit_batch.get() is batch
                and batch.db is db and valid_key and batch.unchanged(batch.storage, db)):
            batch.window_records[window_key] = deepcopy(result)
        return deepcopy(result)
    except BaseException:
        if batch is not None:
            batch.invalidate()
        raise


def _window_record_owned(db, world_id, window_id):
    """Same-operation domain prefix, then original legacy or strict LC entry."""
    owner = _lifecycle_value_context.get()
    if owner is None or owner.closed or owner.failed:
        raise _LifecycleLiveLimit('window record requires operation value owner')
    internal_uuid(window_id)
    owner.reserve(16384, 'window-inner-frame-and-builtin-temporaries')
    route = [None]
    _lifecycle_value_invoke(_window_prefix_probe, (db, route))
    if route[0] is False:
        return _window_record_legacy(db, world_id, window_id)
    if route[0] is None:
        # Unknown never authorizes legacy. Preserve the old absent-row primary
        # without parsing malformed genesis; a present row still requires native.
        cursor = db.execute('SELECT 1 FROM coordinator_windows WHERE world_id=? AND window_id=?', (world_id, window_id))
        try:
            missing = cursor.fetchone() is None
        finally:
            cursor.close()
        if missing:unproven()
    read = lifecycle_native_read(db)
    try:
        result = _window_record_native_body(db, world_id, window_id, read)
    except BaseException:
        read.fail()
        raise
    read.close()
    return result


def _lifecycle_activation_identity(read, genesis, activation):
    from .world import _lc_value_memory
    if _lc_value_memory(read) is None:return _lifecycle_activation_identity_legacy(read,genesis,activation)
    return _lifecycle_native_call(read,_lifecycle_activation_identity_owned,read,genesis,activation)


def _lifecycle_rejection(read, world, row, result, decision):
    from .world import _lc_value_memory
    if _lc_value_memory(read) is None:return _lifecycle_rejection_legacy(read,world,row,result,decision)
    return _lifecycle_native_call(read,_lifecycle_rejection_owned,read,world,row,result,decision)


def _lifecycle_compile(world, decision, read):
    from .world import _lc_value_memory
    if _lc_value_memory(read) is None:return _lifecycle_compile_legacy(world,decision,read)
    # _compile returns builtin effect dicts, not a permit or a live SQL resource.
    return _lifecycle_native_call(read,_lifecycle_compile_owned,world,decision,read,export=True)


def _p04_committed_meeting(read, world_id, at, cause):
    # The tuple consists solely of two original scalar IDs and one bool. Price
    # this carrier in the outer owner; nested JSON/Rows do not escape the body.
    from .world import _lc_value_memory
    memory=_lc_value_memory(read)
    if memory is None:return _p04_committed_meeting_legacy(read,world_id,at,cause)
    return tuple(_lifecycle_native_call(read,_p04_committed_meeting_owned,
                 read,world_id,at,cause,export=True))


def _lifecycle_native_sql_one(read, sql, params=()):
    owner=_lifecycle_value_context.get()
    if owner is None or owner.memory is not read.memory:raise _LifecycleLiveLimit('native SQL lacks value owner')
    iterator=read._stream_query(sql,params)
    loan=None
    try:
        loan=next(iterator,None)
        if loan is None:return None
        owner.loans.append(loan)
        return loan.value
    except BaseException:
        if loan is not None:loan.fail()
        raise
    finally:
        iterator.close()


def _lifecycle_native_sql_json(read, sql, params=()):
    iterator=read._stream_query(sql,params)
    loan=row=None
    try:
        loan=next(iterator,None)
        if loan is None:return None
        row=loan.value
        value=_lifecycle_plain_json(row[0])
    except BaseException:
        if loan is not None:loan.fail()
        raise
    finally:
        iterator.close()
    row=None
    loan.close()
    return value


def _lifecycle_native_each_sql(read, sql, params, callback):
    iterator=read._stream_query(sql,params)
    loan=row=None
    try:
        for index,loan in enumerate(iterator):
            row=loan.value
            _lifecycle_native_call(read,callback,row,index)
            row=None
            loan.close();loan=None
    except BaseException:
        if loan is not None:loan.fail()
        raise
    finally:
        row=None
        iterator.close()


def _p04_committed_meeting_owned(read, world_id, at, cause):
    from .world import _lc_reserve, _lc_read
    """Join a real meeting cause to direct submit or committed DM child rows.

    Preview decisions, unsuccessful ruling, domain rejection and uncommitted
    children cannot create an obligation. No new attestation table or witness.
    """
    row=_lc_read(read,'main.intents',(('id',cause),),first=True)
    if row is None or row['receipt'] is None:raise ValueError('missing meet receipt')
    payload=_lifecycle_plain_json(row['payload']);receipt=_lifecycle_plain_json(row['receipt'])
    body=payload['body']
    if (body.get('action')!='meet' or receipt['world_id']!=world_id
            or receipt['person_id']!=row['person'] or receipt['intent_id']!=cause
            or receipt['status']!='committed' or receipt['time']!=at
            or receipt['ruling'] not in ('成功','大成功')
            or receipt['effect'].get('implemented') is not True
            or receipt['effect'].get('participant')!=body['to']):
        raise ValueError('nonimplemented meeting')
    child='parent' in payload
    if child:
        parent=payload['parent'];version=payload['version']
        if type(version) is not int or version<1:raise ValueError('child version')
        source=_lc_read(read,'main.intents',(('id',parent),),first=True)
        decision=_lc_read(read,'main.dm_decisions',(('intent_id',parent),('version',version)),first=True)
        if source is None or source['receipt'] is None or decision is None:raise ValueError('child lineage')
        source_payload=_lifecycle_plain_json(source['payload']);parent_receipt=_lifecycle_plain_json(source['receipt'])
        decision=_lifecycle_plain_json(decision['data'])
        _lc_reserve(decision,2,'meeting-effect-match-scratch')
        matches=[i for i,b in enumerate(decision['effects']) if cause==f'dm:{len(parent)}:{parent}:{version}:{i}' and b==body]
        if (len(matches)!=1 or source['person']!=row['person']
                or source_payload['body']['action']!='propose'
                or parent_receipt['world_id']!=world_id or parent_receipt['person_id']!=row['person']
                or parent_receipt['intent_id']!=parent or parent_receipt['status']!='committed'
                or parent_receipt['time']!=at or parent_receipt['decision_version']!=version
                or receipt['decision_version']!=version or decision['world_id']!=world_id
                or parent_receipt['ruling']!=decision['ruling'] or receipt['ruling']!=decision['ruling']
                or parent_receipt['effect']['effects'][matches[0]]!=receipt['effect']):
            raise ValueError('uncommitted child effect')
    elif payload['ruling']!=receipt['ruling']:
        raise ValueError('direct ruling')
    return [row['person'],body['to'],child]


def _lifecycle_rejection_owned(read, world, row, result, decision):
    from .world import _lc_reserve, _lc_read
    """Same primitive historical producer proof for native and offlock leaves.

    Replay only a private state copy. No permit, fake DB, SQL dispatch adapter,
    saved verdict or persisted candidate is made by this proof.
    """
    if result['action'] not in _LIFECYCLE_ACTIONS:
        return
    from .world import (DomainActionError, _lc_optin, _lc_child, _lc_effect_conditions,
                        _world_mutation_scope, _lc_copy, _lifecycle_project, _lc_historical)
    if result['origin'] != 'adjudication.execute':unproven()
    genesis_row = _lc_read(read,'main.snapshots', columns=('state',), order=(('seq',False),), limit=1, first=True)
    if genesis_row is None:unproven()
    genesis = _lifecycle_plain_json(genesis_row[0])
    if not _lc_optin(genesis):unproven()
    base_revision = decision['base_revision'];integer(base_revision,1)
    # The single causal pass validates every original revision and producer.
    # The state tuple does not retain complete snapshots or authorize anything.
    _lifecycle_project(read,genesis,base_revision)
    states = (read,genesis,base_revision)
    state = _lc_historical(states,base_revision);index = result['effect_index']
    _lc_reserve(state,4,'rejection-original-apply-scratch')
    _lc_reserve(decision,2,'rejection-effects-scratch')
    cause = dict(parent_intent_id=row['intent_id'],decision_version=result['decision_version'],
                 effect_index=index,child_intent_id=f"dm:{len(row['intent_id'])}:{row['intent_id']}:{result['decision_version']}:{index}")
    pid,proposal,body,original,_ = _lc_child(read,row['world_id'],cause,base_revision,states,committed=False)
    if original != decision or pid != row['person_id']:unproven()
    person = world._find(state['persons'],pid,'ACTOR_RECOVERY_UNPROVEN')
    token = _world_mutation_scope.set(None)
    try:
        for position,effect in enumerate(decision['effects'][:index+1]):
            child = f"dm:{len(row['intent_id'])}:{row['intent_id']}:{result['decision_version']}:{position}"
            if effect['action'] not in _LIFECYCLE_ACTIONS:
                # Original pure state operation, with no mutation DB/actor scope.
                try:world._apply_action(state,person,child,effect,decision['ruling'])
                except DomainActionError:unproven()  # An earlier error is not this leaf.
                continue
            try:
                condition = _lc_effect_conditions(state,person,proposal,effect,decision['ruling'],base_revision)
            except DomainActionError as error:
                if (position != index or not _lifecycle_domain_source(error)
                        or error.code != result['code'] or error.action != result['action']):unproven()
                return
            if position == index:unproven()  # Claimed rejection had no real producer.
            if decision['ruling'] not in ('成功','大成功'):continue
            # Only condition dependencies of later lifecycle effects are projected.
            # Notices/event log are not authority and do not influence these gates.
            if effect['action']=='write_office_document':
                state['_lifecycle']['documents'].append(dict(id='office-document-'+child,
                    author_person_id=pid,office_id=person['office_id'],place_id=condition['id'],text=effect['text'],
                    created_revision=base_revision+1,cause_ref=dict(parent_intent_id=row['intent_id'],
                        decision_version=result['decision_version'],effect_index=position,child_intent_id=child),
                    access='current_office_at_place'))
            elif effect['action']=='resolve_physical_attempt' and effect['outcome']=='mortal':
                target,means = condition
                state['_lifecycle']['transitions'].append(dict(kind='mortal_injury',person_id=target['id']))
        unproven()
    finally:
        _world_mutation_scope.reset(token)


def _lifecycle_activation_identity_owned(read, genesis, activation):
    from .world import _lc_reserve, _lc_read
    revision=activation['observation_revision']
    saved=_lc_read(read,'main.snapshots',(('seq',revision),),('state',),first=True)
    if saved is None:unproven()
    state=_lifecycle_plain_json(saved[0]);proof=_lifecycle_snapshot_proof(read,state,genesis)
    pid=activation['person_id']
    if proof is None or pid not in proof['active']:unproven()
    from .world import _lc_tenure
    tenure=_lc_tenure(state,pid,revision)
    private=_lc_read(read,'actor.actor_intents',(('intent_id',activation['intent_id']),('person_id',pid)),first=True)
    if private is None:unproven()
    request=_lifecycle_plain_json(private['request'])
    expected=dict(world_id=state['world_id'],person_id=pid,observation_revision=revision,
                  origin_ref=pid,tenure_ref=tenure['id'] if tenure is not None else None)
    if request.get('schema_version')!=3 or request.get('identity_binding')!=expected:unproven()


def _lifecycle_compile_owned(world, decision, read):
    from .world import _lc_reserve, _lc_read
    from .adjudication import Adjudicator
    from .world import _lc_optin
    from types import SimpleNamespace
    first=_lc_read(read,'main.snapshots',columns=('state',),order=(('seq',False),),limit=1,first=True)
    if first is None:unproven()
    genesis=_lifecycle_plain_json(first[0])
    _lc_reserve(decision,2,'compile-original-body-scratch')
    if _lc_optin(genesis):
        return Adjudicator._compile(SimpleNamespace(world=world),decision,lifecycle_version='p09-lifecycle-v1')
    return Adjudicator._compile(SimpleNamespace(world=world),decision)


def _window_record_native_body(db, world_id, window_id, read):
    from .world import WorldStore, _lc_history_scope
    from .world import _lc_json, _lc_copy, _lc_hash, _lc_reserve
    owner=_lifecycle_value_context.get()
    if owner is None or owner.memory is not read.memory:raise _LifecycleLiveLimit('window body lacks same-ledger owner')
    batch = _history_audit_batch.get()
    if batch is not None:
        owner.reserve(196608,'window-batch-bounded-fingerprint-scratch')
    try:
        internal_uuid(window_id)
        db.row_factory = sqlite3.Row
        from copy import deepcopy
        window_key = (world_id, window_id)
        valid_key = type(world_id) is str and bool(world_id) and type(window_id) is str
        if (batch is not None and batch.db is db and valid_key
                and batch.unchanged(batch.storage, db)):
            # V3 native Rows/results are operation-owned, never cross-Entry memoized.
            cached = None
        row = _lifecycle_native_sql_one(read,'SELECT * FROM coordinator_windows WHERE world_id=? AND window_id=?',(world_id,window_id))
        if row is None:
            unproven()
        owner.reserve(4096,'window-result-dict-slots')
        result = dict(row)
        try:
            result['roster'] = _lifecycle_plain_json(result['roster_json'])
            roster = result['roster']
            _lc_reserve(roster,2,'window-roster-sets-scratch')
            if (type(roster) is not list or any(type(p) is not str or not p for p in roster)
                    or len(set(roster)) != len(roster) or _lc_hash(roster) != result['roster_sha256']
                    or result['state'] not in ('open', 'completed') or result['boundary_kind'] not in ('normal', 'horizon')):
                unproven()
            for key in ('ordinal', 'base_revision', 'advanced_revision', 'created_parent_epoch', 'owner_parent_epoch'):
                if type(result[key]) is not int or not 1 <= result[key] <= MAX_INT:
                    unproven()
            for key in ('target_time', 'horizon_at_open'):
                if type(result[key]) is not int or not 0 <= result[key] <= MAX_INT:
                    unproven()
            if (result['advanced_revision'] != result['base_revision']+1
                    or result['owner_parent_epoch'] < result['created_parent_epoch']
                    or result['horizon_at_open'] < result['target_time']):
                unproven()
            if result['completed_revision'] is not None:
                integer(result['completed_revision'], 1)
            advanced = _lifecycle_native_sql_json(read,'SELECT state FROM snapshots WHERE seq=?',(result['advanced_revision'],))
            base = _lifecycle_native_sql_json(read,'SELECT state FROM snapshots WHERE seq=?',(result['base_revision'],))
            if advanced is None or base is None:
                unproven()
            current = _lifecycle_native_sql_json(read,'SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1')
            if current is None:unproven()
            for value in (advanced,base,current):_lc_reserve(value,2,'window-original-direct-scratch')
            if (advanced['world_id'] != world_id or base['world_id'] != world_id or current['world_id'] != world_id
                    or advanced['revision'] != result['advanced_revision'] or advanced['time'] != result['target_time']
                    or advanced['horizon'] != result['horizon_at_open']
                    or result['target_time'] != WorldStore._p04_target(base)):
                unproven()
            # This slice has no office/country/roster replacement producer. Card
            # edits and journeys are legal; damaged principal identities are not a
            # business loss of office and cannot authorize a rejection leaf.
            genesis=_lifecycle_native_sql_json(read,'SELECT state FROM snapshots ORDER BY seq LIMIT 1')
            if genesis is None:unproven()
            def identities(snapshot):
                return [(p['id'],p['country_id'],p['office']) for p in snapshot['persons']]
            from .world import _lc_optin
            lifecycle_read = read if _lc_optin(genesis) else None
            with _lc_history_scope(lifecycle_read,genesis):
                if lifecycle_read is None:
                    if identities(current)!=identities(genesis) or identities(advanced)!=identities(genesis):
                        unproven()
                else:
                    for historical in (base,advanced,current):
                        _lifecycle_snapshot_proof(lifecycle_read,historical,genesis)
                horizon = result['boundary_kind'] == 'horizon'
                WorldStore._check_frozen_p04(advanced,genesis)
                WorldStore._check_frozen_p04(current,genesis)
                _p04_current_projection(read,current)
                expected = WorldStore._p04_expected(base,advanced,genesis=genesis)
                if '_p04' in advanced and advanced['_p04']['dispatch']!=expected:
                    unproven()
                if (roster != expected or (horizon != (advanced['time'] == advanced['horizon']))
                        or (horizon and (advanced['status'] != 'horizon_paused' or result['state'] != 'completed'))
                        or not set(roster) <= {p['id'] for p in current['persons']}):
                    unproven()
                activation_count = _lifecycle_native_sql_one(read,'SELECT count(*) FROM coordinator_activations WHERE world_id=? AND window_id=?',(world_id,window_id))[0]
                if activation_count != len(roster):unproven()
                late = nonterminal = False
                def validate_activation(activation,index):
                    nonlocal late,nonterminal
                    internal_uuid(activation['activation_id'])
                    integer(activation['roster_index'])
                    integer(activation['private_closed'])
                    for key in ('created_parent_epoch', 'owner_parent_epoch'):
                        integer(activation[key], 1)
                    for key in ('observation_revision', 'card_version', 'base_session_version',
                                'promoted_version', 'owner_actor_epoch'):
                        if activation[key] is not None:
                            integer(activation[key], 0 if key == 'base_session_version' else 1)
                    if (activation['world_id'] != world_id or activation['window_id'] != window_id
                            or activation['created_parent_epoch'] != result['created_parent_epoch']
                            or activation['owner_parent_epoch'] < activation['created_parent_epoch']
                            or (activation['observation_revision'] is not None and not
                                result['advanced_revision'] <= activation['observation_revision'] <= current['revision'])
                            or (activation['promoted_version'] is not None and activation['base_session_version'] is not None
                                and activation['promoted_version'] != activation['base_session_version']+1)):
                        unproven()
                    if lifecycle_read is not None and activation['observation_revision'] is not None:
                        _lifecycle_activation_identity(lifecycle_read,genesis,activation)
                    if activation['intent_id'] is not None:
                        internal_uuid(activation['intent_id'])
                    if (activation['roster_index'] != index or activation['person_id'] != roster[index]
                            or activation['phase'] not in ('pending','prepared','candidate','world_committed','completed','technical_blocked','rejected','skipped')
                            or activation['private_closed'] not in (0,1)
                            or (activation['phase'] == 'pending' and (activation['intent_id'] is not None or activation['private_closed'] != 0))):
                        unproven()
                    completion=result['completed_revision']
                    if result['state']=='completed' and type(completion) is int:
                        late = late or (activation['observation_revision'] is not None and (activation['observation_revision']>completion or (activation['observation_revision']==completion and activation['phase']!='rejected')))
                        nonterminal = nonterminal or activation['phase'] not in ('completed','rejected','skipped') or activation['private_closed']!=1
                _lifecycle_native_each_sql(read,'SELECT * FROM coordinator_activations WHERE world_id=? AND window_id=? ORDER BY roster_index',(world_id,window_id),validate_activation)
                if result['state'] == 'completed':
                    completion = result['completed_revision']
                    if (type(completion) is not int
                            or not result['advanced_revision'] <= completion <= current['revision']
                            or late or nonterminal):
                        unproven()
                    completed = _lifecycle_native_sql_json(read,'SELECT state FROM snapshots WHERE seq=?',(completion,))
                    if completed is None:
                        unproven()
                    _lc_reserve(completed,2,'window-completion-scratch')
                    if lifecycle_read is not None:
                        _lifecycle_snapshot_proof(lifecycle_read,completed,genesis)
                    if (completed['world_id'] != world_id or completed['revision'] != completion
                            or completed['time'] != result['target_time']):
                        unproven()
                elif result['completed_revision'] is not None:
                    unproven()
        except (KeyError, ValueError, TypeError, IndexError):
            unproven()
        if (batch is not None and _history_audit_batch.get() is batch
                and batch.db is db and valid_key and batch.unchanged(batch.storage, db)):
            pass  # No operation-owned value is aliased into the legacy cache.
        return _lc_copy(result)
    except BaseException:
        if batch is not None:
            batch.invalidate()
        raise


def _lifecycle_plain_json_borrow(raw, *, memory, role):
    """Old json.loads semantics for the old nested sourcehelpers only.

    No strict hooks or post-decode canonicalization is added to those helpers.
    The scanner still prices all graph/decoder allocations before full loads.
    """
    if type(memory) is not _LifecycleLiveLedger or type(role) is not str:
        raise _LifecycleLiveLimit('foreign plain lifecycle JSON allocation')
    carrier=memory.hold(4096,role+':scan')
    loan=None
    try:
        if _lifecycle_utf8_size(raw)>1048576:raise _LifecycleLiveLimit('lifecycle JSON input exceeds 1MiB')
        scan=_LifecycleJSONScan(raw,memory,strict=False)
        scan.value();scan.space()
        if scan.offset!=scan.length:raise ValueError('extra lifecycle JSON data')
        loan=_LifecycleValueLoan(memory,memory.hold(scan.charge+256,role+':value'))
        scratch=memory.hold(4096+256*scan.nodes+16*scan.wire,role+':decode')
        loan.value=json.loads(raw)
        raw=scan=None
    except BaseException:
        memory.failed=True
        if loan is not None:loan.fail()
        raise
    scratch.release();carrier.release()
    return loan


def _lifecycle_plain_json(raw):
    owner=_lifecycle_value_context.get()
    if owner is None or owner.closed or owner.failed:
        raise _LifecycleLiveLimit('plain native decode requires operation value owner')
    return owner.take(_lifecycle_plain_json_borrow(raw,memory=owner.memory,role='native-original-json'))


# 1132: inserted before the original Scheduler source registration.
# No business authority, new Entry, new ledger, retained-debt refund or result
# transfer protocol is introduced here. Raw Rows remain actual SQLite Rows.
_lc_ro1132_context = ContextVar('lc_ro1132_native_frame', default=None)


class _lc_ro1132_Frame:
    def __init__(self, proof, region, python_frame):
        self.proof, self.region, self.python_frame = proof, region, python_frame
        self.reader = self.db = self.generation = self.vector = None
        self.ready = self.failed = self.ended = False
        self.failure = None
        self.cursors = []
        self.cleanup = []
        self.cleanup_first = self.cleanup_first_tb = None
        self.cleanup_first_context = self.cleanup_first_cause = None
        self.cleanup_recording = self.cleanup_recording_tb = None
        self.cleanup_recording_context = self.cleanup_recording_cause = None
        self.cleanup_incomplete = False


def _lc_ro1132_original(function):
    from .source_attestation import _REGISTRY
    from types import FunctionType, CodeType
    if type(function) is not FunctionType:
        return False
    namespace = function.__globals__
    stamp = _REGISTRY.get(id(namespace))
    if stamp is None or stamp['namespace'] is not namespace or not stamp['matched']:
        return False
    pending = [stamp['code']]
    while pending:
        code = pending.pop()
        # Actual loaded code constant identity, not merely code equality/name.
        if code is function.__code__:
            return True
        pending.extend(item for item in code.co_consts if type(item) is CodeType)
    return False


def _lc_ro1132_bindings():
    """Freeze the real executable bridge, not a same-code error whitelist."""
    from .source_attestation import _REGISTRY
    from .actor_execution import PrivateStore
    from types import FunctionType, CodeType
    namespace = _lc_ro1132_invoke.__globals__
    stamp = _REGISTRY.get(id(namespace))
    if stamp is None or stamp['namespace'] is not namespace or not stamp['matched']:
        raise _LifecycleLiveLimit('native bridge lacks actual loaded source')
    critical = frozenset(('latest', 'window_record', '_window_record_owned',
        '_window_record_native_body', '_p04_native_current', '_p04_current_projection',
        '_p04_current_projection_owned', '_lifecycle_value_invoke', '_lifecycle_native_call',
        '_lifecycle_native_sql_json', '_lifecycle_native_sql_one', '_lifecycle_native_each_sql',
        'lifecycle_native_read', '_lifecycle_plain_json', '_lifecycle_plain_json_borrow',
        '_lifecycle_canonical_borrow', '_lifecycle_hash_owned', '_lifecycle_value_price',
        '_lifecycle_raw_charge'))
    class_codes = {code.co_name:code for code in stamp['code'].co_consts
                   if type(code) is CodeType}
    for original in stamp['objects']:
        name = original.__name__
        if type(original) is FunctionType:
            if name.startswith('_lc_ro1132_') or name in critical:
                if namespace.get(name) is not original or not _lc_ro1132_original(original):
                    raise _LifecycleLiveLimit('native bridge callable/globals changed')
        elif isinstance(original, type) and (name.startswith('_Lifecycle')
                or name in ('_lc_ro1127_Proof', '_lc_ro1132_Frame')):
            if namespace.get(name) is not original:
                raise _LifecycleLiveLimit('native bridge owner/reader class changed')
            class_code = class_codes.get(name)
            if class_code is None:
                raise _LifecycleLiveLimit('native bridge class has no actual source code')
            for code in class_code.co_consts:
                if type(code) is not CodeType or code.co_name.startswith('<'):
                    continue
                value = vars(original).get(code.co_name)
                functions = ((value.fget, value.fset, value.fdel) if isinstance(value, property)
                    else (value.__func__,) if isinstance(value, (staticmethod, classmethod))
                    else (value,))
                found = False
                for function in functions:
                    source = getattr(function, '__wrapped__', function)
                    if (type(source) is FunctionType and source.__code__ is code
                            and source.__globals__ is namespace):
                        found = True
                        break
                if not found:
                    raise _LifecycleLiveLimit('native reader/owner original method changed')
    if not _lc_ro1132_original(PrivateStore._audit):
        raise _LifecycleLiveLimit('native Private audit original code/globals changed')


def _lc_ro1132_authenticated(proof, error):
    """Authenticate an actual paid invocation, including its still-unwinding TB.

    At an inner finally the outer invoke is not yet on the error traceback.
    The retained real executing frame supplies that boundary; after unwind the
    exact invocation frame must instead occur on the original error's TB chain.
    No callable flag, code-name match, caller-supplied Row or error-code grant.
    """
    frame = getattr(proof, 'native1132', None)
    if (type(frame) is not _lc_ro1132_Frame or frame.proof is not proof
            or not frame.ready or frame.reader is None
            or frame.python_frame.f_code is not _lc_ro1132_invoke.__code__
            or frame.python_frame.f_globals is not _lc_ro1132_invoke.__globals__
            or frame.python_frame.f_locals.get('frame') is not frame
            or frame.python_frame.f_locals.get('proof') is not proof
            or frame.python_frame.f_locals.get('db') is not frame.db
            or type(frame.reader) is not _LifecycleNativeReader
            or frame.reader.db is not frame.db or frame.reader.budget is not proof.entry
            or frame.reader.memory is not proof.memory
            or frame.region.memory is not proof.memory
            or frame.reader.generation is not frame.generation
            or frame.vector is not proof.capsule.vector
            or proof.root is not proof.capsule.root
            or proof.entry is not proof.capsule.budget
            or proof.world is not proof.capsule.world
            or not _lc_ro1132_original(_lc_ro1132_invoke)):
        return False
    active = _lc_ro1132_context.get() is frame and not frame.ended
    related = error
    for _ in range(8):
        if related is None:
            break
        tb = BaseException.__traceback__.__get__(related)
        while tb is not None:
            if tb.tb_frame is frame.python_frame:
                return True
            tb = tb.tb_next
        related = (BaseException.__cause__.__get__(related)
                   or BaseException.__context__.__get__(related))
    return active


def _lc_ro1132_reader_for(db, storage=None):
    proof = _lc_ro1127_context.get()
    if proof is None:
        return None
    frame = _lc_ro1132_context.get()
    if (type(frame) is not _lc_ro1132_Frame or frame is not getattr(proof, 'native1132', None)
            or frame.proof is not proof or not frame.ready or frame.ended or frame.failed
            or proof.failed or proof.ended or frame.db is not db
            or frame.reader.budget is not proof.entry or frame.reader.memory is not proof.memory
            or storage is not None and storage is not proof.capsule.storage):
        proof.memory._failed = True
        raise _LifecycleLiveLimit('unfunded or foreign native proof prefix')
    return frame.reader


def _lc_ro1132_cursor(read, sql, params=()):
    """Quote and debit ALL raw results before returning a real native cursor.

    The same immutable read transaction is used twice. The quote streams real
    prepaid Rows one at a time and drops each quote alias before close. Its own
    buffers are funded by NativeReader. Only after their exact payload/width and
    extra direct-container/cursor charges are admitted does original SQL run.
    The actual cursor/Row, order, columns, scalar values and SQL errors survive;
    this is not a Row/cursor proxy or a synthetic material reader.
    """
    frame = _lc_ro1132_context.get()
    if (type(frame) is not _lc_ro1132_Frame or frame.reader is not read
            or _lc_ro1132_reader_for(read.db) is not read
            or type(sql) is not str or not sql.lstrip().upper().startswith('SELECT ')):
        read.memory._failed = True
        raise _LifecycleLiveLimit('native proof query is not an owned SELECT')
    owner = _lifecycle_value_context.get()
    if owner is None or owner.memory is not read.memory or owner.closed or owner.failed:
        read.memory._failed = True
        raise _LifecycleLiveLimit('native raw query lost its value owner')
    owner.reserve(8192, 'ro1132-original-native-cursor-and-cleanup')
    iterator = read._stream_query(sql, params)
    loan = row = None
    try:
        for loan in iterator:
            row = loan.value
            size = 0
            for value in row:
                if type(value) is str:
                    size += _lifecycle_utf8_size(value)
                elif type(value) is bytes:
                    size += len(value)
                elif value is None or type(value) in (int, float):
                    size += 8
                else:
                    read.memory._failed = True
                    raise _LifecycleLiveLimit('unknown native raw scalar')
            # Original payload is NOT allocated yet. List/dict/set/spine slots
            # for this result are separately paid; graph decode is not inferred
            # from this charge and goes through the funded decoder below.
            owner.reserve(_lifecycle_raw_charge(size, len(row)) + 4096,
                          'ro1132-original-row-and-direct-containers')
            row = value = None
            loan.close()
            loan = None
    except BaseException:
        if loan is not None:
            loan.fail()
        raise
    finally:
        row = None
        iterator.close()
    read.budget.check()
    cursor = read.db.execute(sql, params)
    frame.cursors.append(cursor)
    return cursor


def _lc_ro1132_json(read, raw):
    owner = _lifecycle_value_context.get()
    if owner is None or owner.memory is not read.memory:
        raise _LifecycleLiveLimit('native decode lost same-ledger owner')
    value = _lifecycle_plain_json(raw)
    charge, wire, nodes = _lifecycle_value_price(value, read.memory)
    # Full decode is already prepaid by the scanner. This is the extra native
    # body's slices, sets, lists, dictionaries and comparison temporaries.
    owner.reserve(4096 + 4 * charge, 'ro1132-original-json-direct-scratch')
    return value


def _lc_ro1132_digest(read, raw):
    return _lifecycle_hash_owned(_lc_ro1132_json(read, raw), memory=read.memory)


def _lc_ro1132_canonical(read, value):
    owner = _lifecycle_value_context.get()
    return owner.take(_lifecycle_canonical_borrow(value, memory=read.memory,
                       role='ro1132-original-canonical'))


def _lc_ro1132_body_matches(read, request, body, candidate):
    from .actor_execution import body_matches
    if not _lc_ro1132_original(body_matches):
        read.memory._failed = True
        raise _LifecycleLiveLimit('native body predicate source changed')
    # Decode before handing the graph to the old predicate, so it cannot invoke
    # an unpriced json.loads itself. Its old total predicate remains total.
    try:
        messages = _lc_ro1132_json(read, candidate) if type(candidate) is str else candidate
    except (ValueError, TypeError, RecursionError):
        return False
    owner = _lifecycle_value_context.get()
    for value in (request, body, messages):
        if value is not None:
            charge, wire, nodes = _lifecycle_value_price(value, read.memory)
            owner.reserve(4096 + 4 * charge + 256 * nodes + 16 * wire,
                          'ro1132-body-predicate-canonical-and-direct-scratch')
    return body_matches(request, body, messages)


def _lc_ro1132_private_head(read, storage, db, person):
    from .actor_execution import ActorRunError
    session = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_sessions WHERE person_id=?', (person,)).fetchone()
    head = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_session_heads WHERE person_id=?', (person,)).fetchone()
    if (session is None or head is None or head['world_id'] != storage.host._bound_world_id
            or type(head['version']) is not int or not 0 <= head['version'] <= 2**63-1
            or _lc_ro1132_digest(read, session['messages']) != head['messages_sha256']
            or _lc_ro1132_digest(read, session['seen']) != head['seen_sha256']):
        raise ActorRunError('ACTOR_SESSION_STALE')
    return head


def _lc_ro1132_world_check(read, db, state):
    from .world import WorldStore
    # The LC native proof has already admitted genuine LC genesis. Use the
    # already funded original body, avoiding the old legacy raw-genesis prefix.
    return _lifecycle_value_invoke(WorldStore._lifecycle_check_owned, (db, state),
                                    inputs=(state,), detached=True)


def _lc_ro1132_external_compile(read, world, data):
    from .adjudication import Adjudicator
    from types import SimpleNamespace
    owner = _lifecycle_value_context.get()
    charge, wire, nodes = _lifecycle_value_price(data, read.memory)
    owner.reserve(16384 + 4 * charge + 256 * nodes + 16 * wire,
                  'ro1132-original-non-lifecycle-effect-compile')
    return Adjudicator._compile(SimpleNamespace(world=world), data)


def _lc_ro1132_quote_root(read):
    """Prepay the unchanged original readonly root gate before it allocates."""
    owner = _lifecycle_value_context.get()
    owner.reserve(32768, 'ro1132-original-root-scalar-and-schema-scratch')
    queries = (
        'SELECT state FROM snapshots ORDER BY seq LIMIT 1',
        'SELECT * FROM coordinator_execution_leases',
        'SELECT * FROM execution_clock',
    )
    for sql in queries:
        rows = read._all(sql)
        for row in rows:
            size = 0
            for value in row:
                size += (_lifecycle_utf8_size(value) if type(value) is str
                         else len(value) if type(value) is bytes else 8)
            owner.reserve(2 * _lifecycle_raw_charge(size, len(row)) + 4096,
                          'ro1132-original-root-duplicate-raw')
            if sql == queries[0]:
                # Scanner/decode plus four graph-size direct allowances include
                # the original _execution_identity json.loads and its scratch.
                quote = _lc_ro1132_json(read, row[0])
                charge, wire, nodes = _lifecycle_value_price(quote, read.memory)
                owner.reserve(4096 + charge + 256 * nodes + 16 * wire,
                              'ro1132-original-root-genesis-decode-scratch')


def _lc_ro1132_pin_owned(read, proof, db):
    # The existing proof admission MUST already own the pinned vector. A first
    # pin would export new schema/epoch objects and cannot use this NONE route.
    if proof.capsule.vector is None:
        raise _LifecycleLiveLimit('native pin cannot create an escaping vector')
    owner = _lifecycle_value_context.get()
    owner.leases.append(read._helper_scratch(True))
    proof.capsule.pin(db)


def _lc_ro1132_admit_owned(read, proof, db):
    _lc_ro1132_quote_root(read)
    _lc_ro1132_pin_owned(read, proof, db)
    proof.world._check_coordinator_execution(db, proof.root.permit.world_id,
                                             proof.root.permit, readonly=True)
    # Original _execution_identity returns its existing argument, never its
    # temporary baseline. This complete original root caller returns NONE.


def _lc_ro1132_close_cursors(frame):
    # Fixed fallback fields are part of the already prepaid native frame. A
    # snapshot/list-recording failure cannot replace the original body primary
    # or prevent later independent physical cursor closes.
    for cursor in frame.cursors:
        try:
            cursor.close()
        except BaseException as cleanup:
            frame.proof.memory._failed = True
            if frame.cleanup_first is None:
                frame.cleanup_first = cleanup
                frame.cleanup_first_tb = BaseException.__traceback__.__get__(cleanup)
                frame.cleanup_first_context = BaseException.__context__.__get__(cleanup)
                frame.cleanup_first_cause = BaseException.__cause__.__get__(cleanup)
            try:
                frame.cleanup.append((cleanup, BaseException.__traceback__.__get__(cleanup),
                    BaseException.__context__.__get__(cleanup), BaseException.__cause__.__get__(cleanup)))
            except BaseException as recording:
                frame.cleanup_incomplete = True
                if frame.cleanup_recording is None:
                    frame.cleanup_recording = recording
                    frame.cleanup_recording_tb = BaseException.__traceback__.__get__(recording)
                    frame.cleanup_recording_context = BaseException.__context__.__get__(recording)
                    frame.cleanup_recording_cause = BaseException.__cause__.__get__(recording)
    if frame.failure is None and frame.cleanup_first is not None:
        raise BaseException.with_traceback(frame.cleanup_first, frame.cleanup_first_tb)


def _lc_ro1132_invoke(proof, kind, argument=None):
    """Closed native dispatcher; no arbitrary unpriced callback or supplied DB.

    The original Selection creates the RO physical connection with original
    progress/length/rollback/close ownership. All initialization, latest raw and
    JSON, private audit, nested helper and error carriers use the same Entry and
    shared ledger. Success uses the UNCHANGED region transfer protocol.
    """
    import sys
    if (type(proof) is not _lc_ro1127_Proof or _lc_ro1127_context.get() is not proof
            or proof.failed or proof.ended or not proof.armed
            or _lifecycle_value_context.get() is not proof.owner
            or _lc_ro1132_context.get() is not None
            or getattr(proof.entry, '_lifecycle_value_owner', None) is not proof.parent
            or proof.world._scoped_entry_budget() is not proof.entry
            or proof.world._scoped_coordinator_attempt() is not proof.root):
        raise _LifecycleLiveLimit('native proof is not a fresh active original Entry owner')
    if (kind not in ('current', 'window', 'audit', 'activation', 'head0_empty')
            or kind == 'current' and argument is not None
            or kind != 'current' and type(argument) is not str):
        proof.memory._failed = True
        raise _LifecycleLiveLimit('unknown native proof dispatcher route')
    proof.entry.check()
    region = _LifecycleValueOwner(proof.memory)
    region.reserve(65536, 'ro1132-native-frame-binding-and-first-error-carriers')
    frame = _lc_ro1132_Frame(proof, region, sys._getframe())
    proof.native1132 = frame
    proof.owner.control_errors.append(frame)
    value_token = _lifecycle_value_context.set(region)
    live_token = _lifecycle_live_context.set(proof.memory)
    native_token = _lc_ro1132_context.set(frame)
    result = None
    db = None
    try:
        _lc_ro1132_bindings()
        with _c0_segment(proof.capsule, _C0Fresh(proof.capsule)) as db:
            proof.capsule.check_source()
            frame.db = db
            frame.reader = region.keep_reader(lifecycle_native_read(db))
            read = frame.reader
            # Admission has no escaping result: capsule.vector already belongs
            # to the admitted proof. Original raw/JSON/root/pin scratch is paid
            # inside a real returning frame, not retained across a later failure
            # after that successful frame and all of its local aliases ended.
            _lifecycle_value_invoke(_lc_ro1132_admit_owned, (read, proof, db),
                                    memory=proof.memory)
            if (read.budget is not proof.entry or read.memory is not proof.memory
                    or not read._live_admission or read.world_id != proof.root.permit.world_id
                    or read.generation != proof.capsule.source_generation):
                raise _LifecycleLiveLimit('native reader/source/Entry does not match proof')
            read.check_source()
            proof.capsule.check_source()
            frame.generation, frame.vector = read.generation, proof.capsule.vector
            frame.ready = True
            try:
                if kind == 'current':
                    _p04_native_current(db, latest(db))
                elif kind == 'window':
                    result = window_record(db, proof.root.permit.world_id, argument)
                elif kind == 'audit':
                    proof.capsule.storage.audit(db, argument)
                elif kind == 'activation':
                    row = _lc_ro1132_cursor(read, 'SELECT * FROM coordinator_activations WHERE activation_id=?', (argument,)).fetchone()
                    if row is None:
                        unproven()
                    _lc_ro1132_paid_native_audit(read, proof.capsule.storage, db, row)
                else:
                    head = _lc_ro1132_private_head(read, proof.capsule.storage, db, argument)
                    rows = _lc_ro1132_cursor(read, 'SELECT intent_id FROM actor.actor_intents WHERE person_id=?', (argument,)).fetchall()
                    region.reserve(1024, 'ro1132-head0-result-tuple')
                    result = (head['version'], rows == [])
                read.check_source()
                _lifecycle_value_invoke(_lc_ro1132_pin_owned, (read, proof, db),
                                        memory=proof.memory)
                proof.capsule.check_source()
            except BaseException as error:
                frame.failed = True
                frame.failure = (error, BaseException.__traceback__.__get__(error),
                                 BaseException.__context__.__get__(error),
                                 BaseException.__cause__.__get__(error))
                proof.fail(error)
                region.fail(error)
                raise
            finally:
                # Keep first business/native errors primary and close ALL actual
                # cursors independently; no accounting refund on exceptional TBs.
                _lc_ro1132_close_cursors(frame)
        region.move_to(proof.owner)
        return result
    except BaseException as error:
        frame.failed = True
        if frame.failure is None:
            frame.failure = (error, BaseException.__traceback__.__get__(error),
                             BaseException.__context__.__get__(error),
                             BaseException.__cause__.__get__(error))
        proof.fail(error)
        region.fail(error)
        raise
    finally:
        frame.ended = True
        try:
            _lc_ro1132_context.reset(native_token)
        finally:
            try:
                _lifecycle_live_context.reset(live_token)
            finally:
                _lifecycle_value_context.reset(value_token)


def _lc_ro1132_paid_exists(read, db, table, schema='main'):
    return _lc_ro1132_cursor(read, f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _lc_ro1132_paid_phase_proof(read, world, db, row):
    try:
        return _lc_ro1132_paid__phase_proof(read, world, db, row)
    except (ValueError, TypeError, KeyError, IndexError):
        unproven()


def _lc_ro1132_paid__phase_proof(read, world, db, row):
    if row['phase'] == 'technical_blocked':
        _lc_ro1132_paid_block_proof(read, world, db, row)
        return row
    if row['phase'] == 'completed' and _lc_ro1132_paid_exists(read, db, 'coordinator_activation_blocks') and _lc_ro1132_cursor(read, 'SELECT 1 FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone():
        _lc_ro1132_paid_completed_proof(read, world, db, row)
        return row
    return _lc_ro1132_paid_material_proof(read, world, db, row, row['phase'])


def _lc_ro1132_paid_material_proof(read, world, db, row, phase):
    """Non-recursive original material proof; never grants execution authority."""
    person = row['person_id']
    if phase not in ('pending', 'prepared', 'candidate', 'world_committed', 'completed', 'rejected'):
        unproven()
    if phase != 'rejected':
        fields = ('terminal_reason',) if row['phase'] == 'technical_blocked' else ('terminal_reason', 'resume_phase', 'diagnostic_code')
        if any((row[k] is not None for k in fields)):
            unproven()
        saved = _lc_ro1132_cursor(read, 'SELECT kind FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone() if _lc_ro1132_paid_exists(read, db, 'coordinator_activation_outcomes') else None
        if phase not in ('world_committed', 'completed') and (saved is not None or row['receipt_sha256'] is not None):
            unproven()
    if phase == 'rejected':
        _lc_ro1132_paid_rejection_proof(read, world, db, row)
        return row
    if phase == 'pending':
        if any((row[k] is not None for k in ('intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'promoted_version'))):
            unproven()
        return row
    private = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?', (row['intent_id'], person)).fetchone()
    meta = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (row['intent_id'],)).fetchone()
    if private is None or meta is None or _lc_ro1132_digest(read, private['request']) != row['request_sha256'] or (meta['base_version'] != row['base_session_version']):
        unproven()
    request = _lc_ro1132_json(read, private['request'])
    if type(request) is not dict or request.get('person_id') != person or request.get('intent_id') != row['intent_id'] or (not _lc_ro1132_body_matches(read, request, _lc_ro1132_json(read, private['body']), private['candidate'])) or (meta['person_id'] != person) or (meta['world_id'] != row['world_id']):
        unproven()
    if phase == 'prepared':
        if private['state'] != 'prepared' or private['candidate'] is not None:
            unproven()
    elif private['candidate'] is None or _lc_ro1132_digest(read, private['candidate']) != row['candidate_sha256'] or private['state'] != ('committed' if phase == 'completed' else 'candidate'):
        unproven()
    actual = _lc_ro1132_cursor(read, 'SELECT person,receipt FROM main.intents WHERE id=?', (row['intent_id'],)).fetchone()
    if phase in ('world_committed', 'completed'):
        outcome = _lc_ro1132_cursor(read, 'SELECT * FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone() if _lc_ro1132_paid_exists(read, db, 'coordinator_activation_outcomes') else None
        if actual is None or actual['person'] != person or actual['receipt'] is None or (_lc_ro1132_digest(read, actual['receipt']) != row['receipt_sha256']) or (outcome is None) or (outcome['kind'] != 'committed') or (outcome['intent_id'] != row['intent_id']) or (_lc_ro1132_digest(read, outcome['outcome_json']) != outcome['outcome_sha256']) or (_lc_ro1132_json(read, outcome['outcome_json']) != _lc_ro1132_json(read, actual['receipt'])):
            unproven()
        receipt = _lc_ro1132_json(read, actual['receipt'])
        if type(receipt) is not dict or receipt.get('status') != 'committed' or receipt.get('world_id') != row['world_id'] or (receipt.get('person_id') != person) or (receipt.get('intent_id') != row['intent_id']):
            unproven()
    elif actual is not None and actual['receipt'] is not None:
        unproven()
    if phase == 'completed':
        if row['private_closed'] != 1 or private['state'] != 'committed' or private['receipt'] is None or (_lc_ro1132_digest(read, private['receipt']) != row['receipt_sha256']) or (meta['promoted_version'] != row['promoted_version']) or (row['promoted_version'] != meta['base_version'] + 1):
            unproven()
    elif row['private_closed'] != 0 or meta['promoted_version'] is not None:
        unproven()
    return row


def _lc_ro1132_paid_rejection_proof(read, world, db, row):
    """Row-level proof only; callers separately run the complete native audit.

    Keeping this non-recursive lets the audit prove historical rejected leaves.
    """
    blocked = row['phase'] == 'technical_blocked' and row['resume_phase'] == 'rejected' and (row['private_closed'] == 0)
    nulls = ('receipt_sha256', 'promoted_version') if blocked else ('receipt_sha256', 'promoted_version', 'resume_phase', 'diagnostic_code')
    if row['phase'] != 'rejected' and (not blocked) or row['private_closed'] not in (0, 1) or any((row[k] is not None for k in nulls)):
        unproven()
    private = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?', (row['intent_id'], row['person_id'])).fetchone()
    meta = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (row['intent_id'],)).fetchone()
    actual = _lc_ro1132_cursor(read, 'SELECT * FROM main.intents WHERE id=?', (row['intent_id'],)).fetchone()
    saved = _lc_ro1132_cursor(read, 'SELECT * FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone() if _lc_ro1132_paid_exists(read, db, 'coordinator_activation_outcomes') else None
    if private is None or meta is None or actual is None or (saved is None) or (private['state'] != ('rejected' if row['private_closed'] else 'candidate')) or (private['receipt'] is not None) or (actual['receipt'] is not None) or (actual['person'] != row['person_id']) or (meta['person_id'] != row['person_id']) or (meta['world_id'] != row['world_id']) or (meta['base_version'] != row['base_session_version']) or (meta['promoted_version'] is not None) or (private['candidate'] is None) or (_lc_ro1132_digest(read, private['request']) != row['request_sha256']) or (_lc_ro1132_digest(read, private['candidate']) != row['candidate_sha256']) or (saved['kind'] != 'domain_rejected') or (saved['intent_id'] != row['intent_id']) or (_lc_ro1132_digest(read, saved['outcome_json']) != saved['outcome_sha256']):
        unproven()
    result = _lc_ro1132_json(read, saved['outcome_json'])
    metrics = _lc_ro1132_json(read, private['metrics'])
    if type(metrics) is not dict or set(metrics) != {'sdk_events', 'sdk_requests', 'provider', 'pid', 'module_root'} or type(metrics['sdk_requests']) is not int or (metrics['sdk_requests'] < 0) or (type(metrics['pid']) is not int) or (metrics['pid'] < 0) or any((type(metrics[k]) is not str or not metrics[k] for k in ('provider', 'module_root'))):
        unproven()
    events = metrics['sdk_events']
    if not (type(events) is int and events >= 0 or (type(events) is dict and all((type(k) is str and type(v) is int and (v >= 0) for k, v in events.items())))):
        unproven()
    fields = {'status', 'world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'code', 'origin', 'action', 'decision_version', 'effect_index', 'request_sha256', 'candidate_sha256', 'payload_sha256', 'decision_sha256', 'effects'}
    request = _lc_ro1132_json(read, private['request'])
    payload = _lc_ro1132_json(read, actual['payload'])
    body = _lc_ro1132_json(read, private['body'])
    if type(result) is not dict or set(result) != fields or saved['outcome_json'] != _lc_ro1132_canonical(read, result) or (request.get('person_id') != row['person_id']) or (request.get('intent_id') != row['intent_id']) or (not _lc_ro1132_body_matches(read, request, body, private['candidate'])) or (payload.get('body') != body) or any((result[k] != row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'request_sha256', 'candidate_sha256'))) or (result['status'] != 'domain_rejected') or (result['effects'] != []) or (result['payload_sha256'] != _lc_ro1132_digest(read, actual['payload'])) or (result['code'] != row['terminal_reason']) or (result['action'] not in DOMAIN_ACTIONS.get(result['code'], set())) or (result['origin'] not in ('world.apply_action', 'adjudication.execute')):
        unproven()
    world._validate_body(body)
    if result['origin'] == 'world.apply_action':
        if set(payload) != {'body', 'ruling', 'reason'} or payload['ruling'] not in ('大失败', '失败', '成功', '大成功') or type(payload['reason']) is not str or (not payload['reason'].strip()) or (len(payload['reason']) > 8000) or (result['action'] != body['action']) or any((result[k] is not None for k in ('decision_version', 'effect_index', 'decision_sha256'))):
            unproven()
    else:
        if set(payload) != {'body'}:
            unproven()
        version = result['decision_version']
        index = result['effect_index']
        if type(version) is not int or version < 1 or type(index) is not int or (index < 0) or (body['action'] != 'propose'):
            unproven()
        proposal = _lc_ro1132_cursor(read, 'SELECT world_id,person_id,text FROM dm_proposals WHERE intent_id=?', (row['intent_id'],)).fetchone()
        decision = _lc_ro1132_cursor(read, 'SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1', (row['intent_id'],)).fetchone()
        if proposal is None or tuple(proposal) != (row['world_id'], row['person_id'], body['text']) or decision is None or (decision['version'] != version) or (_lc_ro1132_digest(read, decision['data']) != result['decision_sha256']):
            unproven()
        data = _lc_ro1132_json(read, decision['data'])
        if data['world_id'] != row['world_id'] or type(data['effects']) is not list or (not 0 <= index < len(data['effects'])) or (data['effects'][index]['action'] != result['action']):
            unproven()
        from .adjudication import Adjudicator
        from types import SimpleNamespace
        if any((body.get('action') in _LIFECYCLE_ACTIONS for body in data['effects'])):
            native_read = lifecycle_native_read(db)
            try:
                compiled = _lifecycle_compile(world, data, native_read)
                _lifecycle_rejection(native_read, world, row, result, data)
            except BaseException:
                native_read.fail()
                raise
            native_read.close()
        else:
            compiled = _lc_ro1132_external_compile(read, world, data)
        for i, effect in enumerate(compiled):
            if _lc_ro1132_cursor(read, 'SELECT 1 FROM intents WHERE id=?', (Adjudicator._child_id(row['intent_id'], version, i),)).fetchone():
                unproven()
        if _lc_ro1132_cursor(read, "SELECT 1 FROM dm_timeline WHERE intent_id=? AND kind='commit'", (row['intent_id'],)).fetchone():
            unproven()
    return result


def _lc_ro1132_paid_block_payload(read, db, row, resume):
    saved = _lc_ro1132_cursor(read, 'SELECT outcome_sha256 FROM coordinator_activation_outcomes WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone() if _lc_ro1132_paid_exists(read, db, 'coordinator_activation_outcomes') else None
    return dict(schema_version=1, **{k: row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'base_session_version')}, resume_phase=resume, outcome_sha256=saved[0] if saved else None, private_closed=0)


def _lc_ro1132_paid_block_proof(read, world, db, row):
    _lc_ro1132_paid_block_witness_proof(read, world, db, row)
    _lc_ro1132_paid_material_proof(read, world, db, row, row['resume_phase'])
    return row


def _lc_ro1132_paid_block_witness_proof(read, world, db, row):
    if row['resume_phase'] not in ('pending', 'prepared', 'candidate', 'world_committed', 'rejected') or row['private_closed'] != 0 or row['promoted_version'] is not None or (type(row['diagnostic_code']) is not str) or (re.fullmatch('[A-Z][A-Z0-9_]{0,79}', row['diagnostic_code']) is None) or (not _lc_ro1132_paid_exists(read, db, 'coordinator_activation_blocks')):
        unproven()
    if not _lc_ro1132_paid_exists(read, db, 'actor_activation_delegations') or not _lc_ro1132_paid_exists(read, db, 'actor_coordinator_delegations'):
        unproven()
    binding = _lc_ro1132_cursor(read, 'SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?', (row['world_id'], row['person_id'], row['owner_actor_epoch'])).fetchone()
    parent = _lc_ro1132_cursor(read, 'SELECT parent_epoch FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?', (row['world_id'], row['person_id'], row['owner_actor_epoch'])).fetchone()
    if binding is None or tuple(binding) != (row['window_id'], row['activation_id']) or parent is None or (parent[0] != row['owner_parent_epoch']):
        unproven()
    if row['resume_phase'] != 'pending':
        integer(row['observation_revision'], 1)
        integer(row['card_version'], 1)
        integer(row['base_session_version'])
    if row['resume_phase'] == 'pending' and any((row[k] is not None for k in ('intent_id', 'request_sha256', 'candidate_sha256', 'receipt_sha256', 'promoted_version', 'observation_revision', 'card_version', 'base_session_version'))):
        unproven()
    if row['resume_phase'] == 'prepared' and any((row[k] is not None for k in ('candidate_sha256', 'receipt_sha256', 'promoted_version'))):
        unproven()
    saved = _lc_ro1132_cursor(read, 'SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone()
    if saved is None or saved['resume_phase'] != row['resume_phase'] or saved['diagnostic_code'] != row['diagnostic_code'] or (type(saved['pause_revision']) is not int) or (saved['pause_revision'] < 1) or (saved['proof_json'] != _lc_ro1132_canonical(read, _lc_ro1132_paid_block_payload(read, db, row, row['resume_phase']))) or (saved['proof_sha256'] != _lc_ro1132_digest(read, saved['proof_json'])):
        unproven()
    pause = _lc_ro1132_cursor(read, 'SELECT state FROM snapshots WHERE seq=?', (saved['pause_revision'],)).fetchone()
    if pause is None:
        unproven()
    pause = _lc_ro1132_json(read, pause[0])
    window = window_record(db, row['world_id'], row['window_id'])
    if pause['world_id'] != row['world_id'] or pause['revision'] != saved['pause_revision'] or pause['time'] != window['target_time'] or (pause['status'] != 'technical_paused') or (pause.get('diagnostic') != {'code': row['diagnostic_code'], 'time': window['target_time']}) or (saved['pause_revision'] < (row['observation_revision'] or window['advanced_revision'])):
        unproven()
    _lc_ro1132_paid_fault_issuer_proof(read, world, db, row, required=False)
    return row


def _lc_ro1132_paid_fault_issuer_schema(read, db, *, create=False, authorize=False):
    if not _lc_ro1132_paid_exists(read, db, _ISSUER_TABLE):
        if not create:
            return False
        _lc_ro1132_cursor(read, _ISSUER_DDL)
        for event in ('UPDATE', 'DELETE'):
            _lc_ro1132_cursor(read, 'CREATE TRIGGER fault_issuer_no_' + event.lower() + ' BEFORE ' + event + ' ON ' + _ISSUER_TABLE + " BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END")
        _lc_ro1132_cursor(read, _ISSUER_INSERT)
        _history_cover(db)
    if _lc_ro1132_cursor(read, 'SELECT sql FROM sqlite_master WHERE name=?', (_ISSUER_TABLE,)).fetchone()[0] != _ISSUER_DDL:
        unproven()
    for event in ('UPDATE', 'DELETE'):
        expected = 'CREATE TRIGGER fault_issuer_no_' + event.lower() + ' BEFORE ' + event + ' ON ' + _ISSUER_TABLE + " BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"
        found = _lc_ro1132_cursor(read, 'SELECT sql FROM sqlite_master WHERE name=?', ('fault_issuer_no_' + event.lower(),)).fetchone()
        if found is None or found[0] != expected:
            unproven()
    found = _lc_ro1132_cursor(read, "SELECT sql FROM sqlite_master WHERE name='fault_issuer_no_replace'").fetchone()
    if found is None and (create or authorize) or (found is not None and found[0] != _ISSUER_INSERT):
        unproven()
    return True


def _lc_ro1132_paid_decode_fault_issuer(read, raw):
    """Strict v1 history: owner fields describe the publishing fault, not a claim.

    Canonical JSON and append-only SQL are integrity checks within the trusted
    publisher/storage boundary, not authentication against a DB administrator.
    """
    try:
        value = _lc_ro1132_json(read, raw)
    except (ValueError, TypeError):
        unproven()

    def exact(obj, keys):
        if type(obj) is not dict or set(obj) != set(keys):
            unproven()

    def text(value):
        if type(value) is not str or not value.strip():
            unproven()

    def hash_text(value):
        if type(value) is not str or re.fullmatch('[0-9a-f]{64}', value) is None:
            unproven()
    exact(value, ('schema_version', 'binding', 'parent', 'pause_revision', 'block_sha256', 'meta'))
    if type(value['schema_version']) is not int or value['schema_version'] != 1:
        unproven()
    binding = value['binding']
    meta = value['meta']
    parent = value['parent']
    exact(binding, ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id', 'owner_parent_epoch', 'owner_actor_epoch'))
    exact(meta, ('intent_id', 'person_id', 'world_id', 'base_version', 'base_messages_sha256', 'base_seen_sha256', 'owner_epoch', 'owner_token', 'attempt_id'))
    for k in ('world_id', 'person_id'):
        text(binding[k])
        text(meta[k])
    for k in ('window_id', 'activation_id', 'intent_id'):
        internal_uuid(binding[k])
    for k in ('intent_id', 'owner_token', 'attempt_id'):
        internal_uuid(meta[k])
    for k in ('owner_actor_epoch', 'owner_parent_epoch'):
        integer(binding[k], 1)
    integer(meta['owner_epoch'], 1)
    integer(meta['base_version'])
    integer(value['pause_revision'], 1)
    for k in ('base_messages_sha256', 'base_seen_sha256'):
        hash_text(meta[k])
    hash_text(value['block_sha256'])
    if type(parent) is not list or len(parent) != 2:
        unproven()
    integer(parent[0], 1)
    internal_uuid(parent[1])
    if parent[0] != binding['owner_parent_epoch'] or meta['owner_epoch'] != binding['owner_actor_epoch'] or any((meta[k] != binding[k] for k in ('world_id', 'person_id', 'intent_id'))) or (_lc_ro1132_canonical(read, value) != raw):
        unproven()
    return value


def _lc_ro1132_paid_fault_issuer_proof(read, world, db, row, *, required=True):
    """Original pause-time authority, never minted from maintenance-time metadata."""
    found = _lc_ro1132_cursor(read, 'SELECT * FROM ' + _ISSUER_TABLE + ' WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone() if _lc_ro1132_paid_fault_issuer_schema(read, db, authorize=required) else None
    if found is None:
        if required:
            unproven()
        return None
    witness = _lc_ro1132_paid_decode_fault_issuer(read, found['witness_json'])
    if found['world_id'] != row['world_id'] or found['activation_id'] != row['activation_id'] or found['witness_sha256'] != _lc_ro1132_digest(read, found['witness_json']):
        unproven()
    block = _lc_ro1132_cursor(read, 'SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone()
    fault = witness['binding']
    integer(fault['owner_actor_epoch'], 1)
    integer(fault['owner_parent_epoch'], 1)
    parent = _lc_ro1132_cursor(read, 'SELECT parent_epoch,parent_owner_token FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])).fetchone()
    delegation = _lc_ro1132_cursor(read, 'SELECT window_id,activation_id FROM actor_activation_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?', (row['world_id'], row['person_id'], fault['owner_actor_epoch'])).fetchone()
    meta = _lc_ro1132_cursor(read, 'SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (row['intent_id'],)).fetchone()
    if block is None or parent is None or meta is None or (delegation is None) or any((fault[k] != row[k] for k in ('world_id', 'window_id', 'activation_id', 'person_id', 'intent_id'))) or (tuple(delegation) != (row['window_id'], row['activation_id'])) or (parent['parent_epoch'] != fault['owner_parent_epoch']) or (witness['pause_revision'] != block['pause_revision']) or (witness['block_sha256'] != block['proof_sha256']) or (witness['parent'] != list(parent)) or (witness['meta'] != {k: meta[k] for k in meta.keys() if k != 'promoted_version'}) or (meta['owner_epoch'] != fault['owner_actor_epoch']):
        unproven()
    attempt = world._scoped_attempt()
    if attempt is not None and attempt.permit.epoch == fault['owner_actor_epoch']:
        if attempt.permit.world_id != row['world_id'] or attempt.permit.person_id != row['person_id'] or attempt.permit.owner_token != meta['owner_token'] or (attempt.id != meta['attempt_id']) or (attempt.activation != scoped(world)) or (attempt.coordinator is None) or ([attempt.coordinator.epoch, attempt.coordinator.owner_token] != witness['parent']):
            unproven()
        fault_issuer_scope(world, row, attempt.permit)
    return witness


def _lc_ro1132_paid_native_audit(read, storage, db, row):
    """Full private lineage, separate from the non-recursive row material proof."""
    from .world import WorldStore
    _lc_ro1132_world_check(read, db, latest(db))
    person = row['person_id']
    if _lc_ro1132_paid_exists(read, db, 'actor_session_heads', 'actor') and _lc_ro1132_cursor(read, 'SELECT 1 FROM actor.actor_session_heads WHERE person_id=?', (person,)).fetchone():
        batch = _history_audit_batch.get()
        if batch is not None and batch.db is db:
            batch.audit(storage, db, person)
        else:
            storage.audit(db, person)
        return
    from .actor_execution import ActorRunError
    untouched = row['phase'] == 'pending' or (row['phase'] == 'technical_blocked' and row['resume_phase'] == 'pending')
    if not untouched or any((_lc_ro1132_paid_exists(read, db, table, 'actor') and _lc_ro1132_cursor(read, f'SELECT 1 FROM actor.{table} WHERE person_id=?', (person,)).fetchone() for table in ('actor_intents', 'actor_calls', 'actor_intent_execution'))):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
    session = _lc_ro1132_cursor(read, 'SELECT messages,seen FROM actor.actor_sessions WHERE person_id=?', (person,)).fetchone()
    if session is not None and any((_lc_ro1132_json(read, raw) != [] for raw in session)):
        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')


def _lc_ro1132_paid_maintenance_schema(read, db, *, create=False):
    from . import coordinator_maintenance as _lc_ro1132_maintenance
    if not _lc_ro1132_paid_exists(read, db, _lc_ro1132_maintenance._TABLE):
        if not create:
            return False
        _lc_ro1132_cursor(read, _lc_ro1132_maintenance._DDL)
        for event in ('UPDATE', 'DELETE'):
            _lc_ro1132_cursor(read, 'CREATE TRIGGER technical_maintenance_no_' + event.lower() + ' BEFORE ' + event + ' ON ' + _lc_ro1132_maintenance._TABLE + " BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END")
        _lc_ro1132_cursor(read, _lc_ro1132_maintenance._INSERT)
        _history_cover(db)
    actual = _lc_ro1132_cursor(read, 'SELECT sql FROM sqlite_master WHERE name=?', (_lc_ro1132_maintenance._TABLE,)).fetchone()[0]
    if actual != _lc_ro1132_maintenance._DDL:
        _lc_ro1132_maintenance.fail()
    for event in ('UPDATE', 'DELETE'):
        expected = 'CREATE TRIGGER technical_maintenance_no_' + event.lower() + ' BEFORE ' + event + ' ON ' + _lc_ro1132_maintenance._TABLE + " BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"
        found = _lc_ro1132_cursor(read, 'SELECT sql FROM sqlite_master WHERE name=?', ('technical_maintenance_no_' + event.lower(),)).fetchone()
        if found is None or found[0] != expected:
            _lc_ro1132_maintenance.fail()
    found = _lc_ro1132_cursor(read, "SELECT sql FROM sqlite_master WHERE name='technical_maintenance_no_replace'").fetchone()
    if found is None or found[0] != _lc_ro1132_maintenance._INSERT:
        _lc_ro1132_maintenance.fail()
    return True


def _lc_ro1132_paid_completed_proof(read, world, db, row):
    """Re-prove original witness/issuer without pretending private is unpromoted."""
    from . import coordinator_maintenance as _lc_ro1132_maintenance
    if not _lc_ro1132_paid_maintenance_schema(read, db):
        _lc_ro1132_maintenance.fail()
    saved = _lc_ro1132_cursor(read, 'SELECT * FROM ' + _lc_ro1132_maintenance._TABLE + ' WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone()
    if saved is None or saved['schema_version'] != 1 or _lc_ro1132_digest(read, saved['record_json']) != saved['record_sha256']:
        _lc_ro1132_maintenance.fail()
    record = _lc_ro1132_json(read, saved['record_json'])
    old = record['original_row']
    if old['phase'] != 'technical_blocked' or old['resume_phase'] != 'world_committed':
        _lc_ro1132_maintenance.fail()
    changed = {'phase', 'private_closed', 'promoted_version', 'resume_phase', 'diagnostic_code'}
    if set(old) != set(row.keys()) or any((old[k] != row[k] for k in old if k not in changed)):
        _lc_ro1132_maintenance.fail()
    _lc_ro1132_paid_block_witness_proof(read, world, db, old)
    if _lc_ro1132_paid_fault_issuer_proof(read, world, db, old) != record.get('original_fault_issuer'):
        _lc_ro1132_maintenance.fail()
    block = dict(_lc_ro1132_cursor(read, 'SELECT * FROM coordinator_activation_blocks WHERE world_id=? AND activation_id=?', (row['world_id'], row['activation_id'])).fetchone())
    if block != record['original_block'] or record['operation_id'] != saved['operation_id']:
        _lc_ro1132_maintenance.fail()
    _lc_ro1132_paid_material_proof(read, world, db, row, 'completed')
    return record


# Insert before the existing source register; owned only by parent1136.
_lc_adm1136_budget_context = ContextVar('lifecycle_cold1136_budget', default=None)


def _lc_adm1136_one(db, sql, params=()):
    """Finite scalar metadata or already-funded real Row, never a Row proxy."""
    owner = _lifecycle_source_owner()
    budget = getattr(db, '_attempt', None) or _lc_adm1136_budget_context.get()
    if budget is not None:
        budget.check()
    cursor = None
    primary = None
    try:
        cursor = db.execute(sql, params)
        value = cursor.fetchone()
        if budget is not None:
            budget.check()
        return value
    except BaseException as error:
        primary = error
        owner.fail(error)
        raise
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except BaseException as cleanup:
                owner.fail(primary if primary is not None else cleanup)
                if primary is None:
                    raise
                try:
                    owner.control_errors.append((cleanup, BaseException.__traceback__.__get__(cleanup),
                        BaseException.__context__.__get__(cleanup), BaseException.__cause__.__get__(cleanup)))
                except BaseException:
                    # A remains owner.error with its first native relationship
                    # tuple. Accounting is poisoned independently of diagnostics.
                    owner.memory._failed = True


def _lc_adm1136_quote_rows(db, sql, columns, role, *, params=()):
    """Length scalars in the SAME pinned transaction precede original payload."""
    owner = _lifecycle_source_owner()
    if not db.in_transaction:
        raise _LifecycleLiveLimit('first admission requires the original transaction')
    expression = '+'.join("CASE typeof(" + key + ") WHEN 'text' THEN length(CAST(" + key +
        " AS BLOB)) WHEN 'blob' THEN length(" + key + ") ELSE 8 END" for key in columns)
    row = _lc_adm1136_one(db, 'SELECT coalesce(sum(n),0),count(*),coalesce(max(n),0) FROM '
        '(SELECT ' + expression + ' AS n FROM (' + sql + '))', params)
    total, count, maximum = row
    _lifecycle_raw_charge(maximum, len(columns))  # Original 1MiB row gate.
    owner.reserve(32 * total + 8192 * count, role)


def _lc_adm1136_first_pin(db):
    """Prepay the escaping first vector, all catalog/coverage Rows and scratch.

    No fake Reader/vector or first-error-carrier reuse. Malformed epoch/coverage
    DDL is left to the unchanged original witness, in its original order. Only
    actual scalar quote queries precede payload; original pin still constructs
    and verifies every native Row/catalog/trigger/schema tuple.
    """
    from .world import _HISTORY_COVER_DDL, _HISTORY_EPOCH_DDL
    owner = _lifecycle_source_owner()
    owner.reserve(32768, 'adm1136-first-pin-metadata-cursors')
    for schema in ('main', 'actor'):
        _lc_adm1136_quote_rows(db, 'SELECT type,name,tbl_name,sql FROM ' + schema + '.sqlite_master',
            ('type','name','tbl_name','sql'), 'adm1136-first-pin-catalog-and-vector')
        for name, ddl, columns in (
                ('history_proof_epochs', _HISTORY_EPOCH_DDL,
                    ('singleton','protocol_version','store_nonce','generation')),
                ('history_proof_coverage', _HISTORY_COVER_DDL, ('table_name','table_sql'))):
            shape = _lc_adm1136_one(db, 'SELECT sql FROM ' + schema + '.sqlite_master '
                "WHERE type='table' AND name=?", (name,))
            if shape is None or shape[0] != ddl:
                # Original _history_epoch refuses before reading either table.
                break
            _lc_adm1136_quote_rows(db, 'SELECT ' + ','.join(columns) + ' FROM ' + schema + '.' + name,
                columns, 'adm1136-first-pin-coverage-and-epoch')


_ROOT1159R_RESERVE = vars(_LifecycleValueOwner)['reserve']


def _lc_adm1154_root_gate(edge, name, actual=None, *, world=None):
    """Bind a visited first-Root edge to its loaded definition, not its name."""
    from types import CodeType, FunctionType, MethodType
    from .source_attestation import _REGISTRY

    if world is not None or name == 'json.loads':
        from . import world as world_module
        space = vars(world_module)
    else:
        space = globals()
    stamp = _REGISTRY.get(id(space))
    if stamp is not None and stamp['matched'] and stamp['namespace'] is space:
        if name == '_LifecycleValueOwner.reserve' and world is None:
            cls = type(actual)
            if (cls is _LifecycleValueOwner and any(cls is old for old in stamp['objects'])
                    and 'reserve' not in getattr(actual, '__dict__', ())):
                descriptor = vars(cls).get('reserve')
                if (descriptor is _ROOT1159R_RESERVE and type(descriptor) is FunctionType
                        and descriptor.__globals__ is space and descriptor.__closure__ is None
                        and any(type(code) is CodeType and code.co_name == '_LifecycleValueOwner'
                                and any(type(body) is CodeType and body.co_name == 'reserve'
                                        and descriptor.__code__ is body for body in code.co_consts)
                                for code in stamp['code'].co_consts)):
                    bound = actual.reserve
                    if (type(bound) is MethodType and bound.__self__ is actual
                            and bound.__func__ is descriptor):
                        return bound
        elif world is not None:
            cls = type(world)
            if (cls is space.get('WorldStore') and any(cls is old for old in stamp['objects'])
                    and name not in vars(world)):
                static = name == '_execution_identity'
                descriptor = vars(cls).get(name)
                original = space['_ROOT1154_DESCRIPTORS'][0 if static else 1]
                if descriptor is original and type(descriptor) is (staticmethod if static else FunctionType):
                    inner = descriptor.__func__ if static else descriptor
                    if (type(inner) is FunctionType and inner.__globals__ is space
                            and inner.__closure__ is None
                            and any(type(code) is CodeType and code.co_name == 'WorldStore'
                                    and any(type(body) is CodeType and body.co_name == name
                                            and inner.__code__ is body for body in code.co_consts)
                                    for code in stamp['code'].co_consts)):
                        bound = getattr(world, name)
                        if (bound is inner if static else type(bound) is MethodType
                                and bound.__self__ is world and bound.__func__ is inner):
                            return bound
        elif name == 'json.loads':
            decoder, code, namespace = space['_ROOT1154_LOADS']
            if (actual is decoder and type(actual) is FunctionType and actual.__code__ is code
                    and actual.__globals__ is namespace and actual.__closure__ is None):
                return actual
        elif (type(actual) is FunctionType and space.get(name) is actual
                and actual.__globals__ is space and actual.__closure__ is None
                and any(actual is old for old in stamp['objects'])
                and any(type(code) is CodeType and code.co_name == name
                        and actual.__code__ is code for code in stamp['code'].co_consts)):
            return actual
    raise WorldError('WINDOW_RECOVERY_UNPROVEN', 'first readonly Root: ' + edge)


def _lc_adm1136_first_root(db, *, gate):
    """Fund the first real root metadata read, not the unvisited payloads."""
    proof = _lc_ro1127_context.get()
    source_owner = gate('owner lookup', '_lifecycle_source_owner', _lifecycle_source_owner)
    owner = source_owner()
    if (type(proof) is not _lc_ro1127_Proof or proof.armed or proof.ended
            or owner is not proof.owner or proof.capsule.vector is None
            or getattr(db, '_attempt', None) is not proof.entry or not db.in_transaction):
        raise _LifecycleLiveLimit('first root requires the pinned readonly proof transaction')
    # Native existence rows are constant width, with the original metadata
    # cursor/scratch allowance. Variable payloads are priced at consumption.
    owner.reserve(32768, 'adm1136-first-root-raw-and-json')
    return _lc_adm1148_root_pay


def _lc_adm1148_root_pay(db, sql, params, columns, *, gate):
    # All three original reads consume only fetchone. Keep their exact column
    # projection, bound predicate and ordering in the scalar quote.
    quote = gate('raw row quote', '_lc_adm1136_quote_rows', _lc_adm1136_quote_rows)
    gate('raw row scalar', '_lc_adm1136_one', _lc_adm1136_one)
    gate('raw row cap', '_lifecycle_raw_charge', _lifecycle_raw_charge)
    gate('raw row owner', '_lifecycle_source_owner', _lifecycle_source_owner)
    selected = 'SELECT * FROM (' + sql + ') LIMIT 1'
    quote(db, selected, columns, 'adm1136-first-root-raw-and-json', params=params)


def _lc_adm1148_root_json(raw, *, gate):
    """Price one original loads without parsing, validating or copying raw."""
    source_owner = gate('genesis owner', '_lifecycle_source_owner', _lifecycle_source_owner)
    owner = source_owner()
    # The native Row is already paid and passed the existing 1 MiB gate.
    # One linear pass using bounded scalar scratch from the prefix allowance.
    # Count potential entries/strings/containers, not legitimate JSON values.
    # A rejected token or escape is never a syntax decision made by this quote.
    kind = type(raw)
    length = len(raw) if kind in (str, bytes, bytearray) else 0
    entries = opens = 0
    if kind is str:
        quoted = escaped = False
        for char in raw:
            if quoted:
                if escaped:
                    escaped = False
                elif char == '\\':
                    escaped = True
                elif char == '"':
                    quoted = False
            elif char == '"':
                quoted = True
                entries += 1
            elif char in '[{':
                opens += 1
            elif char in ',:':
                entries += 1
    elif kind in (bytes, bytearray):
        # Bounds ASCII structural code units even with UTF-16/32 and BOM;
        # unrelated byte matches only overquote. No encoding preflight/copy.
        entries = raw.count(b'"') + raw.count(b',') + raw.count(b':')
        opens = raw.count(b'[') + raw.count(b'{')
    # Model: payload/conversion/error/numeric scratch plus possible object
    # growth, key memo, and nested decoder frames; not an RSS bound. Keep
    # the debt on this owner across loads and its possible first exception.
    reserve = gate('genesis reserve', '_LifecycleValueOwner.reserve', owner)
    reserve(8192 + 16 * length + 512 * entries + 1536 * opens,
            'adm1154-first-root-original-json')


def _lc_adm1136_member_image(cls, name, function, stamp):
    """Actual registered MODULE code image, not disk or class identity alone."""
    from types import CodeType, FunctionType
    if type(function) is not FunctionType or function.__globals__ is not stamp['namespace']:
        unproven()
    class_code = next((item for item in stamp['code'].co_consts
        if type(item) is CodeType and item.co_name == cls.__name__), None)
    code = None if class_code is None else next((item for item in class_code.co_consts
        if type(item) is CodeType and item.co_name == name), None)
    if code is None or function.__code__ is not code:
        unproven()


def _lc_adm1136_world_members():
    """Finite class-member audit used by both Work and the cold native route."""
    from .world import WorldStore, _lc_geo_Decoder
    from .source_attestation import _REGISTRY
    from types import FunctionType
    candidate = vars(WorldStore).get('_lc_geo1133_prepare_body')
    if type(candidate) is not FunctionType:
        unproven()
    namespace = candidate.__globals__
    stamp = _REGISTRY.get(id(namespace))
    if (stamp is None or not stamp['matched'] or stamp['namespace'] is not namespace
            or namespace.get('WorldStore') is not WorldStore
            or namespace.get('_lc_geo_Decoder') is not _lc_geo_Decoder
            or not all(any(cls is item for item in stamp['objects']) for cls in (WorldStore, _lc_geo_Decoder))):
        unproven()
    for cls, names in ((WorldStore, ('_lc_geo1133_mutate_body','_lc_geo1133_preview_body',
            '_lc_geo1133_prepare_body','_lc_geo1133_preview_lifecycle_body','_lc_geo1133_commit_body',
            '_lifecycle_bound_attempt','_lc_stale1141_fail_body')),
            (_lc_geo_Decoder, ('__init__','pay','space','error','scalar','descend','value'))):
        for name in names:
            function = vars(cls).get(name)
            if type(function) is not FunctionType:
                unproven()
            _lc_adm1136_member_image(cls, name, function, stamp)


def _lc_adm1136_stage_callee(function):
    """Authenticate a real Work's selected inner body before stage dispatch.

    The 1137-owned stage calls this hook before invoking function. Non-Work
    legacy operation routing and the normal result protocol stay unchanged.
    """
    work = _lifecycle_scope.get()
    if work is None:
        return
    if type(work) is not _LifecycleWork:
        unproven()
    work.check_source()
    owner = getattr(function, '__self__', None)
    actual = getattr(function, '__func__', None)
    names = ('_lc_geo1133_mutate_body','_lc_geo1133_preview_body',
        '_lc_geo1133_prepare_body','_lc_geo1133_preview_lifecycle_body','_lc_geo1133_commit_body',
        '_lifecycle_bound_attempt','_lc_stale1141_fail_body')
    name = getattr(actual, '__name__', None)
    if (owner is not work.world or name not in names
            or name in vars(owner) or vars(type(owner)).get(name) is not actual):
        unproven()
    if name in ('_lc_geo1133_preview_lifecycle_body','_lc_geo1133_commit_body',
                '_lifecycle_bound_attempt','_lc_stale1141_fail_body'):
        _lc_stale1141_registered()


class _lc_stale1141_Control:
    # Before delivery this contains only two scalars and empty diagnostic slots.
    # In particular no producer frame, connection, state, loan or authority object.
    __slots__ = ('phase', 'retry', 'first', 'ready', 'accepted')


def _lc_stale1141_registered():
    from .source_attestation import _REGISTRY
    from types import CodeType, FunctionType
    space = globals()
    stamp = _REGISTRY.get(id(space))
    if (stamp is None or stamp['namespace'] is not space or not stamp['matched']
            or not any(obj is _lc_stale1141_Control for obj in stamp['objects'])):
        unproven()
    for name in ('_lc_stale1141_registered', '_lc_stale1141_host', '_lc_stale1141_issue',
                 '_lc_stale1141_ready', '_lc_stale1141_deliver', '_lc_stale1141_raise',
                 '_lc_stale1141_accept'):
        function = space.get(name)
        if (type(function) is not FunctionType or function.__globals__ is not space
                or function.__closure__ is not None
                or not any(obj is function for obj in stamp['objects'])
                or not any(type(code) is CodeType and code.co_name == name and code is function.__code__
                           for code in stamp['code'].co_consts)):
            unproven()


def _lc_stale1141_host(work):
    from .actor_execution import LifecycleCallBudget
    work.check()
    site = work.caller.f_locals
    retry, io = site.get('retry'), site.get('io')
    if (type(retry) is not int or not 0 <= retry < 3
            or type(io) is not LifecycleCallBudget or io.entry is not work.entry
            or io.host is not work.host or io.captured_root is not work.root
            or site.get('entry') is not work.entry or site.get('job_id') != work.job_id
            or site.get('service_version') != work.service_version
            or site.get('gateway') is not work.ordinary.gateway
            or site.get('adjudicator') is not work.ordinary.adjudicator
            or work.host.real_gateway is not work.ordinary.gateway
            or work.host.adjudicator is not work.ordinary.adjudicator
            or type(work.host._lifecycle_active) is not tuple
            or len(work.host._lifecycle_active) != 3
            or work.host._lifecycle_active[0] is not work.root
            or work.host._lifecycle_active[1] is not io
            or work.host._lifecycle_active[2] is not work.ordinary.gateway):
        unproven()
    io.check()
    return retry


def _lc_stale1141_issue(world, job_id, request_sha256):
    """Called only at the authenticated mismatch predicate, inside its txn."""
    import sys
    from .world import WorldStore, _lc_geo1133_copied_result, _lc_geo1133_stage
    from .adjudication import Adjudicator
    work = _lifecycle_scope.get()
    if type(work) is not _LifecycleWork or work.world is not world or work.job_id != job_id:
        unproven()
    parent = work.entry._lifecycle_value_owner
    child = _lifecycle_source_owner()
    if (type(parent) is not _LifecycleValueOwner or parent is child or parent.memory is not child.memory
            or work.stale1141 is not None or not work.host._lock._is_owned()):
        unproven()
    # Fixed evidence and frame-view carriers are funded before obtaining
    # them. Invalid paths keep this real debit and poison, never refund it.
    parent.reserve(4096, 'stale1141-retained-control-and-first-error')
    _lc_stale1141_registered()
    retry = _lc_stale1141_host(work)
    if request_sha256 != work.caller.f_locals.get('request_sha256'):
        unproven()

    def at(frame, function):
        return (frame is not None and frame.f_code is function.__code__
                and frame.f_globals is function.__globals__)

    predicate = sys._getframe(1)
    if not at(predicate, WorldStore._lifecycle_bound_attempt):
        unproven()
    body = predicate.f_back
    if at(body, WorldStore._lc_geo1133_preview_lifecycle_body):
        phase = 'preview'
        inner = WorldStore._lc_geo1133_preview_lifecycle_body
        if work.db is not None or work.state is not None:
            unproven()
    else:
        mutation = body.f_back
        inner = WorldStore._lc_geo1133_commit_body
        if (not at(mutation, WorldStore._lc_geo1133_mutate_body)
                or not at(mutation.f_back, inner)):
            unproven()
        operation = mutation.f_locals.get('operation')
        from types import FunctionType
        if (type(operation) is not FunctionType or not at(body, operation)
                or not any(code is operation.__code__ for code in inner.__code__.co_consts)
                or mutation.f_locals.get('_precommit') is not work
                or mutation.f_locals.get('db') is not work.db or work.db is None
                or predicate.f_locals.get('state') is not work.state or work.state is None):
            unproven()
        phase = 'commit'
        body = mutation.f_back
    copied = body.f_back
    stage = copied.f_back
    # _lifecycle_value_invoke owns the producer call; exact World public and
    # ordinary Adjudicator frames tie it to the active original Host invocation.
    stage = stage.f_back
    public = getattr(WorldStore, phase + '_lifecycle')
    adjudicate = getattr(Adjudicator, phase + '_lifecycle')
    if (not at(copied, _lc_geo1133_copied_result) or not at(copied.f_back, _lifecycle_value_invoke)
            or not at(stage, _lc_geo1133_stage) or not at(stage.f_back, public)
            or not at(stage.f_back.f_back, adjudicate)
            or stage.f_back.f_back.f_back is not work.caller
            or getattr(copied.f_locals.get('function'), '__func__', None) is not inner):
        unproven()
    control = _lc_stale1141_Control()
    control.phase, control.retry = phase, retry
    control.first, control.ready, control.accepted = None, False, False
    work.stale1141 = control
    return control


def _lc_stale1141_ready(control, function):
    """No producer frames are active. Reprove actual source before closing child."""
    work = _lifecycle_scope.get()
    if (type(work) is not _LifecycleWork or work.stale1141 is not control
            or work.db is not None or work.state is not None
            or control.first is not None or control.ready or control.accepted
            or getattr(function, '__func__', None) is not getattr(type(work.world),
                '_lc_geo1133_preview_lifecycle_body' if control.phase == 'preview' else '_lc_geo1133_commit_body')
            or control.phase not in ('preview', 'commit')):
        unproven()
    with source_lock(work.world):
        _lc_adm1136_stage_callee(function)
        _lc_stale1141_registered()
        if control.retry != _lc_stale1141_host(work):
            unproven()
    control.ready = True


def _lc_stale1141_deliver(parent):
    """The child's invoke has closed and restored the original Entry owner."""
    import sys
    from .world import _lc_geo1133_stage
    work = _lifecycle_scope.get()
    if type(work) is not _LifecycleWork or work.stale1141 is None:
        return
    control = work.stale1141
    try:
        caller = sys._getframe(1)
        if (caller.f_code is not _lc_geo1133_stage.__code__
                or caller.f_globals is not _lc_geo1133_stage.__globals__
                or type(parent) is not _LifecycleValueOwner or parent is not work.entry._lifecycle_value_owner
                or parent is not _lifecycle_value_context.get() or parent.closed or parent.failed
                or parent.memory.failed or type(control) is not _lc_stale1141_Control
                or work.db is not None or work.state is not None
                or not control.ready or control.first is not None or control.accepted
                or not work.host._lock._is_owned() or control.retry != _lc_stale1141_host(work)):
            unproven()
        # Do not retain the stage frame in a new caller local on the normal
        # control exception path. The real traceback will retain only its
        # actual raising/propagating frames, with their existing paid inputs.
        del caller
        parent.control_errors.append(control)
    except BaseException as error:
        if type(parent) is _LifecycleValueOwner:
            parent.fail(error)
        raise
    try:
        _lc_stale1141_raise(control)
    except BaseException as error:
        # Resource or unexpected construction/capture failure is not the
        # authenticated control; poison before Host attempts anything else.
        if control.first is None or control.first[0] is not error:
            parent.fail(error)
        raise


def _lc_stale1141_raise(control):
    # First actual raise; no traceback surgery. Bare propagation keeps this
    # exact first TB tail and distinct first context/cause even if live fields grow.
    try:
        raise WorldError('STALE_PREVIEW', '继任裁定基准已变化')
    except WorldError as error:
        control.first = (error, BaseException.__traceback__.__get__(error),
                         BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))
        raise


def _lc_stale1141_accept(error):
    """Original Host catch only: consume an issued first-error identity once."""
    import sys
    work = _lifecycle_scope.get()
    if type(work) is not _LifecycleWork or work.caller is not sys._getframe(1):
        return False
    control = work.stale1141
    parent = _lifecycle_value_context.get()
    if (type(control) is not _lc_stale1141_Control or not control.ready or control.accepted or control.first is None
            or control.first[0] is not error or type(parent) is not _LifecycleValueOwner
            or parent is not work.entry._lifecycle_value_owner or parent.closed or parent.failed
            or parent.memory.failed or not parent.control_errors or parent.control_errors[-1] is not control
            or work.db is not None or work.state is not None
            or work.caller.f_locals.get('retry') != control.retry):
        return False
    control.accepted = True
    work.stale1141 = None
    return True


# Bound once, before any product proof can enter this module. The code from
# the source registration is the actual module frame code, not a new compile.
_PRISTINE1193_P04 = tuple((f, f.__code__) for f in (
    _p04_native_current, _p04_current_projection, _p04_current_projection_legacy,
    _lifecycle_snapshot_proof))
_PRISTINE1193_CONTENTION = vars(_PRISTINE1193_ENTRY).get('_contention')
_PRISTINE1193_CONTENTION_SOURCE = getattr(_PRISTINE1193_CONTENTION, '__wrapped__', None)
_PRISTINE1193_CONTEXTMANAGER_CODE = next((c for c in contextmanager.__code__.co_consts
    if type(c) is _Pristine1193Code and c.co_name == 'helper'), None)
_PRISTINE1193_CONTENTION_CODE = next((c for top in _pristine1193_actor._SOURCE_GENERATION['code'].co_consts
    if type(top) is _Pristine1193Code and top.co_name == 'PreparationBudget'
    for c in top.co_consts if type(c) is _Pristine1193Code and c.co_name == '_contention'), None)


from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())

# Explicit load-root routes for the World consumer proof. Data is grouped by
# phase: same restore/rule/codec closure for both quotes and the six proofs;
# post-restore mutation, card callback, proof-only and scoped native edges
# are certified only on the routes that use them. No runtime graph discovery.
from . import disease as _consumer1196_disease, scenario as _consumer1196_scenario
from .world import _lc1196_capture_consumers

_CONSUMER1196 = {
    'restore': (
        _lc1196_capture_consumers(vars(_pristine1193_actor),
            "ActorRunError.__init__ EntryBudget.__init__ PreparationBudget.__init__ "
            "PreparationBudget._contention PreparationBudget.check PreparationBudget.remaining "
            "PreparationBudget.wait PrivateStore.__init__ _ContentionAccount.owned "
            "_ContentionAccount.remaining _budget_chain body_matches canonical hashlib.sha256 json.dumps "
            "json.loads request_digest v3_body_matches valid_identity_binding valid_physical_attempt "
            "valid_request valid_usage valid_v3_request "
        ),
        _lc1196_capture_consumers(globals(),
            "WorldError _C0V2BorrowIterator.close _C0V3BorrowIterator.__init__ _C0V3BorrowIterator.close "
            "_LIFECYCLE_TABLES _LifecycleJSONScan.__init__ _LifecycleJSONScan.hex4 "
            "_LifecycleJSONScan.space _LifecycleJSONScan.string _LifecycleJSONScan.value "
            "_LifecycleLiveLease.__init__ _LifecycleLiveLease.release _LifecycleLiveLedger.__init__ "
            "_LifecycleLiveLedger.failed _LifecycleLiveLedger.hold _LifecycleLiveLedger.release "
            "_LifecycleLiveLimit _LifecycleNativeLoan.__init__ _LifecycleNativeLoan.close "
            "_LifecycleNativeLoan.fail _LifecycleNativeReader _LifecycleNativeReader.__call__ "
            "_LifecycleNativeReader.__init__ _LifecycleNativeReader._all "
            "_LifecycleNativeReader._attest_owned _LifecycleNativeReader._borrow_rows "
            "_LifecycleNativeReader._helper_scratch _LifecycleNativeReader._initial_json "
            "_LifecycleNativeReader._initialize_owned _LifecycleNativeReader._one "
            "_LifecycleNativeReader._paths _LifecycleNativeReader._paths_owned "
            "_LifecycleNativeReader._stream_query _LifecycleNativeReader.borrow "
            "_LifecycleNativeReader.check_source _LifecycleNativeReader.close _LifecycleNativeReader.fail "
            "_LifecycleNativeReader.iter_borrows _LifecycleValueLoan.__init__ _LifecycleValueLoan.close "
            "_LifecycleValueLoan.fail _LifecycleValueLoan.transfer _LifecycleValueOwner.__init__ "
            "_LifecycleValueOwner._remember_failure _LifecycleValueOwner.close _LifecycleValueOwner.fail "
            "_LifecycleValueOwner.handoff _LifecycleValueOwner.keep_reader _LifecycleValueOwner.move_to "
            "_LifecycleValueOwner.reserve _LifecycleValueOwner.take _c0_admission_busy "
            "_c0_admission_record _c0_bounded_rows _c0_collect_pin _c0_first_pin_busy "
            "_c0_preparation_pin_busy _c0_recovery_wait _c0_segment _c0_source_attest _c0_v3_acquire "
            "_c0_v3_fetch _c0_v3_fingerprint _c0_v3_material _c0_v3_original _c0_v3_text_size "
            "_history_cover _history_original_method _json _lc_adm1136_member_image _lc_adm1136_one "
            "_lc_adm1136_world_members _lc_cold1136_p04_rows _lc_cold1158_p04_catalog "
            "_lc_ro1127_account_failure _lc_ro1127_context _lc_ro1127_write_guard "
            "_lifecycle_canonical_borrow _lifecycle_copy_borrow _lifecycle_hash_owned "
            "_lifecycle_json_borrow _lifecycle_json_constant _lifecycle_json_pairs _lifecycle_live_context "
            "_lifecycle_native_call _lifecycle_plain_json _lifecycle_plain_json_borrow "
            "_lifecycle_raw_charge _lifecycle_scalar_row _lifecycle_snapshot_proof _lifecycle_source_owner "
            "_lifecycle_utf8_size _lifecycle_value_context _lifecycle_value_invoke _lifecycle_value_price "
            "_lifecycle_world_primitives _p04_committed_meeting _p04_committed_meeting_legacy "
            "_p04_committed_meeting_owned _p04_current_projection _p04_current_projection_legacy "
            "_p04_current_projection_owned _p04_native_current body_matches exists hashlib.sha256 "
            "json.loads lifecycle_native_read one_open_predicate private_binding re.compile registration "
            "schema sha source source_lock sqlite3.Connection sqlite3.DatabaseError "
            "sqlite3.OperationalError sqlite3.ProgrammingError sqlite3.Row sqlite3.SQLITE_BUSY "
            "sqlite3.SQLITE_LOCKED unproven "
        ),
        _lc1196_capture_consumers(vars(_consumer1196_disease),
            "contact_mechanism_digest hashlib.sha256 json.dumps pathology_digest "
        ),
        _lc1196_capture_consumers(vars(_consumer1196_scenario),
            "ScenarioError.__init__ _require _text _validate_economy _validate_lifecycle "
            "_validate_military _validate_projects canonical economy_digest hashlib.sha256 json.dumps "
            "re.fullmatch "
        ),
        _lc1196_capture_consumers(vars(_pristine1193_world),
            "CONTACT_RUNTIME DomainActionError.__init__ WorldError.__init__ WorldStore WorldStore.__init__ "
            "WorldStore._check_frozen_economy WorldStore._check_frozen_military "
            "WorldStore._check_frozen_p04 WorldStore._check_frozen_p04_owned "
            "WorldStore._check_frozen_pathology WorldStore._check_frozen_projects WorldStore._connection "
            "WorldStore._controller WorldStore._dynamic_state WorldStore._find WorldStore._history_segment "
            "WorldStore._lifecycle_check WorldStore._lifecycle_check_owned WorldStore._military_accounts "
            "WorldStore._p04_continuity WorldStore._p04_expected WorldStore._p04_expected_owned "
            "WorldStore._p04_target WorldStore._restore_geography WorldStore._scoped_attempt "
            "WorldStore._scoped_coordinator_attempt WorldStore._scoped_entry_budget _AttemptConnection "
            "_AttemptConnection._bounded _actor_entry_scope _bootstrap_baseline _bootstrap_derived "
            "_history_check_triggers _history_cover _history_epoch _history_failure _history_schema_check "
            "_history_trigger _history_witness _json _lc_active _lc_alive _lc_basis _lc_call_loan "
            "_lc_canonical _lc_cause_ref _lc_child _lc_child_legacy _lc_child_owned _lc_close_iterator "
            "_lc_cold1136_P04Failure.__init__ _lc_cold1136_connection _lc_cold1136_p04_signal _lc_copy "
            "_lc_count _lc_death_records _lc_death_window _lc_each _lc_effect_conditions _lc_exact "
            "_lc_fail _lc_finish_job _lc_geo1133_direct_input _lc_geo1137_Diagnostics "
            "_lc_geo1137_Diagnostics.__init__ _lc_geo1137_close _lc_geo1137_tuple _lc_geo_Decoder "
            "_lc_geo_Decoder.__init__ _lc_geo_Decoder.descend _lc_geo_Decoder.error _lc_geo_Decoder.pay "
            "_lc_geo_Decoder.scalar _lc_geo_Decoder.space _lc_geo_Decoder.value _lc_geo_borrow "
            "_lc_geo_fetch_decode _lc_geo_query _lc_hash _lc_historical _lc_history_context "
            "_lc_history_owner _lc_history_scope _lc_int _lc_iter_loans _lc_json _lc_native_snapshot_json "
            "_lc_office_place _lc_optin _lc_private_base _lc_private_base_legacy _lc_private_step_owned "
            "_lc_project_borrow _lc_project_step _lc_project_step_owned _lc_project_steps "
            "_lc_project_to_owner _lc_read _lc_request _lc_reserve _lc_response _lc_root_json _lc_root_row "
            "_lc_scratch _lc_seed _lc_select _lc_source_ref _lc_step_digest _lc_tenure _lc_text "
            "_lc_text_hash _lc_value_memory _lifecycle_effect_body _lifecycle_project "
            "_lifecycle_project_owned _lifecycle_shape _native_attempt_indexes _readonly_attach_retry "
            "closing contact_mechanism_digest contextmanager hashlib.sha256 json.JSONDecodeError "
            "json.JSONDecoder json.dumps json.loads pathology_digest sqlite3.Connection sqlite3.Error "
            "sqlite3.OperationalError sqlite3.Row sqlite3.SQLITE_BUSY sqlite3.SQLITE_LIMIT_LENGTH "
            "sqlite3.SQLITE_LOCKED sqlite3.SQLITE_TOOBIG sqlite3.connect uuid.uuid4 "
        ),
        _lc1196_capture_consumers(vars(json), "JSONDecoder.__init__ JSONDecoder.decode JSONDecoder.raw_decode "
            "JSONEncoder.__init__ JSONEncoder.encode JSONEncoder.iterencode"),
    ),
    'proof': (
        _lc1196_capture_consumers(globals(),
            "_bootstrap_baseline _bootstrap_derived _json _pristine1193_live_ticket "
            "_release_pristine_geography _release_pristine_legacy_work json.loads "
        ),
    ),
    'mutation': (
        _lc1196_capture_consumers(vars(_pristine1193_world),
            "WorldStore._encode_state WorldStore._public _lc_geo1133_reserve_dynamic hashlib.sha256 "
        ),
    ),
    'card': (
        _lc1196_capture_consumers(vars(_pristine1193_world),
            "WorldStore._event sqlite3.connect "
        ),
    ),
    'attempt': (
        _lc1196_capture_consumers(vars(_pristine1193_actor),
            "PreparationBudget PreparationBudget._busy_elapsed PreparationBudget.native_call "
            "_ContentionAccount _ContentionAccount.__init__ sqlite3.OperationalError sqlite3.SQLITE_BUSY "
            "sqlite3.SQLITE_LOCKED time.monotonic "
        ),
        _lc1196_capture_consumers(vars(_pristine1193_world),
            "_AttemptConnection._bounded _AttemptConnection.commit _AttemptConnection.execute "
            "sqlite3.Connection "
        ),
    ),
}

