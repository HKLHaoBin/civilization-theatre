"""Atomic local world. All mechanisms are uncalibrated fictional game rules.

Only this module writes authority. Each operation owns and closes its SQLite
connection; SQLite context managers alone do not close handles on Windows.
"""
from contextlib import closing, contextmanager
from contextvars import ContextVar


# Trusted, process-local call scope; never persisted or supplied by a role.
_actor_attempt_scope = ContextVar('world_actor_attempt', default=None)
_coordinator_attempt_scope = ContextVar('world_coordinator_attempt', default=None)
_world_mutation_scope = ContextVar('world_mutation_connection', default=None)
_actor_entry_scope = ContextVar('world_actor_entry_budget', default=None)


import hashlib
import heapq
from fractions import Fraction
import json
from pathlib import Path
import sqlite3
import uuid
import time
from .execution import ExecutionPermit
from .disease import pathology_digest, contact_mechanism_digest, CONTACT_RUNTIME
from .scenario import validate_scenario, scenario_digest, ScenarioError, load_scenario

DAY = 86400
DATA = Path(__file__).with_name('data')


class WorldError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


class DomainActionError(WorldError):
    """Internal business producer; never derived from a provider's error code."""
    def __init__(self, code, message, action):
        super().__init__(code, message)
        self.action = action


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


# Connection-local pure derivations; never authority or exported world state.
_bootstrap_material_context = ContextVar('world_bootstrap_material', default=None)


@contextmanager
def _bootstrap_material_scope(world, db, *, enabled=True):
    if not enabled:
        yield
        return
    # Four single-entry slots; no growth with snapshot count. Different scopes,
    # even on the same connection, own independent material and restore nesting.
    cache = {}
    token = _bootstrap_material_context.set((world, db, cache))
    try:
        yield
    finally:
        cache.clear()
        _bootstrap_material_context.reset(token)


def _bootstrap_derived(db, slot, raw, build):
    if slot not in ('baseline', 'genesis-sha', 'normalized', 'snapshot-normalized'):
        raise ValueError('INVALID_BOOTSTRAP_MATERIAL_SLOT')
    scope = _bootstrap_material_context.get()
    if scope is None or scope[1] is not db or type(raw) is not str:
        return build()
    cache = scope[2]
    entry = cache.get(slot)
    if entry is not None and entry[0] == raw:
        return entry[1]
    # Remove stale material before a fallible rebuild. Only successful pure
    # derivations are retained, never a passed proof or a current mutable state.
    cache.pop(slot, None)
    value = build()
    cache[slot] = (raw, value)
    return value


def _bootstrap_baseline(db, raw):
    # Only private proof readers receive this object. It must never be assigned
    # into a restored state; geography below is deliberately decoded afresh.
    return _bootstrap_derived(db, 'baseline', raw, lambda: json.loads(raw))


def _native_attempt_indexes():
    # Saturation preserves the legacy three-attempt classifier's index witness.
    yield 0
    yield 1
    while True:
        yield 2


def _readonly_attach_retry(db, attempt, function, args, kwargs):
    # Only a fresh legacy entry's original native RO admission may use its
    # remaining allowance. No transaction/payload/commit or preparation retry.
    from .actor_execution import EntryBudget
    return (type(attempt) is EntryBudget and not db.in_transaction
            and function == sqlite3.Connection.execute.__get__(db)
            and type(args) is tuple and len(args) == 2
            and args[0] == 'ATTACH DATABASE ? AS actor'
            and type(args[1]) is tuple and len(args[1]) == 1
            and type(args[1][0]) is str and args[1][0].endswith('?mode=ro')
            and type(kwargs) is dict and not kwargs)


class _AttemptConnection(sqlite3.Connection):
    """Short SQLite waits with the captured attempt's independent final gate."""
    def _bounded(self, function, *args, **kwargs):
        attempt = self._attempt
        from .actor_execution import PreparationBudget
        if type(attempt) is PreparationBudget:
            attempt.check()
            super().execute('PRAGMA busy_timeout=0')
            return attempt.native_call(function, *args, **kwargs)
        end = min(attempt.deadline, getattr(attempt, 'busy_deadline', float('inf')),
                  time.monotonic() + attempt.policy.busy_budget_seconds)
        for index in _native_attempt_indexes():
            attempt.check()
            remaining = end - time.monotonic()
            if remaining <= 0:
                attempt.check()
                raise WorldError('EXECUTION_STORAGE_BUSY', '执行存储繁忙')
            # Use the base method for this local connection setting, not recursion.
            # Windows' native busy handler can substantially overshoot even
            # a 50ms request. Keep native waits minimal; Event handles backoff.
            super().execute('PRAGMA busy_timeout=' + str(max(1, int(min(.001, remaining)*1000))))
            try:
                return function(*args, **kwargs)
            except sqlite3.OperationalError as error:
                attempt.check()  # Cancellation/deadline wins over a simultaneous BUSY.
                if getattr(error, 'sqlite_errorcode', 0) & 255 not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                if index == 2 and not (getattr(error, 'sqlite_errorcode', None) == sqlite3.SQLITE_BUSY and _readonly_attach_retry(self, attempt, function, args, kwargs)):
                    raise WorldError('EXECUTION_STORAGE_BUSY', '执行存储繁忙') from None
                attempt.stop.wait(min((.05, .10, .10)[index], max(0, end-time.monotonic())))
        raise AssertionError('unreachable')

    def execute(self, *args, **kwargs):
        return self._bounded(super().execute, *args, **kwargs)

    def commit(self):
        return self._bounded(super().commit)



# P09 opt-in facts and causal reconstruction. No new SQL authority/storage.
_LC_VERSION = 'p09-lifecycle-v1'
_LC_WAKEUP = 'p04-event-wakeup-v2-lifecycle'
_LC_MAX = 2**63-1
_LC_ACTIONS = {
    'resolve_physical_attempt': {'action','outcome','basis_refs','reason'},
    'write_office_document': {'action','text','quote_start','quote_end'},
    'read_office_document': {'action','document_id'},
}
_LC_FIELDS = {
    'origin': 'kind introduced_revision identity source_ref',
    'identity': 'id name country_id office office_id location',
    'tenure': 'id office_id person_id from_revision to_revision basis_refs source_ref',
    'death': 'id person_id injury_id occurred_at committed_revision cause_ref node_id source_snapshot_revision',
    'vacancy': 'id office_id former_person_id death_id opened_revision status generation',
    'job': 'id kind vacancy_id generation attempts result',
    'attempt': 'request_id prepared_revision request request_sha256 response_raw response decision_sha256 status failure_code',
    'document': 'id author_person_id office_id place_id text created_revision cause_ref access',
    'delivery': 'id document_id recipient_person_id delivered_revision time channel cause_ref text_sha256',
    'request': 'kind protocol world job_id vacancy_id generation request_id base_revision proposed_person_id office death_ref established_facts unresolved_background_slots conflicts service_version',
    'result': 'job_id vacancy_id generation status person_id committed_revision request_sha256 decision_sha256',
}


def _lc_fail(message='生命周期因果材料不可证明'):
    raise WorldError('ACTOR_RECOVERY_UNPROVEN', message)


def _lc_exact(value, fields):
    if type(value) is not dict or set(value) != set(fields.split() if type(fields) is str else fields):
        _lc_fail('生命周期字段不精确')
    return value


def _lc_int(value, minimum=0):
    if type(value) is not int or not minimum <= value <= _LC_MAX:
        _lc_fail('生命周期整数越界')
    return value


def _lc_text(value, maximum=2000):
    if type(value) is not str or not value or value.isspace() or len(value) > maximum:
        _lc_fail('生命周期文本非法')
    from .coordinator_scheduler import _lifecycle_utf8_size
    try:_lifecycle_utf8_size(value)
    except UnicodeError:_lc_fail('生命周期文本不可UTF-8编码')
    return value


def _lc_read(read, table, equality=(), columns=None, order=(), limit=None, first=False):
    memory = _lc_value_memory(read)
    from .coordinator_scheduler import _lifecycle_value_context
    owner = _lifecycle_value_context.get()
    if memory is None or owner is None:
        return read(table, equality, columns, order, limit, first)
    if owner.memory is not memory:_lc_fail('借用不属于当前材料账')
    loan = read.borrow(table, equality, columns, order, limit, first)
    # The producer is the exact trusted Native/V3 type, not a foreign callable.
    # Its frozen loan wire is value/close/fail, independent of producer class.
    owner.loans.append(loan)
    return loan.value

def _lc_step_digest(expected, owner):
    from .coordinator_scheduler import _lifecycle_value_context
    token = _lifecycle_value_context.set(owner) if owner is not None else None
    try:
        # The fixed history carrier prices the escaping 32-byte digest. The
        # temporary 64-char hexadecimal result belongs to this generation.
        return bytes.fromhex(_lc_hash(expected))
    finally:
        if token is not None:_lifecycle_value_context.reset(token)

def _lc_value_memory(read):
    # Exact class identity is checked against the source registry, not only a
    # replaceable module binding. No foreign .borrow object becomes authority.
    from . import coordinator_scheduler as scheduler
    from .source_attestation import _REGISTRY
    native = scheduler._LifecycleNativeReader
    material = scheduler.__dict__.get('_LifecycleMaterialReader')
    kind = type(read)
    if kind is not native and (material is None or kind is not material):return None
    namespace = scheduler.__dict__
    stamp = _REGISTRY.get(id(namespace))
    if (stamp is None or stamp['namespace'] is not namespace or not stamp['matched']
            or not any(kind is original for original in stamp['objects'])):
        _lc_fail('借用reader不是受证原类型')
    for name in ('borrow', 'check_source'):
        function = getattr(kind, name, None)
        bound = getattr(read, name, None)
        if (getattr(bound, '__func__', None) is not function
                or getattr(function, '__globals__', None) is not namespace
                or not scheduler._history_original_method(function, stamp)):
            _lc_fail('借用reader原方法绑定变更')
    return read.memory

def _lc_reserve(value, factor, role):
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_value_price
    owner = _lifecycle_value_context.get()
    if owner is not None:
        charge, wire, nodes = _lifecycle_value_price(value, owner.memory)
        owner.reserve(factor * charge + 4096, role)


def _lc_canonical(value):
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_canonical_borrow
    owner = _lifecycle_value_context.get()
    if owner is None:return _json(value)
    return owner.take(_lifecycle_canonical_borrow(value, memory=owner.memory, role='world-canonical'))


def _lc_text_hash(value):
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_utf8_size
    owner = _lifecycle_value_context.get()
    if owner is None:return hashlib.sha256(value.encode('utf-8')).hexdigest()
    memory = owner.memory
    try:
        width = _lifecycle_utf8_size(value)
        scratch = memory.hold(width + 4096, 'world-text-hash-utf8')
        owner.leases.append(memory.hold(256, 'world-text-hash-result'))
        encoded = value.encode('utf-8')
        result = hashlib.sha256(encoded).hexdigest()
        encoded = value = None
    except BaseException:
        memory.failed = True
        raise
    scratch.release()
    return result


def _lc_hash(value):
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_hash_owned
    owner = _lifecycle_value_context.get()
    if owner is not None:
        return _lifecycle_hash_owned(value, memory=owner.memory)
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _lc_copy(value):
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_copy_borrow
    owner = _lifecycle_value_context.get()
    if owner is not None:
        return owner.take(_lifecycle_copy_borrow(value, memory=owner.memory, role='world-copy'))
    return json.loads(_json(value))


def _lc_json(raw):
    _lc_text(raw, 1048576)
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_json_borrow
    owner = _lifecycle_value_context.get()
    if owner is not None:
        try:
            return owner.take(_lifecycle_json_borrow(raw, memory=owner.memory, role='world-json'))
        except ValueError as error:
            if error.args == ('duplicate lifecycle JSON key',):_lc_fail('生命周期JSON重复键')
            if error.args == ('non-finite lifecycle JSON constant',):_lc_fail('生命周期JSON非有限值')
            _lc_fail('生命周期JSON非法')
        except (TypeError, UnicodeError, RecursionError):
            _lc_fail('生命周期JSON非法')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:_lc_fail('生命周期JSON重复键')
            result[key] = value
        return result
    def constant(value):
        _lc_fail('生命周期JSON非有限值')
    try:
        value = json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
        _json(value).encode('utf-8')
        return value
    except (ValueError, TypeError, UnicodeError, RecursionError):
        _lc_fail('生命周期JSON非法')


def _lc_optin(genesis):
    config=genesis['_config']; wake=config.get('event_wakeup')
    return (type(wake) is dict and wake.get('version')==_LC_WAKEUP
            and type(config.get('lifecycle')) is dict and config['lifecycle'].get('version')==_LC_VERSION)


def _lc_seed(genesis):
    from .scenario import _validate_lifecycle
    config=genesis['_config']
    if not _lc_optin(genesis):_lc_fail('非首快照共同optin')
    _validate_lifecycle(config['lifecycle'],config['countries'],config['event_wakeup'])
    identities=[]
    for country in config['countries']:
        for p in country['positions']:
            identities.append(dict(id=p['person_id'],name=p['person_name'],country_id=country['id'],
                office=p['office'],office_id=p['id'],location=p['location']))
    for p in config['event_wakeup']['temporary_persons']:
        identities.append({k:p[k] for k in ('id','name','country_id','office','location')}|{'office_id':None})
    actual=[{k:p.get(k) for k in _LC_FIELDS['identity'].split()} for p in genesis['persons']]
    if actual!=identities:_lc_fail('首人物与声明不符')
    origins={};tenures=[]
    for identity in identities:
        pid=identity['id']; ref=dict(kind='genesis',revision=1,record_id=pid)
        origins[pid]=dict(kind='genesis',introduced_revision=1,identity=identity,source_ref=ref)
        if identity['office_id'] is not None:
            tenures.append(dict(id='tenure-'+identity['office_id']+':'+pid+':1',office_id=identity['office_id'],
                person_id=pid,from_revision=1,to_revision=None,basis_refs=[],source_ref=ref))
    return dict(version=_LC_VERSION,rules=_lc_copy(config['lifecycle']),person_origins=origins,
        deaths=[],tenures=tenures,vacancies=[],jobs=[],documents=[],deliveries=[],transitions=[])


def _lc_alive(state, pid):
    life=state.get('_lifecycle')
    return life is None or (pid in life['person_origins'] and not any(d['person_id']==pid for d in life['deaths']))


def _lc_tenure(state,pid,revision=None):
    if '_lifecycle' not in state:return None
    revision=state['revision'] if revision is None else revision
    matches=[t for t in state['_lifecycle']['tenures'] if t['person_id']==pid
        and t['from_revision']<=revision and (t['to_revision'] is None or revision<t['to_revision'])]
    if len(matches)>1:_lc_fail('重叠任期')
    return matches[0] if matches else None


def _lc_active(state,pid):
    if not _lc_alive(state,pid):return False
    identity=state['_lifecycle']['person_origins'][pid]['identity'] if '_lifecycle' in state else None
    return identity is None or identity['office_id'] is None or _lc_tenure(state,pid) is not None


def _lc_cause_ref(cause):
    _lc_exact(cause,'parent_intent_id decision_version effect_index child_intent_id')
    parent=_lc_text(cause['parent_intent_id'],128)
    _lc_int(cause['decision_version'],1);_lc_int(cause['effect_index'])
    if cause['child_intent_id']!=f"dm:{len(parent)}:{parent}:{cause['decision_version']}:{cause['effect_index']}":
        _lc_fail('子效果ID不符')


def _lc_source_ref(ref):
    if type(ref) is not dict:_lc_fail()
    kind=ref.get('kind')
    if kind=='genesis':
        _lc_exact(ref,'kind revision record_id')
        if type(ref['revision']) is not int or ref['revision']!=1:_lc_fail()
        _lc_text(ref['record_id'],128)
    elif kind=='dm_child':
        _lc_exact(ref,'kind parent_intent_id decision_version effect_index child_intent_id')
        _lc_cause_ref({k:v for k,v in ref.items() if k!='kind'})
    elif kind=='due_event':
        _lc_exact(ref,'kind node_id injury_id committed_revision');_lc_int(ref['committed_revision'],1)
        _lc_text(ref['node_id'],1024);_lc_text(ref['injury_id'],1024)
    elif kind=='lifecycle_job':
        _lc_exact(ref,'kind job_id generation request_id committed_revision')
        _lc_int(ref['generation'],1);_lc_int(ref['committed_revision'],1)
        _lc_text(ref['job_id'],1024);_lc_text(ref['request_id'],128)
    else:_lc_fail('未知生命周期来源')


def _lifecycle_shape(state,genesis):
    """Typed declaration/structure only. Causal authority is _lifecycle_project."""
    try:
        if not _lc_optin(genesis):
            wake=state['_config'].get('event_wakeup')
            if ('_lifecycle' in state or 'lifecycle' in state['_config']
                    or (type(wake) is dict and wake.get('version')==_LC_WAKEUP)):_lc_fail('旧局不得迁移')
            return None
        seed=_lc_seed(genesis);life=state['_lifecycle']
        _lc_exact(life,seed)
        if (life['version']!=_LC_VERSION or _lc_canonical(life['rules'])!=_lc_canonical(seed['rules'])
                or _lc_canonical(state['_config']['lifecycle'])!=_lc_canonical(seed['rules'])
                or _lc_canonical(state['_config']['event_wakeup'])!=_lc_canonical(genesis['_config']['event_wakeup'])):_lc_fail('开局规则漂移')
        _lc_int(state['revision'],1);_lc_int(state['time'])
        origins=life['person_origins']
        if type(origins) is not dict:_lc_fail()
        people={p['id']:p for p in state['persons']}
        if len(people)!=len(state['persons']) or set(people)!=set(origins):_lc_fail('人物集合不符')
        for pid,origin in origins.items():
            _lc_text(pid,128);_lc_exact(origin,_LC_FIELDS['origin']);_lc_exact(origin['identity'],_LC_FIELDS['identity'])
            _lc_int(origin['introduced_revision'],1);_lc_source_ref(origin['source_ref'])
            if origin['kind'] not in ('genesis','dm_introduction') or origin['introduced_revision']>state['revision']+1:_lc_fail()
            if origin['identity']['id']!=pid:_lc_fail()
            for key in ('id','name','country_id','office'):
                _lc_text(origin['identity'][key],128)
                if people[pid][key]!=origin['identity'][key]:_lc_fail('永久身份漂移')
            if people[pid].get('office_id')!=origin['identity']['office_id']:_lc_fail('职位ID漂移')
            if pid not in state['_observations'] or pid not in state['_messages']:_lc_fail('人物会话地图缺失')
        for key,spec in [('tenures','tenure'),('deaths','death'),('vacancies','vacancy'),('jobs','job'),('documents','document'),('deliveries','delivery')]:
            records=life[key]
            if type(records) is not list:_lc_fail()
            seen=set()
            for record in records:
                _lc_exact(record,_LC_FIELDS[spec]);rid=_lc_text(record['id'],1024)
                if rid in seen:_lc_fail('生命周期重复ID')
                seen.add(rid)
        occupied=set()
        for t in life['tenures']:
            _lc_int(t['from_revision'],1);_lc_source_ref(t['source_ref'])
            if t['to_revision'] is not None:
                _lc_int(t['to_revision'],1)
                if not t['from_revision']<t['to_revision']<=state['revision']+1:_lc_fail()
            if (t['person_id'] not in origins or origins[t['person_id']]['identity']['office_id']!=t['office_id']
                    or t['id']!='tenure-'+t['office_id']+':'+t['person_id']+':'+str(t['from_revision'])):_lc_fail()
            if t['to_revision'] is None:
                if t['office_id'] in occupied:_lc_fail('并行任职')
                occupied.add(t['office_id'])
        for d in life['deaths']:
            _lc_int(d['occurred_at']);_lc_int(d['committed_revision'],1);_lc_int(d['source_snapshot_revision'],1);_lc_cause_ref(d['cause_ref'])
            if d['id']!='death-'+d['injury_id'] or d['person_id'] not in origins:_lc_fail()
        if len({d['person_id'] for d in life['deaths']})!=len(life['deaths']):_lc_fail('重复死亡')
        for j in life['jobs']:
            if j['kind']!='succession' or type(j['attempts']) is not list or type(j['generation']) is not int or j['generation']!=1:_lc_fail()
            for a in j['attempts']:
                _lc_exact(a,_LC_FIELDS['attempt']);_lc_exact(a['request'],_LC_FIELDS['request']);_lc_int(a['prepared_revision'],1)
                if a['status'] not in ('pending','stale','failed','committed') or a['request_sha256']!=_lc_hash(a['request']):_lc_fail()
                if a['status']=='committed':
                    if _lc_json(a['response_raw'])!=a['response'] or a['decision_sha256']!=_lc_hash(a['response']):_lc_fail()
                elif any(a[k] is not None for k in ('response_raw','response','decision_sha256')):_lc_fail()
                if a['status']=='failed':_lc_text(a['failure_code'],128)
                elif a['failure_code'] is not None:_lc_fail()
            if j['result'] is not None:_lc_exact(j['result'],_LC_FIELDS['result'])
        kinds={
            'mortal_injury':'id kind person_id actor_person_id cause_ref occurred_at committed_revision basis_refs means_id node',
            'person_death':_LC_FIELDS['death']+' kind',
            'person_introduced':'id kind person_id committed_revision job_id generation request_id',
            'office_vacated':'id kind office_id person_id committed_revision source_ref',
            'office_assumed':'id kind office_id person_id committed_revision source_ref',
            'task_interrupted':'id kind person_id committed_revision original_node death_id',
            'wake_cancelled':'id kind person_id committed_revision original_node death_id',
        }
        if type(life['transitions']) is not list:_lc_fail()
        seen=set()
        for t in life['transitions']:
            if type(t) is not dict or t.get('kind') not in kinds:_lc_fail('未知transition')
            _lc_exact(t,kinds[t['kind']]);_lc_int(t['committed_revision'],1)
            if t['committed_revision']>state['revision']+1 or t['id'] in seen:_lc_fail()
            seen.add(t['id'])
            if 'cause_ref' in t:_lc_cause_ref(t['cause_ref'])
            if 'source_ref' in t:_lc_source_ref(t['source_ref'])
        return life
    except (KeyError,TypeError,ValueError,IndexError,OverflowError,RecursionError):
        _lc_fail()


def _lc_basis(state,refs,revision,allowed):
    if type(refs) is not list or not 1<=len(refs)<=8:_lc_fail('缺少事实依据')
    resolved=[];seen=set();life=state['_lifecycle'];rules=life['rules']
    mapping={'personal_object':rules['personal_objects'],'scene_person':state['persons'],
        'institution':rules['institutional_facts'],'relationship':rules['relationship_facts'],
        'death':life['deaths'],'vacancy':life['vacancies']}
    for ref in refs:
        _lc_exact(ref,'kind id at_revision');_lc_text(ref['id'],1024)
        if (ref['kind'] not in allowed or type(ref['at_revision']) is not int or ref['at_revision']!=revision
                or (ref['kind'],ref['id']) in seen):_lc_fail('依据版本/种类/重复')
        values=[v for v in mapping[ref['kind']] if v['id']==ref['id']]
        if len(values)!=1:_lc_fail('未知依据')
        resolved.append((ref['kind'],values[0]));seen.add((ref['kind'],ref['id']))
    return resolved


def _lifecycle_effect_body(body):
    if type(body) is not dict or body.get('action') not in _LC_ACTIONS:_lc_fail('未知生命周期效果')
    _lc_exact(body,_LC_ACTIONS[body['action']])
    if body['action']=='resolve_physical_attempt':
        if body['outcome'] not in ('no_contact','nonfatal','mortal'):_lc_fail('未知物理后果')
        _lc_text(body['reason'])
        if type(body['basis_refs']) is not list or not 1<=len(body['basis_refs'])<=8:_lc_fail()
        for ref in body['basis_refs']:
            _lc_exact(ref,'kind id at_revision');_lc_int(ref['at_revision'],1);_lc_text(ref['id'],1024)
    elif body['action']=='write_office_document':
        _lc_text(body['text'],8000);_lc_int(body['quote_start']);_lc_int(body['quote_end'])
    else:_lc_text(body['document_id'],1024)


def _lc_office_place(state,person,action):
    place=next((p for p in state['places'] if p['id']==person['location']),None)
    if place is None:raise DomainActionError('NOT_AT_LOCATION','须在机构实际现场',action)
    tenure=_lc_tenure(state,person['id'])
    facts=[f for f in state['_lifecycle']['rules']['institutional_facts'] if f['office_id']==person.get('office_id')]
    if (person['office']!='finance' or tenure is None or place['country_id']!=person['country_id']
            or WorldStore._controller(state,place)!=person['country_id']):
        raise DomainActionError('NOT_AUTHORIZED','须当前本国财政任职及本国实际控制',action)
    if not any(f['place_id']==place['id'] for f in facts):
        raise DomainActionError('NOT_AT_LOCATION','不在既成机构现场',action)
    return place


def _lc_effect_conditions(state,person,proposal,body,ruling,base_revision):
    _lifecycle_effect_body(body)
    if not _lc_active(state,person['id']):raise WorldError('ACTOR_EXECUTION_PERSON_DEAD','人物已亡或无当前任期')
    action=body['action'];life=state['_lifecycle']
    if action=='resolve_physical_attempt':
        from .actor_execution import valid_physical_attempt
        attempt=proposal.get('physical_attempt')
        if not valid_physical_attempt(attempt) or attempt is None:_lc_fail('没有原人物物理意图')
        resolved=_lc_basis(state,body['basis_refs'],base_revision,{'personal_object','scene_person','institution','relationship'})
        target=WorldStore._find(state['persons'],attempt['target_person_id'],'ACTOR_RECOVERY_UNPROVEN')
        if target['id']==person['id']:_lc_fail('物理目标须另一人物')
        if not _lc_alive(state,target['id']):raise DomainActionError('TARGET_NOT_ALIVE','目标已经死亡',action)
        if person['location'] is None or target['location'] is None or person['location']!=target['location']:
            raise DomainActionError('NOT_AT_LOCATION','行动双方未实际同地',action)
        means=next((m for m in life['rules']['personal_objects'] if m['id']==attempt['means_id']),None)
        if means is None:_lc_fail('未知原物')
        if means['holder_person_id']!=person['id']:
            raise DomainActionError('PERSONAL_MEANS_REQUIRED','本人未实际持有该原物',action)
        if not any(k=='personal_object' and v['id']==means['id'] for k,v in resolved):_lc_fail('缺少原物依据')
        if not any(k=='scene_person' and v['id']==target['id'] for k,v in resolved):_lc_fail('缺少目标现场依据')
        if body['outcome']=='mortal':
            if ruling not in ('成功','大成功'):_lc_fail('失败档不可兑现致命伤')
            if any(t['kind']=='mortal_injury' and t['person_id']==target['id'] for t in life['transitions']):
                raise DomainActionError('MORTAL_INJURY_PENDING','目标已有致命伤',action)
        return target,means
    place=_lc_office_place(state,person,action)
    if action=='write_office_document':
        start,end=body['quote_start'],body['quote_end'];text=proposal['text']
        if not start<end<=len(text) or text[start:end]!=body['text']:_lc_fail('公文非原文Unicode片段')
        return place
    document=next((d for d in life['documents'] if d['id']==body['document_id']),None)
    if document is None:_lc_fail('未知文书')
    if document['office_id']!=person.get('office_id'):
        raise DomainActionError('NOT_AUTHORIZED','无该文书当前职位权限',action)
    if document['place_id']!=place['id']:raise DomainActionError('NOT_AT_LOCATION','文书不在本人现场',action)
    return document


def _lc_request(state,job,request_id,proposed_person_id,revision,service_version):
    reserved={p['id'] for p in state['persons']}|{p['id'] for p in state['places']}|{c['id'] for c in state['countries']}
    reserved|={p['office_id'] for p in state['persons'] if p.get('office_id') is not None}
    for key in ('personal_objects','institutional_facts','relationship_facts'):
        reserved.update(f['id'] for f in state['_lifecycle']['rules'][key])
    if proposed_person_id in reserved:_lc_fail('新人物ID与既成实体冲突')
    vacancy=WorldStore._find(state['_lifecycle']['vacancies'],job['vacancy_id'],'ACTOR_RECOVERY_UNPROVEN')
    death=WorldStore._find(state['_lifecycle']['deaths'],vacancy['death_id'],'ACTOR_RECOVERY_UNPROVEN')
    former=WorldStore._find(state['persons'],vacancy['former_person_id'],'ACTOR_RECOVERY_UNPROVEN')
    rules=state['_lifecycle']['rules']; facts=[];slots=[]
    institutions=[f for f in rules['institutional_facts'] if f['office_id']==vacancy['office_id']]
    for kind,records in [('institution',institutions),('relationship',[f for f in rules['relationship_facts'] if
            state['_lifecycle']['person_origins'][f['subject_person_id']]['identity']['country_id']==former['country_id']])]:
        for f in records:facts.append(dict(kind=kind,record=_lc_copy(f),source_ref=dict(kind='genesis',revision=1,record_id=f['id'])))
    for f in institutions:
        slots.append(dict(id='slot-'+f['id'],office_id=f['office_id'],background_role=f['background_role'],
            place_id=f['place_id'],permitted_fields=['name','card']))
    conflicts=[dict(kind='institution',id=f['id'],at_revision=revision) for f in institutions] if len(institutions)>1 else []
    return dict(kind='succession',protocol=_LC_VERSION,world=dict(world_id=state['world_id'],revision=revision,time=state['time']),
        job_id=job['id'],vacancy_id=vacancy['id'],generation=job['generation'],request_id=request_id,base_revision=revision,
        proposed_person_id=proposed_person_id,office=dict(id=vacancy['office_id'],country_id=former['country_id'],office=former['office']),
        death_ref=dict(death_id=death['id'],person_id=death['person_id'],committed_revision=death['committed_revision']),
        established_facts=facts,unresolved_background_slots=slots,conflicts=conflicts,service_version=service_version)


def _lc_response(state,request,response_raw,response):
    if _lc_json(response_raw)!=response:_lc_fail('响应原文与解码不同')
    _lc_exact(response,'status reason basis_refs successor unresolved');_lc_text(response['reason'])
    if response['status'] not in ('appointed','vacant','disputed'):_lc_fail('未知继任结果')
    unresolved=response['unresolved']
    if type(unresolved) is not list or len(unresolved)>8:_lc_fail()
    for item in unresolved:_lc_text(item)
    refs=_lc_basis(state,response['basis_refs'],request['base_revision'],{'institution','relationship','death','vacancy'})
    allowed={(f['kind'],f['record']['id']) for f in request['established_facts']}|{
        ('death',request['death_ref']['death_id']),('vacancy',request['vacancy_id'])}
    if any((k,r['id']) not in allowed for k,r in refs):_lc_fail('依据不属于该请求')
    slots=request['unresolved_background_slots'];conflicts=request['conflicts']
    if response['status']=='appointed':
        successor=_lc_exact(response['successor'],'name background_role location card')
        _lc_text(successor['name'],128);_lc_exact(successor['card'],'values interests risk mission')
        for value in successor['card'].values():_lc_text(value)
        if (request['office']['office']!='finance' or len(slots)!=1 or conflicts
                or successor['background_role']!=slots[0]['background_role'] or successor['location']!=slots[0]['place_id']
                or any(p['name']==successor['name'] for p in state['persons'])
                or not any(k=='institution' and 'slot-'+r['id']==slots[0]['id'] for k,r in refs)):_lc_fail('无唯一可证自动代理条件')
    else:
        if response['successor'] is not None:_lc_fail('空缺/争议不得引人')
        if response['status']=='disputed' and (not conflicts or not all(ref in response['basis_refs'] for ref in conflicts)):_lc_fail('无具证冲突')
    return _lc_hash(response)


def _lc_finish_job(state,job,attempt,response_raw,response,revision):
    request=attempt['request'];decision_sha=_lc_response(state,request,response_raw,response)
    vacancy=WorldStore._find(state['_lifecycle']['vacancies'],job['vacancy_id'],'ACTOR_RECOVERY_UNPROVEN')
    pid=request['proposed_person_id'] if response['status']=='appointed' else None
    result=dict(job_id=job['id'],vacancy_id=job['vacancy_id'],generation=job['generation'],status=response['status'],
        person_id=pid,committed_revision=revision,request_sha256=attempt['request_sha256'],decision_sha256=decision_sha)
    life=state['_lifecycle'];source=dict(kind='lifecycle_job',job_id=job['id'],generation=job['generation'],
        request_id=attempt['request_id'],committed_revision=revision)
    if pid is not None:
        if pid in life['person_origins'] or any(p['id']==pid for p in state['persons']):_lc_fail('新人物ID已占用')
        successor=response['successor'];office=request['office']
        identity=dict(id=pid,name=successor['name'],country_id=office['country_id'],office=office['office'],
            office_id=office['id'],location=successor['location'])
        state['persons'].append(dict(identity,card=_lc_copy(successor['card']),card_version=1,card_author='dm'))
        life['person_origins'][pid]=dict(kind='dm_introduction',introduced_revision=revision,identity=identity,source_ref=source)
        life['tenures'].append(dict(id='tenure-'+office['id']+':'+pid+':'+str(revision),office_id=office['id'],person_id=pid,
            from_revision=revision,to_revision=None,basis_refs=_lc_copy(response['basis_refs']),source_ref=source))
        state['_observations'][pid]=[];state['_messages'][pid]=[]
        state['_p04']['resident_last_dispatch'][pid]=-1
        state['_p04']['pending'].append(pid)
        life['transitions'].extend([
            dict(id=job['id']+':person_introduced',kind='person_introduced',person_id=pid,committed_revision=revision,
                job_id=job['id'],generation=job['generation'],request_id=attempt['request_id']),
            dict(id=job['id']+':office_assumed',kind='office_assumed',office_id=office['id'],person_id=pid,
                committed_revision=revision,source_ref=source)])
    vacancy['status']=response['status'];job['result']=result
    attempt.update(response_raw=response_raw,response=_lc_copy(response),decision_sha256=decision_sha,status='committed')
    return result



def _lc_death_records(state,node,revision):
    life=state['_lifecycle']
    injury=WorldStore._find(life['transitions'],node['injury_id'],'ACTOR_RECOVERY_UNPROVEN')
    if (injury['kind']!='mortal_injury' or node!=injury['node'] or node['at']!=state['time']
            or node['at']!=injury['occurred_at']+life['rules']['death_delay_seconds']
            or node['at']<=injury['occurred_at'] or node['lifecycle_version']!=_LC_VERSION
            or node['person_id']!=injury['person_id'] or node['cause_id']!=injury['cause_ref']['child_intent_id']
            or injury['committed_revision']>=revision or not _lc_alive(state,node['person_id'])):_lc_fail('非法到期死亡')
    pid=node['person_id'];death=dict(id='death-'+injury['id'],person_id=pid,injury_id=injury['id'],
        occurred_at=state['time'],committed_revision=revision,cause_ref=_lc_copy(injury['cause_ref']),
        node_id=node['id'],source_snapshot_revision=injury['committed_revision'])
    life['deaths'].append(death);life['transitions'].append(dict(death,kind='person_death'))
    person=WorldStore._find(state['persons'],pid,'ACTOR_RECOVERY_UNPROVEN');office=person.get('office_id')
    if office is not None:
        tenures=[t for t in life['tenures'] if t['person_id']==pid and t['to_revision'] is None]
        if len(tenures)!=1:_lc_fail('死亡无唯一原任期')
        tenures[0]['to_revision']=revision
        vacancy=dict(id='vacancy-'+office+':'+death['id'],office_id=office,former_person_id=pid,death_id=death['id'],
            opened_revision=revision,status='pending_dm',generation=1)
        job=dict(id='vacancy:'+office+':'+death['id'],kind='succession',vacancy_id=vacancy['id'],generation=1,attempts=[],result=None)
        life['vacancies'].append(vacancy);life['jobs'].append(job)
        life['transitions'].append(dict(id=death['id']+':office_vacated',kind='office_vacated',office_id=office,
            person_id=pid,committed_revision=revision,source_ref=dict(kind='due_event',node_id=node['id'],
                injury_id=injury['id'],committed_revision=revision)))
    cancelled=[]
    for q in list(state['queue']):
        personal=q['kind'] in ('arrival','investigation','p04_deferred_wake') and q.get('person_id')==pid
        project=(q['kind']=='project_stage' and any(t['id']==q.get('project_task_id') and t['owner_id']==pid
            and t['status']=='pending' and t['runs'][-1]['node']==q for t in state.get('projects',{}).get('tasks',[])))
        if personal or project:
            kind='wake_cancelled' if q['kind']=='p04_deferred_wake' else 'task_interrupted'
            life['transitions'].append(dict(id=death['id']+':'+kind+':'+q['id'],kind=kind,person_id=pid,
                committed_revision=revision,original_node=_lc_copy(q),death_id=death['id']))
            state['queue'].remove(q);cancelled.append(q)
    state['_p04']['pending']=[p for p in state['_p04']['pending'] if p!=pid]
    if pid in state['_p04']['remaining']:state['_p04']['remaining'][pid]=0
    return death,cancelled



def _lc_private_base_legacy(read,world_id,pid,base_version):
    """Primitive version spine. Never call the outer authorization/audit graph."""
    _lc_int(base_version)
    rows=_lc_read(read,'actor.actor_intents',(('person_id',pid),))
    metas=_lc_read(read,'actor.actor_intent_execution',(('person_id',pid),))
    if base_version>len(metas):_lc_fail('私库基准超出原spine')
    calls=_lc_read(read,'actor.actor_calls',(('person_id',pid),))
    by_id={r['intent_id']:r for r in rows}
    if len(by_id)!=len(rows):_lc_fail('重复私库业务ID')
    messages=[];seen=[]
    from .actor_execution import body_matches
    for version in range(base_version):
        candidates=[m for m in metas if type(m['promoted_version']) is int and m['promoted_version']==version+1]
        if len(candidates)!=1:_lc_fail('私库版本spine断链')
        meta=candidates[0];row=by_id.get(meta['intent_id'])
        if row is None or row['state']!='committed' or row['candidate'] is None or row['receipt'] is None:_lc_fail()
        request=_lc_json(row['request']);body=_lc_json(row['body']);candidate=_lc_json(row['candidate']);consumed=_lc_json(row['consumed'])
        actual=_lc_read(read,'main.intents',(('id',row['intent_id']),),first=True)
        mapped=[c for c in calls if c['intent_id']==row['intent_id']]
        if (meta['world_id']!=world_id or type(meta['base_version']) is not int or meta['base_version']!=version
                or meta['base_messages_sha256']!=_lc_hash(messages) or meta['base_seen_sha256']!=_lc_hash(seen)
                or request['person_id']!=pid or request['intent_id']!=row['intent_id'] or request['messages']!=messages
                or not body_matches(request,body,candidate) or request.get('schema_version')!=3
                or len(mapped)!=1 or mapped[0]['call_id']!='real-'+row['intent_id']
                or type(consumed) is not list or consumed[:len(seen)]!=seen or len(set(consumed))!=len(consumed)
                or any(type(v) is not str for v in consumed)
                or actual is None or actual['person']!=pid or _lc_json(actual['payload'])['body']!=body
                or actual['receipt'] is None or _lc_json(actual['receipt'])!=_lc_json(row['receipt'])):_lc_fail('原私密spine未接回空基准')
        receipt=_lc_json(row['receipt'])
        if receipt.get('world_id')!=world_id or receipt.get('person_id')!=pid or receipt.get('intent_id')!=row['intent_id'] or receipt.get('status')!='committed':_lc_fail()
        messages=candidate;seen=consumed
    return messages,seen


def _lc_child_legacy(read,world_id,cause,revision,states,*,committed=True):
    """Primitive join only: no window_record, audit, acquisition or recursion."""
    _lc_cause_ref(cause);parent=cause['parent_intent_id'];version=cause['decision_version'];index=cause['effect_index']
    child=_lc_read(read,'main.intents',(('id',cause['child_intent_id']),),first=True)
    source=_lc_read(read,'main.intents',(('id',parent),),first=True)
    decision=_lc_read(read,'main.dm_decisions',(('intent_id',parent),('version',version)),first=True)
    proposal=_lc_read(read,'main.dm_proposals',(('intent_id',parent),),first=True)
    if source is None or decision is None or proposal is None:_lc_fail('原提案/裁定缺失')
    decision=_lc_json(decision['data']);payload=_lc_json(source['payload']);body=payload['body'];pid=source['person']
    if (set(payload)!={'body'} or set(body)!={'action','text','physical_attempt'} or body['action']!='propose'
            or proposal['world_id']!=world_id or proposal['person_id']!=pid or proposal['text']!=body['text']
            or decision['world_id']!=world_id or index>=len(decision['effects'])):_lc_fail('原人物提案绑定不符')
    base=_lc_int(decision['base_revision'],1)
    base_state=_lc_historical(states,base)
    if base_state['world_id']!=world_id:_lc_fail('裁定基准不存在')
    del base_state
    actor=_lc_read(read,'actor.actor_intents',(('intent_id',parent),),first=True)
    meta=_lc_read(read,'actor.actor_intent_execution',(('intent_id',parent),),first=True)
    calls=_lc_read(read,'actor.actor_calls',(('intent_id',parent),))
    activations=_lc_read(read,'main.coordinator_activations',(('intent_id',parent),))
    if actor is None or meta is None or len(activations)!=1 or len(calls)!=1:_lc_fail('原私库候选链缺失')
    request=_lc_json(actor['request']);candidate=_lc_json(actor['candidate'])
    base_messages,base_seen=_lc_private_base(read,world_id,pid,meta['base_version'])
    if (request['messages']!=base_messages or meta['base_seen_sha256']!=_lc_hash(base_seen)
            or activations[0]['base_session_version']!=meta['base_version']
            or activations[0]['request_sha256']!=_lc_hash(request)
            or activations[0]['candidate_sha256']!=_lc_hash(candidate)):_lc_fail('原请求/候选hash或空起点不符')
    from .actor_execution import body_matches
    if (not body_matches(request,body,candidate) or request.get('schema_version')!=3
            or request['person_id']!=pid or request['intent_id']!=parent
            or actor['person_id']!=pid or _lc_json(actor['body'])!=body
            or meta['person_id']!=pid or meta['world_id']!=world_id or meta['intent_id']!=parent
            or type(meta['base_version']) is not int or meta['base_version']<0
            or meta['base_messages_sha256']!=_lc_hash(request['messages'])
            or calls[0]['person_id']!=pid or calls[0]['call_id']!='real-'+parent
            or activations[0]['person_id']!=pid or activations[0]['world_id']!=world_id
            or activations[0]['intent_id']!=parent):_lc_fail('原candidate/meta/call不符')
    binding=request['identity_binding'];observed=binding['observation_revision']
    if activations[0]['observation_revision']!=observed:_lc_fail('观测基准不符')
    historical=_lc_historical(states,observed);origin=historical['_lifecycle']['person_origins'].get(pid)
    tenure=_lc_tenure(historical,pid)
    expected_binding=dict(world_id=world_id,person_id=pid,observation_revision=observed,origin_ref=pid,
        tenure_ref=tenure['id'] if tenure is not None else None)
    if origin is None or binding!=expected_binding or not _lc_active(historical,pid):_lc_fail('原身份未授权')
    for kind,v in [('proposal',0),('decision',version)]:
        timeline=_lc_read(read,'main.dm_timeline',(('intent_id',parent),('kind',kind),('version',v)),first=True)
        if timeline is None or type(timeline['visible_from']) is not int or timeline['visible_from']>revision+(0 if committed else 1):_lc_fail('原可见性未成立')
    effect=decision['effects'][index]
    if committed:
        timeline=_lc_read(read,'main.dm_timeline',(('intent_id',parent),('kind','commit'),('version',version)),first=True)
        if child is None or child['receipt'] is None or source['receipt'] is None or timeline is None or timeline['visible_from']!=revision:_lc_fail('缺真实父子提交')
        cp=_lc_json(child['payload']);cr=_lc_json(child['receipt']);pr=_lc_json(source['receipt'])
        if (cp!={'parent':parent,'version':version,'body':effect} or child['person']!=pid
                or any(r.get('world_id')!=world_id or r.get('person_id')!=pid or r.get('status')!='committed'
                    or r.get('decision_version')!=version or r.get('ruling')!=decision['ruling'] for r in (cr,pr))
                or cr['intent_id']!=cause['child_intent_id'] or pr['intent_id']!=parent
                or cr['time']!=pr['time'] or pr['effect']['effects'][index]!=cr['effect']):_lc_fail('父子receipt不符')
        return pid,body,effect,decision,cr
    return pid,body,effect,decision,None


# Structural history only. This token never carries permission or Entry success.
_lc_history_context = ContextVar('world_lifecycle_history', default=None)


def _lc_native_snapshot_json(read, revision, missing='历史快照断链'):
    """Raw loan ends after decode; decoded loan belongs to explicit caller region."""
    memory = _lc_value_memory(read)
    if memory is None:
        row = read('main.snapshots', (('seq', revision),), ('state',), first=True)
        if row is None:_lc_fail(missing)
        return _lc_json(row[0])
    loan = read.borrow('main.snapshots', (('seq', revision),), ('state',), first=True)
    row = loan.value
    try:
        if row is None:_lc_fail(missing)
        value = _lc_json(row[0])
    except BaseException:
        row = None
        loan.fail()
        raise
    row = None
    loan.close()
    return value


def _lc_historical(states, revision):
    """Decode one original historical Row, with no all-snapshots backing.

    states is the private (read, actual_genesis, inclusive_ceiling) tuple.
    A producer's base/observation must already precede the step being proved.
    """
    read, genesis, ceiling = states
    _lc_int(revision, 1)
    if revision > ceiling:_lc_fail('历史依据来自未来')
    if revision == 1:
        state = _lc_copy(genesis)
    else:
        state = _lc_native_snapshot_json(read, revision)
    if state['revision'] != revision or state['world_id'] != genesis['world_id']:_lc_fail('历史依据身份不符')
    _lifecycle_shape(state, genesis)
    return state


def _lc_history_scope(read, genesis):
    # Keep registered globals as genuine World source functions. The stdlib
    # contextmanager wrapper is lexical, never registered as a World function.
    return contextmanager(_lc_history_owner)(read, genesis)


def _lc_history_owner(read, genesis):
    """Bounded structural backing; raw/native budget shared by nested readers.

    This owner accounts its own fixed backing and carrier. It does not assert
    ownership of an external decoded genesis, consumer state or proof result.
    """
    from .coordinator_scheduler import (_LifecycleNativeReader,
        _lifecycle_live_context)
    outer = _lc_history_context.get()
    if outer is not None and outer['read'] is read and outer['genesis'] is genesis:
        yield outer
        return
    memory = _lc_value_memory(read)
    if memory is None:memory = _lifecycle_live_context.get()
    lease = memory.hold(1048576 + 4096, 'history-fixed-backing') if memory is not None else None
    facts = owner = token = memory_token = None
    try:
        # Never grow/overallocate the backing a digest at a time. Payload usage
        # is last*32; the full allocation, not that usage, is prepaid above.
        facts = bytearray(1048576)
        owner = dict(read=read, genesis=genesis, genesis_sha=_lc_hash(genesis),
                     iterator=None, facts=facts, last=0)
        token = _lc_history_context.set(owner)
        if memory is not None:
            memory_token = _lifecycle_live_context.set(memory)
        yield owner
    finally:
        try:
            if owner is not None and owner['iterator'] is not None:
                owner['iterator'].close()
        finally:
            if owner is not None:
                owner['facts'] = None
                owner.clear()
            facts = None
            if token is not None:_lc_history_context.reset(token)
            if memory_token is not None:_lifecycle_live_context.reset(memory_token)
            if lease is not None:lease.release()


def _lc_project_steps(read, genesis):
    """Keep exactly one generation owner across yield; no history of decoded states."""
    from .coordinator_scheduler import _LifecycleValueOwner, _lifecycle_value_context
    memory = _lc_value_memory(read)
    held = _LifecycleValueOwner(memory) if memory is not None else None
    token = _lifecycle_value_context.set(held) if held is not None else None
    previous = expected = old = None
    try:
        _lc_reserve(genesis['_lifecycle'], 2, 'causal-seed-direct-containers')
        expected = _lc_seed(genesis)
        if genesis['_lifecycle'] != expected:_lc_fail('首生命周期镜像非法')
        previous = genesis
        if token is not None:
            _lifecycle_value_context.reset(token)
            token = None
        yield 1, _lc_step_digest(expected, held)
        revision = 2
        while True:
            old = held
            previous, expected, held = _lc_project_step(read, genesis, revision, previous, expected)
            # _lc_project_step_owned has returned: all its nonescaping local
            # aliases have gone; previous/expected now refer to the NEW region.
            if old is not None:
                old.close()
            old = None
            yield revision, _lc_step_digest(expected, held)
            revision += 1
    except GeneratorExit:
        # Deliberate iterator.close, not an inference from GC or scope exit.
        previous = expected = None
        if held is not None:held.close()
        held = None
        raise
    except BaseException:
        if held is not None:held.fail()
        if old is not None:old.fail()
        raise
    finally:
        if token is not None:_lifecycle_value_context.reset(token)
        previous = expected = None
        if held is not None and not held.failed:held.close()


def _lifecycle_project_owned(read, genesis, through_revision):
    """Original typed proof API, compact structural facts within one scope.

    No stored authority verdict or cross-Entry cache. Explicit native snapshot
    loans and fixed compact backing are charged, but decoded/canonical/results
    and legacy reads are NOT unified. The independent old IO gate remains.
    This source is PARTIAL_NOT_ADMISSIBLE, not the complete live-memory repair.
    """
    try:
        _lc_int(through_revision, 1)
        if not _lc_optin(genesis):
            _lifecycle_shape(genesis, genesis)
            return None
        with _lc_history_scope(read, genesis) as history:
            if history['genesis_sha'] != _lc_hash(genesis):_lc_fail('首快照材料变更')
            if history['iterator'] is None:
                history['iterator'] = _lc_project_steps(read, genesis)
            while history['last'] < through_revision:
                # Fixed backing was prepaid, not bytearray growth guessed.
                if (history['last'] + 1) * 32 > 1048576:_lc_fail('结构历史事实超界')
                revision, digest = next(history['iterator'])
                if revision != history['last'] + 1 or len(digest) != 32:_lc_fail()
                offset = history['last'] * 32
                history['facts'][offset:offset+32] = digest
                history['last'] = revision
            state = _lc_historical((read, genesis, history['last']), through_revision)
            life = state['_lifecycle']
            offset = (through_revision - 1) * 32
            if bytes.fromhex(_lc_hash(life)) != history['facts'][offset:offset+32]:
                _lc_fail('生命周期不是原历史producer推导')
            _lc_reserve(life,1,'project-result-direct-containers')
            return dict(world_id=genesis['world_id'], revision=through_revision,
                lifecycle=_lc_copy(life), identities=_lc_copy(life['person_origins']),
                alive=[pid for pid in life['person_origins'] if _lc_alive(state, pid)],
                active=[pid for pid in life['person_origins'] if _lc_active(state, pid)])
    except (KeyError, TypeError, ValueError, IndexError, OverflowError, RecursionError):
        _lc_fail()


def _lc_root_row(db, sql, params=(), *, columns):
    """Exact source-owned root query; prepay native backing before its fetch.

    The extra size query is a new acquisition gate, not a substitute for any
    original _AttemptConnection pre/post read gate. It runs in the same pinned
    transaction. Column/SQL strings below are trusted literal caller arguments.
    """
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_raw_charge
    owner = _lifecycle_value_context.get()
    if owner is None:
        cursor = db.execute(sql, params)
        try:
            return cursor.fetchone()
        finally:
            cursor.close()
    owner.reserve(4096, 'root-query-metadata')
    expressions = '+'.join("CASE typeof("+key+") WHEN 'text' THEN length(CAST("+key+
        " AS BLOB)) WHEN 'blob' THEN length("+key+") ELSE 8 END" for key in columns)
    cursor = None
    try:
        cursor = db.execute('SELECT '+expressions+' FROM ('+sql+')', params)
        width = cursor.fetchone()
        cursor.close(); cursor = None
        size = 0 if width is None else width[0]
        owner.reserve(_lifecycle_raw_charge(size, len(columns)), 'root-native-row')
        cursor = db.execute(sql, params)
        return cursor.fetchone()
    finally:
        if cursor is not None:
            cursor.close()


def _lc_root_json(db, sql, params=(), *, column='state'):
    from .coordinator_scheduler import _lifecycle_value_context
    if _lifecycle_value_context.get() is None:
        return json.loads(db.execute(sql, params).fetchone()[0])
    row = _lc_root_row(db, sql, params, columns=(column,))
    return _lc_json(row[0])


def _lifecycle_project(read, genesis, through_revision):
    from .coordinator_scheduler import _lifecycle_value_invoke
    memory = _lc_value_memory(read)
    if memory is None:
        return _lifecycle_project_owned(read, genesis, through_revision)
    return _lifecycle_value_invoke(_lc_project_to_owner, (read, genesis, through_revision),
        memory=memory, inputs=(genesis,), detached=True)


def _lc_project_to_owner(read, genesis, through_revision):
    from .coordinator_scheduler import _lifecycle_value_context
    loan = _lc_project_borrow(read, genesis, through_revision)
    return None if loan is None else _lifecycle_value_context.get().take(loan)


def _lc_project_borrow(read, genesis, through_revision):
    from .coordinator_scheduler import (_LifecycleValueOwner, _lifecycle_value_context,
                                        _lifecycle_copy_borrow)
    memory = _lc_value_memory(read)
    if memory is None:_lc_fail('借用project须受证真实reader')
    owner = _LifecycleValueOwner(memory)
    token = _lifecycle_value_context.set(owner)
    result = None
    try:
        result = _lifecycle_project_owned(read, genesis, through_revision)
        loan = None if result is None else _lifecycle_copy_borrow(
            result, memory=memory, role='causal-proof-result')
    except BaseException:
        owner.fail()
        raise
    finally:
        _lifecycle_value_context.reset(token)
    result = None
    owner.close()
    return loan


def _lc_project_step_owned(read,genesis,revision,previous,expected):
    states=(read,genesis,revision-1)
    state=_lc_native_snapshot_json(read,revision);_lc_reserve(state,3,'causal-current-direct-containers');_lifecycle_shape(state,genesis)
    if state['revision']!=revision or state['world_id']!=genesis['world_id']:_lc_fail()
    work=_lc_copy(previous);work['revision']=revision;work['time']=state['time'];work['_lifecycle']=_lc_copy(expected)
    current=state['_lifecycle'];old_transition_count=len(expected['transitions'])
    if current['transitions'][:old_transition_count]!=expected['transitions']:_lc_fail('transition前缀改写')
    new=current['transitions'][old_transition_count:]
    # Consume only actual producer heads. Derived transitions are rebuilt.
    for transition in new:
        kind=transition['kind']
        if kind=='mortal_injury':
            cause=transition['cause_ref']
            pid,proposal,body,decision,receipt=_lc_child(read,state['world_id'],cause,revision,states)
            if body['action']!='resolve_physical_attempt' or body['outcome']!='mortal':_lc_fail()
            base=_lc_historical(states,decision['base_revision']);base['_lifecycle']=_lc_copy(expected)
            target,means=_lc_effect_conditions(base,WorldStore._find(base['persons'],pid,'ACTOR_RECOVERY_UNPROVEN'),
                proposal,body,decision['ruling'],decision['base_revision'])
            node=transition['node'];_lc_exact(node,'id seq at kind person_id injury_id cause_id lifecycle_version')
            wanted=dict(id='injury-'+cause['child_intent_id'],kind='mortal_injury',person_id=target['id'],actor_person_id=pid,
                cause_ref=cause,occurred_at=receipt['time'],committed_revision=revision,basis_refs=body['basis_refs'],means_id=means['id'],node=node)
            if (transition!=wanted or receipt['time']!=previous['time'] or type(node['seq']) is not int
                    or not previous['_queue_seq']<node['seq']<=state['_queue_seq'] or node['id']!='q-'+str(node['seq'])
                    or node['kind']!='person_death_due' or node['person_id']!=target['id'] or node['injury_id']!=wanted['id']
                    or node['cause_id']!=cause['child_intent_id'] or node['lifecycle_version']!=_LC_VERSION
                    or node['at']!=receipt['time']+expected['rules']['death_delay_seconds']
                    or [q for q in state['queue'] if q['id']==node['id']]!=[node]
                    or receipt['effect']!=dict(implemented=True,outcome='mortal',injury_id=wanted['id'],due=node['at'])):_lc_fail('伤害原producer不符')
            expected['transitions'].append(_lc_copy(wanted));work['_lifecycle']=expected
        elif kind=='person_death':
            injury=WorldStore._find(expected['transitions'],transition['injury_id'],'ACTOR_RECOVERY_UNPROVEN');node=injury['node']
            if node not in previous['queue'] or node['at']!=state['time']:_lc_fail('无原真实到期node')
            if _lc_value_memory(read) is None:
                windows=_lc_read(read,'main.coordinator_windows',(('advanced_revision',revision),))
                if len(windows)!=1 or windows[0]['base_revision']!=revision-1 or windows[0]['target_time']!=state['time']:_lc_fail('死亡非原开窗advance')
                earlier=_lc_read(read,'main.coordinator_windows',(('world_id',state['world_id']),),order=(('ordinal',False),))
                for window in earlier:
                    if window['ordinal']>=windows[0]['ordinal']:break
                    if window['state']!='completed' or window['completed_revision']>revision-1:_lc_fail('旧窗未闭合')
                    for activation in _lc_read(read,'main.coordinator_activations',(('window_id',window['window_id']),)):
                        if activation['private_closed']!=1 or activation['phase'] not in ('completed','rejected'):_lc_fail('旧私密未闭合')
            else:
                _lc_death_window(read,state,revision)
            work['_lifecycle']=expected
            work['queue']=[q for q in previous['queue'] if (q['at'],q['seq'])>(node['at'],node['seq'])]
            death,cancelled=_lc_death_records(work,node,revision)
            if dict(death,kind='person_death')!=transition:_lc_fail('死亡记录不符')
            if any(q['id']==node['id'] or q['id'] in {c['id'] for c in cancelled} for q in state['queue']):_lc_fail('死亡/中断node未消费')
            for q in cancelled:
                if q['kind']=='project_stage':
                    task=WorldStore._find(state['projects']['tasks'],q['project_task_id'],'ACTOR_RECOVERY_UNPROVEN');run=task['runs'][-1]
                    if run['status']!='interrupted' or run['node']!=q or run['ended_at']!=death['occurred_at']:_lc_fail('项目未真实中断')
            expected=work['_lifecycle']
    # Documents/deliveries are reconstructed from their original child receipt.
    for collection in ('documents','deliveries'):
        old=expected[collection]
        if current[collection][:len(old)]!=old:_lc_fail('文书/delivery前缀改写')
        for record in current[collection][len(old):]:
            pid,proposal,body,decision,receipt=_lc_child(read,state['world_id'],record['cause_ref'],revision,states)
            base=_lc_historical(states,decision['base_revision']);base['_lifecycle']=expected
            person=WorldStore._find(base['persons'],pid,'ACTOR_RECOVERY_UNPROVEN')
            condition=_lc_effect_conditions(base,person,proposal,body,decision['ruling'],decision['base_revision'])
            child=record['cause_ref']['child_intent_id']
            if decision['ruling'] not in ('成功','大成功'):_lc_fail('失败档写/读文书')
            if collection=='documents':
                if body['action']!='write_office_document':_lc_fail()
                wanted=dict(id='office-document-'+child,author_person_id=pid,office_id=person['office_id'],place_id=condition['id'],
                    text=body['text'],created_revision=revision,cause_ref=record['cause_ref'],access='current_office_at_place')
                effect=dict(implemented=True,document_id=wanted['id'])
            else:
                if body['action']!='read_office_document':_lc_fail()
                wanted=dict(id='office-delivery-'+child,document_id=condition['id'],recipient_person_id=pid,delivered_revision=revision,
                    time=receipt['time'],channel='onsite_document_read',cause_ref=record['cause_ref'],
                    text_sha256=_lc_text_hash(condition['text']))
                effect=dict(implemented=True,document_id=condition['id'],delivery_id=wanted['id'])
                notices=state['_observations'][pid][len(previous['_observations'][pid]):]
                if not any(n['text']==condition['text'] and n['source']=='机构公文：'+condition['id'] for n in notices):_lc_fail('未实际特定投递')
            if record!=wanted or receipt['effect']!=effect:_lc_fail('文书原producer不符')
            old.append(_lc_copy(wanted))
    work['_lifecycle']=expected
    # A request is durable at its prepared_revision; only UUID values are
    # newly chosen. Everything else is reconstructed from preceding facts.
    if len(current['jobs'])!=len(expected['jobs']):_lc_fail('伪造job')
    for job,actual in zip(expected['jobs'],current['jobs']):
        if any(job[k]!=actual[k] for k in ('id','kind','vacancy_id','generation')):_lc_fail()
        attempts=job['attempts'];incoming=actual['attempts']
        if len(incoming)<len(attempts) or len(incoming)>len(attempts)+1:_lc_fail('attempt非追加')
        for old,new_attempt in zip(attempts,incoming):
            if old['status']!='pending':
                if new_attempt!=old:_lc_fail('终结attempt改写')
            elif new_attempt!=old:
                if any(new_attempt[k]!=old[k] for k in ('request_id','prepared_revision','request','request_sha256')):_lc_fail('原request改写')
                if new_attempt['status']=='committed':
                    if previous['revision']!=old['request']['base_revision'] or actual['result'] is None:_lc_fail('继任失基准')
                    _lc_finish_job(work,job,old,new_attempt['response_raw'],new_attempt['response'],revision)
                elif new_attempt['status'] in ('stale','failed'):
                    if new_attempt['status']=='stale' and previous['revision']==old['request']['base_revision']:_lc_fail('无失基准')
                    old.update(status=new_attempt['status'],failure_code=new_attempt['failure_code'])
                else:_lc_fail('未知attempt转移')
        if len(incoming)>len(attempts):
            new_attempt=incoming[-1];request=new_attempt['request']
            if (job['result'] is not None or new_attempt['status']!='pending' or new_attempt['prepared_revision']!=revision
                    or (attempts and attempts[-1]['status']=='pending')):_lc_fail('非法prepare')
            rid=request['request_id'];newpid=request['proposed_person_id']
            if type(rid) is not str or len(rid)!=32 or any(c not in '0123456789abcdef' for c in rid):_lc_fail('request UUID非法')
            if (type(newpid) is not str or not newpid.startswith('person-') or len(newpid)!=39
                    or any(c not in '0123456789abcdef' for c in newpid[7:]) or newpid in expected['person_origins']):_lc_fail('新人物UUID非法')
            if attempts and newpid!=attempts[0]['request']['proposed_person_id']:_lc_fail('重裁换人ID')
            if any(rid==a['request_id'] for j in expected['jobs'] for a in j['attempts']):_lc_fail('复用request ID')
            service=request['service_version']
            if type(service) is not str or len(service)!=64 or any(c not in '0123456789abcdef' for c in service):_lc_fail('服务版本非法')
            wanted=_lc_request(previous,job,rid,newpid,revision,service)
            if _lc_canonical(request)!=_lc_canonical(wanted):_lc_fail('完整请求不符历史')
            if any(j['id']!=job['id'] and a['request']['proposed_person_id']==newpid for j in expected['jobs'] for a in j['attempts']):_lc_fail('跨job复用新人物ID')
            attempts.append(_lc_copy(new_attempt))
        if job!=actual:_lc_fail('job/result非真实producer')
    if _lc_canonical(expected)!=_lc_canonical(current):_lc_fail('生命周期不是原历史producer推导')
    for injury in expected['transitions']:
        if injury['kind']!='mortal_injury':continue
        terminal=any(d['injury_id']==injury['id'] for d in expected['deaths'])
        nodes=[q for q in state['queue'] if q['id']==injury['node']['id']]
        if (not terminal and (nodes!=[injury['node']] or injury['node']['at']<=state['time'])) or (terminal and nodes):_lc_fail('原致命node义务丢失/复活')
    for origin in expected['person_origins'].values():
        if origin['introduced_revision']>revision:_lc_fail('身份来自未来')
    # Every new person starts with empty world maps/ledger, never old session.
    for pid,origin in expected['person_origins'].items():
        if origin['introduced_revision']==revision:
            if (state['_observations'][pid]!=[] or state['_messages'][pid]!=[]
                    or state['_p04']['resident_last_dispatch'].get(pid)!=-1 or pid not in state['_p04']['pending']
                    or pid in state['_p04']['dispatch']):_lc_fail('新人非空或回灌旧roster')
    for death in expected['deaths']:
        if death['committed_revision']<revision:
            pid=death['person_id']
            if (state['_observations'][pid]!=previous['_observations'][pid]
                    or state['_messages'][pid]!=previous['_messages'][pid]):_lc_fail('死后投递新知识')
    return state,expected


def _lc_project_step(read, genesis, revision, previous, expected):
    from .coordinator_scheduler import _LifecycleValueOwner, _lifecycle_value_context
    memory = _lc_value_memory(read)
    if memory is None:
        state, expected = _lc_project_step_owned(read, genesis, revision, previous, expected)
        return state, expected, None
    owner = _LifecycleValueOwner(memory)
    token = _lifecycle_value_context.set(owner)
    try:
        # The original engine mutates expected. Detach it before the preceding
        # iterator region can be released, rather than pretending it is dead.
        expected = _lc_copy(expected)
        _lc_reserve(previous, 3, 'causal-step-direct-containers')
        state, expected = _lc_project_step_owned(read, genesis, revision, previous, expected)
        return state, expected, owner
    except BaseException:
        owner.fail()
        raise
    finally:
        _lifecycle_value_context.reset(token)



class WorldStore:
    @contextmanager
    def _entry_budget(self, budget):
        token = _actor_entry_scope.set((self, budget))
        try:
            budget.check()
            from .coordinator_scheduler import _lifecycle_operation_scope
            with _lifecycle_operation_scope(entry=budget):
                yield
        finally:
            _actor_entry_scope.reset(token)

    def _scoped_entry_budget(self):
        scope = _actor_entry_scope.get()
        return scope[1] if scope is not None and scope[0] is self else None

    @contextmanager
    def _coordinator_scope(self, permit, attempt=None):
        self._validate_coordinator_execution(permit)
        outer = _coordinator_attempt_scope.get()
        if outer is not None and outer[0] is self:
            self._scoped_coordinator(permit)
            if outer[2] is not None:
                attempt = outer[2]
        token = _coordinator_attempt_scope.set((self, permit, attempt))
        try:
            if attempt is not None:
                attempt.check()
            yield
        finally:
            _coordinator_attempt_scope.reset(token)

    def _scoped_coordinator(self, coordinator=None):
        scope = _coordinator_attempt_scope.get()
        if scope is not None and scope[0] is self:
            if scope[2] is not None:
                scope[2].check()
            if coordinator is not None and coordinator != scope[1]:
                raise WorldError('COORDINATOR_EXECUTION_FENCED', '协调尝试不可替换原许可')
            return scope[1]
        return coordinator

    def _scoped_coordinator_attempt(self):
        scope = _coordinator_attempt_scope.get()
        return scope[2] if scope is not None and scope[0] is self else None

    @contextmanager
    def _actor_attempt(self, attempt):
        attempt.check()
        token = _actor_attempt_scope.set((self, attempt))
        parent_token = None
        if attempt.coordinator is not None:
            parent_token = _coordinator_attempt_scope.set((self, attempt.coordinator, attempt.parent_attempt))
        try:
            from .coordinator_scheduler import activation_scope
            with activation_scope(self, attempt.activation):
                yield
        finally:
            if parent_token is not None:
                _coordinator_attempt_scope.reset(parent_token)
            _actor_attempt_scope.reset(token)

    def _scoped_attempt(self):
        scope = _actor_attempt_scope.get()
        return scope[1] if scope is not None and scope[0] is self else None

    @contextmanager
    def _connection(self, *, readonly=False, _cold_entry=None):
        if not readonly:
            from .coordinator_scheduler import _lc_ro1127_write_guard
            _lc_ro1127_write_guard(self)
        attempt = self._scoped_attempt()
        if attempt is None:
            scope = _coordinator_attempt_scope.get()
            if scope is not None and scope[0] is self:
                attempt = scope[2]
        attempt = self._scoped_entry_budget() or attempt
        if attempt is not None:
            attempt.check()
        if _cold_entry is not None and (not readonly or attempt is not _cold_entry):
            raise RuntimeError('cold reader requires the original scoped Entry')
        target = self.db_path.resolve().as_uri()+'?mode=ro' if readonly else str(self.db_path)
        options = dict(uri=True)  # Also honor read-only ATTACH URIs on write connections.
        if attempt is not None:
            options['factory'] = _AttemptConnection
        from .actor_execution import PreparationBudget
        timeout = 0 if type(attempt) is PreparationBudget else (30 if attempt is None else .05)
        closer = closing(sqlite3.connect(target, timeout=timeout, **options))
        db = closer.__enter__()
        if _cold_entry is not None:
            with _lc_cold1136_connection(closer, db, _cold_entry, readonly=True):
                yield db
            return
        latched = lane = None
        try:
            if attempt is not None:
                db._attempt = attempt
            try:
                yield db
                if attempt is not None and readonly:
                    attempt.check()
            except BaseException as error:
                from .coordinator_scheduler import _lifecycle_value_context
                owner = _lifecycle_value_context.get()
                pending = owner.rollback_cleanup if owner is not None else None
                first = owner.first_failure if pending is not None else None
                if pending is None or pending[0] is not db or first[0] is not error:
                    db.rollback()
                    raise
                latched, lane = first, pending
                try:
                    db.rollback()
                except BaseException as secondary:
                    lane[1] = latched if secondary is latched[0] else _lc_geo1137_tuple(secondary)
                raise
        finally:
            # The original closing.__exit__ ignores its three arguments and
            # calls this very connection's close. Only the armed A lane can
            # contain a close exception; all old exits still propagate it.
            if lane is None:
                closer.__exit__(None, None, None)
            else:
                try:
                    closer.__exit__(None, None, None)
                except BaseException as secondary:
                    rollback = lane[1]
                    lane[2] = (latched if secondary is latched[0] else rollback
                        if rollback is not None and secondary is rollback[0]
                        else _lc_geo1137_tuple(secondary))
                finally:
                    lane[0] = None  # Disarm routing; no accounting or Work refund.

    @contextmanager
    def _history_segment(self, actor_path, budget):
        """Fresh original RO connection: sole progress ownership, same-thread close."""
        from .actor_execution import PrivateStore
        from .coordinator_scheduler import _history_original_method, unproven
        import _sqlite3
        import types
        bound=self._connection
        method=getattr(WorldStore._connection,'__wrapped__',None)
        stamp=method.__globals__.get('_SOURCE_GENERATION') if isinstance(method,types.FunctionType) else None
        if (type(self) is not WorldStore or getattr(bound,'__self__',None) is not self
                or getattr(bound,'__func__',None) is not WorldStore._connection
                or stamp is None or not stamp['matched'] or not _history_original_method(bound,stamp)
                or sqlite3.connect is not _sqlite3.connect):
            unproven()
        budget.check()
        with self._connection(readonly=True) as db:
            if type(db) is not _AttemptConnection or getattr(db,'_attempt',None) is not budget:unproven()
            db.row_factory=sqlite3.Row
            failure=[]
            def progress():
                try:budget.check()
                except BaseException as error:
                    if not failure:failure.append(error)
                    return 1
                return 0
            # This exact fresh lifecycle has no existing progress/trace callback.
            db.set_progress_handler(progress,1000)
            old_length=db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
            try:
                db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,min(old_length,1048576))
                if db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)!=min(old_length,1048576):unproven()
                db.execute('ATTACH DATABASE ? AS actor',(actor_path.as_uri()+'?mode=ro',)).close()
                db.execute('BEGIN').close()
                yield db
                budget.check()
            except sqlite3.Error as error:
                if failure:raise failure[0]
                root=self._scoped_coordinator_attempt()
                if root is not None:root.check()
                budget.check()
                if getattr(error,'sqlite_errorcode',None)==sqlite3.SQLITE_TOOBIG:unproven()
                raise
            finally:
                db.set_progress_handler(None,0)
                try:db.rollback()
                finally:db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH,old_length)

    def __init__(self, db_path, scenario_path=None, *, execution_clock_ns=None):
        import threading
        import weakref
        self._private_sources_lock = threading.RLock()
        self._private_sources = weakref.WeakSet()
        self._execution_clock_ns = time.time_ns if execution_clock_ns is None else execution_clock_ns
        self.db_path = Path(db_path)
        self._explicit_scenario = scenario_path is not None
        self.scenario_path = Path(scenario_path) if scenario_path is not None else DATA / 'scenarios' / 'iron-4.json'

    @staticmethod
    def _execution_ttl(ttl_seconds):
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 3600:
            raise WorldError('INVALID_EXECUTION_TTL', '执行许可期限必须为1至3600整数秒')
        return ttl_seconds * 1000000000

    def _execution_now(self, db, duration=0, *, _first_root_pay=None, _first_root_gate=None):
        try:
            now = self._execution_clock_ns()
        except Exception:
            raise WorldError('EXECUTION_CLOCK_UNSAFE', '执行时钟不可信') from None
        if type(now) is not int or not 0 < now <= 2**63-1-duration:
            raise WorldError('EXECUTION_CLOCK_UNSAFE', '执行时钟越界')
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_clock'").fetchone()
        floor_sql = 'SELECT high_water_wall_ns FROM execution_clock WHERE singleton=1'
        if exists and _first_root_gate is not None:
            pay = _first_root_gate('floor quote', '_lc_adm1148_root_pay', _first_root_pay)
            pay(db, floor_sql, (), ('high_water_wall_ns',), gate=_first_root_gate)
        floor = db.execute(floor_sql).fetchone() if exists else None
        if floor and now < floor[0]:
            raise WorldError('EXECUTION_CLOCK_UNSAFE', '执行时钟倒退')
        return now

    @staticmethod
    def _execution_floor(db, now):
        db.execute('CREATE TABLE IF NOT EXISTS execution_clock (singleton INTEGER PRIMARY KEY CHECK(singleton=1), high_water_wall_ns INTEGER NOT NULL)')
        db.execute('INSERT OR REPLACE INTO execution_clock VALUES (1,?)', (now,))

    @contextmanager
    def _execution_connection(self):
        if not self.db_path.exists():
            raise WorldError('NOT_INITIALIZED', '世界尚未初始化')
        try:
            entry = self._scoped_entry_budget()
            options = dict(uri=True)
            if entry is not None:
                entry.check()
                options['factory'] = _AttemptConnection
            with closing(sqlite3.connect(str(self.db_path), timeout=0.25 if entry is None else .001, **options)) as db:
                if entry is not None:
                    db._attempt = entry
                yield db
        except sqlite3.OperationalError as error:
            if getattr(error, 'sqlite_errorcode', 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                raise WorldError('EXECUTION_STORAGE_BUSY', '执行许可存储繁忙') from None
            raise

    def acquire_coordinator_execution(self, owner_instance, *, ttl_seconds=60):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self)
        from .coordinator_execution import CoordinatorPermit
        duration = self._execution_ttl(ttl_seconds)
        if type(owner_instance) is not str or not owner_instance.strip():
            raise WorldError('INVALID_COORDINATOR_EXECUTION', '协调宿主必须为非空字符串')
        with self._execution_connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self._execution_identity(db, state['world_id'])
                db.execute('CREATE TABLE IF NOT EXISTS coordinator_execution_leases (world_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, owner_token TEXT, owner_instance TEXT NOT NULL, acquired_wall_ns INTEGER NOT NULL, expires_wall_ns INTEGER NOT NULL, last_wall_ns INTEGER NOT NULL, schema_version INTEGER NOT NULL)')
                now = self._execution_now(db, duration)
                row = db.execute('SELECT epoch,owner_token,expires_wall_ns FROM coordinator_execution_leases WHERE world_id=?', (state['world_id'],)).fetchone()
                if row and row[1] is not None and now < row[2]:
                    raise WorldError('COORDINATOR_EXECUTION_BUSY', '世界已有协调宿主')
                epoch = row[0]+1 if row else 1
                if not 1 <= epoch <= 2**63-1:
                    raise WorldError('COORDINATOR_EXECUTION_FENCED', '协调代际已耗尽')
                token = uuid.uuid4().hex
                db.execute('INSERT INTO coordinator_execution_leases VALUES (?,?,?,?,?,?,?,1) ON CONFLICT(world_id) DO UPDATE SET epoch=excluded.epoch,owner_token=excluded.owner_token,owner_instance=excluded.owner_instance,acquired_wall_ns=excluded.acquired_wall_ns,expires_wall_ns=excluded.expires_wall_ns,last_wall_ns=excluded.last_wall_ns', (state['world_id'], epoch, token, owner_instance, now, now+duration, now))
                self._execution_floor(db, now)
                db.commit()
                return CoordinatorPermit(state['world_id'], token, epoch)
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _validate_coordinator_execution(permit):
        from .coordinator_execution import CoordinatorPermit
        if (type(permit) is not CoordinatorPermit
                or any(type(v) is not str or not v.strip() for v in (permit.world_id, permit.owner_token))
                or type(permit.epoch) is not int or not 1 <= permit.epoch <= 2**63-1):
            raise WorldError('INVALID_COORDINATOR_EXECUTION', '协调许可格式不合法')

    def _check_coordinator_execution(self, db, state_world_id, coordinator, *, readonly=False, _first_readonly=False):
        coordinator = self._scoped_coordinator(coordinator)
        pay = gate = None
        if readonly and _first_readonly:
            from . import coordinator_scheduler as root_source
            from .source_attestation import _REGISTRY
            from types import CodeType, FunctionType
            space = vars(root_source)
            stamp = _REGISTRY.get(id(space))
            gate = space.get('_lc_adm1154_root_gate')
            if (stamp is None or not stamp['matched'] or stamp['namespace'] is not space
                    or type(gate) is not FunctionType or gate.__globals__ is not space
                    or gate.__closure__ is not None or not any(gate is old for old in stamp['objects'])
                    or not any(type(code) is CodeType and code.co_name == '_lc_adm1154_root_gate'
                               and gate.__code__ is code for code in stamp['code'].co_consts)):
                raise WorldError('WINDOW_RECOVERY_UNPROVEN', 'first readonly Root: binding gate')
            first = gate('owner admission', '_lc_adm1136_first_root', space.get('_lc_adm1136_first_root'))
            pay = first(db, gate=gate)
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_execution_leases'").fetchone()
        # Preserve trusted pre-registration semantics, including legacy fixture
        # mutations; any root registration permanently enables identity checks.
        if coordinator is None and not exists:
            return
        if gate is None:
            world_id = self._execution_identity(db, state_world_id)
        else:
            identity = gate('_execution_identity', '_execution_identity', world=self)
            world_id = identity(db, state_world_id, _first_root_pay=pay, _first_root_gate=gate)
        if coordinator is not None:
            self._validate_coordinator_execution(coordinator)
            if coordinator.world_id != world_id:
                raise WorldError('EXECUTION_WORLD_MISMATCH', '协调许可不属于此世界')
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_execution_leases'").fetchone()
        lease_sql = 'SELECT epoch,owner_token,expires_wall_ns FROM coordinator_execution_leases WHERE world_id=?'
        if exists and gate is not None:
            quote = gate('lease quote', '_lc_adm1148_root_pay', pay)
            quote(db, lease_sql, (world_id,), ('epoch', 'owner_token', 'expires_wall_ns'), gate=gate)
        row = db.execute(lease_sql, (world_id,)).fetchone() if exists else None
        if coordinator is None:
            if row:
                raise WorldError('COORDINATOR_EXECUTION_REQUIRED', '世界必须持协调许可')
            return
        if row is None or row[0] != coordinator.epoch or row[1] != coordinator.owner_token:
            raise WorldError('COORDINATOR_EXECUTION_FENCED', '协调许可已失权')
        if gate is None:
            now = self._execution_now(db)
        else:
            clock_gate = gate('_execution_now', '_execution_now', world=self)
            now = clock_gate(db, 0, _first_root_pay=pay, _first_root_gate=gate)
        if now >= row[2]:
            raise WorldError('COORDINATOR_EXECUTION_EXPIRED', '协调许可已到期')

    def renew_coordinator_execution(self, permit, *, ttl_seconds=60):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self)
        self._validate_coordinator_execution(permit)
        duration = self._execution_ttl(ttl_seconds)
        with self._execution_connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self._check_coordinator_execution(db, state['world_id'], permit)
                now = self._execution_now(db, duration)
                changed = db.execute('UPDATE coordinator_execution_leases SET expires_wall_ns=?,last_wall_ns=? WHERE world_id=? AND epoch=? AND owner_token=? AND expires_wall_ns>?', (now+duration, now, permit.world_id, permit.epoch, permit.owner_token, now)).rowcount
                if not changed:
                    raise WorldError('COORDINATOR_EXECUTION_EXPIRED', '协调许可已到期')
                self._execution_floor(db, now)
                db.commit()
                return permit
            except BaseException:
                db.rollback()
                raise

    def release_coordinator_execution(self, permit):
        self._validate_coordinator_execution(permit)
        with self._execution_connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self._execution_identity(db, state['world_id'])
                if permit.world_id != state['world_id']:
                    raise WorldError('EXECUTION_WORLD_MISMATCH', '协调许可不属于此世界')
                exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_execution_leases'").fetchone()
                changed = db.execute('UPDATE coordinator_execution_leases SET owner_token=NULL WHERE world_id=? AND epoch=? AND owner_token=?', (permit.world_id, permit.epoch, permit.owner_token)).rowcount if exists else 0
                db.commit()
                return bool(changed)
            except BaseException:
                db.rollback()
                raise

    def acquire_actor_execution(self, person_id, owner_instance, *, ttl_seconds=60, coordinator=None, activation=None):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self)
        from .coordinator_scheduler import scoped, acquisition_proof, claim, source_lock
        activation = scoped(self, activation)
        coordinator = self._scoped_coordinator(coordinator)
        duration = self._execution_ttl(ttl_seconds)
        if type(owner_instance) is not str or not owner_instance.strip():
            raise WorldError('INVALID_EXECUTION', '执行宿主必须为非空字符串')
        with source_lock(self), self._execution_connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self._execution_identity(db, state['world_id'])
                self._find(state['persons'], person_id, 'UNKNOWN_PERSON')
                self._check_coordinator_execution(db, state['world_id'], coordinator)
                self._lifecycle_actor_gate(db,state,person_id)
                acquisition_proof(self, db, state, person_id, activation)
                db.execute('CREATE TABLE IF NOT EXISTS actor_execution_leases (world_id TEXT NOT NULL, person_id TEXT NOT NULL, epoch INTEGER NOT NULL, owner_token TEXT, owner_instance TEXT NOT NULL, acquired_wall_ns INTEGER NOT NULL, expires_wall_ns INTEGER NOT NULL, last_wall_ns INTEGER NOT NULL, PRIMARY KEY(world_id,person_id))')
                now = self._execution_now(db, duration)
                row = db.execute('SELECT epoch,owner_token,expires_wall_ns FROM actor_execution_leases WHERE world_id=? AND person_id=?', (state['world_id'], person_id)).fetchone()
                if row and row[1] is not None and now < row[2]:
                    if coordinator is None or self._actor_delegation(db, state['world_id'], person_id, row[0]) == (coordinator.epoch, coordinator.owner_token):
                        raise WorldError('ACTOR_EXECUTION_BUSY', '人物已有执行宿主')
                epoch = row[0]+1 if row else 1
                if not 1 <= epoch <= 2**63-1:
                    raise WorldError('ACTOR_EXECUTION_FENCED', '执行代际已耗尽')
                token = uuid.uuid4().hex
                db.execute('INSERT INTO actor_execution_leases VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(world_id,person_id) DO UPDATE SET epoch=excluded.epoch,owner_token=excluded.owner_token,owner_instance=excluded.owner_instance,acquired_wall_ns=excluded.acquired_wall_ns,expires_wall_ns=excluded.expires_wall_ns,last_wall_ns=excluded.last_wall_ns', (state['world_id'], person_id, epoch, token, owner_instance, now, now+duration, now))
                if coordinator is not None:
                    db.execute('CREATE TABLE IF NOT EXISTS actor_coordinator_delegations (world_id TEXT NOT NULL, person_id TEXT NOT NULL, actor_epoch INTEGER NOT NULL, parent_epoch INTEGER NOT NULL, parent_owner_token TEXT NOT NULL, PRIMARY KEY(world_id,person_id,actor_epoch))')
                    _history_cover(db)
                    db.execute('INSERT INTO actor_coordinator_delegations VALUES (?,?,?,?,?)', (state['world_id'], person_id, epoch, coordinator.epoch, coordinator.owner_token))
                claim(self, db, state, person_id, activation, ExecutionPermit(state['world_id'], person_id, token, epoch), coordinator)
                self._check_coordinator_execution(db, state['world_id'], coordinator)
                self._execution_floor(db, now)
                acquisition_proof(self, db, state, person_id, activation)
                self._check_coordinator_execution(db, state['world_id'], coordinator)
                db.commit()
                return ExecutionPermit(state['world_id'], person_id, token, epoch)
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _validate_execution(permit):
        if (type(permit) is not ExecutionPermit
                or any(type(v) is not str or not v.strip() for v in (permit.world_id, permit.person_id, permit.owner_token))
                or type(permit.epoch) is not int or not 1 <= permit.epoch <= 2**63-1):
            raise WorldError('INVALID_EXECUTION', '执行许可格式不合法')

    @staticmethod
    def _execution_identity(db, state_world_id, *, _first_root_pay=None, _first_root_gate=None):
        state_sql = 'SELECT state FROM snapshots ORDER BY seq LIMIT 1'
        if _first_root_gate is not None:
            pay = _first_root_gate('identity quote', '_lc_adm1148_root_pay', _first_root_pay)
            pay(db, state_sql, (), ('state',), gate=_first_root_gate)
        row = db.execute(state_sql).fetchone()
        if _first_root_gate is not None and row is not None:
            from . import coordinator_scheduler as root_source
            quote = _first_root_gate('genesis quote', '_lc_adm1148_root_json',
                                     vars(root_source).get('_lc_adm1148_root_json'))
            quote(row[0], gate=_first_root_gate)
            loads = _first_root_gate('genesis decode', 'json.loads', getattr(json, 'loads', None))
        else:
            loads = json.loads
        baseline = loads(row[0])
        if type(state_world_id) is not str or not state_world_id or baseline['world_id'] != state_world_id:
            raise WorldError('EXECUTION_WORLD_MISMATCH', '世界执行身份与首快照不符')
        return state_world_id

    @staticmethod
    def _actor_delegation(db, world_id, person_id, epoch):
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_coordinator_delegations'").fetchone()
        row = db.execute('SELECT parent_epoch,parent_owner_token FROM actor_coordinator_delegations WHERE world_id=? AND person_id=? AND actor_epoch=?', (world_id, person_id, epoch)).fetchone() if exists else None
        return tuple(row) if row is not None else None

    def _check_actor_execution(self, db, state_world_id, person_id, execution, *, readonly=False, coordinator=None):
        coordinator = self._scoped_coordinator(coordinator)
        self._check_coordinator_execution(db, state_world_id, coordinator, readonly=readonly)
        attempt = self._scoped_attempt()
        if attempt is not None:
            attempt.check()
            if execution != attempt.permit:
                raise WorldError('ACTOR_EXECUTION_FENCED', '执行尝试不可替换原许可')
        world_id = self._execution_identity(db, state_world_id)
        if execution is not None:
            self._validate_execution(execution)
            if execution.world_id != world_id:
                raise WorldError('EXECUTION_WORLD_MISMATCH', '执行许可不属于此世界')
            if execution.person_id != person_id:
                raise WorldError('EXECUTION_PERSON_MISMATCH', '执行许可不属于此人物')
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_execution_leases'").fetchone()
        row = db.execute('SELECT epoch,owner_token,expires_wall_ns FROM actor_execution_leases WHERE world_id=? AND person_id=?', (world_id, person_id)).fetchone() if exists else None
        if execution is None:
            if row:
                raise WorldError('ACTOR_EXECUTION_REQUIRED', '此人物必须持执行许可')
            if coordinator is not None:
                raise WorldError('ACTOR_EXECUTION_REQUIRED', '人物必须持执行许可')
            self._lifecycle_actor_gate(db,json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0]),person_id)
            return
        if coordinator is not None and self._actor_delegation(db, world_id, person_id, execution.epoch) != (coordinator.epoch, coordinator.owner_token):
            raise WorldError('COORDINATOR_EXECUTION_FENCED', '人物原协调授权已失权')
        if row is None or row[0] != execution.epoch or row[1] != execution.owner_token:
            raise WorldError('ACTOR_EXECUTION_FENCED', '执行许可已失权')
        now = self._execution_now(db)
        if now >= row[2]:
            raise WorldError('ACTOR_EXECUTION_EXPIRED', '执行许可已到期')
        from .coordinator_scheduler import activation_row, latest
        current=latest(db)
        self._lifecycle_actor_gate(db,current,person_id)
        activation_row(self, db, current, person_id, execution=execution)

    def renew_actor_execution(self, permit, *, ttl_seconds=60, coordinator=None):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self)
        coordinator = self._scoped_coordinator(coordinator)
        self._validate_execution(permit)
        duration = self._execution_ttl(ttl_seconds)
        from .coordinator_scheduler import maintenance_source, scoped, exists
        with maintenance_source(self) as source_captured, self._execution_connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                reference=scoped(self)
                if not source_captured and reference is not None and exists(db,'coordinator_scheduler_registration'):
                    phase=db.execute('SELECT phase FROM coordinator_activations WHERE activation_id=?',(reference.activation_id,)).fetchone()
                    if phase is not None and phase[0] in ('rejected','technical_blocked'):
                        raise WorldError('EXECUTION_STORAGE_BUSY', '拒绝证明的配置源正在占用')
                self._check_actor_execution(db, state['world_id'], permit.person_id, permit, coordinator=coordinator)
                now = self._execution_now(db, duration)
                changed = db.execute('UPDATE actor_execution_leases SET expires_wall_ns=?,last_wall_ns=? WHERE world_id=? AND person_id=? AND epoch=? AND owner_token=? AND expires_wall_ns>?', (now+duration, now, permit.world_id, permit.person_id, permit.epoch, permit.owner_token, now)).rowcount
                if not changed:
                    raise WorldError('ACTOR_EXECUTION_EXPIRED', '执行许可已到期')
                self._check_coordinator_execution(db, state['world_id'], coordinator)
                self._execution_floor(db, now)
                db.commit()
                return permit
            except BaseException:
                db.rollback()
                raise

    def release_actor_execution(self, permit):
        self._validate_execution(permit)
        with self._execution_connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self._execution_identity(db, state['world_id'])
                if permit.world_id != state['world_id']:
                    raise WorldError('EXECUTION_WORLD_MISMATCH', '执行许可不属于此世界')
                exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_execution_leases'").fetchone()
                changed = db.execute('UPDATE actor_execution_leases SET owner_token=NULL WHERE world_id=? AND person_id=? AND epoch=? AND owner_token=?', (permit.world_id, permit.person_id, permit.epoch, permit.owner_token)).rowcount if exists else 0
                db.commit()
                return bool(changed)
            except BaseException:
                db.rollback()
                raise

    def initialize(self):
        from .actor_execution import EntryBudget, ExecutionPolicy
        import threading
        entry = self._scoped_entry_budget()
        if entry is None:
            entry = EntryBudget(ExecutionPolicy(), threading.Event(),
                captured=(self._scoped_attempt(), self._scoped_coordinator_attempt()))
        entry.check()
        if self.db_path.exists():
            with _lc_cold1136_io(self, entry), self._connection(readonly=True, _cold_entry=entry) as db:
                db.execute('BEGIN')
                saved = _lc_cold1136_existing(self, db, entry)
                if saved is not None: return saved
        with _lc_cold1136_writer(self, entry) as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                db.execute('CREATE TABLE IF NOT EXISTS snapshots (seq INTEGER PRIMARY KEY, state TEXT NOT NULL)')
                saved = _lc_cold1136_existing(self, db, entry, known_snapshots=True)
                if saved is not None:
                    db.commit()
                    return saved
                config = load_scenario(self.scenario_path)
                try:
                    raw = (DATA / 'geography.json').read_bytes()
                    geography = json.loads(raw)
                except (OSError, UnicodeError, ValueError, RecursionError):
                    raise WorldError('SOURCE_DRIFT', '地理来源缺失或不是合法JSON') from None
                config = validate_scenario(config, geography)
                if hashlib.sha256(raw).hexdigest() != config['geography_sha256']:
                    raise WorldError('SOURCE_DRIFT', '地理来源摘要不符')
                state = self._initial_state(config, geography)
                db.execute('CREATE TABLE intents (id TEXT PRIMARY KEY, person TEXT NOT NULL, payload TEXT NOT NULL, receipt TEXT)')
                db.execute('CREATE TRIGGER snapshots_no_update BEFORE UPDATE ON snapshots BEGIN SELECT RAISE(ABORT, \'immutable snapshots\'); END')
                db.execute('CREATE TRIGGER snapshots_no_delete BEFORE DELETE ON snapshots BEGIN SELECT RAISE(ABORT, \'immutable snapshots\'); END')
                encoded = self._encode_state(db, state)
                db.execute('INSERT INTO snapshots VALUES (?,?)', (1, encoded))
                db.execute('CREATE TABLE scheduler_genesis (world_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version=1), initial_revision INTEGER NOT NULL CHECK(initial_revision=1), initial_sha256 TEXT NOT NULL)')
                _history_cover(db)
                db.execute('INSERT INTO scheduler_genesis VALUES (?,1,1,?)', (state['world_id'], hashlib.sha256(encoded.encode('utf-8')).hexdigest()))
                db.commit()
                return self._public(state)
            except ScenarioError as error:
                db.rollback()
                raise WorldError(error.code, str(error)) from None
            except BaseException as error:
                if not _lc_cold1136_p04_signal(db, error): db.rollback()
                raise

    def _p04_initial(self, state):
        """Explicit new-world opt-in; temporary identities exist before genesis.

        This is not inheritance or an NPC backstory generator. Descriptors may
        contain only authored identity/card/location, never private history.
        """
        rules = state['_config'].get('event_wakeup')
        if rules is None:
            return
        try:
            if (type(rules) is not dict or set(rules) != {'version','government_interval_seconds','temporary_steps','temporary_persons'}
                    or rules['version'] not in ('p04-event-wakeup-v1',_LC_WAKEUP)
                    or type(rules['government_interval_seconds']) is not int
                    or not 1 <= rules['government_interval_seconds'] <= 30*DAY
                    or type(rules['temporary_steps']) is not int or not 1 <= rules['temporary_steps'] <= 8
                    or type(rules['temporary_persons']) is not list or len(rules['temporary_persons']) > 16):
                raise ValueError('rules')
            people = {p['id'] for p in state['persons']}
            reserved = people | {c['id'] for c in state['countries']} | {p['id'] for p in state['places']}
            reserved |= {p['office_id'] for p in state['persons'] if 'office_id' in p}
            for person in rules['temporary_persons']:
                if (type(person) is not dict or set(person) != {'id','name','country_id','office','location','card'}
                        or any(type(person[k]) is not str or not person[k].strip() or len(person[k])>128
                               for k in ('id','name','country_id','office','location'))
                        or person['id'] in reserved or person['office'] in ('king','finance','marshal')
                        or person['name'] in {p['name'] for p in state['persons']}
                        or type(person['card']) is not dict or set(person['card']) != {'values','interests','risk','mission'}
                        or any(type(v) is not str or not v.strip() or len(v)>2000 for v in person['card'].values())
                        or not any(p['id']==person['location'] and p['country_id']==person['country_id'] for p in state['places'])):
                    raise ValueError('temporary identity')
                reserved.add(person['id'])
                state['persons'].append(dict(json.loads(_json(person)),card_version=1,card_author='scenario'))
                state['_observations'][person['id']] = []
                state['_messages'][person['id']] = []
            temporary = [p['id'] for p in rules['temporary_persons']]
            state['_p04'] = dict(rules=json.loads(_json(rules)),pending=[p['id'] for p in state['persons'] if p['id'] not in temporary],
                                 remaining={p:0 for p in temporary},last_grant={p:-1 for p in temporary},dispatch=[],
                                 resident_tick_seconds=60,
                                 resident_last_dispatch={p['id']:-1 for p in state['persons'] if p['id'] not in temporary})
            self._schedule(state,rules['government_interval_seconds'],'p04_government')
        except (KeyError,TypeError,ValueError,RecursionError):
            raise WorldError('INVALID_EVENT_WAKEUP','事件唤醒新局声明或临时人物不合法') from None

    @staticmethod
    def _check_frozen_p04(state, baseline):
        if (type(state) is not dict or type(baseline) is not dict
                or '_lifecycle' not in state and '_lifecycle' not in baseline):
            return WorldStore._check_frozen_p04_owned(state, baseline)
        from .coordinator_scheduler import _lifecycle_value_invoke
        return _lifecycle_value_invoke(WorldStore._check_frozen_p04_owned,
            (state, baseline), inputs=(_lc_geo1133_direct_input(state),
                _lc_geo1133_direct_input(baseline)), direct_factor=2)

    @staticmethod
    def _check_frozen_p04_owned(state, baseline):
        _lifecycle_shape(state,baseline)
        try:
            if '_p04' not in baseline:
                if '_p04' in state:
                    raise ValueError('late opt-in')
                return
            original = baseline['_p04']['rules']
            if (_lc_canonical(state['_config'].get('event_wakeup')) != _lc_canonical(original)
                    or _lc_canonical(state['_p04']['rules']) != _lc_canonical(original)):
                raise ValueError('rules changed')
            data = state['_p04']; temporary = {p['id'] for p in original['temporary_persons']}
            people = {p['id'] for p in state['persons']}
            residents = people - temporary
            if (type(data) is not dict
                    or set(data) != {'rules','pending','remaining','last_grant','dispatch','resident_tick_seconds','resident_last_dispatch'}
                    or type(data['remaining']) is not dict or type(data['last_grant']) is not dict
                    or set(data['remaining']) != temporary or set(data['last_grant']) != temporary
                    or type(data['resident_tick_seconds']) is not int or data['resident_tick_seconds'] != 60
                    or type(baseline['_p04'].get('resident_tick_seconds')) is not int
                    or baseline['_p04']['resident_tick_seconds'] != 60
                    or type(data['resident_last_dispatch']) is not dict
                    or set(data['resident_last_dispatch']) != residents):
                raise ValueError('state shape')
            for key in ('pending','dispatch'):
                values = data[key]
                if type(values) is not list or any(type(p) is not str or p not in people for p in values) or len(values)!=len(set(values)):
                    raise ValueError('roster')
            for pid in residents:
                last = data['resident_last_dispatch'][pid]
                if (type(last) is not int or not -1 <= last <= state['time']
                        or (pid in data['pending'] and last == state['time'])
                        or (pid in data['dispatch'] and last != state['time'])):
                    raise ValueError('resident dispatch ledger')
            # Opt-in only: the shared producer counter and identities belong to
            # the whole queue, not one person or one event kind. Never repair a
            # duplicate by silently dropping or renumbering a persisted node.
            if (type(state['_queue_seq']) is not int or not 0 <= state['_queue_seq'] <= 9223372036854775807
                    or type(state['queue']) is not list):
                raise ValueError('queue counter')
            ids=set();seqs=set()
            for node in state['queue']:
                if (type(node) is not dict or type(node['seq']) is not int
                        or not 1 <= node['seq'] <= state['_queue_seq']
                        or type(node['id']) is not str or node['id']!='q-'+str(node['seq'])
                        or node['id'] in ids or node['seq'] in seqs):
                    raise ValueError('whole queue identity collision')
                ids.add(node['id']);seqs.add(node['seq'])
            deferred = [q for q in state['queue'] if q['kind']=='p04_deferred_wake']
            if len({q['person_id'] for q in deferred}) != len(deferred):
                raise ValueError('duplicate deferred wake')
            for q in deferred:
                if (type(q) is not dict or set(q)-{'cause_id'} != {'id','seq','at','kind','person_id','requested_at'}
                        or ('cause_id' in q and (type(q['cause_id']) is not str or not q['cause_id']))
                        or type(q['person_id']) is not str or q['person_id'] not in residents
                        or type(q['seq']) is not int or not 1 <= q['seq'] <= state['_queue_seq']
                        or q['id'] != 'q-'+str(q['seq'])
                        or type(q['requested_at']) is not int
                        or not 0 <= q['requested_at'] <= data['resident_last_dispatch'][q['person_id']]
                        or type(q['at']) is not int or q['at'] != q['requested_at']+data['resident_tick_seconds']
                        or q['at'] <= state['time']):
                    raise ValueError('deferred wake node')
            for pid in temporary:
                if (type(data['remaining'][pid]) is not int or not 0 <= data['remaining'][pid] <= original['temporary_steps']
                        or type(data['last_grant'][pid]) is not int or not -1 <= data['last_grant'][pid] <= state['time']):
                    raise ValueError('finite allowance')
            if not _lc_optin(baseline) and [(p['id'],p['country_id'],p['office']) for p in state['persons']] != [(p['id'],p['country_id'],p['office']) for p in baseline['persons']]:
                raise ValueError('identity drift')
        except (KeyError,TypeError,ValueError,RecursionError):
            raise WorldError('FROZEN_EVENT_WAKEUP','事件唤醒规则或人物身份与首快照不符') from None

    @staticmethod
    def _p04_continuity(anchor, state, committed_meeting):
        """Narrow post-opening projection, not a replay of the whole world.

        Ledger/dispatch change only at proven openings. Actual meeting events
        are joined to original intent/receipt (including DM child lineage), not
        a witness/hash supplied by the state being checked. Retained obligations
        keep their original complete node. New IDs are checked against the whole
        queue by the frozen shape guard and against the anchor counter here.
        """
        if '_p04' not in anchor:
            return
        try:
            old=anchor['_p04']; new=state['_p04']
            expected_ledger=dict(old['resident_last_dispatch'])
            if '_lifecycle' in anchor:
                for pid,origin in state['_lifecycle']['person_origins'].items():
                    if pid not in anchor['_lifecycle']['person_origins']:
                        if origin['introduced_revision']<=anchor['revision']:_lc_fail()
                        expected_ledger[pid]=-1
            if (state['world_id']!=anchor['world_id'] or state['revision']<anchor['revision']
                    or (old['dispatch'] and state['time']!=anchor['time'])
                    or new['resident_last_dispatch']!=expected_ledger
                    or new['dispatch']!=old['dispatch'] or state['_queue_seq']<anchor['_queue_seq']):
                raise ValueError('post-opening ledger continuity')
            old_events=[e for e in anchor['events'] if e['kind']=='meeting']
            events=[e for e in state['events'] if e['kind']=='meeting']
            if events[:len(old_events)]!=old_events:
                raise ValueError('meeting prefix')
            expected={q['person_id']:q for q in anchor['queue'] if q['kind']=='p04_deferred_wake'}
            created={}; causes=set(); previous_seq=anchor['_queue_seq']
            for event in events[len(old_events):]:
                cause=event['cause_id']
                if cause in causes or event['time']!=state['time']:
                    raise ValueError('meeting cause/time')
                causes.add(cause)
                person,target,child=committed_meeting(cause)
                if event['person_id']!=person:
                    raise ValueError('meeting actor')
                if target in old['resident_last_dispatch'] and old['resident_last_dispatch'][target]==state['time'] and target not in expected:
                    spec=dict(person_id=target,requested_at=state['time'],at=state['time']+old['resident_tick_seconds'])
                    if child:spec['cause_id']=cause
                    created[target]=spec;expected[target]=None
            if '_lifecycle' in anchor:
                deliveries=state['_lifecycle']['deliveries'][len(anchor['_lifecycle']['deliveries']):]
                for delivery in deliveries:
                    target=delivery['recipient_person_id']
                    if old['resident_last_dispatch'].get(target)==state['time'] and target not in expected:
                        spec=dict(person_id=target,requested_at=state['time'],at=state['time']+old['resident_tick_seconds'],cause_id=delivery['cause_ref']['child_intent_id'])
                        created[target]=spec;expected[target]=None
            actual={q['person_id']:q for q in state['queue'] if q['kind']=='p04_deferred_wake'}
            if set(actual)!=set(expected):
                raise ValueError('post-action deferred obligations')
            for pid,node in expected.items():
                if node is not None:
                    if actual[pid]!=node:raise ValueError('earliest deferred identity changed')
                else:
                    q=actual[pid];spec=created[pid]
                    if ({k:v for k,v in q.items() if k not in ('id','seq','kind')}!=spec
                            or q['seq']<=previous_seq):
                        raise ValueError('new deferred provenance/order')
                    previous_seq=q['seq']
        except (KeyError,TypeError,ValueError,IndexError,RecursionError):
            raise WorldError('INVALID_EVENT_WAKEUP','动作后常驻账本或延迟义务不符合已证开窗及原提交事实') from None

    @staticmethod
    def _p04_target(state):
        if '_p04' not in state:
            return min(state['time']+DAY,state['horizon'])
        if state['_p04']['pending']:
            return min(state['time'],state['horizon'])
        times = [q['at'] for q in state['queue']]
        if any(type(t) is not int or t < state['time'] for t in times):
            raise WorldError('INVALID_EVENT_WAKEUP','事件时刻不可倒退')
        return min([state['horizon'],*times])

    @staticmethod
    def _p04_wake(state, person_id):
        if '_lifecycle' in state and not _lc_active(state,person_id):return
        if '_p04' not in state:
            return
        data = state['_p04']
        if person_id in data['resident_last_dispatch']:
            if data['resident_last_dispatch'][person_id] == state['time']:
                # Words/receipts have already committed at the actual meeting
                # time. Coalesce only scheduling obligations, never business
                # effects. A fixed frozen 60 simulated-second response cadence
                # creates a real queue node, not a wall-clock sleep or time jump.
                if not any(q['kind']=='p04_deferred_wake' and q['person_id']==person_id for q in state['queue']):
                    WorldStore._schedule(state,state['time']+data['resident_tick_seconds'],
                                         'p04_deferred_wake',person_id=person_id,requested_at=state['time'])
                return
        elif person_id not in data['remaining']:
            raise WorldError('INVALID_EVENT_WAKEUP','未知唤醒人物')
        if person_id in data['remaining']:
            # No same-instant refill by a chain of immediate meetings. A real
            # later event can grant another finite window; private reads cannot.
            if data['last_grant'][person_id] < state['time']:
                data['last_grant'][person_id] = state['time']
                data['remaining'][person_id] = data['rules']['temporary_steps']
            if data['remaining'][person_id] == 0:
                return
        if person_id not in data['pending']:
            data['pending'].append(person_id)

    @staticmethod
    def _p04_expected(base, advanced, *, genesis=None):
        if (type(base) is not dict or type(advanced) is not dict
                or '_lifecycle' not in base and '_lifecycle' not in advanced):
            return WorldStore._p04_expected_owned(base, advanced, genesis=genesis)
        from .coordinator_scheduler import _lifecycle_value_invoke
        def produce():
            return _lc_copy(WorldStore._p04_expected_owned(base, advanced, genesis=genesis))
        return _lifecycle_value_invoke(produce, (), inputs=(base, advanced, genesis),
            direct_factor=2, detached=True)

    @staticmethod
    def _p04_expected_owned(base, advanced, *, genesis=None):
        """Independently derive roster AND resident transition from bound input.

        Native and offlock callers supply their own authenticated base/advanced
        snapshots. Neither dispatch nor the advanced ledger authorizes itself.
        Due deferred nodes carry only an outstanding wake, not repeated words.
        """
        if '_p04' not in base:
            return [] if advanced['time']==advanced['horizon'] else [p['id'] for p in advanced['persons']]
        if ('_lifecycle' in base or '_lifecycle' in advanced
                or _lc_optin(base) or _lc_optin(advanced)):
            if (type(genesis) is not dict or type(genesis.get('revision')) is not int
                    or genesis['revision'] != 1
                    or genesis.get('world_id') != base.get('world_id')
                    or genesis.get('world_id') != advanced.get('world_id')):
                _lc_fail('lifecycle roster requires the actual immutable genesis')
            WorldStore._check_frozen_p04(base,genesis)
            WorldStore._check_frozen_p04(advanced,genesis)
        else:
            WorldStore._check_frozen_p04(base,base)
        at = advanced['time']
        data = base['_p04']
        temporary = set(data['remaining'])
        pending = set(data['pending'])
        government = any(q['kind']=='p04_government' and q['at']==at for q in base['queue'])
        old_deferred = {q['person_id']:q for q in base['queue'] if q['kind']=='p04_deferred_wake'}
        kept = {pid:q for pid,q in old_deferred.items() if q['at']>at and _lc_active(advanced,pid)}
        pending.update(pid for pid,q in old_deferred.items() if q['at']==at)
        for p in advanced['persons']:
            pid = p['id']
            new_obs = advanced['_observations'][pid][len(base['_observations'].get(pid,[])):]
            new_msg = advanced['_messages'][pid][len(base['_messages'].get(pid,[])):]
            event = bool(new_obs or any(m['from']!=pid for m in new_msg))
            if event or (government and pid not in temporary):
                if pid not in temporary or data['last_grant'][pid] < at or data['remaining'][pid]>0:
                    pending.add(pid)
        if '_lifecycle' in advanced:
            pending={pid for pid in pending if _lc_active(advanced,pid) and next(p for p in advanced['persons'] if p['id']==pid)['location'] is not None}
        spent = {pid for pid in pending if pid in data['resident_last_dispatch']
                 and data['resident_last_dispatch'][pid]==at}
        pending -= spent
        roster = [] if at==advanced['horizon'] else [p['id'] for p in advanced['persons'] if p['id'] in pending]
        expected_ledger = dict(data['resident_last_dispatch'])
        for pid in roster:
            if pid in expected_ledger:
                expected_ledger[pid] = at
        actual_deferred = {q['person_id']:q for q in advanced['queue'] if q['kind']=='p04_deferred_wake'}
        if (advanced['_p04']['resident_last_dispatch'] != expected_ledger
                or set(actual_deferred) != set(kept)|spent
                or any(actual_deferred[pid] != q for pid,q in kept.items())
                or any((actual_deferred[pid]['requested_at'] != at
                        or actual_deferred[pid]['at'] != at+data['resident_tick_seconds']
                        or actual_deferred[pid]['seq'] <= base['_queue_seq']) for pid in spent-set(kept))
                or set(advanced['_p04']['pending']) != (pending if at==advanced['horizon'] else set())):
            raise WorldError('INVALID_EVENT_WAKEUP','常驻派发或延迟节点不符合已认证基础及真实到期事实')
        return roster

    @staticmethod
    def _p04_open(state):
        if '_p04' not in state:
            return [] if state['time']==state['horizon'] else [p['id'] for p in state['persons']]
        data = state['_p04']
        roster = [] if state['time']==state['horizon'] else [p['id'] for p in state['persons'] if p['id'] in data['pending'] and ('_lifecycle' not in state or (_lc_active(state,p['id']) and p['location'] is not None))]
        data['dispatch'] = roster
        if state['time']!=state['horizon']:
            data['pending'] = []
            for pid in roster:
                if pid in data['remaining']:
                    if data['remaining'][pid]<1:
                        raise WorldError('INVALID_EVENT_WAKEUP','无自主额度的临时人物不可开窗')
                    data['remaining'][pid]-=1
                else:
                    if data['resident_last_dispatch'][pid] >= state['time']:
                        raise WorldError('INVALID_EVENT_WAKEUP','常驻人物同刻不可重复派发')
                    data['resident_last_dispatch'][pid] = state['time']
        return roster

    @staticmethod
    def _p04_committed(state, person_id, body):
        if '_p04' not in state or person_id not in state['_p04']['remaining']:
            return
        data = state['_p04']
        if body['action']=='wait':
            data['remaining'][person_id] = 0
            data['pending'] = [p for p in data['pending'] if p!=person_id]
        elif data['remaining'][person_id]>0 and person_id not in data['pending']:
            # Same-person next activation, only after real durable world commit.
            data['pending'].append(person_id)

    def _initial_state(self, config, geography):
        try:
            mechanism_sha256 = pathology_digest(config.get('pathology'))
        except ValueError as error:
            raise WorldError('INVALID_PATHOLOGY', str(error)) from None
        regions = {r['id']: r for r in geography['regions']}
        countries, places, persons = [], [], []
        if config.get('schema_version') in (2, 3):
            for i, country in enumerate(config['countries']):
                countries.append({k: v for k, v in country.items() if k not in ('places', 'positions')})
                for j, place in enumerate(country['places']):
                    places.append({**place, 'country_id': country['id'], 'x':120+i*240, 'y':140+j*160,
                                   'neighbors': regions[place['id']]['neighbors'],
                                   'disease': {'susceptible':place['population'], 'cohorts':[], 'restricted':False}})
                for position in country['positions']:
                    persons.append({'id':position['person_id'], 'name':position['person_name'],
                                    'country_id':country['id'], 'office':position['office'], 'office_id':position['id'],
                                    'location':position['location'], 'card':position['card'],
                                    'card_version':1, 'card_author':'scenario'})
        else:
            countries, places, persons = self._legacy_entities(config, geography, regions)
        state = {'world_id': str(uuid.uuid4()), 'revision': 1, 'time': 0, 'horizon': config['horizon_days']*DAY,
                'status':'user_paused', 'mode':'scripted', 'countries': countries, 'places':places, 'persons':persons,
                'events':[], 'queue':[], 'in_transit':[], '_config':config, '_geography':regions,
                '_queue_seq': 0, 'ledger': {'initial': sum(p['grain'] for p in places), 'consumed':0, 'harvested':0, 'unmet_consumption':0}}
        state.update(scenario_id=config.get('scenario_id', 'legacy-four-country'), scenario_version=config['version'],
                     scenario_sha256=scenario_digest(config))
        state['pathology'] = json.loads(_json(config['pathology']))
        state['pathology_frozen_at'] = 0
        state['pathology_sha256'] = mechanism_sha256
        if 'pathology_runtime' in config:
            state['pathology_runtime'] = config['pathology_runtime']
            state['pathology_mechanism_sha256'] = contact_mechanism_digest(config['pathology_runtime'], state['pathology'])
        if config.get('schema_version') == 3:
            from .scenario import economy_digest
            economy = json.loads(_json(config['economy']))
            initial = economy['initial_materials']
            zeros = {pid: {rid: 0 for rid in values} for pid, values in initial.items()}
            state['economy'] = {
                'rules_version': economy['version'], 'rules_sha256': economy_digest(economy), 'frozen_at': 0,
                'units': economy['units'], 'recipes': economy['recipes'],
                'labor_capacity_hours_per_day': economy['labor_hours_per_day'],
                'materials': json.loads(_json(initial)),
                'material_ledger': {'initial': initial, 'produced': json.loads(_json(zeros)),
                                    'consumed': json.loads(_json(zeros)), 'lost': json.loads(_json(zeros))},
                'tasks': []}
        self._initial_military(state)
        state['_observations'] = {p['id']: [] for p in persons}
        state['_commands'] = []
        state['_messages'] = {p['id']: [] for p in persons}
        self._p04_initial(state)
        if _lc_optin(state):state['_lifecycle']=_lc_seed(state)
        self._initial_projects(state)
        self._infect(state, next(place for place in places if place['id'] == 'place-1'), 1)
        self._schedule(state, config['pathology']['contact_offset'], 'contact')
        self._schedule(state, DAY, 'consume')
        if config.get('schema_version') != 3:
            self._schedule(state, config['harvest_day']*DAY, 'harvest')
        return state

    @staticmethod
    def _legacy_entities(config, geography, regions):
        countries, places, persons = [], [], []
        for i, country in enumerate(geography['countries']):
            pair = config['pairs'][i]
            countries.append({k: country[k] for k in ('id', 'name')} | {'color': ['#b86d51','#548bb0','#b59e42','#7b8e53'][i]})
            for j, place_id in enumerate(pair):
                region = regions[place_id]
                settlement = next(s for s in geography['settlements'] if s['place_id'] == place_id)
                places.append({'id': place_id, 'name': settlement['source_name'], 'country_id': country['id'],
                               'population': config['population'], 'grain': config['grain'], 'x':120+i*240, 'y':140+j*160,
                               'source_region_id':region['source_region_id'], 'source_settlement_id':settlement['source_settlement_id'],
                               'source_cell':settlement['source_cell'], 'neighbors':region['neighbors'],
                               'disease':{'susceptible':config['population'], 'cohorts':[], 'restricted':False}})
            for role, title, j in [('king','君主',0), ('finance','内政财政官',0), ('marshal','军事官',1)]:
                persons.append({'id':f"person-{country['id']}-{role}", 'name':country['name']+title,
                                'country_id':country['id'], 'office':role, 'location':pair[j],
                                'card':{'values':'珍视秩序与民生', 'interests':'维护本国与本人职责', 'risk':'谨慎', 'mission':'在条件允许时推进文明'},
                                'card_version':1, 'card_author':'scenario'})
        return countries, places, persons

    @staticmethod
    def _schedule(state, at, kind, **data):
        state['_queue_seq'] += 1
        state['queue'].append({'id': f"q-{state['_queue_seq']}", 'seq': state['_queue_seq'], 'at': at, 'kind': kind, **data})
        state['queue'].sort(key=lambda q: (q['at'], q['seq']))
        return state['queue'][-1] if not data else next(q for q in state['queue'] if q['seq'] == state['_queue_seq'])

    @staticmethod
    def _event(state, kind, text, person_id=None, cause_id=None):
        event = {'id':f"event-{len(state['events'])+1}", 'time':state['time'], 'kind':kind, 'text':text, 'person_id':person_id}
        if cause_id is not None:
            event['cause_id'] = cause_id
        state['events'].append(event)
        return event

    @staticmethod
    def _find(items, object_id, code):
        if type(object_id) is not str or not object_id:
            raise WorldError(code, '标识必须为非空字符串')
        for item in items:
            if item['id'] == object_id:
                return item
        raise WorldError(code, '未知标识')

    def _notice(self, state, person_id, text, source):
        if not _lc_alive(state,person_id):return
        observations = state['_observations'][person_id]
        observations.append({'id':f"observation-{person_id}-{len(observations)+1}", 'time':state['time'], 'text':text, 'source':source})

    def _local_notice(self, state, place, text):
        for person in state['persons']:
            if person['location'] == place['id']:
                self._notice(state, person['id'], text, '亲见：'+place['name'])

    def _infect(self, state, place, number):
        disease = place['disease']
        disease['susceptible'] -= number
        cohort = {'id':f"cohort-{place['id']}-{len(disease['cohorts'])+1}", 'infected_at':state['time'], 'count':number, 'stage':'latent'}
        disease['cohorts'].append(cohort)
        for stage, delay in [('symptomatic','symptom_seconds'), ('severe','severe_seconds'), ('recovered','recovery_seconds')]:
            self._schedule(state, state['time']+state['pathology'][delay], 'stage', place_id=place['id'], cohort_id=cohort['id'], stage=stage)

    def observe(self, person_id, seq=None):
        return self._read_observation(person_id, seq)

    def _observation_envelope(self, person_id):
        return self._read_observation(person_id, None, envelope=True)

    def _read_observation(self, person_id, seq, *, envelope=False):
        # Private records are read through a read-only connection, never a public snapshot.
        if seq is not None and (type(seq) is not int or seq < 1):
            raise WorldError('INVALID_SEQUENCE', '提交序号必须为正整数')
        if not self.db_path.exists():
            raise WorldError('NOT_INITIALIZED', '世界尚未初始化')
        with self._connection(readonly=True) as db:
            db.execute('BEGIN')
            row = db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1' if seq is None else 'SELECT state FROM snapshots WHERE seq=?', () if seq is None else (seq,)).fetchone()
            if row is None:
                raise WorldError('UNKNOWN_SNAPSHOT', '不存在该提交序号')
            state = json.loads(row[0])
            self._lifecycle_check(db,state)
        person = self._find(state['persons'], person_id, 'UNKNOWN_PERSON')
        location = next((p['name'] for p in state['places'] if p['id'] == person['location']), '旅途中')
        identity = {k:person[k] for k in ('id','name','country_id','office','card')}
        identity['location'] = location
        view = json.loads(_json({'person':identity, 'time':state['time'], 'revision':state['revision'], 'observations':state['_observations'][person_id]+self._military_observations(state, person),
                                 'messages':[{k:v for k,v in m.items() if k!='project_knowledge_source'} for m in state['_messages'][person_id]], 'contacts':[{k:p[k] for k in ('id','name','office','country_id')} for p in state['persons'] if p['country_id'] == person['country_id']]}))
        if '_lifecycle' in state:
            view['person'].update(alive=_lc_alive(state,person_id),current_office=_lc_tenure(state,person_id) is not None)
            for contact in view['contacts']:
                contact.update(alive=_lc_alive(state,contact['id']),current_office=_lc_tenure(state,contact['id']) is not None)
            # Same already-proved observation snapshot; never infer ownership
            # from office/tenure, or publish the DM's wider fact projection.
            view['personal_objects'] = [dict(id=item['id'], kind=item['kind'],
                description=item['source_text'])
                for item in state['_lifecycle']['rules']['personal_objects']
                if item['holder_person_id'] == person_id]
            view['office_documents']=[{k:d[k] for k in ('id','office_id','place_id','author_person_id','created_revision')} for d in state['_lifecycle']['documents'] if d['place_id']==person['location'] and d['office_id']==person.get('office_id') and _lc_active(state,person_id)]
        return dict(view=view,world_id=state['world_id'],revision=state['revision'],card_version=person.get('card_version',1)) if envelope else view

    def _process_event(self, state, event):
        config, ledger = state['_config'], state['ledger']
        if event['kind']=='person_death_due':
            self._lifecycle_death_due(state,event)
            return
        if state.get('pathology_runtime') == CONTACT_RUNTIME and event['kind'] in ('arrival','shipment','deliver'):
            completed = state.setdefault('_completed_contact_tasks', [])
            packets = state.setdefault('_completed_contact_packets', [])
            if (event['id'] in completed or
                    event.get('contact_packet_id') in packets):
                return
            # Stored in the same transaction as arrival and exposure; failures
            # roll this marker back together with inventory/messages/cohorts.
            completed.append(event['id'])
            if event.get('contact_packet_id'):
                packets.append(event['contact_packet_id'])
        if event['kind']=='p04_deferred_wake':
            if ('_p04' not in state or event['at']!=state['time']
                    or event['person_id'] not in state['_p04']['resident_last_dispatch']
                    or event['at'] != event['requested_at']+state['_p04']['resident_tick_seconds']
                    or event['requested_at'] > state['_p04']['resident_last_dispatch'][event['person_id']]):
                raise WorldError('INVALID_EVENT_WAKEUP','非法延迟唤醒节点')
            self._p04_wake(state,event['person_id'])
        elif event['kind']=='p04_government':
            if '_p04' not in state or event['at']!=state['time']:
                raise WorldError('INVALID_EVENT_WAKEUP','非法政务节点')
            temporary = set(state['_p04']['remaining'])
            for p in state['persons']:
                if p['id'] not in temporary:
                    self._p04_wake(state,p['id'])
            self._event(state,'government','定期政务节点实际到期；各常驻人物仍自行选择意图。')
            self._schedule(state,state['time']+state['_p04']['rules']['government_interval_seconds'],'p04_government')
        elif event['kind'].startswith('military_'):
            self._military_event(state, event)
            self._project_conditions(state, 'military-event:'+event['id'])
        elif event['kind'] == 'deliver':
            target = self._find(state['persons'], event['recipient'], 'UNKNOWN_PERSON')
            if not _lc_alive(state,target['id']) or target['location'] != event['destination']:
                self._event(state, 'undeliverable', '信使抵达原址，但收件人已经离开。', cause_id=event.get('cause_id'))
                return
            sender = self._find(state['persons'], event['sender'], 'UNKNOWN_PERSON')
            message = {'id':event['message_id'], 'time':state['time'], 'from':sender['id'], 'text':event['text']}
            if event.get('command_id'):
                command = self._find(state['_commands'], event['command_id'], 'UNKNOWN_COMMAND')
                command['delivered'] = True
                message['command_id'] = command['id']
            if 'project_knowledge_source' in event:
                message['project_knowledge_source'] = event['project_knowledge_source']
            state['_messages'][target['id']].append(message)
            self._project_receive(state, message, target['id'])
            arrival = self._event(state, 'message_delivered', sender['name']+'的信使实际抵达。', target['id'], cause_id=event.get('cause_id'))
            self._arrive_contact(state, event, arrival)
        elif event['kind'] == 'shipment':
            shipment = self._find(state['in_transit'], event['shipment_id'], 'UNKNOWN_SHIPMENT')
            place = self._find(state['places'], shipment['to'], 'UNKNOWN_PLACE')
            place['grain'] += shipment['quantity']
            state['in_transit'].remove(shipment)
            text = f"{shipment['quantity']}份粮实际到达并入仓。"
            arrival = self._event(state, 'shipment_arrived', place['name']+'：'+text, cause_id=event.get('cause_id'))
            self._local_notice(state, place, text)
            self._arrive_contact(state, event, arrival)
        elif event['kind'] == 'investigation':
            person = self._find(state['persons'], event['person_id'], 'UNKNOWN_PERSON')
            if person['location'] != event['place_id'] or person.get('journey_version',0) != event['journey_version']:
                self._event(state, 'investigation_interrupted', person['name']+'离开现场，调查未完成。', person['id'], cause_id=event.get('cause_id'))
                return
            place = self._find(state['places'], event['place_id'], 'UNKNOWN_PLACE')
            ruling = event['ruling']
            if ruling == '大失败':
                text = '调查者提出未经证实的猜测：也许只是劳累；这不是已证实病因。'
            elif ruling == '失败':
                text = '调查证据不足，尚不能下结论。'
            elif person['office'] == 'finance' and person['country_id'] == place['country_id'] and self._controller(state, place) == person['country_id']:
                text = f"核账：此刻本地粮{place['grain']}份；数字来自本地账簿清点。"
            else:
                symptomatic = sum(c['count'] for c in place['disease']['cohorts'] if c['stage'] in ('symptomatic','severe'))
                text = '现场记录：有人咳嗽发热，病因未证实。' if symptomatic else '现场暂未发现明显咳嗽发热，不能据此断言没有潜在病患。'
            if ruling == '大成功':
                text += ' 聚集限制'+('已实施。' if place['disease']['restricted'] else '未实施。')
            self._notice(state, person['id'], text, '现场调查：'+place['name'])
            self._event(state, 'investigation_completed', person['name']+'完成现场调查。', person['id'], cause_id=event.get('cause_id'))
        elif event['kind'] == 'arrival':
            person = self._find(state['persons'], event['person_id'], 'UNKNOWN_PERSON')
            place = self._find(state['places'], event['destination'], 'UNKNOWN_PLACE')
            person['location'] = place['id']
            arrival = self._event(state, 'arrival', person['name']+'抵达'+place['name'], person['id'], cause_id=event.get('cause_id'))
            self._notice(state, person['id'], '已抵达'+place['name'], '实际旅行')
            self._arrive_contact(state, event, arrival)
        elif event['kind'] == 'stage':
            place = self._find(state['places'], event['place_id'], 'UNKNOWN_PLACE')
            cohort = self._find(place['disease']['cohorts'], event['cohort_id'], 'UNKNOWN_COHORT')
            cohort['stage'] = event['stage']
            text = {'symptomatic':'当地有人咳嗽发热。', 'severe':'部分患者症状加重。', 'recovered':'部分患者症状缓解。'}[event['stage']]
            self._event(state, 'symptom', place['name']+'：'+text)
            self._local_notice(state, place, text)
        elif event['kind'] == 'contact':
            rules = state['pathology']
            for place in state['places']:
                d = place['disease']
                active = sum(c['count'] for c in d['cohorts'] if c['stage'] in ('symptomatic','severe'))
                contacts = active*rules['restricted_contacts' if d['restricted'] else 'open_contacts']
                new = min(d['susceptible'], contacts//rules['contacts_per_infection'])
                if new:
                    self._infect(state, place, new)
            self._schedule(state, state['time']+DAY, 'contact')
        elif event['kind'] == 'consume':
            for place in state['places']:
                amount = min(place['grain'], config['daily_consumption'])
                place['grain'] -= amount
                ledger['consumed'] += amount
                ledger['unmet_consumption'] += config['daily_consumption']-amount
            self._event(state, 'consumption', '各地按实际库存消耗口粮，缺口单独入账。')
            self._schedule(state, state['time']+DAY, 'consume')
        elif event['kind'] == 'project_stage':
            self._finish_project(state, event)
        elif event['kind'] == 'production':
            self._finish_production(state, event)
        elif event['kind'] == 'material_shipment':
            self._finish_material_transport(state, event)
        elif event['kind'] == 'harvest':
            for place in state['places']:
                place['grain'] += config['harvest_quantity']
                ledger['harvested'] += config['harvest_quantity']
            self._event(state, 'harvest', '本期收获实际入仓。')
            self._schedule(state, state['time']+config['harvest_day']*DAY, 'harvest')

    @staticmethod
    def _public(state):
        from .coordinator_scheduler import _lifecycle_value_context
        owner = _lifecycle_value_context.get()
        if owner is not None:owner.reserve(256+128*len(state),'public-result-field-container')
        return _lc_json(_lc_canonical({k:v for k,v in state.items() if not k.startswith('_')}))

    @staticmethod
    def _dynamic_state(state):
        # Static geography is immutable and stored once per world, not per action.
        return {key: value for key, value in state.items() if key != '_geography'}

    def _encode_state(self, db, state):
        db.execute('CREATE TABLE IF NOT EXISTS world_constants (key TEXT PRIMARY KEY, value TEXT NOT NULL, sha256 TEXT NOT NULL)')
        _history_cover(db)
        if not db.execute("SELECT 1 FROM world_constants WHERE key='geography'").fetchone():
            geography = _json(state['_geography'])
            db.execute('INSERT INTO world_constants VALUES (?,?,?)',
                       ('geography', geography, hashlib.sha256(geography.encode('utf-8')).hexdigest()))
        db.execute("CREATE TRIGGER IF NOT EXISTS constants_no_update BEFORE UPDATE ON world_constants BEGIN SELECT RAISE(ABORT, 'immutable world constants'); END")
        db.execute("CREATE TRIGGER IF NOT EXISTS constants_no_delete BEFORE DELETE ON world_constants BEGIN SELECT RAISE(ABORT, 'immutable world constants'); END")
        from .coordinator_scheduler import _lifecycle_value_context
        owner = _lifecycle_value_context.get()
        if owner is not None:owner.reserve(256+128*len(state),'encoded-state-field-container')
        return _lc_canonical(self._dynamic_state(state))

    @staticmethod
    def _restore_geography(db, state, *, cold_decode=None, _pristine_loan=False):
        from .coordinator_scheduler import _lifecycle_value_context
        if cold_decode is not None:
            baseline = cold_decode(_lc_root_row(db, 'SELECT state FROM snapshots ORDER BY seq LIMIT 1', columns=('state',))[0])
        elif _lifecycle_value_context.get() is None:
            baseline = _bootstrap_baseline(db, db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
        else:
            baseline = _lc_root_json(db, 'SELECT state FROM snapshots ORDER BY seq LIMIT 1')
        WorldStore._check_frozen_pathology(state, baseline)
        WorldStore._check_frozen_economy(state, baseline)
        WorldStore._check_frozen_military(state, baseline)
        WorldStore._check_frozen_p04(state, baseline)
        if cold_decode is None:
            WorldStore._lifecycle_check(db,state)
            from .coordinator_scheduler import _p04_native_current
            _p04_native_current(db, state)
        else:
            WorldStore._lifecycle_check(db,state,cold_decode=cold_decode)
            from .coordinator_scheduler import _p04_native_current
            _p04_native_current(db, state,cold_decode=cold_decode)
        if '_geography' in state:
            WorldStore._military_accounts(state)
            return  # Read original development saves without rewriting old snapshots.
        owner = _lifecycle_value_context.get()
        if owner is None:
            row = db.execute("SELECT value,sha256 FROM world_constants WHERE key='geography'").fetchone()
            if row is None or hashlib.sha256(row[0].encode('utf-8')).hexdigest() != row[1]:
                raise WorldError('SOURCE_DRIFT', '世界静态地理缺失或摘要不符')
            state['_geography'] = json.loads(row[0])
        elif _pristine_loan:
            # Only the fresh, proof-local snapshot caller consumes this loan.
            # Its frame, not this one, delimits the lifetime of the state alias.
            loan = _lc_geo_borrow(db, owner, _return_loan=True)
            state['_geography'] = loan.value
            WorldStore._military_accounts(state)
            return loan
        else:
            state['_geography'] = _lc_geo_borrow(db, owner)
        WorldStore._military_accounts(state)

    @staticmethod
    def _check_frozen_economy(state, baseline):
        # The first immutable public opt-in is authoritative, not two current
        # declarations that an operation could change together.
        WorldStore._check_frozen_projects(state, baseline)
        from .scenario import _validate_economy, economy_digest
        try:
            original = baseline['_config']
            current = state['_config']
            if 'economy' not in baseline:
                if ('economy' in state or original.get('schema_version') == 3
                        or _json(current.get('schema_version')) != _json(original.get('schema_version'))):
                    raise ValueError('legacy world cannot opt into economy after genesis')
                return
            if (type(original.get('schema_version')) is not int or original['schema_version'] != 3
                    or type(current.get('schema_version')) is not int or current['schema_version'] != 3):
                raise ValueError('economic schema identity changed')
            declaration = original['economy']
            _validate_economy(declaration, original['countries'])
            digest = economy_digest(declaration)
            if _json(current['economy']) != _json(declaration):
                raise ValueError('economic declaration changed')
            expected = {'rules_version': declaration['version'], 'rules_sha256': digest, 'frozen_at': 0,
                        'units': declaration['units'], 'recipes': declaration['recipes'],
                        'labor_capacity_hours_per_day': declaration['labor_hours_per_day']}
            for candidate in (baseline['economy'], state['economy']):
                if type(candidate) is not dict or type(candidate['frozen_at']) is not int:
                    raise ValueError('invalid economic frame')
                for key, value in expected.items():
                    if _json(candidate[key]) != _json(value):
                        raise ValueError('economic initial mirror changed')
                if _json(candidate['material_ledger']['initial']) != _json(declaration['initial_materials']):
                    raise ValueError('economic initial ledger changed')
        except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
            raise WorldError('FROZEN_ECONOMY', '开局经济规则、声明或初始镜像与首快照不符') from None

    @staticmethod
    def _check_frozen_pathology(state, baseline):
        # The first immutable snapshot, never a mutable current copy, is identity.
        # Compare canonical values so bool/float cannot masquerade as integer time.
        try:
            digest = pathology_digest(baseline['pathology'])
            for key in ('pathology_runtime', 'pathology_mechanism_sha256'):
                if ((key in state) != (key in baseline)
                        or (key in baseline and _json(state[key]) != _json(baseline[key]))):
                    raise ValueError('frozen runtime identity changed')
            if (('pathology_runtime' in state['_config']) != ('pathology_runtime' in baseline['_config'])
                    or ('pathology_runtime' in baseline['_config'] and
                        _json(state['_config']['pathology_runtime']) != _json(baseline['_config']['pathology_runtime']))):
                raise ValueError('frozen runtime declaration changed')
            # A legacy config may have unknown runtime-like keys. Only the
            # first public snapshot declaration opts into the new mechanism.
            if 'pathology_runtime' in baseline:
                joint = contact_mechanism_digest(baseline['pathology_runtime'], baseline['pathology'])
                if (baseline['_config'].get('pathology_runtime') != baseline['pathology_runtime']
                        or baseline['pathology_mechanism_sha256'] != joint):
                    raise ValueError('invalid initial runtime identity')
            if (_json(state['pathology']) != _json(baseline['pathology'])
                    or _json(state['_config']['pathology']) != _json(baseline['_config']['pathology'])
                    or _json(state['pathology_frozen_at']) != _json(baseline['pathology_frozen_at'])
                    or ('pathology_sha256' in state) != ('pathology_sha256' in baseline)
                    or ('pathology_sha256' in baseline and
                        (baseline['pathology_sha256'] != digest or state['pathology_sha256'] != digest))):
                raise ValueError('frozen identity changed')
        except (KeyError, TypeError, ValueError, RecursionError):
            raise WorldError('FROZEN_PATHOLOGY', '病理规则、配置副本、冻结时间或摘要与首快照不符') from None

    def _mutate(self, operation, *, execution=None, person_id=None, _precommit=None):
        # Arbitrary callbacks may return state/subaliases/opaque values or store
        # them externally. Never copy or declare their geography nonescaping.
        return self._lc_geo1133_mutate_body(operation, execution=execution,
            person_id=person_id, _precommit=_precommit)

    def _lc_geo1133_mutate_body(self, operation, *, execution=None, person_id=None, _precommit=None, _geo_direct=False):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self)
        from .coordinator_scheduler import _lifecycle_operation_scope, _lc_stale1141_Control
        with _lifecycle_operation_scope(mutation=True) as owner:
            from .coordinator_scheduler import source_lock
            attempt = self._scoped_attempt()
            if attempt is not None and execution is None:
                execution, person_id = attempt.permit, attempt.permit.person_id
            if not self.db_path.exists():
                raise WorldError('NOT_INITIALIZED', '世界尚未初始化')
            with source_lock(self), self._connection() as db:
                try:
                    db.execute('BEGIN IMMEDIATE')
                    state = _lc_root_json(db, 'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1')
                    self._check_coordinator_execution(db, state['world_id'], None)
                    if person_id is not None or execution is not None:
                        self._check_actor_execution(db, state['world_id'], person_id, execution)
                    geo_start = owner.memory.next_ticket
                    self._restore_geography(db, state)
                    if _geo_direct or (execution is person_id is _precommit is None and _lc_geo1187_adjudicator_ddl(self, operation, db, owner, state, geo_start)):
                        _lc_geo1133_reserve_dynamic(state, 1, 'root-mutation-direct-containers')
                    elif not _lc_geo1191_card_quote(self, operation, db, state, owner, execution, person_id, _precommit, geo_start):
                        _lc_reserve(state, 1, 'root-mutation-direct-containers')
                    before = _lc_canonical(self._dynamic_state(state))
                    baseline = _lc_root_json(db, 'SELECT state FROM snapshots ORDER BY seq LIMIT 1')
                    self._check_frozen_pathology(state, baseline)
                    self._check_frozen_p04(state, baseline)
                    self._check_frozen_economy(state, baseline)
                    mutation_token = _world_mutation_scope.set((self, db, execution))
                    try:
                        result = operation(state, db)
                    finally:
                        _world_mutation_scope.reset(mutation_token)
                    if type(result) is not _lc_stale1141_Control:
                        self._check_frozen_pathology(state, baseline)
                        self._check_frozen_p04(state, baseline)
                        self._check_frozen_economy(state, baseline)
                        self._check_frozen_military(state, baseline)
                        self._military_accounts(state)
                        if _lc_canonical(self._dynamic_state(state)) != before:
                            state['revision'] += 1
                            db.execute('INSERT INTO snapshots VALUES (?,?)', (state['revision'], self._encode_state(db, state)))
                        if person_id is not None or execution is not None:
                            self._check_actor_execution(db, state['world_id'], person_id, execution)
                            from .coordinator_scheduler import private_proof
                            private_proof(self, db, person_id, execution)
                        self._lifecycle_check(db,state)
                        if _precommit is not None and self._lifecycle_work(_precommit.job_id) is not _precommit:_lc_fail('root scope替换')
                        self._check_coordinator_execution(db, state['world_id'], None)
                        if _precommit is not None:_precommit(db)
                        db.commit()
                        return self._public(state) if result is None else result
                except BaseException:
                    db.rollback()
                    raise
                # Only the issued real commit callback may return this object.
                # Check before touching the transaction or detaching Work's borrow.
                if (_precommit is None or _precommit.stale1141 is not result
                        or _precommit.db is not db or _precommit.state is not state
                        or not db.in_transaction):
                    _lc_fail('非法继任重裁事务返回')
                # Fixed rollback/close slots use the existing mutation failure
                # carrier. Bind only this authenticated transaction's unwind.
                rollback_cleanup = [db, None, None]
                try:
                    db.rollback()
                except BaseException as error:
                    first = _lc_geo1137_tuple(error)
                    owner._remember_failure(first, rollback_cleanup)
                    raise
                if db.in_transaction:
                    _lc_fail('继任重裁回滚未结束')
                _precommit.db = None
                _precommit.state = None
                return result

    @staticmethod
    def _text(value, code='INVALID_BODY'):
        if type(value) is not str or not value.strip() or len(value) > 8000:
            raise WorldError(code, '必须为1至8000字的非空字符串')

    def _validate_body(self, body):
        if type(body) is not dict or type(body.get('action')) is not str:
            raise WorldError('INVALID_BODY', '意图必须为精确白名单对象')
        if body.get('action')=='propose' and set(body)=={'action','text','physical_attempt'}:
            from .actor_execution import valid_physical_attempt
            self._text(body['text'])
            if not valid_physical_attempt(body['physical_attempt']):raise WorldError('INVALID_BODY','非法物理意图锚')
            return
        fields = {'wait':set(), 'investigate':set(), 'send':{'to','text'}, 'travel':{'to'},
                  'ship':{'to','quantity'}, 'command':{'to','text'}, 'respond':{'command_id','accept'}, 'propose':{'text'}, 'meet':{'to','text'}}
        action = body['action']
        if action not in fields or set(body) != fields[action] | {'action'}:
            raise WorldError('INVALID_BODY', '非法意图字段')
        for key in fields[action] - {'quantity','accept'}:
            self._text(body[key])
        if action == 'ship' and (type(body['quantity']) is not int or body['quantity'] < 1):
            raise WorldError('INVALID_BODY', '粮数必须为正整数')
        if action == 'respond' and type(body['accept']) is not bool:
            raise WorldError('INVALID_BODY', 'accept必须为布尔值')

    def receipt(self, person_id, intent_id):
        self._text(person_id, 'UNKNOWN_PERSON')
        self._text(intent_id, 'INVALID_INTENT_ID')
        with self._connection(readonly=True) as db:
            row = db.execute('SELECT person,receipt FROM intents WHERE id=?', (intent_id,)).fetchone()
            if row and row[0] != person_id:
                raise WorldError('ID_PERSON_CONFLICT', '业务ID绑定其他人物')
            return json.loads(row[1]) if row and row[1] else None

    def preview(self, person_id, intent_id, body, ruling='成功', reason='脚本裁定器依据当前条件裁定', *, execution=None):
        return _lc_geo1133_stage(self._lc_geo1133_preview_body, (person_id,intent_id,body,ruling,reason), {'execution':execution})

    def _lc_geo1133_preview_body(self, person_id, intent_id, body, ruling='成功', reason='脚本裁定器依据当前条件裁定', *, execution=None):
        """Trusted DM-only rule simulation, never a committed receipt or actor input."""
        from .coordinator_scheduler import _lifecycle_operation_scope
        with _lifecycle_operation_scope():
            self._text(person_id, 'UNKNOWN_PERSON')
            self._text(intent_id, 'INVALID_INTENT_ID')
            self._validate_body(body)
            if type(ruling) is not str or ruling not in ('大失败','失败','成功','大成功'):
                raise WorldError('INVALID_RULING', '裁定必须为固定四档')
            self._text(reason, 'INVALID_REASON')
            payload = _lc_canonical({'body':body, 'ruling':ruling, 'reason':reason})
            body = _lc_json(payload)['body']
            if not self.db_path.exists():
                raise WorldError('NOT_INITIALIZED', '世界尚未初始化')
            from .coordinator_scheduler import source_lock
            with source_lock(self), self._connection(readonly=True) as db:
                db.execute('BEGIN')  # Pin one read snapshot for state, intent identity and geography.
                state = _lc_root_json(db, 'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1')
                self._check_actor_execution(db, state['world_id'], person_id, execution, readonly=True)
                row = _lc_root_row(db,'SELECT person,payload,receipt FROM intents WHERE id=?',(intent_id,),columns=('person','payload','receipt'))
                if row:
                    if row[0] != person_id or row[1] != payload:
                        raise WorldError('ID_PAYLOAD_CONFLICT', '同业务ID不可改变人物或载荷')
                    if row[2]:
                        raise WorldError('ALREADY_COMMITTED', '意图已提交，应读取原回执而非重复试算')
                self._restore_geography(db, state)
            if state['status'] != 'running':
                raise WorldError('WORLD_PAUSED', '世界没有运行')
            person = self._find(state['persons'], person_id, 'UNKNOWN_PERSON')
            base_revision = state['revision']
            _lc_geo1133_reserve_dynamic(state, 2, 'preview-direct-containers-and-public-result')
            effect = self._apply_action(state, person, intent_id, body, ruling)
            self._event(state, 'action', person['name']+'：'+body['action']+'；'+reason, person_id)
            state['revision'] += 1
            return {'status':'preview', 'world_id':state['world_id'], 'base_revision':base_revision,
                    'effect':effect, 'projected':self._public(state)}

    @staticmethod
    def _check_version(state, expected_version):
        if expected_version is not None:
            if state['world_id'] != expected_version['world_id']:
                raise WorldError('WORLD_MISMATCH', '试算不属于此世界')
            if state['revision'] != expected_version['revision']:
                raise WorldError('STALE_PREVIEW', '试算所据世界已变化，必须重新试算')

    def submit(self, person_id, intent_id, body, ruling='成功', reason='脚本裁定器依据当前条件裁定', *, expected_version=None, execution=None, activation=None):
        from .coordinator_scheduler import activation_scope
        with activation_scope(self, activation):
            return self._submit_bound(person_id,intent_id,body,ruling,reason,
                                      expected_version=expected_version,execution=execution)

    def _submit_bound(self, person_id, intent_id, body, ruling='成功', reason='脚本裁定器依据当前条件裁定', *, expected_version=None, execution=None):
        from .coordinator_scheduler import source_lock
        self._text(person_id, 'UNKNOWN_PERSON')
        self._text(intent_id, 'INVALID_INTENT_ID')
        self._validate_body(body)
        if type(ruling) is not str or ruling not in ('大失败','失败','成功','大成功'):
            raise WorldError('INVALID_RULING', '裁定必须为固定四档')
        self._text(reason, 'INVALID_REASON')
        if expected_version is not None:
            if (type(expected_version) is not dict or set(expected_version) != {'world_id', 'revision'}
                    or type(expected_version['world_id']) is not str or not expected_version['world_id'].strip()
                    or type(expected_version['revision']) is not int or expected_version['revision'] < 1):
                raise WorldError('INVALID_BASE_VERSION', '必须指定精确世界ID及正整数版本')
            expected_version = dict(expected_version)
        payload = _json({'body':body, 'ruling':ruling, 'reason':reason})
        body = json.loads(payload)['body']  # Effects must use the exact durable intent, not a caller-owned mutable dict.
        # Durable intent precedes effects. Replaying a known receipt is read-only.
        with source_lock(self), self._connection() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                self._check_actor_execution(db, state['world_id'], person_id, execution)
                self._check_scheduler_role(db, person_id, intent_id, execution)
                from .coordinator_scheduler import durable_body
                durable_body(self, db, state, person_id, intent_id, body, execution)
                row = db.execute('SELECT person,payload,receipt FROM intents WHERE id=?', (intent_id,)).fetchone()
                if row:
                    if row[0] != person_id or row[1] != payload:
                        raise WorldError('ID_PAYLOAD_CONFLICT', '同业务ID不可改变人物或载荷')
                    from .coordinator_scheduler import rejected_result
                    rejected = rejected_result(self,db,person_id,intent_id,execution)
                    if rejected is not None:
                        if expected_version is not None and expected_version['world_id'] != state['world_id']:
                            raise WorldError('WORLD_MISMATCH', '试算不属于此世界')
                        self._check_actor_execution(db,state['world_id'],person_id,execution)
                        durable_body(self,db,state,person_id,intent_id,body,execution)
                        db.commit()
                        return rejected
                    if row[2]:
                        receipt = json.loads(row[2])
                        if expected_version is not None and receipt['world_id'] != expected_version['world_id']:
                            raise WorldError('WORLD_MISMATCH', '试算不属于此世界')
                        self._check_actor_execution(db, state['world_id'], person_id, execution)
                        durable_body(self, db, state, person_id, intent_id, body, execution)
                        db.commit()
                        return receipt
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                baseline = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
                self._check_frozen_pathology(state, baseline)
                self._check_frozen_p04(state, baseline)
                self._find(state['persons'], person_id, 'UNKNOWN_PERSON')
                self._check_version(state, expected_version)
                if state['status'] != 'running':
                    raise WorldError('WORLD_PAUSED', '世界没有运行')
                if not row:
                    db.execute('INSERT INTO intents VALUES (?,?,?,NULL)', (intent_id, person_id, payload))
                self._check_actor_execution(db, state['world_id'], person_id, execution)
                durable_body(self, db, state, person_id, intent_id, body, execution)
                db.commit()
            except BaseException:
                db.rollback()
                raise
        def apply(state, db):
            self._check_actor_execution(db, state['world_id'], person_id, execution)
            from .coordinator_scheduler import durable_body
            durable_body(self, db, state, person_id, intent_id, body, execution)
            from .coordinator_scheduler import rejected_result
            rejected = rejected_result(self,db,person_id,intent_id,execution)
            if rejected is not None:
                return rejected
            row = db.execute('SELECT receipt FROM intents WHERE id=?', (intent_id,)).fetchone()
            if row[0]:
                return json.loads(row[0])
            self._check_version(state, expected_version)
            if state['status'] != 'running':
                raise WorldError('WORLD_PAUSED', '世界没有运行')
            person = self._find(state['persons'], person_id, 'UNKNOWN_PERSON')
            from .coordinator_scheduler import effect_or_reject
            effect = effect_or_reject(self, db, state, person_id, intent_id, execution,
                                     lambda tentative: self._apply_action(tentative, self._find(tentative['persons'],person_id,'UNKNOWN_PERSON'), intent_id, body, ruling))
            if effect.get('status') == 'domain_rejected':
                return effect
            receipt = {'world_id':state['world_id'], 'person_id':person_id, 'intent_id':intent_id,
                       'status':'committed', 'ruling':ruling, 'time':state['time'], 'effect':effect}
            self._event(state, 'action', person['name']+'：'+body['action']+'；'+reason, person_id)
            db.execute('UPDATE intents SET receipt=? WHERE id=?', (_json(receipt), intent_id))
            from .coordinator_scheduler import committed
            committed(self, db, state, person_id, intent_id, receipt, execution)
            return receipt
        if execution is None:
            # Preserve the legacy internal call shape (including existing
            # transaction interleaving adapters). apply still guards under
            # this transaction's write lock: new registration cannot bypass it.
            return self._mutate(apply)
        return self._mutate(apply, execution=execution, person_id=person_id)

    def _route_seconds(self, state, source, target):
        if source is None or target is None:
            raise WorldError('NOT_AT_LOCATION', '人物正在旅途')
        if source == target:
            return state['_config']['local_message_seconds']
        pending, best = [(0, source)], {source:0}
        while pending:
            distance, current = heapq.heappop(pending)
            if current == target:
                return distance
            if distance != best[current]:
                continue
            for edge in state['_geography'][current]['neighbors']:
                cost = distance+edge['seconds']
                if cost < best.get(edge['to'], float('inf')):
                    best[edge['to']] = cost
                    heapq.heappush(pending, (cost, edge['to']))
        raise WorldError('NO_ROUTE', '源邻接图无可达路径')

    def _arrive_contact(self, state, scheduled, arrival):
        if state.get('pathology_runtime') != CONTACT_RUNTIME or not scheduled.get('contact_packet_id'):
            return
        packet = self._find(state['contact_packets'], scheduled['contact_packet_id'], 'UNKNOWN_CONTACT_PACKET')
        place = self._find(state['places'], packet['destination_place_id'], 'UNKNOWN_PLACE')
        units = packet['contact_units']
        if place['disease']['restricted']:
            rules = state['pathology']
            units = units*rules['restricted_contacts']//max(rules['open_contacts'], 1)
        valid = packet['departed_at'] <= state['time'] < packet['expires_at']
        new = min(place['disease']['susceptible'], units//state['pathology']['contacts_per_infection']) if valid else 0
        cohort_id = None
        if new:
            self._infect(state, place, new)
            cohort = place['disease']['cohorts'][-1]
            cohort['contact_source'] = dict(packet_id=packet['id'], source_place_id=packet['source_place_id'],
                                            arrived_at=state['time'], cause_id=packet['cause_id'])
            cohort_id = cohort['id']
        application = dict(packet_id=packet['id'], arrived_at=state['time'], arrival_event_id=arrival['id'],
                           effective_contact_units=units, new_infections=new, cohort_id=cohort_id,
                           status='applied' if valid else 'expired')
        state.setdefault('contact_applications', []).append(application)
        fact = self._event(state, 'contact_exposure_applied', place['name']+'：实际跨区接触按固定虚构规则结算。',
                           cause_id=packet['cause_id'])
        fact.update(application, source_place_id=packet['source_place_id'], destination_place_id=place['id'],
                    mechanism_sha256=packet['mechanism_sha256'])

    def _depart_contact(self, state, channel, source_id, destination_id, cause_id):
        """Host-only departure facts; never infer personal infection or use text."""
        if state.get('pathology_runtime') != CONTACT_RUNTIME:
            return {}
        source = next((p for p in state['places'] if p['id'] == source_id), None)
        destination = next((p for p in state['places'] if p['id'] == destination_id), None)
        if source is None or destination is None or source_id == destination_id:
            return {}
        disease, rules = source['disease'], state['pathology']
        active = [c for c in disease['cohorts'] if c['stage'] in ('symptomatic','severe')]
        count = sum(c['count'] for c in active)
        if count <= 0:
            return {}
        packet = dict(id='contact-'+channel+'-'+cause_id, channel=channel,
                      source_place_id=source_id, destination_place_id=destination_id,
                      departed_at=state['time'], expires_at=state['time']+rules['exposure_lifetime_seconds'],
                      contact_units=rules['restricted_contacts' if disease['restricted'] else 'open_contacts'],
                      cause_id=cause_id, mechanism_sha256=state['pathology_mechanism_sha256'],
                      source_activity=dict(active_count=count, restricted=disease['restricted'],
                                           cohorts=json.loads(_json(active)), revision=state['revision']))
        state.setdefault('contact_packets', []).append(packet)
        return {'contact_packet_id':packet['id'], 'cause_id':cause_id}

    def _check_scheduler_role(self, db, person_id=None, intent_id=None, execution=None):
        from .coordinator_scheduler import activation_row, latest
        attempt = self._scoped_attempt()
        execution = attempt.permit if attempt else execution
        row = activation_row(self, db, latest(db), person_id, execution=execution, intent=intent_id)
        if row is not None and execution is None:
            raise WorldError('ACTOR_EXECUTION_REQUIRED', '人物必须持执行许可')

    def _action_location(self, state, person, action):
        if person['location'] is None:
            journeys=[q for q in state['queue'] if q['kind']=='arrival' and q.get('person_id')==person['id'] and q['at']>state['time']]
            if len(journeys)==1 and type(person.get('journey_version')) is int and person['journey_version']>0:
                self._find(state['places'], journeys[0]['destination'], 'UNKNOWN_PLACE')
                raise DomainActionError('NOT_AT_LOCATION', '人物正在旅途', action)
        return self._find(state['places'],person['location'],'NOT_AT_LOCATION')

    @staticmethod
    def _material_transport_body(body):
        """DM-only closed material effect, never an actor raw action."""
        import re
        if (type(body) is not dict or set(body) != {'action','to','resource_id','quantity'}
                or body['action'] != 'ship_material' or type(body['to']) is not str or not body['to'].strip()
                or type(body['resource_id']) is not str or body['resource_id'] == 'grain'
                or re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', body['resource_id']) is None
                or type(body['quantity']) is not int or not 1 <= body['quantity'] <= 2**63-1):
            raise WorldError('INVALID_MATERIAL_TRANSPORT', '非法材料运输字段或数量')

    @staticmethod
    def _material_transport_integer(value, minimum=0):
        if type(value) is not int or not minimum <= value <= 2**63-1:
            raise WorldError('INVALID_MATERIAL_TRANSPORT_STATE', '材料运输数量或时刻越界')
        return value

    def _material_transport_accounts(self, state, *, current_event=None):
        """Prove immutable transport facts independently of production accounts."""
        try:
            economy = state['economy']
            places = {p['id']:p for p in state['places']}
            resources = set(economy['units'])-{'grain'}
            out = {p:{r:0 for r in resources} for p in places}
            incoming = {p:{r:0 for r in resources} for p in places}
            pending = {p:{r:0 for r in resources} for p in places}
            transport = economy.get('transport')
            if 'transport' not in economy:
                if current_event is not None or any(q['kind']=='material_shipment' for q in state['queue']):
                    raise ValueError('node without transport')
                return out, incoming, pending
            if (type(transport) is not dict or set(transport) != {'version','transfers'}
                    or transport['version'] != 'p08-material-transport-v1' or type(transport['transfers']) is not list):
                raise ValueError('transport shape')
            keys = {'id','cause_id','owner_id','country_id','from','to','resource_id','quantity','unit',
                    'rules_sha256','generation','departed_at','duration','due','departure_event_id','node','status'}
            node_keys = {'id','seq','at','kind','transfer_id','generation','source','destination','resource_id',
                         'quantity','unit','rules_sha256','cause_id'}
            event_keys = {'id','time','kind','text','person_id','cause_id','transfer_id','source','destination',
                          'resource_id','quantity','unit','rules_sha256','generation','node_id'}
            ids = set(); node_ids = set(); matched_current = current_event is None
            for transfer in transport['transfers']:
                if type(transfer) is not dict:
                    raise ValueError('transfer shape')
                status = transfer.get('status')
                optional = {'contact_packet_id'} if 'contact_packet_id' in transfer else set()
                terminal = {'arrived_at','arrival_event_id'} if status == 'arrived' else set()
                if set(transfer) != keys|optional|terminal or status not in ('pending','arrived'):
                    raise ValueError('transfer keys/status')
                t = transfer; node = t['node']
                if (type(t['id']) is not str or t['id'] in ids or type(t['cause_id']) is not str or not t['cause_id']
                        or t['id'] != 'material-shipment-'+t['cause_id'] or type(t['generation']) is not int or t['generation'] != 1
                        or t['rules_sha256'] != economy['rules_sha256'] or t['resource_id'] not in resources
                        or t['unit'] != economy['units'][t['resource_id']] or t['from'] == t['to']
                        or t['from'] not in places or t['to'] not in places
                        or any(places[p]['country_id'] != t['country_id'] for p in (t['from'],t['to']))):
                    raise ValueError('transfer identity/binding')
                owner = self._find(state['persons'],t['owner_id'],'INVALID_MATERIAL_TRANSPORT_STATE')
                if owner['office'] != 'finance' or owner['country_id'] != t['country_id']:
                    raise ValueError('transfer owner')
                for field in ('quantity','duration','due'): self._material_transport_integer(t[field],1)
                self._material_transport_integer(t['departed_at'])
                if (t['departed_at'] > state['time'] or t['due'] != t['departed_at']+t['duration']
                        or t['duration'] != self._route_seconds(state,t['from'],t['to'])):
                    raise ValueError('route/time binding')
                if (type(node) is not dict or set(node) != node_keys|optional
                        or type(node['seq']) is not int or not 1 <= node['seq'] <= state['_queue_seq']
                        or node['id'] != 'q-'+str(node['seq']) or node['id'] in node_ids
                        or node['kind'] != 'material_shipment'):
                    raise ValueError('original node identity')
                expected = dict(transfer_id=t['id'],generation=t['generation'],source=t['from'],destination=t['to'],
                                resource_id=t['resource_id'],quantity=t['quantity'],unit=t['unit'],
                                rules_sha256=t['rules_sha256'],cause_id=t['cause_id'],at=t['due'])
                if optional: expected['contact_packet_id'] = t['contact_packet_id']
                if any(_json(node[k]) != _json(v) for k,v in expected.items()):
                    raise ValueError('node payload binding')
                relevant = [q for q in state['queue'] if q['kind']=='material_shipment'
                            and (q.get('transfer_id')==t['id'] or q['id']==node['id'] or q.get('cause_id')==t['cause_id'])]
                processing = current_event is not None and current_event.get('transfer_id') == t['id']
                if processing:
                    if _json(current_event) != _json(node):raise ValueError('popped node changed')
                    matched_current = True
                if status == 'pending':
                    if t['due'] < state['time'] or (processing and (relevant or state['time'] != t['due'])):
                        raise ValueError('pending clock/node')
                    if not processing and (len(relevant) != 1 or _json(relevant[0]) != _json(node)):
                        raise ValueError('pending original node missing/duplicated')
                    pending[t['from']][t['resource_id']] += t['quantity']
                elif relevant:
                    raise ValueError('arrived node remains queued')
                facts = dict(transfer_id=t['id'],source=t['from'],destination=t['to'],resource_id=t['resource_id'],
                             quantity=t['quantity'],unit=t['unit'],rules_sha256=t['rules_sha256'],generation=t['generation'],node_id=node['id'])
                starts = [e for e in state['events'] if e.get('cause_id')==t['cause_id'] and e['kind']=='material_shipment_departed']
                finishes = [e for e in state['events'] if e.get('cause_id')==t['cause_id'] and e['kind']=='material_shipment_arrived']
                for events,identity,at,person in [(starts,t['departure_event_id'],t['departed_at'],t['owner_id'])]+(
                        [(finishes,t['arrival_event_id'],t['due'],None)] if status=='arrived' else []):
                    if (len(events)!=1 or set(events[0]) != event_keys or events[0]['id'] != identity
                            or type(events[0]['time']) is not int or events[0]['time']!=at or events[0]['person_id']!=person
                            or any(_json(events[0][k]) != _json(v) for k,v in facts.items())):
                        raise ValueError('transport event binding')
                if status=='pending' and finishes:raise ValueError('pending arrival event')
                if status=='arrived':
                    self._material_transport_integer(t['arrived_at'],1)
                    if t['arrived_at']!=t['due'] or t['due']>state['time']:raise ValueError('arrived clock')
                    incoming[t['to']][t['resource_id']] += t['quantity']
                if optional:
                    packets = [p for p in state.get('contact_packets',[]) if p['id']==t['contact_packet_id'] or p['cause_id']==t['cause_id']]
                    if (state.get('pathology_runtime') != CONTACT_RUNTIME or len(packets)!=1
                            or packets[0]['id'] != 'contact-ship-'+t['cause_id'] or packets[0]['id'] != t['contact_packet_id']
                            or packets[0]['channel']!='ship' or packets[0]['source_place_id']!=t['from']
                            or packets[0]['destination_place_id']!=t['to'] or packets[0]['cause_id']!=t['cause_id']
                            or packets[0]['departed_at']!=t['departed_at']
                            or packets[0]['expires_at']!=t['departed_at']+state['pathology']['exposure_lifetime_seconds']
                            or packets[0]['mechanism_sha256']!=state['pathology_mechanism_sha256']):
                        raise ValueError('original contact binding')
                    applications = [a for a in state.get('contact_applications',[]) if a['packet_id']==t['contact_packet_id']]
                    if status=='pending' and applications:raise ValueError('premature contact')
                    if status=='arrived' and (len(applications)!=1 or applications[0]['arrived_at']!=t['due']
                            or applications[0]['arrival_event_id']!=t['arrival_event_id']):raise ValueError('arrival contact binding')
                elif any(p['cause_id']==t['cause_id'] for p in state.get('contact_packets',[])):
                    raise ValueError('unbound contact packet')
                out[t['from']][t['resource_id']] += t['quantity']
                ids.add(t['id']);node_ids.add(node['id'])
            if not matched_current or any(q['kind']=='material_shipment' and q['id'] not in node_ids for q in state['queue']):
                raise ValueError('orphan material node')
            return out, incoming, pending
        except (KeyError,TypeError,ValueError,OverflowError,RecursionError):
            raise WorldError('INVALID_MATERIAL_TRANSPORT_STATE','材料运输记录或原节点不一致') from None

    def _ship_material(self, state, person, intent_id, body, implemented):
        self._material_transport_body(body)
        economy = self._production_accounts(state)
        rid, quantity = body['resource_id'], body['quantity']
        if rid not in economy['units'] or rid == 'grain':
            raise WorldError('UNSUPPORTED','未声明的非粮物资')
        source = self._action_location(state,person,'ship_material')
        target = self._find(state['places'],body['to'],'UNKNOWN_PLACE')
        if (person['office'] != 'finance' or source['country_id'] != person['country_id']
                or target['country_id'] != person['country_id']
                or self._controller(state, source) != person['country_id']
                or self._controller(state, target) != person['country_id']):
            raise DomainActionError('NOT_AUTHORIZED','仅本国财政官可在本国实际地点调运本国材料','ship_material')
        if source['id'] == target['id']:raise WorldError('INVALID_TARGET','发收仓不得相同')
        if economy['materials'][source['id']][rid] < quantity:
            raise DomainActionError('INSUFFICIENT_MATERIALS','本地实际可支配材料不足','ship_material')
        duration = self._material_transport_integer(self._route_seconds(state,source['id'],target['id']),1)
        due = self._material_transport_integer(state['time']+duration,1)
        transfer_id = 'material-shipment-'+intent_id
        if any(t['id']==transfer_id for t in economy.get('transport',{}).get('transfers',[])):
            raise WorldError('INVALID_MATERIAL_TRANSPORT_STATE','运输业务ID重复')
        if not implemented:return dict(implemented=False)
        economy['materials'][source['id']][rid] -= quantity
        contact = self._depart_contact(state,'ship',source['id'],target['id'],intent_id)
        node = self._schedule(state,due,'material_shipment',transfer_id=transfer_id,generation=1,
                              source=source['id'],destination=target['id'],resource_id=rid,quantity=quantity,
                              unit=economy['units'][rid],rules_sha256=economy['rules_sha256'],cause_id=intent_id,
                              **{k:v for k,v in contact.items() if k!='cause_id'})
        event = self._event(state,'material_shipment_departed',source['name']+'：实际材料已出仓，在途尚未入目的仓。',person['id'],cause_id=intent_id)
        event.update(transfer_id=transfer_id,source=source['id'],destination=target['id'],resource_id=rid,
                     quantity=quantity,unit=economy['units'][rid],rules_sha256=economy['rules_sha256'],generation=1,node_id=node['id'])
        transfer = dict(id=transfer_id,cause_id=intent_id,owner_id=person['id'],country_id=person['country_id'],
                        **{'from':source['id'],'to':target['id']},resource_id=rid,quantity=quantity,unit=economy['units'][rid],
                        rules_sha256=economy['rules_sha256'],generation=1,departed_at=state['time'],duration=duration,due=due,
                        departure_event_id=event['id'],node=json.loads(_json(node)),status='pending',
                        **{k:v for k,v in contact.items() if k!='cause_id'})
        economy.setdefault('transport',dict(version='p08-material-transport-v1',transfers=[]))['transfers'].append(transfer)
        self._production_accounts(state)
        self._notice(state,person['id'],f"已出仓 {rid} {quantity} {economy['units'][rid]}，目前在途，目的仓尚未收到。",'发地出仓清点：'+source['name'])
        return dict(implemented=True,material_shipment_id=transfer_id,due=due)

    def _finish_material_transport(self, state, event):
        """Original clock's popped node, exact proof before settlement or retry."""
        if type(event) is not dict or event.get('kind') != 'material_shipment':
            raise WorldError('INVALID_MATERIAL_TRANSPORT_STATE','非法材料到达节点')
        economy = self._production_accounts(state, transport_event=event)
        transfer = self._find(economy['transport']['transfers'],event.get('transfer_id'),'INVALID_MATERIAL_TRANSPORT_STATE')
        # _material_transport_accounts already matched every original node field,
        # including cause/at/seq/id/qty/destination, even for arrived retries.
        if transfer['status'] == 'arrived':
            return
        if type(state['time']) is not int or state['time'] != transfer['due']:
            raise WorldError('INVALID_MATERIAL_TRANSPORT_STATE','材料尚未实际到达')
        place = self._find(state['places'],transfer['to'],'UNKNOWN_PLACE')
        available = economy['materials'][place['id']]
        rid, quantity = transfer['resource_id'], transfer['quantity']
        available[rid] = self._material_transport_integer(available[rid]+quantity)
        text = f"{rid} {quantity} {transfer['unit']} 实际到达并入仓。"
        arrival = self._event(state,'material_shipment_arrived',place['name']+'：'+text,cause_id=transfer['cause_id'])
        arrival.update(transfer_id=transfer['id'],source=transfer['from'],destination=transfer['to'],
                       resource_id=rid,quantity=quantity,unit=transfer['unit'],rules_sha256=transfer['rules_sha256'],
                       generation=transfer['generation'],node_id=event['id'])
        transfer.update(status='arrived',arrived_at=state['time'],arrival_event_id=arrival['id'])
        self._arrive_contact(state,event,arrival)
        self._production_accounts(state, transport_event=event)
        for person in state['persons']:
            if (person['country_id'] == transfer['country_id'] and person['location'] == place['id']
                    and self._controller(state, place) == person['country_id']):
                self._notice(state,person['id'],text,'亲见：'+place['name'])

    @staticmethod
    def _production_body(body):
        """DM-only closed effect; deliberately absent from actor _validate_body."""
        import re
        if (type(body) is not dict or set(body) != {'action', 'recipe_id', 'quantity'}
                or body['action'] != 'start_production'
                or type(body['recipe_id']) is not str
                or re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', body['recipe_id']) is None
                or type(body['quantity']) is not int or not 1 <= body['quantity'] <= 2**63-1):
            raise WorldError('INVALID_PRODUCTION', '非法生产效果字段或数量')

    @staticmethod
    def _production_integer(value, minimum=0):
        if type(value) is not int or not minimum <= value <= 2**63-1:
            raise WorldError('INVALID_PRODUCTION_STATE', '生产数量或时间越界')
        return value

    def _production_plan(self, state, recipe_id, quantity, started_at):
        self._production_integer(quantity, 1)
        self._production_integer(started_at)
        economy = state['economy']
        if recipe_id not in economy['recipes']:
            raise WorldError('UNSUPPORTED', '未声明的生产配方')
        recipe = economy['recipes'][recipe_id]
        inputs = {rid: self._production_integer(n*quantity, 1) for rid, n in recipe['inputs'].items()}
        outputs = {rid: self._production_integer(n*quantity, 1) for rid, n in recipe['outputs'].items()}
        hours = self._production_integer(recipe['labor_hours']*quantity, 1)
        duration = self._production_integer(recipe['duration_seconds']*quantity, 1)
        due = self._production_integer(started_at+duration, 1)
        return inputs, outputs, hours, due

    def _production_nodes(self, state, current_event=None):
        """Pending tasks require exactly one bound completion, queued or popped."""
        try:
            tasks = {t['id']: t for t in state['economy']['tasks']}
            counts = {key: 0 for key in tasks}
            keys = {'id','seq','at','kind','task_id','generation','place_id','rules_sha256','cause_id'}
            nodes = [q for q in state['queue'] if q['kind'] == 'production']
            if current_event is not None:
                nodes.append(current_event)
            seen = set()
            for node in nodes:
                if (type(node) is not dict or set(node) != keys or node['kind'] != 'production'
                        or type(node['seq']) is not int or not 1 <= node['seq'] <= state['_queue_seq']
                        or node['id'] != 'q-'+str(node['seq']) or node['id'] in seen):
                    raise ValueError('completion node identity or duplicate')
                seen.add(node['id'])
                task = tasks.get(node['task_id'])
                if task is None:
                    raise ValueError('orphan completion node')
                expected = dict(at=task['due'], generation=task['generation'], place_id=task['place_id'],
                                rules_sha256=task['rules_sha256'], cause_id=task['cause_id'])
                if any(_json(node[k]) != _json(v) for k,v in expected.items()):
                    raise ValueError('completion node payload')
                # A production identity may not collide with another queue kind.
                if any(q['kind'] != 'production' and (q['id'] == node['id'] or q['seq'] == node['seq'])
                       for q in state['queue']):
                    raise ValueError('completion queue identity collision')
                if node is current_event and state['time'] != task['due']:
                    raise ValueError('popped completion clock')
                counts[task['id']] += 1
            for key,task in tasks.items():
                if ((task['status'] == 'pending' and counts[key] != 1)
                        or (task['status'] == 'completed' and counts[key] > 1)):
                    raise ValueError('missing or duplicate task completion')
        except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
            raise WorldError('INVALID_PRODUCTION_STATE', '生产任务与到期节点不一致') from None

    def _production_accounts(self, state, *, transport_event=None, production_event=None, project_event=None):
        """Only production boundaries validate mutable accounts, never all _mutate."""
        try:
            if (type(state['_config'].get('schema_version')) is not int
                    or state['_config']['schema_version'] != 3 or 'economy' not in state):
                raise WorldError('UNSUPPORTED', '此世界未显式启用生产经济')
            economy = state['economy']; ledger = economy['material_ledger']
            places = {p['id']:p for p in state['places']}
            resources = set(economy['units'])-{'grain'}
            self._production_integer(state['time'])
            for book in (economy['materials'], *(ledger[k] for k in ('initial','produced','consumed','lost'))):
                if type(book) is not dict or set(book) != set(places):
                    raise ValueError('account places')
                for values in book.values():
                    if type(values) is not dict or set(values) != resources:
                        raise ValueError('account resources')
                    for value in values.values(): self._production_integer(value)
            if type(economy['tasks']) is not list:
                raise ValueError('tasks')
            held = {pid:{rid:0 for rid in resources} for pid in places}
            consumed = {pid:{rid:0 for rid in resources} for pid in places}
            produced = {pid:{rid:0 for rid in resources} for pid in places}
            ids = set()
            for task in economy['tasks']:
                if type(task) is not dict or type(task.get('id')) is not str or task['id'] in ids:
                    raise ValueError('task identity')
                ids.add(task['id'])
                if (type(task.get('cause_id')) is not str or not task['cause_id']
                        or task['id'] != 'production-'+task['cause_id']
                        or type(task.get('generation')) is not int or task['generation'] != 1
                        or task['rules_sha256'] != economy['rules_sha256']
                        or type(task.get('recipe_id')) is not str or task['recipe_id'] not in economy['recipes']
                        or task['place_id'] not in places or places[task['place_id']]['country_id'] != task['country_id']):
                    raise ValueError('task bindings')
                owner = self._find(state['persons'], task['owner_id'], 'INVALID_PRODUCTION_STATE')
                if owner['country_id'] != task['country_id'] or owner['office'] != 'finance':
                    raise ValueError('task owner')
                inputs, outputs, hours, due = self._production_plan(state,task['recipe_id'],task['quantity'],task['started_at'])
                self._production_integer(task['due'],1);self._production_integer(task['labor_hours'],1)
                if (task['started_at'] > state['time'] or task['due'] != due or task['labor_hours'] != hours
                        or _json(task['inputs']) != _json(inputs) or _json(task['expected_outputs']) != _json(outputs)):
                    raise ValueError('task plan')
                starts = [e for e in state['events'] if e.get('cause_id') == task['cause_id'] and e['kind'] == 'production_started']
                if (len(starts) != 1 or starts[0]['id'] != task['start_event_id']
                        or type(starts[0]['time']) is not int or starts[0]['time'] != task['started_at']
                        or starts[0]['person_id'] != task['owner_id']):
                    raise ValueError('start event binding')
                finishes = [e for e in state['events'] if e.get('cause_id') == task['cause_id'] and e['kind'] == 'production_completed']
                if task['status'] == 'pending':
                    if (_json(task['reserved_inputs']) != _json(inputs) or task['due'] < state['time']
                            or finishes or any(k in task for k in ('completed_at','actual_outputs','actual_labor_hours','completion_event_id'))):
                        raise ValueError('pending task reservation')
                    for rid,n in inputs.items(): held[task['place_id']][rid] += n
                elif task['status'] == 'completed':
                    self._production_integer(task['completed_at'],1)
                    self._production_integer(task['actual_labor_hours'],1)
                    if (task['reserved_inputs'] != {} or task['completed_at'] != due or due > state['time']
                            or task['actual_labor_hours'] != hours or _json(task['actual_outputs']) != _json(outputs)
                            or len(finishes) != 1 or finishes[0]['id'] != task['completion_event_id']
                            or type(finishes[0]['time']) is not int or finishes[0]['time'] != due
                            or finishes[0]['person_id'] != task['owner_id']):
                        raise ValueError('completed task binding')
                    for rid,n in inputs.items(): consumed[task['place_id']][rid] += n
                    for rid,n in outputs.items(): produced[task['place_id']][rid] += n
                else:
                    raise ValueError('task status')
            if 'projects' in state or project_event is not None:
                project_held, project_consumed, project_produced = self._project_accounts(state, project_event)
                for pid in places:
                    for rid in resources:
                        held[pid][rid] += project_held[pid][rid]
                        consumed[pid][rid] += project_consumed[pid][rid]
                        produced[pid][rid] += project_produced[pid][rid]
            self._production_nodes(state, current_event=production_event)
            dispatched, arrived, pending_cargo = self._material_transport_accounts(state, current_event=transport_event)
            for pid in places:
                for rid in resources:
                    if (ledger['consumed'][pid][rid] != consumed[pid][rid]
                            or ledger['produced'][pid][rid] != produced[pid][rid] or ledger['lost'][pid][rid] != 0):
                        raise ValueError('completion-derived books')
                    if (ledger['initial'][pid][rid]+ledger['produced'][pid][rid]
                            -ledger['consumed'][pid][rid]-ledger['lost'][pid][rid]
                            +arrived[pid][rid]-dispatched[pid][rid]
                            != economy['materials'][pid][rid]+held[pid][rid]):
                        raise ValueError('material balance')
            for rid in resources:
                if (sum(ledger['initial'][pid][rid]+produced[pid][rid]-consumed[pid][rid] for pid in places)
                        != sum(economy['materials'][pid][rid]+held[pid][rid]+pending_cargo[pid][rid] for pid in places)):
                    raise ValueError('global resource balance')
            return economy
        except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
            raise WorldError('INVALID_PRODUCTION_STATE', '生产任务或物资分账不一致') from None

    def _finish_production(self, state, event):
        """Trusted public clock delivery; settlement and event share _mutate."""
        economy = self._production_accounts(state, production_event=event)
        try:
            if (type(event) is not dict or set(event) != {'id','seq','at','kind','task_id','generation','place_id','rules_sha256','cause_id'}
                    or event['kind'] != 'production' or type(event['seq']) is not int
                    or not 1 <= event['seq'] <= state['_queue_seq'] or event['id'] != 'q-'+str(event['seq'])):
                raise ValueError('node identity')
            task = self._find(economy['tasks'],event['task_id'],'INVALID_PRODUCTION_STATE')
            if (type(event['generation']) is not int or event['generation'] != task['generation']
                    or event['place_id'] != task['place_id'] or event['rules_sha256'] != task['rules_sha256']
                    or event['cause_id'] != task['cause_id'] or type(event['at']) is not int
                    or event['at'] != task['due'] or state['time'] != event['at']):
                raise ValueError('node binding')
            if task['status'] == 'completed':
                return  # Only a fully matching, already-settled generation is a retry.
            for rid,n in task['inputs'].items():
                row=economy['material_ledger']['consumed'][task['place_id']]
                row[rid]=self._production_integer(row[rid]+n)
            for rid,n in task['expected_outputs'].items():
                available=economy['materials'][task['place_id']]
                produced=economy['material_ledger']['produced'][task['place_id']]
                available[rid]=self._production_integer(available[rid]+n)
                produced[rid]=self._production_integer(produced[rid]+n)
            completed=self._event(state,'production_completed','实际投入已结算，工班完成生产，产物实际入仓。',task['owner_id'],cause_id=task['cause_id'])
            task.update(status='completed',reserved_inputs={},actual_outputs=json.loads(_json(task['expected_outputs'])),
                        actual_labor_hours=task['labor_hours'],completed_at=state['time'],completion_event_id=completed['id'])
            self._production_accounts(state, production_event=event)
            owner = self._find(state['persons'], task['owner_id'], 'UNKNOWN_PERSON')
            if (owner['location'] == task['place_id']
                    and self._controller(state, self._find(state['places'], task['place_id'], 'UNKNOWN_PLACE')) == owner['country_id']):
                place = self._find(state['places'], task['place_id'], 'UNKNOWN_PLACE')
                outputs = '、'.join(f"{rid} {n} {economy['units'][rid]}" for rid,n in task['actual_outputs'].items())
                self._notice(state, owner['id'], '工班完成生产，实际产物已入仓：'+outputs+'。', '亲见：'+place['name'])
        except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
            raise WorldError('INVALID_PRODUCTION_STATE','生产到期节点不一致') from None

    def _production_labor(self, state, place_id, hours, due):
        # All legal starts are now, never future: pending rates can only fall
        # on [now,due), so its maximum is the left endpoint. Half-open intervals.
        now = state['time']; economy = state['economy']
        rate = Fraction(hours*DAY,due-now)
        rate += sum((Fraction(t['labor_hours']*DAY,t['due']-t['started_at'])
                     for t in economy['tasks'] if t['status']=='pending'
                     and t['place_id']==place_id and t['started_at'] <= now < t['due']),Fraction(0))
        if 'projects' in state:
            for task in state['projects']['tasks']:
                if task['place_id']==place_id and task['status']=='pending':
                    run=task['runs'][-1]
                    stage=state['projects']['rules']['projects'][task['project_id']]['stages'][run['index']]
                    if run['started_at'] <= now < run['due']:
                        rate += Fraction(stage['labor_hours']*DAY, run['due']-run['started_at'])
        if rate > economy['labor_capacity_hours_per_day'][place_id]:
            raise DomainActionError('INSUFFICIENT_LABOR', '本地同一时段工班容量已不足', 'start_production')

    def _start_production(self, state, person, intent_id, body, implemented):
        self._production_body(body)
        economy = self._production_accounts(state)
        inputs, outputs, hours, due = self._production_plan(state,body['recipe_id'],body['quantity'],state['time'])
        place = self._action_location(state,person,'start_production')
        if (person['office'] != 'finance' or person['country_id'] != place['country_id']
                or self._controller(state, place) != person['country_id']):
            raise DomainActionError('NOT_AUTHORIZED', '仅本国财政官可在本国实际地点组织工班', 'start_production')
        available = economy['materials'][place['id']]
        if any(available[rid] < n for rid,n in inputs.items()):
            raise DomainActionError('INSUFFICIENT_MATERIALS', '本地现存原料不足', 'start_production')
        self._production_labor(state,place['id'],hours,due)
        task_id = 'production-'+intent_id
        if any(t['id']==task_id for t in economy['tasks']):
            raise WorldError('INVALID_PRODUCTION_STATE', '生产业务ID重复')
        if not implemented:
            return {'implemented':False}
        task = dict(id=task_id,owner_id=person['id'],country_id=person['country_id'],place_id=place['id'],
                    rules_sha256=economy['rules_sha256'],recipe_id=body['recipe_id'],quantity=body['quantity'],
                    started_at=state['time'],due=due,generation=1,inputs=inputs,
                    reserved_inputs=json.loads(_json(inputs)),expected_outputs=outputs,
                    labor_hours=hours,status='pending',cause_id=intent_id)
        for rid,n in inputs.items(): available[rid] -= n
        economy['tasks'].append(task)
        self._schedule(state,due,'production',task_id=task_id,generation=1,place_id=place['id'],
                       rules_sha256=economy['rules_sha256'],cause_id=intent_id)
        started=self._event(state,'production_started',place['name']+'：实际原料已预留，工班开工，产物尚未形成。',person['id'],cause_id=intent_id)
        task['start_event_id']=started['id']
        self._production_accounts(state)
        return dict(implemented=True,production_id=task_id,due=due,reserved_inputs=json.loads(_json(inputs)),labor_hours=hours)

    @staticmethod
    def _project_body(body):
        fields = {'start_project':{'project_id'}, 'continue_project':{'project_task_id'},
                  'interrupt_project':{'project_task_id'}, 'share_project_knowledge':{'to','knowledge_id'}}
        if (type(body) is not dict or body.get('action') not in fields
                or set(body) != fields[body['action']] | {'action'}
                or any(type(body[k]) is not str or not body[k].strip() or len(body[k]) > 8000
                       for k in fields[body['action']])):
            raise WorldError('INVALID_PROJECT', '阶段项目效果须为精确白名单字段')

    def _initial_projects(self, state):
        rules = state['_config'].get('staged_projects')
        if rules is None:
            return
        from .scenario import economy_digest
        state['projects'] = dict(version=rules['version'], rules_sha256=economy_digest(rules),
                                 rules=json.loads(_json(rules)), tasks=[], knowledge=[])
        for index, source in enumerate(rules['initial_knowledge']):
            grant = dict(id='project-genesis-'+str(index), person_id=source['person_id'],
                         knowledge_id=source['knowledge_id'], time=0, kind='scenario', source=str(index))
            state['projects']['knowledge'].append(grant)
            self._notice(state, grant['person_id'], rules['knowledge'][grant['knowledge_id']], source['source'])

    @staticmethod
    def _check_frozen_projects(state, baseline):
        from .scenario import _validate_projects, economy_digest
        try:
            original = baseline['_config'].get('staged_projects')
            if original is None:
                if 'projects' in state or 'staged_projects' in state['_config']:
                    raise ValueError('late project opt-in')
                return
            _validate_projects(original, baseline['_config']['countries'], baseline['_config']['economy'])
            for candidate in (baseline,state):
                project = candidate['projects']
                if (set(project) != {'version','rules_sha256','rules','tasks','knowledge'}
                        or project['version'] != original['version']
                        or project['rules_sha256'] != economy_digest(original)
                        or _json(project['rules']) != _json(original)
                        or _json(candidate['_config']['staged_projects']) != _json(original)):
                    raise ValueError('project rules changed')
        except (KeyError,TypeError,ValueError,OverflowError,RecursionError):
            raise WorldError('FROZEN_PROJECTS','阶段项目声明与不可变首快照不符') from None

    def _project_accounts(self, state, current_event=None):
        """Derive project contributions; same material books as production/transport.

        Inputs burn on stage entry, tools are exclusive physical holdings until
        terminal settlement, later stages hold nothing. No future output counts.
        This validates facts, not a second world store or an authority shortcut.
        """
        resources = set(state['economy']['units']) - {'grain'}
        def zeros():
            return {p['id']:{r:0 for r in resources} for p in state['places']}
        held, consumed, produced = zeros(), zeros(), zeros()
        if 'projects' not in state:
            if current_event is not None or any(q['kind']=='project_stage' for q in state['queue']):
                raise WorldError('INVALID_PROJECT_STATE','未启用项目却存在节点')
            return held, consumed, produced
        try:
            data=state['projects']; rules=data['rules']; ids=set(); causes=set(); node_ids=set()
            expected_grants=[]
            for i, source in enumerate(rules['initial_knowledge']):
                expected_grants.append(dict(id='project-genesis-'+str(i),person_id=source['person_id'],
                    knowledge_id=source['knowledge_id'],time=0,kind='scenario',source=str(i)))
            if type(data['tasks']) is not list or type(data['knowledge']) is not list:
                raise ValueError('project collections')
            grants={g['id']:g for g in data['knowledge']}
            if len(grants)!=len(data['knowledge']):raise ValueError('duplicate knowledge source')
            for task in data['tasks']:
                if (type(task) is not dict or set(task) != {'id','project_id','owner_id','country_id','place_id','runs','status'}
                        or task['id'] in ids or task['project_id'] not in rules['projects']
                        or type(task['runs']) is not list or not task['runs']):
                    raise ValueError('project identity')
                ids.add(task['id'])
                owner=self._find(state['persons'],task['owner_id'],'INVALID_PROJECT_STATE')
                place=self._find(state['places'],task['place_id'],'INVALID_PROJECT_STATE')
                if owner['office']!='finance' or owner['country_id']!=task['country_id'] or place['country_id']!=task['country_id']:
                    raise ValueError('project original ownership')
                stages=rules['projects'][task['project_id']]['stages']
                if len(task['runs'])>len(stages):raise ValueError('too many stages')
                previous=None
                for index, run in enumerate(task['runs']):
                    stage=stages[index]; terminal=run['status']!='pending'
                    keys={'index','cause_id','started_at','due','ruling','journey_version','knowledge_sources','node','status','start_event_id'}
                    if terminal:keys|={'ended_at','reason','end_event_id','labor_elapsed_seconds','actual_labor_hours'}
                    if (set(run)!=keys or type(run['index']) is not int or run['index']!=index
                            or type(run['cause_id']) is not str or not run['cause_id'] or run['cause_id'] in causes
                            or run['ruling'] not in ('成功','大成功','失败','大失败')
                            or type(run['journey_version']) is not int or run['journey_version']<0):
                        raise ValueError('stage shape/identity')
                    causes.add(run['cause_id'])
                    if index==0 and task['id']!='project-'+run['cause_id']:raise ValueError('original project ID')
                    for k in ('started_at','due'):self._production_integer(run[k])
                    if (run['started_at']>state['time'] or run['due']!=run['started_at']+stage['duration_seconds']
                            or (previous is not None and (previous['status']!='completed' or previous['ended_at']>run['started_at']))):
                        raise ValueError('stage order/time')
                    if set(run['knowledge_sources'])!=set(stage['requires']):raise ValueError('required sources')
                    for kid,gid in run['knowledge_sources'].items():
                        grant=grants[gid]
                        if grant['knowledge_id']!=kid or grant['person_id']!=owner['id'] or grant['time']>run['started_at']:
                            raise ValueError('unacquired knowledge')
                    node=run['node']
                    if (set(node)!={'id','seq','at','kind','project_task_id','stage_index','cause_id','rules_sha256'}
                            or type(node['seq']) is not int or not 1<=node['seq']<=state['_queue_seq']
                            or node['id']!='q-'+str(node['seq']) or node['id'] in node_ids
                            or node['kind']!='project_stage' or node['project_task_id']!=task['id']
                            or type(node['at']) is not int
                            or any(q['kind']!='project_stage' and (q['id']==node['id'] or q['seq']==node['seq']) for q in state['queue'])
                            or type(node['stage_index']) is not int or node['stage_index']!=index
                            or node['at']!=run['due'] or node['cause_id']!=run['cause_id']
                            or node['rules_sha256']!=data['rules_sha256']):
                        raise ValueError('stage original node')
                    node_ids.add(node['id'])
                    queued=[q for q in state['queue'] if q['id']==node['id'] or
                            (q['kind']=='project_stage' and q.get('cause_id')==run['cause_id'])]
                    processing=current_event is not None and current_event.get('id')==node['id']
                    if processing and _json(current_event)!=_json(node):raise ValueError('popped node payload')
                    if run['status']=='pending':
                        if index!=len(task['runs'])-1 or run['due']<state['time']:
                            raise ValueError('pending stage order/clock')
                        if processing:
                            if queued or state['time']!=run['due']:raise ValueError('popped node time')
                        elif len(queued)!=1 or _json(queued[0])!=_json(node):raise ValueError('missing original future')
                        for rid,n in stage['tools'].items():held[place['id']][rid]+=n
                    elif run['status'] not in ('completed','failed','interrupted') or queued:
                        raise ValueError('terminal stage/node')
                    starts=[e for e in state['events'] if e['kind']=='project_stage_started' and e.get('cause_id')==run['cause_id']]
                    if (len(starts)!=1 or starts[0]['id']!=run['start_event_id'] or starts[0]['time']!=run['started_at']
                            or starts[0]['person_id']!=owner['id']):raise ValueError('stage start source')
                    ends=[e for e in state['events'] if e['kind'] in ('project_stage_completed','project_stage_failed','project_stage_interrupted')
                          and e.get('cause_id')==run['cause_id']]
                    if terminal:
                        self._production_integer(run['ended_at'])
                        if (not run['started_at']<=run['ended_at']<=min(state['time'],run['due'])
                                or type(run['labor_elapsed_seconds']) is not int
                                or run['labor_elapsed_seconds']!=run['ended_at']-run['started_at']
                                or _json(run['actual_labor_hours'])!=_json(dict(
                                    numerator=Fraction(stage['labor_hours']*run['labor_elapsed_seconds'],stage['duration_seconds']).numerator,
                                    denominator=Fraction(stage['labor_hours']*run['labor_elapsed_seconds'],stage['duration_seconds']).denominator))
                                or type(run['reason']) is not str or not run['reason']
                                or len(ends)!=1 or ends[0]['id']!=run['end_event_id']
                                or ends[0]['kind']!='project_stage_'+run['status'] or ends[0]['time']!=run['ended_at']
                                or ends[0]['person_id']!=owner['id']):raise ValueError('stage terminal source')
                        if run['status'] in ('completed','failed') and run['ended_at']!=run['due']:
                            raise ValueError('before due settlement')
                        if (run['status']=='completed') != (run['ruling'] in ('成功','大成功')) and run['status']!='interrupted':
                            raise ValueError('ruling settlement')
                    elif ends:raise ValueError('premature terminal')
                    for rid,n in stage['inputs'].items():consumed[place['id']][rid]+=n
                    if run['status']=='completed':
                        for rid,n in stage['outputs'].items():produced[place['id']][rid]+=n
                        for kid in stage['knowledge']:
                            expected_grants.append(dict(id='project-knowledge-'+run['end_event_id']+'-'+kid,
                                person_id=owner['id'],knowledge_id=kid,time=run['ended_at'],kind='stage',source=run['end_event_id']))
                    previous=run
                final=task['runs'][-1]['status']
                expected_status=('completed' if len(task['runs'])==len(stages) else 'ready') if final=='completed' else final
                if task['status']!=expected_status:raise ValueError('project status')
            if any(q['kind']=='project_stage' and q['id'] not in node_ids for q in state['queue']):raise ValueError('orphan node')
            if current_event is not None and current_event['id'] not in node_ids:raise ValueError('orphan popped node')
            # Only actual delivered courier messages can create transferred facts.
            for pid,messages in state['_messages'].items():
                for message in messages:
                    source_id=message.get('project_knowledge_source')
                    if source_id is None:continue
                    source=grants[source_id]
                    if source['person_id']!=message['from'] or source['time']>message['time']:
                        raise ValueError('courier did not own source')
                    expected_grants.append(dict(id='project-message-'+message['id'],person_id=pid,
                        knowledge_id=source['knowledge_id'],time=message['time'],kind='message',source=message['id']))
            expected={g['id']:g for g in expected_grants}
            if len(expected)!=len(expected_grants) or _json(grants)!=_json(expected):raise ValueError('knowledge provenance')
            rooted={g['id'] for g in expected_grants if g['kind'] in ('scenario','stage')}
            links={}
            for messages in state['_messages'].values():
                for message in messages:
                    if 'project_knowledge_source' in message:
                        links['project-message-'+message['id']]=message['project_knowledge_source']
            unresolved=dict(links)
            while unresolved:
                ready=[gid for gid,source_id in unresolved.items() if source_id in rooted]
                if not ready:raise ValueError('cyclic or unrooted courier knowledge')
                for gid in ready:rooted.add(gid);del unresolved[gid]
            return held,consumed,produced
        except (KeyError,TypeError,ValueError,OverflowError,RecursionError):
            raise WorldError('INVALID_PROJECT_STATE','阶段项目投入、原节点、成果或合法知识来源不一致') from None

    def _project_grant(self, state, person_id, knowledge_id, at, kind, source, grant_id):
        if not _lc_alive(state,person_id):return
        grant=dict(id=grant_id,person_id=person_id,knowledge_id=knowledge_id,time=at,kind=kind,source=source)
        existing=next((g for g in state['projects']['knowledge'] if g['id']==grant_id),None)
        if existing is not None:
            if existing!=grant:raise WorldError('INVALID_PROJECT_STATE','知识来源ID冲突')
            return
        state['projects']['knowledge'].append(grant)
        self._notice(state,person_id,state['projects']['rules']['knowledge'][knowledge_id],'实际项目知识来源：'+kind)

    def _project_receive(self, state, message, person_id):
        source_id=message.get('project_knowledge_source')
        if source_id is None:return
        source=self._find(state['projects']['knowledge'],source_id,'INVALID_PROJECT_STATE')
        if source['person_id']!=message['from']:raise WorldError('INVALID_PROJECT_STATE','信使知识来源不属于发信者')
        self._project_grant(state,person_id,source['knowledge_id'],state['time'],'message',message['id'],
                            'project-message-'+message['id'])

    def _project_stage_start(self, state, person, task, intent_id, ruling, action):
        data=state['projects']; index=len(task['runs']); stage=data['rules']['projects'][task['project_id']]['stages'][index]
        place=self._action_location(state,person,action)
        if (person['office']!='finance' or task['owner_id']!=person['id'] or task['place_id']!=place['id']
                or place['country_id']!=person['country_id'] or self._controller(state,place)!=person['country_id']
                or place['disease']['restricted']):
            raise DomainActionError('NOT_AUTHORIZED','须由原本国财政官在受控且可组织工班的现场申请',action)
        sources={}
        for kid in stage['requires']:
            grant=next((g for g in reversed(data['knowledge']) if g['person_id']==person['id'] and g['knowledge_id']==kid),None)
            if grant is None:raise DomainActionError('KNOWLEDGE_REQUIRED','本人尚未通过合法来源获得必要工艺',action)
            sources[kid]=grant['id']
        stock=state['economy']['materials'][place['id']]
        if any(stock[r]<n for r,n in stage['inputs'].items()):
            raise DomainActionError('INSUFFICIENT_MATERIALS','现场实际材料不足',action)
        if any(stock[r]<n for r,n in stage['tools'].items()):
            raise DomainActionError('INSUFFICIENT_TOOLS','现场工具已占用或不足',action)
        due=self._production_integer(state['time']+stage['duration_seconds'],1)
        try:self._production_labor(state,place['id'],stage['labor_hours'],due)
        except DomainActionError as error:
            raise DomainActionError(error.code,str(error),action) from None
        node=self._schedule(state,due,'project_stage',project_task_id=task['id'],stage_index=index,
                            cause_id=intent_id,rules_sha256=data['rules_sha256'])
        event=self._event(state,'project_stage_started','阶段开工，材料已消耗，工具占用；成果尚不存在。',person['id'],cause_id=intent_id)
        run=dict(index=index,cause_id=intent_id,started_at=state['time'],due=due,ruling=ruling,
                 journey_version=person.get('journey_version',0),knowledge_sources=sources,node=json.loads(_json(node)),
                 status='pending',start_event_id=event['id'])
        for rid,n in stage['inputs'].items():
            stock[rid]-=n
            book=state['economy']['material_ledger']['consumed'][place['id']]
            book[rid]=self._production_integer(book[rid]+n)
        for rid,n in stage['tools'].items():stock[rid]-=n
        task['runs'].append(run);task['status']='pending'
        return dict(implemented=True,project_task_id=task['id'],stage_index=index,due=due,
                    consumed_inputs=json.loads(_json(stage['inputs'])),held_tools=json.loads(_json(stage['tools'])),
                    labor_hours=stage['labor_hours'])

    def _project_end(self, state, task, status, reason):
        run=task['runs'][-1]
        if run['status']!='pending':return
        stage=state['projects']['rules']['projects'][task['project_id']]['stages'][run['index']]
        stock=state['economy']['materials'][task['place_id']]
        for rid,n in stage['tools'].items():stock[rid]=self._production_integer(stock[rid]+n)
        event=self._event(state,'project_stage_'+status,'项目阶段'+status+'：'+reason,task['owner_id'],cause_id=run['cause_id'])
        elapsed=state['time']-run['started_at']
        actual=Fraction(stage['labor_hours']*elapsed,stage['duration_seconds'])
        run.update(status=status,ended_at=state['time'],reason=reason,end_event_id=event['id'],
                   labor_elapsed_seconds=elapsed,actual_labor_hours=dict(numerator=actual.numerator,denominator=actual.denominator))
        task['status']=status
        if status=='completed':
            for rid,n in stage['outputs'].items():
                stock[rid]=self._production_integer(stock[rid]+n)
                row=state['economy']['material_ledger']['produced'][task['place_id']]
                row[rid]=self._production_integer(row[rid]+n)
            for kid in stage['knowledge']:
                self._project_grant(state,task['owner_id'],kid,state['time'],'stage',event['id'],
                                    'project-knowledge-'+event['id']+'-'+kid)
            task['status']='completed' if len(task['runs'])==len(state['projects']['rules']['projects'][task['project_id']]['stages']) else 'ready'
        self._notice(state,task['owner_id'],'本人项目阶段'+{'completed':'完成','failed':'失败','interrupted':'中断'}[status]+'；'+{
            'completed':'本段成果实际形成，后段仍须本人决定是否申请。',
            'failed':'本段试验未形成成果，已投入材料与实际劳动不返还。',
            'interrupted':'现场工作已中断，材料投入保留，工具已解除占用。'}[status],'本人项目现场记录')

    def _finish_project(self, state, event):
        self._production_accounts(state,project_event=event)
        task=self._find(state['projects']['tasks'],event['project_task_id'],'INVALID_PROJECT_STATE')
        run=task['runs'][event['stage_index']]
        if _json(event)!=_json(run['node']):raise WorldError('INVALID_PROJECT_STATE','原阶段节点不符')
        if run['status']!='pending':return
        if state['time']!=run['due']:raise WorldError('INVALID_PROJECT_STATE','阶段不能提前结算')
        owner=self._find(state['persons'],task['owner_id'],'UNKNOWN_PERSON')
        place=self._find(state['places'],task['place_id'],'UNKNOWN_PLACE')
        if (owner['location']!=place['id'] or owner.get('journey_version',0)!=run['journey_version']
                or self._controller(state,place)!=task['country_id'] or place['disease']['restricted']):
            self._project_end(state,task,'interrupted','现场组织者、通行或本地控制条件已改变；投入不退，未形成成果。')
        elif run['ruling'] in ('失败','大失败'):
            self._project_end(state,task,'failed','原DM裁定的实验失败实际到期；投入保留，无知识或成品。')
        else:self._project_end(state,task,'completed','原阶段实际到期；本段成果已形成，后段须另行申请。')
        self._production_accounts(state,project_event=event)

    def _project_action(self, state, person, intent_id, body, ruling):
        self._project_body(body)
        if 'projects' not in state:raise WorldError('UNSUPPORTED','本局未显式启用阶段项目')
        self._production_accounts(state)
        action=body['action'];data=state['projects']
        if action=='share_project_knowledge':
            grant=next((g for g in reversed(data['knowledge']) if g['person_id']==person['id'] and g['knowledge_id']==body['knowledge_id']),None)
            if grant is None:raise DomainActionError('KNOWLEDGE_REQUIRED','不能传授本人未获得的成果',action)
            try:
                result=self._apply_action(state,person,intent_id,dict(action='send',to=body['to'],
                    text=data['rules']['knowledge'][body['knowledge_id']]),ruling)
            except DomainActionError as error:raise DomainActionError(error.code,str(error),action) from None
            if result['implemented']:
                node=next(q for q in state['queue'] if q.get('message_id')==result['message_id'])
                node['project_knowledge_source']=grant['id']
            return result
        if action=='start_project':
            if body['project_id'] not in data['rules']['projects']:raise WorldError('UNSUPPORTED','未声明项目')
            task=dict(id='project-'+intent_id,project_id=body['project_id'],owner_id=person['id'],
                country_id=person['country_id'],place_id=person['location'],runs=[],status='pending')
            if any(t['id']==task['id'] for t in data['tasks']):raise WorldError('INVALID_PROJECT_STATE','项目业务ID重复')
            result=self._project_stage_start(state,person,task,intent_id,ruling,action)
            data['tasks'].append(task)
        else:
            task=self._find(data['tasks'],body['project_task_id'],'INVALID_PROJECT')
            if person['id']!=task['owner_id']:raise DomainActionError('NOT_AUTHORIZED','仅原组织者可申请后段或中断',action)
            if action=='interrupt_project':
                place=self._action_location(state,person,action)
                if place['id']!=task['place_id']:
                    raise DomainActionError('NOT_AT_LOCATION','显式中断须原组织者实际到场，不能远程瞬时调拨工具',action)
                if self._controller(state,place)!=task['country_id']:
                    raise DomainActionError('NOT_AUTHORIZED','失去实际控制不能调拨现场工具',action)
                if task['status']!='pending':raise DomainActionError('PROJECT_NOT_READY','没有进行中的阶段',action)
                if ruling not in ('成功','大成功'):return dict(implemented=False)
                node=task['runs'][-1]['node']
                state['queue']=[q for q in state['queue'] if q['id']!=node['id']]
                self._project_end(state,task,'interrupted','原组织者显式中断，来源意图：'+intent_id)
                result=dict(implemented=True,project_task_id=task['id'],status='interrupted')
            else:
                if task['status']!='ready':raise DomainActionError('PROJECT_NOT_READY','前段未完成或项目已终止',action)
                result=self._project_stage_start(state,person,task,intent_id,ruling,action)
        self._production_accounts(state)
        return result

    def _project_conditions(self, state, source):
        """Immediate forward interruption at existing control/restriction producers."""
        if 'projects' not in state:return
        for task in state['projects']['tasks']:
            if task['status']!='pending':continue
            run=task['runs'][-1]
            owner=self._find(state['persons'],task['owner_id'],'UNKNOWN_PERSON')
            place=self._find(state['places'],task['place_id'],'UNKNOWN_PLACE')
            if (owner['location']!=place['id'] or owner.get('journey_version',0)!=run['journey_version']
                    or self._controller(state,place)!=task['country_id'] or place['disease']['restricted']):
                state['queue']=[q for q in state['queue'] if q['id']!=run['node']['id']]
                self._project_end(state,task,'interrupted','实际现场条件改变，来源：'+source)

    def _project_departure(self, state, person, intent_id):
        if 'projects' not in state:return
        for task in state['projects']['tasks']:
            if task['owner_id']==person['id'] and task['status']=='pending':
                node=task['runs'][-1]['node']
                state['queue']=[q for q in state['queue'] if q['id']!=node['id']]
                self._project_end(state,task,'interrupted','组织者实际出发，来源意图：'+intent_id)

    @staticmethod
    def _controller(state, place):
        """Military occupation grants local store access, not legal annexation."""
        return state['military']['control'][place['id']] if 'military' in state else place['country_id']

    @staticmethod
    def _military_body(body):
        fields = {'military_move': {'army_id', 'to'}, 'military_supply': {'army_id', 'quantity'},
                  'military_garrison': {'army_id'}, 'military_engage': {'army_id', 'losses'}}
        if (type(body) is not dict or type(body.get('action')) is not str
                or body['action'] not in fields or set(body) != fields[body['action']] | {'action'}):
            raise WorldError('INVALID_BODY', '军事效果必须使用精确版本字段')
        for key in fields[body['action']] - {'quantity', 'losses'}:
            WorldStore._text(body[key])
        if 'quantity' in body and (type(body['quantity']) is not int or not 1 <= body['quantity'] <= 2**63-1):
            raise WorldError('INVALID_BODY', '补给必须为有界正整数')
        if 'losses' in body and (type(body['losses']) is not dict or not 2 <= len(body['losses']) <= 64
                or any(type(k) is not str or not k or type(v) is not int or not 0 <= v <= 2**63-1
                       for k, v in body['losses'].items())):
            raise WorldError('INVALID_BODY', '战损必须是各实际参战军队的有界非负整数')

    def _initial_military(self, state):
        declaration = state['_config'].get('military')
        if declaration is None:
            return
        # Initial grain is transferred from declared civilian warehouses, never added.
        military = dict(version=declaration['version'], rules=json.loads(_json(declaration)),
                        control={p['id']: p['country_id'] for p in state['places']}, armies=[], records=[])
        state['military'] = military
        for initial in declaration['armies']:
            place = self._find(state['places'], initial['location'], 'INVALID_MILITARY_STATE')
            place['grain'] -= initial['grain']
            military['armies'].append(dict(**initial, stance='field', journey=None))
        self._schedule(state, DAY, 'military_rations')

    @staticmethod
    def _check_frozen_military(state, baseline):
        """Genesis opt-in/rules are immutable; no army injection into legacy saves."""
        try:
            declared = baseline['_config'].get('military')
            if _json(state['_config'].get('military')) != _json(declared):
                raise ValueError('changed declaration')
            if declared is None:
                if 'military' in state or 'military' in baseline:
                    raise ValueError('late opt-in')
                return
            from .scenario import _validate_military
            _validate_military(declared, baseline['_config']['countries'])
            for candidate in (state, baseline):
                if (_json(candidate['military']['rules']) != _json(declared)
                        or candidate['military']['version'] != declared['version']):
                    raise ValueError('rules mirror')
        except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
            raise WorldError('FROZEN_MILITARY', '军事开局声明或规则镜像与首快照不符') from None

    @staticmethod
    def _military_accounts(state, current_event=None):
        """Validate troop/grain provenance, control history and exact active nodes."""
        if 'military' not in state:
            if any(q['kind'].startswith('military_') for q in state['queue']):
                raise WorldError('INVALID_MILITARY_STATE', '未启用军事却有军事节点')
            return None
        try:
            m = state['military']; rules = m['rules']
            places = {p['id']: p for p in state['places']}
            countries = {c['id'] for c in state['countries']}
            initial = {a['id']: a for a in rules['armies']}
            armies = {a['id']: a for a in m['armies']}
            if len(armies) != len(m['armies']) or set(armies) != set(initial):
                raise ValueError('army roster')
            balance = {aid: [a['strength'], a['grain']] for aid, a in initial.items()}
            control = {p['id']: p['country_id'] for p in state['places']}
            seen = set()
            events = {e['id']: e for e in state['events']}
            for record in m['records']:
                if (type(record) is not dict or set(record) != {'event_id', 'army_id', 'kind', 'troops', 'grain', 'place_id', 'controller'}
                        or record['event_id'] in seen or record['event_id'] not in events
                        or record['army_id'] not in armies or record['place_id'] not in places):
                    raise ValueError('record identity')
                seen.add(record['event_id'])
                event = events[record['event_id']]
                if (event['kind'] != 'military_'+record['kind'] or event['military_record'] != record
                        or type(event['time']) is not int or not 0 <= event['time'] <= state['time']):
                    raise ValueError('event binding')
                for value in (record['troops'], record['grain']):
                    if type(value) is not int or not 0 <= value <= 2**63-1:
                        raise ValueError('record amount')
                kind = record['kind']; aid = record['army_id']
                if kind == 'supply':
                    if record['troops'] or record['controller'] is not None:
                        raise ValueError('supply shape')
                    balance[aid][1] += record['grain']
                elif kind in ('rations', 'battle'):
                    if record['controller'] is not None:
                        raise ValueError('loss shape')
                    balance[aid][0] -= record['troops']; balance[aid][1] -= record['grain']
                elif kind == 'control':
                    if (record['troops'] or record['grain'] or record['controller'] not in countries
                            or record['controller'] != armies[aid]['country_id']):
                        raise ValueError('control record')
                    control[record['place_id']] = record['controller']
                else:
                    raise ValueError('record kind')
            if _json(m['control']) != _json(control):
                raise ValueError('control history')
            nodes = list(state['queue'])
            if current_event is not None:
                nodes.append(current_event)
            for aid, army in armies.items():
                if set(army) != set(initial[aid]) | {'stance', 'journey'}:
                    raise ValueError('army fields')
                for key in ('name', 'country_id'):
                    if army[key] != initial[aid][key]:
                        raise ValueError('army identity')
                if (any(type(army[k]) is not int or not 0 <= army[k] <= 2**63-1 for k in ('strength', 'grain'))
                        or [army['strength'], army['grain']] != balance[aid]):
                    raise ValueError('troop or army grain balance')
                arrivals = [q for q in nodes if q['kind'] == 'military_arrival' and q.get('army_id') == aid]
                if army['journey'] is None:
                    if army['location'] not in places or army['stance'] not in ('field', 'garrison') or arrivals:
                        raise ValueError('stationary army')
                else:
                    trip = army['journey']
                    if (set(trip) != {'from', 'to', 'route', 'departed_at', 'due', 'node', 'cause_id'}
                            or army['location'] is not None or army['stance'] != 'marching'
                            or trip['from'] not in places or trip['to'] not in places
                            or trip['from'] == trip['to'] or type(trip['departed_at']) is not int
                            or type(trip['due']) is not int or not 0 <= trip['departed_at'] <= state['time'] <= trip['due']
                            or trip['route'][0] != trip['from'] or trip['route'][-1] != trip['to']
                            or len(arrivals) != 1 or _json(arrivals[0]) != _json(trip['node'])):
                        raise ValueError('in-transit army')
                    seconds = 0
                    for source, target in zip(trip['route'], trip['route'][1:]):
                        edge = next(e for e in state['_geography'][source]['neighbors'] if e['to'] == target)
                        seconds += edge['seconds']
                    if trip['due'] != trip['departed_at'] + seconds:
                        raise ValueError('route clock')
                    node = trip['node']
                    if (set(node) != {'id', 'seq', 'at', 'kind', 'army_id', 'cause_id'}
                            or node['at'] != trip['due'] or node['army_id'] != aid
                            or node['cause_id'] != trip['cause_id'] or type(node['seq']) is not int
                            or not 1 <= node['seq'] <= state['_queue_seq'] or node['id'] != 'q-'+str(node['seq'])):
                        raise ValueError('arrival node')
            if len([q for q in nodes if q['kind'] == 'military_arrival']) != sum(a['journey'] is not None for a in armies.values()):
                raise ValueError('orphan arrival')
            rations = [q for q in nodes if q['kind'] == 'military_rations']
            if (len(rations) != 1 or set(rations[0]) != {'id', 'seq', 'at', 'kind'}
                    or type(rations[0]['seq']) is not int or not 1 <= rations[0]['seq'] <= state['_queue_seq']
                    or rations[0]['id'] != 'q-'+str(rations[0]['seq'])
                    or type(rations[0]['at']) is not int or rations[0]['at'] < state['time']
                    or rations[0]['at'] % DAY or rations[0]['at'] > state['time'] + DAY):
                raise ValueError('ration clock')
            if any(q['kind'].startswith('military_') and q['kind'] not in ('military_arrival', 'military_rations') for q in nodes):
                raise ValueError('unknown node')
            for place in state['places']:
                if type(place['grain']) is not int or place['grain'] < 0:
                    raise ValueError('warehouse grain')
            grain = sum(p['grain'] for p in state['places']) + sum(s['quantity'] for s in state['in_transit'])
            grain += sum(a['grain'] for a in armies.values()) + state['ledger']['consumed']
            if grain != state['ledger']['initial'] + state['ledger']['harvested']:
                raise ValueError('world grain conservation')
            return m
        except (KeyError, TypeError, ValueError, IndexError, StopIteration, OverflowError, RecursionError):
            raise WorldError('INVALID_MILITARY_STATE', '军事兵粮账、控制或时空节点不一致') from None

    def _military_record(self, state, army, kind, *, troops=0, grain=0, place_id=None, controller=None, cause_id=None):
        text = {'supply': '军粮从现场仓储实际转入军队。', 'rations': '部队消耗实有口粮，缺粮者离散，不能继续作战。',
                'battle': 'DM按实际在场兵粮和防御条件裁定实际战损。', 'control': '在场部队实际驻防并控制本地仓储与行政权限。'}[kind]
        event = self._event(state, 'military_'+kind, text, cause_id=cause_id)
        record = dict(event_id=event['id'], army_id=army['id'], kind=kind, troops=troops, grain=grain,
                      place_id=place_id or army['location'] or army['journey']['from'], controller=controller)
        event['military_record'] = json.loads(_json(record))
        state['military']['records'].append(record)
        if army['location'] is not None:
            self._local_notice(state, self._find(state['places'], army['location'], 'UNKNOWN_PLACE'), text)

    def _military_route(self, state, source, target):
        pending = [(0, source, [source])]; best = {source: 0}
        while pending:
            seconds, current, route = heapq.heappop(pending)
            if current == target:
                return route, seconds
            if best[current] != seconds:
                continue
            for edge in state['_geography'][current]['neighbors']:
                duration = seconds + edge['seconds']
                if duration < best.get(edge['to'], float('inf')):
                    best[edge['to']] = duration
                    heapq.heappush(pending, (duration, edge['to'], route + [edge['to']]))
        raise WorldError('NO_ROUTE', '军队无实际可达路线')

    def _military_action(self, state, person, intent_id, body, implemented):
        self._military_body(body)
        m = self._military_accounts(state)
        if m is None:
            raise WorldError('UNSUPPORTED', '场景未显式启用军事版本')
        action = body['action']
        army = self._find(m['armies'], body['army_id'], 'UNKNOWN_ARMY')
        if person['office'] not in ('marshal', 'king') or person['country_id'] != army['country_id']:
            raise DomainActionError('NOT_AUTHORIZED', '仅同国君主或军事官可指挥自己的军队', action)
        place = self._action_location(state, person, action)
        if army['journey'] is not None or army['location'] != place['id']:
            raise DomainActionError('NOT_AT_LOCATION', '即时指挥须本人和军队实际同地，信件不能瞬移命令', action)
        if army['strength'] == 0:
            raise DomainActionError('NO_TROOPS', '无现存兵力，招募不在此版本', action)
        rules = m['rules']; ration = army['strength'] * rules['grain_per_soldier_day']
        if action == 'military_supply':
            if self._controller(state, place) != person['country_id']:
                raise DomainActionError('NOT_AUTHORIZED', '未控制的粮仓不能补给己方军队', action)
            if place['grain'] < body['quantity']:
                raise DomainActionError('INSUFFICIENT_GRAIN', '本地实粮不足', action)
            if army['grain'] + body['quantity'] > 2**63-1:
                raise WorldError('INVALID_BODY', '军粮存量越界')
            if implemented:
                place['grain'] -= body['quantity']; army['grain'] += body['quantity']
                self._military_record(state, army, 'supply', grain=body['quantity'], cause_id=intent_id)
        else:
            if army['grain'] < ration:
                raise DomainActionError('INSUFFICIENT_GRAIN', '行动前至少须有一日实有军粮', action)
            if action == 'military_move':
                target = self._find(state['places'], body['to'], 'UNKNOWN_PLACE')
                if target['id'] == place['id']:
                    raise DomainActionError('ALREADY_THERE', '军队已经在此地', action)
                route, seconds = self._military_route(state, place['id'], target['id'])
                if not implemented:
                    return dict(implemented=False)
                due = state['time'] + seconds
                node = self._schedule(state, due, 'military_arrival', army_id=army['id'], cause_id=intent_id)
                army.update(location=None, stance='marching', journey=dict(
                    **{'from': place['id'], 'to': target['id']}, route=route, departed_at=state['time'],
                    due=due, node=json.loads(_json(node)), cause_id=intent_id))
                self._event(state, 'military_departure', '军队实际出发，途中不防守出发地，也尚未控制目的地。', person['id'], cause_id=intent_id)
                self._military_accounts(state)
                return dict(implemented=True, army_id=army['id'], military_journey_id='military-'+intent_id, due=due)
            enemies = [a for a in m['armies'] if a['location'] == place['id']
                       and a['country_id'] != army['country_id'] and a['strength'] > 0]
            if action == 'military_garrison':
                if enemies:
                    raise DomainActionError('HOSTILE_PRESENT', '敌军实际在场，不能跳过接战占领', action)
                if implemented:
                    army['stance'] = 'garrison'
                    if m['control'][place['id']] != army['country_id']:
                        m['control'][place['id']] = army['country_id']
                        self._military_record(state, army, 'control', controller=army['country_id'], cause_id=intent_id)
            elif action == 'military_engage':
                if not enemies:
                    raise DomainActionError('NO_ENEMY', '此地没有实际可接战敌军', action)
                losses = body['losses']; participants = [army] + enemies
                if set(losses) != {a['id'] for a in participants} or not any(losses.values()):
                    raise WorldError('INVALID_BATTLE', '必须裁定全部实际参战部队，不能选择性忽略守军或零战损')
                defense = rules['defense_multiplier'][place['id']]
                defenders = sum(a['strength'] for a in enemies)
                ready_defenders = sum(a['strength'] for a in enemies if a['grain'] >= a['strength'] * rules['grain_per_soldier_day'])
                if (any(losses[a['id']] > a['strength'] for a in participants)
                        or sum(losses[a['id']] for a in enemies) > army['strength'] // defense
                        or losses[army['id']] > ready_defenders * defense):
                    raise WorldError('INVALID_BATTLE', '战损越出现存兵力或有粮在场力量及显式防御界限')
                if implemented:
                    for combatant in participants:
                        spent = ration if combatant is army else min(combatant['grain'], combatant['strength'] * rules['grain_per_soldier_day'])
                        combatant['grain'] -= spent; state['ledger']['consumed'] += spent
                        combatant['strength'] -= losses[combatant['id']]
                        self._military_record(state, combatant, 'battle', troops=losses[combatant['id']], grain=spent, cause_id=intent_id)
                    # No surrender, loyalty, active-person death or forced choice is inferred.
                    army['stance'] = 'field'
        self._military_accounts(state)
        return dict(implemented=implemented, army_id=army['id'])

    def _military_event(self, state, event):
        m = self._military_accounts(state, current_event=event)
        if m is None:
            raise WorldError('INVALID_MILITARY_STATE', '无军事世界事件')
        if event['kind'] == 'military_arrival':
            army = self._find(m['armies'], event['army_id'], 'INVALID_MILITARY_STATE')
            trip = army['journey']
            if _json(trip['node']) != _json(event) or event['at'] != state['time']:
                raise WorldError('INVALID_MILITARY_STATE', '未匹配实际到达节点')
            army.update(location=trip['to'], stance='field', journey=None)
            text = army['name']+'实际抵达；尚未建立驻防控制。'
            self._event(state, 'military_arrived', text, cause_id=event['cause_id'])
            self._local_notice(state, self._find(state['places'], army['location'], 'UNKNOWN_PLACE'), text)
        elif event['kind'] == 'military_rations':
            if event['at'] != state['time']:
                raise WorldError('INVALID_MILITARY_STATE', '军粮节点尚未到期')
            rate = m['rules']['grain_per_soldier_day']
            for army in m['armies']:
                needed = army['strength'] * rate
                spent = min(army['grain'], needed)
                # Partial soldier-day is consumed but does not sustain a full soldier.
                dispersed = army['strength'] - spent // rate
                army['grain'] -= spent; army['strength'] -= dispersed
                state['ledger']['consumed'] += spent
                self._military_record(state, army, 'rations', troops=dispersed, grain=spent)
            self._schedule(state, state['time'] + DAY, 'military_rations')
        else:
            raise WorldError('INVALID_MILITARY_STATE', '未知军事节点')
        self._military_accounts(state)

    def _military_observations(self, state, person):
        """Only co-located direct sight; no same-country remote intelligence."""
        if 'military' not in state or person['location'] is None or not _lc_alive(state,person['id']):
            return []
        result = []
        for army in state['military']['armies']:
            if army['location'] != person['location'] or army['strength'] == 0:
                continue
            text = army['name']+'在现场，'+('已驻防。' if army['stance'] == 'garrison' else '尚未驻防。')
            if person['country_id'] == army['country_id'] and person['office'] in ('marshal', 'king'):
                text += f"当场点验兵力{army['strength']}人，随军粮{army['grain']}份。"
            result.append(dict(id='military-sight-'+army['id']+'-'+str(state['revision']), time=state['time'], text=text, source='亲见：现场部队'))
        return result

    def _apply_action(self, state, person, intent_id, body, ruling):
        mutation = _world_mutation_scope.get()
        if mutation is not None and mutation[0] is self:
            from .coordinator_scheduler import parent_intent
            self._check_scheduler_role(mutation[1], person['id'], parent_intent(self,mutation[1],person['id'],intent_id), mutation[2])
        action = body['action']
        if '_lifecycle' in state and not _lc_active(state,person['id']):raise WorldError('ACTOR_EXECUTION_PERSON_DEAD','人物已亡或无当前任期')
        if action in _LC_ACTIONS:return self._lifecycle_effect(state,person,intent_id,body,ruling)
        implemented = ruling in ('成功','大成功')
        if action in ('military_move', 'military_supply', 'military_garrison', 'military_engage'):
            result = self._military_action(state, person, intent_id, body, implemented)
            self._project_conditions(state, 'military-action:'+intent_id)
            return result
        if action in ('start_project','continue_project','interrupt_project','share_project_knowledge'):
            return self._project_action(state, person, intent_id, body, ruling)
        if action == 'start_production':
            return self._start_production(state, person, intent_id, body, implemented)
        if action == 'ship_material':
            return self._ship_material(state, person, intent_id, body, implemented)
        if action == 'meet':
            if '_p04' not in state:
                raise WorldError('UNSUPPORTED','此局未启用事件会面')
            target = self._find(state['persons'],body['to'],'UNKNOWN_PERSON')
            if not _lc_alive(state,target['id']):raise WorldError('TARGET_NOT_ALIVE','目标已经死亡')
            if target['id']==person['id']:
                raise WorldError('INVALID_TARGET','会面须有另一实际人物')
            place = self._action_location(state,person,action)
            self._action_location(state,target,action)
            if target['location']!=place['id']:
                raise DomainActionError('NOT_AT_LOCATION','会面双方尚未实际同地',action)
            if not implemented:
                return dict(implemented=False)
            self._notice(state,target['id'],body['text'],'实际会面')
            self._p04_wake(state,target['id'])
            self._event(state,'meeting','双方实际同地会面；言语仅交给参与者。',person['id'],cause_id=intent_id)
            return dict(implemented=True,participant=target['id'])
        if action == 'wait':
            return {'implemented': implemented}
        if action == 'investigate':
            place = self._action_location(state, person, action)
            due = state['time']+state['_config']['investigation_seconds']
            self._schedule(state, due, 'investigation', person_id=person['id'], place_id=place['id'], ruling=ruling, journey_version=person.get('journey_version',0))
            return {'implemented':True, 'task_id':'task-'+intent_id, 'due':due}
        if action in ('send', 'command'):
            target = self._find(state['persons'], body['to'], 'UNKNOWN_PERSON')
            if action == 'command' and (person['office'] != 'king' or target['country_id'] != person['country_id'] or target['id'] == person['id']):
                raise DomainActionError('NOT_AUTHORIZED', '仅君主可向本国其他人物发命令', action)
            self._action_location(state, person, action)
            self._action_location(state, target, action)
            duration = self._route_seconds(state, person['location'], target['location'])
            if not implemented:
                return {'implemented':False}
            due, message_id = state['time']+duration, 'message-'+intent_id
            command_id = 'command-'+intent_id if action == 'command' else None
            if command_id:
                state['_commands'].append({'id':command_id, 'issuer':person['id'], 'recipient':target['id'], 'place':target['location'],
                                           'delivered':False, 'response':None, 'restrict': target['office']=='marshal' and any(word in body['text'] for word in ('限制聚集','封禁','限制接触'))})
            contact = self._depart_contact(state, 'messenger', person['location'], target['location'], intent_id)
            self._schedule(state, due, 'deliver', message_id=message_id, sender=person['id'], recipient=target['id'], destination=target['location'], text=body['text'], command_id=command_id, **contact)
            return {'implemented':True, 'due':due, 'message_id':message_id, **({'command_id':command_id} if command_id else {})}
        if action == 'respond':
            command = self._find(state['_commands'], body['command_id'], 'UNKNOWN_COMMAND')
            if command['recipient'] != person['id'] or not command['delivered']:
                raise WorldError('COMMAND_NOT_DELIVERED', '本人尚未收到这份命令')
            if person['location'] != command['place']:
                # The rejection is business-only when this is a real delivered
                # command produced by a durable committed action, not a bad row.
                mutation = _world_mutation_scope.get()
                actual = None
                if mutation is not None and mutation[0] is self:
                    producer_db = mutation[1]
                else:
                    from .coordinator_scheduler import _child_effect_scope
                    child_scope = _child_effect_scope.get()
                    producer_db = child_scope[1] if child_scope is not None and child_scope[0] is self else None
                if producer_db is not None and command['id'].startswith('command-'):
                    actual = producer_db.execute('SELECT person,payload,receipt FROM intents WHERE id=?', (command['id'][8:],)).fetchone()
                if actual is not None and actual[2] is not None:
                    issued=json.loads(actual[1])['body'];receipt=json.loads(actual[2])
                    if (actual[0]==command['issuer'] and issued.get('action')=='command'
                            and issued.get('to')==person['id'] and receipt.get('status')=='committed'
                            and receipt.get('effect',{}).get('command_id')==command['id']):
                        self._find(state['places'],command['place'],'UNKNOWN_PLACE')
                        self._action_location(state,person,action)
                        raise DomainActionError('NOT_AT_LOCATION', '不在命令执行地点', action)
                raise WorldError('NOT_AT_LOCATION', '不在命令执行地点')
            if command['response'] is not None:
                raise WorldError('ALREADY_RESPONDED', '该命令已回应')
            if not implemented:
                return {'implemented':False}
            if body['accept'] and command['restrict']:
                command_place = self._find(state['places'], command['place'], 'UNKNOWN_PLACE')
                if self._controller(state, command_place) != person['country_id']:
                    raise DomainActionError('NOT_AUTHORIZED', '失去本地控制后不能执行封禁命令', action)
            command['response'] = body['accept']
            effect = {'implemented':True, 'accepted':body['accept'], 'command_id':command['id']}
            if body['accept'] and command['restrict']:
                place = self._find(state['places'], command['place'], 'UNKNOWN_PLACE')
                before = place['disease']['restricted']
                place['disease']['restricted'] = True
                effect['contact_restriction'] = {'place_id':place['id'], 'before':before, 'after':True,
                                                 'mechanism_sha256':(state['pathology_mechanism_sha256'] if state.get('pathology_runtime') == CONTACT_RUNTIME
                                                                     else pathology_digest(state['pathology']))}
                if not before:
                    text = '现场开始限制聚集，人员依令减少接触。'
                    self._event(state, 'contact_restriction_applied', place['name']+'：'+text,
                                person['id'], cause_id=intent_id)
                    self._local_notice(state, place, text)
                    self._project_conditions(state, 'restriction:'+intent_id)
            self._notice(state, person['id'], '已按自己的选择'+('接受' if body['accept'] else '拒绝')+'命令。', '本人回应')
            return effect
        if action == 'ship':
            source = self._action_location(state, person, action)
            target = self._find(state['places'], body['to'], 'UNKNOWN_PLACE')
            if (person['office'] != 'finance' or source['country_id'] != person['country_id'] or target['country_id'] != person['country_id']
                    or self._controller(state, source) != person['country_id'] or self._controller(state, target) != person['country_id']):
                raise DomainActionError('NOT_AUTHORIZED', '仅本国财政官可调运本国库存', action)
            if target['id'] == source['id']:
                raise WorldError('INVALID_TARGET', '发收仓不得相同')
            if source['grain'] < body['quantity']:
                raise DomainActionError('INSUFFICIENT_GRAIN', '实际库存不足', action)
            duration = self._route_seconds(state, source['id'], target['id'])
            if not implemented:
                return {'implemented':False}
            due, shipment_id = state['time']+duration, 'shipment-'+intent_id
            source['grain'] -= body['quantity']
            state['in_transit'].append({'id':shipment_id, 'from':source['id'], 'to':target['id'], 'quantity':body['quantity'], 'due':due})
            contact = self._depart_contact(state, 'ship', source['id'], target['id'], intent_id)
            self._schedule(state, due, 'shipment', shipment_id=shipment_id, **contact)
            self._notice(state, person['id'], f"实际发出{body['quantity']}份粮，尚在途中。", '发地出仓清点')
            return {'implemented':True, 'due':due, 'shipment_id':shipment_id}
        if action == 'travel':
            target = self._find(state['places'], body['to'], 'UNKNOWN_PLACE')
            if target['id'] == person['location']:
                raise DomainActionError('ALREADY_THERE', '已经在目的地', action)
            self._action_location(state, person, action)
            duration = self._route_seconds(state, person['location'], target['id'])
            if not implemented:
                return {'implemented':False}
            due = state['time']+duration
            contact = self._depart_contact(state, 'travel', person['location'], target['id'], intent_id)
            self._schedule(state, due, 'arrival', person_id=person['id'], destination=target['id'], **contact)
            self._project_departure(state, person, intent_id)
            person['location'] = None
            person['journey_version'] = person.get('journey_version',0)+1
            return {'implemented':True, 'due':due, 'journey_id':'journey-'+intent_id}
        raise WorldError('UNSUPPORTED', '动作尚未实现')

    def technical_pause(self, code, *, execution=None, coordinator=None):
        self._text(code, 'INVALID_DIAGNOSTIC')
        if execution is not None:
            self._validate_execution(execution)
        def change(state, db):
            exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='actor_execution_leases'").fetchone()
            if execution is None and self._scoped_coordinator(coordinator) is None and exists and db.execute('SELECT 1 FROM actor_execution_leases WHERE world_id=? LIMIT 1', (state['world_id'],)).fetchone():
                raise WorldError('ACTOR_EXECUTION_REQUIRED', '受保护世界的暂停必须持执行许可')
            from .coordinator_scheduler import block_activation
            captured = execution or (self._scoped_attempt().permit if self._scoped_attempt() is not None else None)
            block_activation(self, db, state, captured, code)
            state['status'] = 'technical_paused'
            state['diagnostic'] = {'code':code, 'time':state['time']}
        parent = self._scoped_coordinator(coordinator)
        if parent is not None:
            with self._coordinator_scope(parent):
                return self._mutate(change, execution=execution, person_id=execution.person_id if execution is not None else None)
        return self._mutate(change, execution=execution, person_id=execution.person_id if execution is not None else None)

    def control(self, action, *, coordinator=None):
        if type(action) is not str or action not in ('pause', 'resume'):
            raise WorldError('INVALID_CONTROL', '仅允许pause/resume')
        def change(state, db):
            if action == 'pause' and state['status'] == 'running':
                state['status'] = 'user_paused'
            elif action == 'resume' and state['status'] == 'user_paused':
                state['status'] = 'running'
            elif action == 'resume' and state['status'] == 'horizon_paused':
                state['horizon'] += 90*DAY
                state['status'] = 'running'
                self._event(state, 'horizon_extended', '观众显式续跑，观察期延长90日。')
            else:
                raise WorldError('WORLD_PAUSED', '当前暂停状态不允许该操作')
        parent = self._scoped_coordinator(coordinator)
        if parent is not None:
            with self._coordinator_scope(parent):
                return self._mutate(change)
        return self._mutate(change)

    def _register_private_source(self, source):
        from .coordinator_scheduler import register_source
        register_source(self, source)

    def inspect_scheduler_boundary(self, *, actor_db_path):
        from .coordinator_scheduler import inspect
        return inspect(self, actor_db_path)

    def register_scheduler(self, *, coordinator, expected_version, boundary, actor_db_path):
        from .coordinator_scheduler import register
        return register(self, coordinator=coordinator, expected_version=expected_version,
                        boundary=boundary, actor_db_path=actor_db_path)

    def open_or_recover_window(self, *, coordinator, expected_version):
        from .coordinator_scheduler import open_window
        return open_window(self, coordinator=coordinator, expected_version=expected_version)

    def next_activation(self, window_id, *, coordinator):
        from .coordinator_scheduler import next_activation
        return next_activation(self, window_id, coordinator=coordinator)

    def complete_window(self, window_id, *, coordinator):
        from .coordinator_scheduler import complete_window
        return complete_window(self, window_id, coordinator=coordinator)

    def _advance_state(self, state, db, target_seconds):
        if state['status'] != 'running':
            raise WorldError('WORLD_PAUSED', '世界没有运行')
        if '_lifecycle' in state:
            self._lifecycle_check(db,state)
            from .coordinator_scheduler import exists
            if not exists(db,'coordinator_scheduler_registration'):raise WorldError('ACTIVATION_REQUIRED','生命周期到期须原登记开窗')
            if db.execute("SELECT 1 FROM coordinator_windows WHERE state='open'").fetchone():raise WorldError('WINDOW_INCOMPLETE','旧窗未闭合')
            if db.execute("SELECT 1 FROM coordinator_activations WHERE private_closed!=1 OR phase NOT IN ('completed','rejected')").fetchone():raise WorldError('WINDOW_INCOMPLETE','旧私密未闭合')
        if target_seconds < state['time']:
            raise WorldError('INVALID_TIME', '不能倒退时间')
        if 'economy' in state:
            self._production_accounts(state)
        elif any(q['kind'] == 'production' for q in state['queue']):
            raise WorldError('INVALID_PRODUCTION_STATE', '未启用经济却存在生产节点')
        target = min(target_seconds, state['horizon'])
        while state['queue'] and state['queue'][0]['at'] <= target:
            event = state['queue'].pop(0)
            state['time'] = event['at']
            before = {p['id']:(len(state['_observations'][p['id']]),len(state['_messages'][p['id']])) for p in state['persons']} if '_p04' in state else None
            self._process_event(state, event)
            if before is not None:
                for pid,(observed,received) in before.items():
                    observations = state['_observations'][pid][observed:]
                    messages = state['_messages'][pid][received:]
                    if observations or any(m['from']!=pid for m in messages):
                        self._p04_wake(state,pid)
        state['time'] = target
        if state['time'] == state['horizon']:
            state['status'] = 'horizon_paused'

    def advance(self, target_seconds, *, coordinator=None):
        if type(target_seconds) is not int or target_seconds < 0:
            raise WorldError('INVALID_TIME', '模拟时间必须为非负整数秒')
        def change(state, db):
            from .coordinator_scheduler import exists
            if exists(db, 'coordinator_scheduler_registration'):
                pending = db.execute("SELECT 1 FROM coordinator_windows WHERE state='open'").fetchone()
                raise WorldError('WINDOW_INCOMPLETE' if pending else 'ACTIVATION_REQUIRED', '登记后必须由持久窗口推进')
            self._advance_state(state, db, target_seconds)
        parent = self._scoped_coordinator(coordinator)
        if parent is not None:
            with self._coordinator_scope(parent):
                return self._mutate(change)
        return self._mutate(change)

    def edit_card(self, person_id, changes, *, coordinator=None):
        if type(person_id) is not str or not person_id:
            raise WorldError('INVALID_PERSON', '人物标识必须为非空字符串')
        if type(changes) is not dict or not changes:
            raise WorldError('INVALID_CARD', '变更必须为非空对象')
        allowed = {'values', 'interests', 'risk', 'mission'}
        if not set(changes) <= allowed:
            raise WorldError('INVALID_CARD', '仅可编辑人格、利益、风险偏好和长期信念')
        for key, value in changes.items():
            if type(value) is not str or not value.strip() or len(value) > 2000:
                raise WorldError('INVALID_CARD', '每项必须为非空字符串且不超过2000字')
        def change(state, db):
            person = self._find(state['persons'], person_id, 'UNKNOWN_PERSON')
            person['card'] = {**person['card'], **changes}
            person['card_version'] = person.get('card_version', 1) + 1
            person['card_author'] = 'user'
            self._event(state, 'card_edited', f"角色卡已由用户前向修改：{','.join(changes.keys())}", person_id)
        parent = self._scoped_coordinator(coordinator)
        if parent is not None:
            with self._coordinator_scope(parent):
                return self._mutate(change)
        return self._mutate(change)


    @staticmethod
    def _lifecycle_check(db, state, *, cold_decode=None):
        if type(state) is dict and '_lifecycle' not in state:
            sql = 'SELECT state FROM snapshots ORDER BY seq LIMIT 1'
            if cold_decode is None:
                genesis=json.loads(db.execute(sql).fetchone()[0])
            else:
                genesis=cold_decode(_lc_root_row(db, sql, columns=('state',))[0])
            _lifecycle_shape(state,genesis)
            if not _lc_optin(genesis):return None
        from .coordinator_scheduler import _lifecycle_value_invoke
        return _lifecycle_value_invoke(WorldStore._lifecycle_check_owned, (db, state),
            inputs=(_lc_geo1133_direct_input(state),), detached=True)

    @staticmethod
    def _lifecycle_check_owned(db,state):
        genesis=_lc_root_json(db,'SELECT state FROM snapshots ORDER BY seq LIMIT 1')
        _lifecycle_shape(state,genesis)
        if not _lc_optin(genesis):return None
        from .coordinator_scheduler import lifecycle_native_read, _lifecycle_value_context
        read=_lifecycle_value_context.get().keep_reader(lifecycle_native_read(db))
        projected=_lifecycle_project(read,genesis,state['revision'])
        if projected['lifecycle']!=state['_lifecycle']:_lc_fail('当前值与原持久历史不符')
        return projected

    @staticmethod
    def _lifecycle_actor_gate(db,state,person_id):
        proof=WorldStore._lifecycle_check(db,state)
        if proof is not None and person_id not in proof['active']:
            raise WorldError('ACTOR_EXECUTION_PERSON_DEAD','人物已亡或无当前合法任期')

    def lifecycle_identity(self,person_id,revision=None):
        from .coordinator_scheduler import _lifecycle_operation_scope
        with _lifecycle_operation_scope():
            if revision is not None:_lc_int(revision,1)
            with self._connection(readonly=True) as db:
                db.execute('BEGIN')
                row=_lc_root_row(db,'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1' if revision is None else
                    'SELECT state FROM snapshots WHERE seq=?',() if revision is None else (revision,),columns=('state',))
                if row is None:raise WorldError('UNKNOWN_SNAPSHOT','不存在该提交序号')
                state=_lc_json(row[0]);proof=self._lifecycle_check(db,state)
                self._find(state['persons'],person_id,'UNKNOWN_PERSON')
                if proof is None:return None
                _lc_reserve(state,1,'identity-result-and-local-containers')
                tenure=_lc_tenure(state,person_id)
                return dict(world_id=state['world_id'],person_id=person_id,observation_revision=state['revision'],
                    origin_ref=person_id,tenure_ref=tenure['id'] if tenure is not None else None)

    def lifecycle_effect_protocol(self,person_id,intent_id,*,execution=None):
        from .coordinator_scheduler import _lifecycle_operation_scope
        with _lifecycle_operation_scope():
            from .coordinator_scheduler import source_lock,durable_body
            with source_lock(self),self._connection(readonly=True) as db:
                db.execute('BEGIN')
                state=_lc_root_json(db,'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1')
                self._check_actor_execution(db,state['world_id'],person_id,execution,readonly=True)
                proof=self._lifecycle_check(db,state)
                if proof is None:return None
                row=_lc_root_row(db,'SELECT person,payload FROM intents WHERE id=?',(intent_id,),columns=('person','payload'))
                if row is None or row[0]!=person_id:_lc_fail()
                body=_lc_json(row[1])['body'];self._validate_body(body)
                if set(body)!={'action','text','physical_attempt'}:_lc_fail('缺原v3提案')
                durable_body(self,db,state,person_id,intent_id,body,execution)
                _lc_reserve(state,1,'protocol-result-and-local-containers')
                life=state['_lifecycle'];person=self._find(state['persons'],person_id,'UNKNOWN_PERSON')
                return dict(version=_LC_VERSION,actions={k:sorted(v-{'action'}) for k,v in _LC_ACTIONS.items()},
                    base_revision=state['revision'],physical_attempt=_lc_copy(body['physical_attempt']),
                    personal_objects=[_lc_copy(m) for m in life['rules']['personal_objects'] if m['holder_person_id']==person_id],
                    institutional_facts=[_lc_copy(f) for f in life['rules']['institutional_facts'] if f['country_id']==person['country_id']],
                    relationship_facts=_lc_copy(life['rules']['relationship_facts']),
                    scene_persons=[{k:p.get(k) for k in ('id','location','country_id','office','office_id')}|
                        dict(alive=_lc_alive(state,p['id'])) for p in state['persons']],
                    constraints='仅原人物明确锚；同地实持手段；公文只原文Unicode片段；死亡须正时长原开窗到期。')

    def _lifecycle_context(self, state, person, intent_id, body):
        from .coordinator_scheduler import _lifecycle_value_invoke
        return _lifecycle_value_invoke(self._lifecycle_context_owned,
            (state, person, intent_id, body), inputs=(state, body), direct_factor=2)

    def _lifecycle_context_owned(self,state,person,intent_id,body):
        from .coordinator_scheduler import _child_effect_scope,lifecycle_native_read
        child=_child_effect_scope.get()
        if child is None or child[0] is not self or child[2]!=person['id'] or child[4]!=intent_id:_lc_fail('须原DM子效果scope')
        db=child[1];parent=child[3]
        row=_lc_root_row(db,'SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1',(parent,),columns=('version','data'))
        if row is None:_lc_fail()
        version=row[0];decision=_lc_json(row[1])
        _lc_reserve(decision,1,'context-indices-cause-and-tuple')
        indices=[i for i,e in enumerate(decision['effects']) if intent_id==f'dm:{len(parent)}:{parent}:{version}:{i}' and e==body]
        if len(indices)!=1:_lc_fail('原完整decision效果不符')
        from .coordinator_scheduler import _lifecycle_value_context
        read=_lifecycle_value_context.get().keep_reader(lifecycle_native_read(db))
        first=_lc_read(read,'main.snapshots',columns=('state',),order=(('seq',False),),limit=1,first=True)
        if first is None:_lc_fail()
        genesis=_lc_json(first[0])
        _lifecycle_project(read,genesis,state['revision'])
        states=(read,genesis,state['revision'])
        cause=dict(parent_intent_id=parent,decision_version=version,effect_index=indices[0],child_intent_id=intent_id)
        pid,proposal,effect,decision,_=_lc_child(read,state['world_id'],cause,state['revision'],states,committed=False)
        if pid!=person['id'] or effect!=body or decision['base_revision']!=state['revision']:_lc_fail('原基准不符')
        return proposal,decision,cause

    def _lifecycle_effect(self,state,person,intent_id,body,ruling):
        if '_lifecycle' not in state or not _lc_optin(state):raise WorldError('UNSUPPORTED','本局未共同启用生命周期')
        _lifecycle_effect_body(body)
        proposal,decision,cause=self._lifecycle_context(state,person,intent_id,body)
        if ruling!=decision['ruling']:_lc_fail('原裁定档位不符')
        condition=_lc_effect_conditions(state,person,proposal,body,ruling,decision['base_revision'])
        implemented=ruling in ('成功','大成功');life=state['_lifecycle'];revision=state['revision']+1
        if body['action']=='resolve_physical_attempt':
            target,means=condition;outcome=body['outcome']
            if outcome!='mortal':
                event=self._event(state,'physical_attempt_resolved',body['reason'],person['id'],cause_id=intent_id)
                event.update(outcome=outcome,target_person_id=target['id'],means_id=means['id'],basis_refs=_lc_copy(body['basis_refs']))
                return dict(implemented=implemented,outcome=outcome)
            injury_id='injury-'+intent_id
            node=self._schedule(state,_lc_int(state['time']+life['rules']['death_delay_seconds'],1),
                'person_death_due',person_id=target['id'],injury_id=injury_id,cause_id=intent_id,lifecycle_version=_LC_VERSION)
            life['transitions'].append(dict(id=injury_id,kind='mortal_injury',person_id=target['id'],actor_person_id=person['id'],
                cause_ref=cause,occurred_at=state['time'],committed_revision=revision,basis_refs=_lc_copy(body['basis_refs']),
                means_id=means['id'],node=_lc_copy(node)))
            return dict(implemented=True,outcome=outcome,injury_id=injury_id,due=node['at'])
        if not implemented:return dict(implemented=False)
        if body['action']=='write_office_document':
            document=dict(id='office-document-'+intent_id,author_person_id=person['id'],office_id=person['office_id'],
                place_id=condition['id'],text=body['text'],created_revision=revision,cause_ref=cause,access='current_office_at_place')
            if any(d['id']==document['id'] for d in life['documents']):_lc_fail('文书ID重复')
            life['documents'].append(document)
            return dict(implemented=True,document_id=document['id'])
        delivery=dict(id='office-delivery-'+intent_id,document_id=condition['id'],recipient_person_id=person['id'],
            delivered_revision=revision,time=state['time'],channel='onsite_document_read',cause_ref=cause,
            text_sha256=hashlib.sha256(condition['text'].encode('utf-8')).hexdigest())
        existing=next((d for d in life['deliveries'] if d['id']==delivery['id']),None)
        if existing is not None:
            if existing!=delivery:_lc_fail('delivery重放换载荷')
        else:
            life['deliveries'].append(delivery)
            self._notice(state,person['id'],condition['text'],'机构公文：'+condition['id'])
            self._p04_wake(state,person['id'])
        return dict(implemented=True,document_id=condition['id'],delivery_id=delivery['id'])

    def _lifecycle_death_due(self,state,node):
        death,cancelled=_lc_death_records(state,node,state['revision']+1)
        for q in cancelled:
            if q['kind']=='project_stage':
                task=self._find(state['projects']['tasks'],q['project_task_id'],'INVALID_PROJECT_STATE')
                self._project_end(state,task,'interrupted','组织者实际死亡；'+death['id'])
            elif q['kind'] in ('arrival','investigation'):
                self._event(state,'arrival_interrupted' if q['kind']=='arrival' else 'investigation_interrupted',
                    '本人实际死亡，个人任务中断。',death['person_id'],cause_id=q.get('cause_id'))
        self._event(state,'person_death','原致命伤于正时长真实到期，任期关闭。',death['person_id'],cause_id=node['cause_id'])

    def pending_lifecycle_jobs(self):
        from .coordinator_scheduler import _lifecycle_operation_scope
        with _lifecycle_operation_scope():
            with self._connection(readonly=True) as db:
                db.execute('BEGIN')
                state=_lc_root_json(db,'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1')
                proof=self._lifecycle_check(db,state)
                _lc_reserve(state,1,'pending-job-result')
                return [] if proof is None else [j['id'] for j in proof['lifecycle']['jobs'] if j['result'] is None]

    def _lifecycle_work(self,job_id):
        from .coordinator_scheduler import lifecycle_work
        if self._scoped_attempt() is not None:raise WorldError('ACTIVATION_BINDING_CONFLICT','root job不是actor工作')
        return lifecycle_work(self,job_id)

    def prepare_lifecycle(self,job_id,service_version):
        return _lc_geo1133_stage(self._lc_geo1133_prepare_body, (job_id,service_version), {})

    def _lc_geo1133_prepare_body(self,job_id,service_version):
        work=self._lifecycle_work(job_id)
        if type(service_version) is not str or len(service_version)!=64 or any(c not in '0123456789abcdef' for c in service_version):_lc_fail('非法服务版本')
        def prepare(state,db):
            work.before_effect(db,state)
            if '_lifecycle' not in state:raise WorldError('UNSUPPORTED','未启用生命周期')
            if state['status']!='running':raise WorldError('WORLD_PAUSED','暂停不请求继任')
            job=self._find(state['_lifecycle']['jobs'],job_id,'ACTOR_RECOVERY_UNPROVEN')
            if job['result'] is not None:return dict(status='completed',result=_lc_copy(job['result']))
            attempts=job['attempts']
            if attempts and attempts[-1]['status']=='pending':
                last=attempts[-1]
                if last['request']['base_revision']==state['revision']:
                    if last['request']['service_version']!=service_version:_lc_fail('同请求换服务')
                    return dict(status='pending',request=_lc_copy(last['request']),request_sha256=last['request_sha256'])
                last['status']='stale'
            revision=state['revision']+1
            pid=attempts[0]['request']['proposed_person_id'] if attempts else 'person-'+uuid.uuid4().hex
            request=_lc_request(state,job,uuid.uuid4().hex,pid,revision,service_version)
            attempt=dict(request_id=request['request_id'],prepared_revision=revision,request=request,
                request_sha256=_lc_hash(request),response_raw=None,response=None,decision_sha256=None,status='pending',failure_code=None)
            attempts.append(attempt)
            return dict(status='pending',request=_lc_copy(request),request_sha256=attempt['request_sha256'])
        return self._lc_geo1133_mutate_body(prepare,_precommit=work,_geo_direct=True)

    def _lifecycle_bound_attempt(self,state,job_id,request_sha256):
        job=self._find(state['_lifecycle']['jobs'],job_id,'ACTOR_RECOVERY_UNPROVEN')
        if not job['attempts']:_lc_fail('没有durable请求')
        attempt=job['attempts'][-1]
        if attempt['request_sha256']!=request_sha256 or _lc_hash(attempt['request'])!=request_sha256:_lc_fail('请求摘要不符')
        if job['result'] is None and (attempt['status']!='pending' or attempt['request']['base_revision']!=state['revision']):
            from .coordinator_scheduler import _lc_stale1141_issue
            return _lc_stale1141_issue(self,job_id,request_sha256)
        return job,attempt

    def preview_lifecycle(self,job_id,request_sha256,response_raw,response):
        return _lc_geo1133_stage(self._lc_geo1133_preview_lifecycle_body, (job_id,request_sha256,response_raw,response), {})

    def _lc_geo1133_preview_lifecycle_body(self,job_id,request_sha256,response_raw,response):
        work=self._lifecycle_work(job_id)
        _lc_reserve(response,1,'preview-response-input-and-direct-result')
        from .coordinator_scheduler import source_lock, _lc_stale1141_Control
        with source_lock(self),self._connection(readonly=True) as db:
            db.execute('BEGIN')
            state=_lc_root_json(db,'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1')
            self._check_coordinator_execution(db,state['world_id'],None,readonly=True)
            self._restore_geography(db,state);work.before_preview(db,state)
            bound=self._lifecycle_bound_attempt(state,job_id,request_sha256)
            if type(bound) is _lc_stale1141_Control:
                return bound
            job,attempt=bound
            decision_sha=_lc_hash(response)
            if _lc_json(response_raw)!=response:_lc_fail('响应原文不符')
            if job['result'] is not None:
                if attempt['response_raw']!=response_raw or attempt['response']!=response or attempt['decision_sha256']!=decision_sha:_lc_fail('已完成job换载荷')
                work.check_source()
                return dict(status='completed',result=_lc_copy(job['result']))
            _lc_response(state,attempt['request'],response_raw,response)
            result=_lc_finish_job(state,job,attempt,response_raw,response,state['revision']+1)
            work.check_source()
            return dict(status='preview',world_id=state['world_id'],base_revision=state['revision'],
                request_sha256=request_sha256,decision_sha256=decision_sha,result=_lc_copy(result))

    def commit_lifecycle(self,job_id,request_sha256,response_raw,response,*,decision_sha256):
        return _lc_geo1133_stage(self._lc_geo1133_commit_body, (job_id,request_sha256,response_raw,response), {'decision_sha256':decision_sha256})

    def _lc_geo1133_commit_body(self,job_id,request_sha256,response_raw,response,*,decision_sha256):
        work=self._lifecycle_work(job_id)
        _lc_reserve(response,1,'commit-response-input-and-direct-result')
        response=_lc_copy(response)
        if _lc_hash(response)!=decision_sha256 or _lc_json(response_raw)!=response:_lc_fail('裁定摘要/原文不符')
        from .coordinator_scheduler import _lc_stale1141_Control
        def commit(state,db):
            work.before_effect(db,state)
            bound=self._lifecycle_bound_attempt(state,job_id,request_sha256)
            if type(bound) is _lc_stale1141_Control:
                return bound
            job,attempt=bound
            if job['result'] is not None:
                if attempt['response_raw']!=response_raw or attempt['response']!=response or attempt['decision_sha256']!=decision_sha256:_lc_fail('已完成job换载荷')
                return _lc_copy(job['result'])
            if state['status']!='running':raise WorldError('WORLD_PAUSED','暂停不提交继任')
            if _lc_response(state,attempt['request'],response_raw,response)!=decision_sha256:_lc_fail()
            return _lc_copy(_lc_finish_job(state,job,attempt,response_raw,response,state['revision']+1))
        return self._lc_geo1133_mutate_body(commit,_precommit=work,_geo_direct=True)

    def fail_lifecycle(self,job_id,request_sha256,failure_code):
        # The terminal failure write is followed by technical_pause on the same
        # Entry. Its known detached result must not pin a previous Geo graph.
        return _lc_geo1133_stage(self._lc_stale1141_fail_body, (job_id,request_sha256,failure_code), {})

    def _lc_stale1141_fail_body(self,job_id,request_sha256,failure_code):
        work=self._lifecycle_work(job_id)
        if (type(failure_code) is not str or not 1<=len(failure_code)<=128
                or any(c not in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for c in failure_code)):_lc_fail('故障码须脱敏')
        def fail(state,db):
            work.before_effect(db,state)
            job=self._find(state['_lifecycle']['jobs'],job_id,'ACTOR_RECOVERY_UNPROVEN')
            if job['result'] is not None:_lc_fail('已完成job不得失败')
            if not job['attempts'] or job['attempts'][-1]['request_sha256']!=request_sha256:_lc_fail()
            attempt=job['attempts'][-1]
            if attempt['status']!='pending':_lc_fail('attempt已终结')
            attempt.update(status='failed',failure_code=failure_code)
            return dict(job_id=job_id,request_sha256=request_sha256,status='failed',failure_code=failure_code)
        return self._lc_geo1133_mutate_body(fail,_precommit=work,_geo_direct=True)


    def snapshot(self, seq=None):
        if seq is not None and (type(seq) is not int or seq < 1):
            raise WorldError('INVALID_SEQUENCE', '提交序号必须为正整数')
        if not self.db_path.exists():
            raise WorldError('NOT_INITIALIZED', '世界尚未初始化')
        with self._connection(readonly=True) as db:
            row = db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1' if seq is None else 'SELECT state FROM snapshots WHERE seq=?', () if seq is None else (seq,)).fetchone()
            if row is None:
                raise WorldError('UNKNOWN_SNAPSHOT', '不存在该提交序号')
            return self._public(json.loads(row[0]))


# Diagnostic loaded generation only; no lifecycle or authority semantics change.
# Host-only dual-store material generations. No business authority is minted.
_HISTORY_MAIN = ('snapshots','world_constants','scheduler_genesis','intents',
    'coordinator_scheduler_registration','coordinator_windows','coordinator_activations',
    'actor_coordinator_delegations','actor_activation_delegations',
    'coordinator_activation_outcomes','coordinator_activation_blocks','coordinator_fault_issuers',
    'coordinator_technical_maintenance','coordinator_technical_window_releases',
    'dm_proposals','dm_decisions','dm_timeline')
_HISTORY_PRIVATE = ('actor_world_binding','actor_sessions','actor_intents','actor_calls',
    'actor_session_heads','actor_intent_execution')
_HISTORY_EPOCH_DDL = ("CREATE TABLE history_proof_epochs (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
    "protocol_version INTEGER NOT NULL CHECK(typeof(protocol_version)='integer' AND protocol_version=1), "
    "store_nonce TEXT NOT NULL CHECK(typeof(store_nonce)='text' AND length(store_nonce)=32), "
    "generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=0))")
_HISTORY_COVER_DDL = ("CREATE TABLE history_proof_coverage (table_name TEXT PRIMARY KEY, table_sql TEXT NOT NULL)")


def _history_failure():
    raise WorldError('WINDOW_RECOVERY_UNPROVEN', '历史代际或schema不可证明')


def _history_trigger(table, event):
    # Names/operations come exclusively from the explicit trusted proof domain.
    return (f'CREATE TRIGGER history_proof_{table}_{event.lower()} BEFORE {event} ON {table} BEGIN '
        "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 "
        "AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 "
        "AND typeof(protocol_version)='integer' AND protocol_version=1 "
        "AND typeof(store_nonce)='text' AND length(store_nonce)=32) "
        "THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; "
        'UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END')


def _history_cover(db, schema='main'):
    """Only trusted schema producers call this in their original DDL transaction.

    Existing coverage is verified, never repaired. Newly-created optional proof
    tables receive local triggers before the same original transaction commits.
    This metadata is not a historical-success certificate.
    """
    if schema not in ('main','actor'):
        _history_failure()
    domain=_HISTORY_MAIN if schema=='main' else _HISTORY_PRIVATE
    found=db.execute(f"SELECT sql FROM {schema}.sqlite_master WHERE type='table' AND name='history_proof_epochs'").fetchone()
    if found is None:
        db.execute(_HISTORY_EPOCH_DDL.replace('CREATE TABLE ',f'CREATE TABLE {schema}.',1))
        db.execute(f'INSERT INTO {schema}.history_proof_epochs VALUES (1,1,?,0)',(uuid.uuid4().hex,))
        db.execute(_HISTORY_COVER_DDL.replace('CREATE TABLE ',f'CREATE TABLE {schema}.',1))
    _history_epoch(db,schema)
    for table in domain:
        actual=db.execute(f"SELECT sql FROM {schema}.sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
        covered=db.execute(f'SELECT table_sql FROM {schema}.history_proof_coverage WHERE table_name=?',(table,)).fetchone()
        if actual is None:
            if covered is not None:_history_failure()
            continue
        if covered is None:
            for event in ('INSERT','UPDATE','DELETE'):
                sql=_history_trigger(table,event)
                trigger='history_proof_'+table+'_'+event.lower()
                if db.execute(f'SELECT 1 FROM {schema}.sqlite_master WHERE name=?',(trigger,)).fetchone():
                    _history_failure()
                db.execute(sql.replace('CREATE TRIGGER ',f'CREATE TRIGGER {schema}.',1))
            db.execute(f'INSERT INTO {schema}.history_proof_coverage VALUES (?,?)',(table,actual[0]))
        else:
            if covered[0]!=actual[0]:_history_failure()
            _history_check_triggers(db,schema,table)


def _history_epoch(db,schema):
    for name,ddl in (('history_proof_epochs',_HISTORY_EPOCH_DDL),('history_proof_coverage',_HISTORY_COVER_DDL)):
        actual=db.execute(f'SELECT sql FROM {schema}.sqlite_master WHERE type=\'table\' AND name=?',(name,)).fetchone()
        if actual is None or actual[0]!=ddl:_history_failure()
    rows=db.execute(f'SELECT singleton,protocol_version,store_nonce,generation FROM {schema}.history_proof_epochs').fetchall()
    if len(rows)!=1:_history_failure()
    singleton,version,nonce,generation=tuple(rows[0])
    if (type(singleton) is not int or singleton!=1 or type(version) is not int or version!=1
            or type(nonce) is not str or len(nonce)!=32 or any(c not in '0123456789abcdef' for c in nonce)
            or type(generation) is not int or not 0<=generation<=9223372036854775807):
        _history_failure()
    return (nonce,generation)


def _history_check_triggers(db,schema,table):
    for event in ('INSERT','UPDATE','DELETE'):
        expected=_history_trigger(table,event)
        name='history_proof_'+table+'_'+event.lower()
        actual=db.execute(f'SELECT sql FROM {schema}.sqlite_master WHERE type=\'trigger\' AND name=?',(name,)).fetchone()
        if actual is None or actual[0]!=expected:_history_failure()


# Fixed producer declarations compiled in native SQLite during generation.
# Not captured from a consumer database; optional table absence is retained.
_HISTORY_SCHEMA_CONTRACT = {'main': (('index', 'coordinator_one_open_window', 'coordinator_windows', "CREATE UNIQUE INDEX coordinator_one_open_window ON coordinator_windows(world_id) WHERE state='open'"), ('index', 'sqlite_autoindex_actor_activation_delegations_1', 'actor_activation_delegations', None), ('index', 'sqlite_autoindex_actor_coordinator_delegations_1', 'actor_coordinator_delegations', None), ('index', 'sqlite_autoindex_actor_execution_leases_1', 'actor_execution_leases', None), ('index', 'sqlite_autoindex_coordinator_activation_blocks_1', 'coordinator_activation_blocks', None), ('index', 'sqlite_autoindex_coordinator_activation_outcomes_1', 'coordinator_activation_outcomes', None), ('index', 'sqlite_autoindex_coordinator_activation_outcomes_2', 'coordinator_activation_outcomes', None), ('index', 'sqlite_autoindex_coordinator_activations_1', 'coordinator_activations', None), ('index', 'sqlite_autoindex_coordinator_activations_2', 'coordinator_activations', None), ('index', 'sqlite_autoindex_coordinator_activations_3', 'coordinator_activations', None), ('index', 'sqlite_autoindex_coordinator_activations_4', 'coordinator_activations', None), ('index', 'sqlite_autoindex_coordinator_execution_leases_1', 'coordinator_execution_leases', None), ('index', 'sqlite_autoindex_coordinator_fault_issuers_1', 'coordinator_fault_issuers', None), ('index', 'sqlite_autoindex_coordinator_scheduler_registration_1', 'coordinator_scheduler_registration', None), ('index', 'sqlite_autoindex_coordinator_technical_maintenance_1', 'coordinator_technical_maintenance', None), ('index', 'sqlite_autoindex_coordinator_technical_maintenance_2', 'coordinator_technical_maintenance', None), ('index', 'sqlite_autoindex_coordinator_technical_window_releases_1', 'coordinator_technical_window_releases', None), ('index', 'sqlite_autoindex_coordinator_technical_window_releases_2', 'coordinator_technical_window_releases', None), ('index', 'sqlite_autoindex_coordinator_windows_1', 'coordinator_windows', None), ('index', 'sqlite_autoindex_coordinator_windows_2', 'coordinator_windows', None), ('index', 'sqlite_autoindex_dm_decisions_1', 'dm_decisions', None), ('index', 'sqlite_autoindex_dm_proposals_1', 'dm_proposals', None), ('index', 'sqlite_autoindex_dm_timeline_1', 'dm_timeline', None), ('index', 'sqlite_autoindex_history_proof_coverage_1', 'history_proof_coverage', None), ('index', 'sqlite_autoindex_intents_1', 'intents', None), ('index', 'sqlite_autoindex_scheduler_genesis_1', 'scheduler_genesis', None), ('index', 'sqlite_autoindex_world_constants_1', 'world_constants', None), ('table', 'actor_activation_delegations', 'actor_activation_delegations', 'CREATE TABLE actor_activation_delegations (\n        world_id TEXT NOT NULL,person_id TEXT NOT NULL,actor_epoch INTEGER NOT NULL,\n        window_id TEXT NOT NULL,activation_id TEXT NOT NULL,PRIMARY KEY(world_id,person_id,actor_epoch))'), ('table', 'actor_coordinator_delegations', 'actor_coordinator_delegations', 'CREATE TABLE actor_coordinator_delegations (world_id TEXT NOT NULL, person_id TEXT NOT NULL, actor_epoch INTEGER NOT NULL, parent_epoch INTEGER NOT NULL, parent_owner_token TEXT NOT NULL, PRIMARY KEY(world_id,person_id,actor_epoch))'), ('table', 'actor_execution_leases', 'actor_execution_leases', 'CREATE TABLE actor_execution_leases (world_id TEXT NOT NULL, person_id TEXT NOT NULL, epoch INTEGER NOT NULL, owner_token TEXT, owner_instance TEXT NOT NULL, acquired_wall_ns INTEGER NOT NULL, expires_wall_ns INTEGER NOT NULL, last_wall_ns INTEGER NOT NULL, PRIMARY KEY(world_id,person_id))'), ('table', 'coordinator_activation_blocks', 'coordinator_activation_blocks', "CREATE TABLE coordinator_activation_blocks (\n        world_id TEXT NOT NULL, activation_id TEXT NOT NULL,\n        resume_phase TEXT NOT NULL CHECK(resume_phase IN ('pending','prepared','candidate','world_committed','rejected')),\n        diagnostic_code TEXT NOT NULL, pause_revision INTEGER NOT NULL CHECK(pause_revision>0),\n        proof_json TEXT NOT NULL, proof_sha256 TEXT NOT NULL, PRIMARY KEY(world_id,activation_id))"), ('table', 'coordinator_activation_outcomes', 'coordinator_activation_outcomes', "CREATE TABLE coordinator_activation_outcomes (\n        world_id TEXT NOT NULL,activation_id TEXT NOT NULL,intent_id TEXT NOT NULL,\n        kind TEXT NOT NULL CHECK(kind IN ('committed','domain_rejected')),outcome_json TEXT NOT NULL,\n        outcome_sha256 TEXT NOT NULL,PRIMARY KEY(world_id,activation_id),UNIQUE(world_id,intent_id))"), ('table', 'coordinator_activations', 'coordinator_activations', "CREATE TABLE coordinator_activations (\n        world_id TEXT NOT NULL, window_id TEXT NOT NULL, activation_id TEXT NOT NULL,\n        roster_index INTEGER NOT NULL CHECK(roster_index>=0), person_id TEXT NOT NULL,\n        phase TEXT NOT NULL CHECK(phase IN ('pending','prepared','candidate','world_committed','completed','technical_blocked','rejected','skipped')),\n        intent_id TEXT, observation_revision INTEGER, card_version INTEGER,\n        base_session_version INTEGER, request_sha256 TEXT, candidate_sha256 TEXT,\n        receipt_sha256 TEXT, promoted_version INTEGER, terminal_reason TEXT,\n        private_closed INTEGER NOT NULL DEFAULT 0 CHECK(private_closed IN (0,1)),\n        resume_phase TEXT, diagnostic_code TEXT,\n        created_parent_epoch INTEGER NOT NULL CHECK(created_parent_epoch>0), owner_parent_epoch INTEGER NOT NULL CHECK(owner_parent_epoch>0),\n        owner_actor_epoch INTEGER,\n        PRIMARY KEY(world_id,activation_id), UNIQUE(world_id,window_id,person_id),\n        UNIQUE(world_id,window_id,roster_index), UNIQUE(world_id,intent_id))"), ('table', 'coordinator_execution_leases', 'coordinator_execution_leases', 'CREATE TABLE coordinator_execution_leases (world_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, owner_token TEXT, owner_instance TEXT NOT NULL, acquired_wall_ns INTEGER NOT NULL, expires_wall_ns INTEGER NOT NULL, last_wall_ns INTEGER NOT NULL, schema_version INTEGER NOT NULL)'), ('table', 'coordinator_fault_issuers', 'coordinator_fault_issuers', 'CREATE TABLE coordinator_fault_issuers (\n    world_id TEXT NOT NULL, activation_id TEXT NOT NULL,\n    witness_json TEXT NOT NULL, witness_sha256 TEXT NOT NULL,\n    PRIMARY KEY(world_id,activation_id))'), ('table', 'coordinator_scheduler_registration', 'coordinator_scheduler_registration', "CREATE TABLE coordinator_scheduler_registration (\n        world_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version=1),\n        boundary_kind TEXT NOT NULL CHECK(boundary_kind='pristine'), boundary_revision INTEGER NOT NULL CHECK(boundary_revision>0),\n        boundary_time INTEGER NOT NULL CHECK(boundary_time=0), evidence_json TEXT NOT NULL,\n        evidence_sha256 TEXT NOT NULL, created_parent_epoch INTEGER NOT NULL CHECK(created_parent_epoch>0),\n        private_identity_json TEXT NOT NULL)"), ('table', 'coordinator_technical_maintenance', 'coordinator_technical_maintenance', 'CREATE TABLE coordinator_technical_maintenance (\n    operation_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, activation_id TEXT NOT NULL UNIQUE,\n    record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version=1))'), ('table', 'coordinator_technical_window_releases', 'coordinator_technical_window_releases', 'CREATE TABLE coordinator_technical_window_releases (\n    operation_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, window_id TEXT NOT NULL,\n    record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL,\n    schema_version INTEGER NOT NULL CHECK(schema_version=1), UNIQUE(world_id,window_id))'), ('table', 'coordinator_windows', 'coordinator_windows', "CREATE TABLE coordinator_windows (\n        world_id TEXT NOT NULL, window_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal>0),\n        base_revision INTEGER NOT NULL CHECK(base_revision>0), target_time INTEGER NOT NULL CHECK(target_time>=0),\n        horizon_at_open INTEGER NOT NULL CHECK(horizon_at_open>=target_time), advanced_revision INTEGER NOT NULL CHECK(advanced_revision>0),\n        state TEXT NOT NULL CHECK(state IN ('open','completed')),\n        boundary_kind TEXT NOT NULL CHECK(boundary_kind IN ('normal','horizon')),\n        roster_json TEXT NOT NULL, roster_sha256 TEXT NOT NULL,\n        created_parent_epoch INTEGER NOT NULL CHECK(created_parent_epoch>0), owner_parent_epoch INTEGER NOT NULL CHECK(owner_parent_epoch>0),\n        completed_revision INTEGER,\n        PRIMARY KEY(world_id,window_id), UNIQUE(world_id,ordinal))"), ('table', 'dm_decisions', 'dm_decisions', 'CREATE TABLE dm_decisions (intent_id TEXT NOT NULL, version INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY(intent_id,version))'), ('table', 'dm_proposals', 'dm_proposals', 'CREATE TABLE dm_proposals (intent_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, person_id TEXT NOT NULL, text TEXT NOT NULL)'), ('table', 'dm_timeline', 'dm_timeline', 'CREATE TABLE dm_timeline (intent_id TEXT NOT NULL, kind TEXT NOT NULL, version INTEGER NOT NULL, visible_from INTEGER NOT NULL, PRIMARY KEY(intent_id,kind,version))'), ('table', 'execution_clock', 'execution_clock', 'CREATE TABLE execution_clock (singleton INTEGER PRIMARY KEY CHECK(singleton=1), high_water_wall_ns INTEGER NOT NULL)'), ('table', 'history_proof_coverage', 'history_proof_coverage', 'CREATE TABLE history_proof_coverage (table_name TEXT PRIMARY KEY, table_sql TEXT NOT NULL)'), ('table', 'history_proof_epochs', 'history_proof_epochs', "CREATE TABLE history_proof_epochs (singleton INTEGER PRIMARY KEY CHECK(singleton=1), protocol_version INTEGER NOT NULL CHECK(typeof(protocol_version)='integer' AND protocol_version=1), store_nonce TEXT NOT NULL CHECK(typeof(store_nonce)='text' AND length(store_nonce)=32), generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=0))"), ('table', 'intents', 'intents', 'CREATE TABLE intents (id TEXT PRIMARY KEY, person TEXT NOT NULL, payload TEXT NOT NULL, receipt TEXT)'), ('table', 'scheduler_genesis', 'scheduler_genesis', 'CREATE TABLE scheduler_genesis (world_id TEXT PRIMARY KEY, schema_version INTEGER NOT NULL CHECK(schema_version=1), initial_revision INTEGER NOT NULL CHECK(initial_revision=1), initial_sha256 TEXT NOT NULL)'), ('table', 'snapshots', 'snapshots', 'CREATE TABLE snapshots (seq INTEGER PRIMARY KEY, state TEXT NOT NULL)'), ('table', 'world_constants', 'world_constants', 'CREATE TABLE world_constants (key TEXT PRIMARY KEY, value TEXT NOT NULL, sha256 TEXT NOT NULL)'), ('trigger', 'constants_no_delete', 'world_constants', "CREATE TRIGGER constants_no_delete BEFORE DELETE ON world_constants BEGIN SELECT RAISE(ABORT, 'immutable world constants'); END"), ('trigger', 'constants_no_update', 'world_constants', "CREATE TRIGGER constants_no_update BEFORE UPDATE ON world_constants BEGIN SELECT RAISE(ABORT, 'immutable world constants'); END"), ('trigger', 'dm_decisions_no_delete', 'dm_decisions', "CREATE TRIGGER dm_decisions_no_delete BEFORE DELETE ON dm_decisions BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END"), ('trigger', 'dm_decisions_no_update', 'dm_decisions', "CREATE TRIGGER dm_decisions_no_update BEFORE UPDATE ON dm_decisions BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END"), ('trigger', 'dm_proposals_no_delete', 'dm_proposals', "CREATE TRIGGER dm_proposals_no_delete BEFORE DELETE ON dm_proposals BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END"), ('trigger', 'dm_proposals_no_update', 'dm_proposals', "CREATE TRIGGER dm_proposals_no_update BEFORE UPDATE ON dm_proposals BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END"), ('trigger', 'dm_timeline_no_delete', 'dm_timeline', "CREATE TRIGGER dm_timeline_no_delete BEFORE DELETE ON dm_timeline BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END"), ('trigger', 'dm_timeline_no_update', 'dm_timeline', "CREATE TRIGGER dm_timeline_no_update BEFORE UPDATE ON dm_timeline BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END"), ('trigger', 'fault_issuer_no_delete', 'coordinator_fault_issuers', "CREATE TRIGGER fault_issuer_no_delete BEFORE DELETE ON coordinator_fault_issuers BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"), ('trigger', 'fault_issuer_no_replace', 'coordinator_fault_issuers', "CREATE TRIGGER fault_issuer_no_replace BEFORE INSERT ON coordinator_fault_issuers WHEN EXISTS (SELECT 1 FROM coordinator_fault_issuers WHERE world_id=NEW.world_id AND activation_id=NEW.activation_id) BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"), ('trigger', 'fault_issuer_no_update', 'coordinator_fault_issuers', "CREATE TRIGGER fault_issuer_no_update BEFORE UPDATE ON coordinator_fault_issuers BEGIN SELECT RAISE(ABORT, 'immutable fault issuer'); END"), ('trigger', 'history_proof_actor_activation_delegations_delete', 'actor_activation_delegations', "CREATE TRIGGER history_proof_actor_activation_delegations_delete BEFORE DELETE ON actor_activation_delegations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_activation_delegations_insert', 'actor_activation_delegations', "CREATE TRIGGER history_proof_actor_activation_delegations_insert BEFORE INSERT ON actor_activation_delegations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_activation_delegations_update', 'actor_activation_delegations', "CREATE TRIGGER history_proof_actor_activation_delegations_update BEFORE UPDATE ON actor_activation_delegations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_coordinator_delegations_delete', 'actor_coordinator_delegations', "CREATE TRIGGER history_proof_actor_coordinator_delegations_delete BEFORE DELETE ON actor_coordinator_delegations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_coordinator_delegations_insert', 'actor_coordinator_delegations', "CREATE TRIGGER history_proof_actor_coordinator_delegations_insert BEFORE INSERT ON actor_coordinator_delegations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_coordinator_delegations_update', 'actor_coordinator_delegations', "CREATE TRIGGER history_proof_actor_coordinator_delegations_update BEFORE UPDATE ON actor_coordinator_delegations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activation_blocks_delete', 'coordinator_activation_blocks', "CREATE TRIGGER history_proof_coordinator_activation_blocks_delete BEFORE DELETE ON coordinator_activation_blocks BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activation_blocks_insert', 'coordinator_activation_blocks', "CREATE TRIGGER history_proof_coordinator_activation_blocks_insert BEFORE INSERT ON coordinator_activation_blocks BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activation_blocks_update', 'coordinator_activation_blocks', "CREATE TRIGGER history_proof_coordinator_activation_blocks_update BEFORE UPDATE ON coordinator_activation_blocks BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activation_outcomes_delete', 'coordinator_activation_outcomes', "CREATE TRIGGER history_proof_coordinator_activation_outcomes_delete BEFORE DELETE ON coordinator_activation_outcomes BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activation_outcomes_insert', 'coordinator_activation_outcomes', "CREATE TRIGGER history_proof_coordinator_activation_outcomes_insert BEFORE INSERT ON coordinator_activation_outcomes BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activation_outcomes_update', 'coordinator_activation_outcomes', "CREATE TRIGGER history_proof_coordinator_activation_outcomes_update BEFORE UPDATE ON coordinator_activation_outcomes BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activations_delete', 'coordinator_activations', "CREATE TRIGGER history_proof_coordinator_activations_delete BEFORE DELETE ON coordinator_activations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activations_insert', 'coordinator_activations', "CREATE TRIGGER history_proof_coordinator_activations_insert BEFORE INSERT ON coordinator_activations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_activations_update', 'coordinator_activations', "CREATE TRIGGER history_proof_coordinator_activations_update BEFORE UPDATE ON coordinator_activations BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_fault_issuers_delete', 'coordinator_fault_issuers', "CREATE TRIGGER history_proof_coordinator_fault_issuers_delete BEFORE DELETE ON coordinator_fault_issuers BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_fault_issuers_insert', 'coordinator_fault_issuers', "CREATE TRIGGER history_proof_coordinator_fault_issuers_insert BEFORE INSERT ON coordinator_fault_issuers BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_fault_issuers_update', 'coordinator_fault_issuers', "CREATE TRIGGER history_proof_coordinator_fault_issuers_update BEFORE UPDATE ON coordinator_fault_issuers BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_scheduler_registration_delete', 'coordinator_scheduler_registration', "CREATE TRIGGER history_proof_coordinator_scheduler_registration_delete BEFORE DELETE ON coordinator_scheduler_registration BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_scheduler_registration_insert', 'coordinator_scheduler_registration', "CREATE TRIGGER history_proof_coordinator_scheduler_registration_insert BEFORE INSERT ON coordinator_scheduler_registration BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_scheduler_registration_update', 'coordinator_scheduler_registration', "CREATE TRIGGER history_proof_coordinator_scheduler_registration_update BEFORE UPDATE ON coordinator_scheduler_registration BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_technical_maintenance_delete', 'coordinator_technical_maintenance', "CREATE TRIGGER history_proof_coordinator_technical_maintenance_delete BEFORE DELETE ON coordinator_technical_maintenance BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_technical_maintenance_insert', 'coordinator_technical_maintenance', "CREATE TRIGGER history_proof_coordinator_technical_maintenance_insert BEFORE INSERT ON coordinator_technical_maintenance BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_technical_maintenance_update', 'coordinator_technical_maintenance', "CREATE TRIGGER history_proof_coordinator_technical_maintenance_update BEFORE UPDATE ON coordinator_technical_maintenance BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_technical_window_releases_delete', 'coordinator_technical_window_releases', "CREATE TRIGGER history_proof_coordinator_technical_window_releases_delete BEFORE DELETE ON coordinator_technical_window_releases BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_technical_window_releases_insert', 'coordinator_technical_window_releases', "CREATE TRIGGER history_proof_coordinator_technical_window_releases_insert BEFORE INSERT ON coordinator_technical_window_releases BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_technical_window_releases_update', 'coordinator_technical_window_releases', "CREATE TRIGGER history_proof_coordinator_technical_window_releases_update BEFORE UPDATE ON coordinator_technical_window_releases BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_windows_delete', 'coordinator_windows', "CREATE TRIGGER history_proof_coordinator_windows_delete BEFORE DELETE ON coordinator_windows BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_windows_insert', 'coordinator_windows', "CREATE TRIGGER history_proof_coordinator_windows_insert BEFORE INSERT ON coordinator_windows BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_coordinator_windows_update', 'coordinator_windows', "CREATE TRIGGER history_proof_coordinator_windows_update BEFORE UPDATE ON coordinator_windows BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_decisions_delete', 'dm_decisions', "CREATE TRIGGER history_proof_dm_decisions_delete BEFORE DELETE ON dm_decisions BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_decisions_insert', 'dm_decisions', "CREATE TRIGGER history_proof_dm_decisions_insert BEFORE INSERT ON dm_decisions BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_decisions_update', 'dm_decisions', "CREATE TRIGGER history_proof_dm_decisions_update BEFORE UPDATE ON dm_decisions BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_proposals_delete', 'dm_proposals', "CREATE TRIGGER history_proof_dm_proposals_delete BEFORE DELETE ON dm_proposals BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_proposals_insert', 'dm_proposals', "CREATE TRIGGER history_proof_dm_proposals_insert BEFORE INSERT ON dm_proposals BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_proposals_update', 'dm_proposals', "CREATE TRIGGER history_proof_dm_proposals_update BEFORE UPDATE ON dm_proposals BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_timeline_delete', 'dm_timeline', "CREATE TRIGGER history_proof_dm_timeline_delete BEFORE DELETE ON dm_timeline BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_timeline_insert', 'dm_timeline', "CREATE TRIGGER history_proof_dm_timeline_insert BEFORE INSERT ON dm_timeline BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_dm_timeline_update', 'dm_timeline', "CREATE TRIGGER history_proof_dm_timeline_update BEFORE UPDATE ON dm_timeline BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_intents_delete', 'intents', "CREATE TRIGGER history_proof_intents_delete BEFORE DELETE ON intents BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_intents_insert', 'intents', "CREATE TRIGGER history_proof_intents_insert BEFORE INSERT ON intents BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_intents_update', 'intents', "CREATE TRIGGER history_proof_intents_update BEFORE UPDATE ON intents BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_scheduler_genesis_delete', 'scheduler_genesis', "CREATE TRIGGER history_proof_scheduler_genesis_delete BEFORE DELETE ON scheduler_genesis BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_scheduler_genesis_insert', 'scheduler_genesis', "CREATE TRIGGER history_proof_scheduler_genesis_insert BEFORE INSERT ON scheduler_genesis BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_scheduler_genesis_update', 'scheduler_genesis', "CREATE TRIGGER history_proof_scheduler_genesis_update BEFORE UPDATE ON scheduler_genesis BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_snapshots_delete', 'snapshots', "CREATE TRIGGER history_proof_snapshots_delete BEFORE DELETE ON snapshots BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_snapshots_insert', 'snapshots', "CREATE TRIGGER history_proof_snapshots_insert BEFORE INSERT ON snapshots BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_snapshots_update', 'snapshots', "CREATE TRIGGER history_proof_snapshots_update BEFORE UPDATE ON snapshots BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_world_constants_delete', 'world_constants', "CREATE TRIGGER history_proof_world_constants_delete BEFORE DELETE ON world_constants BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_world_constants_insert', 'world_constants', "CREATE TRIGGER history_proof_world_constants_insert BEFORE INSERT ON world_constants BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_world_constants_update', 'world_constants', "CREATE TRIGGER history_proof_world_constants_update BEFORE UPDATE ON world_constants BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'snapshots_no_delete', 'snapshots', "CREATE TRIGGER snapshots_no_delete BEFORE DELETE ON snapshots BEGIN SELECT RAISE(ABORT, 'immutable snapshots'); END"), ('trigger', 'snapshots_no_update', 'snapshots', "CREATE TRIGGER snapshots_no_update BEFORE UPDATE ON snapshots BEGIN SELECT RAISE(ABORT, 'immutable snapshots'); END"), ('trigger', 'technical_maintenance_no_delete', 'coordinator_technical_maintenance', "CREATE TRIGGER technical_maintenance_no_delete BEFORE DELETE ON coordinator_technical_maintenance BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"), ('trigger', 'technical_maintenance_no_replace', 'coordinator_technical_maintenance', "CREATE TRIGGER technical_maintenance_no_replace BEFORE INSERT ON coordinator_technical_maintenance WHEN EXISTS (SELECT 1 FROM coordinator_technical_maintenance WHERE operation_id=NEW.operation_id OR activation_id=NEW.activation_id) BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"), ('trigger', 'technical_maintenance_no_update', 'coordinator_technical_maintenance', "CREATE TRIGGER technical_maintenance_no_update BEFORE UPDATE ON coordinator_technical_maintenance BEGIN SELECT RAISE(ABORT, 'immutable technical maintenance'); END"), ('trigger', 'technical_window_release_no_delete', 'coordinator_technical_window_releases', "CREATE TRIGGER technical_window_release_no_delete BEFORE DELETE ON coordinator_technical_window_releases BEGIN SELECT RAISE(ABORT, 'immutable technical window release'); END"), ('trigger', 'technical_window_release_no_replace', 'coordinator_technical_window_releases', "CREATE TRIGGER technical_window_release_no_replace BEFORE INSERT ON coordinator_technical_window_releases WHEN EXISTS (SELECT 1 FROM coordinator_technical_window_releases WHERE operation_id=NEW.operation_id OR (world_id=NEW.world_id AND window_id=NEW.window_id)) BEGIN SELECT RAISE(ABORT, 'immutable technical window release'); END"), ('trigger', 'technical_window_release_no_update', 'coordinator_technical_window_releases', "CREATE TRIGGER technical_window_release_no_update BEFORE UPDATE ON coordinator_technical_window_releases BEGIN SELECT RAISE(ABORT, 'immutable technical window release'); END")), 'actor': (('index', 'sqlite_autoindex_actor_calls_1', 'actor_calls', None), ('index', 'sqlite_autoindex_actor_intent_execution_1', 'actor_intent_execution', None), ('index', 'sqlite_autoindex_actor_intents_1', 'actor_intents', None), ('index', 'sqlite_autoindex_actor_session_heads_1', 'actor_session_heads', None), ('index', 'sqlite_autoindex_actor_sessions_1', 'actor_sessions', None), ('index', 'sqlite_autoindex_history_proof_coverage_1', 'history_proof_coverage', None), ('table', 'actor_calls', 'actor_calls', 'CREATE TABLE actor_calls (\n                    call_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL,\n                    person_id TEXT NOT NULL)'), ('table', 'actor_intent_execution', 'actor_intent_execution', 'CREATE TABLE actor_intent_execution (\n                intent_id TEXT PRIMARY KEY, person_id TEXT NOT NULL, world_id TEXT NOT NULL,\n                base_version INTEGER NOT NULL, base_messages_sha256 TEXT NOT NULL, base_seen_sha256 TEXT NOT NULL,\n                owner_epoch INTEGER NOT NULL, owner_token TEXT NOT NULL, attempt_id TEXT NOT NULL,\n                promoted_version INTEGER)'), ('table', 'actor_intents', 'actor_intents', "CREATE TABLE actor_intents (\n                    intent_id TEXT PRIMARY KEY, person_id TEXT NOT NULL,\n                    body TEXT NOT NULL, state TEXT NOT NULL,\n                    candidate TEXT, metrics TEXT, receipt TEXT,\n                    consumed TEXT NOT NULL DEFAULT '[]', request TEXT)"), ('table', 'actor_session_heads', 'actor_session_heads', 'CREATE TABLE actor_session_heads (\n                person_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, version INTEGER NOT NULL,\n                messages_sha256 TEXT NOT NULL, seen_sha256 TEXT NOT NULL,\n                last_promoted_intent_id TEXT, migration_kind TEXT NOT NULL, proof_digest TEXT NOT NULL,\n                original_messages TEXT, original_seen TEXT)'), ('table', 'actor_sessions', 'actor_sessions', "CREATE TABLE actor_sessions (\n                    person_id TEXT PRIMARY KEY, messages TEXT NOT NULL DEFAULT '[]',\n                    seen TEXT NOT NULL DEFAULT '[]')"), ('table', 'actor_world_binding', 'actor_world_binding', 'CREATE TABLE actor_world_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), world_id TEXT NOT NULL)'), ('table', 'history_proof_coverage', 'history_proof_coverage', 'CREATE TABLE history_proof_coverage (table_name TEXT PRIMARY KEY, table_sql TEXT NOT NULL)'), ('table', 'history_proof_epochs', 'history_proof_epochs', "CREATE TABLE history_proof_epochs (singleton INTEGER PRIMARY KEY CHECK(singleton=1), protocol_version INTEGER NOT NULL CHECK(typeof(protocol_version)='integer' AND protocol_version=1), store_nonce TEXT NOT NULL CHECK(typeof(store_nonce)='text' AND length(store_nonce)=32), generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=0))"), ('trigger', 'history_proof_actor_calls_delete', 'actor_calls', "CREATE TRIGGER history_proof_actor_calls_delete BEFORE DELETE ON actor_calls BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_calls_insert', 'actor_calls', "CREATE TRIGGER history_proof_actor_calls_insert BEFORE INSERT ON actor_calls BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_calls_update', 'actor_calls', "CREATE TRIGGER history_proof_actor_calls_update BEFORE UPDATE ON actor_calls BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_intent_execution_delete', 'actor_intent_execution', "CREATE TRIGGER history_proof_actor_intent_execution_delete BEFORE DELETE ON actor_intent_execution BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_intent_execution_insert', 'actor_intent_execution', "CREATE TRIGGER history_proof_actor_intent_execution_insert BEFORE INSERT ON actor_intent_execution BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_intent_execution_update', 'actor_intent_execution', "CREATE TRIGGER history_proof_actor_intent_execution_update BEFORE UPDATE ON actor_intent_execution BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_intents_delete', 'actor_intents', "CREATE TRIGGER history_proof_actor_intents_delete BEFORE DELETE ON actor_intents BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_intents_insert', 'actor_intents', "CREATE TRIGGER history_proof_actor_intents_insert BEFORE INSERT ON actor_intents BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_intents_update', 'actor_intents', "CREATE TRIGGER history_proof_actor_intents_update BEFORE UPDATE ON actor_intents BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_session_heads_delete', 'actor_session_heads', "CREATE TRIGGER history_proof_actor_session_heads_delete BEFORE DELETE ON actor_session_heads BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_session_heads_insert', 'actor_session_heads', "CREATE TRIGGER history_proof_actor_session_heads_insert BEFORE INSERT ON actor_session_heads BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_session_heads_update', 'actor_session_heads', "CREATE TRIGGER history_proof_actor_session_heads_update BEFORE UPDATE ON actor_session_heads BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_sessions_delete', 'actor_sessions', "CREATE TRIGGER history_proof_actor_sessions_delete BEFORE DELETE ON actor_sessions BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_sessions_insert', 'actor_sessions', "CREATE TRIGGER history_proof_actor_sessions_insert BEFORE INSERT ON actor_sessions BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_sessions_update', 'actor_sessions', "CREATE TRIGGER history_proof_actor_sessions_update BEFORE UPDATE ON actor_sessions BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_world_binding_delete', 'actor_world_binding', "CREATE TRIGGER history_proof_actor_world_binding_delete BEFORE DELETE ON actor_world_binding BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_world_binding_insert', 'actor_world_binding', "CREATE TRIGGER history_proof_actor_world_binding_insert BEFORE INSERT ON actor_world_binding BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"), ('trigger', 'history_proof_actor_world_binding_update', 'actor_world_binding', "CREATE TRIGGER history_proof_actor_world_binding_update BEFORE UPDATE ON actor_world_binding BEGIN SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM history_proof_epochs WHERE singleton=1 AND typeof(generation)='integer' AND generation>=0 AND generation<9223372036854775807 AND typeof(protocol_version)='integer' AND protocol_version=1 AND typeof(store_nonce)='text' AND length(store_nonce)=32) THEN RAISE(ABORT, 'history proof generation invalid or exhausted') END; UPDATE history_proof_epochs SET generation=generation+1 WHERE singleton=1; END"))}


def _history_schema_check(db, schema, catalog):
    expected = {row[1]:row for row in _HISTORY_SCHEMA_CONTRACT[schema]}
    present = {row[1] for row in catalog if row[0]=='table'}
    actual = {row[1]:row for row in catalog}
    for row in catalog:
        reference = expected.get(row[1])
        if row == reference:
            continue
        # Preserve the pre-existing legal formatting/predicate grammar for
        # this known index; its native uniqueness/column shape is still checked.
        if (schema=='main' and row[:3]==('index','coordinator_one_open_window','coordinator_windows')
                and reference is not None):
            from .coordinator_scheduler import one_open_predicate
            indexes = {r[1]:r for r in db.execute('PRAGMA main.index_list(coordinator_windows)')}
            index = indexes.get(row[1])
            if (one_open_predicate(row[3]) and index is not None and index[2]==1 and index[4]==1
                    and [r[2] for r in db.execute('PRAGMA main.index_info(coordinator_one_open_window)')]==['world_id']):
                continue
        _history_failure()
    for name, row in expected.items():
        # Original diagnostic legacy issuer evidence explicitly permits the
        # old two-trigger form; authorization callers retain the stronger gate.
        if name=='fault_issuer_no_replace':
            continue
        if row[2] in present and name not in actual:
            _history_failure()


def _history_witness(db,schema):
    """Actually pins each local schema/data snapshot; exact optional absence."""
    if schema not in ('main','actor'):_history_failure()
    epoch=_history_epoch(db,schema)
    domain=_HISTORY_MAIN if schema=='main' else _HISTORY_PRIVATE
    coverage=db.execute(f'SELECT table_name,table_sql FROM {schema}.history_proof_coverage ORDER BY table_name').fetchall()
    cover={r[0]:r[1] for r in coverage}
    actual={}
    for table in domain:
        row=db.execute(f'SELECT sql FROM {schema}.sqlite_master WHERE type=\'table\' AND name=?',(table,)).fetchone()
        actual[table]=None if row is None else row[0]
        if row is None:
            if table in cover:_history_failure()
        else:
            if cover.get(table)!=row[0]:_history_failure()
            _history_check_triggers(db,schema,table)
    if set(cover)!={t for t,v in actual.items() if v is not None}:_history_failure()
    cookie=db.execute(f'PRAGMA {schema}.schema_version').fetchone()[0]
    if type(cookie) is not int or cookie<0:_history_failure()
    # Whole catalog includes all index predicates, immutable guards and optional
    # schemas. Compare the actual SQL text, not names or a guessed schema hash.
    catalog=tuple(tuple(r) for r in db.execute(f'SELECT type,name,tbl_name,sql FROM {schema}.sqlite_master ORDER BY type,name'))
    _history_schema_check(db,schema,catalog)
    return (epoch,cookie,tuple(sorted(actual.items())),catalog)


def _lc_iter_loans(read, table, equality=(), columns=None, order=()):
    """Only the already source-authenticated reader can acquire native Rows."""
    memory = _lc_value_memory(read)
    if memory is None:_lc_fail('stream须原生受证reader')
    from . import coordinator_scheduler as scheduler
    from .source_attestation import _REGISTRY
    namespace=scheduler.__dict__;stamp=_REGISTRY.get(id(namespace))
    if stamp is None or stamp['namespace'] is not namespace or not stamp['matched']:_lc_fail('stream源码未受证')
    if type(read) is scheduler._LifecycleNativeReader:
        for name in ('iter_borrows','_stream_query'):
            function=getattr(type(read),name,None)
            if (getattr(getattr(read,name,None),'__func__',None) is not function
                    or getattr(function,'__globals__',None) is not namespace
                    or not scheduler._history_original_method(function,stamp)):_lc_fail('stream原方法绑定变更')
        return read.iter_borrows(table, equality, columns, order)
    function=scheduler._c0_iter_borrows
    if function.__globals__ is not namespace or not scheduler._history_original_method(function,stamp):_lc_fail('material stream原方法绑定变更')
    if table not in read.material['tables']:
        read.material['check']();read.material['check']()
        return (loan for loan in ())
    return function(read.material,table,equality,columns,order)


def _lc_each(read, table, equality, callback, order=()):
    """One original Row plus one callback frame at a time; no escaping aliases."""
    from .coordinator_scheduler import _LifecycleValueOwner, _lifecycle_value_context
    memory = _lc_value_memory(read)
    iterator = _lc_iter_loans(read, table, equality, order=order)
    loan = region = row = None
    try:
        for loan in iterator:
            region = _LifecycleValueOwner(memory)
            region.reserve(8192,'stream-callback-frame-and-scalar-scratch')
            token = _lifecycle_value_context.set(region)
            try:
                row = loan.value
                callback(row)
            except BaseException:
                loan.fail();region.fail()
                raise
            finally:
                _lifecycle_value_context.reset(token)
            row = None
            region.close();region = None
            loan.close();loan = None
    except BaseException:
        if loan is not None:loan.fail()
        if region is not None:region.fail()
        raise
    finally:
        # Closing a cursor/iterator is physical cleanup, not an accounting refund.
        row = None
        _lc_close_iterator(iterator)


def _lc_count(read, table, equality=()):
    count = 0
    def visit(row):
        nonlocal count
        count += 1
    _lc_each(read, table, equality, visit)
    return count


def _lc_select(read, table, equality, predicate, message):
    """Census then bounded original-key reread; never suspend a material loan.

    The material iterator requires close-before-next and close-before-close.
    The only retained selection is a prepaid key scalar, not an original Row.
    """
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_copy_borrow
    owner=_lifecycle_value_context.get()
    memory=_lc_value_memory(read)
    if owner is None or owner.memory is not memory:_lc_fail('选择缺同账上层owner')
    owner.reserve(2048,'stream-selection-key-carrier')
    fields=('advanced_revision',) if table=='main.coordinator_windows' else ('intent_id',)
    selected=None
    count=0
    def visit(row):
        nonlocal count,selected
        if predicate(row):
            count+=1
            key=_lifecycle_copy_borrow([row[name] for name in fields],memory=memory,role='stream-selected-key')
            selected=owner.take(key)
    _lc_each(read,table,equality,visit)
    if count!=1:_lc_fail(message)
    # All census Row aliases/frames have ended before this genuine acquisition.
    more=tuple((name,value) for name,value in zip(fields,selected) if name not in {k for k,v in equality})
    return _lc_read(read,table,equality+more,first=True)


def _lc_call_loan(read, function, *args, **kwargs):
    """After the full inner frame returns, export a separately prepaid result."""
    from .coordinator_scheduler import (_LifecycleValueOwner, _lifecycle_value_context,
                                        _lifecycle_copy_borrow)
    memory = _lc_value_memory(read)
    owner = _LifecycleValueOwner(memory)
    owner.reserve(16384,'world-stream-inner-frame-and-temporaries')
    token = _lifecycle_value_context.set(owner)
    result = None
    try:
        result = function(*args, **kwargs)
        loan = _lifecycle_copy_borrow(result, memory=memory, role='world-stream-result')
    except BaseException:
        owner.fail()
        raise
    finally:
        _lifecycle_value_context.reset(token)
    result = None
    owner.close()
    return loan


def _lc_private_step_owned(read, world_id, pid, version, messages, seen):
    from .actor_execution import body_matches
    meta = _lc_select(read, 'actor.actor_intent_execution', (('person_id',pid),),
        lambda m:type(m['promoted_version']) is int and m['promoted_version']==version+1,
        '私库版本spine断链')
    row = _lc_read(read, 'actor.actor_intents', (('person_id',pid),('intent_id',meta['intent_id'])), first=True)
    if row is None or row['state']!='committed' or row['candidate'] is None or row['receipt'] is None:_lc_fail()
    request=_lc_json(row['request']);body=_lc_json(row['body']);candidate=_lc_json(row['candidate']);consumed=_lc_json(row['consumed'])
    _lc_scratch(request,'private-body-matches-canonical-utf8-scratch')
    actual=_lc_read(read,'main.intents',(('id',row['intent_id']),),first=True)
    call = _lc_select(read, 'actor.actor_calls', (('person_id',pid),('intent_id',row['intent_id'])),
                      lambda c:True, '原私密spine未接回空基准')
    if (meta['world_id']!=world_id or type(meta['base_version']) is not int or meta['base_version']!=version
            or meta['base_messages_sha256']!=_lc_hash(messages) or meta['base_seen_sha256']!=_lc_hash(seen)
            or request['person_id']!=pid or request['intent_id']!=row['intent_id'] or request['messages']!=messages
            or not body_matches(request,body,candidate) or request.get('schema_version')!=3
            or call['call_id']!='real-'+row['intent_id']
            or type(consumed) is not list or consumed[:len(seen)]!=seen or len(set(consumed))!=len(consumed)
            or any(type(v) is not str for v in consumed)
            or actual is None or actual['person']!=pid or _lc_json(actual['payload'])['body']!=body
            or actual['receipt'] is None or _lc_json(actual['receipt'])!=_lc_json(row['receipt'])):_lc_fail('原私密spine未接回空基准')
    receipt=_lc_json(row['receipt'])
    if receipt.get('world_id')!=world_id or receipt.get('person_id')!=pid or receipt.get('intent_id')!=row['intent_id'] or receipt.get('status')!='committed':_lc_fail()
    return [candidate, consumed]


def _lc_private_base(read,world_id,pid,base_version):
    if _lc_value_memory(read) is None:
        return _lc_private_base_legacy(read,world_id,pid,base_version)
    from .coordinator_scheduler import _lifecycle_value_context
    outer = _lifecycle_value_context.get()
    if outer is None or outer.memory is not _lc_value_memory(read):_lc_fail('私密spine缺上层owner')
    outer.reserve(8192,'private-spine-frame-and-empty-basis')
    _lc_int(base_version)
    # Preserve the original discovery prefix and validate every original Row.
    _lc_count(read,'actor.actor_intents',(('person_id',pid),))
    meta_count = _lc_count(read,'actor.actor_intent_execution',(('person_id',pid),))
    if base_version>meta_count:_lc_fail('私库基准超出原spine')
    _lc_count(read,'actor.actor_calls',(('person_id',pid),))
    def duplicate(row):
        if _lc_count(read,'actor.actor_intents',(('person_id',pid),('intent_id',row['intent_id'])))!=1:
            _lc_fail('重复私库业务ID')
    _lc_each(read,'actor.actor_intents',(('person_id',pid),),duplicate)
    messages=[];seen=[];held=None;incoming=None
    try:
        for version in range(base_version):
            incoming = _lc_call_loan(read, _lc_private_step_owned, read, world_id, pid, version, messages, seen)
            messages,seen=incoming.value
            # The step's full frame and all its original Row aliases have gone.
            if held is not None:held.close()
            held,incoming=incoming,None
        if held is not None:
            outer.take(held)
        return messages,seen
    except BaseException:
        if held is not None:held.fail()
        if incoming is not None:incoming.fail()
        raise


def _lc_child(read,world_id,cause,revision,states,*,committed=True):
    if _lc_value_memory(read) is None:
        return _lc_child_legacy(read,world_id,cause,revision,states,committed=committed)
    from .coordinator_scheduler import _lifecycle_value_context
    owner = _lifecycle_value_context.get()
    if owner is None or owner.memory is not _lc_value_memory(read):_lc_fail('因果child缺上层owner')
    loan = _lc_call_loan(read, _lc_child_owned, read, world_id, cause, revision, states, committed=committed)
    return tuple(owner.take(loan))


def _lc_death_window(read, state, revision):
    """Validate each older window and activation without a history list."""
    window = _lc_select(read,'main.coordinator_windows',(('advanced_revision',revision),),
                        lambda w:True,'死亡非原开窗advance')
    if window['base_revision']!=revision-1 or window['target_time']!=state['time']:_lc_fail('死亡非原开窗advance')
    def activation(row):
        if row['private_closed']!=1 or row['phase'] not in ('completed','rejected'):_lc_fail('旧私密未闭合')
    def older(row):
        if row['ordinal']>=window['ordinal']:return
        if row['state']!='completed' or row['completed_revision']>revision-1:_lc_fail('旧窗未闭合')
        _lc_each(read,'main.coordinator_activations',(('window_id',row['window_id']),),activation)
    _lc_each(read,'main.coordinator_windows',(('world_id',state['world_id']),),older,order=(('ordinal',False),))


def _lc_child_owned(read,world_id,cause,revision,states,*,committed=True):
    """Primitive join only: no window_record, audit, acquisition or recursion."""
    _lc_cause_ref(cause);parent=cause['parent_intent_id'];version=cause['decision_version'];index=cause['effect_index']
    child=_lc_read(read,'main.intents',(('id',cause['child_intent_id']),),first=True)
    source=_lc_read(read,'main.intents',(('id',parent),),first=True)
    decision=_lc_read(read,'main.dm_decisions',(('intent_id',parent),('version',version)),first=True)
    proposal=_lc_read(read,'main.dm_proposals',(('intent_id',parent),),first=True)
    if source is None or decision is None or proposal is None:_lc_fail('原提案/裁定缺失')
    decision=_lc_json(decision['data']);payload=_lc_json(source['payload']);body=payload['body'];pid=source['person']
    if (set(payload)!={'body'} or set(body)!={'action','text','physical_attempt'} or body['action']!='propose'
            or proposal['world_id']!=world_id or proposal['person_id']!=pid or proposal['text']!=body['text']
            or decision['world_id']!=world_id or index>=len(decision['effects'])):_lc_fail('原人物提案绑定不符')
    base=_lc_int(decision['base_revision'],1)
    base_state=_lc_historical(states,base)
    if base_state['world_id']!=world_id:_lc_fail('裁定基准不存在')
    del base_state
    actor=_lc_read(read,'actor.actor_intents',(('intent_id',parent),),first=True)
    meta=_lc_read(read,'actor.actor_intent_execution',(('intent_id',parent),),first=True)
    calls=_lc_select(read,'actor.actor_calls',(('intent_id',parent),),lambda r:True,'原私库候选链缺失')
    activations=_lc_select(read,'main.coordinator_activations',(('intent_id',parent),),lambda r:True,'原私库候选链缺失')
    if actor is None or meta is None:_lc_fail('原私库候选链缺失')
    request=_lc_json(actor['request']);candidate=_lc_json(actor['candidate'])
    base_messages,base_seen=_lc_private_base(read,world_id,pid,meta['base_version'])
    if (request['messages']!=base_messages or meta['base_seen_sha256']!=_lc_hash(base_seen)
            or activations['base_session_version']!=meta['base_version']
            or activations['request_sha256']!=_lc_hash(request)
            or activations['candidate_sha256']!=_lc_hash(candidate)):_lc_fail('原请求/候选hash或空起点不符')
    from .actor_execution import body_matches
    _lc_scratch(request,'child-body-matches-canonical-utf8-scratch')
    if (not body_matches(request,body,candidate) or request.get('schema_version')!=3
            or request['person_id']!=pid or request['intent_id']!=parent
            or actor['person_id']!=pid or _lc_json(actor['body'])!=body
            or meta['person_id']!=pid or meta['world_id']!=world_id or meta['intent_id']!=parent
            or type(meta['base_version']) is not int or meta['base_version']<0
            or meta['base_messages_sha256']!=_lc_hash(request['messages'])
            or calls['person_id']!=pid or calls['call_id']!='real-'+parent
            or activations['person_id']!=pid or activations['world_id']!=world_id
            or activations['intent_id']!=parent):_lc_fail('原candidate/meta/call不符')
    binding=request['identity_binding'];observed=binding['observation_revision']
    if activations['observation_revision']!=observed:_lc_fail('观测基准不符')
    historical=_lc_historical(states,observed);origin=historical['_lifecycle']['person_origins'].get(pid)
    tenure=_lc_tenure(historical,pid)
    expected_binding=dict(world_id=world_id,person_id=pid,observation_revision=observed,origin_ref=pid,
        tenure_ref=tenure['id'] if tenure is not None else None)
    if origin is None or binding!=expected_binding or not _lc_active(historical,pid):_lc_fail('原身份未授权')
    for kind,v in [('proposal',0),('decision',version)]:
        timeline=_lc_read(read,'main.dm_timeline',(('intent_id',parent),('kind',kind),('version',v)),first=True)
        if timeline is None or type(timeline['visible_from']) is not int or timeline['visible_from']>revision+(0 if committed else 1):_lc_fail('原可见性未成立')
    effect=decision['effects'][index]
    if committed:
        timeline=_lc_read(read,'main.dm_timeline',(('intent_id',parent),('kind','commit'),('version',version)),first=True)
        if child is None or child['receipt'] is None or source['receipt'] is None or timeline is None or timeline['visible_from']!=revision:_lc_fail('缺真实父子提交')
        cp=_lc_json(child['payload']);cr=_lc_json(child['receipt']);pr=_lc_json(source['receipt'])
        if (cp!={'parent':parent,'version':version,'body':effect} or child['person']!=pid
                or any(r.get('world_id')!=world_id or r.get('person_id')!=pid or r.get('status')!='committed'
                    or r.get('decision_version')!=version or r.get('ruling')!=decision['ruling'] for r in (cr,pr))
                or cr['intent_id']!=cause['child_intent_id'] or pr['intent_id']!=parent
                or cr['time']!=pr['time'] or pr['effect']['effects'][index]!=cr['effect']):_lc_fail('父子receipt不符')
        return [pid,body,effect,decision,cr]
    return [pid,body,effect,decision,None]


def _lc_close_iterator(iterator):
    # A failed C0 iterator has no physical SQL cursor to close. Its outstanding
    # failed Row and metadata must stay charged, and close() must not replace
    # the original primary. Native generators always run independent cursor close.
    from .coordinator_scheduler import _C0V3BorrowIterator
    if type(iterator) is _C0V3BorrowIterator:
        previous=iterator.previous
        if previous is not None and not previous.closed:return
    iterator.close()


def _lc_scratch(value, role):
    """Original sourcehelper canonical/chunks/UTF8 work, not just graph slots."""
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_value_price
    owner=_lifecycle_value_context.get()
    if owner is None:_lc_fail('sourcehelper scratch缺上层owner')
    charge,wire,nodes=_lifecycle_value_price(value,owner.memory)
    owner.reserve(charge+4096+256*nodes+16*wire,role)


class _lc_geo_Decoder:
    """Plain JSON, one primitive and one ancestor frontier, no canonical copy.

    Containers/slots/scalars are paid into the escaping graph loan BEFORE
    allocation. The 64KiB graph arenas are real aggregate admission, not a
    per-node lease table; their unused tail remains paid. Recursive frames and
    the sole scalar token are separately paid and released only on success.
    This changes allocation representation, not the ordinary JSON domain.
    """
    def __init__(self, raw, owner, loan, check):
        self.raw, self.owner, self.loan, self.check = raw, owner, loan, check
        self.offset = 0
        self.available = 0
        self.decoder = json.JSONDecoder()
        self._keys = None

    def pay(self, amount):
        if amount > self.available:
            amount_needed = max(65536, amount - self.available)
            self.loan.leases.append(self.owner.memory.hold(amount_needed, 'geo:retained-graph-arena'))
            self.available += amount_needed
        self.available -= amount

    def space(self):
        while self.offset < len(self.raw) and self.raw[self.offset] in ' \t\r\n':
            self.offset += 1

    def error(self, message):
        raise json.JSONDecodeError(message, self.raw, self.offset)

    def scalar(self, *, _object_key):
        self.check()
        start = self.offset
        if self.raw[start] == '"':
            end = start + 1
            while end < len(self.raw):
                char = self.raw[end]
                end += 1
                if char == '\\':
                    end += 1
                elif char == '"':
                    break
            end = min(end, len(self.raw))
            # Source token width bounds decoded codepoints, including escapes.
            width = end - start
            if not _object_key:
                self.pay(80 + 4 * width)
        else:
            end = start
            while end < len(self.raw) and self.raw[end] not in ' \t\r\n,]}:':
                end += 1
            width = end - start
            self.pay(64 + width)
        # A hit still has to decode the token. Keep both original key quote
        # terms funded until decode and the retained-key decision finish.
        key_decode = 80 + 4 * width if _object_key else 0
        temporary = self.owner.memory.hold(8 * width + 4096 + key_decode, 'geo:frontier-scalar')
        try:
            value, self.offset = self.decoder.raw_decode(self.raw, start)
            if type(value) in (dict, list):
                raise TypeError('geography scalar cannot decode a container')
        except BaseException:
            # The loan and temporary remain charged to the first failing TB.
            raise
        if _object_key:
            assert type(value) is str
            pool = self._keys
            prior = pool.get(value) if pool is not None else None
            if prior is not None:
                value = prior
            else:
                self.pay(80 + 4 * width)
                if pool is None:
                    self.pay(256)
                    self._keys = pool = {}
                if len(pool) < 640:
                    self.pay(128)
                    pool[value] = value
        value_end = self.offset
        temporary.release()
        self.offset = value_end
        return value

    def descend(self):
        # Pay BEFORE the actual child Python/native parser frame is created.
        frame = self.owner.memory.hold(2048, 'geo:frontier-frame')
        try:
            result = self.value()
        except BaseException:
            raise
        frame.release()
        return result

    def value(self):
        self.check()
        self.space()
        if self.offset >= len(self.raw):
            self.error('Expecting value')
        char = self.raw[self.offset]
        if char not in '{[':
            return self.scalar(_object_key=False)
        mapping = char == '{'
        end = '}' if mapping else ']'
        self.pay(256 if mapping else 80)
        result = {} if mapping else []
        self.offset += 1
        self.space()
        if self.offset < len(self.raw) and self.raw[self.offset] == end:
            self.offset += 1
            return result
        while True:
            self.check()
            self.space()
            if mapping:
                if self.offset >= len(self.raw) or self.raw[self.offset] != '"':
                    self.error('Expecting property name enclosed in double quotes')
                key = self.scalar(_object_key=True)
                self.space()
                if self.offset >= len(self.raw) or self.raw[self.offset] != ':':
                    self.error("Expecting ':' delimiter")
                self.offset += 1
            self.pay(128 if mapping else 32)
            value = self.descend()
            if mapping:
                result[key] = value  # Original plain JSON last duplicate wins.
                key = None
            else:
                result.append(value)
            value = None
            self.space()
            if self.offset >= len(self.raw):
                self.error("Expecting ',' delimiter")
            char = self.raw[self.offset]
            self.offset += 1
            if char == end:
                return result
            if char != ',':
                self.offset -= 1
                self.error("Expecting ',' delimiter")


class _lc_geo1137_Diagnostics:
    """Fixed prepaid fallback; primary and first tuples never use foreign dicts."""
    __slots__ = ('primary', 'first_secondary', 'recording', 'incomplete',
                 'records', 'attempts', 'omitted', 'first_observations')

    def __init__(self):
        self.primary = self.first_secondary = self.recording = None
        self.omitted = None
        self.incomplete = False
        self.records = []
        self.attempts = 0
        self.first_observations = [None, None, None, None]


def _lc_geo1137_tuple(error):
    return (error, BaseException.__traceback__.__get__(error),
        BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))


def _lc_geo1137_close(holder, cursors, owner):
    # Independent physical attempts precede all optional recording. The fixed
    # slots survive first append failure; an earlier primary NEVER changes.
    for cursor in cursors:
        try:
            cursor.close()
        except BaseException as cleanup:
            if holder.primary is None:
                holder.primary = _lc_geo1137_tuple(cleanup)
                first = holder.primary
            elif holder.primary[0] is cleanup:
                first = holder.primary
            elif holder.first_secondary is None:
                holder.first_secondary = _lc_geo1137_tuple(cleanup)
                first = holder.first_secondary
            elif holder.first_secondary[0] is cleanup:
                first = holder.first_secondary
            else:
                first = _lc_geo1137_tuple(cleanup)
            # Fixed identity cache is independent of optional append storage.
            for index, observed in enumerate(holder.first_observations):
                if observed is not None and observed[0] is cleanup:
                    first = observed
                    break
                if observed is None:
                    holder.first_observations[index] = first
                    break
            # Poison must not become another route that replaces A or prevents
            # later physical cleanup if error recording itself fails.
            if owner is not None:
                try:
                    owner.fail(holder.primary[0], first=holder.primary)
                except BaseException as recording:
                    holder.incomplete = True
                    if holder.recording is None:
                        holder.recording = _lc_geo1137_tuple(recording)
            holder.attempts += 1
            if holder.attempts > 4:
                holder.incomplete = True
                if holder.omitted is None:
                    holder.omitted = first
                continue
            try:
                holder.records.append(first)
            except BaseException as recording:
                holder.incomplete = True
                if holder.recording is None:
                    holder.recording = _lc_geo1137_tuple(recording)
    # No raise here: query's original bare raise retains A; without A query
    # forwards the saved B. The helper also permits independent multi-close
    # component controls without pretending they are native SQL cursors.


def _lc_geo_query(db, sql):
    from .coordinator_scheduler import _lifecycle_value_context
    owner = _lifecycle_value_context.get()
    if owner is not None:
        # FIRST, before execute/fetch/close or diagnostic-holder allocation.
        # One query has one physical cursor; four bounded records plus first
        # secondary/recording/omitted tuples fit the declared fixed carrier.
        owner.reserve(8192, 'geo1137-query-first-secondary-recording-fallback')
    holder = _lc_geo1137_Diagnostics()
    cursor = None
    try:
        cursor = db.execute(sql)
        return cursor.fetchone()
    except BaseException as error:
        holder.primary = _lc_geo1137_tuple(error)
        raise
    finally:
        if cursor is not None:
            had_primary = holder.primary is not None
            _lc_geo1137_close(holder, (cursor,), owner)
            if not had_primary and holder.primary is not None:
                # No fetch/execute A: original close B remains the primary.
                raise BaseException.with_traceback(holder.primary[0], holder.primary[1])
        if holder.primary is not None and holder.attempts == 0 and owner is not None:
            # Execute A never acquired a cursor; fetch A may have closed
            # normally. In both cases no close failure delivered A's tuple.
            try:
                owner.fail(holder.primary[0], first=holder.primary)
            except BaseException as recording:
                holder.incomplete = True
                holder.recording = _lc_geo1137_tuple(recording)

def _lc_geo_fetch_decode(db, owner):
    from .coordinator_scheduler import _LifecycleValueLoan, _LifecycleLiveLimit, _lifecycle_utf8_size
    if not db.in_transaction:
        raise _LifecycleLiveLimit('geography requires the original stable transaction')
    budget = getattr(db, '_attempt', None)
    check = budget.check if budget is not None else lambda: None
    check()
    meta = _lc_geo_query(db, "SELECT typeof(value),typeof(sha256),length(CAST(value AS BLOB))+length(CAST(sha256 AS BLOB)) FROM world_constants WHERE key='geography'")
    if meta is None:
        raise WorldError('SOURCE_DRIFT', '世界静态地理缺失或摘要不符')
    if meta[0] != 'text' or meta[1] != 'text':
        raise WorldError('SOURCE_DRIFT', '世界静态地理缺失或摘要不符')
    if type(meta[2]) is not int or not 0 <= meta[2] <= 1048576:
        raise _LifecycleLiveLimit('geography native row exceeds 1MiB')
    width = meta[2]
    meta = None
    check()
    row = _lc_geo_query(db, "SELECT value,sha256 FROM world_constants WHERE key='geography'")
    if row is None:
        raise WorldError('SOURCE_DRIFT', '世界静态地理缺失或摘要不符')
    raw = row[0]
    encoded = raw.encode('utf-8')
    if len(encoded) + _lifecycle_utf8_size(row[1]) != width:
        raise _LifecycleLiveLimit('geography changed within stable transaction')
    if hashlib.sha256(encoded).hexdigest() != row[1]:
        raise WorldError('SOURCE_DRIFT', '世界静态地理缺失或摘要不符')
    # Only authenticated raw enters JSON. No strict duplicate/nonfinite hooks
    # and no canonical/UTF8 roundtrip are inserted after plain JSON parsing.
    encoded = row = None
    loan = _LifecycleValueLoan(owner.memory, owner.memory.hold(4096, 'geo:retained-loan-carrier'))
    owner.take(loan)  # Register before the first graph allocation/failure.
    decoder = _lc_geo_Decoder(raw, owner, loan, check)
    loan.value = decoder.descend()
    decoder.space()
    if decoder.offset != len(raw):
        decoder.error('Extra data')
    check()
    raw = decoder = None
    # Seal the original vector, not just the currently visible prefix. The
    # extra reference per lease fits its fixed carrier allowance; the header
    # fits the existing loan carrier. No business value/DB/Entry is retained.
    loan._geo1196 = (id(db), id(budget), tuple(loan.leases),
                     loan.leases[0].ticket, loan.leases[-1].ticket)
    return loan


def _lc_geo_borrow(db, owner, *, _return_loan=False):
    """Prepay raw/UTF8/hash/frontier BEFORE even the first metadata SELECT.

    Native Python TEXT/Row allowance is the unchanged 1MiB row ceiling, not a
    stat-priced read. Graph arenas are admitted before every decoded node and
    slot. All raw aliases live in a separate returned frame, so only successful
    return permits releasing its scratch. The result loan never closes here:
    state['_geography'] and outer TB/result aliases stay on the original owner.
    """
    from .coordinator_scheduler import _LifecycleLiveLimit
    if owner.closed or owner.failed or owner.memory.failed:
        raise _LifecycleLiveLimit('geography requires the live original owner')
    try:
        raw = owner.memory.hold(4 * 1048576 + 4096, 'geo:frontier-native-text-row')
        hashing = owner.memory.hold(1048576 + 8192, 'geo:frontier-utf8-hash')
        frontier = owner.memory.hold(16384, 'geo:frontier-metadata-decoder')
        loan = _lc_geo_fetch_decode(db, owner)
    except BaseException as error:
        owner.fail(error)
        raise
    # _lc_geo_fetch_decode's complete frame returned: raw/row/scalar aliases
    # ended; the graph itself remains strongly retained by the SAME loan.
    frontier.release()
    hashing.release()
    raw.release()
    return loan if _return_loan else loan.value


def _lc_geo1133_direct_input(state):
    """Allocation envelope only; NEVER replace the business body's state.

    The original separately charged geo loan remains live on its actual owner.
    A legacy embedded graph or a foreign/unpaid alias receives no exemption.
    Callers are exclusively source-known readonly/non-geo-copying bodies.
    """
    from .coordinator_scheduler import _lifecycle_value_context, _LifecycleValueLoan
    owner = _lifecycle_value_context.get()
    if owner is None or type(state) is not dict or '_geography' not in state:
        return state
    geography = state['_geography']
    for loan in owner.loans:
        if (type(loan) is _LifecycleValueLoan and loan.owner is owner
                and loan.memory is owner.memory and loan.value is geography
                and not loan.closed and not loan.failed
                and not owner.closed and not owner.failed and not owner.memory.failed):
            # Price the actual shallow projection BEFORE constructing it.
            owner.reserve(256 + 128 * len(state), 'geo1133:dynamic-allocation-envelope')
            return WorldStore._dynamic_state(state)
    return state


def _lc_geo1133_reserve_dynamic(state, factor, role):
    # Keep every non-geography occurrence, including private/config fields and
    # normal action scratch. _public's independent serialization pays its actual
    # output separately. There is no graph release or general price change here.
    _lc_reserve(_lc_geo1133_direct_input(state), factor, role)


# Source image for the one constructor DDL operation; these anchors are only
# compared, never called in place of a live descriptor or history producer.
_DDL1187_MUTATE_CODE = WorldStore._mutate.__code__
_DDL1187_CONNECTION = (WorldStore._connection, WorldStore._connection.__wrapped__,
                       WorldStore._connection.__wrapped__.__code__)
_DDL1187_DYNAMIC = (WorldStore._dynamic_state, WorldStore._dynamic_state.__code__)
_DDL1187_EXECUTE = (_AttemptConnection.execute, _AttemptConnection.execute.__code__,
                    _AttemptConnection._bounded, _AttemptConnection._bounded.__code__)
_DDL1187_HISTORY = tuple((f.__name__, f, f.__code__) for f in (
    _history_cover, _history_epoch, _history_trigger, _history_check_triggers, _history_failure))


def _lc_geo1187_adjudicator_ddl(world, operation, db, owner, state, geo_start):
    """Recognize the original constructor on its actual native write lane.

    Cannot dispatch an operation, issue a capability, or exempt an unpaid Geo.
    Keep the ordinary price for a protocol double, override or replay.
    """
    from types import FunctionType
    if (type(world) is not WorldStore or type(operation) is not FunctionType
            or type(operation.__code__.co_qualname) is not str
            or operation.__code__.co_qualname != 'Adjudicator.__init__.<locals>.initialize'
            or type(db) is not _AttemptConnection):
        return False
    # A staticmethod is non-data: even the original function in the
    # instance dict is a shadow, not the source-proven class dispatch.
    # Do not resolve an untrusted projection or replace the live calls.
    if (WorldStore.__getattribute__ is not object.__getattribute__
            or '_dynamic_state' in vars(world)):
        return False
    projection, projection_code = _DDL1187_DYNAMIC
    descriptor = vars(WorldStore).get('_dynamic_state')
    if (type(descriptor) is not staticmethod or descriptor.__func__ is not projection
            or projection.__code__ is not projection_code
            or projection.__globals__ is not globals()
            or world._dynamic_state is not projection):
        return False
    from .actor_execution import PreparationBudget
    binding = _actor_entry_scope.get()
    if (binding is None or binding[0] is not world or type(binding[1]) is not PreparationBudget
            or vars(db).get('_attempt') is not binding[1] or binding[1]._lifecycle_value_owner is not owner
            or '_connection' in vars(world) or WorldStore._connection is not _DDL1187_CONNECTION[0]
            or getattr(WorldStore._connection, '__wrapped__', None) is not _DDL1187_CONNECTION[1]
            or _DDL1187_CONNECTION[1].__code__ is not _DDL1187_CONNECTION[2]
            or 'execute' in vars(db) or '_bounded' in vars(db)
            or _AttemptConnection.execute is not _DDL1187_EXECUTE[0]
            or _AttemptConnection.execute.__code__ is not _DDL1187_EXECUTE[1]
            or _AttemptConnection._bounded is not _DDL1187_EXECUTE[2]
            or _AttemptConnection._bounded.__code__ is not _DDL1187_EXECUTE[3]
            or not db.in_transaction or db.row_factory is not None or db.text_factory is not str):
        return False
    import sys
    from types import ModuleType
    module = sys.modules.get(__package__ + '.adjudication')
    if type(module) is not ModuleType or operation.__globals__ is not vars(module):
        return False
    space = vars(module)
    anchor = space.get('_ADJ1187_INIT')
    if (anchor is None or space.get('Adjudicator') is not anchor[0]
            or anchor[0].__init__ is not anchor[1] or anchor[1].__code__ is not anchor[2]
            or space.get('_history_cover') is not _DDL1187_HISTORY[0][1]
            or not any(code is operation.__code__ for code in anchor[2].co_consts)):
        return False
    for name, function, code in _DDL1187_HISTORY:
        if globals().get(name) is not function or function.__code__ is not code:
            return False
    if (type(state) is not dict or '_geography' not in state or not owner.loans
            or not _lc1196_consumer_lane(world, 'restore', 'mutation', 'attempt')
            or not _lc1196_geo_paid(db, owner, owner.loans[-1], state['_geography'], geo_start)):
        return False
    # Borrow frame views only during this predicate; do not retain the caller,
    # its locals, the mutation state, or its connection in a witness object.
    outer = sys._getframe(2)
    try:
        if outer.f_code is not _DDL1187_MUTATE_CODE or outer.f_globals is not globals():
            return False
        actual = outer.f_locals
        if (actual.get('self') is not world or actual.get('operation') is not operation
                or actual.get('execution') is not None or actual.get('person_id') is not None
                or actual.get('_precommit') is not None):
            return False
        ctor = outer.f_back
        if ctor is None or ctor.f_code is not anchor[2] or ctor.f_globals is not space:
            return False
        actual = ctor.f_locals
        return (type(actual.get('self')) is anchor[0] and actual.get('world') is world
                and actual.get('initialize') is operation)
    finally:
        del outer


def _lc_geo1133_copied_result(function, args, kwargs):
    # The actual business producer has RETURNED before copy starts. This helper
    # owns no state locals or callbacks supplied by _mutate's unknown callers.
    # An independently prepaid JSON loan, not a field container or shallow copy,
    # is the sole result eligible for detached=True.
    from .coordinator_scheduler import (_lc_adm1136_stage_callee,
        _lc_stale1141_Control, _lc_stale1141_ready)
    _lc_adm1136_stage_callee(function)
    result = function(*args, **kwargs)
    if type(result) is _lc_stale1141_Control:
        _lc_stale1141_ready(result, function)
        # None chooses the existing successful nonescaping invoke/close path.
        # The prepaid slotted object already resides in the original Work.
        return None
    return _lc_copy(result)


def _lc_geo1133_stage(function, args, kwargs):
    from .coordinator_scheduler import _lifecycle_value_context, _lifecycle_value_invoke
    parent = _lifecycle_value_context.get()
    # Same ledger/Entry/context and original Work checks throughout. Only this
    # fresh region is closed after the producer and copying frames return. Any
    # exception uses the unchanged invoke failure route: first TB and ALL debt
    # remain; no finally refund, locals clearing, cache or budget renewal.
    result = _lifecycle_value_invoke(_lc_geo1133_copied_result,
        (function, args, kwargs), memory=parent.memory if parent is not None else None,
        detached=True)
    if result is None:
        from .coordinator_scheduler import _lc_stale1141_deliver
        _lc_stale1141_deliver(parent)
    return result


@contextmanager
def _lc_cold1136_io(world, entry):
    """Bind only the pre-existing native IO scope; do not enter/finish a value owner."""
    token = _actor_entry_scope.set((world, entry))
    try:
        entry.check()
        yield
    finally:
        _actor_entry_scope.reset(token)


_lc_cold1136_p04_db = ContextVar('world_cold1136_p04_db', default=None)


class _lc_cold1136_P04Failure(BaseException):
    """Private exact-DB transit; not one of P04's domain translation types."""
    __slots__ = ('db', 'owner', 'first', 'start', 'suppress_context')

    def __init__(self, db, owner, first, start):
        self.db, self.owner, self.first, self.start = db, owner, first, start
        self.suppress_context = BaseException.__suppress_context__.__get__(first[0])

    def secondary(self, error):
        # Only the current quote/payload/rollback/close tail, never inherited history.
        try:
            saved = self.first
            if saved[0] is not error:
                errors = self.owner.control_errors
                for index in range(self.start, len(errors)):
                    if errors[index][0] is error:
                        saved = errors[index]
                        break
                else:
                    saved = _lc_geo1137_tuple(error)
            self.owner.control_errors.append(saved)
        except BaseException:
            self.owner.memory._failed = True


def _lc_cold1136_p04_signal(db, error):
    return (type(error) is _lc_cold1136_P04Failure and error.db is db
            and _lc_cold1136_p04_db.get() is db)


@contextmanager
def _lc_cold1136_connection(closer, db, entry, *, readonly):
    """Only initialize's own native handle; independent rollback and close."""
    token = _lc_cold1136_p04_db.set(db)
    failed = None
    try:
        try:
            db._attempt = entry
            if not readonly:
                entry.check()
            yield db
            if readonly:
                entry.check()
        except BaseException as error:
            if not _lc_cold1136_p04_signal(db, error):
                if readonly:
                    db.rollback()
                raise
            failed = error
            try:
                db.rollback()
            except BaseException as secondary:
                failed.secondary(secondary)
    finally:
        try:
            try:
                closer.__exit__(None, None, None)
            except BaseException as secondary:
                if failed is None:
                    raise
                failed.secondary(secondary)
        finally:
            _lc_cold1136_p04_db.reset(token)
    if failed is not None:
        error, tb, context, cause = failed.first
        # Restore the native tuple, not the transit exception's relationships.
        # Propagation extends the live traceback; owner.first_failure stays exact.
        try:
            raise BaseException.with_traceback(error, tb)
        finally:
            BaseException.__context__.__set__(error, context)
            BaseException.__cause__.__set__(error, cause)
            BaseException.__suppress_context__.__set__(error, failed.suppress_context)


@contextmanager
def _lc_cold1136_writer(world, entry):
    """A path probe never certifies ownership of SQLite file creation."""
    from .coordinator_scheduler import _lc_ro1127_write_guard
    _lc_ro1127_write_guard(world)
    world.db_path.parent.mkdir(parents=True, exist_ok=True)
    with _lc_cold1136_io(world, entry):
        closer = closing(sqlite3.connect(str(world.db_path), timeout=0,
                                         factory=_AttemptConnection))
        db = closer.__enter__()
        with _lc_cold1136_connection(closer, db, entry, readonly=False):
            yield db


def _lc_cold1136_json(raw, owner, entry):
    """Preprice lexical allocation candidates, not JSON validity or values.

    Only the original stdlib decoder decides syntax, first error, and values.
    Run pricing before *any* JSON decode; no decoded keys, temporary tokens,
    recursive Python grammar or post-decode graph walk. Invalid input is priced
    conservatively like valid input. Cap failure is a separate admission error.
    """
    import re
    from .coordinator_scheduler import _LifecycleLiveLimit
    entry.check()
    scratch = owner.memory.hold(8192, 'cold1146-plain-json-lexical-scratch')
    binary = None
    text = raw
    if type(raw) in (bytes, bytearray):
        if len(raw) > 1048576:
            raise _LifecycleLiveLimit('cold JSON input exceeds 1MiB')
        # Same encoding/errors as json.loads. Fund both Unicode buffers before
        # this conversion; still hand the original raw to the original decoder.
        binary = owner.memory.hold(8 * len(raw) + 1024, 'cold1146-plain-json-binary-decode')
        text = raw.decode(json.detect_encoding(raw), 'surrogatepass')
    charge = 4096
    longest = 0
    if type(text) is str:
        if len(text) > 1048576:
            raise _LifecycleLiveLimit('cold JSON input exceeds 1MiB')
        outer = re.compile(r'"|[{}\[\],:]|[ \t\r\n]+|[^"{}\[\],: \t\r\n]+')
        inner = re.compile(r'[^"\\]+|\\[\s\S]|["\\]')
        i = visits = 0
        while i < len(text):
            start = i
            i = outer.match(text, i).end()
            head = text[start]
            if head == '"':
                while i < len(text):
                    end = inner.match(text, i).end()
                    closed = text[i] == '"'
                    i = end
                    visits += 1
                    if visits % 256 == 0:
                        entry.check()
                    if closed:
                        break
                # Per-string/key storage plus conservative decoder copies.
                charge += 192 + 8 * (i - start)
            elif head in '[{':
                charge += 384
            elif head in ',:':
                charge += 96
            elif head not in ' \t\r\n]}':
                charge += 128 + 4 * (i - start)
            else:
                # Whitespace does not generate a graph or token allocation.
                continue
            longest = max(longest, i - start)
            visits += 1
            if visits % 256 == 0:
                entry.check()
    owner.reserve(charge, 'cold1146-plain-json-value')
    decode = owner.memory.hold(4096 + 8 * longest, 'cold1146-plain-json-decoder-scratch')
    entry.check()
    value = json.loads(raw)
    entry.check()
    text = None
    decode.release()
    scratch.release()
    if binary is not None:
        binary.release()
    return value


def _lc_cold1136_scenario(path, owner):
    """Run the original strict scenario loader with a paid bounded file read.

    Only its read_text seam is adapted. Duplicate/nonfinite/Unicode/IO errors
    still use load_scenario's original INVALID_SCENARIO conversion and order.
    """
    from types import SimpleNamespace
    def read_text(*, encoding):
        size = path.stat().st_size
        owner.reserve(128 * (size + 1) + 65536, 'cold1136-scenario-raw-json-canonical')
        with path.open('rb') as stream:
            raw = stream.read(size + 1)
        if len(raw) != size:
            raise ValueError('scenario changed during bounded cold read')
        return raw.decode(encoding)
    return load_scenario(SimpleNamespace(read_text=read_text))


def _lc_cold1136_existing(world, db, entry, *, known_snapshots=False):
    """One real stored-world operation, including Source generations and Geo.

    Caller creates its original connection/transaction. Every payload acquisition
    is inside the earliest Entry's same value owner; only scalar existence runs
    before baseline quotation. No bootstrap label, test-owned with, fake Row,
    new allowance per reader, early generation refund or legacy schema change.
    Returning None means genuine absence; new-world construction runs outside
    this cold scope and therefore does not inherit an unrelated Geo allocation.
    """
    from .coordinator_scheduler import (_lc_adm1136_budget_context, _lifecycle_value_context,
        _lc_adm1136_one, _p04_native_current, _LifecycleValueOwner, _LifecycleLiveLedger,
        _LifecycleLiveLimit, _lifecycle_live_context)
    entry.check()
    previous = getattr(entry, '_lifecycle_value_owner', None)
    parent = _lifecycle_value_context.get()
    owner = parent if parent is not None else previous
    if owner is None:
        memory = _lifecycle_live_context.get()
        owner = _LifecycleValueOwner(memory if memory is not None else _LifecycleLiveLedger())
        entry._lc_cold1136_empty_owner = owner
    if (owner.closed or owner.failed or owner.memory.failed
            or previous is not None and previous is not owner):
        raise _LifecycleLiveLimit('cold initialize cannot renew the original Entry owner')
    entry._lifecycle_value_owner = owner
    token = _lc_adm1136_budget_context.set(entry)
    value_token = _lifecycle_value_context.set(owner)
    try:
        owner.reserve(16384, 'cold1136-record-metadata-and-error-carriers')
        exists = known_snapshots or _lc_adm1136_one(db,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='snapshots'")
        baseline = _lc_root_row(db, 'SELECT state FROM snapshots ORDER BY seq LIMIT 1', columns=('state',)) if exists else None
    except BaseException as error:
        owner.fail(error)
        raise
    finally:
        _lifecycle_value_context.reset(value_token)
        _lc_adm1136_budget_context.reset(token)
    if baseline is None:
        # On the original writer's empty result all prefix frames/Rows have
        # returned. Only our own absence-only owner may be released; never a
        # caller's owner, an escaping record, or an exceptional traceback debt.
        if known_snapshots and getattr(entry, '_lc_cold1136_empty_owner', None) is owner:
            owner.close()
        return None
    with world._entry_budget(entry):
        token = _lc_adm1136_budget_context.set(entry)
        def decode(raw):
            return _lc_cold1136_json(raw, owner, entry)
        try:
            if world._explicit_scenario:
                try:
                    requested = _lc_cold1136_scenario(world.scenario_path, owner)
                except ScenarioError as error:
                    raise WorldError(error.code, str(error)) from None
                original = decode(baseline[0])['_config']
                _lc_reserve(requested, 2, 'cold1136-scenario-digest-scratch')
                _lc_reserve(original, 2, 'cold1136-original-digest-scratch')
                if scenario_digest(requested) != scenario_digest(original):
                    raise WorldError('SCENARIO_MISMATCH', '场景与存档不符；请使用新的数据目录创建世界')
            latest_row = _lc_root_row(db, 'SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1', columns=('state',))
            latest = decode(latest_row[0])
            world._check_frozen_pathology(latest, decode(baseline[0]))
            world._check_frozen_economy(latest, decode(baseline[0]))
            world._check_frozen_military(latest, decode(baseline[0]))
            world._check_frozen_p04(latest, decode(baseline[0]))
            world._lifecycle_check(db, latest, cold_decode=decode)
            _p04_native_current(db, latest, cold_decode=decode)
            if 'military' in latest:
                if _lc_optin(latest):
                    world._restore_geography(db, latest)
                else:
                    world._restore_geography(db, latest, cold_decode=decode)
            entry.check()
            return world._public(latest)
        finally:
            _lc_adm1136_budget_context.reset(token)


# Only a direct public card edit on the original plain writer is eligible.
# These anchors certify the quote, never replace the dynamically dispatched calls.
from types import CodeType as _Card1191Code, FunctionType as _Card1191Function

_CARD1191_WORLD = WorldStore
_CARD1191_NATIVE = (sqlite3.Connection, sqlite3.connect)
_CARD1191_METHODS = tuple((name, vars(WorldStore)[name], getattr(WorldStore, name),
                           getattr(WorldStore, name).__code__) for name in (
    'edit_card', '_mutate', '_lc_geo1133_mutate_body', '_connection', '_restore_geography',
    '_find', '_event', '_dynamic_state', '_encode_state', '_public', '_check_frozen_pathology',
    '_check_frozen_p04', '_check_frozen_economy', '_check_frozen_military', '_military_accounts',
    '_lifecycle_check', '_check_coordinator_execution', '_scoped_coordinator'))
_CARD1191_WRITER = (WorldStore._connection.__wrapped__, WorldStore._connection.__wrapped__.__code__)
_CARD1191_CHANGE = next(code for code in _CARD1191_METHODS[0][3].co_consts
                        if type(code) is _Card1191Code and code.co_name == 'change')
_CARD1191_PRODUCERS = tuple((f.__name__, f, f.__code__) for f in (
    _lc_root_json, _lc_geo_borrow, _lc_geo_fetch_decode, _lc_reserve, _history_cover))


def _lc_geo1191_card_quote(world, operation, db, state, owner, execution, person_id, precommit, geo_start):
    """Prepay the real card edit's working domain; retain every existing loan."""
    if (type(operation) is not _Card1191Function or operation.__code__ is not _CARD1191_CHANGE
            or operation.__globals__ is not globals() or operation.__defaults__ is not None
            or operation.__kwdefaults__ is not None or not execution is person_id is precommit is None
            or type(world) is not _CARD1191_WORLD or WorldStore is not _CARD1191_WORLD
            or type(db) is not _CARD1191_NATIVE[0] or sqlite3.connect is not _CARD1191_NATIVE[1]
            or not db.in_transaction or db.row_factory is not None or db.text_factory is not str
            or type(state) is not dict or '_geography' not in state or '_lifecycle' in state
            or 'military' in state or 'economy' in state):
        return False
    bound = vars(world)
    methods = vars(WorldStore)
    if WorldStore.__getattribute__ is not object.__getattribute__ or '__getattr__' in methods:
        return False
    for name, descriptor, function, code in _CARD1191_METHODS:
        if name in bound or methods.get(name) is not descriptor or function.__code__ is not code:
            return False
    wrapped, wrapped_code = _CARD1191_WRITER
    if (getattr(WorldStore._connection, '__wrapped__', None) is not wrapped
            or wrapped.__code__ is not wrapped_code):
        return False
    for name, function, code in _CARD1191_PRODUCERS:
        if globals().get(name) is not function or function.__code__ is not code:
            return False
    if (not owner.loans or not _lc1196_consumer_lane(world, 'restore', 'mutation', 'card')
            or not _lc1196_geo_paid(db, owner, owner.loans[-1], state['_geography'], geo_start)):
        return False
    import sys
    mutation = sys._getframe(2)
    try:
        if mutation.f_code is not _CARD1191_METHODS[1][3] or mutation.f_globals is not globals():
            return False
        edit = mutation.f_back
        if edit is None or edit.f_code is not _CARD1191_METHODS[0][3] or edit.f_globals is not globals():
            return False
        actual = edit.f_locals
        if actual.get('self') is not world or actual.get('change') is not operation or actual.get('parent') is not None:
            return False
    finally:
        del mutation
    # The original callback only finds a person, merges its card, and appends
    # one event. Existing non-Geo material/slots are still fully quoted; cover
    # the new bounded card fields, event, and shallow envelope independently.
    # No Geo is cloned or detached, including on an exception/TB path.
    owner.reserve(8192 + 256 + 128 * len(state), 'card1191:edit-and-allocation-envelope')
    _lc_reserve({key: value for key, value in state.items() if key != '_geography'},
                1, 'root-mutation-direct-containers')
    return True


def _lc1196_geo_paid(db, owner, loan, value, ticket_start):
    """One freshly decoded, fully paid Geo graph; never an open-flag proxy."""
    from .actor_execution import PreparationBudget
    from .coordinator_scheduler import (_LifecycleValueOwner, _LifecycleValueLoan,
        _LifecycleLiveLedger, _LifecycleLiveLease, _lifecycle_value_context,
        _lifecycle_live_context, _lc_ro1127_context)
    if (type(owner) is not _LifecycleValueOwner or owner.closed or owner.failed
            or owner.control_errors or owner.ro1127 is not None or _lc_ro1127_context.get() is not None
            or _lifecycle_value_context.get() is not owner):
        return False
    memory = owner.memory
    if (type(memory) is not _LifecycleLiveLedger or memory.failed
            or _lifecycle_live_context.get() is not memory
            or type(owner.loans) is not list or not owner.loans or owner.loans[-1] is not loan
            or type(loan) is not _LifecycleValueLoan or loan.owner is not owner
            or loan.memory is not memory or loan.failed or loan.closed or loan.value is not value
            or type(loan.leases) is not list or type(memory.leases) is not dict):
        return False
    scope = _actor_entry_scope.get()
    if type(db) is _AttemptConnection:
        entry = vars(db).get('_attempt')
        if (type(entry) is not PreparationBudget or type(scope) is not tuple or len(scope) != 2
                or scope[1] is not entry or vars(entry).get('_lifecycle_value_owner') is not owner):
            return False
    elif type(db) is _CARD1191_NATIVE[0] and scope is None:
        entry = None
    else:
        return False
    paid = loan._geo1196
    if (type(paid) is not tuple or len(paid) != 5
            or paid[0] != id(db) or paid[1] != id(entry)
            or type(paid[2]) is not tuple or len(paid[2]) != len(loan.leases) or len(paid[2]) < 2
            or not ticket_start < paid[3] <= paid[4] <= memory.next_ticket):
        return False
    # The producer seals the count and endpoints only on complete decode.
    # A live prefix, missing final arena, repeated/foreign ticket or a later
    # allocation cannot stand in for any of the original graph charges.
    previous = ticket_start
    for i, lease in enumerate(loan.leases):
        if (lease is not paid[2][i] or type(lease) is not _LifecycleLiveLease or lease.ledger is not memory
                or lease.released or lease.ro1127 is not None or type(lease.ticket) is not int
                or not previous < lease.ticket <= paid[4] or type(lease.charge) is not int
                or memory.leases.get(lease.ticket) != lease.charge):
            return False
        if i == 0:
            if lease.ticket != paid[3] or lease.role != 'geo:retained-loan-carrier' or lease.charge != 4608:
                return False
        elif lease.role != 'geo:retained-graph-arena' or lease.charge < 66048:
            return False
        previous = lease.ticket
    return previous == paid[4]


def _lc1196_capture_consumers(space, names):
    """Load-root image of selected consumer edges, never a dispatch table."""
    result = []
    for path in names.split():
        first, _, tail = path.partition('.')
        root = space[first]
        if tail:
            namespace, name = vars(root), tail
        else:
            namespace, name = space, first
        item = namespace[name]
        if type(item) in (staticmethod, classmethod):
            functions = (item.__func__,)
        elif type(item) is property:
            functions = (item.fget, item.fset, item.fdel)
        elif type(item) is _Card1191Function:
            wrapped = getattr(item, '__wrapped__', None)
            functions = (item,) if type(wrapped) is not _Card1191Function else (item, wrapped)
        else:
            functions = ()  # Exact imported binding, native slot, class or constant.
        result.append((space, first, root, namespace, name, item,
                       tuple((f, f.__code__) for f in functions if type(f) is _Card1191Function)))
    return tuple(result)


def _lc1196_consumers_match(world, *domains):
    if (type(world) is not _CARD1191_WORLD or WorldStore is not _CARD1191_WORLD
            or WorldStore.__getattribute__ is not object.__getattribute__
            or '__getattr__' in vars(WorldStore)):
        return False
    instance = vars(world)
    # No business call, instance descriptor or arbitrary getter is used to
    # test the image. For type-qualified calls the class slot is the live edge.
    for domain in domains:
        for space, first, root, namespace, name, item, functions in domain:
            if (space.get(first) is not root or namespace.get(name) is not item
                    or root is WorldStore and namespace is not space and name in instance):
                return False
            for function, code in functions:
                if function.__code__ is not code:
                    return False
    return True


def _lc1196_consumer_lane(world, *lanes):
    from .coordinator_scheduler import _CONSUMER1196
    for lane in lanes:
        if not _lc1196_consumers_match(world, *_CONSUMER1196[lane]):
            return False
    return True


# Finite bootstrap anchors for the first readonly Root only; never dispatch
# through these in lieu of validating the actual runtime binding.
_ROOT1154_DESCRIPTORS = (vars(WorldStore)['_execution_identity'], vars(WorldStore)['_execution_now'])
_ROOT1154_LOADS = (json.loads, json.loads.__code__, json.loads.__globals__)
from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())
