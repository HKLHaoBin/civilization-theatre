"""Bounded host-only failure metadata. Never messages, args or domain inputs.

Reported locations are observations of an existing loaded-generation registry,
not a fresh source attestation. No filesystem access is made during projection.
"""
import builtins
import json
import math
import sqlite3
import time
import types

MAX_CHAIN = 4
MAX_FRAMES = 16
MAX_SCAN = 64
MAX_NOTE = 24576


def _number(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def _codes(code, depth=0):
    if depth > 12:
        return
    yield code
    for value in code.co_consts:
        if type(value) is types.CodeType:
            yield from _codes(value, depth+1)


def _reported(frame):
    from .source_attestation import _REGISTRY, FILES
    namespace = frame.f_globals
    stamp = _REGISTRY.get(id(namespace))
    if (type(stamp) is not dict or stamp.get('namespace') is not namespace
            or stamp.get('matched') is not True):
        return None
    # Fixed lexical mapping only: never resolve/stat/open a traceback filename.
    filename = frame.f_code.co_filename.replace('\\', '/')
    module_filename = namespace.get('__file__')
    if type(module_filename) is not str or filename != module_filename.replace('\\', '/'):
        return None
    name = filename.rsplit('/', 1)[-1]
    if name not in FILES or not any(frame.f_code is code for code in _codes(stamp['code'])):
        return None
    return 'app/' + name


def _class_name(error):
    cls = type(error)
    for name in ('RuntimeError', 'ValueError', 'TypeError', 'TimeoutError', 'OSError', 'AssertionError'):
        if cls is vars(builtins)[name]:
            return name
    for name in ('Error', 'DatabaseError', 'OperationalError', 'IntegrityError', 'ProgrammingError', 'InterfaceError', 'DataError', 'InternalError', 'NotSupportedError'):
        if cls is vars(sqlite3)[name]:
            return 'sqlite3.' + name
    from .source_attestation import _REGISTRY
    for stamp in tuple(_REGISTRY.values()):
        if type(stamp) is not dict or stamp.get('matched') is not True:
            continue
        for name in ('WorldError', 'ActorRunError', 'RequestError', 'SourceMismatch'):
            if stamp['namespace'].get(name) is cls:
                return name
    return 'unknown_exception_type'


def _budget(frame, relative):
    if relative != 'app/actor_execution.py':
        return []
    from .actor_execution import EntryBudget, PreparationBudget, Attempt, _ContentionAccount
    result = []
    # Only named entries, never copy or iterate the locals dictionary.
    for name in ('self', 'budget', 'entry', 'attempt'):
        item = frame.f_locals.get(name)
        if type(item) not in (EntryBudget, PreparationBudget, Attempt):
            continue
        values = object.__getattribute__(item, '__dict__')
        row = {'kind': type.__getattribute__(type(item), '__name__'),
               'deadline': _number(values.get('deadline')),
               'busy_deadline': _number(values.get('busy_deadline'))}
        accounts = values.get('_accounts')
        if type(accounts) is tuple:
            row['accounts'] = []
            for account in accounts[:4]:
                if type(account) is _ContentionAccount:
                    data = object.__getattribute__(account, '__dict__')
                    row['accounts'].append({key: _number(data.get(key)) for key in ('limit', 'spent', 'started', 'depth')})
            row['accounts_truncated'] = len(accounts) > 4
        result.append(row)
    return result


def project(error, *, first=None, _firsts=()):
    """Caller contains failure; transient frame references never enter result.

    _firsts is the private rollback delivery's ordered, bounded tuple table.
    The root keeps its explicit snapshot; a linked node uses the earliest
    strong identity match or its original live-descriptor path if unknown.
    """
    if first is not None and first[0] is not error:
        raise ValueError('foreign first failure snapshot')
    observed = _number(time.monotonic())
    result = {'schema': 'safe-technical-failure-v1', 'origin': 'caught_exception',
              'observed_at_catch_monotonic': observed, 'raise_time': 'unknown',
              'classification': 'technical_failure', 'chain': [], 'chain_truncated': False}
    pending = [(error, 'primary')]
    seen = []
    while pending and len(result['chain']) < MAX_CHAIN:
        current, relation = pending.pop(0)
        if any(current is prior for prior in seen):
            result['chain_truncated'] = True
            continue
        seen.append(current)
        row = {'relation': relation, 'type': _class_name(current), 'reported_frames': [],
               'foreign_frames_omitted': False, 'frames_truncated': False}
        if type(current) in (sqlite3.OperationalError, sqlite3.DatabaseError, sqlite3.IntegrityError, sqlite3.ProgrammingError):
            data = object.__getattribute__(current, '__dict__')
            code = data.get('sqlite_errorcode')
            name = data.get('sqlite_errorname')
            row['sqlite_errorcode'] = code if type(code) is int and 0 <= code <= 65535 else None
            row['sqlite_errorname'] = name if type(name) is str and name in vars(sqlite3) and name.startswith('SQLITE_') and vars(sqlite3)[name] == code else None
        saved = first if current is error else next(
            (snapshot for snapshot in _firsts if snapshot[0] is current), None)
        trace = saved[1] if saved is not None else BaseException.__traceback__.__get__(current)
        scanned = 0
        while trace is not None and scanned < MAX_SCAN:
            scanned += 1
            frame = trace.tb_frame
            relative = _reported(frame)
            if relative is None:
                row['foreign_frames_omitted'] = True
            elif len(row['reported_frames']) < MAX_FRAMES:
                row['reported_frames'].append({'file': relative, 'function': frame.f_code.co_name,
                    'line': trace.tb_lineno, 'budget_observed_at_catch': _budget(frame, relative)})
            else:
                row['frames_truncated'] = True
            trace = trace.tb_next
        if trace is not None:
            row['frames_truncated'] = True
        result['chain'].append(row)
        for index, descriptor, label in ((3, BaseException.__cause__, 'cause'),
                                         (2, BaseException.__context__, 'context')):
            linked = saved[index] if saved is not None else descriptor.__get__(current)
            if linked is not None:
                pending.append((linked, label))
    if pending:
        result['chain_truncated'] = True
    return result


def unavailable(origin='projection_unavailable'):
    return {'schema': 'safe-technical-failure-v1', 'origin': origin, 'raise_time': 'unknown'}


def note(projection):
    encoded = json.dumps(projection, ensure_ascii=True, allow_nan=False, separators=(',', ':'))
    if len(encoded) > MAX_NOTE:
        encoded = json.dumps(unavailable('projection_size_limit'), separators=(',', ':'))
    return 'safe_technical_failure=' + encoded
