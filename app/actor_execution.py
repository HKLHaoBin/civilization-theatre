"""Trusted host storage boundary. No authority data is sent to Pi."""
from __future__ import annotations
from typing import TYPE_CHECKING, Iterator
if TYPE_CHECKING:
    from .world import WorldStore
    from .actors import ActorHost
    from .coordinator_execution import CoordinatorAttempt, CoordinatorPermit
    from .coordinator_scheduler import ActivationRef
from contextlib import closing, contextmanager
from pathlib import Path
import json
import os
import sqlite3
from .execution import ExecutionPermit
from .world import WorldError, _history_cover


from dataclasses import dataclass
import math


@dataclass(frozen=True)
class ExecutionPolicy:
    ttl_seconds: int = 60
    renew_interval_seconds: float = 15
    busy_budget_seconds: float = 2
    act_deadline_seconds: float = 120

    def __post_init__(self):
        if type(self.ttl_seconds) is not int or not 1 <= self.ttl_seconds <= 3600:
            raise ValueError('INVALID_EXECUTION_TTL')
        for value in (self.renew_interval_seconds, self.busy_budget_seconds, self.act_deadline_seconds):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('INVALID_EXECUTION_POLICY')
        if (self.renew_interval_seconds >= self.ttl_seconds / 3
                or self.busy_budget_seconds > 2 or self.act_deadline_seconds > 120):
            raise ValueError('INVALID_EXECUTION_POLICY')


class ActorRunError(WorldError, RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code, code)


import hashlib


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(raw):
    return hashlib.sha256(canonical(json.loads(raw)).encode('utf-8')).hexdigest()


# V2 intent relation is inside the ORIGINAL attested authority module.
# Additional facade/provider files cannot replace this recovery predicate.
REAL_REQUEST_FIELDS={'schema_version','person_id','intent_id','body','messages','system_prompt','prompt','service_version'}


def request_digest(request):
    return hashlib.sha256(canonical(request).encode('utf-8')).hexdigest()


def make_request(person,intent,messages,system,prompt,service_version, *, identity_binding=None):
    if identity_binding is not None:
        request = dict(schema_version=3,person_id=person,intent_id=intent,body=None,
            messages=json.loads(canonical(messages)),system_prompt=system,prompt=prompt,
            service_version=service_version,identity_binding=json.loads(canonical(identity_binding)))
        if not valid_v3_request(request):
            raise ValueError('REAL_INVALID_REQUEST')
        return request
    return dict(schema_version=2,person_id=person,intent_id=intent,body=None,
                messages=json.loads(canonical(messages)),system_prompt=system,prompt=prompt,
                service_version=service_version)


def valid_request(q):
    if type(q) is dict and type(q.get('schema_version')) is int and q['schema_version'] == 3:
        return valid_v3_request(q)
    return (type(q) is dict and set(q)==REAL_REQUEST_FIELDS and type(q['schema_version']) is int
            and q['schema_version']==2 and q['body'] is None
            and all(type(q[k]) is str and q[k] for k in ('person_id','intent_id','system_prompt','prompt','service_version'))
            and len(q['service_version'])==64 and all(c in '0123456789abcdef' for c in q['service_version'])
            and type(q['messages']) is list)


def valid_usage(usage):
    return (type(usage) is dict and set(usage)=={'requests','input_tokens','output_tokens','cost'}
            and type(usage['requests']) is int and 1<=usage['requests']<=4
            and all(usage[k] is None or type(usage[k]) is int and 0<=usage[k]<=2**63-1
                    for k in ('input_tokens','output_tokens')) and usage['cost'] is None)


def make_candidate(request,text,usage):
    if type(request) is dict and type(request.get('schema_version')) is int and request['schema_version'] == 3:
        return make_v3_candidate(request,text,usage)
    if (not valid_request(request) or type(text) is not str or not text.strip()
            or len(text)>8000 or not valid_usage(usage)):
        raise ValueError('REAL_INVALID_CANDIDATE')
    body={'action':'propose','text':text}
    binding=dict(schema_version=2,person_id=request['person_id'],intent_id=request['intent_id'],
                 request_sha256=request_digest(request),body=body,usage=usage)
    messages=request['messages']+[{'role':'user','content':request['prompt']},
                                {'role':'assistant','content':text,'intent_binding':binding}]
    return dict(body=body,messages=messages,calls=[{'call_id':'real-'+request['intent_id']}])


def body_matches(request,body,candidate):
    """Total fail-closed predicate for native AND material recovery validators."""
    try:
        if type(request) is not dict:
            return False
        if type(request.get('schema_version')) is int and request['schema_version'] == 3:
            return v3_body_matches(request, body, candidate)
        if 'schema_version' not in request:
            return 'body' in request and request['body']==body
        if not valid_request(request):
            return False
        if candidate is None:
            return body is None
        messages=json.loads(candidate) if type(candidate) is str else candidate
        base=request['messages']
        if type(messages) is not list or len(messages)!=len(base)+2 or messages[:len(base)]!=base:
            return False
        if messages[-2]!={'role':'user','content':request['prompt']}:
            return False
        last=messages[-1]
        if type(last) is not dict or set(last)!={'role','content','intent_binding'} or last['role']!='assistant':
            return False
        text=last['content'];b=last['intent_binding']
        return (type(text) is str and bool(text.strip()) and len(text)<=8000
                and body=={'action':'propose','text':text} and type(b) is dict
                and set(b)=={'schema_version','person_id','intent_id','request_sha256','body','usage'}
                and type(b['schema_version']) is int and b['schema_version']==2
                and b['person_id']==request['person_id'] and b['intent_id']==request['intent_id']
                and b['request_sha256']==request_digest(request) and b['body']==body and valid_usage(b['usage']))
    except (ValueError,KeyError,TypeError,RecursionError):
        return False


import threading
import time
import uuid


def storage_retry(function, policy, *, deadline=None, stop=None, budget=None, _heartbeat_deadline=False):
    if type(budget) is PreparationBudget:
        return _preparation_storage_retry(function, budget, deadline=deadline, stop=stop)
    end = time.monotonic() + policy.busy_budget_seconds
    if deadline is not None:
        end = min(end, deadline)
    for index in range(3):
        try:
            return function()
        except Exception as error:
            if getattr(error, 'code', None) != 'EXECUTION_STORAGE_BUSY':
                raise
            delay = (.05, .10, 0)[index]
            if index == 2 or time.monotonic()+delay >= end:
                raise
            if stop is None:
                threading.Event().wait(delay)
            elif stop.wait(delay):
                raise
            if _heartbeat_deadline and time.monotonic() >= end:
                raise
    raise ActorRunError('EXECUTION_STORAGE_BUSY')


class EntryBudget:
    """Trusted pre-permit budget/cancellation, never an execution capability."""
    def __init__(self, policy, closing, captured=()):
        self.policy = policy
        self.stop = closing
        self.captured = tuple(item for item in captured if item is not None)
        now = time.monotonic()
        self.deadline = min([now + policy.act_deadline_seconds, *(item.deadline for item in self.captured)])
        self.busy_deadline = min(self.deadline, now + policy.busy_budget_seconds,
                                 *(getattr(item, 'busy_deadline', item.deadline) for item in self.captured))

    def check(self):
        if self.stop.is_set():
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        for item in self.captured:
            item.check()
        if time.monotonic() >= self.deadline:
            raise ActorRunError('ACTOR_EXECUTION_DEADLINE')
        if time.monotonic() >= self.busy_deadline:
            raise ActorRunError('EXECUTION_STORAGE_BUSY')


class _ContentionAccount:
    """One thread-owned union of contention intervals; never successful work."""
    def __init__(self, limit):
        self.owner = threading.current_thread()
        self.limit, self.spent = limit, 0.0
        self.depth, self.started = 0, None

    def owned(self):
        if threading.current_thread() is not self.owner:
            raise ActorRunError('PREPARATION_THREAD_MISMATCH')

    def remaining(self, now):
        self.owned()
        return self.limit - self.spent - (max(0.0, now-self.started) if self.depth else 0.0)


class PreparationBudget:
    """Internal synchronous startup IO policy, NOT an execution capability.

    Total wall time is finite (<=120s); actual contention is shared (<=2s).
    Captured legacy entries retain their original inclusive two-second limit.
    Native calls cannot be interrupted; a successful commit is not undone by
    a later deadline failure. Reconcile uncertain outcomes by original receipts.
    """
    def __init__(self, policy, closing, captured=()):
        self.policy, self.stop = policy, closing
        self.captured = tuple(item for item in captured if item is not None)
        now = time.monotonic()
        self.deadline = min([now+policy.act_deadline_seconds,
                             *(item.deadline for item in self.captured)])
        # Compatibility bound means the total deadline, not renewable wait time.
        self.busy_deadline = min([self.deadline,
            *(getattr(item, 'busy_deadline', item.deadline) for item in self.captured)])
        accounts = []
        for parent in self.captured:
            for item in _budget_chain(parent):
                if type(item) is PreparationBudget:
                    for account in item._accounts:
                        account.owned()
                        if not any(account is old for old in accounts):
                            accounts.append(account)
        if not accounts or policy.busy_budget_seconds < min(a.limit for a in accounts):
            accounts.append(_ContentionAccount(policy.busy_budget_seconds))
        self._accounts = tuple(accounts)

    def check(self):
        for account in self._accounts:
            account.owned()
        if self.stop.is_set():
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        for parent in self.captured:
            parent.check()
        now = time.monotonic()
        if now >= self.deadline:
            raise ActorRunError('ACTOR_EXECUTION_DEADLINE')
        if any(account.remaining(now) <= 0 for account in self._accounts):
            raise ActorRunError('EXECUTION_STORAGE_BUSY')

    def remaining(self):
        self.check()
        now = time.monotonic()
        return max(0.0, min(self.busy_deadline-now,
                           *(account.remaining(now) for account in self._accounts)))

    @contextmanager
    def _contention(self):
        self.check()
        now = time.monotonic()
        for account in self._accounts:
            if not account.depth:
                account.started = now
            account.depth += 1
        try:
            yield
        finally:
            now = time.monotonic()
            for account in self._accounts:
                account.depth -= 1
                if not account.depth:
                    account.spent += max(0.0, now-account.started)
                    account.started = None

    def _busy_elapsed(self, started):
        now = time.monotonic()
        for account in self._accounts:
            account.owned()
            if not account.depth:
                account.spent += max(0.0, now-started)

    def wait(self, delay, stop=None):
        self.check()
        event = self.stop if stop is None else stop
        with self._contention():
            stopped = event.wait(min(delay, self.remaining()))
        self.check()
        if stopped:
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')

    @contextmanager
    def locked(self, lock):
        self.check()
        acquired = False
        try:
            acquired = lock.acquire(blocking=False)
            while not acquired:
                self.check()
                with self._contention():
                    acquired = lock.acquire(timeout=min(.025, self.remaining()))
            self.check()
            yield
        finally:
            if acquired:
                lock.release()

    def native_call(self, function, *args, **kwargs):
        # _AttemptConnection sets native busy_timeout=0 via the base method.
        for index in range(3):
            self.check()
            started = time.monotonic()
            try:
                return function(*args, **kwargs)
            except sqlite3.OperationalError as error:
                if getattr(error, 'sqlite_errorcode', 0) & 255 not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    self.check()
                    raise
                self._busy_elapsed(started)
                self.check()
                if index == 2:
                    raise ActorRunError('EXECUTION_STORAGE_BUSY') from None
                self.wait((.05, .10)[index])
        raise AssertionError('unreachable')


def _preparation_storage_retry(function, budget, *, deadline=None, stop=None):
    for index in range(3):
        budget.check()
        if stop is not None and stop.is_set():
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        if deadline is not None and time.monotonic() >= deadline:
            raise ActorRunError('ACTOR_EXECUTION_DEADLINE')
        try:
            return function()
        except Exception as error:
            budget.check()
            if getattr(error, 'code', None) != 'EXECUTION_STORAGE_BUSY' or index == 2:
                raise
            delay = (.05, .10)[index]
            if deadline is not None:
                delay = min(delay, max(0.0, deadline-time.monotonic()))
            budget.wait(delay, stop)
    raise AssertionError('unreachable')


def _nested_entry_budget(policy, closing, captured):
    # Only an existing actual trusted preparation route propagates this mode.
    kind = PreparationBudget if any(type(item) is PreparationBudget
        for parent in captured for item in _budget_chain(parent)) else EntryBudget
    return kind(policy, closing, captured)


class Attempt:
    """One captured authority; never read a newer Host permit."""
    def __init__(self, permit, policy, *, coordinator=None, parent_attempt=None, activation=None, deadline=None):
        self.activation = activation
        self.permit = permit
        self.coordinator = coordinator
        self.parent_attempt = parent_attempt
        self.id = uuid.uuid4().hex
        self.policy = policy
        self.stop = threading.Event()
        self.failure = None
        self.deadline = time.monotonic() + policy.act_deadline_seconds
        if deadline is not None:
            self.deadline = min(self.deadline, deadline)
        self.thread = None
        self.calls = []
        self._lifecycle = threading.RLock()
        self._cancel_generation = 0
        self._finish_observation = None

    def cancel(self, error):
        """Monotonic cancellation; ordinary later failures never replace the first."""
        with self._lifecycle:
            if self.failure is None:
                self.failure = error
            self._cancel_generation += 1
            self.stop.set()

    def start(self, world):
        def maintain():
            while not self.stop.wait(self.policy.renew_interval_seconds):
                try:
                    self.check()
                    with world._actor_attempt(self):
                        storage_retry(lambda: world.renew_actor_execution(self.permit, ttl_seconds=self.policy.ttl_seconds, coordinator=self.coordinator),
                                      self.policy, deadline=self.deadline, stop=self.stop)
                except Exception as error:
                    self.cancel(error)
        self.thread = threading.Thread(target=maintain, name='actor-heartbeat-'+self.id, daemon=True)
        self.thread.start()

    def check(self):
        if self.parent_attempt is not None:
            self.parent_attempt.check()
        if self.failure is not None:
            raise self.failure
        if time.monotonic() >= self.deadline:
            self.cancel(ActorRunError('ACTOR_EXECUTION_DEADLINE'))
            raise self.failure

    def call(self, host, function, *args):
        self.check()
        done = threading.Event()
        outcome = []
        def run():
            host._local.attempt = self
            try:
                self.check()
                with host._authority._actor_attempt(self):
                    outcome.append((True, function(*args)))
            except BaseException as error:
                outcome.append((False, error))
            finally:
                done.set()
        thread = threading.Thread(target=run, name='actor-provider-'+self.id, daemon=True)
        self.calls.append(thread)
        thread.start()
        while not done.wait(min(.025, max(.001, self.deadline-time.monotonic()))):
            self.check()
        thread.join(timeout=.1)
        self.check()
        ok, value = outcome[0]
        if not ok:
            raise value
        return value

    def finish(self):
        with self._lifecycle:
            if self._finish_observation is None:
                self._finish_observation = (self.failure, self.stop.is_set(), self._cancel_generation)
            self.stop.set()  # Maintenance-only stop, not a cancellation.
        self.thread.join(timeout=2.5)
        return not self.thread.is_alive()


from contextvars import ContextVar
from enum import Enum


class DispatchExit(Enum):
    RETURNED = 'returned'
    RETURNED_RECONCILED = 'returned_reconciled'
    HOST_NONTERMINAL_ERROR = 'host_nonterminal_error'
    PROOF_FAILED = 'proof_failed'
    AUTHORITY_STOP = 'authority_stop'
    CONTROL_FLOW = 'control_flow'
    UNCLASSIFIED_ERROR = 'unclassified_error'


_dispatch_scope = ContextVar('original_dispatch_capture', default=None)


def _dispatch_invalid():
    raise ActorRunError('ACTIVATION_BINDING_CONFLICT')


def _budget_chain(item):
    if item is not None:
        yield item
        for captured in getattr(item, 'captured', ()):
            yield from _budget_chain(captured)


@dataclass(frozen=True, repr=False)
class DispatchCapture:
    _scope_nonce: object
    _world: WorldStore
    _host: ActorHost
    _storage: PrivateStore
    reference: ActivationRef
    roster_index: int
    _root: CoordinatorAttempt
    _root_permit: CoordinatorPermit
    _attempt: Attempt
    attempt_id: str
    _actor_identity: ExecutionPermit
    intent_id: str | None
    deadline: float
    busy_deadline: float
    _outer_actor: Attempt | None
    _outer_entry: EntryBudget | None
    _closing: threading.Event
    _coordinator_closing: threading.Event
    exit_disposition: DispatchExit
    failure_before_finish: BaseException | None
    stop_before_finish: bool
    maintenance_joined: bool
    providers_joined: bool
    lease_released: bool
    cleanup_pending: bool
    _cancel_generation: int
    _source_identity: tuple[Path, tuple[int, int], Path, tuple[int, int]]

    def check(self) -> None:
        slot = _dispatch_scope.get()
        if slot is None:
            _dispatch_invalid()
        slot.validate(self)
        if self.exit_disposition not in (DispatchExit.RETURNED, DispatchExit.RETURNED_RECONCILED):
            _dispatch_invalid()
        if (self.failure_before_finish is not None or self.stop_before_finish
                or not self.maintenance_joined or not self.providers_joined
                or not self.lease_released or self.cleanup_pending):
            _dispatch_invalid()
        self._root.check()  # Root remains higher priority than child failure.
        if self._closing.is_set() or self._coordinator_closing.is_set():
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        with self._attempt._lifecycle:
            if self._attempt.failure is not None:
                raise self._attempt.failure
            if self._attempt._cancel_generation != self._cancel_generation:
                raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
            if self._attempt._finish_observation != (None, False, self._cancel_generation):
                _dispatch_invalid()
        if self._outer_actor is not None:
            self._outer_actor.check()
        if self._outer_entry is not None:
            self._outer_entry.check()
        if time.monotonic() >= self.deadline:
            raise ActorRunError('ACTOR_EXECUTION_DEADLINE')
        if time.monotonic() >= self.busy_deadline:
            raise ActorRunError('EXECUTION_STORAGE_BUSY')

    def remaining(self) -> float:
        self.check()
        return max(0, self.deadline - time.monotonic())


class _DispatchSlot:
    def __init__(self, world, host, reference, root, roster_index):
        from .coordinator_scheduler import scoped, integer
        reference = scoped(world, reference)
        integer(roster_index)
        if (reference is None or root is None or root.world is not world
                or reference.world_id != root.permit.world_id
                or world._scoped_coordinator_attempt() is not root
                or host._authority is not world or host.world is not world
                or host._storage.world is not world or host._storage.host is not host):
            _dispatch_invalid()
        root.check()
        self.world, self.host, self.reference, self.root = world, host, reference, root
        self.storage = host._storage
        self.root_permit = root.permit
        self.roster_index = roster_index
        self.outer_actor = world._scoped_attempt()
        self.outer_entry = world._scoped_entry_budget()
        if self.outer_actor is not None:
            outer = self.outer_actor
            if (outer.permit.world_id != reference.world_id or outer.permit.person_id != reference.person_id
                    or outer.activation != reference or outer.parent_attempt is not root
                    or outer.coordinator != root.permit):
                _dispatch_invalid()
            outer.check()
        if self.outer_entry is not None:
            self.outer_entry.check()
        self.nonce = object()
        self.open = True
        self.entered = False
        self.attempt = None
        self.published = None
        self.coordinator_closing = None
        self.source_identity = (self.storage.world_path, self.storage.world_file,
                                self.storage.actor_path, self.storage.actor_file)

    def _bind_coordinator_closing(self, event):
        if not self.open or self.entered or self.coordinator_closing is not None:
            _dispatch_invalid()
        self.coordinator_closing = event

    def enter(self, host, person, coordinator, reference):
        if (not self.open or _dispatch_scope.get() is not self or self.entered
                or self.coordinator_closing is None or host is not self.host
                or person != self.reference.person_id or reference != self.reference
                or coordinator != self.root_permit or self.root.permit != self.root_permit
                or host._authority is not self.world or host.world is not self.world
                or host._storage is not self.storage or self.storage.world is not self.world
                or self.storage.host is not host
                or self.world._scoped_coordinator_attempt() is not self.root
                or self.world._scoped_attempt() is not self.outer_actor
                or self.world._scoped_entry_budget() is not self.outer_entry):
            _dispatch_invalid()
        self.root.check()
        if self.outer_actor is not None:
            self.outer_actor.check()
        if self.outer_entry is not None:
            self.outer_entry.check()
        self.entered = True

    def bind(self, attempt):
        if (not self.open or not self.entered or self.attempt is not None
                or attempt is None or attempt.parent_attempt is not self.root
                or attempt.coordinator != self.root_permit or attempt.activation != self.reference
                or attempt.permit.world_id != self.reference.world_id
                or attempt.permit.person_id != self.reference.person_id):
            _dispatch_invalid()
        self.attempt = attempt

    def publish(self, disposition, intent_id, *, maintenance_joined, providers_joined, lease_released):
        if (not self.open or _dispatch_scope.get() is not self
                or self.published is not None or self.attempt is None):
            _dispatch_invalid()
        attempt = self.attempt
        with attempt._lifecycle:
            observation = attempt._finish_observation
            if observation is None:
                _dispatch_invalid()
            failure, stopped, generation = observation
        deadlines = [attempt.deadline, *(item.deadline for outer in (self.outer_actor, self.outer_entry)
                                         for item in _budget_chain(outer))]
        deadline = min(deadlines)
        busy = min([deadline, *(getattr(item, 'busy_deadline', item.deadline)
                               for item in _budget_chain(self.outer_entry))])
        self.published = DispatchCapture(
            self.nonce, self.world, self.host, self.storage, self.reference, self.roster_index,
            self.root, self.root_permit, attempt, attempt.id, attempt.permit, intent_id,
            deadline, busy, self.outer_actor, self.outer_entry, self.host._closing,
            self.coordinator_closing, disposition, failure, stopped, maintenance_joined,
            providers_joined, lease_released, self.host.cleanup_pending, generation,
            self.source_identity)
        # Do not check deadlines here: normal Host returns retain their old semantics.

    def capture(self) -> DispatchCapture | None:
        if not self.open or _dispatch_scope.get() is not self:
            _dispatch_invalid()
        return self.published

    def validate(self, capture):
        if (not self.open or _dispatch_scope.get() is not self or self.published is not capture
                or capture._scope_nonce is not self.nonce or capture._world is not self.world
                or capture._host is not self.host or capture._storage is not self.storage
                or self.host._storage is not self.storage or self.host._authority is not self.world
                or self.host.world is not self.world or self.storage.world is not self.world
                or self.storage.host is not self.host or capture._root is not self.root
                or self.root.permit != self.root_permit or capture._root_permit != self.root_permit
                or capture.reference != self.reference or capture.roster_index != self.roster_index
                or capture._attempt is not self.attempt or self.attempt.id != capture.attempt_id
                or self.attempt.permit != capture._actor_identity
                or self.attempt.parent_attempt is not self.root
                or self.attempt.coordinator != self.root_permit or self.attempt.activation != self.reference
                or self.world._scoped_coordinator_attempt() is not self.root
                or self.world._scoped_attempt() is not self.outer_actor
                or self.world._scoped_entry_budget() not in (self.outer_entry, getattr(self, '_io', None))):
            _dispatch_invalid()


@contextmanager
def dispatch_capture_scope(world: WorldStore, host: ActorHost, reference: ActivationRef,
                           captured_root: CoordinatorAttempt, *, roster_index: int) -> Iterator[_DispatchSlot]:
    if _dispatch_scope.get() is not None:
        _dispatch_invalid()
    slot = _DispatchSlot(world, host, reference, captured_root, roster_index)
    token = _dispatch_scope.set(slot)
    try:
        yield slot
    finally:
        slot.open = False
        _dispatch_scope.reset(token)


class _DispatchWait:
    def __init__(self, capture):
        self.capture = capture
        self._event = threading.Event()

    def wait(self, seconds):
        end = min(time.monotonic() + seconds, self.capture.deadline, self.capture.busy_deadline)
        while True:
            self.capture.check()
            remaining = end - time.monotonic()
            if remaining <= 0:
                return False
            self._event.wait(min(.025, remaining))


class _DispatchReadBudget:
    def __init__(self, capture):
        self.policy = capture._attempt.policy
        self.deadline, self.busy_deadline = capture.deadline, capture.busy_deadline
        self.check = capture.check
        self.stop = _DispatchWait(capture)


@contextmanager
def _dispatch_read_scope(world: WorldStore, slot: _DispatchSlot, capture: DispatchCapture) -> Iterator[None]:
    slot.validate(capture)
    capture.check()
    if world is not capture._world:
        _dispatch_invalid()
    budget = _DispatchReadBudget(capture)
    previous = getattr(slot, '_io', None)
    slot._io = budget
    try:
        with world._coordinator_scope(capture._root_permit, capture._root), world._entry_budget(budget):
            yield
            capture.check()
    finally:
        slot._io = previous


class LegacyStoreAdapter:
    """Explicit allowlist for shipped standard pre-fencing World sources."""
    SOURCES = frozenset({
        '344e2d896e163fbec347fa45f1e58b05a8c216f6ebd6da1662e47d8fbd5957c6',
        'ec46999617663eca90e0ad5367fcc07b0d03e73436ac3291db1022f9fe846942',
    })

    @classmethod
    def open(cls, old, adjudicator):
        from .world import WorldStore
        if type(old).__name__ != 'WorldStore' or adjudicator is not None:
            raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
        try:
            source = Path(type(old).snapshot.__code__.co_filename)
            if hashlib.sha256(source.read_bytes()).hexdigest() not in cls.SOURCES:
                raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
            path = Path(old.db_path).resolve()
            if str(old.db_path) == ':memory:' or not path.is_file():
                raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
            with closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
                for table, columns in (('snapshots',['seq','state']), ('intents',['id','person','payload','receipt'])):
                    if [r[1] for r in db.execute('PRAGMA table_info('+table+')')] != columns:
                        raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
                first = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0])
                last = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            current = WorldStore(path)
            if first['world_id'] != last['world_id'] or canonical(old.snapshot()) != canonical(current.snapshot()):
                raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
            return current
        except (AttributeError, TypeError, ValueError, OSError, sqlite3.Error):
            raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED') from None


def authority_backend(world, adjudicator):
    from .world import WorldStore
    names = ('acquire_actor_execution', 'renew_actor_execution', 'release_actor_execution',
             '_check_actor_execution', '_execution_identity')
    if not hasattr(world, 'db_path') and not any(hasattr(world, name) for name in names):
        return None, 'protocol_double'
    if isinstance(world, WorldStore):
        if not all(callable(getattr(world, name, None)) for name in names):
            raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
        return world, 'current'
    if any(hasattr(world, name) for name in names):
        raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
    if hasattr(world, 'db_path'):
        return LegacyStoreAdapter.open(world, adjudicator), 'legacy_standard_adapter'
    return None, 'protocol_double'


class PrivateStore:
    def __init__(self, host, world):
        self.host, self.world = host, world
        self.world_path = Path(world.db_path).resolve()
        self.actor_path = Path(host.db_path).resolve()
        if (str(world.db_path) == ':memory:' or self.actor_path == self.world_path
                or (self.actor_path.exists() and os.path.samefile(self.actor_path, self.world_path))):
            raise ActorRunError('ACTOR_EXECUTION_BACKEND_UNSUPPORTED')
        self.world_file = self.world_path.stat().st_dev, self.world_path.stat().st_ino
        self.actor_file = ((self.actor_path.stat().st_dev, self.actor_path.stat().st_ino)
                           if self.actor_path.is_file() else None)
        self._bootstrap_file = None  # Only an O_EXCL file created by this store.
        self._bootstrap_origin = None  # Immutable original-root/source creation tuple.
        world._register_private_source(self)

    def _bootstrap_provenance(self, root):
        return (root, root.permit if root is not None else None, self.world, self.host,
                self.world_path, self.world_file, self.host._bound_world_id,
                self.actor_path, self._bootstrap_file)

    def paths(self):
        if (Path(self.world.db_path).resolve() != self.world_path
                or Path(self.host.db_path).resolve() != self.actor_path):
            raise ActorRunError('EXECUTION_WORLD_MISMATCH')
        for path, expected in ((self.world_path, self.world_file), (self.actor_path, self.actor_file)):
            if expected is not None and (path.stat().st_dev, path.stat().st_ino) != expected:
                raise ActorRunError('EXECUTION_WORLD_MISMATCH')

    @staticmethod
    def schema(db):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(None)
        for statement in (
            '''CREATE TABLE IF NOT EXISTS actor.actor_session_heads (
                person_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, version INTEGER NOT NULL,
                messages_sha256 TEXT NOT NULL, seen_sha256 TEXT NOT NULL,
                last_promoted_intent_id TEXT, migration_kind TEXT NOT NULL, proof_digest TEXT NOT NULL,
                original_messages TEXT, original_seen TEXT)''',
            '''CREATE TABLE IF NOT EXISTS actor.actor_intent_execution (
                intent_id TEXT PRIMARY KEY, person_id TEXT NOT NULL, world_id TEXT NOT NULL,
                base_version INTEGER NOT NULL, base_messages_sha256 TEXT NOT NULL, base_seen_sha256 TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL, owner_token TEXT NOT NULL, attempt_id TEXT NOT NULL,
                promoted_version INTEGER)'''):
            db.execute(statement)
        _history_cover(db,'actor')

    def head(self, db, person):
        session = db.execute('SELECT * FROM actor.actor_sessions WHERE person_id=?', (person,)).fetchone()
        head = db.execute('SELECT * FROM actor.actor_session_heads WHERE person_id=?', (person,)).fetchone()
        if (session is None or head is None or head['world_id'] != self.host._bound_world_id
                or type(head['version']) is not int or not 0 <= head['version'] <= 2**63-1
                or digest(session['messages']) != head['messages_sha256']
                or digest(session['seen']) != head['seen_sha256']):
            raise ActorRunError('ACTOR_SESSION_STALE')
        return head

    def legacy_chain(self, db, person, session, execution, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        """P072a uncompressed histories only: prove edges before numbering."""
        try:
            return self._legacy_chain(db, person, session, execution, attempt)
        except (ValueError, TypeError, KeyError, IndexError, AttributeError):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN') from None

    def _legacy_chain(self, db, person, session, execution, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        rows = [dict(r) for r in db.execute('SELECT * FROM actor.actor_intents WHERE person_id=?', (person,))]
        if sum(r['state'] != 'committed' for r in rows) > 1:
            raise ActorRunError('ACTOR_PENDING_CONFLICT')
        if db.execute('SELECT 1 FROM actor.actor_intent_execution WHERE person_id=?', (person,)).fetchone():
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        all_calls = [dict(r) for r in db.execute('SELECT * FROM actor.actor_calls WHERE person_id=?', (person,))]
        if any(c['intent_id'] not in {r['intent_id'] for r in rows} for c in all_calls):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        for row in rows:
            request = json.loads(row['request']);body = json.loads(row['body']);consumed = json.loads(row['consumed'])
            if (request['person_id'] != person or request['intent_id'] != row['intent_id'] or request['body'] != body
                    or type(request['messages']) is not list or type(consumed) is not list
                    or any(type(v) is not str for v in consumed) or len(set(consumed)) != len(consumed)):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            actual = db.execute('SELECT person,payload,receipt FROM main.intents WHERE id=?', (row['intent_id'],)).fetchone()
            receipt = json.loads(actual['receipt']) if actual and actual['receipt'] else None
            if actual and (actual['person'] != person or json.loads(actual['payload']).get('body') != body):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if receipt is not None and (receipt.get('world_id') != execution.world_id or receipt.get('person_id') != person
                                        or receipt.get('intent_id') != row['intent_id']):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['receipt'] is not None and json.loads(row['receipt']) != receipt:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['state'] == 'prepared' and row['candidate'] is None and receipt is None and row['metrics'] is None:
                if any(c['intent_id'] == row['intent_id'] for c in all_calls):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                row.update(parsed_request=request, parsed_candidate=None, parsed_consumed=consumed, actual_receipt=None)
                continue
            if ((row['state'] == 'committed' and (receipt is None or row['receipt'] is None))
                    or row['state'] not in ('committed', 'candidate') or row['candidate'] is None):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            candidate = json.loads(row['candidate']);base = request['messages'];metrics = json.loads(row['metrics'])
            if type(candidate) is not list or candidate[:len(base)] != base or len(candidate) <= len(base):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            tail = candidate[len(base):]
            if not base:
                if tail[0].get('role') != 'system' or tail[0].get('content') != request['system_prompt']:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                tail = tail[1:]
            if [m.get('role') for m in tail] != ['user','assistant','toolResult','assistant']:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if tail[0].get('content') != [{'type':'text','text':request['prompt']}]:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            calls = [c for c in tail[1]['content'] if c.get('type') == 'toolCall']
            maps = [c for c in all_calls if c['intent_id'] == row['intent_id']]
            if (len(calls) != 1 or calls[0].get('name') != 'record_proposal' or calls[0].get('arguments') != body
                    or tail[2].get('toolCallId') != calls[0]['id'] or tail[2].get('isError') is not False
                    or {c['call_id'] for c in maps} != {calls[0]['id']}
                    or metrics.get('provider') != 'scripted-pi' or metrics.get('sdk_requests') != 2
                    or type(metrics.get('sdk_events')) is not dict):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            row.update(parsed_request=request, parsed_candidate=candidate, parsed_consumed=consumed, actual_receipt=receipt)
        # A request equality edge, NOT rowid, clock, epoch or committed COUNT.
        remaining = list(rows);ordered=[];messages=[];seen=[];nodes=[([],[])]
        while remaining:
            next_rows = [r for r in remaining if r['parsed_request']['messages'] == messages]
            if len(next_rows) != 1:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            row=next_rows[0]
            if row['parsed_consumed'][:len(seen)] != seen:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            ordered.append((row, messages, seen));remaining.remove(row)
            if row['state'] != 'committed':
                if remaining:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                break
            messages=row['parsed_candidate'];seen=row['parsed_consumed'];nodes.append((messages,seen))
        current = (json.loads(session['messages']), json.loads(session['seen']))
        if current not in nodes:
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        repair = current != (messages,seen)
        proof = hashlib.sha256(canonical([dict(row=r, base_messages=m, base_seen=s) for r,m,s in ordered]).encode('utf-8')).hexdigest()
        for version, (row, base_messages, base_seen) in enumerate(ordered):
            db.execute('INSERT INTO actor.actor_intent_execution VALUES (?,?,?,?,?,?,?,?,?,?)',
                       (row['intent_id'], person, execution.world_id, version,
                        digest(canonical(base_messages)), digest(canonical(base_seen)), execution.epoch, execution.owner_token, attempt,
                        version+1 if row['state'] == 'committed' else None))
        promoted = [r for r,m,s in ordered if r['state'] == 'committed']
        final_raw_messages = promoted[-1]['candidate'] if promoted else session['messages']
        final_raw_seen = promoted[-1]['consumed'] if promoted else session['seen']
        db.execute('INSERT INTO actor.actor_session_heads VALUES (?,?,?,?,?,?,?,?,?,?)',
                   (person, execution.world_id, len(promoted), digest(final_raw_messages), digest(final_raw_seen),
                    promoted[-1]['intent_id'] if promoted else None,
                    'legacy_proven_chain_repair' if repair else 'legacy_proven_chain', proof,
                    session['messages'], session['seen']))
        if repair:
            db.execute('UPDATE actor.actor_sessions SET messages=?,seen=? WHERE person_id=?', (final_raw_messages, final_raw_seen, person))

    def audit(self, db, person):
        """Re-prove persisted lineage; the presence of a head is not proof."""
        try:
            self._audit(db, person)
        except (ValueError, KeyError, TypeError, IndexError):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN') from None

    def _audit(self, db, person):
        from .coordinator_scheduler import _lc_ro1132_reader_for, _lc_ro1132_cursor, _lc_ro1132_json, _lc_ro1132_digest, _lc_ro1132_canonical, _lc_ro1132_body_matches, _lc_ro1132_private_head, _lc_ro1132_paid_exists, _lc_ro1132_paid_phase_proof, _lc_ro1132_paid_rejection_proof, window_record, exists, phase_proof, rejection_proof, _lifecycle_value_context
        read = _lc_ro1132_reader_for(db, self)
        execute, decode = (db.execute, json.loads)
        native_digest, native_canonical, native_body_matches = (digest, canonical, body_matches)
        head = self.head
        if read is not None:
            _lifecycle_value_context.get().reserve(32768, 'ro1132-private-audit-frame-and-closures')
            execute = lambda sql, params=(): _lc_ro1132_cursor(read, sql, params)
            decode = lambda raw: _lc_ro1132_json(read, raw)
            native_digest = lambda raw: _lc_ro1132_digest(read, raw)
            native_canonical = lambda value: _lc_ro1132_canonical(read, value)
            native_body_matches = lambda request, body, candidate: _lc_ro1132_body_matches(read, request, body, candidate)
            head = lambda db, person: _lc_ro1132_private_head(read, self, db, person)
            exists = lambda db, table, schema='main': _lc_ro1132_paid_exists(read, db, table, schema)
            phase_proof = lambda world, db, row: _lc_ro1132_paid_phase_proof(read, world, db, row)
            rejection_proof = lambda world, db, row: _lc_ro1132_paid_rejection_proof(read, world, db, row)
        head = head(db, person)
        rows = {r['intent_id']: r for r in execute('SELECT * FROM actor.actor_intents WHERE person_id=?', (person,))}
        metas = execute('SELECT * FROM actor.actor_intent_execution WHERE person_id=? ORDER BY base_version', (person,)).fetchall()
        if len(rows) != len(metas) or {r['intent_id'] for r in metas} != set(rows):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        calls = execute('SELECT * FROM actor.actor_calls WHERE person_id=?', (person,)).fetchall()
        if any((r['intent_id'] not in rows for r in calls)):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        messages = []
        seen = []
        version = 0
        last = None
        pending = 0
        nodes = {0: (messages, seen)}
        promoted = [m for m in metas if m['promoted_version'] is not None]
        leaves = [m for m in metas if m['promoted_version'] is None]
        for meta in promoted + leaves:
            row = rows[meta['intent_id']]
            request = decode(row['request'])
            body = decode(row['body'])
            consumed = decode(row['consumed'])
            base = meta['base_version']
            if type(base) is not int or base not in nodes:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            base_messages, base_seen = nodes[base]
            if exists(db, 'coordinator_scheduler_registration'):
                activations = execute('SELECT * FROM coordinator_activations WHERE intent_id=?', (row['intent_id'],)).fetchall()
                if len(activations) != 1 or activations[0]['person_id'] != person or activations[0]['world_id'] != self.host._bound_world_id:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                window_record(db, self.host._bound_world_id, activations[0]['window_id'])
                phase_proof(self.world, db, activations[0])
            if meta['world_id'] != self.host._bound_world_id or type(meta['base_version']) is not int or meta['base_messages_sha256'] != native_digest(native_canonical(base_messages)) or (meta['base_seen_sha256'] != native_digest(native_canonical(base_seen))) or (request['messages'] != base_messages) or (request['person_id'] != person) or (request['intent_id'] != row['intent_id']) or (not native_body_matches(request, body, row['candidate'])) or (type(consumed) is not list) or (consumed[:len(base_seen)] != base_seen) or any((type(v) is not str for v in consumed)) or (len(set(consumed)) != len(consumed)):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            actual = execute('SELECT person,payload,receipt FROM main.intents WHERE id=?', (row['intent_id'],)).fetchone()
            receipt = decode(actual['receipt']) if actual and actual['receipt'] else None
            if actual and (actual['person'] != person or decode(actual['payload']).get('body') != body):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if receipt is not None and (receipt.get('world_id') != self.host._bound_world_id or receipt.get('person_id') != person or receipt.get('intent_id') != row['intent_id']):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['receipt'] is not None and decode(row['receipt']) != receipt:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            mapped = [c for c in calls if c['intent_id'] == row['intent_id']]
            if request.get('schema_version') in (2, 3) and row['candidate'] is not None:
                if len(mapped) != 1 or mapped[0]['call_id'] != 'real-' + row['intent_id']:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['candidate'] is None:
                if receipt is not None or row['state'] != 'prepared' or mapped or (row['metrics'] is not None):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            else:
                candidate = decode(row['candidate'])
                metrics = decode(row['metrics'])
                if type(candidate) is not list or candidate[:len(base_messages)] != base_messages or (not isinstance(metrics, dict)):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                no_work = candidate == base_messages and (not mapped) and (type(metrics.get('sdk_requests')) is int) and (metrics['sdk_requests'] == 0) and (type(metrics.get('sdk_events')) is int) and (metrics['sdk_events'] == 0)
                if not no_work and (len(candidate) <= len(base_messages) or not mapped):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if meta['promoted_version'] is not None:
                if base != version or row['state'] != 'committed' or receipt is None or (row['receipt'] is None) or (type(meta['promoted_version']) is not int) or (meta['promoted_version'] != version + 1):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                version += 1
                messages = candidate
                seen = consumed
                last = row['intent_id']
                nodes[version] = (messages, seen)
            elif row['state'] == 'rejected':
                activation = execute('SELECT * FROM coordinator_activations WHERE intent_id=?', (row['intent_id'],)).fetchone()
                if activation is None:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                rejection_proof(self.world, db, activation)
            else:
                pending += 1
                if base != version or pending > 1 or row['state'] not in ('prepared', 'candidate'):
                    raise ActorRunError('ACTOR_PENDING_CONFLICT')
        if version != head['version'] or last != head['last_promoted_intent_id'] or head['messages_sha256'] != native_digest(native_canonical(messages)) or (head['seen_sha256'] != native_digest(native_canonical(seen))):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')

    def _material_head(self, material, person):
        from .coordinator_scheduler import _c0_v3_material, _c0_v3_invoke
        if _c0_v3_material(material) is not None:
            return _c0_v3_invoke(material, self._material_head_v3, material, person)
        from .coordinator_scheduler import _c0_read
        session = _c0_read(material, 'actor.actor_sessions', (('person_id', (person,)[0]),), None, (), None, True)
        head = _c0_read(material, 'actor.actor_session_heads', (('person_id', (person,)[0]),), None, (), None, True)
        if session is None or head is None or head['world_id'] != self.host._bound_world_id or (type(head['version']) is not int) or (not 0 <= head['version'] <= 2 ** 63 - 1) or (digest(session['messages']) != head['messages_sha256']) or (digest(session['seen']) != head['seen_sha256']):
            raise ActorRunError('ACTOR_SESSION_STALE')
        return head


    def _material_audit_rules(self, material, person):
        from .coordinator_scheduler import _c0_v3_material, _c0_v3_invoke
        if _c0_v3_material(material) is not None:
            return _c0_v3_invoke(material, self._material_audit_rules_v3, material, person)
        from .coordinator_scheduler import _c0_read
        head = self._material_head(material, person)
        rows = {r['intent_id']: r for r in _c0_read(material, 'actor.actor_intents', (('person_id', (person,)[0]),), None, (), None, False)}
        metas = _c0_read(material, 'actor.actor_intent_execution', (('person_id', (person,)[0]),), None, (('base_version', False),), None, False)
        if len(rows) != len(metas) or {r['intent_id'] for r in metas} != set(rows):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        calls = _c0_read(material, 'actor.actor_calls', (('person_id', (person,)[0]),), None, (), None, False)
        if any((r['intent_id'] not in rows for r in calls)):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        messages = []
        seen = []
        version = 0
        last = None
        pending = 0
        nodes = {0: (messages, seen)}
        promoted = [m for m in metas if m['promoted_version'] is not None]
        leaves = [m for m in metas if m['promoted_version'] is None]
        for meta in promoted + leaves:
            row = rows[meta['intent_id']]
            request = json.loads(row['request'])
            body = json.loads(row['body'])
            consumed = json.loads(row['consumed'])
            base = meta['base_version']
            if type(base) is not int or base not in nodes:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            base_messages, base_seen = nodes[base]
            from .coordinator_scheduler import _c0_exists, _c0_phase_proof, _c0_window_record
            if _c0_exists(material, 'coordinator_scheduler_registration'):
                activations = _c0_read(material, 'main.coordinator_activations', (('intent_id', (row['intent_id'],)[0]),), None, (), None, False)
                if len(activations) != 1 or activations[0]['person_id'] != person or activations[0]['world_id'] != self.host._bound_world_id:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                _c0_window_record(material, self.host._bound_world_id, activations[0]['window_id'])
                _c0_phase_proof(self.world, material, activations[0])
            if meta['world_id'] != self.host._bound_world_id or type(meta['base_version']) is not int or meta['base_messages_sha256'] != digest(canonical(base_messages)) or (meta['base_seen_sha256'] != digest(canonical(base_seen))) or (request['messages'] != base_messages) or (request['person_id'] != person) or (request['intent_id'] != row['intent_id']) or (not body_matches(request, body, row['candidate'])) or (type(consumed) is not list) or (consumed[:len(base_seen)] != base_seen) or any((type(v) is not str for v in consumed)) or (len(set(consumed)) != len(consumed)):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            actual = _c0_read(material, 'main.intents', (('id', (row['intent_id'],)[0]),), ('person', 'payload', 'receipt'), (), None, True)
            receipt = json.loads(actual[2]) if actual and actual[2] else None
            if actual and (actual[0] != person or json.loads(actual[1]).get('body') != body):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if receipt is not None and (receipt.get('world_id') != self.host._bound_world_id or receipt.get('person_id') != person or receipt.get('intent_id') != row['intent_id']):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['receipt'] is not None and json.loads(row['receipt']) != receipt:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            mapped = [c for c in calls if c['intent_id'] == row['intent_id']]
            if request.get('schema_version') in (2, 3) and row['candidate'] is not None:
                if len(mapped) != 1 or mapped[0]['call_id'] != 'real-'+row['intent_id']:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['candidate'] is None:
                if receipt is not None or row['state'] != 'prepared' or mapped or (row['metrics'] is not None):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            else:
                candidate = json.loads(row['candidate'])
                metrics = json.loads(row['metrics'])
                if type(candidate) is not list or candidate[:len(base_messages)] != base_messages or (not isinstance(metrics, dict)):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                no_work = candidate == base_messages and (not mapped) and (type(metrics.get('sdk_requests')) is int) and (metrics['sdk_requests'] == 0) and (type(metrics.get('sdk_events')) is int) and (metrics['sdk_events'] == 0)
                if not no_work and (len(candidate) <= len(base_messages) or not mapped):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if meta['promoted_version'] is not None:
                if base != version or row['state'] != 'committed' or receipt is None or (row['receipt'] is None) or (type(meta['promoted_version']) is not int) or (meta['promoted_version'] != version + 1):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                version += 1
                messages = candidate
                seen = consumed
                last = row['intent_id']
                nodes[version] = (messages, seen)
            elif row['state'] == 'rejected':
                from .coordinator_scheduler import _c0_rejection_proof
                activation = _c0_read(material, 'main.coordinator_activations', (('intent_id', (row['intent_id'],)[0]),), None, (), None, True)
                if activation is None:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                _c0_rejection_proof(self.world, material, activation)
            else:
                pending += 1
                if base != version or pending > 1 or row['state'] not in ('prepared', 'candidate'):
                    raise ActorRunError('ACTOR_PENDING_CONFLICT')
        if version != head['version'] or last != head['last_promoted_intent_id'] or head['messages_sha256'] != digest(canonical(messages)) or (head['seen_sha256'] != digest(canonical(seen))):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')



    def _material_head_v3(self, material, person):
        from .coordinator_scheduler import _c0_v3_digest, _c0_v3_json, _c0_v3_hash, _c0_v3_body_matches, _c0_v3_copy, _c0_v3_canonical
        from .coordinator_scheduler import _c0_read
        session = _c0_read(material, 'actor.actor_sessions', (('person_id', (person,)[0]),), None, (), None, True)
        head = _c0_read(material, 'actor.actor_session_heads', (('person_id', (person,)[0]),), None, (), None, True)
        if session is None or head is None or head['world_id'] != self.host._bound_world_id or (type(head['version']) is not int) or (not 0 <= head['version'] <= 2 ** 63 - 1) or (_c0_v3_digest(material, session['messages']) != head['messages_sha256']) or (_c0_v3_digest(material, session['seen']) != head['seen_sha256']):
            raise ActorRunError('ACTOR_SESSION_STALE')
        return head

    def _material_audit_rules_v3(self, material, person):
        from .coordinator_scheduler import (_c0_read, _c0_v3_each, _c0_v3_json,
            _c0_v3_material, _c0_v3_adopt, _C0V3Loan, _c0_v3_digest, _c0_v3_hash,
            _c0_v3_body_matches, _c0_exists, _c0_phase_proof, _c0_window_record,
            _c0_rejection_proof)
        owner = _c0_v3_material(material)
        head = self._material_head(material, person)
        counts = dict(rows=0, metas=0)
        def private_row(row):
            counts['rows'] += 1
            meta = _c0_read(material, 'actor.actor_intent_execution', (('intent_id', row['intent_id']),), first=True)
            if meta is None or meta['person_id'] != person:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        _c0_v3_each(material, 'actor.actor_intents', (('person_id', person),), None, (), private_row)
        def meta_row(meta):
            counts['metas'] += 1
            row = _c0_read(material, 'actor.actor_intents', (('intent_id', meta['intent_id']),), first=True)
            if row is None or row['person_id'] != person:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        _c0_v3_each(material, 'actor.actor_intent_execution', (('person_id', person),), None, (), meta_row)
        if counts['rows'] != counts['metas']:
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        def orphan_call(call):
            row = _c0_read(material, 'actor.actor_intents', (('intent_id', call['intent_id']),), first=True)
            if row is None or row['person_id'] != person:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        _c0_v3_each(material, 'actor.actor_calls', (('person_id', person),), None, (), orphan_call)
        # Compact version -> original promoted intent key, never all historical messages.
        nodes = {0: None}
        progress = dict(version=0, last=None, pending=0)
        def audit_leaf(meta):
            row = _c0_read(material, 'actor.actor_intents', (('intent_id', meta['intent_id']),), first=True)
            request = _c0_v3_json(material, row['request'])
            body = _c0_v3_json(material, row['body'])
            consumed = _c0_v3_json(material, row['consumed'])
            base = meta['base_version']
            if type(base) is not int or base not in nodes:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if base == 0:
                base_messages, base_seen = [], []
            else:
                original_base = _c0_read(material, 'actor.actor_intents', (('intent_id', nodes[base]),), first=True)
                if original_base is None:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                base_messages = _c0_v3_json(material, original_base['candidate'])
                base_seen = _c0_v3_json(material, original_base['consumed'])
            if _c0_exists(material, 'coordinator_scheduler_registration'):
                activations = _c0_read(material, 'main.coordinator_activations', (('intent_id', row['intent_id']),))
                if len(activations) != 1 or activations[0]['person_id'] != person or activations[0]['world_id'] != self.host._bound_world_id:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                _c0_window_record(material, self.host._bound_world_id, activations[0]['window_id'])
                _c0_phase_proof(self.world, material, activations[0])
            if (meta['world_id'] != self.host._bound_world_id or type(meta['base_version']) is not int
                    or meta['base_messages_sha256'] != _c0_v3_hash(material, base_messages)
                    or meta['base_seen_sha256'] != _c0_v3_hash(material, base_seen)
                    or request['messages'] != base_messages or request['person_id'] != person
                    or request['intent_id'] != row['intent_id']
                    or not _c0_v3_body_matches(material, request, body, row['candidate'])
                    or type(consumed) is not list or consumed[:len(base_seen)] != base_seen
                    or any(type(v) is not str for v in consumed) or len(set(consumed)) != len(consumed)):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            actual = _c0_read(material, 'main.intents', (('id', row['intent_id']),), ('person', 'payload', 'receipt'), first=True)
            receipt = _c0_v3_json(material, actual[2]) if actual and actual[2] else None
            if actual and (actual[0] != person or _c0_v3_json(material, actual[1]).get('body') != body):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if receipt is not None and (receipt.get('world_id') != self.host._bound_world_id or receipt.get('person_id') != person or receipt.get('intent_id') != row['intent_id']):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['receipt'] is not None and _c0_v3_json(material, row['receipt']) != receipt:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            mapped = _c0_read(material, 'actor.actor_calls', (('person_id', person), ('intent_id', row['intent_id'])))
            if request.get('schema_version') in (2, 3) and row['candidate'] is not None:
                if len(mapped) != 1 or mapped[0]['call_id'] != 'real-' + row['intent_id']:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if row['candidate'] is None:
                if receipt is not None or row['state'] != 'prepared' or mapped or row['metrics'] is not None:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            else:
                candidate = _c0_v3_json(material, row['candidate'])
                metrics = _c0_v3_json(material, row['metrics'])
                if type(candidate) is not list or candidate[:len(base_messages)] != base_messages or not isinstance(metrics, dict):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                no_work = (candidate == base_messages and not mapped and type(metrics.get('sdk_requests')) is int
                           and metrics['sdk_requests'] == 0 and type(metrics.get('sdk_events')) is int and metrics['sdk_events'] == 0)
                if not no_work and (len(candidate) <= len(base_messages) or not mapped):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if meta['promoted_version'] is not None:
                if (base != progress['version'] or row['state'] != 'committed' or receipt is None
                        or row['receipt'] is None or type(meta['promoted_version']) is not int
                        or meta['promoted_version'] != progress['version'] + 1):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                next_version = progress['version'] + 1
                intent = row['intent_id']
                # Transfer small key ownership, not the candidate/seen graph, to audit frame.
                charge = owner.memory.hold(1024 + 4 * len(intent), 'c0-v3-private-version-spine-key')
                spine_loan = _C0V3Loan(owner, leases=(charge,))
                # The audit root (not this leaf frame) owns every spine key.
                spine_loans.append(spine_loan)
                nodes[next_version] = intent
                progress['version'], progress['last'] = next_version, intent
            elif row['state'] == 'rejected':
                activation = _c0_read(material, 'main.coordinator_activations', (('intent_id', row['intent_id']),), first=True)
                if activation is None:
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                _c0_rejection_proof(self.world, material, activation)
            else:
                progress['pending'] += 1
                if base != progress['version'] or progress['pending'] > 1 or row['state'] not in ('prepared', 'candidate'):
                    raise ActorRunError('ACTOR_PENDING_CONFLICT')
        spine_loans = []
        try:
            for promoted in (True, False):
                def visit(meta):
                    if (meta['promoted_version'] is not None) == promoted:
                        audit_leaf(meta)
                _c0_v3_each(material, 'actor.actor_intent_execution', (('person_id', person),),
                    None, (('base_version', False),), visit)
            if progress['version'] == 0:
                messages, seen = [], []
            else:
                final = _c0_read(material, 'actor.actor_intents', (('intent_id', nodes[progress['version']]),), first=True)
                messages = _c0_v3_json(material, final['candidate'])
                seen = _c0_v3_json(material, final['consumed'])
            if (progress['version'] != head['version'] or progress['last'] != head['last_promoted_intent_id']
                    or head['messages_sha256'] != _c0_v3_hash(material, messages)
                    or head['seen_sha256'] != _c0_v3_hash(material, seen)):
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        except BaseException:
            for loan in spine_loans:
                loan.fail()
            raise
        else:
            nodes.clear(); progress.clear()
            for loan in reversed(spine_loans):
                loan.close()
            spine_loans.clear()


    def _material_audit(self, material, person):
        try:
            self._material_audit_rules(material,person)
        except (ValueError, KeyError, TypeError, IndexError):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN') from None

    def adopt(self, db, person, execution, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        from .coordinator_scheduler import private_proof
        activation=private_proof(self.world, db, person, execution)
        if activation is not None and activation['phase']=='technical_blocked':
            from .world import WorldError
            raise WorldError('ACTIVATION_TECHNICAL_BLOCKED', '技术阻塞不授权私密收养或恢复')
        self.schema(db)
        db.execute('INSERT OR IGNORE INTO actor.actor_sessions(person_id) VALUES (?)', (person,))
        head = db.execute('SELECT * FROM actor.actor_session_heads WHERE person_id=?', (person,)).fetchone()
        if head is None:
            session = db.execute('SELECT * FROM actor.actor_sessions WHERE person_id=?', (person,)).fetchone()
            self.legacy_chain(db, person, session, execution, attempt)
        self.audit(db, person)
        # Full audit above proves every excluded rejection leaf.
        pending = db.execute("SELECT * FROM actor.actor_intents WHERE person_id=? AND state IN ('prepared','candidate')", (person,)).fetchall()
        if len(pending) > 1:
            raise ActorRunError('ACTOR_PENDING_CONFLICT')
        for row in pending:
            meta = db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (row['intent_id'],)).fetchone()
            if meta is None:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            self.check_base(db, row['intent_id'], person)
            db.execute('UPDATE actor.actor_intent_execution SET owner_epoch=?,owner_token=?,attempt_id=? WHERE intent_id=?',
                       (execution.epoch, execution.owner_token, attempt, row['intent_id']))

    def prepare(self, db, intent, person, execution, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        head = self.head(db, person)
        db.execute('INSERT INTO actor.actor_intent_execution VALUES (?,?,?,?,?,?,?,?,?,NULL)',
                   (intent, person, execution.world_id, head['version'], head['messages_sha256'], head['seen_sha256'],
                    execution.epoch, execution.owner_token, attempt))

    def check_base(self, db, intent, person, execution=None, attempt=None):
        head = self.head(db, person)
        meta = db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (intent,)).fetchone()
        if meta is None or meta['person_id'] != person or meta['world_id'] != self.host._bound_world_id:
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        if execution is not None and (meta['owner_epoch'] != execution.epoch or meta['owner_token'] != execution.owner_token
                                      or meta['attempt_id'] != attempt):
            raise ActorRunError('ACTOR_EXECUTION_FENCED')
        if (head['version'], head['messages_sha256'], head['seen_sha256']) != (meta['base_version'], meta['base_messages_sha256'], meta['base_seen_sha256']):
            raise ActorRunError('ACTOR_SESSION_STALE')
        return head, meta

    def reject(self, db, intent, person, execution, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        from .coordinator_scheduler import private_proof, rejection_proof, unproven
        activation = private_proof(self.world, db, person, execution, intent=intent)
        if activation is None or activation['phase'] != 'rejected':
            unproven()
        self.audit(db, person)
        result = rejection_proof(self.world, db, activation)
        if activation['private_closed']:
            return result
        self.check_base(db, intent, person, execution, attempt)
        changed = db.execute("UPDATE actor.actor_intents SET state='rejected' WHERE intent_id=? AND person_id=? AND state='candidate' AND receipt IS NULL",
                             (intent,person)).rowcount
        closed = db.execute("UPDATE coordinator_activations SET private_closed=1 WHERE world_id=? AND activation_id=? AND phase='rejected' AND private_closed=0",
                            (execution.world_id,activation['activation_id'])).rowcount
        if changed != 1 or closed != 1:
            unproven()
        self.audit(db, person)
        return result

    def finish(self, db, intent, person, candidate, receipt, execution, base_version, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        from .coordinator_scheduler import private_proof, unproven
        activation = private_proof(self.world, db, person, execution, intent=intent)
        maintenance = False
        if activation is not None and activation['phase'] == 'technical_blocked':
            from .coordinator_maintenance import begin_finish
            maintenance = begin_finish(self, db, intent, person, execution, attempt)
        if activation is not None and not maintenance and activation['phase'] not in ('world_committed','completed'):
            unproven()
        row = db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?', (intent, person)).fetchone()
        meta = db.execute('SELECT * FROM actor.actor_intent_execution WHERE intent_id=?', (intent,)).fetchone()
        actual = db.execute('SELECT person,payload,receipt FROM main.intents WHERE id=?', (intent,)).fetchone()
        if (row is None or meta is None or row['candidate'] is None or actual is None or actual['receipt'] is None
                or actual['person'] != person or json.loads(actual['payload']).get('body') != json.loads(row['body'])
                or json.loads(actual['receipt']) != receipt or row['candidate'] != candidate
                or (row['receipt'] is not None and json.loads(row['receipt']) != receipt)):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        request = json.loads(row['request'])
        if request['person_id'] != person or request['intent_id'] != intent or not body_matches(request, json.loads(row['body']), row['candidate']):
            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
        if meta['promoted_version'] is None:
            self.check_base(db, intent, person)
        self.audit(db, person)
        head = self.head(db, person)
        if meta['promoted_version'] is not None:
            if row['state'] != 'committed' or not meta['base_version'] < meta['promoted_version'] <= head['version']:
                raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            return
        # M1 proves the unchanged original native metadata in this transaction;
        # the new maintenance actor never adopts or overwrites its old owner.
        if maintenance:
            from .coordinator_maintenance import original_base
            head, meta = original_base(self, db, intent, person)
        else:
            head, meta = self.check_base(db, intent, person, execution, attempt)
        if base_version is not None and base_version != meta['base_version']:
            raise ActorRunError('ACTOR_SESSION_STALE')
        if head['version'] == 2**63-1:
            raise ActorRunError('ACTOR_SESSION_VERSION_EXHAUSTED')
        changed = db.execute('''UPDATE actor.actor_session_heads SET version=?,messages_sha256=?,seen_sha256=?,last_promoted_intent_id=?
            WHERE person_id=? AND world_id=? AND version=? AND messages_sha256=? AND seen_sha256=?''',
            (head['version']+1, digest(row['candidate']), digest(row['consumed']), intent, person, execution.world_id,
             head['version'], meta['base_messages_sha256'], meta['base_seen_sha256'])).rowcount
        if changed != 1:
            raise ActorRunError('ACTOR_SESSION_STALE')
        db.execute('UPDATE actor.actor_sessions SET messages=?,seen=? WHERE person_id=?', (row['candidate'], row['consumed'], person))
        db.execute("UPDATE actor.actor_intents SET state='committed',receipt=? WHERE intent_id=?", (canonical(receipt), intent))
        db.execute('UPDATE actor.actor_intent_execution SET promoted_version=? WHERE intent_id=?', (head['version']+1, intent))
        if activation is not None:
            if maintenance:
                changed = db.execute("UPDATE coordinator_activations SET phase='completed',private_closed=1,promoted_version=?,resume_phase=NULL,diagnostic_code=NULL WHERE world_id=? AND activation_id=? AND phase='technical_blocked' AND private_closed=0 AND owner_parent_epoch=? AND owner_actor_epoch=?",
                    (head['version']+1, execution.world_id, activation['activation_id'], activation['owner_parent_epoch'], activation['owner_actor_epoch'])).rowcount
                if changed != 1:
                    unproven()
                self.audit(db, person)
            else:
                db.execute("UPDATE coordinator_activations SET phase='completed',private_closed=1,promoted_version=? WHERE world_id=? AND activation_id=?",
                           (head['version']+1, execution.world_id, activation['activation_id']))

    def _open(self, attempt):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        self.paths()
        timeout = min(.25, self.host.execution_policy.busy_budget_seconds)
        if attempt is not None:
            attempt.check()
            timeout = min(timeout, max(.001, attempt.deadline-time.monotonic()))
        options = {}
        if isinstance(attempt, (EntryBudget, PreparationBudget)):
            from .world import _AttemptConnection
            options['factory'] = _AttemptConnection
        if type(attempt) is PreparationBudget:
            timeout = 0
        db = sqlite3.connect(str(self.world_path), timeout=timeout, **options)
        if options:
            db._attempt = attempt
        try:
            db.row_factory = sqlite3.Row
            # A failed main admission must not manufacture an empty configured
            # private file and poison the next legitimate missing-source retry.
            missing_entry = isinstance(attempt, (EntryBudget, PreparationBudget)) and not self.actor_path.exists()
            if missing_entry:
                db.execute('BEGIN IMMEDIATE')
                fd = os.open(self.actor_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                try:
                    stat = os.fstat(fd)
                    self._bootstrap_file = (stat.st_dev, stat.st_ino)
                    self._bootstrap_origin = self._bootstrap_provenance(self.world._scoped_coordinator_attempt())
                finally:
                    os.close(fd)
            db.execute('ATTACH DATABASE ? AS actor', (str(self.actor_path),))
            if not missing_entry:
                db.execute('BEGIN IMMEDIATE')
            return db
        except BaseException as error:
            db.rollback()
            db.close()
            if isinstance(error, sqlite3.OperationalError) and getattr(error, 'sqlite_errorcode', 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                if attempt is not None:
                    attempt.check()
                raise ActorRunError('EXECUTION_STORAGE_BUSY') from None
            raise

    @contextmanager
    def transaction(self, execution=None, person=None, *, initialize=False, attempt=None, _precommit=None):
        # Pipeline (when present) -> source configuration -> main/ATTACH.
        # Maintenance does not take either of these Python locks.
        from .coordinator_scheduler import source_lock
        with source_lock(self.world, attempt):
            options={} if _precommit is None else dict(_precommit=_precommit)
            with self._transaction(execution, person, initialize=initialize, attempt=attempt, **options) as db:
                yield db

    @contextmanager
    def _transaction(self, execution=None, person=None, *, initialize=False, attempt=None, _precommit=None):
        from .coordinator_scheduler import _lc_ro1127_write_guard
        _lc_ro1127_write_guard(self.world)
        captured = self.world._scoped_attempt()
        if initialize and captured is not None:
            execution, person, attempt = captured.permit, captured.permit.person_id, captured
        if not initialize and type(execution) is not ExecutionPermit:
            raise ActorRunError('ACTOR_EXECUTION_REQUIRED')
        io_attempt = self.world._scoped_entry_budget() or attempt
        try:
            db = storage_retry(lambda: self._open(io_attempt), self.host.execution_policy,
                               deadline=min(io_attempt.deadline, getattr(io_attempt, 'busy_deadline', float('inf'))) if io_attempt else None,
                               stop=io_attempt.stop if io_attempt else None, budget=io_attempt)
        except Exception:
            if io_attempt is not None:
                io_attempt.check()
            raise
        try:
            with closing(db):
                try:
                    state = json.loads(db.execute('SELECT state FROM main.snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                    self.world._execution_identity(db, state['world_id'])
                    if state['world_id'] != self.host._bound_world_id:
                        raise ActorRunError('EXECUTION_WORLD_MISMATCH')
                    self.world._check_coordinator_execution(db, state['world_id'], None)
                    if initialize and captured is not None:
                        self.world._check_actor_execution(db, state['world_id'], person, execution)
                    if not initialize:
                        binding = db.execute('SELECT world_id FROM actor.actor_world_binding WHERE singleton=1').fetchone()
                        if not binding or binding[0] != state['world_id']:
                            raise ActorRunError('ACTOR_WORLD_MISMATCH')
                        self.world._check_actor_execution(db, state['world_id'], person, execution)
                    yield db
                    self.paths()
                    if initialize and captured is not None:
                        self.world._check_actor_execution(db, state['world_id'], person, execution)
                    if not initialize:
                        self.world._check_actor_execution(db, state['world_id'], person, execution)
                        from .coordinator_scheduler import private_proof
                        private_proof(self.world, db, person, execution)
                    if attempt is not None:
                        attempt.check()
                    self.world._check_coordinator_execution(db, state['world_id'], None)
                    if _precommit is not None:_precommit(db)
                    db.commit()
                except BaseException:
                    db.rollback()
                    raise
            if self.actor_file is None:
                self.actor_file = self.actor_path.stat().st_dev, self.actor_path.stat().st_ino
            self._bootstrap_file = None
            self._bootstrap_origin = None
        except sqlite3.OperationalError as error:
            if getattr(error, 'sqlite_errorcode', 0) & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                raise ActorRunError('EXECUTION_STORAGE_BUSY') from None
            raise


REAL_REQUEST_V3_FIELDS = REAL_REQUEST_FIELDS | {'identity_binding'}


def valid_identity_binding(binding, person):
    return (type(binding) is dict and set(binding) == {
        'world_id','person_id','observation_revision','origin_ref','tenure_ref'}
        and type(binding['world_id']) is str and bool(binding['world_id'].strip())
        and binding['person_id'] == person and type(binding['person_id']) is str
        and type(binding['observation_revision']) is int
        and 1 <= binding['observation_revision'] <= 2**63-1
        and type(binding['origin_ref']) is str and bool(binding['origin_ref'].strip())
        and len(binding['origin_ref']) <= 128
        and (binding['tenure_ref'] is None or type(binding['tenure_ref']) is str
             and bool(binding['tenure_ref'].strip()) and len(binding['tenure_ref']) <= 1024))


def valid_physical_attempt(attempt):
    return (attempt is None or type(attempt) is dict and set(attempt) == {
        'mode','target_person_id','means_id'} and attempt['mode'] == 'strike_with_held_weapon'
        and all(type(attempt[k]) is str and bool(attempt[k].strip()) and len(attempt[k]) <= 128
                for k in ('target_person_id','means_id')))


def valid_v3_request(q):
    if not (type(q) is dict and set(q) == REAL_REQUEST_V3_FIELDS
            and type(q['schema_version']) is int and q['schema_version'] == 3
            and valid_identity_binding(q['identity_binding'], q['person_id'])):
        return False
    legacy = {k:v for k,v in q.items() if k != 'identity_binding'}
    legacy['schema_version'] = 2
    return valid_request(legacy)


def make_v3_candidate(request, response, usage):
    if (not valid_v3_request(request) or type(response) is not dict
            or set(response) != {'text','physical_attempt'}
            or type(response['text']) is not str or not response['text'].strip()
            or len(response['text']) > 8000 or not valid_physical_attempt(response['physical_attempt'])
            or not valid_usage(usage)):
        raise ValueError('REAL_INVALID_CANDIDATE')
    # Freeze the complete body; no caller-owned mutable physical anchor escapes.
    body = json.loads(canonical(dict(action='propose', **response)))
    binding = dict(schema_version=3,person_id=request['person_id'],intent_id=request['intent_id'],
        request_sha256=request_digest(request),body=body,usage=json.loads(canonical(usage)))
    messages = json.loads(canonical(request['messages'])) + [
        {'role':'user','content':request['prompt']},
        {'role':'assistant','content':response['text'],'intent_binding':binding}]
    return dict(body=body,messages=messages,calls=[{'call_id':'real-'+request['intent_id']}])


def v3_body_matches(request, body, candidate):
    if not valid_v3_request(request):
        return False
    if candidate is None:
        return body is None
    messages = json.loads(candidate) if type(candidate) is str else candidate
    base = request['messages']
    if (type(messages) is not list or len(messages) != len(base)+2
            or messages[:len(base)] != base or messages[-2] != {'role':'user','content':request['prompt']}):
        return False
    last = messages[-1]
    if (type(last) is not dict or set(last) != {'role','content','intent_binding'}
            or last['role'] != 'assistant' or type(body) is not dict
            or set(body) != {'action','text','physical_attempt'} or body['action'] != 'propose'
            or type(last['content']) is not str or not last['content'].strip()
            or len(last['content']) > 8000 or body['text'] != last['content']
            or not valid_physical_attempt(body['physical_attempt'])):
        return False
    binding = last['intent_binding']
    return (type(binding) is dict and set(binding) == {
        'schema_version','person_id','intent_id','request_sha256','body','usage'}
        and type(binding['schema_version']) is int and binding['schema_version'] == 3
        and binding['person_id'] == request['person_id'] and binding['intent_id'] == request['intent_id']
        and binding['request_sha256'] == request_digest(request) and binding['body'] == body
        and valid_usage(binding['usage']))


class LifecycleCallBudget:
    """Finite synchronous root DM IO policy, never an actor/root capability.

    The caller must retain the SAME PreparationBudget around prepare, DM,
    preview, commit and stale retries. No actor scope, worker or lease is made.
    """
    def __init__(self, entry, captured_root, host):
        from .coordinator_execution import CoordinatorAttempt
        if type(captured_root) is not CoordinatorAttempt:
            raise ActorRunError('COORDINATOR_EXECUTION_REQUIRED')
        captured_root.check()
        if type(entry) is not PreparationBudget:
            raise ActorRunError('ACTOR_EXECUTION_REQUIRED')
        entry.check()
        self.entry, self.captured_root, self.host = entry, captured_root, host
        self.world = captured_root.world
        self.root_permit = captured_root.permit
        self.deadline = entry.deadline
        self.calls = []  # Original Attempt.calls is a list of provider threads.
        # This adapter creates no provider thread; synchronous transport uses
        # the coordinator worker and close cancels the existing gateway owner.
        self.check()

    def check(self):
        root, entry, host, world = self.captured_root, self.entry, self.host, self.world
        root.check()  # Preserve captured root failure priority.
        entry.check()  # Same entry: cancellation, parent and contention accounts.
        if (root.world is not world or root.permit is not self.root_permit
                or host.world is not world or host._authority is not world
                or host._storage is None or host._storage.host is not host
                or host._storage.world is not world
                or world._scoped_coordinator_attempt() is not root
                or world._scoped_entry_budget() is not entry
                or world._scoped_attempt() is not None
                or host._bound_world_id != self.root_permit.world_id
                or self.deadline != entry.deadline):
            raise ActorRunError('ACTIVATION_BINDING_CONFLICT')
        if host._closed or host._closing.is_set():
            raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
        if time.monotonic() >= self.deadline:
            raise ActorRunError('ACTOR_EXECUTION_DEADLINE')


from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())
