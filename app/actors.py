"""Offline scripted characters hosted by the real Pi SDK, not a real LLM.

The Node worker only records a proposal. World writes happen here, AFTER the
candidate session is durable. Database files belong to one world/host service.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
import json
import os
from pathlib import Path
import queue
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import uuid
from .actor_execution import PrivateStore, ExecutionPolicy, ActorRunError, Attempt, authority_backend, storage_retry
from .execution import ExecutionPermit
from .actor_execution import body_matches
from .world import WorldError, _history_cover, _bootstrap_material_scope


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


class ActorHost:
    def __init__(self, world, db_path, *, adjudicator=None, dm_provider=None, execution_policy=None):
        if execution_policy is not None and type(execution_policy) is not ExecutionPolicy:
            raise ValueError('INVALID_EXECUTION_POLICY')
        self.execution_policy = execution_policy or ExecutionPolicy()
        if (adjudicator is None) != (dm_provider is None):
            raise ValueError('DM_CONFIGURATION_REQUIRES_ADJUDICATOR_AND_PROVIDER')
        if adjudicator is not None and (getattr(adjudicator, 'world', None) is not world
                or not callable(getattr(adjudicator, 'resolve', None)) or not callable(dm_provider)):
            raise ValueError('DM_CONFIGURATION_WORLD_OR_PROVIDER_MISMATCH')
        self.world = world
        self._owner_instance = uuid.uuid4().hex
        snapshot = getattr(world, 'snapshot', None)
        identity = snapshot().get('world_id') if callable(snapshot) else getattr(world, 'world_id', None)
        if not isinstance(identity, str) or not identity or identity.isspace():
            raise RuntimeError('ACTOR_WORLD_ID_REQUIRED')
        self._bound_world_id = identity
        self.adjudicator = adjudicator
        self.dm_provider = dm_provider
        self.real_gateway = None
        self.real_settings = None
        self.provider_router = None
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._worker_lock = threading.RLock()
        self._local = threading.local()
        self._pending_threads = []
        self._process = None
        self._closed = False
        self._closing = threading.Event()
        self._attempt = None
        self._lifecycle_active = None  # (captured root, IO budget, original gateway); no actor lease.
        self.cleanup_pending = False
        self._lease_cleanup_pending = False
        self._lease_cleanups = []
        self._responses = queue.Queue()
        self.worker_exit_code = None
        self.timeout = 30.0
        self._authority, self.backend_kind = authority_backend(world, adjudicator)
        self.execution_fenced = self._authority is not None
        self._schema = 'actor.' if self.execution_fenced else 'main.'
        self._storage = PrivateStore(self, self._authority) if self.execution_fenced else None
        self._bootstrap_pending = False
        if self._authority is not None:
            with self._authority._connection(readonly=True) as db:
                try:
                    self._authority._check_coordinator_execution(db, identity, None, readonly=True)
                except Exception as error:
                    if getattr(error, 'code', None) != 'COORDINATOR_EXECUTION_REQUIRED':
                        raise
                    self._bootstrap_pending = True
                    return  # Observer construction never bootstraps private storage.
        self._initialize_private()

    @contextmanager
    def _entry_scope(self):
        from .actor_execution import PreparationBudget, _nested_entry_budget
        world = self._authority
        outer = world._scoped_entry_budget() if world is not None else None
        if type(outer) is not PreparationBudget:
            yield
            return
        entry = _nested_entry_budget(self.execution_policy, self._closing,
            (world._scoped_attempt(), world._scoped_coordinator_attempt(), outer))
        with world._entry_budget(entry):
            yield
            entry.check()

    @contextmanager
    def _with_entry_pipeline(self):
        from .actor_execution import PreparationBudget
        entry = self._authority._scoped_entry_budget() if self._authority is not None else None
        if type(entry) is PreparationBudget:
            with entry.locked(self._lock):
                yield
        else:
            with self._lock:
                yield

    def _bootstrap_scheduler_private(self):
        """Only missing configured storage, under the original root and proof.

        The mode selects stricter validation; it is never an authority ticket.
        All source, genesis and empty-file facts are checked again in ATTACH.
        """
        from . import coordinator_scheduler as scheduler
        world = self._authority
        if world is None:
            scheduler.unproven()
        with self._entry_scope(), self._with_entry_pipeline(), scheduler.source_lock(world):
            attempt = world._scoped_coordinator_attempt()
            if attempt is None:
                scheduler.unproven()
            attempt.check()
            if self._storage.actor_file is not None:
                scheduler.unproven()
            if (self._storage._bootstrap_file is not None
                    and (self._storage._bootstrap_origin != self._storage._bootstrap_provenance(attempt)
                         or not self._storage.actor_path.is_file())):
                scheduler.unproven()
            if self._storage.actor_path.exists():
                stat = self._storage.actor_path.stat()
                if self._storage._bootstrap_file != (stat.st_dev, stat.st_ino):
                    scheduler.unproven()
            with world._connection(readonly=True) as db:
                db.execute('BEGIN')
                if scheduler.exists(db, 'coordinator_scheduler_registration'):
                    scheduler.unproven()
                scheduler.pristine_main(world, db, scheduler.latest(db))
            self._initialize_private(_scheduler_bootstrap=True)

    def _scheduler_bootstrap_guard(self, db, *, final=False, file_identity=None):
        from . import coordinator_scheduler as scheduler
        world = self._authority
        attempt = world._scoped_coordinator_attempt()
        if attempt is None or self._closing.is_set() or self._closed:
            scheduler.unproven()
        attempt.check()
        state = scheduler.latest(db)
        world._check_coordinator_execution(db, state['world_id'], attempt.permit)
        if scheduler.exists(db, 'coordinator_scheduler_registration'):
            scheduler.unproven()
        scheduler.pristine_main(world, db, state)
        storage = self._storage
        storage.paths()
        stat = storage.actor_path.stat()
        identity = (stat.st_dev, stat.st_ino)
        if (storage._bootstrap_file is None or identity != storage._bootstrap_file
                or storage._bootstrap_origin != storage._bootstrap_provenance(attempt)):
            scheduler.unproven()
        if file_identity is not None and identity != file_identity:
            scheduler.unproven()
        if storage.actor_file is not None:
            scheduler.unproven()
        active = [s for s in world._private_sources if not getattr(s.host, '_closed', False)]
        if storage not in active:
            scheduler.unproven()
        for source in active:
            if (type(source) is not PrivateStore or source.world is not world
                    or source.host.world is not world or source.host._authority is not world
                    or source.host._storage is not source or source.host._closing.is_set()
                    or source.actor_path != storage.actor_path
                    or source.actor_file not in (None, identity)):
                scheduler.unproven()
            source.paths()
        if final:
            scheduler.pristine(world, db, state)
        elif db.execute("SELECT 1 FROM actor.sqlite_master WHERE type='table' LIMIT 1").fetchone():
            scheduler.unproven()
        return identity

    def _initialize_private(self, *, _scheduler_bootstrap=False):
        identity = self._bound_world_id
        with self._private(initialize=True) as db, _bootstrap_material_scope(
                self._authority, db, enabled=_scheduler_bootstrap):
            bootstrap_identity = (self._scheduler_bootstrap_guard(db)
                                  if _scheduler_bootstrap else None)
            if not self.execution_fenced:
                db.execute('BEGIN IMMEDIATE')
            schema = 'actor.' if self.execution_fenced else ''
            db.execute(f'CREATE TABLE IF NOT EXISTS {schema}actor_world_binding (singleton INTEGER PRIMARY KEY CHECK(singleton=1), world_id TEXT NOT NULL)')
            if self.execution_fenced: _history_cover(db,'actor')
            binding = db.execute(f'SELECT world_id FROM {self._schema}actor_world_binding WHERE singleton=1').fetchone()
            if binding is not None and binding['world_id'] != identity:
                raise RuntimeError('ACTOR_WORLD_MISMATCH')
            self._check_stored_receipt_identities(db, identity)
            if binding is None:
                self._prove_legacy_ownership(db, identity)
                db.execute(f'INSERT INTO {self._schema}actor_world_binding VALUES (1,?)', (identity,))
            for statement in ('''
                CREATE TABLE IF NOT EXISTS actor_sessions (
                    person_id TEXT PRIMARY KEY, messages TEXT NOT NULL DEFAULT '[]',
                    seen TEXT NOT NULL DEFAULT '[]')''', '''
                CREATE TABLE IF NOT EXISTS actor_intents (
                    intent_id TEXT PRIMARY KEY, person_id TEXT NOT NULL,
                    body TEXT NOT NULL, state TEXT NOT NULL,
                    candidate TEXT, metrics TEXT, receipt TEXT,
                    consumed TEXT NOT NULL DEFAULT '[]', request TEXT)''', '''
                CREATE TABLE IF NOT EXISTS actor_calls (
                    call_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL,
                    person_id TEXT NOT NULL)'''):
                db.execute(statement.replace('CREATE TABLE IF NOT EXISTS ', 'CREATE TABLE IF NOT EXISTS '+schema))
            if self.execution_fenced: _history_cover(db,'actor')
            if _scheduler_bootstrap:
                self._scheduler_bootstrap_guard(db, final=True, file_identity=bootstrap_identity)
        self._bootstrap_pending = False

    def _check_stored_receipt_identities(self, db, identity):
        master = 'actor.sqlite_master' if self.execution_fenced else 'sqlite_master'
        if db.execute(f"SELECT 1 FROM {master} WHERE type='table' AND name='actor_intents'").fetchone():
            for row in db.execute(f'SELECT intent_id,person_id,receipt FROM {self._schema}actor_intents WHERE receipt IS NOT NULL'):
                try:
                    receipt = json.loads(row['receipt'])
                    if receipt.get('world_id') != identity:
                        raise ActorRunError('ACTOR_WORLD_MISMATCH')
                    if receipt.get('person_id') != row['person_id'] or receipt.get('intent_id') != row['intent_id']:
                        raise ActorRunError('ACTOR_WORLD_UNPROVEN')
                except (ValueError, TypeError, AttributeError):
                    raise ActorRunError('ACTOR_WORLD_UNPROVEN') from None

    def _prove_legacy_ownership(self, db, identity):
        """Adopt only state whose actual world receipts prove its principal/ID.

        A private session without a proven intent is NOT evidence of ownership.
        Called under the same write transaction as registration; failures roll
        back metadata/DDL as well as preserving every original session row.
        """
        master = 'actor.sqlite_master' if self.execution_fenced else 'sqlite_master'
        tables = {row['name'] for row in db.execute(f"SELECT name FROM {master} WHERE type='table'")}
        proven = set()
        if 'actor_intents' in tables:
            for row in db.execute(f'SELECT intent_id,person_id,receipt FROM {self._schema}actor_intents'):
                stored = None
                if row['receipt'] is not None:
                    try:
                        stored = json.loads(row['receipt'])
                    except (ValueError, TypeError) as error:
                        raise RuntimeError('ACTOR_WORLD_UNPROVEN') from error
                    if not isinstance(stored, dict) or not stored.get('world_id'):
                        raise RuntimeError('ACTOR_WORLD_UNPROVEN')
                    if stored['world_id'] != identity:
                        raise RuntimeError('ACTOR_WORLD_MISMATCH')
                if self.execution_fenced:
                    record = db.execute('SELECT receipt FROM main.intents WHERE id=? AND person=?', (row['intent_id'], row['person_id'])).fetchone()
                    receipt = json.loads(record[0]) if record and record[0] else None
                else:
                    receipt = self.world.receipt(row['person_id'], row['intent_id'])
                if not isinstance(receipt, dict) or not receipt.get('world_id'):
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN')
                if receipt['world_id'] != identity:
                    raise RuntimeError('ACTOR_WORLD_MISMATCH')
                if receipt.get('person_id') != row['person_id'] or receipt.get('intent_id') != row['intent_id']:
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN')
                if stored is not None and json.dumps(stored, sort_keys=True) != json.dumps(receipt, sort_keys=True):
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN')
                proven.add((row['person_id'], row['intent_id']))
        principals = {person for person, intent in proven}
        if 'actor_sessions' in tables:
            for row in db.execute(f'SELECT person_id,messages,seen FROM {self._schema}actor_sessions'):
                try:
                    messages, seen = json.loads(row['messages']), json.loads(row['seen'])
                except (ValueError, TypeError) as error:
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN') from error
                if not isinstance(messages, list) or not isinstance(seen, list):
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN')
                if (messages or seen) and row['person_id'] not in principals:
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN')
        if 'actor_calls' in tables:
            for row in db.execute(f'SELECT person_id,intent_id FROM {self._schema}actor_calls'):
                if (row['person_id'], row['intent_id']) not in proven:
                    raise RuntimeError('ACTOR_WORLD_UNPROVEN')

    @contextmanager
    def _private(self, execution=None, person=None, *, initialize=False):
        attempt = self._attempt
        if attempt is not None and attempt.permit == execution:
            attempt.check()
        context = self._storage.transaction(execution, person, initialize=initialize,
                                            attempt=attempt if attempt and attempt.permit == execution else None) if self.execution_fenced else self._connect()
        with self._entry_scope(), context as db:
            yield db
            if attempt is not None and attempt.permit == execution:
                attempt.check()

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @property
    def worker_pid(self):
        return self._process.pid if self._process else None

    def _start(self):
        with self._worker_lock:
            attempt = getattr(self._local, 'attempt', None)
            if attempt is not None:
                attempt.check()
            return self._start_worker()

    def _start_worker(self):
        if self._process is not None:
            if self._process.poll() is None:
                return
            self._stop()
        node = os.environ.get('PI_NODE_EXE') or shutil.which('node')
        if not node:
            raise RuntimeError('NODE_NOT_FOUND')
        # Strict allowlist: credentials from the parent environment are not copied.
        env = {k: v for k, v in os.environ.items() if k.upper() in
               {'PATH', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP', 'TMPDIR',
                'PI_MODULE_ROOT', 'NODE_COMPILE_CACHE'}}
        self._process = subprocess.Popen(
            [node, str(Path(__file__).with_name('pi_worker.mjs'))],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', env=env, bufsize=1)
        self._stderr = []
        self._responses = queue.Queue()
        process = self._process
        responses = self._responses
        stderr = self._stderr

        def read_stdout():
            try:
                for line in process.stdout:
                    responses.put(line)
            except (OSError, ValueError):
                pass
            finally:
                responses.put(None)

        def read_stderr():
            try:
                for line in process.stderr:
                    stderr.append(line[:500])
                    del stderr[:-20]
            except (OSError, ValueError):
                pass

        self._readers = [threading.Thread(target=read_stdout, daemon=True),
                         threading.Thread(target=read_stderr, daemon=True)]
        for reader in self._readers:
            reader.start()

    def bind_provider_router(self, router):
        """Bind an explicit real or scripted provider route once.

        Real mode is intentionally not allowed to retain a Pi fallback. The
        legacy direct attributes remain supported for existing hosts/tests.
        """
        from .provider_adapter import ProviderRouter
        if not isinstance(router, ProviderRouter):
            raise ValueError('INVALID_PROVIDER_ROUTER')
        if router.real != (self.real_gateway is not None):
            raise ValueError('PROVIDER_ROUTER_MODE_MISMATCH')
        if self.provider_router is not None and self.provider_router is not router:
            raise ValueError('PROVIDER_ROUTER_ALREADY_BOUND')
        self.provider_router = router
        return router.snapshot()

    def _request(self, payload):
        if self.provider_router is not None:
            return self.provider_router.role(payload, getattr(self._local, 'attempt', None))
        if self.real_gateway is not None:
            return self.real_gateway.role(payload, getattr(self._local, 'attempt', None))
        self._start()
        process, responses = self._process, self._responses
        request_id = uuid.uuid4().hex
        payload = dict(payload, id=request_id)
        try:
            process.stdin.write(_json(payload) + '\n')
            process.stdin.flush()
            line = responses.get(timeout=self.timeout)
        except (queue.Empty, OSError, ValueError) as error:
            self._stop()
            raise RuntimeError('PI_WORKER_TIMEOUT_OR_IO') from error
        if line is None:
            self._stop()
            raise RuntimeError('PI_WORKER_EXITED')
        try:
            response = json.loads(line)
        except (ValueError, TypeError) as error:
            self._stop()
            raise RuntimeError('PI_WORKER_PROTOCOL_ERROR: invalid JSON') from error
        if not isinstance(response, dict) or response.get('id') != request_id or not response.get('ok'):
            self._stop()
            raise RuntimeError('PI_WORKER_PROTOCOL_ERROR: invalid or unsuccessful reply')
        return response.get('result', response)

    def _stop(self):
        if self.provider_router is not None:
            self.provider_router.close()
        elif self.real_gateway is not None:
            self.real_gateway.close()
        with self._worker_lock:
            return self._stop_worker()

    def _stop_worker(self):
        process = self._process
        readers = getattr(self, '_readers', [])
        self._process = None
        self._readers = []
        self._responses.put(None)
        self._responses = queue.Queue()
        self._stderr = []
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        self.worker_exit_code = process.returncode
        for reader in readers:
            reader.join(timeout=1)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass

    def act(self, person_id, *, pause_on_failure=False, coordinator=None, activation=None):
        from .coordinator_scheduler import _lifecycle_operation_scope
        with _lifecycle_operation_scope():
            from .coordinator_scheduler import scoped
            activation = scoped(self._authority, activation) if self._authority is not None else activation
            if self._authority is not None:
                coordinator = self._authority._scoped_coordinator(coordinator)
            from .actor_execution import _dispatch_scope, DispatchExit
            dispatch = _dispatch_scope.get()
            if dispatch is not None:
                dispatch.enter(self, person_id, coordinator, activation)
            with self._with_entry_pipeline():
                if self._closed or self._closing.is_set():
                    raise RuntimeError('ACTOR_HOST_CLOSED')
                self._pending_threads = [t for t in self._pending_threads if t.is_alive()]
                self.cleanup_pending = self._lease_cleanup_pending or bool(self._pending_threads)
                if self._pending_threads:
                    raise ActorRunError('ACTOR_EXECUTION_CLEANUP_PENDING')
                issuer = self._authority
                acquire = getattr(issuer, 'acquire_actor_execution', None)
                permit = None
                if callable(acquire):
                    from .actor_execution import PreparationBudget, _nested_entry_budget
                    had_lease_cleanups = bool(self._lease_cleanups)
                    entry = _nested_entry_budget(self.execution_policy, self._closing,
                                        (issuer._scoped_attempt(), issuer._scoped_coordinator_attempt(),
                                         issuer._scoped_entry_budget() if (had_lease_cleanups
                                             or type(issuer._scoped_entry_budget()) is PreparationBudget) else None))
                    self._drain_lease_cleanups(entry)
                    entry.check()
                    if self.cleanup_pending:
                        raise ActorRunError('ACTOR_EXECUTION_CLEANUP_PENDING')
                    def acquire_entry():
                        entry.check()
                        try:
                            return acquire(person_id, self._owner_instance, ttl_seconds=self.execution_policy.ttl_seconds, coordinator=coordinator, activation=activation)
                        except Exception:
                            entry.check()  # Original cancellation/deadline wins over BUSY.
                            raise
                    with self._authority._entry_budget(entry):
                        try:
                            permit = storage_retry(acquire_entry, self.execution_policy, deadline=entry.busy_deadline, stop=entry.stop, budget=entry)
                        except Exception:
                            entry.check()  # Also check interrupted outer retry backoff.
                            raise
                attempt = Attempt(permit, self.execution_policy, coordinator=coordinator,
                                  parent_attempt=self._authority._scoped_coordinator_attempt(), activation=activation,
                                  deadline=entry.deadline) if permit is not None else None
                self._attempt = attempt
                if attempt is not None:
                    if self._closing.is_set():
                        attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'))
                    attempt.start(self._authority)
                disposition = DispatchExit.UNCLASSIFIED_ERROR
                returned_intent = None
                try:
                    if dispatch is not None:
                        dispatch.bind(attempt)
                    if attempt is not None:
                        with self._authority._actor_attempt(attempt):
                            for observation_try in range(3):
                                try:
                                    result = self._act(person_id, execution=permit, attempt_id=attempt.id)
                                    returned_intent = result.get('intent_id')
                                    disposition = DispatchExit.RETURNED
                                    return result
                                except Exception as conflict:
                                    if getattr(conflict,'code',None) != 'WINDOW_VERSION_CONFLICT' or observation_try == 2:
                                        raise
                                    attempt.check()
                    return self._act(person_id, execution=permit, attempt_id=None)
                except BaseException as error:
                    code = getattr(error, 'code', 'HOST_FAILURE')
                    if not isinstance(code, str) or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', code) is None:
                        code = 'HOST_FAILURE'
                    if not isinstance(error, Exception):
                        disposition = DispatchExit.CONTROL_FLOW
                    elif (attempt is not None and attempt.failure is not None
                            or code.startswith(('ACTOR_EXECUTION_', 'EXECUTION_', 'INVALID_EXECUTION', 'COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION', 'ACTIVATION_', 'INVALID_ACTIVATION', 'WINDOW_RECOVERY_UNPROVEN', 'WINDOW_VERSION_CONFLICT', 'ACTOR_RECOVERY_UNPROVEN', 'ACTOR_WORLD_', 'ACTOR_SESSION_'))):
                        disposition = DispatchExit.AUTHORITY_STOP
                    if (pause_on_failure and permit is not None
                            and (attempt is None or attempt.failure is None)
                            and (isinstance(error, Exception) or attempt is None or attempt.activation is None)
                            and not code.startswith(('ACTOR_EXECUTION_', 'EXECUTION_', 'INVALID_EXECUTION', 'COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION', 'ACTIVATION_', 'INVALID_ACTIVATION', 'WINDOW_RECOVERY_UNPROVEN', 'WINDOW_VERSION_CONFLICT', 'ACTOR_RECOVERY_UNPROVEN', 'ACTOR_WORLD_', 'ACTOR_SESSION_'))):
                        proof_failed = False
                        if isinstance(error, Exception) and attempt is not None and attempt.activation is not None:
                            try:
                                with self._authority._actor_attempt(attempt):
                                    terminal = self._terminal_ack_result(person_id, permit, attempt)
                                if terminal is not None:
                                    self._terminal_ack_diagnostic = {'code': code, 'phase':
                                        'rejected' if terminal.get('result_kind') == 'domain_rejected' else 'completed'}
                                    returned_intent = terminal.get('intent_id')
                                    disposition = DispatchExit.RETURNED_RECONCILED
                                    return terminal
                                disposition = DispatchExit.HOST_NONTERMINAL_ERROR
                            except Exception:
                                # No stronger-root fallback, no new budget, no pause of
                                # possibly terminal or successor facts. Keep original error.
                                proof_failed = True
                                disposition = DispatchExit.PROOF_FAILED
                                self._stop()
                        try:
                            if not proof_failed:
                                with self._authority._actor_attempt(attempt):
                                    self._authority.technical_pause(code, execution=permit)
                        except Exception:
                            pass  # An expired/fenced pause must not mask the original failure.
                    if (attempt is not None and attempt.failure is not None
                            or code.startswith(('ACTOR_EXECUTION_', 'EXECUTION_', 'INVALID_EXECUTION', 'COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION', 'ACTIVATION_', 'INVALID_ACTIVATION', 'WINDOW_RECOVERY_UNPROVEN', 'WINDOW_VERSION_CONFLICT', 'ACTOR_RECOVERY_UNPROVEN', 'ACTOR_WORLD_', 'ACTOR_SESSION_'))):
                        self._stop()
                    raise
                finally:
                    maintenance_joined = attempt is not None and attempt.finish()
                    if attempt is not None:
                        for thread in attempt.calls:
                            thread.join(timeout=.05)
                    providers_joined = attempt is not None and not any(t.is_alive() for t in attempt.calls)
                    lease_released = False
                    if maintenance_joined and providers_joined:
                        try:
                            if attempt.deadline <= time.monotonic() or getattr(attempt.failure, 'code', None) == 'ACTOR_EXECUTION_CANCELLED':
                                # One original .25s CAS release only; never restart a
                                # full retry budget after cancellation/deadline.
                                lease_released = issuer.release_actor_execution(permit) is True
                            else:
                                lease_released = storage_retry(lambda: issuer.release_actor_execution(permit), self.execution_policy,
                                              deadline=attempt.deadline) is True
                        except Exception:
                            pass  # Retain the original cleanup-only obligation below.
                    if attempt is not None:
                        if not lease_released:
                            self._lease_cleanups.append((issuer, permit, attempt))
                        self._lease_cleanup_pending = bool(self._lease_cleanups)
                        self._pending_threads.extend(t for t in attempt.calls if t.is_alive())
                        self.cleanup_pending = self._lease_cleanup_pending or bool(self._pending_threads)
                        if dispatch is not None and dispatch.attempt is attempt:
                            dispatch.publish(disposition, returned_intent,
                                maintenance_joined=maintenance_joined,
                                providers_joined=not any(t.is_alive() for t in attempt.calls),
                                lease_released=lease_released)
                    self._attempt = None

    def _terminal_ack_result(self, person_id, execution, attempt):
        """Read original terminal material under the captured scope and deadline.

        The ordinary private transaction supplies the original authority, binding,
        source/file and final gates. private_proof includes full native lineage;
        this path never adopts, closes, promotes or executes an activation.
        """
        from .coordinator_scheduler import private_proof
        with self._storage.transaction(execution, person_id, attempt=attempt) as db:
            activation = private_proof(self._authority, db, person_id, execution)
            terminal = activation is not None and (activation['phase'] == 'completed'
                       or activation['phase'] == 'rejected' and activation['private_closed'] == 1)
            if not terminal:
                return None
            original = db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=? AND person_id=?',
                                  (activation['intent_id'], person_id)).fetchone()
            metrics = json.loads(original['metrics'])
            from .coordinator_scheduler import unproven
            if (type(metrics) is not dict or set(metrics) != {'sdk_events','sdk_requests','provider','pid','module_root'}
                    or type(metrics['sdk_requests']) is not int or metrics['sdk_requests'] < 0
                    or type(metrics['pid']) is not int or metrics['pid'] < 0
                    or any(type(metrics[k]) is not str or not metrics[k] for k in ('provider','module_root'))):
                unproven()
            events = metrics['sdk_events']
            if not ((type(events) is int and events >= 0) or
                    (type(events) is dict and all(type(k) is str and type(v) is int and v >= 0 for k,v in events.items()))):
                unproven()
            result = dict(metrics, person_id=person_id,
                          intent_id=original['intent_id'], body=json.loads(original['body']),
                          receipt=json.loads(original['receipt']) if original['receipt'] is not None else None,
                          reconciled=True)
            if activation['phase'] == 'rejected':
                from .coordinator_scheduler import rejection_proof
                result.update(outcome=rejection_proof(self._authority, db, activation),
                              result_kind='domain_rejected')
            return result

    def _observation_envelope(self, person_id):
        return self._authority._observation_envelope(person_id)

    def _act(self, person_id, *, execution=None, attempt_id=None):
        with self._lock:
            if self._closed:
                raise RuntimeError('ACTOR_HOST_CLOSED')
            if self._bootstrap_pending:
                self._initialize_private()
            if self.execution_fenced:
                with self._private(execution, person_id) as db:
                    from .coordinator_scheduler import private_proof
                    activation = private_proof(self._authority, db, person_id, execution)
                    if activation is not None and activation['phase'] == 'technical_blocked':
                        raise WorldError('ACTIVATION_TECHNICAL_BLOCKED', '原激活已技术阻塞，禁止自动重执行')
                    self._storage.adopt(db, person_id, execution, attempt_id)
                    if activation is not None and activation['phase'] == 'completed':
                        original = db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=?', (activation['intent_id'],)).fetchone()
                        return dict(json.loads(original['metrics']), person_id=person_id,
                                    intent_id=original['intent_id'], body=json.loads(original['body']),
                                    receipt=json.loads(original['receipt']), reconciled=True)
                if activation is not None and activation['phase'] == 'rejected':
                    return self._reject_result(person_id, activation['intent_id'], execution, attempt_id, reconciled=True)
            with self._connect() as db:
                # Native audit/adopt above has proved every excluded rejected leaf.
                pending = db.execute("SELECT * FROM actor_intents WHERE person_id=? AND state IN ('prepared','candidate') ORDER BY rowid LIMIT 1", (person_id,)).fetchone()
            if pending is not None:
                saved_request = json.loads(pending['request'])
                if (saved_request.get('schema_version') in (2, 3)) != (self.real_gateway is not None):
                    raise ActorRunError('ACTOR_PROVIDER_MODE_MISMATCH')
                if not body_matches(saved_request, json.loads(pending['body']), pending['candidate']):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if pending is not None and pending['candidate']:
                # A missing acknowledgement is NOT permission to make a new intent.
                receipt = self.world.receipt(person_id, pending['intent_id'])
                reconciled = receipt is not None
                if receipt is None:
                    receipt = self._submit(person_id, pending['intent_id'], json.loads(pending['body']), execution=execution)
                if receipt.get('status') == 'domain_rejected':
                    return self._reject_result(person_id, pending['intent_id'], execution, attempt_id, reconciled=True)
                self._finish(pending['intent_id'], person_id, pending['candidate'], receipt, execution=execution, attempt_id=attempt_id)
                return dict(json.loads(pending['metrics']), person_id=person_id,
                            intent_id=pending['intent_id'], body=json.loads(pending['body']),
                            receipt=receipt, reconciled=reconciled)
            if pending is None:
                envelope = self._observation_envelope(person_id) if self.execution_fenced and activation is not None else None
                view = envelope['view'] if envelope is not None else self.world.observe(person_id)
                if view['person']['id'] != person_id:
                    raise RuntimeError('PRINCIPAL_MISMATCH')
                identity_binding = (self._authority.lifecycle_identity(person_id, revision=view['revision'])
                    if self.real_gateway is not None and self._authority is not None else None)
                if identity_binding is not None and (envelope is None
                        or identity_binding['world_id'] != envelope['world_id']
                        or identity_binding['observation_revision'] != envelope['revision']):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                with self._private(execution, person_id) as db:
                    if envelope is not None:
                        from .coordinator_scheduler import latest
                        current = latest(db)
                        card = self._authority._find(current['persons'], person_id, 'UNKNOWN_PERSON')
                        if (current['world_id'],current['revision'],card.get('card_version',1)) != (envelope['world_id'],envelope['revision'],envelope['card_version']):
                            raise ActorRunError('WINDOW_VERSION_CONFLICT')
                    db.execute(f'INSERT OR IGNORE INTO {self._schema}actor_sessions(person_id) VALUES (?)', (person_id,))
                    session = db.execute(f'SELECT * FROM {self._schema}actor_sessions WHERE person_id=?', (person_id,)).fetchone()
                    if identity_binding is not None:
                        # adopt/audit above reconstruct this person's spine from []/[];
                        # an office is never a source of another person's session.
                        head = self._storage.head(db, person_id)
                        if head['version'] == 0 and (json.loads(session['messages']) != []
                                or json.loads(session['seen']) != []):
                            raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                    intent_id = uuid.uuid4().hex
                    if self.real_gateway is not None:
                        body, consumed = None, json.loads(session['seen'])
                    else:
                        body, consumed = self._choose(view, json.loads(session['seen']))
                    identity = view['person']
                    office = {'king':'君主','finance':'内政财政官','marshal':'军事官'}.get(identity['office'], identity['office'])
                    request = {'person_id':person_id,'intent_id':intent_id,'body':body,
                        'messages':json.loads(session['messages']),
                        'system_prompt':f"你是{identity['name']}，担任{office}。只依亲见及实际收到的消息行动。" +
                            ' 你的处事原则：' + '；'.join(str(v) for v in identity.get('card', {}).values()),
                        'prompt':f"你现在位于{identity.get('location', '途中')}。" + '\n' + '\n'.join(
                            str(item.get('text', '')) for item in view.get('observations', []) + view.get('messages', []))}
                    if self.real_gateway is not None:
                        from .actor_execution import make_request
                        if identity_binding is None:
                            request = make_request(person_id, intent_id, request['messages'],
                                request['system_prompt'], _json(view), self.real_gateway.version())
                        else:
                            request = make_request(person_id, intent_id, request['messages'],
                                request['system_prompt'], _json(view), self.real_gateway.version(),
                                identity_binding=identity_binding)
                    db.execute(f'INSERT INTO {self._schema}actor_intents(intent_id,person_id,body,state,consumed,request) VALUES (?,?,?,?,?,?)',
                               (intent_id, person_id, _json(body), 'prepared', _json(consumed), _json(request)))
                    if self.execution_fenced:
                        self._storage.prepare(db, intent_id, person_id, execution, attempt_id)
                        from .coordinator_scheduler import activation_row, latest
                        from .actor_execution import digest
                        state = latest(db)
                        active = activation_row(self._authority, db, state, person_id, execution=execution)
                        if active is not None:
                            if active['phase'] != 'pending' or state['revision'] != view['revision']:
                                raise ActorRunError('WINDOW_VERSION_CONFLICT')
                            person = self._authority._find(state['persons'], person_id, 'UNKNOWN_PERSON')
                            base = self._storage.head(db, person_id)['version']
                            db.execute("UPDATE coordinator_activations SET phase='prepared',intent_id=?,observation_revision=?,card_version=?,base_session_version=?,request_sha256=? WHERE world_id=? AND activation_id=?",
                                       (intent_id, view['revision'], person.get('card_version',1), base, digest(_json(request)), state['world_id'], active['activation_id']))
            else:
                intent_id = pending['intent_id']
                body = json.loads(pending['body'])
                request = json.loads(pending['request'])
                if request['person_id'] != person_id or request['intent_id'] != intent_id:
                    raise RuntimeError('DURABLE_PRINCIPAL_MISMATCH')
                if self.world.receipt(person_id, intent_id) is not None:
                    raise RuntimeError('RECEIPT_WITHOUT_CANDIDATE_SESSION')
            is_real = request.get('schema_version') in (2, 3)
            if is_real != (self.real_gateway is not None):
                raise ActorRunError('ACTOR_PROVIDER_MODE_MISMATCH')
            attempt = self._attempt
            result = attempt.call(self, self._request, request) if attempt is not None else self._request(request)
            if is_real:
                body = result.get('body')
                if not body_matches(request, body, result.get('messages')):
                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
            if self._attempt is not None and self._attempt.permit == execution:
                self._attempt.check()
            metrics = {key: result[key] for key in ('sdk_events', 'sdk_requests', 'provider', 'pid', 'module_root')}
            with self._private(execution, person_id) as db:
                if self.execution_fenced:
                    self._storage.check_base(db, intent_id, person_id, execution, attempt_id)
                if is_real:
                    # V2 is an explicit one-way null -> model candidate transition.
                    # Request/hash remain immutable; v1 body never changes.
                    stored = db.execute(f'SELECT * FROM {self._schema}actor_intents WHERE intent_id=? AND person_id=?',
                                        (intent_id, person_id)).fetchone()
                    if (stored is None or stored['state'] != 'prepared' or stored['candidate'] is not None
                            or json.loads(stored['body']) is not None or json.loads(stored['request']) != request):
                        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                    changed = db.execute(f"UPDATE {self._schema}actor_intents SET state='candidate',body=?,candidate=?,metrics=? WHERE intent_id=? AND person_id=? AND state='prepared' AND candidate IS NULL AND request=? AND body=?",
                        (_json(body), _json(result['messages']), _json(metrics), intent_id, person_id, stored['request'], stored['body'])).rowcount
                    if changed != 1:
                        raise ActorRunError('ACTOR_SESSION_STALE')
                else:
                    db.execute(f'UPDATE {self._schema}actor_intents SET state=?,candidate=?,metrics=? WHERE intent_id=?',
                               ('candidate', _json(result['messages']), _json(metrics), intent_id))
                for call in result['calls']:
                    db.execute(f'INSERT INTO {self._schema}actor_calls VALUES (?,?,?)',
                               (call['call_id'], intent_id, person_id))
                if self.execution_fenced:
                    from .coordinator_scheduler import activation_row, latest, unproven
                    from .actor_execution import digest
                    active = activation_row(self._authority, db, latest(db), person_id, execution=execution, intent=intent_id)
                    if active is not None:
                        if active['phase'] != 'prepared':
                            unproven()
                        db.execute("UPDATE coordinator_activations SET phase='candidate',candidate_sha256=? WHERE world_id=? AND activation_id=?",
                                   (digest(_json(result['messages'])), execution.world_id, active['activation_id']))
            receipt = self._submit(person_id, intent_id, body, execution=execution)
            if receipt.get('status') == 'domain_rejected':
                return self._reject_result(person_id, intent_id, execution, attempt_id)
            self._finish(intent_id, person_id, _json(result['messages']), receipt, execution=execution, attempt_id=attempt_id)
            return dict(metrics, person_id=person_id, intent_id=intent_id, body=body, receipt=receipt)

    def _reject_result(self, person_id, intent_id, execution, attempt_id, *, reconciled=False):
        with self._private(execution, person_id) as db:
            outcome = self._storage.reject(db, intent_id, person_id, execution, attempt_id)
            row = db.execute('SELECT * FROM actor.actor_intents WHERE intent_id=?', (intent_id,)).fetchone()
            result = dict(json.loads(row['metrics']),person_id=person_id,intent_id=intent_id,
                          body=json.loads(row['body']),receipt=None,outcome=outcome,result_kind='domain_rejected')
            if reconciled:
                result['reconciled'] = True
            return result

    def _submit(self, person_id, intent_id, body, *, execution=None):
        if self.execution_fenced:
            if execution is None:
                raise ActorRunError('ACTOR_EXECUTION_REQUIRED')
            if type(execution) is not ExecutionPermit:
                raise ActorRunError('INVALID_EXECUTION')
        if self._attempt is not None and self._attempt.permit == execution:
            self._attempt.check()
        options = {'execution': execution} if execution is not None else {}
        if body.get('action') == 'propose' and self.adjudicator is not None:
            attempt = self._attempt
            provider_target = self.dm_provider
            if not callable(provider_target):
                raise ActorRunError('DM_PROVIDER_UNAVAILABLE')
            if self.real_gateway is not None:
                gateway = self.real_gateway
                router = self.provider_router
                with self._private(execution, person_id) as db:
                    row = db.execute(f'SELECT request FROM {self._schema}actor_intents WHERE person_id=? AND intent_id=?',
                                     (person_id, intent_id)).fetchone()
                    if row is None:
                        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                    original_request = json.loads(row['request'])
                    if original_request.get('schema_version') not in (2, 3):
                        raise ActorRunError('ACTOR_PROVIDER_MODE_MISMATCH')
                    version = original_request['service_version']
                if router is not None:
                    def routed_target(payload):
                        return router.dm(payload, getattr(self._local, 'attempt', None), version)
                    provider_target = routed_target
                else:
                    def gateway_target(payload):
                        return gateway.dm(payload, getattr(self._local, 'attempt', None), version)
                    provider_target = gateway_target
            provider = provider_target
            if attempt is not None and attempt.permit == execution:
                def attempted_provider(payload):
                    return attempt.call(self, provider_target, payload)
                provider = attempted_provider
            if 'physical_attempt' in body:
                # Preserve the complete durable v3 body; omission remains the
                # original v1/v2 keyword/call shape (not an injected null).
                options['physical_attempt'] = body['physical_attempt']
            return self.adjudicator.resolve(person_id, intent_id, body['text'], provider, **options)
        return (self._authority or self.world).submit(person_id, intent_id, body, **options)

    def _cancel_lifecycle(self, captured_root):
        # No pipeline/source/SQL lock: cancellation must reach the very gateway
        # currently blocked in synchronous IO, even when there is no actor.
        active = self._lifecycle_active
        if active is not None and active[0] is captured_root:
            active[2].close()

    def resolve_lifecycle_jobs(self, captured_root):
        """Root worker business, without an actor, inherited session or HTTP API.

            The Coordinator supplies its real closing-bound preparation parent.
            Each job retains one exact entry and IO object across all stale retries.
            Pipeline locking is stage-local; World owns and exits each SQL/source
            region before lock-free independent gateway IO.
            """
        from . import coordinator_scheduler as scheduler
        from .actor_execution import PreparationBudget, LifecycleCallBudget
        from .coordinator_execution import CoordinatorAttempt
        world = self._authority
        if type(captured_root) is not CoordinatorAttempt:
            raise ActorRunError('COORDINATOR_EXECUTION_REQUIRED')
        captured_root.check()
        if world is None or captured_root.world is not world or self.world is not world or (world._scoped_coordinator_attempt() is not captured_root) or (world._scoped_attempt() is not None):
            raise ActorRunError('ACTIVATION_BINDING_CONFLICT')
        parent = world._scoped_entry_budget()
        if type(parent) is not PreparationBudget:
            raise ActorRunError('ACTIVATION_BINDING_CONFLICT')
        parent.check()
        with parent.locked(self._lock), scheduler.source_lock(world):
            captured_root.check()
            if self._closed or self._closing.is_set():
                raise ActorRunError('ACTOR_EXECUTION_CANCELLED')
            if world.snapshot()['status'] == 'horizon_paused':
                return []
            jobs = world.pending_lifecycle_jobs()
        results = []
        for job_id in jobs:
            entry = PreparationBudget(self.execution_policy, self._closing, (captured_root, parent))
            with world._entry_budget(entry):
                with self._with_entry_pipeline():
                    captured_root.check()
                    entry.check()
                    gateway, adjudicator = (self.real_gateway, self.adjudicator)
                    if gateway is None or adjudicator is None or adjudicator.world is not world:
                        raise WorldError('ADJUDICATION_UNSUPPORTED', '继任须原独立真实DM配置')
                    service_version = gateway.version()
                with scheduler.lifecycle_scope(self, captured_root, entry, job_id, service_version):
                    io = LifecycleCallBudget(entry, captured_root, self)
                    self._lifecycle_active = (captured_root, io, gateway)
                    request_sha256 = None
                    try:
                        for retry in range(3):
                            with self._with_entry_pipeline():
                                io.check()
                                if self.real_gateway is not gateway or self.adjudicator is not adjudicator:
                                    raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                                prepared = adjudicator.prepare_lifecycle(job_id, service_version)
                            if prepared['status'] == 'completed':
                                results.append(prepared['result'])
                                break
                            request = prepared['request']
                            request_sha256 = prepared['request_sha256']
                            try:
                                io.check()
                                carrier = gateway.dm(request, io, service_version)
                                with self._with_entry_pipeline():
                                    io.check()
                                    if self.real_gateway is not gateway or self.adjudicator is not adjudicator:
                                        raise ActorRunError('ACTOR_RECOVERY_UNPROVEN')
                                    if type(carrier) is not dict or set(carrier) != {'response_raw', 'response'}:
                                        raise WorldError('INVALID_DECISION', '继任响应载体非法')
                                    from .world import _lc_reserve
                                    _lc_reserve(carrier,1,'gateway-response-handoff')
                                    preview = adjudicator.preview_lifecycle(job_id, request_sha256, carrier['response_raw'], carrier['response'])
                                    io.check()
                                    result = adjudicator.commit_lifecycle(job_id, request_sha256, carrier['response_raw'], carrier['response'], decision_sha256=preview['result']['decision_sha256'] if preview['status'] == 'completed' else preview['decision_sha256'])
                                    results.append(result)
                                    break
                            except WorldError as error:
                                if scheduler._lc_rollback1152_latch(error, self, captured_root, entry) is not None:
                                    # Never try the stale consumer for a rollback failure.
                                    # The terminal catch owns the check and its diagnostic.
                                    raise
                                io.check()
                                if not scheduler._lc_stale1141_accept(error):
                                    if error.code == 'STALE_PREVIEW':
                                        # An unauthenticated lookalike is a terminal
                                        # error, even when injected outside World.
                                        entry._lifecycle_value_owner.fail(error)
                                    raise
                                if retry == 2:
                                    raise WorldError('ADJUDICATION_RETRY_EXHAUSTED', '继任重裁额度耗尽') from None
                                continue
                    except BaseException as error:
                        latched = scheduler._lc_rollback1152_latch(error, self, captured_root, entry)
                        if latched is not None:
                            first, lane = latched
                            check = None
                            try:
                                io.check()  # original root/Entry/stop; terminal even if live
                            except BaseException as later:
                                if later is first[0]:
                                    check = first
                                elif lane[1] is not None and later is lane[1][0]:
                                    check = lane[1]
                                elif lane[2] is not None and later is lane[2][0]:
                                    check = lane[2]
                                else:
                                    check = (later, BaseException.__traceback__.__get__(later),
                                             BaseException.__context__.__get__(later),
                                             BaseException.__cause__.__get__(later))
                            slot = scheduler._lc_rollback1152_delivery.get()
                            if slot is not None and slot[0] is self and slot[1] is captured_root and slot[2] is None:
                                slot[2] = (first, lane, check)
                            raise  # A, not the check fault and not a synthetic error
                        if not isinstance(error, Exception):
                            raise
                        io.check()
                        code = getattr(error, 'code', 'ADJUDICATION_FAILED')
                        if type(code) is not str or re.fullmatch('[A-Z][A-Z0-9_]{0,79}', code) is None:
                            code = 'ADJUDICATION_FAILED'
                        if code.startswith(('ACTOR_EXECUTION_', 'EXECUTION_', 'INVALID_EXECUTION', 'COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION', 'ACTIVATION_', 'INVALID_ACTIVATION', 'WINDOW_RECOVERY_UNPROVEN', 'WINDOW_VERSION_CONFLICT', 'ACTOR_RECOVERY_UNPROVEN', 'ACTOR_WORLD_', 'ACTOR_SESSION_')):
                            raise
                        try:
                            with self._with_entry_pipeline():
                                if request_sha256 is not None:
                                    world.fail_lifecycle(job_id, request_sha256, code)
                                world.technical_pause(code)
                        except Exception:
                            io.check()
                            raise error
                        raise WorldError('ADJUDICATION_FAILED', '继任流程技术暂停') from None
                    finally:
                        self._lifecycle_active = None
        return results

    def _finish(self, intent_id, person_id, candidate, receipt, *, execution=None,
                base_session_version=None, attempt_id=None):
        if receipt.get('person_id') != person_id or receipt.get('intent_id') != intent_id:
            raise RuntimeError('RECEIPT_PRINCIPAL_MISMATCH')
        if receipt.get('world_id') != self._bound_world_id:
            raise RuntimeError('RECEIPT_WORLD_MISMATCH')
        with self._private(execution, person_id) as db:
            if self.execution_fenced:
                return self._storage.finish(db, intent_id, person_id, candidate, receipt, execution, base_session_version, attempt_id)
            db.execute(f'UPDATE {self._schema}actor_intents SET state=?,receipt=? WHERE intent_id=? AND person_id=?',
                       ('committed', _json(receipt), intent_id, person_id))
            consumed = db.execute(f'SELECT consumed FROM {self._schema}actor_intents WHERE intent_id=? AND person_id=?',
                                  (intent_id, person_id)).fetchone()['consumed']
            db.execute(f'UPDATE {self._schema}actor_sessions SET messages=?,seen=? WHERE person_id=?', (candidate, consumed, person_id))

    @staticmethod
    def _choose(view, seen):
        role = view['person']['office']
        if role in ('king', '君主'):
            finance = next((c for c in view['contacts'] if c['office'] in ('finance', '内政财政官')), None)
            for message in view['messages']:
                key = 'message:' + message['id']
                if key not in seen:
                    if finance:
                        return {'action':'command','to':finance['id'],
                                'text':'收到报告，请独立决定回应，核账后酌情赈粮；不得凭空增粮。原报告：' + message['text']}, seen + [key]
                    return {'action':'send','to':message['from'],
                            'text':'请进一步核查亲见情况与地点，未证实之事不要当作事实。'}, seen + [key]
        if role in ('finance', 'marshal', '内政财政官', '军事官'):
            for message in view['messages']:
                key = 'message:' + message['id']
                if key in seen:
                    continue
                command_id = message.get('command_id')
                if command_id and 'respond:' + message['id'] not in seen:
                    card = ' '.join(str(v) for v in view['person'].get('card', {}).values())
                    accept = not any(word in card for word in ('拒绝', '抗命', '不服从'))
                    return {'action':'respond','command_id':command_id,'accept':accept}, seen + ['respond:' + message['id']] + ([] if accept else [key])
                if role in ('finance', '内政财政官'):
                    check_key = 'ledger:' + message['id']
                    if check_key not in seen:
                        return {'action':'investigate'}, seen + [check_key]
                    target = re.search(r'(?:目的地|地点)[：:]\s*([^\s，。；;]+)', message['text'])
                    # Only a personally available ledger can justify even this tiny shipment.
                    ledger = next((obs for obs in reversed(view['observations'])
                                   if ('核账' in obs['text'] or '账簿' in obs['text'])
                                   and re.search(r'(?:粮食|粮)\s*(\d+)', obs['text'])), None)
                    if ledger and target:
                        quantity = int(re.search(r'(?:粮食|粮)\s*(\d+)', ledger['text']).group(1))
                        if quantity >= 10:
                            return {'action':'ship','to':target.group(1),'quantity':10}, seen + [key]
                    return {'action':'wait'}, seen
            king = next((c for c in view['contacts'] if c['office'] in ('king', '君主')), None)
            for obs in view['observations']:
                key = 'observation:' + obs['id']
                if key not in seen and any(word in obs['text'] for word in ('发热', '咳嗽', '症状')) and king:
                    return {'action':'send', 'to':king['id'],
                            'text':f"我在{view['person']['location']}亲见：{obs['text']}；病因尚未证实。"}, seen + [key]
        return {'action':'wait'}, seen

    @staticmethod
    def _cleanup_retired(issuer, permit, entry=None):
        """False CAS is not proof. Read only the original issuer's tombstone.

        No actor/root scope is re-entered: execution has ended, not recovered.
        Missing rows, malformed generations and identity errors remain pending.
        """
        if entry is not None:
            entry.check()
        issuer._validate_execution(permit)
        deadline = entry.busy_deadline if entry is not None else time.monotonic() + .25
        def progress():
            if entry is not None:
                entry.check()
            return int(time.monotonic() >= deadline)
        target = issuer.db_path.resolve().as_uri() + '?mode=ro'
        with closing(sqlite3.connect(target, uri=True, timeout=0)) as db:
            db.set_progress_handler(progress, 1000)
            db.execute('BEGIN')
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            issuer._execution_identity(db, state['world_id'])
            if state['world_id'] != permit.world_id:
                return False
            row = db.execute('SELECT epoch,owner_token FROM actor_execution_leases WHERE world_id=? AND person_id=?',
                             (permit.world_id, permit.person_id)).fetchone()
            if row is None or type(row[0]) is not int or not permit.epoch <= row[0] <= 2**63-1:
                return False
            return (row[1] is None or row[0] > permit.epoch
                    and type(row[1]) is str and bool(row[1].strip())) and time.monotonic() < deadline

    def _drain_lease_cleanups(self, entry=None):
        # Pipeline lock only. The admission entry is IO policy, not old authority.
        # close retains one native short CAS per obligation; act shares one entry.
        pending = list(self._lease_cleanups)
        try:
            for obligation in self._lease_cleanups:
                if entry is not None:
                    entry.check()
                issuer, permit, original = obligation
                if original.thread.is_alive() or any(t.is_alive() for t in original.calls):
                    continue
                original.thread.join(timeout=0)
                for thread in original.calls:
                    thread.join(timeout=0)
                released = False
                try:
                    if entry is None:
                        released = issuer.release_actor_execution(permit) is True
                    else:
                        with issuer._entry_budget(entry):
                            released = issuer.release_actor_execution(permit) is True
                    if not released:
                        released = self._cleanup_retired(issuer, permit, entry)
                except Exception:
                    pass  # Unknown retirement retains the exact original obligation.
                if released:
                    pending.remove(obligation)
                if entry is not None:
                    entry.check()  # Original stop/authority/deadline outranks cleanup pending.
        finally:
            self._lease_cleanups = pending
            self._lease_cleanup_pending = bool(pending)
            self._pending_threads = [t for t in self._pending_threads if t.is_alive()]
            self.cleanup_pending = bool(pending or self._pending_threads)

    def close(self):
        self._closing.set()
        attempt = self._attempt
        lifecycle = self._lifecycle_active
        if attempt is not None:
            attempt.cancel(ActorRunError('ACTOR_EXECUTION_CANCELLED'))
            self._stop()
        elif lifecycle is not None:
            # Root DM holds the pipeline lock, not an actor Attempt. Cancel its
            # ORIGINAL transport before waiting for that lock or any worker.
            self._cancel_lifecycle(lifecycle[0])
        with self._lock:
            self._drain_lease_cleanups()
            if self._closed:
                return
            try:
                if self._process is not None and self._process.poll() is None:
                    self._request({'op': 'close'})
                    self._process.wait(timeout=5)
            finally:
                self._stop()
                self._closed = True


# Diagnostic loaded generation only; no lifecycle or authority semantics change.
from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())
