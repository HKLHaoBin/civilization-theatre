"""Local-only HTTP surface; product world/SDK imports stay in main()."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import secrets
import threading
import re
from pathlib import Path
from urllib.parse import urlsplit,parse_qs

DAY = 86400
_CURRENT_OWNER = object()

class RequestError(Exception):
    def __init__(self,status,code,message):
        super().__init__(message)
        self.status,self.code,self.message=status,code,message

class Coordinator:
    def __init__(self, world, actors, *, execution_policy=None):
        from .actor_execution import authority_backend, ExecutionPolicy
        import uuid
        self.world, self.actors = world, actors
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.worker = None
        self.busy = False
        self.error = None
        self.failure_projection = None
        self._dispatch_ack_diagnostic = None
        self._authority, self.backend_kind = authority_backend(world, None)
        self._owner_instance = uuid.uuid4().hex
        self._owner_attempt = None
        self._owner_lock = threading.RLock()
        self._closed = threading.Event()
        self.execution_policy = execution_policy or ExecutionPolicy()
        self.cleanup_pending = False
        self._dispatch_local = threading.local()

    def acquire_owner(self):
        from .coordinator_execution import CoordinatorAttempt
        from .actor_execution import storage_retry
        with self._entry_lock(self._owner_lock):
            if self._closed.is_set():
                from .world import WorldError
                raise WorldError('COORDINATOR_EXECUTION_CANCELLED', '协调宿主已停止')
            if self._authority is None:
                return None
            if self._owner_attempt is None:
                permit = storage_retry(lambda: self._authority.acquire_coordinator_execution(self._owner_instance, ttl_seconds=self.execution_policy.ttl_seconds), self.execution_policy, stop=self._closed,
                    budget=self._authority._scoped_entry_budget())
                if self._closed.is_set():
                    from .world import WorldError
                    try:
                        self._authority.release_coordinator_execution(permit)
                    except Exception:
                        self.cleanup_pending = True
                    raise WorldError('COORDINATOR_EXECUTION_CANCELLED', '协调宿主已停止')
                self._owner_attempt = CoordinatorAttempt(self._authority, permit, self.execution_policy)
                captured = self._owner_attempt
                captured.start(lambda: self._cancel_children(captured))
            self._owner_attempt.check()
            return self._owner_attempt.permit

    def _cancel_children(self, captured_owner=_CURRENT_OWNER):
        captured_owner = self._owner_attempt if captured_owner is _CURRENT_OWNER else captured_owner
        if captured_owner is self._owner_attempt:
            self.stop.set()
        attempt = getattr(self.actors, '_attempt', None)
        if (attempt is not None and (captured_owner is None
                or (attempt.parent_attempt is captured_owner
                    and attempt.coordinator == captured_owner.permit))):
            from .actor_execution import ActorRunError
            # Unregistered legacy dispatch has no root cancellation origin.
            # Preserve its Host-close diagnostic without overwriting first failure.
            code = 'ACTOR_EXECUTION_CANCELLED' if captured_owner is None else 'COORDINATOR_EXECUTION_CANCELLED'
            attempt.cancel(ActorRunError(code))
        cancel_lifecycle = getattr(self.actors, '_cancel_lifecycle', None)
        if captured_owner is not None and callable(cancel_lifecycle):
            cancel_lifecycle(captured_owner)

    def _owner_scope(self, attempt=_CURRENT_OWNER):
        from contextlib import nullcontext
        attempt = self._owner_attempt if attempt is _CURRENT_OWNER else attempt
        if attempt is not None:
            attempt.check()
        return self._authority._coordinator_scope(attempt.permit, attempt) if attempt is not None else nullcontext()

    def recover_technical(self, ref, *, operation_id, repair_evidence):
        # Trusted backend only: deliberately absent from every HTTP action map.
        from .coordinator_maintenance import recover
        return recover(self, ref, operation_id=operation_id, repair_evidence=repair_evidence)

    def release_technical_window(self, ref, *, operation_id, repair_evidence):
        # Independent root-only operation; never published in HTTP action maps.
        from .coordinator_maintenance import release_window
        return release_window(self, ref, operation_id=operation_id, repair_evidence=repair_evidence)

    def edit_card(self, person_id, changes):
        self.acquire_owner()
        with self._owner_scope():
            return (self._authority or self.world).edit_card(person_id, changes)

    def snapshot(self,seq=None):
        state=self.world.snapshot(seq)
        adjudicator = getattr(self.actors, 'adjudicator', None)
        if adjudicator is not None:
            state['causality'] = adjudicator.trace(seq=state['revision'])
        state['runtime']=({'busy':self.busy,'pause_requested':self.stop.is_set(),'error':self.error}
                          if seq is None else {'busy':False,'pause_requested':False,'error':None,'historical':True})
        return state

    def _preparation_scope(self, captured_owner, *, check_exit=True):
        from contextlib import contextmanager
        from .actor_execution import PreparationBudget
        @contextmanager
        def scoped():
            if self._authority is None:
                yield
                return
            if captured_owner is not None:
                captured_owner.check()
            world = self._authority
            entry = PreparationBudget(self.execution_policy, self._closed,
                (world._scoped_attempt(), world._scoped_coordinator_attempt(),
                 captured_owner, world._scoped_entry_budget()))
            with world._entry_budget(entry):
                yield
                if check_exit:
                    entry.check()
        return scoped()

    def _entry_lock(self, lock):
        from contextlib import contextmanager
        @contextmanager
        def locked():
            entry = self._authority._scoped_entry_budget() if self._authority is not None else None
            if entry is None:
                with lock:
                    yield
                return
            from .actor_execution import PreparationBudget
            if type(entry) is PreparationBudget:
                with entry.locked(lock):
                    yield
                return
            while True:
                entry.check()
                if lock.acquire(timeout=.025):
                    break
            try:
                entry.check()
                yield
            finally:
                lock.release()
        return locked()

    def _prepare_scheduler(self, captured_owner):
        from .actors import ActorHost
        from contextlib import nullcontext
        with self._preparation_scope(captured_owner):
            # Keep pipeline -> source -> SQL even on missing-file bootstrap.
            lock = self._entry_lock(self.actors._lock) if isinstance(self.actors, ActorHost) else nullcontext()
            with lock:
                return self._prepare_scheduler_bounded(captured_owner)

    def _prepare_scheduler_bounded(self, captured_owner):
        """Prove the actual configured backend before any control or dispatch."""
        if self.backend_kind == 'protocol_double':
            return None
        from .actors import ActorHost
        from .actor_execution import PrivateStore
        from . import coordinator_scheduler as scheduler
        from .world import WorldError
        host = self.actors
        if (not isinstance(host, ActorHost) or type(host._storage) is not PrivateStore
                or host.world is not self.world or host._authority is not self._authority
                or host._storage.world is not self._authority
                or host._storage.host is not host
                or not all(callable(getattr(self._authority, name, None)) for name in (
                    'inspect_scheduler_boundary','register_scheduler','open_or_recover_window',
                    'next_activation','complete_window'))):
            raise WorldError('SCHEDULER_BACKEND_UNSUPPORTED', '需要真实持久角色宿主')
        captured_owner.check()
        world = self._authority
        with self._owner_scope(captured_owner):
            if not host._storage.actor_path.exists() or host._storage._bootstrap_file is not None:
                host._bootstrap_scheduler_private()
        with self._owner_scope(captured_owner), scheduler.source_lock(world), world._connection(readonly=True) as db:
            db.execute('BEGIN')
            state = scheduler.latest(db)
            world._check_coordinator_execution(db, state['world_id'], captured_owner.permit, readonly=True)
            storage, identity = scheduler.source(world)
            if scheduler.exists(db, 'coordinator_scheduler_registration'):
                db.execute('ATTACH DATABASE ? AS actor', (storage.actor_path.as_uri()+'?mode=ro',))
                result = scheduler.registration(db, state['world_id'], identity)
                scheduler.window_history(db, state['world_id'], state)
                for row in db.execute('SELECT window_id FROM coordinator_windows'):
                    scheduler.window_phase_proof(world, db, row[0])
                storage.paths()
                world._check_coordinator_execution(db, state['world_id'], captured_owner.permit, readonly=True)
                return result
        with self._owner_scope(captured_owner):
            inspection = world.inspect_scheduler_boundary(actor_db_path=host.db_path)
            if not inspection['eligible']:
                scheduler.unproven()
            return world.register_scheduler(coordinator=captured_owner.permit,
                expected_version={k:inspection[k] for k in ('world_id','revision')},
                boundary=inspection['boundary'], actor_db_path=host.db_path)

    def start(self,continuous=False):
        with self._preparation_scope(self._owner_attempt, check_exit=False), self._entry_lock(self.lock):
            return self._start_bounded(continuous)

    def _start_bounded(self,continuous=False):
        with self.lock:
            if self._closed.is_set():
                from .world import WorldError
                raise WorldError('COORDINATOR_EXECUTION_CANCELLED', '协调宿主已停止')
            if self.busy or self.cleanup_pending or (self.worker is not None and self.worker.is_alive()):
                raise RequestError(409,'BUSY','当前事件窗口尚未结束')
            self.failure_projection = None
            self._dispatch_ack_diagnostic = None
            self.acquire_owner()
            captured_owner = self._owner_attempt
            with self._owner_scope(captured_owner), self._preparation_scope(captured_owner):
                self._prepare_scheduler(captured_owner)
                state = self.world.snapshot()
                if state['status'] != 'running':
                    (self._authority or self.world).control('resume')
            if captured_owner is not None:
                captured_owner.check()
            entry = self._authority._scoped_entry_budget() if self._authority is not None else None
            if entry is not None:
                entry.check()  # Last synchronous gate, before publishing any worker.
            self.stop.clear(); self.error=None; self.busy=True
            try:
                self.worker=threading.Thread(target=self._step,args=(continuous,captured_owner),name='theatre-coordinator',daemon=True)
                self.worker.start()
            except BaseException:
                self.busy=False
                self.error='HOST_FAILURE'
                self.stop.set()
                raise
        return {'accepted':True}

    def _capture_failure(self, error, *, rollback=None):
        # Only the dispatch rethrow may reuse this transient strong identity.
        # Cleared in _step.finally; persistent state contains primitives only.
        first = getattr(self._dispatch_local, 'first_failure', None)
        if first is not None and first[0] is error:
            self.failure_projection = first[1]
            return first[1]
        if rollback is not None:
            followup_inputs = (('rollback_retry', rollback[1][1]),
                               ('connection_close', rollback[1][2]),
                               ('host_io_check', rollback[2]))
            # One transient strong table, at most four tuples, in first
            # observation order. All four bounded traversals share it.
            firsts = (rollback[0], *(saved for _, saved in followup_inputs if saved is not None))
        try:
            from .failure_projection import project
            projection = (project(error) if rollback is None else
                          project(error, first=rollback[0], _firsts=firsts))
        except BaseException:
            projection = {'schema':'safe-technical-failure-v1',
                          'origin':'projection_unavailable', 'raise_time':'unknown'}
        if rollback is not None:
            # B/C/check are independent of A and of each other. Only bounded
            # primitives escape; strong objects stay in the worker's scope.
            followups = []
            for stage, saved in followup_inputs:
                secondary = None
                if saved is not None:
                    try:
                        secondary = project(saved[0], first=saved, _firsts=firsts)
                    except BaseException:
                        secondary = {'schema':'safe-technical-failure-v1',
                                     'origin':'projection_unavailable', 'raise_time':'unknown'}
                followups.append({'stage': stage, 'failure': secondary})
            projection['rollback_followups'] = followups
        self.failure_projection = projection
        return projection

    def _step(self, continuous=False, captured_owner=None):
        self._dispatch_local.owner = captured_owner
        self._dispatch_local.first_failure = None
        self._dispatch_local.host_boundary = False
        slot = token = None
        try:
            if captured_owner is not None:
                from .coordinator_scheduler import _lc_rollback1152_delivery
                slot = [self.actors, captured_owner, None]
                token = _lc_rollback1152_delivery.set(slot)
            with self._owner_scope(captured_owner):
                self._run_step(continuous)
        except BaseException as error:
            rollback = slot[2] if slot is not None else None
            if rollback is not None and rollback[0][0] is not error:
                rollback = None
            if rollback is None:
                if not isinstance(error, Exception):
                    raise
                self._capture_failure(error)
                self.error = getattr(error, 'code', 'HOST_FAILURE')
            else:
                self.stop.set()
                self._capture_failure(error, rollback=rollback)
                self.error = object.__getattribute__(error, '__dict__').get('code', 'HOST_FAILURE')
            if not isinstance(self.error, str) or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', self.error) is None:
                self.error = 'HOST_FAILURE'
            self.stop.set()
            if rollback is not None and not isinstance(error, Exception):
                raise
            if (rollback is None and not self._dispatch_local.host_boundary
                    and not self.error.startswith(('COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION', 'ACTOR_EXECUTION_', 'EXECUTION_', 'INVALID_EXECUTION', 'ACTIVATION_', 'INVALID_ACTIVATION', 'WINDOW_RECOVERY_UNPROVEN', 'WINDOW_VERSION_CONFLICT', 'ACTOR_RECOVERY_UNPROVEN', 'ACTOR_WORLD_', 'ACTOR_SESSION_'))):
                try:
                    if ((captured_owner is not None or getattr(self.actors, 'execution_fenced', False) is not True)
                            and self.world.snapshot()['status'] != 'technical_paused'):
                        with self._owner_scope(captured_owner):
                            (self._authority or self.world).technical_pause(self.error)
                except Exception:
                    pass
        finally:
            try:
                if token is not None:
                    slot[2] = None
                    _lc_rollback1152_delivery.reset(token)
            finally:
                self._dispatch_local.first_failure = None
                self.busy = False

    def _drain_lifecycle_jobs(self, captured_root):
        # The actual closing Event is captured in this parent entry, not a
        # guessed Host.coordinator attribute or a caller-owned bool closure.
        with self._preparation_scope(captured_root):
            return self.actors.resolve_lifecycle_jobs(captured_root)

    def _run_step(self, continuous):
        captured = self._dispatch_local.owner
        if captured is None and self._authority is not None:
            from .coordinator_scheduler import exists
            from .world import WorldError
            with self._authority._connection(readonly=True) as db:
                if exists(db, 'coordinator_execution_leases') or exists(db, 'coordinator_scheduler_registration'):
                    raise WorldError('COORDINATOR_EXECUTION_REQUIRED', '持久调度需要原协调许可')
        if captured is not None:
            from .coordinator_scheduler import ActivationRef
            world = self._authority
            while True:
                captured.check()
                state = world.snapshot()
                if state['status'] == 'horizon_paused':
                    return
                self._drain_lifecycle_jobs(captured)
                # A durable prepare/result may have advanced the revision.
                state = world.snapshot()
                window = world.open_or_recover_window(coordinator=captured.permit,
                    expected_version={k:state[k] for k in ('world_id','revision')})
                if window['boundary_kind'] == 'horizon':
                    return
                self._drain_lifecycle_jobs(captured)  # Even a genuine empty roster.
                while True:
                    captured.check()
                    row = world.next_activation(window['window_id'], coordinator=captured.permit)
                    if row is None:
                        world.complete_window(window['window_id'], coordinator=captured.permit)
                        break
                    if row['phase'] == 'technical_blocked':
                        self.error = row['diagnostic_code'] or 'ACTIVATION_TECHNICAL_BLOCKED'
                        retained = self._dispatch_ack_diagnostic
                        activation_key = (row['world_id'], row['window_id'], row['activation_id'])
                        self.failure_projection = (retained['projection']
                            if retained is not None and retained['activation_key'] == activation_key
                            and retained['code'] == self.error else
                            {'schema':'safe-technical-failure-v1', 'origin':'stored_code_only',
                             'raise_time':'unknown'})
                        self.stop.set()
                        return
                    reference = ActivationRef(row['world_id'],row['window_id'],row['activation_id'],row['person_id'])
                    self._dispatch_ack_diagnostic = None
                    self.failure_projection = None
                    self._dispatch_local.host_boundary = True
                    from .actor_execution import dispatch_capture_scope
                    from .coordinator_scheduler import prove_dispatch_ack
                    with dispatch_capture_scope(world, self.actors, reference, captured,
                            roster_index=row['roster_index']) as slot:
                        slot._bind_coordinator_closing(self._closed)
                        try:
                            self.actors.act(row['person_id'], pause_on_failure=True,
                                coordinator=captured.permit, activation=reference)
                        except Exception as caller_error:
                            projection = self._capture_failure(caller_error)
                            self._dispatch_local.first_failure = (caller_error, projection)
                            acknowledged = False
                            try:
                                capture = slot.capture()
                                if capture is not None:
                                    capture.check()
                                    proof = prove_dispatch_ack(world, slot=slot, capture=capture)
                                    capture.check()
                                    acknowledged = proof.terminal is True
                            except Exception:
                                pass  # Retain original caller error, not the proof error.
                            if not acknowledged:
                                raise
                            code = getattr(caller_error, 'code', 'HOST_FAILURE')
                            if not isinstance(code, str) or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}', code) is None:
                                code = 'HOST_FAILURE'
                            self._dispatch_ack_diagnostic = {'code':code, 'phase':proof.phase,
                                'activation_key':(row['world_id'], row['window_id'], row['activation_id']),
                                'projection':projection}
                            self._dispatch_local.first_failure = None
                            self.failure_projection = None
                    self._dispatch_local.host_boundary = False
                if not continuous or self.stop.is_set():
                    if world.snapshot()['status'] == 'running':
                        world.control('pause')
                    return
        while True:
            state = self.world.snapshot()
            if state['status'] == 'horizon_paused':
                break
            (self._authority or self.world).advance(min(state['time'] + DAY, state['horizon']))
            if self.world.snapshot()['status'] == 'running':
                for person in self.world.snapshot()['persons']:
                    if getattr(self.actors, 'execution_fenced', False) is True:
                        self.actors.act(person['id'], pause_on_failure=True)
                    else:
                        self.actors.act(person['id'])
            if not continuous or self.stop.is_set():
                if self.world.snapshot()['status'] == 'running':
                    (self._authority or self.world).control('pause')
                break

    def pause(self):
        self.acquire_owner()
        self.stop.set()
        with self.lock:
            if not self.busy and self.world.snapshot()['status'] == 'running':
                with self._owner_scope():
                    (self._authority or self.world).control('pause')
        return self.snapshot()

    def close(self):
        self._closed.set()
        if self._owner_attempt is not None:
            self._owner_attempt.stop.set()
        self._cancel_children()
        if self.actors is None:
            pass
        elif getattr(self.actors, 'execution_fenced', False) is True:
            self.actors.close()
            if self.worker and self.worker.is_alive(): self.worker.join(5)
        else:
            if self.worker and self.worker.is_alive(): self.worker.join(5)
            self.actors.close()
            if self.worker and self.worker.is_alive(): self.worker.join(5)
        with self._owner_lock:
            owner = self._owner_attempt
            maintenance_done = owner is None or owner.finish()
            pending = (bool(self.worker and self.worker.is_alive())
                       or bool(getattr(self.actors, 'cleanup_pending', False))
                       or any(t.is_alive() for t in getattr(self.actors, '_pending_threads', ())))
            self.cleanup_pending = pending or not maintenance_done
            if not self.cleanup_pending and owner is not None:
                try:
                    self._authority.release_coordinator_execution(owner.permit)
                except Exception:
                    self.cleanup_pending = True

class TheatreServer(ThreadingHTTPServer):
    daemon_threads=True
    def handle_error(self,request,client_address):
        pass  # Never print callback request/exception context to stderr.
    def server_close(self):
        self.coordinator.close()
        settings=getattr(getattr(self.coordinator,'actors',None),'real_settings',None)
        self.siwc_close_result=settings.siwc.close() if settings is not None else None
        self.account_cleanup_pending=bool(self.siwc_close_result and self.siwc_close_result.get('account_mutation_pending'))
        super().server_close()
        return self.siwc_close_result


def make_server(world,actors,port=17431, *, coordinator=None):
    csrf=secrets.token_urlsafe(32)
    if coordinator is None:
        coordinator=Coordinator(world,actors)
    elif coordinator.world is not world or coordinator.actors is not actors:
        raise ValueError('COORDINATOR_WORLD_OR_ACTORS_MISMATCH')
    class Handler(BaseHTTPRequestHandler):
        def log_request(self,*args):
            pass  # Includes raw OAuth code/state query: NEVER access-log it.
        def log_message(self,*args):
            pass
        def send_payload(self,code,payload,mime):
            self.send_response(code)
            self.send_header('Content-Type',mime+'; charset=utf-8')
            self.send_header('Cache-Control','no-store')
            self.send_header('Referrer-Policy','no-referrer')
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Security-Policy',"default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'")
            self.send_header('Content-Length',str(len(payload)))
            self.end_headers(); self.wfile.write(payload)
        def reply(self,code,value):
            payload=json.dumps(value,ensure_ascii=False,allow_nan=False).encode('utf-8')
            self.send_payload(code,payload,'application/json')
        def trusted(self,write=False):
            port=self.server.server_address[1]
            hosts={f'127.0.0.1:{port}',f'localhost:{port}'}
            origin=self.headers.get('Origin')
            host=self.headers.get('Host','').lower()
            if host not in hosts or len(self.headers.get_all('Host',[]))!=1:
                return False
            if len(self.headers.get_all('Origin', [])) > 1 or len(self.headers.get_all('X-Theatre-CSRF', [])) > 1:
                return False
            if origin is not None and origin not in {f'http://{h}' for h in hosts}:
                return False
            if write and (origin is None or not secrets.compare_digest(self.headers.get('X-Theatre-CSRF',''),csrf)):
                return False
            return True
        def forbidden(self):
            self.reply(403,{'error':{'code':'FORBIDDEN','message':'仅允许本机同源授权请求'}})
        def read_body(self):
            if self.headers.get('Content-Type','').split(';')[0].strip().lower()!='application/json':
                raise RequestError(415,'JSON_REQUIRED','写操作必须使用JSON')
            raw_length=self.headers.get('Content-Length','')
            if (len(self.headers.get_all('Content-Length', [])) != 1 or not raw_length.isascii()
                    or not raw_length.isdigit() or self.headers.get('Transfer-Encoding')):
                raise RequestError(400,'INVALID_BODY','请求长度不合法')
            length=int(raw_length)
            if length>65536:
                raise RequestError(413,'BODY_TOO_LARGE','请求过大')
            try:
                body=json.loads(self.rfile.read(length))
            except (ValueError,UnicodeError):
                raise RequestError(400,'INVALID_JSON','JSON格式不正确') from None
            if not isinstance(body,dict):
                raise RequestError(400,'INVALID_BODY','请求必须是对象')
            return body
        def error_reply(self,error):
            if isinstance(error,RequestError):
                self.reply(error.status,{'error':{'code':error.code,'message':error.message}})
            else:
                code=getattr(error,'code','TECHNICAL_ERROR')
                if type(code) is not str or re.fullmatch(r'[A-Z][A-Z0-9_]{0,79}',code) is None:
                    code='TECHNICAL_ERROR'
                self.reply(409 if code!='TECHNICAL_ERROR' else 500,
                           {'error':{'code':code,'message':'操作未完成，请检查当前状态；技术诊断不作为角色见闻'}})
        def do_POST(self):
            if not self.trusted(write=True):
                self.forbidden(); return
            try:
                body=self.read_body()
                if self.path=='/api/settings':
                    settings=getattr(actors,'real_settings',None)
                    if settings is None:
                        raise RequestError(409,'REAL_MODE_REQUIRED','仅real模式可保存服务设置')
                    if coordinator.busy:
                        raise RequestError(409,'SETTINGS_BUSY','请先暂停并等待当前执行完成')
                    try:
                        result=settings.save(body)
                    except Exception as error:
                        if getattr(error,'code','').startswith('SIWC_'):raise
                        raise RequestError(400,'SETTINGS_SAVE_FAILED','未保存；请检查服务字段和本机凭据服务') from None
                    self.reply(200,result)
                elif self.path in ('/api/siwc/begin','/api/siwc/models','/api/siwc/select','/api/siwc/logout','/api/siwc/welcome'):
                    settings=getattr(actors,'real_settings',None)
                    if settings is None:raise RequestError(409,'REAL_MODE_REQUIRED','需要real模式')
                    if coordinator.busy:raise RequestError(409,'SETTINGS_BUSY','请先暂停并等待当前执行完成')
                    session=settings.siwc
                    if self.path in ('/api/siwc/begin','/api/siwc/select'):
                        if set(body)!={'account'} or (body['account'] is not None and type(body['account']) is not str):
                            raise RequestError(400,'INVALID_SETTINGS','账号选择无效')
                    elif body:raise RequestError(400,'INVALID_SETTINGS','不接受额外字段')
                    if self.path=='/api/siwc/begin':
                        result=session.begin(f'http://127.0.0.1:{self.server.server_address[1]}/auth/callback',body['account'])
                    elif self.path=='/api/siwc/select':result=session.select(body['account'])
                    elif self.path=='/api/siwc/logout':result=session.logout()
                    elif self.path=='/api/siwc/welcome':result=session.welcome()
                    else:
                        from .real_provider import RequestBudget
                        result={'models':session.models(session.binding(),RequestBudget(None,30))}
                    self.reply(200,result)
                elif self.path=='/api/control':
                    if set(body)!={'action'} or body['action'] not in ('pause','run','step','resume'):
                        raise RequestError(400,'INVALID_CONTROL','未知控制动作')
                    if body['action']=='pause': self.reply(200,coordinator.pause())
                    elif body['action']=='step': self.reply(202,coordinator.start())
                    elif body['action']=='run': self.reply(202,coordinator.start(continuous=True))
                    elif body['action']=='resume': self.reply(200,coordinator.start(continuous=True))
                    else: raise RequestError(409,'NOT_READY','连续运行尚未就绪')
                elif self.path=='/api/card':
                    changes=body.get('changes')
                    if (set(body)!={'person_id','changes'} or not isinstance(body['person_id'],str)
                            or not isinstance(changes,dict) or not changes
                            or not set(changes)<={'values','interests','risk','mission'}
                            or any(not isinstance(v,str) or not v.strip() or len(v)>2000 for v in changes.values())):
                        raise RequestError(400,'INVALID_CARD','仅可编辑人格、利益、风险偏好和长期信念')
                    self.reply(200,coordinator.edit_card(body['person_id'],changes))
                else: raise RequestError(404,'NOT_FOUND','接口不存在')
            except Exception as error:
                self.error_reply(error)
        def do_GET(self):
            # Native OAuth navigation normally has no Origin. It is NOT a normal
            # CSRF-exempt settings route: exact loopback Host/path + one-shot
            # state/PKCE/nonce/issued-client verification are its own boundary.
            if self.path.split('?',1)[0]=='/auth/callback':
                port=self.server.server_address[1]
                if (len(self.headers.get_all('Host',[]))!=1 or self.headers.get('Host')!=f'127.0.0.1:{port}'
                    or len(self.path)>16400 or '#' in self.path):
                    self.forbidden();return
                try:
                    settings=getattr(actors,'real_settings',None)
                    if settings is None:raise RequestError(409,'REAL_MODE_REQUIRED','需要real模式')
                    result=settings.siwc.callback(urlsplit(self.path).query,f'http://127.0.0.1:{port}/auth/callback')
                    if not result.get('durable_ack',False):
                        self.send_payload(202,'账号事务待确认；不能称为取消完成。请返回设置读取恢复结果，尚未验证模型调用或费用。'.encode('utf-8'),'text/plain')
                    else:self.send_payload(200,'授权已保存；请关闭本页并返回设置，尚未验证模型调用。'.encode('utf-8'),'text/plain')
                except Exception:
                    ack=settings.siwc.transaction_status() if settings is not None else {}
                    if ack.get('account_mutation_pending'):
                        self.send_payload(202,'账号事务待确认；请返回设置读取恢复结果，不代表取消完成或零费用。'.encode('utf-8'),'text/plain')
                    else:self.send_payload(400,'授权未完成；请返回设置重新发起。'.encode('utf-8'),'text/plain')
                return
            if not self.trusted():
                self.forbidden(); return
            try:
                parsed=urlsplit(self.path)
                query=parse_qs(parsed.query,keep_blank_values=True)
                route=parsed.path
                if route=='/api/settings' and not query:
                    settings=getattr(actors,'real_settings',None)
                    self.reply(200,settings.status() if settings is not None else {'state':'not_real_mode','credential_status':'unknown','connection_verified':False})
                elif route=='/api/meta' and not query:
                    dm_enabled = getattr(actors, 'adjudicator', None) is not None
                    real_mode = getattr(actors, 'real_gateway', None) is not None
                    self.reply(200,{'mode':'real' if real_mode else 'scripted-pi','csrf':csrf,
                        'dm_mode':'independent-real-dm' if real_mode else 'scripted-dm' if dm_enabled else 'legacy-scripted',
                        'capabilities':['map-topology','step','run','pause','readonly-replay','forward-card-edit']
                            + (['causal-trace','history-person'] if dm_enabled else []),
                        'limitations':(['真实服务须独立配置；配置状态不证明连接或计费验收','角色自然语言意图，DM效果仍受已实现世界规则限制','拓扑示意图，不是测绘地图'] if real_mode else ['脚本角色与有限模板DM；无真实模型或付费调用','场景驱动的脚本原型，军事/继任/真实开放DM尚未完成','拓扑示意图，不是测绘地图'])})
                elif route=='/api/state':
                    if set(query)-{'seq'} or ('seq' in query and (len(query['seq'])!=1 or not query['seq'][0].isascii() or not query['seq'][0].isdigit() or int(query['seq'][0])<1)):
                        raise RequestError(400,'INVALID_SEQUENCE','回放序号必须为正整数')
                    seq=int(query['seq'][0]) if 'seq' in query else None
                    self.reply(200,coordinator.snapshot(seq))
                elif route=='/api/person':
                    if set(query)-{'id','seq'} or 'id' not in query or len(query['id'])!=1 or not query['id'][0]:
                        raise RequestError(400,'INVALID_PERSON','需要人物标识')
                    if 'seq' in query:
                        if (len(query['seq'])!=1 or not query['seq'][0].isascii()
                                or not query['seq'][0].isdigit() or int(query['seq'][0])<1):
                            raise RequestError(400,'INVALID_SEQUENCE','历史人物版本必须为正整数')
                        self.reply(200,world.observe(query['id'][0],seq=int(query['seq'][0])))
                    else:
                        self.reply(200,world.observe(query['id'][0]))
                elif route in ('/','/index.html','/app.js','/app.css','/settings.html','/settings.js') and not query:
                    name='index.html' if route=='/' else route[1:]
                    mime={'index.html':'text/html','app.js':'text/javascript','app.css':'text/css','settings.html':'text/html','settings.js':'text/javascript'}[name]
                    self.send_payload(200,(Path(__file__).with_name('web')/name).read_bytes(),mime)
                else: raise RequestError(404,'NOT_FOUND','接口不存在')
            except Exception as error:
                self.error_reply(error)
    server=TheatreServer(('127.0.0.1',port),Handler)
    server.coordinator=coordinator
    return server


def create_default_actors(world, db_path, provider='scripted-pi'):
    if provider == 'real':
        from .product_cli import _create_product_actors
        return _create_product_actors(world, db_path, provider)
    from app.adjudication import Adjudicator
    from app.scripted_dm import DefaultActors, ScriptedDM
    from app.actors import ActorHost
    host = ActorHost(world, db_path, adjudicator=Adjudicator(world), dm_provider=ScriptedDM(world))
    host._choose = DefaultActors._choose
    return host


def main(argv=None):
    import argparse
    parser=argparse.ArgumentParser(description='文明剧团本机观察台（仅脚本模型替身）')
    parser.add_argument('--provider',choices=('scripted-pi','real'),default='scripted-pi')
    parser.add_argument('--port',type=int,default=17431)
    parser.add_argument('--data-dir',type=Path,default=Path(__file__).with_name('runtime'))
    parser.add_argument('--scenario',type=Path,default=None,help='新世界场景包；已有世界换包须使用新的data-dir')
    args=parser.parse_args(argv)
    if not 0<=args.port<=65535:
        parser.error('port must be between 0 and 65535')
    from app.world import WorldStore
    from app.actors import ActorHost
    args.data_dir.mkdir(parents=True,exist_ok=True)
    world=WorldStore(args.data_dir/'world.sqlite',scenario_path=args.scenario)
    current=world.initialize()
    coordinator=Coordinator(world,None)
    try:
        with coordinator._preparation_scope(coordinator._owner_attempt):
            coordinator.acquire_owner()
            if coordinator.backend_kind == 'protocol_double':
                # Retain the explicit nonpersistent protocol for CLI forwarding
                # doubles; persistent stores can never enter this branch.
                actors=create_default_actors(world,args.data_dir/'actors.sqlite',**({'provider':'real'} if args.provider=='real' else {}))
            else:
                # Source discovery is deferred and outside the root scope:
                # construction cannot create/migrate private storage before proof.
                with coordinator._preparation_scope(coordinator._owner_attempt):
                    observer=ActorHost(world,args.data_dir/'actors.sqlite')
                    coordinator.actors=observer
                    with coordinator._owner_scope(coordinator._owner_attempt):
                        coordinator._prepare_scheduler(coordinator._owner_attempt)
                        if current['status']=='running':
                            world.control('pause')
                        actors=create_default_actors(world,args.data_dir/'actors.sqlite',**({'provider':'real'} if args.provider=='real' else {}))
                    observer.close()
        coordinator.actors=actors
        server=make_server(world,actors,args.port,coordinator=coordinator)
    except BaseException:
        coordinator.close()
        raise
    print('THEATRE_READY http://127.0.0.1:'+str(server.server_address[1]),flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


from .source_attestation import register as _register_source
_SOURCE_GENERATION = _register_source(globals())

if __name__=='__main__':
    main()

