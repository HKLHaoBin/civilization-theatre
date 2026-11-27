"""Forward product entry. No native diagnostic installer or sys.monitoring.

Existing WorldStore, Host, Coordinator, budgets and source authority are intact.
component-scripted exercises local business routing, NOT a model or SDK test.
scripted-pi uses the existing real Pi SDK with its offline scripted provider.
Neither mode claims the frozen real-model/90-day release acceptance.
"""
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import time

ROOT = Path(__file__).resolve().parent.parent
SCENARIOS = ROOT / 'app/data/scenarios'
MANIFEST = ROOT / 'generation-manifest.json'
PROVIDERS = ('component-scripted', 'scripted-pi', 'real')


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False)


def verify_generation():
    manifest = json.loads(MANIFEST.read_text(encoding='utf-8'))
    if manifest['root'] != str(ROOT) or manifest['schema'] != 'parent950-product-v1':
        raise ValueError('PRODUCT_GENERATION_MISMATCH')
    for relative, pin in manifest['files'].items():
        part = Path(relative)
        if part.is_absolute() or '..' in part.parts or part.parts[0] != 'app':
            raise ValueError('PRODUCT_MANIFEST_PATH')
        file = ROOT / part
        if file.is_symlink() or file.resolve().is_relative_to(ROOT) is False:
            raise ValueError('PRODUCT_SOURCE_LINK')
        raw = file.read_bytes()
        if len(raw) != pin['bytes'] or hashlib.sha256(raw).hexdigest() != pin['sha256']:
            raise ValueError('PRODUCT_SOURCE_DRIFT')
    return hashlib.sha256(MANIFEST.read_bytes()).hexdigest()


def source_identity(*actual_objects):
    from .world import WorldStore
    from .actors import ActorHost
    from .server import Coordinator
    from .actor_execution import PrivateStore
    from .coordinator_execution import CoordinatorAttempt
    from .coordinator_maintenance import recover
    from .execution import ExecutionPermit
    from .source_attestation import attest
    return attest(ROOT, WorldStore, ActorHost, Coordinator, PrivateStore,
                  CoordinatorAttempt, recover, ExecutionPermit, *actual_objects)


def initialize_owned(data_dir, scenario):
    pin = verify_generation()
    source_identity()
    directory = Path(data_dir).absolute()
    # Never adopt an existing directory or an old database, even if empty.
    directory.mkdir(parents=True, exist_ok=False)
    if directory.is_symlink():
        raise ValueError('OWNED_DIRECTORY_LINK')
    directory = directory.resolve(strict=True)
    scenario_bytes = Path(scenario).read_bytes()
    owned_scenario = directory / 'scenario.json'
    owned_scenario.write_bytes(scenario_bytes)
    from .world import WorldStore
    world = WorldStore(directory / 'world.sqlite', scenario_path=owned_scenario)
    state = world.initialize()
    readback = world.snapshot()
    if state != readback:
        raise RuntimeError('INITIALIZATION_READBACK_MISMATCH')
    marker = dict(schema='parent950-owned-world-v1', world_id=state['world_id'],
                  generation_sha256=pin, data_dir=str(directory),
                  scenario_sha256=hashlib.sha256(scenario_bytes).hexdigest())
    with (directory / 'owner.json').open('x', encoding='utf-8', newline='\n') as stream:
        stream.write(_json(marker) + '\n')
    return state


def _owned(data_dir):
    pin = verify_generation()
    directory = Path(data_dir).absolute()
    if directory.is_symlink() or not directory.is_dir():
        raise ValueError('OWNED_WORLD_REQUIRED')
    directory = directory.resolve(strict=True)
    marker_file = directory / 'owner.json'
    if not marker_file.is_file() or marker_file.is_symlink():
        raise ValueError('OWNED_WORLD_REQUIRED')
    marker = json.loads(marker_file.read_text(encoding='utf-8'))
    if (marker.get('schema') != 'parent950-owned-world-v1'
            or marker.get('generation_sha256') != pin
            or marker.get('data_dir') != str(directory)):
        raise ValueError('OWNED_WORLD_MISMATCH')
    for name in ('world.sqlite', 'scenario.json'):
        file = directory / name
        if not file.is_file() or file.is_symlink():
            raise ValueError('OWNED_WORLD_FILE')
    actor_file = directory / 'actors.sqlite'
    if actor_file.is_symlink():
        raise ValueError('OWNED_WORLD_FILE')
    scenario = directory / 'scenario.json'
    if hashlib.sha256(scenario.read_bytes()).hexdigest() != marker['scenario_sha256']:
        raise ValueError('OWNED_SCENARIO_DRIFT')
    from .world import WorldStore
    world = WorldStore(directory / 'world.sqlite', scenario_path=scenario)
    if world.snapshot()['world_id'] != marker['world_id']:
        raise ValueError('OWNED_WORLD_ID_MISMATCH')
    source_identity()
    return directory, world


def read_owned(data_dir, seq=None):
    # No initialize, root lease, Host, model, scheduler, or mutation on read path.
    directory, world = _owned(data_dir)
    return world.snapshot(seq)


def _component_request(payload):
    return dict(messages=payload['messages'] + [dict(role='user', content=payload['prompt'])],
                calls=[dict(call_id='component-' + payload['intent_id'])],
                sdk_events=0, sdk_requests=0, provider='component-scripted',
                pid=os.getpid(), module_root=str(ROOT))


def _create_product_actors(world, db_path, provider='scripted-pi'):
    if provider == 'real':
        from .actors import ActorHost
        from .adjudication import Adjudicator
        from .real_settings import SettingsStore
        from .real_provider import RealGateway, ProviderFault
        settings = SettingsStore(Path(db_path).with_name('services.json'))
        gateway = RealGateway(settings)
        def dm(payload):
            # Host._submit captures original request version and Attempt before
            # invoking the real target. No alternate unbound DM entry.
            raise ProviderFault('REAL_DM_SCOPE_REQUIRED')
        host = ActorHost(world, db_path, adjudicator=Adjudicator(world), dm_provider=dm)
        host.real_gateway, host.real_settings = gateway, settings
        return host
    # A genuine registered ActorHost; chooser strategy does not change its type.
    from .actors import ActorHost
    from .adjudication import Adjudicator
    from .scripted_dm import DefaultActors, ScriptedDM
    host = ActorHost(world, db_path, adjudicator=Adjudicator(world),
                     dm_provider=ScriptedDM(world))
    host._choose = DefaultActors._choose
    return host


class ProductSession:
    def __init__(self, data_dir, provider):
        if provider not in PROVIDERS:
            raise ValueError('EXPLICIT_SUPPORTED_PROVIDER_REQUIRED')
        self.directory, self.world = _owned(data_dir)
        self.provider = provider
        self.host = None
        self.coordinator = None
        from .server import Coordinator
        from .actors import ActorHost
        coordinator = Coordinator(self.world, None)
        self.coordinator = coordinator
        try:
            with coordinator._preparation_scope(coordinator._owner_attempt):
                coordinator.acquire_owner()
                observer = ActorHost(self.world, self.directory / 'actors.sqlite')
                coordinator.actors = observer
                with coordinator._owner_scope(coordinator._owner_attempt):
                    coordinator._prepare_scheduler(coordinator._owner_attempt)
                    host = _create_product_actors(self.world, self.directory / 'actors.sqlite', provider)
                    self.host = host
                # Track both objects if observer.close fails.
                observer.close()
                if observer.cleanup_pending:
                    raise RuntimeError('OBSERVER_CLEANUP_PENDING')
                coordinator.actors = host
                if provider == 'component-scripted':
                    host._request = _component_request
        except BaseException as original:
            cleanup_failure = None
            try:
                coordinator.close()
            except BaseException as cleanup:
                cleanup_failure = cleanup
            if self.host is not None and coordinator.actors is not self.host:
                try:
                    self.host.close()
                except BaseException as cleanup:
                    cleanup_failure = cleanup_failure or cleanup
            if cleanup_failure is not None:
                original.add_note('Cleanup also failed: ' + type(cleanup_failure).__name__)
                raise original from cleanup_failure
            raise

    def step(self, timeout):
        if not 0 < timeout <= 120:
            raise ValueError('FINITE_STEP_TIMEOUT_REQUIRED')
        self.coordinator.start(continuous=False)
        worker = self.coordinator.worker
        worker.join(timeout)
        if worker.is_alive():
            self.coordinator.close()
            raise TimeoutError('PRODUCT_WINDOW_TIMEOUT')
        if self.coordinator.error is not None:
            failure = RuntimeError(self.coordinator.error)
            try:
                from .failure_projection import note
                projection = getattr(self.coordinator, 'failure_projection', None)
                if projection is not None:
                    failure.add_note(note(projection))
            except BaseException:
                pass  # Diagnostics must not replace the original fixed code.
            raise failure
        return self.world.snapshot()

    def ledger(self):
        # Component oracle: finite public tables, ordinary RO connection.
        import sqlite3
        from contextlib import closing
        names = ('coordinator_scheduler_registration', 'coordinator_windows',
                 'coordinator_activations')
        with closing(sqlite3.connect((self.directory / 'world.sqlite').as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            return {name: [dict(row) for row in db.execute('SELECT * FROM ' + name)]
                    for name in names}

    def close(self):
        if self.coordinator is not None:
            self.coordinator.close()
            if self.coordinator.cleanup_pending or getattr(self.host, 'cleanup_pending', False):
                raise RuntimeError('PRODUCT_CLEANUP_PENDING')

    def __enter__(self):
        return self

    def __exit__(self, kind, value, trace):
        if value is None:
            self.close()
        else:
            try:
                self.close()
            except BaseException as cleanup:
                value.add_note('Cleanup also failed: ' + type(cleanup).__name__)
                raise value.with_traceback(trace) from cleanup


def open_session(data_dir, provider):
    return ProductSession(data_dir, provider)


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='文明剧团前向产品入口；real为显式独立服务模式，配置前不调用')
    parser.add_argument('--data-dir', required=True, type=Path)
    sub = parser.add_subparsers(dest='command', required=True)
    init = sub.add_parser('init')
    init.add_argument('--scenario', type=Path, default=SCENARIOS / 'iron-4.json')
    read = sub.add_parser('read')
    read.add_argument('--seq', type=int)
    step = sub.add_parser('step')
    step.add_argument('--provider', required=True, choices=PROVIDERS)
    step.add_argument('--timeout', type=float, default=60)
    serve = sub.add_parser('serve')
    serve.add_argument('--provider', required=True, choices=('scripted-pi','real'))
    serve.add_argument('--port', type=int, default=17431)
    args = parser.parse_args(argv)
    if args.command == 'init':
        result = initialize_owned(args.data_dir, args.scenario)
    elif args.command == 'read':
        if args.seq is not None and args.seq < 1:
            parser.error('seq must be positive')
        result = read_owned(args.data_dir, args.seq)
    else:
        if args.command == 'step' and not 0 < args.timeout <= 120:
            parser.error('timeout must be in (0, 120]')
        if args.command == 'serve' and not 0 <= args.port <= 65535:
            parser.error('port must be in [0, 65535]')
        with open_session(args.data_dir, args.provider) as session:
            if args.command == 'step':
                result = session.step(args.timeout)
            else:
                from .server import make_server
                server = make_server(session.world, session.host, args.port,
                                     coordinator=session.coordinator)
                print('THEATRE_READY http://127.0.0.1:' + str(server.server_address[1]), flush=True)
                try:
                    server.serve_forever()
                except KeyboardInterrupt:
                    pass
                finally:
                    server.server_close()
                result = session.world.snapshot()
    print(_json(dict(world_id=result['world_id'], revision=result['revision'],
                     time=result['time'], status=result['status'],
                     product_runtime_executed=True, diagnostic_installer_requested=False,
                     provider_mode=getattr(args, 'provider', None), real_call_verified=False,
                     real_model=False)), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
