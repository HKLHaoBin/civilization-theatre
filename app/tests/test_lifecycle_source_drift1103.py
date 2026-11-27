"""1103 W3 ordinary-source DISK drift: definitions only, never run by author.

One discovered method, no imported/inherited TestCase alias. Two fresh real worlds
share ONE whole120 including the current finite SourceProfile-v2 copy/imports.
Only transport supplies model output; the negative changes owned gateway bytes
at the original final Work gate, never reloads code or changes source9 FILES.
"""
import ast
import copy
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import sys
import threading
import time
import unittest
import uuid
from contextlib import closing


# Explicit forward authorization: 1108 SourceProfile-v2, NOT a repin of BASE156.
_STOPPED_FIRST_CLOSE_1163 = None
PROFILE_VERSION = '1108-current-loaded-root-finite-manifest-v2'
SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
SOURCE9 = frozenset(('actors.py', 'actor_execution.py', 'coordinator_scheduler.py',
    'server.py', 'world.py', 'execution.py', 'coordinator_execution.py',
    'coordinator_maintenance.py', 'source_attestation.py'))


def digest(raw):
    return dict(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def error_record(error):
    return dict(type=type(error).__name__, code=getattr(error, 'code', None))


def typed(value):
    return dict(type=type(value).__name__, value=value.hex() if type(value) is bytes else value)


def database_image(directory):
    """Full typed rows AND metadata of only this fresh owned dual store, RO.

    Not a fake Row, adapter, selective count, old world or schema mutation. Lease
    and clock tables stay present in this image and are accounted separately.
    """
    result = {}
    for filename in ('world.sqlite', 'actors.sqlite'):
        path = (directory / filename).resolve(strict=True)
        with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('BEGIN')
            schema = db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').fetchall()
            tables = {}
            for kind, name, owner, sql in schema:
                if kind != 'table':
                    continue
                quote = '"' + name.replace('"', '""') + '"'
                cursor = db.execute('SELECT * FROM ' + quote)
                rows = [[typed(value) for value in row] for row in cursor.fetchall()]
                rows.sort(key=lambda row: json.dumps(row, sort_keys=True, ensure_ascii=False))
                tables[name] = dict(columns=[d[0] for d in cursor.description], rows=rows,
                    table_info=[list(row) for row in db.execute('PRAGMA table_info(' + quote + ')')],
                    foreign_keys=[list(row) for row in db.execute('PRAGMA foreign_key_list(' + quote + ')')])
            result[filename] = dict(schema=[list(row) for row in schema], tables=tables)
    return result


def business_image(image):
    result = copy.deepcopy(image)
    # Exact original mutable control planes; all other tables and ALL schema
    # and column metadata remain compared, including registration and bindings.
    for name in ('actor_execution_leases', 'coordinator_execution_leases', 'execution_clock',
                 'actor_coordinator_delegations', 'actor_activation_delegations'):
        if name in result['world.sqlite']['tables']:
            result['world.sqlite']['tables'][name]['rows'] = []
    return result


def validate_app_module(test, name, module, root, files, old=None):
    """Real file modules or manifest-derived initializer-free namespaces only."""
    import types
    from importlib.machinery import NamespaceLoader, SourceFileLoader
    root = root.resolve(strict=True)
    test.assertIs(sys.modules[name], module)
    test.assertEqual(module.__name__, name)
    spec = module.__spec__
    test.assertIsNotNone(spec)
    test.assertEqual(spec.name, name)
    test.assertIs(module.__loader__, spec.loader)
    test.assertIsNot(module, old)
    if old is not None:
        test.assertIsNot(spec, old.__spec__)
        test.assertIsNot(spec.loader, old.__spec__.loader)
    part = Path(*name.split('.'))
    initializer = (part / '__init__.py').as_posix()
    ordinary = part.as_posix() + '.py'
    if getattr(module, '__file__', None) is None:
        # Derive only from finite registered Python descendants. Absence on disk
        # is checked too: an unregistered initializer cannot be called namespace.
        descendants = sorted(key for key in files if key.startswith(part.as_posix() + '/') and key.endswith('.py'))
        test.assertTrue(descendants)
        test.assertNotIn(initializer, files)
        test.assertNotIn(ordinary, files)
        directory = root / part
        test.assertTrue(directory.is_dir())
        test.assertFalse(directory.is_symlink())
        test.assertEqual(directory.resolve(strict=True), directory)
        test.assertFalse((root / initializer).exists())
        test.assertFalse((root / ordinary).exists())
        test.assertIs(type(spec.loader), NamespaceLoader)
        test.assertIsNone(spec.origin)
        test.assertFalse(spec.has_location)
        test.assertIsNone(getattr(module, '__cached__', None))
        test.assertEqual(spec.parent, name)
        test.assertEqual([Path(p).resolve(strict=True) for p in module.__path__], [directory])
        test.assertEqual([Path(p).resolve(strict=True) for p in spec.submodule_search_locations], [directory])
        test.assertIs(module.__path__, spec.submodule_search_locations)
        if old is not None:
            test.assertIsNot(module.__path__, getattr(old, '__path__', None))
        return dict(kind='namespace', directory=str(directory), absent_initializer=initializer,
            descendant_manifest_members=descendants, exact_paths=[str(directory)], new_identity=True)
    relative = initializer if initializer in files else ordinary
    test.assertIn(relative, files, 'unknown app module cannot be skipped')
    path = (root / relative).resolve(strict=True)
    test.assertEqual(Path(module.__file__).resolve(strict=True), path)
    test.assertFalse(path.is_symlink())
    test.assertIs(type(spec.loader), SourceFileLoader)
    test.assertEqual(Path(spec.origin).resolve(strict=True), path)
    test.assertTrue(spec.has_location)
    test.assertEqual(spec.loader.name, name)
    test.assertEqual(Path(spec.loader.path).resolve(strict=True), path)
    if relative == initializer:
        test.assertEqual([Path(p).resolve(strict=True) for p in module.__path__], [path.parent])
        test.assertEqual([Path(p).resolve(strict=True) for p in spec.submodule_search_locations], [path.parent])
    else:
        test.assertIsNone(spec.submodule_search_locations)
    raw = path.read_bytes()
    test.assertEqual(digest(raw), files[relative])
    namespace = vars(module)
    compiled = compile(raw, str(module.__file__), 'exec', dont_inherit=True)
    codes = {}
    def collect(code):
        codes.setdefault(code.co_qualname, []).append(code)
        for item in code.co_consts:
            if isinstance(item, types.CodeType):
                collect(item)
    collect(compiled)
    seen = set()
    bound = []
    def bind(value):
        if isinstance(value, (staticmethod, classmethod)):
            value = value.__func__
        if isinstance(value, property):
            for accessor in (value.fget, value.fset, value.fdel):
                if accessor is not None:
                    bind(accessor)
            return
        if not isinstance(value, types.FunctionType) or id(value) in seen:
            return
        seen.add(id(value))
        wrapped = getattr(value, '__wrapped__', None)
        if value.__globals__ is namespace:
            matches = [c for c in codes.get(value.__code__.co_qualname, [])
                if c.co_firstlineno == value.__code__.co_firstlineno]
            test.assertEqual(len(matches), 1)
            test.assertEqual(value.__code__, matches[0])
            test.assertEqual(value.__code__.co_filename, str(module.__file__))
            test.assertEqual(value.__module__, name)
            bound.append(value.__qualname__)
        elif wrapped is not None:
            import contextlib
            test.assertTrue(value.__globals__ is vars(contextlib), 'contextmanager wrapper globals mismatch')
            native = [c for c in contextlib.contextmanager.__code__.co_consts if isinstance(c, types.CodeType)]
            test.assertTrue(any(value.__code__ == c for c in native))
        if wrapped is not None:
            bind(wrapped)
    # Authenticate every actual source definition, not dataclass/Enum's
    # stdlib-generated methods whose source is not this app file. No module is
    # omitted; imported aliases are authenticated in their defining app module.
    tree = ast.parse(raw, filename=str(module.__file__))
    required = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            test.assertIn(node.name, namespace)
            value = namespace[node.name]
            before = len(bound)
            bind(value)
            test.assertGreater(len(bound), before)
            required.append(node.name)
        elif isinstance(node, ast.ClassDef):
            value = namespace[node.name]
            test.assertIsInstance(value, type)
            test.assertEqual(value.__module__, name)
            for member in node.body:
                if isinstance(member, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    test.assertIn(member.name, vars(value))
                    bind(vars(value)[member.name])
                    required.append(node.name + '.' + member.name)
    test.assertTrue(all(any(q == r or q.startswith(r + '.') for q in bound) for r in required))
    return dict(kind='source', path=str(path), relative=relative, pin=files[relative],
        functions=sorted(bound), source_definition_coverage=sorted(set(required)),
        generated_stdlib_methods_not_app_source=True, new_identity=True)


class LifecycleSourceDriftTests(unittest.TestCase):
    def test_owned_loaded_gateway_disk_drift_rejects_original_final_work_without_ghosts(self):
        deadline = time.monotonic() + 120  # BEFORE copy/import/setUp, never renewed.
        owned = SCRATCH / ('parent1103-owned-source-' + uuid.uuid4().hex)
        owned.mkdir(parents=True, exist_ok=False)
        source = owned / 'source'
        source.mkdir()
        report = dict(schema='parent1103-future-runtime-evidence-v1', status='INCOMPLETE',
            whole_deadline=deadline, profile_version=PROFILE_VERSION, owned_source=str(source),
            phases=[], events=[], failures=[], source_pass=False)
        fixtures = []
        saved_modules = None
        saved_path = list(sys.path)
        saved_bytecode = sys.dont_write_bytecode
        prior_thread_trace, prior_sys_trace = threading.gettrace(), sys.gettrace()
        target = None
        original_bytes = None
        primary = None
        helper = None
        injected = False
        trace_installed = False
        active = {}  # recovery state exists even if source admission fails
        self.assertIsNone(prior_thread_trace, 'must not displace foreign thread observer')
        self.assertIsNone(prior_sys_trace, 'must not displace foreign main observer')

        def budget():
            self.assertLess(time.monotonic(), deadline, 'one cumulative whole120; no retry renewal')

        def resources(fixture):
            s = fixture.session
            if s is None:
                return dict(session=False, live=[])
            c, h = s.coordinator, s.host
            root = c._owner_attempt
            threads = [c.worker, None if root is None else root.thread, *h._pending_threads]
            for attempt in fixture.attempts:
                threads.extend([attempt.thread, *attempt.calls])
            actual = []
            for thread in threads:
                if thread is not None and all(thread is not old for old in actual):
                    actual.append(thread)
            for thread in actual:
                thread.join(0)
            return dict(session=True, host_pending=h.cleanup_pending, coordinator_pending=c.cleanup_pending,
                lifecycle_active=h._lifecycle_active is not None, lease_cleanups=len(h._lease_cleanups),
                process=h._process is not None, root_failure=None if root is None or root.failure is None else error_record(root.failure),
                root_stop=None if root is None else root.stop.is_set(),
                threads=[dict(name=t.name, ident=t.ident, alive=t.is_alive()) for t in actual],
                live=[t.name for t in actual if t.is_alive()])

        def close_first(fixture, phase):
            """Preserve FIRST native close, never overwrite with a later success."""
            if getattr(fixture, '_first1103_close', None) is not None:
                return fixture._first1103_close
            record = dict(phase=phase, at=time.monotonic(), before=resources(fixture))
            start = len(report['events'])
            try:
                if fixture.session is not None:
                    record['return'] = typed(fixture.session.close())
            except BaseException as error:
                record['error'] = error_record(error)
                fixture._first1103_close_error = error  # original stays private, no str/args
            record['events'] = copy.deepcopy(report['events'][start:])
            fixture._first1103_close = record
            report['phases'].append(record)
            try:
                record['after'] = resources(fixture)
            except BaseException as error:
                record['resource_error'] = error_record(error)
                raise
            return record

        def new_fixture():
            budget()
            fixture = helper.LifecycleAuthorityProductTests(methodName='runTest')
            fixtures.append(fixture)
            fixture.setUp()
            fixture.deadline = min(fixture.deadline, deadline)
            self.assertLessEqual(fixture.deadline, deadline)
            return fixture

        def assert_closed(record):
            self.assertNotIn('error', record, 'original close error is not washed by source recovery')
            self.assertNotIn('resource_error', record, 'first resource observation remains unknown')
            after = record['after']
            self.assertEqual(after['live'], [])
            self.assertFalse(after['host_pending'])
            self.assertFalse(after['coordinator_pending'])
            self.assertFalse(after['lifecycle_active'])
            self.assertEqual(after['lease_cleanups'], 0)
            self.assertFalse(after['process'])

        try:
            # Root is derived from THIS actual loaded annex, never guessed future root.
            actual_module = sys.modules[self.__class__.__module__]
            actual_function = self.__class__.test_owned_loaded_gateway_disk_drift_rejects_original_final_work_without_ghosts
            self.assertIs(vars(actual_module)[self.__class__.__name__], self.__class__)
            self.assertTrue(actual_function.__globals__ is vars(actual_module), 'actual annex globals mismatch')
            actual_path = Path(actual_module.__file__).resolve(strict=True)
            profile_root = actual_path.parents[2]
            self.assertEqual(actual_path.relative_to(profile_root).as_posix(), 'app/tests/test_lifecycle_source_drift1103.py')
            manifest_raw = (profile_root / 'generation-manifest.json').read_bytes()
            manifest = json.loads(manifest_raw)
            self.assertEqual(manifest['schema'], 'parent950-product-v1')
            self.assertEqual(Path(manifest['root']).resolve(strict=True), profile_root)
            self.assertIs(type(manifest['files']), dict)
            self.assertTrue(manifest['files'])
            self.assertIn('app/tests/test_lifecycle_source_drift1103.py', manifest['files'])
            for name, module in tuple(sys.modules.items()):
                if name == 'app' or name.startswith('app.'):
                    validate_app_module(self, name, module, profile_root, manifest['files'])
            report['actual_source_profile'] = dict(version=PROFILE_VERSION, root=str(profile_root),
                manifest=digest(manifest_raw), members=len(manifest['files']),
                own_pin=digest(actual_path.read_bytes()), files=copy.deepcopy(manifest['files']),
                old_BASE156_evidence_unchanged=True)
            copied = {}
            for relative, pin in manifest['files'].items():
                budget()
                part = Path(relative)
                self.assertFalse(part.is_absolute())
                self.assertNotIn('..', part.parts)
                self.assertEqual(part.parts[0], 'app')
                src, dst = profile_root / part, source / part
                self.assertFalse(src.is_symlink())
                self.assertTrue(src.resolve(strict=True).is_relative_to(profile_root))
                raw = src.read_bytes()
                self.assertEqual(digest(raw), pin)
                dst.parent.mkdir(parents=True, exist_ok=True)
                with dst.open('xb') as stream:
                    stream.write(raw)  # ordinary independent bytes, NOT link/copytree
                self.assertFalse(dst.is_symlink())
                self.assertEqual(dst.stat().st_nlink, 1)
                self.assertFalse(dst.samefile(src))
                self.assertEqual(digest(dst.read_bytes()), pin)
                copied[relative] = pin
            self.assertEqual(copied, manifest['files'])
            self.assertEqual((profile_root / 'generation-manifest.json').read_bytes(), manifest_raw)
            # The copy has its OWN manifest identity. No claim of original SHA.
            clone_manifest = copy.deepcopy(manifest)
            clone_manifest['root'] = str(source.resolve())
            clone_raw = (json.dumps(clone_manifest, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
            (source / 'generation-manifest.json').write_bytes(clone_raw)
            self.assertEqual((source / 'generation-manifest.json').read_bytes(), clone_raw)
            report['copy'] = dict(members=len(copied), files=copied, parent_manifest=digest(manifest_raw),
                owned_manifest=digest(clone_raw), changed_metadata=['root'], same_parent_manifest_sha=False)

            # Save/switch ONLY app modules. No foreign/stdlib/Hermes eviction or
            # registry reset. This method has not created a canonical app object.
            saved_modules = {k: v for k, v in tuple(sys.modules.items()) if k == 'app' or k.startswith('app.')}
            for name in saved_modules:
                del sys.modules[name]
            sys.path.insert(0, str(source))
            sys.dont_write_bytecode = True
            helper = importlib.import_module('app.tests.test_lifecycle_authority1100')
            product = helper.product_cli
            scheduler = helper.scheduler
            execution = importlib.import_module('app.actor_execution')
            coordinator_execution = importlib.import_module('app.coordinator_execution')
            attestation = importlib.import_module('app.source_attestation')
            self.assertEqual(product.ROOT.resolve(), source.resolve())
            self.assertEqual(attestation.FILES, SOURCE9)
            self.assertEqual(product.verify_generation(), digest(clone_raw)['sha256'])
            # Pin every actual app module path and source against the copied
            # manifest. The retained old dictionaries never supply new globals.
            loaded = {}
            for name, module in tuple(sys.modules.items()):
                if name == 'app' or name.startswith('app.'):
                    loaded[name] = validate_app_module(self, name, module, source, copied, saved_modules.get(name))
            report['loaded_app_bindings'] = loaded
            target = source / 'app/real_provider.py'
            original_bytes = target.read_bytes()
            original_gateway_code = helper.RealGateway._generate_owned.__code__
            original_gateway_namespace = helper.RealGateway._generate_owned.__globals__

            binder = helper.LifecycleAuthorityProductTests(methodName='runTest')
            resource_specs = ((helper.WorldStore, 'release_actor_execution', 'actor_release_CAS'),
                (helper.WorldStore, 'release_coordinator_execution', 'root_release_CAS'),
                (execution.Attempt, 'finish', 'actor_heartbeat_finish'),
                (execution.Attempt, 'cancel', 'actor_cancel'),
                (coordinator_execution.CoordinatorAttempt, 'finish', 'root_heartbeat_finish'),
                (helper.RealGateway, 'close', 'original_gateway_close'),
                (helper.ActorHost, '_cancel_lifecycle', 'original_root_cancel_gateway'),
                (helper.ActorHost, 'close', 'original_host_close'),
                (helper.ActorHost, 'act', 'original_host_exit'),
                (helper.Coordinator, 'close', 'original_coordinator_close'),
                (helper.SyntheticTransport, 'close', 'synthetic_transport_close'))
            observed = {binding['code']: (binding, label) for cls, method, label in resource_specs
                for binding in [binder.source_binding(cls, method)]}
            observed_calls = {}
            active = dict(fixture=None, mutate_code=None, mutate_line=None, leaf_code=None, leaf_line=None,
                          armed=False, work=None, leaf_errors=[])

            def append_event(item):
                self.assertLess(len(report['events']), 1024, 'finite owned fixture ledger overflow')
                item['order'] = len(report['events'])
                item['phase'] = report.get('cleanup_phase', 'real_work')
                report['events'].append(item)

            def resource_event(frame, event, arg):
                selected = observed.get(frame.f_code)
                if selected is None:
                    return False
                binding, label = selected
                self.assertIs(frame.f_code, binding['code'])
                self.assertTrue(frame.f_globals is binding['namespace'], 'owned return globals mismatch')
                if event == 'call':
                    obj = frame.f_locals.get('self')
                    owner = None
                    for index, fixture in enumerate(fixtures):
                        if fixture.session is None:
                            continue
                        s = fixture.session
                        allowed = (s.world, s.host, s.coordinator, s.host.real_gateway,
                            s.coordinator._owner_attempt, fixture.transport, *fixture.attempts)
                        if any(obj is value for value in allowed):
                            owner = (index, fixture)
                            break
                    if owner is None:
                        return False
                    index, fixture = owner
                    permit = frame.f_locals.get('permit') if label in ('actor_release_CAS', 'root_release_CAS') else getattr(obj, 'permit', None)
                    identity = None
                    if permit is not None:
                        if permit is fixture.original_root.permit:
                            identity = 'root'
                        else:
                            matches = [i for i,a in enumerate(fixture.attempts) if a.permit is permit]
                            self.assertEqual(len(matches), 1)
                            identity = matches[0]
                    self.assertLess(len(observed_calls), 64)
                    observed_calls[frame] = dict(operation=label, fixture=index, permit=identity)
                    append_event(dict(observed_calls[frame], event='call'))
                elif frame in observed_calls:
                    item = dict(observed_calls[frame], event=event)
                    if event in ('exception', 'return'):
                        expected = None
                        if binding['path'] == target and active.get('drifted') and not active.get('source_recovered'):
                            expected = original_bytes + b'\n# parent1103 owned-only ordinary disk bytes drift; NO reload\n'
                        binder.check_source_binding(binding, expected_raw=expected)
                    if event == 'exception':
                        item['error'] = error_record(arg[1])
                        append_event(item)
                    elif event == 'return':
                        item['value'] = typed(arg) if type(arg) is bool or arg is None else dict(type=type(arg).__name__, value='NOT_SERIALIZED')
                        if label == 'original_host_exit':
                            fixture = fixtures[item['fixture']]
                            attempt = frame.f_locals.get('attempt')
                            matches = [i for i,a in enumerate(fixture.attempts) if a is attempt]
                            self.assertEqual(len(matches), 1)
                            item['permit'] = matches[0]
                            for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                                value = frame.f_locals.get(key)
                                item[key] = value if type(value) is bool else 'UNKNOWN'
                        append_event(item)
                        del observed_calls[frame]
                return True

            def assert_owned_returns(fixture):
                index = next(i for i,f in enumerate(fixtures) if f is fixture)
                events = [e for e in report['events'] if e['fixture'] == index]
                for number, attempt in enumerate(fixture.attempts):
                    exits = [e for e in events if e['operation'] == 'original_host_exit' and e['event'] == 'return' and e['permit'] == number]
                    self.assertEqual(len(exits), 1)
                    for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                        self.assertIs(exits[0][key], True)
                    chains = []
                    for label in ('actor_heartbeat_finish', 'actor_release_CAS'):
                        rows = [e for e in events if e['operation'] == label and e['permit'] == number]
                        self.assertEqual([e['event'] for e in rows], ['call', 'return'])
                        self.assertEqual(rows[-1]['value'], typed(True))
                        chains.append(rows)
                    self.assertLess(chains[0][-1]['order'], chains[1][0]['order'])
                    self.assertLess(chains[1][-1]['order'], exits[0]['order'])
                roots = [e for e in events if e['operation'] == 'root_release_CAS']
                self.assertEqual([e['event'] for e in roots], ['call', 'return'])
                self.assertEqual(roots[-1]['value'], typed(True))
                self.assertEqual(roots[-1]['permit'], 'root')
                finishes = [e for e in events if e['operation'] == 'root_heartbeat_finish']
                self.assertEqual([e['event'] for e in finishes], ['call', 'return'])
                self.assertEqual(finishes[-1]['value'], typed(True))
                self.assertLess(finishes[-1]['order'], roots[0]['order'])
                self.assertFalse(any(item['fixture'] == index for item in observed_calls.values()))

            def trace(frame, event, arg):
                # Whitelisted locals only after exact code/globals+owned self;
                # never foreign values, token, exception str/args/back-chain.
                code = frame.f_code
                owned_resource = resource_event(frame, event, arg)
                if code is active['mutate_code']:
                    self.assertTrue(frame.f_globals is active['mutate_binding']['namespace'], 'final Work globals mismatch')
                if code is active['leaf_code']:
                    self.assertTrue(frame.f_globals is active['leaf_binding']['namespace'], 'ordinary leaf globals mismatch')
                if code is active['mutate_code'] and event == 'line' and frame.f_lineno == active['mutate_line'] and active['armed']:
                    active['armed'] = False
                    f = active['fixture']
                    try:
                        binder.check_source_binding(active['mutate_binding'])
                        binder.check_source_binding(active['leaf_binding'])
                        budget()
                        work = scheduler.lifecycle_work(f.session.world, f.root_request['job_id'])
                        self.assertIs(work.root, f.original_root)
                        self.assertIs(work.entry, f.root_entry)
                        self.assertIs(work.db._attempt, f.root_entry)
                        self.assertTrue(work.db.in_transaction)
                        self.assertLessEqual(work.entry.deadline, deadline)
                        self.assertIs(work.host, f.session.host)
                        work.ordinary.validate()  # real SAME Work healthy positive
                        self.assertEqual(f.business(), f.prepared)
                        tentative = copy.deepcopy(work.state)
                        job = next(j for j in tentative['_lifecycle']['jobs'] if j['id'] == f.root_request['job_id'])
                        self.assertEqual(job['result']['status'], 'appointed')
                        self.assertIn(f.root_request['proposed_person_id'], [p['id'] for p in tentative['persons']])
                        active['work'] = work
                        report['negative'] = dict(at=time.monotonic(), request=copy.deepcopy(f.root_request),
                            response=copy.deepcopy(f.root_response), tentative=tentative,
                            leaf_exceptions=active['leaf_errors'],
                            before_injection=database_image(f.session.directory), controls_before=f.controls(),
                            ordinary_paths=[str(item[2]) for item in work.ordinary.modules])
                        for name, module in tuple(sys.modules.items()):
                            if name == 'app' or name.startswith('app.'):
                                loaded[name] = validate_app_module(self, name, module, source, copied, saved_modules.get(name))
                        self.assertIs(helper.RealGateway._generate_owned.__code__, original_gateway_code)
                        self.assertIs(helper.RealGateway._generate_owned.__globals__, original_gateway_namespace)
                        drift = original_bytes + b'\n# parent1103 owned-only ordinary disk bytes drift; NO reload\n'
                        self.assertEqual(target.read_bytes(), original_bytes)
                        target.write_bytes(drift)
                        report['negative']['bytes'] = dict(path=str(target), before=digest(original_bytes),
                            after=digest(target.read_bytes()), code_reloaded=False)
                        report['negative']['post_injection'] = database_image(f.session.directory)
                        self.assertEqual(business_image(report['negative']['post_injection']),
                            business_image(report['negative']['before_injection']))
                        f.unchanged_controls(report['negative']['controls_before'])
                        self.assertEqual(attestation.FILES, SOURCE9)
                        self.assertEqual(product.source_identity(f.session.world, f.session.host, f.session.coordinator), f.source_identity)
                        self.assertIs(helper.RealGateway._generate_owned.__code__, original_gateway_code)
                        self.assertIs(helper.RealGateway._generate_owned.__globals__, original_gateway_namespace)
                        active['drifted'] = True
                    except BaseException as error:
                        f.trace_error = error
                        raise
                if code is active['leaf_code'] and event == 'exception' and active.get('drifted'):
                    if not active.get('leaf_checked'):
                        binder.check_source_binding(active['leaf_binding'])
                        active['leaf_checked'] = True
                    active['leaf_errors'].append(dict(line=frame.f_lineno, error=error_record(arg[1])))
                if owned_resource or code in (active['mutate_code'], active['leaf_code']):
                    return trace
                return None

            threading.settrace(trace)
            sys.settrace(trace)
            trace_installed = True
            # Full healthy committed root and completed-job replay, same freshly
            # imported copy, separate world, still this method's original120.
            positive = new_fixture()
            positive.build()
            positive.original_root.deadline = min(positive.original_root.deadline, deadline)
            # Already-read stable method owns its actual CLI/build; call its
            # injury/replay implementation WITHOUT building another session.
            positive.injury()
            def replay():
                positive.final_armed = False
                a = positive.session.host.adjudicator
                q, response = positive.root_request, positive.root_response
                before, calls = positive.business(), len(positive.calls)
                result = next(j for j in positive.raw()['_lifecycle']['jobs'] if j['id'] == q['job_id'])['result']
                self.assertEqual(result['status'], 'appointed')
                self.assertEqual(a.prepare_lifecycle(q['job_id'], q['service_version']), dict(status='completed', result=result))
                self.assertEqual(a.preview_lifecycle(q['job_id'], helper.sha(q), helper.canonical(response), response), dict(status='completed', result=result))
                self.assertEqual(a.commit_lifecycle(q['job_id'], helper.sha(q), helper.canonical(response), response,
                    decision_sha256=helper.sha(response)), result)
                self.assertEqual(positive.business(), before)
                self.assertEqual(len(positive.calls), calls)
                positive.hits.append(copy.deepcopy(result))
            # helper.observe installs its own thread observer; multiplexing it
            # would lose resource returns, so use one exact source site below.
            replay_binding = positive.source_binding(helper.ActorHost, 'resolve_lifecycle_jobs')
            replay_code, replay_line = positive.source_site(helper.ActorHost, 'resolve_lifecycle_jobs', 'results.append(result)')
            original_trace = trace
            def positive_trace(frame, event, arg):
                if frame.f_code is replay_code:
                    self.assertTrue(frame.f_globals is replay_binding['namespace'], 'replay globals mismatch')
                    original_trace(frame, event, arg)
                    if event == 'line' and frame.f_lineno == replay_line and positive.final_armed:
                        try:
                            positive.check_source_binding(replay_binding)
                            replay()
                        except BaseException as error:
                            positive.trace_error = error
                            raise
                    return positive_trace
                return original_trace(frame, event, arg)
            threading.settrace(positive_trace)
            try:
                positive.step(61)
            finally:
                threading.settrace(trace)
            if positive.trace_error is not None:
                raise positive.trace_error
            self.assertEqual(len(positive.hits), 1)
            self.assertEqual(positive.hits[0]['person_id'], positive.root_request['proposed_person_id'])
            self.assertEqual([c['kind'] for c in positive.calls].count('succession'), 1)
            report['positive'] = dict(request=positive.root_request, response=positive.root_response,
                result=positive.hits[0], image=database_image(positive.session.directory), calls=copy.deepcopy(positive.calls))
            report['cleanup_phase'] = 'healthy_first_close'
            assert_closed(close_first(positive, 'healthy_first_close'))
            assert_owned_returns(positive)
            report['cleanup_phase'] = 'real_work'
            positive.doCleanups()
            self.assertEqual(product.verify_generation(), digest(clone_raw)['sha256'])

            negative = new_fixture()
            negative.build()
            negative.original_root.deadline = min(negative.original_root.deadline, deadline)
            negative.injury()
            active['fixture'] = negative
            active['mutate_code'], active['mutate_line'] = negative.source_site(helper.WorldStore, '_lc_geo1133_mutate_body',
                'if _precommit is not None:_precommit(db)')
            active['mutate_binding'] = negative.source_binding(helper.WorldStore, '_lc_geo1133_mutate_body')
            active['leaf_binding'] = negative.source_binding(scheduler._LifecycleOrdinarySources, 'validate')
            active['leaf_code'] = active['leaf_binding']['code']
            # validate has several unproven calls: select the complete unique
            # original raw/path predicate, not an ambiguous bare unproven().
            tree = ast.parse(Path(scheduler.__file__).read_bytes())
            owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == '_LifecycleOrdinarySources')
            validate = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == 'validate')
            raw_gate = [n for n in ast.walk(validate) if isinstance(n, ast.If) and
                ast.unparse(n.test) == "vars(module) is not namespace or Path(namespace['__file__']).resolve() != path or not _lifecycle_source_matches(path, raw, self.check)"]
            self.assertEqual(len(raw_gate), 1)
            active['leaf_line'] = raw_gate[0].body[0].lineno
            active['armed'] = True
            # Arm only after original succession transport has set final_armed;
            # otherwise the prepare precommit is not the final candidate gate.
            base_trace = trace
            def negative_trace(frame, event, arg):
                if frame.f_code is active['mutate_code'] and event == 'line' and not negative.final_armed:
                    return negative_trace
                result = base_trace(frame, event, arg)
                return negative_trace if frame.f_code is active['mutate_code'] else result
            threading.settrace(negative_trace)
            try:
                error = negative.start_join()
            finally:
                threading.settrace(trace)
            injected = active.get('drifted', False)
            self.assertTrue(injected, 'must reach tentative final Work; earlier source/schema refusal is not coverage')
            self.assertEqual(error, 'WINDOW_RECOVERY_UNPROVEN')
            self.assertTrue(any(item['line'] == active['leaf_line'] and item['error']['type'] == 'WorldError'
                and item['error']['code'] == 'WINDOW_RECOVERY_UNPROVEN' for item in active['leaf_errors']),
                'actual original ordinary raw-byte rejection leaf must raise')
            report['negative']['leaf_exceptions'] = copy.deepcopy(active['leaf_errors'])
            report['negative']['coordinator_error'] = error
            report['negative']['after_rejection'] = database_image(negative.session.directory)
            self.assertEqual(business_image(report['negative']['after_rejection']), business_image(report['negative']['post_injection']))
            self.assertEqual(negative.business(), negative.prepared)
            negative.unchanged_controls(report['negative']['controls_before'])
            state = negative.raw()
            job = next(j for j in state['_lifecycle']['jobs'] if j['id'] == negative.root_request['job_id'])
            self.assertIsNone(job['result'])
            self.assertEqual(job['attempts'][-1]['status'], 'pending')
            self.assertEqual(state['status'], 'running')
            self.assertNotIn(negative.root_request['proposed_person_id'], [p['id'] for p in state['persons']])
            self.assertEqual([c['kind'] for c in negative.calls].count('succession'), 1)
            read_before = database_image(negative.session.directory)
            receipt_controls = negative.controls()
            calls_before_receipt = len(negative.calls)
            self.assertEqual(negative.session.world.receipt(helper.OLD, negative.old_activation['intent_id']), negative.old_receipt)
            self.assertEqual(business_image(database_image(negative.session.directory)), business_image(read_before))
            negative.unchanged_controls(receipt_controls)
            self.assertEqual(len(negative.calls), calls_before_receipt)
            self.assertNotEqual(digest(target.read_bytes()), digest(original_bytes))
            self.assertIs(helper.RealGateway._generate_owned.__code__, original_gateway_code)
            self.assertIs(helper.RealGateway._generate_owned.__globals__, original_gateway_namespace)
            # Preserve FIRST original close/CAS while bytes are STILL DRIFTED.
            report['cleanup_phase'] = 'drifted_first_close'
            first = close_first(negative, 'drifted_first_close')
            report['negative']['after_first_close'] = database_image(negative.session.directory)
            report['negative']['calls'] = copy.deepcopy(negative.calls)
            self.assertEqual(business_image(report['negative']['after_first_close']), business_image(read_before))
            assert_closed(first)
            assert_owned_returns(negative)
            root_cas = [e for e in first['events'] if e['operation'] == 'root_release_CAS' and e['event'] == 'return']
            self.assertEqual([e['value'] for e in root_cas], [typed(True)], 'native original first root release CAS must be True')
            self.assertTrue(any(e['operation'] == 'root_heartbeat_finish' and e.get('value') == typed(True) for e in first['events']))
            self.assertTrue(any(e['operation'] == 'original_gateway_close' and e['event'] == 'return' for e in first['events']))
            self.assertTrue(any(e['operation'] == 'synthetic_transport_close' and e['event'] == 'return' for e in first['events']))
            self.assertTrue(negative.transport.cancelled.is_set())
            self.assertTrue(negative.transport.close_threads)
            self.assertIs(negative.transport.root_thread, negative.session.coordinator.worker)
            lease = negative.rows('coordinator_execution_leases')[0]
            self.assertEqual((lease['world_id'], lease['epoch'], lease['owner_token']),
                (negative.original_root.permit.world_id, negative.original_root.permit.epoch, None))
            # Every actual native CAS/finish return is retained in order, not
            # rewritten from a final closed-looking host flag.
            actor_cas = [e for e in report['events'] if e['operation'] == 'actor_release_CAS' and e['event'] == 'return']
            actor_finish = [e for e in report['events'] if e['operation'] == 'actor_heartbeat_finish' and e['event'] == 'return']
            self.assertTrue(actor_cas)
            self.assertTrue(actor_finish)
            self.assertTrue(all(e['value'] == typed(True) for e in actor_cas + actor_finish))
            ordinary = active['work'].ordinary
            for module, namespace, path, raw, bindings, functions in ordinary.modules:
                self.assertTrue(vars(module) is namespace, 'ordinary namespace identity mismatch')
                self.assertEqual(Path(namespace['__file__']).resolve(), path)
                for owner, name, original, chain in functions:
                    for function, code, space in chain:
                        self.assertIs(function.__code__, code)
                        self.assertTrue(function.__globals__ is space, 'ordinary globals identity mismatch')
            self.assertTrue(negative.attempts)
            self.assertTrue(all(a.calls and not a.thread.is_alive() and not any(t.is_alive() for t in a.calls) for a in negative.attempts))
            self.assertEqual(attestation.FILES, SOURCE9)
            budget()
            report['status'] = 'ASSERTIONS_COMPLETE_RUNTIME_ONLY_NOT_SOURCE_PASS'
        except BaseException as error:
            primary = error
            report['failures'].append(dict(phase='semantic_or_preparation', error=error_record(error)))
        finally:
            # Even on an earlier assertion failure, record first original close
            # BEFORE restoring any bytes. Never replace its actual pending/CAS.
            for fixture in fixtures:
                try:
                    close_first(fixture, 'failure_first_close_before_source_recovery')
                except BaseException as error:
                    report['failures'].append(dict(phase='first_close_recording', error=error_record(error)))
                    primary = primary or error
            if target is not None and original_bytes is not None and target.read_bytes() != original_bytes:
                recovery = dict(phase='RESOURCE_ONLY_SOURCE_RECOVERY', at=time.monotonic(),
                    before=digest(target.read_bytes()), semantic_retry=False, does_not_erase_first_results=True)
                target.write_bytes(original_bytes)
                active['source_recovered'] = True
                recovery['after'] = digest(target.read_bytes())
                report['phases'].append(recovery)
            for fixture in fixtures:
                try:
                    first = getattr(fixture, '_first1103_close', None)
                    if first is not None and (first.get('error') or first.get('resource_error') or first['after']['live'] or
                            first['after'].get('host_pending') or first['after'].get('coordinator_pending')):
                        # Independent PHYSICAL cleanup only; not a semantic rerun.
                        report['cleanup_phase'] = 'RESOURCE_ONLY_REPEAT_CLEANUP'
                        start = len(report['events'])
                        fixture.session.close()
                        report['phases'].append(dict(phase='RESOURCE_ONLY_NATIVE_RECLOSE', at=time.monotonic(),
                            resources=resources(fixture), events=copy.deepcopy(report['events'][start:])))
                    self.assertTrue(fixture.doCleanups(), 'fixture cleanup callback failure is not success')
                    report['phases'].append(dict(phase='final_physical_resources', resources=resources(fixture)))
                    self.assertEqual(resources(fixture)['live'], [])
                    self.assertEqual(fixture.violations, [])
                except BaseException as error:
                    report['failures'].append(dict(phase='physical_cleanup', error=error_record(error)))
                    primary = primary or error
            if trace_installed:
                threading.settrace(prior_thread_trace)
                sys.settrace(prior_sys_trace)
            # Restore only after all this method's original workers/providers
            # and heartbeats have exited; no foreign module reset is performed.
            live = [name for fixture in fixtures for name in resources(fixture)['live']]
            if saved_modules is not None and not live:
                for name in tuple(sys.modules):
                    if name == 'app' or name.startswith('app.'):
                        del sys.modules[name]
                sys.modules.update(saved_modules)
                sys.path[:] = saved_path
                sys.dont_write_bytecode = saved_bytecode
                restored = {name: module for name, module in tuple(sys.modules.items())
                    if name == 'app' or name.startswith('app.')}
                self.assertEqual(set(restored), set(saved_modules))
                self.assertTrue(all(restored[name] is module for name, module in saved_modules.items()))
                self.assertEqual(sys.path, saved_path)
                self.assertIs(sys.dont_write_bytecode, saved_bytecode)
                report['namespace_restored'] = True
            else:
                report['namespace_restored'] = saved_modules is None
                report['live_blocker'] = live
                if live:
                    primary = primary or AssertionError('owned threads remain live; namespace NOT restored')
            try:
                budget()
            except BaseException as error:
                report['failures'].append(dict(phase='whole120_final', error=error_record(error)))
                primary = primary or error
            report['elapsed_end'] = time.monotonic()
            if primary is not None:
                report['status'] = 'FAILED_PRESERVED_NOT_PASS'
            # Diagnostic write only after semantic gate and resource work. The
            # full typed post-injection state is never replaced by a clean run.
            (owned / 'source-drift1103-runtime.json').write_text(json.dumps(report, ensure_ascii=False,
                indent=2, allow_nan=False) + '\n', encoding='utf-8')
        if primary is not None:
            raise primary


    def test_owned_loaded_gateway_growth_over_16mib_rejects_original_final_work_without_ghosts(self):
        deadline = time.monotonic() + 120  # BEFORE copy/import/setUp, never renewed.
        owned = SCRATCH / ('parent1103-owned-source-' + uuid.uuid4().hex)
        owned.mkdir(parents=True, exist_ok=False)
        source = owned / 'source'
        source.mkdir()
        report = dict(schema='parent1103-future-runtime-evidence-v1', status='INCOMPLETE',
            whole_deadline=deadline, profile_version=PROFILE_VERSION, owned_source=str(source),
            phases=[], events=[], failures=[], source_pass=False)
        fixtures = []
        saved_modules = None
        saved_path = list(sys.path)
        saved_bytecode = sys.dont_write_bytecode
        prior_thread_trace, prior_sys_trace = threading.gettrace(), sys.gettrace()
        target = None
        original_bytes = None
        primary = None
        helper = None
        injected = False
        trace_installed = False
        active = {}  # recovery state exists even if source admission fails
        self.assertIsNone(prior_thread_trace, 'must not displace foreign thread observer')
        self.assertIsNone(prior_sys_trace, 'must not displace foreign main observer')

        def budget():
            self.assertLess(time.monotonic(), deadline, 'one cumulative whole120; no retry renewal')

        def resources(fixture):
            s = fixture.session
            if s is None:
                return dict(session=False, live=[])
            c, h = s.coordinator, s.host
            root = c._owner_attempt
            threads = [c.worker, None if root is None else root.thread, *h._pending_threads]
            for attempt in fixture.attempts:
                threads.extend([attempt.thread, *attempt.calls])
            actual = []
            for thread in threads:
                if thread is not None and all(thread is not old for old in actual):
                    actual.append(thread)
            for thread in actual:
                thread.join(0)
            return dict(session=True, host_pending=h.cleanup_pending, coordinator_pending=c.cleanup_pending,
                lifecycle_active=h._lifecycle_active is not None, lease_cleanups=len(h._lease_cleanups),
                process=h._process is not None, root_failure=None if root is None or root.failure is None else error_record(root.failure),
                root_stop=None if root is None else root.stop.is_set(),
                threads=[dict(name=t.name, ident=t.ident, alive=t.is_alive()) for t in actual],
                live=[t.name for t in actual if t.is_alive()])

        def close_first(fixture, phase):
            """Preserve FIRST native close, never overwrite with a later success."""
            if getattr(fixture, '_first1103_close', None) is not None:
                return fixture._first1103_close
            record = dict(phase=phase, at=time.monotonic(), before=resources(fixture))
            start = len(report['events'])
            try:
                if fixture.session is not None:
                    record['return'] = typed(fixture.session.close())
            except BaseException as error:
                record['error'] = error_record(error)
                fixture._first1103_close_error = error  # original stays private, no str/args
            record['events'] = copy.deepcopy(report['events'][start:])
            fixture._first1103_close = record
            report['phases'].append(record)
            try:
                record['after'] = resources(fixture)
            except BaseException as error:
                record['resource_error'] = error_record(error)
                raise
            return record

        def new_fixture():
            budget()
            fixture = helper.LifecycleAuthorityProductTests(methodName='runTest')
            fixtures.append(fixture)
            fixture.setUp()
            fixture.deadline = min(fixture.deadline, deadline)
            self.assertLessEqual(fixture.deadline, deadline)
            return fixture

        def assert_closed(record):
            self.assertNotIn('error', record, 'original close error is not washed by source recovery')
            self.assertNotIn('resource_error', record, 'first resource observation remains unknown')
            after = record['after']
            self.assertEqual(after['live'], [])
            self.assertFalse(after['host_pending'])
            self.assertFalse(after['coordinator_pending'])
            self.assertFalse(after['lifecycle_active'])
            self.assertEqual(after['lease_cleanups'], 0)
            self.assertFalse(after['process'])

        try:
            # Root is derived from THIS actual loaded annex, never guessed future root.
            actual_module = sys.modules[self.__class__.__module__]
            actual_function = self.__class__.test_owned_loaded_gateway_growth_over_16mib_rejects_original_final_work_without_ghosts
            self.assertIs(vars(actual_module)[self.__class__.__name__], self.__class__)
            self.assertTrue(actual_function.__globals__ is vars(actual_module), 'actual annex globals mismatch')
            actual_path = Path(actual_module.__file__).resolve(strict=True)
            profile_root = actual_path.parents[2]
            self.assertEqual(actual_path.relative_to(profile_root).as_posix(), 'app/tests/test_lifecycle_source_drift1103.py')
            manifest_raw = (profile_root / 'generation-manifest.json').read_bytes()
            manifest = json.loads(manifest_raw)
            self.assertEqual(manifest['schema'], 'parent950-product-v1')
            self.assertEqual(Path(manifest['root']).resolve(strict=True), profile_root)
            self.assertIs(type(manifest['files']), dict)
            self.assertTrue(manifest['files'])
            self.assertIn('app/tests/test_lifecycle_source_drift1103.py', manifest['files'])
            for name, module in tuple(sys.modules.items()):
                if name == 'app' or name.startswith('app.'):
                    validate_app_module(self, name, module, profile_root, manifest['files'])
            report['actual_source_profile'] = dict(version=PROFILE_VERSION, root=str(profile_root),
                manifest=digest(manifest_raw), members=len(manifest['files']),
                own_pin=digest(actual_path.read_bytes()), files=copy.deepcopy(manifest['files']),
                old_BASE156_evidence_unchanged=True)
            copied = {}
            for relative, pin in manifest['files'].items():
                budget()
                part = Path(relative)
                self.assertFalse(part.is_absolute())
                self.assertNotIn('..', part.parts)
                self.assertEqual(part.parts[0], 'app')
                src, dst = profile_root / part, source / part
                self.assertFalse(src.is_symlink())
                self.assertTrue(src.resolve(strict=True).is_relative_to(profile_root))
                raw = src.read_bytes()
                self.assertEqual(digest(raw), pin)
                dst.parent.mkdir(parents=True, exist_ok=True)
                with dst.open('xb') as stream:
                    stream.write(raw)  # ordinary independent bytes, NOT link/copytree
                self.assertFalse(dst.is_symlink())
                self.assertEqual(dst.stat().st_nlink, 1)
                self.assertFalse(dst.samefile(src))
                self.assertEqual(digest(dst.read_bytes()), pin)
                copied[relative] = pin
            self.assertEqual(copied, manifest['files'])
            self.assertEqual((profile_root / 'generation-manifest.json').read_bytes(), manifest_raw)
            # The copy has its OWN manifest identity. No claim of original SHA.
            clone_manifest = copy.deepcopy(manifest)
            clone_manifest['root'] = str(source.resolve())
            clone_raw = (json.dumps(clone_manifest, ensure_ascii=False, indent=2) + '\n').encode('utf-8')
            (source / 'generation-manifest.json').write_bytes(clone_raw)
            self.assertEqual((source / 'generation-manifest.json').read_bytes(), clone_raw)
            report['copy'] = dict(members=len(copied), files=copied, parent_manifest=digest(manifest_raw),
                owned_manifest=digest(clone_raw), changed_metadata=['root'], same_parent_manifest_sha=False)

            # Save/switch ONLY app modules. No foreign/stdlib/Hermes eviction or
            # registry reset. This method has not created a canonical app object.
            saved_modules = {k: v for k, v in tuple(sys.modules.items()) if k == 'app' or k.startswith('app.')}
            for name in saved_modules:
                del sys.modules[name]
            sys.path.insert(0, str(source))
            sys.dont_write_bytecode = True
            helper = importlib.import_module('app.tests.test_lifecycle_authority1100')
            product = helper.product_cli
            scheduler = helper.scheduler
            execution = importlib.import_module('app.actor_execution')
            coordinator_execution = importlib.import_module('app.coordinator_execution')
            attestation = importlib.import_module('app.source_attestation')
            self.assertEqual(product.ROOT.resolve(), source.resolve())
            self.assertEqual(attestation.FILES, SOURCE9)
            self.assertEqual(product.verify_generation(), digest(clone_raw)['sha256'])
            # Pin every actual app module path and source against the copied
            # manifest. The retained old dictionaries never supply new globals.
            loaded = {}
            for name, module in tuple(sys.modules.items()):
                if name == 'app' or name.startswith('app.'):
                    loaded[name] = validate_app_module(self, name, module, source, copied, saved_modules.get(name))
            report['loaded_app_bindings'] = loaded
            target = source / 'app/real_provider.py'
            original_bytes = target.read_bytes()
            original_gateway_code = helper.RealGateway._generate_owned.__code__
            original_gateway_namespace = helper.RealGateway._generate_owned.__globals__

            binder = helper.LifecycleAuthorityProductTests(methodName='runTest')
            resource_specs = ((helper.WorldStore, 'release_actor_execution', 'actor_release_CAS'),
                (helper.WorldStore, 'release_coordinator_execution', 'root_release_CAS'),
                (execution.Attempt, 'finish', 'actor_heartbeat_finish'),
                (execution.Attempt, 'cancel', 'actor_cancel'),
                (coordinator_execution.CoordinatorAttempt, 'finish', 'root_heartbeat_finish'),
                (helper.RealGateway, 'close', 'original_gateway_close'),
                (helper.ActorHost, '_cancel_lifecycle', 'original_root_cancel_gateway'),
                (helper.ActorHost, 'close', 'original_host_close'),
                (helper.ActorHost, 'act', 'original_host_exit'),
                (helper.Coordinator, 'close', 'original_coordinator_close'),
                (helper.SyntheticTransport, 'close', 'synthetic_transport_close'))
            observed = {binding['code']: (binding, label) for cls, method, label in resource_specs
                for binding in [binder.source_binding(cls, method)]}
            observed_calls = {}
            active = dict(fixture=None, mutate_code=None, mutate_line=None, leaf_code=None, leaf_line=None,
                          armed=False, work=None, leaf_errors=[])

            def append_event(item):
                self.assertLess(len(report['events']), 1024, 'finite owned fixture ledger overflow')
                item['order'] = len(report['events'])
                item['phase'] = report.get('cleanup_phase', 'real_work')
                report['events'].append(item)

            def resource_event(frame, event, arg):
                selected = observed.get(frame.f_code)
                if selected is None:
                    return False
                binding, label = selected
                self.assertIs(frame.f_code, binding['code'])
                self.assertTrue(frame.f_globals is binding['namespace'], 'owned return globals mismatch')
                if event == 'call':
                    obj = frame.f_locals.get('self')
                    owner = None
                    for index, fixture in enumerate(fixtures):
                        if fixture.session is None:
                            continue
                        s = fixture.session
                        allowed = (s.world, s.host, s.coordinator, s.host.real_gateway,
                            s.coordinator._owner_attempt, fixture.transport, *fixture.attempts)
                        if any(obj is value for value in allowed):
                            owner = (index, fixture)
                            break
                    if owner is None:
                        return False
                    index, fixture = owner
                    permit = frame.f_locals.get('permit') if label in ('actor_release_CAS', 'root_release_CAS') else getattr(obj, 'permit', None)
                    identity = None
                    if permit is not None:
                        if permit is fixture.original_root.permit:
                            identity = 'root'
                        else:
                            matches = [i for i,a in enumerate(fixture.attempts) if a.permit is permit]
                            self.assertEqual(len(matches), 1)
                            identity = matches[0]
                    self.assertLess(len(observed_calls), 64)
                    observed_calls[frame] = dict(operation=label, fixture=index, permit=identity)
                    append_event(dict(observed_calls[frame], event='call'))
                elif frame in observed_calls:
                    item = dict(observed_calls[frame], event=event)
                    if event in ('exception', 'return'):
                        expected = None
                        if binding['path'] == target and active.get('drifted') and not active.get('source_recovered'):
                            expected = original_bytes + b'\n#' + b'x' * 16777217
                        binder.check_source_binding(binding, expected_raw=expected)
                    if event == 'exception':
                        item['error'] = error_record(arg[1])
                        append_event(item)
                    elif event == 'return':
                        item['value'] = typed(arg) if type(arg) is bool or arg is None else dict(type=type(arg).__name__, value='NOT_SERIALIZED')
                        if label == 'original_host_exit':
                            fixture = fixtures[item['fixture']]
                            attempt = frame.f_locals.get('attempt')
                            matches = [i for i,a in enumerate(fixture.attempts) if a is attempt]
                            self.assertEqual(len(matches), 1)
                            item['permit'] = matches[0]
                            for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                                value = frame.f_locals.get(key)
                                item[key] = value if type(value) is bool else 'UNKNOWN'
                        append_event(item)
                        del observed_calls[frame]
                return True

            def assert_owned_returns(fixture):
                index = next(i for i,f in enumerate(fixtures) if f is fixture)
                events = [e for e in report['events'] if e['fixture'] == index]
                for number, attempt in enumerate(fixture.attempts):
                    exits = [e for e in events if e['operation'] == 'original_host_exit' and e['event'] == 'return' and e['permit'] == number]
                    self.assertEqual(len(exits), 1)
                    for key in ('maintenance_joined', 'providers_joined', 'lease_released'):
                        self.assertIs(exits[0][key], True)
                    chains = []
                    for label in ('actor_heartbeat_finish', 'actor_release_CAS'):
                        rows = [e for e in events if e['operation'] == label and e['permit'] == number]
                        self.assertEqual([e['event'] for e in rows], ['call', 'return'])
                        self.assertEqual(rows[-1]['value'], typed(True))
                        chains.append(rows)
                    self.assertLess(chains[0][-1]['order'], chains[1][0]['order'])
                    self.assertLess(chains[1][-1]['order'], exits[0]['order'])
                roots = [e for e in events if e['operation'] == 'root_release_CAS']
                self.assertEqual([e['event'] for e in roots], ['call', 'return'])
                self.assertEqual(roots[-1]['value'], typed(True))
                self.assertEqual(roots[-1]['permit'], 'root')
                finishes = [e for e in events if e['operation'] == 'root_heartbeat_finish']
                self.assertEqual([e['event'] for e in finishes], ['call', 'return'])
                self.assertEqual(finishes[-1]['value'], typed(True))
                self.assertLess(finishes[-1]['order'], roots[0]['order'])
                self.assertFalse(any(item['fixture'] == index for item in observed_calls.values()))

            def trace(frame, event, arg):
                # Whitelisted locals only after exact code/globals+owned self;
                # never foreign values, token, exception str/args/back-chain.
                code = frame.f_code
                owned_resource = resource_event(frame, event, arg)
                if code is active['mutate_code']:
                    self.assertTrue(frame.f_globals is active['mutate_binding']['namespace'], 'final Work globals mismatch')
                if code is active['leaf_code']:
                    self.assertTrue(frame.f_globals is active['leaf_binding']['namespace'], 'ordinary leaf globals mismatch')
                if code is active['mutate_code'] and event == 'line' and frame.f_lineno == active['mutate_line'] and active['armed']:
                    active['armed'] = False
                    f = active['fixture']
                    try:
                        binder.check_source_binding(active['mutate_binding'])
                        binder.check_source_binding(active['leaf_binding'])
                        budget()
                        work = scheduler.lifecycle_work(f.session.world, f.root_request['job_id'])
                        self.assertIs(work.root, f.original_root)
                        self.assertIs(work.entry, f.root_entry)
                        self.assertIs(work.db._attempt, f.root_entry)
                        self.assertTrue(work.db.in_transaction)
                        self.assertLessEqual(work.entry.deadline, deadline)
                        self.assertIs(work.host, f.session.host)
                        work.ordinary.validate()  # real SAME Work healthy positive
                        self.assertEqual(f.business(), f.prepared)
                        tentative = copy.deepcopy(work.state)
                        job = next(j for j in tentative['_lifecycle']['jobs'] if j['id'] == f.root_request['job_id'])
                        self.assertEqual(job['result']['status'], 'appointed')
                        self.assertIn(f.root_request['proposed_person_id'], [p['id'] for p in tentative['persons']])
                        active['work'] = work
                        report['negative'] = dict(at=time.monotonic(), request=copy.deepcopy(f.root_request),
                            response=copy.deepcopy(f.root_response), tentative=tentative,
                            leaf_exceptions=active['leaf_errors'],
                            before_injection=database_image(f.session.directory), controls_before=f.controls(),
                            ordinary_paths=[str(item[2]) for item in work.ordinary.modules])
                        for name, module in tuple(sys.modules.items()):
                            if name == 'app' or name.startswith('app.'):
                                loaded[name] = validate_app_module(self, name, module, source, copied, saved_modules.get(name))
                        self.assertIs(helper.RealGateway._generate_owned.__code__, original_gateway_code)
                        self.assertIs(helper.RealGateway._generate_owned.__globals__, original_gateway_namespace)
                        drift = original_bytes + b'\n#' + b'x' * 16777217
                        self.assertEqual(target.read_bytes(), original_bytes)
                        target.write_bytes(drift)
                        report['negative']['bytes'] = dict(path=str(target), before=digest(original_bytes),
                            after=digest(target.read_bytes()), code_reloaded=False)
                        report['negative']['post_injection'] = database_image(f.session.directory)
                        self.assertEqual(business_image(report['negative']['post_injection']),
                            business_image(report['negative']['before_injection']))
                        f.unchanged_controls(report['negative']['controls_before'])
                        self.assertEqual(attestation.FILES, SOURCE9)
                        self.assertEqual(product.source_identity(f.session.world, f.session.host, f.session.coordinator), f.source_identity)
                        self.assertIs(helper.RealGateway._generate_owned.__code__, original_gateway_code)
                        self.assertIs(helper.RealGateway._generate_owned.__globals__, original_gateway_namespace)
                        active['drifted'] = True
                    except BaseException as error:
                        f.trace_error = error
                        raise
                if code is active['leaf_code'] and event == 'exception' and active.get('drifted'):
                    if not active.get('leaf_checked'):
                        binder.check_source_binding(active['leaf_binding'])
                        active['leaf_checked'] = True
                    active['leaf_errors'].append(dict(line=frame.f_lineno, error=error_record(arg[1])))
                if owned_resource or code in (active['mutate_code'], active['leaf_code']):
                    return trace
                return None

            threading.settrace(trace)
            sys.settrace(trace)
            trace_installed = True
            # Full healthy committed root and completed-job replay, same freshly
            # imported copy, separate world, still this method's original120.
            positive = new_fixture()
            positive.build()
            positive.original_root.deadline = min(positive.original_root.deadline, deadline)
            # Already-read stable method owns its actual CLI/build; call its
            # injury/replay implementation WITHOUT building another session.
            positive.injury()
            def replay():
                positive.final_armed = False
                a = positive.session.host.adjudicator
                q, response = positive.root_request, positive.root_response
                before, calls = positive.business(), len(positive.calls)
                result = next(j for j in positive.raw()['_lifecycle']['jobs'] if j['id'] == q['job_id'])['result']
                self.assertEqual(result['status'], 'appointed')
                self.assertEqual(a.prepare_lifecycle(q['job_id'], q['service_version']), dict(status='completed', result=result))
                self.assertEqual(a.preview_lifecycle(q['job_id'], helper.sha(q), helper.canonical(response), response), dict(status='completed', result=result))
                self.assertEqual(a.commit_lifecycle(q['job_id'], helper.sha(q), helper.canonical(response), response,
                    decision_sha256=helper.sha(response)), result)
                self.assertEqual(positive.business(), before)
                self.assertEqual(len(positive.calls), calls)
                positive.hits.append(copy.deepcopy(result))
            # helper.observe installs its own thread observer; multiplexing it
            # would lose resource returns, so use one exact source site below.
            replay_binding = positive.source_binding(helper.ActorHost, 'resolve_lifecycle_jobs')
            replay_code, replay_line = positive.source_site(helper.ActorHost, 'resolve_lifecycle_jobs', 'results.append(result)')
            original_trace = trace
            def positive_trace(frame, event, arg):
                if frame.f_code is replay_code:
                    self.assertTrue(frame.f_globals is replay_binding['namespace'], 'replay globals mismatch')
                    original_trace(frame, event, arg)
                    if event == 'line' and frame.f_lineno == replay_line and positive.final_armed:
                        try:
                            positive.check_source_binding(replay_binding)
                            replay()
                        except BaseException as error:
                            positive.trace_error = error
                            raise
                    return positive_trace
                return original_trace(frame, event, arg)
            threading.settrace(positive_trace)
            try:
                positive.step(61)
            finally:
                threading.settrace(trace)
            if positive.trace_error is not None:
                raise positive.trace_error
            self.assertEqual(len(positive.hits), 1)
            self.assertEqual(positive.hits[0]['person_id'], positive.root_request['proposed_person_id'])
            self.assertEqual([c['kind'] for c in positive.calls].count('succession'), 1)
            report['positive'] = dict(request=positive.root_request, response=positive.root_response,
                result=positive.hits[0], image=database_image(positive.session.directory), calls=copy.deepcopy(positive.calls))
            report['cleanup_phase'] = 'healthy_first_close'
            assert_closed(close_first(positive, 'healthy_first_close'))
            assert_owned_returns(positive)
            report['cleanup_phase'] = 'real_work'
            positive.doCleanups()
            self.assertEqual(product.verify_generation(), digest(clone_raw)['sha256'])

            negative = new_fixture()
            negative.build()
            negative.original_root.deadline = min(negative.original_root.deadline, deadline)
            negative.injury()
            active['fixture'] = negative
            active['mutate_code'], active['mutate_line'] = negative.source_site(helper.WorldStore, '_lc_geo1133_mutate_body',
                'if _precommit is not None:_precommit(db)')
            active['mutate_binding'] = negative.source_binding(helper.WorldStore, '_lc_geo1133_mutate_body')
            active['leaf_binding'] = negative.source_binding(scheduler._LifecycleOrdinarySources, 'validate')
            active['leaf_code'] = active['leaf_binding']['code']
            # validate has several unproven calls: select the complete unique
            # original raw/path predicate, not an ambiguous bare unproven().
            tree = ast.parse(Path(scheduler.__file__).read_bytes())
            owner = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == '_LifecycleOrdinarySources')
            validate = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == 'validate')
            raw_gate = [n for n in ast.walk(validate) if isinstance(n, ast.If) and
                ast.unparse(n.test) == "vars(module) is not namespace or Path(namespace['__file__']).resolve() != path or not _lifecycle_source_matches(path, raw, self.check)"]
            self.assertEqual(len(raw_gate), 1)
            active['leaf_line'] = raw_gate[0].body[0].lineno
            active['armed'] = True
            # Arm only after original succession transport has set final_armed;
            # otherwise the prepare precommit is not the final candidate gate.
            base_trace = trace
            def negative_trace(frame, event, arg):
                if frame.f_code is active['mutate_code'] and event == 'line' and not negative.final_armed:
                    return negative_trace
                result = base_trace(frame, event, arg)
                return negative_trace if frame.f_code is active['mutate_code'] else result
            threading.settrace(negative_trace)
            try:
                error = negative.start_join()
            finally:
                threading.settrace(trace)
            injected = active.get('drifted', False)
            self.assertTrue(injected, 'must reach tentative final Work; earlier source/schema refusal is not coverage')
            self.assertEqual(error, 'WINDOW_RECOVERY_UNPROVEN')
            self.assertTrue(any(item['line'] == active['leaf_line'] and item['error']['type'] == 'WorldError'
                and item['error']['code'] == 'WINDOW_RECOVERY_UNPROVEN' for item in active['leaf_errors']),
                'actual original ordinary raw-byte rejection leaf must raise')
            report['negative']['leaf_exceptions'] = copy.deepcopy(active['leaf_errors'])
            report['negative']['coordinator_error'] = error
            report['negative']['after_rejection'] = database_image(negative.session.directory)
            self.assertEqual(business_image(report['negative']['after_rejection']), business_image(report['negative']['post_injection']))
            self.assertEqual(negative.business(), negative.prepared)
            negative.unchanged_controls(report['negative']['controls_before'])
            state = negative.raw()
            job = next(j for j in state['_lifecycle']['jobs'] if j['id'] == negative.root_request['job_id'])
            self.assertIsNone(job['result'])
            self.assertEqual(job['attempts'][-1]['status'], 'pending')
            self.assertEqual(state['status'], 'running')
            self.assertNotIn(negative.root_request['proposed_person_id'], [p['id'] for p in state['persons']])
            self.assertEqual([c['kind'] for c in negative.calls].count('succession'), 1)
            read_before = database_image(negative.session.directory)
            receipt_controls = negative.controls()
            calls_before_receipt = len(negative.calls)
            self.assertEqual(negative.session.world.receipt(helper.OLD, negative.old_activation['intent_id']), negative.old_receipt)
            self.assertEqual(business_image(database_image(negative.session.directory)), business_image(read_before))
            negative.unchanged_controls(receipt_controls)
            self.assertEqual(len(negative.calls), calls_before_receipt)
            self.assertNotEqual(digest(target.read_bytes()), digest(original_bytes))
            self.assertIs(helper.RealGateway._generate_owned.__code__, original_gateway_code)
            self.assertIs(helper.RealGateway._generate_owned.__globals__, original_gateway_namespace)
            # Preserve FIRST original close/CAS while bytes are STILL DRIFTED.
            report['cleanup_phase'] = 'drifted_first_close'
            first = close_first(negative, 'drifted_first_close')
            report['negative']['after_first_close'] = database_image(negative.session.directory)
            report['negative']['calls'] = copy.deepcopy(negative.calls)
            self.assertEqual(business_image(report['negative']['after_first_close']), business_image(read_before))
            assert_closed(first)
            assert_owned_returns(negative)
            root_cas = [e for e in first['events'] if e['operation'] == 'root_release_CAS' and e['event'] == 'return']
            self.assertEqual([e['value'] for e in root_cas], [typed(True)], 'native original first root release CAS must be True')
            self.assertTrue(any(e['operation'] == 'root_heartbeat_finish' and e.get('value') == typed(True) for e in first['events']))
            self.assertTrue(any(e['operation'] == 'original_gateway_close' and e['event'] == 'return' for e in first['events']))
            self.assertTrue(any(e['operation'] == 'synthetic_transport_close' and e['event'] == 'return' for e in first['events']))
            self.assertTrue(negative.transport.cancelled.is_set())
            self.assertTrue(negative.transport.close_threads)
            self.assertIs(negative.transport.root_thread, negative.session.coordinator.worker)
            lease = negative.rows('coordinator_execution_leases')[0]
            self.assertEqual((lease['world_id'], lease['epoch'], lease['owner_token']),
                (negative.original_root.permit.world_id, negative.original_root.permit.epoch, None))
            # Every actual native CAS/finish return is retained in order, not
            # rewritten from a final closed-looking host flag.
            actor_cas = [e for e in report['events'] if e['operation'] == 'actor_release_CAS' and e['event'] == 'return']
            actor_finish = [e for e in report['events'] if e['operation'] == 'actor_heartbeat_finish' and e['event'] == 'return']
            self.assertTrue(actor_cas)
            self.assertTrue(actor_finish)
            self.assertTrue(all(e['value'] == typed(True) for e in actor_cas + actor_finish))
            ordinary = active['work'].ordinary
            for module, namespace, path, raw, bindings, functions in ordinary.modules:
                self.assertTrue(vars(module) is namespace, 'ordinary namespace identity mismatch')
                self.assertEqual(Path(namespace['__file__']).resolve(), path)
                for owner, name, original, chain in functions:
                    for function, code, space in chain:
                        self.assertIs(function.__code__, code)
                        self.assertTrue(function.__globals__ is space, 'ordinary globals identity mismatch')
            self.assertTrue(negative.attempts)
            self.assertTrue(all(a.calls and not a.thread.is_alive() and not any(t.is_alive() for t in a.calls) for a in negative.attempts))
            self.assertEqual(attestation.FILES, SOURCE9)
            budget()
            report['status'] = 'ASSERTIONS_COMPLETE_RUNTIME_ONLY_NOT_SOURCE_PASS'
        except BaseException as error:
            primary = error
            report['failures'].append(dict(phase='semantic_or_preparation', error=error_record(error)))
        finally:
            # Even on an earlier assertion failure, record first original close
            # BEFORE restoring any bytes. Never replace its actual pending/CAS.
            for fixture in fixtures:
                try:
                    close_first(fixture, 'failure_first_close_before_source_recovery')
                except BaseException as error:
                    report['failures'].append(dict(phase='first_close_recording', error=error_record(error)))
                    primary = primary or error
            if target is not None and original_bytes is not None and target.read_bytes() != original_bytes:
                recovery = dict(phase='RESOURCE_ONLY_SOURCE_RECOVERY', at=time.monotonic(),
                    before=digest(target.read_bytes()), semantic_retry=False, does_not_erase_first_results=True)
                target.write_bytes(original_bytes)
                active['source_recovered'] = True
                recovery['after'] = digest(target.read_bytes())
                report['phases'].append(recovery)
            for fixture in fixtures:
                try:
                    first = getattr(fixture, '_first1103_close', None)
                    if first is not None and (first.get('error') or first.get('resource_error') or first['after']['live'] or
                            first['after'].get('host_pending') or first['after'].get('coordinator_pending')):
                        # Independent PHYSICAL cleanup only; not a semantic rerun.
                        report['cleanup_phase'] = 'RESOURCE_ONLY_REPEAT_CLEANUP'
                        start = len(report['events'])
                        fixture.session.close()
                        report['phases'].append(dict(phase='RESOURCE_ONLY_NATIVE_RECLOSE', at=time.monotonic(),
                            resources=resources(fixture), events=copy.deepcopy(report['events'][start:])))
                    self.assertTrue(fixture.doCleanups(), 'fixture cleanup callback failure is not success')
                    report['phases'].append(dict(phase='final_physical_resources', resources=resources(fixture)))
                    self.assertEqual(resources(fixture)['live'], [])
                    self.assertEqual(fixture.violations, [])
                except BaseException as error:
                    report['failures'].append(dict(phase='physical_cleanup', error=error_record(error)))
                    primary = primary or error
            if trace_installed:
                threading.settrace(prior_thread_trace)
                sys.settrace(prior_sys_trace)
            # Restore only after all this method's original workers/providers
            # and heartbeats have exited; no foreign module reset is performed.
            live = [name for fixture in fixtures for name in resources(fixture)['live']]
            if saved_modules is not None and not live:
                for name in tuple(sys.modules):
                    if name == 'app' or name.startswith('app.'):
                        del sys.modules[name]
                sys.modules.update(saved_modules)
                sys.path[:] = saved_path
                sys.dont_write_bytecode = saved_bytecode
                restored = {name: module for name, module in tuple(sys.modules.items())
                    if name == 'app' or name.startswith('app.')}
                self.assertEqual(set(restored), set(saved_modules))
                self.assertTrue(all(restored[name] is module for name, module in saved_modules.items()))
                self.assertEqual(sys.path, saved_path)
                self.assertIs(sys.dont_write_bytecode, saved_bytecode)
                report['namespace_restored'] = True
            else:
                report['namespace_restored'] = saved_modules is None
                report['live_blocker'] = live
                if live:
                    primary = primary or AssertionError('owned threads remain live; namespace NOT restored')
            try:
                budget()
            except BaseException as error:
                report['failures'].append(dict(phase='whole120_final', error=error_record(error)))
                primary = primary or error
            report['elapsed_end'] = time.monotonic()
            if primary is not None:
                report['status'] = 'FAILED_PRESERVED_NOT_PASS'
            # Diagnostic write only after semantic gate and resource work. The
            # full typed post-injection state is never replaced by a clean run.
            (owned / 'source-drift1103-runtime.json').write_text(json.dumps(report, ensure_ascii=False,
                indent=2, allow_nan=False) + '\n', encoding='utf-8')
        if primary is not None:
            raise primary


    def test_ordinary_constructor_prepaid_bounded_growth_and_unchanged_bytes(self):
        # Independent inert constructor domains, never another allowance for a
        # poisoned Entry. Full business growth is the separate complete method.
        import io
        import types
        from unittest.mock import patch
        from app import coordinator_scheduler as scheduler
        from app import real_provider
        root = Path(scheduler.__file__).resolve().parent.parent
        target = Path(real_provider.__file__).resolve()
        original = target.read_bytes()
        host = types.SimpleNamespace(adjudicator=None, real_gateway=None, real_settings=None, world=None)
        opened = Path.open
        reads = []
        class ReadGuard:
            def __init__(self, stream, growth):
                self.stream, self.growth = stream, growth
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return self.stream.__exit__(*args)
            def read(self, size=-1):
                reads.append(size)
                self_outer.assertGreater(size, 0, 'no read_bytes/unbounded read before payment')
                self_outer.assertLessEqual(size, 65536)
                return self.stream.read(size)
        self_outer = self
        growth = False
        def guarded(path, *args, **kwargs):
            if path.resolve() == target and args and args[0] == 'rb':
                # stat remains the original, then the exact owned source stream
                # grows beyond 16MiB before the first read; no source9 mutation.
                stream = io.BytesIO(original + b'\n#' + b'x' * 16777217) if growth else opened(path, *args, **kwargs)
                return ReadGuard(stream, growth)
            return opened(path, *args, **kwargs)
        with patch.object(Path, 'open', guarded):
            with scheduler._lifecycle_operation_scope() as healthy:
                ordinary = scheduler._LifecycleOrdinarySources(host, root, lambda: None)
                self.assertEqual(next(item[3] for item in ordinary.modules if item[2] == target), original)
                ordinary.validate()
                self.assertFalse(healthy.failed)
                self.assertTrue(any('source' in lease.role for lease in healthy.leases))
            self.assertTrue(reads)
            reads.clear()
            growth = True
            with self.assertRaises(scheduler.WorldError) as caught:
                with scheduler._lifecycle_operation_scope() as negative:
                    scheduler._LifecycleOrdinarySources(host, root, lambda: None)
            self.assertEqual(caught.exception.code, 'WINDOW_RECOVERY_UNPROVEN')
            self.assertTrue(negative.failed)
            self.assertTrue(negative.memory.failed)
            self.assertIs(negative.error, caught.exception)
            self.assertIsNotNone(negative.traceback)
            self.assertGreater(negative.memory.live, 0)
            self.assertTrue(reads)
            self.assertTrue(all(0 < width <= 65536 for width in reads))

    def test_geography_frontier_real_constant_typed_loan_and_same_owner_debt(self):
        # Real original producer shape, not the complete geography document.
        from app import world as world_module
        from app import coordinator_scheduler as scheduler
        raw_geo = json.loads((world_module.DATA / 'geography.json').read_bytes())
        regions = {region['id']: region for region in raw_geo['regions']}
        raw = world_module._json(regions)
        self.assertEqual(len(raw.encode('utf-8')), 330224)
        self.assertEqual(hashlib.sha256(raw.encode('utf-8')).hexdigest(),
            'cc5b59d3e183b01c6587eb65a1057f90f52944065d77e1d0457e096d4a22aab9')
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('CREATE TABLE world_constants (key TEXT, value TEXT, sha256 TEXT)')
            db.execute('INSERT INTO world_constants VALUES (?,?,?)',
                ('geography', raw, hashlib.sha256(raw.encode('utf-8')).hexdigest()))
            with scheduler._lifecycle_operation_scope() as owner:
                result = world_module._lc_geo_borrow(db, owner)
                self.assertEqual(result, regions)
                self.assertIs(type(result), dict)
                self.assertIs(type(result['place-0']['id']), str)
                loan = next(loan for loan in owner.loans if loan.value is result)
                self.assertIs(loan.owner, owner)
                self.assertIs(loan.memory, owner.memory)
                self.assertFalse(loan.closed)
                self.assertTrue(all(not lease.released for lease in loan.leases))
                self.assertLessEqual(owner.memory.peak, owner.memory.LIMIT)
                self.assertEqual(tuple(lease.charge for lease in owner.leases), (1536, 8704, 8704))
                self.assertEqual(sum(lease.charge for lease in loan.leases), 6477312)
                first_live = 1536 + 8704 + 8704 + 6477312
                self.assertEqual(owner.memory.live, first_live)
                with scheduler._lifecycle_operation_scope() as same:
                    self.assertIs(same, owner)
                    self.assertIs(loan.value, result)
                    self.assertEqual(owner.memory.live, first_live)
            self.assertIs(loan.value, result)
            self.assertFalse(loan.closed)
            self.assertEqual(owner.destination, 'external-caller; physical-reclamation-unknown')
            self.assertGreater(owner.memory.live, 0)

    def test_geography_hash_priority_plain_domain_and_exception_debt(self):
        from app import world as world_module
        from app import coordinator_scheduler as scheduler
        positives = ('{}', '[]', 'null', 'true', '42', '-0.0',
            '{"x":1,"x":2}', '{"nested":[1,2.5,null,false,"\\ud83d\\ude00"]}',
            '{"n":NaN,"p":Infinity,"m":-Infinity}', '  {"a":[]} \n')
        for raw in positives:
            with self.subTest(raw=raw), closing(sqlite3.connect(':memory:')) as db:
                db.execute('CREATE TABLE world_constants (key TEXT, value TEXT, sha256 TEXT)')
                db.execute('INSERT INTO world_constants VALUES (?,?,?)',
                    ('geography', raw, hashlib.sha256(raw.encode('utf-8')).hexdigest()))
                with scheduler._lifecycle_operation_scope() as owner:
                    actual = world_module._lc_geo_borrow(db, owner)
                    # dump comparison preserves numeric distinctions and NaN's
                    # original plain-load domain rather than NaN != itself.
                    self.assertEqual(json.dumps(actual, sort_keys=True), json.dumps(json.loads(raw), sort_keys=True))
        for raw, valid_hash in (('{broken', False), ('{broken', True), ('[1,]', True)):
            with self.subTest(raw=raw, valid_hash=valid_hash), closing(sqlite3.connect(':memory:')) as db:
                db.execute('CREATE TABLE world_constants (key TEXT, value TEXT, sha256 TEXT)')
                db.execute('INSERT INTO world_constants VALUES (?,?,?)', ('geography', raw,
                    hashlib.sha256(raw.encode('utf-8')).hexdigest() if valid_hash else '0'*64))
                expected = json.JSONDecodeError if valid_hash else world_module.WorldError
                with self.assertRaises(expected) as caught:
                    with scheduler._lifecycle_operation_scope() as owner:
                        world_module._lc_geo_borrow(db, owner)
                if not valid_hash:self.assertEqual(caught.exception.code, 'SOURCE_DRIFT')
                self.assertTrue(owner.failed)
                self.assertTrue(owner.memory.failed)
                self.assertIs(owner.error, caught.exception)
                self.assertIsNotNone(owner.traceback)
                debt = owner.memory.live
                self.assertGreater(debt, 0)
                with self.assertRaises(scheduler._LifecycleLiveLimit):
                    with scheduler._lifecycle_operation_scope(memory=owner.memory):
                        self.fail('poisoned debt cannot become a new allowance')
                self.assertEqual(owner.memory.live, debt)

    def test_geography_raw_hash_and_decoder_prepaid_before_first_metadata(self):
        from app import world as world_module
        from app import coordinator_scheduler as scheduler
        calls = []
        class Observed(sqlite3.Connection):
            def execute(self, sql, *args):
                calls.append(sql)
                return super().execute(sql, *args)
        with closing(sqlite3.connect(':memory:', factory=Observed)) as db:
            db.execute('CREATE TABLE world_constants (key TEXT, value TEXT, sha256 TEXT)')
            db.execute('INSERT INTO world_constants VALUES (?,?,?)',
                ('geography', '{}', hashlib.sha256(b'{}').hexdigest()))
            calls.clear()
            with self.assertRaises(scheduler._LifecycleLiveLimit) as caught:
                with scheduler._lifecycle_operation_scope() as owner:
                    owner.reserve(owner.memory.LIMIT - owner.memory.live - 2048, 'existing-real-operation-debt')
                    world_module._lc_geo_borrow(db, owner)
            self.assertEqual(calls, [], 'denial must precede metadata or TEXT fetch')
            self.assertIs(owner.error, caught.exception)
            self.assertTrue(owner.failed and owner.memory.failed)

    def test_geography_complete_restore_original_frozen_prefix_and_alias(self):
        # Full real initialized LC World and original _restore_geography; not a
        # replacement root, fake Row, patched frozen check, or fixture adapter.
        from app import world as world_module
        from app import coordinator_scheduler as scheduler
        deadline = time.monotonic() + 120
        root = SCRATCH / ('parent1128-geography-restore-' + uuid.uuid4().hex)
        root.mkdir(parents=True, exist_ok=False)
        store = world_module.WorldStore(root / 'world.sqlite', scenario_path=world_module.DATA / 'scenarios/lifecycle-v1-iron-4.json')
        store.initialize()
        self.assertLess(time.monotonic(), deadline)
        with store._connection(readonly=True) as db:
            db.execute('BEGIN')
            raw = db.execute('SELECT state FROM snapshots ORDER BY seq LIMIT 1').fetchone()[0]
            expected_raw = db.execute("SELECT value FROM world_constants WHERE key='geography'").fetchone()[0]
            self.assertEqual(len(expected_raw.encode('utf-8')), 330224)
            state = json.loads(raw)
            self.assertNotIn('_geography', state)
            with scheduler._lifecycle_operation_scope() as owner:
                world_module.WorldStore._restore_geography(db, state)
                self.assertEqual(state['_geography'], json.loads(expected_raw))
                loan = next(loan for loan in owner.loans if loan.value is state['_geography'])
                first = state['_geography']
                before = sum(loan.value is first for loan in owner.loans)
                world_module.WorldStore._restore_geography(db, state)
                self.assertIs(state['_geography'], first)
                self.assertEqual(sum(loan.value is first for loan in owner.loans), before)
                self.assertIs(loan.value, first)
                self.assertFalse(loan.closed)
                self.assertLess(time.monotonic(), deadline)
            self.assertIs(loan.value, state['_geography'])



    def test_geo1133_real_prepare_preview_commit_one_entry_and_ended_geo_frames(self):
        # Complete original product path, no fake Work, no patched business method.
        from app.tests.test_lifecycle_product1087 import LifecycleProductTests
        from app import world as wm, coordinator_scheduler as sc
        fixture = LifecycleProductTests(methodName='test_product_mortal_result_root_succession_empty_session_document_and_historical_reopen')
        events = []
        old_observer = fixture.owned_observer
        def observer(frame, event, arg):
            if frame.f_code is wm._lc_geo1133_stage.__code__:
                parent = frame.f_locals.get('parent')
                function = frame.f_locals.get('function')
                work = sc._lifecycle_scope.get()
                if parent is not None and function is not None and work is not None:
                    events.append((event, function.__name__, parent, parent.memory, work.entry,
                        parent.memory.live, parent.memory.peak,
                        tuple(loan for loan in parent.loans if any('geo:retained' in lease.role for lease in loan.leases))))
                return observer
            return old_observer(frame, event, arg)
        fixture.owned_observer = observer
        fixture.setUp()
        try:
            fixture.test_product_mortal_result_root_succession_empty_session_document_and_historical_reopen()
            returns = [row for row in events if row[0] == 'return']
            names = [row[1] for row in returns]
            required = ['_lc_geo1133_prepare_body', '_lc_geo1133_preview_lifecycle_body', '_lc_geo1133_commit_body']
            start = next(i for i, name in enumerate(names) if name == required[0])
            cohort = returns[start:start+3]
            self.assertEqual([row[1] for row in cohort], required)
            for row in cohort:
                self.assertIs(row[2], cohort[0][2])
                self.assertIs(row[3], cohort[0][3])
                self.assertIs(row[4], cohort[0][4])
                self.assertLessEqual(row[6], 16777216)
                self.assertEqual(row[7], (), 'only independently paid result loans escape ended stages')
            self.assertFalse(cohort[0][3].failed)
        finally:
            fixture.doCleanups()

    def test_geo1133_preview_reservation_prices_dynamic_not_existing_geo(self):
        from app import world as wm, coordinator_scheduler as sc
        regions = {r['id']: r for r in json.loads((wm.DATA / 'geography.json').read_bytes())['regions']}
        raw = wm._json(regions)
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('CREATE TABLE world_constants (key TEXT, value TEXT, sha256 TEXT)')
            db.execute('INSERT INTO world_constants VALUES (?,?,?)', ('geography', raw, hashlib.sha256(raw.encode()).hexdigest()))
            with sc._lifecycle_operation_scope() as owner:
                geo = wm._lc_geo_borrow(db, owner)
                state = {'_geography': geo, 'world_id': 'owned', 'revision': 1, '_private': {'keep': [1]}}
                before = owner.memory.live
                dynamic = wm.WorldStore._dynamic_state(state)
                charge, _, _ = sc._lifecycle_value_price(dynamic, owner.memory)
                wm._lc_geo1133_reserve_dynamic(state, 2, 'preview-direct-containers-and-public-result')
                self.assertEqual(owner.memory.live - before, 256 + 128*len(state) + 512 + 2*charge + 4096 + 512)
                self.assertIs(state['_geography'], geo)
                self.assertTrue(any(loan.value is geo and not loan.closed for loan in owner.loans))
                public = wm.WorldStore._public(state)
                self.assertNotIn('_geography', public)
                self.assertNotIn('_private', public)
                self.assertLessEqual(owner.memory.peak, 16777216)

    def test_geo1133_generic_mutate_preserves_unknown_alias_and_opaque_identity(self):
        from app import world as wm, coordinator_scheduler as sc
        deadline = time.monotonic() + 120
        for kind in ('state', 'subalias', 'opaque'):
            with self.subTest(kind=kind):
                root = SCRATCH / ('parent1133-alias-' + uuid.uuid4().hex)
                root.mkdir(parents=True, exist_ok=False)
                store = wm.WorldStore(root / 'world.sqlite')
                store.initialize()
                captured = []
                opaque = object()
                def callback(state, db):
                    captured.append(state)
                    return state if kind == 'state' else state['_geography'] if kind == 'subalias' else opaque
                with sc._lifecycle_operation_scope() as owner:
                    result = store._mutate(callback)
                    state = captured[0]
                    self.assertIs(result, state if kind == 'state' else state['_geography'] if kind == 'subalias' else opaque)
                    loan = next(loan for loan in owner.loans if loan.value is state['_geography'])
                    self.assertFalse(loan.closed)
                    self.assertIs(loan.owner, owner)
                    self.assertGreater(owner.memory.live, 0)
                    self.assertLess(time.monotonic(), deadline)

    def test_geo1133_stage_exception_retains_original_tb_and_all_geo_debt(self):
        from app import world as wm, coordinator_scheduler as sc
        marker = RuntimeError('parent1133 original primary')
        held = []
        with closing(sqlite3.connect(':memory:')) as db:
            db.execute('CREATE TABLE world_constants (key TEXT, value TEXT, sha256 TEXT)')
            raw = '{"region":{"children":[1,2,3]}}'
            db.execute('INSERT INTO world_constants VALUES (?,?,?)', ('geography', raw, hashlib.sha256(raw.encode()).hexdigest()))
            def producer():
                owner = sc._lifecycle_value_context.get()
                state = {'_geography': wm._lc_geo_borrow(db, owner)}
                held.append((owner, owner.memory.live, state))
                raise marker
            with self.assertRaises(RuntimeError) as caught:
                with sc._lifecycle_operation_scope() as parent:
                    wm._lc_geo1133_stage(producer, (), {})
            self.assertIs(caught.exception, marker)
            child, debt, state = held[0]
            self.assertTrue(child.failed and parent.failed and parent.memory.failed)
            self.assertIs(child.error, marker)
            self.assertIsNotNone(child.traceback)
            self.assertGreaterEqual(parent.memory.live, debt)
            tb = child.traceback
            while tb is not None and tb.tb_frame.f_code is not producer.__code__:
                tb = tb.tb_next
            self.assertIsNotNone(tb)
            self.assertIs(tb.tb_frame.f_locals['state'], state)
            self.assertFalse(any(lease.released for loan in child.loans for lease in loan.leases))


    def _geo1145_assert_edit_boundary(self, before, after, independent):
        owner, ledger, entry, parent, root, io, numbers = before
        current, current_ledger, current_entry, current_parent, current_root, current_io, current_numbers = after
        for old, new in zip((owner, ledger, entry, parent, root, io),
                            (current, current_ledger, current_entry, current_parent, current_root, current_io)):
            self.assertIs(new, old)
        self.assertEqual(current_numbers, numbers)
        edit_entry, edit_owner = independent
        self.assertIsNot(edit_entry, entry)
        self.assertIsNot(edit_entry, parent)
        self.assertIsNot(edit_owner, owner)
        self.assertIsNot(edit_owner.memory, ledger)
        self.assertIs(edit_entry._lifecycle_value_owner, edit_owner)
        self.assertIs(edit_entry.stop, entry.stop)
        self.assertEqual(edit_entry.captured, (root, entry))
        self.assertEqual((edit_entry.deadline, edit_entry.busy_deadline), (entry.deadline, entry.busy_deadline))
        self.assertEqual(len(edit_entry._accounts), len(entry._accounts))
        for used, original in zip(edit_entry._accounts, entry._accounts):
            self.assertIs(used, original)
        self.assertTrue(edit_owner.closed)
        self.assertFalse(edit_owner.failed or edit_owner.memory.failed)
        self.assertFalse(owner.closed or owner.failed or ledger.failed)

    def _geo1145_assert_persisted_success(self, job, rounds, native_result):
        self.assertEqual([a['status'] for a in job['attempts']], ['stale'] * rounds + ['committed'])
        self.assertEqual(native_result, [job['result']])
        attempt = job['attempts'][-1]
        for key in ('request_sha256', 'decision_sha256'):
            self.assertEqual(attempt[key], job['result'][key])

    def _geo1145_assert_first_resources(self, observed):
        self.assertEqual(observed['live'], [])
        self.assertIs(observed['host_pending'], False)
        self.assertIs(observed['coordinator_pending'], False)
        self.assertEqual(observed['lease_cleanups'], 0)
        self.assertIs(observed['lifecycle_active'], False)

    def _geo1145_assert_native_close(self, stage_closes, phase, rounds):
        self.assertEqual(stage_closes, [(phase, i, True) for i in range(rounds)],
                         'each real stale producer connection must already reject the native in_transaction descriptor')

    def _geo1151_rollback_sites(self, binding):
        # Shared source protocol: derive both LINE sites from the actual bound body.
        text = binding['raw'].decode('utf-8')
        node = binding['node']
        candidates = []
        for block in ast.walk(node):
            for child_field in ('body', 'orelse', 'finalbody'):
                suite = getattr(block, child_field, ())
                if not isinstance(suite, list):
                    continue
                for before, after in zip(suite, suite[1:]):
                    if (isinstance(before, ast.Try) and len(before.body) == 1
                            and ast.get_source_segment(text, before.body[0]) == 'db.rollback()'
                            and isinstance(after, ast.If)
                            and ast.get_source_segment(text, after.test) == 'db.in_transaction'):
                        candidates.append((before.body[0].lineno, after.lineno))
        self.assertEqual(len(candidates), 1)
        pre, post = candidates[0]
        self.assertLess(pre, post)
        return pre, post

    def _geo1151_replay(self, fixture, work, host_locals):
        from app import coordinator_scheduler as sc
        world, host, entry = work.world, work.host, work.entry
        f = host_locals
        a, q, carrier, io = f['adjudicator'], f['request'], f['carrier'], f['io']
        result, req_sha = f['result'], f['request_sha256']
        owner, root = entry._lifecycle_value_owner, work.root
        self.assertIs(a, host.adjudicator)
        self.assertIs(a, work.ordinary.adjudicator)
        self.assertIs(a.world, world)
        self.assertIs(f['gateway'], host.real_gateway)
        self.assertTrue(host._lock._is_owned())  # preserve original commit lock ancestry
        self.assertIs(f['parent'], entry.captured[1])
        def scope():
            self.assertIs(sc._lifecycle_scope.get(), work)
            self.assertIs(sc._lifecycle_value_context.get(), owner)
            self.assertIs(world._scoped_entry_budget(), entry)
            self.assertIs(world._scoped_coordinator_attempt(), root)
            self.assertIs(world._scoped_coordinator(), work.permit)
            self.assertIs(host._lifecycle_active[1], io)
            self.assertIs(io.entry, entry)
            self.assertFalse(owner.closed or owner.failed or owner.memory.failed)
            return (entry.deadline, entry.busy_deadline, root.deadline, io.deadline)
        bounds = scope()
        accounts, memory, policy, stop = entry._accounts, owner.memory, entry.policy, entry.stop
        contention = tuple((a.owner, a.limit, a.spent) for a in accounts)
        before, calls, controls = fixture.business(), len(fixture.calls), fixture.controls()
        job = next(j for j in json.loads(before['public']['snapshots'][-1]['state'])['_lifecycle']['jobs']
                   if j['id'] == work.job_id)
        last = job['attempts'][-1]
        self.assertEqual(last['status'], 'committed')
        self.assertEqual(last['request'], q)
        self.assertEqual(last['request_sha256'], req_sha)
        self.assertEqual(last['response_raw'], carrier['response_raw'])
        self.assertEqual(last['response'], carrier['response'])
        self.assertEqual(job['result'], result)
        completed = dict(status='completed', result=result)
        outcomes = (
            a.prepare_lifecycle(work.job_id, f['service_version']),
            a.preview_lifecycle(work.job_id, req_sha, carrier['response_raw'], carrier['response']),
            a.commit_lifecycle(work.job_id, req_sha, carrier['response_raw'], carrier['response'],
                               decision_sha256=last['decision_sha256']),
        )
        self.assertEqual(outcomes, (completed, completed, result))
        self.assertEqual(fixture.business(), before)
        fixture.unchanged_controls(controls)  # independently ticking root heartbeat is not a business effect
        self.assertEqual(len(fixture.calls), calls)
        self.assertEqual(scope(), bounds)
        for current, original in ((entry._accounts, accounts), (owner.memory, memory),
                                  (entry.policy, policy), (entry.stop, stop)):
            self.assertIs(current, original)
        for a, (thread, limit, spent) in zip(accounts, contention):
            self.assertIs(a.owner, thread)
            self.assertEqual(a.limit, limit)
            self.assertGreaterEqual(a.spent, spent)
        return (work.job_id, q['request_id'], req_sha, last['decision_sha256'], outcomes)

    def _geo1151_assert_negative(self, fixture, authority, baseline, job_id, request, fault):
        before, controls = baseline
        after = fixture.business()
        self.assertEqual(after['private'], before['private'])
        snapshots = before['public']['snapshots']
        previous = json.loads(snapshots[-1]['state'])
        current = json.loads(after['public']['snapshots'][-1]['state'])
        if fault == 'forged':
            # Coordinator's existing terminal technical pause is not a succession
            # effect. Require precisely that one appended snapshot, no other write.
            expected = dict(previous, revision=previous['revision'] + 1, status='technical_paused',
                            diagnostic=dict(code='STALE_PREVIEW', time=previous['time']))
            self.assertEqual(after['public']['snapshots'][:-1], snapshots)
            self.assertEqual(len(after['public']['snapshots']), len(snapshots) + 1)
            self.assertEqual(after['public']['snapshots'][-1]['seq'], expected['revision'])
            self.assertEqual(current, expected)
        else:
            self.assertEqual(after['public']['snapshots'], snapshots)
        self.assertEqual({k: v for k, v in after['public'].items() if k != 'snapshots'},
                         {k: v for k, v in before['public'].items() if k != 'snapshots'})
        job = next(j for j in current['_lifecycle']['jobs'] if j['id'] == job_id)
        self.assertEqual(len(job['attempts']), 1)
        self.assertEqual(job['attempts'][0]['status'], 'pending')
        self.assertEqual(job['attempts'][0]['request'], request)
        self.assertIsNone(job['result'])
        self.assertNotIn(request['proposed_person_id'], [p['id'] for p in current['persons']])
        self.assertEqual(fixture.session.world.receipt(authority.OLD, fixture.old_activation['intent_id']),
                         fixture.old_receipt)
        fixture.unchanged_controls(controls)

    def run(self, result=None):
        if result is not None and _STOPPED_FIRST_CLOSE_1163 is not None:
            result.stop()
            return result
        try:
            return super().run(result)
        finally:
            if result is not None and _STOPPED_FIRST_CLOSE_1163 is not None:
                result.stop()

    def _geo1145_first(self, fixture, whole, run):
        global _STOPPED_FIRST_CLOSE_1163
        if _STOPPED_FIRST_CLOSE_1163 is not None:
            self._geo1145_first_record = _STOPPED_FIRST_CLOSE_1163
            self.fail('STOP: prior first-close fixture guards retained')
        def first(error):
            return (error, BaseException.__traceback__.__get__(error),
                    BaseException.__context__.__get__(error), BaseException.__cause__.__get__(error))
        record = dict(primary=None, close=None, secondary=[], physical=None, scopes_restored=False)
        self._geo1145_first_record = record

        def secondary(label, operation):
            try:
                return operation()
            except BaseException as error:
                record['secondary'].append((label, first(error)))
                return None

        def step(at):
            self.assertIsNone(fixture.trace_error)
            code = fixture.start_join()
            if code is not None:
                raise RuntimeError(code)
            state = fixture.session.world.snapshot()
            fixture.budget()
            fixture.assertEqual((state['time'], state['status']), (at, 'user_paused'))
            fixture.assertIsNone(fixture.session.coordinator.error)
            fixture.assertFalse(fixture.session.coordinator.worker.is_alive())
            fixture.assertTrue(fixture.rows('coordinator_windows'))
            fixture.assertEqual(fixture.rows('coordinator_windows')[-1]['state'], 'completed')
            return state

        owned = record['owned'] = dict(session=None, coordinator=None, root=None, hosts=[],
                                       threads=[], construction_closes=[])

        def capture():
            c = owned['coordinator']
            hosts = tuple(owned['hosts'])
            attempts = list(fixture.attempts)
            for host in hosts:
                attempt = getattr(host, '_attempt', None)
                if attempt is not None:
                    attempts.append(attempt)
                attempts.extend(row[2] for row in getattr(host, '_lease_cleanups', ()))
            root = owned['root']
            present = [getattr(c, 'worker', None), getattr(root, 'thread', None), fixture.transport.root_thread]
            for host in hosts:
                present.extend(getattr(host, '_pending_threads', ()))
                present.extend(getattr(host, '_readers', ()))
            for attempt in attempts:
                present.extend([attempt.thread, *attempt.calls])
            threads = owned['threads']
            for thread in present:
                if thread is not None and not any(thread is old for old in threads):
                    threads.append(thread)
            return attempts

        def physical_state():
            capture()
            c, root, hosts = owned['coordinator'], owned['root'], owned['hosts']
            live = [t for t in owned['threads'] if t.is_alive()]
            return dict(live=[t.name for t in live],
                        live_other=[t.name for t in live if t is not getattr(root, 'thread', None)],
                        lifecycle_active=any(getattr(h, '_lifecycle_active', None) is not None for h in hosts),
                        actor_active=any(getattr(h, '_attempt', None) is not None for h in hosts),
                        process_present=any(getattr(h, '_process', None) is not None for h in hosts),
                        busy=getattr(c, 'busy', False))

        def quiet(status, *, allow_heartbeat=False):
            return status is not None and not (status['live_other' if allow_heartbeat else 'live']
                    or status['lifecycle_active'] or status['actor_active']
                    or status['process_present'] or status['busy'])

        def stop_wake_join(label):
            fixture.close_phase = 'physical_only'
            events = fixture.close_events
            start, mark = len(events), len(record['secondary'])
            c, root = owned['coordinator'], owned['root']
            attempts = secondary(label + '/retain before stop', capture)
            # Only known Events: no gateway.close, Attempt.cancel, finish, CAS,
            # or acquisition of a lock that an unjoined thread might own.
            signals = [getattr(c, '_closed', None), getattr(c, 'stop', None), getattr(root, 'stop', None),
                       *(getattr(h, '_closing', None) for h in owned['hosts']),
                       *(a.stop for a in (attempts or ())), fixture.transport.cancelled]
            for signal in signals:
                if signal is not None:
                    secondary(label + '/stop or wake', signal.set)
            deadline = min(whole, time.monotonic() + 5)
            seen = []
            for _ in range(2):
                secondary(label + '/retain threads', capture)
                for t in tuple(owned['threads']):
                    if any(t is old for old in seen):
                        continue
                    if t.ident is not None or t.is_alive():
                        seen.append(t)
                        secondary(label + '/join', lambda t=t: t.join(max(0, deadline - time.monotonic())))
            after = secondary(label + '/recheck', physical_state)
            return dict(after=after, events_start=start, events_end=len(events),
                        events=tuple(dict(e) for e in events[start:]),
                        secondary_range=(mark, len(record['secondary'])))

        def close_owned(session):
            observed = fixture.close_events
            close = dict(status='not_called', events_start=len(observed))
            record['close'] = fixture.first_close_record = close
            physical = record['physical'] = dict(before=secondary('physical/read before close', physical_state),
                                                  preclose=None, first=None, fallback=None)
            if session is None:
                # ProductSession.__init__ already owned its own first cleanup.
                # The envelope observes it but must never replay it or issue a CAS.
                close.update(reason='session_unpublished', events_start=0, events_end=len(observed),
                             events=tuple(dict(e) for e in observed))
                physical['fallback'] = stop_wake_join('physical/unpublished')
                return False  # Physical quietness does not prove constructor closure/ownership.

            def deny(reason):
                close.update(reason=reason, events_end=len(observed),
                             events=tuple(dict(e) for e in observed))
                return False

            ceiling = min(whole, fixture.deadline)
            if not time.monotonic() < ceiling:
                deny('whole_exhausted')
                physical['fallback'] = stop_wake_join('physical/expired before close')
                return False

            # Non-quiet lanes still use the same owned physical backstop.
            status = physical['before']
            if not quiet(status, allow_heartbeat=True):
                physical['preclose'] = stop_wake_join('physical/preclose')
                status = physical['preclose']['after']
            if not quiet(status, allow_heartbeat=physical['preclose'] is None):
                return deny('owned_work_not_quiescent')

            def admitted_close():
                from unittest.mock import patch
                from app import world as world_module
                from app.actor_execution import PreparationBudget
                from app.real_provider import RealGateway
                from app.siwc_responses import PublicHTTP

                c, h, world, root = session.coordinator, session.host, session.world, owned['root']
                if not (session is owned['session'] and c is owned['coordinator']
                        and root is not None and c._owner_attempt is root
                        and any(h is old for old in owned['hosts']) and c.actors is h
                        and c._authority is world and root.world is world and h.world is world
                        and h.execution_fenced is True):
                    return deny('owned_close_binding_unproven')
                gateway = h.real_gateway
                if (type(gateway) is not RealGateway or type(gateway.responses) is not PublicHTTP
                        or gateway.transport is not fixture.transport):
                    return deny('owned_transport_binding_unproven')
                http = gateway.responses

                # Stop and join the exact one-shot heartbeat before the original
                # finish (which keeps its fixed 2.5s join on this now-dead Thread).
                thread = root.thread
                if type(thread) is not threading.Thread or thread.ident is None or not any(
                        thread is old for old in owned['threads']):
                    return deny('root_thread_binding_unproven')
                if not time.monotonic() < ceiling:
                    return deny('whole_exhausted')
                root.stop.set()
                if thread.is_alive():
                    thread.join(max(0, min(5, ceiling - time.monotonic())))
                if thread.is_alive():
                    return deny('root_heartbeat_not_quiescent')

                acquired = []
                try:
                    # Original reentrant lock objects, all acquired without a wait;
                    # no queue behind an unknown holder, including both gateway locks.
                    for lock in (h._lock, c._owner_lock, h._worker_lock, gateway.lock, http.lock):
                        if not lock.acquire(blocking=False):
                            return deny('close_lock_unavailable')
                        acquired.append(lock)
                    if not time.monotonic() < ceiling:
                        return deny('whole_exhausted')
                    # An actor retirement obligation can take a different issuer or
                    # an independent tombstone reader; that is not this proven lane.
                    if (not quiet(physical_state()) or root.thread is not thread
                            or h._lease_cleanups or http.active
                            or gateway._responses_transport is not http
                            or gateway.responses is not http or gateway.transport is not fixture.transport
                            or world._scoped_entry_budget() is not None):
                        return deny('close_io_route_unproven')

                    class CloseCeiling:
                        deadline = min(ceiling, root.deadline)

                        def check(self):
                            if not time.monotonic() < self.deadline:
                                raise TimeoutError('1168 original first-close IO ceiling exhausted')

                    bound = CloseCeiling()
                    # Actual WorldStore._execution_connection selects _AttemptConnection
                    # from this IO-only scope; do not inherit the deliberately stopped
                    # root as a cancellation parent or change its authority/deadline.
                    io = PreparationBudget(c.execution_policy, threading.Event(), captured=(bound,))
                    # Only this fixture's proven close lane gets a connect gate.
                    # Keep the original WorldStore generator, options and native call.
                    sqlite = world_module.sqlite3
                    connect = sqlite.connect
                    connection_body = world_module.WorldStore._execution_connection.__wrapped__
                    close_thread = threading.current_thread()

                    class CloseSQLite:
                        def __getattr__(self, name):
                            return getattr(sqlite, name)

                        def connect(self, *args, **kwargs):
                            caller = sys._getframe(1)
                            if (threading.current_thread() is close_thread
                                    and caller.f_code is connection_body.__code__
                                    and caller.f_globals is vars(world_module)
                                    and caller.f_locals.get('self') is world
                                    and caller.f_locals.get('entry') is io):
                                # Sample after argument preparation, before forwarding
                                # the original fixed 1 ms wait. No new deadline or retry.
                                if not time.monotonic() < io.deadline - .001:
                                    raise TimeoutError('1173 original first-close SQLite connect headroom exhausted')
                            return connect(*args, **kwargs)

                    with world._entry_budget(io), patch.object(world_module, 'sqlite3', CloseSQLite()):
                        # The original scoped connect has a fixed .001s setting;
                        # admit only with headroom, then SQL/commit uses native_call.
                        if not time.monotonic() < io.deadline - .001:
                            return deny('whole_exhausted')
                        fixture.close_phase = 'first_close'
                        close.update(status='entered', events_start=len(observed))
                        try:
                            close['return'] = session.close()
                            close['status'] = 'returned'
                        except BaseException as error:
                            close.update(status='raised', error=first(error))
                            record['secondary'].append(('session.close', close['error']))
                        close['events_end'] = len(observed)
                        close['events'] = tuple(dict(e) for e in observed[:close['events_end']])
                    return True
                finally:
                    for lock in reversed(acquired):
                        lock.release()

            close['scope_gate'] = secondary('bounded first close scope', admitted_close)
            if close['status'] == 'not_called':
                if 'reason' not in close:
                    deny('close_admission_unknown')
                physical['fallback'] = stop_wake_join('physical/first-close denial')
                return False
            close['resources'] = secondary('resources/read', fixture.resource_record)
            close['resource_gate'] = secondary('resources/check', lambda: (
                self._geo1145_assert_first_resources(close['resources']), True)[1])
            close['cas_gate'] = secondary('original actor/root CAS', lambda: (
                fixture.assert_close_oracle(close['events_end']), True)[1])
            close['return_gate'] = secondary('session.close result', lambda: (
                self.assertIs(close.get('return', close), None), True)[1])
            physical['first'] = secondary('physical/read after close', physical_state)
            close['physical_gate'] = secondary('first physical quiescence', lambda: (
                self.assertTrue(quiet(physical['first'])), True)[1])
            # This is a separate STOP gate, never the proof of bounded admission.
            close['deadline_gate'] = time.monotonic() < ceiling
            if not (close['scope_gate'] and close['deadline_gate'] and close['physical_gate']
                    and close['resource_gate'] and close['cas_gate'] and close['return_gate']):
                physical['fallback'] = stop_wake_join('physical/postclose')
                return False  # No replacement of the first result by later physical convergence.
            return True

        build_entered = False
        try:
            fixture.setUp()
            fixture.deadline = min(whole, fixture.deadline)
            fixture.install_close_observer(owned=owned)
            build_entered = True
            fixture.build()
            fixture.original_root.deadline = min(fixture.original_root.deadline, whole)
            fixture.step = step
            fixture.injury()
            run()
        except BaseException as error:
            record['primary'] = first(error)
            raise
        finally:
            session = getattr(fixture, 'session', None)
            safe = not build_entered or secondary('first close envelope', lambda: close_owned(session)) is True
            if safe:
                def clean_scopes():
                    self.assertIs(fixture.doCleanups(), True)
                    record['scopes_restored'] = True
                secondary('scope restoration', clean_scopes)
            if not record['scopes_restored']:
                # Keep the owned guard/observer stack installed; this is a terminal
                # isolation failure, not permission to continue with another case.
                record['scopes_retained'] = fixture
                record['stop'] = 'STOP: owned first-close isolation not proven'
                _STOPPED_FIRST_CLOSE_1163 = record
                secondary('physical/quiescence', lambda: self.fail('owned resource quiescence not proven; guards retained'))
            if record['primary'] is None and record['secondary']:
                error, tb, _, _ = record['secondary'][0][1]
                raise BaseException.with_traceback(error, tb)

    def _geo1145_edit(self, world, work, parent, io, person, mission):
        from contextvars import Context
        from app import coordinator_scheduler as sc
        from app.actor_execution import PreparationBudget

        entry, root = work.entry, work.root
        owner = entry._lifecycle_value_owner

        def job():
            current_entry = world._scoped_entry_budget()
            current = sc._lifecycle_value_context.get()
            self.assertIs(current_entry, entry)
            self.assertIs(current, owner)
            self.assertIs(current_entry._lifecycle_value_owner, current)
            self.assertIs(parent._lifecycle_value_owner, current)
            self.assertIs(world._scoped_coordinator_attempt(), root)
            self.assertIs(world._scoped_coordinator(), work.permit)
            self.assertIs(host_io(), io)
            return (current, current.memory, current_entry, parent, root, host_io(),
                    (entry.deadline, entry.busy_deadline, parent.deadline, parent.busy_deadline,
                     root.deadline, current.memory.live, current.memory.peak, len(current.loans),
                     len(current.control_errors), current.closed, current.failed, current.memory.failed))

        def host_io():
            return work.host._lifecycle_active[1]

        def producer():
            # Same worker (PreparationBudget accounts are thread-owned). Capture
            # the original job's bounds/accounts, but never enter either bound Entry.
            self.assertIsNone(sc._lifecycle_scope.get())
            edit = PreparationBudget(entry.policy, entry.stop, (root, entry))
            self.assertIsNone(getattr(edit, '_lifecycle_value_owner', None))
            with world._coordinator_scope(work.permit, root), world._entry_budget(edit):
                separate = sc._lifecycle_value_context.get()
                self.assertIsNot(separate, owner)
                self.assertIsNot(separate.memory, owner.memory)
                world.edit_card(person, {'mission': mission}, coordinator=work.permit)
                edit.check()
            return edit, separate

        before = job()
        independent = Context().run(producer)
        self._geo1145_assert_edit_boundary(before, job(), independent)

    def _geo1145_actual_closed(self, db, entry):
        from app import world as wm
        self.assertIs(type(db), wm._AttemptConnection)
        self.assertIs(db._attempt, entry)
        # Native data descriptor on the exact already-returning connection;
        # no second close, SQL probe, trace event fabrication or DB retained.
        try:
            sqlite3.Connection.in_transaction.__get__(db)
        except sqlite3.ProgrammingError as error:
            return error.args == ('Cannot operate on a closed database.',)
        return False

    def _geo1141_real_host_stale(self, phase, rounds, *, fault=None):
        # Future reviewed native execution only; no product method is replaced.
        import threading
        from app import actors, coordinator_scheduler as sc, world as wm
        from app.tests import test_lifecycle_authority1100 as authority

        fixture = authority.LifecycleAuthorityProductTests(methodName='runTest')
        whole = time.monotonic() + 120
        def run():
            world, host = fixture.session.world, fixture.session.host
            host_b = fixture.source_binding(actors.ActorHost, 'resolve_lifecycle_jobs')
            mut_b = fixture.source_binding(wm.WorldStore, '_lc_geo1133_mutate_body')
            prv_b = fixture.source_binding(wm.WorldStore, '_lc_geo1133_preview_lifecycle_body')
            com_b = fixture.source_binding(wm.WorldStore, '_lc_geo1133_commit_body')
            digest_b = fixture.source_binding(wm.WorldStore, '_lifecycle_bound_attempt') if fault == 'digest' else None
            sites = {}
            for label, prefix in (('preview', 'preview = adjudicator.preview_lifecycle('),
                                  ('commit', 'result = adjudicator.commit_lifecycle('),
                                  ('replay', 'results.append(result)')):
                hits = [n for n in ast.walk(host_b['node']) if isinstance(n, (ast.Assign, ast.Expr))
                        and ast.get_source_segment(host_b['raw'].decode('utf8'), n).startswith(prefix)]
                self.assertEqual(len(hits), 1)
                sites[label] = hits[0].lineno
            rollback_pre, rollback_post = self._geo1151_rollback_sites(mut_b)
            authority_trace = threading.gettrace()
            edits, phases, host_errors, catches, final_errors = [], [], [], [], []
            negative, replays, rollbacks = [], [], []
            pending_rollback = None
            stage_closes, native_results = [], []
            fake = wm.WorldError('STALE_PREVIEW', '继任裁定基准已变化') if fault == 'forged' else None

            def remember_negative(work, request, business):
                negative.append(((business, fixture.controls()), work.job_id, request))

            def observe_body(frame, event, arg):
                nonlocal pending_rollback
                code = frame.f_code
                if code is host_b['code'] and frame.f_locals.get('self') is host:
                    self.assertIs(frame.f_globals, host_b['namespace'])
                    f = frame.f_locals
                    work = sc._lifecycle_scope.get()
                    if event == 'return' and work is None and edits and type(arg) is list and arg:
                        native_results.append(arg)
                    if work is not None and work.host is host and 'retry' in f:
                        self.assertIs(work.caller, frame)
                        owner = work.entry._lifecycle_value_owner
                        retry = f['retry']
                        if event == 'line' and frame.f_lineno == sites['replay'] and edits and fault is None:
                            fixture.check_source_binding(host_b)
                            self.assertEqual(retry, rounds)
                            self.assertFalse(replays)
                            # Trace suppresses nested callbacks while handling this line.
                            # Direct returned values and independent reads are the oracle.
                            replays.append(self._geo1151_replay(fixture, work, f))
                        if event == 'line' and frame.f_lineno == sites[phase] and retry < rounds:
                            fixture.check_source_binding(host_b)
                            request = f['request']
                            if fault == 'forged':
                                business = fixture.business()
                                remember_negative(work, request, business)
                                edits.append((retry, work, owner, work.entry, business, None, request))
                                raise fake  # deliberate negative only, not a stale producer
                            live = owner.memory.live
                            mission = '真实前向修订-%d' % retry
                            self._geo1145_edit(world, work, f['parent'], f['io'], authority.KING, mission)
                            self.assertIs(sc._lifecycle_value_context.get(), owner)
                            self.assertEqual(owner.memory.live, live)
                            raw = fixture.raw()
                            card = next(p for p in raw['persons'] if p['id'] == authority.KING)
                            self.assertEqual((card['card_author'], card['card']['mission']), ('user', mission))
                            self.assertEqual(raw['revision'], request['base_revision'] + 1)
                            job = next(j for j in raw['_lifecycle']['jobs'] if j['id'] == work.job_id)
                            self.assertEqual((raw['status'], job['result'], job['attempts'][-1]['status']), ('running', None, 'pending'))
                            self.assertEqual(job['attempts'][-1]['request']['request_id'], request['request_id'])
                            self.assertGreater(raw['revision'], request['base_revision'])
                            business = fixture.business()
                            edits.append((retry, work, owner, work.entry, business, live, request))
                            if fault == 'digest':
                                remember_negative(work, request, business)
                                # Same real stale durable base, wrong request SHA.
                                carrier = f['carrier']
                                try:
                                    world.preview_lifecycle(work.job_id, '0' * 64, carrier['response_raw'], carrier['response'])
                                except wm.WorldError as error:
                                    catches.append((error, owner, owner.traceback, owner.error_context, owner.error_cause,
                                                    BaseException.__traceback__.__get__(error)))
                                    raise
                                self.fail('wrong request digest returned from real stale preview')
                        if event == 'exception':
                            error = arg[1]
                            if type(error) is wm.WorldError:
                                host_errors.append((retry, error, arg[2], work, owner, work.db is None and work.state is None,
                                    owner.memory.live, tuple(owner.control_errors)))
                                if error.code in ('STALE_PREVIEW', 'ACTOR_RECOVERY_UNPROVEN') and edits and edits[-1][4] is not None:
                                    final_errors.append(fixture.business())
                    return observe
                matches = [(b, name) for b, name in ((mut_b, 'mutate'), (prv_b, 'preview'), (com_b, 'commit')) if code is b['code']]
                if matches:
                    b, name = matches[0]
                    self.assertIs(frame.f_globals, b['namespace'])
                    work = sc._lifecycle_scope.get()
                    if work is None or work.host is not host:
                        return None
                    f = frame.f_locals
                    if f.get('self') is not world or (name == 'mutate' and f.get('_precommit') is not work):
                        return None
                    if name == 'mutate' and event == 'line' and frame.f_lineno in (rollback_pre, rollback_post):
                        # Ordinary prepare/commit and post-close scope exits never read db.
                        if frame.f_lineno == rollback_pre:
                            fixture.check_source_binding(b)
                        control = f['result']
                        self.assertIs(type(control), sc._lc_stale1141_Control)
                        self.assertIs(work.stale1141, control)
                        self.assertEqual(control.phase, 'commit')
                        self.assertEqual(control.retry, work.caller.f_locals['retry'])
                        self.assertIsNone(control.first)
                        self.assertFalse(control.ready or control.accepted)
                        self.assertIs(frame.f_back.f_code, com_b['code'])
                        self.assertIs(frame.f_back.f_globals, com_b['namespace'])
                        db = f['db']
                        self.assertIs(type(db), wm._AttemptConnection)
                        self.assertIs(db._attempt, work.entry)
                        self.assertIs(work.db, db)
                        self.assertIs(work.state, f['state'])
                        if frame.f_lineno == rollback_pre:
                            self.assertIsNone(pending_rollback)
                            pending_rollback = (id(frame), db, control, sqlite3.Connection.in_transaction.__get__(db))
                        else:
                            before = pending_rollback
                            pending_rollback = None
                            self.assertIsNotNone(before)
                            self.assertEqual(before[0], id(frame))
                            self.assertIs(before[1], db)
                            self.assertIs(before[2], control)
                            rollbacks.append((control.retry, id(frame), id(db), before[3],
                                              sqlite3.Connection.in_transaction.__get__(db)))
                    if name == 'mutate' and event in ('exception', 'return') and pending_rollback is not None:
                        if pending_rollback[0] == id(frame):
                            pending_rollback = None
                    if event == 'return':
                        target = 'preview' if phase == 'preview' else 'mutate'
                        if name == target and type(arg) is sc._lc_stale1141_Control:
                            self.assertIs(work.stale1141, arg)
                            self.assertEqual(arg.phase, phase)
                            self.assertEqual(arg.retry, work.caller.f_locals['retry'])
                            self.assertIsNone(arg.first)
                            self.assertFalse(arg.ready or arg.accepted)
                            self.assertIsNone(work.db)
                            self.assertIsNone(work.state)
                            db = f['db']
                            if name == 'mutate':
                                self.assertTrue(rollbacks)
                                self.assertEqual(rollbacks[-1][:3], (arg.retry, id(frame), id(db)))
                            else:
                                fixture.check_source_binding(b)
                            stage_closes.append((phase, arg.retry, self._geo1145_actual_closed(db, work.entry)))
                        child = sc._lifecycle_value_context.get()
                        phases.append((name, type(arg).__name__, work.db is None and work.state is None,
                            child.memory.live, child.memory.peak, len(child.loans)))
                    return observe
                return authority_trace(frame, event, arg)

            def observe(frame, event, arg):
                try:
                    return observe_body(frame, event, arg)
                except BaseException as error:
                    if error is not fake and not (fault == 'digest' and catches and error is catches[-1][0]
                            and error.code == 'ACTOR_RECOVERY_UNPROVEN' and '请求摘要不符' in error.args):
                        if fixture.trace_error is None:
                            fixture.trace_error = error
                    raise

            # Keep the original guard stack and trace restoration behind quiescence.
            fixture.addCleanup(threading.settrace, authority_trace)
            threading.settrace(observe)
            if fault is None and rounds < 3:
                fixture.step(61)
            else:
                expected = ('STALE_PREVIEW' if fault == 'forged' else
                            'ACTOR_RECOVERY_UNPROVEN' if fault == 'digest' else 'ADJUDICATION_FAILED')
                self.assertEqual(fixture.start_join(), expected)
            self.assertEqual(len(edits), rounds)
            self.assertEqual([e[0] for e in edits], list(range(rounds)))
            work, owner, entry = edits[0][1:4]
            for _, w, o, e, _, _, _ in edits:
                self.assertIs(w, work)
                self.assertIs(o, owner)
                self.assertIs(e, entry)
                self.assertIs(o.memory, owner.memory)
            self.assertIs(work.root, fixture.original_root)
            self.assertEqual([c['kind'] for c in fixture.calls].count('succession'), rounds + int(fault is None and rounds < 3))
            self.assertLessEqual(owner.memory.peak, 16777216)
            if fault is not None:
                self.assertTrue(owner.failed and owner.memory.failed)
                self.assertFalse(any(isinstance(x, sc._lc_stale1141_Control) for x in owner.control_errors))
                self.assertIsNotNone(owner.traceback)
                self.assertEqual((replays, rollbacks, stage_closes, native_results), ([], [], [], []))
                if fault == 'forged':
                    self.assertIs(owner.error, fake)
                    tb = owner.traceback
                    while tb is not None and tb.tb_frame.f_code is not observe_body.__code__:
                        tb = tb.tb_next
                    self.assertIsNotNone(tb, 'must retain the exact deliberate Host-site injection')
                    self.assertIsNone(tb.tb_next)
                else:
                    self.assertEqual(len(catches), 1)
                    error, o, tb, context, cause, propagated = catches[0]
                    self.assertIs(o, owner)
                    self.assertIs(owner.error, error)
                    self.assertIs(owner.traceback, tb)
                    self.assertIs(owner.error_context, context)
                    self.assertIs(owner.error_cause, cause)
                    self.assertEqual(error.code, 'ACTOR_RECOVERY_UNPROVEN')
                    self.assertIn('请求摘要不符', error.args)
                    fixture.check_source_binding(digest_b)
                    digest_lines = [n.lineno for n in ast.walk(digest_b['node']) if isinstance(n, ast.If)
                                    and '请求摘要不符' in ast.get_source_segment(digest_b['raw'].decode('utf8'), n)]
                    self.assertEqual(len(digest_lines), 1)
                    leaves = []
                    while propagated is not None:
                        where = propagated.tb_frame
                        if where.f_code is digest_b['code']:
                            self.assertIs(where.f_globals, digest_b['namespace'])
                            self.assertIs(where.f_locals['self'], world)
                            self.assertEqual(where.f_locals['job_id'], work.job_id)
                            self.assertEqual(where.f_locals['request_sha256'], '0' * 64)
                            leaves.append(propagated.tb_lineno)
                        propagated = propagated.tb_next
                    self.assertEqual(leaves, digest_lines)
                self.assertEqual(len(negative), 1)
                self._geo1151_assert_negative(fixture, authority, *negative[0], fault)
                return
            stale = [o for o in host_errors if o[1].code == 'STALE_PREVIEW']
            self.assertEqual(len(stale), rounds)
            self.assertEqual(final_errors[:rounds], [e[4] for e in edits], 'no snapshot/effect/private publication during stale stage')
            self.assertEqual([x[1] for x in stale], [x.first[0] for x in owner.control_errors if type(x) is sc._lc_stale1141_Control])
            banned = {b['code'] for b in (mut_b, prv_b, com_b)} | {wm.WorldStore._lifecycle_bound_attempt.__code__}
            for index, error, tb, w, o, detached, live, records in stale:
                self.assertIs(w, work)
                self.assertIs(o, owner)
                self.assertTrue(detached)
                self.assertLess(live - edits[index][5], 6477312, 'no full old Geography remains charged')
                control = records[-1]
                original, first_tb, context, cause = control.first
                self.assertIs(original, error)
                self.assertEqual(control.phase, phase)
                self.assertEqual(control.retry, index)
                self.assertIsNone(context)
                self.assertIsNone(cause)
                while tb is not first_tb:
                    self.assertIsNotNone(tb)
                    self.assertNotIn(tb.tb_frame.f_code, banned)
                    tb = tb.tb_next
                self.assertIs(first_tb.tb_frame.f_code, sc._lc_stale1141_raise.__code__)
                self.assertIsNone(first_tb.tb_next)
            self.assertFalse(any(any('geo:retained' in lease.role for lease in loan.leases) for loan in owner.loans)
                if rounds < 3 else False)
            ended = [p for p in phases if p[0] == ('preview' if phase == 'preview' else 'mutate') and p[1] == '_lc_stale1141_Control']
            self.assertEqual(len(ended), rounds)
            self._geo1145_assert_native_close(stage_closes, phase, rounds)
            self.assertTrue(all(p[2] and p[-1] > 0 for p in ended))
            self.assertIsNone(pending_rollback)
            self.assertEqual([(i, before, after) for i, _, _, before, after in rollbacks],
                             [(i, True, False) for i in range(rounds)] if phase == 'commit' else [])
            state = fixture.raw()
            job = next(j for j in state['_lifecycle']['jobs'] if j['id'] == work.job_id)
            self.assertEqual(len({e[-1]['request_id'] for e in edits}), rounds)
            if rounds < 3:
                self.assertFalse(owner.failed or owner.memory.failed)
                self._geo1145_assert_persisted_success(job, rounds, native_results[0] if len(native_results) == 1 else None)
                attempt = job['attempts'][-1]
                self.assertNotIn(attempt['request_id'], {e[-1]['request_id'] for e in edits})
                self.assertEqual(len(replays), 1)
                self.assertEqual(replays[0][:4], (job['id'], attempt['request_id'], attempt['request_sha256'], attempt['decision_sha256']))
                self.assertEqual(replays[0][4][2], native_results[0][0])
                self.assertEqual(job['result']['status'], 'appointed')
                self.assertEqual(sum(p['id'] == job['result']['person_id'] for p in state['persons']), 1)
                self.assertEqual(job['result']['person_id'], edits[0][-1]['proposed_person_id'])
            else:
                self.assertEqual(replays, [])
                exhausted = [o[1] for o in host_errors if o[1].code == 'ADJUDICATION_RETRY_EXHAUSTED']
                self.assertEqual(len(exhausted), 1)
                self.assertIs(exhausted[0].__context__, stale[-1][1])
                self.assertEqual([a['status'] for a in job['attempts']], ['stale', 'stale', 'failed'])
                self.assertEqual(job['attempts'][-1]['failure_code'], 'ADJUDICATION_RETRY_EXHAUSTED')
                self.assertEqual(state['status'], 'technical_paused')
                self.assertIsNone(job['result'])
                self.assertNotIn(edits[0][-1]['proposed_person_id'], [p['id'] for p in state['persons']])
            self.assertEqual(world.receipt(authority.OLD, fixture.old_activation['intent_id']), fixture.old_receipt)

        self._geo1145_first(fixture, whole, run)

    def test_geo1141_real_preview_stale_once(self):
        self._geo1141_real_host_stale('preview', 1)

    def test_geo1141_real_preview_stale_twice(self):
        self._geo1141_real_host_stale('preview', 2)

    def test_geo1141_real_commit_stale_once(self):
        self._geo1141_real_host_stale('commit', 1)

    def test_geo1141_real_commit_stale_twice(self):
        self._geo1141_real_host_stale('commit', 2)

    def test_geo1141_real_preview_third_stale_exhausts(self):
        self._geo1141_real_host_stale('preview', 3)

    def test_geo1141_real_commit_third_stale_exhausts(self):
        self._geo1141_real_host_stale('commit', 3)

    def test_geo1141_unissued_result_object_is_not_control(self):
        from app import coordinator_scheduler as sc, world as wm
        fake = sc._lc_stale1141_Control()
        failure = None
        try:
            with sc._lifecycle_operation_scope() as owner:
                wm._lc_geo1133_stage(lambda: fake, (), {})
        except wm.WorldError as error:
            failure = error
        self.assertIsNotNone(failure)
        self.assertEqual(failure.code, 'ACTOR_RECOVERY_UNPROVEN')
        self.assertIs(owner.error, failure)
        self.assertIsNotNone(owner.traceback)
        self.assertIs(owner.traceback.tb_next.tb_frame.f_code, wm._lc_geo1133_copied_result.__code__)
        self.assertTrue(owner.failed and owner.memory.failed)
        self.assertEqual(owner.control_errors, [])

    def test_geo1141_forged_same_code_is_not_retry_control(self):
        self._geo1141_real_host_stale('preview', 1, fault='forged')

    def test_geo1141_digest_error_precedes_real_stale_predicate(self):
        self._geo1141_real_host_stale('preview', 1, fault='digest')


if __name__ == '__main__':
    unittest.main()
