"""Stdlib-only loaded-generation witnesses for trusted maintenance.

Registration samples disk at module completion AND compares the actually running
module code object with compilation of those exact bytes. This detects the
load/sample gap (including a stale pyc) instead of claiming a hash is the loader.
Maintenance rechecks original dictionaries/objects and current bytes; sys.modules
names are not provenance. The boundary trusts Python/compiler, this helper and
local publisher code. It is not protection against arbitrary memory/DB admins,
custom native loaders, monkeypatching this registry, or a compromised interpreter.
Registration is diagnostic only; ordinary execution/pause policy is unchanged.
"""
import hashlib
import inspect
from pathlib import Path
import sys
import types

FILES=frozenset(('actors.py','actor_execution.py','coordinator_scheduler.py','server.py','world.py','execution.py','coordinator_execution.py','coordinator_maintenance.py','source_attestation.py'))
_REGISTRY={}

class SourceMismatch(ValueError):
    pass

def _namespace(obj):
    if isinstance(obj,types.ModuleType):return vars(obj)
    if not isinstance(obj,type) and not isinstance(obj,types.FunctionType):obj=type(obj)
    if isinstance(obj,types.FunctionType):return obj.__globals__
    for stamp in tuple(_REGISTRY.values()):
        if any(obj is old for old in stamp['objects']):return stamp['namespace']
    for value in vars(obj).values():
        if isinstance(value,(classmethod,staticmethod)):value=value.__func__
        if isinstance(value,types.FunctionType) and value.__module__==obj.__module__:
            return value.__globals__
    raise SourceMismatch('no actual source namespace')

def register(namespace):
    frame=inspect.currentframe().f_back
    try:code=frame.f_code
    finally:del frame
    path=Path(namespace['__file__']).resolve();raw=path.read_bytes()
    compiled=compile(raw,code.co_filename,'exec',dont_inherit=True,optimize=sys.flags.optimize)
    # Keep the actual dictionary and definitions, not a module-name lookup.
    objects=tuple(v for v in namespace.values() if isinstance(v,(type,types.FunctionType)) and v.__module__==namespace['__name__'])
    dependencies=[]
    for value in tuple(namespace.values()):
        try:other=_namespace(value)
        except SourceMismatch:continue
        file=other.get('__file__')
        if file and other is not namespace:
            candidate=Path(file).resolve()
            if candidate.parent==path.parent and candidate.name in FILES and all(other is not d for d in dependencies):dependencies.append(other)
    stamp=dict(namespace=namespace,path=path,code=code,sha256=hashlib.sha256(raw).hexdigest(),matched=code==compiled,objects=objects,dependencies=dependencies)
    _REGISTRY[id(namespace)]=stamp
    return stamp

def source_digest(path, check=None):
    """Bounded source hashing; file growth never creates an unbounded buffer.

    Registration/loading remain trusted publisher/bootstrap work. Controlled
    attestation/fingerprinting callers prepay the fixed 64KiB stream scratch.
    """
    digest=hashlib.sha256()
    with Path(path).open('rb') as stream:
        while True:
            if check is not None:check()
            chunk=stream.read(65536)
            if not chunk:break
            digest.update(chunk)
            chunk=None
    return digest.hexdigest()

def attest(root,*objects,check=None):
    """Return internally trusted hashes only for the ACTUAL object's generation."""
    root=Path(root).resolve();pending=[]
    for obj in objects:
        namespace=_namespace(obj);stamp=_REGISTRY.get(id(namespace))
        actual=obj if isinstance(obj,(type,types.FunctionType)) else type(obj)
        if stamp is None or not any(actual is old for old in stamp['objects']):raise SourceMismatch('unregistered actual object')
        pending.append(namespace)
    pending.append(globals());seen=set();result={}
    while pending:
        namespace=pending.pop()
        if id(namespace) in seen:continue
        seen.add(id(namespace));stamp=_REGISTRY.get(id(namespace))
        if stamp is None or stamp['namespace'] is not namespace or not stamp['matched']:raise SourceMismatch('loaded code differs from sampled bytes')
        path=stamp['path']
        if path.parent!=root/'app' or path.name not in FILES:raise SourceMismatch('source outside trusted scope')
        if check is not None:check()
        current=source_digest(path,check)
        if check is not None:check()
        if current!=stamp['sha256']:raise SourceMismatch('disk differs from actual loaded generation')
        key='app/'+path.name
        if key in result and result[key]!=current:raise SourceMismatch('mixed loaded generations')
        result[key]=current;pending.extend(stamp['dependencies'])
    if set(result)!= {'app/'+name for name in FILES}:raise SourceMismatch('incomplete trusted source dependency closure')
    return result

_SOURCE_GENERATION=register(globals())
