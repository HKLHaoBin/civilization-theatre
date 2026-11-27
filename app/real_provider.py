"""Explicit stdlib chat-completions protocol; NOT Pi SDK evidence.
No provider discovery, environment credentials, proxy discovery, worker, shell,
redirect or SDK fallback. Each invocation is under the original Attempt.call.
Only the configured game ref is read inside a bounded request; owned secret is
closed in finally. Immutable Python/TLS/HTTP copies are NOT provably erased.
"""
from contextlib import contextmanager
import http.client
import json
import os
from pathlib import Path
import socket
import ssl
import threading
import time
import uuid
from urllib.parse import urlsplit
from .real_intent import make_candidate,valid_request
from .real_settings import checked,fingerprint,default_credential_factory

MAX_RESPONSE=1048576
MAX_REQUEST=1048576


class ProviderFault(RuntimeError):
    def __init__(self,code,retryable=False):
        self.code=code;self.retryable=retryable
        super().__init__(code)


class RequestBudget:
    def __init__(self,attempt,seconds):
        self.attempt=attempt
        self.deadline=min(time.monotonic()+seconds,getattr(attempt,'deadline',float('inf')))
        self.requests=0

    def check(self):
        if self.attempt is not None:self.attempt.check()
        if time.monotonic()>=self.deadline:raise ProviderFault('REAL_REQUEST_DEADLINE')

    def remaining(self):
        self.check()
        return max(.001,self.deadline-time.monotonic())


class HTTPTransport:
    def __init__(self):
        self.active=set();self.lock=threading.RLock()

    def close(self):
        with self.lock:
            connections=tuple(self.active)
        for connection in connections:
            try:
                if connection.sock is not None:connection.sock.shutdown(socket.SHUT_RDWR)
            except (OSError,AttributeError):pass
            try:connection.close()
            except Exception:pass

    def __call__(self,service,messages,token,budget):
        parsed=urlsplit(service['base_url'])
        data=json.dumps({'model':service['model'],'messages':messages,'stream':False,
                         'max_tokens':4096},ensure_ascii=False,allow_nan=False).encode('utf-8')
        if len(data)>MAX_REQUEST:raise ProviderFault('REAL_REQUEST_TOO_LARGE')
        budget.check()
        timeout=budget.remaining()
        connection=None;response=None
        try:
            if parsed.scheme=='https':
                context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.load_default_certs()
                connection=http.client.HTTPSConnection(parsed.hostname,parsed.port or 443,timeout=timeout,context=context)
            else:
                connection=http.client.HTTPConnection(parsed.hostname,parsed.port or 80,timeout=timeout)
            connection.debuglevel=0  # Never emit request/auth frames to stdout.
            with self.lock:self.active.add(connection)
            headers={'Content-Type':'application/json','Accept':'application/json'}
            if token is not None:headers['Authorization']='Bearer '+token
            path=parsed.path.rstrip('/')+'/chat/completions'
            budget.check()
            connection.request('POST',path,body=data,headers=headers)
            budget.check()
            response=connection.getresponse()
            budget.check()
            if response.status!=200:
                # Never consume an error body or follow Location, even same host.
                raise ProviderFault('REAL_HTTP_STATUS',response.status in (429,502,503,504))
            content_length=response.getheader('Content-Length')
            if content_length is not None and (not content_length.isascii() or not content_length.isdigit()
                    or int(content_length)>MAX_RESPONSE):
                raise ProviderFault('REAL_RESPONSE_TOO_LARGE')
            chunks=[];size=0
            while True:
                budget.check()
                if connection.sock is not None:connection.sock.settimeout(budget.remaining())
                chunk=response.read1(min(16384,MAX_RESPONSE+1-size))
                budget.check()
                if not chunk:break
                chunks.append(chunk);size+=len(chunk)
                if size>MAX_RESPONSE:raise ProviderFault('REAL_RESPONSE_TOO_LARGE')
            raw=b''.join(chunks)
            # Reject an echo of this request's credential before any persistence.
            if token is not None and token.encode('utf-8') in raw:
                raise ProviderFault('REAL_SENSITIVE_RESPONSE')
            try:return json.loads(raw)
            except (ValueError,UnicodeError,RecursionError):raise ProviderFault('REAL_PROTOCOL') from None
        except ProviderFault:raise
        except (TimeoutError,OSError,http.client.HTTPException):
            budget.check()  # Original cancellation/deadline takes precedence.
            raise ProviderFault('REAL_TRANSPORT',True) from None
        except Exception:
            budget.check()
            raise ProviderFault('REAL_TRANSPORT') from None
        finally:
            if response is not None:
                try:response.close()
                except Exception:pass
            if connection is not None:
                try:connection.close()
                finally:
                    with self.lock:self.active.discard(connection)


def usage_values(response,requests):
    raw=response.get('usage') if type(response) is dict else None
    def value(name):
        v=raw.get(name) if type(raw) is dict else None
        return v if type(v) is int and 0<=v<=2**63-1 else None
    # Retried calls can have unknown billed usage. Never add UNKNOWN as zero.
    return dict(requests=requests,input_tokens=value('prompt_tokens') if requests==1 else None,
                output_tokens=value('completion_tokens') if requests==1 else None,cost=None)


def _strict_lifecycle_json(text):
    """New wire only: legacy role2 and ordinary DM decoding stay literal."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate key')
            result[key] = value
        return result
    def constant(value):
        raise ValueError('nonfinite')
    result = json.loads(text,object_pairs_hook=pairs,parse_constant=constant)
    json.dumps(result,ensure_ascii=False,allow_nan=False).encode('utf-8')
    return result


LIFECYCLE_PROTOCOL = 'p09-lifecycle-v1'
_LIFECYCLE_MAX_INT = 2**63-1


def _lifecycle_object(value, fields):
    if type(value) is not dict or set(value) != set(fields.split()):
        raise ValueError('shape')
    return value


def _lifecycle_text(value, maximum=2000):
    if type(value) is not str or not value.strip() or len(value) > maximum:
        raise ValueError('text')
    value.encode('utf-8')
    return value


def _lifecycle_int(value, minimum=1):
    if type(value) is not int or not minimum <= value <= _LIFECYCLE_MAX_INT:
        raise ValueError('integer')
    return value


def _lifecycle_list(value, maximum, minimum=0):
    if type(value) is not list or not minimum <= len(value) <= maximum:
        raise ValueError('list')
    return value


def _lifecycle_wire(text, maximum=MAX_RESPONSE):
    # Bound nesting BEFORE json.loads; duplicate keys and nonfinite values are
    # rejected by the inherited role3 decoder. No lossy UTF-8 replacement.
    if type(text) is not str or len(text.encode('utf-8')) > maximum:
        raise ValueError('wire size')
    depth = 0
    quoted = escaped = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in '[{':
            depth += 1
            if depth > 16:
                raise ValueError('depth')
        elif char in ']}':
            depth -= 1
    result = _strict_lifecycle_json(text)
    pending = [result]
    nodes = 0
    while pending:
        value = pending.pop()
        nodes += 1
        if nodes > 4096:
            raise ValueError('nodes')
        if type(value) is dict:
            pending.extend(value.keys())
            pending.extend(value.values())
        elif type(value) is list:
            pending.extend(value)
        elif type(value) not in (str, int, bool, type(None)):
            # This lifecycle wire has no floating-point fields.
            raise ValueError('scalar')
    return result


def _lifecycle_source_ref(ref):
    if type(ref) is not dict:
        raise ValueError('source ref')
    kind = ref.get('kind')
    fields = {
        'genesis': 'kind revision record_id',
        'dm_child': 'kind parent_intent_id decision_version effect_index child_intent_id',
        'due_event': 'kind node_id injury_id committed_revision',
        'lifecycle_job': 'kind job_id generation request_id committed_revision',
    }.get(kind)
    if fields is None:
        raise ValueError('source kind')
    _lifecycle_object(ref, fields)
    for key, value in ref.items():
        if key in ('revision','decision_version','committed_revision','generation'):
            _lifecycle_int(value)
        elif key == 'effect_index':
            _lifecycle_int(value, 0)
        else:
            _lifecycle_text(value, 1024)
    if kind == 'genesis' and ref['revision'] != 1:
        raise ValueError('genesis revision')


def _lifecycle_basis(ref, revision, kinds):
    _lifecycle_object(ref, 'kind id at_revision')
    if ref['kind'] not in kinds:
        raise ValueError('basis kind')
    _lifecycle_text(ref['id'], 1024)
    if _lifecycle_int(ref['at_revision']) != revision:
        raise ValueError('basis revision')
    return (ref['kind'], ref['id'], ref['at_revision'])


def _succession_request(payload):
    # A trusted root producer owns this carrier. Structural binding is NOT
    # evidence that a fact/death/slot actually exists: World re-proves it.
    _lifecycle_object(payload, 'kind protocol world job_id vacancy_id generation request_id base_revision proposed_person_id office death_ref established_facts unresolved_background_slots conflicts service_version')
    if payload['kind'] != 'succession' or payload['protocol'] != LIFECYCLE_PROTOCOL:
        raise ValueError('protocol')
    world = _lifecycle_object(payload['world'], 'world_id revision time')
    _lifecycle_text(world['world_id'], 128)
    revision = _lifecycle_int(world['revision'])
    _lifecycle_int(world['time'], 0)
    if _lifecycle_int(payload['base_revision']) != revision:
        raise ValueError('base')
    _lifecycle_int(payload['generation'])
    for key in ('job_id','vacancy_id'):
        _lifecycle_text(payload[key], 1024)
    _lifecycle_text(payload['request_id'], 128)
    person = _lifecycle_text(payload['proposed_person_id'], 128)
    if (not person.startswith('person-') or len(person) != 39
            or any(c not in '0123456789abcdef' for c in person[7:])):
        raise ValueError('allocated person')
    version = _lifecycle_text(payload['service_version'], 64)
    if len(version) != 64 or any(c not in '0123456789abcdef' for c in version):
        raise ValueError('service version')
    office = _lifecycle_object(payload['office'], 'id country_id office')
    for key in ('id','country_id'):
        _lifecycle_text(office[key], 128)
    if office['office'] not in ('king','finance','marshal'):
        raise ValueError('office')
    death = _lifecycle_object(payload['death_ref'], 'death_id person_id committed_revision')
    _lifecycle_text(death['death_id'], 1024)
    _lifecycle_text(death['person_id'], 128)
    if _lifecycle_int(death['committed_revision']) > revision or death['person_id'] == person:
        raise ValueError('death binding')
    available = {('death', death['death_id'], revision),
                 ('vacancy', payload['vacancy_id'], revision)}
    facts = {}
    for fact in _lifecycle_list(payload['established_facts'], 64):
        _lifecycle_object(fact, 'kind record source_ref')
        kind = fact['kind']
        record = fact['record']
        if kind == 'institution':
            _lifecycle_object(record, 'id country_id office_id place_id institution_text rule background_role public_text')
            for key in ('id','country_id','office_id','place_id'):
                _lifecycle_text(record[key], 128)
            for key in ('institution_text','background_role','public_text'):
                _lifecycle_text(record[key])
            if record['rule'] != 'automatic_acting_officer':
                raise ValueError('institution rule')
        elif kind == 'relationship':
            _lifecycle_object(record, 'id subject_person_id predicate object_text public_text')
            for key in ('id','subject_person_id'):
                _lifecycle_text(record[key], 128)
            for key in ('object_text','public_text'):
                _lifecycle_text(record[key])
            if record['predicate'] != 'institutional_role':
                raise ValueError('relationship predicate')
        else:
            raise ValueError('fact kind')
        _lifecycle_source_ref(fact['source_ref'])
        if fact['source_ref']['kind'] == 'genesis' and fact['source_ref']['record_id'] != record['id']:
            raise ValueError('source record binding')
        key = (kind, record['id'], revision)
        if key in available:
            raise ValueError('duplicate fact')
        available.add(key)
        facts[key] = record
    slots = _lifecycle_list(payload['unresolved_background_slots'], 32)
    slot_ids = set()
    for slot in slots:
        _lifecycle_object(slot, 'id office_id background_role place_id permitted_fields')
        for key in ('id','office_id','place_id'):
            _lifecycle_text(slot[key], 128)
        _lifecycle_text(slot['background_role'])
        if (slot['permitted_fields'] != ['name','card'] or slot['office_id'] != office['id']
                or slot['id'] in slot_ids):
            raise ValueError('slot')
        slot_ids.add(slot['id'])
        if not any(key[0] == 'institution' and record['office_id'] == office['id']
                   and record['country_id'] == office['country_id']
                   and record['place_id'] == slot['place_id']
                   and record['background_role'] == slot['background_role']
                   for key, record in facts.items()):
            raise ValueError('slot fact binding')
    conflicts = set()
    for ref in _lifecycle_list(payload['conflicts'], 64):
        key = _lifecycle_basis(ref, revision, ('institution','relationship'))
        if key not in available or key in conflicts:
            raise ValueError('conflict binding')
        conflicts.add(key)
    return available, facts, conflicts


def _succession_response(result, payload):
    available, facts, conflicts = _succession_request(payload)
    _lifecycle_object(result, 'status reason basis_refs successor unresolved')
    status = result['status']
    if status not in ('appointed','vacant','disputed'):
        raise ValueError('status')
    _lifecycle_text(result['reason'])
    refs = set()
    for ref in _lifecycle_list(result['basis_refs'], 8, 1):
        key = _lifecycle_basis(ref, payload['base_revision'], ('institution','relationship','death','vacancy'))
        if key not in available or key in refs:
            raise ValueError('response basis binding')
        refs.add(key)
    for item in _lifecycle_list(result['unresolved'], 8):
        _lifecycle_text(item)
    successor = result['successor']
    if status != 'appointed':
        if successor is not None:
            raise ValueError('nonappointed successor')
        if status == 'disputed' and (not conflicts or not conflicts.intersection(refs)):
            raise ValueError('dispute basis')
        return result
    _lifecycle_object(successor, 'name background_role location card')
    _lifecycle_text(successor['name'], 128)
    _lifecycle_text(successor['background_role'])
    _lifecycle_text(successor['location'], 128)
    card = _lifecycle_object(successor['card'], 'values interests risk mission')
    for value in card.values():
        _lifecycle_text(value)
    if payload['office']['office'] != 'finance' or conflicts:
        raise ValueError('unsupported or conflicting appointment')
    if not any(slot['background_role'] == successor['background_role']
               and slot['place_id'] == successor['location']
               and any(key in refs and key[0] == 'institution'
                       and record['office_id'] == slot['office_id']
                       and record['country_id'] == payload['office']['country_id']
                       and record['place_id'] == slot['place_id']
                       and record['background_role'] == slot['background_role']
                       for key, record in facts.items())
               for slot in payload['unresolved_background_slots']):
        raise ValueError('successor slot binding')
    return result


def _lifecycle_effects(result, payload):
    # Validate only the three explicitly published additions. The existing
    # compiler/World remain owners of legacy effects and all causal legality.
    revision = _lifecycle_int(payload['world']['revision'])
    for effect in result['effects']:
        action = effect.get('action')
        if action == 'resolve_physical_attempt':
            _lifecycle_object(effect, 'action outcome basis_refs reason')
            if effect['outcome'] not in ('no_contact','nonfatal','mortal'):
                raise ValueError('outcome')
            if effect['outcome'] == 'mortal' and result['ruling'] not in ('成功','大成功'):
                raise ValueError('failed mortal result')
            _lifecycle_text(effect['reason'])
            seen = set()
            for ref in _lifecycle_list(effect['basis_refs'], 8, 1):
                key = _lifecycle_basis(ref, revision, ('personal_object','scene_person','institution','relationship'))
                if key in seen:
                    raise ValueError('duplicate basis')
                seen.add(key)
        elif action == 'write_office_document':
            _lifecycle_object(effect, 'action text quote_start quote_end')
            _lifecycle_text(effect['text'], 8000)
            start = _lifecycle_int(effect['quote_start'], 0)
            end = _lifecycle_int(effect['quote_end'], 0)
            proposal = payload['proposal']['text']
            if type(proposal) is not str or not 0 <= start < end <= len(proposal) or effect['text'] != proposal[start:end]:
                raise ValueError('quote')
        elif action == 'read_office_document':
            _lifecycle_object(effect, 'action document_id')
            _lifecycle_text(effect['document_id'], 1024)
    return result


class RealGateway:
    def __init__(self,settings,*,transport=None,credential_factory=None):
        self.settings=settings
        self.transport=transport or HTTPTransport()
        self.credential_factory=credential_factory or default_credential_factory
        self.lock=threading.RLock()
        self.budgets={}
        self.responses=self.settings.siwc.http
        self._responses_transport=None;self._responses_owner=None
        self._bind_response_owner()

    def version(self):
        config=self.settings.snapshot()
        # Stable marker permits durable preparation even before configuration.
        return fingerprint(config)

    def _bind_response_owner(self):
        # Legacy request-only synthetic fakes do not have scope semantics. Never
        # close their shared HTTP/session as a substitute for actual isolation.
        # Replacement is an existing test-injection seam, not a concurrent API.
        with self.lock:
            if self.responses is not self._responses_transport:
                scoped=all(callable(getattr(self.responses,name,None))
                           for name in ('new_owner','operation','cancel_owner'))
                owner=self.responses.new_owner() if scoped else None
                self._responses_transport=self.responses;self._responses_owner=owner
            return self._responses_transport,self._responses_owner

    @contextmanager
    def _response_operation(self):
        transport,owner=self._bind_response_owner()
        if owner is None:
            yield None
        else:
            with transport.operation(None,owner=owner):
                # PublicHTTP retains the lexical guard in its original local
                # slot. Do not change operation's original yielded None API.
                yield getattr(getattr(transport,'local',None),'check',None)

    def close(self):
        transport,owner=self._bind_response_owner()
        try:
            if owner is not None:transport.cancel_owner(owner)
        finally:
            closer=getattr(self.transport,'close',None)
            if closer is not None:closer()

    def _budget(self,kind,service,attempt):
        if attempt is None:return RequestBudget(None,service['timeout_seconds'])
        with self.lock:
            # Retain only the current/unfinished Attempt; Host forbids a new act
            # until calls have joined. An old blocked call cannot borrow a new one.
            key=(attempt,kind)
            for old in tuple(self.budgets):
                if old[0] is not attempt and not any(t.is_alive() for t in old[0].calls):
                    del self.budgets[old]
            if key not in self.budgets:self.budgets[key]=RequestBudget(attempt,service['timeout_seconds'])
            return self.budgets[key]

    def _generate(self,kind,messages,attempt,expected_version=None):
        # One lexical owner around the ORIGINAL access/refresh/Responses/retry
        # route. Inner session.operation(guard) inherits this owner, not None.
        with self._response_operation() as guard:
            result=self._generate_owned(kind,messages,attempt,expected_version)
            if guard is not None:guard()
            return result

    def _generate_owned(self,kind,messages,attempt,expected_version=None):
        if attempt is not None:attempt.check()
        config=self.settings.snapshot()
        if config is None:raise ProviderFault('REAL_UNCONFIGURED')
        config=checked(config)
        if expected_version is not None and expected_version!=fingerprint(config):
            raise ProviderFault('REAL_CONFIGURATION_CHANGED')
        service=config[kind]  # Independent descriptor; never fall back to roles.
        budget=self._budget(kind,service,attempt)
        budget.check()
        request_id=uuid.uuid4().hex
        last=None
        for _ in range(service['max_retries']+1):
            budget.check()
            if budget.requests>=service['max_retries']+1:
                raise ProviderFault('REAL_REQUEST_LIMIT')
            event=dict(kind=kind,request_id=request_id,request_index=budget.requests+1,
                       input_tokens=None,output_tokens=None,cost=None,billing_status='UNKNOWN')
            secret=None;token=None;started=False;session_context=None
            try:
                if service['provider']=='openai-siwc-responses':
                    pending_context=self.settings.siwc.access(service['credential_ref'],budget)
                    token=pending_context.__enter__()
                    session_context=pending_context
                elif service['credential_ref'] is not None:
                    try:
                        secret=self.credential_factory().read(service['credential_ref'])
                        if secret is None:raise ProviderFault('REAL_CREDENTIAL_UNAVAILABLE')
                        raw=secret.reveal_bytes()
                        token=raw.decode('utf-8')
                        if not token or any(ord(c)<33 or ord(c)>126 for c in token):
                            raise ProviderFault('REAL_CREDENTIAL_INVALID')
                        del raw
                    except ProviderFault:raise
                    except Exception:raise ProviderFault('REAL_CREDENTIAL_UNAVAILABLE') from None
                budget.check()
                self.settings.usage(dict(event,event='request_started'))
                budget.requests+=1;started=True
                try:
                    if service['provider']=='openai-siwc-responses':
                        response=self.responses.responses(service,messages,token,budget)
                    else:response=self.transport(service,messages,token,budget)
                except ProviderFault:raise
                except Exception:
                    budget.check()
                    raise ProviderFault('REAL_TRANSPORT') from None
                budget.check()
                if token is not None and token in json.dumps(response,ensure_ascii=False):
                    raise ProviderFault('REAL_SENSITIVE_RESPONSE')
                try:
                    text=response['choices'][0]['message']['content']
                    if type(text) is not str or not text.strip() or len(text)>65536:
                        raise ValueError
                except (KeyError,IndexError,TypeError,ValueError):
                    raise ProviderFault('REAL_PROTOCOL') from None
                if token is not None:
                    if token in text:
                        raise ProviderFault('REAL_SENSITIVE_RESPONSE')
                    if kind=='dm':
                        # DM's content is itself JSON; inspect its decoded text
                        # values too, so JSON escaping cannot hide a raw key.
                        def contains(value):
                            if type(value) is str:return token in value
                            if type(value) is list:return any(contains(v) for v in value)
                            if type(value) is dict:return any(contains(k) or contains(v) for k,v in value.items())
                            return False
                        try: decoded=json.loads(text)
                        except (ValueError,RecursionError):raise ProviderFault('REAL_DM_PROTOCOL') from None
                        if contains(decoded):raise ProviderFault('REAL_SENSITIVE_RESPONSE')
                usage=usage_values(response,budget.requests)
                # Per-response fields are known even if earlier retried billing
                # is unknown. Keep actual returned usage in this indexed event;
                # aggregate candidate usage remains UNKNOWN when requests > 1.
                returned=usage_values(response,1)
                self.settings.usage(dict(event,event='request_returned',
                                         input_tokens=returned['input_tokens'],output_tokens=returned['output_tokens']))
                return text,usage
            except ProviderFault as fault:
                last=fault
                if started:self.settings.usage(dict(event,event='request_failed'))
                if not fault.retryable:raise
            finally:
                token=None
                if session_context is not None:
                    import sys
                    session_context.__exit__(*sys.exc_info())
                if secret is not None:
                    try:secret.close()
                    except Exception:raise ProviderFault('REAL_CREDENTIAL_CLOSE_FAILED') from None
        raise last or ProviderFault('REAL_REQUEST_LIMIT')

    def role(self,request,attempt):
        # Missing configuration has its own stable code, not a scripted fallback.
        if self.settings.snapshot() is None:raise ProviderFault('REAL_UNCONFIGURED')
        if not valid_request(request):raise ProviderFault('REAL_INVALID_REQUEST')
        messages=[{'role':'system','content':request['system_prompt']+
                   '\n你自主决定下一步意图；可以等待、拒绝或提出任何自然语言计划。只返回意图原文，不捏造未收到的世界事实。'}]
        if request['schema_version'] == 3:
            messages[0]['content'] = request['system_prompt'] + (
                '\n你自主决定下一步意图。返回严格JSON，仅有text和physical_attempt。'
                'text为1至8000字自然语言原文；普通意图physical_attempt=null。'
                '仅当你本人确实选择以实持武器近身行动，physical_attempt才为'
                '{mode:"strike_with_held_weapon",target_person_id:真实人物ID,means_id:真实本人实持物ID}。'
                '锚不是成功保证，不替目标选择反击、投降、遗言或忠诚；不捏造手段和未获知识。')
        for m in request['messages']:
            if type(m) is not dict or m.get('role') not in ('user','assistant') or type(m.get('content')) is not str:
                raise ProviderFault('REAL_SESSION_PROTOCOL')
            messages.append({'role':m['role'],'content':m['content']})
        messages.append({'role':'user','content':request['prompt']})
        text,usage=self._generate('roles',messages,attempt,request['service_version'])
        if request['schema_version'] == 3:
            try:
                response = _strict_lifecycle_json(text)
                result = make_candidate(request,response,usage)
            except (ValueError,TypeError,KeyError,RecursionError,UnicodeError):
                raise ProviderFault('REAL_ROLE_PROTOCOL') from None
        else:
            if len(text)>8000:raise ProviderFault('REAL_INTENT_TOO_LARGE')
            result=make_candidate(request,text,usage)
        return dict(result,sdk_events={'http_requests':usage['requests']},sdk_requests=0,
                    provider='real-chat-v1',pid=os.getpid(),module_root=str(Path(__file__).resolve().parent.parent))

    def _dm_succession(self,payload,attempt,expected_version):
        from .actor_execution import LifecycleCallBudget
        if type(attempt) is not LifecycleCallBudget:
            raise ProviderFault('REAL_LIFECYCLE_BUDGET_REQUIRED')
        attempt.check()
        try:
            # Freeze the exact trusted request before IO; do not pass mutable
            # caller containers to the transport or use returned IDs as grants.
            request_text=json.dumps(payload,ensure_ascii=False,allow_nan=False)
            payload=_lifecycle_wire(request_text,MAX_REQUEST)
            _succession_request(payload)
            if payload['world']['world_id'] != attempt.root_permit.world_id:
                raise ValueError('root world binding')
            if expected_version is not None and expected_version != payload['service_version']:
                raise ValueError('service binding')
        except (ValueError,TypeError,KeyError,RecursionError,UnicodeError):
            attempt.check()
            raise ProviderFault('REAL_DM_PROTOCOL') from None
        system=('你是独立世界主持者；这是p09-lifecycle-v1的succession根业务，不是角色会话。'
                '只能依据本请求的既成制度、关系、真实死亡、空缺和conflicts裁定；不修改历史，'
                '不代活跃在位者任命，不虚构血缘、秘密、旧经历或继任安排。'
                '仅返回严格JSON五键status,reason,basis_refs,successor,unresolved，禁止额外键。'
                'status为appointed/vacant/disputed；reason为1至2000字；basis_refs为1至8项'
                '{kind:institution/relationship/death/vacancy,id,at_revision:本base_revision}，'
                '必须引用本请求实际事实或death_ref.death_id/vacancy_id；unresolved为至多8条1至2000字文字。'
                '只有finance已具证且无冲突的automatic_acting_officer习惯与实际slot支持时才能appointed，'
                'successor仅含name,background_role,location,card；name为1至128字，'
                'background_role和location必须精确匹配被引用制度支持的同一slot。'
                'card仅含values,interests,risk,mission四个1至2000字文字。只填slot允许的name/card，'
                '不得提供person_id/world_id/base_revision/permit或复制前任私密会话。'
                '无自动制度或无slot正常vacant；有实际冲突可disputed且引用冲突依据；'
                'vacant/disputed必须successor=null，不是技术失败，不强造人。'
                '你的响应不是权限，World仍重证事实、身份、历史与事务。')
        text,usage=self._generate('dm',[{'role':'system','content':system},
            {'role':'user','content':request_text}],attempt,payload['service_version'])
        attempt.check()
        try:
            response=_succession_response(_lifecycle_wire(text),payload)
        except (ValueError,TypeError,KeyError,RecursionError,UnicodeError):
            attempt.check()
            raise ProviderFault('REAL_DM_PROTOCOL') from None
        attempt.check()
        return {'response_raw':text,'response':response}

    def dm(self,payload,attempt,expected_version=None):
        if type(payload) is dict and payload.get('kind') == 'succession':
            return self._dm_succession(payload,attempt,expected_version)
        system=('你是独立世界主持者，不替活跃角色选择意图，不修改既成历史。只裁定proposal原意图。'
                '返回严格JSON对象，且仅有ruling,reason,goal,costs,benefits,effects。'
                'ruling必须是大失败、失败、成功、大成功之一；reason和goal为简短文字；'
                'costs和benefits为最多8个文字；effects为1至8个前向动作对象。'
                '可执行效果协议（不是角色意图上限）：wait；investigate；send(to,text)；meet(to,text)；'
                'meet是可选效果，最终由当前真实规则判可用；仅显式启用会面的optin新局，'
                '两名不同人物实际同地且均不在途时才兑现。仅兑现本次提案者向对方的真实指定发言，'
                '不得替对方答应、撒谎或决定忠诚；失败或大失败不投递。对方接收后由其独立会话'
                '决定回答、拒绝或等待；未达成或无共识是正常政治结果，不是技术故障。'
                'travel(to)；ship(to,quantity)；command(to,text)；respond(command_id,accept)；'
                'start_production(recipe_id,quantity)；ship_material(to,resource_id,quantity)；'
                'military_move(army_id,to)；military_supply(army_id,quantity)；'
                'military_garrison(army_id)；military_engage(army_id,losses)。'
                '军事仅在场景显式启用且实际存在军队时可用；army_id使用真实军队ID，to为地点ID。'
                '仅同国君主或军事官与己方军队实际同地且不在途时可即时指挥；无兵力不得行动。'
                '补给从本地已控制粮仓转移实粮；行军按真实路线排队，抵达不等于驻防控制。'
                '驻防前不得有在场敌军；除补给外行动须至少一日实有军粮。'
                '接战losses是实际全部参战军队ID到非负整数战损的映射（至少2军、至多64军），'
                '至少一方有损失，且不超过现存兵力、在场有粮力量与场景防御界限；不预定胜负。'
                '不得捏造未启用的军队、招募、投降、忠诚、人物死亡或继任能力。'
                '每个效果仅含action和所列字段，数量为正整数；to须用对应人物或地点ID而非显示名。'
                '经济效果必须满足世界已声明配方、库存、劳动和时空约束；'
                '缺少执行能力不得伪造结果；返回的效果将由真实规则事务校验，不拥有SQL/HTTP写权。')
        protocol=payload.get('opt_in_effect_protocol') if type(payload) is dict else None
        lifecycle=(type(protocol) is dict and protocol.get('version') == LIFECYCLE_PROTOCOL)
        if lifecycle:
            system=system.replace(
                '不得捏造未启用的军队、招募、投降、忠诚、人物死亡或继任能力。',
                '不得捏造未启用的军队、招募、投降、忠诚或超出本显式协议的死亡继任能力。')
            system+=('本次可信opt_in_effect_protocol显式启用p09-lifecycle-v1三个有限新效果：'
                '{action:"resolve_physical_attempt",outcome:"no_contact"/"nonfatal"/"mortal",'
                'basis_refs:[{kind:"personal_object"/"scene_person"/"institution"/"relationship",id,at_revision}],reason}；'
                'basis_refs为1至8项真实本base_revision引用，reason为1至2000字。'
                '只能裁proposal.physical_attempt原锚，不得新增或改选actor、target、means，'
                '原锚null或普通死亡断言不能造成死亡；仅成功或大成功可兑现mortal。'
                '{action:"write_office_document",text,quote_start,quote_end}，'
                '严格非负整数Unicode codepoint半开区间，text精确等于proposal.text该非空片段；'
                '{action:"read_office_document",document_id}只读真实指定文书。'
                '手段、当前职权、机构地点与已锚意图只依本可信描述和proposal，'
                '效果不得替目标选择反击、投降或遗言，不能任意kill、appoint或继承session；'
                'World会在preview/commit重证物理、任职、知识与因果，预览不投递。')
        text,usage=self._generate('dm',[{'role':'system','content':system},
                     {'role':'user','content':json.dumps(payload,ensure_ascii=False,allow_nan=False)}],attempt,expected_version)
        try:
            result=_lifecycle_wire(text) if lifecycle else json.loads(text)
            fields={'ruling','reason','goal','costs','benefits','effects'}
            if type(result) is not dict or set(result)!=fields or result['ruling'] not in ('大失败','失败','成功','大成功'):
                raise ValueError
            if any(type(result[k]) is not str or not result[k].strip() or len(result[k])>2000 for k in ('reason','goal')):
                raise ValueError
            for k in ('costs','benefits'):
                if type(result[k]) is not list or len(result[k])>8 or any(type(t) is not str or not t.strip() or len(t)>2000 for t in result[k]):
                    raise ValueError
            if type(result['effects']) is not list or not 1<=len(result['effects'])<=8 or any(type(e) is not dict for e in result['effects']):
                raise ValueError
            if lifecycle:_lifecycle_effects(result,payload)
            return dict(result,world_id=payload['world']['world_id'],base_revision=payload['world']['revision'])
        except (ValueError,TypeError,KeyError,RecursionError,UnicodeError):raise ProviderFault('REAL_DM_PROTOCOL') from None
