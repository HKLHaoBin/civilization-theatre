"""Public Responses-only protocol. No tools, server conversation or billing fallback.
Import is inert. The real Gateway supplies its ORIGINAL shared Attempt budget.
"""
import codecs
from contextlib import contextmanager
import http.client
import json
import socket
import ssl
import threading
from .real_provider import ProviderFault

API='https://api.openai.com/v1'
MAX_WIRE=2*1024*1024
MAX_EVENT=512*1024
MAX_TEXT=65536
KNOWN_ERRORS=frozenset(('subscription_sharing_user_not_eligible','subscription_sharing_usage_limit_exceeded',
    'subscription_sharing_usage_unavailable','subscription_sharing_unsupported_capability',
    'subscription_sharing_route_not_supported','subscription_sharing_invalid_user',
    'subscription_sharing_user_unavailable','chatpass_v2_scope_not_authorized',
    'chatpass_v2_invalid_authorization_context','invalid_grant','invalid_client','invalid_refresh_token',
    'token_expired','refresh_token_expired','refresh_token_invalidated','refresh_token_reused'))


def upstream_failure(value,status=None):
    # Preserve safe fixed machine fields, NEVER provider diagnostic text (which
    # may echo prompts/tokens). Full body logging is deliberately not supported.
    error=value.get('error') if type(value) is dict else None
    if type(error) is str:code=error
    elif type(error) is dict:code=error.get('code')
    else:code=None
    safe=code if type(code) is str and code in KNOWN_ERRORS else None
    fault=ProviderFault('SIWC_'+safe.upper() if safe else 'SIWC_RESPONSE_FAILED')
    fault.upstream_code=safe;fault.http_status=status
    fault.response_shape='error' if error is not None else 'detail' if type(value) is dict and 'detail' in value else 'other'
    return fault


def bounded_json(raw):
    if len(raw)>MAX_EVENT:raise ProviderFault('SIWC_EVENT_LIMIT')
    try:
        def pairs(items):
            out={}
            for k,v in items:
                if k in out:raise ValueError()
                out[k]=v
            return out
        value=json.loads(raw,object_pairs_hook=pairs,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
        stack=[(value,0)];nodes=0
        while stack:
            v,d=stack.pop();nodes+=1
            if d>24 or nodes>16384:raise ValueError()
            if type(v) is dict:stack.extend((c,d+1) for c in v.values())
            elif type(v) is list:stack.extend((c,d+1) for c in v)
        return value
    except (ValueError,TypeError,UnicodeError,RecursionError):
        raise ProviderFault('SIWC_PROTOCOL') from None


class ResponsesDecoder:
    """Incremental UTF8 and SSE framing with independent terminal reserve.
    Terminal text is authoritative and checked against deltas (if present).
    Unknown lifecycle events are bounded, never executable. Output items accept
    assistant messages/reasoning only; any tool/function/refusal fails closed.
    """
    def __init__(self,budget):
        self.budget=budget;self.utf8=codecs.getincrementaldecoder('utf-8')('strict')
        self.buffer='';self.lines=[];self.size=0;self.delta='';self.terminal=None;self.events=0

    def feed(self,chunk):
        self.budget.check()
        if type(chunk) is not bytes:raise ProviderFault('SIWC_PROTOCOL')
        self.size+=len(chunk)
        if self.size>MAX_WIRE:raise ProviderFault('SIWC_STREAM_LIMIT')
        try:self.buffer+=self.utf8.decode(chunk)
        except UnicodeError:raise ProviderFault('SIWC_UTF8') from None
        while True:
            import re
            match=re.search(r'[\r\n]',self.buffer)
            if match is None:break
            index=match.start()
            if self.buffer[index]=='\r' and index+1==len(self.buffer):break
            width=2 if self.buffer[index:index+2]=='\r\n' else 1
            line=self.buffer[:index];self.buffer=self.buffer[index+width:]
            self._line(line)
        if len(self.buffer)>MAX_EVENT:raise ProviderFault('SIWC_EVENT_LIMIT')

    def _line(self,line):
        if len(line)>MAX_EVENT:raise ProviderFault('SIWC_EVENT_LIMIT')
        if line=='':
            if self.lines:self._event('\n'.join(self.lines))
            self.lines=[]
        elif line.startswith('data:'):
            self.lines.append(line[5:].removeprefix(' '))
        elif not line.startswith((':','event:','id:','retry:')):
            raise ProviderFault('SIWC_PROTOCOL')
        if sum(map(len,self.lines))>MAX_EVENT:raise ProviderFault('SIWC_EVENT_LIMIT')

    def _event(self,data):
        self.budget.check();self.events+=1
        if self.events>16384:raise ProviderFault('SIWC_EVENT_LIMIT')
        if data=='[DONE]':
            if self.terminal is None:raise ProviderFault('SIWC_MISSING_COMPLETED')
            return  # Framing marker is never a substitute for completed.
        if self.terminal is not None:raise ProviderFault('SIWC_AFTER_TERMINAL')
        e=bounded_json(data)
        if type(e) is not dict or type(e.get('type')) is not str:raise ProviderFault('SIWC_PROTOCOL')
        kind=e['type']
        if kind in ('error','response.failed','response.incomplete'):
            raise upstream_failure(e.get('response',e))
        if any(w in kind for w in ('function_call','tool_call','computer_call','web_search','file_search','refusal')):
            raise ProviderFault('SIWC_TOOLS_FORBIDDEN')
        if kind.startswith('response.output_item.'):
            item=e.get('item')
            if type(item) is not dict or item.get('type') not in ('message','reasoning'):
                raise ProviderFault('SIWC_TOOLS_FORBIDDEN')
        if kind=='response.output_text.delta':
            text=e.get('delta')
            if type(text) is not str:raise ProviderFault('SIWC_PROTOCOL')
            self.delta+=text
            if len(self.delta)>MAX_TEXT:raise ProviderFault('SIWC_TEXT_LIMIT')
        elif kind=='response.completed':
            r=e.get('response')
            if type(r) is not dict or r.get('status')!='completed' or r.get('error') is not None:
                raise ProviderFault('SIWC_RESPONSE_FAILED')
            out=r.get('output');text=[]
            if type(out) is not list:raise ProviderFault('SIWC_PROTOCOL')
            for item in out:
                if type(item) is not dict:raise ProviderFault('SIWC_PROTOCOL')
                if item.get('type')=='reasoning':continue
                if item.get('type')!='message' or item.get('role')!='assistant':
                    raise ProviderFault('SIWC_TOOLS_FORBIDDEN')
                if item.get('status') not in (None,'completed'):raise ProviderFault('SIWC_PROTOCOL')
                content=item.get('content')
                if type(content) is not list:raise ProviderFault('SIWC_PROTOCOL')
                for part in content:
                    if type(part) is not dict or part.get('type')!='output_text' or type(part.get('text')) is not str:
                        raise ProviderFault('SIWC_TOOLS_FORBIDDEN')
                    text.append(part['text'])
            value=''.join(text)
            if not value.strip() or len(value)>MAX_TEXT:raise ProviderFault('SIWC_TEXT_LIMIT')
            if self.delta and self.delta!=value:raise ProviderFault('SIWC_TEXT_MISMATCH')
            usage=r.get('usage')
            def count(name):
                v=usage.get(name) if type(usage) is dict else None
                if v is not None and (type(v) is not int or not 0<=v<=2**63-1):
                    raise ProviderFault('SIWC_USAGE_INVALID')
                return v
            self.terminal={'choices':[{'message':{'content':value}}],
                           'usage':{'prompt_tokens':count('input_tokens'),'completion_tokens':count('output_tokens')}}

    def ready(self):
        # Stop at a validated line/frame boundary, not keepalive EOF. Already
        # received partial trailers (including split UTF8) must be completed
        # and validated under the same budget before success is observable.
        return (self.terminal is not None and not self.buffer and not self.lines
                and not self.utf8.getstate()[0])

    def finish(self):
        self.budget.check()
        try:tail=self.utf8.decode(b'',final=True)
        except UnicodeError:raise ProviderFault('SIWC_UTF8') from None
        if self.buffer.endswith('\r'):
            self._line(self.buffer[:-1]);self.buffer=''
        if tail or self.buffer or self.lines or self.terminal is None:
            raise ProviderFault('SIWC_MISSING_COMPLETED')
        return self.terminal


INHERIT=object()


class _HTTPOwner:
    """Nonpersistent capability; held only by its caller/active lexical scope.
    The transport keeps no history registry. key does not retain the transport.
    """
    __slots__=('key','generation','__weakref__')
    def __init__(self,key):self.key=key;self.generation=0


class _RequestCancellation:
    """One request's monotonic latch. All lock sections are memory-only.

    Admission precedes each native boundary, not native completion. Cancel does
    not hard-interrupt DNS/TLS or unsend bytes already admitted. Late socket
    results are closed on the worker before it may advance to the next boundary.
    """
    def __init__(self,owner,budget,guard,scope):
        self.owner=owner;self.budget=budget;self.guard=guard;self.scope=scope
        self.cancelled=False;self.connection=None

    def check(self):
        with self.owner.lock:
            self.budget.check()
            if self.guard is not None:self.guard()
            if self.cancelled:raise ProviderFault('SIWC_AUTH_CANCELLED')

    def result(self,function,*args,**kwargs):
        # Socket creation and TLS wrapping can publish a socket after cancel.
        self.check();value=function(*args,**kwargs)
        try:self.check()
        except BaseException:
            try:value.close()
            finally:raise
        return value

    def call(self,function,*args,**kwargs):
        self.check()
        try:
            value=function(*args,**kwargs)
            self.check()
            return value
        finally:
            # The worker owns HTTPConnection/HTTPResponse close: their buffered
            # file locks must NEVER become a prerequisite for control cancellation.
            if self.cancelled and self.connection is not None:
                try:self.connection.close()
                except Exception:pass

    def attach(self,connection):
        with self.owner.lock:
            self.connection=connection
            self.check()
        # Never permit http.client.send to resurrect a closed old operation.
        # Connect explicitly below. Legacy fake request-only factories remain a
        # supported test seam; actual HTTPSConnection has all these boundaries.
        connection.auto_open=0
        if hasattr(connection,'_create_connection'):
            create=connection._create_connection
            connection._create_connection=lambda *a,**k:self.result(create,*a,**k)
        if hasattr(connection,'_context'):
            connection._context=_CancellationTLS(connection._context,self)
        for name in ('connect','send'):
            original=getattr(connection,name,None)
            if original is not None:
                def guarded(*a,_original=original,**k):return self.call(_original,*a,**k)
                setattr(connection,name,guarded)


class _CancellationTLS:
    """Only wrap_socket is intercepted; verification uses the original context."""
    def __init__(self,context,latch):self.context=context;self.latch=latch
    def __getattr__(self,name):return getattr(self.context,name)
    def wrap_socket(self,*args,**kwargs):
        return self.latch.result(self.context.wrap_socket,*args,**kwargs)


class PublicHTTP:
    """Fixed-host HTTPS, no proxies/redirects/retries; finite byte/time bounds.
    JSON OAuth/JWKS calls and SSE inference share this cancellable transport.
    """
    def __init__(self):
        self.lock=threading.RLock();self.active=set();self.local=threading.local();self.generation=0
        self._owner_key=object();self._default_owner=_HTTPOwner(self._owner_key)

    def new_owner(self):
        # No global strong/weak history table or monotonically growing owner map.
        return _HTTPOwner(self._owner_key)

    def _scope_owner(self,owner):
        if owner is None:return self._default_owner
        if type(owner) is not _HTTPOwner or owner.key is not self._owner_key:
            raise ProviderFault('SIWC_HTTP_OWNER')
        return owner

    def _check_scope(self,scope,generation,scope_generation):
        with self.lock:
            if generation!=self.generation or scope_generation!=scope.generation:
                raise ProviderFault('SIWC_AUTH_CANCELLED')

    @contextmanager
    def operation(self,guard,*,owner=INHERIT):
        # Omitted owner inherits; explicit None selects the OAuth/settings scope.
        # Capture before lock/budget/store/native waits. Every nested guard keeps
        # its original fence, so an old operation cannot recapture after cancel.
        previous=getattr(self.local,'check',None)
        previous_owner=getattr(self.local,'owner',None)
        scope=(previous_owner or self._default_owner) if owner is INHERIT else self._scope_owner(owner)
        scope_generation=scope.generation;generation=self.generation
        def combined():
            if previous is not None:previous()
            self._check_scope(scope,generation,scope_generation)
            if guard is not None:guard()
        self.local.check=combined;self.local.owner=scope
        try:yield
        finally:self.local.check=previous;self.local.owner=previous_owner

    def _check(self,budget):
        budget.check()
        check=getattr(self.local,'check',None)
        if check is not None:check()

    def _cancel_locked(self,owner=INHERIT):
        # Short metadata-only admission. Preserve no-arg global compatibility
        # until the separate account payload routes ordinary cancel to None.
        if owner is INHERIT:self.generation+=1
        else:owner.generation+=1
        items=[]
        for latch in self.active:
            if owner is not INHERIT and latch.scope is not owner:continue
            latch.cancelled=True
            c=latch.connection
            if c is not None and c.sock is not None:items.append(c.sock)
        return tuple(items)

    def cancel_owner(self,owner):
        scope=self._scope_owner(owner)
        with self.lock:items=self._cancel_locked(scope)
        self._close_sockets(items)

    @staticmethod
    def _close_sockets(items):
        for sock in items:
            try:sock.shutdown(socket.SHUT_RDWR)
            except (OSError,AttributeError):pass
            try:sock.close()
            except Exception:pass

    def close(self):
        with self.lock:items=self._cancel_locked()
        self._close_sockets(items)

    def request(self,url,method,body,headers,budget,stream=False):
        generation=self.generation  # Entry baseline, before constructor/lock waits.
        scope=getattr(self.local,'owner',None) or self._default_owner
        scope_generation=scope.generation
        from urllib.parse import urlsplit
        p=urlsplit(url)
        if p.scheme!='https' or p.netloc not in ('auth.openai.com','api.openai.com') or p.query or p.fragment:
            raise ProviderFault('SIWC_ENDPOINT')
        if body is not None and len(body)>1048576:raise ProviderFault('SIWC_REQUEST_LIMIT')
        c=None;r=None
        latch=_RequestCancellation(self,budget,getattr(self.local,'check',None),scope)
        with self.lock:
            self._check(budget)
            self._check_scope(scope,generation,scope_generation)
            self.active.add(latch)
        try:
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);context.load_default_certs()
            latch.check()
            c=http.client.HTTPSConnection(p.hostname,443,timeout=budget.remaining(),context=context);c.debuglevel=0
            latch.attach(c)
            connect=getattr(c,'connect',None)
            if connect is not None:connect()
            # Native request/connect/send/read never hold the cancellation lock.
            # Each later native boundary has its own monotonic admission fence.
            latch.call(c.request,method,p.path,body=body,headers=headers)
            latch.check()
            r=c.getresponse();latch.check()
            if r.status!=200:
                # Bounded diagnostic shape/code only. Never persist body text.
                chunks=[];size=0
                while size<=8192:
                    latch.check()
                    if c.sock is not None:c.sock.settimeout(budget.remaining())
                    chunk=r.read1(min(2048,8193-size));latch.check()
                    if not chunk:break
                    chunks.append(chunk);size+=len(chunk)
                raw=b''.join(chunks)
                try:value=bounded_json(raw) if len(raw)<=8192 else None
                except ProviderFault:value=None
                fault=upstream_failure(value,r.status)
                if fault.upstream_code is None:fault.code='SIWC_HTTP_'+str(r.status);fault.args=(fault.code,)
                import re
                request_id=r.getheader('x-request-id') or r.getheader('openai-request-id')
                fault.request_id=request_id if type(request_id) is str and re.fullmatch(r'req_[A-Za-z0-9_-]{1,96}',request_id) else None
                raise fault
            mime=r.getheader('Content-Type','').split(';')[0].strip().lower()
            if stream and mime!='text/event-stream':raise ProviderFault('SIWC_CONTENT_TYPE')
            decoder=ResponsesDecoder(budget) if stream else None
            chunks=[];size=0
            while True:
                latch.check()
                if c.sock is not None:c.sock.settimeout(budget.remaining())
                chunk=r.read1(16384);latch.check()
                if not chunk:break
                if decoder is not None:
                    decoder.feed(chunk)
                    # Do not discard an already-received partial trailer, and
                    # do not read beyond a clean completed boundary for EOF.
                    if decoder.ready():
                        latch.check();return decoder.finish()
                else:
                    size+=len(chunk)
                    if size>MAX_EVENT:raise ProviderFault('SIWC_RESPONSE_LIMIT')
                    chunks.append(chunk)
            if decoder is not None:return decoder.finish()
            raw=b''.join(chunks)
            return bounded_json(raw) if raw else None
        except ProviderFault:raise
        except Exception:
            latch.check();raise ProviderFault('SIWC_TRANSPORT') from None
        finally:
            if r is not None:
                try:r.close()
                except Exception:pass
            try:
                if c is not None:c.close()
            finally:
                with self.lock:self.active.discard(latch)

    def responses(self,service,messages,token,budget):
        if service['base_url']!=API:raise ProviderFault('SIWC_ENDPOINT')
        instructions='\n'.join(m['content'] for m in messages if m['role']=='system')
        inputs=[{'role':m['role'],'content':m['content']} for m in messages if m['role']!='system']
        body=json.dumps(dict(model=service['model'],instructions=instructions,input=inputs,
                             store=False,stream=True),ensure_ascii=False,allow_nan=False).encode('utf-8')
        return self.request(API+'/responses','POST',body,{'Content-Type':'application/json',
            'Accept':'text/event-stream','Authorization':'Bearer '+token},budget,stream=True)
