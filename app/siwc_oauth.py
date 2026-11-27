"""Independent game OAuth, never Hermes/Codex/environment credential discovery.
All network endpoints fixed to OpenAI. Caller starts loopback HTTP server first.
Only sanitized metadata reaches settings renderer; OAuth hints remain backend.
"""
import base64
from contextlib import contextmanager,nullcontext
import hashlib
import json
import re
import secrets
import threading
import time
import uuid
from urllib.parse import parse_qs,urlencode,urlsplit
from .real_provider import ProviderFault,RequestBudget
from .siwc_responses import API,PublicHTTP
from .siwc_storage import SessionStore
from .siwc_transactions import AuthTransaction

ISSUER='https://auth.openai.com'
AUTHORIZE=ISSUER+'/api/accounts/authorize'
TOKEN=ISSUER+'/api/accounts/oauth/token'
DISCOVERY=ISSUER+'/.well-known/openid-configuration'
JWKS=ISSUER+'/.well-known/jwks.json'
SCOPES=frozenset(('openid','profile','email','offline_access','resource.invoke','chatgpt.tokens.use.direct'))


class IdentityVerifier:
    def dependency(self):
        try:
            import jwt
            from jwt.algorithms import RSAAlgorithm
        except ImportError:raise ProviderFault('SIWC_CRYPTO_UNCONFIGURED') from None
        return jwt,RSAAlgorithm

    def verify(self,token,client,nonce,jwks,now):
        jwt,rsa=self.dependency()
        try:
            header=jwt.get_unverified_header(token)
            if header.get('alg')!='RS256' or header.get('crit') or type(header.get('kid')) is not str:raise ValueError()
            keys=jwks.get('keys') if type(jwks) is dict else None
            if type(keys) is not list or len(keys)>32:raise ValueError()
            matched=[k for k in keys if type(k) is dict and k.get('kid')==header['kid']
                     and k.get('kty')=='RSA' and k.get('use','sig')=='sig' and k.get('alg','RS256')=='RS256'
                     and ('key_ops' not in k or 'verify' in k['key_ops'])]
            if len(matched)!=1:raise ValueError()
            key=rsa.from_jwk(json.dumps(matched[0]))
            claims=jwt.decode(token,key,algorithms=['RS256'],issuer=ISSUER,audience=client,
                options={'require':['iss','aud','exp','iat','sub'],'verify_signature':True,
                         'verify_exp':True,'verify_iat':True,'verify_nbf':True},leeway=5)
            return validate_claims(claims,client,nonce,now)
        except ProviderFault:raise
        except Exception:raise ProviderFault('SIWC_IDENTITY_INVALID') from None


def validate_claims(c,client,nonce,now):
    # Additional shape checks independent of the crypto library (never a bypass).
    if type(c) is not dict or c.get('iss')!=ISSUER:raise ProviderFault('SIWC_IDENTITY_INVALID')
    aud=c.get('aud')
    if not (aud==client or (type(aud) is list and client in aud and all(type(x) is str for x in aud))):
        raise ProviderFault('SIWC_IDENTITY_INVALID')
    if type(aud) is list and len(aud)>1 and c.get('azp')!=client:raise ProviderFault('SIWC_IDENTITY_INVALID')
    if c.get('azp',client)!=client:raise ProviderFault('SIWC_IDENTITY_INVALID')
    if type(c.get('sub')) is not str or not 1<=len(c['sub'])<=512:raise ProviderFault('SIWC_IDENTITY_INVALID')
    if any(type(c.get(k)) is not int for k in ('exp','iat')):raise ProviderFault('SIWC_IDENTITY_INVALID')
    if c['exp']<=now-5 or c['iat']>now+5 or c['exp']<=c['iat']:raise ProviderFault('SIWC_IDENTITY_INVALID')
    if 'nbf' in c and (type(c['nbf']) is not int or c['nbf']>now+5):raise ProviderFault('SIWC_IDENTITY_INVALID')
    if nonce is not None and (type(c.get('nonce')) is not str or not secrets.compare_digest(c['nonce'],nonce)):
        raise ProviderFault('SIWC_NONCE_INVALID')
    return c


def browser_open(url):
    import webbrowser
    if not webbrowser.open(url,new=1):raise ProviderFault('SIWC_BROWSER_UNAVAILABLE')


def empty_account(client,subject):
    return dict(client_id=client,subject=subject,slots=[],sha256=hashlib.sha256(b'').hexdigest(),
                size=0,rotation_pending=False,welcome_seen=False)


class SIWCSession:
    def __init__(self,path,credential_factory,*,http=None,verifier=None,clock=None,opener=None,file_lock=None):
        self.store=SessionStore(path,credential_factory,file_lock=file_lock)
        self.http=http or PublicHTTP();self.verifier=verifier or IdentityVerifier()
        self.clock=clock or time.time;self.opener=opener or browser_open
        self.pending=None;self.pending_lock=threading.RLock();self.epoch=0;self.closed=False
        self.lifecycle_lock=threading.RLock();self.last_error=None
        self.retry_client=None;self.retry_subject=None;self._view=None;self._tx=None

    def _transaction_status(self):
        with self.lifecycle_lock:tx=self._tx
        return tx.status() if tx is not None else dict(txid=None,outcome='cancelled_complete',request_ack=True,
            durable_ack=True,durability='confirmed',cancellation_complete=True,account_mutation_pending=False)

    def transaction_status(self,budget=None):
        with self.lifecycle_lock:tx=self._tx
        if tx is not None and tx.recovery_ready() and not getattr(self.store.local,'depth',0):
            budget=budget or RequestBudget(None,2)
            tx.confirm_view(False)
            try:
                with self.store.locked(budget):
                    if tx.recovery_ready():m,decision=self.store.reconcile(tx,budget)
                    else:return self._transaction_status()
                # No ACK before THIS query's native unlock returned either.
                if decision is not None:tx.finish(decision)
                tx.confirm_view();self._remember(m,tx=tx)
            except Exception:pass  # unknown stays unknown; no retry/new budget
        return self._transaction_status()

    def _remember(self,m,*,tx=None):
        import copy
        # History belongs to the captured tx; the shared identity view belongs
        # to the current Session. Never hold this short lock while waiting on L.
        with self.lifecycle_lock:
            if ((tx is None or self._tx is tx)
                and (self._view is None or m.revision>=self._view.revision)):
                self._view=copy.deepcopy(m)
        # A late caller still consumes its OWN snapshot, never newer authority.
        return m

    def _intent(self,budget,key=None):
        # Preserve identity BEFORE waiting, but never reuse a token generation
        # forever. Capture current read-only authority on every new operation.
        with self.lifecycle_lock:previous=self._view
        anchor=previous.handle(key) if previous is not None else None
        m=self.store.load(budget)
        if anchor is not None:self.store.assert_handle(m,anchor,rotation=True)
        self._remember(m)
        return m.handle(key)

    @contextmanager
    def _mutation(self,budget,epoch,*,kind='account'):
        # A synchronous reentrant mutation cannot ACK the outer owner's native
        # unlock, and must never recover/overwrite its in-flight PREPARED state.
        if getattr(self.store.local,'depth',0):raise ProviderFault('SIWC_MUTATION_PENDING')
        tx=AuthTransaction(lambda:self._check_access(epoch,budget),kind=kind)
        failure=None;decision='UNKNOWN';released=False
        try:
            with self.store.locked(budget):
                # A queued operation becomes current only AFTER acquiring L.
                self._check_access(epoch,budget)
                with self.lifecycle_lock:self._tx=tx
                try:
                    yield tx
                    decision='COMMITTED'
                except BaseException as fault:
                    failure=fault
                    if tx.status()['outcome'] in ('preparing','cancel_won_pending'):
                        # Same L recovery even after an unknown PREPARED return.
                        self.store._gc(self.store._recover());decision='CANCELLED'
            released=True;tx.finish(decision)
            if failure is not None:raise failure
        except BaseException:
            # An exception from native unlock never produces a durable ACK.
            if not released:tx.finish('UNKNOWN')
            raise
        finally:tx.writer_stopped()

    def status(self):
        with self.pending_lock:
            if self.pending is not None and self.pending['expires']<=self.clock():self.pending=None
            connecting=self.pending is not None
        budget=RequestBudget(None,2);ack=self.transaction_status(budget)
        if ack['account_mutation_pending']:
            return dict(state='account_mutation_pending',accounts=[],active=None,connecting=connecting,
                connection_verified=False,real_call_verified=False,credential_status='unknown',
                error=self.last_error,welcome_required=False,**ack)
        try:m=self._remember(self.store.load(budget))
        except ProviderFault:
            return dict(state='account_mutation_pending',accounts=[],active=None,connecting=connecting,
                connection_verified=False,real_call_verified=False,credential_status='unknown',
                error='SIWC_RECOVERY_PENDING',welcome_required=False,request_ack=True,durable_ack=False,
                outcome='commit_unknown',durability='pending_or_unknown',account_mutation_pending=True,cancellation_complete=False)
        active=m['active'];a=m['accounts'].get(active)
        return dict(state=('reauthorization_required' if a and a['rotation_pending'] else
                          'authorized_not_verified' if a and a['slots'] else 'unconfigured'),
            accounts=[{'account':k,'label':'ChatGPT account '+k.rsplit('/',1)[-1],'active':k==active,
                       'signed_in_metadata':bool(v['slots'])} for k,v in m['accounts'].items()],
            active=active,connecting=connecting,connection_verified=False,real_call_verified=False,
            credential_status='unknown',error=self.last_error,
            welcome_required=bool(a and a['slots'] and not a['welcome_seen']),**ack)

    def binding(self):
        if self.closed:raise ProviderFault('SIWC_UNCONFIGURED')
        budget=RequestBudget(None,2)
        if self.transaction_status(budget)['account_mutation_pending']:raise ProviderFault('SIWC_MUTATION_PENDING')
        m=self._remember(self.store.load(budget));a=m['accounts'].get(m['active'])
        if a and a['rotation_pending']:raise ProviderFault('SIWC_REAUTHORIZATION_REQUIRED')
        return m['active'] if a and a['slots'] else None

    @contextmanager
    def binding_snapshot(self,budget=None):
        # Settings binds BOTH role descriptors from one locked snapshot. Stored
        # descriptor refs remain names, never authority: access rechecks every use.
        budget=budget or RequestBudget(None,10)
        if self.transaction_status(budget)['account_mutation_pending']:raise ProviderFault('SIWC_MUTATION_PENDING')
        with self.store.locked(budget):
            if self.closed or self._transaction_status()['account_mutation_pending']:raise ProviderFault('SIWC_MUTATION_PENDING')
            m=self._remember(self.store.load(budget));a=m['accounts'].get(m['active'])
            if a and a['rotation_pending']:raise ProviderFault('SIWC_REAUTHORIZATION_REQUIRED')
            yield m['active'] if a and a['slots'] else None

    def _invalidate(self,permanent=False):
        # Cancel/commit competition occurs on tx.M BEFORE epoch invalidation.
        # It never holds M across transport socket close or L wait.
        with self.lifecycle_lock:
            tx=self._tx
            if tx is not None:tx.cancel()
            if permanent:self.closed=True
            self.epoch+=1;epoch=self.epoch
        with self.pending_lock:self.pending=None
        if isinstance(self.http,PublicHTTP):
            if permanent:self.http.close()
            else:self.http.cancel_owner(None)
        else:
            # Legacy synthetic HTTP has no scope API; explicitly not evidence of
            # transport-owner isolation. Production always uses PublicHTTP.
            self.http.close()
        return epoch

    def begin(self,redirect_uri,account=None):
        budget=RequestBudget(None,30);self.verifier.dependency();p=urlsplit(redirect_uri)
        if (p.scheme!='http' or p.hostname!='127.0.0.1' or p.netloc!=f'127.0.0.1:{p.port}'
            or not p.port or p.path!='/auth/callback' or p.query or p.fragment):raise ProviderFault('SIWC_CALLBACK_URI')
        with self.pending_lock:
            if self.closed:raise ProviderFault('SIWC_CLOSED')
            if self.pending is not None and self.pending['expires']>self.clock():raise ProviderFault('SIWC_AUTH_IN_PROGRESS')
        epoch=self._invalidate();guard=lambda:self._check_access(epoch,budget)
        reservation=dict(expires=self.clock()+300,epoch=epoch,starting=True)
        with self.pending_lock:
            guard();self.pending=reservation
        operation=self.http.operation(guard,owner=None) if isinstance(self.http,PublicHTTP) else nullcontext()
        retained=None;pending=None
        try:
            with operation:
                with self._mutation(budget,epoch,kind='begin') as tx:
                    m=self.store.load(budget);tx.check()
                    selected=m['accounts'].get(account) if account is not None else None
                    if account is not None and selected is None:raise ProviderFault('SIWC_ACCOUNT_INVALID')
                    if selected is None and len(m['accounts'])>=8:raise ProviderFault('SIWC_ACCOUNT_LIMIT')
                    if not m['host_id']:m['host_id']='urn:uuid:'+str(uuid.uuid4())
                    # This is the durable owner installation, NOT the earlier reservation.
                    self.store.commit(m,budget,tx=tx,owner=uuid.uuid4().hex)
                    self._remember(m)
                    retained=self.store.secret(selected) if selected and selected['slots'] and not selected['rotation_pending'] else None
                    handle=m.handle(account)
                guard();self._discovery(budget);guard()
                state=secrets.token_urlsafe(32);nonce=secrets.token_urlsafe(32);pkce=secrets.token_urlsafe(64)
                client=selected['client_id'] if selected else (self.retry_client or 'dynamic_agent_client')
                pending=dict(state=state,nonce=nonce,pkce=pkce,redirect=redirect_uri,expires=reservation['expires'],
                    account=account,client=client,subject=selected['subject'] if selected else self.retry_subject,
                    epoch=epoch,handle=handle)
                params=dict(client_id=client,ext_agent_host_id=m['host_id'],response_type='code',redirect_uri=redirect_uri,
                    scope=' '.join(sorted(SCOPES)),resource=API,state=state,nonce=nonce,code_challenge_method='S256',
                    code_challenge=base64.urlsafe_b64encode(hashlib.sha256(pkce.encode('ascii')).digest()).rstrip(b'=').decode('ascii'))
                if client=='dynamic_agent_client':params['agent_name_hint']='Civilization Theatre'
                elif retained:params['id_token_hint']=retained['id_token']
                with self.pending_lock:
                    guard()
                    if self.pending is not reservation:raise ProviderFault('SIWC_AUTH_CANCELLED')
                    self.pending=pending
                guard()
                try:self.opener(AUTHORIZE+'?'+urlencode(params))
                except Exception:raise ProviderFault('SIWC_BROWSER_UNAVAILABLE') from None
                guard()
            return dict(state='connecting',owner_effective=True,request_ack=True,durable_ack=True)
        except BaseException:
            with self.pending_lock:
                if self.pending is reservation or self.pending is pending:self.pending=None
            raise
        finally:retained=None

    def callback(self,query,redirect_uri):
        if type(query) is not str or len(query)>16384:raise ProviderFault('SIWC_CALLBACK_INVALID')
        try:values=parse_qs(query,keep_blank_values=True,strict_parsing=True,max_num_fields=8)
        except ValueError:raise ProviderFault('SIWC_CALLBACK_INVALID') from None
        if (set(values)-{'state','code','client_id','scope','error','error_description','error_uri'}
            or any(len(v)!=1 or not v[0] for v in values.values())):raise ProviderFault('SIWC_CALLBACK_INVALID')
        with self.pending_lock:
            pending=self.pending
            if pending is None or pending.get('starting') or pending['epoch']!=self.epoch or self.closed:raise ProviderFault('SIWC_STATE_INVALID')
            if (pending['expires']<=self.clock() or pending['redirect']!=redirect_uri
                or not secrets.compare_digest(values.get('state',[''])[0],pending['state'])):raise ProviderFault('SIWC_STATE_INVALID')
            self.pending=None
        budget=RequestBudget(None,30);guard=lambda:self._check_access(pending['epoch'],budget)
        operation=self.http.operation(guard,owner=None) if isinstance(self.http,PublicHTTP) else nullcontext()
        tokens=None;record=None
        try:
            with operation:
                guard()
                if 'error' in values:raise ProviderFault('SIWC_AUTH_DENIED')
                client=values.get('client_id',[pending['client']])[0]
                if (not re.fullmatch(r'[A-Za-z0-9_-]{1,200}',client) or client=='dynamic_agent_client'
                    or (pending['client']!='dynamic_agent_client' and client!=pending['client'])):raise ProviderFault('SIWC_CLIENT_INVALID')
                code=values.get('code',[''])[0]
                if not code or len(code)>8192:raise ProviderFault('SIWC_CALLBACK_INVALID')
                with self.pending_lock:
                    guard()
                    if pending['account'] is None:self.retry_client=client
                tokens=self._form(TOKEN,dict(grant_type='authorization_code',client_id=client,code=code,
                    code_verifier=pending['pkce'],redirect_uri=pending['redirect'],resource=API),budget)
                jwks=self.http.request(JWKS,'GET',None,{'Accept':'application/json'},budget)
                record,claims=self._record(tokens,client,pending['nonce'],jwks)
                if pending['subject'] is not None and claims['sub']!=pending['subject']:raise ProviderFault('SIWC_IDENTITY_SWAP')
                with self._mutation(budget,pending['epoch'],kind='callback') as tx:
                    m=self.store.load(budget);tx.check();self.store.assert_handle(m,pending['handle']);key=pending['account']
                    if key is None:
                        self.retry_subject=claims['sub']
                        if any(a['client_id']==client for a in m['accounts'].values()):raise ProviderFault('SIWC_CLIENT_CONFLICT')
                        if len(m['accounts'])>=8:raise ProviderFault('SIWC_ACCOUNT_LIMIT')
                        key='civilization-theatre/service/'+uuid.uuid4().hex;m['accounts'][key]=empty_account(client,claims['sub'])
                    elif key not in m['accounts'] or (m['accounts'][key]['client_id'],m['accounts'][key]['subject'])!=(client,claims['sub']):raise ProviderFault('SIWC_IDENTITY_SWAP')
                    m['active']=key;self.store.replace_secret(m,key,record,budget,tx=tx);self._remember(m)
                    self.last_error=None;self.retry_client=None;self.retry_subject=None
                # No post-F epoch guard pretending a won commit was cancelled.
                return dict(state='authorized_not_verified',real_call_verified=False,**tx.status())
        except ProviderFault as e:
            self.last_error=e.code
            ack=self._transaction_status()
            if ack['outcome']=='commit_unknown':return dict(state='account_mutation_pending',real_call_verified=False,**ack)
            raise
        except Exception:
            self.last_error='SIWC_AUTH_ERROR';raise ProviderFault('SIWC_AUTH_ERROR') from None
        finally:pending.clear();tokens=None;record=None;values.clear()

    @contextmanager
    def access(self,binding,budget):
        epoch=self.epoch
        if self.closed:raise ProviderFault('SIWC_UNCONFIGURED')
        # A previously confirmed snapshot fixes revision/generation before L wait.
        handle=self._intent(budget,binding)
        self._check_access(epoch,budget);self.verifier.dependency()
        guard=lambda:self._check_access(epoch,budget)
        operation=self.http.operation(guard) if isinstance(self.http,PublicHTTP) else nullcontext()
        record=None
        with operation,self.store.locked(budget):
            guard();m=self.store.load(budget);self.store.assert_handle(m,handle,rotation=True)
            a=m['accounts'].get(binding)
            if a is None or binding!=m['active'] or not a['slots']:raise ProviderFault('SIWC_UNCONFIGURED')
            if a['rotation_pending']:raise ProviderFault('SIWC_REAUTHORIZATION_REQUIRED')
            record=self.store.secret(a)
            if (record.get('client_id'),record.get('subject'))!=(a['client_id'],a['subject']):raise ProviderFault('SIWC_IDENTITY_SWAP')
            if not SCOPES.issubset(record.get('scopes',[])):raise ProviderFault('SIWC_SCOPE_REQUIRED')
            if type(record.get('expires_at')) not in (int,float):raise ProviderFault('SIWC_TOKEN_INVALID')
            if record['expires_at']<=self.clock():
                guard();a['rotation_pending']=True;self.store.commit(m,budget);self._remember(m)
                try:
                    guard();tokens=self._form(TOKEN,dict(grant_type='refresh_token',client_id=a['client_id'],
                        refresh_token=record['refresh_token'],resource=API),budget)
                    guard();jwks=self.http.request(JWKS,'GET',None,{'Accept':'application/json'},budget) if 'id_token' in tokens else None
                    replacement,claims=self._record(tokens,a['client_id'],None,jwks,record)
                    if claims['sub']!=a['subject']:raise ProviderFault('SIWC_IDENTITY_SWAP')
                    guard()
                    # Safe-save uses original budget but NOT cancelled request guard;
                    # same L pins owner/generation/active throughout rotation.
                    self.store.replace_secret(m,binding,replacement,budget);self._remember(m);record=replacement
                    guard()
                except ProviderFault as fault:
                    if getattr(fault,'upstream_code',None) in ('invalid_grant','invalid_refresh_token','token_expired',
                        'refresh_token_expired','refresh_token_invalidated','refresh_token_reused'):
                        self.store.clear(m,binding,budget);self._remember(m)
                    raise
                except Exception:raise ProviderFault('SIWC_AUTH_ERROR') from None
            self._remember(m);guard()
            try:
                yield record['access_token']
                guard()
                # Reentrant local mutation is allowed by L; reject its old lease.
                self.store.assert_handle(self.store.load(budget),m.handle(binding))
            finally:record=None

    def select(self,key):
        budget=RequestBudget(None,10);handle=self._intent(budget);epoch=self._invalidate()
        self.retry_client=None;self.retry_subject=None
        with self._mutation(budget,epoch,kind='select') as tx:
            m=self.store.load(budget);tx.check();self.store.assert_handle(m,handle)
            if key not in m['accounts']:raise ProviderFault('SIWC_ACCOUNT_INVALID')
            m['active']=key;self.store.commit(m,budget,tx=tx);self._remember(m)
        return self.status()

    def welcome(self):
        budget=RequestBudget(None,10);handle=self._intent(budget);epoch=self.epoch
        with self._mutation(budget,epoch,kind='welcome') as tx:
            m=self.store.load(budget);tx.check();self.store.assert_handle(m,handle);key=handle[2]
            if key is None or not m['accounts'][key]['slots']:raise ProviderFault('SIWC_UNCONFIGURED')
            m['accounts'][key]['welcome_seen']=True;self.store.commit(m,budget,tx=tx);self._remember(m)
        return self.status()

    def logout(self):
        budget=RequestBudget(None,15)
        with self.lifecycle_lock:previous=self._view
        handle=previous.handle() if previous is not None else self._intent(budget)
        epoch=self._invalidate();confirmed=False;record=None
        self.retry_client=None;self.retry_subject=None
        try:
            with self._mutation(budget,epoch,kind='logout') as tx:
                m=self.store.load(budget);tx.check();self.store.assert_handle(m,handle,rotation=True);key=handle[2]
                if key is not None and m['accounts'][key]['slots']:
                    # Capture ONLY the fixed old target. Local disable precedes
                    # remote send; no later lookup of active chooses its target.
                    try:record=self.store.secret(m['accounts'][key])
                    except ProviderFault:record=None
                    self.store.clear(m,key,budget,tx=tx)
                else:self.store.commit(m,budget,tx=tx)
                self._remember(m)
            local_complete=tx.status()['durable_ack']
            if record is not None:
                try:
                    # The immutable fixed record is only for revocation now.
                    # L is released; a newer login cannot retarget this request.
                    d=self._discovery(budget);endpoint=d.get('revocation_endpoint');p=urlsplit(endpoint) if type(endpoint) is str else None
                    if p is None or p.scheme!='https' or p.netloc!='auth.openai.com' or p.query or p.fragment:raise ProviderFault('SIWC_DISCOVERY_INVALID')
                    for retry in range(2):
                        try:
                            self._form(endpoint,dict(token=record['refresh_token'],token_type_hint='refresh_token',client_id=record['client_id']),budget)
                            confirmed=True;break
                        except ProviderFault as fault:
                            if retry or not (fault.code=='SIWC_TRANSPORT' or (getattr(fault,'http_status',0) or 0)>=500):raise
                            time.sleep(min(.2,budget.remaining()))
                except Exception:pass
            return dict(self.status(),local_logout_complete=local_complete,remote_revocation_confirmed=confirmed)
        finally:record=None

    def cancel(self):
        self._invalidate();return self._transaction_status()

    def close(self):
        self._invalidate(permanent=True);return dict(self._transaction_status(),instance_closed=True)

    def _discovery(self,budget):
        d=self.http.request(DISCOVERY,'GET',None,{'Accept':'application/json'},budget)
        if type(d) is not dict or any(d.get(k)!=v for k,v in (
            ('issuer',ISSUER),('authorization_endpoint',AUTHORIZE),('token_endpoint',TOKEN),('jwks_uri',JWKS))):
            raise ProviderFault('SIWC_DISCOVERY_INVALID')
        return d

    def _form(self,url,fields,budget):
        return self.http.request(url,'POST',urlencode(fields).encode('ascii'),
            {'Accept':'application/json','Content-Type':'application/x-www-form-urlencoded'},budget)

    def _record(self,tokens,client,nonce,jwks,previous=None):
        if type(tokens) is not dict:raise ProviderFault('SIWC_TOKEN_INVALID')
        scopes=tokens.get('scope')
        if scopes is None and previous is not None:scopes=' '.join(previous['scopes'])
        if type(scopes) is not str or not SCOPES.issubset(scopes.split()):raise ProviderFault('SIWC_SCOPE_REQUIRED')
        for field in ('access_token','refresh_token'):
            v=tokens.get(field)
            if type(v) is not str or not 1<=len(v)<=32768 or any(ord(c)<33 or ord(c)>126 for c in v):
                raise ProviderFault('SIWC_TOKEN_INVALID')
        if tokens.get('token_type','').lower()!='bearer':raise ProviderFault('SIWC_TOKEN_INVALID')
        seconds=tokens.get('expires_in')
        if type(seconds) is not int or not 1<=seconds<=86400:raise ProviderFault('SIWC_TOKEN_INVALID')
        idtoken=tokens.get('id_token')
        if idtoken is None and previous is not None:
            # Refresh response need not replace ID token: identity remains the
            # verified session; replacement ID token, if supplied, MUST verify.
            claims={'sub':previous['subject']};idtoken=previous['id_token']
        else:
            if type(idtoken) is not str or not 1<=len(idtoken)<=32768:raise ProviderFault('SIWC_IDENTITY_INVALID')
            claims=self.verifier.verify(idtoken,client,nonce,jwks,self.clock())
        record=dict(client_id=client,subject=claims['sub'],id_token=idtoken,access_token=tokens['access_token'],
                    refresh_token=tokens['refresh_token'],scopes=sorted(set(scopes.split())),
                    expires_at=self.clock()+seconds,earliest_refresh_at=tokens.get('earliest_refresh_at'))
        return record,claims

    def _check_access(self,epoch,budget):
        budget.check()
        if self.closed or self.epoch!=epoch:raise ProviderFault('SIWC_AUTH_CANCELLED')

    def models(self,binding,budget):
        with self.access(binding,budget) as token:
            result=self.http.request(API+'/models','GET',None,{'Accept':'application/json','Authorization':'Bearer '+token},budget)
            rows=result.get('models') if type(result) is dict else None
            if type(rows) is not list or len(rows)>512:raise ProviderFault('SIWC_MODELS_INVALID')
            out=[];seen=set()
            for row in rows:
                if type(row) is not dict:raise ProviderFault('SIWC_MODELS_INVALID')
                if row.get('visibility')!='list':continue
                slug=row.get('slug');label=row.get('display_name')
                if any(type(v) is not str or not 1<=len(v)<=256 or any(ord(c)<32 for c in v) for v in (slug,label)):
                    raise ProviderFault('SIWC_MODELS_INVALID')
                if token in slug or token in label:raise ProviderFault('SIWC_SENSITIVE_RESPONSE')
                if slug in seen:raise ProviderFault('SIWC_MODELS_INVALID')
                seen.add(slug);out.append({'slug':slug,'display_name':label})
            return out

