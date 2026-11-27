"""Game-owned non-secret metadata. Status never creates/reads a native vault.
Saving never calls a model. Credentials get fresh game-owned refs only; an old
credential is never deleted or overwritten. A failed metadata replace may leave
an unused NEW ref, which is safer than deleting or modifying an old credential.
"""
from contextlib import nullcontext
import hashlib
import json
import os
from pathlib import Path
import threading
import uuid
from urllib.parse import urlsplit
from .service_descriptors import validate,public_summary
from .usage_accounting import UsageJournal,UsageJournalError


def default_credential_factory():
    from .credential_adapter import WindowsCredentialAdapter
    return WindowsCredentialAdapter()


def checked(config):
    config=validate(config)
    for s in config.values():
        url=s['base_url'];p=urlsplit(url)
        if '?' in url or '#' in url or len(url)>2048:
            raise ValueError('INVALID_SERVICE_DESCRIPTOR')
        if s['provider']=='openai-chat-https':
            if p.scheme!='https' or s['credential_ref'] is None:
                raise ValueError('REAL_CREDENTIAL_REQUIRED')
        elif s['provider']=='openai-chat-loopback':
            if p.scheme!='http' or p.hostname not in ('127.0.0.1','::1') or s['credential_ref'] is not None:
                raise ValueError('INVALID_LOOPBACK_DESCRIPTOR')
        elif s['provider']=='openai-siwc-responses':
            if url!='https://api.openai.com/v1':
                raise ValueError('INVALID_SIWC_ENDPOINT')
        else:
            raise ValueError('UNSUPPORTED_REAL_PROTOCOL')
    return config


def fingerprint(config):
    return hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode('utf-8')).hexdigest()


class SettingsStore:
    def __init__(self,path,*,credential_factory=None):
        self.path=Path(path)
        self.usage_path=self.path.with_name('provider-usage.jsonl')
        self.usage_journal=UsageJournal(self.usage_path)
        self.credential_factory=credential_factory or default_credential_factory
        self.lock=threading.RLock()
        from .siwc_oauth import SIWCSession
        self.siwc=SIWCSession(self.path.with_name('siwc-sessions.json'),self.credential_factory)

    def snapshot(self):
        with self.lock:
            if not self.path.exists():return None
            if self.path.is_symlink() or self.path.stat().st_size>16384:
                raise ValueError('INVALID_SETTINGS_FILE')
            try:
                envelope=json.loads(self.path.read_text(encoding='utf-8'))
                if type(envelope) is not dict or set(envelope)!={'schema_version','services'} or envelope['schema_version']!=1:
                    raise ValueError
                return checked(envelope['services'])
            except Exception:
                raise ValueError('INVALID_SETTINGS_FILE') from None

    def status(self):
        config=self.snapshot()
        return dict(state='metadata_configured' if config else 'unconfigured',
                    services=public_summary(config) if config else None,
                    credential_status='unknown',connection_verified=False,real_call_verified=False,
                    sdk_used=False,cost_status='UNKNOWN',siwc=self.siwc.status())

    def save(self,payload):
        if type(payload) is not dict or set(payload)!={'services','secrets'}:
            raise ValueError('INVALID_SETTINGS')
        raw=payload['services'];secrets=payload['secrets']
        # Browser cannot select an existing credential from any namespace.
        incoming=validate(raw)
        if any(s['credential_ref'] is not None for s in incoming.values()):
            raise ValueError('CREDENTIAL_REF_NOT_WRITABLE')
        if (type(secrets) is not dict or not set(secrets)<={'roles','dm'}
                or any(type(v) is not str or not 1<=len(v.encode('utf-8'))<=2560
                       or any(ord(c)<32 or ord(c)==127 for c in v) for v in secrets.values())):
            raise ValueError('INVALID_SECRET_INPUT')
        binding_context=self.siwc.binding_snapshot() if any(s['provider']=='openai-siwc-responses' for s in incoming.values()) else nullcontext(None)
        # Global order: account L -> settings. Never wait for L while holding
        # settings.lock, including the non-SIWC return-status path.
        with binding_context as siwc_binding:
            with self.lock:
                old=self.snapshot()
                pending=[]
                for kind,s in incoming.items():
                    secret=secrets.get(kind)
                    if s['provider']=='openai-chat-https':
                        if secret is not None:
                            s['credential_ref']='civilization-theatre/service/'+uuid.uuid4().hex
                            pending.append((s['credential_ref'],secret))
                        elif old and old[kind]['provider']==s['provider'] and old[kind]['base_url']==s['base_url']:
                            s['credential_ref']=old[kind]['credential_ref']
                    elif s['provider']=='openai-siwc-responses':
                        if secret is not None:raise ValueError('SIWC_SECRET_INPUT_FORBIDDEN')
                        s['credential_ref']=siwc_binding
                    elif secret is not None:
                        raise ValueError('LOOPBACK_SECRET_FORBIDDEN')
                incoming=checked(incoming)  # Validate EVERY field before any native call.
                for ref,secret in pending:
                    owned=bytearray(secret.encode('utf-8'))
                    try:
                        self.credential_factory().write(ref,owned)
                    except Exception:
                        raise ValueError('CREDENTIAL_SAVE_FAILED') from None
                    finally:
                        for i in range(len(owned)):owned[i]=0
                self.path.parent.mkdir(parents=True,exist_ok=True)
                temporary=self.path.with_name(self.path.name+'.'+uuid.uuid4().hex+'.pending')
                try:
                    with temporary.open('x',encoding='utf-8',newline='\n') as stream:
                        stream.write(json.dumps({'schema_version':1,'services':incoming},ensure_ascii=False,sort_keys=True)+'\n')
                        stream.flush();os.fsync(stream.fileno())
                    os.replace(temporary,self.path)
                    if self.snapshot()!=incoming:raise ValueError('SETTINGS_READBACK_FAILED')
                finally:
                    if temporary.exists():temporary.unlink()
            return self.status()

    def usage(self,event):
        # Fixed sanitized shape only; no prompts/URL/model/errors/key/ref/response.
        with self.lock:
            try:
                return self.usage_journal.append(event)
            except UsageJournalError as error:
                raise ValueError(str(error)) from None
