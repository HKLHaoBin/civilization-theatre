"""App-owned credential transactions, schema1005 PREPARED/COMMITTED authority.
All public loads/recovery/mutations use the SAME legacy .lock byte-range lock.
Supported schema1 writers lock before load, hence reject schema1005 before write;
a writer already holding L completes before migration can acquire L. Direct raw
commit by arbitrary same-permission programs is not a supported legacy writer.
fsync(file), atomic replace and exact readback target process termination/cold
reopen only; no claim about power loss, directory durability or machine reboot.
"""
from contextlib import contextmanager
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import threading
import time
import uuid
from .real_provider import ProviderFault, RequestBudget
from .siwc_transactions import AuthTransaction

REF=re.compile(r'civilization-theatre/service/[0-9a-f]{32}')
CHUNK=2400
MAX_SECRET=65536
MAX_FILE=262144

class Snapshot(dict):
    def __init__(self,value,revision=0,owner=None,generations=None,last_txid=None):
        super().__init__(copy.deepcopy(value));self.revision=revision;self.owner=owner
        self.generations=dict(generations or {});self.last_txid=last_txid
    def handle(self,key=None):
        key=self['active'] if key is None else key
        a=self['accounts'].get(key)
        return (self.revision,self.owner,key,self.generations.get(key,0),self['active'],
                a['client_id'] if a else None,a['subject'] if a else None,bool(a and a['slots']))

class SessionStore:
    def __init__(self,path,credential_factory,*,file_lock=None):
        self.path=Path(path);self.factory=credential_factory;self.thread_lock=threading.RLock()
        self.file_lock=file_lock;self.local=threading.local()

    def _edge(self,label):
        """Source test seam immediately adjacent to real operations; no authority."""

    @contextmanager
    def locked(self,budget):
        if getattr(self.local,'depth',0):
            self.local.depth+=1
            try:budget.check();yield
            finally:self.local.depth-=1
            return
        while not self.thread_lock.acquire(timeout=.025):budget.check()
        try:
            budget.check()
            if self.file_lock is not None:
                with self.file_lock(budget):
                    self.local.depth=1
                    try:yield
                    finally:self.local.depth=0;self._edge('unlock')
                return
            try:import msvcrt
            except ImportError:raise ProviderFault('SIWC_PLATFORM_UNCONFIGURED') from None
            self.path.parent.mkdir(parents=True,exist_ok=True)
            lockpath=self.path.with_suffix('.lock')
            if self.path.parent.is_symlink() or lockpath.is_symlink():raise ProviderFault('SIWC_SESSION_FILE')
            with lockpath.open('a+b') as f:
                if f.seek(0,2)==0:f.write(b'0');f.flush()
                acquired=False
                try:
                    while not acquired:
                        budget.check();f.seek(0)
                        try:msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1);acquired=True
                        except OSError:time.sleep(min(.025,budget.remaining()))
                    budget.check();self.local.depth=1;yield
                finally:
                    self.local.depth=0
                    if acquired:
                        self._edge('unlock');f.seek(0);msvcrt.locking(f.fileno(),msvcrt.LK_UNLCK,1)
        finally:self.thread_lock.release()

    def _held(self):
        if not getattr(self.local,'depth',0):raise ProviderFault('SIWC_STORE_LOCK_REQUIRED')

    @staticmethod
    def _value(v):
        if type(v) is not dict or set(v)!={'schema','host_id','active','accounts'} or v['schema']!=1:raise ValueError()
        if v['host_id'] is not None and (type(v['host_id']) is not str or not v['host_id'].startswith('urn:uuid:') or len(v['host_id'])>100):raise ValueError()
        accounts=v['accounts']
        if type(accounts) is not dict or len(accounts)>8:raise ValueError()
        if v['active'] is not None and v['active'] not in accounts:raise ValueError()
        for key,a in accounts.items():
            if type(key) is not str or REF.fullmatch(key) is None or type(a) is not dict:raise ValueError()
            if set(a)!={'client_id','subject','slots','sha256','size','rotation_pending','welcome_seen'}:raise ValueError()
            if not all(type(a[k]) is str and 1<=len(a[k])<=512 for k in ('client_id','subject')):raise ValueError()
            if a['client_id']=='dynamic_agent_client':raise ValueError()
            if type(a['rotation_pending']) is not bool or type(a['welcome_seen']) is not bool:raise ValueError()
            slots=a['slots']
            if type(slots) is not list or len(slots)>28 or any(type(s) is not str or REF.fullmatch(s) is None for s in slots) or len(set(slots))!=len(slots):raise ValueError()
            if type(a['size']) is not int or not 0<=a['size']<=MAX_SECRET:raise ValueError()
            if type(a['sha256']) is not str or re.fullmatch('[0-9a-f]{64}',a['sha256']) is None:raise ValueError()
            if bool(slots)!=bool(a['size']):raise ValueError()
        return v

    @classmethod
    def _stable(cls,s):
        if type(s) is not dict or set(s)!={'revision','owner','generations','value','last_txid'}:raise ValueError()
        if type(s['revision']) is not int or not 0<=s['revision']<2**63:raise ValueError()
        for k in ('owner','last_txid'):
            if s[k] is not None and (type(s[k]) is not str or re.fullmatch('[0-9a-f]{32}',s[k]) is None):raise ValueError()
        cls._value(s['value']);g=s['generations']
        if type(g) is not dict or set(g)!=set(s['value']['accounts']) or any(type(n) is not int or not 0<=n<2**63 for n in g.values()):raise ValueError()
        return s

    @staticmethod
    def _refs(value):return {r for a in value['accounts'].values() for r in a['slots']}

    @staticmethod
    def _envelope(state,stable,*,txid=None,candidate=None,planned=(),gc=()):
        return dict(schema=1005,state=state,stable=stable,txid=txid,candidate=candidate,planned=list(planned),gc=sorted(set(gc)))

    def _read(self):
        self._held()
        if not self.path.exists():return None
        if self.path.is_symlink() or self.path.stat().st_size>MAX_FILE:raise ProviderFault('SIWC_SESSION_FILE')
        try:
            def pairs(items):
                out={}
                for k,v in items:
                    if k in out:raise ValueError()
                    out[k]=v
                return out
            v=json.loads(self.path.read_text(encoding='utf-8'),object_pairs_hook=pairs)
            if type(v) is not dict:raise ValueError()
            if v.get('schema')==1:
                self._value(v)
                s=dict(revision=0,owner=None,generations={k:0 for k in v['accounts']},value=v,last_txid=None)
                return self._envelope('COMMITTED',s)
            if set(v)!={'schema','state','stable','txid','candidate','planned','gc'} or v['schema']!=1005:raise ValueError()
            self._stable(v['stable'])
            for name,limit in (('planned',28),('gc',1024)):
                refs=v[name]
                if type(refs) is not list or len(refs)>limit or any(type(r) is not str or REF.fullmatch(r) is None for r in refs) or len(set(refs))!=len(refs):raise ValueError()
            if self._refs(v['stable']['value']) & set(v['gc']):raise ValueError()
            if v['state']=='COMMITTED':
                if v['candidate'] is not None or v['txid'] is not None or v['planned']:raise ValueError()
            elif v['state']=='PREPARED':
                self._stable(v['candidate'])
                if type(v['txid']) is not str or re.fullmatch('[0-9a-f]{32}',v['txid']) is None:raise ValueError()
                if v['candidate']['revision']!=v['stable']['revision']+1 or v['candidate']['last_txid']!=v['txid']:raise ValueError()
                if self._refs(v['stable']['value']) & set(v['planned']):raise ValueError()
                if self._refs(v['candidate']['value'])-self._refs(v['stable']['value'])!=set(v['planned']):raise ValueError()
                if self._refs(v['candidate']['value']) & set(v['gc']):raise ValueError()
            else:raise ValueError()
            return v
        except (ValueError,TypeError,KeyError,UnicodeError):raise ProviderFault('SIWC_SESSION_FILE') from None

    def _temp(self,value,stage):
        self._held();self.path.parent.mkdir(parents=True,exist_ok=True)
        if self.path.is_symlink() or self.path.parent.is_symlink():raise ProviderFault('SIWC_SESSION_FILE')
        raw=(json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False)+'\n').encode('utf-8')
        if len(raw)>MAX_FILE:raise ProviderFault('SIWC_SESSION_LIMIT')
        temp=self.path.with_name(self.path.name+'.'+uuid.uuid4().hex+'.pending')
        try:
            with temp.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno());self._edge(stage+'_fsync')
            return temp,raw
        except BaseException:
            if temp.exists():temp.unlink()
            raise

    def _publish(self,temp,raw,stage):
        try:
            os.replace(temp,self.path);self._edge(stage+'_replaced')
            # File fsync and exact readback, still under L. Not machine power-loss D.
            with self.path.open('r+b') as f:
                f.flush();os.fsync(f.fileno());self._edge(stage+'_flush');actual=f.read(MAX_FILE+1)
            if actual!=raw:raise ProviderFault('SIWC_COMMIT_UNKNOWN')
            self._edge(stage+'_readback')
        finally:
            if temp.exists():temp.unlink()

    def _write(self,value,stage):
        temp,raw=self._temp(value,stage);self._publish(temp,raw,stage)

    def _recover(self):
        self._held();e=self._read()
        if e is None:
            s=dict(revision=0,owner=None,generations={},value=dict(schema=1,host_id=None,active=None,accounts={}),last_txid=None)
            return self._envelope('COMMITTED',s)
        if e['state']=='PREPARED':
            e=self._envelope('COMMITTED',e['stable'],gc=e['gc']+e['planned'])
            self._write(e,'recovery')
        # On first schema1 read, publish new schema while holding the legacy L.
        # Empty missing stores need not write simply to display unconfigured.
        elif self.path.exists():
            current=json.loads(self.path.read_text(encoding='utf-8'))
            if current.get('schema')==1:self._write(e,'migration')
        return e

    def load(self,budget=None):
        if getattr(self.local,'writing',False):raise ProviderFault('SIWC_MUTATION_PENDING')
        budget=budget or RequestBudget(None,2)
        if not getattr(self.local,'depth',0):
            with self.locked(budget):return self.load(budget)
        e=self._recover();s=e['stable'];self._edge('loaded')
        return Snapshot(s['value'],s['revision'],s['owner'],s['generations'],s['last_txid'])

    def assert_handle(self,snapshot,handle,*,rotation=False):
        current=snapshot.handle(handle[2])
        if current==handle:return
        # A new owner (including same-account reauth), active identity change,
        # revocation or unresolved refresh is NEVER a token-rotation retry.
        a=snapshot['accounts'].get(handle[2])
        if (rotation and current[1:3]==handle[1:3] and current[4:]==handle[4:]
            and handle[7] and current[7] and current[0]>handle[0]
            and current[3]>handle[3] and a and not a['rotation_pending']):return
        raise ProviderFault('SIWC_STALE_OWNER_CANCELLED')

    def reconcile(self,tx,budget):
        # Caller proves writer exit, owns L, and retains the ORIGINAL budget.
        self._held();budget.check()
        if not tx.recovery_ready():raise ProviderFault('SIWC_MUTATION_PENDING')
        e=self._read();decision=None;plan=tx.plan
        if e is not None and plan is not None:
            old,candidate=plan
            if (e['state']=='PREPARED' and e['txid']==tx.id
                and e['stable']==old and e['candidate']==candidate):
                decision='CANCELLED'
            elif e['state']=='COMMITTED' and e['stable']==candidate and candidate['last_txid']==tx.id:
                decision='COMMITTED'
        # Validation/recovery is authoritative even when the historical tx was
        # overwritten. An unrelated COMMITTED is safe view, NOT proof of ACK.
        budget.check();e=self._recover();budget.check()
        if decision=='COMMITTED':self._write(e,'reconcile')
        budget.check();e=self._gc(e);budget.check();s=e['stable']
        return Snapshot(s['value'],s['revision'],s['owner'],s['generations'],s['last_txid']),decision

    def _gc(self,e):
        self._held()
        if not e['gc']:return e
        self._edge('gc');remaining=[];live=self._refs(e['stable']['value'])
        # PREPARED never reaches GC: recovery first preserves its old snapshot.
        try:vault=self.factory()
        except Exception:return e
        for ref in e['gc']:
            if ref in live:raise ProviderFault('SIWC_SESSION_FILE')
            try:vault.delete(ref)
            except Exception:remaining.append(ref)
        if remaining!=e['gc']:
            e=self._envelope('COMMITTED',e['stable'],gc=remaining);self._write(e,'gc')
        return e

    def commit(self,value,budget=None,*,tx=None,owner=None):
        return self._change(value,budget or RequestBudget(None,10),tx=tx,owner=owner)

    def _change(self,value,budget,*,tx=None,owner=None,key=None,record=None,clear=False):
        if not getattr(self.local,'depth',0):
            with self.locked(budget):return self._change(value,budget,tx=tx,owner=owner,key=key,record=record,clear=clear)
        if getattr(self.local,'writing',False):raise ProviderFault('SIWC_MUTATION_PENDING')
        own=tx is None;tx=tx or AuthTransaction(budget.check,kind='safe-save')
        raw=None;temp=None;prepared=False;finalizing=False
        self.local.writing=True
        try:
            tx.check();old=self._gc(self._recover());s=old['stable']
            if not isinstance(value,Snapshot) or (value.revision,value.owner)!=(s['revision'],s['owner']):raise ProviderFault('SIWC_STALE_OWNER_CANCELLED')
            candidate=copy.deepcopy(dict(value));generations=dict(s['generations']);planned=[]
            generations={k:generations.get(k,0) for k in candidate['accounts']}
            if record is not None:
                raw=bytearray(json.dumps(record,sort_keys=True,ensure_ascii=False,allow_nan=False).encode('utf-8'))
                if not 1<=len(raw)<=MAX_SECRET:raise ProviderFault('SIWC_CREDENTIAL_LIMIT')
                planned=['civilization-theatre/service/'+uuid.uuid4().hex for _ in range(0,len(raw),CHUNK)]
                candidate['accounts'][key]=dict(candidate['accounts'][key],slots=planned,size=len(raw),sha256=hashlib.sha256(raw).hexdigest(),rotation_pending=False)
                generations[key]+=1
            if clear:
                candidate['accounts'][key]=dict(candidate['accounts'][key],slots=[],size=0,sha256=hashlib.sha256(b'').hexdigest(),rotation_pending=False)
                generations[key]+=1
            self._value(candidate)
            new=dict(revision=s['revision']+1,owner=owner if owner is not None else s['owner'],generations=generations,value=candidate,last_txid=tx.id)
            self._stable(new);tx.bind_plan(s,new)
            p=self._envelope('PREPARED',s,txid=tx.id,candidate=new,planned=planned,gc=old['gc'])
            tx.check();self._write(p,'prepared');prepared=True;tx.check()
            if raw is not None:
                tx.check();vault=self.factory();self._edge('vault_factory');tx.check()
                for offset,ref in zip(range(0,len(raw),CHUNK),planned):
                    tx.check();chunk=raw[offset:offset+CHUNK]
                    try:vault.write(ref,chunk);self._edge('vault_written')
                    finally:
                        for i in range(len(chunk)):chunk[i]=0
                    tx.check();check=vault.read(ref)
                    if check is None:raise ProviderFault('SIWC_CREDENTIAL_SAVE_FAILED')
                    try:
                        if check.reveal_bytes()!=bytes(raw[offset:offset+CHUNK]):raise ProviderFault('SIWC_CREDENTIAL_SAVE_FAILED')
                        self._edge('vault_readback')
                    finally:check.close()
                    tx.check()
            retired=self._refs(s['value'])-self._refs(candidate)
            final=self._envelope('COMMITTED',new,gc=old['gc']+sorted(retired))
            temp,encoded=self._temp(final,'committed');tx.check();tx.finalize();finalizing=True
            self._edge('finalizing');self._publish(temp,encoded,'committed');temp=None
            value.clear();value.update(copy.deepcopy(candidate));value.revision=new['revision'];value.owner=new['owner'];value.generations=generations;value.last_txid=tx.id
            self._gc(final)
            if own:tx.finish('COMMITTED')
            return value
        except BaseException as fault:
            if not finalizing:
                try:
                    # Read even if PREPARED write returned unknown; no blind ref deletion.
                    recovered=self._recover();self._gc(recovered)
                    if tx.status()['outcome']=='cancel_won_pending':tx.finish('CANCELLED' if own else 'CANCEL_WON')
                except BaseException:tx.finish('UNKNOWN')
            else:tx.finish('UNKNOWN')
            if isinstance(fault,ProviderFault):raise
            raise ProviderFault('SIWC_COMMIT_UNKNOWN' if finalizing else 'SIWC_CREDENTIAL_SAVE_FAILED') from None
        finally:
            self.local.writing=False
            if temp is not None and temp.exists():temp.unlink()
            if raw is not None:
                for i in range(len(raw)):raw[i]=0

    def replace_secret(self,metadata,key,record,budget,*,tx=None):
        return self._change(metadata,budget,tx=tx,key=key,record=record)

    def clear(self,metadata,key,budget=None,*,tx=None):
        return self._change(metadata,budget or RequestBudget(None,15),tx=tx,key=key,clear=True)

    def secret(self,account,budget=None):
        # Even diagnostic/legacy callers cannot read a stale detached snapshot.
        # Production access additionally checks active/rotation/generation and
        # holds L across the entire HTTP lease, not merely this credential read.
        if getattr(self.local,'writing',False):raise ProviderFault('SIWC_MUTATION_PENDING')
        if not getattr(self.local,'depth',0):
            with self.locked(budget or RequestBudget(None,2)):return self.secret(account,budget)
        current=self._recover()['stable']['value']['accounts']
        if not any(value==account for value in current.values()):raise ProviderFault('SIWC_STALE_OWNER_CANCELLED')
        if not account['slots']:raise ProviderFault('SIWC_UNCONFIGURED')
        owned=[]
        try:
            vault=self.factory();parts=[]
            for ref in account['slots']:
                if REF.fullmatch(ref) is None:raise ProviderFault('SIWC_CREDENTIAL_INVALID')
                secret=vault.read(ref)
                if secret is None:raise ProviderFault('SIWC_CREDENTIAL_UNAVAILABLE')
                owned.append(secret);part=secret.reveal_bytes()
                if not 1<=len(part)<=CHUNK:raise ProviderFault('SIWC_CREDENTIAL_INVALID')
                parts.append(part)
            raw=b''.join(parts)
            if len(raw)!=account['size'] or hashlib.sha256(raw).hexdigest()!=account['sha256']:raise ProviderFault('SIWC_CREDENTIAL_INVALID')
            value=json.loads(raw)
            if type(value) is not dict:raise ProviderFault('SIWC_CREDENTIAL_INVALID')
            return value
        except ProviderFault:raise
        except Exception:raise ProviderFault('SIWC_CREDENTIAL_UNAVAILABLE') from None
        finally:
            failed=False
            for secret in owned:
                try:secret.close()
                except Exception:failed=True
            if failed:raise ProviderFault('SIWC_CREDENTIAL_CLOSE_FAILED') from None
