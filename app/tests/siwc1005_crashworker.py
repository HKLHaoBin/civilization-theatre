"""Future owned synthetic child only. No HTTP, identity, crypto or WinCred.
Invoked only by test_siwc1005_account after separate runtime authorization.
"""
import json
import os
from pathlib import Path
import sys
from app.real_provider import RequestBudget
from app.siwc_storage import SessionStore, REF

KEY='civilization-theatre/service/'+'1'*32

class FileSecret:
    def __init__(self,raw):self.raw=bytearray(raw)
    def reveal_bytes(self):return bytes(self.raw)
    def close(self):
        for i in range(len(self.raw)):self.raw[i]=0

class FileVault:
    def __init__(self,root):
        self.root=Path(root)
        if (self.root/'OWNED-SYNTHETIC-1005').read_text()!='synthetic only':raise ValueError('not an owned fixture')
    def __call__(self):return self
    def path(self,ref):
        if REF.fullmatch(ref) is None:raise ValueError('not an owned application ref')
        return self.root/('synthetic-slot-'+ref.rsplit('/',1)[-1])
    def write(self,ref,raw):
        p=self.path(ref)
        with p.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    def read(self,ref):
        p=self.path(ref)
        return FileSecret(p.read_bytes()) if p.exists() else None
    def delete(self,ref):
        p=self.path(ref)
        if p.exists():p.unlink()

def main():
    if len(sys.argv)!=3:raise ValueError('owned root and stage required')
    root=Path(sys.argv[1]);stage=sys.argv[2]
    allowed={'prepared_replaced','vault_written','committed_replaced','committed_readback','unlock','gc'}
    if stage not in allowed:raise ValueError('unknown fixture stage')
    vault=FileVault(root);store=SessionStore(root/'session.json',vault);budget=RequestBudget(None,30)
    with store.locked(budget):
        m=store.load(budget);m['host_id']='urn:uuid:synthetic-owned-1005'
        m['accounts'][KEY]=dict(client_id='oaiapp_SYNTHETIC',subject='SYNTHETIC',slots=[],size=0,
            sha256='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',rotation_pending=False,welcome_seen=False)
        m['active']=KEY
        store.replace_secret(m,KEY,dict(access_token='SYNTHETIC_OLD'),budget)
    armed=[True]
    def hold(label):
        if label==stage and armed[0]:
            armed[0]=False;print('GATE '+stage,flush=True)
            if sys.stdin.readline()!='RELEASE\n':raise RuntimeError('owned process gate not released')
    store._edge=hold
    with store.locked(budget):
        m=store.load(budget);store.replace_secret(m,KEY,dict(access_token='SYNTHETIC_NEW'),budget)

if __name__=='__main__':main()
