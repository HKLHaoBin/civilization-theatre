"""SYNTHETIC ONLY. No real identity, signature, JWKS, account or credential.
Fake crypto implements NO AUTHENTICATION and is never selected by production.
"""
from contextlib import contextmanager
import copy
import json
from pathlib import Path
from urllib.parse import parse_qs,urlsplit,urlencode

NOW=2000000000
CALLBACK='http://127.0.0.1:32123/auth/callback'

@contextmanager
def synthetic_file_lock(budget):
    budget.check();yield

class SyntheticSecret:
    def __init__(self,value,owner):self.value=bytearray(value);self.closed=False;owner.reads.append(self)
    def reveal_bytes(self):return bytes(self.value)
    def close(self):self.closed=True;self.value[:]=b'\0'*len(self.value)

class SyntheticVault:
    def __init__(self):self.data={};self.reads=[];self.deleted=[];self.writes=[];self.fail_at=None
    def __call__(self):return self
    def read(self,ref):return SyntheticSecret(self.data[ref],self) if ref in self.data else None
    def write(self,ref,data):
        self.writes.append(ref)
        if self.fail_at==len(self.writes):raise OSError('SYNTHETIC_SLOT_FAILURE')
        self.data[ref]=bytes(data)
    def delete(self,ref):self.deleted.append(ref);return self.data.pop(ref,None) is not None

class SyntheticVerifier:
    def dependency(self):return None
    def verify(self,token,client,nonce,jwks,now):
        from app.siwc_oauth import validate_claims
        if jwks!={'keys':['SYNTHETIC_NOT_A_JWK']}:raise AssertionError('synthetic JWKS required')
        return validate_claims(json.loads(token),client,nonce,now)

class SyntheticHTTP:
    def __init__(self):self.calls=[];self.tokens=None;self.models_value={'models':[{'slug':'synthetic-role','display_name':'Synthetic role','visibility':'list'}]};self.closed=0;self.hook=None
    def close(self):self.closed+=1
    def request(self,url,method,body,headers,budget,stream=False):
        from app.siwc_oauth import DISCOVERY,ISSUER,AUTHORIZE,TOKEN,JWKS
        budget.check();self.calls.append((url,method,body,copy.deepcopy(headers),budget))
        if self.hook:self.hook(url)
        if url==DISCOVERY:return dict(issuer=ISSUER,authorization_endpoint=AUTHORIZE,token_endpoint=TOKEN,jwks_uri=JWKS,revocation_endpoint=ISSUER+'/synthetic-revoke')
        if url==JWKS:return {'keys':['SYNTHETIC_NOT_A_JWK']}
        if url==TOKEN:return copy.deepcopy(self.tokens)
        if url.endswith('/models'):return copy.deepcopy(self.models_value)
        if url.endswith('/synthetic-revoke'):return None
        raise AssertionError('unexpected synthetic endpoint')

class Harness:
    def __init__(self,directory):
        from app.siwc_oauth import SIWCSession
        self.vault=SyntheticVault();self.http=SyntheticHTTP();self.urls=[];self.now=NOW
        self.session=SIWCSession(Path(directory)/'synthetic-sessions.json',self.vault,http=self.http,
            verifier=SyntheticVerifier(),clock=lambda:self.now,opener=self.urls.append,file_lock=synthetic_file_lock)
    def begin(self,account=None):
        self.session.begin(CALLBACK,account)
        return {k:v[0] for k,v in parse_qs(urlsplit(self.urls[-1]).query).items()}
    def tokens(self,params,client='oaiapp_SYNTHETIC',subject='SYNTHETIC_SUBJECT',**changes):
        from app.siwc_oauth import SCOPES,ISSUER
        claims=dict(iss=ISSUER,aud=client,sub=subject,iat=self.now,exp=self.now+3600,nonce=params['nonce'])
        self.http.tokens=dict(access_token='SYNTHETIC_ACCESS_NEVER_REAL',refresh_token='SYNTHETIC_REFRESH_NEVER_REAL',
            id_token=json.dumps(claims),scope=' '.join(sorted(SCOPES)),token_type='Bearer',expires_in=3600)
        self.http.tokens.update(changes)
    def callback(self,params,client='oaiapp_SYNTHETIC',**changes):
        values=dict(state=params['state'],code='SYNTHETIC_AUTH_CODE',client_id=client);values.update(changes)
        return self.session.callback(urlencode(values),CALLBACK)
    def authorize(self,client='oaiapp_SYNTHETIC',subject='SYNTHETIC_SUBJECT'):
        p=self.begin();self.tokens(p,client,subject);self.callback(p,client);return self.session.binding()

def completed(text='合成文本',usage=None):
    return dict(type='response.completed',response=dict(status='completed',output=[dict(type='message',role='assistant',
        content=[dict(type='output_text',text=text)])],usage=usage))

def frame(event):return ('data: '+json.dumps(event,ensure_ascii=False)+'\n\n').encode('utf-8')
