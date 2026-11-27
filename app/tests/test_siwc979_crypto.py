"""PROSPECTIVE ONLY, NOT EXECUTED. Ephemeral SYNTHETIC RSA keys/ID tokens.
Requires the explicitly optional SIWC crypto dependencies. This is an offline
signature contract test, not OpenAI/OAuth acceptance. No stored user keys.
"""
import copy
import json
import time
import unittest

class SignatureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from cryptography.hazmat.primitives.asymmetric import rsa
        import jwt
        cls.jwt=jwt
        cls.private=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        cls.other=rsa.generate_private_key(public_exponent=65537,key_size=2048)
        cls.key=json.loads(jwt.algorithms.RSAAlgorithm.to_jwk(cls.private.public_key()))
        cls.key.update(kid='SYNTHETIC_TEST_KEY',alg='RS256',use='sig')

    def claims(self):
        from app.siwc_oauth import ISSUER
        now=int(time.time())
        return dict(iss=ISSUER,aud='oaiapp_SYNTHETIC',sub='SYNTHETIC_SUBJECT',nonce='SYNTHETIC_NONCE',iat=now,exp=now+120)

    def token(self,claims=None,key=None,**kw):
        return self.jwt.encode(claims or self.claims(),key or self.private,algorithm='RS256',headers={'kid':'SYNTHETIC_TEST_KEY'},**kw)

    def verify(self,token,jwks=None):
        from app.siwc_oauth import IdentityVerifier
        return IdentityVerifier().verify(token,'oaiapp_SYNTHETIC','SYNTHETIC_NONCE',jwks or {'keys':[self.key]},time.time())

    def test_maintained_library_signature_and_jwks_exact_key(self):
        self.assertEqual(self.verify(self.token())['sub'],'SYNTHETIC_SUBJECT')
        for token in (self.token(key=self.other),self.jwt.encode(self.claims(),'SYNTHETIC_HMAC_SECRET',algorithm='HS256',headers={'kid':'SYNTHETIC_TEST_KEY'})):
            with self.assertRaisesRegex(Exception,'IDENTITY_INVALID'):self.verify(token)
        for keys in ([],[dict(self.key,kid='WRONG')],[self.key,self.key],[dict(self.key,use='enc')]):
            with self.assertRaises(Exception):self.verify(self.token(),{'keys':keys})

    def test_valid_signature_does_not_excuse_nonce_issuer_aud_time_or_subject(self):
        now=int(time.time())
        for key,value in [('nonce','OTHER'),('iss','https://synthetic.invalid'),('aud','oaiapp_OTHER'),('sub',''),('exp',now-100),('iat',now+100),('nbf',now+100),('azp','oaiapp_OTHER')]:
            c=self.claims();c[key]=value
            with self.subTest(key=key),self.assertRaises(Exception):self.verify(self.token(c))

    def test_no_unverified_payload_or_header_endpoint_can_supply_identity(self):
        token=self.token();a,b,c=token.split('.');tampered=b[:-1]+('A' if b[-1]!='A' else 'B')
        with self.assertRaises(Exception):self.verify('.'.join((a,tampered,c)))
        claims=self.claims();claims['aud']=['oaiapp_SYNTHETIC','oaiapp_OTHER']
        with self.assertRaises(Exception):self.verify(self.token(claims))
        claims['azp']='oaiapp_SYNTHETIC';self.assertEqual(self.verify(self.token(claims))['aud'],claims['aud'])
