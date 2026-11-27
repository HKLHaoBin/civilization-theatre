"""Additional unexecuted provider lifecycle cases. Synthetic transports only."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from app.tests.test_real_provider967 import config


class LifecycleTests(unittest.TestCase):
    def test_factory_read_reveal_and_close_faults_are_redacted_and_owned_close_runs(self):
        from app.real_provider import RealGateway
        from app.real_settings import SettingsStore
        from app.real_intent import make_request
        for mode in ('factory','read','reveal','close'):
            closed=[]
            class Secret:
                def reveal_bytes(self):
                    if mode=='reveal':raise RuntimeError('SYNTHETIC_NEVER_LOG')
                    return b'SYNTHETIC_NEVER_LOG'
                def close(self):
                    closed.append(True)
                    if mode=='close':raise RuntimeError('SYNTHETIC_NEVER_LOG')
            class Vault:
                def write(self,*a):pass
                def read(self,*a):
                    if mode=='read':raise RuntimeError('SYNTHETIC_NEVER_LOG')
                    return Secret()
            def factory():
                if mode=='factory':raise RuntimeError('SYNTHETIC_NEVER_LOG')
                return Vault()
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d:
                s=SettingsStore(Path(d)/'services.json',credential_factory=Vault)
                c=config()
                for v in c.values():v.update(provider='openai-chat-https',base_url='https://synthetic.example.invalid/v1',max_retries=0)
                s.save({'services':c,'secrets':{'roles':'SYNTHETIC_NEVER_LOG','dm':'SYNTHETIC_NEVER_LOG'}})
                g=RealGateway(s,credential_factory=factory,transport=lambda *a:{'choices':[{'message':{'content':'等待'}}]})
                with self.assertRaises(Exception) as caught:g.role(make_request('p','i',[],'s','p',g.version()),None)
                self.assertNotIn('SYNTHETIC_NEVER_LOG',str(caught.exception))
                self.assertEqual(bool(closed),mode in ('reveal','close'))
                if s.usage_path.exists():self.assertNotIn('SYNTHETIC_NEVER_LOG',s.usage_path.read_text())

    def test_cancel_during_read_closes_response_connection_and_preserves_original_cancel(self):
        from app.real_provider import HTTPTransport,RequestBudget
        class Attempt:
            deadline=float('inf')
            cancelled=False
            def check(self):
                if self.cancelled:raise RuntimeError('ACTOR_EXECUTION_CANCELLED')
        attempt=Attempt();closed=[]
        class Response:
            status=200
            def getheader(self,k):return None
            def read1(self,n):attempt.cancelled=True;return b'{}'
            def close(self):closed.append('response')
        class Connection:
            sock=None
            def request(self,*a,**k):pass
            def getresponse(self):return Response()
            def close(self):closed.append('connection')
        t=HTTPTransport()
        with patch('app.real_provider.http.client.HTTPConnection',lambda *a,**k:Connection()):
            with self.assertRaisesRegex(RuntimeError,'ACTOR_EXECUTION_CANCELLED'):
                t(config()['roles'],[],None,RequestBudget(attempt,2))
        self.assertEqual(closed,['response','connection']);self.assertEqual(t.active,set())

    def test_oversize_content_length_does_not_read_body(self):
        from app.real_provider import HTTPTransport,RequestBudget,ProviderFault,MAX_RESPONSE
        closed=[]
        class Response:
            status=200
            def getheader(self,k):return str(MAX_RESPONSE+1)
            def read1(self,n):raise AssertionError('oversize body read')
            def close(self):closed.append('response')
        class Connection:
            sock=None
            def request(self,*a,**k):pass
            def getresponse(self):return Response()
            def close(self):closed.append('connection')
        t=HTTPTransport()
        with patch('app.real_provider.http.client.HTTPConnection',lambda *a,**k:Connection()):
            with self.assertRaisesRegex(ProviderFault,'REAL_RESPONSE_TOO_LARGE'):
                t(config()['roles'],[],None,RequestBudget(None,2))
        self.assertEqual(closed,['response','connection']);self.assertFalse(t.active)

    def test_roles_same_model_never_share_private_messages_or_system(self):
        from app.real_provider import RealGateway
        from app.real_settings import SettingsStore
        from app.real_intent import make_request
        seen=[]
        with tempfile.TemporaryDirectory() as d:
            s=SettingsStore(Path(d)/'services.json');s.save({'services':config(),'secrets':{}})
            def transport(service,messages,*a):seen.append(messages);return {'choices':[{'message':{'content':'独立等待'}}]}
            g=RealGateway(s,transport=transport)
            a=make_request('a','ia',[{'role':'assistant','content':'PRIVATE_A_HISTORY'}],'PRIVATE_A_SYSTEM','PRIVATE_A_VIEW',g.version())
            b=make_request('b','ib',[],'PRIVATE_B_SYSTEM','PRIVATE_B_VIEW',g.version())
            ra=g.role(a,None);rb=g.role(b,None)
            self.assertNotIn('PRIVATE_A',json.dumps(seen[1]))
            self.assertNotIn('PRIVATE_B',json.dumps(seen[0]))
            self.assertEqual(ra['messages'][-1]['intent_binding']['person_id'],'a')
            self.assertEqual(rb['messages'][-1]['intent_binding']['person_id'],'b')

    def test_unknown_usage_does_not_erase_billable_retry_uncertainty(self):
        from app.real_provider import usage_values
        self.assertEqual(usage_values({},1),{'requests':1,'input_tokens':None,'output_tokens':None,'cost':None})
        self.assertEqual(usage_values({'usage':{'prompt_tokens':23,'completion_tokens':9}},2),
                         {'requests':2,'input_tokens':None,'output_tokens':None,'cost':None})
        self.assertEqual(usage_values({'usage':{'prompt_tokens':23,'completion_tokens':9}},1),
                         {'requests':1,'input_tokens':23,'output_tokens':9,'cost':None})


    def test_retry_returned_usage_is_preserved_but_aggregate_stays_unknown(self):
        from app.real_provider import RealGateway,ProviderFault
        from app.real_settings import SettingsStore
        from app.real_intent import make_request
        calls=[]
        with tempfile.TemporaryDirectory() as d:
            s=SettingsStore(Path(d)/'services.json');s.save({'services':config(),'secrets':{}})
            def transport(*a):
                calls.append(1)
                if len(calls)==1:raise ProviderFault('REAL_TRANSPORT',True)
                return {'choices':[{'message':{'content':'等待'}}],'usage':{'prompt_tokens':23,'completion_tokens':9}}
            g=RealGateway(s,transport=transport)
            result=g.role(make_request('p','i',[],'s','p',g.version()),None)
            usage=result['messages'][-1]['intent_binding']['usage']
            self.assertEqual(usage,{'requests':2,'input_tokens':None,'output_tokens':None,'cost':None})
            events=[json.loads(line) for line in s.usage_path.read_text().splitlines()]
            returned=[e for e in events if e['event']=='request_returned']
            self.assertEqual(len(returned),1)
            self.assertEqual((returned[0]['request_index'],returned[0]['input_tokens'],returned[0]['output_tokens']),(2,23,9))
            self.assertIsNone(returned[0]['cost']);self.assertEqual(returned[0]['billing_status'],'UNKNOWN')


if __name__=='__main__':unittest.main()
