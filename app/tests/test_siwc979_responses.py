"""Prospective SOURCE ONLY: production SSE parser, synthetic byte streams.
NOT RUN in 979. No account, socket, thread, vault or cryptographic operations.
"""
import copy
import json
import unittest
from unittest.mock import patch
from app.tests.siwc979_synthetic import frame,completed

class Budget:
    def __init__(self):self.calls=0;self.cancel_at=None
    def check(self):
        self.calls+=1
        if self.cancel_at is not None and self.calls>=self.cancel_at:raise RuntimeError('SYNTHETIC_CANCELLED')
    def remaining(self):self.check();return 1

class StreamTests(unittest.TestCase):
    def decode(self,chunks):
        from app.siwc_responses import ResponsesDecoder
        d=ResponsesDecoder(Budget())
        for chunk in chunks:d.feed(chunk)
        return d.finish()

    def test_every_utf8_and_sse_split_boundary(self):
        wire=frame(dict(type='response.output_text.delta',delta='合成文本'))+frame(completed(usage={'input_tokens':31,'output_tokens':7}))
        for split in range(len(wire)+1):
            with self.subTest(split=split):
                r=self.decode([wire[:split],wire[split:]])
                self.assertEqual(r['choices'][0]['message']['content'],'合成文本')
                self.assertEqual(r['usage'],{'prompt_tokens':31,'completion_tokens':7})
        self.assertEqual(self.decode([bytes([x]) for x in wire])['usage']['completion_tokens'],7)

    def test_multiline_crlf_comments_and_no_fabricated_usage(self):
        raw=json.dumps(completed(),ensure_ascii=False)
        wire=('event: response.completed\r\n: synthetic comment\r\ndata: '+raw[:raw.index('"response"')]+ '\r\ndata: '+raw[raw.index('"response"'):]+'\r\n\r\n').encode()
        r=self.decode([bytes([x]) for x in wire]);self.assertIsNone(r['usage']['prompt_tokens']);self.assertIsNone(r['usage']['completion_tokens'])

    def test_bare_cr_frames_and_done_is_not_completion(self):
        self.assertEqual(self.decode([frame(completed()).replace(b'\n',b'\r')])['choices'][0]['message']['content'],'合成文本')
        with self.assertRaisesRegex(Exception,'MISSING_COMPLETED'):self.decode([b'data: [DONE]\n\n'])
        self.assertIsNotNone(self.decode([frame(completed())+b'data: [DONE]\n\n']))

    def test_delta_eof_http200_is_never_success(self):
        bad=[b'',frame(dict(type='response.output_text.delta',delta='not complete')),
             frame(completed())[:-1],b'data: {"type":"response.completed"}',b'data: \xff\n\n',b'data: \xe4']
        for wire in bad:
            with self.subTest(wire=wire),self.assertRaises(Exception):self.decode([wire])

    def test_failed_incomplete_error_and_usage_limits_never_succeed(self):
        for kind in ('response.failed','response.incomplete','error'):
            for code in ('subscription_sharing_usage_limit_exceeded','subscription_sharing_usage_unavailable'):
                wire=frame(dict(type='response.output_text.delta',delta='partial'))+frame(dict(type=kind,response={'error':{'code':code}}))
                with self.subTest(kind=kind,code=code),self.assertRaises(Exception):self.decode([wire])

    def test_terminal_status_delta_mismatch_duplicate_and_bad_usage(self):
        values=[]
        for status in ('incomplete','failed',None):
            e=completed();e['response']['status']=status;values.append(frame(e))
        values.append(frame(dict(type='response.output_text.delta',delta='other'))+frame(completed()))
        values.append(frame(completed())+frame(completed()))
        for usage in ({'input_tokens':True},{'input_tokens':-1},{'output_tokens':'7'},{'output_tokens':2**64}):values.append(frame(completed(usage=usage)))
        for wire in values:
            with self.subTest(wire=wire),self.assertRaises(Exception):self.decode([wire])

    def test_tools_refusal_and_nonassistant_items_have_no_execution_path(self):
        for kind in ('function_call','computer_call','web_search_call','custom_tool_call'):
            e=completed();e['response']['output']=[{'type':kind,'arguments':'SYNTHETIC_DANGEROUS_COMMAND'}]
            with self.subTest(kind=kind),self.assertRaisesRegex(Exception,'TOOLS_FORBIDDEN'):self.decode([frame(e)])
            with self.assertRaisesRegex(Exception,'TOOLS_FORBIDDEN'):self.decode([frame({'type':'response.output_item.added','item':{'type':kind}})])
        for part in ({'type':'refusal','refusal':'no'},{'type':'tool_result','text':'bad'}):
            e=completed();e['response']['output'][0]['content']=[part]
            with self.assertRaisesRegex(Exception,'TOOLS_FORBIDDEN'):self.decode([frame(e)])

    def test_bounded_bytes_text_nodes_depth_events_and_terminal_reserve(self):
        from app.siwc_responses import ResponsesDecoder,bounded_json
        with self.assertRaises(Exception):self.decode([b'x'*(2*1024*1024+1)])
        with self.assertRaises(Exception):self.decode([frame(completed('x'*65537))])
        with self.assertRaises(Exception):bounded_json('['*26+'0'+']'*26)
        with self.assertRaises(Exception):bounded_json(json.dumps([0]*17000))
        with self.assertRaises(Exception):bounded_json('{"type":1,"type":2}')
        with self.assertRaises(Exception):bounded_json('{"x":NaN}')
        d=ResponsesDecoder(Budget())
        for _ in range(16000):d.feed(frame({'type':'response.created'}))
        d.feed(frame(completed('x'*65536)));self.assertEqual(len(d.finish()['choices'][0]['message']['content']),65536)

    def test_cancellation_checked_between_chunks_and_before_terminal(self):
        from app.siwc_responses import ResponsesDecoder
        b=Budget();d=ResponsesDecoder(b);d.feed(frame({'type':'response.created'}));b.cancel_at=b.calls+1
        with self.assertRaisesRegex(RuntimeError,'SYNTHETIC_CANCELLED'):d.feed(frame(completed()))

    def test_actual_request_body_has_full_isolated_input_and_no_preview_fields(self):
        from app.siwc_responses import PublicHTTP,API
        h=PublicHTTP();calls=[]
        def record(*args,**kwargs):calls.append((args,kwargs));return {'synthetic':True}
        h.request=record
        messages=[{'role':'system','content':'private own instructions'},{'role':'user','content':'own observation'},
                  {'role':'assistant','content':'own prior output'},{'role':'user','content':'own fresh prompt'}]
        h.responses({'base_url':API,'model':'SYNTHETIC_MODEL'},messages,'SYNTHETIC_ACCESS',Budget())
        a,k=calls[0];body=json.loads(a[2]);self.assertEqual(a[:2],(API+'/responses','POST'))
        self.assertEqual(set(body),{'model','instructions','input','store','stream'})
        self.assertIs(body['store'],False);self.assertIs(body['stream'],True)
        self.assertEqual(body['input'],messages[1:]);self.assertEqual(body['instructions'],messages[0]['content'])
        self.assertTrue(k['stream']);self.assertNotIn('previous_response_id',body)

    def test_actual_http_closes_on_cancel_and_rejects_status_redirect_mime(self):
        from app.siwc_responses import PublicHTTP,API
        class Response:
            status=200
            def getheader(self,*a):return 'text/event-stream'
            def read1(self,n):return frame(completed())
            def close(self):self.closed=True
        class Connection:
            sock=None
            def __init__(self,*a,**k):self.response=Response();self.closed=False
            def request(self,*a,**k):pass
            def getresponse(self):return self.response
            def close(self):self.closed=True
        for status,mime in ((302,'text/event-stream'),(429,'application/json'),(200,'application/json')):
            connection=Connection();connection.response.status=status;connection.response.getheader=lambda *a:mime
            with patch('app.siwc_responses.ssl.SSLContext') as tls,patch('app.siwc_responses.http.client.HTTPSConnection',return_value=connection):
                with self.assertRaises(Exception):PublicHTTP().request(API+'/responses','POST',b'{}',{},Budget(),True)
            self.assertTrue(connection.closed);self.assertTrue(connection.response.closed)
