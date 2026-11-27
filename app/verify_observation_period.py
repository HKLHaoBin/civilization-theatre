"""Bounded observation-period verification, with real wall-clock accounting.

Keeps the original 240-second attempts intact. Does not alter simulation time,
actor count, policy, durability or the product implementation.
"""
from contextlib import closing
from pathlib import Path
import argparse
import hashlib
import http.client
import json
import sqlite3
import threading
import time
import uuid
from app.world import WorldStore
from app.actors import ActorHost
from app.server import make_server


def main():
    parser=argparse.ArgumentParser(description='真实组件90日脚本联调；非真实LLM验收')
    parser.add_argument('--timeout-seconds',type=int,default=900)
    args=parser.parse_args()
    if not 1<=args.timeout_seconds<=1800: parser.error('timeout must be 1..1800 seconds')
    root=Path(__file__).resolve().parent
    run=root/'evidence'/('observation-'+uuid.uuid4().hex[:10]); run.mkdir(parents=True)
    files=['world.py','actors.py','server.py','pi_worker.mjs','data/scenario.json','verify_observation_period.py']
    sha=lambda:{f:hashlib.sha256((root/f).read_bytes()).hexdigest() for f in files}
    source=sha(); started=time.monotonic()
    world=WorldStore(run/'world.sqlite'); first=world.initialize()
    host=ActorHost(world,run/'actors.sqlite'); server=make_server(world,host,0)
    thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
    port=server.server_address[1]; token=None
    def request(route,body=None):
        connection=http.client.HTTPConnection('127.0.0.1',port,timeout=30)
        try:
            headers={} if body is None else {'Origin':f'http://127.0.0.1:{port}',
                     'X-Theatre-CSRF':token,'Content-Type':'application/json'}
            connection.request('GET' if body is None else 'POST',route,
                               body=None if body is None else json.dumps(body),headers=headers)
            response=connection.getresponse(); data=json.loads(response.read())
            assert response.status<400,(response.status,data)
            return data
        finally: connection.close()
    result={'kind':'actual_http_world_pi_scripted_observation_period_not_real_llm',
            'run_directory':str(run),'wall_clock_budget_seconds':args.timeout_seconds,
            'prior_240_second_attempts_retained':True,'source_sha256':source,
            'real_llm_acceptance':False,'status':'failed'}
    failure=None
    try:
        token=request('/api/meta')['csrf']; request('/api/control',{'action':'run'})
        deadline=time.monotonic()+args.timeout_seconds; last=-1
        while time.monotonic()<deadline:
            state=request('/api/state'); day=state['time']//86400
            if day//5 != last:
                last=day//5
                print(json.dumps({'day':day,'elapsed_seconds':round(time.monotonic()-started,2),'status':state['status']},ensure_ascii=False),flush=True)
            if not state['runtime']['busy']: break
            time.sleep(.5)
        assert not state['runtime']['busy'],'bounded wall-clock verification budget exhausted'
        assert state['status']=='horizon_paused' and state['time']==90*86400,state['runtime']
        final=world.snapshot(); ledger=final['ledger']
        assert (len(final['countries']),len(final['persons']),len(final['places']))==(4,12,8)
        assert sum(p['grain'] for p in final['places'])+sum(p['quantity'] for p in final['in_transit'])+ledger['consumed']==ledger['initial']+ledger['harvested']
        before=hashlib.sha256((run/'world.sqlite').read_bytes()).hexdigest()
        assert request('/api/state?seq=1')['time']==first['time']==0
        for person in final['persons']:
            view=request('/api/person?id='+person['id']); assert 'pathology' not in view
        assert hashlib.sha256((run/'world.sqlite').read_bytes()).hexdigest()==before
        with closing(sqlite3.connect(run/'actors.sqlite')) as db:
            metrics=[json.loads(row[0]) for row in db.execute("SELECT metrics FROM actor_intents WHERE state='committed'")]
            sessions=db.execute('SELECT count(*) FROM actor_sessions').fetchone()[0]
            pending=db.execute("SELECT count(*) FROM actor_intents WHERE state!='committed'").fetchone()[0]
        assert len(metrics)==89*12 and sessions==12 and pending==0
        assert all(m['provider']=='scripted-pi' and m['sdk_requests']==2 for m in metrics)
        assert source==sha(),'source changed during verification'
        result.update(status='passed',days=90,countries=4,persons=12,sessions=sessions,
            committed_intents=len(metrics),scripted_provider_requests=sum(m['sdk_requests'] for m in metrics),
            sdk_events=sum(sum(m['sdk_events'].values()) for m in metrics),
            world_status=final['status'],queue_preserved=len(final['queue']),
            grain_conserved=True,read_only_file_unchanged=True,pending_intents=pending)
    except Exception as error:
        failure=error; result['error']=str(error)
    finally:
        server.shutdown(); server.server_close(); thread.join(5)
        current=world.snapshot()
        result.update(final_day=current['time']//86400,final_status=current['status'],
            node_exit_code=host.worker_exit_code,elapsed_seconds=round(time.monotonic()-started,3),
            source_unchanged=source==sha(),world_bytes=(run/'world.sqlite').stat().st_size,
            actors_bytes=(run/'actors.sqlite').stat().st_size)
        reopened=WorldStore(run/'world.sqlite'); result['reopened_equal']=reopened.initialize()==current
        if host.worker_exit_code!=0 or not result['reopened_equal']:
            result['status']='failed'; failure=failure or RuntimeError('cleanup/reopen verification failed')
        text=json.dumps(result,ensure_ascii=False,indent=2)+'\n'
        (run/'result.json').write_text(text,encoding='utf-8')
        (root/'evidence/observation-period-report.json').write_text(text,encoding='utf-8')
        print(text,flush=True)
    if failure: raise SystemExit(1)

if __name__=='__main__': main()
