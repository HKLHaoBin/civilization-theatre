"""Exercise the actual local HTTP/world/Pi stack to its 90-day boundary.

Scripted provider only. This is component acceptance, not real-model gameplay.
Run: python -B -m app.verify_release
"""
from contextlib import closing
from pathlib import Path
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
    root=Path(__file__).resolve().parent
    run=root/'evidence'/('release-'+uuid.uuid4().hex[:10])
    run.mkdir(parents=True)
    world=WorldStore(run/'world.sqlite'); first=world.initialize()
    host=ActorHost(world,run/'actors.sqlite')
    server=make_server(world,host,0)
    thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
    port=server.server_address[1]
    token=None
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
    started=time.monotonic()
    try:
        token=request('/api/meta')['csrf']
        request('/api/control',{'action':'run'})
        deadline=time.monotonic()+240
        while time.monotonic()<deadline:
            state=request('/api/state')
            if not state['runtime']['busy']: break
            time.sleep(.3)
        assert not state['runtime']['busy'], '90-day run exceeded bounded timeout'
        assert state['time']==90*86400 and state['status']=='horizon_paused',state['runtime']
        assert state['runtime']['error'] is None
        final=world.snapshot()
        assert len(final['countries'])==4 and len(final['persons'])==12 and len(final['places'])==8
        ledger=final['ledger']
        assert sum(p['grain'] for p in final['places'])+sum(p['quantity'] for p in final['in_transit'])+ledger['consumed']==ledger['initial']+ledger['harvested']
        dbhash=lambda:hashlib.sha256((run/'world.sqlite').read_bytes()).hexdigest()
        before=dbhash()
        old=request('/api/state?seq=1')
        assert old['time']==first['time']==0
        request('/api/person?id=person-country-1-king')
        assert dbhash()==before
        assert all('pathology' not in world.observe(p['id']) for p in final['persons'])
        with closing(sqlite3.connect(run/'actors.sqlite')) as db:
            metrics=[json.loads(row[0]) for row in db.execute("SELECT metrics FROM actor_intents WHERE state='committed'")]
            sessions=db.execute('SELECT count(*) FROM actor_sessions').fetchone()[0]
        assert len(metrics)==89*12 and sessions==12
        assert all(m['provider']=='scripted-pi' for m in metrics)
        requests=sum(m['sdk_requests'] for m in metrics)
    finally:
        server.shutdown(); server.server_close(); thread.join(5)
    assert host.worker_exit_code==0
    restored=WorldStore(run/'world.sqlite'); assert restored.initialize()==final
    result={'kind':'actual_http_world_pi_scripted_90_day_component_run_not_real_llm',
            'status':'passed','run_directory':str(run),'days':final['time']//86400,
            'countries':len(final['countries']),'persons':len(final['persons']),
            'sessions':sessions,'committed_intents':len(metrics),'scripted_provider_requests':requests,
            'sdk_events':sum(sum(m['sdk_events'].values()) for m in metrics),
            'world_status':final['status'],'pending_queue':len(final['queue']),
            'node_exit_code':host.worker_exit_code,'replay_file_unchanged':True,
            'reopened_world_equal':True,'grain_conserved':True,
            'real_llm_acceptance':False,'elapsed_seconds':round(time.monotonic()-started,3)}
    report=root/'evidence/90day-report.json'
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__': main()
