"""P01.1 real Pi/scripted-policy + HTTP/SQLite acceptance, NOT real models.

Two production coordinator day windows per 4/5/6 pack, plus one explicit
same-person restored-session proof action. Synthetic private observations are
labelled fixtures, not discoveries/LLM agency. Separate 90-day worlds execute
only production rules without characters: never called model observation.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
import time
import traceback
from urllib.request import Request, urlopen
import uuid

from .actors import ActorHost
from .server import create_default_actors, make_server
from .world import WorldStore, WorldError, DAY

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT/'app/data'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def http(port, route, body=None, token=None):
    headers = {}
    if body is not None:
        headers = {'Origin':f'http://127.0.0.1:{port}', 'X-Theatre-CSRF':token, 'Content-Type':'application/json'}
    req = Request(f'http://127.0.0.1:{port}'+route, data=None if body is None else canonical(body).encode('utf-8'), headers=headers)
    with urlopen(req, timeout=15) as response:
        return response.status, json.load(response)


def step(port, token, target):
    assert http(port, '/api/control', {'action':'step'}, token)[0] == 202
    deadline = time.monotonic()+120
    while time.monotonic() < deadline:
        state = http(port, '/api/state')[1]
        assert not state['runtime']['error'], state['runtime']
        if not state['runtime']['busy']:
            assert state['status']=='user_paused' and state['time']==target, state['status']
            return state
        time.sleep(.04)
    raise AssertionError('bounded production coordinator window timed out')


def actor_rows(path):
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        return (db.execute('SELECT person_id,messages FROM actor_sessions ORDER BY person_id').fetchall(),
                db.execute('SELECT intent_id,person_id,state,request,metrics,receipt FROM actor_intents ORDER BY rowid').fetchall(),
                db.execute('SELECT world_id FROM actor_world_binding').fetchone()[0])


def run_world(directory, count, marker):
    directory.mkdir()
    path = DATA/f'scenarios/iron-{count}.json'
    pack = json.loads(path.read_text(encoding='utf-8'))
    world = WorldStore(directory/'world.sqlite', scenario_path=path)
    initial = world.initialize()
    assert initial['scenario_id']==pack['scenario_id']
    assert initial['scenario_sha256']==hashlib.sha256(canonical(pack).encode('utf-8')).hexdigest()
    assert len(initial['countries'])==count and len(initial['persons'])==3*count
    expected_people = {p['person_id'] for c in pack['countries'] for p in c['positions']}
    assert {p['id'] for p in initial['persons']}==expected_people
    king = 'person-country-1-king'
    # Controlled, explicit privacy fixture; no claim of autonomous observation.
    world._mutate(lambda state, db: world._notice(state, king, marker, '隔离验收夹具：只对本人记录'))
    host = create_default_actors(world, directory/'actors.sqlite')
    server = make_server(world, host, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    try:
        meta = http(port, '/api/meta')[1]
        assert meta['dm_mode']=='scripted-dm' and not any('四国' in x for x in meta['limitations'])
        first = step(port, meta['csrf'], DAY)
        second = step(port, meta['csrf'], 2*DAY)
        assert len(second['causality']['proposals'])==6*count
        assert not http(port, f'/api/person?id={king}&seq=1')[1]['observations']
        assert marker in canonical(http(port, '/api/person?id='+king)[1])
        before = {p.name:sha(p) for p in directory.glob('*.sqlite')}
        for route in ('/api/state?seq=1', f'/api/state?seq={first["revision"]}', '/api/person?id='+king+'&seq=1'):
            assert http(port, route)[0]==200
        assert before=={p.name:sha(p) for p in directory.glob('*.sqlite')}
        _, intents, _ = actor_rows(directory/'actors.sqlite')
        assert len(intents)==6*count and all(row[2]=='committed' for row in intents)
    finally:
        server.shutdown();server.server_close();thread.join(5)
    assert host.worker_exit_code==0 and host.worker_pid is None
    sessions, _, owner = actor_rows(directory/'actors.sqlite')
    assert owner==initial['world_id'] and {p for p,msg in sessions}==expected_people
    previous = dict(sessions)[king]
    restored = create_default_actors(world, directory/'actors.sqlite')
    try:
        world.control('resume')
        extra = restored.act(king)
        assert extra['provider']=='scripted-pi' and extra['sdk_requests']==2
        world.control('pause')
    finally:
        restored.close()
    assert restored.worker_exit_code==0
    sessions, intents, owner = actor_rows(directory/'actors.sqlite')
    request = json.loads(intents[-1][3])
    assert canonical(request['messages'])==canonical(json.loads(previous))
    assert len(intents)==6*count+1 and all(row[2]=='committed' for row in intents)
    assert marker in dict(sessions)[king]
    assert all(marker not in messages for person,messages in sessions if person!=king)
    forbidden = [initial['world_id'], initial['scenario_id'], initial['scenario_sha256'], initial['pathology_sha256'], pack['geography_sha256']]
    for intent,person,status,request,metrics,receipt in intents:
        metric=json.loads(metrics);receipt=json.loads(receipt)
        assert metric['provider']=='scripted-pi' and metric['sdk_requests']==2
        assert receipt['world_id']==owner and receipt['person_id']==person and receipt['intent_id']==intent
        assert not any(token in request for token in forbidden)
        if person!=king:
            assert marker not in request
    # A real read-only rule preflight, not a fake policy authorization check.
    world.control('resume')
    before=world.snapshot();digest=sha(directory/'world.sqlite')
    try:
        world.preview('person-country-2-finance', 'p011-foreign-stock-preflight', {'action':'ship','to':'place-0','quantity':1})
    except WorldError as error:
        assert error.code=='NOT_AUTHORIZED', error.code
    else:
        raise AssertionError('foreign-country inventory preflight accepted')
    assert world.snapshot()==before and sha(directory/'world.sqlite')==digest
    world.control('pause')
    return {'countries':count,'persons':len(sessions),'world_id':owner,'scenario_id':initial['scenario_id'],
            'scenario_sha256':initial['scenario_sha256'],'coordinator_days':2,'explicit_restore_proof_actions':1,
            'committed_intents':len(intents),'sdk_requests':sum(json.loads(row[4])['sdk_requests'] for row in intents),
            'normal_node_exits':[host.worker_exit_code,restored.worker_exit_code],
            'synthetic_private_fixture_person':king,'no_other_person_marker':True,
            'readonly_history':True,'restored_actual_pi_session':True,'foreign_stock_preflight_denied':True,
            'first_day_revision':first['revision'],'second_day_revision':second['revision'],
            'final_revision':world.snapshot()['revision'],'files_sha256':{p.name:sha(p) for p in directory.glob('*.sqlite')}}


def run_rules(directory, count):
    world=WorldStore(directory, scenario_path=DATA/f'scenarios/iron-{count}.json')
    first=world.initialize();world.control('resume');final=world.advance(90*DAY)
    assert final['status']=='horizon_paused' and final['time']==90*DAY
    assert final['ledger']['unmet_consumption']==0 and min(p['grain'] for p in final['places'])>=0
    assert final['queue'] and all(q['at']>90*DAY for q in final['queue'])
    grain=sum(p['grain'] for p in final['places'])
    assert final['ledger']['initial']+final['ledger']['harvested']==grain+final['ledger']['consumed']
    assert final['pathology_sha256']==first['pathology_sha256']
    assert WorldStore(directory).initialize()==final
    return {'countries':count,'kind':'actual production rules only, NO character/provider actions',
            'time':final['time'],'ledger':final['ledger'],'grain':grain,'future_queue':len(final['queue']),
            'horizon_paused':True,'not_model_90_day_observation':True}


def run(directory):
    markers={n:f'PRIVATE_FIXTURE_WORLD_{n}_{uuid.uuid4().hex}' for n in (4,5,6)}
    worlds=[run_world(directory/f'iron-{n}',n,markers[n]) for n in (4,5,6)]
    assert len({x['world_id'] for x in worlds})==3
    for n in (4,5,6):
        sessions,intents,_=actor_rows(directory/f'iron-{n}/actors.sqlite')
        all_private=canonical([sessions,intents])
        assert all(m not in all_private for other,m in markers.items() if other!=n)
    a=directory/'iron-4/actors.sqlite'
    before=sha(a)
    foreign=WorldStore(directory/'iron-6/world.sqlite')
    foreign.initialize()
    try:
        ActorHost(foreign,a)
    except RuntimeError as error:
        assert str(error)=='ACTOR_WORLD_MISMATCH'
    else:
        raise AssertionError('real cross-world actor store reuse accepted')
    assert sha(a)==before
    rules=[run_rules(directory/f'rules-{n}.sqlite',n) for n in (4,5,6)]
    return {'status':'passed','worlds':worlds,'pure_rules_90_day_worlds':rules,
            'intents':sum(w['committed_intents'] for w in worlds),'sdk_requests':sum(w['sdk_requests'] for w in worlds),
            'cross_world_nonempty_private_markers_isolated':True,'cross_world_actor_db_mismatch_refused':True,
            'full_release_complete':False,'real_model_calls':0,'synthetic_fixture_limit':'Finite marker check, NOT general semantic safety/personality proof'}


def main():
    directory=ROOT/'app/evidence'/('p011-integration-'+uuid.uuid4().hex[:10])
    directory.mkdir()
    try:
        result=run(directory)
    except BaseException:
        result={'status':'failed','error':traceback.format_exc(),'directory':str(directory)}
        (directory/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        raise
    result['directory']=str(directory)
    (directory/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    main()
