"""P07.2a finite policy pilot: frozen World + current adapters, NOT new-v2 acceptance.

Default/--legacy-policy-pilot executes the 61-file P01.1 archive's World,
scenario and disease modules. --current is an opt-in parent acceptance run.
No real model, NLP generalization or general semantic-safety claim is made.
"""
from contextlib import closing
import argparse
import hashlib
import heapq
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys
import threading
import traceback
import types
from urllib.request import urlopen
import uuid

APP = Path(__file__).resolve().parent
FROZEN = APP/'evidence/candidates/p01-1'
SCRATCH = Path('C:/Users/We3q/AppData/Local/hermes/cache/scratch')
DAY = 86400
KING = 'person-country-1-king'
MARSHAL = 'person-country-1-marshal'
FINANCE = 'person-country-1-finance'
TRAVEL = '按本人已获路线计划，前往place-10；尚未抵达。'
SETUP = '按本人已获路线计划，前往place-1核查粮账；尚未抵达。'
SHIP = '依据本人现场症状与已核粮账，向place-0调运10份粮；尚未到达。'


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')


def sources(current):
    manifest = json.loads((FROZEN/'manifest.json').read_text(encoding='utf-8'))
    archived = {name:sha(FROZEN/name) for name in manifest['sha256']}
    assert archived == manifest['sha256'], 'frozen archive differs from manifest'
    assert len(archived) == manifest['files']
    paths = ['actors.py','adjudication.py','server.py','scripted_dm.py','pi_worker.mjs','verify_transmission.py']
    if not current:
        for name in paths[:-1]:
            assert sha(APP/name)==archived['app/'+name], 'legacy pilot requires unchanged archived adapter: '+name
    if current:
        paths += ['world.py','disease.py','scenario.py','data/geography.json']
        paths += [f'data/scenarios/contact-v2-iron-{n}.json' for n in (4,5,6)]
    return {'archive_files':len(archived),'archive_sha256':archived,
            'active_sha256':{name:sha(APP/name) for name in paths}}


def load_boundary(current):
    """Private package: adapters' relative imports resolve to selected real World.

    Frozen source is executed from its actual archived file, preserving DATA.
    No monkeypatch of World, scenario, pathology or production modules occurs.
    """
    namespace = '_p072a_policy_'+uuid.uuid4().hex
    package = types.ModuleType(namespace)
    package.__path__ = [str(APP)]
    sys.modules[namespace] = package
    modules = {}
    for name in ('disease','scenario','world','actors','adjudication','scripted_dm','server'):
        root = APP if current or name not in ('disease','scenario','world') else FROZEN/'app'
        spec = importlib.util.spec_from_file_location(namespace+'.'+name, root/(name+'.py'))
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        modules[name] = module
    return modules


def classes(modules):
    default = modules['scripted_dm'].DefaultActors
    dm = modules['scripted_dm'].ScriptedDM
    investigate = modules['scripted_dm'].INVESTIGATE
    wait = modules['scripted_dm'].WAIT

    class Policy(default):
        # Only the supplied personal view/card and actual records select intent.
        @staticmethod
        def _choose(view, seen):
            mission = view['person']['card'].get('mission','')
            symptoms = any(any(word in o['text'] for word in ('咳嗽','发热')) for o in view['observations'])
            if 'P072A_TRAVEL' in mission and symptoms and 'pilot-travel' not in seen:
                return {'action':'propose','text':TRAVEL}, seen+['pilot-travel']
            if 'P072A_SHIP' in mission:
                if 'pilot-setup' not in seen:
                    return {'action':'propose','text':SETUP}, seen+['pilot-setup']
                ledger = any('核账' in o['text'] for o in view['observations'])
                if symptoms and not ledger and 'pilot-investigate' not in seen:
                    return {'action':'propose','text':investigate}, seen+['pilot-investigate']
                if symptoms and ledger and 'pilot-ship' not in seen:
                    return {'action':'propose','text':SHIP}, seen+['pilot-ship']
            if 'P072A_SEND' in mission and 'pilot-send' not in seen and symptoms:
                body, consumed = default._choose(view, seen)
                assert body['action']=='propose' and '报信' in body['text']
                return body, consumed+['pilot-send']
            return {'action':'propose','text':wait}, seen

        def _request(self, payload):
            result = super()._request(payload)
            if payload.get('op') != 'close':
                self.audit.append({'request':payload,'result':result})
            return result

    class FixtureDM(dm):
        def __call__(self, context):
            text = context['proposal']['text']
            mapping = {TRAVEL:{'action':'travel','to':'place-10'},
                       SETUP:{'action':'travel','to':'place-1'},
                       SHIP:{'action':'ship','to':'place-0','quantity':10}}
            if text not in mapping:
                return super().__call__(context)
            state = context['world']
            return {'world_id':state['world_id'],'base_revision':state['revision'],
                    'ruling':'成功','reason':'验收夹具逐字有限模板翻译；不替人物选择意图，不是NLP。',
                    'goal':text,'costs':[],'benefits':[],'effects':[mapping[text]]}
    return Policy, FixtureDM


def get(port, route):
    with urlopen(f'http://127.0.0.1:{port}'+route, timeout=15) as response:
        assert response.status == 200
        return json.load(response)


def find(items, identity):
    return next(item for item in items if item['id']==identity)


def healthy(state, place):
    return not find(state['places'], place)['disease']['cohorts']


def grain(state):
    ledger = state['ledger']
    assert ledger['initial']+ledger['harvested'] == sum(p['grain'] for p in state['places'])+ledger['consumed']+sum(s['quantity'] for s in state['in_transit'])


def route(geography, source, destination):
    graph = {r['id']:r['neighbors'] for r in geography['regions']}
    heap, best = [(0,source)], {source:0}
    while heap:
        cost, place = heapq.heappop(heap)
        if cost != best[place]:
            continue
        if place==destination:
            return cost
        for edge in graph[place]:
            new = cost+edge['seconds']
            if new < best.get(edge['to'], float('inf')):
                best[edge['to']] = new
                heapq.heappush(heap,(new,edge['to']))
    raise AssertionError('no authored route')


def task(adjudicator, intent):
    proposal = next(p for p in adjudicator.trace()['proposals'] if p['intent_id']==intent)
    assert proposal['decisions'][-1]['status']=='committed'
    return proposal['committed']['tasks'][0]


def sql_read(path, query, args=()):
    with closing(sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
        return db.execute(query,args).fetchall()


def privacy(value, identity):
    # Finite structural/key + exact-identity checks on REAL provider inputs,
    # session contents and tool replies, not an all-language safety theorem.
    forbidden_keys = {'pathology_runtime','pathology','pathology_sha256','pathology_mechanism_sha256',
                      'contact_units','packet','exposure_packet','runtime','cohorts'}
    def walk(item):
        if isinstance(item,dict):
            assert not (set(item)&forbidden_keys), set(item)&forbidden_keys
            for child in item.values(): walk(child)
        elif isinstance(item,list):
            for child in item: walk(child)
    walk(value)
    text = canonical(value)
    for token in identity:
        assert token not in text, 'observer identity leaked to real actor context'
    for token in ('CONTACT_EXPOSURE_V2','NON_MEDICAL_FICTIONAL_CONTACT_V2','contact_units','pathology_runtime','pathology_mechanism_sha256'):
        assert token not in text


def run_case(evidence, database_root, modules, current, channel, count):
    directory = evidence/f'{channel}-{count}'
    directory.mkdir()
    dbdir = database_root/f'{channel}-{count}'
    dbdir.mkdir()
    data = APP/'data' if current else FROZEN/'app/data'
    scenario = data/'scenarios'/(f'contact-v2-iron-{count}.json' if current else f'iron-{count}.json')
    geography = json.loads((data/'geography.json').read_text(encoding='utf-8'))
    world = modules['world'].WorldStore(dbdir/'world.sqlite',scenario_path=scenario)
    first = world.initialize()
    assert len(first['countries'])==count
    assert first['pathology_sha256']==hashlib.sha256(canonical(first['pathology']).encode()).hexdigest()
    if current:
        assert first['pathology_runtime']=='CONTACT_EXPOSURE_V2'
        assert first['pathology']['version']=='NON_MEDICAL_FICTIONAL_CONTACT_V2'
        assert first['pathology']['exposure_lifetime_seconds']==DAY
        mechanism = hashlib.sha256(canonical({'runtime':'CONTACT_EXPOSURE_V2','pathology':first['pathology']}).encode()).hexdigest()
        assert first['pathology_mechanism_sha256']==mechanism
    else:
        assert 'pathology_runtime' not in first and 'pathology_mechanism_sha256' not in first
    assert len(find(first['places'],'place-1')['disease']['cohorts'])==1
    assert find(first['places'],'place-1')['disease']['cohorts'][0]['infected_at']==0
    adjudicator = modules['adjudication'].Adjudicator(world)
    Policy, FixtureDM = classes(modules)
    person = FINANCE if channel=='shipment' else MARSHAL
    mission = {'travel':'P072A_TRAVEL：作者提供背景：已听闻目的地place-10；计划亲见症状后沿已知路线出访，抵达前不称成功。',
               'shipment':'P072A_SHIP：作者提供背景：已听闻本国place-1与首府place-0；计划从首府赴place-1核账，亲见症状且核账后调10粮回首府。',
               'message':'P072A_SEND：计划在实际亲见症状后向本国君主报信；沿用原DefaultActors报信模板。'}[channel]
    world.edit_card(person,{'mission':mission})
    world.control('resume')
    records, wires, hosts, nodes = [], [], [], []
    checkpoints = {'initial':first}
    host = Policy(world,dbdir/'actors.sqlite',adjudicator=adjudicator,dm_provider=FixtureDM(world))
    host.audit = wires
    hosts.append(host)
    server = thread = None
    result = None
    def act(expected):
        output = host.act(person)
        assert output['body']['action']=='propose' and output['body']['text']==expected
        assert output['provider']=='scripted-pi'
        assert output['receipt']['status']=='committed'
        records.append(output)
        return output
    try:
        if channel=='shipment':
            setup = act(SETUP)
            due = setup['receipt']['effect']['effects'][0]['due']
            assert due==route(geography,'place-0','place-1')
            assert task(adjudicator,setup['intent_id'])['status']=='pending'
            world.advance(due-1)
            assert find(world.snapshot()['persons'],person)['location'] is None
            world.advance(due)
            assert find(world.snapshot()['persons'],person)['location']=='place-1'
            assert task(adjudicator,setup['intent_id'])['status']=='completed'
        world.advance(max(DAY,world.snapshot()['time']))
        assert any('咳嗽' in o['text'] or '发热' in o['text'] for o in world.observe(person)['observations'])
        if channel=='shipment':
            investigation = act(modules['scripted_dm'].INVESTIGATE)
            world.advance(investigation['receipt']['effect']['effects'][0]['due'])
            assert task(adjudicator,investigation['intent_id'])['status']=='completed'
        checkpoints['before_proposal'] = world.snapshot()
        target = 'place-10' if channel=='travel' else 'place-0'
        assert healthy(checkpoints['before_proposal'],target)
        if channel=='travel':
            assert find(first['places'],target)['country_id']!=find(first['places'],'place-1')['country_id']
            action = act(TRAVEL)
        elif channel=='shipment':
            action = act(SHIP)
        else:
            action = host.act(person)
            assert action['body']['action']=='propose' and '报信' in action['body']['text']
            assert action['provider']=='scripted-pi' and action['receipt']['status']=='committed'
            records.append(action)
        receipt = action['receipt']
        effect = receipt['effect']['effects'][0]
        due, departed = effect['due'], receipt['time']
        assert due-departed==route(geography,'place-1',target)
        if current: assert 0 < due-departed < DAY, 'positive route must arrive within actual TTL'
        checkpoints['departure'] = world.snapshot()
        source_disease = find(checkpoints['departure']['places'],'place-1')['disease']
        assert sum(c['count'] for c in source_disease['cohorts'] if c['stage'] in ('symptomatic','severe'))>0
        assert healthy(checkpoints['departure'],target)
        assert task(adjudicator,action['intent_id'])['status']=='pending'
        if channel=='message': assert not world.observe(KING)['messages']
        if channel=='shipment': assert len(checkpoints['departure']['in_transit'])==1
        if channel=='travel': assert find(checkpoints['departure']['persons'],person)['location'] is None
        world.advance(due-1)
        checkpoints['due_minus_one'] = world.snapshot()
        assert healthy(checkpoints['due_minus_one'],target)
        assert task(adjudicator,action['intent_id'])['status']=='pending'
        if channel=='message': assert not world.observe(KING)['messages']
        if channel=='travel': assert find(checkpoints['due_minus_one']['persons'],person)['location'] is None
        if channel=='shipment': assert len(checkpoints['due_minus_one']['in_transit'])==1
        world.advance(due)
        checkpoints['arrival'] = world.snapshot()
        terminal = task(adjudicator,action['intent_id'])
        assert terminal['status']=='completed'
        terminal_event = find(checkpoints['arrival']['events'],terminal['event_id'])
        assert terminal_event['kind']=={'travel':'arrival','shipment':'shipment_arrived','message':'message_delivered'}[channel]
        assert terminal_event['cause_id']==terminal['cause_id']
        if channel=='travel': assert find(checkpoints['arrival']['persons'],person)['location']==target
        if channel=='shipment':
            assert not checkpoints['arrival']['in_transit']
            assert find(checkpoints['arrival']['places'],target)['grain']==find(checkpoints['due_minus_one']['places'],target)['grain']+10
        if channel=='message': assert any(m['id']==effect['message_id'] for m in world.observe(KING)['messages'])
        if current:
            disease = find(checkpoints['arrival']['places'],target)['disease']
            rules = first['pathology']
            units = rules['restricted_contacts' if source_disease['restricted'] else 'open_contacts']
            if disease['restricted']: units = units*rules['restricted_contacts']//max(rules['open_contacts'],1)
            expected = min(find(first['places'],target)['population'],units//rules['contacts_per_infection'])
            assert expected>0
            cohorts = disease['cohorts']
            assert len(cohorts)==1 and cohorts[0]['stage']=='latent' and cohorts[0]['count']==expected and cohorts[0]['infected_at']==due
            # Preserve actual implementation metadata without guessing packet key names.
            new_events = [e for e in checkpoints['arrival']['events'] if e['id'] not in {x['id'] for x in checkpoints['due_minus_one']['events']}]
            assert len(new_events)>1, 'actual contact consequence must be auditable alongside terminal'
        else:
            assert healthy(checkpoints['arrival'],target)
        for state in checkpoints.values(): grain(state)
        # Restore an actual persisted Pi session in a second real Node process.
        host.close()
        assert host.worker_exit_code==0 and host.worker_pid is None
        nodes.append(host.worker_exit_code)
        previous = sql_read(dbdir/'actors.sqlite','SELECT messages FROM actor_sessions WHERE person_id=?',(person,))[0][0]
        host = Policy(world,dbdir/'actors.sqlite',adjudicator=adjudicator,dm_provider=FixtureDM(world))
        host.audit = wires
        hosts.append(host)
        restored = act(modules['scripted_dm'].WAIT)
        restored_request = json.loads(sql_read(dbdir/'actors.sqlite','SELECT request FROM actor_intents WHERE intent_id=?',(restored['intent_id'],))[0][0])
        assert restored_request['messages']==json.loads(previous)
        world.control('pause')
        final = world.snapshot()
        server = modules['server'].make_server(world,host,0)
        thread = threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        port = server.server_address[1]
        before = {name:sha(dbdir/name) for name in ('world.sqlite','actors.sqlite')}
        latest = get(port,'/api/state')
        historical = get(port,f"/api/state?seq={checkpoints['before_proposal']['revision']}")
        old_person = get(port,f"/api/person?id={KING}&seq={first['revision']}")
        new_person = get(port,f"/api/person?id={KING}&seq={final['revision']}")
        arrival_http = get(port,f"/api/state?seq={checkpoints['arrival']['revision']}")
        pending_http = get(port,f"/api/state?seq={checkpoints['due_minus_one']['revision']}")
        observed_person = person if channel=='travel' else KING
        pending_person = get(port,f"/api/person?id={observed_person}&seq={checkpoints['due_minus_one']['revision']}")
        arrival_person = get(port,f"/api/person?id={observed_person}&seq={checkpoints['arrival']['revision']}")
        if channel=='message':
            assert not pending_person['messages'] and arrival_person['messages']
        else:
            assert arrival_person['observations'] != pending_person['observations'], 'real arrival notice must supply a nonempty future personal record control'
        assert healthy(historical,target) and healthy(pending_http,target)
        assert not any(p['intent_id']==action['intent_id'] for p in historical['causality']['proposals'])
        assert not any(e['id']==terminal_event['id'] for e in pending_http['events'])
        assert any(e['id']==terminal_event['id'] for e in latest['events'])
        hp = next(p for p in pending_http['causality']['proposals'] if p['intent_id']==action['intent_id'])
        ap = next(p for p in arrival_http['causality']['proposals'] if p['intent_id']==action['intent_id'])
        assert hp['committed']['tasks'][0]['status']=='pending'
        assert ap['committed']['tasks'][0]['status']=='completed'
        if channel=='message':
            assert not old_person['messages'] and new_person['messages']
            old_delivery = get(port,f"/api/person?id={KING}&seq={checkpoints['due_minus_one']['revision']}")
            assert not old_delivery['messages']
        if current: assert not healthy(arrival_http,target)
        # Nonempty future disease control is the REAL source progression in v1;
        # in v2 also the new destination cohort, never a fabricated infection.
        assert find(latest['places'],'place-1')['disease']['cohorts'] != find(first['places'],'place-1')['disease']['cohorts']
        first_http = get(port,f"/api/state?seq={first['revision']}")
        assert first_http['places']==first['places']
        assert before=={name:sha(dbdir/name) for name in before}
        identity = [first[k] for k in ('world_id','scenario_id','scenario_sha256','pathology_sha256')]
        identity += [first['pathology']['version']]
        if current: identity.append(first['pathology_mechanism_sha256'])
        privacy([old_person,new_person,pending_person,arrival_person],identity)
        intents = sql_read(dbdir/'actors.sqlite','SELECT intent_id,person_id,state,request,metrics,receipt,candidate FROM actor_intents ORDER BY rowid')
        sessions = sql_read(dbdir/'actors.sqlite','SELECT person_id,messages,seen FROM actor_sessions')
        calls = sql_read(dbdir/'actors.sqlite','SELECT call_id,intent_id,person_id FROM actor_calls')
        assert len(intents)==len(records)==len(calls)
        assert {(r[1],r[2]) for r in calls}=={(r[0],r[1]) for r in intents}
        assert len({r[0] for r in calls})==len(calls)
        for intent,pid,status,request,metrics,stored_receipt,candidate in intents:
            assert status=='committed' and pid==person
            privacy([json.loads(request),json.loads(candidate)],identity)
            metric = json.loads(metrics)
            assert metric['provider']=='scripted-pi' and metric['sdk_requests']==2
            assert json.loads(stored_receipt)==world.receipt(pid,intent)
        for pid,messages,seen in sessions: privacy(json.loads(messages),identity)
        for wire in wires:
            privacy(wire['request'],identity)
            privacy(wire['result']['messages'],identity)
            assert any(m['role']=='toolResult' for m in wire['result']['messages'])
        assert len(wires)==len(intents)
        children = sql_read(dbdir/'world.sqlite','SELECT id,payload,receipt FROM intents')
        child = next((i,json.loads(p),json.loads(r)) for i,p,r in children if i==terminal['cause_id'])
        assert child[1]['parent']==action['intent_id'] and child[2]['intent_id']==terminal['cause_id']
        sql_first = json.loads(sql_read(dbdir/'world.sqlite','SELECT state FROM snapshots WHERE seq=?',(first['revision'],))[0][0])
        assert sql_first['pathology_sha256']==first['pathology_sha256']
        sql_last = json.loads(sql_read(dbdir/'world.sqlite','SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1')[0][0])
        assert sql_last['revision']==final['revision'] and sql_last['time']==final['time']
        all_snapshots = [json.loads(row[0]) for row in sql_read(dbdir/'world.sqlite','SELECT state FROM snapshots ORDER BY seq')]
        assert all(healthy(s,target) for s in all_snapshots if not current or s['time']<due)
        sql_checkpoints = {}
        for label,state in checkpoints.items():
            raw = json.loads(sql_read(dbdir/'world.sqlite','SELECT state FROM snapshots WHERE seq=?',(state['revision'],))[0][0])
            assert {k:v for k,v in raw.items() if not k.startswith('_')}==state
            sql_checkpoints[label] = raw
        assert len(sql_read(dbdir/'world.sqlite',"SELECT intent_id FROM dm_timeline WHERE kind='commit'"))==len(intents)
        save(directory/'sql-checkpoints.json',sql_checkpoints)
        save(directory/'checkpoints.json',checkpoints)
        save(directory/'actor-provider-wire.json',wires)
        save(directory/'actor-sql-readback.json',{'intents':intents,'sessions':sessions,'calls':calls})
        save(directory/'http-readback.json',{'latest':latest,'historical':historical,'initial':first_http,'pending':pending_http,'arrival':arrival_http,'old_person':old_person,'new_person':new_person,'pending_person':pending_person,'arrival_person':arrival_person})
        save(directory/'proposals-receipts-causality.json',{'records':records,'trace':adjudicator.trace(),'child':child})
        result = {'status':'passed','channel':channel,'countries':count,'database_directory':str(dbdir),
                  'scenario_path':str(scenario),'world_id':first['world_id'],'mission_fixture':mission,
                  'author_intervention':'forward mission card only; no retrospective background field',
                  'intents':len(intents),'sdk_requests':sum(json.loads(r[4])['sdk_requests'] for r in intents),
                  'actor_calls':len(calls),'departed_at':departed,'due':due,'route_seconds':due-departed,
                  'first_revision':first['revision'],'departure_revision':checkpoints['departure']['revision'],
                  'arrival_revision':checkpoints['arrival']['revision'],'terminal':terminal,'terminal_event':terminal_event,
                  'readonly_http_database_sha256':before,'historical_nonempty_controls':['source actual stage progression','future committed task and terminal']+(['actual delivered letter'] if channel=='message' else [])+(['actual destination latent cohort'] if current else []),
                  'destination_result':'new latent on actual arrival' if current else 'healthy throughout; v1 never spreads cross-place',
                  'privacy_scope':'actual provider requests + actual candidate/session/toolResult; finite keys and exact identity controls, NOT general semantic safety'}
    except BaseException:
        save(directory/'failed-case.json',{'status':'failed','error':traceback.format_exc(),'channel':channel,'countries':count,'database_directory':str(dbdir)})
        save(directory/'checkpoints-at-failure.json',checkpoints)
        save(directory/'provider-wire-at-failure.json',wires)
        save(directory/'records-at-failure.json',records)
        raise
    finally:
        if server is not None:
            server.shutdown()
            server.server_close()
            thread.join(5)
            assert not thread.is_alive()
        for item in hosts: item.close()
        for item in hosts:
            assert item.worker_exit_code==0 and item.worker_pid is None
        nodes = [item.worker_exit_code for item in hosts]
        save(directory/'cleanup.json',{'normal_node_exits':nodes,'http_closed':server is not None and not thread.is_alive()})
    result['normal_node_exits'] = nodes
    result['http_closed'] = True
    save(directory/'result.json',result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--legacy-policy-pilot',action='store_true')
    mode.add_argument('--current',action='store_true')
    args = parser.parse_args(argv)
    run_id = uuid.uuid4().hex
    evidence = APP/'evidence'/('p072a-policy-pilot-'+run_id)
    evidence.mkdir()
    database_root = SCRATCH/('p072a-policy-pilot-'+run_id)
    database_root.mkdir(parents=True)
    result = {'status':'failed','current_branch_executed':False,'directory':str(evidence),'database_directory':str(database_root)}
    try:
        before = sources(args.current)
        save(evidence/'source-before.json',before)
        modules = load_boundary(args.current)
        result['current_branch_executed'] = args.current
        cases = [run_case(evidence,database_root,modules,args.current,channel,count)
                 for channel,count in (('travel',6),('shipment',4),('message',5))]
        after = sources(args.current)
        save(evidence/'source-after.json',after)
        assert before==after, 'source fence changed during pilot'
        result.update(status='passed',mode='current-v2' if args.current else 'legacy-policy-pilot',
                      current_branch_executed=args.current,cases=cases,source_fence_equal=True,
                      archived_files=after['archive_files'],worlds=len(cases),
                      intents=sum(c['intents'] for c in cases),sdk_requests=sum(c['sdk_requests'] for c in cases),
                      normal_node_exits=[code for c in cases for code in c['normal_node_exits']],
                      real_model_calls=0,full_release_complete=False,
                      boundary='current real World + current adapters' if args.current else 'frozen 61-file P01.1 World/scenario/disease + current fingerprinted actors/adjudication/server/scripted_dm/pi_worker',
                      limitations=['finite scripted provider and exact-template DM, not real LLM/NLP/personality proof',
                                   'world-only explicit time advancement, not autonomous coordinator scheduling',
                                   'not exhaustive transmission negatives/recovery/retry/full regression/browser acceptance',
                                   'current v2 branch awaits parent execution after backend freeze' if not args.current else 'current three-positive integration only; backend negative suite is separate'])
    except BaseException:
        result['error'] = traceback.format_exc()
        (evidence/'exception.log').write_text(result['error'],encoding='utf-8')
        raise
    finally:
        save(evidence/'p072a-policy-pilot-report.json',result)
        print(canonical(result))


if __name__=='__main__':
    main()
