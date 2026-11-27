"""P07.1 real Pi/scripted-policy + HTTP/SQLite acceptance, not real-model testing.

Creates only a new evidence world. The king policy fixture explicitly chooses
an intervention order from an actually delivered symptom report; it is not
claimed as LLM agency, and the DM never chooses the marshal's response.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
from urllib.request import urlopen
import uuid
from .adjudication import Adjudicator
from .scripted_dm import DefaultActors, ScriptedDM
from .server import make_server
from .world import WorldStore, DAY

KING = 'person-country-1-king'
MARSHAL = 'person-country-1-marshal'


class InterventionPolicy(DefaultActors):
    @staticmethod
    def _choose(view, seen):
        if view['person']['office'] == 'king':
            for message in view['messages']:
                key = 'p071-report:'+message['id']
                if key not in seen and ('咳嗽' in message['text'] or '发热' in message['text']):
                    contact = next(p for p in view['contacts'] if p['office'] == 'marshal')
                    text = f"向「{contact['name']}」发出命令：已收到你的现场报告，请依本人职责自主决定是否限制聚集。"
                    return {'action':'propose','text':text}, [key]
        return DefaultActors._choose(view, seen)


def fingerprint(pathology):
    value = json.dumps(pathology,ensure_ascii=False,sort_keys=True,separators=(',', ':'),allow_nan=False)
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def get(port, route):
    with urlopen(f'http://127.0.0.1:{port}'+route,timeout=10) as response:
        return json.load(response)


def run(directory):
    w = WorldStore(directory/'world.sqlite')
    initial = w.initialize()
    assert initial['pathology_sha256'] == fingerprint(initial['pathology'])
    assert initial['pathology_frozen_at'] == 0
    a = Adjudicator(w)
    h = InterventionPolicy(w,directory/'actors.sqlite',adjudicator=a,dm_provider=ScriptedDM(w))
    server = None
    try:
        w.control('resume')
        w.advance(DAY)
        assert any('咳嗽' in o['text'] for o in w.observe(MARSHAL)['observations'])
        assert not w.observe(KING)['messages']
        report = h.act(MARSHAL)
        assert report['body']['action']=='propose'
        w.advance(report['receipt']['effect']['effects'][0]['due'])
        assert w.observe(KING)['messages']
        order = h.act(KING)
        order_effect = order['receipt']['effect']['effects'][0]
        command_id = order_effect['command_id']
        w.advance(order_effect['due'])
        before = w.snapshot()
        assert not before['places'][1]['disease']['restricted']
        cohorts = json.loads(json.dumps(before['places'][1]['disease']['cohorts']))
        response = h.act(MARSHAL)
        effect = response['receipt']['effect']['effects'][0]
        assert effect['accepted'] is True and effect['command_id']==command_id
        restriction = effect['contact_restriction']
        assert restriction == dict(place_id='place-1',before=False,after=True,mechanism_sha256=initial['pathology_sha256'])
        committed = w.snapshot()
        assert committed['places'][1]['disease']['restricted']
        assert committed['places'][1]['disease']['cohorts']==cohorts
        events = [event for event in committed['events'] if event['kind']=='contact_restriction_applied']
        assert len(events)==1 and events[0]['cause_id']
        assert a.commit(MARSHAL,response['intent_id'],response['receipt']['decision_version']) == response['receipt']
        assert w.snapshot()==committed
        assert initial['pathology_sha256'] not in json.dumps(w.observe(MARSHAL),ensure_ascii=False)
        assert initial['pathology']['version'] not in json.dumps(w.observe(MARSHAL),ensure_ascii=False)
        w.advance(4*DAY)
        w.control('pause')
        final = w.snapshot()
        assert final['pathology']==initial['pathology']
        assert final['pathology_sha256']==initial['pathology_sha256']
        baseline = WorldStore(directory/'counterfactual.sqlite')
        baseline.initialize(); baseline.control('resume'); unmitigated = baseline.advance(4*DAY)
        actual_infections = sum(c['count'] for c in final['places'][1]['disease']['cohorts'])
        control_infections = sum(c['count'] for c in unmitigated['places'][1]['disease']['cohorts'])
        assert actual_infections < control_infections
        assert final['places'][1]['disease']['cohorts'][0]['infected_at']==0
        assert final['places'][1]['disease']['cohorts'][0]['count']==cohorts[0]['count']
        assert final['ledger']==unmitigated['ledger']
        server = make_server(w,h,0)
        thread = threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        port = server.server_address[1]
        world_hash = hashlib.sha256((directory/'world.sqlite').read_bytes()).hexdigest()
        actors_hash = hashlib.sha256((directory/'actors.sqlite').read_bytes()).hexdigest()
        latest = get(port,'/api/state')
        earlier = get(port,f"/api/state?seq={before['revision']}")
        old_person = get(port,f"/api/person?id={MARSHAL}&seq={before['revision']}")
        current_person = get(port,f"/api/person?id={MARSHAL}&seq={final['revision']}")
        visible = next(p for p in latest['causality']['proposals'] if p['intent_id']==response['intent_id'])
        assert visible['committed']['effects'][0]['contact_restriction']==restriction
        assert not any(p['intent_id']==response['intent_id'] for p in earlier['causality']['proposals'])
        assert not earlier['places'][1]['disease']['restricted']
        assert initial['pathology_sha256'] not in json.dumps([old_person,current_person],ensure_ascii=False)
        assert hashlib.sha256((directory/'world.sqlite').read_bytes()).hexdigest()==world_hash
        assert hashlib.sha256((directory/'actors.sqlite').read_bytes()).hexdigest()==actors_hash
        with closing(sqlite3.connect((directory/'world.sqlite').resolve().as_uri()+'?mode=ro',uri=True)) as db:
            assert db.execute('SELECT count(*) FROM dm_proposals').fetchone()[0]==3
            assert db.execute("SELECT count(*) FROM dm_timeline WHERE kind='commit'").fetchone()[0]==3
            first = json.loads(db.execute('SELECT state FROM snapshots WHERE seq=1').fetchone()[0])
            assert fingerprint(first['pathology'])==first['pathology_sha256']
            child = [row[0] for row in db.execute('SELECT id,payload FROM intents') if json.loads(row[1]).get('parent')==response['intent_id']]
            assert child == [events[0]['cause_id']]
        with closing(sqlite3.connect((directory/'actors.sqlite').resolve().as_uri()+'?mode=ro',uri=True)) as db:
            metrics = [json.loads(row[0]) for row in db.execute('SELECT metrics FROM actor_intents')]
            assert len(metrics)==3 and sum(m['sdk_requests'] for m in metrics)==6
        result = dict(status='passed',kind=__doc__,run_directory=str(directory),
                    pathology_sha256=initial['pathology_sha256'],original_cohorts_preserved=True,
                    actual_infections=actual_infections,counterfactual_infections=control_infections,
                    contact_restriction=restriction,cause_event=events[0],historical_revision=before['revision'],
                    intervention_revision=committed['revision'],final_revision=final['revision'],
                    response_intent=response['intent_id'],real_pi_intents=3,scripted_provider_requests=6,
                    http_history_and_person_readonly=True,grain_ledger_unchanged_by_intervention=True)
    finally:
        if server:
            server.shutdown();server.server_close();thread.join(5)
        h.close()
        assert h.worker_exit_code==0
    return result | {'node_exit':h.worker_exit_code}


def main():
    directory = Path(__file__).with_name('evidence')/('p071-integration-'+uuid.uuid4().hex[:10])
    directory.mkdir(parents=True)
    result = dict(status='failed',run_directory=str(directory),kind=__doc__)
    try:
        result = run(directory)
    finally:
        (directory/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
        print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':
    main()
