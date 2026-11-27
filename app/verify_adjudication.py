"""Actual Pi + SQLite + scripted DM P06.2 acceptance, not real-model semantics.

Creates only a fresh evidence directory. Injects one deterministic competing
commit and one lost acknowledgement, then really kills the owned Node child.
"""
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid
from unittest.mock import patch

from app.actors import ActorHost
from app.world import WorldStore, WorldError


PERSON = 'person-country-1-finance'
KING = 'person-country-1-king'
TEXT = '若库存允许，请酌情调粮救济邻城，同时向君主报信并核查本地仓情。'
DM_MARKER = 'DM_ONLY_BACKEND_MARKER'


class ScriptedProposalActor(ActorHost):
    @staticmethod
    def _choose(view, seen):
        if view['person']['office'] == 'finance':
            return {'action': 'propose', 'text': TEXT}, seen
        return ActorHost._choose(view, seen)


def decision(context, *, ruling='成功', quantity=10, unsupported=False):
    """Explicit fixture, not an NLP parser or live model."""
    world = context['world']
    return {'world_id': world['world_id'], 'base_revision': world['revision'],
            'ruling': ruling, 'reason': DM_MARKER + '：按当前原文与条件作脚本裁定',
            'goal': '酌情赈粮、报告君主并核查仓情',
            'costs': [f'从本人有权调度的库存发出{quantity}份粮，实际运输/通信/调查均需模拟时间'],
            'benefits': ['到达后形成真实入仓和合法来信；调查到期后提供有来源的数字'],
            'effects': ([{'action': 'teleport', 'to': 'place-1'}] if unsupported else [
                {'action': 'ship', 'to': 'place-1', 'quantity': quantity},
                {'action': 'send', 'to': KING, 'text': '我已发起赈济运输，粮尚未到达。'},
                {'action': 'investigate'}])}


def run(directory):
    from app.adjudication import Adjudicator
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    world = WorldStore(directory / 'world.sqlite')
    world.initialize()
    world.control('resume')
    initial = world.snapshot()
    judge = Adjudicator(world)
    dm_calls = []
    preview_checks = []
    real_preview = judge.preview

    def readonly_preview(*args, **kwargs):
        before = hashlib.sha256(world.db_path.read_bytes()).hexdigest()
        try:
            value = real_preview(*args, **kwargs)
            preview_checks.append(value['status'])
            return value
        except WorldError as error:
            preview_checks.append(error.code)
            raise
        finally:
            assert hashlib.sha256(world.db_path.read_bytes()).hexdigest() == before
    judge.preview = readonly_preview

    def scripted_dm(context):
        assert context['proposal']['text'] == TEXT
        assert context['proposal']['person_id'] == PERSON
        dm_calls.append(context['world']['revision'])
        if len(dm_calls) == 1:
            # Inject a real other connection's commit after the DM context was read.
            WorldStore(world.db_path).edit_card(PERSON, {'risk': '并发验收：优先保留本地口粮'})
            return decision(context, ruling='大成功', quantity=20)
        return decision(context, ruling='成功', quantity=10)

    host = ScriptedProposalActor(world, directory/'actors.sqlite', adjudicator=judge, dm_provider=scripted_dm)
    try:
        with patch.object(host, '_finish', side_effect=RuntimeError('TEST_LOST_ACK_AFTER_DM_WORLD_COMMIT')):
            try:
                host.act(PERSON)
            except RuntimeError as error:
                assert str(error) == 'TEST_LOST_ACK_AFTER_DM_WORLD_COMMIT'
            else:
                raise AssertionError('lost acknowledgement injection not reached')
        with closing(sqlite3.connect(directory/'actors.sqlite')) as db:
            intent, state, metrics = db.execute('SELECT intent_id,state,metrics FROM actor_intents').fetchone()
        assert state == 'candidate'
        original_receipt = world.receipt(PERSON, intent)
        assert original_receipt is not None and original_receipt['decision_version'] == 2
        history = judge.history(PERSON, intent)
        assert [entry['version'] for entry in history] == [1, 2]
        assert history[0]['decision']['ruling'] == '大成功' and history[1]['decision']['ruling'] == '成功'
        assert [entry['decision']['effects'][0]['quantity'] for entry in history] == [20, 10]
        assert len(dm_calls) == 2 and dm_calls[1] > dm_calls[0]
        assert preview_checks == ['STALE_PREVIEW', 'preview']
        committed = world.snapshot()
        assert committed['places'][0]['grain'] == initial['places'][0]['grain']-10
        assert len(committed['in_transit']) == 1
        pending = [item for item in committed['queue'] if item['kind'] in ['shipment', 'deliver', 'investigation']]
        assert len(pending) == 3 and all(item['at'] > committed['time'] for item in pending)
        assert not world.observe(KING)['messages']
        killed_pid = host.worker_pid
        assert killed_pid is not None
        host._process.kill()
        killed_exit = host._process.wait(5)
    finally:
        host.close()
    restored = ScriptedProposalActor(world, directory/'actors.sqlite', adjudicator=judge, dm_provider=scripted_dm)
    try:
        recovered = restored.act(PERSON)
        assert recovered['reconciled'] and recovered['intent_id'] == intent
        assert recovered['receipt'] == original_receipt and restored.worker_pid is None
        assert len(dm_calls) == 2 and world.snapshot() == committed
        replacement = restored.act(KING)
        assert replacement['body'] == {'action': 'wait'} and restored.worker_pid != killed_pid
        replacement_pid = restored.worker_pid
    finally:
        restored.close()
    assert restored.worker_exit_code == 0
    world.advance(max(item['at'] for item in pending))
    arrived = world.snapshot()
    assert len(world.observe(KING)['messages']) == 1 and not arrived['in_transit']
    assert any('核账' in item['text'] for item in world.observe(PERSON)['observations'])
    ledger = arrived['ledger']
    assert sum(item['grain'] for item in arrived['places']) + ledger['consumed'] == ledger['initial'] + ledger['harvested']
    assert arrived['pathology'] == initial['pathology']
    with closing(sqlite3.connect(directory/'actors.sqlite')) as db:
        records = db.execute('SELECT request,candidate,metrics FROM actor_intents').fetchall()
        sessions = db.execute('SELECT messages FROM actor_sessions').fetchall()
        for request, candidate, _ in records:
            assert DM_MARKER not in request and DM_MARKER not in candidate
            assert 'NON_MEDICAL' not in request and '"pathology"' not in request
        assert all(DM_MARKER not in row[0] for row in sessions)
        sdk_requests = sum(json.loads(row[2])['sdk_requests'] for row in records)
    assert judge.proposal(PERSON, intent)['text'] == TEXT
    unsupported = _unsupported_case(directory/'unsupported')
    return {'kind': 'actual_pi_sqlite_scripted_dm_not_real_llm', 'status': 'passed',
            'run_directory': str(directory), 'original_intent': intent, 'raw_text': TEXT,
            'decision_versions': len(history), 'scripted_dm_calls': len(dm_calls),
            'preview_checks': preview_checks, 'shipment_count': 1, 'grain_debited_at_commit': 10,
            'scheduled_effects': len(pending), 'messages_delivered': len(world.observe(KING)['messages']),
            'simulation_time': arrived['time'], 'proposal_sdk_requests': json.loads(metrics)['sdk_requests'],
            'all_scripted_sdk_requests': sdk_requests, 'killed_node_pid': killed_pid,
            'injected_kill_exit': killed_exit, 'replacement_node_pid': replacement_pid,
            'replacement_node_exit': restored.worker_exit_code, 'unknown_commit_reconciled': True,
            'dm_context_not_in_role_request_or_session': True, 'grain_conserved': True,
            'unsupported_chain': unsupported}


def _unsupported_case(directory):
    from app.adjudication import Adjudicator
    directory.mkdir(parents=True)
    world = WorldStore(directory/'world.sqlite')
    world.initialize()
    world.control('resume')
    judge = Adjudicator(world)
    before = world.snapshot()
    knowledge = world.observe(PERSON)
    provider_calls = []
    def unsupported_dm(context):
        provider_calls.append(1)
        return decision(context, unsupported=True)
    host = ScriptedProposalActor(world, directory/'actors.sqlite', adjudicator=judge, dm_provider=unsupported_dm)
    try:
        try:
            host.act(PERSON)
        except WorldError as error:
            assert 'UNSUPPORTED' in error.code, error.code
            code = error.code
        else:
            raise AssertionError('unsupported effect was accepted')
    finally:
        host.close()
    after = world.snapshot()
    assert after['status'] == 'technical_paused'
    for key in ['events', 'places', 'queue', 'in_transit', 'ledger', 'time', 'pathology']:
        assert after[key] == before[key], key
    after_knowledge = world.observe(PERSON)
    assert after_knowledge['revision'] == after['revision']
    assert knowledge['revision'] == before['revision']
    assert {k:v for k,v in after_knowledge.items() if k != 'revision'} == {
        k:v for k,v in knowledge.items() if k != 'revision'}
    with closing(sqlite3.connect(directory/'actors.sqlite')) as db:
        intent, state, metrics = db.execute('SELECT intent_id,state,metrics FROM actor_intents').fetchone()
    assert state == 'candidate' and world.receipt(PERSON, intent) is None
    assert len(judge.history(PERSON, intent)) == 1 and len(provider_calls) == 1
    assert judge.proposal(PERSON, intent)['text'] == TEXT and host.worker_exit_code == 0
    try:
        world.control('resume')
    except WorldError:
        pass
    else:
        raise AssertionError('ordinary resume bypassed technical pause')
    return {'status': 'passed', 'error_code': code, 'world_status': after['status'],
            'no_fictional_failure_or_effect': True, 'raw_intent_and_decision_retained': True,
            'scripted_dm_calls': len(provider_calls), 'sdk_requests': json.loads(metrics)['sdk_requests'],
            'node_exit': host.worker_exit_code}


def main():
    directory = Path(__file__).with_name('evidence')/('p062-integration-'+uuid.uuid4().hex[:10])
    directory.mkdir(parents=True)
    result = {'status': 'failed', 'kind': 'actual_pi_sqlite_scripted_dm_not_real_llm', 'run_directory': str(directory)}
    try:
        result = run(directory)
    finally:
        (directory/'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=True))


if __name__ == '__main__':
    main()
