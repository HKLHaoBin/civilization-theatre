"""Bounded P06.1 two-process acceptance; real SQLite, no HTTP/Pi/LLM.

Run from project root: python -B -m app.verify_preview
Only a new evidence world is mutated; runtime and first-slice archives are untouched.
"""
from contextlib import closing
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import uuid

from app.world import WorldError, WorldStore


PERSON = 'person-country-1-finance'


def _intervening_writer(db_path, result_queue):
    world = WorldStore(db_path)
    snapshot = world.edit_card(PERSON, {'risk': '并发验收：另一进程已提交'})
    result_queue.put({'pid': os.getpid(), 'revision': snapshot['revision']})


def main():
    evidence = Path(__file__).with_name('evidence')
    directory = evidence / ('preview-process-' + uuid.uuid4().hex[:10])
    directory.mkdir(parents=True)
    db_path = directory / 'world.sqlite'
    result = {'kind': 'real_two_process_sqlite_preview_not_http_pi_or_llm', 'status': 'failed',
              'real_models_called': False, 'run_directory': str(directory), 'parent_pid': os.getpid(),
              'sha256': {name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
                         for name in ['world.py', 'verify_preview.py']}}
    try:
        world = WorldStore(db_path)
        world.initialize()
        world.control('resume')
        before = world.snapshot()
        body = {'action': 'ship', 'to': 'place-1', 'quantity': 10}
        intent = 'real-process-stale-ship'
        digest = hashlib.sha256(db_path.read_bytes()).hexdigest()
        draft = world.preview(PERSON, intent, body)
        assert hashlib.sha256(db_path.read_bytes()).hexdigest() == digest
        version = {'world_id': draft['world_id'], 'revision': draft['base_revision']}
        original_mutate = world._mutate
        child_result = {}

        def intervene(operation):
            # Deterministic prepare→effect synchronization, not a random stress race.
            context = multiprocessing.get_context('spawn')
            queue = context.Queue()
            child = context.Process(target=_intervening_writer, args=(str(db_path), queue))
            try:
                child.start()
                child.join(15)
                if child.is_alive():
                    child.terminate()
                    child.join(5)
                    raise RuntimeError('INTERVENING_WRITER_TIMEOUT')
                assert child.exitcode == 0, child.exitcode
                child_result.update(queue.get(timeout=3))
                child_result['exit_code'] = child.exitcode
                assert child_result['pid'] != os.getpid()
                return original_mutate(operation)
            finally:
                if child.is_alive():
                    child.terminate()
                    child.join(5)
                queue.close()
                queue.join_thread()
                if child.pid is not None and not child.is_alive():
                    child.close()

        world._mutate = intervene
        try:
            try:
                world.submit(PERSON, intent, body, expected_version=version)
            except WorldError as error:
                assert error.code == 'STALE_PREVIEW', error.code
                result['stale_error'] = error.code
            else:
                raise AssertionError('stale proposal unexpectedly committed')
        finally:
            world._mutate = original_mutate
        assert world.snapshot()['in_transit'] == []
        with closing(sqlite3.connect(db_path.resolve().as_uri()+'?mode=ro', uri=True)) as db:
            rows = db.execute('SELECT id,person,payload,receipt FROM intents').fetchall()
            assert len(rows) == 1 and rows[0][0] == intent and rows[0][1] == PERSON and rows[0][3] is None
            assert json.loads(rows[0][2])['body'] == body
        fresh = world.preview(PERSON, intent, body)
        assert fresh['base_revision'] == child_result['revision']
        new_version = {'world_id': fresh['world_id'], 'revision': fresh['base_revision']}
        receipt = world.submit(PERSON, intent, body, expected_version=new_version)
        world.control('pause')
        digest = hashlib.sha256(db_path.read_bytes()).hexdigest()
        assert world.submit(PERSON, intent, body, expected_version=new_version) == receipt
        assert hashlib.sha256(db_path.read_bytes()).hexdigest() == digest
        final = world.snapshot()
        assert len(final['in_transit']) == 1 and final['in_transit'][0]['quantity'] == 10
        grain_delta = before['places'][0]['grain'] - final['places'][0]['grain']
        assert grain_delta == 10 and final['time'] == before['time']
        ledger = final['ledger']
        assert (sum(place['grain'] for place in final['places']) + sum(item['quantity'] for item in final['in_transit'])
                + ledger['consumed'] == ledger['initial'] + ledger['harvested'])
        assert all(hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest() == digest
                   for name, digest in result['sha256'].items())
        result.update(status='passed', child=child_result, original_intent=intent,
                      pending_original_intent_preserved=True, original_revision=draft['base_revision'],
                      repreview_revision=fresh['base_revision'], shipment_count=len(final['in_transit']),
                      grain_delta=grain_delta, time_unchanged=True, replay_readonly=True, grain_conserved=True)
    except Exception as error:
        result['error_type'] = type(error).__name__
        raise
    finally:
        (directory / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=True))


if __name__ == '__main__':
    main()
