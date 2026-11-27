"""Trusted-host adjudication. Projected states must never enter actor context."""
from contextlib import closing, contextmanager
import json
import sqlite3
from .world import WorldError, _json, _history_cover


def activation_api(method):
    """Keep explicit references consistent with independently captured authority."""
    from functools import wraps
    @wraps(method)
    def guarded(self, *args, **kwargs):
        from .coordinator_scheduler import activation_scope
        with activation_scope(self.world, kwargs.get('activation')):
            return method(self, *args, **kwargs)
    return guarded


_NO_PHYSICAL_ATTEMPT = object()


class Adjudicator:
    def __init__(self, world):
        self.world = world
        def initialize(state, db):
            db.execute('CREATE TABLE IF NOT EXISTS dm_proposals (intent_id TEXT PRIMARY KEY, world_id TEXT NOT NULL, person_id TEXT NOT NULL, text TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS dm_decisions (intent_id TEXT NOT NULL, version INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY(intent_id,version))')
            db.execute('CREATE TABLE IF NOT EXISTS dm_timeline (intent_id TEXT NOT NULL, kind TEXT NOT NULL, version INTEGER NOT NULL, visible_from INTEGER NOT NULL, PRIMARY KEY(intent_id,kind,version))')
            for table in ('dm_proposals', 'dm_decisions', 'dm_timeline'):
                for operation in ('UPDATE', 'DELETE'):
                    db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT, 'immutable adjudication'); END")
            _history_cover(db)
        world._mutate(initialize)

    @contextmanager
    def _read(self):
        from .coordinator_scheduler import source_lock
        with source_lock(self.world), self.world._connection(readonly=True) as db:
            yield db

    def trace(self, seq=None):
        """Observer-only evidence pinned to one immutable world snapshot.

        Unanchored legacy metadata is intentionally omitted: its record time is
        unknowable, and a decision's base revision is not its visibility time.
        """
        if seq is not None and (type(seq) is not int or seq < 1):
            raise WorldError('INVALID_SEQUENCE', '提交序号必须为正整数')
        if not self.world.db_path.exists():
            raise WorldError('NOT_INITIALIZED', '世界尚未初始化')
        with self._read() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1' if seq is None else 'SELECT state FROM snapshots WHERE seq=?', () if seq is None else (seq,)).fetchone()
            if row is None:
                raise WorldError('UNKNOWN_SNAPSHOT', '不存在该提交序号')
            state = json.loads(row[0])
            revision = state['revision']
            result = dict(world_id=state['world_id'], revision=revision, proposals=[])
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'dm_timeline','dm_proposals','dm_decisions','intents'} <= tables:
                return result
            people = {p['id']:p for p in state['persons']}
            rows = db.execute("SELECT p.intent_id,p.person_id,p.text,t.visible_from FROM dm_proposals p JOIN dm_timeline t ON t.intent_id=p.intent_id AND t.kind='proposal' AND t.version=0 WHERE t.visible_from<=? AND p.world_id=? ORDER BY t.visible_from,p.intent_id", (revision,state['world_id'])).fetchall()
            for intent, person, text, recorded in rows:
                proposal = dict(intent_id=intent, person_id=person, person_name=people[person]['name'], text=text, recorded_revision=recorded, decisions=[], committed=None)
                decisions = db.execute("SELECT d.version,d.data,t.visible_from FROM dm_decisions d JOIN dm_timeline t ON t.intent_id=d.intent_id AND t.kind='decision' AND t.version=d.version WHERE d.intent_id=? AND t.visible_from<=? ORDER BY d.version", (intent,revision)).fetchall()
                anchor = db.execute("SELECT version,visible_from FROM dm_timeline WHERE intent_id=? AND kind='commit' AND visible_from<=?", (intent,revision)).fetchone()
                receipt_row = db.execute('SELECT receipt FROM intents WHERE id=?', (intent,)).fetchone()
                receipt = json.loads(receipt_row[0]) if receipt_row and receipt_row[0] else None
                committed_version = anchor[0] if anchor and receipt and receipt.get('decision_version') == anchor[0] and receipt.get('world_id') == state['world_id'] and receipt.get('person_id') == person and receipt.get('intent_id') == intent else None
                for version, data, visible in decisions:
                    decision = json.loads(data)
                    status = 'committed' if version == committed_version else 'superseded' if version != decisions[-1][0] else 'stale' if decision['base_revision'] != revision else 'draft'
                    proposal['decisions'].append(dict(version=version, recorded_revision=visible, **{k:decision[k] for k in ('base_revision','ruling','reason','goal','costs','benefits','effects')}, status=status))
                if committed_version is not None:
                    committed = dict(decision_version=committed_version, revision=anchor[1], time=receipt['time'], effects=receipt['effect']['effects'], resource_changes=[], tasks=[])
                    pair = {r[0]:json.loads(r[1]) for r in db.execute('SELECT seq,state FROM snapshots WHERE seq IN (?,?)', (anchor[1]-1,anchor[1]))}
                    before, after = pair.get(anchor[1]-1), pair.get(anchor[1])
                    if before is not None and after is not None:
                        old_places = {p['id']:p for p in before['places']}
                        for place in after['places']:
                            previous = old_places[place['id']]['grain']
                            if place['grain'] != previous:
                                committed['resource_changes'].append(dict(place_id=place['id'], name=place['name'], resource='grain', before=previous, after=place['grain'], delta=place['grain']-previous))
                        if 'economy' in before and 'economy' in after:
                            old_e, new_e = before['economy'], after['economy']
                            for place in after['places']:
                                pid = place['id']
                                for resource, unit in new_e['units'].items():
                                    if resource == 'grain':
                                        continue
                                    previous, current = old_e['materials'][pid][resource], new_e['materials'][pid][resource]
                                    reserved = [sum(t['reserved_inputs'].get(resource,0) for t in e['tasks']
                                                    if t['status']=='pending' and t['place_id']==pid) for e in (old_e,new_e)]
                                    if previous != current or reserved[0] != reserved[1]:
                                        committed['resource_changes'].append(dict(place_id=pid,name=place['name'],resource=resource,
                                            unit=unit,before=previous,after=current,delta=current-previous,
                                            reserved_before=reserved[0],reserved_after=reserved[1],reserved_delta=reserved[1]-reserved[0]))
                    kinds = {'task_id':'investigation','message_id':'deliver','shipment_id':'shipment','journey_id':'arrival','production_id':'production','material_shipment_id':'material_shipment','military_journey_id':'military_arrival','project_task_id':'project_stage'}
                    terminals = {'investigation':{'investigation_completed':'completed','investigation_interrupted':'interrupted'}, 'deliver':{'message_delivered':'completed','undeliverable':'undelivered'}, 'shipment':{'shipment_arrived':'completed'}, 'arrival':{'arrival':'completed'}, 'production':{'production_completed':'completed'}, 'material_shipment':{'material_shipment_arrived':'completed'},'military_arrival':{'military_arrived':'completed'},'project_stage':{'project_stage_completed':'completed','project_stage_failed':'failed','project_stage_interrupted':'interrupted'}}
                    for index, effect in enumerate(committed['effects']):
                        if 'due' not in effect:
                            continue
                        cause = self._child_id(intent, committed_version, index)
                        original = next((q for q in after['queue'] if q.get('cause_id') == cause), None) if after else None
                        kind = original['kind'] if original else next((kind for key,kind in kinds.items() if key in effect), 'unknown')
                        task = dict(cause_id=cause, kind=kind, due=effect['due'], status='untracked')
                        if original:
                            terminal = next((e for e in state['events'] if e.get('cause_id') == cause and e['kind'] in terminals.get(kind,{})), None)
                            if terminal:
                                task.update(status=terminals[kind][terminal['kind']], event_id=terminal['id'])
                            elif any(q.get('cause_id') == cause for q in state['queue']):
                                task['status'] = 'pending'
                        committed['tasks'].append(task)
                    proposal['committed'] = committed
                result['proposals'].append(proposal)
            return result

    def _identity(self, person_id, intent_id):
        self.world._text(person_id, 'UNKNOWN_PERSON')
        self.world._text(intent_id, 'INVALID_INTENT_ID')

    def _proposal(self, db, person_id, intent_id, state=None):
        row = db.execute('SELECT world_id,person_id,text FROM dm_proposals WHERE intent_id=?', (intent_id,)).fetchone()
        if row is None:
            raise WorldError('UNKNOWN_PROPOSAL', '原始提案不存在')
        if row[1] != person_id:
            raise WorldError('ID_PERSON_CONFLICT', '业务ID绑定其他人物')
        if state is not None and row[0] != state['world_id']:
            raise WorldError('WORLD_MISMATCH', '提案不属于此世界')
        return dict(world_id=row[0], person_id=row[1], intent_id=intent_id, text=row[2])

    @activation_api
    def propose(self, person_id, intent_id, text, *, execution=None, activation=None, physical_attempt=_NO_PHYSICAL_ATTEMPT):
        self._identity(person_id, intent_id)
        self.world._text(text)
        body = {'action':'propose','text':text}
        if physical_attempt is not _NO_PHYSICAL_ATTEMPT:
            from .actor_execution import valid_physical_attempt
            if not valid_physical_attempt(physical_attempt):
                raise WorldError('INVALID_BODY','非法原人物物理意图锚')
            body['physical_attempt'] = self._freeze(physical_attempt)
        def persist(state, db):
            if 'physical_attempt' in body and (
                    state['_config'].get('event_wakeup',{}).get('version') != 'p04-event-wakeup-v2-lifecycle'
                    or state['_config'].get('lifecycle',{}).get('version') != 'p09-lifecycle-v1'
                    or state.get('_lifecycle',{}).get('version') != 'p09-lifecycle-v1'):
                raise WorldError('INVALID_BODY','旧世界不可消费v3意图')
            self.world._find(state['persons'], person_id, 'UNKNOWN_PERSON')
            from .coordinator_scheduler import durable_body
            durable_body(self.world, db, state, person_id, intent_id, body, execution)
            row = db.execute('SELECT person,payload FROM intents WHERE id=?', (intent_id,)).fetchone()
            payload = _json({'body': body})
            if row:
                original = self._proposal(db, person_id, intent_id, state)
                if original['text'] != text or tuple(row) != (person_id, payload):
                    raise WorldError('ID_PAYLOAD_CONFLICT', '同业务ID不可改变人物或原文')
                return original
            db.execute('INSERT INTO intents VALUES (?,?,?,NULL)', (intent_id, person_id, payload))
            db.execute('INSERT INTO dm_proposals VALUES (?,?,?,?)', (intent_id, state['world_id'], person_id, text))
            db.execute('INSERT INTO dm_timeline VALUES (?,?,?,?)', (intent_id, 'proposal', 0, state['revision']+1))
            return self._proposal(db, person_id, intent_id, state)
        return self.world._mutate(persist, execution=execution, person_id=person_id)

    @staticmethod
    def _freeze(value):
        def check(item):
            if type(item) is dict:
                if any(type(k) is not str for k in item):
                    raise ValueError('JSON keys')
                for child in item.values():
                    check(child)
            elif type(item) is list:
                for child in item:
                    check(child)
            elif type(item) not in (str, int, float, bool, type(None)):
                raise ValueError('JSON type')
        try:
            check(value)
            return json.loads(_json(value))
        except (ValueError, TypeError, RecursionError):
            raise WorldError('INVALID_DECISION', '裁定必须为合法JSON') from None

    @activation_api
    def record(self, person_id, intent_id, decision, *, execution=None, activation=None):
        self._identity(person_id, intent_id)
        decision = self._freeze(decision)
        fields = {'world_id','base_revision','ruling','reason','goal','costs','benefits','effects'}
        if type(decision) is not dict or set(decision) != fields:
            raise WorldError('INVALID_DECISION', '非法裁定字段')
        if type(decision['base_revision']) is not int or decision['base_revision'] < 1:
            raise WorldError('INVALID_BASE_VERSION', '裁定基准必须为正整数')
        for key in ('world_id','reason','goal'):
            self.world._text(decision[key], 'INVALID_DECISION')
        if type(decision['ruling']) is not str or decision['ruling'] not in ('大失败','失败','成功','大成功'):
            raise WorldError('INVALID_RULING', '非法四档裁定')
        for key in ('costs','benefits'):
            if type(decision[key]) is not list or len(decision[key]) > 8:
                raise WorldError('INVALID_DECISION', '非法代价收益列表')
            for text in decision[key]:
                self.world._text(text, 'INVALID_DECISION')
        if type(decision['effects']) is not list or not 1 <= len(decision['effects']) <= 8 or any(type(e) is not dict for e in decision['effects']):
            raise WorldError('INVALID_DECISION', '非法效果列表')
        data = _json(decision)
        def persist(state, db):
            self._bound_proposal(db, state, person_id, intent_id, execution)
            from .coordinator_scheduler import rejected_result
            if rejected_result(self.world,db,person_id,intent_id,execution) is not None:
                raise WorldError('ALREADY_REJECTED', '已拒绝意图不可重裁')
            if decision['world_id'] != state['world_id']:
                raise WorldError('WORLD_MISMATCH', '裁定不属于此世界')
            if db.execute('SELECT receipt FROM intents WHERE id=?', (intent_id,)).fetchone()[0]:
                raise WorldError('ALREADY_COMMITTED', '已提交意图不可重裁')
            row = db.execute('SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1', (intent_id,)).fetchone()
            if row and row[1] == data:
                return row[0]
            version = row[0]+1 if row else 1
            db.execute('INSERT INTO dm_decisions VALUES (?,?,?)', (intent_id, version, data))
            db.execute('INSERT INTO dm_timeline VALUES (?,?,?,?)', (intent_id, 'decision', version, state['revision']+1))
            return version
        return self.world._mutate(persist, execution=execution, person_id=person_id)

    def history(self, person_id, intent_id):
        self._identity(person_id, intent_id)
        with self._read() as db:
            db.execute('BEGIN')
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            self._proposal(db, person_id, intent_id, state)
            return [{'version':r[0], 'decision':json.loads(r[1])} for r in db.execute('SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version', (intent_id,))]

    def _bound_proposal(self, db, state, person_id, intent_id, execution):
        proposal = self._proposal(db, person_id, intent_id, state)
        from .coordinator_scheduler import durable_body
        row = db.execute('SELECT person,payload FROM intents WHERE id=?',(intent_id,)).fetchone()
        if row is None or row[0] != person_id:
            raise WorldError('ACTOR_RECOVERY_UNPROVEN','原主意图缺失或换人')
        body = json.loads(row[1]).get('body')
        if type(body) is not dict or body.get('action') != 'propose' or body.get('text') != proposal['text']:
            raise WorldError('ACTOR_RECOVERY_UNPROVEN','原提案与主意图不符')
        if set(body) == {'action','text','physical_attempt'}:
            from .actor_execution import valid_physical_attempt
            if (not valid_physical_attempt(body['physical_attempt'])
                    or state['_config'].get('event_wakeup',{}).get('version') != 'p04-event-wakeup-v2-lifecycle'
                    or state['_config'].get('lifecycle',{}).get('version') != 'p09-lifecycle-v1'
                    or state.get('_lifecycle',{}).get('version') != 'p09-lifecycle-v1'):
                raise WorldError('ACTOR_RECOVERY_UNPROVEN','非法或旧版本物理意图锚')
            proposal['physical_attempt'] = body['physical_attempt']
        elif set(body) != {'action','text'}:
            raise WorldError('ACTOR_RECOVERY_UNPROVEN','原提案字段非法')
        durable_body(self.world, db, state, person_id, intent_id,body,execution)
        return proposal

    def _load(self, db, state, person_id, intent_id, version):
        if type(version) is not int or version < 1:
            raise WorldError('INVALID_DECISION_VERSION', '裁定版本必须为正整数')
        self._proposal(db, person_id, intent_id, state)
        row = db.execute('SELECT version,data FROM dm_decisions WHERE intent_id=? ORDER BY version DESC LIMIT 1', (intent_id,)).fetchone()
        if row is None:
            raise WorldError('UNKNOWN_DECISION', '裁定不存在')
        if row[0] != version:
            raise WorldError('SUPERSEDED_DECISION', '裁定版本已被取代')
        decision = json.loads(row[1])
        if decision['world_id'] != state['world_id']:
            raise WorldError('WORLD_MISMATCH', '裁定不属于此世界')
        if decision['base_revision'] != state['revision']:
            raise WorldError('STALE_PREVIEW', '裁定基准已过期')
        if state['status'] != 'running':
            raise WorldError('WORLD_PAUSED', '世界没有运行')
        return decision

    def _compile(self, decision, lifecycle_version=None):
        for body in decision['effects']:
            if (lifecycle_version == 'p09-lifecycle-v1'
                    and body.get('action') in ('resolve_physical_attempt','write_office_document','read_office_document')):
                from .world import _lifecycle_effect_body
                _lifecycle_effect_body(body)
                continue
            if body.get('action') in ('start_project','continue_project','interrupt_project','share_project_knowledge'):
                self.world._project_body(body)
                continue
            if body.get('action') in ('military_move', 'military_supply', 'military_garrison', 'military_engage'):
                self.world._military_body(body)
                continue
            if body.get('action') == 'start_production':
                self.world._production_body(body)
                continue
            if body.get('action') == 'ship_material':
                self.world._material_transport_body(body)
                continue
            if body.get('action') not in ('wait','investigate','send','travel','ship','command','respond','meet'):
                raise WorldError('UNSUPPORTED', '裁定效果不支持')
            self.world._validate_body(body)
        return decision['effects']

    @staticmethod
    def _child_id(intent_id, version, index):
        # Length-delimited parent identity; namespace is additionally reserved in SQL.
        return f'dm:{len(intent_id)}:{intent_id}:{version}:{index}'

    def _execute(self, state, db, person_id, intent_id, version, decision, persist):
        proof = self.world._lifecycle_check(db, state)
        bodies = self._compile(decision, lifecycle_version=None if proof is None else proof['lifecycle']['version'])
        if (any(body['action'] in ('start_production','ship_material','military_move','military_supply','military_garrison','military_engage','start_project','continue_project','interrupt_project','share_project_knowledge') for body in bodies)
                and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='coordinator_scheduler_registration'").fetchone() is not None):
            from .coordinator_scheduler import scoped, activation_row
            if scoped(self.world, None) is None:
                raise WorldError('UNSUPPORTED', '生产效果必须持原登记激活')
            attempt = self.world._scoped_attempt()
            if attempt is None:
                raise WorldError('ACTOR_EXECUTION_REQUIRED', '生产效果必须持原人物执行尝试')
            self.world._check_actor_execution(db, state['world_id'], person_id, attempt.permit, readonly=not persist)
            activation_row(self.world, db, state, person_id, execution=attempt.permit, intent=intent_id)
        person = self.world._find(state['persons'], person_id, 'UNKNOWN_PERSON')
        effects = []
        for index, body in enumerate(bodies):
            child = self._child_id(intent_id, version, index)
            if db.execute('SELECT 1 FROM intents WHERE id=?', (child,)).fetchone():
                raise WorldError('ID_PAYLOAD_CONFLICT', '子效果业务ID已被占用')
            if persist:
                db.execute('INSERT INTO intents VALUES (?,?,?,NULL)', (child, person_id, _json({'parent':intent_id, 'version':version, 'body':body})))
            queued = {q['id'] for q in state['queue']}
            from .coordinator_scheduler import child_effect
            with child_effect(self.world, db, person_id, intent_id, child):
                from .world import DomainActionError
                try:
                    effect = self.world._apply_action(state, person, child, body, decision['ruling'])
                except DomainActionError as error:
                    error.effect_index = index
                    raise
            for task in state['queue']:
                if task['id'] not in queued:
                    task['cause_id'] = child
            effects.append(effect)
            self.world._event(state, 'action', person['name']+'：'+body['action']+'；'+decision['reason'], person_id)
            if persist:
                receipt = dict(world_id=state['world_id'], person_id=person_id, intent_id=child,
                               status='committed', decision_version=version, ruling=decision['ruling'], time=state['time'], effect=effect)
                db.execute('UPDATE intents SET receipt=? WHERE id=?', (_json(receipt), child))
        return effects

    @activation_api
    def preview(self, person_id, intent_id, version, *, execution=None, activation=None):
        self._identity(person_id, intent_id)
        with self._read() as db:
            db.execute('BEGIN')
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            self.world._check_actor_execution(db, state['world_id'], person_id, execution, readonly=True)
            self._bound_proposal(db, state, person_id, intent_id, execution)
            from .coordinator_scheduler import rejected_result
            if rejected_result(self.world,db,person_id,intent_id,execution) is not None:
                raise WorldError('ALREADY_REJECTED', '已拒绝意图应读取原结果')
            if db.execute('SELECT receipt FROM intents WHERE id=?', (intent_id,)).fetchone()[0]:
                raise WorldError('ALREADY_COMMITTED', '已提交意图应读取原回执')
            decision = self._load(db, state, person_id, intent_id, version)
            self.world._restore_geography(db, state)
            effects = self._execute(state, db, person_id, intent_id, version, decision, False)
            base = state['revision']
            state['revision'] += 1
            return dict(status='preview', world_id=state['world_id'], base_revision=base,
                        decision_version=version, effects=effects, projected=self.world._public(state))

    @activation_api
    def commit(self, person_id, intent_id, version, *, execution=None, activation=None):
        self._identity(person_id, intent_id)
        if type(version) is not int or version < 1:
            raise WorldError('INVALID_DECISION_VERSION', '裁定版本必须为正整数')
        def apply(state, db):
            self._bound_proposal(db, state, person_id, intent_id, execution)
            from .coordinator_scheduler import rejected_result
            rejected=rejected_result(self.world,db,person_id,intent_id,execution)
            if rejected is not None:
                if rejected['decision_version']!=version:
                    raise WorldError('ALREADY_REJECTED', '已按其他版本拒绝')
                return rejected
            row = db.execute('SELECT receipt FROM intents WHERE id=?', (intent_id,)).fetchone()
            if row[0]:
                receipt = json.loads(row[0])
                if receipt['decision_version'] != version:
                    raise WorldError('ALREADY_COMMITTED', '已按其他版本提交')
                return receipt
            decision = self._load(db, state, person_id, intent_id, version)
            proof = self.world._lifecycle_check(db, state)
            self._compile(decision, lifecycle_version=None if proof is None else proof['lifecycle']['version'])
            # Schema/unsupported faults remain outside the original savepoint business region.
            from .coordinator_scheduler import effect_or_reject
            effects = effect_or_reject(self.world,db,state,person_id,intent_id,execution,
                lambda tentative: self._execute(tentative,db,person_id,intent_id,version,decision,True),
                decision=decision,version=version)
            if type(effects) is dict and effects.get('status') == 'domain_rejected':
                return effects
            receipt = dict(world_id=state['world_id'], person_id=person_id, intent_id=intent_id,
                           status='committed', decision_version=version, ruling=decision['ruling'],
                           time=state['time'], effect={'effects':effects})
            db.execute('UPDATE intents SET receipt=? WHERE id=?', (_json(receipt), intent_id))
            db.execute('INSERT INTO dm_timeline VALUES (?,?,?,?)', (intent_id, 'commit', version, state['revision']+1))
            from .coordinator_scheduler import committed
            committed(self.world, db, state, person_id, intent_id, receipt, execution)
            return receipt
        try:
            return self.world._mutate(apply, execution=execution, person_id=person_id)
        except WorldError as error:
            if error.code == 'UNSUPPORTED':
                self.world.technical_pause('ADJUDICATION_UNSUPPORTED', execution=execution)
                raise WorldError('ADJUDICATION_UNSUPPORTED', '后果执行能力未实现，整局技术暂停') from None
            raise

    def _execution_receipt(self, person_id, intent_id, execution):
        with self._read() as db:
            db.execute('BEGIN')
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            self.world._check_actor_execution(db, state['world_id'], person_id, execution, readonly=True)
            self._bound_proposal(db, state, person_id, intent_id, execution)
            row = db.execute('SELECT person,receipt FROM intents WHERE id=?', (intent_id,)).fetchone()
            if row and row[0] != person_id:
                raise WorldError('ID_PERSON_CONFLICT', '业务ID绑定其他人物')
            from .coordinator_scheduler import rejected_result
            rejected=rejected_result(self.world,db,person_id,intent_id,execution)
            return rejected if rejected is not None else (json.loads(row[1]) if row and row[1] else None)

    @activation_api
    def resolve(self, person_id, intent_id, text, provider, *, execution=None, activation=None, physical_attempt=_NO_PHYSICAL_ATTEMPT):
        options = {} if physical_attempt is _NO_PHYSICAL_ATTEMPT else {'physical_attempt':physical_attempt}
        proposal = self.propose(person_id, intent_id, text, execution=execution, **options)
        if physical_attempt is not _NO_PHYSICAL_ATTEMPT:
            with self._read() as db:
                db.execute('BEGIN')
                state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
                proposal = self._bound_proposal(db,state,person_id,intent_id,execution)
        receipt = self._execution_receipt(person_id, intent_id, execution)
        if receipt is not None:
            return receipt
        retryable = {'STALE_PREVIEW', 'SUPERSEDED_DECISION'}
        for attempt in range(3):
            try:
                receipt = self._execution_receipt(person_id, intent_id, execution)
                if receipt is not None:
                    return receipt
                state = self.world.snapshot()
                if state['status'] != 'running':
                    raise WorldError('WORLD_PAUSED', '世界暂停，不调用裁定服务')
                history = self.history(person_id, intent_id)
                latest = history[-1] if history else None
                if latest and latest['decision']['base_revision'] == state['revision'] and latest['decision']['world_id'] == state['world_id']:
                    version = latest['version']
                else:
                    context = {'proposal':self._freeze(proposal), 'world':state}
                    if 'projects' in state:
                        context['opt_in_effect_protocol'] = dict(version=state['projects']['version'],
                            actions={'start_project':['project_id'], 'continue_project':['project_task_id'],
                                     'interrupt_project':['project_task_id'], 'share_project_knowledge':['to','knowledge_id']},
                            constraints='显式本局新增协议；仅原财政官现场分段申请；投入即消耗，工具占用，正时长到期才有成果；一般现代知识不受此限制，本地实验事实须有来源。')
                    protocol = self.world.lifecycle_effect_protocol(person_id, intent_id, execution=execution)
                    if protocol is not None:
                        if protocol['base_revision'] != state['revision']:
                            raise WorldError('STALE_PREVIEW', '可信生命周期投影与本次裁定版本不符')
                        context['opt_in_effect_protocol'] = protocol
                    decision = provider(context)
                    version = self.record(person_id, intent_id, decision, execution=execution)
                from .world import DomainActionError
                try:
                    self.preview(person_id, intent_id, version, execution=execution)
                except DomainActionError:
                    from .coordinator_scheduler import scoped
                    if scoped(self.world) is None:
                        raise
                    # Preview is read-only evidence, never durable no-effect.
                    # Re-evaluate the same decision under the actual write gate.
                return self.commit(person_id, intent_id, version, execution=execution)
            except Exception as error:
                if isinstance(error, WorldError) and error.code.startswith(('ACTOR_EXECUTION_', 'EXECUTION_', 'INVALID_EXECUTION', 'COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION', 'ACTIVATION_', 'INVALID_ACTIVATION', 'WINDOW_RECOVERY_UNPROVEN', 'WINDOW_VERSION_CONFLICT', 'ACTOR_RECOVERY_UNPROVEN', 'ACTOR_WORLD_', 'ACTOR_SESSION_')):
                    raise
                # A concurrent successful commit wins even if our preview failed.
                # Failure cleanup keeps the original provider exception when the
                # captured authority/deadline rejects a receipt check or pause.
                try:
                    receipt = self._execution_receipt(person_id, intent_id, execution)
                    if receipt is not None:
                        return receipt
                    if isinstance(error, WorldError) and error.code in retryable:
                        continue
                    if isinstance(error, WorldError) and error.code == 'WORLD_PAUSED':
                        raise error
                    if isinstance(error, WorldError) and error.code in {'UNSUPPORTED', 'ADJUDICATION_UNSUPPORTED'}:
                        self.world.technical_pause('ADJUDICATION_UNSUPPORTED', execution=execution)
                        raise WorldError('ADJUDICATION_UNSUPPORTED', '后果执行能力未实现，整局技术暂停') from None
                    self.world.technical_pause('ADJUDICATION_FAILED', execution=execution)
                except WorldError as cleanup_error:
                    if (not isinstance(error, WorldError) and cleanup_error.code.startswith(
                            ('COORDINATOR_EXECUTION_', 'INVALID_COORDINATOR_EXECUTION'))):
                        raise error
                    raise
                raise WorldError('ADJUDICATION_FAILED', '裁定流程技术暂停') from None
        self.world.technical_pause('ADJUDICATION_RETRY_EXHAUSTED', execution=execution)
        raise WorldError('ADJUDICATION_FAILED', '裁定流程技术暂停') from None

    def prepare_lifecycle(self, job_id, service_version):
        # World obtains the actual scheduler Work from the current scope itself.
        # No caller-owned dictionary is accepted as an authority carrier.
        prepared = self.world.prepare_lifecycle(job_id, service_version)
        from .world import _lc_hash
        if type(prepared) is not dict:
            raise WorldError('ACTOR_RECOVERY_UNPROVEN', '原继任准备载体缺失')
        if prepared.get('status') == 'completed':
            if set(prepared) != {'status','result'} or prepared['result']['job_id'] != job_id:
                raise WorldError('ACTOR_RECOVERY_UNPROVEN', '原完成job不符')
        elif (set(prepared) != {'status','request','request_sha256'} or prepared['status'] != 'pending'
                or prepared['request']['job_id'] != job_id
                or prepared['request']['service_version'] != service_version
                or _lc_hash(prepared['request']) != prepared['request_sha256']):
            raise WorldError('ACTOR_RECOVERY_UNPROVEN', '原继任准备摘要/业务绑定不符')
        return prepared

    @staticmethod
    def _lifecycle_response(response_raw, response):
        from .world import _lc_json, _lc_hash
        if (type(response_raw) is not str or type(response) is not dict
                or set(response) != {'status','reason','basis_refs','successor','unresolved'}
                or _lc_json(response_raw) != response
                or _lc_hash(_lc_json(response_raw)) != _lc_hash(response)):
            raise WorldError('INVALID_DECISION', '继任响应原文与严格解码不符')
        return _lc_json(response_raw), _lc_hash(response)

    def preview_lifecycle(self, job_id, request_sha256, response_raw, response):
        decoded, decision_sha256 = self._lifecycle_response(response_raw, response)
        preview = self.world.preview_lifecycle(job_id, request_sha256, response_raw, decoded)
        if preview['status'] == 'completed':
            result = preview['result']
            if (result['job_id'] != job_id or result['request_sha256'] != request_sha256
                    or result['decision_sha256'] != decision_sha256):
                raise WorldError('ACTOR_RECOVERY_UNPROVEN', '原完成继任绑定不符')
        elif (preview['status'] != 'preview' or preview['request_sha256'] != request_sha256
                or preview['decision_sha256'] != decision_sha256
                or preview['result']['job_id'] != job_id):
            raise WorldError('ACTOR_RECOVERY_UNPROVEN', '原继任预览绑定不符')
        return preview

    def commit_lifecycle(self, job_id, request_sha256, response_raw, response, *, decision_sha256):
        decoded, digest = self._lifecycle_response(response_raw, response)
        if digest != decision_sha256:
            raise WorldError('INVALID_DECISION', '继任响应裁定摘要不符')
        return self.world.commit_lifecycle(job_id, request_sha256, response_raw, decoded,
            decision_sha256=decision_sha256)

    def proposal(self, person_id, intent_id):
        self._identity(person_id, intent_id)
        with self._read() as db:
            db.execute('BEGIN')
            state = json.loads(db.execute('SELECT state FROM snapshots ORDER BY seq DESC LIMIT 1').fetchone()[0])
            return self._proposal(db, person_id, intent_id, state)


# Original constructor image for WorldStore's native DDL accounting gate.
_ADJ1187_INIT = (Adjudicator, Adjudicator.__init__, Adjudicator.__init__.__code__)
