"""Explicit authored scenario normalization; source records are never rewritten."""
import hashlib
import json
import re

from .disease import pathology_digest, contact_mechanism_digest


class ScenarioError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def _require(condition, message):
    if not condition:
        raise ScenarioError('INVALID_SCENARIO', message)


def _text(value):
    return type(value) is str and bool(value.strip())


def _integer(value, minimum=1):
    return type(value) is int and value >= minimum


def canonical(value):
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    try:
        encoded.encode('utf-8')  # JSON escapes may decode to unencodable lone surrogates.
    except UnicodeError:
        raise ScenarioError('INVALID_SCENARIO', '场景JSON必须可编码为UTF-8') from None
    return encoded


def load_scenario(path):
    """Reject invalid JSON, duplicate keys and unreadable files at the boundary."""
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate JSON key')
            result[key] = value
        return result
    def constant(value):
        raise ValueError('non-finite JSON number')
    try:
        config = json.loads(path.read_text(encoding='utf-8'), object_pairs_hook=pairs, parse_constant=constant)
        canonical(config)  # Overflow exponents also decode to non-finite floats, including nested values.
        return config
    except (OSError, UnicodeError, ValueError, RecursionError):
        raise ScenarioError('INVALID_SCENARIO', '场景文件不可读或不是严格JSON') from None


def scenario_digest(config):
    return hashlib.sha256(canonical(config).encode('utf-8')).hexdigest()


ECONOMY_VERSION = 'p08-production-economy-v1'
ECONOMY_MAX = 2**63 - 1


def economy_digest(economy):
    return hashlib.sha256(canonical(economy).encode('utf-8')).hexdigest()


def _validate_economy(economy, countries):
    _require(type(economy) is dict and set(economy) == {
        'version', 'assumptions', 'units', 'initial_materials', 'labor_hours_per_day', 'recipes'},
        '经济声明字段不完整或未知')
    _require(economy['version'] == ECONOMY_VERSION, '不支持的经济版本')
    _require(_text(economy['assumptions']) and len(economy['assumptions']) <= 2000, '经济假设必须明确且有界')
    def identifier(value):
        return type(value) is str and re.fullmatch(r'[a-z][a-z0-9_-]{0,63}', value) is not None
    def quantity(value, minimum=0, maximum=ECONOMY_MAX):
        return type(value) is int and minimum <= value <= maximum
    units = economy['units']
    _require(type(units) is dict and len(units) >= 2 and units.get('grain') == 'grain_unit'
             and all(identifier(k) and _text(v) and len(v) <= 64 for k, v in units.items()), '物资单位非法')
    materials = set(units) - {'grain'}
    places = {p['id']: p for c in countries for p in c['places']}
    initial, labor = economy['initial_materials'], economy['labor_hours_per_day']
    _require(type(initial) is dict and set(initial) == set(places), '初始物资地点不完整或未知')
    for values in initial.values():
        _require(type(values) is dict and set(values) == materials and all(quantity(v) for v in values.values()),
                 '初始非粮物资必须完整且为有界非负整数')
    _require(type(labor) is dict and set(labor) == set(places), '基础劳动地点不完整或未知')
    for pid, value in labor.items():
        _require(quantity(value, maximum=min(places[pid]['population'] * 24, ECONOMY_MAX)), '基础劳动工时越界')
    recipes = economy['recipes']
    _require(type(recipes) is dict and bool(recipes), '必须声明配方')
    for rid, recipe in recipes.items():
        _require(identifier(rid) and type(recipe) is dict and set(recipe) == {
            'inputs', 'outputs', 'labor_hours', 'duration_seconds'}, '配方标识或字段非法')
        for side in ('inputs', 'outputs'):
            values = recipe[side]
            _require(type(values) is dict and bool(values) and set(values) <= materials
                     and all(quantity(v, 1) for v in values.values()), '配方必须使用已声明非粮物资及正整数')
        _require(not (set(recipe['inputs']) & set(recipe['outputs'])), '配方两侧不能出现同一物资')
        _require(quantity(recipe['labor_hours'], 1) and quantity(recipe['duration_seconds'], 1), '配方工时及时间必须为有界正整数')


PROJECT_VERSION = 'p08-staged-projects-v1'


def _validate_projects(declaration, countries, economy):
    """Separate opt-in: never widen economy-v1's exact shape or recipes."""
    _require(type(declaration) is dict and set(declaration) == {
        'version','assumptions','knowledge','initial_knowledge','projects'}, '阶段项目声明字段非法')
    _require(declaration['version'] == PROJECT_VERSION, '未知阶段项目版本')
    _require(_text(declaration['assumptions']) and len(declaration['assumptions']) <= 2000,
             '必须显式声明虚构游戏假设，投入在每阶段开工即消耗，工具占用后归还，后段另行申请')
    def ident(v):
        return type(v) is str and re.fullmatch(r'[a-z][a-z0-9_-]{0,63}',v) is not None
    knowledge = declaration['knowledge']
    _require(type(knowledge) is dict and 1 <= len(knowledge) <= 64 and
             all(ident(k) and _text(v) and len(v) <= 2000 for k,v in knowledge.items()), '知识声明非法')
    people = {p['person_id'] for c in countries for p in c['positions']}
    initial = declaration['initial_knowledge']
    _require(type(initial) is list and len(initial) <= 128, '初始知识来源列表非法')
    pairs = set()
    for item in initial:
        _require(type(item) is dict and set(item) == {'person_id','knowledge_id','source'} and
                 item['person_id'] in people and item['knowledge_id'] in knowledge and
                 _text(item['source']) and len(item['source']) <= 2000, '初始知识须绑定实际人物及显式来源')
        pair = (item['person_id'],item['knowledge_id'])
        _require(pair not in pairs, '初始知识重复')
        pairs.add(pair)
    projects = declaration['projects']
    _require(type(projects) is dict and 1 <= len(projects) <= 32, '项目目录非法')
    materials = set(economy['units']) - {'grain'}
    for project_id, project in projects.items():
        _require(ident(project_id) and type(project) is dict and set(project) == {'name','stages'} and
                 _text(project['name']) and len(project['name']) <= 128, '项目名称或字段非法')
        stages = project['stages']
        _require(type(stages) is list and 2 <= len(stages) <= 8, '项目必须含2至8段')
        _require(all(type(s) is dict for s in stages), '阶段必须是对象')
        _require(stages[0].get('kind') == 'research' and stages[-1].get('kind') == 'manufacture', '首段研究末段制造')
        for stage in stages:
            _require(type(stage) is dict and set(stage) == {
                'kind','inputs','tools','labor_hours','duration_seconds','requires','knowledge','outputs'}, '阶段字段非法')
            _require(stage['kind'] in ('research','manufacture'), '阶段种类非法')
            for side in ('inputs','tools','outputs'):
                values = stage[side]
                _require(type(values) is dict and set(values) <= materials and
                         all(type(v) is int and 1 <= v <= ECONOMY_MAX for v in values.values()), '阶段资源非法')
            _require(bool(stage['inputs']) and bool(stage['tools']), '每段须有真实材料及占用工具')
            _require(not (set(stage['tools']) & (set(stage['inputs']) | set(stage['outputs']))), '工具不能同时消耗或产出')
            for key in ('requires','knowledge'):
                value = stage[key]
                _require(type(value) is list and len(value) == len(set(value)) and set(value) <= set(knowledge), '阶段知识引用非法')
            _require(type(stage['labor_hours']) is int and 1 <= stage['labor_hours'] <= ECONOMY_MAX and
                     type(stage['duration_seconds']) is int and 1 <= stage['duration_seconds'] <= ECONOMY_MAX,
                     '阶段劳动及模拟时长须为正整数')
            _require((stage['kind'] == 'research' and bool(stage['knowledge']) and not stage['outputs']) or
                     (stage['kind'] == 'manufacture' and bool(stage['outputs']) and bool(stage['requires'])),
                     '研究知识与制造成品必须分开，制造须有已获工艺来源')


MILITARY_VERSION = 'p08-standing-armies-v1'


def _validate_military(declaration, countries):
    """Optional versioned standing forces, explicitly separate from civilian cohorts."""
    _require(type(declaration) is dict and set(declaration) == {
        'version', 'assumptions', 'population_basis', 'grain_per_soldier_day', 'defense_multiplier', 'armies'},
        '军事声明字段必须精确完整')
    _require(declaration['version'] == MILITARY_VERSION, '未知军事规则版本')
    _require(_text(declaration['assumptions']) and len(declaration['assumptions']) <= 2000, '军事游戏假设必须显式有界')
    _require(declaration['population_basis'] == 'standing-forces-excluded-from-civilian-population-and-labor',
        '本版本仅支持开局独立常备军；不能重用民口或工班')
    _require(type(declaration['grain_per_soldier_day']) is int and 1 <= declaration['grain_per_soldier_day'] <= 100,
        '每兵日粮必须为1至100份的显式游戏参数')
    places = {p['id']: (c['id'], p) for c in countries for p in c['places']}
    country_ids = {c['id'] for c in countries}
    reserved_ids = country_ids | {x['id'] for c in countries for x in c['positions']} | {
        x['person_id'] for c in countries for x in c['positions']} | set(places)
    defense = declaration['defense_multiplier']
    _require(type(defense) is dict and set(defense) == set(places)
        and all(type(n) is int and 1 <= n <= 4 for n in defense.values()), '各地防御条件须显式为1至4')
    armies = declaration['armies']
    _require(type(armies) is list and 1 <= len(armies) <= 64, '开局军队必须为1至64支显式军队')
    ids, names, stores = set(), set(), {pid: 0 for pid in places}
    for army in armies:
        _require(type(army) is dict and set(army) == {'id', 'name', 'country_id', 'location', 'strength', 'grain'}, '军队字段非法')
        _require(_text(army['id']) and len(army['id']) <= 64 and army['id'] not in ids | reserved_ids, '军队ID须独立且唯一')
        _require(_text(army['name']) and len(army['name']) <= 128 and army['name'] not in names, '军队名须有界唯一')
        _require(army['country_id'] in country_ids and army['location'] in places
            and places[army['location']][0] == army['country_id'], '开局军队须在自己实际拥有的聚落')
        _require(type(army['strength']) is int and 1 <= army['strength'] <= 10**6
            and type(army['grain']) is int and 0 <= army['grain'] <= 2**63-1, '军队兵粮必须为有界整数')
        ids.add(army['id']); names.add(army['name']); stores[army['location']] += army['grain']
    _require(all(n <= places[pid][1]['grain'] for pid, n in stores.items()), '初始军粮必须全部来自对应现有仓储，不能重复支取')


LIFECYCLE_VERSION = 'p09-lifecycle-v1'
LIFECYCLE_WAKEUP_VERSION = 'p04-event-wakeup-v2-lifecycle'


def _validate_lifecycle(declaration, countries, event_wakeup):
    """Only authored genesis facts, never a future cast or mortality script."""
    _require(type(event_wakeup) is dict and set(event_wakeup) == {
        'version','government_interval_seconds','temporary_steps','temporary_persons'}
        and event_wakeup['version'] == LIFECYCLE_WAKEUP_VERSION,
        '生命周期必须与新局事件唤醒v2共同声明')
    _require(type(event_wakeup['government_interval_seconds']) is int
        and 1 <= event_wakeup['government_interval_seconds'] <= 30*86400
        and type(event_wakeup['temporary_steps']) is int
        and 1 <= event_wakeup['temporary_steps'] <= 8
        and type(event_wakeup['temporary_persons']) is list
        and len(event_wakeup['temporary_persons']) <= 16, '事件唤醒字段非法')
    _require(type(declaration) is dict and set(declaration) == {
        'version','assumptions','death_delay_seconds','personal_objects',
        'institutional_facts','relationship_facts'}, '生命周期声明字段必须精确完整')
    _require(declaration['version'] == LIFECYCLE_VERSION
        and type(declaration['death_delay_seconds']) is int
        and declaration['death_delay_seconds'] == 1, '未知生命周期版本或死亡正时长')
    def text(value):
        return _text(value) and len(value) <= 2000
    def identifier(value):
        return _text(value) and len(value) <= 128
    _require(text(declaration['assumptions']), '必须显式声明有界虚构游戏假设')
    people = {p['person_id']:dict(p,country_id=c['id']) for c in countries for p in c['positions']}
    places = {p['id']:c['id'] for c in countries for p in c['places']}
    offices = {p['id']:dict(p,country_id=c['id']) for c in countries for p in c['positions']}
    country_map = {c['id']:c for c in countries}
    reserved = set(people) | set(places) | set(offices) | set(country_map)
    _require(sum(map(len,(people,places,offices,country_map))) == len(reserved)
        and all(identifier(v) for v in reserved), '生命周期全域ID须独立有界')
    names = {p['person_name'] for p in people.values()}
    for person in event_wakeup['temporary_persons']:
        _require(type(person) is dict and set(person) == {
            'id','name','country_id','office','location','card'}, '临时人物字段非法')
        _require(all(identifier(person[k]) for k in ('id','name','country_id','office','location'))
            and person['id'] not in reserved and person['name'] not in names
            and person['office'] not in ('king','finance','marshal')
            and person['country_id'] in country_map
            and places.get(person['location']) == person['country_id'], '临时人物身份不合法')
        _require(type(person['card']) is dict and set(person['card']) == {'values','interests','risk','mission'}
            and all(text(v) for v in person['card'].values()), '临时人物卡非法')
        reserved.add(person['id']); names.add(person['name'])
        people[person['id']] = dict(person)
    specs = (
        ('personal_objects',16,{'id','kind','holder_person_id','source_text'}),
        ('institutional_facts',32,{'id','country_id','office_id','place_id','institution_text','rule','background_role','public_text'}),
        ('relationship_facts',32,{'id','subject_person_id','predicate','object_text','public_text'}))
    for key, limit, fields in specs:
        items = declaration[key]
        _require(type(items) is list and len(items) <= limit, '生命周期事实集合越界')
        for item in items:
            _require(type(item) is dict and set(item) == fields, '生命周期事实字段非法')
            _require(identifier(item['id']) and item['id'] not in reserved, '事实ID须全域独立唯一')
            reserved.add(item['id'])
            if key == 'personal_objects':
                _require(item['kind'] == 'held_iron_weapon' and item['holder_person_id'] in people
                    and text(item['source_text']), '实物须绑定原人物及显式独立计量来源')
            elif key == 'institutional_facts':
                office = offices.get(item['office_id'])
                country = country_map.get(item['country_id'])
                _require(office is not None and country is not None and office['office'] == 'finance'
                    and office['country_id'] == item['country_id']
                    and places.get(item['place_id']) == item['country_id']
                    and item['institution_text'] == country['institution']
                    and item['rule'] == 'automatic_acting_officer'
                    and all(text(item[k]) for k in ('institution_text','background_role','public_text')),
                    '自动代理习惯须绑定原财政职位、国家制度及明确机构现场')
            else:
                _require(item['subject_person_id'] in people and item['predicate'] == 'institutional_role'
                    and text(item['object_text']) and text(item['public_text']), '关系须为原人物既成制度角色')


def _source_graph(geography):
    _require(type(geography) is dict and type(geography.get('regions')) is list, '缺源地域')
    regions, source_ids = {}, set()
    for region in geography['regions']:
        _require(type(region) is dict and _text(region.get('id')) and _integer(region.get('source_region_id'), 0), '源地域标识非法')
        _require(region['id'] not in regions and region['source_region_id'] not in source_ids, '源地域重复')
        regions[region['id']] = region
        source_ids.add(region['source_region_id'])
    _require(bool(regions), '源地图为空')
    edges = {}
    for region in regions.values():
        _require(type(region.get('neighbors')) is list, '缺源邻接')
        for edge in region['neighbors']:
            _require(type(edge) is dict and _text(edge.get('to')) and edge['to'] in regions and edge['to'] != region['id'], '邻接目标非法')
            _require(_integer(edge.get('seconds')) and _integer(edge.get('source_kind'), 0), '边时间或source_kind非法')
            length = edge.get('source_length')
            _require(type(length) in (int, float) and length > 0, '源边长非法')
            key = (region['id'], edge['to'])
            _require(key not in edges, '重复源边')
            edges[key] = edge
    for (source, target), edge in edges.items():
        reverse = edges.get((target, source))
        _require(reverse is not None and all(reverse[k] == edge[k] for k in ('seconds','source_kind','source_length')), '邻接缺反向或属性不符')
    unseen, components = set(regions), []
    while unseen:
        start = min(unseen)
        reached, todo = {start}, [start]
        while todo:
            for edge in regions[todo.pop()]['neighbors']:
                if edge['to'] not in reached:
                    reached.add(edge['to'])
                    todo.append(edge['to'])
        components.append(reached)
        unseen -= reached
    main = max(components, key=len)
    _require(type(geography.get('settlements')) is list and type(geography.get('countries')) is list, '缺源聚落或国家')
    settlements = {}
    for settlement in geography['settlements']:
        _require(type(settlement) is dict and _integer(settlement.get('source_settlement_id'), 0), '源聚落标识非法')
        sid = settlement['source_settlement_id']
        _require(sid not in settlements and settlement.get('place_id') in regions and _integer(settlement.get('source_cell'), 0)
                 and _text(settlement.get('source_name')), '源聚落缺失或重复')
        settlements[sid] = settlement
    countries = {}
    for country in geography['countries']:
        _require(type(country) is dict and _text(country.get('id')) and country['id'] not in countries, '源国家标识非法')
        countries[country['id']] = country
    return regions, settlements, countries, main


def _validate_sources(config, geography):
    regions, settlements, countries, main = _source_graph(geography)
    active = set()
    for country in config['countries']:
        source = country.get('source_country_id')
        authored = country.get('authored_political_entity')
        _require('source_country_id' in country and type(authored) is bool, '缺政治实体来源声明')
        _require((source is None and authored is True) or (type(source) is str and source in countries and authored is False), '国家来源无效')
        for place in country['places']:
            pid, sid = place.get('id'), place.get('source_settlement_id')
            _require(_text(pid) and pid in regions and pid not in active, '地点缺源或重复占有')
            active.add(pid)
            _require(pid in main, '核心地点不在可达主分量')
            _require(_integer(sid, 0) and sid in settlements and settlements[sid]['place_id'] == pid, '聚落不属于地点')
            _require(_integer(place.get('source_region_id'), 0) and place['source_region_id'] == regions[pid]['source_region_id'], '地域引用不符')
            _require(_integer(place.get('source_cell'), 0) and place['source_cell'] == settlements[sid]['source_cell'], '聚落cell不符')
            _require(_text(place.get('name')) and place['name'] == settlements[sid]['source_name'], '源聚落名称不符')
            if source is not None:
                _require(pid in countries[source]['regions'], '地点不属于所引源国')
    _require('place-1' in active, '缺原首例活跃地点place-1')


def _validate_identities(countries):
    country_ids, office_ids, person_ids, person_names = set(), set(), set(), set()
    for country in countries:
        for key in ('id','name','institution'):
            _require(_text(country.get(key)), f'国家缺{key}')
        _require(country['id'] not in country_ids, '国家ID重复')
        country_ids.add(country['id'])
        positions = country.get('positions')
        _require(type(positions) is list and len(positions) == 3, '每国必须三个职位')
        offices = set()
        local_places = {place['id'] for place in country['places']}
        for position in positions:
            _require(type(position) is dict, '职位必须为对象')
            for key in ('id','office','person_id','person_name','location'):
                _require(_text(position.get(key)), f'职位缺{key}')
            _require(position['office'] in ('king','finance','marshal') and position['office'] not in offices, '职位缺失或重复')
            offices.add(position['office'])
            _require(position['id'] not in office_ids and position['person_id'] not in person_ids
                     and position['person_name'] not in person_names, '职位或人物身份重复')
            office_ids.add(position['id'])
            person_ids.add(position['person_id'])
            person_names.add(position['person_name'])
            _require(position['location'] in local_places, '人物不在本国地点')
            card = position.get('card')
            _require(type(card) is dict and set(card) == {'values','interests','risk','mission'}
                     and all(_text(value) and len(value) <= 2000 for value in card.values()), '角色卡不合法')
    _require(not (country_ids & office_ids or country_ids & person_ids or office_ids & person_ids), '国家职位人物ID必须分开')


def validate_scenario(config, geography):
    """Validate untrusted input without leaking Python shape/type exceptions."""
    try:
        canonical(geography)  # Reject non-finite values even in unused source records.
        return _validate_scenario(config, geography)
    except ScenarioError:
        raise
    except (KeyError, TypeError, ValueError, OverflowError, RecursionError):
        raise ScenarioError('INVALID_SCENARIO', '场景或地理字段格式非法') from None


def _validate_scenario(config, geography):
    """Return a private canonical copy; invalid pathology retains its own code."""
    _require(type(config) is dict, '场景必须为对象')
    try:
        pathology_digest(config.get('pathology'))
        if 'pathology_runtime' in config:
            contact_mechanism_digest(config['pathology_runtime'], config['pathology'])
    except ValueError as error:
        raise ScenarioError('INVALID_PATHOLOGY', str(error)) from None
    try:
        config = json.loads(canonical(config))
    except (TypeError, ValueError, RecursionError):
        raise ScenarioError('INVALID_SCENARIO', '场景必须为有限合法JSON') from None
    for key in ('version', 'assumptions'):
        _require(_text(config.get(key)), f'{key}必须为非空字符串')
    digest = config.get('geography_sha256')
    _require(type(digest) is str and re.fullmatch(r'[0-9a-f]{64}', digest) is not None, '缺合法地理摘要')
    economic = type(config.get('schema_version')) is int and config['schema_version'] == 3
    if economic:
        _require('harvest_day' not in config and 'harvest_quantity' not in config, '经济新局不能声明旧固定收获')
    else:
        _require('economy' not in config, '旧格式不能声明新经济')
        for key in ('harvest_day', 'harvest_quantity'):
            _require(_integer(config.get(key)), f'{key}必须为正整数')
    for key in ('horizon_days','daily_consumption',
                'investigation_seconds','local_message_seconds','country_count'):
        _require(_integer(config.get(key)), f'{key}必须为正整数')
    if 'schema_version' in config:
        _require(type(config['schema_version']) is int and config['schema_version'] in (2, 3), '不支持的场景版本')
        _require(_text(config.get('scenario_id')), '缺场景ID')
        _require(config['horizon_days'] == 90, '新场景观察期必须为90日')
        countries = config.get('countries')
        _require(type(countries) is list and len(countries) == config['country_count'] and 4 <= len(countries) <= 6,
                 '必须显式定义4至6国且数量一致')
        for country in countries:
            _require(type(country) is dict, '国家必须为对象')
            color = country.get('color')
            _require(type(color) is str and re.fullmatch(r'#[0-9a-fA-F]{6}', color) is not None, '颜色必须为hex')
            _require(type(country.get('places')) is list and len(country['places']) >= 2, '每国至少两个核心地点')
            for place in country['places']:
                _require(type(place) is dict and _integer(place.get('population')) and _integer(place.get('grain'), 0),
                         '人口必须为正整数且粮储为非负整数')
        _validate_sources(config, geography)
        _validate_identities(countries)
        if economic:
            _validate_economy(config.get('economy'), countries)
    else:
        _require('countries' not in config, '显式国家缺schema_version')
        original_pairs = [['place-0','place-1'], ['place-10','place-78'], ['place-3','place-65'], ['place-37','place-80']]
        _require(config['country_count'] == 4 and config.get('pairs') == original_pairs, '旧格式仅支持原四国位置配置')
        _require(_integer(config.get('population')) and _integer(config.get('grain'), 0), '旧人口或粮储非法')
        regions, settlements, countries, main = _source_graph(geography)
        _require(list(countries) == ['country-1','country-2','country-3','country-4'], '旧源四国不符')
        for country, pair in zip(countries.values(), original_pairs):
            for pid in pair:
                records = [s for s in settlements.values() if s['place_id'] == pid]
                _require(pid in main and pid in country['regions'] and len(records) == 1, '旧地点缺源或聚落不唯一')
    if 'staged_projects' in config:
        _require(economic, '阶段项目仅允许显式schema3新经济场景')
        _validate_projects(config['staged_projects'], config['countries'], config['economy'])
    if 'military' in config:
        _require(config.get('schema_version') in (2, 3), '军事扩展须显式国家场景')
        _validate_military(config['military'], config['countries'])
    wakeup = config.get('event_wakeup')
    if 'lifecycle' in config or (type(wakeup) is dict and wakeup.get('version') == LIFECYCLE_WAKEUP_VERSION):
        _require(config.get('schema_version') in (2, 3) and 'countries' in config, '生命周期仅允许显式国家新局')
        _validate_lifecycle(config.get('lifecycle'), config['countries'], wakeup)
    return config
