// No external dependencies. Importable under Node for pure behavior tests.
export function writePolicy(snapshot, replay, busy, meta) {
  const allowed = Boolean(snapshot && meta?.read_only !== true && meta?.csrf && !replay && !busy);
  const paused = snapshot?.status === 'user_paused' || snapshot?.status === 'ready';
  return {step:allowed && paused,
    run:allowed && (paused || snapshot?.status === 'horizon_paused'),
    pause:allowed && snapshot?.status === 'running', card:allowed && paused};
}
const CARD_KEYS = ['values', 'interests', 'risk', 'mission'];
export function cardChanges(input) {
  const changes = {};
  for (const key of CARD_KEYS) {
    if (typeof input[key] !== 'string') throw new Error('人格卡四个字段都必须是字符串。');
    changes[key] = input[key];
  }
  return changes;
}

export function createRequestGate() {
  let generation = 0;
  return {next:() => ++generation, isCurrent:id => id === generation, invalidate:() => ++generation};
}

export function createAPI(fetcher, getToken) {
  return async (path, body) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 20000);
    try {
      const headers = {Accept:'application/json'};
      if (body !== undefined) {
        const token = getToken();
        if (!token) throw new Error('缺少本机安全令牌，请重新读取。');
        headers['Content-Type'] = 'application/json';
        headers['X-Theatre-CSRF'] = token;
      }
      const response = await fetcher(path, {method:body === undefined ? 'GET' : 'POST',
        credentials:'same-origin', cache:'no-store', headers, signal:controller.signal,
        ...(body === undefined ? {} : {body:JSON.stringify(body)})});
      let data;
      try { data = await response.json(); }
      catch { throw new Error(`本机服务返回了非 JSON 响应（${response.status || '未知状态'}）。`); }
      if (!response.ok || data?.error) throw new Error(data?.error?.message || `请求失败（${response.status}）。`);
      return data;
    } catch (error) {
      if (error.name === 'AbortError') throw new Error('本机请求超时；写入结果未知，请重新读取核对，勿直接重复提交。');
      throw error;
    } finally { clearTimeout(timer); }
  };
}

export function topology(snapshot) {
  const countries = new Map((snapshot.countries || []).map(c => [c.id, c]));
  const places = snapshot.places || [];
  const nodes = places.filter(p => Number.isFinite(p.x) && Number.isFinite(p.y)).map(p => ({
    ...p, color: /^#[0-9a-f]{3,8}$/i.test(countries.get(p.country_id)?.color || '') ? countries.get(p.country_id).color : '#6b7770'
  }));
  const byId = new Map(nodes.map(n => [n.id, n]));
  const regions = new Map(nodes.filter(n => n.region_id !== undefined).map(n => [n.region_id, n.id]));
  const edges = [];
  const seen = new Set();
  const connect = (a, b) => {
    b = byId.has(b) ? b : regions.get(b);
    if (!byId.has(a) || !byId.has(b) || a === b) return;
    const pair = [a, b].sort();
    const key = JSON.stringify(pair);
    if (!seen.has(key)) { seen.add(key); edges.push(pair); }
  };
  for (const place of nodes) {
    for (const other of (place.neighbors || place.adjacent || [])) connect(place.id, typeof other === 'object' && other !== null ? other.to : other);
  }
  for (const edge of snapshot.edges || []) {
    if (Array.isArray(edge)) connect(edge[0], edge[1]);
    else connect(edge.from, edge.to);
  }
  const xs = nodes.map(n => n.x), ys = nodes.map(n => n.y);
  const minX = nodes.length ? Math.min(...xs) : 0, minY = nodes.length ? Math.min(...ys) : 0;
  const width = nodes.length ? Math.max(...xs) - minX : 1;
  const height = nodes.length ? Math.max(...ys) - minY : 1;
  const pad = Math.max(width, height, 1) * 0.19;
  return {nodes, edges, omitted: places.length - nodes.length,
    viewBox: `${minX-pad} ${minY-pad} ${Math.max(width, pad)+2*pad} ${Math.max(height, pad)+2*pad}`,
    unit: Math.max(width, height, 1) / 30};
}

// Browser adapter: all narrative/API content enters the DOM as text nodes.
export function boot(doc = document, fetcher = window.fetch.bind(window)) {
  const $ = id => doc.getElementById(id);
  const element = (tag, text, className) => {
    const node = doc.createElement(tag);
    if (text !== undefined) node.textContent = String(text);
    if (className) node.className = className;
    return node;
  };
  const text = (id, value) => { $(id).textContent = value; };
  const show = (id, message, error = false) => {
    text(id, message);
    $(id).classList.toggle('error', error);
  };
  const list = value => Array.isArray(value) ? value : [];
  const format = value => Number.isFinite(value) ? value.toLocaleString('zh-CN', {maximumFractionDigits:1}) : '未提供';
  const day = seconds => Number.isFinite(seconds) ? `第 ${Math.floor(seconds / 86400)} 日` : '时刻未知';
  const statusLabels = {ready:'就绪',running:'运行中',user_paused:'已暂停',technical_paused:'技术暂停',horizon_paused:'观察期到期暂停'};
  const stateGate = createRequestGate(), personGate = createRequestGate();
  const drafts = new Map();
  let snapshot = null, meta = null, latestRevision = 0, replaySeq = null;
  let busy = false, loading = false, healthy = false, personId = '', countryId = '', placeId = '';
  let quietDraft = false;
  let personLoadedAt = '', personStateReady = false;
  const api = createAPI(fetcher, () => meta?.csrf);
  const isReplay = () => replaySeq !== null;
  const person = () => list(snapshot?.persons).find(p => p.id === personId);
  const policy = () => writePolicy(snapshot, isReplay(), busy || loading || !healthy, meta);

  function updateControls() {
    const allow = policy();
    for (const action of ['step','run','pause']) $(action).disabled = !allow[action];
    // A quiet read may preserve editing, never permission to submit.
    const draftAllow = writePolicy(snapshot, isReplay(), busy || (loading && !quietDraft) || !healthy, meta);
    $('card-fields').disabled = !draftAllow.card || !person();
    $('save-card').disabled = !allow.card || !person();
    $('run').textContent = snapshot?.status === 'horizon_paused' ? '续跑 90 日' : '运行';
    $('latest').disabled = !isReplay() || busy;
    $('replay').disabled = !snapshot || busy || (loading && !isReplay());
    $('country-select').disabled = !snapshot || busy;
    $('person-select').disabled = !snapshot || busy || !list(snapshot.persons).length;
    $('retry').disabled = busy || loading;
    $('workspace').setAttribute('aria-busy', String(busy || loading));
    text('replay-status', isReplay() ? `只读回放 · 版本 ${replaySeq} · 所有写操作禁用` : meta?.read_only === true ? '只读归档 · 最新版本 · 所有写操作禁用' : '最新世界 · 回放不创建分支');
  }

  function validateState(data) {
    if (!data || !Number.isInteger(data.revision) || data.revision < 0 ||
        !['countries','places','persons','events'].every(key => Array.isArray(data[key]))) {
      throw new Error('世界响应格式不符合合同；未用演示数据替代。');
    }
    return data;
  }

  function keepDraft() {
    if (!personId || isReplay()) return;
    const current = person();
    if (!current) return;
    const values = Object.fromEntries(CARD_KEYS.map(key => [key, $('card-form').elements.namedItem(key).value]));
    if (CARD_KEYS.some(key => values[key] !== (current.card?.[key] || ''))) drafts.set(personId, values);
    else drafts.delete(personId);
  }

  function renderSelectors() {
    const countries = list(snapshot.countries);
    if (!countries.some(c => c.id === countryId)) countryId = '';
    const options = [element('option', '全部国家')]; options[0].value = '';
    for (const country of countries) { const option = element('option', country.name); option.value = country.id; options.push(option); }
    $('country-select').replaceChildren(...options); $('country-select').value = countryId;
    const people = list(snapshot.persons).filter(p => !countryId || p.country_id === countryId);
    if (!people.some(p => p.id === personId)) personId = people[0]?.id || '';
    const personOptions = people.map(p => { const option = element('option', `${p.name} · ${p.office}`); option.value = p.id; return option; });
    if (!people.length) { const option = element('option', '没有可选人物'); option.value = ''; personOptions.push(option); }
    $('person-select').replaceChildren(...personOptions); $('person-select').value = personId;
  }

  function renderCard() {
    const selected = person();
    const location = list(snapshot.places).find(p => p.id === selected?.location);
    const country = list(snapshot.countries).find(c => c.id === selected?.country_id);
    text('person-identity', selected ? `${selected.name} / ${selected.office} · ${country?.name || selected.country_id} · ${location?.name || selected.location || '位置未提供'}（观众身份资料）` : '暂无人物。');
    const card = (!isReplay() && drafts.get(personId)) || selected?.card || {};
    for (const key of CARD_KEYS) $('card-form').elements.namedItem(key).value = card[key] || '';
    show('card-feedback', isReplay() ? '历史人物卡 · 只读；未提交草稿留在最新视图。' : drafts.has(personId) ? '此人物有未提交草稿，仅保存在当前页面。' : '人格修改仅向前生效。');
  }

  function renderMetrics() {
    const places = list(snapshot.places);
    const sum = key => places.length && places.every(p => Number.isFinite(p[key])) ? places.reduce((total,p) => total+p[key],0) : null;
    const metrics = [
      ['国家 / 聚落', `${snapshot.countries.length} / ${places.length}`, '实际场景规模'],
      ['人口', format(sum('population')), '后台人口真值'],
      ['在仓粮食', format(sum('grain')), '不含在途库存'],
      ['在途任务', Array.isArray(snapshot.in_transit) ? format(snapshot.in_transit.length) : '未提供', '全知任务记录']
    ];
    $('world-metrics').replaceChildren(...metrics.map(([label,value,note]) => {
      const item = element('div', undefined, 'metric');
      item.append(element('span',label),element('strong',value),element('small',note)); return item;
    }));
  }

  function renderPlace() {
    const place = list(snapshot.places).find(p => p.id === placeId);
    const target = $('place-detail');
    target.replaceChildren();
    if (!place) { target.append(element('p','选择聚落查看全知库存与病理状态。')); return; }
    target.append(element('strong', `${place.name} · 全知事实`));
    const stats = element('div', undefined, 'detail-grid');
    const disease = typeof place.disease === 'object' ? JSON.stringify(place.disease) : String(place.disease ?? '未提供');
    for (const detail of [`人口 ${format(place.population)}`, `在仓粮食 ${format(place.grain)}`, `病理真值 ${disease}`]) stats.append(element('span', detail));
    target.append(stats, element('p','这里的病理与库存不是当前人物见闻。', 'muted'));
    const local = list(snapshot.persons).filter(p => p.location === place.id);
    if (local.length) {
      const actions = element('div', undefined, 'form-actions');
      for (const p of local) {
        const button = element('button', `${p.name} · ${p.office}`); button.type = 'button';
        button.addEventListener('click', () => selectPerson(p.id)); actions.append(button);
      }
      target.append(actions);
    }
  }

  function renderMap() {
    const model = topology(snapshot);
    const svg = $('map'); svg.replaceChildren(); svg.setAttribute('viewBox', model.viewBox);
    const svgNode = (tag, attrs, content) => {
      const node = doc.createElementNS('http://www.w3.org/2000/svg',tag);
      for (const [key,value] of Object.entries(attrs)) node.setAttribute(key, String(value));
      if (content !== undefined) node.textContent = content;
      return node;
    };
    svg.append(svgNode('title', {}, '真实聚落拓扑示意；不是测绘地图'));
    const byId = new Map(model.nodes.map(n => [n.id,n]));
    for (const [a,b] of model.edges) {
      const start = byId.get(a), end = byId.get(b);
      svg.append(svgNode('line',{x1:start.x,y1:start.y,x2:end.x,y2:end.y,class:'route'}));
    }
    for (const place of model.nodes) {
      const group = svgNode('g',{class:`settlement${countryId && countryId !== place.country_id ? ' dim' : ''}`,
        tabindex:'0',role:'button','aria-label':`${place.name}，查看聚落全知资料`,'aria-pressed':String(place.id === placeId), 'data-place-id':place.id});
      group.append(svgNode('circle',{cx:place.x,cy:place.y,r:model.unit * .6,fill:place.color}),
        svgNode('text',{x:place.x,y:place.y+model.unit*1.55,'text-anchor':'middle','font-size':model.unit*.95},place.name));
      const select = () => {
        placeId = place.id;
        for (const node of svg.querySelectorAll('[data-place-id]')) node.setAttribute('aria-pressed', String(node.getAttribute('data-place-id') === placeId));
        renderPlace();
      };
      group.addEventListener('click', select);
      group.addEventListener('keydown', event => { if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); select(); } });
      svg.append(group);
    }
    text('map-status', model.nodes.length ? `${model.nodes.length} 聚落 · ${model.edges.length} 条真实邻接${model.omitted ? ` · ${model.omitted} 处缺少坐标未绘制` : ''}${model.edges.length ? '' : ' · 后端未提供可绘制邻接，不推测连线'}` : '没有可绘制的聚落数据。');
    const legends = snapshot.countries.map(country => {
      const item = element('span',undefined,'legend-item'), swatch = element('span',undefined,'swatch');
      const color = model.nodes.find(n => n.country_id === country.id)?.color;
      if (color) swatch.style.backgroundColor = color;
      item.append(swatch,element('span',country.name)); return item;
    });
    $('map-legend').replaceChildren(...legends); renderPlace();
  }

  function renderMechanism() {
    const target = $('pathology-identity'); target.replaceChildren();
    const absent = '未记录（不补造）';
    const runtime = typeof snapshot.pathology_runtime === 'string' ? snapshot.pathology_runtime : '原局未声明运行机制；保持原规则';
    target.append(auditValue({
      '运行机制': runtime,
      '规则摘要': typeof snapshot.pathology_sha256 === 'string' ? snapshot.pathology_sha256 : absent,
      '联合机制摘要': typeof snapshot.pathology_mechanism_sha256 === 'string' ? snapshot.pathology_mechanism_sha256 : absent,
      '冻结时刻': Number.isSafeInteger(snapshot.pathology_frozen_at) && snapshot.pathology_frozen_at >= 0 ? `${snapshot.pathology_frozen_at} 秒` : absent,
      '该版本规则（标签不自动启用新机制）': snapshot.pathology ?? absent
    }));
  }

  function renderEvents() {
    // A historical snapshot already defines its own events. Event IDs are NOT revisions.
    const events = list(snapshot.events).slice().reverse();
    $('events').replaceChildren(...events.map(event => {
      const item = element('li',undefined,'event');
      item.append(element('time',day(event.time)),element('span',event.kind || '世界事件','kind'),element('p',event.text ?? '（无文本）'));
      const metadata = Object.fromEntries(Object.entries(event).filter(([key]) => !['time','kind','text'].includes(key)));
      if (Object.keys(metadata).length) {
        const detail = element('details');
        detail.append(element('summary','记录身份与因果数据（全知）'),auditValue(metadata));
        item.append(detail);
      }
      return item;
    }));
    if (!events.length) $('events').append(element('li','此版本尚无事件。','empty'));
  }

  // Structured text nodes, not a full-width JSON dump or an HTML template.
  function auditValue(value) {
    if (value === null || value === undefined) return element('span','未提供');
    if (typeof value !== 'object') return element('span',String(value));
    const entries = Array.isArray(value) ? value.map((v,i) => [String(i+1),v]) : Object.entries(value);
    if (!entries.length) return element('span','无');
    const rows = element('ul',undefined,'audit-values');
    for (const [key,item] of entries) {
      const row = element('li'); row.append(element('span',`${key}：`),auditValue(item)); rows.append(row);
    }
    return rows;
  }

  function renderCausality() {
    const root = $('causality'); root.replaceChildren(); root.setAttribute('aria-busy','false');
    const trace = snapshot.causality;
    if (!trace) { show('causality-status',`版本 ${snapshot.revision} · 未提供因果记录；旧存档无时间锚的记录不推测、不补齐。`); return; }
    if (trace.revision !== snapshot.revision || trace.world_id !== snapshot.world_id || !Array.isArray(trace.proposals)) {
      show('causality-status','因果记录版本或格式不匹配；未显示其他版本的记录。',true); return;
    }
    show('causality-status',`版本 ${snapshot.revision} / ${isReplay() ? '历史' : '最新'} · ${trace.proposals.length} 条原文 · 旧无时间锚记录不推测、不补齐。`);
    if (!trace.proposals.length) { root.append(element('p','此版本尚无可追溯提案。','empty')); return; }
    const decisions = {committed:'已提交（事实见下）',superseded:'已替代 · 未兑现',stale:'过期 · 未兑现',draft:'草案 · 未兑现'};
    const tasks = {pending:'待处理',completed:'已完成',interrupted:'已中断',undelivered:'未送达',untracked:'无可追溯证据'};
    for (const proposal of trace.proposals) {
      const card = element('article',undefined,'causal-item');
      card.append(element('h3',`${proposal.person_name || proposal.person_id} · ${proposal.intent_id}`),
        element('p',`原文记录版本 ${proposal.recorded_revision} · ${proposal.committed ? `已提交第 ${proposal.committed.decision_version} 版` : '未兑现'}`,'muted'));
      const raw = element('details');
      raw.append(element('summary',`原话：${String(proposal.text ?? '（无文本）').slice(0,100)}`),element('p',proposal.text ?? '（无文本）','raw-text')); card.append(raw);
      for (const decision of list(proposal.decisions)) {
        const detail = element('details',undefined,'decision-record');
        detail.append(element('summary',`第 ${decision.version} 版 · ${decisions[decision.status] || '状态未知 · 不认定兑现'} · 基准版本 ${decision.base_revision}`),
          element('p',`记录版本 ${decision.recorded_revision} · 裁定：${decision.ruling} · 目标：${decision.goal ?? '未提供'}`),
          element('p',`理由：${decision.reason ?? '未提供'}`));
        for (const [label,value] of [['草案代价',decision.costs],['草案预期收益',decision.benefits],['草案效果（不等于已发生事实）',decision.effects]]) {
          detail.append(element('h4',label),auditValue(value));
        }
        card.append(detail);
      }
      if (!list(proposal.decisions).length) card.append(element('p','尚无裁定 · 未兑现','muted'));
      const committed = proposal.committed;
      if (committed) {
        const facts = element('details',undefined,'committed-facts');
        facts.append(element('summary',`已兑现事实 · 第 ${committed.decision_version} 版 · 提交版本 ${committed.revision}`),
          element('p',`提交时刻：${day(committed.time)} / ${committed.time} 秒`),
          element('h4','执行回执（任务入队不等于完成）'),auditValue(committed.effects),element('h4','提交时资源净差（不是当前累计库存）'));
        for (const resource of list(committed.resource_changes)) {
          facts.append(element('p',`${resource.name || resource.place_id} · ${resource.resource} · ${format(resource.before)} → ${format(resource.after)} · 净变化 ${resource.delta > 0 ? '+' : ''}${format(resource.delta)}`));
        }
        if (!list(committed.resource_changes).length) facts.append(element('p','无已记录资源净差。','muted'));
        facts.append(element('h4','任务真实状态（到期不推定成功）'));
        for (const task of list(committed.tasks)) {
          facts.append(element('p',`${task.kind} · ${tasks[task.status] || '状态未知'} [${task.status}] · 到期 ${day(task.due)} / ${task.due ?? '未知'} 秒 · 因果 ${task.cause_id ?? '未追踪'}${task.event_id === undefined ? '' : ` · 事件 ${task.event_id}`}`));
        }
        if (!list(committed.tasks).length) facts.append(element('p','无已记录任务。','muted'));
        card.append(facts);
      } else card.append(element('p','未兑现 · 无提交事实、资源净差或实际任务可展示。','uncommitted'));
      root.append(card);
    }
  }

  function render() {
    text('day-status', `${day(snapshot.time)} · ${statusLabels[snapshot.status] || snapshot.status}`);
    text('world-version', `版本 ${snapshot.revision}${isReplay() ? ' / 历史' : ' / 最新'}`);
    $('replay').max = String(latestRevision);
    $('replay').value = String(snapshot.revision);
    text('replay-value', String(snapshot.revision));
    renderSelectors(); renderCard(); renderMetrics(); renderMap(); renderMechanism(); renderEvents(); renderCausality(); updateControls();
  }

  async function loadPerson(force = false) {
    const request = personGate.next();
    if (!personStateReady) return;
    if (!personId) {
      personLoadedAt = '';
      $('observations').replaceChildren(element('p','没有可选人物。','empty'));
      show('person-status','暂无人物见闻。'); return;
    }
    const id = personId, version = snapshot?.revision, mode = replaySeq;
    const key = `${id}:${version}:${mode}`;
    if (!force && personLoadedAt === key) return;
    $('observations').replaceChildren(element('p','加载人物合法见闻…','empty'));
    show('person-status','正在读取人物接口；不会用全知数字补齐。');
    try {
      const data = await api(`/api/person?id=${encodeURIComponent(id)}&seq=${encodeURIComponent(version)}`);
      if (!personGate.isCurrent(request) || id !== personId || version !== snapshot?.revision || mode !== replaySeq) return;
      if (data.revision !== version) throw new Error('人物见闻版本不匹配；未回退到最新，也不使用全知数据补齐。');
      if (data.person?.id !== id || !Array.isArray(data.observations) || !Array.isArray(data.messages)) throw new Error('人物见闻格式不符合合同。');
      const target = $('observations'); target.replaceChildren();
      for (const observation of data.observations) {
        const item = element('article',undefined,'knowledge-item');
        item.append(element('p',observation.text),element('small',`${day(observation.time)} · 渠道 / 来源：${typeof observation.source === 'string' ? observation.source : JSON.stringify(observation.source ?? '未标明')}`)); target.append(item);
      }
      if (data.messages.length) target.append(element('h4','已实际送达的来信'));
      for (const message of data.messages) {
        const item = element('article',undefined,'knowledge-item');
        item.append(element('p',message.text),element('small',`${day(message.time)} · 已送达来信 · 发信人：${message.from}`)); target.append(item);
      }
      if (!data.observations.length && !data.messages.length) target.append(element('p','此人物尚无已获得的见闻或已送达来信。','empty'));
      const contacts = list(data.contacts);
      if (contacts.length) {
        target.append(element('h4','公开同国名册（不代表知晓其私事）'));
        for (const contact of contacts) target.append(element('p',`${contact.name} · ${contact.office}`,'muted'));
      }
      personLoadedAt = key;
      show('person-status', `${data.person.name || id} · 版本 ${version} / ${isReplay() ? '历史' : '最新'} · ${day(data.time)} · 仅展示本人合法获得记录`);
    } catch (error) {
      if (!personGate.isCurrent(request)) return;
      personLoadedAt = '';
      $('observations').replaceChildren(element('p','见闻读取失败；未显示其他人物或全知数据。','empty'));
      show('person-status',error.message,true);
    }
  }

  async function selectPerson(id) {
    if (busy) return;
    keepDraft(); personId = id;
    const selected = person();
    if (selected && countryId && selected.country_id !== countryId) countryId = selected.country_id;
    renderSelectors(); renderCard(); updateControls(); await loadPerson(true);
  }

  async function loadState(seq = replaySeq, quiet = false) {
    const request = stateGate.next();
    quietDraft = quiet && seq === null && !isReplay() && policy().card && Boolean(person());
    loading = true;
    // Read-only lock is applied BEFORE the historical fetch completes (including failures).
    replaySeq = seq;
    personGate.invalidate();
    personStateReady = false;
    personLoadedAt = '';
    $('observations').replaceChildren(element('p','等待目标版本；不会显示其他版本的角色见闻。','empty'));
    show('person-status','正在切换版本，角色见闻暂不展示。');
    if (!quiet) {
      $('causality').replaceChildren(); $('causality').setAttribute('aria-busy','true');
      show('causality-status',seq === null ? '正在读取最新因果记录…' : `正在读取历史版本 ${seq} 的因果记录…`);
    }
    updateControls();
    if (!quiet) show('app-status', isReplay() ? `读取历史版本 ${seq}…` : '正在读取最新世界…');
    try {
      const data = validateState(await api(seq === null ? '/api/state' : `/api/state?seq=${encodeURIComponent(seq)}`));
      if (!stateGate.isCurrent(request)) return false;
      if (seq !== null && data.revision !== seq) throw new Error('历史版本不匹配；保持只读，未回退到最新。');
      const changed = snapshot?.world_id !== data.world_id || snapshot?.revision !== data.revision ||
        snapshot?.status !== data.status || JSON.stringify(snapshot?.persons) !== JSON.stringify(data.persons) || !quiet;
      if (changed) { quietDraft = false; updateControls(); }
      if (seq === null) latestRevision = data.revision;
      snapshot = data; healthy = true; personStateReady = true;
      if (changed) render();
      show('app-status', isReplay() ? '历史只读视图 · 不调用模型，不推进世界。' : meta?.read_only === true ? '只读归档 · 不调用模型，不推进世界。' : meta?.mode === 'real' ? '已连接本机世界 · real模式；服务未配置或失败将技术暂停，不回退脚本。' : '已连接本机世界 · 脚本模式，不是真实模型输出。');
      await loadPerson();
      return true;
    } catch (error) {
      if (!stateGate.isCurrent(request)) return false;
      healthy = false;
      personStateReady = false;
      $('causality').replaceChildren(); $('causality').setAttribute('aria-busy','false');
      show('causality-status','因果读取失败；未展示其他版本或伪造记录。',true);
      show('person-status','目标世界读取失败，人物见闻不可用；未回退到最新。',true);
      show('app-status',`${error.message}${snapshot ? ' 保留上次成功读取内容；写操作已锁定。' : ' 尚无世界数据，未加载演示回退。'}`,true);
      return false;
    } finally {
      if (stateGate.isCurrent(request)) { loading = false; quietDraft = false; updateControls(); }
    }
  }

  async function control(action) {
    if (!policy()[action]) return;
    keepDraft(); busy = true; stateGate.invalidate(); personGate.invalidate(); updateControls();
    show('app-status','正在提交控制请求；请等待一致状态…');
    try {
      await api('/api/control', {action:action === 'run' && snapshot.status === 'horizon_paused' ? 'resume' : action});
      const verified = await loadState(null);
      if (!verified) show('app-status','控制请求已返回，但最新世界核对失败；不要盲目重试，请重新读取。',true);
    } catch (error) {
      healthy = false;
      show('app-status',`${error.message} 请重新读取最新世界核对后再操作。`,true);
    } finally { busy = false; updateControls(); }
  }

  async function submitCard(event) {
    event.preventDefault();
    if (!policy().card || !personId || !person()) return;
    const id = personId;
    const changes = cardChanges(Object.fromEntries(CARD_KEYS.map(key => [key,$('card-form').elements.namedItem(key).value])));
    drafts.set(id,changes); busy = true; stateGate.invalidate(); personGate.invalidate(); updateControls();
    show('card-feedback','正在保存前向修改…');
    try {
      await api('/api/card',{person_id:id,changes});
      const verified = await loadState(null);
      const saved = snapshot?.persons.find(p => p.id === id)?.card;
      if (!verified || !CARD_KEYS.every(key => saved?.[key] === changes[key])) throw new Error('保存后的读回未确认一致，请重新读取核对；草稿已保留。');
      drafts.delete(id); renderCard(); show('card-feedback','已保存并从最新世界核对；仅影响今后的角色行为。');
    } catch (error) {
      healthy = false;
      show('card-feedback',`${error.message} 草稿保留；请重新读取核对。`,true);
    } finally { busy = false; updateControls(); }
  }

  async function loadMeta() {
    try {
      meta = await api('/api/meta');
      const archive = meta?.mode === 'readonly-replay' && meta.read_only === true;
      if (!archive && (!meta?.csrf || !['scripted-pi', 'real'].includes(meta.mode))) { meta = null; throw new Error('模型模式 / 安全令牌未就绪；写操作禁用。'); }
      text('model-mode', archive ? '只读归档 · 不调用模型，不推进世界' : meta.mode === 'real' ? 'real · 自主角色 / 独立DM（连接与计费尚未验收）' : meta.dm_mode === 'scripted-dm' ? '脚本角色 / 脚本 DM · scripted-pi / scripted-dm' : '脚本角色 · scripted-pi / DM 元信息未提供');
      if (archive && healthy && !isReplay()) show('app-status','只读归档 · 不调用模型，不推进世界。');
      $('limitations').replaceChildren(...list(meta.limitations).map(item => element('li', typeof item === 'string' ? item : JSON.stringify(item))));
      return true;
    } catch (error) { meta = null; text('model-mode','脚本角色 / 脚本 DM · 元信息读取失败'); show('app-status',error.message,true); return false; }
    finally { updateControls(); }
  }

  $('country-select').addEventListener('change', () => {
    keepDraft(); countryId = $('country-select').value;
    renderSelectors(); renderCard(); renderMap(); updateControls(); void loadPerson(true);
  });
  $('person-select').addEventListener('change', () => void selectPerson($('person-select').value));
  $('card-form').addEventListener('input', () => { keepDraft(); show('card-feedback','未提交草稿 · 仅保留在当前页面。'); });
  $('card-form').addEventListener('submit', event => void submitCard(event));
  $('reset-card').addEventListener('click', () => { drafts.delete(personId); renderCard(); });
  $('replay').addEventListener('input', () => {
    if (busy) return;
    keepDraft();
    const seq = Number($('replay').value);
    // Even the latest revision selected with the slider remains explicitly read-only.
    void loadState(seq);
  });
  $('latest').addEventListener('click', () => { if (!busy) void loadState(null); });
  $('retry').addEventListener('click', async () => { if (busy || loading) return; await loadMeta(); await loadState(replaySeq); });
  for (const action of ['step','run','pause']) $(action).addEventListener('click', () => void control(action));
  const onUnload = event => { if (drafts.size) { event.preventDefault(); event.returnValue = ''; } };
  doc.defaultView?.addEventListener('beforeunload',onUnload);
  const poll = setInterval(() => {
    if (snapshot && meta?.read_only !== true && !isReplay() && !busy && !loading && healthy && !doc.hidden) void loadState(null,true);
  }, 2500);
  const ready = Promise.all([loadMeta(), loadState(null)]);
  return {ready, dispose:() => { clearInterval(poll); stateGate.invalidate(); personGate.invalidate(); doc.defaultView?.removeEventListener('beforeunload',onUnload); }};
}

if (typeof document !== 'undefined') boot();
