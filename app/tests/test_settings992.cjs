'use strict';
// Prospective source-bound fakeDOM/VM tests. Not browser/auth evidence.
// node app/tests/test_settings992.cjs [absolute-or-relative-settings.js]
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {test} = require('node:test');
const sourcePath = path.resolve(process.argv[2] || path.join(__dirname, '../web/settings.js'));
const source = fs.readFileSync(sourcePath, 'utf8');
const SIWC = 'openai-siwc-responses', CHAT = 'openai-chat-https';
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return {promise, resolve, reject};
}
function harness() {
  const nodes = new Map(), calls = [], windowListeners = new Map();
  class Element {
    constructor(tag) { this.tagName = tag; this.children = []; this.listeners = new Map(); this._value = ''; this.disabled = false; this.hidden = false; this.textContent = ''; this.open = false; }
    set id(id) { this._id = id; nodes.set(id, this); }
    get id() { return this._id; }
    get value() {
      if (this.tagName !== 'select') return this._value;
      return (this.children.find(o => o.selected) || this.children[0])?.value || '';
    }
    set value(value) {
      value = String(value);
      if (this.tagName === 'select') {
        for (const o of this.children) o.selected = o.value === value;
        // These tests assign only existing options, as the browser does.
      } else this._value = value;
    }
    append(...children) {
      for (const child of children) {
        if (child.selected && this.tagName === 'select') for (const old of this.children) old.selected = false;
        this.children.push(child);
      }
    }
    replaceChildren(...children) { this.children = []; this.append(...children); }
    setAttribute(name, value) { this[name] = value; }
    addEventListener(event, fn) { const list = this.listeners.get(event) || []; list.push(fn); this.listeners.set(event, list); }
    fire(event) { return Promise.all((this.listeners.get(event) || []).map(fn => fn({preventDefault() {}}))); }
    showModal() { this.open = true; }
    close() { this.open = false; }
  }
  for (const id of ['services','status','settings','siwc-state','siwc-account','siwc-welcome','siwc-login','siwc-select','siwc-models','siwc-logout','siwc-got-it','save']) {
    const el = new Element(id === 'siwc-account' ? 'select' : 'div'); el.id = id;
  }
  const document = {getElementById: id => nodes.get(id), createElement: tag => new Element(tag), querySelector: selector => { assert.equal(selector, '#settings button'); return nodes.get('save'); }};
  const context = vm.createContext({document, window: {addEventListener: (name, fn) => windowListeners.set(name, fn)}, fetch: (url, options = {}) => {
    const d = deferred(); calls.push({url, options, ...d, taken: false}); return d.promise;
  }});
  vm.runInContext(source, context, {filename: sourcePath});
  function take(url) { const call = calls.find(c => c.url === url && !c.taken); assert.ok(call, `missing request ${url}`); call.taken = true; return call; }
  function reply(call, value, ok = true) { call.resolve({ok, json: async () => value}); }
  // A deterministic microtask fence, not a wall-clock sleep.
  async function settle() { for (let i = 0; i < 24; i++) await Promise.resolve(); }
  function value(account = 'A', providers = [SIWC, SIWC]) {
    return {services: Object.fromEntries(['roles','dm'].map((kind, i) => [kind, {provider: providers[i], model: `${kind}-original`, timeout_seconds: 53 + i, max_retries: i}])),
      siwc: {active: account, accounts: ['A','B'].map(a => ({account: a, label: `account ${a}`, active: a === account})), state: 'authorized_not_verified', connecting: false, connection_verified: false, real_call_verified: false, credential_status: 'unknown', error: null, welcome_required: false}};
  }
  async function load(account = 'A', providers = [SIWC, SIWC]) {
    reply(take('/api/meta'), {csrf: 'synthetic-csrf', mode: 'real'}); await settle();
    reply(take('/api/settings'), value(account, providers)); await settle();
  }
  const get = id => nodes.get(id);
  const catalog = slug => ({models: [{slug, display_name: slug}]});
  return {get, take, reply, settle, load, value, catalog, calls, context, reload: () => vm.runInContext('reload()', context)};
}
function fields(h, kind) { return Object.fromEntries(['provider','base_url','model','secret','timeout_seconds','max_retries'].map(n => [n, h.get(`${kind}-${n}`).value])); }
function seedForeign(h, kind) {
  for (const [name, value] of Object.entries({base_url:'https://example.invalid/custom/v1', model:'foreign-slug', secret:'synthetic-not-a-real-secret', timeout_seconds:'47', max_retries:'2'})) h.get(`${kind}-${name}`).value = value;
}
async function models(h, slug) {
  const work = h.get('siwc-models').fire('click'); h.reply(h.take('/api/siwc/models'), h.catalog(slug)); await work; return work;
}
for (const providers of [[SIWC, CHAT], [CHAT, SIWC], [CHAT, CHAT], [SIWC, SIWC]]) {
  test(`catalog respects independent providers ${providers.join('/')}`, async () => {
    const h = harness(); await h.load('A', providers);
    for (const [i, kind] of ['roles','dm'].entries()) if (providers[i] !== SIWC) seedForeign(h, kind);
    const before = Object.fromEntries(['roles','dm'].map(k => [k, fields(h,k)]));
    await models(h, 'A-model');
    for (const [i, kind] of ['roles','dm'].entries()) {
      const picker = h.get(`${kind}-siwc-model`);
      if (providers[i] !== SIWC) {
        assert.ok(picker.disabled || picker.hidden);
        // Programmatic change must also be guarded, not just UI-disabled.
        await picker.fire('change'); assert.deepEqual(fields(h, kind), before[kind]);
      } else { assert.equal(picker.disabled, false); assert.equal(h.get(`${kind}-model`).value, 'A-model'); }
    }
    const save = h.get('settings').fire('submit'); const request = h.take('/api/settings');
    const body = JSON.parse(request.options.body);
    for (const [i, kind] of ['roles','dm'].entries()) if (providers[i] !== SIWC) {
      const expected = before[kind];
      assert.deepEqual(body.services[kind], {provider:expected.provider, base_url:expected.base_url, model:expected.model, timeout_seconds:Number(expected.timeout_seconds), max_retries:Number(expected.max_retries), credential_ref:null});
      assert.equal(body.secrets[kind], expected.secret);
    }
    h.reply(request, {}); await save;
  });
}
test('A late catalog cannot overwrite B after select and B catalog completion', async () => {
  const h = harness(); await h.load();
  const old = h.get('siwc-models').fire('click'), a = h.take('/api/siwc/models');
  h.get('siwc-account').value = 'B'; await h.get('siwc-account').fire('change');
  const selection = h.get('siwc-select').fire('click'); h.reply(h.take('/api/siwc/select'), h.value('B').siwc); await h.settle();
  await h.load('B'); await selection;
  // B finishes its own catalog before the already-completed A response arrives.
  assert.equal(h.get('siwc-models').disabled, false);
  await models(h, 'B-model');
  const beforeState = h.get('siwc-state').textContent;
  h.reply(a, h.catalog('A-stale')); await old;
  assert.equal(h.get('siwc-models').disabled, false);
  assert.equal(h.get('siwc-state').textContent, beforeState);
  assert.equal(h.get('roles-model').value, 'B-model');
  assert.equal(h.get('siwc-account').value, 'B');
  assert.ok(!h.get('siwc-state').textContent.includes('A-stale'));
});
test('older settings reload completing last cannot roll back B or enable stale pickers', async () => {
  const h = harness(); await h.load();
  const first = h.reload(); h.reply(h.take('/api/meta'), {csrf:'old',mode:'real'}); await h.settle(); const oldStatus = h.take('/api/settings');
  const second = h.reload(); await h.load('B'); await second;
  h.reply(oldStatus, h.value('A')); await first;
  assert.equal(h.get('siwc-account').value, 'B'); await models(h, 'B-only');
  assert.equal(h.get('dm-model').value, 'B-only');
});
test('provider changes invalidate pending catalog in both directions without rewriting foreign fields', async () => {
  const h = harness(); await h.load();
  const work = h.get('siwc-models').fire('click'), request = h.take('/api/siwc/models');
  h.get('roles-provider').value = CHAT; await h.get('roles-provider').fire('change'); seedForeign(h,'roles');
  const before = fields(h,'roles'); h.reply(request, h.catalog('old')); await work;
  assert.deepEqual(fields(h,'roles'), before); assert.equal(h.get('dm-model').value, 'dm-original');
  await models(h,'fresh'); assert.equal(h.get('dm-model').value,'fresh');
  h.get('roles-provider').value = SIWC; await h.get('roles-provider').fire('change');
  assert.equal(h.get('roles-siwc-model').disabled, true);
  await models(h,'new'); assert.equal(h.get('roles-model').value,'new');
});
test('reload cancels pending catalog; stale rejection cannot replace current state; button restored', async () => {
  const h = harness(); await h.load();
  const old = h.get('siwc-models').fire('click'), request = h.take('/api/siwc/models');
  const reload = h.reload(); await h.load('B'); await reload;
  const before = h.get('siwc-state').textContent; request.reject(new Error('OLD_FAILURE')); await old;
  assert.equal(h.get('siwc-state').textContent, before); assert.equal(h.get('siwc-models').disabled, false);
  const failure = h.get('siwc-models').fire('click'); h.reply(h.take('/api/siwc/models'), {error:{code:'CURRENT_FAILURE'}}, false); await failure;
  assert.ok(h.get('siwc-state').textContent.includes('CURRENT_FAILURE')); assert.equal(h.get('siwc-models').disabled, false);
  await models(h,'recovered'); assert.equal(h.get('roles-model').value,'recovered');
});
test('old model finally cannot enable the newer pending model button', async () => {
  const h = harness(); await h.load();
  const old = h.get('siwc-models').fire('click'), a = h.take('/api/siwc/models');
  const refresh = h.reload(); await h.load('B'); await refresh;
  const current = h.get('siwc-models').fire('click'), b = h.take('/api/siwc/models');
  h.reply(a,h.catalog('A')); await old;
  assert.equal(h.get('siwc-models').disabled,true);
  h.reply(b,h.catalog('B')); await current;
  assert.equal(h.get('siwc-models').disabled,false); assert.equal(h.get('roles-model').value,'B');
});
test('account mutation blocks competing mutations and focus reload until its own status read', async () => {
  const h = harness(); await h.load();
  const reload = h.reload(); h.reply(h.take('/api/meta'), {csrf:'old',mode:'real'}); await h.settle();
  const oldStatus = h.take('/api/settings');
  h.get('siwc-account').value = 'B'; await h.get('siwc-account').fire('change');
  const selection = h.get('siwc-select').fire('click'), selectRequest = h.take('/api/siwc/select');
  const count = h.calls.length;
  await h.get('siwc-logout').fire('click'); await h.reload();
  assert.equal(h.calls.length,count);
  h.reply(selectRequest,h.value('B').siwc); await h.settle(); await h.load('B'); await selection;
  h.reply(oldStatus,h.value('A')); await reload;
  assert.equal(h.get('siwc-account').value,'B'); assert.equal(h.get('siwc-select').disabled,false);
});
for (const [button, action] of [['siwc-login','begin'], ['siwc-logout','logout'], ['siwc-got-it','welcome']]) {
  test(`${action} fences old catalog, failure restores action buttons`, async () => {
    const h = harness(); await h.load();
    const old = h.get('siwc-models').fire('click'), request = h.take('/api/siwc/models');
    const work = h.get(button).fire('click'); const mutation = h.take(`/api/siwc/${action}`);
    h.reply(request,h.catalog('stale')); await old;
    assert.equal(h.get('roles-model').value,'roles-original');
    mutation.reject(new Error('MUTATION_FAILED')); await work;
    assert.equal(h.get(button).disabled,false); assert.equal(h.get('siwc-select').disabled,false);
    const refresh = h.reload(); await h.load('B'); await refresh; await models(h,'B');
    assert.equal(h.get('dm-model').value,'B');
  });
}
