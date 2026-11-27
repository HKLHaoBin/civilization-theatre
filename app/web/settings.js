'use strict';
const kinds = [['roles', '角色基础服务'], ['dm', '独立世界主持者']];
const container = document.getElementById('services');
const status = document.getElementById('status');
let csrf = null;
let siwcRevision = 0, siwcAccount = null, realMode = false, accountAction = null;
const siwcPending = new Map(), siwcCatalog = new Map();
const siwcButtons = {begin:'siwc-login',select:'siwc-select',models:'siwc-models',logout:'siwc-logout',welcome:'siwc-got-it'};
function siwcButtonsUpdate() {
  for (const [action, id] of Object.entries(siwcButtons)) {
    document.getElementById(id).disabled = !realMode || !!accountAction || siwcPending.has(action)
      || (action === 'models' && (!siwcAccount || document.getElementById('siwc-account').value !== siwcAccount));
  }
}
function invalidateSIWC() {
  siwcRevision += 1;
  // Superseded models may settle later; they no longer own this button or UI.
  siwcPending.delete('models'); siwcCatalog.clear();
  for (const [kind] of kinds) {
    const picker = document.getElementById(`${kind}-siwc-model`);
    if (picker) { picker.replaceChildren(); picker.disabled = true; }
  }
  siwcButtonsUpdate();
  return siwcRevision;
}
for (const [kind, title] of kinds) {
  const fieldset = document.createElement('fieldset');
  const legend = document.createElement('legend'); legend.textContent = title; fieldset.append(legend);
  const fields = [
    ['provider', '协议', 'select'], ['base_url', 'API 基址（不含认证、查询或片段）', 'url'],
    ['model', '模型标识', 'text'], ['timeout_seconds', '独立请求总预算秒数（1–120）', 'number'],
    ['max_retries', '最多重试次数（0–3）', 'number'], ['secret', '新 API 密钥（留空保留，不回显）', 'password']
  ];
  for (const [name, text, type] of fields) {
    const label = document.createElement('label'); label.textContent = text;
    const input = document.createElement(type === 'select' ? 'select' : 'input');
    input.name = `${kind}.${name}`; input.id = `${kind}-${name}`;
    if (type === 'select') {
      for (const [value, text] of [['openai-chat-https', 'HTTPS / 独立 API 密钥'], ['openai-chat-loopback', 'Loopback HTTP / 无密钥'], ['openai-siwc-responses', 'Using ChatGPT plan / 本游戏独立授权']]) {
        const option = document.createElement('option'); option.value = value; option.textContent = text; input.append(option);
      }
    } else {
      input.type = type; input.autocomplete = 'off';
      if (name === 'base_url') input.value = 'https://api.openai.com/v1';
      if (name === 'timeout_seconds') { input.min = '1'; input.max = '120'; input.value = '60'; }
      if (name === 'max_retries') { input.min = '0'; input.max = '3'; input.value = '0'; }
      if (name === 'secret') { input.maxLength = 2560; input.autocomplete = 'new-password'; }
      else input.required = true;
    }
    label.append(input); fieldset.append(label, document.createElement('br'));
  }
  container.append(fieldset);
  const picker = document.createElement('select'); picker.id = `${kind}-siwc-model`;
  picker.setAttribute('aria-label', `${title} ChatGPT account models`); picker.disabled = true;
  picker.addEventListener('change', () => {
    const owner = siwcCatalog.get(kind);
    if (document.getElementById(`${kind}-provider`).value !== 'openai-siwc-responses'
        || picker.disabled || !owner || owner.revision !== siwcRevision || owner.account !== siwcAccount
        || document.getElementById('siwc-account').value !== owner.account) return;
    document.getElementById(`${kind}-model`).value = picker.value;
  });
  fieldset.append(picker);
  const provider = document.getElementById(`${kind}-provider`);
  const update = () => {
    const siwc = provider.value === 'openai-siwc-responses';
    const url = document.getElementById(`${kind}-base_url`), secret = document.getElementById(`${kind}-secret`);
    url.readOnly = siwc; secret.disabled = siwc;
    document.getElementById(`${kind}-model`).readOnly = siwc;
    picker.hidden = !siwc; picker.disabled = !siwc || !siwcCatalog.has(kind);
    if (siwc) { url.value = 'https://api.openai.com/v1'; secret.value = ''; }
  };
  provider.addEventListener('change', () => { invalidateSIWC(); update(); });
  provider.updateSIWC = update;
}
document.getElementById('siwc-account').addEventListener('change', invalidateSIWC);
async function reload(owner = null) {
  // Do not let a focus/status read race an account mutation still in flight.
  if (accountAction && accountAction !== owner) return false;
  siwcAccount = null;
  const revision = invalidateSIWC();
  try {
    const meta = await fetch('/api/meta', {cache: 'no-store'}).then(r => r.json());
    if (revision !== siwcRevision) return false;
    const response = await fetch('/api/settings', {cache: 'no-store'});
    if (revision !== siwcRevision) return false;
    if (!response.ok) throw new Error('STATUS_FAILED');
    const value = await response.json();
    if (revision !== siwcRevision) return false;
    csrf = meta.csrf; realMode = meta.mode === 'real';
    status.textContent = JSON.stringify(value, null, 2);
    if (value.services) for (const [kind] of kinds) {
      for (const name of ['provider', 'model', 'timeout_seconds', 'max_retries'])
        document.getElementById(`${kind}-${name}`).value = value.services[kind][name];
      document.getElementById(`${kind}-provider`).updateSIWC();
    }
    // URL/ref intentionally never returned by status; no false connected badge.
    document.querySelector('#settings button').disabled = !realMode;
    siwcStatus(value.siwc);
    siwcButtonsUpdate();
    return true;
  } catch (_) {
    if (revision === siwcRevision) status.textContent = '读取失败；未检查凭据或连接。';
    return false;
  }
}
document.getElementById('settings').addEventListener('submit', async event => {
  event.preventDefault(); if (!csrf) return;
  const services = {}, secrets = {};
  for (const [kind] of kinds) {
    const get = name => document.getElementById(`${kind}-${name}`).value;
    services[kind] = {provider: get('provider'), base_url: get('base_url'), model: get('model'),
      timeout_seconds: Number(get('timeout_seconds')), max_retries: Number(get('max_retries')), credential_ref: null};
    const key = get('secret'); if (key) secrets[kind] = key;
  }
  const button = document.querySelector('#settings button'); button.disabled = true;
  try {
    const response = await fetch('/api/settings', {method: 'POST', cache: 'no-store',
      headers: {'Content-Type': 'application/json', 'X-Theatre-CSRF': csrf}, body: JSON.stringify({services, secrets})});
    if (!response.ok) { status.textContent = '保存失败；请检查服务字段、运行状态和本机凭据服务。'; return; }
    status.textContent = JSON.stringify(await response.json(), null, 2);
  } catch (_) { status.textContent = '保存失败；未测试模型连接。'; }
  finally {
    for (const [kind] of kinds) { document.getElementById(`${kind}-secret`).value = ''; delete secrets[kind]; }
    button.disabled = false;
  }
});
function siwcStatus(value) {
  siwcAccount = value?.active || null;
  if (!value) return;
  document.getElementById('siwc-state').textContent = (value.account_mutation_pending
    ? '账号事务待确认；请求已接收不等于持久完成，也不代表零调用或零费用。若提交先赢，需显式登出。\n' : '') + JSON.stringify(value, null, 2);
  const picker = document.getElementById('siwc-account');
  picker.replaceChildren();
  const fresh = document.createElement('option'); fresh.value = ''; fresh.textContent = '新增账号'; picker.append(fresh);
  for (const account of value.accounts || []) {
    const option = document.createElement('option'); option.value = account.account; option.textContent = account.label;
    option.selected = account.active; picker.append(option);
  }
  if (value.welcome_required && !document.getElementById('siwc-welcome').open) document.getElementById('siwc-welcome').showModal();
}
async function siwcAction(action, payload = {}) {
  if (!csrf) return;
  const response = await fetch(`/api/siwc/${action}`, {method: 'POST', cache: 'no-store',
    headers: {'Content-Type': 'application/json', 'X-Theatre-CSRF': csrf}, body: JSON.stringify(payload)});
  const value = await response.json();
  if (!response.ok) throw new Error(value.error?.code || 'SIWC_AUTH_ERROR');
  return value;
}
for (const action of ['begin', 'select', 'models', 'logout', 'welcome']) {
  const id = siwcButtons[action];
  document.getElementById(id).addEventListener('click', async () => {
    const button = document.getElementById(id);
    if (!csrf || !realMode || button.disabled || accountAction) return;
    const account = siwcAccount;
    const payload = ['begin','select'].includes(action) ? {account:document.getElementById('siwc-account').value || null} : {};
    const revision = invalidateSIWC(), token = {};
    siwcPending.set(action, token);
    if (action !== 'models') { accountAction = token; siwcAccount = null; }
    siwcButtonsUpdate();
    try {
      const value = await siwcAction(action, payload);
      if (revision !== siwcRevision) return;
      if (action === 'models') {
        if (!account || account !== siwcAccount || document.getElementById('siwc-account').value !== account) return;
        for (const [kind] of kinds) {
          if (document.getElementById(`${kind}-provider`).value !== 'openai-siwc-responses') continue;
          const picker = document.getElementById(`${kind}-siwc-model`); picker.replaceChildren();
          const current = document.getElementById(`${kind}-model`).value;
          for (const model of value.models) {
            const option = document.createElement('option'); option.value = model.slug; option.textContent = model.display_name;
            option.selected = model.slug === current; picker.append(option);
          }
          picker.disabled = !value.models.length;
          siwcCatalog.set(kind, {revision, account});
          if (value.models.length) document.getElementById(`${kind}-model`).value = picker.value;
        }
        document.getElementById('siwc-state').textContent = JSON.stringify(value, null, 2);
      } else {
        if (action === 'welcome') document.getElementById('siwc-welcome').close();
        const followRevision = siwcRevision + 1;
        if (await reload(token) && siwcRevision === followRevision && action === 'begin')
          document.getElementById('siwc-state').textContent = '系统浏览器中完成本游戏授权后，点击读取账号模型或刷新本页；尚未连接模型。';
      }
    } catch (error) {
      if (revision === siwcRevision) document.getElementById('siwc-state').textContent = error.message + '；没有自动切换计费路径。';
    } finally {
      if (siwcPending.get(action) === token) siwcPending.delete(action);
      if (accountAction === token) accountAction = null;
      siwcButtonsUpdate();
    }
  });
}
window.addEventListener('focus', () => { if (document.getElementById('siwc-state').textContent.includes('系统浏览器') || document.getElementById('siwc-state').textContent.includes('待确认')) reload(); });
reload();
