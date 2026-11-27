// Real, unmodified Pi SDK. ONLY the provider is a scripted test double.
// No credentials, remote API, or external network is used by this process.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath, pathToFileURL} from 'node:url';
import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import tls from 'node:tls';
import dns from 'node:dns';
import dgram from 'node:dgram';
import {syncBuiltinESMExports} from 'node:module';
import readline from 'node:readline';
import {randomUUID} from 'node:crypto';

const deny = () => { throw Error('NETWORK_DISABLED'); };
globalThis.fetch = deny;
for (const mod of [http, https]) { mod.request = deny; mod.get = deny; }
net.Socket.prototype.connect = deny;
net.connect = net.createConnection = tls.connect = deny;
dgram.createSocket = deny;
for (const key of Object.keys(dns)) {
  if (typeof dns[key] === 'function') dns[key] = deny;
}
for (const key of Object.keys(dns.promises)) {
  if (typeof dns.promises[key] === 'function') dns.promises[key] = deny;
}
syncBuiltinESMExports();
const here = path.dirname(fileURLToPath(import.meta.url));
const candidates = process.env.PI_MODULE_ROOT ? [path.resolve(process.env.PI_MODULE_ROOT)] : [
  path.join(here, 'node_modules'),
  path.resolve(here, '../spikes/002-pi-isolation/npm/node_modules'),
];
const root = candidates.find(p => fs.existsSync(path.join(p, '@earendil-works/pi-agent-core/dist/index.js')));
if (!root) throw Error('REAL_PI_SDK_NOT_FOUND');
const load = relative => import(pathToFileURL(path.join(root, relative)).href);
const {Agent} = await load('@earendil-works/pi-agent-core/dist/index.js');
const {AssistantMessageEventStream} = await load('@earendil-works/pi-ai/dist/utils/event-stream.js');
const {Type} = await load('typebox/build/index.mjs');
const model = {id:'SCRIPTED_PROVIDER_NOT_A_REAL_MODEL', name:'Local scripted provider',
  api:'local-scripted', provider:'scripted-pi', baseUrl:'', reasoning:false, input:['text'],
  cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:64000,maxTokens:1000};
const usage = {input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,
  cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
const text = value => ({type:'text',text:value});
async function act(q) {
  let requests=0, calls=[];
  const events={};
  const streamFn = (_model, context) => {
    if (++requests > 2) throw Error('SDK_REQUEST_LIMIT');
    const stream = new AssistantMessageEventStream();
    const content = requests===1 ? [{type:'toolCall',id:'pi-'+randomUUID(),
      name:'record_proposal',arguments:q.body}] : [text('提案已记下，尚待裁定；不能当作已经实现。')];
    const message = {role:'assistant',content,api:model.api,provider:model.provider,model:model.id,
      usage,stopReason:requests===1?'toolUse':'stop',timestamp:Date.now()};
    queueMicrotask(() => { stream.push({type:'start',partial:structuredClone(message)});
      stream.push({type:'done',reason:message.stopReason,message}); stream.end(message); });
    return stream;
  };
  const tool = {name:'record_proposal', label:'记录提案',
    description:'记录你的一个行动提案，等待裁定；不是行动已实现。',
    parameters:Type.Object({action:Type.String()}, {additionalProperties:true}),
    execute:async (callId,args) => {
      if (calls.length || JSON.stringify(args)!==JSON.stringify(q.body)) throw Error('PROPOSAL_MISMATCH');
      calls.push({call_id:callId,intent_id:q.intent_id,person_id:q.person_id});
      return {content:[text('提案已记录，等候裁定。')],details:{recorded:true}};
    }};
  const agent = new Agent({initialState:{model,tools:[tool],messages:q.messages,
    thinkingLevel:'off',systemPrompt:q.system_prompt},streamFn,getApiKey:()=>undefined});
  agent.subscribe(event => { events[event.type]=(events[event.type]||0)+1; });
  await agent.prompt({role:'user',content:[text(q.prompt)],timestamp:Date.now()});
  await agent.waitForIdle();
  if (agent.state.errorMessage || calls.length!==1 || agent.state.messages.some(m=>m.role==='toolResult'&&m.isError)) {
    throw Error('PI_TECHNICAL_ERROR');
  }
  return {messages:agent.state.messages,calls,sdk_events:events,sdk_requests:requests,
    provider:'scripted-pi',module_root:root,pid:process.pid};
}
async function networkProbe() {
  const blocked=[];
  const attempts=[['fetch',()=>fetch('https://example.invalid')],
    ['http',()=>http.get('http://127.0.0.1:1')],['https',()=>https.get('https://example.invalid')],
    ['socket',()=>net.connect(1,'127.0.0.1')],['tls',()=>tls.connect(1,'127.0.0.1')],
    ['dns',()=>dns.lookup('example.invalid',()=>{})],['udp',()=>dgram.createSocket('udp4')]];
  for(const [name, attempt] of attempts) {
    try { await attempt(); throw Error('NETWORK_GUARD_FAILED:'+name); }
    catch(error) { if(error.message!=='NETWORK_DISABLED') throw error; blocked.push(name); }
  }
  return {network_blocked:blocked,module_root:root,pid:process.pid};
}
const lines=readline.createInterface({input:process.stdin,crlfDelay:Infinity});
for await (const line of lines) {
  let q;
  try { q=JSON.parse(line);
    if(q.op==='close') { process.stdout.write(JSON.stringify({id:q.id,ok:true,closed:true})+'\n'); break; }
    const result=q.op==='network_probe' ? await networkProbe() : await act(q);
    process.stdout.write(JSON.stringify({id:q.id,ok:true,result})+'\n');
  } catch(error) {
    process.stdout.write(JSON.stringify({id:q?.id,ok:false,error:String(error)})+'\n');
  }
}
lines.close();
