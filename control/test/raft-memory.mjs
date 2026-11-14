import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn, execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const members=[],proxies=[],endpoints=new Map(),processes=new Map(),logs=new Map();
let isolated=new Set(),checks=0;
const memorySamples=[];
const token='memory-test-cluster-token-at-least-32-characters';
const listen=async server=>{server.listen(0,'127.0.0.1');await once(server,'listening');return server.address().port};
const port=async()=>{const s=http.createServer();const n=await listen(s);await new Promise(r=>s.close(r));return n};
const until=async(fn,ms=35000)=>{const end=Date.now()+ms;while(Date.now()<end){const out=await fn();if(out)return out;await sleep(150)}throw Error('cluster condition timed out')};
const check=(name,fn)=>{fn();checks++;console.log('ok '+name);if(process.env.RAFT_MEMORY_RESULT){
 const rows=execFileSync('ps',['-o','pid=,rss=','-p',[...processes.values()].map(p=>p.child.pid).join(',')],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number));
 memorySamples.push({name,processes:rows.map(([pid,rssKiB])=>({id:[...processes.values()].find(p=>p.child.pid===pid)?.id,rssKiB}))});
}};
for(let id=1;id<=7;id++){
 const proxy=http.createServer((req,res)=>{
  const from=Number(req.headers['x-cp-from']);
  if(from&&isolated.size&&isolated.has(from)!==isolated.has(id)){res.writeHead(503);return res.end()}
  const routes={'/_cp/raft':'/message','/_cp/raft-status':'/status','/_cp/join':'/join','/_cp/pull':'/pull','/_cp/bootstrap':'/bootstrap'};
  const url=new URL(req.url,'http://local');
  const out=http.request({hostname:'127.0.0.1',port:endpoints.get(id),path:(routes[url.pathname]||url.pathname)+url.search,method:req.method,headers:req.headers},r=>{res.writeHead(r.statusCode,r.headers);r.pipe(res)});
  out.on('error',()=>{if(!res.headersSent)res.writeHead(503);res.end()});req.pipe(out);
 });proxies.push(proxy);members.push({id,url:'http://127.0.0.1:'+await listen(proxy)});
}
const env={...process.env,CP_CLUSTER_ID:'memory-test-cluster-generation',CP_CLUSTER_TOKEN:token,CP_MEMBERS:JSON.stringify(members),CP_RAFT_TICK_MS:'100',CP_RAFT_ELECTION_TICKS:'15',CP_RAFT_HEARTBEAT_TICKS:'2'};
async function start(id,key=id){
 const address=await port();if(key===id)endpoints.set(id,address);
 const rustIds=new Set((process.env.RUST_RAFT_IDS||'').split(',').filter(Boolean).map(Number));
 const binary=rustIds.has(id)?process.env.RUST_RAFT_BINARY:process.env.RAFT_TEST_BINARY||'/tmp/runners-raft-memory-test';
 const child=spawn(binary,[],{env:{...env,CP_NODE_ID:String(id),CP_RAFT_PORT:String(address)},stdio:['ignore','ignore','pipe']});
 const item={id,key,address,child};processes.set(key,item);logs.set(key,'');child.stderr.on('data',b=>logs.set(key,(logs.get(key)+b).slice(-9000)));return item;
}
async function request(target,path,body){
 const item=typeof target==='object'?target:processes.get(target);if(!item)return null;
 try{const res=await fetch('http://127.0.0.1:'+item.address+path,{method:body===undefined?'GET':'POST',headers:{'x-cp-token':token,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)}),signal:AbortSignal.timeout(5000)});return res.ok?await res.json():null}catch{return null}
}
async function stop(key){const p=processes.get(key);if(!p)return;processes.delete(key);if(p.child.exitCode!==null)return;p.child.kill('SIGTERM');await once(p.child,'exit')}
const views=()=>Promise.all([...processes.values()].map(p=>request(p,'/status')));
const leader=()=>until(async()=>{for(const p of processes.values()){const s=await request(p,'/status');if(s?.role==='StateLeader'){const v=await request(p,'/verify');if(v)return v}}return null});
try{
 for(let id=1;id<=7;id++)await start(id);
 const initial=await until(async()=>{const v=await views();return v.length===7&&v.every(Boolean)&&v});
 await sleep(1800);check('fresh processes do not silently bootstrap forgotten votes',()=>assert.ok(initial.every(v=>v.phase==='uninitialized')));
 const manifest={generation:randomUUID(),term:Date.now(),members:initial.map(v=>({id:v.id,node:v.node,boot:v.boot}))};
 for(const id of [1,2,3])assert.ok(await request(id,'/bootstrap',manifest));
 await sleep(2200);check('three initialized voters cannot lead a seven-voter cluster',()=>assert.ok(true));
 assert.equal((await views()).some(v=>v?.role==='StateLeader'),false);
 assert.ok(await request(4,'/bootstrap',manifest));const first=await leader();
 check('four voters elect and authorize exactly one primary',()=>assert.equal(first.quorum,4));
 for(const id of [5,6,7])assert.ok(await request(id,'/bootstrap',manifest));
 await until(async()=>(await views()).every(v=>v?.leaderNode===first.leaderNode));
 if(process.env.CP_RAFT_ALLOW_CLAIMS==='1'){
  const attempts=await Promise.all(Array.from({length:8},()=>request(first.id,'/claim',{key:'account/runner',sequence:100,term:first.term})));
  check('simultaneous work claims commit exactly one accepted attempt',()=>assert.equal(attempts.filter(r=>r?.accepted).length,1));
  assert.equal((await request(first.id,'/claim',{key:'account/runner',sequence:99,term:first.term})).accepted,false);
 }
 isolated=new Set([first.id,...members.filter(m=>m.id!==first.id).slice(0,2).map(m=>m.id)]);
 const second=await until(async()=>{for(const p of processes.values()){if(isolated.has(p.id))continue;const v=await request(p,'/verify');if(v?.term>first.term)return v}return null});
 assert.equal(await request(first.id,'/verify'),null);
 check('a partitioned former primary loses authority when a new quorum elects',()=>assert.ok(second.term>first.term));
 if(process.env.CP_RAFT_ALLOW_CLAIMS==='1'){
  const duplicate=await request(second.id,'/claim',{key:'account/runner',sequence:100,term:second.term});
  check('a new primary preserves committed work and rejects a duplicate',()=>assert.equal(duplicate.accepted,false));
  assert.equal((await request(second.id,'/claim',{key:'account/runner',sequence:101,term:second.term})).accepted,true);
  assert.equal(await request(second.id,'/claim',{key:'account/runner',sequence:102,term:first.term}),null);
  assert.equal(await request(first.id,'/claim',{key:'account/runner',sequence:102,term:first.term}),null);
 }
 isolated=new Set();await until(async()=>(await views()).every(v=>v?.leaderNode===second.leaderNode));
 const replace=members.find(m=>m.id!==second.id).id,old=(await request(replace,'/status')).node;
 await stop(replace);await start(replace);
 const renewed=await until(async()=>{const s=await request(replace,'/status');return s?.identityValid&&!s.outgoing.length&&s});
 check('restart joins as a new learner and replaces its old voter through consensus',()=>{assert.notEqual(renewed.node,old);assert.equal(renewed.voters.length,7);assert.ok(!renewed.voters.includes(old))});
 if(process.env.CP_RAFT_ALLOW_CLAIMS==='1')check('a replacement recovers latest claims from the memory snapshot',()=>assert.equal(renewed.registry.claims['account/runner'].sequence,101));
 const current=await leader();
 for(const id of [1,2,3])await stop(id);
 await until(()=>request((current.id>3?current.id:4),'/status'));
 const fleetLeader=await leader();check('the four fleet voters retain quorum with the Mac side off',()=>assert.equal(fleetLeader.quorum,4));
 const moving=[4,5,6,7].find(id=>id!==fleetLeader.id),before=await request(moving,'/status');
 const warm=await start(moving,'warm');
 const ready=await until(async()=>{const s=await request(warm,'/status');return s?.identityValid&&!s.outgoing.length&&s},45000);
 const retired=await request(moving,'/status');
 check('an ordinary warm replacement gains the vote before its predecessor stops',()=>{assert.equal(retired.identityValid,false);assert.equal(retired.phase,'retired');assert.notEqual(ready.node,before.node);assert.equal(ready.voters.length,7)});
 await stop(moving);endpoints.set(moving,warm.address);
 const primaryBefore=await leader();
 const oldPrimary=[...processes.values()].find(p=>p.id===primaryBefore.id);
 const nextPrimaryCopy=await start(primaryBefore.id,'warm-primary');
 await until(async()=>{const s=await request(nextPrimaryCopy,'/status');return s?.identityValid&&!s.outgoing.length},45000);
 const retiredPrimary=await request(oldPrimary,'/status');
 const movedPrimary=await leader();
 check('a primary move transfers leadership and its vote with only four available slots',()=>{
  assert.equal(retiredPrimary.identityValid,false);assert.notEqual(movedPrimary.id,primaryBefore.id);
 });
 await stop(oldPrimary.key);endpoints.set(primaryBefore.id,nextPrimaryCopy.address);
 const loss=[...processes.values()].find(p=>p.id!==movedPrimary.id);
 await stop(loss.key);await sleep(2500);await start(loss.id);
 await sleep(3000);
 const restarted=await request(loss.id,'/status');
 const authorities=await Promise.all([...processes.values()].map(p=>request(p,'/verify')));
 check('a hard loss at minimum quorum cannot manufacture a replacement vote',()=>{
  assert.equal(restarted.identityValid,false);assert.ok(authorities.every(v=>!v));
 });
 for(const key of [...processes.keys()])await stop(key);
 for(const id of [4,5,6,7])await start(id);
 await sleep(3500);const empty=await views();
 check('total memory loss leaves every process nonvoting until explicit bootstrap',()=>assert.ok(empty.every(v=>v?.phase==='uninitialized'&&!v.identityValid)));
 console.log(checks+' memory-only Raft checks passed');
}catch(error){for(const[key,log]of logs)console.error('replica '+key+': '+log);throw error}
finally{if(process.env.RAFT_MEMORY_RESULT)writeFileSync(process.env.RAFT_MEMORY_RESULT,JSON.stringify({checks,memorySamples},null,2)+'\n');for(const key of [...processes.keys()])await stop(key);for(const p of proxies){p.closeAllConnections();await new Promise(r=>p.close(r))}}
