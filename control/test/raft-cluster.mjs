import assert from 'node:assert/strict';
import http from 'node:http';
import {spawn,fork} from 'node:child_process';
import {once} from 'node:events';
import {ClusterDatabase} from '../src/ha/database.mjs';
import {replicaCount,clusterConfig} from '../src/ha/config.mjs';
import {RuntimeTail} from '../src/ha/tail.mjs';
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
let checks=0;
const check=(name,fn)=>{fn();checks++;console.log('ok '+name)};
check('single mode and odd replicated sizes are accepted',()=>{for(const n of [1,3,5,7,9])assert.equal(replicaCount(n),n);assert.equal(clusterConfig({CP_REPLICAS:'1'}).enabled,false)});
check('two, even, fractional, zero and invalid counts are rejected',()=>{for(const n of [0,2,4,6,-1,3.5,'','three','03'])assert.throws(()=>replicaCount(n),/CP_REPLICAS/)});
check('runtime tail is bounded and reports gaps without persistence',()=>{const t=new RuntimeTail({maxBytes:2000,maxEntries:3});for(let i=0;i<8;i++)t.append('event',{i});assert.equal(t.entries.length,3);assert.equal(t.read(0).gap,true);assert.equal(t.read(7).events.length,1)});
const fixture=fork(new URL('./d1-fixture.mjs',import.meta.url),{stdio:['ignore','ignore','inherit','ipc']});
const [{port:dbport}]=await once(fixture,'message');
const env={...process.env,ACCOUNT_ID:'test',D1_DATABASE_ID:'test',D1_API_KEY:'test-global-key',D1_API_EMAIL:'test@example.com',D1_API_BASE:`http://127.0.0.1:${dbport}`,CP_CLUSTER_ID:'test-cluster-generation',CP_CLUSTER_TOKEN:'test-cluster-token-at-least-32-characters',CP_RAFT_TICK_MS:'100',CP_RAFT_ELECTION_TICKS:'10',CP_RAFT_HEARTBEAT_TICKS:'2'};
const db=new ClusterDatabase(env),proxies=[],ports=[],members=[],children=new Map(),logs=new Map();
let isolated=new Set();
const listen=async server=>{server.listen(0,'127.0.0.1');await once(server,'listening');return server.address().port};
for(let id=1;id<=7;id++){
 const reservation=http.createServer();ports[id]=await listen(reservation);await new Promise(r=>reservation.close(r));
 const proxy=http.createServer((req,res)=>{
  const from=Number(req.headers['x-cp-from']);
  if(isolated.size&&isolated.has(from)!==isolated.has(id)){res.writeHead(503);res.end();return}
  const out=http.request({hostname:'127.0.0.1',port:ports[id],path:'/message',method:'POST',headers:req.headers},reply=>{res.writeHead(reply.statusCode,reply.headers);reply.pipe(res)});out.on('error',()=>{res.writeHead(503);res.end()});req.pipe(out);
 });proxies.push(proxy);members.push({id,url:`http://127.0.0.1:${await listen(proxy)}`,label:`test-${id}`});
}
env.CP_MEMBERS=JSON.stringify(members);
await db.query('INSERT INTO cp_cluster VALUES(1,?,?,?,0)',[env.CP_CLUSTER_ID,7,env.CP_MEMBERS]);
const start=id=>{const p=spawn('/tmp/runners-raft-test',[],{env:{...env,CP_NODE_ID:String(id),CP_RAFT_PORT:String(ports[id])},stdio:['ignore','ignore','pipe']});logs.set(id,'');p.stderr.on('data',b=>logs.set(id,(logs.get(id)+b).slice(-8000)));children.set(id,p);return p};
const request=async(id,path)=>{try{const r=await fetch(`http://127.0.0.1:${ports[id]}${path}`,{headers:{'x-cp-token':env.CP_CLUSTER_TOKEN},signal:AbortSignal.timeout(5000)});return r.ok?await r.json():null}catch{return null}};
const until=async(fn,ms=15000)=>{const end=Date.now()+ms;while(Date.now()<end){const value=await fn();if(value)return value;await sleep(150)}throw new Error('timed out waiting for cluster condition')};
const leaders=async()=>{const rows=await Promise.all([...children.keys()].map(id=>request(id,'/status')));return rows.filter(s=>s?.role==='StateLeader')};
async function stop(id){const p=children.get(id);if(!p)return;children.delete(id);if(p.exitCode!==null)return;p.kill('SIGTERM');await once(p,'exit')}
try{
 for(const id of [1,2,3])start(id);await sleep(2300);
 const initialLeaders=await leaders();
 check('three voters cannot elect a leader in a configured seven-member cluster',()=>assert.equal(initialLeaders.length,0));
 start(4);const first=await until(async()=>{const l=await leaders();return l.length===1&&await request(l[0].id,'/verify')});
 check('four voters elect exactly one leader and confirm authority with a quorum',()=>{assert.ok(first);assert.equal(first.quorum,4)});
 for(const id of [5,6,7])start(id);await until(async()=>{const rows=await Promise.all([5,6,7].map(id=>request(id,'/status')));return rows.every(s=>s?.leader===first.id)});
 const duplicate=spawn('/tmp/runners-raft-test',[],{env:{...env,CP_NODE_ID:'7',CP_RAFT_PORT:'49999'},stdio:'ignore'});const [duplicateCode]=await once(duplicate,'exit');
 check('a second process cannot vote under an already-owned member identity',()=>assert.notEqual(duplicateCode,0));
 isolated=new Set([first.id,...[1,2,3,4,5,6,7].filter(id=>id!==first.id).slice(0,2)]);
 const replacement=await until(async()=>{const ls=await leaders();for(const l of ls)if(!isolated.has(l.id)){const verified=await request(l.id,'/verify');if(verified)return verified}return null});
 const obsolete=await request(first.id,'/verify');
 check('partitioned former leader cannot authorize changes; four-voter side can',()=>{assert.equal(obsolete,null);assert.ok(replacement.term>first.term)});
 isolated=new Set();await until(async()=>{const ls=await leaders();return ls.length===1&&ls[0].id===replacement.id});
 check('healing a partition converges to one primary',()=>assert.ok(true));
 const follower=[1,2,3,4,5,6,7].find(id=>id!==replacement.id);const before=(await request(follower,'/status')).term;
 await stop(follower);start(follower);const after=await until(()=>request(follower,'/status'));
 check('restart reloads durable election state and rejoins as the same voter',()=>assert.ok(after.term>=before));
 const rows=await db.query('SELECT state FROM cp_raft_state');
 check('D1 contains bounded election metadata and no application log payloads',()=>{assert.equal(rows.length,7);for(const r of rows){const s=JSON.parse(r.state);assert.ok(s.tail.length<8);for(const m of s.tail)assert.deepEqual(Object.keys(m).sort(),['index','term']);assert.ok(!('reports'in s))}});
 for(const id of [4,5,6,7])await stop(id);
 await sleep(2600);const results=await Promise.all([1,2,3].map(id=>request(id,'/verify')));
 check('losing four members stops authority rather than shrinking quorum automatically',()=>assert.ok(results.every(r=>r===null)));
 console.log(`${checks} cluster checks passed`);
}catch(error){for(const[id,log]of logs)console.error(`member ${id}: ${log}`);throw error}
finally{for(const id of [...children.keys()])await stop(id);for(const p of proxies)await new Promise(r=>p.close(r));fixture.kill('SIGTERM')}
