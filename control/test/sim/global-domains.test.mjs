import assert from 'node:assert/strict';
import {makeControl,Fleet,deploy,MIN,T} from './lib.mjs';

const c=makeControl({CONTROL_HOST:'dashboard.sim.example',RECOVER_TUNNELS:'1'});
let checks=0;
const check=(name,fn)=>{fn();checks++;console.log('ok '+name);};
check('a new fleet has no required routing workload or special infrastructure status',()=>{
 assert.equal(c.projects.size,0);assert.equal(c.status().infrastructure,undefined);
});
assert.equal((await c.fetch(new Request('https://cp.sim.example/api/routing/metrics'))).status,404);
await assert.rejects(c.join({agent:'early',tag:'hosts'}),/recovering/);
await c.recoverTunnels();
check('tunnel discovery does not provision a fleet-wide gateway',()=>{assert.equal(c.publicTunnels.size,0);assert.equal(c.projects.size,0);});
const fleet=new Fleet(c,{hosts:{size:4,cap:4}},{drainAt:()=>null});
const body=fleet.body.bind(fleet);fleet.body=(s,extra={})=>({...body(s),protocol:5,...extra});
await fleet.startPool('hosts');
deploy(c,'web',{port:8080,replicas:2,compose:'services:\n  app:\n    image: nginx\n    ports: ["8080:80"]\n'});
deploy(c,'worker',{compose:'services:\n  worker:\n    image: busybox\n'});
await fleet.run(15*MIN);c.putSettings({rebalance:false});await c.ensurePublicTunnels();
const tunnel=c.publicTunnels.get('web');
for(const s of fleet.alive())c.sync(fleet.body(s,{publicTunnels:{web:tunnel.id}}));
check('every public app has global and numbered domains, while background work has no URL',()=>{
 assert.equal(c.getProject('web').globalUrl,'https://web.sim.example/');assert.equal(c.getProject('worker').globalUrl,null);
 assert.deepEqual([...c.dnsTargets(T).keys()].filter(n=>n.startsWith('web')).sort(),['web','web-1','web-2']);
 assert.equal(c.dnsTargets(T).get('web'),tunnel.id);assert.equal(c.publicTunnels.size,1);
});
check('automatic domains cannot be disabled through partial JSON or Compose deployment',()=>{
 for(const shared of [false,true,null]){
  assert.throws(()=>c.putProject('web',{routing:{shared}},{trusted:true}),/addresses are automatic/);
  assert.throws(()=>c.putProject('option',{port:8080,compose:`x-runner:\n  routing: ${JSON.stringify({shared})}\nservices:\n  app:\n    image: nginx\n`}),/addresses are automatic/);
 }
});
check('controller hostnames remain reserved and apps retain the 50-replica limit',()=>{
 for(const name of ['runners','control','dashboard'])assert.throws(()=>c.putProject(name,{dockerfile:'FROM nginx',port:80}),/reserved/);
 assert.equal(c.putProject('large',{dockerfile:'FROM nginx',port:8081,replicas:50}).replicas,50);c.deleteProject('large');
});
let records=[{id:'global',type:'CNAME',name:'web.sim.example',content:tunnel.id+'.cfargotunnel.com',proxied:false},
 {id:'external',type:'CNAME',name:'external.sim.example',content:tunnel.id+'.cfargotunnel.com',comment:'other owner'}],writes=0,serial=0;
c.cf=async(path,init={})=>{
 if(path.endsWith('/dns_records/batch')){
  writes++;const b=JSON.parse(init.body);
  for(const r of b.deletes||[])records=records.filter(x=>x.id!==r.id);
  for(const r of b.patches||[])Object.assign(records.find(x=>x.id===r.id),r);
  for(const r of b.posts||[])records.push({id:'dns-'+(++serial),...r});return {result:{}};
 }
 assert.match(path,/dns_records/);return {result:structuredClone(records),result_info:{total_pages:1}};
};
const record=name=>records.find(r=>r.name===name+'.sim.example');
await c.syncDns();
check('global routes are adopted and proxied without touching outside aliases',()=>{
 assert.equal(record('web').comment,'runner fleet');assert.equal(record('web').proxied,true);
 assert.ok(record('web-1')&&record('web-2'));assert.equal(record('external').comment,'other owner');
});
const before=writes;await c.syncDns();check('unchanged addresses produce no DNS writes',()=>assert.equal(writes,before));
const source=c.copiesOf('web')[0],spare=fleet.alive().find(s=>!c.copiesOf('web').some(x=>x.machine===s.machine));
c.move('web',source.machine,spare.machine,source.replica);await fleet.run(3*MIN);await c.syncDns();
check('a replica move retains global ingress and repoints only its numbered address',()=>{
 assert.equal(record('web').content,tunnel.id+'.cfargotunnel.com');assert.equal(record('web-'+source.replica).content,c.slots.get(spare.machine).tunnel+'.cfargotunnel.com');
});
c.putProject('web',{replicas:1},{trusted:true});await fleet.run(3*MIN);await c.syncDns();
check('scaling down keeps the global URL and removes surplus numbered records',()=>{assert.ok(record('web')&&record('web-1'));assert.ok(!record('web-2'));});
record('web').content='foreign.cfargotunnel.com';await c.syncDns();
check('conflicting outside DNS is preserved and reported',()=>{assert.equal(record('web').content,'foreign.cfargotunnel.com');assert.match(c.dnsNotes.join('\n'),/web.sim.example/);});
record('web').content=tunnel.id+'.cfargotunnel.com';c.copies.clear();c.runs.clear();await c.syncDns();
check('cold inventory loss keeps the shared target and existing numbered records',()=>assert.ok(record('web')&&record('web-1')));

const cold=makeControl({CONTROL_HOST:'dashboard.sim.example',RECOVER_TUNNELS:'1'},c.db),calls=[];
cold.cf=async(path,init={})=>{
 calls.push({path,method:init.method||'GET'});
 if(path.endsWith('/token'))return {result:'rotated-private-app-token'};
 return {result:[{id:tunnel.id,name:cold.publicTunnelName('web'),config_src:'local'}],result_info:{total_pages:1}};
};
await cold.recoverTunnels();
check('cold discovery resolves app keys before releasing plans and makes no provider writes',()=>{
 assert.equal(cold.publicTunnels.get('web').id,tunnel.id);assert.equal(cold.publicTunnels.get('web').token,'rotated-private-app-token');assert.ok(calls.every(r=>r.method==='GET'));
});
await cold.ensurePublicTunnels();
check('restart retrieves the current app token without exposing or persisting it',()=>{
 assert.equal(cold.publicTunnels.get('web').token,'rotated-private-app-token');
 assert.ok(!JSON.stringify(cold.status()).includes('rotated-private-app-token'));assert.ok(!JSON.stringify(cold.getProject('web')).includes('rotated-private-app-token'));
 assert.equal(cold.all('SELECT * FROM app_env').length,0);assert.equal(cold.redact('rotated-private-app-token'),'[hidden]');
});
const blocked=makeControl({RECOVER_TUNNELS:'1'},c.db);
blocked.cf=async(path,init={})=>{if(path.endsWith('/token'))throw Error('provider token lookup failed');return cold.cf(path,init);};
await assert.rejects(blocked.recoverTunnels(),/provider token lookup failed/);
check('a failed app token lookup keeps cold plans on hold without publishing partial credentials',()=>{
 assert.equal(blocked.tunnelsReady,false);assert.equal(blocked.publicTunnels.size,0);
});
c.deleteProject('web');await c.syncDns();
check('deletion removes app addresses while preserving outside aliases',()=>{assert.ok(!record('web')&&!record('web-1'));assert.ok(record('external'));});
c.db.close();console.log(`${checks} automatic public-domain checks passed`);
