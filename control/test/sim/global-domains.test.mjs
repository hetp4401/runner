import assert from 'node:assert/strict';
import {fileURLToPath} from 'node:url';
import {makeControl,Fleet,deploy,advance,MIN,T} from './lib.mjs';
import {loadSystemRouter} from '../../src/system-router.mjs';

const definition=loadSystemRouter(fileURLToPath(new URL('../../../',import.meta.url)));
const c=makeControl({CONTROL_HOST:'dashboard.sim.example'},undefined,{systemRouter:definition});
let checks=0;
const check=(name,fn)=>{fn();checks++;console.log('ok '+name);};
check('the required router exists before any user app or host joins',()=>{
  const router=c.project('global-router');assert.equal(router.enabled,1);
  assert.equal(c.getProject('global-router').replicas,3);
  assert.equal(c.status().projects.length,0);
  assert.equal(c.status().infrastructure.globalRouter.required,true);
});
await assert.rejects(c.join({agent:'early',tag:'hosts'}),/recovering/);
await c.recoverTunnels();
check('startup provisions a dedicated routing tunnel and keeps its token private',()=>{
  assert.ok(c.globalTunnel && c.globalToken);assert.equal(c.tunnelsReady,true);
  assert.ok(!JSON.stringify(c.status()).includes(c.globalToken));
  assert.ok(!JSON.stringify(c.getProject('global-router')).includes(c.globalToken));
  assert.equal(c.redact(c.globalToken),'[hidden]');
});
check('router deployment, scaling, disabling, deletion and environment changes are protected',()=>{
  assert.throws(()=>c.putProject('global-router',{replicas:1},{trusted:true}),/required system/);
  assert.throws(()=>c.setEnabled('global-router',false),/required system/);
  assert.throws(()=>c.deleteProject('global-router'),/required system/);
  assert.throws(()=>c.putEnv('global-router',{TUNNEL_TOKEN:'wrong'}),/required system/);
});
for(const [method,path,body] of [
  ['PUT','', {replicas:1}],['DELETE','',null],['POST','/disable',null],
  ['PUT','/password',{password:'new-password'}],['PUT','/env',{DOMAIN:'elsewhere.test'}],
]) {
  const response=await c.fetch(new Request('https://cp.sim.example/api/projects/global-router'+path,
    {method,headers:{'x-admin-password':'adm','content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})}));
  check(`${method} ${path||'/'} cannot alter required routing even with the admin password`,()=>assert.equal(response.status,403));
}

const fleet=new Fleet(c,{hosts:{size:4,cap:6}},{drainAt:()=>null});
await fleet.startPool('hosts');
deploy(c,'web',{port:8080,replicas:2,compose:'services:\n  app:\n    image: nginx\n    ports: ["8080:80"]\n'});
await fleet.run(15*MIN);c.putSettings({rebalance:false});
check('the required router has three healthy copies spread across hosts',()=>{
  const copies=c.copiesOf('global-router');assert.equal(copies.length,3);
  assert.equal(new Set(copies.map(x=>x.machine)).size,3);
  for(const x of copies) assert.equal(c.liveMachines(T).get(x.machine).status[x.key].s,'healthy');
});
check('legacy generic agents can run system routing and public apps receive both address types',()=>{
  assert.ok([...c.runs.values()].every(r=>r.protocol===3));
  assert.equal(c.getProject('web').globalUrl,'https://web.sim.example/');
  assert.equal(c.getProject('global-router').globalUrl,null);
  assert.deepEqual([...c.dnsTargets(T).keys()].filter(n=>n.startsWith('web')).sort(),['web','web-1','web-2']);
  assert.equal(c.dnsTargets(T).get('web'),c.globalTunnel);
});
check('the global token reaches only agents running the system connector',()=>{
  for(const s of fleet.alive()) for(const [key,plan] of Object.entries(c.desiredFor(s.machine))) {
    if(plan.app==='global-router') assert.ok(plan.files['.env'].includes(c.globalToken));
    else assert.ok(!plan.files['.env'].includes(c.globalToken));
  }
});
check('global routing cannot be toggled in Compose, JSON or partial deploys',()=>{
  for(const shared of [false,true,null]) {
    assert.throws(()=>c.putProject('web',{routing:{shared}},{trusted:true}),/addresses are automatic/);
    assert.throws(()=>c.putProject('option',{port:8080,compose:`x-runner:\n  routing: ${JSON.stringify({shared})}\nservices:\n  app:\n    image: nginx\n`}),/addresses are automatic/);
  }
});
check('background jobs have no URL, control names are reserved and public apps retain 50 replicas',()=>{
  deploy(c,'worker',{compose:'services:\n  worker:\n    image: busybox\n'});
  assert.equal(c.getProject('worker').globalUrl,null);
  for(const name of ['runners','control','dashboard']) assert.throws(()=>c.putProject(name,{dockerfile:'FROM nginx',port:80}),/reserved/);
  assert.equal(c.putProject('large',{dockerfile:'FROM nginx',port:8081,replicas:50}).replicas,50);
  c.deleteProject('large');
});

let records=[
  {id:'legacy',type:'CNAME',name:'web.sim.example',content:c.globalTunnel+'.cfargotunnel.com',comment:'legacy shared gateway',proxied:true},
  {id:'external',type:'CNAME',name:'external.sim.example',content:c.globalTunnel+'.cfargotunnel.com',comment:'another owner'},
],batches=0,serial=0;
c.cf=async(path,init={})=>{
  if(path.endsWith('/dns_records/batch')) {
    const batch=JSON.parse(init.body);batches++;
    for(const row of batch.deletes??[]) records=records.filter(r=>r.id!==row.id);
    for(const row of batch.patches??[]) Object.assign(records.find(r=>r.id===row.id),row);
    for(const row of batch.posts??[]) records.push({id:'new-'+(++serial),...row});
    return {result:{}};
  }
  assert.match(path,/\/dns_records\?/);return {result:structuredClone(records),result_info:{total_pages:1}};
};
const record=name=>records.find(r=>r.name===name+'.sim.example');
await c.syncDns();
check('DNS adopts existing global routes, adds numbered routes and preserves external aliases',()=>{
  assert.equal(record('web').comment,'runner fleet');assert.ok(record('web-1')&&record('web-2'));
  assert.equal(record('external').comment,'another owner');assert.equal(c.dnsNotes.length,0);
});
const writes=batches;await c.syncDns();
check('unchanged app addresses make no duplicate DNS records or writes',()=>assert.equal(batches,writes));
const source=c.copiesOf('web')[0],spare=fleet.alive().find(s=>!c.copiesOf('web').some(x=>x.machine===s.machine));
c.move('web',source.machine,spare.machine,source.replica);await fleet.run(3*MIN);await c.syncDns();
check('moving an app replica retains its global domain and repoints only its numbered route',()=>{
  assert.equal(record('web').content,c.globalTunnel+'.cfargotunnel.com');
  assert.equal(record('web-'+source.replica).content,c.slots.get(spare.machine).tunnel+'.cfargotunnel.com');
});

c.systemRouter={...definition,files:{...definition.files,'router.mjs':definition.files['router.mjs']+'\n// next system release\n'}};
const old=c.project('global-router').version;c.ensureSystemRouter();
check('a bundled router upgrade selects one copy while retaining the previous release on the others',()=>{
  const versions=c.copiesOf('global-router').map(x=>c.desiredFor(x.machine)[x.key].v);
  assert.equal(versions.filter(v=>v===old+1).length,1);assert.equal(versions.filter(v=>v===old).length,2);
});
let minimum=3;
await fleet.run(8*MIN,()=>{
  const healthy=c.copiesOf('global-router').filter(x=>{
    const s=c.liveMachines(T).get(x.machine)?.status[x.key];return s?.s==='healthy'&&s.r!==false;
  }).length;minimum=Math.min(minimum,healthy);
});
check('rolling infrastructure upgrades retain two healthy copies and eventually update all three',()=>{
  assert.ok(minimum>=2);assert.equal(c.project('global-router').stable,old+1);
  for(const x of c.copiesOf('global-router')) assert.equal(c.liveMachines(T).get(x.machine).status[x.key].v,old+1);
});
const systemVersion=c.project('global-router').version;c.ensureSystemRouter();
check('reconciling the required system workload is idempotent',()=>assert.equal(c.project('global-router').version,systemVersion));
 c.mergeEnv('global-router',{TUNNEL_TOKEN:'legacy-router-credential'},T);
 c.ensureSystemRouter();
check('legacy app credentials are removed from durable configuration while remaining redacted in memory',()=>{
  assert.equal(c.all("SELECT value FROM app_env WHERE name='global-router' AND key='TUNNEL_TOKEN'").length,0);
  assert.equal(c.redact('legacy-router-credential'),'[hidden]');
});

const fetchBefore=globalThis.fetch;
let metricReads=0,phase=0;
try {
  globalThis.fetch=async url=>{
    metricReads++;const replica=Number(new URL(url).hostname.match(/global-router-(\d+)/)[1]);
    return Response.json({instance:'instance-'+replica,startedAt:T-10000,requests:100+phase*10,active:replica,
      bytesToClients:10000+phase*1000,failedResponses:0,upstreamFailures:1,failovers:1,streamResumes:1,websockets:0,
      arbitrarySecret:'must-not-leave-the-router'});
  };
  const [one,two]=await Promise.all([c.systemRoutingMetrics(),c.systemRoutingMetrics()]);
  check('routing metrics coalesce requests, aggregate replicas and expose no arbitrary backend fields',()=>{
    assert.equal(metricReads,3);assert.equal(one, two);assert.equal(one.totals.active,6);
    assert.equal(one.totals.requests,300);assert.ok(!JSON.stringify(one).includes('must-not-leave'));
  });
  phase++;advance(6000);const next=await c.systemRoutingMetrics();
  check('routing metrics calculate request and bandwidth rates from each process independently',()=>{
    assert.ok(Math.abs(next.totals.requestsPerSecond-5)<0.0001);
    assert.ok(Math.abs(next.totals.bytesPerSecond-500)<0.0001);
  });
  const response=await c.fetch(new Request('https://cp.sim.example/api/routing/metrics'));
  check('the dedicated routing metrics API serves cached public counters',()=>{
    assert.equal(response.status,200);assert.equal(metricReads,6);
  });
} finally {globalThis.fetch=fetchBefore;}

record('web').content='foreign-tunnel.cfargotunnel.com';await c.syncDns();
check('conflicting foreign DNS is preserved and reported',()=>{
  assert.equal(record('web').content,'foreign-tunnel.cfargotunnel.com');assert.match(c.dnsNotes.join('\n'),/web.sim.example/);
});
record('web').content=c.globalTunnel+'.cfargotunnel.com';
c.copies.clear();c.runs.clear();await c.syncDns();
check('cold inventory loss preserves existing global and numbered records',()=>assert.ok(record('web')&&record('web-1')&&record('web-2')));
c.deleteProject('web');await c.syncDns();
check('app deletion removes its global and numbered records without deleting external aliases',()=>{
  assert.ok(!record('web')&&!record('web-1')&&!record('web-2'));assert.ok(record('external'));
});
c.db.close();console.log(`${checks} required-routing and automatic-domain checks passed`);
