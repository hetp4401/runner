import assert from 'node:assert/strict';
import {PublicTunnels,appRoutes,OWNER_LABEL} from '../../agent/runtime.mjs';
import {makeControl,Fleet,deploy,MIN,T} from './sim/lib.mjs';

let checks=0;
const check=(name,fn)=>{fn();console.log('ok '+name);checks++;};
const projects=new Map([['web',{app:'web',replica:1,port:8080,s:'healthy'}],['web-r2',{app:'web',replica:2,port:9080,s:'healthy'}],['worker',{app:'worker',replica:1,s:'healthy'}]]);
check('numbered hosts retain their own port; the global host uses a ready local copy',()=>{
 const routes=appRoutes(projects.values(),'example.test');
 assert.equal(routes.length,3);assert.equal(routes[1].handle[0].upstreams[0].dial,'127.0.0.1:9080');
 assert.equal(routes[2].match[0].host[0],'web.example.test');assert.equal(routes[2].handle[0].upstreams[0].dial,'127.0.0.1:8080');
 projects.get('web').r=false;
 assert.equal(appRoutes(projects.values(),'example.test')[2].handle[0].upstreams[0].dial,'127.0.0.1:9080');
 projects.get('web-r2').busy=true;
 assert.equal(appRoutes(projects.values(),'example.test')[2].handle[0].status_code,503);
 projects.get('web').r=true;projects.get('web-r2').busy=false;
});

let serial=0,healthy=true,wakes=0;const containers=new Map(),calls=[];
const command=async(cmd,args,opts={})=>{
 assert.equal(cmd,'docker');calls.push({args:[...args],hasToken:Boolean(opts.extraEnv?.TUNNEL_TOKEN)});
 const ok=out=>({ok:true,out}),missing=()=>({ok:false,out:'Error: No such container'});
 const byId=id=>[...containers.values()].find(c=>c.Id===id||c.Name===id);
 if(args[0]==='ps')return ok([...containers.values()].filter(c=>c.Config.Labels[OWNER_LABEL]==='owner'&&c.Config.Labels['io.runners.component']==='public-tunnel').map(c=>c.Id).join('\n'));
 if(args[0]==='inspect'){const cs=args.slice(1).map(byId);return cs.every(Boolean)?ok(JSON.stringify(cs)):missing();}
 if(args[0]==='logs')return ok('Registered tunnel connection');
 if(args[0]==='run'){
  const labels={};for(let i=0;i<args.length;i++)if(args[i]==='--label'){const [k,...v]=args[++i].split('=');labels[k]=v.join('=');}
  const name=args[args.indexOf('--name')+1], id='container-'+(++serial);
  assert.equal(args.includes('test-token'),false,'secret must travel via environment, never argv');
  assert.equal(opts.extraEnv.TUNNEL_TOKEN,'test-token');
  containers.set(name,{Id:id,Name:name,Config:{Labels:labels},State:{Running:true,StartedAt:String(serial)}});return ok(id);
 }
 const c=byId(args.at(-1));if(!c)return missing();
 if(args[0]==='stop'){c.State.Running=false;return ok(c.Id);}
 if(args[0]==='start'){c.State={Running:true,StartedAt:'restart-'+(++serial)};return ok(c.Id);}
 if(args[0]==='rm'){containers.delete(c.Name);return ok(c.Id);}
 throw Error('unexpected Docker command '+args.join(' '));
};
const options={owner:'owner',command,configFile:'/private/tunnel.yml',probe:async()=>({code:healthy?200:502,routed:true}),changed:()=>wakes++};
const manager=new PublicTunnels(options),auth={web:{id:'app-tunnel',token:'test-token'}};
await manager.reconcile(auth,projects,{open:false});
check('a held successor cannot open a public connector',()=>assert.equal(containers.size,0));
await manager.reconcile(auth,projects);
check('one connector serves colocated replicas and keeps credentials out of command arguments',()=>{
 assert.equal(containers.size,1);assert.deepEqual(manager.active,{web:'app-tunnel'});assert.ok(wakes);
});
const created=serial;await manager.reconcile(auth,projects);
check('unchanged plans preserve the live connector and its connections',()=>assert.equal(serial,created));
healthy=false;await manager.reconcile(auth,projects);assert.equal([...containers.values()][0].State.Running,true);
await manager.reconcile(auth,projects);
check('two failed probes withdraw the connector without stopping the app',()=>{assert.equal([...containers.values()][0].State.Running,false);assert.deepEqual(manager.active,{});assert.equal(projects.get('web').s,'healthy');});
healthy=true;await manager.reconcile(auth,projects);
check('local recovery reconnects the same owned container',()=>{assert.equal(containers.size,1);assert.equal([...containers.values()][0].State.Running,true);assert.deepEqual(manager.active,{web:'app-tunnel'});});
const restarted=new PublicTunnels(options);await restarted.reconcile(auth,projects);
check('an agent restart adopts healthy existing connectors',()=>assert.equal(containers.size,1));
containers.set('foreign',{Id:'foreign',Name:'foreign',Config:{Labels:{[OWNER_LABEL]:'another-fleet','io.runners.component':'public-tunnel'}},State:{Running:true}});
await restarted.reconcile({},projects);
check('revocation removes only this fleet’s connector',()=>{assert.equal(containers.size,1);assert.ok(containers.has('foreign'));});
containers.set(manager.name('web'),{Id:'collision',Name:manager.name('web'),Config:{Labels:{}},State:{Running:true}});
await assert.rejects(manager.reconcile(auth,projects),/another container/);
check('a foreign container with the desired name is never adopted or removed',()=>assert.ok(containers.has(manager.name('web'))));
containers.delete(manager.name('web'));
let release;const deferred=new Promise(r=>release=r),queued=new PublicTunnels({...options,probe:async()=>{await deferred;return {code:200,routed:true};}});
const pending=queued.reconcile(auth,projects),withdraw=queued.withdraw();release();await Promise.all([pending,withdraw]);
check('withdrawal queued during an in-flight check completes before returning',()=>{assert.deepEqual(queued.active,{});assert.equal(containers.size,1);});

const c=makeControl(),fleet=new Fleet(c,{hosts:{size:3,cap:3}},{drainAt:()=>null});
const body=fleet.body.bind(fleet);fleet.body=(s,extra={})=>({...body(s),protocol:5,...extra});
await fleet.startPool('hosts');deploy(c,'web',{port:8080,replicas:3,compose:'services:\n  web:\n    image: nginx\n    ports: ["8080:80"]\n'});
await fleet.run(15*MIN);c.putSettings({rebalance:false});await c.ensurePublicTunnels();
const t=c.publicTunnels.get('web');assert.ok(t.id&&t.token);
check('all app hosts receive the same private token independently of workload revisions',()=>{
 const revisions=fleet.alive().map(s=>c.desiredFor(s.machine).web.revision);
 for(const s of fleet.alive()){
  const plan=c.sync(fleet.body(s));assert.deepEqual(plan.publicTunnels.web,{id:t.id,token:t.token});
  assert.equal(JSON.stringify(plan.desired).includes(t.token),false);
 }
 assert.deepEqual(fleet.alive().map(s=>c.desiredFor(s.machine).web.revision),revisions);
 assert.equal(JSON.stringify(c.status()).includes(t.token),false);assert.equal(c.redact(t.token),'[hidden]');
});
const s=fleet.alive()[0];c.sync(fleet.body(s,{publicTunnels:{web:t.id,unplaced:'wrong'}}));
check('connector reports expose only owned identifiers, never credentials',()=>assert.deepEqual(c.runs.get(s.run).publicTunnels,{web:t.id}));
check('a legacy agent receives no app tunnel credentials',()=>assert.equal(c.sync(fleet.body(s,{protocol:3})).publicTunnels,undefined));
c.sync(fleet.body(s));
const target=c.dnsTargets(T).get('web');fleet.stop(s,'test host loss');await fleet.run(MIN);
check('host loss preserves the shared tunnel identity and numbered replica routes',()=>{
 assert.equal(c.dnsTargets(T).get('web'),target);assert.ok(c.dnsTargets(T).get('web-2'));assert.ok(c.dnsTargets(T).get('web-3'));
});
check('connector authorization stays below the provider limit during large deployments',()=>{
 const fake=Array.from({length:50},(_,i)=>({machine:i+1,replica:i+1}));
 const copies=c.copiesOf,live=c.liveMachines;c.copiesOf=()=>fake;c.liveMachines=()=>new Map(fake.map(x=>[x.machine,{protocol:5}]));
 assert.equal(c.publicHosts('web').size,20);c.copiesOf=copies;c.liveMachines=live;
});
let records=[{id:'global',type:'CNAME',name:'web.sim.example',content:'old-router.cfargotunnel.com',proxied:true,comment:'runner fleet'},
 {id:'external',type:'CNAME',name:'external.sim.example',content:t.id+'.cfargotunnel.com',proxied:true,comment:'external owner'}];
let dnsWrites=0,deletes=0,denyDelete=true;
c.cf=async(path,init={})=>{
 if(path.includes('/dns_records/batch')){
  dnsWrites++;const batch=JSON.parse(init.body);
  for(const r of batch.deletes||[])records=records.filter(x=>x.id!==r.id);
  for(const r of batch.patches||[])Object.assign(records.find(x=>x.id===r.id),r);
  for(const r of batch.posts||[])records.push({id:'dns-'+(++serial),...r});
  return {result:{}};
 }
 if(init.method==='DELETE') {if(denyDelete)throw Error('connector still active');deletes++;return {result:{}};}
 assert.match(path,/dns_records/);return {result:structuredClone(records),result_info:{total_pages:1}};
};
c.globalTunnel='old-router';
await c.syncDns();
check('cutover adopts the existing global domain while preserving external aliases',()=>{
 const r=records.find(x=>x.name==='web.sim.example');assert.equal(r.content,t.id+'.cfargotunnel.com');assert.equal(r.comment,'runner fleet');
 assert.ok(records.some(x=>x.id==='external'));assert.equal(c.dnsNotes.length,0);
});
const writes=dnsWrites;await c.syncDns();
check('DNS reconciliation is idempotent',()=>assert.equal(dnsWrites,writes));
c.deleteProject('web');await c.syncDns();
check('deleting an app revokes its connectors and removes its addresses while preserving outside aliases',()=>{
 assert.deepEqual(c.publicTunnelsFor(fleet.alive()[0],{}),{});
 assert.ok(!records.some(x=>x.name.startsWith('web.')));assert.ok(!records.some(x=>x.name.startsWith('web-')));
 assert.ok(records.some(x=>x.id==='external'));assert.equal(deletes,0);
});
records=records.filter(x=>x.id!=='external');await c.syncDns();
check('a still-connected tunnel is retained for retry rather than forcefully deleted',()=>{assert.ok(c.publicTunnelIds.has(t.id));assert.ok(c.publicCleanupDue>T);});
denyDelete=false;await c.syncDns();
check('an unused tunnel is collected once its connectors have withdrawn',()=>{assert.equal(deletes,1);assert.equal(c.publicTunnels.size,0);});
c.db.close();console.log(`${checks} direct public tunnel checks passed`);
