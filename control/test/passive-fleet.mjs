import assert from 'node:assert/strict';
import {createHmac} from 'node:crypto';
import {mkdtempSync,existsSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import http from 'node:http';
import {Control} from '../src/index.js';
import {LocalStore} from '../src/local-store.mjs';
import {resolveServerTag} from '../../agent/runtime.mjs';

const directory=mkdtempSync(join(tmpdir(),'runner-passive-'));
const env={DOMAIN:'example.test',JOIN_TOKEN:'test-join',ADMIN_PASSWORD:'test-owner',STATE_SIGNING_KEY:'test-receipt-key',
  POOLS:JSON.stringify({rafael:18,yohan:18,github:1}),DNS:'off',QUIET_MS:'0',YOUNG_MS:'0'};
let sequence=0,checks=0;
const opened=[];
function make(filename=join(directory,`${++sequence}.db`),extra={}) {
  const store=new LocalStore(filename,{initialize:!existsSync(filename)});
  const control=new Control({sql:store,env:{...env,...extra},page:readFileSync(new URL('../src/app.html',import.meta.url),'utf8'),alarm(){},restart(){}});
  store.protect(control);
  control.tunnelFor=async n=>({token:`test-tunnel-${n}`});
  control.settleAt=Date.now()-3600_000;control.lastCheckin=Date.now();
  const close=()=>store.close();opened.push(close);
  return {control,store,filename,close};
}
async function api(control,path,{method='GET',body,node=false,admin=false}={}) {
  const response=await control.fetch(new Request('https://cp.example.test'+(node?'/api':'/admin/api')+path,{method,
    headers:{'content-type':'application/json',...(node?{authorization:'Bearer test-join'}:{}),...(admin?{'x-admin-password':'test-owner'}:{})},
    body:body===undefined?undefined:JSON.stringify(body)}));
  return {status:response.status,body:await response.json()};
}
async function add(control,tag,id=`agent-${++sequence}`) {
  const joined=await control.join({agent:id,tag});
  const report={machine:joined.machine,agent:id,run:id+'-run',started:Date.now()-60000,tag,
    protocol:3,ready:true,starts:false,status:{},inventory:{}};
  const plan=control.sync(report);
  return {joined,report,plan};
}
function check(name,fn){return Promise.resolve().then(fn).then(()=>{checks++;console.log('PASS:',name);});}

async function serve() {
  const {control}=make();
  control.putSettings({rebalance:false});
  control.putProject('web',{port:8080,replicas:1,compose:'services:\n  web:\n    image: nginx\n    ports: ["8080:80"]\n'},{trusted:true});
  const a=await add(control,'github','browser-github');
  a.report.status={web:{v:1,s:'healthy'}};control.sync(a.report);
  const b=await add(control,'standalone','browser-standalone');
  setInterval(()=>{control.sync(a.report);control.sync(b.report);},1000).unref();
  const server=http.createServer(async(req,res)=>{
    try{
      let text='';for await(const part of req)text+=part;
      const response=await control.fetch(new Request(`http://127.0.0.1:${server.address().port}${req.url}`,{method:req.method,
        headers:req.headers,body:['GET','HEAD'].includes(req.method)?undefined:text}));
      res.writeHead(response.status,Object.fromEntries(response.headers));res.end(Buffer.from(await response.arrayBuffer()));
    }catch(error){res.writeHead(500,{'content-type':'application/json'});res.end(JSON.stringify({error:error.message}));}
  });
  await new Promise(done=>server.listen(0,'127.0.0.1',done));
  console.log(JSON.stringify({url:`http://127.0.0.1:${server.address().port}`}));
  process.on('SIGTERM',()=>{server.close();for(const close of opened)try{close();}catch{}rmSync(directory,{recursive:true,force:true});process.exit(0);});
}
async function tests() {
  const fixture=make(),{control}=fixture;
  await check('an empty fleet has no expected count or configured missing groups',()=>{
    const state=control.status();assert.equal(state.fleetMode,'passive');assert.equal(state.servers,0);
    assert.deepEqual(state.groups,[]);assert.equal('expected' in state,false);assert.equal('pools' in state,false);
  });
  await check('unknown agents without a tag are rejected before receiving a slot',async()=>{
    const response=await api(control,'/join',{method:'POST',node:true,body:{agent:'untagged'}});
    assert.equal(response.status,400);assert.equal(control.agents.size,0);
    assert.throws(()=>control.sync({machine:1,agent:'untagged',run:'untagged-run',started:Date.now(),ready:true}),/tag is required/);
    assert.equal(control.runs.size,0);
  });
  await check('invalid tags are rejected',async()=>{
    for(const tag of ['',null,'Bad Tag','-bad','a'.repeat(31),{}]) {
      const result=await api(control,'/join',{method:'POST',node:true,body:{agent:'bad',tag}});
      assert.equal(result.status,400,JSON.stringify(tag));
    }
  });
  const standalone=await add(control,'standalone','independent');
  await check('standalone is an explicit tag, not an omitted value',()=>{
    assert.equal(standalone.joined.tag,'standalone');assert.equal(control.status().runs[0].tag,'standalone');
  });
  await check('legacy POOL values migrate to tags without importing their sizes',async()=>{
    const joined=await control.join({agent:'legacy-group',pool:'github',poolSize:999});
    const plan=control.sync({machine:joined.machine,agent:'legacy-group',run:'legacy-run',started:Date.now()-60000,protocol:3,
      pool:'github',poolSize:999,ready:true,starts:true,status:{}});
    assert.deepEqual(plan.start,[]);assert.equal(control.settings.has('pool_asks'),false);
    assert.equal(control.status().runs.find(r=>r.id==='legacy-run').tag,'github');
  });
  await check('legacy claim requests cannot restore server targets or create starts',async()=>{
    const response=await api(control,'/claim?pool=rafael&size=18',{method:'POST',node:true});
    assert.deepEqual(response.body,{start:[],mode:'passive'});assert.equal(control.starts.size,0);
    assert.equal(control.groups().some(g=>g.tag==='rafael'),false);
  });
  await check('fixed server settings are rejected atomically',async()=>{
    const before=control.rebalanceOn();
    const response=await api(control,'/settings',{method:'PUT',admin:true,body:{rebalance:!before,pools:{github:1}}});
    assert.equal(response.status,400);assert.equal(control.rebalanceOn(),before);
  });
  await check('additional servers are accepted without any surplus retirement',async()=>{
    for(let i=0;i<8;i++) {
      const {plan}=await add(control,'github',`extra-${i}`);
      assert.equal(plan.retire,false);assert.deepEqual(plan.start,[]);assert.equal(plan.handover,false);
    }
    control.spreadDepartures(Date.now());
    assert.equal([...control.runs.values()].some(r=>r.evict||r.retire),false);
    assert.equal(control.groups().find(g=>g.tag==='github').online,9);
  });
  await check('a tag alone creates no lifetime or replacement expectation',()=>{
    const r=[...control.runs.values()].find(r=>r.pool==='github');
    assert.equal(control.dueAt(r),null);assert.equal(control.heldBy(r.machine,Date.now()),null);
  });
  await check('an already known standalone agent can resume during migration',()=>{
    const legacy={...standalone.report};delete legacy.tag;
    assert.doesNotThrow(()=>control.sync(legacy));
    assert.equal(control.runs.get(legacy.run).pool,null);
  });
  await check('fresh agents require SERVER_TAG while persisted legacy identity can migrate',()=>{
    assert.throws(()=>resolveServerTag({CONTROL_URL:'https://cp.example'}),/SERVER_TAG is required/);
    assert.throws(()=>resolveServerTag({SERVER_TAG:''}),/SERVER_TAG is required/);
    assert.equal(resolveServerTag({SERVER_TAG:'standalone'}),'standalone');
    assert.equal(resolveServerTag({POOL:'github',POOL_SIZE:'99'}),'github');
    assert.equal(resolveServerTag({CONTROL_URL:'https://cp.example'},{control:'https://cp.example',run:'existing'}),'standalone');
    assert.equal(resolveServerTag({},{tag:'school',run:'existing'}),'school');
  });
  await check('old size settings are backed up and removed from durable configuration',()=>{
    const legacy=make();
    legacy.store.db.prepare("INSERT INTO main.settings(key,value) VALUES('pools',?)").run(JSON.stringify({rafael:18,yohan:18}));
    legacy.close();
    const restored=make(legacy.filename);
    assert.ok(restored.store.backupPath&&existsSync(restored.store.backupPath));
    assert.equal(restored.store.db.prepare("SELECT 1 FROM main.settings WHERE key='pools'").get(),undefined);
    assert.deepEqual(restored.control.status().groups,[]);
  });
  await check('old signed receipts cannot restore sizes or fixed-count start requests',()=>{
    const receipt=control.recoveryFor(control.runs.get(standalone.report.run),Date.now());
    const data=JSON.parse(receipt.payload);data.runtime.push({key:'pools',value:'{"rafael":18}'},{key:'pool_asks',value:'{"yohan":18}'});
    data.starts=[{machine:50,at:Date.now(),by:'claim'}];
    if(data.fleet)data.fleet.starts=data.starts;
    const payload=JSON.stringify(data),signature=createHmac('sha256',env.STATE_SIGNING_KEY).update(payload).digest('hex');
    const recovered=make(undefined,{RECOVER_RUNTIME:'1',RECOVERY_MS:'60000'});
    recovered.control.sync({...standalone.report,recovery:{payload,signature}});
    assert.equal(recovered.control.settings.has('pools'),false);assert.equal(recovered.control.settings.has('pool_asks'),false);
    assert.equal(recovered.control.starts.size,0);
  });
  console.log(`${checks} passive fleet and mandatory tag regressions passed.`);
}
if(process.argv[2]==='serve')await serve();
else try{await tests();}finally{for(const close of opened)try{close();}catch{}rmSync(directory,{recursive:true,force:true});}
