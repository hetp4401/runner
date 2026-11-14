import {DatabaseSync} from 'node:sqlite';
import {writeFile,mkdir} from 'node:fs/promises';
import {Control} from '../../control/src/index.js';
import YAML from '../../control/node_modules/yaml/dist/index.js';
let now=1_800_000_000_000;Date.now=()=>now;
const env={DOMAIN:'test.example',JOIN_TOKEN:'test-join',ADMIN_PASSWORD:'test-admin',STATE_SIGNING_KEY:'fixture-signing-key',DNS:'off',POLL_S:'20',QUIET_MS:'0'};
const db=new DatabaseSync(':memory:');const c=new Control({sql:{exec(q,...args){const rows=db.prepare(q).all(...args);return{toArray:()=>rows}}},env,alarm(){},restart(){},page:''});
const trace=[];const agents=new Map();
function normalized(value){value=structuredClone(value);if(value?.recovery)delete value.recovery;
  for(const key of ['env','keys'])if(Array.isArray(value?.[key]))value[key].sort();
  for(const plan of Object.values(value?.desired||{})){plan.compose=YAML.parse(plan.compose,{merge:true});plan.managedCompose=JSON.parse(plan.managedCompose);delete plan.revision;}
  return value;
}
function state(){return {projects:[...c.projects.values()],copies:Object.fromEntries([...c.copies].filter(([,list])=>list.length).map(([name,list])=>[name,list.map(x=>({...x})).sort((a,b)=>a.replica-b.replica||a.machine-b.machine)])),runs:[...c.runs.values()].map(r=>({id:r.id,machine:r.machine,ready:r.ready,handover:r.handover,retire:r.retire,drain:r.drain||0})).sort((a,b)=>a.id.localeCompare(b.id)),blocked:Object.fromEntries(c.blocked)};}
function step(method,args){const input=structuredClone(args);let result,error;try{result=c[method](...args);}catch(e){error={status:e.status||500};}trace.push({now,method,args:input,expected:error?{error}:{result:normalized(result??null)},state:structuredClone(state())});return result;}
function report(id,patch={}){const agent=agents.get(id);const body={...agent,metrics:{live:{t:now,h:{cpu:10,memUsed:1e9,memTotal:8e9},a:{}},minutes:[]},...patch};const plan=step('sync',[body]);if(plan?.desired&&!plan.hold){agent.status=Object.fromEntries(Object.entries(plan.desired).map(([k,p])=>[k,{v:p.v,s:'healthy',n:0,t0:now}]));agent.inventory=Object.fromEntries(Object.entries(plan.desired).map(([k,p])=>[k,{app:p.app,replica:p.replica,ports:p.ports}]));}return plan;}
for(let machine=1;machine<=4;machine++){const run=`agent-${machine}`;agents.set(run,{run,agent:run,machine,started:now-60*60_000,protocol:5,tag:machine%2?'mac':'fleet',region:machine<=2?'us-west':'us-east',ready:true,starts:true,status:{},inventory:{},cpus:4});report(run);}
step('putProject',['web',{compose:'services:\n  app:\n    image: nginx\n    ports: ["8080:80"]\n',port:8080,replicas:2},{trusted:true}]);
for(const id of agents.keys())report(id);
step('putProject',['worker',{compose:'services:\n  app:\n    image: busybox\n    command: sleep infinity\n',replicas:4},{trusted:true}]);
for(const id of agents.keys())report(id);
now+=20_000;step('move',['web',1,'4',1,{cause:'hand'}]);for(const id of agents.keys())report(id);now+=20_000;for(const id of agents.keys())report(id);
step('putEnv',['web',{TOKEN:'private-fixture',ALPHA:'a$\nb'}]);for(const id of agents.keys())report(id);
step('putProject',['web',{replicas:6},{trusted:true}]);for(let i=0;i<3;i++){now+=20_000;for(const id of agents.keys())report(id);}
step('putProject',['regional',{compose:'x-runner:\n  regions: [us-west, us-east]\n  spread-tags: true\nservices:\n  app:\n    image: busybox\n',replicas:2},{trusted:true}]);for(const id of agents.keys())report(id);
step('setEnabled',['worker',false]);for(const id of agents.keys())report(id);step('setEnabled',['worker',true]);for(const id of agents.keys())report(id);
step('roll',[2]);for(const id of agents.keys())report(id);step('cancelRoll',[]);
step('deleteProject',['regional']);for(const id of agents.keys())report(id);
const output=new URL('../runner-control/tests/fixtures/trace.json',import.meta.url);await mkdir(new URL('.',output),{recursive:true});await writeFile(output,JSON.stringify({env,initial:1_800_000_000_000,trace},null,2)+'\n');console.log(`${trace.length} deployed-controller scheduling steps`);db.close();
