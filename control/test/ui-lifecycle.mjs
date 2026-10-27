// Run the shipped inline application, with controlled asynchronous responses.
// No service or production changes are made by these regressions.
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { setImmediate as tick } from 'node:timers/promises';

const html = readFileSync(new URL('../src/app.html', import.meta.url), 'utf8');
const script = html.split('<script>')[1].split('</script>')[0];
const startup = script.lastIndexOf('\nroute();\nloadStatus();');
assert.ok(startup > 0);
const now = 1_800_000_000_000;
const project = { name:'web', version:2, enabled:true, replicas:1, staying:1, files:[], port:8080,
  updated:now-10000, placed:[{machine:1,replica:1,key:'web'}], hasPassword:false };
const base = { now, domain:'example.test', projects:[project], slots:[1], runs:[{id:'new-run',machine:1,
  started:now-10000,seen:now,live:true,ready:true,retiring:false,projects:{web:{s:'healthy',v:2}}}],
  pools:[{name:'pool',size:1,live:1}], rebalance:{hot:[],state:'on'}, departures:{list:[]},
  dark:[], alerts:[], starting:[], settling:false };

function harness(status = structuredClone(base)) {
  const nodes = new Map();
  const node = (id) => {
    const e = { innerHTML:'',textContent:'',value:'',className:'hint',style:{},open:false,
      contains:()=>false,setAttribute(){},addEventListener(){},focus(){},scrollIntoView(){},
      querySelector:q=>nodes.get(q)??null,querySelectorAll:()=>[],
      showModal(){this.open=true;},close(){this.open=false;queueMicrotask(()=>this.onclose?.());} };
    nodes.set(id,e); return e;
  };
  for (const id of ['#main','#sheet','form','.err','[type=submit]','#ad-head','#ad-status','#ad-replicas',
    '#ad-versions','#ad-logs','#ad-spec','#ad-code','#ad-version','#ed-form','#ed-up','#ed-updir',
    '#ed-compose','#ed-dockerfile','#ed-files','#ed-port','#ed-replicas','#ed-name','#ed-name-hint']) node(id);
  const context = vm.createContext({ console,URLSearchParams,Request,Response,AbortController,
    setTimeout,clearTimeout,queueMicrotask,innerWidth:1365,addEventListener(){},
    localStorage:{getItem:()=>null,setItem(){}},location:{hash:'#/',search:''},
    getSelection:()=>null,document:{querySelector:q=>nodes.get(q)??null,querySelectorAll:()=>[],addEventListener(){},activeElement:null},
    hooks:{}, fixture:status });
  const run = s => vm.runInContext(s,context);
  run(script.slice(0,startup));
  run('S.status=fixture; getSpec=(...args)=>hooks.spec(...args); api=(...args)=>hooks.api(...args);');
  return {context,run,nodes,node,status};
}
function deferred() { let resolve,reject; const promise=new Promise((a,b)=>{resolve=a;reject=b;}); return {promise,resolve,reject}; }
const spec = v => ({name:'web',version:v,compose:`version ${v}`,files:{},port:8080,
  versions:[1,2,3].map(version=>({version,port:8080,replicas:1,created:now}))});
let passed=0;
async function check(name,fn) { await fn(); console.log('PASS:',name); passed++; }

await check('recovered incidents remain in history without counting as current problems',()=>{
  const h=harness();
  h.status.alerts=[{cause:'fails-everywhere',app:'web',t:now-20000,text:'web version 2 failed'},
    {cause:'grace',machine:1,t:now-20000,text:'old run stopped'},
    {cause:'departure-waiting',machine:1,t:now-20000,text:'old run waiting'}];
  assert.equal(h.run('problems().length'),0);
  assert.equal(h.status.alerts.length,3);
});
await check('a current app failure stays visible and an older version alert does not recur',()=>{
  const h=harness(); h.status.runs[0].projects.web.s='failed';
  h.status.alerts=[{cause:'fails-everywhere',app:'web',t:now-1000,text:'web version 1 failed'}];
  const current=h.run('problems().map(p=>p.text).join(" | ")');
  assert.match(current,/web: failing/); assert.doesNotMatch(current,/version 1 failed/);
  h.status.alerts[0].text='web version 2 failed';
  assert.match(h.run('problems().map(p=>p.text).join(" | ")'),/version 2 failed/);
});
await check('departure alerts clear when handover starts and do not transfer to a replacement run',()=>{
  const h=harness(); h.status.alerts=[{cause:'departure-waiting',machine:1,t:now-1000,text:'waiting for budget'}];
  h.status.departures.list=[{machine:1,due:true,wait:'budget',leaving:null}];
  assert.match(h.run('problems().map(p=>p.text).join(" | ")'),/waiting for budget/);
  h.status.departures.list[0].leaving='handover'; assert.equal(h.run('problems().length'),0);
  h.status.departures.list[0].leaving=null; h.status.runs[0].started=now;
  assert.equal(h.run('problems().length'),0);
});
await check('real missing capacity names its pools and remains visible',()=>{
  const h=harness(); h.status.pools.push({name:'missing-pool',size:18,live:0});
  assert.match(h.run('problems().map(p=>p.text).join(" | ")'),/18 servers missing \(missing-pool: 0\/18 online\)/);
});
await check('a late app response cannot overwrite the next page',async()=>{
  const h=harness(), d=deferred(); h.context.hooks.spec=()=>d.promise;
  h.run('view=vApp("web"); view.status(); view.unmount(); view=null;');
  h.nodes.get('#ad-spec').innerHTML='next page'; d.resolve(spec(2)); await tick();
  assert.equal(h.nodes.get('#ad-spec').innerHTML,'next page');
});
await check('rapid version selections keep the most recently selected version',async()=>{
  const h=harness(), old=deferred(); h.context.hooks.spec=(_name,v)=>v===1?old.promise:Promise.resolve(spec(v));
  h.run('view=vApp("web"); view.status();'); await tick();
  const click=v=>h.run('view').click({target:{closest:()=>({dataset:{act:'view',v:String(v)}})}});
  const first=click(1); await click(3); old.resolve(spec(1)); await first; await tick();
  assert.equal(h.nodes.get('#ad-code').textContent,'version 3');
});
await check('a late editor load cannot fill a different editor',async()=>{
  const h=harness(), d=deferred(); h.context.hooks.api=()=>d.promise;
  const editor=h.run('vEditor("web")'); const mounting=editor.mount(); editor.unmount();
  h.nodes.get('#ed-compose').value='new unsaved draft'; d.resolve(spec(2)); await mounting;
  assert.equal(h.nodes.get('#ed-compose').value,'new unsaved draft');
});
await check('navigation cancels a password sheet before another sheet opens',async()=>{
  const h=harness(); const first=h.run('sheet({title:"First",html:""})');
  const second=h.run('sheet({title:"Second",html:""})'); assert.equal(await first,null);
  h.run('cancelSheet()'); assert.equal(await second,null);
});
function logHarness() {
  const h=harness(), sockets=[];
  h.node('#ad-logbar'); Object.assign(h.node('#ad-logtext'),{scrollTop:0,scrollHeight:100,clientHeight:100});
  h.context.WebSocket=class {constructor(url){this.url=url;sockets.push(this);}close(){this.closed=true;}};
  h.context.hooks.spec=(_name,v)=>Promise.resolve(spec(v));
  h.run('withPassword=async(_options,fn)=>fn({admin:"test-only"});view=vApp("web");view.status();');
  const click=act=>h.run('view').click({target:{closest:()=>({dataset:{act}})}});
  return {...h,sockets,click};
}
await check('leaving during log authentication never opens a background stream',async()=>{
  const h=logHarness(),d=deferred(); h.context.hooks.api=()=>d.promise;
  const opening=h.click('logs-open'); h.run('view.unmount()');
  d.resolve({session:'late-session'}); await opening; await tick();
  assert.equal(h.sockets.length,0);
});
await check('closing logs cancels an in-flight resume instead of reopening the socket',async()=>{
  const h=logHarness(); h.context.hooks.api=()=>Promise.resolve({session:'first-session'});
  await h.click('logs-open'); assert.equal(h.sockets.length,1);
  await h.click('logs-pause'); assert.equal(h.sockets[0].closed,true);
  const d=deferred();h.context.hooks.api=()=>d.promise;
  const resuming=h.click('logs-pause');await h.click('logs-close');
  d.resolve({session:'cancelled-resume'});await resuming;await tick();
  assert.equal(h.sockets.length,1);
});
console.log(`${passed} UI lifecycle and current-health regressions passed.`);
