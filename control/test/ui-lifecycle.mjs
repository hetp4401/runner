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
    setTimeout,clearTimeout,setInterval,clearInterval,queueMicrotask,innerWidth:1365,addEventListener(){},
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

function overviewHarness(status = structuredClone(base)) {
  const h=harness(status);
  for(const id of ['#ov-sub','#ov-summary','#ov-problems','#ov-apps','#ov-apps-meta',
    '#ov-machines','#ov-machines-meta','#ov-activity','#ov-activity-meta']) h.node(id);
  h.run('S.statusAt=fixture.now; view=vOverview();');
  return h;
}
function appFixture(name,state='live',machine=1) {
  return {...structuredClone(project),name,enabled:state!=='disabled',
    placed:[{machine,replica:1,key:name}]};
}
function appsHarness() {
  const status=structuredClone(base);
  status.projects=['live','deploying','failing','disabled'].map(state=>appFixture(`web-${state}`,state));
  status.runs[0].projects=Object.fromEntries(status.projects.map(p=>[p.name,
    {v:2,s:p.name==='web-failing'?'failed':p.name==='web-deploying'?'starting':'healthy',e:'Test deployment failed'}]));
  const h=harness(status);
  for(const id of ['#apps-filter input','#apps-count','#apps-list'])h.node(id);
  for(const key of ['all','attention','live','disabled']){
    h.node(`[data-app-count="${key}"]`);h.node(`[data-app-filter="${key}"]`);
  }
  h.run('view=vApps(); view.mount(); view.status();');
  return h;
}
const shownApps=h=>[...new Set([...h.nodes.get('#apps-list').innerHTML.matchAll(/href="#\/apps\/([^"/?]+)"/g)].map(x=>x[1]))];
const shownServers=h=>[...h.nodes.get('#mc-list').innerHTML.matchAll(/href="#\/machines\/(\d+)"/g)].map(x=>Number(x[1]));
function chooseFilter(h,kind,value) {
  h.run('view').click({target:{closest:()=>({dataset:{[kind==='app'?'appFilter':'serverFilter']:value},hasAttribute:()=>false})}});
}
function clearFilters(h,kind) {
  h.run('view').click({target:{closest:()=>({dataset:{},hasAttribute:name=>name===`data-${kind}-clear`})}});
}

await check('overview replica totals exclude disabled apps and report only ready servers',()=>{
  const status=structuredClone(base);
  status.projects.push({...appFixture('paused','disabled'),replicas:20,placed:[]});
  status.slots.push(2);status.runs.push({...structuredClone(status.runs[0]),id:'starting',machine:2,ready:false,projects:{}});
  const h=overviewHarness(status);h.run('view.status();');
  const summary=h.nodes.get('#ov-summary').innerHTML;
  assert.match(summary,/Servers online<\/span><strong[^>]*>1<\/strong>/);
  assert.match(summary,/Apps running<\/span><strong[^>]*>1 \/ 1<\/strong>/);
  assert.match(summary,/Replicas ready<\/span><strong[^>]*>1 \/ 1<\/strong>/);
  assert.match(summary,/1 disabled · 2 total/);
});
await check('overview previews stay bounded and surface an app failure before healthy apps',()=>{
  const status=structuredClone(base);
  status.projects=Array.from({length:12},(_,i)=>appFixture(`app-${String(i).padStart(2,'0')}`));
  status.projects.push(appFixture('zzz-failing'));
  status.runs[0].projects=Object.fromEntries(status.projects.map(p=>[p.name,{v:2,s:p.name==='zzz-failing'?'failed':'healthy'}]));
  status.slots=Array.from({length:12},(_,i)=>i+1);
  status.runs.push(...status.slots.slice(1).map(machine=>({...structuredClone(base.runs[0]),id:`run-${machine}`,machine,projects:{}})));
  status.events=[{t:now,kind:'dns',text:'Quiet DNS event'},...Array.from({length:10},(_,i)=>({t:now-i*1000,kind:'move-start',text:`Move event ${i}`}))];
  const h=overviewHarness(status);h.run('view.status();');
  const apps=[...h.nodes.get('#ov-apps').innerHTML.matchAll(/href="#\/apps\/([^"/?]+)"/g)].map(x=>x[1]);
  assert.equal(apps.length,6);assert.equal(apps[0],'zzz-failing');
  assert.equal([...h.nodes.get('#ov-machines').innerHTML.matchAll(/href="#\/machines\/\d+"/g)].length,6);
  const events=h.nodes.get('#ov-activity').innerHTML;
  assert.equal([...events.matchAll(/Move event \d+/g)].length,5);
  assert.doesNotMatch(events,/Quiet DNS event/);
  assert.match(h.nodes.get('#ov-problems').innerHTML,/zzz-failing: failing/);
});
await check('an empty overview offers deployment and server entry points without inventing activity',()=>{
  const status={...structuredClone(base),projects:[],slots:[],runs:[],events:[]};
  const h=overviewHarness(status);h.run('view.status();');
  assert.match(h.nodes.get('#ov-apps').innerHTML,/href="#\/apps\/new"/);
  assert.match(h.nodes.get('#ov-machines').innerHTML,/href="#\/contribute"/);
  assert.match(h.nodes.get('#ov-activity').innerHTML,/No recent activity/);
  h.run('S.statusErr="offline";view.status();');
  assert.match(h.nodes.get('#ov-problems').innerHTML,/last received fleet status/);
  assert.doesNotMatch(h.nodes.get('#ov-problems').innerHTML,/No active issues reported/);
});
await check('app filters distinguish healthy, changing, failed, and disabled deployments',()=>{
  const h=appsHarness();assert.equal(shownApps(h).length,4);
  chooseFilter(h,'app','attention');assert.deepEqual(shownApps(h),['web-deploying','web-failing']);
  chooseFilter(h,'app','live');assert.deepEqual(shownApps(h),['web-live']);
  chooseFilter(h,'app','disabled');assert.deepEqual(shownApps(h),['web-disabled']);
  chooseFilter(h,'app','all');assert.equal(shownApps(h).length,4);
  assert.equal(h.nodes.get('[data-app-count="attention"]').textContent,2);
});
await check('app search intersects status filters and Clear filters restores all apps',()=>{
  const h=appsHarness(),search=h.nodes.get('#apps-filter input');
  chooseFilter(h,'app','attention');search.value='  WEB-FAIL  ';search.oninput({target:search});
  assert.deepEqual(shownApps(h),['web-failing']);
  chooseFilter(h,'app','live');assert.deepEqual(shownApps(h),[]);
  assert.match(h.nodes.get('#apps-list').innerHTML,/No matching apps/);
  clearFilters(h,'app');assert.equal(shownApps(h).length,4);assert.equal(search.value,'');
  h.status.projects=[];h.run('view.status();');
  assert.match(h.nodes.get('#apps-list').innerHTML,/Deploy your first app/);
  assert.doesNotMatch(h.nodes.get('#apps-list').innerHTML,/No matching apps/);
});

function serversHarness() {
  const status=structuredClone(base);
  status.slots=[1,2,3,4,5];
  status.runs=status.slots.map(machine=>({...structuredClone(base.runs[0]),id:`run-${machine}`,machine,projects:{},
    label:machine===1?'Maple Lab':`Host ${machine}`,tag:machine===2?'Edge-Blue':'pool',region:machine===3?'eu-west':'us-east'}));
  status.runs[1].ready=false;
  status.runs[2].quarantine={until:now+60000};
  status.runs[3].live=false;
  status.runs[4].live=false;status.runs[4].retiring=true;
  status.projects=[appFixture('missing-copy','live',4)];
  const h=harness(status);
  for(const id of ['#mc-sub','#mc-search','#mc-count','#mc-list'])h.node(id);
  for(const key of ['all','healthy','attention','changing']){
    h.node(`[data-server-count="${key}"]`);h.node(`[data-server-filter="${key}"]`);
  }
  h.run('view=vMachines();view.mount();view.status();');
  return h;
}
await check('server status filters separate failures from starting or retired machines',()=>{
  const h=serversHarness();assert.deepEqual(shownServers(h),[1,2,3,4,5]);
  chooseFilter(h,'server','healthy');assert.deepEqual(shownServers(h),[1]);
  chooseFilter(h,'server','attention');assert.deepEqual(shownServers(h),[3,4]);
  chooseFilter(h,'server','changing');assert.deepEqual(shownServers(h),[2,5]);
  chooseFilter(h,'server','all');assert.deepEqual(shownServers(h),[1,2,3,4,5]);
});
await check('server search matches labels, tags and regions without case sensitivity',()=>{
  const h=serversHarness(),search=h.nodes.get('#mc-search');
  const find=value=>{search.value=value;search.oninput({target:search});};
  find('  MAPLE LAB  ');assert.deepEqual(shownServers(h),[1]);
  find('EDGE-BLUE');assert.deepEqual(shownServers(h),[2]);
  find('EU-WEST');assert.deepEqual(shownServers(h),[3]);
  chooseFilter(h,'server','healthy');assert.deepEqual(shownServers(h),[]);
  assert.match(h.nodes.get('#mc-list').innerHTML,/No matching servers/);
  clearFilters(h,'server');assert.deepEqual(shownServers(h),[1,2,3,4,5]);assert.equal(search.value,'');
  h.status.slots=[];h.status.runs=[];h.run('view.status();');
  assert.match(h.nodes.get('#mc-list').innerHTML,/No servers have checked in/);
  assert.doesNotMatch(h.nodes.get('#mc-list').innerHTML,/No matching servers/);
});

await check('malformed URL escapes do not crash navigation', () => {
  const h = harness(); h.context.location.hash = '#/apps/broken%ZZ?version=2';
  assert.equal(h.run('parseHash().parts[1]'), 'broken%ZZ');
  assert.equal(h.run('parseHash().q.get("version")'), '2');
});

await check('the main app link is global and replica rows keep their numbered URLs',()=>{
  const h=harness(); h.context.hooks.spec=(_name,v)=>Promise.resolve(spec(v));
  h.run('view=vApp("web"); view.status();');
  assert.match(h.nodes.get('#ad-head').innerHTML,/href="https:\/\/web\.example\.test\/"[^>]*aria-label="Global app address"/);
  assert.doesNotMatch(h.nodes.get('#ad-head').innerHTML,/href="https:\/\/web-1\.example\.test\//);
  assert.match(h.nodes.get('#ad-replicas').innerHTML,/href="https:\/\/web-1\.example\.test\/"/);
});

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
await check('legacy server targets do not generate missing-capacity warnings',()=>{
  const h=harness(); h.status.pools.push({name:'missing-pool',size:18,live:0});
  assert.equal(h.run('problems().length'),0);
});
await check('a departed empty server is history rather than missing capacity',()=>{
  const h=harness();h.status.runs[0].live=false;h.status.projects[0].enabled=false;
  assert.equal(h.run('problems().some(p=>p.kind==="gone")'),false);
  assert.equal(h.run('machineState(1,null).text'),'inactive');
});
await check('the join form requires a tag and offers standalone explicitly',()=>{
  const h=harness(), form=h.run('joinBlock()');
  assert.match(form,/id="cj-tag" required/);
  assert.match(form,/value="standalone"/);
});
await check('join command values are quoted as literal shell arguments',()=>{
  const h=harness();
  h.context.label="host $(printf surprise) 'quoted'";
  assert.equal(h.run('shellQuote(label)'),"'host $(printf surprise) '\\''quoted'\\'''" );
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
function clusterHarness() {
  const h=harness();
  for(const id of ['#cp-history','#cp-refresh','#cp-meta','#cp-summary','#cp-replicas','#cp-transitions']) h.node(id);
  const at=Date.now();
  h.cluster={self:2,count:3,quorum:2,term:4,observedAt:at,replicas:[1,2,3].map(id=>({
    id,url:`https://cp-${id}.example.test`,label:`Host ${id}`,reachable:true,lastSeen:at,
    view:{primary:id===1,raft:{identityValid:true,role:id===1?'StateLeader':'StateFollower',leader:1,term:4},rlog:{entries:5,bytes:100},memoryBytes:1000}
  })),transitions:[{at,term:4,leader:1}]};
  h.context.fetch=async path=>{assert.equal(path,'/api/cluster');return Response.json(h.cluster);};
  h.panel=h.run('controlPlanePanel()');return h;
}
await check('Fleet shows local replica health and transitions when the primary status is unavailable',async()=>{
  const h=clusterHarness();h.run('S.status=null');await h.panel.refresh();
  assert.match(h.nodes.get('#cp-summary').innerHTML,/3 \/ 3/);
  assert.match(h.nodes.get('#cp-meta').innerHTML,/View from Host 2/);
  assert.match(h.nodes.get('#cp-replicas').innerHTML,/https:\/\/cp-3.example.test\/#\/fleet/);
  assert.match(h.nodes.get('#cp-transitions').innerHTML,/Host 1/);
});
await check('an unreachable former primary is not displayed as the current primary',async()=>{
  const h=clusterHarness();h.cluster.replicas[0].reachable=false;await h.panel.refresh();
  assert.match(h.nodes.get('#cp-summary').innerHTML,/No primary is ready/);
  assert.match(h.nodes.get('#cp-summary').innerHTML,/2 \/ 3/);
  assert.match(h.nodes.get('#cp-replicas').innerHTML,/Last seen: primary/);
});
await check('reachable candidates are shown as electing, without presenting connectivity as quorum',async()=>{
  const h=clusterHarness();
  Object.assign(h.cluster.replicas[1].view.raft,{role:'StatePreCandidate',leader:0,term:2});
  h.cluster.replicas[2].view.raft.identityValid=false;
  await h.panel.refresh();
  const summary=h.nodes.get('#cp-summary').innerHTML;
  assert.match(summary,/Replicas reachable/);assert.match(summary,/3 \/ 3/);
  assert.match(summary,/reachability alone does not confirm quorum/);
  assert.doesNotMatch(summary,/Reachable voters/);
  const rows=h.nodes.get('#cp-replicas').innerHTML.split('<tr');
  const candidate=rows.find(row=>row.includes('https://cp-2.example.test/'));
  assert.match(candidate,/>Electing</);assert.match(candidate,/None observed/);assert.match(candidate,/Term 2/);
  assert.doesNotMatch(candidate,/>Healthy<|>Aligned</);
});
await check('replicas with no leader or a different leader and term are not marked aligned',async()=>{
  const h=clusterHarness();
  Object.assign(h.cluster.replicas[1].view.raft,{role:'StateFollower',leader:0,term:2});
  Object.assign(h.cluster.replicas[2].view.raft,{role:'StateFollower',leader:2,term:3});
  await h.panel.refresh();
  let rows=h.nodes.get('#cp-replicas').innerHTML.split('<tr');
  assert.match(rows.find(row=>row.includes('https://cp-2.example.test/')),/>No primary observed</);
  assert.match(rows.find(row=>row.includes('https://cp-3.example.test/')),/>Different view</);
  Object.assign(h.cluster.replicas[2].view.raft,{leader:1,term:3});
  await h.panel.refresh();
  rows=h.nodes.get('#cp-replicas').innerHTML.split('<tr');
  assert.match(rows.find(row=>row.includes('https://cp-3.example.test/')),/>Different view</);
  Object.assign(h.cluster.replicas[2].view.raft,{leader:1,term:4});
  await h.panel.refresh();
  rows=h.nodes.get('#cp-replicas').innerHTML.split('<tr');
  assert.match(rows.find(row=>row.includes('https://cp-3.example.test/')),/>Aligned</);
});
await check('a failed replica refresh retains history but marks current health unknown',async()=>{
  const h=clusterHarness();await h.panel.refresh();
  h.context.fetch=async()=>{throw Error('offline');};await h.panel.refresh();
  assert.match(h.nodes.get('#cp-meta').innerHTML,/Showing the last received view/);
  assert.match(h.nodes.get('#cp-summary').innerHTML,/Unknown/);
  assert.doesNotMatch(h.nodes.get('#cp-replicas').innerHTML,/>Healthy</);
  assert.match(h.nodes.get('#cp-transitions').innerHTML,/Host 1/);
});
await check('leaving Fleet aborts a pending replica request and prevents late rendering',async()=>{
  const h=clusterHarness(),d=deferred();let signal;
  h.context.fetch=async(_path,options)=>{signal=options.signal;return d.promise;};
  const loading=h.panel.refresh();h.panel.unmount();
  assert.equal(signal.aborted,true);h.nodes.get('#cp-summary').innerHTML='next page';
  d.resolve(Response.json(h.cluster));await loading;
  assert.equal(h.nodes.get('#cp-summary').innerHTML,'next page');
});
await check('a standalone controller displays without requesting Raft diagnostics',async()=>{
  const h=clusterHarness();h.run('S.status.controlPlane={name:"Local controller",runtime:"docker"}');
  h.context.fetch=async()=>{throw Error('must not request a cluster');};await h.panel.refresh();
  assert.match(h.nodes.get('#cp-summary').innerHTML,/Local controller/);
  assert.equal(h.nodes.get('#cp-history').hidden,true);
  assert.doesNotMatch(h.nodes.get('#cp-meta').innerHTML,/unavailable/);
});
console.log(`${passed} UI lifecycle and current-health regressions passed.`);
