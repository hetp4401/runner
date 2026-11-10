import assert from 'node:assert/strict';
import { makeControl, Fleet, deploy, MIN, T } from './sim/lib.mjs';
let checks=0;
const check=(name,fn)=>{fn();checks++;console.log('ok '+name)};
const spec=(spread,replicas=4,region=null)=>({compose:`x-runner:\n  replicas: ${replicas}\n${spread===undefined?'':`  spread-tags: ${spread}\n`}${region?`  region: ${region}\n`:''}services:\n  web:\n    image: nginx\n`});
async function setup(groups={a:{size:3,cap:3},b:{size:1,cap:1},c:{size:1,cap:1}}){const c=makeControl();const f=new Fleet(c,groups,{drainAt:()=>null});for(const k of Object.keys(groups))await f.startPool(k);await f.run(5*MIN);return {c,f};}
const copies=(c,name)=>c.copiesOf(name).filter(x=>!x.leaving);
const tags=(c,name)=>copies(c,name).map(x=>c.liveMachines(T).get(x.machine)?.pool||'standalone');
{
 const {c,f}=await setup();
 deploy(c,'spread',spec(true));await f.run(4*MIN);
 check('opted-in replicas use all available tags before reusing them',()=>{assert.equal(copies(c,'spread').length,4);assert.equal(new Set(tags(c,'spread')).size,3)});
 check('tag preference is visible in app status',()=>assert.equal(c.describe(c.project('spread')).spreadTags,true));
 deploy(c,'spread',{replicas:5});check('replica-only updates preserve the preference',()=>assert.equal(c.describe(c.project('spread')).spreadTags,true));
 c.putEnv('spread',{EXAMPLE:'value'});check('environment updates preserve the preference',()=>assert.equal(c.describe(c.project('spread')).spreadTags,true));
 for(const value of ['"true"','1','[]','null'])check('invalid spread-tags '+value+' is rejected',()=>assert.throws(()=>deploy(c,'invalid',spec(value)),/spread-tags must be true or false/));
}
{
 const {c,f}=await setup({a:{size:3,cap:3}});deploy(c,'one',spec(true,5));await f.run(4*MIN);
 check('one tag is sufficient to run every requested replica',()=>assert.equal(copies(c,'one').length,5));
}
{
 const {c,f}=await setup();const up=c.liveMachines(T);const machines=[...up.values()];
 for(const r of machines){r.region=r.pool==='a'?'eu-west':'eu-east';c.runs.set(r.id,r)}
 deploy(c,'regional',spec(true,3,'us-west'));check('tag diversity never bypasses a missing required region',()=>assert.equal(copies(c,'regional').length,0));
 deploy(c,'custom',spec(true,3,'eu-west'));
 check('custom region constraints still restrict every opted-in replica',()=>{assert.equal(copies(c,'custom').length,3);assert.ok(tags(c,'custom').every(tag=>tag==='a'))});
}
{
 const {c,f}=await setup();deploy(c,'plain',spec(undefined,1));deploy(c,'disabled',spec(false,1));await f.run(3*MIN);
 check('the preference is off unless the app opts in',()=>{assert.equal(c.describe(c.project('plain')).spreadTags,false);assert.equal(c.describe(c.project('disabled')).spreadTags,false)});
 const before=copies(c,'plain').map(x=>x.machine);deploy(c,'plain',spec(true,1));check('enabling the preference does not move a healthy existing copy',()=>assert.deepEqual(copies(c,'plain').map(x=>x.machine),before));
}
console.log(`${checks} tag-spreading checks passed`);
