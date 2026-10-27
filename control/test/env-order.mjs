import assert from 'node:assert/strict';
import {Control} from '../src/index.js';
import {agentPlan} from '../src/agent-plan.mjs';

const variables=[['REGISTRY_ACCESS_TOKEN','test-registry'],['TUNNEL_TOKEN','test-tunnel'],['SESSION_SIGNING_KEY','test-signing']];
const copy={replica:3,machine:7};
const file=values=>Control.prototype.envFile.call({env:{DOMAIN:'example.test'},appEnv:new Map([['web',new Map(values)]])},'web',{},copy,1);
const revision=env=>agentPlan({app:'web',replica:3,port:8080,compose:'services:\n  web:\n    image: nginx\n',files:{'.env':env}}).revision;
const hot=file(variables),cold=file([...variables].sort(([a],[b])=>a<b?-1:a>b?1:0));
assert.equal(hot,cold,'cold recovery must retain identical environment bytes');
assert.equal(revision(hot),revision(cold),'restart must not create a new workload revision');
assert.equal(file([...variables].reverse()),hot,'API insertion order must not affect the deployed revision');
assert.notEqual(revision(file([...variables,['NEW_VALUE','test-change']])),revision(hot),'actual environment changes still trigger a deployment');
assert.notEqual(revision(file(variables.slice(1))),revision(hot),'removing a value still triggers a deployment');
assert.match(file([['QUOTE',"line one\nline 'two' $literal"]]),/QUOTE=/,'quoted and multiline values remain supported');
console.log('6 stable environment and workload-revision regressions passed.');
