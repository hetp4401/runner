import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';
import { setImmediate as tick } from 'node:timers/promises';

// Exercise the shipped reconciliation function with a blocked Docker removal.
// No Docker daemon or real application is changed.
const source = readFileSync(new URL('../../agent/agent.mjs', import.meta.url), 'utf8');
const start = source.indexOf('const queued = new Set();');
const end = source.indexOf('// Every wanted project', start);
assert.ok(start > 0 && end > start);
function harness(name = 'old-app') {
  let release, reject;
  const removal = new Promise((resolve, fail) => { release = resolve; reject = fail; });
  const projects = new Map([[name, { s: 'healthy', busy: false }]]), applied = [], removed = [];
  const context = vm.createContext({ projects, currentDesired: {}, ready: true,
    log() {}, wake() {}, fingerprint: want => want.revision,
    async remove(key) { removed.push(key); projects.get(key).busy = true; await removal; projects.delete(key); },
    async apply(key, want) { applied.push({ key, version: want.v }); projects.set(key, { ...want, busy: true }); },
  });
  vm.runInContext(source.slice(start, end), context);
  return { applied, removed, release, reject, reconcile: vm.runInContext('reconcile', context) };
}
const want = v => ({ v, revision: 'revision-' + v, app: 'next-app', replica: 1 });

{
  const h = harness(); h.reconcile({ 'next-app': want(1) }); h.reconcile({});
  h.release(); await tick();
  assert.deepEqual(h.applied, []);
  console.log('ok cancelled queued apps never start after a slow removal finishes');
}
{
  const h = harness(); h.reconcile({ 'next-app': want(1) }); h.reconcile({ 'next-app': want(2) });
  h.release(); await tick();
  assert.deepEqual(h.applied, [{ key: 'next-app', version: 2 }]);
  console.log('ok queued apps use the latest accepted version when their ports become available');
}
{
  const h = harness(); h.reconcile({}); h.reconcile({ 'next-app': want(1) });
  await tick(); assert.deepEqual(h.applied, []);
  h.release(); await tick();
  assert.deepEqual(h.applied, [{ key: 'next-app', version: 1 }]);
  console.log('ok new plans also wait for removals started by an earlier plan');
}
{
  const h = harness(); h.reconcile({ 'next-app': want(1) }); h.reject(new Error('Docker refused to stop'));
  await tick(); assert.deepEqual(h.applied, []);
  console.log('ok failed removals do not launch conflicting replacement work');
}
{
  const h = harness('constructor'); h.reconcile({}); h.release(); await tick();
  assert.deepEqual(h.removed, ['constructor']);
  console.log('ok application names matching object properties are removed normally');
}
