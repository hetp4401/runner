import assert from 'node:assert/strict';
import test from 'node:test';
import { replenish, refillSlot } from './scheduler.mjs';
import { reconcileCapacity } from '../../.github/scripts/capacity.mjs';
import { verifyScheduledOrigin } from '../../.github/scripts/verify-schedule.mjs';
import { providerConfig } from './config.mjs';

const now = Date.parse('2026-11-10T12:02:30Z'), slot = Date.parse('2026-11-10T12:02:00Z');
const provider = { repo: 'owner/runner', token: 'private-test-token' };
function fixture() {
  const calls = [], claims = new Map(), state = { term: 1, phase: '2', target: '6', active: [], history: [], fail: false, fenced: false };
  const options = { now: () => now,
    authority: { check: async expected => {
      if (state.fenced || expected && expected.term !== state.term) throw new Error('leadership lost');
      return { term: state.term };
    } },
    claim: async (key, sequence) => {
      if ((claims.get(key) || 0) >= sequence) return { accepted: false };
      claims.set(key, sequence); return { accepted: true };
    },
    api: async (path, options = {}) => {
      calls.push({ path, ...options });
      if (options.method === 'POST') { if (state.fail) throw new Error('response lost'); return null; }
      if (path.includes('/actions/variables')) return { variables: Object.entries({ MACHINE_COUNT: state.target,
        CAPACITY_TRIGGER: 'provider', CAPACITY_PHASE_MIN: state.phase, SERVER_TAG: 'test' }).map(([name, value]) => ({ name, value })) };
      if (path === '/repos/owner/runner') return { default_branch: 'main' };
      if (path.includes('/watchdog.yml/runs')) return { workflow_runs: state.history };
      const status = new URL(path, 'https://api.example').searchParams.get('status');
      return { workflow_runs: state.active.filter(r => !status || r.status === status) };
    },
  };
  return { calls, claims, state, options };
}
test('account phases are deterministic and distinct', () => {
  assert.equal(refillSlot(now, '2'), slot);
  assert.equal(refillSlot(now, '5'), slot - 7 * 60_000);
  assert.equal(refillSlot(now, '8'), slot - 4 * 60_000);
  assert.throws(() => refillSlot(now, '10'));
});
test('two contenders with stale inventory still request just one refill', async () => {
  const f = fixture();
  const results = await Promise.all([replenish(provider, f.options), replenish(provider, f.options)]);
  assert.equal(results.filter(r => r.action === 'requested').length, 1);
  assert.equal(f.calls.filter(c => c.method === 'POST').length, 1);
  assert.deepEqual(f.calls.find(c => c.method === 'POST').body, { ref: 'main', inputs: { refill_slot: String(slot) } });
});
test('lost response consumes the attempt; the next leader does not repeat it', async () => {
  const f = fixture(); f.state.fail = true;
  await assert.rejects(replenish(provider, f.options), /response lost/);
  f.state.fail = false; f.state.term++;
  assert.equal((await replenish(provider, f.options)).action, 'already_claimed');
  assert.equal(f.calls.filter(c => c.method === 'POST').length, 1);
});
test('leadership loss after the committed claim prevents the external write', async () => {
  const f = fixture(), claim = f.options.claim;
  f.options.claim = async (...args) => { const r = await claim(...args); f.state.fenced = true; return r; };
  await assert.rejects(replenish(provider, f.options), /leadership lost/);
  assert.equal(f.calls.filter(c => c.method === 'POST').length, 0);
  assert.equal(f.claims.size, 1);
});
test('a cold bootstrap also recognizes an earlier GitHub request for the slot', async () => {
  const f = fixture(); f.state.history = [{ display_title: `Machine schedule (refill ${slot})`, head_branch: 'main' }];
  assert.equal((await replenish(provider, f.options)).action, 'already_requested');
  assert.equal(f.claims.size, 0);
});
test('disabled targets, off-phase accounts and reserved capacity do not request hosts', async () => {
  for (const change of [{ target: '0' }, { phase: '5' }, { target: '1', active: [{ id: 99,
    display_title: 'machine capacity 1', status: 'queued', created_at: new Date(now - 60_000).toISOString() }] }]) {
    const f = fixture(); Object.assign(f.state, change);
    await replenish(provider, f.options);
    assert.equal(f.calls.filter(c => c.method === 'POST').length, 0);
  }
});
const workflow = { GITHUB_REPOSITORY: 'owner/runner', MACHINE_REF: 'main', MACHINE_COUNT: '6', SERVER_TAG: 'test',
  GITHUB_EVENT_NAME: 'workflow_dispatch', GITHUB_RUN_ID: '1234', GITHUB_WORKFLOW: 'Machine schedule',
  GITHUB_WORKFLOW_REF: 'owner/runner/.github/workflows/watchdog.yml@refs/heads/main',
  CAPACITY_TRIGGER: 'provider', CAPACITY_PHASE_MIN: '2', CAPACITY_SLOT: String(slot) };
test('provider mode allows a fresh phased request and keeps cron and empty manual runs read-only', async () => {
  const f = fixture(); assert.equal((await reconcileCapacity(workflow, f.options)).launched, 1);
  for (const override of [{ GITHUB_EVENT_NAME: 'schedule' }, { CAPACITY_SLOT: '' },
    { GITHUB_WORKFLOW_REF: 'owner/runner/.github/workflows/other.yml@refs/heads/main' }, { CAPACITY_DRY_RUN: '1' }]) {
    const g = fixture(); assert.equal((await reconcileCapacity({ ...workflow, ...override }, g.options)).launched, 0);
  }
});
test('stale slots, wrong phases, future slots and workflow reruns cannot launch', async () => {
  for (const override of [{ CAPACITY_SLOT: String(slot - 600_000) }, { CAPACITY_PHASE_MIN: '5' },
    { CAPACITY_SLOT: String(slot + 600_000) }]) {
    const f = fixture(); await assert.rejects(reconcileCapacity({ ...workflow, ...override }, f.options), /refill slot/);
    assert.equal(f.calls.length, 0);
  }
  const f = fixture(); assert.equal((await reconcileCapacity({ ...workflow, GITHUB_RUN_ATTEMPT: '2' }, f.options)).launched, 0);
  assert.equal(f.calls.length, 0);
});
test('machine jobs recognize provider requests but reject previews and other branches', async () => {
  const env = { GITHUB_REPOSITORY: 'owner/runner', GITHUB_REF_NAME: 'main', SCHEDULE_RUN: '1234' };
  const parent = { event: 'workflow_dispatch', path: '.github/workflows/watchdog.yml', head_branch: 'main', display_title: `Machine schedule (refill ${slot})` };
  assert.equal(await verifyScheduledOrigin(env, async () => parent), true);
  for (const change of [{ display_title: 'Machine schedule' }, { head_branch: 'other' }, { path: '.github/workflows/machine.yml' }])
    await assert.rejects(verifyScheduledOrigin(env, async () => ({ ...parent, ...change })));
});
test('the provider uses generic consensus without D1 and rejects even replica counts', () => {
  const env = { REPLICAS: '7', FLEET_REPLICA: '4', CLUSTER_ID: 'provider-test-cluster', CLUSTER_TOKEN: 'private-test-token-at-least-32-characters',
    MEMBERS: JSON.stringify(Array.from({ length: 7 }, (_, i) => ({ id: i + 1, url: `https://provider-${i + 1}.example.com` }))),
    GITHUB_PROVIDERS: JSON.stringify([provider]) };
  const config = providerConfig(env);
  assert.equal(config.id, 4); assert.equal(config.quorum, 4); assert.equal(config.raftEnv.CP_RAFT_ALLOW_CLAIMS, '1');
  assert.throws(() => providerConfig({ ...env, REPLICAS: '4' }));
});
