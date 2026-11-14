import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { bootstrapManifest } from '../../control/cluster-init.mjs';
const children = [], logs = new Map(), token = 'provider-service-test-token-at-least-32-characters';
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function port() { const s = http.createServer(); s.listen(0, '127.0.0.1'); await once(s, 'listening'); const p = s.address().port; await new Promise(r => s.close(r)); return p; }
async function until(fn, timeout = 30000) { const end = Date.now() + timeout; while (Date.now() < end) { const r = await fn(); if (r) return r; await sleep(150); } throw Error('provider condition timed out'); }
const members = []; for (let id = 1; id <= 7; id++) members.push({ id, url: 'http://127.0.0.1:' + await port() });
async function request(member, path, body) {
  try {
    const r = await fetch(member.url + path, { method: body === undefined ? 'GET' : 'POST',
      headers: { 'x-cp-token': token, 'content-type': 'application/json' },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }), signal: AbortSignal.timeout(6000) });
    return r.ok ? await r.json() : null;
  } catch { return null; }
}
try {
  for (const member of members) {
    const child = spawn(process.execPath, ['providers/github/server.mjs'], { detached: true,
      env: { ...process.env, REPLICAS: '7', NODE_ID: String(member.id), MEMBERS: JSON.stringify(members),
        CLUSTER_ID: 'provider-service-test-cluster', CLUSTER_TOKEN: token, GITHUB_PROVIDERS: '[]',
        PORT: new URL(member.url).port, RAFT_PORT: String(await port()), BIND_HOST: '127.0.0.1',
        RAFT_BINARY: '/tmp/runners-raft-provider-test', RAFT_TICK_MS: '100', RAFT_ELECTION_TICKS: '15' },
      stdio: ['ignore', 'pipe', 'pipe'] });
    children.push(child); logs.set(child.pid, '');
    for (const stream of [child.stdout, child.stderr]) stream.on('data', b => logs.set(child.pid, (logs.get(child.pid) + b).slice(-4000)));
  }
  const views = await until(async () => { const v = await Promise.all(members.map(m => request(m, '/_cp/raft-status'))); return v.every(Boolean) && v; });
  assert.ok((await Promise.all(members.map(m => request(m, '/healthz')))).every(v => v?.ok));
  assert.ok((await Promise.all(members.map(m => request(m, '/readyz')))).every(v => !v));
  console.log('ok fresh provider processes are live but are not ready voters');
  const manifest = bootstrapManifest({ members, quorum: 4 }, views);
  for (const m of members) assert.ok(await request(m, '/_cp/bootstrap', manifest));
  const leader = await until(async () => {
    const v = await Promise.all(members.map(m => request(m, '/api/status')));
    const primary = v.filter(x => x?.primary);
    return primary.length === 1 && v.every(x => x?.ready) && primary[0];
  });
  const authority = await Promise.all(members.map(m => request(m, '/_cp/verify')));
  assert.equal(authority.filter(Boolean).length, 1);
  console.log('ok seven ready replicas expose exactly one authorized primary');
  assert.ok(await request(members[leader.id - 1], '/_cp/transfer', {}));
  const replacement = await until(async () => {
    const v = await Promise.all(members.map(m => request(m, '/api/status')));
    const active = v.filter(x => x?.primary);
    return active.length === 1 && active[0].id !== leader.id && active[0].term > leader.term && active[0];
  });
  assert.equal(await request(members[leader.id - 1], '/_cp/verify'), null);
  const overview = await until(async () => { const v = await request(members[replacement.id - 1], '/api/status'); return v.members.every(m => m.reachable) && v; });
  assert.equal(overview.storage, 'memory'); assert.equal(overview.quorum, 4);
  assert.ok(overview.transitions.length <= 30);
  console.log('ok handoff fences the previous primary and every replica exposes peer health');
} catch (error) { for (const [pid, log] of logs) console.error(pid, log); throw error; }
finally {
  for (const child of children) { try { process.kill(-child.pid, 'SIGTERM'); } catch {} }
  await sleep(4000);
  for (const child of children) { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }
}
