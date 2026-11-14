import http from 'node:http';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { providerConfig } from './config.mjs';
import { replenish } from './scheduler.mjs';
import { githubClient } from '../../.github/scripts/capacity.mjs';
import { Authority } from '../../control/src/ha/authority.mjs';
import { equalSecret } from '../../control/src/ha/config.mjs';
import { RuntimeTail } from '../../control/src/ha/tail.mjs';

const env = process.env, config = providerConfig(env), started = Date.now();
const incarnation = randomUUID(), tail = new RuntimeTail();
const peers = new Map(), results = new Map(), singleClaims = new Map();
let raft = null, core = null, stopping = false, checking = false, working = false, polling = false;
let error = null, nextRenew = 0, lastTransition = null, activeRequests = new AbortController();
const log = message => console.log(new Date().toISOString() + ' provider ' + config.id + ' ' + message);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function request(url, init = {}) {
  const response = await fetch(url, { ...init, signal: AbortSignal.timeout(5500),
    headers: { 'x-cp-token': config.token, 'content-type': 'application/json', ...init.headers } });
  if (!response.ok) throw new Error('consensus request failed (HTTP ' + response.status + ')');
  return response.status === 204 ? null : response.json();
}
const authority = config.enabled ? new Authority(config, () => request(config.raftOrigin + '/verify')) : {
  current: { node: config.id, term: 0, boot: incarnation },
  live: () => !stopping,
  check: async () => { if (stopping) throw new Error('provider stopped'); return authority.current; },
};
function demote() {
  if (config.enabled) authority.clear();
  activeRequests.abort(); activeRequests = new AbortController(); nextRenew = 0;
}
function view() {
  return { id: config.id, incarnation, started, observedAt: Date.now(), mode: config.enabled ? 'raft' : 'single',
    primary: Boolean(authority.live()), leader: raft?.leader || (config.enabled ? null : config.id),
    term: raft?.term || 0, phase: raft?.phase || (config.enabled ? 'starting' : 'standalone'),
    ready: !config.enabled || Boolean(raft?.identityValid && !raft.outgoing?.length),
    claims: config.enabled ? raft?.registry?.claims || {} : Object.fromEntries(singleClaims),
    providers: [...results.values()], transitions: tail.history(), error };
}
function clusterView() {
  const local = view(), at = Date.now();
  return { ...local, replicas: config.count, quorum: config.quorum || 1, storage: 'memory',
    members: (config.members || [{ id: config.id }]).map(member => {
      const row = member.id === config.id ? { at, view: local } : peers.get(member.id);
      return { ...member, reachable: Boolean(row && at - row.at < 20_000), view: row?.view || null };
    }) };
}
function reply(res, code, data) {
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(data));
}
function proxy(req, res, path) {
  const out = http.request(config.raftOrigin + path, { method: req.method, headers: req.headers, timeout: 6000 }, incoming => {
    res.writeHead(incoming.statusCode, incoming.headers); incoming.pipe(res);
  });
  out.on('error', () => { if (!res.headersSent) reply(res, 503, { error: 'consensus unavailable' }); else res.destroy(); });
  out.on('timeout', () => out.destroy()); req.pipe(out); res.on('close', () => out.destroy());
}
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  try {
    if (url.pathname.startsWith('/_cp/')) {
      if (!config.enabled || !equalSecret(req.headers['x-cp-token'], config.token)) return reply(res, 401, { error: 'unauthorized' });
      if (url.pathname === '/_cp/status') return reply(res, 200, view());
      if (url.pathname === '/_cp/verify') return reply(res, 200, await authority.check());
      const paths = { '/_cp/raft-status': '/status', '/_cp/raft': '/message', '/_cp/join': '/join', '/_cp/pull': '/pull', '/_cp/bootstrap': '/bootstrap', '/_cp/transfer': '/transfer' };
      if (!paths[url.pathname] || !core) return reply(res, 503, { error: 'consensus unavailable' });
      if (['/_cp/join', '/_cp/transfer'].includes(url.pathname) && req.method === 'POST') await request(config.raftOrigin + '/verify');
      return proxy(req, res, paths[url.pathname] + url.search);
    }
    if (url.pathname === '/healthz') return reply(res, !config.enabled || core ? 200 : 503, { ok: !config.enabled || Boolean(core) });
    if (url.pathname === '/readyz') return reply(res, view().ready ? 200 : 503, { ready: view().ready, phase: view().phase });
    if (['/', '/api/status', '/api/cluster'].includes(url.pathname)) return reply(res, 200, clusterView());
    reply(res, 404, { error: 'not found' });
  } catch { reply(res, 503, { error: 'current quorum unavailable' }); }
});

async function checkLeadership() {
  if (!config.enabled || checking || stopping) return;
  checking = true;
  try {
    raft = await request(config.raftOrigin + '/status');
    const transition = raft.leader ? `${raft.generation}:${raft.term}:${raft.leader}` : null;
    if (transition && transition !== lastTransition) {
      lastTransition = transition;
      tail.transition({ id: transition, at: Date.now(), term: raft.term, leader: raft.leader, observedBy: config.id });
    }
    if (!raft.identityValid || raft.role !== 'StateLeader' || raft.leader !== config.id) {
      if (authority.current) demote();
      return;
    }
    if (Date.now() >= nextRenew) { await authority.renew(); nextRenew = Date.now() + 5000; }
    if (!authority.live()) throw new Error('leadership expired');
    error = null;
  } catch { error = 'quorum unavailable'; demote(); }
  finally { checking = false; }
}
async function claim(key, sequence, fence) {
  if (!config.enabled) {
    const before = singleClaims.get(key);
    if (before?.sequence >= sequence) return { accepted: false, claim: before };
    const value = { sequence, owner: config.id, term: 0 };
    singleClaims.set(key, value); return { accepted: true, claim: value };
  }
  return request(config.raftOrigin + '/claim', { method: 'POST', body: JSON.stringify({ key, sequence, term: fence.term }) });
}
async function work() {
  if (working || stopping || !authority.live()) return;
  working = true;
  const signal = activeRequests.signal;
  try {
    for (const provider of config.providers) {
      if (stopping || signal.aborted || !authority.live()) break;
      try {
        const api = githubClient(provider.token, env.GITHUB_API_URL, (url, init) => fetch(url, {
          ...init, signal: AbortSignal.any([signal, init.signal]),
        }));
        const result = await replenish(provider, { api, authority, claim });
        results.set(provider.repo, result);
        if (result.action === 'requested') log(`requested ${provider.repo} slot ${result.slot}`);
      } catch (failure) {
        results.set(provider.repo, { repo: provider.repo, at: Date.now(), action: 'error', error: failure.message });
        // githubClient omits response bodies and credentials from its errors.
        log(provider.repo + ': ' + failure.message);
      }
    }
  } finally { working = false; }
}
async function pollPeers() {
  if (!config.enabled || polling || stopping) return;
  polling = true;
  try {
    await Promise.all(config.members.filter(m => m.id !== config.id).map(async member => {
      try {
        const remote = await request((config.overrides[String(member.id)] || member.url) + '/_cp/status');
        peers.set(member.id, { at: Date.now(), view: remote });
        for (const transition of remote.transitions || []) tail.transition(transition);
      } catch {}
    }));
  } finally { polling = false; }
}
let shuttingDown = false;
async function shutdown(code = 0) {
  if (shuttingDown) return; shuttingDown = true; stopping = true;
  for (const timer of timers) clearInterval(timer);
  demote();
  if (core && raft?.leader === config.id) {
    await request(config.raftOrigin + '/transfer', { method: 'POST' }).catch(() => {});
    await sleep(1500);
  }
  if (core) core.kill('SIGTERM');
  server.closeAllConnections(); server.close();
  setTimeout(() => { if (core) core.kill('SIGKILL'); process.exit(code); }, 1500);
}
const timers = [];
server.listen(config.port, config.host, () => log(`listening; ${config.count} configured replicas`));
if (config.enabled) {
  core = spawn(env.RAFT_BINARY || '/usr/local/bin/runner-raft', [], { env: { ...env, ...config.raftEnv }, stdio: ['ignore', 'ignore', 'pipe'] });
  core.stderr.on('data', () => {}); // expose status, not unbounded consensus debug logs
  core.on('error', () => { core = null; shutdown(1); });
  core.on('exit', () => { core = null; if (!stopping) shutdown(1); });
  timers.push(setInterval(checkLeadership, 500), setInterval(pollPeers, 5000));
}
timers.push(setInterval(work, 10_000));
process.on('SIGTERM', () => shutdown()); process.on('SIGINT', () => shutdown());
checkLeadership(); pollPeers();
