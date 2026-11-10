import http from 'node:http';
import https from 'node:https';
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { clusterConfig, equalSecret } from './src/ha/config.mjs';
import { ClusterDatabase } from './src/ha/database.mjs';
import { Authority } from './src/ha/authority.mjs';
import { RuntimeTail } from './src/ha/tail.mjs';
import { replicaTunnel, route } from './src/ha/tunnel.mjs';

const env = process.env, config = clusterConfig(env);
if (!config.enabled) throw new Error('ha-server requires a replicated configuration');
const database = new ClusterDatabase(env), tail = new RuntimeTail();
const started = Date.now(), incarnation = randomUUID();
const raftOrigin = 'http://127.0.0.1:' + (env.CP_RAFT_PORT || 8931);
const childPort = Number(env.CP_CHILD_PORT || 8930), port = Number(env.PORT || 8920);
const childOrigin = 'http://127.0.0.1:' + childPort;
const overrides = JSON.parse(env.CP_PEER_OVERRIDES || '{}');
const peers = new Map(), cursors = new Map(), sockets = new Set();
const children = new Set(), page = readFileSync(new URL('./src/app.html', import.meta.url), 'utf8');
let raft = null, core = null, application = null, tunnel = null, connector = null;
let stopping = false, busy = false, peersBusy = false, nextRenew = 0, nextRoute = 0;
let appReady = false, lastError = null, lastTransition = null, coreRetry = 0, startBusy = false;
let identityWaitingUntil = 0;
let routing = false;
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const log = text => console.log(new Date().toISOString() + ' replica ' + config.id + ' ' + text);
const origin = member => overrides[String(member.id)] || member.url;
async function jsonRequest(url, init = {}) {
  const response = await fetch(url, { signal: AbortSignal.timeout(5000), ...init,
    headers: { 'x-cp-token': config.token, 'content-type': 'application/json', ...init.headers } });
  if (!response.ok) throw new Error('peer request failed (HTTP ' + response.status + ')');
  return response.status === 204 ? null : response.json();
}
const authority = new Authority(config, database, () => jsonRequest(raftOrigin + '/verify'));
function launch(binary, args, childEnv, kind) {
  const child = spawn(binary, args, { env: childEnv, stdio: 'inherit' });
  children.add(child);
  child.on('error', () => { lastError = kind + ' could not start'; });
  child.on('exit', (code) => {
    children.delete(child);
    if (child === core) {
      core = null; raft = null;
      identityWaitingUntil = code === 75 ? Date.now() + 15_000 : 0;
      coreRetry = Date.now() + (code === 75 ? 2000 : 10_000);
      demote('voter stopped');
    }
    if (child === application) { application = null; appReady = false; }
    if (child === connector) { connector = null; if (!stopping) shutdown(1); }
  });
  return child;
}
async function stopChild(child) {
  if (!child || !children.has(child)) return;
  child.kill('SIGTERM');
  for (let i = 0; i < 60 && children.has(child); i++) await sleep(100);
  if (children.has(child)) child.kill('SIGKILL');
}
function closeConnections() { for (const socket of sockets) socket.destroy(); sockets.clear(); }
function demote(reason) {
  authority.clear(); appReady = false; nextRenew = 0;
  closeConnections();
  if (application) {
    const old = application; application = null;
    stopChild(old).catch(() => {});
    log('scheduler stopped: ' + reason);
  }
}
function localView() {
  return { id: config.id, label: config.self.label, origin: config.self.url, incarnation,
    started, observedAt: Date.now(), raft, primary: Boolean(application && appReady && authority.live()),
    authority: authority.live() ? authority.current : null, recovering: Boolean(application && !appReady),
    tunnelReady: Boolean(connector), error: lastError, memoryBytes: process.memoryUsage().rss,
    rlog: tail.stats(), transitions: tail.history(), d1: { ...database.stats } };
}
function clusterView() {
  const now = Date.now();
  return { mode: 'raft', self: config.id, count: config.count, quorum: config.quorum,
    leader: raft?.leader || null, term: raft?.term || null, observedAt: now,
    replicas: config.members.map(member => {
      const row = member.id === config.id ? { at: now, view: localView() } : peers.get(member.id);
      return { ...member, reachable: Boolean(row && now - row.at < 15_000), lastSeen: row?.at || null, view: row?.view || null };
    }), transitions: tail.history(), persistence: { configuration: 'D1', electionMetadata: 'D1', rlog: 'bounded memory only' } };
}
async function body(req, limit = 2 * 1024 * 1024) {
  const chunks = []; let size = 0;
  for await (const chunk of req) { size += chunk.length; if (size > limit) throw new Error('request too large'); chunks.push(chunk); }
  return Buffer.concat(chunks);
}
function reply(res, code, value) {
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(JSON.stringify(value));
}
function destination(req) {
  if (application && appReady && authority.live()) return childOrigin;
  const leader = config.members.find(member => member.id === raft?.leader && member.id !== config.id);
  return leader && !req.headers['x-cp-forwarded'] ? origin(leader) : null;
}
function proxy(req, res, target, path = req.url) {
  const address = new URL(target), transport = address.protocol === 'https:' ? https : http;
  const headers = { ...req.headers };
  if (target !== childOrigin) { headers.host = address.host; headers['x-cp-forwarded'] = String(config.id); }
  const request = transport.request(address.origin + path, { method: req.method, headers, timeout: 65_000 }, incoming => {
    res.writeHead(incoming.statusCode, incoming.headers); incoming.pipe(res);
  });
  request.on('timeout', () => request.destroy());
  request.on('error', () => { if (!res.headersSent) reply(res, 503, { error: 'primary unavailable; retry shortly' }); else res.destroy(); });
  req.pipe(request); res.on('close', () => request.destroy());
}
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  try {
    if (url.pathname.startsWith('/_cp/')) {
      if (!equalSecret(req.headers['x-cp-token'], config.token)) return reply(res, 401, { error: 'unauthorized' });
      if (url.pathname === '/_cp/raft') {
        if (!core) return reply(res, 503, { error: 'voter warming up' });
        return proxy(req, res, raftOrigin, '/message');
      }
      if (url.pathname === '/_cp/status') return reply(res, 200, localView());
      if (url.pathname === '/_cp/tail') return reply(res, 200, { incarnation, ...tail.read(Number(url.searchParams.get('after')) || 0) });
      if (url.pathname === '/_cp/recovery') return reply(res, 200, { reports: [...tail.reports.values()].map(({ at, body }) => ({ at, body })) });
      if (url.pathname === '/_cp/verify') {
        const expected = req.method === 'POST' ? JSON.parse(await body(req, 4096)) : null;
        return reply(res, 200, await authority.check(expected));
      }
      if (url.pathname === '/_cp/report' && req.method === 'POST') {
        const report = JSON.parse(await body(req));
        await authority.check(report.authority);
        if (report.kind === 'report') tail.append('report', report.body);
        else if (report.kind === 'event') tail.append('event', report.body);
        return reply(res, 200, { ok: true });
      }
      return reply(res, 404, { error: 'unknown peer endpoint' });
    }
    if (url.pathname === '/api/cluster') return reply(res, 200, clusterView());
    if (url.pathname === '/api/leadership/verify' && req.method === 'POST') {
      if (!equalSecret(req.headers.authorization, 'Bearer ' + env.JOIN_TOKEN)) return reply(res, 401, { error: 'unauthorized' });
      if (authority.live()) return reply(res, 200, await authority.check(JSON.parse(await body(req, 4096))));
      const target = destination(req);
      if (target && target !== childOrigin) return proxy(req, res, target);
      return reply(res, 503, { error: 'primary unavailable' });
    }
    if (url.pathname === '/cluster' || url.pathname === '/cluster/') {
      res.writeHead(302, { location: '/#/fleet', 'cache-control': 'no-store' }); return res.end();
    }
    // Every replica serves the same application shell. Its Fleet section can
    // inspect the local cluster view even when no primary can serve fleet APIs.
    if (req.method === 'GET' && url.pathname === '/') {
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' }); return res.end(page);
    }
    if (url.pathname === '/readyz') {
      const warm = env.CP_WARM_REPLACEMENT === '1' && Date.now() < identityWaitingUntil;
      return reply(res, (core && raft?.identityValid) || warm ? 200 : 503,
        { ok: Boolean((core && raft?.identityValid) || warm), voterReady: Boolean(core && raft?.identityValid), replacementWaiting: warm });
    }
    if (url.pathname === '/healthz') return reply(res, core && raft?.identityValid ? 200 : 503,
      { ok: Boolean(core && raft?.identityValid), tunnelsReady: Boolean(core && raft?.identityValid),
        mode: 'raft', replica: config.id, primary: Boolean(appReady && authority.live()), quorum: config.quorum });
    const target = destination(req);
    if (!target) {
      return reply(res, 503, { error: 'no primary is ready; existing workloads keep running', cluster: '/#/fleet' });
    }
    proxy(req, res, target);
  } catch { reply(res, 503, { error: 'current leader authority unavailable; retry shortly' }); }
});
server.on('upgrade', async (req, socket, head) => {
  const target = destination(req);
  if (!target) return socket.end('HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n\r\n');
  if (target === childOrigin) {
    try { await authority.check(); } catch { return socket.destroy(); }
  }
  const address = new URL(target), transport = address.protocol === 'https:' ? https : http;
  const headers = { ...req.headers, ...(target !== childOrigin ? { host: address.host, 'x-cp-forwarded': String(config.id) } : {}) };
  const request = transport.request(address.origin + req.url, { headers, timeout: 10_000 });
  request.on('upgrade', (response, upstream, extra) => {
    const lines = Object.entries(response.headers).map(([key, value]) => key + ': ' + value).join('\r\n');
    socket.write('HTTP/1.1 101 Switching Protocols\r\n' + lines + '\r\n\r\n');
    if (extra.length) socket.write(extra); if (head.length) upstream.write(head);
    sockets.add(socket); sockets.add(upstream);
    socket.on('close', () => { sockets.delete(socket); upstream.destroy(); });
    upstream.on('close', () => { sockets.delete(upstream); socket.destroy(); });
    socket.on('error', () => {}); upstream.on('error', () => {});
    socket.pipe(upstream).pipe(socket);
  });
  request.on('response', response => { socket.end('HTTP/1.1 ' + response.statusCode + ' Unavailable\r\nConnection: close\r\n\r\n'); response.resume(); });
  request.on('error', () => socket.destroy()); request.on('timeout', () => request.destroy()); request.end();
});

async function startTunnel() {
  if (connector || env.AUTO_TUNNEL === '0' || env.AUTO_TUNNEL === 'false') return;
  tunnel = await replicaTunnel(env, config);
  const directory = env.RUNTIME_DIR || '/run/runner';
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = directory + '/replica-ingress.yml';
  writeFileSync(file, 'ingress:\n' + [new URL(config.self.url).hostname, env.CONTROL_HOST]
    .map(host => '  - hostname: ' + JSON.stringify(host) + '\n    service: http://127.0.0.1:' + port + '\n').join('') +
    '  - service: http_status:404\n', { mode: 0o600 });
  connector = launch('cloudflared', ['tunnel', '--no-autoupdate', '--config', file, '--metrics',
    '127.0.0.1:' + (env.TUNNEL_METRICS_PORT || 49312), 'run'], { PATH: env.PATH, TUNNEL_TOKEN: tunnel.token }, 'tunnel');
}
async function tick() {
  if (busy || stopping) return;
  busy = true;
  try {
    if (!core && Date.now() >= coreRetry) {
      coreRetry = Date.now() + 10_000;
      core = launch(env.CP_RAFT_BINARY || '/usr/local/bin/runner-raft', [], env, 'voter');
    }
    if (!core) return;
    raft = await jsonRequest(raftOrigin + '/status');
    if (!raft.identityValid) throw new Error('member identity expired');
    identityWaitingUntil = 0;
    lastError = null;
    if (!startBusy && !connector && !['0', 'false'].includes(env.AUTO_TUNNEL)) {
      startBusy = true;
      startTunnel().catch(() => { lastError = 'replica tunnel unavailable'; }).finally(() => { startBusy = false; });
    }
    const transition = raft.leader ? config.cluster + ':' + raft.term + ':' + raft.leader : null;
    if (transition && transition !== lastTransition) {
      lastTransition = transition;
      tail.append('transition', { id: transition, at: Date.now(), term: raft.term, leader: raft.leader, observedBy: config.id });
    }
    if (raft.role !== 'StateLeader' || raft.leader !== config.id) { if (application || authority.current) demote('Raft leadership changed'); return; }
    if (Date.now() >= nextRenew) {
      const fence = await authority.renew(); nextRenew = Date.now() + 5000;
      if (!fence) { if (application) demote('cluster inactive or fenced'); return; }
    }
    if (!authority.current) return; // prepared but not activated
    if (!authority.live()) throw new Error('authority lease expired');
    if (!application) {
      const fence = await authority.check();
      application = launch(process.execPath, [fileURLToPath(new URL('./server.mjs', import.meta.url))], {
        ...env, PORT: String(childPort), BIND_HOST: '127.0.0.1', CP_HA_CHILD: '1',
        CP_HA_MANAGER: 'http://127.0.0.1:' + port, CP_HA_AUTHORITY: JSON.stringify(fence),
      }, 'scheduler');
      log('starting primary scheduler for term ' + fence.term);
    }
    const response = await fetch(childOrigin + '/healthz', { signal: AbortSignal.timeout(1000) }).catch(() => null);
    appReady = Boolean(response?.ok && (await response.json()).tunnelsReady);
    if (appReady && tunnel && Date.now() >= nextRoute) {
      nextRoute = Date.now() + 15_000;
      if (!routing) {
        routing = true;
        route(env, env.CONTROL_HOST, tunnel.id, 'runner control ' + env.CONTROL_HOST, undefined, () => authority.check())
          .catch(() => { lastError = 'primary hostname reconciliation unavailable'; })
          .finally(() => { routing = false; });
      }
    }
    lastError = null;
  } catch (error) {
    lastError = error.message;
    if (application || authority.current) demote('quorum or authority could not be confirmed');
  } finally { busy = false; }
}
async function pollPeers() {
  if (peersBusy || stopping) return;
  peersBusy = true;
  try {
    await Promise.allSettled(config.members.filter(member => member.id !== config.id).map(async member => {
      const view = await jsonRequest(origin(member) + '/_cp/status');
      if (view.id !== member.id) throw new Error('peer identity mismatch');
      peers.set(member.id, { at: Date.now(), view });
      for (const transition of view.transitions || []) tail.transition(transition);
    }));
    const primary = config.members.find(member => member.id === raft?.leader && member.id !== config.id);
    if (!primary) return;
    const cursor = cursors.get(primary.id) || { incarnation: null, sequence: 0 };
    let result = await jsonRequest(origin(primary) + '/_cp/tail?after=' + cursor.sequence);
    if (cursor.incarnation !== result.incarnation) result = await jsonRequest(origin(primary) + '/_cp/tail?after=0');
    for (const event of result.events) tail.append(event.kind, event.data, event.at);
    cursors.set(primary.id, { incarnation: result.incarnation, sequence: result.events.at(-1)?.sequence || result.sequence });
    if (result.gap) {
      const recovery = await jsonRequest(origin(primary) + '/_cp/recovery');
      for (const report of recovery.reports) tail.append('report', report.body, report.at);
    }
  } catch { /* A missing tail does not reduce Raft safety; agents retain signed inventory. */ }
  finally { peersBusy = false; }
}
async function shutdown(code = 0) {
  if (stopping) return; stopping = true;
  demote('replica stopping');
  for (const child of [...children]) if (child !== core && child !== connector) await stopChild(child);
  if (raft?.leader === config.id && core) {
    await jsonRequest(raftOrigin + '/transfer', { method: 'POST' }).catch(() => {});
    for (let i = 0; i < 20; i++) {
      const view = await jsonRequest(raftOrigin + '/status').catch(() => null);
      if (!view || view.leader !== config.id) break;
      await sleep(250);
    }
  }
  await stopChild(core);
  await stopChild(connector);
  server.close(); server.closeAllConnections();
  process.exit(code);
}
for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => shutdown());
const cluster = await database.query('SELECT * FROM cp_cluster WHERE id=1');
if (cluster.length !== 1 || cluster[0].cluster_id !== config.cluster || cluster[0].replicas !== config.count ||
    cluster[0].members !== JSON.stringify(config.members)) throw new Error('cluster must be explicitly prepared with this membership');
server.listen(port, env.BIND_HOST || '127.0.0.1', () => log('replica endpoints ready'));
setInterval(tick, 1000); setInterval(pollPeers, 3000);
await tick();
