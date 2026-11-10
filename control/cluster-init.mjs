// Explicit recovery boundary for a completely volatile consensus cluster.
// Nothing in this tool, including its bootstrap manifest, is saved to storage.
import { randomBytes, randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { clusterConfig } from './src/ha/config.mjs';

export function bootstrapManifest(config, views, term = Date.now()) {
  const available = views.filter(view => view && config.members.some(member => member.id === view.id));
  if (new Set(available.map(view => view.id)).size < config.quorum) throw new Error('a quorum of reachable, fresh processes is required');
  if (available.some(view => view.generation || view.phase !== 'uninitialized')) throw new Error('cluster memory still exists; rejoin it instead of bootstrapping');
  if (!Number.isSafeInteger(term) || term < 1 || term > 2 ** 52) throw new Error('bootstrap term must be a safe positive integer');
  const used = new Set();
  const members = config.members.map(member => {
    const view = available.find(v => v.id === member.id);
    let node = view?.node;
    if (!view) do { node = randomBytes(6).readUIntBE(0, 6); } while (!node || used.has(node));
    if (!Number.isSafeInteger(node) || node < 1 || used.has(node) || (view && typeof view.boot !== 'string')) throw new Error('invalid or duplicate process identity');
    used.add(node);
    return { id: member.id, node, boot: view?.boot || randomUUID() };
  });
  return { generation: randomUUID(), term, members };
}

export async function initializeCluster(env, action, confirmed = false) {
  const members = JSON.parse(env.CP_MEMBERS || '[]');
  const config = clusterConfig({ ...env, CP_NODE_ID: env.CP_NODE_ID || String(members[0]?.id || '') });
  if (!config.enabled) throw new Error('single-controller mode has no consensus bootstrap');
  const overrides = JSON.parse(env.CP_PEER_OVERRIDES || '{}');
  async function request(member, path, body) {
    const response = await fetch((overrides[String(member.id)] || member.url) + path, {
      method: body ? 'POST' : 'GET', signal: AbortSignal.timeout(8000),
      headers: { 'x-cp-token': config.token, 'content-type': 'application/json' },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
    if (!response.ok) throw new Error('replica ' + member.id + ': HTTP ' + response.status);
    return response.json();
  }
  const views = await Promise.all(config.members.map(member => request(member, '/_cp/raft-status').catch(() => null)));
  if (action === 'status') return views.map((view, index) => ({ id: config.members[index].id, reachable: Boolean(view), phase: view?.phase, leader: view?.leader, term: view?.term }));
  if (action !== 'bootstrap') throw new Error('command must be status or bootstrap');
  if (!confirmed) throw new Error('bootstrap requires --confirm-old-cluster-stopped; never bootstrap across a partition');
  const manifest = bootstrapManifest(config, views, Number(env.CP_BOOTSTRAP_TERM || Date.now()));
  for (const member of config.members.filter((_, index) => views[index])) {
    await request(member, '/_cp/bootstrap', manifest);
  }
  return { cluster: config.cluster, generation: manifest.generation, replicas: config.count, quorum: config.quorum, initialized: views.filter(Boolean).length, storage: 'memory' };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  initializeCluster(process.env, process.argv[2] || 'status', process.argv.includes('--confirm-old-cluster-stopped'))
    .then(result => console.log(JSON.stringify(result)))
    .catch(error => { console.error(error.message); process.exitCode = 1; });
}
