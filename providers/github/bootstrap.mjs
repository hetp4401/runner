import { providerConfig } from './config.mjs';
import { bootstrapManifest } from '../../control/cluster-init.mjs';

const config = providerConfig(process.env), action = process.argv[2] || 'status';
if (!config.enabled) throw new Error('a single provider has no consensus bootstrap');
async function request(member, path, body) {
  const response = await fetch((config.overrides[String(member.id)] || member.url) + path, {
    method: body ? 'POST' : 'GET', signal: AbortSignal.timeout(8000),
    headers: { 'x-cp-token': config.token, 'content-type': 'application/json' },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  if (!response.ok) throw new Error('replica ' + member.id + ': HTTP ' + response.status);
  return response.json();
}
const views = await Promise.all(config.members.map(m => request(m, '/_cp/raft-status').catch(() => null)));
if (action === 'status') console.log(JSON.stringify(views.map((v, i) => ({ id: config.members[i].id, reachable: Boolean(v), phase: v?.phase, leader: v?.leader }))));
else {
  if (action !== 'bootstrap' || !process.argv.includes('--confirm-old-cluster-stopped'))
    throw new Error('bootstrap requires --confirm-old-cluster-stopped');
  const manifest = bootstrapManifest(config, views);
  for (let i = 0; i < config.members.length; i++) if (views[i]) await request(config.members[i], '/_cp/bootstrap', manifest);
  console.log(JSON.stringify({ initialized: views.filter(Boolean).length, replicas: config.count, quorum: config.quorum, generation: manifest.generation }));
}
