import { timingSafeEqual } from 'node:crypto';

export function replicaCount(value = '1') {
  const text = String(value);
  const n = Number(text);
  if (!/^[1-9]\d*$/.test(text) || !Number.isSafeInteger(n) || (n !== 1 && (n < 3 || n % 2 === 0))) throw new Error('CP_REPLICAS must be 1, or an odd number of at least 3');
  return n;
}
export function clusterConfig(env) {
  const count = replicaCount(env.CP_REPLICAS ?? '1');
  if (count > 1 && env.CONFIG_STORAGE !== 'd1') throw new Error('replicated controllers require shared application configuration (CONFIG_STORAGE=d1)');
  return consensusConfig(env);
}
// Consensus itself has no application database dependency. Other replicated
// workers can reuse the voter and authority protocol with their own configuration.
export function consensusConfig(env) {
  const count = replicaCount(env.CP_REPLICAS ?? '1');
  if (count === 1) return { count, enabled: false };
  let members;
  try { members = JSON.parse(env.CP_MEMBERS); } catch { throw new Error('CP_MEMBERS must be a JSON array'); }
  if (!Array.isArray(members) || members.length !== count) throw new Error('CP_MEMBERS must contain CP_REPLICAS distinct members');
  const ids = new Set(), urls = new Set();
  for (const member of members) {
    if (!member || !Number.isSafeInteger(member.id) || member.id < 1 || ids.has(member.id)) throw new Error('member IDs must be distinct positive integers');
    let url;
    try { url = new URL(member.url); } catch { throw new Error('each member requires an HTTP(S) origin'); }
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash || url.pathname !== '/' || urls.has(url.origin)) throw new Error('member origins must be distinct HTTP(S) origins without paths or credentials');
    member.url = url.origin; member.label = String(member.label || `Controller ${member.id}`).slice(0, 80);
    ids.add(member.id); urls.add(url.origin);
  }
  members.sort((a, b) => a.id - b.id);
  const id = Number(env.CP_NODE_ID), self = members.find((m) => m.id === id);
  if (!self) throw new Error('CP_NODE_ID must identify one configured member');
  if (!/^[A-Za-z0-9_-]{16,100}$/.test(env.CP_CLUSTER_ID || '') || (env.CP_CLUSTER_TOKEN || '').length < 32) throw new Error('CP_CLUSTER_ID and a private CP_CLUSTER_TOKEN of at least 32 characters are required');
  return { count, enabled: true, id, self, members, quorum: Math.floor(count / 2) + 1, cluster: env.CP_CLUSTER_ID, token: env.CP_CLUSTER_TOKEN };
}
export function equalSecret(a, b) {
  const aa = Buffer.from(String(a || '')), bb = Buffer.from(String(b || ''));
  return aa.length === bb.length && aa.length > 0 && timingSafeEqual(aa, bb);
}
