import { consensusConfig } from '../../control/src/ha/config.mjs';

export function providerConfig(env) {
  const id = env.NODE_ID || String(Number(env.FLEET_REPLICA || 1) + Number(env.NODE_ID_OFFSET || 0));
  const raftEnv = {
    CP_REPLICAS: env.REPLICAS || '1', CP_NODE_ID: id, CP_MEMBERS: env.MEMBERS,
    CP_CLUSTER_ID: env.CLUSTER_ID, CP_CLUSTER_TOKEN: env.CLUSTER_TOKEN,
    CP_PEER_OVERRIDES: env.PEER_OVERRIDES || '{}', CP_RAFT_ALLOW_CLAIMS: '1',
    CP_RAFT_PORT: env.RAFT_PORT || '8931',
    CP_RAFT_TICK_MS: env.RAFT_TICK_MS || '500', CP_RAFT_ELECTION_TICKS: env.RAFT_ELECTION_TICKS || '20',
    CP_RAFT_HEARTBEAT_TICKS: env.RAFT_HEARTBEAT_TICKS || '2',
  };
  const config = consensusConfig(raftEnv);
  let providers;
  try { providers = JSON.parse(env.GITHUB_PROVIDERS || '[]'); } catch { throw new Error('GITHUB_PROVIDERS must be JSON'); }
  if (!Array.isArray(providers) || providers.length > 50) throw new Error('configure at most 50 providers');
  const seen = new Set();
  for (const provider of providers) {
    if (!provider || !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(provider.repo || '') ||
        typeof provider.token !== 'string' || !provider.token || seen.has(provider.repo.toLowerCase()))
      throw new Error('each provider requires a distinct repository and private token');
    seen.add(provider.repo.toLowerCase());
  }
  return { ...config, id: Number(id), raftEnv, providers,
    port: Number(env.PORT || 8924), host: env.BIND_HOST || '0.0.0.0',
    raftOrigin: 'http://127.0.0.1:' + raftEnv.CP_RAFT_PORT,
    overrides: JSON.parse(raftEnv.CP_PEER_OVERRIDES),
  };
}
