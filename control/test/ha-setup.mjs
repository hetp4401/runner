import assert from 'node:assert/strict';
import { fork, spawn } from 'node:child_process';
import { once } from 'node:events';
import { ClusterDatabase } from '../src/ha/database.mjs';
import { replicaTunnel, route } from '../src/ha/tunnel.mjs';
const fixture = fork(new URL('./d1-fixture.mjs', import.meta.url), { stdio: ['ignore', 'ignore', 'inherit', 'ipc'] });
const [{ port }] = await once(fixture, 'message');
const members = [1, 2, 3].map(id => ({ id, url: 'https://peer-' + id + '.example.com', label: 'Peer ' + id }));
const env = { ...process.env, CONFIG_STORAGE: 'd1', ACCOUNT_ID: 'test', D1_DATABASE_ID: 'test',
  D1_API_KEY: 'test-global-key', D1_API_EMAIL: 'test@example.com', D1_API_BASE: 'http://127.0.0.1:' + port,
  CP_REPLICAS: '3', CP_NODE_ID: '1', CP_CLUSTER_ID: 'test-cluster-generation',
  CP_CLUSTER_TOKEN: 'test-cluster-token-at-least-32-characters', CP_MEMBERS: JSON.stringify(members) };
const database = new ClusterDatabase(env);
async function command(action, config = env) {
  const child = spawn(process.execPath, [new URL('../cluster-init.mjs', import.meta.url).pathname, action], { env: config, stdio: 'ignore' });
  return (await once(child, 'exit'))[0];
}
try {
  assert.equal(await command('prepare'), 0);
  assert.notEqual(await command('activate'), 0);
  console.log('ok activation requires initialized durable voter identities');
  const addresses = members.map(member => ({ ...member, url: 'https://app-' + member.id + '.example.com' }));
  const updated = { ...env, CP_MEMBERS: JSON.stringify(addresses) };
  assert.equal(await command('update-addresses', updated), 0);
  const stored = (await database.query('SELECT members FROM cp_cluster'))[0].members;
  assert.equal(stored, updated.CP_MEMBERS);
  console.log('ok inactive address changes preserve the fixed voter identities');
  const other = addresses.map(member => member.id === 3 ? { ...member, id: 4 } : member);
  assert.notEqual(await command('update-addresses', { ...env, CP_MEMBERS: JSON.stringify(other) }), 0);
  await database.query('UPDATE cp_cluster SET active=1');
  assert.notEqual(await command('update-addresses', env), 0);
  assert.equal((await database.query('SELECT members FROM cp_cluster'))[0].members, stored);
  console.log('ok address maintenance cannot change voters or an active cluster');
  const calls = [];
  const api = async (_env, path, init = {}) => {
    calls.push({ path, method: init.method || 'GET' });
    if (path.includes('/cfd_tunnel?')) return [{ id: 'persistent-tunnel', name: 'control-cp-2', config_src: 'local' }];
    if (path.endsWith('/token')) return 'test-tunnel-token';
    if (path.includes('/dns_records')) return [];
    throw new Error('unexpected API request');
  };
  const tunnelEnv = { ACCOUNT_ID: 'test', ZONE: 'test', CONTROLLER_TUNNEL_NAME: 'control', CONTROL_HOST: 'runners.example.com', CP_PEER_TUNNEL: '0' };
  const config = { id: 2, cluster: env.CP_CLUSTER_ID, self: { url: 'https://app-2.example.com' } };
  const tunnel = await replicaTunnel(tunnelEnv, config, api);
  assert.equal(tunnel.id, 'persistent-tunnel');
  assert.equal(calls.some(call => call.path.includes('dns_records')), false);
  console.log('ok fleet peers keep ordinary app DNS and reuse their publication tunnel');
  calls.length = 0;
  await replicaTunnel({ ...tunnelEnv, CP_PEER_TUNNEL: '1' }, config, api);
  assert.equal(calls.filter(call => call.method === 'POST' && call.path.includes('dns_records')).length, 1);
  assert.equal(calls.some(call => call.method === 'POST' && call.path.includes('cfd_tunnel')), false);
  console.log('ok externally hosted peers reuse a persistent named tunnel for their domain');
  calls.length = 0;
  await assert.rejects(route(tunnelEnv, tunnelEnv.CONTROL_HOST, tunnel.id, 'runner control ' + tunnelEnv.CONTROL_HOST, api,
    async () => { throw new Error('leadership lost'); }), /leadership lost/);
  assert.equal(calls.some(call => call.method !== 'GET'), false);
  console.log('ok stale leadership cannot publish a primary DNS change');
  const migrated = [];
  const legacyAPI = async (_env, path, init = {}) => {
    if (path.includes('/cfd_tunnel?')) return [{ id: 'old-control', name: 'control' }];
    if (init.method === 'PATCH') { migrated.push(JSON.parse(init.body)); return {}; }
    return [{ id: 'dns', type: 'CNAME', content: 'old-control.cfargotunnel.com', comment: 'legacy control install' }];
  };
  await route(tunnelEnv, tunnelEnv.CONTROL_HOST, tunnel.id, 'runner control ' + tunnelEnv.CONTROL_HOST, legacyAPI);
  assert.equal(migrated[0].content, 'persistent-tunnel.cfargotunnel.com');
  await assert.rejects(route(tunnelEnv, 'someone.example.com', tunnel.id, 'other', legacyAPI), /owned by another/);
  console.log('ok first takeover can adopt only the configured legacy standalone tunnel');
} finally { fixture.kill('SIGTERM'); }
