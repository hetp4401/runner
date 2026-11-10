import { randomBytes } from 'node:crypto';
import { cloudflare } from '../bootstrap.mjs';

export async function replicaTunnel(env, config, api = cloudflare) {
  const name = env.CONTROLLER_TUNNEL_NAME + '-cp-' + config.id;
  const account = '/accounts/' + env.ACCOUNT_ID + '/cfd_tunnel';
  const found = (await api(env, account + '?name=' + encodeURIComponent(name) + '&is_deleted=false'))
    .filter(t => t.name === name && !t.deleted_at);
  if (found.length > 1) throw new Error('multiple tunnels have this replica name');
  const tunnel = found[0] || await api(env, account, { method: 'POST', body: JSON.stringify({
    name, config_src: 'local', tunnel_secret: randomBytes(32).toString('base64'),
  }) });
  if (!tunnel.id || (tunnel.config_src && tunnel.config_src !== 'local')) throw new Error('replica tunnel must be locally managed');
  const token = await api(env, account + '/' + tunnel.id + '/token');
  if (typeof token !== 'string' || !token) throw new Error('replica tunnel has no token');
  const hostname = new URL(config.self.url).hostname;
  if (hostname === env.CONTROL_HOST) throw new Error('peer origin must differ from the primary hostname');
  await route(env, hostname, tunnel.id, 'runner peer ' + config.cluster + ' ' + config.id, api);
  return { id: tunnel.id, token };
}

export async function route(env, hostname, tunnel, comment, api = cloudflare, beforeWrite = async () => {}) {
  const path = '/zones/' + env.ZONE + '/dns_records';
  const records = await api(env, path + '?name=' + encodeURIComponent(hostname) + '&per_page=100');
  const content = tunnel + '.cfargotunnel.com';
  if (records.length > 1 || records.some(r => r.type !== 'CNAME' || (r.content !== content && r.comment !== comment))) {
    throw new Error('hostname is owned by another DNS record');
  }
  const record = { type: 'CNAME', name: hostname, content, proxied: true, comment };
  if (!records.length) {
    await beforeWrite();
    await api(env, path, { method: 'POST', body: JSON.stringify(record) });
  } else if (records[0].content !== content || !records[0].proxied) {
    await beforeWrite();
    await api(env, path + '/' + records[0].id, { method: 'PATCH', body: JSON.stringify(record) });
  }
}
