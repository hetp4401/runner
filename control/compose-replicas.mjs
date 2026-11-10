// Paths and identities only; credentials remain in the private environment.
import { readFileSync, writeFileSync } from 'node:fs';
import { isAbsolute } from 'node:path';
import { isIP } from 'node:net';
const [input, output] = process.argv.slice(2);
if (!input || !output) throw new Error('usage: node control/compose-replicas.mjs local.json compose.json');
const config = JSON.parse(readFileSync(input, 'utf8'));
if (!isAbsolute(config.environment_file) || !config.image || !Array.isArray(config.members) || !config.members.length) throw new Error('environment_file, image and local members are required');
const ids = new Set(), ports = new Set();
if (config.dns !== undefined && (!Array.isArray(config.dns) || !config.dns.length || config.dns.some(address => !isIP(address)))) throw new Error('dns must contain resolver IP addresses');
for (const member of config.members) {
  if (!Number.isSafeInteger(member.id) || member.id < 1 || ids.has(member.id) ||
      !Number.isInteger(member.port) || member.port < 1 || member.port > 65535 || ports.has(member.port)) throw new Error('local member IDs and ports must be distinct');
  ids.add(member.id); ports.add(member.port);
}
const overrides = Object.fromEntries(config.members.map(m => [m.id, 'http://cp-' + m.id + ':8920']));
const services = {}, volumes = {};
for (const member of config.members) {
  const name = 'cp-' + member.id, volume = name + '-data';
  services[name] = {
    image: config.image, init: true, restart: 'unless-stopped', env_file: config.environment_file,
    environment: { CP_NODE_ID: String(member.id), CP_PEER_OVERRIDES: JSON.stringify(overrides) },
    volumes: [volume + ':/data'], tmpfs: ['/run/runner:uid=1000,gid=1000,mode=0700', '/tmp:uid=1000,gid=1000,mode=1777'],
    ports: ['127.0.0.1:' + member.port + ':8920'], stop_grace_period: '30s',
    ...(config.dns ? { dns: config.dns } : {}),
    security_opt: ['no-new-privileges:true'], cap_drop: ['ALL'],
    logging: { driver: 'json-file', options: { 'max-size': '10m', 'max-file': '3' } },
  };
  volumes[volume] = {};
}
writeFileSync(output, JSON.stringify({ services, volumes }, null, 2) + '\n', { mode: 0o600 });
console.log('Prepared ' + config.members.length + ' local replicas');
