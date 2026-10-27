import { createHash, randomBytes } from "node:crypto";
import { closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Control } from "./index.js";
import { LocalStore } from "./local-store.mjs";

const IDENTITY_KEYS = ["ADMIN_PASSWORD", "JOIN_TOKEN", "STATE_SIGNING_KEY"];
const NAMESPACE_KEYS = ["TUNNEL_PREFIX", "CONTROLLER_TUNNEL_NAME"];
const hostname = (value, label) => {
  const text = String(value || "").toLowerCase().replace(/\.$/, "");
  if (text.length > 253 || !/^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(text)) throw new Error(`${label} must be a DNS hostname, without a scheme or path`);
  return text;
};

function flag(value, name, fallback) {
  if (value === undefined) return fallback;
  if (value === "1" || value === "true") return "1";
  if (value === "0" || value === "false") return "0";
  throw new Error(`${name} must be 1/true or 0/false; an empty value is not a default`);
}

function namespace(env, saved = {}) {
  const suffix = createHash("sha256").update(env.CONTROL_HOST).digest("hex").slice(0, 12);
  const defaults = { TUNNEL_PREFIX: `runner-${suffix}`, CONTROLLER_TUNNEL_NAME: `control-${suffix}` };
  const out = {};
  for (const key of NAMESPACE_KEYS) {
    if (Object.hasOwn(saved, key) && (typeof saved[key] !== "string" || !saved[key])) throw new Error(`invalid ${key} in saved fleet identity`);
    if (saved[key] && env[key] && saved[key] !== env[key]) throw new Error(`${key} conflicts with the saved fleet identity; restore the original setting instead of adopting another fleet`);
    out[key] = saved[key] || env[key] || defaults[key];
  }
  if (typeof out.TUNNEL_PREFIX !== "string" || !/^[a-z0-9][a-z0-9-]{0,62}$/.test(out.TUNNEL_PREFIX)) throw new Error("invalid TUNNEL_PREFIX");
  if (typeof out.CONTROLLER_TUNNEL_NAME !== "string" || !/^[a-z0-9][a-z0-9-]{0,99}$/.test(out.CONTROLLER_TUNNEL_NAME)) throw new Error("invalid CONTROLLER_TUNNEL_NAME");
  if (out.CONTROLLER_TUNNEL_NAME.startsWith(`${out.TUNNEL_PREFIX}-`) && /^[1-9]\d*$/.test(out.CONTROLLER_TUNNEL_NAME.slice(out.TUNNEL_PREFIX.length + 1))) throw new Error("the controller tunnel name must not overlap the agent tunnel namespace");
  return out;
}

function readIdentity(env) {
  const file = join(env.DATA_DIR, "identity.json");
  const saved = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {};
  if (!saved || typeof saved !== "object" || Array.isArray(saved)) throw new Error("identity.json must contain a fleet identity object");
  return saved;
}

function saveIdentity(env, saved) {
  const file = join(env.DATA_DIR, "identity.json");
  const temporary = join(env.DATA_DIR, `.identity-${randomBytes(12).toString("hex")}.tmp`);
  let fd;
  try {
    fd = openSync(temporary, "wx", 0o600);
    writeFileSync(fd, JSON.stringify(saved) + "\n");
    fsyncSync(fd);
    closeSync(fd); fd = undefined;
    renameSync(temporary, file);
    fd = openSync(env.DATA_DIR, "r");
    fsyncSync(fd);
  } finally {
    if (fd !== undefined) closeSync(fd);
    if (existsSync(temporary)) unlinkSync(temporary);
  }
}

export function settings(input) {
  const env = { ...input };
  env.DOMAIN = hostname(env.DOMAIN, "DOMAIN");
  env.CONTROL_HOST = hostname(env.CONTROL_HOST || `runners.${env.DOMAIN}`, "CONTROL_HOST");
  if (!env.CONTROL_HOST.endsWith(`.${env.DOMAIN}`)) throw new Error("CONTROL_HOST must be a subdomain of DOMAIN");
  // Defaults are resolved with identity(), so changing the public hostname never
  // changes a previously initialized fleet's tunnel namespace.
  namespace(env);
  env.PUBLIC_URL ||= `https://${env.CONTROL_HOST}`;
  const publicUrl = new URL(env.PUBLIC_URL);
  if (!["https:", "http:"].includes(publicUrl.protocol) || publicUrl.username || publicUrl.password || publicUrl.search || publicUrl.hash || publicUrl.pathname !== "/") throw new Error("PUBLIC_URL must be an HTTP(S) origin without credentials or a path");
  env.PUBLIC_URL = publicUrl.origin;
  env.PORT ||= "8920";
  if (!/^\d+$/.test(env.PORT) || Number(env.PORT) < 1 || Number(env.PORT) > 65535) throw new Error("invalid PORT");
  env.TUNNEL_METRICS_PORT ||= "49312";
  if (!/^\d+$/.test(env.TUNNEL_METRICS_PORT) || Number(env.TUNNEL_METRICS_PORT) < 1 || Number(env.TUNNEL_METRICS_PORT) > 65535 || Number(env.TUNNEL_METRICS_PORT) === Number(env.PORT)) throw new Error("invalid TUNNEL_METRICS_PORT");
  env.DATA_DIR ||= "/data";
  env.BIND_HOST ||= "0.0.0.0";
  env.POOLS ||= "{}";
  env.CONTROLLER_NAME ||= env.CONTROL_HOST;
  env.REQUIRE_DEPLOY_AUTH = flag(env.REQUIRE_DEPLOY_AUTH, "REQUIRE_DEPLOY_AUTH", "1");
  env.AUTO_TUNNEL = flag(env.AUTO_TUNNEL, "AUTO_TUNNEL", "1");
  // A missing volume must never silently initialize an empty fleet on restart.
  delete env.INIT_CONTROL_DB;
  return env;
}

export function identity(env, { required = true, persistNamespace = false } = {}) {
  const saved = readIdentity(env);
  const names = namespace(env, saved);
  const out = { ...env, ...names };
  for (const key of IDENTITY_KEYS) {
    out[key] ||= saved[key];
    if (required && (typeof out[key] !== "string" || !out[key])) throw new Error(`${key} is missing; initialize a new fleet or restore its identity.json`);
  }
  // Existing environment-only installations save only the namespace here; their
  // secrets continue to come from the operator's environment file.
  if (persistNamespace && NAMESPACE_KEYS.some((key) => saved[key] !== names[key])) saveIdentity(env, { ...saved, ...names });
  return out;
}

export function initialize(env) {
  const database = join(env.DATA_DIR, "control.db");
  const file = join(env.DATA_DIR, "identity.json");
  if (existsSync(database) || existsSync(file)) throw new Error("fleet data already exists; init will not overwrite it");
  mkdirSync(env.DATA_DIR, { recursive: true, mode: 0o700 });
  const saved = { ...Object.fromEntries(IDENTITY_KEYS.map((key) => [key, env[key] || randomBytes(32).toString("base64url")])), ...namespace(env) };
  saveIdentity(env, saved);
  const sql = new LocalStore(database, { initialize: true });
  try {
    const control = new Control({ sql, env: { ...env, ...saved }, page: "", alarm() {}, restart() {} });
    sql.protect(control);
  } finally { sql.close(); }
}

export async function cloudflare(env, path, init = {}) {
  const auth = env.CF_API_TOKEN ? { authorization: `Bearer ${env.CF_API_TOKEN}` }
    : env.CF_API_KEY && env.CF_API_EMAIL ? { "x-auth-key": env.CF_API_KEY, "x-auth-email": env.CF_API_EMAIL } : null;
  if (!auth) throw new Error("set CF_API_TOKEN, or both CF_API_KEY and CF_API_EMAIL");
  const response = await fetch(`${env.CF_API_BASE || "https://api.cloudflare.com/client/v4"}${path}`, {
    signal: AbortSignal.timeout(30_000), ...init, headers: { ...auth, "content-type": "application/json" },
  });
  const data = await response.json();
  if (!response.ok || !data.success) throw new Error(`Cloudflare ${init.method || "GET"} ${path.split("?")[0]} failed (${response.status}); check account permissions and configuration`);
  return data.result;
}

export async function resolveCloudflare(env, api = cloudflare) {
  if (env.ACCOUNT_ID && env.ZONE) return env;
  const zones = await api(env, `/zones?name=${encodeURIComponent(env.DOMAIN)}&status=active&per_page=50`);
  const matches = zones.filter((zone) => zone.name === env.DOMAIN && (!env.ACCOUNT_ID || zone.account?.id === env.ACCOUNT_ID));
  if (matches.length !== 1) throw new Error("DOMAIN must identify one active Cloudflare zone; grant Zone Read or provide ACCOUNT_ID and ZONE explicitly");
  if (env.ZONE && env.ZONE !== matches[0].id) throw new Error("ZONE does not match DOMAIN");
  return { ...env, ACCOUNT_ID: matches[0].account.id, ZONE: matches[0].id };
}

// Only the control hostname is managed here. Fleet tunnel identities and tokens
// are independently rediscovered by the scheduler and never saved on disk.
export async function controllerTunnel(env, api = cloudflare) {
  const account = `/accounts/${env.ACCOUNT_ID}/cfd_tunnel`;
  const matches = (await api(env, `${account}?name=${encodeURIComponent(env.CONTROLLER_TUNNEL_NAME)}&is_deleted=false`)).filter((t) => t.name === env.CONTROLLER_TUNNEL_NAME && !t.deleted_at);
  if (matches.length > 1) throw new Error("multiple tunnels have the configured controller tunnel name");
  let tunnel = matches[0];
  const path = `/zones/${env.ZONE}/dns_records`;
  const records = await api(env, `${path}?name=${encodeURIComponent(env.CONTROL_HOST)}&per_page=100`);
  const comment = `runner control ${env.CONTROL_HOST}`;
  if (records.length > 1 || records.some((r) => r.type !== "CNAME" || (r.content !== `${tunnel?.id}.cfargotunnel.com` && r.comment !== comment))) throw new Error(`${env.CONTROL_HOST} already belongs to another DNS record; choose an unused control hostname`);
  if (!tunnel) tunnel = await api(env, account, { method: "POST", body: JSON.stringify({ name: env.CONTROLLER_TUNNEL_NAME, config_src: "local", tunnel_secret: randomBytes(32).toString("base64") }) });
  if (!tunnel.id || (tunnel.config_src && tunnel.config_src !== "local")) throw new Error("the control tunnel must be locally managed");
  const token = await api(env, `${account}/${tunnel.id}/token`);
  if (typeof token !== "string" || !token) throw new Error("Cloudflare returned no controller tunnel token");
  const record = { type: "CNAME", name: env.CONTROL_HOST, content: `${tunnel.id}.cfargotunnel.com`, proxied: true, comment };
  if (!records.length) await api(env, path, { method: "POST", body: JSON.stringify(record) });
  else if (records[0].content !== record.content || !records[0].proxied) await api(env, `${path}/${records[0].id}`, { method: "PATCH", body: JSON.stringify(record) });
  return token;
}
