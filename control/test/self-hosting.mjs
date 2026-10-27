import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, statSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";
import http from "node:http";
import { once } from "node:events";
import { settings, identity, initialize, resolveCloudflare, controllerTunnel, cloudflare } from "../src/bootstrap.mjs";
import { LocalStore } from "../src/local-store.mjs";
import { Control } from "../src/index.js";

process.umask(0o077);
const directory = mkdtempSync(join(tmpdir(), "runner-self-host-"));
let checks = 0, store;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const calls = [];
let tunnels = [], records = [], currentToken = "controller-token-1";
const api = async (_env, path, options = {}) => {
  calls.push({ path, method: options.method || "GET" });
  if (path.startsWith("/zones?")) return [{ name: "example.test", id: "zone", account: { id: "account" } }];
  if (path.endsWith("/token")) return currentToken;
  if (path.includes("/cfd_tunnel")) {
    if (options.method === "POST") { const t = { ...JSON.parse(options.body), id: "controller-id" }; tunnels.push(t); return t; }
    return tunnels;
  }
  if (path.includes("/dns_records")) {
    if (options.method === "POST") { records.push({ ...JSON.parse(options.body), id: "record" }); return records[0]; }
    if (options.method === "PATCH") { records[0] = { ...records[0], ...JSON.parse(options.body) }; return records[0]; }
    return records;
  }
  throw new Error(`unexpected API call ${path}`);
};
try {
  let env = settings({ DOMAIN: "example.test", DATA_DIR: directory });
  check("each controller gets its own tunnel prefix and an owner-controlled deployment policy", () => {
    assert.notEqual(env.TUNNEL_PREFIX, settings({ DOMAIN: "another.test" }).TUNNEL_PREFIX);
    assert.equal(env.REQUIRE_DEPLOY_AUTH, "1");
    assert.equal(settings({ DOMAIN: "example.test", TUNNEL_PREFIX: "runner", REQUIRE_DEPLOY_AUTH: "0", INIT_CONTROL_DB: "1" }).INIT_CONTROL_DB, undefined);
    for (const domain of ["https://example.test", "bad.test/", "x;touch.test", ""]) assert.throws(() => settings({ DOMAIN: domain }));
    assert.throws(() => settings({ DOMAIN: "example.test", CONTROL_HOST: "outside.test" }));
  });
  initialize(env);
  env = identity(env);
  check("initialization creates private, stable fleet credentials and refuses to overwrite data", () => {
    assert.equal(statSync(join(directory, "identity.json")).mode & 0o777, 0o600);
    assert.ok(env.ADMIN_PASSWORD.length >= 32 && env.JOIN_TOKEN.length >= 32 && env.STATE_SIGNING_KEY.length >= 32);
    assert.equal(identity(settings({ DOMAIN: "example.test", DATA_DIR: directory })).STATE_SIGNING_KEY, env.STATE_SIGNING_KEY);
    assert.throws(() => initialize(env), /will not overwrite/);
  });
  env = await resolveCloudflare(env, api);
  const beforeExplicit = calls.length;
  await resolveCloudflare(env, api);
  check("account and zone IDs are discovered once, or supplied explicitly", () => {
    assert.equal(env.ZONE, "zone"); assert.equal(env.ACCOUNT_ID, "account");
    assert.equal(calls.length, beforeExplicit);
  });
  assert.equal(await controllerTunnel(env, api), currentToken);
  const created = calls.filter((c) => c.method !== "GET").length;
  currentToken = "controller-token-2";
  assert.equal(await controllerTunnel(env, api), currentToken);
  check("controller startup reuses its tunnel and DNS, fetching the current token without saving it", () => {
    assert.equal(tunnels.length, 1); assert.equal(records.length, 1);
    assert.equal(calls.filter((c) => c.method !== "GET").length, created);
    assert.equal(records[0].name, "runners.example.test");
    assert.equal(readFileSync(join(directory, "identity.json"), "utf8").includes("controller-token"), false);
  });
  tunnels = [];
  records = [{ name: env.CONTROL_HOST, type: "A", content: "192.0.2.1" }];
  const beforeCollision = calls.length;
  await assert.rejects(controllerTunnel(env, api), /another DNS record/);
  check("an existing unrelated DNS record is never replaced and no orphan tunnel is created", () => assert.ok(calls.slice(beforeCollision).every((c) => c.method === "GET")));

  store = new LocalStore(join(directory, "control.db"));
  const files = new Map(["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs"].map((f) => [f, readFileSync(new URL(`../../agent/${f}`, import.meta.url), "utf8")]));
  const control = new Control({ sql: store, env: { ...env, RECOVER_TUNNELS: "1", RECOVERY_MS: "0" }, page: "", alarm() {}, restart() {}, agentFile: (name) => files.get(name) });
  store.protect(control);
  control.cf = async () => ({ result: [{ name: `${env.TUNNEL_PREFIX}-1`, id: "ours" }, { name: "runner-other-1", id: "theirs" }, { name: env.CONTROLLER_TUNNEL_NAME, id: "controller-id" }], result_info: { total_count: 3 } });
  await control.recoverTunnels();
  check("fleet discovery isolates the configured namespace and excludes the controller's own tunnel", () => assert.deepEqual([...control.slots].map(([n, s]) => [n, s.tunnel]), [[1, "ours"]]));
  const request = (route, method = "GET", body, headers = {}) => control.fetch(new Request(`https://spoofed.test${route}`, { method, headers: { "content-type": "application/json", ...headers }, body: body === undefined ? undefined : JSON.stringify(body) }));
  const spec = { compose: "services:\n  web:\n    image: nginx\n", replicas: 1 };
  assert.equal((await request("/api/projects/web", "PUT", spec)).status, 401);
  assert.equal((await request("/api/projects/web", "PUT", spec, { authorization: `Bearer ${env.JOIN_TOKEN}` })).status, 401);
  assert.equal((await request("/api/projects/web", "PUT", spec, { "x-admin-password": env.ADMIN_PASSWORD })).status, 200);
  assert.equal((await request("/api/projects/web", "DELETE")).status, 401);
  check("public visitors and agents cannot deploy or change apps in a new self-hosted fleet", () => assert.equal(control.projects.size, 1));
  assert.equal((await request("/api/projects/locked", "PUT", { ...spec, password: "app-password" }, { "x-admin-password": env.ADMIN_PASSWORD })).status, 200);
  assert.equal((await request("/api/projects/locked/env", "PUT", { TOKEN: "delegated-secret" }, { "x-app-password": "app-password" })).status, 200);
  check("app passwords can still delegate updates without sharing the admin password", () => assert.equal(control.appEnv.get("locked").get("TOKEN"), "delegated-secret"));
  for (const [name, content] of files) assert.equal(await (await request(`/agent/${name}`)).text(), content);
  assert.equal((await request("/agent/identity.json")).status, 404);
  const script = await (await request("/install.sh")).text();
  check("agents download the controller's bundled release, with no embedded credentials or upstream URL", () => {
    assert.ok(script.includes("https://runners.example.test/agent"));
    assert.equal(script.includes("githubusercontent.com"), false);
    assert.equal(script.includes(env.JOIN_TOKEN), false);
    assert.equal(script.includes("spoofed.test"), false);
    assert.equal(spawnSync("sh", ["-n"], { input: script }).status, 0);
  });
  check("only five configuration tables are persisted after bootstrapping and tunnel discovery", () => assert.deepEqual(store.db.prepare("SELECT name FROM main.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").all().map((r) => r.name), ["app_env", "app_passwords", "projects", "settings", "versions"]));

  const observed = [];
  const mock = http.createServer((req, res) => { observed.push(req.headers); res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ success: true, result: [] })); });
  mock.listen(0, "127.0.0.1"); await once(mock, "listening");
  try {
    const base = { CF_API_BASE: `http://127.0.0.1:${mock.address().port}` };
    await cloudflare({ ...base, CF_API_TOKEN: "scoped-token" }, "/zones");
    await cloudflare({ ...base, CF_API_EMAIL: "owner@example.test", CF_API_KEY: "global-key" }, "/zones");
    check("both scoped API tokens and a Global API Key plus email are supported", () => {
      assert.equal(observed[0].authorization, "Bearer scoped-token");
      assert.equal(observed[1]["x-auth-key"], "global-key");
      assert.equal(observed[1]["x-auth-email"], "owner@example.test");
      assert.equal(observed[1].authorization, undefined);
    });
  } finally { mock.closeAllConnections(); await new Promise((resolve) => mock.close(resolve)); }
  console.log(`${checks} self-hosting checks passed`);
} finally { store?.close(); rmSync(directory, { recursive: true, force: true }); }
