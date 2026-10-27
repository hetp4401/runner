import assert from "node:assert/strict";
import http from "node:http";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, openSync, closeSync, rmSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";
import { WebSocket } from "ws";

const directory = mkdtempSync(join(tmpdir(), "runner-local-server-"));
const logPath = join(directory, "server.log");
const logFd = openSync(logPath, "a", 0o600);
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
let child, socket, seq = 0;
const tunnels = new Map();
let tokenRequests = 0, createdTunnels = 0;
const mock = http.createServer((req, res) => {
  let body = "";
  req.on("data", (chunk) => { body += chunk; });
  req.on("end", () => {
    const url = new URL(req.url, "http://mock");
    let result = [];
    if (url.pathname.endsWith("/cfd_tunnel")) {
      if (req.method === "POST") {
        const { name } = JSON.parse(body);
        result = { id: `test-tunnel-${++createdTunnels}`, name };
        tunnels.set(name, result);
      } else result = [...tunnels.values()].filter((t) => !url.searchParams.has("name") || t.name === url.searchParams.get("name"));
    } else if (url.pathname.endsWith("/token")) { tokenRequests++; result = "test-tunnel-token"; }
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify({ success: true, result, result_info: { total_pages: 1 } }));
  });
});
mock.listen(0, "127.0.0.1");
await once(mock, "listening");
const reservation = http.createServer();
reservation.listen(0, "127.0.0.1");
await once(reservation, "listening");
const port = reservation.address().port;
await new Promise((resolve) => reservation.close(resolve));
const origin = `http://127.0.0.1:${port}`;
const request = async (path, { method = "GET", body, node = false } = {}) => {
  const response = await fetch(`${origin}/api${path}`, { method, headers: { "content-type": "application/json", ...(node ? { authorization: "Bearer test-join" } : { "x-admin-password": "test-admin" }) }, body: body === undefined ? undefined : JSON.stringify(body), signal: AbortSignal.timeout(5000) });
  const data = await response.json();
  assert.ok(response.ok, `${method} ${path}: ${response.status} ${data.error ?? ""}`);
  return data;
};
async function start(initialize) {
  child = spawn(process.execPath, ["server.mjs"], {
    cwd: fileURLToPath(new URL("..", import.meta.url)), stdio: ["ignore", logFd, logFd],
    env: { ...process.env, PORT: String(port), DATA_DIR: directory, DOMAIN: "sim.example", DNS: "off", QUIET_MS: "0", POLL_S: "5", RECOVERY_MS: "1000", POOLS: '{"test":1}',
      INIT_CONTROL_DB: initialize ? "1" : "0", TRUSTED_PROXIES: "127.0.0.1/32", CLIENT_IP_HEADER: "x-forwarded-for", ADMIN_PASSWORD: "test-admin", JOIN_TOKEN: "test-join", STATE_SIGNING_KEY: "test-signing-secret", CF_API_TOKEN: "test-cf-token", ACCOUNT_ID: "test-account", CF_API_BASE: `http://127.0.0.1:${mock.address().port}` },
  });
  for (let i = 0; i < 100; i++) {
    if (child.exitCode != null) throw new Error(`server exited: ${readFileSync(logPath, "utf8")}`);
    try { await request("/status"); return; } catch { await pause(50); }
  }
  throw new Error("server did not listen");
}
async function stop(signal = "SIGTERM") {
  socket?.terminate(); socket = null;
  if (child && child.exitCode == null && child.signalCode == null) {
    const stopped = once(child, "exit");
    child.kill(signal);
    await stopped;
  }
}
async function connect() {
  socket = new WebSocket(`ws://127.0.0.1:${port}/api/agent`, { headers: { authorization: "Bearer test-join" } });
  socket.on("error", () => {});
  await once(socket, "open");
}
function sync(body) {
  const id = ++seq;
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { socket.off("message", receive); reject(new Error("plan timed out")); }, 5000);
    function receive(raw) {
      const message = JSON.parse(raw);
      if (message.id !== id) return;
      clearTimeout(timeout); socket.off("message", receive);
      if (message.type === "error") reject(new Error(message.error)); else resolve(message.plan);
    }
    socket.on("message", receive);
    socket.send(JSON.stringify({ type: "sync", id, body }));
  });
}

try {
  await start(true);
  assert.equal((await fetch(`${origin}/api/events?limit=1.5`)).status, 400);
  assert.equal((await fetch(`${origin}/healthz`)).status, 200);
  const passwordAttempt = (ip, password, extra = {}) => fetch(`${origin}/api/settings`, { method: "PUT",
    headers: { "content-type": "application/json", "x-forwarded-for": ip, "x-admin-password": password, ...extra }, body: "{}" });
  for (let i = 0; i < 5; i++) assert.equal((await passwordAttempt("198.51.100.10", "wrong")).status, 401);
  assert.equal((await passwordAttempt("198.51.100.11", "test-admin")).status, 200);
  assert.equal((await passwordAttempt("198.51.100.10", "test-admin", { "x-runner-client-ip": "198.51.100.12", "cf-connecting-ip": "198.51.100.13" })).status, 429);
  console.log("ok malformed queries stay request-local and trusted proxy clients have separate, unspoofable identities");
  const bundle = await (await fetch(`${origin}/agent/bundle.json`)).json();
  for (const [name, hash] of Object.entries(bundle.hashes)) assert.equal(createHash("sha256").update(bundle.files[name]).digest("hex"), hash);
  assert.equal(bundle.revision, createHash("sha256").update(JSON.stringify(Object.entries(bundle.hashes).sort(([a], [b]) => a.localeCompare(b)))).digest("hex"));
  const archive = Buffer.from(await (await fetch(`${origin}/agent/bundle.tar`)).arrayBuffer());
  assert.equal(createHash("sha256").update(archive).digest("hex"), (await (await fetch(`${origin}/agent/bundle.sha256`)).text()).trim());
  console.log("ok served agent image archive and offline code release match their content digests");
  await pause(1050);
  const agent = await request("/join", { method: "POST", node: true, body: { agent: "test-agent", pool: "test", poolSize: 1 } });
  for (const [name, port] of [["web", 8080], ["removed", 8081]]) {
    await request(`/projects/${name}`, { method: "PUT", body: { compose: `services:\n  web:\n    image: nginx\n    ports: ["${port}:80"]\n`, port, replicas: 1, env: { TOKEN: "test-app-secret" }, password: "test-app-password" } });
  }
  const started = Date.now();
  const body = { agent: "test-agent", machine: agent.machine, run: "test-run", pool: "test", poolSize: 1, started, protocol: 2, ready: true, status: {}, metrics: { live: { t: started, h: { cpu: 5, memUsed: 100, memTotal: 1000 }, a: {} } } };
  await connect();
  let plan = await sync(body);
  assert.equal(Object.keys(plan.desired).length, 2);
  body.status = Object.fromEntries(Object.entries(plan.desired).map(([key, p]) => [key, { v: p.v, s: "healthy", r: true, t0: started, n: 0 }]));
  body.recovery = plan.recovery;
  plan = await sync(body);
  body.recovery = plan.recovery;
  assert.ok(body.recovery.signature);
  console.log("ok HTTP deploy and WebSocket plans include durable secrets and recovery receipts");
  await request("/projects/web/env", { method: "PUT", body: { TOKEN: "updated-durable-secret" } });
  const changed = await request("/projects/web");
  await request("/projects/removed", { method: "DELETE" });
  await stop("SIGKILL");

  const disk = new DatabaseSync(join(directory, "control.db"), { readOnly: true });
  assert.equal(disk.prepare("SELECT value FROM app_env WHERE name='web' AND key='TOKEN'").get().value, "updated-durable-secret");
  assert.equal(disk.prepare("SELECT count(*) AS n FROM projects WHERE name='removed'").get().n, 0);
  assert.equal(disk.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name IN ('runs','copies','events','metrics_1m','slots')").get().n, 0);
  disk.close();
  console.log("ok SIGKILL preserves acknowledged configuration and no runtime tables");

  await start(false);
  const cold = await request("/status");
  assert.equal(cold.runs.length, 0);
  assert.equal(cold.controlPlane.runtimeStorage, "memory");
  await connect();
  plan = await sync(body);
  assert.equal(plan.hold, true);
  assert.equal(plan.desired, undefined);
  console.log("ok a cold controller holds workloads while recovering agent inventory");
  await pause(1100);
  const ready = await request("/status");
  assert.equal(ready.controlPlane.tunnelStorage, "memory");
  assert.equal(ready.controlPlane.tunnelsReady, true);
  assert.equal(ready.controlPlane.tunnelsDiscovered, 1);
  assert.equal(createdTunnels, 1);
  assert.equal(tokenRequests, 1);
  console.log("ok restart discovers the existing tunnel without recreating it or fetching its token");
  plan = await sync(body);
  assert.equal(plan.hold, undefined);
  assert.equal(plan.retire, false);
  assert.deepEqual(Object.keys(plan.desired), ["web"]);
  assert.equal(plan.desired.web.v, changed.version);
  assert.match(plan.desired.web.files[".env"], /updated-durable-secret/);
  console.log("ok recovery keeps the known replica, applies current config and removes the deleted app");

  const invalid = new WebSocket(`ws://127.0.0.1:${port}/api/agent`, { headers: { authorization: "Bearer wrong" } });
  invalid.on("error", () => {});
  await new Promise((resolve, reject) => {
    invalid.on("unexpected-response", (_req, res) => { try { assert.equal(res.statusCode, 401); res.resume(); invalid.terminate(); resolve(); } catch (e) { reject(e); } });
    invalid.on("open", () => { invalid.terminate(); reject(new Error("unauthenticated agent accepted")); });
  });
  console.log("ok agent authentication remains enforced");
  console.log("8 local server checks passed");
} finally {
  await stop();
  mock.closeAllConnections();
  await new Promise((resolve) => mock.close(resolve));
  closeSync(logFd);
  rmSync(directory, { recursive: true, force: true });
}
