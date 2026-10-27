// Run against a fresh wrangler dev with the test vars documented in test/README.
// No production credentials or real Cloudflare API are used.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";
import { ControlLink } from "../../agent/control-link.mjs";
import WebSocket from "ws";

const base = "http://127.0.0.1:8787";
const cloudflareOnly = process.argv.includes("--cloudflare");
const request = async (path, method = "GET", body, token = "test-admin") => {
  const response = await fetch(base + path, { method, headers: { "content-type": "application/json", "x-admin-password": token === "test-admin" ? token : "", authorization: `Bearer ${token}` }, body: body === undefined ? undefined : JSON.stringify(body) });
  const answer = await response.json();
  assert.ok(response.ok, `${path}: ${response.status} ${JSON.stringify(answer)}`);
  return answer;
};
const info = () => request("/internal/hybrid/status");
async function until(test, description, timeout = 15_000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) { const result = await test(); if (result) return result; await sleep(100); }
  throw new Error(`timed out: ${description}`);
}
let mac;
const agents = [];
let output = "";
function startMac() {
  mac = spawn(process.execPath, [new URL("../mac.mjs", import.meta.url).pathname], {
    env: { ...process.env, CONTROL_URL: base, MAC_TOKEN: "test-mac", HEARTBEAT_MS: "250" }, stdio: ["ignore", "pipe", "pipe"],
  });
  mac.stdout.on("data", (chunk) => { output += chunk; });
  mac.stderr.on("data", (chunk) => { output += chunk; });
}
try {
  await request("/internal/enable", "POST", { enabled: true }, "test-mac");
  await until(async () => !(await info()).macConnected, "previous Mac disconnected");
  if (cloudflareOnly) {
    const rejected = await new Promise((resolve, reject) => {
      const ws = new WebSocket(base.replace("http:", "ws:") + "/internal/mac", { headers: { authorization: "Bearer test-mac" }, handshakeTimeout: 5000 });
      ws.on("unexpected-response", (_, response) => { response.resume(); resolve(response.statusCode); ws.terminate(); });
      ws.on("open", () => { ws.close(); reject(new Error("external scheduler was accepted")); });
      ws.on("error", reject);
    });
    assert.equal(rejected, 403);
    assert.equal((await info()).mode, "cloudflare-v1");
    assert.equal((await info()).owner, "do");
    console.log("ok Cloudflare schedules directly and refuses an authenticated external scheduler");
  } else {
    startMac();
    await until(async () => (await info()).owner === "mac", "Mac becomes primary");
  }
  await until(async () => Date.now() >= (await info()).recoveryUntil, "initial recovery window");
  if (!cloudflareOnly) console.log("ok Mac acquires leadership after state transfer");
  const name = "hybrid-canary";
  const compose = "services:\n  app:\n    image: nginx:alpine\n    ports: ['8080:80']\n";
  await request(`/api/projects/${name}`, "PUT", { compose, port: 8080, replicas: 2 });
  await request(`/api/projects/${name}/env`, "PUT", { TOKEN: `integration-secret-${Date.now()}` });
  const before = await info();
  assert.ok(before.d1RowsWritten > 0);
  const suffix = Date.now();
  for (let i = 1; i <= 3; i++) {
    const agent = `integration-${suffix}-${i}`;
    const joined = await request("/api/join", "POST", { agent }, "test-node");
    const link = new ControlLink(base, "test-node", () => {});
    const body = { machine: joined.machine, agent, run: `${agent}-run`, started: Date.now(), ready: true, status: {}, protocol: 2, cpus: 4 };
    agents.push({ link, body });
    const plan = await link.report(body);
    body.recovery = plan.recovery;
    assert.ok(plan.recovery?.signature);
  }
  console.log("ok agent authentication and signed recovery receipts over WebSocket");
  const steady = await info();
  for (let i = 0; i < 30; i++) {
    const { link, body } = agents[i % agents.length];
    const plan = await link.report(body);
    body.recovery = plan.recovery;
  }
  const later = await info();
  assert.equal(later.d1RowsWritten, steady.d1RowsWritten);
  assert.equal(later.d1RowsRead, steady.d1RowsRead);
  console.log("ok routine reports perform zero D1 reads and writes");
  if (!cloudflareOnly) {
    const oldEpoch = later.epoch;
    mac.kill("SIGSTOP");
    await until(async () => (await info()).owner === "do", "DO lease takeover");
    assert.notEqual((await info()).epoch, oldEpoch);
    const fallbackPlan = await agents[0].link.report(agents[0].body);
    assert.ok(fallbackPlan.recovery?.signature);
    assert.equal((await request(`/api/projects/${name}`)).name, name);
    console.log("ok a sleeping Mac is fenced and DO continues serving the same configuration");
    mac.kill("SIGCONT");
    await until(async () => (await info()).owner === "mac", "returning Mac synchronizes and resumes", 20_000);
    assert.notEqual((await info()).epoch, oldEpoch);
    const resumed = await agents[1].link.report(agents[1].body);
    assert.ok(resumed.recovery?.signature);
    console.log("ok returning Mac reloads current state before getting a new generation");
    mac.kill("SIGTERM");
    await until(async () => (await info()).owner === "do", "disconnect takeover");
    console.log("ok clean Mac disconnect activates fallback immediately");
  } else {
    assert.equal(later.owner, "do");
    assert.equal(later.macConnected, false);
    assert.equal((await request(`/api/projects/${name}`)).name, name);
    console.log("ok configuration and agent plans are served entirely by Cloudflare");
  }
  const bad = new ControlLink(base, "wrong-token", () => {});
  await assert.rejects(bad.report(agents[0].body), /401/);
  console.log("ok unauthenticated agents are refused");
  for (const { link, body } of agents) body.recovery = (await link.report(body)).recovery;
  assert.ok(JSON.parse(agents[0].body.recovery.payload).fleet);
  const previous = await info();
  await request("/internal/enable", "POST", { enabled: false }, "test-mac");
  await request("/internal/reset", "POST", {}, "test-mac");
  await sleep(120);
  await until(async () => (await info()).epoch !== previous.epoch, "cold DO restart");
  await request("/internal/enable", "POST", { enabled: true }, "test-mac");
  for (const { link, body } of agents) await link.report(body);
  await until(async () => Date.now() >= (await info()).recoveryUntil, "recovery receipts collected");
  for (const { link, body } of agents) {
    const plan = await link.report(body);
    assert.ok(plan.recovery?.signature);
    assert.equal(plan.hold, undefined);
  }
  const cold = await info();
  assert.equal(cold.recoveryMissing.length, 0);
  assert.equal(cold.d1RowsWritten, 0);
  console.log("ok a cold DO rebuilds runtime from agents and credentials from D1 without writes");
} catch (error) { console.error(output); throw error; }
finally {
  mac?.kill("SIGCONT"); mac?.kill("SIGTERM");
  for (const { link } of agents) link.close();
}
