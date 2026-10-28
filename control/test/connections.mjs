import assert from "node:assert/strict";
import { mkdtempSync, rmSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createSocket } from "node:dgram";
import vm from "node:vm";
import { VisitorBuffer, startVisitors } from "../../agent/runtime.mjs";
import { VisitorHistory } from "../src/connections.mjs";
import { Control } from "../src/index.js";
import { LocalStore } from "../src/local-store.mjs";

const now = Date.now(), run = { id: "run-one", machine: 1, seen: now };
const allowed = (app, replica) => app === "demo" && Number.isInteger(replica) && replica >= 1 && replica <= 2;
const observation = (ip, last = now, replica = 1) => ({ app: "demo", replica, ip, first: last, last });
const buffer = new VisitorBuffer({ limit: 2 });
buffer.observe("demo", 1, "198.51.100.1", now - 1000);
const first = buffer.payload(now);
buffer.observe("demo", 1, "198.51.100.1", now);
buffer.confirm(first);
assert.equal(buffer.payload(now).rows.length, 1, "a new observation survives acknowledgement of an older report");
buffer.confirm(buffer.payload(now));
assert.equal(buffer.payload(now).rows.length, 0, "acknowledged deltas are not resent");
buffer.observe("demo", 1, "127.0.0.1", now);
buffer.observe("demo", 1, "not-an-ip", now);
assert.equal(buffer.rows.size, 1, "health probes and malformed IPs are ignored");
buffer.observe("demo", 1, "::ffff:198.51.100.2", now);
assert.equal([...buffer.rows.values()][1].ip, "198.51.100.2");
buffer.observe("demo", 2, "2001:db8::1", now);
assert.equal(buffer.rows.size, 2);
assert.equal(buffer.dropped, 1);
assert.equal(buffer.payload(now + 3600_001).rows.length, 0, "inactive IPs expire");
console.log("ok agent deltas, IPv4/IPv6, health-probe exclusion, caps and expiry");

const history = new VisitorHistory({ limit: 3 });
history.take(run, { v: 1, rows: [observation("198.51.100.1"), observation("198.51.100.2", now - 120_000),
  observation("bad"), { ...observation("198.51.100.3"), app: "other" }, observation("198.51.100.3", now + 60_000)] }, now, allowed);
const projects = new Map([["demo", {}]]);
const query = (text = "", at = now) => history.query(new URLSearchParams(text), at, [run], projects, 90_000);
assert.equal(query().total, 1);
assert.equal(query("window=1h").total, 2);
assert.equal(query("window=1h&q=100.2").total, 1);
assert.equal(query("window=1h&replica=2").total, 0);
history.take({ ...run, id: "successor", machine: 2 }, { v: 1, rows: [observation("198.51.100.1")] }, now, allowed);
assert.equal(query().total, 1, "handover reports do not duplicate an IP on a replica");
history.take(run, { v: 1, rows: [observation("198.51.100.4", now, 2)] }, now, allowed);
assert.equal(history.rows.size, 3);
assert.equal(history.dropped, 1);
assert.throws(() => query("window=forever"), /choose/);
assert.throws(() => query("replica=NaN"), /choose/);
assert.equal(query("window=1h", now + 3600_001).total, 0);
console.log("ok controller windows, filters, retry/handover deduplication and bounded retention");

const collector = await startVisitors({ copyForHost: (host) => host === "demo-1.example.test" ? { app: "demo", replica: 1 } : null });
const sender = createSocket("udp4");
const send = (request) => new Promise((resolve, reject) => sender.send(Buffer.from(JSON.stringify({ request })), collector.port, "127.0.0.1", (e) => e ? reject(e) : resolve()));
await send({ host: "demo-1.example.test", remote_ip: "203.0.113.1", client_ip: "198.51.100.8" });
await send({ host: "wrong.example.test", remote_ip: "127.0.0.1", client_ip: "198.51.100.8" });
await send({ host: "demo-1.example.test", remote_ip: "127.0.0.1", client_ip: "198.51.100.8" });
await new Promise((resolve) => setTimeout(resolve, 30));
assert.deepEqual(collector.payload().rows.map((row) => row.ip), ["198.51.100.8"]);
collector.close(); sender.close();
console.log("ok UDP collection accepts only host-local, mapped replica observations");

const directory = mkdtempSync(join(tmpdir(), "runner-connections-"));
const store = new LocalStore(join(directory, "control.db"), { initialize: true });
try {
  const control = new Control({ sql: store, page: "", alarm() {}, restart() {}, env: {
    DOMAIN: "example.test", DNS: "off", ADMIN_PASSWORD: "test-admin", JOIN_TOKEN: "test-join", STATE_SIGNING_KEY: "test-key",
  } });
  store.protect(control);
  control.projects.set("demo", { name: "demo", version: 1, enabled: 1, replicas: 1 });
  control.visitors.take(run, { v: 1, rows: [observation("198.51.100.10")] }, now, allowed);
  const get = (path, headers = {}) => control.fetch(new Request(`https://cp.example.test${path}`, { headers }));
  for (const path of ["/api/connections", "/admin/api/connections"]) {
    assert.equal((await get(path)).status, 401);
    assert.equal((await get(path, { authorization: "Bearer test-join" })).status, 401, "join tokens cannot read visitor IPs");
    assert.equal((await get(path, { "x-app-password": "test-admin" })).status, 401);
    const response = await get(path, { "x-admin-password": "test-admin" });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("cache-control"), "no-store");
    assert.equal((await response.json()).rows[0].ip, "198.51.100.10");
  }
  assert.equal((await get("/api/connections?window=forever", { "x-admin-password": "test-admin" })).status, 400);
  assert.equal(JSON.stringify(control.status()).includes("198.51.100.10"), false);
  assert.equal(store.db.prepare("SELECT name FROM main.sqlite_master WHERE name LIKE '%visitor%' OR name LIKE '%connection%'").all().length, 0);
  console.log("ok real API requires admin credentials on both routes, disables caching and keeps IPs out of status/SQLite");
} finally { store.close(); rmSync(directory, { recursive: true, force: true }); }

const page = readFileSync(new URL("../src/app.html", import.meta.url), "utf8");
new vm.Script(page.match(/<script>([\s\S]*?)<\/script>/)[1]);
console.log("ok dashboard JavaScript parses");
