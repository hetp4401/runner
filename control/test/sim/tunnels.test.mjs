import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { makeControl, Fleet, deploy, advance, MIN, T } from "./lib.mjs";
import { Control } from "../../src/index.js";
import { LocalStore } from "../../src/local-store.mjs";

process.umask(0o077);
const directory = mkdtempSync(join(tmpdir(), "runner-tunnels-"));
const filename = join(directory, "control.db");
const signingKey = "test-tunnel-recovery-key";
let checks = 0, store, observer;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const requests = [], batches = [];
let failPage = true, duplicate = false, failToken = false, tokenValue = "fresh-token";

try {
  const source = makeControl({ STATE_SIGNING_KEY: signingKey });
  const fleet = new Fleet(source, { test: { size: 2, cap: 2 } }, { drainAt: () => null });
  await fleet.startPool("test");
  deploy(source, "web", { replicas: 1, port: 8080, compose: 'services:\n  web:\n    image: nginx\n    ports: ["8080:80"]\n' });
  await fleet.run(15 * MIN);
  source.putSettings({ rebalance: false });
  const reports = fleet.alive().map((s) => {
    const body = { ...fleet.body(s), protocol: 2 };
    return { ...body, recovery: source.sync(body).recovery };
  });
  const cloud = new Map([...source.slots.values()].map((x) => [x.n, { name: `runner-${x.n}`, id: x.tunnel }]));
  source.db.prepare("VACUUM INTO ?").run(filename);
  source.db.close();
  let records = [];
  const cf = async (path, init = {}) => {
    requests.push({ path, method: init.method ?? "GET" });
    const url = new URL(path, "http://mock");
    if (url.pathname.endsWith("/token")) {
      if (failToken) throw new Error("token API unavailable");
      return { result: tokenValue };
    }
    if (url.pathname.endsWith("/cfd_tunnel")) {
      if (init.method === "POST") {
        const { name } = JSON.parse(init.body);
        const tunnel = { id: `created-${name}`, name };
        cloud.set(Number(name.slice(7)), tunnel);
        return { result: tunnel };
      }
      if (url.searchParams.has("name")) return { result: [...cloud.values(), { name: "runner-90", id: "unrelated-name-match" }] };
      const page = Number(url.searchParams.get("page"));
      if (page === 2 && failPage) throw new Error("second page unavailable");
      const rows = page === 1 ? [cloud.get(1), { name: "another-app", id: "not-ours" }]
        : [cloud.get(2), duplicate ? { name: "runner-1", id: "conflicting-tunnel" } : { name: "runner-999", id: "deleted", deleted_at: "2026-01-01" }];
      // Cloudflare's actual response has total_count, not total_pages.
      return { result: rows, result_info: { per_page: 2, total_count: 4 } };
    }
    if (init.method === "DELETE" && url.pathname.includes("/cfd_tunnel/")) {
      for (const [n, t] of cloud) if (url.pathname.endsWith(`/${t.id}`)) cloud.delete(n);
      return { result: {} };
    }
    if (url.pathname.endsWith("/dns_records/batch")) { batches.push(JSON.parse(init.body)); return { result: {} }; }
    if (url.pathname.endsWith("/dns_records")) return { result: records.filter((r) => !url.searchParams.has("content") || r.content === url.searchParams.get("content")), result_info: { total_pages: 1 } };
    throw new Error(`unexpected fake API call: ${path}`);
  };
  const open = () => {
    store = new LocalStore(filename);
    const control = new Control({ sql: store, env: { STATE_SIGNING_KEY: signingKey, JOIN_TOKEN: "test-join", DOMAIN: "sim.example", ACCOUNT_ID: "test-account", ZONE: "test-zone", RECOVER_RUNTIME: "1", RECOVER_TUNNELS: "1", RECOVERY_MS: "1000", POLL_S: "20" }, page: "", alarm() {}, restart() {} });
    store.protect(control);
    control.cf = cf;
    return control;
  };
  let control = open();
  observer = new DatabaseSync(filename, { readOnly: true });
  const diskVersion = observer.prepare("PRAGMA data_version").get().data_version;
  const walHash = () => createHash("sha256").update(readFileSync(`${filename}-wal`)).digest("hex");
  const beforeWal = walHash();
  advance(2000);
  for (const report of reports) assert.equal(control.sync(report).hold, true);
  await assert.rejects(control.join({ agent: "new-agent" }), /recovering/);
  await assert.rejects(control.retireSlot(1), /recovering/);
  await assert.rejects(control.syncDns(), /DNS changes are on hold/);
  check("tunnel discovery holds joins, retirement, plans and DNS beyond the inventory deadline", () => {
    assert.equal(control.recoveryActive(), true);
    assert.equal(control.copiesOf("web").length, 1);
    assert.equal(requests.length, 0);
  });

  await control.alarm();
  check("a failed listing page publishes no partial mappings and schedules a retry", () => {
    assert.equal(control.slots.size, 0);
    assert.equal(control.tunnelsReady, false);
    assert.match(control.tunnelRecoveryError, /second page unavailable/);
    assert.equal(control.dnsDue, T + MIN);
    assert.equal(batches.length, 0);
  });
  advance(MIN);
  failPage = false;
  await control.alarm();
  check("retry loads all pages and skips deleted or unrelated tunnels without requesting tokens", () => {
    assert.equal(control.tunnelsReady, true);
    assert.equal(control.tunnelRecoveryError, null);
    assert.deepEqual([...control.slots.keys()], [1, 2]);
    assert.ok([...control.slots.values()].every((s) => s.token === ""));
    assert.equal(requests.some((r) => r.path.endsWith("/token") || r.method !== "GET"), false);
    assert.equal(control.sync(reports[0]).hold, undefined);
  });

  const copy = control.copiesOf("web")[0];
  const destination = control.slots.get(copy.machine);
  const other = control.slots.get(copy.machine === 1 ? 2 : 1);
  records = [
    { id: "replica", type: "CNAME", name: "web-1.sim.example", content: `${other.tunnel}.cfargotunnel.com` },
    { id: "retired-app", type: "CNAME", name: "removed-1.sim.example", content: `${destination.tunnel}.cfargotunnel.com` },
    { id: "unrelated", type: "CNAME", name: "other.sim.example", content: "not-ours.cfargotunnel.com" },
  ];
  await control.syncDns();
  check("rediscovered IDs restore replica DNS and cleanup while preserving unrelated records", () => {
    assert.deepEqual(batches, [{ deletes: [{ id: "retired-app" }], patches: [{ id: "replica", content: `${destination.tunnel}.cfargotunnel.com` }] }]);
  });
  const beforeToken = requests.length;
  const [first, second] = await Promise.all([control.tunnelFor(copy.machine), control.tunnelFor(copy.machine)]);
  check("a replacement reuses its discovered tunnel and coalesces token requests in memory", () => {
    assert.equal(first, second);
    assert.equal(first.tunnel, destination.tunnel);
    assert.equal(first.token, tokenValue);
    assert.deepEqual(requests.slice(beforeToken), [{ path: `/accounts/test-account/cfd_tunnel/${destination.tunnel}/token`, method: "GET" }]);
  });
  const fresh = await control.tunnelFor(9);
  check("a genuinely new slot creates its own tunnel instead of using a partial name match", () => assert.equal(fresh.tunnel, "created-runner-9"));
  await control.retireSlot(9);
  check("retiring a new slot forgets its in-memory credentials", () => {
    assert.equal(control.slots.has(9), false);
    assert.equal(cloud.has(9), false);
  });
  check("discovery, token fetches, DNS and slot retirement make zero durable database writes", () => {
    assert.equal(observer.prepare("PRAGMA data_version").get().data_version, diskVersion);
    assert.equal(walHash(), beforeWal);
    assert.equal(observer.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='slots'").get().n, 0);
  });
  observer.close(); observer = null;
  store.close(); store = null;

  control = open();
  duplicate = true;
  await assert.rejects(control.recoverTunnels(), /multiple Cloudflare tunnels/);
  check("ambiguous tunnel names block recovery without restoring old cached credentials", () => {
    assert.equal(control.slots.size, 0);
    assert.equal(control.tunnelsReady, false);
    assert.equal(control.all("SELECT * FROM slots").length, 0);
  });
  duplicate = false;
  await control.recoverTunnels();
  failToken = true;
  await assert.rejects(control.tunnelFor(1), /token API unavailable/);
  assert.equal(control.slots.get(1).token, "");
  failToken = false;
  tokenValue = "rotated-token";
  const rotated = await control.tunnelFor(1);
  check("a restart fetches the current token and a failed fetch can be retried", () => {
    assert.equal(rotated.token, "rotated-token");
    assert.equal(rotated.tunnel, cloud.get(1).id);
  });
  console.log(`${checks} tunnel recovery checks passed`);
} finally {
  observer?.close();
  store?.close();
  rmSync(directory, { recursive: true, force: true });
}
