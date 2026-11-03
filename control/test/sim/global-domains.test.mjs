import assert from "node:assert/strict";
import { makeControl, Fleet, deploy, advance, MIN, T } from "./lib.mjs";

let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const c = makeControl({ CONTROL_HOST: "dashboard.sim.example" });
const fleet = new Fleet(c, { hosts: { size: 3, cap: 6 } }, { drainAt: () => null });
await fleet.startPool("hosts");
deploy(c, "web", { port: 8080, replicas: 2, compose: 'services:\n  app:\n    image: nginx\n    ports: ["8080:80"]\n' });
await fleet.run(15 * MIN);
c.putSettings({ rebalance: false });
check("public deployments always expose a global URL and keep numbered addresses", () => {
  assert.equal(c.getProject("web").globalUrl, "https://web.sim.example/");
  assert.equal(c.getProject("web").replicas, 2);
  assert.deepEqual([...c.dnsTargets(T).keys()].sort(), ["web-1", "web-2"]);
});
check("routing is not configurable through JSON, Compose, trusted deploys or partial updates", () => {
  for (const shared of [false, true, "false", null]) {
    assert.throws(() => c.putProject("web", { routing: { shared } }, { trusted: true }), /addresses are automatic/);
    assert.throws(() => c.putProject("option", { port: 8080,
      compose: `x-runner:\n  routing: ${JSON.stringify({ shared })}\nservices:\n  app:\n    image: nginx\n` }), /addresses are automatic/);
  }
  assert.equal(c.getProject("web").version, 1);
});
check("background jobs have no public URL and public apps retain the 50-replica limit", () => {
  deploy(c, "worker", { compose: "services:\n  app:\n    image: busybox\n" });
  assert.equal(c.getProject("worker").globalUrl, null);
  const spec = c.putProject("large", { dockerfile: "FROM nginx", port: 8081, replicas: 50 });
  assert.equal(spec.globalUrl, "https://large.sim.example/");
  assert.equal(spec.replicas, 50);
  c.deleteProject("large");
});
check("control hostnames cannot be claimed by new apps", () => {
  for (const name of ["runners", "control", "dashboard"]) assert.throws(() => c.putProject(name, { dockerfile: "FROM nginx", port: 80 }), /reserved/);
});

const body = fleet.body.bind(fleet);
fleet.body = (s, extra = {}) => body(s, { protocol: 4, ...extra });
for (const s of fleet.alive()) await fleet.sync(s);
const copies = c.copiesOf("web").sort((a, b) => a.replica - b.replica);
const first = copies[0], second = copies[1];
const run = (x) => c.liveMachines(T).get(x.machine);
const report = (x) => run(x).status[c.keyOn("web", x.machine, x.replica)];
const tunnel = (x) => c.slots.get(x.machine).tunnel;
const target = () => c.dnsTargets(T).get("web");
check("updated agents automatically enable the global route through an existing machine tunnel", () => assert.equal(target(), tunnel(first)));

let records = [], batches = [], serial = 0;
c.cf = async (path, init = {}) => {
  if (path.endsWith("/dns_records/batch")) {
    const batch = JSON.parse(init.body); batches.push(batch);
    for (const row of batch.deletes ?? []) records = records.filter((r) => r.id !== row.id);
    for (const row of batch.patches ?? []) Object.assign(records.find((r) => r.id === row.id), row);
    for (const row of batch.posts ?? []) records.push({ id: `record-${++serial}`, ...row });
    return { result: {} };
  }
  assert.match(path, /\/dns_records\?/); // No app tunnel creation or token fetches.
  return { result: structuredClone(records), result_info: { total_pages: 1 } };
};
const record = (label) => records.find((r) => r.name === `${label}.sim.example`);
await c.syncDns();
check("DNS creates both global and replica CNAMEs automatically", () => {
  assert.equal(records.length, 3);
  assert.equal(record("web").content, `${tunnel(first)}.cfargotunnel.com`);
  assert.equal(record("web-1").content, `${tunnel(first)}.cfargotunnel.com`);
  assert.equal(record("web-2").content, `${tunnel(second)}.cfargotunnel.com`);
  assert.ok(records.every((r) => r.proxied));
});
const count = batches.length;
await c.syncDns();
check("unchanged routing makes no duplicate DNS records or writes", () => assert.equal(batches.length, count));

for (const [label, change, restore] of [
  ["not ready", () => report(first).r = false, () => delete report(first).r],
  ["failed", () => report(first).s = "failed", () => report(first).s = "healthy"],
  ["disconnected", () => run(first).edges = 0, () => delete run(first).edges],
  ["starting", () => run(first).ready = 0, () => run(first).ready = 1],
  ["legacy", () => run(first).protocol = 3, () => run(first).protocol = 4],
]) {
  change();
  await c.syncDns();
  check(`the global address fails over when its copy is ${label}, retaining both replica names`, () => {
    assert.equal(record("web").content, `${tunnel(second)}.cfargotunnel.com`);
    assert.ok(record("web-1") && record("web-2"));
  });
  restore();
}

const predecessor = { ...run(first), id: "old-connector", protocol: 3, started: T - 20 * MIN, ready: 1, retire: 1 };
c.runs.set(predecessor.id, predecessor);
check("a retiring legacy connector sharing a slot blocks global traffic until its liveness expires", () => {
  assert.equal(target(), tunnel(second));
  predecessor.seen = T - c.liveMs - 1;
  assert.equal(target(), tunnel(first));
});
c.runs.delete(predecessor.id);

const spare = fleet.alive().find((s) => !copies.some((x) => x.machine === s.machine));
c.move("web", first.machine, spare.machine, first.replica);
report(second).r = false; // Isolate the moving replica's handover.
check("a global address keeps the healthy old copy while a move builds", () => assert.equal(target(), tunnel(first)));
await fleet.run(3 * MIN);
check("a completed move repoints the global and numbered address together", () => {
  const copy = c.copiesOf("web").find((x) => x.replica === first.replica && !x.leaving);
  assert.equal(copy.machine, spare.machine);
  assert.equal(target(), tunnel(copy));
  assert.equal(c.dnsTargets(T).get("web-1"), tunnel(copy));
});
await c.syncDns();

record("web").content = "external-router.cfargotunnel.com";
await c.syncDns();
check("an external global DNS record remains intact and its conflict is visible", () => {
  assert.equal(record("web").content, "external-router.cfargotunnel.com");
  assert.match(c.dnsNotes.join("\n"), /web.sim.example is already used/);
});
record("web").content = `${target()}.cfargotunnel.com`;
for (const r of c.runs.values()) r.ready = 0;
await c.syncDns();
check("a temporary loss of healthy global targets preserves the hostname for recovery", () => assert.ok(record("web")));
c.copies.clear();
c.runs.clear();
await c.syncDns();
check("cold inventory recovery preserves existing global and numbered records", () => assert.equal(records.length, 3));
c.deleteProject("web");
await c.syncDns();
check("deleting an app removes its owned global and numbered records", () => assert.equal(records.length, 0));
c.db.close();
console.log(`${checks} automatic global-domain checks passed`);
