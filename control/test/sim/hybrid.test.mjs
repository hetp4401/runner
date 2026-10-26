import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { makeControl, Fleet, deploy, advance, MIN, T } from "./lib.mjs";
import { WorkingStore, CONFIG_TABLES } from "../../src/hybrid/store.js";
import { Configuration } from "../../src/hybrid/config.js";
import { capture, restore, difference, applyDifference } from "../../src/hybrid/codec.js";

let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const secret = "receipt-test-secret";
const source = makeControl({ STATE_SIGNING_KEY: secret });
const fleet = new Fleet(source, { test: { size: 3, cap: 3 } }, { drainAt: () => null });
await fleet.startPool("test");
await fleet.run(5 * MIN);
deploy(source, "web", { replicas: 2, port: 8080, compose: 'services:\n  web:\n    image: nginx\n    ports: ["8080:8080"]\n' });
await fleet.run(15 * MIN);

const original = source.copiesOf("web").find((x) => x.replica === 1);
const destination = fleet.alive().find((s) => !source.copiesOn("web", s.machine).length);
source.move("web", original.machine, String(destination.machine), 1);
const reports = [];
for (const server of fleet.alive()) {
  const plan = await fleet.sync(server);
  reports.push({ ...fleet.body(server), protocol: 2, recovery: plan.recovery });
}
const placements = JSON.stringify(source.copiesOf("web").map((x) => [x.machine, x.replica, x.key, x.leaving]).sort());
reports[0].recovery = source.sync({ ...reports[0], protocol: 2 }).recovery;
check("a healthy witness stores fleet scheduling state without credentials or source", () => {
  const receipt = JSON.parse(reports[0].recovery.payload);
  assert.ok(receipt.fleet.runs.length >= 3);
  assert.equal(receipt.fleet.slots, undefined);
  assert.equal(receipt.fleet.versions, undefined);
  assert.equal(receipt.fleet.app_env, undefined);
});
const db = new DatabaseSync(":memory:");
let recovered = makeControl({}, db);
const store = new WorkingStore((q, a) => db.prepare(q).all(...a));
store.load(Object.fromEntries(CONFIG_TABLES.map((table) => [table, source.all(`SELECT * FROM ${table}`)])));
recovered = makeControl({ STATE_SIGNING_KEY: secret, RECOVER_RUNTIME: "1", RECOVERY_MS: "180000" }, db);
for (const body of reports) {
  const plan = recovered.sync(body);
  check("recovery holds commands until inventory has arrived", () => assert.equal(plan.hold, true));
}
check("cold recovery restores an in-progress move including the serving source", () => assert.equal(JSON.stringify(recovered.copiesOf("web").map((x) => [x.machine, x.replica, x.key, x.leaving]).sort()), placements));
check("cold recovery retains original run identities and machine deadlines", () => {
  for (const report of reports) assert.equal(recovered.runs.get(report.run).started, report.started);
});
check("a tampered receipt is refused", () => assert.throws(() => recovered.sync({ ...reports[0], recovery: { ...reports[0].recovery, payload: reports[0].recovery.payload + " " } }), /invalid recovery/));
check("another agent cannot reuse a signed receipt", () => assert.throws(() => recovered.sync({ ...reports[0], agent: "someone-else" }), /does not identify/));
const mixedDb = new DatabaseSync(":memory:");
makeControl({}, mixedDb);
const mixedStore = new WorkingStore((q, a) => mixedDb.prepare(q).all(...a));
mixedStore.load(Object.fromEntries(CONFIG_TABLES.map((table) => [table, source.all(`SELECT * FROM ${table}`)])));
const mixed = makeControl({ STATE_SIGNING_KEY: secret, RECOVER_RUNTIME: "1" }, mixedDb);
const legacy = { ...reports.find((r) => r !== reports[0] && Object.keys(r.status).length), protocol: 1, recovery: null };
check("a legacy agent receives an error instead of an incompatible hold plan", () => assert.throws(() => mixed.sync(legacy), /reconstructing/));
mixed.sync(reports[0]);
check("one witness recovers legacy agents and clears the missing-receipt gate", () => {
  assert.equal(mixed.recoveryMissing.size, 0);
  assert.equal(mixed.runs.get(legacy.run).seen, T);
  assert.equal(JSON.stringify(mixed.copiesOf("web").map((x) => [x.machine, x.replica, x.key, x.leaving]).sort()), placements);
});
advance(181_000);
check("legacy agents resume their existing desired state after reconstruction", () => assert.ok(mixed.sync(legacy).desired));
check("recovery resumes serving the existing placement", () => {
  const plan = recovered.sync(reports[0]);
  assert.equal(plan.hold, undefined);
  assert.equal(plan.retire, false);
  assert.ok(plan.recovery.signature);
});

const before = capture(source);
source.samples.set(99, [{ t: T, cpu: 10 }]);
source.failedOn.set("web|99", T + MIN);
source.failedOn.delete("unused");
const after = capture(source);
restore(recovered, before);
applyDifference(recovered, difference(before, after));
check("incremental mirror retains Maps, Sets and nested runtime values", () => assert.deepEqual(capture(recovered), after));

// A local D1 binding substitute executes real SQL and atomic batches.
const durable = new DatabaseSync(":memory:");
durable.exec("CREATE TABLE runner_config(bucket TEXT,id TEXT,payload TEXT,updated INTEGER,PRIMARY KEY(bucket,id))");
let queries = 0;
const binding = {
  prepare(sql) { return { bind(...args) { return { sql, args,
    async all() { queries++; const rows = durable.prepare(sql).all(...args); return { results: rows, meta: { rows_read: rows.length } }; },
  }; } }; },
  async batch(statements) {
    durable.exec("BEGIN");
    try {
      const results = statements.map(({ sql, args }) => ({ success: true, meta: { rows_written: Number(durable.prepare(sql).run(...args).changes) } }));
      durable.exec("COMMIT"); return results;
    } catch (error) { durable.exec("ROLLBACK"); throw error; }
  },
};
const working = new WorkingStore((q, a) => source.db.prepare(q).all(...a));
source.sql = working;
working.exec("INSERT INTO app_env(name,key,value,updated) VALUES(?,?,?,?)", "web", "TOKEN", "sentinel-secret-value", T);
for (const table of CONFIG_TABLES) working.dirty.add(table);
const key = Buffer.alloc(32, 7).toString("base64");
const config = new Configuration(binding, key);
await config.flush(working);
check("D1 holds encrypted payloads", () => {
  const stored = durable.prepare("SELECT payload FROM runner_config").all();
  assert.ok(stored.length > 0);
  assert.equal(JSON.stringify(stored).includes("sentinel-secret-value"), false);
  assert.equal(JSON.stringify(stored).includes("services:"), false);
});
check("D1 contains only essential configuration buckets", () => {
  for (const row of durable.prepare("SELECT DISTINCT bucket FROM runner_config").all()) assert.ok(CONFIG_TABLES.includes(row.bucket));
  assert.equal(durable.prepare("SELECT count(*) AS n FROM runner_config WHERE bucket IN ('runs','copies','events','metrics_1m')").get().n, 0);
});
for (const report of reports) source.sync(report);
await config.flush(working);
const writes = config.writes;
for (let i = 0; i < 100; i++) {
  advance(1000);
  source.sync(reports[i % reports.length]);
  await config.flush(working);
}
check("100 runtime reports perform zero additional D1 writes", () => assert.equal(config.writes, writes));
working.exec("INSERT INTO settings(key,value) VALUES('rebalance','off') ON CONFLICT(key) DO UPDATE SET value='off'");
await config.flush(working);
check("an operator setting is persisted", () => assert.ok(config.writes > writes));
const reader = new Configuration(binding, key);
await config.setCredentials({ JOIN_TOKEN: "agent-authentication-secret", MAC_TOKEN: "mac-authentication-secret" });
const credentialWrites = config.writes;
await config.setCredentials({ JOIN_TOKEN: "agent-authentication-secret" });
check("unchanged credentials cause no D1 writes", () => assert.equal(config.writes, credentialWrites));
const loaded = await reader.load();
check("agent authentication is loaded once from encrypted D1", () => assert.equal(reader.credentials.get("JOIN_TOKEN"), "agent-authentication-secret"));
check("tunnel credentials survive a controller restart", () => assert.equal(loaded.slots[0].token, source.slots.get(loaded.slots[0].n).token));
check("configuration decrypts after restart", () => assert.equal(loaded.app_env.find((x) => x.key === "TOKEN").value, "sentinel-secret-value"));
check("runtime observations are absent from the D1 restore", () => assert.equal(loaded.runs, undefined));
await assert.rejects(new Configuration(binding, Buffer.alloc(32, 8).toString("base64")).load());
checks++; console.log("ok an incorrect encryption key cannot read configuration");
// Three tunnel slots per machine, with existing version history and room to grow.
for (let n = 1; n <= 300; n++) {
  const id = JSON.stringify([n]);
  durable.prepare("INSERT OR REPLACE INTO runner_config VALUES(?,?,?,?)").run("slots", id, await config.seal("slots", id, { n, token: `tunnel-${n}` }), T);
}
for (let version = 1; version <= 250; version++) {
  const id = JSON.stringify(["web", version]);
  durable.prepare("INSERT OR REPLACE INTO runner_config VALUES(?,?,?,?)").run("versions", id, await config.seal("versions", id, { name: "web", version, files: "{}" }), T);
}
queries = 0;
const scaled = await new Configuration(binding, key).load();
check("100 machines and version history cold-load within the free D1 query limit", () => {
  assert.equal(scaled.slots.length, 300);
  assert.equal(scaled.versions.length, 250);
  assert.ok(queries < 50, `${queries} startup queries exceed the free allowance`);
});
console.log(`${checks} hybrid checks passed`);
