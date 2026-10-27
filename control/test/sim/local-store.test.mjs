import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash, createHmac } from "node:crypto";
import { makeControl, Fleet, deploy, advance, MIN, T } from "./lib.mjs";
import { Control } from "../../src/index.js";
import { LocalStore } from "../../src/local-store.mjs";
import { CONFIG_TABLES } from "../../src/state-policy.js";

process.umask(0o077);
const dir = mkdtempSync(join(tmpdir(), "runner-local-store-"));
const filename = join(dir, "control.db");
const signingKey = "test-recovery-signing-key";
let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const env = { STATE_SIGNING_KEY: signingKey, JOIN_TOKEN: "test-join", ADMIN_PASSWORD: "test-admin", DNS: "off", DOMAIN: "sim.example", POLL_S: "20", RECOVER_RUNTIME: "1", MEMORY_HISTORY_MIN: "360" };
const open = () => {
  const store = new LocalStore(filename);
  const control = new Control({ sql: store, env, page: "", alarm() {}, restart() {} });
  store.protect(control);
  return { store, control };
};
const schema = (db, database = "main") => db.prepare(`SELECT name FROM ${database}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`).all().map((r) => r.name);
const snapshot = (db) => Object.fromEntries(CONFIG_TABLES.map((table) => [table, db.prepare(`SELECT * FROM main.${table} ORDER BY rowid`).all()]));
const fileHash = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");

try {
  check("a missing configuration database does not initialize an empty fleet", () => {
    assert.throws(() => new LocalStore(filename), /control.db is missing/);
  });
  const empty = join(dir, "empty.db");
  writeFileSync(empty, "");
  check("an existing empty database is rejected without explicit initialization", () => {
    assert.throws(() => new LocalStore(empty), /configuration table projects is missing/);
  });

  // A legacy on-disk controller with healthy workloads and signed agent receipts.
  const source = makeControl({ STATE_SIGNING_KEY: signingKey }, new DatabaseSync(filename));
  const fleet = new Fleet(source, { test: { size: 3, cap: 3 } }, { drainAt: () => null });
  await fleet.startPool("test");
  const compose = (port) => `services:\n  web:\n    image: nginx\n    ports: ["${port}:80"]\n`;
  deploy(source, "web", { replicas: 2, port: 8080, compose: compose(8080), env: { TOKEN: "test-app-secret" } });
  deploy(source, "removed", { replicas: 1, port: 8081, compose: compose(8081) });
  deploy(source, "disabled", { replicas: 1, port: 8082, compose: compose(8082) });
  await fleet.run(15 * MIN);
  const reports = fleet.alive().map((s) => {
    const body = { ...fleet.body(s), protocol: 2 };
    return { ...body, recovery: source.sync(body).recovery };
  });
  // Desired configuration changed after the last receipts were signed. The old
  // running inventory must not resurrect deleted apps, sizes, or source versions.
  source.deleteProject("removed");
  source.setEnabled("disabled", false);
  deploy(source, "web", { replicas: 1 });
  source.saveAppPassword("web", { salt: "test-salt", hash: "test-hash" });
  source.setSetting("rebalance", "off");
  source.setSetting("roll_99", T);
  source.setSetting("last_hot_move", T);
  const desiredVersion = source.projects.get("web").version;
  const originalCredentials = source.all("SELECT * FROM slots");
  const legacyRuns = source.all("SELECT * FROM runs").length;
  source.db.close();

  let { store, control } = open();
  check("migration retains a private full database backup", () => {
    assert.ok(store.backupPath);
    assert.equal(statSync(store.backupPath).mode & 0o777, 0o600);
    const backup = new DatabaseSync(store.backupPath, { readOnly: true });
    assert.equal(backup.prepare("SELECT count(*) AS n FROM runs").get().n, legacyRuns);
    backup.close();
  });
  check("only configuration tables remain on disk", () => assert.deepEqual(schema(store.db), [...CONFIG_TABLES].sort()));
  check("all observation tables are in the in-memory TEMP database", () => {
    for (const table of ["runs", "copies", "starts", "agents", "events", "geo", "metrics_1m", "metrics_10m", "metrics_1h", "settings"]) assert.ok(schema(store.db, "temp").includes(table));
    assert.equal(store.db.prepare("PRAGMA temp_store").get().temp_store, 2);
    assert.equal(store.db.prepare("PRAGMA synchronous").get().synchronous, 2);
  });
  check("startup discards saved health and placements", () => {
    assert.equal(control.runs.size, 0);
    assert.equal(control.copies.size, 0);
    assert.equal(control.settings.has("last_hot_move"), false);
  });
  check("specs, secrets and tunnel credentials survived migration", () => {
    assert.equal(control.projects.get("web").version, desiredVersion);
    assert.equal(control.appEnv.get("web").get("TOKEN"), "test-app-secret");
    assert.equal(control.appPasswords.get("web").hash, "test-hash");
    assert.deepEqual(control.all("SELECT * FROM slots"), originalCredentials);
    assert.equal(control.settings.get("roll_99"), String(T));
  });
  const observer = new DatabaseSync(filename, { readOnly: true });
  const diskBefore = snapshot(observer);
  const beforeVersion = observer.prepare("PRAGMA data_version").get().data_version;
  const beforeWal = fileHash(`${filename}-wal`);
  for (const body of reports) assert.equal(control.sync(body).hold, true);
  check("recovery holds workloads while agents reconnect", () => assert.ok(control.runs.size >= 3));
  check("a signed old receipt cannot recreate a deleted app", () => {
    assert.equal(control.projects.has("removed"), false);
    assert.equal(control.copies.has("removed"), false);
  });

  // Even a historically issued receipt containing operator settings must not
  // overwrite today's durable settings. Observed health wins over witness data.
  const old = JSON.parse(reports[0].recovery.payload);
  old.at = T + 1;
  old.runtime.push({ key: "rebalance", value: "on" }, { key: "pools", value: '{"test":99}' });
  const payload = JSON.stringify(old);
  control.sync({ ...reports[0], recovery: { payload, signature: createHmac("sha256", signingKey).update(payload).digest("hex") } });
  check("receipts never override current operator intent", () => {
    assert.equal(control.rebalanceOn(), false);
    assert.equal(control.poolSize("test"), 3);
  });
  for (let i = 0; i < 10; i++) {
    advance(20_000);
    for (const body of reports) control.sync(body);
  }
  const plans = reports.map((body) => control.sync(body));
  check("normal scheduling resumes after inventory recovery", () => assert.ok(plans.every((p) => !p.hold)));
  check("current specs remove deleted, disabled and excess copies", () => {
    const copies = plans.flatMap((p) => Object.values(p.desired));
    assert.equal(copies.filter((p) => p.app === "web").length, 1);
    assert.ok(copies.every((p) => p.app === "web" && p.replica === 1 && p.v === desiredVersion));
    assert.equal(control.projects.has("removed"), false);
  });
  check("a valid existing replica keeps its machine and local key", () => {
    const before = reports.flatMap((r) => JSON.parse(r.recovery.payload).copies).find((x) => x.name === "web" && x.replica === 1);
    const after = control.copiesOf("web")[0];
    assert.equal(after.machine, before.machine);
    assert.equal(after.key, before.key);
  });
  for (let i = 0; i < 200; i++) {
    advance(1000);
    const body = reports[i % reports.length];
    control.sync({ ...body, metrics: { live: { t: T, h: { cpu: 10, memUsed: 100, memTotal: 1000 }, a: {} }, minutes: [] } });
    control.setSetting("rolled10", T);
    control.setSetting("pool_asks", '{"test":3}');
    control.logEvent("test", { detail: "runtime event" });
  }
  check("recovery, reports, metrics and cooldowns produce zero durable commits", () => {
    assert.deepEqual(snapshot(observer), diskBefore);
    assert.equal(observer.prepare("PRAGMA data_version").get().data_version, beforeVersion);
    assert.equal(fileHash(`${filename}-wal`), beforeWal);
  });
  control.putSettings({ rebalance: false });
  check("an unchanged operator setting performs no disk write", () => assert.equal(fileHash(`${filename}-wal`), beforeWal));
  control.putEnv("web", { TOKEN: "updated-secret" });
  check("an acknowledged spec/secret update is durable immediately", () => {
    assert.equal(observer.prepare("SELECT value FROM app_env WHERE name='web' AND key='TOKEN'").get().value, "updated-secret");
    assert.equal(observer.prepare("SELECT version FROM projects WHERE name='web'").get().version, control.projects.get("web").version);
    assert.notEqual(observer.prepare("PRAGMA data_version").get().data_version, beforeVersion);
  });
  const saved = snapshot(observer);
  observer.close();
  store.close();
  ({ store, control } = open());
  check("a second restart retains configuration but no runtime tables", () => {
    assert.equal(store.backupPath, undefined);
    assert.deepEqual(snapshot(store.db), saved);
    assert.equal(control.runs.size, 0);
    assert.equal(control.copies.size, 0);
    assert.equal(control.settings.has("rolled10"), false);
    assert.equal(control.appEnv.get("web").get("TOKEN"), "updated-secret");
  });
  const beforeFailedEdit = snapshot(store.db);
  assert.throws(() => control.putSettings({ rebalance: true, pools: { test: -1 } }), /whole number/);
  check("a failed multi-part configuration edit is rolled back and stops further plans", () => {
    assert.deepEqual(snapshot(store.db), beforeFailedEdit);
    assert.equal(store.failed, true);
    assert.throws(() => control.sync(reports[0]), /configuration storage failed/);
  });
  store.close();

  // Broken desired state is fatal; it never becomes an empty desired plan.
  const broken = new DatabaseSync(filename);
  broken.exec("DELETE FROM versions WHERE name='web'");
  broken.close();
  check("missing source for a persisted app prevents startup", () => assert.throws(() => open(), /configuration is incomplete/));
  const corrupt = join(dir, "corrupt.db");
  writeFileSync(corrupt, "not a database");
  check("an unreadable configuration database prevents startup", () => assert.throws(() => new LocalStore(corrupt), /not a database/));
  console.log(`${checks} local persistence checks passed`);
} finally {
  rmSync(dir, { recursive: true, force: true });
}
