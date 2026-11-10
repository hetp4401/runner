import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { D1Client } from "../src/d1-client.mjs";
import { D1Store, metadata } from "../src/d1-store.mjs";
import { Control } from "../src/index.js";
const fixture = fork(new URL("./d1-fixture.mjs", import.meta.url), { stdio: ["ignore", "ignore", "inherit", "ipc"] });
const [{ port }] = await once(fixture, "message");
const env = { ACCOUNT_ID: "test", D1_DATABASE_ID: "test", D1_API_KEY: "test-global-key", D1_API_EMAIL: "test@example.com", D1_API_BASE: `http://127.0.0.1:${port}`, CONTROL_HOST: "runners.test.example", DOMAIN: "test.example", DNS: "off", JOIN_TOKEN: "test-join", STATE_SIGNING_KEY: "test-signing", ADMIN_PASSWORD: "test-admin", RECOVER_RUNTIME: "1" };
const observer = new D1Client(env), stores = [];
const open = () => {
  const store = new D1Store(env); stores.push(store);
  const control = new Control({ sql: store, env, page: "", alarm() {}, restart() {} });
  store.protect(control); return { store, control };
};
let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const query = (sql) => observer.query(sql)[0].results;
try {
  let { store, control } = open();
  check("D1 working database and runtime tables are entirely in RAM", () => {
    assert.ok(store.db.prepare("PRAGMA database_list").all().every((r) => r.file === ""));
    assert.equal(store.db.prepare("SELECT COUNT(*) AS n FROM temp.runs").get().n, 0);
  });
  store.mutate(() => {
    control.putProject("web", { compose: 'services:\n  web:\n    image: nginx\n    ports: ["8080:80"]\n', port: 8080, replicas: 2, env: { TOKEN: "private-value" } });
    control.saveAppPassword("web", { salt: "test-salt", hash: "test-hash" });
  });
  check("app, source, environment and password commit together before returning", () => {
    assert.equal(metadata(observer).revision, 1);
    assert.equal(query("SELECT version FROM projects")[0].version, 1);
    assert.equal(query("SELECT value FROM app_env")[0].value, "private-value");
    assert.equal(query("SELECT hash FROM app_passwords")[0].hash, "test-hash");
  });
  const requests = store.client.stats.requests;
  for (let i = 0; i < 200; i++) { control.setSetting("last_hot_move", i); control.logEvent("test", { detail: "runtime" }); control.status(); }
  control.putProject("web", { replicas: 2 });
  check("runtime traffic and unchanged configuration make zero D1 requests", () => assert.equal(store.client.stats.requests, requests));
  const second = open();
  check("a new process working store reloads configuration and secrets without a local file", () => {
    assert.equal(second.control.projects.get("web").version, 1);
    assert.equal(second.control.appEnv.get("web").get("TOKEN"), "private-value");
    assert.equal(second.control.appPasswords.get("web").hash, "test-hash");
    assert.equal(second.control.settings.has("last_hot_move"), false);
  });
  control.putEnv("web", { TOKEN: "changed" });
  check("environment edit and its new application version persist in one commit", () => {
    assert.equal(metadata(observer).revision, 2);
    assert.equal(query("SELECT version FROM projects")[0].version, 2);
    assert.equal(query("SELECT COUNT(*) AS n FROM versions")[0].n, 2);
  });
  check("a stale controller cannot overwrite newer configuration", () => {
    assert.throws(() => second.control.setEnabled("web", false), /D1 request failed/);
    assert.equal(second.store.failed, true);
    assert.equal(query("SELECT enabled FROM projects")[0].enabled, 1);
  });
  await fetch(`http://127.0.0.1:${port}/lose-response`);
  control.putSettings({ rebalance: false });
  check("a lost response is confirmed by commit ID without replaying the edit", () => {
    assert.equal(metadata(observer).revision, 3);
    assert.equal(query("SELECT value FROM settings WHERE key='rebalance'")[0].value, "off");
    assert.equal(store.revision, 3);
  });
  observer.query("CREATE TRIGGER fail_env BEFORE INSERT ON app_env BEGIN SELECT RAISE(ABORT, 'fixture rejection'); END");
  const version = query("SELECT version FROM projects")[0].version;
  check("a rejected transaction rolls back every row and stops new plans", () => {
    assert.throws(() => control.putEnv("web", { TOKEN: "rejected" }), /D1 request failed/);
    assert.equal(query("SELECT value FROM app_env")[0].value, "changed");
    assert.equal(query("SELECT version FROM projects")[0].version, version);
    assert.equal(metadata(observer).revision, 3);
    assert.equal(store.failed, true);
    assert.throws(() => control.status(), /configuration storage failed/);
  });
  observer.query("DROP TRIGGER fail_env");
  ({ store, control } = open());
  control.deleteProject("web");
  check("deleting an app removes its sources, environment and password atomically", () => {
    assert.equal(metadata(observer).revision, 4);
    for (const table of ["projects", "versions", "app_env", "app_passwords"]) assert.equal(query(`SELECT COUNT(*) AS n FROM ${table}`)[0].n, 0);
  });
  check("missing identity, wrong controller and inactive D1 fleets fail closed", () => {
    assert.throws(() => new D1Store({ ...env, D1_API_KEY: "" }), /D1 requires/);
    assert.throws(() => new D1Store({ ...env, CONTROL_HOST: "other.test.example" }), /another controller/);
    observer.query("UPDATE runner_meta SET ready=0 WHERE id=1");
    assert.throws(() => new D1Store(env), /not ready/);
  });
  console.log(`${checks} D1 persistence checks passed`);
} finally {
  for (const store of stores) store.close();
  observer.close(); fixture.kill("SIGTERM");
}
