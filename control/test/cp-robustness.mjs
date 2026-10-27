import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { createHash } from "node:crypto";
import { Control } from "../src/index.js";
import { LocalStore } from "../src/local-store.mjs";
import { CONFIG_TABLES } from "../src/state-policy.js";
import { Fleet, advance, MIN, T } from "./sim/lib.mjs";

process.umask(0o077);
const directory = mkdtempSync(join(tmpdir(), "runner-cp-robustness-"));
const stores = new Set();
let sequence = 0, checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const baseEnv = { DOMAIN: "example.test", ADMIN_PASSWORD: "test-owner-password", JOIN_TOKEN: "test-join",
  STATE_SIGNING_KEY: "test-recovery-signing-key", DNS: "off", POLL_S: "20", QUIET_MS: "0", REQUIRE_DEPLOY_AUTH: "true" };
const web = (image = "nginx") => ({ compose: `services:\n  web:\n    image: ${image}\n    ports: ["8080:80"]\n`, port: 8080 });
const request = (control, path, { method = "GET", body, admin = false, appPassword } = {}) => control.fetch(new Request(`https://cp.example.test${path}`, {
  method, headers: { "content-type": "application/json", ...(admin ? { "x-admin-password": baseEnv.ADMIN_PASSWORD } : {}), ...(appPassword ? { "x-app-password": appPassword } : {}) },
  body: body === undefined ? undefined : JSON.stringify(body),
}));

function open(filename = join(directory, `${++sequence}.db`), env = {}, initialize = true, tunnels = new Map()) {
  const failures = [];
  const store = new LocalStore(filename, { initialize, onFailure: (error) => failures.push(error) });
  stores.add(store);
  let control;
  try {
    control = new Control({ sql: store, env: { ...baseEnv, ...env }, page: "", alarm() {}, restart() {} });
    store.protect(control);
  } catch (error) { store.close(); stores.delete(store); throw error; }
  control.cf = async (path, options = {}) => {
    if (path.endsWith("/token")) return { success: true, result: "test-connector-token" };
    if (path.includes("/cfd_tunnel")) {
      if (options.method === "POST") {
        const { name } = JSON.parse(options.body), tunnel = { name, id: `test-tunnel-${tunnels.size + 1}` };
        tunnels.set(name, tunnel); return { success: true, result: tunnel };
      }
      const name = new URL(`https://fake.test${path}`).searchParams.get("name");
      const result = [...tunnels.values()].filter((tunnel) => !name || tunnel.name === name);
      return { success: true, result, result_info: { total_count: result.length, per_page: 1000 } };
    }
    throw new Error(`unexpected fake Cloudflare request: ${path}`);
  };
  return { control, store, filename, failures, tunnels };
}

async function fixture(size = 1) {
  const state = open();
  const fleet = new Fleet(state.control, { test: { size, cap: size + 1 } }, { drainAt: () => null });
  await fleet.startPool("test");
  await fleet.run(4 * MIN);
  state.control.putProject("web", { ...web(), env: { TOKEN: "test-app-secret" } }, { trusted: true });
  await fleet.run(4 * MIN);
  return { ...state, fleet };
}
function report(state, server, extra = {}) {
  const body = { ...state.fleet.body(server), protocol: 2, ...extra };
  return { ...body, recovery: state.control.sync(body).recovery };
}
async function cold(state) {
  state.store.close(); stores.delete(state.store);
  const reopened = open(state.filename, { RECOVER_RUNTIME: "1", RECOVER_TUNNELS: "1", RECOVERY_MS: "180000" }, false, state.tunnels);
  await reopened.control.recoverTunnels();
  return reopened;
}
function durableSnapshot(db) {
  return Object.fromEntries(CONFIG_TABLES.map((table) => [table, db.prepare(`SELECT * FROM main.${table} ORDER BY rowid`).all()]));
}
const fileHash = (filename) => createHash("sha256").update(readFileSync(filename)).digest("hex");

try {
  {
    const { control, store, failures } = open();
    const rejected = await request(control, "/api/events?limit=1.5");
    const subsequent = await request(control, "/api/status");
    check("a fractional public event limit is rejected without invalidating storage", () => {
      assert.equal(rejected.status, 400); assert.equal(subsequent.status, 200);
      assert.equal(Boolean(store.failed), false); assert.equal(failures.length, 0);
    });
    const denied = await request(control, "/api/projects/authcheck", { method: "PUT", body: web() });
    const accepted = await request(control, "/api/projects/authcheck", { method: "PUT", body: web(), admin: true });
    check("native Control treats REQUIRE_DEPLOY_AUTH=true as an authenticated deployment policy", () => {
      assert.equal(denied.status, 401); assert.equal(accepted.status, 200); assert.equal(control.status().requireDeployAuth, true);
    });
    check("blank and misspelled native deployment policies fail during startup", () => {
      for (const value of ["", "tru", "yes"]) assert.throws(() => open(undefined, { REQUIRE_DEPLOY_AUTH: value }), /REQUIRE_DEPLOY_AUTH/);
    });
  }

  {
    const state = await fixture(), { control, fleet } = state;
    control.putEnv("web", { TOKEN: "dummy-build-secret-12345" });
    control.saveAppPassword("web", await control.newAppSecret("test-app-password"));
    const server = fleet.alive()[0];
    const plan = control.sync({ ...fleet.body(server), protocol: 2, status: {
      web: { v: control.project("web").version, s: "failed", e: "build failed while using dummy-build-secret-12345; useful diagnostic" },
    } });
    const response = await request(control, "/api/status"), publicText = await response.text();
    const events = await (await request(control, "/api/events")).text();
    check("public status, events and signed recovery receipts exclude raw build failures", () => {
      assert.equal(response.status, 200);
      for (const text of [publicText, events, JSON.stringify(plan.recovery)]) {
        assert.equal(text.includes("dummy-build-secret-12345"), false);
        assert.equal(text.includes("useful diagnostic"), false);
      }
    });
    const unauthorized = await request(control, "/api/projects/web/diagnostics", { method: "POST" });
    const authenticated = await request(control, "/api/projects/web/diagnostics", { method: "POST", admin: true });
    const details = await authenticated.json();
    check("authenticated diagnostics preserve useful details while redacting configured secrets", () => {
      assert.equal(unauthorized.status, 401); assert.equal(authenticated.status, 200);
      assert.equal(details.errors.length, 1);
      assert.match(details.errors[0].error, /useful diagnostic/);
      assert.match(details.errors[0].error, /\[hidden\]/);
      assert.equal(JSON.stringify(details).includes("dummy-build-secret-12345"), false);
    });
    const delegated = await request(control, "/api/projects/web/diagnostics", { method: "POST", appPassword: "test-app-password" });
    check("an app password can access its redacted diagnostics", () => assert.equal(delegated.status, 200));
    const publicFleet = open(undefined, { REQUIRE_DEPLOY_AUTH: "false" });
    publicFleet.control.putProject("web", web(), { trusted: false });
    const publicDiagnostics = await request(publicFleet.control, "/api/projects/web/diagnostics", { method: "POST" });
    check("public deployment access does not grant access to app diagnostics", () => assert.equal(publicDiagnostics.status, 401));
  }

  {
    const state = open(), { control } = state;
    const fleet = new Fleet(control, { test: { size: 2, cap: 2 } }, { drainAt: () => null });
    const labels = ["eu-west", "ap-south"];
    fleet.sync = async (server, extra = {}) => {
      const plan = control.sync({ ...fleet.body(server, extra), region: labels[fleet.servers.indexOf(server)] });
      server.lastPlan = plan; return plan;
    };
    await fleet.startPool("test"); await fleet.run(4 * MIN);
    control.putProject("regional", { compose: 'x-runner:\n  regions: [eu-west, ap-south]\n  replicas: 2\n  port: 8080\nservices:\n  web:\n    image: nginx\n    ports: ["8080:80"]\n' }, { trusted: true });
    await fleet.run(4 * MIN);
    check("operator region labels place each replica on the matching reported region", () => {
      const copies = control.copiesOf("regional").filter((copy) => !copy.leaving).sort((a, b) => a.replica - b.replica);
      assert.equal(copies.length, 2);
      assert.deepEqual(copies.map((copy) => control.liveMachines(T).get(copy.machine).region), labels);
      for (const label of labels) assert.equal(control.status().regions.find((region) => region.name === label)?.machines, 1);
    });
  }

  for (const age of [181_000, 2 * 86400_000]) {
    const state = await fixture(), server = state.fleet.alive()[0];
    const body = report(state, server), original = { ...state.control.copiesOf("web")[0] };
    const restored = await cold(state);
    const observer = new DatabaseSync(restored.filename, { readOnly: true });
    try {
      const before = durableSnapshot(observer), version = observer.prepare("PRAGMA data_version").get().data_version;
      const wal = fileHash(`${restored.filename}-wal`);
      advance(age);
      const plan = restored.control.sync(body);
      check(age > 86400_000 ? "a signed receipt older than 24 hours recovers a healthy agent" : "the first report after the three-minute window retains its existing healthy app", () => {
        assert.equal(plan.hold, undefined); assert.equal(plan.retire, false);
        assert.equal(plan.desired[original.key]?.app, "web");
        assert.equal(restored.control.copiesOf("web")[0].machine, original.machine);
        assert.equal(restored.control.copiesOf("web")[0].key, original.key);
      });
      for (let index = 0; index < 12; index++) { advance(20_000); restored.control.sync({ ...body, recovery: plan.recovery }); }
      check(`receipt recovery and subsequent reports perform no durable commits (${age}ms outage)`, () => {
        assert.deepEqual(durableSnapshot(observer), before);
        assert.equal(observer.prepare("PRAGMA data_version").get().data_version, version);
        assert.equal(fileHash(`${restored.filename}-wal`), wal);
      });
    } finally { observer.close(); }
  }

  {
    const state = await fixture(), server = state.fleet.alive()[0];
    await state.fleet.run(12 * MIN);
    state.control.roll(String(server.machine));
    const body = report(state, server, { protocol: 3, starts: false });
    assert.ok(JSON.parse(body.recovery.payload).runRow.handover, "fixture must contain a previously issued handover");
    const original = { ...state.control.copiesOf("web")[0] }, restored = await cold(state);
    advance(181_000);
    const restarted = { ...body, protocol: 3, previousRun: body.run, run: `${body.run}-restarted`, started: T };
    const before = durableSnapshot(restored.store.db), wal = fileHash(`${restored.filename}-wal`);
    const plan = restored.control.sync(restarted);
    check("a protocol3 process restart keeps placement and clears the obsolete handover", () => {
      assert.equal(plan.hold, undefined); assert.equal(plan.retire, false); assert.equal(plan.handover, false);
      assert.equal(plan.desired[original.key]?.app, "web");
      assert.equal(restored.control.copiesOf("web")[0].machine, original.machine);
      assert.equal(restored.control.runs.get(restarted.run).handover, 0);
      assert.ok(!restored.control.runs.has(body.run) || restored.control.runs.get(body.run).retire);
    });
    check("a process restart clears runtime handover without changing durable configuration", () => {
      assert.deepEqual(durableSnapshot(restored.store.db), before);
      assert.equal(fileHash(`${restored.filename}-wal`), wal);
    });
    check("a restart cannot attach another agent's signed inventory", () => {
      assert.throws(() => restored.control.sync({ ...restarted, agent: "different-agent" }), /does not identify/);
    });
  }

  {
    const state = await fixture(2), oldCopy = state.control.copiesOf("web")[0];
    const oldServer = state.fleet.alive().find((server) => server.machine === oldCopy.machine);
    const newServer = state.fleet.alive().find((server) => server !== oldServer);
    const stale = report(state, oldServer);
    state.control.move("web", oldServer.machine, String(newServer.machine), oldCopy.replica);
    await state.fleet.run(4 * MIN);
    assert.deepEqual(state.control.copiesOf("web").map((copy) => copy.machine), [newServer.machine]);
    const current = report(state, newServer), restored = await cold(state);
    advance(181_000);
    restored.control.sync(current);
    const late = restored.control.sync(stale);
    check("a late stale receipt cannot replace a newer live assignment", () => {
      assert.deepEqual(restored.control.copiesOf("web").map((copy) => copy.machine), [newServer.machine]);
      assert.equal(Object.values(late.desired ?? {}).some((copy) => copy.app === "web"), false);
    });
  }

  {
    const state = open();
    state.fleet = new Fleet(state.control, { test: { size: 2, cap: 3 } }, { drainAt: () => null });
    await state.fleet.startPool("test"); await state.fleet.run(4 * MIN);
    const witness = report(state, state.fleet.alive()[0]);
    state.control.putProject("web", { ...web(), replicas: 2 }, { trusted: true });
    await state.fleet.run(4 * MIN);
    const first = state.control.copiesOf("web").find((copy) => copy.replica === 1);
    const second = state.control.copiesOf("web").find((copy) => copy.replica === 2);
    state.control.move("web", first.machine, String(second.machine), 1);
    await state.fleet.run(4 * MIN);
    state.control.putProject("web", { replicas: 1 }, { trusted: true });
    await state.fleet.run(4 * MIN);
    const original = { ...state.control.copiesOf("web")[0] };
    const owner = state.fleet.alive().find((server) => server.machine === original.machine);
    const own = report(state, owner), restored = await cold(state);
    restored.control.sync(witness);
    assert.ok(restored.control.runs.has(own.run), "fixture must recover the late run through a witness");
    assert.equal(restored.control.copiesOf("web").length, 0, "old witness predates the app");
    advance(181_000);
    const plan = restored.control.sync(own);
    check("a witness-only run recovers its newer inventory on its first direct report after the window", () => {
      assert.equal(plan.hold, undefined);
      assert.equal(plan.desired[original.key]?.app, "web");
      assert.equal(restored.control.copiesOf("web")[0].machine, original.machine);
      assert.equal(restored.control.copiesOf("web")[0].key, original.key);
      assert.deepEqual(restored.control.copiesOf("web")[0].ports, original.ports);
    });
  }

  {
    const state = await fixture(3), oldCopy = state.control.copiesOf("web")[0];
    const legacy = state.fleet.alive().find((server) => server.machine === oldCopy.machine);
    const [witness, target] = state.fleet.alive().filter((server) => server !== legacy);
    const legacyBody = { ...state.fleet.body(legacy), protocol: 2 };
    const oldWitness = report(state, witness);
    assert.ok(JSON.parse(oldWitness.recovery.payload).fleet.runs.some((run) => run.id === legacy.run));
    state.control.move("web", legacy.machine, String(target.machine), oldCopy.replica);
    await state.fleet.run(4 * MIN);
    const current = report(state, target), restored = await cold(state);
    advance(181_000);
    assert.equal(restored.control.sync(current).hold, undefined);
    assert.deepEqual(restored.control.copiesOf("web").map((copy) => copy.machine), [target.machine]);
    assert.equal(restored.control.sync(legacyBody).hold, true);
    assert.ok(restored.control.recoveryMissing.has(legacy.run));
    const witnessPlan = restored.control.sync(oldWitness);
    check("a late legacy report cannot reopen fleet replay over a warm controller's newer placement", () => {
      assert.deepEqual(restored.control.copiesOf("web").map((copy) => copy.machine), [target.machine]);
      assert.equal(Object.values(witnessPlan.desired ?? {}).some((copy) => copy.app === "web"), false);
    });
    const legacyPlan = restored.control.sync(legacyBody);
    check("a verified witness clears late legacy recovery without replaying its old placement", () => {
      assert.equal(restored.control.recoveryMissing.size, 0);
      assert.equal(witnessPlan.hold, undefined); assert.equal(legacyPlan.hold, undefined);
      assert.equal(Object.values(legacyPlan.desired ?? {}).some((copy) => copy.app === "web"), false);
      assert.deepEqual(restored.control.copiesOf("web").map((copy) => copy.machine), [target.machine]);
    });
  }

  {
    const state = await fixture(4);
    for (const server of state.fleet.alive()) report(state, server);
    let reports = state.fleet.alive().map((server) => report(state, server));
    let target = reports.find((body) => !JSON.parse(body.recovery.payload).fleet);
    assert.ok(target, "four healthy agents must include a non-witness receipt");
    if (state.control.copiesOf("web")[0].machine === target.machine) {
      const other = state.fleet.alive().find((server) => server.machine !== target.machine);
      state.control.move("web", target.machine, String(other.machine), 1);
      await state.fleet.run(4 * MIN);
      for (const server of state.fleet.alive()) report(state, server);
      reports = state.fleet.alive().map((server) => report(state, server));
      target = reports.find((body) => body.machine === target.machine);
    }
    assert.equal(JSON.parse(target.recovery.payload).copies.length, 0);
    const oldMachine = state.control.copiesOf("web")[0].machine;
    const legacy = { ...reports.find((body) => body.machine === oldMachine), recovery: undefined };
    const witness = reports.find((body) => body.machine !== oldMachine && JSON.parse(body.recovery.payload).fleet);
    assert.ok(witness);
    const restored = await cold(state);
    assert.equal(restored.control.sync(target).hold, true);
    check("manual moves are rejected while cold recovery is active", () => {
      assert.throws(() => restored.control.move("web", oldMachine, String(target.machine), 1), (error) => error.status === 503 && /recovering/.test(error.message));
    });
    for (let index = 0; index < 4; index++) { advance(40_000); assert.equal(restored.control.sync(target).hold, true); }
    advance(21_000);
    const response = await request(restored.control, "/api/projects/web/env", { method: "PUT", body: { TOKEN: "updated-test-secret" }, admin: true });
    assert.equal(response.status, 200);
    assert.deepEqual(restored.control.copiesOf("web").map((copy) => copy.machine), [target.machine], "API places the app before any post-window agent sync");
    assert.equal(restored.control.sync(legacy).hold, true);
    const plan = restored.control.sync(witness);
    check("API placement before the first normal agent report closes fleet snapshot replay", () => {
      assert.equal(plan.hold, undefined);
      assert.equal(restored.control.recoveryMissing.size, 0);
      assert.deepEqual(restored.control.copiesOf("web").map((copy) => copy.machine), [target.machine]);
    });
  }
  console.log(`${checks} control-plane robustness checks passed`);
} finally {
  for (const store of stores) store.close();
  rmSync(directory, { recursive: true, force: true });
}
