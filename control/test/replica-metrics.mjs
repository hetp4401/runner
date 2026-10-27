import assert from "node:assert/strict";
import { readFileSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import vm from "node:vm";
import { replicaSample } from "../../agent/metrics.mjs";
import { Control } from "../src/index.js";
import { LocalStore } from "../src/local-store.mjs";

let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const copy = (key, replica) => ({ key, app: "web", replica });
const container = (id, identity, cpu, mem, read, write) => ({ id, app: "web", copy: identity, cpuNs: cpu, mem, rx: read, tx: write, br: read, bw: write });
const first = { at: 0, list: [container("a", copy("web", 1), 0, 10, 0, 0), container("b", copy("web", 1), 0, 20, 0, 0), container("c", copy("web-r2", 2), 0, 80, 0, 0)] };
const next = { at: 1000, list: [container("a", copy("web", 1), 600_000_000, 10, 2, 40), container("b", copy("web", 1), 400_000_000, 20, 8, 50), container("c", copy("web-r2", 2), 2_000_000_000, 80, 80, 5)] };
const replicas = replicaSample(first, next);
check("services combine within a replica; colocated replicas stay separate", () => {
  assert.equal(Object.keys(replicas).length, 2);
  assert.deepEqual(replicas.web, { app: "web", replica: 1, values: { cpu: 100, mem: 30, netRx: 10, netTx: 90, diskRead: 10, diskWrite: 90 } });
  assert.equal(replicas["web-r2"].values.cpu, 200);
});
check("incomplete or newly reassigned container samples cannot masquerade as zero usage", () => {
  const incomplete = structuredClone(next); delete incomplete.list[0].cpuNs;
  assert.deepEqual(Object.keys(replicaSample(first, incomplete)), ["web-r2"]);
  const reassigned = structuredClone(next); reassigned.list[2].copy.replica = 3;
  assert.deepEqual(Object.keys(replicaSample(first, reassigned)), ["web"]);
});

const page = readFileSync(new URL("../src/app.html", import.meta.url), "utf8");
const definitions = page.slice(page.indexOf("const METRICS ="), page.indexOf("const savedMetric ="));
const helpers = page.slice(page.indexOf("// Rank numeric samples"), page.indexOf("function appMetricPanel("));
const fixture = { projects: [{ name: "web", replicas: 2, placed: [{ machine: 1, key: "web", replica: 1 }, { machine: 1, key: "web-r2", replica: 2 }] }], live: { 1: { a: { web: { cpu: 300, mem: 110 } }, status: { web: {}, "web-r2": {} }, replicas } } };
const state = { metric: "disk", appAggregate: "avg" };
const context = vm.createContext({ S: state, fixture, isNum: (x) => typeof x === "number" && Number.isFinite(x), appLabel: (x) => x, appHref: (x) => x,
  liveRows: () => Object.entries(fixture.live).map(([m, row]) => ({ m: Number(m), ...row })) });
vm.runInContext(`const M=()=>fixture; ${definitions}\n${helpers}`, context);
const evaluate = (source) => JSON.parse(JSON.stringify(vm.runInContext(source, context)));
check("Average uses individual replicas on one host", () => {
  const row = evaluate("liveAppRows()[0]"); assert.equal(row.values.cpu, 150); assert.equal(row.values.mem, 55); assert.equal(row.coverage.cpu, 2);
});
state.appAggregate = "max";
check("Max disk and network use one replica's combined directions", () => {
  const row = evaluate("liveAppRows()[0]"); assert.equal(row.values.cpu, 200);
  assert.equal(row.values.diskRead, 10); assert.equal(row.values.diskWrite, 90);
  assert.equal(row.values.netRx, 10); assert.equal(row.values.netTx, 90);
});
state.appAggregate = "p99";
check("P99 interpolates across replicas and preserves paired I/O values", () => {
  const row = evaluate("liveAppRows()[0]"); assert.equal(row.values.cpu, 199); assert.equal(row.values.mem, 79.5);
  assert.ok(Math.abs(row.values.diskRead + row.values.diskWrite - 99.85) < 1e-9);
});
delete fixture.live[1].replicas;
check("legacy summed data on a host with multiple replicas is marked missing", () => assert.equal(evaluate("liveAppRows()[0]").coverage.cpu, 0));
fixture.projects[0].placed.pop(); fixture.projects[0].replicas = 1;
delete fixture.live[1].status["web-r2"];
check("legacy data remains exact for a single reported replica on a host", () => assert.equal(evaluate("liveAppRows()[0]").values.cpu, 300));
fixture.live[1].status["web-r2"] = {};
check("an extra legacy copy awaiting removal cannot be treated as one replica", () => assert.equal(evaluate("liveAppRows()[0]").coverage.cpu, 0));
fixture.live[1].replicas = {};
check("an explicit missing replica sample cannot fall back to a host total", () => assert.equal(evaluate("liveAppRows()[0]").coverage.cpu, 0));

const directory = mkdtempSync(join(tmpdir(), "runner-replica-metrics-"));
const store = new LocalStore(join(directory, "control.db"), { initialize: true });
let observer;
try {
  const now = Date.now(), control = new Control({ sql: store, env: { DOMAIN: "example.test", DNS: "off", STATE_SIGNING_KEY: "test-key" }, page: "", alarm() {}, restart() {} });
  store.protect(control);
  const run = { id: "test-run", machine: 1, agent: "test-agent", label: null, started: now - 1000, seen: now, ready: 1, retire: 0, handover: 0, status: {} };
  control.saveRun(run);
  observer = new DatabaseSync(store.filename, { readOnly: true });
  const version = observer.prepare("PRAGMA data_version").get().data_version;
  for (let i = 0; i < 10; i++) control.takeMetrics(run, { live: { t: now, h: { cpu: 2 }, a: {}, replicas } }, now);
  const response = await (await control.metricsResponse("1h")).json();
  check("the CP forwards replica samples from memory with zero durable writes", () => {
    assert.deepEqual(response.live[1].replicas, replicas);
    assert.equal(observer.prepare("PRAGMA data_version").get().data_version, version);
  });
} finally { observer?.close(); store.close(); rmSync(directory, { recursive: true, force: true }); }
console.log(`${checks} replica metric checks passed`);
