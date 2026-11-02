import assert from "node:assert/strict";
import test from "node:test";
import { ACTIVE_STATUSES, capacityPlan, githubClient, reconcileCapacity, targetCount } from "./capacity.mjs";

const now = Date.parse("2026-11-02T12:00:00Z");
const run = (id, slot, status = "in_progress", age = 60) => ({ id, name: `machine ${slot}`, status,
  created_at: new Date(now - age * 60_000).toISOString() });
const env = { GITHUB_REPOSITORY: "school/Runners", MACHINE_REF: "main", MACHINE_COUNT: "15",
  SERVER_TAG: "school", CONTROL_URL: "https://runners.example.test" };

function fixture({ active = [], recent = [], status = { fleetMode: "passive", groups: [{ tag: "school", online: 10 }] }, fail } = {}) {
  const calls = [];
  return {
    calls,
    options: {
      now: () => now, log() {}, readStatus: async () => status,
      api: async (path, options = {}) => {
        calls.push({ path, ...options });
        if (fail) await fail(path, options, calls);
        if (options.method === "POST") return null;
        const url = new URL(path, "https://api.example.test"), state = url.searchParams.get("status");
        return { workflow_runs: state ? active.filter((r) => r.status === state) : recent };
      },
    },
  };
}

test("targets are provider settings, accept the legacy alias, and reject unsafe quantities", () => {
  assert.equal(targetCount({}), 0);
  assert.equal(targetCount({ POOL_SIZE: "15" }), 15);
  assert.equal(targetCount({ MACHINE_COUNT: "6", POOL_SIZE: "15" }), 6);
  assert.equal(targetCount({ MACHINE_COUNT: "0", POOL_SIZE: "15" }), 0);
  for (const value of ["-1", "1.5", "01", "51", "many", " "]) assert.throws(() => targetCount({ MACHINE_COUNT: value }));
});

test("handover jobs reserve one slot while queued and fresh jobs each reserve capacity", () => {
  const active = [run(1, 7), run(2, 7, "queued"), run(3, 8), run(4, 0), run(5, 0, "pending")];
  assert.deepEqual(capacityPlan(6, active, [], now), { active: 4, missing: 2, launch: 2, deferred: 0 });
});

test("requested, waiting and queued hosts prevent duplicate provisioning before they join", () => {
  const active = ACTIVE_STATUSES.map((state, index) => run(index + 1, index + 1, state));
  assert.deepEqual(capacityPlan(5, active, [], now), { active: 5, missing: 0, launch: 0, deferred: 0 });
});

test("the latest listing resolves completion races and duplicate API snapshots", () => {
  assert.deepEqual(capacityPlan(3, [run(1, 1), run(2, 2)], [run(1, 1, "completed"), run(2, 2)], now),
    { active: 1, missing: 2, launch: 2, deferred: 0 });
});

test("rapid failed launches cannot form an immediate completion/refill loop", () => {
  const recent = Array.from({ length: 6 }, (_, i) => run(i + 10, 0, "completed", 1));
  assert.deepEqual(capacityPlan(6, [], recent, now), { active: 0, missing: 6, launch: 0, deferred: 6 });
  assert.equal(capacityPlan(6, [], recent, now + 11 * 60_000).launch, 6);
});

test("the launch budget bounds partial replenishment and healthy excess is never removed", () => {
  const active = [run(1, 1), run(2, 2)];
  const recent = Array.from({ length: 5 }, (_, i) => run(i + 10, 0, "completed", 1));
  assert.deepEqual(capacityPlan(6, active, recent, now), { active: 2, missing: 4, launch: 1, deferred: 3 });
  assert.equal(capacityPlan(1, active, [], now).launch, 0);
});

test("malformed inventory fails closed", () => {
  for (const entry of [{ ...run(1, 1), id: null }, { ...run(1, 1), status: "unknown" }, { ...run(1, 1), created_at: "bad" }]) {
    assert.throws(() => capacityPlan(10, [entry], [entry], now));
  }
});

test("disabled providers do not contact either service", async () => {
  const f = fixture({ fail() { throw new Error("must not call API"); } });
  assert.deepEqual(await reconcileCapacity({ MACHINE_COUNT: "0" }, f.options), { target: 0, launched: 0 });
  assert.equal(f.calls.length, 0);
});

test("a ten-host provider with a target of fifteen requests exactly five fresh machines", async () => {
  const f = fixture({ active: Array.from({ length: 10 }, (_, i) => run(i + 1, i + 1)) });
  const result = await reconcileCapacity(env, f.options);
  assert.equal(result.launched, 5);
  const starts = f.calls.filter((c) => c.method === "POST");
  assert.equal(starts.length, 5);
  for (const request of starts) assert.deepEqual(request, {
    path: "/repos/school/Runners/actions/workflows/machine.yml/dispatches", method: "POST", body: { ref: "main", inputs: { machine: "0" } },
  });
  assert.ok(f.calls.every((c) => c.method === "POST" || !c.method));
});

test("a repeat check sees pending jobs and requests no more capacity", async () => {
  const active = [...Array.from({ length: 10 }, (_, i) => run(i + 1, i + 1)), ...Array.from({ length: 5 }, (_, i) => run(i + 11, 0, "queued", 1))];
  const f = fixture({ active });
  assert.equal((await reconcileCapacity(env, f.options)).launched, 0);
  assert.ok(f.calls.every((c) => !c.method));
});

test("an unavailable host report does not duplicate an already active provider job", async () => {
  const f = fixture({ active: [run(1, 1)], status: { fleetMode: "passive", groups: [] } });
  const result = await reconcileCapacity({ ...env, MACHINE_COUNT: "1" }, f.options);
  assert.equal(result.joined, 0);
  assert.equal(result.launched, 0);
});

test("dry runs compute the deficit without dispatching", async () => {
  const f = fixture();
  const result = await reconcileCapacity({ ...env, MACHINE_COUNT: "2", CAPACITY_DRY_RUN: "1" }, f.options);
  assert.equal(result.launch, 2);
  assert.equal(result.launched, 0);
  assert.ok(f.calls.every((c) => !c.method));
});

test("API and controller errors cannot turn an unreadable fleet into an empty fleet", async () => {
  const f = fixture({ fail(path) { if (path.includes("status=queued")) throw new Error("API unavailable"); } });
  await assert.rejects(reconcileCapacity(env, f.options), /API unavailable/);
  assert.ok(f.calls.every((c) => !c.method));
  const legacy = fixture({ status: { fleetMode: "managed", groups: [] } });
  await assert.rejects(reconcileCapacity(env, legacy.options), /passive controller/);
  assert.equal(legacy.calls.length, 0);
});

test("active listings are paginated and not truncated to the newest hundred", async () => {
  const f = fixture();
  const baseApi = f.options.api;
  f.options.api = async (path, options) => {
    const url = new URL(path, "https://api.example.test");
    if (url.searchParams.get("status") === "in_progress") {
      f.calls.push({ path });
      const page = Number(url.searchParams.get("page"));
      return { workflow_runs: page === 1 ? Array.from({ length: 100 }, (_, i) => run(i + 1, i + 1)) : [run(101, 101)] };
    }
    return baseApi(path, options);
  };
  const result = await reconcileCapacity(env, f.options);
  assert.equal(result.active, 101);
  assert.equal(result.launched, 0);
  assert.ok(f.calls.some((c) => c.path.includes("status=in_progress") && c.path.includes("page=2")));
});

test("an ambiguous dispatch failure is not retried within the same reconciliation", async () => {
  const f = fixture({ fail(_path, options, calls) {
    if (options.method === "POST" && calls.filter((c) => c.method === "POST").length === 2) throw new Error("dispatch connection lost");
  } });
  await assert.rejects(reconcileCapacity({ ...env, MACHINE_COUNT: "3" }, f.options), /dispatch connection lost/);
  assert.equal(f.calls.filter((c) => c.method === "POST").length, 2);
});

test("GitHub HTTP errors omit response bodies and credential values", async () => {
  const client = githubClient("test-private-credential", undefined, async () => new Response("private error payload", { status: 403 }));
  await assert.rejects(client("/repos/example/runner"), (error) => {
    assert.match(error.message, /HTTP 403/);
    assert.ok(!error.message.includes("test-private-credential"));
    assert.ok(!error.message.includes("private error payload"));
    return true;
  });
});
