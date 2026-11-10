import assert from "node:assert/strict";
import test from "node:test";
import { ACTIVE_STATUSES, capacityPlan, githubClient, reconcileCapacity, targetCount } from "./capacity.mjs";
import { verifyScheduledOrigin } from "./verify-schedule.mjs";

const now = Date.parse("2026-11-10T12:00:00Z");
const run = (id, slot, status = "in_progress", age = 60) => ({ id, display_title: `machine ${slot}`, status,
  created_at: new Date(now - age * 60_000).toISOString() });
const env = { GITHUB_REPOSITORY: "school/Runners", MACHINE_REF: "main", MACHINE_COUNT: "15",
  SERVER_TAG: "school", GITHUB_EVENT_NAME: "schedule", GITHUB_RUN_ID: "1234" };

function fixture({ active = [], recent = [], fail } = {}) {
  const calls = [];
  return { calls, options: { now: () => now, log() {},
    api: async (path, options = {}) => {
      calls.push({ path, ...options });
      if (fail) await fail(path, options);
      if (options.method === "POST") return null;
      const url = new URL(path, "https://api.example.test"), state = url.searchParams.get("status");
      return { workflow_runs: state ? active.filter(r => r.status === state) : recent };
    },
  } };
}

test("targets remain provider settings with legacy aliases and quantity validation", () => {
  assert.equal(targetCount({}), 0);
  assert.equal(targetCount({ POOL_SIZE: "15" }), 15);
  assert.equal(targetCount({ MACHINE_COUNT: "6", POOL_SIZE: "15" }), 6);
  assert.equal(targetCount({ MACHINE_COUNT: "0", POOL_SIZE: "15" }), 0);
  for (const value of ["-1", "1.5", "01", "51", "many", " "]) assert.throws(() => targetCount({ MACHINE_COUNT: value }));
});

test("old handovers reserve one slot, while each scheduled job reserves its own host", () => {
  const active = [run(1, 7), run(2, 7, "queued"), run(3, 8), run(4, "scheduled 100"), run(5, "scheduled 101", "pending")];
  const plan = capacityPlan(6, active, [], now);
  assert.equal(plan.active, 4);
  assert.equal(plan.jobs, 5);
  assert.equal(plan.missing, 2);
  assert.equal(plan.launch, 1);
  assert.equal(plan.deferred, 1);
});

test("every pending state reserves capacity even if the controller is offline", async () => {
  const active = ACTIVE_STATUSES.map((state, i) => run(i + 1, i + 1, state));
  const f = fixture({ active });
  assert.equal((await reconcileCapacity({ ...env, MACHINE_COUNT: "5" }, f.options)).launched, 0);
  assert.ok(f.calls.every(c => c.path.startsWith('/repos/school/Runners/actions/') && !c.method));
});

test("a missing cohort is refilled one host at a time, at least ten minutes apart", () => {
  let active = [], recent = [];
  for (let minute = 0; minute < 60; minute++) {
    const at = now + minute * 60_000;
    const plan = capacityPlan(6, active, recent, at);
    assert.equal(plan.launch, minute % 10 === 0 ? 1 : 0);
    if (plan.launch) {
      const r = { ...run(minute + 1, `scheduled ${minute}`, "queued"), created_at: new Date(at).toISOString() };
      active.push(r); recent.push(r);
    }
  }
  assert.equal(active.length, 6);
});

test("bunched schedules and failed launches cannot burst-launch replacements", () => {
  const recent = [run(10, "scheduled 500", "completed", 1)];
  assert.equal(capacityPlan(15, [], recent, now).launch, 0);
  assert.equal(capacityPlan(15, [], recent, now).retryAfterMs, 9 * 60_000);
  assert.equal(capacityPlan(15, [], recent, now + 9 * 60_000).launch, 1);
});

test("an aging host gets at most one overlap host and healthy jobs are never cancelled", async () => {
  const active = [run(1, 1, "in_progress", 220), run(2, 2)];
  assert.equal(capacityPlan(2, active, [], now).launch, 1);
  active.push(run(3, "scheduled 50", "queued", 1));
  const f = fixture({ active });
  const result = await reconcileCapacity({ ...env, MACHINE_COUNT: "2" }, f.options);
  assert.equal(result.launched, 0);
  assert.ok(f.calls.every(c => !c.method));
  assert.equal(capacityPlan(1, active, [], now).launch, 0);
});

test("waiting jobs and a young same-slot successor do not cause extra overlap launches", () => {
  assert.equal(capacityPlan(1, [run(1, 1, "queued", 240)], [], now).aging, 0);
  const active = [run(1, 1, "in_progress", 240), run(2, 1, "in_progress", 20)];
  assert.equal(capacityPlan(1, active, [], now).aging, 0);
  assert.equal(capacityPlan(1, active, [], now).launch, 0);
  const delayed = { ...run(1, 1, "in_progress", 240), run_started_at: new Date(now - 30 * 60_000).toISOString() };
  assert.equal(capacityPlan(1, [delayed], [], now).aging, 0);
});

test("actual job count caps overlap during migration from old handovers", () => {
  const active = [run(1, 1), run(2, 1), run(3, 2), run(4, 2)];
  const plan = capacityPlan(3, active, [], now);
  assert.equal(plan.missing, 1);
  assert.equal(plan.jobs, 4);
  assert.equal(plan.launch, 0);
});

test("later listings resolve completion races and malformed inventory fails closed", () => {
  assert.equal(capacityPlan(3, [run(1, 1), run(2, 2)], [run(1, 1, "completed"), run(2, 2)], now).active, 1);
  for (const entry of [{ ...run(1, 1), id: null }, { ...run(1, 1), status: "unknown" },
    { ...run(1, 1), created_at: "bad" }, { ...run(1, 1), run_started_at: "bad" }]) {
    assert.throws(() => capacityPlan(10, [entry], [], now));
  }
});

test("disabled providers do not contact any service", async () => {
  const f = fixture({ fail() { throw new Error("must not call API"); } });
  assert.deepEqual(await reconcileCapacity({ MACHINE_COUNT: "0" }, f.options), { target: 0, launched: 0 });
});

test("scheduled dispatches carry their GitHub run ID and require no control-plane endpoint", async () => {
  const f = fixture({ active: Array.from({ length: 10 }, (_, i) => run(i + 1, i + 1)) });
  const result = await reconcileCapacity(env, f.options);
  assert.equal(result.launched, 1);
  assert.deepEqual(f.calls.filter(c => c.method), [{
    path: "/repos/school/Runners/actions/workflows/machine.yml/dispatches", method: "POST",
    body: { ref: "main", inputs: { schedule_run: "1234" } },
  }]);
});

test("manual workflows, completion events, local runs and explicit dry runs never dispatch", async () => {
  for (const override of [{ GITHUB_EVENT_NAME: "workflow_dispatch" }, { GITHUB_EVENT_NAME: "workflow_run" },
    { GITHUB_EVENT_NAME: undefined }, { CAPACITY_DRY_RUN: "1" }]) {
    const f = fixture();
    const result = await reconcileCapacity({ ...env, ...override }, f.options);
    assert.equal(result.launch, 1);
    assert.equal(result.launched, 0);
    assert.ok(f.calls.every(c => !c.method));
  }
});

test("unreadable GitHub inventory and ambiguous dispatch errors fail without retrying a launch", async () => {
  for (const fail of [
    path => { if (path.includes("status=queued")) throw new Error("API unavailable"); },
    (_path, options) => { if (options.method === "POST") throw new Error("dispatch connection lost"); },
  ]) {
    const f = fixture({ fail });
    await assert.rejects(reconcileCapacity(env, f.options));
    assert.ok(f.calls.filter(c => c.method === "POST").length <= 1);
  }
});

test("active inventory is paginated beyond the newest hundred", async () => {
  const f = fixture(), api = f.options.api;
  f.options.api = async (path, options) => {
    const url = new URL(path, "https://api.example.test");
    if (url.searchParams.get("status") === "in_progress") {
      f.calls.push({ path });
      return { workflow_runs: url.searchParams.get("page") === "1"
        ? Array.from({ length: 100 }, (_, i) => run(i + 1, i + 1)) : [run(101, 101)] };
    }
    return api(path, options);
  };
  const result = await reconcileCapacity(env, f.options);
  assert.equal(result.active, 101);
  assert.equal(result.launched, 0);
});

test("machine jobs accept only their own branch's scheduled watchdog as origin", async () => {
  const launchEnv = { SCHEDULE_RUN: "1234", GITHUB_REPOSITORY: "school/Runners", GITHUB_REF_NAME: "main" };
  const origin = { event: "schedule", path: ".github/workflows/watchdog.yml", head_branch: "main" };
  assert.equal(await verifyScheduledOrigin(launchEnv, async path => {
    assert.equal(path, "/repos/school/Runners/actions/runs/1234"); return origin;
  }), true);
  for (const change of [{ event: "workflow_dispatch" }, { event: "workflow_run" },
    { path: ".github/workflows/machine.yml" }, { head_branch: "other" }]) {
    await assert.rejects(verifyScheduledOrigin(launchEnv, async () => ({ ...origin, ...change })), /scheduled watchdog/);
  }
  await assert.rejects(verifyScheduledOrigin({ ...launchEnv, SCHEDULE_RUN: "../42" }), /scheduled watchdog run/);
});

test("GitHub HTTP errors omit response bodies and credentials", async () => {
  const client = githubClient("test-private-credential", undefined, async () => new Response("private error payload", { status: 403 }));
  await assert.rejects(client("/repos/example/runner"), error => {
    assert.match(error.message, /HTTP 403/);
    assert.ok(!error.message.includes("test-private-credential"));
    assert.ok(!error.message.includes("private error payload"));
    return true;
  });
});
