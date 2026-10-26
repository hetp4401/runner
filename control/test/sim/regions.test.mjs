// Region constraints across placement, capacity, app updates, moves, eviction,
// unknown geolocation, process restarts and same-slot replacements in another region.
import { makeControl, Fleet, deploy, ok, summary, MIN, T } from "./lib.mjs";
import { regionOf } from "../../src/regions.js";

const locations = {
  west: "Phoenix, Arizona, US · Microsoft Azure Cloud (westus3)",
  west2: "San Francisco, California, US · Microsoft Azure Cloud (westus)",
  central: "Des Moines, Iowa, US · Microsoft Azure Cloud (centralus)",
  east: "Boydton, Virginia, US · Microsoft Azure Cloud (eastus)",
};
const app = (region = null, replicas = 1, fixed = false) => ({
  compose: `x-runner:\n  port: 8080\n  replicas: ${replicas}\n${region == null ? "" : Array.isArray(region) ? `  regions: ${JSON.stringify(region)}\n` : `  region: ${region}\n`}services:\n  web:\n    image: traefik/whoami\n    ports: ["8080:8080"]\n${fixed ? "    container_name: region-fixed\n" : ""}`,
});

async function setup(kinds = ["west", "west2", "central", "east"], opts = {}) {
  const c = makeControl();
  const f = new Fleet(c, { a: { size: kinds.length, cap: 20 } }, { drainAt: () => null, build: MIN, ...opts });
  const where = new Map();
  f.sync = async (s, extra = {}) => {
    const i = f.servers.indexOf(s) + 1;
    const ip = `203.0.113.${i}`;
    const text = locations[where.get(s.agent)] ?? null;
    if (text && c.geo.get(ip)?.text !== text) c.sql.exec("INSERT OR REPLACE INTO geo (ip, text, at) VALUES (?, ?, ?)", ip, text, T);
    c.geo.set(ip, { text });
    const plan = c.sync(f.body(s, extra), ip);
    s.lastPlan = plan;
    return plan;
  };
  await f.startPool("a");
  f.servers.forEach((s, i) => where.set(s.agent, kinds[i]));
  await f.run(5 * MIN);
  return { c, f, where };
}
const staying = (c, name = "web") => c.copiesOf(name).filter((x) => !x.leaving);
const regionFor = (c, machine) => regionOf(c.liveMachines(T).get(machine)?.location);
const allIn = (c, region, name = "web") => staying(c, name).every((x) => regionFor(c, x.machine) === region);

for (const [state, expected] of [
  ["Arizona", "us-west"], ["California", "us-west"], ["Washington", "us-west"], ["Wyoming", "us-west"],
  ["Iowa", "us-central"], ["Illinois", "us-central"], ["Virginia", "us-east"],
]) ok(`${state} maps to ${expected}`, regionOf(`Any city, ${state}, US · provider`) === expected);
ok("unknown and non-US locations aren't guessed", [null, "", "London, England, GB", "Paris, Texas, FR"].every((v) => regionOf(v) === null));

{
  const { c, f } = await setup();
  for (const invalid of ["phoenix", "westus3", "eu-west", "true", "[]", "42"] ) {
    let rejected = false;
    try { deploy(c, "invalid", app(invalid)); } catch (e) { rejected = e.status === 400 && /x-runner.region/.test(e.message); }
    ok(`invalid region ${invalid} is rejected before deployment`, rejected);
  }
  deploy(c, "web", app("us-west", 3));
  await f.run(4 * MIN);
  ok("all replicas stay in-region, doubling there before using another region", staying(c).length === 3 && allIn(c, "us-west"));
  ok("app policy, run regions and deduplicated region counts are exposed", c.describe(c.project("web")).region === "us-west" && c.status().regions.find((r) => r.name === "us-west").machines === 2 && c.status().runs.every((r) => r.region));
  deploy(c, "global", app(null, 4));
  await f.run(3 * MIN);
  ok("an app without a region still uses the whole fleet", new Set(staying(c, "global").map((x) => regionFor(c, x.machine))).size === 3);
  let rejected = false;
  try { c.move("web", staying(c)[0].machine, f.alive()[3].machine, staying(c)[0].replica); }
  catch (e) { rejected = e.status === 409 && /requires us-west/.test(e.message); }
  ok("manual moves cannot bypass the app's region", rejected);
}

{
  const { c, f, where } = await setup(["east", "unknown"]);
  deploy(c, "web", app("us-west"));
  await f.run(MIN);
  ok("missing region capacity waits instead of falling back", !staying(c).length && /us-west/.test(c.describe(c.project("web")).blocked));
  where.set(f.alive()[1].agent, "west");
  await f.run(3 * MIN);
  ok("placement resumes when a runner's location is established", staying(c).length === 1 && allIn(c, "us-west"));
  const restarted = makeControl({}, c.db);
  const s = f.alive()[1];
  restarted.sync(f.body(s), `203.0.113.${f.servers.indexOf(s) + 1}`);
  ok("region policy and cached locations survive a control-plane restart", restarted.describe(restarted.project("web")).region === "us-west" && regionOf(restarted.runs.get(s.run).location) === "us-west");
}

{
  const { c, f } = await setup(["west", "east"]);
  deploy(c, "web", app("us-west", 2, true));
  await f.run(3 * MIN);
  ok("fixed container names wait for in-region capacity", staying(c).length === 1 && allIn(c, "us-west") && Boolean(c.blocked.get("web")));
}

{
  const { c, f } = await setup(["west", "west2", "east"], { reportDeadline: true });
  const near = f.alive()[0];
  // The hard deadline is over an hour away, but the planned departure is in
  // twenty minutes. Low CPU must not make this the preferred destination.
  near.deadline = T + 65 * MIN;
  f.cpu.set(near.machine, 0);
  f.cpu.set(f.alive()[1].machine, 40);
  await f.run(MIN);
  deploy(c, "web", app("us-west"));
  ok("a planned departure in under 30 minutes loses to a fresh regional runner", staying(c)[0]?.machine === f.alive()[1].machine && c.dueAt(c.runs.get(near.run)) - T < 30 * MIN);
  await f.run(3 * MIN);
  const x = staying(c)[0];
  c.evict(x.machine, { force: true });
  const target = staying(c)[0];
  ok("eviction selects another runner in the same region", target.machine !== x.machine && regionFor(c, target.machine) === "us-west");
}

{
  const { c, f } = await setup(["east", "west", "west2"]);
  deploy(c, "web", app());
  await f.run(3 * MIN);
  const old = staying(c)[0];
  ok("the migration fixture starts in the east", regionFor(c, old.machine) === "us-east");
  deploy(c, "web", app("us-west"));
  ok("adding a region starts a replacement and retains the serving copy", c.copiesOf("web").some((x) => x.machine === old.machine && x.leaving) && c.desiredFor(old.machine).web);
  await f.run(4 * MIN);
  ok("adding a region moves existing replicas into it", staying(c).length === 1 && allIn(c, "us-west") && !c.copiesOf("web").some((x) => x.leaving));
  const x = staying(c)[0];
  c.move("web", x.machine, null, x.replica);
  deploy(c, "web", app("us-east"));
  await f.run(4 * MIN);
  ok("a region change cancels an in-flight move to the wrong region", allIn(c, "us-east") && c.recentEvents.some((e) => e.kind === "move-cancel" && /required region/.test(e.detail)));
}

{
  const { c, f, where } = await setup(["west", "west2", "east"], { reportDeadline: true });
  deploy(c, "web", app("us-west"));
  await f.run(4 * MIN);
  const old = f.alive().find((s) => s.machine === staying(c)[0].machine);
  const realStart = f.start.bind(f);
  f.start = async (pool, want) => {
    const s = await realStart(pool, want);
    if (s) where.set(s.agent, want === old.machine ? "east" : "west");
    return s;
  };
  c.drain({ agent: old.agent });
  let wrongBuild = false, dark = false, prematureTunnel = false;
  await f.run(10 * MIN, () => {
    const successor = f.alive().find((s) => s.machine === old.machine && s !== old);
    if (successor && Object.values(successor.lastPlan?.desired ?? {}).some((d) => d.app === "web")) wrongBuild = true;
    if (successor?.ready && c.copiesOn("web", old.machine).length) prematureTunnel = true;
    if (!c.copiesOf("web").some((x) => f.alive().some((s) => s.machine === x.machine && s.ready && s.status[x.key]?.s === "healthy"))) dark = true;
  });
  ok("a successor in another region never inherits the constrained app", !wrongBuild);
  ok("the successor's tunnel waits for the app's regional move", !prematureTunnel);
  ok("the old runner retires after a healthy regional replacement takes over", !old.alive && staying(c).length === 1 && allIn(c, "us-west"));
  ok("cross-region handover keeps the replica serving throughout", !dark);
}

{
  const { c, f, where } = await setup(["west", "west2", "west", "west2", "east"]);
  deploy(c, "web", app("us-west", 2));
  await f.run(4 * MIN);
  for (const x of staying(c)) {
    const s = f.alive().find((r) => r.machine === x.machine);
    where.set(s.agent, "east");
    f.crash.add(`web@${s.machine}`);
  }
  await f.run(4 * MIN);
  ok("several non-serving copies in the wrong region can recover without a budget deadlock", staying(c).length === 2 && allIn(c, "us-west") && staying(c).every((x) => c.copyHealthy("web", x, c.liveMachines(T).get(x.machine))));
}

const threeRegions = ["us-west", "us-central", "us-east"];
const assignmentsMatch = (c) => staying(c).every((x) => regionFor(c, x.machine) === threeRegions[(x.replica - 1) % 3]);

{
  const { c, f } = await setup(["west", "central", "east", "west2", "central", "east"]);
  for (const invalid of [[], ["unknown"], ["us-west", 42], Array(51).fill("us-west")]) {
    let rejected = false;
    try { deploy(c, "invalid", app(invalid, 3)); } catch (e) { rejected = e.status === 400 && /x-runner.regions/.test(e.message); }
    ok(`invalid region list (${invalid.length} entries) is rejected`, rejected);
  }
  let rejected = false;
  try { deploy(c, "invalid", { compose: app("us-west", 3).compose.replace("  region: us-west", "  region: us-west\n  regions: [us-east]") }); }
  catch (e) { rejected = e.status === 400 && /not both/.test(e.message); }
  ok("single and per-replica region policies cannot be combined", rejected);
  deploy(c, "web", app(threeRegions, 3));
  await f.run(4 * MIN);
  ok("a region list places exactly one replica in each region", staying(c).length === 3 && assignmentsMatch(c));
  const desc = c.describe(c.project("web"));
  ok("the API exposes the list and each replica's required region", JSON.stringify(desc.regions) === JSON.stringify(threeRegions) && desc.placed.every((x) => x.region === threeRegions[x.replica - 1]));
  const central = staying(c).find((x) => x.replica === 2);
  rejected = false;
  try { c.move("web", central.machine, staying(c).find((x) => x.replica === 1).machine, 2); }
  catch (e) { rejected = e.status === 409 && /requires us-central/.test(e.message); }
  ok("manual moves enforce the selected replica's region", rejected);
  const evicted = c.evict(central.machine, { force: true });
  await f.run(3 * MIN);
  ok("eviction keeps replica 2 in Central instead of using replica 1's region", evicted.moved.length === 1 && staying(c).find((x) => x.replica === 2).machine !== central.machine && assignmentsMatch(c));
  deploy(c, "web", { replicas: 4 });
  await f.run(3 * MIN);
  ok("replica-count-only changes preserve the list and repeat its assignments", staying(c).length === 4 && assignmentsMatch(c) && JSON.stringify(c.describe(c.project("web")).regions) === JSON.stringify(threeRegions));
}

{
  const { c, f } = await setup(["west", "east"]);
  deploy(c, "web", app(threeRegions, 3));
  await f.run(3 * MIN);
  ok("a missing Central region does not prevent the East replica starting", staying(c).length === 2 && staying(c).some((x) => x.replica === 3) && assignmentsMatch(c) && /us-central/.test(c.blocked.get("web")));
}

{
  const { c, f, where } = await setup(["west", "central", "east", "central"], { reportDeadline: true });
  deploy(c, "web", app(threeRegions, 3));
  await f.run(4 * MIN);
  const old = f.alive().find((s) => s.machine === staying(c).find((x) => x.replica === 2).machine);
  const realStart = f.start.bind(f);
  f.start = async (pool, want) => {
    const s = await realStart(pool, want);
    if (s) where.set(s.agent, "west");
    return s;
  };
  c.drain({ agent: old.agent });
  let wrongBuild = false, dark = false;
  await f.run(10 * MIN, () => {
    const successor = f.alive().find((s) => s.machine === old.machine && s !== old);
    if (successor && Object.values(successor.lastPlan?.desired ?? {}).some((d) => d.app === "web" && d.replica === 2)) wrongBuild = true;
    if (!c.copiesOf("web").some((x) => x.replica === 2 && f.alive().some((s) => s.machine === x.machine && s.ready && s.status[x.key]?.s === "healthy"))) dark = true;
  });
  ok("a West successor cannot inherit the Central replica from a region list", !wrongBuild && !old.alive && assignmentsMatch(c));
  ok("per-replica region handover preserves readiness", !dark);
}

{
  const { c, f } = await setup(["west", "central", "east", "central"]);
  deploy(c, "web", app(threeRegions, 3));
  await f.run(12 * MIN);
  const central = staying(c).find((x) => x.replica === 2);
  f.crash.add(`web@${central.machine}`);
  await f.run(20 * MIN);
  ok("failed-copy rebalancing uses the failed replica's Central assignment", staying(c).find((x) => x.replica === 2).machine !== central.machine && assignmentsMatch(c) && c.recentEvents.some((e) => e.kind === "move-start" && e.cause === "failed" && e.replica === 2));
}

summary();
