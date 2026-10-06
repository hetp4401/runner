// Phase 3: the control plane schedules departures (gates, earliest deadline first, ceilings), hands over on the same
// slot, moves copies out first when a pool can't start a successor or must shrink, holds a dead machine's slot for its
// pool's next machine, retires a successor that can't take over.
import { makeControl, Fleet, deploy, ok, summary, placed, MIN, T } from "./lib.mjs";

const app = (name, replicas, xr = "", port = 8080) => ({ port, replicas, compose: `x-runner:\n  port: ${port}\n  replicas: ${replicas}\n${xr}services:\n  app:\n    image: ${name}\n    ports: ["${port}:${port}"]\n` });

// Watches a fleet: departures in flight (per pool, fleet), departures started per 10 min, hard stops with copies,
// and per replica the seconds with no live ready server reporting it healthy ("dark").
function watcher(c, f, apps) {
  const w = { maxPool: 0, maxFleet: 0, max10: 0, hardStops: 0, dark: {}, starts: [], stops: 0 };
  const seen = new Set();
  w.each = () => {
    const live = c.liveRuns(T);
    const dep = c.departing(T);
    const byPool = {};
    for (const r of dep) byPool[r.pool] = (byPool[r.pool] ?? 0) + 1;
    w.maxPool = Math.max(w.maxPool, ...Object.values(byPool), 0);
    w.maxFleet = Math.max(w.maxFleet, dep.length);
    for (const d of c.departLog) if (!seen.has(`${d.run}|${d.t}`)) { seen.add(`${d.run}|${d.t}`); w.starts.push(d.t); }
    w.max10 = Math.max(w.max10, w.starts.filter((t) => T - t < 10 * MIN).length);
    for (const [name, n] of Object.entries(apps)) {
      for (let k = 1; k <= n; k++) {
        const lit = c.copiesOf(name).some((x) => x.replica === k && f.alive().some((s) => s.machine === x.machine && s.ready && s.status[x.key]?.s === "healthy"));
        if (!lit) w.dark[`${name}-${k}`] = (w.dark[`${name}-${k}`] ?? 0) + 20;
      }
    }
  };
  w.hard = () => f.log.filter((l) => l.includes("hard stop")).length;
  return w;
}

{ // 1. a whole fleet started together, deadlines reported: departures are spread, gated, early, warm
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 6, cap: 20 }, b: { size: 6, cap: 20 }, c: { size: 6, cap: 20 } }, { drainAt: () => null, reportDeadline: true, build: 60_000 });
  for (const p of ["a", "b", "c"]) await f.startPool(p);
  await f.run(5 * MIN);
  deploy(c, "web", app("web", 12));
  deploy(c, "kv", app("kv", 5, "  stateful: true\n  resync: 120\n", 7000));
  await f.run(10 * MIN);
  const w = watcher(c, f, { web: 12, kv: 5 });
  await f.run(7 * 60 * MIN, w.each);
  const lifetimes = f.servers.filter((s) => !s.alive).map((s) => Math.round((s.stoppedAt - s.started) / MIN));
  ok("every one of the first 18 servers left before its 355-minute hard stop", w.hard() === 0, `${w.hard()} hard stops`);
  ok("at most one departure in flight per pool", w.maxPool <= 1, `max ${w.maxPool}`);
  ok("at most 3 in flight fleet-wide", w.maxFleet <= 3, `max ${w.maxFleet}`);
  ok("at most 4 departures started in any 10 minutes", w.max10 <= 4, `max ${w.max10}`);
  ok("lifetimes between 3 and 6 hours", lifetimes.every((m) => m >= 180 && m <= 355), `${Math.min(...lifetimes)}-${Math.max(...lifetimes)} min over ${lifetimes.length} departures`);
  const darkTotal = Object.values(w.dark).reduce((a, b) => a + b, 0);
  ok("no replica went dark during departures", darkTotal === 0, JSON.stringify(w.dark));
  const moves = c.recentEvents.filter((e) => e.kind === "move-start").length;
  ok("same-slot handovers: no moves", moves === 0, `${moves} moves`);
}

{ // 2. a pool at its job cap can't start a successor: after 12 minutes it's marked capped and the run moves its copies
  //    out before leaving
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 3 }, b: { size: 3, cap: 20 } }, { drainAt: () => null, reportDeadline: true, build: 60_000 });
  await f.startPool("a");
  await f.startPool("b");
  await f.run(5 * MIN);
  deploy(c, "web", app("web", 6));
  await f.run(5 * MIN);
  const w = watcher(c, f, { web: 6 });
  const s0 = f.alive("a")[0];
  c.drain({ agent: s0.agent });
  await f.run(40 * MIN, w.each);
  ok("pool a was marked capped", c.recentEvents.some((e) => e.kind === "capped" && e.cause === "a"), c.recentEvents.filter((e) => e.kind === "capped").map((e) => e.detail).join(" | "));
  ok("the drained server left after moving its copies out", !s0.alive && c.recentEvents.some((e) => e.kind === "retire" && e.cause === "evicted" && e.machine === s0.machine));
  ok("no replica went dark", Object.keys(w.dark).length === 0, JSON.stringify(w.dark));
}

{ // 3. shrinking a pool: one server at a time leaves, each after its copies moved out
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 6, cap: 20 } }, { drainAt: () => null, reportDeadline: true, build: 60_000 });
  await f.startPool("a");
  await f.run(5 * MIN);
  deploy(c, "web", app("web", 6));
  await f.run(12 * MIN);
  const w = watcher(c, f, { web: 6 });
  c.putSettings({ pools: { a: 3 } });
  f.pools.a.size = 3;
  await f.run(40 * MIN, w.each);
  ok("the pool shrank to 3", f.alive("a").length === 3, `${f.alive("a").length} left`);
  ok("one eviction at a time", w.maxPool <= 1, `max ${w.maxPool}`);
  ok("no replica went dark", Object.keys(w.dark).length === 0, JSON.stringify(w.dark));
  ok("every web copy is placed on the remaining servers", c.copiesOf("web").filter((x) => !x.leaving).length === 6 && c.copiesOf("web").every((x) => f.alive().some((s) => s.machine === x.machine)), placed(c, "web").join(" "));
}

{ // 4. a server that dies without warning: its pool's next machine takes its slot and inherits its copies
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 4, cap: 20 } }, { drainAt: () => null, reportDeadline: true, build: 60_000 });
  await f.startPool("a");
  await f.run(5 * MIN);
  deploy(c, "web", app("web", 4));
  await f.run(12 * MIN);
  const victim = f.alive()[1];
  const eventsBefore = c.recentEvents.length;
  f.stop(victim, "crash"); // no last check-in
  await f.run(15 * MIN);
  const after = c.recentEvents.slice(eventsBefore);
  const back = f.alive().find((s) => s.machine === victim.machine);
  ok("a new server took the dead one's slot", Boolean(back), f.alive().map((s) => s.machine).join(","));
  ok("and inherited its copy: no move, no re-placement elsewhere", !after.some((e) => (e.kind === "place" || e.kind === "move-start") && e.app === "web"), after.filter((e) => e.app).map((e) => `${e.kind} ${e.app}#${e.replica}@${e.machine}`).join(", "));
  ok("its loss was recorded", after.some((e) => e.kind === "server-lost" && e.machine === victim.machine));
}

{ // 5. a successor that can't run an app its predecessor runs is retired after 25 minutes; the predecessor then moves
  //    its copies out instead
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 20 } }, { drainAt: () => null, reportDeadline: true, build: 60_000 });
  await f.startPool("a");
  await f.run(5 * MIN);
  deploy(c, "web", app("web", 3));
  await f.run(10 * MIN);
  const s0 = f.alive()[0];
  const realStart = f.start.bind(f);
  f.start = async (pool, want) => { const s = await realStart(pool, want); if (s && want === s0.machine) f.fail.add(`web@${s0.machine}`); return s; };
  c.drain({ agent: s0.agent });
  await f.run(45 * MIN);
  ok("the stuck successor was retired", c.recentEvents.some((e) => e.kind === "retire" && e.cause === "stuck-successor"), c.recentEvents.filter((e) => e.kind === "retire").map((e) => e.cause).join(","));
  ok("the drained server still left, warm", !s0.alive && f.log.every((l) => !l.includes("hard stop")));
}
summary();
