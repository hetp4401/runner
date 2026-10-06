// Phase 4: a copy failing on one server moves once (not when its version fails on 2 servers, not in an outage); sick
// servers are quarantined and ask to leave (not 3 at once); the hot rule moves the app making a server hot (not for
// steal, not for small apps; a server still hot after a move is suspect); the thrash breaker; moves in flight stay
// under min(8, 10% of the settled servers), one arrival per server.
import { makeControl, Fleet, deploy, ok, summary, placed, MIN, T } from "./lib.mjs";

const app = (name, replicas, port = 8080) => ({ port, replicas, compose: `x-runner:\n  port: ${port}\n  replicas: ${replicas}\nservices:\n  app:\n    image: ${name}\n    ports: ["${port}:${port}"]\n` });
const events = (c, kind, f = {}) => c.all("SELECT * FROM events WHERE kind = ? ORDER BY id", kind).filter((e) => Object.entries(f).every(([k, v]) => e[k] === v));
const onMachine = (c, name, k) => c.copiesOf(name).find((x) => x.replica === k && !x.leaving)?.machine;
const newest = (f, m) => f.alive().filter((s) => s.machine === m).sort((a, b) => b.started - a.started)[0];
const allHealthy = (c, f, name) => c.copiesOf(name).every((x) => newest(f, x.machine)?.status[x.key]?.s === "healthy");

{ // 1. a copy failing on one server while healthy elsewhere moves once, 10 minutes in, and its app isn't placed there
  //    again; when its version then fails on a second server too, it's the app: nothing more moves, an alert says so
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 4, cap: 20 }, b: { size: 4, cap: 20 } }, { drainAt: () => null });
  for (const p of ["a", "b"]) await f.startPool(p);
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 3));
  await f.run(10 * MIN);
  const x = onMachine(c, "web", 2);
  f.crash.add(`web@${x}`);
  const t0 = T;
  let movedAt = null;
  await f.run(16 * MIN, () => { if (!movedAt && events(c, "move-start", { cause: "failed" }).length) movedAt = T; });
  const ev = events(c, "move-start", { cause: "failed", app: "web" });
  ok("the failing copy moved, once", ev.length === 1 && ev[0].machine === x && ev[0].replica === 2, JSON.stringify(ev.map((e) => [e.replica, e.machine, e.other])));
  ok("10 minutes after it failed", movedAt && movedAt - t0 >= 10 * MIN && movedAt - t0 <= 12 * MIN, `${movedAt ? Math.round((movedAt - t0) / 1000) : "-"} s`);
  const y = onMachine(c, "web", 2);
  ok("replica 2 runs healthy elsewhere, and the failing copy is gone", y && y !== x && !c.copiesOn("web", x).length && allHealthy(c, f, "web"), placed(c, "web").join(" "));
  ok("its app isn't placed on that server again for now", c.failedHere("web", x, T));
  f.crash.add(`web@${y}`);
  await f.run(14 * MIN);
  ok("failing on a second server: no second move", events(c, "move-start", { cause: "failed", app: "web" }).length === 1);
  ok("an alert says it's the app", events(c, "alert", { cause: "fails-everywhere", app: "web" }).length === 1, events(c, "alert").map((e) => e.detail).join(" | "));
}

{ // 2. in an outage (every copy failing), no server is to blame: once two copies are back, the third moves 10 minutes
  //    after that, not at once
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 6, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 3));
  await f.run(10 * MIN);
  const ms = [1, 2, 3].map((k) => onMachine(c, "web", k));
  for (const m of ms) f.crash.add(`web@${m}`);
  await f.run(15 * MIN);
  ok("nothing moves while every copy fails", events(c, "move-start", { app: "web" }).length === 0);
  f.crash.delete(`web@${ms[0]}`);
  f.crash.delete(`web@${ms[1]}`);
  const back = T;
  let movedAt = null;
  await f.run(14 * MIN, () => { if (!movedAt && events(c, "move-start", { cause: "failed" }).length) movedAt = T; });
  ok("the copy still failing moves 10 minutes after the others came back", movedAt && movedAt - back >= 10 * MIN, `${movedAt ? Math.round((movedAt - back) / 1000) : "-"} s`);
}

{ // 3. a server where two apps fail while healthy elsewhere gets strikes, is quarantined (not a destination) and asks
  //    to leave: its copies move out, and its pool hands the slot to a fresh server
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 5, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 5));
  deploy(c, "api", app("api", 5, 9000));
  await f.run(10 * MIN);
  const s = f.alive().find((y) => c.copiesOn("web", y.machine).length && c.copiesOn("api", y.machine).length);
  f.crash.add(`web@${s.run}`);
  f.crash.add(`api@${s.run}`);
  await f.run(14 * MIN);
  ok("strikes against it", events(c, "strike", { run: s.run }).length >= 2, events(c, "strike", { run: s.run }).map((e) => `${e.cause}: ${e.detail}`).join(" | "));
  ok("quarantined as sick", events(c, "quarantine", { run: s.run, cause: "sick" }).length === 1);
  ok("it asked to leave", events(c, "drain", { run: s.run, cause: "sick" }).length === 1);
  ok("nothing was placed on it after", !c.all("SELECT * FROM events WHERE kind IN ('place', 'move-start') AND t > ?", events(c, "quarantine", { run: s.run })[0]?.t ?? T)
    .some((e) => (e.kind === "place" ? e.machine : e.other) === s.machine));
  await f.run(15 * MIN);
  ok("a fresh server took its slot", !s.alive && f.alive().some((y) => y.machine === s.machine && y.run !== s.run), f.log.slice(-3).join(" | "));
  ok("every replica of both apps runs healthy", ["web", "api"].every((n) => c.copiesOf(n).filter((x) => !x.leaving).length === 5 && allHealthy(c, f, n)),
    `${placed(c, "web").join(" ")} / ${placed(c, "api").join(" ")}`);
}

{ // 4. three servers sick at once is probably an app: none is quarantined, and an alert says so
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 8, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 8));
  deploy(c, "api", app("api", 8, 9000));
  await f.run(10 * MIN);
  for (const s of f.alive().slice(0, 3)) {
    f.crash.add(`web@${s.run}`);
    f.crash.add(`api@${s.run}`);
  }
  await f.run(14 * MIN);
  ok("no server quarantined as sick", events(c, "quarantine", { cause: "sick" }).length === 0);
  ok("an alert says it's probably an app", events(c, "alert", { cause: "many-sick" }).length >= 1, events(c, "alert").map((e) => e.detail).join(" | "));
}

{ // 5. the hot rule: the app making a server hot (4 of 5 minutes over 85% CPU) moves to a server with room; steal isn't
  //    heat; a server hot from no app of ours moves nothing; still hot 10 minutes after a move, a server is suspect;
  //    the same replica due to move for heat again within 2 hours trips the breaker: everything pauses for an hour
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 6, cap: 20 } }, { drainAt: () => null, cpuFromApps: true });
  await f.startPool("a");
  await f.run(4 * MIN);
  f.appCpu.set("cruncher", 200); // half of a 4-core server
  deploy(c, "cruncher", app("cruncher", 1, 7000));
  deploy(c, "web", app("web", 6));
  await f.run(10 * MIN);
  const x = onMachine(c, "cruncher", 1);
  const [stolen, busy] = f.alive().map((s) => s.machine).filter((m) => m !== x);
  f.steal.set(stolen, 40);
  f.cpu.set(stolen, 95);
  f.cpu.set(busy, 95);
  f.cpu.set(x, 45); // + 50 for the cruncher
  await f.run(8 * MIN);
  let ev = events(c, "move-start", { cause: "hot" });
  ok("the cruncher moved off its hot server", ev.length === 1 && ev[0].app === "cruncher" && ev[0].machine === x, JSON.stringify(ev.map((e) => [e.app, e.machine, e.other])));
  ok("to a server with room", ev.length === 1 && ![stolen, busy].includes(ev[0].other));
  ok("steal isn't heat: nothing moved off the server it's stolen from", !ev.some((e) => e.machine === stolen));
  ok("hot from no app of ours: nothing moves, and a note says why", !ev.some((e) => e.machine === busy) &&
    events(c, "rebalance").some((e) => e.machine === busy && e.detail.includes("15%")));
  const y = ev[0]?.other;
  f.cpu.set(x, 95); // the old server stays hot without it
  await f.run(12 * MIN);
  ok("still hot 10 minutes after the move: suspect, not a destination", events(c, "quarantine", { machine: x, cause: "hot" }).length === 1);
  f.cpu.set(y, 45); // the cruncher's new server gets hot too
  await f.run(40 * MIN);
  ev = events(c, "move-start", { cause: "hot" });
  ok("the cruncher wasn't moved for heat a second time", ev.length === 1, JSON.stringify(ev.map((e) => [e.app, e.machine, e.other])));
  ok("the breaker paused automatic moves", events(c, "pause", { cause: "thrash" }).length === 1 && c.paused(T) && c.status().rebalance.paused > T,
    events(c, "pause").map((e) => e.detail).join(" | "));
  ok("and raised an alert", events(c, "alert", { cause: "paused" }).length === 1);
}

{ // 6. un-stacking after new servers arrive goes in parallel, never more than min(8, 10% of the settled servers) moves
  //    in flight, one arrival per server
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 10, cap: 20 }, b: { size: 10, cap: 20 }, c: { size: 10, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 20));
  await f.run(4 * MIN);
  const most = () => Math.max(...Object.values(c.copiesOf("web").filter((x) => !x.leaving).reduce((m, x) => ({ ...m, [x.machine]: (m[x.machine] ?? 0) + 1 }), {})));
  ok("20 copies on 10 servers: two each", c.copiesOf("web").length === 20 && most() === 2);
  await f.startPool("b");
  await f.startPool("c");
  let maxInFlight = 0;
  let maxArrivals = 0;
  await f.run(40 * MIN, () => {
    const to = c.copiesOf("web").filter((x) => x.leaving).map((x) => x.leaving.to);
    maxInFlight = Math.max(maxInFlight, to.length);
    maxArrivals = Math.max(maxArrivals, ...to.map((m) => to.filter((y) => y === m).length), 0);
  });
  ok("every copy on its own server now", most() === 1, placed(c, "web").join(" "));
  ok("moves went in parallel, at most 3 at once (10% of 30 servers)", maxInFlight >= 2 && maxInFlight <= 3, `max ${maxInFlight}`);
  ok("one arrival per server at a time", maxArrivals === 1, `max ${maxArrivals}`);
}

{ // 7. strikes for a nearly full disk, and for a copy restarting over and over while healthy elsewhere: two within 30
  //    minutes quarantine the server
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 5, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 5));
  await f.run(10 * MIN);
  const [d, r] = f.alive();
  f.disk.set(d.machine, 0.05);
  f.restartLoop.add(`web@${r.run}`);
  await f.run(20 * MIN);
  ok("a nearly full disk: quarantined", events(c, "quarantine", { run: d.run, cause: "sick" }).length === 1, events(c, "strike", { run: d.run }).map((e) => e.detail).join(" | "));
  ok("a copy restarting over and over: quarantined", events(c, "quarantine", { run: r.run, cause: "sick" }).length === 1, events(c, "strike", { run: r.run }).map((e) => e.detail).join(" | "));
  ok("one sick server per pool asks to leave at a time", events(c, "drain", { cause: "sick" }).length === 1);
}

summary();
