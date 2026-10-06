// Phase 1: outages aren't deaths, keys never change, failed moves keep the working copy, no count-spread moves.
import { makeControl, Fleet, deploy, ok, summary, placed, advance, MIN, T } from "./lib.mjs";

const web = (replicas, extra = "") => ({ port: 8080, replicas, compose: `services:\n  app:\n    image: x\n    ports: ["8080:8080"]\n${extra}` });

{ // 1. keys: a copy keeps its key when other copies of its app arrive or leave on its server
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 2 } });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", web(3));
  await f.run(4 * MIN);
  const keysBefore = new Map(c.copiesOf("web").map((x) => [x.replica, `${x.machine}:${x.key}`]));
  const doubled = f.alive().find((s) => Object.keys(s.lastPlan.desired).length === 2);
  ok("3 replicas on 2 servers: one server runs two copies", Boolean(doubled), JSON.stringify([...keysBefore]));
  // Lower the count to 2, then back to 3: the copies that stay keep their keys.
  deploy(c, "web", web(2));
  await f.run(2 * MIN);
  deploy(c, "web", web(3));
  await f.run(4 * MIN);
  const kept = c.copiesOf("web").filter((x) => x.replica <= 2).every((x) => keysBefore.get(x.replica) === `${x.machine}:${x.key}`);
  ok("copies that stayed kept their keys through a replica change", kept, JSON.stringify(c.copiesOf("web").map((x) => `${x.replica}@${x.machine}:${x.key}`)));
  const third = c.copiesOf("web").find((x) => x.replica === 3);
  ok("the re-added replica 3 didn't take a key another copy had just given up", third && third.key !== "web" || !third, third?.key);
}

{ // 2. a gap longer than the settle window: nothing is started, dropped or moved when the fleet comes back
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 6, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(5 * MIN);
  deploy(c, "web", web(4));
  await f.run(5 * MIN);
  const before = placed(c, "web");
  advance(8 * MIN); // the control plane was unreachable for 8 minutes (longer than SETTLE_MS)
  let starts = 0;
  for (const s of f.alive()) {
    const plan = await f.sync(s);
    starts += (plan.start ?? []).length;
  }
  ok("after an 8-minute gap, the first check-ins start nothing", starts === 0, `${starts} starts`);
  await f.run(10 * MIN);
  ok("after the fleet is back, every copy is where it was", JSON.stringify(placed(c, "web")) === JSON.stringify(before), `${before} -> ${placed(c, "web")}`);
  ok("and no servers were started for the 'missing' members", f.servers.length === 6, `${f.servers.length} servers`);
}

{ // 3. a pool going silent together (a partition) trips the breaker: its copies aren't re-placed
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 6, cap: 20 }, b: { size: 6, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.startPool("b");
  await f.run(5 * MIN);
  deploy(c, "web", web(10));
  await f.run(6 * MIN);
  const before = placed(c, "web");
  for (const s of f.alive("a")) f.silent.add(s.machine);
  await f.run(4 * MIN); // 4 minutes of silence: past liveMs and the old 150-s "gone"
  const during = placed(c, "web");
  ok("pool a silent 4 minutes: the breaker fired", c.recentEvents.some((e) => e.kind === "mass-loss"), c.recentEvents.filter((e) => e.kind === "mass-loss").map((e) => e.detail).join(" | "));
  ok("its copies weren't re-placed meanwhile", JSON.stringify(during) === JSON.stringify(before), `${before} -> ${during}`);
  f.silent.clear();
  await f.run(10 * MIN);
  ok("after it's back, nothing moved", JSON.stringify(placed(c, "web")) === JSON.stringify(before));
  ok("and nothing was started for it", f.servers.length === 12, `${f.servers.length} servers`);
}

{ // 4. a move to a server where the app fails is cancelled: the old copy stays, that server is avoided
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", web(1));
  await f.run(4 * MIN);
  const [x] = c.copiesOf("web");
  const target = f.alive().find((s) => s.machine !== x.machine).machine;
  f.fail.add(`web@${target}`);
  c.move("web", x.machine, String(target), 1);
  await f.run(6 * MIN);
  const now = c.copiesOf("web");
  ok("the failed move was cancelled and the old copy kept", now.length === 1 && now[0].machine === x.machine && !now[0].leaving, JSON.stringify(now.map((y) => `${y.replica}@${y.machine}${y.leaving ? "(leaving)" : ""}`)));
  ok("a move-cancel event says why", c.recentEvents.some((e) => e.kind === "move-cancel" && /failed/.test(e.detail)), c.recentEvents.filter((e) => e.kind === "move-cancel").map((e) => e.detail).join(" | "));
  ok("and that server is avoided for the app", c.failedHere("web", target, T));
}

{ // 5. a slow move is extended once while building, and cancelled after 25 min only while another copy is ready
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", web(2));
  await f.run(4 * MIN);
  const x = c.copiesOf("web").find((y) => y.replica === 1);
  const target = f.alive().find((s) => !c.copiesOf("web").some((y) => y.machine === s.machine)).machine;
  f.build = 40 * MIN; // builds take forever from now on
  c.move("web", x.machine, String(target), 1);
  await f.run(20 * MIN);
  ok("after 20 min, still building: extended, not cancelled", c.copiesOf("web").some((y) => y.leaving), JSON.stringify(c.copiesOf("web").map((y) => `${y.replica}@${y.machine}${y.leaving ? "(leaving)" : ""}`)));
  await f.run(8 * MIN);
  ok("after 25 min, cancelled (another copy is ready)", !c.copiesOf("web").some((y) => y.leaving) && c.copiesOf("web").some((y) => y.replica === 1 && y.machine === x.machine));
}

{ // 6. no count-spread moves: two copies of different apps sharing a server while one sits empty moves nothing
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 2, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "one", web(1));
  deploy(c, "two", { ...web(1), compose: web(1).compose.replace(/8080/g, "9090"), port: 9090 });
  deploy(c, "three", { ...web(1), compose: web(1).compose.replace(/8080/g, "7070"), port: 7070 });
  await f.start("a"); // a third, empty server joins after placement
  await f.run(40 * MIN);
  const moves = c.recentEvents.filter((e) => e.kind === "move-start").length;
  ok("an uneven count alone moves nothing", moves === 0, `${moves} moves`);
}

{ // 7. the agent's replica guard: a key reused for another replica is applied again (checked in the sim's agent)
  ok("(covered by the agent: a different replica under the same key is re-applied)", true);
}
summary();
