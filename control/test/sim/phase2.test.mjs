// Phase 2: every app is treated alike (an x-runner.stateful left in a spec changes nothing), readiness, replica changes
// recreate nothing, placement filters (deadline, starting servers), budgeted evictions, spreading across pools.
import { makeControl, Fleet, deploy, ok, summary, placed, MIN, T } from "./lib.mjs";

const app = (name, replicas, xr = "", port = 8080) => ({ port, replicas, compose: `x-runner:\n  port: ${port}\n  replicas: ${replicas}\n${xr}services:\n  app:\n    image: ${name}\n    ports: ["${port}:${port}"]\n` });
const poolsOf = (c, f, name) => {
  const by = {};
  for (const x of c.copiesOf(name).filter((y) => !y.leaving)) {
    const s = f.alive().find((y) => y.machine === x.machine);
    by[s?.pool ?? "?"] = (by[s?.pool ?? "?"] ?? 0) + 1;
  }
  return by;
};

{ // 1. x-runner.stateful is gone: a spec that still says it deploys, and is placed like any app (here, with more
  //    replicas than servers, a server runs two copies)
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 2, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "kv", app("kv", 3, "  stateful: true\n  resync: 120\n", 7000));
  await f.run(4 * MIN);
  ok("all 3 copies placed on 2 servers", c.copiesOf("kv").length === 3 && c.blocked.get("kv") == null, placed(c, "kv").join(" "));
  ok("the budget is the usual one: a fifth of the replicas, at least one", c.budget("kv") === 1);
}

{ // 3. a replica-count change recreates nothing: the copies that stay get the same files and .env
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 4, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 2));
  await f.run(4 * MIN);
  const s1 = f.alive().find((s) => c.copiesOn("web", s.machine).some((x) => x.replica === 1));
  const before = c.desiredFor(s1.machine);
  deploy(c, "web", app("web", 3));
  await f.run(1 * MIN);
  const after = c.desiredFor(s1.machine);
  const key = Object.keys(before)[0];
  const services = (d) => JSON.stringify((d.compose.match(/^services:[\s\S]*/m) ?? [""])[0]);
  ok("replica 1's files, .env and services are identical after 2 -> 3 (compose ignores x-runner)", JSON.stringify(before[key].files) === JSON.stringify(after[key].files) && services(before[key]) === services(after[key]),
    `${before[key]?.files?.[".env"]?.length} vs ${after[key]?.files?.[".env"]?.length} bytes of .env`);
  ok("and .env has no FLEET_REPLICAS or FLEET_VERSION", !/FLEET_REPLICAS|FLEET_VERSION/.test(after[key].files[".env"]));
}

{ // 4. readiness: a copy that answers but isn't ready isn't healthy (a move to it doesn't finish), and the app's
  //    ready path reaches the agent
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "db", app("db", 1, "  ready: /healthz\n", 5432));
  await f.run(4 * MIN);
  const [x] = c.copiesOf("db");
  const s = f.alive().find((y) => y.machine === x.machine);
  ok("the ready path is sent with the copy", s.lastPlan.desired[x.key]?.ready === "/healthz");
  const target = f.alive().find((y) => y.machine !== x.machine).machine;
  f.readyFn = (srv, st) => srv.machine !== target; // never ready on the target
  c.move("db", x.machine, String(target), 1);
  await f.run(8 * MIN);
  const moving = c.copiesOf("db").find((y) => y.leaving);
  ok("a move to a copy that answers but isn't ready doesn't finish", Boolean(moving), placed(c, "db").join(" "));
  f.readyFn = null;
  await f.run(3 * MIN);
  ok("once it's ready, the move finishes", !c.copiesOf("db").some((y) => y.leaving) && c.copiesOf("db")[0].machine === target, placed(c, "db").join(" "));
}

{ // 5. doubling waits for a server that's starting
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 2, cap: 20 } }, { drainAt: () => null, build: 60_000 });
  await f.startPool("a");
  await f.run(4 * MIN);
  c.putSettings({ pools: { a: 3 } }); // a third server is asked for; it takes a few minutes to arrive
  f.pools.a.size = 3;
  const realStart = f.start.bind(f);
  const pending = [];
  f.start = async (pool, want) => { pending.push([pool, want]); return null; }; // the start takes a while
  await f.run(1 * MIN);
  deploy(c, "web", app("web", 3));
  await f.run(2 * MIN);
  const doubledEarly = f.alive().some((s) => c.copiesOn("web", s.machine).length > 1);
  ok("while a server is starting, the third copy waits instead of doubling up", !doubledEarly && c.blocked.get("web")?.includes("starting"), c.blocked.get("web"));
  f.start = realStart;
  for (const [pool, want] of pending) await f.start(pool, want);
  await f.run(5 * MIN);
  ok("then it goes to the new server", c.copiesOf("web").length === 3 && !f.alive().some((s) => c.copiesOn("web", s.machine).length > 1), placed(c, "web").join(" "));
}

{ // 6. eviction respects the budget unless forced
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 20 }, b: { size: 3, cap: 20 } }, { drainAt: () => null, build: 60_000 });
  await f.startPool("a");
  await f.startPool("b");
  await f.run(4 * MIN);
  deploy(c, "kv", app("kv", 3, "", 7000));
  await f.run(20 * MIN);
  const [x1, x2] = c.copiesOf("kv");
  c.move("kv", x1.machine, null, x1.replica); // one copy is changing now
  const r = c.evict(x2.machine);
  ok("evicting another server of the app while one copy is changing leaves its copy", r.failed.length === 1 && r.moved.length === 0, JSON.stringify(r));
  const forced = c.evict(x2.machine, { force: true });
  ok("forcing it moves it", forced.moved.length === 1, JSON.stringify(forced));
}

{ // 7. a server near its deadline gets no new copies while another can take them
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 3, cap: 20 } }, { drainAt: () => null, reportDeadline: true });
  await f.startPool("a");
  await f.run(4 * MIN);
  const s0 = f.alive()[0];
  s0.deadline = T + 28 * MIN; // this server will be stopped in 28 minutes (not yet urgent: that's under 25)
  await f.run(1 * MIN);
  deploy(c, "web", app("web", 2));
  await f.run(1 * MIN);
  ok("the server with 27 minutes left got no copy", !c.copiesOn("web", s0.machine).length && c.copiesOf("web").length === 2, placed(c, "web").join(" "));
}

{ // 8. stateless copies spread across pools as well as servers
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 4, cap: 20 }, b: { size: 4, cap: 20 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.startPool("b");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 4));
  await f.run(3 * MIN);
  const by = poolsOf(c, f, "web");
  ok("4 copies over two pools: two in each", by.a === 2 && by.b === 2, JSON.stringify(by));
}
summary();
