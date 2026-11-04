import assert from "node:assert/strict";
import { makeControl, Fleet, deploy, advance, MIN, T } from "./lib.mjs";

const app = (replicas = 1) => ({ port: 8080, replicas, compose: 'services:\n  web:\n    image: test\n    ports: ["8080:8080"]\n' });

// Reproduce recovery after a long control outage: the first replacement joins
// after the controller's startup grace has already elapsed, ahead of its peers.
{
  const c = makeControl();
  const f = new Fleet(c, { provider: { size: 10 } }, { build: 2 * MIN, drainAt: () => null });
  advance(10 * MIN);
  for (const name of ["browser", "proxy", "shell"]) deploy(c, name, app(25));
  const first = await f.start("provider");
  await f.tick();
  assert.equal(c.placementCounts().get(first.machine), 3, "first host reserves only one build batch");
  await f.run(MIN);
  assert.equal(c.placementCounts().get(first.machine), 3, "unreported/building copies reserve capacity across reports");
  for (let i = 1; i < 10; i++) await f.start("provider");
  await f.tick();
  for (const n of c.startingCounts(c.liveMachines(T)).values()) assert.ok(n <= 3, "incoming hosts also get bounded batches");
  await f.run(15 * MIN);
  assert.equal([...c.placementCounts().values()].reduce((a, b) => a + b, 0), 75, "healthy reports release capacity for all replicas");
  assert.equal(f.alive().filter((s) => Object.keys(s.status).length).length, 10, "recovery uses the rest of the fleet");
  assert.ok(Math.max(...c.placementCounts().values()) < 15, "no first-host pileup remains");
  for (const [name, list] of c.copies) for (const copy of list) {
    assert.ok(c.copyHealthy(name, copy, c.liveMachines(T).get(copy.machine)));
  }
}

// A broken deployment may finish in failure. It cannot reserve all deployment
// slots forever and prevent an unrelated application from starting.
{
  const c = makeControl();
  const f = new Fleet(c, { provider: { size: 1 } }, { drainAt: () => null });
  await f.startPool("provider");
  await f.run(4 * MIN);
  f.fail.add("broken");
  deploy(c, "broken", app(3));
  await f.run(4 * MIN);
  deploy(c, "working", app());
  await f.run(4 * MIN);
  const [copy] = c.copiesOf("working");
  assert.ok(copy && c.copyHealthy("working", copy, c.liveMachines(T).get(copy.machine)), "completed failures cannot starve other apps");
}
console.log("Startup capacity recovery and failed-build starvation scenarios passed.");
