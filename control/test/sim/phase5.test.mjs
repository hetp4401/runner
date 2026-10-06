// Phase 5: what the pages show: replicas going dark and coming back (cold departures), alerts and the alert webhook,
// warm handovers with their times, churn by cause, and departures coming up with why a due one waits.
import http from "node:http";
import { makeControl, Fleet, deploy, ok, summary, placed, MIN, T } from "./lib.mjs";

const hooks = [];
const hook = http.createServer((req, res) => {
  let b = "";
  req.on("data", (c) => (b += c));
  req.on("end", () => { hooks.push(JSON.parse(b)); res.end("ok"); });
});
await new Promise((r) => hook.listen(0, "127.0.0.1", r));
const url = `http://127.0.0.1:${hook.address().port}/hook`;
const app = (name, replicas, port = 8080, image = name) => ({ port, replicas, compose: `x-runner:\n  port: ${port}\n  replicas: ${replicas}\nservices:\n  app:\n    image: ${image}\n    ports: ["${port}:${port}"]\n` });
const events = (c, kind, f = {}) => c.all("SELECT * FROM events WHERE kind = ? ORDER BY id", kind).filter((e) => Object.entries(f).every(([k, v]) => e[k] === v));
const onMachine = (c, name, k) => c.copiesOf(name).find((x) => x.replica === k && !x.leaving)?.machine;

{ // 1. a single-copy app whose server stops (silently, and its pool can't start another) goes dark until it's placed
  //    elsewhere and healthy; the loss counts against its pool; 5 minutes dark raises an alert, posted to the webhook
  const c = makeControl({ ALERT_WEBHOOK: url });
  const f = new Fleet(c, { a: { size: 4, cap: 4 } }, { drainAt: () => null });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "solo", app("solo", 1));
  await f.run(12 * MIN);
  const m = onMachine(c, "solo", 1);
  const s = f.alive().find((y) => y.machine === m);
  f.pools.a.cap = 3; // its account is at its limit: no replacement
  f.stop(s, "test: silent");
  await f.run(20 * MIN);
  const dark = events(c, "dark", { app: "solo" });
  const lit = events(c, "lit", { app: "solo" });
  ok("it went dark, blamed on the stopped server's run", dark.length === 1 && dark[0].run === s.run, dark.map((e) => `${e.cause}: ${e.detail}`).join(" | "));
  ok("and lit again once placed elsewhere, with how long it was dark", lit.length === 1 && lit[0].other >= 300 && onMachine(c, "solo", 1) !== m, lit.map((e) => `${e.other} s: ${e.detail}`).join(" | "));
  const st = c.status();
  ok("the day counts a lost server", st.departures.day.lost === 1, JSON.stringify(st.departures.day));
  ok("a day's dark time is on the status", st.cold.spells === 1 && st.cold.seconds >= 300, JSON.stringify(st.cold));
  ok("5 minutes dark raised an alert", events(c, "alert", { cause: "dark", app: "solo" }).length === 1);
  await new Promise((r) => setTimeout(r, 300));
  ok("posted to the webhook", hooks.some((h) => h.cause === "dark" && h.text.includes("solo")), hooks.map((h) => h.text).join(" | "));
}

{ // 2. a drained server hands over with nothing going dark (a warm departure, with its handover times); its successor's
  //    copies count as churn; a new version's restarts too; a second drained server in the same pool waits, and says why
  const c = makeControl();
  const f = new Fleet(c, { a: { size: 4, cap: 20 } }, { drainAt: () => null, reportDeadline: true });
  await f.startPool("a");
  await f.run(4 * MIN);
  deploy(c, "web", app("web", 3));
  await f.run(12 * MIN);
  const [s1, s2] = f.alive().filter((y) => c.copiesOn("web", y.machine).length);
  c.drain({ agent: s1.agent });
  c.drain({ agent: s2.agent });
  await f.run(40_000);
  const list = c.status().departures.list;
  const second = list.find((d) => d.machine === s2.machine);
  ok("the first is handing over", list.find((d) => d.machine === s1.machine)?.leaving === "handover", JSON.stringify(list.map((d) => [d.machine, d.leaving, d.wait])));
  ok("the second waits, and says why", second?.due && !second.leaving && /same group is leaving/.test(second.wait ?? ""), second?.wait);
  await f.run(25 * MIN);
  const st = c.status();
  const a = st.departures.day;
  ok("both left warm: nothing went dark", a?.planned === 2 && a.plannedCold === 0 && events(c, "dark").length === 0, JSON.stringify(a));
  ok("handover times recorded", a?.handover?.n === 2 && a.handover.build > 0, JSON.stringify(a?.handover));
  ok("the successors' rebuilds count as churn", st.churn.h1.successor === 2, JSON.stringify(st.churn.h1));
  deploy(c, "web", app("web", 3, 8080, "web2"));
  await f.run(5 * MIN);
  ok("a new version's restarts count as churn", c.status().churn.h1.version === 3, JSON.stringify(c.status().churn.h1));
  ok("every run shows its deadline and when it's due", c.status().runs.filter((r) => r.live).every((r) => r.deadline && r.dueAt));
}

hook.close();
summary();
