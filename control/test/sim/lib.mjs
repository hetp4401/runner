// A simulated fleet for the control plane: the real Control class in-process (SQLite in memory, a fake Cloudflare),
// a fake clock, and fake servers that behave like the agent: they join, check in every POLL, build what they're told
// (taking BUILD), report it healthy (or failed, for apps told to fail), open their tunnel when settled, start the
// servers the control plane asks for (within their pool's job cap, like GitHub's), hand over, and leave at their
// lifetime's end. Scenarios drive it and check invariants every tick.
import { DatabaseSync } from "node:sqlite";

export let T = Date.parse("2026-10-06T00:00:00Z");
Date.now = () => T;
export const MIN = 60_000;
const { Control } = await import("../../src/index.js");

let tid = 0;
class SimControl extends Control {
  async cf(path, init = {}) {
    if (path.includes("/cfd_tunnel?name=")) return { success: true, result: [] };
    if (path.endsWith("/token")) return { success: true, result: `token-${path}` };
    if (path.includes("/cfd_tunnel") && init.method === "POST") return { success: true, result: { id: `tunnel-${++tid}` } };
    return { success: true, result: [], result_info: { total_pages: 1 } };
  }
}

export function makeControl(env = {}, db = new DatabaseSync(":memory:")) {
  const sql = { exec(q, ...a) { const rows = db.prepare(q).all(...a); return { toArray: () => rows }; } };
  const control = new SimControl({
    sql, page: "", alarm() {}, restart() {},
    env: { DOMAIN: "sim.example", JOIN_TOKEN: "node", ADMIN_PASSWORD: "adm", DNS: "off", POLL_S: "20", ...env },
  });
  control.db = db;
  return control;
}

let agentSeq = 0;
export class Fleet {
  // pools: { name: { size, cap } }; opts: build (ms), lifetime (ms), drainAt (run -> ms after start, or null)
  constructor(control, pools, opts = {}) {
    this.c = control;
    this.pools = pools;
    this.build = opts.build ?? 90_000;
    this.lifetime = opts.lifetime ?? 355 * MIN;
    this.drainAt = opts.drainAt ?? ((s) => (180 + Math.floor(rand() * 170)) * MIN); // today's machine.yml timer
    this.reportDeadline = opts.reportDeadline ?? false;
    this.servers = []; // every server ever started
    this.queue = []; // starts waiting for a job slot: { pool, want }
    this.fail = new Set(); // "app" or "app@machine": copies of these fail to build
    this.silent = new Set(); // machines (slots) whose check-ins don't arrive (a partition)
    this.cpu = new Map(); // machine -> cpu % reported (its base, with cpuFromApps)
    this.appCpu = new Map(); // "app" -> % of a core per copy
    this.mem = new Map(); // machine -> memory used, % (12.5 if not set)
    this.steal = new Map(); // machine -> cpu steal %
    this.disk = new Map(); // machine -> share of its disk free (0.5 if not set)
    this.crash = new Set(); // "app@machine" or "app@run": a healthy copy there fails, and comes back once removed
    this.restartLoop = new Set(); // "app@machine" or "app@run": a copy there restarts at every check-in
    this.cpuFromApps = opts.cpuFromApps ?? false; // a server's CPU is its base plus its healthy copies' (4 cores)
    this.log = [];
    for (const [name, p] of Object.entries(pools)) control.putSettings({ pools: { [name]: p.size } });
  }
  alive(pool = null) { return this.servers.filter((s) => s.alive && (!pool || s.pool === pool)); }
  jobs(pool) { return this.alive(pool).length; }
  async start(pool, want) {
    if (this.jobs(pool) >= (this.pools[pool].cap ?? Infinity)) { this.queue.push({ pool, want }); return null; }
    const agent = `${pool}-${++agentSeq}`;
    const ans = await this.c.join({ agent, pool, want });
    const s = { agent, pool, machine: ans.machine, started: T, run: `${agent}-${T}`, status: {}, ready: false, alive: true,
      deadline: T + this.lifetime, drainAt: null, handoverAt: 0 };
    s.drainAt = this.drainAt(s) == null ? null : T + this.drainAt(s);
    this.servers.push(s);
    return s;
  }
  async startPool(pool, n = this.pools[pool].size) { for (let i = 0; i < n; i++) await this.start(pool); }
  hostCpu(s) {
    let cpu = this.cpu.get(s.machine) ?? 10;
    if (this.cpuFromApps) for (const st of Object.values(s.status)) if (st.s === "healthy") cpu += (this.appCpu.get(st.app) ?? 5) / 4;
    return Math.min(100, cpu);
  }
  body(s, extra = {}) {
    const a = {};
    for (const [key, st] of Object.entries(s.status)) {
      const app = st.app;
      a[app] = { cpu: (a[app]?.cpu ?? 0) + (this.appCpu.get(app) ?? 5), mem: (a[app]?.mem ?? 0) + 100e6 };
    }
    const h = { cpu: this.hostCpu(s), steal: this.steal.get(s.machine) ?? 0, memUsed: ((this.mem.get(s.machine) ?? 12.5) / 100) * 16e9, memTotal: 16e9 };
    // The minutes finished since its last check-in, each summarised with the figures of now.
    const minute = T - (T % MIN);
    s.minuteT ??= minute;
    const minutes = [];
    for (let t = s.minuteT; t < minute; t += MIN) minutes.push({ t, h: { ...h, cpuMax: h.cpu }, a });
    s.minuteT = minute;
    const status = Object.fromEntries(Object.entries(s.status).map(([k, st]) => [k, { v: st.v, s: st.s, ...(st.r !== undefined ? { r: st.r } : {}), ...(st.e ? { e: st.e } : {}), t0: st.t0, n: st.n ?? 0 }]));
    return {
      machine: s.machine, run: s.run, agent: s.agent, pool: s.pool, poolSize: this.pools[s.pool].size, starts: true, started: s.started,
      ready: s.ready, status, ...(this.reportDeadline ? { deadline: s.deadline } : {}), cpus: 4,
      disk: { free: (this.disk.get(s.machine) ?? 0.5) * 100e9, total: 100e9 },
      metrics: { live: { t: T, h, a }, minutes }, ...extra,
    };
  }
  async sync(s, extra = {}) {
    const plan = this.c.sync(this.body(s, extra));
    s.lastPlan = plan;
    return plan;
  }
  apply(s, desired) {
    for (const key of Object.keys(s.status)) if (!(key in desired)) delete s.status[key];
    for (const [key, d] of Object.entries(desired)) {
      const cur = s.status[key];
      if (cur && cur.v === d.v && cur.replica === d.replica && cur.app === d.app) continue;
      s.status[key] = { v: d.v, s: "applying", t0: T, doneAt: T + this.build, app: d.app, replica: d.replica, ready: d.ready ?? null };
    }
    for (const st of Object.values(s.status)) {
      if (st.s === "applying" && T >= st.doneAt) {
        const fails = this.fail.has(st.app) || this.fail.has(`${st.app}@${s.machine}`);
        st.s = fails ? "failed" : "healthy";
        if (fails) st.e = "simulated failure";
      }
      const crash = this.crash.has(`${st.app}@${s.machine}`) || this.crash.has(`${st.app}@${s.run}`);
      if (st.s === "healthy" && crash) Object.assign(st, { s: "failed", e: "simulated crash", crashed: true });
      else if (st.crashed && !crash) Object.assign(st, { s: "healthy", e: undefined, crashed: false });
      if (this.restartLoop.has(`${st.app}@${s.machine}`) || this.restartLoop.has(`${st.app}@${s.run}`)) st.n = (st.n ?? 0) + 1;
      if (st.ready) st.r = st.s === "healthy" && (this.readyFn ? this.readyFn(s, st) : true);
    }
  }
  settled(s, desired) { return Object.entries(desired).every(([k]) => s.status[k]?.s === "healthy"); }
  // One check-in round: every live server checks in (unless partitioned), acts on its plan; then time moves on.
  async tick(dt = 20_000) {
    for (const s of [...this.alive()]) {
      if (this.silent.has(s.machine)) continue;
      if (T >= s.deadline) { // the job's hard stop: one last check-in saying it's leaving
        await this.sync(s, { leaving: true });
        this.stop(s, "hard stop");
        continue;
      }
      if (s.drainAt && T >= s.drainAt && !s.drained) {
        s.drained = true;
        try { this.c.drain({ agent: s.agent }); } catch {}
      }
      const plan = await this.sync(s);
      if (plan.retire) { this.stop(s, "retired"); continue; }
      this.apply(s, plan.desired);
      if (!s.ready) {
        const serve = plan.successor ? (plan.serve ?? []) : null;
        const ok = serve ? serve.every((k) => s.status[k]?.s === "healthy" && s.status[k]?.r !== false)
          : this.settled(s, plan.desired) || T > s.started + 10 * MIN;
        if (ok) s.ready = true;
      }
      for (const m of plan.start ?? []) await this.start(s.pool, m);
      if (plan.handover && T - s.handoverAt > 10 * MIN) {
        s.handoverAt = T;
        await this.start(s.pool, s.machine);
      }
    }
    // Queued starts run when their pool has a free job slot.
    for (const q of [...this.queue]) {
      if (this.jobs(q.pool) < (this.pools[q.pool].cap ?? Infinity)) {
        this.queue.splice(this.queue.indexOf(q), 1);
        await this.start(q.pool, q.want);
      }
    }
    T += dt;
  }
  stop(s, why) {
    s.alive = false;
    s.stoppedAt = T;
    this.log.push(`${new Date(T).toISOString().slice(11, 16)} stop m${s.machine} ${s.pool} (${why})`);
  }
  async run(ms, each = null) { const end = T + ms; while (T < end) { await this.tick(); if (each) each(); } }
}

export function deploy(control, name, body) {
  return control.putProject(name, body, { trusted: true });
}

// Deterministic random numbers, so a scenario plays out the same every time.
let seed = 42;
export function rand() { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; }
export function reseed(n) { seed = n; }

export const results = [];
export function ok(name, cond, detail = "") {
  const line = `${cond ? "  ok   " : "  WRONG "}${name}${detail !== "" ? ` (${detail})` : ""}`;
  results.push(line);
  console.log(line);
}
export function summary() {
  const wrong = results.filter((l) => l.includes("WRONG")).length;
  console.log(wrong ? `${wrong} WRONG` : "all ok");
  process.exitCode = wrong ? 1 : 0;
}

// Where each replica of an app runs: "k@m" for staying copies.
export function placed(control, app) {
  return control.copiesOf(app).filter((x) => !x.leaving).map((x) => `${x.replica}@${x.machine}`).sort();
}
export function advance(ms) { T += ms; }
