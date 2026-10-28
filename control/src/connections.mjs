import { isIP } from "node:net";

export const VISITOR_KEEP_MS = 3600_000;
const WINDOWS = { "1m": 60_000, "5m": 300_000, "15m": 900_000, "1h": VISITOR_KEEP_MS };

// Deliberately outside SQLite, status, export, receipts and log streams. Only
// the administrator can read these bounded, expiring IP observations.
export class VisitorHistory {
  constructor({ limit = 20_000 } = {}) {
    this.limit = limit;
    this.rows = new Map();
    this.sources = new Map();
    this.dropped = 0;
    this.prunedAt = 0;
  }
  take(run, payload, now, allowed) {
    if (payload?.v !== 1 || !Array.isArray(payload.rows)) return;
    if (now - this.prunedAt >= 30_000) this.prune(now);
    if (!this.sources.has(run.id) && this.sources.size >= 2000) this.sources.delete(this.sources.keys().next().value);
    this.sources.set(run.id, { machine: run.machine, seen: now, dropped: Math.max(0, Math.min(1e9, Number(payload.dropped) || 0)) });
    for (const row of payload.rows.slice(0, 512)) {
      if (!row || typeof row.ip !== "string" || row.ip.length > 45 || !isIP(row.ip) || !allowed(row.app, row.replica)) continue;
      const last = Number(row.last), first = Number(row.first);
      if (!Number.isFinite(first) || !Number.isFinite(last) || first > last || last > now + 5000 || last < now - VISITOR_KEEP_MS) continue;
      const key = `${run.id}|${row.app}|${row.replica}|${row.ip}`, previous = this.rows.get(key);
      if (previous && previous.last >= last) continue; // retries cannot double-count or move time backwards
      if (!previous && this.rows.size >= this.limit) { this.rows.delete(this.rows.keys().next().value); this.dropped++; }
      this.rows.delete(key);
      this.rows.set(key, { ip: row.ip, app: row.app, replica: row.replica, machine: run.machine,
        first: Math.min(now, previous?.first ?? first, first), last: Math.min(now, last) });
    }
  }
  prune(now) {
    for (const [key, row] of this.rows) if (now - row.last > VISITOR_KEEP_MS) this.rows.delete(key);
    for (const [key, source] of this.sources) if (now - source.seen > VISITOR_KEEP_MS) this.sources.delete(key);
    this.prunedAt = now;
  }
  query(params, now, runs, projects, liveMs) {
    const window = params.get("window") || "1m", duration = WINDOWS[window];
    const app = params.get("app") || "", replica = params.get("replica"), search = (params.get("q") || "").slice(0, 64).toLowerCase();
    if (typeof duration !== "number" || (replica != null && (!/^\d+$/.test(replica) || +replica < 1 || +replica > 50)))
      throw Object.assign(new Error("choose a 1m, 5m, 15m or 1h window and a valid replica"), { status: 400 });
    this.prune(now);
    const combined = new Map();
    for (const row of this.rows.values()) {
      if (row.last < now - duration || !projects.has(row.app) || (app && row.app !== app) || (replica != null && row.replica !== +replica) || (search && !row.ip.toLowerCase().includes(search))) continue;
      const key = `${row.app}|${row.replica}|${row.ip}`, previous = combined.get(key);
      if (!previous) combined.set(key, { ...row });
      else {
        previous.first = Math.min(previous.first, row.first);
        if (row.last > previous.last) Object.assign(previous, { last: row.last, machine: row.machine });
      }
    }
    const rows = [...combined.values()].sort((a, b) => b.last - a.last || a.ip.localeCompare(b.ip));
    const current = runs.filter((run) => !run.retire && now - run.seen <= liveMs);
    const reporting = current.filter((run) => now - (this.sources.get(run.id)?.seen ?? 0) <= liveMs);
    return { now, window, keepMs: VISITOR_KEEP_MS, refreshMs: 10_000, total: rows.length,
      uniqueIPs: new Set(rows.map((row) => row.ip)).size, liveIPs: new Set(rows.filter((row) => now - row.last <= 60_000).map((row) => row.ip)).size,
      rows: rows.slice(0, 500), truncated: rows.length > 500,
      collectors: { reporting: reporting.length, online: current.length,
        lastReport: Math.max(0, ...reporting.map((run) => this.sources.get(run.id).seen)),
        dropped: this.dropped + [...this.sources.values()].reduce((n, source) => n + source.dropped, 0) } };
  }
}
