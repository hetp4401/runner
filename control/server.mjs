// The control plane as one process: SQLite in DATA_DIR, HTTP on HOST:PORT (a Cloudflare tunnel in front gives it
// runners.<domain>). Settings and secrets come from the environment (see the README).
//   PORT (8920), HOST (127.0.0.1), DATA_DIR (.), DOMAIN, CONTROL_HOST, ZONE, ACCOUNT_ID, POOLS, MAX_SLOTS (optional cap on
//   machines), FLEET_PASSWORD, JOIN_TOKEN, CF_API_TOKEN (or CF_API_KEY + CF_API_EMAIL), CF_API_BASE (tests), POLL_S,
//   DNS=off (no DNS changes), ALERT_WEBHOOK, QUIET_MS and YOUNG_MS (tests)
// As one of several copies (see src/replica.mjs): STORE_URLS (zkmetadata replicas, comma-separated), STATE_KEY (seals
// the snapshots), TUNNEL_CMD (the tunnel connector, run while this copy leads), SELF_URL (where this copy answers),
// LEASE_MS (30000), SNAP_S (60), TAKEOVER_DELAY_S (0: how long the lease must be free before this copy takes it).
import http from "node:http";
import { randomUUID } from "node:crypto";
import { mkdirSync, readFileSync } from "node:fs";
import { hostname } from "node:os";
import { DatabaseSync } from "node:sqlite";
import { WebSocketServer } from "ws";
import { Control } from "./src/index.js";
import { Lease, LostLease, Snapshots, Store, Tunnel, importState, sealKey } from "./src/replica.mjs";

const env = process.env;
const port = Number(env.PORT) || 8920;
const host = env.HOST || "127.0.0.1";
const replicated = Boolean(env.STORE_URLS);
const dataDir = env.DATA_DIR || ".";
const MAX_BODY = 1_000_000; // bytes; a spec is at most 250 KB, plus JSON quoting
const log = (msg) => console.log(`${new Date().toISOString()} ${msg}`);

mkdirSync(dataDir, { recursive: true });
const db = new DatabaseSync(`${dataDir}/control.db`);
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
// The same shape the code was written against: exec(query, ...args) runs the statement and gives { toArray() }.
const sql = {
  exec(query, ...args) {
    const rows = db.prepare(query).all(...args);
    return { toArray: () => rows };
  },
};

let timer = null;
const page = readFileSync(new URL("./src/app.html", import.meta.url), "utf8");
let control = null; // built at once on its own; as one of several copies, only by the leader
function build() {
  control = new Control({
    sql,
    env,
    page,
    alarm(at) {
      clearTimeout(timer);
      timer = setTimeout(() => control.alarm().catch((e) => log(`alarm failed: ${e.message}`)), Math.max(0, at - Date.now()));
    },
    restart() {
      log("restarting on request");
      shutdown(0);
    },
  });
}
if (!replicated) build();

const server = http.createServer(async (req, res) => {
  if (!control) return standby(req, res);
  if (replicated && req.method !== "GET" && !/^\/(admin\/)?api\/(sync|wake|logs)/.test(req.url)) snapSoon(); // (a change worth keeping)
  try {
    const chunks = [];
    let size = 0;
    for await (const c of req) {
      size += c.length;
      if (size > MAX_BODY) {
        res.writeHead(413, { "content-type": "application/json" });
        return res.end(JSON.stringify({ error: "the request is too big" }));
      }
      chunks.push(c);
    }
    // Behind the tunnel the request was https at the edge (cloudflared says so); the Host header is the public name.
    const host = req.headers.host || env.CONTROL_HOST || `127.0.0.1:${port}`;
    const proto = req.headers["x-forwarded-proto"] === "https" ? "https" : "http";
    const headers = {};
    for (const [k, v] of Object.entries(req.headers)) if (v != null) headers[k] = Array.isArray(v) ? v.join(", ") : v;
    const request = new Request(`${proto}://${host}${req.url}`, {
      method: req.method,
      headers,
      body: req.method === "GET" || req.method === "HEAD" ? undefined : Buffer.concat(chunks),
    });
    const response = await control.fetch(request);
    const body = Buffer.from(await response.arrayBuffer());
    res.writeHead(response.status, { ...Object.fromEntries(response.headers), "content-length": body.length, server: "runner-control" });
    res.end(body);
  } catch (e) {
    log(`request failed: ${e.stack ?? e}`);
    if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
    res.end(JSON.stringify({ error: e.message }));
  }
});
server.keepAliveTimeout = 65_000;

// WebSockets, for live logs: the web app's viewer (/admin/api/logs/<session>, from its own site) and the agent that
// streams them (/api/logs/stream?session=...&run=..., with the join token). The control plane passes lines from one
// to the other and keeps nothing (see openLogSession).
const wss = new WebSocketServer({ noServer: true, maxPayload: 1 << 20 });
server.on("upgrade", (req, socket, head) => {
  const refuse = (status) => {
    socket.end(`HTTP/1.1 ${status} ${http.STATUS_CODES[status]}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`);
  };
  try {
    const url = new URL(req.url, "http://localhost");
    const viewer = url.pathname.match(/^\/admin\/api\/logs\/([0-9a-f-]{36})$/);
    if (!control) return refuse(503);
    if (viewer) {
      const origin = req.headers.origin;
      if (origin && new URL(origin).host !== req.headers.host) return refuse(403); // another site's page
      return wss.handleUpgrade(req, socket, head, (ws) => control.attachLogViewer(viewer[1], adapt(ws)));
    }
    if (url.pathname === "/api/logs/stream") {
      if (!control.isJoinToken(req.headers.authorization)) return refuse(401);
      return wss.handleUpgrade(req, socket, head, (ws) => control.attachLogAgent(url.searchParams.get("session"), url.searchParams.get("run"), adapt(ws)));
    }
    refuse(404);
  } catch (e) {
    log(`upgrade failed: ${e.message}`);
    refuse(400);
  }
});
// What the control plane sees of a socket. Pinged every 25 s, so a quiet one stays open through the tunnel; one that
// falls more than 2 MB behind drops what doesn't fit.
function adapt(ws) {
  const ping = setInterval(() => ws.readyState === ws.OPEN && ws.ping(), 25_000);
  ws.on("close", () => clearInterval(ping));
  ws.on("error", () => {});
  return {
    send(text) {
      if (ws.readyState === ws.OPEN && ws.bufferedAmount < 2_000_000) ws.send(text);
    },
    close(code = 1000, reason = "") {
      try { ws.close(code, reason); } catch { ws.terminate(); }
    },
    onMessage: (fn) => ws.on("message", (data, binary) => fn(binary ? data : data.toString())),
    onClose: (fn) => ws.on("close", () => fn()),
  };
}
server.listen(port, host, () => log(`control plane listening on ${host}:${port}, data in ${dataDir}${replicated ? " (one of several copies: on standby until it holds the lease)" : ""}`));

let stopping = false;

// ---- one of several copies ----
const me = { id: `${env.FLEET_APP ? `${env.FLEET_APP}-${env.FLEET_REPLICA}` : hostname()}-${randomUUID().slice(0, 8)}`, url: env.SELF_URL || null };
const store = replicated ? new Store(env.STORE_URLS.split(",").map((u) => u.trim()).filter(Boolean), env.ADMIN_PASSWORD) : null;
const lease = replicated ? new Lease(store, me, Number(env.LEASE_MS) || 30_000, (Number(env.TAKEOVER_DELAY_S) || 0) * 1000) : null;
const snaps = replicated ? new Snapshots(store, sealKey(env.STATE_KEY || ""), me) : null;
const tunnel = new Tunnel(env.TUNNEL_CMD, log);
const SNAP_MS = (Number(env.SNAP_S) || 60) * 1000;
let leading = false;
let holder = null; // the lease as a copy on standby last saw it
let snapDue = Infinity;
let saving = null;
let soon = null;
let lastSnap = null;

// What a copy on standby says: who leads (any other request gets 503; the tunnel sends requests to the leader).
function standby(req, res) {
  const ok = req.method === "GET" && ["/", "/healthz", "/api/replica"].includes(req.url.split("?")[0]);
  res.writeHead(ok ? 200 : 503, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(ok
    ? { standby: true, me: me.id, leader: holder && holder.until > Date.now() ? { id: holder.holder, url: holder.url, until: holder.until } : null }
    : { error: `this copy of the control plane is on standby${holder?.url ? `; the leader answers at ${holder.url}` : ""}` }));
}

async function tick() {
  if (stopping) return;
  try {
    if (!leading) {
      const { taken, cur } = await lease.take();
      holder = cur;
      if (taken) await lead(cur);
    } else {
      await lease.renew();
      if (Date.now() >= snapDue) await snapshot("every minute");
    }
  } catch (e) {
    if (e instanceof LostLease) return stepDown(e.message);
    log(`replica: ${e.message}`); // the store didn't answer: carry on as before (no other copy can take the lease either)
  }
}

// Took the lease: the state from the latest snapshot (or, with none yet, this copy's own: the first copy), then serve.
async function lead(cur) {
  log(`took the lease${cur?.holder && cur.holder !== me.id ? ` from ${cur.holder}` : ""}: restoring the state`);
  const snap = await snaps.load();
  new Control({ sql, env, page, alarm() {}, restart() {} }); // (makes or brings up to date every table, then is dropped)
  if (snap) {
    importState(db, snap.tables);
    log(`restored the snapshot ${snap.manifest.gen} (${snap.manifest.by}, ${Math.round((Date.now() - snap.manifest.t) / 1000)} s old)`);
  } else {
    const projects = db.prepare("SELECT COUNT(*) AS n FROM projects").get().n;
    log(projects ? `no snapshot yet: starting from this copy's own data (${projects} apps)` : "no snapshot yet: starting empty");
  }
  build();
  leading = true;
  await snapshot("taking over");
  tunnel.start();
  log("leading");
}

async function snapshot(why) {
  if (!leading) return;
  if (saving) return saving;
  saving = (async () => {
    try {
      const m = await snaps.save(db);
      lastSnap = { ...m, why };
      snapDue = Date.now() + SNAP_MS;
    } catch (e) {
      if (e instanceof LostLease) return stepDown(e.message);
      log(`snapshot (${why}) failed: ${e.message}`);
      snapDue = Date.now() + 10_000;
    } finally {
      saving = null;
    }
  })();
  return saving;
}

function snapSoon() {
  if (!leading || soon) return;
  soon = setTimeout(() => {
    soon = null;
    snapshot("a change");
  }, 2_000);
}

// Another copy leads now: this one stops at once (and starts again on standby, with a clean slate).
function stepDown(why) {
  if (stopping) return;
  log(`no longer the leader: ${why}; starting again on standby`);
  leading = false;
  tunnel.stop();
  stopping = true;
  setTimeout(() => process.exit(1), 200);
}

if (replicated) {
  setInterval(tick, 10_000);
  tick();
}

async function shutdown(code) {
  if (stopping) return;
  stopping = true;
  setTimeout(() => process.exit(code), 30_000).unref();
  if (replicated && leading) { // hand over cleanly: the state as it is now, then the lease, at once
    leading = false;
    clearTimeout(soon);
    try {
      await snaps.save(db);
      await lease.release();
      log("saved the state and handed the lease on");
    } catch (e) {
      log(`handing over: ${e.message}`);
    }
    tunnel.stop();
  }
  clearTimeout(timer);
  for (const ws of wss.clients) ws.terminate();
  server.close(() => {
    db.close();
    process.exit(code);
  });
  setTimeout(() => process.exit(code), 5_000).unref();
}
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => shutdown(0));
