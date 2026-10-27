// The control plane as one process: configuration in SQLite, runtime tables in memory, HTTP on 127.0.0.1:PORT (a Cloudflare tunnel in front gives it
// runners.<domain>). Settings and secrets come from the environment (see the README).
//   PORT (8920), DATA_DIR (.), DOMAIN, CONTROL_HOST, ZONE, ACCOUNT_ID, POOLS, MAX_SLOTS (optional cap on machines), FLEET_PASSWORD, JOIN_TOKEN,
//   CF_API_TOKEN (or CF_API_KEY + CF_API_EMAIL), CF_API_BASE (tests), POLL_S, DNS=off (no DNS changes), ALERT_WEBHOOK,
//   STATE_SIGNING_KEY (required), RECOVERY_MS (180000), MEMORY_HISTORY_MIN (360), INIT_CONTROL_DB=1 (new fleet only),
//   QUIET_MS and YOUNG_MS (tests)
import http from "node:http";
import { mkdirSync, readFileSync } from "node:fs";
import { WebSocketServer } from "ws";
import { fileURLToPath } from "node:url";
import { Control } from "./src/index.js";
import { LocalStore } from "./src/local-store.mjs";
import { loadAgentBundle } from "./src/agent-bundle.mjs";
import { clientAddress, validateProxyConfig } from "./src/client-ip.mjs";

const env = { ...process.env, RECOVER_RUNTIME: "1", RECOVER_TUNNELS: "1", MEMORY_HISTORY_MIN: process.env.MEMORY_HISTORY_MIN || "360" };
const port = Number(env.PORT) || 8920;
const host = env.BIND_HOST || "127.0.0.1";
const dataDir = env.DATA_DIR || ".";
const MAX_BODY = 1_000_000; // bytes; a spec is at most 250 KB, plus JSON quoting
const log = (msg) => console.log(`${new Date().toISOString()} ${msg}`);
validateProxyConfig(env);

if (!env.STATE_SIGNING_KEY || !env.JOIN_TOKEN) throw new Error("STATE_SIGNING_KEY and JOIN_TOKEN are required for agent recovery");
process.umask(0o077);
mkdirSync(dataDir, { recursive: true, mode: 0o700 });
const sql = new LocalStore(`${dataDir}/control.db`, { initialize: env.INIT_CONTROL_DB === "1", onFailure() {
  log("configuration storage failed; restarting before issuing more plans");
  setImmediate(() => shutdown(1));
} });
if (sql.backupPath) log(`configuration-only storage migration complete; previous database backed up at ${sql.backupPath}`);

let timer = null;
const bundle = loadAgentBundle(fileURLToPath(new URL("..", import.meta.url)));
const control = new Control({
  sql,
  env,
  page: readFileSync(new URL("./src/app.html", import.meta.url), "utf8"),
  agentFile: (name) => name === "agent.mjs" ? bundle.legacyAgent : bundle.files[name],
  agentBundle: () => bundle,
  alarm(at) {
    clearTimeout(timer);
    timer = setTimeout(() => control.alarm().catch((e) => log(`alarm failed: ${e.message}`)), Math.max(0, at - Date.now()));
  },
  restart() {
    log("restarting on request");
    shutdown(0);
  },
});
sql.protect(control);

const server = http.createServer(async (req, res) => {
  try {
    if (sql.failed) throw new Error("configuration storage is unavailable");
    if (req.method === "GET" && req.url === "/healthz") {
      res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
      return res.end(JSON.stringify({ ok: true, tunnelsReady: control.tunnelsReady }));
    }
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
    // Only this adapter supplies the identity used for rate limits/geolocation.
    delete headers["x-runner-client-ip"];
    delete headers["cf-connecting-ip"];
    headers["x-runner-client-ip"] = clientAddress(req, env) || "unknown";
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
const agentSockets = new Map();
const status = control.status.bind(control);
control.status = () => ({ ...status(), controlPlane: {
  mode: "vps-v1", owner: "vps", name: env.CONTROLLER_NAME || "VPS",
  configStorage: "local-sqlite", enabled: true, agentsConnected: agentSockets.size,
  runtimeStorage: "memory", metricsHistoryMinutes: Number(env.MEMORY_HISTORY_MIN),
  tunnelStorage: "memory", tunnelsReady: control.tunnelsReady, tunnelsDiscovered: control.slots.size,
  tunnelRecoveryError: control.tunnelRecoveryError,
  recoveryUntil: control.recoveringUntil, recoveryMissing: [...control.recoveryMissing],
} });
const originalWake = control.wake.bind(control);
control.wake = (run) => {
  const ws = agentSockets.get(run);
  if (ws?.readyState === ws.OPEN) ws.send(JSON.stringify({ type: "wake" }));
  originalWake(run);
};
server.on("upgrade", (req, socket, head) => {
  const refuse = (status) => {
    socket.end(`HTTP/1.1 ${status} ${http.STATUS_CODES[status]}\r\nconnection: close\r\ncontent-length: 0\r\n\r\n`);
  };
  try {
    const url = new URL(req.url, "http://localhost");
    if (url.pathname === "/api/agent") {
      if (!control.isJoinToken(req.headers.authorization)) return refuse(401);
      return wss.handleUpgrade(req, socket, head, (ws) => {
        let run = null;
        ws.on("message", (raw) => {
          let message;
          try {
            if (sql.failed) throw new Error("configuration storage is unavailable");
            message = JSON.parse(raw);
            if (message.type !== "sync" || !Number.isSafeInteger(message.id)) throw new Error("invalid report");
            run = message.body?.run;
            if (run) agentSockets.set(run, ws);
            control.setPace(Date.now());
            const plan = control.sync(message.body, clientAddress(req, env));
            ws.send(JSON.stringify({ type: "plan", id: message.id, plan }));
          } catch (error) { ws.send(JSON.stringify({ type: "error", id: message?.id, error: error.message })); }
        });
        ws.on("close", () => { if (agentSockets.get(run) === ws) agentSockets.delete(run); });
        ws.on("error", () => {});
      });
    }
    const viewer = url.pathname.match(/^\/admin\/api\/logs\/([0-9a-f-]{36})$/);
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
server.listen(port, host, () => log(`control plane listening on ${host}:${port}, data in ${dataDir}`));

let stopping = false;
function shutdown(code) {
  if (stopping) return;
  stopping = true;
  clearTimeout(timer);
  for (const ws of wss.clients) ws.terminate();
  server.close(() => {
    sql.close();
    process.exit(code);
  });
  setTimeout(() => process.exit(code), 5_000).unref();
}
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => shutdown(0));
