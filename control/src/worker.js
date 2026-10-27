import { DurableObject } from "cloudflare:workers";
import initSqlJs from "sql.js/dist/sql-wasm-browser.js";
import wasm from "../node_modules/sql.js/dist/sql-wasm-browser.wasm";
import page from "./app.html";
import { wasmStore, CONFIG_TABLES } from "./hybrid/store.js";
import { capture } from "./hybrid/codec.js";
import { Configuration, CREDENTIAL_KEYS } from "./hybrid/config.js";
import { Engine, operationOf, responseOf } from "./hybrid/engine.js";

// Emscripten reads self.location even when the compiled module is supplied. CF
// has no location global; this inert URL is never fetched by our module loader.
if (!globalThis.location) globalThis.location = Object.freeze({ href: "https://runner.invalid/sql-wasm.js" });
const SQL = initSqlJs({ instantiateWasm(imports, receive) {
  const instance = new WebAssembly.Instance(wasm, imports);
  receive(instance, wasm);
  return instance.exports;
} });
const json = (body, status = 200) => Response.json(body, { status });
const send = (ws, message) => { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message)); };
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const LEASE_MS = 90_000;

export default {
  fetch(request, env) {
    if (new URL(request.url).pathname === "/internal/mac" && env.SCHEDULER !== "hybrid") return json({ error: "external schedulers are disabled" }, 403);
    const forwarded = new Request(request);
    forwarded.headers.delete("x-runner-location");
    const cf = request.cf;
    if (cf?.country && cf.region) {
      const place = [cf.city, cf.region, cf.country].filter(Boolean).join(", ");
      forwarded.headers.set("x-runner-location", encodeURIComponent([place, cf.asOrganization].filter(Boolean).join(" · ").slice(0, 120)));
    }
    return env.CONTROL.get(env.CONTROL.idFromName("fleet")).fetch(forwarded);
  },
};

export class FleetCoordinator extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx; this.env = env;
    this.chain = Promise.resolve();
    this.effects = Promise.resolve();
    this.agents = new Map();
    this.sequence = 0;
    this.mac = null;
    this.pending = null;
    this.requests = 0;
    this.lastError = null;
    this.queueDepth = 0;
    ctx.blockConcurrencyWhile(async () => {
      this.config = new Configuration(env.CONFIG, env.CONFIG_KEY);
      const tables = await this.config.load();
      // Bootstrap secrets are only needed for the initial deployment. Thereafter
      // encrypted D1 is the source and authentication uses this in-memory cache.
      await this.config.setCredentials(Object.fromEntries(CREDENTIAL_KEYS.filter((key) => !this.config.credentials.has(key) && env[key]).map((key) => [key, env[key]])));
      this.env = { ...env, ...Object.fromEntries(this.config.credentials) };
      const store = wasmStore(await SQL);
      this.engine = new Engine({ store, env: { ...this.env, USE_EDGE_LOCATION: "1", RECOVER_RUNTIME: "1", MEMORY_HISTORY_MIN: "60", POLL_S: "60" }, page,
        effect: (path, init) => this.effect(path, init), wake: (run) => this.wake(run), restart: () => this.restart() });
      this.engine.load(tables);
      // A new incarnation always fences old sockets. Leases and heartbeats are
      // memory only; a unique generation avoids a persistent counter.
      this.leadership = { epoch: crypto.randomUUID(), owner: "do", expires: 0, enabled: (await ctx.storage.get("enabled")) === true };
      await this.schedule();
    });
    // One coordinator is budgeted at <=11,060 GB-s/day even if continuously
    // resident. Its DB contains only the deployment switch and watchdog alarm.
    this.timer = setInterval(() => this.enqueue(() => this.tick()).catch((e) => this.error(e)), Number(env.TICK_MS ?? 30_000));
  }
  error(error) { this.lastError = String(error?.message ?? error).slice(0, 250); console.error(this.lastError); }
  enqueue(fn) {
    if (this.queueDepth > 500) return Promise.reject(new Error("coordinator busy; retry"));
    this.queueDepth++;
    const work = this.chain.then(fn).finally(() => { this.queueDepth--; });
    this.chain = work.catch(() => {});
    return work;
  }
  authorized(request, key) { return Boolean(key) && request.headers.get("authorization") === `Bearer ${key}`; }
  async schedule() {
    const now = Date.now();
    const at = Math.max(now + 1000, Math.min(now + 60_000, this.leadership?.enabled ? this.engine?.alarmAt ?? Infinity : Infinity,
      this.leadership?.owner === "mac" ? this.leadership.expires : Infinity));
    const rounded = Math.ceil(at / 1000) * 1000;
    // Never slide an existing alarm forward on each report/heartbeat. One
    // watchdog write per minute is enough; an earlier deadline can bring it in.
    if (!this.scheduledAt || this.scheduledAt <= now || rounded < this.scheduledAt) {
      await this.ctx.storage.setAlarm(rounded); this.scheduledAt = rounded;
    }
  }
  async alarm() { this.scheduledAt = null; return this.enqueue(() => this.tick()); }
  async tick() {
    if (this.leadership.owner === "mac" && (!this.mac || Date.now() >= this.leadership.expires)) await this.takeover("Mac lease expired");
    if (this.mac?.readyAt && this.leadership.owner === "do" && Date.now() - this.mac.readyAt >= Number(this.env.FAILBACK_MS ?? 120_000)) await this.grantMac();
    if (this.leadership.enabled && Date.now() >= (this.engine.alarmAt ?? Infinity)) await this.execute({ kind: "alarm" });
    await this.schedule();
  }
  async takeover(reason) {
    const old = this.mac;
    this.pending?.reject(new Error(reason));
    this.pending = null;
    this.mac = null;
    old?.ws.close(1012, "controller handover");
    // Finish all already-issued Cloudflare mutations before the new scheduler
    // starts. Stale Mac commands cannot bypass this gate or retain API keys.
    await this.effects.catch(() => {});
    this.leadership = { ...this.leadership, owner: "do", expires: 0, epoch: crypto.randomUUID() };
    console.log(`fallback active: ${reason}`);
  }
  async grantMac() {
    if (this.env.SCHEDULER !== "hybrid") return;
    const mac = this.mac;
    if (!mac || !mac.readyAt || mac.ws.readyState !== WebSocket.OPEN) return;
    if (mac.ack < this.sequence) {
      const target = this.sequence;
      const ok = await Promise.race([new Promise((resolve) => { mac.barrier = { target, resolve }; }), delay(5000).then(() => false)]);
      mac.barrier = null;
      if (!ok || this.mac !== mac) return;
    }
    this.leadership = { ...this.leadership, epoch: crypto.randomUUID(), owner: "mac", expires: Date.now() + Number(this.env.LEASE_MS ?? LEASE_MS) };
    send(mac.ws, { type: "leadership", ...this.leadership, sequence: this.sequence });
    console.log("Mac primary active");
  }
  async effect(path, init = {}) {
    const account = `/accounts/${this.env.ACCOUNT_ID}/cfd_tunnel`;
    const zone = `/zones/${this.env.ZONE}/dns_records`;
    if (!((path === account || path.startsWith(account + "/") || path.startsWith(account + "?")) ||
      (path === zone || path.startsWith(zone + "/") || path.startsWith(zone + "?"))) || path.includes("..")) throw new Error("Cloudflare operation outside runner scope");
    const work = this.effects.then(async () => {
      const auth = this.env.CF_API_TOKEN ? { authorization: `Bearer ${this.env.CF_API_TOKEN}` }
        : { "x-auth-key": this.env.CF_API_KEY, "x-auth-email": this.env.CF_API_EMAIL };
      const response = await fetch(`${this.env.CF_API_BASE ?? "https://api.cloudflare.com/client/v4"}${path}`, {
        method: init.method || "GET", body: init.body, headers: { ...auth, "content-type": "application/json" }, signal: AbortSignal.timeout(15_000) });
      const data = await response.json();
      if (!data.success) throw new Error(`Cloudflare operation failed: ${response.status} ${JSON.stringify(data.errors)}`);
      return data;
    });
    this.effects = work.catch(() => {});
    return work;
  }
  async execute(operation) {
    // Do not release any further command while a previous configuration commit
    // is unconfirmed. Retrying the same encrypted UPSERTs is idempotent.
    await this.config.flush(this.engine.store);
    if (this.leadership.owner === "mac" && (!this.mac || Date.now() >= this.leadership.expires)) await this.takeover("Mac unavailable");
    let transaction;
    if (this.leadership.owner === "mac") {
      try {
        const id = crypto.randomUUID();
        transaction = await new Promise((resolve, reject) => {
          const timer = setTimeout(() => reject(new Error("Mac execution timed out")), Number(this.env.EXECUTION_TIMEOUT_MS ?? 25_000));
          this.pending = { id, epoch: this.leadership.epoch, resolve: (value) => { clearTimeout(timer); resolve(value); }, reject: (error) => { clearTimeout(timer); reject(error); } };
          send(this.mac.ws, { type: "execute", id, epoch: this.leadership.epoch, sequence: this.sequence, operation });
        });
        this.pending = null;
        this.engine.accept(transaction, { track: true });
      } catch (error) {
        await this.takeover(error.message);
        transaction = await this.engine.execute(operation);
      }
    } else transaction = await this.engine.execute(operation, { mirror: this.env.SCHEDULER === "hybrid" });
    await this.config.flush(this.engine.store);
    this.engine.store.takeJournal();
    this.sequence++;
    if (this.mac) {
      if (this.leadership.owner === "mac") send(this.mac.ws, { type: "committed", sequence: this.sequence });
      else send(this.mac.ws, { type: "mirror", sequence: this.sequence, transaction });
    }
    await this.schedule();
    return transaction.result;
  }
  wake(run) { send(this.agents.get(run), { type: "wake" }); }
  info() {
    return { owner: this.leadership.owner, epoch: this.leadership.epoch, leaseExpires: this.leadership.expires,
      enabled: this.leadership.enabled, macConnected: Boolean(this.mac), macReady: Boolean(this.mac?.readyAt),
      agentsConnected: this.agents.size, sequence: this.sequence, recoveryUntil: this.engine.control.recoveringUntil,
      recoveryMissing: [...this.engine.control.recoveryMissing], d1RowsRead: this.config.reads, d1RowsWritten: this.config.writes,
      requests: this.requests, lastError: this.lastError, mode: this.env.SCHEDULER === "hybrid" ? "hybrid-v2" : "cloudflare-v1",
      configuredApps: this.engine.control.projects.size, tunnelCredentials: this.engine.control.slots.size };
  }
  socketPair() { const pair = new WebSocketPair(); pair[1].accept(); return pair; }
  async fetch(request) {
    this.requests++;
    const url = new URL(request.url);
    try {
      if (url.pathname === "/internal/hybrid/status") return json(this.info());
      if (url.pathname === "/internal/reset" && request.method === "POST") {
        if (!this.authorized(request, this.env.MAC_TOKEN)) return json({ error: "unauthorized" }, 401);
        return await this.enqueue(async () => {
          if (this.leadership.enabled) return json({ error: "disable the controller before a cold recovery test" }, 409);
          await this.config.flush(this.engine.store);
          await this.takeover("cold recovery test");
          for (const ws of this.agents.values()) ws.close(1012, "controller restarting");
          setTimeout(() => { this.engine.store.close(); this.ctx.abort("requested cold recovery test"); }, 50);
          return json({ restarting: true }, 202);
        });
      }
      if (url.pathname === "/internal/mac" && request.headers.get("upgrade")?.toLowerCase() === "websocket") {
        if (this.env.SCHEDULER !== "hybrid") return json({ error: "external schedulers are disabled" }, 403);
        if (!this.authorized(request, this.env.MAC_TOKEN)) return json({ error: "unauthorized" }, 401);
        return await this.enqueue(() => this.connectMac());
      }
      if (url.pathname === "/internal/enable" && request.method === "POST") {
        if (!this.authorized(request, this.env.MAC_TOKEN)) return json({ error: "unauthorized" }, 401);
        return await this.enqueue(async () => {
          const { enabled } = await request.json();
          this.leadership.enabled = enabled === true;
          await this.ctx.storage.put("enabled", this.leadership.enabled);
          if (!this.leadership.enabled) {
            for (const ws of this.agents.values()) ws.close(1012, "controller disabled");
            this.agents.clear();
          }
          return json(this.info());
        });
      }
      if (url.pathname === "/internal/import" && request.method === "POST") {
        if (!this.authorized(request, this.env.MAC_TOKEN)) return json({ error: "unauthorized" }, 401);
        return await this.enqueue(async () => {
          if (this.leadership.enabled) return json({ error: "disable the staged controller before importing" }, 409);
          if (this.mac) return json({ error: "stop the Mac controller before importing" }, 409);
          const { table, rows, clear = false } = await request.json();
          this.engine.store.load({ [table]: rows }, { replace: clear });
          if (CONFIG_TABLES.includes(table)) this.engine.store.dirty.add(table);
          await this.config.flush(this.engine.store);
          this.engine.makeControl();
          this.engine.baseline = capture(this.engine.control);
          this.engine.store.takeJournal();
          return json({ imported: rows.length });
        });
      }
      if (url.pathname === "/api/wake") return json({ error: "use agent WebSocket" }, 404);
      if (url.pathname === "/api/agent" && request.headers.get("upgrade")?.toLowerCase() === "websocket") {
        if (!this.authorized(request, this.env.JOIN_TOKEN)) return json({ error: "unauthorized" }, 401);
        if (!this.leadership.enabled) return json({ error: "migration is not active" }, 503);
        return this.connectAgent(request);
      }
      if (request.headers.get("upgrade")?.toLowerCase() === "websocket") return this.connectLog(request);
      if (!this.leadership.enabled && !this.authorized(request, this.env.MAC_TOKEN) && request.headers.get("x-admin-password") !== this.env.ADMIN_PASSWORD) return json({ error: "migration is not active" }, 503);
      const operation = await operationOf(request);
      return await this.enqueue(async () => {
        // Log sockets belong to the gateway even when the Mac schedules work.
        const logs = /\/(?:admin\/)?api\/projects\/[^/]+\/logs$/.test(url.pathname);
        let result;
        if (logs) { const tx = await this.engine.execute(operation, { mirror: this.env.SCHEDULER === "hybrid" }); result = tx.result; }
        else result = await this.execute(operation);
        if (/\/(?:admin\/)?api\/status$/.test(url.pathname) && result.status === 200) {
          const body = JSON.parse(result.body); body.controlPlane = this.info(); result.body = JSON.stringify(body);
        }
        return responseOf(result);
      });
    } catch (error) { this.error(error); return json({ error: error.message }, 503); }
  }
  connectAgent(request) {
    const [client, server] = Object.values(this.socketPair());
    let run = null;
    let busy = false;
    server.addEventListener("message", (event) => {
      if (busy) return server.close(1008, "one report at a time");
      let message;
      try { if (typeof event.data !== "string" || event.data.length > 1_000_000) throw new Error(); message = JSON.parse(event.data); }
      catch { return server.close(1008, "invalid report"); }
      if (message.type !== "sync" || !Number.isSafeInteger(message.id)) return server.close(1008, "invalid report");
      busy = true;
      this.enqueue(async () => {
        if (!this.leadership.enabled) throw new Error("controller disabled; keep current workloads");
        run = String(message.body?.run || "");
        if (!run) throw new Error("run is required");
        this.agents.set(run, server);
        const result = await this.execute({ kind: "http", url: new URL("/api/sync", request.url).href, method: "POST",
          headers: [["authorization", `Bearer ${this.env.JOIN_TOKEN}`], ["content-type", "application/json"], ["cf-connecting-ip", request.headers.get("cf-connecting-ip") || ""], ["x-runner-location", request.headers.get("x-runner-location") || ""]], body: JSON.stringify(message.body) });
        if (result.status !== 200) throw new Error(JSON.parse(result.body).error || `report failed: ${result.status}`);
        const plan = JSON.parse(result.body);
        const logs = [...this.engine.control.logSessions.values()].filter((s) => s.viewer && !s.agent && s.machine === message.body.machine && s.key in (message.body.status ?? {})).map((s) => ({ session: s.id, key: s.key, tail: 200 }));
        if (logs.length) plan.logs = logs;
        send(server, { type: "plan", id: message.id, epoch: this.leadership.epoch, plan });
      }).catch((error) => { this.error(error); send(server, { type: "error", id: message.id, error: error.message }); }).finally(() => { busy = false; });
    });
    server.addEventListener("close", () => { if (this.agents.get(run) === server) this.agents.delete(run); });
    return new Response(null, { status: 101, webSocket: client });
  }
  connectMac() {
    if (this.mac?.ws.readyState === WebSocket.OPEN) return json({ error: "a Mac is already connected" }, 409);
    const [client, server] = Object.values(this.socketPair());
    const mac = this.mac = { ws: server, id: crypto.randomUUID(), ack: -1, readyAt: 0 };
    server.addEventListener("message", (event) => this.macMessage(mac, event.data));
    server.addEventListener("close", () => {
      if (this.mac !== mac) return;
      this.pending?.reject(new Error("Mac connection closed"));
      this.enqueue(() => this.mac === mac ? this.takeover("Mac connection closed") : null).catch((e) => this.error(e));
    });
    const safeEnv = Object.fromEntries(["DOMAIN", "CONTROL_HOST", "ACCOUNT_ID", "ZONE", "POOLS", "MAX_SLOTS", "ADMIN_PASSWORD", "JOIN_TOKEN", "STATE_SIGNING_KEY", "QUIET_MS", "YOUNG_MS", "DNS", "AGENT_URL", "RECOVERY_MS"].filter((key) => this.env[key] != null).map((key) => [key, this.env[key]]));
    send(server, { type: "bootstrap", env: { ...safeEnv, USE_EDGE_LOCATION: "1", RECOVER_RUNTIME: "1", MEMORY_HISTORY_MIN: "60", POLL_S: "60" }, session: mac.id });
    for (const table of this.engine.store.tables()) {
      send(server, { type: "table", table, rows: [], clear: true });
      for (const row of this.engine.store.rows(table)) send(server, { type: "table", table, rows: [row], clear: false });
    }
    send(server, { type: "bootstrapped", sequence: this.sequence, ...this.engine.checkpoint() });
    return new Response(null, { status: 101, webSocket: client });
  }
  macMessage(mac, raw) {
    if (this.mac !== mac) return;
    let message;
    try { if (typeof raw !== "string" || raw.length > 16_000_000) throw new Error(); message = JSON.parse(raw); }
    catch { return mac.ws.close(1008, "invalid message"); }
    if (message.type === "result" || message.type === "execution-error") {
      const pending = this.pending;
      if (!pending || message.id !== pending.id || message.epoch !== pending.epoch || this.leadership.owner !== "mac") return;
      if (message.type === "execution-error") pending.reject(new Error(message.error));
      else pending.resolve(message.transaction);
      return;
    }
    if (message.type === "effect") {
      if (!this.pending || message.operation !== this.pending.id || message.epoch !== this.leadership.epoch || this.leadership.owner !== "mac" || Date.now() >= this.leadership.expires) {
        send(mac.ws, { type: "effect-result", id: message.id, error: "leadership is no longer valid" }); return;
      }
      this.effect(message.path, message.init).then((result) => send(mac.ws, { type: "effect-result", id: message.id, result }),
        (error) => send(mac.ws, { type: "effect-result", id: message.id, error: error.message }));
      return;
    }
    if (["ack", "ready", "heartbeat"].includes(message.type)) {
      if (Number.isSafeInteger(message.sequence)) mac.ack = Math.max(mac.ack, message.sequence);
      if (mac.barrier && mac.ack >= mac.barrier.target) mac.barrier.resolve(true);
      if (message.type === "ack") return;
      this.enqueue(async () => {
        if (this.mac !== mac) return;
        if (!mac.readyAt && message.type === "ready") mac.readyAt = Date.now();
        send(mac.ws, { type: "heartbeat-ack" });
        if (this.leadership.owner === "mac" && message.epoch === this.leadership.epoch && Date.now() < this.leadership.expires) {
          this.leadership.expires = Date.now() + Number(this.env.LEASE_MS ?? LEASE_MS);
        }
        await this.tick();
      }).catch((e) => this.error(e));
    }
  }
  connectLog(request) {
    const url = new URL(request.url);
    const viewer = url.pathname.match(/^\/admin\/api\/logs\/([0-9a-f-]{36})$/);
    if (!viewer && (url.pathname !== "/api/logs/stream" || !this.authorized(request, this.env.JOIN_TOKEN))) return json({ error: "unauthorized" }, 401);
    if (viewer && request.headers.get("origin") && new URL(request.headers.get("origin")).host !== url.host) return json({ error: "origin refused" }, 403);
    const [client, server] = Object.values(this.socketPair());
    const adapter = { send: (text) => { if (server.readyState === WebSocket.OPEN) server.send(text); }, close: (code, reason) => server.close(code, reason),
      onMessage: (fn) => server.addEventListener("message", (event) => fn(event.data)), onClose: (fn) => server.addEventListener("close", fn) };
    if (viewer) this.engine.control.attachLogViewer(viewer[1], adapter);
    else this.engine.control.attachLogAgent(url.searchParams.get("session"), url.searchParams.get("run"), adapter);
    return new Response(null, { status: 101, webSocket: client });
  }
  restart() {
    // The admin restart API changes leadership safely without discarding agent
    // inventory. Full process eviction is exercised separately in recovery tests.
    this.enqueue(() => this.takeover("restart requested")).catch((e) => this.error(e));
  }
}
