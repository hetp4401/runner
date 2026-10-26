// Outbound-only Mac primary. It holds a disposable SQLite database in memory;
// D1 and Cloudflare mutations are committed/fenced by the Durable Object.
import { DatabaseSync } from "node:sqlite";
import WebSocket from "ws";
import { readFileSync } from "node:fs";
import { WorkingStore } from "./src/hybrid/store.js";
import { Engine } from "./src/hybrid/engine.js";
import { restore } from "./src/hybrid/codec.js";

const env = process.env;
if (!env.CONTROL_URL || !env.MAC_TOKEN) throw new Error("CONTROL_URL and MAC_TOKEN are required");
const page = readFileSync(new URL("./src/app.html", import.meta.url), "utf8");
const log = (message) => console.log(`${new Date().toISOString()} ${message}`);
let stopping = false;
let active = null;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function session() {
  const url = new URL("/internal/mac", env.CONTROL_URL);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  const ws = new WebSocket(url, { headers: { authorization: `Bearer ${env.MAC_TOKEN}` }, maxPayload: 32_000_000, handshakeTimeout: 20_000 });
  active = ws;
  let db = null, engine = null, sequence = 0, epoch = 0, operation = null;
  let work = Promise.resolve();
  let lastReceived = Date.now();
  const effects = new Map();
  const send = (message) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message)); };
  const heartbeat = setInterval(() => {
    if (Date.now() - lastReceived > 75_000) return ws.terminate();
    if (engine) send({ type: "heartbeat", sequence, epoch });
  }, Number(env.HEARTBEAT_MS ?? 30_000));
  return new Promise((resolve) => {
    ws.on("open", () => log("connected; loading the current controller state"));
    ws.on("message", (raw) => {
      lastReceived = Date.now();
      let message;
      try { message = JSON.parse(raw); } catch { return ws.close(1008, "invalid gateway message"); }
      if (message.type === "effect-result") {
        const pending = effects.get(message.id);
        if (pending) { effects.delete(message.id); clearTimeout(pending.timer); message.error ? pending.reject(new Error(message.error)) : pending.resolve(message.result); }
        return;
      }
      work = work.then(async () => {
        switch (message.type) {
          case "bootstrap": {
            db?.close();
            db = new DatabaseSync(":memory:");
            const store = new WorkingStore((query, args) => db.prepare(query).all(...args));
            engine = new Engine({ store, env: message.env, page, effect: (path, init) => {
              if (!operation || !epoch || ws.readyState !== WebSocket.OPEN) throw new Error("no active leadership operation");
              const id = crypto.randomUUID();
              return new Promise((resolve, reject) => {
                const timer = setTimeout(() => { effects.delete(id); reject(new Error("Cloudflare operation timed out")); }, 20_000);
                effects.set(id, { resolve, reject, timer });
                send({ type: "effect", id, operation, epoch, path, init });
              });
            } });
            break;
          }
          case "table": {
            if (!engine) throw new Error("snapshot arrived before bootstrap");
            engine.store.load({ [message.table]: message.rows }, { replace: message.clear });
            break;
          }
          case "bootstrapped": {
            engine.makeControl();
            restore(engine.control, message.memory);
            engine.alarmAt = message.alarmAt;
            engine.baseline = message.memory;
            engine.store.takeJournal(); engine.store.dirty.clear();
            sequence = message.sequence;
            send({ type: "ready", sequence });
            log("state loaded; standing by for a coordinated handover");
            break;
          }
          case "mirror": {
            if (message.sequence !== sequence + 1) throw new Error("mirror sequence gap");
            engine.accept(message.transaction);
            sequence = message.sequence;
            send({ type: "ack", sequence });
            break;
          }
          case "leadership": {
            if (message.sequence !== sequence) throw new Error("leadership snapshot is incomplete");
            epoch = message.epoch;
            log(`primary active, generation ${epoch}`);
            break;
          }
          case "execute": {
            if (message.epoch !== epoch || message.sequence !== sequence) throw new Error("stale execution request");
            operation = message.id;
            try {
              const transaction = await engine.execute(message.operation);
              send({ type: "result", id: message.id, epoch, transaction });
            } catch (error) { send({ type: "execution-error", id: message.id, epoch, error: error.message }); }
            finally { operation = null; }
            break;
          }
          case "committed": sequence = message.sequence; break;
        }
      }).catch((error) => { log(error.message); ws.close(1012, "reload state"); });
    });
    ws.on("error", (error) => log(`connection: ${error.message}`));
    ws.on("close", () => {
      clearInterval(heartbeat);
      epoch = 0;
      for (const pending of effects.values()) { clearTimeout(pending.timer); pending.reject(new Error("leadership connection closed")); }
      effects.clear();
      work.finally(() => { try { db?.close(); } catch {} resolve(); });
      log("disconnected; Cloudflare owns failover");
    });
  });
}
for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopping = true; active?.close(1000, "Mac controller stopping"); });
while (!stopping) {
  await session();
  if (!stopping) await sleep(5000 + Math.random() * 5000);
}
