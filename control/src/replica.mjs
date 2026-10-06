// The control plane as one of several copies (STORE_URLS set; see the README). The copies share two private keys in
// zkmetadata: a lease, and a snapshot of the state. The copy holding the lease is the leader: it alone builds the
// control plane (from the latest snapshot), serves, and runs the tunnel connector that gives it runners.<domain>; it
// saves the state every minute, soon after every change that isn't an agent's check-in, and when it stops. The other
// copies wait, and take over when the lease runs out (a leader that stops cleanly hands it on at once). Nothing here
// needs a particular machine: a copy restored from the snapshot carries on where the last leader left off, and the
// agents' next check-ins fill in the rest.
import { spawn } from "node:child_process";
import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";
import { gunzipSync, gzipSync } from "node:zlib";

// What a snapshot holds: everything but the metrics (a new leader starts its charts afresh).
export const SNAP_TABLES = ["projects", "versions", "copies", "runs", "starts", "settings", "slots", "agents", "app_passwords", "app_env", "events"];
const LEASE = "control.lease";
const SNAP = "control.snap"; // the manifest: {gen, n, sha256, t, by}; the sealed snapshot is in control.snap.<gen>.<i>
const CHUNK = 700_000; // bytes per key (zkmetadata takes 900 KB)
const NEW = 999_999_999; // as a version to write at: only if the key doesn't exist yet (no real version gets this high)

export class LostLease extends Error {}

// zkmetadata's key-value API on any of its replicas, tried in turn: the keys are private, made and read with the admin
// password (which zkmetadata knows only as a hash).
export class Store {
  constructor(urls, password) {
    this.urls = urls;
    this.password = password;
    this.next = 0;
  }

  async call(method, key, { body, version } = {}) {
    let last;
    for (let i = 0; i < this.urls.length; i++) {
      const at = (this.next + i) % this.urls.length;
      const headers = { "x-admin-password": this.password, "x-password": this.password, "user-agent": "runner-control" };
      if (method === "PUT") Object.assign(headers, { "x-private": "1", "content-type": "application/octet-stream" });
      if (version != null) headers["if-match"] = String(version);
      try {
        const r = await fetch(`${this.urls[at]}/kv/${encodeURIComponent(key)}`, { method, headers, body, signal: AbortSignal.timeout(15_000) });
        if (r.status >= 500 || r.status === 429) {
          last = new Error(`${this.urls[at]}: HTTP ${r.status} ${(await r.text()).slice(0, 200)}`);
          continue; // that replica can't serve right now: the next one
        }
        this.next = at; // keep to one that answers
        return r;
      } catch (e) {
        last = e;
      }
    }
    throw last ?? new Error("no zkmetadata replica answered");
  }

  async get(key) {
    const r = await this.call("GET", key);
    if (r.status === 404) return null;
    if (!r.ok) throw new Error(`reading ${key}: HTTP ${r.status} ${await r.text()}`);
    return { data: Buffer.from(await r.arrayBuffer()), version: Number(r.headers.get("x-version")) };
  }

  // Writes the key; with a version, only if it's still at it (NEW: only if there's no such key). The new version, or
  // null when someone else got there first.
  async put(key, data, { version = null } = {}) {
    const r = await this.call("PUT", key, { body: data, version });
    if (r.status === 409) return null;
    if (!r.ok) throw new Error(`writing ${key}: HTTP ${r.status} ${await r.text()}`);
    return (await r.json()).version;
  }

  async del(key) {
    const r = await this.call("DELETE", key);
    if (!r.ok && r.status !== 404) throw new Error(`deleting ${key}: HTTP ${r.status} ${await r.text()}`);
  }
}

// The lease: {holder, url, until, since}, kept by writing it at its version (so of two copies only one can ever win).
export class Lease {
  constructor(store, me, ms = 30_000) {
    this.store = store;
    this.me = me; // {id, url}
    this.ms = ms;
    this.version = null; // the version this copy last wrote, while it's the leader
  }

  async read() {
    const got = await this.store.get(LEASE);
    return got ? { ...JSON.parse(got.data.toString()), version: got.version } : null;
  }

  // Takes the lease if it's free: nobody has it, it ran out (with 5 s for clocks that disagree), or it's this copy's.
  async take(now = Date.now()) {
    const cur = await this.read();
    if (cur && cur.holder !== this.me.id && cur.until > now - 5_000) return { taken: false, cur };
    const v = await this.store.put(LEASE, this.body(now), { version: cur ? cur.version : NEW });
    if (v == null) {
      const after = await this.read();
      if (after?.holder !== this.me.id) return { taken: false, cur: after };
      this.version = after.version; // (an earlier try of this write went through: see renew)
      return { taken: true, cur };
    }
    this.version = v;
    return { taken: true, cur };
  }

  // Renews it: throws LostLease when another copy has it now. A store that doesn't answer leaves it as it is (the
  // other copies can't take it either while the store is down). A write is tried again on another zkmetadata replica
  // when the first doesn't answer, and the first may have written it all the same: then the retry finds the key a
  // version on, written by this copy. So a conflict is only another copy's doing if the lease says so.
  async renew(now = Date.now()) {
    const v = await this.store.put(LEASE, this.body(now), { version: this.version });
    if (v != null) {
      this.version = v;
      return;
    }
    const cur = await this.read();
    if (cur?.holder !== this.me.id) throw new LostLease(`${cur?.holder ?? "nobody"} holds the lease now`);
    this.version = cur.version;
  }

  // Hands it on at once (a clean stop): it runs out now.
  async release() {
    if (this.version == null) return;
    await this.store.put(LEASE, JSON.stringify({ holder: this.me.id, url: this.me.url, until: 0, since: 0 }), { version: this.version });
    this.version = null;
  }

  body(now) {
    return JSON.stringify({ holder: this.me.id, url: this.me.url, until: now + this.ms, since: now });
  }
}

// Sealing: gzip, then AES-256-GCM with a key made from STATE_KEY (zkmetadata's values are readable to anything on the
// tailnet, and the state holds tunnel tokens and apps' secrets).
export function sealKey(secret) {
  return createHash("sha256").update(String(secret)).digest();
}

function seal(plain, key) {
  const iv = randomBytes(12);
  const c = createCipheriv("aes-256-gcm", key, iv);
  const body = Buffer.concat([c.update(gzipSync(plain)), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), body]);
}

function unseal(sealed, key) {
  const d = createDecipheriv("aes-256-gcm", key, sealed.subarray(0, 12));
  d.setAuthTag(sealed.subarray(12, 28));
  return gunzipSync(Buffer.concat([d.update(sealed.subarray(28)), d.final()]));
}

const sha256 = (b) => createHash("sha256").update(b).digest("hex");

export function exportState(db) {
  const tables = {};
  for (const t of SNAP_TABLES) tables[t] = db.prepare(`SELECT * FROM ${t}`).all();
  return Buffer.from(JSON.stringify({ t: Date.now(), tables }));
}

// Puts a snapshot's rows in place of the tables' (the schema must exist; columns the snapshot has that the tables
// don't are left out, so an older or newer snapshot still loads). The metrics tables are left as they are.
export function importState(db, tables) {
  db.exec("BEGIN");
  try {
    for (const [t, rows] of Object.entries(tables)) {
      if (!SNAP_TABLES.includes(t)) continue;
      const known = new Set(db.prepare(`PRAGMA table_info(${t})`).all().map((c) => c.name));
      db.prepare(`DELETE FROM ${t}`).run();
      for (const row of rows) {
        const cols = Object.keys(row).filter((c) => known.has(c));
        db.prepare(`INSERT OR REPLACE INTO ${t} (${cols.join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`).run(...cols.map((c) => row[c]));
      }
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

export class Snapshots {
  constructor(store, key, me) {
    this.store = store;
    this.key = key;
    this.me = me;
    this.version = null; // the manifest's version this leader last read or wrote: a write at it fails if another wrote since
    this.manifest = null;
  }

  // The latest snapshot's tables, or null if there's none yet.
  async load() {
    const got = await this.store.get(SNAP);
    if (!got) {
      this.version = NEW;
      return null;
    }
    const m = JSON.parse(got.data.toString());
    const parts = [];
    for (let i = 0; i < m.n; i++) {
      const part = await this.store.get(`${SNAP}.${m.gen}.${i}`);
      if (!part) throw new Error(`the snapshot ${m.gen} is missing part ${i}`);
      parts.push(part.data);
    }
    const sealed = Buffer.concat(parts);
    if (sha256(sealed) !== m.sha256) throw new Error(`the snapshot ${m.gen} doesn't match its checksum`);
    this.version = got.version;
    this.manifest = m;
    return { manifest: m, tables: JSON.parse(unseal(sealed, this.key).toString()).tables };
  }

  // Saves the state: the parts first, then the manifest that points at them (at the version this leader knows, so a
  // copy that's no longer the leader can't overwrite a newer snapshot: LostLease), then the old parts go.
  async save(db) {
    const sealed = seal(exportState(db), this.key);
    const gen = Date.now().toString(36);
    const n = Math.ceil(sealed.length / CHUNK);
    for (let i = 0; i < n; i++) await this.store.put(`${SNAP}.${gen}.${i}`, sealed.subarray(i * CHUNK, (i + 1) * CHUNK));
    const m = { gen, n, sha256: sha256(sealed), t: Date.now(), by: this.me.id, bytes: sealed.length };
    let v = await this.store.put(SNAP, JSON.stringify(m), { version: this.version ?? NEW });
    if (v == null) { // another copy's snapshot, or this very write's earlier try (see Lease.renew)
      const got = await this.store.get(SNAP);
      const cur = got && JSON.parse(got.data.toString());
      if (cur?.by === this.me.id && cur.gen === gen) v = got.version;
      else {
        for (let i = 0; i < n; i++) await this.store.del(`${SNAP}.${gen}.${i}`).catch(() => {});
        throw new LostLease("another copy saved a snapshot since: it's the leader now");
      }
    }
    const old = this.manifest;
    this.version = v;
    this.manifest = m;
    if (old && old.gen !== gen) for (let i = 0; i < old.n; i++) await this.store.del(`${SNAP}.${old.gen}.${i}`).catch(() => {});
    return m;
  }
}

// The tunnel connector (TUNNEL_CMD, a shell command): run while this copy is the leader, started again if it exits.
export class Tunnel {
  constructor(cmd, log) {
    this.cmd = cmd;
    this.log = log;
    this.proc = null;
    this.wanted = false;
  }

  start() {
    if (!this.cmd || this.wanted) return;
    this.wanted = true;
    const run = () => {
      if (!this.wanted) return;
      this.proc = spawn("sh", ["-c", this.cmd], { stdio: ["ignore", "inherit", "inherit"], detached: true });
      this.log(`tunnel connector started (pid ${this.proc.pid})`);
      this.proc.on("exit", (code) => {
        this.proc = null;
        if (this.wanted) {
          this.log(`tunnel connector exited (${code}); starting it again in 5 s`);
          setTimeout(run, 5_000);
        }
      });
    };
    run();
  }

  stop() {
    this.wanted = false;
    if (this.proc) {
      try {
        process.kill(-this.proc.pid, "SIGTERM"); // its process group: the shell and the connector
      } catch {}
      this.proc = null;
    }
  }
}
