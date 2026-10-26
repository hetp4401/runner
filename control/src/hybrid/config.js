import { CONFIG_TABLES, rowKey } from "./store.js";
import { createHash } from "node:crypto";

const bytes = new TextEncoder();
const text = new TextDecoder();
const to64 = (array) => { let out = ""; for (const x of array) out += String.fromCharCode(x); return btoa(out); };
const from64 = (value) => Uint8Array.from(atob(value), (x) => x.charCodeAt(0));
const fingerprint = (row) => createHash("sha256").update(JSON.stringify(row, Object.keys(row).sort())).digest("hex");
export const CREDENTIAL_KEYS = ["JOIN_TOKEN", "ADMIN_PASSWORD", "MAC_TOKEN", "STATE_SIGNING_KEY", "CF_API_TOKEN", "CF_API_KEY", "CF_API_EMAIL"];

export class Configuration {
  constructor(db, key) {
    this.db = db;
    this.key = crypto.subtle.importKey("raw", from64(key), "AES-GCM", false, ["encrypt", "decrypt"]);
    this.known = new Map(CONFIG_TABLES.map((table) => [table, new Map()]));
    this.credentials = new Map();
    this.reads = 0;
    this.writes = 0;
  }
  async seal(table, id, row) {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const encrypted = await crypto.subtle.encrypt({ name: "AES-GCM", iv, additionalData: bytes.encode(`${table}:${id}`) }, await this.key, bytes.encode(JSON.stringify(row)));
    return JSON.stringify({ v: 1, iv: to64(iv), data: to64(new Uint8Array(encrypted)) });
  }
  async open(table, id, payload) {
    const value = JSON.parse(payload);
    if (value.v !== 1) throw new Error("unsupported configuration encryption version");
    const raw = await crypto.subtle.decrypt({ name: "AES-GCM", iv: from64(value.iv), additionalData: bytes.encode(`${table}:${id}`) }, await this.key, from64(value.data));
    return JSON.parse(text.decode(raw));
  }
  async load() {
    // Source bundles need small pages. Credentials and specs are much smaller;
    // larger pages keep a 100-machine fleet below the free plan's 50 queries per
    // invocation. Rowid scans avoid repeatedly sorting/scanning a bucket index.
    const tables = Object.fromEntries(CONFIG_TABLES.map((table) => [table, []]));
    for (const [filter, size] of [["bucket != 'versions'", 100], ["bucket = 'versions'", 10]]) {
      let cursor = 0;
      for (;;) {
        const res = await this.db.prepare(`SELECT rowid AS cursor,bucket,id,payload FROM runner_config NOT INDEXED WHERE rowid>? AND ${filter} ORDER BY rowid LIMIT ${size}`).bind(cursor).all();
        this.reads += res.meta?.rows_read ?? res.results.length;
        for (const row of res.results) {
          const data = await this.open(row.bucket, row.id, row.payload);
          cursor = row.cursor;
          if (row.bucket === "credentials") {
            if (!CREDENTIAL_KEYS.includes(data.key)) throw new Error("unknown controller credential");
            this.credentials.set(data.key, data.value);
            continue;
          }
          if (!tables[row.bucket]) throw new Error("unknown configuration bucket");
          tables[row.bucket].push(data);
          this.known.get(row.bucket).set(row.id, fingerprint(data));
        }
        if (res.results.length < size) break;
      }
    }
    return tables;
  }
  async setCredentials(input) {
    const updates = Object.entries(input).filter(([key, value]) => CREDENTIAL_KEYS.includes(key) && typeof value === "string" && value && this.credentials.get(key) !== value);
    if (!updates.length) return;
    const statements = [];
    for (const [key, value] of updates) {
      const id = JSON.stringify([key]);
      statements.push(this.db.prepare("INSERT INTO runner_config(bucket,id,payload,updated) VALUES(?,?,?,?) ON CONFLICT(bucket,id) DO UPDATE SET payload=excluded.payload,updated=excluded.updated")
        .bind("credentials", id, await this.seal("credentials", id, { key, value }), Date.now()));
    }
    const results = await this.db.batch(statements);
    if (results.some((r) => r.success === false)) throw new Error("D1 credential commit failed");
    for (const result of results) this.writes += result.meta?.rows_written ?? 1;
    for (const [key, value] of updates) this.credentials.set(key, value);
  }
  async flush(store) {
    const statements = [];
    const next = new Map();
    for (const table of store.dirty) {
      const before = this.known.get(table);
      if (!before) continue;
      const rows = store.configuration(table);
      const after = new Map(rows.map((row) => [rowKey(table, row), fingerprint(row)]));
      next.set(table, after);
      for (const row of rows) {
        const id = rowKey(table, row);
        if (after.get(id) === before.get(id)) continue;
        const sealed = await this.seal(table, id, row);
        statements.push(this.db.prepare("INSERT INTO runner_config(bucket,id,payload,updated) VALUES(?,?,?,?) ON CONFLICT(bucket,id) DO UPDATE SET payload=excluded.payload,updated=excluded.updated").bind(table, id, sealed, Date.now()));
      }
      for (const id of before.keys()) if (!after.has(id)) statements.push(this.db.prepare("DELETE FROM runner_config WHERE bucket=? AND id=?").bind(table, id));
    }
    if (statements.length) {
      // One atomic D1 batch: acknowledge configuration changes only after commit.
      const results = await this.db.batch(statements);
      for (const result of results) {
        if (result.success === false) throw new Error("D1 configuration commit failed");
        this.writes += result.meta?.rows_written ?? 1;
      }
    }
    for (const [table, rows] of next) this.known.set(table, rows);
    store.dirty.clear();
  }
}
