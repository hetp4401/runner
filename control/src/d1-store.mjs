import { createHash, randomUUID } from "node:crypto";
import { LocalStore } from "./local-store.mjs";
import { D1Client } from "./d1-client.mjs";
import { CONFIG_TABLES, durableSetting } from "./state-policy.js";

export const TABLES = {
  projects: { columns: ["name", "version", "stable", "rollout", "halted", "updated", "enabled"], keys: ["name"], schema: "name TEXT PRIMARY KEY, version INTEGER NOT NULL, stable INTEGER, rollout INTEGER NOT NULL, halted TEXT, updated INTEGER NOT NULL, enabled INTEGER NOT NULL DEFAULT 1" },
  versions: { columns: ["name", "version", "compose", "port", "created", "files", "replicas"], keys: ["name", "version"], schema: "name TEXT NOT NULL, version INTEGER NOT NULL, compose TEXT NOT NULL, port INTEGER, created INTEGER NOT NULL, files TEXT, replicas INTEGER, PRIMARY KEY(name,version)" },
  settings: { columns: ["key", "value"], keys: ["key"], schema: "key TEXT PRIMARY KEY, value TEXT NOT NULL" },
  app_passwords: { columns: ["name", "salt", "hash", "updated"], keys: ["name"], schema: "name TEXT PRIMARY KEY, salt TEXT NOT NULL, hash TEXT NOT NULL, updated INTEGER NOT NULL" },
  app_env: { columns: ["name", "key", "value", "updated"], keys: ["name", "key"], schema: "name TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, updated INTEGER NOT NULL, PRIMARY KEY(name,key)" },
};
export const SCHEMA = CONFIG_TABLES.map((table) => ({ sql: `CREATE TABLE IF NOT EXISTS ${table} (${TABLES[table].schema})`, params: [] }));
export const META_SCHEMA = "CREATE TABLE IF NOT EXISTS runner_meta (id INTEGER PRIMARY KEY CHECK(id=1), schema_version INTEGER NOT NULL, origin TEXT NOT NULL, ready INTEGER NOT NULL, revision INTEGER NOT NULL, commit_id TEXT NOT NULL)";
export const values = (table, row) => TABLES[table].columns.map((column) => row[column] ?? null);
export const rowHash = (table, row) => createHash("sha256").update(JSON.stringify(values(table, row))).digest("hex");
export const upsert = (table, row) => {
  const { columns, keys } = TABLES[table];
  return { sql: `INSERT INTO ${table} (${columns.join(",")}) VALUES (${columns.map(() => "?").join(",")}) ON CONFLICT (${keys.join(",")}) DO UPDATE SET ${columns.filter((c) => !keys.includes(c)).map((c) => `${c}=excluded.${c}`).join(",")}`, params: values(table, row) };
};
export function metadata(client) {
  const row = client.query("SELECT * FROM runner_meta WHERE id=1")[0].results[0];
  if (!row || row.schema_version !== 1) throw new Error("D1 has no supported fleet configuration; run the migration explicitly");
  return row;
}
export function readTable(client, table, consume) {
  const { columns, keys } = TABLES[table];
  let last;
  for (;;) {
    const where = last ? ` WHERE (${keys.join(",")}) > (${keys.map(() => "?").join(",")})` : "";
    const rows = client.query([{ sql: `SELECT ${columns.join(",")} FROM ${table}${where} ORDER BY ${keys.join(",")} LIMIT 10`, params: last || [] }])[0].results;
    if (!rows.length) break;
    consume(rows);
    if (rows.length < 10) break;
    last = keys.map((key) => rows.at(-1)[key]);
  }
}

export class D1Store extends LocalStore {
  constructor(env, options = {}) {
    super(":memory:", { initialize: true, onFailure: options.onFailure });
    this.known = new Map();
    this.dirty = new Set();
    this.commits = 0;
    this.origin = env.CONTROL_HOST;
    try {
      this.client = options.client || new D1Client(env);
      const meta = metadata(this.client);
      if (meta.ready !== 1 || meta.origin !== this.origin) throw new Error("D1 fleet is not ready or belongs to another controller");
      this.revision = meta.revision;
      this.db.exec(SCHEMA.map((s) => s.sql).join(";"));
      for (const table of CONFIG_TABLES) {
        const insert = this.db.prepare(upsert(table, {}).sql);
        readTable(this.client, table, (rows) => { for (const row of rows) insert.run(...values(table, row)); });
        if (table === "settings" && this.db.prepare("SELECT key FROM settings").all().some((r) => !durableSetting(r.key))) throw new Error("D1 contains non-configuration settings; migration required");
        this.known.set(table, this.fingerprints(table));
      }
      const after = metadata(this.client);
      if (after.revision !== meta.revision || after.commit_id !== meta.commit_id || after.ready !== 1 || after.origin !== meta.origin) throw new Error("D1 configuration changed while loading; retry startup");
      this.validate();
      this.onConfigChange = (table) => this.dirty.add(table);
      this.beforeCommit = () => this.commit();
    } catch (error) { this.close(); throw error; }
  }
  fingerprints(table, rows = this.db.prepare(`SELECT * FROM main.${table}`).all()) {
    return new Map(rows.map((row) => {
      const keys = TABLES[table].keys.map((key) => row[key]);
      return [JSON.stringify(keys), { keys, hash: rowHash(table, row) }];
    }));
  }
  commit() {
    if (!this.dirty.size) return;
    const batch = [], next = new Map();
    for (const table of this.dirty) {
      const rows = this.db.prepare(`SELECT * FROM main.${table}`).all();
      const before = this.known.get(table), after = this.fingerprints(table, rows);
      next.set(table, after);
      for (const row of rows) {
        const key = JSON.stringify(TABLES[table].keys.map((k) => row[k]));
        if (before.get(key)?.hash !== after.get(key).hash) batch.push(upsert(table, row));
      }
      for (const [key, entry] of before) if (!after.has(key)) batch.push({ sql: `DELETE FROM ${table} WHERE ${TABLES[table].keys.map((k) => `${k}=?`).join(" AND ")}`, params: entry.keys });
    }
    if (batch.length) {
      const id = randomUUID();
      // REST batches are atomic. A stale revision rejects the entire edit.
      batch.unshift({ sql: "SELECT CASE WHEN EXISTS (SELECT 1 FROM runner_meta WHERE id=1 AND ready=1 AND origin=? AND revision=?) THEN 1 ELSE json('configuration revision conflict') END AS allowed", params: [this.origin, this.revision] });
      batch.push({ sql: "UPDATE runner_meta SET revision=revision+1,commit_id=? WHERE id=1", params: [id] });
      try { this.client.query(batch, { verifyLeadership: true }); }
      catch (error) {
        // A response may be lost after commit. Confirm its unique ID; never
        // replay an edit that increments a version or changes a password.
        let confirmed;
        try { confirmed = metadata(this.client); } catch {}
        if (confirmed?.commit_id !== id || confirmed.revision !== this.revision + 1 || confirmed.origin !== this.origin) throw error;
      }
      this.revision++;
      this.commits++;
    }
    for (const [table, map] of next) this.known.set(table, map);
    this.dirty.clear();
  }
  diagnostics() { return { revision: this.revision, commits: this.commits, ...this.client.stats }; }
  close() { this.client?.close(); super.close(); }
}
