import { DatabaseSync } from "node:sqlite";
import { chmodSync, existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { randomUUID } from "node:crypto";
import { CONFIG_TABLES, durableSetting } from "./state-policy.js";

const RUNTIME_TABLES = new Set(["runs", "copies", "starts", "agents", "events", "geo", "metrics_1m", "metrics_10m", "metrics_1h", "slots"]);

// One SQLite connection, two storage lifetimes. Only configuration lives in main
// (control.db). TEMP tables and their indexes live entirely in RAM. SQL used by
// the scheduler stays the same, including receipts, metrics and recovery queries.
export class LocalStore {
  constructor(filename, { initialize = false, onFailure = () => {} } = {}) {
    this.filename = filename;
    this.onFailure = onFailure;
    this.settingsReady = false;
    this.persistedSettings = new Map();
    this.sequence = 0;
    if (!existsSync(filename) && !initialize) {
      throw new Error("control.db is missing; restore the configuration backup (INIT_CONTROL_DB=1 is only for a new fleet)");
    }
    mkdirSync(dirname(filename), { recursive: true, mode: 0o700 });
    this.db = new DatabaseSync(filename);
    chmodSync(filename, 0o600);
    try {
      const tables = this.db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all().map((r) => r.name);
      if (tables.length || !initialize) {
        for (const name of CONFIG_TABLES) if (!tables.includes(name)) throw new Error(`configuration table ${name} is missing; refusing to start an empty fleet`);
        this.validate();
      }
      if (this.db.prepare("PRAGMA quick_check").get().quick_check !== "ok") throw new Error("configuration integrity check failed");
      this.db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA temp_store=MEMORY");
      const obsolete = tables.filter((name) => RUNTIME_TABLES.has(name) || name === "placements");
      const volatile = tables.includes("settings") ? this.db.prepare("SELECT key FROM main.settings").all().filter((r) => !durableSetting(r.key)) : [];
      if (obsolete.length || volatile.length) {
        const directory = join(dirname(filename), "backups");
        mkdirSync(directory, { recursive: true, mode: 0o700 });
        this.backupPath = join(directory, `before-memory-${new Date().toISOString().replaceAll(":", "-")}-${randomUUID()}.db`);
        this.db.prepare("VACUUM main INTO ?").run(this.backupPath);
        chmodSync(this.backupPath, 0o600);
        this.atomic(() => {
          for (const name of obsolete) this.db.exec(`DROP TABLE main.${name}`);
          for (const { key } of volatile) this.db.prepare("DELETE FROM main.settings WHERE key=?").run(key);
        });
        // Reclaim the old metric/history pages once, after the full private backup.
        this.db.exec("VACUUM main");
      }
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  validate() {
    const missing = this.db.prepare(`SELECT p.name FROM main.projects p
      LEFT JOIN main.versions v ON v.name=p.name AND v.version=p.version
      LEFT JOIN main.versions s ON s.name=p.name AND s.version=p.stable
      WHERE v.name IS NULL OR (p.stable IS NOT NULL AND s.name IS NULL) LIMIT 1`).get();
    if (missing) throw new Error(`configuration is incomplete for app ${missing.name}; refusing to issue plans`);
    const pools = this.db.prepare("SELECT value FROM main.settings WHERE key='pools'").get();
    if (pools) {
      const value = JSON.parse(pools.value);
      if (!value || typeof value !== "object" || Array.isArray(value) || Object.values(value).some((n) => !Number.isInteger(n) || n < 0)) throw new Error("invalid persisted fleet sizes");
    }
  }

  atomic(fn) {
    const point = `config_${++this.sequence}`;
    const settings = new Map(this.persistedSettings);
    this.db.exec(`SAVEPOINT ${point}`);
    try {
      const result = fn();
      if (result && typeof result.then === "function") throw new Error("configuration transactions must be synchronous");
      this.db.exec(`RELEASE ${point}`);
      return result;
    } catch (error) {
      this.db.exec(`ROLLBACK TO ${point}; RELEASE ${point}`);
      this.persistedSettings = settings;
      throw error;
    }
  }

  syncSettings() {
    const after = new Map(this.db.prepare("SELECT key,value FROM temp.settings").all().filter((r) => durableSetting(r.key)).map((r) => [r.key, r.value]));
    for (const [key, value] of after) if (this.persistedSettings.get(key) !== value) {
      this.db.prepare("INSERT INTO main.settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(key, value);
    }
    for (const key of this.persistedSettings.keys()) if (!after.has(key)) this.db.prepare("DELETE FROM main.settings WHERE key=?").run(key);
    this.persistedSettings = after;
  }

  protect(control) {
    this.validate();
    // Parse referenced source bundles before this process accepts any agents.
    for (const p of control.projects.values()) {
      control.version(p.name, p.version);
      if (p.stable != null) control.version(p.name, p.stable);
    }
    // These operations are synchronous. Never keep a transaction open across a
    // Cloudflare request or an HTTP body read, where another agent may check in.
    for (const method of ["putProject", "putEnv", "deleteProject", "setEnabled", "putSettings", "roll", "cancelRoll", "saveAppPassword"]) {
      const original = control[method].bind(control);
      control[method] = (...args) => this.mutate(() => original(...args));
    }
  }

  mutate(fn) {
    if (this.failed) throw new Error("configuration storage failed; refusing changes");
    const before = this.db.prepare("SELECT total_changes() AS n").get().n;
    try { return this.atomic(fn); }
    catch (error) {
      // SQL rolls back, but the scheduler also holds Maps. Restart if a failed
      // edit changed either store instead of issuing plans from partial state.
      if (this.db.prepare("SELECT total_changes() AS n").get().n !== before && !this.failed) {
        this.failed = true;
        this.onFailure(error);
      }
      throw error;
    }
  }

  exec(query, ...args) {
    if (this.failed) throw new Error("configuration storage failed; restarting before sending more plans");
    try {
      const table = query.match(/^\s*CREATE TABLE IF NOT EXISTS (\w+)/i)?.[1];
      if (table) {
        if (table === "settings") {
          if (!this.settingsReady) {
            this.db.prepare(query).all(...args); // durable settings, before the TEMP table shadows it
            this.db.prepare(query.replace(/CREATE TABLE/i, "CREATE TEMP TABLE")).all(...args);
            this.db.exec("INSERT INTO temp.settings SELECT * FROM main.settings");
            this.persistedSettings = new Map(this.db.prepare("SELECT key,value FROM main.settings").all().map((r) => [r.key, r.value]));
            this.settingsReady = true;
          }
          return { toArray: () => [] };
        }
        if (RUNTIME_TABLES.has(table)) query = query.replace(/CREATE TABLE/i, "CREATE TEMP TABLE");
        else if (!CONFIG_TABLES.includes(table)) throw new Error(`storage lifetime is not defined for ${table}`);
      }
      const changesSettings = /^\s*(?:INSERT(?: OR \w+)? INTO|UPDATE|DELETE FROM)\s+settings\b/i.test(query);
      const run = () => {
        const rows = this.db.prepare(query).all(...args);
        if (changesSettings) this.syncSettings();
        return { toArray: () => rows };
      };
      return changesSettings ? this.atomic(run) : run();
    } catch (error) {
      // No subsequent report may publish a plan based on an uncommitted change.
      this.failed = true;
      this.onFailure(error);
      throw error;
    }
  }

  close() { this.db.close(); }
}
