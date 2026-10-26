// SQLite is a disposable working copy. Only the explicitly selected configuration
// rows below are written to D1; runs, health, placements and metrics never are.
export const CONFIG_TABLES = ["projects", "versions", "settings", "app_passwords", "app_env", "slots"];
const KEYS = {
  projects: ["name"], versions: ["name", "version"], settings: ["key"],
  app_passwords: ["name"], app_env: ["name", "key"], slots: ["n"],
};
export const durableSetting = (key) => ["pools", "rebalance", "roll"].includes(key) || /^roll_\d+$/.test(key);
export const rowKey = (table, row) => JSON.stringify(KEYS[table].map((key) => row[key]));
export const isConfiguration = (table, row) => CONFIG_TABLES.includes(table) && (table !== "settings" || durableSetting(row.key));

export class WorkingStore {
  constructor(run) {
    this.run = run;
    this.journal = [];
    this.dirty = new Set();
    this.tracking = true;
  }
  exec(query, ...args) {
    const rows = this.run(query, args);
    if (this.tracking && /^(?:INSERT|UPDATE|DELETE|ALTER|CREATE|DROP)\b/i.test(query.trim())) {
      this.journal.push([query, args]);
      const table = query.match(/^\s*(?:INSERT(?: OR \w+)? INTO|UPDATE|DELETE FROM)\s+(\w+)/i)?.[1];
      if (CONFIG_TABLES.includes(table)) this.dirty.add(table);
    }
    return { toArray: () => rows };
  }
  rows(table) {
    if (!/^[a-z][a-z0-9_]*$/.test(table)) throw new Error("invalid table");
    return this.exec(`SELECT * FROM ${table}`).toArray();
  }
  configuration(table) { return this.rows(table).filter((row) => isConfiguration(table, row)); }
  tables() {
    return this.exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'").toArray().map((x) => x.name);
  }
  snapshot() { return Object.fromEntries(this.tables().map((table) => [table, this.rows(table)])); }
  insert(table, row) {
    const allowed = new Set(this.exec(`PRAGMA table_info(${table})`).toArray().map((x) => x.name));
    const columns = Object.keys(row).filter((key) => allowed.has(key));
    if (!columns.length) return;
    this.exec(`INSERT OR REPLACE INTO ${table} (${columns.join(",")}) VALUES (${columns.map(() => "?").join(",")})`, ...columns.map((key) => row[key]));
  }
  load(tables, { replace = true } = {}) {
    const known = new Set(this.tables());
    const before = this.tracking;
    this.tracking = false;
    try {
      for (const [table, rows] of Object.entries(tables)) {
        if (!known.has(table) || !Array.isArray(rows)) throw new Error(`invalid snapshot table ${table}`);
        if (replace) this.exec(`DELETE FROM ${table}`);
        for (const row of rows) this.insert(table, row);
      }
    } finally { this.tracking = before; }
  }
  replay(journal, { track = false } = {}) {
    const before = this.tracking;
    this.tracking = track;
    try { for (const [query, args] of journal) this.exec(query, ...args); }
    finally { this.tracking = before; }
  }
  takeJournal() { const out = this.journal; this.journal = []; return out; }
}

// sql.js keeps the Worker working copy in WebAssembly memory, not DO storage.
export function wasmStore(SQL) {
  const db = new SQL.Database();
  const store = new WorkingStore((query, args) => {
    const stmt = db.prepare(query);
    try {
      stmt.bind(args);
      const rows = [];
      while (stmt.step()) rows.push(stmt.getAsObject());
      return rows;
    } finally { stmt.free(); }
  });
  store.close = () => db.close();
  return store;
}
