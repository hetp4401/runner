// Offline, one-way migration. Stop the controller first. Reads the original
// database in one transaction; creates no export files or local database copies.
import { DatabaseSync } from "node:sqlite";
import { createHash, randomUUID } from "node:crypto";
import { D1Client } from "./src/d1-client.mjs";
import { TABLES, SCHEMA, META_SCHEMA, values, upsert, readTable, metadata } from "./src/d1-store.mjs";
import { CONFIG_TABLES, durableSetting } from "./src/state-policy.js";

const filename = process.argv[2];
if (!filename || !process.env.CONTROL_HOST) throw new Error("Usage: node d1-migrate.mjs /data/control.db [--verify]; set CONTROL_HOST and D1 credentials in the environment");
const verifyOnly = process.argv.includes("--verify");
const source = new DatabaseSync(filename, { readOnly: true });
const client = new D1Client(process.env);
const origin = process.env.CONTROL_HOST;
const digest = (table, rows) => {
  const hash = createHash("sha256");
  for (const row of rows) hash.update(JSON.stringify(values(table, row)) + "\n");
  return hash.digest("hex");
};
try {
  source.exec("BEGIN");
  if (source.prepare("PRAGMA quick_check").get().quick_check !== "ok") throw new Error("source database integrity check failed");
  const missing = source.prepare("SELECT p.name FROM projects p LEFT JOIN versions v ON v.name=p.name AND v.version=p.version LEFT JOIN versions s ON s.name=p.name AND s.version=p.stable WHERE v.name IS NULL OR (p.stable IS NOT NULL AND s.name IS NULL) LIMIT 1").get();
  if (missing) throw new Error("source database has incomplete application versions");
  if (!verifyOnly) {
    client.query([...SCHEMA, { sql: META_SCHEMA, params: [] }]);
    const existing = client.query("SELECT * FROM runner_meta WHERE id=1")[0].results[0];
    if (existing && (existing.ready !== 0 || existing.revision !== 0 || existing.origin !== origin || existing.schema_version !== 1)) throw new Error("destination already contains an active or different fleet; refusing to overwrite it");
    if (!existing) {
      for (const table of CONFIG_TABLES) if (client.query(`SELECT COUNT(*) AS n FROM ${table}`)[0].results[0].n) throw new Error("destination has unowned configuration rows; refusing to overwrite it");
      client.query([{ sql: "INSERT INTO runner_meta VALUES (1,1,?,0,0,?)", params: [origin, randomUUID()] }]);
    }
    client.query(CONFIG_TABLES.map((table) => ({ sql: `DELETE FROM ${table}`, params: [] })));
  } else if (metadata(client).origin !== origin) throw new Error("destination belongs to another controller");
  const report = {};
  for (const table of CONFIG_TABLES) {
    const { columns, keys } = TABLES[table];
    let rows = source.prepare(`SELECT ${columns.join(",")} FROM ${table} ORDER BY ${keys.join(",")}`).all();
    if (table === "settings") rows = rows.filter((row) => durableSetting(row.key));
    if (!verifyOnly) for (let i = 0; i < rows.length; i += 10) client.query(rows.slice(i, i + 10).map((row) => upsert(table, row)));
    const remote = [];
    readTable(client, table, (page) => remote.push(...page));
    const hash = digest(table, rows);
    if (rows.length !== remote.length || hash !== digest(table, remote)) throw new Error(`D1 verification failed for ${table}; fleet remains inactive`);
    report[table] = { rows: rows.length, sha256: hash };
    console.log(`Verified ${table}: ${rows.length} rows`);
  }
  if (!verifyOnly) client.query([{ sql: "UPDATE runner_meta SET ready=1,commit_id=? WHERE id=1 AND ready=0 AND revision=0", params: [randomUUID()] }]);
  console.log(JSON.stringify({ verified: true, report, usage: client.stats }));
} finally {
  source.close();
  client.close();
}
