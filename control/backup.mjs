// Run inside the controller container: node backup.mjs /tmp/unique-backup
// SQLite's backup API includes committed WAL pages while the server is running.
import { DatabaseSync, backup } from "node:sqlite";
import { mkdirSync, copyFileSync, readFileSync, writeFileSync, chmodSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";
import { createHash } from "node:crypto";

process.umask(0o077);
const destination = process.argv[2];
if (!destination) throw new Error("supply a new backup directory");
const directory = resolve(destination), data = process.env.DATA_DIR || "/data";
mkdirSync(directory, { mode: 0o700 }); // Refuse to replace an existing backup.
let source;
try {
  source = new DatabaseSync(join(data, "control.db"), { readOnly: true });
  const database = join(directory, "control.db");
  await backup(source, database);
  const snapshot = new DatabaseSync(database, { readOnly: true });
  let projects;
  try {
    if (Object.values(snapshot.prepare("PRAGMA quick_check").get())[0] !== "ok") throw new Error("backup integrity check failed");
    projects = snapshot.prepare("SELECT name,version,enabled FROM projects ORDER BY name").all();
  } finally { snapshot.close(); }
  copyFileSync(join(data, "identity.json"), join(directory, "identity.json"));
  const files = {};
  for (const name of ["control.db", "identity.json"]) {
    chmodSync(join(directory, name), 0o600);
    files[name] = createHash("sha256").update(readFileSync(join(directory, name))).digest("hex");
  }
  writeFileSync(join(directory, "manifest.json"), JSON.stringify({ schema: 1,
    created_at: new Date().toISOString(), source_revision: process.env.SOURCE_REVISION || null,
    projects, files }, null, 2) + "\n", { mode: 0o600 });
  console.log(JSON.stringify({ backup: directory, projects: projects.length }));
} catch (error) {
  rmSync(directory, { recursive: true, force: true });
  throw error;
} finally { source?.close(); }
