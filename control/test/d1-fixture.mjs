import http from "node:http";
import { DatabaseSync } from "node:sqlite";
import { SCHEMA, META_SCHEMA } from "../src/d1-store.mjs";
const db = new DatabaseSync(":memory:");
db.exec([...SCHEMA.map((s) => s.sql), META_SCHEMA, "INSERT INTO runner_meta VALUES(1,1,'runners.test.example',1,0,'initial')"].join(";"));
let failAfterCommit = false;
const server = http.createServer(async (req, res) => {
  if (req.url === "/lose-response") { failAfterCommit = true; res.end("ok"); return; }
  const chunks = []; for await (const chunk of req) chunks.push(chunk);
  if (req.headers["x-auth-key"] !== "test-global-key" || req.headers["x-auth-email"] !== "test@example.com") { res.writeHead(401); res.end('{}'); return; }
  try {
    const { batch } = JSON.parse(Buffer.concat(chunks));
    db.exec("BEGIN");
    const result = batch.map(({ sql, params = [] }) => {
      const before = db.prepare("SELECT total_changes() AS n").get().n;
      const results = db.prepare(sql).all(...params);
      return { success: true, results, meta: { rows_read: results.length, rows_written: db.prepare("SELECT total_changes() AS n").get().n - before } };
    });
    db.exec("COMMIT");
    if (failAfterCommit && batch.some((s) => s.sql.startsWith("UPDATE runner_meta SET revision"))) {
      failAfterCommit = false; res.writeHead(502); res.end('{"success":false}'); return;
    }
    res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ success: true, result }));
  } catch {
    try { db.exec("ROLLBACK"); } catch {}
    res.writeHead(400); res.end(JSON.stringify({ success: false, errors: [{ code: 7500, message: "private SQL must not reach logs" }] }));
  }
});
server.listen(0, "127.0.0.1", () => process.send({ port: server.address().port }));
