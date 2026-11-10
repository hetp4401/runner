import { Worker } from "node:worker_threads";

// Scheduler SQL is synchronous. A separate I/O worker makes network progress
// while a config edit waits. Heartbeats and ordinary reads never call D1.
export class D1Client {
  constructor(env) {
    const account = env.D1_ACCOUNT_ID || env.ACCOUNT_ID, database = env.D1_DATABASE_ID;
    const key = env.D1_API_KEY || env.CF_API_KEY, email = env.D1_API_EMAIL || env.CF_API_EMAIL;
    if (!account || !database || !key || !email) throw new Error("D1 requires ACCOUNT_ID, D1_DATABASE_ID, D1_API_KEY and D1_API_EMAIL");
    this.shared = new SharedArrayBuffer(32 * 1024 * 1024);
    this.state = new Int32Array(this.shared, 0, 2);
    this.bytes = new Uint8Array(this.shared, 8);
    this.worker = new Worker(new URL("./d1-worker.mjs", import.meta.url), { workerData: {
      url: `${env.D1_API_BASE || "https://api.cloudflare.com/client/v4"}/accounts/${encodeURIComponent(account)}/d1/database/${encodeURIComponent(database)}/query`, key, email,
    } });
    this.worker.on("error", () => { this.broken = true; });
    this.worker.unref();
    this.stats = { requests: 0, rowsRead: 0, rowsWritten: 0 };
  }
  query(statements) {
    if (this.broken) throw new Error("D1 connection is unavailable");
    const batch = typeof statements === "string" ? [{ sql: statements, params: [] }] : statements;
    Atomics.store(this.state, 0, 0);
    this.worker.postMessage({ batch, shared: this.shared });
    this.stats.requests++;
    if (Atomics.wait(this.state, 0, 0, 35_000) === "timed-out") {
      this.broken = true;
      this.worker.terminate();
      throw new Error("D1 request timed out; configuration outcome must be checked on restart");
    }
    const result = JSON.parse(new TextDecoder().decode(this.bytes.subarray(0, Atomics.load(this.state, 1))));
    if (Atomics.load(this.state, 0) !== 1) throw new Error(result.error);
    for (const item of result) {
      this.stats.rowsRead += item.meta?.rows_read || 0;
      this.stats.rowsWritten += item.meta?.rows_written || 0;
    }
    return result;
  }
  close() { this.broken = true; this.worker.terminate(); }
}
