import { parentPort, workerData } from "node:worker_threads";

parentPort.on("message", async ({ batch, shared }) => {
  const state = new Int32Array(shared, 0, 2), destination = new Uint8Array(shared, 8);
  let output, status = 1;
  try {
    const response = await fetch(workerData.url, {
      method: "POST", signal: AbortSignal.timeout(30_000),
      headers: { "content-type": "application/json", "x-auth-key": workerData.key, "x-auth-email": workerData.email },
      body: JSON.stringify({ batch }),
    });
    let body;
    try { body = await response.json(); } catch { throw new Error(`D1 returned an unreadable response (HTTP ${response.status})`); }
    if (!response.ok || !body.success || !Array.isArray(body.result) || body.result.some((item) => !item.success)) {
      // Error text can contain bound secrets. Only expose HTTP status and codes.
      const codes = (body.errors || []).map((e) => Number(e.code)).filter(Number.isFinite).join(",");
      throw new Error(`D1 request failed (HTTP ${response.status}${codes ? `; codes ${codes}` : ""})`);
    }
    output = body.result;
  } catch (error) {
    status = 2;
    output = { error: error.message?.startsWith("D1 ") ? error.message : "D1 network request failed; configuration was not acknowledged" };
  }
  let bytes = new TextEncoder().encode(JSON.stringify(output));
  if (bytes.length > destination.length) {
    status = 2;
    bytes = new TextEncoder().encode(JSON.stringify({ error: "D1 response exceeded the working buffer; use smaller pages" }));
  }
  destination.set(bytes);
  Atomics.store(state, 1, bytes.length);
  Atomics.store(state, 0, status);
  Atomics.notify(state, 0);
});
