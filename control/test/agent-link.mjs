import assert from "node:assert/strict";
import { WebSocketServer } from "ws";
import { ControlLink } from "../../agent/control-link.mjs";
const server = new WebSocketServer({ port: 0, perMessageDeflate: false });
await new Promise((resolve) => server.on("listening", resolve));
let woke = 0, mode = "large";
const link = new ControlLink(`http://127.0.0.1:${server.address().port}`, "test", () => { woke++; });
server.on("connection", (socket, request) => {
  assert.equal(request.headers.authorization, "Bearer test");
  socket.on("message", (raw) => {
    const message = JSON.parse(raw);
    if (mode === "invalid") return socket.send("null");
    if (mode === "missing-plan") return socket.send(JSON.stringify({ type: "plan", id: message.id }));
    if (mode === "extension") {
      socket.send(JSON.stringify({ type: "information", id: message.id }));
      return socket.send(JSON.stringify({ type: "plan", id: message.id, plan: { desired: {} } }));
    }
    assert.equal(message.body.large.length, 300_000);
    const response = JSON.stringify({ type: "plan", id: message.id, plan: { desired: { large: "x".repeat(300_000) } } });
    socket.send(response.slice(0, 1000), { fin: false });
    socket.ping("ping between fragments");
    socket.send(response.slice(1000), { fin: true });
    socket.send(JSON.stringify({ type: "wake" }));
  });
});
try {
  const plan = await link.report({ large: "x".repeat(300_000) });
  assert.equal(plan.desired.large.length, 300_000);
  assert.equal(woke, 1);
  console.log("ok large agent inventory, fragmented response, ping and wake");
  const bounded = async promise => {
    let timer;
    try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("reply remained pending")), 2000); })]); }
    finally { clearTimeout(timer); }
  };
  mode = "extension";
  assert.deepEqual(await bounded(link.report({})), { desired: {} });
  assert.equal(link.pending.size, 0);
  console.log("ok unknown extension replies cannot consume an agent's pending plan");
  for (mode of ["invalid", "missing-plan"]) {
    await assert.rejects(bounded(link.report({})), /connection closed/);
    assert.equal(link.pending.size, 0);
  }
  console.log("ok malformed and missing plans close cleanly instead of hanging or crashing");
  mode = "extension";
  assert.deepEqual(await bounded(link.report({})), { desired: {} });
  await assert.rejects(link.report({ invalid: 1n }), /BigInt/);
  assert.equal(link.pending.size, 0);
  console.log("ok reconnect succeeds and unsendable reports leave no pending timers");
} finally { link.close(); server.close(); }
