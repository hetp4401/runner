import assert from "node:assert/strict";
import { WebSocketServer } from "ws";
import { ControlLink } from "../../agent/control-link.mjs";
const server = new WebSocketServer({ port: 0, perMessageDeflate: false });
await new Promise((resolve) => server.on("listening", resolve));
let woke = 0;
const link = new ControlLink(`http://127.0.0.1:${server.address().port}`, "test", () => { woke++; });
server.on("connection", (socket, request) => {
  assert.equal(request.headers.authorization, "Bearer test");
  socket.on("message", (raw) => {
    const message = JSON.parse(raw);
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
} finally { link.close(); server.close(); }
