// A small WebSocket client (RFC 6455) with no dependencies, for the agent's outgoing streams (live logs): it connects
// with any headers (the join token goes in one), sends text, answers pings, and reports its end once, whichever side
// ends it. The agent only sends; what the control plane sends back besides pings and the close is ignored.
import { createHash, randomBytes } from "node:crypto";
import http from "node:http";
import https from "node:https";

const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

// Resolves { send(text) -> false when the socket is backed up, close(), onEnd(fn), open, socket } once connected.
export function connect(url, headers = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const key = randomBytes(16).toString("base64");
    const req = (u.protocol === "https:" ? https : http).request({
      host: u.hostname,
      port: u.port || (u.protocol === "https:" ? 443 : 80),
      path: `${u.pathname}${u.search}`,
      headers: { ...headers, connection: "Upgrade", upgrade: "websocket", "sec-websocket-version": "13", "sec-websocket-key": key },
      timeout: 15_000,
    });
    req.on("upgrade", (res, socket, head) => {
      if (res.headers["sec-websocket-accept"] !== createHash("sha1").update(key + GUID).digest("base64")) {
        socket.destroy();
        return reject(new Error("not a WebSocket answer"));
      }
      socket.setNoDelay(true);
      socket.setTimeout(0);
      let open = true;
      const ends = [];
      const end = () => {
        if (!open) return;
        open = false;
        socket.destroy();
        for (const fn of ends) fn();
      };
      // A frame from the client: final, masked.
      const write = (op, data) => {
        if (!open) return false;
        const mask = randomBytes(4);
        const n = data.length;
        let head;
        if (n < 126) head = Buffer.from([0x80 | op, 0x80 | n]);
        else if (n < 65536) head = Buffer.from([0x80 | op, 0x80 | 126, n >> 8, n & 255]);
        else {
          head = Buffer.alloc(10);
          head[0] = 0x80 | op;
          head[1] = 0x80 | 127;
          head.writeBigUInt64BE(BigInt(n), 2);
        }
        const body = Buffer.from(data);
        for (let i = 0; i < body.length; i++) body[i] ^= mask[i & 3];
        return socket.write(Buffer.concat([head, mask, body]));
      };
      let buf = Buffer.from(head ?? []);
      const read = () => {
        while (buf.length >= 2) {
          const op = buf[0] & 15;
          let n = buf[1] & 127;
          let at = 2;
          if (n === 126) {
            if (buf.length < 4) return;
            n = buf.readUInt16BE(2);
            at = 4;
          } else if (n === 127) {
            if (buf.length < 10) return;
            n = Number(buf.readBigUInt64BE(2));
            at = 10;
          }
          if (buf[1] & 0x80) at += 4; // a server doesn't mask; skip a mask if one came anyway
          if (buf.length < at + n) return;
          const data = buf.subarray(at, at + n);
          buf = buf.subarray(at + n);
          if (op === 8) { // close: answer it, then go
            write(8, Buffer.alloc(0));
            return end();
          }
          if (op === 9) write(10, data); // ping: pong
        }
      };
      socket.on("data", (d) => {
        buf = Buffer.concat([buf, d]);
        read();
      });
      socket.on("close", end);
      socket.on("end", end);
      socket.on("error", end);
      read();
      resolve({
        send: (text) => write(1, Buffer.from(String(text))),
        close() {
          if (!open) return;
          write(8, Buffer.from([0x03, 0xe8])); // 1000: normal
          end();
        },
        onEnd: (fn) => (open ? ends.push(fn) : fn()),
        get open() { return open; },
        socket,
      });
    });
    req.on("response", (res) => {
      res.resume();
      reject(new Error(`HTTP ${res.statusCode}`));
    });
    req.on("timeout", () => req.destroy(new Error("timed out")));
    req.on("error", reject);
    req.end();
  });
}
