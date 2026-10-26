import { connect } from "./ws.mjs";

// One bidirectional connection replaces sync polling plus the separate wake loop.
// A 404 permits a gradual rollout against the old controller.
export class ControlLink {
  constructor(url, token, wake) {
    Object.assign(this, { url, token, wake });
    this.pending = new Map();
    this.sequence = 0;
    this.retryAt = 0;
  }
  async socket() {
    if (this.connection?.open) return this.connection;
    if (Date.now() < this.retryAt) return null;
    if (this.connecting) return this.connecting;
    this.connecting = (async () => {
      try {
        const connection = await connect(`${this.url}/api/agent`, { authorization: `Bearer ${this.token}` });
        this.connection = connection;
        connection.onMessage((raw) => {
          let message;
          try { message = JSON.parse(raw); } catch { return connection.close(); }
          if (message.type === "wake") return this.wake();
          const pending = this.pending.get(message.id);
          if (!pending) return;
          this.pending.delete(message.id);
          clearTimeout(pending.timer);
          if (message.type === "error") pending.reject(new Error(message.error || "controller refused report"));
          else if (message.type === "plan") pending.resolve(message.plan);
        });
        connection.onEnd(() => {
          if (this.connection === connection) this.connection = null;
          for (const pending of this.pending.values()) { clearTimeout(pending.timer); pending.reject(new Error("controller connection closed")); }
          this.pending.clear();
        });
        return connection;
      } catch (error) {
        if (/404|not a WebSocket|upgrade.*refused/i.test(error.message)) {
          this.retryAt = Date.now() + 60_000;
          return null;
        }
        throw error;
      } finally { this.connecting = null; }
    })();
    return this.connecting;
  }
  async report(body) {
    const connection = await this.socket();
    if (!connection) return null;
    const id = ++this.sequence;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error("controller response timed out")); connection.close(); }, 30_000);
      this.pending.set(id, { resolve, reject, timer });
      if (!connection.open) {
        clearTimeout(timer); this.pending.delete(id); reject(new Error("controller connection closed")); return;
      }
      // socket.write(false) means the message was queued, not rejected. Large
      // inventories must be allowed to drain; close/timeout handles real failure.
      connection.send(JSON.stringify({ type: "sync", id, body }));
    });
  }
  close() { this.connection?.close(); }
}
