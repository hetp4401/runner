import { Control } from "../index.js";
import { capture, restore, difference, applyDifference } from "./codec.js";

export class Engine {
  constructor({ store, env, page = "", effect, wake = () => {}, restart = () => {} }) {
    Object.assign(this, { store, env, page, effect, wake, restart });
    this.makeControl();
    store.takeJournal();
    store.dirty.clear();
    this.baseline = capture(this.control);
  }
  makeControl() {
    const engine = this;
    class HybridControl extends Control {
      cf(path, init = {}) { return engine.effect(path, init); }
      wake(run) { engine.wake(run); return super.wake(run); }
    }
    this.control = new HybridControl({ sql: this.store, env: this.env, page: this.page,
      alarm: (at) => { this.alarmAt = at; }, restart: this.restart });
  }
  load(tables, memory = null) {
    this.store.load(tables);
    this.makeControl();
    if (memory) restore(this.control, memory);
    this.store.takeJournal();
    this.store.dirty.clear();
    this.baseline = capture(this.control);
  }
  checkpoint() { return { memory: capture(this.control), alarmAt: this.alarmAt }; }
  accept({ journal, memory, changes, alarmAt }, { track = false } = {}) {
    const before = capture(this.control);
    const journalLength = this.store.journal.length;
    const dirty = new Set(this.store.dirty);
    this.store.exec("BEGIN");
    try {
      this.store.replay(journal, { track });
      if (memory) restore(this.control, memory);
      else applyDifference(this.control, changes);
      this.store.exec("COMMIT");
    } catch (error) {
      this.store.exec("ROLLBACK");
      restore(this.control, before);
      this.store.journal.length = journalLength;
      this.store.dirty = dirty;
      throw error;
    }
    this.baseline = capture(this.control);
    this.alarmAt = alarmAt;
  }
  async execute(operation, { mirror = true } = {}) {
    let result;
    if (operation.kind === "alarm") {
      await this.control.alarm();
      result = { status: 204, headers: [], body: "" };
    } else {
      const request = new Request(operation.url, { method: operation.method, headers: operation.headers,
        body: ["GET", "HEAD"].includes(operation.method) ? undefined : operation.body });
      const response = await this.control.fetch(request);
      result = { status: response.status, headers: [...response.headers], body: await response.text() };
    }
    // Cloudflare-only scheduling has no external replica to mirror. Avoid
    // cloning/comparing the whole fleet on every report in that mode.
    const memory = mirror ? capture(this.control) : null;
    const changes = mirror ? difference(this.baseline, memory) : null;
    this.baseline = memory;
    return { result, journal: this.store.takeJournal(), changes, alarmAt: this.alarmAt };
  }
}

export async function operationOf(request) {
  const body = ["GET", "HEAD"].includes(request.method) ? "" : await request.text();
  if (new TextEncoder().encode(body).length > 1_000_000) throw new Error("request too large");
  return { kind: "http", url: request.url, method: request.method, headers: [...request.headers], body };
}
export const responseOf = (result) => new Response([204, 205, 304].includes(result.status) ? null : result.body, { status: result.status, headers: result.headers });
