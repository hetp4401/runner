import { DatabaseSync } from 'node:sqlite';
import { writeFile } from 'node:fs/promises';
import { Control } from '../../control/src/index.js';

let now = 1_800_000_000_000;
Date.now = () => now;
const env = { DOMAIN: 'test.example', DNS: 'off', QUIET_MS: '0' };
const db = new DatabaseSync(':memory:');
const c = new Control({ sql: { exec(q, ...args) { const rows = db.prepare(q).all(...args); return { toArray: () => rows }; } }, env, alarm() {}, restart() {}, page: '' });
const events = [];
function sample(machine, minute) {
  return { t: now - 60_000, h: { cpu: (minute * 7 + machine) % 100 + .12, cpuMax: minute % 11 + 80.32, cpu0: minute % 17 + .87, memUsed: 1e9 + minute * 10000, memTotal: 8e9, netRx: minute * 71, diskRead: minute * 19, steal: .13 }, a: { worker: { cpu: minute % 29 + .61, cpuMax: minute % 13 + .43, mem: 12500000 + minute } } };
}
function report(id, machine, started, ready, metrics) {
  const run = { id, agent: id, machine, started, seen: now, ready: ready ? now : 0, handover: 0, retire: 0, status: {}, label: id };
  c.runs.set(id, run);
  c.takeMetrics(run, metrics, now);
  events.push({ now, run, metrics });
}
for (let minute = 0; minute < 145; minute++) {
  now = 1_800_000_000_000 + minute * 60_000;
  for (let machine = 1; machine <= 2; machine++) {
    const m = sample(machine, minute);
    report(`host-${machine}`, machine, 1_799_990_000_000, true, { live: { ...m, t: now, replicas: { 'worker~1': { cpu: 1.25 } } }, minutes: [m] });
  }
  c.cleanup(now);
  events.push({ now, cleanup: true });
}
// An unready successor must not replace the ready predecessor on charts.
report('successor', 1, now - 10_000, false, { live: sample(1, 999), minutes: [sample(1, 999)] });
report('successor', 1, now - 10_000, true, { live: { ...sample(1, 150), t: now }, minutes: [{ ...sample(1, 150), t: now - 70 * 60_000 }] });
report('host-1', 1, 1_799_990_000_000, true, { live: sample(1, 888), minutes: [sample(1, 888)] });
c.cleanup(now); events.push({ now, cleanup: true });
const expected = { live: c.liveNow(now), history: Object.fromEntries(['1h', '6h', '24h', '7d', '30d'].map(range => [range, c.history(range, now)])) };
await writeFile(new URL('../runner-control/tests/fixtures/metrics.json', import.meta.url), JSON.stringify({ env, initial: 1_800_000_000_000, events, expected }) + '\n');
console.log(`${events.length} metrics events, all five ranges, late rollup correction and successor selection`);
db.close();
