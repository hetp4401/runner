// The streaming half of logs.sh: a viewer (the web app's WebSocket) and an agent (agent/ws.mjs) talking through the
// control plane on localhost:8911, which test/start.sh started. Each line says ok, or WRONG.
import { connect } from "../../agent/ws.mjs";

const B = "localhost:8911";
const check = (what, want, got) => console.log(want === got ? `  ok   ${what}` : `  WRONG ${what}: wanted [${want}], got [${got}]`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const sync = async () => (await fetch(`http://${B}/api/sync`, {
  method: "POST", headers: { authorization: "Bearer node", "content-type": "application/json" },
  body: JSON.stringify({ machine: 1, run: "r1", agent: "a1", pool: "main", starts: false, started: Number(process.env.START), ready: true,
    status: { web: { v: 2, s: "healthy" }, locked: { v: 1, s: "healthy" } }, metrics: { live: { t: Date.now(), h: { cpu: 10, memUsed: 20, memTotal: 100 }, a: {} } } }),
})).json();
const open = async (app, headers) => (await fetch(`http://${B}/api/projects/${app}/logs?replica=1`, { method: "POST", headers })).json();
// A viewer: its messages, and how it closed.
function viewer(session) {
  const v = { msgs: [], closed: null };
  v.ws = new WebSocket(`ws://${B}/admin/api/logs/${session}`);
  v.ws.onmessage = (e) => v.msgs.push(JSON.parse(e.data));
  v.ws.onclose = (e) => (v.closed = e.code);
  v.text = () => v.msgs.filter((m) => m.t === "log").map((m) => m.text).join("");
  return v;
}
const until = async (cond, ms = 5000) => { const end = Date.now() + ms; while (!cond() && Date.now() < end) await sleep(50); return cond(); };

console.log("== a viewer connects: the server waiting at /api/wake is told at once, and its check-in names the session");
const { session } = await open("web", { "x-admin-password": "adm" });
check("a session", true, /^[0-9a-f-]{36}$/.test(session ?? ""));
check("before a viewer connects, nothing is asked of the server", undefined, (await sync()).logs);
const t0 = Date.now();
const woke = fetch(`http://${B}/api/wake?run=r1`, { headers: { authorization: "Bearer node" } }).then((r) => r.json());
await sleep(300);
const v = viewer(session);
check("the server is woken", true, (await woke).wake);
check("within a second or two", true, Date.now() - t0 < 2500);
check("the viewer hears it's asking server 1", true, await until(() => v.msgs.some((m) => m.t === "info" && m.text.includes("server 1"))));
const plan = await sync();
check("the check-in names the session", JSON.stringify([{ session, key: "web", tail: 200 }]), JSON.stringify(plan.logs));
check("the usual check-in interval", 20, plan.poll);

console.log("== the agent connects and streams: lines reach the viewer as they come, the app's env value hidden");
let refused = "";
await connect(`http://${B}/api/logs/stream?session=${session}&run=r1`, { authorization: "Bearer wrong" }).catch((e) => (refused = e.message));
check("a wrong join token is refused", "HTTP 401", refused);
const agent = await connect(`http://${B}/api/logs/stream?session=${session}&run=r1`, { authorization: "Bearer node" });
let agentEnded = false;
agent.onEnd(() => (agentEnded = true));
check("the viewer hears the stream opened", true, await until(() => v.msgs.some((m) => m.t === "open")));
agent.send("w-1  | 2026-10-06T07:00:00Z hello\n");
agent.send("w-1  | 2026-10-06T07:00:01Z config: SECRET=s3cret-value\n");
check("both lines arrived", true, await until(() => v.text().includes("config:")));
check("the env value is hidden", "w-1  | 2026-10-06T07:00:00Z hello\nw-1  | 2026-10-06T07:00:01Z config: SECRET=[hidden]\n", v.text());
const big = "x".repeat(70_000) + "\n";
agent.send(big);
check("a 70 kB message arrives whole", true, await until(() => v.text().endsWith(big)));
check("the session isn't offered again once streaming", undefined, (await sync()).logs);
const second = viewer(session);
check("a second viewer can't join the session", 4404, await until(() => second.closed != null) && second.closed);

console.log("== the viewer leaves: the agent's stream is closed, and nothing is left");
v.ws.close();
check("the agent's socket ends", true, await until(() => agentEnded));
check("the session is gone", undefined, (await sync()).logs);
const stray = viewer("00000000-0000-0000-0000-000000000000");
check("a viewer for no session is closed", 4404, await until(() => stray.closed != null) && stray.closed);

console.log("== the agent ends (its containers went): the viewer is told");
const s2 = (await open("locked", { "x-app-password": "lockpass" })).session;
const v2 = viewer(s2);
await until(() => v2.msgs.length > 0);
check("the locked app's session is offered", "locked", (await sync()).logs?.[0]?.key);
const a2 = await connect(`http://${B}/api/logs/stream?session=${s2}&run=r1`, { authorization: "Bearer node" });
await until(() => v2.msgs.some((m) => m.t === "open"));
a2.close();
check("the viewer hears it ended", true, await until(() => v2.msgs.some((m) => m.t === "end" && m.text.includes("stopped streaming"))));
check("and is closed", 1000, await until(() => v2.closed != null) && v2.closed);
process.exit(0);
