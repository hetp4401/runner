// Control plane for the runner fleet: one Node process with a SQLite file (see ../server.mjs), reached through its own
// Cloudflare tunnel at runners.<domain>. Cloudflare provides nothing else: the machines' tunnels and the DNS names.
// It knows nothing about where machines come from; see the agent's settings for what a machine tells it.
// Holds the project specs (a docker compose file plus any Dockerfiles and build files) and sends every change to all
// machines. Machines find it, not the other way round: any agent joins at /api/join, gets the lowest free slot n and
// the token for tunnel runner-<n> (created through the Cloudflare API the first time a slot is used), then checks in
// at /api/sync. The agent describes its machine; this never knows what's behind it:
//   pool     the name of a replaceable set it belongs to (the control plane keeps each pool at its size), or none
//            for a standalone host that keeps its slot across restarts
//   starts   whether it can start machines: pool members that can are asked to start missing peers and their own
//            replacements; for the rest, whatever watches the pool asks /api/claim
//   label    a name for it, shown on the pages
//   leaving  on its last check-in, when it's going for good (its replicas are placed elsewhere at once)
// and whatever runs the machine pings POST /api/drain when it's going down soon (a timer, a cron before
// maintenance, a cloud termination notice): the control plane then hands the machine over, one at a time, the same
// way as for a requested roll. Nothing here predicts lifetimes.
// It also keeps DNS in line: the public URL of replica k, <project>-<k>.<domain>, is a CNAME to the tunnel of the
// machine running that copy, moved when the copy moves (replicas are numbered 1 to N and keep their number, so these
// URLs only change with the replica count). There's no shared URL in front of a project's replicas, and nothing in
// between: the machine's router answers the replica's name itself.
//   /            the UI (everything public)  /api/*        API: apps are open to everyone (deploying, changing, removing),
//                                                          unless an app was locked with its app password, which its
//                                                          changes then need (x-app-password) or the admin password
//                                                          (x-admin-password); fleet changes need the admin password;
//                                                          machines send the join token (Bearer): join/sync/claim/drain/roll
//   PUT /api/projects/<name>/env  the app's env: {KEY: "value"} sets, {KEY: null} removes. Every copy gets it as its
//                                 .env (compose reads ${VAR} from it; env_file: .env passes it into containers), along
//                                 with FLEET_APP, FLEET_REPLICA, FLEET_MACHINE, FLEET_HOST and FLEET_DOMAIN (nothing that
//                                 changes with the replica count). Values are never sent out again (GET gives the keys), so secrets go
//                                 here rather than in the compose file, which everyone can read. A change is a new version.
//                                            /admin/api/*  the same, for the UI (changes from other sites are refused)
//   /admin, /metrics  redirect to the app      /api/metrics  machine and app metrics (no token needed)
import { randomUUID, timingSafeEqual } from "node:crypto";
import YAML from "yaml";

// How often agents check in: the whole fleet together makes about CHECKINS_PER_DAY check-ins a day, so the interval
// grows with the number of machines (20 machines: every 20 s; 50: every 43 s). Never under MIN_POLL_S or over
// MAX_POLL_S; the POLL_S var fixes it instead.
const CHECKINS_PER_DAY = 100_000; // on its own server this is comfortable; it was 30,000 under the Workers free plan
const MIN_POLL_S = 20;
const MAX_POLL_S = 300;
const PASSWORD_TRIES = 5; // wrong passwords from one address (per app, and for the fleet) before it's refused for PASSWORD_LOCKOUT_MS
const PASSWORD_LOCKOUT_MS = 15 * 60_000;
const APP_PASSWORD_MIN = 6;
const APP_PASSWORD_MAX = 200;
const PBKDF2_ITERATIONS = 10_000; // app passwords are stored as salted PBKDF2-SHA256 (kept light for the CPU limit)
// A run counts as up if it checked in within liveMs: two and a half check-ins, so one late check-in doesn't count as down.
const START_WAIT_MS = 8 * 60_000; // after starting a machine, give it this long to show up before trying again
const HANDOVER_STUCK_MS = 15 * 60_000; // a handover slower than this stops holding up the other machines
const SUCCESSOR_WAIT_MS = 12 * 60_000; // a successor that hasn't joined by then isn't coming (its pool's jobs are all taken)
const CAPPED_MS = 30 * 60_000; // a pool that couldn't start a machine is treated as full for this long
const URGENT_MS = 25 * 60_000; // a run this close to its deadline leaves now, whatever the gates say
const NAME = /^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$/;
const FILE_PATH = /^[A-Za-z0-9_.@+-]+(?:\/[A-Za-z0-9_.@+-]+)*$/;
const MAX_FILES = 30;
const MAX_SPEC_BYTES = 256_000;
const MAX_ENV = 40; // variables in an app's env (PUT /api/projects/<name>/env), each value at most MAX_ENV_VALUE characters
const MAX_ENV_VALUE = 8192;
const ENV_KEY = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
const STANDALONE_HOLD_MS = 30 * 60_000; // a standalone machine that drops out keeps its slot this long, so a restart gets the same one
const DNS_COMMENT = "runner fleet"; // on the DNS records this makes, so they can be told apart in the zone
// Where machines that join with install.sh get the agent's code: a URL serving agent.mjs, metrics.mjs and ws.mjs (AGENT_URL).
const DEFAULT_AGENT_URL = "https://raw.githubusercontent.com/hetp4401/runner/main/agent";
const NUMBERED = /-m?\d+$/; // <project>-<k> is replica k's URL and <project>-m<n> machine n's, so no project name ends like that
const MAX_REPLICAS = 50; // as many as the fleet can have machines; with more replicas than machines, machines run two or more copies
const ARRIVAL_MS = 3 * 60_000; // after a (re)start, wait this long for the fleet and its metrics before placing anything
const SETTLE_MS = 5 * 60_000; // after a (re)start, machines that haven't checked in yet aren't gone, and pools aren't short or oversized, for this long
// The same grace follows a gap in check-ins longer than the liveness window: every machine going quiet at once means the
// control plane was unreachable (its tunnel dropped, say), not that the fleet died. Without it, the first machine back
// would be the only live one and get every copy of everything (which happened on 2026-10-04: two tunnel drops, 40-odd
// copies each time onto one machine, and an app's data gone with its copies recreated at once).
const MOVE_TIMEOUT_MS = 15 * 60_000; // a move whose new copy isn't healthy by then is cancelled (if another copy of the app is ready)
const MOVE_TIMEOUT_EXT_MS = 25 * 60_000; // ...or by then, once, while the new copy is still building
const FAILED_ON_MS = 6 * 3600_000; // a server a move of an app failed on isn't chosen for that app again for this long
const DNS_WAIT_MAX_MS = 5 * 60_000; // a finished move keeps its old copy until DNS points at the new one, at most this long
const EVENTS_KEEP_MS = 30 * 86400_000;
const MASS_LOSS_MIN = 3; // this many servers (or 10% of the fleet) going silent together is an outage, not deaths
const QUIET_AFTER_SETTLE_MS = 10 * 60_000; // nothing is moved, evicted or handed over this soon after a (re)start or gap
// Automatic rebalancing: a server that's hot (4 of its last 5 minutes over these) has the app making it hot moved
// off, to a server with room, at most one hot move per COOLDOWN_MS fleet-wide, and no app more than once per
// PROJECT_COOLDOWN_MS (see rebalanceHot).
const HOT_CPU = 85;
const HOT_MEM = 90;
const ROOM_CPU = 70; // a destination must be under these
const ROOM_MEM = 80;
const COOLDOWN_MS = 10 * 60_000;
const PROJECT_COOLDOWN_MS = 30 * 60_000;
const SETTLED_MS = 5 * 60_000; // a machine takes part once it's been up this long
const FAIL_MS = 10 * 60_000; // a copy failing (or not ready) this long on one server, while its app is healthy elsewhere, moves once
const STRIKE_WINDOW_MS = 30 * 60_000; // two strikes against a server within this make it sick
const QUARANTINE_MS = 60 * 60_000; // a sick or stubbornly hot server isn't a destination for this long
const SICK_DRAIN_GAP_MS = 30 * 60_000; // at most one sick server per pool is asked to leave per this long
const HOT_DEST_MS = 30 * 60_000; // a server that took a hot move isn't a hot move's destination again for this long
const PAUSE_MS = 60 * 60_000; // the thrash breaker pauses automatic moves this long
const DEFAULT_CPUS = 4; // cores assumed for a server whose agent doesn't say
// Logs are streamed from a copy's server while someone watches them, and never kept (see openLogSession).
const LOG_TAIL = 200; // lines of each container's log a stream starts with
const LOG_CONNECT_MS = 30_000; // a session's viewer connects within this long, or it's dropped
const LOG_SESSIONS_MAX = 50; // watched at once, fleet-wide
const WAKE_HOLD_MS = 45_000; // an agent's open /api/wake request is answered after this long if nothing's wanted
// Metrics: agents send one summary per machine per minute; the fleet's minute is stored as one row (few writes),
// and rolled up into 10-minute rows for the day view
const MIN = 60_000;
// and into hourly rows for the week and month views, so those read a few hundred rows rather than thousands.
const KEEP_1M_MS = 48 * 3600_000;
const KEEP_10M_MS = 3 * 24 * 3600_000;
const KEEP_1H_MS = 30 * 24 * 3600_000;
const FLUSH_AFTER_MS = 150_000; // a minute is written once its summaries have had time to arrive
const RANGES = { // range -> [span, step, table]
  "1h": [3600_000, MIN, "metrics_1m"],
  "6h": [6 * 3600_000, 2 * MIN, "metrics_1m"],
  "24h": [24 * 3600_000, 10 * MIN, "metrics_10m"],
  "7d": [7 * 24 * 3600_000, 60 * MIN, "metrics_1h"],
  "30d": [30 * 24 * 3600_000, 240 * MIN, "metrics_1h"],
};

// Combines summaries of the same machine or app: averages, except <field>Max (peaks), which take the largest.
function mergeSummaries(list) {
  const sum = {};
  const cnt = {};
  for (const x of list) {
    for (const [k, v] of Object.entries(x ?? {})) {
      if (typeof v !== "number") continue;
      if (k.endsWith("Max")) sum[k] = Math.max(sum[k] ?? -Infinity, v);
      else {
        sum[k] = (sum[k] ?? 0) + v;
        cnt[k] = (cnt[k] ?? 0) + 1;
      }
    }
  }
  const out = {};
  for (const [k, v] of Object.entries(sum)) {
    const x = cnt[k] ? v / cnt[k] : v;
    out[k] = Math.abs(x) >= 100 ? Math.round(x) : Math.round(x * 100) / 100;
  }
  return out;
}

// { machine: { h, a: { app } } } rows of one bucket -> one { machine: { h, a } }.
function mergeFleet(rows) {
  const byMachine = {};
  for (const row of rows) for (const [m, x] of Object.entries(row)) (byMachine[m] ??= []).push(x);
  const out = {};
  for (const [m, list] of Object.entries(byMachine)) {
    const apps = {};
    for (const x of list) for (const [app, a] of Object.entries(x.a ?? {})) (apps[app] ??= []).push(a);
    out[m] = { h: mergeSummaries(list.map((x) => x.h)), a: Object.fromEntries(Object.entries(apps).map(([k, v]) => [k, mergeSummaries(v)])) };
  }
  return out;
}

class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

const json = (data, status = 200) => Response.json(data, { status });
// The admin password a request carries (x-admin-password, or x-fleet-password, its old name).
const adminPassword = (request) => request.headers.get("x-admin-password") ?? request.headers.get("x-fleet-password");
const html = (body) => new Response(body, { headers: { "content-type": "text/html; charset=utf-8" } });
const isMap = (x) => x !== null && typeof x === "object" && !Array.isArray(x);
const plural = (n, word) => `${n} ${word}${n === 1 ? "" : "s"}`;
const toHex = (bytes) => [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
const fromHex = (hex) => new Uint8Array(hex.match(/../g).map((h) => parseInt(h, 16)));
// Two hex digests compared in constant time.
const sameHex = (a, b) => typeof a === "string" && a.length === b.length && timingSafeEqual(fromHex(a), fromHex(b));

// "./web/" + "Dockerfile" -> "web/Dockerfile"; null if it climbs out of the project folder.
function joinPath(...parts) {
  const out = [];
  for (const seg of parts.join("/").split("/")) {
    if (seg === "" || seg === ".") continue;
    if (seg === "..") {
      if (!out.length) return null;
      out.pop();
    } else out.push(seg);
  }
  return out.join("/");
}

// Apps deployed without the admin password get an ordinary container and nothing more: no way out to the machine
// (its files, its processes, its network, Docker itself), which is where the join token and the machine's other
// secrets are. So their compose files may only use what's listed here; anything else needs the admin password. It's
// a list of what's allowed, so whatever compose adds later is refused until someone has looked at it.
const SAFE_TOP = new Set(["services", "volumes", "networks", "name", "version"]);
const SAFE_SERVICE = new Set(["image", "build", "command", "entrypoint", "environment", "env_file", "ports", "expose",
  "restart", "healthcheck", "depends_on", "working_dir", "user", "labels", "networks", "volumes", "tmpfs", "init",
  "stop_signal", "stop_grace_period", "mem_limit", "mem_reservation", "memswap_limit", "cpus", "cpu_shares", "shm_size",
  "pids_limit", "ulimits", "read_only", "stdin_open", "tty", "hostname", "domainname", "dns", "dns_search", "dns_opt",
  "extra_hosts", "platform", "pull_policy", "logging", "deploy", "network_mode", "cap_drop", "links", "profiles", "scale"]);
const SAFE_BUILD = new Set(["context", "dockerfile", "dockerfile_inline", "args", "target", "labels", "no_cache", "pull",
  "shm_size", "tags", "platforms"]);
const RESERVED_PORTS = new Set([2019, 19080]); // the machine's router and its admin API

// What an untrusted compose file asks for that it may not have (empty if nothing).
function sandboxProblems(doc) {
  const out = [];
  const no = (where, why) => out.push(why ? `${where} (${why})` : where);
  const ext = (k) => k.startsWith("x-");
  // A path on the machine is fine only inside the app's own folder, written plainly: no variables, which compose
  // fills in on the machine (from .env too), so what's checked here is what it uses.
  const inside = (p) => typeof p === "string" && !p.includes("$") && !/^[/~\\]|^[a-z]:/i.test(p) && joinPath(p) !== null;
  for (const k of Object.keys(doc)) if (!SAFE_TOP.has(k) && !ext(k)) no(k);
  for (const [k, v] of Object.entries(isMap(doc.volumes) ? doc.volumes : {})) {
    // driver_opts can bind any folder of the machine, external/name reach volumes that aren't this app's
    if (isMap(v) && Object.keys(v).some((x) => !["labels", "driver"].includes(x) && !ext(x)) || (isMap(v) && v.driver && v.driver !== "local")) {
      no(`volumes.${k}`, "only plain named volumes");
    }
  }
  for (const [k, v] of Object.entries(isMap(doc.networks) ? doc.networks : {})) {
    if (isMap(v) && (Object.keys(v).some((x) => !["labels", "internal", "attachable", "driver", "enable_ipv6"].includes(x) && !ext(x)) || (v.driver && v.driver !== "bridge"))) {
      no(`networks.${k}`, "only plain bridge networks");
    }
  }
  const services = Object.keys(doc.services);
  for (const [name, s] of Object.entries(doc.services)) {
    if (!isMap(s)) continue;
    const at = `services.${name}`;
    for (const k of Object.keys(s)) if (!SAFE_SERVICE.has(k) && !ext(k)) no(`${at}.${k}`);
    if (s.build != null) {
      const b = isMap(s.build) ? s.build : { context: s.build };
      for (const k of Object.keys(b)) if (!SAFE_BUILD.has(k) && !ext(k)) no(`${at}.build.${k}`);
      const context = String(b.context ?? ".");
      const remote = /^(https?|git):\/\//i.test(context) || context.startsWith("git@");
      if (!remote && !inside(context)) no(`${at}.build.context`, "it must be a folder of the app's own files");
      if (b.dockerfile != null && !inside(String(b.dockerfile))) no(`${at}.build.dockerfile`, "it must be one of the app's own files");
    }
    for (const v of [s.volumes ?? []].flat()) {
      const m = typeof v === "string" ? v : isMap(v) ? v : null;
      if (typeof m === "string") {
        const parts = m.split(":");
        if (m.includes("$")) no(`${at}.volumes`, `${m}: no variables`);
        else if (parts.length > 1 && /^[./~\\]|^[a-z]:$/i.test(parts[0]) && !inside(parts[0])) no(`${at}.volumes`, `${parts[0]} is outside the app's folder`);
      } else if (m) {
        const type = m.type ?? "volume";
        if (!["volume", "bind", "tmpfs"].includes(type)) no(`${at}.volumes`, `type ${type}`);
        else if (type === "bind" && !inside(m.source)) no(`${at}.volumes`, `${m.source} is outside the app's folder`);
        else if (type === "volume" && typeof m.source === "string" && m.source.includes("$")) no(`${at}.volumes`, `${m.source}: no variables`);
        if (isMap(m.bind) && Object.keys(m.bind).some((x) => x !== "propagation" && x !== "create_host_path")) no(`${at}.volumes`, "bind options");
      }
    }
    for (const f of [s.env_file ?? []].flat()) {
      const path = isMap(f) ? f.path : f;
      if (!inside(path)) no(`${at}.env_file`, `${path} must be one of the app's own files`);
    }
    if (s.network_mode != null && !["bridge", "none"].includes(s.network_mode) &&
      !(typeof s.network_mode === "string" && s.network_mode.startsWith("service:") && services.includes(s.network_mode.slice(8)))) {
      no(`${at}.network_mode`, `${s.network_mode}`);
    }
    for (const h of [s.extra_hosts ?? []].flat()) if (JSON.stringify(h).includes("host-gateway")) no(`${at}.extra_hosts`, "host-gateway reaches the server");
    for (const p of [s.ports ?? []].flat()) {
      const host = isMap(p) ? p.published : String(p).split("/")[0].split(":").slice(-2, -1)[0];
      if (String(host ?? "").includes("$") || RESERVED_PORTS.has(Number(host))) no(`${at}.ports`, `${JSON.stringify(p)}: that port is the server's`);
    }
    if (isMap(s.logging) && s.logging.driver && !["json-file", "local", "none"].includes(s.logging.driver)) no(`${at}.logging.driver`);
    if (isMap(s.deploy)) {
      for (const k of Object.keys(s.deploy)) if (!["resources", "restart_policy", "replicas", "mode"].includes(k)) no(`${at}.deploy.${k}`);
      if (JSON.stringify(s.deploy.resources ?? {}).includes("devices")) no(`${at}.deploy.resources`, "devices");
    }
  }
  return out;
}

// What a compose file takes on its machine that two projects can't share: the host ports it publishes and its
// container names. Placement keeps projects that would clash off the same machine.
function claimsOf(compose) {
  const out = new Set();
  let doc;
  try {
    doc = YAML.parse(compose, { merge: true });
  } catch {
    return [];
  }
  for (const s of Object.values(isMap(doc?.services) ? doc.services : {})) {
    if (!isMap(s)) continue;
    if (typeof s.container_name === "string") out.add(`container name ${s.container_name}`);
    for (const p of [s.ports ?? []].flat()) {
      // "8080:80", "127.0.0.1:8080:80", "8080-8081:80-81", "8080:80/udp" or { published: 8080 }; a container port on
      // its own gets a random host port, so it claims nothing.
      const host = isMap(p) ? p.published : String(p).split("/")[0].split(":").slice(-2, -1)[0];
      if (host == null || host === "") continue;
      const [a, b = a] = String(host).split("-").map(Number);
      if (!Number.isInteger(a) || !Number.isInteger(b) || b < a || b - a > 100) continue;
      for (let n = a; n <= b; n++) out.add(`port ${n}`);
    }
  }
  return [...out];
}

// What a machine calls the copies of a project it runs (its keys in what the agent is told and reports back): the
// lowest-numbered replica there is plain <name>, so a machine with one copy (the usual case) calls it by its name, as
// agents always have; any further copy there is <name>-r<k>.
function keysOn(replicas, name) {
  const sorted = [...replicas].sort((a, b) => a - b);
  return new Map(sorted.map((k, i) => [k, i === 0 ? name : `${name}-r${k}`]));
}

// What an app says about itself in its compose file's x-runner block, beyond port and replicas:
//   ready: /path     a path that answers 2xx once a copy is ready to serve (synced, say): "healthy" waits for it, but a
//                    copy that isn't ready is never restarted for it (only a copy that stops answering at all is)
// Every app is treated alike otherwise: a move starts the new copy and drops the old one only once the new one works.
function policyOf(compose) {
  let xr = {};
  try {
    xr = YAML.parse(compose)?.["x-runner"] ?? {};
  } catch {}
  if (!isMap(xr)) xr = {};
  return {
    ready: typeof xr.ready === "string" ? xr.ready : null,
  };
}

// Why this compose file's published ports can't be moved to other host ports (null: they can). The machine's network
// itself can't be shared, and a port written with a variable can't be rewritten.
function fixedPorts(compose) {
  let doc;
  try {
    doc = YAML.parse(compose, { merge: true });
  } catch {
    return "the compose file";
  }
  for (const [name, s] of Object.entries(isMap(doc?.services) ? doc.services : {})) {
    if (!isMap(s)) continue;
    if (s.network_mode === "host") return `services.${name}.network_mode: host`;
    for (const p of [s.ports ?? []].flat()) {
      if (JSON.stringify(p).includes("$")) return `services.${name}.ports: variables`;
      if (!isMap(p) && !/^(?:(.*):)?(\d+(?:-\d+)?):(\d+(?:-\d+)?)(\/\w+)?$/.test(String(p)) && !/^\d+(?:-\d+)?(\/\w+)?$/.test(String(p))) return `services.${name}.ports: ${JSON.stringify(p)}`;
    }
  }
  return null;
}

// Why a second copy of this compose file couldn't run next to a first one on the same machine (null: it could):
// its ports can't be moved, or it names its containers.
function dupProblem(compose) {
  let doc;
  try {
    doc = YAML.parse(compose, { merge: true });
  } catch {
    return "the compose file";
  }
  for (const [name, s] of Object.entries(isMap(doc?.services) ? doc.services : {})) {
    if (!isMap(s)) continue;
    if (s.network_mode === "host") return `services.${name}.network_mode: host`;
    if (s.container_name != null) return `services.${name}.container_name`;
    for (const p of [s.ports ?? []].flat()) {
      if (JSON.stringify(p).includes("$")) return `services.${name}.ports: variables`;
      if (!isMap(p) && !/^(?:(.*):)?(\d+(?:-\d+)?):(\d+(?:-\d+)?)(\/\w+)?$/.test(String(p)) && !/^\d+(?:-\d+)?(\/\w+)?$/.test(String(p))) return `services.${name}.ports: ${JSON.stringify(p)}`;
    }
  }
  return null;
}

// "8000-8003" -> [8000, 8001, 8002, 8003]; null if it isn't ports.
function portRange(x) {
  if (x == null || x === "") return null;
  const [a, b = a] = String(x).split("-").map(Number);
  if (!Number.isInteger(a) || !Number.isInteger(b) || b < a || b - a > 100) return null;
  return Array.from({ length: b - a + 1 }, (_, i) => a + i);
}

// The compose file with its published host ports moved as `ports` says ({ "8080": 30412 }), for a copy that shares
// its machine with another copy of the same project. Comments and layout go; the machine only runs it.
function remapCompose(compose, ports) {
  const doc = YAML.parse(compose, { merge: true });
  const at = (i, list) => list[Math.min(i, list.length - 1)];
  for (const s of Object.values(isMap(doc?.services) ? doc.services : {})) {
    if (!isMap(s) || s.ports == null) continue;
    s.ports = [s.ports].flat().flatMap((p) => {
      if (isMap(p)) {
        const pub = portRange(p.published);
        if (!pub) return [p];
        const tgt = portRange(p.target) ?? pub;
        return pub.map((h, i) => ({ ...p, published: String(ports[h] ?? h), target: at(i, tgt) }));
      }
      const m = String(p).match(/^(?:(.*):)?(\d+(?:-\d+)?):(\d+(?:-\d+)?)(\/\w+)?$/);
      if (!m) return [p]; // "80" alone: a random host port, nothing to move
      const [, ip, host, target, proto = ""] = m;
      const hs = portRange(host), ts = portRange(target) ?? hs;
      return hs.map((h, i) => `${ip ? `${ip}:` : ""}${ports[h] ?? h}:${at(i, ts)}${proto}`);
    });
  }
  return YAML.stringify(doc);
}

// The compose file made below for a Dockerfile on its own, as the editor sends it back. It's made afresh on every
// deploy from the port and replica count given now, so a changed port can't disagree with it.
const GENERATED = /^(?:x-runner:\n(?:  port: \d+\n)?(?:  replicas: \d+\n)?)?services:\n  app:\n    build: \.\n(?:    ports: \["\d+:\d+"\]\n)?    restart: unless-stopped\n?$/;

// Turns what was submitted into a spec the machines can run: { compose, port, files }.
// Accepts a compose file, a Dockerfile, extra build files, or a mix; a Dockerfile on its own becomes a
// one-service compose file. Rejects anything that would only fail later on a machine, and, unless the deploy comes
// with the admin password (trusted), anything that would reach outside the app's container (sandboxProblems).
function buildSpec(body, { trusted = false } = {}) {
  if (!isMap(body)) throw new HttpError(400, "send a JSON object");
  let { compose = "", dockerfile = null, files = {}, port = null, replicas = null } = body;
  if (typeof compose !== "string") throw new HttpError(400, "compose must be the compose file as text");
  if (!isMap(files)) throw new HttpError(400, "files must be an object of path: content");
  files = { ...files };
  if (dockerfile !== null) {
    if (typeof dockerfile !== "string" || !dockerfile.trim()) throw new HttpError(400, "dockerfile must be the Dockerfile as text");
    files.Dockerfile = dockerfile;
  }
  if (Object.keys(files).length > MAX_FILES) throw new HttpError(400, `at most ${MAX_FILES} files`);
  let size = compose.length;
  for (const [path, content] of Object.entries(files)) {
    if (typeof content !== "string") throw new HttpError(400, `${path} must be text`);
    if (!FILE_PATH.test(path) || joinPath(path) !== path) throw new HttpError(400, `${path} isn't a usable file path`);
    if (/^(docker-)?compose\.ya?ml$/.test(path)) throw new HttpError(400, "send the compose file as compose, not as a file");
    if (path === ".env") throw new HttpError(400, "the app's .env is made from its env (PUT /api/projects/<name>/env): put the values there, not in a file");
    size += path.length + content.length;
  }
  if (size > MAX_SPEC_BYTES) throw new HttpError(400, "the spec is bigger than 250 KB");
  if (port !== null && !(Number.isInteger(port) && port > 0 && port < 65536)) {
    throw new HttpError(400, "port must be a whole number from 1 to 65535");
  }
  const parseReplicas = (x, where) => {
    if (x === null || x === undefined || x === "") return null;
    const n = Number(x);
    if (!(Number.isInteger(n) && n >= 1 && n <= MAX_REPLICAS)) throw new HttpError(400, `${where} must be a whole number from 1 to ${MAX_REPLICAS}`);
    return n;
  };
  replicas = parseReplicas(replicas, "replicas");
  if (files.Dockerfile && GENERATED.test(compose)) {
    const old = YAML.parse(compose)["x-runner"] ?? {};
    port ??= old.port ?? null;
    replicas ??= parseReplicas(old.replicas, "x-runner.replicas");
    compose = "";
  }
  if (!compose.trim()) {
    if (!files.Dockerfile) throw new HttpError(400, "send a compose file, a Dockerfile, or both");
    // A Dockerfile on its own: build it and publish the port (the app should listen on it inside the container).
    compose = [
      ...(port || replicas !== null ? ["x-runner:"] : []),
      ...(port ? [`  port: ${port}`] : []),
      ...(replicas !== null ? [`  replicas: ${replicas}`] : []),
      "services:",
      "  app:",
      "    build: .",
      ...(port ? [`    ports: ["${port}:${port}"]`] : []),
      "    restart: unless-stopped",
      "",
    ].join("\n");
  }
  let doc;
  try {
    doc = YAML.parse(compose);
  } catch (e) {
    throw new HttpError(400, `the compose file isn't valid YAML: ${e.message.split("\n")[0]}`);
  }
  if (!isMap(doc) || !isMap(doc.services) || !Object.keys(doc.services).length) {
    throw new HttpError(400, "the compose file needs a services: section");
  }
  const declared = doc["x-runner"]?.port ?? null;
  if (declared !== null && !(Number.isInteger(declared) && declared > 0 && declared < 65536)) {
    throw new HttpError(400, "x-runner.port must be a whole number from 1 to 65535");
  }
  if (port !== null && declared !== null && declared !== port) {
    throw new HttpError(400, `the port given (${port}) isn't the compose file's x-runner.port (${declared}); change one of them`);
  }
  port ??= declared;
  replicas ??= parseReplicas(doc["x-runner"]?.replicas, "x-runner.replicas") ?? 1;
  const xr = isMap(doc["x-runner"]) ? doc["x-runner"] : {};
  if (xr.ready != null && !(typeof xr.ready === "string" && /^\/[\x21-\x7e]{0,199}$/.test(xr.ready))) throw new HttpError(400, "x-runner.ready is a path, like /healthz");
  if (!trusted) {
    // Checked as compose reads it: with YAML merge keys (<<) applied.
    const merged = YAML.parse(compose, { merge: true });
    const problems = isMap(merged) && isMap(merged.services) ? sandboxProblems(merged) : ["the compose file"];
    if (problems.length) {
      throw new HttpError(400, `without the admin password, an app gets an ordinary container only, so its compose file can't use: ${problems.slice(0, 6).join("; ")}${problems.length > 6 ? `; and ${problems.length - 6} more` : ""}`);
    }
  }
  // Every service that builds from a local folder needs its Dockerfile among the files.
  for (const [service, def] of Object.entries(doc.services)) {
    if (!isMap(def) || def.build == null) continue;
    const build = isMap(def.build) ? def.build : { context: def.build };
    if (build.dockerfile_inline) continue;
    const context = String(build.context ?? ".");
    if (/^[a-z][a-z0-9+.-]*:\/\//i.test(context) || context.startsWith("git@")) continue; // a git repo or URL
    const dockerfile = joinPath(context, String(build.dockerfile ?? "Dockerfile"));
    if (dockerfile === null) throw new HttpError(400, `service ${service} builds from outside the app's folder`);
    if (!(dockerfile in files)) {
      throw new HttpError(400, `service ${service} builds from ${dockerfile}, but no file with that path was sent`);
    }
  }
  const sorted = Object.fromEntries(Object.keys(files).sort().map((k) => [k, files[k]]));
  return { compose, port, replicas, files: sorted };
}

// Served at /install.sh: runs the agent in a container on any machine with Docker. The token isn't in it; the agent's
// code comes from agentUrl (the repo's main branch unless AGENT_URL says otherwise).
function installScript(origin, agentUrl) {
  return `#!/bin/sh
# Adds this server to the fleet at ${origin}. Needs Docker.
#   curl -fsSL ${origin}/install.sh | sudo JOIN_TOKEN=<token> sh
# Optional: LABEL=<name> names it on the status pages (default: its hostname).
# The agent runs in the container runner-agent, takes a free slot n, and runs the replicas placed on this
# server. It fetches the latest agent code each time it starts.
# Remove the server:  docker rm -f runner-agent tunnel router
set -eu
: "\${JOIN_TOKEN:?set JOIN_TOKEN; the fleet's owner gets it from the Contribute page or with: runnerctl join-token}"
DATA=\${RUNNER_DATA:-/var/lib/runner}
command -v docker >/dev/null 2>&1 || { echo "Install Docker first: https://docs.docker.com/engine/install/" >&2; exit 1; }
mkdir -p "$DATA"
docker rm -f runner-agent >/dev/null 2>&1 || true
docker run -d --name runner-agent --restart unless-stopped --stop-timeout 180 --network host --hostname "$(hostname)" \\
  -v /var/run/docker.sock:/var/run/docker.sock -v "$DATA:$DATA" \\
  -e CONTROL_URL=${origin} -e JOIN_TOKEN="$JOIN_TOKEN" -e RUNNER_DATA="$DATA" \\
  -e LABEL="\${LABEL:-$(hostname)}" \\
  docker:cli sh -c '
    set -e
    apk add --no-cache nodejs >/dev/null
    mkdir -p /agent && cd /agent
    for f in agent.mjs metrics.mjs ws.mjs; do wget -qO $f ${agentUrl}/$f; done
    exec node agent.mjs'
echo "Joined. Follow it with: docker logs -f runner-agent"
`;
}

const EXPORT_TABLES = ["projects", "versions", "copies", "runs", "starts", "settings", "slots", "agents", "app_passwords", "app_env", "events", "metrics_1m", "metrics_10m", "metrics_1h"];

// One line for an event, for the pages and logs.
const fmtDur = (ms) => (ms < 90_000 ? `${Math.round(ms / 1000)} s` : ms < 90 * MIN ? `${Math.round(ms / MIN)} min` : `${(ms / 3600_000).toFixed(1)} h`);

// A copy's report says it isn't serving: it failed, or it answers but isn't ready.
function failing(st) {
  return isMap(st) && (st.s === "failed" || (st.s === "healthy" && st.r === false));
}

function eventText(e) {
  const copy = e.app ? `${e.app}${e.replica != null ? ` replica ${e.replica}` : ""}` : "";
  switch (e.kind) {
    case "move-start": return `moving ${copy} from server ${e.machine} to server ${e.other}${e.detail ? `: ${e.detail}` : ""}`;
    case "move-done": return `moved ${copy} from server ${e.machine} to server ${e.other}`;
    case "move-cancel": return `kept ${copy} on server ${e.machine}: ${e.detail}`;
    case "place": return `placed ${copy} on server ${e.machine}`;
    case "drop": return `removed ${copy} from server ${e.machine} (${e.cause})`;
    case "strike": return `strike against server ${e.machine}: ${e.detail}`;
    case "copy-start": return `${e.cause === "successor" ? "the successor on" : "a new version on"} server ${e.machine} started ${copy}`;
    case "dns": return `${e.app}-${e.replica} now points at server ${e.machine}`;
    case "dark": return `${copy} went dark: ${e.detail}`;
    case "lit": return `${copy} is back: ${e.detail}`;
    case "quarantine": return `server ${e.machine} quarantined: ${e.detail}`;
    case "alert": return `alert: ${e.detail}`;
    default: return e.detail ?? `${e.kind}${copy ? ` ${copy}` : ""}`;
  }
}

export class Control {
  // sql: { exec(query, ...args) -> { toArray() } } over SQLite; env: settings and secrets; alarm(at): call this.alarm()
  // at that time (replacing any earlier call); page: the web app's HTML; restart(): exit so the process is started again.
  constructor({ sql, env, alarm, page, restart }) {
    this.sql = sql;
    this.env = env;
    this.setAlarm = alarm;
    this.page = page;
    this.restart = restart;
    for (const query of [
      `CREATE TABLE IF NOT EXISTS projects (name TEXT PRIMARY KEY, version INTEGER NOT NULL, stable INTEGER,
         rollout INTEGER NOT NULL, halted TEXT, updated INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS versions (name TEXT NOT NULL, version INTEGER NOT NULL, compose TEXT NOT NULL,
         port INTEGER, created INTEGER NOT NULL, PRIMARY KEY (name, version))`,
      `CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, machine INTEGER NOT NULL, started INTEGER NOT NULL,
         status TEXT NOT NULL, ready INTEGER NOT NULL, handover INTEGER NOT NULL, retire INTEGER NOT NULL,
         seen INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS starts (machine INTEGER PRIMARY KEY, at INTEGER NOT NULL, by TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS copies (name TEXT NOT NULL, machine INTEGER NOT NULL, replica INTEGER NOT NULL,
         since INTEGER NOT NULL, reason TEXT NOT NULL, leaving TEXT, ports TEXT, PRIMARY KEY (name, machine, replica))`,
      `CREATE TABLE IF NOT EXISTS slots (n INTEGER PRIMARY KEY, tunnel TEXT NOT NULL, token TEXT NOT NULL, created INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS agents (id TEXT PRIMARY KEY, slot INTEGER NOT NULL, kind TEXT NOT NULL, label TEXT, joined INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS metrics_1m (t INTEGER PRIMARY KEY, data TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS metrics_10m (t INTEGER PRIMARY KEY, data TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS metrics_1h (t INTEGER PRIMARY KEY, data TEXT NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS app_passwords (name TEXT PRIMARY KEY, salt TEXT NOT NULL, hash TEXT NOT NULL, updated INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS app_env (name TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, updated INTEGER NOT NULL, PRIMARY KEY (name, key))`,
      `CREATE TABLE IF NOT EXISTS geo (ip TEXT PRIMARY KEY, text TEXT, at INTEGER NOT NULL)`,
      `CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, t INTEGER NOT NULL, kind TEXT NOT NULL,
         app TEXT, replica INTEGER, machine INTEGER, other INTEGER, run TEXT, cause TEXT, detail TEXT)`,
      `CREATE INDEX IF NOT EXISTS events_t ON events (t)`,
    ]) {
      this.sql.exec(query);
    }
    // Columns added after the first release.
    const columns = (table) => new Set(this.all(`PRAGMA table_info(${table})`).map((c) => c.name));
    if (!columns("projects").has("enabled")) this.sql.exec("ALTER TABLE projects ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1");
    if (!columns("versions").has("files")) this.sql.exec("ALTER TABLE versions ADD COLUMN files TEXT");
    if (!columns("versions").has("replicas")) this.sql.exec("ALTER TABLE versions ADD COLUMN replicas INTEGER");
    const runColumns = columns("runs");
    if (!runColumns.has("agent")) this.sql.exec("ALTER TABLE runs ADD COLUMN agent TEXT");
    if (!runColumns.has("kind")) this.sql.exec("ALTER TABLE runs ADD COLUMN kind TEXT NOT NULL DEFAULT ''"); // no longer used
    if (!runColumns.has("label")) this.sql.exec("ALTER TABLE runs ADD COLUMN label TEXT");
    for (const col of ["pool TEXT", "drain INTEGER", "deadline INTEGER", "evict INTEGER", "quarantine INTEGER"]) {
      if (!runColumns.has(col.split(" ")[0])) this.sql.exec(`ALTER TABLE runs ADD COLUMN ${col}`);
    }
    if (!columns("agents").has("pool")) this.sql.exec("ALTER TABLE agents ADD COLUMN pool TEXT");
    if (columns("events").has("pool")) this.sql.exec("ALTER TABLE events DROP COLUMN pool"); // (briefly had one)
    const copyColumns = columns("copies");
    for (const col of ["key TEXT", "cause TEXT"]) if (!copyColumns.has(col.split(" ")[0])) this.sql.exec(`ALTER TABLE copies ADD COLUMN ${col}`);
    // Working state lives in memory (the object is single-threaded); SQLite keeps it across restarts,
    // which happen when the process is restarted.
    this.projects = new Map(this.all("SELECT * FROM projects").map((p) => [p.name, p]));
    this.runs = new Map(this.all("SELECT * FROM runs").map((r) => [r.id, { ...r, status: JSON.parse(r.status), savedSeen: r.seen }]));
    this.starts = new Map(this.all("SELECT machine, at FROM starts").map((s) => [s.machine, s.at]));
    this.startPools = new Map();
    this.settings = new Map(this.all("SELECT key, value FROM settings").map((s) => [s.key, s.value]));
    this.poolAsks = new Map(Object.entries(JSON.parse(this.settings.get("pool_asks") ?? "{}"))); // pool sizes as their joiners gave them
    this.versions = new Map(); // "name@version" -> { compose, port, replicas, files, claims }
    this.blocked = new Map(); // project -> why a replica of it has no machine (a clash with what's placed everywhere)
    this.copies = new Map(); // project -> [{ machine, replica, since, reason, leaving, ports }], one per copy
    // Copies used to be kept one per machine (table placements); carry them over the first time.
    if (!this.all("SELECT 1 FROM copies LIMIT 1").length && this.all("SELECT name FROM sqlite_master WHERE name = 'placements'").length) {
      const used = new Map();
      for (const x of this.all("SELECT * FROM placements ORDER BY since")) {
        const taken = used.get(x.name) ?? used.set(x.name, new Set()).get(x.name);
        let k = x.replica;
        if (k == null) for (k = 1; taken.has(k); k++);
        taken.add(k);
        this.sql.exec("INSERT OR REPLACE INTO copies (name, machine, replica, since, reason, leaving, ports) VALUES (?, ?, ?, ?, ?, ?, NULL)",
          x.name, x.machine, k, x.since, x.reason, x.leaving);
      }
    }
    for (const x of this.all("SELECT * FROM copies ORDER BY since")) {
      (this.copies.get(x.name) ?? this.copies.set(x.name, []).get(x.name)).push({
        machine: x.machine, replica: x.replica, since: x.since, reason: x.reason, leaving: x.leaving ? JSON.parse(x.leaving) : null,
        ports: x.ports ? JSON.parse(x.ports) : null, key: x.key ?? null, cause: x.cause ?? null,
      });
    }
    // Copies from before keys were stored get the key their machine knows them by now, so nothing is renamed.
    for (const [name, list] of this.copies) {
      for (const m of new Set(list.filter((x) => !x.key).map((x) => x.machine))) {
        const keys = keysOn(list.filter((x) => x.machine === m).map((x) => x.replica), name);
        for (const x of list.filter((y) => y.machine === m && !y.key)) {
          x.key = keys.get(x.replica);
          this.sql.exec("UPDATE copies SET key = ? WHERE name = ? AND machine = ? AND replica = ?", x.key, name, m, x.replica);
        }
      }
    }
    this.remapped = new Map(); // "name@v|ports" -> compose text with moved ports
    this.plainDrops = new Map(); // "name|machine" -> when the copy known by the plain name there was dropped (see freeKey)
    this.failedOn = new Map(); // "name|machine" -> until when a move of the app to that server isn't tried again
    this.unhealthySince = new Map(); // "name|replica" -> since when its copy hasn't been healthy (see transitions)
    this.missingAt = new Map(); // "name|replica" -> when it lost its last copy (see transitions)
    this.deferSince = new Map(); // app -> since when doubling it up has been put off for a starting server
    this.cappedUntil = new Map(); // pool -> until when it's treated as unable to start machines
    this.departLog = []; // [{ t, kind: handover | evict, run }]: departures started by the control plane, last 10 min
    this.lossLog = []; // times of unannounced machine losses, last 10 min
    this.lossNoted = new Set(); // runs whose loss has been noted
    this.pulled = new Set(); // runs made due early to spread a pool's departures (see flatten)
    this.bootAt = Date.now();
    this.settleAt = this.bootAt; // the (re)start, or the end of the last gap in check-ins: SETTLE_MS and ARRIVAL_MS count from here
    // The last check-in before the (re)start or gap: the runs live then are the ones settling() waits for.
    this.quietSince = Math.max(0, ...[...this.runs.values()].map((r) => r.seen));
    this.lastCheckin = this.bootAt;
    this.lastLossCheck = 0;
    this.massArms = JSON.parse(this.settings.get("mass_arms") ?? "[]"); // when the mass-loss breaker last fired
    this.dnsSyncedAt = 0; // the last DNS sync that succeeded (a finished move waits for one, see moveOutcome)
    this.setPace(this.bootAt);
    this.samples = new Map(); // machine -> [{ t, cpu, mem }] from the last few minutes, for the room a server has
    this.lastRebalance = 0;
    this.failures = new Map(); // "address|fleet" or "address|app:<name>" -> { n, at }: wrong passwords
    this.appPasswords = new Map(this.all("SELECT name, salt, hash FROM app_passwords").map((x) => [x.name, x])); // never sent out
    this.geo = new Map(this.all("SELECT ip, text, at FROM geo").map((x) => [x.ip, x])); // where an address is (see locate)
    this.geoPending = new Set();
    this.appEnv = new Map(); // project -> Map(key -> value), its env (see putEnv): sent to machines in .env, never out to the API
    for (const x of this.all("SELECT name, key, value FROM app_env ORDER BY key")) {
      (this.appEnv.get(x.name) ?? this.appEnv.set(x.name, new Map()).get(x.name)).set(x.key, x.value);
    }
    this.autoMoved = new Map(Object.entries(JSON.parse(this.settings.get("auto_moved") ?? "{}"))); // project -> when it was last moved automatically
    this.minutes = new Map(); // run -> its last 10 minute summaries [{ t, cpu, steal, mem, memTotal, a }] (the hot rule)
    this.lastHealthy = new Map(); // "run|key" -> when that copy last reported healthy and ready there (failure clocks)
    this.firstSeen = new Map(); // "run|key" -> when that run first reported the copy (failure clocks)
    this.readySince = new Map(); // app -> since when some copy of it has been healthy (outage-aware failure clocks)
    this.restarts = new Map(); // run -> { last: { key: n }, log: [{ t, key, k }] }: restarts seen in the last hour
    this.lowDiskSince = new Map(); // run -> since when it's had under 10% of its disk free
    this.strikes = new Map(); // run -> [{ t, kind, why }] in the last 30 minutes
    this.sickWhy = new Map(); // run -> why it was quarantined as sick (a pool member then asks to leave)
    this.sickDrains = JSON.parse(this.settings.get("sick_drains") ?? "{}"); // pool -> when its last sick server was asked to leave
    // "app@version" -> servers it failed on: after 2, it's the app, and its copies aren't moved for failing any more.
    this.versionFails = new Map(Object.entries(JSON.parse(this.settings.get("version_fails") ?? "{}")).map(([k, v]) => [k, new Set(v)]));
    this.aloneSince = new Map(); // "app@version" -> since when it has failed somewhere and been healthy nowhere (settle)
    this.noted = new Map(); // what the rebalancer last noted, by subject: once per 30 minutes each
    this.alertedAt = new Map(); // "cause|target" -> when that alert was last raised
    this.lastSickCheck = 0;
    this.lastReadyCheck = 0;
    this.lastV = new Map(); // "run|key" -> the version that run last reported for the copy (version churn)
    this.litAt = new Map(); // "app|k" -> { t, run, machine }: when replica k last had a copy serving, and where
    this.dark = new Map(); // "app|k" -> { since, run, machine, why, alerted }: replicas with no copy serving now
    this.lastDarkCheck = 0;
    this.dueWait = new Map(); // run -> { since, why }: a due departure waiting at its gate
    this.handoverTimes = []; // [{ t, machine, queue, build, overlap }] (ms), the last 24 hours
    this.webhookLog = []; // when alerts were last posted to ALERT_WEBHOOK (at most 30 an hour)
    this.logSessions = new Map(); // id -> { name, replica, machine, key, created, viewer, agent, run, waiting }: logs being watched
    this.wakers = new Map(); // run -> wake(woken): its agent's open /api/wake request
    this.recentEvents = this.all("SELECT * FROM events ORDER BY id DESC LIMIT 200").reverse(); // the newest, for the pages
    this.slots = new Map(this.all("SELECT n, tunnel, token FROM slots").map((x) => [x.n, x])); // slot -> its tunnel
    this.agents = new Map(this.all("SELECT * FROM agents").map((a) => [a.id, a])); // agent -> the slot it last had
    this.holds = new Map(); // slot -> { agent, at }: just given out at /join, not checked in yet
    this.tunnelJobs = new Map(); // slot -> promise, while its tunnel is being looked up or created
    this.lastCleanup = 0;
    this.liveMetrics = new Map(); // machine -> { run, t, h, a }, the newest sample from the run that speaks for it
    this.pendingMetrics = new Map(); // minute -> { machine: { h, a } }, not yet written
    this.lastRollup = 0;
    this.metricsCache = new Map(); // range -> { at, body }
    this.dnsError = null;
    this.dnsTargetsSeen = "";
    this.scheduleDns(); // DNS may have drifted while this wasn't running
  }

  all(query, ...args) {
    return this.sql.exec(query, ...args).toArray();
  }

  // The check-in interval (pollS) for the fleet's current size, and how long a run counts as up without checking in
  // (liveMs: two and a half intervals, so one late check-in doesn't count as down). Worked out from the machines seen
  // in the last 10 minutes, not from liveMs itself.
  setPace(now) {
    const fixed = Number(this.env.POLL_S);
    const machines = new Set([...this.runs.values()].filter((r) => !r.retire && now - r.seen < 10 * 60_000).map((r) => r.machine)).size;
    this.pollS = fixed > 0 ? Math.max(5, fixed) : Math.min(MAX_POLL_S, Math.max(MIN_POLL_S, Math.round((86400 * Math.max(1, machines)) / CHECKINS_PER_DAY)));
    this.liveMs = Math.round(2.5 * this.pollS * 1000);
  }

  setSetting(key, value) {
    this.settings.set(key, String(value));
    this.sql.exec(
      "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
      key,
      String(value),
    );
  }

  // Pools and how many machines each should have running. Any pool can join: its members (and whatever watches it,
  // through /api/claim) say how big it should be. The POOLS setting (portal, runnerctl pool) overrides that, and the
  // POOLS var is the default for a pool nobody has given a size for.
  pools() {
    const fromEnv = typeof this.env.POOLS === "string" ? JSON.parse(this.env.POOLS || "{}") : this.env.POOLS ?? {};
    const out = { ...fromEnv, ...Object.fromEntries(this.poolAsks), ...JSON.parse(this.settings.get("pools") ?? "{}") };
    for (const r of this.runs.values()) if (r.pool && !(r.pool in out)) out[r.pool] = 0; // pools that showed up without a size
    return out;
  }

  // A pool's size as its joiners give it (members re-send it with every check-in; kept across restarts, so a pool
  // whose members can't start machines isn't taken for oversized until its watcher asks again).
  askPoolSize(pool, size) {
    const n = Number(size);
    if (!pool || size == null || size === "" || !Number.isInteger(n) || n < 0 || n > this.maxSlots() || this.poolAsks.get(pool) === n) return;
    this.poolAsks.set(pool, n);
    this.setSetting("pool_asks", JSON.stringify(Object.fromEntries(this.poolAsks)));
  }

  // Just after a (re)start, or a gap in check-ins (the control plane was unreachable), the runs that were live when it
  // went quiet haven't checked in yet to say they're still there. Until every one of them has (or SETTLE_MS have
  // passed), a pool looks smaller than it is and copies look lost, so nothing is started, retired, moved or re-placed.
  // However long the gap was: a run counts if it was live at quietSince.
  settling(now) {
    return now - this.settleAt < SETTLE_MS &&
      [...this.runs.values()].some((x) => !x.retire && x.seen >= this.quietSince - this.liveMs && x.seen < this.settleAt);
  }

  // Quiet: nothing voluntary (moves, evictions, handovers) for a while after a (re)start or gap, or before the fleet
  // has shown up.
  quiet(now) {
    return this.settling(now) || now - this.settleAt < Number(this.env.QUIET_MS ?? QUIET_AFTER_SETTLE_MS);
  }

  // ---- events ----
  // One row per change the control plane makes or sees, with its cause: what the pages show as activity and churn.
  logEvent(kind, f = {}) {
    const e = { t: f.t ?? Date.now(), kind, app: f.app ?? null, replica: f.replica ?? null, machine: f.machine ?? null,
      other: f.other ?? null, run: f.run ?? null, cause: f.cause ?? null, detail: f.detail != null ? String(f.detail).slice(0, 500) : null };
    const row = this.all(`INSERT INTO events (t, kind, app, replica, machine, other, run, cause, detail) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      RETURNING id`, e.t, e.kind, e.app, e.replica, e.machine, e.other, e.run, e.cause, e.detail)[0];
    this.recentEvents.push({ id: row?.id, ...e });
    if (this.recentEvents.length > 200) this.recentEvents.splice(0, this.recentEvents.length - 200);
  }

  events(q) {
    const where = [];
    const args = [];
    const since = Number(q.get("since") ?? 0);
    where.push("t >= ?");
    args.push(Number.isFinite(since) ? since : 0);
    for (const f of ["app", "kind", "cause"]) if (q.get(f)) { where.push(`${f} = ?`); args.push(q.get(f)); }
    const kinds = String(q.get("kinds") ?? "").split(",").filter((k) => /^[a-z-]{1,20}$/.test(k)).slice(0, 20);
    if (kinds.length) {
      where.push(`kind IN (${kinds.map(() => "?").join(", ")})`);
      args.push(...kinds);
    }
    if (q.get("machine")) { where.push("(machine = ? OR other = ?)"); args.push(Number(q.get("machine")), Number(q.get("machine"))); }
    const limit = Math.min(1000, Math.max(1, Number(q.get("limit") ?? 200) || 200));
    return { events: this.all(`SELECT * FROM events WHERE ${where.join(" AND ")} ORDER BY id DESC LIMIT ?`, ...args, limit).map((e) => ({ ...e, text: eventText(e) })) };
  }

  poolSize(pool) {
    return Number(this.pools()[pool] ?? 0);
  }

  // Machines expected to be up: every pool's size (standalone machines come and go as they please).
  expectedMachines() {
    return Object.values(this.pools()).reduce((a, b) => a + Number(b), 0);
  }

  // How many machines can have a slot (a tunnel each): MAX_SLOTS if it's set to a number above 0, else no cap.
  maxSlots() {
    const n = Number(this.env.MAX_SLOTS);
    return Number.isInteger(n) && n > 0 ? n : Infinity;
  }

  live(run, now) {
    return now - run.seen < this.liveMs;
  }

  liveRuns(now) {
    return [...this.runs.values()].filter((r) => !r.retire && this.live(r, now));
  }

  version(name, v) {
    const key = `${name}@${v}`;
    if (!this.versions.has(key)) {
      const row = this.all("SELECT compose, port, files, replicas FROM versions WHERE name = ? AND version = ?", name, v)[0];
      if (!row) return undefined; // not cached: a version asked for before it's deployed mustn't stay missing once it is
      const claims = new Set(claimsOf(row.compose));
      if (row.port) claims.add(`port ${row.port}`);
      const policy = policyOf(row.compose);
      this.versions.set(key, { compose: row.compose, port: row.port, replicas: row.replicas ?? 1, files: JSON.parse(row.files ?? "{}"), claims: [...claims],
        nodup: dupProblem(row.compose), fixed: fixedPorts(row.compose), policy });
    }
    return this.versions.get(key);
  }

  saveProject(p) {
    this.projects.set(p.name, p);
    this.sql.exec(
      `INSERT INTO projects (name, version, stable, rollout, halted, updated, enabled) VALUES (?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (name) DO UPDATE SET version = excluded.version, stable = excluded.stable,
         rollout = excluded.rollout, halted = excluded.halted, updated = excluded.updated, enabled = excluded.enabled`,
      p.name, p.version, p.stable, p.rollout, p.halted, p.updated, p.enabled,
    );
  }

  saveRun(r) {
    this.runs.set(r.id, r);
    r.savedSeen = r.seen;
    this.sql.exec(
      `INSERT INTO runs (id, machine, started, status, ready, handover, retire, seen, agent, kind, label, pool, drain, deadline, evict, quarantine)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET status = excluded.status, ready = excluded.ready, handover = excluded.handover,
         retire = excluded.retire, seen = excluded.seen, label = excluded.label, pool = excluded.pool, drain = excluded.drain,
         deadline = excluded.deadline, evict = excluded.evict, quarantine = excluded.quarantine`,
      r.id, r.machine, r.started, JSON.stringify(r.status), r.ready, r.handover, r.retire, r.seen, r.agent, "", r.label, r.pool ?? null, r.drain ?? 0,
      r.deadline ?? null, r.evict ?? null, r.quarantine ?? null,
    );
  }

  markStart(machine, at, by, pool) {
    this.starts.set(machine, at);
    this.startPools.set(machine, pool); // in memory only: after a restart claims wait for runs to re-identify anyway
    this.sql.exec(
      "INSERT INTO starts (machine, at, by) VALUES (?, ?, ?) ON CONFLICT (machine) DO UPDATE SET at = excluded.at, by = excluded.by",
      machine, at, by,
    );
  }

  // ---- HTTP ----

  async fetch(request) {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    this.setPace(Date.now());
    try {
      if (request.method === "GET" && path === "/") return html(this.page);
      // The old pages are views of the app now; the app turns their #fragments into its own routes.
      if (request.method === "GET" && (path === "/admin" || path === "/metrics")) {
        return new Response(null, { status: 302, headers: { location: `/?from=${path.slice(1)}` } });
      }
      if (request.method === "GET" && path === "/install.sh") return new Response(installScript(url.origin, this.env.AGENT_URL || DEFAULT_AGENT_URL), { headers: { "content-type": "text/plain; charset=utf-8" } });
      if (request.method === "GET" && path === "/internal/dns") return json({ names: Object.fromEntries(this.dnsTargets(Date.now())) }); // where each replica's name points (tests)
      if (path.startsWith("/admin/api/")) {
        // The UI: apps are open unless locked with a password; fleet changes need the admin password. Changes sent
        // from other sites are refused. No machine powers here (joining, checking in): those hand out tunnel tokens.
        const origin = request.headers.get("origin");
        if (request.method !== "GET" && origin && origin !== url.origin) throw new HttpError(403, "cross-site request refused");
        const route = path.slice("/admin/api".length);
        const ip = request.headers.get("cf-connecting-ip") ?? "";
        // Passwords are only looked at where they count (fleet changes, changes to apps), so an address refused for
        // wrong guesses can still see everything.
        const fleet = /^\/(unlock|settings|roll|join-token|machines\/\d+\/evict|slots\/\d+)$/.test(route);
        const appChange = request.method !== "GET" && route.startsWith("/projects/");
        const password = adminPassword(request);
        const admin = (fleet || appChange) && password != null && await this.checkPassword(password, ip);
        if (request.method === "POST" && route === "/unlock") {
          if (!admin) throw new HttpError(401, "wrong password");
          return json({ ok: true });
        }
        return await this.api(request, url, route, {
          admin, node: false, ip, adminWrong: password != null && !admin, appPassword: request.headers.get("x-app-password"),
        });
      }
      if (path.startsWith("/api/")) {
        // Scripts: the same as the UI (apps are open; the admin password for fleet changes and locked apps); machines
        // send the join token.
        const password = adminPassword(request);
        const ip = request.headers.get("cf-connecting-ip") ?? "";
        const admin = password != null && await this.checkPassword(password, ip);
        const node = admin || (Boolean(this.env.JOIN_TOKEN) && request.headers.get("authorization") === `Bearer ${this.env.JOIN_TOKEN}`);
        return await this.api(request, url, path.slice("/api".length), {
          admin, node, ip, adminWrong: password != null && !admin, appPassword: request.headers.get("x-app-password"),
        });
      }
      throw new HttpError(404, "not found");
    } catch (e) {
      return json({ error: e.message }, e.status ?? 500);
    }
  }

  // Wrong passwords are counted per address and per thing guessed at (key "address|fleet" or "address|app:<name>"), so
  // getting one app's password right doesn't reset the count for another app or the fleet. Too many, and that address
  // is refused for a while.
  refuseIfLocked(key, now) {
    const f = this.failures.get(key);
    if (f && f.n >= PASSWORD_TRIES && now - f.at < PASSWORD_LOCKOUT_MS) {
      throw new HttpError(429, `too many wrong passwords; try again in ${Math.ceil((PASSWORD_LOCKOUT_MS - (now - f.at)) / 60_000)} min`);
    }
  }

  noteTry(key, ok, now) {
    const f = this.failures.get(key);
    if (ok) return void this.failures.delete(key);
    this.failures.set(key, { n: (f && now - f.at < PASSWORD_LOCKOUT_MS ? f.n : 0) + 1, at: now });
    if (this.failures.size > 10_000) for (const [k, x] of this.failures) if (now - x.at >= PASSWORD_LOCKOUT_MS) this.failures.delete(k);
  }

  // The admin password (the setting ADMIN_PASSWORD; FLEET_PASSWORD, its old name, still works), compared in constant time.
  async checkPassword(given, ip) {
    const expected = this.env.ADMIN_PASSWORD || this.env.FLEET_PASSWORD;
    if (!expected) return false;
    const key = `${ip}|fleet`;
    this.refuseIfLocked(key, Date.now());
    const digest = async (x) => crypto.subtle.digest("SHA-256", new TextEncoder().encode(x));
    const ok = timingSafeEqual(Buffer.from(await digest(given)), Buffer.from(await digest(expected)));
    this.noteTry(key, ok, Date.now());
    return ok;
  }

  // App passwords are kept as salted PBKDF2-SHA256 (hex), never the password itself.
  async passwordHash(password, salt) {
    const key = await crypto.subtle.importKey("raw", new TextEncoder().encode(password), "PBKDF2", false, ["deriveBits"]);
    const bits = await crypto.subtle.deriveBits({ name: "PBKDF2", hash: "SHA-256", salt: fromHex(salt), iterations: PBKDF2_ITERATIONS }, key, 256);
    return toHex(new Uint8Array(bits));
  }

  async newAppSecret(password) {
    if (typeof password !== "string" || password.length < APP_PASSWORD_MIN || password.length > APP_PASSWORD_MAX) {
      throw new HttpError(400, `an app's password ("password") is ${APP_PASSWORD_MIN} to ${APP_PASSWORD_MAX} characters`);
    }
    const salt = toHex(crypto.getRandomValues(new Uint8Array(16)));
    return { salt, hash: await this.passwordHash(password, salt) };
  }

  saveAppPassword(name, secret) {
    this.appPasswords.set(name, { name, ...secret });
    this.sql.exec(
      `INSERT INTO app_passwords (name, salt, hash, updated) VALUES (?, ?, ?, ?)
       ON CONFLICT (name) DO UPDATE SET salt = excluded.salt, hash = excluded.hash, updated = excluded.updated`,
      name, secret.salt, secret.hash, Date.now(),
    );
  }

  // admin: fleet changes (pools, rebalancing, restarts, evictions, slots, the join token), and any app, locked or not;
  // node: machines. Apps are open to everyone otherwise, unless locked with a password (appPassword then).
  async api(request, url, route, { admin, node, ip = "", adminWrong = false, appPassword = null }) {
    const { method } = request;
    const body = () => request.json().catch(() => {
      throw new HttpError(400, "the body must be JSON");
    });
    const need = (ok, what = "the admin password") => {
      if (!ok) throw new HttpError(401, `this needs ${what}`);
    };
    if (method === "GET" && route === "/status") return json(this.status());
    if (method === "GET" && route === "/events") return json(this.events(url.searchParams));
    if (method === "GET" && route === "/metrics") return this.metricsResponse(url.searchParams.get("range") ?? "1h");
    if (method === "POST" && route === "/join") return need(node, "the join token"), json(await this.join(await body()));
    if (method === "POST" && route === "/sync") return need(node, "the join token"), json(this.sync(await body(), ip));
    if (method === "POST" && route === "/claim") {
      need(node, "the join token");
      const pool = url.searchParams.get("pool");
      this.askPoolSize(pool, url.searchParams.get("size"));
      return json({ start: this.claimStarts(pool, "claim", Date.now()) });
    }
    if (method === "POST" && route === "/drain") return need(node, "the join token"), json(this.drain(await body()));
    if (method === "GET" && route === "/wake") return need(node, "the join token"), json(await this.wait(url.searchParams.get("run")));
    if (method === "POST" && route === "/roll") return need(admin || node, "the admin password or the join token"), json(this.roll(url.searchParams.get("machine")));
    if (method === "DELETE" && route === "/roll") return need(admin, "the admin password"), json(this.cancelRoll());
    if (method === "GET" && route === "/join-token") return need(admin), json({ token: this.env.JOIN_TOKEN });
    if (method === "GET" && route === "/export") { // everything stored, a table at a time, for moving the control plane
      need(admin);
      const table = url.searchParams.get("table");
      if (!EXPORT_TABLES.includes(table)) throw new HttpError(400, `table is one of ${EXPORT_TABLES.join(", ")}`);
      const after = Number(url.searchParams.get("after") ?? 0);
      const limit = Math.min(2000, Number(url.searchParams.get("limit") ?? 500));
      const rows = this.all(`SELECT rowid AS _rowid, * FROM ${table} WHERE rowid > ? ORDER BY rowid LIMIT ?`, after, limit);
      return json({ rows, next: rows.length === limit ? rows[rows.length - 1]._rowid : null });
    }
    if (method === "POST" && route === "/import") { // moving the control plane in: rows of one table at a time, then ?done=1 to start again on them
      need(admin);
      if (url.searchParams.get("done")) {
        setTimeout(() => this.restart?.(), 300);
        return json({ restarting: true });
      }
      const { table, rows } = await body();
      if (!EXPORT_TABLES.includes(table) || !Array.isArray(rows)) throw new HttpError(400, "send {table, rows}");
      const known = new Set(this.all(`PRAGMA table_info(${table})`).map((c) => c.name)); // columns from features since dropped are skipped
      for (const row of rows) {
        const cols = Object.keys(row).filter((c) => known.has(c));
        this.sql.exec(`INSERT OR REPLACE INTO ${table} (${cols.join(", ")}) VALUES (${cols.map(() => "?").join(", ")})`, ...cols.map((c) => row[c]));
      }
      return json({ imported: rows.length });
    }
    if (method === "PUT" && route === "/settings") return need(admin), json(this.putSettings(await body()));
    const ev = route.match(/^\/machines\/(\d+)\/evict$/);
    if (ev && method === "POST") return need(admin), json(this.evict(Number(ev[1]), { force: url.searchParams.get("force") === "1" }));
    const sl = route.match(/^\/slots\/(\d+)$/);
    if (sl && method === "DELETE") return need(admin), json(await this.retireSlot(Number(sl[1])));
    // Apps are open: anyone can look, deploy a new one, change or remove one. Unless it's locked: a "password" sent
    // with a new app (or set later at /password) locks it, and changing it then needs that password (x-app-password)
    // or the admin password.
    const m = route.match(/^\/projects\/([^/]+)(?:\/(enable|disable|move|unlock|password|env|logs))?$/);
    if (m) {
      const [, name, action] = m;
      if (!action && method === "GET") return json(this.getProject(name, url.searchParams.get("version")));
      if (action === "env" && method === "GET") return json({ name, keys: this.envKeys(name) });
      const input = method === "PUT" ? await body() : null;
      const key = `${ip}|app:${name}`;
      // The hashing comes first, so the checks and the change below run with no await between them and no other
      // request can slip in (deploying the same new name, say).
      const stored = this.appPasswords.get(name);
      let given = null;
      if (!admin && appPassword != null && stored) {
        this.refuseIfLocked(key, Date.now());
        given = { salt: stored.salt, hash: await this.passwordHash(appPassword, stored.salt) };
      }
      const wasNew = !action && method === "PUT" && !this.projects.has(name);
      const secret = action === "password" || (wasNew && input?.password != null) ? await this.newAppSecret(input?.password) : null;
      // No awaits from here on.
      const creating = !action && method === "PUT" && !this.projects.has(name);
      if (!creating && !admin) {
        this.project(name); // 404 if there's no such app
        const current = this.appPasswords.get(name);
        if (current) { // locked
          if (appPassword == null) throw new HttpError(401, adminWrong ? "wrong admin password" : `${name} is locked: changing it needs its password or the admin password`);
          const now = Date.now();
          this.refuseIfLocked(key, now);
          const ok = given?.salt === current.salt && sameHex(given.hash, current.hash);
          this.noteTry(key, ok, now);
          if (!ok) throw new HttpError(401, `wrong password for ${name}`);
        } else if (adminWrong) throw new HttpError(401, "wrong admin password");
      }
      if (action === "unlock" && method === "POST") return json({ ok: true });
      // Logs can hold secrets and other people's data: watching them needs the app's password, or the admin password
      // (an app without a password of its own: the admin password).
      if (action === "logs" && method === "POST") {
        if (!admin && !this.appPasswords.has(name)) throw new HttpError(401, `${name} has no app password, so its logs need the admin password`);
        return json(this.openLogSession(name, Number(url.searchParams.get("replica") ?? 1), Date.now()));
      }
      if (action === "password" && method === "PUT") {
        this.project(name);
        this.saveAppPassword(name, secret);
        return json({ ok: true });
      }
      if (action === "env" && method === "PUT") return json(this.putEnv(name, input));
      if (action === "move" && method === "POST") return json(this.move(name, Number(url.searchParams.get("from")), url.searchParams.get("to"), url.searchParams.get("replica")));
      if ((action === "enable" || action === "disable") && method === "POST") return json(this.setEnabled(name, action === "enable"));
      if (!action && method === "PUT") {
        const out = this.putProject(name, input, { trusted: admin });
        if (creating && secret) {
          this.saveAppPassword(name, secret);
          out.hasPassword = true;
        }
        return json(out);
      }
      if (!action && method === "DELETE") return json(this.deleteProject(name));
    }
    throw new HttpError(404, "not found");
  }


  // ---- projects ----

  describe(p) {
    const latest = this.version(p.name, p.version);
    const placed = this.copiesOf(p.name).map((x) => ({ machine: x.machine, replica: x.replica, key: x.key, since: x.since, reason: x.reason, cause: x.cause, leaving: x.leaving, moved: Boolean(x.ports) }))
      .sort((a, b) => a.replica - b.replica || Boolean(a.leaving) - Boolean(b.leaving));
    const staying = placed.filter((x) => !x.leaving).length;
    return {
      name: p.name,
      version: p.version,
      stable: p.stable,
      port: latest?.port ?? null,
      replicas: latest?.replicas ?? 1,
      placed, // each replica's machine, by replica number; a copy being moved away (leaving) comes after its new copy
      staying,
      blocked: this.blocked.get(p.name) ?? null, // why a replica has no machine, when it's a clash rather than a shortage
      files: Object.keys(latest?.files ?? {}),
      enabled: Boolean(p.enabled),
      hasPassword: this.appPasswords.has(p.name), // locked: changing it needs that password (or the fleet's); else anyone can
      env: [...(this.appEnv.get(p.name)?.keys() ?? [])], // its env's variable names (the values stay here)
      state: !p.enabled ? "disabled" : p.halted ? "halted" : p.stable === p.version ? "live" : "deploying",
      halted: p.halted,
      updated: p.updated,
    };
  }

  project(name) {
    const p = this.projects.get(name);
    if (!p) throw new HttpError(404, `no app called ${name}`);
    return p;
  }

  getProject(name, versionParam) {
    const p = this.project(name);
    const v = versionParam ? Number(versionParam) : p.version;
    const spec = this.version(name, v);
    if (!spec) throw new HttpError(404, `${name} has no version ${versionParam}`);
    return {
      ...this.describe(p),
      shown: v,
      compose: spec.compose,
      files: spec.files,
      env: this.envKeys(name), // the keys only
      port: spec.port,
      replicas: spec.replicas,
      versions: this.all("SELECT version, port, replicas, created FROM versions WHERE name = ? ORDER BY version", name)
        .map((v) => ({ ...v, replicas: v.replicas ?? 1 })),
    };
  }

  // A new spec becomes a new version, which goes to every machine at once.
  putProject(name, body, { trusted = false } = {}) {
    if (!NAME.test(name)) throw new HttpError(400, "app names are lowercase letters, digits and dashes");
    if (!this.projects.has(name) && NUMBERED.test(name)) {
      throw new HttpError(400, "an app name can't end in -<number> or -m<number>: those are the URLs of its replicas and servers");
    }
    const p = this.projects.get(name);
    const latest = p && this.version(name, p.version);
    // Only a replica count or port: the latest version's files are kept as they are, so a page that was open for a
    // while can't put back files someone else has changed since.
    if (latest && isMap(body) && body.compose == null && body.dockerfile == null && body.files == null) {
      body = { ...body, compose: latest.compose, files: latest.files, port: body.port === undefined ? latest.port : body.port,
        replicas: body.replicas === undefined ? latest.replicas : body.replicas };
    }
    const spec = buildSpec(body, { trusted });
    const t = Date.now();
    const machines = this.expectedMachines();
    // env: {KEY: "value"} in the same request sets the app's env (see putEnv) before the version is made, so a new
    // app's first version already has it (a secret the containers need to start, say).
    const envChanged = body.env !== undefined ? this.mergeEnv(name, body.env, t) : false;
    const same = latest && !envChanged && latest.compose === spec.compose && latest.port === spec.port && latest.replicas === spec.replicas &&
      JSON.stringify(latest.files) === JSON.stringify(spec.files);
    let next;
    if (same) {
      // Same spec again: a no-op, unless the version hasn't reached every machine yet (then push it there).
      if (!p.halted && p.stable === p.version) return { ...this.describe(p), unchanged: true };
      next = { ...p, halted: null, rollout: machines, stable: p.version, updated: t };
    } else {
      const version = (p?.version ?? 0) + 1;
      this.sql.exec(
        "INSERT INTO versions (name, version, compose, port, files, replicas, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
        name, version, spec.compose, spec.port, JSON.stringify(spec.files), spec.replicas, t,
      );
      next = {
        name, version, stable: version, rollout: machines, halted: null,
        updated: t, enabled: p?.enabled ?? 1,
      };
    }
    this.saveProject(next);
    this.place(t);
    this.scheduleDns();
    return this.describe(next);
  }

  // Disabled projects stay in the list with their versions, but no machine runs them.
  setEnabled(name, enabled) {
    const p = this.project(name);
    this.saveProject({ ...p, enabled: enabled ? 1 : 0, updated: Date.now() });
    this.place(Date.now());
    return this.describe(this.projects.get(name));
  }

  // ---- an app's env ----
  // KEY=value pairs every copy gets in its .env: compose fills ${VAR} from it, and `env_file: .env` passes it into a
  // container. Values never leave here again (only the keys do), so this is where secrets belong: the compose file
  // itself is public. The copy's place in the fleet goes in the same file as FLEET_* variables. A change makes a new
  // version with the same spec, so every machine rewrites the file and restarts whatever read it.
  envKeys(name) {
    this.project(name);
    return [...(this.appEnv.get(name)?.keys() ?? [])];
  }

  // Applies {KEY: "value" | null} to an app's env (stored; the project needn't exist yet). True if anything changed.
  mergeEnv(name, patch, t) {
    if (!isMap(patch)) throw new HttpError(400, 'env must be an object: {"KEY": "value"} sets a variable, {"KEY": null} removes one');
    const env = new Map(this.appEnv.get(name) ?? []);
    for (const [key, value] of Object.entries(patch)) {
      if (!ENV_KEY.test(key)) throw new HttpError(400, `${key} isn't a usable variable name (letters, digits and _, up to 64)`);
      if (key.startsWith("FLEET_")) throw new HttpError(400, `${key}: FLEET_ variables are set by the fleet`);
      if (value === null) env.delete(key);
      else if (typeof value !== "string") throw new HttpError(400, `${key} must be text (or null to remove it)`);
      else if (value.length > MAX_ENV_VALUE) throw new HttpError(400, `${key} is longer than ${MAX_ENV_VALUE} characters`);
      else env.set(key, value);
    }
    if (env.size > MAX_ENV) throw new HttpError(400, `at most ${MAX_ENV} variables`);
    const before = this.appEnv.get(name);
    const changed = (before?.size ?? 0) !== env.size || [...env].some(([k, v]) => before?.get(k) !== v);
    if (!changed) return false;
    this.sql.exec("DELETE FROM app_env WHERE name = ?", name);
    for (const [k, v] of env) this.sql.exec("INSERT INTO app_env (name, key, value, updated) VALUES (?, ?, ?, ?)", name, k, v, t);
    if (env.size) this.appEnv.set(name, env);
    else this.appEnv.delete(name);
    return true;
  }

  putEnv(name, body) {
    const p = this.project(name);
    if (!isMap(body) || !Object.keys(body).length) throw new HttpError(400, 'send {"KEY": "value"} to set a variable and {"KEY": null} to remove one');
    const t = Date.now();
    const changed = this.mergeEnv(name, body, t);
    const latest = this.version(name, p.version);
    if (changed && latest) {
      const version = p.version + 1;
      this.sql.exec(
        "INSERT INTO versions (name, version, compose, port, files, replicas, created) VALUES (?, ?, ?, ?, ?, ?, ?)",
        name, version, latest.compose, latest.port, JSON.stringify(latest.files), latest.replicas, t,
      );
      this.saveProject({ ...p, version, stable: version, rollout: this.expectedMachines(), halted: null, updated: t });
      this.place(t);
    }
    return { ...this.describe(this.project(name)), keys: this.envKeys(name), unchanged: !changed || undefined };
  }

  // The .env for copy x of a project: where it is in the fleet, then the app's own variables. Written the way compose
  // reads the file: single quotes keep a value as it is; a value with a quote or a line break is double-quoted and escaped.
  envFile(name, spec, x, v) {
    const quote = (s) => (!/['\n\r]/.test(s) ? `'${s}'` : `"${s.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\$/g, "$$$$").replace(/\r?\n/g, "\\n")}"`);
    // Nothing here changes with the app's replica count or version: a copy whose files and .env are the same isn't
    // recreated, so changing the count leaves the copies that stay alone.
    const lines = [
      `FLEET_APP=${name}`,
      `FLEET_REPLICA=${x.replica}`,
      `FLEET_MACHINE=${x.machine}`,
      `FLEET_HOST=${name}-${x.replica}.${this.env.DOMAIN}`,
      `FLEET_DOMAIN=${this.env.DOMAIN}`,
    ];
    for (const [k, val] of this.appEnv.get(name) ?? []) lines.push(`${k}=${quote(val)}`);
    return `${lines.join("\n")}\n`;
  }

  deleteProject(name) {
    this.project(name);
    this.projects.delete(name);
    this.sql.exec("DELETE FROM projects WHERE name = ?", name);
    this.sql.exec("DELETE FROM versions WHERE name = ?", name);
    this.sql.exec("DELETE FROM copies WHERE name = ?", name);
    this.sql.exec("DELETE FROM app_passwords WHERE name = ?", name);
    this.sql.exec("DELETE FROM app_env WHERE name = ?", name);
    this.copies.delete(name);
    this.blocked.delete(name);
    this.appPasswords.delete(name);
    this.appEnv.delete(name);
    for (const key of this.versions.keys()) if (key.startsWith(`${name}@`)) this.versions.delete(key);
    this.scheduleDns();
    return { deleted: name };
  }

  putSettings(body) {
    const { pools, rebalance } = isMap(body) ? body : {};
    if (rebalance !== undefined) {
      if (typeof rebalance !== "boolean") throw new HttpError(400, "rebalance must be true or false");
      this.setSetting("rebalance", rebalance ? "on" : "off");
    }
    if (pools !== undefined) {
      if (!isMap(pools)) throw new HttpError(400, 'pools must be an object of pool name to size, like {"name": 10}');
      const next = { ...JSON.parse(this.settings.get("pools") ?? "{}") };
      for (const [name, size] of Object.entries(pools)) {
        if (!/^[a-z0-9-]{1,30}$/.test(name)) throw new HttpError(400, "a pool name is lowercase letters, digits and dashes");
        if (!(Number.isInteger(size) && size >= 0 && size <= this.maxSlots())) throw new HttpError(400, Number.isFinite(this.maxSlots()) ? `a pool's size is 0-${this.maxSlots()}` : "a pool's size is a whole number, 0 or more");
        next[name] = size;
      }
      this.setSetting("pools", JSON.stringify(next));
      this.scheduleDns();
    }
    return { pools: this.pools(), rebalance: this.rebalanceOn() };
  }

  rebalanceOn() {
    return this.settings.get("rebalance") !== "off";
  }

  // Copies younger than this (built on their server) aren't moved automatically: a start-up spike isn't heat (YOUNG_MS).
  youngMs() {
    return Number(this.env.YOUNG_MS ?? 10 * MIN);
  }

  // Replace machines one at a time (all of them, or just one), e.g. after the agent code changes.
  roll(machine) {
    const now = Date.now();
    this.setSetting(machine ? `roll_${Number(machine)}` : "roll", now);
    this.logEvent("roll", { machine: machine ? Number(machine) : null, cause: "start", detail: machine ? `server ${Number(machine)} is to be replaced` : "every server is to be replaced, one at a time", t: now });
    return { rolling: machine ? [Number(machine)] : "all", since: now };
  }

  // Call a roll off: the servers it made due go back to their own schedules (one already handing over finishes).
  cancelRoll() {
    const keys = [...this.settings.keys()].filter((k) => k === "roll" || /^roll_\d+$/.test(k));
    for (const k of keys) {
      this.settings.delete(k);
      this.sql.exec("DELETE FROM settings WHERE key = ?", k);
    }
    if (keys.length) this.logEvent("roll", { cause: "cancelled", detail: "the roll was called off: servers leave on their own schedules again" });
    return { cancelled: keys.length > 0 };
  }

  // What machine n should run: every copy placed on it, at its project's latest version (or the last good one if the
  // latest is halted), keyed by the copy's key (see freeKey). Disabled projects run nowhere.
  desiredFor(machine, successor = null) {
    const out = {};
    for (const p of this.projects.values()) {
      if (!p.enabled) continue;
      const v = p.halted ? p.stable : p.version;
      if (v == null) continue;
      const spec = this.version(p.name, v);
      for (const x of this.copiesOn(p.name, machine)) {
        // A successor doesn't build copies that are moving off the machine.
        if (successor && x.leaving) continue;
        out[x.key] = { v, ...this.composeFor(p.name, v, spec, x), files: { ...spec.files, ".env": this.envFile(p.name, spec, x, v) }, app: p.name, replica: x.replica,
          ...(spec.policy.ready ? { ready: spec.policy.ready } : {}) };
      }
    }
    return out;
  }

  // The key machine n knows copy k of a project by: stored with the copy when it was placed, never recomputed.
  keyOn(name, machine, k) {
    return this.copiesOn(name, machine).find((x) => x.replica === k)?.key ?? name;
  }

  // The key for a new copy of a project on a machine: the plain name when no copy there has it (the usual, only copy,
  // so the key agents always used), else <name>-r<k>. Once given, a copy keeps its key while it stays on the machine,
  // so other copies arriving or leaving never rename it. The plain name isn't reused for a different replica within
  // 10 minutes of being freed there: an agent from before keys were stable could mistake the new copy for the old one.
  freeKey(name, machine, k, now) {
    const used = new Set(this.copiesOn(name, machine).map((x) => x.key));
    const freedAt = this.plainDrops.get(`${name}|${machine}`);
    if (!used.has(name) && !(freedAt && now - freedAt.at < 10 * 60_000 && freedAt.replica !== k)) return name;
    return `${name}-r${k}`;
  }

  // What a copy runs: its spec as it is, or, for a copy whose published ports were moved (it shares its machine with
  // another copy of the same project), the compose file with those ports and the port the router dials along with them.
  composeFor(name, v, spec, x) {
    if (!x.ports) return { compose: spec.compose, port: spec.port };
    const key = `${name}@${v}|${JSON.stringify(x.ports)}`;
    if (!this.remapped.has(key)) this.remapped.set(key, remapCompose(spec.compose, x.ports));
    return { compose: this.remapped.get(key), port: x.ports[spec.port] ?? spec.port };
  }

  runsOn(name, machine) {
    return this.copiesOn(name, machine).length > 0;
  }

  // A new version becomes stable once every copy of it is healthy. It's halted (its copies go back to the stable
  // version) only when it failed on 2 or more servers, on its only server, or on one while healthy nowhere for
  // FAIL_MS: one sick server doesn't stop a rollout.
  settle(now) {
    const up = this.liveMachines(now);
    for (const p of this.projects.values()) {
      if (!p.enabled || p.halted || p.stable === p.version) continue;
      const seen = this.copiesOf(p.name).filter((x) => !x.leaving && up.has(x.machine)).map((x) => ({ x, st: up.get(x.machine).status?.[x.key] }));
      if (!seen.length) continue;
      const mine = seen.filter((o) => o.st?.v === p.version);
      const failedOn = [...new Set(mine.filter((o) => o.st.s === "failed").map((o) => o.x.machine))];
      const healthy = mine.some((o) => o.st.s === "healthy" && o.st.r !== false);
      const key = `${p.name}@${p.version}`;
      if (failedOn.length && !healthy) {
        if (!this.aloneSince.has(key)) this.aloneSince.set(key, now);
      } else this.aloneSince.delete(key);
      let halt = null;
      if (failedOn.length >= 2) halt = `it failed on servers ${failedOn.join(" and ")}`;
      else if (failedOn.length && seen.length === 1) halt = `it failed on server ${failedOn[0]}, its only server`;
      else if (failedOn.length && now - this.aloneSince.get(key) >= FAIL_MS) halt = `it failed on server ${failedOn[0]} and has been healthy nowhere for 10 minutes`;
      if (halt) {
        const e = mine.find((o) => o.st.s === "failed")?.st.e;
        p.halted = `${halt}: ${String(e || "failed").split("\n").pop()}`.slice(0, 600);
        this.aloneSince.delete(key);
        this.alert("halted", { app: p.name }, `${p.name} version ${p.version} was halted (its copies go back to version ${p.stable ?? "none"}): ${halt}`, now);
      } else if (seen.every((o) => o.st?.v === p.version && o.st.s === "healthy" && o.st.r !== false)) p.stable = p.version;
      else continue;
      p.updated = now;
      this.saveProject(p);
    }
  }

  // ---- placement ----
  // A project with N replicas has N copies, numbered 1 to N. A new copy goes to the machine with the most room: the
  // least CPU and memory in use (from its latest metrics) and the fewest copies already placed on it, machines without
  // a copy of the same project first; so with more replicas than machines, some machines run two or more. A copy
  // stays where it is until its machine is gone; then it moves to the best machine left, keeping its number (and so
  // its URL, <project>-<k>); lowering N removes the highest numbers. A second copy on a machine gets its published
  // host ports moved out of the first one's way (ports: { "8080": 30412 }).

  copiesOf(name) {
    return this.copies.get(name) ?? [];
  }

  copiesOn(name, machine) {
    return this.copiesOf(name).filter((x) => x.machine === machine);
  }

  saveCopy(name, x) {
    const list = this.copies.get(name) ?? this.copies.set(name, []).get(name);
    const i = list.findIndex((y) => y.machine === x.machine && y.replica === x.replica);
    if (i >= 0) list[i] = x;
    else list.push(x);
    this.sql.exec("INSERT OR REPLACE INTO copies (name, machine, replica, since, reason, leaving, ports, key, cause) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      name, x.machine, x.replica, x.since, x.reason, x.leaving ? JSON.stringify(x.leaving) : null, x.ports ? JSON.stringify(x.ports) : null,
      x.key ?? name, x.cause ?? null);
  }

  // `cause` says why, for the events (removed, replicas, gone, moved, cancelled, disabled, retired).
  dropCopy(name, machine, replica, cause = null, now = Date.now()) {
    const list = this.copies.get(name);
    if (list) {
      const i = list.findIndex((y) => y.machine === machine && y.replica === replica);
      if (i >= 0) {
        if (list[i].key === name) this.plainDrops.set(`${name}|${machine}`, { at: now, replica });
        list.splice(i, 1);
      }
      if (!list.length) this.copies.delete(name);
    }
    // A replica left with no copy, other than by its app shrinking or going away, is missing from now on (transitions).
    if (!["replicas", "removed", "disabled"].includes(cause) && !(this.copies.get(name) ?? []).some((y) => y.replica === replica)) {
      this.missingAt.set(`${name}|${replica}`, now);
    }
    this.sql.exec("DELETE FROM copies WHERE name = ? AND machine = ? AND replica = ?", name, machine, replica);
    if (cause) this.logEvent("drop", { app: name, replica, machine, cause, t: now });
  }

  // What a copy takes on its machine: its project's claims, with the ports that were moved for it.
  copyClaims(name, x) {
    const p = this.projects.get(name);
    const spec = p && this.version(name, p.version);
    return (spec?.claims ?? []).map((c) => {
      const m = c.match(/^port (\d+)$/);
      return m && x.ports?.[m[1]] ? `port ${x.ports[m[1]]}` : c;
    });
  }

  // How busy a machine is, for choosing between them; lower is better. Without metrics (an agent that's just
  // started) it counts as half busy, so a machine that's measured and quiet wins.
  load(machine, placedCount, now) {
    const m = this.liveMetrics.get(machine);
    const fresh = m && now - m.t < 3 * this.liveMs;
    const cpu = fresh ? m.h.cpu : 50;
    const mem = fresh && m.h.memTotal ? (100 * m.h.memUsed) / m.h.memTotal : 50;
    return { score: cpu + mem + 30 * placedCount, reason: fresh ? `cpu ${Math.round(cpu)}%, memory ${Math.round(mem)}%, ${placedCount} other ${placedCount === 1 ? "copy" : "copies"} placed` : "no metrics yet" };
  }

  place(now) {
    const up = this.liveMachines(now); // machine -> its newest live run
    let changed = false;
    // After a (re)start the first machine to check in would get every replica, because it's the only one with
    // metrics; wait until every machine that's up has reported some (or 3 minutes).
    const measured = (machine) => now - (this.liveMetrics.get(machine)?.t ?? 0) < 3 * this.liveMs;
    const arrived = now - this.settleAt > ARRIVAL_MS || (up.size >= this.expectedMachines() && [...up.keys()].every(measured));
    const counts = new Map(); // machine -> copies placed on it
    for (const [name, list] of this.copies) {
      // Projects that are gone or disabled need no copies; neither do machines that have gone, nor replicas
      // numbered above the count (it was lowered), along with any copy of theirs being moved.
      const p = this.projects.get(name);
      const spec = p ? this.version(name, p.version) : null;
      const replicas = spec?.replicas ?? 1;
      for (const x of [...list]) {
        if (!list.includes(x)) continue; // dropped meanwhile (a cancelled move's new copy)
        const extra = x.replica > replicas;
        const lost = Boolean(p?.enabled) && !extra && this.goneCopy(x, up, now);
        const why = !p ? "removed" : !p.enabled ? "disabled" : extra ? "replicas" : lost ? "gone" : null;
        if (why === "gone") {
          const left = [...this.runs.values()].filter((r) => r.machine === x.machine).sort((a, b) => b.seen - a.seen)[0]?.left;
          this.alert("grace", { machine: x.machine }, left ? `server ${x.machine} was stopped before it could hand over: its copies are placed elsewhere`
            : `server ${x.machine} is gone and no replacement took its slot in time: its copies are placed elsewhere`, now);
        }
        if (why) {
          this.dropCopy(name, x.machine, x.replica, why, now);
          changed = true;
          continue;
        }
        if (x.leaving) {
          const outcome = this.moveOutcome(name, x, up, now);
          if (outcome === "done") {
            this.dropCopy(name, x.machine, x.replica, null, now);
            this.logEvent("move-done", { app: name, replica: x.replica, machine: x.machine, other: x.leaving.to, cause: x.leaving.cause ?? "hand", t: now });
            changed = true;
          } else if (outcome?.cancel) {
            const to = x.leaving.to;
            if (!outcome.noNew) this.dropCopy(name, to, x.replica, null, now);
            if (outcome.failed) {
              this.failedOn.set(`${name}|${to}`, now + FAILED_ON_MS);
              if (p) this.noteVersionFail(name, p.halted ? p.stable : p.version, to, now);
            }
            this.logEvent("move-cancel", { app: name, replica: x.replica, machine: x.machine, other: to, cause: x.leaving.cause ?? "hand", detail: outcome.cancel, t: now });
            x.leaving = null;
            this.saveCopy(name, x);
            counts.set(x.machine, (counts.get(x.machine) ?? 0) + 1);
            changed = true;
          }
          continue;
        }
        counts.set(x.machine, (counts.get(x.machine) ?? 0) + 1);
      }
    }
    for (const p of [...this.projects.values()].sort((a, b) => a.name.localeCompare(b.name))) {
      if (!p.enabled) continue;
      const spec = this.version(p.name, p.version);
      const want = spec?.replicas ?? 1;
      // Each number from 1 to the count needs a copy that's staying: a move's new copy counts, its old one doesn't.
      const have = new Set(this.copiesOf(p.name).filter((x) => !x.leaving).map((x) => x.replica));
      this.blocked.delete(p.name);
      if (!arrived) continue;
      const missing = [];
      for (let k = 1; k <= want; k++) if (!have.has(k)) missing.push(k);
      if (!missing.length) {
        this.deferSince.delete(p.name);
        continue;
      }
      for (const k of missing) {
        const best = this.bestMachine(p.name, up, counts, now);
        if (!best) {
          if (up.size) this.blocked.set(p.name, this.whyBlocked(p.name, spec, up));
          break;
        }
        if (best.doubling && want > 1 && this.deferDoubling(p.name, up, now)) {
          this.blocked.set(p.name, "waiting for a server that's starting, rather than running two copies on one");
          break;
        }
        const copy = this.newCopy(p.name, spec, k, best, now, best.reason, this.everPlaced(p.name, k) ? "replace" : "new");
        this.saveCopy(p.name, copy);
        this.logEvent("place", { app: p.name, replica: k, machine: best.machine, cause: copy.cause, detail: copy.reason, t: now });
        counts.set(best.machine, (counts.get(best.machine) ?? 0) + 1);
        changed = true;
      }
    }
    return changed;
  }

  // A machine counts as gone once it's been silent past the liveness window, but never just after a (re)start or a gap
  // in check-ins: then every machine looks down until it has checked in again. (For a move's destination.)
  gone(machine, up, now) {
    return !up.has(machine) && now - this.settleAt > Math.max(this.liveMs, SETTLE_MS) && !this.settling(now);
  }

  // A copy's machine counts as gone once it's been silent for three check-in intervals (at least 5 minutes) and its slot
  // isn't held for its pool's next machine (which inherits the copy). A machine whose run said it's leaving is gone at
  // once. Never while settling.
  goneCopy(x, up, now) {
    if (up.has(x.machine)) return false;
    const last = [...this.runs.values()].filter((r) => r.machine === x.machine).sort((a, b) => b.seen - a.seen)[0];
    if (last?.left) return true; // it said so itself: no need to wait out a gap
    if (this.settling(now) || now - this.settleAt <= Math.max(this.liveMs, SETTLE_MS)) return false;
    if (now - this.lastSeenOf(x.machine) < Math.max(3 * this.liveMs, 5 * MIN)) return false;
    return !this.heldBy(x.machine, now);
  }

  // Whether replica k of an app has had a copy before (a copy placed for it now replaces one, rather than adding one).
  everPlaced(name, k) {
    return this.all("SELECT 1 FROM events WHERE app = ? AND replica = ? AND kind = 'place' LIMIT 1", name, k).length > 0;
  }

  // ---- the transition budget ----
  // A replica is in transition while it's being moved (two copies), lost its last copy less than 30 minutes ago and has
  // none yet (countMissing: one that never had a copy is waiting for room, not changing), sits on a machine that
  // isn't live, isn't healthy on its machine's newest run (for up to 30 minutes: longer is a broken copy, not a change,
  // and mustn't freeze its app), or sits on a run that's leaving (handing over, or moving its copies out). Every
  // voluntary change of an app (a move, an eviction, a handover) waits while its app has `budget` replicas in transition.
  transitions(name, now, { countMissing = true, countLost = true, exclude = null, excludeReplica = null } = {}) {
    const p = this.projects.get(name);
    const spec = p && this.version(name, p.version);
    if (!spec) return 0;
    const up = this.liveMachines(now);
    let n = 0;
    for (let k = 1; k <= spec.replicas; k++) {
      if (k === excludeReplica) continue;
      const copies = this.copiesOf(name).filter((x) => x.replica === k);
      const key = `${name}|${k}`;
      if (!copies.length) {
        if (countMissing && now - (this.missingAt.get(key) ?? -Infinity) < 30 * MIN) n++;
        continue;
      }
      this.missingAt.delete(key);
      if (copies.some((x) => x.leaving)) {
        n++;
        continue;
      }
      const x = copies[0];
      const run = up.get(x.machine);
      if (!run) { // its machine is silent
        if (countLost) n++;
        continue;
      }
      if (!this.copyHealthy(name, x, run)) {
        const since = this.unhealthySince.get(key) ?? this.unhealthySince.set(key, now).get(key);
        if (now - since < 30 * MIN) n++;
        continue;
      }
      this.unhealthySince.delete(key);
      if ((run.handover || run.evict) && run.id !== exclude) n++; // leaving now (a drain is only a request: it waits its turn)
    }
    return n;
  }

  // How many replicas of an app may be in transition at once: a fifth of them, at least one.
  budget(name) {
    const p = this.projects.get(name);
    const spec = p && this.version(name, p.version);
    return spec ? Math.max(1, Math.ceil(0.2 * spec.replicas)) : 1;
  }

  // `exclude`: a run whose own departure doesn't count (moving its copies out is that departure, not another change);
  // `excludeReplica`: a replica whose own change is the one asked about (a failing copy isn't serving anyway).
  mayTransition(name, now, exclude = null, excludeReplica = null) {
    return this.transitions(name, now, { exclude, excludeReplica }) < this.budget(name);
  }

  // When a copy was last (re)started where it runs: the run's own report of it, else the copy's placement, else the run's start.
  builtAt(x, run) {
    const t0 = Number(run?.status?.[x.key]?.t0);
    return Math.max(x.since, Number.isFinite(t0) && t0 > 0 ? t0 : run?.started ?? 0);
  }

  // The pool a machine counts in for spreading: its pool, or itself for a standalone host.
  poolOf(machine, up) {
    return up.get(machine)?.pool ?? `#${machine}`;
  }

  // Room left on a machine before it's too busy to take more, from its last 3 minutes (half busy without figures).
  headroom(machine, now) {
    const list = (this.samples.get(machine) ?? []).filter((x) => now - x.t <= 3 * MIN);
    const m = this.liveMetrics.get(machine);
    let cpu = 50;
    let mem = 50;
    if (list.length) {
      cpu = list.reduce((a, x) => a + x.cpu, 0) / list.length;
      mem = list.reduce((a, x) => a + x.mem, 0) / list.length;
    } else if (m && now - m.t < 3 * this.liveMs && m.h.memTotal) {
      cpu = Number(m.h.cpu) || 0;
      mem = (100 * (m.h.memUsed || 0)) / m.h.memTotal;
    }
    return { cpu, mem, h: Math.min(ROOM_CPU - cpu, ROOM_MEM - mem), measured: list.length > 0 || Boolean(m) };
  }

  // When the control plane wants a run to leave: some time between 4 h and 5 h 10 min after its start (a fixed spread per
  // run), always at least 45 minutes before its deadline. Only runs that know their deadline (LIFETIME_MIN) and belong to
  // a pool (so a replacement can be started) get one.
  dueAt(r) {
    if (!r.deadline || !r.pool) return null;
    let h = 2166136261;
    for (const ch of r.id) h = Math.imul(h ^ ch.charCodeAt(0), 16777619) >>> 0;
    return Math.min(r.started + (240 + (h % 70)) * MIN, r.deadline - 45 * MIN);
  }

  // Put off doubling an app up for up to 8 minutes while a server that doesn't run it is starting (or one is being
  // started): it'll have room in a moment.
  deferDoubling(name, up, now) {
    const starting = [...this.starts].some(([n, at]) => now - at < START_WAIT_MS && !up.has(n));
    const notReady = [...up.values()].some((r) => !r.ready && !this.copiesOn(name, r.machine).length);
    if (!starting && !notReady) {
      this.deferSince.delete(name);
      return false;
    }
    const since = this.deferSince.get(name) ?? this.deferSince.set(name, now).get(name);
    return now - since < 8 * MIN;
  }

  // A copy is healthy on a run when the run reports it running the wanted version, answering, and (for an app that
  // declares a readiness path) ready.
  copyHealthy(name, x, run, v = null) {
    const st = run?.status?.[x.key];
    if (!st || st.s !== "healthy" || st.r === false) return false;
    if (v == null) {
      const p = this.projects.get(name);
      v = p?.halted ? p.stable : p?.version;
    }
    return st.v === v;
  }

  // Whether some other replica of the app has a healthy copy right now: a slow new copy is only given up on then (in an
  // outage, nothing is ready, and waiting is all there is to do).
  anyReady(name, up, exceptReplica = null) {
    return this.copiesOf(name).some((y) => !y.leaving && y.replica !== exceptReplica && this.copyHealthy(name, y, up.get(y.machine)));
  }

  // How a move stands (x is its old copy): "done" once the new copy is healthy and DNS points at it; { cancel } when the
  // new copy failed, its machine is gone, or it took too long while another copy of the app is ready; null meanwhile.
  // A cancelled move keeps the old copy: a working copy is never dropped for one that doesn't work.
  moveOutcome(name, x, up, now) {
    const lv = x.leaving;
    const nx = this.copiesOf(name).find((y) => y.machine === lv.to && y.replica === x.replica && !y.leaving);
    if (!nx) return { cancel: "its new copy is gone", noNew: true };
    const run = up.get(lv.to);
    if (this.copyHealthy(name, nx, run)) {
      if (!lv.okAt) {
        lv.okAt = now;
        this.saveCopy(name, x);
      }
      const p = this.projects.get(name);
      const spec = this.version(name, p.halted ? p.stable : p.version);
      if (!spec?.port || this.env.DNS === "off" || this.dnsSyncedAt >= lv.okAt || now - lv.okAt > DNS_WAIT_MAX_MS) return "done";
      return null;
    }
    if (this.gone(lv.to, up, now)) return { cancel: `server ${lv.to} went away`, failed: false };
    const st = run?.status?.[nx.key];
    if (st?.s === "failed") return { cancel: `it failed on server ${lv.to}: ${String(st.e ?? "").split("\n").pop()}`.slice(0, 300), failed: true };
    const limit = lv.ext ? MOVE_TIMEOUT_EXT_MS : MOVE_TIMEOUT_MS;
    if (now - lv.at > limit) {
      if (!lv.ext && st?.s === "applying") {
        lv.ext = true;
        this.saveCopy(name, x);
        return null;
      }
      if (this.anyReady(name, up, x.replica)) return { cancel: `its new copy on server ${lv.to} wasn't healthy after ${Math.round(limit / 60_000)} min`, failed: true };
    }
    return null;
  }

  // A copy for machine `best` (from bestMachine). Published host ports that something on the machine already uses (a
  // copy of the same project, or another project) are moved aside; the reason says so.
  newCopy(name, spec, k, best, now, reason = best.reason, cause = "new") {
    const x = { machine: best.machine, replica: k, since: now, reason, leaving: null, ports: null, key: this.freeKey(name, best.machine, k, now), cause };
    const ports = this.movedPorts(name, spec, best.machine, k);
    const moved = Object.entries(ports).filter(([a, b]) => Number(a) !== b);
    if (moved.length) {
      x.ports = ports;
      x.reason = `${best.doubling ? `copy ${best.doubling + 1} on this machine, every machine having one; ` : ""}port${moved.length > 1 ? "s" : ""} ${moved.map(([a, b]) => `${a}→${b}`).join(", ")} moved aside; ${reason}`;
    } else if (best.doubling) x.reason = `copy ${best.doubling + 1} on this server, every server having one; ${reason}`;
    return x;
  }

  // Why no machine could take a copy, for the project's page.
  whyBlocked(name, spec, up) {
    const empty = [...up.keys()].filter((m) => !this.copiesOn(name, m).length);
    if (empty.length) {
      const clash = empty.map((m) => this.clash(name, m)).find(Boolean);
      return clash ? `every other server already has an app using ${clash.what} (${clash.other})` : "no server can take it";
    }
    return `every server already runs it, and it can't run twice on one server (${spec?.nodup ?? "its compose file"})`;
  }

  // Host ports for a copy of a project on a machine: each of the project's published ports as it is when nothing on
  // the machine uses it, else moved to one that's free (30000-54999). Fixed once chosen, so the copy's containers
  // aren't recreated for a port change.
  movedPorts(name, spec, machine, k) {
    const taken = new Set(RESERVED_PORTS);
    for (const [other, list] of this.copies) {
      for (const x of list) {
        if (x.machine !== machine) continue;
        for (const c of this.copyClaims(other, x)) {
          const m = c.match(/^port (\d+)$/);
          if (m) taken.add(Number(m[1]));
        }
      }
    }
    const ports = {};
    if (spec?.fixed) return ports; // these can't be moved; clash() keeps such a copy off a machine where they're taken
    for (const c of spec?.claims ?? []) {
      const m = c.match(/^port (\d+)$/);
      if (!m) continue;
      const P = Number(m[1]);
      let q = P;
      if (taken.has(q)) {
        q = 30000 + ((P * 131 + k * 7919) % 25000);
        while (taken.has(q) || Object.values(ports).includes(q)) q = q + 1 < 55000 ? q + 1 : 30000;
      }
      ports[P] = q;
    }
    return ports;
  }

  // What keeps a copy of a project off a machine: another copy placed there (of this project or another) uses one of
  // its container names, or one of its published ports when this project's ports can't be moved. { what, other }, or null.
  clash(name, machine) {
    const p = this.projects.get(name);
    const spec = p && this.version(name, p.version);
    const mine = spec?.claims ?? [];
    if (!mine.length) return null;
    const matters = (c) => c.startsWith("container name ") || Boolean(spec.fixed);
    for (const [other, list] of this.copies) {
      for (const x of list) {
        if (x.machine !== machine) continue;
        const what = this.copyClaims(other, x).find((c) => matters(c) && mine.includes(c));
        if (what) return { what, other };
      }
    }
    return null;
  }

  // The machine for a copy of an app. Out: machines where it would clash, where a second copy can't run, that are
  // leaving (drain, handover, evict, due), mid-handover, quarantined, or where a move of the app just failed. Machines
  // that will be stopped soon (under 30 minutes left) or are hot (over HOT_CPU / HOT_MEM on their last 3 minutes) are
  // passed over unless nothing else is left. Then: ready first, fewest copies of the app on the machine (a second copy
  // only where every machine has one), then in its pool, then machines with room (under ROOM_CPU / ROOM_MEM) before
  // busy ones, fewest copies placed, most room.
  bestMachine(name, up, counts, now) {
    const p = this.projects.get(name);
    const spec = p && this.version(name, p.version);
    const mine = new Map(); // machine -> copies of this app there (ones moving away included: their ports are still in use)
    for (const x of this.copiesOf(name)) mine.set(x.machine, (mine.get(x.machine) ?? 0) + 1);
    const inPool = new Map(); // pool -> staying copies of this app there
    for (const x of this.copiesOf(name)) if (!x.leaving) inPool.set(this.poolOf(x.machine, up), (inPool.get(this.poolOf(x.machine, up)) ?? 0) + 1);
    const runsOnSlot = new Map();
    for (const r of this.liveRuns(now)) runsOnSlot.set(r.machine, (runsOnSlot.get(r.machine) ?? 0) + 1);
    const cands = [];
    for (const r of up.values()) {
      const doubling = mine.get(r.machine) ?? 0;
      if (this.clash(name, r.machine) || (doubling && spec?.nodup)) continue;
      if (r.drain || r.handover || r.retire || r.evict || (runsOnSlot.get(r.machine) ?? 0) > 1) continue;
      if ((r.quarantine ?? 0) > now || this.failedHere(name, r.machine, now)) continue;
      const due = this.dueAt(r);
      if (due && now >= due) continue;
      const pool = this.poolOf(r.machine, up);
      const room = this.headroom(r.machine, now);
      const placed = counts.get(r.machine) ?? 0;
      cands.push({
        machine: r.machine, ready: r.ready, doubling, pool: inPool.get(pool) ?? 0, placed, h: room.h, roomy: room.h > 0,
        lifeOk: !r.deadline || r.deadline - now >= 30 * MIN, hot: room.cpu > HOT_CPU || room.mem > HOT_MEM,
        reason: `${room.measured ? `cpu ${Math.round(room.cpu)}%, memory ${Math.round(room.mem)}%` : "no metrics yet"}, ${placed} other ${placed === 1 ? "copy" : "copies"} placed`,
      });
    }
    let list = cands.filter((c) => c.lifeOk && !c.hot);
    if (!list.length) list = cands.filter((c) => c.lifeOk);
    if (!list.length) list = cands;
    return list.sort((a, b) => b.ready - a.ready || a.doubling - b.doubling || a.pool - b.pool || b.roomy - a.roomy || a.placed - b.placed || b.h - a.h)[0] ?? null;
  }

  liveMachines(now) {
    const up = new Map();
    for (const r of this.liveRuns(now)) if ((up.get(r.machine)?.started ?? -1) < r.started) up.set(r.machine, r);
    return up;
  }

  placementCounts() {
    const counts = new Map();
    for (const list of this.copies.values()) for (const x of list) if (!x.leaving) counts.set(x.machine, (counts.get(x.machine) ?? 0) + 1);
    return counts;
  }

  // Move one copy of a project off a machine (replica k, or the first copy there): the new copy is placed first, and
  // the old one is dropped once the new one is healthy (see place()). `to` picks the destination; otherwise it's the
  // machine with the most room.
  move(name, from, to, replica = null, { cause = "hand", why = null } = {}) {
    const p = this.project(name);
    const now = Date.now();
    const onFrom = this.copiesOn(name, from);
    const x = replica != null ? onFrom.find((y) => y.replica === Number(replica)) : onFrom.find((y) => !y.leaving) ?? onFrom[0];
    if (!x) throw new HttpError(400, replica != null ? `replica ${replica} of ${name} isn't on server ${from}` : `${name} isn't placed on server ${from}`);
    if (x.leaving) throw new HttpError(409, `replica ${x.replica} of ${name} is already moving from server ${from} to ${x.leaving.to}`);
    // A copy that's the new end of a move still under way (its old copy is still serving): that move is redirected
    // instead of chained, so the replica never ends up with two copies.
    const origin = this.copiesOf(name).find((y) => y.replica === x.replica && y.leaving?.to === from);
    const spec = this.version(name, p.version);
    const up = this.liveMachines(now);
    let dest;
    if (origin && to && Number(to) === origin.machine) { // back where it came from: the move is called off
      this.dropCopy(name, from, x.replica, null, now);
      this.logEvent("move-cancel", { app: name, replica: x.replica, machine: origin.machine, other: from, cause: origin.leaving.cause ?? "hand", detail: `sent back to server ${origin.machine}${why ? `: ${why}` : " by hand"}`, t: now });
      this.saveCopy(name, { ...origin, leaving: null });
      return this.describe(p);
    }
    if (to) {
      const m = Number(to);
      if (!up.has(m)) throw new HttpError(400, `server ${to} isn't up`);
      if (m === from) throw new HttpError(400, `replica ${x.replica} of ${name} is on server ${to} already`);
      const doubling = this.copiesOn(name, m).length;
      if (doubling && spec?.nodup) throw new HttpError(409, `server ${to} already runs ${name}, which can't run twice on one server (${spec.nodup})`);
      const clash = this.clash(name, m);
      if (clash) throw new HttpError(409, `server ${to} already has ${clash.other}, which uses ${clash.what} too`);
      dest = { machine: m, doubling, reason: `moved here from server ${from} ${why ? `automatically: ${why}` : "by hand"}` };
    } else {
      const best = this.bestMachine(name, new Map([...up].filter(([m]) => m !== from && m !== origin?.machine && !this.failedHere(name, m, now))), this.placementCounts(), now);
      if (!best) throw new HttpError(409, `no other server is up for ${name}`);
      dest = { ...best, reason: `moved here from server ${from}${why ? ` automatically: ${why}` : ""}: ${best.reason}` };
    }
    if (origin) {
      this.dropCopy(name, from, x.replica, null, now);
      this.saveCopy(name, this.newCopy(name, spec, x.replica, dest, now, dest.reason.replace(`from server ${from}`, `from server ${origin.machine} (instead of ${from})`), `move:${cause}`));
      const { lost, fromRun } = origin.leaving;
      this.saveCopy(name, { ...origin, leaving: { to: dest.machine, at: now, cause, ...(lost ? { lost, fromRun } : {}) } });
      this.logEvent("move-start", { app: name, replica: x.replica, machine: origin.machine, other: dest.machine, cause, detail: `redirected from server ${from}: ${why ?? dest.reason}`, t: now });
      return this.describe(p);
    }
    this.saveCopy(name, this.newCopy(name, spec, x.replica, dest, now, dest.reason, `move:${cause}`));
    this.saveCopy(name, { ...x, leaving: { to: dest.machine, at: now, cause } });
    this.logEvent("move-start", { app: name, replica: x.replica, machine: from, other: dest.machine, cause, detail: why ?? dest.reason, t: now });
    return this.describe(p);
  }

  failedHere(name, machine, now) {
    return (this.failedOn.get(`${name}|${machine}`) ?? 0) > now;
  }

  // Move every copy off a machine (it's hot, or about to be removed). Apps already changing as much as they may (see
  // transitions) are left for later unless forced.
  evict(machine, { force = false } = {}) {
    const now = Date.now();
    const moved = [];
    const failed = [];
    for (const [name, list] of this.copies) {
      for (const x of [...list]) {
        if (x.machine !== machine || x.leaving) continue;
        if (!force && !this.mayTransition(name, now)) {
          failed.push(`${name} (replica ${x.replica}): it's already changing as much as it may; try again in a few minutes, or force it`);
          continue;
        }
        try {
          this.move(name, machine, null, x.replica, { cause: "evict" });
          moved.push(`${name} (replica ${x.replica})`);
        } catch (e) {
          failed.push(`${name} (replica ${x.replica}): ${e.message}`);
        }
      }
    }
    return { machine, moved, failed };
  }

  // ---- automatic rebalancing ----
  // Every automatic move has a cause: a hot server (the app making it hot moves), a copy failing on one server while
  // its app is healthy elsewhere, or a server running two copies of an app while another runs none (un-stack). Moves
  // in flight fleet-wide (any cause, by hand included) stay
  // under min(8, 10% of the settled servers), the last slot kept for a hot server; a server takes one arrival at a
  // time. Hot and failed-copy moves feed a breaker that pauses all of this for an hour when they look like thrashing.

  // A run's last 5 minute summaries, if it sent at least 4: hot when 4 are over HOT_CPU (CPU minus steal: time the
  // host took away isn't the apps') or HOT_MEM. { cpu, mem, kind: "cpu" | "mem", score, list }, or null.
  hotness(r, now) {
    const list = (this.minutes.get(r.id) ?? []).filter((m) => now - m.t <= 7 * MIN).slice(-5);
    if (list.length < 4) return null;
    const over = list.filter((m) => m.cpu - m.steal > HOT_CPU || m.mem > HOT_MEM).length;
    if (over < 4) return null;
    const avg = (f) => list.reduce((a, m) => a + f(m), 0) / list.length;
    const cpu = avg((m) => m.cpu - m.steal);
    const mem = avg((m) => m.mem);
    const cpuOver = list.filter((m) => m.cpu - m.steal > HOT_CPU).length;
    const memOver = list.filter((m) => m.mem > HOT_MEM).length;
    const kind = cpuOver !== memOver ? (memOver > cpuOver ? "mem" : "cpu") : mem - HOT_MEM > cpu - HOT_CPU ? "mem" : "cpu";
    return { cpu, mem, kind, score: Math.max(cpu - HOT_CPU, mem - HOT_MEM), list };
  }

  // What one copy of each app on a run takes of it over those minutes: cpu and mem as % of the host, and mem in bytes.
  // Copies of one app on a server share its figures (a copy moving away included: it still runs).
  appShares(r, list) {
    const cores = r.cpus || DEFAULT_CPUS;
    const memTotal = list.at(-1)?.memTotal || 1;
    const out = new Map();
    for (const [name, copies] of this.copies) {
      const all = copies.filter((x) => x.machine === r.machine);
      const staying = all.filter((x) => !x.leaving);
      const rows = list.map((m) => m.a?.[name]).filter(isMap);
      if (!staying.length || !rows.length) continue;
      const cpu = rows.reduce((a, x) => a + (Number(x.cpu) || 0), 0) / rows.length / cores / all.length;
      const memBytes = rows.reduce((a, x) => a + (Number(x.mem) || 0), 0) / rows.length / all.length;
      out.set(name, { cpu, mem: (100 * memBytes) / memTotal, memBytes, copies: staying });
    }
    return out;
  }

  hasRoom(machine, now) {
    const m = this.liveMetrics.get(machine);
    if (!m || now - m.t > 3 * this.liveMs || !m.h.memTotal) return false;
    return m.h.cpu < ROOM_CPU && (100 * m.h.memUsed) / m.h.memTotal < ROOM_MEM;
  }

  // Why a copy mustn't be moved automatically now, or null: an app that isn't running, or whose latest version isn't
  // settled.
  neverAuto(name) {
    const p = this.projects.get(name);
    if (!p?.enabled || !this.version(name, p.version)) return "it isn't running";
    if (p.stable !== p.version) return "its latest version hasn't settled yet";
    return null;
  }

  lastAutoMove(name) {
    return Number(this.autoMoved.get(name) ?? 0);
  }

  autoMovedAt(name, now) {
    this.autoMoved.set(name, now);
    this.setSetting("auto_moved", JSON.stringify(Object.fromEntries([...this.autoMoved].filter(([, t]) => now - t < PROJECT_COOLDOWN_MS))));
  }

  // Hot and failed-copy moves started in the last `span` (from the events).
  autoMoves(now, span) {
    return this.all("SELECT app, replica, machine, other, t, cause FROM events WHERE kind = 'move-start' AND cause IN ('hot', 'failed') AND t > ?", now - span);
  }

  // When a hot move of replica k may come again: 30 minutes after its first hot move in 24 hours, an hour after its
  // second; never after a third (pinned: null).
  hotBackoff(name, k, moves24) {
    const mine = moves24.filter((e) => e.cause === "hot" && e.app === name && e.replica === k).map((e) => e.t);
    if (mine.length >= 3) return null;
    return mine.length ? Math.max(...mine) + Math.min(240, 30 * 2 ** (mine.length - 1)) * MIN : 0;
  }

  // The thrash breaker, over hot and failed-copy moves only: why this move would trip it (more than 3 between one pair
  // of servers in 2 hours, more than 6 in an hour, or a replica's second in 2 hours), or null.
  thrash(name, k, from, to, moves2h, now) {
    if (moves2h.filter((e) => (e.machine === from && e.other === to) || (e.machine === to && e.other === from)).length >= 3) {
      return `it would be the 4th automatic move between servers ${from} and ${to} in 2 hours`;
    }
    if (moves2h.filter((e) => now - e.t < 3600_000).length >= 6) return "it would be the 7th automatic move in an hour";
    if (moves2h.some((e) => e.app === name && e.replica === k)) return `${name} replica ${k} would move automatically a second time in 2 hours`;
    return null;
  }

  paused(now) {
    return Number(this.settings.get("rebalance_paused_until") ?? 0) > now;
  }

  pause(why, now) {
    this.setSetting("rebalance_paused_until", now + PAUSE_MS);
    this.logEvent("pause", { cause: "thrash", detail: `automatic moves paused for an hour: ${why}`, t: now });
    this.alert("paused", {}, `automatic moves are paused for an hour: ${why}`, now);
  }

  // A note in the events about something the rebalancer saw but didn't act on, once per 30 minutes per subject.
  noteRebalance(subject, text, now, f = {}) {
    if (now - (this.noted.get(subject) ?? 0) < 30 * MIN) return;
    this.noted.set(subject, now);
    this.logEvent("rebalance", { ...f, detail: text, t: now });
  }

  rebalance(now) {
    if (!this.rebalanceOn() || now - this.lastRebalance < 30_000 || this.quiet(now) || this.paused(now)) return;
    this.lastRebalance = now;
    const up = this.liveMachines(now);
    const settled = [...up.values()].filter((r) => r.ready && now - r.started > SETTLED_MS && this.liveMetrics.has(r.machine));
    if (settled.length < 2) return;
    const isSettled = new Set(settled.map((r) => r.machine));
    let inFlight = 0;
    const arriving = new Set();
    for (const list of this.copies.values()) {
      for (const x of list) {
        if (!x.leaving) continue;
        inFlight++;
        arriving.add(x.leaving.to);
      }
    }
    const moves24 = this.autoMoves(now, 24 * 3600_000);
    const ctx = {
      now, up, isSettled, moves24, counts: this.placementCounts(),
      moves2h: moves24.filter((e) => now - e.t < 2 * 3600_000),
      hotRuns: settled.map((r) => ({ r, hot: this.hotness(r, now) })).filter((x) => x.hot).sort((a, b) => b.hot.score - a.hot.score),
      cap: Math.max(1, Math.min(8, Math.ceil(0.1 * settled.length))),
      // Room for one more move of this kind: the last slot is a hot server's while one is hot.
      free: (kind) => inFlight < (kind === "hot" || !ctx.hotRuns.length ? ctx.cap : ctx.cap - 1),
      // Destinations: settled servers other than `from`, with nothing arriving, that pass `ok`.
      dests: (from, ok = () => true) => new Map([...up].filter(([m]) => m !== from && isSettled.has(m) && !arriving.has(m) && ok(m))),
      // Starts a move (the breaker may pause everything instead): true, false, or "paused".
      start: (name, x, dest, cause, why) => {
        if (cause === "hot" || cause === "failed") {
          const t = this.thrash(name, x.replica, x.machine, dest.machine, ctx.moves2h, now);
          if (t) {
            this.pause(t, now);
            return "paused";
          }
        }
        try {
          this.move(name, x.machine, String(dest.machine), x.replica, { cause, why });
        } catch (e) {
          this.noteRebalance(`fail|${name}|${x.replica}`, `couldn't move ${name} replica ${x.replica} off server ${x.machine}: ${e.message}`, now, { app: name, replica: x.replica, machine: x.machine });
          return false;
        }
        const nx = this.copiesOn(name, dest.machine).find((y) => y.replica === x.replica && !y.leaving);
        if (nx) {
          nx.reason = `moved here from server ${x.machine} automatically: ${why}; ${dest.reason}`;
          this.saveCopy(name, nx);
        }
        inFlight++;
        arriving.add(dest.machine);
        ctx.counts.set(dest.machine, (ctx.counts.get(dest.machine) ?? 0) + 1);
        if (cause !== "unstack") this.autoMovedAt(name, now);
        if (cause === "hot" || cause === "failed") ctx.moves2h.push({ app: name, replica: x.replica, machine: x.machine, other: dest.machine, t: now, cause });
        return true;
      },
    };
    // Priority: hot, then failed copies, then un-stacking.
    for (const step of [this.rebalanceHot, this.rebalanceFailed, this.rebalanceUnstack]) {
      if (step.call(this, ctx) === "paused") return;
    }
  }

  // A hot server has the copy taking the most of it moved off (at least 15% of it, else the heat isn't the apps'),
  // once no copy on it is new or unhealthy; to a server that would still have room with it there and took no hot move
  // in 30 minutes. At most 2 moves off a server an hour; still hot 10 minutes after one, it's suspect (not a
  // destination for an hour). A replica moved for heat waits 30 minutes, then an hour, and stays put after 3 in a day.
  // One hot move per COOLDOWN_MS fleet-wide, one per app per PROJECT_COOLDOWN_MS.
  rebalanceHot(ctx) {
    const { now, up } = ctx;
    const cooling = now - Number(this.settings.get("last_hot_move") ?? 0) < COOLDOWN_MS;
    for (const { r, hot } of ctx.hotRuns) {
      const what = `server ${r.machine} is hot (${hot.kind === "cpu" ? `cpu ${Math.round(hot.cpu)}%` : `memory ${Math.round(hot.mem)}%`} over 5 minutes)`;
      const off = ctx.moves24.filter((e) => e.cause === "hot" && e.machine === r.machine && now - e.t < 3600_000);
      if (off.length && now - Math.max(...off.map((e) => e.t)) >= 10 * MIN && !((r.quarantine ?? 0) > now)) {
        this.quarantine(r, "still hot 10 minutes after an app was moved off it", now, { sick: false });
      }
      if (off.length >= 2) {
        this.noteRebalance(`hot2|${r.id}`, `${what}, but 2 apps were moved off it this hour already`, now, { machine: r.machine });
        continue;
      }
      const here = [];
      for (const [name, list] of this.copies) for (const x of list) if (x.machine === r.machine && !x.leaving) here.push({ name, x });
      if (here.some(({ name, x }) => now - this.builtAt(x, r) < this.youngMs() || !this.copyHealthy(name, x, r))) {
        this.noteRebalance(`hotwait|${r.id}`, `${what}, but a copy on it is new or not healthy: waiting`, now, { machine: r.machine });
        continue;
      }
      const shares = this.appShares(r, hot.list);
      const share = (s) => (hot.kind === "cpu" ? s.cpu : s.mem);
      if (Math.max(0, ...[...shares.values()].map(share)) < 15) {
        this.noteRebalance(`hotapps|${r.id}`, `${what}, but no app on it uses 15% of it: nothing to move`, now, { machine: r.machine });
        continue;
      }
      if (cooling || !ctx.free("hot")) { // (notes and suspicion above go on meanwhile)
        this.noteRebalance(`hotcool|${r.id}`, `${what}: waiting, ${cooling ? "a hot move was made less than 10 minutes ago" : "enough moves are under way"}`, now, { machine: r.machine });
        continue;
      }
      const cands = [];
      for (const [name, s] of shares) {
        if (share(s) < 15 || now - this.lastAutoMove(name) < PROJECT_COOLDOWN_MS || !this.mayTransition(name, now)) continue;
        for (const x of s.copies) {
          const at = this.hotBackoff(name, x.replica, ctx.moves24);
          if (at != null && at <= now && !this.neverAuto(name)) cands.push({ name, x, s });
        }
      }
      const pick = cands.sort((a, b) => share(b.s) - share(a.s) || b.x.replica - a.x.replica)[0];
      if (!pick) {
        this.noteRebalance(`hotnone|${r.id}`, `${what}, but the apps using it can't move now (cooldown, budget, backoff or policy)`, now, { machine: r.machine });
        continue;
      }
      const tookHot = new Set(ctx.moves24.filter((e) => e.cause === "hot" && now - e.t < HOT_DEST_MS).map((e) => e.other));
      const fits = (m) => {
        const d = up.get(m);
        const room = this.headroom(m, now);
        const memTotal = this.liveMetrics.get(m)?.h.memTotal || 0;
        const cpu = room.cpu + pick.s.cpu * ((r.cpus || DEFAULT_CPUS) / (d.cpus || DEFAULT_CPUS));
        const mem = room.mem + (memTotal ? (100 * pick.s.memBytes) / memTotal : pick.s.mem);
        return cpu < ROOM_CPU && mem < ROOM_MEM && !this.hotness(d, now);
      };
      const dest = this.bestMachine(pick.name, ctx.dests(r.machine, (m) => !tookHot.has(m) && fits(m)), ctx.counts, now);
      if (!dest) {
        this.noteRebalance(`hotroom|${r.id}`, `${what}, but no server has room for ${pick.name} (${Math.round(share(pick.s))}% of it)`, now, { machine: r.machine, app: pick.name });
        continue;
      }
      const res = ctx.start(pick.name, pick.x, dest, "hot", `${what}; ${pick.name} used ${Math.round(share(pick.s))}% of it`);
      if (res === true) this.setSetting("last_hot_move", now);
      if (res) return res;
    }
    return null;
  }

  // A copy failing (or not ready) for FAIL_MS on a settled server while its app is healthy elsewhere moves once, and
  // its app isn't placed there again for FAILED_ON_MS. A version that failed on 2 servers is the app's fault: its
  // copies stay where they are, and an alert says so.
  rebalanceFailed(ctx) {
    const { now, up } = ctx;
    for (const [name, list] of [...this.copies]) {
      const p = this.projects.get(name);
      if (!p?.enabled) continue;
      const v = p.halted ? p.stable : p.version;
      for (const x of [...list]) {
        if (x.leaving || !ctx.isSettled.has(x.machine)) continue;
        const run = up.get(x.machine);
        const st = run?.status?.[x.key];
        if (!failing(st) || st.v !== v || !this.anyReady(name, up, x.replica)) continue;
        const since = this.failClock(name, x, run, now);
        if (now - since < FAIL_MS) continue;
        this.noteVersionFail(name, v, x.machine, now);
        if ((this.versionFails.get(`${name}@${v}`)?.size ?? 0) >= 2) continue;
        if (!ctx.free("failed")) return null;
        const no = this.neverAuto(name);
        if (no || now - this.lastAutoMove(name) < PROJECT_COOLDOWN_MS || !this.mayTransition(name, now, null, x.replica)) {
          if (no) this.noteRebalance(`failno|${name}|${x.replica}`, `${name} replica ${x.replica} is failing on server ${x.machine}, but it's left there: ${no}`, now, { app: name, replica: x.replica, machine: x.machine });
          continue;
        }
        const dest = this.bestMachine(name, ctx.dests(x.machine), ctx.counts, now);
        if (!dest) {
          this.noteRebalance(`failroom|${name}|${x.replica}`, `${name} replica ${x.replica} is failing on server ${x.machine}, and no other server can take it`, now, { app: name, replica: x.replica, machine: x.machine });
          continue;
        }
        const err = String(st.e ?? "").split("\n").pop().slice(0, 160);
        const res = ctx.start(name, x, dest, "failed", `it ${st.s === "failed" ? "failed" : "wasn't ready"} on server ${x.machine} for ${Math.round((now - since) / MIN)} minutes while healthy elsewhere${err ? ` (${err})` : ""}`);
        if (res === "paused") return res;
        if (res) this.failedOn.set(`${name}|${x.machine}`, now + FAILED_ON_MS);
      }
    }
    return null;
  }

  // A server running two or more copies of an app while a settled server with room runs none: the highest replica
  // there moves, one per app per pass, within its budget. Copies younger than 10
  // minutes are left alone.
  rebalanceUnstack(ctx) {
    const { now, up } = ctx;
    const young = (x) => now - this.builtAt(x, up.get(x.machine)) < this.youngMs();
    for (const [name, list] of [...this.copies]) {
      if (!ctx.free("unstack")) return null;
      if (!this.mayTransition(name, now)) continue;
      const by = new Map();
      for (const x of list) if (!x.leaving && !young(x)) by.set(x.machine, (by.get(x.machine) ?? 0) + 1);
      const from = [...by].filter(([m, n]) => n > 1 && ctx.isSettled.has(m)).sort((a, b) => b[1] - a[1])[0];
      if (!from) continue;
      const x = list.filter((y) => y.machine === from[0] && !y.leaving).sort((a, b) => b.replica - a.replica)[0];
      if (this.neverAuto(name)) continue;
      const dest = this.bestMachine(name, ctx.dests(from[0], (m) => !list.some((y) => y.machine === m) && this.hasRoom(m, now)), ctx.counts, now);
      if (dest) ctx.start(name, x, dest, "unstack", `server ${from[0]} ran ${from[1]} copies of ${name} and server ${dest.machine} none`);
    }
    return null;
  }

  // ---- copy clocks, sick servers, alerts ----

  // What a run's check-in says about its copies: when each was first reported there, last healthy there, and how often
  // each restarted (strikes).
  noteCopies(r, status, now) {
    const rs = this.restarts.get(r.id) ?? this.restarts.set(r.id, { last: {}, log: [] }).get(r.id);
    for (const [key, st] of Object.entries(status)) {
      if (!isMap(st)) continue;
      const k = `${r.id}|${key}`;
      const copy = () => {
        for (const [name, list] of this.copies) for (const x of list) if (x.machine === r.machine && x.key === key) return { app: name, replica: x.replica };
        return { app: null, replica: null };
      };
      if (!this.firstSeen.has(k)) {
        this.firstSeen.set(k, now);
        // A successor building a copy its predecessor serves (after a restart, only builds still under way count).
        const older = this.predecessorOf(r, now);
        if (older && st.s !== "healthy" && older.status?.[key]?.s === "healthy") this.logEvent("copy-start", { ...copy(), machine: r.machine, run: r.id, cause: "successor", t: now });
      }
      const pv = this.lastV.get(k);
      if (st.v != null && pv != null && st.v !== pv) this.logEvent("copy-start", { ...copy(), machine: r.machine, run: r.id, cause: "version", detail: `v${pv} -> v${st.v}`, t: now });
      if (st.v != null) this.lastV.set(k, st.v);
      if (st.s === "healthy" && st.r !== false) this.lastHealthy.set(k, now);
      const n = Number(st.n);
      if (!Number.isFinite(n)) continue;
      if (rs.last[key] != null && n > rs.last[key]) rs.log.push({ t: now, key, k: n - rs.last[key] });
      rs.last[key] = n;
    }
    while (rs.log.length && now - rs.log[0].t > 3600_000) rs.log.shift();
  }

  // Since when each app has had a healthy copy somewhere (checked every 10 s).
  noteReady(now) {
    if (now - this.lastReadyCheck < 10_000) return;
    this.lastReadyCheck = now;
    const up = this.liveMachines(now);
    for (const name of this.projects.keys()) {
      if (this.copiesOf(name).some((x) => !x.leaving && this.copyHealthy(name, x, up.get(x.machine)))) {
        if (!this.readySince.has(name)) this.readySince.set(name, now);
      } else this.readySince.delete(name);
    }
  }

  // When a copy's current failure started, on the control plane's clock: the latest of its last healthy report on its
  // server's run, its placement and its first report there; outage-aware, no earlier than when some copy of its app
  // was last healthy again (in an outage, nothing is ready, and nothing is any one server's fault).
  failClock(name, x, run, now, { outageAware = true } = {}) {
    const k = `${run.id}|${x.key}`;
    const t = Math.max(this.lastHealthy.get(k) ?? 0, x.since, this.firstSeen.get(k) ?? now);
    return outageAware ? Math.max(t, this.readySince.get(name) ?? now) : t;
  }

  noteVersionFail(name, v, machine, now) {
    const key = `${name}@${v}`;
    const set = this.versionFails.get(key) ?? this.versionFails.set(key, new Set()).get(key);
    if (set.has(machine)) return;
    set.add(machine);
    this.saveVersionFails();
    if (set.size >= 2) {
      this.alert("fails-everywhere", { app: name }, `${name} version ${v} failed on servers ${[...set].join(" and ")}: that's the app, not the servers, so its copies aren't moved for failing any more`, now);
    }
  }

  saveVersionFails() {
    this.setSetting("version_fails", JSON.stringify(Object.fromEntries([...this.versionFails].map(([k, set]) => [k, [...set]]))));
  }

  // Something automation gave up on, or a human should see: an event of kind "alert", once per cause and target per
  // 10 minutes.
  alert(cause, f, text, now) {
    const key = `${cause}|${f.app ?? ""}|${f.machine ?? ""}`;
    if (now - (this.alertedAt.get(key) ?? 0) < 10 * MIN) return;
    this.alertedAt.set(key, now);
    this.logEvent("alert", { ...f, cause, detail: text, t: now });
    // ALERT_WEBHOOK: a JSON POST per alert (text and content, for Slack- and Discord-style hooks), at most 30 an hour.
    const url = this.env.ALERT_WEBHOOK;
    this.webhookLog = this.webhookLog.filter((t) => now - t < 3600_000);
    if (url && this.webhookLog.length < 30) {
      this.webhookLog.push(now);
      const message = `Runners: ${text}`;
      fetch(url, { method: "POST", headers: { "content-type": "application/json" }, signal: AbortSignal.timeout(10_000),
        body: JSON.stringify({ text: message, content: message, cause, app: f.app ?? null, machine: f.machine ?? null, t: now }) })
        .catch((e) => console.log(`alert webhook: ${e.message}`));
    }
  }

  // Strikes against a server, at most one of each kind per 10 minutes: copies of 2 or more apps failing there for 5
  // minutes while healthy elsewhere; half or more of its copies failing for 10 minutes (counting apps healthy
  // elsewhere, or every failing app when 2 or more fail); under 10% of its disk free for 5 minutes; 5 or more restarts
  // in an hour of apps healthy elsewhere. Two strikes within 30 minutes: quarantined for an hour (not a destination),
  // and a pool member asks to leave through the departure gate (one per pool per 30 minutes, none while its pool is
  // short or can't start servers); a server outside a pool raises an alert. 3 or more sick servers at once is likely
  // an app: nothing is done automatically, and an alert says so.
  checkSick(now) {
    if (now - this.lastSickCheck < 30_000 || this.quiet(now)) return;
    this.lastSickCheck = now;
    const up = this.liveMachines(now);
    const candidates = [];
    for (const r of up.values()) {
      if (!r.ready || now - r.started < SETTLED_MS) continue;
      for (const [kind, why] of this.sickSigns(r, up, now)) this.strike(r, kind, why, now);
      const recent = (this.strikes.get(r.id) ?? []).filter((x) => now - x.t < STRIKE_WINDOW_MS);
      if (recent.length) this.strikes.set(r.id, recent);
      else this.strikes.delete(r.id);
      if (recent.length >= 2 && !((r.quarantine ?? 0) > now)) candidates.push({ r, why: [...new Set(recent.map((x) => x.why))].join("; ") });
    }
    const sick = [...up.values()].filter((r) => (r.quarantine ?? 0) > now && this.sickWhy.has(r.id));
    if (sick.length + candidates.length >= 3) {
      if (candidates.length) {
        const list = [...sick, ...candidates.map((c) => c.r)].map((r) => r.machine).sort((a, b) => a - b);
        this.alert("many-sick", {}, `servers ${list.join(", ")} look sick at once: probably an app, not the servers, so nothing is done automatically`, now);
      }
      return;
    }
    for (const { r, why } of candidates) this.quarantine(r, why, now, { sick: true });
    for (const r of [...sick, ...candidates.map((c) => c.r)]) this.sickDrain(r, now);
  }

  sickSigns(r, up, now) {
    const out = [];
    const here = [];
    for (const [name, list] of this.copies) for (const x of list) if (x.machine === r.machine && !x.leaving) here.push({ name, x });
    const bad = here.filter(({ x }) => failing(r.status?.[x.key])).map((o) => {
      const elsewhere = this.anyReady(o.name, up, o.x.replica);
      return { ...o, elsewhere, plain: now - this.failClock(o.name, o.x, r, now, { outageAware: false }), aware: elsewhere ? now - this.failClock(o.name, o.x, r, now) : 0 };
    });
    const apps = new Set(bad.filter((o) => o.aware >= 5 * MIN).map((o) => o.name));
    if (apps.size >= 2) out.push(["apps", `${[...apps].join(" and ")} failing on it while healthy elsewhere`]);
    const long = bad.filter((o) => o.plain >= FAIL_MS);
    const counted = new Set(long.map((o) => o.name)).size >= 2 ? long : long.filter((o) => o.aware >= FAIL_MS);
    if (counted.length && 2 * counted.length >= here.length) out.push(["half", `${counted.length} of its ${here.length} copies failing for 10 minutes`]);
    const d = r.disk;
    if (d?.total && d.free / d.total < 0.1) {
      const since = this.lowDiskSince.get(r.id) ?? this.lowDiskSince.set(r.id, now).get(r.id);
      if (now - since >= 5 * MIN) out.push(["disk", `${Math.round((100 * d.free) / d.total)}% of its disk free`]);
    } else this.lowDiskSince.delete(r.id);
    const okElsewhere = (key) => {
      const o = here.find((y) => y.x.key === key);
      return o && this.anyReady(o.name, up, o.x.replica);
    };
    const n = (this.restarts.get(r.id)?.log ?? []).filter((e) => now - e.t < 3600_000 && okElsewhere(e.key)).reduce((a, e) => a + e.k, 0);
    if (n >= 5) out.push(["restarts", `${n} restarts in the last hour of apps healthy elsewhere`]);
    return out;
  }

  strike(r, kind, why, now) {
    const list = this.strikes.get(r.id) ?? this.strikes.set(r.id, []).get(r.id);
    if (list.some((x) => x.kind === kind && now - x.t < 10 * MIN)) return;
    list.push({ t: now, kind, why });
    this.logEvent("strike", { machine: r.machine, run: r.id, cause: kind, detail: why, t: now });
  }

  // Not a destination for QUARANTINE_MS. `sick`: it also asks to leave (see sickDrain).
  quarantine(r, why, now, { sick }) {
    r.quarantine = now + QUARANTINE_MS;
    this.saveRun(r);
    if (sick) this.sickWhy.set(r.id, why);
    this.logEvent("quarantine", { machine: r.machine, run: r.id, cause: sick ? "sick" : "hot", detail: `not a destination for an hour: ${why}`, t: now });
  }

  sickDrain(r, now) {
    if (!((r.quarantine ?? 0) > now) || !this.sickWhy.has(r.id) || r.drain || r.handover || r.evict || r.retire) return;
    const why = this.sickWhy.get(r.id);
    if (!r.pool) {
      this.alert("sick-server", { machine: r.machine }, `server ${r.machine} looks sick (${why}); nothing replaces it automatically, so it needs a look`, now);
      return;
    }
    if (now - (this.sickDrains[r.pool] ?? 0) < SICK_DRAIN_GAP_MS || this.poolShort(r.pool, now) || this.capped(r.pool, now)) return;
    r.drain = now;
    this.saveRun(r);
    this.sickDrains[r.pool] = now;
    this.setSetting("sick_drains", JSON.stringify(this.sickDrains));
    this.logEvent("drain", { machine: r.machine, run: r.id, cause: "sick", detail: `server ${r.machine} looks sick (${why}): it asks to leave, for a fresh one`, t: now });
  }

  // A pool with fewer live servers than its size.
  poolShort(pool, now) {
    const size = this.pools()[pool];
    return Boolean(size) && new Set(this.liveRuns(now).filter((r) => r.pool === pool).map((r) => r.machine)).size < size;
  }

  // ---- logs ----
  // Watching a replica's logs: a session, opened with the app's password or the admin password. Its viewer connects
  // (a WebSocket), which wakes the copy's server (see wait); the server's next check-in names the session, and its
  // agent connects its own WebSocket and sends the copy's log lines as they come (the last LOG_TAIL first). The control
  // plane passes them on, with the app's env values blanked out, and keeps nothing. Either side leaving ends it.
  openLogSession(name, k, now) {
    this.project(name);
    const copies = this.copiesOf(name).filter((x) => x.replica === k);
    const x = copies.find((y) => !y.leaving) ?? copies[0];
    if (!x) throw new HttpError(404, `replica ${k} of ${name} has no copy`);
    for (const [id, s] of this.logSessions) if (!s.viewer && now - s.created > LOG_CONNECT_MS) this.logSessions.delete(id);
    if (this.logSessions.size >= LOG_SESSIONS_MAX) throw new HttpError(429, "too many logs are being watched; try again in a minute");
    const id = randomUUID();
    this.logSessions.set(id, { id, name, replica: k, machine: x.machine, key: x.key, created: now, viewer: null, agent: null, run: null, waiting: null });
    return { session: id, machine: x.machine, key: x.key };
  }

  // A viewer connected: ask the copy's server to stream (it's woken, so that's its next check-in, within seconds).
  attachLogViewer(id, sock) {
    const s = this.logSessions.get(id);
    if (!s || s.viewer) return sock.close(4404, "no such session");
    s.viewer = sock;
    sock.onClose(() => this.endLogSession(id, null));
    const say = (t, text) => sock.send(JSON.stringify({ t, text }));
    const runs = this.liveRuns(Date.now()).filter((r) => r.machine === s.machine);
    if (!runs.length) say("info", `server ${s.machine} isn't checking in right now; waiting for it`);
    else say("info", `asking server ${s.machine} to stream ${s.name} replica ${s.replica}`);
    for (const r of runs) this.wakers.get(r.id)?.(true);
    s.waiting = setTimeout(() => {
      if (this.logSessions.get(id) === s && !s.agent) say("info", `server ${s.machine} hasn't started streaming: its agent may be older than live logs (servers get the new one as they're replaced)`);
    }, 25_000);
  }

  // The copy's agent connected: from now on its messages (log text) go to the viewer.
  attachLogAgent(id, run, sock) {
    const s = this.logSessions.get(String(id ?? ""));
    if (!s || !s.viewer || s.agent) return sock.close(4404, "no such session");
    s.agent = sock;
    s.run = String(run ?? "");
    clearTimeout(s.waiting);
    const secrets = [...(this.appEnv.get(s.name)?.values() ?? [])].flatMap((v) => [v, ...v.split("\n")]).filter((v) => v.length >= 4).sort((a, b) => b.length - a.length);
    const hide = (text) => secrets.reduce((t, v) => (t.includes(v) ? t.split(v).join("[hidden]") : t), text);
    s.viewer.send(JSON.stringify({ t: "open", text: `streaming from server ${s.machine}` }));
    sock.onMessage((text) => {
      if (this.logSessions.get(s.id) === s && typeof text === "string") s.viewer.send(JSON.stringify({ t: "log", text: hide(text) }));
    });
    sock.onClose(() => this.endLogSession(s.id, `server ${s.machine} stopped streaming`));
  }

  // One side left (why: the viewer's last message, null when the viewer is the one who left): the other is closed.
  endLogSession(id, why) {
    const s = this.logSessions.get(id);
    if (!s) return;
    this.logSessions.delete(id);
    clearTimeout(s.waiting);
    if (why && s.viewer) s.viewer.send(JSON.stringify({ t: "end", text: why }));
    s.viewer?.close(1000, "");
    s.agent?.close(1000, "");
  }

  isJoinToken(header) {
    return Boolean(this.env.JOIN_TOKEN) && header === `Bearer ${this.env.JOIN_TOKEN}`;
  }

  // An agent waiting to be told to check in now (it keeps this request open, so it hears at once when someone's
  // waiting on its server): answered with { wake: true } when that happens, else { wake: false } after WAKE_HOLD_MS.
  wait(runId) {
    const run = String(runId ?? "");
    if (!run) throw new HttpError(400, "run is required");
    this.wakers.get(run)?.(false); // an earlier wait of the same run ends
    return new Promise((resolve) => {
      const done = (wake) => {
        clearTimeout(timer);
        if (this.wakers.get(run) === done) this.wakers.delete(run);
        resolve({ wake });
      };
      const timer = setTimeout(() => done(false), WAKE_HOLD_MS);
      this.wakers.set(run, done);
    });
  }

  // ---- cold replicas, churn, departures (for the pages) ----

  // Replica k is lit while a live run of its server reports its copy healthy and ready, with the run's tunnel connected
  // (edges, when the agent reports them); dark otherwise, from the last moment it was lit. A dark spell is noted when
  // it's seen (never while settling) and closed by "lit" (other: its length in seconds); both name the run that last
  // served it, whose departure or loss is the cause, so a departure that left a replica dark counts as cold. Dark for
  // 5 minutes: an alert.
  checkDark(now) {
    if (now - this.lastDarkCheck < 10_000) return;
    this.lastDarkCheck = now;
    const byMachine = new Map();
    for (const r of this.liveRuns(now)) (byMachine.get(r.machine) ?? byMachine.set(r.machine, []).get(r.machine)).push(r);
    const settling = this.settling(now);
    const wanted = new Set();
    for (const p of this.projects.values()) {
      const spec = p.enabled ? this.version(p.name, p.version) : null;
      if (!spec) continue;
      for (let k = 1; k <= spec.replicas; k++) {
        const key = `${p.name}|${k}`;
        wanted.add(key);
        let serving = null;
        for (const x of this.copiesOf(p.name)) {
          if (x.replica !== k) continue;
          const r = (byMachine.get(x.machine) ?? []).find((y) => {
            const st = y.status?.[x.key];
            return isMap(st) && st.s === "healthy" && st.r !== false && y.edges !== 0;
          });
          if (r) {
            serving = { run: r.id, machine: x.machine };
            break;
          }
        }
        const d = this.dark.get(key);
        if (serving) {
          if (d) {
            this.dark.delete(key);
            this.logEvent("lit", { app: p.name, replica: k, machine: serving.machine, run: d.run, cause: d.cause, other: Math.round((now - d.since) / 1000),
              detail: `dark for ${fmtDur(now - d.since)}, after ${d.why}`, t: now });
          }
          this.litAt.set(key, { t: now, ...serving });
        } else if (!d) {
          const last = this.litAt.get(key);
          if (!last || settling) continue; // never served yet (a new replica), or not known yet
          const r = this.runs.get(last.run);
          const s = `server ${last.machine}`;
          const [cause, why] = !r ? ["gone", `${s} went away`] : r.left ? ["stopped", `${s} was stopped`]
            : r.evict ? ["evict", `${s} moved its copies out`] : r.handover ? ["handover", `${s} handed over`]
            : r.retire ? ["retired", `${s} retired`] : !this.live(r, now) ? ["silent", `${s} went silent`]
            : ["copy", `its copy on ${s} stopped serving`];
          this.dark.set(key, { since: last.t, run: last.run, machine: last.machine, cause, why });
          this.logEvent("dark", { app: p.name, replica: k, machine: last.machine, run: last.run, cause, detail: why, t: now });
        } else if (!d.alerted && !settling && now - d.since >= 5 * MIN) {
          d.alerted = true;
          this.alert("dark", { app: p.name }, `${p.name} replica ${k} has had no healthy copy for ${fmtDur(now - d.since)} (${d.why})`, now);
        }
      }
    }
    for (const key of [...this.dark.keys()]) if (!wanted.has(key)) this.dark.delete(key); // removed or disabled: not dark
    for (const key of [...this.litAt.keys()]) if (!wanted.has(key)) this.litAt.delete(key);
  }

  // Copy starts by cause over the last hour and day (placements, moves by cause, successors' rebuilds, new versions),
  // DNS re-points apart; starts per app.
  churn(now) {
    if (this.churnCache && now - this.churnCache.at < 30_000) return this.churnCache.v;
    const rows = this.all("SELECT t, kind, cause, app FROM events WHERE t > ? AND kind IN ('place', 'move-start', 'copy-start', 'dns')", now - 24 * 3600_000);
    const blank = () => ({ starts: 0, placed: 0, moved: 0, successor: 0, version: 0, dns: 0, moves: {} });
    const h1 = blank();
    const h24 = blank();
    const apps = new Map();
    for (const e of rows) {
      for (const b of now - e.t < 3600_000 ? [h1, h24] : [h24]) {
        if (e.kind === "dns") {
          b.dns++;
          continue;
        }
        b.starts++;
        if (e.kind === "place") b.placed++;
        else if (e.kind === "move-start") {
          b.moved++;
          b.moves[e.cause ?? "hand"] = (b.moves[e.cause ?? "hand"] ?? 0) + 1;
        } else b[e.cause === "successor" ? "successor" : "version"]++;
      }
      if (e.kind !== "dns" && e.app) {
        const a = apps.get(e.app) ?? apps.set(e.app, { app: e.app, h1: 0, h24: 0 }).get(e.app);
        a.h24++;
        if (now - e.t < 3600_000) a.h1++;
      }
    }
    const v = { h1, h24, apps: [...apps.values()].sort((a, b) => b.h24 - a.h24) };
    this.churnCache = { at: now, v };
    return v;
  }

  // Departures: under way, coming up (when each run is due, and why a due one waits), and over the last day: planned
  // (ours) and stopped (the server was stopped on its own), each cold when a replica went dark with it; lost (gone
  // silent); median handover times (start: grant to successor start; build: to successor ready; overlap: to the old
  // run's exit). `capped`: whether new servers aren't arriving for some departures right now.
  departures(now) {
    const list = this.liveRuns(now).filter((r) => !r.retire && !this.predecessorOf(r, now) && (this.dueAt(r) || r.drain || r.handover || r.evict || this.rolled(r)))
      .map((r) => {
        const g = this.departureGate(r, now);
        const due = this.due(r, now);
        return { machine: r.machine, run: r.id, dueAt: this.dueAt(r), deadline: r.deadline ?? null, due, why: due ? this.dueWhy(r, now) : null,
          leaving: r.handover ? "handover" : r.evict ? "evict" : null, since: r.handover || r.evict || null,
          how: g?.how ?? null, wait: g?.wait ?? null, waitingSince: g?.wait ? this.dueWait.get(r.id)?.since ?? null : null };
      })
      .sort((a, b) => Boolean(b.leaving) - Boolean(a.leaving) || b.due - a.due || (a.dueAt ?? Infinity) - (b.dueAt ?? Infinity));
    const ev = this.all("SELECT kind, run FROM events WHERE t > ? AND kind IN ('depart', 'server-lost', 'left', 'dark')", now - 24 * 3600_000);
    const darkRuns = new Set(ev.filter((e) => e.kind === "dark").map((e) => e.run));
    const day = { planned: 0, plannedCold: 0, stopped: 0, stoppedCold: 0, lost: 0, handover: null };
    for (const e of ev) {
      if (e.kind === "depart") {
        day.planned++;
        if (darkRuns.has(e.run)) day.plannedCold++;
      } else if (e.kind === "left") {
        day.stopped++;
        if (darkRuns.has(e.run)) day.stoppedCold++;
      } else if (e.kind === "server-lost") day.lost++;
    }
    const hs = this.handoverTimes;
    const median = (xs) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
    if (hs.length) day.handover = { n: hs.length, queue: median(hs.map((h) => h.queue)), build: median(hs.map((h) => h.build)), overlap: median(hs.map((h) => h.overlap)) };
    return { list, day, capped: [...this.cappedUntil.values()].some((t) => t > now) };
  }

  // ---- machines ----

  sync(body, ip = null) {
    const now = Date.now();
    // Every machine silent for longer than the liveness window: this was unreachable, so give the fleet time to show
    // up again before anything counts as gone or gets placed (see SETTLE_MS).
    if (now - this.lastCheckin > this.liveMs) {
      this.quietSince = this.lastCheckin;
      this.settleAt = now;
      const text = `no check-in for ${Math.round((now - this.lastCheckin) / 1000)}s: the control plane was unreachable; nothing counts as gone until the fleet is back`;
      console.log(text);
      this.logEvent("gap", { cause: "unreachable", detail: text });
    }
    this.lastCheckin = now;
    const machine = Number(body?.machine);
    const started = Number(body?.started);
    const run = String(body?.run ?? "");
    if (!Number.isInteger(machine) || machine < 1 || !run || !Number.isFinite(started)) {
      throw new HttpError(400, "machine, run and started are required");
    }
    const status = isMap(body.status) ? body.status : {};
    const ready = body.ready ? 1 : 0;
    // How the agent describes its machine (see the top of this file).
    const pool = typeof body.pool === "string" && /^[a-z0-9-]{1,30}$/.test(body.pool) ? body.pool : null;
    const label = String(body.label ?? "").slice(0, 80) || null;
    const starts = body.starts !== false; // agents from before this field could all start machines
    // When whatever runs the machine will stop it (LIFETIME_MIN), if it knows: between 1 minute and a day after its start.
    const deadline = Number(body.deadline) > started + MIN && Number(body.deadline) < started + 86400_000 ? Number(body.deadline) : null;
    this.askPoolSize(pool, body.poolSize);
    let r = this.runs.get(run);
    if (!r) {
      const agent = String(body.agent ?? run).slice(0, 100);
      r = { id: run, machine, started, status, ready, handover: 0, retire: 0, seen: now, agent, label, pool, drain: 0, starts, deadline };
      this.saveRun(r);
    } else {
      const changed = ready !== r.ready || JSON.stringify(status) !== JSON.stringify(r.status) ||
        pool !== (r.pool ?? null) || label !== (r.label ?? null) || deadline !== (r.deadline ?? null);
      Object.assign(r, { status, ready, seen: now, pool, label, deadline });
      if (ready && !r.readyAt) r.readyAt = now; // (kept in memory: a handover's build time)
      r.starts = starts; // sent with every check-in, so it's kept in memory only
      // "Last seen" is written on every check-in: after a restart, settling() needs to know who was live.
      if (changed) this.saveRun(r);
      else this.sql.exec("UPDATE runs SET seen = ? WHERE id = ?", now, r.id);
    }
    const cpus = Number(body.cpus);
    if (Number.isInteger(cpus) && cpus > 0 && cpus <= 1024) r.cpus = cpus;
    this.takeMetrics(r, body.metrics, now);
    this.noteCopies(r, status, now);
    if (isMap(body.disk) && Number(body.disk.total) > 0) r.disk = { free: Number(body.disk.free) || 0, total: Number(body.disk.total) };
    if (Number.isInteger(body.edges)) r.edges = body.edges;
    // What the pages show about the machine itself: its latency (the agent's last round trip here) and where it is.
    const rtt = Number(body.rtt);
    if (Number.isFinite(rtt) && rtt >= 0) r.rtt = Math.min(60_000, Math.round(rtt));
    if (ip) {
      r.ip = ip;
      r.location = this.locate(ip) ?? r.location ?? null;
    }
    if (body.leaving && !r.retire) {
      r.retire = 1; // going away for good: its replicas can be placed elsewhere right now
      r.left = now;
      this.saveRun(r);
      this.logEvent("left", { machine: r.machine, run: r.id, cause: "leaving", detail: "the server said it's going away" });
    }
    this.checkMassLoss(now);
    this.place(now);
    this.settle(now);
    this.noteReady(now);
    this.checkSick(now);
    this.rebalance(now);
    this.checkDark(now);
    if (!r.retire && !this.settling(now)) {
      if (this.superseded(r, now)) {
        r.retire = 1;
        this.saveRun(r);
        const succ = this.successorOf(r, now);
        let timing = "";
        if (r.handover && succ) { // queue: grant to successor start; build: to successor ready; overlap: until now
          const x = { t: now, machine: r.machine, queue: Math.max(0, succ.started - r.handover),
            build: Math.max(0, (succ.readyAt ?? now) - succ.started), overlap: Math.max(0, now - (succ.readyAt ?? now)) };
          this.handoverTimes.push(x);
          const fmt = (ms) => (ms < 90_000 ? `${Math.round(ms / 1000)} s` : `${Math.round(ms / MIN)} min`);
          timing = ` (start ${fmt(x.queue)}, build ${fmt(x.build)}, overlap ${fmt(x.overlap)})`;
        }
        this.logEvent("retire", { machine: r.machine, run: r.id, cause: "superseded", detail: `its successor serves everything it did${timing}` });
      } else if (r.evict) this.progressEvict(r, now);
      else this.retireStuckSuccessor(r, now);
    }
    this.trimPools(now);
    const handover = !r.retire && this.wantsHandover(r, now);
    const start = !r.retire && r.ready && r.pool && r.starts ? this.claimStarts(r.pool, run, now) : []; // pool members start their peers
    this.followCopies(now);
    this.cleanup(now);
    const older = r.retire ? null : this.predecessorOf(r, now);
    const desired = r.retire ? {} : this.desiredFor(r.machine, older ? r : null);
    // Logs someone's waiting to watch, of copies this run has: the agent streams them (see openLogSession).
    const logs = r.retire ? [] : [...this.logSessions.values()].filter((x) => x.viewer && !x.agent && x.machine === r.machine && x.key in (r.status ?? {}))
      .map((x) => ({ session: x.id, key: x.key, tail: LOG_TAIL }));
    return {
      domain: this.env.DOMAIN,
      poll: this.pollS,
      ...(logs.length ? { logs } : {}),
      desired,
      retire: Boolean(r.retire),
      handover,
      start,
      // A successor (an older run of this machine still serves): it opens its tunnel once it serves what that run does.
      ...(older ? { successor: true, serve: Object.keys(desired).filter((k) => older.status?.[k]?.s === "healthy" && older.status[k].r !== false) } : {}),
    };
  }

  // Many servers going silent together is the control plane's view of the network failing, not that many deaths: treat
  // it like a gap (settling) so nothing is re-placed or started for them. Only unannounced silences count (a run that
  // said it's leaving, or reached its deadline, is a departure); at most twice an hour.
  checkMassLoss(now) {
    if (now - this.lastLossCheck < 5_000 || this.settling(now)) return;
    this.lastLossCheck = now;
    const live = new Set(this.liveRuns(now).map((r) => r.machine));
    const lost = new Set();
    for (const r of this.runs.values()) {
      if (r.retire || live.has(r.machine) || (r.deadline && now > r.deadline - 2 * 60_000)) continue;
      const silent = now - r.seen;
      if (silent >= this.liveMs && silent < 2 * this.liveMs) lost.add(r.machine);
    }
    // Each unannounced loss, once (voluntary departures pause after 3 in 10 minutes).
    for (const r of this.runs.values()) {
      if (r.retire || r.left || this.lossNoted.has(r.id) || live.has(r.machine) || now - r.seen < this.liveMs || r.seen < this.settleAt - this.liveMs) continue;
      if (r.deadline && now > r.deadline - 2 * 60_000) continue;
      this.lossNoted.add(r.id);
      this.lossLog = this.lossLog.filter((t) => now - t < 10 * MIN);
      this.lossLog.push(now);
      this.logEvent("server-lost", { machine: r.machine, run: r.id, cause: "silent", detail: `server ${r.machine} stopped checking in without saying it was leaving`, t: now });
    }
    if (lost.size < Math.max(MASS_LOSS_MIN, Math.ceil(0.1 * this.expectedMachines()))) return;
    this.massArms = this.massArms.filter((t) => now - t < 3600_000);
    if (this.massArms.length >= 2) {
      if (now - (this.massExhaustedAt ?? 0) > 3600_000) {
        this.massExhaustedAt = now;
        this.logEvent("alert", { cause: "grace-exhausted", detail: `${lost.size} servers went silent together, the third time this hour: treated as lost` });
      }
      return;
    }
    this.massArms.push(now);
    this.setSetting("mass_arms", JSON.stringify(this.massArms));
    this.quietSince = now - this.liveMs;
    this.settleAt = now;
    this.logEvent("mass-loss", { cause: "silent", detail: `${lost.size} servers went silent together (${[...lost].sort((a, b) => a - b).join(", ")}): waiting for them before re-placing anything` });
  }

  // Where a machine is, from the address it checks in from, as "City, Region, CC · Network" (null until a free geo-IP
  // lookup has answered; cached, one lookup per address, never retried within a run of this process once it failed).
  locate(ip) {
    if (!ip || /^(127\.|10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|::1$|f[cd])/i.test(ip)) return null;
    const hit = this.geo.get(ip);
    if (hit) return hit.text ?? null;
    if (this.geoPending.has(ip) || this.geoPending.size >= 20) return null;
    this.geoPending.add(ip);
    fetch(`http://ip-api.com/json/${encodeURIComponent(ip)}?fields=status,message,city,regionName,countryCode,org`, { signal: AbortSignal.timeout(6000) })
      .then((res) => res.json())
      .then((g) => {
        if (g?.status !== "success") throw new Error(g?.message || "no answer");
        const place = [g.city, g.regionName, g.countryCode].filter(Boolean).join(", ");
        const text = [place, g.org].filter(Boolean).join(" · ").slice(0, 120) || null;
        this.geo.set(ip, { ip, text, at: Date.now() });
        this.sql.exec("INSERT OR REPLACE INTO geo (ip, text, at) VALUES (?, ?, ?)", ip, text, Date.now());
        for (const r of this.runs.values()) if (r.ip === ip) r.location = text;
      })
      .catch((e) => {
        console.log(`geo lookup for a server's address failed: ${e.message}`);
        this.geo.set(ip, { ip, text: null, at: Date.now() }); // not written: tried again after a restart
      })
      .finally(() => this.geoPending.delete(ip));
    return null;
  }

  // ---- departures ----
  // The control plane decides when a run leaves: when something asked (drain, roll), at its dueAt (it knows its
  // deadline), early to spread a crowded pool (flatten), or urgently (its deadline is near). Leaving is a same-slot
  // handover by default: the run starts its successor, which inherits its copies (no moves, no DNS changes), and exits
  // once the successor serves everything it did. In a pool that can't start a successor right now (capped: a server it
  // started didn't show up), when the deadline is near, or to shrink a pool, it leaves by moving its copies out first
  // (evict), then exits.
  // Gates: one departure in flight per pool (standalone hosts share one), 3 fleet-wide, at most 4 started in 10
  // minutes (2 of them evictions), earliest deadline first, none while settling, within 10 minutes of a (re)start, or
  // after 3 unannounced losses in 10 minutes; a run carrying a copy of an app already changing as much as it may waits.
  // An urgent departure skips them all: the deadline takes the machine anyway.

  rolled(r) {
    return r.started < Number(this.settings.get("roll") ?? 0) || r.started < Number(this.settings.get(`roll_${r.machine}`) ?? 0);
  }

  due(r, now) {
    const d = this.dueAt(r);
    return Boolean(r.drain) || this.rolled(r) || Boolean(d && now >= d) || this.pulled.has(r.id);
  }

  // Due only because a roll asked for it (nothing of its own: no drain, not past its due time, not pulled forward).
  rollOnly(r, now) {
    return this.rolled(r) && !r.drain && !this.pulled.has(r.id) && !(this.dueAt(r) && now >= this.dueAt(r));
  }

  urgent(r, now) {
    return Boolean(r.deadline) && r.deadline - now < URGENT_MS;
  }

  // Why a run is leaving now (for the events).
  dueWhy(r, now) {
    return this.urgent(r, now) ? "urgent" : r.drain ? (this.sickWhy.has(r.id) ? "sick" : "drain") : this.rolled(r) ? "roll" : this.pulled.has(r.id) ? "spread" : "due";
  }

  predecessorOf(r, now) {
    return this.liveRuns(now).filter((y) => y.machine === r.machine && y.started < r.started && !y.retire).sort((a, b) => a.started - b.started)[0] ?? null;
  }

  successorOf(r, now) {
    return this.liveRuns(now).filter((y) => y.machine === r.machine && y.started > r.started).sort((a, b) => b.started - a.started)[0] ?? null;
  }

  capped(pool, now) {
    return Boolean(pool) && (this.cappedUntil.get(pool) ?? 0) > now;
  }

  markCapped(pool, now, why) {
    if (!pool) return;
    if (!this.capped(pool, now)) {
      this.logEvent("capped", { cause: "capped", detail: `new servers aren't arriving (${why}): departures that would wait for one move their copies out first for ${CAPPED_MS / MIN} min` });
      this.alert("capped", {}, `new servers aren't arriving (${why}): whatever starts them may be at its limit`, now);
    }
    this.cappedUntil.set(pool, now + CAPPED_MS);
  }

  // Runs leaving by our doing right now: handing over (until the successor takes over, or the handover is stuck) or evicting.
  departing(now) {
    return this.liveRuns(now).filter((r) => (r.handover && now - r.handover < HANDOVER_STUCK_MS && !r.evict) || r.evict);
  }

  noteDeparture(r, kind, why, now) {
    this.departLog = this.departLog.filter((d) => now - d.t < 10 * MIN);
    this.departLog.push({ t: now, kind, run: r.id });
    this.logEvent("depart", { machine: r.machine, run: r.id, cause: `${kind}:${why}`,
      detail: `server ${r.machine} is leaving (${why}) by ${kind === "handover" ? "handing over to a successor on its slot" : "moving its copies out first"}`, t: now });
  }

  // Whether run r may start leaving now: "handover", "evict" or null (not due, or a gate says wait).
  departure(r, now) {
    const g = this.departureGate(r, now);
    return g && !g.wait ? g.how : null;
  }

  // How a due run would leave ("handover" or "evict") and, while it can't yet, why it waits: { how, wait }; null when
  // it isn't due (or is leaving already).
  departureGate(r, now) {
    if (r.retire || r.evict || r.handover || this.successorOf(r, now) || !this.due(r, now)) return null;
    const urgent = this.urgent(r, now);
    const how = urgent || this.capped(r.pool, now) ? "evict" : "handover";
    const out = (wait) => ({ how, wait });
    if (urgent) return out(null);
    if (this.quiet(now)) return out("the control plane is in its quiet period after a restart or gap");
    if (this.lossLog.filter((t) => now - t < 10 * MIN).length >= 3) return out("3 or more servers were lost in the last 10 minutes");
    const recent = this.departLog.filter((d) => now - d.t < 10 * MIN);
    if (recent.length >= 4) return out("4 departures started in the last 10 minutes");
    if (how === "evict" && recent.filter((d) => d.kind === "evict").length >= 2) return out("2 evictions started in the last 10 minutes");
    const inFlight = this.departing(now);
    if (inFlight.length >= 3) return out("3 servers are leaving already");
    // A roll replaces healthy servers on request (after an agent update, say), with no deadline behind it: one server
    // at a time fleet-wide, as it always has. Servers due for their own reasons don't wait for it.
    const rollWaits = (x) => this.rollOnly(x, now) && inFlight.length > 0;
    const poolBusy = (x) => inFlight.some((y) => (y.pool ?? null) === (x.pool ?? null));
    const busyApp = (x) => [...this.copies].find(([name, list]) => list.some((c) => c.machine === x.machine && !c.leaving) && !this.mayTransition(name, now))?.[0];
    const carriesBusyApp = (x) => Boolean(busyApp(x));
    // Earliest deadline first: only the first due run that its gates let through may go.
    const queue = this.liveRuns(now).filter((x) => !x.retire && !x.evict && !x.handover && !this.successorOf(x, now) && this.due(x, now))
      .sort((a, b) => (this.sickWhy.has(a.id) ? 0 : 1) - (this.sickWhy.has(b.id) ? 0 : 1) ||
        ((a.deadline ?? Infinity) - (b.deadline ?? Infinity) || a.started - b.started));
    const first = queue.find((x) => !poolBusy(x) && !carriesBusyApp(x) && !rollWaits(x));
    if (first?.id === r.id) return out(null);
    if (rollWaits(r)) return out("a roll replaces one server at a time");
    if (poolBusy(r)) return out("a server of the same group is leaving");
    if (carriesBusyApp(r)) return out(`${busyApp(r)} is already changing as much as it may`);
    return out(first ? `server ${first.machine} goes first` : "waiting its turn");
  }

  // "This server is going down soon": from whatever runs it (a timer, a cron before maintenance, a cloud termination
  // notice). Its departure is then scheduled like any due one.
  drain(body) {
    const now = Date.now();
    const runId = body?.run != null ? String(body.run) : null;
    const agent = body?.agent != null ? String(body.agent) : null;
    const machine = Number(body?.machine);
    const runs = [...this.runs.values()].filter((x) => this.live(x, now) && !x.retire &&
      ((runId && x.id === runId) || (agent && x.agent === agent) || (Number.isInteger(machine) && x.machine === machine)));
    if (!runs.length) throw new HttpError(404, "no live run matches that run, agent or server");
    for (const x of runs) {
      if (!x.drain) {
        x.drain = now;
        this.saveRun(x);
      }
    }
    return { draining: runs.map((x) => ({ run: x.id, machine: x.machine })) };
  }

  // Asked on each check-in of run r: whether it should start its successor now.
  wantsHandover(r, now) {
    if (r.retire || r.evict) return false;
    if (r.handover) {
      if (this.successorOf(r, now)) return false; // its successor is starting
      if (now - r.handover > SUCCESSOR_WAIT_MS) { // none came: its pool can't start one now
        this.markCapped(r.pool, now, `server ${r.machine}'s successor didn't start within ${SUCCESSOR_WAIT_MS / MIN} minutes`);
        this.startEvict(r, now, "no successor came");
        return false;
      }
      return true; // keep asking until a replacement shows up
    }
    if (this.settling(now)) return false;
    const g = this.departureGate(r, now);
    if (!g || g.wait) {
      if (!g) this.dueWait.delete(r.id);
      else {
        const w = this.dueWait.get(r.id) ?? this.dueWait.set(r.id, { since: now }).get(r.id);
        w.why = g.wait;
        // (A roll takes hours by design: its waits aren't news.)
        if (now - w.since >= 20 * MIN && !this.rollOnly(r, now)) this.alert("departure-waiting", { machine: r.machine }, `server ${r.machine} has been due to leave for ${Math.round((now - w.since) / MIN)} minutes: ${g.wait}`, now);
      }
      return false;
    }
    this.dueWait.delete(r.id);
    const how = g.how;
    const why = this.dueWhy(r, now);
    if (how === "evict") {
      this.startEvict(r, now, why);
      return false;
    }
    r.handover = now; // its replacement joins for this slot while this run is still up (join() allows that)
    this.saveRun(r);
    this.noteDeparture(r, "handover", why, now);
    return true;
  }

  startEvict(r, now, why) {
    r.evict = now;
    this.saveRun(r);
    this.noteDeparture(r, "evict", why, now);
    this.progressEvict(r, now);
  }

  // An evicting run moves its copies out (through each app's budget, unless its deadline is near) and exits once none
  // is left on its machine.
  progressEvict(r, now) {
    const urgent = this.urgent(r, now);
    let here = 0;
    for (const [name, list] of [...this.copies]) {
      for (const x of list.filter((y) => y.machine === r.machine)) {
        here++;
        if (x.leaving || (!urgent && !this.mayTransition(name, now, r.id))) continue;
        try {
          this.move(name, r.machine, null, x.replica, { cause: "evict", why: `server ${r.machine} is leaving` });
        } catch (e) {
          if (!this.evictStuck?.has(`${r.id}|${name}|${x.replica}`)) {
            (this.evictStuck ??= new Set()).add(`${r.id}|${name}|${x.replica}`);
            this.logEvent("rebalance", { app: name, replica: x.replica, machine: r.machine, cause: "evict", detail: `couldn't move ${name} replica ${x.replica} off leaving server ${r.machine}: ${e.message}`, t: now });
          }
        }
      }
    }
    if (!here) {
      r.retire = 1;
      this.saveRun(r);
      this.logEvent("retire", { machine: r.machine, run: r.id, cause: "evicted", detail: "its copies moved out", t: now });
    }
  }

  // A run can go once a newer run of the same machine is ready and serves everything it does: every copy healthy here
  // is healthy there (a copy failing on both holds nothing up).
  superseded(r, now) {
    const succ = this.successorOf(r, now);
    if (!succ?.ready) return false;
    return Object.entries(r.status ?? {}).every(([key, st]) => !(st?.s === "healthy" && st.r !== false) ||
      (succ.status?.[key]?.s === "healthy" && succ.status[key].r !== false));
  }

  // A successor that, 25 minutes in, still can't run a copy its predecessor runs fine (and that's healthy elsewhere) is
  // retired, so the predecessor leaves another way (it asks again, and then moves its copies out).
  retireStuckSuccessor(r, now) {
    const older = this.predecessorOf(r, now);
    if (!older || now - r.started < 25 * MIN) return;
    const up = this.liveMachines(now);
    const stuck = Object.entries(older.status ?? {}).find(([key, st]) => st?.s === "healthy" && st.r !== false &&
      !(r.status?.[key]?.s === "healthy" && r.status[key].r !== false));
    if (!stuck) return;
    r.retire = 1;
    this.saveRun(r);
    this.logEvent("retire", { machine: r.machine, run: r.id, cause: "stuck-successor", detail: `after 25 minutes it still couldn't run ${stuck[0]}, which its predecessor runs`, t: now });
    if (!older.evict) this.startEvict(older, now, "its successor couldn't take over");
  }

  // Pools over their size lose one run at a time, by moving its copies out first: the run with the fewest copies, the
  // youngest of those. Pools also get their crowded departures spread out (flatten).
  trimPools(now) {
    if (now - (this.lastTrim ?? 0) < 10_000) return;
    this.lastTrim = now;
    if (this.quiet(now)) return;
    const live = this.liveRuns(now);
    for (const [pool, size] of Object.entries(this.pools())) {
      const members = live.filter((r) => r.pool === pool && !this.successorOf(r, now));
      if (members.length > size && !members.some((r) => r.evict) && !this.departing(now).some((r) => r.pool === pool)) {
        const recent = this.departLog.filter((d) => now - d.t < 10 * MIN);
        if (recent.length < 4 && recent.filter((d) => d.kind === "evict").length < 2) {
          const count = (r) => [...this.copies.values()].reduce((n, list) => n + list.filter((x) => x.machine === r.machine).length, 0);
          const victim = members.sort((a, b) => count(a) - count(b) || b.started - a.started)[0];
          this.startEvict(victim, now, "surplus");
        }
      }
      this.flatten(pool, members, now);
    }
  }

  // A pool whose runs will come due in a crowd (2 or more within the next hour) while its gate is idle starts the oldest
  // run past 3 hours now, so departures spread out instead of queueing against their deadlines.
  flatten(pool, members, now) {
    if (this.capped(pool, now) || this.departing(now).some((r) => r.pool === pool)) return;
    if (members.some((r) => this.due(r, now))) return; // something is already due: the gate is about to be busy
    const soon = members.filter((r) => { const d = this.dueAt(r); return d && d - now < 60 * MIN; });
    if (soon.length < 2) return;
    const oldest = members.filter((r) => this.dueAt(r) && now - r.started >= 180 * MIN && !this.pulled.has(r.id)).sort((a, b) => a.started - b.started)[0];
    if (oldest) this.pulled.add(oldest.id);
  }

  // A pool machine that went silent keeps its copies for its slot for up to START_WAIT_MS, so the next machine its pool
  // starts takes the slot and inherits them (no moves, no DNS changes): the pool, while that holds, else null. Not for a
  // run that said it's leaving (it may not be replaced) or that moved its copies out.
  heldBy(n, now) {
    if (this.liveRuns(now).some((r) => r.machine === n)) return null;
    const last = [...this.runs.values()].filter((r) => r.machine === n).sort((a, b) => b.seen - a.seen)[0];
    if (!last?.pool || last.left || last.evict) return null;
    if (![...this.copies.values()].some((list) => list.some((x) => x.machine === n && !x.leaving))) return null;
    return now - (last.seen + this.liveMs) < START_WAIT_MS ? last.pool : null;
  }

  // When a machine last checked in (any of its runs).
  lastSeenOf(n) {
    let t = 0;
    for (const r of this.runs.values()) if (r.machine === n && r.seen > t) t = r.seen;
    return t;
  }

  // Machines to start so a pool has its size. Whoever asks (a member of the pool, or something watching it from
  // outside through /api/claim) starts them, each with the slot it should take; the slot is held until it shows up.
  claimStarts(pool, by, now) {
    if (!pool || !(pool in this.pools())) return [];
    const live = this.liveRuns(now);
    if (this.settling(now)) return []; // claiming now would start a whole pool's worth of extras
    // A start of this pool that never showed up: whatever starts its servers can't start more right now. Claim nothing
    // new until that passes (a start that's only late still arrives).
    for (const [n, at] of [...this.starts]) {
      if (this.startPools.get(n) !== pool || now - at < START_WAIT_MS) continue;
      if (!live.some((x) => x.machine === n && x.started >= at - MIN)) this.markCapped(pool, now, `a machine started for slot ${n} didn't show up within ${START_WAIT_MS / MIN} minutes`);
      this.startPools.delete(n);
    }
    if (this.capped(pool, now)) return [];
    const members = new Set(live.filter((x) => x.pool === pool).map((x) => x.machine));
    // Only this pool's starts count (a start from before a restart, pool unknown, counts for every pool).
    const starting = [...this.starts].filter(([n, at]) => now - at < START_WAIT_MS && !live.some((x) => x.machine === n) &&
      (this.startPools.get(n) ?? pool) === pool).length;
    const out = [];
    // Its machines' held slots first: a machine started for one inherits its copies.
    const held = [...new Set([...this.runs.values()].map((r) => r.machine))].filter((n) => this.heldBy(n, now) === pool &&
      !(now - (this.starts.get(n) ?? 0) < START_WAIT_MS)).sort((a, b) => a - b);
    for (let missing = this.poolSize(pool) - members.size - starting; missing > 0; missing--) {
      const n = held.shift() ?? this.freeSlot(now, null, pool);
      if (!n) break;
      this.markStart(n, now, by, pool);
      out.push(n);
    }
    if (out.length) this.logEvent("start", { cause: "start", detail: `starting ${out.length === 1 ? "a server" : `${out.length} servers`}: slot${out.length === 1 ? "" : "s"} ${out.join(", ")}`, t: now });
    return out;
  }

  // A pool's runs beyond its size: the ones in the highest slots go.
  surplusInPool(r, now) {
    if (!r.pool) return false;
    const slots = [...new Set(this.liveRuns(now).filter((x) => x.pool === r.pool).map((x) => x.machine))].sort((a, b) => a - b);
    return slots.indexOf(r.machine) >= this.poolSize(r.pool);
  }

  // A slot is taken while another agent runs in it, while a machine is starting for it, and for a while after a
  // standalone machine drops out (so it gets the slot back when it restarts).
  slotTaken(n, now, agent, pool = null) {
    if (now - (this.starts.get(n) ?? 0) < START_WAIT_MS) return true;
    const held = this.heldBy(n, now);
    if (held && held !== pool) return true; // kept for its own pool's next machine
    const hold = this.holds.get(n);
    if (hold && hold.agent !== agent && now - hold.at < 2 * 60_000) return true;
    return [...this.runs.values()].some((x) => x.machine === n && x.agent !== agent && !x.retire &&
      (this.live(x, now) || (!x.pool && now - x.seen < STANDALONE_HOLD_MS)));
  }

  freeSlot(now, agent, pool = null) {
    for (let n = 1; n <= this.maxSlots(); n++) if (!this.slotTaken(n, now, agent, pool)) return n;
    return null;
  }

  // An agent starting up: give it a slot and that slot's tunnel token. A machine started for a slot asks for it
  // (`want`); otherwise it gets the slot its agent had last time if that's still free, else the lowest free one.
  async join(body) {
    const now = Date.now();
    const agent = String(body?.agent ?? "");
    if (!agent || agent.length > 100) throw new HttpError(400, "agent (an ID for this agent) is required");
    const pool = typeof body.pool === "string" && /^[a-z0-9-]{1,30}$/.test(body.pool) ? body.pool : null;
    const label = String(body.label ?? "").slice(0, 80) || null;
    const want = Number(body.want);
    const max = this.maxSlots();
    let slot = null;
    if (Number.isInteger(want) && want >= 1 && want <= max) {
      // A slot with another agent's live run in it is only given out when a machine was asked for there: a replacement
      // for a run that's handing over, or a start claimed for the slot. The join token alone mustn't take over a live
      // machine's tunnel.
      const others = this.liveRuns(now).filter((x) => x.machine === want && x.agent !== agent);
      const asked = others.every((x) => x.handover) || now - (this.starts.get(want) ?? 0) < START_WAIT_MS;
      if (others.length && !asked) throw new HttpError(409, `server ${want} is up and hasn't asked for a replacement`);
      slot = want;
    } else {
      const before = this.agents.get(agent)?.slot;
      slot = before && !this.slotTaken(before, now, agent, pool) ? before : this.freeSlot(now, agent, pool);
    }
    if (!slot) throw new HttpError(503, `all ${max} slots are taken`);
    // Recorded before the tunnel lookup, so an agent joining at the same moment doesn't get the same slot.
    const a = { id: agent, slot, pool, label, joined: now };
    this.agents.set(agent, a);
    this.sql.exec(
      `INSERT INTO agents (id, slot, kind, label, joined, pool) VALUES (?, ?, '', ?, ?, ?)
       ON CONFLICT (id) DO UPDATE SET slot = excluded.slot, label = excluded.label, joined = excluded.joined, pool = excluded.pool`,
      agent, slot, label, now, pool,
    );
    this.holds.set(slot, { agent, at: now }); // holds the slot until its first check-in
    const tunnel = await this.tunnelFor(slot);
    return { machine: slot, tunnelToken: tunnel.token, domain: this.env.DOMAIN };
  }

  async cf(path, init = {}) {
    const auth = this.env.CF_API_TOKEN ? { authorization: `Bearer ${this.env.CF_API_TOKEN}` }
      : { "x-auth-email": this.env.CF_API_EMAIL, "x-auth-key": this.env.CF_API_KEY };
    const res = await fetch(`${this.env.CF_API_BASE ?? "https://api.cloudflare.com/client/v4"}${path}`, {
      ...init,
      headers: { ...auth, "content-type": "application/json" },
    });
    const data = await res.json().catch(() => ({ success: false, errors: [{ message: `HTTP ${res.status}` }] }));
    if (!data.success) throw new HttpError(502, `Cloudflare API ${path.split("?")[0]}: ${JSON.stringify(data.errors)}`);
    return data;
  }

  // Tunnel runner-<n>: found by name (so tunnels made earlier are reused) or created, and kept with its token.
  tunnelFor(n) {
    if (this.slots.has(n)) return Promise.resolve(this.slots.get(n));
    if (!this.tunnelJobs.has(n)) {
      const job = (async () => {
        const account = this.env.ACCOUNT_ID;
        const name = `runner-${n}`;
        let t = (await this.cf(`/accounts/${account}/cfd_tunnel?name=${name}&is_deleted=false`)).result[0];
        if (!t) {
          const secret = btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))));
          t = (await this.cf(`/accounts/${account}/cfd_tunnel`, {
            method: "POST",
            body: JSON.stringify({ name, config_src: "local", tunnel_secret: secret }),
          })).result;
        }
        const token = (await this.cf(`/accounts/${account}/cfd_tunnel/${t.id}/token`)).result;
        const slot = { n, tunnel: t.id, token };
        this.sql.exec("INSERT OR REPLACE INTO slots (n, tunnel, token, created) VALUES (?, ?, ?, ?)", n, t.id, token, Date.now());
        this.slots.set(n, slot);
        this.scheduleDns();
        return slot;
      })().finally(() => this.tunnelJobs.delete(n));
      this.tunnelJobs.set(n, job);
    }
    return this.tunnelJobs.get(n);
  }

  // Retire a slot nothing runs in any more: delete its tunnel, forget its runs and agents, and drop its DNS names.
  // (A pool that's short of machines takes the lowest free slot, so it may come back as a fresh slot.)
  async retireSlot(n) {
    const now = Date.now();
    if (!Number.isInteger(n) || n < 1) throw new HttpError(400, "slot must be a server number");
    if (this.liveRuns(now).some((r) => r.machine === n)) throw new HttpError(409, `server ${n} is still up; stop it first`);
    const slot = this.slots.get(n);
    if (slot) {
      // Its <project>-m<n> names first: the regular DNS sync only touches records of tunnels it still knows.
      const records = (await this.cf(`/zones/${this.env.ZONE}/dns_records?type=CNAME&content=${slot.tunnel}.cfargotunnel.com&per_page=1000`)).result;
      if (records.length) await this.cf(`/zones/${this.env.ZONE}/dns_records/batch`, { method: "POST", body: JSON.stringify({ deletes: records.map((r) => ({ id: r.id })) }) });
      await this.cf(`/accounts/${this.env.ACCOUNT_ID}/cfd_tunnel/${slot.tunnel}?cascade=true`, { method: "DELETE" });
      this.slots.delete(n);
      this.sql.exec("DELETE FROM slots WHERE n = ?", n);
    }
    for (const [id, a] of this.agents) if (a.slot === n) this.agents.delete(id);
    this.sql.exec("DELETE FROM agents WHERE slot = ?", n);
    for (const r of [...this.runs.values()]) if (r.machine === n) this.runs.delete(r.id);
    this.sql.exec("DELETE FROM runs WHERE machine = ?", n);
    this.starts.delete(n);
    this.sql.exec("DELETE FROM starts WHERE machine = ?", n);
    this.holds.delete(n);
    this.liveMetrics.delete(n);
    for (const [name, list] of this.copies) for (const x of [...list]) if (x.machine === n) this.dropCopy(name, n, x.replica);
    this.scheduleDns();
    return { retired: n, tunnel: slot?.tunnel ?? null };
  }

  // Where each replica's public name should point: the tunnel of the machine running that copy. While a replica is
  // being moved (two copies), the new one once it's healthy, else the old one. Replicas with no copy yet have no
  // target (their names are left as they are).
  dnsTargets(now) {
    const want = new Map(); // "<project>-<k>" -> tunnel id
    const newest = this.liveMachines(now);
    const healthy = (name, x) => newest.get(x.machine)?.status[this.keyOn(name, x.machine, x.replica)]?.s === "healthy";
    for (const p of this.projects.values()) {
      const spec = this.version(p.name, p.version);
      if (!spec?.port) continue;
      for (let k = 1; k <= spec.replicas; k++) {
        const copies = this.copiesOf(p.name).filter((x) => x.replica === k);
        const pick = copies.find((x) => !x.leaving && healthy(p.name, x)) ?? copies.find((x) => x.leaving && healthy(p.name, x))
          ?? copies.find((x) => !x.leaving) ?? copies[0];
        const tunnel = pick && this.slots.get(pick.machine)?.tunnel;
        if (tunnel) want.set(`${p.name}-${k}`, tunnel);
      }
    }
    return want;
  }

  // A DNS sync whenever a replica's name should point somewhere else than it did.
  followCopies(now) {
    const seen = JSON.stringify([...this.dnsTargets(now)].sort());
    if (seen === this.dnsTargetsSeen) return;
    this.dnsTargetsSeen = seen;
    this.scheduleDns();
  }

  cleanup(now) {
    this.flushMetrics(now);
    for (const [id, x] of this.logSessions) if (!x.viewer && now - x.created > LOG_CONNECT_MS) this.logSessions.delete(id);
    if (now - this.lastRollup > MIN) {
      this.lastRollup = now;
      this.rollupMetrics(now);
    }
    if (now - this.lastCleanup < 10 * 60_000) return;
    this.lastCleanup = now;
    this.sql.exec("DELETE FROM metrics_1m WHERE t < ?", now - KEEP_1M_MS);
    this.sql.exec("DELETE FROM metrics_10m WHERE t < ? AND t < ?", now - KEEP_10M_MS, Number(this.settings.get("rolled60") ?? 0)); // once rolled into hours
    this.sql.exec("DELETE FROM metrics_1h WHERE t < ?", now - KEEP_1H_MS);
    this.sql.exec("DELETE FROM geo WHERE at < ?", now - 30 * 86400_000);
    this.sql.exec("DELETE FROM events WHERE t < ?", now - EVENTS_KEEP_MS);
    for (const r of this.runs.values()) {
      if (now - r.seen > 2 * 3600_000) {
        this.runs.delete(r.id);
        this.sql.exec("DELETE FROM runs WHERE id = ?", r.id);
      }
    }
    for (const map of [this.lastHealthy, this.firstSeen, this.lastV]) {
      for (const key of map.keys()) if (!this.runs.has(key.slice(0, key.lastIndexOf("|")))) map.delete(key);
    }
    this.handoverTimes = this.handoverTimes.filter((x) => now - x.t < 24 * 3600_000);
    for (const map of [this.minutes, this.restarts, this.lowDiskSince, this.strikes, this.sickWhy, this.dueWait]) {
      for (const id of map.keys()) if (!this.runs.has(id)) map.delete(id);
    }
    let pruned = false;
    for (const key of this.versionFails.keys()) {
      const [name, v] = [key.slice(0, key.lastIndexOf("@")), Number(key.slice(key.lastIndexOf("@") + 1))];
      const p = this.projects.get(name);
      if (!p || (p.version !== v && p.stable !== v)) {
        this.versionFails.delete(key);
        pruned = true;
      }
    }
    if (pruned) this.saveVersionFails();
  }

  // ---- metrics ----

  // The run that speaks for a machine: its newest ready run, or its newest run if none is ready yet.
  // During a handover two runs (two different VMs) report; only one of them is the machine on the charts.
  speaksFor(r, now) {
    const runs = this.liveRuns(now).filter((x) => x.machine === r.machine);
    const pick = (list) => list.sort((a, b) => b.started - a.started)[0];
    return (pick(runs.filter((x) => x.ready)) ?? pick(runs))?.id === r.id;
  }

  takeMetrics(r, metrics, now) {
    if (!isMap(metrics) || r.retire || !this.speaksFor(r, now)) return;
    if (isMap(metrics.live) && isMap(metrics.live.h)) {
      const h = metrics.live.h;
      this.liveMetrics.set(r.machine, { run: r.id, started: r.started, t: Number(metrics.live.t) || now, h, a: isMap(metrics.live.a) ? metrics.live.a : {} });
      const list = this.samples.get(r.machine) ?? this.samples.set(r.machine, []).get(r.machine);
      list.push({ t: now, cpu: Number(h.cpu) || 0, mem: h.memTotal ? (100 * (h.memUsed || 0)) / h.memTotal : 0 });
      while (list.length && now - list[0].t > 5 * MIN) list.shift(); // room is judged on the last 3 minutes
    }
    const mine = this.minutes.get(r.id) ?? [];
    for (const m of Array.isArray(metrics.minutes) ? metrics.minutes.slice(-60) : []) {
      const t = Number(m?.t);
      if (!Number.isInteger(t) || t % MIN || t > now || now - t > KEEP_1M_MS || !isMap(m.h)) continue;
      const slot = this.pendingMetrics.get(t) ?? {};
      slot[r.machine] = { h: m.h, a: isMap(m.a) ? m.a : {} };
      this.pendingMetrics.set(t, slot);
      if (now - t <= 10 * MIN && !mine.some((x) => x.t === t)) {
        const memTotal = Number(m.h.memTotal) || 0;
        mine.push({ t, cpu: Number(m.h.cpu) || 0, steal: Number(m.h.steal) || 0, mem: memTotal ? (100 * (Number(m.h.memUsed) || 0)) / memTotal : 0, memTotal, a: isMap(m.a) ? m.a : {} });
      }
    }
    if (mine.length) this.minutes.set(r.id, mine.sort((a, b) => a.t - b.t).slice(-10));
  }

  // Writes each finished minute as one row; a minute that already has a row (a late summary) is merged into it.
  flushMetrics(now) {
    for (const [t, slot] of this.pendingMetrics) {
      if (now - t < FLUSH_AFTER_MS) continue;
      const old = this.all("SELECT data FROM metrics_1m WHERE t = ?", t)[0];
      const data = old ? { ...JSON.parse(old.data), ...slot } : slot;
      this.sql.exec("INSERT INTO metrics_1m (t, data) VALUES (?, ?) ON CONFLICT (t) DO UPDATE SET data = excluded.data", t, JSON.stringify(data));
      this.pendingMetrics.delete(t);
      // A late minute in an already rolled-up 10-minute bucket: roll that bucket up again, and its hour if that's done too.
      const ten = t - (t % (10 * MIN));
      if (ten <= Number(this.settings.get("rolled10") ?? 0)) {
        this.rollupBucket("metrics_10m", "metrics_1m", ten, 10 * MIN);
        const hour = t - (t % (60 * MIN));
        if (hour <= Number(this.settings.get("rolled60") ?? 0)) this.rollupBucket("metrics_1h", "metrics_10m", hour, 60 * MIN);
      }
    }
  }

  // One row of `table` for the bucket starting at b, from the finer rows of `from` within it.
  rollupBucket(table, from, b, step) {
    const rows = this.all(`SELECT data FROM ${from} WHERE t >= ? AND t < ?`, b, b + step).map((r) => JSON.parse(r.data));
    if (!rows.length) return;
    this.sql.exec(`INSERT INTO ${table} (t, data) VALUES (?, ?) ON CONFLICT (t) DO UPDATE SET data = excluded.data`, b, JSON.stringify(mergeFleet(rows)));
  }

  // 10-minute rows for every bucket whose minutes are all written, then hourly rows for every hour whose 10-minute
  // buckets are all there.
  rollupMetrics(now) {
    const ten = 10 * MIN;
    const hour = 60 * MIN;
    let last10 = Number(this.settings.get("rolled10") ?? 0);
    if (!last10) last10 = now - (now % ten) - 2 * ten;
    for (let b = last10 + ten, n = 0; b + ten + FLUSH_AFTER_MS <= now && n < 300; b += ten, n++) {
      this.rollupBucket("metrics_10m", "metrics_1m", b, ten);
      last10 = b;
    }
    if (String(last10) !== this.settings.get("rolled10")) this.setSetting("rolled10", last10);
    let last60 = Number(this.settings.get("rolled60") ?? 0);
    if (!last60) { // the first time, start from the oldest 10-minute row, so the history already there gets its hours
      const oldest = this.all("SELECT MIN(t) AS t FROM metrics_10m")[0]?.t;
      last60 = (oldest ? oldest - (oldest % hour) : now - (now % hour)) - hour;
    }
    for (let b = last60 + hour, n = 0; b + hour <= last10 + ten && n < 300; b += hour, n++) {
      this.rollupBucket("metrics_1h", "metrics_10m", b, hour);
      last60 = b;
    }
    if (String(last60) !== this.settings.get("rolled60")) this.setSetting("rolled60", last60);
  }

  // Columns for charts: t[i], and for each machine its host fields and each app's fields as arrays aligned to t.
  history(rangeName, now) {
    const [span, step, table] = RANGES[rangeName];
    const end = now - (now % step);
    const from = end - span;
    const buckets = new Map();
    for (const row of this.all(`SELECT t, data FROM ${table} WHERE t >= ? ORDER BY t`, from)) {
      const b = row.t - (row.t % step);
      (buckets.get(b) ?? buckets.set(b, []).get(b)).push(JSON.parse(row.data));
    }
    const t = [];
    for (let b = from; b <= end; b += step) t.push(b);
    const machines = {};
    const col = (obj, k) => (obj[k] ??= new Array(t.length).fill(null));
    t.forEach((b, i) => {
      const rows = buckets.get(b);
      if (!rows) return;
      for (const [m, x] of Object.entries(rows.length > 1 ? mergeFleet(rows) : rows[0])) {
        const mm = (machines[m] ??= { h: {}, a: {} });
        for (const [k, v] of Object.entries(x.h ?? {})) col(mm.h, k)[i] = v;
        for (const [app, a] of Object.entries(x.a ?? {})) {
          const aa = (mm.a[app] ??= {});
          for (const [k, v] of Object.entries(a)) col(aa, k)[i] = v;
        }
      }
    });
    return { step, t, m: machines };
  }

  // The newest sample from every machine that's reporting.
  liveNow(now) {
    const live = {};
    for (const [m, x] of this.liveMetrics) {
      const r = this.runs.get(x.run);
      if (now - x.t > this.liveMs || !r) continue;
      live[m] = { ...x, ready: Boolean(r.ready), status: r.status, label: r.label };
    }
    return live;
  }

  // The history is built at most every 15 s for the short ranges, every minute for the day, every 5 minutes for the
  // week and month (their rows only change hourly); the live part is fresh every time.
  metricsResponse(rangeName) {
    const range = RANGES[rangeName];
    if (!range) throw new HttpError(400, `range is one of ${Object.keys(RANGES).join(", ")}`);
    const now = Date.now();
    const ttl = range[1] >= 60 * MIN ? 5 * MIN : range[1] >= 10 * MIN ? MIN : 15_000;
    let c = this.metricsCache.get(rangeName);
    if (!c || now - c.at > ttl) {
      c = { at: now, history: this.history(rangeName, now) };
      this.metricsCache.set(rangeName, c);
    }
    return Response.json({
      now, range: rangeName, expected: this.expectedMachines(),
      projects: [...this.projects.values()].map((p) => this.describe(p)).sort((a, b) => a.name.localeCompare(b.name)),
      live: this.liveNow(now), ...c.history,
    }, { headers: { "cache-control": "no-store" } });
  }

  status() {
    const now = Date.now();
    return {
      now,
      domain: this.env.DOMAIN,
      pools: Object.entries(this.pools()).map(([name, size]) => ({ name, size, live: new Set(this.liveRuns(now).filter((r) => r.pool === name).map((r) => r.machine)).size })),
      expected: this.expectedMachines(),
      dnsError: this.dnsError,
      dnsNotes: this.dnsNotes ?? [],
      rebalance: {
        on: this.rebalanceOn(),
        log: this.recentEvents.filter((e) => ["move-start", "move-done", "move-cancel", "rebalance"].includes(e.kind)).slice(-10).reverse()
          .map((e) => ({ t: e.t, text: eventText(e) })),
        hot: [...this.liveMachines(now).values()].filter((r) => this.hotness(r, now)).map((r) => r.machine),
        paused: this.paused(now) ? Number(this.settings.get("rebalance_paused_until")) : null,
        state: !this.rebalanceOn() ? "off" : this.paused(now) ? "paused" : this.quiet(now) ? "quiet" : "on",
        inFlight: [...this.copies.values()].reduce((n, list) => n + list.filter((x) => x.leaving).length, 0),
        cap: Math.max(1, Math.min(8, Math.ceil(0.1 * [...this.liveMachines(now).values()].filter((r) => r.ready && now - r.started > SETTLED_MS).length))),
      },
      // A roll under way: since when, and how many servers it still has to replace.
      roll: (() => {
        const left = this.liveRuns(now).filter((r) => !r.retire && !this.predecessorOf(r, now) && this.rolled(r)).length;
        return left ? { since: Number(this.settings.get("roll") ?? 0) || null, left } : null;
      })(),
      churn: this.churn(now),
      departures: this.departures(now),
      dark: [...this.dark].map(([key, d]) => ({ app: key.slice(0, key.lastIndexOf("|")), replica: Number(key.slice(key.lastIndexOf("|") + 1)), since: d.since, machine: d.machine, why: d.why })),
      cold: (() => { // dark spells in the last day: how many, and how long in all (seconds)
        const done = this.all("SELECT COUNT(*) AS n, COALESCE(SUM(other), 0) AS s FROM events WHERE kind = 'lit' AND t > ?", now - 86400_000)[0];
        return { spells: done.n + this.dark.size, seconds: done.s + [...this.dark.values()].reduce((a, d) => a + Math.round((now - d.since) / 1000), 0) };
      })(),
      alerts: this.recentEvents.filter((e) => e.kind === "alert" && now - e.t < 24 * 3600_000).slice(-20).reverse()
        .map((e) => ({ t: e.t, cause: e.cause, app: e.app, machine: e.machine, text: e.detail })),
      events: this.recentEvents.slice(-50).reverse().map((e) => ({ ...e, text: eventText(e) })),
      // Slots to show: ones with a recent run, plus ones a machine is starting for.
      starting: [...this.starts].filter(([n, at]) => now - at < START_WAIT_MS && !this.liveRuns(now).some((r) => r.machine === n)).map(([n]) => n),
      settling: this.settling(now), // just (re)started, or reachable again after a gap: machines not heard from yet aren't down
      slots: [...new Set([
        ...[...this.runs.values()].filter((r) => now - r.seen < 3600_000).map((r) => r.machine),
        ...[...this.starts].filter(([, at]) => now - at < START_WAIT_MS).map(([n]) => n),
      ])].sort((a, b) => a - b),
      projects: [...this.projects.values()].sort((a, b) => a.name.localeCompare(b.name)).map((p) => this.describe(p)),
      runs: [...this.runs.values()]
        .filter((r) => now - r.seen < 3600_000)
        .sort((a, b) => a.machine - b.machine || a.started - b.started)
        .map((r) => ({
          id: r.id,
          machine: r.machine,
          started: r.started,
          seen: r.seen,
          live: this.live(r, now),
          ready: Boolean(r.ready),
          handover: Boolean(r.handover),
          retiring: Boolean(r.retire),
          pool: r.pool ?? null,
          draining: Boolean(r.drain),
          label: r.label,
          rtt: r.rtt ?? null, // ms, the agent's last check-in round trip: the machine's latency to the control plane
          quarantine: (r.quarantine ?? 0) > now ? { until: r.quarantine, why: this.sickWhy.get(r.id) ?? null, sick: this.sickWhy.has(r.id) } : null,
          deadline: r.deadline ?? null,
          dueAt: this.dueAt(r),
          departing: r.handover ? "handover" : r.evict ? "evict" : null,
          edges: r.edges ?? null, // the tunnel's live connections to Cloudflare's edge (0: nothing reaches it)
          cpus: r.cpus ?? null,
          disk: r.disk ?? null,
          strikes: (this.strikes.get(r.id) ?? []).filter((x) => now - x.t < STRIKE_WINDOW_MS).map((x) => x.why),
          location: r.location ?? null,
          projects: r.status,
        })),
    };
  }

  // ---- DNS ----
  // For every project with a port: <project>-<k>.DOMAIN (k from 1 to its replica count) is a proxied CNAME to the tunnel
  // of the machine running replica k, moved when the copy moves. Names whose replica has no copy yet keep their record,
  // if any. Our records are the CNAMEs pointing at our tunnels: the ones no longer wanted (a removed replica) are
  // deleted. Records that aren't ours are left alone and noted.

  scheduleDns() {
    this.dnsDue = Date.now() + 2_000;
    this.setAlarm(this.dnsDue);
  }

  // The alarm keeps DNS in line: hourly, or 2s after a change.
  async alarm() {
    const now = Date.now();
    if (now >= (this.dnsDue ?? 0)) {
      const due = this.dnsDue; // a change during the sync asks for another one (scheduleDns), which must stand
      try {
        if (this.env.DNS === "off") this.dnsNotes = ["DNS sync is switched off (DNS=off)"];
        else await this.syncDns();
        this.dnsError = null;
        this.dnsSyncedAt = now;
        if (this.dnsDue === due) this.dnsDue = now + 3600_000; // re-check hourly in case something drifted
      } catch (e) {
        this.dnsError = e.message;
        console.log(`DNS sync failed: ${e.message}`);
        if (this.dnsDue === due) this.dnsDue = now + 60_000;
      }
    }
    this.setAlarm(this.dnsDue);
  }

  async syncDns() {
    const domain = this.env.DOMAIN;
    const zone = this.env.ZONE;
    const now = Date.now();
    const ours = new Set([...this.slots.values()].map((x) => `${x.tunnel}.cfargotunnel.com`));
    const want = new Map([...this.dnsTargets(now)].map(([label, tunnel]) => [`${label}.${domain}`, `${tunnel}.cfargotunnel.com`]));
    const names = new Set(); // every replica name, with a target or not
    for (const p of this.projects.values()) {
      const spec = this.version(p.name, p.version);
      if (!spec?.port) continue;
      for (let k = 1; k <= spec.replicas; k++) names.add(`${p.name}-${k}.${domain}`);
    }
    const existing = [];
    for (let page = 1; ; page++) {
      const data = await this.cf(`/zones/${zone}/dns_records?per_page=1000&page=${page}`);
      existing.push(...data.result);
      if (page >= (data.result_info?.total_pages ?? 1)) break;
    }
    const deletes = existing.filter((r) => r.type === "CNAME" && ours.has(r.content) && !names.has(r.name));
    const gone = new Set(deletes.map((r) => r.id));
    const byName = new Map();
    for (const r of existing) if (!gone.has(r.id)) (byName.get(r.name) ?? byName.set(r.name, []).get(r.name)).push(r);
    const notes = [];
    const posts = [];
    const patches = [];
    const repointed = [];
    for (const [name, content] of want) {
      const rs = byName.get(name) ?? [];
      const r = rs.find((x) => x.type === "CNAME");
      if (!rs.length) posts.push({ type: "CNAME", name, content, proxied: true, comment: DNS_COMMENT });
      else if (r && r.content !== content && ours.has(r.content)) {
        patches.push({ id: r.id, content });
        repointed.push([name, content]);
      }
      else if (!r || !ours.has(r.content)) notes.push(`${name} is already used by another DNS record, so it can't point at its server`);
    }
    // At most 100 changes per batch (the free plan's limit is 200), deletes first; a batch applies its own deletes
    // before its posts, and nothing at all if one change fails.
    const ops = [...deletes.map((r) => ["deletes", { id: r.id }]), ...patches.map((x) => ["patches", x]), ...posts.map((x) => ["posts", x])];
    for (let i = 0; i < ops.length; i += 100) {
      const batch = {};
      for (const [kind, x] of ops.slice(i, i + 100)) (batch[kind] ??= []).push(x);
      await this.cf(`/zones/${zone}/dns_records/batch`, { method: "POST", body: JSON.stringify(batch) });
    }
    if (ops.length) console.log(`DNS: ${deletes.length} deleted, ${patches.length} moved, ${posts.length} added`);
    for (const [name, content] of repointed) {
      const m = name.slice(0, -(domain.length + 1)).match(/^(.+)-(\d+)$/);
      const slot = [...this.slots.values()].find((x) => `${x.tunnel}.cfargotunnel.com` === content);
      if (m) this.logEvent("dns", { app: m[1], replica: Number(m[2]), machine: slot?.n ?? null, t: now });
    }
    this.dnsNotes = notes;
  }
}
