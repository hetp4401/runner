// Runner agent: keeps one machine's projects in line with the control plane.
// It joins the fleet (the control plane gives it a slot n and the token for tunnel runner-n), starts a local router
// (each copy of a project answers its replica's public name, <project>-<k>, which DNS points at this machine's tunnel)
// and the tunnel, checks in every few seconds, starts, updates and removes docker compose projects to match what it's
// told, and restarts ones that stop answering. It doesn't know what kind of machine it's on: the settings below
// describe it.
// No dependencies: Node's built-ins plus the docker CLI.
//   CONTROL_URL, JOIN_TOKEN   the control plane and the fleet's join token (required)
//   RUNNER_DATA   where projects and the agent's ID live (default /var/lib/runner)
//   AGENT_ID      this agent's ID (default: one made up once and kept in RUNNER_DATA, so a restart gets its slot back)
//   SERVER_TAG    required origin group, e.g. vps, lab, or standalone (POOL is a legacy tag alias)
//   MACHINE       the slot to take, when the machine was started for one
//   LABEL         what the pages call it (optional; without one it's just its machine number)
//   START_CMD     a shell command that starts this machine's replacement for slot $SLOT when handed over; without it, a
//                 handover restarts the agent (where it runs under a supervisor, it comes back with the latest code)
//   LIFETIME_MIN  the most this machine runs, in minutes, when whatever runs it stops it then: the control plane plans
//                 its departure before that. Without it, the machine has no deadline.
import { execFile, spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import * as fsp from "node:fs/promises";
import http from "node:http";
import { availableParallelism } from "node:os";
import { resolve } from "node:path";
import { startMetrics } from "./metrics.mjs";
import { ControlLink } from "./control-link.mjs";
import { namespace, projectName, fingerprint, resumeState, resolveServerTag, ownsCompose, ownsInfrastructure, containerHealth,
  privateDirectory, stageDeployment, writeDataFiles, bindLayout, validateBindSources, cacheAgentBundle, downloadAgentBundle, OWNER_LABEL, COPY_LABEL } from "./runtime.mjs";

const env = process.env;
if (env.REGION && !/^[a-z][a-z0-9-]{0,31}$/.test(env.REGION)) throw new Error("REGION must start with a letter and contain up to 32 lowercase letters, digits or dashes");
const base = resolve(env.RUNNER_DATA || "/var/lib/runner");
const bootId = (await readFile("/proc/sys/kernel/random/boot_id", "utf8").catch(() => "")).trim();
let saved = null;
try { saved = JSON.parse(await readFile(`${base}/agent-state.json`, "utf8")); } catch (error) { if (error.code !== "ENOENT") throw new Error("cannot read the saved agent inventory; restore it before restarting"); }
const resumed = resumeState(saved, bootId, env.CONTROL_URL, Date.now(), env.RUNNER_INSTALL_REVISION);
const tag = resolveServerTag(env, saved);
const pool = tag === "standalone" ? null : tag;
const started = resumed.started;
const owner = namespace(env.CONTROL_URL);
const settleBy = started + 10 * 60_000; // open the tunnel by then even if a project is still struggling
const ROUTER_PORT = Number(env.ROUTER_PORT) || 19080; // the tunnel sends everything here, and the router (Caddy) picks the project by hostname
const METRICS_PORT = ROUTER_PORT + 1; // the tunnel's own metrics (its live edge connections)
const ADMIN_PORT = ROUTER_PORT + 2;
const dir = `${base}/projects`;
const routerDir = `${base}/router`;
const deploymentsDir = `${base}/deployments`;
let routerName = `${owner}-router`, tunnelName = `${owner}-tunnel`;
let machine = saved?.machine || 0; // the slot, from the control plane
let tunnelToken = saved?.tunnelToken || "";
let agent = saved?.agent || "";
let run = resumed.run; // crash resumes the run; host reboot or deliberate upgrade starts a new incarnation
let previousRun = resumed.previousRun;
let restartRequested = false;
let wasReady = saved?.wasReady === true;
let recovery = saved?.recovery || null;
let cachedDesired = saved?.desired || {};
// What this agent tells the control plane about its machine.
const deadline = Number(env.LIFETIME_MIN) > 0 ? started + Number(env.LIFETIME_MIN) * 60_000 : undefined;
const describe = () => ({
  tag,
  pool,
  label: env.LABEL || null,
  region: env.REGION || undefined,
  starts: Boolean(env.START_CMD),
  deadline, // when this machine will be stopped, if it knows (LIFETIME_MIN)
  cpus: availableParallelism(), // its cores: how much of it an app's CPU use is
});
// Commands run without secrets (anything named like a token, key or password), so a compose file can't read them.
const cleanEnv = Object.fromEntries(Object.entries(env).filter(([k]) => !/TOKEN|SECRET|PASSWORD|KEY/i.test(k)));

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const log = (msg) => console.log(`${new Date().toISOString().slice(11, 19)} ${msg}`);

function sh(cmd, args, { timeout = 15 * 60_000, extraEnv = {} } = {}) {
  return new Promise((resolve) => {
    execFile(cmd, args, { timeout, maxBuffer: 32 << 20, env: { ...cleanEnv, ...extraEnv } }, (err, stdout, stderr) =>
      resolve({ ok: !err, out: `${stdout}${stderr}`.trim() }),
    );
  });
}

// Each copy answers its replica's public name, <app>-<k>.<domain>, which DNS points at this machine's tunnel while the
// copy runs here.
const replicaHost = (app, replica) => `${app}-${replica}.${domain}`;

// GET a path (/ by default) for a hostname through the local router: { code, routed }, with code 0 if nothing answered.
// The router's own "no such project" 404 carries X-Runner-Route: none, so it isn't mistaken for the project's.
function probe(host, path = "/") {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port: ROUTER_PORT, path, headers: { host }, timeout: 5000 }, (res) => {
      res.resume();
      resolve({ code: res.statusCode, routed: res.headers["x-runner-route"] !== "none" });
    });
    req.on("timeout", () => req.destroy());
    req.on("error", () => resolve({ code: 0, routed: false }));
  });
}

// True once the project answers through the router, the way real traffic reaches it. Any status counts
// except the router's "no such project" and its 502 for "the project isn't answering".
async function answers(name, seconds) {
  const end = Date.now() + seconds * 1000;
  const p = projects.get(name);
  for (;;) {
    const { code, routed } = await probe(replicaHost(p.app, p.replica));
    if (code && routed && code !== 502) return true;
    if (Date.now() >= end) return false;
    await sleep(2000);
  }
}

// Whether a copy is ready by its app's own readiness path (x-runner.ready): a 2xx answer. Readiness is only reported:
// a copy that isn't ready is never restarted for it (liveness, above, decides that), so an app waiting for its peers
// (a quorum it can't reach yet, say) isn't restarted in a loop.
async function isReady(p) {
  if (!p.readyPath) return undefined;
  const { code, routed } = await probe(replicaHost(p.app, p.replica), p.readyPath);
  return routed && code >= 200 && code < 300;
}

// ---- projects ----

// Keyed by what the control plane calls each copy: a project's name for the lowest-numbered replica of it here (the
// usual, only copy), <name>-r<k> for any further copy of it on this machine.
const projects = new Map((saved?.projects || []).map(([key, value]) => [key, { ...value,
  fingerprint: value.fingerprint || (saved?.desired?.[key] ? fingerprint(saved.desired[key]) : null),
  busy: false, s: value.busy ? "failed" : value.s }]));
let domain = saved?.domain || "";
let stateWrites = Promise.resolve();
function saveState() {
  const data = JSON.stringify({ v: 2, bootId, control: env.CONTROL_URL, started, machine, tunnelToken, agent, run, previousRun, restartRequested, wasReady,
    installedRevision: env.RUNNER_INSTALL_REVISION || saved?.installedRevision, domain, tag,
    recovery, desired: cachedDesired, projects: [...projects], metrics: metrics.history() });
  stateWrites = stateWrites.catch(() => {}).then(async () => {
    const path = `${base}/agent-state.json`;
    const temporary = `${path}.${randomUUID()}.tmp`;
    const file = await fsp.open(temporary, "wx", 0o600);
    try { await file.writeFile(data); await file.sync(); } finally { await file.close(); }
    await fsp.rename(temporary, path);
    const directory = await fsp.open(base, "r");
    try { await directory.sync(); } finally { await directory.close(); }
  });
  return stateWrites;
}

// Inspect by immutable container IDs. Never run an app-writable Compose file to
// discover or stop containers; its author can alter relative bind-mounted files.
async function inspectContainers(ids) {
  if (!ids.length) return [];
  const result = await sh("docker", ["inspect", ...ids], { timeout: 30_000 });
  if (!result.ok) throw new Error("could not inspect Docker containers");
  return JSON.parse(result.out);
}
async function projectContainers(project) {
  const result = await sh("docker", ["ps", "-aq", "--filter", `label=com.docker.compose.project=${project}`], { timeout: 30_000 });
  if (!result.ok) throw new Error("could not list Docker containers");
  return inspectContainers(result.out.split(/\s+/).filter(Boolean));
}
const ownedCopy = (container, name) => ownsCompose(container, { owner, key: name, legacyDir: `${dir}/${name}` });
async function selectProject(name, previous) {
  const named = projectName(owner, name);
  if (previous?.composeProject) {
    if (![name, named].includes(previous.composeProject)) throw new Error("saved Compose identity does not belong to this fleet");
    const containers = await projectContainers(previous.composeProject);
    if (containers.some((c) => !ownedCopy(c, name))) throw new Error("refusing to change a Compose project with unrelated containers");
    return previous.composeProject;
  }
  const current = await projectContainers(named);
  if (current.some((c) => !ownedCopy(c, name))) throw new Error("the fleet's Compose name is already owned by another application");
  if (current.length) return named;
  const legacy = await projectContainers(name);
  // Preserve legacy project names only for existing, proven-owned containers.
  // This preserves their named volumes and lets applications migrate on updates.
  return legacy.length && legacy.every((c) => ownedCopy(c, name)) ? name : named;
}
async function ownedContainers(name, p) {
  p.composeProject ||= await selectProject(name, p);
  const containers = await projectContainers(p.composeProject);
  if (containers.some((c) => !ownedCopy(c, name))) throw new Error("refusing to change unrelated Docker containers");
  return containers;
}
async function pruneDeploymentFiles(name, p, previousSource) {
  const root = `${deploymentsDir}/${name}/`;
  const keep = new Set([p.sourceDir, previousSource].filter(Boolean));
  for (const container of await ownedContainers(name, p)) {
    const labels = container.Config?.Labels ?? {};
    for (const path of [labels["com.docker.compose.project.working_dir"], ...(container.Mounts ?? []).map((m) => m.Source)]) {
      if (typeof path === "string" && path.startsWith(root)) keep.add(`${root}${path.slice(root.length).split("/")[0]}`);
    }
  }
  for (const release of await readdir(root).catch(() => [])) {
    if (release.startsWith("release-") && !keep.has(`${root}${release}`)) await rm(`${root}${release}`, { recursive: true, force: true });
  }
}

// Source and Compose definitions live in fresh, private release directories.
// App-writable bind mounts stay in projects/<key>, preserving existing data.
async function apply(name, want, { recreate = false } = {}) {
  const before = projects.get(name);
  const same = before && before.replica === (want.replica ?? 1) && before.app === (want.app ?? name);
  const p = { app: want.app ?? name, replica: want.replica ?? 1, v: want.v, port: want.port, readyPath: want.ready ?? null, s: "applying", e: "",
    fingerprint: fingerprint(want), revision: want.revision ?? null, busy: true, at: Date.now(), misses: 0,
    oneshotServices: want.oneshotServices ?? [],
    composeProject: before?.composeProject, sourceDir: before?.sourceDir,
    t0: same ? before.t0 : Date.now(), n: (same ? before.n ?? 0 : 0) + (recreate ? 1 : 0) };
  projects.set(name, p);
  updateRouter();
  log(`${name}: starting v${want.v}${recreate ? " again" : ""}`);
  let s = "healthy", e = "";
  try {
    p.composeProject = await selectProject(name, before);
    if (typeof want.managedCompose !== "string") throw new Error("update the control plane to provide safe deployment files before changing this app");
    const compose = JSON.parse(want.managedCompose);
    for (const [service, config] of Object.entries(compose.services ?? {})) {
      const labels = Array.isArray(config.labels) ? Object.fromEntries(config.labels.map((x) => { const at = x.indexOf("="); return at < 0 ? [x, ""] : [x.slice(0, at), x.slice(at + 1)]; })) : config.labels ?? {};
      config.labels = { ...labels, [OWNER_LABEL]: owner, [COPY_LABEL]: name,
        "io.runners.oneshot": want.oneshotServices?.includes(service) ? "true" : "false" };
    }
    const dataDir = `${dir}/${name}`;
    await mkdir(dataDir, { recursive: true });
    if (before?.bindSources === undefined || bindLayout(before.bindSources) !== bindLayout(want.bindSources)) {
      const containers = await ownedContainers(name, p);
      if (containers.some((c) => c.State?.Running)) {
        const stopped = await sh("docker", ["stop", "-t", "10", ...containers.filter((c) => c.State?.Running).map((c) => c.Id)]);
        if (!stopped.ok) throw new Error("could not safely stop the app before changing its bind mounts");
      }
    }
    await writeDataFiles(dataDir, want.dataFiles ?? {});
    await validateBindSources(dataDir, want.bindSources ?? []);
    p.bindSources = want.bindSources ?? [];
    const sourceDir = await stageDeployment(deploymentsDir, name, want.files ?? {}, JSON.stringify(compose));
    const file = `${sourceDir}/compose.yaml`;
    const fresh = recreate || Boolean(before && before.fingerprint !== p.fingerprint);
    const up = await sh("docker", ["compose", "-p", p.composeProject, "-f", file, "up", "-d", "--build", "--remove-orphans",
      "--wait", "--wait-timeout", "300", ...(fresh ? ["--force-recreate"] : [])], { extraEnv: { RUNNER_APP_DATA: dataDir } });
    p.sourceDir = sourceDir;
    // The controller keeps .e in authenticated diagnostics only. Preserve the
    // useful build tail here; stdout/GitHub job logs must never print this text.
    if (!up.ok) [s, e] = ["failed", up.out.slice(-4000) || "Docker could not start this application's services"];
    else {
      const containers = await ownedContainers(name, p);
      p.services = [...new Set(containers.map((c) => c.Config?.Labels?.["com.docker.compose.service"]).filter(Boolean))];
      const problem = containerHealth(containers, p.services, p.oneshotServices);
      if (problem) [s, e] = ["failed", problem];
      else if (want.port && !(await answers(name, 60))) [s, e] = ["failed", `nothing answers on port ${want.port} through the router`];
    }
  } catch (err) {
    [s, e] = ["failed", String(err.message).slice(-4000)];
  }
  Object.assign(p, { s, e, at: Date.now(), r: s === "healthy" ? await isReady(p) : undefined });
  if (p.sourceDir) await pruneDeploymentFiles(name, p, s === "healthy" ? null : before?.sourceDir).catch(() => {});
  p.busy = false;
  await saveState();
  wake();
  log(`${name}: v${want.v} ${s}${e ? " (details in authenticated diagnostics)" : ""}`);
}

// Deletes a project's folder. Files a container wrote through a bind mount belong to root, so if the plain delete is
// refused, it's done from a container instead.
async function removeDir(path) {
  try {
    await rm(path, { recursive: true, force: true });
  } catch (e) {
    const r = await sh("docker", ["run", "--rm", "-v", `${path}:/p`, "busybox", "sh", "-c", "find /p -mindepth 1 -delete"]);
    await rm(path, { recursive: true, force: true }).catch(() => {
      throw new Error(`couldn't delete ${path}: ${e.message}${r.ok ? "" : `; ${r.out.split("\n").pop()}`}`);
    });
  }
}

async function remove(name) {
  const p = projects.get(name);
  p.busy = true;
  log(`${name}: removing`);
  try {
    const containers = await ownedContainers(name, p);
    if (containers.length) {
      const removed = await sh("docker", ["rm", "-f", ...containers.map((c) => c.Id)]);
      if (!removed.ok) throw new Error("could not remove this application's Docker containers");
    }
    await removeDir(`${dir}/${name}`);
    await removeDir(`${deploymentsDir}/${name}`);
    // Whatever happened to its files, its route goes: the router then answers "no such project", which tells the
    // Worker to try another machine.
    projects.delete(name);
    updateRouter();
    await saveState();
  } catch (error) {
    p.busy = false;
    throw error;
  }
}

// Start what's new or changed, retry what failed, remove what's no longer wanted. Removals go first and new projects
// wait for them: a project on its way out may still hold the ports the new one publishes (the same copy under a new
// name, say), and compose would refuse to start it.
const queued = new Set(); // new projects waiting for removals to finish
function reconcile(desired, { repairOnly = false } = {}) {
  const removals = [];
  for (const [name, p] of projects) {
    if (!repairOnly && !(name in desired) && !p.busy) removals.push(remove(name).catch((e) => log(`${name}: ${e.message}`)));
  }
  for (const [name, want] of Object.entries(desired)) {
    const p = projects.get(name);
    if (p?.busy || queued.has(name)) continue;
    if (repairOnly && (!p || p.fingerprint !== fingerprint(want))) continue;
    if (!p) {
      queued.add(name);
      Promise.all(removals).then(() => {
        queued.delete(name);
        if (!projects.has(name)) return apply(name, want);
      }).catch((e) => log(`${name}: ${e.message}`));
      continue;
    }
    if (p.fingerprint === fingerprint(want)) {
      if (p.v !== want.v) { p.v = want.v; wake(); }
      p.revision = want.revision ?? p.revision;
      p.services = want.services ?? p.services;
      p.oneshotServices = want.oneshotServices ?? p.oneshotServices;
    }
    // A failed project is tried again: soon while the machine is still starting up (a hiccup mustn't hold its tunnel
    // back for long), every 3 minutes once it's online.
    const retry = p.s === "failed" && Date.now() - p.at > (ready ? 3 * 60_000 : 30_000);
    // A different replica under the same key (the control plane gave the key to another copy) is a different copy:
    // its .env differs, so it's applied again even at the same version.
    const other = p.replica !== (want.replica ?? 1) || p.app !== (want.app ?? name);
    if (p.fingerprint !== fingerprint(want) || other || retry) {
      apply(name, want, { recreate: retry && p.v === want.v }).catch((e) => log(`${name}: ${e.message}`));
    }
  }
}

// Every wanted project runs its wanted version and answers (a project without a port counts once it's up).
const settled = (desired) => Object.entries(desired).every(([name, want]) => {
  const p = projects.get(name);
  return p && p.v === want.v && !p.busy && p.s === "healthy" && !queued.has(name);
});

// A project that stops answering three checks in a row is marked failed, which makes reconcile start it again.
// Readiness is checked alongside, and only reported.
async function checkHealth() {
  for (const [name, p] of projects) {
    if (p.busy || p.s !== "healthy") continue;
    let problem;
    try { problem = containerHealth(await ownedContainers(name, p), p.services ?? [], p.oneshotServices ?? []); }
    catch { problem = "Docker state could not be verified"; }
    if (!problem && (!p.port || await answers(name, 0))) {
      p.misses = 0;
      if (p.readyPath) p.r = await isReady(p);
    } else if (++p.misses >= 3) {
      const reason = problem || `stopped answering on port ${p.port}`;
      log(`${name}: ${reason}; restarting it`);
      Object.assign(p, { s: "failed", e: reason, at: 0, misses: 0 });
    }
  }
}

// ---- router (Caddy) and tunnel (cloudflared) ----

async function namedContainer(name) {
  const result = await sh("docker", ["inspect", name], { timeout: 30_000 });
  if (result.ok) return JSON.parse(result.out)[0] ?? null;
  if (/No such (object|container)/i.test(result.out)) return null;
  throw new Error("could not inspect Docker infrastructure");
}
const infrastructureOwner = (kind) => ({ owner, kind,
  legacySource: kind === "router" ? routerDir : `${base}/tunnel.yml`,
  destination: kind === "router" ? "/etc/router" : "/etc/cloudflared/config.yml" });
async function selectInfrastructure(kind) {
  const name = `${owner}-${kind}`;
  const current = await namedContainer(name);
  if (current && !ownsInfrastructure(current, infrastructureOwner(kind))) throw new Error(`the fleet's ${kind} container name is already in use`);
  if (current) return name;
  const legacy = await namedContainer(kind);
  return legacy && ownsInfrastructure(legacy, infrastructureOwner(kind)) ? kind : name;
}
async function removeInfrastructure(kind) {
  const name = kind === "router" ? routerName : tunnelName;
  const container = await namedContainer(name);
  if (!container) return;
  if (!ownsInfrastructure(container, infrastructureOwner(kind))) throw new Error(`refusing to remove an unrelated ${kind} container`);
  const result = await sh("docker", ["rm", "-f", container.Id]);
  if (!result.ok) throw new Error(`could not replace the ${kind} container`);
}

let routerChain = Promise.resolve();
let routerConfig = "";
// Queued so configs are applied in order; a failure is logged and the next update tries again.
const updateRouter = () => (routerChain = routerChain.then(loadRouter, loadRouter).catch((e) => log(`router update failed: ${e.message}`)));

function routerJson() {
  // Each copy answers its replica's public name. (stream_close_delay: a config reload would otherwise cut every
  // websocket on the machine.)
  const routes = [...projects.values()].filter((p) => p.port && domain).map((p) => ({
    match: [{ host: [replicaHost(p.app, p.replica)] }],
    handle: [{ handler: "reverse_proxy", upstreams: [{ dial: `127.0.0.1:${p.port}` }], flush_interval: -1, stream_close_delay: "1h" }],
  }));
  routes.push({
    handle: [{
      handler: "static_response",
      status_code: 404,
      headers: { "X-Runner-Route": ["none"] },
      body: `No such project on machine ${machine}\n`,
    }],
  });
  return JSON.stringify({
    admin: { listen: `127.0.0.1:${ADMIN_PORT}` },
    apps: {
      http: {
        servers: {
          router: {
            listen: [`127.0.0.1:${ROUTER_PORT}`],
            automatic_https: { disable: true },
            routes,
            // The tunnel is on this machine, so it's trusted: the X-Forwarded-* headers it carries (the replica's
            // public host, https, the visitor's address) reach the project instead of being replaced by this
            // machine's own. Projects that build links from them get their public address.
            trusted_proxies: { source: "static", ranges: ["127.0.0.1/32", "::1/128"] },
          },
        },
      },
    },
  });
}

// Goes through `caddy reload` rather than the admin API directly: Caddy's own CLI passes its origin checks.
async function loadRouter() {
  const body = routerJson();
  if (body === routerConfig) return;
  await writeFile(`${routerDir}/caddy.json`, body);
  let r = await sh("docker", ["exec", routerName, "caddy", "reload", "--address", `127.0.0.1:${ADMIN_PORT}`, "--config", "/etc/router/caddy.json"]);
  if (!r.ok && routerName === "router") r = await sh("docker", ["exec", routerName, "caddy", "reload", "--address", "127.0.0.1:2019", "--config", "/etc/router/caddy.json"]);
  if (r.ok) routerConfig = body;
  else log(`router update failed: ${r.out.split("\n").slice(-3).join(" ")}`);
}

async function startRouter() {
  await privateDirectory(routerDir);
  routerConfig = routerJson();
  await writeFile(`${routerDir}/caddy.json`, routerConfig);
  await removeInfrastructure("router");
  const r = await sh("docker", ["run", "-d", "--name", routerName, "--network", "host", "--restart", "unless-stopped",
    "--label", `${OWNER_LABEL}=${owner}`, "--label", "io.runners.component=router",
    "-v", `${routerDir}:/etc/router:ro`, "caddy:2", "caddy", "run", "--config", "/etc/router/caddy.json"]);
  if (!r.ok) throw new Error(`router didn't start: ${r.out}`);
  for (let i = 0; i < 30; i++) {
    if ((await probe("check")).code) return;
    await sleep(1000);
  }
  throw new Error("router is not answering");
}

async function startTunnel() {
  await writeFile(`${base}/tunnel.yml`, `ingress:\n  - service: http://127.0.0.1:${ROUTER_PORT}\n`);
  await removeInfrastructure("tunnel");
  const r = await sh("docker", ["run", "-d", "--name", tunnelName, "--network", "host", "--restart", "unless-stopped",
    "--label", `${OWNER_LABEL}=${owner}`, "--label", "io.runners.component=tunnel",
    "-e", "TUNNEL_TOKEN", "-v", `${base}/tunnel.yml:/etc/cloudflared/config.yml:ro`,
    "cloudflare/cloudflared:latest", "tunnel", "--no-autoupdate", "--metrics", `127.0.0.1:${METRICS_PORT}`, "--config", "/etc/cloudflared/config.yml", "run"],
  { extraEnv: { TUNNEL_TOKEN: tunnelToken } });
  if (!r.ok) throw new Error(`tunnel didn't start: ${r.out}`);
  for (let i = 0; i < 45; i++) {
    if ((await sh("docker", ["logs", tunnelName])).out.includes("Registered tunnel connection")) return log("tunnel online");
    await sleep(2000);
  }
  throw new Error("tunnel did not connect");
}

// How many edge connections the tunnel has (0: the machine is up but unreachable from outside), or undefined.
function tunnelEdges() {
  return new Promise((resolve) => {
    const req = http.get({ host: "127.0.0.1", port: METRICS_PORT, path: "/metrics", timeout: 3000 }, (res) => {
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => {
        const m = body.match(/^cloudflared_tunnel_ha_connections\s+(\d+)/m);
        resolve(m ? Number(m[1]) : undefined);
      });
    });
    req.on("timeout", () => req.destroy());
    req.on("error", () => resolve(undefined));
  });
}

// Free and total bytes on the disk the projects live on.
async function diskSpace() {
  try {
    if (!fsp.statfs) return undefined; // Node before 18.15
    const s = await fsp.statfs(base);
    return { free: s.bavail * s.bsize, total: s.blocks * s.bsize };
  } catch {
    return undefined;
  }
}

// ---- control plane ----

let ready = false;

// App totals retain their existing format; live replica samples distinguish
// separate copies on one host and combine all services within each copy.
const metricsSystem = new Set(["runner-agent", `${owner}-agent`]);
const metricCopy = (project) => {
  const entry = [...projects].find(([key, p]) => (p.composeProject || key) === project);
  return entry ? { key: entry[0], app: entry[1].app, replica: entry[1].replica } : null;
};
const metrics = startMetrics({ appOf: (project) => metricCopy(project)?.app ?? null, copyOf: metricCopy,
  systemContainers: metricsSystem, history: saved?.metrics ?? [] });

// Get a slot and its tunnel token. Keeps trying while the control plane is unreachable or every slot is taken; gives up
// (so whatever runs this machine can start another) if the token is refused, or, for a pool member, after 10 minutes.
async function join() {
  if (saved && machine && tunnelToken && agent && run) return log(`resuming machine ${machine} on the same VM`);
  agent = env.AGENT_ID || "";
  if (!agent) {
    const idFile = `${base}/agent-id`;
    agent = (await readFile(idFile, "utf8").catch(() => "")).trim();
    if (!agent) {
      agent = `host-${randomUUID().slice(0, 8)}`;
      await writeFile(idFile, agent);
    }
  }
  run ||= `${agent}-${started}`;
  let want = env.MACHINE ? Number(env.MACHINE) : undefined;
  const giveUpAt = Date.now() + 10 * 60_000;
  for (let wait = 5;; wait = Math.min(wait * 2, 60)) {
    try {
      const res = await fetch(`${env.CONTROL_URL}/api/join`, {
        method: "POST",
        headers: { authorization: `Bearer ${env.JOIN_TOKEN}`, "content-type": "application/json" },
        body: JSON.stringify({ agent, ...describe(), want }),
        signal: AbortSignal.timeout(30_000),
      });
      const data = await res.json().catch(() => ({}));
      if (res.status === 401 || res.status === 403) throw new Error(`the join token was refused: ${data.error ?? `HTTP ${res.status}`}`);
      if (res.status === 409 && want) { // the slot this machine was started for is in use; any other will do
        log(`slot ${want} is taken (${data.error ?? "HTTP 409"}); asking for a free one`);
        want = undefined;
        continue;
      }
      if (!res.ok) throw new Error(data.error ?? `HTTP ${res.status}`);
      if (!(Number.isInteger(data.machine) && data.machine >= 1) || typeof data.tunnelToken !== "string" || !data.tunnelToken ||
        typeof data.domain !== "string" || !data.domain) {
        throw new Error(`the control plane's answer makes no sense: ${JSON.stringify(data).slice(0, 200)}`);
      }
      ({ machine, tunnelToken, domain } = data);
      await saveState();
      return log(`joined as machine ${machine} (${agent})`);
    } catch (e) {
      if (e.message.startsWith("the join token was refused")) throw e;
      if (pool && Date.now() > giveUpAt) throw new Error(`couldn't join for 10 minutes (${e.message})`);
      log(`couldn't join (${e.message}); trying again in ${wait}s`);
      await sleep(wait * 1000);
    }
  }
}

// After a restart: projects from last time that are no longer wanted.
async function removeLeftovers(desired) {
  for (const name of await readdir(dir).catch(() => [])) {
    if (name in desired || projects.has(name)) continue;
    log(`${name}: left over from before; removing`);
    try {
      const composeProject = await selectProject(name, null);
      const containers = await projectContainers(composeProject);
      if (containers.some((c) => !ownedCopy(c, name))) throw new Error("leftover Docker containers have another owner");
      if (containers.length && !(await sh("docker", ["rm", "-f", ...containers.map((c) => c.Id)])).ok) throw new Error("could not remove leftover Docker containers");
      await removeDir(`${dir}/${name}`);
      await removeDir(`${deploymentsDir}/${name}`);
    } catch (e) {
      log(`${name}: ${e.message}`);
    }
  }
}

let rtt = null; // how long the last check-in took, machine to control plane and back: sent with the next one, shown as the machine's latency
async function sync() {
  const status = Object.fromEntries([...projects].map(([name, p]) => [name, { v: p.v, revision: p.revision, s: p.s, e: p.e || undefined, r: p.r, t0: p.t0, n: p.n }]));
  const inventory = Object.fromEntries([...projects].map(([name, p]) => [name, { app: p.app, replica: p.replica, v: p.v,
    revision: p.revision ?? undefined, ports: cachedDesired[name]?.ports ?? undefined }]));
  const sent = metrics.payload();
  const t0 = Date.now();
  const edges = ready ? await tunnelEdges() : undefined;
  const disk = await diskSpace();
  const body = { machine, run, previousRun, agent, ...describe(), started, ready, status, inventory, leaving, metrics: sent, rtt, edges, disk, protocol: 3, recovery };
  const linked = await controlLink.report(body);
  if (linked) { rtt = Date.now() - t0; metrics.confirm(sent); return linked; }
  const res = await fetch(`${env.CONTROL_URL}/api/sync`, {
    method: "POST",
    headers: { authorization: `Bearer ${env.JOIN_TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  rtt = Date.now() - t0;
  if (!res.ok) throw new Error(`HTTP ${res.status} ${(await res.text()).slice(0, 200)}`);
  metrics.confirm(sent);
  return res.json();
}

// START_CMD with SLOT=m, and the agent's full environment: it may need a token the projects don't get. It runs in the
// background (a slow one mustn't hold up the check-ins), once at a time per slot.
const starting = new Set();
function startMachine(m) {
  if (starting.has(m)) return;
  starting.add(m);
  execFile("sh", ["-c", env.START_CMD], { timeout: 120_000, env: { ...env, SLOT: String(m) } }, (err, stdout, stderr) => {
    starting.delete(m);
    log(err ? `couldn't start machine ${m}: ${`${stdout}${stderr}`.trim().split("\n").pop() || err.message}` : `started machine ${m}`);
  });
}

let stopping = false;
let leaving = false; // telling the control plane this run is going away for good (its replicas can be placed elsewhere now)
async function shutdown(reason, code = 0, { keepTunnel = false } = {}) {
  if (stopping) return;
  stopping = true;
  for (const stop of streams.values()) stop(); // (live logs: their docker processes end with them)
  log(reason);
  // Under a supervisor (Docker's restart policy, say) the agent comes back with the same slot.
  if (!keepTunnel) {
    const container = await namedContainer(tunnelName).catch(() => null);
    if (container && ownsInfrastructure(container, infrastructureOwner("tunnel"))) await sh("docker", ["stop", "-t", "10", container.Id]);
  }
  process.exit(code);
}
// Stopping an agent is a restart, regardless of its origin tag. Preserve its
// serving tunnel and inventory; deliberate departures use the drain protocol.
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    if (stopping || leaving) return;
    saveState().finally(() => shutdown("cancelled", 0, { keepTunnel: true }));
  });
}

// The outgoing WebSocket lets the controller request a fresh report immediately,
// without a second HTTP polling loop or any inbound port on the machine.
let wakeNow = null; // ends the current wait between check-ins
let wakePending = false; // a wake that came while no wait was going on: the next one ends at once
const wake = () => { if (wakeNow) wakeNow(); else wakePending = true; };
const controlLink = new ControlLink(env.CONTROL_URL, env.JOIN_TOKEN, wake);
const nap = (ms) => new Promise((resolve) => {
  if (wakePending) {
    wakePending = false;
    return resolve();
  }
  const done = () => {
    clearTimeout(timer);
    wakeNow = null;
    resolve();
  };
  const timer = setTimeout(done, ms);
  wakeNow = done;
});

// Live logs of a copy someone is watching (the control plane names the session in a check-in, having woken the agent):
// `docker compose logs --follow` of the copy's containers, the last `tail` lines first, sent to the control plane as
// they come, in whole lines at most every quarter second, over a WebSocket the agent opens. It stops when the viewer
// leaves (the control plane closes the socket) or the containers go. At most 4 at once. Without ws.mjs (an install
// from before live logs) the agent just doesn't stream.
const wsClient = import("./ws.mjs").catch(() => null);
const streams = new Map(); // session -> stop()
function streamLogs(wanted) {
  for (const w of Array.isArray(wanted) ? wanted : []) {
    const session = String(w?.session ?? "");
    const key = String(w?.key ?? "");
    if (!/^[0-9a-f-]{36}$/.test(session) || streams.has(session) || streams.size >= 4 || !projects.has(key)) continue;
    streams.set(session, () => {});
    (async () => {
      const ws = await wsClient;
      if (!ws) throw new Error("this install has no ws.mjs");
      const conn = await ws.connect(`${env.CONTROL_URL}/api/logs/stream?session=${session}&run=${encodeURIComponent(run)}`, { authorization: `Bearer ${env.JOIN_TOKEN}` });
      const tail = Math.max(0, Math.min(1000, Number(w.tail) || 200));
      const containers = await ownedContainers(key, projects.get(key));
      const children = containers.map((container) => spawn("docker", ["logs", "--follow", "--timestamps", "--tail", String(tail), container.Id],
        { env: cleanEnv, stdio: ["ignore", "pipe", "pipe"] }));
      if (!children.length) throw new Error("the application's containers are no longer present");
      let pending = "";
      let timer = null;
      const flush = (all = false) => {
        clearTimeout(timer);
        timer = null;
        const cut = all ? pending.length : pending.lastIndexOf("\n") + 1;
        if (!cut) return;
        const out = pending.slice(0, cut);
        pending = pending.slice(cut);
        if (!conn.send(out)) { // backed up: wait for the socket before reading more
          for (const child of children) { child.stdout.pause(); child.stderr.pause(); }
          conn.socket.once("drain", () => { for (const child of children) { child.stdout.resume(); child.stderr.resume(); } });
        }
      };
      const take = (d) => {
        pending += d;
        if (pending.length > 64_000) flush(true);
        else timer ??= setTimeout(flush, 250);
      };
      let remaining = children.length;
      for (const child of children) {
        child.stdout.setEncoding("utf8").on("data", take);
        child.stderr.setEncoding("utf8").on("data", take);
        child.on("error", () => conn.close());
        child.on("close", () => { if (!--remaining) { flush(true); conn.close(); } });
      }
      streams.set(session, () => conn.close());
      await new Promise((resolve) => conn.onEnd(() => {
        for (const child of children) child.kill();
        resolve();
      }));
    })().catch((e) => log(`live logs of ${key}: ${e.message}`)).finally(() => streams.delete(session));
  }
}

async function main() {
  await privateDirectory(dir);
  await privateDirectory(deploymentsDir);
  await join();
  log(`machine ${machine}${describe().label ? `: ${describe().label}` : ""}, tag ${tag}`);
  routerName = await selectInfrastructure("router");
  tunnelName = await selectInfrastructure("tunnel");
  metricsSystem.add(routerName); metricsSystem.add(tunnelName);
  // Existing owned infrastructure and containers keep serving through an agent
  // restart or host reboot. Recreate only infrastructure that is actually down.
  for (;;) {
    try {
      const router = await namedContainer(routerName);
      if (!router?.State?.Running) await startRouter();
      else { await privateDirectory(routerDir); await updateRouter(); }
      ready = Boolean((await namedContainer(tunnelName))?.State?.Running);
      if (ready) wasReady = true;
      break;
    } catch (e) {
      log(`${e.message.split("\n")[0]}; trying again in 30s`);
      await sleep(30_000);
    }
  }
  let cleaned = false;
  let successorAt = 0;
  let lastReport = 0;
  let tunnelRetryAt = 0;
  let checking = false;
  let lastUpgradeAttempt = 0;
  setInterval(async () => {
    if (checking || stopping || leaving) return;
    checking = true;
    try {
      const before = JSON.stringify([...projects].map(([k, p]) => [k, p.s, p.r, p.n]));
      if (!(await namedContainer(routerName))?.State?.Running) await startRouter();
      if (!(await namedContainer(tunnelName))?.State?.Running && wasReady) {
        ready = false;
        await startTunnel();
        ready = true;
        wake();
      }
      await checkHealth();
      // Repair an already-authorized workload even while the control plane is
      // unreachable. Placement changes and new workloads still need a plan.
      reconcile(cachedDesired, { repairOnly: true });
      const changed = before !== JSON.stringify([...projects].map(([k, p]) => [k, p.s, p.r, p.n]));
      if (changed) { await saveState(); wake(); }
    } catch (error) { log(`health: ${error.message}`); }
    finally { checking = false; }
  }, 10_000).unref();
  for (;;) {
    if (leaving) { // a signal handler is checking in for the last time; don't start anything meanwhile
      await sleep(1000);
      continue;
    }
    let plan = null;
    try {
      plan = await sync();
    } catch (e) {
      log(`control plane unreachable (${e.message}); keeping what's running`);
    }
    if (plan?.retire && !plan.hold) return shutdown("a newer run of this machine is healthy, so this one is leaving");
    if (plan && !plan.hold) {
      if (!plan.desired || typeof plan.desired !== "object") throw new Error("controller sent no desired state");
      recovery = plan.recovery ?? recovery;
      if (plan.recovery?.payload) {
        try { if (JSON.parse(plan.recovery.payload).run === run) previousRun = null; } catch {}
      }
      cachedDesired = plan.desired;
      await saveState(); // accept the receipt durably before executing commands
      if (plan.domain !== domain) {
        domain = plan.domain;
        updateRouter();
      }
      reconcile(plan.desired);
      streamLogs(plan.logs);
      if (!cleaned) {
        cleaned = true;
        await removeLeftovers(plan.desired);
      }
      if (plan.handover && !env.START_CMD && Date.now() - lastUpgradeAttempt >= 60_000) {
        lastUpgradeAttempt = Date.now();
        try {
          if (env.RUNNER_CODE_MANAGED === "1") await cacheAgentBundle(base, await downloadAgentBundle(env.CONTROL_URL));
          restartRequested = true;
          await saveState();
          return shutdown("handing over: restarting the agent", 0, { keepTunnel: true });
        } catch (error) {
          restartRequested = false;
          log(`agent upgrade failed (${error.message}); keeping the current agent and applications`);
          wake();
        }
      }
      if (plan.handover && env.START_CMD && Date.now() - successorAt > 10 * 60_000) {
        successorAt = Date.now();
        log("handing over: starting a replacement for this machine");
        startMachine(machine);
      }
    }
    // Open the tunnel only once the projects are up and answering, so a fresh run doesn't take traffic it can't serve
    // yet (after 10 minutes, open it anyway: the control plane sees what's failing). A successor (an older run of this
    // machine is still serving) opens once it serves everything its predecessor did (the control plane lists those
    // copies), with no 10-minute fallback: the predecessor keeps serving meanwhile. A tunnel that won't connect is
    // tried again in a minute.
    const serves = (key) => projects.get(key)?.s === "healthy" && !projects.get(key)?.busy && projects.get(key)?.r !== false;
    const openNow = plan?.hold ? false : plan?.successor
      ? Array.isArray(plan.serve) && plan.serve.every(serves) && (plan.serve.length > 0 || settled(plan.desired))
      : plan && (settled(plan.desired) || Date.now() > settleBy);
    if (!ready && plan && Date.now() >= tunnelRetryAt && openNow) {
      try {
        await startTunnel();
        ready = true;
        wasReady = true;
        continue; // check in straight away as ready
      } catch (e) {
        log(`${e.message.split("\n")[0]}; trying again in a minute`);
        tunnelRetryAt = Date.now() + 60_000;
      }
    }
    await saveState();
    if (Date.now() - lastReport > 5 * 60_000) {
      lastReport = Date.now();
      const summary = [...projects].map(([name, p]) => `${name} v${p.v} ${p.s}`).join(", ") || "no projects";
      log(`${ready ? "online" : "starting"}: ${summary}`);
    }
    await nap((plan?.poll ?? 20) * 1000);
  }
}

main().catch((e) => shutdown(`agent failed: ${e.message}`, 1, { keepTunnel: true }));
