// Provider capacity belongs here, outside the control plane and agent protocol.
// The watchdog workflow serializes invocations for each repository.
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const ACTIVE_STATUSES = ["requested", "pending", "waiting", "queued", "in_progress"];
const LAUNCH_WINDOW_MS = 10 * 60_000;

export function targetCount(env) {
  const raw = env.MACHINE_COUNT || env.POOL_SIZE || "0";
  if (!/^(0|[1-9]\d*)$/.test(raw) || Number(raw) > 50) throw new Error("MACHINE_COUNT must be a whole number from 0 to 50");
  return Number(raw);
}

export function capacityPlan(target, active, recent, now) {
  const runs = new Map();
  for (const run of [...active, ...recent]) {
    if (!Number.isSafeInteger(run.id) || run.id <= 0 || typeof run.status !== "string") throw new Error("invalid workflow run inventory");
    if (run.status !== "completed" && !ACTIVE_STATUSES.includes(run.status)) throw new Error("unknown workflow run status");
    runs.set(run.id, run); // the later recent listing resolves status transitions
  }
  const slots = new Set();
  for (const run of runs.values()) {
    if (!ACTIVE_STATUSES.includes(run.status)) continue;
    const title = run.display_title || run.name || "";
    const slot = /^machine ([1-9]\d*)$/.exec(title.trim());
    // A predecessor and its replacement reserve one existing slot. Fresh
    // machines use input 0 and reserve one place per run until they join.
    // A fresh host's first handover may temporarily overcount capacity, which
    // delays a refill rather than creating surplus hosts.
    slots.add(slot ? `slot:${slot[1]}` : `run:${run.id}`);
  }
  const created = new Set(recent.filter((run) => {
    const at = Date.parse(run.created_at);
    if (!Number.isFinite(at)) throw new Error("invalid workflow run creation time");
    return at >= now - LAUNCH_WINDOW_MS;
  }).map((run) => run.id)).size;
  // Failed or very short runs must not create a completion/refill feedback
  // loop. At most one target's worth of launches is requested per ten minutes.
  const missing = Math.max(0, target - slots.size);
  const budget = Math.max(0, target - created);
  const launch = Math.min(missing, budget);
  return { active: slots.size, missing, launch, deferred: missing - launch };
}

export function githubClient(token, base = "https://api.github.com", fetcher = fetch) {
  const origin = new URL(base);
  if (origin.protocol !== "https:" || origin.username || origin.password || origin.search || origin.hash) throw new Error("invalid GitHub API URL");
  return async (path, { method = "GET", body } = {}) => {
    const response = await fetcher(`${base.replace(/\/$/, "")}${path}`, {
      method,
      headers: { authorization: `Bearer ${token}`, accept: "application/vnd.github+json", "content-type": "application/json",
        "x-github-api-version": "2026-03-10", "user-agent": "runner-provider-capacity" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(20_000),
    });
    if (!response.ok) throw new Error(`GitHub ${method} ${path}: HTTP ${response.status}`);
    return response.status === 204 ? null : response.json();
  };
}

export async function reconcileCapacity(env, { api, readStatus, now = Date.now, log = console.log } = {}) {
  const target = targetCount(env);
  if (!target) {
    log("Provider refill disabled (MACHINE_COUNT is unset or zero).");
    return { target, launched: 0 };
  }
  const repo = env.GITHUB_REPOSITORY;
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repo || "")) throw new Error("GITHUB_REPOSITORY is required");
  if (!env.GH_TOKEN && !api) throw new Error("GH_TOKEN is required");
  const tag = env.SERVER_TAG || env.POOL || "github";
  if (!/^[a-z0-9][a-z0-9-]{0,29}$/.test(tag)) throw new Error("invalid SERVER_TAG");
  const ref = env.MACHINE_REF || env.GITHUB_REF_NAME;
  if (!ref || /[\r\n\0]/.test(ref)) throw new Error("MACHINE_REF is required");
  const controller = new URL(env.CONTROL_URL);
  if (controller.protocol !== "https:" || controller.username || controller.password || controller.pathname !== "/" || controller.search || controller.hash) throw new Error("CONTROL_URL must be an HTTPS origin");
  api ||= githubClient(env.GH_TOKEN, env.GITHUB_API_URL);
  readStatus ||= async () => {
    const response = await fetch(new URL("/api/status", controller), { signal: AbortSignal.timeout(20_000) });
    if (!response.ok) throw new Error(`Controller status: HTTP ${response.status}`);
    return response.json();
  };
  const status = await readStatus();
  if (status.fleetMode !== "passive" || !Array.isArray(status.groups)) throw new Error("provider refill requires a passive controller");
  const endpoint = `/repos/${repo}/actions/workflows/machine.yml`;
  const active = (await Promise.all(ACTIVE_STATUSES.map(async (state) => {
    const runs = [];
    for (let page = 1; page <= 10; page++) {
      const result = await api(`${endpoint}/runs?status=${state}&per_page=100&page=${page}`);
      if (!Array.isArray(result.workflow_runs)) throw new Error("invalid workflow run listing");
      runs.push(...result.workflow_runs);
      if (result.workflow_runs.length < 100) return runs;
    }
    throw new Error("too many active machine runs; refusing automatic refill");
  }))).flat();
  const listing = await api(`${endpoint}/runs?per_page=100`);
  if (!Array.isArray(listing.workflow_runs)) throw new Error("invalid recent workflow run listing");
  const plan = capacityPlan(target, active, listing.workflow_runs, now());
  const joined = status.groups.find((group) => group.tag === tag)?.online ?? 0;
  const dry = env.CAPACITY_DRY_RUN === "1";
  log(`${tag}: target ${target}, reserved ${plan.active}, joined ${joined}, ${dry ? "would start" : "starting"} ${plan.launch}, deferred ${plan.deferred}.`);
  for (let i = 0; i < plan.launch && !dry; i++) {
    // 0 requests a fresh slot. The CP atomically allocates its number, so
    // providers never race over a guessed free slot or displace a live host.
    await api(`${endpoint}/dispatches`, { method: "POST", body: { ref, inputs: { machine: "0" } } });
  }
  return { target, ...plan, joined, launched: dry ? 0 : plan.launch };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  reconcileCapacity(process.env).catch((error) => { console.error(error.message); process.exitCode = 1; });
}
