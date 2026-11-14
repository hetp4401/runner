// Each repository's serialized workflow owns launch accounting. It can be
// woken by its schedule or by the optional replicated provider application.
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

export const ACTIVE_STATUSES = ["requested", "pending", "waiting", "queued", "in_progress"];
const LAUNCH_SPACING_MS = 10 * 60_000;
const RENEW_AFTER_MS = 210 * 60_000;

export function capacityInterval(env) {
  const value = String(env.CAPACITY_INTERVAL_MIN || '10');
  if (!['1', '2', '5', '10'].includes(value)) throw new Error('CAPACITY_INTERVAL_MIN must be 1, 2, 5 or 10');
  return Number(value) * 60_000;
}

export function refillSlot(now, phase, interval = LAUNCH_SPACING_MS) {
  if (!/^[0-9]$/.test(String(phase))) throw new Error('CAPACITY_PHASE_MIN must be from 0 to 9');
  // Keep the existing ten-minute phases; scale the same offsets for faster supply.
  const offset = Number(phase) * interval / 10;
  return Math.floor((now - offset) / interval) * interval + offset;
}

export function targetCount(env) {
  const raw = env.MACHINE_COUNT || env.POOL_SIZE || "0";
  if (!/^(0|[1-9]\d*)$/.test(raw) || Number(raw) > 50) throw new Error("MACHINE_COUNT must be a whole number from 0 to 50");
  return Number(raw);
}

export function capacityPlan(target, active, recent, now, { spacingMs = LAUNCH_SPACING_MS } = {}) {
  const runs = new Map();
  for (const run of [...active, ...recent]) {
    if (!Number.isSafeInteger(run.id) || run.id <= 0 || typeof run.status !== "string") throw new Error("invalid workflow run inventory");
    if (run.status !== "completed" && !ACTIVE_STATUSES.includes(run.status)) throw new Error("unknown workflow run status");
    if (!Number.isFinite(Date.parse(run.created_at))) throw new Error("invalid workflow run creation time");
    if (run.run_started_at && !Number.isFinite(Date.parse(run.run_started_at))) throw new Error("invalid workflow run start time");
    runs.set(run.id, run); // the later recent listing resolves status transitions
  }
  const slots = new Map();
  let jobs = 0, latestLaunch = -Infinity;
  for (const run of runs.values()) {
    latestLaunch = Math.max(latestLaunch, Date.parse(run.created_at));
    if (!ACTIVE_STATUSES.includes(run.status)) continue;
    jobs++;
    const title = run.display_title || run.name || "";
    const slot = /^machine ([1-9]\d*)$/.exec(title.trim());
    // Preserve accounting for old, overlapping same-slot handovers while the
    // fleet drains into schedule-only hosts. New jobs each supply one host.
    const key = slot ? `slot:${slot[1]}` : `run:${run.id}`;
    const started = Date.parse(run.run_started_at || run.created_at);
    const aging = run.status === "in_progress" && now - started >= RENEW_AFTER_MS;
    slots.set(key, (slots.get(key) ?? true) && aging);
  }
  const aging = [...slots.values()].filter(Boolean).length;
  const missing = Math.max(0, target - slots.size);
  // One overlap host can warm before an aging host leaves. Never cancel a
  // healthy job to enforce a count, or burst-launch an entire missing cohort.
  const wanted = missing || (target > 0 && aging > 0 && slots.size === target ? 1 : 0);
  const retryAfterMs = Math.max(0, latestLaunch + spacingMs - now);
  const launch = wanted > 0 && jobs < target + 1 && retryAfterMs === 0 ? 1 : 0;
  return { active: slots.size, jobs, aging, missing, launch, deferred: Math.max(0, wanted - launch), retryAfterMs };
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

export async function reconcileCapacity(env, { api, now = Date.now, log = console.log } = {}) {
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
  const interval = capacityInterval(env);
  const scheduled = env.GITHUB_EVENT_NAME === "schedule" && env.CAPACITY_TRIGGER !== "provider";
  const provider = env.CAPACITY_TRIGGER === "provider" && env.GITHUB_EVENT_NAME === "workflow_dispatch" &&
    env.GITHUB_WORKFLOW === "Machine schedule" && Boolean(env.CAPACITY_SLOT) &&
    env.GITHUB_WORKFLOW_REF === `${repo}/.github/workflows/watchdog.yml@refs/heads/${ref}`;
  const dry = env.CAPACITY_DRY_RUN === "1" || !(scheduled || provider);
  if (!dry && !/^[1-9]\d*$/.test(env.GITHUB_RUN_ID || "")) throw new Error("scheduled GITHUB_RUN_ID is required");
  if (!dry && env.GITHUB_RUN_ATTEMPT && env.GITHUB_RUN_ATTEMPT !== "1") {
    log("A capacity rerun cannot repeat a host request.");
    return { target, launched: 0, duplicate: true };
  }
  if (provider && !dry) {
    const phase = env.CAPACITY_PHASE_MIN || "2", slot = Number(env.CAPACITY_SLOT);
    if (!/^[0-9]$/.test(phase) || !/^[1-9]\d{12}$/.test(env.CAPACITY_SLOT) ||
        slot !== refillSlot(slot, phase, interval) || slot > now() || now() - slot >= LAUNCH_SPACING_MS)
      throw new Error("provider refill slot is stale, in the future, or has the wrong phase");
  }
  api ||= githubClient(env.GH_TOKEN, env.GITHUB_API_URL);
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
  // Raft admits only one provider request per fixed UTC slot. A half-interval
  // floor also prevents adjacent late requests from bunching. Measuring a full
  // interval from GitHub's later job creation time caused cumulative phase drift.
  const spacingMs = env.CAPACITY_TRIGGER === 'provider' ? interval / 2 : LAUNCH_SPACING_MS;
  const plan = capacityPlan(target, active, listing.workflow_runs, now(), { spacingMs });
  log(`${tag}: target ${target}, reserved ${plan.active}, jobs ${plan.jobs}, aging ${plan.aging}, ${dry ? "would start" : "starting"} ${plan.launch}, deferred ${plan.deferred}, spacing wait ${Math.ceil(plan.retryAfterMs / 1000)}s.`);
  for (let i = 0; i < plan.launch && !dry; i++) {
    await api(`${endpoint}/dispatches`, { method: "POST", body: { ref, inputs: { schedule_run: env.GITHUB_RUN_ID } } });
  }
  return { target, ...plan, launched: dry ? 0 : plan.launch };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  reconcileCapacity(process.env).catch((error) => { console.error(error.message); process.exitCode = 1; });
}
