import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
import { githubClient } from "./capacity.mjs";

export async function verifyScheduledOrigin(env, api) {
  if (!/^[1-9]\d*$/.test(env.SCHEDULE_RUN || "")) throw new Error("a scheduled watchdog run is required");
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(env.GITHUB_REPOSITORY || "")) throw new Error("GITHUB_REPOSITORY is required");
  if (!api && !env.GH_TOKEN) throw new Error("GH_TOKEN is required");
  api ||= githubClient(env.GH_TOKEN, env.GITHUB_API_URL);
  const run = await api(`/repos/${env.GITHUB_REPOSITORY}/actions/runs/${env.SCHEDULE_RUN}`);
  const provider = run.event === "workflow_dispatch" && /^Machine schedule \(refill [1-9]\d{12}\)$/.test(run.display_title || "");
  if (!(run.event === "schedule" || provider) || run.path !== ".github/workflows/watchdog.yml" || run.head_branch !== env.GITHUB_REF_NAME)
    throw new Error("host requests must come from this branch's scheduled watchdog or replicated provider");
  return true;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  verifyScheduledOrigin(process.env).catch(error => { console.error(error.message); process.exitCode = 1; });
}
