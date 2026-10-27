// A reboot needs no package repository or control-plane download. A roll may
// select an already verified local release; otherwise use the image's baseline.
import { createHash } from "node:crypto";
import { existsSync, lstatSync, readFileSync, readdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const required = ["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs", "runtime.mjs"];
const digest = (data) => createHash("sha256").update(data).digest("hex");
const hex = /^[a-f0-9]{64}$/;

export function agentEntrypoint(env = process.env, baseline = "/agent") {
  const directory = join(env.RUNNER_DATA || "/var/lib/runner", "agent-code");
  const current = join(directory, "current.json");
  if (!existsSync(current)) return join(baseline, "agent.mjs");
  if (!lstatSync(current).isFile() || lstatSync(current).size > 1024) throw new Error("invalid cached agent selection");
  const { revision } = JSON.parse(readFileSync(current, "utf8"));
  if (typeof revision !== "string" || !hex.test(revision)) throw new Error("invalid cached agent revision");
  const release = join(directory, "releases", revision);
  if (!lstatSync(release).isDirectory()) throw new Error("invalid cached agent release directory");
  const manifestFile = join(release, "manifest.json");
  if (!lstatSync(manifestFile).isFile() || lstatSync(manifestFile).size > 16_384) throw new Error("invalid cached agent manifest");
  const manifest = JSON.parse(readFileSync(manifestFile, "utf8"));
  const hashes = manifest.hashes;
  if (manifest.revision !== revision || !hashes || typeof hashes !== "object" || Array.isArray(hashes)) throw new Error("invalid cached agent manifest");
  const files = Object.keys(hashes).sort();
  if (files.length > 32 || required.some((name) => !files.includes(name)) || files.some((name) => !/^[a-z][a-z0-9-]*\.mjs$/.test(name) || !hex.test(hashes[name]))) throw new Error("invalid cached agent file list");
  if (digest(JSON.stringify(Object.entries(hashes).sort(([a], [b]) => a.localeCompare(b)))) !== revision) throw new Error("cached agent revision does not match its manifest");
  if (JSON.stringify(readdirSync(release).filter((name) => name !== "manifest.json").sort()) !== JSON.stringify(files)) throw new Error("cached agent release contains unexpected files");
  let bytes = 0;
  for (const name of files) {
    const file = join(release, name), stat = lstatSync(file);
    bytes += stat.size;
    if (!stat.isFile() || bytes > 1_000_000 || digest(readFileSync(file)) !== hashes[name]) throw new Error(`cached agent file failed verification: ${name}`);
  }
  return join(release, "agent.mjs");
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { await import(pathToFileURL(agentEntrypoint()).href); }
  catch (error) { console.error(`agent startup failed: ${error.message}`); process.exitCode = 1; }
}
