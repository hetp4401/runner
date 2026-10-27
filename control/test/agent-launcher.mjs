import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { agentEntrypoint } from "../agent-launcher.mjs";

const directory = mkdtempSync(join(tmpdir(), "runner-launcher-"));
const env = { RUNNER_DATA: directory };
const hash = (value) => createHash("sha256").update(value).digest("hex");
const code = Object.fromEntries(["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs", "runtime.mjs"].map((name) => [name, `// ${name}\n`]));
const hashes = Object.fromEntries(Object.entries(code).map(([name, text]) => [name, hash(text)]));
const revision = hash(JSON.stringify(Object.entries(hashes).sort(([a], [b]) => a.localeCompare(b))));
const cache = join(directory, "agent-code"), release = join(cache, "releases", revision), current = join(cache, "current.json");
try {
  assert.equal(agentEntrypoint(env, "/bundled-agent"), "/bundled-agent/agent.mjs");
  mkdirSync(release, { recursive: true });
  for (const [name, content] of Object.entries(code)) writeFileSync(join(release, name), content);
  writeFileSync(join(release, "manifest.json"), JSON.stringify({ revision, hashes }));
  writeFileSync(current, JSON.stringify({ revision }));
  assert.equal(agentEntrypoint(env), join(release, "agent.mjs"));
  writeFileSync(join(release, "metrics.mjs"), "tampered");
  assert.throws(() => agentEntrypoint(env), /failed verification/);
  writeFileSync(join(release, "metrics.mjs"), code["metrics.mjs"]);
  writeFileSync(join(release, "extra.mjs"), "unexpected");
  assert.throws(() => agentEntrypoint(env), /unexpected files/);
  rmSync(join(release, "extra.mjs"));
  writeFileSync(current, JSON.stringify({ revision: "../../another-agent" }));
  assert.throws(() => agentEntrypoint(env), /invalid cached agent revision/);
  writeFileSync(current, JSON.stringify({ revision }));
  rmSync(join(release, "agent.mjs"));
  symlinkSync(join(release, "metrics.mjs"), join(release, "agent.mjs"));
  assert.throws(() => agentEntrypoint(env), /failed verification/);
  console.log("agent launcher: bundled startup, verified offline release, tamper/file/path/symlink rejection passed");
} finally { rmSync(directory, { recursive: true, force: true }); }
