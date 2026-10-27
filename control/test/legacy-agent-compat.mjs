import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { loadAgentBundle } from "../src/agent-bundle.mjs";

const root = fileURLToPath(new URL("../..", import.meta.url));
const bundle = loadAgentBundle(root);
const directory = mkdtempSync(join(tmpdir(), "runner-legacy-agent-"));
try {
  for (const name of ["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs"]) {
    writeFileSync(join(directory, name), name === "agent.mjs" ? bundle.legacyAgent : bundle.files[name]);
  }
  assert.equal(existsSync(join(directory, "runtime.mjs")), false);
  const run = () => spawnSync(process.execPath, [join(directory, "agent.mjs")], {
    encoding: "utf8", timeout: 5000,
    // This validation happens immediately after module linking, before file,
    // Docker or network operations. Reaching it proves the four files link.
    env: { ...process.env, REGION: "invalid-region!", CONTROL_URL: "https://example.test", RUNNER_DATA: join(directory, "unused") },
  });
  const compatible = run();
  assert.equal(compatible.status, 1, compatible.error?.message);
  assert.match(compatible.stderr, /REGION must start with a letter/);
  assert.doesNotMatch(compatible.stderr, /ERR_MODULE_NOT_FOUND/);
  assert.equal(existsSync(join(directory, "unused")), false);

  writeFileSync(join(directory, "agent.mjs"), bundle.files["agent.mjs"]);
  const original = run();
  assert.equal(original.status, 1);
  assert.match(original.stderr, /ERR_MODULE_NOT_FOUND/);
  assert.match(original.stderr, /runtime\.mjs/);

  for (const [name, source] of Object.entries(bundle.files)) {
    assert.equal(source, readFileSync(join(root, "agent", name), "utf8"));
    assert.equal(createHash("sha256").update(source).digest("hex"), bundle.hashes[name]);
  }
  assert.ok(bundle.archive.includes(Buffer.from(bundle.files["agent.mjs"])), "verified image archive retains the raw agent entrypoint");
  assert.notEqual(bundle.legacyAgent, bundle.files["agent.mjs"]);
  console.log("legacy four-file agent links without runtime.mjs; verified bundles and image source stay unchanged");
} finally { rmSync(directory, { recursive: true, force: true }); }
