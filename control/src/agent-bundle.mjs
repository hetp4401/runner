import { createHash } from "node:crypto";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

const digest = (value) => createHash("sha256").update(value).digest("hex");

function legacyAgentSource(agent, runtime) {
  // The original installer fetches only four files on every boot. Keep that
  // entrypoint self-contained for its new runtime dependency, without changing
  // the raw files/digests used by verified bundles and immutable images.
  for (const match of runtime.matchAll(/\b(?:from\s+|import\s*\(?\s*)(["'])([^"']+)\1/g)) {
    if (!match[2].startsWith("node:")) throw new Error("legacy agent runtime must import only Node built-ins");
  }
  const imports = [...agent.matchAll(/\bfrom\s+(["'])\.\/runtime\.mjs\1/g)];
  if (imports.length !== 1) throw new Error("legacy agent runtime import could not be identified");
  const url = `data:text/javascript;base64,${Buffer.from(runtime).toString("base64")}`;
  return agent.replace(imports[0][0], `from ${JSON.stringify(url)}`);
}

// A deterministic archive needs no tar executable in the control container.
function archive(files) {
  const parts = [];
  for (const [name, text] of Object.entries(files).sort(([a], [b]) => a.localeCompare(b))) {
    if (Buffer.byteLength(name) > 100 || name.includes("..")) throw new Error("invalid agent archive path");
    const bytes = Buffer.from(text), header = Buffer.alloc(512);
    const field = (at, size, value) => header.write(value, at, size, "ascii");
    const octal = (at, size, value) => field(at, size, value.toString(8).padStart(size - 1, "0") + "\0");
    field(0, 100, name); octal(100, 8, 0o644); octal(108, 8, 0); octal(116, 8, 0);
    octal(124, 12, bytes.length); octal(136, 12, 0); field(148, 8, "        ");
    field(156, 1, "0"); field(257, 6, "ustar\0"); field(263, 2, "00");
    field(148, 8, [...header].reduce((sum, byte) => sum + byte, 0).toString(8).padStart(6, "0") + "\0 ");
    parts.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) % 512));
  }
  return Buffer.concat([...parts, Buffer.alloc(1024)]);
}

export function loadAgentBundle(root) {
  const files = Object.fromEntries(readdirSync(join(root, "agent")).filter((name) => /^[a-z][a-z0-9-]*\.mjs$/.test(name)).sort()
    .map((name) => [name, readFileSync(join(root, "agent", name), "utf8")]));
  for (const required of ["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs", "runtime.mjs"]) {
    if (!files[required]) throw new Error(`agent bundle is missing ${required}`);
  }
  const hashes = Object.fromEntries(Object.entries(files).map(([name, content]) => [name, digest(content)]));
  const revision = digest(JSON.stringify(Object.entries(hashes).sort(([a], [b]) => a.localeCompare(b))));
  const tar = archive({ Dockerfile: readFileSync(join(root, "control/agent.Dockerfile"), "utf8"),
    "control/agent-launcher.mjs": readFileSync(join(root, "control/agent-launcher.mjs"), "utf8"),
    ...Object.fromEntries(Object.entries(files).map(([name, content]) => [`agent/${name}`, content])) });
  return { files, hashes, revision, archive: tar, archiveHash: digest(tar), legacyAgent: legacyAgentSource(files["agent.mjs"], files["runtime.mjs"]) };
}
