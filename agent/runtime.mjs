// Host-local runtime helpers. No network calls or dependencies.
import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { chmod, lstat, mkdir, mkdtemp, open, realpath, rm } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve, sep } from "node:path";

export const OWNER_LABEL = "io.runners.owner";
export const COPY_LABEL = "io.runners.copy";
export function resolveServerTag(env, saved = null) {
  const value = env.SERVER_TAG ?? env.POOL ?? saved?.tag ?? saved?.pool ??
    (saved?.run && saved?.control === env.CONTROL_URL ? "standalone" : null);
  if (typeof value !== "string" || !/^[a-z0-9][a-z0-9-]{0,29}$/.test(value))
    throw new Error("SERVER_TAG is required: use 1-30 lowercase letters, digits or dashes, or standalone");
  return value;
}
export const namespace = (control) => `runner-${createHash("sha256").update(new URL(control).origin).digest("hex").slice(0, 12)}`;
export function projectName(owner, key) {
  if (!/^[a-z0-9][a-z0-9-]{0,70}$/.test(key)) throw new Error("invalid application copy name");
  return `${owner}-${key}`;
}
export function fingerprint(want) {
  return createHash("sha256").update(JSON.stringify({ app: want.app, replica: want.replica ?? 1,
    port: want.port ?? null, ready: want.ready ?? null, incarnation: want.incarnation ?? null,
    compose: want.compose, files: Object.entries(want.files ?? {}).sort(([a], [b]) => a.localeCompare(b)) })).digest("hex");
}
export function resumeState(saved, bootId, control, now = Date.now(), installedRevision = undefined) {
  if (!saved) return { saved: null, started: now, run: "", previousRun: null };
  if (saved.control !== control) throw new Error("RUNNER_DATA belongs to another control plane; use a separate directory for another fleet");
  const fresh = saved.bootId !== bootId || saved.restartRequested || installedRevision && saved.installedRevision !== installedRevision;
  const started = fresh ? Math.max(now, Number(saved.started) + 1) : saved.started || now;
  return { saved, started, run: fresh ? `${saved.agent}-${started}-${randomUUID().slice(0, 8)}` : saved.run || "",
    previousRun: fresh ? saved.previousRun || saved.run || null : saved.previousRun || null };
}
export function ownsCompose(container, { owner, key, legacyDir }) {
  const labels = container?.Config?.Labels ?? {};
  if (labels[OWNER_LABEL] === owner && labels[COPY_LABEL] === key) return true;
  const files = String(labels["com.docker.compose.project.config_files"] ?? "").split(",");
  return !labels[OWNER_LABEL] && resolve(labels["com.docker.compose.project.working_dir"] || "/") === resolve(legacyDir) &&
    files.length === 1 && resolve(files[0]) === resolve(legacyDir, "compose.yaml");
}
export function ownsInfrastructure(container, { owner, kind, legacySource, destination }) {
  if (container?.Config?.Labels?.[COPY_LABEL]) return false;
  if (container?.Config?.Labels?.[OWNER_LABEL] === owner && container.Config.Labels["io.runners.component"] === kind) return true;
  return !container?.Config?.Labels?.[OWNER_LABEL] && (container?.Mounts ?? []).some((m) =>
    m.Type === "bind" && resolve(m.Source || "/") === resolve(legacySource) && m.Destination === destination);
}
export function containerHealth(containers, expectedServices = [], oneshotServices = []) {
  const normal = containers.filter((c) => c?.Config?.Labels?.["com.docker.compose.oneoff"] !== "True");
  if (!normal.length) return "no containers are running";
  const present = new Set(normal.map((c) => c.Config?.Labels?.["com.docker.compose.service"]));
  if (expectedServices.some((name) => !present.has(name))) return "an application service is missing";
  for (const c of normal) {
    const state = c.State ?? {};
    // Successful one-shot dependencies may have completed by the time the main service becomes ready.
    if (state.Status === "exited" && state.ExitCode === 0 && (c.Config?.Labels?.["io.runners.oneshot"] === "true" || oneshotServices.includes(c.Config?.Labels?.["com.docker.compose.service"]))) continue;
    if (!state.Running || state.Restarting || state.Paused || state.Dead) return "an application container has stopped";
    if (state.Health && !["healthy", "starting"].includes(state.Health.Status)) return "an application container is unhealthy";
  }
  return null;
}
// Walk existing parents without accepting symlinks. Managed source is created under
// a private root outside all app-writable bind mounts, so no app can race this walk.
export async function privateDirectory(path) {
  const absolute = resolve(path);
  let current = absolute.startsWith(sep) ? sep : "";
  for (const part of absolute.split(sep).filter(Boolean)) {
    current = join(current, part);
    try { await mkdir(current, { mode: 0o700 }); } catch (e) { if (e.code !== "EEXIST") throw e; }
    const stat = await lstat(current);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("managed directory contains a symlink or non-directory");
  }
  if (await realpath(absolute) !== absolute) throw new Error("managed directory must not resolve through symlinks");
  return absolute;
}
export async function stageDeployment(root, key, files, compose) {
  projectName("validate", key);
  const parent = await privateDirectory(join(root, key));
  const directory = await mkdtemp(join(parent, "release-"));
  try {
    const entries = { ...files, "compose.yaml": compose };
    for (const [name, value] of Object.entries(entries)) {
      if (typeof value !== "string" || isAbsolute(name) || name.includes("\\") || name.split("/").some((p) => !p || p === "." || p === "..")) throw new Error("invalid deployment file path");
      const path = resolve(directory, name);
      if (!path.startsWith(`${directory}${sep}`)) throw new Error("deployment file points outside its release");
      // Only the private release boundary is 0700. Docker COPY preserves file
      // and nested-directory modes, so application source must remain readable
      // by a non-root image user. No app can reach this fresh tree while staging.
      await mkdir(dirname(path), { recursive: true, mode: 0o755 });
      for (let folder = dirname(path); folder !== directory; folder = dirname(folder)) await chmod(folder, 0o755);
      const mode = [".env", "compose.yaml"].includes(name) ? 0o600 : 0o644;
      const file = await open(path, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        mode);
      try { await file.chmod(mode); await file.writeFile(value); } finally { await file.close(); }
    }
    return directory;
  } catch (error) { await rm(directory, { recursive: true, force: true }); throw error; }
}

// Application containers can modify this tree. Every child lookup is relative to
// a held directory FD, never a previously checked pathname. Replacing the final
// file atomically also avoids following an existing symlink or hard link.
export async function writeDataFiles(root, files) {
  if (!Object.keys(files ?? {}).length) return;
  await mkdir(root, { recursive: true });
  const top = await open(root, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
    for (const [name, content] of Object.entries(files)) {
      const parts = name.split("/");
      if (isAbsolute(name) || name.includes("\\") || parts.some((p) => !p || p === "." || p === "..") || typeof content !== "string") throw new Error("invalid app data file path");
      let directory = top;
      const opened = [];
      let temporary;
      try {
        for (const part of parts.slice(0, -1)) {
          const child = `/proc/self/fd/${directory.fd}/${part}`;
          try { await mkdir(child, { mode: 0o755 }); } catch (e) { if (e.code !== "EEXIST") throw e; }
          directory = await open(child, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
          opened.push(directory);
        }
        temporary = `/proc/self/fd/${directory.fd}/.runner-write-${randomUUID()}`;
        const file = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o644);
        try { await file.writeFile(content); } finally { await file.close(); }
        const { rename } = await import("node:fs/promises");
        await rename(temporary, `/proc/self/fd/${directory.fd}/${parts.at(-1)}`);
        temporary = null;
      } finally {
        if (temporary) await rm(temporary, { force: true }).catch(() => {});
        for (const fd of opened.reverse()) await fd.close();
      }
    }
  } finally { await top.close(); }
}

const REQUIRED_AGENT_FILES = ["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs", "runtime.mjs"];
export function validateBundle(bundle) {
  if (!bundle || typeof bundle.files !== "object" || typeof bundle.hashes !== "object" || !/^[0-9a-f]{64}$/.test(bundle.revision ?? "")) throw new Error("invalid agent bundle manifest");
  const entries = Object.entries(bundle.files);
  if (entries.length > 30 || REQUIRED_AGENT_FILES.some((name) => typeof bundle.files[name] !== "string") || Object.keys(bundle.hashes).length !== entries.length) throw new Error("agent bundle is incomplete");
  let bytes = 0;
  for (const [name, content] of entries) {
    if (!/^[a-z][a-z0-9-]*\.mjs$/.test(name) || typeof content !== "string") throw new Error("invalid agent bundle filename");
    bytes += Buffer.byteLength(content);
    if (bytes > 1_000_000 || createHash("sha256").update(content).digest("hex") !== bundle.hashes[name]) throw new Error("agent bundle checksum failed");
  }
  const revision = createHash("sha256").update(JSON.stringify(Object.entries(bundle.hashes).sort(([a], [b]) => a.localeCompare(b)))).digest("hex");
  if (revision !== bundle.revision) throw new Error("agent bundle revision failed");
  return bundle;
}
export async function cacheAgentBundle(base, bundle) {
  validateBundle(bundle);
  const { readFile, rename } = await import("node:fs/promises");
  const root = await privateDirectory(join(base, "agent-code"));
  const releases = await privateDirectory(join(root, "releases"));
  const target = join(releases, bundle.revision);
  let exists = false;
  try {
    const manifest = JSON.parse(await readFile(join(target, "manifest.json"), "utf8"));
    const files = {};
    for (const name of Object.keys(manifest.hashes ?? {})) {
      if (!/^[a-z][a-z0-9-]*\.mjs$/.test(name)) throw new Error("invalid cached filename");
      const file = await open(join(target, name), constants.O_RDONLY | constants.O_NOFOLLOW);
      try { files[name] = await file.readFile("utf8"); } finally { await file.close(); }
    }
    validateBundle({ ...manifest, files });
    if (manifest.revision !== bundle.revision) throw new Error("cached revision differs");
    exists = true;
  } catch (e) { if (e.code !== "ENOENT") throw e; }
  if (!exists) {
    const stage = await mkdtemp(join(releases, ".download-"));
    try {
      for (const [name, value] of Object.entries({ ...bundle.files, "manifest.json": JSON.stringify({ revision: bundle.revision, hashes: bundle.hashes }) })) {
        const file = await open(join(stage, name), constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
        try { await file.writeFile(value); await file.sync(); } finally { await file.close(); }
      }
      const staged = await open(stage, constants.O_RDONLY | constants.O_DIRECTORY);
      try { await staged.sync(); } finally { await staged.close(); }
      await rename(stage, target);
      const retained = await open(releases, constants.O_RDONLY | constants.O_DIRECTORY);
      try { await retained.sync(); } finally { await retained.close(); }
    } finally { await rm(stage, { recursive: true, force: true }); }
  }
  const temporary = join(root, `.current-${randomUUID()}.json`);
  const file = await open(temporary, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { await file.writeFile(JSON.stringify({ revision: bundle.revision })); await file.sync(); } finally { await file.close(); }
  await rename(temporary, join(root, "current.json"));
  const directory = await open(root, constants.O_RDONLY | constants.O_DIRECTORY);
  try { await directory.sync(); } finally { await directory.close(); }
  return bundle.revision;
}
export async function downloadAgentBundle(url, fetcher = fetch) {
  const response = await fetcher(`${url.replace(/\/$/, "")}/agent/bundle.json`, { signal: AbortSignal.timeout(30_000) });
  if (!response.ok) throw new Error(`agent download failed: HTTP ${response.status}`);
  const chunks = []; let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    if (size > 1_100_000) throw new Error("agent bundle is too large");
    chunks.push(chunk);
  }
  return validateBundle(JSON.parse(Buffer.concat(chunks).toString("utf8")));
}

export function bindLayout(sources = []) {
  return JSON.stringify(sources.map(({ path, readOnly }) => ({ path, readOnly: Boolean(readOnly) }))
    .sort((a, b) => a.path.localeCompare(b.path) || Number(a.readOnly) - Number(b.readOnly)));
}
// The caller stops owned containers first when the bind layout changes. With an
// unchanged, disjoint layout a container cannot rename the root of its own mount.
export async function validateBindSources(root, sources = []) {
  if (!sources.length) return;
  const top = await open(root, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
  try {
    for (const source of sources) {
      const name = source.path;
      if (name === ".") continue;
      if (typeof name !== "string" || isAbsolute(name) || name.includes("\\")) throw new Error("invalid bind source path");
      const parts = name.replace(/^\.\//, "").split("/");
      if (parts.some((part) => !part || part === "." || part === "..")) throw new Error("invalid bind source path");
      let directory = top;
      const opened = [];
      try {
        for (let i = 0; i < parts.length; i++) {
          const path = `/proc/self/fd/${directory.fd}/${parts[i]}`;
          let child;
          try { child = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | (i < parts.length - 1 ? constants.O_DIRECTORY : 0)); }
          catch (error) {
            if (error.code !== "ENOENT") throw error;
            try { await mkdir(path, { mode: 0o755 }); } catch (e) { if (e.code !== "EEXIST") throw e; }
            child = await open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
          }
          opened.push(child);
          const stat = await child.stat();
          if (!(stat.isDirectory() || stat.isFile()) || stat.isFile() && stat.nlink > 1) throw new Error("bind source must be a directory or a file without hard links");
          directory = child;
        }
      } finally { for (const file of opened.reverse()) await file.close(); }
    }
  } finally { await top.close(); }
}
