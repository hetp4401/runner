import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, realpath, writeFile, symlink, link, rm, readdir, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { namespace, projectName, fingerprint, resumeState, ownsCompose, ownsInfrastructure, containerHealth,
  stageDeployment, writeDataFiles, bindLayout, validateBindSources, cacheAgentBundle, downloadAgentBundle, OWNER_LABEL, COPY_LABEL } from "../../agent/runtime.mjs";

const root = await realpath(await mkdtemp(join(tmpdir(), "runner-agent-runtime-")));
let checks = 0;
const check = (name) => { checks++; console.log(`ok ${name}`); };
try {
  const control = "https://control.example.test";
  const saved = { control, bootId: "boot-1", started: 100, run: "old-run", agent: "agent-a", machine: 7,
    projects: [["web", { app: "web", replica: 1 }]], recovery: { payload: "receipt", signature: "signed" } };
  const crash = resumeState(saved, "boot-1", control, 200);
  assert.equal(crash.run, "old-run"); assert.equal(crash.started, 100);
  const reboot = resumeState(saved, "boot-2", control, 200);
  assert.notEqual(reboot.run, "old-run"); assert.equal(reboot.previousRun, "old-run");
  assert.deepEqual(reboot.saved.projects, saved.projects); assert.equal(reboot.saved.recovery, saved.recovery);
  const upgrade = resumeState({ ...saved, restartRequested: true }, "boot-1", control, 200);
  assert.notEqual(upgrade.run, "old-run"); assert.equal(upgrade.previousRun, "old-run");
  assert.notEqual(resumeState(saved, "boot-1", control, 200, "install-2").run, "old-run");
  assert.equal(resumeState({ ...saved, installedRevision: "install-2" }, "boot-1", control, 200, "install-2").run, "old-run");
  const twiceOffline = resumeState({ ...saved, run: reboot.run, started: reboot.started, previousRun: reboot.previousRun, bootId: "boot-2" }, "boot-3", control, 300);
  assert.notEqual(twiceOffline.run, reboot.run);
  assert.equal(twiceOffline.previousRun, saved.run);
  assert.throws(() => resumeState(saved, "boot-1", "https://other.test"), /another control plane/);
  check("crash resume, host reboot and deliberate upgrade retain inventory with appropriate run identities");

  const owner = namespace(control), key = "runner";
  assert.notEqual(projectName(owner, key), "runner");
  assert.notEqual(namespace(control), namespace("https://different.test"));
  const legacyDir = join(root, "projects/runner");
  const legacy = { Config: { Labels: { "com.docker.compose.project.working_dir": legacyDir,
    "com.docker.compose.project.config_files": `${legacyDir}/compose.yaml` } } };
  assert.equal(ownsCompose(legacy, { owner, key, legacyDir }), true);
  assert.equal(ownsCompose({ Config: { Labels: { "com.docker.compose.project.working_dir": root } } }, { owner, key, legacyDir }), false);
  assert.equal(ownsCompose({ Config: { Labels: { [OWNER_LABEL]: owner, [COPY_LABEL]: "another" } } }, { owner, key, legacyDir }), false);
  assert.equal(ownsInfrastructure({ Mounts: [{ Type: "bind", Source: "/someone-else/router", Destination: "/etc/router" }] },
    { owner, kind: "router", legacySource: join(root, "router"), destination: "/etc/router" }), false);
  assert.equal(ownsInfrastructure({ Config: { Labels: { [OWNER_LABEL]: owner, [COPY_LABEL]: "app", "io.runners.component": "router" } } },
    { owner, kind: "router", legacySource: join(root, "router"), destination: "/etc/router" }), false);
  check("fleet names and ownership checks isolate CP, other Compose apps and unrelated infrastructure");

  const background = { Config: { Labels: { "com.docker.compose.service": "worker" } }, State: { Running: true, Status: "running" } };
  assert.equal(containerHealth([background], ["worker"]), null);
  assert.match(containerHealth([{ ...background, State: { Running: false, Status: "exited", ExitCode: 1 } }]), /stopped/);
  assert.match(containerHealth([{ ...background, State: { Running: true, Health: { Status: "unhealthy" } } }]), /unhealthy/);
  assert.match(containerHealth([background], ["worker", "sidecar"]), /missing/);
  const once = { Config: { Labels: { "com.docker.compose.service": "migrate", "io.runners.oneshot": "true" } }, State: { Running: false, Status: "exited", ExitCode: 0 } };
  assert.equal(containerHealth([background, once]), null);
  check("background service exits, Docker health, missing sidecars and one-shot dependencies are distinguished");

  const first = { v: 1, app: "web", replica: 1, compose: "services: {}", files: { "app.js": "first", ".env": "TOKEN=old" } };
  assert.notEqual(fingerprint(first), fingerprint({ ...first, files: { ...first.files, "app.js": "new" } }));
  assert.notEqual(fingerprint(first), fingerprint({ ...first, files: { ...first.files, ".env": "TOKEN=new" } }));
  assert.equal(fingerprint(first), fingerprint({ ...first, v: 2 }));
  assert.equal(fingerprint(first), fingerprint({ ...first, files: { ".env": "TOKEN=old", "app.js": "first" } }));
  check("same-version replacement and secret updates change desired identity");

  const outside = join(root, "outside"); await mkdir(outside); await writeFile(join(outside, "victim"), "unchanged");
  const managed = join(root, "managed"); await mkdir(managed); await symlink(outside, join(managed, "evil"));
  await assert.rejects(stageDeployment(managed, "evil", { victim: "overwritten" }, "{}"), /symlink/);
  const oldMask = process.umask(0o077);
  let source;
  try { source = await stageDeployment(managed, "web", { "nested/file.txt": "new", ".env": "TOKEN=fake" }, "{}"); }
  finally { process.umask(oldMask); }
  assert.equal(await readFile(join(source, "nested/file.txt"), "utf8"), "new");
  assert.equal((await stat(join(source, "nested/file.txt"))).mode & 0o777, 0o644);
  assert.equal((await stat(join(source, "nested"))).mode & 0o777, 0o755);
  assert.equal((await stat(source)).mode & 0o777, 0o700);
  assert.equal((await stat(join(source, ".env"))).mode & 0o777, 0o600);
  assert.equal(await readFile(join(outside, "victim"), "utf8"), "unchanged");
  check("private release staging refuses symlink roots and writes only fresh managed source");

  if (process.platform === "linux") {
    const data = join(root, "data"); await mkdir(data);
    await symlink(outside, join(data, "link"));
    await assert.rejects(writeDataFiles(data, { "link/victim": "escape" }));
    await link(join(outside, "victim"), join(data, "hardlink"));
    await symlink(join(outside, "victim"), join(data, "symlink"));
    await writeDataFiles(data, { hardlink: "safe replacement", symlink: "also safe", "nested/file.txt": "allowed" });
    assert.equal(await readFile(join(outside, "victim"), "utf8"), "unchanged");
    assert.equal(await readFile(join(data, "hardlink"), "utf8"), "safe replacement");
    assert.equal(await readFile(join(data, "nested/file.txt"), "utf8"), "allowed");
    await assert.rejects(validateBindSources(data, [{ path: "link", readOnly: false }]));
    await validateBindSources(data, [{ path: "nested", readOnly: false }, { path: "missing/directory", readOnly: false }]);
    assert.notEqual(bindLayout([{ path: ".", readOnly: false }]), bindLayout([{ path: "nested", readOnly: false }]));
    check("Linux FD-anchored data updates cannot overwrite symlink or hardlink targets outside the app");
  } else console.log("skip Linux-only FD-anchored data test (run in the agent image)");

  const files = Object.fromEntries(["agent.mjs", "metrics.mjs", "ws.mjs", "control-link.mjs", "runtime.mjs"].map((name) => [name, `// ${name}\n`]));
  const hashes = Object.fromEntries(Object.entries(files).map(([name, value]) => [name, createHash("sha256").update(value).digest("hex")]));
  const revision = createHash("sha256").update(JSON.stringify(Object.entries(hashes).sort(([a], [b]) => a.localeCompare(b)))).digest("hex");
  const bundle = { files, hashes, revision };
  await cacheAgentBundle(root, bundle);
  const active = await readFile(join(root, "agent-code/current.json"), "utf8");
  await assert.rejects(cacheAgentBundle(root, { ...bundle, files: { ...files, "agent.mjs": "tampered" } }), /checksum/);
  await assert.rejects(downloadAgentBundle(control, async () => { throw new Error("offline"); }), /offline/);
  assert.equal(await readFile(join(root, "agent-code/current.json"), "utf8"), active);
  assert.equal(await readFile(join(root, "agent-code/releases", revision, "agent.mjs"), "utf8"), files["agent.mjs"]);
  assert.deepEqual((await readdir(join(root, "agent-code/releases"))).sort(), [revision]);
  check("verified agent bundles install atomically and failed or offline refresh preserves last-known-good code");
  console.log(`${checks} agent runtime checks passed`);
} finally { await rm(root, { recursive: true, force: true }); }
