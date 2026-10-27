// Actual agent processes against a fake Docker CLI and local control plane.
// Only the Linux boot-id path and the health-check timer are redirected; all
// lifecycle, ownership, deployment and state-file code is the shipped agent.
import assert from "node:assert/strict";
import { mkdtemp, mkdir, writeFile, readFile, copyFile, readdir, realpath, rm, symlink, rename } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";
import http from "node:http";
import { namespace } from "../../agent/runtime.mjs";

const root = await realpath(await mkdtemp(join(tmpdir(), "runner-agent-lifecycle-")));
const repo = new URL("../../", import.meta.url);
const bin = join(root, "bin"), source = join(root, "agent"), data = join(root, "data");
await mkdir(bin); await mkdir(source); await mkdir(data); await mkdir(join(data, "router"));
await mkdir(join(data, "projects"));
await symlink(process.execPath, join(bin, "node"));
for (const name of await readdir(new URL("agent/", repo))) if (name.endsWith(".mjs")) await copyFile(new URL(`agent/${name}`, repo), join(source, name));
const original = await readFile(join(source, "agent.mjs"), "utf8");
await writeFile(join(source, "agent.mjs"), original.replace('"/proc/sys/kernel/random/boot_id"', JSON.stringify(join(root, "boot-id"))).replace("}, 10_000).unref();", "}, 100).unref();"));
await writeFile(join(root, "boot-id"), "boot-current");
const dockerState = join(root, "docker.json");
const atomic = async (file, value) => { const temp = `${file}.test-${Math.random()}`; await writeFile(temp, JSON.stringify(value)); await rename(temp, file); };
await writeFile(join(bin, "docker"), `#!/usr/bin/env node
const fs=require('node:fs');
const args=process.argv.slice(2), path=process.env.FAKE_DOCKER_STATE;
const db=JSON.parse(fs.readFileSync(path,'utf8'));
const write=()=>{const tmp=path+'.'+process.pid;fs.writeFileSync(tmp,JSON.stringify(db));fs.renameSync(tmp,path)};
const value=(flag)=>args[args.indexOf(flag)+1];
const match=(name)=>Object.values(db).find(c=>c.Id===name||c.Name===name);
if(args[0]==='compose'&&process.env.FAKE_COMPOSE_ERROR){console.error('build output '.repeat(600)+process.env.FAKE_COMPOSE_ERROR);process.exit(1)}
if(args[0]==='inspect') { const found=args.slice(1).map(match); if(found.some(x=>!x)){console.error('No such object');process.exit(1)} console.log(JSON.stringify(found)); }
else if(args[0]==='ps') {const project=value('--filter').split('=').slice(2).join('='); console.log(Object.values(db).filter(c=>c.Config.Labels['com.docker.compose.project']===project).map(c=>c.Id).join('\\n'));}
else if(args[0]==='exec') {}
else if(args[0]==='stop') {for(const id of args.slice(3)){const c=match(id);if(c){c.State.Running=false;c.State.Status='exited'}}write();}
else if(args[0]==='rm') {for(const id of args.slice(2)){const c=match(id);if(c)delete db[c.Id]}write();}
else if(args[0]==='run') {const name=value('--name'),labels={};for(let i=0;i<args.length;i++)if(args[i]==='--label'){const text=args[++i],at=text.indexOf('=');labels[text.slice(0,at)]=text.slice(at+1)}db[name]={Id:name,Name:name,Config:{Labels:labels},State:{Running:true,Status:'running'}};write();console.log(name);}
else if(args[0]==='compose') { const project=value('-p'), file=value('-f'), spec=JSON.parse(fs.readFileSync(file,'utf8'));for(const [name,svc] of Object.entries(spec.services||{})){const id=project+'-'+name;db[id]={Id:id,Name:id,UpCount:(db[id]?.UpCount||0)+1,Config:{Labels:{...svc.labels,'com.docker.compose.project':project,'com.docker.compose.service':name,'com.docker.compose.project.working_dir':require('node:path').dirname(file),'com.docker.compose.project.config_files':file}},State:{Running:true,Status:'running'}}}write(); }
else if(args[0]==='logs') console.log('Registered tunnel connection');
else {console.error('unexpected fake Docker command: '+args[0]);process.exit(2)}
`, { mode: 0o755 });
const reports = [];
let desired = {}, rollAt = 0, active, offline = false, hold = false;
const controller = http.createServer(async (req, res) => {
  let raw = ""; for await (const chunk of req) raw += chunk;
  if (req.url === "/api/agent") { res.writeHead(404); res.end(); return; }
  if (req.url === "/api/sync") {
    const body = JSON.parse(raw); reports.push(body);
    if (offline) { res.writeHead(503); res.end("offline fixture"); return; }
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(hold ? { hold: true, poll: 0.05 } : { domain: "example.test", desired, poll: 0.05,
      handover: body.started < rollAt, recovery: { payload: JSON.stringify({ run: body.run }), signature: "fake-signature" } }));
    return;
  }
  res.writeHead(500); res.end("unexpected control request");
});
await new Promise((done) => controller.listen(0, "127.0.0.1", done));
const control = `http://127.0.0.1:${controller.address().port}`;
const owner = namespace(control);
const infrastructure = {
  router: { Id: "router", Name: "router", Config: { Labels: {} }, Mounts: [{ Type: "bind", Source: join(data, "router"), Destination: "/etc/router" }], State: { Running: true } },
  tunnel: { Id: "tunnel", Name: "tunnel", Config: { Labels: {} }, Mounts: [{ Type: "bind", Source: join(data, "tunnel.yml"), Destination: "/etc/cloudflared/config.yml" }], State: { Running: true } },
};
const seed = { v: 1, bootId: "boot-current", control, started: Date.now() - 10000, agent: "host-review", run: "run-before-upgrade", machine: 1,
  tunnelToken: "fake-token", domain: "example.test", recovery: { payload: "old-receipt", signature: "fake" }, projects: [], desired: {}, metrics: [] };
async function launch(extraEnv = {}) {
  const child = spawn(process.execPath, [join(source, "agent.mjs")], { env: { ...process.env, PATH: `${bin}:${process.env.PATH}`, CONTROL_URL: control,
    RUNNER_DATA: data, JOIN_TOKEN: "fake-join", FAKE_DOCKER_STATE: dockerState, ROUTER_PORT: "29990", ...extraEnv }, stdio: ["ignore", "pipe", "pipe"] });
  child.output = ""; child.stdout.on("data", (x) => child.output += x); child.stderr.on("data", (x) => child.output += x);
  child.exited = new Promise((done) => child.once("exit", (code) => done(code)));
  active = child; return child;
}
async function until(check, child, timeout = 8000) {
  const end = Date.now() + timeout;
  while (!(await check())) { if (child.exitCode !== null || Date.now() > end) throw new Error(`agent condition failed: ${child.output}`); await new Promise((r) => setTimeout(r, 25)); }
}
async function stop(child) { child.kill("SIGTERM"); assert.equal(await child.exited, 0, child.output); active = null; }
let checks = 0;
try {
  await atomic(dockerState, infrastructure); await atomic(join(data, "agent-state.json"), seed);
  rollAt = Date.now() - 1;
  const first = await launch(); assert.equal(await first.exited, 0, first.output); active = null;
  assert.equal(reports.at(-1).run, seed.run);
  assert.equal(JSON.parse(await readFile(join(data, "agent-state.json"))).restartRequested, true);
  const secondStart = reports.length;
  const second = await launch(); await until(() => reports.length > secondStart + 1, second);
  assert.notEqual(reports[secondStart].run, seed.run);
  assert.equal(reports[secondStart].previousRun, seed.run);
  assert.equal(reports[secondStart].started < rollAt, false);
  assert.equal(JSON.parse(await readFile(dockerState)).tunnel.State.Running, true);
  await stop(second); checks++; console.log("ok an actual standalone upgrade creates one new run and preserves the serving tunnel");

  await atomic(join(data, "agent-state.json"), seed);
  const beforeFailure = reports.length;
  const failedUpgrade = await launch({ RUNNER_CODE_MANAGED: "1" });
  await until(() => failedUpgrade.output.includes("agent upgrade failed") && reports.length > beforeFailure + 2, failedUpgrade);
  assert.equal(failedUpgrade.exitCode, null);
  assert.equal(JSON.parse(await readFile(dockerState)).tunnel.State.Running, true);
  assert.equal(JSON.parse(await readFile(join(data, "agent-state.json"))).restartRequested, false);
  await stop(failedUpgrade); checks++; console.log("ok a failed managed-code upgrade keeps the same agent and tunnel running");

  rollAt = 0;
  const old = JSON.parse(await readFile(join(data, "agent-state.json")));
  old.bootId = "previous-boot";
  old.projects = [["worker", { app: "worker", replica: 1, v: 1, s: "healthy", port: null, busy: false, at: Date.now(), n: 0 }]];
  old.desired = { worker: { v: 1, app: "worker", replica: 1, port: null, compose: "services: {}", files: {} } };
  desired = old.desired;
  await atomic(join(data, "agent-state.json"), old);
  const beforeBoot = reports.length;
  const third = await launch(); await until(() => reports.length > beforeBoot, third);
  assert.notEqual(reports[beforeBoot].run, old.run);
  assert.equal(reports[beforeBoot].previousRun, old.run);
  assert.equal(reports[beforeBoot].inventory.worker.app, "worker");
  assert.equal(reports[beforeBoot].status.worker.s, "healthy");
  assert.deepEqual(reports[beforeBoot].recovery, old.recovery);
  await stop(third); checks++; console.log("ok an actual host reboot reports retained workload inventory and predecessor receipt");

  desired = {};
  hold = true;
  await atomic(dockerState, { ...infrastructure, tunnel: { ...infrastructure.tunnel, State: { Running: false, Status: "exited" } } });
  await atomic(join(data, "agent-state.json"), { ...seed, wasReady: false });
  const beforeHold = reports.length;
  const held = await launch(); await until(() => reports.length > beforeHold + 4, held);
  assert.equal(JSON.parse(await readFile(dockerState)).tunnel.State.Running, false);
  assert.equal(held.output.includes("tunnel online"), false);
  await stop(held); checks++; console.log("ok restarting a never-ready successor cannot bypass the controller's tunnel hold");

  hold = false; offline = true;
  await atomic(dockerState, { ...infrastructure, tunnel: { ...infrastructure.tunnel, State: { Running: false, Status: "exited" } } });
  await atomic(join(data, "agent-state.json"), { ...seed, wasReady: true });
  const beforeOffline = reports.length;
  const serving = await launch();
  await until(() => serving.output.includes("tunnel online") && reports.slice(beforeOffline).some((r) => r.ready), serving);
  assert.equal(JSON.parse(await readFile(dockerState)).tunnel.State.Running, true);
  await stop(serving); offline = false;
  checks++; console.log("ok an already-serving agent restores its owned tunnel while the controller is offline");

  const cpContainer = { Id: "cp", Name: "cp", Config: { Labels: { "com.docker.compose.project": "runner", "com.docker.compose.service": "control",
    "com.docker.compose.project.working_dir": join(root, "control"), "com.docker.compose.project.config_files": join(root, "control/compose.yaml") } }, State: { Running: true } };
  await atomic(dockerState, { ...infrastructure, cp: cpContainer });
  await atomic(join(data, "agent-state.json"), { ...seed, started: Date.now() });
  desired = { runner: { v: 1, app: "runner", replica: 1, port: null, compose: "services:\n  worker:\n    image: fake\n", files: {},
    managedCompose: JSON.stringify({ services: { worker: { image: "fake" } } }), dataFiles: {}, bindSources: [], revision: "rev-1" } };
  const fourth = await launch();
  await until(() => reports.at(-1)?.status?.runner?.s === "healthy", fourth);
  const project = `${owner}-runner`;
  let docker = JSON.parse(await readFile(dockerState));
  assert.equal(docker.cp.State.Running, true); assert.ok(docker[`${project}-worker`]);
  docker[`${project}-worker`].State = { Running: false, Status: "exited", ExitCode: 1 };
  await atomic(dockerState, docker);
  await until(() => reports.some((r) => r.status.runner?.n >= 1 && r.status.runner?.s === "healthy"), fourth);
  await until(async () => (await readdir(join(data, "deployments/runner"))).filter((name) => name.startsWith("release-")).length === 1, fourth);
  docker = JSON.parse(await readFile(dockerState)); assert.equal(docker[`${project}-worker`].State.Running, true); assert.equal(docker.cp.State.Running, true);
  const beforeVersion = docker[`${project}-worker`].UpCount;
  desired = { runner: { ...desired.runner, v: 2 } };
  await until(() => reports.at(-1)?.status?.runner?.v === 2, fourth);
  assert.equal(JSON.parse(await readFile(dockerState))[`${project}-worker`].UpCount, beforeVersion);
  await stop(fourth); checks++; console.log("ok namespaced deployment preserves a colocated CP and repairs a stopped background worker");

  await atomic(join(data, "agent-state.json"), { ...seed, started: Date.now() });
  desired = { broken: { ...desired.runner, app: "broken" } };
  const sentinel = "private-build-secret-sentinel";
  const broken = await launch({ FAKE_COMPOSE_ERROR: sentinel });
  await until(() => reports.at(-1)?.status?.broken?.s === "failed", broken);
  const diagnostic = reports.at(-1).status.broken.e;
  assert.ok(diagnostic.includes(sentinel)); assert.ok(diagnostic.length <= 4000);
  assert.ok(JSON.parse(await readFile(join(data, "agent-state.json"))).projects.find(([key]) => key === "broken")[1].e.includes(sentinel));
  await stop(broken);
  assert.equal(broken.output.includes(sentinel), false);
  assert.ok(broken.output.includes("details in authenticated diagnostics"));
  checks++; console.log("ok bounded build failure details reach private state/reports without leaking into agent stdout");
  console.log(`${checks} agent lifecycle checks passed`);
} finally {
  if (active) { active.kill("SIGKILL"); await active.exited; }
  await new Promise((done) => controller.close(done));
  await rm(root, { recursive: true, force: true });
}
