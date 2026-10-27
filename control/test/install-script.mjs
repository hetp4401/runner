// Run with Node 24. All Docker/network commands are fake and all writes stay in
// temporary directories; this never touches a host daemon or /var/lib.
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, existsSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { installScript } from "../src/install-script.mjs";
import { loadAgentBundle } from "../src/agent-bundle.mjs";

const root = mkdtempSync(join(tmpdir(), "runner-installer-check-"));
const origin = "https://runners.example.test";
const previousSelection = JSON.stringify({ revision: "a".repeat(64) });
const bundle = loadAgentBundle(fileURLToPath(new URL("../..", import.meta.url)));
const program = installScript(origin, `${origin}/agent`).replace(
  "exec 9>/var/lib/runner-installer.lock", 'exec 9>"$RUNNER_TEST_INSTALLER_LOCK"');
assert.ok(!program.includes("9>/var/lib/"), "installer lock must be redirected into the test directory");

const fake = `#!/usr/bin/env node
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const command=path.basename(process.argv[1]),args=process.argv.slice(2),scenario=process.env.SCENARIO;
const state=JSON.parse(fs.readFileSync(process.env.STATE,'utf8'));
const save=()=>fs.writeFileSync(process.env.STATE,JSON.stringify(state));
const fail=(code=1)=>process.exit(code);
fs.appendFileSync(process.env.AUDIT,JSON.stringify([command,...args])+'\\n');
if(command==='curl') {
  if(args.some(a=>a.endsWith('/bundle.sha256'))) console.log(scenario==='bad_digest'?'f'.repeat(64):process.env.ARCHIVE_HASH);
  else {const i=args.indexOf('-o');if(i<0)fail();fs.copyFileSync(process.env.ARCHIVE,args[i+1]);}
} else if(command==='sha256sum') {
  console.log(crypto.createHash('sha256').update(fs.readFileSync(args[0])).digest('hex')+'  '+args[0]);
} else if(command==='flock'||command==='sleep') {
  // OS locking is covered by Docker integration; here only replacement control flow is under test.
} else if(command==='docker') {
  const op=args[0],find=(ref)=>Object.values(state.containers).find(c=>c.id===ref||c.name===ref);
  if(op==='info') console.log('linux');
  else if(op==='inspect') {
    const c=find(args.at(-1));if(!c)fail();
    const format=args[args.indexOf('--format')+1];
    if(args.includes('--format')) {
      if(format.includes('.Config.Env'))console.log('CONTROL_URL='+c.control);
      else if(format==='{{.Id}}')console.log(c.id);
      else if(format==='{{.Name}}')console.log('/'+c.name);
      else if(format==='{{.State.Running}}')console.log(c.running?'true':'false');
      else fail(4);
    } else console.log(JSON.stringify(c));
  } else if(op==='build') {
    if(scenario==='build_failure')fail(7);
    if(scenario==='changed_owner'){state.containers.old.control='https://another.example.test';save();}
  } else if(op==='stop') {
    const c=find(args.at(-1));if(!c)fail();c.running=false;save();
  } else if(op==='rename') {
    const c=find(args[1]);if(!c)fail();
    if(scenario==='rename_failure'&&args[2]!=='runner-agent'&&!state.renameFailed){state.renameFailed=true;save();fail(7);}
    c.name=args[2];save();
  } else if(op==='run') {
    state.containers.new={id:'new',name:'runner-agent',control:process.env.ORIGIN,running:scenario!=='immediate_exit'};save();
    if(scenario==='run_failure')fail(125);
    console.log('new');
  } else if(op==='rm') {
    const c=find(args.at(-1));if(!c)fail();delete state.containers[c.id];save();
  } else if(op==='start') {
    const c=find(args.at(-1));if(!c)fail();c.running=true;save();console.log(c.id);
  } else fail(5);
} else fail(6);
`;

try {
  const archive = join(root, "bundle.tar"); writeFileSync(archive, bundle.archive);
  const bin = join(root, "bin"); mkdirSync(bin);
  symlinkSync(process.execPath, join(bin, "node"));
  for (const command of ["docker", "curl", "sha256sum", "flock", "sleep"]) writeFileSync(join(bin, command), fake, { mode: 0o755 });
  const scenarios = ["bad_digest", "build_failure", "symlink_cache", "symlink_selection", "changed_owner", "run_failure", "immediate_exit", "success", "rename_failure"];
  for (const scenario of scenarios) {
    const directory = join(root, scenario), data = join(directory, "data"), cache = join(data, "agent-code");
    mkdirSync(cache, { recursive: true });
    const current = join(cache, "current.json"); writeFileSync(current, previousSelection);
    if (scenario === "symlink_cache") {
      rmSync(cache, { recursive: true }); const other = join(directory, "other"); mkdirSync(other);
      writeFileSync(join(other, "current.json"), previousSelection); symlinkSync(other, cache);
    } else if (scenario === "symlink_selection") {
      rmSync(current); const other = join(directory, "selection.json"); writeFileSync(other, previousSelection); symlinkSync(other, current);
    }
    const stateFile = join(directory, "state.json"), audit = join(directory, "audit.jsonl");
    writeFileSync(stateFile, JSON.stringify({ containers: { old: { id: "old", name: "runner-agent", control: origin, running: true } } }));
    const run = spawnSync("/bin/sh", ["-c", program], { encoding: "utf8", timeout: 20_000, env: {
      ...process.env, PATH: `${bin}:${process.env.PATH}`, TMPDIR: directory, RUNNER_DATA: data, JOIN_TOKEN: "fake-test-join", ORIGIN: origin,
      RUNNER_TEST_INSTALLER_LOCK: join(directory, "installer.lock"), SCENARIO: scenario, STATE: stateFile, AUDIT: audit,
      ARCHIVE: archive, ARCHIVE_HASH: bundle.archiveHash,
    } });
    assert.ok(existsSync(audit), `${scenario}: fake commands did not run: ${run.stderr || run.error || run.stdout}`);
    const state = JSON.parse(readFileSync(stateFile, "utf8"));
    const calls = readFileSync(audit, "utf8").trim().split("\n").map(JSON.parse).filter(([command]) => command === "docker");
    assert.notEqual(run.status, null, `${scenario}: ${run.error || "installer timed out"}`);
    if (scenario === "success") assert.equal(run.status, 0, `${scenario}: ${run.stderr}`);
    else assert.notEqual(run.status, 0, `${scenario}: an invalid installation unexpectedly succeeded`);
    if (scenario === "success") {
      assert.deepEqual(Object.keys(state.containers), ["new"]);
      assert.equal(state.containers.new.name, "runner-agent"); assert.equal(state.containers.new.running, true);
      assert.equal(existsSync(current), false);
      assert.ok(calls.findIndex(([, op]) => op === "build") < calls.findIndex(([, op]) => op === "stop"));
      assert.ok(calls.findIndex(([, op]) => op === "run") < calls.findIndex(([, op]) => op === "rm"));
    } else {
      assert.deepEqual(Object.keys(state.containers), ["old"], `${scenario}: previous container was lost`);
      assert.equal(state.containers.old.name, "runner-agent", `${scenario}: previous name was not restored`);
      assert.equal(state.containers.old.running, true, `${scenario}: previous agent is stopped`);
      assert.equal(readFileSync(current, "utf8"), previousSelection, `${scenario}: previous code selection changed`);
      if (!["run_failure", "immediate_exit", "rename_failure"].includes(scenario)) assert.ok(!calls.some(([, op]) => ["stop", "rename", "rm", "run"].includes(op)), `${scenario}: existing agent was changed before preflight finished`);
    }
    console.log(`ok installer ${scenario.replaceAll("_", " ")}`);
  }
  console.log(`${scenarios.length} safe installer replacement checks passed`);
} finally { rmSync(root, { recursive: true, force: true }); }
