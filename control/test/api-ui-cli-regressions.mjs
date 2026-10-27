import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";
import { once } from "node:events";
import http from "node:http";
import vm from "node:vm";
import { Control } from "../src/index.js";
import { LocalStore } from "../src/local-store.mjs";

process.umask(0o077);
const directory = mkdtempSync(join(tmpdir(), "runner-api-ui-cli-"));
const stores = [];
let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const make = (requireAuth = true) => {
  const failures = [];
  const store = new LocalStore(join(directory, `${stores.length}.db`), { initialize: true, onFailure: (error) => failures.push(error) });
  stores.push(store);
  const control = new Control({ sql: store, page: "", alarm() {}, restart() {}, env: {
    DOMAIN: "example.test", ADMIN_PASSWORD: "test-owner-password", JOIN_TOKEN: "test-join",
    STATE_SIGNING_KEY: "test-signing-key", DNS: "off", REQUIRE_DEPLOY_AUTH: requireAuth ? "1" : "0",
  } });
  store.protect(control);
  return { control, store, failures };
};

try {
  const reads = make();
  assert.throws(() => reads.store.exec("SELECT * FROM events LIMIT ?", 1.5), /datatype mismatch/);
  assert.throws(() => reads.store.exec("SELECT * FROM nonexistent_table"), /no such table/);
  const status = await reads.control.fetch(new Request("https://cp.example.test/api/status"));
  check("bad read queries cannot poison storage or interrupt later requests", () => {
    assert.equal(status.status, 200);
    assert.equal(Boolean(reads.store.failed), false);
    assert.equal(reads.failures.length, 0);
  });

  const partial = make();
  const before = partial.store.db.prepare("SELECT * FROM main.settings").all();
  assert.throws(() => partial.control.putSettings({ rebalance: false, pools: { bad: -1 } }), /whole number/);
  check("partial configuration edits roll back and still stop further plans", () => {
    assert.deepEqual(partial.store.db.prepare("SELECT * FROM main.settings").all(), before);
    assert.equal(partial.store.failed, true);
    assert.equal(partial.failures.length, 1);
    assert.throws(() => partial.store.exec("SELECT 1"), /configuration storage failed/);
  });

  const write = make();
  assert.throws(() => write.control.saveAppPassword("broken", { salt: null, hash: null }), /NOT NULL/);
  check("a failed first write invalidates Maps even before SQLite changes a row", () => {
    assert.equal(write.store.failed, true);
    assert.equal(write.failures.length, 1);
    assert.equal(write.store.db.prepare("SELECT count(*) AS n FROM app_passwords").get().n, 0);
  });

  const editRead = make();
  assert.throws(() => editRead.store.mutate(() => {
    editRead.control.projects.set("incomplete", { name: "incomplete" });
    editRead.store.exec("SELECT 1 LIMIT ?", 1.5);
  }), /datatype mismatch/);
  check("read failures within configuration mutations remain fail-closed", () => {
    assert.equal(editRead.store.failed, true);
    assert.equal(editRead.failures.length, 1);
    assert.equal(editRead.store.mutationDepth, 0);
  });

  // Exercise the shipped browser handlers, including their password helper,
  // against the real API. DOM stubs provide only the controls these handlers use.
  const page = readFileSync(new URL("../src/app.html", import.meta.url), "utf8");
  const passwordStart = page.indexOf("const locked = ");
  const passwordEnd = page.indexOf("// ======================================================================== API", passwordStart);
  const submitStart = page.lastIndexOf("form.onsubmit = async (ev) => {", page.indexOf("// A new app's password is optional"));
  const submitEnd = page.indexOf("\n      };", submitStart);
  assert.ok(passwordStart >= 0 && passwordEnd > passwordStart && submitStart >= 0 && submitEnd > submitStart);
  const submitSource = page.slice(submitStart + "form.onsubmit = ".length, submitEnd + "\n      };".length - 1);
  async function submitNew({ requireAuth, cancel = false }) {
    const { control } = make(requireAuth);
    const elements = Object.fromEntries(["#ed-error", "#ed-name", "#ed-pw", "#ed-pw2", "#ed-save", "#pw-in", "#pw-admin"].map((id) => [id, {
      value: id === "#ed-name" ? "browser-app" : id === "#pw-in" ? "test-owner-password" : "",
      scrollIntoView() {}, focus() {}, select() {},
    }]));
    let prompts = 0, requests = 0;
    const context = vm.createContext({
      $: (id) => elements[id], S: { status: { requireDeployAuth: requireAuth, projects: [] } },
      name: null, latest: null, disposed: false, MIN_APP_PASSWORD: 8, checkName: () => true, checkReplicas: () => true,
      formSpec: () => ({ compose: 'services:\n  web:\n    image: nginx\n    ports: ["8080:80"]\n', port: 8080 }),
      enc: encodeURIComponent, esc: (text) => text, dirty: false, fast() {}, toast() {}, appHref: (name) => name,
      location: {}, loadStatus() {},
      sheet: async ({ onOk }) => { prompts++; if (cancel) return false; await onOk({}); return true; },
      api: async (method, path, body, { secret = {} } = {}) => {
        requests++;
        const response = await control.fetch(new Request(`https://cp.example.test/admin/api${path}`, { method,
          headers: { "content-type": "application/json", ...(secret.admin ? { "x-admin-password": secret.admin } : {}), ...(secret.app ? { "x-app-password": secret.app } : {}) },
          body: JSON.stringify(body),
        }));
        const data = await response.json();
        if (!response.ok) throw Object.assign(new Error(data.error), { status: response.status });
        return data;
      },
    });
    vm.runInContext(page.slice(passwordStart, passwordEnd), context);
    await vm.runInContext(`(${submitSource})`, context)({ preventDefault() {} });
    return { prompts, requests, created: control.projects.has("browser-app"), error: elements["#ed-error"].innerHTML, disabled: elements["#ed-save"].disabled };
  }
  const owner = await submitNew({ requireAuth: true });
  check("the new-app browser form prompts and deploys successfully on a private fleet", () => {
    assert.equal(owner.prompts, 1); assert.equal(owner.created, true); assert.equal(owner.error, undefined);
  });
  const cancelled = await submitNew({ requireAuth: true, cancel: true });
  check("cancelling the new-app password prompt sends no deployment", () => {
    assert.equal(cancelled.prompts, 1); assert.equal(cancelled.requests, 0); assert.equal(cancelled.created, false); assert.equal(cancelled.disabled, false);
  });
  const community = await submitNew({ requireAuth: false });
  check("explicit public fleets can still deploy through the browser without a prompt", () => {
    assert.equal(community.prompts, 0); assert.equal(community.created, true); assert.equal(community.error, undefined);
  });

  const guideStart = page.indexOf("function vGuide() {");
  const guideEnd = page.indexOf("// ======================================================================== shell", guideStart);
  for (const requireAuth of [true, false]) {
    const guide = { childElementCount: 0 };
    const context = vm.createContext({ $: () => guide, location: { origin: "https://cp.example.test" }, S: { status: { domain: "example.test", requireDeployAuth: requireAuth } },
      esc: (value) => value, codebox: (value) => value, MAX_REPLICAS: 50, MIN_APP_PASSWORD: 8 });
    vm.runInContext(page.slice(guideStart, guideEnd), context);
    context.vGuide().status();
    check(`the deployed guide describes ${requireAuth ? "private" : "public"} fleet authentication`, () => {
      assert.equal(guide.innerHTML.includes(' -H "x-admin-password: $ADMIN_PASSWORD"'), requireAuth);
      assert.equal(guide.innerHTML.includes("no credentials needed for new apps"), !requireAuth);
    });
  }

  // A real curl/jq pipeline must preserve HTTP failures, including commands that
  // normally print only one response field. The server and credentials are fake.
  let replyStatus = 401;
  const mock = http.createServer((request, response) => {
    request.resume();
    response.writeHead(replyStatus, { "content-type": "application/json" });
    response.end(JSON.stringify(replyStatus === 200 ? { name: "cli-app", version: 1 } : { error: "test deployment rejected" }));
  });
  mock.listen(0, "127.0.0.1");
  await once(mock, "listening");
  const repo = fileURLToPath(new URL("../..", import.meta.url));
  const cli = (args) => new Promise((resolve, reject) => {
    const child = spawn("bash", [join(repo, "bin/runnerctl"), ...args], { cwd: repo,
      env: { ...process.env, RUNNER_URL: `http://127.0.0.1:${mock.address().port}`, ADMIN_PASSWORD: "test-cli-password" }, stdio: ["ignore", "pipe", "pipe"] });
    let out = "", err = "";
    child.stdout.on("data", (data) => { out += data; }); child.stderr.on("data", (data) => { err += data; });
    child.on("error", reject); child.on("exit", (code) => resolve({ code, out, err }));
  });
  try {
    for (const args of [["apply", "examples/hello", "cli-app", "--port", "9000"], ["move", "cli-app", "1", "2"], ["status"]]) {
      const result = await cli(args);
      check(`runnerctl ${args[0]} fails on HTTP errors and retains the server explanation`, () => {
        assert.notEqual(result.code, 0); assert.match(result.err, /test deployment rejected/);
      });
    }
    replyStatus = 200;
    const result = await cli(["apply", "examples/hello", "cli-app", "--port", "9000"]);
    check("successful CLI deployments retain their JSON output and zero status", () => {
      assert.equal(result.code, 0); assert.equal(JSON.parse(result.out).name, "cli-app");
    });
  } finally { mock.closeAllConnections(); await new Promise((resolve) => mock.close(resolve)); }
  console.log(`${checks} API, UI and CLI regression checks passed`);
} finally {
  for (const store of stores) store.close();
  rmSync(directory, { recursive: true, force: true });
}
