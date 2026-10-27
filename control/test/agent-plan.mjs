import assert from "node:assert/strict";
import { agentPlan } from "../src/agent-plan.mjs";
import { makeControl } from "./sim/lib.mjs";

const compose = `services:
  web:
    image: busybox
    volumes: ["./data:/data", "./config.txt:/config.txt:ro"]
    env_file: .env
    depends_on:
      migration:
        condition: service_completed_successfully
  migration:
    image: busybox
`;
const plan = { app: "web", replica: 1, v: 1, compose, port: null,
  files: { "data/seed.txt": "seed", "config.txt": "config", ".env": "TOKEN='private'", "Dockerfile": "FROM busybox" } };
const result = agentPlan(plan), managed = JSON.parse(result.managedCompose);
assert.deepEqual(managed.services.web.volumes, ["${RUNNER_APP_DATA}/data:/data", "${RUNNER_APP_DATA}/config.txt:/config.txt:ro"]);
assert.deepEqual(result.dataFiles, { "data/seed.txt": "seed", "config.txt": "config" });
assert.equal(managed.services.web.env_file, ".env");
assert.deepEqual(result.bindSources, [{ path: "data", readOnly: false }, { path: "config.txt", readOnly: true }]);
assert.deepEqual(result.oneshotServices, ["migration"]);
assert.equal(agentPlan({ ...plan, v: 2 }).revision, result.revision);
assert.notEqual(agentPlan({ ...plan, files: { ...plan.files, ".env": "TOKEN='changed'" } }).revision, result.revision);
console.log("ok private deployment source retains stable bind data, secret files, one-shot dependencies and content identity");
const control = makeControl();
try {
  assert.throws(() => control.putProject("unsafe", { compose: 'services:\n  app:\n    image: busybox\n    volumes: ["./:/app", "./data:/data"]\n' }), /writable bind must not contain/);
  for (const [name, volumes] of [["readonly", '["./:/app:ro", "./data:/data"]'], ["shared", '["./data:/one", "./data:/two"]']]) {
    control.putProject(name, { compose: `services:\n  app:\n    image: busybox\n    volumes: ${volumes}\n` });
  }
  console.log("ok delegated bind layouts prevent source-path races while allowing shared data and read-only parents");
} finally { control.db.close(); }
