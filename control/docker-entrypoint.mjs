import { spawn } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { settings, identity, initialize, resolveCloudflare, controllerTunnel } from "./src/bootstrap.mjs";

process.umask(0o077);
const command = process.argv[2] || "start";
const children = new Set();
let stopping = false;
function stop(code = 0) {
  if (stopping) return;
  stopping = true;
  for (const child of children) child.kill("SIGTERM");
  if (!children.size) process.exit(code);
  const deadline = setTimeout(() => process.exit(code), 10_000);
  for (const child of children) child.once("exit", () => { if (!children.size) { clearTimeout(deadline); process.exit(code); } });
}
function launch(binary, args, env) {
  const child = spawn(binary, args, { env, stdio: "inherit" });
  children.add(child);
  child.on("error", (error) => { console.error(`child could not start: ${error.message}`); children.delete(child); stop(1); });
  child.on("exit", (code) => { children.delete(child); if (!stopping) stop(code || 1); });
  return child;
}
process.on("SIGTERM", () => stop());
process.on("SIGINT", () => stop());

try {
  let env = settings(process.env);
  if (command === "init") {
    initialize(env);
    console.log("Fleet initialized. Read its admin password and join token with: docker compose run --rm control credentials");
  } else if (command === "credentials") {
    env = identity(env);
    console.log(JSON.stringify({ ADMIN_PASSWORD: env.ADMIN_PASSWORD, JOIN_TOKEN: env.JOIN_TOKEN }, null, 2));
  } else if (command === "start") {
    if (!existsSync(join(env.DATA_DIR, "control.db"))) throw new Error("control.db is missing; restore the fleet or run init for a new fleet");
    env = identity(env, { persistNamespace: true });
    if (!env.CF_API_TOKEN && !(env.CF_API_KEY && env.CF_API_EMAIL)) throw new Error("set CF_API_TOKEN, or both CF_API_KEY and CF_API_EMAIL");
    env = await resolveCloudflare(env);
    let token;
    if (env.AUTO_TUNNEL === "1") token = await controllerTunnel(env);
    if (stopping) process.exit(0);
    launch(process.execPath, ["server.mjs"], env);
    if (token) {
      const directory = "/run/runner";
      mkdirSync(directory, { recursive: true, mode: 0o700 });
      const file = join(directory, "ingress.yml");
      writeFileSync(file, `ingress:\n  - hostname: ${JSON.stringify(env.CONTROL_HOST)}\n    service: http://127.0.0.1:${env.PORT}\n  - service: http_status:404\n`, { mode: 0o600 });
      launch("cloudflared", ["tunnel", "--no-autoupdate", "--config", file, "--metrics", `127.0.0.1:${env.TUNNEL_METRICS_PORT}`, "run"], { PATH: process.env.PATH, TUNNEL_TOKEN: token });
    }
    console.log(`Control plane starting at ${env.PUBLIC_URL}`);
  } else throw new Error("command must be start, init or credentials");
} catch (error) {
  console.error(error.message);
  stop(1);
}
