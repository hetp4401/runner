import { createHash } from "node:crypto";
import { posix } from "node:path";
import YAML from "yaml";
const normalize = (source) => posix.normalize(source).replace(/\/+$/, "") || ".";

export function relativeBinds(doc) {
  const out = [];
  for (const service of Object.values(doc.services ?? {})) for (const volume of Array.isArray(service?.volumes) ? service.volumes : []) {
    const parts = typeof volume === "string" ? volume.split(":") : null;
    const source = parts ? (parts.length > 1 && /^\./.test(parts[0]) ? parts[0] : null) : volume?.type === "bind" ? volume.source : null;
    if (typeof source !== "string" || source.startsWith("/") || source.includes("$") || source.startsWith("~")) continue;
    const path = normalize(source);
    if (path === ".." || path.startsWith("../")) continue;
    out.push({ path, readOnly: parts ? (parts[2] ?? "").split(",").includes("ro") : volume.read_only === true });
  }
  return out;
}

// Compose/source are staged in a private, fresh generation on the agent. Bind
// data remains at its stable host path, so updates never rename local app data.
export function agentPlan(plan) {
  const doc = YAML.parse(plan.compose, { merge: true });
  const binds = new Set();
  const dataSource = (source) => {
    if (typeof source !== "string" || !source || source.startsWith("/") || source.includes("$") || source.startsWith("~")) return source;
    const relative = normalize(source);
    if (relative !== ".." && !relative.startsWith("../")) binds.add(relative);
    return `\${RUNNER_APP_DATA}/${relative}`;
  };
  for (const service of Object.values(doc.services ?? {})) {
    if (!Array.isArray(service?.volumes)) continue;
    service.volumes = service.volumes.map((volume) => {
      if (typeof volume === "string") {
        const parts = volume.split(":");
        if (parts.length > 1 && /^\./.test(parts[0])) parts[0] = dataSource(parts[0]);
        return parts.join(":");
      }
      return volume?.type === "bind" ? { ...volume, source: dataSource(volume.source) } : volume;
    });
  }
  const dataFiles = Object.fromEntries(Object.entries(plan.files ?? {}).filter(([path]) =>
    [...binds].some((bind) => bind === "." || path === bind || path.startsWith(`${bind}/`))));
  const services = Object.entries(doc.services ?? {}).filter(([, service]) => !service?.profiles?.length).map(([name]) => name);
  const oneshotServices = [...new Set(Object.values(doc.services ?? {}).flatMap((service) =>
    Object.entries(service?.depends_on ?? {}).filter(([, dependency]) => dependency?.condition === "service_completed_successfully").map(([name]) => name)))];
  const revision = createHash("sha256").update(JSON.stringify({ app: plan.app, replica: plan.replica,
    compose: plan.compose, port: plan.port, files: Object.entries(plan.files ?? {}).sort(([a], [b]) => a.localeCompare(b)), ready: plan.ready ?? null })).digest("hex");
  return { ...plan, revision, managedCompose: JSON.stringify(doc), dataFiles, services, oneshotServices,
    bindSources: relativeBinds(YAML.parse(plan.compose, { merge: true })) };
}
