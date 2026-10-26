// Tagged tuples preserve Maps/Sets without interpreting keys in user data.
export function pack(value) {
  if (value instanceof Map) return ["m", [...value].map(([k, v]) => [pack(k), pack(v)])];
  if (value instanceof Set) return ["s", [...value].map(pack)];
  if (Array.isArray(value)) return ["a", value.map(pack)];
  if (value === undefined) return ["u"];
  if (typeof value === "number" && !Number.isFinite(value)) return ["n", String(value)];
  if (value && typeof value === "object") {
    if (Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) throw new Error("non-data object in checkpoint");
    return ["o", Object.entries(value).map(([k, v]) => [k, pack(v)])];
  }
  if (typeof value === "function") throw new Error("function in checkpoint");
  return ["v", value];
}
export function unpack([type, value]) {
  switch (type) {
    case "m": return new Map(value.map(([k, v]) => [unpack(k), unpack(v)]));
    case "s": return new Set(value.map(unpack));
    case "a": return value.map(unpack);
    case "o": return Object.fromEntries(value.map(([k, v]) => [k, unpack(v)]));
    case "u": return undefined;
    case "n": return Number(value);
    case "v": return value;
    default: throw new Error("invalid checkpoint encoding");
  }
}

const LOCAL = new Set(["db", "sql", "env", "page", "setAlarm", "restart", "versions", "remapped", "logSessions", "wakers", "geoPending", "tunnelJobs", "metricsCache"]);
export function capture(control) {
  return pack(Object.fromEntries(Object.entries(control).filter(([key, value]) => !LOCAL.has(key) && typeof value !== "function")));
}
export function restore(control, state) {
  const values = unpack(state);
  for (const key of Object.keys(control)) {
    if (!LOCAL.has(key) && typeof control[key] !== "function" && !Object.hasOwn(values, key)) delete control[key];
  }
  for (const [key, value] of Object.entries(values)) {
    if (LOCAL.has(key) || !/^[a-zA-Z][a-zA-Z0-9]*$/.test(key) || key in Object.prototype || typeof control[key] === "function") throw new Error(`invalid control checkpoint field ${key}`);
    control[key] = value;
  }
  control.metricsCache.clear();
}

export function difference(before, after) {
  const old = new Map(before?.[1] ?? []);
  const updates = [];
  for (const [key, value] of after[1]) {
    const previous = old.get(key);
    old.delete(key);
    if (value[0] === "m" && previous?.[0] === "m") {
      const entries = new Map(previous[1].map(([k, v]) => [JSON.stringify(k), JSON.stringify(v)]));
      const puts = [];
      for (const [k, v] of value[1]) {
        const id = JSON.stringify(k);
        if (entries.get(id) !== JSON.stringify(v)) puts.push([k, v]);
        entries.delete(id);
      }
      if (puts.length || entries.size) updates.push([key, "map", puts, [...entries.keys()].map(JSON.parse)]);
    } else if (JSON.stringify(previous) !== JSON.stringify(value)) updates.push([key, "value", value]);
  }
  for (const key of old.keys()) updates.push([key, "delete"]);
  return updates;
}
export function applyDifference(control, updates) {
  for (const [key, type, value, removed] of updates) {
    if (LOCAL.has(key) || !/^[a-zA-Z][a-zA-Z0-9]*$/.test(key) || key in Object.prototype || typeof control[key] === "function") throw new Error("invalid checkpoint update");
    if (type === "delete") delete control[key];
    else if (type === "value") control[key] = unpack(value);
    else if (type === "map" && control[key] instanceof Map) {
      for (const k of removed) control[key].delete(unpack(k));
      for (const [k, v] of value) control[key].set(unpack(k), unpack(v));
    } else throw new Error("invalid checkpoint update type");
  }
  control.metricsCache.clear();
}
