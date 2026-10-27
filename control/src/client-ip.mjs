import { BlockList, isIP } from "node:net";

const DEFAULT_PROXIES = "127.0.0.0/8,::1/128";
const HEADERS = new Set(["cf-connecting-ip", "x-forwarded-for", "none"]);
const configurations = new WeakMap();

function normalizeAddress(value, mapIPv4 = true) {
  if (typeof value !== "string") return null;
  const address = value.trim();
  const family = isIP(address);
  if (family === 4) return address;
  if (family !== 6 || address.includes("%")) return null;
  // URL parsing gives IPv6 one spelling, so equivalent addresses share a rate
  // limit bucket. Scoped addresses and host:port values are not forwarded IPs.
  const canonical = new URL(`http://[${address}]/`).hostname.slice(1, -1);
  const mapped = mapIPv4 && /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(canonical);
  if (!mapped) return canonical;
  const high = parseInt(mapped[1], 16), low = parseInt(mapped[2], 16);
  return `${high >> 8}.${high & 255}.${low >> 8}.${low & 255}`;
}

// Validate once at startup; request handling reuses the result while settings
// are unchanged. Empty TRUSTED_PROXIES deliberately disables forwarded IPs.
export function validateProxyConfig(env = {}) {
  const rawHeader = env.CLIENT_IP_HEADER ?? "cf-connecting-ip";
  const rawProxies = env.TRUSTED_PROXIES ?? DEFAULT_PROXIES;
  const cached = configurations.get(env);
  if (cached?.rawHeader === rawHeader && cached?.rawProxies === rawProxies) return cached.config;
  if (typeof rawHeader !== "string" || !HEADERS.has(rawHeader.trim().toLowerCase())) {
    throw new Error("CLIENT_IP_HEADER must be cf-connecting-ip, x-forwarded-for, or none");
  }
  if (typeof rawProxies !== "string") throw new Error("TRUSTED_PROXIES must be a list of IP addresses or CIDRs");
  const header = rawHeader.trim().toLowerCase();
  const entries = rawProxies.trim() ? rawProxies.trim().split(/[\s,]+/) : [];
  const trusted = new BlockList();
  for (const entry of entries) {
    const parts = entry.split("/");
    const address = normalizeAddress(parts[0], false);
    const family = address && isIP(address);
    const bits = family === 4 ? 32 : 128;
    if (!address || parts.length > 2 || (parts.length === 2 && (!/^\d+$/.test(parts[1]) || Number(parts[1]) > bits))) {
      throw new Error(`invalid TRUSTED_PROXIES entry: ${entry}`);
    }
    const type = family === 4 ? "ipv4" : "ipv6";
    if (parts.length === 1) trusted.addAddress(address, type);
    else trusted.addSubnet(address, Number(parts[1]), type);
  }
  const config = Object.freeze({
    header,
    trustedProxies: Object.freeze(entries),
    isTrusted: (address) => trusted.check(address, isIP(address) === 4 ? "ipv4" : "ipv6"),
  });
  configurations.set(env, { rawHeader, rawProxies, config });
  return config;
}

export function clientAddress(req, env = {}) {
  const config = validateProxyConfig(env);
  const remote = normalizeAddress(req.socket?.remoteAddress);
  if (!remote || config.header === "none" || !config.isTrusted(remote)) return remote;
  const header = req.headers?.[config.header];
  if (typeof header !== "string" || !header.trim()) return remote;
  if (config.header === "cf-connecting-ip") return normalizeAddress(header) ?? remote;

  const chain = header.split(",").map((address) => normalizeAddress(address));
  if (chain.some((address) => !address)) return remote;
  // The nearest proxy appends its peer. Stop at the first untrusted hop, rather
  // than accepting an attacker-controlled address from the left of the list.
  let address = remote;
  for (let index = chain.length - 1; index >= 0 && config.isTrusted(address); index--) address = chain[index];
  return address;
}
