import assert from "node:assert/strict";
import { clientAddress, validateProxyConfig } from "../src/client-ip.mjs";

let checks = 0;
const check = (name, fn) => { fn(); checks++; console.log(`ok ${name}`); };
const request = (remoteAddress, headers = {}) => ({ socket: { remoteAddress }, headers });
const address = (remote, headers, env) => clientAddress(request(remote, headers), env);

check("loopback Cloudflare proxies preserve distinct client addresses by default", () => {
  assert.equal(address("127.0.0.1", { "cf-connecting-ip": "198.51.100.1" }), "198.51.100.1");
  assert.equal(address("127.0.0.1", { "cf-connecting-ip": "198.51.100.2" }), "198.51.100.2");
  assert.equal(address("::1", { "cf-connecting-ip": "2001:0DB8:0000::0001" }), "2001:db8::1");
});

check("forwarding headers from an untrusted peer cannot spoof the client", () => {
  assert.equal(address("203.0.113.8", { "cf-connecting-ip": "198.51.100.1", "x-forwarded-for": "198.51.100.2" }), "203.0.113.8");
  assert.equal(address("172.17.0.1", { "cf-connecting-ip": "198.51.100.1" }), "172.17.0.1");
});

check("an explicitly trusted Docker bridge peer does not trust its whole network", () => {
  const env = { TRUSTED_PROXIES: "172.17.0.1/32" };
  assert.equal(address("172.17.0.1", { "cf-connecting-ip": "198.51.100.1" }, env), "198.51.100.1");
  assert.equal(address("172.17.0.2", { "cf-connecting-ip": "198.51.100.1" }, env), "172.17.0.2");
  assert.equal(address("127.0.0.1", { "cf-connecting-ip": "198.51.100.1" }, env), "127.0.0.1");
});

check("IPv4-mapped IPv6 peers and clients normalize to their IPv4 identity", () => {
  assert.equal(address("::ffff:127.0.0.1", { "cf-connecting-ip": "::ffff:c633:6402" }), "198.51.100.2");
  assert.equal(address("0:0:0:0:0:ffff:7f00:1", { "cf-connecting-ip": "::ffff:198.51.100.2" }), "198.51.100.2");
  assert.equal(address("::ffff:cb00:7108", { "cf-connecting-ip": "198.51.100.2" }), "203.0.113.8");
});

check("missing, malformed, duplicate or port-qualified headers fall back to the socket", () => {
  for (const value of [undefined, "", "invalid", "198.51.100.1:443", "[2001:db8::1]", "fe80::1%eth0", "198.51.100.1, 198.51.100.2", ["198.51.100.1"]]) {
    assert.equal(address("127.0.0.1", { "cf-connecting-ip": value }), "127.0.0.1");
  }
  assert.equal(address("127.0.0.1", { "x-forwarded-for": "198.51.100.1" }), "127.0.0.1");
});

check("X-Forwarded-For walks the trusted chain from the nearest proxy", () => {
  const env = { CLIENT_IP_HEADER: "x-forwarded-for", TRUSTED_PROXIES: "127.0.0.1,10.0.0.0/24" };
  assert.equal(address("127.0.0.1", { "x-forwarded-for": "198.51.100.4, 10.0.0.2, 10.0.0.3" }, env), "198.51.100.4");
  assert.equal(address("127.0.0.1", { "x-forwarded-for": "198.51.100.99, 203.0.113.8, 10.0.0.3" }, env), "203.0.113.8");
  assert.equal(address("127.0.0.1", { "x-forwarded-for": "198.51.100.99, 203.0.113.8" }, env), "203.0.113.8");
  assert.equal(address("203.0.113.8", { "x-forwarded-for": "198.51.100.99" }, env), "203.0.113.8");
});

check("X-Forwarded-For supports IPv6 chains and mapped IPv4 addresses", () => {
  const env = { CLIENT_IP_HEADER: "x-forwarded-for", TRUSTED_PROXIES: "::1,2001:db8:1::/48,10.0.0.0/8" };
  assert.equal(address("::1", { "x-forwarded-for": "2001:0DB8:2:0:0:0:0:4, 2001:db8:1::3" }, env), "2001:db8:2::4");
  assert.equal(address("::1", { "x-forwarded-for": "::ffff:c633:6402, ::ffff:10.0.0.1" }, env), "198.51.100.2");
});

check("a malformed X-Forwarded-For chain is never partially trusted", () => {
  const env = { CLIENT_IP_HEADER: "x-forwarded-for" };
  for (const value of [",198.51.100.1", "198.51.100.1,", "bad, 198.51.100.1", "198.51.100.1:1234", "unknown"]) {
    assert.equal(address("127.0.0.1", { "x-forwarded-for": value }, env), "127.0.0.1");
  }
});

check("none and an empty proxy list explicitly disable forwarding trust", () => {
  assert.equal(address("127.0.0.1", { "cf-connecting-ip": "198.51.100.1" }, { CLIENT_IP_HEADER: "none" }), "127.0.0.1");
  assert.equal(address("127.0.0.1", { "cf-connecting-ip": "198.51.100.1" }, { TRUSTED_PROXIES: "" }), "127.0.0.1");
});

check("CIDR validation accepts exact IPs, IPv6, and mapped networks", () => {
  validateProxyConfig({ TRUSTED_PROXIES: "192.0.2.1 198.51.100.0/24,2001:db8::/32,::ffff:10.0.0.0/104" });
  assert.equal(address("::ffff:10.0.0.1", { "cf-connecting-ip": "198.51.100.1" }, { TRUSTED_PROXIES: "::ffff:10.0.0.0/104" }), "198.51.100.1");
  assert.equal(address("192.0.2.1", { "cf-connecting-ip": "198.51.100.1" }, { TRUSTED_PROXIES: "192.0.2.1" }), "198.51.100.1");
});

check("malformed proxy settings fail validation before serving requests", () => {
  for (const value of ["localhost", "192.0.2.1/33", "::1/129", "192.0.2.1/-1", "192.0.2.1/1.5", "192.0.2.1/", "::1/128/1", "fe80::1%eth0", "192.0.2.1:80", 42]) {
    assert.throws(() => validateProxyConfig({ TRUSTED_PROXIES: value }), /TRUSTED_PROXIES/);
  }
  for (const value of ["", "x-real-ip", "x-forwarded-for\r\nx-other", 42]) {
    assert.throws(() => validateProxyConfig({ CLIENT_IP_HEADER: value }), /CLIENT_IP_HEADER/);
  }
});

check("cached configurations are revalidated when environment values change", () => {
  const env = { CLIENT_IP_HEADER: " CF-Connecting-IP " };
  const first = validateProxyConfig(env);
  assert.equal(first, validateProxyConfig(env));
  env.CLIENT_IP_HEADER = "none";
  assert.notEqual(first, validateProxyConfig(env));
  assert.equal(address("127.0.0.1", { "cf-connecting-ip": "198.51.100.1" }, env), "127.0.0.1");
  env.TRUSTED_PROXIES = "invalid";
  assert.throws(() => validateProxyConfig(env), /TRUSTED_PROXIES/);
});

check("a missing or invalid socket address cannot borrow a forwarded identity", () => {
  assert.equal(clientAddress({ headers: { "cf-connecting-ip": "198.51.100.1" } }), null);
  assert.equal(address("invalid", { "cf-connecting-ip": "198.51.100.1" }), null);
});

console.log(`${checks} client IP checks passed`);
