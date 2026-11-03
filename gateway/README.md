# Required system router

This generic routing module is bundled with the control plane. It is required platform infrastructure, deployed as three protected workloads on separate agents. Do not deploy or manage it as a user app. Starting the controller provisions its shared tunnel, provides connection credentials privately and reconciles its bundled source automatically.

All public apps receive a global hostname through this router plus their existing numbered hostnames. The router discovers apps and replica health from the controller, forwards the same request path/query, supports HTTP streaming and WebSockets, and retains its last known routing during controller outages. No app or machine provider is hardcoded.

GET file streams with byte ranges, known lengths and strong matching ETags can continue on another replica after an upstream disconnect or stall. Unsafe continuations are rejected; writes are forwarded once. A router-process failure still requires the client to reconnect and resume. See [public addresses](../docs/public-addresses.md) for complete failure behavior.

The dedicated Routing page provides health, traffic counters and resource charts. `/api/routing/metrics` aggregates sanitized counters from each router; the process-local `/metrics` endpoint is served through the router's own numbered hostname. Counters reset when that router process restarts. No request paths, tokens or client headers are included.

The module uses Node.js 24 built-ins and cloudflared from its Docker image, with no npm dependencies. The controller packages the Dockerfile, source and Compose definition into each managed plan; agents build and execute it through their ordinary workload runtime.

Run from the repository root:

```sh
node --test gateway/test/*.test.mjs
node control/test/sim/global-domains.test.mjs
```
