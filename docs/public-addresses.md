# Public addresses and system routing

Every app with a public port automatically receives both address types:

| Address | Destination |
| --- | --- |
| `https://hello.example.com/` | Required global router, then a live replica of hello. |
| `https://hello-1.example.com/` | Replica 1 through its assigned agent tunnel. |
| `https://hello-2.example.com/` | Replica 2 when requested. |

Replace `example.com` with the fleet's `DOMAIN`. Both types are mandatory, including for single-replica apps. The dashboard uses the global URL as the main link and retains numbered links in replica chips and tables. The deployment form previews both. JSON `routing` and `x-runner.routing` are rejected; there is no routing toggle or CLI flag. Background jobs without a public port have no public URL.

Project descriptions expose the derived `globalUrl`. Scaling, source updates, handovers and rollback retain that hostname. Scaling down removes the highest numbered replica addresses. The existing limit of 50 app replicas remains.

## Required global router

Global routing is bundled system infrastructure. Starting the controller provisions or reuses one dedicated routing tunnel and ensures three protected router workloads on separate agent hosts. Each includes the routing process and a tunnel connector. No manual app deployment, app secret or additional management credential is needed. Router tunnel tokens stay private and are sent only to agents assigned a router copy.

The router is excluded from the regular Apps list. The dedicated **Routing** section shows its replica health, requests per second, bandwidth, active requests, WebSockets, failover and stream-resume counters, plus CPU, memory, disk and network charts. Traffic counters are per router process and reset on restart. Metrics are public and contain no request URLs, cookies, headers or tokens.

App APIs cannot edit, scale, disable, delete or change the environment/password of this system workload, including with the admin password. Updating the controller's bundled routing source upgrades router copies one at a time. Agents restart failed containers; the controller replenishes missing placements. Three healthy agent hosts are needed for full redundancy. With fewer hosts, available copies serve and remaining router copies wait for capacity. The controller VPS carries management traffic only.

```text
Global URL -> Cloudflare shared tunnel -> system router -> app replica URL -> agent -> app
Numbered URL -> Cloudflare machine tunnel -> agent -> specified replica
```

The router keeps the hostname, path, query, method, authorization, cookies, forwarding headers, streaming and WebSocket behavior. It discovers apps from public controller status and retains its last valid routes during a controller outage. Safe requests can fail over to another app replica; writes are forwarded once to avoid duplicate operations. Global routing does not replicate application data or sessions.

## Interrupted streams

For a GET response supporting byte ranges, a strong ETag, a known length and identity encoding, the router can keep the client response open when an app replica disconnects or stalls. It forwards the same path and query to another replica with the next byte offset. The replacement must return matching partial-content boundaries, file size and ETag. Bytes are streamed with backpressure; the router does not store the entire file.

A changed file, unsupported range response, weak/missing validator, compressed response or exhausted replicas ends the request rather than joining potentially different content. Retry attempts and time spent recovering are bounded. Clients must support reconnect/resume for other streams and for failure of the router carrying their connection. An established WebSocket cannot transfer to a different router or app process.

If one router or host fails, surviving connectors can serve new requests while recovery proceeds. If all router copies are unavailable, global addresses are unavailable until recovery. Numbered replica addresses remain independent. Application state required across replicas must live in shared or replicated storage.

## DNS and migration

The controller manages proxied global CNAMEs through its system routing tunnel and numbered CNAMEs through machine tunnels. Existing CNAMEs already pointing to the required ingress are adopted with the fleet's ownership marker. External records occupying an app address are preserved and reported as conflicts. Unmarked aliases outside the fleet are preserved even when they use the same ingress tunnel.

For an existing deployment, back up the database, private environment and DNS before upgrading. Preserve `TUNNEL_PREFIX` and the exact existing routing tunnel name with `GLOBAL_TUNNEL_NAME`; the saved identity keeps these names stable on subsequent starts. This namespace setting never disables routing. The controller retrieves the tunnel token from Cloudflare at startup; it does not persist it in the configuration database.

`global-router`, `runners`, `control` and the configured controller hostname are reserved. App names cannot end in `-<number>` or `-m<number>`, which are reserved for numbered addresses.
