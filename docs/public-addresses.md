# Public app addresses

Every app with a public port automatically receives both kinds of address:

| Address | Behavior |
| --- | --- |
| `https://hello.example.com/` | Global app URL. Follows a healthy copy. |
| `https://hello-1.example.com/` | Replica 1, retaining its number across moves. |
| `https://hello-2.example.com/` | Replica 2, when the app requests at least two replicas. |

Replace `example.com` with the controller's `DOMAIN`. The dashboard uses the global URL as the main app link and retains numbered links in replica chips and the replica table. The deployment form previews both address types. Both are always provided; there is no routing checkbox or CLI flag. JSON `routing` and `x-runner.routing` are rejected. Background jobs without a public port have no public URL.

Project descriptions expose the derived `globalUrl` field in deployment responses, project reads, status and metrics. Scaling, source updates and rollback retain the global hostname. Scaling down removes the highest numbered replica addresses.

## Routing

The controller creates proxied CNAME records using its existing machine tunnels. A global address points to a machine with a healthy, ready copy. Its router forwards directly to the lowest numbered ready local copy, using its actual mapped port. A numbered address always selects its specified replica. No extra tunnel, connector or app secret is needed; the limit remains 50 replicas.

Global requests preserve the public hostname, HTTPS forwarding headers, method, path, query and body. Streaming and WebSocket upgrades work at both address types. App traffic stays on agents; the controller does not proxy it.

The global address selects a serving copy; it does not promise equal traffic distribution or replicate application sessions. When a selected copy becomes unavailable, the controller repoints the global record to another healthy copy. Agent reporting, DNS synchronization and Cloudflare propagation introduce a failover delay. Clients should reconnect streams and WebSockets after disconnection.

Readiness changes update the local global route without restarting healthy apps. Without a ready local copy, that machine answers 503 for the global address while the controller selects another target. Existing routers, DNS and workloads keep serving during a controller outage. Reconciliation retains existing records when no healthy destination is known, including during cold inventory recovery.

## Upgrades and existing DNS

Upgrade the control plane and roll agents through the Fleet page or `bin/runnerctl roll`. Protocol 4 agents support global hosts. The controller waits for older live connectors sharing a slot to leave before sending global traffic there, preventing requests reaching a replica-only router during an upgrade.

The controller changes only records pointing to its known machine tunnels. An external record occupying an app address is preserved and reported as a DNS conflict. To migrate an existing shared gateway, back up its DNS, wait for healthy upgraded agents, then explicitly repoint only the intended app CNAMEs to the controller's selected tunnels. Preserve unrelated records and tunnel resources.

The `runners`, `control` and configured controller hostname labels are reserved. App names cannot end in `-<number>` or `-m<number>`, which are reserved for numbered addresses. App hostnames must be available in the fleet's domain.
