# Public addresses

Every app with a public HTTP port automatically receives both address types:

| Address | Destination |
| --- | --- |
| `https://hello.example.com/` | A ready hello replica through the app's shared tunnel. |
| `https://hello-1.example.com/` | Replica 1 through its assigned machine tunnel. |
| `https://hello-2.example.com/` | Replica 2 when requested. |

Replace `example.com` with the fleet's `DOMAIN`. Both types are mandatory, including for single-replica apps. The dashboard uses the global URL as the main app link and retains numbered links in replica chips and tables. The deployment form previews both. JSON `routing` and `x-runner.routing` are rejected; there is no routing toggle or CLI flag. Background jobs without a public port have no public URL.

Descriptions expose the derived `globalUrl`. Scaling, source updates, handovers and rollback keep that hostname. Scaling down removes the highest numbered addresses. Apps support up to 50 replicas.

## Direct shared tunnels

The controller creates one shared Cloudflare tunnel per public app. Agents hosting that app connect the same tunnel to their local Caddy proxy. A connector opens only when a local app copy is healthy and ready. Numbered requests select their own replica; the global hostname selects a ready local copy, including when ports were remapped or multiple copies share one host.

```text
Global URL -> Cloudflare app tunnel -> agent's local proxy -> ready local app copy
Numbered URL -> Cloudflare machine tunnel -> agent's local proxy -> specified copy
```

These connectors are agent infrastructure outside app Compose stacks. They are not apps in the dashboard. There is no global-router workload, separate Routing section or intermediate fleet-wide gateway. The controller VPS handles management traffic only. Paths, queries, request bodies, authorization and streaming/WebSocket traffic use the existing local reverse proxy.

Cloudflare supports [25 active connectors per tunnel](https://developers.cloudflare.com/cloudflare-one/account-limits/). The controller selects up to 20 distinct replica hosts for an app's shared tunnel, leaving headroom for overlapping host handovers. All replicas keep numbered URLs, including copies beyond those 20 hosts. Colocated copies share one local connector. Cloudflare chooses an available connector; this is not a promise of even traffic distribution.

## Failure and playback

The agent checks the app's readiness path, or `/` when none is configured, through the local proxy every two seconds. Two failed probes withdraw its app connector. An unready, removed or disabled local app withdraws immediately on reconciliation. Surviving app connectors serve new requests under the same global hostname without waiting for a DNS change. A failed connector is restarted when the app recovers. Numbered domains continue to select their own replica.

An in-progress HTTP response or WebSocket cannot transfer its connection to another host. A player must reconnect and, for progressive media, request the next byte range. Buffered playback can cover a brief interruption when recovery finishes before the buffer runs out. This does not guarantee uninterrupted playback for every browser, player, media format or outage duration. Every replica must serve identical media bytes and support consistent range responses. Shared application state, files and sessions remain the application's responsibility.

The agent keeps its approved tunnel credentials and workload plans in private host state. During a controller outage it retains working connectors, checks local health and repairs authorized workloads. Controller restarts discover tunnel identities from Cloudflare and fetch current tokens into memory; tokens are not saved in the controller's configuration database or exposed in public status, source, receipts or app environment files. Only an agent assigned a public copy receives that app's token.

## DNS lifecycle and upgrades

Global CNAMEs point at their app tunnel; numbered CNAMEs point at machine tunnels. Existing records already using an owned target are adopted with the fleet marker. Conflicting outside records are preserved and reported. Unmarked aliases outside the app address are preserved even if they share its tunnel.

Deleting an app removes its global and numbered records and revokes connector authorization. The controller removes an unused tunnel after its connectors withdraw and no DNS records still reference it. It retries deletion rather than forcibly disconnecting a late connector or deleting an outside alias.

For a new fleet, install the current controller and agent normally. Protocol 5 agents support shared app connectors. Preserve `TUNNEL_PREFIX`, the controller identity and the data directory through upgrades; new app tunnel names are derived from the saved namespace and app name.

For a deployment using the retired global router, verify the current app definitions, private environment, identity and DNS before upgrading. Upgrade through preparation release `1f6423c`, which keeps the old ingress serving while protocol 5 agents connect app tunnels. Wait until every public app's global CNAME points at its app tunnel and the expected replica hosts report connected. Then install the current release, delete the old `global-router` project through the authenticated app API, and wait for its containers and numbered records to disappear. Delete the obsolete shared routing tunnel only after verifying that no DNS records reference it, and remove the obsolete `GLOBAL_TUNNEL_NAME` setting from the environment and saved identity. Do not stop the old ingress before app routes are ready.

`runners`, `control` and the configured controller hostname are reserved. App names cannot end in `-<number>` or `-m<number>`, which are reserved for numbered addresses.
