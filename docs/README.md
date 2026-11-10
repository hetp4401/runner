# Documentation

Start with the [requirements](requirements.md) and [self-hosting guide](self-hosting.md) to bring up a new fleet. All examples use domains and credentials that you replace with your own.

| Guide | Contents |
| --- | --- |
| [Control plane module](../control/README.md) | Responsibilities, setup, environment variables, operation, source map and development. |
| [Agent module](../agent/README.md) | Host requirements, installer, environment variables, upgrades, data, removal and source map. |
| [Requirements](requirements.md) | Software, permissions, Cloudflare scopes, networking, storage and preflight checks. |
| [Architecture](architecture.md) | Web app, controller, agents, storage, networking and recovery. |
| [Mac Docker operations](mac-control.md) | Colima, startup supervision, persistence and migration. |
| [Provider adapters](providers.md) | Independent GitHub capacity, multiple accounts and watchdog fallback. |
| [Self-hosting](self-hosting.md) | Initialize a controller, connect hosts, deploy an app and configure a reverse proxy. |
| [VPS operations](vps-control.md) | Service checks, persistence, upgrades and migration. |
| [Native Mac control](native-control.md) | Run the same controller under launchd, migrate a fleet, and verify crash recovery. |
| [Region affinity](region-affinity.md) | Custom labels and regional replica placement. |
| [Public addresses](public-addresses.md) | Automatic global and replica URLs, failover and upgrades. |
| [Root reference](../README.md#projects) | Workload definitions, placement, authentication, API, CLI and recovery architecture. |
| [Tests](../control/test/README.md) | Simulations, local integration and Linux Docker checks. |

The active modules are `control/server.mjs` with `control/src/local-store.mjs`, and `agent/agent.mjs` with its local runtime helpers. Build the controller and agent images with the repository root as build context so their shared release packaging is included.
