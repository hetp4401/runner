# Architecture

Runners is a web app and API backed by one controller and a fleet of Linux agents.
The controller can run in Docker on a Linux server or on a Mac through Colima.
Application containers run on the agent hosts.

```mermaid
flowchart LR
  Browser[Dashboard / runnerctl] --> CF[Cloudflare]
  CF --> Tunnel[Controller cloudflared]
  subgraph Controller container
    Tunnel --> CP[Node HTTP API and scheduler]
    CP --> DB[(SQLite /data)]
  end
  Providers[Provider adapters / GitHub workflows] --> Hosts[Linux agent hosts]
  Hosts <-->|outgoing authenticated WebSocket| CF
  Hosts --> Apps[Docker Compose apps]
  CF <-->|app tunnels| Apps
```

## Dashboard and controller

`control/src/app.html` is served by `control/server.mjs`. The same process exposes
the HTTP API, authenticated agent WebSockets, metrics and live-log forwarding.
`control/src/index.js` validates desired app configuration and reconciles it with
reports from the agents. It places replicas, manages moves and handovers, and
requests Cloudflare DNS/tunnel resources. Public app traffic travels directly
through Cloudflare to the app hosts; it does not pass through the controller.

The Docker image bundles Node 24, controller dependencies, cloudflared and the
matching downloadable agent release. Its entrypoint supervises the server and
connector together. If either exits, the container exits so Docker can restart
it. It runs as UID 1000, without the Docker socket or elevated capabilities.
A lock prevents concurrent controllers using the same Linux data volume.

On a Mac, `control/mac-docker.py` runs under launchd and starts the dedicated
Colima engine and Compose service. Docker owns the Node/connector processes.
The wrapper holds an AC-power idle-sleep assertion and exports daily backups.
The [Mac runbook](mac-control.md) covers service operation and availability.

## State and recovery

`control/src/local-store.mjs` persists five configuration tables in SQLite:
`projects`, `versions`, `settings`, `app_passwords` and `app_env`. They contain the
authoritative app definitions, source history, operator settings and secrets.
`identity.json` and the private controller environment preserve fleet identity
and Cloudflare access. Back up all of these together.

Machine reports, scheduling observations, metrics and tunnel credentials remain
in controller memory. Agents retain their approved plans, inventory and signed
recovery receipts locally. After a restart the controller discovers tunnels and
waits for agent reports before scheduling. Receipts recover observations; they
cannot reconstruct missing app source, password hashes or application secrets.
The default recovery hold is three minutes.

Run exactly one controller for a fleet. A local volume lock cannot fence another
computer or another volume. Before migration, disable the old scheduler and its
automatic startup, preserve the public origin, join token, signing key and tunnel
namespace, and transfer a consistent SQLite backup. Never initialize a new empty
fleet as an outage-recovery shortcut.

## Agents and providers

`agent/agent.mjs` applies controller-approved plans to Docker Compose on a Linux
host. Its helpers manage container ownership, router/tunnel connections, metrics,
stable local data paths and verified agent releases. Host failure, application
readiness and replacement capacity are distinct signals. Agents can continue
serving approved workloads while the controller is unavailable.

The controller accepts supplied capacity and the agent reports an origin tag.
Neither contains GitHub account lists or desired machine counts. Provider
adapters own those targets and create machines. GitHub's workflows and optional
watchdog fallback are documented in [provider adapters](providers.md).

App data stays on the machine unless the app implements replication or shared
storage. Scheduling another replica does not copy an application's volumes.

## Access and release boundaries

The dashboard, status, metrics and app source are public. App environment values,
logs, diagnostics and administrative actions require their relevant credentials.
New fleets require deployment authentication; existing fleets retain their
configured policy. Only the controller holds Cloudflare management credentials.
Agents receive fleet credentials and scoped tunnel connection tokens.

Build images from the repository root so `control/` and `agent/` stay together.
Updating a controller image does not automatically restart all application
containers. Agent upgrades are an explicit roll or provider job replacement;
ordinary agent restarts use verified local code. See [controller](../control/README.md),
[agent](../agent/README.md) and [tests](../control/test/README.md).
