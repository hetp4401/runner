# Agent module

The agent manages Docker workloads on one Linux host. It connects to the control plane, receives a desired plan, builds and runs each assigned Compose project, maintains the local router and tunnel, and reports inventory, health and metrics.

Run one agent per Docker host. The controller chooses placements; the agent applies its approved plan locally and can keep existing workloads running while the controller is unreachable.

## Requirements

- A Linux Docker host with a running Docker Engine, the default `/var/run/docker.sock`, and permission to manage it.
- Root access for the installer, plus `curl`, `tar`, `sha256sum` and `flock`.
- Persistent host storage, defaulting to `/var/lib/runner`, and enough CPU, memory and disk for assigned workloads and image builds.
- The controller's HTTPS URL, its fleet join token, and an origin tag such as `vps`, `lab` or `standalone`.
- Outbound HTTPS to the controller and registries, plus Cloudflare Tunnel connectivity.

The installer builds an image containing Node and Docker Compose. You do not need Node or npm on the host for this installation, and the agent source uses Node built-ins without npm packages. Direct source execution uses Node.js 24 and Docker Compose v2 on Linux. See [requirements](../docs/requirements.md) for network ports and host checks.

## Install

Get the join token from the controller's Fleet page or from its private credentials command. Substitute your controller URL and token below. On the Linux host:

```sh
curl -fsSL https://runners.example.com/install.sh |
  sudo JOIN_TOKEN='your-join-token' SERVER_TAG=standalone LABEL='my-server' sh
```

For explicit placement labels, include a region:

```sh
curl -fsSL https://runners.example.com/install.sh |
  sudo JOIN_TOKEN='your-join-token' SERVER_TAG=vps LABEL='web-01' REGION=eu-west sh
```

The installer verifies the controller's agent bundle, builds a local image before replacing a previous installation, and starts `runner-agent` with host networking, the Docker socket and its persistent host directory. Subsequent restarts use local code. The controller URL is taken from the installer endpoint; you do not pass `CONTROL_URL` separately in these commands.

Check the container and the controller's Machines page:

```sh
sudo docker ps --filter name=runner-agent
sudo docker logs --tail 100 -f runner-agent
```

The agent receives a slot and a tunnel connection token. Cloudflare's management API credential stays on the controller.

## Configuration

| Setting | Purpose |
| --- | --- |
| `CONTROL_URL` | Required controller origin for direct execution. Set automatically by the installer. Keep it stable. |
| `JOIN_TOKEN` | Required fleet join token. |
| `SERVER_TAG` | Required origin label, 1–30 lowercase letters, digits or dashes, beginning with a letter or digit. |
| `LABEL` | Display name; the installer defaults to the host's name. |
| `REGION` | Optional placement label, 1–32 lowercase letters, digits or dashes, beginning with a letter. |
| `RUNNER_DATA` | Persistent absolute host path; defaults to `/var/lib/runner`. |
| `MACHINE` | Optional requested slot during a controller-approved replacement. Ordinary joins should omit it. |
| `ROUTER_PORT` | Local router port; defaults to `19080`. The next two ports are also reserved for infrastructure. |
| `START_CMD` | Optional replacement command for direct execution. Receives the requested slot as `$SLOT`. |
| `LIFETIME_MIN` | Optional known host lifetime for direct execution. Omit for a host without a deadline. |

The installer forwards `SERVER_TAG`, `LABEL`, `REGION`, `RUNNER_DATA`, `MACHINE` and `ROUTER_PORT` into the container. It does not forward `START_CMD` or `LIFETIME_MIN`; those are integration settings for a process started by your own supervisor. `POOL` is a legacy tag alias and `POOL_SIZE` has no effect. Tags describe connected hosts and do not request a server count.

To change installer-supported settings, rerun the install command with the same controller URL and data path. The saved identity keeps the host's slot. See [region affinity](../docs/region-affinity.md) for app placement constraints.

## Upgrade and restart

First update the controller checkout and rebuild its image so it serves the new agent release. Request a roll through the Fleet page or, from the repository root on your operator machine:

```sh
export RUNNER_URL=https://runners.example.com
export ADMIN_PASSWORD='your-admin-password'
bin/runnerctl roll
```

An explicit roll verifies and stages a release before restarting. A failed download leaves the running release in place. Reinstall on each host to refresh the base image's Node and Docker tools as well as its agent code. A normal container restart uses its verified local code:

```sh
sudo docker restart runner-agent
```

A restart can interrupt that host's connections. Use controller-managed moves before planned host maintenance when other hosts have capacity.

## Persistent data and removal

The data directory holds agent identity, inventory, approved plans, signed receipts, local tunnel credentials, cached releases, recent metrics and app data. Keep it private and preserve it through reinstallations. Changing the controller URL or reusing another fleet's data directory requires a coordinated migration.

App volumes and bind-mounted data remain on the host. Moving or adding replicas does not copy that data to another host; provide application-level shared storage or replication where needed.

To remove a host:

1. Use **Move apps off** in the dashboard, or `bin/runnerctl evict <slot>` from the operator machine. Wait for healthy replacement copies and an empty slot.
2. Stop the agent with `sudo docker stop runner-agent` and remove its container when ready.
3. Remove only the fleet-owned router and tunnel containers. Ownership uses `io.runners.owner`; infrastructure also has `io.runners.component=router` or `tunnel`. Preserve unrelated containers and any data you still need.
4. Retire the empty slot in the dashboard or with `bin/runnerctl retire <slot>` to remove its tunnel and DNS records.

## Source and verification

| File | Responsibility |
| --- | --- |
| [`agent.mjs`](agent.mjs) | Main loop, inventory, builds, workload reconciliation and local repair. |
| [`runtime.mjs`](runtime.mjs) | Container ownership, safe files, identity recovery and verified code caching. |
| [`control-link.mjs`](control-link.mjs) | Bidirectional reports and plans. |
| [`ws.mjs`](ws.mjs) | WebSocket client. |
| [`metrics.mjs`](metrics.mjs) | Host and per-replica metrics. |
| [`../control/agent-launcher.mjs`](../control/agent-launcher.mjs) | Verified local release selection. |
| [`../control/agent.Dockerfile`](../control/agent.Dockerfile) | Agent image. Build with the repository root as context. |

With Node.js 24, run these checks from the repository root:

```sh
node control/test/agent-runtime.mjs
node control/test/agent-launcher.mjs
node control/test/install-script.mjs
```

The runtime checks skip Linux-only filesystem cases on other systems. Use the Linux Docker integration tests in the [test reference](../control/test/README.md) to verify real workload execution.

If a host does not join, inspect `runner-agent` logs, verify the controller URL/token/tag, and check network access. If apps wait for placement, inspect the controller's status and the app's region and capacity constraints. If the router cannot start, check the reserved local ports before changing `ROUTER_PORT`.
