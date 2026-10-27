# Billy Bishop control plane

The only active scheduler is the `runner-control` Docker container on the Billy Bishop VPS. Its port is published at `127.0.0.1:8920`; the existing Cloudflare Tunnel exposes `runners.billybishop4-workers.xyz` and `control.billybishop4-workers.xyz`. Cloudflare continues to provide tunnels and DNS for the fleet, but no Worker, Durable Object, D1 or Mac is required to run the control plane. New owners can use the generic [self-hosting setup](self-hosting.md).

## State and services

- SQLite: `/home/claude/runner-control/control.db`, containing only app specifications, source versions, app secrets/passwords and operator settings (five tables). Existing source history is retained for rollback.
- Runtime tables (runs, placements, starts, agent inventory, health, metrics, events and geolocation) use SQLite TEMP tables with `temp_store=MEMORY`. Runtime settings such as cooldowns and metric cursors also stay in RAM. They are never copied back to disk at shutdown.
- Tunnel mappings and tokens also use an in-memory TEMP table. On every startup the controller lists Cloudflare tunnels, selects exact `runner-<slot>` names, and publishes the mappings only after all pages succeed. Unrelated and deleted tunnels are ignored; duplicate slot names prevent recovery. Connection tokens are fetched only when a machine joins and are kept in RAM until restart. Existing agents retain their own connection credentials.
- Controller credentials and settings: `/home/claude/.config/runner-control/docker.env`, readable only by its owner. The existing join token, admin password and receipt signing key are supplied through this private file. No new identity is generated for the migrated fleet. The previous `env` file is retained for rollback.
- Docker Compose: `/home/claude/runner-control/docker/compose.yaml`, project `runner-control`. It mounts `/home/claude/runner-control` at `/data`, uses an image tagged by its source commit, runs as UID 1000, and restarts unless explicitly stopped. `AUTO_TUNNEL=0` keeps the existing public tunnel; `TUNNEL_PREFIX=runner` retains the fleet's existing tunnel identities. `REQUIRE_DEPLOY_AUTH=0` preserves this fleet's existing public-deployment policy.
- Services: the old `runner-control.service` is disabled and stopped. `runner-control-tunnel.service` remains enabled under the `claude` systemd user manager, with user lingering enabled. Docker is enabled at boot. Previous source releases and the old service definition are retained for rollback.

From root over SSH, service commands use:

```sh
docker compose -p runner-control -f /home/claude/runner-control/docker/compose.yaml ps
runuser -u claude -- env XDG_RUNTIME_DIR=/run/user/1000 systemctl --user show runner-control-tunnel.service -p ActiveState -p SubState
```

`/api/status` identifies `controlPlane.owner` as `vps`, `mode` as `vps-v1`, `runtimeStorage` and `tunnelStorage` as `memory`, and the controller as Billy Bishop. It also reports tunnel discovery readiness/errors, agent WebSocket connections, the recovery gate and the metrics retention window. The dashboard displays Billy Bishop controller.

The server uses Node 24 and retains support for WebSocket agents, signed recovery receipts, and region stickiness. Every startup validates configuration before listening, then waits three minutes for agent inventory and receipts before changing placements. Existing containers keep running during this window. Saved receipts reconstruct placements and handovers; fresh agent reports supply current health. A receipt cannot recreate a deleted app or overwrite current operator settings. After recovery, the scheduler removes deleted/disabled/excess copies, applies current source versions and fills missing replicas. An agent with workloads but no recoverable receipt holds recovery until a witness supplies its placement information or the agent stops checking in.

Desired configuration is authoritative. A missing database, missing configuration table, corrupt database, or missing referenced source version prevents startup instead of sending empty plans. `INIT_CONTROL_DB=1` is reserved for initializing a new fleet and must not be set in the production service. `STATE_SIGNING_KEY` and `JOIN_TOKEN` are required and must survive restarts.

Tunnel discovery is an additional recovery gate. Until the Cloudflare listing succeeds, joins, scheduling, slot retirement and DNS changes wait, even after the agent inventory window expires. Agents continue reporting and retain their existing containers. Cloudflare requests time out after 30 seconds; discovery failures retry after one minute. A restart reuses existing tunnel identities and does not recreate their tunnels or eagerly fetch every token.

Configuration changes use WAL with `synchronous=FULL`; multi-statement app/settings edits commit before acknowledgement. Settings are persisted only when their durable values change. Heartbeats, health, metrics and ordinary placement changes do not commit to `control.db`. A storage failure stops further plans and restarts the controller. Metrics remain cached for six hours by default (`MEMORY_HISTORY_MIN=360`), events for six hours; controller history resets on restart, while agents retain their own recent metrics and receipts. GDrive remains deployed on runners with its WARP sidecar and one replica in each of the three fleet regions.

## Migration and recovery

The Cloudflare controller was disabled before importing its final configuration and live scheduling tables into the VPS database. A private backup of the prior VPS database and exported cloud state is kept under `/home/claude/.config/runner-hybrid/vps-return/`. The existing proxy DNS names continue to point to the VPS tunnel; their intercepting Worker routes are removed after the VPS is ready.

Back up SQLite with its online backup API, and back up the private controller and tunnel environment files separately. Preserve file permissions and the signing key. To restore, stop the container with Docker Compose, restore its database and environment, then start it again. Verify the recovery gate clears and agents reconnect before making placement changes. Never run the old systemd scheduler alongside the container; stop the container before rolling back to that service.

When an older database still contains runtime tables, the startup migration makes a private, consistent full backup under `/home/claude/runner-control/backups/before-memory-*.db`, removes those tables/settings from the active database and reclaims their pages. This includes removing the old persistent `slots` table; tunnel IDs and tokens are reloaded from Cloudflare as needed. App specs, source versions, app secrets and operator settings are preserved. Subsequent startups do not repeat the migration. Offline SQLite backups now contain configuration only; historical migration backups retain the previous data. Runtime `/api/export` queries still expose the process's live temporary tables to an authenticated administrator.

The Mac LaunchAgent remains disabled. After successful VPS recovery, `runner-control-hybrid`, its `FleetCoordinator` Durable Object namespace and the `runners-config` D1 database were deleted. Other Workers, including GDrive2, remain active. Never enable a second scheduler alongside the VPS. The historical [Cloudflare document](hybrid-control.md) and implementation remain for reference.
