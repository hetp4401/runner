# Billy Bishop control plane

The only active scheduler is `control/server.mjs` on the Billy Bishop VPS. It serves `127.0.0.1:8920`; the existing Cloudflare Tunnel exposes `runners.billybishop4-workers.xyz` and `control.billybishop4-workers.xyz`. Cloudflare continues to provide tunnels and DNS for the fleet, but no Worker, Durable Object, D1 or Mac is required to run the control plane.

## State and services

- SQLite: `/home/claude/runner-control/control.db`, containing only app specifications, source versions, app secrets/passwords, operator settings and tunnel credentials. Existing source history is retained for rollback.
- Runtime tables (runs, placements, starts, agent inventory, health, metrics, events and geolocation) use SQLite TEMP tables with `temp_store=MEMORY`. Runtime settings such as cooldowns and metric cursors also stay in RAM. They are never copied back to disk at shutdown.
- Controller credentials and settings: `/home/claude/.config/runner-control/env`, readable only by its owner. The receipt signing key stays consistent with the fleet's existing receipts.
- Releases: `/home/claude/runner-control/releases/`, selected by the systemd service override at `~/.config/systemd/user/runner-control.service.d/vps-release.conf`. The older working checkout is preserved.
- Services: `runner-control.service` and `runner-control-tunnel.service` under the `claude` systemd user manager. Both are enabled, with user lingering enabled for boot startup.

From root over SSH, service commands use:

```sh
runuser -u claude -- env XDG_RUNTIME_DIR=/run/user/1000 systemctl --user status runner-control.service runner-control-tunnel.service
```

`/api/status` identifies `controlPlane.owner` as `vps`, `mode` as `vps-v1`, `runtimeStorage` as `memory`, and the controller as Billy Bishop. It also reports agent WebSocket connections, the recovery gate and the metrics retention window. The dashboard displays Billy Bishop controller.

The server uses Node 24 and retains support for WebSocket agents, signed recovery receipts, and region stickiness. Every startup validates configuration before listening, then waits three minutes for agent inventory and receipts before changing placements. Existing containers keep running during this window. Saved receipts reconstruct placements and handovers; fresh agent reports supply current health. A receipt cannot recreate a deleted app or overwrite current operator settings. After recovery, the scheduler removes deleted/disabled/excess copies, applies current source versions and fills missing replicas. An agent with workloads but no recoverable receipt holds recovery until a witness supplies its placement information or the agent stops checking in.

Desired configuration is authoritative. A missing database, missing configuration table, corrupt database, or missing referenced source version prevents startup instead of sending empty plans. `INIT_CONTROL_DB=1` is reserved for initializing a new fleet and must not be set in the production service. `STATE_SIGNING_KEY` and `JOIN_TOKEN` are required and must survive restarts.

Configuration changes use WAL with `synchronous=FULL`; multi-statement app/settings edits commit before acknowledgement. Settings are persisted only when their durable values change. Heartbeats, health, metrics and ordinary placement changes do not commit to `control.db`. A storage failure stops further plans and restarts the controller. Metrics remain cached for six hours by default (`MEMORY_HISTORY_MIN=360`), events for six hours; controller history resets on restart, while agents retain their own recent metrics and receipts. GDrive remains deployed on runners with its WARP sidecar and one replica in each of the three fleet regions.

## Migration and recovery

The Cloudflare controller was disabled before importing its final configuration and live scheduling tables into the VPS database. A private backup of the prior VPS database and exported cloud state is kept under `/home/claude/.config/runner-hybrid/vps-return/`. The existing proxy DNS names continue to point to the VPS tunnel; their intercepting Worker routes are removed after the VPS is ready.

Back up SQLite with its online backup API, and back up the private controller and tunnel environment files separately. Preserve file permissions and the signing key. To restore, stop the controller, restore its database and environment, then start the controller and tunnel. Verify the recovery gate clears and agents reconnect before making placement changes.

The first startup of the memory-only runtime makes a private, consistent full backup under `/home/claude/runner-control/backups/before-memory-*.db`, removes legacy runtime tables/settings from the active database and reclaims their pages. It preserves all app specs, source versions and credentials. Subsequent startups do not repeat the migration. Offline SQLite backups now contain configuration only; runtime `/api/export` queries still expose the process's live temporary tables to an authenticated administrator.

The Mac LaunchAgent remains disabled. After successful VPS recovery, `runner-control-hybrid`, its `FleetCoordinator` Durable Object namespace and the `runners-config` D1 database were deleted. Other Workers, including GDrive2, remain active. Never enable a second scheduler alongside the VPS. The historical [Cloudflare document](hybrid-control.md) and implementation remain for reference.
