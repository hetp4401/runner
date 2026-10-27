# Billy Bishop control plane

The only active scheduler is `control/server.mjs` on the Billy Bishop VPS. It serves `127.0.0.1:8920`; the existing Cloudflare Tunnel exposes `runners.billybishop4-workers.xyz` and `control.billybishop4-workers.xyz`. Cloudflare continues to provide tunnels and DNS for the fleet, but no Worker, Durable Object, D1 or Mac is required to run the control plane.

## State and services

- SQLite: `/home/claude/runner-control/control.db`, including app specifications, source versions, app secrets, tunnel credentials and scheduling state.
- Controller credentials and settings: `/home/claude/.config/runner-control/env`, readable only by its owner. The receipt signing key stays consistent with the fleet's existing receipts.
- Active release: `/home/claude/runner-control/releases/20261026-vps-return/control`, selected by the systemd service override. The older working checkout is preserved.
- Services: `runner-control.service` and `runner-control-tunnel.service` under the `claude` systemd user manager. Both are enabled, with user lingering enabled for boot startup.

From root over SSH, service commands use:

```sh
runuser -u claude -- env XDG_RUNTIME_DIR=/run/user/1000 systemctl --user status runner-control.service runner-control-tunnel.service
```

`/api/status` identifies `controlPlane.owner` as `vps`, `mode` as `vps-v1`, and the controller as Billy Bishop. It also reports agent WebSocket connections and the recovery gate. The dashboard displays Billy Bishop controller.

The server uses Node 24 and retains support for WebSocket agents, signed recovery receipts, and region stickiness. Startup waits three minutes for agent inventory and receipts before changing placements. Existing containers keep running during this window. GDrive remains deployed on runners with its WARP sidecar and one replica in each of the three fleet regions.

## Migration and recovery

The Cloudflare controller was disabled before importing its final configuration and live scheduling tables into the VPS database. A private backup of the prior VPS database and exported cloud state is kept under `/home/claude/.config/runner-hybrid/vps-return/`. The existing proxy DNS names continue to point to the VPS tunnel; their intercepting Worker routes are removed after the VPS is ready.

Back up SQLite with its online backup API, and back up the private controller and tunnel environment files separately. Preserve file permissions and the signing key. To restore, stop the controller, restore its database and environment, then start the controller and tunnel. Verify the recovery gate clears and agents reconnect before making placement changes.

The Mac LaunchAgent remains disabled. After successful VPS recovery, `runner-control-hybrid`, its `FleetCoordinator` Durable Object namespace and the `runners-config` D1 database were deleted. Other Workers, including GDrive2, remain active. Never enable a second scheduler alongside the VPS. The historical [Cloudflare document](hybrid-control.md) and implementation remain for reference.
