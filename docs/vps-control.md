# VPS control plane operations

Run the control plane on a VPS or another server with Docker Engine, Compose v2 and persistent disk. The same package works with hosts from different providers. Start with the [control plane module](../control/README.md), [requirements](requirements.md) and [self-hosting guide](self-hosting.md) for a new installation.

## Service layout

Keep the checkout in a stable directory of your choice, such as `/srv/runner`. Run the commands below from that directory so Compose keeps using the same project and data volume.

| Component | Location or endpoint |
| --- | --- |
| Controller service | `control` in the root `compose.yaml`. |
| Private configuration | `.env` in the checkout; readable only by its owner. |
| Persistent data | Compose volume `control-data`, mounted at `/data` in the controller. |
| Desired configuration | `/data/control.db`. |
| Fleet identity | `/data/identity.json`, plus credentials supplied through `.env`. |
| Local HTTP endpoint | `127.0.0.1:8920`, or the host port chosen by `CONTROL_PORT`. |
| Public endpoint | `https://runners.example.com`, replacing the domain with your own. |

The controller process schedules workloads and manages DNS/tunnels. Agent hosts run the actual applications. The controller container runs as UID 1000 and has no Docker socket mount. Its restart policy starts it after the Docker daemon returns; configure Docker to start at host boot using your operating system's service manager.

## Inspect and restart

```sh
docker compose ps
docker compose logs --tail 100 control
curl -fsS http://127.0.0.1:8920/healthz
curl -fsS https://runners.example.com/api/status
docker compose restart control
```

Adjust the URLs for your domain and `CONTROL_PORT`. `/healthz` reports HTTP liveness and tunnel discovery. Docker health also checks the bundled tunnel's readiness when enabled. A fresh controller holds scheduling while it rediscovers its tunnels and collects agent receipts; running agents keep serving approved workloads.

Only one controller may schedule a fleet. The Docker entrypoint takes an operating-system lock when sharing a volume; copied volumes on different hosts cannot protect each other. Stop the original controller before starting a migrated copy.

## Upgrade

Update the checkout to the release you intend to run, then rebuild and recreate the service:

```sh
docker compose build
docker compose up -d
docker compose ps
docker compose logs --tail 100 control
```

Keep the same `.env`, data volume, public controller URL and tunnel namespace. Do not rerun `init`. The updated image also serves the matching agent release; use the [agent upgrade procedure](../agent/README.md#upgrade-and-restart) to update hosts.

## Back up

Back up both the complete data volume and `.env` to private storage. The database contains app definitions, source versions, app secrets/password hashes and fleet settings. `identity.json` and environment credentials preserve the join token, admin password, signing key and tunnel namespace.

For a consistent filesystem backup:

1. Record the checkout's source commit and the Compose project/volume names.
2. Stop the controller with `docker compose stop control`; leave the agents running.
3. Archive the complete `control-data` volume and copy `.env` into private backup storage. Include `identity.json` and any database WAL files present.
4. Start the same controller with `docker compose start control` and check service health.

For online database backups use SQLite's backup API. Copying only a live `control.db` can miss committed data in its WAL. The private identity and environment still need their own backups. The [Docker volume backup guide](https://docs.docker.com/engine/storage/volumes/#back-up-restore-or-migrate-data-volumes) explains how to archive a named volume.

Keep agent/app storage backups separately. Controller recovery reconstructs desired workloads and placements, not application data stored on another machine. Do not run `docker compose down -v` when retaining fleet state.

## Restore or migrate

1. Stop the old scheduler and prevent it from restarting during cutover.
2. Restore the matching checkout, `.env` and complete data volume on the destination host. Preserve existing identity credentials and tunnel namespace. Do not initialize a new fleet.
3. Ensure the restored volume is writable by container UID/GID `1000:1000`. Retain private permissions on identity and configuration files.
4. Preserve the controller's public URL. If you use an external proxy, point it at the new controller, support WebSockets, and configure its actual peer address in `TRUSTED_PROXIES`.
5. Start with `docker compose up -d`. Verify health, tunnel discovery, agent reconnection, current app versions, region assignments and replica readiness before retiring the old host.

Keep the original backup until recovery is verified. Agents bind their identity and Docker ownership namespace to the controller URL; changing that URL requires a coordinated migration. A missing or incomplete database prevents startup instead of creating an empty fleet.

Runtime observations, metrics, events and tunnel tokens are held in controller memory. After a restart, agents provide signed receipts and current inventory while Cloudflare supplies tunnel identities. App source versions and operator settings remain authoritative in SQLite.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Startup refuses a missing database | Restore the correct volume; use `init` only for a new fleet. |
| Permission error under `/data` | Volume ownership and file permissions for UID/GID 1000. |
| Controller lock cannot be acquired | Another controller may be using the same volume. Do not delete a live lock file. |
| Tunnel discovery never becomes ready | Account/zone IDs, API permissions, tunnel namespace and outbound API access. |
| Public controller is unreachable | DNS, tunnel connector readiness and outbound port 7844. |
| External proxy loses agent connections | WebSocket forwarding, original HTTPS scheme and proxy timeouts. |
| Apps wait after startup | Recovery status, connected capacity, regions, workload errors and placement constraints in `/api/status`. |
