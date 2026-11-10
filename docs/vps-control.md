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

Only one primary may schedule a fleet. A standalone deployment must run one scheduler; [replicated mode](control-replication.md) explicitly coordinates multiple copies. The Docker entrypoint takes an operating-system lock when sharing a volume; copied volumes on different hosts cannot protect each other. Stop the original controller before starting a migrated copy.

## Upgrade

Update the checkout to the release you intend to run, then rebuild and recreate the service:

```sh
docker compose build
docker compose up -d
docker compose ps
docker compose logs --tail 100 control
```

Keep the same `.env`, data volume, public controller URL and tunnel namespace. Do not rerun `init`. The updated image also serves the matching agent release; use the [agent upgrade procedure](../agent/README.md#upgrade-and-restart) to update hosts.

## Persisted state

The data volume contains the live SQLite database and fleet identity. Configuration
changes commit immediately. The database stores app definitions, source versions,
app secrets/password hashes and fleet settings. `identity.json` and the private
`.env` preserve credentials and the tunnel namespace. Keep the same data volume
and environment across upgrades. Do not run `docker compose down -v` when retaining
fleet state.

Runtime observations, metrics, events and tunnel tokens are held in controller
memory. After a restart, agents provide signed receipts and current inventory
while Cloudflare supplies tunnel identities. App source versions and operator
settings remain authoritative in SQLite. Application data stays on agent hosts.

## Migrate

1. Stop the old scheduler and prevent it from restarting during cutover.
2. Transfer the matching checkout, private `.env` and complete stopped data volume
   to the destination host. Include any remaining SQLite WAL files. Preserve
   existing identity credentials and the tunnel namespace. Do not initialize a
   new fleet.
3. Ensure the volume is writable by container UID/GID `1000:1000`. Retain private
   permissions on identity and configuration files.
4. Preserve the controller's public URL. If you use an external proxy, point it at
   the new controller, support WebSockets, and configure its actual peer address
   in `TRUSTED_PROXIES`.
5. Start with `docker compose up -d`. Verify health, tunnel discovery, agent
   reconnection, current app versions, region assignments and replica readiness.

Agents bind their identity and Docker ownership namespace to the controller URL;
changing that URL requires a coordinated migration. A missing or incomplete
database prevents startup instead of creating an empty fleet.

## Provider refill fallback

GitHub-hosted machines have a finite job lifetime. An agent reconnects while its
host remains alive; provider automation must replace hosts whose jobs have ended.
The provider watchdog workflows maintain each repository's `MACHINE_COUNT`.

For installations that need a backstop when GitHub schedule events are delayed,
`.github/scripts/capacity-fallback.py` can run under an external supervisor. It
checks connected counts every five minutes and dispatches the existing provider
watchdog when a group is below its repository target. The watchdog retains its
active-job checks, serialization and launch budget. The fallback waits through
controller recovery and leaves already queued watchdogs alone.

Its private JSON configuration supplies `control_url`, a local `status_url`,
`providers` (each with `login` and `repo`), and operator-selected
`credential_sources`. It reads credentials into memory without writing token
values. Use a dedicated service account and keep the configuration and aggregate
state private. `--once --dry-run` checks access and shows intended actions before
enabling the supervisor.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Startup refuses a missing database | Mount the existing volume; use `init` only for a new fleet. |
| Permission error under `/data` | Volume ownership and file permissions for UID/GID 1000. |
| Controller lock cannot be acquired | Another controller may be using the same volume. Do not delete a live lock file. |
| Tunnel discovery never becomes ready | Account/zone IDs, API permissions, tunnel namespace and outbound API access. |
| Public controller is unreachable | DNS, tunnel connector readiness and outbound port 7844. |
| External proxy loses agent connections | WebSocket forwarding, original HTTPS scheme and proxy timeouts. |
| Apps wait after startup | Recovery status, connected capacity, regions, workload errors and placement constraints in `/api/status`. |
