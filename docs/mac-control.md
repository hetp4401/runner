# Docker control plane on a Mac

The controller and its Cloudflare connector run together in the existing Linux
Docker image. Colima supplies Docker Engine on macOS. The fleet's applications
and agents continue running on their Linux hosts. No Docker socket is mounted in
the controller container.

## Install the engine and stage source

On Apple Silicon with Homebrew and Python 3:

```sh
brew install colima docker docker-compose docker-buildx
colima start runners --vm-type vz --mount-type virtiofs \
  --cpu 2 --memory 2 --disk 10 --root-disk 8 --activate=false
export DOCKER_CONTEXT=colima-runners
docker compose version
```

Follow Homebrew's caveat to add its `lib/docker/cli-plugins` directory to
`cliPluginsExtraDirs` in `~/.docker/config.json`, preserving existing settings.
See the [Colima project](https://github.com/abiosoft/colima) and
[configuration reference](https://colima.run/docs/configuration/).

Use a dedicated `runners` profile. Its sparse disks hold Docker images and named
volumes; exported backups must live outside that VM. The defaults above reserve
2 GiB RAM while the VM is running. Monitor free disk space as builds and backups
accumulate. Keep the installed release outside Desktop/Documents, for example
`~/.local/share/runners-control/releases/<revision>`, so launchd can read it after
login without depending on interactive document access permissions.

Build from the staged repository root with `docker build -t runner-control:local .`.
Put the private Docker environment at `~/.config/runners-control/docker.env`,
mode 0600, with its parent mode 0700. Copy the values from `.env.example` for a new
fleet. Set `CONTROLLER_KIND=mac` and `CONTROLLER_NAME` to the host's name.
Keep container paths `DATA_DIR=/data`, `RUNTIME_DIR=/run/runner`,
`BIND_HOST=0.0.0.0`, and `PORT=8920`. Do not copy a native Mac `PATH` into Docker.
The published host port remains loopback-only.

Use these explicit settings for every administrative Compose command:

```sh
export DOCKER_CONTEXT=colima-runners
export CONTROL_ENV_FILE="$HOME/.config/runners-control/docker.env"
export COMPOSE_PROJECT_NAME=runners
export COMPOSE_DISABLE_ENV_FILE=1
# Run from the staged repository root.
docker compose config --quiet
```

For a **new fleet only**, run `docker compose run --rm control init`, then the
private `credentials` command and `docker compose up -d`. For an existing fleet,
follow the migration procedure instead. Never run `init` on a replacement fleet.

## Move an existing native or VPS controller

1. Build and test the image before the cutover. Docker smoke tests use fake
   Cloudflare credentials and disposable volumes; do not start a second live
   scheduler for testing.
2. Disable and stop the old controller, its connector and all automatic startup
   paths. For launchd, use `launchctl disable gui/$(id -u)/<label>` and `bootout`,
   and archive its plist outside `~/Library/LaunchAgents`. Retire the old backup
   schedule too so it cannot keep backing up a stale database. An unreachable
   VPS is not proof its old scheduler is stopped.
3. Make a final consistent SQLite snapshot and copy `identity.json` plus private
   configuration. The native helper is `python3 control/backup-native.py --config
   /absolute/environment.json --destination /absolute/backups/before-docker`.
   Preserve source versions, app passwords/environment, admin/join credentials,
   signing key, domain, public URL, tunnel prefix, controller tunnel name and
   deployment-auth policy. Keep this snapshot for rollback.
4. Create the Docker data volume without starting the controller:
   `docker compose create control`. Get its container ID with
   `docker compose ps --all --quiet control`. Use `docker cp` to copy only the
   snapshot's `control.db` and `identity.json` into that container's `/data/`.
   Use a one-off container with the same volume and `--user 0 --entrypoint sh`
   to set ownership to UID/GID 1000, directory mode 0700 and file mode 0600.
   Do not copy a live WAL/database pair separately or a stale runtime directory.
5. Verify the copied database's integrity, project/version counts and canonical
   contents of all five durable tables against the final snapshot. Preserve the
   existing named controller tunnel; no DNS or provider URL change is needed.
6. Start the Docker service, then allow its recovery window and agent reconnects.
   Verify localhost and public health, container health, admin authentication,
   connected provider groups, current app versions and healthy replicas. Only
   the Docker scheduler should be active.

SQLite stays on the Linux named volume, not a macOS shared bind mount. A local
Linux lock cannot fence a native process or an old VPS; retirement is explicit.

## Login startup and backups

Create a mode-0600 service JSON outside the source checkout, using absolute paths:

```json
{
  "profile": "runners",
  "project": "runners",
  "source": "/Users/you/.local/share/runners-control/releases/your-revision",
  "environment_file": "/Users/you/.config/runners-control/docker.env",
  "logs": "/Users/you/.local/share/runners-control/logs",
  "backups": "/Users/you/.local/share/runners-control/backups"
}
```

From that release directory:

```sh
python3 control/mac-docker.py check --config /absolute/service.json
python3 control/mac-docker.py install --config /absolute/service.json
# Activate only after the old controller is retired and data is restored.
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/io.runners.docker.plist"
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/io.runners.docker.backup.plist"
```

The first service starts Colima when necessary and ensures the existing Compose
service is up. It never initializes a fleet, builds an image or deletes a volume.
Docker restarts the controller on process failure; launchd restarts the Mac
supervisor. A graceful supervisor stop also stops the controller. The second
LaunchAgent exports a backup at 04:15 local time when the user session is active.
Backups are retained until the operator removes them; monitor disk usage.

Run an on-demand backup with:

```sh
python3 control/mac-docker.py backup --config /absolute/service.json
```

`backup.mjs` uses SQLite's online backup API inside the running container,
including committed WAL content. The Mac helper exports the snapshot and fleet
identity, validates checksums and SQLite integrity, and adds the private Docker
environment and service configuration. Every output directory/file is private.
Copy verified backups to a separate device or private storage to survive loss of
the Mac. A local export alone is not disaster recovery.

## Checks, upgrades and rollback

```sh
launchctl print gui/$(id -u)/io.runners.docker
docker compose ps
docker compose logs --tail 100 control
curl -fsS http://127.0.0.1:8920/healthz
```

Status reports `controlPlane.owner=mac`, `runtime=docker`, and `mode=docker-v1`.
The dashboard shows **Mac controller (Docker)**. A healthy HTTP server alone does
not prove fleet recovery; inspect agent groups and app readiness too.

Before upgrades, export a backup and build/test the new image. Point the private
service JSON to the staged release, restart the LaunchAgent and verify recovery.
Keep the same profile, project and volume. Never use `docker compose down -v` or
delete Colima's data disk while retaining the fleet.

For rollback, disable/boot out the Docker LaunchAgent and stop the Compose
controller before starting any native/VPS service. If Docker accepted any config
changes, export a **current** snapshot first and restore it to the fallback host;
do not silently discard them by restoring the pre-cutover database. Preserve
credentials and namespaces, verify data, and then enable exactly one scheduler.
If configuration is unchanged, the verified pre-cutover snapshot can be used.

The Mac must stay logged in, awake and connected. The supervisor prevents idle
system sleep on AC power; it cannot prevent lid/manual sleep or run at the
FileVault login screen. The independent provider fallback may remain a native
LaunchAgent. Provider credentials stay outside the controller; see
[provider adapters](providers.md).
