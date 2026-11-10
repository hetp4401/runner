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
volumes. The defaults above reserve 2 GiB RAM while the VM is running. Monitor
free disk space as builds and source history accumulate. Keep the installed release outside Desktop/Documents, for example
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
   and remove its plist from `~/Library/LaunchAgents`. An unreachable VPS is not
   proof its old scheduler is stopped.
3. Transfer the stopped controller's current SQLite files, `identity.json` and
   private configuration together, including any remaining database WAL files.
   Preserve source versions, app passwords/environment, admin/join credentials,
   signing key, domain, public URL, tunnel prefix, controller tunnel name and
   deployment-auth policy.
4. Create the Docker data volume without starting the controller:
   `docker compose create control`. Get its container ID with
   `docker compose ps --all --quiet control`. Use `docker cp` to transfer the
   stopped controller's SQLite files and `identity.json` into that container's
   `/data/`.
   Use a one-off container with the same volume and `--user 0 --entrypoint sh`
   to set ownership to UID/GID 1000, directory mode 0700 and file mode 0600.
   Do not copy a live WAL/database pair separately or a stale runtime directory.
5. Verify the copied database's integrity, project/version counts and canonical
   contents of all five durable tables against the stopped source database. Preserve the
   existing named controller tunnel; no DNS or provider URL change is needed.
6. Start the Docker service, then allow its recovery window and agent reconnects.
   Verify localhost and public health, container health, admin authentication,
   connected provider groups, current app versions and healthy replicas. Only
   the Docker scheduler should be active.

SQLite stays on the Linux named volume, not a macOS shared bind mount. A local
Linux lock cannot fence a native process or an old VPS; retirement is explicit.

## Login startup

Create a mode-0600 service JSON outside the source checkout, using absolute paths:

```json
{
  "profile": "runners",
  "project": "runners",
  "source": "/Users/you/.local/share/runners-control/releases/your-revision",
  "environment_file": "/Users/you/.config/runners-control/docker.env",
  "logs": "/Users/you/.local/share/runners-control/logs"
}
```

From that release directory:

```sh
python3 control/mac-docker.py check --config /absolute/service.json
python3 control/mac-docker.py install --config /absolute/service.json
# Activate only after the old controller is retired and data is restored.
launchctl bootstrap gui/$(id -u) "$HOME/Library/LaunchAgents/io.runners.docker.plist"
```

The service starts Colima when necessary and ensures the existing Compose
service is up. It never initializes a fleet, builds an image or deletes a volume.
Docker restarts the controller on process failure; launchd restarts the Mac
supervisor. A graceful supervisor stop also stops the controller. Configuration
changes commit immediately to SQLite on the persistent Docker volume.

## Checks and upgrades

```sh
launchctl print gui/$(id -u)/io.runners.docker
docker compose ps
docker compose logs --tail 100 control
curl -fsS http://127.0.0.1:8920/healthz
```

Status reports `controlPlane.owner=mac`, `runtime=docker`, and `mode=docker-v1`.
The dashboard shows **Mac controller (Docker)**. A healthy HTTP server alone does
not prove fleet recovery; inspect agent groups and app readiness too.

Before upgrades, build and test the new image. Point the private service JSON to
the staged release, restart the LaunchAgent and verify recovery. Keep the same
profile, project and volume. Never use `docker compose down -v` or delete Colima's
data disk while retaining the fleet.

When moving back to a native or VPS controller, stop and disable the Docker
LaunchAgent first, then transfer the current stopped database and private
configuration using the migration procedure above. Preserve credentials and
namespaces, verify data, and enable exactly one scheduler.

The Mac must stay logged in, awake and connected. The supervisor prevents idle
system sleep on AC power; it cannot prevent lid/manual sleep or run at the
FileVault login screen. The independent provider fallback may remain a native
LaunchAgent. Provider credentials stay outside the controller; see
[provider adapters](providers.md).
