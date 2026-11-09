# Native Mac control plane

The same Node.js controller can run on macOS without Docker. Linux agent hosts
continue running application containers. The native launcher uses the existing
controller and Cloudflare connector supervisor, with a private runtime directory
and an advisory lock on the fleet database. The dashboard identifies the Mac as
the active controller.

Use Node.js 24 or newer, Python 3, and `cloudflared`. Install controller dependencies
with `npm ci --omit=dev` in `control/`, using the selected Node runtime. Keep the
entire repository, including `agent/`, in a stable local directory.

## Move an existing fleet

1. Stop and disable the original controller, including any service that restarts
   it. An unreachable host is not proof that its controller cannot return.
2. Copy a consistent, current `control.db`, `identity.json`, and the private
   environment configuration. For an online database use SQLite's backup API;
   copying a live database without its WAL is insufficient. Preserve app source,
   passwords, environment variables, the join token, signing key, public URL,
   `TUNNEL_PREFIX`, and `CONTROLLER_TUNNEL_NAME`.
3. Put restored state in a directory readable only by its owner. Create a
   mode-0600 JSON environment file outside the source checkout. Each value must
   be a string. Set absolute `DATA_DIR`, `RUNTIME_DIR`, and `PATH`; set
   `BIND_HOST=127.0.0.1`, `CONTROLLER_KIND=mac`, and an appropriate
   `CONTROLLER_NAME`. Preserve the previous deployment-auth policy.
4. Run `python3 control/native-service.py --config /absolute/environment.json
   --node /absolute/node --check`. This preflight does not initialize a fleet.
5. Once the original scheduler is disabled and the restored state is verified,
   run `python3 control/install-native.py --config /absolute/environment.json
   --node /absolute/node --logs /absolute/logs --start`. This installs a user
   LaunchAgent, starts it, and restarts it after a crash. Without `--start` it
   writes the definition for activation at the next login.
6. Verify localhost health, the public dashboard, agent reconnections, app
   versions, passwords, environment keys, replica readiness, and each provider's
   refill settings. Keep the original consistent backup until recovery passes.

Never initialize an empty replacement controller or substitute an old backup
without reconciling later configuration changes. Agent receipts restore runtime
inventory; they do not contain authoritative app definitions or credentials.
The local advisory lock does not fence a controller running on another computer.

## Availability and operation

The Mac must remain awake, connected to the Internet, and logged in. A LaunchAgent
starts after login, not at the FileVault login screen. Set `PREVENT_IDLE_SLEEP=1`
to hold an AC-power sleep assertion while this controller runs. Closing the lid,
explicitly sleeping, losing power, or logging out can still interrupt control.
Application containers on Linux hosts can continue serving during a controller
disconnect; replacement capacity and scheduling require a reachable controller.

Inspect `launchctl print gui/$(id -u)/com.hetpatel.runners-native`, the configured
logs, and the controller's `/healthz` and `/api/status` endpoints. Stop the service
with `launchctl bootout gui/$(id -u)/com.hetpatel.runners-native`. Bootstrap the
saved plist to start it again. Use `launchctl kickstart -k` for a controlled
restart under launchd, which also manages the supervisor's child process group.

Provider credentials and machine-count targets stay in provider adapters. The
three GitHub pools need no controller URL change when the public hostname is
preserved. Their existing watchdogs can keep supplying hosts after the move.

Create a consistent backup while the controller is running with
`python3 control/backup-native.py --config /absolute/environment.json
--destination /absolute/backups/unique-name`. It snapshots SQLite through the
backup API, includes the private environment and identity, and records hashes.
Keep a copy on a separate device or private storage to survive loss of the Mac.

The macOS integration check runs only disposable local state and a fake
Cloudflare API:

```sh
python3 control/test/native-service.py --node /absolute/node
```

It verifies missing-state refusal, private configuration permissions, launchd
startup, duplicate-controller rejection, and automatic recovery after a forced
supervisor crash. It removes the temporary LaunchAgent afterward.
