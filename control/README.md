# Control plane module

The control plane accepts app definitions, schedules their replicas across connected agents, manages public DNS and tunnels, and exposes the dashboard and API. Run one controller per fleet.

The controller runs independently of the agent hosts. It includes a matching agent release so new hosts can install directly from its `/install.sh` endpoint. Build the image from the repository root: it needs both `control/` and `agent/`.

## Requirements

- Docker Engine with Docker Compose v2 and persistent storage on the controller host.
- An active Cloudflare DNS zone and a credential permitted to manage that zone's DNS and the account's tunnels.
- Outbound HTTPS and Cloudflare Tunnel connectivity.
- Node.js 24 and npm only when running directly or developing outside Docker. The image includes its runtime and dependencies.

See [requirements](../docs/requirements.md) for exact credential scopes, ports and host tools. The controller container runs as UID 1000 and does not mount the Docker socket.

## Start a new fleet

From the repository root:

```sh
cp .env.example .env
chmod 600 .env
```

Edit `.env` and supply your own values:

```dotenv
DOMAIN=example.com
CF_API_TOKEN=replace-with-your-scoped-api-token
REQUIRE_DEPLOY_AUTH=1
```

Then initialize and start:

```sh
docker compose build
docker compose run --rm control init
docker compose run --rm control credentials
docker compose up -d
docker compose ps
docker compose logs -f control
```

Initialization creates `control.db` and private fleet identity in the `control-data` volume. `credentials` displays the generated admin password and join token; keep that output private. `init` is only for a new fleet and refuses existing data.

Open `https://runners.example.com`, replacing the domain. The bundled connector exposes the controller and its WebSocket API. The full sequence, including a first app deployment, is in [self-hosting](../docs/self-hosting.md).

## Configuration

The Docker package reads `.env` through the root [Compose file](../compose.yaml). An environment variable does not automatically create a corresponding process outside the package.

| Setting | Purpose and default |
| --- | --- |
| `DOMAIN` | Required DNS zone apex, without a scheme or path. |
| `CF_API_TOKEN` | Scoped management token. Alternatively use both `CF_API_EMAIL` and `CF_API_KEY`. |
| `CONTROL_HOST` | Public controller hostname; defaults to `runners.<DOMAIN>`. |
| `ACCOUNT_ID`, `ZONE` | Discovered from Cloudflare; supply both to skip discovery. |
| `ADMIN_PASSWORD` | Operator credential; generated at initialization unless supplied. |
| `JOIN_TOKEN` | Agent credential; generated at initialization unless supplied. |
| `STATE_SIGNING_KEY` | Recovery receipt signing key; generated at initialization unless supplied. Preserve it across restores. |
| `REQUIRE_DEPLOY_AUTH` | `1` by default. Requires authentication for deployments and changes to unlocked apps. |
| `AUTO_TUNNEL` | `1` by default. Starts the bundled controller connector. Set `0` to use your own proxy/tunnel. |
| `PUBLIC_URL` | Controller's public origin; defaults to `https://<CONTROL_HOST>`. Set it explicitly with your own proxy. |
| `CONTROL_PORT` | Host-side loopback port in Compose; defaults to `8920`. |
| `CLIENT_IP_HEADER` | Forwarded client address header; defaults to `cf-connecting-ip`. |
| `TRUSTED_PROXIES` | Explicit proxy IPs/CIDRs; default is loopback. Set the actual peer address when using an external proxy. |
| `MEMORY_HISTORY_MIN` | Cached metrics window; defaults to `360` minutes. |
| `RECOVERY_MS` | Agent recovery window after restart; defaults to `180000` milliseconds. |
| `ALERT_WEBHOOK` | Optional destination for alert JSON. |

`DATA_DIR=/data`, `PORT=8920` and `BIND_HOST=0.0.0.0` are set by the image. Keep the container port at 8920 with the provided Compose mapping; use `CONTROL_PORT` to change only the host port. Boolean settings accept `1`/`true` and `0`/`false`.

The [self-hosting guide](../docs/self-hosting.md#existing-reverse-proxy-or-control-plane) covers external proxies, stable tunnel namespaces and migration. The dashboard, status, metrics and app source are public. Secret values, logs and administrative operations require credentials; put application secrets in app environment variables.

## Operation

```sh
# Inspect HTTP health and controller recovery status.
curl -fsS http://127.0.0.1:8920/healthz
curl -fsS https://runners.example.com/api/status

# Rebuild from your updated checkout, preserving the data volume.
docker compose build
docker compose up -d
```

Adjust the local URL if you changed `CONTROL_PORT`. `/healthz` confirms HTTP liveness and reports `tunnelsReady`; Docker's health check also requires tunnel discovery and, with the bundled connector, a ready connection. Scheduling waits for recovery before placing workloads.

Back up the data volume and `.env` together. Agents continue serving their approved workloads while the controller is stopped, but new deployments and scheduling wait for it. Do not run `init` for an upgrade or use `docker compose down -v` when retaining fleet state. See [VPS operations](../docs/vps-control.md) for backup and restore procedures.

## Source and development

| File | Responsibility |
| --- | --- |
| [`server.mjs`](server.mjs) | HTTP server, agent WebSockets, live log connections and shutdown. |
| [`src/index.js`](src/index.js) | App validation, authentication, placement, reconciliation and plans. |
| [`src/local-store.mjs`](src/local-store.mjs) | Durable SQLite configuration and in-memory runtime tables. |
| [`src/state-policy.js`](src/state-policy.js) | Boundary between authoritative configuration and recoverable observations. |
| [`src/bootstrap.mjs`](src/bootstrap.mjs) | Initialization, identity and Cloudflare discovery. |
| [`src/install-script.mjs`](src/install-script.mjs) | Host agent installer. |
| [`src/agent-bundle.mjs`](src/agent-bundle.mjs) | Agent release packaging and hashes. |
| [`src/app.html`](src/app.html) | Dashboard. |
| [`docker-entrypoint.mjs`](docker-entrypoint.mjs) | Container startup and connector supervision. |

With Node.js 24, run from `control/`:

```sh
npm ci --omit=dev
npm run test:self-hosting
npm run test:local
```

These tests use disposable state and a fake Cloudflare API. See the [test reference](test/README.md) for scheduler simulations and Docker integration. Direct `npm start` requires an initialized database, the controller's identity credentials and its Cloudflare/account/zone environment; a `.env` file alone is not loaded by `npm start`. Use the Docker path above for a complete first installation.
