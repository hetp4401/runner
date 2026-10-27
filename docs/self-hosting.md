# Run your own fleet

The control plane runs in Docker on your server. Agents run on your Linux Docker machines and connect to your control plane. GitHub Actions is optional. No Cloudflare Worker, Durable Object, D1 database or connection to the author's control plane is required.

## Requirements

- A host with Docker Engine and Docker Compose v2 for the control plane, with persistent storage for its data volume.
- An active DNS zone on Cloudflare, such as `example.com`. Use the zone's apex as `DOMAIN`; replicas get names such as `hello-1.example.com`. The default controller hostname is `runners.example.com` and must be unused.
- A Cloudflare API token with **Account → Cloudflare Tunnel → Edit**, **Zone → DNS → Edit**, and **Zone → Zone → Read**, scoped to that account and zone. Zone Read allows automatic discovery of the account and zone IDs; it can be omitted if both `ACCOUNT_ID` and `ZONE` are supplied. A Global API Key also works with `CF_API_EMAIL` and `CF_API_KEY` instead of `CF_API_TOKEN`.
- Linux machines with Docker Engine for agents. Agents need access to the host Docker socket and use host networking. Treat access to the fleet's join token and deployment credentials as access to its machines.
- Outbound access to the controller over HTTPS, container registries, and Cloudflare Tunnel (port 7844 TCP/UDP). No public IP or inbound port forwarding is needed when using the included tunnel.

Cloudflare documents the [Tunnel requirements and permissions](https://developers.cloudflare.com/tunnel/get-started/). A domain below your zone's apex needs certificate coverage for its replica subdomains; the apex-domain setup avoids needing an additional wildcard certificate.

## Start a control plane

Clone this repository and run these commands from its root:

```sh
cp .env.example .env
chmod 600 .env
# Edit .env: set DOMAIN and CF_API_TOKEN (or CF_API_EMAIL + CF_API_KEY).
docker compose build
docker compose run --rm control init
docker compose run --rm control credentials
docker compose up -d
docker compose logs -f control
```

`init` creates the SQLite database and generates an admin password, an agent join token and a receipt signing key. It refuses to overwrite an existing fleet. `credentials` displays the admin password and join token; keep them private. You may supply these credentials in `.env` before initialization instead.

Open `https://runners.example.com`, substituting your domain. Startup automatically discovers your Cloudflare account/zone, creates or reuses the controller's tunnel and sets its DNS record. It refuses to replace an unrelated record. The `cloudflared` process runs inside the same container; its token is retrieved at startup and passed in memory, never saved in the database. The container runs as UID 1000 and does not mount the Docker socket.

New fleets require the admin password to deploy apps. An app password can delegate updates to that app. Dashboard status and app source are public; secret values and logs require credentials. `REQUIRE_DEPLOY_AUTH=0` explicitly enables public deployments for community fleets.

## Join your machines

On each Linux Docker host, use its fleet's join token:

```sh
curl -fsSL https://runners.example.com/install.sh |
  sudo JOIN_TOKEN='your-join-token' LABEL='my-server' sh
```

The installer starts the `runner-agent` container with a restart policy. The agent downloads its code from **your control plane's bundled release** at `/agent/`, claims a machine slot, and starts its router and Cloudflare connector. Agents receive only their connection token; they do not need the Cloudflare management key, account ID or domain configuration. Each host keeps its agent identity, recovery receipt and app data under `/var/lib/runner`.

Use one agent per Docker host. `LABEL` is optional; `RUNNER_DATA` can select another absolute host path. `POOL` and `POOL_SIZE` are optional advanced settings for managed groups of machines. A standalone host needs no pool or machine-starting command.

```sh
docker logs -f runner-agent
```

To remove a host, stop its agent, router and tunnel, then retire its empty slot in the dashboard. App data on that host is local; this system does not replicate Docker volumes between machines.

## Deploy an app

Use the dashboard, or the CLI from this repository:

```sh
export RUNNER_URL=https://runners.example.com
export ADMIN_PASSWORD='your-admin-password'
bin/runnerctl apply examples/hello hello --port 9000 --replicas 1
```

The app is published at `https://hello-1.example.com`. The CP schedules its container on a connected agent. Change `--replicas` to run more copies; region constraints in the app spec limit where those copies can run.

The CLI also reads its URL from `~/.config/runnerctl/url` and its password from `~/.config/runnerctl/admin-password`. It has no default fleet address.

## Persistence, restart and upgrades

The `control-data` volume contains:

- `control.db`: app specs, source versions, app secrets/password hashes and operator settings.
- `identity.json`: the admin password, join token and receipt signing key generated by `init`.
- Migration backups, when upgrading an older database.

Runtime observations, placements, health, metrics and fleet tunnel mappings/tokens stay in RAM. On restart the CP rediscovers its tunnel namespace from Cloudflare and recovers running placements from agent receipts. It holds scheduling for three minutes and until tunnel discovery succeeds. Existing agents keep their containers during recovery.

Back up the data volume **and `.env`**. For a simple consistent backup, stop only the CP, archive the volume, then start it again; running agents keep their workloads. For an online backup, use SQLite's backup API rather than copying a live `control.db` without its WAL. The signing key must survive recovery. Never start two control planes for the same fleet.

Upgrade from an updated checkout:

```sh
docker compose build
docker compose up -d
```

Do not rerun `init` during upgrades. Do not use `docker compose down -v` unless deliberately deleting the fleet's persisted configuration. A missing database prevents startup rather than creating an empty desired fleet.

## Existing reverse proxy or control plane

Set `AUTO_TUNNEL=0` and `PUBLIC_URL=https://your-control-host.example.com` to use an existing tunnel/reverse proxy. The container listens on port 8920; Compose publishes it only on host loopback. Set `CONTROL_PORT` to change that host-side port. The proxy must support WebSockets and forward the original HTTPS scheme.

To move an existing Node control plane into Docker, stop the old scheduler first, preserve its database and its three identity credentials, and mount its data directory at `/data` (owned by UID 1000). Supply the existing credentials through the container environment, set `TUNNEL_PREFIX=runner` for legacy `runner-<n>` tunnels, and preserve the previous deployment access policy with `REQUIRE_DEPLOY_AUTH`. Do not run `init` for an existing fleet. Keep its existing public tunnel running with `AUTO_TUNNEL=0` during the move.

By default, new Docker fleets derive a tunnel prefix from their control hostname, so they do not adopt another fleet's tunnels in the same Cloudflare account. Give separate fleets separate app domains; identical app names on the same domain would need the same DNS record.

## Optional GitHub Actions agents

Set repository variable `CONTROL_URL` to your CP URL and repository secret `JOIN_TOKEN` to its join token. Set `POOL` and `POOL_SIZE` if using a managed pool. `AGENT_REPO` optionally selects a different checkout; otherwise it uses the current repository. Forked workflows stay inactive until `CONTROL_URL` is configured. Agent code changes request a rolling restart; workflow configuration changes can be rolled manually.
