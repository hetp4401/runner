# Requirements

The system has two modules: one control plane per fleet, and one agent on each Docker host. The compute machines can come from any provider. The current implementation uses Cloudflare DNS and Tunnel for public networking.

## Software and permissions by module

| Requirement | Control plane host | Agent host |
| --- | --- | --- |
| Operating system | A server capable of running the provided Linux containers. | Linux; the installer rejects other Docker host types. |
| Docker | Running Docker Engine and Compose v2. | Running Docker Engine with `/var/run/docker.sock` accessible. |
| Host permissions | Permission to build/run the controller and manage its data volume. | Root for installation; Docker socket and host networking for the agent. |
| Host tools | Git for cloning the source. | `curl`, `tar`, `sha256sum` and `flock`, plus a POSIX shell and standard shell tools. |
| Runtime | Node.js 24 and npm dependencies are included in the image. | Node and Docker Compose are included in the installed image; the agent uses Node built-ins. |
| Credentials | Cloudflare management credential and private fleet identity. | Controller URL, join token and required server tag. |
| Persistent storage | `control-data` volume and private `.env`. | Absolute host directory, default `/var/lib/runner`, plus workload volumes/images. |

For direct source execution and tests, use Node.js 24. The controller uses `node:sqlite`; Node 14 and 20 cannot run it. Run `npm ci --omit=dev` in `control/` for controller runtime dependencies. A directly executed agent also needs Docker Compose v2 on its Linux host.

Docker access allows the agent to manage the host's workloads. Keep agent join credentials, controller credentials and persisted state private. The controller container runs as UID 1000 and does not need the Docker socket.

Use the official [Docker Engine installation guide](https://docs.docker.com/engine/install/) and [Compose plugin guide](https://docs.docker.com/compose/install/linux/) for your distribution. The repository does not enforce a fixed CPU, RAM or disk minimum: size the controller for fleet/source-history growth and agents for the workloads and builds they will run. App data is local unless the application supplies shared storage or replication.

## Cloudflare account and DNS

Use an active DNS zone, such as `example.com`, in your Cloudflare account. Set `DOMAIN=example.com`. The default controller hostname is `runners.example.com`; app replicas receive names such as `hello-1.example.com`.

Create an API token scoped to that account and zone:

| Scope | Permission | Used for |
| --- | --- | --- |
| Account | Cloudflare Tunnel: Edit | Creating, discovering and managing the fleet's tunnels. |
| Zone | DNS: Edit | Creating and updating controller and replica DNS records. |
| Zone | Zone: Read | Discovering account/zone IDs from `DOMAIN`. Optional if both `ACCOUNT_ID` and `ZONE` are supplied. |

Set `CF_API_TOKEN` only on the controller. The alternative is a Global API Key with both `CF_API_KEY` and `CF_API_EMAIL`. See Cloudflare's [API token permissions](https://developers.cloudflare.com/fundamentals/api/reference/permissions/).

The controller hostname must be available; setup refuses to replace unrelated DNS records. Keep the fleet's app domain distinct from other fleets. Using the zone apex as `DOMAIN` lets replica names use its normal wildcard certificate coverage; a nested app domain needs matching certificate coverage.

## Network access and ports

| Connection or port | Purpose |
| --- | --- |
| Outbound HTTPS, TCP 443 | Controller API/WebSockets, Cloudflare management API, registries and image/package downloads. |
| Outbound TCP/UDP 7844 | Cloudflare Tunnel: HTTP/2 uses TCP; QUIC uses UDP. |
| DNS resolution | Controller hostname, registries, package mirrors and Cloudflare endpoints. |
| Controller host loopback TCP 8920 | Default published HTTP port; change the host side with `CONTROL_PORT`. |
| Agent host TCP 19080–19082 | Default local router, tunnel metrics and router administration. Keep available for agent infrastructure. |
| App ports | Ports published by each assigned Compose stack. Avoid infrastructure ports; port 2019 is also reserved for legacy routers. |

Follow Cloudflare's [Tunnel firewall requirements](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-with-firewall/) for destination addresses and restricted firewalls. App image builds may require additional egress specific to their dependencies.

With the bundled tunnels, no public host IP or inbound forwarding is required for controller/app access. Docker-published app ports can still be reachable directly depending on your host network configuration; set host/network rules for the access you intend. An external controller proxy must support WebSockets and forward the original HTTPS scheme; configure only its actual peer IPs in `TRUSTED_PROXIES`.

## Operator CLI

[`bin/runnerctl`](../bin/runnerctl) runs on the operator's computer and requires Bash, curl 7.76 or later, jq, and Python 3. It needs a controller URL and the admin password for protected operations. Node and Docker are not needed just to use the CLI.

```sh
export RUNNER_URL=https://runners.example.com
export ADMIN_PASSWORD='your-admin-password'
bin/runnerctl status
```

## Preflight checks

On the controller host:

```sh
docker info --format '{{.OSType}}'
docker compose version
git --version
```

On a prospective agent host:

```sh
sudo docker info --format '{{.OSType}}'
command -v curl tar sha256sum flock
curl -fsS https://runners.example.com/healthz
```

The Docker host type must be `linux` for an agent. Successful HTTP health alone does not prove scheduling is ready; also check Docker health and the controller's recovery/tunnel status. Continue with the [control plane guide](../control/README.md), [agent guide](../agent/README.md) or complete [self-hosting sequence](self-hosting.md).
