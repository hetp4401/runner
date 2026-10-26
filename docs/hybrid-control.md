# Hybrid control plane

The Mac is the preferred scheduler. A Worker and one SQLite-backed Durable Object (DO) provide the stable endpoint, authenticate agents, fence controller commands and take over when the Mac disconnects. Both schedulers run the same placement and region rules. Neither writes its working SQLite database to disk.

## Where state lives

| Data | Home | When written or read |
| --- | --- | --- |
| App specs, source files, saved versions, enabled state | Encrypted D1 | Written on changes; read at controller startup |
| App secrets and password hashes | Encrypted D1 | Written on changes; cached in memory |
| Desired pool sizes, rebalance setting, requested rolls | Encrypted D1 | Written on operator changes |
| Tunnel IDs and connector tokens | Encrypted D1 | Written when creating/retiring a slot; read at startup |
| Fleet join token, admin password, Mac token, receipt signing key, scoped Cloudflare API token | Encrypted D1 | Read at startup; authentication uses the memory cache |
| D1 encryption key | Worker secret plus private recovery backup | Needed to decrypt D1; never sent to fleet agents |
| Current containers, health, restarts, approved local plan | Agent's private host state | Updated locally; reported over its outgoing WebSocket |
| Placements, handovers, pending starts, scheduling cooldowns | Controller memory and signed receipts on agents | No D1 writes; rebuilt after a cold start |
| Fleet recovery snapshot | Three healthy agents, preferring different regions | Updated with plans; contains no app source or credentials |
| Metrics | Six hours on each agent; one hour in controller memory | No durable central metrics database |
| Logs | Application containers | Streamed on demand, never saved centrally |
| Active leader, lease, sequence, connection state | DO memory | No heartbeat persistence |
| Deployment enable switch and watchdog alarm | DO native storage | Switch changes; watchdog at most once per minute unless a nearer deadline is needed |

D1 payloads use AES-GCM with a random IV and authenticated bucket/row identity. The Mac receives app configuration and the credentials needed to authenticate API requests and sign receipts. Cloudflare API credentials and the D1 encryption key remain at the gateway. Agents authenticate with the fleet join token and receive only their assigned tunnel connector token and app configuration.

Routine reports and heartbeats do **zero D1 reads and writes**. Configuration changes are acknowledged only after their D1 batch commits. D1 holds no health, run, placement, event, metric or lease rows.

## Handover and recovery

1. The Mac opens an authenticated outbound WebSocket and loads a consistent working snapshot. It receives incremental changes while standing by.
2. After two minutes of stable connection and acknowledgement of every change, the DO grants a new leadership generation. The Mac renews a 90-second lease every 30 seconds.
3. Disconnects trigger immediate fallback; a silent Mac times out through its lease or a 25-second operation timeout. All Cloudflare effects pass through the DO, which rejects stale generations and completes already-issued effects before switching schedulers.
4. A returning Mac reloads the latest state and passes the same stability and acknowledgement barrier before resuming.
5. If the DO itself restarts, it loads configuration from D1 and spends three minutes collecting signed agent receipts. Agents keep running and repairing existing containers. Legacy agents receive a retryable error during reconstruction so they do not interpret an incomplete plan as a command to remove workloads.

Each agent retains its own receipt. Three healthy agents also retain a fleet snapshot, preserving moves and the state of older agents during rolling upgrades. The newest signed snapshot wins per machine; current agent observations update health and last-seen timestamps. If a live older agent has work but no usable recovery record, scheduling waits instead of deleting unknown workloads. If the whole fleet is lost, D1 still contains the desired configuration needed to rebuild it; runtime history is lost.

## Deployment and operation

The Worker is `runner-control-hybrid`, with D1 `runners-config` and a single named `fleet` coordinator. The staged endpoint is `https://runner-control-hybrid.billythebishop4.workers.dev`. Production routes are the existing controller hostnames, so agents retain their URL.

From `control/`, use Node 24 and `npm ci`, apply `migrations/0001_configuration.sql` to the configured D1 database, then `wrangler deploy`. The initial deployment accepts a private JSON secrets file through `--secrets-file`; it includes `CONFIG_KEY` (32 random bytes, standard base64), `MAC_TOKEN`, `STATE_SIGNING_KEY`, `JOIN_TOKEN`, `ADMIN_PASSWORD`, and a scoped `CF_API_TOKEN`. Initial startup encrypts controller credentials into D1. Remove the bootstrap Worker copies after verifying D1; keep `CONFIG_KEY`. Do not commit credentials.

The Mac runs `control/mac.mjs` with `CONTROL_URL` and `MAC_TOKEN`. Its runtime is installed under `~/Library/Application Support/Runners`, outside Documents, and starts at login through `~/Library/LaunchAgents/com.hetpatel.runners-control.plist`. `mac.env` and the recovery key backup are private files. There is no inbound Mac port, Mac tunnel or runtime database file.

`GET /internal/hybrid/status` reports the active scheduler, connection state, recovery gate and this instance's D1 row counters. `/api/status` includes the same `controlPlane` object; the dashboard labels the active scheduler. Run entries include `protocol`, `recoveryAt` and `witnessAt` to verify that agents have returned their stored receipts.

Authenticated migration endpoints require `Authorization: Bearer <MAC_TOKEN>`:

- `POST /internal/enable` with `{"enabled":false|true}` controls staging.
- `POST /internal/import` with `{"table":"projects","rows":[...],"clear":true}` imports a working table while disabled. Only configuration tables reach D1. Import before connecting the Mac.
- `POST /internal/reset` forces a cold-start test while disabled. Configuration is flushed first, and the next request rebuilds the working database from D1.

For cutover, stop the old scheduler before importing its final working snapshot and enabling the new one. Verify current app versions, secret-key names, region assignments and healthy replicas. Then route the controller hostnames to the Worker and disable the old VPS services. Keep the old SQLite backup for rollback. Never run two unfenced schedulers against the same tunnels and DNS.

## Free-tier budget and limits

At 100 agents with one report per minute, reports produce 144,000 incoming DO WebSocket messages per day, about 7,200 metered requests at the documented 20:1 ratio. At 80% Mac uptime, Mac replies add about 5,760 requests and heartbeats about 116. Watchdog alarms add up to 1,440, plus connections, admin traffic, configuration changes and live logs. Agent WebSocket messages do not each create a new outer Worker HTTP request. Legacy HTTP agents temporarily count once per report, so use the configured 60-second interval while upgrading.

One continuously resident DO at the 128 MB billing size uses approximately 11,059 GB-s/day, within the 13,000 GB-s/day free allowance. This budgets continuous operation, including a full-day Mac outage; 80% uptime is not required for the duration budget. Do not create extra always-running coordinator objects under the same free allowance.

The free allowances are shared by other account workloads. The current design fits its expected control traffic; unusually heavy public API use or log streaming can consume the remaining request budget. No paid upgrade or automatic paid overage is configured. Source/version growth also needs monitoring against D1 storage and Worker memory limits; the initial migration contains 166 versions and about 18 MB of source. Older history is preserved, not silently pruned.

References: [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [Workers limits](https://developers.cloudflare.com/workers/platform/limits/), [D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/).
