# Cloudflare control plane

The production controller runs entirely on Cloudflare. A Worker provides the stable endpoint and one SQLite-backed Durable Object (DO) authenticates agents and runs all placement and region rules. D1 holds essential configuration. The working SQLite database stays in memory; neither a Mac nor a VPS is needed.

Production sets `SCHEDULER=cloudflare`. External scheduler connections are refused at both the Worker and DO, so an old Mac process cannot acquire leadership. The previous hybrid implementation remains available for rollback and tests only, by explicitly setting `SCHEDULER=hybrid`.

## Where state lives

| Data | Home | When written or read |
| --- | --- | --- |
| App specs, source files, saved versions, enabled state | Encrypted D1 | Written on changes; read at controller startup |
| App secrets and password hashes | Encrypted D1 | Written on changes; cached in memory |
| Desired pool sizes, rebalance setting, requested rolls | Encrypted D1 | Written on operator changes |
| Tunnel IDs and connector tokens | Encrypted D1 | Written when creating/retiring a slot; read at startup |
| Fleet join token, admin password, maintenance token, receipt signing key, scoped Cloudflare API token | Encrypted D1 | Read at startup; authentication uses the memory cache |
| D1 encryption key | Worker secret plus private recovery backup | Needed to decrypt D1; never sent to fleet agents |
| Current containers, health, restarts, approved local plan | Agent's private host state | Updated locally; reported over its outgoing WebSocket |
| Placements, handovers, pending starts, scheduling cooldowns | Controller memory and signed receipts on agents | No D1 writes; rebuilt after a cold start |
| Fleet recovery snapshot | Three healthy agents, preferring different regions | Updated with plans; contains no app source or credentials |
| Metrics | Six hours on each agent; one hour in controller memory | No durable central metrics database |
| Logs | Application containers | Streamed on demand, never saved centrally |
| Controller generation, sequence, connection state | DO memory | No heartbeat persistence |
| Deployment enable switch and watchdog alarm | DO native storage | Switch changes; watchdog at most once per minute unless a nearer deadline is needed |

D1 payloads use AES-GCM with a random IV and authenticated bucket/row identity. Cloudflare API credentials and the D1 encryption key remain in Cloudflare. Agents authenticate with the fleet join token and receive only their assigned tunnel connector token and app configuration.

Routine reports and heartbeats do **zero D1 reads and writes**. Configuration changes are acknowledged only after their D1 batch commits. D1 holds no health, run, placement, event, metric or lease rows.

## Recovery

1. The DO loads configuration and credentials from D1 on a cold start, then caches them in memory.
2. It spends three minutes collecting signed agent receipts before changing placements. Agents keep running and repairing existing containers. Legacy agents receive a retryable error during reconstruction so they do not interpret an incomplete plan as a command to remove workloads.
3. Once reconstruction is complete, the DO resumes scheduling and serializes configuration changes and Cloudflare API operations. There is one named coordinator for the whole fleet.

Each agent retains its own receipt. Three healthy agents also retain a fleet snapshot, preserving moves and the state of older agents during rolling upgrades. The newest signed snapshot wins per machine; current agent observations update health and last-seen timestamps. If a live older agent has work but no usable recovery record, scheduling waits instead of deleting unknown workloads. If the whole fleet is lost, D1 still contains the desired configuration needed to rebuild it; runtime history is lost.

## Deployment and operation

The existing Worker name `runner-control-hybrid` is retained to preserve its DO identity and D1 bindings. D1 is `runners-config`, with a single named `fleet` coordinator. Its workers.dev endpoint is `https://runner-control-hybrid.billythebishop4.workers.dev`. Production routes are the existing controller hostnames, so agents retain their URL.

From `control/`, use Node 24 and `npm ci`, apply `migrations/0001_configuration.sql` to the configured D1 database, then `wrangler deploy`. The initial deployment accepts a private JSON secrets file through `--secrets-file`; it includes `CONFIG_KEY` (32 random bytes, standard base64), `MAC_TOKEN`, `STATE_SIGNING_KEY`, `JOIN_TOKEN`, `ADMIN_PASSWORD`, and a scoped `CF_API_TOKEN`. Initial startup encrypts controller credentials into D1. Remove the bootstrap Worker copies after verifying D1; keep `CONFIG_KEY`. Do not commit credentials.

The Mac LaunchAgent has been stopped and disabled, and its plist moved out of `~/Library/LaunchAgents` into `~/Library/Application Support/Runners/disabled`. The private recovery key backup and inactive runtime are retained there. The old VPS controller and its tunnel remain disabled.

`GET /internal/hybrid/status` retains its URL for compatibility and reports `mode: "cloudflare-v1"`, `owner: "do"`, connection state, recovery gate and this instance's D1 row counters. `/api/status` includes the same `controlPlane` object; the dashboard labels it Cloudflare controller. Run entries include `protocol`, `recoveryAt` and `witnessAt` to verify that agents have returned their stored receipts.

Cloudflare can restart the DO during platform updates or when enabling live logs. That triggers configuration reload and agent recovery even when no application code changed; see [Cloudflare's known issues](https://developers.cloudflare.com/durable-objects/platform/known-issues/).

Authenticated migration endpoints require `Authorization: Bearer <MAC_TOKEN>`; the legacy credential name is retained as a maintenance token, without permitting Mac scheduler access:

- `POST /internal/enable` with `{"enabled":false|true}` controls staging.
- `POST /internal/import` with `{"table":"projects","rows":[...],"clear":true}` imports a working table while disabled. Only configuration tables reach D1. Import before connecting the Mac.
- `POST /internal/reset` forces a cold-start test while disabled. Configuration is flushed first, and the next request rebuilds the working database from D1.

For cutover, stop the old scheduler before importing its final working snapshot and enabling the new one. Verify current app versions, secret-key names, region assignments and healthy replicas. Then route the controller hostnames to the Worker and disable the old VPS services. Keep the old SQLite backup for rollback. Never run two unfenced schedulers against the same tunnels and DNS.

## Usage budget and billing limits

At 100 agents with one report per minute, reports produce 144,000 incoming DO WebSocket messages per day, about 7,200 metered requests at the documented 20:1 ratio. Watchdog alarms add up to 1,440 per day, plus connections, admin traffic, configuration changes and live logs. There are no Mac replication replies or heartbeats. Agent WebSocket messages do not each create a new outer Worker HTTP request. Legacy HTTP agents temporarily count once per report, so use the configured 60-second interval while upgrading.

One continuously resident DO at the 128 MB billing size uses approximately 11,059 GB-s/day, within the 13,000 GB-s/day free allowance. For a 31-day month, that is about 342,835 GB-s, below the paid plan's included 400,000 GB-s. At 100 updated agents, reports plus minute alarms use about 267,840 metered DO requests over 31 days, before other traffic, below the paid plan's included one million. These are estimates for this controller, not an account-wide spending guarantee.

Allowances are shared by other account workloads. The current design fits its expected control traffic; unusually heavy public API use or log streaming can consume the remaining request budget. Source/version growth also needs monitoring against D1 storage and Worker memory limits; the initial migration contains 166 versions and about 18 MB of source. Older history is preserved, not silently pruned. Stored Workers observability logs are explicitly disabled; runtime status and on-demand diagnostics remain available.

Workers Paid has a $5 monthly minimum plus metered overages. Cloudflare does not provide a hard $5 cap for this combination of Workers, DO and D1. Budget alerts notify after their threshold is crossed; they do not stop usage. Per-invocation CPU limits, rate limits and application-level cutoffs can reduce exposure but cannot guarantee a fixed bill, since rejected Worker requests still count and other account resources share the allowances. A strict no-overage requirement is met by keeping these services on Workers Free, which rejects operations after its limits are reached.

Startup loads small configuration records in pages of 100 and source versions in pages of 10. A regression check restores 300 tunnel slots and 250 saved versions within the free limit of 50 D1 queries per invocation. Routine reports still use only the memory cache.

References: [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/), [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/), [Workers limits](https://developers.cloudflare.com/workers/platform/limits/), [D1 pricing](https://developers.cloudflare.com/d1/platform/pricing/), [budget alerts](https://developers.cloudflare.com/billing/manage/budget-alerts/).
