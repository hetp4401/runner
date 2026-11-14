# Replicated GitHub provider

`providers/github/` is an optional, ordinary Docker application that replenishes
GitHub-hosted machines. It owns provider credentials and capacity targets;
neither the control plane nor the generic agent calls account APIs. Other
compute providers can continue supplying the same fleet independently.

The provider reads each repository's target, inventory, interval and phase. A
primary may request its existing serialized capacity workflow once per fixed UTC
interval. Set `CAPACITY_INTERVAL_MIN=2` for prompt recovery: each account can add
one missing host every two minutes, without launching a whole cohort together.
Pending jobs reserve capacity. Once the target is reserved, refill stops; aging
hosts permit one overlap above the account target.

## Deployment

Build from the repository root:

```sh
docker build -f providers/github/Dockerfile -t your-registry/github-provider:VERSION .
```

Use [the ordinary Compose app](../examples/github-provider/compose.yaml). Its
seven fleet replicas use `FLEET_REPLICA` as their logical IDs. The standard
`x-runner.spread-tags: true` preference spreads copies across origin tags when
possible. It is a preference: seven copies across three tags necessarily share
some tags. No Mac installation, provider LaunchAgent or special fleet placement
code is needed.

Supply these values in the app's private environment:

| Setting | Meaning |
| --- | --- |
| `REPLICAS` | `1`, or an odd count of at least three. Seven needs four votes. |
| `MEMBERS` | JSON array of `{ "id": 1, "url": "https://provider-1.example.com" }`, one distinct origin per logical replica. |
| `CLUSTER_ID` | Stable identifier of 16–100 letters, digits, underscores or dashes. |
| `CLUSTER_TOKEN` | Private random peer credential of at least 32 characters. |
| `GITHUB_PROVIDERS` | Private JSON array of `{ "repo": "owner/repository", "token": "..." }`. Tokens need repository variables read and Actions write access. |
| `NODE_ID` | Optional explicit ID for an independently hosted copy; fleet copies use `FLEET_REPLICA`. |
| `NODE_ID_OFFSET` | Optional offset for fleet IDs when some logical slots run elsewhere. |
| `PORT` | HTTP port, default `8924`. |

Existing repository settings remain `CONTROL_URL`, `JOIN_TOKEN`, `SERVER_TAG`,
`MACHINE_COUNT`, and optional `AGENT_REPO`. Add `CAPACITY_TRIGGER=provider` and
`CAPACITY_PHASE_MIN` from `0` through `9`; distinct phases such as `2`, `5`, `8`
interleave account requests. `CAPACITY_INTERVAL_MIN` accepts `1`, `2`, `5`, or
`10` (default `10`). The legacy phase setting scales with the interval: at two
minutes, phases `2`, `5`, `8` open at seconds `24`, `60`, `96` of each interval.
The whole interval remains eligible, so launch or polling delays do not close a
narrow window. A half-interval minimum between actual machine requests prevents
late adjacent slots from bunching, without shifting every later phase. Workflows
recheck inventory and reject requests more than ten minutes old, allowing bounded
queue delay. In provider mode, cron does not launch hosts.
Removing provider mode restores normal repository-owned scheduled supply.
Manual capacity workflow runs with an empty slot remain previews. Credentials
are never returned by the provider's status API or passed to machine jobs.

## Consensus and duplicate attempts

The provider reuses the control plane's etcd Raft core, with a separate cluster
identity and an opt-in monotonic work-claim operation. Control-plane election
behavior and application configuration storage are unchanged.

Before a GitHub write, the primary confirms a fresh quorum and commits a claim
for `(repository, UTC interval slot)`. Only a sequence newer than that
repository's last claim can succeed. A second quorum confirmation checks the
same primary term before submitting the request. A new primary receives the
claim in its Raft state and cannot retry that slot. Requests already accepted
by GitHub before a handoff can finish; they cannot be recalled.

Claims, votes, membership, and the latest 30 transitions stay in memory. There
are no GitHub claim tags, D1 coordination records, local state files or backups.
The ordinary app definition and private configuration use normal deployment
storage. GitHub retains its ordinary workflow history. The app is not globally
stateless: its coordination is replicated in RAM.

This provides at-most-once attempts within a surviving cluster, not guaranteed
exactly-once delivery to an external API. A crash between committing a claim and
sending the request skips that attempt; the next interval can replenish capacity.
A lost HTTP response is not retried under the same claim. The worker also checks
GitHub's prior request titles to avoid replay after an explicit cold bootstrap.
Machine jobs reject unrelated origins, and capacity workflow reruns cannot
request another host.

## Startup, moves and recovery

Fresh processes are live but nonvoting. Once at least four of seven are reachable,
bootstrap this new cluster using its private environment:

```sh
node --env-file=/private/provider.env providers/github/bootstrap.mjs status
node --env-file=/private/provider.env providers/github/bootstrap.mjs bootstrap --confirm-old-cluster-stopped
```

Only use bootstrap after ensuring every previous incarnation is stopped. A
surviving quorum admits restarted processes as new learners and replaces their
old identities through joint consensus. `/readyz` becomes successful only after
promotion; `/healthz` stays successful while waiting for quorum. Normal fleet
moves can therefore start the replacement before removing its predecessor.

With all seven copies on the fleet, a complete fleet outage also removes the
provider. Recovery then requires externally starting compute and restoring the
control-plane and provider quorums. A minority must not bootstrap itself or
shrink membership to bypass this requirement. Losing all consensus memory also
requires explicit bootstrap. Ordinary fleet app version changes currently
restart all copies, so treat provider source/environment upgrades as coordinated
maintenance, not rolling updates. Keep independent access to bootstrap it again.

Every replica exposes `/api/status` and `/api/cluster`, including peer health,
the primary, latest decisions and transitions. Private `/_cp/*` endpoints need
the cluster credential; `POST /_cp/transfer` requests a graceful primary handoff.

## Verification

With Node 24 and Go 1.24 or newer:

```sh
node --test .github/scripts/capacity.test.mjs providers/github/scheduler.test.mjs
go -C control/raft build -o /tmp/runners-raft-provider-test .
CP_RAFT_ALLOW_CLAIMS=1 RAFT_TEST_BINARY=/tmp/runners-raft-provider-test node control/test/raft-memory.mjs
node providers/github/service.test.mjs
node control/test/ha-setup.mjs
```

These cover duplicate claims, stale primaries, lost responses, snapshots on voter
replacement, phased supply, workflow validation, seven ready app replicas and
leadership transfer through the app's actual HTTP endpoints.
