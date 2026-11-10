# Replicated control plane

The same Docker image can run alone or as a Raft voter. `CP_REPLICAS=1` is
the ordinary single-controller service: no election process, peer traffic or
coordination writes. Replicated mode accepts an odd number of at least three;
two and all even values are rejected. Application replica counts are independent.

Raft uses `go.etcd.io/raft/v3` with pre-voting, quorum checks and quorum-confirmed
ReadIndex requests. Membership is explicit and fixed for a cluster generation.
Starting another copy does not replace a healthy primary or lower the quorum.

| Voters | Votes required | Voter failures tolerated |
| --- | --- | --- |
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

Count physical failure domains as well as containers. Three Mac containers plus
four fleet containers need four voters to make changes. The fleet can continue
without the Mac only while all four fleet voters are available. Losing all four
fleet voters leaves three votes and pauses the control plane. Existing agent
workloads keep running. Membership never shrinks automatically after failures.

## Configuration and activation

Replicated mode currently requires the existing D1 configuration store. All
copies share the fleet's identity credentials, tunnel namespace and these private
settings:

- `CP_REPLICAS`: total voting members, including externally hosted copies.
- `CP_CLUSTER_ID`: a stable, unique identifier, 16–100 letters, digits, underscores or dashes.
- `CP_CLUSTER_TOKEN`: a private random secret of at least 32 characters.
- `CP_MEMBERS`: JSON array of `{ "id": 1, "url": "https://cp-1.example.com", "label": "Host A" }` entries. IDs and HTTP(S) origins must be unique.
- `CP_NODE_ID`: this copy's positive integer identity.
- Existing D1, Cloudflare, admin, join-token and receipt-signing settings.

Peer origins must differ from the primary control hostname. External voters use persistent named Cloudflare Tunnels. Fleet voters can use their normal numbered app domains with `CP_PEER_TUNNEL=0`; the app owns and maintains those routes. Each voter also has a bundled tunnel that can publish the shared primary hostname. Local deployments may set
`CP_PEER_OVERRIDES` to an ID-to-origin JSON map for private container networking.
Public peer addresses and membership remain identical on every copy.

Run `node control/cluster-init.mjs prepare` with the intended environment.
This creates only coordination tables and leaves the cluster inactive. Start
the voters; they can elect and expose diagnostics without starting a scheduler.
Start every configured voter at least once so its durable identity exists. Stop the previous standalone scheduler, then run
`node control/cluster-init.mjs activate`. The primary starts the scheduler and,
after tunnel inventory recovery, points the existing control hostname at its
own tunnel. Do not run an uncoordinated standalone scheduler alongside it.

`deactivate` stops authorization of new work. It is an explicit maintenance
operation, not a way to rewrite membership. A different membership requires a
coordinated migration; changing the member list in one process is rejected.
For an address-only change, keep the same voter IDs and use `update-addresses`
while inactive, then roll out the matching private environment to all copies.

## Fleet copies are an ordinary app

Use [the example Compose app](../examples/control-replicas/compose.yaml). It has
normal app replicas, private environment, health checks, placement, moves and
container shutdown. There are no controller-specific GitHub jobs, provider
watchdogs, special scheduling rules or additional Cloudflare services.

The application receives the normal `FLEET_REPLICA` variable. Setting
`CP_NODE_ID_OFFSET=3` maps app replicas 1–4 to voters 4–7; voters 1–3 can run
elsewhere. The offset is deployment configuration, not a built-in Mac topology.
Do not put a fixed `CP_NODE_ID` in the app's shared environment. Its `CP_MEMBERS` entry can use that copy's stable numbered app domain; `CP_PEER_TUNNEL=0` preserves the app-managed DNS record.

Provide `CONTROL_IMAGE` for an image built from this repository, or replace
the example image with a standard Compose remote Git build context pinned to a
full commit. Store credentials through the ordinary private app environment,
never in public Compose/source files.

Set `x-runner.spread-tags: true` to prefer different machine origin tags.
This preference is best effort, applies only to apps that request it, and never
overrides a required region. It does not move healthy copies merely to improve
tag diversity. See [placement preferences](region-affinity.md).

A replacement starts before the old copy is removed. With
`CP_WARM_REPLACEMENT=1`, `/readyz` also recognizes a fully started replacement
waiting for its predecessor's voter identity. It does not vote yet. Once the
normal scheduler removes the old copy, that copy stops its scheduler, attempts
a leadership transfer and releases its identity. The replacement then loads the
same durable election state and joins. Set a 30-second container stop grace
period. Updated agents honor every app's configured stop signal and grace
period; older agents that force-remove a copy require identity-lease expiry.

A hard host failure can skip graceful handoff. An identity lease prevents a
second process from voting as the same member; crash takeover can wait up to
90 seconds. App readiness and voter readiness are separate fields in the API.
Regular app source updates use the existing app update behavior; upgrading all
four fleet copies together can temporarily lose quorum if only three external
voters remain. Plan upgrades while sufficient voters are available.

This is intentionally a recursive dependency. An existing quorum can schedule
replacement CP app copies. If fewer than a quorum survive, the scheduler cannot
create the voters it needs. Recovery requires restoring existing voter hosts,
or temporarily starting a missing voter with its normal image and private
configuration on an available host. Never reset its vote state or lower quorum.

## External copies and Mac supervision

`control/compose-replicas.mjs` generates a normal Compose deployment from a
JSON file containing `environment_file` (an absolute private path), `image`
and local `members` such as `[{ "id": 1, "port": 8920 }]`. Each copy gets an
independent identity volume and loopback port. An optional `dns` array selects ordinary DNS resolvers for these containers,
for example when the host resolver has cached missing, newly created peer names.
The existing Mac supervisor accepts optional `compose_file` and `services` settings; its default remains
the single `control` service.

## Leadership, routing and persistence

Only the primary starts a scheduler child. Scheduling, agent plans, API
operations and Cloudflare effects require a fresh quorum confirmation.
Leadership loss stops that child and closes its agent WebSockets. Standbys
forward ordinary dashboard/API traffic to the current primary, which also
handles connections reaching a former primary during DNS propagation.

D1 configuration transactions also check the current leadership epoch inside
the same atomic batch. Updated agents verify plan authority through the stable
control origin and remember their highest accepted epoch. A new primary fences
old configuration writes and obsolete plans. Work already accepted before a
handoff may finish; an external Cloudflare request cannot be recalled. The
primary periodically reconciles its hostname.

D1 stores desired configuration plus minimal election and fencing metadata:
term/vote, fixed membership, the compacted consensus boundary, pending empty
leadership-barrier index/term pairs, voter ownership and current authority.
No application commands, runtime reports, metrics or runtime log history are
written into the Raft protocol state.

The runtime `rlog` is a bounded memory tail (up to 2,048 entries / 8 MiB),
with a separately bounded 8 MiB latest-report cache. Peers exchange it in memory
and recover signed inventory from agents. Recent primary transitions are
bounded in memory too. A full restart can lose that history.

Stable operation renews seven voter identity leases every 30 seconds and one
authority fence every five seconds: about 37,440 updated rows/day before
elections and configuration edits. These coordination writes are new in HA
mode; ordinary runtime reports still do not write to D1. D1/API unavailability
eventually stops authority rather than permitting concurrent schedulers.

## Inspection and verification

Every replica serves `/cluster` and `/api/cluster` independently of the primary:
its view of each peer, reachability, leader, term, runtime-tail size and recent
primary transitions. Each peer link opens that peer's own perspective.
`/_cp/*` requires the cluster secret; raw runtime reports are not public.

Build the Go test binary with `go -C control/raft build -o /tmp/runners-raft-test .`,
then run `node control/test/raft-cluster.mjs` and
`node control/test/ha-service.mjs` using Node 24. The tests cover seven-voter
elections, partitions, restart, duplicate identities, stale D1 writes, agent
fencing, standby forwarding and normal-app replacement handoff.
