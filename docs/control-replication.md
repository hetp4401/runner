# Replicated control plane

The same Docker image runs alone or as a member of a Raft cluster.
`CP_REPLICAS=1` runs the ordinary controller without elections or peer traffic.
Replicated mode accepts an odd number of at least three. Two and all even
values are rejected. Application replica counts are independent.

Coordination is entirely in memory. The implementation uses
[`go.etcd.io/raft/v3`](https://github.com/etcd-io/raft), pre-voting, quorum checks,
and quorum-confirmed ReadIndex requests. D1 stores application configuration;
it does not store terms, votes, membership changes, authority, transitions, or
Raft log entries. No Cloudflare service beyond the existing D1 configuration
store, tunnels and DNS is required.

| Configured slots | Votes required | Unavailable voters tolerated |
| --- | --- | --- |
| 3 | 2 | 1 |
| 5 | 3 | 2 |
| 7 | 4 | 3 |

Count physical failure domains as well as containers. With three Mac copies and
four fleet copies, the fleet can continue without the Mac while all four fleet
voters remain available. Losing all four fleet copies leaves three votes and
pauses control changes. Existing agent workloads keep running. Membership never
shrinks automatically to work around quorum loss.

## Configuration and first startup

Copies share the fleet's configuration credentials, tunnel namespace and:

- `CP_REPLICAS`: total logical member slots, including external copies.
- `CP_CLUSTER_ID`: stable identifier, 16–100 letters, digits, underscores or dashes.
- `CP_CLUSTER_TOKEN`: private random secret of at least 32 characters.
- `CP_MEMBERS`: JSON array of `{ "id": 1, "url": "https://cp-1.example.com", "label": "Host A" }`. IDs and HTTP(S) origins must be unique.
- `CP_NODE_ID`: this copy's logical member slot.
- Existing D1, Cloudflare, admin, join-token and receipt-signing settings.

These are ordinary deployment settings. An app definition may contain its peer
addresses in D1, just as it contains other application environment settings;
no changing consensus state is saved there.

Peer origins must differ from the primary hostname. External copies use
persistent named Cloudflare Tunnels. Fleet copies use their normal numbered app
domains with `CP_PEER_TUNNEL=0`. Each copy also has a tunnel that can publish the
shared primary hostname. `CP_PEER_OVERRIDES` optionally maps logical IDs to
private networking origins without changing the public member list.

A fresh process starts without a vote. If a surviving cluster has quorum, it
admits that process automatically. If this is the first cluster startup, start
at least a quorum of copies and ensure every previous scheduler has stopped.
Then, using the private deployment environment, run:

```sh
node control/cluster-init.mjs status
node control/cluster-init.mjs bootstrap --confirm-old-cluster-stopped
```

The tool creates a manifest only in memory and distributes it to the fresh
processes. Unavailable slots receive placeholder identities; the surviving
quorum can replace them when their processes appear. Initialized members refuse
a different bootstrap generation. Never bootstrap across a network partition
or while an old cluster can still act. Do not run a standalone scheduler beside
a replicated cluster.

The initial term defaults to the current Unix time in milliseconds. After a
clock rollback, set `CP_BOOTSTRAP_TERM` above the highest epoch still remembered
by agents. Normal elections increase the term without consulting wall time.

## Memory-only safety and recovery

Ordinary Raft requires durable term and vote state when a node restarts with
the same identity. This implementation never restarts a forgotten voter: every
process creates a new random physical Raft identity and boot token. The logical
slot and stable hostname remain the same.

A new process first joins as a non-voting learner. After it catches up, the
leader replaces the previous physical identity through joint consensus. Both
the old and new voting majorities must agree before the replacement is ready.
A leader must confirm a live quorum before accepting a join request, including
during the interval before an isolated leader notices it has lost authority.
A removed incarnation stops acting and cannot rejoin in that process. Application
commands are not replicated through this Raft group; its compacted protocol
state and membership snapshots are held in RAM.

This deliberately favors exclusive authority over automatic recovery:

- A surviving quorum can admit restarted copies and perform planned handoffs.
- If fewer than a quorum retain their memory, new processes cannot restore the
  missing votes by themselves. Restarting a container does not recover its vote.
- Complete cluster memory loss requires stopping every old incarnation and
  explicitly bootstrapping a fresh generation. History is lost. Do not lower the
  quorum or reset a minority while another partition might still be active.

## Fleet copies are an ordinary app

Use [the example Compose app](../examples/control-replicas/compose.yaml). It uses
normal replicas, private environment, placement, readiness, moves, and container
shutdown. There are no controller-specific GitHub jobs or scheduling helpers.

The normal `FLEET_REPLICA` variable identifies each app replica.
`CP_NODE_ID_OFFSET=3` maps app replicas 1–4 to logical slots 4–7; slots 1–3 may
run elsewhere. The offset is deployment configuration, not a built-in Mac
topology. Do not set a fixed `CP_NODE_ID` in the shared app environment.

Use an image built from this repository, or a standard Compose remote Git build
context pinned to a full commit. Keep credentials in the private app environment.
`x-runner.spread-tags: true` asks placement to prefer different machine tags on
a best-effort basis; see [placement preferences](region-affinity.md).

During an ordinary app move, the replacement starts while the old copy is still
running. It exchanges Raft messages through bounded in-memory peer mailboxes
until the stable app domain moves to it. `/readyz` returns 200 only after the
learner has become a voter and joint consensus has finished. The old incarnation
has already lost authority before the scheduler removes its container. Moving
a primary first transfers leadership to another voter. A 30-second container
stop grace period allows graceful shutdown.

`/healthz` is a separate liveness check: a fresh process or learner is alive even
when it cannot vote. It must not be repeatedly restarted just because quorum is
unavailable. `CP_WARM_REPLACEMENT` is no longer needed.

A planned handoff works with exactly a quorum when both old and new processes
remain available until promotion. A hard failure that leaves fewer than a quorum
requires operator recovery. The control-plane app is intentionally a recursive
dependency: a quorum can schedule replacements; a minority cannot create votes.

Ordinary app source updates currently replace all copies in parallel. With the
Mac copies stopped, a fleet-wide source upgrade loses the remaining cluster
memory. Agents receive their plans independently: the first updated copy can
stop before the remaining agents receive the new version. A replacement may
also retain a joining state from the old generation, so waiting for every copy
to become uninitialized is not a recovery procedure.

Before updating a self-hosted controller app, prepare independent access to its
hosts and enough recovery compute to form a quorum. Treat the update as planned
maintenance: stop/fence the previous cluster, restart every recovery process
with fresh memory, then explicitly bootstrap. Never reset a minority while an
old quorum can still act. Return the ordinary fleet copies through the normal
learner admission process before removing temporary recovery capacity. Do not
expect a rolling upgrade from a normal app source update.

## External copies and Mac supervision

`control/compose-replicas.mjs` generates a normal Compose deployment from a JSON
file with `environment_file`, `image`, and local `members`, for example
`[{ "id": 1, "port": 8920 }]`. Each copy gets a loopback port and a volume for
ordinary fleet identity credentials. That volume contains no Raft vote state.
The optional `dns` array configures ordinary container resolvers. The Mac
supervisor supports `compose_file` and `services`; its default is the standalone
`control` service. Explicitly stopped external copies stay stopped during fleet
maintenance unless the operator chooses to restore them.

## Authority and routing

Only the primary starts a scheduler child. Scheduling, agent plans, API changes,
and Cloudflare effects require fresh quorum confirmation. Leadership loss stops
the child and closes its agent WebSockets. Standbys forward dashboard/API traffic
to the primary, including traffic arriving through old DNS routes. The primary
reconciles the existing shared hostname to its tunnel.

The D1 I/O worker confirms authority immediately before submitting each
configuration transaction. The transaction also checks the ordinary application
configuration revision atomically, so a stale edit cannot overwrite a newer
commit. No leadership record is written to D1. Agents verify plan authority
through the stable control origin and remember the highest accepted term/epoch.
They reject obsolete or missing proofs.

A request authorized before a handoff may finish afterward. Already submitted
D1 or Cloudflare HTTP requests cannot be recalled. Quorum guards prevent new
operations by a former primary; revision checks protect newer configuration
and the current primary reconciles external routing.

## History and inspection

Each copy retains the latest **30 primary transitions in memory** and exchanges
that history with peers. Nothing writes transition history to D1, SQLite, or a
history file. Full memory loss clears it. The runtime `rlog` is a bounded tail
of at most 2,048 entries / 8 MiB, with a separate 8 MiB latest-report cache. Peers
exchange observations in memory and recover signed inventory from agents.
There are zero periodic D1 coordination writes.

The Runners **Fleet** tab (`/#/fleet`) shows replica health and transitions using
the panel's existing theme. Every copy serves the shell and `/api/cluster`
independently, including during elections and quorum loss. The view shows peer
reachability, current leader, term, voter readiness, and runtime-tail size.
Peer links open that replica's perspective. `/_cp/*` requires the cluster secret;
raw runtime reports are not public. Old `/cluster` links redirect to Fleet.

## Verification

With Go 1.24 or newer and Node 24:

```sh
go -C control/raft build -o /tmp/runners-raft-memory-test .
node control/test/raft-memory.mjs
node control/test/ha-service.mjs
node control/test/ha-setup.mjs
node control/test/d1-store.mjs
```

Tests cover explicit bootstrap, seven-member elections, partitions, quorum loss,
new identities after restart, learner promotion, replacement with only four
available voters, primary handoff, stale configuration and agent rejection,
standby forwarding, and the 30-transition limit.
