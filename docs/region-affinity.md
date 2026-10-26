# Region affinity

An app can keep all its replicas in a broad geographic region:

```yaml
x-runner:
  port: 8080
  replicas: 3
  region: us-west
services:
  web:
    image: traefik/whoami
    command: ["--port", "8080"]
    ports: ["8080:8080"]
```

The supported groups reflect the current US fleet:

| Spec value | Current locations | Runners observed October 25, 2026 |
| --- | --- | ---: |
| `us-west` | California, Arizona, Washington, Wyoming | 17 |
| `us-central` | Iowa, Illinois | 7 |
| `us-east` | Virginia (`eastus` and `eastus2`) | 7 |

Counts change as jobs are replaced. `GET /api/status` reports `regions` with current machine counts, `runs[].region`, and each app's `region`. Handover pairs count as one machine. The deploy guide shows the current counts, and the app page shows its region.

To keep one of three replicas in each region, use a region list instead of a single `region`:

```yaml
x-runner:
  replicas: 3
  regions: [us-west, us-central, us-east]
```

Replica 1 is assigned to West, replica 2 to Central and replica 3 to East. A list repeats when the count grows: replica 4 would be in West, while existing replica assignments stay the same. Set either `region` or `regions`; specifying both is rejected. The app's status includes `regions`, and every placement includes its required `region`. Moves, eviction, rebalancing and same-slot replacements use each replica's assignment. If one region has no capacity, its replicas wait while the other regions' replicas can still be placed.

The control plane derives the group from the state and country in its existing cached IP geolocation. It does not pin a city, pool, Azure datacenter or numbered machine slot. The full US state mapping lives in `control/src/regions.js`; locations outside the supported groups or not yet established have a null region. A fresh runner with an unknown location waits for the lookup before receiving a constrained app. Apps without `x-runner.region` can use the entire fleet.

Affinity is strict. New placements, replacement copies, rebalancing, manual moves and evictions all stay in the selected region. A region with no usable capacity leaves copies waiting with an explanation on the app page. If the region's machines already run a copy, more copies can share a machine using the existing port remapping rules; a Compose stack that cannot share waits instead. Unknown region names are rejected before a deployment is saved.

Departing, draining, quarantined and already-due runners remain excluded. The scheduler prefers runners with at least 30 minutes before the earlier of their scheduled departure and hard deadline. If every usable runner in the region is nearer departure, it can use one of those rather than leave a copy unplaced. Region affinity does not extend a machine's job lifetime.

When a replacement VM reuses a slot in another region, it cannot inherit the constrained app. The control plane starts another copy in the required region and retains the old serving copy until the new one is healthy and DNS has followed. The successor's tunnel waits for that move, so it cannot replace the old machine's routes prematurely. If no replacement capacity exists, the old copy serves while its machine can stay up; its eventual hard deadline can still cause an outage.

Adding or changing an app's region moves existing copies through the normal transition budget. Their URLs and replica numbers remain stable. A region change also cancels a move already headed to the wrong region. Removing the setting permits future placements throughout the fleet.

Changing only the replica count or the private app environment preserves the region because it is part of the versioned Compose specification. Rollback restores the selected version's region along with its files. Existing deployment credentials apply; no agent update or additional credential is required.

Run the region scenarios with:

```sh
node control/test/sim/regions.test.mjs
```

They exercise grouping, validation, unknown locations, capacity shortages, fixed container names, manual moves, scheduled departures, eviction, region changes, restart persistence and cross-region handovers with continued readiness.
