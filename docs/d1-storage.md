# Cloudflare D1 configuration storage

The Mac controller runs in Docker and uses D1 with `CONFIG_STORAGE=d1`. The
scheduler, HTTP API and tunnel stay on the Mac. This does not deploy the archived
Worker/Durable Object controller or require a new Cloudflare Worker.

D1 stores `projects`, `versions`, durable `settings`, `app_env` and
`app_passwords`. These hold app specs and source history, replica counts, rollout
choices, environment secrets and password hashes. `runner_meta` records the
schema version, controller origin, readiness and commit revision. Cloudflare
credentials, the admin/join/signing secrets and fleet namespace stay in the
private environment and `identity.json`.

At startup the CP loads D1 into an in-memory SQLite working database and verifies
referenced versions. Runtime tables, heartbeats, placements, health, metrics,
events, discovered tunnels and cooldowns stay in RAM. Normal fleet traffic and
unchanged settings make no D1 requests. Changed configuration rows commit as one
D1 transaction before the API acknowledges the edit. A worker thread handles
HTTPS while the scheduler waits; configuration edits can briefly pause request
processing. Network requests have a 30-second timeout.

A revision guard prevents a stale controller from overwriting newer config.
A unique commit ID resolves a lost response without replaying an edit. If the
commit cannot be confirmed, the process refuses further plans and restarts from
D1. A missing, incomplete, wrong-origin or unavailable D1 fleet prevents startup;
there is no local fallback or empty-fleet initialization. Existing agents keep
their last approved workloads. Run exactly one scheduler; the revision guard is
not an active/active scheduling protocol.

## Private configuration

Create a D1 database in the existing Cloudflare account. Keep these values in the
owner-only environment file used by Compose; never commit real credentials:

```dotenv
ACCOUNT_ID=your-account-id
D1_DATABASE_ID=your-database-id
D1_API_EMAIL=your-cloudflare-account-email
D1_API_KEY=your-global-api-key
CONFIG_STORAGE=d1
```

`D1_ACCOUNT_ID` optionally overrides `ACCOUNT_ID`. D1 authentication uses the
Global API Key with `X-Auth-Key` and `X-Auth-Email`; it can also use
`CF_API_KEY`/`CF_API_EMAIL` if the D1-specific values are omitted. The tunnel/DNS
credential can remain separately configured as `CF_API_TOKEN`.

## Move an existing SQLite fleet

Build and test the new image first. Keep the existing source database until the
D1 controller has been verified. The migration reads it in one SQLite transaction
and sends rows directly to D1 without creating export files.

1. Stop the controller and its automatic supervisor. Agents keep serving their
   approved workloads. Preserve the original public origin, admin/join/signing
   secrets, namespace and `identity.json`.
2. Supply the D1 credentials and database ID in the private Compose environment.
3. Run the migration from the repository/release directory, with the existing
   data volume. It refuses an active destination, verifies row counts and SHA-256
   contents for every configuration table, then marks D1 ready:

   ```sh
   docker compose run --rm --no-deps --entrypoint flock control \
     --exclusive --nonblock /data/controller.lock \
     node d1-migrate.mjs /data/control.db
   ```

4. Set `CONFIG_STORAGE=d1` and start the single controller. Confirm
   `/api/status` reports `controlPlane.configStorage=cloudflare-d1`, verify admin
   access and app versions, then let agents reconnect through the recovery hold.
5. After verification, remove the old `/data/control.db`, `control.db-wal` and
   `control.db-shm`. Restart once more and verify D1 recovery without those files.
   Keep `identity.json`, `controller.lock` and the private environment.

A failed initial upload can be retried only while its destination is unready,
revision zero and bound to the same controller origin. `--verify` compares the
source against D1 without changing either. New fleets can run the usual local
`init`, then follow this migration before their first D1 startup. Do not run
`init` to recover an existing fleet. D1 migration is offline; the legacy HTTP
import endpoint is not a D1 migration tool.

## Usage and limits

As verified November 9, 2026, D1 Free includes 5 million rows read and 100,000 rows
written per day, 5 GB account storage, and a 500 MB per-database limit. Index
maintenance also counts toward writes. Exhausting a daily quota causes queries
to fail until reset at 00:00 UTC. See Cloudflare's
[pricing](https://developers.cloudflare.com/d1/platform/pricing/) and
[limits](https://developers.cloudflare.com/d1/platform/limits/).

The migrated fleet has 14 projects, 241 source versions, 20 environment entries,
6 app password hashes and 2 durable settings, around 30 MB. Runtime traffic does
not consume D1 row quotas. Configuration edits write only changed rows plus the
commit metadata. Startup reads the configuration once in indexed pages.
`GET /api/usage` with admin authentication includes `configuration` counters for
this process's D1 requests, rows read/written and committed revisions. These
counters are in memory; they are not a monitoring job or account-wide billing
report. Cloudflare's dashboard is authoritative for account usage.
