# Control plane tests

Scenario scripts that run the control plane locally (`node server.mjs`) against a fake Cloudflare API
(`cloudflare-mock.mjs`, which answers tunnel, DNS and route calls), then drive it with curl as the agents and the
portal would. Each prints what happened; read the output.

```sh
control/test/placement.sh   # replicas and placement by capacity
control/test/moves.sh       # moving replicas, evicting a machine
control/test/rebalance.sh   # the hot rule over minute summaries, the cooldown, switching it off
control/test/replicas.sh    # replica numbers, their routes and DNS names
control/test/pools.sh       # pool sizes, who starts machines, draining by agent ID
control/test/auth.sh        # what needs an app's password, the admin password, or nothing (prints ok/WRONG)
control/test/clashes.sh     # port and container-name clashes in placement, replica-only deploys, the generated compose, the join guard (ok/WRONG)
control/test/doubling.sh    # more replicas than machines: second copies with moved ports, their routes, moves by replica (ok/WRONG)
control/test/env.sh         # an app's env variables and each copy's .env
control/test/gap.sh         # a gap in check-ins isn't machines dying
control/test/logs.sh        # live logs: passwords, the wake-up, the agent's stream to the viewer, hidden env values (ok/WRONG)
```

They need Node 24 (`node:sqlite`), `jq` and `curl`, and use ports 8911 (control plane, started by `start.sh`) and 8790 (mock).

## Simulations

`sim/*.test.mjs` run the real control plane in-process (SQLite in memory, a fake Cloudflare API) on a fake clock,
against a simulated fleet (`sim/lib.mjs`): servers that join, check in every 20 seconds, build what they're told, fail
or crash when a scenario says so, report minute summaries (CPU, steal, memory, per app), disk and restarts, start the
servers their pool asks for within their account's job cap, hand over, and stop at their deadline. Hours of fleet life
take seconds. Each prints ok or WRONG per check and exits non-zero on a WRONG.

```sh
node control/test/sim/phase1.test.mjs   # gaps and mass silences, stable copy keys, moves that keep the old copy, events
node control/test/sim/phase2.test.mjs   # every app alike, readiness, replica changes, placement filters, budgets
node control/test/sim/phase3.test.mjs   # scheduled departures and their gates, same-slot handovers, capped pools, losses
node control/test/sim/phase4.test.mjs   # failing copies, sick servers, the hot rule, the thrash breaker, moves in flight
node control/test/sim/phase5.test.mjs   # dark and lit replicas, cold departures, churn, wait reasons, the alert webhook
```
