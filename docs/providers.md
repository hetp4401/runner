# Provider adapters

Machine supply is independent of app scheduling. The controller accepts machines
that authenticate and report a required `SERVER_TAG`; tags describe origins and
do not declare desired machine counts. Keep account APIs, quotas and targets in
provider adapters. The same controller and agent work with other Linux providers.

## GitHub Actions

Each participating repository has `machine.yml` and `watchdog.yml` workflows.
Only scheduled watchdog runs launch machines. The watchdog reads its own
repository's Actions inventory and target; it does not call the control plane.
Manual watchdog runs are previews, and machine completion does not trigger a
refill. There is no external provider service on a Mac or VPS.

`machine.yml` starts one Ubuntu Docker host and runs the generic agent directly
in the job. Apps still run in Docker. Machine jobs verify the requesting
`schedule_run` belongs to this repository's scheduled watchdog on the same
branch. They have read-only Actions permission and supply no `START_CMD` or
GitHub token to the agent. Existing hosts finish their normal drain; their old
direct handover dispatches cannot start new jobs through this workflow.

Configure each repository independently:

| Repository setting | Purpose |
| --- | --- |
| Variable `CONTROL_URL` | The same stable HTTPS controller origin for the fleet. |
| Secret `JOIN_TOKEN` | The same fleet join token; never a public variable. |
| Variable `SERVER_TAG` | Distinct origin tag, such as `github`, `school` or `secondary`. |
| Variable `MACHINE_COUNT` | Desired capacity owned by this provider adapter. |
| Variable `AGENT_REPO` | Optional source repository, such as `hetp4401/runner`; defaults to this repository. |
| Variable `REGION` | Optional explicit location label. |

`POOL` and `POOL_SIZE` are compatibility aliases. The watchdog uses its own
repository-scoped GitHub token; no cross-account credentials are needed.
Machine jobs have a six-hour timeout and report a 355-minute lifetime. The
control plane schedules app evacuation before the deadline using its ordinary
departure gates. Agent and controller source contain no account-specific logic.

For multiple accounts, configure each provider repository with its own target
and tag, while sharing only the fleet origin/join identity. A controller move
between VPS, native Mac and Docker requires no provider URL change when that
public origin remains stable.

## Interleaved schedules

Put `watchdog.yml` on each repository's default branch and enable its Actions
workflow. Give accounts different minute offsets, for example:

| Account | UTC cron | Minutes within each hour |
| --- | --- | --- |
| First | `1-59/10 * * * *` | 01, 11, 21, 31, 41, 51 |
| Second | `4-59/10 * * * *` | 04, 14, 24, 34, 44, 54 |
| Third | `7-59/10 * * * *` | 07, 17, 27, 37, 47, 57 |

Each reconciliation starts at most one host, with at least ten minutes between
launch requests even if scheduled runs arrive together. Failed launches count
toward that spacing. Pending jobs reserve capacity. A host running for at least
210 minutes permits one extra host above the target to warm before departure;
the provider never cancels healthy jobs to enforce its target. Actual running
and pending jobs are capped at target plus one for new launch decisions,
including during migration from overlapping legacy handovers.

This spreads replenishment over time instead of creating a whole cohort with
the same deadline. An empty pool deliberately ramps up one host per interval.
The generic control plane still staggers departures and moves app replicas to
available hosts before retiring a machine. New scheduled hosts pick up source
updates naturally; `roll.yml` is explicit operator maintenance only.

[GitHub schedule events can be delayed or dropped](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).
Offsets and overlap provide headroom, not exact-time guarantees. A prolonged
GitHub scheduling interruption can reduce capacity; no external fallback
dispatches jobs. Account quotas and workflow concurrency limits also apply.

Use **Run workflow** on the watchdog to preview the account's plan without
launching machines. Run its regression checks with Node.js 24:

```sh
node --test .github/scripts/capacity.test.mjs
```
