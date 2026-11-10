# Provider adapters

Machine supply is independent of app scheduling. The controller accepts machines
that authenticate and report a required `SERVER_TAG`; tags describe origins and
do not declare desired machine counts. Keep account APIs, quotas and targets in
provider adapters. The same controller and agent work with other Linux providers.

## GitHub Actions

Each participating repository has `machine.yml` and `watchdog.yml` workflows.
The watchdog supplies missing capacity within that repository's target and
concurrency constraints. `machine.yml` starts one Ubuntu Docker host and runs the
agent directly in the job. It supplies a lifetime and a replacement command so
an expiring host can request its own successor. Apps still run in Docker.

Configure each repository independently:

| Repository setting | Purpose |
| --- | --- |
| Variable `CONTROL_URL` | The same stable HTTPS controller origin for the fleet. |
| Secret `JOIN_TOKEN` | The same fleet join token; never a public variable. |
| Variable `SERVER_TAG` | Distinct origin tag, such as `github`, `school` or `secondary`. |
| Variable `MACHINE_COUNT` | Desired capacity owned by this provider adapter. |
| Variable `AGENT_REPO` | Optional source repository, such as `hetp4401/runner`; defaults to this repository. |
| Variable `REGION` | Optional explicit location label. |

`POOL` and `POOL_SIZE` are compatibility aliases. The job's GitHub token stays
in the provider process environment for its replacement command; agent source
contains no account-specific logic. Machine jobs currently have a six-hour
workflow timeout and report a 355-minute lifetime. GitHub account availability,
quotas and workflow limits still govern whether replacement capacity can start.

For multiple accounts, configure each provider repository with its own target
and tag, while sharing only the fleet origin/join identity. A controller move
between VPS, native Mac and Docker requires no provider URL change when that
public origin remains stable.

## Optional watchdog fallback

`.github/scripts/capacity-fallback.py` runs independently of the controller. It
reads current targets from repository variables, observes connected counts and
dispatches the existing serialized watchdog when capacity is below target. It
waits through controller recovery and avoids dispatching while a watchdog is
already active. This is a provider adapter, not part of the scheduler.

Example private configuration (replace paths and repository names):

```json
{
  "control_url": "https://runners.example.com",
  "providers": [
    {"login": "your-account", "repo": "your-account/runner"}
  ],
  "credential_sources": ["/absolute/private/github-credentials.json"],
  "interval_seconds": 300
}
```

Keep the credential file outside the checkout with mode 0600. It can contain a
JSON mapping from login to token. Tokens need access to the relevant repository's
variables and workflow dispatch.

Status checks use the stable control origin by default, with an explicit
client identity accepted by the public edge. An optional `status_url` overrides
the endpoint for private deployments. Do not use a replica's localhost address
when the helper must keep working after that replica is stopped.

Test without dispatching:

```sh
python3 .github/scripts/capacity-fallback.py \
  --config /absolute/private/provider.json \
  --state /absolute/private/provider-state.json --once --dry-run
```

Omit `--once --dry-run` for a supervised adapter. On the Mac it may remain a
separate LaunchAgent using the stable public controller origin. Do not put
these credentials in the controller image, app environment or public logs.
