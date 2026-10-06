#!/bin/bash
# Deploys (or updates) the control plane's fleet copies as the app "control", running this repo at REF (default: the
# commit checked out here, which must be pushed). With ENV=1 (the first time, or to change a setting) it also sets the
# app's env from this machine's ~/.config/runner-control/{env,tunnel.env} (all but PORT and DATA_DIR), plus STORE_URLS.
set -euo pipefail
cd "$(dirname "$0")"
URL=${RUNNER_URL:-https://runners.billybishop4-workers.xyz}
REF=${REF:-$(git rev-parse HEAD)}
ADMIN_PASSWORD=${ADMIN_PASSWORD:-$(cat ~/.config/runnerctl/admin-password)}
compose=$(sed "s/^        REF: .*/        REF: $REF/" compose.yaml)
body=$(jq -n --arg compose "$compose" --rawfile dockerfile Dockerfile --rawfile tunnel tunnel.yml \
  '{compose: $compose, files: {Dockerfile: $dockerfile, "tunnel.yml": $tunnel}, port: 8080}')
if [ "${ENV:-}" = 1 ]; then
  env=$(cat ~/.config/runner-control/env ~/.config/runner-control/tunnel.env | grep -vE '^(#|PORT=|DATA_DIR=|$)' \
    | jq -R -n '[inputs | capture("^(?<k>[A-Za-z_][A-Za-z0-9_]*)=(?<v>.*)$")] | map({(.k): .v}) | add')
  urls=$(seq 1 17 | sed 's|.*|https://zkmetadata-&.billybishop4-workers.xyz|' | paste -sd,)
  env=$(jq --arg u "$urls" '. + {STORE_URLS: $u}' <<<"$env")
  body=$(jq --argjson env "$env" '. + {env: $env}' <<<"$body")
fi
curl -sS -X PUT "$URL/api/projects/control" -H "x-admin-password: $ADMIN_PASSWORD" -H 'content-type: application/json' --data-binary "$body" \
  | jq -c '{name, version, replicas, env, placed: [.placed[]? | "r\(.replica)@m\(.machine)"], error}'
