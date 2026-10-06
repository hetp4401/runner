#!/bin/bash
# Live logs: watching them needs the app's password or the admin password (an app without one: the admin password);
# a viewer's WebSocket wakes the copy's server, its check-in names the session, and its agent's WebSocket streams
# through the control plane to the viewer, with the app's env values hidden; either side leaving ends both. The
# streaming half is logs-stream.mjs. Each line says ok, or WRONG.
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."
. "$HERE/stop.sh"
(node "$HERE/cloudflare-mock.mjs" > /tmp/runner-test-mock.log 2>&1 &)
. "$HERE/start.sh" ADMIN_PASSWORD=adm JOIN_TOKEN=node 'POOLS={"main":1}' QUIET_MS=0
for i in $(seq 1 30); do curl -sf localhost:8911/api/status >/dev/null && break; sleep 1; done
B=localhost:8911; START=$(date +%s%3N)
J=(-H content-type:application/json)
N=(-H 'authorization: Bearer node')
check() { if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  WRONG $1: wanted [$2], got [$3]"; fi; }
code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }
sync() { curl -s -X POST $B/api/sync "${N[@]}" "${J[@]}" -d "{\"machine\":1,\"run\":\"r1\",\"agent\":\"a1\",\"pool\":\"main\",\"starts\":false,\"started\":$START,\"ready\":true,\"status\":{\"web\":{\"v\":2,\"s\":\"healthy\"},\"locked\":{\"v\":1,\"s\":\"healthy\"}},\"metrics\":{\"live\":{\"t\":$(date +%s%3N),\"h\":{\"cpu\":10,\"memUsed\":20,\"memTotal\":100},\"a\":{}}}}"; }
curl -s -X POST $B/api/join "${N[@]}" "${J[@]}" -d '{"agent":"a1","pool":"main","want":1}' > /dev/null
curl -s -X PUT $B/api/projects/web "${J[@]}" -d '{"port":8080,"dockerfile":"FROM x"}' > /dev/null
curl -s -X PUT $B/api/projects/web/env "${J[@]}" -d '{"SECRET":"s3cret-value"}' > /dev/null
curl -s -X PUT $B/api/projects/locked "${J[@]}" -d '{"port":8081,"dockerfile":"FROM x","password":"lockpass"}' > /dev/null
sync > /dev/null; sync > /dev/null
check "both apps placed on server 1" "locked:1 web:1" "$(curl -s $B/api/status | jq -r '[.projects[] | "\(.name):\([.placed[].machine] | join(","))"] | join(" ")')"
echo "== nobody's watching: no logs asked for"
r=$(sync)
check "no logs in the plan" null "$(echo "$r" | jq -c .logs)"
check "the usual interval" 20 "$(echo "$r" | jq .poll)"
echo "== reading logs needs a password"
check "an open app's logs without a password" 401 "$(code -X POST $B/api/projects/web/logs?replica=1)"
check "with a wrong admin password" 401 "$(code -X POST $B/api/projects/web/logs?replica=1 -H 'x-admin-password: nope')"
check "a locked app's logs with a wrong app password" 401 "$(code -X POST $B/api/projects/locked/logs?replica=1 -H 'x-app-password: nope')"
check "the same through the web app's API" 401 "$(code -X POST $B/admin/api/projects/web/logs?replica=1)"
echo "== a locked app: its own password, or the admin password"
check "its password works" 200 "$(code -X POST $B/api/projects/locked/logs?replica=1 -H 'x-app-password: lockpass')"
check "and the admin password" 200 "$(code -X POST $B/api/projects/locked/logs?replica=1 -H 'x-admin-password: adm')"
check "a replica with no copy" 404 "$(code -X POST $B/api/projects/web/logs?replica=5 -H 'x-admin-password: adm')"
START=$START node "$HERE/logs-stream.mjs"
grep -i "error\|exception" /tmp/runner-test-dev.log | grep -v "Cloudflare API" | head -5
. "$HERE/stop.sh"
