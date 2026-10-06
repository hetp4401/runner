#!/bin/bash
# The hot rule over the agents' minute summaries: a server over 85% CPU in 4 of its last 5 minutes has the app making
# it hot moved to the server with the most room; the old copy goes once the new one is healthy; another hot server
# waits for the fleet's 10-minute cooldown; switching rebalancing off.
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."
. "$HERE/stop.sh"
(node "$HERE/cloudflare-mock.mjs" > /tmp/runner-test-mock.log 2>&1 &)
. "$HERE/start.sh" ADMIN_PASSWORD=adm JOIN_TOKEN=node 'POOLS={"main":3}' QUIET_MS=0 YOUNG_MS=0
for i in $(seq 1 30); do curl -sf localhost:8911/api/status >/dev/null && break; sleep 1; done
B=localhost:8911; START=$(( $(date +%s%3N) - 6*60*1000 ))   # machines "up" 6 min, so they're settled
A='x-admin-password: adm'
put() { curl -s -X PUT $B/api/projects/$1 -H "$A" -H content-type:application/json -d "$2" >/dev/null; }
join() { curl -s -X POST $B/api/join -H 'authorization: Bearer node' -H content-type:application/json -d "{\"agent\":\"$1\",\"pool\":\"main\",\"want\":$2}" >/dev/null; }
# The last 5 finished minutes, each at cpu $1, memory $2 (of 100), apps $3.
minutes() { local now=$(date +%s%3N); local m=$(( now - now % 60000 )); echo -n "["; for k in 5 4 3 2 1; do echo -n "{\"t\":$(( m - k*60000 )),\"h\":{\"cpu\":$1,\"memUsed\":$2,\"memTotal\":100},\"a\":${3:-{\}}}"; [ $k -gt 1 ] && echo -n ","; done; echo -n "]"; }
# sync machine cpu mem [statusjson [appsjson [cpu of its last 5 minutes]]]: a 4-core machine (no cpus field: 4 is
# assumed). A minute is summarised once (a second summary of the same minute is ignored), so only the hot syncs send them.
sync() { curl -s -X POST $B/api/sync -H 'authorization: Bearer node' -H content-type:application/json -d "{\"machine\":$1,\"run\":\"r$1\",\"agent\":\"agent-r$1\",\"pool\":\"main\",\"started\":$START,\"ready\":true,\"status\":${4:-{\}},\"metrics\":{\"live\":{\"t\":$(date +%s%3N),\"h\":{\"cpu\":$2,\"memUsed\":$3,\"memTotal\":100},\"a\":${5:-{\}}}${6:+,\"minutes\":$(minutes $6 $3 "${5:-{\}}")}}}" >/dev/null; }
show() { curl -s $B/api/status | jq -c '{placed:[.projects[] | "\(.name)→\([.placed[] | "\(.machine)\(if .leaving then "(leaving)" else "" end)"]|join(","))"], hot:.rebalance.hot, log:[.rebalance.log[].text]}'; }
put a '{"port":8001,"dockerfile":"FROM x"}'; put b '{"port":8002,"dockerfile":"FROM x"}'; put c '{"port":8003,"dockerfile":"FROM x"}'
join a 1; join b 2; join c 3
echo "== all quiet, m1 quietest: placements spread by load"
sync 1 5 10; sync 2 20 30; sync 3 40 50; sync 1 5 10
show
H='{"a":{"v":1,"s":"healthy"},"b":{"v":1,"s":"healthy"},"c":{"v":1,"s":"healthy"}}'
echo "== machine 1 has been at cpu 95 for 5 minutes, a using 70% of a core (17.5% of it): a moves to the server with the most room"
sync 1 95 60 "$H" '{"a":{"cpu":70,"mem":20}}' 95; sync 2 20 30 "$H"; sync 3 40 50 "$H"; sleep 31; sync 1 95 60 "$H" '{"a":{"cpu":70,"mem":20}}'
show
echo "== destination reports a healthy -> old copy dropped once DNS follows it (2 s later)"
A2='{"a":{"v":1,"s":"healthy"},"b":{"v":1,"s":"healthy"}}'
sync 2 20 30 "$A2"; sleep 3; sync 2 20 30 "$A2"; sync 1 30 40 "$H"
show
echo "== machine 3 is hot from c (20% of it), but a hot move was just made: it waits for the 10-minute cooldown"
sync 3 95 60 "$H" '{"c":{"cpu":80,"mem":20}}' 95; sleep 31; sync 3 95 60 "$H" '{"c":{"cpu":80,"mem":20}}'
show
echo "== rebalance off:"; curl -s -X PUT $B/api/settings -H "$A" -H content-type:application/json -d '{"rebalance":false}'; echo
curl -s $B/api/status | jq -c '.rebalance.on'
. "$HERE/stop.sh"
