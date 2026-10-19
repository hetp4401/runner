#!/bin/bash
# x-runner.websockets: the deploy API's websockets field writes it into the compose file (and takes it out again), a
# value that isn't true or false is refused, a Dockerfile-only app's generated compose file keeps it, the app's details
# say it, and a machine running the app is told (desired.<key>.websockets). Each line says ok, or WRONG.
HERE=$(cd "$(dirname "$0")" && pwd)
cd "$HERE/.."
. "$HERE/stop.sh"
(node "$HERE/cloudflare-mock.mjs" > /tmp/runner-test-mock.log 2>&1 &)
. "$HERE/start.sh" ADMIN_PASSWORD=adm JOIN_TOKEN=node 'POOLS={"main":1}' QUIET_MS=0
for i in $(seq 1 30); do curl -sf localhost:8911/api/status >/dev/null && break; sleep 1; done
B=localhost:8911; START=$(date +%s%3N)
A='x-admin-password: adm'
J=(-H content-type:application/json)
check() { if [ "$2" = "$3" ]; then echo "  ok   $1"; else echo "  WRONG $1: wanted [$2], got [$3]"; fi; }
put() { curl -s -X PUT $B/api/projects/$1 -H "$A" "${J[@]}" -d "$2"; }
code() { curl -s -o /dev/null -w '%{http_code}' -X PUT $B/api/projects/$1 -H "$A" "${J[@]}" -d "$2"; }
sync() { curl -s -X POST $B/api/sync -H 'authorization: Bearer node' "${J[@]}" -d "{\"machine\":1,\"run\":\"r1\",\"agent\":\"agent-r1\",\"pool\":\"main\",\"started\":$START,\"ready\":true,\"status\":{},\"metrics\":{\"live\":{\"t\":$(date +%s%3N),\"h\":{\"cpu\":10,\"memUsed\":20,\"memTotal\":100},\"a\":{}}}}"; }
details() { curl -s $B/api/projects/$1; }
curl -s -o /dev/null -X POST $B/api/join -H 'authorization: Bearer node' "${J[@]}" -d '{"agent":"a1","pool":"main","want":1}'

echo "== off by default"
put chat '{"port":3000,"compose":"# the chat app\nservices:\n  app:\n    image: x\n    ports: [\"3000:3000\"]\n"}' > /dev/null
for i in 1 2; do sync > /dev/null; done
check "details: websockets false" false "$(details chat | jq .websockets)"
check "the machine isn't told websockets" null "$(sync | jq '.desired.chat.websockets')"
echo "== the websockets field turns it on, in the compose file"
r=$(put chat '{"websockets":true}')
check "a new version" 2 "$(echo "$r" | jq .version)"
check "compose has x-runner.websockets: true" true "$(details chat | jq -r .compose | grep -q '^  websockets: true' && echo true)"
check "its comment kept" true "$(details chat | jq -r .compose | grep -q '^# the chat app' && echo true)"
check "details: websockets true" true "$(details chat | jq .websockets)"
check "the machine is told" true "$(sync | jq '.desired.chat.websockets')"
check "the same again is a no-op" true "$(put chat '{"websockets":true}' | jq .unchanged)"
echo "== in the compose file itself"
put chat2 '{"port":3001,"compose":"x-runner:\n  websockets: true\nservices:\n  app:\n    image: x\n    ports: [\"3001:3001\"]\n"}' > /dev/null
check "details: websockets true" true "$(details chat2 | jq .websockets)"
check "x-runner.websockets: yes-please is refused" 400 "$(code chat3 '{"port":3002,"compose":"x-runner:\n  websockets: please\nservices:\n  app:\n    image: x\n"}')"
check "websockets: \"true\" (text) is refused" 400 "$(code chat '{"websockets":"true"}')"
echo "== and off again"
put chat '{"websockets":false}' > /dev/null
check "details: websockets false" false "$(details chat | jq .websockets)"
check "no x-runner left in the compose file" false "$(details chat | jq -r .compose | grep -q 'x-runner' && echo true || echo false)"
echo "== a Dockerfile-only app"
put dock '{"port":4000,"dockerfile":"FROM a\n","websockets":true}' > /dev/null
check "generated compose has it" true "$(details dock | jq -r .compose | grep -q '^  websockets: true' && echo true)"
r=$(put dock '{"port":4001}')
check "a new port keeps it" true "$(details dock | jq '.websockets and (.compose | contains("4001:4001"))')"
. "$HERE/stop.sh"
