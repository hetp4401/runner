#!/bin/sh
# Build and verify the native agent before replacing this fleet's old agent.
set -eu
: "${JOIN_TOKEN:?set JOIN_TOKEN to the fleet join token}"
SERVER_TAG=${SERVER_TAG:-${POOL:-}}
[ -n "$SERVER_TAG" ] || { echo "Set SERVER_TAG to the server's origin group, or standalone." >&2; exit 1; }
printf '%s\n' "$SERVER_TAG" | grep -Eq '^[a-z0-9][a-z0-9-]{0,29}$' || { echo "Invalid SERVER_TAG." >&2; exit 1; }
DATA=${RUNNER_DATA:-/var/lib/runner}
ORIGIN=@ORIGIN@
AGENT_URL=@AGENT_URL@
for command in docker curl tar sha256sum flock; do
  command -v "$command" >/dev/null 2>&1 || { echo "Install $command first." >&2; exit 1; }
done
[ "$(docker info --format '{{.OSType}}')" = linux ] || { echo "Agents need a Linux Docker host." >&2; exit 1; }
case "$(docker info --format '{{.Architecture}}')" in
  x86_64|amd64) ARCH=amd64 ;;
  aarch64|arm64) ARCH=arm64 ;;
  *) echo "This agent supports Linux amd64 and arm64." >&2; exit 1 ;;
esac
case "$DATA" in /*) ;; *) echo "RUNNER_DATA must be an absolute host path." >&2; exit 1;; esac
exec 9>/var/lib/runner-installer.lock
flock -n 9 || { echo "Another agent installation is in progress." >&2; exit 1; }
check_owner() {
  if docker inspect runner-agent >/dev/null 2>&1; then
    previous=$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' runner-agent | sed -n 's/^CONTROL_URL=//p')
    [ "$previous" = "$ORIGIN" ] || { echo "runner-agent belongs to another controller." >&2; exit 1; }
  fi
}
check_owner
BUILD_DIR=$(mktemp -d)
OLD_ID=""
CUTOVER=0
SUCCESS=0
rollback_previous() {
  current_id=$(docker inspect --format '{{.Id}}' runner-agent 2>/dev/null || true)
  if [ -n "$current_id" ] && [ "$current_id" != "$OLD_ID" ]; then docker rm -f "$current_id" >/dev/null 2>&1 || true; fi
  for pointer in current.json rust-current.json; do
    if [ -f "$BUILD_DIR/previous-$pointer" ]; then cp "$BUILD_DIR/previous-$pointer" "$DATA/agent-code/$pointer" || return 1
    else rm -f "$DATA/agent-code/$pointer"; fi
  done
  if [ -n "$OLD_ID" ]; then
    old_name=$(docker inspect --format '{{.Name}}' "$OLD_ID" 2>/dev/null || true)
    if [ "$old_name" != /runner-agent ]; then docker rename "$OLD_ID" runner-agent || return 1; fi
    docker start "$OLD_ID" >/dev/null || return 1
  fi
}
cleanup() {
  status=$?
  trap - EXIT HUP INT TERM
  if [ "$CUTOVER" -eq 1 ] && [ "$SUCCESS" -eq 0 ]; then
    if rollback_previous; then echo "Installation failed; the previous agent was restored." >&2
    else echo "Installation failed; restore the retained previous container manually." >&2; fi
    status=1
  fi
  rm -rf "$BUILD_DIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
PACKAGE=$(curl -fsSL --max-time 60 "$AGENT_URL/rust/bundle.sha256")
case "$PACKAGE" in *[!a-f0-9]*|'') echo "Invalid agent bundle digest." >&2; exit 1;; esac
[ "${#PACKAGE}" -eq 64 ] || { echo "Invalid agent bundle digest." >&2; exit 1; }
curl -fsSL --max-time 120 --max-filesize 67108864 "$AGENT_URL/rust/bundle.tar" -o "$BUILD_DIR/bundle.tar"
[ "$(sha256sum "$BUILD_DIR/bundle.tar" | cut -d ' ' -f 1)" = "$PACKAGE" ] || { echo "Agent bundle verification failed." >&2; exit 1; }
tar -tf "$BUILD_DIR/bundle.tar" | while IFS= read -r path; do
  echo "$path" | grep -Eq '^(Dockerfile|bin/runner-agent-linux-(amd64|arm64))$' || exit 1
done
tar -tvf "$BUILD_DIR/bundle.tar" | awk '$1 !~ /^-/ {exit 1}'
tar -xf "$BUILD_DIR/bundle.tar" -C "$BUILD_DIR"
rm "$BUILD_DIR/bundle.tar"
IMAGE="runner-agent:rust-$PACKAGE"
docker build --pull --build-arg TARGETARCH="$ARCH" -t "$IMAGE" "$BUILD_DIR"
mkdir -p "$DATA"
DATA=$(cd "$DATA" && pwd -P)
export JOIN_TOKEN SERVER_TAG
[ ! -L "$DATA/agent-code" ] || { echo "Unsafe agent-code directory." >&2; exit 1; }
for pointer in current.json rust-current.json; do
  [ ! -L "$DATA/agent-code/$pointer" ] || { echo "Unsafe agent release selection." >&2; exit 1; }
  if [ -e "$DATA/agent-code/$pointer" ]; then
    [ -f "$DATA/agent-code/$pointer" ] || { echo "Invalid agent release selection." >&2; exit 1; }
    cp "$DATA/agent-code/$pointer" "$BUILD_DIR/previous-$pointer"
  fi
done
check_owner
OLD_ID=$(docker inspect --format '{{.Id}}' runner-agent 2>/dev/null || true)
BACKUP="runner-agent-previous-$$"
if [ -n "$OLD_ID" ]; then
  docker inspect "$BACKUP" >/dev/null 2>&1 && { echo "Previous installation backup exists." >&2; exit 1; }
  CUTOVER=1
  docker stop -t 180 "$OLD_ID" >/dev/null
  docker rename "$OLD_ID" "$BACKUP"
fi
CUTOVER=1
rm -f "$DATA/agent-code/current.json" "$DATA/agent-code/rust-current.json"
docker run -d --name runner-agent --restart unless-stopped --stop-timeout 180 --network host --hostname "$(hostname)" \
  --label io.runners.kind=agent --label io.runners.control="$ORIGIN" \
  -v /var/run/docker.sock:/var/run/docker.sock -v "$DATA:$DATA" \
  -e CONTROL_URL="$ORIGIN" -e AGENT_URL="$AGENT_URL" -e JOIN_TOKEN -e RUNNER_DATA="$DATA" \
  -e RUNNER_INSTALL_REVISION="$PACKAGE" -e RUNNER_CODE_MANAGED=1 \
  -e SERVER_TAG -e MACHINE -e ROUTER_PORT -e REGION \
  -e LABEL="${LABEL:-$(hostname)}" "$IMAGE"
sleep 2
[ "$(docker inspect --format '{{.State.Running}}' runner-agent)" = true ] || exit 1
SUCCESS=1
if [ -n "$OLD_ID" ]; then docker rm "$OLD_ID" >/dev/null; fi
echo "Installed the Rust agent. Follow it with: docker logs -f runner-agent"
