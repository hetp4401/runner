export function installScript(origin, agentUrl) {
  const quote = (value) => "'" + value.replaceAll("'", "'\\''") + "'";
  return `#!/bin/sh
# Install this fleet's agent. Build and verify its image before replacing a running agent.
# Restarts use local code; explicit rolls download and verify a new code release.
set -eu
: "\${JOIN_TOKEN:?set JOIN_TOKEN to the fleet join token}"
SERVER_TAG=\${SERVER_TAG:-\${POOL:-}}
[ -n "$SERVER_TAG" ] || { echo "Set SERVER_TAG to the server's origin group, or standalone." >&2; exit 1; }
printf '%s\\n' "$SERVER_TAG" | grep -Eq '^[a-z0-9][a-z0-9-]{0,29}$' || { echo "SERVER_TAG must be 1-30 lowercase letters, digits or dashes." >&2; exit 1; }
DATA=\${RUNNER_DATA:-/var/lib/runner}
ORIGIN=${quote(origin)}
AGENT_URL=${quote(agentUrl)}
for command in docker curl tar sha256sum flock; do
  command -v "$command" >/dev/null 2>&1 || { echo "Install $command first." >&2; exit 1; }
done
[ "$(docker info --format '{{.OSType}}')" = linux ] || { echo "Agents need a Linux Docker host." >&2; exit 1; }
case "$DATA" in /*) ;; *) echo "RUNNER_DATA must be an absolute host path." >&2; exit 1;; esac
# The documented installer runs as root. This lock lives outside app data and
# is released by the OS if installation fails or the host reboots.
exec 9>/var/lib/runner-installer.lock
flock -n 9 || { echo "Another agent installation is in progress." >&2; exit 1; }
check_owner() {
  if docker inspect runner-agent >/dev/null 2>&1; then
    previous=$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' runner-agent | sed -n 's/^CONTROL_URL=//p')
    [ "$previous" = "$ORIGIN" ] || { echo "runner-agent belongs to another controller; refusing to replace it." >&2; exit 1; }
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
  if [ -f "$BUILD_DIR/previous-current.json" ]; then
    cp "$BUILD_DIR/previous-current.json" "$DATA/agent-code/current.json" || true
  else rm -f "$DATA/agent-code/current.json"; fi
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
    else echo "Installation failed; restore the retained previous agent container manually." >&2; fi
    status=1
  fi
  rm -rf "$BUILD_DIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
PACKAGE=$(curl -fsSL --max-time 60 "$AGENT_URL/bundle.sha256")
case "$PACKAGE" in *[!a-f0-9]*|'') echo "Invalid agent bundle digest." >&2; exit 1;; esac
[ "\${#PACKAGE}" -eq 64 ] || { echo "Invalid agent bundle digest." >&2; exit 1; }
curl -fsSL --max-time 120 "$AGENT_URL/bundle.tar" -o "$BUILD_DIR/bundle.tar"
[ "$(sha256sum "$BUILD_DIR/bundle.tar" | cut -d ' ' -f 1)" = "$PACKAGE" ] || { echo "Agent bundle verification failed." >&2; exit 1; }
tar -tf "$BUILD_DIR/bundle.tar" | while IFS= read -r path; do
  echo "$path" | grep -Eq '^(Dockerfile|control/agent-launcher\\.mjs|agent/[a-z][a-z0-9-]*\\.mjs)$' || exit 1
done
tar -xf "$BUILD_DIR/bundle.tar" -C "$BUILD_DIR"
rm "$BUILD_DIR/bundle.tar"
IMAGE="runner-agent:$PACKAGE"
docker build --pull -t "$IMAGE" "$BUILD_DIR"
mkdir -p "$DATA"
DATA=$(cd "$DATA" && pwd -P)
export JOIN_TOKEN SERVER_TAG
# A reinstall selects this image's baseline; old releases remain available on disk.
[ ! -L "$DATA/agent-code" ] || { echo "Unsafe agent-code directory." >&2; exit 1; }
[ ! -L "$DATA/agent-code/current.json" ] || { echo "Unsafe agent release selection." >&2; exit 1; }
if [ -e "$DATA/agent-code/current.json" ]; then
  [ -f "$DATA/agent-code/current.json" ] || { echo "Invalid agent release selection." >&2; exit 1; }
  cp "$DATA/agent-code/current.json" "$BUILD_DIR/previous-current.json"
fi
check_owner
OLD_ID=$(docker inspect --format '{{.Id}}' runner-agent 2>/dev/null || true)
BACKUP="runner-agent-previous-$$"
if [ -n "$OLD_ID" ]; then
  docker inspect "$BACKUP" >/dev/null 2>&1 && { echo "Previous installation backup already exists." >&2; exit 1; }
  CUTOVER=1
  docker stop -t 180 "$OLD_ID" >/dev/null
  docker rename "$OLD_ID" "$BACKUP"
fi
restore_previous() {
  exit 1
}
CUTOVER=1
rm -f "$DATA/agent-code/current.json"
if ! docker run -d --name runner-agent --restart unless-stopped --stop-timeout 180 --network host --hostname "$(hostname)" \\
  --label io.runners.kind=agent --label io.runners.control="$ORIGIN" \\
  -v /var/run/docker.sock:/var/run/docker.sock -v "$DATA:$DATA" \\
  -e CONTROL_URL="$ORIGIN" -e AGENT_URL="$AGENT_URL" -e JOIN_TOKEN -e RUNNER_DATA="$DATA" \\
  -e RUNNER_INSTALL_REVISION="$PACKAGE" -e RUNNER_CODE_MANAGED=1 \\
  -e SERVER_TAG -e MACHINE -e ROUTER_PORT -e REGION \\
  -e LABEL="\${LABEL:-$(hostname)}" "$IMAGE"; then restore_previous; fi
sleep 2
[ "$(docker inspect --format '{{.State.Running}}' runner-agent)" = true ] || restore_previous
SUCCESS=1
if [ -n "$OLD_ID" ]; then docker rm "$OLD_ID" >/dev/null; fi
echo "Installed. Follow it with: docker logs -f runner-agent"
`;
}
