#!/bin/sh
# Extract the verified release and exec it; no launcher stays resident.
set -eu
case "$(uname -s):$(uname -m)" in
  Linux:x86_64) ARCH=amd64 ;;
  Linux:aarch64|Linux:arm64) ARCH=arm64 ;;
  *) echo "The native agent requires a Linux amd64 or arm64 host." >&2; exit 1 ;;
esac
for command in docker sha256sum; do
  command -v "$command" >/dev/null 2>&1 || { echo "Install $command first." >&2; exit 1; }
done
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
IMAGE=${RUNNER_RUST_IMAGE:-$(cat "$SCRIPT_DIR/runtime-image")}
DATA=${RUNNER_DATA:-/var/lib/runner}
case "$DATA" in /*) ;; *) echo "RUNNER_DATA must be an absolute host path." >&2; exit 1;; esac
[ ! -L "$DATA/agent-code" ] || { echo "Unsafe agent-code directory." >&2; exit 1; }
umask 077
mkdir -p "$DATA/agent-code"
CODE=$(CDPATH= cd -- "$DATA/agent-code" && pwd -P)
STAGE=$(mktemp -d "$CODE/.native-bootstrap.XXXXXX")
CONTAINER=""
cleanup() {
  if [ -n "$CONTAINER" ]; then docker rm -v "$CONTAINER" >/dev/null 2>&1 || true; fi
  rm -rf "$STAGE"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
# Docker verifies the pinned image digest and its layers. The container is never
# started; remove only its own anonymous volume after extracting the binary.
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then docker pull "$IMAGE" >&2; fi
CONTAINER=$(docker create "$IMAGE" --version)
docker cp "$CONTAINER:/usr/local/share/runner-agent/rust/runner-agent-linux-$ARCH" "$STAGE/runner-agent"
chmod 700 "$STAGE/runner-agent"
"$STAGE/runner-agent" --version >&2
RUNNER_INSTALL_REVISION=$(sha256sum "$STAGE/runner-agent" | cut -d ' ' -f 1)
[ ! -d "$CODE/native-base" ] || { echo "Invalid native agent path." >&2; exit 1; }
mv -f "$STAGE/runner-agent" "$CODE/native-base"
export RUNNER_INSTALL_REVISION
export RUNNER_CODE_MANAGED=1
export RUNNER_DATA="$DATA"
cleanup
trap - EXIT HUP INT TERM
exec "$CODE/native-base" "$@"
