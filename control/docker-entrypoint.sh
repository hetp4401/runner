#!/bin/sh
set -eu
umask 077

case "${1:-start}" in
  start|init)
    # An OS lock works across PID namespaces and disappears on crashes. The
    # empty lock file carries no state and must stay on the shared data volume.
    mkdir -p "${DATA_DIR:-/data}"
    exec flock --exclusive --nonblock --no-fork --conflict-exit-code 75 --verbose \
      "${DATA_DIR:-/data}/controller.lock" node docker-entrypoint.mjs "$@"
    ;;
  *) exec node docker-entrypoint.mjs "$@" ;;
esac
