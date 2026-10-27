#!/bin/bash
# Compatibility entrypoint for the passive-fleet and required-tag regressions.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
exec node "$HERE/passive-fleet.mjs"
