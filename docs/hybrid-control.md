# Historical controller implementations

The supported setup uses the [control plane module](../control/README.md) and [agent module](../agent/README.md). Follow the [self-hosting guide](self-hosting.md) to initialize and operate a fleet.

Older controller entrypoints, storage adapters and associated tests remain in the source tree for compatibility and reference. They are separate from the active `control/server.mjs` and `control/src/local-store.mjs` path. The current deployment uses the root Dockerfile and Compose file with persistent SQLite configuration and in-memory runtime observations.

Use [VPS operations](vps-control.md) when moving an existing controller. Preserve its identity, database, public URL and tunnel namespace, and stop the old scheduler before starting another one for that fleet. The [test reference](../control/test/README.md) identifies historical integration checks separately from the active module checks.
