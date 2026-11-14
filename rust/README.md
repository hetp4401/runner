# Native Runners runtime

This workspace replaces the resident Node controller, Go consensus helper, and Node agent with native Rust programs. The controller embeds its HTTP API, WebSockets, scheduler, SQLite/D1 adapter, and volatile Raft member in one process. Cloudflare's connector remains a separate upstream executable. Agents use the Docker API for observations and the Docker CLI for workload changes.

The existing application configuration, API, UI, recovery signatures, deployment ownership labels, and consensus wire format are retained. The `raft-proto` compatibility patch is documented in `vendor/raft-proto/RUNNERS-PATCH.md`.

## Build and test

Use Rust 1.100.0 or newer:

```sh
cd rust
cargo test --locked --workspace
cargo build --locked --release -p runner-control -p runner-agent -p runner-raft
```

The `Rust runtime` workflow builds and tests static Linux amd64 and arm64 binaries on native GitHub runners. It packages the controller in a scratch image, with certificates, Cloudflare's connector, and verified agent downloads. Neither language toolchains nor Node/Go runtimes are included. The agent image contains its binary and Docker CLI. The old JavaScript download endpoints remain available during migration.

The controller image preserves UID 1000, `/data`, and the existing environment settings. `start`, `init`, `credentials`, `healthcheck`, and `--version` are native commands. Initialization is explicit; a missing configuration database never silently creates an empty fleet. Consensus bootstrap remains explicit through the compatible `/_cp/bootstrap` API and existing `control/cluster-init.mjs` operator tool.

## Memory and failure handling

The processes use a single async runtime thread. Queues have count or byte limits, metric columns use compact numeric arrays, and history is compressed. Defaults include 24 MiB for compressed metric history, 8 MiB for the version cache, an 8 MiB compressed runtime tail, 4 MiB of pending metric input, and a 512 KiB global live-log relay. These are ceilings for particular buffers, not promises about total process memory.

Every configuration commit and issued plan checks current leadership. A lost D1 response is confirmed by its unique commit ID instead of replayed. Quorum loss stops the scheduler. Runtime observations and metrics remain in memory and never become durable election or assignment records.

An isolated seven-replica test on an Apple Silicon Mac measured approximately 41 MiB total RSS for Rust versus 295 MiB for the original Node/Go processes after one application deployment. This excludes Cloudflare connectors and production history; production measurements are still required.

## Migration order

Validate binaries and actual Docker workloads before changing the live fleet. Install the native agent first while the original controller is serving. The agent preserves existing workload ownership, inventory, and connectors. It recognizes a presentation-only YAML change only after comparing all effective deployment inputs and the verified previous fingerprint, avoiding unnecessary workload rebuilds during the controller change.

Replace controllers one at a time, preserving the existing seven-member configuration, D1 credentials, signing keys, tunnel namespace, and at least four available voters. A replacement must become a committed voter before its predecessor stops. Keep the old images and configuration available for rollback. Never bootstrap to bypass a missing quorum.

The native installer builds and verifies the new agent image before cutover and restores both release pointers and the previous container on failure. It does not delete application containers or volumes. Existing cached JavaScript releases remain available for rollback; native agent updates retain two recent native releases and the running executable.

The application test suites include deployed-controller specification and scheduling fixtures, API/authentication and live-log tests, D1 failure tests, and agent ownership and lifecycle checks. `control/test/ha-service.mjs` supports `RUST_CONTROL_BINARY` and optional `RUST_CONTROL_IDS` for full-native and mixed-version seven-member tests. `HA_MEMORY_RESULT` records the complete controller process trees for comparison.
