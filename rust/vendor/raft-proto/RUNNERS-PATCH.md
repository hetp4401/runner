Upstream: TiKV raft-proto 0.7.0, Apache-2.0 (see LICENSE).

The sole protocol change is `ConfChangeType.AddLearnerNode = 3`, matching
go.etcd.io/raft/v3 v3.6.0 used by the deployed Runners fleet. Upstream uses 2,
which etcd reserves for UpdateNode. Runners does not use UpdateNode.

Do not remove this patch during a dependency update. The mixed Go/Rust cluster
tests must pass for election, quorum loss, learner replacement, joint consensus,
snapshots, leadership transfer, and total memory loss.

Only the common peer message types (0–18) are transported. Rust-specific snapshot
requests/priority and etcd's local asynchronous-storage messages are not enabled.

Code generation uses protobuf-codegen-pure 2.28 instead of an external protoc.
This removes a platform-specific build tool; only protobuf-codec is supported.
