FROM rust:1.100-alpine@sha256:de1e0058543a81ed7b046629167db587c67a94bdf37cce14eb27f0ec83baaffb AS build
RUN apk add --no-cache build-base linux-headers
WORKDIR /src
COPY rust ./rust
COPY control/src/app.html ./control/src/app.html
ARG REVISION=development
WORKDIR /src/rust
RUN cargo test --locked --workspace
RUN RUNNER_BUILD_REVISION="$REVISION" cargo build --locked --release -p runner-control -p runner-agent -p runner-raft

FROM scratch AS binaries
COPY --from=build /src/rust/target/release/runner-control /runner-control
COPY --from=build /src/rust/target/release/runner-agent /runner-agent
COPY --from=build /src/rust/target/release/runner-raft /runner-raft
