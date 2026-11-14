# The compiler is absent at runtime. Docker CLI runs only for workload changes.
FROM docker:28-cli@sha256:625d9431a9f54c5a2bc90f24f0e1c3d55b1349fd857dd85035f98c2c9acbdd4d
ARG TARGETARCH
COPY --chmod=755 bin/runner-agent-linux-${TARGETARCH} /usr/local/bin/runner-agent
ENV RUNNER_CODE_MANAGED=1
ENTRYPOINT ["/usr/local/bin/runner-agent"]
