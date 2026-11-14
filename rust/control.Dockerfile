FROM cloudflare/cloudflared:latest@sha256:b94b96edf2c9c4360f8879c8580cfaa99077f19c849ec595ca2f711b27d4f344 AS tunnel
FROM scratch
ARG TARGETARCH
ARG REVISION=development
LABEL org.opencontainers.image.source="https://github.com/hetp4401/runner" org.opencontainers.image.revision="$REVISION"
COPY --from=tunnel /usr/local/bin/cloudflared /usr/local/bin/cloudflared
COPY --from=tunnel /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --chmod=755 rust/dist/binaries/${TARGETARCH}/runner-control /usr/local/bin/runner-control
COPY rust/dist/assets /usr/local/share/runner-agent
COPY --chown=1000:1000 rust/dist/rootfs /
ENV PATH=/usr/local/bin DATA_DIR=/data RUNTIME_DIR=/run/runner BIND_HOST=0.0.0.0 PORT=8920 CONTROLLER_RUNTIME=docker
USER 1000:1000
VOLUME ["/data"]
EXPOSE 8920
HEALTHCHECK --interval=30s --timeout=8s --start-period=60s CMD ["/usr/local/bin/runner-control", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/runner-control"]
CMD ["start"]
