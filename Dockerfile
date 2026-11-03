FROM cloudflare/cloudflared:latest AS tunnel
FROM node:24-bookworm-slim AS control
ENV NODE_ENV=production DATA_DIR=/data BIND_HOST=0.0.0.0 PORT=8920
WORKDIR /app/control
COPY control/package.json control/package-lock.json ./
RUN apt-get update && apt-get install -y --no-install-recommends util-linux && rm -rf /var/lib/apt/lists/* \
    && npm ci --omit=dev && npm cache clean --force
COPY --from=tunnel /usr/local/bin/cloudflared /usr/local/bin/cloudflared
COPY --from=tunnel /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY control/src ./src
COPY control/server.mjs control/docker-entrypoint.mjs control/healthcheck.mjs control/agent.Dockerfile control/agent-launcher.mjs ./
COPY --chmod=755 control/docker-entrypoint.sh ./
COPY agent /app/agent
RUN mkdir -p /data /run/runner && chown node:node /data /run/runner
USER node
VOLUME ["/data"]
EXPOSE 8920
HEALTHCHECK --interval=30s --timeout=8s --start-period=60s CMD node healthcheck.mjs
ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["start"]
