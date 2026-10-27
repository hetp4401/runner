FROM cloudflare/cloudflared:latest AS tunnel
FROM node:24-bookworm-slim
ENV NODE_ENV=production DATA_DIR=/data BIND_HOST=0.0.0.0 PORT=8920
WORKDIR /app/control
COPY control/package.json control/package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --from=tunnel /usr/local/bin/cloudflared /usr/local/bin/cloudflared
COPY --from=tunnel /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY control/src ./src
COPY control/server.mjs control/docker-entrypoint.mjs ./
COPY agent /app/agent
RUN mkdir -p /data /run/runner && chown node:node /data /run/runner
USER node
VOLUME ["/data"]
EXPOSE 8920
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s CMD node -e "fetch('http://127.0.0.1:'+process.env.PORT+'/healthz',{signal:AbortSignal.timeout(4000)}).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
ENTRYPOINT ["node", "docker-entrypoint.mjs"]
CMD ["start"]
