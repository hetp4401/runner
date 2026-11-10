FROM golang:1.24-bookworm AS raft
WORKDIR /src
COPY control/raft/go.mod control/raft/go.sum ./
RUN go mod download
COPY control/raft/main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /runner-raft .
FROM node:24-bookworm-slim
ENV NODE_ENV=production PORT=8924 BIND_HOST=0.0.0.0
WORKDIR /app
COPY --from=raft /runner-raft /usr/local/bin/runner-raft
COPY control/src/ha/config.mjs control/src/ha/authority.mjs control/src/ha/tail.mjs ./control/src/ha/
COPY control/cluster-init.mjs ./control/
COPY .github/scripts/capacity.mjs ./.github/scripts/
COPY providers/github ./providers/github/
USER node
EXPOSE 8924
HEALTHCHECK --interval=20s --timeout=5s --start-period=30s CMD node -e "fetch('http://127.0.0.1:'+process.env.PORT+'/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
CMD ["node", "providers/github/server.mjs"]
