# The control plane as a fleet app (see the README's "Several copies, on no particular server"): this repo's control/
# at a commit (REF), and cloudflared for the tunnel connector the leader runs.
FROM node:24-slim
ARG REF
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
  && curl -fsSL -o /usr/local/bin/cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
  && chmod +x /usr/local/bin/cloudflared \
  && mkdir -p /app && curl -fsSL "https://codeload.github.com/hetp4401/runner/tar.gz/${REF}" | tar -xz -C /app --strip-components=2 "runner-${REF}/control" \
  && cd /app && npm install --omit=dev --no-audit --no-fund
COPY tunnel.yml /app/tunnel.yml
WORKDIR /app
CMD ["node", "server.mjs"]
