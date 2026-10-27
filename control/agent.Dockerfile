FROM docker:28-cli AS agent
RUN apk add --no-cache nodejs && node --version && docker compose version
ENV RUNNER_CODE_MANAGED=1
WORKDIR /agent
COPY agent/*.mjs ./
COPY control/agent-launcher.mjs /usr/local/lib/runner-agent-launcher.mjs
ENTRYPOINT ["node", "/usr/local/lib/runner-agent-launcher.mjs"]
