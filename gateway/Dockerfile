FROM cloudflare/cloudflared:latest AS connector
FROM node:24-alpine
WORKDIR /app
COPY --from=connector /usr/local/bin/cloudflared /usr/local/bin/cloudflared
COPY router.mjs streams.mjs server.mjs connector.mjs ./
USER node
EXPOSE 18278
CMD ["node","server.mjs"]
