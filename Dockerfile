FROM node:22-bookworm-slim AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run test && npm run build && npm prune --omit=dev

FROM mcr.microsoft.com/playwright:v1.55.0-noble AS runtime
ENV NODE_ENV=production \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
WORKDIR /app
COPY --from=build --chown=pwuser:pwuser /app/node_modules ./node_modules
COPY --from=build --chown=pwuser:pwuser /app/package*.json ./
COPY --from=build --chown=pwuser:pwuser /app/dist ./dist
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/recording-bot-entrypoint
RUN mkdir -p /var/lib/conference-recording && chown -R pwuser:pwuser /var/lib/conference-recording
EXPOSE 8090
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=5 \
  CMD node -e "fetch('http://127.0.0.1:8090/health/ready').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
ENTRYPOINT ["recording-bot-entrypoint"]
CMD ["node", "dist/main.js"]
