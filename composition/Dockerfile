# syntax=docker/dockerfile:1.7

FROM node:22.15.0-alpine3.21 AS build
ARG FONT_PACKAGE_VERSION=2.37-r5
WORKDIR /app
RUN apk add --no-cache "font-dejavu=${FONT_PACKAGE_VERSION}"
COPY package.json package-lock.json ./
RUN npm ci
COPY Dockerfile docker-entrypoint.sh ./
COPY tsconfig.json tsconfig.build.json eslint.config.mjs ./
COPY src ./src
RUN npm run typecheck && \
    npm run lint && \
    npm run test:container && \
    npm run build && \
    npm prune --omit=dev

FROM node:22.15.0-alpine3.21 AS production
ARG FFMPEG_PACKAGE_VERSION=6.1.2-r1
ARG FONT_PACKAGE_VERSION=2.37-r5
ARG SU_EXEC_PACKAGE_VERSION=0.2-r3
RUN apk add --no-cache \
        "ffmpeg=${FFMPEG_PACKAGE_VERSION}" \
        "font-dejavu=${FONT_PACKAGE_VERSION}" \
        "su-exec=${SU_EXEC_PACKAGE_VERSION}" && \
    ffmpeg -version && \
    ffprobe -version && \
    mkdir -p /var/lib/conference-recording && \
    chown node:node /var/lib/conference-recording
ENV NODE_ENV=production \
    RECORDING_FFMPEG_PATH=/usr/bin/ffmpeg \
    RECORDING_FFPROBE_PATH=/usr/bin/ffprobe
WORKDIR /app
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/package.json ./package.json
COPY --from=build --chown=node:node /app/dist ./dist
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/recording-composition-entrypoint
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
    CMD node -e "fetch('http://127.0.0.1:8090/health/ready').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"
ENTRYPOINT ["recording-composition-entrypoint"]
CMD ["node", "dist/composition.js"]
