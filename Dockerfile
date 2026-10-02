# syntax=docker/dockerfile:1.7

FROM golang:1.24-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/recording-bot \
    ./cmd/capture

FROM node:22.15.0-alpine3.21 AS composition-build
ARG FONT_PACKAGE_VERSION=2.37-r5
WORKDIR /composition
RUN apk add --no-cache "font-dejavu=${FONT_PACKAGE_VERSION}"
COPY composition/package.json composition/package-lock.json ./
RUN npm ci
COPY composition/Dockerfile composition/docker-entrypoint.sh ./
COPY composition/tsconfig.json composition/tsconfig.build.json composition/eslint.config.mjs ./
COPY composition/src ./src
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
    addgroup -S -g 65532 capture && \
    adduser -S -D -H -u 65532 -G capture capture && \
    mkdir -p /var/lib/conference-recording
WORKDIR /app
COPY --from=build --chown=65532:65532 /out/recording-bot /app/recording-bot
COPY --from=composition-build --chown=node:node /composition/node_modules ./node_modules
COPY --from=composition-build --chown=node:node /composition/package.json ./package.json
COPY --from=composition-build --chown=node:node /composition/dist ./dist
COPY --chmod=755 docker-entrypoint.sh /usr/local/bin/recording-worker-entrypoint
ENV NODE_ENV=production \
    RECORDING_FFMPEG_PATH=/usr/bin/ffmpeg \
    RECORDING_FFPROBE_PATH=/usr/bin/ffprobe
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/app/recording-bot", "healthcheck"]
ENTRYPOINT ["recording-worker-entrypoint"]
CMD ["/app/recording-bot"]
