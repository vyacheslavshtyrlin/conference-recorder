# Recording/Storage Agent Guide

Owner: Recording/storage implementation agent for `recording-bot/**`.
Reviewer: Reviewer agent (`../.agents/reviewer-agent.md`).

Required reading:

- `../AGENTS.md`
- `../docs/architecture.md`
- `../docs/contracts.md`
- `../docs/epics/E12-s3-recording-delivery-control-plane.md`
- `../docs/epics/E13-go-track-capture-and-server-composition.md`

Required skills:

- `golang-code-style` for Go implementation and review.
- `docker-expert` for container, volume, resource-limit or runtime changes.

Ownership:

- Owns the Go/Pion receive-only capture worker, its private spool and direct
  presigned uploads.
- Owns the independent Node.js composition worker under `composition/**`.
- Does not own public recording controls, human participant state, mediasoup
  room liveness or durable integration metadata.

Rules:

- The bot is receive-only and must never appear as a human participant or keep
  a room live.
- Partial, gapped or failed uploads must never publish a ready artifact.
- Bound recording slots, source bytes, duration, FFmpeg runtime and shutdown.
- Keep local files, manifests, admissions and storage credentials private.
- Workers never receive PostgreSQL or permanent S3 credentials.

Verification before handoff:

- `go test ./...`
- `go vet ./...`
- composition typecheck, lint and tests
- root and scoped `docker compose config --quiet`
- root Compose builds for `recording-bot` and `recording-composition`
