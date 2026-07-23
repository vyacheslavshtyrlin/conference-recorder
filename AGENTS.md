# Recording/Storage Agent Guide

Owner: Recording/storage implementation agent for `recording-bot/**`.
Reviewer: Reviewer agent (`../.agents/reviewer-agent.md`).

Required reading:

- `../AGENTS.md`
- `../docs/architecture.md`
- `../docs/contracts.md`
- `../docs/epics/E11-recording-bot-and-storage-boundary.md`
- `../docs/epics/E12-s3-recording-delivery-control-plane.md`

Required skill: `docker-expert` for container, volume, resource-limit or runtime changes.

Ownership:

- Owns private recording-bot control/runtime code, Chromium supervision, FFmpeg finalization, technical manifests and recording storage adapters.
- Does not own public recording controls, human participant state, mediasoup room liveness or durable integration metadata.
- E11-T7 is a behavior-preserving structural refactoring gate, E11-T8 adds
  segmented recovery and E11-T9 adds the capacity-controlled worker pool.
  E11-T10 is decided: E12-T3 owns the remote segment adapter while API owns
  durable metadata, callbacks, retention and download authority.

Rules:

- The bot is receive-only and must never appear as a human participant or keep a room live.
- Partial, gapped or failed uploads must never publish a ready artifact.
- Bound browser processes, recording slots, source bytes, duration, FFmpeg runtime and shutdown.
- Keep local files, manifests, admissions and storage credentials private.
- Publish local artifacts atomically only after stream verification.

Verification before handoff:

- `npm run typecheck`
- `npm run lint`
- `npm test`
- `docker compose config --quiet` and a root Compose build when runtime packaging changes
