# Recording workers

This is the canonical recording deployment repository. It contains the E13
production Go/Pion track-capture implementation and the Node composition worker
under `composition/`. The existing GitLab pipeline builds one deploy image;
Compose runs that image as separate capture and composition containers with
different commands, credentials, volumes and resource limits. Chromium capture
and the temporary Go service aliases have been removed.

The spike proves at code level that:

- mediasoup ICE-lite transport and consumer parameters can be represented as a
  remote SDP offer accepted by Pion, with Pion DTLS parameters extracted for
  `bot:connectRecvTransport`;
- Pion receives Opus, VP8 and H.264 RTP on localhost and sends RTCP PLI for both
  video codecs;
- the unchanged `recording-bot.v1` request order is
  capabilities, create transport, snapshot, consume, connect, resume and PLI;
- the asynchronous WebSocket implementation correlates request IDs, accepts a
  snapshot event before the `bot:syncSnapshot` response, delivers lifecycle
  events in socket order and closes on controller-detected gap/conflict;
- snapshots and lifecycle revisions reconcile camera and screen
  start/stop/replacement, while gaps, conflicting duplicates, overlapping
  replacements and unsupported codec tuples fail explicitly;
- canonical v2 opaque participant and logical-track IDs remain stable across a
  reconnect or producer replacement without persisting raw v1 IDs.

Run from this directory:

```powershell
go test ./...
go vet ./...
```

For a verbose reproducible media-only run:

```powershell
go test -run TestPionReceivesSupportedRTPAndVideoPLI -v
go test -run TestPionAcceptsMediasoupICELiteOffer -v
```

The first command creates real Pion peers on localhost and transfers RTP/RTCP;
the second applies captured-shape mediasoup transport/consumer fixtures,
including VP8/H.264 RTX, header extensions and RTCP metadata.

## Test-only live probe

The assignment gate is approved: a live probe will be the sole transient worker
selected through the existing worker registry and fenced attempt `PUT`. It
receives a dedicated one-use admission through that authoritative assignment;
another worker must not receive or race the same attempt.

T1A now implements that test-only control shim in `cmd/shim`. It exposes the
existing `/health/live`, `/health/ready`, fenced attempt `PUT` and bounded
idempotent `DELETE` worker boundary with capacity one. Signaling supplies the
one-use admission only inside the authenticated attempt body; the shim passes it
directly to the in-process Pion runner and never reads it from environment,
persists it or includes it in readiness, evidence, logs or errors. Direct
`cmd/probe` invocation is disabled so copying, intercepting or replaying an
admission cannot become an operator alternative.

The shim uses the same static worker configuration names as production capture:

```powershell
$env:RECORDING_BOT_WORKER_ID='pion-t1a'
$env:RECORDING_BOT_CONTROL_JWT_HS256_SECRET='<shared-static-control-secret>'
$env:RECORDING_BOT_BIND_HOST='0.0.0.0'
$env:RECORDING_BOT_PORT='8090'
$env:RECORDING_BOT_START_TIMEOUT_MS='10000'
$env:RECORDING_BOT_STOP_TIMEOUT_MS='5000'
$env:RECORDING_CAPTURE_EXPECTED_SOURCES='mic,camera,screen'
go run ./cmd/shim
```

This command is safe only as the sole worker selected by the approved registry
and fenced assignment. The shim reports bounded source/codec evidence in its
attempt snapshot after tracks arrive. No live Docker Compose execution has been
performed yet, so T1A provides a tested assignment seam but not live acceptance
evidence.

The live media router now advertises Opus, VP8 and H.264 constrained baseline.
The future shim live gate still has to demonstrate actual VP8/H.264 media
receipt; the receiver fails explicitly when the router or consumer does not
negotiate the required codec.

## E13-T2/T4 production capture

`cmd/capture` is the production entrypoint. It validates the established
configuration names, private API credentials and writable spool root, then
serves the authenticated fenced production attempt/finalization boundary. It
does not reuse the T1 shim's runner.

Required production configuration:

```text
RECORDING_API_INTERNAL_URL
RECORDING_CAPTURE_SERVICE_SECRET
RECORDING_BOT_CONTROL_JWT_HS256_SECRET
RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS
```

The first three names already existed in the Chromium deployment. The only new
mandatory GitLab variable is `RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS` (recommended
value `300`), and API must use the same value. Optional new overrides are
`RECORDING_COMPOSITION_FFMPEG_TIMEOUT_MS=21600000`,
`RECORDING_COMPOSITION_HTTP_TIMEOUT_MS=120000` and
`RECORDING_BOT_LOG_LEVEL=info`; omitting them uses those defaults.

Established defaults remain unchanged: `RECORDING_BOT_BIND_HOST=0.0.0.0`,
`RECORDING_BOT_PORT=8090`, `RECORDING_BOT_MAX_SLOTS=1`, start timeout 30 seconds,
stop timeout 5 seconds, recovery timeout 30 seconds and
`RECORDING_STORAGE_ROOT=/var/lib/conference-recording`. The shared direct-upload
deadline is required as a base-10 value from 1 through 3600 seconds and has no
service default. `RECORDING_BOT_LOG_LEVEL` is observability-only and
defaults to `info`. `RECORDING_FFMPEG_PATH` defaults to `ffmpeg`;
the composition-only FFmpeg timeout defaults to 6 hours and is capped at 24 hours,
while its HTTP timeout defaults to 120 seconds and is capped at 10 minutes.
`RECORDING_MAX_SOURCE_BYTES` and `RECORDING_MAX_DURATION_SECONDS` retain the established
bounded defaults.

The T4 pipeline:

- consumes unchanged `recording-bot.v1` snapshots/events and keeps raw IDs only
  in memory while persisting recording-scoped opaque identities;
- writes independent per-track RTP intermediates in the bounded private spool,
  supervises FFmpeg `-c copy` muxing into closed VP8/Opus WebM or H.264 Matroska
  segments outside the RTP read loop through bounded per-track queues, fsyncs
  and atomically promotes them;
- checkpoints normalized timeline state before commit, transfers segment bytes
  directly through the v2 presigned URL under the shared whole-PUT deadline,
  and retries durable uncommitted segments after restart;
- loads every bounded recovery page before mutation and seals clean attempts as
  complete or terminal/finalization-only recovery as partial.

Health remains fail-closed:

- `GET /health/live` returns `200` while the process can serve HTTP;
- `GET /health/ready` returns `200` only when the foundation is running and the
  configured FFmpeg executable is resolvable; otherwise it returns `503`,
  `capacity: 0` and `acceptingAttempts: false`. Readiness requires the existing
  recording-control JWT because its payload includes worker/recovery identity;
- the image health check calls only liveness and makes no media acceptance claim.

The capture runtime is composed from `CaptureAttemptRunner`, the signaling
`SignalingClient` and `PeerSession` boundaries, `RTPTrackWriter`,
`DurableSpool`, the v2 control-plane client, `TimelineState` and
`BoundedProcessSupervisor`. Small interfaces remain only at external or
replaceable boundaries; concrete attempt, track and timeline components are not
shadowed by unused one-method abstractions.

Build and run the binary without changing root Compose:

```powershell
go build ./cmd/capture
go run ./cmd/capture
```

The production `Dockerfile` builds the static Go binary plus compiled Node
composition runtime on Node 22 Alpine 3.21. It installs the exact Alpine
`ffmpeg=6.1.2-r1` package, which supplies both `/usr/bin/ffmpeg` and
`/usr/bin/ffprobe`, and asserts both binaries during image build. The container
health check invokes the Go binary's liveness-only `healthcheck` command; it
does not claim readiness or live media acceptance. The entrypoint prepares only
the assigned volume and drops capture to UID/GID 65532 or composition to the
image's `node` user. `composition/Dockerfile` remains available for focused
package builds.

`docker-compose.yml` preserves the deployed topology of five independently
addressable capture containers and one composition container with separate
credentials, storage volumes and resource limits.

The composition worker exposes identity-free `/health/live` and
`/health/ready` endpoints on its internal port. Its container health check uses
readiness. A fresh workspace is used for every lease attempt, and retryable
failures return the job to the API queue. Final artifact upload uses the same
required `RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS` value as capture; no additional
GitLab variable is introduced. Final object keys are attempt-scoped, so artifact
integrity does not depend on bucket versioning. API and composition must receive
the same existing `RECORDING_FINALIZE_MAX_ATTEMPTS` value. The optional
`RECORDING_COMPOSITION_PIDS_LIMIT` override defaults to `128`.
