package recording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
)

type controlPlaneFixture struct {
	calls             []string
	allocateErr       error
	recovery          RecoveryState
	recoveries        []RecoveryState
	recoveryErr       error
	recoverCalls      int
	sealRequest       *SealRequest
	checkpointThrough []int64
	allocated         []SegmentDescriptor
	requiredTrackID   string
}

type lifecycleTrack struct {
	packets   chan *rtp.Packet
	interrupt chan struct{}
	consumed  chan struct{}
}

func newLifecycleTrack() *lifecycleTrack {
	return &lifecycleTrack{
		packets:   make(chan *rtp.Packet, 8),
		interrupt: make(chan struct{}, 1),
		consumed:  make(chan struct{}, 8),
	}
}

func (track *lifecycleTrack) ReadRTP() (*rtp.Packet, error) {
	select {
	case packet := <-track.packets:
		track.consumed <- struct{}{}
		return packet, nil
	case <-track.interrupt:
		return nil, io.EOF
	}
}

func (track *lifecycleTrack) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		return nil
	}
	select {
	case track.interrupt <- struct{}{}:
	default:
	}
	return nil
}

func (fixture *controlPlaneFixture) Allocate(_ context.Context, _ CaptureFence, segments []SegmentDescriptor) ([]UploadTarget, error) {
	fixture.allocated = append(fixture.allocated, segments...)
	fixture.calls = append(fixture.calls, "allocate")
	if fixture.allocateErr != nil {
		return nil, fixture.allocateErr
	}
	return []UploadTarget{{UploadID: "upload-1", UploadURL: "http://object.invalid", UploadURLExpiresAt: "2026-08-25T10:01:00Z", RequiredHeaders: map[string]string{"content-type": "video/webm"}}}, nil
}
func (fixture *controlPlaneFixture) PutFile(_ context.Context, _ UploadTarget, path string) error {
	fixture.calls = append(fixture.calls, "put")
	_, err := os.Stat(path)
	return err
}
func (fixture *controlPlaneFixture) Commit(context.Context, CaptureFence, string) error {
	fixture.calls = append(fixture.calls, "commit")
	return nil
}
func (fixture *controlPlaneFixture) PutCheckpoint(_ context.Context, fence CaptureFence, sequence int32, through int64, records []CheckpointRecord) (CheckpointResult, error) {
	fixture.calls = append(fixture.calls, "checkpoint")
	fixture.checkpointThrough = append(fixture.checkpointThrough, through)
	if fixture.requiredTrackID != "" {
		found := false
		for _, record := range records {
			if record.Category == "tracks" && record.TrackID == fixture.requiredTrackID {
				found = true
				break
			}
		}
		if !found {
			return CheckpointResult{}, errors.New("checkpoint omitted pending segment track")
		}
	}
	hash, err := canonicalCheckpointHash(fence.CaptureGeneration, through, records)
	return CheckpointResult{CheckpointSequence: sequence, PayloadSHA256: hash}, err
}

func TestSameMillisecondLifecycleCheckpointsDeferUntilRepresentableBoundary(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	api := &controlPlaneFixture{}
	session := fixtureCaptureSession(
		api,
		spool,
		mustTimeline(t),
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		root,
		0,
	)
	fixed := session.startedAt
	session.runner.dependencies.Now = func() time.Time { return fixed }
	if err := session.Snapshot(context.Background(), Snapshot{
		ProtocolVersion: ProtocolVersion,
		RecordingID:     "rec-1",
		Revision:        1,
		Participants:    []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := session.Event(context.Background(), Event{
		Type:        "bot:participantUpdated",
		RecordingID: "rec-1",
		Revision:    2,
		Participant: &Participant{ParticipantID: "raw-p1", DisplayName: "Alex 2"},
	}); err != nil {
		t.Fatal(err)
	}
	if len(api.checkpointThrough) != 0 {
		t.Fatalf("same-ms checkpoint fabricated a future boundary: %v", api.checkpointThrough)
	}
	session.runner.dependencies.Now = func() time.Time { return fixed.Add(time.Millisecond) }
	if err := session.checkpoint(context.Background(), session.timelineMS(), false); err != nil {
		t.Fatal(err)
	}
	if len(api.checkpointThrough) != 1 || api.checkpointThrough[0] != 1 {
		t.Fatalf("deferred checkpoint did not use the first representable boundary: %v", api.checkpointThrough)
	}
}

func TestCheckpointRejectsTimelinePastConfiguredMaximum(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	session := fixtureCaptureSession(
		&controlPlaneFixture{},
		spool,
		mustTimeline(t),
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		root,
		0,
	)
	session.runner.dependencies.MaximumCaptureDuration = time.Second
	if err := session.checkpoint(context.Background(), 1001, false); !errors.Is(err, ErrCaptureMaximumDuration) {
		t.Fatalf("checkpoint exceeded maximum duration: %v", err)
	}
	recovery := RecoveryState{GenerationCheckpoints: []GenerationCheckpoint{{TimelineThroughMS: 1001}}}
	if err := validateRecoveryDuration(recovery, time.Second); !errors.Is(err, ErrCaptureMaximumDuration) {
		t.Fatalf("recovery exceeded maximum duration: %v", err)
	}
}

func TestLifecyclePauseFlushesAtBoundaryAndResumeStartsNextSegment(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	timeline, _ := NewTimelineState("rec-1")
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	api := &controlPlaneFixture{}
	session := fixtureCaptureSession(
		api,
		spool,
		timeline,
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		root,
		0,
	)
	now := session.startedAt
	var nowMu sync.Mutex
	session.runner.dependencies.Now = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	setNow := func(value time.Time) {
		nowMu.Lock()
		now = value
		nowMu.Unlock()
	}
	track := newLifecycleTrack()
	track.packets <- vp8Packet(1, true)
	if err := session.startTrack(context.Background(), TrackEvent{
		ProducerID: "producer-1", Source: SourceCamera, Kind: KindVideo,
		Codec: "vp8", SourceTrack: track,
	}); err != nil {
		t.Fatal(err)
	}
	<-track.consumed
	waitForStagingFile(t, root)
	setNow(session.startedAt.Add(time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 2,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(api.allocated) != 1 || api.allocated[0].EndMS != 1000 || api.allocated[0].Sequence != 0 {
		t.Fatalf("pause did not flush at its exact boundary: %+v", api.allocated)
	}

	setNow(session.startedAt.Add(2 * time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerResumed", RecordingID: "rec-1", Revision: 3,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	track.packets <- vp8Packet(2, true)
	<-track.consumed
	waitForStagingFile(t, root)
	setNow(session.startedAt.Add(3 * time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 4,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(api.allocated) != 2 || api.allocated[1].StartMS != 2000 ||
		api.allocated[1].EndMS != 3000 || api.allocated[1].Sequence != 1 {
		t.Fatalf("resume did not create the next independent segment: %+v", api.allocated)
	}
}

func waitForStagingFile(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	pattern := filepath.Join(root, "capture-v2", "rec-1", "staging", "*.part")
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(pattern)
		if len(matches) > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("track writer did not create staging media")
}
func (fixture *controlPlaneFixture) Recover(context.Context, CaptureFence) (RecoveryState, error) {
	fixture.recoverCalls++
	if fixture.recoveryErr != nil {
		return RecoveryState{}, fixture.recoveryErr
	}
	if len(fixture.recoveries) > 0 {
		index := min(fixture.recoverCalls-1, len(fixture.recoveries)-1)
		return fixture.recoveries[index], nil
	}
	return fixture.recovery, nil
}
func (fixture *controlPlaneFixture) RecoverCurrent(context.Context, string, string, string) (RecoveryState, error) {
	return fixture.recovery, nil
}
func (fixture *controlPlaneFixture) Seal(_ context.Context, _ CaptureFence, request SealRequest) (SealResult, error) {
	fixture.calls = append(fixture.calls, "seal")
	fixture.sealRequest = &request
	return SealResult{RecordingID: "rec-1", Status: "CAPTURED", Completeness: request.Completeness, CompositionJobID: "job-1", ManifestSHA256: string(make([]byte, 64))}, nil
}

func TestSegmentPipelineSurvivesAPIOutageAndRetriesAfterRestart(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	fence := CaptureFence{"rec-1", "worker-1", "process-1", 1}
	attemptFence := AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"}
	if _, err := spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	timeline, _ := NewTimelineState("rec-1")
	producer := Producer{ProducerID: "raw-camera", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindVideo, Source: SourceCamera}
	if err := timeline.ApplySnapshot(Snapshot{ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1, Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}}, Producers: []Producer{producer}}, 0); err != nil {
		t.Fatal(err)
	}
	failedAPI := &controlPlaneFixture{allocateErr: &CaptureAPIError{Status: http.StatusServiceUnavailable, Code: "S3_UNAVAILABLE"}}
	session := fixtureCaptureSession(failedAPI, spool, timeline, fence, root, 0)
	media := []byte("closed segment")
	digest := sha256.Sum256(media)
	path := writeTestStaging(t, root, "outage-segment.part", media)
	descriptor := fixtureSegmentDescriptor(int64(len(media)), hex.EncodeToString(digest[:]))
	if err := session.segmentReady(context.Background(), descriptor, path); err == nil {
		t.Fatal("expected allocation outage")
	}
	manifest, err := spool.Snapshot("rec-1")
	if err != nil || len(manifest.Segments) != 1 || manifest.Segments[0].Committed {
		t.Fatalf("uncommitted segment was not retained: %+v %v", manifest, err)
	}
	if len(failedAPI.calls) != 2 || failedAPI.calls[0] != "checkpoint" || failedAPI.calls[1] != "allocate" {
		t.Fatalf("checkpoint was not durable before allocation: %v", failedAPI.calls)
	}
	if session.sequences[descriptor.TrackID] != descriptor.Sequence+1 {
		t.Fatal("durably promoted segment did not reserve its sequence before upload")
	}

	restartedSpool, _ := NewDurableSpool(root, 1_000_000)
	recoveredManifest, err := restartedSpool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture")
	if err != nil {
		t.Fatal(err)
	}
	healthyAPI := &controlPlaneFixture{}
	restarted := fixtureCaptureSession(healthyAPI, restartedSpool, timeline, fence, root, recoveredManifest.LastCheckpointSequence+1)
	if err := restarted.resumePending(context.Background(), recoveredManifest); err != nil {
		t.Fatal(err)
	}
	if got := healthyAPI.calls; len(got) != 4 || got[0] != "checkpoint" || got[1] != "allocate" || got[2] != "put" || got[3] != "commit" {
		t.Fatalf("unexpected restart order: %v", got)
	}
	final, err := restartedSpool.Snapshot("rec-1")
	if err != nil || len(final.Segments) != 0 || final.SourceBytes != 0 {
		t.Fatalf("committed local source was not reclaimed: %+v %v", final, err)
	}
}

func TestSegmentPipelineFailsClosedOnStaleFence(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	fence := CaptureFence{"rec-1", "worker-1", "process-1", 1}
	attemptFence := AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"}
	_, _ = spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture")
	timeline, _ := NewTimelineState("rec-1")
	producer := Producer{ProducerID: "raw-camera", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindVideo, Source: SourceCamera}
	_ = timeline.ApplySnapshot(Snapshot{ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1, Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}}, Producers: []Producer{producer}}, 0)
	api := &controlPlaneFixture{allocateErr: &CaptureAPIError{Status: http.StatusConflict, Code: "STALE_CAPTURE_GENERATION"}}
	session := fixtureCaptureSession(api, spool, timeline, fence, root, 0)
	media := []byte("closed segment")
	digest := sha256.Sum256(media)
	path := writeTestStaging(t, root, "stale-segment.part", media)
	err := session.segmentReady(context.Background(), fixtureSegmentDescriptor(int64(len(media)), hex.EncodeToString(digest[:])), path)
	var failure *CaptureAPIError
	if !errors.As(err, &failure) || failure.Code != "STALE_CAPTURE_GENERATION" {
		t.Fatalf("stale fence was not preserved: %v", err)
	}
}

func TestPendingSegmentRecoveryWaitsForSignalingIdentity(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	fence := CaptureFence{"rec-1", "worker-1", "process-1", 1}
	attemptFence := AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"}
	_, _ = spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture")
	media := []byte("closed before checkpoint")
	digest := sha256.Sum256(media)
	descriptor := fixtureSegmentDescriptor(int64(len(media)), hex.EncodeToString(digest[:]))
	path := writeTestStaging(t, root, "pre-checkpoint-crash.part", media)
	if _, err := spool.StoreSegment(context.Background(), "rec-1", descriptor, path); err != nil {
		t.Fatal(err)
	}

	restartedSpool, _ := NewDurableSpool(root, 1_000_000)
	manifest, err := restartedSpool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture")
	if err != nil {
		t.Fatal(err)
	}
	timeline, _ := NewTimelineState("rec-1")
	api := &controlPlaneFixture{requiredTrackID: descriptor.TrackID}
	session := fixtureCaptureSession(api, restartedSpool, timeline, fence, root, 0)
	if err := session.resumePending(context.Background(), manifest); err == nil {
		t.Fatal("pending segment resumed before signaling restored its opaque identity")
	}
	producer := Producer{ProducerID: "producer-1", ParticipantID: "participant-1", DisplayName: "Alex", Kind: KindVideo, Source: SourceCamera}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "participant-1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := session.resumePending(context.Background(), manifest); err != nil {
		t.Fatalf("pending segment did not converge after signaling identity recovery: %v", err)
	}
}

func TestFinalizationOnlyRecoversAndSealsPartialWithoutOpeningInputs(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	end := int64(8000)
	api := &controlPlaneFixture{recovery: RecoveryState{
		CaptureGeneration: 3, CaptureStartedAt: "2026-08-25T10:00:00Z", BindingMode: "seal-only", TerminalTimelineEnd: &end,
		GenerationCheckpoints: []GenerationCheckpoint{{CaptureGeneration: 2, CheckpointSequence: 7, TimelineThroughMS: 7000, PayloadSHA256: string(make([]byte, 64))}},
	}}
	runner := fixtureCaptureSession(api, spool, mustTimeline(t), CaptureFence{"rec-1", "worker-1", "process-1", 3}, root, 0).runner
	err := runner.Finalize(context.Background(), FinalizationRequest{AttemptID: "attempt-2", RoomID: "room-1", RecordingID: "rec-1", IntegrationID: "crm-1", RequestedAt: "2026-08-25T10:00:00Z", WorkerID: "worker-1", WorkerGenerationID: "process-1"})
	if err != nil {
		t.Fatal(err)
	}
	if api.sealRequest == nil || api.sealRequest.Completeness != "partial" || api.sealRequest.LastCheckpointCaptureGeneration != 2 || api.sealRequest.TimelineEndMS != end {
		t.Fatalf("unexpected seal-only request: %+v", api.sealRequest)
	}
}

func TestCleanStopWaitsForTerminalFenceBeforeCompleteSeal(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	timeline, _ := NewTimelineState("rec-1")
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	end := int64(1000)
	api := &controlPlaneFixture{recoveries: []RecoveryState{
		{CaptureGeneration: 1, BindingMode: "capture"},
		{CaptureGeneration: 1, BindingMode: "capture", TerminalTimelineEnd: &end},
	}}
	session := fixtureCaptureSession(
		api,
		spool,
		timeline,
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		root,
		0,
	)
	if err := session.prepareCleanStop(); err != nil {
		t.Fatal(err)
	}
	if api.sealRequest != nil {
		t.Fatal("capture sealed before signaling inputs reached a terminal fence")
	}
	if err := session.finishCleanStop(); err != nil {
		t.Fatal(err)
	}
	if api.recoverCalls != 2 || api.sealRequest == nil || api.sealRequest.Completeness != "complete" {
		t.Fatalf("terminal polling/seal mismatch: calls=%d seal=%+v", api.recoverCalls, api.sealRequest)
	}
}

func TestCleanStopTerminalPollingTimeoutDoesNotSeal(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	api := &controlPlaneFixture{recovery: RecoveryState{CaptureGeneration: 1, BindingMode: "capture"}}
	session := fixtureCaptureSession(
		api,
		spool,
		mustTimeline(t),
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		root,
		0,
	)
	session.runner.dependencies.FinalizeTimeout = 80 * time.Millisecond
	if err := session.finishCleanStop(); err == nil {
		t.Fatal("terminal polling timeout was accepted")
	}
	if api.sealRequest != nil {
		t.Fatal("timeout path sealed without an authoritative terminal fence")
	}
}

func TestCleanStopTerminalFenceFailureDoesNotSeal(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	stale := &CaptureAPIError{Status: http.StatusConflict, Code: "STALE_CAPTURE_GENERATION"}
	api := &controlPlaneFixture{recoveryErr: stale}
	session := fixtureCaptureSession(
		api,
		spool,
		mustTimeline(t),
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		root,
		0,
	)
	err := session.finishCleanStop()
	var failure *CaptureAPIError
	if !errors.As(err, &failure) || failure.Code != stale.Code {
		t.Fatalf("terminal fence failure was not preserved: %v", err)
	}
	if api.sealRequest != nil {
		t.Fatal("stale terminal fence path sealed the capture")
	}
}

func TestProducerUpdatedDropsTrackInputWhenLogicalIdentityChanges(t *testing.T) {
	timeline, _ := NewTimelineState("rec-1")
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion,
		RecordingID:     "rec-1",
		Revision:        1,
		Participants: []Participant{
			{ParticipantID: "raw-p1", DisplayName: "Alex"},
			{ParticipantID: "raw-p2", DisplayName: "Sam"},
		},
		Producers: []Producer{{
			ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
			Kind: KindVideo, Source: SourceCamera,
		}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	session := &captureSession{
		fence:       CaptureFence{RecordingID: "rec-1"},
		timeline:    timeline,
		trackInputs: map[string]TrackEvent{"producer-1": {ProducerID: "producer-1", Source: SourceCamera}},
	}
	event := Event{
		Type: "bot:producerUpdated", RecordingID: "rec-1", Revision: 2,
		Producer: &Producer{
			ProducerID: "producer-1", ParticipantID: "raw-p2", DisplayName: "Sam",
			Kind: KindVideo, Source: SourceScreen,
		},
	}
	session.reconcileEventInputs(event)
	if _, exists := session.trackInputs["producer-1"]; exists {
		t.Fatal("producer update reused a TrackEvent belonging to the prior logical track")
	}
}

func mustTimeline(t *testing.T) *TimelineState {
	t.Helper()
	timeline, err := NewTimelineState("rec-1")
	if err != nil {
		t.Fatal(err)
	}
	return timeline
}

func fixtureCaptureSession(api CaptureControlPlane, spool *DurableSpool, timeline *TimelineState, fence CaptureFence, root string, sequence int32) *captureSession {
	runner, _ := NewCaptureAttemptRunner(CaptureRunnerDependencies{API: api, Spool: spool, Muxer: copyMuxer{}, Dial: func(context.Context, string, string, SessionIdentity) (SignalingClient, error) {
		return nil, errors.New("not used")
	}, NewPeer: func() (CapturePeer, error) { return nil, errors.New("not used") }, StorageRoot: root, SegmentDuration: time.Second, MaximumSegmentDuration: 2 * time.Second, FinalizeTimeout: time.Second, Now: time.Now})
	return &captureSession{
		runner: runner, fence: fence, timeline: timeline, startedAt: time.Now(),
		checkpointSequence: sequence, writers: map[string]*RTPTrackWriter{},
		trackInputs: map[string]TrackEvent{}, sequences: map[string]int32{},
	}
}
