package recording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	readsignal "conference/recording-bot/internal/signaling"

	"github.com/pion/rtp"
)

type controlPlaneFixture struct {
	calls                 []string
	allocateErr           error
	recovery              RecoveryState
	recoveries            []RecoveryState
	recoveryErr           error
	recoverCalls          int
	sealRequest           *SealRequest
	sealErr               error
	failureCode           string
	failureMessage        string
	checkpointThrough     []int64
	checkpointRecords     [][]CheckpointRecord
	checkpointErr         error
	allocated             []SegmentDescriptor
	requiredTrackID       string
	requireCommitCoverage bool
	recordRecoveryCalls   bool
	checkpointHook        func(int64)
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
	if fixture.requireCommitCoverage && (len(fixture.allocated) == 0 ||
		len(fixture.checkpointThrough) == 0 ||
		fixture.checkpointThrough[len(fixture.checkpointThrough)-1] < fixture.allocated[len(fixture.allocated)-1].EndMS) {
		return &CaptureAPIError{Status: http.StatusConflict, Code: "CHECKPOINT_REQUIRED"}
	}
	return nil
}
func (fixture *controlPlaneFixture) PutCheckpoint(_ context.Context, fence CaptureFence, sequence int32, through int64, records []CheckpointRecord) (CheckpointResult, error) {
	if fixture.checkpointHook != nil {
		fixture.checkpointHook(through)
	}
	fixture.calls = append(fixture.calls, "checkpoint")
	fixture.checkpointThrough = append(fixture.checkpointThrough, through)
	fixture.checkpointRecords = append(fixture.checkpointRecords, append([]CheckpointRecord{}, records...))
	if fixture.checkpointErr != nil {
		err := fixture.checkpointErr
		fixture.checkpointErr = nil
		return CheckpointResult{}, err
	}
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
	const recordingID = "rec_11111111-1111-4111-8111-111111111111"
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	timeline, _ := NewTimelineState(recordingID)
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: recordingID, Revision: 1,
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
		CaptureFence{recordingID, "worker-1", "process-1", 1},
		root,
		0,
	)
	session.spoolFence = newSpoolMutationFence(
		AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"},
		1,
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
	waitForStagingFile(t, root, recordingID)
	setNow(session.startedAt.Add(500 * time.Millisecond))
	updated := producer
	updated.DisplayName = "Alex Updated"
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerUpdated", RecordingID: recordingID, Revision: 2,
		Producer: &updated,
	}); err != nil {
		t.Fatal(err)
	}
	setNow(session.startedAt.Add(600 * time.Millisecond))
	track.packets <- vp8Packet(2, true)
	<-track.consumed
	waitForStagingFile(t, root, recordingID)
	setNow(session.startedAt.Add(time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerPaused", RecordingID: recordingID, Revision: 3,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(api.allocated) != 2 || api.allocated[0].StartMS != 0 || api.allocated[0].EndMS != 500 ||
		api.allocated[0].Sequence != 0 || api.allocated[1].StartMS != 600 ||
		api.allocated[1].EndMS != 1000 || api.allocated[1].Sequence != 1 {
		t.Fatalf("pause did not flush at its exact boundary: %+v", api.allocated)
	}
	records, err := timeline.RecordsThrough(1000, false)
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := TrackID(recordingID, ParticipantInstanceID(recordingID, "raw-p1"), SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	foundHandoffGap := false
	for _, record := range records {
		if record.Category == "gaps" && record.TrackID == trackID && record.StartMS != nil &&
			record.EndMS != nil && *record.StartMS == 500 && *record.EndMS == 600 &&
			record.Reason == "source_unavailable" {
			foundHandoffGap = true
		}
	}
	if !foundHandoffGap {
		t.Fatal("same-track producer update did not preserve the writer handoff boundary")
	}

	setNow(session.startedAt.Add(2 * time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerResumed", RecordingID: recordingID, Revision: 4,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	track.packets <- vp8Packet(3, true)
	<-track.consumed
	waitForStagingFile(t, root, recordingID)
	setNow(session.startedAt.Add(3 * time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerPaused", RecordingID: recordingID, Revision: 5,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(api.allocated) != 3 || api.allocated[2].StartMS != 2000 ||
		api.allocated[2].EndMS != 3000 || api.allocated[2].Sequence != 2 {
		t.Fatalf("resume did not create the next independent segment: %+v", api.allocated)
	}
}

func TestCheckpointWaitsForPendingInitialTrackCoverage(t *testing.T) {
	const recordingID = "rec_22222222-2222-4222-8222-222222222222"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	attemptFence := AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"}
	if _, err := spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	timeline, _ := NewTimelineState(recordingID)
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: recordingID, Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	api := &controlPlaneFixture{}
	session := fixtureCaptureSession(
		api, spool, timeline, CaptureFence{recordingID, "worker-1", "process-1", 1}, root, 0,
	)
	session.spoolFence = newSpoolMutationFence(attemptFence, 1)
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
	if err := session.startTrack(context.Background(), TrackEvent{
		ProducerID: "producer-1", Source: SourceCamera, Kind: KindVideo,
		Codec: "vp8", SourceTrack: track,
	}); err != nil {
		t.Fatal(err)
	}
	setNow(session.startedAt.Add(500 * time.Millisecond))
	if err := session.checkpoint(context.Background(), 500, false); err != nil {
		t.Fatal(err)
	}
	if len(api.checkpointThrough) != 0 {
		t.Fatalf("checkpoint crossed pending initial coverage: %v", api.checkpointThrough)
	}
	setNow(session.startedAt.Add(600 * time.Millisecond))
	track.packets <- vp8Packet(1, true)
	<-track.consumed
	waitForStagingFile(t, root, recordingID)
	setNow(session.startedAt.Add(time.Second))
	if err := session.Event(context.Background(), Event{
		Type: "bot:producerPaused", RecordingID: recordingID, Revision: 2,
		ProducerID: "producer-1",
	}); err != nil {
		t.Fatal(err)
	}
	if len(api.checkpointThrough) != 1 || api.checkpointThrough[0] != 1000 {
		t.Fatalf("resolved coverage did not checkpoint canonically: %v", api.checkpointThrough)
	}
	foundGap := false
	for _, record := range api.checkpointRecords[0] {
		if record.Category == "gaps" && record.StartMS != nil && record.EndMS != nil &&
			*record.StartMS == 0 && *record.EndMS == 600 && record.Reason == "source_unavailable" {
			foundGap = true
		}
	}
	if !foundGap {
		t.Fatalf("resolved checkpoint omitted initial coverage gap: %+v", api.checkpointRecords[0])
	}
}

func TestStartingWriterPreventsResolvedCoverageFromBeingReopened(t *testing.T) {
	const recordingID = "rec_33333333-3333-4333-8333-333333333333"
	timeline, _ := NewTimelineState(recordingID)
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: recordingID, Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	trackID, _, exists := timeline.TrackForProducer(producer.ProducerID)
	if !exists {
		t.Fatal("active track was not created")
	}
	if err := timeline.BeginTrackCoverage(trackID, 0); err != nil {
		t.Fatal(err)
	}
	if err := timeline.ResolveTrackCoverage(trackID, 0, 600); err != nil {
		t.Fatal(err)
	}
	session := &captureSession{
		timeline: timeline,
		writers:  map[string]*RTPTrackWriter{},
		startingWriters: map[string]struct{}{
			trackID: {},
		},
	}
	if err := session.beginTrackCoverage(producer.ProducerID); err != nil {
		t.Fatal(err)
	}
	boundary, err := timeline.CheckpointBoundary(600, false)
	if err != nil || boundary != 600 {
		t.Fatalf("resolved coverage was reopened during writer registration: %d %v", boundary, err)
	}
}

func TestFailAndSealClosesWritersAtExistingTerminalBoundary(t *testing.T) {
	const recordingID = "rec_44444444-4444-4444-8444-444444444444"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	attemptFence := AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"}
	if _, err := spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	timeline, _ := NewTimelineState(recordingID)
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: recordingID, Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	terminalEnd := int64(1000)
	api := &controlPlaneFixture{
		recovery:            RecoveryState{TerminalTimelineEnd: &terminalEnd},
		recordRecoveryCalls: true,
	}
	session := fixtureCaptureSession(
		api, spool, timeline, CaptureFence{recordingID, "worker-1", "process-1", 1}, root, 0,
	)
	session.spoolFence = newSpoolMutationFence(attemptFence, 1)
	now := session.startedAt
	var nowMu sync.Mutex
	session.runner.dependencies.Now = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
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
	waitForStagingFile(t, root, recordingID)
	nowMu.Lock()
	now = session.startedAt.Add(1100 * time.Millisecond)
	nowMu.Unlock()
	err = session.failAndSeal(errors.New("signaling closed after room termination"))
	if !errors.Is(err, ErrCaptureIncomplete) || errors.Is(err, ErrFinalizationFailed) {
		t.Fatalf("terminal recovery did not converge partial capture: %v", err)
	}
	if len(api.allocated) != 1 || api.allocated[0].StartMS != 0 || api.allocated[0].EndMS != terminalEnd {
		t.Fatalf("writer ignored authoritative terminal boundary: %+v", api.allocated)
	}
	for _, throughMS := range api.checkpointThrough {
		if throughMS > terminalEnd {
			t.Fatalf("checkpoint crossed terminal timeline: %v", api.checkpointThrough)
		}
	}
	if api.sealRequest == nil || api.sealRequest.TimelineEndMS != terminalEnd ||
		api.sealRequest.Completeness != "partial" {
		t.Fatalf("unexpected recovered seal request: %+v", api.sealRequest)
	}
	if len(api.calls) == 0 || api.calls[0] != "recover" {
		t.Fatalf("writer closed before terminal recovery: %v", api.calls)
	}
}

func TestFailAndSealRetainsSecondRecoveryFallback(t *testing.T) {
	const recordingID = "rec_55555555-5555-4555-8555-555555555555"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	attemptFence := AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"}
	if _, err := spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	timeline, _ := NewTimelineState(recordingID)
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: recordingID, Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	terminalEnd := int64(1000)
	api := &controlPlaneFixture{
		recoveries:          []RecoveryState{{}, {}, {TerminalTimelineEnd: &terminalEnd}},
		recordRecoveryCalls: true,
	}
	session := fixtureCaptureSession(
		api, spool, timeline, CaptureFence{recordingID, "worker-1", "process-1", 1}, root, 0,
	)
	session.spoolFence = newSpoolMutationFence(attemptFence, 1)
	now := session.startedAt
	var nowMu sync.Mutex
	session.runner.dependencies.Now = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
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
	waitForStagingFile(t, root, recordingID)
	nowMu.Lock()
	now = session.startedAt.Add(900 * time.Millisecond)
	nowMu.Unlock()
	err = session.failAndSeal(errors.New("capture transport failed before terminal close"))
	if !errors.Is(err, ErrCaptureIncomplete) || errors.Is(err, ErrFinalizationFailed) {
		t.Fatalf("fallback recovery did not converge partial capture: %v", err)
	}
	if api.recoverCalls != 3 {
		t.Fatalf("fallback did not retry terminal recovery: %d", api.recoverCalls)
	}
	if len(api.allocated) != 1 || api.allocated[0].EndMS != 900 {
		t.Fatalf("fallback writer did not close at local boundary: %+v", api.allocated)
	}
	if len(api.checkpointThrough) != 2 || api.checkpointThrough[0] != 900 || api.checkpointThrough[1] != terminalEnd {
		t.Fatalf("fallback checkpoints are not bounded: %v", api.checkpointThrough)
	}
	if api.sealRequest == nil || api.sealRequest.TimelineEndMS != terminalEnd {
		t.Fatalf("fallback did not seal at recovered terminal: %+v", api.sealRequest)
	}
	if len(api.calls) < 7 || api.calls[0] != "recover" || api.calls[5] != "recover" || api.calls[6] != "recover" {
		t.Fatalf("unexpected fallback operation order: %v", api.calls)
	}
}

func TestFailAndSealPreservesCauseAndWriterCloseError(t *testing.T) {
	const recordingID = "rec_66666666-6666-4666-8666-666666666666"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	attemptFence := AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"}
	if _, err := spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	terminalEnd := int64(0)
	api := &controlPlaneFixture{recovery: RecoveryState{TerminalTimelineEnd: &terminalEnd}}
	session := fixtureCaptureSession(
		api, spool, mustTimeline(t), CaptureFence{recordingID, "worker-1", "process-1", 1}, root, 0,
	)
	session.spoolFence = newSpoolMutationFence(attemptFence, 1)
	closeFailure := errors.New("close writer")
	done := make(chan struct{})
	close(done)
	session.writers["trk_failed"] = &RTPTrackWriter{
		track:  &trackFixture{release: make(chan struct{})},
		cancel: func() {},
		done:   done,
		result: closeFailure,
	}
	cause := errors.New("capture transport failed")
	err = session.failAndSeal(cause)
	if !errors.Is(err, cause) || !errors.Is(err, closeFailure) {
		t.Fatalf("failure causes were not preserved: %v", err)
	}
}

func waitForStagingFile(t *testing.T, root, recordingID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	pattern := filepath.Join(root, "capture-v2", recordingID, "staging", "*.part")
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
	if fixture.recordRecoveryCalls {
		fixture.calls = append(fixture.calls, "recover")
	}
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
	if fixture.sealErr != nil {
		return SealResult{}, fixture.sealErr
	}
	return SealResult{RecordingID: "rec-1", Status: "CAPTURED", Completeness: request.Completeness, CompositionJobID: "job-1", ManifestSHA256: string(make([]byte, 64))}, nil
}

func (fixture *controlPlaneFixture) Fail(_ context.Context, _ CaptureFence, code string, message string) error {
	fixture.calls = append(fixture.calls, "fail")
	fixture.failureCode = code
	fixture.failureMessage = message
	return nil
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

func TestSegmentPipelineUploadsAfterTerminalFenceRejectsPeriodicCheckpoint(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	fence := CaptureFence{"rec-1", "worker-1", "process-1", 1}
	attemptFence := AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"}
	_, _ = spool.Begin(context.Background(), attemptFence, 1, "2026-08-25T10:00:00Z", "capture")
	timeline := mustTimeline(t)
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "participant-1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "participant-1", DisplayName: "Alex"}},
		Producers:    []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	terminalEnd := int64(1_500)
	api := &controlPlaneFixture{
		checkpointErr: &CaptureAPIError{
			Status: http.StatusConflict,
			Code:   "TERMINAL_TIMELINE_CONFLICT",
		},
		recovery: RecoveryState{
			CaptureGeneration:   1,
			CaptureStartedAt:    "2026-08-25T10:00:00Z",
			BindingMode:         "capture",
			TerminalTimelineEnd: &terminalEnd,
		},
		requireCommitCoverage: true,
		recordRecoveryCalls:   true,
	}
	session := fixtureCaptureSession(api, spool, timeline, fence, root, 0)
	media := []byte("segment already closed when close-inputs won the race")
	digest := sha256.Sum256(media)
	path := writeTestStaging(t, root, "terminal-fence-segment.part", media)
	descriptor := fixtureSegmentDescriptor(int64(len(media)), hex.EncodeToString(digest[:]))

	if err := session.segmentReady(context.Background(), descriptor, path); err != nil {
		t.Fatal(err)
	}
	if got := api.calls; len(got) != 2 || got[0] != "checkpoint" || got[1] != "recover" {
		t.Fatalf("terminal-fenced segment was not deferred: %v", got)
	}
	manifest, err := spool.Snapshot("rec-1")
	if err != nil || len(manifest.Segments) != 1 || manifest.Segments[0].Committed {
		t.Fatalf("terminal-fenced segment was not retained: %+v %v", manifest, err)
	}
	if err := timeline.ApplyEvent(Event{
		Type: "bot:producerClosed", RecordingID: "rec-1", Revision: 2, ProducerID: producer.ProducerID,
	}, 1_400); err != nil {
		t.Fatal(err)
	}
	if err := session.finishCleanStopWithRecovery(api.recovery); err != nil {
		t.Fatal(err)
	}
	if got := api.calls; len(got) != 7 || got[2] != "checkpoint" || got[3] != "allocate" ||
		got[4] != "put" || got[5] != "commit" || got[6] != "seal" {
		t.Fatalf("deferred segment did not resume after stable terminal checkpoint: %v", got)
	}
	if got := api.checkpointThrough; len(got) != 2 || got[0] != descriptor.EndMS || got[1] != terminalEnd {
		t.Fatalf("unexpected checkpoint boundaries: %v", got)
	}
	for _, record := range api.checkpointRecords[1] {
		if (record.Category == "participantIntervals" || record.Category == "trackIntervals") &&
			record.EndMS == nil {
			t.Fatalf("terminal coverage checkpoint retained an open interval: %+v", record)
		}
	}
	manifest, err = spool.Snapshot("rec-1")
	if err != nil || len(manifest.Segments) != 0 {
		t.Fatalf("committed terminal-fenced segment was not reclaimed: %+v %v", manifest, err)
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
	if _, err := spool.StoreSegment(context.Background(), testMutationFence(), descriptor, path); err != nil {
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

func TestFinalizationPersistsTerminalManifestConflict(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	end := int64(8000)
	api := &controlPlaneFixture{
		sealErr: &CaptureAPIError{Status: http.StatusConflict, Code: "CAPTURE_MANIFEST_CONFLICT"},
		recovery: RecoveryState{
			CaptureGeneration: 3, CaptureStartedAt: "2026-08-25T10:00:00Z", BindingMode: "seal-only", TerminalTimelineEnd: &end,
			GenerationCheckpoints: []GenerationCheckpoint{{CaptureGeneration: 2, CheckpointSequence: 7, TimelineThroughMS: 7000, PayloadSHA256: string(make([]byte, 64))}},
		},
	}
	runner := fixtureCaptureSession(api, spool, mustTimeline(t), CaptureFence{"rec-1", "worker-1", "process-1", 3}, root, 0).runner
	err = runner.Finalize(context.Background(), FinalizationRequest{AttemptID: "attempt-2", RoomID: "room-1", RecordingID: "rec-1", IntegrationID: "crm-1", RequestedAt: "2026-08-25T10:00:00Z", WorkerID: "worker-1", WorkerGenerationID: "process-1"})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.Snapshot("rec-1")
	if err != nil || manifest.Status != "failed" {
		t.Fatalf("terminal spool status was not persisted: %+v %v", manifest, err)
	}
	if api.failureCode != "BOT_CAPTURE_INCOMPLETE" || api.failureMessage == "" {
		t.Fatalf("terminal API failure was not persisted: %q %q", api.failureCode, api.failureMessage)
	}
}

func TestFinalizationWithoutCheckpointPersistsTerminalFailure(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	end := int64(8000)
	api := &controlPlaneFixture{recovery: RecoveryState{
		CaptureGeneration: 3, CaptureStartedAt: "2026-08-25T10:00:00Z",
		BindingMode: "seal-only", TerminalTimelineEnd: &end,
	}}
	runner := fixtureCaptureSession(
		api,
		spool,
		mustTimeline(t),
		CaptureFence{"rec-1", "worker-1", "process-1", 3},
		root,
		0,
	).runner
	err = runner.Finalize(context.Background(), FinalizationRequest{
		AttemptID: "attempt-2", RoomID: "room-1", RecordingID: "rec-1",
		IntegrationID: "crm-1", RequestedAt: "2026-08-25T10:00:00Z",
		WorkerID: "worker-1", WorkerGenerationID: "process-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := spool.Snapshot("rec-1")
	if err != nil || manifest.Status != "failed" {
		t.Fatalf("empty terminal spool status was not persisted: %+v %v", manifest, err)
	}
	if api.failureCode != "BOT_CAPTURE_INCOMPLETE" || api.failureMessage == "" {
		t.Fatalf("empty terminal API failure was not persisted: %q %q", api.failureCode, api.failureMessage)
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

func TestCleanStopAfterRootCancellationClosesAllWritersAtTerminalFence(t *testing.T) {
	const recordingID = "rec_11111111-1111-4111-8111-111111111111"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	attemptFence := AttemptFence{
		AttemptID:          "attempt-1",
		RecordingID:        recordingID,
		WorkerID:           "worker-1",
		WorkerGenerationID: "process-1",
	}
	spoolFence := newSpoolMutationFence(attemptFence, 1)
	if _, err := spool.Begin(
		context.Background(),
		attemptFence,
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	); err != nil {
		t.Fatal(err)
	}
	timeline, err := NewTimelineState(recordingID)
	if err != nil {
		t.Fatal(err)
	}
	participant := Participant{ParticipantID: "raw-p1", DisplayName: "Alex"}
	producers := []Producer{
		{
			ProducerID: "producer-mic", ParticipantID: participant.ParticipantID,
			DisplayName: participant.DisplayName, Kind: KindAudio, Source: SourceMic,
		},
		{
			ProducerID: "producer-camera", ParticipantID: participant.ParticipantID,
			DisplayName: participant.DisplayName, Kind: KindVideo, Source: SourceCamera,
		},
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion,
		RecordingID:     recordingID,
		Revision:        1,
		Participants:    []Participant{participant},
		Producers:       producers,
	}, 0); err != nil {
		t.Fatal(err)
	}
	terminalEnd := int64(250)
	api := &controlPlaneFixture{recovery: RecoveryState{
		CaptureGeneration:     1,
		CaptureStartedAt:      "2026-08-25T10:00:00Z",
		BindingMode:           "capture",
		TerminalTimelineEnd:   &terminalEnd,
		GenerationCheckpoints: []GenerationCheckpoint{},
	}}
	session := fixtureCaptureSession(
		api,
		spool,
		timeline,
		CaptureFence{
			RecordingID:        recordingID,
			WorkerID:           "worker-1",
			WorkerGenerationID: "process-1",
			CaptureGeneration:  1,
		},
		root,
		0,
	)
	session.spoolFence = spoolFence
	fixedNow := session.startedAt.Add(100 * time.Millisecond)
	session.runner.dependencies.Now = func() time.Time { return fixedNow }

	rootContext, cancelRoot := context.WithCancel(context.Background())
	captureContext := context.WithoutCancel(rootContext)
	writers := make([]*RTPTrackWriter, 0, len(producers))
	for _, producer := range producers {
		track := newLifecycleTrack()
		packetWritten := make(chan struct{}, 1)
		participantID := ParticipantInstanceID(recordingID, participant.ParticipantID)
		trackID, trackErr := TrackID(recordingID, participantID, producer.Source)
		if trackErr != nil {
			t.Fatal(trackErr)
		}
		if err := timeline.BeginTrackCoverage(trackID, 0); err != nil {
			t.Fatal(err)
		}
		codec := "audio/opus"
		packet := &rtp.Packet{Header: rtp.Header{SequenceNumber: 1}, Payload: []byte{1}}
		if producer.Source == SourceCamera {
			codec = "video/VP8"
			packet = vp8Packet(1, true)
		}
		writer, writerErr := StartRTPTrackWriter(captureContext, RTPTrackWriterConfig{
			Track:                 track,
			ParticipantInstanceID: participantID,
			TrackID:               trackID,
			Source:                producer.Source,
			Codec:                 codec,
			SpoolDirectory: filepath.Join(
				root,
				"capture-v2",
				recordingID,
				"staging",
			),
			SegmentDuration:        time.Second,
			MaximumSegmentDuration: 2 * time.Second,
			TimelineClock:          session.timelineMS,
			Muxer:                  copyMuxer{},
			OnSegment:              session.segmentReady,
			OnInitialGap: func(startMS, endMS int64) error {
				return timeline.ResolveTrackCoverage(trackID, startMS, endMS)
			},
			OperationTimeout: time.Second,
			RecordingID:      recordingID,
			StagingBudget: fencedStagingBudget{
				spool: spool,
				fence: spoolFence,
			},
			newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
				return notifyingPacketWriter{
					packetCaptureWriter: packetCaptureWriter{path: path, payloads: &[][]byte{}},
					written:             packetWritten,
				}, nil
			},
		})
		if writerErr != nil {
			t.Fatal(writerErr)
		}
		session.writers[trackID] = writer
		writers = append(writers, writer)
		track.packets <- packet
		select {
		case <-track.consumed:
		case <-time.After(time.Second):
			t.Fatal("writer did not consume its initial packet")
		}
		select {
		case <-packetWritten:
		case <-time.After(time.Second):
			t.Fatal("writer did not persist its initial packet")
		}
	}

	cancelRoot()
	for _, writer := range writers {
		select {
		case <-writer.done:
			t.Fatal("root cancellation closed a writer before the authoritative fence")
		default:
		}
	}
	if err := session.cleanStop(); err != nil {
		t.Fatal(err)
	}
	if api.sealRequest == nil || api.sealRequest.TimelineEndMS != terminalEnd {
		t.Fatalf("seal did not use terminal timeline: %+v", api.sealRequest)
	}
	if len(api.checkpointThrough) == 0 || api.checkpointThrough[len(api.checkpointThrough)-1] != terminalEnd {
		t.Fatalf("final checkpoint did not use terminal timeline: %v", api.checkpointThrough)
	}
	if len(api.allocated) != len(producers) {
		t.Fatalf("final short segments were not retained: %+v", api.allocated)
	}
	for _, segment := range api.allocated {
		if segment.EndMS != terminalEnd || segment.StartMS >= segment.EndMS {
			t.Fatalf("writer closed outside terminal boundary: %+v", segment)
		}
	}
}

func TestCaptureRunnerRootCancellationFinalizesTwoWritersAtTerminalFence(t *testing.T) {
	const recordingID = "rec_22222222-2222-4222-8222-222222222222"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	participant := Participant{ParticipantID: "raw-p1", DisplayName: "Alex"}
	producers := []Producer{
		{
			ProducerID: "producer-mic", ParticipantID: participant.ParticipantID,
			DisplayName: participant.DisplayName, Kind: KindAudio, Source: SourceMic,
		},
		{
			ProducerID: "producer-camera", ParticipantID: participant.ParticipantID,
			DisplayName: participant.DisplayName, Kind: KindVideo, Source: SourceCamera,
		},
	}
	lifecycle := make(chan Event, 1)
	signalingFixture := &captureRunnerSignaling{
		snapshot: Snapshot{
			ProtocolVersion: ProtocolVersion,
			RecordingID:     recordingID,
			Revision:        1,
			Participants:    []Participant{participant},
			Producers:       producers,
		},
		consumers: map[string]readsignal.Consumer{},
		lifecycle: lifecycle,
	}
	for index, producer := range producers {
		mimeType := "audio/opus"
		clockRate := uint32(48_000)
		channels := uint16(2)
		if producer.Source == SourceCamera {
			mimeType = "video/VP8"
			clockRate = 90_000
			channels = 0
		}
		signalingFixture.consumers[producer.ProducerID] = readsignal.Consumer{
			ID:         "consumer-" + producer.ProducerID,
			ProducerID: producer.ProducerID,
			Kind:       producer.Kind,
			RTPParameters: readsignal.RTPParameters{
				MID: "mid-" + producer.ProducerID,
				Codecs: []readsignal.RTPCodecParameters{{
					MimeType: mimeType, PayloadType: uint8(100 + index),
					ClockRate: clockRate, Channels: channels,
				}},
				Encodings: []readsignal.RTPEncodingParameters{{SSRC: uint32(1000 + index)}},
			},
		}
	}
	peer := &captureRunnerPeer{
		tracks: make(chan TrackEvent, len(producers)),
		errors: make(chan error),
	}
	tracks := make([]*lifecycleTrack, 0, len(producers))
	for _, producer := range producers {
		track := newLifecycleTrack()
		packet := &rtp.Packet{Header: rtp.Header{SequenceNumber: 1}, Payload: []byte{1}}
		codec := "audio/opus"
		if producer.Source == SourceCamera {
			packet = vp8Packet(1, true)
			codec = "video/VP8"
		}
		track.packets <- packet
		tracks = append(tracks, track)
		peer.tracks <- TrackEvent{
			ProducerID:  producer.ProducerID,
			Source:      producer.Source,
			Kind:        producer.Kind,
			Codec:       codec,
			SourceTrack: track,
		}
	}
	startedAt := time.Now().UTC().Add(-100 * time.Millisecond)
	terminalEnd := int64(250)
	api := &controlPlaneFixture{recoveries: []RecoveryState{
		{
			CaptureGeneration: 1,
			CaptureStartedAt:  startedAt.Format(time.RFC3339Nano),
			BindingMode:       "capture",
		},
		{
			CaptureGeneration:     1,
			CaptureStartedAt:      startedAt.Format(time.RFC3339Nano),
			BindingMode:           "capture",
			TerminalTimelineEnd:   &terminalEnd,
			GenerationCheckpoints: []GenerationCheckpoint{},
		},
	}}
	packetsWritten := make(chan struct{}, len(producers))
	runner, err := NewCaptureAttemptRunner(CaptureRunnerDependencies{
		API:   api,
		Spool: spool,
		Muxer: copyMuxer{},
		Dial: func(context.Context, string, string, SessionIdentity) (SignalingClient, error) {
			return signalingFixture, nil
		},
		NewPeer:                  func() (CapturePeer, error) { return peer, nil },
		StorageRoot:              root,
		SegmentDuration:          time.Second,
		MaximumSegmentDuration:   2 * time.Second,
		FinalizeTimeout:          2 * time.Second,
		MaximumCaptureDuration:   10 * time.Second,
		MediaStartupTimeout:      time.Second,
		MaximumConcurrentUploads: 1,
		Now:                      func() time.Time { return startedAt.Add(100 * time.Millisecond) },
		newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
			return notifyingPacketWriter{
				packetCaptureWriter: packetCaptureWriter{path: path, payloads: &[][]byte{}},
				written:             packetsWritten,
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(ctx, AttemptRequest{
			AttemptID:          "attempt-1",
			RoomID:             "room-1",
			RecordingID:        recordingID,
			IntegrationID:      "integration-1",
			RequestedAt:        startedAt.Format(time.RFC3339Nano),
			ProtocolVersion:    CaptureProtocolVersion,
			SignalingURL:       "ws://signaling.invalid",
			Admission:          "secret",
			WorkerID:           "worker-1",
			WorkerGenerationID: "process-1",
			CaptureGeneration:  1,
			CaptureStartedAt:   startedAt.Format(time.RFC3339Nano),
		}, func() {
			ready <- struct{}{}
		}, func(ProbeEvidence) {})
	}()
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("capture runner did not become ready")
	}
	for range producers {
		select {
		case <-packetsWritten:
		case <-time.After(time.Second):
			t.Fatal("capture writer did not persist its packet")
		}
	}
	cancel()
	select {
	case runErr := <-result:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("capture runner did not finish after root cancellation")
	}
	if api.sealRequest == nil || api.sealRequest.TimelineEndMS != terminalEnd {
		t.Fatalf("seal did not use terminal timeline: %+v", api.sealRequest)
	}
	if len(api.checkpointThrough) == 0 || api.checkpointThrough[len(api.checkpointThrough)-1] != terminalEnd {
		t.Fatalf("final checkpoint did not use terminal timeline: %v", api.checkpointThrough)
	}
	if len(api.allocated) != len(producers) {
		t.Fatalf("runner lost final short segments: %+v", api.allocated)
	}
	for _, segment := range api.allocated {
		if segment.EndMS != terminalEnd {
			t.Fatalf("runner used a non-terminal writer boundary: %+v", segment)
		}
	}
}

func TestCaptureRunnerMaximumDurationUsesExplicitPartialFinalization(t *testing.T) {
	const recordingID = "rec_33333333-3333-4333-8333-333333333333"
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now().UTC().Add(-900 * time.Millisecond)
	terminalEnd := int64(1000)
	api := &controlPlaneFixture{recoveries: []RecoveryState{
		{
			CaptureGeneration: 1,
			CaptureStartedAt:  startedAt.Format(time.RFC3339Nano),
			BindingMode:       "capture",
		},
		{
			CaptureGeneration:   1,
			CaptureStartedAt:    startedAt.Format(time.RFC3339Nano),
			BindingMode:         "capture",
			TerminalTimelineEnd: &terminalEnd,
		},
	}}
	var signalingClosed atomic.Bool
	api.checkpointHook = func(int64) {
		if !signalingClosed.Load() {
			t.Error("maximum-duration final checkpoint started before signaling controller closed")
		}
	}
	signalingFixture := &captureRunnerSignaling{
		snapshot: Snapshot{
			ProtocolVersion: ProtocolVersion,
			RecordingID:     recordingID,
			Revision:        1,
			Participants: []Participant{{
				ParticipantID: "raw-p1",
				DisplayName:   "Alex",
			}},
			Producers: []Producer{},
		},
		consumers: map[string]readsignal.Consumer{},
		lifecycle: make(chan Event),
		onClose:   func() { signalingClosed.Store(true) },
	}
	peer := &captureRunnerPeer{
		tracks: make(chan TrackEvent),
		errors: make(chan error),
	}
	runner, err := NewCaptureAttemptRunner(CaptureRunnerDependencies{
		API:   api,
		Spool: spool,
		Muxer: copyMuxer{},
		Dial: func(context.Context, string, string, SessionIdentity) (SignalingClient, error) {
			return signalingFixture, nil
		},
		NewPeer:                  func() (CapturePeer, error) { return peer, nil },
		StorageRoot:              root,
		SegmentDuration:          time.Second,
		MaximumSegmentDuration:   time.Second,
		FinalizeTimeout:          2 * time.Second,
		MaximumCaptureDuration:   time.Second,
		MediaStartupTimeout:      time.Second,
		MaximumConcurrentUploads: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{}, 1)
	result := make(chan error, 1)
	go func() {
		result <- runner.Run(context.Background(), AttemptRequest{
			AttemptID:          "attempt-1",
			RoomID:             "room-1",
			RecordingID:        recordingID,
			IntegrationID:      "integration-1",
			RequestedAt:        startedAt.Format(time.RFC3339Nano),
			ProtocolVersion:    CaptureProtocolVersion,
			SignalingURL:       "ws://signaling.invalid",
			Admission:          "secret",
			WorkerID:           "worker-1",
			WorkerGenerationID: "process-1",
			CaptureGeneration:  1,
			CaptureStartedAt:   startedAt.Format(time.RFC3339Nano),
		}, func() {
			ready <- struct{}{}
		}, func(ProbeEvidence) {})
	}()
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("capture runner did not become ready before its maximum duration")
	}
	select {
	case runErr := <-result:
		if !errors.Is(runErr, ErrCaptureMaximumDuration) {
			t.Fatalf("maximum duration cause was not preserved: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("maximum-duration capture remained running")
	}
	if api.sealRequest == nil || api.sealRequest.Completeness != "partial" ||
		api.sealRequest.TimelineEndMS != terminalEnd {
		t.Fatalf("maximum-duration capture did not seal partial at terminal fence: %+v", api.sealRequest)
	}
}

type captureRunnerSignaling struct {
	snapshot  Snapshot
	consumers map[string]readsignal.Consumer
	lifecycle chan Event
	closeOnce sync.Once
	onClose   func()
}

func (fixture *captureRunnerSignaling) RouterRTPCapabilities(context.Context) (json.RawMessage, error) {
	return json.RawMessage(`{"codecs":[]}`), nil
}

func (fixture *captureRunnerSignaling) CreateRecvTransport(context.Context) (readsignal.TransportOptions, error) {
	return readsignal.TransportOptions{ID: "transport-1"}, nil
}

func (fixture *captureRunnerSignaling) SyncSnapshot(context.Context) (Snapshot, error) {
	return fixture.snapshot, nil
}

func (fixture *captureRunnerSignaling) Consume(
	_ context.Context,
	_ string,
	producerID string,
	_ json.RawMessage,
) (readsignal.Consumer, error) {
	return fixture.consumers[producerID], nil
}

func (*captureRunnerSignaling) ConnectRecvTransport(
	context.Context,
	string,
	readsignal.DTLSParameters,
) error {
	return nil
}

func (*captureRunnerSignaling) ResumeConsumer(context.Context, string) error { return nil }
func (*captureRunnerSignaling) PauseConsumer(context.Context, string) error  { return nil }
func (*captureRunnerSignaling) CloseConsumer(context.Context, string) error  { return nil }

func (fixture *captureRunnerSignaling) Lifecycle() <-chan Event { return fixture.lifecycle }

func (fixture *captureRunnerSignaling) Close() error {
	fixture.closeOnce.Do(func() {
		if fixture.onClose != nil {
			fixture.onClose()
		}
		close(fixture.lifecycle)
	})
	return nil
}

type captureRunnerPeer struct {
	tracks chan TrackEvent
	errors chan error
}

func (*captureRunnerPeer) ReconcileConsumers(
	context.Context,
	readsignal.TransportOptions,
	[]readsignal.Consumer,
) (string, readsignal.DTLSParameters, error) {
	return "answer", readsignal.DTLSParameters{}, nil
}

func (*captureRunnerPeer) RequestKeyFrame(context.Context, uint32) error { return nil }
func (*captureRunnerPeer) Close() error                                  { return nil }
func (peer *captureRunnerPeer) Tracks() <-chan TrackEvent                { return peer.tracks }
func (peer *captureRunnerPeer) Errors() <-chan error                     { return peer.errors }

type notifyingPacketWriter struct {
	packetCaptureWriter
	written chan<- struct{}
}

func (writer notifyingPacketWriter) WriteRTP(packet *rtp.Packet) error {
	if err := writer.packetCaptureWriter.WriteRTP(packet); err != nil {
		return err
	}
	select {
	case writer.written <- struct{}{}:
	default:
	}
	return nil
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
		runner: runner, fence: fence,
		spoolFence: newSpoolMutationFence(
			AttemptFence{"attempt-1", fence.RecordingID, fence.WorkerID, fence.WorkerGenerationID},
			fence.CaptureGeneration,
		),
		timeline: timeline, startedAt: time.Now(),
		checkpointSequence: sequence, writers: map[string]*RTPTrackWriter{},
		trackInputs: map[string]TrackEvent{}, sequences: map[string]int32{},
	}
}
