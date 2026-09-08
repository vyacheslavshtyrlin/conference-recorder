package recording

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"conference/recording-bot/internal/signaling"
)

type CaptureControlPlane interface {
	Allocate(context.Context, CaptureFence, []SegmentDescriptor) ([]UploadTarget, error)
	PutFile(context.Context, UploadTarget, string) error
	Commit(context.Context, CaptureFence, string) error
	PutCheckpoint(context.Context, CaptureFence, int32, int64, []CheckpointRecord) (CheckpointResult, error)
	Recover(context.Context, CaptureFence) (RecoveryState, error)
	Seal(context.Context, CaptureFence, SealRequest) (SealResult, error)
}

type RecoveryDiscoveryControlPlane interface {
	RecoverCurrent(context.Context, string, string, string) (RecoveryState, error)
}

type CapturePeer interface {
	signaling.PeerSession
	Tracks() <-chan signaling.TrackEvent
	Errors() <-chan error
}

type CaptureRunnerDependencies struct {
	API                      CaptureControlPlane
	Spool                    *DurableSpool
	Muxer                    SegmentMuxer
	Dial                     func(context.Context, string, string, signaling.SessionIdentity) (signaling.SignalingClient, error)
	NewPeer                  func() (CapturePeer, error)
	StorageRoot              string
	SegmentDuration          time.Duration
	MaximumSegmentDuration   time.Duration
	FinalizeTimeout          time.Duration
	MaximumStorageBytes      int64
	MaximumCaptureDuration   time.Duration
	SegmentOperationTimeout  time.Duration
	MaximumConcurrentUploads int
	Now                      func() time.Time
}

type CaptureAttemptRunner struct {
	dependencies CaptureRunnerDependencies
	uploadSlots  chan struct{}
}

func (runner *CaptureAttemptRunner) RecoveryHealth() ([]string, []string, error) {
	return runner.dependencies.Spool.RecoveryHealth()
}

func NewCaptureAttemptRunner(dependencies CaptureRunnerDependencies) (*CaptureAttemptRunner, error) {
	if dependencies.MaximumStorageBytes == 0 && dependencies.Spool != nil {
		dependencies.MaximumStorageBytes = dependencies.Spool.maxBytes
	}
	if dependencies.MaximumCaptureDuration == 0 {
		dependencies.MaximumCaptureDuration = 24 * time.Hour
	}
	if dependencies.SegmentOperationTimeout == 0 {
		dependencies.SegmentOperationTimeout = dependencies.FinalizeTimeout
	}
	if dependencies.MaximumConcurrentUploads == 0 {
		dependencies.MaximumConcurrentUploads = 4
	}
	missingDependency := dependencies.API == nil || dependencies.Spool == nil ||
		dependencies.Muxer == nil || dependencies.Dial == nil || dependencies.NewPeer == nil
	invalidSegmentDuration := dependencies.SegmentDuration <= 0 ||
		dependencies.MaximumSegmentDuration < dependencies.SegmentDuration ||
		dependencies.MaximumSegmentDuration > dependencies.MaximumCaptureDuration
	invalidMaximumDuration := dependencies.MaximumCaptureDuration < time.Second ||
		dependencies.MaximumCaptureDuration > 24*time.Hour
	if missingDependency || dependencies.StorageRoot == "" || invalidSegmentDuration ||
		dependencies.FinalizeTimeout <= 0 || dependencies.SegmentOperationTimeout <= 0 ||
		dependencies.MaximumConcurrentUploads < 1 || dependencies.MaximumConcurrentUploads > 32 ||
		dependencies.MaximumStorageBytes < 1 || invalidMaximumDuration {
		return nil, errors.New("invalid capture attempt runner dependencies")
	}
	if dependencies.Now == nil {
		dependencies.Now = time.Now
	}
	return &CaptureAttemptRunner{
		dependencies: dependencies,
		uploadSlots:  make(chan struct{}, dependencies.MaximumConcurrentUploads),
	}, nil
}

func (runner *CaptureAttemptRunner) Run(
	ctx context.Context,
	request AttemptRequest,
	ready func(),
	report func(ProbeEvidence),
) error {
	invalidAssignment := request.ProtocolVersion != CaptureProtocolVersion ||
		request.CaptureGeneration <= 0 || request.CaptureStartedAt == ""
	if invalidAssignment {
		return errors.New("production capture requires recording-capture.v2 assignment")
	}
	startedAt, err := time.Parse(time.RFC3339Nano, request.CaptureStartedAt)
	if err != nil {
		return errors.New("invalid capture timeline origin")
	}
	deadline := startedAt.Add(runner.dependencies.MaximumCaptureDuration)
	if !deadline.After(runner.dependencies.Now()) {
		return ErrCaptureMaximumDuration
	}
	runContext, cancelRun := context.WithDeadline(ctx, deadline)
	defer cancelRun()
	ctx = runContext
	fence := CaptureFence{
		RecordingID: request.RecordingID, WorkerID: request.WorkerID,
		WorkerGenerationID: request.WorkerGenerationID, CaptureGeneration: request.CaptureGeneration,
	}
	recovery, err := runner.dependencies.API.Recover(ctx, fence)
	if err != nil {
		return err
	}
	bindingMismatch := recovery.CaptureGeneration != request.CaptureGeneration ||
		recovery.CaptureStartedAt != request.CaptureStartedAt || recovery.BindingMode != "capture"
	if bindingMismatch {
		return errors.New("recovery binding does not match assignment")
	}
	if err := validateRecoveryDuration(recovery, runner.dependencies.MaximumCaptureDuration); err != nil {
		return err
	}
	attemptFence := AttemptFence{
		AttemptID: request.AttemptID, RecordingID: request.RecordingID,
		WorkerID: request.WorkerID, WorkerGenerationID: request.WorkerGenerationID,
	}
	manifest, err := runner.dependencies.Spool.Begin(
		ctx,
		attemptFence,
		request.CaptureGeneration,
		request.CaptureStartedAt,
		"capture",
	)
	if err != nil {
		return err
	}
	if err := runner.dependencies.Spool.CleanupStaging(request.RecordingID); err != nil {
		return err
	}
	defer runner.dependencies.Spool.CleanupStaging(request.RecordingID)
	timeline, err := NewTimelineState(request.RecordingID)
	if err != nil {
		return err
	}
	if err := timeline.Restore(recovery); err != nil {
		return err
	}
	checkpoint, recoveredCheckpoint := latestGenerationCheckpoint(
		recovery.GenerationCheckpoints,
		fence.CaptureGeneration,
	)
	if !recoveredCheckpoint && manifest.LastCheckpointSequence != -1 {
		return errors.New("local checkpoint is ahead of authoritative recovery")
	}
	if recoveredCheckpoint {
		records, recordsErr := timeline.RecordsThrough(checkpoint.TimelineThroughMS, false)
		if recordsErr != nil {
			return recordsErr
		}
		if err := runner.dependencies.Spool.ReconcileCheckpoint(
			request.RecordingID,
			checkpoint,
			records,
		); err != nil {
			return err
		}
		manifest, err = runner.dependencies.Spool.Snapshot(request.RecordingID)
		if err != nil {
			return err
		}
	}
	session := &captureSession{
		runner: runner, fence: fence, timeline: timeline, startedAt: startedAt,
		checkpointSequence: nextCheckpointSequence(recovery.GenerationCheckpoints, fence.CaptureGeneration),
		writers:            map[string]*RTPTrackWriter{}, trackInputs: map[string]signaling.TrackEvent{},
		sequences:    recoveredSequences(recovery, manifest, fence.CaptureGeneration),
		writerErrors: make(chan trackWriterFailure, 32),
	}
	if checkpoint, exists := latestCheckpoint(recovery.GenerationCheckpoints); exists {
		session.checkpointBoundary = checkpoint.TimelineThroughMS
		if err := timeline.DeclareRecoveryGaps(checkpoint.TimelineThroughMS, session.timelineMS()); err != nil {
			return err
		}
	}
	signalingClient, err := runner.dependencies.Dial(
		ctx,
		request.SignalingURL,
		request.Admission,
		signaling.SessionIdentity{RoomID: request.RoomID, RecordingID: request.RecordingID},
	)
	request.Admission = ""
	if err != nil {
		return err
	}
	peer, err := runner.dependencies.NewPeer()
	if err != nil {
		_ = signalingClient.Close()
		return err
	}
	controller, err := signaling.StartProbeWithObserver(ctx, signalingClient, peer, session)
	if err != nil {
		return err
	}
	defer controller.Close()
	lifecycle := make(chan error, 1)
	go func() { lifecycle <- controller.Run(ctx) }()
	resumeContext, cancelResume := context.WithCancel(ctx)
	defer cancelResume()
	resume := make(chan error, 1)
	go func() { resume <- session.resumePending(resumeContext, manifest) }()
	for {
		select {
		case resumeErr := <-resume:
			resume = nil
			if resumeErr != nil {
				if ctx.Err() != nil {
					return session.cleanStop()
				}
				cancelRun()
				return session.failAndSeal(resumeErr)
			}
			controller.WhileActive(ctx, ready)
		case event, open := <-peer.Tracks():
			if !open {
				cancelRun()
				resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
				return session.failAndSeal(errors.Join(errors.New("Pion track channel closed"), resumeErr))
			}
			if err := session.startTrack(ctx, event); err != nil {
				cancelRun()
				resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
				return session.failAndSeal(errors.Join(err, resumeErr))
			}
			report(ProbeEvidence{ObservedSources: []signaling.Source{event.Source}, Codecs: []string{event.Codec}})
		case peerErr, open := <-peer.Errors():
			if !open || peerErr == nil {
				peerErr = errors.New("Pion error channel closed")
			}
			cancelRun()
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			return session.failAndSeal(errors.Join(peerErr, resumeErr))
		case failure := <-session.writerErrors:
			if session.acceptWriterFailure(failure) {
				cancelRun()
				resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
				return session.failAndSeal(errors.Join(failure.err, resumeErr))
			}
		case lifecycleErr := <-lifecycle:
			if errors.Is(lifecycleErr, signaling.ErrAuthoritativeStop) {
				stopBoundary := session.nextTimelinePoint()
				writers := session.stopWritersAt(stopBoundary)
				controllerErr := controller.Close()
				finalContext, cancelFinal := context.WithTimeout(
					context.Background(),
					runner.dependencies.FinalizeTimeout,
				)
				writersErr := session.waitWriters(finalContext, writers)
				if writersErr != nil {
					cancelFinal()
					cancelRun()
					resumeErr := stopPendingResume(
						cancelResume,
						resume,
						runner.dependencies.FinalizeTimeout,
					)
					cause := errors.Join(ErrFinalizationFailed, controllerErr, writersErr, resumeErr)
					if errors.Is(writersErr, errTrackWriterStillRunning) {
						return errors.Join(ErrCaptureIncomplete, cause)
					}
					return session.failAndSeal(cause)
				}
				checkpointErr := session.checkpoint(finalContext, stopBoundary, false)
				cancelFinal()
				if controllerErr != nil || checkpointErr != nil {
					cancelRun()
					resumeErr := stopPendingResume(
						cancelResume,
						resume,
						runner.dependencies.FinalizeTimeout,
					)
					return session.failAndSeal(errors.Join(
						ErrFinalizationFailed,
						controllerErr,
						checkpointErr,
						resumeErr,
					))
				}
				completed, resumeErr := awaitPendingResume(resume, runner.dependencies.FinalizeTimeout)
				if resumeErr != nil {
					cancelRun()
					if !completed {
						resumeErr = errors.Join(
							resumeErr,
							stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout),
						)
					}
					return session.failAndSeal(errors.Join(ErrFinalizationFailed, resumeErr))
				}
				resume = nil
				if err := session.finishCleanStop(); err != nil {
					return fmt.Errorf("%w: %v", ErrFinalizationFailed, err)
				}
				return signaling.ErrAuthoritativeStop
			}
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			if ctx.Err() == nil {
				return session.failAndSeal(errors.Join(lifecycleErr, resumeErr))
			}
			return errors.Join(session.cleanStop(), resumeErr)
		case <-ctx.Done():
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			return errors.Join(session.cleanStop(), resumeErr)
		}
	}
}

func awaitPendingResume(result <-chan error, timeout time.Duration) (bool, error) {
	if result == nil {
		return true, nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return true, err
	case <-timer.C:
		return false, errors.New("pending segment recovery did not finish")
	}
}

func stopPendingResume(
	cancel context.CancelFunc,
	result <-chan error,
	timeout time.Duration,
) error {
	cancel()
	if result == nil {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-result:
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	case <-timer.C:
		return errors.New("pending segment recovery did not stop")
	}
}

func (runner *CaptureAttemptRunner) Finalize(ctx context.Context, request FinalizationRequest) error {
	discovery, ok := runner.dependencies.API.(RecoveryDiscoveryControlPlane)
	if !ok {
		return errors.New("capture API does not support finalization recovery")
	}
	recovery, err := discovery.RecoverCurrent(ctx, request.RecordingID, request.WorkerID, request.WorkerGenerationID)
	if err != nil {
		return err
	}
	if recovery.BindingMode != "seal-only" || recovery.TerminalTimelineEnd == nil {
		return errors.New("finalization binding is not seal-only")
	}
	if err := validateRecoveryDuration(recovery, runner.dependencies.MaximumCaptureDuration); err != nil {
		return err
	}
	if _, err := time.Parse(time.RFC3339Nano, recovery.CaptureStartedAt); err != nil {
		return errors.New("invalid finalization timeline origin")
	}
	fence := CaptureFence{
		RecordingID: request.RecordingID, WorkerID: request.WorkerID,
		WorkerGenerationID: request.WorkerGenerationID, CaptureGeneration: recovery.CaptureGeneration,
	}
	attemptFence := AttemptFence{
		AttemptID: request.AttemptID, RecordingID: request.RecordingID,
		WorkerID: request.WorkerID, WorkerGenerationID: request.WorkerGenerationID,
	}
	if _, err := runner.dependencies.Spool.Begin(
		ctx,
		attemptFence,
		recovery.CaptureGeneration,
		recovery.CaptureStartedAt,
		"seal-only",
	); err != nil {
		return err
	}
	timeline, err := NewTimelineState(request.RecordingID)
	if err != nil {
		return err
	}
	if err := timeline.Restore(recovery); err != nil {
		return err
	}
	checkpoint, exists := latestCheckpoint(recovery.GenerationCheckpoints)
	if !exists {
		return errors.New("seal-only recovery has no checkpoint")
	}
	_, err = runner.dependencies.API.Seal(ctx, fence, SealRequest{
		Completeness: "partial", TimelineEndMS: *recovery.TerminalTimelineEnd,
		LastCheckpointCaptureGeneration: checkpoint.CaptureGeneration,
		LastCheckpointSequence:          checkpoint.CheckpointSequence,
		CheckpointPayloadSHA256:         checkpoint.PayloadSHA256,
	})
	if err != nil {
		return err
	}
	return runner.dependencies.Spool.MarkSealed(request.RecordingID)
}

type captureSession struct {
	runner             *CaptureAttemptRunner
	fence              CaptureFence
	timeline           *TimelineState
	startedAt          time.Time
	mu                 sync.Mutex
	observerMu         sync.Mutex
	writerMu           sync.Mutex
	checkpointMu       sync.Mutex
	checkpointSequence int32
	checkpointBoundary int64
	writers            map[string]*RTPTrackWriter
	trackInputs        map[string]signaling.TrackEvent
	sequences          map[string]int32
	writerErrors       chan trackWriterFailure
	stopping           bool
}

type trackWriterFailure struct {
	trackID string
	writer  *RTPTrackWriter
	err     error
}

func (session *captureSession) Snapshot(ctx context.Context, snapshot signaling.Snapshot) error {
	session.observerMu.Lock()
	defer session.observerMu.Unlock()
	if session.isStopping() {
		return errCaptureSessionStopping
	}
	now := session.nextTimelinePoint()
	if err := session.closeTrackBarriers(ctx, session.timeline.SnapshotWriterBarriers(snapshot), now); err != nil {
		return err
	}
	if err := session.timeline.ApplySnapshot(snapshot, now); err != nil {
		return err
	}
	if err := session.checkpoint(ctx, now, false); err != nil {
		return err
	}
	return session.reconcileSnapshotInputs(ctx, snapshot)
}

func (session *captureSession) Event(ctx context.Context, event signaling.Event) error {
	session.observerMu.Lock()
	defer session.observerMu.Unlock()
	if session.isStopping() {
		return errCaptureSessionStopping
	}
	now := session.nextTimelinePoint()
	if err := session.closeTrackBarriers(ctx, session.timeline.EventWriterBarriers(event), now); err != nil {
		return err
	}
	session.reconcileEventInputs(event)
	if err := session.timeline.ApplyEvent(event, now); err != nil {
		return err
	}
	if err := session.checkpoint(ctx, now, false); err != nil {
		return err
	}
	if event.Type == "bot:producerResumed" ||
		(event.Type == "bot:producerUpdated" && event.Producer != nil && !event.Producer.Paused) {
		return session.restartTrack(ctx, eventProducerID(event))
	}
	return nil
}

func (session *captureSession) startTrack(ctx context.Context, event signaling.TrackEvent) error {
	if event.SourceTrack == nil {
		return errors.New("remote track source is absent")
	}
	session.mu.Lock()
	if session.trackInputs == nil {
		session.trackInputs = map[string]signaling.TrackEvent{}
	}
	session.trackInputs[event.ProducerID] = event
	session.mu.Unlock()
	return session.startTrackInput(ctx, event)
}

func (session *captureSession) startTrackInput(ctx context.Context, event signaling.TrackEvent) error {
	session.writerMu.Lock()
	defer session.writerMu.Unlock()
	if session.stopping {
		return errCaptureSessionStopping
	}
	trackID, participantID, exists := session.timeline.TrackForProducer(event.ProducerID)
	if !exists || event.SourceTrack == nil {
		return errors.New("remote track has no authoritative timeline identity")
	}
	session.mu.Lock()
	previous := session.writers[trackID]
	if session.writerErrors == nil {
		session.writerErrors = make(chan trackWriterFailure, 32)
	}
	session.mu.Unlock()
	if previous != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), session.runner.dependencies.FinalizeTimeout)
		err := previous.CloseAt(closeContext, session.nextTimelinePoint())
		cancel()
		if err != nil {
			return err
		}
	}
	session.mu.Lock()
	initialSequence := session.sequences[trackID]
	session.mu.Unlock()
	if err := event.SourceTrack.SetReadDeadline(time.Time{}); err != nil {
		return errors.New("reset remote track read deadline")
	}
	writer, err := StartRTPTrackWriter(ctx, RTPTrackWriterConfig{
		Track: event.SourceTrack, ParticipantInstanceID: participantID,
		TrackID: trackID, Source: event.Source, Codec: event.Codec,
		SpoolDirectory: filepath.Join(
			session.runner.dependencies.StorageRoot,
			"capture-v2",
			session.fence.RecordingID,
			"staging",
		),
		SegmentDuration:        session.runner.dependencies.SegmentDuration,
		MaximumSegmentDuration: session.runner.dependencies.MaximumSegmentDuration,
		TimelineClock:          session.timelineMS, Muxer: session.runner.dependencies.Muxer,
		InitialSequence:     initialSequence,
		OperationTimeout:    session.runner.dependencies.SegmentOperationTimeout,
		MaximumStorageBytes: session.runner.dependencies.MaximumStorageBytes,
		RecordingID:         session.fence.RecordingID,
		StagingBudget:       session.runner.dependencies.Spool,
		OnSegment:           session.segmentReady,
	})
	if err != nil {
		return err
	}
	session.mu.Lock()
	session.writers[trackID] = writer
	session.mu.Unlock()
	go session.observeWriter(ctx, trackID, writer)
	return nil
}

func (session *captureSession) observeWriter(ctx context.Context, trackID string, writer *RTPTrackWriter) {
	select {
	case err, open := <-writer.Errors():
		if !open || err == nil {
			return
		}
		select {
		case session.writerErrors <- trackWriterFailure{trackID: trackID, writer: writer, err: err}:
		case <-ctx.Done():
		}
	case <-ctx.Done():
	}
}

func (session *captureSession) acceptWriterFailure(failure trackWriterFailure) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.writers[failure.trackID] != failure.writer {
		return false
	}
	delete(session.writers, failure.trackID)
	return true
}

func (session *captureSession) closeTrackBarriers(ctx context.Context, trackIDs []string, atMS int64) error {
	session.writerMu.Lock()
	writers := make([]*RTPTrackWriter, 0, len(trackIDs))
	for _, trackID := range trackIDs {
		session.mu.Lock()
		writer := session.writers[trackID]
		delete(session.writers, trackID)
		session.mu.Unlock()
		if writer == nil {
			continue
		}
		writer.stop(&atMS)
		writers = append(writers, writer)
	}
	session.writerMu.Unlock()
	return session.waitWriters(ctx, writers)
}

func (session *captureSession) restartTrack(ctx context.Context, producerID string) error {
	session.mu.Lock()
	event, exists := session.trackInputs[producerID]
	session.mu.Unlock()
	if !exists {
		return nil
	}
	return session.startTrackInput(ctx, event)
}

func (session *captureSession) reconcileSnapshotInputs(ctx context.Context, snapshot signaling.Snapshot) error {
	desired := make(map[string]signaling.Producer, len(snapshot.Producers))
	for _, producer := range snapshot.Producers {
		desired[producer.ProducerID] = producer
	}
	session.mu.Lock()
	for producerID := range session.trackInputs {
		if _, exists := desired[producerID]; !exists {
			delete(session.trackInputs, producerID)
		}
	}
	inputs := make([]signaling.TrackEvent, 0, len(session.trackInputs))
	for producerID, input := range session.trackInputs {
		if !desired[producerID].Paused {
			inputs = append(inputs, input)
		}
	}
	session.mu.Unlock()
	for _, input := range inputs {
		trackID, _, exists := session.timeline.TrackForProducer(input.ProducerID)
		if !exists {
			continue
		}
		session.mu.Lock()
		writerExists := session.writers[trackID] != nil
		session.mu.Unlock()
		if !writerExists {
			if err := session.startTrackInput(ctx, input); err != nil {
				return err
			}
		}
	}
	return nil
}

func (session *captureSession) reconcileEventInputs(event signaling.Event) {
	switch event.Type {
	case "bot:producerClosed":
		session.mu.Lock()
		delete(session.trackInputs, event.ProducerID)
		session.mu.Unlock()
	case "bot:participantLeft":
		session.mu.Lock()
		producerIDs := make([]string, 0, len(session.trackInputs))
		for producerID := range session.trackInputs {
			producerIDs = append(producerIDs, producerID)
		}
		session.mu.Unlock()
		participantID := ParticipantInstanceID(session.fence.RecordingID, event.ParticipantID)
		for _, producerID := range producerIDs {
			_, owner, exists := session.timeline.TrackForProducer(producerID)
			if exists && owner == participantID {
				session.mu.Lock()
				delete(session.trackInputs, producerID)
				session.mu.Unlock()
			}
		}
	case "bot:producerUpdated":
		if session.timeline.ProducerUpdateChangesTrack(event) {
			session.mu.Lock()
			delete(session.trackInputs, eventProducerID(event))
			session.mu.Unlock()
		}
	}
}

func eventProducerID(event signaling.Event) string {
	if event.Producer != nil {
		return event.Producer.ProducerID
	}
	return event.ProducerID
}

func (session *captureSession) segmentReady(
	ctx context.Context,
	descriptor SegmentDescriptor,
	stagingPath string,
) error {
	stored, err := session.runner.dependencies.Spool.StoreSegment(
		ctx,
		session.fence.RecordingID,
		descriptor,
		stagingPath,
	)
	if stored.TrackID != "" {
		session.reserveSequence(stored.TrackID, stored.Sequence)
	}
	if err != nil {
		return err
	}
	if err := session.checkpoint(ctx, descriptor.EndMS, false); err != nil {
		return err
	}
	if err := session.upload(ctx, stored); err != nil {
		return err
	}
	return session.runner.dependencies.Spool.RemoveCommitted(session.fence.RecordingID)
}

func (session *captureSession) reserveSequence(trackID string, sequence int32) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if next := sequence + 1; next > session.sequences[trackID] {
		session.sequences[trackID] = next
	}
}

func (session *captureSession) upload(ctx context.Context, segment DurableSegment) error {
	select {
	case session.runner.uploadSlots <- struct{}{}:
		defer func() { <-session.runner.uploadSlots }()
	case <-ctx.Done():
		return ctx.Err()
	}
	targets, err := session.runner.dependencies.API.Allocate(
		ctx,
		session.fence,
		[]SegmentDescriptor{segment.SegmentDescriptor},
	)
	if err != nil {
		return err
	}
	localPath := filepath.Join(
		session.runner.dependencies.StorageRoot,
		"capture-v2",
		session.fence.RecordingID,
		segment.LocalPath,
	)
	if err := session.runner.dependencies.API.PutFile(ctx, targets[0], localPath); err != nil {
		return err
	}
	if err := session.runner.dependencies.API.Commit(ctx, session.fence, targets[0].UploadID); err != nil {
		return err
	}
	return session.runner.dependencies.Spool.MarkCommitted(
		session.fence.RecordingID,
		segment.CaptureGeneration,
		segment.TrackID,
		segment.Sequence,
		targets[0].UploadID,
	)
}

func (session *captureSession) resumePending(ctx context.Context, manifest *SpoolManifest) error {
	for _, segment := range manifest.Segments {
		if segment.Committed {
			continue
		}
		if err := session.checkpoint(ctx, segment.EndMS, false); err != nil {
			return err
		}
		if err := session.upload(ctx, segment); err != nil {
			return err
		}
	}
	return session.runner.dependencies.Spool.RemoveCommitted(session.fence.RecordingID)
}

func (session *captureSession) checkpoint(ctx context.Context, throughMS int64, closeOpen bool) error {
	session.checkpointMu.Lock()
	defer session.checkpointMu.Unlock()

	session.mu.Lock()
	if throughMS > session.maximumTimelineMS() {
		session.mu.Unlock()
		return ErrCaptureMaximumDuration
	}
	checkpointSequence := session.checkpointSequence
	checkpointBoundary := session.checkpointBoundary
	session.mu.Unlock()
	boundary, err := session.timeline.CheckpointBoundary(throughMS, closeOpen)
	if errors.Is(err, ErrCheckpointBoundaryUnavailable) && !closeOpen {
		return nil
	}
	if err != nil {
		return err
	}
	if !closeOpen && boundary <= checkpointBoundary {
		return nil
	}
	records, err := session.timeline.RecordsThrough(boundary, closeOpen)
	if err != nil {
		return err
	}
	result, err := session.runner.dependencies.API.PutCheckpoint(
		ctx,
		session.fence,
		checkpointSequence,
		boundary,
		records,
	)
	if err != nil {
		return err
	}
	checkpoint := GenerationCheckpoint{
		CaptureGeneration:  session.fence.CaptureGeneration,
		CheckpointSequence: result.CheckpointSequence,
		TimelineThroughMS:  boundary,
		PayloadSHA256:      result.PayloadSHA256,
	}
	if err := session.runner.dependencies.Spool.StoreCheckpoint(
		session.fence.RecordingID,
		checkpoint,
		records,
	); err != nil {
		return err
	}
	session.mu.Lock()
	session.checkpointSequence = checkpointSequence + 1
	session.checkpointBoundary = boundary
	session.mu.Unlock()
	return nil
}

func (session *captureSession) cleanStop() error {
	if err := session.prepareCleanStop(); err != nil {
		return err
	}
	return session.finishCleanStop()
}

func (session *captureSession) prepareCleanStop() error {
	finalContext, cancel := context.WithTimeout(context.Background(), session.runner.dependencies.FinalizeTimeout)
	defer cancel()
	stopBoundary := session.nextTimelinePoint()
	if err := session.closeWritersAt(finalContext, stopBoundary); err != nil {
		return err
	}
	return session.checkpoint(finalContext, stopBoundary, false)
}

func (session *captureSession) finishCleanStop() error {
	finalContext, cancel := context.WithTimeout(context.Background(), session.runner.dependencies.FinalizeTimeout)
	defer cancel()
	recovery, err := session.waitForTerminalRecovery(finalContext)
	if err != nil {
		return err
	}
	if recovery.TerminalTimelineEnd == nil {
		return errors.New("clean stop has no authoritative terminal timeline")
	}
	end := *recovery.TerminalTimelineEnd
	if err := session.checkpoint(finalContext, end, true); err != nil {
		return err
	}
	manifest, err := session.runner.dependencies.Spool.Snapshot(session.fence.RecordingID)
	if err != nil {
		return err
	}
	completeness := "complete"
	if session.timeline.HasGaps() {
		completeness = "partial"
	}
	_, err = session.runner.dependencies.API.Seal(finalContext, session.fence, SealRequest{
		Completeness: completeness, TimelineEndMS: end,
		LastCheckpointCaptureGeneration: session.fence.CaptureGeneration,
		LastCheckpointSequence:          manifest.LastCheckpointSequence,
		CheckpointPayloadSHA256:         manifest.LastCheckpointHash,
	})
	if err != nil {
		return err
	}
	return session.runner.dependencies.Spool.MarkSealed(session.fence.RecordingID)
}

func (session *captureSession) waitForTerminalRecovery(ctx context.Context) (RecoveryState, error) {
	for attempt := 0; ; attempt++ {
		recovery, err := session.runner.dependencies.API.Recover(ctx, session.fence)
		if err != nil {
			return RecoveryState{}, err
		}
		if recovery.TerminalTimelineEnd != nil {
			if err := validateRecoveryDuration(
				recovery,
				session.runner.dependencies.MaximumCaptureDuration,
			); err != nil {
				return RecoveryState{}, err
			}
			return recovery, nil
		}
		delay := 50 * time.Millisecond
		if attempt > 0 {
			delay <<= min(attempt, 3)
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return RecoveryState{}, errors.New("terminal capture fence polling timed out")
		}
	}
}

func (session *captureSession) failAndSeal(cause error) error {
	finalContext, cancel := context.WithTimeout(context.Background(), session.runner.dependencies.FinalizeTimeout)
	defer cancel()
	closeErr := session.closeWriters(finalContext)
	if errors.Is(closeErr, errTrackWriterStillRunning) {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr)
	}
	recovery, recoveryErr := session.runner.dependencies.API.Recover(finalContext, session.fence)
	if recoveryErr != nil || recovery.TerminalTimelineEnd == nil {
		if recoveryErr == nil {
			recoveryErr = errors.New("partial seal has no authoritative terminal timeline")
		}
		return errors.Join(
			ErrCaptureIncomplete,
			ErrFinalizationFailed,
			cause,
			closeErr,
			recoveryErr,
		)
	}
	if err := validateRecoveryDuration(
		recovery,
		session.runner.dependencies.MaximumCaptureDuration,
	); err != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
	}
	end := *recovery.TerminalTimelineEnd
	checkpoint, exists := latestCheckpoint(recovery.GenerationCheckpoints)
	if !exists {
		return errors.Join(
			ErrCaptureIncomplete,
			ErrFinalizationFailed,
			cause,
			closeErr,
			errors.New("partial seal has no durable checkpoint"),
		)
	}
	_, sealErr := session.runner.dependencies.API.Seal(finalContext, session.fence, SealRequest{
		Completeness: "partial", TimelineEndMS: end,
		LastCheckpointCaptureGeneration: checkpoint.CaptureGeneration,
		LastCheckpointSequence:          checkpoint.CheckpointSequence,
		CheckpointPayloadSHA256:         checkpoint.PayloadSHA256,
	})
	if sealErr == nil {
		sealErr = session.runner.dependencies.Spool.MarkSealed(session.fence.RecordingID)
	}
	result := errors.Join(ErrCaptureIncomplete, cause, closeErr)
	if sealErr != nil {
		result = errors.Join(result, ErrFinalizationFailed, sealErr)
	}
	return result
}

func (session *captureSession) closeWriters(ctx context.Context) error {
	return session.closeWritersAt(ctx, session.nextTimelinePoint())
}

func (session *captureSession) closeWritersAt(ctx context.Context, endMS int64) error {
	writers := session.stopWritersAt(endMS)
	return session.waitWriters(ctx, writers)
}

func (session *captureSession) stopWritersAt(endMS int64) []*RTPTrackWriter {
	session.observerMu.Lock()
	defer session.observerMu.Unlock()
	session.writerMu.Lock()
	session.stopping = true
	session.mu.Lock()
	writers := make([]*RTPTrackWriter, 0, len(session.writers))
	for _, writer := range session.writers {
		writers = append(writers, writer)
	}
	session.writers = map[string]*RTPTrackWriter{}
	session.mu.Unlock()
	for _, writer := range writers {
		writer.stop(&endMS)
	}
	session.writerMu.Unlock()
	return writers
}

var errCaptureSessionStopping = errors.New("capture session is stopping")

func (session *captureSession) isStopping() bool {
	session.writerMu.Lock()
	defer session.writerMu.Unlock()
	return session.stopping
}

func (session *captureSession) waitWriters(ctx context.Context, writers []*RTPTrackWriter) error {
	results := make(chan error, len(writers))
	for _, writer := range writers {
		go func() { results <- writer.wait(ctx) }()
	}
	errs := make([]error, 0, len(writers))
	for range writers {
		if err := <-results; err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (session *captureSession) timelineMS() int64 {
	value := session.runner.dependencies.Now().Sub(session.startedAt).Milliseconds()
	if value < 0 {
		return 0
	}
	maximum := session.maximumTimelineMS()
	if value > maximum {
		return maximum
	}
	return value
}

func (session *captureSession) maximumTimelineMS() int64 {
	return session.runner.dependencies.MaximumCaptureDuration.Milliseconds()
}

func (session *captureSession) nextTimelinePoint() int64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	value := session.timelineMS()
	if value < session.checkpointBoundary {
		return session.checkpointBoundary
	}
	return value
}

func recoveredSequences(recovery RecoveryState, manifest *SpoolManifest, generation int32) map[string]int32 {
	result := map[string]int32{}
	for _, segment := range recovery.Segments {
		if segment.CaptureGeneration == generation && segment.Sequence >= result[segment.TrackID] {
			result[segment.TrackID] = segment.Sequence + 1
		}
	}
	for _, segment := range manifest.Segments {
		if segment.CaptureGeneration == generation && segment.Sequence >= result[segment.TrackID] {
			result[segment.TrackID] = segment.Sequence + 1
		}
	}
	return result
}

func NewProductionCaptureRunner(
	config Config,
	supervisor *BoundedProcessSupervisor,
) (*CaptureAttemptRunner, error) {
	api, err := NewPrivateUploadClient(config.RecordingAPIURL, config.CaptureServiceSecret, config.MaxInFlightPUT)
	if err != nil {
		return nil, err
	}
	spool, err := NewDurableSpool(config.StorageRoot, config.MaxSourceBytes)
	if err != nil {
		return nil, err
	}
	muxConcurrency := min(4, max(2, config.MaxSlots))
	muxer, err := NewSupervisedFFmpegMuxerWithConcurrency(
		config.FFmpegPath,
		config.FFmpegTimeout,
		nil,
		supervisor,
		muxConcurrency,
	)
	if err != nil {
		return nil, err
	}
	return NewCaptureAttemptRunner(CaptureRunnerDependencies{
		API:   api,
		Spool: spool,
		Muxer: muxer,
		Dial: func(
			ctx context.Context,
			target string,
			admission string,
			identity signaling.SessionIdentity,
		) (signaling.SignalingClient, error) {
			return signaling.DialWebSocketSignalingForAttempt(ctx, target, admission, identity)
		},
		NewPeer: func() (CapturePeer, error) {
			return signaling.NewPionSession()
		},
		StorageRoot:              config.StorageRoot,
		SegmentDuration:          5 * time.Second,
		MaximumSegmentDuration:   30 * time.Second,
		FinalizeTimeout:          config.RecoveryTimeout,
		MaximumStorageBytes:      config.MaxSourceBytes,
		SegmentOperationTimeout:  max(config.RecoveryTimeout, config.MaxInFlightPUT+30*time.Second),
		MaximumConcurrentUploads: min(8, max(2, config.MaxSlots)),
		MaximumCaptureDuration:   config.MaxDuration,
		Now:                      time.Now,
	})
}

var ErrCaptureMaximumDuration = errors.New("capture maximum duration reached")

var (
	ErrCaptureIncomplete  = errors.New("capture incomplete")
	ErrFinalizationFailed = errors.New("capture finalization failed")
)

func validateRecoveryDuration(recovery RecoveryState, maximum time.Duration) error {
	limit := maximum.Milliseconds()
	if recovery.TerminalTimelineEnd != nil {
		if *recovery.TerminalTimelineEnd < 0 {
			return errors.New("invalid negative terminal timeline")
		}
		if *recovery.TerminalTimelineEnd > limit {
			return ErrCaptureMaximumDuration
		}
	}
	for _, checkpoint := range recovery.GenerationCheckpoints {
		if checkpoint.TimelineThroughMS > limit {
			return ErrCaptureMaximumDuration
		}
	}
	for _, segment := range recovery.Segments {
		if segment.EndMS > limit {
			return ErrCaptureMaximumDuration
		}
	}
	return nil
}
