package recording

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

type CaptureFailureControlPlane interface {
	Fail(context.Context, CaptureFence, string, string) error
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
	MediaStartupTimeout      time.Duration
	SegmentOperationTimeout  time.Duration
	MaximumConcurrentUploads int
	Now                      func() time.Time
	newPacketWriter          packetWriterFactory
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
	if dependencies.MediaStartupTimeout == 0 {
		dependencies.MediaStartupTimeout = dependencies.FinalizeTimeout
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
		dependencies.FinalizeTimeout <= 0 || dependencies.MediaStartupTimeout <= 0 ||
		dependencies.SegmentOperationTimeout <= 0 ||
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
	spoolFence := newSpoolMutationFence(attemptFence, request.CaptureGeneration)
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
	if err := runner.dependencies.Spool.CleanupStaging(spoolFence); err != nil {
		return err
	}
	defer runner.dependencies.Spool.CleanupStaging(spoolFence)
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
			spoolFence,
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
		runner: runner, fence: fence, spoolFence: spoolFence, timeline: timeline, startedAt: startedAt,
		checkpointSequence: nextCheckpointSequence(recovery.GenerationCheckpoints, fence.CaptureGeneration),
		writers:            map[string]*RTPTrackWriter{}, trackInputs: map[string]signaling.TrackEvent{},
		sequences:    recoveredSequences(recovery, manifest, fence.CaptureGeneration),
		writerErrors: make(chan trackWriterFailure, 32),
	}
	if checkpoint, exists := latestCheckpoint(recovery.GenerationCheckpoints); exists {
		session.checkpointBoundary = checkpoint.TimelineThroughMS
		recoveryBoundaryMS := session.timelineMS()
		if err := timeline.DeclareRecoveryGaps(checkpoint.TimelineThroughMS, recoveryBoundaryMS); err != nil {
			return err
		}
		session.coverageFloorMS = recoveryBoundaryMS
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
	session.peer = peer
	controller, err := signaling.StartProbeWithObserver(ctx, signalingClient, peer, session)
	if err != nil {
		return err
	}
	defer controller.Close()
	captureContext := context.WithoutCancel(ctx)
	failAndSeal := func(cause error) error {
		cancelRun()
		return session.failAndSeal(errors.Join(cause, controller.Close()))
	}
	lifecycle := make(chan error, 1)
	go func() { lifecycle <- controller.Run(captureContext) }()
	resumeContext, cancelResume := context.WithCancel(ctx)
	defer cancelResume()
	resume := make(chan error, 1)
	go func() { resume <- session.resumePending(resumeContext, manifest) }()
	pendingInitialTracks := activeProducerIDs(controller.State().Producers)
	var mediaStartupTimer *time.Timer
	var mediaStartupDeadline <-chan time.Time
	if len(pendingInitialTracks) > 0 {
		mediaStartupTimer = time.NewTimer(runner.dependencies.MediaStartupTimeout)
		mediaStartupDeadline = mediaStartupTimer.C
		defer mediaStartupTimer.Stop()
	}
	recoveryPending := true
	readyReported := false
	reportReady := func() {
		if readyReported || recoveryPending || len(pendingInitialTracks) != 0 {
			return
		}
		readyReported = controller.WhileActive(ctx, ready)
		if readyReported && mediaStartupTimer != nil {
			mediaStartupTimer.Stop()
			mediaStartupDeadline = nil
		}
	}
	for {
		select {
		case resumeErr := <-resume:
			resume = nil
			recoveryPending = false
			if resumeErr != nil {
				if ctx.Err() != nil {
					if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						return failAndSeal(errors.Join(
							ErrCaptureMaximumDuration,
							resumeErr,
						))
					}
					return errors.Join(controller.Close(), session.cleanStop())
				}
				return failAndSeal(resumeErr)
			}
			reportReady()
		case event, open := <-peer.Tracks():
			if !open {
				resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
				return failAndSeal(errors.Join(errors.New("Pion track channel closed"), resumeErr))
			}
			if err := session.startTrack(captureContext, event); err != nil {
				resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
				return failAndSeal(errors.Join(err, resumeErr))
			}
			delete(pendingInitialTracks, event.ProducerID)
			reportReady()
			report(ProbeEvidence{ObservedSources: []signaling.Source{event.Source}, Codecs: []string{event.Codec}})
		case <-mediaStartupDeadline:
			pendingInitialTracks = retainActiveProducers(
				pendingInitialTracks,
				controller.State().Producers,
			)
			if len(pendingInitialTracks) == 0 {
				mediaStartupDeadline = nil
				reportReady()
				continue
			}
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			return failAndSeal(errors.Join(ErrCaptureMediaUnavailable, resumeErr))
		case peerErr, open := <-peer.Errors():
			if !open || peerErr == nil {
				peerErr = errors.New("Pion error channel closed")
			}
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			return failAndSeal(errors.Join(peerErr, resumeErr))
		case failure := <-session.writerErrors:
			accepted, gapErr := session.acceptWriterFailure(failure)
			if accepted {
				resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
				return failAndSeal(errors.Join(failure.err, gapErr, resumeErr))
			}
		case lifecycleErr := <-lifecycle:
			if errors.Is(lifecycleErr, signaling.ErrAuthoritativeStop) {
				finalContext, cancelFinal := context.WithTimeout(
					context.Background(),
					runner.dependencies.FinalizeTimeout,
				)
				recovery, recoveryErr := session.waitForTerminalRecovery(finalContext)
				if recoveryErr != nil || recovery.TerminalTimelineEnd == nil {
					cancelFinal()
					resumeErr := stopPendingResume(
						cancelResume,
						resume,
						runner.dependencies.FinalizeTimeout,
					)
					return failAndSeal(errors.Join(
						ErrFinalizationFailed,
						recoveryErr,
						resumeErr,
					))
				}
				terminalEnd := *recovery.TerminalTimelineEnd
				writers := session.stopWritersAt(terminalEnd)
				controllerErr := controller.Close()
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
				cancelFinal()
				if controllerErr != nil {
					cancelRun()
					resumeErr := stopPendingResume(
						cancelResume,
						resume,
						runner.dependencies.FinalizeTimeout,
					)
					return session.failAndSeal(errors.Join(
						ErrFinalizationFailed,
						controllerErr,
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
				if err := session.finishCleanStopWithRecovery(recovery); err != nil {
					return fmt.Errorf("%w: %v", ErrFinalizationFailed, err)
				}
				return signaling.ErrAuthoritativeStop
			}
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			if ctx.Err() == nil {
				return failAndSeal(errors.Join(lifecycleErr, resumeErr))
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return failAndSeal(errors.Join(
					ErrCaptureMaximumDuration,
					lifecycleErr,
					resumeErr,
				))
			}
			return errors.Join(controller.Close(), session.cleanStop(), resumeErr)
		case <-ctx.Done():
			resumeErr := stopPendingResume(cancelResume, resume, runner.dependencies.FinalizeTimeout)
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return failAndSeal(errors.Join(ErrCaptureMaximumDuration, resumeErr))
			}
			controllerErr := controller.Close()
			if stopErr := session.cleanStop(); stopErr != nil {
				return errors.Join(ErrFinalizationFailed, controllerErr, stopErr, resumeErr)
			}
			return errors.Join(controllerErr, resumeErr)
		}
	}
}

func activeProducerIDs(producers []signaling.Producer) map[string]struct{} {
	result := make(map[string]struct{}, len(producers))
	for _, producer := range producers {
		if !producer.Paused {
			result[producer.ProducerID] = struct{}{}
		}
	}
	return result
}

func retainActiveProducers(
	pending map[string]struct{},
	producers []signaling.Producer,
) map[string]struct{} {
	active := activeProducerIDs(producers)
	for producerID := range pending {
		if _, exists := active[producerID]; !exists {
			delete(pending, producerID)
		}
	}
	return pending
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
	spoolFence := newSpoolMutationFence(attemptFence, recovery.CaptureGeneration)
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
	terminalEnd := *recovery.TerminalTimelineEnd
	checkpoint, exists := latestCheckpoint(recovery.GenerationCheckpoints)
	if !exists {
		failureAPI, supportsFailure := runner.dependencies.API.(CaptureFailureControlPlane)
		if !supportsFailure {
			return errors.New("seal-only recovery has no checkpoint")
		}
		if err := failureAPI.Fail(
			ctx,
			fence,
			"BOT_CAPTURE_INCOMPLETE",
			"Capture ended before any durable timeline checkpoint",
		); err != nil {
			return fmt.Errorf("persist empty terminal capture failure: %w", err)
		}
		return runner.dependencies.Spool.MarkFailed(spoolFence)
	}
	if checkpoint.TimelineThroughMS > terminalEnd {
		return errors.New("seal-only checkpoint exceeds terminal timeline")
	}
	if checkpoint.CaptureGeneration == recovery.CaptureGeneration {
		checkpointRecords, recordsErr := timeline.RecordsThrough(checkpoint.TimelineThroughMS, false)
		if recordsErr != nil {
			return recordsErr
		}
		if err := runner.dependencies.Spool.ReconcileCheckpoint(
			spoolFence,
			checkpoint,
			checkpointRecords,
		); err != nil {
			return err
		}
	}
	_, err = runner.dependencies.API.Seal(ctx, fence, SealRequest{
		Completeness: "partial", TimelineEndMS: terminalEnd,
		LastCheckpointCaptureGeneration: checkpoint.CaptureGeneration,
		LastCheckpointSequence:          checkpoint.CheckpointSequence,
		CheckpointPayloadSHA256:         checkpoint.PayloadSHA256,
	})
	if err != nil {
		var failure *CaptureAPIError
		failureAPI, supportsFailure := runner.dependencies.API.(CaptureFailureControlPlane)
		if errors.As(err, &failure) && failure.Code == "CAPTURE_MANIFEST_CONFLICT" && supportsFailure {
			if failErr := failureAPI.Fail(
				ctx,
				fence,
				"BOT_CAPTURE_INCOMPLETE",
				"Capture manifest failed validation",
			); failErr != nil {
				return fmt.Errorf("persist terminal capture failure: %w", failErr)
			}
			return runner.dependencies.Spool.MarkFailed(spoolFence)
		}
		return err
	}
	return runner.dependencies.Spool.MarkSealed(spoolFence)
}

type captureSession struct {
	runner               *CaptureAttemptRunner
	fence                CaptureFence
	spoolFence           SpoolMutationFence
	timeline             *TimelineState
	startedAt            time.Time
	mu                   sync.Mutex
	observerMu           sync.Mutex
	writerMu             sync.Mutex
	checkpointMu         sync.Mutex
	checkpointSequence   int32
	checkpointBoundary   int64
	coverageFloorMS      int64
	writers              map[string]*RTPTrackWriter
	peer                 CapturePeer
	startingWriters      map[string]struct{}
	writerHandoffs       map[string]int64
	failedWriterCoverage map[string]int64
	trackInputs          map[string]signaling.TrackEvent
	sequences            map[string]int32
	writerErrors         chan trackWriterFailure
	stopping             bool
}

type trackWriterFailure struct {
	trackID string
	writer  *RTPTrackWriter
	err     error
}

type fencedStagingBudget struct {
	spool *DurableSpool
	fence SpoolMutationFence
}

func (budget fencedStagingBudget) ResizeStaging(recordingID, path string, size int64) error {
	if recordingID != budget.fence.RecordingID {
		return ErrAttemptFenceMismatch
	}
	return budget.spool.ResizeStaging(budget.fence, path, size)
}

func (budget fencedStagingBudget) ReleaseStaging(recordingID, path string) {
	if recordingID == budget.fence.RecordingID {
		budget.spool.ReleaseStaging(budget.fence, path)
	}
}

func newSpoolMutationFence(fence AttemptFence, captureGeneration int32) SpoolMutationFence {
	return SpoolMutationFence{
		AttemptID:          fence.AttemptID,
		RecordingID:        fence.RecordingID,
		WorkerID:           fence.WorkerID,
		WorkerGenerationID: fence.WorkerGenerationID,
		CaptureGeneration:  captureGeneration,
	}
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
	for _, producer := range snapshot.Producers {
		if producer.Paused {
			continue
		}
		if err := session.beginTrackCoverage(producer.ProducerID); err != nil {
			return err
		}
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
	if event.Type == "bot:producerAdded" ||
		(event.Type == "bot:producerUpdated" && event.Producer != nil && !event.Producer.Paused) ||
		event.Type == "bot:producerResumed" {
		if err := session.beginTrackCoverage(eventProducerID(event)); err != nil {
			return err
		}
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
	session.observerMu.Lock()
	defer session.observerMu.Unlock()
	if session.isStopping() {
		return errCaptureSessionStopping
	}
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

func (session *captureSession) beginTrackCoverage(producerID string) error {
	trackID, _, exists := session.timeline.TrackForProducer(producerID)
	if !exists {
		return errors.New("producer has no active timeline track")
	}
	coverageStartMS, exists := session.timeline.ActiveTrackStart(trackID)
	if !exists {
		return errors.New("remote track has no active timeline interval")
	}
	session.mu.Lock()
	_, writerStarting := session.startingWriters[trackID]
	writerExists := session.writers[trackID] != nil || writerStarting
	if session.coverageFloorMS > coverageStartMS {
		coverageStartMS = session.coverageFloorMS
	}
	if handoffMS, handedOff := session.writerHandoffs[trackID]; handedOff && handoffMS > coverageStartMS {
		coverageStartMS = handoffMS
	}
	session.mu.Unlock()
	if writerExists {
		return nil
	}
	return session.timeline.BeginTrackCoverage(trackID, coverageStartMS)
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
	coverageStartMS, exists := session.timeline.ActiveTrackStart(trackID)
	if !exists {
		return errors.New("remote track has no active timeline interval")
	}
	if session.coverageFloorMS > coverageStartMS {
		coverageStartMS = session.coverageFloorMS
	}
	session.mu.Lock()
	previous := session.writers[trackID]
	if handoffMS, handedOff := session.writerHandoffs[trackID]; handedOff && handoffMS > coverageStartMS {
		coverageStartMS = handoffMS
	}
	if session.writerErrors == nil {
		session.writerErrors = make(chan trackWriterFailure, 32)
	}
	session.mu.Unlock()
	if previous != nil {
		handoffMS := session.nextTimelinePoint()
		closeContext, cancel := context.WithTimeout(context.Background(), session.runner.dependencies.FinalizeTimeout)
		err := previous.CloseAt(closeContext, handoffMS)
		cancel()
		if err != nil {
			return err
		}
		coverageStartMS = handoffMS
	}
	session.mu.Lock()
	if session.startingWriters == nil {
		session.startingWriters = map[string]struct{}{}
	}
	session.startingWriters[trackID] = struct{}{}
	session.mu.Unlock()
	writerRegistered := false
	defer func() {
		if writerRegistered {
			return
		}
		session.mu.Lock()
		delete(session.startingWriters, trackID)
		session.mu.Unlock()
	}()
	if err := session.timeline.BeginTrackCoverage(trackID, coverageStartMS); err != nil {
		return err
	}
	session.mu.Lock()
	initialSequence := session.sequences[trackID]
	session.mu.Unlock()
	if err := event.SourceTrack.SetReadDeadline(time.Time{}); err != nil {
		resolveErr := session.timeline.ResolveTrackCoverage(
			trackID,
			coverageStartMS,
			max(coverageStartMS, session.timelineMS()),
		)
		return errors.Join(errors.New("reset remote track read deadline"), resolveErr)
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
		CoverageStartMS: coverageStartMS,
		OnInitialGap: func(startMS, endMS int64) error {
			return session.timeline.ResolveTrackCoverage(trackID, startMS, endMS)
		},
		RequestKeyFrame:     session.keyFrameRequester(event),
		InitialSequence:     initialSequence,
		OperationTimeout:    session.runner.dependencies.SegmentOperationTimeout,
		MaximumStorageBytes: session.runner.dependencies.MaximumStorageBytes,
		RecordingID:         session.fence.RecordingID,
		StagingBudget: fencedStagingBudget{
			spool: session.runner.dependencies.Spool,
			fence: session.spoolFence,
		},
		OnSegment:       session.segmentReady,
		newPacketWriter: session.runner.dependencies.newPacketWriter,
	})
	if err != nil {
		resolveErr := session.timeline.ResolveTrackCoverage(trackID, coverageStartMS, max(coverageStartMS, session.timelineMS()))
		return errors.Join(err, resolveErr)
	}
	session.mu.Lock()
	session.writers[trackID] = writer
	delete(session.startingWriters, trackID)
	delete(session.writerHandoffs, trackID)
	writerRegistered = true
	session.mu.Unlock()
	go session.observeWriter(ctx, trackID, writer)
	return nil
}

func (session *captureSession) keyFrameRequester(event signaling.TrackEvent) KeyFrameRequester {
	if session.peer == nil || event.Kind != signaling.KindVideo || event.MediaSSRC == 0 {
		return nil
	}
	ssrc := event.MediaSSRC
	return func(ctx context.Context) error {
		return session.peer.RequestKeyFrame(ctx, ssrc)
	}
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

func (session *captureSession) acceptWriterFailure(failure trackWriterFailure) (bool, error) {
	session.mu.Lock()
	if session.writers[failure.trackID] != failure.writer {
		session.mu.Unlock()
		return false, nil
	}
	delete(session.writers, failure.trackID)
	session.mu.Unlock()

	coverageEnd, err := session.failedWriterCoverageEnd(failure)
	if err != nil {
		return true, err
	}
	session.checkpointMu.Lock()
	defer session.checkpointMu.Unlock()
	session.mu.Lock()
	if session.failedWriterCoverage == nil {
		session.failedWriterCoverage = map[string]int64{}
	}
	session.failedWriterCoverage[failure.trackID] = coverageEnd
	checkpointBoundary := session.checkpointBoundary
	session.mu.Unlock()
	return true, session.declareFailedWriterGapsLocked(checkpointBoundary)
}

func (session *captureSession) failedWriterCoverageEnd(failure trackWriterFailure) (int64, error) {
	coverageEnd := failure.writer.coverageEnd()
	if session.runner == nil || session.runner.dependencies.Spool == nil {
		return coverageEnd, nil
	}
	manifest, err := session.runner.dependencies.Spool.Snapshot(session.fence.RecordingID)
	if err != nil {
		return 0, err
	}
	for _, segment := range manifest.Segments {
		isCurrentTrack := segment.TrackID == failure.trackID &&
			segment.CaptureGeneration == session.fence.CaptureGeneration
		if isCurrentTrack && segment.EndMS > coverageEnd {
			coverageEnd = segment.EndMS
		}
	}
	return coverageEnd, nil
}

func (session *captureSession) checkpointBoundaryWithFailedWriterGaps() (int64, error) {
	session.checkpointMu.Lock()
	defer session.checkpointMu.Unlock()
	session.mu.Lock()
	checkpointBoundary := session.checkpointBoundary
	session.mu.Unlock()
	return checkpointBoundary, session.declareFailedWriterGapsLocked(checkpointBoundary)
}

func (session *captureSession) declareFailedWriterGapsLocked(throughMS int64) error {
	session.mu.Lock()
	coverage := make(map[string]int64, len(session.failedWriterCoverage))
	for trackID, startMS := range session.failedWriterCoverage {
		coverage[trackID] = startMS
	}
	session.mu.Unlock()
	for trackID, startMS := range coverage {
		if throughMS <= startMS {
			continue
		}
		if err := session.timeline.DeclareGap(trackID, startMS, throughMS, "source_unavailable"); err != nil {
			return err
		}
		session.mu.Lock()
		if session.failedWriterCoverage[trackID] == startMS {
			session.failedWriterCoverage[trackID] = throughMS
		}
		session.mu.Unlock()
	}
	return nil
}

func (session *captureSession) closeTrackBarriers(ctx context.Context, trackIDs []string, atMS int64) error {
	session.writerMu.Lock()
	writers := make([]*RTPTrackWriter, 0, len(trackIDs))
	for _, trackID := range trackIDs {
		session.mu.Lock()
		writer := session.writers[trackID]
		delete(session.writers, trackID)
		if writer != nil {
			if session.writerHandoffs == nil {
				session.writerHandoffs = map[string]int64{}
			}
			session.writerHandoffs[trackID] = atMS
		}
		session.mu.Unlock()
		if writer == nil {
			if err := session.timeline.ResolvePendingTrackCoverage(trackID, atMS); err != nil {
				session.writerMu.Unlock()
				return err
			}
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
		session.spoolFence,
		descriptor,
		stagingPath,
	)
	if stored.TrackID != "" {
		session.reserveSequence(stored.TrackID, stored.Sequence)
	}
	if err != nil {
		return err
	}
	uploadReady, err := session.checkpointBeforeUpload(ctx, descriptor.EndMS)
	if err != nil {
		return err
	}
	if !uploadReady {
		return nil
	}
	if err := session.upload(ctx, stored); err != nil {
		return err
	}
	return session.runner.dependencies.Spool.RemoveCommitted(session.spoolFence)
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
		session.spoolFence,
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
		uploadReady, err := session.checkpointBeforeUpload(ctx, segment.EndMS)
		if err != nil {
			return err
		}
		if !uploadReady {
			continue
		}
		if err := session.upload(ctx, segment); err != nil {
			return err
		}
	}
	return session.runner.dependencies.Spool.RemoveCommitted(session.spoolFence)
}

func (session *captureSession) checkpointBeforeUpload(ctx context.Context, throughMS int64) (bool, error) {
	err := session.checkpoint(ctx, throughMS, false)
	var failure *CaptureAPIError
	if !errors.As(err, &failure) || failure.Status != http.StatusConflict ||
		failure.Code != "TERMINAL_TIMELINE_CONFLICT" {
		return err == nil, err
	}
	// close-inputs can publish the terminal fence while an already closed
	// segment is being processed. Keep it in the durable spool until every
	// writer and lifecycle input has drained; finalization then publishes one
	// stable terminal checkpoint before resuming the upload.
	recovery, recoveryErr := session.runner.dependencies.API.Recover(ctx, session.fence)
	if recoveryErr != nil {
		return false, errors.Join(err, recoveryErr)
	}
	if recovery.TerminalTimelineEnd == nil || throughMS > *recovery.TerminalTimelineEnd {
		return false, err
	}
	return false, nil
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
		session.spoolFence,
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
	if err := session.closeWritersAt(finalContext, end); err != nil {
		return err
	}
	return session.finishCleanStopWithRecovery(recovery)
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
	return session.finishCleanStopWithRecovery(recovery)
}

func (session *captureSession) finishCleanStopWithRecovery(recovery RecoveryState) error {
	if recovery.TerminalTimelineEnd == nil {
		return errors.New("clean stop has no authoritative terminal timeline")
	}
	end := *recovery.TerminalTimelineEnd
	checkpointBoundary := session.currentCheckpointBoundary()
	if end < checkpointBoundary {
		return errors.New("authoritative terminal timeline regressed local checkpoint")
	}
	finalContext, cancel := context.WithTimeout(context.Background(), session.runner.dependencies.FinalizeTimeout)
	defer cancel()
	if err := session.checkpoint(finalContext, end, true); err != nil {
		return err
	}
	manifest, err := session.runner.dependencies.Spool.Snapshot(session.fence.RecordingID)
	if err != nil {
		return err
	}
	if err := session.resumePending(finalContext, manifest); err != nil {
		return err
	}
	manifest, err = session.runner.dependencies.Spool.Snapshot(session.fence.RecordingID)
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
	return session.runner.dependencies.Spool.MarkSealed(session.spoolFence)
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
	recovery, recoveryErr := session.runner.dependencies.API.Recover(finalContext, session.fence)
	var closeErr error
	if recoveryErr == nil && recovery.TerminalTimelineEnd != nil {
		if err := validateRecoveryDuration(
			recovery,
			session.runner.dependencies.MaximumCaptureDuration,
		); err != nil {
			closeErr = session.closeWriters(finalContext)
			return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
		}
		closeErr = session.closeWritersAt(finalContext, *recovery.TerminalTimelineEnd)
	} else {
		closeErr = session.closeWriters(finalContext)
	}
	if errors.Is(closeErr, errTrackWriterStillRunning) {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr)
	}
	if recoveryErr == nil && recovery.TerminalTimelineEnd == nil {
		recovery, recoveryErr = session.waitForTerminalRecovery(finalContext)
	}
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
	checkpointBoundary, gapErr := session.checkpointBoundaryWithFailedWriterGaps()
	if gapErr != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, gapErr)
	}
	if end < checkpointBoundary {
		return errors.Join(
			ErrCaptureIncomplete,
			ErrFinalizationFailed,
			cause,
			closeErr,
			errors.New("authoritative terminal timeline regressed local checkpoint"),
		)
	}
	if err := session.timeline.DeclareRecoveryGaps(checkpointBoundary, end); err != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
	}
	if err := session.checkpoint(finalContext, end, true); err != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
	}
	manifest, err := session.runner.dependencies.Spool.Snapshot(session.fence.RecordingID)
	if err != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
	}
	if err := session.resumePending(finalContext, manifest); err != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
	}
	manifest, err = session.runner.dependencies.Spool.Snapshot(session.fence.RecordingID)
	if err != nil {
		return errors.Join(ErrCaptureIncomplete, ErrFinalizationFailed, cause, closeErr, err)
	}
	_, sealErr := session.runner.dependencies.API.Seal(finalContext, session.fence, SealRequest{
		Completeness: "partial", TimelineEndMS: end,
		LastCheckpointCaptureGeneration: session.fence.CaptureGeneration,
		LastCheckpointSequence:          manifest.LastCheckpointSequence,
		CheckpointPayloadSHA256:         manifest.LastCheckpointHash,
	})
	if sealErr == nil {
		sealErr = session.runner.dependencies.Spool.MarkSealed(session.spoolFence)
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

func (session *captureSession) currentCheckpointBoundary() int64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.checkpointBoundary
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
		MediaStartupTimeout:      config.StartTimeout,
		Now:                      time.Now,
	})
}

var ErrCaptureMaximumDuration = errors.New("capture maximum duration reached")

var (
	ErrCaptureIncomplete       = errors.New("capture incomplete")
	ErrCaptureMediaUnavailable = errors.New("active producer media did not reach capture")
	ErrFinalizationFailed      = errors.New("capture finalization failed")
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
