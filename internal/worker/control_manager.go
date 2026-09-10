package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	recording "conference/recording-bot/internal/recording"
	"conference/recording-bot/internal/signaling"
)

var (
	errAttemptConflict     = errors.New("attempt conflict")
	errWorkerFenceMismatch = errors.New("worker fence mismatch")
	errCapacity            = errors.New("probe capacity exhausted")
	errManagerClosing      = errors.New("probe manager is closing")
	errStartupFailed       = errors.New("probe startup failed")
	errStartupTimeout      = errors.New("probe startup timed out")
	errStopTimeout         = errors.New("probe stop timed out")
)

type attemptIdentity struct {
	attemptID   string
	roomID      string
	recordingID string
}

type controlAttempt struct {
	identity               attemptIdentity
	fingerprint            [sha256.Size]byte
	baseFingerprint        [sha256.Size]byte
	replacementFingerprint [sha256.Size]byte
	captureProtocolVersion string
	captureGeneration      int32
	captureStartedAt       string
	state                  string
	evidence               recording.ProbeEvidence
	failureCode            string
	cancel                 context.CancelFunc
	done                   chan struct{}
	startupDone            chan struct{}
	startupResolved        bool
	startupErr             error
	captureStarted         bool
	stopRequested          bool
	stopFailed             bool
}

type finalizationOperation struct {
	fingerprint [sha256.Size]byte
	done        chan struct{}
	cancel      context.CancelFunc
	err         error
}

type failedAttemptError struct {
	code string
}

func (failure *failedAttemptError) Error() string {
	return "recording attempt failed: " + failure.code
}

// AttemptSnapshot matches worker discovery fields and adds bounded probe evidence.
type AttemptSnapshot struct {
	RecordingID string                  `json:"recordingId"`
	RoomID      string                  `json:"roomId"`
	AttemptID   string                  `json:"attemptId"`
	State       string                  `json:"state"`
	Recoverable bool                    `json:"recoverable"`
	FailureCode string                  `json:"failureCode,omitempty"`
	Evidence    recording.ProbeEvidence `json:"evidence"`
}

type controlAttemptManager struct {
	runner               recording.AssignedProbeRunner
	startTimeout         time.Duration
	stopTimeout          time.Duration
	maxSlots             int
	mu                   sync.Mutex
	attempts             map[string]*controlAttempt
	pendingFinalizations map[string]*finalizationOperation
	closing              bool
}

type controlHealthSnapshot struct {
	attempts             []AttemptSnapshot
	activeRecordingIDs   map[string]struct{}
	pendingFinalizations []string
	occupiedSlots        int
}

func newControlAttemptManager(
	runner recording.AssignedProbeRunner,
	startTimeout time.Duration,
	stopTimeout time.Duration,
	maxSlots int,
) *controlAttemptManager {
	return &controlAttemptManager{
		runner:               runner,
		startTimeout:         startTimeout,
		stopTimeout:          stopTimeout,
		maxSlots:             maxSlots,
		attempts:             map[string]*controlAttempt{},
		pendingFinalizations: map[string]*finalizationOperation{},
	}
}

func (manager *controlAttemptManager) create(request recording.AttemptRequest) (string, error) {
	fingerprint, err := fingerprintRequest(request)
	if err != nil {
		return "", err
	}
	baseFingerprint, err := fingerprintRequestWithoutV2Fence(request)
	if err != nil {
		return "", err
	}
	replacementFingerprint, err := fingerprintReplacementIdentity(request)
	if err != nil {
		return "", err
	}
	manager.mu.Lock()
	if manager.closing {
		manager.mu.Unlock()
		return "", errManagerClosing
	}
	if manager.pendingFinalizations[request.RecordingID] != nil {
		manager.mu.Unlock()
		return "", errAttemptConflict
	}
	if existing := manager.attempts[request.RecordingID]; existing != nil {
		if existing.state == "failed" &&
			canReplaceFailedAttempt(existing, request, replacementFingerprint) {
			delete(manager.attempts, request.RecordingID)
		} else {
			if existing.fingerprint == fingerprint {
				manager.mu.Unlock()
				return manager.awaitStartup(existing)
			}
			fenceMismatch := existing.identity.attemptID == request.AttemptID &&
				existing.baseFingerprint == baseFingerprint &&
				v2AttemptFenceChanged(existing, request)
			manager.mu.Unlock()
			if fenceMismatch {
				return "", errWorkerFenceMismatch
			}
			return "", errAttemptConflict
		}
	}
	if manager.occupiedSlotsLocked() >= manager.maxSlots {
		manager.mu.Unlock()
		return "", errCapacity
	}

	ctx, cancel := context.WithCancel(context.Background())
	attempt := &controlAttempt{
		identity: attemptIdentity{
			attemptID:   request.AttemptID,
			roomID:      request.RoomID,
			recordingID: request.RecordingID,
		},
		fingerprint:            fingerprint,
		baseFingerprint:        baseFingerprint,
		replacementFingerprint: replacementFingerprint,
		captureProtocolVersion: request.ProtocolVersion,
		captureGeneration:      request.CaptureGeneration,
		captureStartedAt:       request.CaptureStartedAt,
		state:                  "starting",
		evidence: recording.ProbeEvidence{
			ObservedSources: []signaling.Source{},
			Codecs:          []string{},
		},
		cancel:      cancel,
		done:        make(chan struct{}),
		startupDone: make(chan struct{}),
	}
	manager.attempts[request.RecordingID] = attempt
	go manager.run(ctx, attempt, request)
	manager.mu.Unlock()
	return manager.awaitStartup(attempt)
}

func canReplaceFailedAttempt(
	existing *controlAttempt,
	request recording.AttemptRequest,
	replacementFingerprint [sha256.Size]byte,
) bool {
	return existing.captureProtocolVersion == recording.CaptureProtocolVersion &&
		request.ProtocolVersion == recording.CaptureProtocolVersion &&
		existing.replacementFingerprint == replacementFingerprint &&
		request.AttemptID != existing.identity.attemptID &&
		request.CaptureGeneration > existing.captureGeneration &&
		request.CaptureStartedAt == existing.captureStartedAt
}

func v2AttemptFenceChanged(existing *controlAttempt, request recording.AttemptRequest) bool {
	return existing.captureProtocolVersion == recording.CaptureProtocolVersion &&
		request.ProtocolVersion == recording.CaptureProtocolVersion &&
		(existing.captureGeneration != request.CaptureGeneration ||
			existing.captureStartedAt != request.CaptureStartedAt)
}

func (manager *controlAttemptManager) awaitStartup(attempt *controlAttempt) (string, error) {
	timer := time.NewTimer(manager.startTimeout)
	defer timer.Stop()
	select {
	case <-attempt.startupDone:
		return manager.startupResult(attempt)
	case <-timer.C:
		manager.mu.Lock()
		if attempt.startupResolved {
			manager.mu.Unlock()
			return manager.startupResult(attempt)
		}
		attempt.state = "stopping"
		attempt.stopRequested = true
		attempt.cancel()
		manager.resolveStartupLocked(attempt, errStartupTimeout)
		manager.mu.Unlock()
		return "", errStartupTimeout
	}
}

func (manager *controlAttemptManager) startupResult(attempt *controlAttempt) (string, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if attempt.startupErr != nil {
		return attempt.state, attempt.startupErr
	}
	if attempt.state == "failed" {
		code := attempt.failureCode
		if code == "" {
			code = "BOT_RUNTIME_EXITED"
		}
		return attempt.state, &failedAttemptError{code: code}
	}
	return attempt.state, nil
}

func (manager *controlAttemptManager) run(
	ctx context.Context,
	attempt *controlAttempt,
	request recording.AttemptRequest,
) {
	runErr := manager.runner.Run(ctx, request, func() {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		if manager.attempts[attempt.identity.recordingID] != attempt || attempt.stopRequested {
			return
		}
		attempt.state = "capturing"
		attempt.captureStarted = true
		manager.resolveStartupLocked(attempt, nil)
	}, func(evidence recording.ProbeEvidence) {
		manager.mu.Lock()
		defer manager.mu.Unlock()
		if manager.attempts[attempt.identity.recordingID] == attempt {
			attempt.evidence = normalizeEvidence(evidence)
		}
	})
	request.Admission = ""
	cleanCompletion := errors.Is(runErr, signaling.ErrAuthoritativeStop)
	if cleanCompletion {
		runErr = nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.attempts[attempt.identity.recordingID] == attempt {
		preReadyExit := !attempt.startupResolved && !attempt.stopRequested
		unexpectedExit := preReadyExit || runErr != nil ||
			(!attempt.stopRequested && !cleanCompletion)
		if unexpectedExit {
			attempt.state = "failed"
			attempt.stopFailed = attempt.stopRequested
			attempt.failureCode = botFailureCode(runErr)
			if attempt.failureCode == "" {
				attempt.failureCode = "BOT_RUNTIME_EXITED"
			}
		}
		if !attempt.startupResolved {
			manager.resolveStartupLocked(attempt, errStartupFailed)
		}
		retainFailure := unexpectedExit &&
			(attempt.captureStarted || attempt.captureProtocolVersion == recording.CaptureProtocolVersion)
		if !retainFailure {
			delete(manager.attempts, attempt.identity.recordingID)
		} else {
			slog.Error(
				"recording capture attempt failed",
				"recordingId", attempt.identity.recordingID,
				"roomId", attempt.identity.roomID,
				"attemptState", attempt.state,
				"failureCode", attempt.failureCode,
				"error", runErr,
			)
		}
	}
	close(attempt.done)
}

func botFailureCode(err error) string {
	switch {
	case errors.Is(err, signaling.ErrAdmissionRejected):
		return "BOT_ADMISSION_REJECTED"
	case errors.Is(err, signaling.ErrConnectTimeout):
		return "BOT_CONNECT_TIMEOUT"
	case errors.Is(err, signaling.ErrRevisionGap),
		errors.Is(err, signaling.ErrRevisionConflict),
		errors.Is(err, signaling.ErrRevisionCapacity):
		return "BOT_SIGNALING_GAP"
	case errors.Is(err, recording.ErrFinalizationFailed):
		return "BOT_FINALIZATION_FAILED"
	case errors.Is(err, recording.ErrCaptureIncomplete),
		errors.Is(err, recording.ErrCaptureMediaUnavailable),
		errors.Is(err, recording.ErrCaptureMaximumDuration):
		return "BOT_CAPTURE_INCOMPLETE"
	case errors.Is(err, context.DeadlineExceeded):
		return "BOT_CONNECT_TIMEOUT"
	}
	var captureFailure *recording.CaptureAPIError
	if errors.As(err, &captureFailure) {
		switch captureFailure.Code {
		case "STALE_CAPTURE_GENERATION", "STALE_WORKER_FENCE":
			return "BOT_CAPTURE_INCOMPLETE"
		case "RECORDING_CAPTURE_SEALED":
			return "BOT_CAPTURE_INCOMPLETE"
		}
	}
	return "BOT_RUNTIME_EXITED"
}

func (manager *controlAttemptManager) resolveStartupLocked(
	attempt *controlAttempt,
	err error,
) {
	if attempt.startupResolved {
		return
	}
	attempt.startupResolved = true
	attempt.startupErr = err
	close(attempt.startupDone)
}

func (manager *controlAttemptManager) stop(recordingID string) (string, error) {
	manager.mu.Lock()
	attempt := manager.attempts[recordingID]
	if attempt == nil {
		manager.mu.Unlock()
		return "stopped", nil
	}
	if !attempt.stopRequested {
		attempt.state = "stopping"
		attempt.stopRequested = true
		attempt.cancel()
	}
	done := attempt.done
	manager.mu.Unlock()

	timer := time.NewTimer(manager.stopTimeout)
	defer timer.Stop()
	select {
	case <-done:
		manager.mu.Lock()
		if manager.attempts[recordingID] == attempt && attempt.stopFailed {
			code := attempt.failureCode
			if code == "" {
				code = "BOT_RUNTIME_EXITED"
			}
			manager.mu.Unlock()
			return attempt.state, &failedAttemptError{code: code}
		}
		if manager.attempts[recordingID] == attempt {
			delete(manager.attempts, recordingID)
		}
		manager.mu.Unlock()
		return "stopped", nil
	case <-timer.C:
		return "stopping", errStopTimeout
	}
}

func (manager *controlAttemptManager) snapshots() []AttemptSnapshot {
	return manager.healthSnapshot().attempts
}

func (manager *controlAttemptManager) healthSnapshot() controlHealthSnapshot {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	attempts := make([]AttemptSnapshot, 0, len(manager.attempts))
	activeRecordingIDs := map[string]struct{}{}
	occupiedSlots := 0
	for _, attempt := range manager.attempts {
		attempts = append(attempts, AttemptSnapshot{
			RecordingID: attempt.identity.recordingID,
			RoomID:      attempt.identity.roomID,
			AttemptID:   attempt.identity.attemptID,
			State:       attempt.state,
			Recoverable: false,
			FailureCode: attempt.failureCode,
			Evidence:    attempt.evidence,
		})
		if attempt.state == "starting" || attempt.state == "capturing" || attempt.state == "stopping" {
			activeRecordingIDs[attempt.identity.recordingID] = struct{}{}
			occupiedSlots++
		}
	}
	sort.Slice(attempts, func(left, right int) bool {
		return attempts[left].RecordingID < attempts[right].RecordingID
	})
	pendingFinalizations := make([]string, 0, len(manager.pendingFinalizations))
	for recordingID := range manager.pendingFinalizations {
		pendingFinalizations = append(pendingFinalizations, recordingID)
	}
	sort.Strings(pendingFinalizations)
	return controlHealthSnapshot{
		attempts:             attempts,
		activeRecordingIDs:   activeRecordingIDs,
		pendingFinalizations: pendingFinalizations,
		occupiedSlots:        occupiedSlots,
	}
}

func (manager *controlAttemptManager) occupiedSlots() int {
	return manager.healthSnapshot().occupiedSlots
}

func (manager *controlAttemptManager) occupiedSlotsLocked() int {
	count := 0
	for _, attempt := range manager.attempts {
		if attempt.state == "starting" || attempt.state == "capturing" || attempt.state == "stopping" {
			count++
		}
	}
	return count
}

func (manager *controlAttemptManager) close(ctx context.Context) error {
	manager.mu.Lock()
	manager.closing = true
	attempts := make([]*controlAttempt, 0, len(manager.attempts))
	for _, attempt := range manager.attempts {
		attempt.state = "stopping"
		attempt.stopRequested = true
		attempt.cancel()
		attempts = append(attempts, attempt)
	}
	finalizations := make([]*finalizationOperation, 0, len(manager.pendingFinalizations))
	for _, operation := range manager.pendingFinalizations {
		operation.cancel()
		finalizations = append(finalizations, operation)
	}
	manager.mu.Unlock()
	for _, attempt := range attempts {
		select {
		case <-attempt.done:
		case <-ctx.Done():
			return errStopTimeout
		}
	}
	for _, operation := range finalizations {
		select {
		case <-operation.done:
		case <-ctx.Done():
			return errStopTimeout
		}
	}
	return nil
}

func (manager *controlAttemptManager) finalize(
	ctx context.Context,
	request recording.FinalizationRequest,
	timeout time.Duration,
	run func(context.Context) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	operation, err := manager.startFinalization(request, timeout, run)
	if err != nil {
		return err
	}

	select {
	case <-operation.done:
		return operation.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (manager *controlAttemptManager) startFinalization(
	request recording.FinalizationRequest,
	timeout time.Duration,
	run func(context.Context) error,
) (*finalizationOperation, error) {
	fingerprint, err := fingerprintFinalizationRequest(request)
	if err != nil {
		return nil, err
	}
	manager.mu.Lock()
	if manager.closing {
		manager.mu.Unlock()
		return nil, errManagerClosing
	}
	if attempt := manager.attempts[request.RecordingID]; attempt != nil && activeAttempt(attempt) {
		manager.mu.Unlock()
		return nil, errAttemptConflict
	}
	operation := manager.pendingFinalizations[request.RecordingID]
	if operation != nil && operation.fingerprint != fingerprint {
		manager.mu.Unlock()
		return nil, errAttemptConflict
	}
	if operation == nil {
		operationContext, cancel := context.WithTimeout(context.Background(), timeout)
		operation = &finalizationOperation{
			fingerprint: fingerprint,
			done:        make(chan struct{}),
			cancel:      cancel,
		}
		manager.pendingFinalizations[request.RecordingID] = operation
		go manager.runFinalization(operationContext, request.RecordingID, operation, run)
	}
	manager.mu.Unlock()
	return operation, nil
}

func activeAttempt(attempt *controlAttempt) bool {
	return attempt.state == "starting" || attempt.state == "capturing" || attempt.state == "stopping"
}

func (manager *controlAttemptManager) runFinalization(
	ctx context.Context,
	recordingID string,
	operation *finalizationOperation,
	run func(context.Context) error,
) {
	err := run(ctx)
	operation.cancel()
	manager.mu.Lock()
	operation.err = err
	if manager.pendingFinalizations[recordingID] == operation {
		delete(manager.pendingFinalizations, recordingID)
	}
	if err == nil {
		if attempt := manager.attempts[recordingID]; attempt != nil && attempt.state == "failed" {
			delete(manager.attempts, recordingID)
		}
	} else {
		slog.Error(
			"recording capture finalization failed",
			"recordingId", recordingID,
			"attemptState", "finalizing",
			"failureCode", "BOT_FINALIZATION_FAILED",
		)
	}
	close(operation.done)
	manager.mu.Unlock()
}

func (manager *controlAttemptManager) finalizations() []string {
	return manager.healthSnapshot().pendingFinalizations
}

func fingerprintRequest(request recording.AttemptRequest) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func fingerprintRequestWithoutV2Fence(
	request recording.AttemptRequest,
) ([sha256.Size]byte, error) {
	request.CaptureGeneration = 0
	request.CaptureStartedAt = ""
	return fingerprintRequest(request)
}

func fingerprintReplacementIdentity(request recording.AttemptRequest) ([sha256.Size]byte, error) {
	request.AttemptID = ""
	request.Admission = ""
	request.WorkerGenerationID = ""
	request.CaptureGeneration = 0
	return fingerprintRequest(request)
}

func fingerprintFinalizationRequest(
	request recording.FinalizationRequest,
) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func normalizeEvidence(evidence recording.ProbeEvidence) recording.ProbeEvidence {
	result := recording.ProbeEvidence{
		ObservedSources: append([]signaling.Source{}, evidence.ObservedSources...),
		Codecs:          append([]string{}, evidence.Codecs...),
		Complete:        evidence.Complete,
	}
	sort.Slice(result.ObservedSources, func(left, right int) bool {
		return result.ObservedSources[left] < result.ObservedSources[right]
	})
	sort.Strings(result.Codecs)
	return result
}
