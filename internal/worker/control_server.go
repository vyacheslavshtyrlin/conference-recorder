package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"

	recording "conference/recording-bot/internal/recording"
	"conference/recording-bot/internal/signaling"
)

const maxAttemptBodyBytes = 16 * 1024

// ControlShimConfig defines the static test-only worker identity and auth fence.
type ControlShimConfig struct {
	WorkerID        string
	GenerationID    string
	JWTSecret       string
	StartTimeout    time.Duration
	StopTimeout     time.Duration
	FinalizeTimeout time.Duration
	MaxSlots        int
}

// ControlShim is the test-only worker HTTP boundary and attempt owner.
type ControlShim struct {
	handler http.Handler
	manager *controlAttemptManager
}

type RecoveryHealthReporter interface {
	RecoveryHealth() (recoverable []string, pendingFinalizations []string, err error)
}

// ServeHTTP implements http.Handler.
func (shim *ControlShim) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	shim.handler.ServeHTTP(response, request)
}

// Close requests bounded stop for every active probe.
func (shim *ControlShim) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), shim.manager.stopTimeout)
	defer cancel()
	_ = shim.manager.close(ctx)
}

func (shim *ControlShim) Shutdown(ctx context.Context) error {
	return shim.manager.close(ctx)
}

// NewControlShimHandler exposes the existing worker discovery and attempt boundary.
func NewControlShimHandler(
	config ControlShimConfig,
	runner recording.AssignedProbeRunner,
) (*ControlShim, error) {
	return newControlHandler(config, runner, nil)
}

// NewProductionControlHandler exposes the ordinary fenced assignment boundary.
// Readiness succeeds only while every supplied production prerequisite is true.
func NewProductionControlHandler(
	config ControlShimConfig,
	runner recording.AssignedProbeRunner,
	readyCheck func() error,
) (*ControlShim, error) {
	if readyCheck == nil {
		return nil, errors.New("production readiness check is required")
	}
	return newControlHandler(config, runner, readyCheck)
}

func newControlHandler(
	config ControlShimConfig,
	runner recording.AssignedProbeRunner,
	readyCheck func() error,
) (*ControlShim, error) {
	if runner == nil {
		return nil, errors.New("assigned probe runner is required")
	}
	if config.MaxSlots == 0 {
		config.MaxSlots = 1
	}
	if config.FinalizeTimeout == 0 {
		config.FinalizeTimeout = config.StopTimeout
	}
	if err := validateControlConfig(config); err != nil {
		return nil, err
	}
	manager := newControlAttemptManager(runner, config.StartTimeout, config.StopTimeout, config.MaxSlots)
	auth := controlJWTConfig{
		issuer:        "conference-signaling",
		audience:      "conference-recording-bot-control",
		secret:        []byte(config.JWTSecret),
		maxTTLSeconds: 300,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, map[string]any{"status": "ok"})
	})
	mux.HandleFunc("GET /health/ready", func(response http.ResponseWriter, request *http.Request) {
		if auth.verify(request.Header.Get("Authorization"), time.Now()) != nil {
			writeJSON(response, http.StatusUnauthorized, map[string]string{"error": "UNAUTHORIZED"})
			return
		}
		statusCode := http.StatusOK
		status := "ready"
		capacity := config.MaxSlots
		if readyCheck != nil {
			if err := readyCheck(); err != nil {
				statusCode = http.StatusServiceUnavailable
				status = "not_ready"
				capacity = 0
			}
		}
		recoverable := []string{}
		pendingFinalizations := manager.finalizations()
		if reporter, ok := runner.(RecoveryHealthReporter); ok {
			spoolRecoverable, spoolFinalizations, recoveryErr := reporter.RecoveryHealth()
			recoverable = spoolRecoverable
			pendingFinalizations = mergeSortedStrings(pendingFinalizations, spoolFinalizations)
			if recoveryErr != nil {
				statusCode = http.StatusServiceUnavailable
				status = "not_ready"
				capacity = 0
			}
		}
		payload := map[string]any{
			"status":                          status,
			"workerId":                        config.WorkerID,
			"generationId":                    config.GenerationID,
			"capacity":                        capacity,
			"occupiedSlots":                   manager.occupiedSlots(),
			"attempts":                        manager.snapshots(),
			"recoverableRecordingIds":         recoverable,
			"pendingFinalizationRecordingIds": pendingFinalizations,
		}
		if readyCheck != nil {
			payload["acceptingAttempts"] = statusCode == http.StatusOK
		}
		writeJSON(response, statusCode, payload)
	})
	mux.HandleFunc("PUT /internal/v1/attempts/{recordingId}", func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		if !authorizeControl(response, request, config, auth) {
			return
		}
		attempt, err := decodeAttempt(response, request)
		if err != nil {
			writeJSON(response, http.StatusBadRequest, map[string]string{"error": "INVALID_PAYLOAD"})
			return
		}
		pathRecordingID := request.PathValue("recordingId")
		bodyFenceMatches := attempt.WorkerID == config.WorkerID &&
			attempt.WorkerGenerationID == config.GenerationID
		if !bodyFenceMatches {
			writeJSON(response, http.StatusConflict, map[string]string{"error": "WORKER_FENCE_MISMATCH"})
			return
		}
		if attempt.RecordingID != pathRecordingID {
			writeJSON(response, http.StatusBadRequest, map[string]string{"error": "INVALID_PAYLOAD"})
			return
		}
		state, createErr := manager.create(attempt)
		switch {
		case errors.Is(createErr, errWorkerFenceMismatch):
			writeJSON(response, http.StatusConflict, map[string]string{"error": "WORKER_FENCE_MISMATCH"})
		case errors.Is(createErr, errAttemptConflict):
			writeJSON(response, http.StatusConflict, map[string]string{"error": "ATTEMPT_CONFLICT"})
		case errors.Is(createErr, errCapacity):
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "BOT_CAPACITY_EXHAUSTED"})
		case errors.Is(createErr, errManagerClosing):
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "WORKER_SHUTTING_DOWN"})
		case errors.Is(createErr, errStartupFailed), errors.Is(createErr, errStartupTimeout):
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "BOT_START_FAILED"})
		case createErr != nil:
			writeJSON(response, http.StatusInternalServerError, map[string]string{"error": "INTERNAL_ERROR"})
		default:
			writeJSON(response, http.StatusAccepted, map[string]string{"state": state})
		}
	})
	mux.HandleFunc("DELETE /internal/v1/attempts/{recordingId}", func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		if !authorizeControl(response, request, config, auth) {
			return
		}
		state, stopErr := manager.stop(request.PathValue("recordingId"))
		if errors.Is(stopErr, errStopTimeout) {
			writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "BOT_STOP_TIMEOUT"})
			return
		}
		writeJSON(response, http.StatusOK, map[string]string{"state": state})
	})
	if finalizer, ok := runner.(recording.AssignedFinalizationRunner); ok {
		mux.HandleFunc("PUT /internal/v1/finalizations/{recordingId}", func(
			response http.ResponseWriter,
			request *http.Request,
		) {
			if !authorizeControl(response, request, config, auth) {
				return
			}
			request.Body = http.MaxBytesReader(response, request.Body, maxAttemptBodyBytes)
			decoder := json.NewDecoder(request.Body)
			decoder.DisallowUnknownFields()
			var finalization recording.FinalizationRequest
			invalidBody := decoder.Decode(&finalization) != nil || ensureJSONEOF(decoder) != nil
			invalidRequest := validateFinalization(finalization) != nil ||
				finalization.RecordingID != request.PathValue("recordingId")
			if invalidBody || invalidRequest {
				writeJSON(response, http.StatusBadRequest, map[string]string{"error": "INVALID_PAYLOAD"})
				return
			}
			if finalization.WorkerID != config.WorkerID || finalization.WorkerGenerationID != config.GenerationID {
				writeJSON(response, http.StatusConflict, map[string]string{"error": "WORKER_FENCE_MISMATCH"})
				return
			}
			finalizeErr := manager.finalize(
				request.Context(),
				finalization,
				config.FinalizeTimeout,
				func(ctx context.Context) error {
					return finalizer.Finalize(ctx, finalization)
				},
			)
			if errors.Is(finalizeErr, errAttemptConflict) {
				writeJSON(response, http.StatusConflict, map[string]string{"error": "ATTEMPT_CONFLICT"})
				return
			}
			if errors.Is(finalizeErr, errManagerClosing) {
				writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "WORKER_SHUTTING_DOWN"})
				return
			}
			if finalizeErr != nil {
				writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "BOT_FINALIZATION_FAILED"})
				return
			}
			writeJSON(response, http.StatusAccepted, map[string]string{"state": "finalized"})
		})
	}
	return &ControlShim{handler: mux, manager: manager}, nil
}

func mergeSortedStrings(left, right []string) []string {
	values := map[string]struct{}{}
	for _, value := range append(append([]string{}, left...), right...) {
		values[value] = struct{}{}
	}
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func validateFinalization(request recording.FinalizationRequest) error {
	values := []string{
		request.AttemptID,
		request.RoomID,
		request.RecordingID,
		request.IntegrationID,
		request.WorkerID,
		request.WorkerGenerationID,
	}
	for _, value := range values {
		if value == "" || len(value) > 128 {
			return errors.New("invalid finalization identity")
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, request.RequestedAt); err != nil {
		return errors.New("invalid finalization requestedAt")
	}
	return nil
}

func authorizeControl(
	response http.ResponseWriter,
	request *http.Request,
	config ControlShimConfig,
	auth controlJWTConfig,
) bool {
	if auth.verify(request.Header.Get("Authorization"), time.Now()) != nil {
		writeJSON(response, http.StatusUnauthorized, map[string]string{"error": "UNAUTHORIZED"})
		return false
	}
	workerMatches := request.Header.Get("X-Recording-Worker-Id") == config.WorkerID
	generationMatches := request.Header.Get("X-Recording-Worker-Generation-Id") == config.GenerationID
	if !workerMatches || !generationMatches {
		writeJSON(response, http.StatusConflict, map[string]string{"error": "WORKER_FENCE_MISMATCH"})
		return false
	}
	return true
}

func decodeAttempt(response http.ResponseWriter, request *http.Request) (recording.AttemptRequest, error) {
	request.Body = http.MaxBytesReader(response, request.Body, maxAttemptBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var attempt recording.AttemptRequest
	if err := decoder.Decode(&attempt); err != nil {
		return recording.AttemptRequest{}, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return recording.AttemptRequest{}, err
	}
	if err := validateAttempt(attempt); err != nil {
		return recording.AttemptRequest{}, err
	}
	return attempt, nil
}

func validateAttempt(attempt recording.AttemptRequest) error {
	boundedValues := []string{
		attempt.AttemptID,
		attempt.RoomID,
		attempt.RecordingID,
		attempt.IntegrationID,
		attempt.WorkerID,
		attempt.WorkerGenerationID,
	}
	for _, value := range boundedValues {
		if value == "" || len(value) > 128 {
			return errors.New("invalid attempt identity")
		}
	}
	if attempt.ProtocolVersion != signaling.ProtocolVersion && attempt.ProtocolVersion != recording.CaptureProtocolVersion {
		return errors.New("invalid protocol version")
	}
	if attempt.ProtocolVersion == recording.CaptureProtocolVersion {
		if attempt.CaptureGeneration <= 0 {
			return errors.New("invalid capture generation")
		}
		if _, err := time.Parse(time.RFC3339Nano, attempt.CaptureStartedAt); err != nil {
			return errors.New("invalid captureStartedAt")
		}
	} else if attempt.CaptureGeneration != 0 || attempt.CaptureStartedAt != "" {
		return errors.New("v2 capture fields are not valid for v1")
	}
	if attempt.Admission == "" || len(attempt.Admission) > 8192 {
		return errors.New("invalid admission")
	}
	if len(attempt.RequestedAt) == 0 || attempt.RequestedAt[len(attempt.RequestedAt)-1] != 'Z' {
		return errors.New("invalid requestedAt")
	}
	if _, err := time.Parse(time.RFC3339Nano, attempt.RequestedAt); err != nil {
		return errors.New("invalid requestedAt")
	}
	parsed, err := url.Parse(attempt.SignalingURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
		return errors.New("invalid signaling URL")
	}
	return nil
}

func validateControlConfig(config ControlShimConfig) error {
	if config.WorkerID == "" || len(config.WorkerID) > 128 {
		return errors.New("invalid worker ID")
	}
	if config.GenerationID == "" || len(config.GenerationID) > 128 {
		return errors.New("invalid worker generation ID")
	}
	if len(config.JWTSecret) < 32 {
		return errors.New("control JWT secret must contain at least 32 bytes")
	}
	if config.StartTimeout <= 0 || config.StartTimeout > 120*time.Second {
		return errors.New("start timeout must be greater than zero and at most 120 seconds")
	}
	if config.StopTimeout <= 0 || config.StopTimeout > 30*time.Second {
		return errors.New("stop timeout must be greater than zero and at most 30 seconds")
	}
	if config.FinalizeTimeout <= 0 || config.FinalizeTimeout > 300*time.Second {
		return errors.New("finalization timeout must be greater than zero and at most 300 seconds")
	}
	if config.MaxSlots < 1 || config.MaxSlots > 32 {
		return errors.New("max slots must be between 1 and 32")
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.Header().Set("Cache-Control", "no-store")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
