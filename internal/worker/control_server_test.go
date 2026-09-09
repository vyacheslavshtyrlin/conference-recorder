package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const testControlSecret = "control-secret-that-is-at-least-32-bytes"

func TestControlShimReadinessMatchesWorkerDiscovery(t *testing.T) {
	server, runner := newControlTestServer(t)
	liveResponse, err := http.Get(server.URL + "/health/live")
	if err != nil {
		t.Fatal(err)
	}
	liveResponse.Body.Close()
	if liveResponse.StatusCode != http.StatusOK {
		t.Fatalf("liveness status=%d", liveResponse.StatusCode)
	}
	response, err := getReady(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var rawReady map[string]json.RawMessage
	if err := json.NewDecoder(response.Body).Decode(&rawReady); err != nil {
		t.Fatal(err)
	}
	expectedKeys := []string{
		"status",
		"workerId",
		"generationId",
		"capacity",
		"occupiedSlots",
		"attempts",
		"recoverableRecordingIds",
		"pendingFinalizationRecordingIds",
	}
	if len(rawReady) != len(expectedKeys) {
		t.Fatalf("readiness keys=%v", rawReady)
	}
	for _, key := range expectedKeys {
		if _, exists := rawReady[key]; !exists {
			t.Fatalf("readiness is missing %q", key)
		}
	}
	encodedReady, err := json.Marshal(rawReady)
	if err != nil {
		t.Fatal(err)
	}
	var ready struct {
		Status                          string            `json:"status"`
		WorkerID                        string            `json:"workerId"`
		GenerationID                    string            `json:"generationId"`
		Capacity                        int               `json:"capacity"`
		OccupiedSlots                   int               `json:"occupiedSlots"`
		Attempts                        []AttemptSnapshot `json:"attempts"`
		RecoverableRecordingIDs         []string          `json:"recoverableRecordingIds"`
		PendingFinalizationRecordingIDs []string          `json:"pendingFinalizationRecordingIds"`
	}
	if err := json.Unmarshal(encodedReady, &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Status != "ready" || ready.WorkerID != "worker-a" || ready.GenerationID != "generation-a" {
		t.Fatalf("unexpected worker identity: %+v", ready)
	}
	if ready.Capacity != 1 || ready.OccupiedSlots != 0 || len(ready.Attempts) != 0 {
		t.Fatalf("unexpected empty capacity: %+v", ready)
	}
	if ready.RecoverableRecordingIDs == nil || ready.PendingFinalizationRecordingIDs == nil {
		t.Fatal("worker discovery arrays must encode as []")
	}
	if runner.starts() != 0 {
		t.Fatal("readiness started a probe")
	}
}

func TestProductionControlReadinessFailsClosedAndAdvertisesOnlyWhenReady(t *testing.T) {
	readyErr := errors.New("ffmpeg missing")
	shim, err := NewProductionControlHandler(ControlShimConfig{
		WorkerID: "worker-a", GenerationID: "generation-a", JWTSecret: testControlSecret,
		StartTimeout: time.Second, StopTimeout: time.Second,
	}, immediateExitRunner{}, func() error { return readyErr })
	if err != nil {
		t.Fatal(err)
	}
	defer shim.Close()
	server := httptest.NewServer(shim)
	defer server.Close()
	response, err := getReady(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Status    string `json:"status"`
		Capacity  int    `json:"capacity"`
		Accepting bool   `json:"acceptingAttempts"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || payload.Status != "not_ready" || payload.Capacity != 0 || payload.Accepting {
		t.Fatalf("readiness was not fail-closed: status=%d payload=%+v", response.StatusCode, payload)
	}
	readyErr = nil
	response, err = getReady(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	payload = struct {
		Status    string `json:"status"`
		Capacity  int    `json:"capacity"`
		Accepting bool   `json:"acceptingAttempts"`
	}{}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || payload.Status != "ready" || payload.Capacity != 1 || !payload.Accepting {
		t.Fatalf("readiness did not advertise ready: status=%d payload=%+v", response.StatusCode, payload)
	}
}

func TestProductionControlReadinessDoesNotReportActiveCaptureForRecovery(t *testing.T) {
	runner := &recoveryHealthBlockingRunner{
		blockingProbeRunner: &blockingProbeRunner{started: make(chan struct{}, 1)},
		recoverable:         []string{"rec-active", "rec-stale"},
		finalizations:       []string{"rec-active", "rec-finalization"},
	}
	shim, err := NewProductionControlHandler(ControlShimConfig{
		WorkerID: "worker-a", GenerationID: "generation-a", JWTSecret: testControlSecret,
		StartTimeout: time.Second, StopTimeout: time.Second,
	}, runner, func() error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer shim.Close()
	server := httptest.NewServer(shim)
	defer server.Close()

	token := testControlToken(time.Now())
	body := testAttemptBody("rec-active", "attempt-active", "one-use-admission-secret")
	if status := putAttempt(t, server.URL, body, token, "worker-a", "generation-a"); status != http.StatusAccepted {
		t.Fatalf("create status=%d", status)
	}
	waitFor(t, runner.started)

	response, err := getReady(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var ready struct {
		RecoverableRecordingIDs         []string `json:"recoverableRecordingIds"`
		PendingFinalizationRecordingIDs []string `json:"pendingFinalizationRecordingIds"`
	}
	if err := json.NewDecoder(response.Body).Decode(&ready); err != nil {
		t.Fatal(err)
	}
	if strings.Join(ready.RecoverableRecordingIDs, ",") != "rec-stale" {
		t.Fatalf("active capture leaked into recovery report: %v", ready.RecoverableRecordingIDs)
	}
	if strings.Join(ready.PendingFinalizationRecordingIDs, ",") != "rec-finalization" {
		t.Fatalf("active capture leaked into finalization report: %v", ready.PendingFinalizationRecordingIDs)
	}
}

func TestControlShimAuthFenceIdempotencyCapacityAndStop(t *testing.T) {
	server, runner := newControlTestServer(t)
	body := testAttemptBody("rec-1", "attempt-1", "one-use-admission-secret")
	if status := putAttempt(t, server.URL, body, "", "worker-a", "generation-a"); status != 401 {
		t.Fatalf("unauthenticated status=%d", status)
	}
	token := testControlToken(time.Now())
	if status := putAttempt(t, server.URL, body, token, "worker-a", "stale"); status != 409 {
		t.Fatalf("stale fence status=%d", status)
	}
	bodyFenceMismatch := testAttemptBody("rec-1", "attempt-1", "admission")
	bodyFenceMismatch["workerGenerationId"] = "stale"
	if status := putAttempt(
		t,
		server.URL,
		bodyFenceMismatch,
		token,
		"worker-a",
		"generation-a",
	); status != 409 {
		t.Fatalf("body fence status=%d", status)
	}
	if status := putAttempt(t, server.URL, body, token, "worker-a", "generation-a"); status != 202 {
		t.Fatalf("create status=%d", status)
	}
	waitFor(t, runner.started)
	if status := putAttempt(t, server.URL, body, token, "worker-a", "generation-a"); status != 202 {
		t.Fatalf("idempotent retry status=%d", status)
	}
	if runner.starts() != 1 {
		t.Fatalf("idempotent retry started %d probes", runner.starts())
	}
	conflict := testAttemptBody("rec-1", "attempt-2", "different-secret")
	if status := putAttempt(t, server.URL, conflict, token, "worker-a", "generation-a"); status != 409 {
		t.Fatalf("conflict status=%d", status)
	}
	second := testAttemptBody("rec-2", "attempt-2", "second-secret")
	if status := putAttempt(t, server.URL, second, token, "worker-a", "generation-a"); status != 503 {
		t.Fatalf("capacity status=%d", status)
	}

	request, err := http.NewRequest(http.MethodDelete, server.URL+"/internal/v1/attempts/rec-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 409 {
		t.Fatalf("unfenced stop status=%d", response.StatusCode)
	}
	if status := deleteAttempt(t, server.URL, "rec-1", token); status != 200 {
		t.Fatalf("stop status=%d", status)
	}
	if status := deleteAttempt(t, server.URL, "rec-1", token); status != 200 {
		t.Fatalf("idempotent stop status=%d", status)
	}
	if runner.stops() != 1 {
		t.Fatalf("stop canceled runner %d times", runner.stops())
	}
}

func TestControlShimNeverEchoesAdmission(t *testing.T) {
	server, runner := newControlTestServer(t)
	const admission = "raw-one-use-admission-must-not-escape"
	token := testControlToken(time.Now())
	body := testAttemptBody("rec-secret", "attempt-secret", admission)
	responseBody := putAttemptBody(t, server.URL, body, token, "worker-a", "generation-a")
	waitFor(t, runner.started)
	readyResponse, err := getReady(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	readyBody := new(bytes.Buffer)
	_, _ = readyBody.ReadFrom(readyResponse.Body)
	readyResponse.Body.Close()
	if strings.Contains(responseBody, admission) || strings.Contains(readyBody.String(), admission) {
		t.Fatal("control response exposed the one-use admission")
	}
	if runner.admission() != admission {
		t.Fatal("runner did not receive the request-body admission")
	}
	var ready struct {
		OccupiedSlots int               `json:"occupiedSlots"`
		Attempts      []AttemptSnapshot `json:"attempts"`
	}
	if err := json.Unmarshal(readyBody.Bytes(), &ready); err != nil {
		t.Fatal(err)
	}
	if ready.OccupiedSlots != 1 || len(ready.Attempts) != 1 {
		t.Fatalf("unexpected occupied readiness: %+v", ready)
	}
	evidence := ready.Attempts[0].Evidence
	if !evidence.Complete || len(evidence.ObservedSources) != 3 || len(evidence.Codecs) != 2 {
		t.Fatalf("unexpected probe evidence: %+v", evidence)
	}
	deleteAttempt(t, server.URL, "rec-secret", token)
}

func TestControlShimStopIsBounded(t *testing.T) {
	runner := &delayedStopRunner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	shim, err := NewControlShimHandler(ControlShimConfig{
		WorkerID:     "worker-a",
		GenerationID: "generation-a",
		JWTSecret:    testControlSecret,
		StartTimeout: time.Second,
		StopTimeout:  20 * time.Millisecond,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(shim)
	t.Cleanup(func() {
		if runner.release != nil {
			close(runner.release)
		}
		shim.Close()
		server.Close()
	})
	token := testControlToken(time.Now())
	body := testAttemptBody("rec-bounded", "attempt-bounded", "admission")
	if status := putAttempt(t, server.URL, body, token, "worker-a", "generation-a"); status != 202 {
		t.Fatalf("create status=%d", status)
	}
	waitFor(t, runner.started)
	startedAt := time.Now()
	responseBody, status := doDeleteAttempt(t, server.URL, "rec-bounded", token)
	if status != 503 || !strings.Contains(responseBody, `"error":"BOT_STOP_TIMEOUT"`) {
		t.Fatalf("bounded stop status=%d body=%s", status, responseBody)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("bounded stop took %s", elapsed)
	}
	close(runner.release)
	runner.release = nil
	waitForEmptyReadiness(t, server.URL)
}

func TestControlShimStopReportsRunnerFinalizationFailure(t *testing.T) {
	runner := &failedStopRunner{started: make(chan struct{})}
	server, _ := newCustomControlTestServer(t, runner, time.Second, time.Second)
	token := testControlToken(time.Now())
	if status := putAttempt(
		t,
		server.URL,
		testAttemptBody("rec-failed-stop", "attempt-failed-stop", "admission"),
		token,
		"worker-a",
		"generation-a",
	); status != http.StatusAccepted {
		t.Fatalf("create status=%d", status)
	}
	waitFor(t, runner.started)

	responseBody, status := doDeleteAttempt(t, server.URL, "rec-failed-stop", token)
	if status != http.StatusServiceUnavailable ||
		!strings.Contains(responseBody, `"error":"BOT_FINALIZATION_FAILED"`) {
		t.Fatalf("failed stop status=%d body=%s", status, responseBody)
	}
	ready := waitForFailedReadiness(t, server.URL)
	if len(ready.Attempts) != 1 || ready.Attempts[0].FailureCode != "BOT_FINALIZATION_FAILED" {
		t.Fatalf("failed stop readiness=%+v", ready)
	}
}

func TestControlShimRejectsExpiredJWTAndInvalidPayload(t *testing.T) {
	server, _ := newControlTestServer(t)
	body := testAttemptBody("rec-1", "attempt-1", "admission")
	expired := testControlToken(time.Now().Add(-10 * time.Minute))
	if status := putAttempt(t, server.URL, body, expired, "worker-a", "generation-a"); status != 401 {
		t.Fatalf("expired token status=%d", status)
	}
	body["unexpected"] = true
	if status := putAttempt(
		t,
		server.URL,
		body,
		testControlToken(time.Now()),
		"worker-a",
		"generation-a",
	); status != 400 {
		t.Fatalf("unknown field status=%d", status)
	}
	delete(body, "unexpected")
	body["requestedAt"] = "2026-08-25T15:00:00+05:00"
	if status := putAttempt(
		t,
		server.URL,
		body,
		testControlToken(time.Now()),
		"worker-a",
		"generation-a",
	); status != 400 {
		t.Fatalf("offset requestedAt status=%d", status)
	}
}

func TestControlShimPutWaitsForStartupReadiness(t *testing.T) {
	runner := &startupProbeRunner{
		started: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	server, shim := newCustomControlTestServer(t, runner, time.Second, time.Second)
	token := testControlToken(time.Now())
	response := make(chan int, 1)
	go func() {
		response <- putAttempt(
			t,
			server.URL,
			testAttemptBody("rec-ready", "attempt-ready", "admission"),
			token,
			"worker-a",
			"generation-a",
		)
	}()
	waitFor(t, runner.started)
	select {
	case status := <-response:
		t.Fatalf("PUT returned before startup readiness with status=%d", status)
	case <-time.After(40 * time.Millisecond):
	}
	close(runner.proceed)
	if status := <-response; status != 202 {
		t.Fatalf("ready PUT status=%d", status)
	}
	shim.Close()
}

func TestControlShimStartupFailureAndTimeoutFreeSlot(t *testing.T) {
	tests := []struct {
		name         string
		runner       AssignedProbeRunner
		startTimeout time.Duration
	}{
		{
			name:         "invalid admission dial failure",
			runner:       &retryProbeRunner{firstFailure: true},
			startTimeout: time.Second,
		},
		{
			name:         "startup timeout",
			runner:       &retryProbeRunner{firstTimeout: true},
			startTimeout: 20 * time.Millisecond,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, _ := newCustomControlTestServer(
				t,
				test.runner,
				test.startTimeout,
				time.Second,
			)
			const admission = "invalid-one-use-admission"
			body, status := doPutAttempt(
				t,
				server.URL,
				testAttemptBody("rec-failed", "attempt-failed", admission),
				testControlToken(time.Now()),
				"worker-a",
				"generation-a",
			)
			if status != 503 || !strings.Contains(body, `"error":"BOT_START_FAILED"`) {
				t.Fatalf("startup failure status=%d body=%s", status, body)
			}
			if strings.Contains(body, admission) {
				t.Fatal("startup failure exposed admission")
			}
			waitForEmptyReadiness(t, server.URL)
			success := testAttemptBody("rec-after-failure", "attempt-after-failure", "valid-admission")
			if status := putAttempt(
				t,
				server.URL,
				success,
				testControlToken(time.Now()),
				"worker-a",
				"generation-a",
			); status != 202 {
				t.Fatalf("reused slot status=%d", status)
			}
			deleteAttempt(t, server.URL, "rec-after-failure", testControlToken(time.Now()))
		})
	}
}

func TestControlShimRetainsFailedV2StartupForExactRetryAndHigherGeneration(t *testing.T) {
	runner := &retryProbeRunner{firstFailure: true}
	server, _ := newCustomControlTestServer(t, runner, time.Second, time.Second)
	token := testControlToken(time.Now())
	failed := testAttemptBody("rec-v2-startup", "attempt-v2-1", "one-use-admission")
	failed["protocolVersion"] = CaptureProtocolVersion
	failed["captureGeneration"] = 1
	failed["captureStartedAt"] = "2026-08-25T10:00:00.000Z"

	firstBody, firstStatus := doPutAttempt(
		t, server.URL, failed, token, "worker-a", "generation-a",
	)
	if firstStatus != 503 || !strings.Contains(firstBody, `"error":"BOT_START_FAILED"`) {
		t.Fatalf("initial startup failure status=%d body=%s", firstStatus, firstBody)
	}
	retryBody, retryStatus := doPutAttempt(
		t, server.URL, failed, token, "worker-a", "generation-a",
	)
	if retryStatus != firstStatus || retryBody != firstBody {
		t.Fatalf("exact retry changed failure: first=%d %s retry=%d %s", firstStatus, firstBody, retryStatus, retryBody)
	}
	runner.mu.Lock()
	callsAfterRetry := runner.calls
	runner.mu.Unlock()
	if callsAfterRetry != 1 {
		t.Fatalf("exact retry reran one-use admission: calls=%d", callsAfterRetry)
	}

	conflicting := testAttemptBody("rec-v2-startup", "attempt-v2-2", "replacement-admission")
	conflicting["protocolVersion"] = CaptureProtocolVersion
	conflicting["captureGeneration"] = 2
	conflicting["captureStartedAt"] = failed["captureStartedAt"]
	conflicting["integrationId"] = "integration-other"
	conflictBody, conflictStatus := doPutAttempt(
		t, server.URL, conflicting, token, "worker-a", "generation-a",
	)
	if conflictStatus != 409 || !strings.Contains(conflictBody, `"error":"ATTEMPT_CONFLICT"`) {
		t.Fatalf("changed assignment identity status=%d body=%s", conflictStatus, conflictBody)
	}

	replacement := testAttemptBody("rec-v2-startup", "attempt-v2-2", "replacement-admission")
	replacement["protocolVersion"] = CaptureProtocolVersion
	replacement["captureGeneration"] = 2
	replacement["captureStartedAt"] = failed["captureStartedAt"]
	if status := putAttempt(
		t, server.URL, replacement, token, "worker-a", "generation-a",
	); status != 202 {
		t.Fatalf("higher generation replacement status=%d", status)
	}
	deleteAttempt(t, server.URL, "rec-v2-startup", token)
}

func TestControlShimFullRequestIdempotency(t *testing.T) {
	server, _ := newControlTestServer(t)
	token := testControlToken(time.Now())
	original := testAttemptBody("rec-idempotent", "attempt-idempotent", "admission-a")
	if status := putAttempt(t, server.URL, original, token, "worker-a", "generation-a"); status != 202 {
		t.Fatalf("create status=%d", status)
	}
	mutations := map[string]any{
		"roomId":        "room-other",
		"integrationId": "integration-other",
		"requestedAt":   "2026-08-25T10:00:01.000Z",
		"signalingUrl":  "ws://other:4000/internal/recording-bot/v1",
		"admission":     "admission-b",
	}
	for field, value := range mutations {
		changed := testAttemptBody("rec-idempotent", "attempt-idempotent", "admission-a")
		changed[field] = value
		body, status := doPutAttempt(t, server.URL, changed, token, "worker-a", "generation-a")
		if status != 409 || !strings.Contains(body, `"error":"ATTEMPT_CONFLICT"`) {
			t.Fatalf("changed %s status=%d body=%s", field, status, body)
		}
	}
}

func TestControlShimClassifiesChangedV2BindingAsWorkerFenceMismatch(t *testing.T) {
	server, _ := newControlTestServer(t)
	token := testControlToken(time.Now())
	original := testAttemptBody("rec-v2-fence", "attempt-v2-fence", "admission")
	original["protocolVersion"] = CaptureProtocolVersion
	original["captureGeneration"] = 7
	original["captureStartedAt"] = "2026-08-25T10:00:00.000Z"
	if status := putAttempt(t, server.URL, original, token, "worker-a", "generation-a"); status != 202 {
		t.Fatalf("create status=%d", status)
	}

	for field, value := range map[string]any{
		"captureGeneration": 8,
		"captureStartedAt":  "2026-08-25T10:00:01.000Z",
	} {
		changed := testAttemptBody("rec-v2-fence", "attempt-v2-fence", "admission")
		changed["protocolVersion"] = CaptureProtocolVersion
		changed["captureGeneration"] = 7
		changed["captureStartedAt"] = "2026-08-25T10:00:00.000Z"
		changed[field] = value
		body, status := doPutAttempt(t, server.URL, changed, token, "worker-a", "generation-a")
		if status != 409 || !strings.Contains(body, `"error":"WORKER_FENCE_MISMATCH"`) {
			t.Fatalf("changed %s status=%d body=%s", field, status, body)
		}
	}

	deleteAttempt(t, server.URL, "rec-v2-fence", token)
}

func TestFinalizationRetriesShareOneOperationAndPendingLifetime(t *testing.T) {
	manager := newControlAttemptManager(immediateExitRunner{}, time.Second, time.Second, 1)
	request := testFinalizationRequest("rec-finalizing", "attempt-finalizing")
	release := make(chan struct{})
	started := make(chan struct{})
	expected := errors.New("finalization failed")
	runs := 0
	run := func(ctx context.Context) error {
		runs++
		close(started)
		select {
		case <-release:
			return expected
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	first, err := manager.startFinalization(request, time.Second, run)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	unexpectedRun := make(chan struct{}, 1)
	second, err := manager.startFinalization(request, time.Second, func(context.Context) error {
		unexpectedRun <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != second || runs != 1 {
		t.Fatalf("finalization did not converge: same=%t runs=%d", first == second, runs)
	}
	select {
	case <-unexpectedRun:
		t.Fatal("retry started a second finalization")
	default:
	}
	if pending := manager.finalizations(); len(pending) != 1 || pending[0] != "rec-finalizing" {
		t.Fatalf("pending finalizations=%v", pending)
	}
	conflict := request
	conflict.AttemptID = "attempt-conflicting"
	if _, err := manager.startFinalization(conflict, time.Second, func(context.Context) error {
		unexpectedRun <- struct{}{}
		return nil
	}); !errors.Is(err, errAttemptConflict) {
		t.Fatalf("conflicting finalization error=%v", err)
	}

	close(release)
	<-first.done
	if !errors.Is(first.err, expected) || !errors.Is(second.err, expected) {
		t.Fatalf("shared finalization result: first=%v second=%v", first.err, second.err)
	}
	if pending := manager.finalizations(); len(pending) != 0 {
		t.Fatalf("completed finalization remains pending: %v", pending)
	}
}

func TestAttemptsAndFinalizationsForSameRecordingAreMutuallyExclusive(t *testing.T) {
	t.Run("active attempt rejects finalization", func(t *testing.T) {
		runner := &blockingProbeRunner{started: make(chan struct{}, 1)}
		manager := newControlAttemptManager(runner, time.Second, time.Second, 1)
		if _, err := manager.create(AttemptRequest{
			AttemptID: "attempt-active", RoomID: "room-1", RecordingID: "rec-exclusive",
			IntegrationID: "integration-1", RequestedAt: "2026-08-25T10:00:00.000Z",
			ProtocolVersion: ProtocolVersion, SignalingURL: "ws://signaling:4000",
			Admission: "admission", WorkerID: "worker-a", WorkerGenerationID: "generation-a",
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.startFinalization(
			testFinalizationRequest("rec-exclusive", "attempt-active"),
			time.Second,
			func(context.Context) error { return nil },
		); !errors.Is(err, errAttemptConflict) {
			t.Fatalf("finalization during active attempt error=%v", err)
		}
		if _, err := manager.stop("rec-exclusive"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("pending finalization rejects attempt", func(t *testing.T) {
		manager := newControlAttemptManager(immediateExitRunner{}, time.Second, time.Second, 1)
		release := make(chan struct{})
		operation, err := manager.startFinalization(
			testFinalizationRequest("rec-exclusive", "attempt-finalizing"),
			time.Second,
			func(context.Context) error {
				<-release
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := manager.create(AttemptRequest{RecordingID: "rec-exclusive"}); !errors.Is(err, errAttemptConflict) {
			t.Fatalf("attempt during pending finalization error=%v", err)
		}
		close(release)
		<-operation.done
	})
}

func TestControlShimRejectsConflictingPendingFinalization(t *testing.T) {
	runner := &blockingFinalizationRunner{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	shim, err := NewControlShimHandler(ControlShimConfig{
		WorkerID: "worker-a", GenerationID: "generation-a", JWTSecret: testControlSecret,
		StartTimeout: time.Second, StopTimeout: time.Second, FinalizeTimeout: time.Second,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(shim)
	t.Cleanup(func() {
		shim.Close()
		server.Close()
	})

	token := testControlToken(time.Now())
	original := testFinalizationRequest("rec-finalizing", "attempt-finalizing")
	firstResult := make(chan int, 1)
	go func() {
		_, status := doPutFinalization(t, server.URL, original, token)
		firstResult <- status
	}()
	waitFor(t, runner.started)
	conflict := original
	conflict.RoomID = "room-other"
	body, status := doPutFinalization(t, server.URL, conflict, token)
	if status != http.StatusConflict || !strings.Contains(body, `"error":"ATTEMPT_CONFLICT"`) {
		t.Fatalf("conflicting finalization status=%d body=%s", status, body)
	}
	close(runner.release)
	if status := <-firstResult; status != http.StatusAccepted {
		t.Fatalf("original finalization status=%d", status)
	}
}

func TestControlShimRejectsNilRunner(t *testing.T) {
	_, err := NewControlShimHandler(ControlShimConfig{
		WorkerID: "worker-a", GenerationID: "generation-a", JWTSecret: testControlSecret,
		StartTimeout: time.Second, StopTimeout: time.Second,
	}, nil)
	if err == nil {
		t.Fatal("nil runner was accepted")
	}
}

func TestControlManagerShutdownCancelsAndWaitsForFinalization(t *testing.T) {
	manager := newControlAttemptManager(immediateExitRunner{}, time.Second, time.Second, 1)
	started := make(chan struct{})
	operation, err := manager.startFinalization(
		testFinalizationRequest("rec-shutdown-finalization", "attempt-finalization"),
		time.Minute,
		func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := manager.close(ctx); err != nil {
		t.Fatal(err)
	}
	<-operation.done
	if !errors.Is(operation.err, context.Canceled) {
		t.Fatalf("finalization shutdown result=%v", operation.err)
	}
	if _, err := manager.startFinalization(testFinalizationRequest("rec-after-close", "attempt-after-close"), time.Second, func(context.Context) error {
		return nil
	}); !errors.Is(err, errManagerClosing) {
		t.Fatalf("post-close finalization error=%v", err)
	}
}

func TestControlShimCloseFencesConcurrentCreate(t *testing.T) {
	runner := &startupProbeRunner{
		started: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	server, shim := newCustomControlTestServer(t, runner, time.Second, time.Second)
	token := testControlToken(time.Now())
	firstResponse := make(chan int, 1)
	go func() {
		firstResponse <- putAttempt(
			t,
			server.URL,
			testAttemptBody("rec-closing", "attempt-closing", "admission"),
			token,
			"worker-a",
			"generation-a",
		)
	}()
	waitFor(t, runner.started)
	shim.Close()
	if status := <-firstResponse; status != 503 {
		t.Fatalf("in-flight create status=%d", status)
	}
	body, status := doPutAttempt(
		t,
		server.URL,
		testAttemptBody("rec-after-close", "attempt-after-close", "admission"),
		token,
		"worker-a",
		"generation-a",
	)
	if status != 503 || !strings.Contains(body, `"error":"WORKER_SHUTTING_DOWN"`) {
		t.Fatalf("post-close create status=%d body=%s", status, body)
	}
}

func TestControlShimFailsClosedWhenReadyRunnerExits(t *testing.T) {
	server, _ := newCustomControlTestServer(
		t,
		immediateExitRunner{},
		time.Second,
		time.Second,
	)
	token := testControlToken(time.Now())
	status := putAttempt(
		t,
		server.URL,
		testAttemptBody("rec-exit", "attempt-exit", "admission"),
		token,
		"worker-a",
		"generation-a",
	)
	if status != http.StatusAccepted && status != http.StatusServiceUnavailable {
		t.Fatalf("initially ready status=%d", status)
	}
	ready := waitForFailedReadiness(t, server.URL)
	if ready.OccupiedSlots != 0 || len(ready.Attempts) != 1 {
		t.Fatalf("unexpected exited readiness: %+v", ready)
	}
	if ready.Attempts[0].State != "failed" ||
		ready.Attempts[0].FailureCode != "BOT_RUNTIME_EXITED" {
		t.Fatalf("runner exit did not fail closed: %+v", ready.Attempts[0])
	}
	if status := deleteAttempt(t, server.URL, "rec-exit", token); status != http.StatusOK {
		t.Fatalf("failed attempt cleanup status=%d", status)
	}
	waitForEmptyReadiness(t, server.URL)
}

func TestBotFailureCodeUsesOnlyDocumentedStableSet(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: ErrAdmissionRejected, want: "BOT_ADMISSION_REJECTED"},
		{err: ErrConnectTimeout, want: "BOT_CONNECT_TIMEOUT"},
		{err: ErrRevisionGap, want: "BOT_SIGNALING_GAP"},
		{err: ErrRevisionCapacity, want: "BOT_SIGNALING_GAP"},
		{err: ErrCaptureIncomplete, want: "BOT_CAPTURE_INCOMPLETE"},
		{err: ErrCaptureMaximumDuration, want: "BOT_CAPTURE_INCOMPLETE"},
		{err: ErrFinalizationFailed, want: "BOT_FINALIZATION_FAILED"},
		{err: errors.New("unexpected"), want: "BOT_RUNTIME_EXITED"},
		{
			err:  &CaptureAPIError{Status: 409, Code: "STALE_CAPTURE_GENERATION"},
			want: "BOT_CAPTURE_INCOMPLETE",
		},
	}
	allowed := map[string]bool{
		"BOT_ADMISSION_REJECTED":  true,
		"BOT_CONNECT_TIMEOUT":     true,
		"BOT_RUNTIME_EXITED":      true,
		"BOT_SIGNALING_GAP":       true,
		"BOT_CAPTURE_INCOMPLETE":  true,
		"BOT_FINALIZATION_FAILED": true,
	}
	for _, test := range tests {
		got := botFailureCode(test.err)
		if got != test.want || !allowed[got] {
			t.Fatalf("failure %v mapped to %q, want %q", test.err, got, test.want)
		}
	}
}

type blockingProbeRunner struct {
	started chan struct{}
	mu      sync.Mutex
	count   int
	stopped int
	secret  string
}

type recoveryHealthBlockingRunner struct {
	*blockingProbeRunner
	recoverable   []string
	finalizations []string
}

func (runner *recoveryHealthBlockingRunner) RecoveryHealth() ([]string, []string, error) {
	return append([]string{}, runner.recoverable...), append([]string{}, runner.finalizations...), nil
}

type delayedStopRunner struct {
	started chan struct{}
	release chan struct{}
}

type failedStopRunner struct {
	started chan struct{}
}

type startupProbeRunner struct {
	started chan struct{}
	proceed chan struct{}
	failure error
}

type retryProbeRunner struct {
	mu           sync.Mutex
	calls        int
	firstFailure bool
	firstTimeout bool
}

type immediateExitRunner struct{}

type blockingFinalizationRunner struct {
	started chan struct{}
	release chan struct{}
}

func (runner *blockingFinalizationRunner) Run(
	ctx context.Context,
	_ AttemptRequest,
	ready func(),
	_ func(ProbeEvidence),
) error {
	ready()
	<-ctx.Done()
	return nil
}

func (runner *blockingFinalizationRunner) Finalize(
	ctx context.Context,
	_ FinalizationRequest,
) error {
	close(runner.started)
	select {
	case <-runner.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (immediateExitRunner) Run(
	_ context.Context,
	_ AttemptRequest,
	ready func(),
	_ func(ProbeEvidence),
) error {
	ready()
	return nil
}

func (runner *retryProbeRunner) Run(
	ctx context.Context,
	_ AttemptRequest,
	ready func(),
	_ func(ProbeEvidence),
) error {
	runner.mu.Lock()
	runner.calls++
	call := runner.calls
	runner.mu.Unlock()
	if call == 1 && runner.firstFailure {
		return errors.New("dial rejected invalid secret")
	}
	if call == 1 && runner.firstTimeout {
		<-ctx.Done()
		return ctx.Err()
	}
	ready()
	<-ctx.Done()
	return nil
}

func (runner *startupProbeRunner) Run(
	ctx context.Context,
	_ AttemptRequest,
	ready func(),
	_ func(ProbeEvidence),
) error {
	close(runner.started)
	if runner.failure != nil {
		return runner.failure
	}
	select {
	case <-runner.proceed:
		ready()
		<-ctx.Done()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (runner *delayedStopRunner) Run(
	_ context.Context,
	_ AttemptRequest,
	ready func(),
	_ func(ProbeEvidence),
) error {
	ready()
	close(runner.started)
	<-runner.release
	return nil
}

func (runner *failedStopRunner) Run(
	ctx context.Context,
	_ AttemptRequest,
	ready func(),
	_ func(ProbeEvidence),
) error {
	ready()
	close(runner.started)
	<-ctx.Done()
	return ErrFinalizationFailed
}

func (runner *blockingProbeRunner) Run(
	ctx context.Context,
	request AttemptRequest,
	ready func(),
	report func(ProbeEvidence),
) error {
	runner.mu.Lock()
	runner.count++
	runner.secret = request.Admission
	runner.mu.Unlock()
	ready()
	report(ProbeEvidence{
		ObservedSources: []Source{SourceMic, SourceCamera, SourceScreen},
		Codecs:          []string{"audio/opus", "video/VP8"},
		Complete:        true,
	})
	select {
	case runner.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	runner.mu.Lock()
	runner.stopped++
	runner.mu.Unlock()
	return nil
}

func (runner *blockingProbeRunner) starts() int {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.count
}

func (runner *blockingProbeRunner) stops() int {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.stopped
}

func (runner *blockingProbeRunner) admission() string {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.secret
}

func newControlTestServer(t *testing.T) (*httptest.Server, *blockingProbeRunner) {
	t.Helper()
	runner := &blockingProbeRunner{started: make(chan struct{}, 1)}
	shim, err := NewControlShimHandler(ControlShimConfig{
		WorkerID:     "worker-a",
		GenerationID: "generation-a",
		JWTSecret:    testControlSecret,
		StartTimeout: time.Second,
		StopTimeout:  time.Second,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(shim)
	t.Cleanup(func() {
		shim.Close()
		server.Close()
	})
	return server, runner
}

func getReady(origin string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, origin+"/health/ready", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+testControlToken(time.Now()))
	return http.DefaultClient.Do(request)
}

func newCustomControlTestServer(
	t *testing.T,
	runner AssignedProbeRunner,
	startTimeout time.Duration,
	stopTimeout time.Duration,
) (*httptest.Server, *ControlShim) {
	t.Helper()
	shim, err := NewControlShimHandler(ControlShimConfig{
		WorkerID:     "worker-a",
		GenerationID: "generation-a",
		JWTSecret:    testControlSecret,
		StartTimeout: startTimeout,
		StopTimeout:  stopTimeout,
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(shim)
	t.Cleanup(func() {
		shim.Close()
		server.Close()
	})
	return server, shim
}

func testAttemptBody(recordingID, attemptID, admission string) map[string]any {
	return map[string]any{
		"attemptId":          attemptID,
		"roomId":             "room-1",
		"recordingId":        recordingID,
		"integrationId":      "integration-1",
		"requestedAt":        "2026-08-25T10:00:00.000Z",
		"protocolVersion":    ProtocolVersion,
		"signalingUrl":       "ws://signaling:4000/internal/recording-bot/v1",
		"admission":          admission,
		"workerId":           "worker-a",
		"workerGenerationId": "generation-a",
	}
}

func testFinalizationRequest(recordingID, attemptID string) FinalizationRequest {
	return FinalizationRequest{
		AttemptID:          attemptID,
		RoomID:             "room-1",
		RecordingID:        recordingID,
		IntegrationID:      "integration-1",
		RequestedAt:        "2026-08-25T10:00:00.000Z",
		WorkerID:           "worker-a",
		WorkerGenerationID: "generation-a",
	}
}

func putAttempt(
	t *testing.T,
	origin string,
	body map[string]any,
	token string,
	workerID string,
	generationID string,
) int {
	t.Helper()
	responseBody, status := doPutAttempt(t, origin, body, token, workerID, generationID)
	_ = responseBody
	return status
}

func putAttemptBody(
	t *testing.T,
	origin string,
	body map[string]any,
	token string,
	workerID string,
	generationID string,
) string {
	t.Helper()
	responseBody, _ := doPutAttempt(t, origin, body, token, workerID, generationID)
	return responseBody
}

func doPutAttempt(
	t *testing.T,
	origin string,
	body map[string]any,
	token string,
	workerID string,
	generationID string,
) (string, int) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recordingID := body["recordingId"].(string)
	request, err := http.NewRequest(
		http.MethodPut,
		origin+"/internal/v1/attempts/"+recordingID,
		bytes.NewReader(encoded),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Recording-Worker-Id", workerID)
	request.Header.Set("X-Recording-Worker-Generation-Id", generationID)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	result := new(bytes.Buffer)
	_, _ = result.ReadFrom(response.Body)
	return result.String(), response.StatusCode
}

func doPutFinalization(
	t *testing.T,
	origin string,
	body FinalizationRequest,
	token string,
) (string, int) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(
		http.MethodPut,
		origin+"/internal/v1/finalizations/"+body.RecordingID,
		bytes.NewReader(encoded),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Recording-Worker-Id", "worker-a")
	request.Header.Set("X-Recording-Worker-Generation-Id", "generation-a")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	result := new(bytes.Buffer)
	_, _ = result.ReadFrom(response.Body)
	return result.String(), response.StatusCode
}

func deleteAttempt(t *testing.T, origin, recordingID, token string) int {
	t.Helper()
	_, status := doDeleteAttempt(t, origin, recordingID, token)
	return status
}

func doDeleteAttempt(t *testing.T, origin, recordingID, token string) (string, int) {
	t.Helper()
	request, err := http.NewRequest(
		http.MethodDelete,
		origin+"/internal/v1/attempts/"+recordingID,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Recording-Worker-Id", "worker-a")
	request.Header.Set("X-Recording-Worker-Generation-Id", "generation-a")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	result := new(bytes.Buffer)
	_, _ = result.ReadFrom(response.Body)
	return result.String(), response.StatusCode
}

func testControlToken(issuedAt time.Time) string {
	header, _ := json.Marshal(map[string]any{
		"alg": "HS256",
		"typ": "recording-bot-control+jwt",
	})
	payload, _ := json.Marshal(map[string]any{
		"iss": "conference-signaling",
		"aud": "conference-recording-bot-control",
		"sub": "conference-signaling",
		"iat": issuedAt.Unix(),
		"exp": issuedAt.Add(5 * time.Minute).Unix(),
	})
	encodedHeader := base64.RawURLEncoding.EncodeToString(header)
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	signed := encodedHeader + "." + encodedPayload
	signature := hmac.New(sha256.New, []byte(testControlSecret))
	_, _ = signature.Write([]byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(signature.Sum(nil))
}

func waitFor(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for probe runner")
	}
}

func waitForEmptyReadiness(t *testing.T, origin string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := getReady(origin)
		if err != nil {
			t.Fatal(err)
		}
		var ready struct {
			OccupiedSlots int               `json:"occupiedSlots"`
			Attempts      []AttemptSnapshot `json:"attempts"`
		}
		err = json.NewDecoder(response.Body).Decode(&ready)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if ready.OccupiedSlots == 0 && len(ready.Attempts) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for empty worker slot")
}

func waitForFailedReadiness(t *testing.T, origin string) struct {
	OccupiedSlots int               `json:"occupiedSlots"`
	Attempts      []AttemptSnapshot `json:"attempts"`
} {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err := getReady(origin)
		if err != nil {
			t.Fatal(err)
		}
		var ready struct {
			OccupiedSlots int               `json:"occupiedSlots"`
			Attempts      []AttemptSnapshot `json:"attempts"`
		}
		err = json.NewDecoder(response.Body).Decode(&ready)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if len(ready.Attempts) == 1 && ready.Attempts[0].State == "failed" {
			return ready
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for failed attempt")
	return struct {
		OccupiedSlots int               `json:"occupiedSlots"`
		Attempts      []AttemptSnapshot `json:"attempts"`
	}{}
}
