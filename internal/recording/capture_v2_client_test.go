package recording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCaptureV2AllocatePutCheckpointCommitAndSeal(t *testing.T) {
	var mu sync.Mutex
	checkpointBodies := [][]byte{}
	objectBytes := []byte{}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/internal/v1/recordings/rec-1/capture-v2/") {
			if request.Header.Get("Authorization") != "Bearer capture-service-secret-at-least-32-bytes" || request.Header.Get("X-Recording-Worker-Id") != "worker-1" {
				http.Error(response, "missing fence", http.StatusUnauthorized)
				return
			}
		}
		switch {
		case request.URL.Path == "/internal/v1/recordings/rec-1/capture-v2/segments/upload-batch":
			writeJSON(response, http.StatusOK, map[string]any{"uploads": []any{map[string]any{
				"uploadId": "upload-1", "uploadUrl": server.URL + "/object", "uploadUrlExpiresAt": "2026-08-25T10:01:00Z",
				"requiredHeaders": map[string]string{"content-type": "video/webm", "content-length": "11"},
			}}})
		case request.URL.Path == "/object":
			if request.ContentLength != 11 || len(request.TransferEncoding) != 0 {
				t.Errorf("presigned PUT was not fixed-length: length=%d transfer=%v", request.ContentLength, request.TransferEncoding)
			}
			objectBytes, _ = io.ReadAll(request.Body)
			response.WriteHeader(http.StatusOK)
		case request.URL.Path == "/internal/v1/recordings/rec-1/capture-v2/segments/commit":
			writeJSON(response, http.StatusOK, map[string]string{"uploadId": "upload-1", "status": "committed"})
		case strings.Contains(request.URL.Path, "/timeline/checkpoints/"):
			body, _ := io.ReadAll(request.Body)
			if len(body) > checkpointRequestLimit {
				http.Error(response, "too large", http.StatusRequestEntityTooLarge)
				return
			}
			mu.Lock()
			checkpointBodies = append(checkpointBodies, body)
			mu.Unlock()
			var chunk checkpointChunkBody
			_ = json.Unmarshal(body, &chunk)
			status := "accepted"
			result := map[string]any{"checkpointSequence": 0, "chunkIndex": chunk.ChunkIndex, "status": status}
			if chunk.Final {
				result["status"] = "committed"
				result["payloadSha256"] = chunk.PayloadSHA256
			}
			writeJSON(response, http.StatusOK, result)
		case request.URL.Path == "/internal/v1/recordings/rec-1/capture-v2/seal":
			writeJSON(response, http.StatusOK, map[string]string{
				"recordingId": "rec-1", "status": "CAPTURED", "completeness": "complete",
				"compositionJobId": "job-1", "manifestSha256": strings.Repeat("a", 64),
			})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client := testCaptureClient(t, server.URL)
	fence := CaptureFence{"rec-1", "worker-1", "process-1", 1}
	media := []byte("closed webm")
	digest := sha256.Sum256(media)
	descriptor := fixtureSegmentDescriptor(int64(len(media)), hex.EncodeToString(digest[:]))
	uploads, err := client.Allocate(context.Background(), fence, []SegmentDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "segment.webm")
	if err := os.WriteFile(path, media, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := client.PutFile(context.Background(), uploads[0], path); err != nil {
		t.Fatal(err)
	}
	if string(objectBytes) != string(media) {
		t.Fatal("direct PUT bytes differ")
	}
	records := make([]CheckpointRecord, 0, 800)
	for index := 0; index < 800; index++ {
		participantID := ParticipantInstanceID("rec-1", "participant-"+strconv.Itoa(index))
		records = append(records, CheckpointRecord{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: strings.Repeat("x", 160)})
	}
	checkpoint, err := client.PutCheckpoint(context.Background(), fence, 0, 1000, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpointBodies) < 2 || checkpoint.PayloadSHA256 == "" {
		t.Fatalf("checkpoint was not chunked: %d", len(checkpointBodies))
	}
	if err := client.Commit(context.Background(), fence, uploads[0].UploadID); err != nil {
		t.Fatal(err)
	}
	seal, err := client.Seal(context.Background(), fence, SealRequest{"complete", 1000, 1, 0, checkpoint.PayloadSHA256})
	if err != nil || seal.Status != "CAPTURED" {
		t.Fatalf("seal failed: %+v %v", seal, err)
	}
}

func TestCaptureV2RetriesTransientFailuresButNotStaleFence(t *testing.T) {
	transientCalls := 0
	staleCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/internal/v1/recordings/rec-1/capture-v2/segments/commit":
			transientCalls++
			if transientCalls < 3 {
				writeJSON(response, http.StatusServiceUnavailable, map[string]string{"error": "S3_UNAVAILABLE"})
				return
			}
			writeJSON(response, http.StatusOK, map[string]string{"uploadId": "upload-1", "status": "committed"})
		case "/internal/v1/recordings/rec-1/capture-v2/seal":
			staleCalls++
			writeJSON(response, http.StatusConflict, map[string]string{"error": "STALE_CAPTURE_GENERATION"})
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()
	client := testCaptureClient(t, server.URL)
	fence := CaptureFence{"rec-1", "worker-1", "process-1", 1}
	if err := client.Commit(context.Background(), fence, "upload-1"); err != nil {
		t.Fatal(err)
	}
	_, err := client.Seal(context.Background(), fence, SealRequest{
		Completeness: "complete", TimelineEndMS: 1,
		LastCheckpointCaptureGeneration: 1, LastCheckpointSequence: 0,
		CheckpointPayloadSHA256: strings.Repeat("a", 64),
	})
	var failure *CaptureAPIError
	if transientCalls != 3 || !errors.As(err, &failure) ||
		failure.Code != "STALE_CAPTURE_GENERATION" || staleCalls != 1 {
		t.Fatalf("retry classification failed: transient=%d stale=%d err=%v", transientCalls, staleCalls, err)
	}
}

func TestCaptureV2RecoveryRestartsExpiredCursorAndLoadsAllPages(t *testing.T) {
	firstRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet {
			body, _ := io.ReadAll(request.Body)
			if request.ContentLength != 0 || len(request.TransferEncoding) != 0 || len(body) != 0 || request.Header.Get("Content-Type") != "" {
				t.Errorf("recovery GET carried a body: length=%d transfer=%v body=%q", request.ContentLength, request.TransferEncoding, body)
			}
		}
		if request.URL.Query().Get("cursor") == "expired" {
			writeJSON(response, http.StatusGone, map[string]string{"error": "RECOVERY_CURSOR_EXPIRED"})
			return
		}
		if request.URL.Query().Get("cursor") == "next" {
			writeJSON(response, http.StatusOK, recoveryPageFixture(nil, []recoveryTrack{{TrackID: mustTrackID(t), ParticipantInstanceID: ParticipantInstanceID("rec-1", "participant-1"), Source: SourceCamera}}))
			return
		}
		firstRequests++
		next := "expired"
		if firstRequests > 1 {
			next = "next"
		}
		page := recoveryPageFixture(&next, nil)
		page.ParticipantInstances = []recoveryParticipantInstance{{
			ParticipantInstanceID: ParticipantInstanceID("rec-1", "participant-1"),
			DisplayName:           "Alex",
		}}
		writeJSON(response, http.StatusOK, page)
	}))
	defer server.Close()
	client := testCaptureClient(t, server.URL)
	state, err := client.Recover(context.Background(), CaptureFence{"rec-1", "worker-1", "process-1", 2})
	if err != nil {
		t.Fatal(err)
	}
	if firstRequests != 2 || len(state.Tracks) != 1 {
		t.Fatalf("recovery did not restart/load all pages: first=%d state=%+v", firstRequests, state)
	}
}

func TestCheckpointChunksRejectsOversizedRecordAfterNonemptyChunk(t *testing.T) {
	records := []CheckpointRecord{
		{Category: "participantInstances", ParticipantInstanceID: ParticipantInstanceID("rec-1", "small"), DisplayName: "small"},
		{Category: "participantInstances", ParticipantInstanceID: ParticipantInstanceID("rec-1", "large"), DisplayName: strings.Repeat("x", checkpointRequestLimit)},
	}
	if _, err := checkpointChunks(1, 1, strings.Repeat("a", 64), records); err == nil {
		t.Fatal("oversized singleton record after a nonempty chunk was accepted")
	}
}

func TestCaptureAPIRequestHasIndependentOperationDeadline(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		select {
		case <-request.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client := testCaptureClient(t, server.URL)
	client.apiTimeout = 10 * time.Millisecond
	err := client.Commit(context.Background(), CaptureFence{"rec-1", "worker-1", "process-1", 1}, "upload-1")
	if err == nil {
		t.Fatal("capture API operation without a response ignored its independent deadline")
	}
}

func TestRecoverySnapshotUsesWorkerMemoryEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, recoveryPageFixture(nil, nil))
	}))
	defer server.Close()
	client := testCaptureClient(t, server.URL)
	client.maxRecoveryBytes = 1
	_, err := client.Recover(context.Background(), CaptureFence{"rec-1", "worker-1", "process-1", 2})
	if err == nil || !strings.Contains(err.Error(), "memory envelope") {
		t.Fatalf("oversized recovery snapshot was not rejected locally: %v", err)
	}
}

func TestRecoveryRejectsDuplicateCommittedSegmentIdentity(t *testing.T) {
	descriptor := fixtureSegmentDescriptor(1, strings.Repeat("a", 64))
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		page := recoveryPageFixture(nil, nil)
		segment := recoverySegment{CaptureGeneration: 2, UploadID: "upload-1", SegmentDescriptor: descriptor}
		page.Segments = []recoverySegment{segment, segment}
		writeJSON(response, http.StatusOK, page)
	}))
	defer server.Close()
	client := testCaptureClient(t, server.URL)
	_, err := client.Recover(context.Background(), CaptureFence{"rec-1", "worker-1", "process-1", 2})
	if err == nil || !strings.Contains(err.Error(), "duplicate recovered segment") {
		t.Fatalf("duplicate recovered segment identity was accepted: %v", err)
	}
}

func TestSealRejectsNoncanonicalManifestChecksum(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		writeJSON(response, http.StatusOK, map[string]string{
			"recordingId": "rec-1", "status": "CAPTURED", "completeness": "complete",
			"compositionJobId": "job-1", "manifestSha256": strings.Repeat("A", 64),
		})
	}))
	defer server.Close()
	client := testCaptureClient(t, server.URL)
	_, err := client.Seal(
		context.Background(),
		CaptureFence{"rec-1", "worker-1", "process-1", 1},
		SealRequest{"complete", 1000, 1, 0, strings.Repeat("a", 64)},
	)
	if err == nil {
		t.Fatal("seal accepted a noncanonical manifest checksum")
	}
}

func recoveryPageFixture(next *string, tracks []recoveryTrack) recoveryPage {
	return recoveryPage{
		ProtocolVersion: CaptureProtocolVersion, RecoverySnapshotID: "snapshot-1", CaptureGeneration: 2,
		CaptureStartedAt: "2026-08-25T10:00:00Z", BindingMode: "capture", ParticipantInstances: []recoveryParticipantInstance{},
		ParticipantIntervals: []recoveryParticipantInterval{}, Tracks: tracks, TrackIntervals: []recoveryTrackInterval{},
		Segments: []recoverySegment{}, Gaps: []recoveryGap{}, GenerationCheckpoints: []GenerationCheckpoint{}, NextCursor: next,
	}
}

func mustTrackID(t *testing.T) string {
	t.Helper()
	value, err := TrackID("rec-1", ParticipantInstanceID("rec-1", "participant-1"), SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func testCaptureClient(t *testing.T, origin string) *PrivateUploadClient {
	t.Helper()
	parsed, err := url.Parse(origin)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewPrivateUploadClient(parsed, "capture-service-secret-at-least-32-bytes", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
