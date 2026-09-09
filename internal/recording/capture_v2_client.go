package recording

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	checkpointRequestLimit = 64 * 1024
	apiResponseLimit       = 4 * 1024 * 1024
)

type CaptureFence struct {
	RecordingID        string
	WorkerID           string
	WorkerGenerationID string
	CaptureGeneration  int32
}

type UploadTarget struct {
	UploadID           string            `json:"uploadId"`
	UploadURL          string            `json:"uploadUrl"`
	UploadURLExpiresAt string            `json:"uploadUrlExpiresAt"`
	RequiredHeaders    map[string]string `json:"requiredHeaders"`
}

type CaptureAPIError struct {
	Status  int
	Code    string
	Message string
}

func (failure *CaptureAPIError) Error() string {
	if failure.Message != "" {
		return fmt.Sprintf(
			"capture API failed with status %d code %s: %s",
			failure.Status,
			failure.Code,
			failure.Message,
		)
	}
	return fmt.Sprintf("capture API failed with status %d code %s", failure.Status, failure.Code)
}

func (client *PrivateUploadClient) Allocate(
	ctx context.Context,
	fence CaptureFence,
	segments []SegmentDescriptor,
) ([]UploadTarget, error) {
	if err := validateCaptureFence(fence); err != nil || len(segments) < 1 || len(segments) > 100 {
		return nil, errors.New("invalid upload allocation")
	}
	for _, segment := range segments {
		if err := validateSegment(segment); err != nil {
			return nil, err
		}
	}
	request := struct {
		ProtocolVersion   string              `json:"protocolVersion"`
		CaptureGeneration int32               `json:"captureGeneration"`
		Segments          []SegmentDescriptor `json:"segments"`
	}{
		ProtocolVersion:   CaptureProtocolVersion,
		CaptureGeneration: fence.CaptureGeneration,
		Segments:          segments,
	}
	var response struct {
		Uploads []UploadTarget `json:"uploads"`
	}
	if err := client.captureJSON(ctx, http.MethodPost, fence, "segments/upload-batch", request, &response); err != nil {
		return nil, err
	}
	if len(response.Uploads) != len(segments) {
		return nil, errors.New("capture API returned an invalid upload count")
	}
	for _, target := range response.Uploads {
		parsed, err := url.Parse(target.UploadURL)
		if err != nil || parsed.Host == "" || target.UploadID == "" || len(target.RequiredHeaders) == 0 {
			return nil, errors.New("capture API returned an invalid upload target")
		}
		if _, err := time.Parse(time.RFC3339Nano, target.UploadURLExpiresAt); err != nil {
			return nil, errors.New("capture API returned an invalid upload expiry")
		}
	}
	return response.Uploads, nil
}

func (client *PrivateUploadClient) PutFile(
	ctx context.Context,
	target UploadTarget,
	path string,
) error {
	parsed, err := url.Parse(target.UploadURL)
	if err != nil || parsed.Host == "" {
		return errors.New("invalid upload target URL")
	}
	info, err := os.Stat(path)
	if err != nil {
		return errors.New("stat durable segment for upload")
	}
	headers := make(http.Header, len(target.RequiredHeaders))
	for name, value := range target.RequiredHeaders {
		if strings.TrimSpace(name) == "" || strings.ContainsAny(name+value, "\r\n") {
			return errors.New("invalid required upload header")
		}
		headers.Set(name, value)
	}
	signedLength, err := strconv.ParseInt(headers.Get("Content-Length"), 10, 64)
	if err != nil || signedLength != info.Size() {
		return errors.New("signed upload content-length does not match durable segment")
	}
	for attempt := 0; attempt < 4; attempt++ {
		file, openErr := os.Open(path)
		if openErr != nil {
			return errors.New("open durable segment for upload")
		}
		status, putErr := client.PutPresigned(ctx, parsed, headers, file)
		if putErr == nil && status >= 200 && status < 300 {
			return nil
		}
		if putErr == nil && status < 500 && status != http.StatusRequestTimeout && status != http.StatusTooManyRequests {
			return fmt.Errorf("presigned PUT failed with status %d", status)
		}
		if attempt == 3 {
			if putErr != nil {
				return putErr
			}
			return fmt.Errorf("presigned PUT failed with status %d", status)
		}
		if err := waitCaptureRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return errors.New("presigned PUT retries exhausted")
}

func (client *PrivateUploadClient) Commit(
	ctx context.Context,
	fence CaptureFence,
	uploadID string,
) error {
	if uploadID == "" || len(uploadID) > 256 {
		return errors.New("invalid upload ID")
	}
	var response struct {
		UploadID string `json:"uploadId"`
		Status   string `json:"status"`
	}
	if err := client.captureJSON(
		ctx,
		http.MethodPost,
		fence,
		"segments/commit",
		map[string]string{"uploadId": uploadID},
		&response,
	); err != nil {
		return err
	}
	if response.UploadID != uploadID || response.Status != "committed" {
		return errors.New("capture API returned an invalid commit response")
	}
	return nil
}

type CheckpointResult struct {
	CheckpointSequence int32
	PayloadSHA256      string
}

func (client *PrivateUploadClient) PutCheckpoint(
	ctx context.Context,
	fence CaptureFence,
	checkpointSequence int32,
	timelineThroughMS int64,
	records []CheckpointRecord,
) (CheckpointResult, error) {
	if checkpointSequence < 0 || timelineThroughMS < 0 {
		return CheckpointResult{}, errors.New("invalid checkpoint bounds")
	}
	canonical := append([]CheckpointRecord{}, records...)
	sortCheckpointRecords(canonical)
	hash, err := canonicalCheckpointHash(fence.CaptureGeneration, timelineThroughMS, canonical)
	if err != nil {
		return CheckpointResult{}, err
	}
	chunks, err := checkpointChunks(fence.CaptureGeneration, timelineThroughMS, hash, canonical)
	if err != nil {
		return CheckpointResult{}, err
	}
	for index, recordsChunk := range chunks {
		body := checkpointChunkBody{
			ProtocolVersion: CaptureProtocolVersion, CaptureGeneration: fence.CaptureGeneration,
			ChunkIndex: int32(index), TotalChunks: int32(len(chunks)), Final: index == len(chunks)-1,
			TimelineThroughMS: timelineThroughMS, PayloadSHA256: hash, Records: recordsChunk,
		}
		var response struct {
			CheckpointSequence int32  `json:"checkpointSequence"`
			ChunkIndex         int32  `json:"chunkIndex"`
			Status             string `json:"status"`
			PayloadSHA256      string `json:"payloadSha256,omitempty"`
		}
		path := "timeline/checkpoints/" + strconv.FormatInt(int64(checkpointSequence), 10)
		if err := client.captureJSON(ctx, http.MethodPut, fence, path, body, &response); err != nil {
			return CheckpointResult{}, err
		}
		wantStatus := "accepted"
		if body.Final {
			wantStatus = "committed"
		}
		wrongEnvelope := response.CheckpointSequence != checkpointSequence ||
			response.ChunkIndex != int32(index) || response.Status != wantStatus
		wrongFinalHash := body.Final && response.PayloadSHA256 != hash
		if wrongEnvelope || wrongFinalHash {
			return CheckpointResult{}, errors.New("capture API returned an invalid checkpoint response")
		}
	}
	return CheckpointResult{CheckpointSequence: checkpointSequence, PayloadSHA256: hash}, nil
}

type checkpointChunkBody struct {
	ProtocolVersion   string             `json:"protocolVersion"`
	CaptureGeneration int32              `json:"captureGeneration"`
	ChunkIndex        int32              `json:"chunkIndex"`
	TotalChunks       int32              `json:"totalChunks"`
	Final             bool               `json:"final"`
	TimelineThroughMS int64              `json:"timelineThroughMs"`
	PayloadSHA256     string             `json:"payloadSha256"`
	Records           []CheckpointRecord `json:"records"`
}

func checkpointChunks(
	generation int32,
	timeline int64,
	hash string,
	records []CheckpointRecord,
) ([][]CheckpointRecord, error) {
	if len(records) == 0 {
		return [][]CheckpointRecord{{}}, nil
	}
	chunks := [][]CheckpointRecord{}
	current := []CheckpointRecord{}
	for _, record := range records {
		candidate := append(append([]CheckpointRecord{}, current...), record)
		probe := checkpointChunkBody{
			ProtocolVersion: CaptureProtocolVersion, CaptureGeneration: generation,
			ChunkIndex: 2_147_483_647, TotalChunks: 2_147_483_647,
			Final: false, TimelineThroughMS: timeline, PayloadSHA256: hash, Records: candidate,
		}
		encoded, err := json.Marshal(probe)
		if err != nil {
			return nil, err
		}
		if len(encoded) > checkpointRequestLimit {
			if len(current) == 0 {
				return nil, errors.New("one checkpoint record exceeds the transport bound")
			}
			chunks = append(chunks, current)
			current = []CheckpointRecord{record}
			singleton := probe
			singleton.Records = current
			encoded, err = json.Marshal(singleton)
			if err != nil {
				return nil, err
			}
			if len(encoded) > checkpointRequestLimit {
				return nil, errors.New("one checkpoint record exceeds the transport bound")
			}
			continue
		}
		current = candidate
	}
	chunks = append(chunks, current)
	return chunks, nil
}

type SealRequest struct {
	Completeness                    string
	TimelineEndMS                   int64
	LastCheckpointCaptureGeneration int32
	LastCheckpointSequence          int32
	CheckpointPayloadSHA256         string
}

type SealResult struct {
	RecordingID      string `json:"recordingId"`
	Status           string `json:"status"`
	Completeness     string `json:"completeness"`
	CompositionJobID string `json:"compositionJobId"`
	ManifestSHA256   string `json:"manifestSha256"`
}

func (client *PrivateUploadClient) Seal(ctx context.Context, fence CaptureFence, seal SealRequest) (SealResult, error) {
	body := struct {
		ProtocolVersion                 string `json:"protocolVersion"`
		CaptureGeneration               int32  `json:"captureGeneration"`
		Completeness                    string `json:"completeness"`
		TimelineEndMS                   int64  `json:"timelineEndMs"`
		LastCheckpointCaptureGeneration int32  `json:"lastCheckpointCaptureGeneration"`
		LastCheckpointSequence          int32  `json:"lastCheckpointSequence"`
		CheckpointPayloadSHA256         string `json:"checkpointPayloadSha256"`
	}{
		ProtocolVersion: CaptureProtocolVersion, CaptureGeneration: fence.CaptureGeneration,
		Completeness: seal.Completeness, TimelineEndMS: seal.TimelineEndMS,
		LastCheckpointCaptureGeneration: seal.LastCheckpointCaptureGeneration,
		LastCheckpointSequence:          seal.LastCheckpointSequence,
		CheckpointPayloadSHA256:         seal.CheckpointPayloadSHA256,
	}
	var response SealResult
	if err := client.captureJSON(ctx, http.MethodPost, fence, "seal", body, &response); err != nil {
		return SealResult{}, err
	}
	wrongSeal := response.RecordingID != fence.RecordingID || response.Status != "CAPTURED" ||
		response.Completeness != seal.Completeness || response.CompositionJobID == "" ||
		!validCanonicalSHA256(response.ManifestSHA256)
	if wrongSeal {
		return SealResult{}, errors.New("capture API returned an invalid seal response")
	}
	return response, nil
}

func (client *PrivateUploadClient) Fail(
	ctx context.Context,
	fence CaptureFence,
	code string,
	message string,
) error {
	if err := validateCaptureFence(fence); err != nil || code == "" || message == "" {
		return errors.New("invalid capture failure")
	}
	var response struct {
		Failed bool `json:"failed"`
	}
	if err := client.recordingJSON(ctx, http.MethodPost, fence, "capture-v2/fail", map[string]any{
		"protocolVersion":   CaptureProtocolVersion,
		"captureGeneration": fence.CaptureGeneration,
		"code":              code,
		"message":           message,
	}, &response); err != nil {
		return err
	}
	if !response.Failed {
		return errors.New("capture API returned an invalid failure response")
	}
	return nil
}

func (client *PrivateUploadClient) captureJSON(
	ctx context.Context,
	method string,
	fence CaptureFence,
	path string,
	body any,
	destination any,
) error {
	return client.recordingJSON(ctx, method, fence, "capture-v2/"+path, body, destination)
}

func (client *PrivateUploadClient) recordingJSON(
	ctx context.Context,
	method string,
	fence CaptureFence,
	path string,
	body any,
	destination any,
) error {
	for attempt := 0; attempt < 4; attempt++ {
		operationContext, cancel := context.WithTimeout(ctx, client.apiTimeout)
		err := client.recordingJSONOnce(operationContext, method, fence, path, body, destination)
		cancel()
		if err == nil || !retryableCaptureError(err) {
			return err
		}
		if attempt == 3 {
			return err
		}
		if err := waitCaptureRetry(ctx, attempt); err != nil {
			return err
		}
	}
	return errors.New("capture API retries exhausted")
}

func (client *PrivateUploadClient) recordingJSONOnce(
	ctx context.Context,
	method string,
	fence CaptureFence,
	path string,
	body any,
	destination any,
) error {
	allowRecoveryDiscovery := path == "capture-v2/recovery" || strings.HasPrefix(path, "capture-v2/recovery?")
	discoveryFence := allowRecoveryDiscovery && fence.CaptureGeneration == 0 &&
		fence.RecordingID != "" && fence.WorkerID != "" && fence.WorkerGenerationID != ""
	if err := validateCaptureFence(fence); err != nil && !discoveryFence {
		return err
	}
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	target := cloneURL(client.baseURL)
	requestPath, rawQuery, _ := strings.Cut(path, "?")
	target.Path = strings.TrimSuffix(target.Path, "/") + "/internal/v1/recordings/" +
		url.PathEscape(fence.RecordingID) + "/" + requestPath
	target.RawQuery = rawQuery
	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, target.String(), requestBody)
	if err != nil {
		return errors.New("build capture API request")
	}
	request.Header.Set("Authorization", "Bearer "+string(client.secret))
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("X-Recording-Worker-Id", fence.WorkerID)
	request.Header.Set("X-Recording-Worker-Generation-Id", fence.WorkerGenerationID)
	response, err := client.httpClient.Do(request)
	if err != nil {
		return errors.New("capture API request failed")
	}
	defer response.Body.Close()
	reader := io.LimitReader(response.Body, apiResponseLimit+1)
	responseBytes, err := io.ReadAll(reader)
	if err != nil || len(responseBytes) > apiResponseLimit {
		return errors.New("capture API response exceeded its bound")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var failure struct {
			Error   string `json:"error"`
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(responseBytes, &failure)
		code := failure.Code
		if code == "" {
			code = failure.Error
		}
		if code == "" {
			code = "CAPTURE_API_ERROR"
		}
		return &CaptureAPIError{
			Status: response.StatusCode,
			Code:   code,
			Message: strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(
				failure.Message,
				"\r", " ",
			), "\n", " "), "\t", " "),
		}
	}
	if destination == nil {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(responseBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errors.New("decode capture API response")
	}
	return nil
}

func retryableCaptureError(err error) bool {
	var failure *CaptureAPIError
	if !errors.As(err, &failure) {
		return true
	}
	return failure.Status >= 500 || failure.Status == http.StatusRequestTimeout ||
		failure.Status == http.StatusTooManyRequests
}

func waitCaptureRetry(ctx context.Context, attempt int) error {
	delay := time.Duration(50*(1<<attempt)) * time.Millisecond
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func validateCaptureFence(fence CaptureFence) error {
	if fence.RecordingID == "" || fence.WorkerID == "" || fence.WorkerGenerationID == "" || fence.CaptureGeneration <= 0 {
		return errors.New("invalid capture fence")
	}
	return nil
}
