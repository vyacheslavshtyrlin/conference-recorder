package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

const signalingRequestTimeout = 10 * time.Second

var (
	ErrAdmissionRejected = errors.New("recording bot admission rejected")
	ErrConnectTimeout    = errors.New("recording bot connect timeout")
)

type responseEnvelope struct {
	Type      string          `json:"type"`
	RequestID string          `json:"requestId"`
	OK        bool            `json:"ok"`
	Data      json.RawMessage `json:"data"`
	Error     *responseError  `json:"error,omitempty"`
}

type responseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type responseResult struct {
	data json.RawMessage
	err  error
}

// SessionIdentity binds a signaling connection to one assigned room recording.
type SessionIdentity struct {
	RoomID      string
	RecordingID string
}

// WebSocketSignalingClient is an asynchronous recording-bot.v1 client. The
// admission is accepted separately and is never retained in an error or log.
type WebSocketSignalingClient struct {
	connection *websocket.Conn
	cancel     context.CancelFunc

	mu       sync.Mutex
	sequence uint64
	pending  map[string]chan responseResult
	writeMu  sync.Mutex

	snapshots         chan Snapshot
	snapshotMu        sync.Mutex
	snapshotActive    bool
	snapshotDelivered bool
	lifecycle         chan Event
	lifecycleMu       sync.RWMutex
	admitted          chan error
	done              chan struct{}
	closeOnce         sync.Once
	identity          SessionIdentity
}

// DialWebSocketSignaling opens the private endpoint and waits for
// bot:admitted. The returned errors never include the admission query.
func DialWebSocketSignaling(
	ctx context.Context,
	signalingURL string,
	admission string,
) (*WebSocketSignalingClient, error) {
	return dialWebSocketSignaling(ctx, signalingURL, admission, SessionIdentity{})
}

// DialWebSocketSignalingForAttempt opens a private connection and requires the
// admitted identity to match the worker assignment before returning.
func DialWebSocketSignalingForAttempt(
	ctx context.Context,
	signalingURL string,
	admission string,
	expected SessionIdentity,
) (*WebSocketSignalingClient, error) {
	if err := validateSessionIdentity(expected); err != nil {
		return nil, err
	}
	return dialWebSocketSignaling(ctx, signalingURL, admission, expected)
}

func dialWebSocketSignaling(
	ctx context.Context,
	signalingURL string,
	admission string,
	expected SessionIdentity,
) (*WebSocketSignalingClient, error) {
	if admission == "" {
		return nil, ErrAdmissionRejected
	}
	endpoint, err := privateSignalingURL(signalingURL, admission)
	if err != nil {
		return nil, err
	}
	connection, response, err := websocket.Dial(ctx, endpoint, nil)
	if err != nil {
		status := 0
		if response != nil {
			status = response.StatusCode
		}
		if status == 401 || status == 403 {
			return nil, ErrAdmissionRejected
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, ErrConnectTimeout
		}
		return nil, fmt.Errorf("dial private recording signaling (status %d)", status)
	}
	connection.SetReadLimit(4 << 20)

	readContext, cancel := context.WithCancel(context.Background())
	client := &WebSocketSignalingClient{
		connection: connection,
		cancel:     cancel,
		pending:    map[string]chan responseResult{},
		snapshots:  make(chan Snapshot, 1),
		lifecycle:  make(chan Event, 256),
		admitted:   make(chan error, 1),
		done:       make(chan struct{}),
		identity:   expected,
	}
	go client.readLoop(readContext)

	waitContext, waitCancel := withRequestTimeout(ctx)
	defer waitCancel()
	select {
	case admittedErr := <-client.admitted:
		if admittedErr != nil {
			_ = client.Close()
			return nil, admittedErr
		}
		return client, nil
	case <-waitContext.Done():
		_ = client.Close()
		return nil, fmt.Errorf("%w: %v", ErrConnectTimeout, waitContext.Err())
	}
}

func privateSignalingURL(rawURL, admission string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("invalid private signaling URL")
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return "", errors.New("private signaling URL must use ws or wss")
	}
	parsed.Path = "/internal/recording-bot/v1"
	parsed.RawQuery = ""
	query := parsed.Query()
	query.Set("admission", admission)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (client *WebSocketSignalingClient) RouterRTPCapabilities(
	ctx context.Context,
) (json.RawMessage, error) {
	return client.request(ctx, "bot:getRouterRtpCapabilities", map[string]any{})
}

func (client *WebSocketSignalingClient) CreateRecvTransport(
	ctx context.Context,
) (TransportOptions, error) {
	var transport TransportOptions
	err := client.requestInto(ctx, "bot:createRecvTransport", map[string]any{}, &transport)
	return transport, err
}

func (client *WebSocketSignalingClient) SyncSnapshot(ctx context.Context) (Snapshot, error) {
	if err := client.beginSnapshotRequest(); err != nil {
		return Snapshot{}, err
	}
	if _, err := client.request(ctx, "bot:syncSnapshot", map[string]any{}); err != nil {
		client.abortSnapshotRequest()
		return Snapshot{}, err
	}
	waitContext, cancel := withRequestTimeout(ctx)
	defer cancel()
	select {
	case snapshot := <-client.snapshots:
		client.finishSnapshotRequest()
		return snapshot, nil
	case <-client.done:
		client.abortSnapshotRequest()
		return Snapshot{}, errors.New("signaling closed before producer snapshot")
	case <-waitContext.Done():
		client.abortSnapshotRequest()
		return Snapshot{}, fmt.Errorf("wait for producer snapshot: %w", waitContext.Err())
	}
}

func (client *WebSocketSignalingClient) Consume(
	ctx context.Context,
	transportID string,
	producerID string,
	rtpCapabilities json.RawMessage,
) (Consumer, error) {
	var consumer Consumer
	err := client.requestInto(ctx, "bot:consume", map[string]any{
		"transportId":     transportID,
		"producerId":      producerID,
		"rtpCapabilities": rtpCapabilities,
	}, &consumer)
	return consumer, err
}

func (client *WebSocketSignalingClient) ConnectRecvTransport(
	ctx context.Context,
	transportID string,
	dtlsParameters DTLSParameters,
) error {
	_, err := client.request(ctx, "bot:connectRecvTransport", map[string]any{
		"transportId":    transportID,
		"dtlsParameters": dtlsParameters,
	})
	return err
}

func (client *WebSocketSignalingClient) ResumeConsumer(
	ctx context.Context,
	producerID string,
) error {
	_, err := client.request(ctx, "bot:resumeConsumer", map[string]any{
		"producerId": producerID,
	})
	return err
}

func (client *WebSocketSignalingClient) PauseConsumer(
	ctx context.Context,
	producerID string,
) error {
	_, err := client.request(ctx, "bot:pauseConsumer", map[string]any{
		"producerId": producerID,
	})
	return err
}

func (client *WebSocketSignalingClient) CloseConsumer(
	ctx context.Context,
	producerID string,
) error {
	_, err := client.request(ctx, "bot:closeConsumer", map[string]any{
		"producerId": producerID,
	})
	return err
}

func (client *WebSocketSignalingClient) Lifecycle() <-chan Event {
	return client.lifecycle
}

// Close shuts down the socket and releases every pending request.
func (client *WebSocketSignalingClient) Close() error {
	client.shutdown(errors.New("signaling client closed"))
	return nil
}

func (client *WebSocketSignalingClient) requestInto(
	ctx context.Context,
	messageType string,
	body map[string]any,
	destination any,
) error {
	data, err := client.request(ctx, messageType, body)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return fmt.Errorf("decode %s response: %w", messageType, err)
	}
	return nil
}

func (client *WebSocketSignalingClient) request(
	ctx context.Context,
	messageType string,
	body map[string]any,
) (json.RawMessage, error) {
	requestID, result := client.registerRequest()
	message := make(map[string]any, len(body)+2)
	message["type"] = messageType
	message["requestId"] = requestID
	for key, value := range body {
		message[key] = value
	}

	writeContext, cancel := withRequestTimeout(ctx)
	defer cancel()
	client.writeMu.Lock()
	err := wsjson.Write(writeContext, client.connection, message)
	client.writeMu.Unlock()
	if err != nil {
		client.removeRequest(requestID)
		client.shutdown(errors.New("private signaling write failed"))
		return nil, errors.New("private signaling write failed")
	}

	select {
	case response := <-result:
		return response.data, response.err
	case <-writeContext.Done():
		client.removeRequest(requestID)
		return nil, fmt.Errorf("%s request: %w", messageType, writeContext.Err())
	case <-client.done:
		client.removeRequest(requestID)
		return nil, errors.New("private signaling closed")
	}
}

func (client *WebSocketSignalingClient) registerRequest() (string, chan responseResult) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.sequence++
	requestID := "go-probe-" + strconv.FormatUint(client.sequence, 10)
	result := make(chan responseResult, 1)
	client.pending[requestID] = result
	return requestID, result
}

func (client *WebSocketSignalingClient) removeRequest(requestID string) {
	client.mu.Lock()
	delete(client.pending, requestID)
	client.mu.Unlock()
}

func (client *WebSocketSignalingClient) readLoop(ctx context.Context) {
	for {
		var raw json.RawMessage
		if err := wsjson.Read(ctx, client.connection, &raw); err != nil {
			client.shutdown(errors.New("private signaling read failed"))
			return
		}
		if err := client.routeMessage(raw); err != nil {
			client.shutdown(err)
			return
		}
	}
}

func (client *WebSocketSignalingClient) routeMessage(raw json.RawMessage) error {
	var header struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &header); err != nil || header.Type == "" {
		return errors.New("invalid private signaling message")
	}
	switch header.Type {
	case "bot:admitted":
		var admitted struct {
			ProtocolVersion string `json:"protocolVersion"`
			RoomID          string `json:"roomId"`
			RecordingID     string `json:"recordingId"`
		}
		if err := json.Unmarshal(raw, &admitted); err != nil || admitted.ProtocolVersion != ProtocolVersion {
			return ErrAdmissionRejected
		}
		identity := SessionIdentity{RoomID: admitted.RoomID, RecordingID: admitted.RecordingID}
		if validateSessionIdentity(identity) != nil {
			return ErrAdmissionRejected
		}
		if client.identity != (SessionIdentity{}) && client.identity != identity {
			return ErrAdmissionRejected
		}
		client.identity = identity
		select {
		case client.admitted <- nil:
		default:
		}
		return nil
	case "response":
		return client.routeResponse(raw)
	case "bot:producerSnapshot":
		var snapshot Snapshot
		if err := json.Unmarshal(raw, &snapshot); err != nil {
			return errors.New("invalid producer snapshot")
		}
		if snapshot.RecordingID != client.identity.RecordingID {
			return errors.New("producer snapshot recordingId does not match admission")
		}
		return client.deliverSnapshot(snapshot)
	case "bot:stop":
		var stop Event
		if err := json.Unmarshal(raw, &stop); err != nil ||
			stop.RecordingID != client.identity.RecordingID {
			return errors.New("recording stop does not match admission")
		}
		client.lifecycleMu.RLock()
		defer client.lifecycleMu.RUnlock()
		select {
		case client.lifecycle <- stop:
			return nil
		default:
			return errors.New("private signaling lifecycle backpressure exceeded")
		}
	default:
		var event Event
		if err := json.Unmarshal(raw, &event); err != nil || event.Revision == 0 {
			return errors.New("invalid lifecycle event")
		}
		if event.RecordingID != client.identity.RecordingID {
			return errors.New("lifecycle event recordingId does not match admission")
		}
		client.lifecycleMu.RLock()
		defer client.lifecycleMu.RUnlock()
		select {
		case <-client.done:
			return errors.New("private signaling closed")
		default:
		}
		select {
		case client.lifecycle <- event:
			return nil
		default:
			return errors.New("private signaling lifecycle backpressure exceeded")
		}
	}
}

func validateSessionIdentity(identity SessionIdentity) error {
	if identity.RoomID == "" || identity.RecordingID == "" ||
		len(identity.RoomID) > 128 || len(identity.RecordingID) > 128 {
		return errors.New("invalid signaling session identity")
	}
	return nil
}

func (client *WebSocketSignalingClient) beginSnapshotRequest() error {
	client.snapshotMu.Lock()
	defer client.snapshotMu.Unlock()
	if client.snapshotActive {
		return errors.New("producer snapshot request is already active")
	}
	client.snapshotActive = true
	client.snapshotDelivered = false
	return nil
}

func (client *WebSocketSignalingClient) deliverSnapshot(snapshot Snapshot) error {
	client.snapshotMu.Lock()
	defer client.snapshotMu.Unlock()
	if !client.snapshotActive || client.snapshotDelivered {
		return errors.New("unexpected producer snapshot")
	}
	client.snapshotDelivered = true
	select {
	case client.snapshots <- snapshot:
		return nil
	default:
		return errors.New("producer snapshot queue is unexpectedly occupied")
	}
}

func (client *WebSocketSignalingClient) finishSnapshotRequest() {
	client.snapshotMu.Lock()
	client.snapshotActive = false
	client.snapshotDelivered = false
	client.snapshotMu.Unlock()
}

func (client *WebSocketSignalingClient) abortSnapshotRequest() {
	client.snapshotMu.Lock()
	client.snapshotActive = false
	client.snapshotDelivered = false
	select {
	case <-client.snapshots:
	default:
	}
	client.snapshotMu.Unlock()
}

func (client *WebSocketSignalingClient) routeResponse(raw json.RawMessage) error {
	var response responseEnvelope
	if err := json.Unmarshal(raw, &response); err != nil || response.RequestID == "" {
		return errors.New("invalid private signaling response")
	}
	client.mu.Lock()
	result, exists := client.pending[response.RequestID]
	if exists {
		delete(client.pending, response.RequestID)
	}
	client.mu.Unlock()
	if !exists {
		return nil
	}
	if response.OK {
		result <- responseResult{data: response.Data}
		return nil
	}
	code := "BOT_REQUEST_FAILED"
	if response.Error != nil && response.Error.Code != "" {
		code = response.Error.Code
	}
	result <- responseResult{err: fmt.Errorf("private signaling request failed: %s", code)}
	return nil
}

func (client *WebSocketSignalingClient) shutdown(cause error) {
	client.closeOnce.Do(func() {
		select {
		case client.admitted <- cause:
		default:
		}
		client.cancel()
		client.mu.Lock()
		for requestID, result := range client.pending {
			result <- responseResult{err: cause}
			delete(client.pending, requestID)
		}
		client.mu.Unlock()
		close(client.done)
		client.lifecycleMu.Lock()
		close(client.lifecycle)
		client.lifecycleMu.Unlock()
		client.connection.CloseNow()
	})
}

func withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, signalingRequestTimeout)
}
