package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestWebSocketClientHandlesSnapshotBeforeResponseAndOrderedEvents(t *testing.T) {
	var requestIDs sync.Map
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/internal/recording-bot/v1" {
			t.Errorf("unexpected path %q", request.URL.Path)
		}
		if request.URL.Query().Get("admission") != "one-use-secret" {
			t.Error("admission was not sent in private query")
		}
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.CloseNow()
		ctx := request.Context()
		_ = wsjson.Write(ctx, connection, map[string]any{
			"type": "bot:admitted", "protocolVersion": ProtocolVersion,
			"roomId": "room-1", "recordingId": "rec-1",
		})
		for {
			var message map[string]json.RawMessage
			if err := wsjson.Read(ctx, connection, &message); err != nil {
				return
			}
			var messageType string
			var requestID string
			_ = json.Unmarshal(message["type"], &messageType)
			_ = json.Unmarshal(message["requestId"], &requestID)
			if _, loaded := requestIDs.LoadOrStore(requestID, struct{}{}); loaded {
				t.Errorf("duplicate request ID %q", requestID)
			}
			switch messageType {
			case "bot:getRouterRtpCapabilities":
				writeResponse(ctx, connection, requestID, json.RawMessage(`{"codecs":[]}`))
			case "bot:createRecvTransport":
				writeResponse(ctx, connection, requestID, fixtureTransport())
			case "bot:syncSnapshot":
				snapshot := baseSnapshot()
				payload, _ := json.Marshal(snapshot)
				var event map[string]any
				_ = json.Unmarshal(payload, &event)
				event["type"] = "bot:producerSnapshot"
				_ = wsjson.Write(ctx, connection, event)
				writeResponse(ctx, connection, requestID, map[string]any{})
				_ = wsjson.Write(ctx, connection, Event{
					Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 11, ProducerID: "camera-1",
				})
				_ = wsjson.Write(ctx, connection, Event{
					Type: "bot:producerResumed", RecordingID: "rec-1", Revision: 12, ProducerID: "camera-1",
				})
			}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := DialWebSocketSignalingForAttempt(
		ctx,
		"ws"+strings.TrimPrefix(server.URL, "http")+"/ignored?old=secret",
		"one-use-secret",
		SessionIdentity{RoomID: "room-1", RecordingID: "rec-1"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.RouterRTPCapabilities(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateRecvTransport(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.SyncSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision != 10 {
		t.Fatalf("unexpected snapshot revision %d", snapshot.Revision)
	}
	for wantRevision := uint64(11); wantRevision <= 12; wantRevision++ {
		select {
		case event := <-client.Lifecycle():
			if event.Revision != wantRevision {
				t.Fatalf("received revision %d, want %d", event.Revision, wantRevision)
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for lifecycle event")
		}
	}
}

func TestWebSocketClientRejectsAdmittedIdentityMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		_ = wsjson.Write(request.Context(), connection, map[string]any{
			"type": "bot:admitted", "protocolVersion": ProtocolVersion,
			"roomId": "room-other", "recordingId": "rec-1",
		})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := DialWebSocketSignalingForAttempt(
		ctx,
		"ws"+strings.TrimPrefix(server.URL, "http"),
		"one-use-secret",
		SessionIdentity{RoomID: "room-1", RecordingID: "rec-1"},
	)
	if !errors.Is(err, ErrAdmissionRejected) {
		t.Fatalf("expected admission rejection, got %v", err)
	}
}

func TestWebSocketClientRejectsMessagesOutsideAdmittedRecording(t *testing.T) {
	client := &WebSocketSignalingClient{
		identity:  SessionIdentity{RoomID: "room-1", RecordingID: "rec-1"},
		snapshots: make(chan Snapshot, 1),
		lifecycle: make(chan Event, 1),
	}
	messages := []any{
		map[string]any{
			"type": "bot:producerSnapshot", "protocolVersion": ProtocolVersion,
			"recordingId": "rec-other", "revision": 1,
			"participants": []Participant{}, "producers": []Producer{},
		},
		Event{
			Type: "bot:participantLeft", RecordingID: "rec-other",
			Revision: 2, ParticipantID: "peer-1",
		},
		Event{Type: "bot:stop", RecordingID: "rec-other", Revision: 2},
	}
	for _, message := range messages {
		raw, err := json.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.routeMessage(raw); err == nil {
			t.Fatalf("cross-recording message was accepted: %s", raw)
		}
	}
}

func TestWebSocketClientAcceptsExactlyOneSnapshotPerRequest(t *testing.T) {
	client := &WebSocketSignalingClient{
		identity:  SessionIdentity{RoomID: "room-1", RecordingID: "rec-1"},
		snapshots: make(chan Snapshot, 1),
	}
	snapshot := baseSnapshot()
	if err := client.deliverSnapshot(snapshot); err == nil {
		t.Fatal("unsolicited snapshot was accepted")
	}
	if err := client.beginSnapshotRequest(); err != nil {
		t.Fatal(err)
	}
	if err := client.deliverSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	<-client.snapshots
	if err := client.deliverSnapshot(snapshot); err == nil {
		t.Fatal("second snapshot was accepted after the first was drained")
	}
	client.finishSnapshotRequest()
}

func TestWebSocketClientHonorsRequestContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		_ = wsjson.Write(request.Context(), connection, map[string]any{
			"type": "bot:admitted", "protocolVersion": ProtocolVersion,
			"roomId": "room-1", "recordingId": "rec-1",
		})
		var ignored map[string]any
		_ = wsjson.Read(request.Context(), connection, &ignored)
		<-request.Context().Done()
	}))
	defer server.Close()
	client, err := DialWebSocketSignaling(
		context.Background(),
		"ws"+strings.TrimPrefix(server.URL, "http"),
		"one-use-secret",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	requestContext, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := client.RouterRTPCapabilities(requestContext); err == nil {
		t.Fatal("unresponsive request ignored context deadline")
	}
}

func TestWebSocketLifecycleOverflowFailsPendingRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		ctx := request.Context()
		_ = wsjson.Write(ctx, connection, map[string]any{
			"type": "bot:admitted", "protocolVersion": ProtocolVersion,
			"roomId": "room-1", "recordingId": "rec-1",
		})
		var ignored map[string]any
		if err := wsjson.Read(ctx, connection, &ignored); err != nil {
			return
		}
		for revision := 1; revision <= 300; revision++ {
			if err := wsjson.Write(ctx, connection, Event{
				Type: "bot:participantLeft", RecordingID: "rec-1",
				Revision: uint64(revision), ParticipantID: "fixture",
			}); err != nil {
				return
			}
		}
		<-ctx.Done()
	}))
	defer server.Close()
	client, err := DialWebSocketSignaling(
		context.Background(),
		"ws"+strings.TrimPrefix(server.URL, "http"),
		"one-use-secret",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.RouterRTPCapabilities(ctx); err == nil {
		t.Fatal("lifecycle overflow did not fail pending request")
	}
	select {
	case <-client.done:
	case <-ctx.Done():
		t.Fatal("lifecycle overflow did not terminate WebSocket client")
	}
}

func TestWebSocketConcurrentCloseWithLifecycleRouting(t *testing.T) {
	requestSeen := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := websocket.Accept(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.CloseNow()
		ctx := request.Context()
		if err := wsjson.Write(ctx, connection, map[string]any{
			"type": "bot:admitted", "protocolVersion": ProtocolVersion,
			"roomId": "room-1", "recordingId": "rec-1",
		}); err != nil {
			return
		}
		var ignored map[string]any
		if err := wsjson.Read(ctx, connection, &ignored); err != nil {
			return
		}
		close(requestSeen)
		writeFailed := make(chan struct{})
		go func() {
			defer close(writeFailed)
			for revision := 1; revision <= 10_000; revision++ {
				if err := wsjson.Write(ctx, connection, Event{
					Type: "bot:participantLeft", RecordingID: "rec-1",
					Revision: uint64(revision), ParticipantID: "fixture",
				}); err != nil {
					return
				}
			}
		}()
		select {
		case <-writeFailed:
		case <-time.After(2 * time.Second):
		}
	}))
	defer server.Close()

	client, err := DialWebSocketSignaling(
		context.Background(),
		"ws"+strings.TrimPrefix(server.URL, "http"),
		"one-use-secret",
	)
	if err != nil {
		t.Fatal(err)
	}
	drained := make(chan struct{})
	go func() {
		for range client.Lifecycle() {
		}
		close(drained)
	}()
	pending := make(chan error, 1)
	go func() {
		_, requestErr := client.RouterRTPCapabilities(context.Background())
		pending <- requestErr
	}()

	select {
	case <-requestSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not observe pending request")
	}
	var closes sync.WaitGroup
	for range 10 {
		closes.Add(1)
		go func() {
			defer closes.Done()
			_ = client.Close()
		}()
	}
	closes.Wait()
	select {
	case requestErr := <-pending:
		if requestErr == nil {
			t.Fatal("pending request succeeded during external close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending request did not resolve during external close")
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle channel did not close")
	}
}

func writeResponse(
	ctx context.Context,
	connection *websocket.Conn,
	requestID string,
	data any,
) {
	_ = wsjson.Write(ctx, connection, map[string]any{
		"type": "response", "requestId": requestID, "ok": true, "data": data,
	})
}
