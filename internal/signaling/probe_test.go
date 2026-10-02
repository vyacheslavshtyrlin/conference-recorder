package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestProbeControllerDrivesCameraAndScreenLifecycle(t *testing.T) {
	signaling := newFixtureSignaling()
	peer := &fixturePeer{calls: []string{}}
	controller, err := StartProbe(context.Background(), signaling, peer)
	if err != nil {
		t.Fatal(err)
	}

	events := []Event{
		{Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 11, ProducerID: "camera-1"},
		{Type: "bot:producerResumed", RecordingID: "rec-1", Revision: 12, ProducerID: "camera-1"},
		{
			Type: "bot:producerAdded", RecordingID: "rec-1", Revision: 13,
			Producer: producerPtr("screen-1", SourceScreen),
		},
		{Type: "bot:producerClosed", RecordingID: "rec-1", Revision: 14, ProducerID: "screen-1"},
		{
			Type: "bot:producerAdded", RecordingID: "rec-1", Revision: 15,
			Producer: producerPtr("screen-2", SourceScreen),
		},
	}
	for _, event := range events {
		if err := controller.Apply(context.Background(), event); err != nil {
			t.Fatalf("apply revision %d: %v", event.Revision, err)
		}
	}
	beforeDuplicate := len(signaling.calls)
	if err := controller.Apply(context.Background(), events[len(events)-1]); err != nil {
		t.Fatal(err)
	}
	if len(signaling.calls) != beforeDuplicate {
		t.Fatal("exact duplicate repeated consumer operations")
	}

	wantPrefix := []string{
		"capabilities", "create-transport", "snapshot",
		"consume:camera-1", "connect-transport", "resume:camera-1",
		"consume:mic-1", "resume:mic-1",
		"pause:camera-1", "resume:camera-1",
		"consume:screen-1", "resume:screen-1", "close:screen-1",
		"consume:screen-2", "resume:screen-2",
	}
	if !reflect.DeepEqual(signaling.calls, wantPrefix) {
		t.Fatalf("signaling calls\n got: %#v\nwant: %#v", signaling.calls, wantPrefix)
	}
	wantPeer := []string{
		"reconcile:camera-1", "pli:2222",
		"reconcile:camera-1,mic-1",
		"pli:2222",
		"reconcile:camera-1,mic-1,screen-1", "pli:3333",
		"reconcile:camera-1,mic-1",
		"reconcile:camera-1,mic-1,screen-2", "pli:4444",
	}
	if !reflect.DeepEqual(peer.calls, wantPeer) {
		t.Fatalf("peer calls\n got: %#v\nwant: %#v", peer.calls, wantPeer)
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if !peer.closed || !signaling.closed {
		t.Fatal("controller did not close signaling and Pion peer")
	}
}

func TestProbeControllerGapFailsClosedAndRollsBackConsumers(t *testing.T) {
	signaling := newFixtureSignaling()
	peer := &fixturePeer{calls: []string{}}
	controller, err := StartProbe(context.Background(), signaling, peer)
	if err != nil {
		t.Fatal(err)
	}
	err = controller.Apply(context.Background(), Event{
		Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 12, ProducerID: "camera-1",
	})
	if !errors.Is(err, ErrRevisionGap) {
		t.Fatalf("expected revision gap, got %v", err)
	}
	if !peer.closed || !signaling.closed {
		t.Fatal("gap did not terminate both peers")
	}
	if !containsCall(signaling.calls, "close:camera-1") || !containsCall(signaling.calls, "close:mic-1") {
		t.Fatalf("gap did not rollback server consumers: %#v", signaling.calls)
	}
}

func TestStartProbeRollsBackOnUnsupportedCodec(t *testing.T) {
	signaling := newFixtureSignaling()
	signaling.consumers["camera-1"] = fixtureConsumer(
		"consumer-camera", "camera-1", KindVideo, "video/AV1", 45, 90_000, 0, 2222,
	)
	peer := &fixturePeer{calls: []string{}}
	_, err := StartProbe(context.Background(), signaling, peer)
	if err == nil {
		t.Fatal("unsupported codec was accepted")
	}
	if !containsCall(signaling.calls, "close:camera-1") {
		t.Fatalf("unsupported consumer was not rolled back: %#v", signaling.calls)
	}
	if !peer.closed || !signaling.closed {
		t.Fatal("failed start did not close resources")
	}
}

type fixtureSignaling struct {
	calls     []string
	consumers map[string]Consumer
	lifecycle chan Event
	closed    bool
	closeErr  error
}

func newFixtureSignaling() *fixtureSignaling {
	return &fixtureSignaling{
		calls: []string{},
		consumers: map[string]Consumer{
			"camera-1": fixtureConsumer(
				"consumer-camera", "camera-1", KindVideo, "video/VP8", 96, 90_000, 0, 2222,
			),
			"mic-1": fixtureConsumer(
				"consumer-mic", "mic-1", KindAudio, "audio/opus", 111, 48_000, 2, 1111,
			),
			"screen-1": fixtureConsumer(
				"consumer-screen-1", "screen-1", KindVideo, "video/VP8", 96, 90_000, 0, 3333,
			),
			"screen-2": fixtureConsumer(
				"consumer-screen-2", "screen-2", KindVideo, "video/VP8", 96, 90_000, 0, 4444,
			),
		},
		lifecycle: make(chan Event, 16),
	}
}

func (fixture *fixtureSignaling) RouterRTPCapabilities(context.Context) (json.RawMessage, error) {
	fixture.calls = append(fixture.calls, "capabilities")
	return json.RawMessage(`{"codecs":[]}`), nil
}

func (fixture *fixtureSignaling) CreateRecvTransport(context.Context) (TransportOptions, error) {
	fixture.calls = append(fixture.calls, "create-transport")
	return fixtureTransport(), nil
}

func (fixture *fixtureSignaling) SyncSnapshot(context.Context) (Snapshot, error) {
	fixture.calls = append(fixture.calls, "snapshot")
	return baseSnapshot(), nil
}

func (fixture *fixtureSignaling) Consume(
	_ context.Context,
	_ string,
	producerID string,
	_ json.RawMessage,
) (Consumer, error) {
	fixture.calls = append(fixture.calls, "consume:"+producerID)
	consumer, exists := fixture.consumers[producerID]
	if !exists {
		return Consumer{}, errors.New("missing fixture consumer")
	}
	return consumer, nil
}

func (fixture *fixtureSignaling) ConnectRecvTransport(
	_ context.Context,
	_ string,
	_ DTLSParameters,
) error {
	fixture.calls = append(fixture.calls, "connect-transport")
	return nil
}

func (fixture *fixtureSignaling) ResumeConsumer(_ context.Context, producerID string) error {
	fixture.calls = append(fixture.calls, "resume:"+producerID)
	return nil
}

func (fixture *fixtureSignaling) PauseConsumer(_ context.Context, producerID string) error {
	fixture.calls = append(fixture.calls, "pause:"+producerID)
	return nil
}

func (fixture *fixtureSignaling) CloseConsumer(_ context.Context, producerID string) error {
	fixture.calls = append(fixture.calls, "close:"+producerID)
	return fixture.closeErr
}

func (fixture *fixtureSignaling) Lifecycle() <-chan Event {
	return fixture.lifecycle
}

func (fixture *fixtureSignaling) Close() error {
	fixture.closed = true
	return nil
}

func TestProbeRunReturnsTypedAuthoritativeStop(t *testing.T) {
	signaling := newFixtureSignaling()
	peer := &fixturePeer{}
	controller, err := StartProbe(context.Background(), signaling, peer)
	if err != nil {
		t.Fatal(err)
	}
	signaling.lifecycle <- Event{Type: "bot:stop"}
	err = controller.Run(context.Background())
	if !errors.Is(err, ErrAuthoritativeStop) {
		t.Fatalf("bot:stop was not preserved as a typed clean stop: %v", err)
	}
}

func TestProbeRunCancellationDefersTransportTeardownToClose(t *testing.T) {
	signaling := newFixtureSignaling()
	peer := &fixturePeer{}
	controller, err := StartProbe(context.Background(), signaling, peer)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled probe returned %v", err)
	}
	if peer.closed || signaling.closed {
		t.Fatal("root cancellation tore down capture transport before session finalization")
	}
	if err := controller.Close(); err != nil {
		t.Fatal(err)
	}
	if !peer.closed || !signaling.closed {
		t.Fatal("explicit close did not release capture transport")
	}
}

func TestAuthoritativeStopLocalTeardownIgnoresDeletedRemoteConsumers(t *testing.T) {
	signaling := newFixtureSignaling()
	signaling.closeErr = errors.New("consumer not found")
	peer := &fixturePeer{}
	controller, err := StartProbe(context.Background(), signaling, peer)
	if err != nil {
		t.Fatal(err)
	}
	signaling.lifecycle <- Event{Type: "bot:stop"}
	if err := controller.Run(context.Background()); !errors.Is(err, ErrAuthoritativeStop) {
		t.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		t.Fatalf("expected remote deletion broke local clean teardown: %v", err)
	}
	if !peer.closed || !signaling.closed {
		t.Fatal("authoritative stop did not close local peer and signaling")
	}
}

func TestProducerUpdatedIdentityRecreatesConsumerBeforeResume(t *testing.T) {
	signaling := newFixtureSignaling()
	peer := &fixturePeer{calls: []string{}}
	controller, err := StartProbe(context.Background(), signaling, peer)
	if err != nil {
		t.Fatal(err)
	}
	signaling.calls = []string{}
	peer.calls = []string{}
	updated := producerPtr("camera-1", SourceScreen)
	if err := controller.Apply(context.Background(), Event{
		Type: "bot:producerUpdated", RecordingID: "rec-1", Revision: 11, Producer: updated,
	}); err != nil {
		t.Fatal(err)
	}
	wantSignaling := []string{"close:camera-1", "consume:camera-1", "resume:camera-1"}
	if !reflect.DeepEqual(signaling.calls, wantSignaling) {
		t.Fatalf("producer rebind calls=%v want=%v", signaling.calls, wantSignaling)
	}
	wantPeer := []string{"reconcile:mic-1", "reconcile:mic-1,camera-1", "pli:2222"}
	if !reflect.DeepEqual(peer.calls, wantPeer) {
		t.Fatalf("producer rebind peer calls=%v want=%v", peer.calls, wantPeer)
	}
}

type fixturePeer struct {
	calls  []string
	closed bool
}

func (fixture *fixturePeer) ReconcileConsumers(
	_ context.Context,
	_ TransportOptions,
	consumers []Consumer,
) (string, DTLSParameters, error) {
	producerIDs := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		producerIDs = append(producerIDs, consumer.ProducerID)
	}
	fixture.calls = append(fixture.calls, "reconcile:"+strings.Join(producerIDs, ","))
	return "answer", DTLSParameters{
		Role:         "client",
		Fingerprints: []DTLSFingerprint{{Algorithm: "sha-256", Value: "AA"}},
	}, nil
}

func (fixture *fixturePeer) RequestKeyFrame(_ context.Context, ssrc uint32) error {
	fixture.calls = append(fixture.calls, "pli:"+strconv.FormatUint(uint64(ssrc), 10))
	return nil
}

func (fixture *fixturePeer) Close() error {
	fixture.closed = true
	return nil
}

func producerPtr(producerID string, source Source) *Producer {
	return &Producer{
		ProducerID: producerID, ParticipantID: "peer-alice", DisplayName: "Alice",
		Kind: KindVideo, Source: source,
	}
}

func containsCall(calls []string, expected string) bool {
	for _, call := range calls {
		if call == expected {
			return true
		}
	}
	return false
}
