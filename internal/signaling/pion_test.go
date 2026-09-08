package signaling

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

func TestPionAcceptsMediasoupICELiteOffer(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	answer, dtls, err := session.AcceptConsumers(
		ctx,
		fixtureTransport(),
		[]Consumer{
			loadConsumerFixture(t, "testdata/consumer-opus.json", SourceMic),
			loadConsumerFixture(t, "testdata/consumer-vp8-rtx.json", SourceCamera),
			loadConsumerFixture(t, "testdata/consumer-h264-rtx.json", SourceScreen),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(answer, "a=ice-ufrag:") {
		t.Fatal("Pion answer did not contain local ICE parameters")
	}
	if dtls.Role != "client" || len(dtls.Fingerprints) == 0 {
		t.Fatalf("unexpected DTLS parameters: %+v", dtls)
	}
}

func TestCapturedMediasoupConsumerShapePreservesRTXAndRTCP(t *testing.T) {
	consumer := loadConsumerFixture(t, "testdata/consumer-vp8-rtx.json", SourceCamera)
	offer, err := BuildMediasoupOffer(fixtureTransport(), []Consumer{consumer})
	if err != nil {
		t.Fatal(err)
	}
	expectedLines := []string{
		"m=video 7 UDP/TLS/RTP/SAVPF 101 102",
		"a=rtpmap:102 rtx/90000",
		"a=fmtp:102 apt=101",
		"a=rtcp-fb:101 nack pli",
		"a=extmap:5 http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01",
		"a=rtcp-rsize",
		"a=ssrc:22222222 cname:mediasoup-vp8",
		"a=ssrc-group:FID 22222222 22222223",
	}
	for _, line := range expectedLines {
		if !strings.Contains(offer, line) {
			t.Fatalf("offer is missing %q\n%s", line, offer)
		}
	}
}

func TestPionRenegotiatesClosedScreenAndReplacementWithStableMIDs(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	camera := loadConsumerFixture(t, "testdata/consumer-vp8-rtx.json", SourceCamera)
	screen := loadConsumerFixture(t, "testdata/consumer-h264-rtx.json", SourceScreen)
	if _, _, err := session.ReconcileConsumers(ctx, fixtureTransport(), []Consumer{camera, screen}); err != nil {
		t.Fatal(err)
	}
	replacement := screen
	replacement.ID = "consumer-h264-replacement"
	replacement.ProducerID = "screen-2"
	replacement.RTPParameters.MID = "3"
	replacement.RTPParameters.Encodings[0].SSRC = 44444444
	if _, _, err := session.ReconcileConsumers(ctx, fixtureTransport(), []Consumer{camera, replacement}); err != nil {
		t.Fatal(err)
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	if !session.slots["screen-1"].Closed {
		t.Fatal("removed screen MID was not retained as a rejected m-line")
	}
	if session.slots["screen-2"].Closed || session.slots["screen-2"].RTPParameters.MID != "3" {
		t.Fatal("replacement screen did not receive its stable new MID")
	}
}

func TestPionRejectsInvalidConsumerBeforePublishingState(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	consumer := loadConsumerFixture(t, "testdata/consumer-vp8-rtx.json", SourceCamera)
	consumer.RTPParameters.Encodings = nil
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, _, err := session.ReconcileConsumers(ctx, fixtureTransport(), []Consumer{consumer}); err == nil {
		t.Fatal("invalid consumer was accepted")
	}
	if len(session.slots) != 0 || len(session.order) != 0 {
		t.Fatal("invalid consumer was published before validation")
	}
}

func TestPionMediaSectionCapacityFailsClosed(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	for index := range maxPionMediaSections {
		producerID := fmt.Sprintf("producer-%d", index)
		session.order = append(session.order, producerID)
		session.slots[producerID] = Consumer{ProducerID: producerID, Closed: true}
	}
	consumer := loadConsumerFixture(t, "testdata/consumer-vp8-rtx.json", SourceCamera)
	if _, _, err := session.ReconcileConsumers(context.Background(), fixtureTransport(), []Consumer{consumer}); err == nil {
		t.Fatal("media-section capacity was not enforced")
	}
}

func TestPionFailedConnectionReportsTerminalError(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	session.connectionStateChanged(webrtc.PeerConnectionStateFailed)
	select {
	case peerErr := <-session.Errors():
		if peerErr == nil || !strings.Contains(peerErr.Error(), "failed") {
			t.Fatalf("unexpected connection failure: %v", peerErr)
		}
	case <-time.After(time.Second):
		t.Fatal("failed connection did not report a terminal error")
	}
}

func TestPionIgnoresStaleDisconnectedGraceCallback(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	session.connectionStateChanged(webrtc.PeerConnectionStateDisconnected)
	firstGeneration := session.disconnectGeneration
	session.connectionStateChanged(webrtc.PeerConnectionStateConnected)
	session.connectionStateChanged(webrtc.PeerConnectionStateDisconnected)
	currentTimer := session.disconnect
	session.failDisconnected(firstGeneration)
	select {
	case peerErr := <-session.Errors():
		t.Fatalf("stale disconnect callback failed the current connection: %v", peerErr)
	default:
	}
	if session.disconnect != currentTimer || session.disconnectGeneration == firstGeneration {
		t.Fatal("stale disconnect callback cleared the current grace timer")
	}
}

func TestPionReceivesSupportedRTPAndVideoPLI(t *testing.T) {
	cases := []struct {
		name        string
		capability  webrtc.RTPCodecCapability
		payloadType uint8
		video       bool
	}{
		{
			name: "opus", payloadType: 111,
			capability: webrtc.RTPCodecCapability{
				MimeType: webrtc.MimeTypeOpus, ClockRate: 48_000, Channels: 2,
			},
		},
		{
			name: "vp8", payloadType: 96, video: true,
			capability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90_000},
		},
		{
			name: "h264", payloadType: 102, video: true,
			capability: webrtc.RTPCodecCapability{
				MimeType: webrtc.MimeTypeH264, ClockRate: 90_000,
				SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
			},
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			testPionRTPAndPLI(t, testCase.capability, testCase.payloadType, testCase.video)
		})
	}
}

func TestPionSessionDeliversCorrelatedTrackAndPLI(t *testing.T) {
	sender, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sender.Close()
		_ = session.Close()
	})
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90_000},
		"camera",
		"fixture",
	)
	if err != nil {
		t.Fatal(err)
	}
	rtpSender, err := sender.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	negotiatePeerConnections(t, sender, session.connection)
	parameters := rtpSender.GetParameters()
	if len(parameters.Encodings) != 1 {
		t.Fatalf("unexpected sender encodings: %+v", parameters.Encodings)
	}
	ssrc := uint32(parameters.Encodings[0].SSRC)
	session.mu.Lock()
	session.slots["camera-1"] = Consumer{
		ProducerID: "camera-1", Kind: KindVideo, Source: SourceCamera,
		RTPParameters: RTPParameters{
			MID: "0", Encodings: []RTPEncodingParameters{{SSRC: ssrc}},
		},
	}
	session.order = append(session.order, "camera-1")
	session.mu.Unlock()
	select {
	case <-session.connected:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for PionSession connection")
	}

	if err := track.WriteRTP(&rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: 1,
			Timestamp: 1, SSRC: ssrc, Marker: true,
		},
		Payload: []byte{0x10, 0x00, 0x00, 0x00},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var observed TrackEvent
	select {
	case observed = <-session.Tracks():
	case <-ctx.Done():
		t.Fatal("timed out waiting for correlated Pion track")
	}
	if observed.ProducerID != "camera-1" || observed.Source != SourceCamera {
		t.Fatalf("unexpected track correlation: %+v", observed)
	}

	pliReceived := make(chan struct{}, 1)
	go func() {
		packets, _, readErr := rtpSender.ReadRTCP()
		if readErr != nil {
			return
		}
		for _, packet := range packets {
			if _, ok := packet.(*rtcp.PictureLossIndication); ok {
				pliReceived <- struct{}{}
				return
			}
		}
	}()
	if err := session.RequestKeyFrame(ctx, ssrc); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pliReceived:
	case <-ctx.Done():
		t.Fatal("timed out waiting for correlated Pion PLI")
	}
}

func TestPionTrackDeliveryOverflowFailsWithoutBlocking(t *testing.T) {
	session, err := NewPionSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	for index := 0; index < cap(session.tracks)+1; index++ {
		session.enqueueTrack(TrackEvent{Source: SourceCamera})
	}
	select {
	case err := <-session.Errors():
		if err == nil || !strings.Contains(err.Error(), "backpressure") {
			t.Fatalf("unexpected overflow error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("track overflow blocked instead of failing explicitly")
	}
	select {
	case <-session.done:
	case <-time.After(time.Second):
		t.Fatal("track overflow did not terminate Pion session")
	}
}

func testPionRTPAndPLI(
	t *testing.T,
	capability webrtc.RTPCodecCapability,
	payloadType uint8,
	video bool,
) {
	t.Helper()
	sender, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = sender.Close()
		_ = receiver.Close()
	})

	track, err := webrtc.NewTrackLocalStaticRTP(capability, "track", "fixture")
	if err != nil {
		t.Fatal(err)
	}
	rtpSender, err := sender.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan *webrtc.TrackRemote, 1)
	connected := make(chan struct{}, 2)
	sender.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			connected <- struct{}{}
		}
	})
	receiver.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			connected <- struct{}{}
		}
	})
	receiver.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		received <- remote
	})
	negotiatePeerConnections(t, sender, receiver)
	for range 2 {
		select {
		case <-connected:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for peer connection")
		}
	}

	packet := &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: payloadType, SequenceNumber: 1,
			Timestamp: 1, SSRC: 4242, Marker: true,
		},
		Payload: []byte{0x10, 0x00, 0x00, 0x00},
	}
	if err := track.WriteRTP(packet); err != nil {
		t.Fatal(err)
	}

	var remote *webrtc.TrackRemote
	select {
	case remote = <-received:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Pion remote track")
	}
	if !strings.EqualFold(remote.Codec().MimeType, capability.MimeType) {
		t.Fatalf("received codec %q, want %q", remote.Codec().MimeType, capability.MimeType)
	}
	if _, _, err := remote.ReadRTP(); err != nil {
		t.Fatal(err)
	}
	if !video {
		return
	}

	pliReceived := make(chan struct{}, 1)
	go func() {
		packets, _, readErr := rtpSender.ReadRTCP()
		if readErr != nil {
			return
		}
		for _, rtcpPacket := range packets {
			if _, ok := rtcpPacket.(*rtcp.PictureLossIndication); ok {
				pliReceived <- struct{}{}
				return
			}
		}
	}()
	if err := receiver.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: uint32(remote.SSRC())},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pliReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Pion PLI")
	}
}

func negotiatePeerConnections(
	t *testing.T,
	offerer *webrtc.PeerConnection,
	answerer *webrtc.PeerConnection,
) {
	t.Helper()
	offer, err := offerer.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	offerGathered := webrtc.GatheringCompletePromise(offerer)
	if err := offerer.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	waitGathering(t, offerGathered)
	if err := answerer.SetRemoteDescription(*offerer.LocalDescription()); err != nil {
		t.Fatal(err)
	}
	answer, err := answerer.CreateAnswer(nil)
	if err != nil {
		t.Fatal(err)
	}
	answerGathered := webrtc.GatheringCompletePromise(answerer)
	if err := answerer.SetLocalDescription(answer); err != nil {
		t.Fatal(err)
	}
	waitGathering(t, answerGathered)
	if err := offerer.SetRemoteDescription(*answerer.LocalDescription()); err != nil {
		t.Fatal(err)
	}
}

func waitGathering(t *testing.T, gatheringComplete <-chan struct{}) {
	t.Helper()
	select {
	case <-gatheringComplete:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ICE gathering")
	}
}

func fixtureTransport() TransportOptions {
	return TransportOptions{
		ID: "transport-1",
		ICEParameters: ICEParameters{
			UsernameFragment: "mediasoup", Password: "mediasoup-password-1234", ICELite: true,
		},
		ICECandidates: []ICECandidate{{
			Foundation: "udpcandidate", Priority: 1_000_000,
			IP: "127.0.0.1", Protocol: "udp", Port: 44444, Type: "host",
		}},
		DTLSParameters: DTLSParameters{
			Role: "auto",
			Fingerprints: []DTLSFingerprint{{
				Algorithm: "sha-256", Value: strings.Repeat("AA:", 31) + "AA",
			}},
		},
	}
}

func fixtureConsumer(
	id string,
	producerID string,
	kind MediaKind,
	mimeType string,
	payloadType uint8,
	clockRate uint32,
	channels uint16,
	ssrc uint32,
) Consumer {
	return Consumer{
		ID: id, ProducerID: producerID, Kind: kind, Paused: true,
		RTPParameters: RTPParameters{
			MID: producerID,
			Codecs: []RTPCodecParameters{{
				MimeType: mimeType, PayloadType: payloadType,
				ClockRate: clockRate, Channels: channels,
			}},
			Encodings: []RTPEncodingParameters{{SSRC: ssrc}},
			RTCP:      RTCPParameters{CNAME: "fixture", ReducedSize: true},
		},
	}
}

func loadConsumerFixture(t *testing.T, path string, source Source) Consumer {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var consumer Consumer
	if err := json.Unmarshal(data, &consumer); err != nil {
		t.Fatal(err)
	}
	consumer.Source = source
	return consumer
}

func fixtureH264Consumer() Consumer {
	consumer := fixtureConsumer(
		"consumer-h264", "screen-1", KindVideo, "video/H264", 102, 90_000, 0, 3333,
	)
	consumer.RTPParameters.Codecs[0].Parameters = map[string]any{
		"level-asymmetry-allowed": float64(1),
		"packetization-mode":      float64(1),
		"profile-level-id":        "42e01f",
	}
	consumer.RTPParameters.MID = "screen"
	return consumer
}

func ExampleBuildMediasoupOffer() {
	offer, err := BuildMediasoupOffer(
		fixtureTransport(),
		[]Consumer{fixtureConsumer(
			"consumer", "camera", KindVideo, "video/VP8", 96, 90_000, 0, 2222,
		)},
	)
	fmt.Println(err == nil, strings.Contains(offer, "a=ice-lite"))
	// Output: true true
}
