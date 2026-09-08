package recording

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
)

type trackFixture struct {
	mu      sync.Mutex
	packets []*rtp.Packet
	release chan struct{}
}

func (track *trackFixture) ReadRTP() (*rtp.Packet, error) {
	track.mu.Lock()
	if len(track.packets) > 0 {
		packet := track.packets[0]
		track.packets = track.packets[1:]
		track.mu.Unlock()
		return packet, nil
	}
	track.mu.Unlock()
	<-track.release
	return nil, io.EOF
}

func (track *trackFixture) SetReadDeadline(time.Time) error {
	select {
	case <-track.release:
	default:
		close(track.release)
	}
	return nil
}

type copyMuxer struct{}

func (copyMuxer) Mux(_ context.Context, request MuxRequest) error {
	bytes, err := os.ReadFile(request.Intermediate)
	if err != nil {
		return err
	}
	return os.WriteFile(request.Output, bytes, 0o600)
}

type packetCaptureWriter struct {
	path     string
	payloads *[][]byte
}

func (writer packetCaptureWriter) WriteRTP(packet *rtp.Packet) error {
	*writer.payloads = append(*writer.payloads, append([]byte{}, packet.Payload...))
	return os.WriteFile(writer.path, []byte{1}, 0o600)
}

func (writer packetCaptureWriter) Close() error { return nil }

func TestRTPTrackWriterCreatesIndependentClosedVP8Segments(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{release: make(chan struct{}), packets: []*rtp.Packet{
		vp8Packet(1, true), vp8Packet(2, false), vp8Packet(3, true), vp8Packet(4, false),
	}}
	clockValues := []int64{0, 500, 1100, 1500, 2000}
	var clockMu sync.Mutex
	clock := func() int64 {
		clockMu.Lock()
		defer clockMu.Unlock()
		value := clockValues[0]
		if len(clockValues) > 1 {
			clockValues = clockValues[1:]
		}
		return value
	}
	var segments []SegmentDescriptor
	var paths []string
	ready := make(chan struct{}, 1)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "vp8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: clock, Muxer: copyMuxer{},
		OperationTimeout: time.Second,
		OnSegment: func(_ context.Context, descriptor SegmentDescriptor, path string) error {
			segments = append(segments, descriptor)
			paths = append(paths, path)
			select {
			case ready <- struct{}{}:
			default:
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("first segment was not rotated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(segments) != 2 || segments[0].Sequence != 0 || segments[1].Sequence != 1 || segments[0].EndMS != 1100 || segments[1].StartMS != 1100 {
		t.Fatalf("unexpected independent segments: %+v", segments)
	}
	for _, path := range paths {
		if filepath.Ext(path) != ".part" {
			t.Fatalf("segment was not staged: %s", path)
		}
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			t.Fatalf("closed segment missing: %v", err)
		}
	}
}

func TestRTPTrackWriterReportsTerminalSegmentFailureImmediately(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, _ := TrackID("rec-1", participantID, SourceCamera)
	fixture := &trackFixture{release: make(chan struct{}), packets: []*rtp.Packet{
		vp8Packet(1, true), vp8Packet(2, true),
	}}
	clockValues := []int64{0, 1100}
	clock := func() int64 {
		value := clockValues[0]
		clockValues = clockValues[1:]
		return value
	}
	injected := errors.New("upload unavailable")
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "vp8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: clock, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		OnSegment: func(context.Context, SegmentDescriptor, string) error { return injected },
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case failure := <-writer.Errors():
		if !errors.Is(failure, injected) {
			t.Fatalf("unexpected terminal writer failure: %v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal writer failure was not surfaced")
	}
}

func TestKeyFrameDetectionCoversVP8AndH264(t *testing.T) {
	if !isKeyFrame("vp8", vp8Payload(true)) || isKeyFrame("vp8", vp8Payload(false)) {
		t.Fatal("VP8 keyframe detection failed")
	}
	if !isKeyFrame("h264", []byte{0x65, 1}) || !isKeyFrame("h264", []byte{0x7c, 0x85, 1}) || isKeyFrame("h264", []byte{0x41, 1}) {
		t.Fatal("H264 keyframe detection failed")
	}
}

func TestH264ParameterSetsHandleSingleSTAPAAndFUA(t *testing.T) {
	sets := h264ParameterSets{}
	sets.Observe(h264Packet(1, []byte{0x67, 0x11}))
	sets.Observe(h264Packet(2, []byte{0x68, 0x22}))
	if !sets.Complete() {
		t.Fatal("single NAL parameter sets were not retained")
	}

	stap := []byte{0x78, 0, 2, 0x67, 0x33, 0, 2, 0x68, 0x44, 0, 2, 0x65, 0x55}
	sets.Observe(h264Packet(3, stap))
	if !isKeyFrame("h264", stap) {
		t.Fatal("STAP-A IDR was not detected")
	}
	if isKeyFrame("h264", append(append([]byte{}, stap...), 0xff)) {
		t.Fatal("malformed STAP-A was accepted as an IDR")
	}
	prefix := sets.Prefix(h264Packet(4, []byte{0x65, 1}))
	if len(prefix) != 2 || prefix[0].Payload[1] != 0x33 || prefix[1].Payload[1] != 0x44 {
		t.Fatalf("STAP-A parameter sets were not retained: %+v", prefix)
	}

	sets.Observe(h264Packet(10, []byte{0x7c, 0x87, 0xaa}))
	sets.Observe(h264Packet(11, []byte{0x7c, 0x47, 0xbb}))
	prefix = sets.Prefix(h264Packet(12, []byte{0x65, 1}))
	if len(prefix) != 2 || string(prefix[0].Payload) != string([]byte{0x67, 0xaa, 0xbb}) {
		t.Fatalf("FU-A SPS was not reconstructed: %x", prefix[0].Payload)
	}

	sets.Observe(h264Packet(20, []byte{0x7c, 0x88, 0xcc}))
	sets.Observe(h264Packet(22, []byte{0x7c, 0x48, 0xdd}))
	prefix = sets.Prefix(h264Packet(23, []byte{0x65, 1}))
	if string(prefix[1].Payload) != string([]byte{0x68, 0x44}) {
		t.Fatalf("discontinuous FU-A replaced the last complete PPS: %x", prefix[1].Payload)
	}
}

func TestH264ParameterSetFragmentAssemblyIsBounded(t *testing.T) {
	sets := h264ParameterSets{}
	sets.observeFUA(1, append([]byte{0x7c, 0x87}, make([]byte, maximumH264ParameterSetBytes/2)...))
	sets.observeFUA(2, append([]byte{0x7c, 0x07}, make([]byte, maximumH264ParameterSetBytes/2+1)...))
	if sets.fragment != nil || sets.Complete() {
		t.Fatal("oversized H264 parameter-set fragment remained buffered")
	}
}

func TestH264SegmentsPrependLatestParameterSetsAcrossRotation(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{release: make(chan struct{}), packets: []*rtp.Packet{
		h264Packet(1, []byte{0x67, 0x11}),
		h264Packet(2, []byte{0x68, 0x22}),
		h264Packet(3, []byte{0x65, 0x31}),
		h264Packet(4, []byte{0x41, 0x41}),
		h264Packet(5, []byte{0x65, 0x51}),
	}}
	clockValues := []int64{0, 0, 0, 500, 1100, 1500}
	clock := func() int64 {
		value := clockValues[0]
		if len(clockValues) > 1 {
			clockValues = clockValues[1:]
		}
		return value
	}
	packetSegments := [][][]byte{}
	factory := func(_ string, path string, _ *rtp.Packet) (packetWriter, error) {
		packetSegments = append(packetSegments, [][]byte{})
		return packetCaptureWriter{path: path, payloads: &packetSegments[len(packetSegments)-1]}, nil
	}
	ready := make(chan struct{}, 2)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "h264", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: clock, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		newPacketWriter: factory,
		OnSegment: func(context.Context, SegmentDescriptor, string) error {
			ready <- struct{}{}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("H264 segment was not rotated")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if len(packetSegments) != 2 {
		t.Fatalf("unexpected H264 segment count: %d", len(packetSegments))
	}
	for index, segment := range packetSegments {
		if len(segment) < 3 || segment[0][0]&0x1f != 7 || segment[1][0]&0x1f != 8 || segment[2][0]&0x1f != 5 {
			t.Fatalf("segment %d is not independently decodable: %x", index, segment)
		}
	}
}

func h264Packet(sequence uint16, payload []byte) *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{SequenceNumber: sequence, Timestamp: uint32(sequence) * 3000, Marker: true},
		Payload: payload,
	}
}

func vp8Packet(sequence uint16, keyFrame bool) *rtp.Packet {
	return &rtp.Packet{Header: rtp.Header{SequenceNumber: sequence, Timestamp: uint32(sequence) * 3000, Marker: true}, Payload: vp8Payload(keyFrame)}
}

func vp8Payload(keyFrame bool) []byte {
	frameType := byte(1)
	if keyFrame {
		frameType = 0
	}
	return []byte{0x10, frameType, 0, 0, 0x9d, 0x01, 0x2a, 0x80, 0x02, 0xe0, 0x01, 1, 2, 3}
}
