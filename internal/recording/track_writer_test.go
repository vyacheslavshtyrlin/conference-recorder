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
	eof     chan struct{}
	readErr error
}

type remoteTrackTimeout struct{}

func (remoteTrackTimeout) Error() string { return "i/o timeout" }
func (remoteTrackTimeout) Timeout() bool { return true }

type packetAfterDeadlineTrack struct {
	packet  *rtp.Packet
	release chan struct{}
	once    sync.Once
}

func (track *packetAfterDeadlineTrack) ReadRTP() (*rtp.Packet, error) {
	<-track.release
	return track.packet, nil
}

func (track *packetAfterDeadlineTrack) SetReadDeadline(time.Time) error {
	track.once.Do(func() { close(track.release) })
	return nil
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
	if track.eof != nil {
		select {
		case track.eof <- struct{}{}:
		default:
		}
	}
	if track.readErr != nil {
		return nil, track.readErr
	}
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

func TestRTPTrackWriterNormalizesSignalingMIMECodec(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{release: make(chan struct{})}
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: func() int64 { return 0 }, Muxer: copyMuxer{},
		OperationTimeout: time.Second,
		OnSegment:        func(context.Context, SegmentDescriptor, string) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if writer.codec != "vp8" {
		t.Fatalf("signaling MIME codec was not normalized: %q", writer.codec)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRTPTrackWriterRequestsKeyFrameBeforeMaximumSegmentDuration(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{
		release: make(chan struct{}),
		packets: []*rtp.Packet{
			vp8Packet(1, true),
			vp8Packet(2, false),
			vp8Packet(3, true),
		},
	}
	clockValues := []int64{0, 1100, 1200}
	clock := func() int64 {
		value := clockValues[0]
		if len(clockValues) > 1 {
			clockValues = clockValues[1:]
		}
		return value
	}
	keyFrameRequested := make(chan struct{}, 1)
	segments := make(chan SegmentDescriptor, 2)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: clock, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		RequestKeyFrame: func(context.Context) error {
			keyFrameRequested <- struct{}{}
			return nil
		},
		OnSegment: func(_ context.Context, segment SegmentDescriptor, _ string) error {
			segments <- segment
			return nil
		},
		newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
			return packetCaptureWriter{path: path, payloads: &[][]byte{}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-keyFrameRequested:
	case <-time.After(time.Second):
		t.Fatal("video writer did not request a key frame at the segment boundary")
	}
	select {
	case segment := <-segments:
		if segment.StartMS != 0 || segment.EndMS != 1200 {
			t.Fatalf("unexpected rotated segment: %+v", segment)
		}
	case <-time.After(time.Second):
		t.Fatal("key frame did not rotate the video segment")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := writer.CloseAt(ctx, 1300); err != nil {
		t.Fatal(err)
	}
}

func TestRTPTrackWriterTreatsRemoteEOFAsCleanTrackEnd(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{
		release: make(chan struct{}),
		eof:     make(chan struct{}, 1),
		packets: []*rtp.Packet{vp8Packet(1, true)},
	}
	var clockCalls int
	clock := func() int64 {
		clockCalls++
		if clockCalls == 1 {
			return 0
		}
		return 500
	}
	segmentReady := make(chan SegmentDescriptor, 1)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: clock, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		OnSegment: func(_ context.Context, descriptor SegmentDescriptor, _ string) error {
			segmentReady <- descriptor
			return nil
		},
		newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
			return packetCaptureWriter{path: path, payloads: &[][]byte{}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(fixture.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-fixture.eof:
	case <-ctx.Done():
		t.Fatal("remote track did not reach EOF")
	}
	if err := writer.CloseAt(ctx, 750); err != nil {
		t.Fatalf("remote EOF failed the track writer: %v", err)
	}
	select {
	case segment := <-segmentReady:
		if segment.StartMS != 0 || segment.EndMS != 750 {
			t.Fatalf("unexpected EOF segment boundary: %+v", segment)
		}
	case <-ctx.Done():
		t.Fatal("remote EOF did not close the current segment")
	}
}

func TestRTPTrackWriterWaitsForAuthoritativeBoundaryAfterRemoteTimeout(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{
		release: make(chan struct{}),
		eof:     make(chan struct{}, 1),
		packets: []*rtp.Packet{vp8Packet(1, true)},
		readErr: remoteTrackTimeout{},
	}
	segmentReady := make(chan SegmentDescriptor, 1)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: func() int64 { return 500 }, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		OnSegment: func(_ context.Context, descriptor SegmentDescriptor, _ string) error {
			segmentReady <- descriptor
			return nil
		},
		newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
			return packetCaptureWriter{path: path, payloads: &[][]byte{}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(fixture.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-fixture.eof:
	case <-ctx.Done():
		t.Fatal("remote track did not reach its timeout")
	}
	select {
	case err := <-writer.Errors():
		t.Fatalf("remote timeout escaped before the authoritative boundary: %v", err)
	default:
	}
	if err := writer.CloseAt(ctx, 750); err != nil {
		t.Fatalf("remote timeout failed the track writer: %v", err)
	}
	select {
	case segment := <-segmentReady:
		if segment.StartMS != 500 || segment.EndMS != 750 {
			t.Fatalf("unexpected timeout segment boundary: %+v", segment)
		}
	case <-ctx.Done():
		t.Fatal("remote timeout did not close at the authoritative boundary")
	}
}

func TestRTPTrackWriterDeclaresGapBeforeFirstCapturablePacket(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{
		release: make(chan struct{}),
		eof:     make(chan struct{}, 1),
		packets: []*rtp.Packet{vp8Packet(1, true)},
	}
	type gap struct{ startMS, endMS int64 }
	gaps := make(chan gap, 1)
	segmentReady := make(chan SegmentDescriptor, 1)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: func() int64 { return 250 }, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		CoverageStartMS: 100,
		OnInitialGap: func(startMS, endMS int64) error {
			gaps <- gap{startMS: startMS, endMS: endMS}
			return nil
		},
		OnSegment: func(_ context.Context, descriptor SegmentDescriptor, _ string) error {
			segmentReady <- descriptor
			return nil
		},
		newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
			return packetCaptureWriter{path: path, payloads: &[][]byte{}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(fixture.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-fixture.eof:
	case <-ctx.Done():
		t.Fatal("remote track did not reach EOF")
	}
	if err := writer.CloseAt(ctx, 750); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gaps:
		if got.startMS != 100 || got.endMS != 250 {
			t.Fatalf("unexpected initial source gap: %+v", got)
		}
	default:
		t.Fatal("initial source-unavailable gap was not declared")
	}
	select {
	case segment := <-segmentReady:
		if segment.StartMS != 250 || segment.EndMS != 750 {
			t.Fatalf("unexpected segment boundary: %+v", segment)
		}
	default:
		t.Fatal("segment was not delivered")
	}
}

func TestRTPTrackWriterDeclaresGapWhenTrackEndsWithoutMedia(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{release: make(chan struct{}), eof: make(chan struct{}, 1)}
	type gap struct{ startMS, endMS int64 }
	gaps := make(chan gap, 1)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: func() int64 { return 500 }, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		CoverageStartMS: 100,
		OnInitialGap: func(startMS, endMS int64) error {
			gaps <- gap{startMS: startMS, endMS: endMS}
			return nil
		},
		OnSegment: func(context.Context, SegmentDescriptor, string) error {
			t.Fatal("empty track produced a segment")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	close(fixture.release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case <-fixture.eof:
	case <-ctx.Done():
		t.Fatal("remote track did not reach EOF")
	}
	if err := writer.CloseAt(ctx, 750); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gaps:
		if got.startMS != 100 || got.endMS != 750 {
			t.Fatalf("unexpected empty-track gap: %+v", got)
		}
	default:
		t.Fatal("empty active track gap was not declared")
	}
}

func TestRTPTrackWriterResolvesPendingGapWhenPacketArrivesAfterClose(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	track := &packetAfterDeadlineTrack{
		packet:  vp8Packet(1, false),
		release: make(chan struct{}),
	}
	type gap struct{ startMS, endMS int64 }
	gaps := make(chan gap, 1)
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: track, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: t.TempDir(),
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: func() int64 { return 500 }, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		CoverageStartMS: 100,
		OnInitialGap: func(startMS, endMS int64) error {
			gaps <- gap{startMS: startMS, endMS: endMS}
			return nil
		},
		OnSegment: func(context.Context, SegmentDescriptor, string) error {
			t.Fatal("packet returned after close produced a segment")
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := writer.CloseAt(ctx, 750); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-gaps:
		if got.startMS != 100 || got.endMS != 750 {
			t.Fatalf("unexpected post-close packet gap: %+v", got)
		}
	default:
		t.Fatal("post-close packet left pending coverage unresolved")
	}
}

func TestRTPTrackWriterCleansUpWhenInitialGapCallbackFails(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, err := TrackID("rec-1", participantID, SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &trackFixture{
		release: make(chan struct{}),
		packets: []*rtp.Packet{vp8Packet(1, true)},
	}
	root := t.TempDir()
	injected := errors.New("persist initial gap")
	writer, err := StartRTPTrackWriter(context.Background(), RTPTrackWriterConfig{
		Track: fixture, ParticipantInstanceID: participantID, TrackID: trackID,
		Source: SourceCamera, Codec: "video/VP8", SpoolDirectory: root,
		SegmentDuration: time.Second, MaximumSegmentDuration: 3 * time.Second,
		TimelineClock: func() int64 { return 250 }, Muxer: copyMuxer{}, OperationTimeout: time.Second,
		CoverageStartMS: 100,
		OnInitialGap: func(int64, int64) error {
			return injected
		},
		OnSegment: func(context.Context, SegmentDescriptor, string) error { return nil },
		newPacketWriter: func(_, path string, _ *rtp.Packet) (packetWriter, error) {
			if err := os.WriteFile(path, []byte{1}, 0o600); err != nil {
				return nil, err
			}
			return packetCaptureWriter{path: path, payloads: &[][]byte{}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-writer.Errors():
		if !errors.Is(got, injected) {
			t.Fatalf("initial gap error was not preserved: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("initial gap failure was not reported")
	}
	matches, err := filepath.Glob(filepath.Join(root, "*.part"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("initial gap failure leaked staging files: %v", matches)
	}
}

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
