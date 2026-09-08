package recording

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"conference/recording-bot/internal/signaling"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4/pkg/media/h264writer"
	"github.com/pion/webrtc/v4/pkg/media/ivfwriter"
	"github.com/pion/webrtc/v4/pkg/media/oggwriter"
)

type RTPTrackSource interface {
	ReadRTP() (*rtp.Packet, error)
	SetReadDeadline(time.Time) error
}

type packetWriter interface {
	WriteRTP(*rtp.Packet) error
	Close() error
}

type packetWriterFactory func(codec, path string, first *rtp.Packet) (packetWriter, error)

type SegmentReady func(context.Context, SegmentDescriptor, string) error

type StagingBudget interface {
	ResizeStaging(recordingID, path string, size int64) error
	ReleaseStaging(recordingID, path string)
}

type RTPTrackWriter struct {
	track               RTPTrackSource
	participantID       string
	trackID             string
	source              signaling.Source
	codec               string
	spoolDirectory      string
	segmentDuration     time.Duration
	maximumDuration     time.Duration
	clock               func() int64
	muxer               SegmentMuxer
	ready               SegmentReady
	newPacketWriter     packetWriterFactory
	initialSequence     int32
	operationTimeout    time.Duration
	maximumStorageBytes int64
	recordingID         string
	stagingBudget       StagingBudget
	stagingID           string
	cancel              context.CancelFunc
	processingContext   context.Context
	cancelProcessing    context.CancelFunc
	done                chan struct{}
	errors              chan error
	mu                  sync.Mutex
	result              error
	closeAtMS           *int64
}

type closedTrackSegment struct {
	intermediate string
	startMS      int64
	endMS        int64
	sequence     int32
}

const trackSegmentQueueSize = 64

type RTPTrackWriterConfig struct {
	Track                  RTPTrackSource
	ParticipantInstanceID  string
	TrackID                string
	Source                 signaling.Source
	Codec                  string
	SpoolDirectory         string
	SegmentDuration        time.Duration
	MaximumSegmentDuration time.Duration
	TimelineClock          func() int64
	Muxer                  SegmentMuxer
	OnSegment              SegmentReady
	InitialSequence        int32
	OperationTimeout       time.Duration
	MaximumStorageBytes    int64
	RecordingID            string
	StagingBudget          StagingBudget
	newPacketWriter        packetWriterFactory
}

func StartRTPTrackWriter(ctx context.Context, config RTPTrackWriterConfig) (*RTPTrackWriter, error) {
	if config.MaximumStorageBytes == 0 {
		config.MaximumStorageBytes = 2_000_000_000
	}
	if config.StagingBudget == nil {
		config.StagingBudget = newMemoryStagingBudget(config.MaximumStorageBytes)
	}
	if config.RecordingID == "" {
		config.RecordingID = "local"
	}
	invalidIdentity := !validOpaqueID(config.ParticipantInstanceID, "pi_") ||
		!validOpaqueID(config.TrackID, "trk_")
	invalidDuration := config.SegmentDuration <= 0 ||
		config.MaximumSegmentDuration < config.SegmentDuration
	if config.Track == nil || invalidIdentity || config.SpoolDirectory == "" || invalidDuration ||
		config.TimelineClock == nil || config.Muxer == nil || config.OnSegment == nil ||
		config.InitialSequence < 0 || config.OperationTimeout <= 0 || config.MaximumStorageBytes < 1 {
		return nil, errors.New("invalid RTP track writer configuration")
	}
	container, _, _, err := mediaTuple(config.Source, config.Codec)
	if err != nil || container == "" {
		return nil, errors.New("invalid RTP track writer codec")
	}
	if err := ensureRealDirectory(config.SpoolDirectory, 0o700); err != nil {
		return nil, errors.New("prepare real RTP track spool")
	}
	stagingID, err := newTrackWriterStagingID()
	if err != nil {
		return nil, err
	}
	runContext, cancel := context.WithCancel(ctx)
	processingContext, cancelProcessing := context.WithCancel(context.Background())
	packetFactory := config.newPacketWriter
	if packetFactory == nil {
		packetFactory = newPionPacketWriter
	}
	writer := &RTPTrackWriter{
		track: config.Track, participantID: config.ParticipantInstanceID, trackID: config.TrackID,
		source: config.Source, codec: config.Codec, spoolDirectory: filepath.Clean(config.SpoolDirectory),
		segmentDuration: config.SegmentDuration, maximumDuration: config.MaximumSegmentDuration,
		clock: config.TimelineClock, muxer: config.Muxer, ready: config.OnSegment,
		newPacketWriter: packetFactory, initialSequence: config.InitialSequence,
		operationTimeout:    config.OperationTimeout,
		maximumStorageBytes: config.MaximumStorageBytes,
		recordingID:         config.RecordingID,
		stagingBudget:       config.StagingBudget,
		stagingID:           stagingID,
		cancel:              cancel,
		processingContext:   processingContext,
		cancelProcessing:    cancelProcessing,
		done:                make(chan struct{}),
		errors:              make(chan error, 1),
	}
	go writer.run(runContext)
	return writer, nil
}

func newTrackWriterStagingID() (string, error) {
	var nonce [16]byte
	if _, err := cryptorand.Read(nonce[:]); err != nil {
		return "", errors.New("generate RTP writer staging identity")
	}
	return hex.EncodeToString(nonce[:]), nil
}

func (writer *RTPTrackWriter) Close(ctx context.Context) error {
	writer.stop(nil)
	return writer.wait(ctx)
}

// Errors reports an unexpected terminal writer failure as soon as capture
// stops. Normal caller-initiated closure closes the channel without a value.
func (writer *RTPTrackWriter) Errors() <-chan error {
	return writer.errors
}

func (writer *RTPTrackWriter) CloseAt(ctx context.Context, endMS int64) error {
	if endMS < 0 {
		return errors.New("invalid track writer close boundary")
	}
	writer.stop(&endMS)
	return writer.wait(ctx)
}

func (writer *RTPTrackWriter) stop(endMS *int64) {
	writer.mu.Lock()
	if endMS != nil {
		value := *endMS
		writer.closeAtMS = &value
	}
	writer.mu.Unlock()
	writer.cancel()
	_ = writer.track.SetReadDeadline(time.Now())
}

func (writer *RTPTrackWriter) wait(ctx context.Context) error {
	select {
	case <-writer.done:
		writer.mu.Lock()
		defer writer.mu.Unlock()
		return writer.result
	case <-ctx.Done():
		writer.cancelProcessing()
		return errors.Join(errTrackWriterStillRunning, ctx.Err())
	}
}

var errTrackWriterStillRunning = errors.New("track writer is still running")

func (writer *RTPTrackWriter) closingBoundary() int64 {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.closeAtMS != nil {
		return *writer.closeAtMS
	}
	return writer.clock()
}

func (writer *RTPTrackWriter) run(ctx context.Context) {
	defer writer.cancelProcessing()
	defer close(writer.done)
	defer close(writer.errors)
	writer.result = writer.capture(ctx)
	if writer.result != nil && ctx.Err() == nil {
		writer.errors <- writer.result
	}
}

func (writer *RTPTrackWriter) capture(ctx context.Context) (result error) {
	var current packetWriter
	var intermediate string
	var segmentStart int64
	sequence := writer.initialSequence
	var havePackets bool
	var intermediateReserved int64
	closedSegments := make(chan closedTrackSegment, trackSegmentQueueSize)
	processingDone := make(chan error, 1)
	go func() {
		var result error
		for segment := range closedSegments {
			if result != nil {
				_ = os.Remove(segment.intermediate)
				writer.stagingBudget.ReleaseStaging(writer.recordingID, segment.intermediate)
				continue
			}
			if err := writer.processClosedSegment(writer.processingContext, segment); err != nil {
				result = err
				_ = writer.track.SetReadDeadline(time.Now())
			}
		}
		processingDone <- result
	}()
	defer func() {
		if result != nil {
			writer.cancelProcessing()
		}
		if current != nil {
			_ = current.Close()
		}
		if intermediate != "" {
			_ = os.Remove(intermediate)
			writer.stagingBudget.ReleaseStaging(writer.recordingID, intermediate)
		}
		close(closedSegments)
		result = errors.Join(result, <-processingDone)
	}()
	parameterSets := h264ParameterSets{}
	openSegment := func(packet *rtp.Packet, now int64) error {
		segmentStart = now
		filename := writer.trackID + "-" + sequenceString(sequence) + "-w" + writer.stagingID +
			intermediateExtension(writer.codec) + ".part"
		intermediate = filepath.Join(writer.spoolDirectory, filename)
		created, err := writer.newPacketWriter(writer.codec, intermediate, packet)
		if err != nil {
			return err
		}
		current = created
		reserved, err := writer.resizeReservation(intermediate)
		if err != nil {
			return err
		}
		intermediateReserved = reserved
		if writer.codec != "h264" {
			return nil
		}
		for _, prefix := range parameterSets.Prefix(packet) {
			if err := writer.reservePacketWrite(intermediate, &intermediateReserved, prefix); err != nil {
				return err
			}
			if err := current.WriteRTP(prefix); err != nil {
				return errors.New("write H264 parameter set")
			}
			if err := writer.refreshReservation(intermediate, &intermediateReserved); err != nil {
				return err
			}
		}
		return nil
	}
	closeSegment := func(endMS int64) error {
		if current == nil {
			return nil
		}
		const closeAllowance = int64(64 * 1024)
		if err := writer.stagingBudget.ResizeStaging(
			writer.recordingID,
			intermediate,
			intermediateReserved+closeAllowance,
		); err != nil {
			return err
		}
		intermediateReserved += closeAllowance
		if err := current.Close(); err != nil {
			return errors.New("close RTP intermediate")
		}
		current = nil
		if err := writer.refreshReservation(intermediate, &intermediateReserved); err != nil {
			return err
		}
		if !havePackets {
			_ = os.Remove(intermediate)
			writer.stagingBudget.ReleaseStaging(writer.recordingID, intermediate)
			intermediate = ""
			intermediateReserved = 0
			return nil
		}
		if endMS <= segmentStart {
			endMS = segmentStart + 1
		}
		segment := closedTrackSegment{
			intermediate: intermediate,
			startMS:      segmentStart,
			endMS:        endMS,
			sequence:     sequence,
		}
		select {
		case closedSegments <- segment:
		default:
			return errors.New("closed segment processing queue exhausted")
		}
		intermediate = ""
		intermediateReserved = 0
		sequence++
		havePackets = false
		return nil
	}
	for {
		packet, err := writer.track.ReadRTP()
		if err != nil {
			if ctx.Err() != nil {
				return closeSegment(writer.closingBoundary())
			}
			return errors.New("read remote RTP track")
		}
		now := writer.clock()
		if writer.codec == "h264" {
			parameterSets.Observe(packet)
		}
		keyFrame := isKeyFrame(writer.codec, packet.Payload)
		canStartSegment := keyFrame && (writer.codec != "h264" || parameterSets.Complete())
		if current == nil {
			if writer.source != signaling.SourceMic && !canStartSegment {
				continue
			}
			if err := openSegment(packet, now); err != nil {
				return err
			}
		}
		elapsed := time.Duration(now-segmentStart) * time.Millisecond
		if havePackets && elapsed >= writer.segmentDuration && (writer.source == signaling.SourceMic || canStartSegment) {
			if err := closeSegment(now); err != nil {
				return err
			}
			if err := openSegment(packet, now); err != nil {
				return err
			}
		} else if elapsed > writer.maximumDuration {
			return errors.New("video keyframe segment bound exceeded")
		}
		if err := writer.reservePacketWrite(intermediate, &intermediateReserved, packet); err != nil {
			return err
		}
		if err := current.WriteRTP(packet); err != nil {
			return errors.New("write RTP intermediate")
		}
		if err := writer.refreshReservation(intermediate, &intermediateReserved); err != nil {
			return err
		}
		havePackets = true
	}
}

func (writer *RTPTrackWriter) processClosedSegment(
	ctx context.Context,
	segment closedTrackSegment,
) error {
	container, codec, contentType, _ := mediaTuple(writer.source, writer.codec)
	outputName := writer.trackID + "-" + sequenceString(segment.sequence) + "-w" +
		writer.stagingID + "." + container + ".part"
	output := filepath.Join(writer.spoolDirectory, outputName)
	operationContext, operationCancel := context.WithTimeout(ctx, writer.operationTimeout)
	defer operationCancel()
	cleanupIntermediate := true
	defer func() {
		if cleanupIntermediate {
			_ = os.Remove(segment.intermediate)
			writer.stagingBudget.ReleaseStaging(writer.recordingID, segment.intermediate)
		}
	}()
	intermediateInfo, err := os.Stat(segment.intermediate)
	if err != nil {
		return errors.New("stat RTP intermediate")
	}
	muxReservationAllowance := min(int64(64*1024), writer.maximumStorageBytes/16)
	maximumOutputBytes := intermediateInfo.Size() + muxReservationAllowance
	if err := writer.stagingBudget.ResizeStaging(writer.recordingID, output, maximumOutputBytes); err != nil {
		return err
	}
	cleanupOutput := true
	defer func() {
		if cleanupOutput {
			_ = os.Remove(output)
			writer.stagingBudget.ReleaseStaging(writer.recordingID, output)
		}
	}()
	if err := writer.muxer.Mux(operationContext, MuxRequest{
		Codec:              codec,
		Intermediate:       segment.intermediate,
		Output:             output,
		MaximumOutputBytes: maximumOutputBytes,
		PendingCleanup: func() error {
			cleanupErr := errors.Join(removeStagingFile(segment.intermediate), removeStagingFile(output))
			if cleanupErr != nil {
				return cleanupErr
			}
			writer.stagingBudget.ReleaseStaging(writer.recordingID, segment.intermediate)
			writer.stagingBudget.ReleaseStaging(writer.recordingID, output)
			return nil
		},
	}); err != nil {
		var pendingCleanup *muxCleanupPendingError
		if errors.As(err, &pendingCleanup) {
			cleanupOutput = false
			cleanupIntermediate = false
		}
		return err
	}
	outputInfo, err := os.Stat(output)
	if err != nil {
		return errors.New("stat muxed segment")
	}
	if err := writer.stagingBudget.ResizeStaging(writer.recordingID, output, outputInfo.Size()); err != nil {
		return err
	}
	if err := os.Remove(segment.intermediate); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.New("remove RTP intermediate")
	}
	writer.stagingBudget.ReleaseStaging(writer.recordingID, segment.intermediate)
	cleanupIntermediate = false
	bytes, digest, err := hashFile(output)
	if err != nil {
		return err
	}
	descriptor := SegmentDescriptor{
		TrackID: writer.trackID, ParticipantInstanceID: writer.participantID, Source: writer.source,
		Sequence: segment.sequence, Container: container, Codec: codec, ContentType: contentType,
		ExpectedBytes: bytes, SHA256: digest, StartMS: segment.startMS, EndMS: segment.endMS,
	}
	if err := writer.ready(operationContext, descriptor, output); err != nil {
		return err
	}
	cleanupOutput = false
	return nil
}

func removeStagingFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (writer *RTPTrackWriter) resizeReservation(path string) (int64, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, writer.stagingBudget.ResizeStaging(writer.recordingID, path, 0)
	}
	if err != nil {
		return 0, errors.New("stat staged media")
	}
	return info.Size(), writer.stagingBudget.ResizeStaging(writer.recordingID, path, info.Size())
}

func (writer *RTPTrackWriter) reservePacketWrite(
	path string,
	reserved *int64,
	packet *rtp.Packet,
) error {
	const packetAllowance = int64(1024)
	next := *reserved + int64(len(packet.Payload)) + packetAllowance
	if err := writer.stagingBudget.ResizeStaging(writer.recordingID, path, next); err != nil {
		return err
	}
	*reserved = next
	return nil
}

func (writer *RTPTrackWriter) refreshReservation(path string, reserved *int64) error {
	size, err := writer.resizeReservation(path)
	if err != nil {
		return err
	}
	*reserved = size
	return nil
}

type memoryStagingBudget struct {
	mu      sync.Mutex
	maximum int64
	paths   map[string]int64
}

func newMemoryStagingBudget(maximum int64) *memoryStagingBudget {
	return &memoryStagingBudget{maximum: maximum, paths: map[string]int64{}}
}

func (budget *memoryStagingBudget) ResizeStaging(_ string, path string, size int64) error {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	var total int64
	for currentPath, currentSize := range budget.paths {
		if currentPath != path {
			total += currentSize
		}
	}
	if total+size > budget.maximum {
		return errors.New("capture storage byte limit exceeded")
	}
	budget.paths[path] = size
	return nil
}

func (budget *memoryStagingBudget) ReleaseStaging(_ string, path string) {
	budget.mu.Lock()
	delete(budget.paths, path)
	budget.mu.Unlock()
}

func newPionPacketWriter(codec, path string, first *rtp.Packet) (packetWriter, error) {
	switch codec {
	case "vp8":
		width, height, err := vp8Dimensions(first.Payload)
		if err != nil {
			return nil, err
		}
		return ivfwriter.New(path, ivfwriter.WithCodec("video/VP8"), ivfwriter.WithWidthAndHeight(width, height))
	case "opus":
		return oggwriter.New(path, 48_000, 2)
	case "h264":
		return h264writer.New(path)
	default:
		return nil, errors.New("unsupported RTP codec writer")
	}
}

func mediaTuple(source signaling.Source, mimeCodec string) (string, string, string, error) {
	codec := strings.ToLower(strings.TrimPrefix(strings.TrimPrefix(mimeCodec, "video/"), "audio/"))
	switch {
	case source == signaling.SourceMic && codec == "opus":
		return "webm", "opus", "audio/webm", nil
	case (source == signaling.SourceCamera || source == signaling.SourceScreen) && codec == "vp8":
		return "webm", "vp8", "video/webm", nil
	case (source == signaling.SourceCamera || source == signaling.SourceScreen) && codec == "h264":
		return "matroska", "h264", "video/x-matroska", nil
	default:
		return "", "", "", errors.New("unsupported media tuple")
	}
}

func intermediateExtension(codec string) string {
	switch codec {
	case "vp8":
		return ".ivf"
	case "opus":
		return ".ogg"
	case "h264":
		return ".h264"
	default:
		return ".media"
	}
}

func sequenceString(sequence int32) string {
	const digits = "0000000000"
	value := []byte(digits)
	number := sequence
	for index := len(value) - 1; index >= 0 && number > 0; index-- {
		value[index] = byte('0' + number%10)
		number /= 10
	}
	return string(value)
}

func hashFile(path string) (int64, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	digest := sha256.New()
	bytes, err := io.Copy(digest, file)
	if err != nil {
		return 0, "", err
	}
	if bytes < 1 || bytes > 2_000_000_000 {
		return 0, "", errors.New("segment file size outside contract")
	}
	return bytes, hex.EncodeToString(digest.Sum(nil)), nil
}

func isKeyFrame(codec string, payload []byte) bool {
	switch codec {
	case "vp8":
		frame, start := vp8FramePayload(payload)
		return start && len(frame) > 0 && frame[0]&1 == 0
	case "h264":
		if len(payload) == 0 {
			return false
		}
		typeID := payload[0] & 0x1f
		if typeID == 5 {
			return true
		}
		if typeID == 28 && len(payload) > 1 {
			return payload[1]&0x80 != 0 && payload[1]&0x1f == 5
		}
		if typeID == 24 {
			keyFrame := false
			for offset := 1; offset+2 <= len(payload); {
				size := int(payload[offset])<<8 | int(payload[offset+1])
				offset += 2
				if size < 1 || offset+size > len(payload) {
					return false
				}
				if payload[offset]&0x1f == 5 {
					keyFrame = true
				}
				offset += size
				if offset == len(payload) {
					return keyFrame
				}
			}
		}
	}
	return false
}

func vp8FramePayload(payload []byte) ([]byte, bool) {
	if len(payload) < 1 {
		return nil, false
	}
	first := payload[0]
	start := first&0x10 != 0 && first&0x0f == 0
	offset := 1
	if first&0x80 != 0 {
		if len(payload) <= offset {
			return nil, false
		}
		extended := payload[offset]
		offset++
		if extended&0x80 != 0 {
			if len(payload) <= offset {
				return nil, false
			}
			if payload[offset]&0x80 != 0 {
				offset += 2
			} else {
				offset++
			}
		}
		if extended&0x40 != 0 {
			offset++
		}
		if extended&0x30 != 0 {
			offset++
		}
	}
	if offset > len(payload) {
		return nil, false
	}
	return payload[offset:], start
}

func vp8Dimensions(payload []byte) (uint16, uint16, error) {
	frame, start := vp8FramePayload(payload)
	if !start || len(frame) < 10 || frame[0]&1 != 0 || frame[3] != 0x9d || frame[4] != 0x01 || frame[5] != 0x2a {
		return 0, 0, errors.New("VP8 segment does not start with a key frame")
	}
	width := uint16(frame[6]) | uint16(frame[7]&0x3f)<<8
	height := uint16(frame[8]) | uint16(frame[9]&0x3f)<<8
	if width == 0 || height == 0 {
		return 0, 0, errors.New("invalid VP8 frame dimensions")
	}
	return width, height, nil
}
