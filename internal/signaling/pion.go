package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

const (
	maxPionMediaSections  = 256
	pionDisconnectedGrace = 5 * time.Second
)

// RTPTrackSource is the bounded RTP input exposed to segment writers.
type RTPTrackSource interface {
	ReadRTP() (*rtp.Packet, error)
	SetReadDeadline(time.Time) error
}

// PionTrackSource adapts a Pion remote track to RTPTrackSource.
type PionTrackSource struct{ Track *webrtc.TrackRemote }

func (source PionTrackSource) ReadRTP() (*rtp.Packet, error) {
	packet, _, err := source.Track.ReadRTP()
	return packet, err
}

func (source PionTrackSource) SetReadDeadline(deadline time.Time) error {
	return source.Track.SetReadDeadline(deadline)
}

// PionSession translates mediasoup transport/consumer parameters into the SDP
// negotiation Pion requires. mediasoup itself remains the SDP-less authority.
type PionSession struct {
	connection           *webrtc.PeerConnection
	tracks               chan TrackEvent
	errors               chan error
	done                 chan struct{}
	connected            chan struct{}
	connectOnce          sync.Once
	mu                   sync.RWMutex
	closeOnce            sync.Once
	errorOnce            sync.Once
	stateMu              sync.Mutex
	state                webrtc.PeerConnectionState
	disconnect           *time.Timer
	disconnectGeneration uint64
	slots                map[string]Consumer
	order                []string
	trackMu              sync.Mutex
	trackArrivals        map[string]chan struct{}
}

// TrackEvent correlates a Pion remote track with its private in-memory
// recording-bot producer/source descriptor.
type TrackEvent struct {
	ProducerID  string
	Source      Source
	Kind        MediaKind
	Codec       string
	Track       *webrtc.TrackRemote
	SourceTrack RTPTrackSource
}

// NewPionSession creates a receive-only Pion peer with the default codec set.
func NewPionSession() (*PionSession, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register Pion codecs: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine))
	connection, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, fmt.Errorf("create Pion peer connection: %w", err)
	}

	session := &PionSession{
		connection:    connection,
		tracks:        make(chan TrackEvent, maxPionMediaSections),
		errors:        make(chan error, 1),
		done:          make(chan struct{}),
		connected:     make(chan struct{}),
		slots:         map[string]Consumer{},
		order:         []string{},
		trackArrivals: map[string]chan struct{}{},
	}
	connection.OnTrack(session.deliverTrack)
	connection.OnConnectionStateChange(session.connectionStateChanged)
	return session, nil
}

// Tracks returns correlated remote-track observations for the opt-in probe.
func (session *PionSession) Tracks() <-chan TrackEvent {
	return session.tracks
}

// Errors reports terminal bounded-backpressure failures from track delivery.
func (session *PionSession) Errors() <-chan error {
	return session.errors
}

func (session *PionSession) ResetTrack(producerID string) {
	session.trackMu.Lock()
	delete(session.trackArrivals, producerID)
	session.trackMu.Unlock()
}

func (session *PionSession) WaitForTrack(ctx context.Context, producerID string) error {
	session.trackMu.Lock()
	arrival := session.trackArrivals[producerID]
	if arrival == nil {
		arrival = make(chan struct{})
		session.trackArrivals[producerID] = arrival
	}
	session.trackMu.Unlock()

	select {
	case <-arrival:
		return nil
	case <-session.done:
		return errors.New("Pion session closed before producer media arrived")
	case <-ctx.Done():
		return fmt.Errorf("producer media arrival: %w", ctx.Err())
	}
}

// AcceptConsumers applies a synthetic mediasoup remote offer and returns the
// Pion answer plus local DTLS parameters for bot:connectRecvTransport.
func (session *PionSession) AcceptConsumers(
	ctx context.Context,
	transport TransportOptions,
	consumers []Consumer,
) (string, DTLSParameters, error) {
	return session.ReconcileConsumers(ctx, transport, consumers)
}

// ReconcileConsumers renegotiates the stable BUNDLE mids. Removed consumers
// remain as rejected m-lines so replacement never reuses a prior MID.
func (session *PionSession) ReconcileConsumers(
	ctx context.Context,
	transport TransportOptions,
	consumers []Consumer,
) (string, DTLSParameters, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	select {
	case <-session.done:
		return "", DTLSParameters{}, errors.New("Pion session is closed")
	default:
	}

	candidateSlots := make(map[string]Consumer, len(session.slots)+len(consumers))
	for producerID, consumer := range session.slots {
		candidateSlots[producerID] = consumer
	}
	candidateOrder := append([]string{}, session.order...)
	current := make(map[string]Consumer, len(consumers))
	for _, consumer := range consumers {
		if consumer.ProducerID == "" || consumer.RTPParameters.MID == "" {
			return "", DTLSParameters{}, errors.New("consumer producerId and MID are required")
		}
		if _, duplicate := current[consumer.ProducerID]; duplicate {
			return "", DTLSParameters{}, errors.New("duplicate consumer producerId")
		}
		current[consumer.ProducerID] = consumer
		if _, exists := candidateSlots[consumer.ProducerID]; !exists {
			if len(candidateOrder) >= maxPionMediaSections {
				return "", DTLSParameters{}, errors.New("Pion media-section capacity exhausted")
			}
			candidateOrder = append(candidateOrder, consumer.ProducerID)
		}
		candidateSlots[consumer.ProducerID] = consumer
	}
	for producerID, consumer := range candidateSlots {
		if _, active := current[producerID]; active {
			continue
		}
		consumer.Closed = true
		candidateSlots[producerID] = consumer
	}

	ordered := make([]Consumer, 0, len(candidateOrder))
	for _, producerID := range candidateOrder {
		ordered = append(ordered, candidateSlots[producerID])
	}
	offer, err := BuildMediasoupOffer(transport, ordered)
	if err != nil {
		return "", DTLSParameters{}, err
	}
	if err := session.connection.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offer,
	}); err != nil {
		return "", DTLSParameters{}, fmt.Errorf("apply mediasoup offer: %w", err)
	}

	answer, err := session.connection.CreateAnswer(nil)
	if err != nil {
		return "", DTLSParameters{}, fmt.Errorf("create Pion answer: %w", err)
	}
	gatheringComplete := webrtc.GatheringCompletePromise(session.connection)
	if err := session.connection.SetLocalDescription(answer); err != nil {
		return "", DTLSParameters{}, fmt.Errorf("apply Pion answer: %w", err)
	}

	select {
	case <-ctx.Done():
		return "", DTLSParameters{}, fmt.Errorf("wait for Pion ICE gathering: %w", ctx.Err())
	case <-gatheringComplete:
	}

	local := session.connection.LocalDescription()
	if local == nil {
		return "", DTLSParameters{}, errors.New("Pion local description is missing")
	}
	dtls, err := ExtractDTLSParameters(local.SDP)
	if err != nil {
		return "", DTLSParameters{}, err
	}
	session.slots = candidateSlots
	session.order = candidateOrder
	return local.SDP, dtls, nil
}

// RequestKeyFrame sends a PLI on the established WebRTC transport.
func (session *PionSession) RequestKeyFrame(ctx context.Context, ssrc uint32) error {
	select {
	case <-session.connected:
	case <-session.done:
		return errors.New("Pion session closed before PLI")
	case <-ctx.Done():
		return fmt.Errorf("wait for Pion connection before PLI: %w", ctx.Err())
	}
	if err := session.connection.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{MediaSSRC: ssrc},
	}); err != nil {
		return fmt.Errorf("send PLI: %w", err)
	}
	return nil
}

// Close releases the feasibility peer connection.
func (session *PionSession) Close() error {
	var err error
	session.closeOnce.Do(func() {
		close(session.done)
		session.stateMu.Lock()
		session.state = webrtc.PeerConnectionStateClosed
		session.disconnectGeneration++
		if session.disconnect != nil {
			session.disconnect.Stop()
			session.disconnect = nil
		}
		session.stateMu.Unlock()
		err = session.connection.Close()
	})
	return err
}

func (session *PionSession) deliverTrack(
	track *webrtc.TrackRemote,
	_ *webrtc.RTPReceiver,
) {
	ssrc := uint32(track.SSRC())
	session.mu.RLock()
	defer session.mu.RUnlock()
	for _, consumer := range session.slots {
		if consumer.Closed || len(consumer.RTPParameters.Encodings) != 1 {
			continue
		}
		encoding := consumer.RTPParameters.Encodings[0]
		isPrimary := encoding.SSRC == ssrc
		isRTX := encoding.RTX != nil && encoding.RTX.SSRC == ssrc
		if !isPrimary && !isRTX {
			continue
		}
		event := TrackEvent{
			ProducerID:  consumer.ProducerID,
			Source:      consumer.Source,
			Kind:        consumer.Kind,
			Codec:       track.Codec().MimeType,
			Track:       track,
			SourceTrack: PionTrackSource{Track: track},
		}
		if session.enqueueTrack(event) {
			session.markTrackReady(consumer.ProducerID)
		}
		return
	}
}

func (session *PionSession) enqueueTrack(event TrackEvent) bool {
	select {
	case session.tracks <- event:
		return true
	case <-session.done:
		return false
	default:
		session.fail(errors.New("Pion track delivery backpressure exceeded"))
		return false
	}
}

func (session *PionSession) markTrackReady(producerID string) {
	session.trackMu.Lock()
	arrival := session.trackArrivals[producerID]
	if arrival == nil {
		arrival = make(chan struct{})
		session.trackArrivals[producerID] = arrival
	}
	select {
	case <-arrival:
	default:
		close(arrival)
	}
	session.trackMu.Unlock()
}

func (session *PionSession) connectionStateChanged(state webrtc.PeerConnectionState) {
	session.stateMu.Lock()
	session.state = state
	session.disconnectGeneration++
	generation := session.disconnectGeneration
	if session.disconnect != nil {
		session.disconnect.Stop()
		session.disconnect = nil
	}
	switch state {
	case webrtc.PeerConnectionStateConnected:
		session.connectOnce.Do(func() { close(session.connected) })
	case webrtc.PeerConnectionStateDisconnected:
		session.disconnect = time.AfterFunc(pionDisconnectedGrace, func() {
			session.failDisconnected(generation)
		})
	case webrtc.PeerConnectionStateFailed:
		go session.fail(errors.New("Pion peer connection failed"))
	case webrtc.PeerConnectionStateClosed:
		select {
		case <-session.done:
		default:
			go session.fail(errors.New("Pion peer connection closed unexpectedly"))
		}
	}
	session.stateMu.Unlock()
}

func (session *PionSession) failDisconnected(generation uint64) {
	session.stateMu.Lock()
	defer session.stateMu.Unlock()
	if session.state != webrtc.PeerConnectionStateDisconnected ||
		session.disconnectGeneration != generation {
		return
	}
	session.disconnect = nil
	session.fail(errors.New("Pion peer connection remained disconnected"))
}

func (session *PionSession) fail(cause error) {
	session.errorOnce.Do(func() {
		select {
		case <-session.done:
			return
		default:
		}
		select {
		case session.errors <- cause:
		case <-session.done:
			return
		}
		go func() { _ = session.Close() }()
	})
}

// BuildMediasoupOffer produces the remote SDP representation of one ICE-lite
// mediasoup receive transport and its send-only consumers.
func BuildMediasoupOffer(transport TransportOptions, consumers []Consumer) (string, error) {
	if err := validateTransport(transport); err != nil {
		return "", err
	}
	if len(consumers) == 0 {
		return "", errors.New("at least one consumer is required for SDP negotiation")
	}

	mids := make([]string, 0, len(consumers))
	seenMIDs := make(map[string]struct{}, len(consumers))
	sections := make([]string, 0, len(consumers))
	for _, consumer := range consumers {
		mid := consumer.RTPParameters.MID
		if mid == "" {
			return "", errors.New("consumer has no stable MID")
		}
		if !validSDPToken(mid) {
			return "", errors.New("consumer MID is not a valid SDP token")
		}
		if _, exists := seenMIDs[mid]; exists {
			return "", errors.New("duplicate consumer MID")
		}
		seenMIDs[mid] = struct{}{}
		mids = append(mids, mid)
		section, err := mediaSection(transport, consumer, mid)
		if err != nil {
			return "", err
		}
		sections = append(sections, section)
	}

	var offer strings.Builder
	offer.WriteString("v=0\r\n")
	offer.WriteString("o=mediasoup 0 0 IN IP4 127.0.0.1\r\n")
	offer.WriteString("s=conference-recording-capture\r\n")
	offer.WriteString("t=0 0\r\n")
	offer.WriteString("a=ice-lite\r\n")
	offer.WriteString("a=group:BUNDLE " + strings.Join(mids, " ") + "\r\n")
	offer.WriteString("a=msid-semantic: WMS *\r\n")
	for _, section := range sections {
		offer.WriteString(section)
	}
	return offer.String(), nil
}

func mediaSection(
	transport TransportOptions,
	consumer Consumer,
	mid string,
) (string, error) {
	if len(consumer.RTPParameters.Codecs) == 0 {
		return "", errors.New("consumer has no codecs")
	}
	if len(consumer.RTPParameters.Encodings) != 1 {
		return "", errors.New("consumer must have exactly one encoding")
	}

	primary, err := primaryCodec(consumer.RTPParameters.Codecs)
	if err != nil {
		return "", err
	}
	source := consumer.Source
	if source == "" && consumer.Kind == KindAudio {
		source = SourceMic
	}
	if source == "" && consumer.Kind == KindVideo {
		source = SourceCamera
	}
	if err := ValidateCodec(consumer.Kind, source, primary.MimeType); err != nil {
		return "", err
	}
	if err := validateAssociatedCodecs(primary, consumer.RTPParameters.Codecs); err != nil {
		return "", err
	}

	mediaName := string(consumer.Kind)
	payloadTypes := make([]string, 0, len(consumer.RTPParameters.Codecs))
	for _, codec := range consumer.RTPParameters.Codecs {
		payloadTypes = append(payloadTypes, strconv.Itoa(int(codec.PayloadType)))
	}

	var section strings.Builder
	port := "7"
	direction := "sendonly"
	if consumer.Closed {
		port = "0"
		direction = "inactive"
	}
	section.WriteString("m=" + mediaName + " " + port + " UDP/TLS/RTP/SAVPF " + strings.Join(payloadTypes, " ") + "\r\n")
	section.WriteString("c=IN IP4 0.0.0.0\r\n")
	section.WriteString("a=mid:" + mid + "\r\n")
	section.WriteString("a=" + direction + "\r\n")
	section.WriteString("a=rtcp-mux\r\n")
	if consumer.RTPParameters.RTCP.ReducedSize {
		section.WriteString("a=rtcp-rsize\r\n")
	}
	section.WriteString("a=ice-ufrag:" + transport.ICEParameters.UsernameFragment + "\r\n")
	section.WriteString("a=ice-pwd:" + transport.ICEParameters.Password + "\r\n")
	for _, fingerprint := range transport.DTLSParameters.Fingerprints {
		if !validSDPToken(fingerprint.Algorithm) || !validSDPToken(fingerprint.Value) {
			return "", errors.New("invalid transport DTLS fingerprint")
		}
		section.WriteString("a=fingerprint:" + fingerprint.Algorithm + " " + fingerprint.Value + "\r\n")
	}
	section.WriteString("a=setup:actpass\r\n")
	for _, candidate := range transport.ICECandidates {
		line, candidateErr := candidateLine(candidate)
		if candidateErr != nil {
			return "", candidateErr
		}
		section.WriteString(line + "\r\n")
	}
	section.WriteString("a=end-of-candidates\r\n")
	for _, extension := range consumer.RTPParameters.HeaderExtensions {
		if extension.ID <= 0 || !validSDPToken(extension.URI) {
			return "", errors.New("invalid RTP header extension")
		}
		if extension.Encrypt || len(extension.Parameters) > 0 {
			return "", errors.New("unsupported encrypted or parameterized RTP header extension")
		}
		section.WriteString("a=extmap:" + strconv.Itoa(extension.ID) + " " + extension.URI + "\r\n")
	}
	for _, codec := range consumer.RTPParameters.Codecs {
		if err := writeCodec(&section, codec); err != nil {
			return "", err
		}
	}
	cname := consumer.RTPParameters.RTCP.CNAME
	if cname == "" {
		cname = "conference-recording-capture"
	}
	if !validSDPToken(cname) {
		return "", errors.New("invalid RTCP CNAME")
	}
	encoding := consumer.RTPParameters.Encodings[0]
	if encoding.SSRC == 0 {
		return "", errors.New("consumer encoding SSRC is required")
	}
	ssrc := strconv.FormatUint(uint64(encoding.SSRC), 10)
	section.WriteString("a=ssrc:" + ssrc + " cname:" + cname + "\r\n")
	if encoding.RTX != nil {
		if encoding.RTX.SSRC == 0 {
			return "", errors.New("consumer RTX SSRC is required")
		}
		rtxSSRC := strconv.FormatUint(uint64(encoding.RTX.SSRC), 10)
		section.WriteString("a=ssrc:" + rtxSSRC + " cname:" + cname + "\r\n")
		section.WriteString("a=ssrc-group:FID " + ssrc + " " + rtxSSRC + "\r\n")
	}
	return section.String(), nil
}

func primaryCodec(codecs []RTPCodecParameters) (RTPCodecParameters, error) {
	primary := []RTPCodecParameters{}
	for _, codec := range codecs {
		if strings.EqualFold(codec.MimeType, "video/rtx") {
			continue
		}
		primary = append(primary, codec)
	}
	if len(primary) != 1 {
		return RTPCodecParameters{}, fmt.Errorf("expected one primary codec, got %d", len(primary))
	}
	return primary[0], nil
}

func validateAssociatedCodecs(
	primary RTPCodecParameters,
	codecs []RTPCodecParameters,
) error {
	rtxCount := 0
	for _, codec := range codecs {
		if !strings.EqualFold(codec.MimeType, "video/rtx") {
			continue
		}
		rtxCount++
		if rtxCount > 1 || !strings.HasPrefix(strings.ToLower(primary.MimeType), "video/") {
			return errors.New("invalid associated RTX codec set")
		}
		if codec.ClockRate != primary.ClockRate {
			return errors.New("RTX clock rate does not match primary codec")
		}
		apt, exists := codec.Parameters["apt"]
		if !exists || fmt.Sprint(apt) != strconv.Itoa(int(primary.PayloadType)) {
			return fmt.Errorf("RTX codec does not reference primary payload type %d", primary.PayloadType)
		}
	}
	return nil
}

func writeCodec(section *strings.Builder, codec RTPCodecParameters) error {
	name, err := codecName(codec.MimeType)
	if err != nil {
		return err
	}
	payloadType := strconv.Itoa(int(codec.PayloadType))
	section.WriteString("a=rtpmap:" + payloadType + " " + name + "/" + strconv.FormatUint(uint64(codec.ClockRate), 10))
	if codec.Channels > 1 {
		section.WriteString("/" + strconv.Itoa(int(codec.Channels)))
	}
	section.WriteString("\r\n")
	if len(codec.Parameters) > 0 {
		formatted, err := formatParameters(codec.Parameters)
		if err != nil {
			return err
		}
		section.WriteString("a=fmtp:" + payloadType + " " + formatted + "\r\n")
	}
	for _, feedback := range codec.RTCPFeedback {
		if !validSDPToken(feedback.Type) ||
			(feedback.Parameter != "" && !validSDPToken(feedback.Parameter)) {
			return errors.New("invalid empty RTCP feedback type")
		}
		line := "a=rtcp-fb:" + payloadType + " " + feedback.Type
		if feedback.Parameter != "" {
			line += " " + feedback.Parameter
		}
		section.WriteString(line + "\r\n")
	}
	return nil
}

func validateTransport(transport TransportOptions) error {
	if transport.ID == "" {
		return errors.New("transport id is required")
	}
	if !validSDPToken(transport.ICEParameters.UsernameFragment) ||
		!validSDPToken(transport.ICEParameters.Password) {
		return errors.New("transport ICE credentials are required")
	}
	if len(transport.ICECandidates) == 0 {
		return errors.New("transport ICE candidate is required")
	}
	if len(transport.DTLSParameters.Fingerprints) == 0 {
		return errors.New("transport DTLS fingerprint is required")
	}
	return nil
}

func candidateLine(candidate ICECandidate) (string, error) {
	protocol := strings.ToLower(candidate.Protocol)
	if protocol != "udp" && protocol != "tcp" {
		return "", fmt.Errorf("unsupported ICE protocol %q", candidate.Protocol)
	}
	switch candidate.Type {
	case "host":
	default:
		return "", errors.New("ICE candidate requires unsupported related address fields")
	}
	validFoundation := validSDPToken(candidate.Foundation) && len(candidate.Foundation) <= 32
	if !validFoundation || net.ParseIP(candidate.IP) == nil || candidate.Port == 0 {
		return "", errors.New("incomplete ICE candidate")
	}
	if protocol == "tcp" {
		switch candidate.TCPType {
		case "active", "passive", "so":
		default:
			return "", errors.New("invalid ICE TCP candidate type")
		}
	} else if candidate.TCPType != "" {
		return "", errors.New("UDP candidate cannot carry a TCP type")
	}
	line := fmt.Sprintf(
		"a=candidate:%s 1 %s %d %s %d typ %s",
		candidate.Foundation,
		protocol,
		candidate.Priority,
		candidate.IP,
		candidate.Port,
		candidate.Type,
	)
	if protocol == "tcp" {
		line += " tcptype " + candidate.TCPType
	}
	return line, nil
}

func codecName(mimeType string) (string, error) {
	parts := strings.SplitN(mimeType, "/", 2)
	if len(parts) != 2 || !validSDPToken(parts[0]) || !validSDPToken(parts[1]) {
		return "", fmt.Errorf("invalid codec mimeType %q", mimeType)
	}
	return parts[1], nil
}

func formatParameters(parameters map[string]any) (string, error) {
	keys := make([]string, 0, len(parameters))
	for key := range parameters {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		if !validSDPToken(key) || strings.ContainsAny(key, ";=") {
			return "", errors.New("invalid RTP codec parameter name")
		}
		value, err := formatParameter(parameters[key])
		if err != nil {
			return "", err
		}
		parts = append(parts, key+"="+value)
	}
	return strings.Join(parts, ";"), nil
}

func formatParameter(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		if typed == "" || strings.ContainsAny(typed, ";=\r\n") || !validSDPToken(typed) {
			return "", errors.New("invalid RTP codec parameter string")
		}
		return typed, nil
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(typed), nil
	case json.Number:
		return typed.String(), nil
	default:
		return "", errors.New("unsupported RTP codec parameter type")
	}
}

func validSDPToken(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

// ExtractDTLSParameters extracts Pion's local DTLS role and certificate
// fingerprint from an answer.
func ExtractDTLSParameters(answerSDP string) (DTLSParameters, error) {
	role := ""
	fingerprints := []DTLSFingerprint{}
	for _, line := range strings.Split(answerSDP, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "a=setup:") {
			candidateRole := ""
			switch strings.TrimPrefix(line, "a=setup:") {
			case "active":
				candidateRole = "client"
			case "passive":
				candidateRole = "server"
			default:
				return DTLSParameters{}, errors.New("invalid Pion DTLS setup role")
			}
			if role != "" && role != candidateRole {
				return DTLSParameters{}, errors.New("inconsistent Pion DTLS setup roles")
			}
			role = candidateRole
			continue
		}
		if !strings.HasPrefix(line, "a=fingerprint:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "a=fingerprint:"))
		if len(fields) != 2 {
			return DTLSParameters{}, errors.New("invalid Pion DTLS fingerprint")
		}
		fingerprints = append(
			fingerprints,
			DTLSFingerprint{
				Algorithm: fields[0],
				Value:     fields[1],
			},
		)
	}
	if role == "" {
		return DTLSParameters{}, errors.New("Pion answer has no DTLS setup role")
	}
	if len(fingerprints) == 0 {
		return DTLSParameters{}, errors.New("Pion answer has no DTLS fingerprint")
	}
	return DTLSParameters{Role: role, Fingerprints: fingerprints}, nil
}
