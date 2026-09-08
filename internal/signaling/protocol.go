package signaling

import (
	"context"
	"encoding/json"
)

// ProtocolVersion is the unchanged private signaling contract consumed by the spike.
const ProtocolVersion = "recording-bot.v1"

// Participant is raw v1 identity held only by the in-memory reconciler.
type Participant struct {
	ParticipantID string `json:"participantId"`
	DisplayName   string `json:"displayName"`
}

// Producer is a raw v1 producer descriptor held only in process memory.
type Producer struct {
	ProducerID    string    `json:"producerId"`
	ParticipantID string    `json:"participantId"`
	DisplayName   string    `json:"displayName"`
	Kind          MediaKind `json:"kind"`
	Source        Source    `json:"source"`
	Paused        bool      `json:"paused"`
}

// Snapshot is the authoritative recording-bot.v1 roster and producer barrier.
type Snapshot struct {
	ProtocolVersion string        `json:"protocolVersion"`
	RecordingID     string        `json:"recordingId"`
	Revision        uint64        `json:"revision"`
	Participants    []Participant `json:"participants"`
	Producers       []Producer    `json:"producers"`
}

// Event is one ordered recording-bot.v1 lifecycle mutation.
type Event struct {
	Type          string       `json:"type"`
	RecordingID   string       `json:"recordingId"`
	Revision      uint64       `json:"revision"`
	Participant   *Participant `json:"participant,omitempty"`
	ParticipantID string       `json:"participantId,omitempty"`
	Producer      *Producer    `json:"producer,omitempty"`
	ProducerID    string       `json:"producerId,omitempty"`
}

// TransportOptions is the mediasoup WebRtcTransport receive descriptor.
type TransportOptions struct {
	ID             string          `json:"id"`
	ICEParameters  ICEParameters   `json:"iceParameters"`
	ICECandidates  []ICECandidate  `json:"iceCandidates"`
	DTLSParameters DTLSParameters  `json:"dtlsParameters"`
	SCTPParameters json.RawMessage `json:"sctpParameters,omitempty"`
}

// ICEParameters contains the mediasoup ICE-lite credentials.
type ICEParameters struct {
	UsernameFragment string `json:"usernameFragment"`
	Password         string `json:"password"`
	ICELite          bool   `json:"iceLite"`
}

// ICECandidate is a mediasoup transport candidate.
type ICECandidate struct {
	Foundation string `json:"foundation"`
	Priority   uint32 `json:"priority"`
	IP         string `json:"ip"`
	Protocol   string `json:"protocol"`
	Port       uint16 `json:"port"`
	Type       string `json:"type"`
	TCPType    string `json:"tcpType,omitempty"`
}

// DTLSFingerprint identifies the mediasoup transport certificate.
type DTLSFingerprint struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// DTLSParameters is exchanged by bot:connectRecvTransport.
type DTLSParameters struct {
	Role         string            `json:"role"`
	Fingerprints []DTLSFingerprint `json:"fingerprints"`
}

// RTPCodecParameters is the subset needed to construct a Pion remote offer.
type RTPCodecParameters struct {
	MimeType     string         `json:"mimeType"`
	PayloadType  uint8          `json:"payloadType"`
	ClockRate    uint32         `json:"clockRate"`
	Channels     uint16         `json:"channels,omitempty"`
	Parameters   map[string]any `json:"parameters,omitempty"`
	RTCPFeedback []RTCPFeedback `json:"rtcpFeedback,omitempty"`
}

// RTCPFeedback is one mediasoup codec feedback capability.
type RTCPFeedback struct {
	Type      string `json:"type"`
	Parameter string `json:"parameter,omitempty"`
}

// RTPHeaderExtensionParameters is one negotiated RTP header extension.
type RTPHeaderExtensionParameters struct {
	URI        string         `json:"uri"`
	ID         int            `json:"id"`
	Encrypt    bool           `json:"encrypt,omitempty"`
	Parameters map[string]any `json:"parameters,omitempty"`
}

// RTPRTXParameters identifies an RTX synchronization source.
type RTPRTXParameters struct {
	SSRC uint32 `json:"ssrc"`
}

// RTPEncodingParameters identifies one consumer SSRC.
type RTPEncodingParameters struct {
	SSRC             uint32            `json:"ssrc"`
	RTX              *RTPRTXParameters `json:"rtx,omitempty"`
	CodecPayloadType *uint8            `json:"codecPayloadType,omitempty"`
}

// RTCPParameters contains the mediasoup consumer CNAME and reduced-size flag.
type RTCPParameters struct {
	CNAME       string `json:"cname,omitempty"`
	ReducedSize bool   `json:"reducedSize,omitempty"`
}

// RTPParameters is the mediasoup consumer RTP descriptor.
type RTPParameters struct {
	MID              string                         `json:"mid,omitempty"`
	Codecs           []RTPCodecParameters           `json:"codecs"`
	HeaderExtensions []RTPHeaderExtensionParameters `json:"headerExtensions,omitempty"`
	Encodings        []RTPEncodingParameters        `json:"encodings"`
	RTCP             RTCPParameters                 `json:"rtcp,omitempty"`
}

// Consumer is an idempotent bot:consume result.
type Consumer struct {
	ID            string        `json:"id"`
	ProducerID    string        `json:"producerId"`
	Kind          MediaKind     `json:"kind"`
	RTPParameters RTPParameters `json:"rtpParameters"`
	Paused        bool          `json:"paused"`
	Source        Source        `json:"-"`
	Closed        bool          `json:"-"`
}

// SignalingClient is the unchanged private recording-bot.v1 request surface.
type SignalingClient interface {
	RouterRTPCapabilities(ctx context.Context) (json.RawMessage, error)
	CreateRecvTransport(ctx context.Context) (TransportOptions, error)
	SyncSnapshot(ctx context.Context) (Snapshot, error)
	Consume(
		ctx context.Context,
		transportID string,
		producerID string,
		rtpCapabilities json.RawMessage,
	) (Consumer, error)
	ConnectRecvTransport(
		ctx context.Context,
		transportID string,
		dtlsParameters DTLSParameters,
	) error
	ResumeConsumer(ctx context.Context, producerID string) error
	PauseConsumer(ctx context.Context, producerID string) error
	CloseConsumer(ctx context.Context, producerID string) error
	Lifecycle() <-chan Event
	Close() error
}
