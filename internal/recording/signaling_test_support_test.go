package recording

import "conference/recording-bot/internal/signaling"

type Source = signaling.Source
type Participant = signaling.Participant
type Producer = signaling.Producer
type Snapshot = signaling.Snapshot
type Event = signaling.Event
type TrackEvent = signaling.TrackEvent
type SignalingClient = signaling.SignalingClient
type SessionIdentity = signaling.SessionIdentity

const (
	ProtocolVersion = signaling.ProtocolVersion
	SourceMic       = signaling.SourceMic
	SourceCamera    = signaling.SourceCamera
	SourceScreen    = signaling.SourceScreen
	KindAudio       = signaling.KindAudio
	KindVideo       = signaling.KindVideo
)
