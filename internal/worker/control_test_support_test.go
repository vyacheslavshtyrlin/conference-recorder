package worker

import (
	recording "conference/recording-bot/internal/recording"
	"conference/recording-bot/internal/signaling"
)

type AttemptRequest = recording.AttemptRequest
type FinalizationRequest = recording.FinalizationRequest
type ProbeEvidence = recording.ProbeEvidence
type AssignedProbeRunner = recording.AssignedProbeRunner
type CaptureAPIError = recording.CaptureAPIError
type Source = signaling.Source

const (
	ProtocolVersion        = signaling.ProtocolVersion
	CaptureProtocolVersion = recording.CaptureProtocolVersion
	SourceMic              = signaling.SourceMic
	SourceCamera           = signaling.SourceCamera
	SourceScreen           = signaling.SourceScreen
)

var (
	ErrAdmissionRejected      = signaling.ErrAdmissionRejected
	ErrConnectTimeout         = signaling.ErrConnectTimeout
	ErrRevisionGap            = signaling.ErrRevisionGap
	ErrRevisionCapacity       = signaling.ErrRevisionCapacity
	ErrCaptureIncomplete      = recording.ErrCaptureIncomplete
	ErrCaptureMaximumDuration = recording.ErrCaptureMaximumDuration
	ErrFinalizationFailed     = recording.ErrFinalizationFailed
)
