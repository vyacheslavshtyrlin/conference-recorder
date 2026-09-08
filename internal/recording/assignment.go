package recording

import (
	"context"

	"conference/recording-bot/internal/signaling"
)

// AttemptRequest mirrors the existing recording-bot.v1 worker assignment body.
type AttemptRequest struct {
	AttemptID          string `json:"attemptId"`
	RoomID             string `json:"roomId"`
	RecordingID        string `json:"recordingId"`
	IntegrationID      string `json:"integrationId"`
	RequestedAt        string `json:"requestedAt"`
	ProtocolVersion    string `json:"protocolVersion"`
	SignalingURL       string `json:"signalingUrl"`
	Admission          string `json:"admission"`
	WorkerID           string `json:"workerId"`
	WorkerGenerationID string `json:"workerGenerationId"`
	CaptureGeneration  int32  `json:"captureGeneration,omitempty"`
	CaptureStartedAt   string `json:"captureStartedAt,omitempty"`
}

type FinalizationRequest struct {
	AttemptID          string `json:"attemptId"`
	RoomID             string `json:"roomId"`
	RecordingID        string `json:"recordingId"`
	IntegrationID      string `json:"integrationId"`
	RequestedAt        string `json:"requestedAt"`
	WorkerID           string `json:"workerId"`
	WorkerGenerationID string `json:"workerGenerationId"`
}

type AssignedFinalizationRunner interface {
	Finalize(context.Context, FinalizationRequest) error
}

// ProbeEvidence is bounded test evidence without raw participant/producer IDs.
type ProbeEvidence struct {
	ObservedSources []signaling.Source `json:"observedSources"`
	Codecs          []string           `json:"codecs"`
	Complete        bool               `json:"complete"`
}

// AssignedProbeRunner starts one receive-only probe for an authoritative assignment.
type AssignedProbeRunner interface {
	Run(
		ctx context.Context,
		request AttemptRequest,
		ready func(),
		report func(ProbeEvidence),
	) error
}
