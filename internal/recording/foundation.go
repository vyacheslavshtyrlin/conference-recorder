package recording

import (
	"context"
	"errors"
)

var ErrAttemptFenceMismatch = errors.New("attempt fence mismatch")

// SegmentSpool owns bounded worker-local durable staging.
type SegmentSpool interface {
	Prepare(ctx context.Context) error
	Close() error
}

type UploadClient interface {
	Ready(ctx context.Context) error
}

type ProcessSupervisor interface {
	Ready() error
	Shutdown(ctx context.Context) error
}

type AttemptFence struct {
	AttemptID          string
	RecordingID        string
	WorkerID           string
	WorkerGenerationID string
}

func (fence AttemptFence) validate() error {
	values := []string{
		fence.AttemptID,
		fence.RecordingID,
		fence.WorkerID,
		fence.WorkerGenerationID,
	}
	for _, value := range values {
		if value == "" || len(value) > 128 {
			return ErrAttemptFenceMismatch
		}
	}
	return nil
}
