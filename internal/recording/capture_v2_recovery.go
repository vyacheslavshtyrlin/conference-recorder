package recording

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"conference/recording-bot/internal/signaling"
)

type recoveryParticipantInstance struct {
	ParticipantInstanceID string `json:"participantInstanceId"`
	DisplayName           string `json:"displayName"`
}

type recoveryParticipantInterval struct {
	ParticipantInstanceID string `json:"participantInstanceId"`
	StartMS               int64  `json:"startMs"`
	EndMS                 *int64 `json:"endMs"`
}

type recoveryTrack struct {
	TrackID               string           `json:"trackId"`
	ParticipantInstanceID string           `json:"participantInstanceId"`
	Source                signaling.Source `json:"source"`
}

type recoveryTrackInterval struct {
	TrackID string `json:"trackId"`
	StartMS int64  `json:"startMs"`
	EndMS   *int64 `json:"endMs"`
	State   string `json:"state"`
}

type recoveryGap struct {
	StartMS int64  `json:"startMs"`
	EndMS   *int64 `json:"endMs"`
	Scope   string `json:"scope"`
	TrackID string `json:"trackId,omitempty"`
	Reason  string `json:"reason"`
}

type recoverySegment struct {
	CaptureGeneration int32  `json:"captureGeneration"`
	UploadID          string `json:"uploadId"`
	SegmentDescriptor
}

type recoveryPage struct {
	ProtocolVersion       string                        `json:"protocolVersion"`
	RecoverySnapshotID    string                        `json:"recoverySnapshotId"`
	CaptureGeneration     int32                         `json:"captureGeneration"`
	CaptureStartedAt      string                        `json:"captureStartedAt"`
	BindingMode           string                        `json:"bindingMode"`
	TerminalTimelineEnd   *int64                        `json:"terminalTimelineEndMs"`
	ParticipantInstances  []recoveryParticipantInstance `json:"participantInstances"`
	ParticipantIntervals  []recoveryParticipantInterval `json:"participantIntervals"`
	Tracks                []recoveryTrack               `json:"tracks"`
	TrackIntervals        []recoveryTrackInterval       `json:"trackIntervals"`
	Segments              []recoverySegment             `json:"segments"`
	Gaps                  []recoveryGap                 `json:"gaps"`
	GenerationCheckpoints []GenerationCheckpoint        `json:"generationCheckpoints"`
	NextCursor            *string                       `json:"nextCursor"`
}

func (client *PrivateUploadClient) Recover(ctx context.Context, fence CaptureFence) (RecoveryState, error) {
	if err := validateCaptureFence(fence); err != nil {
		return RecoveryState{}, err
	}
	return client.recover(ctx, fence)
}

func (client *PrivateUploadClient) RecoverCurrent(
	ctx context.Context,
	recordingID string,
	workerID string,
	workerGenerationID string,
) (RecoveryState, error) {
	if recordingID == "" || workerID == "" || workerGenerationID == "" {
		return RecoveryState{}, errors.New("invalid recovery discovery fence")
	}
	return client.recover(ctx, CaptureFence{
		RecordingID:        recordingID,
		WorkerID:           workerID,
		WorkerGenerationID: workerGenerationID,
	})
}

func (client *PrivateUploadClient) recover(ctx context.Context, fence CaptureFence) (RecoveryState, error) {
	for restart := 0; restart < 3; restart++ {
		state, retry, err := client.recoverOnce(ctx, fence)
		if !retry {
			return state, err
		}
	}
	return RecoveryState{}, errors.New("recovery cursor repeatedly expired")
}

func (client *PrivateUploadClient) recoverOnce(ctx context.Context, fence CaptureFence) (RecoveryState, bool, error) {
	var state RecoveryState
	var snapshotID string
	var cursor string
	var encodedBytes int64
	segmentKeys := map[string]string{}
	for pages := 0; pages < 1_000_000; pages++ {
		path := "recovery"
		if cursor != "" {
			path += "?cursor=" + url.QueryEscape(cursor)
		}
		var page recoveryPage
		err := client.captureJSON(ctx, http.MethodGet, fence, path, nil, &page)
		failure := new(CaptureAPIError)
		cursorExpired := errors.As(err, &failure) &&
			failure.Status == http.StatusGone &&
			failure.Code == "RECOVERY_CURSOR_EXPIRED"
		if cursorExpired {
			return RecoveryState{}, true, nil
		}
		if err != nil {
			return RecoveryState{}, false, err
		}
		if err := validateRecoveryPage(page, fence); err != nil {
			return RecoveryState{}, false, err
		}
		encodedPage, err := json.Marshal(page)
		if err != nil {
			return RecoveryState{}, false, errors.New("encode recovery resource accounting")
		}
		encodedBytes += int64(len(encodedPage))
		if encodedBytes > client.maxRecoveryBytes {
			return RecoveryState{}, false, errors.New("recovery snapshot exceeds worker memory envelope")
		}
		for _, segment := range page.Segments {
			key := fmt.Sprintf("%d:%s:%d", segment.CaptureGeneration, segment.TrackID, segment.Sequence)
			if prior, exists := segmentKeys[key]; exists && prior != segment.UploadID {
				return RecoveryState{}, false, errors.New("conflicting recovered segment")
			}
			if _, exists := segmentKeys[key]; exists {
				return RecoveryState{}, false, errors.New("duplicate recovered segment")
			}
			segmentKeys[key] = segment.UploadID
		}
		itemCount := recoveryPageItemCount(page)
		if itemCount > 100 || (itemCount == 0 && page.NextCursor != nil) {
			return RecoveryState{}, false, errors.New("invalid bounded recovery page")
		}
		if snapshotID == "" {
			snapshotID = page.RecoverySnapshotID
			state.CaptureGeneration = page.CaptureGeneration
			state.CaptureStartedAt = page.CaptureStartedAt
			state.BindingMode = page.BindingMode
			state.TerminalTimelineEnd = page.TerminalTimelineEnd
		} else if recoveryBindingChanged(page, state, snapshotID) {
			return RecoveryState{}, false, errors.New("recovery page binding changed")
		}
		appendRecoveryPage(&state, page)
		if page.NextCursor == nil {
			if err := validateRecoveryCheckpoints(state); err != nil {
				return RecoveryState{}, false, err
			}
			return state, false, nil
		}
		if *page.NextCursor == "" || *page.NextCursor == cursor {
			return RecoveryState{}, false, errors.New("invalid recovery cursor progress")
		}
		cursor = *page.NextCursor
	}
	return RecoveryState{}, false, errors.New("recovery page bound exceeded")
}

func validateRecoveryCheckpoints(state RecoveryState) error {
	const maximumSafeTimeline = int64(24 * time.Hour / time.Millisecond)
	var previous *GenerationCheckpoint
	for index := range state.GenerationCheckpoints {
		checkpoint := &state.GenerationCheckpoints[index]
		validHash := len(checkpoint.PayloadSHA256) == 64 &&
			checkpoint.PayloadSHA256 == strings.ToLower(checkpoint.PayloadSHA256)
		if validHash {
			_, err := hex.DecodeString(checkpoint.PayloadSHA256)
			validHash = err == nil
		}
		invalidBounds := checkpoint.CaptureGeneration <= 0 ||
			checkpoint.CaptureGeneration > state.CaptureGeneration ||
			checkpoint.CheckpointSequence < 0 ||
			checkpoint.TimelineThroughMS < 0 ||
			checkpoint.TimelineThroughMS > maximumSafeTimeline
		if invalidBounds || !validHash {
			return errors.New("invalid recovery generation checkpoint")
		}
		if previous != nil {
			generationWentBackward := checkpoint.CaptureGeneration < previous.CaptureGeneration
			sequenceWentBackward := checkpoint.CaptureGeneration == previous.CaptureGeneration &&
				checkpoint.CheckpointSequence <= previous.CheckpointSequence
			nonContiguous := checkpoint.CaptureGeneration == previous.CaptureGeneration &&
				checkpoint.CheckpointSequence != previous.CheckpointSequence+1
			timelineWentBackward := checkpoint.TimelineThroughMS < previous.TimelineThroughMS
			if generationWentBackward || sequenceWentBackward || nonContiguous || timelineWentBackward {
				return errors.New("unordered recovery generation checkpoints")
			}
		}
		previous = checkpoint
	}
	return nil
}

func recoveryPageItemCount(page recoveryPage) int {
	return len(page.ParticipantInstances) +
		len(page.ParticipantIntervals) +
		len(page.Tracks) +
		len(page.TrackIntervals) +
		len(page.Segments) +
		len(page.Gaps) +
		len(page.GenerationCheckpoints)
}

func recoveryBindingChanged(page recoveryPage, state RecoveryState, snapshotID string) bool {
	return page.RecoverySnapshotID != snapshotID ||
		page.CaptureGeneration != state.CaptureGeneration ||
		page.CaptureStartedAt != state.CaptureStartedAt ||
		page.BindingMode != state.BindingMode ||
		!sameOptionalInt(page.TerminalTimelineEnd, state.TerminalTimelineEnd)
}

func validateRecoveryPage(page recoveryPage, fence CaptureFence) error {
	if page.ProtocolVersion != CaptureProtocolVersion || page.RecoverySnapshotID == "" || page.CaptureGeneration <= 0 ||
		(fence.CaptureGeneration > 0 && page.CaptureGeneration != fence.CaptureGeneration) || page.CaptureStartedAt == "" ||
		(page.BindingMode != "capture" && page.BindingMode != "seal-only") {
		return errors.New("invalid recovery envelope")
	}
	if _, err := time.Parse(time.RFC3339Nano, page.CaptureStartedAt); err != nil {
		return errors.New("invalid recovery timeline origin")
	}
	if page.BindingMode == "seal-only" && page.TerminalTimelineEnd == nil {
		return errors.New("terminal recovery has no timeline end")
	}
	const maximumSafeTimeline = int64(24 * time.Hour / time.Millisecond)
	if page.TerminalTimelineEnd != nil &&
		(*page.TerminalTimelineEnd < 0 || *page.TerminalTimelineEnd > maximumSafeTimeline) {
		return errors.New("invalid terminal recovery timeline")
	}
	for _, segment := range page.Segments {
		if segment.CaptureGeneration <= 0 || segment.CaptureGeneration > page.CaptureGeneration ||
			segment.UploadID == "" || validateSegment(segment.SegmentDescriptor) != nil {
			return errors.New("invalid recovered segment")
		}
	}
	return nil
}

func appendRecoveryPage(state *RecoveryState, page recoveryPage) {
	for _, item := range page.ParticipantInstances {
		state.ParticipantInstances = append(state.ParticipantInstances, CheckpointRecord{
			Category:              "participantInstances",
			ParticipantInstanceID: item.ParticipantInstanceID,
			DisplayName:           item.DisplayName,
		})
	}
	for _, item := range page.ParticipantIntervals {
		start := item.StartMS
		state.ParticipantIntervals = append(state.ParticipantIntervals, CheckpointRecord{
			Category:              "participantIntervals",
			ParticipantInstanceID: item.ParticipantInstanceID,
			StartMS:               &start,
			EndMS:                 item.EndMS,
		})
	}
	for _, item := range page.Tracks {
		state.Tracks = append(state.Tracks, CheckpointRecord{
			Category:              "tracks",
			TrackID:               item.TrackID,
			ParticipantInstanceID: item.ParticipantInstanceID,
			Source:                item.Source,
		})
	}
	for _, item := range page.TrackIntervals {
		start := item.StartMS
		state.TrackIntervals = append(state.TrackIntervals, CheckpointRecord{
			Category: "trackIntervals",
			TrackID:  item.TrackID,
			StartMS:  &start,
			EndMS:    item.EndMS,
			State:    item.State,
		})
	}
	for _, item := range page.Segments {
		state.Segments = append(state.Segments, DurableSegment{
			CaptureGeneration: item.CaptureGeneration,
			UploadID:          item.UploadID,
			Committed:         true,
			SegmentDescriptor: item.SegmentDescriptor,
		})
	}
	for _, item := range page.Gaps {
		start := item.StartMS
		state.Gaps = append(state.Gaps, CheckpointRecord{
			Category: "gaps",
			TrackID:  item.TrackID,
			StartMS:  &start,
			EndMS:    item.EndMS,
			Scope:    item.Scope,
			Reason:   item.Reason,
		})
	}
	state.GenerationCheckpoints = append(state.GenerationCheckpoints, page.GenerationCheckpoints...)
}

func sameOptionalInt(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
