package recording

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"conference/recording-bot/internal/signaling"
)

const CaptureProtocolVersion = "recording-capture.v2"

type SegmentDescriptor struct {
	TrackID               string           `json:"trackId"`
	ParticipantInstanceID string           `json:"participantInstanceId"`
	Source                signaling.Source `json:"source"`
	Sequence              int32            `json:"sequence"`
	Container             string           `json:"container"`
	Codec                 string           `json:"codec"`
	ContentType           string           `json:"contentType"`
	ExpectedBytes         int64            `json:"expectedBytes"`
	SHA256                string           `json:"sha256"`
	StartMS               int64            `json:"startMs"`
	EndMS                 int64            `json:"endMs"`
}

type DurableSegment struct {
	CaptureGeneration int32  `json:"captureGeneration"`
	UploadID          string `json:"uploadId,omitempty"`
	LocalPath         string `json:"localPath,omitempty"`
	Committed         bool   `json:"committed"`
	SegmentDescriptor
}

type CheckpointRecord struct {
	Category              string           `json:"category"`
	ParticipantInstanceID string           `json:"participantInstanceId,omitempty"`
	DisplayName           string           `json:"displayName,omitempty"`
	TrackID               string           `json:"trackId,omitempty"`
	Source                signaling.Source `json:"source,omitempty"`
	StartMS               *int64           `json:"startMs,omitempty"`
	EndMS                 *int64           `json:"endMs"`
	State                 string           `json:"state,omitempty"`
	Scope                 string           `json:"scope,omitempty"`
	Reason                string           `json:"reason,omitempty"`
}

func (record CheckpointRecord) MarshalJSON() ([]byte, error) {
	value := map[string]any{"category": record.Category}
	switch record.Category {
	case "participantInstances":
		value["participantInstanceId"] = record.ParticipantInstanceID
		value["displayName"] = record.DisplayName
	case "participantIntervals":
		value["participantInstanceId"] = record.ParticipantInstanceID
		value["startMs"] = record.StartMS
		value["endMs"] = record.EndMS
	case "tracks":
		value["trackId"] = record.TrackID
		value["participantInstanceId"] = record.ParticipantInstanceID
		value["source"] = record.Source
	case "trackIntervals":
		value["trackId"] = record.TrackID
		value["startMs"] = record.StartMS
		value["endMs"] = record.EndMS
		value["state"] = record.State
	case "gaps":
		value["startMs"] = record.StartMS
		value["endMs"] = record.EndMS
		value["scope"] = record.Scope
		if record.Scope == "track" {
			value["trackId"] = record.TrackID
		}
		value["reason"] = record.Reason
	default:
		return nil, errors.New("invalid checkpoint record category")
	}
	return marshalCanonicalJSON(value)
}

func marshalCanonicalJSON(value any) ([]byte, error) {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	result := bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'})
	result = bytes.ReplaceAll(result, []byte(`\u2028`), []byte("\u2028"))
	result = bytes.ReplaceAll(result, []byte(`\u2029`), []byte("\u2029"))
	return result, nil
}

type GenerationCheckpoint struct {
	CaptureGeneration  int32  `json:"captureGeneration"`
	CheckpointSequence int32  `json:"checkpointSequence"`
	TimelineThroughMS  int64  `json:"timelineThroughMs"`
	PayloadSHA256      string `json:"payloadSha256"`
}

type RecoveryState struct {
	CaptureGeneration     int32
	CaptureStartedAt      string
	BindingMode           string
	TerminalTimelineEnd   *int64
	ParticipantInstances  []CheckpointRecord
	ParticipantIntervals  []CheckpointRecord
	Tracks                []CheckpointRecord
	TrackIntervals        []CheckpointRecord
	Segments              []DurableSegment
	Gaps                  []CheckpointRecord
	GenerationCheckpoints []GenerationCheckpoint
}

func validateSegment(segment SegmentDescriptor) error {
	if !validOpaqueID(segment.TrackID, "trk_") ||
		!validOpaqueID(segment.ParticipantInstanceID, "pi_") {
		return errors.New("invalid opaque segment identity")
	}
	if segment.Sequence < 0 || segment.ExpectedBytes < 1 ||
		segment.ExpectedBytes > 2_000_000_000 || segment.StartMS < 0 ||
		segment.EndMS <= segment.StartMS {
		return errors.New("invalid segment bounds")
	}
	if !validCanonicalSHA256(segment.SHA256) {
		return errors.New("invalid segment checksum")
	}
	validTuple := segment.Source == signaling.SourceMic && segment.Container == "webm" &&
		segment.Codec == "opus" && segment.ContentType == "audio/webm"
	validTuple = validTuple || ((segment.Source == signaling.SourceCamera || segment.Source == signaling.SourceScreen) &&
		((segment.Container == "webm" && segment.Codec == "vp8" && segment.ContentType == "video/webm") ||
			(segment.Container == "matroska" && segment.Codec == "h264" && segment.ContentType == "video/x-matroska")))
	if !validTuple {
		return errors.New("invalid source/container/codec/contentType tuple")
	}
	return nil
}

func validCanonicalSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func validOpaqueID(value, prefix string) bool {
	if len(value) != len(prefix)+43 || value[:len(prefix)] != prefix {
		return false
	}
	raw := value[len(prefix):]
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	return err == nil && len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == raw
}

func canonicalCheckpointHash(
	captureGeneration int32,
	timelineThroughMS int64,
	records []CheckpointRecord,
) (string, error) {
	value := map[string]any{
		"protocolVersion":   CaptureProtocolVersion,
		"captureGeneration": captureGeneration,
		"timelineThroughMs": timelineThroughMS,
		"records":           records,
	}
	encoded, err := marshalCanonicalJSON(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func sortCheckpointRecords(records []CheckpointRecord) {
	order := map[string]int{
		"participantInstances": 0,
		"participantIntervals": 1,
		"tracks":               2,
		"trackIntervals":       3,
		"gaps":                 4,
	}
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if order[left.Category] != order[right.Category] {
			return order[left.Category] < order[right.Category]
		}
		return compareCheckpointRecords(left, right) < 0
	})
}

func compareCheckpointRecords(left, right CheckpointRecord) int {
	switch left.Category {
	case "participantInstances":
		return compareText(left.ParticipantInstanceID, right.ParticipantInstanceID)
	case "participantIntervals":
		return firstComparison(
			compareText(left.ParticipantInstanceID, right.ParticipantInstanceID),
			compareNullableTimeline(left.StartMS, right.StartMS),
			compareNullableTimeline(left.EndMS, right.EndMS),
		)
	case "tracks":
		return firstComparison(
			compareText(left.TrackID, right.TrackID),
			compareText(left.ParticipantInstanceID, right.ParticipantInstanceID),
			compareText(string(left.Source), string(right.Source)),
		)
	case "trackIntervals":
		return firstComparison(
			compareText(left.TrackID, right.TrackID),
			compareNullableTimeline(left.StartMS, right.StartMS),
			compareNullableTimeline(left.EndMS, right.EndMS),
			compareText(left.State, right.State),
		)
	case "gaps":
		return firstComparison(
			compareText(left.Scope, right.Scope),
			compareText(left.TrackID, right.TrackID),
			compareNullableTimeline(left.StartMS, right.StartMS),
			compareNullableTimeline(left.EndMS, right.EndMS),
			compareText(left.Reason, right.Reason),
		)
	default:
		return 0
	}
}

func compareText(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareNullableTimeline(left, right *int64) int {
	if left == nil && right == nil {
		return 0
	}
	if left == nil {
		return 1
	}
	if right == nil {
		return -1
	}
	if *left < *right {
		return -1
	}
	if *left > *right {
		return 1
	}
	return 0
}

func firstComparison(values ...int) int {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
