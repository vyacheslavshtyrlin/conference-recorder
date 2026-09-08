package signaling

import (
	"encoding/json"
	"fmt"
	"strings"
)

// MediaKind is the recording-bot.v1 producer kind.
type MediaKind string

const (
	KindAudio MediaKind = "audio"
	KindVideo MediaKind = "video"
)

// Source is the recording-bot.v1 media source.
type Source string

const (
	SourceMic    Source = "mic"
	SourceCamera Source = "camera"
	SourceScreen Source = "screen"
)

// Valid reports whether source belongs to the unchanged v1 source vocabulary.
func (source Source) Valid() bool {
	switch source {
	case SourceMic, SourceCamera, SourceScreen:
		return true
	default:
		return false
	}
}

// ValidateCodec rejects signaling descriptors the v2 capture format cannot
// preserve without transcoding in the capture process.
func ValidateCodec(kind MediaKind, source Source, mimeType string) error {
	normalized := strings.ToLower(strings.TrimSpace(mimeType))
	switch {
	case kind == KindAudio && source == SourceMic && normalized == "audio/opus":
		return nil
	case kind == KindVideo && source == SourceCamera && normalized == "video/vp8":
		return nil
	case kind == KindVideo && source == SourceCamera && normalized == "video/h264":
		return nil
	case kind == KindVideo && source == SourceScreen && normalized == "video/vp8":
		return nil
	case kind == KindVideo && source == SourceScreen && normalized == "video/h264":
		return nil
	default:
		return fmt.Errorf(
			"unsupported codec tuple kind=%q source=%q mimeType=%q",
			kind,
			source,
			mimeType,
		)
	}
}

// RouterSupportsCodec inspects the mediasoup router capability projection
// without weakening consumer-level codec validation.
func RouterSupportsCodec(capabilities json.RawMessage, mimeType string) (bool, error) {
	var projection struct {
		Codecs []struct {
			MimeType string `json:"mimeType"`
		} `json:"codecs"`
	}
	if err := json.Unmarshal(capabilities, &projection); err != nil {
		return false, fmt.Errorf("decode router RTP capabilities: %w", err)
	}
	for _, codec := range projection.Codecs {
		if strings.EqualFold(codec.MimeType, mimeType) {
			return true, nil
		}
	}
	return false, nil
}
