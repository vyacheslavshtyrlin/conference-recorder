package recording

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"

	"conference/recording-bot/internal/signaling"
)

const (
	participantDomain = "conference.recording-capture.v2.participant-instance\x00"
	trackDomain       = "conference.recording-capture.v2.track\x00"
)

// ParticipantInstanceID derives the canonical recording-scoped participant ID.
func ParticipantInstanceID(recordingID, participantID string) string {
	return "pi_" + digest(participantDomain, recordingID, participantID)
}

// TrackID derives the canonical logical track ID. Producer identity is
// deliberately absent so a source replacement retains the same track ID.
func TrackID(recordingID, participantInstanceID string, source signaling.Source) (string, error) {
	if !source.Valid() {
		return "", fmt.Errorf("unsupported source %q", source)
	}

	return "trk_" + digest(
		trackDomain,
		recordingID,
		participantInstanceID,
		string(source),
	), nil
}

func digest(domain string, values ...string) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(domain))
	for _, value := range values {
		writeLengthPrefixed(hasher, value)
	}

	return base64.RawURLEncoding.EncodeToString(hasher.Sum(nil))
}

func writeLengthPrefixed(destination hash.Hash, value string) {
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len([]byte(value))))
	_, _ = destination.Write(length)
	_, _ = destination.Write([]byte(value))
}
