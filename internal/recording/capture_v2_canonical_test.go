package recording

import (
	"fmt"
	"strings"
	"testing"
)

func TestCanonicalCheckpointMatchesTypeScriptGoldenVector(t *testing.T) {
	start := int64(0)
	records := []CheckpointRecord{
		{Category: "participantInstances", ParticipantInstanceID: "pi_z", DisplayName: "A<&\u2028B"},
		{Category: "participantIntervals", ParticipantInstanceID: "pi_z", StartMS: &start},
		{Category: "tracks", TrackID: "trk_a", ParticipantInstanceID: "pi_z", Source: SourceCamera},
	}
	value := map[string]any{
		"protocolVersion":   CaptureProtocolVersion,
		"captureGeneration": int32(2),
		"timelineThroughMs": int64(1),
		"records":           records,
	}
	encoded, err := marshalCanonicalJSON(value)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"captureGeneration\":2,\"protocolVersion\":\"recording-capture.v2\",\"records\":[" +
		"{\"category\":\"participantInstances\",\"displayName\":\"A<&\u2028B\",\"participantInstanceId\":\"pi_z\"}," +
		"{\"category\":\"participantIntervals\",\"endMs\":null,\"participantInstanceId\":\"pi_z\",\"startMs\":0}," +
		"{\"category\":\"tracks\",\"participantInstanceId\":\"pi_z\",\"source\":\"camera\",\"trackId\":\"trk_a\"}" +
		"],\"timelineThroughMs\":1}"
	if string(encoded) != want {
		t.Fatalf("canonical JSON differs from TypeScript:\n got: %s\nwant: %s", encoded, want)
	}
	hash, err := canonicalCheckpointHash(2, 1, records)
	if err != nil {
		t.Fatal(err)
	}
	const wantHash = "2fa139136ef5bde61cddd67a773616c7ad1ef460df100fba1343fd4af610c6fc"
	if hash != wantHash {
		t.Fatalf("canonical hash differs from golden vector: got %s for %s", hash, fmt.Sprintf("%q", encoded))
	}
}

func TestCheckpointRecordSortMatchesTypeScriptCategoryComparators(t *testing.T) {
	records := []CheckpointRecord{
		{Category: "tracks", TrackID: "trk_b", ParticipantInstanceID: "pi_a", Source: SourceCamera},
		{Category: "tracks", TrackID: "trk_a", ParticipantInstanceID: "pi_z", Source: SourceScreen},
	}
	sortCheckpointRecords(records)
	if records[0].TrackID != "trk_a" {
		t.Fatalf("tracks were not ordered by trackId first: %+v", records)
	}
}

func TestSegmentChecksumMustBeCanonicalLowercaseHex(t *testing.T) {
	descriptor := fixtureSegmentDescriptor(1, strings.Repeat("A", 64))
	if err := validateSegment(descriptor); err == nil {
		t.Fatal("uppercase segment checksum was accepted")
	}
}
