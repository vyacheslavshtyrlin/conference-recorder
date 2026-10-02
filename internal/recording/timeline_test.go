package recording

import (
	"errors"
	"testing"
)

func TestTimelineKeepsLogicalTracksAcrossReplacementPauseAndScreen(t *testing.T) {
	timeline, err := NewTimelineState("rec-1")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := Snapshot{ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers: []Producer{
			{ProducerID: "raw-camera-1", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindVideo, Source: SourceCamera},
			{ProducerID: "raw-mic-1", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindAudio, Source: SourceMic},
			{ProducerID: "raw-screen-1", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindVideo, Source: SourceScreen},
		},
	}
	if err := timeline.ApplySnapshot(snapshot, 0); err != nil {
		t.Fatal(err)
	}
	if err := timeline.ApplyEvent(Event{Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 2, ProducerID: "raw-camera-1"}, 1000); err != nil {
		t.Fatal(err)
	}
	if err := timeline.ApplyEvent(Event{Type: "bot:producerResumed", RecordingID: "rec-1", Revision: 3, ProducerID: "raw-camera-1"}, 2000); err != nil {
		t.Fatal(err)
	}
	if err := timeline.ApplyEvent(Event{Type: "bot:producerClosed", RecordingID: "rec-1", Revision: 4, ProducerID: "raw-camera-1"}, 3000); err != nil {
		t.Fatal(err)
	}
	replacement := Producer{ProducerID: "raw-camera-2", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindVideo, Source: SourceCamera}
	if err := timeline.ApplyEvent(Event{Type: "bot:producerAdded", RecordingID: "rec-1", Revision: 5, Producer: &replacement}, 4000); err != nil {
		t.Fatal(err)
	}
	firstTrack, _, exists := timeline.TrackForProducer("raw-camera-2")
	if !exists {
		t.Fatal("replacement track missing")
	}
	wantTrack, _ := TrackID("rec-1", ParticipantInstanceID("rec-1", "raw-p1"), SourceCamera)
	if firstTrack != wantTrack {
		t.Fatalf("replacement changed logical track: %q", firstTrack)
	}
	records, err := timeline.RecordsThrough(5000, true)
	if err != nil {
		t.Fatal(err)
	}
	trackMetadata, cameraIntervals, sources := 0, 0, map[Source]bool{}
	for _, record := range records {
		if record.Category == "tracks" {
			trackMetadata++
			sources[record.Source] = true
		}
		if record.Category == "trackIntervals" && record.TrackID == wantTrack {
			cameraIntervals++
		}
	}
	if trackMetadata != 3 || cameraIntervals != 4 || !sources[SourceMic] || !sources[SourceScreen] {
		t.Fatalf("unexpected timeline topology: tracks=%d cameraIntervals=%d sources=%v", trackMetadata, cameraIntervals, sources)
	}
}

func TestTimelineRejectsTrackOutsidePresenceOnRecovery(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "raw-p1")
	trackID, _ := TrackID("rec-1", participantID, SourceCamera)
	start0, end10, start11, end12 := int64(0), int64(10), int64(11), int64(12)
	timeline, _ := NewTimelineState("rec-1")
	err := timeline.Restore(RecoveryState{
		ParticipantInstances: []CheckpointRecord{{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: "Alex"}},
		ParticipantIntervals: []CheckpointRecord{{Category: "participantIntervals", ParticipantInstanceID: participantID, StartMS: &start0, EndMS: &end10}},
		Tracks:               []CheckpointRecord{{Category: "tracks", TrackID: trackID, ParticipantInstanceID: participantID, Source: SourceCamera}},
		TrackIntervals:       []CheckpointRecord{{Category: "trackIntervals", TrackID: trackID, StartMS: &start11, EndMS: &end12, State: "active"}},
	})
	if err == nil {
		t.Fatal("expected lifecycle containment failure")
	}
}

func TestTimelineRestoreAcceptsOnlyFinalOpenIntervalsAndPreservesContainment(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "raw-p1")
	trackID, _ := TrackID("rec-1", participantID, SourceCamera)
	start0, end10, start10 := int64(0), int64(10), int64(10)
	recovery := RecoveryState{
		ParticipantInstances: []CheckpointRecord{{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: "Alex"}},
		ParticipantIntervals: []CheckpointRecord{
			{Category: "participantIntervals", ParticipantInstanceID: participantID, StartMS: &start0, EndMS: &end10},
			{Category: "participantIntervals", ParticipantInstanceID: participantID, StartMS: &start10, EndMS: nil},
		},
		Tracks:         []CheckpointRecord{{Category: "tracks", TrackID: trackID, ParticipantInstanceID: participantID, Source: SourceCamera}},
		TrackIntervals: []CheckpointRecord{{Category: "trackIntervals", TrackID: trackID, StartMS: &start10, EndMS: nil, State: "active"}},
	}
	timeline, _ := NewTimelineState("rec-1")
	if err := timeline.Restore(recovery); err != nil {
		t.Fatal(err)
	}
	if _, err := timeline.RecordsThrough(20, false); err != nil {
		t.Fatalf("pre-seal open interval was rejected: %v", err)
	}
	closed, err := timeline.RecordsThrough(20, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range closed {
		isInterval := record.Category == "participantIntervals" || record.Category == "trackIntervals"
		if isInterval && record.EndMS == nil {
			t.Fatal("terminal timeline retained an open interval")
		}
	}

	invalid := recovery
	invalid.TrackIntervals = append(invalid.TrackIntervals,
		CheckpointRecord{Category: "trackIntervals", TrackID: trackID, StartMS: &start0, EndMS: nil, State: "paused"},
	)
	if err := timeline.Restore(invalid); err == nil {
		t.Fatal("non-final open recovery interval was accepted")
	}
}

func TestCheckpointBoundaryDefersOpenIntervalAtTimeZero(t *testing.T) {
	timeline, _ := NewTimelineState("rec-1")
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion,
		RecordingID:     "rec-1",
		Revision:        1,
		Participants:    []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	boundary, err := timeline.CheckpointBoundary(0, false)
	if !errors.Is(err, ErrCheckpointBoundaryUnavailable) || boundary != 0 {
		t.Fatalf("unexpected first checkpoint boundary: %d %v", boundary, err)
	}
	boundary, err = timeline.CheckpointBoundary(1, false)
	if err != nil || boundary != 1 {
		t.Fatalf("unexpected representable checkpoint boundary: %d %v", boundary, err)
	}
}

func TestCheckpointBoundaryDefersFiniteGapPastRequestedTime(t *testing.T) {
	timeline, _ := NewTimelineState("rec-1")
	trackID, err := TrackID("rec-1", ParticipantInstanceID("rec-1", "raw-p1"), SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers: []Producer{{
			ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
			Kind: KindVideo, Source: SourceCamera,
		}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := timeline.DeclareGap(trackID, 500, 600, "source_unavailable"); err != nil {
		t.Fatal(err)
	}
	boundary, err := timeline.CheckpointBoundary(550, false)
	if !errors.Is(err, ErrCheckpointBoundaryUnavailable) || boundary != 0 {
		t.Fatalf("checkpoint exposed a future gap: %d %v", boundary, err)
	}
	boundary, err = timeline.CheckpointBoundary(600, false)
	if err != nil || boundary != 600 {
		t.Fatalf("checkpoint did not include a completed gap: %d %v", boundary, err)
	}
	records, err := timeline.RecordsThrough(boundary, false)
	if err != nil {
		t.Fatal(err)
	}
	foundGap := false
	for _, record := range records {
		if record.Category == "gaps" && record.EndMS != nil && *record.EndMS == 600 {
			foundGap = true
		}
	}
	if !foundGap {
		t.Fatalf("completed gap was not preserved: %+v", records)
	}
}

func TestCheckpointBoundaryDefersBeforePendingCoverageBecomesGap(t *testing.T) {
	timeline, _ := NewTimelineState("rec-1")
	trackID, err := TrackID("rec-1", ParticipantInstanceID("rec-1", "raw-p1"), SourceCamera)
	if err != nil {
		t.Fatal(err)
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers: []Producer{{
			ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
			Kind: KindVideo, Source: SourceCamera,
		}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := timeline.BeginTrackCoverage(trackID, 0); err != nil {
		t.Fatal(err)
	}
	boundary, err := timeline.CheckpointBoundary(500, false)
	if !errors.Is(err, ErrCheckpointBoundaryUnavailable) || boundary != 0 {
		t.Fatalf("checkpoint crossed unresolved track coverage: %d %v", boundary, err)
	}
	if err := timeline.ResolveTrackCoverage(trackID, 0, 600); err != nil {
		t.Fatal(err)
	}
	boundary, err = timeline.CheckpointBoundary(600, false)
	if err != nil || boundary != 600 {
		t.Fatalf("resolved coverage did not unblock checkpoint: %d %v", boundary, err)
	}
	records, err := timeline.RecordsThrough(boundary, false)
	if err != nil {
		t.Fatal(err)
	}
	gapCount := 0
	for _, record := range records {
		if record.Category != "gaps" {
			continue
		}
		gapCount++
		if record.StartMS == nil || record.EndMS == nil || *record.StartMS != 0 || *record.EndMS != 600 {
			t.Fatalf("unexpected resolved coverage gap: %+v", record)
		}
	}
	if gapCount != 1 {
		t.Fatalf("resolved coverage produced %d gaps: %+v", gapCount, records)
	}
}

func TestAuthoritativeReconnectSnapshotClosesAbsentAndAppliesPausedState(t *testing.T) {
	timeline, _ := NewTimelineState("rec-1")
	first := Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{
			{ParticipantID: "raw-p1", DisplayName: "Alex"},
			{ParticipantID: "raw-p2", DisplayName: "Blair"},
		},
		Producers: []Producer{
			{ProducerID: "camera-1", ParticipantID: "raw-p1", DisplayName: "Alex", Kind: KindVideo, Source: SourceCamera},
			{ProducerID: "camera-2", ParticipantID: "raw-p2", DisplayName: "Blair", Kind: KindVideo, Source: SourceCamera},
		},
	}
	if err := timeline.ApplySnapshot(first, 0); err != nil {
		t.Fatal(err)
	}
	recovered, err := timeline.RecordsThrough(50, false)
	if err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewTimelineState("rec-1")
	state := RecoveryState{}
	for _, record := range recovered {
		switch record.Category {
		case "participantInstances":
			state.ParticipantInstances = append(state.ParticipantInstances, record)
		case "participantIntervals":
			state.ParticipantIntervals = append(state.ParticipantIntervals, record)
		case "tracks":
			state.Tracks = append(state.Tracks, record)
		case "trackIntervals":
			state.TrackIntervals = append(state.TrackIntervals, record)
		}
	}
	if err := restarted.Restore(state); err != nil {
		t.Fatal(err)
	}
	barrier := Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 2,
		Participants: []Participant{{ParticipantID: "raw-p1", DisplayName: "Alex"}},
		Producers: []Producer{{
			ProducerID: "camera-1", ParticipantID: "raw-p1", DisplayName: "Alex",
			Kind: KindVideo, Source: SourceCamera, Paused: true,
		}},
	}
	if err := restarted.ApplySnapshot(barrier, 100); err != nil {
		t.Fatal(err)
	}
	records, err := restarted.RecordsThrough(200, false)
	if err != nil {
		t.Fatal(err)
	}
	p1 := ParticipantInstanceID("rec-1", "raw-p1")
	p2 := ParticipantInstanceID("rec-1", "raw-p2")
	track1, _ := TrackID("rec-1", p1, SourceCamera)
	track2, _ := TrackID("rec-1", p2, SourceCamera)
	assertClosedAt(t, records, "participantIntervals", p2, 100)
	assertClosedAt(t, records, "trackIntervals", track2, 100)
	assertClosedAt(t, records, "trackIntervals", track1, 100)
	if _, _, active := restarted.TrackForProducer("camera-1"); active {
		t.Fatal("paused producer remained writable after authoritative snapshot")
	}
}

func assertClosedAt(t *testing.T, records []CheckpointRecord, category, owner string, endMS int64) {
	t.Helper()
	for _, record := range records {
		if record.Category != category {
			continue
		}
		matches := record.ParticipantInstanceID == owner || record.TrackID == owner
		if matches && record.EndMS != nil && *record.EndMS == endMS {
			return
		}
	}
	t.Fatalf("%s %s was not closed at %d", category, owner, endMS)
}

func TestTimelineRejectsDuplicateMetadataAndEmptyDisplayName(t *testing.T) {
	participantID := ParticipantInstanceID("rec-1", "raw-p1")
	trackID, _ := TrackID("rec-1", participantID, SourceCamera)
	cases := []RecoveryState{
		{ParticipantInstances: []CheckpointRecord{
			{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: ""},
		}},
		{ParticipantInstances: []CheckpointRecord{
			{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: "Alex"},
			{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: "Alex"},
		}},
		{
			ParticipantInstances: []CheckpointRecord{{Category: "participantInstances", ParticipantInstanceID: participantID, DisplayName: "Alex"}},
			Tracks: []CheckpointRecord{
				{Category: "tracks", TrackID: trackID, ParticipantInstanceID: participantID, Source: SourceCamera},
				{Category: "tracks", TrackID: trackID, ParticipantInstanceID: participantID, Source: SourceCamera},
			},
		},
	}
	for index, state := range cases {
		timeline, _ := NewTimelineState("rec-1")
		if err := timeline.Restore(state); err == nil {
			t.Fatalf("invalid recovery case %d was accepted", index)
		}
	}
}

func TestProducerUpdatedTransitionsPausedAndLogicalIdentityAtExactBoundary(t *testing.T) {
	timeline, _ := NewTimelineState("rec-1")
	producer := Producer{
		ProducerID: "producer-1", ParticipantID: "raw-p1", DisplayName: "Alex",
		Kind: KindVideo, Source: SourceCamera,
	}
	if err := timeline.ApplySnapshot(Snapshot{
		ProtocolVersion: ProtocolVersion, RecordingID: "rec-1", Revision: 1,
		Participants: []Participant{
			{ParticipantID: "raw-p1", DisplayName: "Alex"},
			{ParticipantID: "raw-p2", DisplayName: "Blair"},
		},
		Producers: []Producer{producer},
	}, 0); err != nil {
		t.Fatal(err)
	}
	paused := producer
	paused.Paused = true
	if err := timeline.ApplyEvent(Event{
		Type: "bot:producerUpdated", RecordingID: "rec-1", Revision: 2, Producer: &paused,
	}, 100); err != nil {
		t.Fatal(err)
	}
	if _, _, active := timeline.TrackForProducer(producer.ProducerID); active {
		t.Fatal("paused producer update retained an active writer identity")
	}
	replaced := producer
	replaced.ParticipantID = "raw-p2"
	replaced.DisplayName = "Blair"
	replaced.Source = SourceScreen
	if err := timeline.ApplyEvent(Event{
		Type: "bot:producerUpdated", RecordingID: "rec-1", Revision: 3, Producer: &replaced,
	}, 200); err != nil {
		t.Fatal(err)
	}
	newTrackID, _, active := timeline.TrackForProducer(producer.ProducerID)
	wantNewTrackID, _ := TrackID(
		"rec-1",
		ParticipantInstanceID("rec-1", "raw-p2"),
		SourceScreen,
	)
	if !active || newTrackID != wantNewTrackID {
		t.Fatalf("updated logical identity was not installed: %q", newTrackID)
	}
	records, err := timeline.RecordsThrough(300, true)
	if err != nil {
		t.Fatal(err)
	}
	oldTrackID, _ := TrackID(
		"rec-1",
		ParticipantInstanceID("rec-1", "raw-p1"),
		SourceCamera,
	)
	assertClosedAt(t, records, "trackIntervals", oldTrackID, 100)
	pausedClosed := false
	for _, record := range records {
		if record.Category == "trackIntervals" && record.TrackID == oldTrackID &&
			record.State == "paused" && record.EndMS != nil && *record.EndMS == 200 {
			pausedClosed = true
		}
	}
	if !pausedClosed {
		t.Fatal("paused update interval was not closed at logical replacement")
	}
}
