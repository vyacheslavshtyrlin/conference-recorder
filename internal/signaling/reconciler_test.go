package signaling

import (
	"errors"
	"testing"
)

func TestReconcilerCameraAndScreenLifecycle(t *testing.T) {
	snapshot := baseSnapshot()
	reconciler, err := NewReconciler(snapshot)
	if err != nil {
		t.Fatal(err)
	}

	events := []Event{
		{
			Type: "bot:producerAdded", RecordingID: snapshot.RecordingID, Revision: 11,
			Producer: &Producer{
				ProducerID: "screen-1", ParticipantID: "peer-alice", DisplayName: "Alice",
				Kind: KindVideo, Source: SourceScreen,
			},
		},
		{Type: "bot:producerPaused", RecordingID: snapshot.RecordingID, Revision: 12, ProducerID: "camera-1"},
		{Type: "bot:producerResumed", RecordingID: snapshot.RecordingID, Revision: 13, ProducerID: "camera-1"},
		{Type: "bot:producerClosed", RecordingID: snapshot.RecordingID, Revision: 14, ProducerID: "screen-1"},
		{
			Type: "bot:producerAdded", RecordingID: snapshot.RecordingID, Revision: 15,
			Producer: &Producer{
				ProducerID: "screen-2", ParticipantID: "peer-alice", DisplayName: "Alice",
				Kind: KindVideo, Source: SourceScreen,
			},
		},
		{Type: "bot:producerClosed", RecordingID: snapshot.RecordingID, Revision: 16, ProducerID: "screen-2"},
	}
	for _, event := range events {
		applied, err := reconciler.Apply(event)
		if err != nil {
			t.Fatalf("apply revision %d: %v", event.Revision, err)
		}
		if !applied {
			t.Fatalf("revision %d was not applied", event.Revision)
		}
	}
	if reconciler.Revision() != 16 {
		t.Fatalf("unexpected revision %d", reconciler.Revision())
	}
	if len(reconciler.Producers()) != 2 {
		t.Fatalf("expected camera and mic after screen stop, got %d producers", len(reconciler.Producers()))
	}
}

func TestReconcilerAcceptsExactDuplicateAndRejectsGapOrConflict(t *testing.T) {
	reconciler, err := NewReconciler(baseSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	event := Event{
		Type: "bot:producerPaused", RecordingID: "rec-1", Revision: 11, ProducerID: "camera-1",
	}
	if _, err := reconciler.Apply(event); err != nil {
		t.Fatal(err)
	}
	applied, err := reconciler.Apply(event)
	if err != nil || applied {
		t.Fatalf("exact duplicate result applied=%v err=%v", applied, err)
	}

	conflict := event
	conflict.Type = "bot:producerClosed"
	if _, err := reconciler.Apply(conflict); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("expected revision conflict, got %v", err)
	}
	gap := Event{
		Type: "bot:producerResumed", RecordingID: "rec-1", Revision: 13, ProducerID: "camera-1",
	}
	if _, err := reconciler.Apply(gap); !errors.Is(err, ErrRevisionGap) {
		t.Fatalf("expected revision gap, got %v", err)
	}
}

func TestReconcilerRequiresCloseBeforeSourceReplacement(t *testing.T) {
	reconciler, err := NewReconciler(baseSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	replacement := Event{
		Type: "bot:producerAdded", RecordingID: "rec-1", Revision: 11,
		Producer: &Producer{
			ProducerID: "camera-2", ParticipantID: "peer-alice", DisplayName: "Alice",
			Kind: KindVideo, Source: SourceCamera,
		},
	}
	if _, err := reconciler.Apply(replacement); !errors.Is(err, ErrProducerConflict) {
		t.Fatalf("expected explicit replacement conflict, got %v", err)
	}
}

func TestReconcilerRejectsDuplicateSnapshotIdentities(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Snapshot)
	}{
		{
			name: "participant",
			mutate: func(snapshot *Snapshot) {
				snapshot.Participants = append(snapshot.Participants, snapshot.Participants[0])
			},
		},
		{
			name: "producer",
			mutate: func(snapshot *Snapshot) {
				snapshot.Producers = append(snapshot.Producers, snapshot.Producers[0])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := baseSnapshot()
			test.mutate(&snapshot)
			if _, err := NewReconciler(snapshot); err == nil {
				t.Fatal("duplicate snapshot identity was accepted")
			}
		})
	}
}

func TestReconcilerRetainsExactDuplicateIdentityBeyondLegacyWindow(t *testing.T) {
	reconciler, err := NewReconciler(baseSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	first := Event{
		Type: "bot:participantUpdated", RecordingID: "rec-1", Revision: 11,
		Participant: &Participant{ParticipantID: "peer-alice", DisplayName: "Alice"},
	}
	if _, err := reconciler.Apply(first); err != nil {
		t.Fatal(err)
	}
	for revision := uint64(12); revision <= 300; revision++ {
		event := first
		event.Revision = revision
		if _, err := reconciler.Apply(event); err != nil {
			t.Fatalf("apply revision %d: %v", revision, err)
		}
	}
	applied, err := reconciler.Apply(first)
	if err != nil || applied {
		t.Fatalf("old exact duplicate result applied=%v err=%v", applied, err)
	}
}

func TestReconcilerEventHashCapacityFailsClosed(t *testing.T) {
	reconciler, err := NewReconciler(baseSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	for index := range maxRetainedEventHashes {
		reconciler.eventHashes[uint64(index)+1_000] = [32]byte{1}
	}
	event := Event{
		Type: "bot:participantUpdated", RecordingID: "rec-1", Revision: 11,
		Participant: &Participant{ParticipantID: "peer-alice", DisplayName: "Alice"},
	}
	if _, err := reconciler.Apply(event); !errors.Is(err, ErrRevisionCapacity) {
		t.Fatalf("expected bounded revision capacity failure, got %v", err)
	}
}

func baseSnapshot() Snapshot {
	return Snapshot{
		ProtocolVersion: ProtocolVersion,
		RecordingID:     "rec-1",
		Revision:        10,
		Participants:    []Participant{{ParticipantID: "peer-alice", DisplayName: "Alice"}},
		Producers: []Producer{
			{
				ProducerID: "camera-1", ParticipantID: "peer-alice", DisplayName: "Alice",
				Kind: KindVideo, Source: SourceCamera,
			},
			{
				ProducerID: "mic-1", ParticipantID: "peer-alice", DisplayName: "Alice",
				Kind: KindAudio, Source: SourceMic,
			},
		},
	}
}
