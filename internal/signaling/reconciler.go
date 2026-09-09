package signaling

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrRevisionGap means the authoritative event cursor skipped a revision.
	ErrRevisionGap = errors.New("recording-bot revision gap")
	// ErrRevisionConflict means the same revision arrived with a different payload.
	ErrRevisionConflict = errors.New("recording-bot revision conflict")
	// ErrProducerConflict means two producers claim one logical participant source.
	ErrProducerConflict = errors.New("recording-bot producer conflict")
	// ErrRevisionCapacity means the bounded session cannot retain another event
	// hash while preserving exact-duplicate verification for every accepted event.
	ErrRevisionCapacity = errors.New("recording-bot revision capacity exhausted")
)

const maxRetainedEventHashes = 65_536

type logicalTrackKey struct {
	participantID string
	source        Source
}

// Reconciler owns the authoritative in-memory v1 snapshot and ordered event
// cursor. It never exposes raw participant or producer IDs as durable output.
type Reconciler struct {
	recordingID   string
	revision      uint64
	participants  map[string]Participant
	producers     map[string]Producer
	logicalTracks map[logicalTrackKey]string
	eventHashes   map[uint64][sha256.Size]byte
}

// NewReconciler validates and installs one authoritative snapshot barrier.
func NewReconciler(snapshot Snapshot) (*Reconciler, error) {
	if snapshot.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version %q", snapshot.ProtocolVersion)
	}
	if snapshot.RecordingID == "" {
		return nil, errors.New("snapshot recordingId is required")
	}

	reconciler := &Reconciler{
		recordingID:   snapshot.RecordingID,
		revision:      snapshot.Revision,
		participants:  make(map[string]Participant, len(snapshot.Participants)),
		producers:     make(map[string]Producer, len(snapshot.Producers)),
		logicalTracks: make(map[logicalTrackKey]string, len(snapshot.Producers)),
		eventHashes:   map[uint64][sha256.Size]byte{},
	}
	for _, participant := range snapshot.Participants {
		if _, exists := reconciler.participants[participant.ParticipantID]; exists {
			return nil, errors.New("snapshot contains a duplicate participantId")
		}
		if err := reconciler.upsertParticipant(participant); err != nil {
			return nil, err
		}
	}
	for _, producer := range snapshot.Producers {
		if _, exists := reconciler.producers[producer.ProducerID]; exists {
			return nil, errors.New("snapshot contains a duplicate producerId")
		}
		if err := reconciler.addOrUpdateProducer(producer); err != nil {
			return nil, err
		}
	}

	return reconciler, nil
}

// Revision returns the last authoritative revision applied.
func (reconciler *Reconciler) Revision() uint64 {
	return reconciler.revision
}

// Producers returns a copy of the current raw in-memory producer descriptors.
func (reconciler *Reconciler) Producers() []Producer {
	result := make([]Producer, 0, len(reconciler.producers))
	for _, producer := range reconciler.producers {
		result = append(result, producer)
	}

	return result
}

func (reconciler *Reconciler) participantsSnapshot() []Participant {
	result := make([]Participant, 0, len(reconciler.participants))
	for _, participant := range reconciler.participants {
		result = append(result, participant)
	}
	return result
}

// Apply accepts exact duplicate delivery but fails closed on a gap or a
// conflicting duplicate.
func (reconciler *Reconciler) Apply(event Event) (bool, error) {
	if event.RecordingID != reconciler.recordingID {
		return false, errors.New("event recordingId does not match snapshot")
	}

	hash, err := hashEvent(event)
	if err != nil {
		return false, err
	}
	if event.Revision <= reconciler.revision {
		if reconciler.eventHashes[event.Revision] == hash {
			return false, nil
		}
		return false, fmt.Errorf("%w at revision %d", ErrRevisionConflict, event.Revision)
	}
	if event.Revision != reconciler.revision+1 {
		return false, fmt.Errorf(
			"%w: expected %d, received %d",
			ErrRevisionGap,
			reconciler.revision+1,
			event.Revision,
		)
	}
	if len(reconciler.eventHashes) >= maxRetainedEventHashes {
		return false, ErrRevisionCapacity
	}
	if err := reconciler.applyEvent(event); err != nil {
		return false, err
	}

	reconciler.revision = event.Revision
	reconciler.eventHashes[event.Revision] = hash
	return true, nil
}

func (reconciler *Reconciler) applyEvent(event Event) error {
	switch event.Type {
	case "bot:participantAdded", "bot:participantUpdated":
		if event.Participant == nil {
			return errors.New("participant descriptor is required")
		}
		return reconciler.upsertParticipant(*event.Participant)
	case "bot:participantLeft":
		if event.ParticipantID == "" {
			return errors.New("participantId is required")
		}
		delete(reconciler.participants, event.ParticipantID)
		return nil
	case "bot:producerAdded", "bot:producerUpdated":
		if event.Producer == nil {
			return errors.New("producer descriptor is required")
		}
		return reconciler.addOrUpdateProducer(*event.Producer)
	case "bot:producerPaused":
		return reconciler.setPaused(event.ProducerID, true)
	case "bot:producerResumed":
		return reconciler.setPaused(event.ProducerID, false)
	case "bot:producerClosed":
		return reconciler.closeProducer(event.ProducerID)
	default:
		return fmt.Errorf("unsupported lifecycle event %q", event.Type)
	}
}

func (reconciler *Reconciler) upsertParticipant(participant Participant) error {
	if participant.ParticipantID == "" {
		return errors.New("participantId is required")
	}
	reconciler.participants[participant.ParticipantID] = participant
	return nil
}

func (reconciler *Reconciler) addOrUpdateProducer(producer Producer) error {
	if err := validateProducer(producer); err != nil {
		return err
	}
	if _, exists := reconciler.participants[producer.ParticipantID]; !exists {
		return errors.New("producer references an unknown participant")
	}

	key := logicalTrackKey{
		participantID: producer.ParticipantID,
		source:        producer.Source,
	}
	currentProducerID, occupied := reconciler.logicalTracks[key]
	if occupied && currentProducerID != producer.ProducerID {
		return fmt.Errorf(
			"%w: source %q already has an active producer",
			ErrProducerConflict,
			producer.Source,
		)
	}

	if previous, exists := reconciler.producers[producer.ProducerID]; exists {
		previousKey := logicalTrackKey{
			participantID: previous.ParticipantID,
			source:        previous.Source,
		}
		if previousKey != key {
			delete(reconciler.logicalTracks, previousKey)
		}
	}

	reconciler.producers[producer.ProducerID] = producer
	reconciler.logicalTracks[key] = producer.ProducerID
	return nil
}

func (reconciler *Reconciler) setPaused(producerID string, paused bool) error {
	producer, exists := reconciler.producers[producerID]
	if !exists {
		return errors.New("unknown producer")
	}
	producer.Paused = paused
	reconciler.producers[producerID] = producer
	return nil
}

func (reconciler *Reconciler) closeProducer(producerID string) error {
	producer, exists := reconciler.producers[producerID]
	if !exists {
		return errors.New("unknown producer")
	}
	delete(reconciler.producers, producerID)
	delete(reconciler.logicalTracks, logicalTrackKey{
		participantID: producer.ParticipantID,
		source:        producer.Source,
	})
	return nil
}

func hashEvent(event Event) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(event)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("encode lifecycle event: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func validateProducer(producer Producer) error {
	if producer.ProducerID == "" || producer.ParticipantID == "" {
		return errors.New("producerId and participantId are required")
	}
	if !producer.Source.Valid() {
		return fmt.Errorf("unsupported source %q", producer.Source)
	}
	if producer.Source == SourceMic && producer.Kind != KindAudio {
		return errors.New("mic producer must be audio")
	}
	if producer.Source != SourceMic && producer.Kind != KindVideo {
		return errors.New("camera and screen producers must be video")
	}
	return nil
}
