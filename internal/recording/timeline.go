package recording

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"conference/recording-bot/internal/signaling"
)

var ErrCheckpointBoundaryUnavailable = errors.New("checkpoint boundary has no positive open interval")

type logicalTrackKey struct {
	participantID string
	source        signaling.Source
}

type TimelineState struct {
	recordingID  string
	mu           sync.Mutex
	records      []CheckpointRecord
	participants map[string]string
	producers    map[string]string
}

func NewTimelineState(recordingID string) (*TimelineState, error) {
	if recordingID == "" {
		return nil, errors.New("recording ID is required for timeline")
	}
	return &TimelineState{
		recordingID:  recordingID,
		participants: map[string]string{},
		producers:    map[string]string{},
		records:      []CheckpointRecord{},
	}, nil
}

func (timeline *TimelineState) Restore(recovery RecoveryState) error {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	recordCount := len(recovery.ParticipantInstances) + len(recovery.ParticipantIntervals) +
		len(recovery.Tracks) + len(recovery.TrackIntervals) + len(recovery.Gaps)
	records := make([]CheckpointRecord, 0, recordCount)
	records = append(records, recovery.ParticipantInstances...)
	records = append(records, recovery.ParticipantIntervals...)
	records = append(records, recovery.Tracks...)
	records = append(records, recovery.TrackIntervals...)
	records = append(records, recovery.Gaps...)
	// Recovery checkpoints are pre-seal snapshots. Their final participant and
	// track intervals may therefore remain open; RecordsThrough closes them and
	// applies strict validation when the authoritative terminal boundary arrives.
	if err := validateTimelineRecords(records, true); err != nil {
		return err
	}
	timeline.records = records
	return nil
}

func (timeline *TimelineState) CheckpointBoundary(requestedMS int64, closeOpen bool) (int64, error) {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	if requestedMS < 0 {
		return 0, errors.New("invalid checkpoint boundary")
	}
	if closeOpen {
		return requestedMS, nil
	}
	for _, record := range timeline.records {
		isInterval := record.Category == "participantIntervals" || record.Category == "trackIntervals"
		if !isInterval || record.EndMS != nil || record.StartMS == nil {
			continue
		}
		if *record.StartMS >= requestedMS {
			return 0, ErrCheckpointBoundaryUnavailable
		}
	}
	return requestedMS, nil
}

func (timeline *TimelineState) ApplySnapshot(snapshot signaling.Snapshot, atMS int64) error {
	if snapshot.RecordingID != timeline.recordingID || atMS < 0 {
		return errors.New("timeline snapshot mismatch")
	}
	if err := validateTimelineSnapshot(snapshot); err != nil {
		return err
	}
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	desiredParticipants := make(map[string]signaling.Participant, len(snapshot.Participants))
	for _, participant := range snapshot.Participants {
		desiredParticipants[ParticipantInstanceID(timeline.recordingID, participant.ParticipantID)] = participant
		timeline.ensureParticipantLocked(participant, atMS)
	}
	desiredTracks := make(map[string]signaling.Producer, len(snapshot.Producers))
	for _, producer := range snapshot.Producers {
		participantID := ParticipantInstanceID(timeline.recordingID, producer.ParticipantID)
		trackID, err := TrackID(timeline.recordingID, participantID, producer.Source)
		if err != nil {
			return err
		}
		desiredTracks[trackID] = producer
	}
	for _, record := range append([]CheckpointRecord{}, timeline.records...) {
		if record.EndMS != nil {
			continue
		}
		switch record.Category {
		case "trackIntervals":
			producer, exists := desiredTracks[record.TrackID]
			stateMatches := exists && ((record.State == "paused") == producer.Paused)
			if !stateMatches {
				timeline.closeLastLocked("trackIntervals", record.TrackID, atMS)
			}
		case "participantIntervals":
			if _, exists := desiredParticipants[record.ParticipantInstanceID]; !exists {
				timeline.closeLastLocked("participantIntervals", record.ParticipantInstanceID, atMS)
			}
		}
	}
	timeline.participants = make(map[string]string, len(snapshot.Participants))
	timeline.producers = make(map[string]string, len(snapshot.Producers))
	for _, participant := range snapshot.Participants {
		timeline.ensureParticipantLocked(participant, atMS)
	}
	for _, producer := range snapshot.Producers {
		if err := timeline.ensureProducerLocked(producer, atMS); err != nil {
			return err
		}
	}
	return validateTimelineRecords(timeline.records, true)
}

func (timeline *TimelineState) EventWriterBarriers(event signaling.Event) []string {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	tracks := map[string]struct{}{}
	addProducer := func(producerID string) {
		if trackID := timeline.producers[producerID]; trackID != "" {
			tracks[trackID] = struct{}{}
		}
	}
	switch event.Type {
	case "bot:producerPaused", "bot:producerClosed", "bot:producerUpdated":
		addProducer(event.ProducerID)
		if event.Producer != nil {
			addProducer(event.Producer.ProducerID)
		}
	case "bot:participantLeft":
		participantID := timeline.participants[event.ParticipantID]
		for _, trackID := range timeline.producers {
			if timeline.trackOwnerLocked(trackID) == participantID {
				tracks[trackID] = struct{}{}
			}
		}
	}
	return sortedTrackIDs(tracks)
}

func (timeline *TimelineState) SnapshotWriterBarriers(snapshot signaling.Snapshot) []string {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	desired := make(map[string]signaling.Producer, len(snapshot.Producers))
	for _, producer := range snapshot.Producers {
		desired[producer.ProducerID] = producer
	}
	tracks := map[string]struct{}{}
	for producerID, trackID := range timeline.producers {
		producer, exists := desired[producerID]
		if !exists || producer.Paused {
			tracks[trackID] = struct{}{}
			continue
		}
		participantID := ParticipantInstanceID(timeline.recordingID, producer.ParticipantID)
		desiredTrackID, err := TrackID(timeline.recordingID, participantID, producer.Source)
		if err != nil || desiredTrackID != trackID {
			tracks[trackID] = struct{}{}
		}
	}
	return sortedTrackIDs(tracks)
}

func sortedTrackIDs(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func (timeline *TimelineState) ApplyEvent(event signaling.Event, atMS int64) error {
	if event.RecordingID != timeline.recordingID || atMS < 0 {
		return errors.New("timeline event mismatch")
	}
	if event.Participant != nil {
		if err := validateTimelineParticipant(*event.Participant); err != nil {
			return err
		}
	}
	if event.Producer != nil {
		if err := validateTimelineProducer(*event.Producer); err != nil {
			return err
		}
	}
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	switch event.Type {
	case "bot:participantAdded", "bot:participantUpdated":
		if event.Participant == nil {
			return errors.New("participant event payload missing")
		}
		timeline.ensureParticipantLocked(*event.Participant, atMS)
	case "bot:participantLeft":
		participantID := timeline.participants[event.ParticipantID]
		if participantID == "" {
			return errors.New("participant leave identity missing")
		}
		timeline.closeLastLocked("participantIntervals", participantID, atMS)
		for producerID, trackID := range timeline.producers {
			if timeline.trackOwnerLocked(trackID) == participantID {
				timeline.closeLastLocked("trackIntervals", trackID, atMS)
				delete(timeline.producers, producerID)
			}
		}
	case "bot:producerAdded", "bot:producerUpdated":
		if event.Producer == nil {
			return errors.New("producer event payload missing")
		}
		if event.Type == "bot:producerUpdated" {
			if err := timeline.transitionUpdatedProducerLocked(*event.Producer, atMS); err != nil {
				return err
			}
			break
		}
		if err := timeline.ensureProducerLocked(*event.Producer, atMS); err != nil {
			return err
		}
	case "bot:producerPaused":
		if err := timeline.setProducerStateLocked(event.ProducerID, "paused", atMS); err != nil {
			return err
		}
	case "bot:producerResumed":
		if err := timeline.setProducerStateLocked(event.ProducerID, "active", atMS); err != nil {
			return err
		}
	case "bot:producerClosed":
		trackID := timeline.producers[event.ProducerID]
		if trackID != "" {
			timeline.closeLastLocked("trackIntervals", trackID, atMS)
			delete(timeline.producers, event.ProducerID)
		}
	default:
		return errors.New("unsupported timeline event")
	}
	return validateTimelineRecords(timeline.records, true)
}

func (timeline *TimelineState) transitionUpdatedProducerLocked(producer signaling.Producer, atMS int64) error {
	oldTrackID := timeline.producers[producer.ProducerID]
	participantID := ParticipantInstanceID(timeline.recordingID, producer.ParticipantID)
	newTrackID, err := TrackID(timeline.recordingID, participantID, producer.Source)
	if err != nil {
		return err
	}
	if oldTrackID != "" && oldTrackID != newTrackID {
		timeline.closeLastLocked("trackIntervals", oldTrackID, atMS)
		delete(timeline.producers, producer.ProducerID)
	}
	if oldTrackID == newTrackID && oldTrackID != "" {
		state := "active"
		if producer.Paused {
			state = "paused"
		}
		if err := timeline.setProducerStateLocked(producer.ProducerID, state, atMS); err != nil {
			return err
		}
	}
	return timeline.ensureProducerLocked(producer, atMS)
}

func (timeline *TimelineState) ProducerUpdateChangesTrack(event signaling.Event) bool {
	if event.Type != "bot:producerUpdated" || event.Producer == nil {
		return false
	}
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	oldTrackID := timeline.producers[event.Producer.ProducerID]
	participantID := ParticipantInstanceID(timeline.recordingID, event.Producer.ParticipantID)
	newTrackID, err := TrackID(timeline.recordingID, participantID, event.Producer.Source)
	return err != nil || oldTrackID != newTrackID
}

func validateTimelineSnapshot(snapshot signaling.Snapshot) error {
	participants := make(map[string]struct{}, len(snapshot.Participants))
	for _, participant := range snapshot.Participants {
		if err := validateTimelineParticipant(participant); err != nil {
			return err
		}
		if _, duplicate := participants[participant.ParticipantID]; duplicate {
			return errors.New("duplicate snapshot participant")
		}
		participants[participant.ParticipantID] = struct{}{}
	}
	producers := make(map[string]struct{}, len(snapshot.Producers))
	logicalTracks := map[logicalTrackKey]struct{}{}
	for _, producer := range snapshot.Producers {
		if err := validateTimelineProducer(producer); err != nil {
			return err
		}
		if _, exists := participants[producer.ParticipantID]; !exists {
			return errors.New("snapshot producer participant is absent")
		}
		key := logicalTrackKey{participantID: producer.ParticipantID, source: producer.Source}
		_, duplicateProducer := producers[producer.ProducerID]
		_, duplicateTrack := logicalTracks[key]
		if duplicateProducer || duplicateTrack {
			return errors.New("duplicate snapshot producer")
		}
		producers[producer.ProducerID] = struct{}{}
		logicalTracks[key] = struct{}{}
	}
	return nil
}

func validateTimelineParticipant(participant signaling.Participant) error {
	labelLength := len([]rune(participant.DisplayName))
	if participant.ParticipantID == "" || labelLength < 1 || labelLength > 160 {
		return errors.New("invalid timeline participant")
	}
	return nil
}

func validateTimelineProducer(producer signaling.Producer) error {
	if producer.ProducerID == "" || producer.ParticipantID == "" {
		return errors.New("producerId and participantId are required")
	}
	if !producer.Source.Valid() {
		return fmt.Errorf("unsupported source %q", producer.Source)
	}
	if producer.Source == signaling.SourceMic && producer.Kind != signaling.KindAudio {
		return errors.New("mic producer must be audio")
	}
	if producer.Source != signaling.SourceMic && producer.Kind != signaling.KindVideo {
		return errors.New("camera and screen producers must be video")
	}
	if err := validateTimelineParticipant(signaling.Participant{
		ParticipantID: producer.ParticipantID,
		DisplayName:   producer.DisplayName,
	}); err != nil {
		return err
	}
	return nil
}

func (timeline *TimelineState) TrackForProducer(producerID string) (string, string, bool) {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	trackID := timeline.producers[producerID]
	if trackID == "" {
		return "", "", false
	}
	for index := len(timeline.records) - 1; index >= 0; index-- {
		record := timeline.records[index]
		if record.Category == "trackIntervals" && record.TrackID == trackID && record.EndMS == nil {
			if record.State != "active" {
				return "", "", false
			}
			break
		}
	}
	return trackID, timeline.trackOwnerLocked(trackID), true
}

func (timeline *TimelineState) RecordsThrough(throughMS int64, closeOpen bool) ([]CheckpointRecord, error) {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	records := append([]CheckpointRecord{}, timeline.records...)
	if closeOpen {
		for index := range records {
			isInterval := records[index].Category == "participantIntervals" ||
				records[index].Category == "trackIntervals"
			if isInterval && records[index].EndMS == nil {
				end := throughMS
				records[index].EndMS = &end
			}
		}
		filtered := records[:0]
		for _, record := range records {
			if record.StartMS != nil && record.EndMS != nil && *record.StartMS == *record.EndMS {
				continue
			}
			filtered = append(filtered, record)
		}
		records = filtered
	}
	if err := validateTimelineRecords(records, !closeOpen); err != nil {
		return nil, err
	}
	sortCheckpointRecords(records)
	return records, nil
}

func (timeline *TimelineState) DeclareGap(trackID string, startMS, endMS int64, reason string) error {
	if reason != "capture_restart" && reason != "source_unavailable" {
		return errors.New("invalid worker-authored gap reason")
	}
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	if !validOpaqueID(trackID, "trk_") || startMS < 0 || endMS <= startMS {
		return errors.New("invalid track gap")
	}
	start, end := startMS, endMS
	timeline.records = append(timeline.records, CheckpointRecord{
		Category: "gaps", TrackID: trackID, StartMS: &start, EndMS: &end,
		Scope: "track", Reason: reason,
	})
	return nil
}

func (timeline *TimelineState) DeclareRecoveryGaps(startMS, endMS int64) error {
	if startMS < 0 || endMS < startMS {
		return errors.New("invalid recovery gap bounds")
	}
	if endMS == startMS {
		return nil
	}
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	for _, record := range timeline.records {
		if record.Category != "trackIntervals" || record.State != "active" || record.EndMS != nil || record.StartMS == nil {
			continue
		}
		gapStart := startMS
		if *record.StartMS > gapStart {
			gapStart = *record.StartMS
		}
		if gapStart >= endMS {
			continue
		}
		start, end := gapStart, endMS
		timeline.records = append(timeline.records, CheckpointRecord{
			Category: "gaps", TrackID: record.TrackID, StartMS: &start, EndMS: &end,
			Scope: "track", Reason: "capture_restart",
		})
	}
	return nil
}

func (timeline *TimelineState) HasGaps() bool {
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	for _, record := range timeline.records {
		if record.Category == "gaps" {
			return true
		}
	}
	return false
}

func (timeline *TimelineState) ensureParticipantLocked(participant signaling.Participant, atMS int64) string {
	opaque := timeline.participants[participant.ParticipantID]
	if opaque == "" {
		opaque = ParticipantInstanceID(timeline.recordingID, participant.ParticipantID)
		timeline.participants[participant.ParticipantID] = opaque
		exists := false
		for index := range timeline.records {
			record := timeline.records[index]
			if record.Category == "participantInstances" && record.ParticipantInstanceID == opaque {
				timeline.records[index].DisplayName = participant.DisplayName
				exists = true
			}
		}
		if !exists {
			timeline.records = append(timeline.records, CheckpointRecord{
				Category: "participantInstances", ParticipantInstanceID: opaque,
				DisplayName: participant.DisplayName,
			})
		}
		if !timeline.hasOpenLocked("participantIntervals", opaque) {
			start := atMS
			timeline.records = append(timeline.records, CheckpointRecord{
				Category: "participantIntervals", ParticipantInstanceID: opaque, StartMS: &start,
			})
		}
		return opaque
	}
	for index := range timeline.records {
		record := timeline.records[index]
		if record.Category == "participantInstances" && record.ParticipantInstanceID == opaque {
			timeline.records[index].DisplayName = participant.DisplayName
		}
	}
	if !timeline.hasOpenLocked("participantIntervals", opaque) {
		start := atMS
		timeline.records = append(timeline.records, CheckpointRecord{
			Category: "participantIntervals", ParticipantInstanceID: opaque, StartMS: &start,
		})
	}
	return opaque
}

func (timeline *TimelineState) ensureProducerLocked(producer signaling.Producer, atMS int64) error {
	participantID := timeline.ensureParticipantLocked(signaling.Participant{
		ParticipantID: producer.ParticipantID,
		DisplayName:   producer.DisplayName,
	}, atMS)
	trackID, err := TrackID(timeline.recordingID, participantID, producer.Source)
	if err != nil {
		return err
	}
	timeline.producers[producer.ProducerID] = trackID
	if timeline.trackOwnerLocked(trackID) == "" {
		timeline.records = append(timeline.records, CheckpointRecord{
			Category: "tracks", TrackID: trackID,
			ParticipantInstanceID: participantID, Source: producer.Source,
		})
	}
	state := "active"
	if producer.Paused {
		state = "paused"
	}
	if !timeline.hasOpenLocked("trackIntervals", trackID) {
		start := atMS
		timeline.records = append(timeline.records, CheckpointRecord{
			Category: "trackIntervals", TrackID: trackID, StartMS: &start, State: state,
		})
	}
	return nil
}

func (timeline *TimelineState) setProducerStateLocked(producerID, state string, atMS int64) error {
	trackID := timeline.producers[producerID]
	if trackID == "" {
		return errors.New("producer timeline identity missing")
	}
	for index := len(timeline.records) - 1; index >= 0; index-- {
		record := &timeline.records[index]
		if record.Category == "trackIntervals" && record.TrackID == trackID && record.EndMS == nil {
			if record.State == state {
				return nil
			}
			if record.StartMS != nil && *record.StartMS == atMS {
				record.State = state
				return nil
			}
			end := atMS
			record.EndMS = &end
			start := atMS
			timeline.records = append(timeline.records, CheckpointRecord{
				Category: "trackIntervals", TrackID: trackID, StartMS: &start, State: state,
			})
			return nil
		}
	}
	return errors.New("producer has no open track interval")
}

func (timeline *TimelineState) hasOpenLocked(category, owner string) bool {
	for index := len(timeline.records) - 1; index >= 0; index-- {
		record := timeline.records[index]
		matchesOwner := record.ParticipantInstanceID == owner || record.TrackID == owner
		if record.Category == category && record.EndMS == nil && matchesOwner {
			return true
		}
	}
	return false
}

func (timeline *TimelineState) closeLastLocked(category, owner string, atMS int64) {
	for index := len(timeline.records) - 1; index >= 0; index-- {
		record := &timeline.records[index]
		matchesOwner := record.ParticipantInstanceID == owner || record.TrackID == owner
		if record.Category == category && record.EndMS == nil && matchesOwner {
			if record.StartMS != nil && *record.StartMS == atMS {
				timeline.records = append(timeline.records[:index], timeline.records[index+1:]...)
				return
			}
			end := atMS
			record.EndMS = &end
			return
		}
	}
}

func (timeline *TimelineState) trackOwnerLocked(trackID string) string {
	for _, record := range timeline.records {
		if record.Category == "tracks" && record.TrackID == trackID {
			return record.ParticipantInstanceID
		}
	}
	return ""
}

func validateTimelineRecords(records []CheckpointRecord, allowOpen bool) error {
	participants, tracks := map[string]struct{}{}, map[string]string{}
	for _, record := range records {
		switch record.Category {
		case "participantInstances":
			_, duplicate := participants[record.ParticipantInstanceID]
			labelLength := len([]rune(record.DisplayName))
			if !validOpaqueID(record.ParticipantInstanceID, "pi_") || labelLength < 1 ||
				labelLength > 160 || duplicate {
				return errors.New("invalid participant timeline record")
			}
			participants[record.ParticipantInstanceID] = struct{}{}
		case "tracks":
			_, duplicate := tracks[record.TrackID]
			if !validOpaqueID(record.TrackID, "trk_") || !record.Source.Valid() ||
				!validOpaqueID(record.ParticipantInstanceID, "pi_") || duplicate {
				return errors.New("invalid track timeline record")
			}
			tracks[record.TrackID] = record.ParticipantInstanceID
		case "participantIntervals", "trackIntervals", "gaps":
		default:
			return errors.New("invalid timeline record category")
		}
	}
	for _, record := range records {
		switch record.Category {
		case "participantIntervals":
			if _, ok := participants[record.ParticipantInstanceID]; !ok || invalidInterval(record, allowOpen) {
				return errors.New("invalid participant interval")
			}
		case "trackIntervals":
			_, trackExists := tracks[record.TrackID]
			validState := record.State == "active" || record.State == "paused"
			if !trackExists || invalidInterval(record, allowOpen) || !validState {
				return errors.New("invalid track interval")
			}
		case "gaps":
			validReason := record.Reason == "capture_restart" || record.Reason == "source_unavailable"
			if record.Scope != "track" || tracks[record.TrackID] == "" ||
				invalidInterval(record, false) || !validReason {
				return errors.New("invalid capture gap")
			}
		}
	}
	for trackID, owner := range tracks {
		if _, exists := participants[owner]; !exists || trackID == "" {
			return errors.New("track owner is absent")
		}
	}
	participantIntervals := groupIntervals(records, "participantIntervals")
	trackIntervals := groupIntervals(records, "trackIntervals")
	for owner, intervals := range participantIntervals {
		if !orderedIntervals(intervals, allowOpen) || owner == "" {
			return errors.New("participant intervals overlap")
		}
	}
	for trackID, intervals := range trackIntervals {
		if !orderedIntervals(intervals, allowOpen) {
			return errors.New("track intervals overlap")
		}
		ownerIntervals := participantIntervals[tracks[trackID]]
		for _, interval := range intervals {
			if !containedInterval(interval, ownerIntervals) {
				return errors.New("track interval is outside participant presence")
			}
		}
	}
	gaps := groupIntervals(records, "gaps")
	for trackID, intervals := range gaps {
		if !orderedIntervals(intervals, false) {
			return errors.New("capture gaps overlap")
		}
		for _, gap := range intervals {
			covered := false
			for _, active := range trackIntervals[trackID] {
				if active.State == "active" && containedInterval(gap, []CheckpointRecord{active}) {
					covered = true
					break
				}
			}
			if !covered {
				return errors.New("capture gap is outside an active track interval")
			}
		}
	}
	return nil
}

func invalidInterval(record CheckpointRecord, allowOpen bool) bool {
	missingStart := record.StartMS == nil
	if missingStart {
		return true
	}
	invalidOpen := record.EndMS == nil && !allowOpen
	invalidClosed := record.EndMS != nil && *record.EndMS <= *record.StartMS
	return *record.StartMS < 0 || invalidOpen || invalidClosed
}

func groupIntervals(records []CheckpointRecord, category string) map[string][]CheckpointRecord {
	result := map[string][]CheckpointRecord{}
	for _, record := range records {
		if record.Category != category {
			continue
		}
		owner := record.TrackID
		if category == "participantIntervals" {
			owner = record.ParticipantInstanceID
		}
		result[owner] = append(result[owner], record)
	}
	return result
}

func orderedIntervals(intervals []CheckpointRecord, allowOpen bool) bool {
	sort.Slice(intervals, func(i, j int) bool { return *intervals[i].StartMS < *intervals[j].StartMS })
	for index, interval := range intervals {
		if invalidInterval(interval, allowOpen) || (interval.EndMS == nil && index != len(intervals)-1) {
			return false
		}
		if index > 0 {
			previous := intervals[index-1]
			if previous.EndMS == nil || *interval.StartMS < *previous.EndMS {
				return false
			}
		}
	}
	return true
}

func containedInterval(interval CheckpointRecord, owners []CheckpointRecord) bool {
	for _, owner := range owners {
		if *owner.StartMS > *interval.StartMS {
			continue
		}
		if owner.EndMS == nil {
			return true
		}
		if interval.EndMS != nil && *interval.EndMS <= *owner.EndMS {
			return true
		}
	}
	return false
}

func nextCheckpointSequence(checkpoints []GenerationCheckpoint, generation int32) int32 {
	result := int32(0)
	for _, checkpoint := range checkpoints {
		if checkpoint.CaptureGeneration == generation && checkpoint.CheckpointSequence >= result {
			result = checkpoint.CheckpointSequence + 1
		}
	}
	return result
}

func latestGenerationCheckpoint(
	checkpoints []GenerationCheckpoint,
	generation int32,
) (GenerationCheckpoint, bool) {
	var latest GenerationCheckpoint
	found := false
	for _, checkpoint := range checkpoints {
		if checkpoint.CaptureGeneration != generation {
			continue
		}
		if !found || checkpoint.CheckpointSequence > latest.CheckpointSequence {
			latest = checkpoint
			found = true
		}
	}
	return latest, found
}

func latestCheckpoint(checkpoints []GenerationCheckpoint) (GenerationCheckpoint, bool) {
	if len(checkpoints) == 0 {
		return GenerationCheckpoint{}, false
	}
	copy := append([]GenerationCheckpoint{}, checkpoints...)
	sort.Slice(copy, func(i, j int) bool {
		if copy[i].CaptureGeneration != copy[j].CaptureGeneration {
			return copy[i].CaptureGeneration < copy[j].CaptureGeneration
		}
		return copy[i].CheckpointSequence < copy[j].CheckpointSequence
	})
	return copy[len(copy)-1], true
}
