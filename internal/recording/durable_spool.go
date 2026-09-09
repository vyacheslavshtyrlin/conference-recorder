package recording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type SpoolManifest struct {
	Version                  int                `json:"version"`
	AttemptID                string             `json:"attemptId"`
	RecordingID              string             `json:"recordingId"`
	WorkerID                 string             `json:"workerId"`
	WorkerGenerationID       string             `json:"workerGenerationId"`
	CaptureGeneration        int32              `json:"captureGeneration"`
	CaptureStartedAt         string             `json:"captureStartedAt"`
	BindingMode              string             `json:"bindingMode"`
	Status                   string             `json:"status"`
	SourceBytes              int64              `json:"sourceBytes"`
	LastCheckpointSequence   int32              `json:"lastCheckpointSequence"`
	LastCheckpointHash       string             `json:"lastCheckpointHash,omitempty"`
	LastCheckpointTimelineMS int64              `json:"lastCheckpointTimelineMs"`
	Records                  []CheckpointRecord `json:"records"`
	Segments                 []DurableSegment   `json:"segments"`
}

type SpoolMutationFence struct {
	AttemptID          string
	RecordingID        string
	WorkerID           string
	WorkerGenerationID string
	CaptureGeneration  int32
}

type DurableSpool struct {
	root            string
	maxBytes        int64
	mu              sync.Mutex
	manifests       map[string]*SpoolManifest
	failpoint       func(string) error
	staging         map[string]map[string]int64
	recoveryScanned bool
}

const sealedSpoolRetention = 24 * time.Hour

func NewDurableSpool(root string, maxBytes int64) (*DurableSpool, error) {
	if root == "" || !filepath.IsAbs(root) || maxBytes < 1_000_000 {
		return nil, errors.New("invalid durable spool configuration")
	}
	root = filepath.Clean(root)
	if err := ensureRealDirectory(root, 0o700); err != nil {
		return nil, errors.New("prepare real durable spool root")
	}
	if err := ensureRealDirectory(filepath.Join(root, "capture-v2"), 0o700); err != nil {
		return nil, errors.New("prepare real capture spool root")
	}
	return &DurableSpool{
		root:      root,
		maxBytes:  maxBytes,
		manifests: map[string]*SpoolManifest{},
		staging:   map[string]map[string]int64{},
	}, nil
}

func (spool *DurableSpool) Begin(
	ctx context.Context,
	fence AttemptFence,
	captureGeneration int32,
	captureStartedAt string,
	bindingMode string,
) (*SpoolManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := fence.validate(); err != nil || captureGeneration <= 0 || captureStartedAt == "" ||
		(bindingMode != "capture" && bindingMode != "seal-only") {
		return nil, errors.New("invalid durable spool binding")
	}
	spool.mu.Lock()
	defer spool.mu.Unlock()
	if err := spool.prepareRecordingLocked(fence.RecordingID); err != nil {
		return nil, err
	}
	manifest, err := spool.loadLocked(fence.RecordingID)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if manifest == nil {
		manifest = &SpoolManifest{
			Version: 1, AttemptID: fence.AttemptID, RecordingID: fence.RecordingID, WorkerID: fence.WorkerID,
			WorkerGenerationID: fence.WorkerGenerationID, CaptureGeneration: captureGeneration,
			CaptureStartedAt: captureStartedAt, BindingMode: bindingMode, Status: "capturing",
			LastCheckpointSequence: -1, Records: []CheckpointRecord{}, Segments: []DurableSegment{},
		}
	} else {
		if manifest.RecordingID != fence.RecordingID || manifest.CaptureStartedAt != captureStartedAt {
			return nil, errors.New("durable spool identity conflict")
		}
		if manifest.CaptureGeneration > captureGeneration {
			return nil, ErrAttemptFenceMismatch
		}
		if manifest.CaptureGeneration == captureGeneration {
			exactOwner := manifest.AttemptID == fence.AttemptID &&
				manifest.WorkerID == fence.WorkerID &&
				manifest.WorkerGenerationID == fence.WorkerGenerationID &&
				manifest.BindingMode == bindingMode
			if !exactOwner {
				return nil, ErrAttemptFenceMismatch
			}
		}
		if manifest.CaptureGeneration < captureGeneration {
			manifest.Segments = []DurableSegment{}
			manifest.SourceBytes = 0
			manifest.LastCheckpointSequence = -1
			manifest.LastCheckpointHash = ""
			delete(spool.staging, fence.RecordingID)
		}
		manifest.AttemptID = fence.AttemptID
		manifest.WorkerID = fence.WorkerID
		manifest.WorkerGenerationID = fence.WorkerGenerationID
		manifest.CaptureGeneration = captureGeneration
		manifest.BindingMode = bindingMode
		manifest.Status = "capturing"
	}
	if err := spool.injectFailure("before-manifest-persist"); err != nil {
		return nil, err
	}
	if err := spool.persistLocked(manifest); err != nil {
		return nil, err
	}
	spool.manifests[fence.RecordingID] = manifest
	if err := spool.injectFailure("after-manifest-persist"); err != nil {
		return nil, err
	}
	if err := spool.removeUntrackedLocked(manifest); err != nil {
		return nil, err
	}
	return cloneManifest(manifest), nil
}

func (spool *DurableSpool) StoreSegment(
	ctx context.Context,
	fence SpoolMutationFence,
	descriptor SegmentDescriptor,
	stagingPath string,
) (DurableSegment, error) {
	if err := validateSegment(descriptor); err != nil {
		return DurableSegment{}, err
	}
	if err := ctx.Err(); err != nil {
		return DurableSegment{}, err
	}
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return DurableSegment{}, err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return DurableSegment{}, err
	}
	if err := spool.verifyRecordingDirectoriesLocked(recordingID, true); err != nil {
		return DurableSegment{}, err
	}
	stagingPath = filepath.Clean(stagingPath)
	relativeStaging, err := filepath.Rel(spool.recordingDir(recordingID), stagingPath)
	outsideStaging := err != nil || relativeStaging == ".." ||
		strings.HasPrefix(relativeStaging, ".."+string(filepath.Separator)) ||
		filepath.Dir(relativeStaging) != "staging"
	if outsideStaging {
		return DurableSegment{}, errors.New("staging segment escaped recording directory")
	}
	for _, existing := range manifest.Segments {
		if existing.CaptureGeneration == manifest.CaptureGeneration &&
			existing.TrackID == descriptor.TrackID && existing.Sequence == descriptor.Sequence {
			if existing.SegmentDescriptor == descriptor {
				delete(spool.staging[recordingID], stagingPath)
				if err := unlinkAndSync(stagingPath); err != nil {
					return DurableSegment{}, errors.New("remove duplicate promoted staging segment")
				}
				return existing, nil
			}
			return DurableSegment{}, errors.New("durable segment conflict")
		}
	}
	reserved, hasReservation := spool.staging[recordingID][stagingPath]
	if hasReservation && reserved != descriptor.ExpectedBytes {
		return DurableSegment{}, errors.New("durable segment staging reservation mismatch")
	}
	projected := manifest.SourceBytes + spool.stagingBytesLocked(recordingID) -
		reserved + descriptor.ExpectedBytes
	if projected > spool.maxBytes {
		return DurableSegment{}, errors.New("durable spool byte limit exceeded")
	}
	extension := descriptor.Container
	relative := filepath.Join(
		"segments",
		fmt.Sprintf("g%010d-%s-s%010d.%s", manifest.CaptureGeneration, descriptor.TrackID, descriptor.Sequence, extension),
	)
	destination := filepath.Join(spool.recordingDir(recordingID), relative)
	created, err := linkImmutable(stagingPath, destination, descriptor.ExpectedBytes, descriptor.SHA256)
	if err != nil {
		return DurableSegment{}, err
	}
	segment := DurableSegment{
		CaptureGeneration: manifest.CaptureGeneration,
		LocalPath:         relative,
		SegmentDescriptor: descriptor,
	}
	candidate := cloneManifest(manifest)
	candidate.Segments = append(candidate.Segments, segment)
	candidate.SourceBytes += descriptor.ExpectedBytes
	if err := spool.persistLocked(candidate); err != nil {
		durable := spool.reloadAfterPersistFailureLocked(recordingID)
		if manifestContainsSegment(durable, segment) {
			delete(spool.staging[recordingID], stagingPath)
			_ = unlinkAndSync(stagingPath)
			return segment, err
		} else if created {
			_ = os.Remove(destination)
			_ = syncDirectory(filepath.Dir(destination))
		}
		return DurableSegment{}, err
	}
	spool.manifests[recordingID] = candidate
	delete(spool.staging[recordingID], stagingPath)
	if err := unlinkAndSync(stagingPath); err != nil {
		return segment, errors.New("remove promoted staging segment")
	}
	return segment, nil
}

func (spool *DurableSpool) reloadAfterPersistFailureLocked(recordingID string) *SpoolManifest {
	manifest, err := spool.loadLocked(recordingID)
	if err != nil {
		delete(spool.manifests, recordingID)
		return nil
	}
	spool.manifests[recordingID] = manifest
	return manifest
}

func manifestContainsSegment(manifest *SpoolManifest, wanted DurableSegment) bool {
	if manifest == nil {
		return false
	}
	for _, segment := range manifest.Segments {
		if segment.CaptureGeneration == wanted.CaptureGeneration &&
			segment.TrackID == wanted.TrackID &&
			segment.Sequence == wanted.Sequence &&
			segment.SegmentDescriptor == wanted.SegmentDescriptor {
			return true
		}
	}
	return false
}

func (spool *DurableSpool) ResizeStaging(fence SpoolMutationFence, path string, size int64) error {
	if size < 0 || path == "" {
		return errors.New("invalid staging reservation")
	}
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if err := spool.verifyRecordingDirectoriesLocked(recordingID, true); err != nil {
		return err
	}
	path = filepath.Clean(path)
	recordingDirectory := spool.recordingDir(recordingID)
	relative, err := filepath.Rel(recordingDirectory, path)
	outside := err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator))
	if outside || filepath.Dir(relative) != "staging" {
		return errors.New("staging reservation escaped recording directory")
	}
	if spool.staging[recordingID] == nil {
		spool.staging[recordingID] = map[string]int64{}
	}
	current := spool.staging[recordingID][path]
	projected := manifest.SourceBytes + spool.stagingBytesLocked(recordingID) - current + size
	if projected > spool.maxBytes {
		return errors.New("capture storage byte limit exceeded")
	}
	if size == 0 {
		delete(spool.staging[recordingID], path)
		return nil
	}
	spool.staging[recordingID][path] = size
	return nil
}

func (spool *DurableSpool) ReleaseStaging(fence SpoolMutationFence, path string) {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	manifest, err := spool.requireLocked(fence.RecordingID)
	if err != nil || requireSpoolFence(manifest, fence) != nil {
		return
	}
	delete(spool.staging[fence.RecordingID], filepath.Clean(path))
}

func (spool *DurableSpool) stagingBytesLocked(recordingID string) int64 {
	var total int64
	for _, size := range spool.staging[recordingID] {
		total += size
	}
	return total
}

func (spool *DurableSpool) CleanupStaging(fence SpoolMutationFence) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if len(spool.staging[recordingID]) != 0 {
		return errors.New("capture staging cleanup has active reservations")
	}
	staging := filepath.Join(spool.recordingDir(recordingID), "staging")
	if _, err := os.Lstat(staging); err == nil {
		if err := verifyRealDirectory(staging); err != nil {
			return errors.New("capture staging directory is not a real directory")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("inspect capture staging directory")
	}
	if err := os.RemoveAll(staging); err != nil {
		return errors.New("remove stale capture staging")
	}
	delete(spool.staging, recordingID)
	return syncDirectory(spool.recordingDir(recordingID))
}

func (spool *DurableSpool) MarkCommitted(
	fence SpoolMutationFence,
	trackID string,
	sequence int32,
	uploadID string,
) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	candidate := cloneManifest(manifest)
	for index := range candidate.Segments {
		segment := &candidate.Segments[index]
		if segment.CaptureGeneration == fence.CaptureGeneration && segment.TrackID == trackID && segment.Sequence == sequence {
			if segment.Committed && segment.UploadID != uploadID {
				return errors.New("durable upload identity conflict")
			}
			segment.UploadID = uploadID
			segment.Committed = true
			if err := spool.persistLocked(candidate); err != nil {
				spool.reloadAfterPersistFailureLocked(recordingID)
				return err
			}
			spool.manifests[recordingID] = candidate
			return nil
		}
	}
	return errors.New("durable segment not found")
}

func (spool *DurableSpool) StoreCheckpoint(
	fence SpoolMutationFence,
	checkpoint GenerationCheckpoint,
	records []CheckpointRecord,
) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if checkpoint.CaptureGeneration != manifest.CaptureGeneration ||
		checkpoint.CheckpointSequence != manifest.LastCheckpointSequence+1 ||
		checkpoint.TimelineThroughMS < manifest.LastCheckpointTimelineMS {
		return errors.New("checkpoint sequence or timeline conflict")
	}
	hash, err := canonicalCheckpointHash(checkpoint.CaptureGeneration, checkpoint.TimelineThroughMS, records)
	if err != nil || hash != checkpoint.PayloadSHA256 {
		return errors.New("checkpoint checksum conflict")
	}
	candidate := cloneManifest(manifest)
	candidate.LastCheckpointSequence = checkpoint.CheckpointSequence
	candidate.LastCheckpointTimelineMS = checkpoint.TimelineThroughMS
	candidate.LastCheckpointHash = checkpoint.PayloadSHA256
	candidate.Records = append([]CheckpointRecord{}, records...)
	if err := spool.persistLocked(candidate); err != nil {
		spool.reloadAfterPersistFailureLocked(recordingID)
		return err
	}
	spool.manifests[recordingID] = candidate
	return nil
}

// ReconcileCheckpoint advances the local manifest to an API-authoritative
// checkpoint after recovery. The API may have committed a checkpoint whose
// subsequent local manifest persist was interrupted by a process crash.
func (spool *DurableSpool) ReconcileCheckpoint(
	fence SpoolMutationFence,
	checkpoint GenerationCheckpoint,
	records []CheckpointRecord,
) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if checkpoint.CaptureGeneration != manifest.CaptureGeneration ||
		checkpoint.CheckpointSequence < manifest.LastCheckpointSequence ||
		checkpoint.TimelineThroughMS < manifest.LastCheckpointTimelineMS {
		return errors.New("recovered checkpoint regressed local manifest")
	}
	hash, err := canonicalCheckpointHash(
		checkpoint.CaptureGeneration,
		checkpoint.TimelineThroughMS,
		records,
	)
	if err != nil || hash != checkpoint.PayloadSHA256 {
		return errors.New("recovered checkpoint checksum conflict")
	}
	if checkpoint.CheckpointSequence == manifest.LastCheckpointSequence {
		sameCheckpoint := checkpoint.TimelineThroughMS == manifest.LastCheckpointTimelineMS &&
			checkpoint.PayloadSHA256 == manifest.LastCheckpointHash
		if !sameCheckpoint {
			return errors.New("recovered checkpoint conflicts with local manifest")
		}
		return nil
	}
	candidate := cloneManifest(manifest)
	candidate.LastCheckpointSequence = checkpoint.CheckpointSequence
	candidate.LastCheckpointTimelineMS = checkpoint.TimelineThroughMS
	candidate.LastCheckpointHash = checkpoint.PayloadSHA256
	candidate.Records = append([]CheckpointRecord{}, records...)
	if err := spool.persistLocked(candidate); err != nil {
		spool.reloadAfterPersistFailureLocked(recordingID)
		return err
	}
	spool.manifests[recordingID] = candidate
	return nil
}

func (spool *DurableSpool) Snapshot(recordingID string) (*SpoolManifest, error) {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return nil, err
	}
	return cloneManifest(manifest), nil
}

func (spool *DurableSpool) RecoveryHealth() ([]string, []string, error) {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	if err := spool.verifyBaseDirectoriesLocked(); err != nil {
		return nil, nil, err
	}
	if !spool.recoveryScanned {
		entries, err := os.ReadDir(filepath.Join(spool.root, "capture-v2"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() || filepath.Base(entry.Name()) != entry.Name() {
				continue
			}
			manifest, loadErr := spool.loadLocked(entry.Name())
			if loadErr != nil {
				return nil, nil, fmt.Errorf(
					"load durable recovery manifest %q: %w",
					entry.Name(),
					loadErr,
				)
			}
			spool.manifests[manifest.RecordingID] = manifest
		}
		spool.recoveryScanned = true
	}
	recoverable := []string{}
	finalizations := []string{}
	for recordingID, manifest := range spool.manifests {
		if manifest.Status == "sealed" || manifest.Status == "failed" {
			info, statErr := os.Stat(spool.manifestPath(recordingID))
			if statErr == nil && time.Since(info.ModTime()) >= sealedSpoolRetention {
				if os.RemoveAll(spool.recordingDir(recordingID)) == nil {
					delete(spool.manifests, recordingID)
					delete(spool.staging, recordingID)
				}
			}
			continue
		}
		if manifest.BindingMode == "seal-only" {
			finalizations = append(finalizations, manifest.RecordingID)
			continue
		}
		if len(manifest.Segments) > 0 || manifest.LastCheckpointSequence >= 0 {
			recoverable = append(recoverable, manifest.RecordingID)
		}
	}
	sort.Strings(recoverable)
	sort.Strings(finalizations)
	return recoverable, finalizations, nil
}

func (spool *DurableSpool) MarkSealed(fence SpoolMutationFence) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if manifest.Status == "sealed" {
		return nil
	}
	candidate := cloneManifest(manifest)
	candidate.Status = "sealed"
	if err := spool.persistLocked(candidate); err != nil {
		spool.reloadAfterPersistFailureLocked(recordingID)
		return err
	}
	spool.manifests[recordingID] = candidate
	return nil
}

func (spool *DurableSpool) MarkFailed(fence SpoolMutationFence) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if manifest.Status == "failed" {
		return nil
	}
	candidate := cloneManifest(manifest)
	candidate.Status = "failed"
	if err := spool.persistLocked(candidate); err != nil {
		spool.reloadAfterPersistFailureLocked(recordingID)
		return err
	}
	spool.manifests[recordingID] = candidate
	return nil
}

func (spool *DurableSpool) RemoveCommitted(fence SpoolMutationFence) error {
	spool.mu.Lock()
	defer spool.mu.Unlock()
	recordingID := fence.RecordingID
	manifest, err := spool.requireLocked(recordingID)
	if err != nil {
		return err
	}
	if err := requireSpoolFence(manifest, fence); err != nil {
		return err
	}
	if err := spool.verifyRecordingDirectoriesLocked(recordingID, false); err != nil {
		return err
	}
	candidate := cloneManifest(manifest)
	kept := make([]DurableSegment, 0, len(candidate.Segments))
	remove := []string{}
	for _, segment := range candidate.Segments {
		if !segment.Committed {
			kept = append(kept, segment)
			continue
		}
		remove = append(remove, filepath.Join(spool.recordingDir(recordingID), segment.LocalPath))
		candidate.SourceBytes -= segment.ExpectedBytes
	}
	candidate.Segments = kept
	if err := spool.persistLocked(candidate); err != nil {
		spool.reloadAfterPersistFailureLocked(recordingID)
		return err
	}
	spool.manifests[recordingID] = candidate
	for _, path := range remove {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := spool.injectFailure("before-committed-delete-sync"); err != nil {
		return err
	}
	return syncDirectory(filepath.Join(spool.recordingDir(recordingID), "segments"))
}

func (spool *DurableSpool) removeUntrackedLocked(manifest *SpoolManifest) error {
	directory := filepath.Join(spool.recordingDir(manifest.RecordingID), "segments")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	tracked := map[string]struct{}{}
	for _, segment := range manifest.Segments {
		tracked[filepath.Base(segment.LocalPath)] = struct{}{}
	}
	removed := false
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		if _, exists := tracked[entry.Name()]; exists {
			continue
		}
		if err := os.Remove(filepath.Join(directory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = true
		if err := spool.injectFailure("after-orphan-delete"); err != nil {
			return err
		}
	}
	if removed {
		return syncDirectory(directory)
	}
	return nil
}

func (spool *DurableSpool) injectFailure(phase string) error {
	if spool.failpoint == nil {
		return nil
	}
	return spool.failpoint(phase)
}

func validRecordingID(recordingID string) bool {
	return recordingID != "" && recordingID != "." && recordingID != ".." && len(recordingID) <= 128 &&
		filepath.Base(recordingID) == recordingID && filepath.Clean(recordingID) == recordingID
}

func (spool *DurableSpool) prepareRecordingLocked(recordingID string) error {
	if !validRecordingID(recordingID) {
		return errors.New("invalid recording ID for spool")
	}
	if err := spool.verifyBaseDirectoriesLocked(); err != nil {
		return err
	}
	if err := ensureRealDirectory(spool.recordingDir(recordingID), 0o700); err != nil {
		return errors.New("prepare real recording spool directory")
	}
	if err := ensureRealDirectory(
		filepath.Join(spool.recordingDir(recordingID), "segments"),
		0o700,
	); err != nil {
		return errors.New("prepare real recording segments directory")
	}
	return nil
}

func (spool *DurableSpool) verifyBaseDirectoriesLocked() error {
	if err := verifyRealDirectory(spool.root); err != nil {
		return errors.New("durable spool root is not a real directory")
	}
	if err := verifyRealDirectory(filepath.Join(spool.root, "capture-v2")); err != nil {
		return errors.New("capture spool root is not a real directory")
	}
	return nil
}

func (spool *DurableSpool) verifyRecordingDirectoriesLocked(recordingID string, requireStaging bool) error {
	if err := spool.verifyBaseDirectoriesLocked(); err != nil {
		return err
	}
	directories := []string{
		spool.recordingDir(recordingID),
		filepath.Join(spool.recordingDir(recordingID), "segments"),
	}
	if requireStaging {
		directories = append(directories, filepath.Join(spool.recordingDir(recordingID), "staging"))
	}
	for _, directory := range directories {
		if err := verifyRealDirectory(directory); err != nil {
			return errors.New("recording spool path is not a real directory")
		}
	}
	return nil
}

func ensureRealDirectory(path string, mode os.FileMode) error {
	path = filepath.Clean(path)
	missing := []string{}
	cursor := path
	for {
		info, err := os.Lstat(cursor)
		if err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("directory chain contains a non-directory or symlink")
			}
			if err := verifyRealDirectory(cursor); err != nil {
				return err
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, cursor)
		parent := filepath.Dir(cursor)
		if parent == cursor {
			return errors.New("directory chain has no existing real ancestor")
		}
		cursor = parent
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := os.Mkdir(missing[index], mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := verifyRealDirectory(missing[index]); err != nil {
			return err
		}
	}
	return verifyRealDirectory(path)
}

func verifyRealDirectory(path string) error {
	clean := filepath.Clean(path)
	info, err := os.Lstat(clean)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("path is not a real directory")
	}
	resolved, err := filepath.EvalSymlinks(clean)
	if err != nil || !sameCanonicalPath(clean, resolved) {
		return errors.New("directory chain resolves through a link")
	}
	return nil
}

func sameCanonicalPath(left, right string) bool {
	left, right = filepath.Clean(left), filepath.Clean(right)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(left, right)
	}
	return left == right
}

func (spool *DurableSpool) recordingDir(recordingID string) string {
	return filepath.Join(spool.root, "capture-v2", recordingID)
}

func (spool *DurableSpool) manifestPath(recordingID string) string {
	return filepath.Join(spool.recordingDir(recordingID), "state.json")
}

func (spool *DurableSpool) loadLocked(recordingID string) (*SpoolManifest, error) {
	if !validRecordingID(recordingID) {
		return nil, errors.New("invalid recording ID for spool")
	}
	if err := spool.verifyRecordingDirectoriesLocked(recordingID, false); err != nil {
		return nil, err
	}
	bytes, err := os.ReadFile(spool.manifestPath(recordingID))
	if err != nil {
		return nil, err
	}
	var manifest SpoolManifest
	if err := json.Unmarshal(bytes, &manifest); err != nil || manifest.Version != 1 {
		return nil, errors.New("invalid durable spool manifest")
	}
	if manifest.RecordingID != recordingID {
		return nil, errors.New("durable spool manifest identity mismatch")
	}
	if err := spool.verifyLocked(&manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (spool *DurableSpool) requireLocked(recordingID string) (*SpoolManifest, error) {
	if !validRecordingID(recordingID) {
		return nil, errors.New("invalid recording ID for spool")
	}
	if err := spool.verifyRecordingDirectoriesLocked(recordingID, false); err != nil {
		return nil, err
	}
	if manifest := spool.manifests[recordingID]; manifest != nil {
		return manifest, nil
	}
	manifest, err := spool.loadLocked(recordingID)
	if err != nil {
		return nil, err
	}
	spool.manifests[recordingID] = manifest
	return manifest, nil
}

func requireSpoolFence(manifest *SpoolManifest, fence SpoolMutationFence) error {
	invalidFence := fence.AttemptID == "" || fence.RecordingID == "" ||
		fence.WorkerID == "" || fence.WorkerGenerationID == "" ||
		fence.CaptureGeneration <= 0
	if invalidFence {
		return ErrAttemptFenceMismatch
	}
	exactOwner := manifest.AttemptID == fence.AttemptID &&
		manifest.RecordingID == fence.RecordingID &&
		manifest.WorkerID == fence.WorkerID &&
		manifest.WorkerGenerationID == fence.WorkerGenerationID &&
		manifest.CaptureGeneration == fence.CaptureGeneration
	if !exactOwner {
		return ErrAttemptFenceMismatch
	}
	return nil
}

func (spool *DurableSpool) verifyLocked(manifest *SpoolManifest) error {
	validBinding := manifest.BindingMode == "capture" || manifest.BindingMode == "seal-only"
	validStatus := manifest.Status == "capturing" || manifest.Status == "sealed" || manifest.Status == "failed"
	_, startedAtErr := time.Parse(time.RFC3339Nano, manifest.CaptureStartedAt)
	invalidEnvelope := len(manifest.AttemptID) > 128 || !validRecordingID(manifest.RecordingID) || manifest.WorkerID == "" ||
		len(manifest.WorkerID) > 128 || manifest.WorkerGenerationID == "" ||
		len(manifest.WorkerGenerationID) > 128 || manifest.CaptureGeneration <= 0 || startedAtErr != nil ||
		!validBinding || !validStatus || manifest.SourceBytes < 0 || manifest.LastCheckpointSequence < -1 ||
		manifest.LastCheckpointTimelineMS < 0 ||
		(manifest.LastCheckpointSequence == -1 && manifest.LastCheckpointHash != "")
	if invalidEnvelope {
		return errors.New("invalid durable spool manifest envelope")
	}
	if manifest.LastCheckpointSequence >= 0 {
		hash, err := canonicalCheckpointHash(
			manifest.CaptureGeneration,
			manifest.LastCheckpointTimelineMS,
			manifest.Records,
		)
		if err != nil || hash != manifest.LastCheckpointHash {
			return errors.New("durable spool checkpoint checksum mismatch")
		}
	}
	if err := validateTimelineRecords(manifest.Records, true); err != nil && len(manifest.Records) > 0 {
		return err
	}
	var total int64
	keys := map[string]struct{}{}
	for _, segment := range manifest.Segments {
		if err := validateSegment(segment.SegmentDescriptor); err != nil {
			return err
		}
		if segment.CaptureGeneration <= 0 || segment.CaptureGeneration > manifest.CaptureGeneration ||
			(segment.Committed && segment.UploadID == "") || (!segment.Committed && segment.UploadID != "") {
			return errors.New("invalid durable segment state")
		}
		key := fmt.Sprintf("%d:%s:%d", segment.CaptureGeneration, segment.TrackID, segment.Sequence)
		if _, exists := keys[key]; exists {
			return errors.New("duplicate durable segment")
		}
		keys[key] = struct{}{}
		path, err := spool.validatedSegmentPath(manifest.RecordingID, segment.LocalPath)
		if err != nil {
			return err
		}
		if err := verifyFile(path, segment.ExpectedBytes, segment.SHA256); err != nil {
			return err
		}
		total += segment.ExpectedBytes
	}
	if total != manifest.SourceBytes || total > spool.maxBytes {
		return errors.New("durable spool source byte accounting mismatch")
	}
	return nil
}

func (spool *DurableSpool) validatedSegmentPath(recordingID, localPath string) (string, error) {
	invalidShape := localPath == "" || filepath.IsAbs(localPath) ||
		filepath.Clean(localPath) != localPath || filepath.Dir(localPath) != "segments"
	if invalidShape {
		return "", errors.New("invalid durable segment local path")
	}
	recordingDirectory := spool.recordingDir(recordingID)
	path := filepath.Join(recordingDirectory, localPath)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("durable segment path is not a regular file")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", errors.New("resolve durable segment path")
	}
	relative, err := filepath.Rel(recordingDirectory, resolved)
	escaped := err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator))
	if escaped {
		return "", errors.New("durable segment path escapes recording directory")
	}
	return path, nil
}

func (spool *DurableSpool) persistLocked(manifest *SpoolManifest) error {
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return writeAtomicFileWithHook(
		spool.manifestPath(manifest.RecordingID),
		encoded,
		0o600,
		spool.injectFailure,
	)
}

func writeAtomicFile(path string, bytes []byte, mode os.FileMode) error {
	return writeAtomicFileWithHook(path, bytes, mode, nil)
}

func writeAtomicFileWithHook(
	path string,
	bytes []byte,
	mode os.FileMode,
	hook func(string) error,
) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".state-*.tmp")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err := file.Chmod(mode); err != nil {
		file.Close()
		return err
	}
	if _, err := file.Write(bytes); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if hook != nil {
		if err := hook("after-manifest-temp-fsync"); err != nil {
			file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		return err
	}
	if hook != nil {
		if err := hook("after-manifest-rename"); err != nil {
			return err
		}
	}
	return syncDirectory(filepath.Dir(path))
}

func linkImmutable(
	staging string,
	destination string,
	expectedBytes int64,
	expectedSHA string,
) (bool, error) {
	stagingInfo, err := os.Lstat(staging)
	if err != nil || !stagingInfo.Mode().IsRegular() || stagingInfo.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("staging segment is not a regular file")
	}
	if err := verifyFile(staging, expectedBytes, expectedSHA); err != nil {
		return false, err
	}
	if _, statErr := os.Stat(destination); statErr == nil {
		destinationInfo, err := os.Lstat(destination)
		if err != nil || !destinationInfo.Mode().IsRegular() || destinationInfo.Mode()&os.ModeSymlink != 0 {
			return false, errors.New("immutable destination is not a regular file")
		}
		if existingErr := verifyFile(destination, expectedBytes, expectedSHA); existingErr != nil {
			return false, errors.New("immutable durable segment conflict")
		}
		if !os.SameFile(stagingInfo, destinationInfo) {
			return false, errors.New("immutable destination does not share staging allocation")
		}
		return false, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return false, statErr
	}
	if err := os.Link(staging, destination); err != nil {
		return false, err
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return true, err
	}
	return true, verifyFile(destination, expectedBytes, expectedSHA)
}

func unlinkAndSync(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func verifyFile(path string, expectedBytes int64, expectedSHA string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	digest := sha256.New()
	written, err := io.Copy(digest, file)
	if err != nil || written != expectedBytes || hex.EncodeToString(digest.Sum(nil)) != expectedSHA {
		return errors.New("durable segment checksum conflict")
	}
	return nil
}

func cloneManifest(source *SpoolManifest) *SpoolManifest {
	copy := *source
	copy.Records = append([]CheckpointRecord{}, source.Records...)
	copy.Segments = append([]DurableSegment{}, source.Segments...)
	sort.Slice(copy.Segments, func(i, j int) bool {
		if copy.Segments[i].CaptureGeneration != copy.Segments[j].CaptureGeneration {
			return copy.Segments[i].CaptureGeneration < copy.Segments[j].CaptureGeneration
		}
		if copy.Segments[i].TrackID != copy.Segments[j].TrackID {
			return copy.Segments[i].TrackID < copy.Segments[j].TrackID
		}
		return copy.Segments[i].Sequence < copy.Segments[j].Sequence
	})
	return &copy
}
