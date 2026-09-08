package recording

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDurableSpoolGenerationTransitionIsManifestFirstAtEveryCrashPoint(t *testing.T) {
	phases := []string{
		"before-manifest-persist",
		"after-manifest-temp-fsync",
		"after-manifest-rename",
		"after-manifest-persist",
		"after-orphan-delete",
	}
	for _, phase := range phases {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			spool, oldPath := seededSpoolGeneration(t, root)
			injected := errors.New("simulated crash")
			fired := false
			spool.failpoint = func(current string) error {
				if !fired && current == phase {
					fired = true
					return injected
				}
				return nil
			}
			fence := AttemptFence{
				AttemptID: "attempt-2", RecordingID: "rec-1",
				WorkerID: "worker-1", WorkerGenerationID: "process-2",
			}
			_, err := spool.Begin(context.Background(), fence, 2, "2026-08-25T10:00:00Z", "capture")
			if !errors.Is(err, injected) || !fired {
				t.Fatalf("failpoint %q did not interrupt transition: %v", phase, err)
			}

			restarted, err := NewDurableSpool(root, 1_000_000)
			if err != nil {
				t.Fatal(err)
			}
			onDisk, err := restarted.loadLocked("rec-1")
			if err != nil || (onDisk.CaptureGeneration != 1 && onDisk.CaptureGeneration != 2) {
				t.Fatalf("crash left no valid manifest: generation=%v err=%v", onDisk, err)
			}
			manifest, err := restarted.Begin(
				context.Background(),
				fence,
				2,
				"2026-08-25T10:00:00Z",
				"capture",
			)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.CaptureGeneration != 2 || len(manifest.Segments) != 0 || manifest.SourceBytes != 0 {
				t.Fatalf("transition did not converge: %+v", manifest)
			}
			if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("fenced orphan survived recovery: %v", err)
			}
		})
	}
}

func seededSpoolGeneration(t *testing.T, root string) (*DurableSpool, string) {
	t.Helper()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	fence := AttemptFence{
		AttemptID: "attempt-1", RecordingID: "rec-1",
		WorkerID: "worker-1", WorkerGenerationID: "process-1",
	}
	if _, err := spool.Begin(context.Background(), fence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	bytes := []byte("old-generation-segment")
	digest := sha256.Sum256(bytes)
	staging := writeTestStaging(t, root, "old.part", bytes)
	descriptor := fixtureSegmentDescriptor(int64(len(bytes)), hex.EncodeToString(digest[:]))
	segment, err := spool.StoreSegment(context.Background(), "rec-1", descriptor, staging)
	if err != nil {
		t.Fatal(err)
	}
	return spool, filepath.Join(root, "capture-v2", "rec-1", segment.LocalPath)
}

func TestDurableSpoolRejectsManifestPathEscapeBeforeVerifyOrRemove(t *testing.T) {
	for _, localPath := range []string{filepath.Join("..", "outside.webm"), filepath.Join(t.TempDir(), "absolute.webm")} {
		t.Run(filepath.Base(localPath), func(t *testing.T) {
			root := t.TempDir()
			spool, _ := seededSpoolGeneration(t, root)
			manifestPath := spool.manifestPath("rec-1")
			encoded, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			var manifest SpoolManifest
			if err := json.Unmarshal(encoded, &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.Segments[0].LocalPath = localPath
			encoded, _ = json.Marshal(manifest)
			if err := writeAtomicFile(manifestPath, encoded, 0o600); err != nil {
				t.Fatal(err)
			}
			restarted, _ := NewDurableSpool(root, 1_000_000)
			if _, err := restarted.Begin(
				context.Background(),
				AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
				1,
				"2026-08-25T10:00:00Z",
				"capture",
			); err == nil {
				t.Fatal("corrupt manifest path was accepted")
			}
		})
	}
}

func TestDurableSpoolRejectsDotRecordingPathComponents(t *testing.T) {
	spool, err := NewDurableSpool(t.TempDir(), 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	for _, recordingID := range []string{".", ".."} {
		_, err := spool.Begin(
			context.Background(),
			AttemptFence{"attempt-1", recordingID, "worker-1", "process-1"},
			1,
			"2026-08-25T10:00:00Z",
			"capture",
		)
		if err == nil {
			t.Fatalf("unsafe recording path component %q was accepted", recordingID)
		}
	}
}

func TestDurableSpoolRejectsSymlinkedDirectoryChain(t *testing.T) {
	t.Run("capture root", func(t *testing.T) {
		root := t.TempDir()
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(root, "capture-v2")); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		if _, err := NewDurableSpool(root, 1_000_000); err == nil {
			t.Fatal("symlinked capture root was accepted")
		}
	})

	for _, directory := range []string{"staging", "segments"} {
		t.Run(directory, func(t *testing.T) {
			root := t.TempDir()
			spool, err := NewDurableSpool(root, 1_000_000)
			if err != nil {
				t.Fatal(err)
			}
			_, err = spool.Begin(
				context.Background(),
				AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
				1,
				"2026-08-25T10:00:00Z",
				"capture",
			)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "capture-v2", "rec-1", directory)
			if err := os.RemoveAll(path); err != nil {
				t.Fatal(err)
			}
			outside := t.TempDir()
			if err := os.Symlink(outside, path); err != nil {
				t.Skipf("symlink unavailable: %v", err)
			}
			if directory == "staging" {
				payload := []byte("external segment")
				digest := sha256.Sum256(payload)
				staging := filepath.Join(path, "external.part")
				if err := os.WriteFile(staging, payload, 0o600); err != nil {
					t.Fatal(err)
				}
				_, err = spool.StoreSegment(
					context.Background(),
					"rec-1",
					fixtureSegmentDescriptor(int64(len(payload)), hex.EncodeToString(digest[:])),
					staging,
				)
			} else {
				_, err = spool.Snapshot("rec-1")
			}
			if err == nil {
				t.Fatalf("symlinked %s directory was accepted", directory)
			}
		})
	}
}

func TestStoreSegmentRejectsAndPreservesFileOutsideRecordingStaging(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	)
	payload := []byte("must-not-be-unlinked")
	digest := sha256.Sum256(payload)
	outside := filepath.Join(root, "outside.part")
	if err := os.WriteFile(outside, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.StoreSegment(
		context.Background(),
		"rec-1",
		fixtureSegmentDescriptor(int64(len(payload)), hex.EncodeToString(digest[:])),
		outside,
	); err == nil {
		t.Fatal("segment outside recording staging was accepted")
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("rejected external staging file was mutated: %v", err)
	}
}

func TestManifestMutationsPublishOnlyDurableState(t *testing.T) {
	root := t.TempDir()
	spool, _ := seededSpoolGeneration(t, root)
	manifest, _ := spool.Snapshot("rec-1")
	segment := manifest.Segments[0]
	injected := errors.New("manifest persist interrupted")
	spool.failpoint = func(phase string) error {
		if phase == "after-manifest-temp-fsync" {
			return injected
		}
		return nil
	}
	if err := spool.MarkCommitted("rec-1", segment.CaptureGeneration, segment.TrackID, segment.Sequence, "upload-1"); !errors.Is(err, injected) {
		t.Fatalf("MarkCommitted did not surface persist failure: %v", err)
	}
	afterCommit, _ := spool.Snapshot("rec-1")
	if afterCommit.Segments[0].Committed {
		t.Fatal("failed MarkCommitted leaked into cached manifest")
	}

	records := []CheckpointRecord{}
	hash, _ := canonicalCheckpointHash(1, 1, records)
	checkpoint := GenerationCheckpoint{CaptureGeneration: 1, CheckpointSequence: 0, TimelineThroughMS: 1, PayloadSHA256: hash}
	if err := spool.StoreCheckpoint("rec-1", checkpoint, records); !errors.Is(err, injected) {
		t.Fatalf("StoreCheckpoint did not surface persist failure: %v", err)
	}
	afterCheckpoint, _ := spool.Snapshot("rec-1")
	if afterCheckpoint.LastCheckpointSequence != -1 {
		t.Fatal("failed StoreCheckpoint leaked into cached manifest")
	}

	spool.failpoint = nil
	if err := spool.MarkCommitted("rec-1", segment.CaptureGeneration, segment.TrackID, segment.Sequence, "upload-1"); err != nil {
		t.Fatal(err)
	}
	spool.failpoint = func(phase string) error {
		if phase == "after-manifest-temp-fsync" {
			return injected
		}
		return nil
	}
	if err := spool.RemoveCommitted("rec-1"); !errors.Is(err, injected) {
		t.Fatalf("RemoveCommitted did not surface persist failure: %v", err)
	}
	afterRemove, _ := spool.Snapshot("rec-1")
	if len(afterRemove.Segments) != 1 || !afterRemove.Segments[0].Committed {
		t.Fatal("failed RemoveCommitted leaked into cached manifest")
	}
}

func TestStoreSegmentPersistFailureNeverLeavesDirtyCachedSuccess(t *testing.T) {
	for _, phase := range []string{"after-manifest-temp-fsync", "after-manifest-rename"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			spool, err := NewDurableSpool(root, 1_000_000)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := spool.Begin(
				context.Background(),
				AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
				1,
				"2026-08-25T10:00:00Z",
				"capture",
			); err != nil {
				t.Fatal(err)
			}
			payload := []byte("transactional-segment")
			digest := sha256.Sum256(payload)
			staging := writeTestStaging(t, root, "staging.part", payload)
			descriptor := fixtureSegmentDescriptor(
				int64(len(payload)),
				hex.EncodeToString(digest[:]),
			)
			injected := errors.New("persist failed")
			linked := false
			spool.failpoint = func(current string) error {
				if current == phase {
					stagingInfo, stagingErr := os.Stat(staging)
					destinations, _ := filepath.Glob(filepath.Join(
						root,
						"capture-v2",
						"rec-1",
						"segments",
						"*",
					))
					if stagingErr == nil && len(destinations) == 1 {
						destinationInfo, destinationErr := os.Stat(destinations[0])
						linked = destinationErr == nil && os.SameFile(stagingInfo, destinationInfo)
					}
					return injected
				}
				return nil
			}
			if _, err := spool.StoreSegment(
				context.Background(),
				"rec-1",
				descriptor,
				staging,
			); !errors.Is(err, injected) {
				t.Fatalf("persist failpoint was not returned: %v", err)
			}
			if !linked {
				t.Fatal("promotion duplicated bytes instead of linking one physical file")
			}
			spool.failpoint = nil
			segment, err := spool.StoreSegment(
				context.Background(),
				"rec-1",
				descriptor,
				staging,
			)
			if err != nil {
				t.Fatalf("retry did not converge: %v", err)
			}
			if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("promoted staging link was not reclaimed: %v", err)
			}
			manifest, err := spool.Snapshot("rec-1")
			if err != nil || len(manifest.Segments) != 1 || manifest.Segments[0] != segment {
				t.Fatalf("retry duplicated or lost segment: manifest=%+v err=%v", manifest, err)
			}
		})
	}
}

func TestSealedManifestLeavesRecoveryHealthAfterRestart(t *testing.T) {
	root := t.TempDir()
	spool, _ := NewDurableSpool(root, 1_000_000)
	if _, err := spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"seal-only",
	); err != nil {
		t.Fatal(err)
	}
	if err := spool.MarkSealed("rec-1"); err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewDurableSpool(root, 1_000_000)
	recoverable, finalizations, err := restarted.RecoveryHealth()
	if err != nil || len(recoverable) != 0 || len(finalizations) != 0 {
		t.Fatalf("sealed recording remained unhealthy: %v %v %v", recoverable, finalizations, err)
	}
	manifest, err := restarted.loadLocked("rec-1")
	if err != nil || manifest.Status != "sealed" {
		t.Fatalf("sealed status was not durable: %+v %v", manifest, err)
	}
}

func TestRecoveryHealthFailsClosedForCorruptManifest(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "capture-v2", "rec-corrupt")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "state.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	spool, _ := NewDurableSpool(root, 1_000_000)
	if _, _, err := spool.RecoveryHealth(); err == nil {
		t.Fatal("corrupt durable recovery state was silently omitted")
	}
}

func TestRemoveCommittedSurfacesDirectorySyncFailureAndRetries(t *testing.T) {
	root := t.TempDir()
	spool, _ := seededSpoolGeneration(t, root)
	manifest, err := spool.Snapshot("rec-1")
	if err != nil {
		t.Fatal(err)
	}
	segment := manifest.Segments[0]
	if err := spool.MarkCommitted(
		"rec-1",
		segment.CaptureGeneration,
		segment.TrackID,
		segment.Sequence,
		"upload-1",
	); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("directory fsync failed")
	spool.failpoint = func(phase string) error {
		if phase == "before-committed-delete-sync" {
			return injected
		}
		return nil
	}
	if err := spool.RemoveCommitted("rec-1"); !errors.Is(err, injected) {
		t.Fatalf("directory sync failure was not surfaced: %v", err)
	}
	spool.failpoint = nil
	if err := spool.RemoveCommitted("rec-1"); err != nil {
		t.Fatalf("directory sync retry did not converge: %v", err)
	}
}

func TestDurableSpoolRejectsSymlinkedTrackedSegment(t *testing.T) {
	root := t.TempDir()
	spool, segmentPath := seededSpoolGeneration(t, root)
	external := filepath.Join(root, "external.webm")
	if err := os.WriteFile(external, []byte("old-generation-segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(segmentPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, segmentPath); err != nil {
		t.Skipf("symlink creation is unavailable: %v", err)
	}
	restarted, _ := NewDurableSpool(root, 1_000_000)
	if _, err := restarted.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	); err == nil {
		t.Fatal("symlinked tracked segment was accepted")
	}
	_ = spool
}

func TestDurableSpoolPersistsAndRecoversClosedSegment(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	fence := AttemptFence{AttemptID: "attempt-1", RecordingID: "rec-1", WorkerID: "worker-1", WorkerGenerationID: "process-1"}
	if _, err := spool.Begin(context.Background(), fence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	bytes := []byte("closed-webm-fixture")
	digest := sha256.Sum256(bytes)
	staging := writeTestStaging(t, root, "segment.part", bytes)
	descriptor := fixtureSegmentDescriptor(int64(len(bytes)), hex.EncodeToString(digest[:]))
	stored, err := spool.StoreSegment(context.Background(), "rec-1", descriptor, staging)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LocalPath == "" || stored.Committed {
		t.Fatalf("unexpected stored segment: %+v", stored)
	}
	if err := spool.MarkCommitted("rec-1", 1, descriptor.TrackID, 0, "upload-1"); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := restarted.Begin(context.Background(), fence, 1, "2026-08-25T10:00:00Z", "capture")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Segments) != 1 || !manifest.Segments[0].Committed || manifest.Segments[0].UploadID != "upload-1" {
		t.Fatalf("durable segment did not recover: %+v", manifest.Segments)
	}
}

func TestDurableSpoolFailsClosedOnChecksumConflictAndBound(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	fence := AttemptFence{AttemptID: "attempt-1", RecordingID: "rec-1", WorkerID: "worker-1", WorkerGenerationID: "process-1"}
	if _, err := spool.Begin(context.Background(), fence, 1, "2026-08-25T10:00:00Z", "capture"); err != nil {
		t.Fatal(err)
	}
	staging := writeTestStaging(t, root, "segment.part", []byte("wrong"))
	descriptor := fixtureSegmentDescriptor(7, string(make([]byte, 64)))
	if _, err := spool.StoreSegment(context.Background(), "rec-1", descriptor, staging); err == nil {
		t.Fatal("expected checksum conflict")
	}
}

func TestStagingCleanupAndTreeBoundIncludeCrashLeftovers(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	); err != nil {
		t.Fatal(err)
	}
	recordingDirectory := filepath.Join(root, "capture-v2", "rec-1")
	staging := filepath.Join(recordingDirectory, "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "crash.part"), make([]byte, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(staging, "first.part")
	second := filepath.Join(staging, "second.part")
	if err := spool.ResizeStaging("rec-1", first, 600_000); err != nil {
		t.Fatal(err)
	}
	if err := spool.ResizeStaging("rec-1", second, 500_000); err == nil {
		t.Fatal("concurrent staging reservations exceeded the storage bound")
	}
	spool.ReleaseStaging("rec-1", first)
	if err := spool.CleanupStaging("rec-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("crash staging survived startup cleanup: %v", err)
	}
}

func TestConcurrentStagingReservationsNeverExceedBound(t *testing.T) {
	root := t.TempDir()
	spool, err := NewDurableSpool(root, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Begin(
		context.Background(),
		AttemptFence{"attempt-1", "rec-1", "worker-1", "process-1"},
		1,
		"2026-08-25T10:00:00Z",
		"capture",
	); err != nil {
		t.Fatal(err)
	}

	staging := filepath.Join(root, "capture-v2", "rec-1", "staging")
	if err := os.MkdirAll(staging, 0o700); err != nil {
		t.Fatal(err)
	}
	paths := []string{
		filepath.Join(staging, "first.part"),
		filepath.Join(staging, "second.part"),
	}
	start := make(chan struct{})
	results := make(chan error, len(paths))
	for _, path := range paths {
		go func() {
			<-start
			results <- spool.ResizeStaging("rec-1", path, 600_000)
		}()
	}
	close(start)

	successes := 0
	failures := 0
	for range paths {
		if err := <-results; err != nil {
			failures++
			continue
		}
		successes++
	}
	if successes != 1 || failures != 1 {
		t.Fatalf("concurrent reservations: successes=%d failures=%d", successes, failures)
	}

	for _, path := range paths {
		spool.ReleaseStaging("rec-1", path)
	}
	if err := spool.ResizeStaging("rec-1", paths[0], 600_000); err != nil {
		t.Fatalf("released capacity was not reusable: %v", err)
	}
}

func writeTestStaging(t *testing.T, root, name string, payload []byte) string {
	t.Helper()
	directory := filepath.Join(root, "capture-v2", "rec-1", "staging")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func fixtureSegmentDescriptor(bytes int64, digest string) SegmentDescriptor {
	participantID := ParticipantInstanceID("rec-1", "participant-1")
	trackID, _ := TrackID("rec-1", participantID, SourceCamera)
	return SegmentDescriptor{
		TrackID: trackID, ParticipantInstanceID: participantID, Source: SourceCamera,
		Sequence: 0, Container: "webm", Codec: "vp8", ContentType: "video/webm",
		ExpectedBytes: bytes, SHA256: digest, StartMS: 0, EndMS: 1000,
	}
}
