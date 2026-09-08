package recording

import (
	"strings"
	"testing"
)

func TestRecoveryGenerationCheckpointValidation(t *testing.T) {
	hash := strings.Repeat("a", 64)
	valid := RecoveryState{
		CaptureGeneration: 2,
		GenerationCheckpoints: []GenerationCheckpoint{
			{CaptureGeneration: 1, CheckpointSequence: 7, TimelineThroughMS: 10, PayloadSHA256: hash},
			{CaptureGeneration: 1, CheckpointSequence: 8, TimelineThroughMS: 20, PayloadSHA256: hash},
			{CaptureGeneration: 2, CheckpointSequence: 0, TimelineThroughMS: 20, PayloadSHA256: hash},
		},
	}
	if err := validateRecoveryCheckpoints(valid); err != nil {
		t.Fatalf("valid checkpoints rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*RecoveryState)
	}{
		{name: "generation zero", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[0].CaptureGeneration = 0
		}},
		{name: "future generation", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[2].CaptureGeneration = 3
		}},
		{name: "negative sequence", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[0].CheckpointSequence = -1
		}},
		{name: "noncanonical hash", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[0].PayloadSHA256 = strings.Repeat("A", 64)
		}},
		{name: "sequence gap", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[1].CheckpointSequence = 9
		}},
		{name: "timeline regression", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[1].TimelineThroughMS = 9
		}},
		{name: "timeline beyond bound", mutate: func(state *RecoveryState) {
			state.GenerationCheckpoints[0].TimelineThroughMS = 24*60*60*1000 + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := valid
			state.GenerationCheckpoints = append(
				[]GenerationCheckpoint{},
				valid.GenerationCheckpoints...,
			)
			test.mutate(&state)
			if err := validateRecoveryCheckpoints(state); err == nil {
				t.Fatal("invalid generation checkpoints accepted")
			}
		})
	}
}
