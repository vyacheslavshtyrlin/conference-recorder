package worker

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// NewWorkerGenerationID creates one opaque process-generation fence.
func NewWorkerGenerationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", errors.New("generate worker generation ID")
	}
	return "gen_" + hex.EncodeToString(value), nil
}
