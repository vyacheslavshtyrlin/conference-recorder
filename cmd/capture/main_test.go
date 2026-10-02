package main

import (
	"os"
	"testing"
)

func TestDefaultHealthcheckPortUsesLegacySetting(t *testing.T) {
	t.Setenv("RECORDING_BOT_PORT", "8123")
	if port := defaultHealthcheckPort(); port != 8123 {
		t.Fatalf("healthcheck port=%d", port)
	}
	if err := os.Setenv("RECORDING_BOT_PORT", "invalid"); err != nil {
		t.Fatal(err)
	}
	if port := defaultHealthcheckPort(); port != 8090 {
		t.Fatalf("invalid healthcheck port fallback=%d", port)
	}
}
