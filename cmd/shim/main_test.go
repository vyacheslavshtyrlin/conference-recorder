package main

import (
	"testing"
	"time"
)

func TestServerWriteTimeoutCoversControlOperations(t *testing.T) {
	config := shimConfig{
		startTimeout: 25 * time.Second,
		stopTimeout:  30 * time.Second,
	}
	writeTimeout := serverWriteTimeout(config)
	if writeTimeout <= config.startTimeout {
		t.Fatalf("write timeout %s does not cover start timeout", writeTimeout)
	}
	if writeTimeout <= config.stopTimeout {
		t.Fatalf("write timeout %s does not cover stop timeout", writeTimeout)
	}
}
