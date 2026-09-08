package recording

import (
	"testing"
	"time"
)

func TestLoadConfigPreservesApplicableLegacyDefaults(t *testing.T) {
	environment := map[string]string{
		"RECORDING_API_INTERNAL_URL":             "http://api:3000/internal/v1/",
		"RECORDING_CAPTURE_SERVICE_SECRET":       "capture-service-secret-at-least-32-bytes",
		"RECORDING_BOT_CONTROL_JWT_HS256_SECRET": "control-service-secret-at-least-32-bytes",
		"RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS": "300",
	}
	config, err := loadConfig(mapLookup(environment), "capture-host")
	if err != nil {
		t.Fatal(err)
	}
	if config.Port != 8090 || config.MaxSlots != 1 {
		t.Fatalf("unexpected capacity defaults: port=%d slots=%d", config.Port, config.MaxSlots)
	}
	if config.WorkerID != "capture-host" {
		t.Fatalf("unexpected worker ID %q", config.WorkerID)
	}
	if config.StartTimeout != 30*time.Second || config.StopTimeout != 5*time.Second {
		t.Fatalf("unexpected timeout defaults: start=%s stop=%s", config.StartTimeout, config.StopTimeout)
	}
	if config.RecordingAPIURL.String() != "http://api:3000/internal/v1" {
		t.Fatalf("unexpected private API URL %q", config.RecordingAPIURL.String())
	}
	if config.LogLevel != "info" {
		t.Fatalf("unexpected Go observability default %q", config.LogLevel)
	}
	if config.MaxInFlightPUT != 300*time.Second {
		t.Fatalf("unexpected PUT deadline %s", config.MaxInFlightPUT)
	}
	if config.FFmpegPath != "ffmpeg" || config.FFmpegTimeout != 120*time.Second || config.MaxSourceBytes != 4_000_000_000 || config.MaxDuration != 4*time.Hour {
		t.Fatalf("unexpected capture bounds: path=%q timeout=%s bytes=%d duration=%s", config.FFmpegPath, config.FFmpegTimeout, config.MaxSourceBytes, config.MaxDuration)
	}
}

func TestLoadConfigValidatesLegacyFFmpegAndSourceBounds(t *testing.T) {
	base := map[string]string{
		"RECORDING_API_INTERNAL_URL": "http://api:3000", "RECORDING_CAPTURE_SERVICE_SECRET": "capture-service-secret-at-least-32-bytes",
		"RECORDING_BOT_CONTROL_JWT_HS256_SECRET": "control-service-secret-at-least-32-bytes", "RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS": "300",
	}
	cases := map[string]string{"RECORDING_FFMPEG_PATH": "\n", "RECORDING_FFMPEG_TIMEOUT_MS": "999", "RECORDING_MAX_SOURCE_BYTES": "999999", "RECORDING_MAX_DURATION_SECONDS": "86401"}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			environment := make(map[string]string, len(base)+1)
			for key, item := range base {
				environment[key] = item
			}
			environment[name] = value
			if _, err := loadConfig(mapLookup(environment), "capture-host"); err == nil {
				t.Fatalf("invalid %s was accepted", name)
			}
		})
	}
}

func TestLoadConfigRejectsMissingProductionPrerequisites(t *testing.T) {
	_, err := loadConfig(mapLookup(map[string]string{}), "capture-host")
	if err == nil {
		t.Fatal("missing private API configuration was accepted")
	}
}

func TestLoadConfigRejectsNonObservabilityGoSettingValue(t *testing.T) {
	environment := map[string]string{
		"RECORDING_API_INTERNAL_URL":             "http://api:3000/internal/v1",
		"RECORDING_CAPTURE_SERVICE_SECRET":       "capture-service-secret-at-least-32-bytes",
		"RECORDING_BOT_CONTROL_JWT_HS256_SECRET": "control-service-secret-at-least-32-bytes",
		"RECORDING_BOT_LOG_LEVEL":                "verbose",
		"RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS": "300",
	}
	_, err := loadConfig(mapLookup(environment), "capture-host")
	if err == nil {
		t.Fatal("unsupported observability level was accepted")
	}
}

func TestLoadConfigRejectsInvalidRequiredPUTDeadline(t *testing.T) {
	base := map[string]string{
		"RECORDING_API_INTERNAL_URL":             "http://api:3000/internal/v1",
		"RECORDING_CAPTURE_SERVICE_SECRET":       "capture-service-secret-at-least-32-bytes",
		"RECORDING_BOT_CONTROL_JWT_HS256_SECRET": "control-service-secret-at-least-32-bytes",
	}
	for _, value := range []string{"", "0", "3601", "1e2", "+1", "seconds"} {
		t.Run(value, func(t *testing.T) {
			environment := make(map[string]string, len(base)+1)
			for name, configured := range base {
				environment[name] = configured
			}
			if value != "" {
				environment["RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS"] = value
			}
			if _, err := loadConfig(mapLookup(environment), "capture-host"); err == nil {
				t.Fatalf("invalid PUT deadline %q was accepted", value)
			}
		})
	}
}

func mapLookup(values map[string]string) lookupEnv {
	return func(name string) (string, bool) {
		value, exists := values[name]
		return value, exists
	}
}
