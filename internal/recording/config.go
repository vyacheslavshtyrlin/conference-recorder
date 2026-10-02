package recording

import (
	"errors"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	developmentRuntimeEnvironment = "development"
	productionRuntimeEnvironment  = "production"
	defaultCapturePort            = 8090
	defaultMaxSlots               = 1
	defaultStartTimeout           = 30 * time.Second
	defaultStopTimeout            = 5 * time.Second
	defaultRecoveryTimeout        = 30 * time.Second
	defaultFFmpegTimeout          = 120 * time.Second
	defaultMaxSourceBytes         = int64(4_000_000_000)
	defaultMaxDuration            = 14_400 * time.Second
	defaultStorageRoot            = "/var/lib/conference-recording"
	defaultObservabilityLevel     = "info"
)

var (
	workerIDPattern          = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,128}$`)
	placeholderSecretPattern = regexp.MustCompile(
		`(?i)(^|[^a-z0-9])(change[-_]me|example|local|replace[-_]with|placeholder)($|[^a-z0-9])`,
	)
)

// Config contains capture worker runtime settings.
type Config struct {
	RuntimeEnvironment   string
	BindHost             string
	Port                 int
	WorkerID             string
	MaxSlots             int
	StartTimeout         time.Duration
	StopTimeout          time.Duration
	RecoveryTimeout      time.Duration
	StorageRoot          string
	RecordingAPIURL      *url.URL
	CaptureServiceSecret string
	ControlJWTSecret     string
	MaxInFlightPUT       time.Duration
	FFmpegPath           string
	FFmpegTimeout        time.Duration
	MaxSourceBytes       int64
	MaxDuration          time.Duration
	LogLevel             string
}

// LoadConfig reads and validates capture worker configuration.
func LoadConfig() (Config, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return Config{}, errors.New("read hostname")
	}
	return loadConfig(os.LookupEnv, hostname)
}

type lookupEnv func(string) (string, bool)

func loadConfig(lookup lookupEnv, hostname string) (Config, error) {
	config := Config{
		RuntimeEnvironment: envOrDefault(
			lookup,
			"RECORDING_RUNTIME_ENV",
			productionRuntimeEnvironment,
		),
		BindHost:             envOrDefault(lookup, "RECORDING_BOT_BIND_HOST", "0.0.0.0"),
		WorkerID:             envOrDefault(lookup, "RECORDING_BOT_WORKER_ID", hostname),
		StorageRoot:          envOrDefault(lookup, "RECORDING_STORAGE_ROOT", defaultStorageRoot),
		ControlJWTSecret:     envOrDefault(lookup, "RECORDING_BOT_CONTROL_JWT_HS256_SECRET", ""),
		CaptureServiceSecret: envOrDefault(lookup, "RECORDING_CAPTURE_SERVICE_SECRET", ""),
		FFmpegPath:           envOrDefault(lookup, "RECORDING_FFMPEG_PATH", "ffmpeg"),
		LogLevel: envOrDefault(
			lookup,
			"RECORDING_BOT_LOG_LEVEL",
			defaultObservabilityLevel,
		),
	}
	var err error
	config.Port, err = boundedInt(lookup, "RECORDING_BOT_PORT", defaultCapturePort, 1, 65_535)
	if err != nil {
		return Config{}, err
	}
	config.MaxSlots, err = boundedInt(lookup, "RECORDING_BOT_MAX_SLOTS", defaultMaxSlots, 1, 32)
	if err != nil {
		return Config{}, err
	}
	config.StartTimeout, err = boundedDuration(
		lookup,
		"RECORDING_BOT_START_TIMEOUT_MS",
		defaultStartTimeout,
		1_000,
		120_000,
	)
	if err != nil {
		return Config{}, err
	}
	config.StopTimeout, err = boundedDuration(
		lookup,
		"RECORDING_BOT_STOP_TIMEOUT_MS",
		defaultStopTimeout,
		1_000,
		30_000,
	)
	if err != nil {
		return Config{}, err
	}
	config.RecoveryTimeout, err = boundedDuration(
		lookup,
		"RECORDING_BOT_RECOVERY_TIMEOUT_MS",
		defaultRecoveryTimeout,
		1_000,
		300_000,
	)
	if err != nil {
		return Config{}, err
	}
	config.MaxInFlightPUT, err = requiredBoundedSeconds(
		lookup,
		"RECORDING_S3_MAX_IN_FLIGHT_PUT_SECONDS",
		1,
		3_600,
	)
	if err != nil {
		return Config{}, err
	}
	config.FFmpegTimeout, err = boundedDuration(
		lookup,
		"RECORDING_FFMPEG_TIMEOUT_MS",
		defaultFFmpegTimeout,
		1_000,
		600_000,
	)
	if err != nil {
		return Config{}, err
	}
	config.MaxSourceBytes, err = boundedInt64(
		lookup,
		"RECORDING_MAX_SOURCE_BYTES",
		defaultMaxSourceBytes,
		1_000_000,
		1_000_000_000_000,
	)
	if err != nil {
		return Config{}, err
	}
	config.MaxDuration, err = boundedSeconds(
		lookup,
		"RECORDING_MAX_DURATION_SECONDS",
		defaultMaxDuration,
		1,
		86_400,
	)
	if err != nil {
		return Config{}, err
	}

	rawAPIURL := envOrDefault(lookup, "RECORDING_API_INTERNAL_URL", "")
	config.RecordingAPIURL, err = parsePrivateHTTPURL(rawAPIURL)
	if err != nil {
		return Config{}, err
	}
	if err := config.validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (config Config) validate() error {
	runtimeEnvironment := config.RuntimeEnvironment
	if runtimeEnvironment == "" {
		runtimeEnvironment = productionRuntimeEnvironment
	}
	switch runtimeEnvironment {
	case developmentRuntimeEnvironment, productionRuntimeEnvironment:
	default:
		return errors.New("RECORDING_RUNTIME_ENV must be development or production")
	}
	if strings.TrimSpace(config.BindHost) == "" {
		return errors.New("RECORDING_BOT_BIND_HOST must not be empty")
	}
	if config.Port < 1 || config.Port > 65_535 {
		return errors.New("RECORDING_BOT_PORT must be a valid port")
	}
	if !workerIDPattern.MatchString(config.WorkerID) {
		return errors.New("RECORDING_BOT_WORKER_ID is invalid")
	}
	if config.MaxSlots < 1 || config.MaxSlots > 32 {
		return errors.New("RECORDING_BOT_MAX_SLOTS is outside its supported range")
	}
	validStartTimeout := config.StartTimeout >= time.Second && config.StartTimeout <= 120*time.Second
	validStopTimeout := config.StopTimeout >= time.Second && config.StopTimeout <= 30*time.Second
	validRecoveryTimeout := config.RecoveryTimeout >= time.Second && config.RecoveryTimeout <= 300*time.Second
	validPUTTimeout := config.MaxInFlightPUT >= time.Second && config.MaxInFlightPUT <= time.Hour
	validFFmpegTimeout := config.FFmpegTimeout >= time.Second && config.FFmpegTimeout <= 10*time.Minute
	if !validStartTimeout || !validStopTimeout || !validRecoveryTimeout || !validPUTTimeout ||
		!validFFmpegTimeout {
		return errors.New("recording worker timeout is outside its supported range")
	}
	if strings.TrimSpace(config.FFmpegPath) == "" || strings.ContainsAny(config.FFmpegPath, "\x00\r\n") {
		return errors.New("RECORDING_FFMPEG_PATH is invalid")
	}
	if config.MaxSourceBytes < 1_000_000 || config.MaxSourceBytes > 1_000_000_000_000 {
		return errors.New("RECORDING_MAX_SOURCE_BYTES is outside its supported range")
	}
	if config.MaxDuration < time.Second || config.MaxDuration > 24*time.Hour {
		return errors.New("RECORDING_MAX_DURATION_SECONDS is outside its supported range")
	}
	if strings.TrimSpace(config.StorageRoot) == "" {
		return errors.New("RECORDING_STORAGE_ROOT must not be empty")
	}
	if config.RecordingAPIURL == nil || config.RecordingAPIURL.Host == "" {
		return errors.New("RECORDING_API_INTERNAL_URL is required")
	}
	if len(config.CaptureServiceSecret) < 32 {
		return errors.New("RECORDING_CAPTURE_SERVICE_SECRET must contain at least 32 bytes")
	}
	if runtimeEnvironment == productionRuntimeEnvironment &&
		isPlaceholderSecret(config.CaptureServiceSecret) {
		return errors.New("RECORDING_CAPTURE_SERVICE_SECRET must not use a placeholder value")
	}
	if len(config.ControlJWTSecret) < 32 {
		return errors.New("RECORDING_BOT_CONTROL_JWT_HS256_SECRET must contain at least 32 bytes")
	}
	if runtimeEnvironment == productionRuntimeEnvironment &&
		isPlaceholderSecret(config.ControlJWTSecret) {
		return errors.New("RECORDING_BOT_CONTROL_JWT_HS256_SECRET must not use a placeholder value")
	}
	switch config.LogLevel {
	case "debug", "info", "warn", "error":
		return nil
	default:
		return errors.New("RECORDING_BOT_LOG_LEVEL must be debug, info, warn or error")
	}
}

func isPlaceholderSecret(secret string) bool {
	return placeholderSecretPattern.MatchString(strings.TrimSpace(secret))
}

func requiredBoundedSeconds(
	lookup lookupEnv,
	name string,
	minimum int,
	maximum int,
) (time.Duration, error) {
	raw, exists := lookup(name)
	if !exists {
		return 0, errors.New(name + " is required")
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New(name + " is required")
	}
	for _, character := range raw {
		if character < '0' || character > '9' {
			return 0, errors.New(name + " must be a base-10 integer from 1 through 3600")
		}
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || value < int64(minimum) || value > int64(maximum) {
		return 0, errors.New(name + " must be a base-10 integer from 1 through 3600")
	}
	return time.Duration(value) * time.Second, nil
}

func envOrDefault(lookup lookupEnv, name, fallback string) string {
	value, exists := lookup(name)
	if !exists {
		return fallback
	}
	return strings.TrimSpace(value)
}

func boundedInt(
	lookup lookupEnv,
	name string,
	fallback int,
	minimum int,
	maximum int,
) (int, error) {
	raw, exists := lookup(name)
	if !exists {
		return fallback, nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || value < minimum || value > maximum {
		return 0, errors.New(name + " is outside its supported range")
	}
	return value, nil
}

func boundedDuration(
	lookup lookupEnv,
	name string,
	fallback time.Duration,
	minimumMilliseconds int,
	maximumMilliseconds int,
) (time.Duration, error) {
	value, err := boundedInt(
		lookup,
		name,
		int(fallback/time.Millisecond),
		minimumMilliseconds,
		maximumMilliseconds,
	)
	if err != nil {
		return 0, err
	}
	return time.Duration(value) * time.Millisecond, nil
}

func boundedInt64(
	lookup lookupEnv,
	name string,
	fallback int64,
	minimum int64,
	maximum int64,
) (int64, error) {
	raw, exists := lookup(name)
	if !exists {
		return fallback, nil
	}
	value, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || value < minimum || value > maximum {
		return 0, errors.New(name + " is outside its supported range")
	}
	return value, nil
}

func boundedSeconds(
	lookup lookupEnv,
	name string,
	fallback time.Duration,
	minimumSeconds int,
	maximumSeconds int,
) (time.Duration, error) {
	value, err := boundedInt(
		lookup,
		name,
		int(fallback/time.Second),
		minimumSeconds,
		maximumSeconds,
	)
	if err != nil {
		return 0, err
	}
	return time.Duration(value) * time.Second, nil
}

func parsePrivateHTTPURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return nil, errors.New("RECORDING_API_INTERNAL_URL must be an absolute HTTP(S) URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errors.New("RECORDING_API_INTERNAL_URL must be an absolute HTTP(S) URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("RECORDING_API_INTERNAL_URL must not contain credentials, query or fragment")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	return parsed, nil
}
