package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	recording "conference/recording-bot/internal/recording"
	"conference/recording-bot/internal/signaling"
	"conference/recording-bot/internal/worker"
)

type shimConfig struct {
	host         string
	port         int
	workerID     string
	secret       string
	startTimeout time.Duration
	stopTimeout  time.Duration
	expected     []signaling.Source
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "capture control shim failed:", err)
		os.Exit(1)
	}
}

func run() error {
	configuration, err := readShimConfig()
	if err != nil {
		return err
	}
	generationID, err := newGenerationID()
	if err != nil {
		return errors.New("generate worker generation ID")
	}
	runner, err := recording.NewPionAssignedProbeRunner(configuration.expected)
	if err != nil {
		return err
	}
	shim, err := worker.NewControlShimHandler(worker.ControlShimConfig{
		WorkerID:     configuration.workerID,
		GenerationID: generationID,
		JWTSecret:    configuration.secret,
		StartTimeout: configuration.startTimeout,
		StopTimeout:  configuration.stopTimeout,
	}, runner)
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              net.JoinHostPort(configuration.host, strconv.Itoa(configuration.port)),
		Handler:           shim,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      serverWriteTimeout(configuration),
		IdleTimeout:       30 * time.Second,
	}
	shutdown := make(chan os.Signal, 1)
	shutdownDone := make(chan struct{})
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		defer close(shutdownDone)
		<-shutdown
		ctx, cancel := context.WithTimeout(
			context.Background(),
			serverWriteTimeout(configuration)+time.Second,
		)
		defer cancel()
		_ = server.Shutdown(ctx)
		shim.Close()
	}()
	fmt.Println("capture control shim listening")
	err = server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		<-shutdownDone
		return nil
	}
	return err
}

func readShimConfig() (shimConfig, error) {
	host := os.Getenv("RECORDING_BOT_BIND_HOST")
	if host == "" {
		host = "0.0.0.0"
	}
	port := 8090
	if rawPort := os.Getenv("RECORDING_BOT_PORT"); rawPort != "" {
		parsed, err := strconv.Atoi(rawPort)
		if err != nil || parsed < 1 || parsed > 65_535 {
			return shimConfig{}, errors.New("RECORDING_BOT_PORT must be a valid port")
		}
		port = parsed
	}
	workerID := os.Getenv("RECORDING_BOT_WORKER_ID")
	secret := os.Getenv("RECORDING_BOT_CONTROL_JWT_HS256_SECRET")
	if workerID == "" || len(workerID) > 128 {
		return shimConfig{}, errors.New("RECORDING_BOT_WORKER_ID is required")
	}
	if len(secret) < 32 {
		return shimConfig{}, errors.New("RECORDING_BOT_CONTROL_JWT_HS256_SECRET must contain at least 32 bytes")
	}
	stopTimeout := 5 * time.Second
	if rawTimeout := os.Getenv("RECORDING_BOT_STOP_TIMEOUT_MS"); rawTimeout != "" {
		milliseconds, err := strconv.Atoi(rawTimeout)
		if err != nil || milliseconds < 1_000 || milliseconds > 30_000 {
			return shimConfig{}, errors.New("RECORDING_BOT_STOP_TIMEOUT_MS must be between 1000 and 30000")
		}
		stopTimeout = time.Duration(milliseconds) * time.Millisecond
	}
	startTimeout := 10 * time.Second
	if rawTimeout := os.Getenv("RECORDING_BOT_START_TIMEOUT_MS"); rawTimeout != "" {
		milliseconds, err := strconv.Atoi(rawTimeout)
		if err != nil || milliseconds < 1_000 || milliseconds > 25_000 {
			return shimConfig{}, errors.New("RECORDING_BOT_START_TIMEOUT_MS must be between 1000 and 25000")
		}
		startTimeout = time.Duration(milliseconds) * time.Millisecond
	}
	expected, err := expectedSources(os.Getenv("RECORDING_CAPTURE_EXPECTED_SOURCES"))
	if err != nil {
		return shimConfig{}, err
	}
	return shimConfig{
		host:         host,
		port:         port,
		workerID:     workerID,
		secret:       secret,
		startTimeout: startTimeout,
		stopTimeout:  stopTimeout,
		expected:     expected,
	}, nil
}

func serverWriteTimeout(config shimConfig) time.Duration {
	requestTimeout := max(config.startTimeout, config.stopTimeout)
	return requestTimeout + 5*time.Second
}

func expectedSources(raw string) ([]signaling.Source, error) {
	if raw == "" {
		raw = "mic,camera,screen"
	}
	seen := map[signaling.Source]struct{}{}
	result := []signaling.Source{}
	for _, item := range strings.Split(raw, ",") {
		source := signaling.Source(strings.ToLower(strings.TrimSpace(item)))
		if !source.Valid() {
			return nil, errors.New("RECORDING_CAPTURE_EXPECTED_SOURCES contains an invalid source")
		}
		if _, exists := seen[source]; exists {
			continue
		}
		seen[source] = struct{}{}
		result = append(result, source)
	}
	return result, nil
}

func newGenerationID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return "gen_" + hex.EncodeToString(bytes), nil
}
