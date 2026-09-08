package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	recording "conference/recording-bot/internal/recording"
	"conference/recording-bot/internal/worker"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := runHealthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, "capture liveness check failed")
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "recording capture failed:", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := recording.LoadConfig()
	if err != nil {
		return err
	}
	foundation, err := recording.NewProductionFoundation(config)
	if err != nil {
		return err
	}
	if err := foundation.Start(context.Background()); err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), config.StopTimeout)
		defer cancel()
		_ = foundation.Shutdown(ctx)
	}()
	generationID, err := worker.NewWorkerGenerationID()
	if err != nil {
		return err
	}

	runner, err := recording.NewProductionCaptureRunner(
		config,
		foundation.ProcessSupervisor(),
	)
	if err != nil {
		return err
	}
	control, err := worker.NewProductionControlHandler(worker.ControlShimConfig{
		WorkerID: config.WorkerID, GenerationID: generationID,
		JWTSecret: config.ControlJWTSecret, StartTimeout: config.StartTimeout, StopTimeout: config.StopTimeout,
		FinalizeTimeout: config.RecoveryTimeout, MaxSlots: config.MaxSlots,
	}, runner, func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := foundation.Ready(ctx); err != nil {
			return err
		}
		_, err := exec.LookPath(config.FFmpegPath)
		return err
	})
	if err != nil {
		return err
	}
	defer control.Close()
	server := &http.Server{
		Addr:              net.JoinHostPort(config.BindHost, strconv.Itoa(config.Port)),
		Handler:           control,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      0,
		IdleTimeout:       30 * time.Second,
	}
	logger := newLogger(config.LogLevel)
	logger.Info(
		"recording capture started",
		"workerId",
		config.WorkerID,
		"acceptingAttempts",
		true,
	)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.ListenAndServe() }()

	select {
	case signalReceived := <-shutdown:
		logger.Info("recording capture shutdown requested", "signal", signalReceived.String())
	case serveErr := <-serverResult:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return serveErr
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), config.StopTimeout)
	defer cancel()
	serverErr := server.Shutdown(shutdownContext)
	controlErr := control.Shutdown(shutdownContext)
	foundationErr := foundation.Shutdown(shutdownContext)
	return errors.Join(serverErr, controlErr, foundationErr)
}

func runHealthcheck() error {
	port := defaultHealthcheckPort()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://127.0.0.1:" + strconv.Itoa(port) + "/health/live")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("liveness endpoint is unavailable")
	}
	return nil
}

func defaultHealthcheckPort() int {
	raw := os.Getenv("RECORDING_BOT_PORT")
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > 65_535 {
		return 8090
	}
	return value
}

func newLogger(level string) *slog.Logger {
	selected := slog.LevelInfo
	switch level {
	case "debug":
		selected = slog.LevelDebug
	case "warn":
		selected = slog.LevelWarn
	case "error":
		selected = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: selected}))
}
