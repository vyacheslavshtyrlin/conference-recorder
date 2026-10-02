package recording

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type MuxRequest struct {
	Codec              string
	Intermediate       string
	Output             string
	MaximumOutputBytes int64
	PendingCleanup     func() error
}

type SegmentMuxer interface {
	Mux(ctx context.Context, request MuxRequest) error
}

type CommandRunner interface {
	Run(ctx context.Context, command string, arguments []string, stdout io.Writer) error
}

type FFmpegMuxer struct {
	path        string
	timeout     time.Duration
	runner      CommandRunner
	supervisor  *BoundedProcessSupervisor
	sequence    atomic.Uint64
	pending     atomic.Int32
	processSlot chan struct{}
}

func NewFFmpegMuxer(path string, timeout time.Duration, runner CommandRunner) (*FFmpegMuxer, error) {
	return NewSupervisedFFmpegMuxer(path, timeout, runner, nil)
}

func NewSupervisedFFmpegMuxer(
	path string,
	timeout time.Duration,
	runner CommandRunner,
	supervisor *BoundedProcessSupervisor,
) (*FFmpegMuxer, error) {
	return NewSupervisedFFmpegMuxerWithConcurrency(path, timeout, runner, supervisor, 1)
}

func NewSupervisedFFmpegMuxerWithConcurrency(
	path string,
	timeout time.Duration,
	runner CommandRunner,
	supervisor *BoundedProcessSupervisor,
	concurrency int,
) (*FFmpegMuxer, error) {
	if path == "" || timeout < time.Second || timeout > 10*time.Minute {
		return nil, errors.New("invalid FFmpeg muxer configuration")
	}
	if concurrency < 1 || concurrency > 32 {
		return nil, errors.New("invalid FFmpeg muxer concurrency")
	}
	if runner == nil {
		runner = &ExecCommandRunner{}
	}
	return &FFmpegMuxer{
		path: path, timeout: timeout, runner: runner, supervisor: supervisor,
		processSlot: make(chan struct{}, concurrency),
	}, nil
}

func (muxer *FFmpegMuxer) Mux(ctx context.Context, request MuxRequest) error {
	if muxer.pending.Load() != 0 {
		return errors.New("previous FFmpeg cleanup is still pending")
	}
	if request.Intermediate == "" || request.Output == "" || request.MaximumOutputBytes < 1 ||
		filepath.Dir(request.Intermediate) != filepath.Dir(request.Output) {
		return errors.New("muxer files must share one spool directory")
	}
	format := ""
	switch request.Codec {
	case "vp8", "opus":
		format = "webm"
	case "h264":
		format = "matroska"
	default:
		return errors.New("unsupported mux codec")
	}
	select {
	case muxer.processSlot <- struct{}{}:
	case <-ctx.Done():
		return errors.New("wait for bounded FFmpeg process slot")
	}
	releaseProcessSlot := true
	defer func() {
		if releaseProcessSlot {
			<-muxer.processSlot
		}
	}()
	operationContext, cancel := context.WithTimeout(ctx, muxer.timeout)
	defer cancel()
	done := make(chan struct{})
	processName := fmt.Sprintf("ffmpeg-%d", muxer.sequence.Add(1))
	complete := func() { close(done) }
	if muxer.supervisor != nil {
		if err := muxer.supervisor.Register(processName, cancel, done); err != nil {
			return err
		}
		complete = func() {
			close(done)
			muxer.supervisor.Complete(processName)
		}
	}
	defer func() {
		if complete != nil {
			complete()
		}
	}()
	arguments := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y",
		"-i", request.Intermediate,
		"-map", "0:0", "-c", "copy", "-f", format, "pipe:1",
	}
	file, err := os.OpenFile(request.Output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.New("create bounded mux output")
	}
	limited := &hardLimitWriter{destination: file, remaining: request.MaximumOutputBytes, cancel: cancel}
	runErr := muxer.runner.Run(operationContext, muxer.path, arguments, limited)
	var pendingExit *processExitPendingError
	if errors.As(runErr, &pendingExit) {
		muxer.pending.Add(1)
		if muxer.supervisor != nil {
			muxer.supervisor.MarkPending(processName)
		}
		finish := complete
		complete = nil
		ownedOutput, _ := file.Stat()
		_ = os.Remove(request.Output)
		releaseProcessSlot = false
		go func() {
			<-pendingExit.done
			cleanupErr := errors.Join(
				file.Close(),
				removeSameFile(request.Output, ownedOutput),
			)
			if cleanupErr == nil && request.PendingCleanup != nil {
				cleanupErr = request.PendingCleanup()
			}
			if cleanupErr != nil {
				return
			}
			muxer.pending.Add(-1)
			finish()
			<-muxer.processSlot
		}()
		return &muxCleanupPendingError{}
	}
	closeErr := file.Close()
	if runErr != nil || closeErr != nil || limited.exceeded {
		_ = os.Remove(request.Output)
		return errors.New("bounded FFmpeg segment mux failed")
	}
	info, err := os.Stat(request.Output)
	if err != nil || info.Size() > request.MaximumOutputBytes {
		_ = os.Remove(request.Output)
		return errors.New("muxed segment exceeded hard output byte limit")
	}
	file, err = os.OpenFile(request.Output, os.O_RDWR, 0)
	if err != nil {
		return errors.New("open muxed segment")
	}
	syncErr := file.Sync()
	closeErr = file.Close()
	if syncErr != nil || closeErr != nil {
		return errors.New("sync muxed segment")
	}
	return syncDirectory(filepath.Dir(request.Output))
}

type muxCleanupPendingError struct{}

func (failure *muxCleanupPendingError) Error() string {
	return "bounded FFmpeg segment mux failed while process cleanup remains pending"
}

func removeSameFile(path string, owned os.FileInfo) error {
	if owned == nil {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	current, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !os.SameFile(owned, current) {
		return nil
	}
	return os.Remove(path)
}

type ExecCommandRunner struct{}

const ffmpegExitGrace = 5 * time.Second

type processExitPendingError struct {
	done <-chan error
}

func (failure *processExitPendingError) Error() string {
	return "FFmpeg did not exit within its termination grace period"
}

func (runner *ExecCommandRunner) Run(
	ctx context.Context,
	command string,
	arguments []string,
	stdout io.Writer,
) error {
	process := exec.Command(command, arguments...)
	configureProcessTree(process)
	var stderr boundedBuffer
	process.Stdout = stdout
	process.Stderr = &stderr
	if err := process.Start(); err != nil {
		return errors.New("start FFmpeg")
	}
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("FFmpeg exited unsuccessfully: %s", stderr.String())
		}
		return nil
	case <-ctx.Done():
		cleanupContext, cancel := context.WithTimeout(context.Background(), ffmpegExitGrace)
		defer cancel()
		killProcessTree(cleanupContext, process)
		select {
		case <-done:
		case <-cleanupContext.Done():
			return &processExitPendingError{done: done}
		}
		return errors.New("FFmpeg timed out or was canceled")
	}
}

type hardLimitWriter struct {
	destination io.Writer
	remaining   int64
	cancel      context.CancelFunc
	exceeded    bool
}

func (writer *hardLimitWriter) Write(value []byte) (int, error) {
	if int64(len(value)) <= writer.remaining {
		written, err := writer.destination.Write(value)
		writer.remaining -= int64(written)
		return written, err
	}
	allowed := int(writer.remaining)
	written := 0
	if allowed > 0 {
		written, _ = writer.destination.Write(value[:allowed])
		writer.remaining -= int64(written)
	}
	writer.exceeded = true
	writer.cancel()
	return written, errors.New("mux output byte limit exceeded")
}

type boundedBuffer struct {
	mu sync.Mutex
	bytes.Buffer
}

func (buffer *boundedBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	original := len(value)
	const maximum = 4096
	if buffer.Len() >= maximum {
		return original, nil
	}
	if len(value) > maximum-buffer.Len() {
		value = value[:maximum-buffer.Len()]
	}
	_, _ = buffer.Buffer.Write(value)
	return original, nil
}

func (buffer *boundedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.Buffer.String()
}
