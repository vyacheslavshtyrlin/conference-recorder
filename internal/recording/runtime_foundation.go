package recording

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// FilesystemSegmentSpool verifies the private staging root during startup.
type FilesystemSegmentSpool struct {
	root string
}

// NewFilesystemSegmentSpool constructs the bounded local staging boundary.
func NewFilesystemSegmentSpool(root string) (*FilesystemSegmentSpool, error) {
	if root == "" || !filepath.IsAbs(root) {
		return nil, errors.New("segment spool root must be absolute")
	}
	return &FilesystemSegmentSpool{root: filepath.Clean(root)}, nil
}

// Prepare verifies that the runtime user can create private spool files.
func (spool *FilesystemSegmentSpool) Prepare(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(spool.root, 0o700); err != nil {
		return errors.New("prepare segment spool root")
	}
	if err := os.Chmod(spool.root, 0o700); err != nil {
		return errors.New("secure segment spool root")
	}
	info, err := os.Stat(spool.root)
	if err != nil || !info.IsDir() {
		return errors.New("segment spool root is not a directory")
	}
	probe, err := os.CreateTemp(spool.root, ".foundation-ready-*")
	if err != nil {
		return errors.New("segment spool root is not writable")
	}
	name := probe.Name()
	closeErr := probe.Close()
	removeErr := os.Remove(name)
	if closeErr != nil || removeErr != nil {
		return errors.New("clean segment spool readiness probe")
	}
	return nil
}

func (spool *FilesystemSegmentSpool) Close() error {
	return nil
}

type PrivateUploadClient struct {
	baseURL          *url.URL
	secret           []byte
	putTimeout       time.Duration
	apiTimeout       time.Duration
	maxRecoveryBytes int64
	httpClient       *http.Client
}

const (
	defaultCaptureAPIOperationTimeout = 30 * time.Second
	defaultRecoverySnapshotBytes      = int64(64 * 1024 * 1024)
)

func NewPrivateUploadClient(baseURL *url.URL, secret string, putTimeout time.Duration) (*PrivateUploadClient, error) {
	if baseURL == nil || baseURL.Host == "" {
		return nil, errors.New("private upload API URL is required")
	}
	if len(secret) < 32 {
		return nil, errors.New("capture service secret must contain at least 32 bytes")
	}
	if putTimeout < time.Second || putTimeout > time.Hour {
		return nil, errors.New("presigned PUT timeout must be between 1 and 3600 seconds")
	}
	transport := &http.Transport{DisableKeepAlives: true}
	return &PrivateUploadClient{
		baseURL:          cloneURL(baseURL),
		secret:           append([]byte{}, secret...),
		putTimeout:       putTimeout,
		apiTimeout:       defaultCaptureAPIOperationTimeout,
		maxRecoveryBytes: defaultRecoverySnapshotBytes,
		httpClient:       &http.Client{Transport: transport},
	}, nil
}

// Ready validates only local foundation prerequisites; it does not contact API.
func (client *PrivateUploadClient) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if client.baseURL == nil || client.baseURL.Host == "" || len(client.secret) < 32 ||
		client.putTimeout < time.Second || client.putTimeout > time.Hour ||
		client.apiTimeout < time.Second || client.maxRecoveryBytes < apiResponseLimit || client.httpClient == nil {
		return errors.New("private upload client is not configured")
	}
	return nil
}

// PutPresigned keeps the deadline active until both request bodies are closed.
func (client *PrivateUploadClient) PutPresigned(
	ctx context.Context,
	target *url.URL,
	headers http.Header,
	body io.ReadCloser,
) (int, error) {
	if target == nil || target.Host == "" || body == nil {
		return 0, errors.New("invalid presigned PUT request")
	}
	requestContext, cancel := context.WithTimeout(ctx, client.putTimeout)
	defer cancel()
	bodyClosed := make(chan struct{})
	go func() {
		select {
		case <-requestContext.Done():
			_ = body.Close()
		case <-bodyClosed:
		}
	}()
	defer close(bodyClosed)

	request, err := http.NewRequestWithContext(requestContext, http.MethodPut, target.String(), body)
	if err != nil {
		_ = body.Close()
		return 0, errors.New("build presigned PUT request")
	}
	request.Header = headers.Clone()
	contentLength, err := strconv.ParseInt(request.Header.Get("Content-Length"), 10, 64)
	if err != nil || contentLength < 0 {
		_ = body.Close()
		return 0, errors.New("presigned PUT requires a valid content-length")
	}
	request.ContentLength = contentLength
	request.Close = true
	response, err := client.httpClient.Do(request)
	if err != nil {
		_ = body.Close()
		return 0, errors.New("execute presigned PUT request")
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	_ = body.Close()
	if readErr != nil || closeErr != nil || requestContext.Err() != nil {
		return response.StatusCode, errors.New("complete presigned PUT request")
	}
	return response.StatusCode, nil
}

func cloneURL(source *url.URL) *url.URL {
	copy := *source
	return &copy
}

type BoundedProcessSupervisor struct {
	mu       sync.Mutex
	stopping bool
	running  map[string]context.CancelFunc
	done     map[string]<-chan struct{}
	pending  map[string]struct{}
}

type foundationState uint8

const (
	foundationNew foundationState = iota
	foundationStarting
	foundationRunning
	foundationShuttingDown
	foundationClosed
)

type foundationStartOperation struct {
	done   chan struct{}
	result error
}

type foundationShutdownOperation struct {
	done   chan struct{}
	result error
}

// NewBoundedProcessSupervisor constructs an empty process registry.
func NewBoundedProcessSupervisor() *BoundedProcessSupervisor {
	return &BoundedProcessSupervisor{
		running: map[string]context.CancelFunc{},
		done:    map[string]<-chan struct{}{},
		pending: map[string]struct{}{},
	}
}

// Ready reports whether new supervised processes may be registered.
func (supervisor *BoundedProcessSupervisor) Ready() error {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.stopping {
		return errors.New("process supervisor is stopping")
	}
	if len(supervisor.pending) != 0 {
		return errors.New("supervised process cleanup is pending")
	}
	return nil
}

// Register records an already-started child so shutdown can terminate it.
func (supervisor *BoundedProcessSupervisor) Register(
	name string,
	cancel context.CancelFunc,
	done <-chan struct{},
) error {
	if name == "" || cancel == nil || done == nil {
		return errors.New("invalid supervised process")
	}
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	if supervisor.stopping {
		return errors.New("process supervisor is stopping")
	}
	if len(supervisor.pending) != 0 {
		return errors.New("supervised process cleanup is pending")
	}
	if _, exists := supervisor.running[name]; exists {
		return errors.New("supervised process already exists")
	}
	supervisor.running[name] = cancel
	supervisor.done[name] = done
	return nil
}

func (supervisor *BoundedProcessSupervisor) MarkPending(name string) {
	supervisor.mu.Lock()
	if _, exists := supervisor.running[name]; exists {
		supervisor.pending[name] = struct{}{}
	}
	supervisor.mu.Unlock()
}

func (supervisor *BoundedProcessSupervisor) Complete(name string) {
	supervisor.mu.Lock()
	delete(supervisor.running, name)
	delete(supervisor.done, name)
	delete(supervisor.pending, name)
	supervisor.mu.Unlock()
}

// Shutdown cancels all registered processes and waits within the caller bound.
func (supervisor *BoundedProcessSupervisor) Shutdown(ctx context.Context) error {
	supervisor.mu.Lock()
	supervisor.stopping = true
	done := make([]<-chan struct{}, 0, len(supervisor.done))
	for name, cancel := range supervisor.running {
		cancel()
		done = append(done, supervisor.done[name])
	}
	supervisor.mu.Unlock()

	for _, processDone := range done {
		select {
		case <-processDone:
		case <-ctx.Done():
			return errors.New("process supervisor shutdown timed out")
		}
	}
	return nil
}

type ProductionFoundation struct {
	config     Config
	spool      SegmentSpool
	upload     UploadClient
	supervisor ProcessSupervisor

	mu             sync.Mutex
	state          foundationState
	startCancel    context.CancelFunc
	startOperation *foundationStartOperation
	shutdown       *foundationShutdownOperation
}

// NewProductionFoundation builds the production-only dependency graph.
func NewProductionFoundation(config Config) (*ProductionFoundation, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	spool, err := NewFilesystemSegmentSpool(config.StorageRoot)
	if err != nil {
		return nil, err
	}
	upload, err := NewPrivateUploadClient(
		config.RecordingAPIURL,
		config.CaptureServiceSecret,
		config.MaxInFlightPUT,
	)
	if err != nil {
		return nil, err
	}
	return &ProductionFoundation{
		config:     config,
		spool:      spool,
		upload:     upload,
		supervisor: NewBoundedProcessSupervisor(),
		state:      foundationNew,
	}, nil
}

func (foundation *ProductionFoundation) Start(ctx context.Context) error {
	foundation.mu.Lock()
	switch foundation.state {
	case foundationRunning:
		foundation.mu.Unlock()
		return nil
	case foundationStarting:
		operation := foundation.startOperation
		foundation.mu.Unlock()
		return foundation.waitForStart(ctx, operation)
	case foundationShuttingDown, foundationClosed:
		foundation.mu.Unlock()
		return errors.New("production foundation is closing")
	case foundationNew:
	}

	startContext, cancel := context.WithCancel(ctx)
	foundation.state = foundationStarting
	foundation.startCancel = cancel
	operation := &foundationStartOperation{done: make(chan struct{})}
	foundation.startOperation = operation
	foundation.mu.Unlock()

	startErr := foundation.startDependencies(startContext)
	cancel()
	foundation.mu.Lock()
	if foundation.state == foundationStarting {
		if startErr == nil {
			foundation.state = foundationRunning
		} else {
			foundation.state = foundationNew
		}
	} else if startErr == nil {
		startErr = errors.New("production foundation closed during startup")
	}
	foundation.startCancel = nil
	operation.result = startErr
	close(operation.done)
	foundation.mu.Unlock()
	return startErr
}

func (foundation *ProductionFoundation) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	foundation.mu.Lock()
	running := foundation.state == foundationRunning
	foundation.mu.Unlock()
	if !running {
		return errors.New("production foundation is not running")
	}
	if err := foundation.upload.Ready(ctx); err != nil {
		return err
	}
	return foundation.supervisor.Ready()
}

// Shutdown closes attempt ownership and bounded runtime dependencies.
func (foundation *ProductionFoundation) Shutdown(ctx context.Context) error {
	foundation.mu.Lock()
	switch foundation.state {
	case foundationClosed:
		result := foundation.shutdown.result
		foundation.mu.Unlock()
		return result
	case foundationShuttingDown:
		operation := foundation.shutdown
		foundation.mu.Unlock()
		return foundation.waitForShutdown(ctx, operation)
	case foundationStarting:
		foundation.state = foundationShuttingDown
		foundation.startCancel()
	case foundationNew, foundationRunning:
		foundation.state = foundationShuttingDown
	}
	startOperation := foundation.startOperation
	operation := &foundationShutdownOperation{done: make(chan struct{})}
	foundation.shutdown = operation
	foundation.mu.Unlock()

	go foundation.finishShutdown(operation, startOperation)
	return foundation.waitForShutdown(ctx, operation)
}

func (foundation *ProductionFoundation) finishShutdown(
	operation *foundationShutdownOperation,
	startOperation *foundationStartOperation,
) {
	ctx, cancel := context.WithTimeout(context.Background(), foundation.config.StopTimeout)
	defer cancel()

	var startResult error
	if startOperation != nil {
		select {
		case <-startOperation.done:
		case <-ctx.Done():
			startResult = ctx.Err()
		}
	}
	result := errors.Join(
		startResult,
		foundation.supervisor.Shutdown(ctx),
		foundation.spool.Close(),
	)
	foundation.mu.Lock()
	foundation.state = foundationClosed
	operation.result = result
	close(operation.done)
	foundation.mu.Unlock()
}

// WorkerID returns the non-secret runtime identity used by health responses.
func (foundation *ProductionFoundation) WorkerID() string {
	return foundation.config.WorkerID
}

func (foundation *ProductionFoundation) ProcessSupervisor() *BoundedProcessSupervisor {
	return foundation.supervisor.(*BoundedProcessSupervisor)
}

func (foundation *ProductionFoundation) startDependencies(ctx context.Context) error {
	if err := foundation.spool.Prepare(ctx); err != nil {
		return err
	}
	if err := foundation.upload.Ready(ctx); err != nil {
		return err
	}
	return foundation.supervisor.Ready()
}

func (foundation *ProductionFoundation) waitForStart(
	ctx context.Context,
	operation *foundationStartOperation,
) error {
	select {
	case <-operation.done:
		return operation.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (foundation *ProductionFoundation) waitForShutdown(
	ctx context.Context,
	operation *foundationShutdownOperation,
) error {
	select {
	case <-operation.done:
		return operation.result
	case <-ctx.Done():
		return ctx.Err()
	}
}
