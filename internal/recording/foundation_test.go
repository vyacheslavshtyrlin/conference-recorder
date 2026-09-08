package recording

import (
	"context"
	"errors"
	"net/url"
	"sync"
	"testing"
	"time"
)

func TestProcessSupervisorShutdownIsBoundedAndIdempotent(t *testing.T) {
	supervisor := NewBoundedProcessSupervisor()
	processContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		<-processContext.Done()
		close(done)
	}()
	if err := supervisor.Register("fixture", cancel, done); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFoundationShutdownRacingBlockedStartupNeverBecomesReady(t *testing.T) {
	spool := &blockingSpool{
		entered: make(chan struct{}),
	}
	foundation := foundationWithDependencies(
		t,
		spool,
		readyUpload{},
		&blockingSupervisor{},
	)
	startResult := make(chan error, 1)
	go func() { startResult <- foundation.Start(context.Background()) }()
	<-spool.entered

	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- foundation.Shutdown(context.Background()) }()
	if err := <-startResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("startup returned %v, want cancellation", err)
	}
	if err := <-shutdownResult; err != nil {
		t.Fatal(err)
	}
	if err := foundation.Ready(context.Background()); err == nil {
		t.Fatal("foundation became ready after shutdown won the startup race")
	}
	if err := foundation.Start(context.Background()); err == nil {
		t.Fatal("closed foundation restarted")
	}
}

func TestFoundationShutdownCallerDeadlineBoundsNonCooperativeStartup(t *testing.T) {
	spool := &nonCooperativeSpool{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
	foundation := foundationWithDependencies(
		t,
		spool,
		readyUpload{},
		&blockingSupervisor{},
	)
	foundation.config.StopTimeout = 20 * time.Millisecond
	startResult := make(chan error, 1)
	go func() { startResult <- foundation.Start(context.Background()) }()
	<-spool.entered

	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	started := time.Now()
	shutdownErr := foundation.Shutdown(shutdownContext)
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("shutdown returned %v, want caller deadline", shutdownErr)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("shutdown exceeded caller bound: %s", elapsed)
	}
	if err := foundation.Ready(context.Background()); err == nil {
		t.Fatal("foundation reported ready while bounded shutdown was in progress")
	}
	if err := foundation.Start(context.Background()); err == nil {
		t.Fatal("foundation restarted while bounded shutdown was in progress")
	}
	sharedContext, sharedCancel := context.WithTimeout(context.Background(), time.Second)
	defer sharedCancel()
	if err := foundation.Shutdown(sharedContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared shutdown returned %v, want internal stop deadline", err)
	}
	foundation.mu.Lock()
	closed := foundation.state == foundationClosed
	foundation.mu.Unlock()
	if !closed {
		t.Fatal("foundation did not close after its internal stop deadline")
	}

	close(spool.release)
	select {
	case <-spool.done:
	case <-time.After(time.Second):
		t.Fatal("non-cooperative startup fixture did not exit after release")
	}
	select {
	case err := <-startResult:
		if err == nil {
			t.Fatal("late startup completion resurrected the foundation")
		}
	case <-time.After(time.Second):
		t.Fatal("startup did not return after fixture release")
	}
	finalContext, finalCancel := context.WithTimeout(context.Background(), time.Second)
	defer finalCancel()
	if err := foundation.Shutdown(finalContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("closed foundation lost shared shutdown result: %v", err)
	}
	if err := foundation.Ready(context.Background()); err == nil {
		t.Fatal("foundation became ready after late startup completion")
	}
}

func TestConcurrentFoundationShutdownCallersShareResult(t *testing.T) {
	wantErr := errors.New("supervised process failed to stop")
	supervisor := &blockingSupervisor{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		result:  wantErr,
	}
	foundation := foundationWithDependencies(
		t,
		&readySpool{},
		readyUpload{},
		supervisor,
	)
	if err := foundation.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- foundation.Shutdown(context.Background()) }()
	<-supervisor.entered
	waiting := make(chan struct{})
	secondContext := &observedContext{
		Context:  context.Background(),
		observed: waiting,
	}
	go func() { second <- foundation.Shutdown(secondContext) }()
	<-waiting
	close(supervisor.release)

	firstErr := <-first
	secondErr := <-second
	if !errors.Is(firstErr, wantErr) || !errors.Is(secondErr, wantErr) {
		t.Fatalf("shutdown results diverged: first=%v second=%v", firstErr, secondErr)
	}
	if calls := supervisor.calls(); calls != 1 {
		t.Fatalf("supervisor shutdown calls=%d, want 1", calls)
	}
}

func TestStartWaiterKeepsFailedOperationResultAcrossSuccessfulRetry(t *testing.T) {
	firstErr := errors.New("first startup failed")
	spool := &retrySpool{
		firstEntered: make(chan struct{}),
		firstRelease: make(chan struct{}),
		firstResult:  firstErr,
	}
	foundation := foundationWithDependencies(
		t,
		spool,
		readyUpload{},
		&blockingSupervisor{},
	)

	first := make(chan error, 1)
	go func() { first <- foundation.Start(context.Background()) }()
	<-spool.firstEntered

	foundation.mu.Lock()
	oldOperation := foundation.startOperation
	foundation.mu.Unlock()
	allowOldWaiter := make(chan struct{})
	oldWaiter := make(chan error, 1)
	go func() {
		<-allowOldWaiter
		oldWaiter <- foundation.waitForStart(context.Background(), oldOperation)
	}()

	close(spool.firstRelease)
	if err := <-first; !errors.Is(err, firstErr) {
		t.Fatalf("first startup returned %v", err)
	}
	if err := foundation.Start(context.Background()); err != nil {
		t.Fatalf("retry startup failed: %v", err)
	}
	close(allowOldWaiter)
	if err := <-oldWaiter; !errors.Is(err, firstErr) {
		t.Fatalf("old waiter observed retry result instead of first error: %v", err)
	}
}

func testFoundation(t *testing.T) *ProductionFoundation {
	t.Helper()
	apiURL, err := url.Parse("http://api:3000/internal/v1")
	if err != nil {
		t.Fatal(err)
	}
	foundation, err := NewProductionFoundation(Config{
		BindHost:             "127.0.0.1",
		Port:                 8090,
		WorkerID:             "capture-1",
		MaxSlots:             1,
		StartTimeout:         30 * time.Second,
		StopTimeout:          5 * time.Second,
		RecoveryTimeout:      30 * time.Second,
		StorageRoot:          t.TempDir(),
		RecordingAPIURL:      apiURL,
		CaptureServiceSecret: "capture-service-secret-at-least-32-bytes",
		ControlJWTSecret:     "control-service-secret-at-least-32-bytes",
		MaxInFlightPUT:       300 * time.Second,
		FFmpegPath:           "ffmpeg",
		FFmpegTimeout:        120 * time.Second,
		MaxSourceBytes:       4_000_000_000,
		MaxDuration:          4 * time.Hour,
		LogLevel:             "info",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = foundation.Shutdown(ctx)
	})
	return foundation
}

func foundationWithDependencies(
	t *testing.T,
	spool SegmentSpool,
	upload UploadClient,
	supervisor ProcessSupervisor,
) *ProductionFoundation {
	t.Helper()
	foundation := testFoundation(t)
	foundation.spool = spool
	foundation.upload = upload
	foundation.supervisor = supervisor
	return foundation
}

type blockingSpool struct {
	entered chan struct{}
	once    sync.Once
}

type nonCooperativeSpool struct {
	entered chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (spool *nonCooperativeSpool) Prepare(context.Context) error {
	close(spool.entered)
	<-spool.release
	close(spool.done)
	return nil
}

func (spool *nonCooperativeSpool) Close() error {
	return nil
}

func (spool *blockingSpool) Prepare(ctx context.Context) error {
	spool.once.Do(func() { close(spool.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (spool *blockingSpool) Close() error {
	return nil
}

type readySpool struct{}

func (spool *readySpool) Prepare(context.Context) error {
	return nil
}

func (spool *readySpool) Close() error {
	return nil
}

type readyUpload struct{}

func (readyUpload) Ready(context.Context) error {
	return nil
}

type retrySpool struct {
	mu           sync.Mutex
	calls        int
	firstEntered chan struct{}
	firstRelease chan struct{}
	firstResult  error
}

func (spool *retrySpool) Prepare(context.Context) error {
	spool.mu.Lock()
	spool.calls++
	call := spool.calls
	spool.mu.Unlock()
	if call != 1 {
		return nil
	}
	close(spool.firstEntered)
	<-spool.firstRelease
	return spool.firstResult
}

func (spool *retrySpool) Close() error {
	return nil
}

type blockingSupervisor struct {
	mu       sync.Mutex
	entered  chan struct{}
	release  chan struct{}
	result   error
	shutdown int
	once     sync.Once
}

func (supervisor *blockingSupervisor) Ready() error {
	return nil
}

func (supervisor *blockingSupervisor) Shutdown(ctx context.Context) error {
	supervisor.mu.Lock()
	supervisor.shutdown++
	supervisor.mu.Unlock()
	if supervisor.entered == nil {
		return supervisor.result
	}
	supervisor.once.Do(func() { close(supervisor.entered) })
	select {
	case <-supervisor.release:
		return supervisor.result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (supervisor *blockingSupervisor) calls() int {
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	return supervisor.shutdown
}

type observedContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (ctx *observedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.observed) })
	return ctx.Context.Done()
}
