package signaling

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrAuthoritativeStop = errors.New("authoritative recording stop")

const trackArrivalTimeout = 10 * time.Second

// PeerSession is the receive-only WebRTC lifecycle controlled by signaling.
type PeerSession interface {
	ReconcileConsumers(
		ctx context.Context,
		transport TransportOptions,
		consumers []Consumer,
	) (string, DTLSParameters, error)
	RequestKeyFrame(ctx context.Context, ssrc uint32) error
	Close() error
}

// ProbeController binds ordered v1 lifecycle events to server consumers and a
// dynamically renegotiated Pion receive session.
type ProbeController struct {
	signaling          SignalingClient
	peer               PeerSession
	transport          TransportOptions
	rtpCapabilities    json.RawMessage
	reconciler         *Reconciler
	consumers          map[string]Consumer
	consumerOrder      []string
	connected          bool
	mu                 sync.Mutex
	terminal           bool
	authoritativeStop  bool
	observer           LifecycleObserver
	readinessFailures  chan readinessFailure
	readinessCancels   map[string]context.CancelFunc
	readinessEpochs    map[string]uint64
	nextReadinessEpoch uint64
	lifecycleDone      chan struct{}
	lifecycleDoneOnce  sync.Once
}

type readinessFailure struct {
	producerID string
	epoch      uint64
	err        error
}

type ProbeState struct {
	Participants []Participant
	Producers    []Producer
}

type LifecycleObserver interface {
	Snapshot(context.Context, Snapshot) error
	Event(context.Context, Event) error
}

type trackReadiness interface {
	ResetTrack(producerID string)
	WaitForTrack(ctx context.Context, producerID string) error
}

// StartProbe establishes the authoritative snapshot and initial consumers.
func StartProbe(
	ctx context.Context,
	signaling SignalingClient,
	peer PeerSession,
) (*ProbeController, error) {
	return StartProbeWithObserver(ctx, signaling, peer, nil)
}

func StartProbeWithObserver(
	ctx context.Context,
	signaling SignalingClient,
	peer PeerSession,
	observer LifecycleObserver,
) (*ProbeController, error) {
	rtpCapabilities, err := signaling.RouterRTPCapabilities(ctx)
	if err != nil {
		return nil, failStart(signaling, peer, fmt.Errorf("get router RTP capabilities: %w", err))
	}
	if !json.Valid(rtpCapabilities) {
		return nil, failStart(signaling, peer, errors.New("router RTP capabilities are invalid JSON"))
	}
	transport, err := signaling.CreateRecvTransport(ctx)
	if err != nil {
		return nil, failStart(signaling, peer, fmt.Errorf("create receive transport: %w", err))
	}
	snapshot, err := signaling.SyncSnapshot(ctx)
	if err != nil {
		return nil, failStart(signaling, peer, fmt.Errorf("sync producer snapshot: %w", err))
	}
	reconciler, err := NewReconciler(snapshot)
	if err != nil {
		return nil, failStart(signaling, peer, fmt.Errorf("validate producer snapshot: %w", err))
	}

	controller := &ProbeController{
		signaling:         signaling,
		peer:              peer,
		transport:         transport,
		rtpCapabilities:   rtpCapabilities,
		reconciler:        reconciler,
		consumers:         map[string]Consumer{},
		consumerOrder:     []string{},
		observer:          observer,
		readinessFailures: make(chan readinessFailure, 1),
		readinessCancels:  map[string]context.CancelFunc{},
		readinessEpochs:   map[string]uint64{},
		lifecycleDone:     make(chan struct{}),
	}
	if observer != nil {
		if err := observer.Snapshot(ctx, snapshot); err != nil {
			return nil, controller.fail(fmt.Errorf("checkpoint producer snapshot: %w", err))
		}
	}
	for _, producer := range snapshot.Producers {
		if producer.Paused {
			continue
		}
		controller.mu.Lock()
		err := controller.ensureAndResume(ctx, producer)
		controller.mu.Unlock()
		if err != nil {
			return nil, controller.fail(err)
		}
	}
	return controller, nil
}

// State returns a copied point-in-time view of the authoritative roster.
func (controller *ProbeController) State() ProbeState {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return ProbeState{
		Participants: controller.reconciler.participantsSnapshot(),
		Producers:    controller.reconciler.Producers(),
	}
}

// WhileActive runs action only while lifecycle termination has not begun.
// It shares the terminal-state lock with Run so readiness cannot overtake a
// stop or failure that the controller has already accepted.
func (controller *ProbeController) WhileActive(ctx context.Context, action func()) bool {
	if ctx == nil || action == nil {
		return false
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if ctx.Err() != nil || controller.terminal || controller.authoritativeStop {
		return false
	}
	action()
	return true
}

// Run applies lifecycle events until context cancellation or signaling close.
func (controller *ProbeController) Run(ctx context.Context) error {
	defer controller.stopLifecycle()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, open := <-controller.signaling.Lifecycle():
			if !open {
				return errors.New("private signaling lifecycle closed")
			}
			if event.Type == "bot:stop" {
				controller.mu.Lock()
				controller.authoritativeStop = true
				controller.mu.Unlock()
				return ErrAuthoritativeStop
			}
			if err := controller.Apply(ctx, event); err != nil {
				return err
			}
		case failure := <-controller.readinessFailures:
			if controller.acceptReadinessFailure(failure) {
				return failure.err
			}
		}
	}
}

// Apply reconciles one ordered event and performs its consumer-side mutation.
func (controller *ProbeController) Apply(ctx context.Context, event Event) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.terminal {
		return errors.New("probe controller is terminal")
	}
	prior, hadPrior := Producer{}, false
	if event.Type == "bot:producerUpdated" && event.Producer != nil {
		prior, hadPrior = controller.producer(event.Producer.ProducerID)
	}
	applied, err := controller.reconciler.Apply(event)
	if err != nil {
		return controller.failLocked(err)
	}
	if !applied {
		return nil
	}
	if controller.observer != nil {
		if err := controller.observer.Event(ctx, event); err != nil {
			return controller.failLocked(fmt.Errorf("checkpoint lifecycle event: %w", err))
		}
	}

	switch event.Type {
	case "bot:producerAdded", "bot:producerUpdated":
		if event.Type == "bot:producerUpdated" && hadPrior &&
			producerIdentityChanged(prior, *event.Producer) {
			if err := controller.closeConsumer(ctx, event.Producer.ProducerID); err != nil {
				return err
			}
		}
		if event.Producer.Paused {
			return controller.pause(ctx, event.Producer.ProducerID)
		}
		return controller.ensureAndResume(ctx, *event.Producer)
	case "bot:producerPaused":
		return controller.pause(ctx, event.ProducerID)
	case "bot:producerResumed":
		producer, exists := controller.producer(event.ProducerID)
		if !exists {
			return controller.failLocked(errors.New("resumed producer is missing"))
		}
		return controller.ensureAndResume(ctx, producer)
	case "bot:producerClosed":
		return controller.closeConsumer(ctx, event.ProducerID)
	case "bot:participantAdded", "bot:participantUpdated", "bot:participantLeft":
		return nil
	default:
		return controller.failLocked(fmt.Errorf("unsupported lifecycle event %q", event.Type))
	}
}

// Close rolls back every server consumer and releases the Pion/signaling peers.
func (controller *ProbeController) Close() error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.failLocked(nil)
}

func (controller *ProbeController) ensureAndResume(
	ctx context.Context,
	producer Producer,
) error {
	consumer, exists := controller.consumers[producer.ProducerID]
	if !exists {
		created, err := controller.signaling.Consume(
			ctx,
			controller.transport.ID,
			producer.ProducerID,
			controller.rtpCapabilities,
		)
		if err != nil {
			return controller.failLocked(fmt.Errorf("consume producer: %w", err))
		}
		created.Source = producer.Source
		if err := validateConsumer(producer, created); err != nil {
			_ = controller.signaling.CloseConsumer(ctx, producer.ProducerID)
			return controller.failLocked(err)
		}
		controller.consumers[producer.ProducerID] = created
		controller.consumerOrder = append(controller.consumerOrder, producer.ProducerID)
		consumer = created
		if err := controller.renegotiate(ctx); err != nil {
			_ = controller.signaling.CloseConsumer(ctx, producer.ProducerID)
			controller.removeConsumerLocked(producer.ProducerID)
			return controller.failLocked(err)
		}
	}
	if err := controller.signaling.ResumeConsumer(ctx, producer.ProducerID); err != nil {
		return controller.failLocked(fmt.Errorf("resume consumer: %w", err))
	}
	controller.watchTrackReadinessLocked(ctx, producer.ProducerID)
	if producer.Kind == KindVideo {
		ssrc := consumer.RTPParameters.Encodings[0].SSRC
		keyFrameContext, cancel := context.WithTimeout(ctx, trackArrivalTimeout)
		defer cancel()
		if err := controller.peer.RequestKeyFrame(keyFrameContext, ssrc); err != nil {
			return controller.failLocked(fmt.Errorf("request key frame: %w", err))
		}
	}
	return nil
}

func (controller *ProbeController) pause(ctx context.Context, producerID string) error {
	controller.cancelTrackReadinessLocked(producerID)
	if _, exists := controller.consumers[producerID]; !exists {
		return nil
	}
	if err := controller.signaling.PauseConsumer(ctx, producerID); err != nil {
		return controller.failLocked(fmt.Errorf("pause consumer: %w", err))
	}
	return nil
}

func (controller *ProbeController) closeConsumer(
	ctx context.Context,
	producerID string,
) error {
	controller.cancelTrackReadinessLocked(producerID)
	if _, exists := controller.consumers[producerID]; !exists {
		return nil
	}
	if err := controller.signaling.CloseConsumer(ctx, producerID); err != nil {
		return controller.failLocked(fmt.Errorf("close consumer: %w", err))
	}
	if readiness, ok := controller.peer.(trackReadiness); ok {
		readiness.ResetTrack(producerID)
	}
	controller.removeConsumerLocked(producerID)
	if err := controller.renegotiate(ctx); err != nil {
		return controller.failLocked(err)
	}
	return nil
}

func (controller *ProbeController) watchTrackReadinessLocked(ctx context.Context, producerID string) {
	readiness, ok := controller.peer.(trackReadiness)
	if !ok {
		return
	}
	controller.cancelTrackReadinessLocked(producerID)
	controller.nextReadinessEpoch++
	epoch := controller.nextReadinessEpoch
	trackContext, cancel := context.WithTimeout(ctx, trackArrivalTimeout)
	controller.readinessCancels[producerID] = cancel
	controller.readinessEpochs[producerID] = epoch
	go func() {
		err := readiness.WaitForTrack(trackContext, producerID)
		cancel()
		if err == nil || errors.Is(err, context.Canceled) {
			controller.clearTrackReadiness(producerID, epoch)
			return
		}
		failure := readinessFailure{
			producerID: producerID,
			epoch:      epoch,
			err:        fmt.Errorf("wait for producer media: %w", err),
		}
		select {
		case controller.readinessFailures <- failure:
		case <-ctx.Done():
			controller.clearTrackReadiness(producerID, epoch)
		case <-controller.lifecycleDone:
			controller.clearTrackReadiness(producerID, epoch)
		}
	}()
}

func (controller *ProbeController) cancelTrackReadinessLocked(producerID string) {
	if cancel := controller.readinessCancels[producerID]; cancel != nil {
		cancel()
	}
	delete(controller.readinessCancels, producerID)
	delete(controller.readinessEpochs, producerID)
}

func (controller *ProbeController) clearTrackReadiness(producerID string, epoch uint64) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.readinessEpochs[producerID] != epoch {
		return
	}
	delete(controller.readinessCancels, producerID)
	delete(controller.readinessEpochs, producerID)
}

func (controller *ProbeController) acceptReadinessFailure(failure readinessFailure) bool {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.terminal || controller.readinessEpochs[failure.producerID] != failure.epoch {
		return false
	}
	producer, exists := controller.producer(failure.producerID)
	if !exists || producer.Paused {
		controller.cancelTrackReadinessLocked(failure.producerID)
		return false
	}
	controller.cancelTrackReadinessLocked(failure.producerID)
	return true
}

func (controller *ProbeController) removeConsumerLocked(producerID string) {
	delete(controller.consumers, producerID)
	kept := controller.consumerOrder[:0]
	for _, current := range controller.consumerOrder {
		if current != producerID {
			kept = append(kept, current)
		}
	}
	controller.consumerOrder = kept
}

func (controller *ProbeController) renegotiate(ctx context.Context) error {
	_, dtlsParameters, err := controller.peer.ReconcileConsumers(
		ctx,
		controller.transport,
		controller.orderedConsumers(),
	)
	if err != nil {
		return fmt.Errorf("negotiate Pion receive transport: %w", err)
	}
	if controller.connected {
		return nil
	}
	if err := controller.signaling.ConnectRecvTransport(
		ctx,
		controller.transport.ID,
		dtlsParameters,
	); err != nil {
		return fmt.Errorf("connect receive transport: %w", err)
	}
	controller.connected = true
	return nil
}

func (controller *ProbeController) orderedConsumers() []Consumer {
	result := make([]Consumer, 0, len(controller.consumers))
	for _, producerID := range controller.consumerOrder {
		consumer, exists := controller.consumers[producerID]
		if exists {
			result = append(result, consumer)
		}
	}
	return result
}

func (controller *ProbeController) producer(producerID string) (Producer, bool) {
	for _, producer := range controller.reconciler.Producers() {
		if producer.ProducerID == producerID {
			return producer, true
		}
	}
	return Producer{}, false
}

func (controller *ProbeController) fail(cause error) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.failLocked(cause)
}

func (controller *ProbeController) failLocked(cause error) error {
	if controller.terminal {
		return cause
	}
	controller.terminal = true
	controller.lifecycleDoneOnce.Do(func() { close(controller.lifecycleDone) })
	for producerID := range controller.readinessCancels {
		controller.cancelTrackReadinessLocked(producerID)
	}
	cleanupContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errorsFound := []error{}
	if cause != nil {
		errorsFound = append(errorsFound, cause)
	}
	for producerID := range controller.consumers {
		if err := controller.signaling.CloseConsumer(cleanupContext, producerID); err != nil {
			if !controller.authoritativeStop {
				errorsFound = append(errorsFound, fmt.Errorf("rollback consumer: %w", err))
			}
		}
	}
	if err := controller.peer.Close(); err != nil {
		errorsFound = append(errorsFound, fmt.Errorf("close Pion peer: %w", err))
	}
	if err := controller.signaling.Close(); err != nil {
		errorsFound = append(errorsFound, fmt.Errorf("close signaling: %w", err))
	}
	return errors.Join(errorsFound...)
}

func (controller *ProbeController) stopLifecycle() {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	controller.lifecycleDoneOnce.Do(func() { close(controller.lifecycleDone) })
	for producerID := range controller.readinessCancels {
		controller.cancelTrackReadinessLocked(producerID)
	}
}

func producerIdentityChanged(previous, next Producer) bool {
	return previous.ParticipantID != next.ParticipantID ||
		previous.Source != next.Source ||
		previous.Kind != next.Kind
}

func failStart(signaling SignalingClient, peer PeerSession, cause error) error {
	errorsFound := []error{cause}
	if err := peer.Close(); err != nil {
		errorsFound = append(errorsFound, err)
	}
	if err := signaling.Close(); err != nil {
		errorsFound = append(errorsFound, err)
	}
	return errors.Join(errorsFound...)
}

func validateConsumer(producer Producer, consumer Consumer) error {
	if consumer.ProducerID != producer.ProducerID {
		return errors.New("consumer producerId does not match requested producer")
	}
	if consumer.Kind != producer.Kind {
		return errors.New("consumer kind does not match producer")
	}
	primary, err := primaryCodec(consumer.RTPParameters.Codecs)
	if err != nil {
		return err
	}
	if len(consumer.RTPParameters.Encodings) != 1 {
		return errors.New("consumer must have exactly one RTP encoding")
	}
	if consumer.RTPParameters.MID == "" {
		return errors.New("consumer stable MID is required")
	}
	return ValidateCodec(producer.Kind, producer.Source, primary.MimeType)
}
