package recording

import (
	"context"
	"errors"
	"sort"

	"conference/recording-bot/internal/signaling"
)

// PionAssignedProbeRunner executes one assigned receive-only probe until stop.
type PionAssignedProbeRunner struct {
	expectedSources map[signaling.Source]struct{}
}

// NewPionAssignedProbeRunner creates a receive-only probe with bounded expectations.
func NewPionAssignedProbeRunner(expectedSources []signaling.Source) (*PionAssignedProbeRunner, error) {
	expected := make(map[signaling.Source]struct{}, len(expectedSources))
	for _, source := range expectedSources {
		if !source.Valid() {
			return nil, errors.New("invalid expected source")
		}
		expected[source] = struct{}{}
	}
	if len(expected) == 0 {
		return nil, errors.New("at least one expected source is required")
	}
	return &PionAssignedProbeRunner{expectedSources: expected}, nil
}

// Run consumes the one-use admission, reports bounded media evidence and then
// remains attached until the authoritative DELETE cancels the context.
func (runner *PionAssignedProbeRunner) Run(
	ctx context.Context,
	request AttemptRequest,
	ready func(),
	report func(ProbeEvidence),
) error {
	signalingClient, err := signaling.DialWebSocketSignalingForAttempt(
		ctx,
		request.SignalingURL,
		request.Admission,
		signaling.SessionIdentity{RoomID: request.RoomID, RecordingID: request.RecordingID},
	)
	request.Admission = ""
	if err != nil {
		return err
	}
	peer, err := signaling.NewPionSession()
	if err != nil {
		_ = signalingClient.Close()
		return err
	}
	controller, err := signaling.StartProbe(ctx, signalingClient, peer)
	if err != nil {
		return err
	}
	defer controller.Close()
	ready()

	lifecycleResult := make(chan error, 1)
	go func() { lifecycleResult <- controller.Run(ctx) }()
	observed := map[signaling.Source]struct{}{}
	codecs := map[string]struct{}{}
	for {
		select {
		case event := <-peer.Tracks():
			if _, expected := runner.expectedSources[event.Source]; expected {
				observed[event.Source] = struct{}{}
			}
			if event.Codec != "" {
				codecs[event.Codec] = struct{}{}
			}
			report(buildProbeEvidence(runner.expectedSources, observed, codecs))
		case peerErr := <-peer.Errors():
			return peerErr
		case lifecycleErr := <-lifecycleResult:
			if ctx.Err() != nil {
				return nil
			}
			return lifecycleErr
		case <-ctx.Done():
			return nil
		}
	}
}

func buildProbeEvidence(
	expected map[signaling.Source]struct{},
	observed map[signaling.Source]struct{},
	codecs map[string]struct{},
) ProbeEvidence {
	sources := make([]signaling.Source, 0, len(observed))
	for source := range observed {
		sources = append(sources, source)
	}
	codecNames := make([]string, 0, len(codecs))
	for codec := range codecs {
		codecNames = append(codecNames, codec)
	}
	sort.Slice(sources, func(left, right int) bool { return sources[left] < sources[right] })
	sort.Strings(codecNames)
	return ProbeEvidence{
		ObservedSources: sources,
		Codecs:          codecNames,
		Complete:        len(observed) == len(expected),
	}
}
