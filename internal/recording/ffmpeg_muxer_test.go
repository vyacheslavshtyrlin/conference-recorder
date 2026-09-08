package recording

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type muxRunnerFixture struct {
	arguments []string
	err       error
	output    []byte
}

type blockingMuxRunner struct{ started chan struct{} }

func (runner *blockingMuxRunner) Run(ctx context.Context, _ string, _ []string, _ io.Writer) error {
	close(runner.started)
	<-ctx.Done()
	return ctx.Err()
}

func (fixture *muxRunnerFixture) Run(
	_ context.Context,
	_ string,
	arguments []string,
	stdout io.Writer,
) error {
	fixture.arguments = append([]string{}, arguments...)
	if fixture.err != nil {
		return fixture.err
	}
	output := fixture.output
	if output == nil {
		output = []byte("container")
	}
	_, err := stdout.Write(output)
	return err
}

func TestFFmpegMuxerRejectsOutputBeyondCommandLimit(t *testing.T) {
	fixture := &muxRunnerFixture{output: make([]byte, 33)}
	muxer, err := NewFFmpegMuxer("ffmpeg", time.Second, fixture)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	output := filepath.Join(directory, "output.webm.part")
	err = muxer.Mux(context.Background(), MuxRequest{
		Codec: "vp8", Intermediate: filepath.Join(directory, "input.ivf"),
		Output: output, MaximumOutputBytes: 32,
	})
	if err == nil {
		t.Fatal("oversize mux output was accepted")
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversize mux output survived: %v", err)
	}
}

func TestFFmpegMuxerAllowsExactlyMaximumOutputBytes(t *testing.T) {
	fixture := &muxRunnerFixture{output: make([]byte, 32)}
	muxer, err := NewFFmpegMuxer("ffmpeg", time.Second, fixture)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	output := filepath.Join(directory, "output.webm.part")
	err = muxer.Mux(context.Background(), MuxRequest{
		Codec: "vp8", Intermediate: filepath.Join(directory, "input.ivf"),
		Output: output, MaximumOutputBytes: 32,
	})
	if err != nil {
		t.Fatalf("exact mux output limit was rejected: %v", err)
	}
	info, err := os.Stat(output)
	if err != nil || info.Size() != 32 {
		t.Fatalf("exact bounded output size=%v err=%v", info, err)
	}
}

func TestFFmpegMuxerUsesStreamCopyAndContainerTuple(t *testing.T) {
	fixture := &muxRunnerFixture{}
	muxer, err := NewFFmpegMuxer("ffmpeg", time.Second, fixture)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	input := filepath.Join(directory, "input.ivf")
	output := filepath.Join(directory, "output.webm.part")
	if err := os.WriteFile(input, []byte("ivf"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := MuxRequest{
		Codec: "vp8", Intermediate: input, Output: output, MaximumOutputBytes: 1024,
	}
	if err := muxer.Mux(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-i", input,
		"-map", "0:0", "-c", "copy", "-f", "webm", "pipe:1",
	}
	if len(fixture.arguments) != len(want) {
		t.Fatalf("unexpected arguments: %v", fixture.arguments)
	}
	for index := range want {
		if fixture.arguments[index] != want[index] {
			t.Fatalf("argument %d: got %q want %q", index, fixture.arguments[index], want[index])
		}
	}
}

func TestFFmpegMuxerRemovesPartialOutputOnFailure(t *testing.T) {
	fixture := &muxRunnerFixture{err: errors.New("failed")}
	muxer, err := NewFFmpegMuxer("ffmpeg", time.Second, fixture)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	input := filepath.Join(directory, "input.h264")
	output := filepath.Join(directory, "output.mkv.part")
	if err := os.WriteFile(output, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	request := MuxRequest{
		Codec: "h264", Intermediate: input, Output: output, MaximumOutputBytes: 1024,
	}
	if err := muxer.Mux(context.Background(), request); err == nil {
		t.Fatal("expected mux failure")
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial output remains: %v", err)
	}
}

func TestFFmpegMuxIsCancelledBySharedProcessSupervisor(t *testing.T) {
	supervisor := NewBoundedProcessSupervisor()
	runner := &blockingMuxRunner{started: make(chan struct{})}
	muxer, err := NewSupervisedFFmpegMuxer("ffmpeg", time.Minute, runner, supervisor)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	result := make(chan error, 1)
	go func() {
		result <- muxer.Mux(context.Background(), MuxRequest{
			Codec: "h264", Intermediate: filepath.Join(directory, "input.h264"),
			Output: filepath.Join(directory, "output.mkv.part"), MaximumOutputBytes: 1024,
		})
	}()
	<-runner.started
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("cancelled mux unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("mux did not leave the supervisor registry")
	}
}
