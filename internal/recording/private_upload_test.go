package recording

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingUploadBody struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingUploadBody() *blockingUploadBody {
	return &blockingUploadBody{closed: make(chan struct{})}
}

func (body *blockingUploadBody) Read([]byte) (int, error) {
	<-body.closed
	return 0, errors.New("upload body closed")
}

func (body *blockingUploadBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}

func TestPrivateUploadClientDeadlineClosesSlowBodyAndConnection(t *testing.T) {
	serverCancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		_, _ = io.Copy(io.Discard, request.Body)
		<-request.Context().Done()
		close(serverCancelled)
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewPrivateUploadClient(target, strings.Repeat("s", 32), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body := newBlockingUploadBody()
	started := time.Now()
	_, err = client.PutPresigned(
		context.Background(),
		target,
		http.Header{"Content-Length": []string{"1"}},
		body,
	)
	if err == nil {
		t.Fatal("slow upload body completed without deadline error")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("whole-request deadline exceeded its bound: %s", elapsed)
	}
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("upload body was not closed at deadline")
	}
	select {
	case <-serverCancelled:
	case <-time.After(time.Second):
		t.Fatal("HTTP connection was not cancelled at deadline")
	}
}

func TestPrivateUploadClientDeadlineCoversResponseReadAndClose(t *testing.T) {
	serverCancelled := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter,
		request *http.Request,
	) {
		_, _ = io.Copy(io.Discard, request.Body)
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("partial"))
		writer.(http.Flusher).Flush()
		<-request.Context().Done()
		close(serverCancelled)
	}))
	defer server.Close()

	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewPrivateUploadClient(target, strings.Repeat("s", 32), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	status, err := client.PutPresigned(
		context.Background(),
		target,
		http.Header{"Content-Type": []string{"video/webm"}, "Content-Length": []string{"7"}},
		io.NopCloser(strings.NewReader("segment")),
	)
	if err == nil || status != http.StatusOK {
		t.Fatalf("slow response result status=%d err=%v", status, err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("response deadline exceeded its bound: %s", elapsed)
	}
	select {
	case <-serverCancelled:
	case <-time.After(time.Second):
		t.Fatal("response connection was not cancelled at deadline")
	}
}
