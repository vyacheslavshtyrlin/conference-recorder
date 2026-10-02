package main

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

func TestDockerfileProductionFoundationIsMinimalAndFailClosed(t *testing.T) {
	content, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := string(content)
	required := []string{
		"FROM golang:1.24-alpine@sha256:8bee1901f1e530bfb4a7850aa7a479d17ae3a18beb6e09064ed54cfd245b7191 AS build",
		"FROM node:22.15.0-alpine3.21 AS production",
		"ARG FFMPEG_PACKAGE_VERSION=6.1.2-r1",
		`"ffmpeg=${FFMPEG_PACKAGE_VERSION}"`,
		"ARG SU_EXEC_PACKAGE_VERSION=0.2-r3",
		"ffmpeg -version",
		"ffprobe -version",
		"adduser -S -D -H -u 65532 -G capture capture",
		`CMD ["/app/recording-bot", "healthcheck"]`,
		`ENTRYPOINT ["recording-worker-entrypoint"]`,
	}
	for _, value := range required {
		if !strings.Contains(dockerfile, value) {
			t.Fatalf("Dockerfile is missing %q", value)
		}
	}
	dependencies := strings.Index(dockerfile, "COPY go.mod go.sum ./")
	source := strings.Index(dockerfile, "COPY cmd ./cmd")
	if dependencies < 0 || source < 0 || dependencies > source {
		t.Fatal("Dockerfile does not cache dependencies before source copy")
	}
	secretLayer := regexp.MustCompile(`(?im)^\s*(ARG|ENV)\s+.*(SECRET|TOKEN|PASSWORD)`)
	if secretLayer.MatchString(dockerfile) {
		t.Fatal("Dockerfile persists a secret-like build argument or environment value")
	}
}

func TestDockerContextExcludesGeneratedCompositionArtifacts(t *testing.T) {
	content, err := os.ReadFile("../../.dockerignore")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"composition/node_modules/", "composition/dist/"} {
		if !strings.Contains(string(content), value) {
			t.Fatalf("Docker context does not exclude %q", value)
		}
	}
}

func TestDockerEntrypointSeparatesRuntimeUsers(t *testing.T) {
	content, err := os.ReadFile("../../docker-entrypoint.sh")
	if err != nil {
		t.Fatal(err)
	}
	entrypoint := string(content)
	for _, value := range []string{`exec su-exec capture "$@"`, `exec su-exec node "$@"`} {
		if !strings.Contains(entrypoint, value) {
			t.Fatalf("entrypoint is missing %q", value)
		}
	}
}
