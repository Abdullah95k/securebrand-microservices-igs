# syntax=docker/dockerfile:1.7
# One image for every igs binary; each Deployment picks its command.
FROM golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOFLAGS=-trimpath GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -ldflags="-s -w -X main.version=${VERSION} -X github.com/abdullah95k/securebrand-microservices-igs/internal/platform/buildinfo.version=${VERSION}" -o /out/ ./cmd/...

# `--target browser`: the same binaries with Chromium, for crawlers and
# igs-registry-sync with ig.browser.enabled (headless-Chromium fallback).
FROM debian:12-slim AS browser
# upgrade: the base image's packages with Debian's security fixes since the
# tag was built (the image scan fails on a fixed HIGH or CRITICAL).
RUN apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get install -y --no-install-recommends chromium ca-certificates fonts-liberation tini \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 65532 --create-home --home-dir /home/nonroot nonroot
COPY --from=build /out/ /usr/local/bin/
ENV IGS_BROWSER_BINARY=/usr/bin/chromium HOME=/tmp
USER 65532:65532
EXPOSE 8080 9090
# tini reaps Chromium's orphans under `docker run`; in Kubernetes the chart
# shares the pod's process namespace instead (its command bypasses this).
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/igs-crawler"]

# The default image: static binaries only.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/igs-api"]
