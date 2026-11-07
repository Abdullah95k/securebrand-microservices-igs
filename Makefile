SHELL := /bin/bash
GO ?= go
IMAGE ?= igs:dev
PG_DSN ?= postgres://fbs:fbs@localhost:5432/fbs?sslmode=disable
TEST_ENV = IGS_TEST_PG_DSN="$(PG_DSN)" IGS_TEST_REDIS_ADDR=localhost:6379 IGS_TEST_KAFKA_BROKERS=localhost:19092 IGS_TEST_SCHEMA_REGISTRY=http://localhost:18081
COMPOSE = docker compose -f deploy/compose/docker-compose.yml

.PHONY: all build test lint vet generate golden infra-up infra-down test-integration e2e loadtest soak igs-loadtest igs-soak docker docker-prebuilt compose-up compose-down igs-compose-up igs-compose-down igs-phase0 igs-phase0-dryrun igs-probe igs-probe-dryrun igs-tagged igs-tagged-dryrun igs-spike-bin chart-check igs-chart-check chart-smoke igs-chart-smoke clean

all: generate build test

build:
	$(GO) build ./...

## Unit tests (no infrastructure). Integration tests skip themselves.
test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

lint: vet
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; ran go vet only"
	@command -v buf >/dev/null && buf lint || echo "buf not installed; skipped proto lint"

## Regenerate protobuf code (requires buf + protoc-gen-go).
generate:
	buf generate

## Regenerate the parsers' golden files after an intended parser change.
golden:
	$(GO) test ./internal/ig/parse -update

## Start Postgres, Redpanda and Valkey for integration tests.
infra-up:
	$(COMPOSE) up -d postgres redpanda valkey
	@until docker exec igs-postgres-1 pg_isready -U fbs >/dev/null 2>&1; do sleep 1; done

infra-down:
	$(COMPOSE) down -v

## Integration tests against real Postgres/Redpanda/Valkey (make infra-up first).
test-integration:
	$(TEST_ENV) $(GO) test -count=1 ./internal/platform/kafka/... ./internal/platform/leader/... ./internal/platform/schemareg/... ./internal/store/... ./internal/testenv/... ./internal/ig/... ./cmd/igsctl/...

## igs's load tests against fakeig (opt-in; prints a report). Size with
## IGS_LOADTEST_PROFILES, _POSTS, _COMMENTS, _ROWS; IGS_LOADTEST_OUT appends
## the report to a file.
loadtest igs-loadtest:
	$(TEST_ENV) IGS_LOADTEST=1 $(GO) test -count=1 -v -timeout 45m -run 'TestLoad' ./internal/ig/e2e/

## igs's soak: the scheduled pipeline for IGS_SOAK_DURATION (20m) under a
## changing fakeig world and component restarts; ends checked against
## fakeig's ground truth.
soak igs-soak:
	$(TEST_ENV) IGS_SOAK=1 $(GO) test -count=1 -v -timeout 6h -run 'TestSoak' ./internal/ig/e2e/

## igs's end-to-end suite against fakeig, with ground truth.
e2e:
	$(TEST_ENV) $(GO) test -count=1 -v ./internal/ig/e2e/...

docker:
	docker build -f deploy/docker/Dockerfile -t $(IMAGE) .

## Build binaries on the host and package them (no Go builder image needed).
docker-prebuilt:
	CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -ldflags="-s -w" -o bin/ ./cmd/...
	docker build -f deploy/docker/Dockerfile.prebuilt -t $(IMAGE) .

HELM ?= helm
KUBECONFORM ?= kubeconform
KUBECONFORM_ARGS = -strict -summary -kubernetes-version 1.30.0 -schema-location default \
	-schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

## Lint the igs Helm chart and validate its renders (defaults, every
## optional object, the fakeig rehearsal values) against the Kubernetes and
## CRD schemas.
chart-check: igs-chart-check

igs-chart-check:
	$(HELM) lint deploy/helm/igs
	$(HELM) lint deploy/helm/igs -f deploy/helm/igs/examples/fakeig.yaml
	set -o pipefail; $(HELM) template igs deploy/helm/igs | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	set -o pipefail; $(HELM) template igs deploy/helm/igs --set keda.authSecret=igs-keda-kafka,tlsFiles.existingSecret=igs-tls,alerts.enabled=true,dashboards.enabled=true,serviceMonitor.enabled=true,api.ingress.enabled=true,networkPolicy.enabled=true,hooks.schemas=true \
	  | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	set -o pipefail; $(HELM) template igs deploy/helm/igs -f deploy/helm/igs/examples/fakeig.yaml | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	$(HELM) lint deploy/helm/igs -f deploy/helm/igs/examples/production.yaml
	set -o pipefail; $(HELM) template igs deploy/helm/igs -f deploy/helm/igs/examples/production.yaml | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	$(HELM) lint deploy/helm/igs -f deploy/helm/igs/values-production.yaml
	set -o pipefail; $(HELM) template igs deploy/helm/igs -n igs -f deploy/helm/igs/values-production.yaml | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	$(HELM) lint deploy/helm/igs -f deploy/soak/values-soak.yaml
	set -o pipefail; $(HELM) template igs deploy/helm/igs -n igs-soak -f deploy/soak/values-soak.yaml | $(KUBECONFORM) $(KUBECONFORM_ARGS)
	$(KUBECONFORM) $(KUBECONFORM_ARGS) deploy/soak/stores.yaml deploy/soak/fakeig.yaml deploy/soak/driver.yaml
	python3 scripts/soak-resources.py --helm $(HELM)
	python3 scripts/chart-secrets.py --helm $(HELM)

## Run the chart's hooks and Deployments in Docker exactly as rendered
## (igsctl hooks, every Deployment), then three registered profiles'
## backfills against fakeig (make infra-up and make docker first).
chart-smoke igs-chart-smoke:
	python3 scripts/chart-smoke.py --image $(IMAGE) --helm $(HELM)

## The local stack: infrastructure, fakeig and every igs service
## (deploy/compose/docker-compose.yml); the API answers on localhost:8080.
compose-up igs-compose-up:
	$(COMPOSE) up -d

compose-down igs-compose-down:
	$(COMPOSE) down -v

PHASE0_CONFIG ?= phase0/proxies.yaml
IGS_PHASE0_PROFILES ?= phase0/ig-profiles.txt
IGS_PHASE0_TIERS ?= isp
IGS_PHASE0_OUT ?= phase0/out-ig-phase0

## igs's Phase 0 against real Instagram (docs/runbooks/igs-phase0.md): about
## 150 profiles per tier. Resume with IGS_PHASE0_ARGS="--from run".
igs-phase0:
	@test -f $(PHASE0_CONFIG) || { echo "missing $(PHASE0_CONFIG): cp phase0/proxies.example.yaml $(PHASE0_CONFIG), fill it in, chmod 600"; exit 2; }
	@test -f $(IGS_PHASE0_PROFILES) || { echo "missing $(IGS_PHASE0_PROFILES): one public profile per line"; exit 2; }
	$(GO) run ./cmd/igs-spike phase0 --config $(PHASE0_CONFIG) --profiles $(IGS_PHASE0_PROFILES) --tiers $(IGS_PHASE0_TIERS) --out-dir $(IGS_PHASE0_OUT) $(IGS_PHASE0_ARGS)

## igs's Phase 0 rehearsed against a local fakeig (rotated doc_ids): must say GO.
igs-phase0-dryrun:
	GO=$(GO) phase0/igs-dryrun.sh

IGS_PROBE_PROFILES ?= phase0/ig-profiles.txt
IGS_PROBE_TIERS ?= isp
IGS_PROBE_OUT ?= phase0/out-ig
IGS_PROBE_ARGS ?=

## igs probe against real Instagram (checkpoint C2, docs/runbooks/igs-probe.md).
## One run per tier in IGS_PROBE_TIERS (default isp, the owner's choice; "direct" =
## this machine's own IP);
## each writes $(IGS_PROBE_OUT)/<tier>/ and <tier>.tar.gz.
igs-probe:
	@test -f $(IGS_PROBE_PROFILES) || { echo "missing $(IGS_PROBE_PROFILES): cp phase0/ig-profiles.example.txt $(IGS_PROBE_PROFILES) and fill it in"; exit 2; }
	@for t in $$(echo $(IGS_PROBE_TIERS) | tr , ' '); do \
	  if [ $$t != direct ] && [ ! -f $(PHASE0_CONFIG) ]; then echo "missing $(PHASE0_CONFIG) for tier $$t: cp phase0/proxies.example.yaml $(PHASE0_CONFIG), fill it in, chmod 600"; exit 2; fi; \
	  $(GO) run ./cmd/igs-spike probe --profiles $(IGS_PROBE_PROFILES) $(if $(wildcard $(PHASE0_CONFIG)),--config $(PHASE0_CONFIG)) \
	    --tier $$t --out-dir $(IGS_PROBE_OUT)/$$t $(IGS_PROBE_ARGS) || exit 1; \
	done

## Rehearse the probe against a built-in fake Instagram: no traffic, no proxies.
igs-probe-dryrun:
	$(GO) run ./cmd/igs-spike probe --dry-run --out-dir $(IGS_PROBE_OUT)-dryrun

## The tagged-tab test (docs/runbooks/igs-probe.md, "The tagged tab"): ISP
## tier, the same profiles file; writes phase0/out-tagged/.
IGS_TAGGED_OUT ?= phase0/out-tagged
igs-tagged:
	@test -f $(IGS_PROBE_PROFILES) || { echo "missing $(IGS_PROBE_PROFILES): cp phase0/ig-profiles.example.txt $(IGS_PROBE_PROFILES) and fill it in"; exit 2; }
	$(GO) run ./cmd/igs-spike tagged --profiles $(IGS_PROBE_PROFILES) $(if $(wildcard $(PHASE0_CONFIG)),--config $(PHASE0_CONFIG)) \
	  --tier isp --out-dir $(IGS_TAGGED_OUT) $(IGS_PROBE_ARGS)

igs-tagged-dryrun:
	$(GO) run ./cmd/igs-spike tagged --dry-run --out-dir $(IGS_TAGGED_OUT)-dryrun

## igs probe (checkpoint C2): binaries the owner runs without a Go toolchain.
igs-spike-bin:
	@for t in darwin/arm64 darwin/amd64 linux/amd64 windows/amd64; do \
	  os=$${t%/*}; arch=$${t#*/}; ext=; [ $$os = windows ] && ext=.exe; \
	  echo "bin/igs-spike-$$os-$$arch$$ext"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags="-s -w -X main.version=$$(git describe --always --dirty 2>/dev/null || echo dev)" \
	    -o bin/igs-spike-$$os-$$arch$$ext ./cmd/igs-spike || exit 1; \
	done

clean:
	rm -rf bin/
