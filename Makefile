VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE   ?= nabu-core
LDFLAGS := -s -w -X main.version=$(VERSION)
PI_VERSION ?= $(shell sed -n 's/^PI_VERSION=//p' deploy/versions.env)

.PHONY: build test test-integration lint image

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/nabu ./cmd/nabu

test:
	go test ./...

# Needs Docker (dockertest starts Postgres) or NABU_TEST_DATABASE_URL.
test-integration:
	go test -tags integration -count=1 ./internal/itest/...

lint:
	go vet ./...
	go vet -tags integration ./...

image:
	docker build --build-arg VERSION=$(VERSION) --build-arg PI_VERSION=$(PI_VERSION) -t $(IMAGE):$(VERSION) .
