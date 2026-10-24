VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
IMAGE   ?= nabu-core
LDFLAGS := -s -w -X main.version=$(VERSION)
PI_VERSION ?= $(shell sed -n 's/^PI_VERSION=//p' deploy/versions.env)

.PHONY: build test test-integration lint image sandbox-images sandbox-lock debian-snapshot update-popular

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

# FTR.NAB.CMN-0008: the images of the built-in sandbox profiles and their checks.
sandbox-images:
	docker build --target sandbox-base --build-arg VERSION=$(VERSION) -t nabu-sandbox-base:$(VERSION) .
	docker build --target sandbox-documents --build-arg VERSION=$(VERSION) -t nabu-sandbox-documents:$(VERSION) .
	bash sandbox/test.sh nabu-sandbox-base:$(VERSION) nabu-sandbox-documents:$(VERSION)

# Updates the Python libraries of the profiles (uv.lock with hashes).
sandbox-lock:
	cd sandbox/base && uv lock --upgrade --python 3.13
	cd sandbox/documents && uv lock --upgrade --python 3.13

# The Debian of sandboxes: the latest dated trixie-slim image and the apt archive of its day (FTR.NAB.CMN-0008 R4).
debian-snapshot:
	tag=$$(curl -fsSL "https://hub.docker.com/v2/repositories/library/debian/tags?page_size=100&name=trixie-20" \
	  | python3 -c 'import json,sys; print(sorted(t["name"] for t in json.load(sys.stdin)["results"] if t["name"].endswith("-slim"))[-1])') \
	&& day=$$(echo "$$tag" | sed -E 's/^trixie-([0-9]{8})-slim$$/\1/') \
	&& sed -i -E "s/^ARG DEBIAN_IMAGE=.*/ARG DEBIAN_IMAGE=debian:$$tag/; s/^ARG DEBIAN_SNAPSHOT=.*/ARG DEBIAN_SNAPSHOT=$${day}T000000Z/" Dockerfile \
	&& grep -E '^ARG DEBIAN_(IMAGE|SNAPSHOT)=' Dockerfile

# The lists of popular packages for the «looks like» mark (every quarter).
update-popular:
	curl -fsSL https://hugovk.github.io/top-pypi-packages/top-pypi-packages.min.json \
	  | python3 -c 'import json,sys; print("\n".join(r["project"] for r in json.load(sys.stdin)["rows"][:5000]))' \
	  > internal/pkgsignals/popular/pypi.txt
	curl -fsSL https://popcon.debian.org/by_inst \
	  | awk '$$1 ~ /^[0-9]+$$/ && n < 2000 { print $$2; n++ }' > internal/pkgsignals/popular/debian.txt
