MODULE  := github.com/scttfrdmn/globalfs
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
BINDIR  := bin

.PHONY: all build build-coordinator build-cli test lint fmt clean release-dry-run release

all: build

## build: compile all binaries
build: build-coordinator build-cli

## build-coordinator: compile the globalfs-coordinator daemon
build-coordinator:
	@mkdir -p $(BINDIR)
	go build $(LDFLAGS) -o $(BINDIR)/globalfs-coordinator ./cmd/coordinator

## build-cli: compile the globalfs operator CLI
build-cli:
	@mkdir -p $(BINDIR)
	go build $(LDFLAGS) -o $(BINDIR)/globalfs ./cmd/globalfs

## test: run all tests with the race detector
test:
	go test -race -timeout=120s ./...

## lint: run golangci-lint
lint:
	golangci-lint run ./...

## fmt: gofmt all Go source files
fmt:
	gofmt -w .

## clean: remove compiled binaries
clean:
	rm -rf $(BINDIR)

## release-dry-run: build and package release artifacts without publishing
release-dry-run:
	goreleaser release --snapshot --clean

## release: tag and publish a GitHub release (requires GITHUB_TOKEN)
release:
	goreleaser release --clean

## help: list available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'
