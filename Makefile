MODULE  := github.com/scttfrdmn/globalfs
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
BINDIR  := bin

.PHONY: all build build-coordinator build-cli test lint fmt clean

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

## help: list available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  /'
