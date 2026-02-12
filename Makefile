BINARY_NAME=dbsafe
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS=-ldflags "-X github.com/nethalo/dbsafe/cmd.Version=$(VERSION) -X github.com/nethalo/dbsafe/cmd.CommitSHA=$(COMMIT) -X github.com/nethalo/dbsafe/cmd.BuildDate=$(BUILD_DATE)"

.PHONY: all build clean test lint install

all: build

build:
	go build $(LDFLAGS) -o $(BINARY_NAME) .

install:
	go install $(LDFLAGS) .

clean:
	rm -f $(BINARY_NAME)
	go clean

test:
	go test ./... -v

lint:
	golangci-lint run ./...

# Cross-compilation
build-all:
	GOOS=linux   GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64 .
	GOOS=linux   GOARCH=arm64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-arm64 .
	GOOS=darwin  GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin  GOARCH=arm64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-arm64 .

# Quick test: parse without connection
demo:
	@echo "=== dbsafe version ==="
	./$(BINARY_NAME) version
	@echo ""
	@echo "=== dbsafe help ==="
	./$(BINARY_NAME) --help

tidy:
	go mod tidy

deps:
	go mod download
