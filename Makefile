.PHONY: build clean test lint fmt help

# Binary name
BINARY_NAME=clickhouse-proto-gen
BINARY_PATH=./clickhouse-proto-gen

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOLINT=golangci-lint

# Build variables
VERSION?=dev
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

# Default target
all: build

## help: Show this help message
help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Available targets:"
	@grep -E '^## ' Makefile | sed 's/## /  /'

## build: Build the binary
build:
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_PATH) ./cmd/$(BINARY_NAME)
	@echo "Build complete: $(BINARY_PATH)"

## clean: Clean build artifacts
clean:
	$(GOCLEAN)
	rm -f $(BINARY_PATH)
	rm -rf dist/
	@echo "Cleaned build artifacts"

## test: Run tests
test:
	$(GOTEST) -v -race -cover ./...

## lint: Run linter
lint:
	@if ! which $(GOLINT) > /dev/null; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	$(GOLINT) run --new-from-rev=origin/main

## fmt: Format code
fmt:
	$(GOFMT) -w -s .
	$(GOMOD) tidy

## deps: Download dependencies
deps:
	$(GOMOD) download
	$(GOMOD) tidy

## install-proto-deps: Install Google API proto dependencies for OpenAPI generation
install-proto-deps:
	@echo "📦 Installing Google API proto dependencies..."
	@mkdir -p third_party
	@if [ ! -d "third_party/googleapis" ]; then \
		echo "Cloning googleapis..."; \
		git clone --depth 1 --filter=blob:none --sparse https://github.com/googleapis/googleapis.git third_party/googleapis; \
		cd third_party/googleapis && git sparse-checkout set google/api; \
	else \
		echo "googleapis already installed in third_party/googleapis"; \
	fi
	@echo "✅ Google API proto dependencies installed"

## install: Install the binary to GOPATH/bin
install: build
	$(GOCMD) install ./cmd/$(BINARY_NAME)

## docker-build: Build Docker image
docker-build:
	docker build -t $(BINARY_NAME):$(VERSION) .

## release: Build release binaries for multiple platforms
release:
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64 ./cmd/$(BINARY_NAME)
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-arm64 ./cmd/$(BINARY_NAME)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-amd64 ./cmd/$(BINARY_NAME)
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-arm64 ./cmd/$(BINARY_NAME)
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o dist/$(BINARY_NAME)-windows-amd64.exe ./cmd/$(BINARY_NAME)
	@echo "Release binaries built in dist/"

## run: Run the binary with example config
run: build
	./$(BINARY_NAME) --config config.example.yaml --verbose

## example: Run an example generation
example: build
	./$(BINARY_NAME) \
		--dsn "clickhouse://localhost:9000/default" \
		--tables "system.tables,system.columns" \
		--out ./proto \
		--package system.v1 \
		--verbose

## proto-clean: Clean proto directory
proto-clean:
	@echo "🧹 Cleaning proto directory..."
	rm -rf proto/*

## proto-generate: Generate proto and Go helper files from ClickHouse
proto-generate:
	@if [ ! -f config.yaml ]; then \
		echo "❌ Error: config.yaml not found!"; \
		echo ""; \
		echo "To create a config file, run:"; \
		echo "  cp config.example.yaml config.yaml"; \
		echo ""; \
		echo "Then edit config.yaml with your ClickHouse connection details."; \
		exit 1; \
	fi
	@echo "🔧 Generating proto and Go helper files..."
	$(GOCMD) run cmd/$(BINARY_NAME)/main.go --config config.yaml

## proto-compile: Run protoc to generate pb.go files
proto-compile:
	@echo "📦 Running protoc to generate pb.go files..."
	@if [ ! -d "third_party/googleapis" ]; then \
		echo "⚠️  Google API protos not found. Run 'make install-proto-deps' first."; \
		exit 1; \
	fi
	protoc -I=proto \
		-I=third_party/googleapis \
		--go_out=paths=source_relative:proto \
		--experimental_allow_proto3_optional \
		proto/*.proto

## proto: Full proto generation pipeline (clean, generate, compile)
proto: install-proto-deps proto-clean proto-generate proto-compile
	@echo "✅ Proto generation completed!"
	$(GOCMD) build ./...

## proto-regen: Regenerate protos without cleaning (faster for iterative development)
proto-regen: proto-generate proto-compile
	@echo "♻️  Proto regeneration completed!"