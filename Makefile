.PHONY: build test run clean docker mcp install-mcp

BINARY := loveliness
MCP_BINARY := loveliness-mcp
PKG := ./cmd/loveliness
MCP_PKG := ./cmd/loveliness-mcp

build:
	CGO_ENABLED=1 go build -o $(BINARY) $(PKG)
	CGO_ENABLED=0 go build -o $(MCP_BINARY) $(MCP_PKG)

# mcp builds just the MCP server binary. Useful for quick iteration
# when working on pkg/mcp without touching the main server.
mcp:
	CGO_ENABLED=0 go build -o $(MCP_BINARY) $(MCP_PKG)

# install-mcp builds loveliness-mcp into $GOBIN, registers it with the
# `claude` CLI if present, and links the SKILL into ~/.claude/skills/.
# Pass FLAGS=... to forward args to the installer (e.g. --no-skill).
install-mcp:
	./scripts/install-mcp.sh $(FLAGS)

test:
	go test ./pkg/... -v -count=1

test-short:
	go test ./pkg/... -count=1

race:
	go test ./pkg/... -v -race -count=1

clean:
	rm -f $(BINARY) $(MCP_BINARY)
	rm -rf data/

run: build
	LOVELINESS_BOOTSTRAP=true ./$(BINARY)

docker:
	docker compose up --build

docker-down:
	docker compose down -v

generate: build-generate
	./generate -nodes 100000 -edge-ratio 1.0 -batch 50000

build-generate:
	CGO_ENABLED=0 go build -o generate ./cmd/generate

build-benchmark:
	CGO_ENABLED=0 go build -o benchmark ./cmd/benchmark

bench: build-benchmark
	./benchmark -nodes 50000 -edges 50000 -iters 200

lint:
	golangci-lint run ./...

cover:
	go test ./pkg/... -coverprofile=coverage.txt -covermode=atomic
	go tool cover -func=coverage.txt
