.PHONY: build test run clean docker

BINARY := loveliness
PKG := ./cmd/loveliness

build:
	CGO_ENABLED=1 go build -o $(BINARY) $(PKG)

test:
	go test ./pkg/... -v -count=1

test-short:
	go test ./pkg/... -count=1

race:
	go test ./pkg/... -v -race -count=1

clean:
	rm -f $(BINARY)
	rm -rf data/

run: build
	LOVELINESS_BOOTSTRAP=true ./$(BINARY)

docker:
	docker compose up --build

docker-down:
	docker compose down -v

generate: build-generate
	./generate -nodes 100000 -edge-ratio 1.0 -workers 64

build-generate:
	CGO_ENABLED=0 go build -o generate ./cmd/generate

lint:
	golangci-lint run ./...

cover:
	go test ./pkg/... -coverprofile=coverage.txt -covermode=atomic
	go tool cover -func=coverage.txt
