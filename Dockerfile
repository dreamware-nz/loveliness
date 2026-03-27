FROM golang:1.25-bookworm AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Download LadybugDB shared library into the go module's expected path.
RUN LBUG_MOD=$(go env GOMODCACHE)/github.com/\!ladybug\!d\!b/go-ladybug@v0.13.1 && \
    mkdir -p "$LBUG_MOD/lib/dynamic/linux-amd64" /tmp/lbug && \
    curl -sL https://github.com/LadybugDB/ladybug/releases/latest/download/liblbug-linux-x86_64.tar.gz | tar xz -C /tmp/lbug && \
    (cp /tmp/lbug/lib/* "$LBUG_MOD/lib/dynamic/linux-amd64/" 2>/dev/null || cp /tmp/lbug/*.so "$LBUG_MOD/lib/dynamic/linux-amd64/" 2>/dev/null || true) && \
    ls -la "$LBUG_MOD/lib/dynamic/linux-amd64/"

COPY . .
RUN CGO_ENABLED=1 go build -o /loveliness ./cmd/loveliness

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy LadybugDB shared library from builder.
COPY --from=builder /go/pkg/mod/github.com/!ladybug!d!b/go-ladybug@v0.13.1/lib/dynamic/linux-amd64/liblbug* /usr/local/lib/
RUN ldconfig

COPY --from=builder /loveliness /usr/local/bin/loveliness

EXPOSE 8080 7687 9000 9001
ENTRYPOINT ["loveliness"]
