FROM golang:1.25 AS builder
ARG TARGETARCH

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# Download the LadybugDB shared library matching the target arch.
# Pin to v0.13.1 — must match the go-ladybug module version in go.mod
# or CGo will link against an ABI it doesn't understand at runtime.
# Use `cp -a` to preserve symlinks (liblbug.so → liblbug.so.0).
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) LBUG_ARCH="linux-x86_64";   GO_ARCH="linux-amd64" ;; \
      arm64) LBUG_ARCH="linux-aarch64";  GO_ARCH="linux-arm64" ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 1 ;; \
    esac; \
    LBUG_MOD=$(go env GOMODCACHE)/github.com/\!ladybug\!d\!b/go-ladybug@v0.13.1; \
    mkdir -p "$LBUG_MOD/lib/dynamic/$GO_ARCH" /tmp/lbug; \
    curl -fsSL "https://github.com/LadybugDB/ladybug/releases/download/v0.13.1/liblbug-${LBUG_ARCH}.tar.gz" | tar xz -C /tmp/lbug; \
    cp -a /tmp/lbug/liblbug.so* "$LBUG_MOD/lib/dynamic/$GO_ARCH/"; \
    cp -a /tmp/lbug/liblbug.so* /usr/local/lib/; \
    ldconfig; \
    ls -la "$LBUG_MOD/lib/dynamic/$GO_ARCH/"

COPY . .
RUN CGO_ENABLED=1 go build -o /loveliness ./cmd/loveliness
RUN CGO_ENABLED=1 go build -o /loveliness-benchmark ./cmd/benchmark

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy LadybugDB shared library from builder. The builder stage stages
# the arch-correct .so under /usr/local/lib so this COPY works for any
# target arch without hardcoding linux-amd64 / linux-arm64.
COPY --from=builder /usr/local/lib/liblbug.so* /usr/local/lib/
RUN ldconfig

COPY --from=builder /loveliness /usr/local/bin/loveliness
COPY --from=builder /loveliness-benchmark /usr/local/bin/loveliness-benchmark

# Copy start.sh for Fly.io deployment
COPY deploy/fly/start.sh /start.sh
RUN chmod +x /start.sh

RUN useradd -r -s /bin/false loveliness && mkdir -p /data && chown loveliness:loveliness /data
USER loveliness

EXPOSE 8080 7687 9000 9001
ENTRYPOINT ["/start.sh"]
