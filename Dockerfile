FROM golang:1.25-bookworm AS builder

# Install LadybugDB shared library.
RUN curl -fsSL https://install.ladybugdb.com | sh

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -o /loveliness ./cmd/loveliness

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy LadybugDB shared library from builder.
COPY --from=builder /usr/local/lib/liblbug* /usr/local/lib/
RUN ldconfig

COPY --from=builder /loveliness /usr/local/bin/loveliness

EXPOSE 8080 9000 9001
ENTRYPOINT ["loveliness"]
