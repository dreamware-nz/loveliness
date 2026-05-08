// Command loveliness-mcp runs a Model Context Protocol server that
// exposes a Loveliness cluster to LLM agents over stdio.
//
// It is equivalent to `loveliness mcp` — the main CLI has a subcommand
// that calls the same pkg/mcp.Server.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/johnjansen/loveliness/pkg/mcp"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("loveliness-mcp", flag.ContinueOnError)

	url := fs.String("url", envOr("LOVELINESS_URL", "http://localhost:8080"),
		"Loveliness HTTP endpoint (env: LOVELINESS_URL)")
	token := fs.String("token", os.Getenv("LOVELINESS_TOKEN"),
		"Bearer token for authenticated clusters (env: LOVELINESS_TOKEN)")
	timeout := fs.Duration("timeout", envDuration("LOVELINESS_TIMEOUT", 30*time.Second),
		"Per-request HTTP timeout (env: LOVELINESS_TIMEOUT)")
	readonly := fs.Bool("readonly", envBool("LOVELINESS_READONLY", false),
		"Register read-only tools only (env: LOVELINESS_READONLY)")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	srv := mcp.New(mcp.Options{
		URL:      *url,
		Token:    *token,
		Timeout:  *timeout,
		Readonly: *readonly,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := srv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "loveliness-mcp: %v\n", err)
		return 1
	}
	return 0
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	return def
}

func envBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	if b, err := strconv.ParseBool(v); err == nil {
		return b
	}
	return def
}
