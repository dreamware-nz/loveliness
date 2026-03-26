package logging

import (
	"log/slog"
	"os"
)

// Setup initializes the global structured logger.
func Setup(nodeID string) {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(handler).With("node", nodeID)
	slog.SetDefault(logger)
}
