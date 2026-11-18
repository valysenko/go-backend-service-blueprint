package shared

import (
	"log/slog"
	"os"
)

func NewLogger() *slog.Logger {
	jsonHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(requestIDHandler{Handler: jsonHandler})
	slog.SetDefault(logger)

	return logger
}
