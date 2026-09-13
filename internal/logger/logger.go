package logger

import (
	"log/slog"
	"os"
)

func New(role string) *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, nil)).With("role", role)
}
