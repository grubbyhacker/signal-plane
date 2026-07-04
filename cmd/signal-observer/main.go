package main

import (
	"log/slog"
	"os"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("signal-observer scaffold ready", "version", buildinfo.Version)
}
