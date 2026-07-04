package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/grubbyhacker/signal-plane/internal/gateway"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}

	bus, err := eventbus.Connect(cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Subjects)
	if err != nil {
		logger.Error("connect event bus failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()

	addr := normalizeAddr(cfg.Gateway.Addr)
	handler := gateway.New(logger, cfg.Routes, bus).Handler()
	logger.Info("starting signal-gateway", "addr", addr, "version", buildinfo.Version)
	if err := http.ListenAndServe(addr, handler); err != nil {
		logger.Error("signal-gateway exited", "error", err)
		os.Exit(1)
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func normalizeAddr(addr string) string {
	if _, _, err := net.SplitHostPort(addr); err == nil {
		return addr
	}
	return ":" + addr
}
