// Command workitem-shadow-ingress is the process entrypoint for the Stage-5
// shadow ingress. It loads config, builds the shadow stack (shadowadmit +
// routeresolver + shadowingress), and serves the Unix socket until SIGINT or
// SIGTERM. Disabled by default; it holds no launcher, broker, or dispatcher.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/routeresolver"
	"github.com/grubbyhacker/signal-plane/internal/shadowingresscmd"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	logger.Info("starting workitem-shadow-ingress", "version", buildinfo.Version)

	// SIGINT/SIGTERM cancel the context, which stops Serve gracefully.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The deployment-owned mode catalog. The first route set is YouKnowMe
	// curator; a broader catalog is wired here as more AgentTypes land.
	catalog := routeresolver.YouKnowMeCuratorCatalog()

	if err := shadowingresscmd.Run(ctx, cfg.ShadowAdmission, catalog, logger); err != nil {
		logger.Error("shadow ingress exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("shadow ingress stopped")
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
