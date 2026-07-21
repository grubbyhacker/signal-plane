// github-green-pr-fixture-admit durably admits the sole coordinator fixture.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/agentsession"
	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func main() {
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		fail(err)
	}
	if !cfg.Coordinator.Enabled {
		fail(fmt.Errorf("github green PR coordinator is disabled"))
	}
	result, err := admitFixture(context.Background(), cfg, time.Now().UTC())
	if err != nil {
		fail(err)
	}
	fmt.Fprintln(os.Stdout, result.WorkItem.ID)
}

func admitFixture(ctx context.Context, cfg config.Config, now time.Time) (workledger.AdmissionResult, error) {
	store, err := workledger.Open(cfg.Coordinator.DatabasePath)
	if err != nil {
		return workledger.AdmissionResult{}, err
	}
	defer store.Close()
	registry, err := agentsession.RegisterGitHubGreenPRFixture(store, nil)
	if err != nil {
		return workledger.AdmissionResult{}, err
	}
	return agentsession.AdmitGitHubGreenPRFixture(ctx, store, registry, now)
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
