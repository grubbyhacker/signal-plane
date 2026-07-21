// github-green-pr-coordinator runs the fixture-only durable polling loop.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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
		fail(errors.New("github green PR coordinator is disabled"))
	}
	token := os.Getenv(cfg.Coordinator.BrokerTokenEnv)
	if token == "" {
		fail(fmt.Errorf("broker token is not set in %s", cfg.Coordinator.BrokerTokenEnv))
	}
	interval, _ := time.ParseDuration(cfg.Coordinator.PollInterval)
	broker, err := agentsession.NewHTTPBroker(cfg.Coordinator.BrokerURL, token, nil)
	if err != nil {
		fail(err)
	}
	store, err := workledger.Open(cfg.Coordinator.DatabasePath)
	if err != nil {
		fail(err)
	}
	defer store.Close()
	registry, err := agentsession.RegisterGitHubGreenPRFixture(store, broker)
	if err != nil {
		fail(err)
	}
	if _, err := store.ActivateRoute(context.Background(), agentsession.GitHubGreenPRFixtureRoute(), registry, time.Now().UTC()); err != nil {
		fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	run(ctx, store, &agentsession.Executor{Store: store, Broker: broker}, interval)
}

func run(ctx context.Context, store *workledger.Store, executor workledger.Executor, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if item, attempt, ok, err := store.Claim(ctx, time.Now().UTC()); err == nil && ok {
			result, executeErr := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
			if executeErr != nil {
				result = workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "coordinator_execute", SanitizedError: "coordinator execution failed"}
			}
			_ = store.Complete(ctx, attempt.ID, result, time.Now().UTC())
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
