package agentsession

import (
	"context"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// These values are intentionally not configuration. They identify the sole
// deterministic fixture that may exercise the disabled coordinator.
const (
	GitHubGreenPRFixtureRouteID       = "github-green-pr-fixture-v1"
	GitHubGreenPRFixtureBranchRef     = "agent/fleiglabs-repo-agent/fixture"
	GitHubGreenPRFixtureTaskDigest    = "sha256:33fb8d40a88f14bc2e86652e541422d43e8c8e9a1bd7770a58fb0a9bdab93c77"
	gitHubGreenPRFixtureDeliveryID    = "github-green-pr-fixture-admission-v1"
	gitHubGreenPRFixtureSignalID      = "signal-github-green-pr-fixture-v1"
	gitHubGreenPRFixtureSourceRev     = "9d8c4ed0a4a0ad12d1d4e3870a3c7f9a0c6b5e4f"
	gitHubGreenPRFixtureTransportName = "fixture-github-green-pr-v1"
)

// GitHubGreenPRFixtureRoute is the sole route admitted by the fixture command
// and coordinator. Its task parameters are fixed rather than command inputs.
func GitHubGreenPRFixtureRoute() workledger.RouteDefinition {
	return workledger.RouteDefinition{
		ID:              GitHubGreenPRFixtureRouteID,
		SchemaVersion:   1,
		SemanticVersion: "1.0.0",
		ExecutorID:      ExecutorID,
		Task:            GitHubGreenPRTaskSelection(GitHubGreenPRFixtureBranchRef),
		Admission: workledger.AdmissionPolicy{
			Sources:     []string{"manual"},
			Namespaces:  []string{GitHubGreenPRRepository},
			ObjectKinds: []string{"repository_task"},
			Events:      []string{"repository_change"},
			Actions:     []string{"requested"},
		},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject},
		Retry:       workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}},
	}
}

// RegisterGitHubGreenPRFixture installs exactly the compiled executor and task
// kinds required to activate the fixed fixture route.
func RegisterGitHubGreenPRFixture(store *workledger.Store, broker Broker) (*workledger.Registry, error) {
	registry := workledger.NewRegistry()
	if err := registry.Register(&Executor{Store: store, Broker: broker}); err != nil {
		return nil, err
	}
	if err := registry.RegisterTask(GitHubGreenPRTask{}); err != nil {
		return nil, err
	}
	return registry, nil
}

// AdmitGitHubGreenPRFixture activates the exact compiled route and admits its
// one durable event. Store.Admit supplies idempotence and rejects any existing
// delivery whose persisted identity or content differs.
func AdmitGitHubGreenPRFixture(ctx context.Context, store *workledger.Store, registry *workledger.Registry, now time.Time) (workledger.AdmissionResult, error) {
	snapshot, err := store.ActivateRoute(ctx, GitHubGreenPRFixtureRoute(), registry, now)
	if err != nil {
		return workledger.AdmissionResult{}, err
	}
	return store.Admit(ctx, snapshot.ID, gitHubGreenPRFixtureEvent(now), now)
}

func gitHubGreenPRFixtureEvent(now time.Time) workledger.Event {
	return workledger.Event{
		SignalID:          gitHubGreenPRFixtureSignalID,
		SourceDeliveryID:  gitHubGreenPRFixtureDeliveryID,
		TransportStream:   gitHubGreenPRFixtureTransportName,
		TransportSequence: 1,
		Source:            "manual",
		Namespace:         GitHubGreenPRRepository,
		ObjectKind:        "repository_task",
		ObjectID:          GitHubGreenPRFixtureRouteID,
		EventKind:         "repository_change",
		Action:            "requested",
		ActorClass:        "fixture",
		SourceRevision:    gitHubGreenPRFixtureSourceRev,
		CorrelationID:     GitHubGreenPRFixtureRouteID,
		CausationID:       gitHubGreenPRFixtureDeliveryID,
		PayloadDigest:     GitHubGreenPRFixtureTaskDigest,
		EvidenceRef:       "fixture://github-green-pr-v1/admission",
		ReceivedAt:        now.UTC(),
	}
}
