package agentsession

import (
	"context"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// These values are intentionally not configuration. They identify the sole
// deterministic fixture that may exercise the disabled coordinator.
const (
	GitHubGreenPRFixtureRouteID            = "github-green-pr-fixture-v1"
	GitHubGreenPRFixtureBranchRef          = "agent/fleiglabs-repo-agent/fixture"
	GitHubGreenPRFixtureRepositoryID       = "1307218521"
	GitHubGreenPRFixtureRepositoryNodeID   = "R_kgDOTeqSWQ"
	GitHubGreenPRFixtureTaskPath           = "fixture-task.md"
	GitHubGreenPRFixtureSourceRevision     = "b70aada3ef3f2dc08e7eb5ceeee5712957fd4bbd"
	GitHubGreenPRFixtureTaskBlob           = "2de603bfa26af4aa8a33f88876a3f424cadb5da5"
	GitHubGreenPRFixtureTaskEvidenceDigest = "sha256:2308599ce4b16df188920bd4725dbe0d85361e44d83276146ccfa986653ecd6c"
	gitHubGreenPRFixtureDeliveryID         = "github-green-pr-fixture-admission-v1"
	gitHubGreenPRFixtureSignalID           = "signal-github-green-pr-fixture-v1"
	gitHubGreenPRFixtureTransportName      = "fixture-github-green-pr-v1"
	gitHubGreenPRFixtureEvidenceRef        = "github://R_kgDOTeqSWQ/repositories/1307218521/grubbyhacker/repository-worker-lifecycle-test/contents/fixture-task.md?ref=b70aada3ef3f2dc08e7eb5ceeee5712957fd4bbd&git_blob=2de603bfa26af4aa8a33f88876a3f424cadb5da5"
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
		ObjectID:          GitHubGreenPRFixtureRepositoryID,
		EventKind:         "repository_change",
		Action:            "requested",
		ActorClass:        "fixture",
		SourceRevision:    GitHubGreenPRFixtureSourceRevision,
		CorrelationID:     GitHubGreenPRFixtureRouteID,
		CausationID:       gitHubGreenPRFixtureDeliveryID,
		PayloadDigest:     GitHubGreenPRFixtureTaskEvidenceDigest,
		EvidenceRef:       gitHubGreenPRFixtureEvidenceRef,
		ReceivedAt:        now.UTC(),
	}
}
