package agentsession

import (
	"context"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func coordinatorFixtureAt(t *testing.T, path string) (*workledger.Store, workledger.WorkItem, workledger.ExecutorAttempt, time.Time) {
	t.Helper()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	store, err := workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reg := workledger.NewRegistry()
	_ = reg.Register(&Executor{})
	_ = reg.RegisterTask(GitHubGreenPRTask{})
	route := workledger.RouteDefinition{ID: "agent-session", SchemaVersion: 1, SemanticVersion: "1", ExecutorID: ExecutorID, Task: GitHubGreenPRTaskSelection("agent/fleiglabs-repo-agent/test"), Admission: workledger.AdmissionPolicy{Sources: []string{"manual"}, Namespaces: []string{GitHubGreenPRRepository}, ObjectKinds: []string{"repository_task"}, Events: []string{"repository_change"}, Actions: []string{"requested"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snap, err := store.ActivateRoute(context.Background(), route, reg, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Admit(context.Background(), snap.ID, workledger.Event{SignalID: "s", SourceDeliveryID: "d", TransportStream: "x", TransportSequence: 1, Source: "manual", Namespace: GitHubGreenPRRepository, ObjectKind: "repository_task", ObjectID: "17", EventKind: "repository_change", Action: "requested", ActorClass: "user", SourceRevision: "abc", CorrelationID: "c", CausationID: "c", PayloadDigest: "sha256:" + strings.Repeat("b", 64), EvidenceRef: "nats://x", ReceivedAt: now}, now)
	if err != nil {
		t.Fatal(err)
	}
	item, attempt, ok, err := store.Claim(context.Background(), now)
	if err != nil || !ok {
		t.Fatal(err)
	}
	return store, item, attempt, now
}
func coordinatorFixture(t *testing.T) (*workledger.Store, workledger.WorkItem, workledger.ExecutorAttempt, time.Time) {
	return coordinatorFixtureAt(t, filepath.Join(t.TempDir(), "db"))
}

func TestRegisteredVerifierMappingsAndLocalOpaqueRevision(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, tc := range []struct {
		phase, packageOutcome, signal string
	}{
		{"pending", "waiting", "waiting"},
		{"red", "continuation", "continuation_required"},
		{"red", "missing_or_stale", "continuation_required"},
		{"green", "satisfied", "satisfied"},
		{"escalated", "escalated", "escalated"},
		{"refused", "escalated", "escalated"},
		{"escalated", "escalated", "escalated"},
	} {
		if !verifierPhaseMatches(tc.phase, tc.packageOutcome) || signalOutcome(tc.packageOutcome) != tc.signal {
			t.Fatalf("mapping phase=%s outcome=%s", tc.phase, tc.packageOutcome)
		}
	}
	local := &VerifierEvent{Phase: "refused", Outcome: "escalated", ContractDigest: digest, TaskEvidenceDigest: digest, HeadRevision: "local:unavailable:verifier:turn-42:observation:2", Reasons: []VerifierReason{{Code: "refused", EvidenceRef: "local:refused:" + digest}}, EvidenceRefs: []string{"local:refused:" + digest}}
	if !validVerifierResult(local) {
		t.Fatal("local refusal with an opaque revision was rejected")
	}
	local.Reasons = nil
	if validVerifierResult(local) {
		t.Fatal("non-satisfied verifier result without reasons was accepted")
	}
}
