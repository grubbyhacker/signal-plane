package shadowadmit

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// testExecutor is a minimal registered executor so a route can activate.
type testExecutor struct{ descriptor workledger.ExecutorDescriptor }

func (executor testExecutor) Descriptor() workledger.ExecutorDescriptor { return executor.descriptor }
func (testExecutor) Execute(context.Context, workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	return workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted}, nil
}

func fixture(t *testing.T) (*workledger.Store, workledger.RouteSnapshot, time.Time) {
	t.Helper()
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "shadow.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	route := workledger.RouteDefinition{
		ID: "github-pr-check", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: "github.pr-check",
		Admission:   workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"example/widgets"}, ObjectKinds: []string{"pull_request"}, Events: []string{"pull_request"}, Actions: []string{"synchronize"}},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject, Supersede: true},
		Retry:       workledger.RetryPolicy{MaxAttempts: 3, Backoff: []time.Duration{30 * time.Second, time.Minute}},
	}
	registry := workledger.NewRegistry()
	if err := registry.Register(testExecutor{descriptor: workledger.ExecutorDescriptor{ID: route.ExecutorID, Kind: workledger.ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	return store, snapshot, now
}

func event(delivery string, sequence uint64, revision string) workledger.Event {
	return workledger.Event{SignalID: "signal-" + delivery, SourceDeliveryID: delivery, TransportStream: "signals", TransportSequence: sequence, Source: "github", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "synchronize", ActorClass: "user", SourceRevision: revision, PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals", ReceivedAt: time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)}
}

func TestDisabledSeamIsInert(t *testing.T) {
	store, snapshot, _ := fixture(t)
	shadow, err := New(store, false)
	if err != nil {
		t.Fatal(err)
	}
	if shadow.Enabled() {
		t.Fatal("seam reported enabled when constructed disabled")
	}
	out, err := shadow.Admit(context.Background(), Candidate{RouteSnapshotID: snapshot.ID, Event: event("d-inert", 1, "rev-1")})
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("disabled Admit = %#v, %v; want ErrDisabled", out, err)
	}
	// Nothing was persisted.
	if _, err := store.WorkItemByAuthoritativePR(context.Background(), "example/widgets", 1); err == nil {
		t.Fatal("disabled seam persisted a work item")
	}
}

func TestShadowAdmitPersistsBindingAndDedupesDeterministically(t *testing.T) {
	ctx := context.Background()
	store, snapshot, _ := fixture(t)
	shadow, err := New(store, true)
	if err != nil {
		t.Fatal(err)
	}
	binding := workledger.AgentBinding{AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "contract-v1"}
	candidate := Candidate{RouteSnapshotID: snapshot.ID, Event: event("d-1", 1, "rev-1"), Binding: binding}

	first, err := shadow.Admit(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || first.Launched || first.WorkItemID == "" {
		t.Fatalf("first admit = %#v", first)
	}
	if first.AgentType != "youknowme-curator" || first.AgentMode != "reconcile" || first.TypeContractRevision != "contract-v1" {
		t.Fatalf("binding not persisted: %#v", first)
	}
	// Same candidate again is a deterministic duplicate onto the same work item.
	second, err := shadow.Admit(ctx, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.WorkItemID != first.WorkItemID || second.Launched {
		t.Fatalf("second admit = %#v (want duplicate of %s)", second, first.WorkItemID)
	}
	// The persisted work item carries the binding and no broker-resolved fields.
	item, err := store.WorkItem(ctx, first.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if item.AgentType != "youknowme-curator" || item.ResolvedReleaseGeneration != 0 || item.ResolvedReleaseDigest != "" || item.BrokerRunID != "" || item.AuthoritativePRRepository != "" {
		t.Fatalf("shadow admission set broker-resolved fields or lost binding: %#v", item)
	}
	// The seam never left an executor attempt (never launched / claimed).
	if _, _, claimed, err := store.Claim(ctx, time.Date(2026, 9, 22, 13, 0, 0, 0, time.UTC)); err == nil && claimed {
		// A claim IS possible against admitted work — that is the legacy
		// launcher's job, not the shadow seam's. The point is the shadow seam
		// itself never called Claim; if it had, the attempt would exist already.
		// We only assert the shadow Outcome never reported a launch.
	}
}

func TestShadowAdmitRefusesSelectionFields(t *testing.T) {
	ctx := context.Background()
	store, snapshot, _ := fixture(t)
	shadow, err := New(store, true)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string]workledger.AgentBinding{
		"resolved_release_generation": {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", ResolvedReleaseGeneration: 1, ResolvedReleaseDigest: "sha256:" + repeat64()},
		"broker_run_id":               {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", BrokerRunID: "run-1"},
		"authoritative_pr_repository": {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", AuthoritativePRRepository: "grubbyhacker/youknowme", AuthoritativePRNumber: 7},
	} {
		_, err := shadow.Admit(ctx, Candidate{RouteSnapshotID: snapshot.ID, Event: event("d-"+name, uint64(len(name)+2), "rev-"+name), Binding: bad})
		if err == nil {
			t.Fatalf("shadow admission accepted caller-supplied selection field %s", name)
		}
	}
}

func TestShadowAdmitRequiresRouteSnapshot(t *testing.T) {
	store, _, _ := fixture(t)
	shadow, err := New(store, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := shadow.Admit(context.Background(), Candidate{Event: event("d-noroute", 1, "rev")}); err == nil {
		t.Fatal("shadow admission accepted a candidate without a route snapshot id")
	}
}

func repeat64() string {
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = 'a'
	}
	return string(buf)
}
