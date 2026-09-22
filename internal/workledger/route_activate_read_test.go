package workledger

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

type stubExecutor struct{ descriptor ExecutorDescriptor }

func (e stubExecutor) Descriptor() ExecutorDescriptor { return e.descriptor }
func (e stubExecutor) Execute(context.Context, ExecutorRequest) (ExecutorResult, error) {
	return ExecutorResult{}, nil
}

func activeRouteDefinition() RouteDefinition {
	return RouteDefinition{
		ID:              "youknowme.active_read",
		SchemaVersion:   1,
		SemanticVersion: "1.0.0",
		ExecutorID:      "youknowme-curator",
		Admission:       AdmissionPolicy{Sources: []string{"github"}, ObjectKinds: []string{"release"}, Events: []string{"release"}},
		Concurrency:     ConcurrencyPolicy{Serialization: SerializeObject},
		Retry:           RetryPolicy{MaxAttempts: 1},
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func TestActiveRouteSnapshotAndDigest(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if _, ok, err := store.ActiveRouteSnapshot(ctx, "youknowme.active_read"); err != nil || ok {
		t.Fatalf("expected no active snapshot before activation: ok=%v err=%v", ok, err)
	}

	definition := activeRouteDefinition()
	executor := ExecutorDescriptor{ID: "youknowme-curator", Kind: ExecutorDeterministicTool, Version: "1.0.0"}
	registry := NewRegistry()
	if err := registry.Register(stubExecutor{descriptor: executor}); err != nil {
		t.Fatalf("register: %v", err)
	}
	snapshot, err := store.ActivateRoute(ctx, definition, registry, now)
	if err != nil {
		t.Fatalf("ActivateRoute: %v", err)
	}

	active, ok, err := store.ActiveRouteSnapshot(ctx, definition.ID)
	if err != nil || !ok {
		t.Fatalf("ActiveRouteSnapshot after activation: ok=%v err=%v", ok, err)
	}
	if active.ID != snapshot.ID {
		t.Fatalf("active id %q != minted %q", active.ID, snapshot.ID)
	}

	want, err := ActivationDigest(definition, executor)
	if err != nil {
		t.Fatalf("ActivationDigest: %v", err)
	}
	if active.Digest != want {
		t.Fatalf("active digest %q != ActivationDigest %q", active.Digest, want)
	}
}
