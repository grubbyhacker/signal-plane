package routeactivatecmd

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func validDefinition() workledger.RouteDefinition {
	return workledger.RouteDefinition{
		ID:              "youknowme.reconcile",
		SchemaVersion:   1,
		SemanticVersion: "1.0.0",
		ExecutorID:      "youknowme-curator",
		Admission: workledger.AdmissionPolicy{
			Sources:     []string{"github"},
			ObjectKinds: []string{"release"},
			Events:      []string{"release"},
		},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject},
		Retry:       workledger.RetryPolicy{MaxAttempts: 1},
	}
}

func validExecutor() workledger.ExecutorDescriptor {
	return workledger.ExecutorDescriptor{
		ID:      "youknowme-curator",
		Kind:    workledger.ExecutorDeterministicTool,
		Version: "1.0.0",
	}
}

func mustJSON(t *testing.T, definition workledger.RouteDefinition) []byte {
	t.Helper()
	raw, err := json.Marshal(definition)
	if err != nil {
		t.Fatalf("marshal definition: %v", err)
	}
	return raw
}

func opts(t *testing.T, db string, definition workledger.RouteDefinition) Options {
	t.Helper()
	return Options{
		DatabasePath:        db,
		RouteDefinitionJSON: mustJSON(t, definition),
		Executor:            validExecutor(),
	}
}

func tempDB(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "workledger.db")
}

// The store mints the snapshot id; the CLI never accepts one and reports the
// minted value with the route_snapshot_id shape the store uses.
func TestRunMintsSnapshotID(t *testing.T) {
	db := tempDB(t)
	outcome, err := Run(context.Background(), opts(t, db, validDefinition()), time.Now().UTC())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.HasPrefix(outcome.RouteSnapshotID, "route-") {
		t.Fatalf("expected store-minted route_snapshot_id (route-*), got %q", outcome.RouteSnapshotID)
	}
	if outcome.AlreadyActive || outcome.Superseded {
		t.Fatalf("first activation should be neither already-active nor superseded: %+v", outcome)
	}
	if outcome.RouteID != "youknowme.reconcile" {
		t.Fatalf("unexpected route id %q", outcome.RouteID)
	}
}

// Re-running with the EXACT definition+executor returns the same active id and
// creates no new generation.
func TestRunRerunSameDefinitionIsIdempotent(t *testing.T) {
	db := tempDB(t)
	first, err := Run(context.Background(), opts(t, db, validDefinition()), time.Now().UTC())
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	second, err := Run(context.Background(), opts(t, db, validDefinition()), time.Now().UTC())
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if second.RouteSnapshotID != first.RouteSnapshotID {
		t.Fatalf("rerun minted a new id %q, want stable %q", second.RouteSnapshotID, first.RouteSnapshotID)
	}
	if !second.AlreadyActive {
		t.Fatalf("rerun of the same definition must report already_active=true: %+v", second)
	}
	if second.Superseded {
		t.Fatalf("rerun must not supersede: %+v", second)
	}
}

// A DIFFERENT definition for the same route id fails closed by default.
func TestRunDifferentDefinitionFailsClosed(t *testing.T) {
	db := tempDB(t)
	if _, err := Run(context.Background(), opts(t, db, validDefinition()), time.Now().UTC()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	changed := validDefinition()
	changed.SemanticVersion = "2.0.0" // same route id, different content -> different digest
	_, err := Run(context.Background(), opts(t, db, changed), time.Now().UTC())
	if err == nil {
		t.Fatalf("expected a differing definition to fail closed, got nil error")
	}
	if !strings.Contains(err.Error(), "refusing to supersede") {
		t.Fatalf("expected a fail-closed supersede refusal, got %v", err)
	}
}

// The explicit --allow-supersede transition retires the old snapshot and mints
// a new one, reporting the retired id.
func TestRunAllowSupersedeTransitions(t *testing.T) {
	db := tempDB(t)
	first, err := Run(context.Background(), opts(t, db, validDefinition()), time.Now().UTC())
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	changed := validDefinition()
	changed.SemanticVersion = "2.0.0"
	o := opts(t, db, changed)
	o.AllowSupersede = true
	second, err := Run(context.Background(), o, time.Now().UTC())
	if err != nil {
		t.Fatalf("supersede Run: %v", err)
	}
	if !second.Superseded {
		t.Fatalf("expected superseded=true, got %+v", second)
	}
	if second.PreviousRouteSnapshotID != first.RouteSnapshotID {
		t.Fatalf("expected previous id %q, got %q", first.RouteSnapshotID, second.PreviousRouteSnapshotID)
	}
	if second.RouteSnapshotID == first.RouteSnapshotID {
		t.Fatalf("supersede must mint a new id, got the retired one %q", first.RouteSnapshotID)
	}
}

// A definition whose executor_id does not match the supplied descriptor is
// refused before any write.
func TestRunMismatchedExecutorIDFails(t *testing.T) {
	db := tempDB(t)
	definition := validDefinition()
	definition.ExecutorID = "another-executor"
	_, err := Run(context.Background(), opts(t, db, definition), time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "does not match executor descriptor") {
		t.Fatalf("expected executor id mismatch failure, got %v", err)
	}
}

// An invalid (empty) executor descriptor is refused.
func TestRunMissingExecutorFails(t *testing.T) {
	db := tempDB(t)
	o := opts(t, db, validDefinition())
	o.Executor = workledger.ExecutorDescriptor{}
	_, err := Run(context.Background(), o, time.Now().UTC())
	if err == nil || !strings.Contains(err.Error(), "executor descriptor") {
		t.Fatalf("expected missing executor failure, got %v", err)
	}
}

// The activation write path is Store.ActivateRoute alone: after activation the
// minted id is a genuine ACTIVE snapshot the store recognizes, proving the row
// was created through the store API rather than fabricated.
func TestRunUsesStoreActivateRouteOnly(t *testing.T) {
	db := tempDB(t)
	outcome, err := Run(context.Background(), opts(t, db, validDefinition()), time.Now().UTC())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	store, err := workledger.Open(db)
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	defer store.Close()
	exists, err := store.RouteSnapshotExists(context.Background(), outcome.RouteSnapshotID)
	if err != nil {
		t.Fatalf("RouteSnapshotExists: %v", err)
	}
	if !exists {
		t.Fatalf("minted snapshot %q is not an active store snapshot; activation did not go through Store.ActivateRoute", outcome.RouteSnapshotID)
	}
	active, ok, err := store.ActiveRouteSnapshot(context.Background(), outcome.RouteID)
	if err != nil || !ok {
		t.Fatalf("ActiveRouteSnapshot: ok=%v err=%v", ok, err)
	}
	if active.ID != outcome.RouteSnapshotID {
		t.Fatalf("active snapshot id %q != reported %q", active.ID, outcome.RouteSnapshotID)
	}
}
