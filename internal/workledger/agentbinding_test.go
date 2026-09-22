package workledger

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// agentTestFixture activates a route and returns the store so agent-field tests
// share one admission path.
func agentTestFixture(t *testing.T) (*Store, RouteSnapshot, time.Time) {
	t.Helper()
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "agent.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	route := testRoute()
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	return store, snapshot, now
}

func TestMigrationV21AddsAgentColumns(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "v21.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A pre-agent v21 work_items table lacking the agent columns.
	priorSchema := `CREATE TABLE work_items (id TEXT PRIMARY KEY, route_snapshot_id TEXT NOT NULL, route_id TEXT NOT NULL, semantic_object_key TEXT NOT NULL, source TEXT NOT NULL, namespace TEXT NOT NULL, object_kind TEXT NOT NULL, object_id TEXT NOT NULL, source_revision TEXT NOT NULL, serialization_key TEXT NOT NULL, task_evidence_digest TEXT NOT NULL DEFAULT '', continuation_count INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL, state_version INTEGER NOT NULL DEFAULT 1, superseded_by_id TEXT, latest_executor_correlation TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, terminal_at INTEGER, next_attempt_at INTEGER); PRAGMA user_version=21`
	if _, err := db.Exec(priorSchema); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"agent_type":                  false,
		"agent_mode":                  false,
		"type_contract_revision":      false,
		"resolved_release_generation": false,
		"resolved_release_digest":     false,
		"broker_run_id":               false,
		"authoritative_pr_repository": false,
		"authoritative_pr_number":     false,
	}
	rows, err := db.Query(`PRAGMA table_info(work_items)`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if _, ok := want[name]; ok {
			want[name] = true
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for name, found := range want {
		if !found {
			t.Fatalf("v21 migration omitted agent column %s", name)
		}
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestAdmissionWithoutAgentPreservesZeroValueBehavior(t *testing.T) {
	ctx := context.Background()
	store, snapshot, now := agentTestFixture(t)
	admitted, err := store.Admit(ctx, snapshot.ID, testEvent("delivery-noagent", 1, "rev-1"), now)
	if err != nil {
		t.Fatal(err)
	}
	item, err := store.WorkItem(ctx, admitted.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The pre-agent path must round-trip to a fully zero binding.
	binding := AgentBinding{
		AgentType:                 item.AgentType,
		Mode:                      item.AgentMode,
		TypeContractRevision:      item.TypeContractRevision,
		ResolvedReleaseGeneration: item.ResolvedReleaseGeneration,
		ResolvedReleaseDigest:     item.ResolvedReleaseDigest,
		BrokerRunID:               item.BrokerRunID,
		AuthoritativePRRepository: item.AuthoritativePRRepository,
		AuthoritativePRNumber:     item.AuthoritativePRNumber,
	}
	if !binding.Zero() {
		t.Fatalf("no-agent admission produced a non-zero binding: %#v", binding)
	}
	if admitted.WorkItem.AgentType != "" || admitted.WorkItem.ResolvedReleaseGeneration != 0 {
		t.Fatalf("returned work item carried an agent binding: %#v", admitted.WorkItem)
	}
}

func TestAdmissionBindsAgentTypeAndModeButRejectsSelection(t *testing.T) {
	ctx := context.Background()
	store, snapshot, now := agentTestFixture(t)

	// A deployment-owned route selection is admission-safe and round-trips.
	binding := AgentBinding{AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "contract-v1"}
	admitted, err := store.AdmitWithAgent(ctx, snapshot.ID, testEvent("delivery-agent", 1, "rev-agent"), binding, now)
	if err != nil {
		t.Fatal(err)
	}
	item, err := store.WorkItem(ctx, admitted.WorkItem.ID)
	if err != nil {
		t.Fatal(err)
	}
	if item.AgentType != "youknowme-curator" || item.AgentMode != "reconcile" || item.TypeContractRevision != "contract-v1" {
		t.Fatalf("admission binding not persisted: %#v", item)
	}
	// Broker-resolved fields must remain zero at admission time.
	if item.ResolvedReleaseGeneration != 0 || item.ResolvedReleaseDigest != "" || item.BrokerRunID != "" || item.AuthoritativePRRepository != "" || item.AuthoritativePRNumber != 0 {
		t.Fatalf("admission set broker-resolved fields: %#v", item)
	}

	// Supplying any selection field at admission is rejected.
	for name, bad := range map[string]AgentBinding{
		"resolved_release_generation": {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", ResolvedReleaseGeneration: 1, ResolvedReleaseDigest: "sha256:" + repeat64()},
		"resolved_release_digest":     {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", ResolvedReleaseGeneration: 1, ResolvedReleaseDigest: "sha256:" + repeat64()},
		"broker_run_id":               {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", BrokerRunID: "run-1"},
		"authoritative_pr_repository": {AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", AuthoritativePRRepository: "grubbyhacker/youknowme", AuthoritativePRNumber: 7},
	} {
		event := testEvent("delivery-reject-"+name, uint64(len(name)+2), "rev-"+name)
		if _, err := store.AdmitWithAgent(ctx, snapshot.ID, event, bad, now); err == nil {
			t.Fatalf("admission accepted a caller-supplied selection field %s", name)
		}
	}
}

func TestResolveReleaseIsMonotonicAndDigestLocked(t *testing.T) {
	ctx := context.Background()
	store, snapshot, now := agentTestFixture(t)
	binding := AgentBinding{AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "contract-v1"}
	admitted, err := store.AdmitWithAgent(ctx, snapshot.ID, testEvent("delivery-resolve", 1, "rev-resolve"), binding, now)
	if err != nil {
		t.Fatal(err)
	}
	digest2 := "sha256:" + repeat64()
	digest3 := "sha256:" + repeatChar('b')

	if err := store.ResolveRelease(ctx, admitted.WorkItem.ID, 2, digest2, "run-2", now); err != nil {
		t.Fatal(err)
	}
	item, err := store.WorkItem(ctx, admitted.WorkItem.ID)
	if err != nil || item.ResolvedReleaseGeneration != 2 || item.ResolvedReleaseDigest != digest2 || item.BrokerRunID != "run-2" {
		t.Fatalf("resolve did not persist: %#v err=%v", item, err)
	}
	// Re-resolving the same generation with the same digest is idempotent.
	if err := store.ResolveRelease(ctx, admitted.WorkItem.ID, 2, digest2, "run-2", now); err != nil {
		t.Fatalf("idempotent re-resolve failed: %v", err)
	}
	// Same generation, different digest is refused.
	if err := store.ResolveRelease(ctx, admitted.WorkItem.ID, 2, digest3, "run-2", now); err == nil {
		t.Fatal("same generation accepted a different digest")
	}
	// Moving the generation backward is refused.
	if err := store.ResolveRelease(ctx, admitted.WorkItem.ID, 1, digest3, "run-1", now); err == nil {
		t.Fatal("resolve accepted a non-monotonic generation")
	}
	// Moving forward is allowed.
	if err := store.ResolveRelease(ctx, admitted.WorkItem.ID, 3, digest3, "run-3", now); err != nil {
		t.Fatalf("forward resolve failed: %v", err)
	}
	// A non-digest value is refused.
	if err := store.ResolveRelease(ctx, admitted.WorkItem.ID, 4, "not-a-digest", "run-4", now); err == nil {
		t.Fatal("resolve accepted a non-sha256 digest")
	}
}

func TestAuthoritativePRCorrelationAndLookup(t *testing.T) {
	ctx := context.Background()
	store, snapshot, now := agentTestFixture(t)
	binding := AgentBinding{AgentType: "youknowme-curator", Mode: "address_review_feedback", TypeContractRevision: "contract-v1"}
	admitted, err := store.AdmitWithAgent(ctx, snapshot.ID, testEvent("delivery-pr", 1, "rev-pr"), binding, now)
	if err != nil {
		t.Fatal(err)
	}
	// Unknown PR yields no dispatch (ErrNoRows), never a guess.
	if _, err := store.WorkItemByAuthoritativePR(ctx, "grubbyhacker/youknowme", 42); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown PR lookup = %v, want ErrNoRows", err)
	}
	if err := store.RecordAuthoritativePRCorrelation(ctx, admitted.WorkItem.ID, "grubbyhacker/youknowme", 42, now); err != nil {
		t.Fatal(err)
	}
	// Idempotent re-record with the same correlation.
	if err := store.RecordAuthoritativePRCorrelation(ctx, admitted.WorkItem.ID, "grubbyhacker/youknowme", 42, now); err != nil {
		t.Fatalf("idempotent PR correlation failed: %v", err)
	}
	// A conflicting correlation is refused.
	if err := store.RecordAuthoritativePRCorrelation(ctx, admitted.WorkItem.ID, "grubbyhacker/youknowme", 43, now); err == nil {
		t.Fatal("work item accepted a conflicting PR correlation")
	}
	// The later review-webhook lookup maps back to the originating work item.
	found, err := store.WorkItemByAuthoritativePR(ctx, "grubbyhacker/youknowme", 42)
	if err != nil || found.ID != admitted.WorkItem.ID || found.AgentType != "youknowme-curator" {
		t.Fatalf("PR lookup = %#v err=%v", found, err)
	}
	if err := store.RecordAuthoritativePRCorrelation(ctx, admitted.WorkItem.ID, "bad-repo", 5, now); err == nil {
		t.Fatal("correlation accepted a non owner/name repository")
	}
	if err := store.RecordAuthoritativePRCorrelation(ctx, admitted.WorkItem.ID, "grubbyhacker/youknowme", 0, now); err == nil {
		t.Fatal("correlation accepted a non-positive PR number")
	}
}

func TestAgentBindingValidateAndAdmissionSelectionGuard(t *testing.T) {
	if err := (AgentBinding{}).Validate(); err != nil {
		t.Fatalf("zero binding must validate: %v", err)
	}
	if err := (AgentBinding{}).RejectAdmissionSelection(); err != nil {
		t.Fatalf("zero binding must pass admission guard: %v", err)
	}
	// AgentType/mode/contract are admission-safe.
	safe := AgentBinding{AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c"}
	if err := safe.Validate(); err != nil {
		t.Fatalf("valid binding rejected: %v", err)
	}
	if err := safe.RejectAdmissionSelection(); err != nil {
		t.Fatalf("admission-safe binding failed the guard: %v", err)
	}
	// Bad identifiers are rejected.
	for _, bad := range []AgentBinding{
		{AgentType: "YouKnowMe-Curator", Mode: "reconcile", TypeContractRevision: "c"},
		{AgentType: "youknowme-curator", Mode: "Reconcile", TypeContractRevision: "c"},
		{AgentType: "youknowme-curator", Mode: "reconcile"},                                                          // missing contract revision
		{AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "c", ResolvedReleaseGeneration: 1}, // digest missing
	} {
		if err := bad.Validate(); err == nil {
			t.Fatalf("invalid binding accepted: %#v", bad)
		}
	}
	// The guard names each selection field.
	fields := AdmissionSelectionFields()
	if len(fields) != 5 {
		t.Fatalf("expected 5 admission selection fields, got %d", len(fields))
	}
	for _, bad := range []AgentBinding{
		{ResolvedReleaseGeneration: 1, ResolvedReleaseDigest: "sha256:" + repeat64()},
		{BrokerRunID: "run-1"},
		{AuthoritativePRRepository: "grubbyhacker/youknowme", AuthoritativePRNumber: 1},
	} {
		if err := bad.RejectAdmissionSelection(); err == nil {
			t.Fatalf("admission guard accepted a selection field: %#v", bad)
		}
	}
}

func repeat64() string { return repeatChar('a') }
func repeatChar(c byte) string {
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = c
	}
	return string(buf)
}
