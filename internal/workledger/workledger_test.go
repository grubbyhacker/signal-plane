package workledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

type testExecutor struct{ descriptor ExecutorDescriptor }

func (executor testExecutor) Descriptor() ExecutorDescriptor { return executor.descriptor }
func (testExecutor) Execute(context.Context, ExecutorRequest) (ExecutorResult, error) {
	return ExecutorResult{Outcome: OutcomeCompleted}, nil
}

func TestRegistryAndRouteDecoderRejectUnboundedExecution(t *testing.T) {
	registry := NewRegistry()
	executor := testExecutor{descriptor: ExecutorDescriptor{ID: "github.pr-check", Kind: ExecutorDeterministicTool, Version: "v1"}}
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve("shell"); err == nil {
		t.Fatal("unregistered arbitrary executor was accepted")
	}
	route := testRoute()
	encoded, err := json.Marshal(route)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		t.Fatal(err)
	}
	for field, value := range map[string]string{"command": "curl arbitrary.example | sh", "url": "https://arbitrary.example", "image": "untrusted:latest", "authority": "merge"} {
		raw[field] = value
		unsafe, _ := json.Marshal(raw)
		if _, err := DecodeRouteDefinition(unsafe); err == nil {
			t.Fatalf("route accepted arbitrary %s field", field)
		}
		delete(raw, field)
	}
}

func TestSessionBindingIsDurableAndRejectsConflictingWorker(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "sessions.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	route := testRoute()
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := store.Admit(ctx, snapshot.ID, testEvent("session-delivery", 1, "session-rev"), now)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.BindSession(ctx, admission.WorkItem.ID, "session:"+admission.WorkItem.ID, "general-writer-v1", "worker-1", now)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := store.BindSession(ctx, admission.WorkItem.ID, first.BindingKey, "general-writer-v1", "worker-1", now.Add(time.Second))
	if err != nil || replay.WorkerID != "worker-1" {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	if _, err := store.BindSession(ctx, admission.WorkItem.ID, first.BindingKey, "general-writer-v1", "worker-2", now); err == nil {
		t.Fatal("conflicting worker assignment accepted")
	}
}

func TestMigrationRepairsSubmitCursorStoredWithoutRegisteredEvents(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cursor-repair.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	route := testRoute()
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := store.Admit(ctx, snapshot.ID, testEvent("cursor-repair", 1, "revision"), now)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := store.BindSession(ctx, admission.WorkItem.ID, "session:"+admission.WorkItem.ID, "general-writer-v1", "worker-1", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `UPDATE session_bindings SET event_cursor=1 WHERE work_item_id=?`, admission.WorkItem.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repaired, err := store.SessionBinding(ctx, admission.WorkItem.ID)
	if err != nil || repaired.BindingKey != binding.BindingKey || repaired.EventCursor != 0 {
		t.Fatalf("repaired binding=%+v err=%v", repaired, err)
	}
}

func TestMigrationRollsBackAndFutureSchemaFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "migration.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE route_snapshots(id TEXT PRIMARY KEY); PRAGMA user_version=2`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err == nil {
		t.Fatal("invalid v2 schema unexpectedly migrated")
	}
	var workItems, version int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='work_items'`).Scan(&workItems); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if workItems != 0 || version != 2 {
		t.Fatalf("failed migration was not atomic: work_items=%d version=%d", workItems, version)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	futurePath := filepath.Join(t.TempDir(), "future.db")
	future, err := sql.Open("sqlite", futurePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := future.Exec(fmt.Sprintf(`PRAGMA user_version=%d`, SchemaVersion+1)); err != nil {
		t.Fatal(err)
	}
	if err := future.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err := Open(futurePath); err == nil {
		store.Close()
		t.Fatal("future schema version was accepted")
	}
}

func TestMigrationFromV3AddsOperationIdempotencyEvidence(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "v3.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statement := `CREATE TABLE executor_attempts (id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL, attempt_number INTEGER NOT NULL, executor_id TEXT NOT NULL, executor_kind TEXT NOT NULL, executor_version TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, requested_operation_digest TEXT NOT NULL, state TEXT NOT NULL, retry_classification TEXT NOT NULL DEFAULT '', external_correlation TEXT NOT NULL DEFAULT '', result_digest TEXT NOT NULL DEFAULT '', sanitized_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, started_at INTEGER NOT NULL, completed_at INTEGER, UNIQUE(work_item_id,attempt_number)); PRAGMA user_version=3`
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(`PRAGMA table_info(executor_attempts)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatal(err)
		}
		found = found || name == "operation_idempotency_key"
	}
	if !found {
		t.Fatal("v3 migration omitted operation idempotency evidence")
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestMigrationFromV10ReplacesVerifierOutcomeSchema(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "v10.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE verifier_results (work_item_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL DEFAULT '', result_digest TEXT NOT NULL DEFAULT '', verifier_id TEXT NOT NULL, completion_contract TEXT NOT NULL, contract_digest TEXT NOT NULL, task_evidence_digest TEXT NOT NULL, head_revision TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('satisfied','missing_or_stale','continuation','escalated')), reason_codes_json TEXT NOT NULL, evidence_refs_json TEXT NOT NULL, recorded_at INTEGER NOT NULL); PRAGMA user_version=10`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	var definition string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='verifier_results'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(definition, "'waiting','continuation_required','satisfied','escalated'") {
		t.Fatalf("verifier outcome schema=%s", definition)
	}
}

func TestLedgerAdmissionDedupeSerializationSupersessionRetryAndRecovery(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	route := testRoute()
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	firstEvent := testEvent("delivery-1", 1, "rev-1")
	first, err := store.Admit(ctx, snapshot.ID, firstEvent, now)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.Admit(ctx, snapshot.ID, firstEvent, now.Add(time.Second))
	if err != nil || !duplicate.Duplicate || duplicate.WorkItem.ID != first.WorkItem.ID {
		t.Fatalf("duplicate admission = %#v, %v", duplicate, err)
	}
	redelivery := firstEvent
	redelivery.SignalID = "signal-redelivery"
	redelivery.TransportSequence = 3
	redelivery.CorrelationID = "transport-correlation-redelivery"
	duplicate, err = store.Admit(ctx, snapshot.ID, redelivery, now.Add(time.Second))
	if err != nil || !duplicate.Duplicate || duplicate.WorkItem.ID != first.WorkItem.ID {
		t.Fatalf("realistic redelivery admission = %#v, %v", duplicate, err)
	}
	mutated := firstEvent
	mutated.PayloadDigest = "sha256:different"
	if _, err := store.Admit(ctx, snapshot.ID, mutated, now.Add(time.Second)); err == nil {
		t.Fatal("mutated replay with reused delivery id was accepted")
	}
	second, err := store.Admit(ctx, snapshot.ID, testEvent("delivery-2", 2, "rev-2"), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var state WorkState
	var successor string
	if err := store.db.QueryRow(`SELECT state,superseded_by_id FROM work_items WHERE id=?`, first.WorkItem.ID).Scan(&state, &successor); err != nil {
		t.Fatal(err)
	}
	if state != StateSuperseded || successor != second.WorkItem.ID {
		t.Fatalf("old work = %s superseded by %q", state, successor)
	}
	item, attempt, ok, err := store.Claim(ctx, now.Add(3*time.Second))
	if err != nil || !ok || item.ID != second.WorkItem.ID || attempt.AttemptNumber != 1 {
		t.Fatalf("claim = %#v %#v %v %v", item, attempt, ok, err)
	}
	if _, _, claimed, err := store.Claim(ctx, now.Add(3*time.Second)); err != nil || claimed {
		t.Fatalf("serialization allowed parallel claim: %v %v", claimed, err)
	}
	if err := store.Complete(ctx, attempt.ID, ExecutorResult{Outcome: OutcomeRetryableFailure, RetryClassification: "transient"}, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, claimed, err := store.Claim(ctx, now.Add(10*time.Second)); err != nil || claimed {
		t.Fatalf("retry ran before snapshotted backoff: %v %v", claimed, err)
	}
	_, retry, claimed, err := store.Claim(ctx, now.Add(35*time.Second))
	if err != nil || !claimed || retry.AttemptNumber != 2 {
		t.Fatalf("retry claim = %#v %v %v", retry, claimed, err)
	}
	recovered, err := store.RecoverInterrupted(ctx, now.Add(36*time.Second))
	if err != nil || recovered != 1 {
		t.Fatalf("recover = %d, %v", recovered, err)
	}
	_, recoveredAttempt, claimed, err := store.Claim(ctx, now.Add(37*time.Second))
	if err != nil || !claimed || recoveredAttempt.ID != retry.ID || recoveredAttempt.IdempotencyKey != retry.IdempotencyKey || recoveredAttempt.AttemptNumber != 2 {
		t.Fatalf("recovered claim = %#v %v %v", recoveredAttempt, claimed, err)
	}
	if err := store.Complete(ctx, recoveredAttempt.ID, ExecutorResult{Outcome: OutcomeCompleted, ExternalCorrelation: "github-check-42"}, now.Add(38*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT state FROM work_items WHERE id=?`, second.WorkItem.ID).Scan(&state); err != nil || state != StateCompleted {
		t.Fatalf("completed state = %q, %v", state, err)
	}
}

func TestSupersedingActiveWorkTerminalizesAttemptAndReleasesLease(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	route := testRoute()
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Admit(ctx, snapshot.ID, testEvent("delivery-active-1", 1, "rev-1"), now)
	if err != nil {
		t.Fatal(err)
	}
	_, attempt, claimed, err := store.Claim(ctx, now.Add(time.Second))
	if err != nil || !claimed {
		t.Fatalf("claim active work: %#v %v %v", attempt, claimed, err)
	}
	second, err := store.Admit(ctx, snapshot.ID, testEvent("delivery-active-2", 2, "rev-2"), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var attemptState AttemptState
	var completedAt sql.NullInt64
	if err := store.db.QueryRow(`SELECT state,completed_at FROM executor_attempts WHERE id=?`, attempt.ID).Scan(&attemptState, &completedAt); err != nil {
		t.Fatal(err)
	}
	if attemptState != AttemptSuperseded || !completedAt.Valid {
		t.Fatalf("superseded attempt state=%q completed=%v", attemptState, completedAt.Valid)
	}
	var leases int
	if err := store.db.QueryRow(`SELECT count(*) FROM serialization_leases WHERE work_item_id=?`, first.WorkItem.ID).Scan(&leases); err != nil || leases != 0 {
		t.Fatalf("superseded lease count=%d err=%v", leases, err)
	}
	// Prove startup recovery also cleans an orphan produced by an older process
	// that persisted supersession before terminalizing its attempt and lease.
	if _, err := store.db.Exec(`UPDATE executor_attempts SET state=?,completed_at=NULL WHERE id=?`, AttemptRunning, attempt.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO serialization_leases(serialization_key,work_item_id,attempt_id,acquired_at) VALUES(?,?,?,?)`, first.WorkItem.SerializationKey, first.WorkItem.ID, attempt.ID, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if recovered, err := store.RecoverInterrupted(ctx, now.Add(3*time.Second)); err != nil || recovered != 0 {
		t.Fatalf("recover superseded orphan=%d err=%v", recovered, err)
	}
	if err := store.db.QueryRow(`SELECT state,completed_at FROM executor_attempts WHERE id=?`, attempt.ID).Scan(&attemptState, &completedAt); err != nil {
		t.Fatal(err)
	}
	if attemptState != AttemptSuperseded || !completedAt.Valid {
		t.Fatalf("recovered orphan state=%q completed=%v", attemptState, completedAt.Valid)
	}
	item, successorAttempt, claimed, err := store.Claim(ctx, now.Add(3*time.Second))
	if err != nil || !claimed || item.ID != second.WorkItem.ID {
		t.Fatalf("successor claim=%#v claimed=%v err=%v", item, claimed, err)
	}
	if err := store.Complete(ctx, successorAttempt.ID, ExecutorResult{Outcome: OutcomeRetryableFailure, RetryClassification: "transient"}, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	third, err := store.Admit(ctx, snapshot.ID, testEvent("delivery-active-3", 3, "rev-3"), now.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT state,completed_at FROM executor_attempts WHERE id=?`, successorAttempt.ID).Scan(&attemptState, &completedAt); err != nil {
		t.Fatal(err)
	}
	if attemptState != AttemptSuperseded || !completedAt.Valid {
		t.Fatalf("superseded retry attempt state=%q completed=%v", attemptState, completedAt.Valid)
	}
	item, _, claimed, err = store.Claim(ctx, now.Add(6*time.Second))
	if err != nil || !claimed || item.ID != third.WorkItem.ID {
		t.Fatalf("retry successor claim=%#v claimed=%v err=%v", item, claimed, err)
	}
}

func TestRouteMatchingFailsClosedOnZeroAndAmbiguity(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	event := testEvent("delivery-match", 1, "rev-1")
	if _, err := store.MatchRoute(ctx, event); err == nil {
		t.Fatal("event without an active route matched")
	}
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: "github.pr-check", Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	first := testRoute()
	firstSnapshot, err := store.ActivateRoute(ctx, first, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	matched, err := store.MatchRoute(ctx, event)
	if err != nil || matched.ID != firstSnapshot.ID {
		t.Fatalf("sole route match=%#v err=%v", matched, err)
	}
	second := testRoute()
	second.ID = "github-pr-check-overlap"
	if _, err := store.ActivateRoute(ctx, second, registry, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MatchRoute(ctx, event); err == nil {
		t.Fatal("overlapping active routes did not fail closed")
	}
}

func TestRouteVersionCreatesNewWorkAndWaitingRequiresWakeup(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	route := testRoute()
	registryV1 := NewRegistry()
	if err := registryV1.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshotV1, err := store.ActivateRoute(ctx, route, registryV1, now)
	if err != nil {
		t.Fatal(err)
	}
	repeatedV1, err := store.ActivateRoute(ctx, route, registryV1, now.Add(time.Second))
	if err != nil || repeatedV1.ID != snapshotV1.ID {
		t.Fatalf("identical activation was not idempotent: %#v %v", repeatedV1, err)
	}
	first, err := store.Admit(ctx, snapshotV1.ID, testEvent("delivery-v1", 1, "same-revision"), now)
	if err != nil {
		t.Fatal(err)
	}
	registryV2 := NewRegistry()
	if err := registryV2.Register(testExecutor{descriptor: ExecutorDescriptor{ID: route.ExecutorID, Kind: ExecutorDeterministicTool, Version: "v2"}}); err != nil {
		t.Fatal(err)
	}
	snapshotV2, err := store.ActivateRoute(ctx, route, registryV2, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Admit(ctx, snapshotV2.ID, testEvent("delivery-v2", 2, "same-revision"), now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if first.WorkItem.ID == second.WorkItem.ID || second.WorkItem.RouteSnapshotID != snapshotV2.ID {
		t.Fatalf("route upgrade reused old work: first=%#v second=%#v", first.WorkItem, second.WorkItem)
	}
	item, attempt, claimed, err := store.Claim(ctx, now.Add(3*time.Second))
	if err != nil || !claimed || item.ID != second.WorkItem.ID {
		t.Fatalf("claim = %#v %#v %v %v", item, attempt, claimed, err)
	}
	if err := store.Complete(ctx, attempt.ID, ExecutorResult{Outcome: OutcomeWaiting}, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, claimed, err := store.Claim(ctx, now.Add(9*time.Second)); err != nil || !claimed {
		t.Fatalf("scheduled waiting work was not claimable: %v %v", claimed, err)
	}
}

func TestWaitingDeadlineEscalatesWithoutContinuation(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "deadline.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: "deadline", Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	route := testRoute()
	route.ExecutorID = "deadline"
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := store.Admit(ctx, snapshot.ID, testEvent("deadline-delivery", 91, "deadline"), now)
	if err != nil {
		t.Fatal(err)
	}
	_, attempt, ok, err := store.Claim(ctx, now)
	if err != nil || !ok {
		t.Fatalf("claim=%v err=%v", ok, err)
	}
	if err := store.Complete(ctx, attempt.ID, ExecutorResult{Outcome: OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := store.Claim(ctx, now.Add(durableWaitDeadline+time.Second)); err != nil || ok {
		t.Fatalf("deadline claim=%v err=%v", ok, err)
	}
	var state WorkState
	var continuations int
	var deadline sql.NullInt64
	if err := store.db.QueryRowContext(ctx, `SELECT state,continuation_count,wait_deadline_at FROM work_items WHERE id=?`, admitted.WorkItem.ID).Scan(&state, &continuations, &deadline); err != nil || state != StateFailed || continuations != 0 {
		t.Fatalf("deadline state=%s continuations=%d deadline=%+v err=%v", state, continuations, deadline, err)
	}
}

func TestGitHubAdapterRequiresAuthenticationAndPreservesCausality(t *testing.T) {
	now := time.Now().UTC()
	signal := envelope.Signal{Meta: envelope.Meta{SignalID: "signal-1", Source: "github", SourceEvent: "pull_request", SourceAction: "synchronize", SourceDeliveryID: "delivery-1", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", SourceRevision: "abc123", CorrelationID: "correlation-1", CausationID: "cause-1", RootWorkItemID: "root-1", ParentWorkItemID: "parent-1", HopCount: 2, ReceivedAt: now}, Payload: json.RawMessage(`{"action":"synchronize"}`)}
	adapter := GitHubAdapter{}
	if _, err := adapter.Normalize(signal, "signals", 1); err == nil {
		t.Fatal("unauthenticated envelope was normalized")
	}
	signal.Meta.Authentication = envelope.Authentication{Method: "github_hmac_sha256", Verified: true}
	event, err := adapter.Normalize(signal, "signals", 1)
	if err != nil {
		t.Fatal(err)
	}
	if event.Namespace != "example/widgets" || event.SourceRevision != "abc123" || event.CausationID != "cause-1" || event.ParentWorkItemID != "parent-1" || event.HopCount != 2 || event.PayloadDigest == "" || event.EvidenceRef != "jetstream://signals/1" {
		t.Fatalf("normalized event lost identity or causality: %#v", event)
	}
}

func TestReleaseOperationIsAtomicAndContentResultIsReplaySafe(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	registry := NewRegistry()
	if err := registry.Register(testExecutor{descriptor: ExecutorDescriptor{ID: "youknowme_upload_v1", Kind: ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	route := RouteDefinition{ID: "resume-release", SchemaVersion: 1, SemanticVersion: "1", ExecutorID: "youknowme_upload_v1", Admission: AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"grubbyhacker/resume-builder"}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: ConcurrencyPolicy{Serialization: SerializeObject}, Retry: RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	event := Event{SignalID: "signal-release", SourceDeliveryID: "delivery-release", TransportStream: "SIGNALS", TransportSequence: 1, Source: "github", Namespace: "grubbyhacker/resume-builder", ObjectKind: "release", ObjectID: "77", EventKind: "release", Action: "published", SourceRevision: "rev", PayloadDigest: "sha256:payload", EvidenceRef: "jetstream://SIGNALS/1", ReceivedAt: now}
	digest := "sha256:" + strings.Repeat("a", 64)
	operation := ReleaseOperation{Repository: "grubbyhacker/resume-builder", RepositoryID: 42, InstallationID: 146625575, ReleaseID: 77, Tag: "v2026.07.14-abcdef0", PublishedAt: "2026-07-14T12:00:00Z", TargetCommitish: "abcdef0123456789abcdef0123456789abcdef01", CommitSHA: "abcdef0123456789abcdef0123456789abcdef01", AssetID: 9, AssetName: "Roger_Fleig_20260714.structured.md", AssetSize: 20, AssetContentType: "text/markdown", ProviderDigest: digest, ComputedDigest: digest}
	admitted, err := store.AdmitRelease(ctx, snapshot.ID, event, operation, now)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := store.ReleaseOperation(ctx, admitted.WorkItem.ID)
	if err != nil || stored.ComputedDigest != digest {
		t.Fatalf("stored=%#v err=%v", stored, err)
	}
	conflictOperation := operation
	conflictOperation.AssetID = 99
	conflictEvent := event
	conflictEvent.SignalID = "signal-conflict"
	conflictEvent.SourceDeliveryID = "delivery-conflict"
	conflictEvent.TransportSequence = 2
	if _, err := store.AdmitRelease(ctx, snapshot.ID, conflictEvent, conflictOperation, now); err == nil {
		t.Fatal("same revision accepted conflicting durable operation")
	}
	_, attempt1, claimed, err := store.Claim(ctx, now)
	if err != nil || !claimed || attempt1.OperationIdempotencyKey != "signal-plane:resume:v1:"+strings.TrimPrefix(digest, "sha256:") || attempt1.RequestedOperationDigest != digest {
		t.Fatalf("attempt1=%#v claimed=%v err=%v", attempt1, claimed, err)
	}
	if err := store.Complete(ctx, attempt1.ID, ExecutorResult{Outcome: OutcomeRetryableFailure}, now); err != nil {
		t.Fatal(err)
	}
	_, attempt2, claimed, err := store.Claim(ctx, now.Add(2*time.Second))
	if err != nil || !claimed || attempt2.IdempotencyKey == attempt1.IdempotencyKey || attempt2.OperationIdempotencyKey != attempt1.OperationIdempotencyKey {
		t.Fatalf("attempt2=%#v claimed=%v err=%v", attempt2, claimed, err)
	}
	if err := store.Complete(ctx, attempt2.ID, ExecutorResult{Outcome: OutcomeCompleted}, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	secondOperation := operation
	secondOperation.ReleaseID = 78
	secondOperation.AssetID = 10
	secondEvent := event
	secondEvent.SignalID = "signal-release-2"
	secondEvent.SourceDeliveryID = "delivery-release-2"
	secondEvent.TransportSequence = 2
	secondEvent.ObjectID = "78"
	secondEvent.SourceRevision = "rev-2"
	second, err := store.AdmitRelease(ctx, snapshot.ID, secondEvent, secondOperation, now.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	item, attempt3, claimed, err := store.Claim(ctx, now.Add(3*time.Second))
	if err != nil || !claimed || item.ID != second.WorkItem.ID || attempt3.OperationIdempotencyKey != attempt1.OperationIdempotencyKey {
		t.Fatalf("attempt3=%#v claimed=%v err=%v", attempt3, claimed, err)
	}
	if err := store.RecordContentResult(ctx, digest, "upl_1", "sha256:result", now); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordContentResult(ctx, digest, "upl_1", "sha256:result", now); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordContentResult(ctx, digest, "upl_2", "sha256:other", now); err == nil {
		t.Fatal("conflicting durable content result accepted")
	}
}

func testRoute() RouteDefinition {
	return RouteDefinition{ID: "github-pr-check", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: "github.pr-check", Admission: AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"example/widgets"}, ObjectKinds: []string{"pull_request"}, Events: []string{"pull_request"}, Actions: []string{"synchronize"}}, Concurrency: ConcurrencyPolicy{Serialization: SerializeObject, Supersede: true}, Retry: RetryPolicy{MaxAttempts: 3, Backoff: []time.Duration{30 * time.Second, time.Minute}}}
}

func testEvent(delivery string, sequence uint64, revision string) Event {
	return Event{SignalID: "signal-" + delivery, SourceDeliveryID: delivery, TransportStream: "signals", TransportSequence: sequence, Source: "github", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "synchronize", ActorClass: "user", SourceRevision: revision, CorrelationID: "correlation-1", CausationID: "cause-1", PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals", ReceivedAt: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)}
}
