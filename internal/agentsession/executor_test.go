package agentsession

import (
	"context"
	"errors"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeBroker struct {
	lease, replacement workledger.SessionLease
	acquire            AcquireRequest
	reassign           ReassignRequest
	create             CreateSessionRequest
	turns              []SubmitTurnRequest
	stream             StreamEventsRequest
	events             []Event
	err                error
	seenReassign       map[string]workledger.SessionLease
}

func (f *fakeBroker) Acquire(_ context.Context, r AcquireRequest) (workledger.SessionLease, error) {
	f.acquire = r
	return f.lease, f.err
}
func (f *fakeBroker) Reassign(_ context.Context, r ReassignRequest) (workledger.SessionLease, error) {
	f.reassign = r
	if f.seenReassign == nil {
		f.seenReassign = make(map[string]workledger.SessionLease)
	}
	if prior, ok := f.seenReassign[r.IdempotencyKey]; ok {
		if prior != f.replacement {
			return workledger.SessionLease{}, errors.New("idempotency key conflicts with successor")
		}
		return prior, f.err
	}
	f.seenReassign[r.IdempotencyKey] = f.replacement
	return f.replacement, f.err
}
func (f *fakeBroker) CreateSession(_ context.Context, r CreateSessionRequest) (BrokerSession, error) {
	f.create = r
	return BrokerSession{SessionID: "agentd-session-1", Lease: f.lease}, f.err
}
func (f *fakeBroker) SubmitTurn(_ context.Context, r SubmitTurnRequest) (BrokerTurn, error) {
	f.turns = append(f.turns, r)
	return BrokerTurn{TurnID: "turn-1", Lease: f.lease}, f.err
}
func (f *fakeBroker) StreamEvents(_ context.Context, r StreamEventsRequest) (BrokerEvents, error) {
	f.stream = r
	return BrokerEvents{Lease: f.lease, Events: f.events}, f.err
}
func coordinatorFixture(t *testing.T) (*workledger.Store, workledger.WorkItem, workledger.ExecutorAttempt, time.Time) {
	t.Helper()
	return coordinatorFixtureAt(t, filepath.Join(t.TempDir(), "ledger.db"))
}
func coordinatorFixtureAt(t *testing.T, path string) (*workledger.Store, workledger.WorkItem, workledger.ExecutorAttempt, time.Time) {
	t.Helper()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	store, err := workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	registry := workledger.NewRegistry()
	if err := registry.Register(&Executor{}); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "agent-session", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"example/widgets"}, ObjectKinds: []string{"pull_request"}, Events: []string{"pull_request"}, Actions: []string{"opened"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snap, err := store.ActivateRoute(context.Background(), route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	event := workledger.Event{SignalID: "signal-1", SourceDeliveryID: "delivery-1", TransportStream: "signals", TransportSequence: 1, Source: "github", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "opened", ActorClass: "user", SourceRevision: "abc", CorrelationID: "correlation-1", CausationID: "cause-1", PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals", ReceivedAt: now}
	if _, err := store.Admit(context.Background(), snap.ID, event, now); err != nil {
		t.Fatal(err)
	}
	item, attempt, ok, err := store.Claim(context.Background(), now)
	if err != nil || !ok {
		t.Fatalf("claim %v %v", ok, err)
	}
	return store, item, attempt, now
}
func lease(worker string, epoch int64) workledger.SessionLease {
	return workledger.SessionLease{WorkerID: worker, AuthorityProfile: authorityProfile, AuthorityPolicyVersion: "policy-v1", WorkerLineage: "volume-lineage-1", FenceEpoch: epoch}
}
func usage(in, cached, out, reason int64) workledger.Usage {
	return workledger.Usage{InputTokens: in, CachedInputTokens: cached, OutputTokens: out, ReasoningOutputTokens: reason, TotalTokens: in + out}
}

func TestAgentdV1TokenUsageTotalsAndBreakdowns(t *testing.T) {
	valid := workledger.Usage{InputTokens: 11, CachedInputTokens: 3, OutputTokens: 7, ReasoningOutputTokens: 2, TotalTokens: 18}
	if !valid.Valid() {
		t.Fatal("valid agentd tokenUsage rejected")
	}
	for _, malformed := range []workledger.Usage{
		{InputTokens: 11, CachedInputTokens: 3, OutputTokens: 7, ReasoningOutputTokens: 2, TotalTokens: 23},
		{InputTokens: 2, CachedInputTokens: 3, OutputTokens: 7, TotalTokens: 9},
		{InputTokens: 11, OutputTokens: 1, ReasoningOutputTokens: 2, TotalTokens: 12},
	} {
		if malformed.Valid() {
			t.Fatalf("malformed agentd tokenUsage accepted: %+v", malformed)
		}
	}
}

func TestCoordinatorRuntimeSuccessIsEvidenceNotTaskCompletion(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	broker := &fakeBroker{lease: lease("worker-1", 1), events: []Event{{Cursor: 1, Kind: "evidence", EvidenceRef: "artifact://one"}, {Cursor: 2, Kind: "usage", Usage: usage(3, 2, 5, 5)}, {Cursor: 3, Kind: "attempt_completed", EvidenceRef: "sha256:result"}}}
	ex := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	result, err := ex.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil || result.Outcome != workledger.OutcomeWaiting || result.ResultDigest != "sha256:result" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := store.Complete(ctx, attempt.ID, result, now); err != nil {
		t.Fatal(err)
	}
	if err := store.WakeWaiting(ctx, item.ID, now); err != nil {
		t.Fatalf("runtime success did not leave work waiting: %v", err)
	}
	n, u, err := store.CoordinatorUsage(ctx, item.ID)
	if err != nil || n != 1 || u != usage(3, 2, 5, 5) {
		t.Fatalf("usage n=%d u=%+v err=%v", n, u, err)
	}
	if broker.create.BindingKey != "session:"+item.ID || broker.turns[0].IdempotencyKey != attempt.IdempotencyKey {
		t.Fatalf("broker requests=%+v %+v", broker.create, broker.turns)
	}
}

func TestCursorOrderingDuplicatesAndRestartReplay(t *testing.T) {
	ctx := context.Background()
	store, item, _, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	one := workledger.CoordinatorEvent{Cursor: 1, WorkerID: "worker-1", FenceEpoch: 1, Kind: "usage", Usage: usage(1, 0, 2, 0)}
	if ok, err := store.RecordCoordinatorEvent(ctx, item.ID, one, now); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 3, WorkerID: "worker-1", FenceEpoch: 1, Kind: "evidence"}, now); err != nil {
		t.Fatalf("gaps are explicitly permitted: %v", err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 2, WorkerID: "worker-1", FenceEpoch: 1, Kind: "evidence"}, now); err == nil {
		t.Fatal("out of order accepted")
	}
	if ok, err := store.RecordCoordinatorEvent(ctx, item.ID, one, now); err != nil || ok {
		t.Fatalf("restart replay before cursor=%v %v", ok, err)
	}
	if ok, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 3, WorkerID: "worker-1", FenceEpoch: 1, Kind: "evidence"}, now); err != nil || ok {
		t.Fatalf("restart duplicate=%v %v", ok, err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 3, WorkerID: "worker-1", FenceEpoch: 1, Kind: "usage", Usage: usage(1, 0, 0, 0)}, now); err == nil {
		t.Fatal("duplicate conflict accepted")
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 4, WorkerID: "worker-1", FenceEpoch: 1, Kind: "usage", Usage: workledger.Usage{InputTokens: 1, TotalTokens: 2}}, now); err == nil {
		t.Fatal("inconsistent total accepted")
	}
}

func TestReassignmentIsExactEpochIdempotentAndPreservesSession(t *testing.T) {
	ctx := context.Background()
	store, item, _, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAgentdSession(ctx, item.ID, lease("worker-1", 1), "logical-session", now); err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{replacement: lease("worker-2", 2)}
	ex := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	b, err := ex.ReassignAfterLoss(ctx, item.ID)
	if err != nil || b.AgentdSessionID != "logical-session" {
		t.Fatalf("replacement lost resume identity: %+v %v", b, err)
	}
	wantKey := reassignmentIdempotencyKey("session:"+item.ID, 1)
	if broker.reassign.IdempotencyKey != wantKey {
		t.Fatalf("reassignment idempotency key=%q want %q", broker.reassign.IdempotencyKey, wantKey)
	}
	if _, err := broker.Reassign(ctx, ReassignRequest{BindingKey: "session:" + item.ID, PredecessorWorker: "worker-1", PredecessorEpoch: 1, IdempotencyKey: wantKey}); err != nil {
		t.Fatalf("same-key same-successor replay denied: %v", err)
	}
	broker.replacement = lease("worker-3", 2)
	if _, err := broker.Reassign(ctx, ReassignRequest{BindingKey: "session:" + item.ID, PredecessorWorker: "worker-1", PredecessorEpoch: 1, IdempotencyKey: wantKey}); err == nil {
		t.Fatal("conflicting key/successor replay accepted")
	}
	if _, err := store.ReassignSession(ctx, item.ID, "worker-1", 1, lease("worker-2", 2), now); err != nil {
		t.Fatalf("same broker successor not idempotent: %v", err)
	}
	if _, err := store.ReassignSession(ctx, item.ID, "worker-1", 1, lease("worker-3", 2), now); err == nil {
		t.Fatal("different successor replay accepted")
	}
	if _, err := store.ReassignSession(ctx, item.ID, "worker-2", 2, lease("worker-3", 4), now); err == nil {
		t.Fatal("nonconsecutive epoch accepted")
	}
}

func TestConcurrentEventAndReassignmentFencesOneSide(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	store, item, _, now := coordinatorFixtureAt(t, path)
	defer store.Close()
	other, err := workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := other.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 1, WorkerID: "worker-1", FenceEpoch: 1, Kind: "evidence"}, now)
		errs <- err
	}()
	go func() {
		defer wg.Done()
		_, err := store.ReassignSession(ctx, item.ID, "worker-1", 1, lease("worker-2", 2), now)
		errs <- err
	}()
	wg.Wait()
	close(errs)
	success := 0
	for err := range errs {
		if err == nil {
			success++
		}
	}
	if success == 0 {
		t.Fatal("concurrent boundary made no durable progress")
	}
	b, err := store.SessionBinding(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if b.WorkerID == "worker-2" && b.FenceEpoch != 2 {
		t.Fatalf("torn reassignment binding: %+v", b)
	}
	if b.EventCursor != 0 && b.EventCursor != 1 {
		t.Fatalf("torn event cursor: %+v", b)
	}
}

func TestSubmitRetryUsesSameIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	broker := &fakeBroker{lease: lease("worker-1", 1)}
	ex := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	_, _ = ex.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	_, _ = ex.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if len(broker.turns) != 2 || broker.turns[0].IdempotencyKey != broker.turns[1].IdempotencyKey {
		t.Fatalf("submit did not preserve idempotency: %+v", broker.turns)
	}
}
func TestBrokerFailure(t *testing.T) {
	store, item, _, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(context.Background(), item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	ex := &Executor{Store: store, Broker: &fakeBroker{err: errors.New("down")}}
	if _, err := ex.ReassignAfterLoss(context.Background(), item.ID); err == nil {
		t.Fatal("broker failure reassigned")
	}
}
