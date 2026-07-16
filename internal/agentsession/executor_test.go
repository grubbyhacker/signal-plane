package agentsession

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

type fakeBroker struct {
	lease, replacement workledger.SessionLease
	acquire            AcquireRequest
	reassign           ReassignRequest
	err                error
}

func (f *fakeBroker) Acquire(_ context.Context, r AcquireRequest) (workledger.SessionLease, error) {
	f.acquire = r
	return f.lease, f.err
}
func (f *fakeBroker) Reassign(_ context.Context, r ReassignRequest) (workledger.SessionLease, error) {
	f.reassign = r
	return f.replacement, f.err
}

type fakeAgentd struct {
	create SessionRequest
	turn   TurnRequest
	stream EventRequest
	events []Event
	err    error
}

func (f *fakeAgentd) CreateSession(_ context.Context, r SessionRequest) (string, error) {
	f.create = r
	return "agentd-session-1", f.err
}
func (f *fakeAgentd) SubmitTurn(_ context.Context, r TurnRequest) (string, error) {
	f.turn = r
	return "turn-1", f.err
}
func (f *fakeAgentd) StreamEvents(_ context.Context, r EventRequest) ([]Event, error) {
	f.stream = r
	return f.events, f.err
}

func coordinatorFixture(t *testing.T) (*workledger.Store, workledger.WorkItem, workledger.ExecutorAttempt, time.Time) {
	t.Helper()
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
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
	e := workledger.Event{SignalID: "signal-1", SourceDeliveryID: "delivery-1", TransportStream: "signals", TransportSequence: 1, Source: "github", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "opened", ActorClass: "user", SourceRevision: "abc", CorrelationID: "correlation-1", CausationID: "cause-1", PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals", ReceivedAt: now}
	if _, err := store.Admit(context.Background(), snap.ID, e, now); err != nil {
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

func TestCoordinatorSubmitsFixedTurnAndPersistsReplaySafeEvidence(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	broker := &fakeBroker{lease: lease("worker-1", 1)}
	agentd := &fakeAgentd{events: []Event{{Cursor: "1", Kind: "evidence", EvidenceRef: "artifact://one"}, {Cursor: "2", Kind: "usage", InputTokens: 3, OutputTokens: 5}, {Cursor: "3", Kind: "runtime_succeeded", EvidenceRef: "sha256:result"}}}
	ex := &Executor{Store: store, Broker: broker, Agentd: agentd, Now: func() time.Time { return now }}
	result, err := ex.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != workledger.OutcomeCompleted || result.ExternalCorrelation != "turn-1" {
		t.Fatalf("result=%+v", result)
	}
	if broker.acquire.AuthorityProfile != authorityProfile || broker.acquire.BindingKey != "session:"+item.ID {
		t.Fatalf("acquire=%+v", broker.acquire)
	}
	if agentd.create.RuntimeAdapter != runtimeAdapter || agentd.create.IdempotencyKey != "session:"+item.ID || agentd.turn.EvidenceDigest != attempt.RequestedOperationDigest {
		t.Fatalf("agentd=%+v %+v", agentd.create, agentd.turn)
	}
	b, err := store.SessionBinding(ctx, item.ID)
	if err != nil || b.EventCursor != "3" || b.FenceEpoch != 1 {
		t.Fatalf("binding=%+v err=%v", b, err)
	}
	count, in, out, err := store.CoordinatorUsage(ctx, item.ID)
	if err != nil || count != 3 || in != 3 || out != 5 {
		t.Fatalf("usage count=%d in=%d out=%d err=%v", count, in, out, err)
	}
	// Replaying an already persisted cursor is idempotent across restart/retry.
	inserted, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: "2", WorkerID: "worker-1", FenceEpoch: 1, Kind: "usage", InputTokens: 3, OutputTokens: 5}, now)
	if err != nil || inserted {
		t.Fatalf("replay inserted=%v err=%v", inserted, err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: "2", WorkerID: "worker-1", FenceEpoch: 1, Kind: "usage", InputTokens: 4, OutputTokens: 5}, now); err == nil {
		t.Fatal("conflicting duplicate cursor accepted")
	}
}

func TestCoordinatorRejectsMalformedEventsAndAuthorityEscalation(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	agentd := &fakeAgentd{events: []Event{{Cursor: "", Kind: "usage"}}}
	ex := &Executor{Store: store, Broker: &fakeBroker{lease: lease("worker-1", 1)}, Agentd: agentd, Now: func() time.Time { return now }}
	r, err := ex.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil || r.RetryClassification != "agentd_protocol" {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	store2, item2, attempt2, now2 := coordinatorFixture(t)
	defer store2.Close()
	ex2 := &Executor{Store: store2, Broker: &fakeBroker{lease: workledger.SessionLease{WorkerID: "worker", AuthorityProfile: "merge", AuthorityPolicyVersion: "p", WorkerLineage: "l", FenceEpoch: 1}}, Agentd: &fakeAgentd{}, Now: func() time.Time { return now2 }}
	r, err = ex2.Execute(ctx, workledger.ExecutorRequest{WorkItem: item2, Attempt: attempt2})
	if err != nil || r.RetryClassification != "coordinator_acquire" {
		t.Fatalf("authority escalation result=%+v err=%v", r, err)
	}
}

func TestReassignmentCASFencesPredecessorAndPreservesLineage(t *testing.T) {
	ctx := context.Background()
	store, item, _, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{replacement: lease("worker-2", 2)}
	// Broker failure models a crash/failure before the durable CAS: no cutover.
	before := &Executor{Store: store, Broker: &fakeBroker{err: errors.New("broker unavailable")}, Now: func() time.Time { return now }}
	if _, err := before.ReassignAfterLoss(ctx, item.ID); err == nil {
		t.Fatal("pre-CAS broker failure unexpectedly cut over")
	}
	prior, err := store.SessionBinding(ctx, item.ID)
	if err != nil || prior.WorkerID != "worker-1" || prior.FenceEpoch != 1 {
		t.Fatalf("pre-CAS binding=%+v err=%v", prior, err)
	}
	ex := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now.Add(time.Second) }}
	b, err := ex.ReassignAfterLoss(ctx, item.ID)
	if err != nil || b.WorkerID != "worker-2" || b.FenceEpoch != 2 {
		t.Fatalf("binding=%+v err=%v", b, err)
	}
	if broker.reassign.PredecessorWorker != "worker-1" || broker.reassign.PredecessorEpoch != 1 {
		t.Fatalf("reassign=%+v", broker.reassign)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: "old", WorkerID: "worker-1", FenceEpoch: 1, Kind: "evidence"}, now); err == nil {
		t.Fatal("stale predecessor accepted")
	}
	// A replay after a crash after the CAS cannot reapply the same predecessor.
	if _, err := ex.ReassignAfterLoss(ctx, item.ID); err == nil {
		t.Fatal("reassignment replay accepted stale predecessor")
	}
	bad := &fakeBroker{replacement: workledger.SessionLease{WorkerID: "worker-3", AuthorityProfile: authorityProfile, AuthorityPolicyVersion: "policy-v1", WorkerLineage: "other-lineage", FenceEpoch: 3}}
	if _, err := bad.Reassign(ctx, ReassignRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReassignSession(ctx, item.ID, "worker-2", 2, bad.replacement, now); err == nil {
		t.Fatal("lineage change accepted")
	}
}
