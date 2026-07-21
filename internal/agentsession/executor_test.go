package agentsession

import (
	"context"
	"errors"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
	"path/filepath"
	"reflect"
	"strings"
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
	statusState        string
	statusErrorCode    string
}

func (f *fakeBroker) Acquire(_ context.Context, r AcquireRequest) (workledger.SessionLease, error) {
	f.acquire = r
	return f.lease, f.err
}
func (f *fakeBroker) Reassign(_ context.Context, r ReassignRequest) (BrokerReassignment, error) {
	f.reassign = r
	if f.seenReassign == nil {
		f.seenReassign = make(map[string]workledger.SessionLease)
	}
	if prior, ok := f.seenReassign[r.IdempotencyKey]; ok {
		if prior != f.replacement {
			return BrokerReassignment{}, errors.New("idempotency key conflicts with successor")
		}
		return BrokerReassignment{Lease: prior, State: "broker_committed"}, f.err
	}
	f.seenReassign[r.IdempotencyKey] = f.replacement
	return BrokerReassignment{Lease: f.replacement, State: "broker_committed"}, f.err
}
func (f *fakeBroker) ReassignmentStatus(_ context.Context, r ReassignmentStatusRequest) (BrokerReassignmentStatus, error) {
	state := f.statusState
	if state == "" {
		state = "confirmed"
	}
	return BrokerReassignmentStatus{Lease: f.replacement, PredecessorWorker: f.reassign.PredecessorWorker, PredecessorEpoch: r.PredecessorEpoch, RebindIdempotencyKey: "broker-rebind-key", State: state, ErrorCode: f.statusErrorCode}, f.err
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
	if err := registry.RegisterTask(GitHubGreenPRTask{}); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "agent-session", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: ExecutorID, Task: GitHubGreenPRTaskSelection("agent/fleiglabs-repo-agent/test"), Admission: workledger.AdmissionPolicy{Sources: []string{"manual"}, Namespaces: []string{GitHubGreenPRRepository}, ObjectKinds: []string{"repository_task"}, Events: []string{"repository_change"}, Actions: []string{"requested"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snap, err := store.ActivateRoute(context.Background(), route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	event := workledger.Event{SignalID: "signal-1", SourceDeliveryID: "delivery-1", TransportStream: "signals", TransportSequence: 1, Source: "manual", Namespace: GitHubGreenPRRepository, ObjectKind: "repository_task", ObjectID: "17", EventKind: "repository_change", Action: "requested", ActorClass: "user", SourceRevision: "abc", CorrelationID: "correlation-1", CausationID: "cause-1", PayloadDigest: "sha256:" + strings.Repeat("b", 64), EvidenceRef: "nats://signals", ReceivedAt: now}
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
	return workledger.SessionLease{WorkerID: worker, AuthorityProfile: authorityProfile, ProfileVersion: "profile-v1", PolicyDigest: strings.Repeat("a", 64), SessionLineageID: strings.Repeat("1", 32), WorkerStorageLineageID: strings.Repeat("2", 32), WorkerFenceEpoch: epoch}
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
	broker := &fakeBroker{lease: lease("worker-1", 1), events: []Event{{Cursor: 1, Kind: "attempt_started", EvidenceRef: "sha256:start"}, {Cursor: 2, Kind: "attempt_completed", EvidenceRef: "sha256:result", Usage: usage(3, 2, 5, 5)}}}
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
	if broker.create.BindingKey != "session:"+item.ID || len(broker.turns) != 1 || broker.turns[0].BindingKey != "session:"+item.ID || broker.turns[0].IdempotencyKey != attempt.IdempotencyKey {
		t.Fatalf("broker requests=%+v %+v", broker.create, broker.turns)
	}
}

func TestSubmitTurnRequestExcludesCallerTaskAndPrompt(t *testing.T) {
	typeOfRequest := reflect.TypeOf(SubmitTurnRequest{})
	if typeOfRequest.NumField() != 2 || typeOfRequest.Field(0).Name != "BindingKey" || typeOfRequest.Field(1).Name != "IdempotencyKey" {
		t.Fatalf("submit request permits caller task override: %v", typeOfRequest)
	}
}

func TestCursorOrderingDuplicatesAndRestartReplay(t *testing.T) {
	ctx := context.Background()
	store, item, _, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	one := workledger.CoordinatorEvent{Cursor: 1, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_completed", Usage: usage(1, 0, 2, 0)}
	if ok, err := store.RecordCoordinatorEvent(ctx, item.ID, one, now); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 3, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_started"}, now); err == nil {
		t.Fatal("cursor gap accepted")
	}
	second := workledger.CoordinatorEvent{Cursor: 2, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_started"}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, second, now); err != nil {
		t.Fatalf("contiguous cursor rejected: %v", err)
	}
	if ok, err := store.RecordCoordinatorEvent(ctx, item.ID, one, now); err != nil || ok {
		t.Fatalf("restart replay before cursor=%v %v", ok, err)
	}
	if ok, err := store.RecordCoordinatorEvent(ctx, item.ID, second, now); err != nil || ok {
		t.Fatalf("restart duplicate=%v %v", ok, err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 2, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_completed", Usage: usage(1, 0, 0, 0)}, now); err == nil {
		t.Fatal("duplicate conflict accepted")
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 3, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_completed", Usage: workledger.Usage{InputTokens: 1, TotalTokens: 2}}, now); err == nil {
		t.Fatal("inconsistent total accepted")
	}
}

func TestReassignmentIsExactEpochIdempotentAndPreservesSession(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "committed-reassignment-restart.db")
	store, item, _, now := coordinatorFixtureAt(t, path)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAgentdSession(ctx, item.ID, lease("worker-1", 1), "logical-session", now); err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{replacement: lease("worker-2", 2)}
	ex := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	b, err := ex.ReassignAfterLoss(ctx, item.ID, 1)
	if err != nil || b.AgentdSessionID != "logical-session" {
		t.Fatalf("replacement lost resume identity: %+v %v", b, err)
	}
	wantKey := reassignmentIdempotencyKey("session:"+item.ID, 1)
	if broker.reassign.IdempotencyKey != wantKey {
		t.Fatalf("reassignment idempotency key=%q want %q", broker.reassign.IdempotencyKey, wantKey)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ex.Store = store
	broker.err = errors.New("broker must not be called for committed replay")
	replayed, err := ex.ReassignAfterLoss(ctx, item.ID, 1)
	if err != nil || replayed.WorkerID != "worker-2" || replayed.WorkerFenceEpoch != 2 {
		t.Fatalf("committed replay derived a new generation: binding=%+v err=%v", replayed, err)
	}
	if _, err := store.Reassignment(ctx, item.ID, 2); err == nil {
		t.Fatal("committed replay created a successor-generation transition")
	}
	broker.err = nil
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
		_, err := other.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 1, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_started"}, now)
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
	if b.WorkerID == "worker-2" && b.WorkerFenceEpoch != 2 {
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
	if _, err := ex.ReassignAfterLoss(context.Background(), item.ID, 1); err == nil {
		t.Fatal("broker failure reassigned")
	}
}

func TestReassignmentSagaBlocksRoutingAndResumesAfterConfirmedAdoption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reassignment-restart.db")
	store, item, attempt, now := coordinatorFixtureAt(t, path)
	t.Cleanup(func() { _ = store.Close() })
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	if err := store.SetAgentdSession(ctx, item.ID, lease("worker-1", 1), "agentd-session-1", now); err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{lease: lease("worker-1", 1), replacement: lease("worker-2", 2), statusState: "pending"}
	executor := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	if _, err := executor.ReassignAfterLoss(ctx, item.ID, 1); err == nil {
		t.Fatal("pending broker adoption was committed")
	}
	pending, err := store.Reassignment(ctx, item.ID, 1)
	if err != nil || pending.Phase != workledger.ReassignmentBrokerCommitted {
		t.Fatalf("broker-committed crash point was not durable: transition=%+v err=%v", pending, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	executor.Store = store
	if err := store.RoutingReady(ctx, item.ID); err == nil {
		t.Fatal("routing remained open during reassignment")
	}
	result, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil || result.RetryClassification != "coordinator_reassignment" || len(broker.turns) != 0 {
		t.Fatalf("routing barrier result=%+v turns=%d err=%v", result, len(broker.turns), err)
	}
	broker.statusState = "confirmed"
	binding, err := executor.ReassignAfterLoss(ctx, item.ID, 1)
	if err != nil || binding.WorkerID != "worker-2" || binding.WorkerFenceEpoch != 2 {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
	if err := store.RoutingReady(ctx, item.ID); err != nil {
		t.Fatalf("confirmed reassignment did not reopen routing: %v", err)
	}
	transition, err := store.Reassignment(ctx, item.ID, 1)
	if err != nil || transition.Phase != workledger.ReassignmentCoordinatorCommitted || transition.RebindIdempotencyKey != "broker-rebind-key" {
		t.Fatalf("transition=%+v err=%v", transition, err)
	}
}

func TestVerifierReceiptSurvivesRestartWithoutConsumingContinuationTwice(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "verifier-restart.db")
	store, item, attempt, now := coordinatorFixtureAt(t, path)
	if err := store.Complete(ctx, attempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.WorkTaskSnapshot(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	result := workledger.VerifierResult{AttemptID: attempt.ID, VerifierID: snapshot.VerifierID, CompletionContract: snapshot.CompletionContract, ContractDigest: snapshot.ContractDigest, TaskEvidenceDigest: snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("b", 40), Outcome: "continuation_required", ReasonCodes: []string{"missing"}, EvidenceRefs: []string{"evidence://verification/restart"}}
	if err := (&Executor{Store: store, Now: func() time.Time { return now }}).RecordGitHubGreenPRResult(ctx, item.ID, result); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	executor := &Executor{Store: store, Now: func() time.Time { return now }}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, result); err != nil {
		t.Fatalf("exact verifier replay after restart failed: %v", err)
	}
	conflict := result
	conflict.HeadRevision = strings.Repeat("c", 40)
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, conflict); err == nil {
		t.Fatal("conflicting verifier replay after restart was accepted")
	}
	if _, _, ok, err := store.Claim(ctx, now); err != nil || !ok {
		t.Fatalf("exact replay consumed the continuation twice: ok=%v err=%v", ok, err)
	}
}

func TestNamedVerifierTerminalTransitionAndBoundedContinuation(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 1, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_completed", EvidenceRef: "sha256:runtime", Usage: usage(1, 0, 1, 0)}, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, attempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.WorkTaskSnapshot(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Store: store, Now: func() time.Time { return now }}
	continuation := workledger.VerifierResult{AttemptID: attempt.ID, VerifierID: snapshot.VerifierID, CompletionContract: snapshot.CompletionContract, ContractDigest: snapshot.ContractDigest, TaskEvidenceDigest: snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("b", 40), Outcome: "continuation_required", ReasonCodes: []string{"missing"}, EvidenceRefs: []string{"evidence://verification/1"}}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, continuation); err != nil {
		t.Fatal(err)
	}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, continuation); err != nil {
		t.Fatalf("exact verifier replay was not idempotent: %v", err)
	}
	conflict := continuation
	conflict.EvidenceRefs = []string{"evidence://verification/conflict"}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, conflict); err == nil {
		t.Fatal("conflicting verifier replay was accepted")
	}
	_, secondAttempt, ok, err := store.Claim(ctx, now)
	if err != nil || !ok {
		t.Fatalf("bounded continuation was not claimable: ok=%v err=%v", ok, err)
	}
	if err := store.Complete(ctx, secondAttempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	stale := continuation
	stale.TaskEvidenceDigest = "sha256:stale"
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, stale); err == nil {
		t.Fatal("stale verifier evidence accepted")
	}
	satisfied := continuation
	satisfied.AttemptID = secondAttempt.ID
	satisfied.Outcome, satisfied.ReasonCodes, satisfied.EvidenceRefs = "satisfied", nil, []string{"evidence://verification/2"}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, satisfied); err != nil {
		t.Fatal(err)
	}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, satisfied); err != nil {
		t.Fatalf("terminal verifier replay was not idempotent: %v", err)
	}
	if _, _, ok, err := store.Claim(ctx, now); err != nil || ok {
		t.Fatalf("satisfied work remained claimable: ok=%v err=%v", ok, err)
	}
}

func TestGitHubGreenPRWaitingDoesNotSpendContinuation(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	if err := store.Complete(ctx, attempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.WorkTaskSnapshot(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Store: store, Now: func() time.Time { return now }}
	waiting := workledger.VerifierResult{AttemptID: attempt.ID, VerifierID: snapshot.VerifierID, CompletionContract: snapshot.CompletionContract, ContractDigest: snapshot.ContractDigest, TaskEvidenceDigest: snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("b", 40), Outcome: "waiting", ReasonCodes: []string{"pending"}, EvidenceRefs: []string{"evidence://github/pending"}}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, waiting); err != nil {
		t.Fatal(err)
	}
	if err := store.WakeWaiting(ctx, item.ID, now); err != nil {
		t.Fatalf("waiting observation did not remain pollable: %v", err)
	}
	_, pollAttempt, ok, err := store.Claim(ctx, now)
	if err != nil || !ok {
		t.Fatalf("poll wake was not claimable: ok=%v err=%v", ok, err)
	}
	if err := store.Complete(ctx, pollAttempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	continuation := waiting
	continuation.AttemptID = pollAttempt.ID
	continuation.Outcome = "continuation_required"
	continuation.ReasonCodes = []string{"failed"}
	continuation.EvidenceRefs = []string{"evidence://github/failed"}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, continuation); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := store.Claim(ctx, now); err != nil || !ok {
		t.Fatalf("waiting observation consumed the one continuation: ok=%v err=%v", ok, err)
	}
}

func TestReassignmentConflictIsDurablyEscalated(t *testing.T) {
	ctx := context.Background()
	store, item, _, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	broker := &fakeBroker{replacement: lease("worker-2", 2), statusState: "conflict", statusErrorCode: "agentd_rebind_conflict"}
	executor := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	if _, err := executor.ReassignAfterLoss(ctx, item.ID, 1); err == nil {
		t.Fatal("broker conflict was not surfaced")
	}
	transition, err := store.Reassignment(ctx, item.ID, 1)
	if err != nil || transition.Phase != workledger.ReassignmentEscalated || transition.BrokerState != "conflict" || transition.ErrorCode != "agentd_rebind_conflict" {
		t.Fatalf("conflict was not durable: transition=%+v err=%v", transition, err)
	}
	if _, err := executor.ReassignAfterLoss(ctx, item.ID, 1); err == nil {
		t.Fatal("durably escalated replay was accepted")
	}
}

func TestNamedVerifierCannotSatisfyWithoutDurableRuntimeCompletion(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	if err := store.Complete(ctx, attempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.WorkTaskSnapshot(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Store: store, Now: func() time.Time { return now }}
	result := workledger.VerifierResult{AttemptID: attempt.ID, VerifierID: snapshot.VerifierID, CompletionContract: snapshot.CompletionContract, ContractDigest: snapshot.ContractDigest, TaskEvidenceDigest: snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("b", 40), Outcome: "satisfied", EvidenceRefs: []string{"evidence://verification/1"}}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, result); err == nil {
		t.Fatal("verifier satisfied work without a durable attempt_completed event")
	}
}

func TestNamedVerifierEscalatesAfterContinuationBudgetIsExhausted(t *testing.T) {
	ctx := context.Background()
	store, item, attempt, now := coordinatorFixture(t)
	defer store.Close()
	if _, err := store.BindSessionLease(ctx, item.ID, "session:"+item.ID, lease("worker-1", 1), now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordCoordinatorEvent(ctx, item.ID, workledger.CoordinatorEvent{Cursor: 1, WorkerID: "worker-1", WorkerFenceEpoch: 1, Kind: "attempt_completed", EvidenceRef: "sha256:runtime", Usage: usage(1, 0, 1, 0)}, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, attempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.WorkTaskSnapshot(ctx, item.ID)
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Store: store, Now: func() time.Time { return now }}
	continuation := workledger.VerifierResult{AttemptID: attempt.ID, VerifierID: snapshot.VerifierID, CompletionContract: snapshot.CompletionContract, ContractDigest: snapshot.ContractDigest, TaskEvidenceDigest: snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("b", 40), Outcome: "continuation_required", ReasonCodes: []string{"missing"}, EvidenceRefs: []string{"evidence://verification/1"}}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, continuation); err != nil {
		t.Fatal(err)
	}
	_, secondAttempt, ok, err := store.Claim(ctx, now)
	if err != nil || !ok {
		t.Fatalf("first continuation was not claimable: ok=%v err=%v", ok, err)
	}
	if err := store.Complete(ctx, secondAttempt.ID, workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting}, now); err != nil {
		t.Fatal(err)
	}
	continuation.AttemptID = secondAttempt.ID
	continuation.EvidenceRefs = []string{"evidence://verification/2"}
	if err := executor.RecordGitHubGreenPRResult(ctx, item.ID, continuation); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := store.Claim(ctx, now); err != nil || ok {
		t.Fatalf("exhausted continuation remained claimable: ok=%v err=%v", ok, err)
	}
	if err := store.WakeWaiting(ctx, item.ID, now); err == nil {
		t.Fatal("exhausted continuation remained waiting instead of escalating")
	}
}
