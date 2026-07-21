package agentsession

import (
	"context"
	"errors"
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

func TestRegisteredLifecycleAcceptsExactTransportAndVerifierPhases(t *testing.T) {
	store, item, attempt, _ := coordinatorFixture(t)
	defer store.Close()
	executor := &Executor{Store: store}
	task, err := executor.registeredTask(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	binding := workledger.SessionBinding{AgentdSessionID: "session", SubmittedTurnID: "turn", ModelEffectID: "model:key", WorkerID: "worker", WorkerStorageLineageID: strings.Repeat("2", 32), WorkerFenceEpoch: 1}
	base := func(cursor int64, phase string) Event {
		return Event{Cursor: cursor, SessionID: binding.AgentdSessionID, TurnID: binding.SubmittedTurnID, ModelEffectID: binding.ModelEffectID, Phase: phase, WorkerID: binding.WorkerID, StorageLineageID: binding.WorkerStorageLineageID, FenceEpoch: binding.WorkerFenceEpoch, AdmissionTaskDigest: task.Digest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest}
	}
	for cursor, phase := range []string{"queued", "authorized", "running", "completed"} {
		if !validEvent(base(int64(cursor+1), phase), binding, task) {
			t.Fatalf("transport phase %q was rejected", phase)
		}
	}
	pending := base(5, "pending")
	pending.Verifier = &VerifierEvent{Phase: "pending", Outcome: "waiting", ContractDigest: task.Snapshot.ContractDigest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("a", 40), Reasons: []VerifierReason{{Code: "waiting"}}, EvidenceRefs: []string{"fixture://github-green-pr-v1/verifier"}}
	if !validEvent(pending, binding, task) {
		t.Fatal("pending verifier phase was rejected")
	}
	green := base(6, "green")
	green.Verifier = &VerifierEvent{Phase: "green", Outcome: "satisfied", ContractDigest: task.Snapshot.ContractDigest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("a", 40), EvidenceRefs: []string{"fixture://github-green-pr-v1/verifier"}}
	if !validEvent(green, binding, task) {
		t.Fatal("green verifier phase was rejected")
	}
	queuedWithVerifier := base(1, "queued")
	queuedWithVerifier.Verifier = green.Verifier
	if validEvent(queuedWithVerifier, binding, task) {
		t.Fatal("verifier coupled to queued transport phase was accepted")
	}
	if validEvent(base(5, "pending"), binding, task) {
		t.Fatal("verifier-free pending phase was accepted")
	}
	malformed := green
	malformed.Verifier = &VerifierEvent{Phase: "pending", Outcome: "satisfied", ContractDigest: task.Snapshot.ContractDigest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("a", 40), EvidenceRefs: []string{"fixture://github-green-pr-v1/verifier"}}
	if validEvent(malformed, binding, task) {
		t.Fatal("malformed phase/verifier coupling was accepted")
	}
}

func TestRegisteredSubmitRecoversLostResponseWithDurableWorkItemKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	store, item, first, now := coordinatorFixtureAt(t, path)
	broker := &lostResponseBroker{lease: fixtureTestLease()}
	executor := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	firstResult, err := executor.Execute(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: first})
	if err != nil || firstResult.Outcome != workledger.OutcomeRetryableFailure {
		t.Fatalf("first execute=%+v err=%v", firstResult, err)
	}
	if err := store.Complete(t.Context(), first.ID, firstResult, now); err != nil {
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
	item, retryAttempt, ok, err := store.Claim(t.Context(), now.Add(time.Second))
	if err != nil || !ok {
		t.Fatalf("retry claim ok=%v err=%v", ok, err)
	}
	executor.Store = store
	retryResult, err := executor.Execute(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: retryAttempt})
	if err != nil || retryResult.Outcome != workledger.OutcomeWaiting {
		t.Fatalf("retry execute=%+v err=%v", retryResult, err)
	}
	if len(broker.submitKeys) != 2 || broker.submitKeys[0] != broker.submitKeys[1] || broker.logicalSubmissions != 1 {
		t.Fatalf("submit keys=%v logical submissions=%d", broker.submitKeys, broker.logicalSubmissions)
	}
	if err := store.Complete(t.Context(), retryAttempt.ID, retryResult, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	item, pollAttempt, ok, err := store.Claim(t.Context(), now.Add(6*time.Second))
	if err != nil || !ok {
		t.Fatalf("poll claim ok=%v err=%v", ok, err)
	}
	if result, err := executor.Execute(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: pollAttempt}); err != nil || result.Outcome != workledger.OutcomeWaiting {
		t.Fatalf("poll execute=%+v err=%v", result, err)
	}
	if len(broker.submitKeys) != 2 {
		t.Fatalf("later poll submitted again: %v", broker.submitKeys)
	}
	binding, err := store.SessionBinding(t.Context(), item.ID)
	if err != nil || binding.RegisteredSubmitKey == "" || binding.RegisteredSubmitKey != binding.SubmittedIdempotencyKey {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
}

func TestMigratedSubmittedBindingResumesPollingWithoutSubmitting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	store, item, attempt, now := coordinatorFixtureAt(t, path)
	lease := fixtureTestLease()
	binding, err := store.BindSessionLease(t.Context(), item.ID, "agentd:binding:"+item.ID, lease, now)
	if err != nil {
		t.Fatal(err)
	}
	const historicalKey = "attempt:historical-submission-key"
	if err := store.RecordRegisteredTurn(t.Context(), item.ID, lease, historicalKey, "historical-session", "historical-turn", "model:"+historicalKey, 7, now); err != nil {
		t.Fatal(err)
	}
	if binding.RegisteredSubmitKey != "" {
		t.Fatalf("pre-migration binding registered key=%q", binding.RegisteredSubmitKey)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	migrated, err := store.SessionBinding(t.Context(), item.ID)
	if err != nil || migrated.RegisteredSubmitKey != historicalKey || migrated.SubmittedIdempotencyKey != historicalKey {
		t.Fatalf("migrated binding=%+v err=%v", migrated, err)
	}
	broker := &resumePollingBroker{lease: lease}
	executor := &Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	result, err := executor.Execute(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil || result.Outcome != workledger.OutcomeWaiting || result.ExternalCorrelation != "historical-turn" {
		t.Fatalf("resume execute=%+v err=%v", result, err)
	}
	if broker.submits != 0 || broker.cursor != 7 || broker.bindingKey != migrated.BindingKey {
		t.Fatalf("resume submits=%d cursor=%d binding=%q", broker.submits, broker.cursor, broker.bindingKey)
	}
	resumed, err := store.SessionBinding(t.Context(), item.ID)
	if err != nil || resumed.RegisteredSubmitKey != historicalKey {
		t.Fatalf("resumed binding=%+v err=%v", resumed, err)
	}
}

type lostResponseBroker struct {
	lease              workledger.SessionLease
	submitKeys         []string
	logicalSubmissions int
	turn               BrokerTurn
}

type resumePollingBroker struct {
	lease      workledger.SessionLease
	submits    int
	bindingKey string
	cursor     int64
}

func (b *resumePollingBroker) Acquire(_ context.Context, _ AcquireRequest) (workledger.SessionLease, error) {
	return b.lease, nil
}
func (b *resumePollingBroker) SubmitTurn(context.Context, SubmitTurnRequest) (BrokerTurn, error) {
	b.submits++
	return BrokerTurn{}, errors.New("submitted binding must only poll")
}
func (b *resumePollingBroker) StreamEvents(_ context.Context, request StreamEventsRequest) (BrokerEvents, error) {
	b.bindingKey, b.cursor = request.BindingKey, request.Cursor
	return BrokerEvents{Lease: b.lease}, nil
}
func (b *resumePollingBroker) Reassign(context.Context, ReassignRequest) (BrokerReassignment, error) {
	return BrokerReassignment{}, errors.New("unexpected reassignment")
}
func (b *resumePollingBroker) ReassignmentStatus(context.Context, ReassignmentStatusRequest) (BrokerReassignmentStatus, error) {
	return BrokerReassignmentStatus{}, errors.New("unexpected reassignment")
}

func (b *lostResponseBroker) Acquire(_ context.Context, _ AcquireRequest) (workledger.SessionLease, error) {
	return b.lease, nil
}
func (b *lostResponseBroker) SubmitTurn(_ context.Context, request SubmitTurnRequest) (BrokerTurn, error) {
	b.submitKeys = append(b.submitKeys, request.IdempotencyKey)
	if b.turn.TurnID == "" {
		b.logicalSubmissions++
		b.turn = BrokerTurn{SessionID: "lost-session", TurnID: "lost-turn", ModelEffectID: "model:" + request.IdempotencyKey, Cursor: 1, Lease: b.lease}
		return BrokerTurn{}, errors.New("response lost after broker acceptance")
	}
	if request.IdempotencyKey != b.submitKeys[0] {
		return BrokerTurn{}, errors.New("broker rejected changed replay key")
	}
	return b.turn, nil
}
func (b *lostResponseBroker) StreamEvents(_ context.Context, _ StreamEventsRequest) (BrokerEvents, error) {
	return BrokerEvents{Lease: b.lease}, nil
}
func (b *lostResponseBroker) Reassign(context.Context, ReassignRequest) (BrokerReassignment, error) {
	return BrokerReassignment{}, errors.New("unexpected reassignment")
}
func (b *lostResponseBroker) ReassignmentStatus(context.Context, ReassignmentStatusRequest) (BrokerReassignmentStatus, error) {
	return BrokerReassignmentStatus{}, errors.New("unexpected reassignment")
}

func fixtureTestLease() workledger.SessionLease {
	return workledger.SessionLease{WorkerID: "fixture-worker", AuthorityProfile: authorityProfile, ProfileVersion: "fixture-profile-v1", PolicyDigest: strings.Repeat("a", 64), SessionLineageID: strings.Repeat("1", 32), WorkerStorageLineageID: strings.Repeat("2", 32), WorkerFenceEpoch: 1}
}

func TestRecordVerifierEventRejectsDigestsOutsideRegisteredTaskSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Event)
	}{
		{
			name: "wrong verifier contract digest",
			mutate: func(event *Event) {
				event.Verifier.ContractDigest = "sha256:" + strings.Repeat("c", 64)
			},
		},
		{
			name: "wrong verifier task evidence digest",
			mutate: func(event *Event) {
				event.Verifier.TaskEvidenceDigest = "sha256:" + strings.Repeat("c", 64)
			},
		},
		{
			name: "wrong outer task evidence digest",
			mutate: func(event *Event) {
				event.TaskEvidenceDigest = "sha256:" + strings.Repeat("c", 64)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, item, attempt, now := coordinatorFixture(t)
			defer store.Close()
			executor := &Executor{Store: store, Now: func() time.Time { return now }}
			task, err := executor.registeredTask(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
			if err != nil {
				t.Fatal(err)
			}
			event := Event{
				AdmissionTaskDigest: task.Digest,
				TaskEvidenceDigest:  task.Snapshot.TaskEvidenceDigest,
				Verifier: &VerifierEvent{
					Phase:              "green",
					Outcome:            "satisfied",
					ContractDigest:     task.Snapshot.ContractDigest,
					TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest,
					HeadRevision:       strings.Repeat("a", 40),
					EvidenceRefs:       []string{"fixture://github-green-pr-v1/verifier"},
				},
			}
			tc.mutate(&event)
			if err := executor.recordVerifierEvent(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt}, workledger.SessionBinding{}, event); err == nil {
				t.Fatal("well-formed verifier digests outside the registered snapshot were accepted")
			}

			event.TaskEvidenceDigest = task.Snapshot.TaskEvidenceDigest
			event.Verifier.ContractDigest = task.Snapshot.ContractDigest
			event.Verifier.TaskEvidenceDigest = task.Snapshot.TaskEvidenceDigest
			event.Verifier.HeadRevision = strings.Repeat("b", 40)
			if err := executor.recordVerifierEvent(t.Context(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt}, workledger.SessionBinding{}, event); err != nil {
				t.Fatalf("rejected verifier result was persisted: %v", err)
			}
		})
	}
}
