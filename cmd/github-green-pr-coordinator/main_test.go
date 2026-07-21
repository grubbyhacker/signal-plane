package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/agentsession"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestFixtureRouteRegistersExactDisabledCoordinatorContract(t *testing.T) {
	store, err := workledger.Open(filepath.Join(t.TempDir(), "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := workledger.NewRegistry()
	if err := registry.Register(&agentsession.Executor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterTask(agentsession.GitHubGreenPRTask{}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(context.Background(), agentsession.GitHubGreenPRFixtureRoute(), registry, time.Now().UTC())
	if err != nil || snapshot.TaskKind != agentsession.GitHubGreenPRTaskKind || snapshot.ExecutorID != agentsession.ExecutorID {
		t.Fatalf("route=%+v err=%v", snapshot, err)
	}
}

type fixtureBroker struct {
	lease   workledger.SessionLease
	acquire agentsession.AcquireRequest
	turns   []agentsession.SubmitTurnRequest
	events  []agentsession.Event
}

func (b *fixtureBroker) Acquire(_ context.Context, request agentsession.AcquireRequest) (workledger.SessionLease, error) {
	b.acquire = request
	return b.lease, nil
}
func (b *fixtureBroker) Reassign(context.Context, agentsession.ReassignRequest) (agentsession.BrokerReassignment, error) {
	return agentsession.BrokerReassignment{}, nil
}
func (b *fixtureBroker) ReassignmentStatus(context.Context, agentsession.ReassignmentStatusRequest) (agentsession.BrokerReassignmentStatus, error) {
	return agentsession.BrokerReassignmentStatus{}, nil
}
func (b *fixtureBroker) CreateSession(context.Context, agentsession.CreateSessionRequest) (agentsession.BrokerSession, error) {
	return agentsession.BrokerSession{SessionID: "fixture-session", Lease: b.lease}, nil
}
func (b *fixtureBroker) SubmitTurn(_ context.Context, request agentsession.SubmitTurnRequest) (agentsession.BrokerTurn, error) {
	b.turns = append(b.turns, request)
	return agentsession.BrokerTurn{TurnID: "fixture-turn", Lease: b.lease}, nil
}
func (b *fixtureBroker) StreamEvents(context.Context, agentsession.StreamEventsRequest) (agentsession.BrokerEvents, error) {
	return agentsession.BrokerEvents{Lease: b.lease, Events: b.events}, nil
}

func TestFixtureAdmissionRunsSubmitThenScheduledPollWithoutAnotherTurn(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	store, err := workledger.Open(filepath.Join(t.TempDir(), "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry, err := agentsession.RegisterGitHubGreenPRFixture(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := agentsession.AdmitGitHubGreenPRFixture(ctx, store, registry, now)
	if err != nil || admitted.Duplicate {
		t.Fatalf("admission=%+v err=%v", admitted, err)
	}
	if duplicate, err := agentsession.AdmitGitHubGreenPRFixture(ctx, store, registry, now.Add(time.Second)); err != nil || !duplicate.Duplicate || duplicate.WorkItem.ID != admitted.WorkItem.ID {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}

	broker := &fixtureBroker{lease: fixtureLease()}
	executor := &agentsession.Executor{Store: store, Broker: broker, Now: func() time.Time { return now }}
	item, first, ok, err := store.Claim(ctx, now)
	if err != nil || !ok || item.ID != admitted.WorkItem.ID {
		t.Fatalf("first claim item=%+v attempt=%+v ok=%v err=%v", item, first, ok, err)
	}
	firstResult, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: first})
	if err != nil || firstResult.Outcome != workledger.OutcomeWaiting {
		t.Fatalf("first execute=%+v err=%v", firstResult, err)
	}
	if err := store.Complete(ctx, first.ID, firstResult, now); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := store.Claim(ctx, now); err != nil || ok {
		t.Fatalf("waiting work was claimed before durable schedule: ok=%v err=%v", ok, err)
	}

	pollAt := now.Add(5 * time.Second)
	item, poll, ok, err := store.Claim(ctx, pollAt)
	if err != nil || !ok || poll.ID == first.ID || poll.IdempotencyKey == first.IdempotencyKey {
		t.Fatalf("poll claim attempt=%+v first=%+v ok=%v err=%v", poll, first, ok, err)
	}
	task := broker.acquire.RegisteredTask
	broker.events = []agentsession.Event{
		{Cursor: 1, Kind: "attempt_completed", SessionID: "fixture-session", EvidenceRef: "sha256:runtime", Usage: workledger.Usage{InputTokens: 1, OutputTokens: 1, TotalTokens: 2}},
		{Cursor: 2, Kind: "verifier_evaluated", SessionID: "fixture-session", TurnID: "fixture-turn", AttemptID: poll.ID, EvidenceRef: "sha256:verifier", Verifier: &agentsession.VerifierEvent{WorkItemID: item.ID, AttemptID: poll.ID, AdmissionTaskDigest: task.Digest, VerifierID: agentsession.GitHubGreenPRContract, CompletionContract: agentsession.GitHubGreenPRContract, ContractDigest: task.Snapshot.ContractDigest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("a", 40), EvaluationRevision: strings.Repeat("b", 40), Outcome: "satisfied", EvidenceRefs: []string{"fixture://github-green-pr-v1/verifier"}, WorkerID: broker.lease.WorkerID, SessionID: "fixture-session", FenceEpoch: broker.lease.WorkerFenceEpoch}},
	}
	pollResult, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: poll})
	if err != nil || pollResult.Outcome != workledger.OutcomeWaiting {
		t.Fatalf("poll execute=%+v err=%v", pollResult, err)
	}
	if err := store.Complete(ctx, poll.ID, pollResult, pollAt); err != nil {
		t.Fatal(err)
	}
	if len(broker.turns) != 1 || broker.turns[0].IdempotencyKey != first.IdempotencyKey {
		t.Fatalf("submit turns=%+v, want only first Signal attempt", broker.turns)
	}
	binding, err := store.SessionBinding(ctx, item.ID)
	if err != nil || binding.SubmittedIdempotencyKey != first.IdempotencyKey || binding.ModelEffectID != "model:"+first.IdempotencyKey {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
	if _, _, ok, err := store.Claim(ctx, pollAt.Add(time.Minute)); err != nil || ok {
		t.Fatalf("terminal verifier event left work claimable: ok=%v err=%v", ok, err)
	}
}

func fixtureLease() workledger.SessionLease {
	return workledger.SessionLease{WorkerID: "fixture-worker", AuthorityProfile: "general-writer-v1", ProfileVersion: "fixture-profile-v1", PolicyDigest: strings.Repeat("a", 64), SessionLineageID: strings.Repeat("1", 32), WorkerStorageLineageID: strings.Repeat("2", 32), WorkerFenceEpoch: 1}
}
