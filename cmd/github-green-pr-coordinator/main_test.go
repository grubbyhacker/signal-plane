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
	streams []agentsession.StreamEventsRequest
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
	return agentsession.BrokerTurn{SessionID: "fixture-session", TurnID: "fixture-turn", ModelEffectID: "model:" + request.IdempotencyKey, Cursor: 1, Lease: b.lease}, nil
}
func (b *fixtureBroker) StreamEvents(_ context.Context, request agentsession.StreamEventsRequest) (agentsession.BrokerEvents, error) {
	b.streams = append(b.streams, request)
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
	clock := now
	executor := &agentsession.Executor{Store: store, Broker: broker, Now: func() time.Time { return clock }}
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
	clock = pollAt
	item, poll, ok, err := store.Claim(ctx, pollAt)
	if err != nil || !ok || poll.ID == first.ID || poll.IdempotencyKey == first.IdempotencyKey {
		t.Fatalf("poll claim attempt=%+v first=%+v ok=%v err=%v", poll, first, ok, err)
	}
	task := broker.acquire.RegisteredTask
	broker.events = []agentsession.Event{{Cursor: 2, Attempt: 0, SessionID: "fixture-session", TurnID: "fixture-turn", ModelEffectID: "model:" + first.IdempotencyKey, Phase: "queued", WorkerID: broker.lease.WorkerID, StorageLineageID: broker.lease.WorkerStorageLineageID, FenceEpoch: broker.lease.WorkerFenceEpoch, AdmissionTaskDigest: task.Digest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest}}
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
	item, secondPoll, ok, err := store.Claim(ctx, pollAt.Add(5*time.Second))
	if err != nil || !ok || secondPoll.ID == poll.ID {
		t.Fatalf("second poll=%+v ok=%v err=%v", secondPoll, ok, err)
	}
	clock = pollAt.Add(5 * time.Second)
	broker.events = []agentsession.Event{{Cursor: 3, Attempt: 1, SessionID: "fixture-session", TurnID: "fixture-turn", ModelEffectID: "model:" + first.IdempotencyKey, Phase: "red", WorkerID: broker.lease.WorkerID, StorageLineageID: broker.lease.WorkerStorageLineageID, FenceEpoch: broker.lease.WorkerFenceEpoch, AdmissionTaskDigest: task.Digest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, Verifier: &agentsession.VerifierEvent{Phase: "red", ContractDigest: task.Snapshot.ContractDigest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("a", 40), Outcome: "continuation", Reasons: []agentsession.VerifierReason{{Code: "missing"}}, EvidenceRefs: []string{"fixture://github-green-pr-v1/verifier"}}}}
	secondResult, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: secondPoll})
	if err != nil || secondResult.Outcome != workledger.OutcomeWaiting {
		t.Fatalf("second execute=%+v err=%v", secondResult, err)
	}
	if err := store.Complete(ctx, secondPoll.ID, secondResult, pollAt.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(broker.turns) != 1 || len(broker.streams) != 3 || broker.streams[1].Cursor != 1 || broker.streams[2].Cursor != 2 {
		t.Fatalf("turns=%+v streams=%+v", broker.turns, broker.streams)
	}
	item, terminalPoll, ok, err := store.Claim(ctx, pollAt.Add(10*time.Second))
	if err != nil || !ok || terminalPoll.ID == secondPoll.ID {
		t.Fatalf("terminal poll=%+v ok=%v err=%v", terminalPoll, ok, err)
	}
	clock = pollAt.Add(10 * time.Second)
	broker.events = []agentsession.Event{{Cursor: 4, Attempt: 2, SessionID: "fixture-session", TurnID: "fixture-turn", ModelEffectID: "model:" + first.IdempotencyKey, Phase: "green", WorkerID: broker.lease.WorkerID, StorageLineageID: broker.lease.WorkerStorageLineageID, FenceEpoch: broker.lease.WorkerFenceEpoch, AdmissionTaskDigest: task.Digest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, Verifier: &agentsession.VerifierEvent{Phase: "green", ContractDigest: task.Snapshot.ContractDigest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, HeadRevision: strings.Repeat("a", 40), Outcome: "satisfied", EvidenceRefs: []string{"fixture://github-green-pr-v1/verifier"}}}}
	terminalResult, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: terminalPoll})
	if err != nil || terminalResult.Outcome != workledger.OutcomeWaiting {
		t.Fatalf("terminal execute=%+v err=%v", terminalResult, err)
	}
	if err := store.Complete(ctx, terminalPoll.ID, terminalResult, clock); err != nil {
		t.Fatal(err)
	}
	if len(broker.turns) != 1 || len(broker.streams) != 4 || broker.streams[3].Cursor != 3 {
		t.Fatalf("continuation submitted/restarted a turn: turns=%+v streams=%+v", broker.turns, broker.streams)
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
