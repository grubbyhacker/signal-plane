// Package agentsession contains the deliberately narrow PR 9 coordinator.
// Concrete gh-agent-broker and agentd HTTP clients are intentionally not part
// of this package until their independently reviewed contracts are available.
package agentsession

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const ExecutorID = "agent_session_v1"
const authorityProfile = "general-writer-v1"
const runtimeAdapter = "codex_adapter_v1"

type Broker interface {
	Acquire(context.Context, AcquireRequest) (workledger.SessionLease, error)
	// Reassign accepts only a lost predecessor identity and fence. The broker,
	// not this coordinator, chooses and records the replacement worker.
	Reassign(context.Context, ReassignRequest) (workledger.SessionLease, error)
}
type AcquireRequest struct{ BindingKey, AuthorityProfile, IdempotencyKey string }
type ReassignRequest struct {
	BindingKey, PredecessorWorker string
	PredecessorEpoch              int64
}

type Agentd interface {
	CreateSession(context.Context, SessionRequest) (string, error)
	SubmitTurn(context.Context, TurnRequest) (string, error)
	StreamEvents(context.Context, EventRequest) ([]Event, error)
}
type SessionRequest struct {
	BindingKey, WorkerID, AuthorityProfile, RuntimeAdapter, IdempotencyKey string
	FenceEpoch                                                             int64
}
type TurnRequest struct {
	SessionID, BindingKey, WorkerID, EvidenceRef, EvidenceDigest, IdempotencyKey string
	FenceEpoch                                                                   int64
}
type EventRequest struct {
	SessionID, BindingKey, WorkerID, Cursor string
	FenceEpoch                              int64
}
type Event struct {
	Cursor, Kind, EvidenceRef string
	InputTokens, OutputTokens int64
}

type Executor struct {
	Store  *workledger.Store
	Broker Broker
	Agentd Agentd
	Now    func() time.Time
}

func (e *Executor) Descriptor() workledger.ExecutorDescriptor {
	return workledger.ExecutorDescriptor{ID: ExecutorID, Kind: workledger.ExecutorAgentSession, Version: "v1"}
}
func (e *Executor) now() time.Time {
	if e.Now != nil {
		return e.Now().UTC()
	}
	return time.Now().UTC()
}

func (e *Executor) Execute(ctx context.Context, request workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	if e.Store == nil || e.Broker == nil || e.Agentd == nil {
		return workledger.ExecutorResult{}, errors.New("agent coordinator requires ledger, broker, and agentd contracts")
	}
	binding, err := e.acquire(ctx, request)
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "coordinator_acquire", SanitizedError: "authority session unavailable"}, nil
	}
	if binding.AgentdSessionID == "" {
		sessionID, createErr := e.Agentd.CreateSession(ctx, SessionRequest{BindingKey: binding.BindingKey, WorkerID: binding.WorkerID, AuthorityProfile: binding.AuthorityProfile, RuntimeAdapter: runtimeAdapter, IdempotencyKey: binding.BindingKey, FenceEpoch: binding.FenceEpoch})
		if createErr != nil || sessionID == "" {
			return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_create", SanitizedError: "agent session create unavailable"}, nil
		}
		if err := e.Store.SetAgentdSession(ctx, request.WorkItem.ID, binding.WorkerID, binding.FenceEpoch, sessionID, e.now()); err != nil {
			return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "coordinator_fence", SanitizedError: "session binding changed"}, nil
		}
		binding.AgentdSessionID = sessionID
	}
	turnID, err := e.Agentd.SubmitTurn(ctx, TurnRequest{SessionID: binding.AgentdSessionID, BindingKey: binding.BindingKey, WorkerID: binding.WorkerID, FenceEpoch: binding.FenceEpoch, EvidenceRef: request.WorkItem.ID, EvidenceDigest: request.Attempt.RequestedOperationDigest, IdempotencyKey: request.Attempt.IdempotencyKey})
	if err != nil || turnID == "" {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_submit", SanitizedError: "agent turn submit unavailable"}, nil
	}
	events, err := e.Agentd.StreamEvents(ctx, EventRequest{SessionID: binding.AgentdSessionID, BindingKey: binding.BindingKey, WorkerID: binding.WorkerID, FenceEpoch: binding.FenceEpoch, Cursor: binding.EventCursor})
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_events", SanitizedError: "agent event stream unavailable"}, nil
	}
	result := workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting, ExternalCorrelation: turnID}
	for _, event := range events {
		if !validEvent(event) {
			return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_protocol", SanitizedError: "agent event invalid"}, nil
		}
		_, err := e.Store.RecordCoordinatorEvent(ctx, request.WorkItem.ID, workledger.CoordinatorEvent{Cursor: event.Cursor, WorkerID: binding.WorkerID, FenceEpoch: binding.FenceEpoch, Kind: event.Kind, EvidenceRef: event.EvidenceRef, InputTokens: event.InputTokens, OutputTokens: event.OutputTokens}, e.now())
		if err != nil {
			return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "coordinator_fence", SanitizedError: "agent event rejected"}, nil
		}
		switch event.Kind {
		case "runtime_succeeded":
			result = workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted, ExternalCorrelation: turnID, ResultDigest: event.EvidenceRef}
		case "runtime_waiting":
			result = workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting, ExternalCorrelation: turnID}
		}
	}
	return result, nil
}

func validEvent(e Event) bool {
	return e.Cursor != "" && (e.Kind == "evidence" || e.Kind == "usage" || e.Kind == "runtime_succeeded" || e.Kind == "runtime_waiting") && e.InputTokens >= 0 && e.OutputTokens >= 0
}

func (e *Executor) acquire(ctx context.Context, request workledger.ExecutorRequest) (workledger.SessionBinding, error) {
	binding, err := e.Store.SessionBinding(ctx, request.WorkItem.ID)
	if err == nil {
		return binding, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return workledger.SessionBinding{}, err
	}
	lease, err := e.Broker.Acquire(ctx, AcquireRequest{BindingKey: "session:" + request.WorkItem.ID, AuthorityProfile: authorityProfile, IdempotencyKey: request.Attempt.IdempotencyKey})
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	if lease.AuthorityProfile != authorityProfile {
		return workledger.SessionBinding{}, errors.New("broker returned unauthorized profile")
	}
	return e.Store.BindSessionLease(ctx, request.WorkItem.ID, "session:"+request.WorkItem.ID, lease, e.now())
}

// ReassignAfterLoss performs the broker-recorded replacement followed by one
// durable compare-and-swap. A process crash before CAS leaves the predecessor
// intact; a crash after CAS fences every predecessor response.
func (e *Executor) ReassignAfterLoss(ctx context.Context, workItemID string) (workledger.SessionBinding, error) {
	if e.Store == nil || e.Broker == nil {
		return workledger.SessionBinding{}, errors.New("agent coordinator requires ledger and broker")
	}
	previous, err := e.Store.SessionBinding(ctx, workItemID)
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	next, err := e.Broker.Reassign(ctx, ReassignRequest{BindingKey: previous.BindingKey, PredecessorWorker: previous.WorkerID, PredecessorEpoch: previous.FenceEpoch})
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	if next.WorkerID == previous.WorkerID {
		return workledger.SessionBinding{}, fmt.Errorf("broker replacement did not change worker")
	}
	return e.Store.ReassignSession(ctx, workItemID, previous.WorkerID, previous.FenceEpoch, next, e.now())
}
