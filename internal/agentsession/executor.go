// Package agentsession coordinates a durable broker-mediated agentd session.
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

// Broker is the only authority that addresses agentd. Signal Plane supplies
// durable binding/evidence identifiers, never a worker, profile, or runtime.
type Broker interface {
	Acquire(context.Context, AcquireRequest) (workledger.SessionLease, error)
	Reassign(context.Context, ReassignRequest) (BrokerReassignment, error)
	ReassignmentStatus(context.Context, ReassignmentStatusRequest) (BrokerReassignmentStatus, error)
	CreateSession(context.Context, CreateSessionRequest) (BrokerSession, error)
	SubmitTurn(context.Context, SubmitTurnRequest) (BrokerTurn, error)
	StreamEvents(context.Context, StreamEventsRequest) (BrokerEvents, error)
}
type AcquireRequest struct {
	BindingKey, AuthorityProfile, IdempotencyKey string
	RegisteredTask                               RegisteredTask
}
type ReassignRequest struct {
	BindingKey, SessionLineageID, PredecessorWorker string
	PredecessorEpoch                                int64
	IdempotencyKey                                  string
}
type ReassignmentStatusRequest struct {
	BindingKey       string
	PredecessorEpoch int64
}
type CreateSessionRequest struct{ BindingKey string }
type SubmitTurnRequest struct{ BindingKey, Prompt, IdempotencyKey string }
type StreamEventsRequest struct {
	BindingKey string
	Cursor     int64
}
type BrokerSession struct {
	SessionID string
	Lease     workledger.SessionLease
}
type BrokerTurn struct {
	TurnID string
	Lease  workledger.SessionLease
}
type BrokerEvents struct {
	Lease  workledger.SessionLease
	Events []Event
}
type BrokerReassignment struct {
	Lease workledger.SessionLease
	State string
}
type BrokerReassignmentStatus struct {
	Lease                workledger.SessionLease
	PredecessorWorker    string
	PredecessorEpoch     int64
	RebindIdempotencyKey string
	State, ErrorCode     string
}
type Event struct {
	Cursor            int64
	Kind, EvidenceRef string
	Usage             workledger.Usage
}

type Executor struct {
	Store  *workledger.Store
	Broker Broker
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
	if e.Store == nil || e.Broker == nil {
		return workledger.ExecutorResult{}, errors.New("agent coordinator requires ledger and broker contracts")
	}
	if err := e.Store.RoutingReady(ctx, request.WorkItem.ID); err != nil {
		return retry("coordinator_reassignment", "session reassignment is incomplete"), nil
	}
	binding, err := e.acquire(ctx, request)
	if err != nil {
		return retry("coordinator_acquire", "authority session unavailable"), nil
	}
	if binding.AgentdSessionID == "" {
		created, err := e.Broker.CreateSession(ctx, CreateSessionRequest{BindingKey: binding.BindingKey})
		if err != nil || created.SessionID == "" || !sameLease(binding, created.Lease) {
			return retry("agentd_create", "broker session create unavailable"), nil
		}
		if err := e.Store.SetAgentdSession(ctx, request.WorkItem.ID, created.Lease, created.SessionID, e.now()); err != nil {
			return retry("coordinator_fence", "session binding changed"), nil
		}
		binding.AgentdSessionID = created.SessionID
	}
	prompt, err := e.registeredPrompt(ctx, request)
	if err != nil {
		return retry("registered_task", "registered task snapshot unavailable"), nil
	}
	turn, err := e.Broker.SubmitTurn(ctx, SubmitTurnRequest{BindingKey: binding.BindingKey, Prompt: prompt, IdempotencyKey: request.Attempt.IdempotencyKey})
	if err != nil || turn.TurnID == "" || !sameLease(binding, turn.Lease) {
		return retry("agentd_submit", "broker turn submit unavailable"), nil
	}
	batch, err := e.Broker.StreamEvents(ctx, StreamEventsRequest{BindingKey: binding.BindingKey, Cursor: binding.EventCursor})
	if err != nil || !sameLease(binding, batch.Lease) {
		return retry("agentd_events", "broker event stream unavailable"), nil
	}
	result := workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting, ExternalCorrelation: turn.TurnID}
	for _, event := range batch.Events {
		if !validEvent(event) {
			return retry("agentd_protocol", "agent event invalid"), nil
		}
		if _, err := e.Store.RecordCoordinatorEvent(ctx, request.WorkItem.ID, workledger.CoordinatorEvent{Cursor: event.Cursor, WorkerID: binding.WorkerID, WorkerFenceEpoch: binding.WorkerFenceEpoch, Kind: event.Kind, EvidenceRef: event.EvidenceRef, Usage: event.Usage}, e.now()); err != nil {
			return retry("coordinator_fence", "agent event rejected"), nil
		}
		if event.Kind == "attempt_completed" {
			result.ResultDigest = event.EvidenceRef
		}
	}
	// Runtime success is durable evidence only. PR 10 owns semantic verification
	// and is the only boundary allowed to complete the work item.
	return result, nil
}
func retry(classification, message string) workledger.ExecutorResult {
	return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: classification, SanitizedError: message}
}
func validEvent(e Event) bool {
	return e.Cursor > 0 && knownAgentdEvent(e.Kind) && e.EvidenceRef != "" && e.Usage.Valid()
}
func sameLease(b workledger.SessionBinding, l workledger.SessionLease) bool {
	return l.WorkerID == b.WorkerID && l.WorkerFenceEpoch == b.WorkerFenceEpoch && l.ProfileVersion == b.ProfileVersion && l.PolicyDigest == b.PolicyDigest && l.SessionLineageID == b.SessionLineageID && l.WorkerStorageLineageID == b.WorkerStorageLineageID && l.AuthorityProfile == b.AuthorityProfile
}

func (e *Executor) acquire(ctx context.Context, request workledger.ExecutorRequest) (workledger.SessionBinding, error) {
	b, err := e.Store.SessionBinding(ctx, request.WorkItem.ID)
	if err == nil {
		return b, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return workledger.SessionBinding{}, err
	}
	task, err := e.registeredTask(ctx, request)
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	bindingKey := "session:" + task.Source.WorkItemID
	lease, err := e.Broker.Acquire(ctx, AcquireRequest{BindingKey: bindingKey, AuthorityProfile: authorityProfile, IdempotencyKey: request.Attempt.IdempotencyKey, RegisteredTask: task})
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	if lease.AuthorityProfile != authorityProfile {
		return workledger.SessionBinding{}, errors.New("broker returned unauthorized profile")
	}
	return e.Store.BindSessionLease(ctx, task.Source.WorkItemID, bindingKey, lease, e.now())
}

func (e *Executor) ReassignAfterLoss(ctx context.Context, workItemID string, predecessorEpoch int64) (workledger.SessionBinding, error) {
	if e.Store == nil || e.Broker == nil {
		return workledger.SessionBinding{}, errors.New("agent coordinator requires ledger and broker")
	}
	previous, err := e.Store.SessionBinding(ctx, workItemID)
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	key := reassignmentIdempotencyKey(previous.BindingKey, predecessorEpoch)
	transition, err := e.Store.BeginReassignment(ctx, workItemID, predecessorEpoch, key, e.now())
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	if transition.Phase == workledger.ReassignmentCoordinatorCommitted {
		if previous.WorkerID != transition.SuccessorWorkerID || previous.WorkerFenceEpoch != transition.SuccessorFenceEpoch || previous.SessionLineageID != transition.SessionLineageID || previous.WorkerStorageLineageID != transition.StorageLineageID {
			return workledger.SessionBinding{}, errors.New("committed reassignment conflicts with active binding")
		}
		return previous, nil
	}
	if transition.Phase == workledger.ReassignmentEscalated {
		return workledger.SessionBinding{}, fmt.Errorf("reassignment is durably escalated: %s", transition.ErrorCode)
	}
	if transition.Phase == workledger.ReassignmentRequested {
		brokerResult, err := e.Broker.Reassign(ctx, ReassignRequest{BindingKey: previous.BindingKey, SessionLineageID: transition.SessionLineageID, PredecessorWorker: transition.PredecessorWorkerID, PredecessorEpoch: transition.PredecessorFenceEpoch, IdempotencyKey: key})
		if err != nil {
			return workledger.SessionBinding{}, err
		}
		if brokerResult.Lease.WorkerID == transition.PredecessorWorkerID {
			return workledger.SessionBinding{}, e.escalateReassignment(ctx, workItemID, predecessorEpoch, brokerResult.State, "successor_worker_unchanged")
		}
		if err := e.Store.RecordBrokerReassignment(ctx, workItemID, predecessorEpoch, brokerResult.Lease, brokerResult.State, e.now()); err != nil {
			return workledger.SessionBinding{}, e.escalateReassignment(ctx, workItemID, predecessorEpoch, brokerResult.State, "broker_successor_conflict")
		}
	}
	status, err := e.Broker.ReassignmentStatus(ctx, ReassignmentStatusRequest{BindingKey: previous.BindingKey, PredecessorEpoch: predecessorEpoch})
	if err != nil {
		return workledger.SessionBinding{}, err
	}
	if status.RebindIdempotencyKey == "" || status.PredecessorWorker != transition.PredecessorWorkerID || status.PredecessorEpoch != predecessorEpoch {
		return workledger.SessionBinding{}, e.escalateReassignment(ctx, workItemID, predecessorEpoch, status.State, "broker_status_identity_conflict")
	}
	if status.State != "confirmed" {
		if status.State == "conflict" || status.State == "legacy_unresolved" {
			code := status.ErrorCode
			if code == "" {
				code = "broker_" + status.State
			}
			return workledger.SessionBinding{}, e.escalateReassignment(ctx, workItemID, predecessorEpoch, status.State, code)
		}
		return workledger.SessionBinding{}, fmt.Errorf("broker reassignment adoption is %s", status.State)
	}
	if transition.Phase == workledger.ReassignmentRequested || transition.Phase == workledger.ReassignmentBrokerCommitted {
		if err := e.Store.RecordBrokerReassignment(ctx, workItemID, predecessorEpoch, status.Lease, status.State, e.now()); err != nil {
			return workledger.SessionBinding{}, e.escalateReassignment(ctx, workItemID, predecessorEpoch, status.State, "broker_successor_conflict")
		}
	}
	if err := e.Store.RecordAgentdAdopted(ctx, workItemID, predecessorEpoch, status.State, status.RebindIdempotencyKey, e.now()); err != nil {
		return workledger.SessionBinding{}, e.escalateReassignment(ctx, workItemID, predecessorEpoch, status.State, "agentd_adoption_conflict")
	}
	return e.Store.CommitReassignment(ctx, workItemID, predecessorEpoch, e.now())
}

func (e *Executor) escalateReassignment(ctx context.Context, workItemID string, predecessorEpoch int64, brokerState, code string) error {
	if err := e.Store.EscalateReassignment(ctx, workItemID, predecessorEpoch, brokerState, code, e.now()); err != nil {
		return fmt.Errorf("reassignment conflict %s could not be persisted: %w", code, err)
	}
	return fmt.Errorf("reassignment durably escalated: %s", code)
}

func reassignmentIdempotencyKey(bindingKey string, predecessorEpoch int64) string {
	return fmt.Sprintf("signal-plane:agent-session:reassign:v1:%s:%d", bindingKey, predecessorEpoch)
}
