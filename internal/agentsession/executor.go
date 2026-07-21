// Package agentsession coordinates a durable broker-mediated agentd session.
package agentsession

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const ExecutorID = "agent_session_v1"
const authorityProfile = "general-writer-v1"
const pollInterval = 5 * time.Second
const waitDeadline = 30 * time.Minute

// Broker is the only authority that addresses agentd. Signal Plane supplies
// durable binding/evidence identifiers, never a worker, profile, or runtime.
type Broker interface {
	Acquire(context.Context, AcquireRequest) (workledger.SessionLease, error)
	Reassign(context.Context, ReassignRequest) (BrokerReassignment, error)
	ReassignmentStatus(context.Context, ReassignmentStatusRequest) (BrokerReassignmentStatus, error)
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

// SubmitTurnRequest is the source-closed registered-lifecycle command. The
// broker forwards this exact shape to agentd; no prompt or continuation input
// is selectable by the coordinator.
type SubmitTurnRequest struct {
	Version, IdempotencyKey, TaskKind, AdmissionTaskDigest, TaskEvidenceDigest string
	Parameters                                                                 []byte
}

// Retained for ancillary broker lifecycle helpers; the coordinator does not
// create a session before the registered lifecycle submission.
type CreateSessionRequest struct{ BindingKey string }
type BrokerSession struct {
	SessionID string
	Lease     workledger.SessionLease
}
type StreamEventsRequest struct {
	BindingKey string
	Cursor     int64
}
type BrokerTurn struct {
	SessionID, TurnID, ModelEffectID string
	Cursor                           int64
	Lease                            workledger.SessionLease
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
	Cursor, Attempt, FenceEpoch                                         int64
	SessionID, TurnID, ModelEffectID, Phase, WorkerID, StorageLineageID string
	AdmissionTaskDigest, TaskEvidenceDigest                             string
	Verifier                                                            *VerifierEvent
	// Ignored legacy envelope fields retained for source compatibility.
	Kind, EvidenceRef, AttemptID string
	Usage                        workledger.Usage
}

// VerifierEvent is the authenticated agentd verdict.  It deliberately keeps
// the identity fields separate from EvidenceRef: verdict semantics must never
// be reconstructed from a hash of an opaque event payload.
type VerifierEvent struct {
	Phase              string           `json:"phase"`
	Outcome            string           `json:"outcome"`
	ContractDigest     string           `json:"contractDigest"`
	TaskEvidenceDigest string           `json:"taskEvidenceDigest"`
	HeadRevision       string           `json:"headRevision"`
	Reasons            []VerifierReason `json:"reasons"`
	EvidenceRefs       []string         `json:"evidenceRefs"`
}

// VerifierReason is the package-owned reason statement associated with a
// registered verifier result.
type VerifierReason struct {
	Code        string `json:"code"`
	EvidenceRef string `json:"evidenceRef,omitempty"`
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
	binding, err = e.Store.BindRegisteredSubmitKey(ctx, request.WorkItem.ID, bindingLease(binding), registeredSubmitKey(binding), e.now())
	if err != nil {
		return retry("coordinator_fence", "registered submit key unavailable"), nil
	}
	turnID := binding.SubmittedTurnID
	if binding.SubmittedIdempotencyKey == "" {
		task, taskErr := e.registeredTask(ctx, request)
		if taskErr != nil {
			return retry("agentd_submit", "registered task is unavailable"), nil
		}
		turn, err := e.Broker.SubmitTurn(ctx, SubmitTurnRequest{Version: "agentd/registered-lifecycle/v1", IdempotencyKey: binding.RegisteredSubmitKey, TaskKind: task.Snapshot.TaskKind, AdmissionTaskDigest: task.Digest, TaskEvidenceDigest: task.Snapshot.TaskEvidenceDigest, Parameters: task.Snapshot.Parameters})
		if err != nil || turn.SessionID == "" || turn.TurnID == "" || turn.ModelEffectID != "model:"+binding.RegisteredSubmitKey || turn.Cursor <= 0 || !sameLease(binding, turn.Lease) {
			return retry("agentd_submit", "broker turn submit unavailable"), nil
		}
		if err := e.Store.RecordRegisteredTurn(ctx, request.WorkItem.ID, turn.Lease, binding.RegisteredSubmitKey, turn.SessionID, turn.TurnID, turn.ModelEffectID, turn.Cursor, e.now()); err != nil {
			return retry("coordinator_fence", "submitted turn conflicts"), nil
		}
		turnID = turn.TurnID
		binding.AgentdSessionID, binding.ModelEffectID, binding.EventCursor = turn.SessionID, turn.ModelEffectID, turn.Cursor
	}
	task, err := e.registeredTask(ctx, request)
	if err != nil {
		return retry("agentd_protocol", "registered task is unavailable"), nil
	}
	batch, err := e.Broker.StreamEvents(ctx, StreamEventsRequest{BindingKey: binding.BindingKey, Cursor: binding.EventCursor})
	if err != nil || !sameLease(binding, batch.Lease) {
		return retry("agentd_events", "broker event stream unavailable"), nil
	}
	now := e.now()
	next, deadline := now.Add(pollInterval), now.Add(waitDeadline)
	result := workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting, ExternalCorrelation: turnID, NextAttemptAt: &next, DeadlineAt: &deadline}
	for _, event := range batch.Events {
		if !validEvent(event, binding, task) {
			return retry("agentd_protocol", "agent event invalid"), nil
		}
		if _, err := e.Store.RecordCoordinatorEvent(ctx, request.WorkItem.ID, workledger.CoordinatorEvent{Cursor: event.Cursor, WorkerID: binding.WorkerID, WorkerFenceEpoch: binding.WorkerFenceEpoch, Kind: "registered_" + event.Phase, EvidenceRef: "agentd:registered-events:" + event.ModelEffectID + ":" + fmt.Sprint(event.Cursor)}, e.now()); err != nil {
			return retry("coordinator_fence", "agent event rejected"), nil
		}
		if event.Verifier != nil {
			if err := e.recordVerifierEvent(ctx, request, binding, event); err != nil {
				return retry("agentd_verifier", "agent verifier event rejected"), nil
			}
		}
	}
	// Runtime success is durable evidence only. PR 10 owns semantic verification
	// and is the only boundary allowed to complete the work item.
	return result, nil
}
func retry(classification, message string) workledger.ExecutorResult {
	return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: classification, SanitizedError: message}
}
func validEvent(e Event, binding workledger.SessionBinding, task RegisteredTask) bool {
	if e.Cursor <= 0 || e.Attempt < 0 || e.SessionID != binding.AgentdSessionID || e.TurnID != binding.SubmittedTurnID || e.ModelEffectID != binding.ModelEffectID || e.WorkerID != binding.WorkerID || e.StorageLineageID != binding.WorkerStorageLineageID || e.FenceEpoch != binding.WorkerFenceEpoch || e.AdmissionTaskDigest != task.Digest || e.TaskEvidenceDigest != task.Snapshot.TaskEvidenceDigest {
		return false
	}
	if e.Verifier == nil {
		switch e.Phase {
		case "queued", "authorized", "running", "completed":
			return true
		default:
			return false
		}
	}
	return (e.Phase == "pending" || e.Phase == "green" || e.Phase == "red" || e.Phase == "refused" || e.Phase == "escalated") && e.Phase == e.Verifier.Phase && validVerifierResult(e.Verifier)
}

func registeredSubmitKey(binding workledger.SessionBinding) string {
	return "agentd:registered-turn:v1:" + binding.WorkItemID
}

func bindingLease(binding workledger.SessionBinding) workledger.SessionLease {
	return workledger.SessionLease{AuthorityProfile: binding.AuthorityProfile, ProfileVersion: binding.ProfileVersion, PolicyDigest: binding.PolicyDigest, SessionLineageID: binding.SessionLineageID, WorkerID: binding.WorkerID, WorkerStorageLineageID: binding.WorkerStorageLineageID, WorkerFenceEpoch: binding.WorkerFenceEpoch}
}

func (e *Executor) recordVerifierEvent(ctx context.Context, request workledger.ExecutorRequest, binding workledger.SessionBinding, event Event) error {
	v := event.Verifier
	task, err := e.registeredTask(ctx, request)
	if err != nil ||
		event.AdmissionTaskDigest != task.Digest ||
		event.TaskEvidenceDigest != task.Snapshot.TaskEvidenceDigest ||
		v.ContractDigest != task.Snapshot.ContractDigest ||
		v.TaskEvidenceDigest != task.Snapshot.TaskEvidenceDigest {
		return errors.New("agent verifier event admission identity is stale")
	}
	reasons := make([]string, len(v.Reasons))
	for i, reason := range v.Reasons {
		reasons[i] = reason.Code
	}
	if err := e.RecordGitHubGreenPRResult(ctx, request.WorkItem.ID, workledger.VerifierResult{AttemptID: request.Attempt.ID, ContractDigest: v.ContractDigest, TaskEvidenceDigest: v.TaskEvidenceDigest, HeadRevision: v.HeadRevision, EvaluationRevision: v.HeadRevision, Outcome: signalOutcome(v.Outcome), ReasonCodes: reasons, EvidenceRefs: v.EvidenceRefs}); err != nil {
		return err
	}
	return nil
}
func verifierPhaseMatches(phase, outcome string) bool {
	return (phase == "pending" && outcome == "waiting") || (phase == "green" && outcome == "satisfied") || (phase == "red" && (outcome == "continuation" || outcome == "missing_or_stale")) || ((phase == "refused" || phase == "escalated") && outcome == "escalated")
}

func validVerifierResult(v *VerifierEvent) bool {
	if !verifierPhaseMatches(v.Phase, v.Outcome) || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(v.ContractDigest) || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(v.TaskEvidenceDigest) || !boundedOpaque(v.HeadRevision) || len(v.EvidenceRefs) == 0 || len(v.EvidenceRefs) > 64 {
		return false
	}
	if (v.Outcome == "satisfied") != (len(v.Reasons) == 0) {
		return false
	}
	for _, reason := range v.Reasons {
		if !boundedOpaque(reason.Code) || (reason.EvidenceRef != "" && !boundedOpaque(reason.EvidenceRef)) {
			return false
		}
	}
	for _, ref := range v.EvidenceRefs {
		if !boundedOpaque(ref) {
			return false
		}
	}
	return true
}

func signalOutcome(outcome string) string {
	if outcome == "continuation" || outcome == "missing_or_stale" {
		return "continuation_required"
	}
	return outcome
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
