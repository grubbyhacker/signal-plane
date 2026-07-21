package workledger

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
)

const durablePollInterval = 5 * time.Second
const durableWaitDeadline = 30 * time.Minute

type SessionBinding struct {
	WorkItemID, BindingKey, AuthorityProfile, ProfileVersion, PolicyDigest                            string
	SessionLineageID, WorkerID, WorkerStorageLineageID                                                string
	AgentdSessionID, CheckpointRef, State                                                             string
	RegisteredSubmitKey, SubmittedIdempotencyKey, ModelEffectID, ActiveModelEffectID, SubmittedTurnID string
	EventCursor, WorkerFenceEpoch, ActiveEffectAttempt                                                int64
	CreatedAt, UpdatedAt                                                                              time.Time
}

type SessionLease struct {
	AuthorityProfile, ProfileVersion, PolicyDigest, SessionLineageID string
	WorkerID, WorkerStorageLineageID                                 string
	WorkerFenceEpoch                                                 int64
}

type Usage struct{ InputTokens, CachedInputTokens, OutputTokens, ReasoningOutputTokens, TotalTokens int64 }
type CoordinatorEvent struct {
	Cursor                      int64
	WorkerID, Kind, EvidenceRef string
	WorkerFenceEpoch            int64
	Usage                       Usage
}

// RegisteredCoordinatorEvent is an authenticated registered-lifecycle event.
// Its effect identity is kept separately from the immutable submitted root.
type RegisteredCoordinatorEvent struct {
	CoordinatorEvent
	ModelEffectID string
	Attempt       int64
	Phase         string
}

type ReassignmentPhase string

const (
	ReassignmentRequested            ReassignmentPhase = "requested"
	ReassignmentBrokerCommitted      ReassignmentPhase = "broker_committed"
	ReassignmentAgentdAdopted        ReassignmentPhase = "agentd_adopted"
	ReassignmentCoordinatorCommitted ReassignmentPhase = "coordinator_committed"
	ReassignmentEscalated            ReassignmentPhase = "escalated"
)

type Reassignment struct {
	WorkItemID, IdempotencyKey, RebindIdempotencyKey, SessionLineageID, AuthorityProfile string
	ProfileVersion, PolicyDigest, StorageLineageID                                       string
	PredecessorWorkerID, SuccessorWorkerID, BrokerState, ErrorCode                       string
	PredecessorFenceEpoch, SuccessorFenceEpoch                                           int64
	Phase                                                                                ReassignmentPhase
	CreatedAt, UpdatedAt                                                                 time.Time
}

type VerifierResult struct {
	AttemptID, ContractDigest, TaskEvidenceDigest string
	// EvaluationRevision is the durable representation projection of the
	// package headRevision, not a provider fact.
	HeadRevision, EvaluationRevision, Outcome string
	ReasonCodes, EvidenceRefs                 []string
}

func boundedVerifierRevision(value string) bool {
	return value != "" && len(value) <= 512 && !strings.ContainsAny(value, "\r\n")
}

func (u Usage) Valid() bool {
	if u.InputTokens < 0 || u.CachedInputTokens < 0 || u.OutputTokens < 0 || u.ReasoningOutputTokens < 0 || u.TotalTokens < 0 {
		return false
	}
	if u.CachedInputTokens > u.InputTokens || u.ReasoningOutputTokens > u.OutputTokens || u.InputTokens > math.MaxInt64-u.OutputTokens {
		return false
	}
	return u.TotalTokens == u.InputTokens+u.OutputTokens
}

func (store *Store) BindSession(ctx context.Context, workItemID, bindingKey, authorityProfile, workerID string, now time.Time) (SessionBinding, error) {
	return store.BindSessionLease(ctx, workItemID, bindingKey, SessionLease{
		AuthorityProfile: authorityProfile, ProfileVersion: "legacy-v1", PolicyDigest: fmt.Sprintf("%064d", 0),
		SessionLineageID: fmt.Sprintf("%032d", 1), WorkerID: workerID,
		WorkerStorageLineageID: fmt.Sprintf("%032d", 2), WorkerFenceEpoch: 1,
	}, now)
}

func (store *Store) BindSessionLease(ctx context.Context, workItemID, bindingKey string, lease SessionLease, now time.Time) (SessionBinding, error) {
	if workItemID == "" || bindingKey == "" || !validLease(lease) {
		return SessionBinding{}, errors.New("session binding requires complete fenced broker lease")
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO session_bindings(work_item_id,binding_key,authority_profile,profile_version,policy_digest,session_lineage_id,worker_id,worker_storage_lineage_id,worker_fence_epoch,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_item_id) DO NOTHING`, workItemID, bindingKey, lease.AuthorityProfile, lease.ProfileVersion, lease.PolicyDigest, lease.SessionLineageID, lease.WorkerID, lease.WorkerStorageLineageID, lease.WorkerFenceEpoch, "active", millis(now), millis(now))
	if err != nil {
		return SessionBinding{}, err
	}
	binding, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return SessionBinding{}, err
	}
	if binding.BindingKey != bindingKey || !sameSessionLease(binding, lease) {
		return SessionBinding{}, errors.New("session binding conflicts with durable authority assignment")
	}
	return binding, nil
}

var lineagePattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
var policyPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validLease(lease SessionLease) bool {
	return lease.WorkerID != "" && lease.AuthorityProfile != "" && lease.ProfileVersion != "" &&
		policyPattern.MatchString(lease.PolicyDigest) && lineagePattern.MatchString(lease.SessionLineageID) &&
		lineagePattern.MatchString(lease.WorkerStorageLineageID) && lease.WorkerFenceEpoch > 0
}

func sameSessionLease(binding SessionBinding, lease SessionLease) bool {
	return binding.AuthorityProfile == lease.AuthorityProfile && binding.ProfileVersion == lease.ProfileVersion &&
		binding.PolicyDigest == lease.PolicyDigest && binding.SessionLineageID == lease.SessionLineageID &&
		binding.WorkerID == lease.WorkerID && binding.WorkerStorageLineageID == lease.WorkerStorageLineageID &&
		binding.WorkerFenceEpoch == lease.WorkerFenceEpoch
}

func (store *Store) SessionBinding(ctx context.Context, workItemID string) (SessionBinding, error) {
	var binding SessionBinding
	var created, updated int64
	err := store.db.QueryRowContext(ctx, `SELECT work_item_id,binding_key,authority_profile,profile_version,policy_digest,session_lineage_id,worker_id,worker_storage_lineage_id,worker_fence_epoch,agentd_session_id,registered_submit_key,submitted_idempotency_key,model_effect_id,active_model_effect_id,submitted_turn_id,checkpoint_ref,CAST(event_cursor AS INTEGER),active_effect_attempt,state,created_at,updated_at FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(
		&binding.WorkItemID, &binding.BindingKey, &binding.AuthorityProfile, &binding.ProfileVersion, &binding.PolicyDigest,
		&binding.SessionLineageID, &binding.WorkerID, &binding.WorkerStorageLineageID, &binding.WorkerFenceEpoch,
		&binding.AgentdSessionID, &binding.RegisteredSubmitKey, &binding.SubmittedIdempotencyKey, &binding.ModelEffectID, &binding.ActiveModelEffectID, &binding.SubmittedTurnID, &binding.CheckpointRef, &binding.EventCursor, &binding.ActiveEffectAttempt, &binding.State, &created, &updated,
	)
	if err != nil {
		return SessionBinding{}, err
	}
	binding.CreatedAt, binding.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return binding, nil
}

// BindRegisteredSubmitKey durably reserves the one broker idempotency key for
// this fenced work-item binding before the registered-turn network effect.
func (store *Store) BindRegisteredSubmitKey(ctx context.Context, workItemID string, lease SessionLease, key string, now time.Time) (SessionBinding, error) {
	if key == "" || !validLease(lease) {
		return SessionBinding{}, errors.New("registered submit key requires a complete fenced binding")
	}
	_, err := store.db.ExecContext(ctx, `UPDATE session_bindings SET registered_submit_key=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND worker_fence_epoch=? AND profile_version=? AND policy_digest=? AND session_lineage_id=? AND worker_storage_lineage_id=? AND registered_submit_key=''`, key, millis(now), workItemID, lease.WorkerID, lease.WorkerFenceEpoch, lease.ProfileVersion, lease.PolicyDigest, lease.SessionLineageID, lease.WorkerStorageLineageID)
	if err != nil {
		return SessionBinding{}, err
	}
	binding, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return SessionBinding{}, err
	}
	if !sameSessionLease(binding, lease) || binding.RegisteredSubmitKey != key {
		return SessionBinding{}, errors.New("registered submit key conflicts with durable binding")
	}
	return binding, nil
}

// RecordSubmittedTurn persists the relationship between the Signal attempt
// submission key and agentd's distinct canonical effect model:<key>.
func (store *Store) RecordSubmittedTurn(ctx context.Context, workItemID string, lease SessionLease, idempotencyKey, turnID string, now time.Time) error {
	return store.RecordRegisteredTurn(ctx, workItemID, lease, idempotencyKey, "legacy-session", turnID, "model:"+idempotencyKey, 0, now)
}

// RecordRegisteredTurn durably records the exact agentd v2 acceptance mapping
// before the coordinator schedules any poll. A restart therefore polls after
// the persisted cursor and never submits or continues the model turn again.
func (store *Store) RecordRegisteredTurn(ctx context.Context, workItemID string, lease SessionLease, idempotencyKey, sessionID, turnID, modelEffectID string, cursor int64, now time.Time) error {
	if idempotencyKey == "" || sessionID == "" || turnID == "" || modelEffectID != "model:"+idempotencyKey || cursor <= 0 || !validLease(lease) {
		return errors.New("submitted turn identity is incomplete")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE session_bindings SET agentd_session_id=?,submitted_idempotency_key=?,model_effect_id=?,active_model_effect_id=?,active_effect_attempt=0,submitted_turn_id=?,event_cursor=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND worker_fence_epoch=? AND profile_version=? AND policy_digest=? AND session_lineage_id=? AND worker_storage_lineage_id=? AND submitted_idempotency_key=''`, sessionID, idempotencyKey, modelEffectID, modelEffectID, turnID, cursor, millis(now), workItemID, lease.WorkerID, lease.WorkerFenceEpoch, lease.ProfileVersion, lease.PolicyDigest, lease.SessionLineageID, lease.WorkerStorageLineageID)
	if err != nil {
		return err
	}
	if changed, _ := result.RowsAffected(); changed == 1 {
		return nil
	}
	binding, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return err
	}
	if !sameSessionLease(binding, lease) || binding.AgentdSessionID != sessionID || binding.SubmittedIdempotencyKey != idempotencyKey || binding.ModelEffectID != modelEffectID || binding.ActiveModelEffectID != modelEffectID || binding.SubmittedTurnID != turnID || binding.EventCursor != cursor {
		return errors.New("submitted turn conflicts with durable model effect")
	}
	return nil
}

func (store *Store) RoutingReady(ctx context.Context, workItemID string) error {
	var phase string
	err := store.db.QueryRowContext(ctx, `SELECT phase FROM coordinator_reassignments WHERE work_item_id=? ORDER BY predecessor_fence_epoch DESC LIMIT 1`, workItemID).Scan(&phase)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if ReassignmentPhase(phase) != ReassignmentCoordinatorCommitted {
		return errors.New("coordinator reassignment is incomplete")
	}
	return nil
}

func (store *Store) SetAgentdSession(ctx context.Context, workItemID string, lease SessionLease, sessionID string, now time.Time) error {
	if sessionID == "" || !validLease(lease) {
		return errors.New("complete broker session identity is required")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE session_bindings SET agentd_session_id=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND worker_fence_epoch=? AND profile_version=? AND policy_digest=? AND session_lineage_id=? AND worker_storage_lineage_id=?`, sessionID, millis(now), workItemID, lease.WorkerID, lease.WorkerFenceEpoch, lease.ProfileVersion, lease.PolicyDigest, lease.SessionLineageID, lease.WorkerStorageLineageID)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return errors.New("stale broker session cannot be recorded")
	}
	return nil
}

// RecordCoordinatorEvent atomically appends one exact next event and advances
// the durable cursor. Replays are accepted only when byte-for-byte normalized.
func (store *Store) RecordCoordinatorEvent(ctx context.Context, workItemID string, event CoordinatorEvent, now time.Time) (bool, error) {
	if event.Cursor <= 0 || event.WorkerID == "" || event.WorkerFenceEpoch <= 0 || event.Kind == "" || !event.Usage.Valid() {
		return false, errors.New("invalid normalized coordinator event")
	}
	inserted := false
	err := store.immediate(ctx, func(conn *sql.Conn) error {
		var key, worker, profileVersion, policyDigest, storageLineage string
		var epoch, cursor int64
		if err := conn.QueryRowContext(ctx, `SELECT binding_key,worker_id,profile_version,policy_digest,worker_storage_lineage_id,worker_fence_epoch,CAST(event_cursor AS INTEGER) FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&key, &worker, &profileVersion, &policyDigest, &storageLineage, &epoch, &cursor); err != nil {
			return err
		}
		if worker != event.WorkerID || epoch != event.WorkerFenceEpoch {
			return errors.New("stale predecessor event rejected")
		}
		if event.Cursor <= cursor {
			return store.matchCoordinatorEvent(ctx, conn, key, event)
		}
		if event.Cursor != cursor+1 {
			return fmt.Errorf("coordinator event cursor gap: got %d after %d", event.Cursor, cursor)
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO coordinator_events(binding_key,cursor,worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, key, event.Cursor, event.WorkerID, event.WorkerFenceEpoch, event.Kind, event.EvidenceRef, event.Usage.InputTokens, event.Usage.CachedInputTokens, event.Usage.OutputTokens, event.Usage.ReasoningOutputTokens, event.Usage.TotalTokens, millis(now))
		if err != nil {
			return err
		}
		changed, _ := result.RowsAffected()
		if changed != 1 {
			return errors.New("coordinator event insert lost")
		}
		result, err = conn.ExecContext(ctx, `UPDATE session_bindings SET event_cursor=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND worker_fence_epoch=? AND profile_version=? AND policy_digest=? AND worker_storage_lineage_id=? AND event_cursor=?`, event.Cursor, millis(now), workItemID, event.WorkerID, event.WorkerFenceEpoch, profileVersion, policyDigest, storageLineage, cursor)
		if err != nil {
			return err
		}
		changed, _ = result.RowsAffected()
		if changed != 1 {
			return errors.New("coordinator cursor CAS lost")
		}
		inserted = true
		return nil
	})
	return inserted, err
}

// RecordRegisteredCoordinatorEvent appends an exact ordered event and, only
// for the authorized next continuation, advances the durable active effect.
// The root model effect in session_bindings is intentionally never updated.
func (store *Store) RecordRegisteredCoordinatorEvent(ctx context.Context, workItemID string, event RegisteredCoordinatorEvent, now time.Time) (bool, error) {
	if event.Cursor <= 0 || event.WorkerID == "" || event.WorkerFenceEpoch <= 0 || event.Kind == "" || event.ModelEffectID == "" || event.Attempt < 0 || event.Phase == "" || !event.Usage.Valid() {
		return false, errors.New("invalid registered coordinator event")
	}
	inserted := false
	err := store.immediate(ctx, func(conn *sql.Conn) error {
		var key, worker, profileVersion, policyDigest, storageLineage, rootEffect, activeEffect string
		var epoch, cursor, activeAttempt int64
		if err := conn.QueryRowContext(ctx, `SELECT binding_key,worker_id,profile_version,policy_digest,worker_storage_lineage_id,worker_fence_epoch,CAST(event_cursor AS INTEGER),model_effect_id,active_model_effect_id,active_effect_attempt FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&key, &worker, &profileVersion, &policyDigest, &storageLineage, &epoch, &cursor, &rootEffect, &activeEffect, &activeAttempt); err != nil {
			return err
		}
		if worker != event.WorkerID || epoch != event.WorkerFenceEpoch {
			return errors.New("stale predecessor event rejected")
		}
		if event.Cursor <= cursor {
			return store.matchCoordinatorEvent(ctx, conn, key, event.CoordinatorEvent)
		}
		if event.Cursor != cursor+1 {
			return fmt.Errorf("coordinator event cursor gap: got %d after %d", event.Cursor, cursor)
		}
		if activeEffect == "" {
			return errors.New("registered event active model effect is missing")
		}
		advance := event.ModelEffectID != activeEffect
		if advance {
			if activeEffect != rootEffect {
				return errors.New("registered continuation effect already advanced")
			}
			var outcome string
			if err := conn.QueryRowContext(ctx, `SELECT outcome FROM verifier_results WHERE work_item_id=?`, workItemID).Scan(&outcome); err != nil || outcome != "continuation_required" {
				return errors.New("continuation effect lacks a durable root continuation verdict")
			}
			if event.Phase != "authorized" || event.Attempt != activeAttempt+1 {
				return errors.New("continuation effect progression is invalid")
			}
		} else if event.Attempt < activeAttempt || event.Attempt > activeAttempt+1 {
			return errors.New("registered event attempt progression is invalid")
		}
		result, err := conn.ExecContext(ctx, `INSERT INTO coordinator_events(binding_key,cursor,worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, key, event.Cursor, event.WorkerID, event.WorkerFenceEpoch, event.Kind, event.EvidenceRef, event.Usage.InputTokens, event.Usage.CachedInputTokens, event.Usage.OutputTokens, event.Usage.ReasoningOutputTokens, event.Usage.TotalTokens, millis(now))
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return errors.New("coordinator event insert lost")
		}
		if advance {
			activeEffect = event.ModelEffectID
		}
		result, err = conn.ExecContext(ctx, `UPDATE session_bindings SET event_cursor=?,active_model_effect_id=?,active_effect_attempt=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND worker_fence_epoch=? AND profile_version=? AND policy_digest=? AND worker_storage_lineage_id=? AND event_cursor=?`, event.Cursor, activeEffect, event.Attempt, millis(now), workItemID, event.WorkerID, event.WorkerFenceEpoch, profileVersion, policyDigest, storageLineage, cursor)
		if err != nil {
			return err
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return errors.New("registered coordinator cursor CAS lost")
		}
		inserted = true
		return nil
	})
	return inserted, err
}

func (store *Store) matchCoordinatorEvent(ctx context.Context, conn *sql.Conn, key string, event CoordinatorEvent) error {
	var worker, kind, evidence string
	var epoch int64
	var usage Usage
	err := conn.QueryRowContext(ctx, `SELECT worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens FROM coordinator_events WHERE binding_key=? AND cursor=?`, key, event.Cursor).Scan(&worker, &epoch, &kind, &evidence, &usage.InputTokens, &usage.CachedInputTokens, &usage.OutputTokens, &usage.ReasoningOutputTokens, &usage.TotalTokens)
	if err != nil {
		return err
	}
	if worker != event.WorkerID || epoch != event.WorkerFenceEpoch || kind != event.Kind || evidence != event.EvidenceRef || usage != event.Usage {
		return errors.New("duplicate coordinator cursor conflicts with recorded event")
	}
	return nil
}

func (store *Store) immediate(ctx context.Context, fn func(*sql.Conn) error) (err error) {
	conn, err := store.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err = fn(conn); err != nil {
		return err
	}
	_, err = conn.ExecContext(ctx, "COMMIT")
	return err
}

func (store *Store) BeginReassignment(ctx context.Context, workItemID string, predecessorEpoch int64, idempotencyKey string, now time.Time) (Reassignment, error) {
	transition, err := store.Reassignment(ctx, workItemID, predecessorEpoch)
	if err == nil {
		if transition.IdempotencyKey != idempotencyKey {
			return Reassignment{}, errors.New("reassignment idempotency identity conflicts with durable request")
		}
		return transition, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Reassignment{}, err
	}
	binding, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return Reassignment{}, err
	}
	if binding.WorkerFenceEpoch != predecessorEpoch {
		return Reassignment{}, errors.New("reassignment predecessor epoch conflicts with active binding")
	}
	if err := store.RoutingReady(ctx, workItemID); err != nil {
		return Reassignment{}, err
	}
	_, err = store.db.ExecContext(ctx, `INSERT INTO coordinator_reassignments(work_item_id,predecessor_fence_epoch,idempotency_key,phase,session_lineage_id,authority_profile,profile_version,policy_digest,storage_lineage_id,predecessor_worker_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_item_id,predecessor_fence_epoch) DO NOTHING`, workItemID, predecessorEpoch, idempotencyKey, ReassignmentRequested, binding.SessionLineageID, binding.AuthorityProfile, binding.ProfileVersion, binding.PolicyDigest, binding.WorkerStorageLineageID, binding.WorkerID, millis(now), millis(now))
	if err != nil {
		return Reassignment{}, err
	}
	transition, err = store.Reassignment(ctx, workItemID, predecessorEpoch)
	if err != nil {
		return Reassignment{}, err
	}
	if transition.IdempotencyKey != idempotencyKey || transition.PredecessorWorkerID != binding.WorkerID || transition.SessionLineageID != binding.SessionLineageID {
		return Reassignment{}, errors.New("reassignment transition conflicts with durable request")
	}
	return transition, nil
}

func (store *Store) EscalateReassignment(ctx context.Context, workItemID string, predecessorEpoch int64, brokerState, errorCode string, now time.Time) error {
	if !regexp.MustCompile(`^[a-z0-9_]{1,128}$`).MatchString(errorCode) {
		return errors.New("bounded reassignment error code is required")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE coordinator_reassignments SET phase=?,broker_state=?,error_code=?,updated_at=? WHERE work_item_id=? AND predecessor_fence_epoch=? AND phase<>?`, ReassignmentEscalated, brokerState, errorCode, millis(now), workItemID, predecessorEpoch, ReassignmentCoordinatorCommitted)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return errors.New("reassignment escalation conflicts with committed transition")
	}
	return nil
}

func (store *Store) Reassignment(ctx context.Context, workItemID string, predecessorEpoch int64) (Reassignment, error) {
	var value Reassignment
	var created, updated int64
	err := store.db.QueryRowContext(ctx, `SELECT work_item_id,predecessor_fence_epoch,idempotency_key,rebind_idempotency_key,phase,session_lineage_id,authority_profile,profile_version,policy_digest,storage_lineage_id,predecessor_worker_id,successor_worker_id,successor_fence_epoch,broker_state,error_code,created_at,updated_at FROM coordinator_reassignments WHERE work_item_id=? AND predecessor_fence_epoch=?`, workItemID, predecessorEpoch).Scan(&value.WorkItemID, &value.PredecessorFenceEpoch, &value.IdempotencyKey, &value.RebindIdempotencyKey, &value.Phase, &value.SessionLineageID, &value.AuthorityProfile, &value.ProfileVersion, &value.PolicyDigest, &value.StorageLineageID, &value.PredecessorWorkerID, &value.SuccessorWorkerID, &value.SuccessorFenceEpoch, &value.BrokerState, &value.ErrorCode, &created, &updated)
	value.CreatedAt, value.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return value, err
}

func (store *Store) RecordBrokerReassignment(ctx context.Context, workItemID string, predecessorEpoch int64, successor SessionLease, brokerState string, now time.Time) error {
	if !validLease(successor) || successor.WorkerFenceEpoch != predecessorEpoch+1 || brokerState == "" {
		return errors.New("complete broker successor is required")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE coordinator_reassignments SET phase=?,successor_worker_id=?,successor_fence_epoch=?,broker_state=?,updated_at=? WHERE work_item_id=? AND predecessor_fence_epoch=? AND phase IN (?,?) AND session_lineage_id=? AND authority_profile=? AND profile_version=? AND policy_digest=? AND storage_lineage_id=? AND (successor_worker_id='' OR successor_worker_id=?) AND (successor_fence_epoch=0 OR successor_fence_epoch=?)`, ReassignmentBrokerCommitted, successor.WorkerID, successor.WorkerFenceEpoch, brokerState, millis(now), workItemID, predecessorEpoch, ReassignmentRequested, ReassignmentBrokerCommitted, successor.SessionLineageID, successor.AuthorityProfile, successor.ProfileVersion, successor.PolicyDigest, successor.WorkerStorageLineageID, successor.WorkerID, successor.WorkerFenceEpoch)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return errors.New("broker successor conflicts with durable reassignment")
	}
	return nil
}

func (store *Store) RecordAgentdAdopted(ctx context.Context, workItemID string, predecessorEpoch int64, brokerState, rebindIdempotencyKey string, now time.Time) error {
	if brokerState != "confirmed" || rebindIdempotencyKey == "" {
		return errors.New("only confirmed broker adoption can advance")
	}
	result, err := store.db.ExecContext(ctx, `UPDATE coordinator_reassignments SET phase=?,broker_state=?,rebind_idempotency_key=?,updated_at=? WHERE work_item_id=? AND predecessor_fence_epoch=? AND phase IN (?,?) AND (rebind_idempotency_key='' OR rebind_idempotency_key=?)`, ReassignmentAgentdAdopted, brokerState, rebindIdempotencyKey, millis(now), workItemID, predecessorEpoch, ReassignmentBrokerCommitted, ReassignmentAgentdAdopted, rebindIdempotencyKey)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return errors.New("agentd adoption transition is not broker committed")
	}
	return nil
}

func (store *Store) CommitReassignment(ctx context.Context, workItemID string, predecessorEpoch int64, now time.Time) (SessionBinding, error) {
	err := store.immediate(ctx, func(conn *sql.Conn) error {
		var transition Reassignment
		err := conn.QueryRowContext(ctx, `SELECT phase,session_lineage_id,authority_profile,profile_version,policy_digest,storage_lineage_id,predecessor_worker_id,successor_worker_id,successor_fence_epoch FROM coordinator_reassignments WHERE work_item_id=? AND predecessor_fence_epoch=?`, workItemID, predecessorEpoch).Scan(&transition.Phase, &transition.SessionLineageID, &transition.AuthorityProfile, &transition.ProfileVersion, &transition.PolicyDigest, &transition.StorageLineageID, &transition.PredecessorWorkerID, &transition.SuccessorWorkerID, &transition.SuccessorFenceEpoch)
		if err != nil {
			return err
		}
		if transition.Phase == ReassignmentCoordinatorCommitted {
			return nil
		}
		if transition.Phase != ReassignmentAgentdAdopted {
			return errors.New("reassignment adoption is not confirmed")
		}
		result, err := conn.ExecContext(ctx, `UPDATE session_bindings SET worker_id=?,worker_fence_epoch=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND worker_fence_epoch=? AND session_lineage_id=? AND authority_profile=? AND profile_version=? AND policy_digest=? AND worker_storage_lineage_id=?`, transition.SuccessorWorkerID, transition.SuccessorFenceEpoch, millis(now), workItemID, transition.PredecessorWorkerID, predecessorEpoch, transition.SessionLineageID, transition.AuthorityProfile, transition.ProfileVersion, transition.PolicyDigest, transition.StorageLineageID)
		if err != nil {
			return err
		}
		changed, _ := result.RowsAffected()
		if changed != 1 {
			return errors.New("reassignment predecessor CAS lost")
		}
		_, err = conn.ExecContext(ctx, `UPDATE coordinator_reassignments SET phase=?,updated_at=? WHERE work_item_id=? AND predecessor_fence_epoch=? AND phase=?`, ReassignmentCoordinatorCommitted, millis(now), workItemID, predecessorEpoch, ReassignmentAgentdAdopted)
		return err
	})
	if err != nil {
		return SessionBinding{}, err
	}
	return store.SessionBinding(ctx, workItemID)
}

func (store *Store) ReassignSession(ctx context.Context, workItemID, predecessorWorker string, predecessorEpoch int64, successor SessionLease, now time.Time) (SessionBinding, error) {
	current, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return SessionBinding{}, err
	}
	if sameSessionLease(current, successor) {
		return current, nil
	}
	if current.WorkerID != predecessorWorker || current.WorkerFenceEpoch != predecessorEpoch {
		return SessionBinding{}, errors.New("reassignment predecessor changed")
	}
	key := fmt.Sprintf("legacy-reassign:%s:%d", workItemID, predecessorEpoch)
	transition, err := store.BeginReassignment(ctx, workItemID, predecessorEpoch, key, now)
	if err != nil {
		return SessionBinding{}, err
	}
	if transition.PredecessorWorkerID != predecessorWorker {
		return SessionBinding{}, errors.New("reassignment predecessor changed")
	}
	if err := store.RecordBrokerReassignment(ctx, workItemID, predecessorEpoch, successor, "confirmed", now); err != nil {
		return SessionBinding{}, err
	}
	if err := store.RecordAgentdAdopted(ctx, workItemID, predecessorEpoch, "confirmed", key, now); err != nil {
		return SessionBinding{}, err
	}
	return store.CommitReassignment(ctx, workItemID, predecessorEpoch, now)
}

func (store *Store) CoordinatorUsage(ctx context.Context, workItemID string) (usageEvents int64, usage Usage, err error) {
	err = store.db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(e.input_tokens),0),coalesce(sum(e.cached_input_tokens),0),coalesce(sum(e.output_tokens),0),coalesce(sum(e.reasoning_output_tokens),0),coalesce(sum(e.total_tokens),0) FROM coordinator_events e JOIN session_bindings b ON b.binding_key=e.binding_key WHERE b.work_item_id=? AND e.event_kind='attempt_completed'`, workItemID).Scan(&usageEvents, &usage.InputTokens, &usage.CachedInputTokens, &usage.OutputTokens, &usage.ReasoningOutputTokens, &usage.TotalTokens)
	return
}

func (store *Store) RecordVerifierResult(ctx context.Context, workItemID string, result VerifierResult, now time.Time) error {
	if result.Outcome != "waiting" && result.Outcome != "continuation_required" && result.Outcome != "satisfied" && result.Outcome != "escalated" {
		return errors.New("unknown verifier outcome")
	}
	if result.AttemptID == "" || !boundedVerifierRevision(result.HeadRevision) || result.EvaluationRevision != result.HeadRevision || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(result.ContractDigest) || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(result.TaskEvidenceDigest) || len(result.EvidenceRefs) == 0 || len(result.EvidenceRefs) > 64 {
		return errors.New("verifier result identity is incomplete")
	}
	if result.Outcome == "satisfied" && len(result.ReasonCodes) != 0 {
		return errors.New("satisfied verifier result cannot have reasons")
	}
	if result.Outcome != "satisfied" && len(result.ReasonCodes) == 0 {
		return errors.New("non-satisfied verifier result requires reasons")
	}
	reasons := slices.Clone(result.ReasonCodes)
	slices.Sort(reasons)
	reasons = slices.Compact(reasons)
	evidence := slices.Clone(result.EvidenceRefs)
	slices.Sort(evidence)
	reasonJSON, _ := json.Marshal(reasons)
	evidenceJSON, _ := json.Marshal(evidence)
	identityJSON, _ := json.Marshal(struct {
		AttemptID, ContractDigest, TaskEvidenceDigest string
		HeadRevision, EvaluationRevision, Outcome     string
		ReasonCodes, EvidenceRefs                     []string
	}{result.AttemptID, result.ContractDigest, result.TaskEvidenceDigest, result.HeadRevision, result.EvaluationRevision, result.Outcome, reasons, evidence})
	resultDigest := fmt.Sprintf("sha256:%x", sha256.Sum256(identityJSON))
	return store.immediate(ctx, func(conn *sql.Conn) error {
		var verifierID, contract, digest, taskEvidence, state string
		var continuations int
		err := conn.QueryRowContext(ctx, `SELECT s.verifier_id,s.completion_contract,s.task_contract_digest,w.task_evidence_digest,w.state,w.continuation_count FROM work_items w JOIN route_snapshots s ON s.id=w.route_snapshot_id WHERE w.id=?`, workItemID).Scan(&verifierID, &contract, &digest, &taskEvidence, &state, &continuations)
		if err != nil {
			return err
		}
		if digest != result.ContractDigest || taskEvidence != result.TaskEvidenceDigest {
			return errors.New("stale verifier result rejected")
		}
		var priorAttemptID string
		err = conn.QueryRowContext(ctx, `SELECT attempt_id FROM verifier_results WHERE work_item_id=?`, workItemID).Scan(&priorAttemptID)
		if err == nil && priorAttemptID == "" {
			return errors.New("legacy verifier result has no replay identity")
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var attemptState, receiptDigest string
		if err := conn.QueryRowContext(ctx, `SELECT state FROM executor_attempts WHERE id=? AND work_item_id=?`, result.AttemptID, workItemID).Scan(&attemptState); err != nil {
			return errors.New("verifier result requires its bound attempt")
		}
		err = conn.QueryRowContext(ctx, `SELECT result_digest FROM verifier_result_receipts WHERE work_item_id=? AND attempt_id=?`, workItemID, result.AttemptID).Scan(&receiptDigest)
		if err == nil {
			if receiptDigest == resultDigest {
				return nil
			}
			return errors.New("verifier replay conflicts with durable result identity")
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		activeAttempt := state == string(StateActive) && attemptState == string(AttemptRunning)
		waitingAttempt := state == string(StateWaiting) && (attemptState == string(AttemptSucceeded) || attemptState == string(AttemptRetryScheduled))
		if !activeAttempt && !waitingAttempt {
			return errors.New("verifier result requires its active or completed waiting attempt")
		}
		if _, err = conn.ExecContext(ctx, `INSERT INTO verifier_result_receipts(work_item_id,attempt_id,result_digest,recorded_at) VALUES(?,?,?,?)`, workItemID, result.AttemptID, resultDigest, millis(now)); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `INSERT INTO verifier_results(work_item_id,attempt_id,result_digest,verifier_id,completion_contract,contract_digest,task_evidence_digest,head_revision,evaluation_revision,outcome,reason_codes_json,evidence_refs_json,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_item_id) DO UPDATE SET attempt_id=excluded.attempt_id,result_digest=excluded.result_digest,verifier_id=excluded.verifier_id,completion_contract=excluded.completion_contract,contract_digest=excluded.contract_digest,task_evidence_digest=excluded.task_evidence_digest,head_revision=excluded.head_revision,evaluation_revision=excluded.evaluation_revision,outcome=excluded.outcome,reason_codes_json=excluded.reason_codes_json,evidence_refs_json=excluded.evidence_refs_json,recorded_at=excluded.recorded_at`, workItemID, result.AttemptID, resultDigest, verifierID, contract, result.ContractDigest, result.TaskEvidenceDigest, result.HeadRevision, result.EvaluationRevision, result.Outcome, string(reasonJSON), string(evidenceJSON), millis(now))
		if err != nil {
			return err
		}
		switch result.Outcome {
		case "satisfied":
			_, err = conn.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,continuation_count=?,terminal_at=?,next_attempt_at=NULL,updated_at=? WHERE id=? AND state=?`, StateCompleted, continuations, millis(now), millis(now), workItemID, StateWaiting)
		case "waiting":
			_, err = conn.ExecContext(ctx, `UPDATE work_items SET state_version=state_version+1,next_attempt_at=?,wait_deadline_at=COALESCE(wait_deadline_at,?),updated_at=? WHERE id=? AND state=?`, millis(now.Add(durablePollInterval)), millis(now.Add(durableWaitDeadline)), millis(now), workItemID, StateWaiting)
		case "continuation_required":
			_, err = conn.ExecContext(ctx, `UPDATE work_items SET state_version=state_version+1,continuation_count=?,next_attempt_at=?,updated_at=? WHERE id=? AND state=?`, continuations, millis(now.Add(durablePollInterval)), millis(now), workItemID, StateWaiting)
		case "escalated":
			_, err = conn.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,continuation_count=?,terminal_at=?,next_attempt_at=NULL,updated_at=? WHERE id=? AND state=?`, StateFailed, continuations, millis(now), millis(now), workItemID, StateWaiting)
		}
		if err != nil || !activeAttempt {
			return err
		}
		// A live agentd verdict arrives while the executor attempt still owns the
		// serialization lease.  Commit the attempt and work transition together;
		// Complete recognizes the durable verifier receipt as an idempotent handoff.
		finalAttemptState, workState, next := AttemptRetryScheduled, StateWaiting, any(millis(now.Add(durablePollInterval)))
		terminal := any(nil)
		if result.Outcome == "satisfied" {
			finalAttemptState, workState, terminal = AttemptSucceeded, StateCompleted, millis(now)
		}
		if result.Outcome == "escalated" {
			finalAttemptState, workState, terminal = AttemptFailed, StateFailed, millis(now)
		}
		if _, err = conn.ExecContext(ctx, `UPDATE executor_attempts SET state=?,completed_at=? WHERE id=? AND state=?`, finalAttemptState, millis(now), result.AttemptID, AttemptRunning); err != nil {
			return err
		}
		if _, err = conn.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,continuation_count=?,terminal_at=?,next_attempt_at=?,wait_deadline_at=CASE WHEN ? THEN COALESCE(wait_deadline_at,?) ELSE wait_deadline_at END,updated_at=? WHERE id=? AND state=?`, workState, continuations, terminal, next, result.Outcome == "waiting", millis(now.Add(durableWaitDeadline)), millis(now), workItemID, StateActive); err != nil {
			return err
		}
		_, err = conn.ExecContext(ctx, `DELETE FROM serialization_leases WHERE attempt_id=?`, result.AttemptID)
		return err
	})
}
