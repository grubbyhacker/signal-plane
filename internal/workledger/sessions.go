package workledger

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type SessionBinding struct {
	WorkItemID, BindingKey, AuthorityProfile, AuthorityPolicyVersion, WorkerID, WorkerLineage, AgentdSessionID, CheckpointRef, EventCursor, State string
	FenceEpoch                                                                                                                                    int64
	CreatedAt, UpdatedAt                                                                                                                          time.Time
}

type SessionLease struct {
	WorkerID, AuthorityProfile, AuthorityPolicyVersion, WorkerLineage string
	FenceEpoch                                                        int64
}

type CoordinatorEvent struct {
	Cursor, WorkerID, Kind, EvidenceRef string
	FenceEpoch                          int64
	InputTokens, OutputTokens           int64
}

func (store *Store) BindSession(ctx context.Context, workItemID, bindingKey, authorityProfile, workerID string, now time.Time) (SessionBinding, error) {
	if workItemID == "" || bindingKey == "" || authorityProfile == "" || workerID == "" {
		return SessionBinding{}, errors.New("session binding requires work item, key, authority profile, and worker")
	}
	return store.BindSessionLease(ctx, workItemID, bindingKey, SessionLease{WorkerID: workerID, AuthorityProfile: authorityProfile, AuthorityPolicyVersion: "legacy-v1", WorkerLineage: workerID, FenceEpoch: 1}, now)
}

func (store *Store) BindSessionLease(ctx context.Context, workItemID, bindingKey string, lease SessionLease, now time.Time) (SessionBinding, error) {
	if workItemID == "" || bindingKey == "" || lease.WorkerID == "" || lease.AuthorityProfile == "" || lease.AuthorityPolicyVersion == "" || lease.WorkerLineage == "" || lease.FenceEpoch <= 0 {
		return SessionBinding{}, errors.New("session binding requires complete fenced broker lease")
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO session_bindings(work_item_id,binding_key,authority_profile,authority_policy_version,worker_id,worker_lineage,fence_epoch,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_item_id) DO NOTHING`, workItemID, bindingKey, lease.AuthorityProfile, lease.AuthorityPolicyVersion, lease.WorkerID, lease.WorkerLineage, lease.FenceEpoch, "active", millis(now), millis(now))
	if err != nil {
		return SessionBinding{}, err
	}
	binding, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return SessionBinding{}, err
	}
	if binding.BindingKey != bindingKey || binding.AuthorityProfile != lease.AuthorityProfile || binding.WorkerID != lease.WorkerID || binding.FenceEpoch != lease.FenceEpoch || binding.WorkerLineage != lease.WorkerLineage || binding.AuthorityPolicyVersion != lease.AuthorityPolicyVersion {
		return SessionBinding{}, fmt.Errorf("session binding conflicts with durable authority assignment")
	}
	return binding, nil
}

func (store *Store) SessionBinding(ctx context.Context, workItemID string) (SessionBinding, error) {
	var binding SessionBinding
	var created, updated int64
	err := store.db.QueryRowContext(ctx, `SELECT work_item_id,binding_key,authority_profile,authority_policy_version,worker_id,worker_lineage,fence_epoch,agentd_session_id,checkpoint_ref,event_cursor,state,created_at,updated_at FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&binding.WorkItemID, &binding.BindingKey, &binding.AuthorityProfile, &binding.AuthorityPolicyVersion, &binding.WorkerID, &binding.WorkerLineage, &binding.FenceEpoch, &binding.AgentdSessionID, &binding.CheckpointRef, &binding.EventCursor, &binding.State, &created, &updated)
	if err != nil {
		return SessionBinding{}, err
	}
	binding.CreatedAt, binding.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return binding, nil
}

func (store *Store) SetAgentdSession(ctx context.Context, workItemID, workerID string, fenceEpoch int64, sessionID string, now time.Time) error {
	if sessionID == "" {
		return errors.New("agentd session id is required")
	}
	r, err := store.db.ExecContext(ctx, `UPDATE session_bindings SET agentd_session_id=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND fence_epoch=?`, sessionID, millis(now), workItemID, workerID, fenceEpoch)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return errors.New("stale worker cannot set agentd session")
	}
	return nil
}

// RecordCoordinatorEvent atomically advances the durable cursor. Duplicate
// cursors are no-ops only when they belong to the current fenced worker.
func (store *Store) RecordCoordinatorEvent(ctx context.Context, workItemID string, event CoordinatorEvent, now time.Time) (bool, error) {
	if event.Cursor == "" || event.WorkerID == "" || event.FenceEpoch <= 0 || event.Kind == "" || event.InputTokens < 0 || event.OutputTokens < 0 {
		return false, errors.New("invalid normalized coordinator event")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var bindingKey, workerID string
	var epoch int64
	if err = tx.QueryRowContext(ctx, `SELECT binding_key,worker_id,fence_epoch FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&bindingKey, &workerID, &epoch); err != nil {
		return false, err
	}
	if workerID != event.WorkerID || epoch != event.FenceEpoch {
		return false, errors.New("stale predecessor event rejected")
	}
	r, err := tx.ExecContext(ctx, `INSERT INTO coordinator_events(binding_key,cursor,worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,output_tokens,recorded_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(binding_key,cursor) DO NOTHING`, bindingKey, event.Cursor, event.WorkerID, event.FenceEpoch, event.Kind, event.EvidenceRef, event.InputTokens, event.OutputTokens, millis(now))
	if err != nil {
		return false, err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		var worker, kind, evidence string
		var epoch, input, output int64
		if err = tx.QueryRowContext(ctx, `SELECT worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,output_tokens FROM coordinator_events WHERE binding_key=? AND cursor=?`, bindingKey, event.Cursor).Scan(&worker, &epoch, &kind, &evidence, &input, &output); err != nil {
			return false, err
		}
		if worker != event.WorkerID || epoch != event.FenceEpoch || kind != event.Kind || evidence != event.EvidenceRef || input != event.InputTokens || output != event.OutputTokens {
			return false, errors.New("duplicate coordinator cursor conflicts with recorded event")
		}
		return false, tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE session_bindings SET event_cursor=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND fence_epoch=?`, event.Cursor, millis(now), workItemID, event.WorkerID, event.FenceEpoch); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

// ReassignSession is the cutover CAS. Call it only with the broker's
// replacement record; callers never select the successor worker themselves.
func (store *Store) ReassignSession(ctx context.Context, workItemID, predecessorWorker string, predecessorEpoch int64, successor SessionLease, now time.Time) (SessionBinding, error) {
	if successor.WorkerID == "" || successor.FenceEpoch <= predecessorEpoch || successor.AuthorityProfile == "" || successor.AuthorityPolicyVersion == "" || successor.WorkerLineage == "" {
		return SessionBinding{}, errors.New("invalid broker successor lease")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionBinding{}, err
	}
	defer tx.Rollback()
	var profile, policy, lineage string
	if err = tx.QueryRowContext(ctx, `SELECT authority_profile,authority_policy_version,worker_lineage FROM session_bindings WHERE work_item_id=? AND worker_id=? AND fence_epoch=?`, workItemID, predecessorWorker, predecessorEpoch).Scan(&profile, &policy, &lineage); err != nil {
		return SessionBinding{}, fmt.Errorf("reassignment predecessor CAS: %w", err)
	}
	if profile != successor.AuthorityProfile || policy != successor.AuthorityPolicyVersion || lineage != successor.WorkerLineage {
		return SessionBinding{}, errors.New("successor changes authority policy or storage lineage")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE session_bindings SET worker_id=?,fence_epoch=?,agentd_session_id='',state='active',updated_at=? WHERE work_item_id=? AND worker_id=? AND fence_epoch=?`, successor.WorkerID, successor.FenceEpoch, millis(now), workItemID, predecessorWorker, predecessorEpoch); err != nil {
		return SessionBinding{}, err
	}
	if err = tx.Commit(); err != nil {
		return SessionBinding{}, err
	}
	return store.SessionBinding(ctx, workItemID)
}

func (store *Store) CoordinatorUsage(ctx context.Context, workItemID string) (events, inputTokens, outputTokens int64, err error) {
	err = store.db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(e.input_tokens),0),coalesce(sum(e.output_tokens),0) FROM coordinator_events e JOIN session_bindings b ON b.binding_key=e.binding_key WHERE b.work_item_id=?`, workItemID).Scan(&events, &inputTokens, &outputTokens)
	return
}
