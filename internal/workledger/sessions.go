package workledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"
)

type SessionBinding struct {
	WorkItemID, BindingKey, AuthorityProfile, AuthorityPolicyVersion, WorkerID, WorkerLineage, AgentdSessionID, CheckpointRef, State string
	EventCursor                                                                                                                      int64
	FenceEpoch                                                                                                                       int64
	CreatedAt, UpdatedAt                                                                                                             time.Time
}

type SessionLease struct {
	WorkerID, AuthorityProfile, AuthorityPolicyVersion, WorkerLineage string
	FenceEpoch                                                        int64
}
type Usage struct{ InputTokens, CachedInputTokens, OutputTokens, ReasoningOutputTokens, TotalTokens int64 }
type CoordinatorEvent struct {
	Cursor                      int64
	WorkerID, Kind, EvidenceRef string
	FenceEpoch                  int64
	Usage                       Usage
}

func (u Usage) Valid() bool {
	if u.InputTokens < 0 || u.CachedInputTokens < 0 || u.OutputTokens < 0 || u.ReasoningOutputTokens < 0 || u.TotalTokens < 0 {
		return false
	}
	if u.InputTokens > math.MaxInt64-u.CachedInputTokens || u.InputTokens+u.CachedInputTokens > math.MaxInt64-u.OutputTokens || u.InputTokens+u.CachedInputTokens+u.OutputTokens > math.MaxInt64-u.ReasoningOutputTokens {
		return false
	}
	return u.TotalTokens == u.InputTokens+u.CachedInputTokens+u.OutputTokens+u.ReasoningOutputTokens
}

func (store *Store) BindSession(ctx context.Context, workItemID, bindingKey, authorityProfile, workerID string, now time.Time) (SessionBinding, error) {
	if workItemID == "" || bindingKey == "" || authorityProfile == "" || workerID == "" {
		return SessionBinding{}, errors.New("session binding requires work item, key, authority profile, and worker")
	}
	return store.BindSessionLease(ctx, workItemID, bindingKey, SessionLease{WorkerID: workerID, AuthorityProfile: authorityProfile, AuthorityPolicyVersion: "legacy-v1", WorkerLineage: workerID, FenceEpoch: 1}, now)
}
func (store *Store) BindSessionLease(ctx context.Context, workItemID, bindingKey string, lease SessionLease, now time.Time) (SessionBinding, error) {
	if workItemID == "" || bindingKey == "" || !validLease(lease) {
		return SessionBinding{}, errors.New("session binding requires complete fenced broker lease")
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO session_bindings(work_item_id,binding_key,authority_profile,authority_policy_version,worker_id,worker_lineage,fence_epoch,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_item_id) DO NOTHING`, workItemID, bindingKey, lease.AuthorityProfile, lease.AuthorityPolicyVersion, lease.WorkerID, lease.WorkerLineage, lease.FenceEpoch, "active", millis(now), millis(now))
	if err != nil {
		return SessionBinding{}, err
	}
	b, err := store.SessionBinding(ctx, workItemID)
	if err != nil {
		return SessionBinding{}, err
	}
	if b.BindingKey != bindingKey || b.AuthorityProfile != lease.AuthorityProfile || b.WorkerID != lease.WorkerID || b.FenceEpoch != lease.FenceEpoch || b.WorkerLineage != lease.WorkerLineage || b.AuthorityPolicyVersion != lease.AuthorityPolicyVersion {
		return SessionBinding{}, fmt.Errorf("session binding conflicts with durable authority assignment")
	}
	return b, nil
}
func validLease(l SessionLease) bool {
	return l.WorkerID != "" && l.AuthorityProfile != "" && l.AuthorityPolicyVersion != "" && l.WorkerLineage != "" && l.FenceEpoch > 0
}

func (store *Store) SessionBinding(ctx context.Context, workItemID string) (SessionBinding, error) {
	var b SessionBinding
	var created, updated int64
	err := store.db.QueryRowContext(ctx, `SELECT work_item_id,binding_key,authority_profile,authority_policy_version,worker_id,worker_lineage,fence_epoch,agentd_session_id,checkpoint_ref,CAST(event_cursor AS INTEGER),state,created_at,updated_at FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&b.WorkItemID, &b.BindingKey, &b.AuthorityProfile, &b.AuthorityPolicyVersion, &b.WorkerID, &b.WorkerLineage, &b.FenceEpoch, &b.AgentdSessionID, &b.CheckpointRef, &b.EventCursor, &b.State, &created, &updated)
	if err != nil {
		return SessionBinding{}, err
	}
	b.CreatedAt, b.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return b, nil
}
func (store *Store) SetAgentdSession(ctx context.Context, workItemID string, lease SessionLease, sessionID string, now time.Time) error {
	if sessionID == "" || !validLease(lease) {
		return errors.New("complete broker session identity is required")
	}
	r, err := store.db.ExecContext(ctx, `UPDATE session_bindings SET agentd_session_id=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND fence_epoch=? AND authority_policy_version=? AND worker_lineage=?`, sessionID, millis(now), workItemID, lease.WorkerID, lease.FenceEpoch, lease.AuthorityPolicyVersion, lease.WorkerLineage)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n != 1 {
		return errors.New("stale broker session cannot be recorded")
	}
	return nil
}

// RecordCoordinatorEvent uses a SQLite immediate transaction: event insertion and
// cursor CAS are one write critical section, including reassignment races.
func (store *Store) RecordCoordinatorEvent(ctx context.Context, workItemID string, event CoordinatorEvent, now time.Time) (bool, error) {
	if event.Cursor <= 0 || event.WorkerID == "" || event.FenceEpoch <= 0 || event.Kind == "" || !event.Usage.Valid() {
		return false, errors.New("invalid normalized coordinator event")
	}
	inserted := false
	err := store.immediate(ctx, func(conn *sql.Conn) error {
		var key, worker, policy, lineage string
		var epoch, cursor int64
		if err := conn.QueryRowContext(ctx, `SELECT binding_key,worker_id,authority_policy_version,worker_lineage,fence_epoch,CAST(event_cursor AS INTEGER) FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&key, &worker, &policy, &lineage, &epoch, &cursor); err != nil {
			return err
		}
		if worker != event.WorkerID || epoch != event.FenceEpoch {
			return errors.New("stale predecessor event rejected")
		}
		if event.Cursor <= cursor && cursor != 0 {
			// Replays can start before the durable cursor after a restart, but
			// only an already-recorded byte-for-byte normalized event is valid.
			return store.matchCoordinatorEvent(ctx, conn, key, event)
		}
		r, err := conn.ExecContext(ctx, `INSERT INTO coordinator_events(binding_key,cursor,worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(binding_key,cursor) DO NOTHING`, key, event.Cursor, event.WorkerID, event.FenceEpoch, event.Kind, event.EvidenceRef, event.Usage.InputTokens, event.Usage.CachedInputTokens, event.Usage.OutputTokens, event.Usage.ReasoningOutputTokens, event.Usage.TotalTokens, millis(now))
		if err != nil {
			return err
		}
		n, _ := r.RowsAffected()
		if n == 0 {
			return store.matchCoordinatorEvent(ctx, conn, key, event)
		}
		r, err = conn.ExecContext(ctx, `UPDATE session_bindings SET event_cursor=?,updated_at=? WHERE work_item_id=? AND worker_id=? AND fence_epoch=? AND authority_policy_version=? AND worker_lineage=? AND event_cursor=?`, event.Cursor, millis(now), workItemID, event.WorkerID, event.FenceEpoch, policy, lineage, cursor)
		if err != nil {
			return err
		}
		n, _ = r.RowsAffected()
		if n != 1 {
			return errors.New("coordinator cursor CAS lost")
		}
		inserted = true
		return nil
	})
	return inserted, err
}
func (store *Store) matchCoordinatorEvent(ctx context.Context, conn *sql.Conn, key string, e CoordinatorEvent) error {
	var worker, kind, evidence string
	var epoch int64
	var u Usage
	if err := conn.QueryRowContext(ctx, `SELECT worker_id,fence_epoch,event_kind,evidence_ref,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens FROM coordinator_events WHERE binding_key=? AND cursor=?`, key, e.Cursor).Scan(&worker, &epoch, &kind, &evidence, &u.InputTokens, &u.CachedInputTokens, &u.OutputTokens, &u.ReasoningOutputTokens, &u.TotalTokens); err != nil {
		return err
	}
	if worker != e.WorkerID || epoch != e.FenceEpoch || kind != e.Kind || evidence != e.EvidenceRef || u != e.Usage {
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

func (store *Store) ReassignSession(ctx context.Context, workItemID, predecessorWorker string, predecessorEpoch int64, successor SessionLease, now time.Time) (SessionBinding, error) {
	if !validLease(successor) || successor.FenceEpoch != predecessorEpoch+1 {
		return SessionBinding{}, errors.New("successor fence epoch must equal predecessor plus one")
	}
	err := store.immediate(ctx, func(conn *sql.Conn) error {
		var profile, policy, lineage, worker string
		var epoch int64
		err := conn.QueryRowContext(ctx, `SELECT authority_profile,authority_policy_version,worker_lineage,worker_id,fence_epoch FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&profile, &policy, &lineage, &worker, &epoch)
		if err != nil {
			return err
		}
		if worker == successor.WorkerID && epoch == successor.FenceEpoch {
			if profile == successor.AuthorityProfile && policy == successor.AuthorityPolicyVersion && lineage == successor.WorkerLineage {
				return nil
			}
			return errors.New("broker reassignment replay conflicts with durable successor")
		}
		if worker != predecessorWorker || epoch != predecessorEpoch {
			return errors.New("reassignment predecessor CAS lost")
		}
		if profile != successor.AuthorityProfile || policy != successor.AuthorityPolicyVersion || lineage != successor.WorkerLineage {
			return errors.New("successor changes authority policy or storage lineage")
		}
		r, err := conn.ExecContext(ctx, `UPDATE session_bindings SET worker_id=?,fence_epoch=?,state='active',updated_at=? WHERE work_item_id=? AND worker_id=? AND fence_epoch=? AND authority_policy_version=? AND worker_lineage=?`, successor.WorkerID, successor.FenceEpoch, millis(now), workItemID, predecessorWorker, predecessorEpoch, policy, lineage)
		if err != nil {
			return err
		}
		n, _ := r.RowsAffected()
		if n != 1 {
			return errors.New("reassignment CAS lost")
		}
		return nil
	})
	if err != nil {
		return SessionBinding{}, err
	}
	return store.SessionBinding(ctx, workItemID)
}

func (store *Store) CoordinatorUsage(ctx context.Context, workItemID string) (usageEvents int64, usage Usage, err error) {
	err = store.db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(e.input_tokens),0),coalesce(sum(e.cached_input_tokens),0),coalesce(sum(e.output_tokens),0),coalesce(sum(e.reasoning_output_tokens),0),coalesce(sum(e.total_tokens),0) FROM coordinator_events e JOIN session_bindings b ON b.binding_key=e.binding_key WHERE b.work_item_id=? AND e.event_kind='usage'`, workItemID).Scan(&usageEvents, &usage.InputTokens, &usage.CachedInputTokens, &usage.OutputTokens, &usage.ReasoningOutputTokens, &usage.TotalTokens)
	return
}
