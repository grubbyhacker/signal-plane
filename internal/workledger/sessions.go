package workledger

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type SessionBinding struct {
	WorkItemID, BindingKey, AuthorityProfile, WorkerID, CheckpointRef, State string
	CreatedAt, UpdatedAt                                                     time.Time
}

func (store *Store) BindSession(ctx context.Context, workItemID, bindingKey, authorityProfile, workerID string, now time.Time) (SessionBinding, error) {
	if workItemID == "" || bindingKey == "" || authorityProfile == "" || workerID == "" {
		return SessionBinding{}, errors.New("session binding requires work item, key, authority profile, and worker")
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO session_bindings(work_item_id,binding_key,authority_profile,worker_id,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(work_item_id) DO NOTHING`, workItemID, bindingKey, authorityProfile, workerID, "active", millis(now), millis(now))
	if err != nil {
		return SessionBinding{}, err
	}
	var binding SessionBinding
	var created, updated int64
	err = store.db.QueryRowContext(ctx, `SELECT work_item_id,binding_key,authority_profile,worker_id,checkpoint_ref,state,created_at,updated_at FROM session_bindings WHERE work_item_id=?`, workItemID).Scan(&binding.WorkItemID, &binding.BindingKey, &binding.AuthorityProfile, &binding.WorkerID, &binding.CheckpointRef, &binding.State, &created, &updated)
	if err != nil {
		return SessionBinding{}, err
	}
	if binding.BindingKey != bindingKey || binding.AuthorityProfile != authorityProfile || binding.WorkerID != workerID {
		return SessionBinding{}, fmt.Errorf("session binding conflicts with durable authority assignment")
	}
	binding.CreatedAt, binding.UpdatedAt = time.UnixMilli(created).UTC(), time.UnixMilli(updated).UTC()
	return binding, nil
}
