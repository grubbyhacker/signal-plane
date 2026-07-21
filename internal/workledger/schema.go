package workledger

import (
	"context"
	"database/sql"
	"fmt"
)

// Migrate adds the generalized ledger alongside the legacy proof tables. It is
// additive so an existing dispatcher database retains all recovery evidence.
func Migrate(ctx context.Context, db *sql.DB) error {
	var priorVersion int
	if err := db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&priorVersion); err != nil {
		return fmt.Errorf("read work ledger schema version: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin work ledger migration: %w", err)
	}
	defer tx.Rollback()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS route_snapshots (id TEXT PRIMARY KEY, route_id TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version>0), semantic_version TEXT NOT NULL, digest TEXT NOT NULL, executor_id TEXT NOT NULL, executor_kind TEXT NOT NULL CHECK(executor_kind IN ('deterministic_tool','policy_evaluator','agent_session')), executor_version TEXT NOT NULL, task_kind TEXT NOT NULL DEFAULT '', task_version TEXT NOT NULL DEFAULT '', completion_contract TEXT NOT NULL DEFAULT '', verifier_id TEXT NOT NULL DEFAULT '', task_contract_digest TEXT NOT NULL DEFAULT '', definition_json TEXT NOT NULL, activated_at INTEGER NOT NULL, retired_at INTEGER CHECK(retired_at IS NULL OR retired_at>=activated_at))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS active_route_snapshot ON route_snapshots(route_id) WHERE retired_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS work_items (id TEXT PRIMARY KEY, route_snapshot_id TEXT NOT NULL REFERENCES route_snapshots(id), route_id TEXT NOT NULL, semantic_object_key TEXT NOT NULL, source TEXT NOT NULL, namespace TEXT NOT NULL, object_kind TEXT NOT NULL, object_id TEXT NOT NULL, source_revision TEXT NOT NULL, serialization_key TEXT NOT NULL, task_evidence_digest TEXT NOT NULL DEFAULT '', continuation_count INTEGER NOT NULL DEFAULT 0 CHECK(continuation_count>=0), state TEXT NOT NULL CHECK(state IN ('observed','admitted','active','waiting','completed','failed','cancelled','superseded','dead_letter')), state_version INTEGER NOT NULL DEFAULT 1 CHECK(state_version>0), superseded_by_id TEXT REFERENCES work_items(id), latest_executor_correlation TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, terminal_at INTEGER, next_attempt_at INTEGER, CHECK(state<>'superseded' OR superseded_by_id IS NOT NULL), CHECK(state NOT IN ('completed','failed','cancelled','superseded','dead_letter') OR terminal_at IS NOT NULL))`,
		`CREATE INDEX IF NOT EXISTS work_items_due ON work_items(state,next_attempt_at,created_at)`,
		`CREATE INDEX IF NOT EXISTS work_items_object ON work_items(route_id,semantic_object_key,created_at)`,
		`CREATE TABLE IF NOT EXISTS work_events (id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL REFERENCES work_items(id), signal_id TEXT NOT NULL, source_delivery_id TEXT NOT NULL, event_digest TEXT NOT NULL, transport_stream TEXT NOT NULL, transport_sequence INTEGER NOT NULL CHECK(transport_sequence>0), source TEXT NOT NULL, namespace TEXT NOT NULL, object_kind TEXT NOT NULL, object_id TEXT NOT NULL, event_kind TEXT NOT NULL, action TEXT NOT NULL, actor_class TEXT NOT NULL, source_revision TEXT NOT NULL, correlation_id TEXT NOT NULL, causation_id TEXT NOT NULL, root_work_item_id TEXT NOT NULL, parent_work_item_id TEXT NOT NULL, originating_session TEXT NOT NULL, originating_turn TEXT NOT NULL, hop_count INTEGER NOT NULL CHECK(hop_count>=0), expires_at INTEGER, payload_digest TEXT NOT NULL, evidence_ref TEXT NOT NULL, admission_outcome TEXT NOT NULL CHECK(admission_outcome IN ('admitted','duplicate')), received_at INTEGER NOT NULL, recorded_at INTEGER NOT NULL, UNIQUE(source,namespace,source_delivery_id), UNIQUE(transport_stream,transport_sequence))`,
		`CREATE TABLE IF NOT EXISTS executor_attempts (id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL REFERENCES work_items(id), attempt_number INTEGER NOT NULL CHECK(attempt_number>0), executor_id TEXT NOT NULL, executor_kind TEXT NOT NULL CHECK(executor_kind IN ('deterministic_tool','policy_evaluator','agent_session')), executor_version TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, operation_idempotency_key TEXT NOT NULL DEFAULT '', requested_operation_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('running','recoverable','retry_scheduled','succeeded','failed','interrupted','superseded')), retry_classification TEXT NOT NULL DEFAULT '', external_correlation TEXT NOT NULL DEFAULT '', result_digest TEXT NOT NULL DEFAULT '', sanitized_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, started_at INTEGER NOT NULL, completed_at INTEGER, UNIQUE(work_item_id,attempt_number))`,
		`CREATE TABLE IF NOT EXISTS serialization_leases (serialization_key TEXT PRIMARY KEY, work_item_id TEXT NOT NULL UNIQUE REFERENCES work_items(id), attempt_id TEXT NOT NULL UNIQUE REFERENCES executor_attempts(id), acquired_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS release_operations (work_item_id TEXT PRIMARY KEY REFERENCES work_items(id), repository TEXT NOT NULL, repository_id INTEGER NOT NULL, installation_id INTEGER NOT NULL, release_id INTEGER NOT NULL, tag TEXT NOT NULL, published_at TEXT NOT NULL, target_commitish TEXT NOT NULL, commit_sha TEXT NOT NULL, asset_id INTEGER NOT NULL, asset_name TEXT NOT NULL, asset_size INTEGER NOT NULL, asset_content_type TEXT NOT NULL, provider_digest TEXT NOT NULL, computed_digest TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS content_results (computed_digest TEXT PRIMARY KEY, external_correlation TEXT NOT NULL, result_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS ingress_failures (source TEXT NOT NULL, namespace TEXT NOT NULL, source_delivery_id TEXT NOT NULL, event_digest TEXT NOT NULL, classification TEXT NOT NULL, attempts INTEGER NOT NULL, recorded_at INTEGER NOT NULL, PRIMARY KEY(source,namespace,source_delivery_id))`,
		`CREATE TABLE IF NOT EXISTS session_bindings (work_item_id TEXT PRIMARY KEY REFERENCES work_items(id), binding_key TEXT NOT NULL UNIQUE, authority_profile TEXT NOT NULL, profile_version TEXT NOT NULL DEFAULT '', policy_digest TEXT NOT NULL DEFAULT '', session_lineage_id TEXT NOT NULL DEFAULT '', worker_id TEXT NOT NULL DEFAULT '', worker_storage_lineage_id TEXT NOT NULL DEFAULT '', worker_fence_epoch INTEGER NOT NULL DEFAULT 1, agentd_session_id TEXT NOT NULL DEFAULT '', registered_submit_key TEXT NOT NULL DEFAULT '', checkpoint_ref TEXT NOT NULL DEFAULT '', event_cursor INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL CHECK(state IN ('pending','active','reassigning','checkpointed','terminated','failed')), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS coordinator_events (binding_key TEXT NOT NULL REFERENCES session_bindings(binding_key), cursor INTEGER NOT NULL CHECK(cursor>0), worker_id TEXT NOT NULL, fence_epoch INTEGER NOT NULL CHECK(fence_epoch>0), event_kind TEXT NOT NULL, evidence_ref TEXT NOT NULL DEFAULT '', input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens>=0), cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(cached_input_tokens>=0), output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(output_tokens>=0), reasoning_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(reasoning_output_tokens>=0), total_tokens INTEGER NOT NULL DEFAULT 0 CHECK(total_tokens>=0), recorded_at INTEGER NOT NULL, PRIMARY KEY(binding_key,cursor))`,
		`CREATE TABLE IF NOT EXISTS coordinator_reassignments (work_item_id TEXT NOT NULL REFERENCES work_items(id), predecessor_fence_epoch INTEGER NOT NULL CHECK(predecessor_fence_epoch>0), idempotency_key TEXT NOT NULL, rebind_idempotency_key TEXT NOT NULL DEFAULT '', phase TEXT NOT NULL CHECK(phase IN ('requested','broker_committed','agentd_adopted','coordinator_committed','escalated')), session_lineage_id TEXT NOT NULL, authority_profile TEXT NOT NULL, profile_version TEXT NOT NULL, policy_digest TEXT NOT NULL, storage_lineage_id TEXT NOT NULL, predecessor_worker_id TEXT NOT NULL, successor_worker_id TEXT NOT NULL DEFAULT '', successor_fence_epoch INTEGER NOT NULL DEFAULT 0, broker_state TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, PRIMARY KEY(work_item_id,predecessor_fence_epoch), UNIQUE(idempotency_key))`,
		`CREATE TABLE IF NOT EXISTS verifier_results (work_item_id TEXT PRIMARY KEY REFERENCES work_items(id), attempt_id TEXT NOT NULL DEFAULT '', result_digest TEXT NOT NULL DEFAULT '', verifier_id TEXT NOT NULL, completion_contract TEXT NOT NULL, contract_digest TEXT NOT NULL, task_evidence_digest TEXT NOT NULL, head_revision TEXT NOT NULL, evaluation_revision TEXT NOT NULL DEFAULT '', outcome TEXT NOT NULL CHECK(outcome IN ('waiting','continuation_required','satisfied','escalated')), reason_codes_json TEXT NOT NULL, evidence_refs_json TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS verifier_result_receipts (work_item_id TEXT NOT NULL REFERENCES work_items(id), attempt_id TEXT NOT NULL REFERENCES executor_attempts(id), result_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL, PRIMARY KEY(work_item_id,attempt_id))`,
		`PRAGMA user_version=14`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate work ledger: %w", err)
		}
	}
	if err := ensureMigrationColumn(ctx, tx, "executor_attempts", "operation_idempotency_key", `operation_idempotency_key TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	for _, column := range []struct{ name, definition string }{
		{"task_kind", `task_kind TEXT NOT NULL DEFAULT ''`},
		{"task_version", `task_version TEXT NOT NULL DEFAULT ''`},
		{"completion_contract", `completion_contract TEXT NOT NULL DEFAULT ''`},
		{"verifier_id", `verifier_id TEXT NOT NULL DEFAULT ''`},
		{"task_contract_digest", `task_contract_digest TEXT NOT NULL DEFAULT ''`},
	} {
		if err := ensureMigrationColumn(ctx, tx, "route_snapshots", column.name, column.definition); err != nil {
			return err
		}
	}
	if priorVersion < 11 {
		if err := migrateVerifierResultsV11(ctx, tx); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{
		{"task_evidence_digest", `task_evidence_digest TEXT NOT NULL DEFAULT ''`},
		{"continuation_count", `continuation_count INTEGER NOT NULL DEFAULT 0`},
		{"wait_deadline_at", `wait_deadline_at INTEGER`},
	} {
		if err := ensureMigrationColumn(ctx, tx, "work_items", column.name, column.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{
		{"profile_version", `profile_version TEXT NOT NULL DEFAULT ''`},
		{"policy_digest", `policy_digest TEXT NOT NULL DEFAULT ''`},
		{"session_lineage_id", `session_lineage_id TEXT NOT NULL DEFAULT ''`},
		{"worker_storage_lineage_id", `worker_storage_lineage_id TEXT NOT NULL DEFAULT ''`},
		{"worker_fence_epoch", `worker_fence_epoch INTEGER NOT NULL DEFAULT 1`},
		{"agentd_session_id", `agentd_session_id TEXT NOT NULL DEFAULT ''`},
		{"registered_submit_key", `registered_submit_key TEXT NOT NULL DEFAULT ''`},
		{"submitted_idempotency_key", `submitted_idempotency_key TEXT NOT NULL DEFAULT ''`},
		{"model_effect_id", `model_effect_id TEXT NOT NULL DEFAULT ''`},
		{"submitted_turn_id", `submitted_turn_id TEXT NOT NULL DEFAULT ''`},
		{"event_cursor", `event_cursor INTEGER NOT NULL DEFAULT 0`},
	} {
		if err := ensureMigrationColumn(ctx, tx, "session_bindings", column.name, column.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{
		{"cached_input_tokens", `cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(cached_input_tokens>=0)`},
		{"reasoning_output_tokens", `reasoning_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(reasoning_output_tokens>=0)`},
		{"total_tokens", `total_tokens INTEGER NOT NULL DEFAULT 0 CHECK(total_tokens>=0)`},
	} {
		if err := ensureMigrationColumn(ctx, tx, "coordinator_events", column.name, column.definition); err != nil {
			return err
		}
	}
	for _, column := range []struct{ name, definition string }{
		{"attempt_id", `attempt_id TEXT NOT NULL DEFAULT ''`},
		{"result_digest", `result_digest TEXT NOT NULL DEFAULT ''`},
		{"evaluation_revision", `evaluation_revision TEXT NOT NULL DEFAULT ''`},
	} {
		if err := ensureMigrationColumn(ctx, tx, "verifier_results", column.name, column.definition); err != nil {
			return err
		}
	}
	// v6 stored the cursor as TEXT with an empty-string default. Normalize that
	// legacy sentinel before the int64 cursor reader takes over.
	if _, err := tx.ExecContext(ctx, `UPDATE session_bindings SET event_cursor=0 WHERE CAST(event_cursor AS TEXT)=''`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE session_bindings SET registered_submit_key=submitted_idempotency_key WHERE registered_submit_key='' AND submitted_idempotency_key<>''`); err != nil {
		return fmt.Errorf("backfill registered submit keys: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit work ledger migration: %w", err)
	}
	return nil
}

func migrateVerifierResultsV11(ctx context.Context, tx *sql.Tx) error {
	statements := []string{
		`CREATE TABLE verifier_results_v11 (work_item_id TEXT PRIMARY KEY REFERENCES work_items(id), attempt_id TEXT NOT NULL DEFAULT '', result_digest TEXT NOT NULL DEFAULT '', verifier_id TEXT NOT NULL, completion_contract TEXT NOT NULL, contract_digest TEXT NOT NULL, task_evidence_digest TEXT NOT NULL, head_revision TEXT NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN ('waiting','continuation_required','satisfied','escalated')), reason_codes_json TEXT NOT NULL, evidence_refs_json TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`INSERT INTO verifier_results_v11(work_item_id,attempt_id,result_digest,verifier_id,completion_contract,contract_digest,task_evidence_digest,head_revision,outcome,reason_codes_json,evidence_refs_json,recorded_at) SELECT work_item_id,attempt_id,result_digest,verifier_id,completion_contract,contract_digest,task_evidence_digest,head_revision,CASE outcome WHEN 'continuation' THEN 'continuation_required' WHEN 'missing_or_stale' THEN 'continuation_required' ELSE outcome END,reason_codes_json,evidence_refs_json,recorded_at FROM verifier_results`,
		`DROP TABLE verifier_results`,
		`ALTER TABLE verifier_results_v11 RENAME TO verifier_results`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate verifier results v11: %w", err)
		}
	}
	return nil
}

func ensureMigrationColumn(ctx context.Context, tx *sql.Tx, table, column, definition string) error {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		found = found || name == column
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = tx.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+definition)
	return err
}
