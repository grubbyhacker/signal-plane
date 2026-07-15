package workledger

import (
	"context"
	"database/sql"
	"fmt"
)

// Migrate adds the generalized ledger alongside the legacy proof tables. It is
// additive so an existing dispatcher database retains all recovery evidence.
func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin work ledger migration: %w", err)
	}
	defer tx.Rollback()
	statements := []string{
		`CREATE TABLE IF NOT EXISTS route_snapshots (id TEXT PRIMARY KEY, route_id TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version>0), semantic_version TEXT NOT NULL, digest TEXT NOT NULL, executor_id TEXT NOT NULL, executor_kind TEXT NOT NULL CHECK(executor_kind IN ('deterministic_tool','policy_evaluator','agent_session')), executor_version TEXT NOT NULL, definition_json TEXT NOT NULL, activated_at INTEGER NOT NULL, retired_at INTEGER CHECK(retired_at IS NULL OR retired_at>=activated_at))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS active_route_snapshot ON route_snapshots(route_id) WHERE retired_at IS NULL`,
		`CREATE TABLE IF NOT EXISTS work_items (id TEXT PRIMARY KEY, route_snapshot_id TEXT NOT NULL REFERENCES route_snapshots(id), route_id TEXT NOT NULL, semantic_object_key TEXT NOT NULL, source TEXT NOT NULL, namespace TEXT NOT NULL, object_kind TEXT NOT NULL, object_id TEXT NOT NULL, source_revision TEXT NOT NULL, serialization_key TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('observed','admitted','active','waiting','completed','failed','cancelled','superseded','dead_letter')), state_version INTEGER NOT NULL DEFAULT 1 CHECK(state_version>0), superseded_by_id TEXT REFERENCES work_items(id), latest_executor_correlation TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, terminal_at INTEGER, next_attempt_at INTEGER, CHECK(state<>'superseded' OR superseded_by_id IS NOT NULL), CHECK(state NOT IN ('completed','failed','cancelled','superseded','dead_letter') OR terminal_at IS NOT NULL))`,
		`CREATE INDEX IF NOT EXISTS work_items_due ON work_items(state,next_attempt_at,created_at)`,
		`CREATE INDEX IF NOT EXISTS work_items_object ON work_items(route_id,semantic_object_key,created_at)`,
		`CREATE TABLE IF NOT EXISTS work_events (id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL REFERENCES work_items(id), signal_id TEXT NOT NULL, source_delivery_id TEXT NOT NULL, event_digest TEXT NOT NULL, transport_stream TEXT NOT NULL, transport_sequence INTEGER NOT NULL CHECK(transport_sequence>0), source TEXT NOT NULL, namespace TEXT NOT NULL, object_kind TEXT NOT NULL, object_id TEXT NOT NULL, event_kind TEXT NOT NULL, action TEXT NOT NULL, actor_class TEXT NOT NULL, source_revision TEXT NOT NULL, correlation_id TEXT NOT NULL, causation_id TEXT NOT NULL, root_work_item_id TEXT NOT NULL, parent_work_item_id TEXT NOT NULL, originating_session TEXT NOT NULL, originating_turn TEXT NOT NULL, hop_count INTEGER NOT NULL CHECK(hop_count>=0), expires_at INTEGER, payload_digest TEXT NOT NULL, evidence_ref TEXT NOT NULL, admission_outcome TEXT NOT NULL CHECK(admission_outcome IN ('admitted','duplicate')), received_at INTEGER NOT NULL, recorded_at INTEGER NOT NULL, UNIQUE(source,namespace,source_delivery_id), UNIQUE(transport_stream,transport_sequence))`,
		`CREATE TABLE IF NOT EXISTS executor_attempts (id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL REFERENCES work_items(id), attempt_number INTEGER NOT NULL CHECK(attempt_number>0), executor_id TEXT NOT NULL, executor_kind TEXT NOT NULL CHECK(executor_kind IN ('deterministic_tool','policy_evaluator','agent_session')), executor_version TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, operation_idempotency_key TEXT NOT NULL DEFAULT '', requested_operation_digest TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('running','recoverable','retry_scheduled','succeeded','failed','interrupted','superseded')), retry_classification TEXT NOT NULL DEFAULT '', external_correlation TEXT NOT NULL DEFAULT '', result_digest TEXT NOT NULL DEFAULT '', sanitized_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, started_at INTEGER NOT NULL, completed_at INTEGER, UNIQUE(work_item_id,attempt_number))`,
		`CREATE TABLE IF NOT EXISTS serialization_leases (serialization_key TEXT PRIMARY KEY, work_item_id TEXT NOT NULL UNIQUE REFERENCES work_items(id), attempt_id TEXT NOT NULL UNIQUE REFERENCES executor_attempts(id), acquired_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS release_operations (work_item_id TEXT PRIMARY KEY REFERENCES work_items(id), repository TEXT NOT NULL, repository_id INTEGER NOT NULL, installation_id INTEGER NOT NULL, release_id INTEGER NOT NULL, tag TEXT NOT NULL, published_at TEXT NOT NULL, target_commitish TEXT NOT NULL, commit_sha TEXT NOT NULL, asset_id INTEGER NOT NULL, asset_name TEXT NOT NULL, asset_size INTEGER NOT NULL, asset_content_type TEXT NOT NULL, provider_digest TEXT NOT NULL, computed_digest TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS content_results (computed_digest TEXT PRIMARY KEY, external_correlation TEXT NOT NULL, result_digest TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS ingress_failures (source TEXT NOT NULL, namespace TEXT NOT NULL, source_delivery_id TEXT NOT NULL, event_digest TEXT NOT NULL, classification TEXT NOT NULL, attempts INTEGER NOT NULL, recorded_at INTEGER NOT NULL, PRIMARY KEY(source,namespace,source_delivery_id))`,
		`PRAGMA user_version=4`,
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate work ledger: %w", err)
		}
	}
	if err := ensureMigrationColumn(ctx, tx, "executor_attempts", "operation_idempotency_key", `operation_idempotency_key TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit work ledger migration: %w", err)
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
