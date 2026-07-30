package dispatcher

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestOpenStoreMigratesLegacyProofDatabaseToGeneralLedger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, semantic_key TEXT, stream_sequence INTEGER NOT NULL DEFAULT 0, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, first_launch_attempt_at INTEGER, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`INSERT INTO deliveries(delivery_id,outcome,semantic_key,stream_sequence,recorded_at) VALUES('legacy-delivery','selected','legacy-key',12,1)`,
		`INSERT INTO jobs(id,semantic_key,repository,issue_number,source_delivery_id,status,due_at,created_at,updated_at) VALUES(7,'legacy-key','example/automation-target',42,'legacy-delivery','completed',1,1,1)`,
		`PRAGMA user_version=2`,
	} {
		if _, err := legacy.Exec(statement); err != nil {
			legacy.Close()
			t.Fatal(err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, table := range []string{"deliveries", "jobs", "recovery_runs", "route_snapshots", "work_items", "work_events", "executor_attempts", "serialization_leases", "release_operations", "content_results", "ingress_failures"} {
		var count int
		if err := store.db.QueryRowContext(context.Background(), `SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("table %s missing: count=%d err=%v", table, count, err)
		}
	}
	var deliveryID, semanticKey string
	if err := store.db.QueryRow(`SELECT delivery_id,semantic_key FROM deliveries WHERE delivery_id='legacy-delivery'`).Scan(&deliveryID, &semanticKey); err != nil || semanticKey != "legacy-key" {
		t.Fatalf("legacy delivery was not preserved: %q %q %v", deliveryID, semanticKey, err)
	}
	var version int
	if err := store.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("schema version = %d, %v", version, err)
	}
}

func TestOpenStoreMigratesSchema16TerminalJobWithoutLosingEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema16.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, route_id TEXT NOT NULL DEFAULT '', launch_profile TEXT NOT NULL DEFAULT '', repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, first_launch_attempt_at INTEGER, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, semantic_key TEXT, stream_sequence INTEGER NOT NULL DEFAULT 0, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE dispatcher_metadata (key TEXT PRIMARY KEY,value INTEGER NOT NULL)`,
		`INSERT INTO dispatcher_metadata VALUES('last_persisted_jetstream_sequence',77)`,
		`INSERT INTO deliveries VALUES('d5','accepted','job-5',77,10)`,
		`INSERT INTO jobs VALUES(5,'job-5','route','repository-task','example/automation-target',42,'d5','run-5','completed',3,10,20,'preserved',10,20)`,
		`PRAGMA user_version=16`,
	} {
		if _, err := db.Exec(q); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	db.Close()
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	var status, run, last string
	var attempts int
	if err = s.db.QueryRow(`SELECT status,broker_run_id,last_error,attempts FROM jobs WHERE id=5`).Scan(&status, &run, &last, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != StateReportPending || run != "run-5" || last != "preserved" || attempts != 3 {
		t.Fatalf("job not preserved: %q %q %q %d", status, run, last, attempts)
	}
	work, ok, err := s.ClaimDue(ctx, time.UnixMilli(20))
	if err != nil || !ok || work.Kind != WorkStatus || work.Job.ID != 5 {
		t.Fatalf("reconciliation work=%+v ok=%v err=%v", work, ok, err)
	}
	for _, table := range []string{"terminal_results", "notification_outbox", "work_items", "executor_attempts"} {
		var n int
		if err = s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("missing %s: %d %v", table, n, err)
		}
	}
	if _, err = s.db.Exec(`INSERT INTO terminal_results(job_id,version,run_id,profile,repository,branch,status,outcome,result_json,final_summary,failure_stage,failure_reason,recorded_at) VALUES(999,'v','r','p','repo','','x','failed','','','','',0)`); err == nil {
		t.Fatal("foreign key integrity was not enforced")
	}
}
