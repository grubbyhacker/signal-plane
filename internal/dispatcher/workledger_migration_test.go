package dispatcher

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

func TestMigratedReportPendingDefersWithoutReporterThenReconciles(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(50_000, 0)
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, semantic_key TEXT, stream_sequence INTEGER NOT NULL DEFAULT 0, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, first_launch_attempt_at INTEGER, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`INSERT INTO jobs(id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,created_at,updated_at) VALUES(5,'migrated-job-5','example/automation-target',5,'delivery-5','run-5','completed',1,1,1)`,
		`PRAGMA user_version=16`,
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

	var status, reason string
	var due int64
	if err := store.db.QueryRow(`SELECT status,due_at,last_error FROM jobs WHERE id=5`).Scan(&status, &due, &reason); err != nil || status != StateReportPending || due != 1 {
		t.Fatalf("migrated job status=%q due=%d reason=%q err=%v", status, due, reason, err)
	}

	serverCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalls++
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runs/run-5":
			_, _ = w.Write([]byte(`{"run_id":"run-5","status":"completed"}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/runs/run-5/terminal-result":
			_, _ = w.Write([]byte(`{"version":"repository-task-terminal-result/v1","run_id":"run-5","profile":"","repo":"example/automation-target","branch":"agent/run-5","status":"completed","outcome":"ready_for_review","finalize_reason":"worker_exit","terminal_source":"exited","idempotency_key_digest":"idem-digest","request_fingerprint":"request-fingerprint","launch_config_version":"config-version","result":{"version":"repository-task-worker-result/v1","outcome":"ready_for_review","detail":"pull request created","stage":"completed","run_id":"run-5","repository":"example/automation-target","base_branch":"main","branch":"agent/run-5","verification":{"status":"passed"},"verify_task":"verify","pull_request":{"number":42,"html_url":"https://example.test/pull/42","url":"https://api.example.test/pulls/42"}},"final_summary":"done"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/repos/example/automation-target/issues/5/comments":
			_, _ = w.Write([]byte(`{"id":55,"html_url":"https://example.test/comments/55"}`))
		default:
			t.Fatalf("unexpected broker request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics()
	withoutReporter := &Broker{URL: server.URL, Client: server.Client()}
	if worked, err := RunOne(ctx, logger, metrics, store, withoutReporter, now); err != nil || !worked {
		t.Fatalf("defer worked=%v err=%v", worked, err)
	}
	if serverCalls != 0 {
		t.Fatalf("unconfigured reporter made %d broker calls", serverCalls)
	}
	if err := store.db.QueryRow(`SELECT status,due_at,last_error FROM jobs WHERE id=5`).Scan(&status, &due, &reason); err != nil || status != StateReportPending || due != now.Add(ReporterUnavailableDelay).UnixMilli() || reason != "terminal reporting deferred: reporter broker is not configured" {
		t.Fatalf("deferred job status=%q due=%d reason=%q err=%v", status, due, reason, err)
	}
	if worked, err := RunOne(ctx, logger, metrics, store, withoutReporter, now.Add(time.Second)); err != nil || worked {
		t.Fatalf("deferred job worked=%v err=%v", worked, err)
	}
	if serverCalls != 0 {
		t.Fatalf("deferred reporter made %d broker calls", serverCalls)
	}
	var jobs, terminalResults, outbox int
	if err := store.db.QueryRow(`SELECT count(*) FROM jobs`).Scan(&jobs); err != nil || jobs != 1 {
		t.Fatalf("jobs=%d err=%v", jobs, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM terminal_results`).Scan(&terminalResults); err != nil || terminalResults != 0 {
		t.Fatalf("terminal results=%d err=%v", terminalResults, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM notification_outbox`).Scan(&outbox); err != nil || outbox != 0 {
		t.Fatalf("outbox=%d err=%v", outbox, err)
	}

	withReporter := &Broker{URL: server.URL, ReporterURL: server.URL, Client: server.Client()}
	dueAt := now.Add(ReporterUnavailableDelay)
	if worked, err := RunOne(ctx, logger, metrics, store, withReporter, dueAt); err != nil || !worked {
		t.Fatalf("reconcile worked=%v err=%v", worked, err)
	}
	if worked, err := RunOne(ctx, logger, metrics, store, withReporter, dueAt); err != nil || !worked {
		t.Fatalf("report worked=%v err=%v", worked, err)
	}
	if err := store.db.QueryRow(`SELECT status FROM jobs WHERE id=5`).Scan(&status); err != nil || status != StateCompleted {
		t.Fatalf("reported job status=%q err=%v", status, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM terminal_results`).Scan(&terminalResults); err != nil || terminalResults != 1 {
		t.Fatalf("terminal results=%d err=%v", terminalResults, err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM notification_outbox`).Scan(&outbox); err != nil || outbox != 1 {
		t.Fatalf("outbox=%d err=%v", outbox, err)
	}
	if serverCalls != 3 {
		t.Fatalf("broker calls=%d want status, terminal result, and comment", serverCalls)
	}
}

func TestOpenStoreMigratesDeployedSchema17TerminalLedgerTo19(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	deployed, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE TABLE jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, route_id TEXT NOT NULL DEFAULT '', launch_profile TEXT NOT NULL DEFAULT '', repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, first_launch_attempt_at INTEGER, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE terminal_results (job_id INTEGER PRIMARY KEY REFERENCES jobs(id), version TEXT NOT NULL, run_id TEXT NOT NULL, profile TEXT NOT NULL, repository TEXT NOT NULL, branch TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, outcome TEXT NOT NULL, final_summary TEXT NOT NULL, failure_stage TEXT NOT NULL DEFAULT '', failure_reason TEXT NOT NULL DEFAULT '', recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE notification_outbox (id INTEGER PRIMARY KEY, job_id INTEGER NOT NULL UNIQUE REFERENCES jobs(id), terminal_result_version TEXT NOT NULL, body TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE, status TEXT NOT NULL CHECK(status IN ('pending','retry','delivered','blocked')), attempts INTEGER NOT NULL DEFAULT 0, due_at INTEGER NOT NULL, comment_id INTEGER, comment_url TEXT NOT NULL DEFAULT '', delivered_at INTEGER, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`INSERT INTO jobs(id,semantic_key,route_id,launch_profile,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,created_at,updated_at) VALUES(5,'semantic-5','route','repository-task','owner/repo',25,'delivery-5','run-5','report_pending',1,1,1)`,
		`INSERT INTO jobs(id,semantic_key,route_id,launch_profile,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,last_error,created_at,updated_at) VALUES(6,'semantic-6','route','repository-task','owner/repo',26,'delivery-6','run-6','report_blocked',1,'orphaned projection failure',1,1)`,
		`INSERT INTO terminal_results(job_id,version,run_id,profile,repository,branch,status,outcome,final_summary,recorded_at) VALUES(5,'repository-task-terminal-result/v1','run-5','repository-task','owner/repo','agent/run-5','completed','no_change_required','complete output',1)`,
		`INSERT INTO notification_outbox(job_id,terminal_result_version,body,idempotency_key,status,due_at,created_at,updated_at) VALUES(5,'repository-task-terminal-result/v1','body','key','pending',1,1,1)`,
		`PRAGMA user_version=17`,
	} {
		if _, err := deployed.Exec(statement); err != nil {
			deployed.Close()
			t.Fatal(err)
		}
	}
	if err := deployed.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err := store.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != SchemaVersion {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
	var finalizeReason, terminalSource, idempotencyDigest, fingerprint, launchConfig, resultJSON, summary string
	if err := store.db.QueryRow(`SELECT finalize_reason,terminal_source,idempotency_key_digest,request_fingerprint,launch_config_version,result_json,final_summary FROM terminal_results WHERE job_id=5`).
		Scan(&finalizeReason, &terminalSource, &idempotencyDigest, &fingerprint, &launchConfig, &resultJSON, &summary); err != nil {
		t.Fatal(err)
	}
	if finalizeReason != "" || terminalSource != "" || idempotencyDigest != "" || fingerprint != "" || launchConfig != "" || resultJSON != "" || summary != "complete output" {
		t.Fatalf("migration changed deployed row: %q %q %q %q %q %q %q", finalizeReason, terminalSource, idempotencyDigest, fingerprint, launchConfig, resultJSON, summary)
	}
	var outboxStatus string
	if err := store.db.QueryRow(`SELECT status FROM notification_outbox WHERE job_id=5`).Scan(&outboxStatus); err != nil || outboxStatus != "pending" {
		t.Fatalf("outbox status=%q err=%v", outboxStatus, err)
	}
	var orphanStatus string
	var preOutboxAttempts int
	if err := store.db.QueryRow(`SELECT status,pre_outbox_attempts FROM jobs WHERE id=6`).Scan(&orphanStatus, &preOutboxAttempts); err != nil {
		t.Fatal(err)
	}
	if orphanStatus != StateReportPending || preOutboxAttempts != 0 {
		t.Fatalf("orphaned projection status=%q pre-outbox attempts=%d", orphanStatus, preOutboxAttempts)
	}
}
