package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
)

func TestDisabledStandbyPreparesStoreWithoutBrokerOrNATS(t *testing.T) {
	database := filepath.Join(t.TempDir(), "dispatcher.db")
	t.Setenv("DISABLED_STANDBY_BROKER_TOKEN", "")
	listenerCalled := false
	listenerStopped := errors.New("listener stopped")
	err := runDisabledStandby(config.Config{
		NATS: config.NATSConfig{URL: "nats://127.0.0.1:1"},
		Dispatcher: config.DispatcherConfig{
			Addr:           "127.0.0.1:0",
			DatabasePath:   database,
			BrokerTokenEnv: "DISABLED_STANDBY_BROKER_TOKEN",
		},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), dispatcher.NewMetrics(), func(addr string, handler http.Handler) error {
		listenerCalled = true
		if addr != "127.0.0.1:0" {
			t.Fatalf("listener address = %q", addr)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("standby readiness status = %d", response.Code)
		}
		return listenerStopped
	})
	if !errors.Is(err, listenerStopped) {
		t.Fatalf("run disabled standby: %v", err)
	}
	if !listenerCalled {
		t.Fatal("standby listener was not called")
	}
	store, err := dispatcher.OpenStoreReadOnly(database)
	if err != nil {
		t.Fatalf("open prepared store: %v", err)
	}
	defer store.Close()
	schema, checkpoint, start, err := store.RecoveryMetadata(context.Background())
	if err != nil || schema != dispatcher.SchemaVersion || checkpoint != 0 || start != 1 {
		t.Fatalf("prepared metadata schema=%d checkpoint=%d start=%d err=%v", schema, checkpoint, start, err)
	}
}

func TestDisabledStandbyFailsClosedWhenStoreCannotInitialize(t *testing.T) {
	listenerCalled := false
	err := runDisabledStandby(config.Config{Dispatcher: config.DispatcherConfig{DatabasePath: t.TempDir()}}, slog.New(slog.NewTextHandler(io.Discard, nil)), dispatcher.NewMetrics(), func(string, http.Handler) error {
		listenerCalled = true
		return nil
	})
	if err == nil {
		t.Fatal("expected store initialization error")
	}
	if listenerCalled {
		t.Fatal("standby listener started after store initialization failed")
	}
}

func TestRecoveryMetadataCommandEmptyDatabase(t *testing.T) {
	var output bytes.Buffer
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, err := dispatcher.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runRecoveryMetadata([]string{"--database", path}, &output); err != nil {
		t.Fatal(err)
	}
	var got struct {
		SchemaVersion int    `json:"schema_version"`
		Checkpoint    uint64 `json:"last_persisted_jetstream_sequence"`
		StartSequence uint64 `json:"recovery_start_sequence"`
	}
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != dispatcher.SchemaVersion || got.Checkpoint != 0 || got.StartSequence != 1 {
		t.Fatalf("metadata=%+v", got)
	}
}

func TestRecoveryMetadataCommandRequiresDatabase(t *testing.T) {
	if err := runRecoveryMetadata(nil, &bytes.Buffer{}); err == nil {
		t.Fatal("expected usage error")
	}
}

func TestReportReconciliationCommandPlansThenRequeuesExactBlockedReport(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "dispatcher.db")
	store, err := dispatcher.OpenStore(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", database)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Unix(140_000, 0)
	for _, statement := range []string{
		`INSERT INTO jobs(id,semantic_key,route_id,launch_profile,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,last_error,created_at,updated_at) VALUES(5,'repository-task:v1:route:owner/repo:issue:25','route','repository-task','owner/repo',25,'delivery-5','run-5','report_blocked',1,'agent authentication failed',1,1)`,
		`INSERT INTO terminal_results(job_id,version,run_id,profile,repository,status,outcome,final_summary,recorded_at) VALUES(5,'repository-task-terminal-result/v1','run-5','repository-task','owner/repo','completed','no_change_required','work product',1)`,
		`INSERT INTO notification_outbox(id,job_id,terminal_result_version,body,idempotency_key,status,attempts,due_at,last_error,created_at,updated_at) VALUES(1,5,'repository-task-terminal-result/v1','body','stable-key','blocked',1,1,'agent authentication failed',1,1)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	configPath := filepath.Join(directory, "dispatcher.yaml")
	configText := "nats:\n  url: nats://invalid.example:4222\n  stream: SIGNALS\n  subjects: [signals.>]\n" +
		"dispatcher:\n  enabled: true\n  subject: signals.github.>\n  durable: dispatcher-v1\n  recovery_start_sequence: 1\n  database_path: " + database + "\n" +
		"  broker_url: http://broker.invalid\n  broker_token_env: TEST_BROKER_TOKEN\n  workers: 1\n" +
		"  repository_task_routes:\n  - id: route\n    repository: owner/repo\n    event: issues\n    action: labeled\n    label: automation:requested\n    profile: repository-task\n" +
		"routes:\n  - id: local\n    path: /local\n    source: manual\n    publish_subject: signals.local\n"
	if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{"--config", configPath, "--job-id", "5", "--broker-run-id", "run-5", "--idempotency-key", "stable-key"}
	var output bytes.Buffer
	if err := runReportReconciliation(args, &output, now); err != nil {
		t.Fatal(err)
	}
	var report dispatcher.ReportReconciliation
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Mode != "plan" || report.Status != "validated" || report.PriorAttempts != 1 || report.PriorError != "agent authentication failed" {
		t.Fatalf("plan=%+v", report)
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM notification_outbox WHERE id=1`).Scan(&status); err != nil || status != "blocked" {
		t.Fatalf("plan changed status=%q err=%v", status, err)
	}
	output.Reset()
	if err := runReportReconciliation(append(args, "--execute"), &output, now); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Mode != "execute" || report.Status != "requeued" {
		t.Fatalf("execute=%+v", report)
	}
	var attempts int
	var key, priorError string
	if err := db.QueryRow(`SELECT status,attempts,idempotency_key,last_error FROM notification_outbox WHERE id=1`).Scan(&status, &attempts, &key, &priorError); err != nil {
		t.Fatal(err)
	}
	if status != "retry" || attempts != 1 || key != "stable-key" || priorError != "agent authentication failed" {
		t.Fatalf("status=%q attempts=%d key=%q error=%q", status, attempts, key, priorError)
	}
}

func TestRecoveryCommandDefaultsToReadOnlyPlan(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "dispatcher.db")
	store, err := dispatcher.OpenStore(database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "dispatcher.yaml")
	config := "nats:\n  url: nats://invalid.example:4222\n  stream: SIGNALS\n  subjects: [signals.>]\n" +
		"dispatcher:\n  enabled: true\n  subject: signals.github.>\n  durable: restored-v1\n  recovery_start_sequence: 1\n  database_path: " + database + "\n" +
		"  broker_url: http://broker.invalid\n  broker_token_env: TEST_RECOVERY_TOKEN\n  workers: 1\n" +
		"  repository_task_routes:\n  - id: fixture\n    repository: example/automation-target\n    event: issues\n    action: labeled\n    label: automation:requested\n    profile: repository-task\n" +
		"routes:\n  - id: local\n    path: /local\n    source: manual\n    publish_subject: signals.local\n"
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(database)
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := runRecovery([]string{"--config", configPath, "--manifest-last-sequence", "0", "--recovery-id", "dry-run-proof"}, &output); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Mode   string `json:"mode"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Mode != "dry-run" || report.Status != "validated" {
		t.Fatalf("report=%+v", report)
	}
	after, err := os.Stat(database)
	if err != nil {
		t.Fatal(err)
	}
	if !after.ModTime().Equal(before.ModTime()) || after.Size() != before.Size() {
		t.Fatal("dry-run modified restored SQLite")
	}
}
