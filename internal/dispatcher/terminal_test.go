package dispatcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func terminalTestJob(t *testing.T, store *Store, now time.Time) Job {
	t.Helper()
	job := Job{
		ID: 5, SemanticKey: "repository-task:v1:route:owner/repo:issue:25",
		RouteID: "route", Profile: "repository-task", Repository: "owner/repo",
		IssueNumber: 25, DeliveryID: "delivery-5", BrokerRunID: "run-5", Status: StateLaunched,
	}
	_, err := store.db.Exec(`INSERT INTO jobs(id,semantic_key,route_id,launch_profile,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		job.ID, job.SemanticKey, job.RouteID, job.Profile, job.Repository, job.IssueNumber, job.DeliveryID, job.BrokerRunID, job.Status, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func terminalTestResult(outcome string) TerminalResult {
	result := TerminalResult{
		Version: terminalResultVersion, RunID: "run-5", Profile: "repository-task",
		Repo: "owner/repo", Branch: "agent/run-5", Status: "completed", Outcome: outcome,
		FinalizeReason: "worker_exit", TerminalSource: "exited",
		IdempotencyKeyDigest: "0123456789abcdef", RequestFingerprint: "fedcba9876543210",
		LaunchConfigVersion: "route-config-v1", FinalSummary: "Complete final work product.\n",
	}
	worker := map[string]any{
		"version": workerResultVersion, "outcome": outcome, "detail": "task completed",
		"stage": "completed", "run_id": "run-5", "repository": "owner/repo",
		"base_branch": "main", "branch": "agent/run-5",
		"verification": map[string]any{"status": "passed"}, "verify_task": "verify",
	}
	if outcome == "ready_for_review" {
		worker["pull_request"] = map[string]any{
			"number": float64(42), "html_url": "https://github.example/owner/repo/pull/42",
			"url": "https://api.github.example/repos/owner/repo/pulls/42",
		}
	}
	result.Result = worker
	return result
}

func TestRenderTerminalCommentsForEveryOutcome(t *testing.T) {
	for _, outcome := range []string{"no_change_required", "ready_for_review", "failed", "timed_out", "stopped", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			now := time.Unix(90_000, 0)
			job := terminalTestJob(t, store, now)
			result := terminalTestResult(outcome)
			switch outcome {
			case "failed":
				result.Status = "failed"
				result.Result["verification"] = map[string]any{"status": "failed"}
				result.Result["stage"] = "repository verification task"
				result.Result["detail"] = "worker failed during repository verification task"
			case "timed_out", "stopped", "cancelled":
				result.Status = outcome
				result.Result = nil
				result.FinalSummary = ""
				result.FailureStage = "terminal_result"
				result.FailureReason = "final-summary.md is absent"
			}
			if err := ValidateTerminalResult(job, result); err != nil {
				t.Fatal(err)
			}
			first, err := RenderTerminalComment(job, result)
			if err != nil {
				t.Fatal(err)
			}
			second, err := RenderTerminalComment(job, result)
			if err != nil || first != second {
				t.Fatalf("comment is not deterministic: err=%v", err)
			}
			for _, want := range []string{"Outcome: `" + outcome + "`", "Semantic job: `5`", "Broker run: `run-5`", "Launch config: `route-config-v1`"} {
				if !strings.Contains(first, want) {
					t.Fatalf("comment missing %q:\n%s", want, first)
				}
			}
			for _, want := range []string{"Result version: `" + terminalResultVersion + "`", "Launch idempotency digest: `0123456789abcdef`", "Request fingerprint: `fedcba9876543210`", "Branch: `agent/run-5`"} {
				if !strings.Contains(first, want) {
					t.Fatalf("comment missing correlation %q:\n%s", want, first)
				}
			}
			if outcome == "failed" && (!strings.Contains(first, "Failure stage: `repository verification task`") || !strings.Contains(first, "Diagnostic: worker failed during repository verification task")) {
				t.Fatalf("worker failure is missing stage or diagnostic:\n%s", first)
			}
			if result.FailureStage != "" && (!strings.Contains(first, "Failure stage: `terminal_result`") || !strings.Contains(first, "Diagnostic: final-summary.md is absent")) {
				t.Fatalf("fallback failure is missing stage or diagnostic:\n%s", first)
			}
			if result.FinalSummary != "" && !strings.Contains(first, result.FinalSummary) {
				t.Fatal("complete final work product is missing")
			}
			if result.FinalSummary == "" && !strings.Contains(first, "harness-generated terminal result") {
				t.Fatal("missing-summary fallback is not useful")
			}
			if err := store.QueueTerminalResult(context.Background(), job, result, now); err != nil {
				t.Fatal(err)
			}
			var durableBody, durableKey, durableStatus string
			if err := store.db.QueryRow(`SELECT body,idempotency_key,status FROM notification_outbox WHERE job_id=5`).Scan(&durableBody, &durableKey, &durableStatus); err != nil {
				t.Fatal(err)
			}
			if durableBody != first || durableKey != reportKey(job, result.Version) || durableStatus != "pending" {
				t.Fatalf("durable outbox body/key/status mismatch")
			}
			if outcome == "no_change_required" {
				var finalizeReason, terminalSource, idempotencyDigest, fingerprint, launchConfig, resultJSON, finalSummary string
				if err := store.db.QueryRow(`SELECT finalize_reason,terminal_source,idempotency_key_digest,request_fingerprint,launch_config_version,result_json,final_summary FROM terminal_results WHERE job_id=5`).
					Scan(&finalizeReason, &terminalSource, &idempotencyDigest, &fingerprint, &launchConfig, &resultJSON, &finalSummary); err != nil {
					t.Fatal(err)
				}
				expectedResult, err := json.Marshal(result.Result)
				if err != nil {
					t.Fatal(err)
				}
				if finalizeReason != result.FinalizeReason || terminalSource != result.TerminalSource ||
					idempotencyDigest != result.IdempotencyKeyDigest || fingerprint != result.RequestFingerprint ||
					launchConfig != result.LaunchConfigVersion || resultJSON != string(expectedResult) ||
					finalSummary != result.FinalSummary {
					t.Fatal("durable terminal result did not preserve the complete bounded projection")
				}
			}
		})
	}
}

func TestOutboxSurvivesInsertionRestartAndAcceptedCommentReplay(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100_000, 0)
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	job := terminalTestJob(t, store, now)
	result := terminalTestResult("no_change_required")
	if err := store.QueueTerminalResult(ctx, job, result, now); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	report, ok, err := store.ClaimReportDue(ctx, now)
	if err != nil || !ok {
		t.Fatalf("claim after restart ok=%v err=%v", ok, err)
	}
	semanticComments := map[string]CommentResult{}
	postCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		postCalls++
		if request.Header.Get("X-Agent-ID") != terminalReporterAgentID {
			t.Errorf("X-Agent-ID = %q", request.Header.Get("X-Agent-ID"))
		}
		if request.Header.Get("X-Agent-Secret") != "reporter-secret" {
			t.Errorf("X-Agent-Secret was not the reporter credential")
		}
		if request.Header.Get("Authorization") != "" {
			t.Errorf("Authorization header must not carry the reporter credential")
		}
		key := request.Header.Get("Idempotency-Key")
		comment, exists := semanticComments[key]
		if !exists {
			comment = CommentResult{ID: 9001, URL: "https://github.example/owner/repo/issues/25#issuecomment-9001"}
			semanticComments[key] = comment
		}
		_, _ = w.Write([]byte(`{"id":9001,"html_url":"https://github.example/owner/repo/issues/25#issuecomment-9001"}`))
	}))
	defer server.Close()
	broker := &Broker{ReporterURL: server.URL, ReporterToken: "reporter-secret", Client: server.Client()}
	accepted, err := broker.Comment(ctx, report.Job, report.Body, report.IdempotencyKey)
	if err != nil || accepted.ID != 9001 {
		t.Fatalf("first accepted comment=%+v err=%v", accepted, err)
	}
	// Simulate process death after GitHub accepted the comment but before the
	// local acknowledgement transaction.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	report, ok, err = store.ClaimReportDue(ctx, now)
	if err != nil || !ok {
		t.Fatalf("reclaim after acceptance ok=%v err=%v", ok, err)
	}
	replayed, err := broker.Comment(ctx, report.Job, report.Body, report.IdempotencyKey)
	if err != nil || replayed != accepted {
		t.Fatalf("replayed comment=%+v accepted=%+v err=%v", replayed, accepted, err)
	}
	if postCalls != 2 || len(semanticComments) != 1 {
		t.Fatalf("posts=%d semantic comments=%d", postCalls, len(semanticComments))
	}
	if err := store.MarkReportDelivered(ctx, report, replayed.ID, replayed.URL, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var outboxStatus, jobStatus string
	if err := store.db.QueryRow(`SELECT status FROM notification_outbox WHERE job_id=5`).Scan(&outboxStatus); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status FROM jobs WHERE id=5`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != "delivered" || jobStatus != StateCompleted {
		t.Fatalf("outbox=%s job=%s", outboxStatus, jobStatus)
	}
}

func TestQueueTerminalResultRejectsConflictAndOversizeWithoutTruncation(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(110_000, 0)
	store, err := OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	job := terminalTestJob(t, store, now)
	result := terminalTestResult("no_change_required")
	if err := store.QueueTerminalResult(ctx, job, result, now); err != nil {
		t.Fatal(err)
	}
	conflict := result
	conflict.FinalSummary = "different immutable output"
	if err := store.QueueTerminalResult(ctx, job, conflict, now); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting replay error=%v", err)
	}
	oversize := terminalTestResult("no_change_required")
	oversize.FinalSummary = strings.Repeat("x", maxTerminalTextBytes+1)
	if err := ValidateTerminalResult(job, oversize); err == nil {
		t.Fatal("oversized output was silently accepted")
	}
	var stored string
	if err := store.db.QueryRow(`SELECT final_summary FROM terminal_results WHERE job_id=5`).Scan(&stored); err != nil || stored != result.FinalSummary {
		t.Fatalf("durable output changed or truncated: %q err=%v", stored, err)
	}
}

func TestTemporaryReporterFailureRetriesDurably(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(120_000, 0)
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	job := terminalTestJob(t, store, now)
	if err := store.QueueTerminalResult(ctx, job, terminalTestResult("no_change_required"), now); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, `{"code":"temporary"}`, http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"id":77,"html_url":"https://github.example/owner/repo/issues/25#issuecomment-77"}`))
	}))
	defer server.Close()
	broker := &Broker{ReporterURL: server.URL, Client: server.Client()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now); err != nil || !worked {
		t.Fatalf("temporary failure worked=%v err=%v", worked, err)
	}
	var outboxStatus, jobStatus string
	var due int64
	if err := store.db.QueryRow(`SELECT status,due_at FROM notification_outbox WHERE job_id=5`).Scan(&outboxStatus, &due); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status FROM jobs WHERE id=5`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != "retry" || jobStatus != StateReportRetry || due <= now.UnixMilli() {
		t.Fatalf("outbox=%s job=%s due=%d", outboxStatus, jobStatus, due)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, time.UnixMilli(due)); err != nil || !worked {
		t.Fatalf("durable retry worked=%v err=%v", worked, err)
	}
	if calls != 2 {
		t.Fatalf("reporter calls=%d", calls)
	}
	if err := store.db.QueryRow(`SELECT status FROM notification_outbox WHERE job_id=5`).Scan(&outboxStatus); err != nil || outboxStatus != "delivered" {
		t.Fatalf("outbox=%s err=%v", outboxStatus, err)
	}
}

func TestPermanentReporterFailureIsDurablyBlocked(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(125_000, 0)
	store, err := OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	job := terminalTestJob(t, store, now)
	if err := store.QueueTerminalResult(ctx, job, terminalTestResult("no_change_required"), now); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"code":"policy_denied","message":"repository is not allowed"}`, http.StatusForbidden)
	}))
	defer server.Close()
	broker := &Broker{ReporterURL: server.URL, Client: server.Client()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now); err != nil || !worked {
		t.Fatalf("permanent failure worked=%v err=%v", worked, err)
	}
	var outboxStatus, jobStatus, outboxError, jobError string
	var attempts int
	if err := store.db.QueryRow(`SELECT status,attempts,last_error FROM notification_outbox WHERE job_id=5`).Scan(&outboxStatus, &attempts, &outboxError); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT status,last_error FROM jobs WHERE id=5`).Scan(&jobStatus, &jobError); err != nil {
		t.Fatal(err)
	}
	if outboxStatus != "blocked" || jobStatus != StateReportBlocked || attempts != 1 {
		t.Fatalf("outbox=%s job=%s attempts=%d", outboxStatus, jobStatus, attempts)
	}
	if outboxError == "" || jobError != outboxError {
		t.Fatalf("safe errors outbox=%q job=%q", outboxError, jobError)
	}
	if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now.Add(time.Hour)); err != nil || worked {
		t.Fatalf("blocked report was retried: worked=%v err=%v", worked, err)
	}
}

func TestTemporaryTerminalProjectionFailureRetriesWithoutFalseCompletion(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(130_000, 0)
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = terminalTestJob(t, store, now)
	projectionCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/v1/runs/run-5":
			_, _ = w.Write([]byte(`{"run_id":"run-5","status":"completed"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/v1/runs/run-5/terminal-result":
			projectionCalls++
			if projectionCalls == 1 {
				http.Error(w, `{"code":"temporary"}`, http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(terminalTestResult("no_change_required"))
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()
	broker := &Broker{URL: server.URL, ReporterURL: server.URL, Client: server.Client()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now); err != nil || !worked {
		t.Fatalf("projection retry worked=%v err=%v", worked, err)
	}
	var status string
	var due int64
	if err := store.db.QueryRow(`SELECT status,due_at FROM jobs WHERE id=5`).Scan(&status, &due); err != nil {
		t.Fatal(err)
	}
	if status != StateReportPending || due != now.Add(StatusPollInterval).UnixMilli() {
		t.Fatalf("job status=%s due=%d", status, due)
	}
	var results, outbox int
	_ = store.db.QueryRow(`SELECT count(*) FROM terminal_results`).Scan(&results)
	_ = store.db.QueryRow(`SELECT count(*) FROM notification_outbox`).Scan(&outbox)
	if results != 0 || outbox != 0 {
		t.Fatalf("temporary projection persisted results=%d outbox=%d", results, outbox)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, time.UnixMilli(due)); err != nil || !worked {
		t.Fatalf("projection after restart worked=%v err=%v", worked, err)
	}
	if projectionCalls != 2 {
		t.Fatalf("projection calls=%d", projectionCalls)
	}
	if err := store.db.QueryRow(`SELECT status FROM jobs WHERE id=5`).Scan(&status); err != nil || status != StateReportPending {
		t.Fatalf("queued status=%s err=%v", status, err)
	}
}
