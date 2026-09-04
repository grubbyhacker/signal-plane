package dispatcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBrokerCIObservationAndRepairLaunchContract(t *testing.T) {
	head := strings.Repeat("a", 40)
	mainBroker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/repos/example/automation-target/pulls/9/ci-observation" || r.URL.Query().Get("head_sha") != head {
			t.Fatalf("unexpected observation URL %s", r.URL.String())
		}
		if r.Header.Get("X-Agent-ID") != terminalReporterAgentID || r.Header.Get("X-Agent-Secret") != "reporter-secret" {
			t.Fatal("missing broker-scoped observation credentials")
		}
		_, _ = w.Write([]byte(`{"requested_head_sha":"` + head + `","pull":{"number":9,"head_sha":"` + head + `"},"commit_status":{"statuses":[{"context":"legacy","state":"failure","description":"legacy failed"}]},"check_runs":{"check_runs":[{"name":"unit","conclusion":"failure","output":{"title":"tests","summary":"assertion failed"}}]},"workflow_jobs":[{"name":"integration","conclusion":"timed_out"}],"aggregate_state":"code_failure"}`))
	}))
	defer mainBroker.Close()
	sandbox := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/launch-profiles/terra-medium-v1/launch" || r.Header.Get("Idempotency-Key") != "ci-repair:v1:7:terra-medium-v1:"+head+":1" || r.Header.Get("Authorization") != "Bearer sandbox-token" {
			t.Fatalf("unexpected launch request %s key=%q", r.URL.Path, r.Header.Get("Idempotency-Key"))
		}
		var body struct {
			MaxRuntimeSeconds int `json:"max_runtime_seconds"`
			Parameters        struct {
				IssueNumber      int64  `json:"issue_number"`
				SourceDeliveryID string `json:"source_delivery_id"`
				RepairPRNumber   int64  `json:"repair_pr_number"`
				ExpectedHeadSHA  string `json:"expected_head_sha"`
			} `json:"parameters"`
		}
		if json.NewDecoder(r.Body).Decode(&body) != nil || body.MaxRuntimeSeconds != 600 || body.Parameters.IssueNumber != 41 || body.Parameters.RepairPRNumber != 9 || body.Parameters.ExpectedHeadSHA != head || !strings.HasPrefix(body.Parameters.SourceDeliveryID, "ci-repair-v1-") {
			t.Fatalf("unexpected repair body %+v", body)
		}
		_, _ = w.Write([]byte(`{"run_id":"repair-run"}`))
	}))
	defer sandbox.Close()
	b := &Broker{URL: sandbox.URL, Token: "sandbox-token", ReporterURL: mainBroker.URL, ReporterToken: "reporter-secret", Client: sandbox.Client()}
	now := time.Unix(50_000, 0).UTC()
	task := CIRepairTask{JobID: 7, Repository: "example/automation-target", IssueNumber: 41, PullNumber: 9, DeadlineAt: now.Add(10 * time.Minute)}
	observation, err := b.ObserveCI(context.Background(), task, head)
	if err != nil || observation.HeadSHA != head || observation.State != CIStateCodeFailure || len(observation.FailedChecks) != 3 {
		t.Fatalf("observation=%+v err=%v", observation, err)
	}
	attempt := CIRepairAttempt{AttemptKey: "ci-repair:v1:7:terra-medium-v1:" + head + ":1", FailedHeadSHA: head, AgentModel: "terra-medium-v1"}
	launch, err := b.LaunchRepair(context.Background(), task, attempt, now)
	if err != nil || launch.RunID != "repair-run" {
		t.Fatalf("launch=%+v err=%v", launch, err)
	}
}

type lifecycleRepairBroker struct {
	head    string
	task    CIRepairTask
	attempt CIRepairAttempt
}

func (*lifecycleRepairBroker) Launch(context.Context, Job) (LaunchResult, error) {
	return LaunchResult{}, nil
}
func (b *lifecycleRepairBroker) ObserveCI(_ context.Context, task CIRepairTask, head string) (CIObservation, error) {
	b.task = task
	return observation(head, CIStateCodeFailure), nil
}
func (b *lifecycleRepairBroker) LaunchRepair(_ context.Context, task CIRepairTask, attempt CIRepairAttempt, _ time.Time) (LaunchResult, error) {
	b.task, b.attempt = task, attempt
	return LaunchResult{RunID: "repair-run-1"}, nil
}
func (*lifecycleRepairBroker) Status(context.Context, string) (RunStatus, error) {
	return RunStatus{RunID: "repair-run-1", Status: "completed"}, nil
}
func (b *lifecycleRepairBroker) TerminalResult(context.Context, string) (TerminalResult, error) {
	tree := strings.Repeat("d", 40)
	delivered := strings.Repeat("b", 40)
	return TerminalResult{Version: terminalResultVersion, RunID: "repair-run-1", Profile: b.attempt.AgentModel, Repo: b.task.Repository, Branch: b.task.Branch, Status: "completed", Outcome: "ready_for_review", ModelExecutionStarted: true, Result: map[string]any{
		"version": workerResultVersion, "outcome": "ready_for_review", "run_id": "repair-run-1", "repository": b.task.Repository, "base_branch": "main", "branch": b.task.Branch,
		"expected_old_head_sha": b.head, "candidate_head_sha": delivered, "delivered_head_sha": delivered, "validated_tree_sha": tree, "delivered_tree_sha": tree,
		"verification": map[string]any{"status": "passed"}, "pull_request": map[string]any{"number": b.task.PullNumber, "html_url": "https://example.test/pull/9", "url": "https://api.example.test/pulls/9"},
	}}, nil
}

func TestRunOneDrivesRepairThroughExactDeliveredTree(t *testing.T) {
	now := time.Unix(60_000, 0).UTC()
	store, _ := setupCIRepairTask(t, now.Add(time.Hour))
	defer store.Close()
	if err := store.ConfigureCIRepair(CIRepairPolicy{Deadline: time.Hour, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	broker := &lifecycleRepairBroker{head: strings.Repeat("a", 40)}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics()
	for step := 0; step < 3; step++ {
		worked, err := RunOne(context.Background(), logger, metrics, store, broker, now.Add(time.Duration(step)*StatusPollInterval))
		if err != nil || !worked {
			t.Fatalf("step=%d worked=%v err=%v", step, worked, err)
		}
	}
	var state, head string
	var charged int
	if err := store.db.QueryRow(`SELECT state,current_head_sha,repair_attempts FROM repository_ci_tasks`).Scan(&state, &head, &charged); err != nil {
		t.Fatal(err)
	}
	if state != CIWaiting || head != strings.Repeat("b", 40) || charged != 1 {
		t.Fatalf("state=%s head=%s charged=%d", state, head, charged)
	}
}
