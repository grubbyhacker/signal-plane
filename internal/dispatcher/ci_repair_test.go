package dispatcher

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

func setupCIRepairTask(t *testing.T, deadline time.Time) (*Store, Job) {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "ci-repair.db"))
	if err != nil {
		t.Fatal(err)
	}
	candidate, _ := Select(validSignal("issue-delivery", 41), testRepositoryTaskRoutes())
	now := deadline.Add(-time.Hour)
	if err := store.Record(context.Background(), candidate.DeliveryID, "accepted", 1, &candidate, now); err != nil {
		t.Fatal(err)
	}
	var job Job
	if err := store.db.QueryRow(`SELECT id,semantic_key,route_id,launch_profile,repository,issue_number,source_delivery_id,status FROM jobs WHERE issue_number=41`).Scan(&job.ID, &job.SemanticKey, &job.RouteID, &job.Profile, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.Status); err != nil {
		t.Fatal(err)
	}
	job.Repository = "example/automation-target"
	if err := store.BeginCIWait(context.Background(), job, 9, "agent/contributor/run", strings.Repeat("a", 40), "gpt-5.6-terra/medium", deadline, now, json.RawMessage(`{"outcome":"ready_for_review"}`)); err != nil {
		t.Fatal(err)
	}
	return store, job
}

func ciSignal(delivery, event, action, head string, pull int64, payload any) envelope.Signal {
	body, _ := json.Marshal(payload)
	objectKind, objectID := "commit", head
	if pull > 0 {
		objectKind, objectID = "pull_request", "9"
	}
	return envelope.Signal{Meta: envelope.Meta{Source: "github", SourceDeliveryID: delivery, SourceEvent: event, SourceAction: action, Namespace: "example/automation-target", ObjectKind: objectKind, ObjectID: objectID, SourceRevision: head, ReceivedAt: time.Unix(100, 0)}, Payload: body}
}

func observation(head, state string) CIObservation {
	return CIObservation{Repository: "example/automation-target", PullNumber: 9, HeadSHA: head, State: state, FailedChecks: []FailedCheck{{Name: "unit", Conclusion: "failure", Summary: "expected true"}}}
}

func TestCILifecycleTwoAttemptLimitAndRestartSafeIdentities(t *testing.T) {
	ctx := context.Background()
	start := time.Unix(10_000, 0).UTC()
	store, _ := setupCIRepairTask(t, start.Add(time.Hour))
	path := store.dbStatsPathForTest(t)
	head1, head2, head3 := strings.Repeat("a", 40), strings.Repeat("b", 40), strings.Repeat("c", 40)
	event := ciSignal("ci-1", "check_run", "completed", head1, 9, map[string]any{"id": 1, "conclusion": "failure"})
	queued, err := store.RecordCIEvent(ctx, event, start)
	if err != nil || !queued {
		t.Fatalf("queued=%v err=%v", queued, err)
	}
	if queued, err = store.RecordCIEvent(ctx, event, start); err != nil || queued {
		t.Fatalf("duplicate queued=%v err=%v", queued, err)
	}
	work, ok, err := store.ClaimCIReconciliation(ctx, start)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head1, CIStateCodeFailure), 2, start); err != nil {
		t.Fatal(err)
	}

	task, attempt1, ok, err := store.ClaimCIRepair(ctx, start)
	if err != nil || !ok || attempt1.AttemptNumber != 1 || task.RepairAttempts != 0 {
		t.Fatalf("attempt1=%+v task=%+v ok=%v err=%v", attempt1, task, ok, err)
	}
	if err := store.BindCIRepairRun(ctx, attempt1, "run-1", start); err != nil {
		t.Fatal(err)
	}
	if err := store.BindCIRepairRun(ctx, attempt1, "conflicting-run", start); err == nil {
		t.Fatal("conflicting broker run replay was accepted")
	}
	if err := store.BindCIRepairRun(ctx, attempt1, "run-1", start); err != nil {
		t.Fatal(err)
	}
	if err := store.ChargeCIRepairAttempt(ctx, attempt1.AttemptKey, start); err != nil {
		t.Fatal(err)
	}
	if err := store.ChargeCIRepairAttempt(ctx, attempt1.AttemptKey, start); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := store.ClaimCIRepair(ctx, start); err != nil || ok {
		t.Fatalf("running broker attempt was relaunched: ok=%v err=%v", ok, err)
	}
	// A concurrent winner can advance expected_old_head_sha during the broker's
	// same-attempt recovery without creating or charging a second execution.
	if err := store.CompleteCIRepairDelivery(ctx, attempt1.AttemptKey, CIRepairDelivery{ExpectedOldHeadSHA: strings.Repeat("f", 40), CandidateHeadSHA: head2, ValidatedTreeSHA: strings.Repeat("d", 40), DeliveredHeadSHA: head2, DeliveredTreeSHA: strings.Repeat("d", 40)}, start); err != nil {
		t.Fatal(err)
	}

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	queued, err = store.RecordCIEvent(ctx, ciSignal("ci-2", "check_suite", "completed", head2, 9, map[string]any{"id": 2}), start.Add(time.Minute))
	if err != nil || !queued {
		t.Fatalf("second queued=%v err=%v", queued, err)
	}
	work, ok, err = store.ClaimCIReconciliation(ctx, start.Add(time.Minute))
	if err != nil || !ok {
		t.Fatalf("second claim ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head2, CIStateCodeFailure), 2, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, attempt2, ok, err := store.ClaimCIRepair(ctx, start.Add(time.Minute))
	if err != nil || !ok || attempt2.AttemptNumber != 2 {
		t.Fatalf("attempt2=%+v ok=%v err=%v", attempt2, ok, err)
	}
	if err := store.BindCIRepairRun(ctx, attempt2, "run-2", start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.ChargeCIRepairAttempt(ctx, attempt2.AttemptKey, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteCIRepairDelivery(ctx, attempt2.AttemptKey, CIRepairDelivery{ExpectedOldHeadSHA: head2, CandidateHeadSHA: head3, ValidatedTreeSHA: strings.Repeat("e", 40), DeliveredHeadSHA: head3, DeliveredTreeSHA: strings.Repeat("e", 40)}, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, err = store.RecordCIEvent(ctx, ciSignal("ci-3", "status", "", head3, 0, map[string]any{"context": "unit", "state": "failure"}), start.Add(2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	work, ok, err = store.ClaimCIReconciliation(ctx, start.Add(2*time.Minute))
	if err != nil || !ok {
		t.Fatalf("third claim ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head3, CIStateCodeFailure), 2, start.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var state string
	var attempts, outbox int
	if err := store.db.QueryRow(`SELECT state,repair_attempts FROM repository_ci_tasks`).Scan(&state, &attempts); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM repository_ci_escalation_outbox`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if state != CIEscalationPending || attempts != 2 || outbox != 1 {
		t.Fatalf("state=%s attempts=%d outbox=%d", state, attempts, outbox)
	}
	report, ok, err := store.ClaimCIEscalation(ctx, start.Add(2*time.Minute))
	if err != nil || !ok || !strings.Contains(report.Body, "Attempt 1") || !strings.Contains(report.Body, "Attempt 2") {
		t.Fatalf("report=%+v ok=%v err=%v", report, ok, err)
	}
}

func TestCIEventDuringReconciliationLeavesDurableWake(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(15_000, 0).UTC()
	store, _ := setupCIRepairTask(t, now.Add(time.Hour))
	defer store.Close()
	head := strings.Repeat("a", 40)
	work, ok, err := store.ClaimCIReconciliation(ctx, now)
	if err != nil || !ok {
		t.Fatalf("initial claim ok=%v err=%v", ok, err)
	}
	queued, err := store.RecordCIEvent(ctx, ciSignal("completed-while-observing", "check_run", "completed", head, 9, map[string]any{"id": 5}), now.Add(time.Second))
	if err != nil || !queued {
		t.Fatalf("concurrent wake queued=%v err=%v", queued, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head, CIStatePending), 2, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	replay, ok, err := store.ClaimCIReconciliation(ctx, now.Add(time.Second))
	if err != nil || !ok || replay.OperationKey != work.OperationKey {
		t.Fatalf("durable replay=%+v ok=%v err=%v", replay, ok, err)
	}
}

func TestCIInfrastructureFailureEscalatesWithoutChargingAttempt(t *testing.T) {
	ctx := context.Background()
	deadline := time.Unix(20_000, 0).UTC()
	store, _ := setupCIRepairTask(t, deadline)
	defer store.Close()
	head := strings.Repeat("a", 40)
	if _, err := store.RecordCIEvent(ctx, ciSignal("infra", "check_run", "completed", head, 9, map[string]any{"id": 3}), deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	work, ok, err := store.ClaimCIReconciliation(ctx, deadline.Add(-time.Minute))
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head, CIStateInfrastructure), 2, deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := store.ClaimCIRepair(ctx, deadline.Add(-time.Minute)); err != nil || ok {
		t.Fatalf("infra launched repair ok=%v err=%v", ok, err)
	}
	var attempts int
	var state string
	if err := store.db.QueryRow(`SELECT repair_attempts,state FROM repository_ci_tasks`).Scan(&attempts, &state); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || state != CIEscalationPending {
		t.Fatalf("attempts=%d state=%s", attempts, state)
	}
	report, ok, err := store.ClaimCIEscalation(ctx, deadline.Add(-time.Minute))
	if err != nil || !ok {
		t.Fatalf("escalation claim ok=%v err=%v", ok, err)
	}
	if err := store.MarkCIEscalationFailure(ctx, report, false, "reporter rejected request", deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	var jobState string
	if err := store.db.QueryRow(`SELECT j.status,c.state FROM jobs j JOIN repository_ci_tasks c ON c.job_id=j.id`).Scan(&jobState, &state); err != nil {
		t.Fatal(err)
	}
	if jobState != StateReportBlocked || state != CIEscalationBlocked {
		t.Fatalf("job=%s task=%s", jobState, state)
	}
}

func TestRepairTerminalChargesOnlyWhenModelExecutionStarted(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(25_000, 0).UTC()
	head := strings.Repeat("a", 40)
	for _, test := range []struct {
		name         string
		modelStarted bool
		failureClass string
		wantState    string
		wantCharged  int
	}{
		{name: "pre-model infrastructure", failureClass: "infrastructure", wantState: CIEscalationPending},
		{name: "model failure", modelStarted: true, failureClass: "model_or_code", wantState: CIRepairReady, wantCharged: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, _ := setupCIRepairTask(t, now.Add(time.Hour))
			defer store.Close()
			if _, err := store.RecordCIEvent(ctx, ciSignal("terminal-charge", "check_run", "completed", head, 9, map[string]any{"id": 4}), now); err != nil {
				t.Fatal(err)
			}
			work, ok, err := store.ClaimCIReconciliation(ctx, now)
			if err != nil || !ok {
				t.Fatalf("claim ok=%v err=%v", ok, err)
			}
			if err := store.ApplyCIObservation(ctx, work, observation(head, CIStateCodeFailure), 2, now); err != nil {
				t.Fatal(err)
			}
			_, attempt, ok, err := store.ClaimCIRepair(ctx, now)
			if err != nil || !ok {
				t.Fatalf("attempt ok=%v err=%v", ok, err)
			}
			if err := store.BindCIRepairRun(ctx, attempt, "terminal-run", now); err != nil {
				t.Fatal(err)
			}
			if err := store.FinishCIRepairFailure(ctx, attempt, test.failureClass, test.modelStarted, "bounded failure", 2, now); err != nil {
				t.Fatal(err)
			}
			var state string
			var charged int
			if err := store.db.QueryRow(`SELECT state,repair_attempts FROM repository_ci_tasks`).Scan(&state, &charged); err != nil {
				t.Fatal(err)
			}
			if state != test.wantState || charged != test.wantCharged {
				t.Fatalf("state=%s charged=%d", state, charged)
			}
		})
	}
}

func TestCIDeadlineExpiresInFlightRepairExactlyOnce(t *testing.T) {
	ctx := context.Background()
	deadline := time.Unix(25_000, 0).UTC()
	store, _ := setupCIRepairTask(t, deadline)
	defer store.Close()
	head := strings.Repeat("a", 40)
	work, ok, err := store.ClaimCIReconciliation(ctx, deadline.Add(-time.Minute))
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head, CIStateCodeFailure), 2, deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	_, attempt, ok, err := store.ClaimCIRepair(ctx, deadline.Add(-time.Minute))
	if err != nil || !ok {
		t.Fatalf("repair claim ok=%v err=%v", ok, err)
	}
	if err := store.BindCIRepairRun(ctx, attempt, "run-deadline", deadline.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if expired, err := store.ExpireCIDeadline(ctx, deadline, 2); err != nil || !expired {
		t.Fatalf("expired=%v err=%v", expired, err)
	}
	if expired, err := store.ExpireCIDeadline(ctx, deadline.Add(time.Second), 2); err != nil || expired {
		t.Fatalf("replayed expiration=%v err=%v", expired, err)
	}
	var taskState string
	var outbox int
	if err := store.db.QueryRow(`SELECT state FROM repository_ci_tasks`).Scan(&taskState); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT count(*) FROM repository_ci_escalation_outbox`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if taskState != CIEscalationPending || outbox != 1 {
		t.Fatalf("state=%s outbox=%d", taskState, outbox)
	}
}

func TestCIRepairRunStatusScheduleSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(27_000, 0).UTC()
	store, _ := setupCIRepairTask(t, now.Add(time.Hour))
	path := store.dbStatsPathForTest(t)
	head := strings.Repeat("a", 40)
	work, ok, err := store.ClaimCIReconciliation(ctx, now)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head, CIStateCodeFailure), 2, now); err != nil {
		t.Fatal(err)
	}
	_, attempt, ok, err := store.ClaimCIRepair(ctx, now)
	if err != nil || !ok {
		t.Fatalf("repair claim ok=%v err=%v", ok, err)
	}
	if err := store.BindCIRepairRun(ctx, attempt, "run-restart", now); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, _, ok, err := store.ClaimCIRepairStatus(ctx, now.Add(time.Second)); err != nil || ok {
		t.Fatalf("early status claim ok=%v err=%v", ok, err)
	}
	_, restored, ok, err := store.ClaimCIRepairStatus(ctx, now.Add(StatusPollInterval))
	if err != nil || !ok || restored.AttemptKey != attempt.AttemptKey || restored.BrokerRunID != "run-restart" || restored.Charged {
		t.Fatalf("restored=%+v ok=%v err=%v", restored, ok, err)
	}
}

func TestCISuccessCompletesWithoutPublicReport(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(30_000, 0).UTC()
	store, _ := setupCIRepairTask(t, now.Add(time.Hour))
	defer store.Close()
	head := strings.Repeat("a", 40)
	_, _ = store.RecordCIEvent(ctx, ciSignal("green", "check_run", "completed", head, 9, map[string]any{"id": 4}), now)
	work, ok, err := store.ClaimCIReconciliation(ctx, now)
	if err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	if err := store.ApplyCIObservation(ctx, work, observation(head, CIStateSuccess), 2, now); err != nil {
		t.Fatal(err)
	}
	var state string
	var outbox int
	_ = store.db.QueryRow(`SELECT state FROM repository_ci_tasks`).Scan(&state)
	_ = store.db.QueryRow(`SELECT count(*) FROM repository_ci_escalation_outbox`).Scan(&outbox)
	if state != CICompleted || outbox != 0 {
		t.Fatalf("state=%s outbox=%d", state, outbox)
	}
}

// modernc SQLite does not expose the DSN; tests retain it explicitly through
// the sole attached database file.
func (s *Store) dbStatsPathForTest(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}
