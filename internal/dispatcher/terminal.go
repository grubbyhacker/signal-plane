package dispatcher

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const terminalResultVersion = "repository-task-terminal-result/v1"
const maxFinalSummaryBytes = 60_000 // safely below GitHub's comment limit with the harness header.

// ValidateTerminalResult binds a broker projection to its durable job.  This
// deliberately rejects unknown outcome/output instead of inventing a report.
func ValidateTerminalResult(job Job, r TerminalResult) error {
	if r.Version != terminalResultVersion || r.RunID != job.BrokerRunID || r.Profile != job.Profile || r.Repo != job.Repository {
		return errors.New("invalid terminal result correlation")
	}
	if !utf8.ValidString(r.FinalSummary) || len(r.FinalSummary) > maxFinalSummaryBytes {
		return errors.New("invalid terminal result final summary")
	}
	if !utf8.ValidString(r.FailureStage) || !utf8.ValidString(r.FailureReason) {
		return errors.New("invalid terminal result text")
	}
	switch r.Outcome {
	case "no_change_required", "ready_for_review", "failed", "timed_out", "stopped", "cancelled":
	default:
		return errors.New("invalid terminal result outcome")
	}
	return nil
}

func RenderTerminalComment(job Job, r TerminalResult) string {
	verification := "not reported"
	if r.Outcome == "no_change_required" || r.Outcome == "ready_for_review" {
		verification = "succeeded"
	}
	lines := []string{"## Repository task terminal result", "", "- Outcome: `" + r.Outcome + "`", fmt.Sprintf("- Semantic job: `%d`", job.ID), "- Broker run: `" + r.RunID + "`", "- Verification: " + verification}
	if r.Branch != "" {
		lines = append(lines, "- Branch: `"+r.Branch+"`")
	}
	if r.Outcome == "no_change_required" {
		lines = append(lines, "- Result: the task and verification succeeded; the repository already satisfied the request.")
	}
	if r.FailureStage != "" {
		lines = append(lines, "- Failure stage: `"+r.FailureStage+"`")
	}
	if r.FailureReason != "" {
		lines = append(lines, "- Diagnostic: "+r.FailureReason)
	}
	if r.FinalSummary == "" {
		lines = append(lines, "", "The worker did not produce a valid final summary; this harness-generated terminal result is reported instead.")
	} else {
		lines = append(lines, "", "---", "", r.FinalSummary)
	}
	return strings.Join(lines, "\n") + "\n"
}

func reportKey(job Job, version string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", job.ID, version)))
	return fmt.Sprintf("repository-task-terminal:v1:%x", sum[:])
}

func (s *Store) QueueTerminalResult(ctx context.Context, job Job, r TerminalResult, now time.Time) error {
	if err := ValidateTerminalResult(job, r); err != nil {
		return err
	}
	body := RenderTerminalComment(job, r)
	key := reportKey(job, r.Version)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO terminal_results(job_id,version,run_id,profile,repository,branch,status,outcome,final_summary,failure_stage,failure_reason,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO NOTHING`, job.ID, r.Version, r.RunID, r.Profile, r.Repo, r.Branch, r.Status, r.Outcome, r.FinalSummary, r.FailureStage, r.FailureReason, now.UnixMilli())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO notification_outbox(job_id,terminal_result_version,body,idempotency_key,status,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO NOTHING`, job.ID, r.Version, body, key, "pending", now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=?,last_error='' WHERE id=? AND status IN (?,?)`, StateReportPending, now.UnixMilli(), now.UnixMilli(), job.ID, StateLaunched, StateReportPending)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ClaimReportDue(ctx context.Context, now time.Time) (Report, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT o.id,o.job_id,o.terminal_result_version,o.body,o.idempotency_key,o.attempts,j.id,j.semantic_key,j.route_id,j.launch_profile,j.repository,j.issue_number,j.source_delivery_id,j.broker_run_id,j.status,j.attempts FROM notification_outbox o JOIN jobs j ON j.id=o.job_id WHERE o.status IN ('pending','retry') AND o.due_at<=? ORDER BY o.id LIMIT 1`, now.UnixMilli())
	var r Report
	if err := row.Scan(&r.ID, &r.JobID, &r.Version, &r.Body, &r.IdempotencyKey, &r.Attempts, &r.Job.ID, &r.Job.SemanticKey, &r.Job.RouteID, &r.Job.Profile, &r.Job.Repository, &r.Job.IssueNumber, &r.Job.DeliveryID, &r.Job.BrokerRunID, &r.Job.Status, &r.Job.Attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Report{}, false, nil
		}
		return Report{}, false, err
	}
	return r, true, nil
}

func (s *Store) MarkReportDelivered(ctx context.Context, r Report, id int64, url string, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE notification_outbox SET status='delivered',attempts=attempts+1,comment_id=?,comment_url=?,delivered_at=?,last_error='',updated_at=? WHERE id=? AND status IN ('pending','retry')`, id, url, now.UnixMilli(), now.UnixMilli(), r.ID); err != nil {
		return err
	}
	terminal := StateFailed
	var outcome string
	if err = tx.QueryRowContext(ctx, `SELECT outcome FROM terminal_results WHERE job_id=?`, r.JobID).Scan(&outcome); err != nil {
		return err
	}
	switch outcome {
	case "no_change_required", "ready_for_review":
		terminal = StateCompleted
	case "timed_out":
		terminal = StateTimedOut
	case "stopped":
		terminal = StateStopped
	case "cancelled":
		terminal = StateCancelled
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=? AND status IN (?,?)`, terminal, now.UnixMilli(), r.JobID, StateReportPending, StateReportRetry); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) MarkReportFailure(ctx context.Context, r Report, retry bool, safe string, now time.Time) error {
	status := "blocked"
	jobState := StateReportBlocked
	due := now
	if retry {
		status = "retry"
		jobState = StateReportRetry
		due = now.Add(LaunchRetryDelay(r.Attempts + 1))
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE notification_outbox SET status=?,attempts=attempts+1,due_at=?,last_error=?,updated_at=? WHERE id=? AND status IN ('pending','retry')`, status, due.UnixMilli(), safe, now.UnixMilli(), r.ID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=? AND status IN (?,?)`, jobState, due.UnixMilli(), safe, now.UnixMilli(), r.JobID, StateReportPending, StateReportRetry)
	if err != nil {
		return err
	}
	return tx.Commit()
}
