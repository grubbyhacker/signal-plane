package dispatcher

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const terminalResultVersion = "repository-task-terminal-result/v1"
const maxTerminalBytes = 32 << 10 // gh-agent-broker's complete-or-reject contract.

type workerResult struct {
	Outcome      string `json:"outcome"`
	Verification string `json:"verification"`
	PullRequest  *struct {
		Number int64  `json:"number"`
		URL    string `json:"url"`
	} `json:"pull_request,omitempty"`
}

func resultFields(r TerminalResult) (workerResult, error) {
	var out workerResult
	if len(r.Result) == 0 || len(r.Result) > maxTerminalBytes || !json.Valid(r.Result) || json.Unmarshal(r.Result, &out) != nil || out.Outcome != r.Outcome {
		return out, errors.New("invalid terminal result object")
	}
	return out, nil
}

// ValidateTerminalResult accepts only the two worker result shapes that the
// reporter can make claims about.  Failure fallbacks deliberately have no
// Result object and are generated with HarnessTerminalResult below.
func ValidateTerminalResult(job Job, r TerminalResult) error {
	if r.Version != terminalResultVersion || r.RunID != job.BrokerRunID || r.Profile != job.Profile || r.Repo != job.Repository {
		return errors.New("invalid terminal result correlation")
	}
	if !utf8.ValidString(r.FinalSummary) || len(r.FinalSummary) > maxTerminalBytes || !utf8.ValidString(r.FailureStage) || !utf8.ValidString(r.FailureReason) {
		return errors.New("invalid terminal result text")
	}
	fields, err := resultFields(r)
	if err != nil {
		return err
	}
	switch r.Outcome {
	case "no_change_required":
		if fields.Verification != "passed" || fields.PullRequest != nil {
			return errors.New("invalid no_change_required result")
		}
	case "ready_for_review":
		if fields.Verification != "passed" || fields.PullRequest == nil || fields.PullRequest.Number < 1 || fields.PullRequest.URL == "" {
			return errors.New("invalid ready_for_review result")
		}
	case "failed", "timed_out", "stopped", "cancelled":
		if fields.Verification == "" {
			return errors.New("invalid failure result")
		}
	default:
		return errors.New("invalid terminal result outcome")
	}
	return nil
}

func HarnessTerminalResult(job Job, outcome, reason string) TerminalResult {
	if outcome != "timed_out" && outcome != "stopped" && outcome != "cancelled" {
		outcome = "failed"
	}
	return TerminalResult{Version: terminalResultVersion, RunID: job.BrokerRunID, Profile: job.Profile, Repo: job.Repository, Status: outcome, Outcome: outcome, FailureStage: "terminal_result", FailureReason: reason}
}

func RenderTerminalComment(job Job, r TerminalResult) string {
	verification := "not available"
	var prNumber int64
	var prURL string
	if len(r.Result) != 0 {
		if fields, err := resultFields(r); err == nil {
			verification, prNumber, prURL = fields.Verification, 0, ""
			if fields.PullRequest != nil {
				prNumber, prURL = fields.PullRequest.Number, fields.PullRequest.URL
			}
		}
	}
	lines := []string{"## Repository task terminal result", "", "- Outcome: `" + r.Outcome + "`", fmt.Sprintf("- Semantic job: `%d`", job.ID), "- Broker run: `" + r.RunID + "`", "- Branch: `" + r.Branch + "`", "- Verification: `" + verification + "`"}
	if prNumber != 0 {
		lines = append(lines, fmt.Sprintf("- Ready-for-review PR: [#%d](%s)", prNumber, prURL))
	}
	if r.FailureStage != "" {
		lines = append(lines, "- Failure stage: `"+r.FailureStage+"`")
	}
	if r.FailureReason != "" {
		lines = append(lines, "- Diagnostic: "+r.FailureReason)
	}
	if r.FinalSummary == "" {
		lines = append(lines, "", "The worker did not produce valid terminal output; this harness-generated terminal result is reported instead.")
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
	if len(r.Result) != 0 {
		if err := ValidateTerminalResult(job, r); err != nil {
			return err
		}
	} else if r.FailureStage == "" {
		return errors.New("invalid harness terminal result")
	}
	body, key := RenderTerminalComment(job, r), reportKey(job, r.Version)
	if len(body) > maxTerminalBytes {
		return errors.New("terminal comment exceeds 32768 bytes")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// An idempotent re-observation is safe only when every immutable projection
	// and outbox identity is byte-for-byte identical.  Anything else is a data
	// integrity failure, never an ignored ON CONFLICT.
	var existing struct{ version, run, profile, repo, branch, status, outcome, result, summary, stage, reason string }
	err = tx.QueryRowContext(ctx, `SELECT version,run_id,profile,repository,branch,status,outcome,result_json,final_summary,failure_stage,failure_reason FROM terminal_results WHERE job_id=?`, job.ID).Scan(&existing.version, &existing.run, &existing.profile, &existing.repo, &existing.branch, &existing.status, &existing.outcome, &existing.result, &existing.summary, &existing.stage, &existing.reason)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO terminal_results(job_id,version,run_id,profile,repository,branch,status,outcome,result_json,final_summary,failure_stage,failure_reason,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, job.ID, r.Version, r.RunID, r.Profile, r.Repo, r.Branch, r.Status, r.Outcome, string(r.Result), r.FinalSummary, r.FailureStage, r.FailureReason, now.UnixMilli())
		if err != nil {
			return err
		}
	} else if err != nil || existing != (struct{ version, run, profile, repo, branch, status, outcome, result, summary, stage, reason string }{r.Version, r.RunID, r.Profile, r.Repo, r.Branch, r.Status, r.Outcome, string(r.Result), r.FinalSummary, r.FailureStage, r.FailureReason}) {
		if err != nil {
			return err
		}
		return errors.New("terminal result conflict")
	}
	var oldBody, oldKey string
	err = tx.QueryRowContext(ctx, `SELECT body,idempotency_key FROM notification_outbox WHERE job_id=?`, job.ID).Scan(&oldBody, &oldKey)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO notification_outbox(job_id,terminal_result_version,body,idempotency_key,status,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)`, job.ID, r.Version, body, key, "pending", now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return err
		}
	} else if err != nil || oldBody != body || oldKey != key {
		if err != nil {
			return err
		}
		return errors.New("notification outbox conflict")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=?,last_error='' WHERE id=? AND status IN (?,?)`, StateReportPending, now.UnixMilli(), now.UnixMilli(), job.ID, StateLaunched, StateReportPending); err != nil {
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
	_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=? AND status IN (?,?)`, terminal, now.UnixMilli(), r.JobID, StateReportPending, StateReportRetry)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) MarkReportFailure(ctx context.Context, r Report, retry bool, safe string, now time.Time) error {
	status, jobState, due := "blocked", StateReportBlocked, now
	if retry {
		status, jobState, due = "retry", StateReportRetry, now.Add(LaunchRetryDelay(r.Attempts+1))
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
