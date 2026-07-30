package dispatcher

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	terminalResultVersion = "repository-task-terminal-result/v1"
	workerResultVersion   = "repository-task-worker-result/v1"
	maxTerminalTextBytes  = 32_768
	maxCommentBytes       = 65_536
)

type workerTerminalResult struct {
	Version      string `json:"version"`
	Outcome      string `json:"outcome"`
	Detail       string `json:"detail"`
	Stage        string `json:"stage"`
	RunID        string `json:"run_id"`
	Repository   string `json:"repository"`
	BaseBranch   string `json:"base_branch"`
	Branch       string `json:"branch"`
	VerifyTask   string `json:"verify_task"`
	Verification struct {
		Status string `json:"status"`
	} `json:"verification"`
	PullRequest *struct {
		Number  int64  `json:"number"`
		HTMLURL string `json:"html_url"`
		URL     string `json:"url"`
	} `json:"pull_request,omitempty"`
}

// ValidateTerminalResult binds a broker projection to its durable job.  This
// deliberately rejects unknown outcome/output instead of inventing a report.
func ValidateTerminalResult(job Job, r TerminalResult) error {
	if r.Version != terminalResultVersion || r.RunID != job.BrokerRunID || r.Profile != job.Profile || r.Repo != job.Repository {
		return errors.New("invalid terminal result correlation")
	}
	for _, value := range []string{r.Branch, r.Status, r.Outcome, r.FinalizeReason, r.TerminalSource, r.IdempotencyKeyDigest, r.RequestFingerprint, r.LaunchConfigVersion, r.FinalSummary, r.FailureStage, r.FailureReason} {
		if !utf8.ValidString(value) || len(value) > maxTerminalTextBytes {
			return errors.New("invalid terminal result text")
		}
	}
	if r.Status == "" || r.Outcome == "" || r.IdempotencyKeyDigest == "" || r.RequestFingerprint == "" || r.LaunchConfigVersion == "" {
		return errors.New("terminal result is missing correlation fields")
	}
	if len(r.FinalSummary) > maxTerminalTextBytes {
		return errors.New("invalid terminal result final summary")
	}
	switch r.Outcome {
	case "no_change_required", "ready_for_review", "failed", "timed_out", "stopped", "cancelled":
	default:
		return errors.New("invalid terminal result outcome")
	}
	if !terminalOutcomeMatchesStatus(r.Status, r.Outcome, r.FailureStage) {
		return errors.New("terminal result outcome does not match broker status")
	}
	if r.Result == nil {
		if r.FailureStage == "" || r.FailureReason == "" || r.FinalSummary != "" {
			return errors.New("terminal result fallback is incomplete")
		}
		return nil
	}
	encoded, err := json.Marshal(r.Result)
	if err != nil || len(encoded) > maxTerminalTextBytes {
		return errors.New("invalid bounded worker result")
	}
	var worker workerTerminalResult
	if err := json.Unmarshal(encoded, &worker); err != nil {
		return errors.New("invalid worker result")
	}
	if worker.Version != workerResultVersion || worker.Outcome != r.Outcome || worker.RunID != r.RunID || worker.Repository != r.Repo || worker.Branch != r.Branch {
		return errors.New("invalid worker result correlation")
	}
	switch worker.Verification.Status {
	case "passed", "failed", "not_run":
	default:
		return errors.New("invalid worker verification status")
	}
	if r.FinalSummary == "" {
		return errors.New("worker result is missing final summary")
	}
	if worker.Outcome == "ready_for_review" {
		if worker.PullRequest == nil || worker.PullRequest.Number < 1 || !validHTTPSURL(worker.PullRequest.HTMLURL) || !validHTTPSURL(worker.PullRequest.URL) {
			return errors.New("ready worker result is missing pull request identity")
		}
	} else if worker.PullRequest != nil {
		return errors.New("non-review worker result contains pull request identity")
	}
	return nil
}

func terminalOutcomeMatchesStatus(status, outcome, failureStage string) bool {
	switch status {
	case "completed":
		return outcome == "no_change_required" || outcome == "ready_for_review" || outcome == "failed" && failureStage != ""
	case "failed":
		return outcome == "failed"
	case "timed_out":
		return outcome == "timed_out"
	case "stopped":
		return outcome == "stopped"
	case "cancelled":
		return outcome == "cancelled"
	default:
		return false
	}
}

func validHTTPSURL(raw string) bool {
	if len(raw) == 0 || len(raw) > 2048 {
		return false
	}
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == "https" && parsed.Host != "" && parsed.User == nil
}

func RenderTerminalComment(job Job, r TerminalResult) (string, error) {
	verification := "not_run"
	var worker workerTerminalResult
	if r.Result != nil {
		encoded, _ := json.Marshal(r.Result)
		_ = json.Unmarshal(encoded, &worker)
		verification = worker.Verification.Status
	}
	lines := []string{
		"## Repository task terminal result", "",
		"- Outcome: `" + r.Outcome + "`",
		fmt.Sprintf("- Semantic job: `%d`", job.ID),
		"- Semantic key: `" + job.SemanticKey + "`",
		"- Broker run: `" + r.RunID + "`",
		"- Result version: `" + r.Version + "`",
		"- Launch config: `" + r.LaunchConfigVersion + "`",
		"- Launch idempotency digest: `" + r.IdempotencyKeyDigest + "`",
		"- Request fingerprint: `" + r.RequestFingerprint + "`",
		"- Verification: `" + verification + "`",
	}
	if r.Branch != "" {
		lines = append(lines, "- Branch: `"+r.Branch+"`")
	}
	if r.Outcome == "failed" {
		if worker.Stage != "" {
			lines = append(lines, "- Failure stage: `"+worker.Stage+"`")
		}
		if worker.Detail != "" {
			lines = append(lines, "- Diagnostic: "+worker.Detail)
		}
	} else {
		if worker.Detail != "" {
			lines = append(lines, "- Detail: "+worker.Detail)
		}
		if worker.Stage != "" {
			lines = append(lines, "- Worker stage: `"+worker.Stage+"`")
		}
	}
	if r.Outcome == "no_change_required" {
		if verification == "passed" {
			lines = append(lines, "- Result: the task and verification succeeded; the repository already satisfied the request.")
		} else {
			lines = append(lines, "- Result: the task succeeded; no verification task ran, and the repository already satisfied the request.")
		}
	}
	if worker.PullRequest != nil {
		lines = append(lines, fmt.Sprintf("- Pull request: [#%d](%s)", worker.PullRequest.Number, worker.PullRequest.HTMLURL))
	}
	if r.FailureStage != "" && worker.Stage == "" {
		lines = append(lines, "- Failure stage: `"+r.FailureStage+"`")
	}
	if r.FailureReason != "" && worker.Detail == "" {
		lines = append(lines, "- Diagnostic: "+r.FailureReason)
	}
	if r.FinalSummary == "" {
		lines = append(lines, "", "The worker did not produce a valid final summary; this harness-generated terminal result is reported instead.")
	} else {
		lines = append(lines, "", "---", "", r.FinalSummary)
	}
	body := strings.Join(lines, "\n") + "\n"
	if !utf8.ValidString(body) || len(body) > maxCommentBytes {
		return "", errors.New("terminal comment exceeds GitHub comment limit")
	}
	return body, nil
}

func reportKey(job Job, version string) string {
	sum := sha256.Sum256([]byte(job.SemanticKey + "\x00" + version))
	return fmt.Sprintf("repository-task-terminal:v1:%x", sum[:])
}

func (s *Store) QueueTerminalResult(ctx context.Context, job Job, r TerminalResult, now time.Time) error {
	if err := ValidateTerminalResult(job, r); err != nil {
		return err
	}
	body, err := RenderTerminalComment(job, r)
	if err != nil {
		return err
	}
	key := reportKey(job, r.Version)
	resultJSON := ""
	if r.Result != nil {
		encoded, err := json.Marshal(r.Result)
		if err != nil {
			return err
		}
		resultJSON = string(encoded)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO terminal_results(job_id,version,run_id,profile,repository,branch,status,outcome,finalize_reason,terminal_source,idempotency_key_digest,request_fingerprint,launch_config_version,result_json,final_summary,failure_stage,failure_reason,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO NOTHING`,
		job.ID, r.Version, r.RunID, r.Profile, r.Repo, r.Branch, r.Status, r.Outcome, r.FinalizeReason, r.TerminalSource, r.IdempotencyKeyDigest, r.RequestFingerprint, r.LaunchConfigVersion, resultJSON, r.FinalSummary, r.FailureStage, r.FailureReason, now.UnixMilli())
	if err != nil {
		return err
	}
	var stored TerminalResult
	var storedResult string
	if err = tx.QueryRowContext(ctx, `SELECT version,run_id,profile,repository,branch,status,outcome,finalize_reason,terminal_source,idempotency_key_digest,request_fingerprint,launch_config_version,result_json,final_summary,failure_stage,failure_reason FROM terminal_results WHERE job_id=?`, job.ID).
		Scan(&stored.Version, &stored.RunID, &stored.Profile, &stored.Repo, &stored.Branch, &stored.Status, &stored.Outcome, &stored.FinalizeReason, &stored.TerminalSource, &stored.IdempotencyKeyDigest, &stored.RequestFingerprint, &stored.LaunchConfigVersion, &storedResult, &stored.FinalSummary, &stored.FailureStage, &stored.FailureReason); err != nil {
		return err
	}
	if !sameStoredTerminalResult(stored, r) || storedResult != resultJSON {
		return errors.New("terminal result conflicts with durable job result")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO notification_outbox(job_id,terminal_result_version,body,idempotency_key,status,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO NOTHING`, job.ID, r.Version, body, key, "pending", now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	var storedVersion, storedBody, storedKey string
	if err = tx.QueryRowContext(ctx, `SELECT terminal_result_version,body,idempotency_key FROM notification_outbox WHERE job_id=?`, job.ID).Scan(&storedVersion, &storedBody, &storedKey); err != nil {
		return err
	}
	if storedVersion != r.Version || storedBody != body || storedKey != key {
		return errors.New("notification outbox conflicts with durable terminal result")
	}
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=?,last_error='' WHERE id=? AND status IN (?,?,?)`, StateReportPending, now.UnixMilli(), now.UnixMilli(), job.ID, StateLaunched, StateReportPending, StateReportRetry)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return errors.New("terminal result job state changed concurrently")
	}
	return tx.Commit()
}

func sameStoredTerminalResult(a, b TerminalResult) bool {
	return a.Version == b.Version && a.RunID == b.RunID && a.Profile == b.Profile && a.Repo == b.Repo &&
		a.Branch == b.Branch && a.Status == b.Status && a.Outcome == b.Outcome &&
		a.FinalizeReason == b.FinalizeReason && a.TerminalSource == b.TerminalSource &&
		a.IdempotencyKeyDigest == b.IdempotencyKeyDigest && a.RequestFingerprint == b.RequestFingerprint &&
		a.LaunchConfigVersion == b.LaunchConfigVersion && a.FinalSummary == b.FinalSummary &&
		a.FailureStage == b.FailureStage && a.FailureReason == b.FailureReason
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
	result, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET status='delivered',attempts=attempts+1,comment_id=?,comment_url=?,delivered_at=?,last_error='',updated_at=? WHERE id=? AND status IN ('pending','retry')`, id, url, now.UnixMilli(), now.UnixMilli(), r.ID)
	if err != nil {
		return err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return errors.New("notification outbox delivery changed concurrently")
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
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=? AND status IN (?,?)`, terminal, now.UnixMilli(), r.JobID, StateReportPending, StateReportRetry)
	if err != nil {
		return err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return errors.New("reported job state changed concurrently")
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
	result, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET status=?,attempts=attempts+1,due_at=?,last_error=?,updated_at=? WHERE id=? AND status IN ('pending','retry')`, status, due.UnixMilli(), safe, now.UnixMilli(), r.ID)
	if err != nil {
		return err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return errors.New("notification outbox failure changed concurrently")
	}
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=? AND status IN (?,?)`, jobState, due.UnixMilli(), safe, now.UnixMilli(), r.JobID, StateReportPending, StateReportRetry)
	if err != nil {
		return err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return errors.New("report failure job state changed concurrently")
	}
	return tx.Commit()
}
