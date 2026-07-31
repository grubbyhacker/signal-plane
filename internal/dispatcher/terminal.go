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

type ReportReconciliation struct {
	Mode                  string `json:"mode"`
	Status                string `json:"status"`
	JobID                 int64  `json:"job_id"`
	SemanticKey           string `json:"semantic_key"`
	BrokerRunID           string `json:"broker_run_id"`
	TerminalResultVersion string `json:"terminal_result_version"`
	OutboxID              int64  `json:"outbox_id"`
	IdempotencyKey        string `json:"idempotency_key"`
	PriorAttempts         int    `json:"prior_attempts"`
	PriorError            string `json:"prior_error"`
}

type LaunchFailureReconciliation struct {
	Mode                  string `json:"mode"`
	Status                string `json:"status"`
	JobID                 int64  `json:"job_id"`
	SemanticKey           string `json:"semantic_key"`
	BrokerRunID           string `json:"broker_run_id"`
	TerminalResultVersion string `json:"terminal_result_version"`
	PriorStatus           string `json:"prior_status"`
	PriorAttempts         int    `json:"prior_attempts"`
	PriorError            string `json:"prior_error"`
}

type blockedReportState struct {
	jobID, outboxID                     int64
	semanticKey, brokerRunID, jobStatus string
	jobError, version, idempotencyKey   string
	outboxStatus                        string
	attempts                            int
	outboxError                         string
	commentID, deliveredAt              sql.NullInt64
	commentURL, terminalVersion         string
	terminalRunID                       string
}

// ValidateTerminalResult binds a broker projection to its durable job.  This
// deliberately rejects unknown outcome/output instead of inventing a report.
func ValidateTerminalResult(job Job, r TerminalResult) error {
	if r.Version != terminalResultVersion || r.RunID != job.BrokerRunID || r.Profile != job.Profile || r.Repo != job.Repository {
		return errors.New("invalid terminal result correlation")
	}
	prelaunchFailure := r.RunID == "" && r.TerminalSource == "signal_plane" &&
		r.Status == StateFailed && r.Outcome == StateFailed && r.FailureStage == "broker_launch"
	if r.RunID == "" && !prelaunchFailure {
		return errors.New("terminal result is missing broker run correlation")
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
		"- Broker run: `" + displayBrokerRun(r.RunID) + "`",
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

func displayBrokerRun(runID string) string {
	if runID == "" {
		return "not acknowledged"
	}
	return runID
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
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=?,last_error='' WHERE id=? AND status IN (?,?,?,?,?,?)`, StateReportPending, now.UnixMilli(), now.UnixMilli(), job.ID, StateLaunched, StateReportPending, StateReportRetry, StateFailed, StatePendingLaunch, StateLaunchRetry)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil || updated != 1 {
		return errors.New("terminal result job state changed concurrently")
	}
	return tx.Commit()
}

func (s *Store) QueueLaunchFailure(ctx context.Context, job Job, reason string, now time.Time) error {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "broker launch failed before a run was acknowledged"
	}
	if len(reason) > 512 {
		reason = reason[:512]
	}
	result := TerminalResult{
		Version:              terminalResultVersion,
		RunID:                "",
		Profile:              job.Profile,
		Repo:                 job.Repository,
		Status:               StateFailed,
		Outcome:              StateFailed,
		FinalizeReason:       "broker_launch_failed",
		TerminalSource:       "signal_plane",
		IdempotencyKeyDigest: "unavailable-before-broker-acknowledgement",
		RequestFingerprint:   brokerSourceID(job),
		LaunchConfigVersion:  "unavailable-before-broker-acknowledgement",
		FailureStage:         "broker_launch",
		FailureReason:        reason,
	}
	return s.QueueTerminalResult(ctx, job, result, now)
}

func sameStoredTerminalResult(a, b TerminalResult) bool {
	return a.Version == b.Version && a.RunID == b.RunID && a.Profile == b.Profile && a.Repo == b.Repo &&
		a.Branch == b.Branch && a.Status == b.Status && a.Outcome == b.Outcome &&
		a.FinalizeReason == b.FinalizeReason && a.TerminalSource == b.TerminalSource &&
		a.IdempotencyKeyDigest == b.IdempotencyKeyDigest && a.RequestFingerprint == b.RequestFingerprint &&
		a.LaunchConfigVersion == b.LaunchConfigVersion && a.FinalSummary == b.FinalSummary &&
		a.FailureStage == b.FailureStage && a.FailureReason == b.FailureReason
}

func (s *Store) ReconcileFailedLaunch(
	ctx context.Context,
	jobID int64,
	brokerRunID string,
	terminal TerminalResult,
	execute bool,
	now time.Time,
) (LaunchFailureReconciliation, error) {
	if jobID < 1 || brokerRunID == "" {
		return LaunchFailureReconciliation{}, errors.New("job id and broker run id are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !execute})
	if err != nil {
		return LaunchFailureReconciliation{}, err
	}
	defer tx.Rollback()
	var job Job
	var first sql.NullInt64
	var lastError string
	err = tx.QueryRowContext(ctx, `
		SELECT id,semantic_key,route_id,launch_profile,repository,issue_number,
		       source_delivery_id,broker_run_id,status,attempts,first_launch_attempt_at,last_error
		FROM jobs WHERE id=?`, jobID).
		Scan(
			&job.ID, &job.SemanticKey, &job.RouteID, &job.Profile, &job.Repository,
			&job.IssueNumber, &job.DeliveryID, &job.BrokerRunID, &job.Status,
			&job.Attempts, &first, &lastError,
		)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return LaunchFailureReconciliation{}, errors.New("failed launch job does not exist")
		}
		return LaunchFailureReconciliation{}, err
	}
	if job.Status != StateFailed || job.BrokerRunID != "" {
		return LaunchFailureReconciliation{}, errors.New("job is not an unreported failed launch")
	}
	var terminalRows, outboxRows int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM terminal_results WHERE job_id=?`, jobID).Scan(&terminalRows); err != nil {
		return LaunchFailureReconciliation{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM notification_outbox WHERE job_id=?`, jobID).Scan(&outboxRows); err != nil {
		return LaunchFailureReconciliation{}, err
	}
	if terminalRows != 0 || outboxRows != 0 {
		return LaunchFailureReconciliation{}, errors.New("failed launch already has terminal reporting state")
	}
	job.BrokerRunID = brokerRunID
	if err := ValidateTerminalResult(job, terminal); err != nil {
		return LaunchFailureReconciliation{}, err
	}
	if _, err := RenderTerminalComment(job, terminal); err != nil {
		return LaunchFailureReconciliation{}, err
	}
	report := LaunchFailureReconciliation{
		Mode:                  "plan",
		Status:                "validated",
		JobID:                 job.ID,
		SemanticKey:           job.SemanticKey,
		BrokerRunID:           brokerRunID,
		TerminalResultVersion: terminal.Version,
		PriorStatus:           StateFailed,
		PriorAttempts:         job.Attempts,
		PriorError:            lastError,
	}
	if !execute {
		return report, tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET broker_run_id=?,status=?,due_at=?,updated_at=?
		WHERE id=? AND status=? AND broker_run_id=''`,
		brokerRunID, StateLaunched, now.UnixMilli(), now.UnixMilli(), jobID, StateFailed,
	)
	if err != nil {
		return LaunchFailureReconciliation{}, err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return LaunchFailureReconciliation{}, errors.New("failed launch job changed concurrently")
	}
	if err := tx.Commit(); err != nil {
		return LaunchFailureReconciliation{}, err
	}
	report.Mode = "execute"
	report.Status = "requeued"
	return report, nil
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
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,last_error='',updated_at=? WHERE id=? AND status IN (?,?)`, terminal, now.UnixMilli(), r.JobID, StateReportPending, StateReportRetry)
	if err != nil {
		return err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return errors.New("reported job state changed concurrently")
	}
	return tx.Commit()
}

func (s *Store) ReconcileBlockedReport(ctx context.Context, jobID int64, brokerRunID, idempotencyKey string, execute bool, now time.Time) (ReportReconciliation, error) {
	if jobID < 1 || brokerRunID == "" || idempotencyKey == "" {
		return ReportReconciliation{}, errors.New("job id, broker run id, and idempotency key are required")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: !execute})
	if err != nil {
		return ReportReconciliation{}, err
	}
	defer tx.Rollback()
	var state blockedReportState
	err = tx.QueryRowContext(ctx, `
		SELECT j.id,j.semantic_key,j.broker_run_id,j.status,j.last_error,
		       o.id,o.terminal_result_version,o.idempotency_key,o.status,o.attempts,o.last_error,
		       o.comment_id,o.comment_url,o.delivered_at,t.version,t.run_id
		FROM jobs j
		JOIN terminal_results t ON t.job_id=j.id
		JOIN notification_outbox o ON o.job_id=j.id
		WHERE j.id=?`, jobID).
		Scan(&state.jobID, &state.semanticKey, &state.brokerRunID, &state.jobStatus, &state.jobError,
			&state.outboxID, &state.version, &state.idempotencyKey, &state.outboxStatus, &state.attempts, &state.outboxError,
			&state.commentID, &state.commentURL, &state.deliveredAt, &state.terminalVersion, &state.terminalRunID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ReportReconciliation{}, errors.New("blocked report does not exist")
		}
		return ReportReconciliation{}, err
	}
	if state.brokerRunID != brokerRunID || state.terminalRunID != brokerRunID {
		return ReportReconciliation{}, errors.New("broker run correlation does not match")
	}
	if state.idempotencyKey != idempotencyKey {
		return ReportReconciliation{}, errors.New("outbox idempotency key does not match")
	}
	if state.version != state.terminalVersion {
		return ReportReconciliation{}, errors.New("terminal result version does not match outbox")
	}
	if state.jobStatus != StateReportBlocked || state.outboxStatus != "blocked" {
		return ReportReconciliation{}, errors.New("report is not durably blocked")
	}
	if state.commentID.Valid || state.commentURL != "" || state.deliveredAt.Valid {
		return ReportReconciliation{}, errors.New("blocked report already has delivery identity")
	}
	if state.jobError == "" || state.outboxError == "" || state.jobError != state.outboxError {
		return ReportReconciliation{}, errors.New("blocked report failure evidence is incomplete")
	}
	report := ReportReconciliation{
		Mode:                  "plan",
		Status:                "validated",
		JobID:                 state.jobID,
		SemanticKey:           state.semanticKey,
		BrokerRunID:           state.brokerRunID,
		TerminalResultVersion: state.version,
		OutboxID:              state.outboxID,
		IdempotencyKey:        state.idempotencyKey,
		PriorAttempts:         state.attempts,
		PriorError:            state.outboxError,
	}
	if !execute {
		return report, tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `UPDATE notification_outbox SET status='retry',due_at=?,updated_at=? WHERE id=? AND status='blocked' AND comment_id IS NULL AND comment_url='' AND delivered_at IS NULL`, now.UnixMilli(), now.UnixMilli(), state.outboxID)
	if err != nil {
		return ReportReconciliation{}, err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return ReportReconciliation{}, errors.New("blocked outbox changed concurrently")
	}
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=? WHERE id=? AND status=?`, StateReportRetry, now.UnixMilli(), now.UnixMilli(), state.jobID, StateReportBlocked)
	if err != nil {
		return ReportReconciliation{}, err
	}
	if updated, err := result.RowsAffected(); err != nil || updated != 1 {
		return ReportReconciliation{}, errors.New("blocked job changed concurrently")
	}
	if err := tx.Commit(); err != nil {
		return ReportReconciliation{}, err
	}
	report.Mode = "execute"
	report.Status = "requeued"
	return report, nil
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
