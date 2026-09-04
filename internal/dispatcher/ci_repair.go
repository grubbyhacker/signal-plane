package dispatcher

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

const (
	CIWaiting           = "waiting_ci"
	CIRepairReady       = "repair_ready"
	CIRepairRunning     = "repair_running"
	CICompleted         = "completed"
	CIEscalationPending = "escalation_pending"
	CIEscalationBlocked = "escalation_blocked"
	CIEscalated         = "escalated"

	CIStatePending        = "pending"
	CIStateSuccess        = "success"
	CIStateCodeFailure    = "code_failure"
	CIStateInfrastructure = "infrastructure_failure"

	ciReconcileLease = 2 * time.Minute
)

var githubSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

type CIRepairTask struct {
	JobID          int64
	Repository     string
	IssueNumber    int64
	PullNumber     int64
	Branch         string
	CurrentHeadSHA string
	AgentModel     string
	State          string
	RepairAttempts int
	ReconcileAt    time.Time
	LastCIState    string
}

type CIRepairPolicy struct {
	ReconciliationWake time.Duration
	ActiveTimeout      time.Duration
	MaxAttempts        int
}

func (policy CIRepairPolicy) Validate() error {
	if policy.ReconciliationWake == 0 {
		policy.ReconciliationWake = policy.ActiveTimeout
	}
	if policy.ReconciliationWake < time.Minute || policy.ReconciliationWake > 7*24*time.Hour {
		return errors.New("CI repair reconciliation wake must be between one minute and seven days")
	}
	if policy.ActiveTimeout < time.Minute || policy.ActiveTimeout > time.Hour {
		return errors.New("CI repair active timeout must be between one minute and the reviewed 60-minute broker template maximum")
	}
	if policy.MaxAttempts < 1 || policy.MaxAttempts > 2 {
		return errors.New("CI repair attempts must be between one and two")
	}
	return nil
}

func durationSeconds(value time.Duration) int {
	return int((value + time.Second - 1) / time.Second)
}

func (s *Store) ConfigureCIRepair(policy CIRepairPolicy) error {
	if policy.ReconciliationWake == 0 {
		policy.ReconciliationWake = policy.ActiveTimeout
	}
	if err := policy.Validate(); err != nil {
		return err
	}
	s.ciPolicy = policy
	return nil
}

type CIReconciliation struct {
	OperationKey string
	Task         CIRepairTask
	HeadSHA      string
	DeadlineWake bool
	Operations   int
}

type FailedCheck struct {
	Name       string `json:"name"`
	Conclusion string `json:"conclusion"`
	Summary    string `json:"summary,omitempty"`
	Evidence   string `json:"evidence,omitempty"`
}

type CIObservation struct {
	Repository    string        `json:"repository"`
	PullNumber    int64         `json:"pull_number"`
	HeadSHA       string        `json:"head_sha"`
	State         string        `json:"state"`
	FailedChecks  []FailedCheck `json:"failed_checks,omitempty"`
	Truncated     bool          `json:"truncated,omitempty"`
	ObservedBytes int64         `json:"observed_bytes,omitempty"`
}

type CIRepairAttempt struct {
	AttemptKey        string
	JobID             int64
	AttemptNumber     int
	FailedHeadSHA     string
	AgentModel        string
	State             string
	Charged           bool
	BrokerRunID       string
	MaxRuntimeSeconds int
	ExternalWait      *CIExternalWait
	ExternalResumeKey string
	ResumedGeneration int
	Operations        int
}

type CIExternalWait struct {
	Version    string    `json:"version"`
	Service    string    `json:"service"`
	Phase      string    `json:"phase"`
	Operation  string    `json:"operation"`
	Reason     string    `json:"reason"`
	Generation int       `json:"generation"`
	Since      time.Time `json:"since"`
}

func (wait CIExternalWait) Validate() error {
	if wait.Service != "github" || wait.Generation < 1 || wait.Since.IsZero() {
		return errors.New("external wait lacks GitHub identity, generation, or timestamp")
	}
	if wait.Reason != "unavailable" && wait.Reason != "rate_limited" {
		return errors.New("external wait has invalid reason")
	}
	var operations map[string]bool
	switch wait.Phase {
	case "preparation":
		operations = map[string]bool{"issue.read": true, "issue_comments.read": true, "pull.read": true, "ci.observe": true, "actions_job_log.read": true}
	case "delivery":
		operations = map[string]bool{"pull.read": true, "pull.reconcile": true, "pull.create": true, "git.push": true}
	default:
		return errors.New("external wait has invalid phase")
	}
	if !operations[wait.Operation] {
		return errors.New("external wait has invalid operation")
	}
	return nil
}

func ciResumeKey(attemptKey string, generation int) string {
	return fmt.Sprintf("ci-repair-resume:v1:%s:%d", attemptKey, generation)
}

type CIRepairDelivery struct {
	ExpectedOldHeadSHA string `json:"expected_old_head_sha"`
	CandidateHeadSHA   string `json:"candidate_head_sha"`
	ValidatedTreeSHA   string `json:"validated_tree_sha"`
	DeliveredHeadSHA   string `json:"delivered_head_sha"`
	DeliveredTreeSHA   string `json:"delivered_tree_sha"`
}

type CIEscalationReport struct {
	Job          Job
	OperationKey string
	Body         string
	Attempts     int
}

// BeginCIWait makes ready-for-review a nonterminal state. deadline is the next
// durable reconciliation wake, not an end-to-end lifecycle cutoff.
func (s *Store) BeginCIWait(ctx context.Context, job Job, pullNumber int64, branch, headSHA, agentModel string, deadline, now time.Time, initialResult json.RawMessage) error {
	if job.ID < 1 || job.Repository == "" || job.IssueNumber < 1 || pullNumber < 1 || branch == "" || !githubSHA.MatchString(headSHA) || agentModel == "" || !deadline.After(now) || len(initialResult) == 0 || len(initialResult) > 64*1024 || !json.Valid(initialResult) {
		return errors.New("CI wait requires durable job, PR, branch, exact head, agent/model, and future reconciliation wake")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_tasks(job_id,repository,issue_number,pull_number,branch,current_head_sha,agent_model,state,deadline_at,initial_result_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(job_id) DO NOTHING`, job.ID, job.Repository, job.IssueNumber, pullNumber, branch, headSHA, agentModel, CIWaiting, deadline.UnixMilli(), string(initialResult), now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return err
	}
	var stored CIRepairTask
	var deadlineMillis int64
	var storedInitial string
	err = tx.QueryRowContext(ctx, `SELECT job_id,repository,issue_number,pull_number,branch,current_head_sha,agent_model,state,repair_attempts,deadline_at,last_ci_state,initial_result_json FROM repository_ci_tasks WHERE job_id=?`, job.ID).Scan(&stored.JobID, &stored.Repository, &stored.IssueNumber, &stored.PullNumber, &stored.Branch, &stored.CurrentHeadSHA, &stored.AgentModel, &stored.State, &stored.RepairAttempts, &deadlineMillis, &stored.LastCIState, &storedInitial)
	if err != nil {
		return err
	}
	stored.ReconcileAt = time.UnixMilli(deadlineMillis).UTC()
	if stored.Repository != job.Repository || stored.IssueNumber != job.IssueNumber || stored.PullNumber != pullNumber || stored.Branch != branch || stored.CurrentHeadSHA != headSHA || stored.AgentModel != agentModel || !stored.ReconcileAt.Equal(deadline.UTC()) || storedInitial != string(initialResult) {
		return errors.New("ready-for-review replay conflicts with durable CI coordinates")
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=?,last_error='' WHERE id=? AND status IN (?,?)`, StateCIWaiting, deadline.UnixMilli(), now.UnixMilli(), job.ID, StateLaunched, StateCIWaiting); err != nil {
		return err
	}
	key := ciReconcileKey(job.ID, headSHA)
	if _, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_reconciliations(operation_key,job_id,head_sha,state,due_at,created_at,updated_at) VALUES(?,?,?,'queued',?,?,?) ON CONFLICT(job_id,head_sha) DO NOTHING`, key, job.ID, headSHA, now.UnixMilli(), now.UnixMilli(), now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordCIEvent stores delivery and semantic identities, then queues exactly
// one authoritative reconciliation for every newly observed PR/head state.
// The webhook body is evidence, never the CI decision.
func (s *Store) RecordCIEvent(ctx context.Context, signal envelope.Signal, now time.Time) (bool, error) {
	if signal.Meta.Source != "github" || signal.Meta.SourceDeliveryID == "" || signal.Meta.Namespace == "" || !githubSHA.MatchString(signal.Meta.SourceRevision) {
		return false, errors.New("CI event lacks authenticated GitHub delivery, repository, or exact head identity")
	}
	if signal.Meta.SourceEvent != "check_run" && signal.Meta.SourceEvent != "check_suite" && signal.Meta.SourceEvent != "status" && signal.Meta.SourceEvent != "pull_request" {
		return false, nil
	}
	pullNumber := int64(0)
	if signal.Meta.ObjectKind == "pull_request" {
		if _, err := fmt.Sscan(signal.Meta.ObjectID, &pullNumber); err != nil || pullNumber < 1 {
			return false, errors.New("CI event has invalid pull request identity")
		}
	}
	payloadDigest := sha256.Sum256(signal.Payload)
	digest := "sha256:" + hex.EncodeToString(payloadDigest[:])
	semantic := strings.Join([]string{"github-ci-event/v1", signal.Meta.Namespace, signal.Meta.SourceEvent, signal.Meta.SourceAction, fmt.Sprint(pullNumber), signal.Meta.SourceRevision, digest}, ":")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO repository_ci_events(delivery_id,repository,pull_number,head_sha,event_kind,action,semantic_identity,payload_digest,received_at,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`, signal.Meta.SourceDeliveryID, signal.Meta.Namespace, pullNumber, signal.Meta.SourceRevision, signal.Meta.SourceEvent, signal.Meta.SourceAction, semantic, digest, signal.Meta.ReceivedAt.UnixMilli(), now.UnixMilli())
	if err != nil {
		return false, err
	}
	inserted, _ := result.RowsAffected()
	if inserted == 0 {
		return false, tx.Commit()
	}
	rows, err := tx.QueryContext(ctx, `SELECT t.job_id,t.current_head_sha,t.state,COALESCE(a.attempt_key,''),COALESCE(a.external_wait_generation,0),COALESCE(a.resumed_external_wait_generation,0) FROM repository_ci_tasks t LEFT JOIN repository_ci_attempts a ON a.job_id=t.job_id AND a.state='running' WHERE t.repository=? AND t.state IN ('waiting_ci','repair_running') AND ((? > 0 AND t.pull_number=?) OR t.current_head_sha=?)`, signal.Meta.Namespace, pullNumber, pullNumber, signal.Meta.SourceRevision)
	if err != nil {
		return false, err
	}
	type target struct {
		jobID       int64
		currentHead string
		state       string
		attemptKey  string
		waitGen     int
		resumedGen  int
	}
	var targets []target
	for rows.Next() {
		var t target
		if err := rows.Scan(&t.jobID, &t.currentHead, &t.state, &t.attemptKey, &t.waitGen, &t.resumedGen); err != nil {
			rows.Close()
			return false, err
		}
		targets = append(targets, t)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	queued := false
	for _, target := range targets {
		if target.state == CIRepairRunning {
			if target.attemptKey == "" || target.waitGen <= target.resumedGen {
				continue
			}
			if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET due_at=MIN(due_at,?),updated_at=? WHERE attempt_key=? AND state='running'`, now.UnixMilli(), now.UnixMilli(), target.attemptKey); err != nil {
				return false, err
			}
			queued = true
			continue
		}
		head := signal.Meta.SourceRevision
		key := ciReconcileKey(target.jobID, head)
		_, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_reconciliations(operation_key,job_id,head_sha,state,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(job_id,head_sha) DO UPDATE SET state=CASE WHEN repository_ci_reconciliations.state='running' THEN 'running' ELSE 'queued' END,dirty=CASE WHEN repository_ci_reconciliations.state='running' THEN 1 ELSE repository_ci_reconciliations.dirty END,due_at=excluded.due_at,updated_at=excluded.updated_at`, key, target.jobID, head, "queued", now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return false, err
		}
		queued = true
	}
	return queued, tx.Commit()
}

func (s *Store) ClaimCIReconciliation(ctx context.Context, now time.Time) (CIReconciliation, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CIReconciliation{}, false, err
	}
	defer tx.Rollback()
	// A deadline is a one-shot durable wake. It requests authoritative state;
	// it does not infer failure from the absence of webhook traffic.
	_, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_reconciliations(operation_key,job_id,head_sha,state,deadline_wake,due_at,created_at,updated_at) SELECT 'ci-reconcile:v1:'||job_id||':'||current_head_sha,job_id,current_head_sha,'queued',1,deadline_at,deadline_at,? FROM repository_ci_tasks WHERE state='waiting_ci' AND deadline_at<=? ON CONFLICT(job_id,head_sha) DO UPDATE SET state=CASE WHEN repository_ci_reconciliations.state='completed' THEN 'queued' ELSE repository_ci_reconciliations.state END,deadline_wake=1,dirty=CASE WHEN repository_ci_reconciliations.state='running' THEN 1 ELSE repository_ci_reconciliations.dirty END,due_at=MIN(repository_ci_reconciliations.due_at,excluded.due_at),updated_at=excluded.updated_at`, now.UnixMilli(), now.UnixMilli())
	if err != nil {
		return CIReconciliation{}, false, err
	}
	var work CIReconciliation
	var deadlineWake int
	var deadlineMillis int64
	err = tx.QueryRowContext(ctx, `SELECT r.operation_key,r.head_sha,r.deadline_wake,r.operation_attempts,t.job_id,t.repository,t.issue_number,t.pull_number,t.branch,t.current_head_sha,t.agent_model,t.state,t.repair_attempts,t.deadline_at,t.last_ci_state FROM repository_ci_reconciliations r JOIN repository_ci_tasks t ON t.job_id=r.job_id WHERE t.state='waiting_ci' AND r.state IN ('queued','running') AND r.due_at<=? ORDER BY r.deadline_wake DESC,r.due_at,t.job_id LIMIT 1`, now.UnixMilli()).Scan(&work.OperationKey, &work.HeadSHA, &deadlineWake, &work.Operations, &work.Task.JobID, &work.Task.Repository, &work.Task.IssueNumber, &work.Task.PullNumber, &work.Task.Branch, &work.Task.CurrentHeadSHA, &work.Task.AgentModel, &work.Task.State, &work.Task.RepairAttempts, &deadlineMillis, &work.Task.LastCIState)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return CIReconciliation{}, false, err
		}
		return CIReconciliation{}, false, nil
	}
	if err != nil {
		return CIReconciliation{}, false, err
	}
	work.DeadlineWake = deadlineWake == 1
	work.Task.ReconcileAt = time.UnixMilli(deadlineMillis).UTC()
	_, err = tx.ExecContext(ctx, `UPDATE repository_ci_reconciliations SET state='running',dirty=0,due_at=?,updated_at=? WHERE operation_key=?`, now.Add(ciReconcileLease).UnixMilli(), now.UnixMilli(), work.OperationKey)
	if err != nil {
		return CIReconciliation{}, false, err
	}
	return work, true, tx.Commit()
}

func (s *Store) FailCIReconciliation(ctx context.Context, work CIReconciliation, failure error, retry bool, now time.Time) error {
	if work.OperationKey == "" || failure == nil {
		return errors.New("CI reconciliation failure requires operation identity and error")
	}
	if retry {
		due := now.Add(PreOutboxRetryDelay(work.Operations + 1))
		result, err := s.db.ExecContext(ctx, `UPDATE repository_ci_reconciliations SET state='queued',operation_attempts=operation_attempts+1,last_error=?,due_at=?,updated_at=? WHERE operation_key=? AND state='running'`, safeBrokerError(failure), due.UnixMilli(), now.UnixMilli(), work.OperationKey)
		return expectOne(result, err, "defer CI reconciliation")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE repository_ci_reconciliations SET state='completed',operation_attempts=operation_attempts+1,last_error=?,updated_at=? WHERE operation_key=? AND state='running'`, safeBrokerError(failure), now.UnixMilli(), work.OperationKey)
	if err := expectOne(result, err, "complete failed CI reconciliation"); err != nil {
		return err
	}
	var brokerErr BrokerError
	if errors.As(failure, &brokerErr) && brokerErr.Code == "stale_pull_head" {
		nextWake := now.Add(s.ciPolicy.ReconciliationWake)
		if _, err := tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET deadline_at=?,updated_at=? WHERE job_id=? AND state='waiting_ci'`, nextWake.UnixMilli(), now.UnixMilli(), work.Task.JobID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET due_at=?,updated_at=? WHERE id=? AND status=?`, nextWake.UnixMilli(), now.UnixMilli(), work.Task.JobID, StateCIWaiting); err != nil {
			return err
		}
	} else {
		observation := CIObservation{Repository: work.Task.Repository, PullNumber: work.Task.PullNumber, HeadSHA: work.Task.CurrentHeadSHA, State: CIStateInfrastructure, FailedChecks: []FailedCheck{{Name: "CI observation", Conclusion: "unavailable", Summary: safeBrokerError(failure)}}}
		if err := queueCIEscalation(ctx, tx, work.Task.JobID, observation, work.Task.RepairAttempts, s.ciPolicy.MaxAttempts, "observation_invalid", now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) ApplyCIObservation(ctx context.Context, work CIReconciliation, observation CIObservation, maxAttempts int, now time.Time) error {
	if maxAttempts < 1 || maxAttempts > 2 {
		return errors.New("CI repair attempts must be between one and two")
	}
	if observation.Repository != work.Task.Repository || observation.PullNumber != work.Task.PullNumber || !githubSHA.MatchString(observation.HeadSHA) {
		return errors.New("authoritative CI observation does not match durable PR coordinates")
	}
	switch observation.State {
	case CIStatePending, CIStateSuccess, CIStateCodeFailure, CIStateInfrastructure:
	default:
		return errors.New("authoritative CI observation has unknown state")
	}
	encoded, err := json.Marshal(observation)
	if err != nil || len(encoded) > 64*1024 {
		return errors.New("authoritative CI observation exceeds the durable bound")
	}
	digest := sha256.Sum256(encoded)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var state, currentHead, agentModel string
	var attempts int
	err = tx.QueryRowContext(ctx, `SELECT state,current_head_sha,agent_model,repair_attempts FROM repository_ci_tasks WHERE job_id=?`, work.Task.JobID).Scan(&state, &currentHead, &agentModel, &attempts)
	if err != nil {
		return err
	}
	if state == CICompleted || state == CIEscalated || state == CIEscalationPending || state == CIEscalationBlocked {
		return tx.Commit()
	}
	preserveWake := observation.State == CIStatePending
	if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_reconciliations SET state=CASE WHEN dirty=1 AND ?=1 THEN 'queued' ELSE 'completed' END,due_at=CASE WHEN dirty=1 AND ?=1 THEN ? ELSE due_at END,result_digest=?,updated_at=? WHERE operation_key=?`, preserveWake, preserveWake, now.UnixMilli(), "sha256:"+hex.EncodeToString(digest[:]), now.UnixMilli(), work.OperationKey); err != nil {
		return err
	}
	if currentHead != observation.HeadSHA {
		currentHead = observation.HeadSHA
		if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET current_head_sha=?,state='waiting_ci',updated_at=? WHERE job_id=?`, currentHead, now.UnixMilli(), work.Task.JobID); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET last_ci_state=?,last_ci_json=?,updated_at=? WHERE job_id=?`, observation.State, string(encoded), now.UnixMilli(), work.Task.JobID); err != nil {
		return err
	}
	if observation.State == CIStateSuccess {
		_, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='completed',terminal_at=?,updated_at=? WHERE job_id=?`, now.UnixMilli(), now.UnixMilli(), work.Task.JobID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=?`, StateCompleted, now.UnixMilli(), work.Task.JobID)
		}
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if observation.State == CIStateInfrastructure {
		if err := queueCIEscalation(ctx, tx, work.Task.JobID, observation, attempts, maxAttempts, "infrastructure_failure", now); err != nil {
			return err
		}
		return tx.Commit()
	}
	if observation.State == CIStateCodeFailure && attempts >= maxAttempts {
		if err := queueCIEscalation(ctx, tx, work.Task.JobID, observation, attempts, maxAttempts, "attempts_exhausted", now); err != nil {
			return err
		}
		return tx.Commit()
	}
	if observation.State != CIStateCodeFailure {
		nextWake := now.Add(s.ciPolicy.ReconciliationWake)
		_, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='waiting_ci',deadline_at=?,updated_at=? WHERE job_id=?`, nextWake.UnixMilli(), now.UnixMilli(), work.Task.JobID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=? WHERE id=?`, StateCIWaiting, nextWake.UnixMilli(), now.UnixMilli(), work.Task.JobID)
		}
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	number := attempts + 1
	attemptKey := fmt.Sprintf("ci-repair:v1:%d:%s:%s:%d", work.Task.JobID, agentModel, currentHead, number)
	maxRuntimeSeconds := durationSeconds(s.ciPolicy.ActiveTimeout)
	_, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_attempts(attempt_key,job_id,attempt_number,failed_head_sha,agent_model,state,max_runtime_seconds,expected_old_head_sha,due_at,created_at,updated_at) VALUES(?,?,?,?,?,'ready',?,?,?,?,?) ON CONFLICT(attempt_key) DO NOTHING`, attemptKey, work.Task.JobID, number, currentHead, agentModel, maxRuntimeSeconds, currentHead, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='repair_ready',updated_at=? WHERE job_id=?`, now.UnixMilli(), work.Task.JobID)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=? WHERE id=?`, StateCIRepair, now.UnixMilli(), now.UnixMilli(), work.Task.JobID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ClaimCIRepair(ctx context.Context, now time.Time) (CIRepairTask, CIRepairAttempt, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CIRepairTask{}, CIRepairAttempt{}, false, err
	}
	defer tx.Rollback()
	var task CIRepairTask
	var attempt CIRepairAttempt
	var charged int
	err = tx.QueryRowContext(ctx, `SELECT t.job_id,t.repository,t.issue_number,t.pull_number,t.branch,t.current_head_sha,t.agent_model,t.state,t.repair_attempts,t.last_ci_state,a.attempt_key,a.attempt_number,a.failed_head_sha,a.agent_model,a.state,a.charged,a.broker_run_id,a.max_runtime_seconds,a.operation_attempts FROM repository_ci_tasks t JOIN repository_ci_attempts a ON a.job_id=t.job_id WHERE t.state='repair_ready' AND a.state='ready' AND a.due_at<=? ORDER BY a.due_at,t.job_id LIMIT 1`, now.UnixMilli()).Scan(&task.JobID, &task.Repository, &task.IssueNumber, &task.PullNumber, &task.Branch, &task.CurrentHeadSHA, &task.AgentModel, &task.State, &task.RepairAttempts, &task.LastCIState, &attempt.AttemptKey, &attempt.AttemptNumber, &attempt.FailedHeadSHA, &attempt.AgentModel, &attempt.State, &charged, &attempt.BrokerRunID, &attempt.MaxRuntimeSeconds, &attempt.Operations)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return task, attempt, false, err
		}
		return task, attempt, false, nil
	}
	if err != nil {
		return task, attempt, false, err
	}
	attempt.JobID = task.JobID
	attempt.Charged = charged == 1
	if attempt.MaxRuntimeSeconds == 0 {
		attempt.MaxRuntimeSeconds = durationSeconds(s.ciPolicy.ActiveTimeout)
		if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET max_runtime_seconds=?,updated_at=? WHERE attempt_key=? AND max_runtime_seconds=0`, attempt.MaxRuntimeSeconds, now.UnixMilli(), attempt.AttemptKey); err != nil {
			return task, attempt, false, err
		}
	}
	return task, attempt, true, tx.Commit()
}

// BindCIRepairRun records the broker's durable run identity without charging a
// coding attempt. The broker terminal result is authoritative about whether a
// model execution actually started.
func (s *Store) BindCIRepairRun(ctx context.Context, attempt CIRepairAttempt, brokerRunID string, now time.Time) error {
	if attempt.AttemptKey == "" || brokerRunID == "" {
		return errors.New("repair attempt and broker run identities are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var storedRun string
	err = tx.QueryRowContext(ctx, `SELECT broker_run_id FROM repository_ci_attempts WHERE attempt_key=?`, attempt.AttemptKey).Scan(&storedRun)
	if err != nil {
		return err
	}
	if storedRun != "" && storedRun != brokerRunID {
		return errors.New("repair attempt replay conflicts with broker run")
	}
	_, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET state='running',broker_run_id=?,operation_attempts=operation_attempts+1,last_error='',due_at=?,updated_at=? WHERE attempt_key=? AND state IN ('ready','running')`, brokerRunID, now.Add(StatusPollInterval).UnixMilli(), now.UnixMilli(), attempt.AttemptKey)
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='repair_running',updated_at=? WHERE job_id=? AND state IN ('repair_ready','repair_running')`, now.UnixMilli(), attempt.JobID)
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) DeferCIRepairLaunch(ctx context.Context, attemptKey string, due time.Time, failure error, now time.Time) error {
	if attemptKey == "" || !due.After(now) {
		return errors.New("repair launch retry requires attempt identity and future due time")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE repository_ci_attempts SET operation_attempts=operation_attempts+1,last_error=?,due_at=?,updated_at=? WHERE attempt_key=? AND state='ready'`, safeBrokerError(failure), due.UnixMilli(), now.UnixMilli(), attemptKey)
	return expectOne(result, err, "defer CI repair launch")
}

func (s *Store) ClaimCIRepairStatus(ctx context.Context, now time.Time) (CIRepairTask, CIRepairAttempt, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CIRepairTask{}, CIRepairAttempt{}, false, err
	}
	defer tx.Rollback()
	var task CIRepairTask
	var attempt CIRepairAttempt
	var charged int
	var wait CIExternalWait
	var waitSince sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT t.job_id,t.repository,t.issue_number,t.pull_number,t.branch,t.current_head_sha,t.agent_model,t.state,t.repair_attempts,t.last_ci_state,a.attempt_key,a.attempt_number,a.failed_head_sha,a.agent_model,a.state,a.charged,a.broker_run_id,a.max_runtime_seconds,a.external_wait_service,a.external_wait_phase,a.external_wait_operation,a.external_wait_reason,a.external_wait_generation,a.external_wait_since,a.external_resume_key,a.resumed_external_wait_generation,a.operation_attempts FROM repository_ci_tasks t JOIN repository_ci_attempts a ON a.job_id=t.job_id WHERE t.state='repair_running' AND a.state='running' AND a.due_at<=? ORDER BY a.due_at,t.job_id LIMIT 1`, now.UnixMilli()).Scan(&task.JobID, &task.Repository, &task.IssueNumber, &task.PullNumber, &task.Branch, &task.CurrentHeadSHA, &task.AgentModel, &task.State, &task.RepairAttempts, &task.LastCIState, &attempt.AttemptKey, &attempt.AttemptNumber, &attempt.FailedHeadSHA, &attempt.AgentModel, &attempt.State, &charged, &attempt.BrokerRunID, &attempt.MaxRuntimeSeconds, &wait.Service, &wait.Phase, &wait.Operation, &wait.Reason, &wait.Generation, &waitSince, &attempt.ExternalResumeKey, &attempt.ResumedGeneration, &attempt.Operations)
	if errors.Is(err, sql.ErrNoRows) {
		return task, attempt, false, tx.Commit()
	}
	if err != nil {
		return task, attempt, false, err
	}
	attempt.JobID = task.JobID
	attempt.Charged = charged == 1
	if attempt.MaxRuntimeSeconds == 0 {
		attempt.MaxRuntimeSeconds = durationSeconds(s.ciPolicy.ActiveTimeout)
		if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET max_runtime_seconds=?,updated_at=? WHERE attempt_key=? AND max_runtime_seconds=0`, attempt.MaxRuntimeSeconds, now.UnixMilli(), attempt.AttemptKey); err != nil {
			return task, attempt, false, err
		}
	}
	if wait.Generation > 0 {
		if waitSince.Valid {
			wait.Since = time.UnixMilli(waitSince.Int64).UTC()
		}
		attempt.ExternalWait = &wait
	}
	if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET due_at=?,updated_at=? WHERE attempt_key=? AND state='running'`, now.Add(ciReconcileLease).UnixMilli(), now.UnixMilli(), attempt.AttemptKey); err != nil {
		return task, attempt, false, err
	}
	return task, attempt, true, tx.Commit()
}

func (s *Store) RecordCIRepairExternalWait(ctx context.Context, attempt CIRepairAttempt, wait CIExternalWait, now time.Time) (string, error) {
	if attempt.AttemptKey == "" || attempt.BrokerRunID == "" {
		return "", errors.New("external wait requires durable attempt and broker run identities")
	}
	if err := wait.Validate(); err != nil {
		return "", err
	}
	key := ciResumeKey(attempt.AttemptKey, wait.Generation)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var state, runID, service, phase, operation, reason, storedKey string
	var generation, resumed int
	var since sql.NullInt64
	if err = tx.QueryRowContext(ctx, `SELECT state,broker_run_id,external_wait_service,external_wait_phase,external_wait_operation,external_wait_reason,external_wait_generation,external_wait_since,external_resume_key,resumed_external_wait_generation FROM repository_ci_attempts WHERE attempt_key=?`, attempt.AttemptKey).Scan(&state, &runID, &service, &phase, &operation, &reason, &generation, &since, &storedKey, &resumed); err != nil {
		return "", err
	}
	if state != "running" || runID != attempt.BrokerRunID {
		return "", errors.New("external wait does not match the running repair attempt")
	}
	if wait.Generation < generation || wait.Generation <= resumed {
		return "", errors.New("external wait generation is stale")
	}
	if wait.Generation == generation {
		if service != wait.Service || phase != wait.Phase || operation != wait.Operation || reason != wait.Reason || !since.Valid || since.Int64 != wait.Since.UnixMilli() || storedKey != key {
			return "", errors.New("external wait replay conflicts with durable state")
		}
		return key, tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET external_wait_service=?,external_wait_phase=?,external_wait_operation=?,external_wait_reason=?,external_wait_generation=?,external_wait_since=?,external_resume_key=?,last_error='',updated_at=? WHERE attempt_key=? AND state='running'`, wait.Service, wait.Phase, wait.Operation, wait.Reason, wait.Generation, wait.Since.UnixMilli(), key, now.UnixMilli(), attempt.AttemptKey)
	if err != nil {
		return "", err
	}
	return key, tx.Commit()
}

func (s *Store) MarkCIRepairExternalResumed(ctx context.Context, attemptKey, resumeKey string, generation int, due, now time.Time) error {
	if attemptKey == "" || resumeKey == "" || generation < 1 || !due.After(now) {
		return errors.New("external resume requires durable identity, generation, and future status wake")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE repository_ci_attempts SET resumed_external_wait_generation=?,last_error='',due_at=?,updated_at=? WHERE attempt_key=? AND state='running' AND external_wait_generation=? AND external_resume_key=? AND resumed_external_wait_generation<?`, generation, due.UnixMilli(), now.UnixMilli(), attemptKey, generation, resumeKey, generation)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	var stored int
	if err := s.db.QueryRowContext(ctx, `SELECT resumed_external_wait_generation FROM repository_ci_attempts WHERE attempt_key=?`, attemptKey).Scan(&stored); err != nil {
		return err
	}
	if stored >= generation {
		return nil
	}
	return errors.New("external resume no longer matches the running wait")
}

func (s *Store) DeferCIRepairStatus(ctx context.Context, attemptKey string, due time.Time, failure error, now time.Time) error {
	if attemptKey == "" || !due.After(now) {
		return errors.New("repair status retry requires attempt identity and future due time")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE repository_ci_attempts SET operation_attempts=operation_attempts+1,last_error=?,due_at=?,updated_at=? WHERE attempt_key=? AND state='running'`, safeBrokerError(failure), due.UnixMilli(), now.UnixMilli(), attemptKey)
	return expectOne(result, err, "defer CI repair status")
}

func (s *Store) ScheduleCIRepairStatus(ctx context.Context, attemptKey string, due, now time.Time) error {
	if attemptKey == "" || !due.After(now) {
		return errors.New("repair status schedule requires attempt identity and future due time")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE repository_ci_attempts SET last_error='',due_at=?,updated_at=? WHERE attempt_key=? AND state='running'`, due.UnixMilli(), now.UnixMilli(), attemptKey)
	return expectOne(result, err, "schedule CI repair status")
}

func (s *Store) FinishCIRepairFailure(ctx context.Context, attempt CIRepairAttempt, failureClass string, modelStarted bool, detail string, maxAttempts int, now time.Time) error {
	if attempt.AttemptKey == "" || maxAttempts < 1 || maxAttempts > 2 {
		return errors.New("repair failure requires attempt identity and valid attempt policy")
	}
	switch failureClass {
	case "infrastructure", "model_or_code", "delivery_or_lease":
	default:
		return errors.New("repair terminal has invalid failure_class")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var task CIRepairTask
	var state, storedClass, lastJSON string
	var charged int
	err = tx.QueryRowContext(ctx, `SELECT a.state,a.charged,a.failure_class,t.job_id,t.repository,t.issue_number,t.pull_number,t.branch,t.current_head_sha,t.agent_model,t.state,t.repair_attempts,t.last_ci_json FROM repository_ci_attempts a JOIN repository_ci_tasks t ON t.job_id=a.job_id WHERE a.attempt_key=?`, attempt.AttemptKey).Scan(&state, &charged, &storedClass, &task.JobID, &task.Repository, &task.IssueNumber, &task.PullNumber, &task.Branch, &task.CurrentHeadSHA, &task.AgentModel, &task.State, &task.RepairAttempts, &lastJSON)
	if err != nil {
		return err
	}
	if state == "failed" {
		if storedClass != failureClass || (charged == 1) != modelStarted {
			return errors.New("repair failure replay conflicts with durable terminal")
		}
		return tx.Commit()
	}
	if state != "running" && !(state == "ready" && !modelStarted) {
		return errors.New("repair failure is not active")
	}
	if modelStarted && charged == 0 {
		if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET charged=1 WHERE attempt_key=?`, attempt.AttemptKey); err != nil {
			return err
		}
		task.RepairAttempts++
		if task.RepairAttempts > maxAttempts {
			return errors.New("repair attempt charge exceeds policy")
		}
		if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET repair_attempts=?,updated_at=? WHERE job_id=?`, task.RepairAttempts, now.UnixMilli(), task.JobID); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET state='failed',failure_class=?,last_error=?,updated_at=? WHERE attempt_key=?`, failureClass, safeBrokerError(errors.New(detail)), now.UnixMilli(), attempt.AttemptKey); err != nil {
		return err
	}
	stop := !modelStarted || failureClass == "delivery_or_lease" || task.RepairAttempts >= maxAttempts || attempt.AttemptNumber >= maxAttempts
	if stop {
		observation := CIObservation{Repository: task.Repository, PullNumber: task.PullNumber, HeadSHA: task.CurrentHeadSHA, State: CIStateCodeFailure}
		if lastJSON != "" {
			_ = json.Unmarshal([]byte(lastJSON), &observation)
		}
		reason := "attempts_exhausted"
		if !modelStarted {
			reason = "infrastructure_failure"
			observation.State = CIStateInfrastructure
		}
		if err := queueCIEscalation(ctx, tx, task.JobID, observation, task.RepairAttempts, maxAttempts, reason, now); err != nil {
			return err
		}
		return tx.Commit()
	}
	number := attempt.AttemptNumber + 1
	key := fmt.Sprintf("ci-repair:v1:%d:%s:%s:%d", task.JobID, task.AgentModel, task.CurrentHeadSHA, number)
	maxRuntimeSeconds := durationSeconds(s.ciPolicy.ActiveTimeout)
	if _, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_attempts(attempt_key,job_id,attempt_number,failed_head_sha,agent_model,state,max_runtime_seconds,expected_old_head_sha,due_at,created_at,updated_at) VALUES(?,?,?,?,?,'ready',?,?,?,?,?)`, key, task.JobID, number, task.CurrentHeadSHA, task.AgentModel, maxRuntimeSeconds, task.CurrentHeadSHA, now.UnixMilli(), now.UnixMilli(), now.UnixMilli()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='repair_ready',updated_at=? WHERE job_id=?`, now.UnixMilli(), task.JobID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=?`, StateCIRepair, now.UnixMilli(), task.JobID); err != nil {
		return err
	}
	return tx.Commit()
}

// ChargeCIRepairAttempt records the broker's durable proof that the model
// execution started. Replaying the same terminal result cannot charge twice.
func (s *Store) ChargeCIRepairAttempt(ctx context.Context, attemptKey string, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var jobID int64
	var number, charged int
	var state string
	if err = tx.QueryRowContext(ctx, `SELECT job_id,attempt_number,charged,state FROM repository_ci_attempts WHERE attempt_key=?`, attemptKey).Scan(&jobID, &number, &charged, &state); err != nil {
		return err
	}
	if state != "running" && state != "candidate_delivered" && state != "failed" {
		return errors.New("repair attempt is not chargeable")
	}
	if charged == 1 {
		return tx.Commit()
	}
	if _, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET charged=1,updated_at=? WHERE attempt_key=? AND charged=0`, now.UnixMilli(), attemptKey); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET repair_attempts=repair_attempts+1,updated_at=? WHERE job_id=? AND repair_attempts<?`, now.UnixMilli(), jobID, number)
	if err := expectOne(result, err, "charge CI repair attempt"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CompleteCIRepairDelivery(ctx context.Context, attemptKey string, delivery CIRepairDelivery, now time.Time) error {
	for _, sha := range []string{delivery.ExpectedOldHeadSHA, delivery.CandidateHeadSHA, delivery.ValidatedTreeSHA, delivery.DeliveredHeadSHA, delivery.DeliveredTreeSHA} {
		if !githubSHA.MatchString(sha) {
			return errors.New("repair delivery requires exact commit and tree identities")
		}
	}
	if delivery.ValidatedTreeSHA != delivery.DeliveredTreeSHA {
		return errors.New("delivered commit tree does not match the validated tree")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var jobID int64
	var state string
	var charged int
	if err = tx.QueryRowContext(ctx, `SELECT job_id,state,charged FROM repository_ci_attempts WHERE attempt_key=?`, attemptKey).Scan(&jobID, &state, &charged); err != nil {
		return err
	}
	if charged != 1 {
		return errors.New("successful repair delivery lacks a charged model execution")
	}
	if state == "candidate_delivered" {
		var candidate, validated, delivered, deliveredTree string
		if err = tx.QueryRowContext(ctx, `SELECT candidate_head_sha,validated_tree_sha,delivered_head_sha,delivered_tree_sha FROM repository_ci_attempts WHERE attempt_key=?`, attemptKey).Scan(&candidate, &validated, &delivered, &deliveredTree); err != nil {
			return err
		}
		if candidate != delivery.CandidateHeadSHA || validated != delivery.ValidatedTreeSHA || delivered != delivery.DeliveredHeadSHA || deliveredTree != delivery.DeliveredTreeSHA {
			return errors.New("repair delivery replay conflicts with durable result")
		}
		return tx.Commit()
	}
	if state != "running" {
		return errors.New("repair delivery is not active")
	}
	_, err = tx.ExecContext(ctx, `UPDATE repository_ci_attempts SET state='candidate_delivered',expected_old_head_sha=?,candidate_head_sha=?,validated_tree_sha=?,delivered_head_sha=?,delivered_tree_sha=?,updated_at=? WHERE attempt_key=?`, delivery.ExpectedOldHeadSHA, delivery.CandidateHeadSHA, delivery.ValidatedTreeSHA, delivery.DeliveredHeadSHA, delivery.DeliveredTreeSHA, now.UnixMilli(), attemptKey)
	if err == nil {
		nextWake := now.Add(s.ciPolicy.ReconciliationWake)
		_, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='waiting_ci',current_head_sha=?,last_ci_state='',deadline_at=?,updated_at=? WHERE job_id=?`, delivery.DeliveredHeadSHA, nextWake.UnixMilli(), now.UnixMilli(), jobID)
		if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=? WHERE id=?`, StateCIWaiting, nextWake.UnixMilli(), now.UnixMilli(), jobID)
		}
	}
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ClaimCIEscalation(ctx context.Context, now time.Time) (CIEscalationReport, bool, error) {
	var report CIEscalationReport
	err := s.db.QueryRowContext(ctx, `SELECT j.id,j.semantic_key,j.route_id,j.launch_profile,j.repository,j.issue_number,j.source_delivery_id,j.broker_run_id,j.status,j.attempts,j.pre_outbox_attempts,o.operation_key,o.body,o.attempts FROM repository_ci_escalation_outbox o JOIN jobs j ON j.id=o.job_id WHERE o.state='pending' AND o.due_at<=? ORDER BY o.due_at,o.job_id LIMIT 1`, now.UnixMilli()).Scan(&report.Job.ID, &report.Job.SemanticKey, &report.Job.RouteID, &report.Job.Profile, &report.Job.Repository, &report.Job.IssueNumber, &report.Job.DeliveryID, &report.Job.BrokerRunID, &report.Job.Status, &report.Job.Attempts, &report.Job.PreOutboxAttempts, &report.OperationKey, &report.Body, &report.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return CIEscalationReport{}, false, nil
	}
	return report, err == nil, err
}

func (s *Store) MarkCIEscalationDelivered(ctx context.Context, report CIEscalationReport, result CommentResult, now time.Time) error {
	if result.ID < 1 || !validHTTPSURL(result.URL) {
		return errors.New("CI escalation delivery lacks GitHub comment identity")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	updated, err := tx.ExecContext(ctx, `UPDATE repository_ci_escalation_outbox SET state='delivered',comment_id=?,comment_url=?,attempts=attempts+1,updated_at=? WHERE job_id=? AND operation_key=? AND state='pending'`, result.ID, result.URL, now.UnixMilli(), report.Job.ID, report.OperationKey)
	if err := expectOne(updated, err, "deliver CI escalation"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='escalated',terminal_at=?,updated_at=? WHERE job_id=? AND state='escalation_pending'`, now.UnixMilli(), now.UnixMilli(), report.Job.ID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=?`, StateFailed, now.UnixMilli(), report.Job.ID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkCIEscalationFailure(ctx context.Context, report CIEscalationReport, retry bool, message string, now time.Time) error {
	state := "blocked"
	due := now
	if retry && report.Attempts+1 < ReportRetryMaxAttempts {
		state = "pending"
		due = now.Add(ReportRetryDelay(report.Attempts + 1))
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE repository_ci_escalation_outbox SET state=?,attempts=attempts+1,last_error=?,due_at=?,updated_at=? WHERE job_id=? AND operation_key=? AND state='pending'`, state, safeBrokerError(errors.New(message)), due.UnixMilli(), now.UnixMilli(), report.Job.ID, report.OperationKey)
	if err := expectOne(result, err, "record CI escalation failure"); err != nil {
		return err
	}
	if state == "blocked" {
		if _, err := tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state=?,updated_at=? WHERE job_id=? AND state=?`, CIEscalationBlocked, now.UnixMilli(), report.Job.ID, CIEscalationPending); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?,last_error=?,updated_at=? WHERE id=?`, StateReportBlocked, "CI escalation delivery blocked: "+safeBrokerError(errors.New(message)), now.UnixMilli(), report.Job.ID); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func queueCIEscalation(ctx context.Context, tx *sql.Tx, jobID int64, observation CIObservation, attempts, maxAttempts int, reason string, now time.Time) error {
	checks := append([]FailedCheck(nil), observation.FailedChecks...)
	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	lines := []string{fmt.Sprintf("CI repair stopped after %d of %d coding attempts.", attempts, maxAttempts), fmt.Sprintf("Pull request #%d remains open at `%s`.", observation.PullNumber, observation.HeadSHA[:12])}
	if reason == "observation_invalid" {
		lines[0] = "The authoritative CI observation could not be validated; no speculative code repair was launched."
	} else if observation.State == CIStateInfrastructure {
		lines[0] = "CI reported an infrastructure or runner failure; no speculative code repair was launched."
	}
	if len(checks) > 0 {
		lines = append(lines, "Failed required checks:")
		for i, check := range checks {
			if i == 5 {
				lines = append(lines, "- Additional failures were omitted; inspect the bounded broker evidence.")
				break
			}
			summary := strings.TrimSpace(check.Summary)
			if len(summary) > 160 {
				summary = summary[:160]
			}
			line := fmt.Sprintf("- %s (%s)", check.Name, check.Conclusion)
			if summary != "" {
				line += ": " + summary
			}
			lines = append(lines, line)
		}
	}
	rows, err := tx.QueryContext(ctx, `SELECT attempt_number,failed_head_sha,state,failure_class,delivered_head_sha FROM repository_ci_attempts WHERE job_id=? ORDER BY attempt_number`, jobID)
	if err != nil {
		return err
	}
	var history []string
	for rows.Next() {
		var number int
		var failedHead, state, failureClass, deliveredHead string
		if err := rows.Scan(&number, &failedHead, &state, &failureClass, &deliveredHead); err != nil {
			rows.Close()
			return err
		}
		detail := state
		if failureClass != "" {
			detail = failureClass
		} else if githubSHA.MatchString(deliveredHead) {
			detail = "delivered " + deliveredHead[:12]
		}
		failed := failedHead
		if githubSHA.MatchString(failedHead) {
			failed = failedHead[:12]
		}
		history = append(history, fmt.Sprintf("- Attempt %d for `%s`: %s", number, failed, detail))
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(history) > 0 {
		lines = append(lines, "Repair history:")
		lines = append(lines, history...)
	}
	lines = append(lines, "A human or stronger model should inspect the failed-check evidence, prior repair results, and current branch before continuing.")
	body := strings.Join(lines, "\n")
	key := fmt.Sprintf("ci-escalation:v1:%d", jobID)
	_, err = tx.ExecContext(ctx, `INSERT INTO repository_ci_escalation_outbox(job_id,operation_key,body,state,due_at,created_at,updated_at) VALUES(?,?,?,'pending',?,?,?) ON CONFLICT(job_id) DO NOTHING`, jobID, key, body, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE repository_ci_tasks SET state='escalation_pending',updated_at=? WHERE job_id=?`, now.UnixMilli(), jobID)
	}
	if err == nil {
		_, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,updated_at=? WHERE id=?`, StateCIEscalation, now.UnixMilli(), jobID)
	}
	return err
}

func ciReconcileKey(jobID int64, headSHA string) string {
	return fmt.Sprintf("ci-reconcile:v1:%d:%s", jobID, headSHA)
}
