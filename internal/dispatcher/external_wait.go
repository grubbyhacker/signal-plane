package dispatcher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

type RepositoryRunExternalWait struct {
	JobID             int64
	BrokerRunID       string
	Wait              CIExternalWait
	ResumeKey         string
	MaxRuntimeSeconds int
	State             string
}

func repositoryRunResumeKey(job Job, generation int) string {
	return fmt.Sprintf("repository-run-resume:v1:%d:%s:%d", job.ID, job.BrokerRunID, generation)
}

// RecordRepositoryRunExternalWait persists the broker generation before Signal
// can issue a resume. A previously admitted GitHub event may have made the
// durable row resume_ready; replaying status must not erase that wake.
func (s *Store) RecordRepositoryRunExternalWait(ctx context.Context, job Job, wait CIExternalWait, now time.Time) (RepositoryRunExternalWait, error) {
	if job.ID < 1 || job.BrokerRunID == "" {
		return RepositoryRunExternalWait{}, errors.New("repository run wait requires durable job and broker run identities")
	}
	if err := wait.Validate(); err != nil {
		return RepositoryRunExternalWait{}, err
	}
	key := repositoryRunResumeKey(job, wait.Generation)
	seconds := durationSeconds(s.ciPolicy.ActiveTimeout)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RepositoryRunExternalWait{}, err
	}
	defer tx.Rollback()
	var stored RepositoryRunExternalWait
	var since int64
	err = tx.QueryRowContext(ctx, `SELECT job_id,broker_run_id,service,phase,operation,reason,generation,since,resume_key,max_runtime_seconds,state FROM repository_run_external_waits WHERE job_id=?`, job.ID).Scan(&stored.JobID, &stored.BrokerRunID, &stored.Wait.Service, &stored.Wait.Phase, &stored.Wait.Operation, &stored.Wait.Reason, &stored.Wait.Generation, &since, &stored.ResumeKey, &stored.MaxRuntimeSeconds, &stored.State)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.ExecContext(ctx, `INSERT INTO repository_run_external_waits(job_id,broker_run_id,service,phase,operation,reason,generation,since,resume_key,max_runtime_seconds,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,'waiting',?,?)`, job.ID, job.BrokerRunID, wait.Service, wait.Phase, wait.Operation, wait.Reason, wait.Generation, wait.Since.UnixMilli(), key, seconds, now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return RepositoryRunExternalWait{}, err
		}
		stored = RepositoryRunExternalWait{JobID: job.ID, BrokerRunID: job.BrokerRunID, Wait: wait, ResumeKey: key, MaxRuntimeSeconds: seconds, State: "waiting"}
	} else if err != nil {
		return RepositoryRunExternalWait{}, err
	} else {
		stored.Wait.Since = time.UnixMilli(since).UTC()
		if wait.Generation < stored.Wait.Generation {
			return RepositoryRunExternalWait{}, errors.New("repository run external wait generation is stale")
		}
		if wait.Generation == stored.Wait.Generation {
			if stored.BrokerRunID != job.BrokerRunID || stored.Wait.Service != wait.Service || stored.Wait.Phase != wait.Phase || stored.Wait.Operation != wait.Operation || stored.Wait.Reason != wait.Reason || !stored.Wait.Since.Equal(time.UnixMilli(wait.Since.UnixMilli()).UTC()) || stored.ResumeKey != key {
				return RepositoryRunExternalWait{}, errors.New("repository run external wait replay conflicts with durable state")
			}
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE repository_run_external_waits SET service=?,phase=?,operation=?,reason=?,generation=?,since=?,resume_key=?,max_runtime_seconds=?,state='waiting',updated_at=? WHERE job_id=? AND broker_run_id=?`, wait.Service, wait.Phase, wait.Operation, wait.Reason, wait.Generation, wait.Since.UnixMilli(), key, seconds, now.UnixMilli(), job.ID, job.BrokerRunID)
			if err != nil {
				return RepositoryRunExternalWait{}, err
			}
			stored = RepositoryRunExternalWait{JobID: job.ID, BrokerRunID: job.BrokerRunID, Wait: wait, ResumeKey: key, MaxRuntimeSeconds: seconds, State: "waiting"}
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,updated_at=? WHERE id=? AND broker_run_id=?`, StateExternalWaiting, now.Add(s.ciPolicy.ReconciliationWake).UnixMilli(), now.UnixMilli(), job.ID, job.BrokerRunID); err != nil {
		return RepositoryRunExternalWait{}, err
	}
	return stored, tx.Commit()
}

// RecordRepositoryRunExternalWake accepts only a GitHub issue event for the
// same repository and originating issue. The resumed preparation performs the
// definitive broker-authorized GitHub read, so a false-positive webhook wake
// safely returns to waiting_external with the next generation.
func (s *Store) RecordRepositoryRunExternalWake(ctx context.Context, signal envelope.Signal, now time.Time) (bool, error) {
	if signal.Meta.Source != "github" || !signal.Meta.Authentication.Verified || signal.Meta.Namespace == "" || signal.Meta.ObjectKind != "issue" {
		return false, nil
	}
	var issueNumber int64
	if _, err := fmt.Sscan(signal.Meta.ObjectID, &issueNumber); err != nil || issueNumber < 1 {
		return false, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE repository_run_external_waits SET state='resume_ready',updated_at=? WHERE state IN ('waiting','resume_ready') AND job_id IN (SELECT id FROM jobs WHERE repository=? AND issue_number=? AND status=?)`, now.UnixMilli(), signal.Meta.Namespace, issueNumber, StateExternalWaiting)
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed == 0 {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE jobs SET due_at=?,updated_at=? WHERE repository=? AND issue_number=? AND status=?`, now.UnixMilli(), now.UnixMilli(), signal.Meta.Namespace, issueNumber, StateExternalWaiting); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *Store) DeferRepositoryRunExternalResume(ctx context.Context, job Job, due time.Time, failure error, now time.Time) error {
	message := "repository run external resume deferred"
	if failure != nil {
		message = safeBrokerError(failure)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE jobs SET due_at=?,last_error=?,pre_outbox_attempts=pre_outbox_attempts+1,updated_at=? WHERE id=? AND broker_run_id=? AND status=?`, due.UnixMilli(), message, now.UnixMilli(), job.ID, job.BrokerRunID, StateExternalWaiting)
	return expectOne(result, err, "defer repository run external resume")
}

func (s *Store) MarkRepositoryRunExternalResumed(ctx context.Context, wait RepositoryRunExternalWait, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE repository_run_external_waits SET state='resumed',updated_at=? WHERE job_id=? AND broker_run_id=? AND generation=? AND resume_key=? AND state='resume_ready'`, now.UnixMilli(), wait.JobID, wait.BrokerRunID, wait.Wait.Generation, wait.ResumeKey)
	if err := expectOne(result, err, "mark repository run external resume"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error='',pre_outbox_attempts=0,updated_at=? WHERE id=? AND broker_run_id=? AND status=?`, StateLaunched, now.Add(StatusPollInterval).UnixMilli(), now.UnixMilli(), wait.JobID, wait.BrokerRunID, StateExternalWaiting)
	if err := expectOne(result, err, "schedule resumed repository run"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ReconcileRepositoryRunExternalResume(ctx context.Context, job Job, now time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE repository_run_external_waits SET state='resumed',updated_at=? WHERE job_id=? AND broker_run_id=? AND state='resume_ready'`, now.UnixMilli(), job.ID, job.BrokerRunID)
	if err := expectOne(result, err, "reconcile repository run external resume"); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error='',pre_outbox_attempts=0,updated_at=? WHERE id=? AND broker_run_id=? AND status=?`, StateLaunched, now.UnixMilli(), now.UnixMilli(), job.ID, job.BrokerRunID, StateExternalWaiting)
	if err := expectOne(result, err, "restore reconciled repository run"); err != nil {
		return err
	}
	return tx.Commit()
}
