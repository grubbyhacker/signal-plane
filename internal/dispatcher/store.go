package dispatcher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
	_ "modernc.org/sqlite"
)

const (
	SchemaVersion      = workledger.SchemaVersion
	checkpointKey      = "last_persisted_jetstream_sequence"
	StatePendingLaunch = "pending_launch"
	StateLaunchRetry   = "launch_retry"
	StateLaunched      = "launched"
	StateCompleted     = "completed"
	StateFailed        = "failed"
	StateTimedOut      = "timed_out"
	RecoveryIncomplete = "incomplete"
	RecoveryCompleted  = "completed"
)

var lifecycleStates = []string{StatePendingLaunch, StateLaunchRetry, StateLaunched, StateCompleted, StateFailed, StateTimedOut}

type Store struct{ db *sql.DB }

type Job struct {
	ID             int64
	SemanticKey    string
	Repository     string
	IssueNumber    int64
	DeliveryID     string
	BrokerRunID    string
	Status         string
	Attempts       int
	FirstAttemptAt time.Time
}

type WorkKind int

const (
	WorkLaunch WorkKind = iota + 1
	WorkStatus
)

type Work struct {
	Kind WorkKind
	Job  Job
}

// OpenStoreReadOnly validates and inspects an existing database without
// running migrations or creating SQLite journal files.
func OpenStoreReadOnly(path string) (*Store, error) {
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite read-only: %w", err)
	}
	db.SetMaxOpenConns(1)
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		db.Close()
		return nil, fmt.Errorf("read sqlite schema version: %w", err)
	}
	if version < 1 || version > SchemaVersion {
		db.Close()
		return nil, fmt.Errorf("unsupported sqlite schema version %d", version)
	}
	return &Store{db: db}, nil
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	var existingVersion int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&existingVersion); err != nil {
		db.Close()
		return nil, fmt.Errorf("read sqlite schema version: %w", err)
	}
	if existingVersion > SchemaVersion {
		db.Close()
		return nil, fmt.Errorf("sqlite schema version %d is newer than supported version %d", existingVersion, SchemaVersion)
	}
	statements := []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS deliveries (delivery_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, semantic_key TEXT, stream_sequence INTEGER NOT NULL DEFAULT 0, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, first_launch_attempt_at INTEGER, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS dispatcher_metadata (key TEXT PRIMARY KEY, value INTEGER NOT NULL)`,
		`INSERT INTO dispatcher_metadata(key,value) VALUES('last_persisted_jetstream_sequence',0) ON CONFLICT(key) DO NOTHING`,
		`CREATE TABLE IF NOT EXISTS recovery_runs (recovery_id TEXT PRIMARY KEY, durable TEXT NOT NULL, manifest_sequence INTEGER NOT NULL, start_sequence INTEGER NOT NULL, restored_job_count INTEGER NOT NULL DEFAULT -1, replay_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, error TEXT NOT NULL DEFAULT '', started_at INTEGER NOT NULL, completed_at INTEGER)`,
		`CREATE TABLE IF NOT EXISTS recovery_jobs (recovery_id TEXT NOT NULL REFERENCES recovery_runs(recovery_id), job_id INTEGER NOT NULL, semantic_key TEXT NOT NULL, repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL, prior_status TEXT NOT NULL, attempts INTEGER NOT NULL, first_launch_attempt_at INTEGER, PRIMARY KEY(recovery_id,job_id))`,
		`CREATE TABLE IF NOT EXISTS recovery_reconciliations (recovery_id TEXT NOT NULL REFERENCES recovery_runs(recovery_id), job_id INTEGER NOT NULL, broker_run_id TEXT NOT NULL, prior_status TEXT NOT NULL, broker_status TEXT NOT NULL, reconciled_status TEXT NOT NULL, reconciled_at INTEGER NOT NULL, PRIMARY KEY(recovery_id,job_id))`,
		`CREATE TABLE IF NOT EXISTS recovery_replayed_messages (recovery_id TEXT NOT NULL REFERENCES recovery_runs(recovery_id), stream_sequence INTEGER NOT NULL, PRIMARY KEY(recovery_id,stream_sequence))`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize sqlite: %w", err)
		}
	}
	for _, migration := range []struct{ table, column, definition string }{
		{"jobs", "broker_run_id", `broker_run_id TEXT NOT NULL DEFAULT ''`},
		{"jobs", "first_launch_attempt_at", `first_launch_attempt_at INTEGER`},
		{"deliveries", "stream_sequence", `stream_sequence INTEGER NOT NULL DEFAULT 0`},
	} {
		if err := ensureColumn(db, migration.table, migration.column, migration.definition); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate sqlite: %w", err)
		}
	}
	for _, statement := range []string{
		`UPDATE jobs SET status='pending_launch' WHERE status='pending'`,
		`UPDATE jobs SET status='launch_retry' WHERE status IN ('retry','running')`,
		`UPDATE jobs SET status='launched' WHERE status='succeeded' AND broker_run_id<>''`,
		`UPDATE jobs SET status='failed' WHERE status IN ('succeeded','terminal')`,
		`CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status, due_at)`,
		`UPDATE dispatcher_metadata SET value=MAX(value,(SELECT COALESCE(MAX(stream_sequence),0) FROM deliveries)) WHERE key='last_persisted_jetstream_sequence'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize lifecycle: %w", err)
		}
	}
	if err := workledger.Migrate(context.Background(), db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func ensureColumn(db *sql.DB, table, column, definition string) error {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			rows.Close()
			return err
		}
		found = found || name == column
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + definition)
	return err
}

func (s *Store) Close() error                    { return s.db.Close() }
func (s *Store) Ready(ctx context.Context) error { return s.db.PingContext(ctx) }

// Record persists the delivery's JetStream position and semantically unique job
// atomically. The sequence is the recovery checkpoint: a restored consumer can
// replay starting at RecoverySequence without losing accepted work.
func (s *Store) Record(ctx context.Context, deliveryID, outcome string, streamSequence uint64, candidate *Candidate, now time.Time) error {
	return s.record(ctx, "", deliveryID, outcome, streamSequence, candidate, now)
}

// RecordRecovery persists replay evidence in the same transaction as the
// delivery checkpoint. A crash can therefore redeliver a message, but cannot
// make the recorded replay count disagree with durable SQLite state.
func (s *Store) RecordRecovery(ctx context.Context, recoveryID, deliveryID, outcome string, streamSequence uint64, candidate *Candidate, now time.Time) error {
	if recoveryID == "" {
		return errors.New("recovery id is required")
	}
	return s.record(ctx, recoveryID, deliveryID, outcome, streamSequence, candidate, now)
}

func (s *Store) record(ctx context.Context, recoveryID, deliveryID, outcome string, streamSequence uint64, candidate *Candidate, now time.Time) error {
	if streamSequence == 0 {
		return errors.New("positive JetStream stream sequence is required")
	}
	if candidate != nil && (deliveryID == "" || candidate.DeliveryID != deliveryID) {
		return errors.New("selected job requires matching nonempty delivery id")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if recoveryID != "" {
		result, err := tx.ExecContext(ctx, `INSERT INTO recovery_replayed_messages(recovery_id,stream_sequence) SELECT ?,? WHERE EXISTS (SELECT 1 FROM recovery_runs WHERE recovery_id=? AND status=?) ON CONFLICT DO NOTHING`, recoveryID, streamSequence, recoveryID, RecoveryIncomplete)
		if err != nil {
			return err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			var exists int
			if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM recovery_runs WHERE recovery_id=? AND status=?`, recoveryID, RecoveryIncomplete).Scan(&exists); err != nil {
				return err
			}
			if exists != 1 {
				return errors.New("recovery is not incomplete")
			}
		}
	}
	semantic := ""
	if candidate != nil {
		semantic = candidate.SemanticKey()
	}
	if deliveryID != "" {
		result, err := tx.ExecContext(ctx, `INSERT INTO deliveries(delivery_id,outcome,semantic_key,stream_sequence,recorded_at) VALUES(?,?,?,?,?) ON CONFLICT(delivery_id) DO NOTHING`, deliveryID, outcome, nullable(semantic), streamSequence, now.UnixMilli())
		if err != nil {
			return err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE dispatcher_metadata SET value=MAX(value,?) WHERE key=?`, streamSequence, checkpointKey); err != nil {
				return err
			}
			return tx.Commit()
		}
	}
	if candidate != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO jobs(semantic_key,repository,issue_number,source_delivery_id,status,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(semantic_key) DO NOTHING`, semantic, candidate.Repository, candidate.IssueNumber, candidate.DeliveryID, StatePendingLaunch, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatcher_metadata SET value=MAX(value,?) WHERE key=?`, streamSequence, checkpointKey); err != nil {
		return err
	}
	return tx.Commit()
}

type RecoveryRun struct {
	ID               string `json:"recovery_id"`
	Durable          string `json:"durable"`
	ManifestSequence uint64 `json:"manifest_last_persisted_jetstream_sequence"`
	StartSequence    uint64 `json:"start_sequence"`
	ReplayCount      uint64 `json:"replay_count"`
	Status           string `json:"status"`
	Error            string `json:"error,omitempty"`
}

type Reconciliation struct {
	JobID            int64  `json:"job_id"`
	BrokerRunID      string `json:"broker_run_id"`
	PriorStatus      string `json:"prior_status"`
	BrokerStatus     string `json:"broker_status"`
	ReconciledStatus string `json:"reconciled_status"`
}

func (s *Store) BeginRecovery(ctx context.Context, id, durable string, manifestSequence uint64, now time.Time) error {
	if id == "" || durable == "" {
		return errors.New("recovery id and durable are required")
	}
	checkpoint, err := s.recoveryCheckpoint(ctx)
	if err != nil {
		return err
	}
	if checkpoint != manifestSequence {
		return fmt.Errorf("manifest sequence %d does not match restored SQLite checkpoint %d", manifestSequence, checkpoint)
	}
	start := manifestSequence + 1
	result, err := s.db.ExecContext(ctx, `INSERT INTO recovery_runs(recovery_id,durable,manifest_sequence,start_sequence,status,started_at) VALUES(?,?,?,?,?,?) ON CONFLICT(recovery_id) DO NOTHING`, id, durable, manifestSequence, start, RecoveryIncomplete, now.UnixMilli())
	if err != nil {
		return err
	}
	inserted, _ := result.RowsAffected()
	var run RecoveryRun
	if err := s.db.QueryRowContext(ctx, `SELECT recovery_id,durable,manifest_sequence,start_sequence,replay_count,status,error FROM recovery_runs WHERE recovery_id=?`, id).Scan(&run.ID, &run.Durable, &run.ManifestSequence, &run.StartSequence, &run.ReplayCount, &run.Status, &run.Error); err != nil {
		return err
	}
	if run.Durable != durable || run.ManifestSequence != manifestSequence || run.StartSequence != start || run.Status == RecoveryCompleted && inserted == 0 {
		return errors.New("recovery id already exists with different or completed parameters")
	}
	var other int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM recovery_runs WHERE status=? AND recovery_id<>?`, RecoveryIncomplete, id).Scan(&other); err != nil {
		return err
	}
	if other != 0 {
		return errors.New("another recovery is incomplete")
	}
	return nil
}

func (s *Store) FailRecovery(ctx context.Context, id string, failure error) error {
	message := "recovery failed"
	if failure != nil {
		message = failure.Error()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE recovery_runs SET error=? WHERE recovery_id=? AND status=?`, message, id, RecoveryIncomplete)
	return err
}

func (s *Store) RecoveryJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,status,attempts,first_launch_attempt_at FROM jobs WHERE status IN (?,?,?) ORDER BY id`, StatePendingLaunch, StateLaunchRetry, StateLaunched)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		var job Job
		var first sql.NullInt64
		if err := rows.Scan(&job.ID, &job.SemanticKey, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.BrokerRunID, &job.Status, &job.Attempts, &first); err != nil {
			return nil, err
		}
		if first.Valid {
			job.FirstAttemptAt = time.UnixMilli(first.Int64)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// PrepareRecoveryJobs freezes the restored nonterminal set before replay can
// add new jobs. It is resumable after partial reconciliation.
func (s *Store) PrepareRecoveryJobs(ctx context.Context, recoveryID string) ([]Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var expected int
	if err := tx.QueryRowContext(ctx, `SELECT restored_job_count FROM recovery_runs WHERE recovery_id=? AND status=?`, recoveryID, RecoveryIncomplete).Scan(&expected); err != nil {
		return nil, err
	}
	if expected < 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_jobs(recovery_id,job_id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,prior_status,attempts,first_launch_attempt_at) SELECT ?,id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,status,attempts,first_launch_attempt_at FROM jobs WHERE status IN (?,?,?)`, recoveryID, StatePendingLaunch, StateLaunchRetry, StateLaunched); err != nil {
			return nil, err
		}
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM recovery_jobs WHERE recovery_id=?`, recoveryID).Scan(&expected); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE recovery_runs SET restored_job_count=? WHERE recovery_id=? AND restored_job_count=-1`, expected, recoveryID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT job_id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,prior_status,attempts,first_launch_attempt_at FROM recovery_jobs WHERE recovery_id=? ORDER BY job_id`, recoveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0, expected)
	for rows.Next() {
		var job Job
		var first sql.NullInt64
		if err := rows.Scan(&job.ID, &job.SemanticKey, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.BrokerRunID, &job.Status, &job.Attempts, &first); err != nil {
			return nil, err
		}
		if first.Valid {
			job.FirstAttemptAt = time.UnixMilli(first.Int64)
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *Store) RecordReconciliation(ctx context.Context, recoveryID string, job Job, brokerStatus, reconciledStatus string, now time.Time) error {
	if job.Status != StateLaunched || job.BrokerRunID == "" {
		return fmt.Errorf("restored nonterminal job %d has no reconcilable broker run", job.ID)
	}
	if !validLifecycleState(reconciledStatus) || reconciledStatus == StatePendingLaunch || reconciledStatus == StateLaunchRetry {
		return fmt.Errorf("invalid reconciled status %q", reconciledStatus)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var existing int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM recovery_reconciliations WHERE recovery_id=? AND job_id=?`, recoveryID, job.ID).Scan(&existing); err != nil {
		return err
	}
	if existing == 1 {
		return tx.Commit()
	}
	due := now
	if reconciledStatus == StateLaunched {
		due = now.Add(StatusPollInterval)
	}
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error='',updated_at=? WHERE id=? AND status=? AND broker_run_id=?`, reconciledStatus, due.UnixMilli(), now.UnixMilli(), job.ID, job.Status, job.BrokerRunID)
	if err := expectOne(result, err, "reconcile job"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO recovery_reconciliations(recovery_id,job_id,broker_run_id,prior_status,broker_status,reconciled_status,reconciled_at) VALUES(?,?,?,?,?,?,?)`, recoveryID, job.ID, job.BrokerRunID, job.Status, brokerStatus, reconciledStatus, now.UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CompleteRecovery(ctx context.Context, id string, expectedReconciliations int, now time.Time) (RecoveryRun, []Reconciliation, error) {
	var reconciled int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM recovery_reconciliations WHERE recovery_id=?`, id).Scan(&reconciled); err != nil {
		return RecoveryRun{}, nil, err
	}
	if reconciled != expectedReconciliations {
		return RecoveryRun{}, nil, fmt.Errorf("recorded %d reconciliations for %d restored nonterminal jobs", reconciled, expectedReconciliations)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE recovery_runs SET replay_count=(SELECT count(*) FROM recovery_replayed_messages WHERE recovery_id=?),status=?,error='',completed_at=? WHERE recovery_id=? AND status=?`, id, RecoveryCompleted, now.UnixMilli(), id, RecoveryIncomplete)
	if err := expectOne(result, err, "complete recovery"); err != nil {
		return RecoveryRun{}, nil, err
	}
	return s.RecoveryEvidence(ctx, id)
}

func (s *Store) RecoveryEvidence(ctx context.Context, id string) (RecoveryRun, []Reconciliation, error) {
	var run RecoveryRun
	if err := s.db.QueryRowContext(ctx, `SELECT recovery_id,durable,manifest_sequence,start_sequence,replay_count,status,error FROM recovery_runs WHERE recovery_id=?`, id).Scan(&run.ID, &run.Durable, &run.ManifestSequence, &run.StartSequence, &run.ReplayCount, &run.Status, &run.Error); err != nil {
		return RecoveryRun{}, nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT job_id,broker_run_id,prior_status,broker_status,reconciled_status FROM recovery_reconciliations WHERE recovery_id=? ORDER BY job_id`, id)
	if err != nil {
		return RecoveryRun{}, nil, err
	}
	defer rows.Close()
	var outcomes []Reconciliation
	for rows.Next() {
		var outcome Reconciliation
		if err := rows.Scan(&outcome.JobID, &outcome.BrokerRunID, &outcome.PriorStatus, &outcome.BrokerStatus, &outcome.ReconciledStatus); err != nil {
			return RecoveryRun{}, nil, err
		}
		outcomes = append(outcomes, outcome)
	}
	return run, outcomes, rows.Err()
}

func (s *Store) AssertRecoveryComplete(ctx context.Context, durable string, startSequence uint64) error {
	var incomplete int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM recovery_runs WHERE status=?`, RecoveryIncomplete).Scan(&incomplete); err != nil {
		return err
	}
	if incomplete != 0 {
		return errors.New("dispatcher recovery is incomplete; launches are blocked")
	}
	if startSequence == 0 {
		return nil
	}
	var completed int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM recovery_runs WHERE durable=? AND start_sequence=? AND status=?`, durable, startSequence, RecoveryCompleted).Scan(&completed); err != nil {
		return err
	}
	if completed != 1 {
		return errors.New("dispatcher recovery configuration has no matching completed recovery")
	}
	return nil
}

func (s *Store) recoveryCheckpoint(ctx context.Context) (uint64, error) {
	var checkpoint uint64
	err := s.db.QueryRowContext(ctx, `SELECT value FROM dispatcher_metadata WHERE key=?`, checkpointKey).Scan(&checkpoint)
	return checkpoint, err
}

func validLifecycleState(status string) bool {
	for _, state := range lifecycleStates {
		if status == state {
			return true
		}
	}
	return false
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// ClaimDue serializes the lifecycle by creation order. The oldest nonterminal
// job blocks every later job, including while a launch retry is not due.
func (s *Store) ClaimDue(ctx context.Context, now time.Time) (Work, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Work{}, false, err
	}
	defer tx.Rollback()
	var job Job
	var first sql.NullInt64
	var due int64
	err = tx.QueryRowContext(ctx, `SELECT id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,status,attempts,first_launch_attempt_at,due_at FROM jobs WHERE status IN (?,?,?) ORDER BY created_at,id LIMIT 1`, StatePendingLaunch, StateLaunchRetry, StateLaunched).
		Scan(&job.ID, &job.SemanticKey, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.BrokerRunID, &job.Status, &job.Attempts, &first, &due)
	if errors.Is(err, sql.ErrNoRows) {
		return Work{}, false, nil
	}
	if err != nil {
		return Work{}, false, err
	}
	if first.Valid {
		job.FirstAttemptAt = time.UnixMilli(first.Int64)
	}
	if due > now.UnixMilli() {
		return Work{}, false, nil
	}
	if job.Status == StateLaunched {
		return Work{Kind: WorkStatus, Job: job}, true, tx.Commit()
	}
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET attempts=attempts+1,first_launch_attempt_at=COALESCE(first_launch_attempt_at,?),updated_at=? WHERE id=? AND status IN (?,?)`, now.UnixMilli(), now.UnixMilli(), job.ID, StatePendingLaunch, StateLaunchRetry)
	if err != nil {
		return Work{}, false, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Work{}, false, nil
	}
	job.Attempts++
	if first.Valid {
		job.FirstAttemptAt = time.UnixMilli(first.Int64)
	} else {
		job.FirstAttemptAt = now
	}
	return Work{Kind: WorkLaunch, Job: job}, true, tx.Commit()
}

func (s *Store) MarkLaunched(ctx context.Context, id int64, brokerRunID string, due, now time.Time) error {
	if brokerRunID == "" {
		return errors.New("broker run id is required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,broker_run_id=?,due_at=?,updated_at=?,last_error='' WHERE id=? AND status IN (?,?)`, StateLaunched, brokerRunID, due.UnixMilli(), now.UnixMilli(), id, StatePendingLaunch, StateLaunchRetry)
	return expectOne(result, err, "mark launched")
}

func (s *Store) MarkLaunchFailure(ctx context.Context, id int64, retry bool, due time.Time, message string, now time.Time) error {
	status := StateFailed
	if retry {
		status = StateLaunchRetry
	}
	result, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=? AND status IN (?,?)`, status, due.UnixMilli(), message, now.UnixMilli(), id, StatePendingLaunch, StateLaunchRetry)
	return expectOne(result, err, "mark launch failure")
}

func (s *Store) MarkStatus(ctx context.Context, id int64, status string, due time.Time, message string, now time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=? AND status=?`, status, due.UnixMilli(), message, now.UnixMilli(), id, StateLaunched)
	return expectOne(result, err, "mark status")
}

func expectOne(result sql.Result, err error, operation string) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("%s: expected one lifecycle row, updated %d", operation, rows)
	}
	return nil
}

func (s *Store) RecoverySequence(ctx context.Context) (uint64, error) {
	var checkpoint uint64
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM dispatcher_metadata WHERE key=?`, checkpointKey).Scan(&checkpoint); err != nil {
		return 0, err
	}
	return checkpoint + 1, nil
}

func (s *Store) RecoveryMetadata(ctx context.Context) (schemaVersion int, checkpoint, startSequence uint64, err error) {
	if err = s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&schemaVersion); err != nil {
		return
	}
	if err = s.db.QueryRowContext(ctx, `SELECT value FROM dispatcher_metadata WHERE key=?`, checkpointKey).Scan(&checkpoint); err != nil {
		return
	}
	startSequence = checkpoint + 1
	return
}

type StoreStats struct {
	Counts    map[string]float64
	OldestAge time.Duration
}

func (s *Store) Stats(ctx context.Context, now time.Time) (StoreStats, error) {
	stats := StoreStats{Counts: make(map[string]float64, len(lifecycleStates))}
	for _, state := range lifecycleStates {
		stats.Counts[state] = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT status,count(*) FROM jobs GROUP BY status`)
	if err != nil {
		return stats, err
	}
	for rows.Next() {
		var state string
		var count float64
		if err := rows.Scan(&state, &count); err != nil {
			rows.Close()
			return stats, err
		}
		if _, ok := stats.Counts[state]; ok {
			stats.Counts[state] = count
		}
	}
	if err := rows.Close(); err != nil {
		return stats, err
	}
	var oldest sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(created_at) FROM jobs WHERE status IN (?,?,?)`, StatePendingLaunch, StateLaunchRetry, StateLaunched).Scan(&oldest); err != nil {
		return stats, err
	}
	if oldest.Valid && now.After(time.UnixMilli(oldest.Int64)) {
		stats.OldestAge = now.Sub(time.UnixMilli(oldest.Int64))
	}
	return stats, nil
}

func (s *Store) Counts(ctx context.Context) (deliveries, jobs int, err error) {
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries`).Scan(&deliveries); err != nil {
		return
	}
	err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs)
	return
}
