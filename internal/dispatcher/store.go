package dispatcher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const (
	StatePendingLaunch = "pending_launch"
	StateLaunchRetry   = "launch_retry"
	StateLaunched      = "launched"
	StateCompleted     = "completed"
	StateFailed        = "failed"
	StateTimedOut      = "timed_out"
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

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	statements := []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS deliveries (delivery_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, semantic_key TEXT, stream_sequence INTEGER NOT NULL DEFAULT 0, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, first_launch_attempt_at INTEGER, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
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
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize lifecycle: %w", err)
		}
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
			return tx.Commit()
		}
	}
	if candidate != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO jobs(semantic_key,repository,issue_number,source_delivery_id,status,due_at,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(semantic_key) DO NOTHING`, semantic, candidate.Repository, candidate.IssueNumber, candidate.DeliveryID, StatePendingLaunch, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// ClaimDue serializes the lifecycle. Any nonterminal broker run blocks all new
// launches, even while its next status poll is not due.
func (s *Store) ClaimDue(ctx context.Context, now time.Time) (Work, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Work{}, false, err
	}
	defer tx.Rollback()
	var job Job
	var first sql.NullInt64
	var due int64
	err = tx.QueryRowContext(ctx, `SELECT id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,status,attempts,first_launch_attempt_at,due_at FROM jobs WHERE status=? ORDER BY id LIMIT 1`, StateLaunched).
		Scan(&job.ID, &job.SemanticKey, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.BrokerRunID, &job.Status, &job.Attempts, &first, &due)
	if err == nil {
		if first.Valid {
			job.FirstAttemptAt = time.UnixMilli(first.Int64)
		}
		if due > now.UnixMilli() {
			return Work{}, false, nil
		}
		return Work{Kind: WorkStatus, Job: job}, true, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Work{}, false, err
	}
	err = tx.QueryRowContext(ctx, `SELECT id,semantic_key,repository,issue_number,source_delivery_id,broker_run_id,status,attempts,first_launch_attempt_at,due_at FROM jobs WHERE status IN (?,?) AND due_at<=? ORDER BY due_at,id LIMIT 1`, StatePendingLaunch, StateLaunchRetry, now.UnixMilli()).
		Scan(&job.ID, &job.SemanticKey, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.BrokerRunID, &job.Status, &job.Attempts, &first, &due)
	if errors.Is(err, sql.ErrNoRows) {
		return Work{}, false, nil
	}
	if err != nil {
		return Work{}, false, err
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
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,broker_run_id=?,due_at=?,updated_at=?,last_error='' WHERE id=?`, StateLaunched, brokerRunID, due.UnixMilli(), now.UnixMilli(), id)
	return err
}

func (s *Store) MarkLaunchFailure(ctx context.Context, id int64, retry bool, due time.Time, message string, now time.Time) error {
	status := StateFailed
	if retry {
		status = StateLaunchRetry
	}
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=?`, status, due.UnixMilli(), message, now.UnixMilli(), id)
	return err
}

func (s *Store) MarkStatus(ctx context.Context, id int64, status string, due time.Time, message string, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=?`, status, due.UnixMilli(), message, now.UnixMilli(), id)
	return err
}

func (s *Store) RecoverySequence(ctx context.Context) (uint64, error) {
	var sequence uint64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(stream_sequence),0)+1 FROM deliveries`).Scan(&sequence); err != nil {
		return 0, err
	}
	return sequence, nil
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
