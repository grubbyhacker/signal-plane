package dispatcher

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct{ db *sql.DB }

type Job struct {
	ID          int64
	SemanticKey string
	Repository  string
	IssueNumber int64
	DeliveryID  string
	Attempts    int
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS deliveries (delivery_id TEXT PRIMARY KEY, outcome TEXT NOT NULL, semantic_key TEXT, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, semantic_key TEXT NOT NULL UNIQUE, repository TEXT NOT NULL, issue_number INTEGER NOT NULL, source_delivery_id TEXT NOT NULL, broker_run_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due_at INTEGER NOT NULL, last_error TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`,
		`CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status, due_at)`,
		`UPDATE jobs SET status='retry' WHERE status='running'`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize sqlite: %w", err)
		}
	}
	if err := ensureBrokerRunIDColumn(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate sqlite: %w", err)
	}
	return &Store{db: db}, nil
}

func ensureBrokerRunIDColumn(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(jobs)`)
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
		if name == "broker_run_id" {
			found = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if found {
		return nil
	}
	_, err = db.Exec(`ALTER TABLE jobs ADD COLUMN broker_run_id TEXT NOT NULL DEFAULT ''`)
	return err
}

func (s *Store) Close() error                    { return s.db.Close() }
func (s *Store) Ready(ctx context.Context) error { return s.db.PingContext(ctx) }

// Record persists the delivery and, when selected, the semantically unique job
// in one transaction. Duplicate deliveries and duplicate issue jobs are safe.
func (s *Store) Record(ctx context.Context, deliveryID, outcome string, candidate *Candidate, now time.Time) error {
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
		var result sql.Result
		if result, err = tx.ExecContext(ctx, `INSERT INTO deliveries(delivery_id,outcome,semantic_key,recorded_at) VALUES(?,?,?,?) ON CONFLICT(delivery_id) DO NOTHING`, deliveryID, outcome, nullable(semantic), now.UnixMilli()); err != nil {
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
		_, err = tx.ExecContext(ctx, `INSERT INTO jobs(semantic_key,repository,issue_number,source_delivery_id,status,due_at,created_at,updated_at) VALUES(?,?,?,?, 'pending',?,?,?) ON CONFLICT(semantic_key) DO NOTHING`, semantic, candidate.Repository, candidate.IssueNumber, candidate.DeliveryID, now.UnixMilli(), now.UnixMilli(), now.UnixMilli())
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

func (s *Store) ClaimDue(ctx context.Context, now time.Time) (Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback()
	var job Job
	err = tx.QueryRowContext(ctx, `SELECT id,semantic_key,repository,issue_number,source_delivery_id,attempts FROM jobs WHERE status IN ('pending','retry') AND due_at<=? ORDER BY due_at,id LIMIT 1`, now.UnixMilli()).Scan(&job.ID, &job.SemanticKey, &job.Repository, &job.IssueNumber, &job.DeliveryID, &job.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status='running', attempts=attempts+1, updated_at=? WHERE id=? AND status IN ('pending','retry')`, now.UnixMilli(), job.ID)
	if err != nil {
		return Job{}, false, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return Job{}, false, nil
	}
	job.Attempts++
	if err := tx.Commit(); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func (s *Store) Complete(ctx context.Context, id int64, brokerRunID string, now time.Time) error {
	if brokerRunID == "" {
		return errors.New("broker run id is required to complete job")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status='succeeded',broker_run_id=?,updated_at=?,last_error='' WHERE id=?`, brokerRunID, now.UnixMilli(), id)
	return err
}
func (s *Store) Fail(ctx context.Context, id int64, terminal bool, due time.Time, message string, now time.Time) error {
	status := "retry"
	if terminal {
		status = "terminal"
	}
	_, err := s.db.ExecContext(ctx, `UPDATE jobs SET status=?,due_at=?,last_error=?,updated_at=? WHERE id=?`, status, due.UnixMilli(), message, now.UnixMilli(), id)
	return err
}

func (s *Store) Counts(ctx context.Context) (deliveries, jobs int, err error) {
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM deliveries`).Scan(&deliveries); err != nil {
		return
	}
	err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM jobs`).Scan(&jobs)
	return
}
