package pushscan

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct{ db *sql.DB }

var ErrFingerprintConflict = errors.New("fingerprint registration conflicts with existing metadata")

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, statement := range []string{
		`PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS push_scan_receipts (delivery_id TEXT PRIMARY KEY, stream_sequence INTEGER NOT NULL, repository TEXT NOT NULL, ref TEXT NOT NULL, before_sha TEXT NOT NULL, after_sha TEXT NOT NULL, head_timestamp INTEGER, source_timestamp INTEGER, received_at INTEGER NOT NULL, receipt_at INTEGER NOT NULL, consumer_observed_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS push_scan_results (delivery_id TEXT PRIMARY KEY REFERENCES push_scan_receipts(delivery_id), finding_id TEXT NOT NULL, status TEXT NOT NULL, severity TEXT NOT NULL, reason_code TEXT NOT NULL, fingerprint_id TEXT NOT NULL, profile TEXT NOT NULL, logical_session_id TEXT NOT NULL, session_lineage_id TEXT NOT NULL, worker_id TEXT NOT NULL, worker_storage_lineage_id TEXT NOT NULL, worker_fence_epoch INTEGER NOT NULL, profile_generation INTEGER NOT NULL, scan_started_at INTEGER NOT NULL, material_completed_at INTEGER NOT NULL, finding_at INTEGER, response_requested_at INTEGER, response_last_attempt_at INTEGER, response_attempt_count INTEGER NOT NULL DEFAULT 0, halted_at INTEGER, fence_requested_at INTEGER, fenced_at INTEGER, fence_state TEXT NOT NULL, alert_state TEXT NOT NULL, alert_requested_at INTEGER, terminal_at INTEGER NOT NULL, slo_deadline_at INTEGER NOT NULL, slo_breached INTEGER NOT NULL, slo_state TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS push_scan_token_fingerprints (fingerprint_id TEXT PRIMARY KEY, fingerprint BLOB NOT NULL UNIQUE, profile TEXT NOT NULL, logical_session_id TEXT NOT NULL, session_lineage_id TEXT NOT NULL, worker_id TEXT NOT NULL, worker_storage_lineage_id TEXT NOT NULL, worker_fence_epoch INTEGER NOT NULL, profile_generation INTEGER NOT NULL, issued_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, retained_until INTEGER NOT NULL, state TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS push_scan_security_events (event_id TEXT PRIMARY KEY, delivery_id TEXT NOT NULL REFERENCES push_scan_receipts(delivery_id), payload_json TEXT NOT NULL, published_at INTEGER)`,
	} {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("initialize push scanner store: %w", err)
		}
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error                    { return s.db.Close() }
func (s *Store) Ready(ctx context.Context) error { return s.db.PingContext(ctx) }

func (s *Store) RegisterFingerprint(ctx context.Context, fingerprint []byte, registration FingerprintRegistration) error {
	if len(fingerprint) != sha256.Size || registration.FingerprintID == "" || registration.Profile == "" || registration.LogicalSessionID == "" || registration.SessionLineageID == "" || registration.WorkerID == "" || registration.WorkerStorageLineage == "" || registration.WorkerFenceEpoch <= 0 || registration.ProfileGeneration <= 0 || !registration.IssuedAt.Before(registration.ExpiresAt) || registration.RetainedUntil.Before(registration.ExpiresAt) || (registration.State != "active" && registration.State != "expired" && registration.State != "revoked") {
		return errors.New("fingerprint registration is incomplete")
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO push_scan_token_fingerprints(fingerprint_id,fingerprint,profile,logical_session_id,session_lineage_id,worker_id,worker_storage_lineage_id,worker_fence_epoch,profile_generation,issued_at,expires_at,retained_until,state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING`, registration.FingerprintID, append([]byte(nil), fingerprint...), registration.Profile, registration.LogicalSessionID, registration.SessionLineageID, registration.WorkerID, registration.WorkerStorageLineage, registration.WorkerFenceEpoch, registration.ProfileGeneration, registration.IssuedAt.UnixMilli(), registration.ExpiresAt.UnixMilli(), registration.RetainedUntil.UnixMilli(), registration.State)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil || inserted == 1 {
		return err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT fingerprint_id,fingerprint,profile,logical_session_id,session_lineage_id,worker_id,worker_storage_lineage_id,worker_fence_epoch,profile_generation,issued_at,expires_at,retained_until,state FROM push_scan_token_fingerprints WHERE fingerprint_id=? OR fingerprint=?`, registration.FingerprintID, fingerprint)
	if err != nil {
		return err
	}
	defer rows.Close()
	matches := 0
	for rows.Next() {
		var id, profile, logical, session, worker, storage, state string
		var stored []byte
		var fence, generation, issued, expires, retained int64
		if err := rows.Scan(&id, &stored, &profile, &logical, &session, &worker, &storage, &fence, &generation, &issued, &expires, &retained, &state); err != nil {
			return err
		}
		if id != registration.FingerprintID || !hmac.Equal(stored, fingerprint) || profile != registration.Profile || logical != registration.LogicalSessionID || session != registration.SessionLineageID || worker != registration.WorkerID || storage != registration.WorkerStorageLineage || fence != registration.WorkerFenceEpoch || generation != registration.ProfileGeneration || issued != registration.IssuedAt.UnixMilli() || expires != registration.ExpiresAt.UnixMilli() || retained != registration.RetainedUntil.UnixMilli() || state != registration.State {
			return ErrFingerprintConflict
		}
		matches++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if matches != 1 {
		return ErrFingerprintConflict
	}
	return nil
}

func (s *Store) TransitionFingerprintState(ctx context.Context, fingerprintID, state string) error {
	if fingerprintID == "" || (state != "expired" && state != "revoked") {
		return errors.New("fingerprint state transition is invalid")
	}
	allowed := "('active','expired')"
	if state == "revoked" {
		allowed = "('active','expired','revoked')"
	}
	result, err := s.db.ExecContext(ctx, `UPDATE push_scan_token_fingerprints SET state=? WHERE fingerprint_id=? AND state IN `+allowed, state, fingerprintID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("fingerprint state transition target is unavailable")
	}
	return nil
}

func (s *Store) MatchCandidate(ctx context.Context, key []byte, candidate string, now time.Time) (Attribution, bool, error) {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(candidate))
	var result Attribution
	var issued, expires int64
	err := s.db.QueryRowContext(ctx, `SELECT fingerprint_id,profile,logical_session_id,session_lineage_id,worker_id,worker_storage_lineage_id,worker_fence_epoch,profile_generation,issued_at,expires_at,state FROM push_scan_token_fingerprints WHERE fingerprint=? AND retained_until>=?`, mac.Sum(nil), now.UnixMilli()).Scan(&result.FingerprintID, &result.Profile, &result.LogicalSessionID, &result.SessionLineageID, &result.WorkerID, &result.WorkerStorageLineage, &result.WorkerFenceEpoch, &result.ProfileGeneration, &issued, &expires, &result.State)
	if errors.Is(err, sql.ErrNoRows) {
		return Attribution{}, false, nil
	}
	if err != nil {
		return Attribution{}, false, err
	}
	result.IssuedAt, result.ExpiresAt = time.UnixMilli(issued).UTC(), time.UnixMilli(expires).UTC()
	if result.State == "active" && !now.Before(result.ExpiresAt) {
		result.State = "expired"
	}
	return result, true, nil
}

func (s *Store) PruneFingerprints(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM push_scan_token_fingerprints WHERE retained_until<?`, now.UnixMilli())
	return err
}

func (s *Store) RecordReceipt(ctx context.Context, identity PushIdentity, consumerObservedAt time.Time) (time.Time, time.Time, bool, error) {
	var head any
	if identity.HeadTime != nil {
		head = identity.HeadTime.UnixMilli()
	}
	var source any
	if identity.SourceTime != nil {
		source = identity.SourceTime.UnixMilli()
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO push_scan_receipts(delivery_id,stream_sequence,repository,ref,before_sha,after_sha,head_timestamp,source_timestamp,received_at,receipt_at,consumer_observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(delivery_id) DO NOTHING`, identity.DeliveryID, identity.StreamSequence, identity.Repository, identity.Ref, identity.Before, identity.After, head, source, identity.ReceivedAt.UnixMilli(), identity.ReceivedAt.UnixMilli(), consumerObservedAt.UnixMilli())
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	var recorded, observed, streamSequence, receivedAt int64
	var repository, ref, before, after string
	var recordedHead, recordedSource sql.NullInt64
	if err := s.db.QueryRowContext(ctx, `SELECT stream_sequence,repository,ref,before_sha,after_sha,head_timestamp,source_timestamp,received_at,receipt_at,consumer_observed_at FROM push_scan_receipts WHERE delivery_id=?`, identity.DeliveryID).Scan(&streamSequence, &repository, &ref, &before, &after, &recordedHead, &recordedSource, &receivedAt, &recorded, &observed); err != nil {
		return time.Time{}, time.Time{}, false, err
	}
	if inserted == 0 && (repository != identity.Repository || ref != identity.Ref || before != identity.Before || after != identity.After || !sameOptionalTime(recordedHead, identity.HeadTime) || !sameOptionalTime(recordedSource, identity.SourceTime)) {
		return time.Time{}, time.Time{}, false, errors.New("delivery replay identity conflict")
	}
	return time.UnixMilli(recorded).UTC(), time.UnixMilli(observed).UTC(), inserted == 0, nil
}

func sameOptionalTime(recorded sql.NullInt64, value *time.Time) bool {
	if value == nil {
		return !recorded.Valid
	}
	return recorded.Valid && recorded.Int64 == value.UnixMilli()
}

func (s *Store) Result(ctx context.Context, deliveryID string) (Result, bool, error) {
	var result Result
	var receipt, started, material, finding, requested, halted, fenceRequested, fenced, alertRequested, terminal, deadline sql.NullInt64
	var breached int
	err := s.db.QueryRowContext(ctx, `SELECT r.delivery_id,r.finding_id,r.status,r.severity,r.reason_code,r.fingerprint_id,r.profile,r.logical_session_id,r.session_lineage_id,r.worker_id,r.worker_storage_lineage_id,r.worker_fence_epoch,r.profile_generation,d.receipt_at,r.scan_started_at,r.material_completed_at,r.finding_at,r.response_requested_at,r.halted_at,r.fence_requested_at,r.fenced_at,r.fence_state,r.alert_state,r.alert_requested_at,r.terminal_at,r.slo_deadline_at,r.slo_breached,r.slo_state FROM push_scan_results r JOIN push_scan_receipts d USING(delivery_id) WHERE r.delivery_id=?`, deliveryID).Scan(&result.DeliveryID, &result.FindingID, &result.Status, &result.Severity, &result.ReasonCode, &result.FingerprintID, &result.Profile, &result.LogicalSessionID, &result.SessionLineageID, &result.WorkerID, &result.WorkerStorageLineage, &result.WorkerFenceEpoch, &result.ProfileGeneration, &receipt, &started, &material, &finding, &requested, &halted, &fenceRequested, &fenced, &result.FenceState, &result.AlertState, &alertRequested, &terminal, &deadline, &breached, &result.SLOState)
	if errors.Is(err, sql.ErrNoRows) {
		return Result{}, false, nil
	}
	if err != nil {
		return Result{}, false, err
	}
	result.ReceiptAt, result.ScanStartedAt, result.MaterialCompletedAt = millisTime(receipt), millisTime(started), millisTime(material)
	result.FindingAt, result.ResponseRequestedAt, result.HaltedAt = millisTime(finding), millisTime(requested), millisTime(halted)
	result.FenceRequestedAt, result.FencedAt = millisTime(fenceRequested), millisTime(fenced)
	result.AlertRequestedAt = millisTime(alertRequested)
	result.TerminalAt, result.SLODeadline, result.SLOBreached = millisTime(terminal), millisTime(deadline), breached == 1
	return result, true, nil
}

func (s *Store) RecordResult(ctx context.Context, result Result, event *SecurityEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO push_scan_results(delivery_id,finding_id,status,severity,reason_code,fingerprint_id,profile,logical_session_id,session_lineage_id,worker_id,worker_storage_lineage_id,worker_fence_epoch,profile_generation,scan_started_at,material_completed_at,finding_at,response_requested_at,halted_at,fence_requested_at,fenced_at,fence_state,alert_state,alert_requested_at,terminal_at,slo_deadline_at,slo_breached,slo_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(delivery_id) DO NOTHING`, result.DeliveryID, result.FindingID, result.Status, result.Severity, result.ReasonCode, result.FingerprintID, result.Profile, result.LogicalSessionID, result.SessionLineageID, result.WorkerID, result.WorkerStorageLineage, result.WorkerFenceEpoch, result.ProfileGeneration, result.ScanStartedAt.UnixMilli(), result.MaterialCompletedAt.UnixMilli(), nullableMillis(result.FindingAt), nullableMillis(result.ResponseRequestedAt), nullableMillis(result.HaltedAt), nullableMillis(result.FenceRequestedAt), nullableMillis(result.FencedAt), result.FenceState, result.AlertState, nullableMillis(result.AlertRequestedAt), result.TerminalAt.UnixMilli(), result.SLODeadline.UnixMilli(), boolInt(result.SLOBreached), result.SLOState); err != nil {
		return err
	}
	if event != nil {
		payload, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO push_scan_security_events(event_id,delivery_id,payload_json) VALUES(?,?,?) ON CONFLICT(event_id) DO NOTHING`, event.EventID, event.DeliveryID, string(payload)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) CompleteResponse(ctx context.Context, result Result) error {
	updated, err := s.db.ExecContext(ctx, `UPDATE push_scan_results SET status=?,halted_at=?,fence_requested_at=?,fenced_at=?,fence_state=?,terminal_at=?,slo_breached=?,slo_state=? WHERE delivery_id=? AND status='response_pending'`, result.Status, nullableMillis(result.HaltedAt), nullableMillis(result.FenceRequestedAt), nullableMillis(result.FencedAt), result.FenceState, result.TerminalAt.UnixMilli(), boolInt(result.SLOBreached), result.SLOState, result.DeliveryID)
	if err != nil {
		return err
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("pending response state was not advanced")
	}
	return nil
}

func (s *Store) RecordResponseAttempt(ctx context.Context, deliveryID string, now time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE push_scan_results SET response_last_attempt_at=?,response_attempt_count=response_attempt_count+1 WHERE delivery_id=? AND status='response_pending'`, now.UnixMilli(), deliveryID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("pending response attempt state is unavailable")
	}
	return nil
}

func (s *Store) MarkOverdueResponses(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE push_scan_results SET slo_breached=1,slo_state='breached',terminal_at=MAX(terminal_at,?) WHERE status='response_pending' AND slo_state!='breached' AND slo_deadline_at<?`, now.UnixMilli(), now.UnixMilli())
	return err
}

func (s *Store) PendingResponseDeliveryIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT delivery_id FROM push_scan_results WHERE status='response_pending' ORDER BY COALESCE(response_last_attempt_at,response_requested_at),delivery_id LIMIT 64`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deliveryIDs []string
	for rows.Next() {
		var deliveryID string
		if err := rows.Scan(&deliveryID); err != nil {
			return nil, err
		}
		deliveryIDs = append(deliveryIDs, deliveryID)
	}
	return deliveryIDs, rows.Err()
}

func (s *Store) PushIdentity(ctx context.Context, deliveryID string) (PushIdentity, error) {
	var identity PushIdentity
	var streamSequence int64
	var head, source sql.NullInt64
	var received int64
	err := s.db.QueryRowContext(ctx, `SELECT delivery_id,stream_sequence,repository,ref,before_sha,after_sha,head_timestamp,source_timestamp,received_at FROM push_scan_receipts WHERE delivery_id=?`, deliveryID).Scan(&identity.DeliveryID, &streamSequence, &identity.Repository, &identity.Ref, &identity.Before, &identity.After, &head, &source, &received)
	if err != nil {
		return PushIdentity{}, err
	}
	identity.StreamSequence = uint64(streamSequence)
	identity.ReceivedAt = time.UnixMilli(received).UTC()
	if head.Valid {
		value := time.UnixMilli(head.Int64).UTC()
		identity.HeadTime = &value
	}
	if source.Valid {
		value := time.UnixMilli(source.Int64).UTC()
		identity.SourceTime = &value
	}
	return identity, nil
}

func (s *Store) PendingEvents(ctx context.Context) ([]SecurityEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT payload_json FROM push_scan_security_events WHERE published_at IS NULL ORDER BY event_id LIMIT 32`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []SecurityEvent
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var event SecurityEvent
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) MarkEventPublished(ctx context.Context, eventID string, now time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE push_scan_security_events SET published_at=? WHERE event_id=? AND published_at IS NULL`, now.UnixMilli(), eventID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM push_scan_security_events WHERE event_id=? AND published_at IS NOT NULL`, eventID).Scan(&count); err != nil || count != 1 {
			return errors.New("security event publication state missing")
		}
	}
	return nil
}

func nullableMillis(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UnixMilli()
}
func millisTime(value sql.NullInt64) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return time.UnixMilli(value.Int64).UTC()
}
func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
