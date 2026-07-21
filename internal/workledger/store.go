package workledger

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db    *sql.DB
	owned bool
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		db.Close()
		return nil, err
	}
	if version > SchemaVersion {
		db.Close()
		return nil, fmt.Errorf("sqlite schema version %d is newer than supported version %d", version, SchemaVersion)
	}
	for _, pragma := range []string{`PRAGMA journal_mode=WAL`, `PRAGMA foreign_keys=ON`, `PRAGMA busy_timeout=5000`} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := Migrate(context.Background(), db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, owned: true}, nil
}

func (store *Store) Close() error {
	if store.owned {
		return store.db.Close()
	}
	return nil
}

func (store *Store) Ready(ctx context.Context) error { return store.db.PingContext(ctx) }

func (store *Store) ActivateRoute(ctx context.Context, definition RouteDefinition, registry *Registry, now time.Time) (RouteSnapshot, error) {
	if registry == nil {
		return RouteSnapshot{}, errors.New("executor registry is required")
	}
	executor, err := registry.Resolve(definition.ExecutorID)
	if err != nil {
		return RouteSnapshot{}, err
	}
	var task *TaskDescriptor
	if executor.Descriptor().Kind == ExecutorAgentSession {
		descriptor, parameters, err := registry.ResolveTask(definition.Task, definition.Admission)
		if err != nil {
			return RouteSnapshot{}, err
		}
		definition.Task = &TaskSelection{Kind: descriptor.Kind, Parameters: parameters}
		task = &descriptor
	} else if definition.Task != nil {
		return RouteSnapshot{}, errors.New("only an agent_session executor may select a registered task kind")
	}
	return store.saveRoute(ctx, definition, executor.Descriptor(), task, now)
}

func (store *Store) saveRoute(ctx context.Context, definition RouteDefinition, executor ExecutorDescriptor, task *TaskDescriptor, now time.Time) (RouteSnapshot, error) {
	if err := definition.Validate(); err != nil {
		return RouteSnapshot{}, err
	}
	if definition.ExecutorID != executor.ID {
		return RouteSnapshot{}, errors.New("route executor does not match descriptor")
	}
	if err := executor.Validate(); err != nil {
		return RouteSnapshot{}, err
	}
	digest, err := activationDigest(definition, executor, task)
	if err != nil {
		return RouteSnapshot{}, err
	}
	encoded, err := json.Marshal(definition)
	if err != nil {
		return RouteSnapshot{}, err
	}
	snapshot := RouteSnapshot{ID: newID("route"), RouteID: definition.ID, SchemaVersion: definition.SchemaVersion, SemanticVersion: definition.SemanticVersion, Digest: digest, ExecutorID: executor.ID, ExecutorKind: executor.Kind, ExecutorVersion: executor.Version, Admission: definition.Admission, Concurrency: definition.Concurrency, Retry: definition.Retry, ActivatedAt: now.UTC()}
	if task != nil {
		snapshot.TaskKind, snapshot.TaskVersion = task.Kind, task.Version
		snapshot.CompletionContract, snapshot.VerifierID = task.CompletionContract, task.VerifierID
		snapshot.TaskContractDigest = task.ContractDigest
		snapshot.TaskParameters = append(json.RawMessage(nil), definition.Task.Parameters...)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return RouteSnapshot{}, err
	}
	defer tx.Rollback()
	var existing RouteSnapshot
	var activated int64
	err = tx.QueryRowContext(ctx, `SELECT id,route_id,schema_version,semantic_version,digest,executor_id,executor_kind,executor_version,task_kind,task_version,completion_contract,verifier_id,task_contract_digest,activated_at FROM route_snapshots WHERE route_id=? AND digest=? AND retired_at IS NULL`, definition.ID, digest).Scan(&existing.ID, &existing.RouteID, &existing.SchemaVersion, &existing.SemanticVersion, &existing.Digest, &existing.ExecutorID, &existing.ExecutorKind, &existing.ExecutorVersion, &existing.TaskKind, &existing.TaskVersion, &existing.CompletionContract, &existing.VerifierID, &existing.TaskContractDigest, &activated)
	if err == nil {
		existing.Admission, existing.Concurrency, existing.Retry = definition.Admission, definition.Concurrency, definition.Retry
		if definition.Task != nil {
			existing.TaskParameters = append(json.RawMessage(nil), definition.Task.Parameters...)
		}
		existing.ActivatedAt = time.UnixMilli(activated).UTC()
		if err := tx.Commit(); err != nil {
			return RouteSnapshot{}, err
		}
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return RouteSnapshot{}, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE route_snapshots SET retired_at=? WHERE route_id=? AND retired_at IS NULL`, millis(now), snapshot.RouteID); err == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO route_snapshots(id,route_id,schema_version,semantic_version,digest,executor_id,executor_kind,executor_version,task_kind,task_version,completion_contract,verifier_id,task_contract_digest,definition_json,activated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, snapshot.ID, snapshot.RouteID, snapshot.SchemaVersion, snapshot.SemanticVersion, snapshot.Digest, snapshot.ExecutorID, snapshot.ExecutorKind, snapshot.ExecutorVersion, snapshot.TaskKind, snapshot.TaskVersion, snapshot.CompletionContract, snapshot.VerifierID, snapshot.TaskContractDigest, string(encoded), millis(now))
	}
	if err != nil {
		return RouteSnapshot{}, fmt.Errorf("save route snapshot: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return RouteSnapshot{}, err
	}
	return snapshot, nil
}

func (store *Store) RetireRoute(ctx context.Context, snapshotID string, now time.Time) error {
	result, err := store.db.ExecContext(ctx, `UPDATE route_snapshots SET retired_at=? WHERE id=? AND retired_at IS NULL`, millis(now), snapshotID)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return errors.New("active route snapshot not found")
	}
	return nil
}

// MatchRoute selects the sole active route whose source-neutral admission
// policy accepts event. Zero or overlapping matches fail closed.
func (store *Store) MatchRoute(ctx context.Context, event Event) (RouteSnapshot, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT id,route_id,schema_version,semantic_version,digest,executor_id,executor_kind,executor_version,task_kind,task_version,completion_contract,verifier_id,task_contract_digest,definition_json,activated_at FROM route_snapshots WHERE retired_at IS NULL ORDER BY route_id,id`)
	if err != nil {
		return RouteSnapshot{}, err
	}
	defer rows.Close()
	matches := make([]RouteSnapshot, 0, 1)
	for rows.Next() {
		var snapshot RouteSnapshot
		var encoded string
		var activated int64
		if err := rows.Scan(&snapshot.ID, &snapshot.RouteID, &snapshot.SchemaVersion, &snapshot.SemanticVersion, &snapshot.Digest, &snapshot.ExecutorID, &snapshot.ExecutorKind, &snapshot.ExecutorVersion, &snapshot.TaskKind, &snapshot.TaskVersion, &snapshot.CompletionContract, &snapshot.VerifierID, &snapshot.TaskContractDigest, &encoded, &activated); err != nil {
			return RouteSnapshot{}, err
		}
		definition, err := DecodeRouteDefinition([]byte(encoded))
		if err != nil {
			return RouteSnapshot{}, fmt.Errorf("decode active route %q: %w", snapshot.RouteID, err)
		}
		if !definition.Admission.Matches(event) {
			continue
		}
		snapshot.Admission, snapshot.Concurrency, snapshot.Retry = definition.Admission, definition.Concurrency, definition.Retry
		if definition.Task != nil {
			snapshot.TaskParameters = append(json.RawMessage(nil), definition.Task.Parameters...)
		}
		snapshot.ActivatedAt = time.UnixMilli(activated).UTC()
		matches = append(matches, snapshot)
	}
	if err := rows.Err(); err != nil {
		return RouteSnapshot{}, err
	}
	if len(matches) == 0 {
		return RouteSnapshot{}, errors.New("no active route matches event")
	}
	if len(matches) != 1 {
		return RouteSnapshot{}, fmt.Errorf("ambiguous active routes match event: %d", len(matches))
	}
	return matches[0], nil
}

func (store *Store) DeliveryRecorded(ctx context.Context, event Event) (bool, error) {
	var digest string
	err := store.db.QueryRowContext(ctx, `SELECT event_digest FROM work_events WHERE source=? AND namespace=? AND source_delivery_id=?`, event.Source, event.Namespace, event.SourceDeliveryID).Scan(&digest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if digest != event.Digest() {
		return false, errors.New("source delivery id conflicts with different event content")
	}
	return true, nil
}
func (store *Store) RecordIngressFailure(ctx context.Context, event Event, classification string, attempts int, now time.Time) error {
	if attempts < 1 || classification == "" || len(classification) > 80 {
		return errors.New("invalid bounded ingress failure")
	}
	_, err := store.db.ExecContext(ctx, `INSERT INTO ingress_failures(source,namespace,source_delivery_id,event_digest,classification,attempts,recorded_at) VALUES(?,?,?,?,?,?,?) ON CONFLICT(source,namespace,source_delivery_id) DO UPDATE SET attempts=excluded.attempts,classification=excluded.classification,recorded_at=excluded.recorded_at`, event.Source, event.Namespace, event.SourceDeliveryID, event.Digest(), classification, attempts, millis(now))
	return err
}

func (store *Store) Admit(ctx context.Context, snapshotID string, event Event, now time.Time) (AdmissionResult, error) {
	return store.admit(ctx, snapshotID, event, nil, now)
}

func (store *Store) AdmitRelease(ctx context.Context, snapshotID string, event Event, operation ReleaseOperation, now time.Time) (AdmissionResult, error) {
	if err := operation.Validate(); err != nil {
		return AdmissionResult{}, err
	}
	return store.admit(ctx, snapshotID, event, &operation, now)
}

func (store *Store) admit(ctx context.Context, snapshotID string, event Event, operation *ReleaseOperation, now time.Time) (AdmissionResult, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return AdmissionResult{}, err
	}
	defer tx.Rollback()
	snapshot, definition, err := loadSnapshot(ctx, tx, snapshotID)
	if err != nil {
		return AdmissionResult{}, err
	}
	if snapshot.RetiredAt != nil || !definition.Admission.Matches(event) {
		return AdmissionResult{}, errors.New("event is not admitted by active route snapshot")
	}
	if event.SourceDeliveryID == "" || event.TransportStream == "" || event.TransportSequence == 0 || event.Namespace == "" || event.ObjectID == "" || event.SourceRevision == "" || event.PayloadDigest == "" || event.EvidenceRef == "" {
		return AdmissionResult{}, errors.New("event requires delivery, transport, namespace, object, and revision identity")
	}
	if event.HopCount < 0 || (event.ExpiresAt != nil && !event.ExpiresAt.After(now)) {
		return AdmissionResult{}, errors.New("event is expired or has an invalid hop count")
	}
	eventDigest := event.Digest()
	var duplicateWorkID, duplicateEventID, storedDigest string
	err = tx.QueryRowContext(ctx, `SELECT work_item_id,id,event_digest FROM work_events WHERE source=? AND namespace=? AND source_delivery_id=?`, event.Source, event.Namespace, event.SourceDeliveryID).Scan(&duplicateWorkID, &duplicateEventID, &storedDigest)
	if err == nil {
		if storedDigest != eventDigest {
			return AdmissionResult{}, errors.New("source delivery id conflicts with different event content")
		}
		if operation != nil {
			stored, loadErr := loadReleaseOperation(ctx, tx, duplicateWorkID)
			if loadErr != nil || stored != *operation {
				return AdmissionResult{}, errors.New("duplicate delivery conflicts with durable release operation")
			}
		}
		item, err := loadWorkItem(ctx, tx, duplicateWorkID)
		return AdmissionResult{WorkItem: item, EventID: duplicateEventID, Duplicate: true}, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return AdmissionResult{}, err
	}
	semanticKey := event.SemanticObjectKey()
	var item WorkItem
	err = tx.QueryRowContext(ctx, `SELECT id,route_snapshot_id,route_id,semantic_object_key,source,namespace,object_kind,object_id,source_revision,serialization_key,state,state_version,superseded_by_id,latest_executor_correlation,created_at,updated_at,terminal_at,next_attempt_at FROM work_items WHERE route_snapshot_id=? AND semantic_object_key=? AND source_revision=? ORDER BY created_at DESC LIMIT 1`, snapshot.ID, semanticKey, event.SourceRevision).Scan(workItemScan(&item)...)
	if errors.Is(err, sql.ErrNoRows) {
		item = WorkItem{ID: newID("work"), RouteSnapshotID: snapshot.ID, RouteID: definition.ID, SemanticObjectKey: semanticKey, Source: event.Source, Namespace: event.Namespace, ObjectKind: event.ObjectKind, ObjectID: event.ObjectID, SourceRevision: event.SourceRevision, SerializationKey: serializationKey(definition, event), State: StateAdmitted, StateVersion: 1, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
		_, err = tx.ExecContext(ctx, `INSERT INTO work_items(id,route_snapshot_id,route_id,semantic_object_key,source,namespace,object_kind,object_id,source_revision,serialization_key,task_evidence_digest,state,state_version,created_at,updated_at,next_attempt_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, item.ID, item.RouteSnapshotID, item.RouteID, item.SemanticObjectKey, item.Source, item.Namespace, item.ObjectKind, item.ObjectID, item.SourceRevision, item.SerializationKey, event.PayloadDigest, item.State, item.StateVersion, millis(now), millis(now), millis(now))
		if err != nil {
			return AdmissionResult{}, err
		}
		if definition.Concurrency.Supersede {
			_, err = tx.ExecContext(ctx, `UPDATE executor_attempts SET state=?,completed_at=? WHERE state IN (?,?,?) AND work_item_id IN (SELECT id FROM work_items WHERE route_id=? AND semantic_object_key=? AND id<>? AND state IN (?,?,?,?))`, AttemptSuperseded, millis(now), AttemptRunning, AttemptRecoverable, AttemptRetryScheduled, definition.ID, semanticKey, item.ID, StateObserved, StateAdmitted, StateActive, StateWaiting)
			if err == nil {
				_, err = tx.ExecContext(ctx, `DELETE FROM serialization_leases WHERE work_item_id IN (SELECT id FROM work_items WHERE route_id=? AND semantic_object_key=? AND id<>? AND state IN (?,?,?,?))`, definition.ID, semanticKey, item.ID, StateObserved, StateAdmitted, StateActive, StateWaiting)
			}
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,superseded_by_id=?,updated_at=?,terminal_at=?,next_attempt_at=NULL WHERE route_id=? AND semantic_object_key=? AND id<>? AND state IN (?,?,?,?)`, StateSuperseded, item.ID, millis(now), millis(now), definition.ID, semanticKey, item.ID, StateObserved, StateAdmitted, StateActive, StateWaiting)
			}
			if err != nil {
				return AdmissionResult{}, err
			}
		}
	} else if err != nil {
		return AdmissionResult{}, err
	}
	if operation != nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO release_operations(work_item_id,repository,repository_id,installation_id,release_id,tag,published_at,target_commitish,commit_sha,asset_id,asset_name,asset_size,asset_content_type,provider_digest,computed_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(work_item_id) DO NOTHING`, item.ID, operation.Repository, operation.RepositoryID, operation.InstallationID, operation.ReleaseID, operation.Tag, operation.PublishedAt, operation.TargetCommitish, operation.CommitSHA, operation.AssetID, operation.AssetName, operation.AssetSize, operation.AssetContentType, operation.ProviderDigest, operation.ComputedDigest)
		if err != nil {
			return AdmissionResult{}, err
		}
		stored, loadErr := loadReleaseOperation(ctx, tx, item.ID)
		if loadErr != nil || stored != *operation {
			return AdmissionResult{}, errors.New("work item conflicts with durable release operation")
		}
	}
	eventID := newID("event")
	_, err = tx.ExecContext(ctx, `INSERT INTO work_events(id,work_item_id,signal_id,source_delivery_id,event_digest,transport_stream,transport_sequence,source,namespace,object_kind,object_id,event_kind,action,actor_class,source_revision,correlation_id,causation_id,root_work_item_id,parent_work_item_id,originating_session,originating_turn,hop_count,expires_at,payload_digest,evidence_ref,admission_outcome,received_at,recorded_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, eventID, item.ID, event.SignalID, event.SourceDeliveryID, eventDigest, event.TransportStream, event.TransportSequence, event.Source, event.Namespace, event.ObjectKind, event.ObjectID, event.EventKind, event.Action, event.ActorClass, event.SourceRevision, event.CorrelationID, event.CausationID, event.RootWorkItemID, event.ParentWorkItemID, event.OriginatingSession, event.OriginatingTurn, event.HopCount, optionalMillis(event.ExpiresAt), event.PayloadDigest, event.EvidenceRef, "admitted", millis(event.ReceivedAt), millis(now))
	if err != nil {
		return AdmissionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return AdmissionResult{}, err
	}
	return AdmissionResult{WorkItem: item, EventID: eventID}, nil
}

func (store *Store) ReleaseOperation(ctx context.Context, workItemID string) (ReleaseOperation, error) {
	return loadReleaseOperation(ctx, store.db, workItemID)
}

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadReleaseOperation(ctx context.Context, query rowQuerier, workItemID string) (ReleaseOperation, error) {
	var value ReleaseOperation
	err := query.QueryRowContext(ctx, `SELECT repository,repository_id,installation_id,release_id,tag,published_at,target_commitish,commit_sha,asset_id,asset_name,asset_size,asset_content_type,provider_digest,computed_digest FROM release_operations WHERE work_item_id=?`, workItemID).Scan(&value.Repository, &value.RepositoryID, &value.InstallationID, &value.ReleaseID, &value.Tag, &value.PublishedAt, &value.TargetCommitish, &value.CommitSHA, &value.AssetID, &value.AssetName, &value.AssetSize, &value.AssetContentType, &value.ProviderDigest, &value.ComputedDigest)
	return value, err
}

func (store *Store) ContentResult(ctx context.Context, digest string) (string, string, bool, error) {
	var external, result string
	err := store.db.QueryRowContext(ctx, `SELECT external_correlation,result_digest FROM content_results WHERE computed_digest=?`, digest).Scan(&external, &result)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	return external, result, err == nil, err
}

func (store *Store) RecordContentResult(ctx context.Context, digest, external, result string, now time.Time) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO content_results(computed_digest,external_correlation,result_digest,recorded_at) VALUES(?,?,?,?) ON CONFLICT(computed_digest) DO NOTHING`, digest, external, result, millis(now)); err != nil {
		return err
	}
	var storedExternal, storedResult string
	if err = tx.QueryRowContext(ctx, `SELECT external_correlation,result_digest FROM content_results WHERE computed_digest=?`, digest).Scan(&storedExternal, &storedResult); err != nil {
		return err
	}
	if storedExternal != external || storedResult != result {
		return errors.New("content digest already has a conflicting durable result")
	}
	return tx.Commit()
}

func (store *Store) Claim(ctx context.Context, now time.Time) (WorkItem, ExecutorAttempt, bool, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	defer tx.Rollback()
	var item WorkItem
	err = tx.QueryRowContext(ctx, `SELECT w.id,w.route_snapshot_id,w.route_id,w.semantic_object_key,w.source,w.namespace,w.object_kind,w.object_id,w.source_revision,w.serialization_key,w.state,w.state_version,w.superseded_by_id,w.latest_executor_correlation,w.created_at,w.updated_at,w.terminal_at,w.next_attempt_at FROM work_items w WHERE w.state IN (?,?) AND w.next_attempt_at<=? AND NOT EXISTS(SELECT 1 FROM serialization_leases l WHERE l.serialization_key=w.serialization_key) ORDER BY w.next_attempt_at,w.created_at LIMIT 1`, StateAdmitted, StateWaiting, millis(now)).Scan(workItemScan(&item)...)
	if errors.Is(err, sql.ErrNoRows) {
		return WorkItem{}, ExecutorAttempt{}, false, nil
	}
	if err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	var recovered ExecutorAttempt
	err = tx.QueryRowContext(ctx, `SELECT id,work_item_id,attempt_number,executor_id,executor_kind,executor_version,idempotency_key,operation_idempotency_key,requested_operation_digest,state,retry_classification,external_correlation,result_digest,created_at FROM executor_attempts WHERE work_item_id=? AND state=? ORDER BY attempt_number DESC LIMIT 1`, item.ID, AttemptRecoverable).Scan(&recovered.ID, &recovered.WorkItemID, &recovered.AttemptNumber, &recovered.ExecutorID, &recovered.ExecutorKind, &recovered.ExecutorVersion, &recovered.IdempotencyKey, &recovered.OperationIdempotencyKey, &recovered.RequestedOperationDigest, &recovered.State, &recovered.RetryClassification, &recovered.ExternalCorrelation, &recovered.ResultDigest, millisTime{target: &recovered.CreatedAt})
	if err == nil {
		if _, err := tx.ExecContext(ctx, `UPDATE executor_attempts SET state=?,started_at=?,completed_at=NULL WHERE id=? AND state=?`, AttemptRunning, millis(now), recovered.ID, AttemptRecoverable); err != nil {
			return WorkItem{}, ExecutorAttempt{}, false, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO serialization_leases(serialization_key,work_item_id,attempt_id,acquired_at) VALUES(?,?,?,?)`, item.SerializationKey, item.ID, recovered.ID, millis(now)); err != nil {
			return WorkItem{}, ExecutorAttempt{}, false, err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,updated_at=?,next_attempt_at=NULL WHERE id=? AND state=?`, StateActive, millis(now), item.ID, StateWaiting); err != nil {
			return WorkItem{}, ExecutorAttempt{}, false, err
		}
		item.State, item.StateVersion, item.UpdatedAt, item.NextAttemptAt = StateActive, item.StateVersion+1, now.UTC(), nil
		recovered.State = AttemptRunning
		if err := tx.Commit(); err != nil {
			return WorkItem{}, ExecutorAttempt{}, false, err
		}
		return item, recovered, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	var executorID string
	var kind ExecutorKind
	var version, snapshotDigest string
	if err := tx.QueryRowContext(ctx, `SELECT executor_id,executor_kind,executor_version,digest FROM route_snapshots WHERE id=?`, item.RouteSnapshotID).Scan(&executorID, &kind, &version, &snapshotDigest); err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	var number int
	if err := tx.QueryRowContext(ctx, `SELECT count(*)+1 FROM executor_attempts WHERE work_item_id=?`, item.ID).Scan(&number); err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	attempt := ExecutorAttempt{ID: newID("attempt"), WorkItemID: item.ID, AttemptNumber: number, ExecutorID: executorID, ExecutorKind: kind, ExecutorVersion: version, IdempotencyKey: fmt.Sprintf("executor:%s:%s:%d", item.ID, snapshotDigest, number), RequestedOperationDigest: snapshotDigest, State: AttemptRunning, CreatedAt: now.UTC()}
	var contentDigest string
	if operationErr := tx.QueryRowContext(ctx, `SELECT computed_digest FROM release_operations WHERE work_item_id=?`, item.ID).Scan(&contentDigest); operationErr == nil {
		attempt.OperationIdempotencyKey = "signal-plane:resume:v1:" + strings.TrimPrefix(contentDigest, "sha256:")
		attempt.RequestedOperationDigest = contentDigest
	} else if !errors.Is(operationErr, sql.ErrNoRows) {
		return WorkItem{}, ExecutorAttempt{}, false, operationErr
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO executor_attempts(id,work_item_id,attempt_number,executor_id,executor_kind,executor_version,idempotency_key,operation_idempotency_key,requested_operation_digest,state,created_at,started_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, attempt.ID, attempt.WorkItemID, attempt.AttemptNumber, attempt.ExecutorID, attempt.ExecutorKind, attempt.ExecutorVersion, attempt.IdempotencyKey, attempt.OperationIdempotencyKey, attempt.RequestedOperationDigest, attempt.State, millis(now), millis(now)); err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO serialization_leases(serialization_key,work_item_id,attempt_id,acquired_at) VALUES(?,?,?,?)`, item.SerializationKey, item.ID, attempt.ID, millis(now)); err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,updated_at=?,next_attempt_at=NULL WHERE id=? AND state IN (?,?)`, StateActive, millis(now), item.ID, StateAdmitted, StateWaiting); err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	item.State = StateActive
	item.StateVersion++
	item.UpdatedAt = now.UTC()
	item.NextAttemptAt = nil
	if err := tx.Commit(); err != nil {
		return WorkItem{}, ExecutorAttempt{}, false, err
	}
	return item, attempt, true, nil
}

func (store *Store) Complete(ctx context.Context, attemptID string, result ExecutorResult, now time.Time) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var workID string
	var attemptNumber int
	var attemptState AttemptState
	if err := tx.QueryRowContext(ctx, `SELECT work_item_id,attempt_number,state FROM executor_attempts WHERE id=?`, attemptID).Scan(&workID, &attemptNumber, &attemptState); err != nil {
		return err
	}
	if attemptState != AttemptRunning {
		var receipt int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM verifier_result_receipts WHERE work_item_id=? AND attempt_id=?`, workID, attemptID).Scan(&receipt); err == nil && receipt == 1 {
			return tx.Commit()
		}
		return errors.New("attempt is not running")
	}
	var workState WorkState
	var snapshotID string
	if err := tx.QueryRowContext(ctx, `SELECT state,route_snapshot_id FROM work_items WHERE id=?`, workID).Scan(&workState, &snapshotID); err != nil {
		return err
	}
	if workState == StateSuperseded {
		_, err = tx.ExecContext(ctx, `UPDATE executor_attempts SET state=?,completed_at=? WHERE id=?`, AttemptSuperseded, millis(now), attemptID)
	} else if workState != StateActive {
		return errors.New("work item is not active")
	} else {
		definition, err := loadDefinition(ctx, tx, snapshotID)
		if err != nil {
			return err
		}
		attemptResult, nextState := AttemptSucceeded, StateCompleted
		var terminal any = millis(now)
		var next any
		switch result.Outcome {
		case OutcomeCompleted:
		case OutcomePermanentFailure:
			attemptResult, nextState = AttemptFailed, StateFailed
		case OutcomeRetryableFailure:
			if attemptNumber >= definition.Retry.MaxAttempts {
				attemptResult, nextState = AttemptFailed, StateDeadLetter
			} else {
				attemptResult, nextState, terminal = AttemptRetryScheduled, StateWaiting, nil
				delay := definition.Retry.Backoff[attemptNumber-1]
				next = millis(now.Add(delay))
			}
		case OutcomeWaiting:
			attemptResult, nextState, terminal = AttemptRetryScheduled, StateWaiting, nil
			next = nil
		default:
			return fmt.Errorf("unsupported executor outcome %q", result.Outcome)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE executor_attempts SET state=?,retry_classification=?,external_correlation=?,result_digest=?,sanitized_error=?,completed_at=? WHERE id=?`, attemptResult, result.RetryClassification, result.ExternalCorrelation, result.ResultDigest, result.SanitizedError, millis(now), attemptID); err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,latest_executor_correlation=?,updated_at=?,terminal_at=?,next_attempt_at=? WHERE id=? AND state=?`, nextState, result.ExternalCorrelation, millis(now), terminal, next, workID, StateActive)
		}
	}
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM serialization_leases WHERE attempt_id=?`, attemptID); err != nil {
		return err
	}
	return tx.Commit()
}

// WakeWaiting makes externally-blocked work claimable. The correlated caller
// must first durably record the event or evidence that satisfied the wait.
func (store *Store) WakeWaiting(ctx context.Context, workItemID string, now time.Time) error {
	result, err := store.db.ExecContext(ctx, `UPDATE work_items SET next_attempt_at=?,updated_at=?,state_version=state_version+1 WHERE id=? AND state=? AND next_attempt_at IS NULL`, millis(now), millis(now), workItemID, StateWaiting)
	if err != nil {
		return err
	}
	changed, _ := result.RowsAffected()
	if changed != 1 {
		return errors.New("work item is not waiting for an external wakeup")
	}
	return nil
}

func (store *Store) RecoverInterrupted(ctx context.Context, now time.Time) (int64, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE executor_attempts SET state=?,completed_at=? WHERE state IN (?,?,?) AND work_item_id IN (SELECT id FROM work_items WHERE state=?)`, AttemptSuperseded, millis(now), AttemptRunning, AttemptRecoverable, AttemptRetryScheduled, StateSuperseded); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE executor_attempts SET state=?,sanitized_error='executor completion is ambiguous; reclaim with the same idempotency key' WHERE state=? AND work_item_id IN (SELECT id FROM work_items WHERE state=?)`, AttemptRecoverable, AttemptRunning, StateActive); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE work_items SET state=?,state_version=state_version+1,next_attempt_at=?,updated_at=? WHERE state=?`, StateWaiting, millis(now), millis(now), StateActive)
	if err != nil {
		return 0, err
	}
	count, _ := result.RowsAffected()
	if _, err := tx.ExecContext(ctx, `DELETE FROM serialization_leases`); err != nil {
		return 0, err
	}
	return count, tx.Commit()
}

func loadSnapshot(ctx context.Context, tx *sql.Tx, id string) (RouteSnapshot, RouteDefinition, error) {
	var snapshot RouteSnapshot
	var encoded string
	var activated int64
	var retired sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT id,route_id,schema_version,semantic_version,digest,executor_id,executor_kind,executor_version,task_kind,task_version,completion_contract,verifier_id,task_contract_digest,definition_json,activated_at,retired_at FROM route_snapshots WHERE id=?`, id).Scan(&snapshot.ID, &snapshot.RouteID, &snapshot.SchemaVersion, &snapshot.SemanticVersion, &snapshot.Digest, &snapshot.ExecutorID, &snapshot.ExecutorKind, &snapshot.ExecutorVersion, &snapshot.TaskKind, &snapshot.TaskVersion, &snapshot.CompletionContract, &snapshot.VerifierID, &snapshot.TaskContractDigest, &encoded, &activated, &retired)
	if err != nil {
		return RouteSnapshot{}, RouteDefinition{}, err
	}
	definition, err := DecodeRouteDefinition([]byte(encoded))
	snapshot.Admission, snapshot.Concurrency, snapshot.Retry, snapshot.ActivatedAt = definition.Admission, definition.Concurrency, definition.Retry, time.UnixMilli(activated).UTC()
	if definition.Task != nil {
		snapshot.TaskParameters = append(json.RawMessage(nil), definition.Task.Parameters...)
	}
	if retired.Valid {
		value := time.UnixMilli(retired.Int64).UTC()
		snapshot.RetiredAt = &value
	}
	return snapshot, definition, err
}

func loadDefinition(ctx context.Context, tx *sql.Tx, id string) (RouteDefinition, error) {
	var encoded string
	if err := tx.QueryRowContext(ctx, `SELECT definition_json FROM route_snapshots WHERE id=?`, id).Scan(&encoded); err != nil {
		return RouteDefinition{}, err
	}
	return DecodeRouteDefinition([]byte(encoded))
}

func loadWorkItem(ctx context.Context, tx *sql.Tx, id string) (WorkItem, error) {
	var item WorkItem
	err := tx.QueryRowContext(ctx, `SELECT id,route_snapshot_id,route_id,semantic_object_key,source,namespace,object_kind,object_id,source_revision,serialization_key,state,state_version,superseded_by_id,latest_executor_correlation,created_at,updated_at,terminal_at,next_attempt_at FROM work_items WHERE id=?`, id).Scan(workItemScan(&item)...)
	return item, err
}

func workItemScan(item *WorkItem) []any {
	return []any{&item.ID, &item.RouteSnapshotID, &item.RouteID, &item.SemanticObjectKey, &item.Source, &item.Namespace, &item.ObjectKind, &item.ObjectID, &item.SourceRevision, &item.SerializationKey, &item.State, &item.StateVersion, nullString{target: &item.SupersededByID}, &item.LatestExecutorCorrelation, millisTime{target: &item.CreatedAt}, millisTime{target: &item.UpdatedAt}, optionalTime{target: &item.TerminalAt}, optionalTime{target: &item.NextAttemptAt}}
}

type nullString struct{ target *string }

func (value nullString) Scan(source any) error {
	if source == nil {
		*value.target = ""
		return nil
	}
	*value.target = fmt.Sprint(source)
	return nil
}

type millisTime struct{ target *time.Time }

func (value millisTime) Scan(source any) error {
	milliseconds, ok := source.(int64)
	if !ok {
		return fmt.Errorf("invalid time %T", source)
	}
	*value.target = time.UnixMilli(milliseconds).UTC()
	return nil
}

type optionalTime struct{ target **time.Time }

func (value optionalTime) Scan(source any) error {
	if source == nil {
		*value.target = nil
		return nil
	}
	milliseconds, ok := source.(int64)
	if !ok {
		return fmt.Errorf("invalid optional time %T", source)
	}
	decoded := time.UnixMilli(milliseconds).UTC()
	*value.target = &decoded
	return nil
}

func millis(value time.Time) int64 { return value.UTC().UnixMilli() }
func optionalMillis(value *time.Time) any {
	if value == nil {
		return nil
	}
	return millis(*value)
}
func newID(prefix string) string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(value[:])
}

func activationDigest(definition RouteDefinition, descriptor ExecutorDescriptor, task *TaskDescriptor) (string, error) {
	if err := definition.Validate(); err != nil {
		return "", err
	}
	if err := descriptor.Validate(); err != nil {
		return "", err
	}
	if task != nil {
		if err := task.Validate(); err != nil {
			return "", err
		}
	}
	encoded, err := json.Marshal(struct {
		Route    RouteDefinition    `json:"route"`
		Executor ExecutorDescriptor `json:"executor"`
		Task     *TaskDescriptor    `json:"task,omitempty"`
	}{definition, descriptor, task})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}
