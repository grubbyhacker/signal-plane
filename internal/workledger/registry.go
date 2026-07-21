package workledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

type ExecutorDescriptor struct {
	ID      string
	Kind    ExecutorKind
	Version string
}

func (descriptor ExecutorDescriptor) Validate() error {
	if !identifierPattern.MatchString(descriptor.ID) || descriptor.Version == "" {
		return errors.New("executor requires a bounded id and version")
	}
	switch descriptor.Kind {
	case ExecutorDeterministicTool, ExecutorPolicyEvaluator, ExecutorAgentSession:
		return nil
	default:
		return fmt.Errorf("unsupported executor kind %q", descriptor.Kind)
	}
}

type ExecutorRequest struct {
	WorkItem WorkItem
	Attempt  ExecutorAttempt
}

type ExecutorOutcome string

const (
	OutcomeCompleted        ExecutorOutcome = "completed"
	OutcomeRetryableFailure ExecutorOutcome = "retryable_failure"
	OutcomePermanentFailure ExecutorOutcome = "permanent_failure"
	OutcomeWaiting          ExecutorOutcome = "waiting"
)

type ExecutorResult struct {
	Outcome             ExecutorOutcome
	RetryClassification string
	ExternalCorrelation string
	SanitizedError      string
	ResultDigest        string
	NextAttemptAt       *time.Time
	DeadlineAt          *time.Time
}

type Executor interface {
	Descriptor() ExecutorDescriptor
	Execute(context.Context, ExecutorRequest) (ExecutorResult, error)
}

type TaskDescriptor struct {
	Kind               string
	Version            string
	CompletionContract string
	VerifierID         string
	ContractDigest     string
}

type WorkTaskSnapshot struct {
	TaskDescriptor
	WorkItemID         string
	RouteSnapshotID    string
	Parameters         json.RawMessage
	TaskEvidenceDigest string
}

func (store *Store) WorkTaskSnapshot(ctx context.Context, workItemID string) (WorkTaskSnapshot, error) {
	var snapshot WorkTaskSnapshot
	var definitionJSON string
	err := store.db.QueryRowContext(ctx, `SELECT w.id,w.route_snapshot_id,s.task_kind,s.task_version,s.completion_contract,s.verifier_id,s.task_contract_digest,s.definition_json,w.task_evidence_digest FROM work_items w JOIN route_snapshots s ON s.id=w.route_snapshot_id WHERE w.id=?`, workItemID).Scan(&snapshot.WorkItemID, &snapshot.RouteSnapshotID, &snapshot.Kind, &snapshot.Version, &snapshot.CompletionContract, &snapshot.VerifierID, &snapshot.ContractDigest, &definitionJSON, &snapshot.TaskEvidenceDigest)
	if err != nil {
		return WorkTaskSnapshot{}, err
	}
	definition, err := DecodeRouteDefinition([]byte(definitionJSON))
	if err != nil || definition.Task == nil {
		return WorkTaskSnapshot{}, errors.New("work item has no registered task snapshot")
	}
	snapshot.Parameters = append(json.RawMessage(nil), definition.Task.Parameters...)
	return snapshot, nil
}

func (descriptor TaskDescriptor) Validate() error {
	if !identifierPattern.MatchString(descriptor.Kind) ||
		strings.TrimSpace(descriptor.Version) == "" ||
		!identifierPattern.MatchString(descriptor.CompletionContract) ||
		!identifierPattern.MatchString(descriptor.VerifierID) ||
		!regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(descriptor.ContractDigest) {
		return errors.New("task kind requires registered identifiers and version")
	}
	return nil
}

type TaskKind interface {
	Descriptor() TaskDescriptor
	CanonicalizeParameters(json.RawMessage) (json.RawMessage, error)
	ValidateAdmission(AdmissionPolicy) error
}

type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
	tasks     map[string]TaskKind
}

func NewRegistry() *Registry {
	return &Registry{executors: make(map[string]Executor), tasks: make(map[string]TaskKind)}
}

func (registry *Registry) Register(executor Executor) error {
	if executor == nil {
		return errors.New("executor is required")
	}
	descriptor := executor.Descriptor()
	if err := descriptor.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.executors[descriptor.ID]; exists {
		return fmt.Errorf("executor %q is already registered", descriptor.ID)
	}
	registry.executors[descriptor.ID] = executor
	return nil
}

func (registry *Registry) RegisterTask(task TaskKind) error {
	if task == nil {
		return errors.New("task kind is required")
	}
	descriptor := task.Descriptor()
	if err := descriptor.Validate(); err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.tasks[descriptor.Kind]; exists {
		return fmt.Errorf("task kind %q is already registered", descriptor.Kind)
	}
	registry.tasks[descriptor.Kind] = task
	return nil
}

func (registry *Registry) ResolveTask(selection *TaskSelection, admission AdmissionPolicy) (TaskDescriptor, json.RawMessage, error) {
	if selection == nil {
		return TaskDescriptor{}, nil, errors.New("registered task selection is required")
	}
	registry.mu.RLock()
	task, exists := registry.tasks[selection.Kind]
	registry.mu.RUnlock()
	if !exists {
		return TaskDescriptor{}, nil, fmt.Errorf("task kind %q is not registered", selection.Kind)
	}
	if err := task.ValidateAdmission(admission); err != nil {
		return TaskDescriptor{}, nil, err
	}
	parameters, err := task.CanonicalizeParameters(selection.Parameters)
	if err != nil {
		return TaskDescriptor{}, nil, err
	}
	return task.Descriptor(), parameters, nil
}

func (registry *Registry) Resolve(id string) (Executor, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	executor, exists := registry.executors[id]
	if !exists {
		return nil, fmt.Errorf("executor %q is not registered", id)
	}
	return executor, nil
}
