package workledger

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
}

type Executor interface {
	Descriptor() ExecutorDescriptor
	Execute(context.Context, ExecutorRequest) (ExecutorResult, error)
}

type Registry struct {
	mu        sync.RWMutex
	executors map[string]Executor
}

func NewRegistry() *Registry { return &Registry{executors: make(map[string]Executor)} }

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

func (registry *Registry) Resolve(id string) (Executor, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	executor, exists := registry.executors[id]
	if !exists {
		return nil, fmt.Errorf("executor %q is not registered", id)
	}
	return executor, nil
}
