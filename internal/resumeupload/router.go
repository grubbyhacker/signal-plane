package resumeupload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

type Delivery interface {
	Data() []byte
	StreamSequence() (uint64, error)
	AckSync() error
	Term() error
	NumDelivered() int
}
type Router struct {
	Store         *workledger.Store
	Registry      *workledger.Registry
	GitHub        ReleaseHydrator
	Stream        string
	Now           func() time.Time
	recoveryMu    sync.Mutex
	needsRecovery bool
}
type ReleaseHydrator interface {
	Hydrate(context.Context, int64, int64, int64) (workledger.ReleaseOperation, error)
}

func (router *Router) Process(ctx context.Context, delivery Delivery) error {
	var signal envelope.Signal
	if err := json.Unmarshal(delivery.Data(), &signal); err != nil {
		_ = delivery.Term()
		return err
	}
	sequence, err := delivery.StreamSequence()
	if err != nil {
		return err
	}
	adapter, err := workledger.AdapterFor(signal.Meta.Source)
	if err != nil {
		_ = delivery.Term()
		return err
	}
	event, err := adapter.Normalize(signal, router.Stream, sequence)
	if err != nil {
		_ = delivery.Term()
		return err
	}
	if event.Source != "github" || event.Namespace != Repository || event.ObjectKind != "release" || event.EventKind != "release" || event.Action != "published" {
		_ = delivery.Term()
		return errors.New("event does not match the exact Resume Builder release admission")
	}
	if recorded, err := router.Store.DeliveryRecorded(ctx, event); err != nil {
		return err
	} else if recorded {
		return delivery.AckSync()
	}
	var payload struct {
		Repository struct {
			ID       int64  `json:"id"`
			FullName string `json:"full_name"`
		} `json:"repository"`
		Installation struct {
			ID int64 `json:"id"`
		} `json:"installation"`
		Release struct {
			ID int64 `json:"id"`
		} `json:"release"`
	}
	if err := json.Unmarshal(signal.Payload, &payload); err != nil || payload.Repository.FullName != Repository || event.ObjectID != fmt.Sprint(payload.Release.ID) {
		_ = delivery.Term()
		return errors.New("release webhook identity is invalid")
	}
	operation, err := router.GitHub.Hydrate(ctx, payload.Repository.ID, payload.Installation.ID, payload.Release.ID)
	if err != nil {
		return router.retryOrDeadLetter(ctx, delivery, event, "release_hydration", err)
	}
	snapshot, err := router.Store.MatchRoute(ctx, event)
	if err != nil {
		return err
	}
	if snapshot.ExecutorID != ExecutorID {
		return errors.New("matched route selected an unsupported executor")
	}
	now := time.Now().UTC()
	if router.Now != nil {
		now = router.Now()
	}
	if _, err := router.Store.AdmitRelease(ctx, snapshot.ID, event, operation, now); err != nil {
		return err
	}
	return delivery.AckSync()
}
func (router *Router) retryOrDeadLetter(ctx context.Context, delivery Delivery, event workledger.Event, classification string, cause error) error {
	if delivery.NumDelivered() < 5 {
		return cause
	}
	now := time.Now().UTC()
	if router.Now != nil {
		now = router.Now()
	}
	if err := router.Store.RecordIngressFailure(ctx, event, classification, delivery.NumDelivered(), now); err != nil {
		return fmt.Errorf("record ingress dead letter: %w", err)
	}
	if err := delivery.Term(); err != nil {
		return err
	}
	return cause
}
func (router *Router) Recover(ctx context.Context) error {
	_, err := router.Store.RecoverInterrupted(ctx, time.Now().UTC())
	return err
}
func (router *Router) WorkOne(ctx context.Context) (bool, error) {
	now := time.Now().UTC()
	if router.Now != nil {
		now = router.Now()
	}
	router.recoveryMu.Lock()
	needsRecovery := router.needsRecovery
	router.recoveryMu.Unlock()
	if needsRecovery {
		if _, err := router.Store.RecoverInterrupted(ctx, now); err != nil {
			return false, err
		}
		router.recoveryMu.Lock()
		router.needsRecovery = false
		router.recoveryMu.Unlock()
	}
	item, attempt, ok, err := router.Store.Claim(ctx, now)
	if err != nil || !ok {
		return ok, err
	}
	executor, err := router.Registry.Resolve(attempt.ExecutorID)
	if err != nil {
		result := workledger.ExecutorResult{Outcome: workledger.OutcomePermanentFailure, SanitizedError: "claimed executor is not registered"}
		return true, router.finish(ctx, attempt.ID, result, now)
	}
	descriptor := executor.Descriptor()
	if descriptor.Version != attempt.ExecutorVersion || descriptor.Kind != attempt.ExecutorKind {
		result := workledger.ExecutorResult{Outcome: workledger.OutcomePermanentFailure, SanitizedError: "claimed executor version is not registered"}
		return true, router.finish(ctx, attempt.ID, result, now)
	}
	result, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil {
		result = workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "executor_internal", SanitizedError: "executor returned an internal error"}
	}
	return true, router.finish(ctx, attempt.ID, result, now)
}

func (router *Router) finish(ctx context.Context, attemptID string, result workledger.ExecutorResult, now time.Time) error {
	if err := router.Store.Complete(ctx, attemptID, result, now); err != nil {
		router.recoveryMu.Lock()
		router.needsRecovery = true
		router.recoveryMu.Unlock()
		if _, recoveryErr := router.Store.RecoverInterrupted(ctx, now); recoveryErr != nil {
			return fmt.Errorf("complete attempt: %v; reconcile: %w", err, recoveryErr)
		}
		router.recoveryMu.Lock()
		router.needsRecovery = false
		router.recoveryMu.Unlock()
		return fmt.Errorf("complete attempt after durable reconciliation: %w", err)
	}
	return nil
}
