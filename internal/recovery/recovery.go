// Package recovery implements the explicit, fail-closed dispatcher restore
// workflow. It never launches broker work.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
)

type Options struct {
	RecoveryID       string
	Durable          string
	Subject          string
	ManifestSequence uint64
	Execute          bool
}

type Report struct {
	Mode                    string                      `json:"mode"`
	RecoveryID              string                      `json:"recovery_id"`
	Durable                 string                      `json:"durable"`
	ManifestSequence        uint64                      `json:"manifest_last_persisted_jetstream_sequence"`
	StartSequence           uint64                      `json:"start_sequence"`
	RestoredNonterminalJobs int                         `json:"restored_nonterminal_jobs"`
	ReplayCount             uint64                      `json:"replay_count"`
	Status                  string                      `json:"status"`
	Reconciliations         []dispatcher.Reconciliation `json:"reconciliations"`
}

type StatusClient interface {
	Status(context.Context, string) (dispatcher.RunStatus, error)
}

type Stream interface {
	Reset(context.Context, string, string, uint64) (ReplayConsumer, uint64, error)
}

type ReplayConsumer interface {
	Fetch(time.Duration) (dispatcher.Delivery, error)
}

type Runner struct {
	Store   *dispatcher.Store
	Broker  StatusClient
	Stream  Stream
	Logger  *slog.Logger
	Now     func() time.Time
	Timeout time.Duration
}

func (r Runner) Run(ctx context.Context, opts Options) (report Report, err error) {
	if r.Store == nil || opts.RecoveryID == "" || opts.Durable == "" || opts.Subject == "" {
		return Report{}, errors.New("store, recovery id, durable, and subject are required")
	}
	_, checkpoint, start, err := r.Store.RecoveryMetadata(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("read restored recovery metadata: %w", err)
	}
	if checkpoint != opts.ManifestSequence {
		return Report{}, fmt.Errorf("manifest sequence %d does not match restored SQLite checkpoint %d", opts.ManifestSequence, checkpoint)
	}
	jobs, err := r.Store.RecoveryJobs(ctx)
	if err != nil {
		return Report{}, fmt.Errorf("list restored nonterminal jobs: %w", err)
	}
	report = Report{Mode: "dry-run", RecoveryID: opts.RecoveryID, Durable: opts.Durable, ManifestSequence: checkpoint, StartSequence: start, RestoredNonterminalJobs: len(jobs), Status: "validated"}
	for _, job := range jobs {
		if job.Status != dispatcher.StateLaunched || job.BrokerRunID == "" {
			return report, fmt.Errorf("restored nonterminal job %d is %q without a reconcilable broker run id", job.ID, job.Status)
		}
	}
	if !opts.Execute {
		return report, nil
	}
	if r.Broker == nil || r.Stream == nil {
		return report, errors.New("execute requires broker and stream dependencies")
	}
	now := time.Now().UTC
	if r.Now != nil {
		now = r.Now
	}
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	report.Mode = "execute"
	report.Status = dispatcher.RecoveryIncomplete
	if err := r.Store.BeginRecovery(ctx, opts.RecoveryID, opts.Durable, checkpoint, now()); err != nil {
		return report, fmt.Errorf("begin recovery: %w", err)
	}
	defer func() {
		if err != nil {
			_ = r.Store.FailRecovery(context.Background(), opts.RecoveryID, err)
		}
	}()
	jobs, err = r.Store.PrepareRecoveryJobs(ctx, opts.RecoveryID)
	if err != nil {
		return report, fmt.Errorf("freeze restored nonterminal jobs: %w", err)
	}
	report.RestoredNonterminalJobs = len(jobs)

	consumer, pending, err := r.Stream.Reset(ctx, opts.Subject, opts.Durable, start)
	if err != nil {
		return report, fmt.Errorf("reset dispatcher durable: %w", err)
	}
	timeout := r.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	metrics := dispatcher.NewMetrics()
	for i := uint64(0); i < pending; i++ {
		delivery, fetchErr := consumer.Fetch(timeout)
		if fetchErr != nil {
			return report, fmt.Errorf("fetch replay message %d of %d: %w", i+1, pending, fetchErr)
		}
		if processErr := dispatcher.ProcessRecovery(ctx, logger, metrics, r.Store, opts.RecoveryID, delivery, now()); processErr != nil {
			return report, fmt.Errorf("replay message %d of %d: %w", i+1, pending, processErr)
		}
	}

	type checked struct {
		job          dispatcher.Job
		brokerStatus string
		state        string
	}
	checkedStatuses := make([]checked, 0, len(jobs))
	for _, job := range jobs {
		status, statusErr := r.Broker.Status(ctx, job.BrokerRunID)
		if statusErr != nil {
			return report, fmt.Errorf("reconcile restored job %d: %w", job.ID, statusErr)
		}
		state, stateErr := dispatcher.ReconciledStatus(status.Status)
		if stateErr != nil {
			return report, fmt.Errorf("reconcile restored job %d: %w", job.ID, stateErr)
		}
		checkedStatuses = append(checkedStatuses, checked{job: job, brokerStatus: status.Status, state: state})
	}
	for _, item := range checkedStatuses {
		if err := r.Store.RecordReconciliation(ctx, opts.RecoveryID, item.job, item.brokerStatus, item.state, now()); err != nil {
			return report, fmt.Errorf("record restored job %d reconciliation: %w", item.job.ID, err)
		}
	}
	run, outcomes, err := r.Store.CompleteRecovery(ctx, opts.RecoveryID, len(jobs), now())
	if err != nil {
		return report, fmt.Errorf("complete recovery: %w", err)
	}
	if len(outcomes) != len(jobs) {
		return report, fmt.Errorf("recorded %d reconciliations for %d restored nonterminal jobs", len(outcomes), len(jobs))
	}
	report.ReplayCount = run.ReplayCount
	report.Reconciliations = outcomes
	report.Status = run.Status
	return report, nil
}

type NATSStream struct{ Bus *eventbus.Bus }

func (s NATSStream) Reset(_ context.Context, subject, durable string, start uint64) (ReplayConsumer, uint64, error) {
	if s.Bus == nil {
		return nil, 0, errors.New("event bus is required")
	}
	consumer, pending, err := s.Bus.ResetConsumer(eventbus.ConsumerConfig{Subject: subject, Durable: durable, AckWait: 30 * time.Second, MaxAckPending: 64, MaxDeliver: 10, StartSequence: start})
	if err != nil {
		return nil, 0, err
	}
	return natsReplayConsumer{consumer}, pending, nil
}

type natsReplayConsumer struct{ consumer *eventbus.Consumer }

func (c natsReplayConsumer) Fetch(timeout time.Duration) (dispatcher.Delivery, error) {
	msg, err := c.consumer.Fetch(timeout)
	if err != nil {
		return nil, err
	}
	return dispatcher.NATSDelivery{Message: msg}, nil
}
