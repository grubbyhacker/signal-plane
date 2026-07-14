package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Delivery interface {
	Data() []byte
	StreamSequence() (uint64, error)
	AckSync() error
	Term() error
}
type NATSDelivery struct{ Message *nats.Msg }

func (d NATSDelivery) Data() []byte { return d.Message.Data }
func (d NATSDelivery) StreamSequence() (uint64, error) {
	metadata, err := d.Message.Metadata()
	if err != nil {
		return 0, err
	}
	return metadata.Sequence.Stream, nil
}
func (d NATSDelivery) AckSync() error { return d.Message.AckSync() }
func (d NATSDelivery) Term() error    { return d.Message.Term() }

type Metrics struct {
	registry   *prometheus.Registry
	deliveries *prometheus.CounterVec
	jobs       *prometheus.CounterVec
	ready      prometheus.Gauge
	lifecycle  *prometheus.GaugeVec
	retries    prometheus.Counter
	terminals  *prometheus.CounterVec
	oldestAge  prometheus.Gauge
	mu         sync.RWMutex
	isReady    bool
	disabled   bool
}

func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{
		registry: r, deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "github_task_dispatcher_deliveries_total", Help: "Dispatcher deliveries by bounded outcome."}, []string{"outcome"}),
		jobs:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "github_task_dispatcher_jobs_total", Help: "Broker operations by bounded outcome."}, []string{"outcome"}),
		ready:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "github_task_dispatcher_readiness", Help: "Whether dispatcher dependencies are ready."}),
		lifecycle: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "github_task_dispatcher_lifecycle_jobs", Help: "Jobs in each bounded durable lifecycle state."}, []string{"state"}),
		retries:   prometheus.NewCounter(prometheus.CounterOpts{Name: "github_task_dispatcher_launch_retry_exhausted_total", Help: "Launches that exhausted the durable retry window."}),
		terminals: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "github_task_dispatcher_terminal_outcomes_total", Help: "Broker runs by bounded terminal outcome."}, []string{"outcome"}),
		oldestAge: prometheus.NewGauge(prometheus.GaugeOpts{Name: "github_task_dispatcher_oldest_active_job_age_seconds", Help: "Age of the oldest pending, retrying, or launched job."}),
	}
	r.MustRegister(m.deliveries, m.jobs, m.ready, m.lifecycle, m.retries, m.terminals, m.oldestAge, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "github_task_dispatcher_build_info", Help: "Build information.", ConstLabels: prometheus.Labels{"version": buildinfo.Version}}, func() float64 { return 1 }))
	for _, state := range lifecycleStates {
		m.lifecycle.WithLabelValues(state).Set(0)
	}
	for _, outcome := range []string{StateCompleted, StateFailed, StateTimedOut} {
		m.terminals.WithLabelValues(outcome)
	}
	return m
}
func (m *Metrics) SetReady(v bool) {
	m.mu.Lock()
	m.isReady = v
	m.mu.Unlock()
	if v {
		m.ready.Set(1)
	} else {
		m.ready.Set(0)
	}
}
func (m *Metrics) SetDisabled() {
	m.mu.Lock()
	m.disabled = true
	m.isReady = false
	m.mu.Unlock()
	m.ready.Set(0)
}
func (m *Metrics) Refresh(ctx context.Context, store *Store, now time.Time) error {
	stats, err := store.Stats(ctx, now)
	if err != nil {
		return err
	}
	for _, state := range lifecycleStates {
		m.lifecycle.WithLabelValues(state).Set(stats.Counts[state])
	}
	m.oldestAge.Set(stats.OldestAge.Seconds())
	return nil
}
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		m.mu.RLock()
		ready := m.isReady
		disabled := m.disabled
		m.mu.RUnlock()
		w.Header().Set("content-type", "application/json")
		if !ready {
			w.WriteHeader(503)
			if disabled {
				_, _ = w.Write([]byte("{\"error\":\"disabled\"}\n"))
				return
			}
			_, _ = w.Write([]byte("{\"error\":\"not_ready\"}\n"))
			return
		}
		_, _ = w.Write([]byte("{\"status\":\"ready\"}\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}

func Process(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, delivery Delivery, now time.Time) bool {
	err := process(ctx, logger, metrics, store, delivery, now, "", true)
	return err == nil
}

// ProcessRecovery fails closed without terminating malformed stream data. Its
// replay evidence is committed atomically with the normal dispatcher state.
func ProcessRecovery(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, recoveryID string, delivery Delivery, now time.Time) error {
	if recoveryID == "" {
		return errors.New("recovery id is required")
	}
	return process(ctx, logger, metrics, store, delivery, now, recoveryID, false)
}

func process(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, delivery Delivery, now time.Time, recoveryID string, terminateMalformed bool) error {
	streamSequence, err := delivery.StreamSequence()
	if err != nil || streamSequence == 0 {
		metrics.deliveries.WithLabelValues("metadata_failed").Inc()
		logger.Error("read JetStream delivery metadata failed", "error", err)
		if err == nil {
			err = errors.New("stream sequence is zero")
		}
		return fmt.Errorf("read JetStream delivery metadata: %w", err)
	}
	var signal envelope.Signal
	if err := json.Unmarshal(delivery.Data(), &signal); err != nil {
		metrics.deliveries.WithLabelValues("malformed").Inc()
		logger.Warn("malformed signal; terminating delivery")
		if terminateMalformed {
			_ = delivery.Term()
		}
		return fmt.Errorf("decode replayed signal: %w", err)
	}
	candidate, outcome := Select(signal)
	var selected *Candidate
	if outcome == "accepted" {
		selected = &candidate
	}
	if recoveryID == "" {
		err = store.Record(ctx, signal.Meta.SourceDeliveryID, outcome, streamSequence, selected, now)
	} else {
		err = store.RecordRecovery(ctx, recoveryID, signal.Meta.SourceDeliveryID, outcome, streamSequence, selected, now)
	}
	if err != nil {
		metrics.deliveries.WithLabelValues("store_failed").Inc()
		logger.Error("persist delivery failed", "error", err)
		return fmt.Errorf("persist delivery: %w", err)
	}
	if err := delivery.AckSync(); err != nil {
		metrics.deliveries.WithLabelValues("ack_failed").Inc()
		logger.Error("acknowledge delivery failed", "error", err)
		return fmt.Errorf("acknowledge delivery: %w", err)
	}
	if selected != nil {
		metrics.deliveries.WithLabelValues("accepted").Inc()
	} else {
		metrics.deliveries.WithLabelValues("irrelevant").Inc()
	}
	_ = metrics.Refresh(ctx, store, now)
	return nil
}

type BrokerClient interface {
	Launch(context.Context, Job) (LaunchResult, error)
	Status(context.Context, string) (RunStatus, error)
}

const (
	LaunchRetryWindow  = 10 * time.Minute
	StatusPollInterval = 2 * time.Second
)

func LaunchRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 2 * time.Second * time.Duration(1<<min(attempt-1, 4))
	if delay > 20*time.Second {
		return 20 * time.Second
	}
	return delay
}

func RunOne(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, broker BrokerClient, now time.Time) (bool, error) {
	work, ok, err := store.ClaimDue(ctx, now)
	if err != nil || !ok {
		return ok, err
	}
	job := work.Job
	if work.Kind == WorkStatus {
		return runStatus(ctx, logger, metrics, store, broker, job, now)
	}
	if !job.FirstAttemptAt.IsZero() && !now.Before(job.FirstAttemptAt.Add(LaunchRetryWindow)) {
		metrics.retries.Inc()
		metrics.terminals.WithLabelValues(StateFailed).Inc()
		err := store.MarkLaunchFailure(ctx, job.ID, false, now, "launch retry window exhausted", now)
		_ = metrics.Refresh(ctx, store, now)
		return true, err
	}
	result, err := broker.Launch(ctx, job)
	if err == nil {
		if err := store.MarkLaunched(ctx, job.ID, result.RunID, now.Add(StatusPollInterval), now); err != nil {
			return true, err
		}
		metrics.jobs.WithLabelValues("launched").Inc()
		logger.Info("broker launch accepted", "job_id", job.ID, "broker_run_id", result.RunID)
		_ = metrics.Refresh(ctx, store, now)
		return true, nil
	}
	retry := IsRetryable(err)
	due := now.Add(LaunchRetryDelay(job.Attempts))
	if retry && !due.Before(job.FirstAttemptAt.Add(LaunchRetryWindow)) {
		due = job.FirstAttemptAt.Add(LaunchRetryWindow)
	}
	if retry {
		metrics.jobs.WithLabelValues("retry").Inc()
	} else {
		metrics.jobs.WithLabelValues("failed").Inc()
		metrics.terminals.WithLabelValues(StateFailed).Inc()
	}
	logger.Warn("broker launch failed", "job_id", job.ID, "retry", retry, "attempt", job.Attempts, "error", err)
	storeErr := store.MarkLaunchFailure(ctx, job.ID, retry, due, err.Error(), now)
	_ = metrics.Refresh(ctx, store, now)
	return true, storeErr
}

func runStatus(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, broker BrokerClient, job Job, now time.Time) (bool, error) {
	result, err := broker.Status(ctx, job.BrokerRunID)
	if err != nil {
		if IsRetryable(err) {
			metrics.jobs.WithLabelValues("status_retry").Inc()
			storeErr := store.MarkStatus(ctx, job.ID, StateLaunched, now.Add(StatusPollInterval), err.Error(), now)
			_ = metrics.Refresh(ctx, store, now)
			return true, storeErr
		}
		metrics.terminals.WithLabelValues(StateFailed).Inc()
		storeErr := store.MarkStatus(ctx, job.ID, StateFailed, now, err.Error(), now)
		_ = metrics.Refresh(ctx, store, now)
		return true, storeErr
	}
	state, err := ReconciledStatus(result.Status)
	if err != nil {
		state = StateFailed
	}
	due := now
	if state == StateLaunched {
		due = now.Add(StatusPollInterval)
		metrics.jobs.WithLabelValues("status_nonterminal").Inc()
	} else {
		metrics.terminals.WithLabelValues(state).Inc()
		logger.Info("broker run reached terminal state", "job_id", job.ID, "broker_run_id", job.BrokerRunID, "outcome", state)
	}
	message := ""
	if err != nil {
		message = err.Error()
	}
	storeErr := store.MarkStatus(ctx, job.ID, state, due, message, now)
	_ = metrics.Refresh(ctx, store, now)
	return true, storeErr
}

// ReconciledStatus validates the broker's bounded lifecycle vocabulary. The
// recovery workflow uses this before mutating a restored job.
func ReconciledStatus(status string) (string, error) {
	switch status {
	case "accepted", "queued", "pending", "launching", "running", "in_progress":
		return StateLaunched, nil
	case "completed", "succeeded", "success":
		return StateCompleted, nil
	case "failed", "error", "cancelled", "canceled":
		return StateFailed, nil
	case "timed_out", "timeout":
		return StateTimedOut, nil
	default:
		return "", BrokerError{Malformed: true, Message: "broker returned unknown run status"}
	}
}
