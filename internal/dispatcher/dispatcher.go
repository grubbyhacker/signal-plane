package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
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
	for _, outcome := range []string{StateCompleted, StateFailed, StateTimedOut, StateStopped, StateCancelled} {
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

func Process(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, routes []config.RepositoryTaskRoute, delivery Delivery, now time.Time) bool {
	err := process(ctx, logger, metrics, store, routes, delivery, now, "", true)
	return err == nil
}

// ProcessRecovery fails closed without terminating malformed stream data. Its
// replay evidence is committed atomically with the normal dispatcher state.
func ProcessRecovery(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, routes []config.RepositoryTaskRoute, recoveryID string, delivery Delivery, now time.Time) error {
	if recoveryID == "" {
		return errors.New("recovery id is required")
	}
	return process(ctx, logger, metrics, store, routes, delivery, now, recoveryID, false)
}

func process(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, routes []config.RepositoryTaskRoute, delivery Delivery, now time.Time, recoveryID string, terminateMalformed bool) error {
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
	if isCIWakeEvent(signal.Meta.SourceEvent) && routeOwnsRepository(routes, signal.Meta.Namespace) {
		if _, err := store.RecordCIEvent(ctx, signal, now); err != nil {
			metrics.deliveries.WithLabelValues("store_failed").Inc()
			logger.Error("persist CI wake-up failed", "error", err)
			return fmt.Errorf("persist CI wake-up: %w", err)
		}
	}
	candidate, outcome := Select(signal, routes)
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

func routeOwnsRepository(routes []config.RepositoryTaskRoute, repository string) bool {
	for _, route := range routes {
		if route.Repository == repository {
			return true
		}
	}
	return false
}

type BrokerClient interface {
	Launch(context.Context, Job) (LaunchResult, error)
	Status(context.Context, string) (RunStatus, error)
}
type TerminalBroker interface {
	TerminalResult(context.Context, string) (TerminalResult, error)
	Comment(context.Context, Job, string, string) (CommentResult, error)
}
type TerminalProjectionClient interface {
	TerminalResult(context.Context, string) (TerminalResult, error)
}
type CIRepairBroker interface {
	ObserveCI(context.Context, CIRepairTask, string) (CIObservation, error)
	LaunchRepair(context.Context, CIRepairTask, CIRepairAttempt, time.Time) (LaunchResult, error)
	Status(context.Context, string) (RunStatus, error)
	TerminalResult(context.Context, string) (TerminalResult, error)
}

const (
	LaunchRetryWindow      = 10 * time.Minute
	ReportRetryMaxAttempts = 24
	PreOutboxMaxAttempts   = 10
	StatusPollInterval     = 2 * time.Second
	// ReporterUnavailableDelay bounds disabled reporting checks without
	// treating a deliberately absent reporter as a terminal broker failure.
	ReporterUnavailableDelay = time.Minute
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

// ReportRetryDelay keeps a temporary reporter outage durable without generating
// a high-frequency write loop. Combined with ReportRetryMaxAttempts, a report
// is attempted for roughly 17 hours before it becomes explicitly blocked.
func ReportRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := 30 * time.Second * time.Duration(1<<min(attempt-1, 7))
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

func PreOutboxRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := StatusPollInterval * time.Duration(1<<min(attempt-1, 5))
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func RunOne(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, broker BrokerClient, now time.Time) (bool, error) {
	if store.ciPolicy.Validate() == nil {
		if expired, err := store.ExpireCIDeadline(ctx, now, store.ciPolicy.MaxAttempts); err != nil || expired {
			return expired, err
		}
	}
	if terminal, ok := reportingBroker(broker); ok && store.ciPolicy.Validate() == nil {
		if report, due, err := store.ClaimCIEscalation(ctx, now); err != nil || due {
			if err != nil {
				return due, err
			}
			result, err := terminal.Comment(ctx, report.Job, report.Body, report.OperationKey)
			if err == nil {
				return true, store.MarkCIEscalationDelivered(ctx, report, result, now)
			}
			return true, store.MarkCIEscalationFailure(ctx, report, IsRetryable(err), safeBrokerError(err), now)
		}
	}
	if repair, ok := broker.(CIRepairBroker); ok && store.ciPolicy.Validate() == nil {
		if work, due, err := store.ClaimCIReconciliation(ctx, now); err != nil || due {
			if err != nil {
				return due, err
			}
			operationCtx, cancel := context.WithTimeout(ctx, work.Task.DeadlineAt.Sub(now))
			observation, observeErr := repair.ObserveCI(operationCtx, work.Task, work.HeadSHA)
			cancel()
			if observeErr != nil {
				return true, store.FailCIReconciliation(ctx, work, observeErr, IsRetryable(observeErr), now)
			}
			return true, store.ApplyCIObservation(ctx, work, observation, store.ciPolicy.MaxAttempts, now)
		}
		if task, attempt, due, err := store.ClaimCIRepair(ctx, now); err != nil || due {
			if err != nil {
				return due, err
			}
			operationCtx, cancel := context.WithTimeout(ctx, task.DeadlineAt.Sub(now))
			result, launchErr := repair.LaunchRepair(operationCtx, task, attempt, now)
			cancel()
			if launchErr == nil {
				return true, store.BindCIRepairRun(ctx, attempt, result.RunID, now)
			}
			retryDue := now.Add(LaunchRetryDelay(attempt.Operations + 1))
			if retryDue.After(task.DeadlineAt) {
				retryDue = task.DeadlineAt
			}
			if IsRetryable(launchErr) && attempt.Operations+1 < PreOutboxMaxAttempts && retryDue.After(now) {
				return true, store.DeferCIRepairLaunch(ctx, attempt.AttemptKey, retryDue, launchErr, now)
			}
			return true, store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, safeBrokerError(launchErr), store.ciPolicy.MaxAttempts, now)
		}
		if task, attempt, due, err := store.ClaimCIRepairStatus(ctx, now); err != nil || due {
			if err != nil {
				return due, err
			}
			return runCIRepairStatus(ctx, store, repair, task, attempt, now)
		}
	}
	if terminal, ok := reportingBroker(broker); ok {
		if report, due, err := store.ClaimReportDue(ctx, now); err != nil || due {
			if err != nil {
				return due, err
			}
			result, err := terminal.Comment(ctx, report.Job, report.Body, report.IdempotencyKey)
			if err == nil {
				return true, store.MarkReportDelivered(ctx, report, result.ID, result.URL, now)
			}
			retry := IsRetryable(err)
			safe := safeBrokerError(err)
			if retry && report.Attempts+1 >= ReportRetryMaxAttempts {
				retry = false
				safe = "terminal issue comment delivery retry limit exhausted: " + safe
			}
			return true, store.MarkReportFailure(ctx, report, retry, safe, now)
		}
	}
	work, ok, err := store.ClaimDue(ctx, now)
	if err != nil || !ok {
		return ok, err
	}
	job := work.Job
	if work.Kind == WorkStatus {
		if job.Status == StateReportPending {
			if _, ok := reportingBroker(broker); !ok {
				return true, store.DeferReportPending(ctx, job.ID, now.Add(ReporterUnavailableDelay), now)
			}
		}
		return runStatus(ctx, logger, metrics, store, broker, job, now)
	}
	if !job.FirstAttemptAt.IsZero() && !now.Before(job.FirstAttemptAt.Add(LaunchRetryWindow)) {
		metrics.retries.Inc()
		metrics.terminals.WithLabelValues(StateFailed).Inc()
		err := store.QueueLaunchFailure(ctx, job, "launch retry window exhausted", now)
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
	var storeErr error
	if retry {
		storeErr = store.MarkLaunchFailure(ctx, job.ID, true, due, safeBrokerError(err), now)
	} else {
		storeErr = store.QueueLaunchFailure(ctx, job, safeLaunchFailureReason(err), now)
	}
	_ = metrics.Refresh(ctx, store, now)
	return true, storeErr
}

func runCIRepairStatus(ctx context.Context, store *Store, broker CIRepairBroker, task CIRepairTask, attempt CIRepairAttempt, now time.Time) (bool, error) {
	operationCtx, cancel := context.WithTimeout(ctx, task.DeadlineAt.Sub(now))
	status, err := broker.Status(operationCtx, attempt.BrokerRunID)
	cancel()
	if err != nil {
		return true, deferOrFailCIRepairStatus(ctx, store, task, attempt, err, now)
	}
	if status.RunID != attempt.BrokerRunID {
		return true, store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, "broker repair status run correlation is invalid", store.ciPolicy.MaxAttempts, now)
	}
	state, err := ReconciledStatus(status.Status)
	if err != nil {
		return true, store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, safeBrokerError(err), store.ciPolicy.MaxAttempts, now)
	}
	if state == StateLaunched {
		due := now.Add(StatusPollInterval)
		if due.After(task.DeadlineAt) {
			due = task.DeadlineAt
		}
		if !due.After(now) {
			_, expireErr := store.ExpireCIDeadline(ctx, now, store.ciPolicy.MaxAttempts)
			return true, expireErr
		}
		return true, store.ScheduleCIRepairStatus(ctx, attempt.AttemptKey, due, now)
	}
	operationCtx, cancel = context.WithTimeout(ctx, task.DeadlineAt.Sub(now))
	terminal, err := broker.TerminalResult(operationCtx, attempt.BrokerRunID)
	cancel()
	if err != nil {
		return true, deferOrFailCIRepairStatus(ctx, store, task, attempt, err, now)
	}
	if err := validateCIRepairTerminal(task, attempt, terminal); err != nil {
		return true, store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, err.Error(), store.ciPolicy.MaxAttempts, now)
	}
	if terminal.Outcome != "ready_for_review" {
		if terminal.Outcome == "no_change_required" {
			return true, store.FinishCIRepairFailure(ctx, attempt, "model_or_code", terminal.ModelExecutionStarted, "repair execution produced no candidate head", store.ciPolicy.MaxAttempts, now)
		}
		detail := strings.TrimSpace(terminal.FailureReason)
		if detail == "" {
			detail = "repair execution failed"
		}
		return true, store.FinishCIRepairFailure(ctx, attempt, terminal.FailureClass, terminal.ModelExecutionStarted, detail, store.ciPolicy.MaxAttempts, now)
	}
	if !terminal.ModelExecutionStarted {
		return true, store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, "successful repair terminal lacks model execution proof", store.ciPolicy.MaxAttempts, now)
	}
	encoded, _ := json.Marshal(terminal.Result)
	var worker workerTerminalResult
	if err := json.Unmarshal(encoded, &worker); err != nil {
		return true, store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, "repair terminal result is malformed", store.ciPolicy.MaxAttempts, now)
	}
	if err := store.ChargeCIRepairAttempt(ctx, attempt.AttemptKey, now); err != nil {
		return true, err
	}
	return true, store.CompleteCIRepairDelivery(ctx, attempt.AttemptKey, CIRepairDelivery{
		ExpectedOldHeadSHA: worker.ExpectedOldHeadSHA, CandidateHeadSHA: worker.CandidateHeadSHA,
		DeliveredHeadSHA: worker.DeliveredHeadSHA, ValidatedTreeSHA: worker.ValidatedTreeSHA,
		DeliveredTreeSHA: worker.DeliveredTreeSHA,
	}, now)
}

func deferOrFailCIRepairStatus(ctx context.Context, store *Store, task CIRepairTask, attempt CIRepairAttempt, failure error, now time.Time) error {
	due := now.Add(PreOutboxRetryDelay(attempt.Operations + 1))
	if due.After(task.DeadlineAt) {
		due = task.DeadlineAt
	}
	if IsRetryable(failure) && attempt.Operations+1 < PreOutboxMaxAttempts && due.After(now) {
		return store.DeferCIRepairStatus(ctx, attempt.AttemptKey, due, failure, now)
	}
	return store.FinishCIRepairFailure(ctx, attempt, "infrastructure", false, safeBrokerError(failure), store.ciPolicy.MaxAttempts, now)
}

func validateCIRepairTerminal(task CIRepairTask, attempt CIRepairAttempt, terminal TerminalResult) error {
	job := Job{BrokerRunID: attempt.BrokerRunID, Profile: attempt.AgentModel, Repository: task.Repository}
	if terminal.Version != terminalResultVersion || terminal.RunID != job.BrokerRunID || terminal.Profile != job.Profile || terminal.Repo != job.Repository || !terminalOutcomeMatchesStatus(terminal.Status, terminal.Outcome, terminal.FailureStage) {
		return errors.New("repair terminal result correlation is invalid")
	}
	if terminal.Outcome == "ready_for_review" {
		encoded, err := json.Marshal(terminal.Result)
		if err != nil {
			return errors.New("repair terminal result is invalid")
		}
		var worker workerTerminalResult
		if json.Unmarshal(encoded, &worker) != nil || worker.PullRequest == nil || worker.PullRequest.Number != task.PullNumber || worker.Branch != task.Branch ||
			!githubSHA.MatchString(worker.ExpectedOldHeadSHA) || !githubSHA.MatchString(worker.CandidateHeadSHA) || !githubSHA.MatchString(worker.DeliveredHeadSHA) || !githubSHA.MatchString(worker.ValidatedTreeSHA) || !githubSHA.MatchString(worker.DeliveredTreeSHA) || worker.DeliveredTreeSHA != worker.ValidatedTreeSHA {
			return errors.New("repair terminal lacks exact delivery provenance")
		}
		return nil
	}
	if terminal.Outcome == "no_change_required" {
		return nil
	}
	switch terminal.FailureClass {
	case "infrastructure", "model_or_code", "delivery_or_lease":
		return nil
	default:
		return errors.New("repair terminal failure_class is invalid")
	}
}

func safeLaunchFailureReason(err error) string {
	var brokerErr BrokerError
	if !errors.As(err, &brokerErr) {
		return "sandbox broker launch failed before acknowledging a run"
	}
	switch {
	case brokerErr.Transport:
		return "sandbox broker transport failed before acknowledging a run"
	case brokerErr.Status != 0 && brokerErr.Code != "":
		return fmt.Sprintf("sandbox broker rejected the launch with HTTP %d (%s)", brokerErr.Status, brokerErr.Code)
	case brokerErr.Status != 0:
		return fmt.Sprintf("sandbox broker rejected the launch with HTTP %d", brokerErr.Status)
	case brokerErr.Malformed:
		return "sandbox broker returned an invalid launch response"
	default:
		return "sandbox broker launch failed before acknowledging a run"
	}
}

func runStatus(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, broker BrokerClient, job Job, now time.Time) (bool, error) {
	result, err := broker.Status(ctx, job.BrokerRunID)
	if err != nil {
		state, storeErr := ReconcileStatusFailure(ctx, store, job, err, now)
		if state == StateLaunched {
			metrics.jobs.WithLabelValues("status_retry").Inc()
		} else {
			metrics.terminals.WithLabelValues(StateFailed).Inc()
		}
		_ = metrics.Refresh(ctx, store, now)
		return true, storeErr
	}
	terminal, _ := broker.(TerminalProjectionClient)
	state, err := ReconcileStatusResult(ctx, store, terminal, job, result, now)
	if state == StateLaunched {
		metrics.jobs.WithLabelValues("status_nonterminal").Inc()
	} else if err == nil {
		metrics.terminals.WithLabelValues(state).Inc()
		logger.Info("broker run reached terminal state", "job_id", job.ID, "broker_run_id", job.BrokerRunID, "outcome", state)
	}
	_ = metrics.Refresh(ctx, store, now)
	return true, err
}

// ReconcileStatusFailure keeps status-fetch failures durable and bounded. A
// permanent failure, or exhaustion of transient retries, is itself projected
// through the terminal result/outbox path so it cannot look successful.
func ReconcileStatusFailure(ctx context.Context, store *Store, job Job, failure error, now time.Time) (string, error) {
	return reconcilePreOutboxFailure(ctx, store, job, StateLaunched, "broker_status", failure, IsRetryable(failure), now)
}

// ReconcileStatusResult is shared by the live loop and restored-job recovery.
// It is idempotent because QueueTerminalResult validates the immutable row and
// uses one terminal result and one outbox row per semantic job.
func ReconcileStatusResult(ctx context.Context, store *Store, terminal TerminalProjectionClient, job Job, result RunStatus, now time.Time) (string, error) {
	if result.RunID == "" || result.RunID != job.BrokerRunID {
		return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "broker_status", errors.New("broker status response has invalid run correlation"), false, now)
	}
	state, err := ReconciledStatus(result.Status)
	if err != nil {
		return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "broker_status", err, false, now)
	}
	if state == StateLaunched {
		return state, store.MarkStatus(ctx, job.ID, state, now.Add(StatusPollInterval), "", now)
	}
	if terminal == nil {
		return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "terminal_projection", errors.New("broker does not provide terminal projection"), false, now)
	}
	projected, err := terminal.TerminalResult(ctx, job.BrokerRunID)
	if err != nil {
		return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "terminal_projection", err, IsRetryable(err), now)
	}
	if err := ValidateTerminalResult(job, projected); err != nil {
		return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "terminal_correlation", err, false, now)
	}
	if _, err := RenderTerminalComment(job, projected); err != nil {
		return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "terminal_rendering", err, false, now)
	}
	if projected.Outcome == "ready_for_review" && store.ciPolicy.Validate() == nil {
		encoded, err := json.Marshal(projected)
		if err != nil {
			return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "ci_wait_serialization", err, false, now)
		}
		var worker workerTerminalResult
		workerBytes, _ := json.Marshal(projected.Result)
		if err := json.Unmarshal(workerBytes, &worker); err != nil || worker.PullRequest == nil {
			return reconcilePreOutboxFailure(ctx, store, job, StateReportPending, "ci_wait_correlation", errors.New("ready result lacks durable pull request coordinates"), false, now)
		}
		if err := store.BeginCIWait(ctx, job, worker.PullRequest.Number, projected.Branch, worker.DeliveredHeadSHA, projected.Profile, now.Add(store.ciPolicy.Deadline), now, encoded); err != nil {
			return "", err
		}
		return StateCIWaiting, nil
	}
	if err := store.QueueTerminalResult(ctx, job, projected, now); err != nil {
		return "", err
	}
	return StateReportPending, nil
}

func isCIWakeEvent(event string) bool {
	switch event {
	case "check_run", "check_suite", "status", "pull_request":
		return true
	default:
		return false
	}
}

func reconcilePreOutboxFailure(ctx context.Context, store *Store, job Job, recoverableState, stage string, failure error, retryable bool, now time.Time) (string, error) {
	safe := safeBrokerError(failure)
	attempts, err := store.MarkPreOutboxFailure(ctx, job.ID, recoverableState, now.Add(PreOutboxRetryDelay(job.PreOutboxAttempts+1)), safe, now)
	if err != nil {
		return "", err
	}
	if retryable && attempts < PreOutboxMaxAttempts {
		return recoverableState, nil
	}
	if retryable {
		safe = "pre-outbox terminal reconciliation retry limit exhausted: " + safe
	}
	if err := store.QueuePreOutboxFailure(ctx, job, stage, safe, now); err != nil {
		return "", err
	}
	return StateReportPending, nil
}

func safeBrokerError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	text = strings.ToValidUTF8(text, "\uFFFD")
	if len(text) > 512 {
		text = text[:512]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}

func reportingBroker(b BrokerClient) (TerminalBroker, bool) {
	terminal, ok := b.(*Broker)
	return terminal, ok && terminal.ReporterURL != ""
}

// ReconciledStatus validates the broker's bounded lifecycle vocabulary. The
// recovery workflow uses this before mutating a restored job.
func ReconciledStatus(status string) (string, error) {
	switch status {
	case "accepted", "queued", "pending", "launching", "running", "in_progress":
		return StateLaunched, nil
	case "completed", "succeeded", "success":
		return StateCompleted, nil
	case "failed", "error":
		return StateFailed, nil
	case "timed_out", "timeout":
		return StateTimedOut, nil
	case "stopped":
		return StateStopped, nil
	case "cancelled", "canceled":
		return StateCancelled, nil
	default:
		return "", BrokerError{Malformed: true, Message: "broker returned unknown run status"}
	}
}
