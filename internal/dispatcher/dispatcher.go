package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
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
	AckSync() error
	Term() error
}
type NATSDelivery struct{ Message *nats.Msg }

func (d NATSDelivery) Data() []byte   { return d.Message.Data }
func (d NATSDelivery) AckSync() error { return d.Message.AckSync() }
func (d NATSDelivery) Term() error    { return d.Message.Term() }

type Metrics struct {
	registry   *prometheus.Registry
	deliveries *prometheus.CounterVec
	jobs       *prometheus.CounterVec
	ready      prometheus.Gauge
	mu         sync.RWMutex
	isReady    bool
}

func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	m := &Metrics{registry: r, deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "github_task_dispatcher_deliveries_total", Help: "Dispatcher deliveries by bounded outcome."}, []string{"outcome"}), jobs: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "github_task_dispatcher_jobs_total", Help: "Broker jobs by bounded outcome."}, []string{"outcome"}), ready: prometheus.NewGauge(prometheus.GaugeOpts{Name: "github_task_dispatcher_readiness", Help: "Whether dispatcher dependencies are ready."})}
	r.MustRegister(m.deliveries, m.jobs, m.ready, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "github_task_dispatcher_build_info", Help: "Build information.", ConstLabels: prometheus.Labels{"version": buildinfo.Version}}, func() float64 { return 1 }))
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
func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200); _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		m.mu.RLock()
		ready := m.isReady
		m.mu.RUnlock()
		w.Header().Set("content-type", "application/json")
		if !ready {
			w.WriteHeader(503)
			_, _ = w.Write([]byte("{\"error\":\"not_ready\"}\n"))
			return
		}
		_, _ = w.Write([]byte("{\"status\":\"ready\"}\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}

func Process(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, delivery Delivery, now time.Time) bool {
	var signal envelope.Signal
	if err := json.Unmarshal(delivery.Data(), &signal); err != nil {
		metrics.deliveries.WithLabelValues("malformed").Inc()
		logger.Warn("malformed signal; terminating delivery")
		_ = delivery.Term()
		return false
	}
	candidate, outcome := Select(signal)
	var selected *Candidate
	if outcome == "accepted" {
		selected = &candidate
	}
	if err := store.Record(ctx, signal.Meta.SourceDeliveryID, outcome, selected, now); err != nil {
		metrics.deliveries.WithLabelValues("store_failed").Inc()
		logger.Error("persist delivery failed", "error", err)
		return false
	}
	if err := delivery.AckSync(); err != nil {
		metrics.deliveries.WithLabelValues("ack_failed").Inc()
		logger.Error("acknowledge delivery failed", "error", err)
		return false
	}
	if selected != nil {
		metrics.deliveries.WithLabelValues("accepted").Inc()
	} else {
		metrics.deliveries.WithLabelValues("irrelevant").Inc()
	}
	return true
}

type Launcher interface {
	Launch(context.Context, Job) (LaunchResult, error)
}

func RunOne(ctx context.Context, logger *slog.Logger, metrics *Metrics, store *Store, launcher Launcher, now time.Time, maxAttempts int) (bool, error) {
	job, ok, err := store.ClaimDue(ctx, now)
	if err != nil || !ok {
		return ok, err
	}
	result, err := launcher.Launch(ctx, job)
	if err == nil {
		if err := store.Complete(ctx, job.ID, result.RunID, now); err != nil {
			return true, err
		}
		metrics.jobs.WithLabelValues("succeeded").Inc()
		logger.Info("broker launch accepted", "job_id", job.ID, "broker_run_id", result.RunID)
		return true, nil
	}
	var brokerErr BrokerError
	terminal := errors.As(err, &brokerErr) && brokerErr.Terminal()
	if job.Attempts >= maxAttempts {
		terminal = true
	}
	if terminal {
		metrics.jobs.WithLabelValues("terminal").Inc()
	} else {
		metrics.jobs.WithLabelValues("retry").Inc()
	}
	delay := time.Second << min(job.Attempts-1, 5)
	logger.Warn("broker launch failed", "job_id", job.ID, "terminal", terminal, "attempt", job.Attempts, "error", err)
	return true, store.Fail(ctx, job.ID, terminal, now.Add(delay), err.Error(), now)
}
