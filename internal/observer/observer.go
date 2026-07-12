// Package observer implements the deliberately small, durable signal observer.
package observer

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
	Metadata() (sequence uint64, deliveries uint64, err error)
	AckSync() error
	Term() error
}

type NATSDelivery struct{ Message *nats.Msg }

func (d NATSDelivery) Data() []byte { return d.Message.Data }
func (d NATSDelivery) Metadata() (uint64, uint64, error) {
	meta, err := d.Message.Metadata()
	if err != nil {
		return 0, 0, err
	}
	return meta.Sequence.Stream, meta.NumDelivered, nil
}
func (d NATSDelivery) AckSync() error { return d.Message.AckSync() }
func (d NATSDelivery) Term() error    { return d.Message.Term() }

type Metrics struct {
	registry      *prometheus.Registry
	messages      *prometheus.CounterVec
	redeliveries  prometheus.Counter
	lastAck       prometheus.Gauge
	readiness     prometheus.Gauge
	pending       prometheus.Gauge
	ackPending    prometheus.Gauge
	errors        *prometheus.CounterVec
	processing    prometheus.Histogram
	allowedRoutes map[string]struct{}
	mu            sync.RWMutex
	ready         bool
}

func NewMetrics(routes []string) *Metrics {
	allowed := make(map[string]struct{}, len(routes))
	for _, route := range routes {
		allowed[route] = struct{}{}
	}
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry, allowedRoutes: allowed,
		messages:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "signal_observer_messages_total", Help: "Observed messages by bounded source, route, and outcome."}, []string{"source", "route", "outcome"}),
		redeliveries: prometheus.NewCounter(prometheus.CounterOpts{Name: "signal_observer_redeliveries_total", Help: "Messages delivered more than once."}),
		lastAck:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "signal_observer_last_successful_ack_timestamp_seconds", Help: "Unix time of the last successful acknowledgement."}),
		readiness:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "signal_observer_readiness", Help: "Whether NATS and durable consumer are inspectable."}),
		pending:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "signal_observer_consumer_pending", Help: "Consumer pending messages."}),
		ackPending:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "signal_observer_consumer_ack_pending", Help: "Consumer acknowledgement-pending messages."}),
		errors:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "signal_observer_errors_total", Help: "Normalized observer errors."}, []string{"operation", "reason"}),
		processing:   prometheus.NewHistogram(prometheus.HistogramOpts{Name: "signal_observer_processing_duration_seconds", Help: "Observer processing and acknowledgement duration.", Buckets: prometheus.DefBuckets}),
	}
	registry.MustRegister(m.messages, m.redeliveries, m.lastAck, m.readiness, m.pending, m.ackPending, m.errors, m.processing, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "signal_observer_build_info", Help: "Build information for signal-observer.", ConstLabels: prometheus.Labels{"version": buildinfo.Version}}, func() float64 { return 1 }))
	return m
}

func (m *Metrics) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if m.readinessValue() {
			w.Header().Set("content-type", "application/json")
			_, _ = w.Write([]byte(`{"status":"ready"}\n`))
			return
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"not_ready"}\n`))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	return mux
}

// readinessValue is intentionally separate for testable readiness handling.
func (m *Metrics) readinessValue() bool { m.mu.RLock(); defer m.mu.RUnlock(); return m.ready }

// Process logs only metadata after a successful decode, then synchronously
// acknowledges it. Invalid JSON is terminally rejected to prevent a poison
// message from redelivering forever.
func Process(logger *slog.Logger, metrics *Metrics, delivery Delivery) bool {
	started := time.Now()
	defer func() { metrics.processing.Observe(time.Since(started).Seconds()) }()
	sequence, deliveries, metadataErr := delivery.Metadata()
	if metadataErr != nil {
		metrics.errors.WithLabelValues("metadata", "other").Inc()
	}
	if deliveries > 1 {
		metrics.redeliveries.Inc()
	}
	var signal envelope.Signal
	if err := json.Unmarshal(delivery.Data(), &signal); err != nil {
		metrics.messages.WithLabelValues("unknown", "unknown", "malformed").Inc()
		metrics.errors.WithLabelValues("decode", "invalid_json").Inc()
		logger.Warn("malformed signal; terminating delivery", "stream_sequence", sequence)
		if err := delivery.Term(); err != nil {
			metrics.errors.WithLabelValues("term", normalizedError(err)).Inc()
		}
		return false
	}
	logger.Info("received signal", "signal_id", signal.Meta.SignalID, "source", signal.Meta.Source, "route_id", signal.Meta.RouteID, "source_event", signal.Meta.SourceEvent, "source_action", signal.Meta.SourceAction, "source_delivery_id", signal.Meta.SourceDeliveryID, "stream_sequence", sequence)
	if err := delivery.AckSync(); err != nil {
		metrics.messages.WithLabelValues(metrics.source(signal.Meta.Source), metrics.route(signal.Meta.RouteID), "ack_failed").Inc()
		metrics.errors.WithLabelValues("ack", normalizedError(err)).Inc()
		logger.Error("acknowledge signal failed", "signal_id", signal.Meta.SignalID, "stream_sequence", sequence, "error", err)
		return false
	}
	metrics.messages.WithLabelValues(metrics.source(signal.Meta.Source), metrics.route(signal.Meta.RouteID), "acknowledged").Inc()
	metrics.lastAck.SetToCurrentTime()
	return true
}

func (m *Metrics) SetReady(ready bool, pending, ackPending uint64) {
	if ready {
		m.readiness.Set(1)
	} else {
		m.readiness.Set(0)
	}
	m.mu.Lock()
	m.ready = ready
	m.mu.Unlock()
	m.pending.Set(float64(pending))
	m.ackPending.Set(float64(ackPending))
}

func (m *Metrics) Error(operation string, err error) {
	m.errors.WithLabelValues(boundedOperation(operation), normalizedError(err)).Inc()
}

func (m *Metrics) source(value string) string {
	if value == "github" || value == "manual" {
		return value
	}
	return "other"
}
func (m *Metrics) route(value string) string {
	if _, ok := m.allowedRoutes[value]; ok {
		return value
	}
	return "other"
}
func normalizedError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}
func boundedOperation(value string) string {
	switch value {
	case "connect", "subscribe", "ready", "fetch", "metadata", "decode", "term", "ack":
		return value
	}
	return "other"
}
