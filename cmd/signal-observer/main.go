package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/grubbyhacker/signal-plane/internal/observer"
	"github.com/nats-io/nats.go"
)

const (
	observerFetchWait = 2 * time.Second
	retryInitial      = time.Second
	retryMaximum      = 30 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	routeIDs := make([]string, 0, len(cfg.Routes))
	for _, route := range cfg.Routes {
		routeIDs = append(routeIDs, route.ID)
	}
	metrics := observer.NewMetrics(routeIDs)
	addr := envDefault("SIGNAL_OBSERVER_ADDR", ":8081")
	go func() {
		logger.Info("starting observer HTTP listener", "addr", addr)
		if err := http.ListenAndServe(addr, metrics.Handler()); err != nil {
			logger.Error("observer HTTP listener exited", "error", err)
		}
	}()

	subject := envDefault("SIGNAL_OBSERVER_SUBJECT", config.DefaultSubject)
	durable := envDefault("SIGNAL_OBSERVER_DURABLE", "signal-observer-local")
	once := os.Getenv("SIGNAL_OBSERVER_ONCE") == "true"
	logger.Info("starting signal-observer", "version", buildinfo.Version, "durable", durable, "once", once)

	var bus *eventbus.Bus
	var consumer *eventbus.Consumer
	backoff := retryInitial
	for {
		if bus == nil {
			bus, err = eventbus.Connect(cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Subjects)
			if err != nil {
				retry(logger, metrics, "connect", err, backoff)
				backoff = nextBackoff(backoff)
				continue
			}
		}
		if consumer == nil {
			consumer, err = bus.NewObserverConsumer(subject, durable)
			if err != nil {
				bus.Close()
				bus = nil
				retry(logger, metrics, "subscribe", err, backoff)
				backoff = nextBackoff(backoff)
				continue
			}
		}
		info, readyErr := consumer.Ready(nil)
		if readyErr != nil {
			metrics.SetReady(false, 0, 0)
			retry(logger, metrics, "ready", readyErr, backoff)
			backoff = nextBackoff(backoff)
			consumer = nil
			bus.Close()
			bus = nil
			continue
		}
		metrics.SetReady(true, info.NumPending, uint64(info.NumAckPending))
		backoff = retryInitial

		msg, err := consumer.Fetch(observerFetchWait)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			metrics.SetReady(false, 0, 0)
			retry(logger, metrics, "fetch", err, backoff)
			backoff = nextBackoff(backoff)
			consumer = nil
			bus.Close()
			bus = nil
			continue
		}
		if observer.Process(logger, metrics, observer.NATSDelivery{Message: msg}) && once {
			bus.Close()
			return
		}
	}
}

func retry(logger *slog.Logger, metrics *observer.Metrics, operation string, err error, delay time.Duration) {
	metricsError(metrics, operation, err)
	logger.Warn("observer dependency unavailable; retrying", "operation", operation, "delay", delay.String(), "error", err)
	time.Sleep(delay)
}
func metricsError(metrics *observer.Metrics, operation string, err error) {
	metrics.Error(operation, err)
}
func nextBackoff(value time.Duration) time.Duration {
	value *= 2
	if value > retryMaximum {
		return retryMaximum
	}
	return value
}
func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
