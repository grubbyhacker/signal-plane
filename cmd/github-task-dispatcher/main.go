package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/nats-io/nats.go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	if !cfg.Dispatcher.Enabled {
		logger.Info("github-task-dispatcher disabled by configuration", "version", buildinfo.Version)
		return
	}
	token := os.Getenv(cfg.Dispatcher.BrokerTokenEnv)
	if token == "" {
		logger.Error("broker token is not set", "env", cfg.Dispatcher.BrokerTokenEnv)
		os.Exit(1)
	}
	store, err := dispatcher.OpenStore(cfg.Dispatcher.DatabasePath)
	if err != nil {
		logger.Error("open job store failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	metrics := dispatcher.NewMetrics()
	go func() {
		logger.Info("starting dispatcher HTTP listener", "addr", cfg.Dispatcher.Addr)
		if err := http.ListenAndServe(cfg.Dispatcher.Addr, metrics.Handler()); err != nil {
			logger.Error("dispatcher HTTP listener exited", "error", err)
		}
	}()
	bus, err := eventbus.Connect(cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Subjects)
	if err != nil {
		logger.Error("connect event bus failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()
	consumer, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: cfg.Dispatcher.Subject, Durable: cfg.Dispatcher.Durable, AckWait: 30 * time.Second, MaxAckPending: 64, MaxDeliver: 10})
	if err != nil {
		logger.Error("create dispatcher consumer failed", "error", err)
		os.Exit(1)
	}
	broker := &dispatcher.Broker{URL: cfg.Dispatcher.BrokerURL, Token: token, Client: &http.Client{Timeout: 30 * time.Second}}
	ctx := context.Background()
	metrics.SetReady(true)
	for i := 0; i < cfg.Dispatcher.Workers; i++ {
		go worker(ctx, logger, metrics, store, broker, cfg.Dispatcher.MaxAttempts)
	}
	logger.Info("starting github-task-dispatcher", "version", buildinfo.Version, "durable", cfg.Dispatcher.Durable, "workers", cfg.Dispatcher.Workers)
	for {
		msg, err := consumer.Fetch(2 * time.Second)
		if errors.Is(err, nats.ErrTimeout) {
			_, consumerErr := consumer.Ready(ctx)
			storeErr := store.Ready(ctx)
			metrics.SetReady(consumerErr == nil && storeErr == nil)
			continue
		}
		if err != nil {
			metrics.SetReady(false)
			logger.Error("fetch delivery failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		metrics.SetReady(true)
		dispatcher.Process(ctx, logger, metrics, store, dispatcher.NATSDelivery{Message: msg}, time.Now().UTC())
	}
}
func worker(ctx context.Context, logger *slog.Logger, metrics *dispatcher.Metrics, store *dispatcher.Store, broker *dispatcher.Broker, max int) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for {
				worked, err := dispatcher.RunOne(ctx, logger, metrics, store, broker, now.UTC(), max)
				if err != nil {
					logger.Error("run due job failed", "error", err)
					break
				}
				if !worked {
					break
				}
			}
		}
	}
}
func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
