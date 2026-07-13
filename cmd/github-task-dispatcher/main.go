package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
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
	if len(os.Args) > 1 && os.Args[1] == "recovery-metadata" {
		if err := runRecoveryMetadata(os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	metrics := dispatcher.NewMetrics()
	if !cfg.Dispatcher.Enabled {
		metrics.SetDisabled()
		logger.Info("github-task-dispatcher disabled; entering standby", "version", buildinfo.Version, "addr", cfg.Dispatcher.Addr)
		if err := http.ListenAndServe(cfg.Dispatcher.Addr, metrics.Handler()); err != nil {
			logger.Error("dispatcher standby HTTP listener exited", "error", err)
			os.Exit(1)
		}
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
	consumer, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: cfg.Dispatcher.Subject, Durable: cfg.Dispatcher.Durable, AckWait: 30 * time.Second, MaxAckPending: 64, MaxDeliver: 10, StartSequence: cfg.Dispatcher.RecoveryStartSequence})
	if err != nil {
		logger.Error("create dispatcher consumer failed", "error", err)
		os.Exit(1)
	}
	broker := &dispatcher.Broker{URL: cfg.Dispatcher.BrokerURL, Token: token, Client: &http.Client{Timeout: 30 * time.Second}}
	ctx := context.Background()
	metrics.SetReady(true)
	go worker(ctx, logger, metrics, store, broker)
	logger.Info("starting github-task-dispatcher", "version", buildinfo.Version, "durable", cfg.Dispatcher.Durable, "workers", 1)
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

func runRecoveryMetadata(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("recovery-metadata", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	database := flags.String("database", "", "dispatcher SQLite database path")
	if err := flags.Parse(args); err != nil {
		return fmt.Errorf("usage: github-task-dispatcher recovery-metadata --database PATH: %w", err)
	}
	if *database == "" || flags.NArg() != 0 {
		return errors.New("usage: github-task-dispatcher recovery-metadata --database PATH")
	}
	info, err := os.Stat(*database)
	if err != nil {
		return fmt.Errorf("stat dispatcher database: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("dispatcher database must be a regular file")
	}
	store, err := dispatcher.OpenStore(*database)
	if err != nil {
		return fmt.Errorf("open dispatcher database: %w", err)
	}
	defer store.Close()
	schema, checkpoint, start, err := store.RecoveryMetadata(context.Background())
	if err != nil {
		return fmt.Errorf("read recovery metadata: %w", err)
	}
	return json.NewEncoder(output).Encode(struct {
		SchemaVersion                  int    `json:"schema_version"`
		LastPersistedJetStreamSequence uint64 `json:"last_persisted_jetstream_sequence"`
		RecoveryStartSequence          uint64 `json:"recovery_start_sequence"`
	}{
		SchemaVersion:                  schema,
		LastPersistedJetStreamSequence: checkpoint,
		RecoveryStartSequence:          start,
	})
}
func worker(ctx context.Context, logger *slog.Logger, metrics *dispatcher.Metrics, store *dispatcher.Store, broker *dispatcher.Broker) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for {
				worked, err := dispatcher.RunOne(ctx, logger, metrics, store, broker, now.UTC())
				if err != nil {
					logger.Error("run due job failed", "error", err)
					break
				}
				if !worked {
					break
				}
			}
			if err := metrics.Refresh(ctx, store, now.UTC()); err != nil {
				logger.Error("refresh dispatcher metrics failed", "error", err)
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
