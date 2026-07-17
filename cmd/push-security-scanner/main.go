package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/grubbyhacker/signal-plane/internal/pushscan"
	"github.com/nats-io/nats.go"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	store, err := pushscan.OpenStore(cfg.PushScanner.DatabasePath)
	if err != nil {
		logger.Error("open push scanner store failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	if !cfg.PushScanner.Enabled {
		logger.Info("push security scanner disabled; entering standby", "addr", cfg.PushScanner.Addr)
		if err := http.ListenAndServe(cfg.PushScanner.Addr, standbyHandler(store)); err != nil {
			logger.Error("standby listener exited", "error", err)
			os.Exit(1)
		}
		return
	}
	token, key, holderToken := os.Getenv(cfg.PushScanner.BrokerTokenEnv), []byte(os.Getenv(cfg.PushScanner.FingerprintKeyEnv)), os.Getenv(cfg.PushScanner.HolderTokenEnv)
	bounds := cfg.PushScanner.Bounds
	scanBounds := pushscan.Bounds{MaxCommits: bounds.MaxCommits, MaxPaths: bounds.MaxPaths, MaxBlobBytes: bounds.MaxBlobBytes, MaxTotalBytes: bounds.MaxTotalBytes, MaxCandidates: bounds.MaxCandidates, MaxDecodeDepth: bounds.MaxDecodeDepth}
	broker, err := pushscan.NewHTTPBroker(cfg.PushScanner.BrokerURL, token, &http.Client{Timeout: 30 * time.Second}, scanBounds)
	if err != nil || len(key) < 32 || len(holderToken) < 32 || strings.ContainsAny(holderToken, "\r\n") {
		logger.Error("scanner secret inputs are unavailable")
		os.Exit(1)
	}
	bus, err := eventbus.Connect(cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Subjects)
	if err != nil {
		logger.Error("connect event bus failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()
	forensicRetention, _ := time.ParseDuration(cfg.PushScanner.ForensicRetention)
	go func() {
		if err := http.ListenAndServe(cfg.PushScanner.Addr, (pushscan.RegistryHandler{Store: store, Token: holderToken, ForensicRetention: forensicRetention}).Handler()); err != nil {
			logger.Error("push scanner registry listener exited", "error", err)
			os.Exit(1)
		}
	}()
	consumer, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: cfg.PushScanner.Subject, Durable: cfg.PushScanner.Durable, AckWait: pushscan.ConsumerAckWait, MaxAckPending: 16, MaxDeliver: 10})
	if err != nil {
		logger.Error("create push scanner consumer failed", "error", err)
		os.Exit(1)
	}
	canary := cfg.PushScanner.CanaryAttribution
	scanner := &pushscan.Scanner{
		Store: store, Broker: broker, EventSink: busEventSink{bus: bus, subject: cfg.PushScanner.EventSubject}, FingerprintKey: key,
		Repositories: cfg.PushScanner.Repositories, Refs: cfg.PushScanner.Refs, Profile: cfg.PushScanner.Profile, ProfileGeneration: cfg.PushScanner.ProfileGeneration,
		CanaryAttribution: pushscan.Attribution{Profile: cfg.PushScanner.Profile, ProfileGeneration: cfg.PushScanner.ProfileGeneration, LogicalSessionID: canary.LogicalSessionID, SessionLineageID: canary.SessionLineageID, WorkerID: canary.WorkerID, WorkerStorageLineage: canary.WorkerStorageLineage, WorkerFenceEpoch: canary.WorkerFenceEpoch},
		Bounds:            scanBounds,
	}
	ctx := context.Background()
	reconcileInterval, _ := time.ParseDuration(cfg.PushScanner.ReconcileInterval)
	fingerprintPruneInterval, _ := time.ParseDuration(cfg.PushScanner.FingerprintPruneInterval)
	maintenance := &pushscan.Maintenance{Scanner: scanner, ReconcileInterval: reconcileInterval, FingerprintPruneInterval: fingerprintPruneInterval}
	if err := maintenance.Startup(ctx); err != nil {
		logger.Error("push scanner startup maintenance incomplete; durable retry remains active", "error", err)
	}
	for {
		if err := maintenance.RunDue(ctx); err != nil {
			logger.Error("push scanner periodic maintenance incomplete; durable retry remains active", "error", err)
		}
		message, err := consumer.Fetch(2 * time.Second)
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			logger.Error("fetch push delivery failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		var signal envelope.Signal
		metadata, metadataErr := message.Metadata()
		if json.Unmarshal(message.Data, &signal) != nil || metadataErr != nil {
			_ = message.Term()
			continue
		}
		if signal.Meta.SourceEvent != "push" {
			_ = message.AckSync()
			continue
		}
		identity, err := pushscan.IdentityFromSignal(signal, metadata.Sequence.Stream)
		if err != nil {
			_ = message.Term()
			continue
		}
		result, processErr := scanner.Process(ctx, identity)
		if processErr != nil {
			if result.DeliveryID == "" {
				logger.Error("push scan failed before durable result", "delivery_id", identity.DeliveryID, "error", processErr)
				_ = message.NakWithDelay(scanRetryDelay(processErr))
				continue
			}
			logger.Error("push scan durable side effect pending reconciliation", "delivery_id", identity.DeliveryID, "error", processErr)
		}
		if err := message.AckSync(); err != nil {
			logger.Error("push delivery ack failed", "delivery_id", identity.DeliveryID, "error", err)
		}
	}
}

func scanRetryDelay(_ error) time.Duration {
	return 5 * time.Second
}

type busEventSink struct {
	bus     *eventbus.Bus
	subject string
}

func (sink busEventSink) Publish(_ context.Context, event pushscan.SecurityEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return sink.bus.Publish(sink.subject, envelope.Signal{Meta: envelope.Meta{
		SignalID: event.EventID, Source: "push-security-scanner", RouteID: "push-tripwire-alert", ReceivedAt: event.AlertRequestedAt,
		SourceEvent: "security_finding", SourceDeliveryID: event.EventID, Namespace: event.Repository, ObjectKind: "push_security_finding", ObjectID: event.FindingID,
		Authentication: envelope.Authentication{Method: "internal_durable_outbox", Verified: true},
	}, Payload: payload})
}

func standbyHandler(store *pushscan.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, request *http.Request) {
		if store.Ready(request.Context()) != nil {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte("{\"error\":\"disabled\"}\n"))
	})
	return mux
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
