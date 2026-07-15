package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/grubbyhacker/signal-plane/internal/resumeupload"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
	"github.com/nats-io/nats.go"
)

type delivery struct{ message *nats.Msg }

func (d delivery) Data() []byte { return d.message.Data }
func (d delivery) StreamSequence() (uint64, error) {
	metadata, err := d.message.Metadata()
	if err != nil {
		return 0, err
	}
	return metadata.Sequence.Stream, nil
}
func (d delivery) AckSync() error { return d.message.AckSync() }
func (d delivery) Term() error    { return d.message.Term() }
func (d delivery) NumDelivered() int {
	metadata, err := d.message.Metadata()
	if err != nil {
		return 1
	}
	return int(metadata.NumDelivered)
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	if !cfg.WorkRouter.Enabled {
		if err := runDisabledStandby(cfg.WorkRouter, logger); err != nil {
			logger.Error("resume release router disabled standby failed", "error", err)
			os.Exit(1)
		}
		return
	}
	wr := cfg.WorkRouter
	key, keyErr := os.ReadFile(wr.GitHubPrivateKeyPath)
	ykmConfig := resumeupload.YKMConfig{BaseURL: wr.YKMURL, AuthMode: resumeupload.YKMAuthMode(wr.YKMAuthMode), ClientID: os.Getenv(wr.YKMClientIDEnv), ClientSecret: os.Getenv(wr.YKMClientSecretEnv), LocalSecret: os.Getenv(wr.YKMLocalSecretEnv)}
	if keyErr != nil || len(key) == 0 || ykmConfig.Validate() != nil {
		logger.Error("work router secrets are unavailable")
		os.Exit(1)
	}
	store, err := workledger.Open(wr.DatabasePath)
	if err != nil {
		logger.Error("open ledger failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	github := &resumeupload.GitHubClient{PrivateKeyPEM: key}
	if err := github.ValidateCredentials(); err != nil {
		logger.Error("GitHub App private key is invalid", "error", err)
		os.Exit(1)
	}
	ykm := &resumeupload.YKMClient{Config: ykmConfig}
	executor := &resumeupload.Executor{Store: store, GitHub: github, YKM: ykm}
	registry := workledger.NewRegistry()
	if err := registry.Register(executor); err != nil {
		panic(err)
	}
	route := workledger.RouteDefinition{ID: "resume-builder-release-upload", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: resumeupload.ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{resumeupload.Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject, Supersede: false}, Retry: workledger.RetryPolicy{MaxAttempts: 5, Backoff: []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}}}
	if _, err := store.ActivateRoute(context.Background(), route, registry, time.Now().UTC()); err != nil {
		logger.Error("activate compiled route failed", "error", err)
		os.Exit(1)
	}
	router := &resumeupload.Router{Store: store, Registry: registry, GitHub: github, Stream: cfg.NATS.Stream}
	if err := router.Recover(context.Background()); err != nil {
		logger.Error("recover ledger failed", "error", err)
		os.Exit(1)
	}
	bus, err := eventbus.Connect(cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Subjects)
	if err != nil {
		logger.Error("connect bus failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()
	consumer, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: wr.Subject, Durable: wr.Durable, AckWait: 2 * time.Minute, MaxAckPending: 16, MaxDeliver: 5})
	if err != nil {
		logger.Error("create consumer failed", "error", err)
		os.Exit(1)
	}
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		})
		mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
			_, consumerErr := consumer.Ready(r.Context())
			if consumerErr != nil || store.Ready(r.Context()) != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "not_ready"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
		})
		if err := http.ListenAndServe(wr.Addr, mux); err != nil {
			logger.Error("work router HTTP listener exited", "error", err)
			os.Exit(1)
		}
	}()
	ctx := context.Background()
	go func() {
		for {
			worked, err := router.WorkOne(ctx)
			if err != nil {
				logger.Error("executor work failed", "error", err)
				time.Sleep(time.Second)
			} else if !worked {
				time.Sleep(250 * time.Millisecond)
			}
		}
	}()
	for {
		msg, err := consumer.Fetch(2 * time.Second)
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			logger.Error("fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		if err := router.Process(ctx, delivery{msg}); err != nil {
			logger.Error("delivery rejected or deferred", "error", err)
		}
	}
}

func runDisabledStandby(cfg config.WorkRouterConfig, logger *slog.Logger) error {
	handler, err := disabledHandler(cfg)
	if err != nil {
		return err
	}
	logger.Info("resume release router disabled; entering standby", "addr", cfg.Addr)
	return http.ListenAndServe(cfg.Addr, handler)
}
func disabledHandler(cfg config.WorkRouterConfig) (http.Handler, error) {
	store, err := workledger.Open(cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	if err := store.Ready(context.Background()); err != nil {
		store.Close()
		return nil, err
	}
	if err := store.Close(); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "disabled"})
	})
	return mux, nil
}
func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
