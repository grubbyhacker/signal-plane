package resumeupload

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
	"github.com/nats-io/nats.go"
)

// Service is the in-process Resume Builder release pipeline. It runs INSIDE the
// dispatcher process, against the dispatcher-owned work-ledger handle, so the
// release/published ingress and the YouKnowMe upload executor keep working
// under the single-writer topology without the standalone resume-release-router
// opening the database a second time.
//
// It owns two loops, both bound to the context passed to Run:
//   - ingress: pull release webhooks off NATS and AdmitRelease into the ledger
//     (Router.Process), idempotent by source_delivery_id.
//   - executor: claim admitted release work items and run the upload executor
//     (Router.WorkOne), idempotent by ContentResult digest.
//
// It opens NO database handle: the *workledger.Store is injected by the
// dispatcher (a non-owning Attach view over the one connection). Disabled by
// default: Build returns (nil, nil) when work_router.enabled is false.
type Service struct {
	router   *Router
	consumer *eventbus.Consumer
	logger   *slog.Logger
	interval time.Duration
}

// Deps are the process-level dependencies the dispatcher supplies. Store is the
// dispatcher's own attached work-ledger view; Bus is the shared NATS connection.
type Deps struct {
	Store  *workledger.Store
	Bus    *eventbus.Bus
	Stream string
	Logger *slog.Logger
}

// Build constructs the release pipeline from the work_router config, reading the
// GitHub App key and YouKnowMe secrets from the environment named in config. It
// activates the compiled resume-builder-release-upload route through the
// injected store (single-writer: the dispatcher owns the handle) and creates
// the NATS consumer. It does NOT run; the caller runs Serve.
//
// Build returns (nil, nil) when work_router.enabled is false — the release
// pipeline is simply not wired, which is a clean no-op.
func Build(ctx context.Context, cfg config.WorkRouterConfig, deps Deps) (*Service, error) {
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	if !cfg.Enabled {
		logger.Info("resume-release pipeline disabled; not wiring")
		return nil, nil
	}
	if deps.Store == nil {
		return nil, errors.New("resume-release pipeline requires a work-ledger store")
	}
	if deps.Bus == nil {
		return nil, errors.New("resume-release pipeline requires a NATS bus")
	}

	key, keyErr := os.ReadFile(cfg.GitHubPrivateKeyPath)
	ykmConfig := YKMConfig{
		BaseURL:      cfg.YKMURL,
		AuthMode:     YKMAuthMode(cfg.YKMAuthMode),
		ClientID:     os.Getenv(cfg.YKMClientIDEnv),
		ClientSecret: os.Getenv(cfg.YKMClientSecretEnv),
		LocalSecret:  os.Getenv(cfg.YKMLocalSecretEnv),
	}
	if keyErr != nil || len(key) == 0 || ykmConfig.Validate() != nil {
		return nil, errors.New("resume-release pipeline secrets are unavailable")
	}
	github := &GitHubClient{PrivateKeyPEM: key}
	if err := github.ValidateCredentials(); err != nil {
		return nil, fmt.Errorf("resume-release GitHub App private key is invalid: %w", err)
	}
	ykm := &YKMClient{Config: ykmConfig}
	executor := &Executor{Store: deps.Store, GitHub: github, YKM: ykm}
	registry := workledger.NewRegistry()
	if err := registry.Register(executor); err != nil {
		return nil, fmt.Errorf("register resume upload executor: %w", err)
	}
	route := workledger.RouteDefinition{
		ID: "resume-builder-release-upload", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: ExecutorID,
		Admission:   workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject, Supersede: false},
		Retry:       workledger.RetryPolicy{MaxAttempts: 5, Backoff: []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}},
	}
	if _, err := deps.Store.ActivateRoute(ctx, route, registry, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("activate resume-builder release route: %w", err)
	}
	router := &Router{Store: deps.Store, Registry: registry, GitHub: github, Stream: deps.Stream}
	if err := router.Recover(ctx); err != nil {
		return nil, fmt.Errorf("recover resume-release ledger: %w", err)
	}
	consumer, err := deps.Bus.NewConsumer(eventbus.ConsumerConfig{Subject: cfg.Subject, Durable: cfg.Durable, AckWait: 2 * time.Minute, MaxAckPending: 16, MaxDeliver: 5})
	if err != nil {
		return nil, fmt.Errorf("create resume-release consumer: %w", err)
	}
	return &Service{router: router, consumer: consumer, logger: logger, interval: 250 * time.Millisecond}, nil
}

// natsDelivery adapts a *nats.Msg to the Router's Delivery interface.
type natsDelivery struct{ message *nats.Msg }

func (d natsDelivery) Data() []byte { return d.message.Data }
func (d natsDelivery) StreamSequence() (uint64, error) {
	metadata, err := d.message.Metadata()
	if err != nil {
		return 0, err
	}
	return metadata.Sequence.Stream, nil
}
func (d natsDelivery) AckSync() error { return d.message.AckSync() }
func (d natsDelivery) Term() error    { return d.message.Term() }
func (d natsDelivery) NumDelivered() int {
	metadata, err := d.message.Metadata()
	if err != nil {
		return 1
	}
	return int(metadata.NumDelivered)
}

// Serve runs the ingress and executor loops until ctx is cancelled, then
// returns so the dispatcher's deferred cleanup runs. Both loops observe ctx.
func (service *Service) Serve(ctx context.Context) {
	done := make(chan struct{}, 2)
	go func() {
		defer func() { done <- struct{}{} }()
		service.runExecutorLoop(ctx)
	}()
	// Ingress loop: pull release webhooks and admit them.
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			if ctx.Err() != nil {
				return
			}
			msg, err := service.consumer.Fetch(2 * time.Second)
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			if err != nil {
				service.logger.Error("resume-release fetch failed", "error", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			if err := service.router.Process(ctx, natsDelivery{msg}); err != nil {
				service.logger.Error("resume-release delivery rejected or deferred", "error", err)
			}
		}
	}()
	<-ctx.Done()
	// Let both loops observe cancellation and return before Serve returns, so
	// no in-flight WorkOne/Process races the store handle the dispatcher is
	// about to close. Bounded so shutdown cannot hang on a stuck broker call.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-deadline.C:
			service.logger.Warn("resume-release loop did not stop within shutdown budget")
			return
		}
	}
}

// runExecutorLoop claims and runs admitted release work until ctx is cancelled,
// then returns. Between claims it checks ctx so a shutdown does not start new
// work; an in-flight WorkOne completes (bounded by the executor's own broker
// timeouts) before the loop returns.
func (service *Service) runExecutorLoop(ctx context.Context) {
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for {
				worked, err := service.router.WorkOne(ctx)
				if err != nil {
					service.logger.Error("resume-release executor work failed", "error", err)
					break
				}
				if !worked {
					break
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
	}
}
