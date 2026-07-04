package main

import (
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
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

	bus, err := eventbus.Connect(cfg.NATS.URL, cfg.NATS.Stream, cfg.NATS.Subjects)
	if err != nil {
		logger.Error("connect event bus failed", "error", err)
		os.Exit(1)
	}
	defer bus.Close()

	subject := envDefault("SIGNAL_OBSERVER_SUBJECT", config.DefaultSubject)
	durable := envDefault("SIGNAL_OBSERVER_DURABLE", "signal-observer-local")
	once := os.Getenv("SIGNAL_OBSERVER_ONCE") == "true"

	logger.Info("starting signal-observer", "version", buildinfo.Version, "subject", subject, "durable", durable, "once", once)
	for {
		msg, err := bus.FetchOne(subject, durable, 2*time.Second)
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) {
				continue
			}
			logger.Error("observe failed", "error", err)
			os.Exit(1)
		}

		logger.Info(
			"received signal",
			"signal_id", msg.Signal.Meta.SignalID,
			"source", msg.Signal.Meta.Source,
			"route_id", msg.Signal.Meta.RouteID,
			"source_event", msg.Signal.Meta.SourceEvent,
			"source_action", msg.Signal.Meta.SourceAction,
			"source_delivery_id", msg.Signal.Meta.SourceDeliveryID,
			"subject", msg.Subject,
			"stream_sequence", msg.Sequence,
		)
		if once {
			return
		}
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
