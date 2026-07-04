package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("POST /manual", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		signal := envelope.Signal{
			Meta: envelope.Meta{
				SignalID:   envelope.NewSignalID(),
				Source:     "manual",
				RouteID:    "manual-local",
				ReceivedAt: time.Now().UTC(),
			},
			Payload: json.RawMessage(`{}`),
		}

		logger.Info("accepted signal", "signal_id", signal.Meta.SignalID, "source", signal.Meta.Source)
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(signal)
	})

	addr := envDefault("SIGNAL_GATEWAY_ADDR", ":8080")
	logger.Info("starting signal-gateway", "addr", addr, "version", buildinfo.Version)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("signal-gateway exited", "error", err)
		os.Exit(1)
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
