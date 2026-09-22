package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"

	"github.com/grubbyhacker/signal-plane/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load(envDefault("SIGNAL_GATEWAY_CONFIG", "configs/example.yaml"))
	if err != nil {
		logger.Error("load config failed", "error", err)
		os.Exit(1)
	}
	// Single-writer invariant: github-task-dispatcher is now the SOLE process
	// that opens the operational work-ledger database writable. This router
	// therefore no longer opens the ledger, activates routes, or runs the
	// release executor loop against it — those write paths moved into the
	// dispatcher's single Store handle. Its release-upload executor relocation
	// into the dispatcher worker is the explicit follow-up cutover; until then
	// this binary is a health-only standby that opens NOTHING (not even a
	// read-only handle), so two processes can never contend on the shared 0640
	// SQLite files — the exact failure the standalone-writer topology hit.
	if err := runStandby(cfg.WorkRouter, logger); err != nil {
		logger.Error("resume release router standby failed", "error", err)
		os.Exit(1)
	}
}

func runStandby(cfg config.WorkRouterConfig, logger *slog.Logger) error {
	logger.Info("resume release router in single-writer standby; the ledger is owned by github-task-dispatcher", "addr", cfg.Addr)
	return http.ListenAndServe(cfg.Addr, standbyHandler())
}

// standbyHandler serves health only and opens no database handle. readyz
// reports 503 because this binary intentionally does no work under the
// single-writer topology.
func standbyHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "standby_single_writer"})
	})
	return mux
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
