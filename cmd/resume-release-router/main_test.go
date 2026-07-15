package main

import (
	"github.com/grubbyhacker/signal-plane/internal/config"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestDisabledStandbyInitializesLedgerWithoutSecrets(t *testing.T) {
	handler, err := disabledHandler(config.WorkRouterConfig{DatabasePath: filepath.Join(t.TempDir(), "ledger.db")})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		path   string
		status int
	}{{"/healthz", http.StatusOK}, {"/readyz", http.StatusServiceUnavailable}} {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status {
			t.Fatalf("%s status=%d", test.path, response.Code)
		}
	}
}
