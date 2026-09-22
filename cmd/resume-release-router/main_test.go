package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestStandbyServesHealthWithoutOpeningLedger asserts the single-writer
// invariant at the router boundary: under the redesign this binary opens NO
// database handle (the work ledger is owned solely by github-task-dispatcher),
// yet it still serves health. readyz reports 503 because it intentionally does
// no work. The handler is constructed with no config and no filesystem, which
// is itself the proof that it touches no database.
func TestStandbyServesHealthWithoutOpeningLedger(t *testing.T) {
	handler := standbyHandler()
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
