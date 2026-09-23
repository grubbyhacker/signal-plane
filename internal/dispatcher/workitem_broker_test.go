package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBrokerLaunchWorkItemContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/launch-profiles/ykm-curator-workitem-intake/launch" {
			t.Fatalf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer dispatcher-token" || r.Header.Get("Idempotency-Key") != "executor:work-123:digest:1" {
			t.Fatalf("headers = %#v", r.Header)
		}
		var body struct {
			Parameters map[string]any `json:"parameters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if len(body.Parameters) != 2 || body.Parameters["work_item_id"] != "work-123" {
			t.Fatalf("parameters = %#v", body.Parameters)
		}
		uploads, ok := body.Parameters["upload_ids"].([]any)
		if !ok || len(uploads) != 1 || uploads[0] != "upl_123" {
			t.Fatalf("upload_ids = %#v", body.Parameters["upload_ids"])
		}
		_, _ = w.Write([]byte(`{"version":"broker-run-launch/v1","run_id":"run-123"}`))
	}))
	defer server.Close()

	broker := &Broker{URL: server.URL, Token: "dispatcher-token", Client: server.Client()}
	result, err := broker.LaunchWorkItem(context.Background(), "ykm-curator-workitem-intake", "work-123", "upl_123", "executor:work-123:digest:1")
	if err != nil {
		t.Fatal(err)
	}
	if result.RunID != "run-123" {
		t.Fatalf("result = %#v", result)
	}
}
