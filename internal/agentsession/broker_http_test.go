package agentsession

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestHTTPBrokerConsumesSharedLeaseAndReassignmentFixtures(t *testing.T) {
	leaseFixture := mustFixture(t, "lease-v1.json")
	statusFixture := mustFixture(t, "reassignment-status-v1.json")
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer coordinator-token" {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch request.URL.Path {
		case "/v1/authority-workers/coordinator/v1/leases":
			writer.Write(leaseFixture)
		case "/v1/authority-workers/coordinator/v1/reassignments/status":
			writer.Write(statusFixture)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	broker, err := NewHTTPBroker(server.URL, "coordinator-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := broker.Acquire(t.Context(), AcquireRequest{BindingKey: "logical-session", AuthorityProfile: "writer", IdempotencyKey: "acquire-1"})
	if err != nil || lease.SessionLineageID != "11111111111111111111111111111111" || lease.PolicyDigest != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("lease=%+v err=%v", lease, err)
	}
	status, err := broker.ReassignmentStatus(t.Context(), ReassignmentStatusRequest{BindingKey: "logical-session", PredecessorEpoch: 1})
	if err != nil || status.State != "confirmed" || status.Lease.WorkerID != "worker-2" || status.Lease.ProfileVersion != "profile-v1" || status.RebindIdempotencyKey != "opaque-rebind-key" {
		t.Fatalf("status=%+v err=%v", status, err)
	}
}

func TestHTTPBrokerRequiresContiguousAgentdEvents(t *testing.T) {
	leaseFixture := mustFixture(t, "lease-v1.json")
	var admission struct {
		Admission struct {
			Lease json.RawMessage `json:"lease"`
		} `json:"admission"`
	}
	if err := json.Unmarshal(leaseFixture, &admission); err != nil {
		t.Fatal(err)
	}
	gap := false
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		cursor := 1
		if gap {
			cursor = 2
		}
		result := []map[string]any{{"version": "agentd/v1", "cursor": cursor, "kind": "attempt_completed", "sessionId": "session-1", "turnId": "turn-1", "attemptId": "attempt-1", "payload": map[string]any{"conversation": map[string]any{"adapterKind": "codex", "adapterVersion": "v1", "backendThreadRef": "thread-1"}, "tokenUsage": map[string]any{"inputTokens": 3, "cachedInputTokens": 1, "outputTokens": 2, "reasoningOutputTokens": 1, "totalTokens": 5}}}}
		_ = json.NewEncoder(writer).Encode(map[string]any{"version": brokerCoordinatorVersion, "lease": admission.Admission.Lease, "result": result})
	}))
	defer server.Close()
	broker, _ := NewHTTPBroker(server.URL, "coordinator-token", server.Client())
	batch, err := broker.StreamEvents(t.Context(), StreamEventsRequest{BindingKey: "logical-session", Cursor: 0})
	if err != nil || len(batch.Events) != 1 || batch.Events[0].Usage.TotalTokens != 5 || batch.Events[0].EvidenceRef == "" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	gap = true
	if _, err := broker.StreamEvents(t.Context(), StreamEventsRequest{BindingKey: "logical-session", Cursor: 0}); err == nil {
		t.Fatal("event cursor gap was accepted")
	}
}

func TestHTTPBrokerRejectsUnknownWireFields(t *testing.T) {
	base := mustFixture(t, "lease-v1.json")
	fixture := append(append([]byte(nil), base[:len(base)-2]...), []byte(",\"caller_model\":\"forbidden\"}\n")...)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { writer.Write(fixture) }))
	defer server.Close()
	broker, _ := NewHTTPBroker(server.URL, "coordinator-token", server.Client())
	if _, err := broker.Acquire(t.Context(), AcquireRequest{BindingKey: "logical-session", AuthorityProfile: "writer", IdempotencyKey: "acquire-1"}); err == nil {
		t.Fatal("unknown broker field accepted")
	}
}

func mustFixture(t *testing.T, name string) []byte {
	t.Helper()
	value, err := os.ReadFile(filepath.Join("..", "..", "testdata", "coordinator-wire", name))
	if err != nil {
		t.Fatal(err)
	}
	return value
}
