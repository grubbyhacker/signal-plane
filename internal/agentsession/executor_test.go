package agentsession

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestExecutorUsesFixedAuthorityProfileAndDurableBinding(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var leaseRequest struct {
		Profile        string `json:"profile"`
		IdempotencyKey string `json:"idempotency_key"`
		SessionBinding string `json:"session_binding"`
	}
	var createPath, authorization, expectedBinding string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v1/authority-workers/leases":
			if err := json.NewDecoder(request.Body).Decode(&leaseRequest); err != nil {
				t.Fatalf("decode lease request: %v", err)
			}
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"lease":{"worker_id":"authority-1"}}`))
		default:
			if request.URL.Path != "/v1/authority-workers/leases/"+expectedBinding+"/sessions" {
				t.Fatalf("unexpected request path %q", request.URL.Path)
			}
			createPath, authorization = request.URL.Path, request.Header.Get("Authorization")
			writer.Header().Set("Content-Type", "application/json")
			writer.WriteHeader(http.StatusCreated)
			_, _ = writer.Write([]byte(`{"sessionId":"session-1"}`))
		}
	}))
	defer server.Close()

	executor := &Executor{Store: store, BrokerURL: server.URL, Token: "coordinator-token", Client: server.Client()}
	registry := workledger.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "agent-session", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"example/widgets"}, ObjectKinds: []string{"pull_request"}, Events: []string{"pull_request"}, Actions: []string{"opened"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	event := workledger.Event{SignalID: "signal-1", SourceDeliveryID: "delivery-1", TransportStream: "signals", TransportSequence: 1, Source: "github", Namespace: "example/widgets", ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "opened", ActorClass: "user", SourceRevision: "abc", CorrelationID: "correlation-1", CausationID: "cause-1", PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals", ReceivedAt: now}
	if _, err := store.Admit(ctx, snapshot.ID, event, now); err != nil {
		t.Fatal(err)
	}
	item, attempt, ok, err := store.Claim(ctx, now)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	expectedBinding = "session:" + item.ID
	result, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != workledger.OutcomeWaiting || result.ExternalCorrelation != "session-1" {
		t.Fatalf("result=%+v", result)
	}
	if leaseRequest.Profile != authorityProfile || leaseRequest.IdempotencyKey != attempt.IdempotencyKey || leaseRequest.SessionBinding != expectedBinding {
		t.Fatalf("lease request=%+v", leaseRequest)
	}
	if createPath != "/v1/authority-workers/leases/"+expectedBinding+"/sessions" || authorization != "Bearer coordinator-token" {
		t.Fatalf("create path=%q authorization=%q", createPath, authorization)
	}
}
