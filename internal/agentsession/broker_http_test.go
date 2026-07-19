package agentsession

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
		case "/v1/authority-workers/coordinator/v2/leases":
			var got brokerAcquireV2Request
			if err := decodeStrict(mustReadBody(t, request), &got); err != nil || got.Version != brokerCoordinatorV2Version || got.SessionBinding != "session:work-1" || got.AdmissionTaskDigest == "" {
				t.Fatalf("v2 request=%+v err=%v", got, err)
			}
			writer.Write(v2LeaseFixture(t, leaseFixture))
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
	lease, err := broker.Acquire(t.Context(), testAcquireRequest(t))
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
	if _, err := broker.Acquire(t.Context(), testAcquireRequest(t)); err == nil {
		t.Fatal("unknown broker field accepted")
	}
}

func TestHTTPBrokerUsesOnlySupportedSessionLifecycleOperations(t *testing.T) {
	leaseFixture := mustFixture(t, "lease-v1.json")
	var admission struct {
		Admission struct {
			Lease json.RawMessage `json:"lease"`
		} `json:"admission"`
	}
	if err := json.Unmarshal(leaseFixture, &admission); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seen[request.URL.Path] = true
		result := any(map[string]any{
			"version": "agentd/v1", "sessionId": "session-1", "coordinatorBinding": "logical-session", "authorityBinding": "writer",
			"workerId": "worker-1", "storageLineageId": "22222222222222222222222222222222", "fenceEpoch": 1,
			"sessionLineageId": "11111111111111111111111111111111", "workspace": map[string]any{"workspaceRef": "/workspace", "uid": 20000, "gid": 20000},
			"phase": "active", "turnIds": []string{"turn-1"}, "nextCursor": 2,
		})
		if request.URL.Path == "/v1/authority-workers/coordinator/v1/sessions/checkpoint" {
			result.(map[string]any)["workspace"].(map[string]any)["checkpointRef"] = "checkpoint-1"
		}
		if request.URL.Path == "/v1/authority-workers/coordinator/v1/sessions/cancel" {
			result = map[string]any{"sessionId": "session-1", "turnId": "turn-1", "phase": "cancelled"}
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"version": brokerCoordinatorVersion, "lease": admission.Admission.Lease, "result": result})
	}))
	defer server.Close()
	broker, _ := NewHTTPBroker(server.URL, "coordinator-token", server.Client())
	checkpoint, err := broker.Checkpoint(t.Context(), "logical-session", "checkpoint-1")
	if err != nil || checkpoint.Workspace.CheckpointRef != "checkpoint-1" {
		t.Fatalf("checkpoint=%+v err=%v", checkpoint, err)
	}
	if _, err := broker.Resume(t.Context(), "logical-session"); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Status(t.Context(), "logical-session"); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.Cancel(t.Context(), "logical-session", "turn-1"); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"checkpoint", "resume", "status", "cancel"} {
		if !seen["/v1/authority-workers/coordinator/v1/sessions/"+operation] {
			t.Fatalf("supported %s path was not used", operation)
		}
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

func testAcquireRequest(t *testing.T) AcquireRequest {
	t.Helper()
	task := RegisteredTask{Source: RegisteredTaskSource{WorkItemID: "work-1", RouteSnapshotID: "route-1"}, Snapshot: RegisteredTaskSnapshot{TaskKind: RepositoryChangeTaskKind, TaskVersion: "1.0.0", CompletionContract: RepositoryCompletionContract, VerifierID: RepositoryCompletionContract, ContractDigest: repositoryContractDigest, TaskEvidenceDigest: "sha256:" + strings.Repeat("a", 64), Parameters: []byte(`{"repositoryId":"neutral/pr10-proof","baseRevision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","branchRef":"agent/pr10-proof/test","validationSelection":"required"}`)}}
	digest, err := admissionTaskDigest(task.Source, task.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	task.Digest = digest
	return AcquireRequest{BindingKey: "session:work-1", AuthorityProfile: "writer", IdempotencyKey: "acquire-1", RegisteredTask: task}
}

func v2LeaseFixture(t *testing.T, v1 []byte) []byte {
	t.Helper()
	var response map[string]any
	if err := json.Unmarshal(v1, &response); err != nil {
		t.Fatal(err)
	}
	response["version"] = brokerCoordinatorV2Version
	value, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustReadBody(t *testing.T, request *http.Request) []byte {
	t.Helper()
	defer request.Body.Close()
	value, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
