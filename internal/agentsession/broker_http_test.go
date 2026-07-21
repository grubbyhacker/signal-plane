package agentsession

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestRegisteredTurnGoldenContractIsStrict(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("..", "..", "testdata", "agentd", "registered-turn-v2.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Request  json.RawMessage            `json:"request"`
		Response map[string]json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(golden, &fixture); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/authority-workers/coordinator/v1/registered-turn" {
			http.NotFound(w, r)
			return
		}
		var got json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&got)
		var gotValue, wantValue any
		_ = json.Unmarshal(got, &gotValue)
		_ = json.Unmarshal(fixture.Request, &wantValue)
		if !reflect.DeepEqual(gotValue, wantValue) {
			t.Fatalf("request=%s want=%s", got, fixture.Request)
		}
		fixture.Response["lease"] = testLeaseJSON(t)
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(fixture.Response)
	}))
	defer server.Close()
	var request struct {
		Version             string          `json:"version"`
		IdempotencyKey      string          `json:"idempotencyKey"`
		TaskKind            string          `json:"taskKind"`
		AdmissionTaskDigest string          `json:"admissionTaskDigest"`
		TaskEvidenceDigest  string          `json:"taskEvidenceDigest"`
		Parameters          json.RawMessage `json:"parameters"`
	}
	_ = json.Unmarshal(fixture.Request, &request)
	broker, _ := NewHTTPBroker(server.URL, "token", server.Client())
	turn, err := broker.SubmitTurn(t.Context(), SubmitTurnRequest{Version: request.Version, IdempotencyKey: request.IdempotencyKey, TaskKind: request.TaskKind, AdmissionTaskDigest: request.AdmissionTaskDigest, TaskEvidenceDigest: request.TaskEvidenceDigest, Parameters: request.Parameters})
	if err != nil || turn.SessionID != "session-42" || turn.TurnID != "turn:turn-42" || turn.ModelEffectID != "model:turn-42" || turn.Cursor != 1 {
		t.Fatalf("turn=%+v err=%v", turn, err)
	}
}

func TestRegisteredTurnRejectsWrongVersionAndExtraFields(t *testing.T) {
	for _, body := range []string{`{"version":"agentd/registered-turn/v1","sessionId":"s","turnId":"t","modelEffectId":"model:k","phase":"queued","cursor":0}`, `{"version":"agentd/registered-turn/v2","sessionId":"s","turnId":"t","modelEffectId":"model:k","phase":"queued","cursor":0,"extra":true}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"lease":` + string(testLeaseJSON(t)) + `,` + body[1:]))
		}))
		broker, _ := NewHTTPBroker(server.URL, "token", server.Client())
		if _, err := broker.SubmitTurn(t.Context(), SubmitTurnRequest{Version: "agentd/registered-lifecycle/v1", IdempotencyKey: "k", TaskKind: "task", AdmissionTaskDigest: "sha256:a", TaskEvidenceDigest: "sha256:b", Parameters: []byte(`{}`)}); err == nil {
			t.Fatal("invalid response accepted")
		}
		server.Close()
	}
}

func TestRegisteredEventsAcceptPackageVerifierAndRejectLegacyMembers(t *testing.T) {
	golden, err := os.ReadFile(filepath.Join("..", "..", "testdata", "agentd", "registered-turn-v2.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Events map[string]json.RawMessage `json:"events"`
	}
	if err := json.Unmarshal(golden, &fixture); err != nil {
		t.Fatal(err)
	}
	fixture.Events["lease"] = testLeaseJSON(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/authority-workers/coordinator/v1/registered-events" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(fixture.Events)
	}))
	defer server.Close()
	broker, _ := NewHTTPBroker(server.URL, "token", server.Client())
	batch, err := broker.StreamEvents(t.Context(), StreamEventsRequest{BindingKey: "binding", Cursor: 0})
	if err != nil || len(batch.Events) != 6 || batch.Events[0].Phase != "queued" || batch.Events[1].Phase != "authorized" || batch.Events[2].Phase != "running" || batch.Events[3].Phase != "completed" || batch.Events[4].Verifier == nil || batch.Events[4].Verifier.Outcome != "waiting" || batch.Events[5].Verifier == nil || batch.Events[5].Verifier.Outcome != "satisfied" {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
	binding := workledger.SessionBinding{AgentdSessionID: "session-42", SubmittedTurnID: "turn:turn-42", ModelEffectID: "model:turn-42", WorkerID: "worker-42", WorkerStorageLineageID: "lineage-42", WorkerFenceEpoch: 7}
	task := RegisteredTask{Digest: "sha256:" + strings.Repeat("a", 64), Snapshot: RegisteredTaskSnapshot{TaskEvidenceDigest: "sha256:" + strings.Repeat("b", 64)}}
	for _, event := range batch.Events {
		if !validEvent(event, binding, task) {
			t.Fatalf("shared registered lifecycle fixture event was rejected: %+v", event)
		}
	}

	var invalid map[string]any
	encoded, _ := json.Marshal(fixture.Events)
	_ = json.Unmarshal(encoded, &invalid)
	events := invalid["events"].([]any)
	verifier := events[4].(map[string]any)["verifier"].(map[string]any)
	verifier["workItemId"] = "legacy-wire-member"
	invalidServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(invalid)
	}))
	defer invalidServer.Close()
	invalidBroker, _ := NewHTTPBroker(invalidServer.URL, "token", invalidServer.Client())
	if _, err := invalidBroker.StreamEvents(t.Context(), StreamEventsRequest{BindingKey: "binding", Cursor: 0}); err == nil {
		t.Fatal("legacy verifier member was accepted")
	}
}

func testLeaseJSON(t *testing.T) json.RawMessage {
	t.Helper()
	return json.RawMessage(`{"principal":"p","profile":"general-writer-v1","worker_id":"worker-42","session_lineage_id":"11111111111111111111111111111111","worker_storage_lineage_id":"22222222222222222222222222222222","worker_fence_epoch":7,"profile_version":"v1","policy_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","session_binding_digest":"x","idempotency_key_digest":"y","created_at":"now","released_at":"","replay":false}`)
}

func testAcquireRequest(t *testing.T) AcquireRequest {
	t.Helper()
	task := RegisteredTask{Source: RegisteredTaskSource{WorkItemID: "work-1", RouteSnapshotID: "route-1"}, Snapshot: RegisteredTaskSnapshot{TaskKind: GitHubGreenPRTaskKind, TaskVersion: "1.0.0", CompletionContract: GitHubGreenPRContract, VerifierID: GitHubGreenPRContract, ContractDigest: gitHubGreenPRDigest, TaskEvidenceDigest: "sha256:" + strings.Repeat("a", 64), Parameters: []byte(`{"repository":"grubbyhacker/repository-worker-lifecycle-test","baseBranch":"main","branchRef":"agent/fleiglabs-repo-agent/test"}`)}}
	digest, err := admissionTaskDigest(task.Source, task.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	task.Digest = digest
	return AcquireRequest{BindingKey: "session:work-1", AuthorityProfile: "writer", IdempotencyKey: "acquire-1", RegisteredTask: task}
}
