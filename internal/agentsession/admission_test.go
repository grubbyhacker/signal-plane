package agentsession

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestAdmissionTaskDigestJCSVectorAndExactWire(t *testing.T) {
	request := testAcquireRequest(t)
	if request.RegisteredTask.Digest != "sha256:5f0d2a3038b8d130b2db0f1bd3fe1097089cbed6e985e4111d35d764c17ccdf6" {
		t.Fatalf("admission digest=%s", request.RegisteredTask.Digest)
	}
	wire, err := json.Marshal(brokerAcquireV2Request{Version: brokerCoordinatorV2Version, Profile: request.AuthorityProfile, IdempotencyKey: request.IdempotencyKey, SessionBinding: request.BindingKey, RegisteredTaskSource: request.RegisteredTask.Source, RegisteredTask: request.RegisteredTask.Snapshot, AdmissionTaskDigest: request.RegisteredTask.Digest})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"version":"broker/coordinator/v2","profile":"writer","idempotency_key":"acquire-1","session_binding":"session:work-1","registered_task_source":{"work_item_id":"work-1","route_snapshot_id":"route-1"},"registered_task":{"taskKind":"github_green_pr_v1","taskVersion":"1.0.0","completionContract":"github_green_pr_v1","verifierId":"github_green_pr_v1","contractDigest":"sha256:40963efb60fd00563bd6a33f1325b45008a917ebf17c110f9d3c86f7dd77d1fb","taskEvidenceDigest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","parameters":{"repository":"grubbyhacker/repository-worker-lifecycle-test","baseBranch":"main","branchRef":"agent/fleiglabs-repo-agent/test"}},"admission_task_digest":"sha256:5f0d2a3038b8d130b2db0f1bd3fe1097089cbed6e985e4111d35d764c17ccdf6"}`
	if string(wire) != want {
		t.Fatalf("wire=%s", wire)
	}
}

func TestRegisteredAdmissionFailsClosedBeforeBrokerForMalformedSnapshot(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	broker, err := NewHTTPBroker(server.URL, "coordinator-token", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	request := testAcquireRequest(t)
	request.RegisteredTask.Snapshot.TaskEvidenceDigest = "sha256:bad"
	if _, err := broker.Acquire(context.Background(), request); err == nil || calls != 0 {
		t.Fatalf("err=%v broker calls=%d", err, calls)
	}
}

func TestRegisteredAdmissionRejectsCallerSelectedAndBindingMismatch(t *testing.T) {
	request := testAcquireRequest(t)
	request.RegisteredTask.Snapshot.Parameters = []byte(`{"repository":"grubbyhacker/repository-worker-lifecycle-test","baseBranch":"main","branchRef":"agent/fleiglabs-repo-agent/test","prompt":"caller"}`)
	if err := request.RegisteredTask.Validate(request.BindingKey); err == nil {
		t.Fatal("caller-selected task field was accepted")
	}
	request = testAcquireRequest(t)
	request.BindingKey = "session:other-work"
	if err := request.RegisteredTask.Validate(request.BindingKey); err == nil {
		t.Fatal("mismatched session binding was accepted")
	}
}

func TestRegisteredAdmissionStableAcrossRetryAndStoreRestart(t *testing.T) {
	path := t.TempDir() + "/ledger.db"
	store, item, attempt, _ := coordinatorFixtureAt(t, path)
	t.Cleanup(func() { _ = store.Close() })
	ex := &Executor{Store: store}
	first, err := ex.registeredTask(context.Background(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := workledger.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	store = reopened
	ex.Store = store
	second, err := ex.registeredTask(context.Background(), workledger.ExecutorRequest{WorkItem: item, Attempt: attempt})
	if err != nil || !reflect.DeepEqual(first, second) || !strings.HasPrefix(first.Digest, "sha256:") {
		t.Fatalf("first=%+v second=%+v err=%v", first, second, err)
	}
}
