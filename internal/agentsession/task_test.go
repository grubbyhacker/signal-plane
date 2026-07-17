package agentsession

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestNeutralRepositoryTaskIsRegisteredAndSnapshotted(t *testing.T) {
	store, err := workledger.Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := workledger.NewRegistry()
	if err := registry.Register(&Executor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterTask(RepositoryChangeTask{}); err != nil {
		t.Fatal(err)
	}
	route := neutralTaskRoute()
	snapshot, err := store.ActivateRoute(context.Background(), route, registry, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := (RepositoryChangeTask{}).Descriptor()
	if snapshot.TaskKind != RepositoryChangeTaskKind ||
		snapshot.TaskVersion != descriptor.Version ||
		snapshot.CompletionContract != RepositoryCompletionContract ||
		snapshot.VerifierID != RepositoryCompletionContract ||
		snapshot.TaskContractDigest != descriptor.ContractDigest {
		t.Fatalf("task snapshot lost compiled contract: %+v", snapshot)
	}
	var parameters RepositoryChangeParameters
	if err := json.Unmarshal(snapshot.TaskParameters, &parameters); err != nil ||
		parameters.RepositoryID != NeutralRepositoryID ||
		parameters.ValidationSelection != "required" {
		t.Fatalf("task parameters=%+v err=%v", parameters, err)
	}
}

func TestNeutralRepositoryTaskRejectsUnregisteredAndCallerSelectedBehavior(t *testing.T) {
	store, err := workledger.Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := workledger.NewRegistry()
	if err := registry.Register(&Executor{}); err != nil {
		t.Fatal(err)
	}
	route := neutralTaskRoute()
	if _, err := store.ActivateRoute(context.Background(), route, registry, time.Now()); err == nil {
		t.Fatal("unregistered task kind was accepted")
	}
	if err := registry.RegisterTask(RepositoryChangeTask{}); err != nil {
		t.Fatal(err)
	}
	route.Task.Parameters = json.RawMessage(`{"repositoryId":"neutral/pr10-proof","baseRevision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","branchRef":"agent/pr10-proof/test","validationSelection":"required","verifier":"shell -c arbitrary"}`)
	if _, err := store.ActivateRoute(context.Background(), route, registry, time.Now()); err == nil {
		t.Fatal("caller-selected verifier field was accepted")
	}
	route = neutralTaskRoute()
	route.Admission.Namespaces = []string{"grubbyhacker/production"}
	if _, err := store.ActivateRoute(context.Background(), route, registry, time.Now()); err == nil {
		t.Fatal("generic or production route was accepted")
	}
}

func neutralTaskRoute() workledger.RouteDefinition {
	return workledger.RouteDefinition{
		ID:              "agent-session",
		SchemaVersion:   1,
		SemanticVersion: "1.0.0",
		ExecutorID:      ExecutorID,
		Task:            NeutralRepositoryTaskSelection(strings.Repeat("a", 40), "agent/pr10-proof/test"),
		Admission: workledger.AdmissionPolicy{
			Sources:     []string{"manual"},
			Namespaces:  []string{NeutralRepositoryID},
			ObjectKinds: []string{"repository_task"},
			Events:      []string{"repository_change"},
			Actions:     []string{"requested"},
		},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject},
		Retry:       workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}},
	}
}
