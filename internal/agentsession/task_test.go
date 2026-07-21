package agentsession

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestGitHubGreenPRTaskIsRegisteredAndSnapshotted(t *testing.T) {
	store, err := workledger.Open(filepath.Join(t.TempDir(), "tasks.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := workledger.NewRegistry()
	if err := registry.Register(&Executor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterTask(GitHubGreenPRTask{}); err != nil {
		t.Fatal(err)
	}
	route := neutralTaskRoute()
	snapshot, err := store.ActivateRoute(context.Background(), route, registry, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := (GitHubGreenPRTask{}).Descriptor()
	if descriptor.Kind != "github_green_pr_v1" || descriptor.Version != "1.0.0" || descriptor.CompletionContract != "github_green_pr_v1" || descriptor.VerifierID != "github_green_pr_v1" || descriptor.ContractDigest != gitHubGreenPRDigest {
		t.Fatalf("descriptor does not match locked registered-task contract: %+v", descriptor)
	}
	if snapshot.TaskKind != GitHubGreenPRTaskKind ||
		snapshot.TaskVersion != descriptor.Version ||
		snapshot.CompletionContract != GitHubGreenPRContract ||
		snapshot.VerifierID != GitHubGreenPRContract ||
		snapshot.TaskContractDigest != descriptor.ContractDigest {
		t.Fatalf("task snapshot lost compiled contract: %+v", snapshot)
	}
	var parameters GitHubGreenPRParameters
	if err := json.Unmarshal(snapshot.TaskParameters, &parameters); err != nil ||
		parameters.Repository != GitHubGreenPRRepository || parameters.BaseBranch != "main" {
		t.Fatalf("task parameters=%+v err=%v", parameters, err)
	}
}

func TestGitHubGreenPRTaskRejectsUnregisteredAndCallerSelectedBehavior(t *testing.T) {
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
	if err := registry.RegisterTask(GitHubGreenPRTask{}); err != nil {
		t.Fatal(err)
	}
	route.Task.Parameters = json.RawMessage(`{"repository":"grubbyhacker/repository-worker-lifecycle-test","baseBranch":"main","branchRef":"agent/fleiglabs-repo-agent/test","verifier":"shell -c arbitrary"}`)
	if _, err := store.ActivateRoute(context.Background(), route, registry, time.Now()); err == nil {
		t.Fatal("caller-selected verifier field was accepted")
	}
	route = neutralTaskRoute()
	route.Admission.Namespaces = []string{"grubbyhacker/production"}
	if _, err := store.ActivateRoute(context.Background(), route, registry, time.Now()); err == nil {
		t.Fatal("generic or production route was accepted")
	}
}

func TestGitHubGreenPRContractDocumentMatchesLockedDigest(t *testing.T) {
	sum := sha256.Sum256([]byte(gitHubGreenPRDocument))
	if got := "sha256:" + hex.EncodeToString(sum[:]); got != gitHubGreenPRDigest {
		t.Fatalf("github green PR contract document digest=%s, want %s", got, gitHubGreenPRDigest)
	}
}

func TestGitHubGreenPRTaskRejectsPredecessorIdentifiers(t *testing.T) {
	for _, raw := range []string{
		`{"repository":"neutral/repository-proof","baseBranch":"main","branchRef":"agent/fleiglabs-repo-agent/test"}`,
		`{"repository":"grubbyhacker/repository-worker-lifecycle-test","baseBranch":"main","branchRef":"agent/repository-proof/test"}`,
	} {
		if _, err := (GitHubGreenPRTask{}).CanonicalizeParameters(json.RawMessage(raw)); err == nil {
			t.Fatalf("predecessor task parameters accepted: %s", raw)
		}
	}
}

func neutralTaskRoute() workledger.RouteDefinition {
	return workledger.RouteDefinition{
		ID:              "agent-session",
		SchemaVersion:   1,
		SemanticVersion: "1.0.0",
		ExecutorID:      ExecutorID,
		Task:            GitHubGreenPRTaskSelection("agent/fleiglabs-repo-agent/test"),
		Admission: workledger.AdmissionPolicy{
			Sources:     []string{"manual"},
			Namespaces:  []string{GitHubGreenPRRepository},
			ObjectKinds: []string{"repository_task"},
			Events:      []string{"repository_change"},
			Actions:     []string{"requested"},
		},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject},
		Retry:       workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}},
	}
}
