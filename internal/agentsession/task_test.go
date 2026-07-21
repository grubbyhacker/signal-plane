package agentsession

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
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

func TestGitHubGreenPRFixtureAdmissionIsIdempotentAndRejectsConflictingContent(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	route := GitHubGreenPRFixtureRoute()
	var parameters GitHubGreenPRParameters
	if err := json.Unmarshal(route.Task.Parameters, &parameters); err != nil || parameters.BaseBranch != "main" || parameters.BranchRef != GitHubGreenPRFixtureBranchRef {
		t.Fatalf("fixture route did not retain the fixed base and worker branch namespace: %+v err=%v", parameters, err)
	}
	store, err := workledger.Open(filepath.Join(t.TempDir(), "fixture.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry, err := RegisterGitHubGreenPRFixture(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := AdmitGitHubGreenPRFixture(ctx, store, registry, now)
	if err != nil || first.Duplicate {
		t.Fatalf("first admission=%+v err=%v", first, err)
	}
	duplicate, err := AdmitGitHubGreenPRFixture(ctx, store, registry, now.Add(24*time.Hour))
	if err != nil || !duplicate.Duplicate || duplicate.WorkItem.ID != first.WorkItem.ID {
		t.Fatalf("duplicate admission=%+v err=%v", duplicate, err)
	}
	fixture := gitHubGreenPRFixtureEvent(now)
	if fixture.ObjectID != GitHubGreenPRFixtureRepositoryID || fixture.SourceRevision != GitHubGreenPRFixtureSourceRevision || fixture.PayloadDigest != GitHubGreenPRFixtureTaskEvidenceDigest || fixture.EvidenceRef != gitHubGreenPRFixtureEvidenceRef {
		t.Fatalf("fixture is not bound to inspected coordinates: %+v", fixture)
	}
	for _, coordinate := range []string{GitHubGreenPRRepository, GitHubGreenPRFixtureRepositoryNodeID, GitHubGreenPRFixtureRepositoryID, GitHubGreenPRFixtureTaskPath, GitHubGreenPRFixtureSourceRevision, GitHubGreenPRFixtureTaskBlob} {
		if !strings.Contains(fixture.EvidenceRef, coordinate) {
			t.Fatalf("fixture evidence does not contain %q: %s", coordinate, fixture.EvidenceRef)
		}
	}
	registered, err := (&Executor{Store: store}).registeredTask(ctx, workledger.ExecutorRequest{WorkItem: first.WorkItem})
	if err != nil {
		t.Fatal(err)
	}
	wantRegisteredDigest, err := admissionTaskDigest(registered.Source, registered.Snapshot)
	if err != nil || registered.Digest != wantRegisteredDigest || registered.Digest == fixture.PayloadDigest {
		t.Fatalf("registered digest=%s want=%s task evidence=%s err=%v", registered.Digest, wantRegisteredDigest, fixture.PayloadDigest, err)
	}
	for name, mutate := range map[string]func(*workledger.Event){
		"repository ID":  func(event *workledger.Event) { event.ObjectID = "1307218522" },
		"revision":       func(event *workledger.Event) { event.SourceRevision = strings.Repeat("a", 40) },
		"payload digest": func(event *workledger.Event) { event.PayloadDigest = "sha256:" + strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			conflict := gitHubGreenPRFixtureEvent(now)
			mutate(&conflict)
			snapshot, err := store.MatchRoute(ctx, conflict)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Admit(ctx, snapshot.ID, conflict, now.Add(48*time.Hour)); err == nil {
				t.Fatal("fixture delivery with altered inspected coordinates was accepted")
			}
		})
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
