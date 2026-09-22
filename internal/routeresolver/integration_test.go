package routeresolver_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/routeresolver"
	"github.com/grubbyhacker/signal-plane/internal/shadowadmit"
	"github.com/grubbyhacker/signal-plane/internal/shadowingress"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

type testExecutor struct{ descriptor workledger.ExecutorDescriptor }

func (e testExecutor) Descriptor() workledger.ExecutorDescriptor { return e.descriptor }
func (testExecutor) Execute(context.Context, workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	return workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted}, nil
}

// allowAll authorizes any peer (SO_PEERCRED is Linux-only; the integration test
// runs on dev machines too). It is injected via NewServerForTest-equivalent
// path: shadowingress exposes no setter here, so we rely on the Linux authorizer
// on Linux and skip the socket drive elsewhere.
func activateRoute(t *testing.T, store *workledger.Store, id, objectKind, eventKind, action string, now time.Time) workledger.RouteSnapshot {
	t.Helper()
	route := workledger.RouteDefinition{
		ID: id, SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: "youknowme.curator",
		Admission:   workledger.AdmissionPolicy{Sources: []string{"youknowme"}, Namespaces: []string{"grubbyhacker/youknowme"}, ObjectKinds: []string{objectKind}, Events: []string{eventKind}, Actions: []string{action}},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject},
		Retry:       workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}},
	}
	registry := workledger.NewRegistry()
	if err := registry.Register(testExecutor{descriptor: workledger.ExecutorDescriptor{ID: "youknowme.curator", Kind: workledger.ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(context.Background(), route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// TestResolverDrivesShadowAdmissionEndToEnd wires the deployment-owned resolver
// into the merged shadow ingress and admits a domain fact through it. It proves
// the resolver's routing (not caller input) reaches shadowadmit and persists.
func TestResolverDrivesShadowAdmissionEndToEnd(t *testing.T) {
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "e2e.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)

	intake := activateRoute(t, store, "ykm-upload-intake", "upload", "upload", "completed", now)
	reconcile := activateRoute(t, store, "ykm-reconcile", "youknowme", "youknowme", "reconciliation_due", now)

	cfg := routeresolver.Config{
		Version: 1,
		Routes: []routeresolver.RouteEntry{
			{Fact: "upload.completed", AgentType: "youknowme-curator", Mode: "process_intake", RouteSnapshotID: intake.ID, ContractRevision: "contract-v1"},
			{Fact: "youknowme.reconciliation_due", AgentType: "youknowme-curator", Mode: "reconcile", RouteSnapshotID: reconcile.ID, ContractRevision: "contract-v1"},
		},
	}
	resolver, err := routeresolver.New(cfg, routeresolver.YouKnowMeCuratorCatalog())
	if err != nil {
		t.Fatal(err)
	}

	shadow, err := shadowadmit.New(store, true)
	if err != nil {
		t.Fatal(err)
	}

	// Admit an upload.completed fact directly through the resolver + shadowadmit,
	// exactly as the ingress handler does, and assert the resolver's binding is
	// what persists. (The socket transport itself is covered by the ingress
	// package's own tests; here we prove the resolver wiring.)
	event := workledger.Event{
		SignalID: "s-1", SourceDeliveryID: "d-1", TransportStream: "signals", TransportSequence: 1,
		Source: "youknowme", Namespace: "grubbyhacker/youknowme", ObjectKind: "upload", ObjectID: "upl_1",
		EventKind: "upload", Action: "completed", SourceRevision: "rev-1",
		PayloadDigest: "sha256:payload", EvidenceRef: "unix://ingress", ReceivedAt: now,
	}
	resolution, err := resolver.Resolve(ctx, event)
	if err != nil || !resolution.Matched {
		t.Fatalf("resolve upload.completed = %#v err=%v", resolution, err)
	}
	outcome, err := shadow.Admit(ctx, shadowadmit.Candidate{
		RouteSnapshotID: resolution.RouteSnapshotID,
		Event:           event,
		Binding:         resolution.Binding.AdmissionBinding(),
	})
	if err != nil || outcome.Launched || outcome.WorkItemID == "" {
		t.Fatalf("admit = %#v err=%v", outcome, err)
	}
	item, err := store.WorkItem(ctx, outcome.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if item.AgentType != "youknowme-curator" || item.AgentMode != "process_intake" || item.RouteSnapshotID != intake.ID {
		t.Fatalf("persisted item did not carry the resolver's routing: %#v", item)
	}

	// An unknown fact resolves to no match and is never admitted.
	unknown := event
	unknown.EventKind = "pull_request"
	unknown.Action = "synchronize"
	if res, err := resolver.Resolve(ctx, unknown); err != nil || res.Matched {
		t.Fatalf("unknown fact resolved: %#v err=%v", res, err)
	}
}

// TestResolverSatisfiesIngressInterface is a compile-time + runtime check that
// the resolver is usable as shadowingress.RouteResolver, and that an enabled
// ingress accepts it. The socket is not driven here (SO_PEERCRED is Linux-only);
// the ingress package owns the peer-cred and socket round-trip tests.
func TestResolverSatisfiesIngressInterface(t *testing.T) {
	store, err := workledger.Open(filepath.Join(t.TempDir(), "iface.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	intake := activateRoute(t, store, "ykm-upload-intake", "upload", "upload", "completed", now)
	cfg := routeresolver.Config{Version: 1, Routes: []routeresolver.RouteEntry{
		{Fact: "upload.completed", AgentType: "youknowme-curator", Mode: "process_intake", RouteSnapshotID: intake.ID},
	}}
	resolver, err := routeresolver.New(cfg, routeresolver.YouKnowMeCuratorCatalog())
	if err != nil {
		t.Fatal(err)
	}
	shadow, err := shadowadmit.New(store, true)
	if err != nil {
		t.Fatal(err)
	}
	var _ shadowingress.RouteResolver = resolver
	socket := filepath.Join(shortDir(t), "i.sock")
	server, err := shadowingress.NewServer(shadowingress.Config{Enabled: true, SocketPath: socket}, shadow, resolver, nil)
	if err != nil {
		t.Fatalf("enabled ingress rejected the resolver: %v", err)
	}
	// Serve briefly to confirm it binds, then stop.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(ctx) }()
	waitForSocket(t, socket)
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("serve = %v", err)
	}
}

func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "rr")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("socket %s never appeared", path)
}
