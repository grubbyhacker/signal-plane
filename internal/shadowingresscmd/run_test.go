package shadowingresscmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/routeresolver"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

type testExecutor struct{ descriptor workledger.ExecutorDescriptor }

func (e testExecutor) Descriptor() workledger.ExecutorDescriptor { return e.descriptor }
func (testExecutor) Execute(context.Context, workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	return workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted}, nil
}

// prepareLedger opens a ledger, activates the upload-intake route, and returns
// the db path plus the activated snapshot id.
func prepareLedger(t *testing.T) (string, string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "ledger.db")
	store, err := workledger.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	route := workledger.RouteDefinition{
		ID: "ykm-upload-intake", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: "youknowme.curator",
		Admission:   workledger.AdmissionPolicy{Sources: []string{"youknowme"}, Namespaces: []string{"grubbyhacker/youknowme"}, ObjectKinds: []string{"upload"}, Events: []string{"upload"}, Actions: []string{"completed"}},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject},
		Retry:       workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}},
	}
	registry := workledger.NewRegistry()
	if err := registry.Register(testExecutor{descriptor: workledger.ExecutorDescriptor{ID: "youknowme.curator", Kind: workledger.ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(context.Background(), route, registry, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return dbPath, snapshot.ID
}

// shortSocketPath keeps the socket path under the macOS sun_path limit.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sic")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "i.sock")
}

func writeRouteConfig(t *testing.T, snapshotID string) string {
	t.Helper()
	body := `version: 1
routes:
  - fact: upload.completed
    agent_type: youknowme-curator
    mode: process_intake
    route_snapshot_id: ` + snapshotID + `
    contract_revision: contract-v1
`
	path := filepath.Join(t.TempDir(), "routes.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func enabledConfig(dbPath, socket, routeCfg string) config.ShadowAdmissionConfig {
	return config.ShadowAdmissionConfig{
		Enabled:      true,
		DatabasePath: dbPath,
		Ingress: config.ShadowIngressConfig{
			Enabled:         true,
			SocketPath:      socket,
			RouteConfigPath: routeCfg,
		},
	}
}

func TestBuildDisabledIsNoOp(t *testing.T) {
	// Both disabled and admission-only-enabled produce nothing to serve.
	for _, cfg := range []config.ShadowAdmissionConfig{
		{},
		{Enabled: true, Ingress: config.ShadowIngressConfig{Enabled: false}},
	} {
		service, err := Build(context.Background(), cfg, routeresolver.YouKnowMeCuratorCatalog(), nil)
		if err != nil || service != nil {
			t.Fatalf("disabled Build = %v, %v; want nil, nil", service, err)
		}
	}
	// Run is a clean no-op when disabled.
	if err := Run(context.Background(), config.ShadowAdmissionConfig{}, routeresolver.YouKnowMeCuratorCatalog(), nil); err != nil {
		t.Fatalf("disabled Run = %v, want nil", err)
	}
}

func TestBuildRequiresPathsWhenEnabled(t *testing.T) {
	catalog := routeresolver.YouKnowMeCuratorCatalog()
	// Missing database path.
	if _, err := Build(context.Background(), config.ShadowAdmissionConfig{Enabled: true, Ingress: config.ShadowIngressConfig{Enabled: true, SocketPath: shortSocketPath(t), RouteConfigPath: "x"}}, catalog, nil); err == nil {
		t.Fatal("enabled Build accepted a missing database_path")
	}
	// Missing route config path.
	if _, err := Build(context.Background(), config.ShadowAdmissionConfig{Enabled: true, DatabasePath: filepath.Join(t.TempDir(), "d.db"), Ingress: config.ShadowIngressConfig{Enabled: true, SocketPath: shortSocketPath(t)}}, catalog, nil); err == nil {
		t.Fatal("enabled Build accepted a missing route_config_path")
	}
}

func TestBuildAndServeLifecycle(t *testing.T) {
	dbPath, snapshotID := prepareLedger(t)
	socket := shortSocketPath(t)
	routeCfg := writeRouteConfig(t, snapshotID)

	service, err := Build(context.Background(), enabledConfig(dbPath, socket, routeCfg), routeresolver.YouKnowMeCuratorCatalog(), nil)
	if err != nil {
		t.Fatalf("Build = %v", err)
	}
	if service == nil {
		t.Fatal("Build returned nil service for an enabled config")
	}
	defer service.Close()
	if service.Revision() == "" {
		t.Fatal("service has no route revision")
	}

	// Serve until ctx cancel, then confirm it returns (graceful shutdown).
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- service.Serve(ctx) }()
	waitForSocket(t, socket)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v after cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after context cancel (shutdown hung)")
	}
	// The socket is cleaned up on shutdown.
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("socket not removed on shutdown: %v", err)
	}
}

func TestRunReturnsOnContextCancel(t *testing.T) {
	dbPath, snapshotID := prepareLedger(t)
	socket := shortSocketPath(t)
	routeCfg := writeRouteConfig(t, snapshotID)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, enabledConfig(dbPath, socket, routeCfg), routeresolver.YouKnowMeCuratorCatalog(), nil)
	}()
	waitForSocket(t, socket)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v after cancel", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestBuildFailsClosedOnMissingRouteSnapshot(t *testing.T) {
	dbPath, _ := prepareLedger(t)
	socket := shortSocketPath(t)
	// Point the route config at a snapshot id that is NOT activated.
	routeCfg := writeRouteConfig(t, "route-snapshot-that-does-not-exist")
	if _, err := Build(context.Background(), enabledConfig(dbPath, socket, routeCfg), routeresolver.YouKnowMeCuratorCatalog(), nil); err == nil {
		t.Fatal("Build served with a route config referencing a missing route snapshot")
	}
}

func TestBuildFailsClosedOnCatalogMismatch(t *testing.T) {
	dbPath, snapshotID := prepareLedger(t)
	socket := shortSocketPath(t)
	// A mode the curator catalog does not declare.
	body := `version: 1
routes:
  - fact: upload.completed
    agent_type: youknowme-curator
    mode: address_review_feedback
    route_snapshot_id: ` + snapshotID + `
`
	routeCfg := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(routeCfg, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(context.Background(), enabledConfig(dbPath, socket, routeCfg), routeresolver.YouKnowMeCuratorCatalog(), nil); err == nil {
		t.Fatal("Build accepted a mode the catalog does not declare")
	}
}

func TestBuildFailsClosedOnBadSocketPermissions(t *testing.T) {
	dbPath, snapshotID := prepareLedger(t)
	routeCfg := writeRouteConfig(t, snapshotID)
	// A world-writable socket directory must be refused.
	openDir, err := os.MkdirTemp("", "open")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(openDir) })
	if err := os.Chmod(openDir, 0o777); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(openDir, "i.sock")
	if _, err := Build(context.Background(), enabledConfig(dbPath, socket, routeCfg), routeresolver.YouKnowMeCuratorCatalog(), nil); err == nil {
		t.Fatal("Build accepted a world-writable socket directory")
	}
	// A relative (non-absolute) socket path is refused too.
	if _, err := Build(context.Background(), enabledConfig(dbPath, "relative.sock", routeCfg), routeresolver.YouKnowMeCuratorCatalog(), nil); err == nil {
		t.Fatal("Build accepted a relative socket path")
	}
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("socket %s never appeared", path)
}
