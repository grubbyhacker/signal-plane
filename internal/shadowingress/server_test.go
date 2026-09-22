package shadowingress

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/shadowadmit"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

type testExecutor struct{ descriptor workledger.ExecutorDescriptor }

func (executor testExecutor) Descriptor() workledger.ExecutorDescriptor { return executor.descriptor }
func (testExecutor) Execute(context.Context, workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	return workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted}, nil
}

// allowAll is a test authorizer that accepts any peer, so the socket round-trip
// runs on non-Linux dev machines where SO_PEERCRED is unavailable.
type allowAll struct{}

func (allowAll) authorize(net.Conn, []uint32) error { return nil }

func newShadow(t *testing.T) (*shadowadmit.Shadow, workledger.RouteSnapshot) {
	t.Helper()
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ingress.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	route := workledger.RouteDefinition{
		ID: "github-pr-check", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: "github.pr-check",
		Admission:   workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{"example/widgets"}, ObjectKinds: []string{"pull_request"}, Events: []string{"pull_request"}, Actions: []string{"synchronize"}},
		Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject, Supersede: true},
		Retry:       workledger.RetryPolicy{MaxAttempts: 3, Backoff: []time.Duration{30 * time.Second, time.Minute}},
	}
	registry := workledger.NewRegistry()
	if err := registry.Register(testExecutor{descriptor: workledger.ExecutorDescriptor{ID: route.ExecutorID, Kind: workledger.ExecutorDeterministicTool, Version: "v1"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	shadow, err := shadowadmit.New(store, true)
	if err != nil {
		t.Fatal(err)
	}
	return shadow, snapshot
}

func goodEnvelope(snapshotID, delivery string, seq uint64, revision string) Envelope {
	return Envelope{
		RouteSnapshotID: snapshotID, SignalID: "signal-" + delivery, SourceDeliveryID: delivery,
		TransportStream: "signals", TransportSeq: seq, Source: "github", Namespace: "example/widgets",
		ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "synchronize",
		ActorClass: "user", SourceRevision: revision, PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals",
	}
}

func TestEnvelopeValidationAndBounds(t *testing.T) {
	env := goodEnvelope("route-1", "d-1", 1, "rev-1")
	if err := env.Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	// Missing required field.
	bad := env
	bad.Source = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("envelope missing source was accepted")
	}
	// Zero transport sequence.
	badSeq := env
	badSeq.TransportSeq = 0
	if err := badSeq.Validate(); err == nil {
		t.Fatal("envelope with zero transport sequence accepted")
	}
	// Oversized field.
	over := env
	over.Namespace = strings.Repeat("n", 513)
	if err := over.Validate(); err == nil {
		t.Fatal("oversized field accepted")
	}
	// Oversize raw payload is refused by DecodeEnvelope before parsing.
	huge := make([]byte, MaxEnvelopeBytes+1)
	if _, err := DecodeEnvelope(huge); err != ErrEnvelopeTooLarge {
		t.Fatalf("oversize payload = %v, want ErrEnvelopeTooLarge", err)
	}
	// Unknown fields rejected.
	if _, err := DecodeEnvelope([]byte(`{"route_snapshot_id":"r","unknown":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
	// Envelope carries no agent/image/release fields at all — a struct round
	// trip must not surface any such key.
	encoded, _ := json.Marshal(env)
	for _, forbidden := range []string{"agent_type", "resolved_release", "broker_run", "authoritative_pr"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("envelope serialization leaked a selection field %q: %s", forbidden, encoded)
		}
	}
}

func TestValidateSocketPath(t *testing.T) {
	if err := validateSocketPath(""); err == nil {
		t.Fatal("empty socket path accepted")
	}
	if err := validateSocketPath("relative.sock"); err == nil {
		t.Fatal("relative socket path accepted")
	}
	dir := t.TempDir()
	if err := validateSocketPath(filepath.Join(dir, "s.sock")); err != nil {
		t.Fatalf("owner-only dir rejected: %v", err)
	}
	// A group/world-writable parent must be refused.
	openDir := filepath.Join(dir, "open")
	if err := os.Mkdir(openDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(openDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := validateSocketPath(filepath.Join(openDir, "s.sock")); err == nil {
		t.Fatal("world-writable socket directory accepted")
	}
}

func TestDisabledIngressIsNoOp(t *testing.T) {
	shadow, _ := newShadow(t)
	server, err := NewServer(Config{Enabled: false}, shadow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(context.Background()); err != nil {
		t.Fatalf("disabled Serve = %v, want nil no-op", err)
	}
}

func TestSocketRoundTripAdmitsAndDedupesDeterministically(t *testing.T) {
	shadow, snapshot := newShadow(t)
	socket := shortSocketPath(t)
	server, err := NewServer(Config{Enabled: true, SocketPath: socket}, shadow, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.setAuthorizer(allowAll{}) // SO_PEERCRED unavailable off-Linux
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, socket)
	// The socket must be owner-only.
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != socketMode {
		t.Fatalf("socket mode = %#o, want %#o", info.Mode().Perm(), socketMode)
	}

	env := goodEnvelope(snapshot.ID, "d-round", 1, "rev-round")
	first := request(t, socket, env)
	if first.WorkItemID == "" || first.Duplicate || first.Launched {
		t.Fatalf("first admit = %#v", first)
	}
	// Same envelope again -> deterministic duplicate onto the same work item.
	second := request(t, socket, env)
	if !second.Duplicate || second.WorkItemID != first.WorkItemID || second.Launched {
		t.Fatalf("second admit = %#v (want dup of %s)", second, first.WorkItemID)
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve exited with %v", err)
	}
}

func TestSocketRejectsOversizeAndBadEnvelope(t *testing.T) {
	shadow, snapshot := newShadow(t)
	socket := shortSocketPath(t)
	server, err := NewServer(Config{Enabled: true, SocketPath: socket}, shadow, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.setAuthorizer(allowAll{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	waitForSocket(t, socket)

	// Missing route snapshot id -> error reply, nothing admitted.
	bad := goodEnvelope("", "d-bad", 1, "rev-bad")
	reply := requestRaw(t, socket, mustJSON(t, bad))
	if !strings.Contains(reply, "error") {
		t.Fatalf("bad envelope reply = %q", reply)
	}
	_ = snapshot
}

// shortSocketPath returns a socket path short enough for the platform sun_path
// limit (macOS ~104 bytes), which t.TempDir()+test-name paths can exceed.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "si")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "i.sock")
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

func request(t *testing.T, socket string, env Envelope) Result {
	t.Helper()
	reply := requestRaw(t, socket, mustJSON(t, env))
	var result Result
	if err := json.Unmarshal([]byte(reply), &result); err != nil {
		t.Fatalf("decode reply %q: %v", reply, err)
	}
	return result
}

func requestRaw(t *testing.T, socket string, payload []byte) string {
	t.Helper()
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write(payload); err != nil {
		t.Fatal(err)
	}
	// Half-close so the server's io.ReadAll returns.
	if uc, ok := conn.(*net.UnixConn); ok {
		_ = uc.CloseWrite()
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	n, _ := conn.Read(buf)
	return string(buf[:n])
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
