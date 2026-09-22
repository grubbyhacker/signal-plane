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

// allowAll accepts any peer, so the socket round-trip runs on non-Linux dev
// machines where SO_PEERCRED is unavailable.
type allowAll struct{}

func (allowAll) authorize(net.Conn, []uint32) error { return nil }

// fakeResolver is a deployment-owned RouteResolver stand-in. It matches only a
// configured object kind, returns a fixed route snapshot and binding, and
// records the last event it saw so a test can prove the resolver — not the
// caller — chose the routing.
type fakeResolver struct {
	snapshotID string
	binding    workledger.AgentBinding
	matchKind  string
	lastEvent  workledger.Event
	calls      int
}

func (resolver *fakeResolver) Resolve(_ context.Context, event workledger.Event) (Resolution, error) {
	resolver.calls++
	resolver.lastEvent = event
	if resolver.matchKind != "" && event.ObjectKind != resolver.matchKind {
		return Resolution{Matched: false}, nil
	}
	return Resolution{Matched: true, RouteSnapshotID: resolver.snapshotID, Binding: resolver.binding}, nil
}

func newShadow(t *testing.T) (*shadowadmit.Shadow, *fakeResolver, *workledger.Store) {
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
	resolver := &fakeResolver{
		snapshotID: snapshot.ID,
		binding:    workledger.AgentBinding{AgentType: "youknowme-curator", Mode: "reconcile", TypeContractRevision: "contract-v1"},
		matchKind:  "pull_request",
	}
	return shadow, resolver, store
}

// goodEnvelope builds a valid domain-fact envelope. It carries NO routing — the
// resolver chooses the route snapshot.
func goodEnvelope(delivery string, seq uint64, revision string) Envelope {
	return Envelope{
		SignalID: "signal-" + delivery, SourceDeliveryID: delivery,
		TransportStream: "signals", TransportSeq: seq, Source: "github", Namespace: "example/widgets",
		ObjectKind: "pull_request", ObjectID: "17", EventKind: "pull_request", Action: "synchronize",
		ActorClass: "user", SourceRevision: revision, PayloadDigest: "sha256:payload", EvidenceRef: "nats://signals",
	}
}

func TestEnvelopeValidationAndBounds(t *testing.T) {
	env := goodEnvelope("d-1", 1, "rev-1")
	if err := env.Validate(); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	bad := env
	bad.Source = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("envelope missing source was accepted")
	}
	badSeq := env
	badSeq.TransportSeq = 0
	if err := badSeq.Validate(); err == nil {
		t.Fatal("envelope with zero transport sequence accepted")
	}
	over := env
	over.Namespace = strings.Repeat("n", 513)
	if err := over.Validate(); err == nil {
		t.Fatal("oversized field accepted")
	}
	huge := make([]byte, MaxEnvelopeBytes+1)
	if _, err := DecodeEnvelope(huge); err != ErrEnvelopeTooLarge {
		t.Fatalf("oversize payload = %v, want ErrEnvelopeTooLarge", err)
	}
	// The caller may not select routing or an agent/image/release/generation:
	// any such JSON field is an unknown field and is rejected by DecodeEnvelope.
	base := `"source_delivery_id":"d","transport_stream":"s","transport_sequence":1,"source":"github","namespace":"n","object_kind":"pull_request","object_id":"1","event_kind":"pull_request","source_revision":"r","payload_digest":"sha256:p","evidence_ref":"e"`
	for _, forbidden := range []string{
		`"route_snapshot_id":"r"`,
		`"agent_type":"youknowme-curator"`,
		`"mode":"reconcile"`,
		`"image":"ghcr.io/x@sha256:deadbeef"`,
		`"resolved_release_generation":3`,
		`"resolved_release_digest":"sha256:x"`,
		`"generation":3`,
		`"broker_run_id":"run-1"`,
	} {
		payload := "{" + base + "," + forbidden + "}"
		if _, err := DecodeEnvelope([]byte(payload)); err == nil {
			t.Fatalf("caller-supplied field accepted: %s", forbidden)
		}
	}
	// The valid envelope decodes.
	if _, err := DecodeEnvelope([]byte("{" + base + "}")); err != nil {
		t.Fatalf("valid minimal envelope rejected: %v", err)
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

func TestEnabledIngressRequiresResolver(t *testing.T) {
	shadow, _, _ := newShadow(t)
	if _, err := NewServer(Config{Enabled: true, SocketPath: shortSocketPath(t)}, shadow, nil, nil); err == nil {
		t.Fatal("enabled ingress accepted a nil route resolver")
	}
}

func TestDisabledIngressIsNoOp(t *testing.T) {
	shadow, _, _ := newShadow(t)
	server, err := NewServer(Config{Enabled: false}, shadow, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(context.Background()); err != nil {
		t.Fatalf("disabled Serve = %v, want nil no-op", err)
	}
}

func TestSocketRoundTripUsesResolverOutputAndDedupes(t *testing.T) {
	shadow, resolver, store := newShadow(t)
	socket := shortSocketPath(t)
	server, err := NewServer(Config{Enabled: true, SocketPath: socket}, shadow, resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.setAuthorizer(allowAll{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(ctx) }()
	waitForSocket(t, socket)
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != socketMode {
		t.Fatalf("socket mode = %#o, want %#o", info.Mode().Perm(), socketMode)
	}

	env := goodEnvelope("d-round", 1, "rev-round")
	first := request(t, socket, env)
	if !first.Matched || first.WorkItemID == "" || first.Duplicate || first.Launched {
		t.Fatalf("first admit = %#v", first)
	}
	// The resolver saw the domain fact and chose the routing; the caller did not.
	if resolver.calls == 0 || resolver.lastEvent.SourceDeliveryID != "d-round" {
		t.Fatalf("resolver was not consulted: calls=%d last=%#v", resolver.calls, resolver.lastEvent)
	}
	// The persisted work item carries the RESOLVER's binding.
	item, err := store.WorkItem(context.Background(), first.WorkItemID)
	if err != nil {
		t.Fatal(err)
	}
	if item.AgentType != "youknowme-curator" || item.AgentMode != "reconcile" || item.TypeContractRevision != "contract-v1" {
		t.Fatalf("persisted binding is not the resolver's: %#v", item)
	}

	second := request(t, socket, env)
	if !second.Matched || !second.Duplicate || second.WorkItemID != first.WorkItemID || second.Launched {
		t.Fatalf("second admit = %#v (want dup of %s)", second, first.WorkItemID)
	}
	cancel()
	if err := <-serveErr; err != nil {
		t.Fatalf("serve exited with %v", err)
	}
}

func TestUnmatchedFactIsDroppedNotAdmitted(t *testing.T) {
	shadow, resolver, _ := newShadow(t)
	resolver.matchKind = "issues" // the envelope's pull_request will not match
	socket := shortSocketPath(t)
	server, err := NewServer(Config{Enabled: true, SocketPath: socket}, shadow, resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.setAuthorizer(allowAll{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	waitForSocket(t, socket)

	result := request(t, socket, goodEnvelope("d-nomatch", 1, "rev"))
	if result.Matched || result.WorkItemID != "" || result.Launched {
		t.Fatalf("unmatched fact was not dropped: %#v", result)
	}
}

func TestClearStaleSocketRefusesNonSocket(t *testing.T) {
	dir := shortDir(t)
	// A regular file at the socket path must be preserved and the clear refused.
	regular := filepath.Join(dir, "notasock")
	if err := os.WriteFile(regular, []byte("precious"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := clearStaleSocket(regular); err == nil {
		t.Fatal("clearStaleSocket removed / accepted a regular file")
	}
	if data, err := os.ReadFile(regular); err != nil || string(data) != "precious" {
		t.Fatalf("regular file was not preserved: data=%q err=%v", data, err)
	}
	// A missing path is a clean no-op.
	if err := clearStaleSocket(filepath.Join(dir, "absent.sock")); err != nil {
		t.Fatalf("clearStaleSocket on absent path = %v, want nil", err)
	}
	// A real socket is removed.
	sockPath := filepath.Join(dir, "real.sock")
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	if err := clearStaleSocket(sockPath); err != nil {
		t.Fatalf("clearStaleSocket on a real socket = %v, want nil", err)
	}
	if _, err := os.Lstat(sockPath); !os.IsNotExist(err) {
		t.Fatalf("real socket was not removed: %v", err)
	}
}

func TestServeRefusesToStartOnNonSocketPath(t *testing.T) {
	shadow, resolver, _ := newShadow(t)
	dir := shortDir(t)
	regular := filepath.Join(dir, "block")
	if err := os.WriteFile(regular, []byte("do not delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(Config{Enabled: true, SocketPath: regular}, shadow, resolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	server.setAuthorizer(allowAll{})
	if err := server.Serve(context.Background()); err == nil {
		t.Fatal("Serve started on a path occupied by a regular file")
	}
	if data, err := os.ReadFile(regular); err != nil || string(data) != "do not delete" {
		t.Fatalf("regular file was clobbered by startup: data=%q err=%v", data, err)
	}
}

func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "si")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func shortSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortDir(t), "i.sock")
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
