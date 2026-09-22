package resumeupload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// TestServiceDisabledBuildsNothing: with work_router disabled, Build wires no
// pipeline (nil, nil) — the dispatcher simply does not run the release loops.
func TestServiceDisabledBuildsNothing(t *testing.T) {
	svc, err := Build(context.Background(), config.WorkRouterConfig{Enabled: false}, Deps{})
	if err != nil {
		t.Fatalf("disabled Build error: %v", err)
	}
	if svc != nil {
		t.Fatalf("disabled Build should return nil service, got %#v", svc)
	}
}

// TestExecutorLoopDrainsThenStopsOnCancel proves the relocated release executor
// keeps working in-process AND shuts down cleanly. It admits one release work
// item into a real work-ledger handle (the same handle the dispatcher would
// inject), runs runExecutorLoop, asserts the item is executed to a durable
// content result, then that cancelling ctx makes the loop return promptly so
// the dispatcher's deferred store.Close can run.
func TestExecutorLoopDrainsThenStopsOnCancel(t *testing.T) {
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatalf("open ledger: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	asset := fakeAssetReader{content: []byte("# Resume\n")}
	uploader := &fakeUploader{uploadID: "upl_1"}
	executor := &Executor{Store: store, GitHub: asset, YKM: uploader, Now: func() time.Time { return now }}
	registry := workledger.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatalf("register executor: %v", err)
	}
	route := workledger.RouteDefinition{ID: "resume-builder-release-upload", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatalf("activate route: %v", err)
	}
	digestBytes := sha256.Sum256(asset.content)
	digest := "sha256:" + hex.EncodeToString(digestBytes[:])
	admitReleaseForTest(t, store, snapshot.ID, validOperation(digest), "delivery-1", "77", "rev-1", 1, now)

	svc := &Service{
		router:   &Router{Store: store, Registry: registry, Stream: "SIGNALS", Now: func() time.Time { return now }},
		logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		interval: time.Millisecond,
	}

	runCtx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() { svc.runExecutorLoop(runCtx); close(returned) }()

	deadline := time.After(3 * time.Second)
	for {
		if _, _, ok, err := store.ContentResult(context.Background(), digest); err == nil && ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("executor loop did not execute the admitted release item in time")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if uploader.calls == 0 {
		t.Fatal("release executor did not run through the injected handle")
	}

	cancel()
	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("executor loop did not return promptly after ctx cancel (shutdown regression)")
	}
}
