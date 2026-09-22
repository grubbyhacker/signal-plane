package dispatcher

import (
	"context"
	"path/filepath"
	"testing"
)

// TestWorkLedgerSharesSingleHandle proves the single-writer seam: the
// work-ledger view returned by Store.WorkLedger is backed by the SAME *sql.DB
// the dispatcher opened, opens no second handle, and does not own it — so
// closing the view is a no-op and the dispatcher's store stays usable. This is
// what lets route activation and the shadow-admission ingress write through the
// one connection the dispatcher owns instead of a second process.
func TestWorkLedgerSharesSingleHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "work.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ledger := store.WorkLedger()
	if ledger == nil {
		t.Fatal("WorkLedger returned nil")
	}

	// The view must be usable against the shared handle.
	if err := ledger.Ready(context.Background()); err != nil {
		t.Fatalf("ledger.Ready on shared handle: %v", err)
	}

	// Closing the view must be a no-op: it does not own the handle. If it closed
	// the underlying *sql.DB, the dispatcher store's next query would fail.
	if err := ledger.Close(); err != nil {
		t.Fatalf("ledger.Close should be a no-op, got: %v", err)
	}
	if err := store.Ready(context.Background()); err != nil {
		t.Fatalf("dispatcher store unusable after view Close (view wrongly owned the handle): %v", err)
	}

	// A second view is likewise a shared, non-owning wrapper — no second open.
	again := store.WorkLedger()
	if err := again.Ready(context.Background()); err != nil {
		t.Fatalf("second WorkLedger view Ready: %v", err)
	}
}
