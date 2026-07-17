package pushscan

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"
)

func TestMaintenancePrunesFingerprintsAtStartupAndConfiguredCadenceWithoutPush(t *testing.T) {
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, err := OpenStore(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registerMaintenanceFingerprint(t, store, "expired-before-startup", now.Add(-3*time.Hour), now.Add(-2*time.Hour), now.Add(-time.Hour))
	broker := &fakeBroker{now: now}
	scanner := configuredTestScanner(store, broker, &captureSink{}, now)
	maintenanceNow := now
	maintenance := &Maintenance{Scanner: scanner, ReconcileInterval: 5 * time.Second, FingerprintPruneInterval: time.Minute, Clock: func() time.Time { return maintenanceNow }}
	if err := maintenance.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count := fingerprintCount(t, store); count != 0 {
		t.Fatalf("startup fingerprint count = %d", count)
	}
	registerMaintenanceFingerprint(t, store, "expires-without-push", now, now.Add(10*time.Second), now.Add(30*time.Second))
	maintenanceNow = now.Add(59 * time.Second)
	if err := maintenance.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count := fingerprintCount(t, store); count != 1 {
		t.Fatalf("pre-cadence fingerprint count = %d", count)
	}
	maintenanceNow = now.Add(time.Minute)
	if err := maintenance.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count := fingerprintCount(t, store); count != 0 {
		t.Fatalf("periodic fingerprint count = %d", count)
	}
}

func registerMaintenanceFingerprint(t *testing.T, store *Store, id string, issuedAt, expiresAt, retainedUntil time.Time) {
	t.Helper()
	digest := sha256.Sum256([]byte(id))
	err := store.RegisterFingerprint(context.Background(), digest[:], FingerprintRegistration{FingerprintID: id, Profile: "general-writer-v1", LogicalSessionID: "logical", SessionLineageID: "session", WorkerID: "worker", WorkerStorageLineage: "storage", WorkerFenceEpoch: 1, ProfileGeneration: 1, IssuedAt: issuedAt, ExpiresAt: expiresAt, RetainedUntil: retainedUntil, State: "expired"})
	if err != nil {
		t.Fatal(err)
	}
}

func fingerprintCount(t *testing.T, store *Store) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`SELECT count(*) FROM push_scan_token_fingerprints`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
