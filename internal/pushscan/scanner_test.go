package pushscan

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeBroker struct {
	material         Material
	materialErr      error
	respondErr       error
	respondAlwaysErr error
	materialCalls    int
	respondCalls     int
	respondSuccesses int
	requests         []ResponseRequest
	keys             []string
	now              time.Time
}

func (broker *fakeBroker) Material(_ context.Context, _ MaterialRequest) (Material, error) {
	broker.materialCalls++
	return broker.material, broker.materialErr
}

func (broker *fakeBroker) Respond(_ context.Context, key string, request ResponseRequest) (ResponseResult, error) {
	broker.respondCalls++
	broker.keys = append(broker.keys, key)
	broker.requests = append(broker.requests, request)
	if broker.respondAlwaysErr != nil {
		return ResponseResult{}, broker.respondAlwaysErr
	}
	if broker.respondErr != nil {
		err := broker.respondErr
		broker.respondErr = nil
		return ResponseResult{}, err
	}
	broker.respondSuccesses++
	actions := []ActionResult{{Action: "halt_issuance", State: "halted", CompletedAt: broker.now}}
	if request.Binding != nil {
		actions = append(actions, ActionResult{Action: "fence_worker_session", State: "fence_requested", CompletedAt: broker.now.Add(time.Second)})
	}
	return ResponseResult{Version: WireVersion, FindingID: request.FindingID, IdempotentReplay: broker.respondCalls > 1, Actions: actions}, nil
}

type captureSink struct {
	events []SecurityEvent
	err    error
}

func (sink *captureSink) Publish(_ context.Context, event SecurityEvent) error {
	if sink.err != nil {
		return sink.err
	}
	sink.events = append(sink.events, event)
	return nil
}

func TestPushScannerExactFingerprintIsDurableSanitizedAndIdempotent(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	token := "AbCDef0123456789SecretTokenValue/plus+"
	scanner, store, broker, sink := testScanner(t, now, token)
	identity := testIdentity("delivery-exact", now)

	result, err := scanner.Process(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if result.Severity != "high" || result.ReasonCode != "issued_token_fingerprint_match" || result.SLOState != "met" || result.HaltedAt.IsZero() || result.FenceState != "fence_requested" || !result.FencedAt.IsZero() {
		t.Fatalf("result = %#v", result)
	}
	if want := findingID(identity, "issued_token_fingerprint_match", "fingerprint-01"); result.FindingID != want {
		t.Fatalf("finding id = %q want %q", result.FindingID, want)
	}
	if broker.respondCalls != 1 || broker.requests[0].Binding == nil || len(sink.events) != 1 || sink.events[0].State != "alert_requested" {
		t.Fatalf("respond=%d request=%#v events=%#v", broker.respondCalls, broker.requests, sink.events)
	}
	if replay, err := scanner.Process(context.Background(), identity); err != nil || replay.FindingID != result.FindingID || broker.materialCalls != 1 || broker.respondCalls != 1 || len(sink.events) != 1 {
		t.Fatalf("replay=%#v err=%v material=%d respond=%d events=%d", replay, err, broker.materialCalls, broker.respondCalls, len(sink.events))
	}
	republished := identity
	republished.StreamSequence++
	republished.ReceivedAt = republished.ReceivedAt.Add(time.Minute)
	if replay, err := scanner.Process(context.Background(), republished); err != nil || replay.FindingID != result.FindingID || !replay.ReceiptAt.Equal(identity.ReceivedAt) || broker.materialCalls != 1 || broker.respondCalls != 1 {
		t.Fatalf("republished replay=%#v err=%v material=%d respond=%d", replay, err, broker.materialCalls, broker.respondCalls)
	}
	var eventJSON string
	if err := store.db.QueryRow(`SELECT payload_json FROM push_scan_security_events WHERE delivery_id=?`, identity.DeliveryID).Scan(&eventJSON); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(eventJSON, token) {
		t.Fatal("durable event contains token value")
	}
}

func TestPushScannerReconcilesAlertAndResponseIndependentlyAcrossRestartAndDeadline(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	token := "AbCDef0123456789SecretTokenValue/plus+"
	databasePath := filepath.Join(t.TempDir(), "scanner.db")
	store, err := OpenStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	identity := testIdentity("delivery-crash-seam", now)
	broker := &fakeBroker{material: testMaterial(identity.DeliveryID, token), now: now.Add(10 * time.Second), respondAlwaysErr: errors.New("broker unavailable")}
	sink := &captureSink{}
	scanner := configuredTestScanner(store, broker, sink, now)
	registerTestFingerprint(t, store, scanner.FingerprintKey, token, now.Add(-time.Minute), now.Add(time.Hour), "active")
	result, err := scanner.Process(context.Background(), identity)
	if err == nil || result.Status != "response_pending" {
		t.Fatal("expected response seam failure")
	}
	pending, ok, err := store.Result(context.Background(), identity.DeliveryID)
	pendingEvents, pendingEventErr := store.PendingEvents(context.Background())
	if err != nil || pendingEventErr != nil || !ok || pending.Status != "response_pending" || pending.AlertState != "alert_requested" || len(pendingEvents) != 0 || len(sink.events) != 1 {
		t.Fatalf("pending=%#v ok=%v err=%v pending_events=%d pending_event_err=%v published=%d", pending, ok, err, len(pendingEvents), pendingEventErr, len(sink.events))
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := OpenStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restartedStore.Close() })
	afterDeadline := now.Add(6 * time.Minute)
	restarted := configuredTestScanner(restartedStore, broker, sink, afterDeadline)
	maintenanceNow := afterDeadline
	maintenance := &Maintenance{Scanner: restarted, ReconcileInterval: 5 * time.Second, FingerprintPruneInterval: time.Hour, Clock: func() time.Time { return maintenanceNow }}
	if err := maintenance.Startup(context.Background()); err == nil {
		t.Fatal("expected broker outage during restart reconciliation")
	}
	pending, ok, err = restartedStore.Result(context.Background(), identity.DeliveryID)
	if err != nil || !ok || pending.Status != "response_pending" || pending.SLOState != "breached" || !pending.SLOBreached || len(sink.events) != 1 {
		t.Fatalf("overdue pending=%#v ok=%v err=%v events=%d", pending, ok, err, len(sink.events))
	}
	broker.respondAlwaysErr = nil
	broker.now = afterDeadline.Add(10 * time.Second)
	maintenanceNow = maintenanceNow.Add(5 * time.Second)
	if err := maintenance.RunDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, ok, err = restartedStore.Result(context.Background(), identity.DeliveryID)
	var responseAttempts int
	attemptErr := restartedStore.db.QueryRow(`SELECT response_attempt_count FROM push_scan_results WHERE delivery_id=?`, identity.DeliveryID).Scan(&responseAttempts)
	if err != nil || attemptErr != nil || !ok || result.Status != "finding_high" || result.SLOState != "breached" || !result.SLOBreached || broker.materialCalls != 1 || broker.respondCalls != 3 || broker.respondSuccesses != 1 || responseAttempts != 3 || broker.keys[0] != broker.keys[1] || broker.keys[1] != broker.keys[2] || len(sink.events) != 1 {
		t.Fatalf("result=%#v ok=%v err=%v material=%d respond=%d successes=%d keys=%v events=%d", result, ok, err, broker.materialCalls, broker.respondCalls, broker.respondSuccesses, broker.keys, len(sink.events))
	}
}

func TestPushScannerStartupReconciliationFlushesOutboxWithoutRepeatingResponse(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	token := "AbCDef0123456789SecretTokenValue/plus+"
	databasePath := filepath.Join(t.TempDir(), "scanner.db")
	store, err := OpenStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	identity := testIdentity("delivery-outbox-restart", now)
	broker := &fakeBroker{material: testMaterial(identity.DeliveryID, token), now: now.Add(10 * time.Second)}
	sink := &captureSink{err: errors.New("event bus unavailable")}
	scanner := configuredTestScanner(store, broker, sink, now)
	registerTestFingerprint(t, store, scanner.FingerprintKey, token, now.Add(-time.Minute), now.Add(time.Hour), "active")
	result, err := scanner.Process(context.Background(), identity)
	if err == nil || result.Status != "finding_high" || broker.respondSuccesses != 1 {
		t.Fatalf("result=%#v err=%v response_successes=%d", result, err, broker.respondSuccesses)
	}
	if pending, err := store.PendingEvents(context.Background()); err != nil || len(pending) != 1 {
		t.Fatalf("pending events=%d err=%v", len(pending), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restartedStore, err := OpenStore(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedStore.Close()
	recoveredSink := &captureSink{}
	restarted := configuredTestScanner(restartedStore, broker, recoveredSink, now.Add(time.Minute))
	maintenance := &Maintenance{Scanner: restarted, ReconcileInterval: 5 * time.Second, FingerprintPruneInterval: time.Hour, Clock: func() time.Time { return now.Add(time.Minute) }}
	if err := maintenance.Startup(context.Background()); err != nil {
		t.Fatal(err)
	}
	if pending, err := restartedStore.PendingEvents(context.Background()); err != nil || len(pending) != 0 || len(recoveredSink.events) != 1 || broker.respondSuccesses != 1 || broker.respondCalls != 1 {
		t.Fatalf("pending=%d err=%v events=%d response_calls=%d successes=%d", len(pending), err, len(recoveredSink.events), broker.respondCalls, broker.respondSuccesses)
	}
}

func TestPushScannerSupportedReversibleEncodingsAndCanary(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	token := "AbCDef0123456789SecretTokenValue/plus+"
	encodings := map[string]string{
		"raw":       token,
		"base64":    base64.StdEncoding.EncodeToString([]byte(token)),
		"base64url": base64.RawURLEncoding.EncodeToString([]byte(token)),
		"hex":       hex.EncodeToString([]byte(token)),
		"url":       url.PathEscape(token),
		"nested":    base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString([]byte(token)))),
	}
	for name, encoded := range encodings {
		t.Run(name, func(t *testing.T) {
			scanner, _, broker, _ := testScanner(t, now, token)
			broker.material = testMaterial("delivery-"+name, encoded)
			result, err := scanner.Process(context.Background(), testIdentity("delivery-"+name, now))
			if err != nil || result.ReasonCode != "issued_token_fingerprint_match" {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
	canary := "PR10_CREDENTIAL_CANARY:synthetic-value-123456"
	scanner, _, broker, sink := testScanner(t, now, token)
	broker.material = testMaterial("delivery-canary", base64.StdEncoding.EncodeToString([]byte(canary)))
	result, err := scanner.Process(context.Background(), testIdentity("delivery-canary", now))
	if err != nil || result.ReasonCode != "seeded_canary_match" || result.WorkerID != "canary-worker" || len(sink.events) != 1 {
		t.Fatalf("result=%#v err=%v events=%#v", result, err, sink.events)
	}
	if raw := string(mustEventPayload(t, scanner.Store, "delivery-canary")); strings.Contains(raw, canary) {
		t.Fatal("event contains canary bytes")
	}
}

func TestPushScannerExpiredFingerprintIsForensicOnly(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	token := "AbCDef0123456789SecretTokenValue/plus+"
	scanner, store, broker, _ := testScannerWithoutRegistration(t, now, token)
	registerTestFingerprint(t, store, scanner.FingerprintKey, token, now.Add(-2*time.Hour), now.Add(-time.Hour), "expired")
	broker.material = testMaterial("delivery-expired", token)
	result, err := scanner.Process(context.Background(), testIdentity("delivery-expired", now))
	if err != nil || result.Severity != "high" || result.ReasonCode != "expired_token_fingerprint_match" || result.SLOState != "not_live_when_scanned" || broker.respondCalls != 0 {
		t.Fatalf("result=%#v err=%v respond=%d", result, err, broker.respondCalls)
	}
}

func TestPushScannerGenericShapeIsLowAndLocal(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	scanner, _, broker, sink := testScanner(t, now, "AbCDef0123456789SecretTokenValue/plus+")
	broker.material = testMaterial("delivery-generic", "eyJabcdefghijk.abcdefghijk.abcdefghijk")
	result, err := scanner.Process(context.Background(), testIdentity("delivery-generic", now))
	if err != nil || result.Severity != "low" || result.ReasonCode != "generic_credential_shape" || broker.respondCalls != 0 || len(sink.events) != 1 {
		t.Fatalf("result=%#v err=%v respond=%d events=%d", result, err, broker.respondCalls, len(sink.events))
	}
}

func TestPushScannerNegativeMaterialProofsFailClosedHigh(t *testing.T) {
	reasons := []string{"before_mismatch", "ref_deletion_rejected", "non_fast_forward_rejected", "commit_bound_exceeded", "path_bound_exceeded", "blob_bound_exceeded", "total_bytes_exceeded", "candidate_bound_exceeded", "decode_bound_exceeded"}
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	for index, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			scanner, _, broker, _ := testScanner(t, now, "AbCDef0123456789SecretTokenValue/plus+")
			id := testIdentity("delivery-negative-"+string(rune('a'+index)), now)
			if reason == "ref_deletion_rejected" {
				id.After = strings.Repeat("0", 40)
			}
			broker.material = Material{Version: WireVersion, DeliveryID: id.DeliveryID, Repository: id.Repository, Ref: id.Ref, Before: id.Before, After: id.After, Complete: false, ReasonCode: reason}
			result, err := scanner.Process(context.Background(), id)
			if err != nil || result.Severity != "high" || result.ReasonCode != reason || broker.respondCalls != 1 {
				t.Fatalf("result=%#v err=%v respond=%d", result, err, broker.respondCalls)
			}
		})
	}
}

func TestPushScannerRevalidatesEveryLocalBoundAndNeverReportsClean(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	for _, name := range []string{"commit", "path", "blob", "total", "candidate", "decode"} {
		t.Run(name, func(t *testing.T) {
			scanner, _, broker, _ := testScanner(t, now, "AbCDef0123456789SecretTokenValue/plus+")
			id := testIdentity("delivery-local-bound-"+name, now)
			material := testMaterial(id.DeliveryID, "ordinary message")
			switch name {
			case "commit":
				material.Commits = make([]Commit, scanner.Bounds.MaxCommits+1)
				material.Bounds.CommitCount = len(material.Commits)
			case "path":
				material.Files = make([]File, scanner.Bounds.MaxPaths+1)
				material.Bounds.PathCount = len(material.Files)
			case "blob":
				material.Files = []File{{CommitSHA: id.After, Path: "large.bin", Side: "after", Status: "modified", BlobSHA: strings.Repeat("c", 40), Size: scanner.Bounds.MaxBlobBytes + 1}}
				material.Bounds.PathCount = 1
			case "total":
				material.Bounds.TotalBytes = scanner.Bounds.MaxTotalBytes + 1
			case "candidate":
				scanner.Bounds.MaxCandidates = 1
				material.Commits[0].Message = base64.StdEncoding.EncodeToString([]byte("ordinary decoded message"))
			case "decode":
				scanner.Bounds.MaxDecodeDepth = 1
				material.Commits[0].Message = base64.StdEncoding.EncodeToString([]byte(base64.StdEncoding.EncodeToString([]byte("ordinary decoded message"))))
			}
			broker.material = material
			result, err := scanner.Process(context.Background(), id)
			if err != nil || result.Severity != "high" || result.Status == "clean" || broker.respondCalls != 1 {
				t.Fatalf("result=%#v err=%v respond=%d", result, err, broker.respondCalls)
			}
		})
	}
}

func TestPushScannerCatalogReplayCollisionAndDelayedSLO(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	token := "AbCDef0123456789SecretTokenValue/plus+"
	scanner, _, broker, _ := testScanner(t, now, token)
	outside := testIdentity("delivery-outside", now)
	outside.Repository = "attacker/repo"
	result, err := scanner.Process(context.Background(), outside)
	if err != nil || result.Status != "rejected_catalog" || broker.materialCalls != 0 {
		t.Fatalf("catalog result=%#v err=%v material=%d", result, err, broker.materialCalls)
	}
	collision := outside
	collision.After = strings.Repeat("c", 40)
	if _, err := scanner.Process(context.Background(), collision); err == nil || !strings.Contains(err.Error(), "identity conflict") {
		t.Fatalf("collision err=%v", err)
	}
	retryScanner, retryStore, retryBroker, _ := testScanner(t, now, token)
	retryID := testIdentity("delivery-receipt-only", now)
	retryBroker.material = testMaterial(retryID.DeliveryID, token)
	if _, _, _, err := retryStore.RecordReceipt(context.Background(), retryID, now); err != nil {
		t.Fatal(err)
	}
	if retryResult, err := retryScanner.Process(context.Background(), retryID); err != nil || retryResult.Status != "finding_high" || retryBroker.materialCalls != 1 {
		t.Fatalf("receipt-only retry=%#v err=%v material=%d", retryResult, err, retryBroker.materialCalls)
	}

	delayedScanner, _, delayedBroker, _ := testScanner(t, now, token)
	delayedID := testIdentity("delivery-delayed", now)
	delayedBroker.material = testMaterial(delayedID.DeliveryID, token)
	delayed := now.Add(ConsumerAckWait)
	delayedScanner.Clock = func() time.Time { return delayed }
	delayedBroker.now = delayed
	delayedResult, err := delayedScanner.Process(context.Background(), delayedID)
	if err != nil || delayedResult.SLOState != "breached" || !delayedResult.SLOBreached || !delayedResult.ReceiptAt.Equal(now) {
		t.Fatalf("delayed=%#v err=%v", delayedResult, err)
	}
}

func testScanner(t *testing.T, now time.Time, token string) (*Scanner, *Store, *fakeBroker, *captureSink) {
	scanner, store, broker, sink := testScannerWithoutRegistration(t, now, token)
	registerTestFingerprint(t, store, scanner.FingerprintKey, token, now.Add(-time.Minute), now.Add(time.Hour), "active")
	return scanner, store, broker, sink
}

func testScannerWithoutRegistration(t *testing.T, now time.Time, token string) (*Scanner, *Store, *fakeBroker, *captureSink) {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "scanner.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	id := testIdentity("delivery-exact", now)
	broker := &fakeBroker{material: testMaterial(id.DeliveryID, token), now: now.Add(10 * time.Second)}
	sink := &captureSink{}
	scanner := configuredTestScanner(store, broker, sink, now)
	return scanner, store, broker, sink
}

func configuredTestScanner(store *Store, broker *fakeBroker, sink *captureSink, now time.Time) *Scanner {
	id := testIdentity("delivery-exact", now)
	return &Scanner{Store: store, Broker: broker, EventSink: sink, FingerprintKey: []byte("0123456789abcdef0123456789abcdef"), Bounds: Bounds{MaxCommits: 100, MaxPaths: 300, MaxBlobBytes: 1 << 20, MaxTotalBytes: 16 << 20, MaxCandidates: 4096, MaxDecodeDepth: 2}, Repositories: []string{id.Repository}, Refs: []string{id.Ref}, Profile: "general-writer-v1", ProfileGeneration: 1, CanaryAttribution: Attribution{Profile: "general-writer-v1", ProfileGeneration: 1, LogicalSessionID: "canary-logical", SessionLineageID: "canary-session", WorkerID: "canary-worker", WorkerStorageLineage: "canary-storage", WorkerFenceEpoch: 1}, Clock: func() time.Time { return now }}
}

func registerTestFingerprint(t *testing.T, store *Store, key []byte, token string, issued, expires time.Time, state string) {
	t.Helper()
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(token))
	err := store.RegisterFingerprint(context.Background(), mac.Sum(nil), FingerprintRegistration{FingerprintID: "fingerprint-01", Profile: "general-writer-v1", ProfileGeneration: 1, LogicalSessionID: "logical-01", SessionLineageID: "session-01", WorkerID: "worker-01", WorkerStorageLineage: "storage-01", WorkerFenceEpoch: 1, IssuedAt: issued, ExpiresAt: expires, RetainedUntil: expires.Add(24 * time.Hour), State: state})
	if err != nil {
		t.Fatal(err)
	}
}

func testIdentity(delivery string, now time.Time) PushIdentity {
	head, source := now.Add(-time.Minute), now.Add(-30*time.Second)
	return PushIdentity{DeliveryID: delivery, Repository: "grubbyhacker/gh-agent-broker", Ref: "refs/heads/agent/hermes-agent-infra/pr10-security-proof", Before: strings.Repeat("a", 40), After: strings.Repeat("b", 40), HeadTime: &head, SourceTime: &source, ReceivedAt: now, StreamSequence: 42}
}

func testMaterial(delivery, message string) Material {
	return Material{Version: WireVersion, DeliveryID: delivery, Repository: "grubbyhacker/gh-agent-broker", Ref: "refs/heads/agent/hermes-agent-infra/pr10-security-proof", Before: strings.Repeat("a", 40), After: strings.Repeat("b", 40), Commits: []Commit{{SHA: strings.Repeat("b", 40), Message: message}}, Bounds: MaterialBounds{CommitCount: 1, TotalBytes: int64(len(message))}, Complete: true}
}

func mustEventPayload(t *testing.T, store *Store, delivery string) []byte {
	t.Helper()
	var raw string
	if err := store.db.QueryRow(`SELECT payload_json FROM push_scan_security_events WHERE delivery_id=?`, delivery).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	return []byte(raw)
}
