package resumeupload

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestStagingSyntheticReleaseToDeployedYouKnowMe(t *testing.T) {
	if os.Getenv("SIGNAL_PLANE_STAGING_E2E") != "1" {
		t.Skip("set SIGNAL_PLANE_STAGING_E2E=1 to run the guarded local staging proof")
	}
	stagingURL := os.Getenv("SIGNAL_PLANE_STAGING_YKM_URL")
	if stagingURL != "http://127.0.0.1:8765/mcp" {
		t.Fatal("SIGNAL_PLANE_STAGING_YKM_URL must be the fixed local staging endpoint")
	}
	localSecret := os.Getenv("SIGNAL_PLANE_STAGING_YKM_LOCAL_SECRET")
	if localSecret == "" {
		t.Fatal("SIGNAL_PLANE_STAGING_YKM_LOCAL_SECRET is required")
	}
	runID := os.Getenv("SIGNAL_PLANE_STAGING_RUN_ID")
	if !regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,39}$`).MatchString(runID) {
		t.Fatal("SIGNAL_PLANE_STAGING_RUN_ID must be a bounded lowercase identifier")
	}

	content := []byte("# Synthetic PR7 structured resume staging proof\n\nRun: " + runID + "\n")
	sum := sha256.Sum256(content)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	commits := map[int64]string{
		77: "abcdef0123456789abcdef0123456789abcdef01",
		78: "1234567890abcdef1234567890abcdef12345678",
	}
	assets := map[int64]int64{77: 9077, 78: 9078}
	var releaseReads atomic.Int64
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	githubServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/access_tokens"):
			_ = json.NewEncoder(w).Encode(map[string]string{"token": "staging-installation-token"})
		case r.URL.Path == "/repos/"+Repository:
			_ = json.NewEncoder(w).Encode(map[string]any{"id": 42, "full_name": Repository})
		case strings.Contains(r.URL.Path, "/releases/assets/"):
			_, _ = w.Write(content)
		case strings.Contains(r.URL.Path, "/releases/"):
			releaseReads.Add(1)
			var releaseID int64
			if strings.HasSuffix(r.URL.Path, "/77") {
				releaseID = 77
			} else if strings.HasSuffix(r.URL.Path, "/78") {
				releaseID = 78
			} else {
				http.NotFound(w, r)
				return
			}
			commit := commits[releaseID]
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": releaseID, "tag_name": "v2026.07.15-" + commit[:7],
				"target_commitish": commit, "draft": false, "prerelease": false,
				"published_at": "2026-07-15T08:00:00Z",
				"assets":       []map[string]any{{"id": assets[releaseID], "name": "Synthetic_20260715.structured.md", "size": len(content), "content_type": "text/markdown", "digest": digest}},
			})
		case strings.Contains(r.URL.Path, "/git/ref/tags/"):
			commit := commits[77]
			if strings.Contains(r.URL.Path, commits[78][:7]) {
				commit = commits[78]
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"type": "commit", "sha": commit}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer githubServer.Close()

	target, err := url.Parse(stagingURL)
	if err != nil {
		t.Fatal(err)
	}
	ykm := &YKMClient{
		Config: YKMConfig{BaseURL: "http://youknowme-mcp:8765/mcp", AuthMode: YKMAuthLocal, LocalSecret: localSecret},
		Client: &http.Client{Transport: stagingRewriteTransport{target: target, base: http.DefaultTransport}},
	}
	recordedYKM := &stagingRecordingUploader{client: ykm}
	github := &GitHubClient{
		Client:        githubServer.Client(),
		APIBase:       githubServer.URL,
		PrivateKeyPEM: pemPrivateKey(t, key),
		Now:           func() time.Time { return time.Date(2026, 7, 15, 8, 0, 0, 0, time.UTC) },
	}
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	executor := &Executor{Store: store, GitHub: github, YKM: recordedYKM}
	registry := workledger.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "resume-builder-release-upload", SchemaVersion: 1, SemanticVersion: "1.0.0", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	if _, err := store.ActivateRoute(ctx, route, registry, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	router := &Router{Store: store, Registry: registry, GitHub: github, Stream: "SIGNALS"}

	firstData := stagingSignal(t, "signal-first", "delivery-first", 77, commits[77][:7])
	first := &fakeDelivery{data: firstData, sequence: 1}
	if err := router.Process(ctx, first); err != nil || !first.acked {
		t.Fatalf("first delivery: ack=%v err=%v", first.acked, err)
	}
	if worked, err := router.WorkOne(ctx); err != nil || !worked {
		t.Fatalf("first execution: worked=%v err=%v", worked, err)
	}
	if recordedYKM.err != nil || recordedYKM.calls != 1 {
		t.Fatalf("first deployed YouKnowMe call: calls=%d response=%#v err=%v", recordedYKM.calls, recordedYKM.response, recordedYKM.err)
	}
	uploadID, resultDigest, ok, err := store.ContentResult(ctx, digest)
	if err != nil || !ok || uploadID == "" || resultDigest == "" {
		t.Fatalf("first content result: upload=%q result=%q ok=%v err=%v", uploadID, resultDigest, ok, err)
	}

	duplicate := &fakeDelivery{data: firstData, sequence: 2}
	if err := router.Process(ctx, duplicate); err != nil || !duplicate.acked || releaseReads.Load() != 2 {
		t.Fatalf("duplicate delivery: ack=%v release_reads=%d err=%v", duplicate.acked, releaseReads.Load(), err)
	}

	second := &fakeDelivery{data: stagingSignal(t, "signal-second", "delivery-second", 78, commits[78][:7]), sequence: 3}
	if err := router.Process(ctx, second); err != nil || !second.acked {
		t.Fatalf("same-content release: ack=%v err=%v", second.acked, err)
	}
	if worked, err := router.WorkOne(ctx); err != nil || !worked {
		t.Fatalf("same-content execution: worked=%v err=%v", worked, err)
	}
	if recordedYKM.calls != 1 {
		t.Fatalf("same-content release called YouKnowMe again: calls=%d want=1", recordedYKM.calls)
	}
	replayedUploadID, replayedResult, ok, err := store.ContentResult(ctx, digest)
	if err != nil || !ok || replayedUploadID != uploadID || replayedResult != resultDigest {
		t.Fatalf("content replay: upload=%q result=%q ok=%v err=%v", replayedUploadID, replayedResult, ok, err)
	}
	filename := "resume_" + strings.TrimPrefix(digest, "sha256:") + ".structured.md"
	idempotencyKey := "signal-plane:resume:v1:" + strings.TrimPrefix(digest, "sha256:")
	downstreamReplay, err := ykm.Upload(ctx, filename, string(content), idempotencyKey)
	if err != nil || !downstreamReplay.Replayed || downstreamReplay.UploadID != uploadID {
		t.Fatalf("downstream replay: response=%#v err=%v", downstreamReplay, err)
	}
	t.Logf("proof upload_id=%s digest=%s duplicate_delivery_acked=true content_replay=true downstream_replayed=true", uploadID, digest)
}

type stagingRewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

type stagingRecordingUploader struct {
	client   *YKMClient
	calls    int
	response uploadResponse
	err      error
}

func (uploader *stagingRecordingUploader) Upload(ctx context.Context, filename, content, key string) (uploadResponse, error) {
	uploader.calls++
	uploader.response, uploader.err = uploader.client.Upload(ctx, filename, content, key)
	return uploader.response, uploader.err
}

func (transport stagingRewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	copy := request.Clone(request.Context())
	copy.URL.Scheme = transport.target.Scheme
	copy.URL.Host = transport.target.Host
	copy.URL.Path = transport.target.Path
	copy.Host = transport.target.Host
	return transport.base.RoundTrip(copy)
}

func stagingSignal(t *testing.T, signalID, deliveryID string, releaseID int64, revision string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"repository": map[string]any{"id": 42, "full_name": Repository}, "installation": map[string]any{"id": InstallationID}, "release": map[string]any{"id": releaseID}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(envelope.Signal{Meta: envelope.Meta{SignalID: signalID, Source: "github", SourceDeliveryID: deliveryID, Namespace: Repository, ObjectKind: "release", ObjectID: fmt.Sprint(releaseID), SourceEvent: "release", SourceAction: "published", SourceRevision: revision, ReceivedAt: time.Now().UTC(), Authentication: envelope.Authentication{Method: "github_hmac_sha256", Verified: true}}, Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func pemPrivateKey(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
}
