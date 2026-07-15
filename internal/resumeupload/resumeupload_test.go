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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestGitHubHydrateStrictReleaseProvenance(t *testing.T) {
	content := []byte("# Structured resume\n")
	digest := sha256.Sum256(content)
	provider := "sha256:" + hex.EncodeToString(digest[:])
	for _, test := range []struct {
		name, mode string
		wantErr    bool
	}{
		{"valid", "", false}, {"ambiguous assets", "ambiguous", true}, {"bad tag", "tag", true}, {"repository mismatch", "repo", true}, {"digest mismatch", "digest", true}, {"draft", "draft", true}, {"target mismatch", "target", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			key, _ := rsa.GenerateKey(rand.Reader, 2048)
			keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: mustPKCS8(t, key)})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.Contains(r.URL.Path, "access_tokens"):
					json.NewEncoder(w).Encode(map[string]string{"token": "installation-token"})
				case r.URL.Path == "/repos/"+Repository:
					id := int64(42)
					if test.mode == "repo" {
						id = 43
					}
					json.NewEncoder(w).Encode(map[string]any{"id": id, "full_name": Repository})
				case strings.Contains(r.URL.Path, "/releases/77"):
					tag := "v2026.07.14-abcdef0"
					if test.mode == "tag" {
						tag = "v2026.07.14-deadbee"
					}
					assets := []map[string]any{{"id": 9, "name": "Roger_Fleig_20260714.structured.md", "size": len(content), "content_type": "text/markdown", "digest": provider}}
					if test.mode == "ambiguous" {
						assets = append(assets, map[string]any{"id": 10, "name": "Other_20260714.structured.md", "size": len(content), "content_type": "text/markdown", "digest": provider})
					}
					target := "abcdef0123456789abcdef0123456789abcdef01"
					if test.mode == "target" {
						target = "main"
					}
					json.NewEncoder(w).Encode(map[string]any{"id": 77, "tag_name": tag, "target_commitish": target, "draft": test.mode == "draft", "prerelease": false, "published_at": "2026-07-14T12:00:00Z", "assets": assets})
				case strings.Contains(r.URL.Path, "/git/ref/tags/"):
					json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"type": "commit", "sha": "abcdef0123456789abcdef0123456789abcdef01"}})
				case strings.Contains(r.URL.Path, "/releases/assets/"):
					body := content
					if test.mode == "digest" {
						body = []byte("# Changed resume\n")
					}
					w.Write(body)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client := GitHubClient{Client: server.Client(), APIBase: server.URL, PrivateKeyPEM: keyPEM, Now: func() time.Time { return time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC) }}
			operation, err := client.Hydrate(context.Background(), 42, InstallationID, 77)
			if (err != nil) != test.wantErr {
				t.Fatalf("operation=%#v err=%v", operation, err)
			}
			if err == nil && operation.ComputedDigest != provider {
				t.Fatalf("digest=%s", operation.ComputedDigest)
			}
		})
	}
}
func TestGitHubCredentialsFailClosedBeforeTraffic(t *testing.T) {
	if (&GitHubClient{PrivateKeyPEM: []byte("not a pem")}).ValidateCredentials() == nil {
		t.Fatal("invalid PEM accepted")
	}
}

func TestYKMAuthModesFailClosedAndSetOnlyOwnedHeaders(t *testing.T) {
	invalid := []YKMConfig{{BaseURL: "https://example.test/mcp", AuthMode: "caller"}, {BaseURL: "https://example.test/other", AuthMode: YKMAuthCloudflare, ClientID: "id", ClientSecret: "secret"}, {BaseURL: "http://external.test/mcp", AuthMode: YKMAuthLocal, LocalSecret: "secret"}, {BaseURL: "https://example.test/mcp", AuthMode: YKMAuthCloudflare, ClientID: "id", ClientSecret: "secret", LocalSecret: "also"}}
	for _, cfg := range invalid {
		if cfg.Validate() == nil {
			t.Fatalf("invalid config accepted: %#v", cfg)
		}
	}
	transport := &mcpTransport{}
	client := YKMClient{Config: YKMConfig{BaseURL: "https://mcp.fleiglabs.cc/mcp", AuthMode: YKMAuthCloudflare, ClientID: "id", ClientSecret: "secret"}, Client: &http.Client{Transport: transport}}
	response, err := client.Upload(context.Background(), "Roger_20260714.structured.md", "# Resume\n", "signal-plane:resume:v1:abc")
	if err != nil || response.UploadID != "upl_1" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	if transport.headers.Get("CF-Access-Client-Id") != "id" || transport.headers.Get("X-YKM-Local-Secret") != "" {
		t.Fatalf("headers=%v", transport.headers)
	}
}

func TestYKMRedirectCannotForwardSecrets(t *testing.T) {
	calls := 0
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		header := make(http.Header)
		header.Set("Location", "https://attacker.example/mcp")
		return &http.Response{StatusCode: http.StatusFound, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
	})
	client := YKMClient{Config: YKMConfig{BaseURL: "https://mcp.fleiglabs.cc/mcp", AuthMode: YKMAuthCloudflare, ClientID: "id", ClientSecret: "secret"}, Client: &http.Client{Transport: transport}}
	if _, err := client.Upload(context.Background(), "resume_x.structured.md", "# Resume\n", "signal-plane:resume:v1:abc"); err == nil || calls != 1 {
		t.Fatalf("redirect err=%v calls=%d", err, calls)
	}
}

func TestYKMStatelessStreamableHTTP(t *testing.T) {
	transport := &mcpTransport{stateless: true}
	client := YKMClient{Config: YKMConfig{BaseURL: "http://youknowme-mcp:8765/mcp", AuthMode: YKMAuthLocal, LocalSecret: "secret"}, Client: &http.Client{Transport: transport}}
	response, err := client.Upload(context.Background(), "resume_x.structured.md", "# Resume\n", "signal-plane:resume:v1:abc")
	if err != nil || response.UploadID != "upl_1" || transport.calls != 3 {
		t.Fatalf("response=%#v calls=%d err=%v", response, transport.calls, err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

type mcpTransport struct {
	calls     int
	headers   http.Header
	stateless bool
}

func (transport *mcpTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.calls++
	transport.headers = request.Header.Clone()
	session := ""
	var body string
	switch transport.calls {
	case 1:
		session = "session-1"
		body = `{"jsonrpc":"2.0","id":1,"result":{}}`
	case 2:
		body = `{}`
	default:
		body = `{"jsonrpc":"2.0","id":2,"result":{"isError":false,"content":[{"type":"text","text":"{\"accepted\":true,\"upload_id\":\"upl_1\",\"status\":\"pending\",\"file_count\":1,\"total_bytes\":9,\"warnings\":[],\"staged_path\":\"uploads/pending/upl_1\",\"replayed\":false}"}]}}`
	}
	header := make(http.Header)
	header.Set("content-type", "application/json")
	if session != "" && !transport.stateless {
		header.Set("Mcp-Session-Id", session)
	}
	return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestRouterRejectsAnythingExceptExactReleasePublished(t *testing.T) {
	for _, change := range []func(*envelope.Meta){func(m *envelope.Meta) { m.Namespace = "other/repo" }, func(m *envelope.Meta) { m.ObjectKind = "pull_request" }, func(m *envelope.Meta) { m.SourceAction = "created" }} {
		meta := envelope.Meta{SignalID: "s", Source: "github", SourceDeliveryID: "d", Namespace: Repository, ObjectKind: "release", ObjectID: "77", SourceEvent: "release", SourceAction: "published", SourceRevision: "rev", ReceivedAt: time.Now(), Authentication: envelope.Authentication{Method: "github_hmac_sha256", Verified: true}}
		change(&meta)
		data, _ := json.Marshal(envelope.Signal{Meta: meta, Payload: json.RawMessage(`{}`)})
		delivery := &fakeDelivery{data: data}
		router := Router{Stream: "SIGNALS"}
		if err := router.Process(context.Background(), delivery); err == nil || !delivery.termed {
			t.Fatalf("non-exact event accepted: %#v", meta)
		}
	}
}

func TestRouterAcknowledgesDurableDuplicateWithoutGitHub(t *testing.T) {
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := workledger.NewRegistry()
	executor := &Executor{Store: store, GitHub: fakeAssetReader{}, YKM: &fakeUploader{}}
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "resume-release", SchemaVersion: 1, SemanticVersion: "1", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 1}}
	snapshot, err := store.ActivateRoute(ctx, route, registry, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	_ = snapshot
	digest := "sha256:" + strings.Repeat("a", 64)
	hydrator := &fakeHydrator{operation: validOperation(digest)}
	router := Router{Store: store, Registry: registry, GitHub: hydrator, Stream: "SIGNALS"}
	payload := json.RawMessage(`{"repository":{"id":42,"full_name":"grubbyhacker/resume-builder"},"installation":{"id":146625575},"release":{"id":77}}`)
	signal := envelope.Signal{Meta: envelope.Meta{SignalID: "signal-1", Source: "github", SourceDeliveryID: "delivery-1", Namespace: Repository, ObjectKind: "release", ObjectID: "77", SourceEvent: "release", SourceAction: "published", SourceRevision: "rev", ReceivedAt: time.Now(), Authentication: envelope.Authentication{Method: "github_hmac_sha256", Verified: true}}, Payload: payload}
	data, _ := json.Marshal(signal)
	first := &fakeDelivery{data: data, sequence: 1}
	if err := router.Process(ctx, first); err != nil || !first.acked {
		t.Fatalf("first err=%v ack=%v", err, first.acked)
	}
	hydrator.err = errors.New("GitHub unavailable")
	second := &fakeDelivery{data: data, sequence: 2}
	if err := router.Process(ctx, second); err != nil || !second.acked || hydrator.calls != 1 {
		t.Fatalf("duplicate err=%v ack=%v calls=%d", err, second.acked, hydrator.calls)
	}
}

func TestRouterRecordsAndTerminatesExhaustedHydration(t *testing.T) {
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	hydrator := &fakeHydrator{err: errors.New("github unavailable")}
	router := Router{Store: store, GitHub: hydrator, Stream: "SIGNALS"}
	payload := json.RawMessage(`{"repository":{"id":42,"full_name":"grubbyhacker/resume-builder"},"installation":{"id":146625575},"release":{"id":77}}`)
	signal := envelope.Signal{Meta: envelope.Meta{SignalID: "signal-1", Source: "github", SourceDeliveryID: "delivery-exhausted", Namespace: Repository, ObjectKind: "release", ObjectID: "77", SourceEvent: "release", SourceAction: "published", SourceRevision: "rev", ReceivedAt: time.Now(), Authentication: envelope.Authentication{Method: "github_hmac_sha256", Verified: true}}, Payload: payload}
	data, _ := json.Marshal(signal)
	delivery := &fakeDelivery{data: data, sequence: 1, deliveries: 5}
	if err := router.Process(ctx, delivery); err == nil || !delivery.termed {
		t.Fatalf("exhausted err=%v termed=%v", err, delivery.termed)
	}
}

type fakeHydrator struct {
	operation workledger.ReleaseOperation
	err       error
	calls     int
}

func (h *fakeHydrator) Hydrate(context.Context, int64, int64, int64) (workledger.ReleaseOperation, error) {
	h.calls++
	return h.operation, h.err
}

func TestExecutorUsesContentIdempotencyForTimeoutAndLaterReleaseReplay(t *testing.T) {
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	registry := workledger.NewRegistry()
	uploader := &fakeUploader{err: errors.New("ambiguous timeout")}
	assetReader := fakeAssetReader{content: []byte("# Resume\n")}
	executor := &Executor{Store: store, GitHub: assetReader, YKM: uploader, Now: func() time.Time { return now }}
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "resume-release", SchemaVersion: 1, SemanticVersion: "1", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	digestBytes := sha256.Sum256(assetReader.content)
	digest := "sha256:" + hex.EncodeToString(digestBytes[:])
	operation := validOperation(digest)
	first := admitReleaseForTest(t, store, snapshot.ID, operation, "delivery-1", "77", "rev-1", 1, now)
	result, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: first})
	if err != nil || result.Outcome != workledger.OutcomeRetryableFailure || uploader.key != "signal-plane:resume:v1:"+strings.TrimPrefix(digest, "sha256:") {
		t.Fatalf("timeout result=%#v key=%q err=%v", result, uploader.key, err)
	}
	uploader.err = nil
	uploader.uploadID = "upl_1"
	firstFilename := uploader.filename
	secondOperation := operation
	secondOperation.ReleaseID = 78
	secondOperation.AssetID = 10
	secondOperation.AssetName = "Different_Profile_20260714.structured.md"
	second := admitReleaseForTest(t, store, snapshot.ID, secondOperation, "delivery-2", "78", "rev-2", 2, now.Add(time.Second))
	result, err = executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: second})
	if err != nil || result.Outcome != workledger.OutcomeCompleted {
		t.Fatalf("completed result=%#v err=%v", result, err)
	}
	if uploader.filename != firstFilename {
		t.Fatalf("content-stable filename changed: %q != %q", uploader.filename, firstFilename)
	}
	calls := uploader.calls
	replay, err := executor.Execute(ctx, workledger.ExecutorRequest{WorkItem: first})
	if err != nil || replay.ExternalCorrelation != "upl_1" || uploader.calls != calls {
		t.Fatalf("content replay=%#v calls=%d err=%v", replay, uploader.calls, err)
	}
}

func TestWorkOneEventuallyReconcilesRunningAttemptInProcess(t *testing.T) {
	ctx := context.Background()
	store, err := workledger.Open(filepath.Join(t.TempDir(), "ledger.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	content := []byte("# Resume\n")
	sum := sha256.Sum256(content)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	uploader := &fakeUploader{uploadID: "upl_1"}
	executor := &Executor{Store: store, GitHub: fakeAssetReader{content: content}, YKM: uploader, Now: func() time.Time { return now }}
	registry := workledger.NewRegistry()
	if err := registry.Register(executor); err != nil {
		t.Fatal(err)
	}
	route := workledger.RouteDefinition{ID: "resume-release", SchemaVersion: 1, SemanticVersion: "1", ExecutorID: ExecutorID, Admission: workledger.AdmissionPolicy{Sources: []string{"github"}, Namespaces: []string{Repository}, ObjectKinds: []string{"release"}, Events: []string{"release"}, Actions: []string{"published"}}, Concurrency: workledger.ConcurrencyPolicy{Serialization: workledger.SerializeObject}, Retry: workledger.RetryPolicy{MaxAttempts: 2, Backoff: []time.Duration{time.Second}}}
	snapshot, err := store.ActivateRoute(ctx, route, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	admitReleaseForTest(t, store, snapshot.ID, validOperation(digest), "delivery-stuck", "77", "rev", 1, now)
	if _, _, claimed, err := store.Claim(ctx, now); err != nil || !claimed {
		t.Fatalf("initial claim=%v err=%v", claimed, err)
	}
	router := Router{Store: store, Registry: registry, Now: func() time.Time { return now }}
	router.needsRecovery = true
	worked, err := router.WorkOne(ctx)
	if err != nil || !worked || uploader.calls != 1 {
		t.Fatalf("reconciled worked=%v calls=%d err=%v", worked, uploader.calls, err)
	}
	if _, _, claimed, err := store.Claim(ctx, now.Add(time.Hour)); err != nil || claimed {
		t.Fatalf("terminal work remained claimable=%v err=%v", claimed, err)
	}
}

type fakeAssetReader struct{ content []byte }

func (reader fakeAssetReader) DownloadVerified(context.Context, workledger.ReleaseOperation) ([]byte, error) {
	return reader.content, nil
}

type fakeUploader struct {
	calls                   int
	key, uploadID, filename string
	err                     error
}

func (uploader *fakeUploader) Upload(_ context.Context, filename, _, key string) (uploadResponse, error) {
	uploader.calls++
	uploader.key = key
	uploader.filename = filename
	if uploader.err != nil {
		return uploadResponse{}, uploader.err
	}
	return uploadResponse{UploadID: uploader.uploadID, Accepted: true}, nil
}
func validOperation(digest string) workledger.ReleaseOperation {
	return workledger.ReleaseOperation{Repository: Repository, RepositoryID: 42, InstallationID: InstallationID, ReleaseID: 77, Tag: "v2026.07.14-abcdef0", PublishedAt: "2026-07-14T12:00:00Z", TargetCommitish: "abcdef0123456789abcdef0123456789abcdef01", CommitSHA: "abcdef0123456789abcdef0123456789abcdef01", AssetID: 9, AssetName: "Roger_Fleig_20260714.structured.md", AssetSize: 9, AssetContentType: "text/markdown", ProviderDigest: digest, ComputedDigest: digest}
}
func admitReleaseForTest(t *testing.T, store *workledger.Store, snapshotID string, operation workledger.ReleaseOperation, delivery, object, revision string, sequence uint64, now time.Time) workledger.WorkItem {
	t.Helper()
	event := workledger.Event{SignalID: "signal-" + delivery, SourceDeliveryID: delivery, TransportStream: "SIGNALS", TransportSequence: sequence, Source: "github", Namespace: Repository, ObjectKind: "release", ObjectID: object, EventKind: "release", Action: "published", SourceRevision: revision, PayloadDigest: "sha256:payload-" + delivery, EvidenceRef: "jetstream://SIGNALS/1", ReceivedAt: now}
	admitted, err := store.AdmitRelease(context.Background(), snapshotID, event, operation, now)
	if err != nil {
		t.Fatal(err)
	}
	return admitted.WorkItem
}

type fakeDelivery struct {
	data       []byte
	termed     bool
	acked      bool
	sequence   uint64
	deliveries int
}

func (d *fakeDelivery) Data() []byte { return d.data }
func (d *fakeDelivery) StreamSequence() (uint64, error) {
	if d.sequence == 0 {
		return 1, nil
	}
	return d.sequence, nil
}
func (d *fakeDelivery) AckSync() error { d.acked = true; return nil }
func (d *fakeDelivery) Term() error    { d.termed = true; return nil }
func (d *fakeDelivery) NumDelivered() int {
	if d.deliveries == 0 {
		return 1
	}
	return d.deliveries
}

func mustPKCS8(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	value, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
