package dispatcher

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/grubbyhacker/signal-plane/internal/gateway"
	"github.com/nats-io/nats-server/v2/server"
)

const (
	phase5V1Secret  = "phase5-v1-local-only-secret"
	phase5V1Stream  = "PHASE5_V1_SIGNALS"
	phase5V1Subject = "signals.github.phase5-v1"
)

// TestPhase5LocalV1Proof is the authoritative, versioned Phase 5 proof. Keep
// its public entry point stable: vps-ops invokes it through
// `mise run proof:phase5-local`, never by selecting individual subtests.
func TestPhase5LocalV1Proof(t *testing.T) {
	t.Run("signed ingress through terminal broker state and checkpoint replay", phase5V1EndToEnd)
	t.Run("launch retry and permanent error policy", phase5V1RetryPolicy)
	t.Run("terminal status mapping", phase5V1TerminalMapping)
}

func phase5V1EndToEnd(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fixture := phase5V1Fixture(t)
	bus := phase5V1Bus(t)
	t.Setenv("PHASE5_V1_GITHUB_SECRET", phase5V1Secret)

	route := config.Route{
		ID: "github-phase5-v1", Path: "/webhooks/github", Source: "github", MaxBodyBytes: 1 << 20, PublishSubject: phase5V1Subject,
		GitHub: config.GitHubConfig{WebhookSecretEnv: "PHASE5_V1_GITHUB_SECRET"},
		Admission: config.AdmissionSet{Tuples: []config.AdmissionTuple{
			{Repository: Repository, Event: "issues", Actions: []string{"labeled"}},
			{Repository: "grubbyhacker/signal-plane", Event: "pull_request", Actions: []string{"opened"}},
		}},
	}
	gatewayServer := httptest.NewServer(gateway.New(logger, []config.Route{route}, bus).Handler())
	t.Cleanup(gatewayServer.Close)

	phase5V1Post(t, gatewayServer, fixture, "issues", "phase5-v1-original", http.StatusAccepted)
	// A duplicate GitHub delivery is accepted at HTTP admission but JetStream's
	// message ID barrier keeps only one transport message.
	phase5V1Post(t, gatewayServer, fixture, "issues", "phase5-v1-original", http.StatusAccepted)
	phase5V1Post(t, gatewayServer, fixture, "issues", "phase5-v1-relabel", http.StatusAccepted)
	phase5V1Post(t, gatewayServer, phase5V1Mutate(t, fixture, func(event map[string]any) {
		event["label"] = map[string]any{"name": "triage"}
	}), "issues", "phase5-v1-wrong-label", http.StatusAccepted)
	phase5V1Post(t, gatewayServer, phase5V1Mutate(t, fixture, func(event map[string]any) {
		delete(event, "label")
	}), "issues", "phase5-v1-missing-label", http.StatusAccepted)
	// This repository/event pair exists only in the Cartesian product of the
	// configured tuples and therefore must be rejected by gateway admission.
	phase5V1Post(t, gatewayServer, phase5V1Mutate(t, fixture, func(event map[string]any) {
		event["repository"] = map[string]any{"full_name": "grubbyhacker/signal-plane"}
	}), "issues", "phase5-v1-cartesian-negative", http.StatusForbidden)
	secondIssue := phase5V1Mutate(t, fixture, func(event map[string]any) {
		event["issue"].(map[string]any)["number"] = float64(43)
	})
	phase5V1Post(t, gatewayServer, secondIssue, "issues", "phase5-v1-second-issue", http.StatusAccepted)

	consumer, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: phase5V1Subject, Durable: "phase5-v1-dispatcher", AckWait: time.Second, MaxAckPending: 1, MaxDeliver: 3})
	if err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(t.TempDir(), "phase5-v1.db")
	store, err := OpenStore(database)
	if err != nil {
		t.Fatal(err)
	}
	metrics := NewMetrics()
	started := time.Unix(1_800_000_000, 0).UTC()
	for i := 0; i < 5; i++ {
		message, err := consumer.Fetch(time.Second)
		if err != nil {
			t.Fatalf("fetch admitted message %d: %v", i+1, err)
		}
		if !Process(ctx, logger, metrics, store, NATSDelivery{Message: message}, started.Add(time.Duration(i)*time.Millisecond)) {
			t.Fatalf("process admitted message %d", i+1)
		}
	}

	deliveries, jobs, err := store.Counts(ctx)
	if err != nil || deliveries != 5 || jobs != 2 {
		t.Fatalf("durable counts deliveries=%d jobs=%d err=%v", deliveries, jobs, err)
	}
	phase5V1AssertDeliveryOutcome(t, store, "phase5-v1-original", "accepted")
	phase5V1AssertDeliveryOutcome(t, store, "phase5-v1-relabel", "accepted")
	phase5V1AssertDeliveryOutcome(t, store, "phase5-v1-wrong-label", "label_filtered")
	phase5V1AssertDeliveryOutcome(t, store, "phase5-v1-missing-label", "label_filtered")
	var firstDelivery string
	if err := store.db.QueryRow(`SELECT source_delivery_id FROM jobs WHERE issue_number=42`).Scan(&firstDelivery); err != nil || firstDelivery != "phase5-v1-original" {
		t.Fatalf("semantic job audit delivery=%q err=%v", firstDelivery, err)
	}

	fakeBroker := newPhase5V1HTTPBroker(t, map[string][]string{"run-phase5-v1": {"running", "completed"}})
	broker := &Broker{URL: fakeBroker.URL() + config.BrokerProfilePath, Token: "phase5-v1-token", Client: fakeBroker.Client()}
	launchAt := started.Add(time.Minute)
	worked, err := RunOne(ctx, logger, metrics, store, broker, launchAt)
	if err != nil || !worked {
		t.Fatalf("launch correlated job worked=%v err=%v", worked, err)
	}
	launches, statuses := fakeBroker.Snapshot()
	if len(launches) != 1 || len(statuses) != 0 {
		t.Fatalf("broker calls after launch: launches=%d statuses=%v", len(launches), statuses)
	}
	wantKey := "github-task-dispatcher:v2:" + Repository + ":issue:42:" + Profile
	wantFingerprint := brokerSourceID(Job{Repository: Repository, IssueNumber: 42, DeliveryID: "any-delivery"})
	if wantFingerprint != brokerSourceID(Job{Repository: Repository, IssueNumber: 42, DeliveryID: "phase5-v1-original"}) ||
		wantFingerprint != brokerSourceID(Job{Repository: Repository, IssueNumber: 42, DeliveryID: "phase5-v1-relabel"}) {
		t.Fatal("broker fingerprint changed across duplicate delivery or relabel")
	}
	if launches[0].idempotencyKey != wantKey || launches[0].issueNumber != 42 || launches[0].sourceDeliveryID != wantFingerprint {
		t.Fatalf("launch=%+v want key=%q fingerprint=%q", launches[0], wantKey, wantFingerprint)
	}
	if worked, err := RunOne(ctx, logger, metrics, store, broker, launchAt.Add(time.Second)); err != nil || worked {
		t.Fatalf("launched job must block issue 43 before status due: worked=%v err=%v", worked, err)
	}

	// Reopening the same SQLite file must resume scoped status polling using the
	// persisted run ID, without repeating the broker launch.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(database)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if worked, err := RunOne(ctx, logger, metrics, store, broker, launchAt.Add(StatusPollInterval)); err != nil || !worked {
		t.Fatalf("resume first status poll worked=%v err=%v", worked, err)
	}
	if worked, err := RunOne(ctx, logger, metrics, store, broker, launchAt.Add(3*time.Second)); err != nil || worked {
		t.Fatalf("running job must block issue 43: worked=%v err=%v", worked, err)
	}
	if worked, err := RunOne(ctx, logger, metrics, store, broker, launchAt.Add(2*StatusPollInterval)); err != nil || !worked {
		t.Fatalf("terminal status poll worked=%v err=%v", worked, err)
	}
	launches, statuses = fakeBroker.Snapshot()
	if len(launches) != 1 || len(statuses) != 2 || statuses[0] != "/v1/runs/run-phase5-v1" || statuses[1] != "/v1/runs/run-phase5-v1" {
		t.Fatalf("restart relaunched or used unscoped status endpoint: launches=%d statuses=%v", len(launches), statuses)
	}
	var state string
	if err := store.db.QueryRow(`SELECT status FROM jobs WHERE issue_number=42`).Scan(&state); err != nil || state != StateCompleted {
		t.Fatalf("correlated terminal state=%q err=%v", state, err)
	}

	_, checkpoint, recoveryStart, err := store.RecoveryMetadata(ctx)
	if err != nil || checkpoint != 5 || recoveryStart != checkpoint+1 {
		t.Fatalf("checkpoint=%d recovery_start=%d err=%v", checkpoint, recoveryStart, err)
	}
	replayFixture := phase5V1Mutate(t, fixture, func(event map[string]any) {
		event["label"] = map[string]any{"name": "proof:checkpoint"}
	})
	phase5V1Post(t, gatewayServer, replayFixture, "issues", "phase5-v1-after-checkpoint", http.StatusAccepted)
	recoveryConsumer, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: phase5V1Subject, Durable: "phase5-v1-recovery", AckWait: time.Second, MaxAckPending: 1, MaxDeliver: 3, StartSequence: recoveryStart})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := recoveryConsumer.Fetch(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := replayed.Metadata()
	if err != nil || metadata.Sequence.Stream != checkpoint+1 {
		t.Fatalf("replayed stream sequence=%d want=%d err=%v", metadata.Sequence.Stream, checkpoint+1, err)
	}
}

func phase5V1RetryPolicy(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Unix(1_810_000_000, 0).UTC()

	for _, test := range []struct {
		name      string
		transport http.RoundTripper
		status    int
		body      string
	}{
		{name: "transport", transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("deterministic transport failure") })},
		{name: "429", status: http.StatusTooManyRequests, body: `{}`},
		{name: "5xx", status: http.StatusServiceUnavailable, body: `{}`},
		{name: "profile_busy", status: http.StatusConflict, body: `{"error":{"code":"profile_busy","message":"busy"}}`},
	} {
		t.Run("retry_"+test.name, func(t *testing.T) {
			store := phase5V1StoreWithJobs(t, now, 51, 52)
			client := &http.Client{Transport: test.transport}
			var server *httptest.Server
			if test.transport == nil {
				server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.body))
				}))
				t.Cleanup(server.Close)
				client = server.Client()
			}
			baseURL := "http://phase5-v1.invalid"
			if server != nil {
				baseURL = server.URL
			}
			broker := &Broker{URL: baseURL + config.BrokerProfilePath, Client: client}
			if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now); err != nil || !worked {
				t.Fatalf("first retryable attempt worked=%v err=%v", worked, err)
			}
			phase5V1AssertJobTiming(t, store, 51, StateLaunchRetry, 1, now.Add(2*time.Second))
			if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now.Add(time.Second)); err != nil || worked {
				t.Fatalf("retry wait must block second launch: worked=%v err=%v", worked, err)
			}
			if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now.Add(2*time.Second)); err != nil || !worked {
				t.Fatalf("second retryable attempt worked=%v err=%v", worked, err)
			}
			phase5V1AssertJobTiming(t, store, 51, StateLaunchRetry, 2, now.Add(6*time.Second))
			var secondAttempts int
			if err := store.db.QueryRow(`SELECT attempts FROM jobs WHERE issue_number=52`).Scan(&secondAttempts); err != nil || secondAttempts != 0 {
				t.Fatalf("second job attempts=%d err=%v", secondAttempts, err)
			}
		})
	}

	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "authentication", status: http.StatusUnauthorized, body: `{}`},
		{name: "validation", status: http.StatusUnprocessableEntity, body: `{}`},
		{name: "idempotency_conflict", status: http.StatusConflict, body: `{"code":"idempotency_conflict","message":"different request"}`},
	} {
		t.Run("permanent_"+test.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			t.Cleanup(server.Close)
			store := phase5V1StoreWithJobs(t, now, 61)
			broker := &Broker{URL: server.URL + config.BrokerProfilePath, Client: server.Client()}
			if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now); err != nil || !worked {
				t.Fatalf("permanent attempt worked=%v err=%v", worked, err)
			}
			phase5V1AssertJobTiming(t, store, 61, StateFailed, 1, now.Add(LaunchRetryDelay(1)))
			if worked, err := RunOne(ctx, logger, NewMetrics(), store, broker, now.Add(time.Hour)); err != nil || worked || calls != 1 {
				t.Fatalf("permanent error retried: worked=%v calls=%d err=%v", worked, calls, err)
			}
		})
	}
}

func phase5V1TerminalMapping(t *testing.T) {
	for _, test := range []struct{ broker, stored string }{
		{broker: "completed", stored: StateCompleted},
		{broker: "failed", stored: StateFailed},
		{broker: "timed_out", stored: StateTimedOut},
	} {
		t.Run(test.broker, func(t *testing.T) {
			now := time.Unix(1_820_000_000, 0).UTC()
			store := phase5V1StoreWithJobs(t, now, 71)
			broker := &phase5V1SequenceBroker{status: test.broker}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			if worked, err := RunOne(context.Background(), logger, NewMetrics(), store, broker, now); err != nil || !worked {
				t.Fatalf("launch worked=%v err=%v", worked, err)
			}
			if worked, err := RunOne(context.Background(), logger, NewMetrics(), store, broker, now.Add(StatusPollInterval)); err != nil || !worked {
				t.Fatalf("status worked=%v err=%v", worked, err)
			}
			var state string
			if err := store.db.QueryRow(`SELECT status FROM jobs WHERE issue_number=71`).Scan(&state); err != nil || state != test.stored {
				t.Fatalf("broker=%s stored=%s err=%v", test.broker, state, err)
			}
		})
	}
}

type phase5V1Launch struct {
	idempotencyKey   string
	issueNumber      int64
	sourceDeliveryID string
}

type phase5V1HTTPBroker struct {
	server   *httptest.Server
	mu       sync.Mutex
	launches []phase5V1Launch
	statuses []string
	runs     map[string][]string
}

func newPhase5V1HTTPBroker(t *testing.T, runs map[string][]string) *phase5V1HTTPBroker {
	t.Helper()
	fake := &phase5V1HTTPBroker{runs: runs}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (fake *phase5V1HTTPBroker) URL() string          { return fake.server.URL }
func (fake *phase5V1HTTPBroker) Client() *http.Client { return fake.server.Client() }

func (fake *phase5V1HTTPBroker) serveHTTP(w http.ResponseWriter, r *http.Request) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost && r.URL.Path == config.BrokerProfilePath {
		var request struct {
			Parameters struct {
				IssueNumber      int64  `json:"issue_number"`
				SourceDeliveryID string `json:"source_delivery_id"`
			} `json:"parameters"`
		}
		if r.Header.Get("Authorization") != "Bearer phase5-v1-token" || json.NewDecoder(r.Body).Decode(&request) != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fake.launches = append(fake.launches, phase5V1Launch{r.Header.Get("Idempotency-Key"), request.Parameters.IssueNumber, request.Parameters.SourceDeliveryID})
		_, _ = io.WriteString(w, `{"run_id":"run-phase5-v1"}`)
		return
	}
	if r.Method == http.MethodGet && strings.HasPrefix(r.URL.EscapedPath(), "/v1/runs/") {
		if r.Header.Get("Authorization") != "Bearer phase5-v1-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fake.statuses = append(fake.statuses, r.URL.EscapedPath())
		runID := strings.TrimPrefix(r.URL.Path, "/v1/runs/")
		states := fake.runs[runID]
		if len(states) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		status := states[0]
		fake.runs[runID] = states[1:]
		_, _ = io.WriteString(w, `{"run_id":`+strconv.Quote(runID)+`,"status":`+strconv.Quote(status)+`}`)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

func (fake *phase5V1HTTPBroker) Snapshot() ([]phase5V1Launch, []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]phase5V1Launch(nil), fake.launches...), append([]string(nil), fake.statuses...)
}

type phase5V1SequenceBroker struct{ status string }

func (*phase5V1SequenceBroker) Launch(context.Context, Job) (LaunchResult, error) {
	return LaunchResult{RunID: "phase5-v1-terminal-run"}, nil
}
func (broker *phase5V1SequenceBroker) Status(context.Context, string) (RunStatus, error) {
	return RunStatus{RunID: "phase5-v1-terminal-run", Status: broker.status}, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func phase5V1Bus(t *testing.T) *eventbus.Bus {
	t.Helper()
	natsServer, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	natsServer.Start()
	if !natsServer.ReadyForConnections(5 * time.Second) {
		natsServer.Shutdown()
		t.Fatal("in-process NATS server did not become ready")
	}
	t.Cleanup(natsServer.Shutdown)
	bus, err := eventbus.Connect(natsServer.ClientURL(), phase5V1Stream, []string{phase5V1Subject})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	return bus
}

func phase5V1Fixture(t *testing.T) []byte {
	t.Helper()
	body, err := os.ReadFile("testdata/phase5/v1/apple-jobs-matcher-issues-labeled.json")
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func phase5V1Mutate(t *testing.T, fixture []byte, mutate func(map[string]any)) []byte {
	t.Helper()
	var event map[string]any
	if err := json.Unmarshal(fixture, &event); err != nil {
		t.Fatal(err)
	}
	mutate(event)
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func phase5V1Post(t *testing.T, server *httptest.Server, body []byte, event, delivery string, wantStatus int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/webhooks/github", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Hub-Signature-256", phase5V1Signature(body))
	request.Header.Set("X-GitHub-Event", event)
	request.Header.Set("X-GitHub-Delivery", delivery)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode != wantStatus {
		t.Fatalf("delivery %s status=%d want=%d body=%s", delivery, response.StatusCode, wantStatus, responseBody)
	}
}

func phase5V1Signature(body []byte) string {
	mac := hmac.New(sha256.New, []byte(phase5V1Secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func phase5V1AssertDeliveryOutcome(t *testing.T, store *Store, deliveryID, want string) {
	t.Helper()
	var outcome string
	if err := store.db.QueryRow(`SELECT outcome FROM deliveries WHERE delivery_id=?`, deliveryID).Scan(&outcome); err != nil || outcome != want {
		t.Fatalf("delivery %s outcome=%q want=%q err=%v", deliveryID, outcome, want, err)
	}
}

func phase5V1StoreWithJobs(t *testing.T, now time.Time, issues ...int64) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "phase5-v1-policy.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	for index, issue := range issues {
		delivery := "phase5-v1-policy-" + strconv.FormatInt(issue, 10)
		candidate := Candidate{Repository: Repository, IssueNumber: issue, DeliveryID: delivery}
		if err := store.Record(context.Background(), delivery, "accepted", uint64(index+1), &candidate, now.Add(time.Duration(index)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

func phase5V1AssertJobTiming(t *testing.T, store *Store, issue int64, wantState string, wantAttempts int, wantDue time.Time) {
	t.Helper()
	var state string
	var attempts int
	var due int64
	if err := store.db.QueryRow(`SELECT status,attempts,due_at FROM jobs WHERE issue_number=?`, issue).Scan(&state, &attempts, &due); err != nil {
		t.Fatal(err)
	}
	if state != wantState || attempts != wantAttempts || due != wantDue.UnixMilli() {
		t.Fatalf("issue=%d state=%s attempts=%d due=%s want state=%s attempts=%d due=%s", issue, state, attempts, time.UnixMilli(due), wantState, wantAttempts, wantDue)
	}
}
