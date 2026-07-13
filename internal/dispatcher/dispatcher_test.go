package dispatcher

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

func validSignal(delivery string, issue int64) envelope.Signal {
	payload := map[string]any{"action": "labeled", "repository": map[string]any{"full_name": Repository}, "issue": map[string]any{"number": issue, "state": "open"}, "label": map[string]any{"name": "agent:implement"}, "sender": map[string]any{"login": "roger"}}
	body, _ := json.Marshal(payload)
	return envelope.Signal{Meta: envelope.Meta{Source: "github", SourceEvent: "issues", SourceAction: "labeled", SourceDeliveryID: delivery}, Payload: body}
}

func TestSelectPredicate(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*envelope.Signal)
		accepted bool
	}{
		{"accepted", func(*envelope.Signal) {}, true},
		{"wrong event", func(s *envelope.Signal) { s.Meta.SourceEvent = "pull_request" }, false},
		{"missing delivery", func(s *envelope.Signal) { s.Meta.SourceDeliveryID = "" }, false},
		{"wrong repository", func(s *envelope.Signal) {
			s.Payload = []byte(`{"action":"labeled","repository":{"full_name":"grubbyhacker/other"},"issue":{"number":7,"state":"open"},"label":{"name":"agent:implement"},"sender":{"login":"x"}}`)
		}, false},
		{"wrong label", func(s *envelope.Signal) {
			s.Payload = []byte(`{"action":"labeled","repository":{"full_name":"grubbyhacker/apple-jobs-matcher"},"issue":{"number":7,"state":"open"},"label":{"name":"triage"},"sender":{"login":"x"}}`)
		}, false},
		{"nonpositive issue", func(s *envelope.Signal) {
			s.Payload = []byte(`{"action":"labeled","repository":{"full_name":"grubbyhacker/apple-jobs-matcher"},"issue":{"number":0,"state":"open"},"label":{"name":"agent:implement"},"sender":{"login":"x"}}`)
		}, false},
		{"missing sender", func(s *envelope.Signal) {
			s.Payload = []byte(`{"action":"labeled","repository":{"full_name":"grubbyhacker/apple-jobs-matcher"},"issue":{"number":7,"state":"open"},"label":{"name":"agent:implement"},"sender":{}}`)
		}, false},
		{"closed", func(s *envelope.Signal) {
			s.Payload = []byte(`{"action":"labeled","repository":{"full_name":"grubbyhacker/apple-jobs-matcher"},"issue":{"number":7,"state":"closed"},"label":{"name":"agent:implement"},"sender":{"login":"x"}}`)
		}, false},
		{"pull request", func(s *envelope.Signal) {
			s.Payload = []byte(`{"action":"labeled","repository":{"full_name":"grubbyhacker/apple-jobs-matcher"},"issue":{"number":7,"state":"open","pull_request":{"url":"x"}},"label":{"name":"agent:implement"},"sender":{"login":"x"}}`)
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := validSignal("d", 7)
			tt.mutate(&s)
			c, outcome := Select(s)
			if (outcome == "accepted") != tt.accepted {
				t.Fatalf("outcome=%s", outcome)
			}
			if tt.accepted && c.SemanticKey() != "github-issue-implement:v1:grubbyhacker/apple-jobs-matcher:7" {
				t.Fatal(c.SemanticKey())
			}
		})
	}
}

func TestStoreDeliveryAndSemanticDedupeAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	now := time.Unix(100, 0)
	ctx := context.Background()
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := Select(validSignal("delivery-1", 42))
	b, _ := Select(validSignal("delivery-2", 42))
	if err := s.Record(ctx, a.DeliveryID, "accepted", 10, &a, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, a.DeliveryID, "accepted", 10, &a, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, b.DeliveryID, "accepted", 11, &b, now); err != nil {
		t.Fatal(err)
	}
	deliveries, jobs, err := s.Counts(ctx)
	if err != nil || deliveries != 2 || jobs != 1 {
		t.Fatalf("counts=%d,%d err=%v", deliveries, jobs, err)
	}
	s.Close()
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	deliveries, jobs, err = s.Counts(ctx)
	if err != nil || deliveries != 2 || jobs != 1 {
		t.Fatalf("restart counts=%d,%d err=%v", deliveries, jobs, err)
	}
	work, ok, err := s.ClaimDue(ctx, now)
	if err != nil || !ok || work.Job.DeliveryID != "delivery-1" {
		t.Fatalf("work=%+v ok=%v err=%v", work, ok, err)
	}
	sequence, err := s.RecoverySequence(ctx)
	if err != nil || sequence != 12 {
		t.Fatalf("recovery sequence=%d err=%v", sequence, err)
	}
}

func TestBrokerRequest(t *testing.T) {
	var header, auth, path string
	var body map[string]any
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		header = r.Header.Get("Idempotency-Key")
		auth = r.Header.Get("Authorization")
		path = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"run_id":"run-123","status":"running"}`))
	}))
	defer server.Close()
	b := Broker{URL: server.URL + "/v1/launch-profiles/codex-issue-implement/launch", Token: "token", Client: server.Client()}
	job := Job{Repository: Repository, IssueNumber: 9, DeliveryID: "abc"}
	for i := 0; i < 2; i++ {
		result, err := b.Launch(context.Background(), job)
		if err != nil || result.RunID != "run-123" {
			t.Fatalf("launch %d result=%+v err=%v", i+1, result, err)
		}
	}
	if calls != 2 || path != "/v1/launch-profiles/codex-issue-implement/launch" {
		t.Fatalf("calls=%d path=%q", calls, path)
	}
	if header != "github-task-dispatcher:v2:grubbyhacker/apple-jobs-matcher:issue:9:codex-issue-implement" || auth != "Bearer token" {
		t.Fatalf("headers %q %q", header, auth)
	}
	want := map[string]any{"parameters": map[string]any{"issue_number": float64(9), "source_delivery_id": "abc"}}
	if !reflect.DeepEqual(body, want) {
		t.Fatalf("body=%#v", body)
	}
}

func TestBrokerRejectsInvalidSuccessResponses(t *testing.T) {
	for _, body := range []string{"", `{}`, `{"run_id":""}`, `not-json`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			_, err := (&Broker{URL: server.URL, Client: server.Client()}).Launch(context.Background(), Job{})
			if err == nil {
				t.Fatal("expected invalid response error")
			}
			if IsRetryable(err) {
				t.Fatalf("malformed success must fail immediately: %v", err)
			}
		})
	}
}

type launchSequence struct {
	errors       []error
	calls        int
	statuses     []RunStatus
	statusErrors []error
	statusCalls  int
}

func (l *launchSequence) Launch(context.Context, Job) (LaunchResult, error) {
	var err error
	if l.calls < len(l.errors) {
		err = l.errors[l.calls]
	}
	l.calls++
	return LaunchResult{RunID: "run-sequence"}, err
}

func TestLaunchRetrySchedule(t *testing.T) {
	want := []time.Duration{2, 4, 8, 16, 20, 20}
	for i, seconds := range want {
		if got := LaunchRetryDelay(i + 1); got != seconds*time.Second {
			t.Fatalf("attempt %d delay=%s want=%s", i+1, got, seconds*time.Second)
		}
	}
}

func TestBrokerRetryClassification(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		retryable bool
	}{
		{"rate limited", 429, `{}`, true},
		{"server error", 503, `{}`, true},
		{"profile busy", 409, `{"error":{"code":"profile_busy","message":"busy"}}`, true},
		{"authentication", 401, `{}`, false},
		{"authorization", 403, `{}`, false},
		{"validation", 422, `{}`, false},
		{"idempotency conflict", 409, `{"code":"idempotency_conflict","message":"different request"}`, false},
		{"other permanent", 418, `{}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()
			_, err := (&Broker{URL: server.URL, Client: server.Client()}).Launch(context.Background(), Job{})
			if err == nil || IsRetryable(err) != tt.retryable {
				t.Fatalf("error=%v retryable=%v", err, IsRetryable(err))
			}
		})
	}
	if !IsRetryable(BrokerError{Transport: true, Message: "network"}) {
		t.Fatal("transport failure must retry")
	}
}

func TestStatusLifecycleSerializesLaunches(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(3000, 0)
	s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	first, _ := Select(validSignal("first", 1))
	second, _ := Select(validSignal("second", 2))
	_ = s.Record(ctx, "first", "accepted", 1, &first, now)
	_ = s.Record(ctx, "second", "accepted", 2, &second, now)
	broker := &launchSequence{statuses: []RunStatus{{RunID: "run-sequence", Status: "running"}, {RunID: "run-sequence", Status: "completed"}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics()
	late := now.Add(11 * time.Minute)
	for _, at := range []time.Time{now, late, late.Add(2 * time.Second)} {
		if worked, err := RunOne(ctx, logger, metrics, s, broker, at); err != nil || !worked {
			t.Fatalf("at=%s worked=%v err=%v", at, worked, err)
		}
	}
	if broker.calls != 1 || broker.statusCalls != 2 {
		t.Fatalf("launches=%d status calls=%d", broker.calls, broker.statusCalls)
	}
	if worked, err := RunOne(ctx, logger, metrics, s, broker, late.Add(2*time.Second)); err != nil || !worked {
		t.Fatalf("second launch worked=%v err=%v", worked, err)
	}
	if broker.calls != 2 {
		t.Fatalf("launches=%d want=2", broker.calls)
	}
}

func TestBrokerTerminalStatusMapping(t *testing.T) {
	for _, tt := range []struct{ broker, stored string }{
		{"completed", StateCompleted},
		{"failed", StateFailed},
		{"timed_out", StateTimedOut},
	} {
		t.Run(tt.broker, func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(3500, 0)
			s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
			defer s.Close()
			candidate, _ := Select(validSignal("terminal", 3))
			_ = s.Record(ctx, "terminal", "accepted", 1, &candidate, now)
			broker := &launchSequence{statuses: []RunStatus{{RunID: "run-sequence", Status: tt.broker}}}
			metrics := NewMetrics()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			_, _ = RunOne(ctx, logger, metrics, s, broker, now)
			_, _ = RunOne(ctx, logger, metrics, s, broker, now.Add(StatusPollInterval))
			var state string
			if err := s.db.QueryRow(`SELECT status FROM jobs`).Scan(&state); err != nil || state != tt.stored {
				t.Fatalf("state=%s err=%v", state, err)
			}
		})
	}
}

func TestCrashBeforeLaunchResponseReplaysSameSemanticJob(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(4000, 0)
	path := filepath.Join(t.TempDir(), "db")
	s, _ := OpenStore(path)
	candidate, _ := Select(validSignal("audit-delivery", 8))
	_ = s.Record(ctx, candidate.DeliveryID, "accepted", 7, &candidate, now)
	if work, ok, err := s.ClaimDue(ctx, now); err != nil || !ok || work.Job.Attempts != 1 {
		t.Fatalf("claim before crash=%+v ok=%v err=%v", work, ok, err)
	}
	_ = s.Close()
	s, _ = OpenStore(path)
	defer s.Close()
	broker := &launchSequence{}
	if worked, err := RunOne(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(), s, broker, now); err != nil || !worked {
		t.Fatalf("replay worked=%v err=%v", worked, err)
	}
	var attempts int
	var status string
	_ = s.db.QueryRow(`SELECT attempts,status FROM jobs`).Scan(&attempts, &status)
	if attempts != 2 || status != StateLaunched || broker.calls != 1 {
		t.Fatalf("attempts=%d status=%s launches=%d", attempts, status, broker.calls)
	}
}

func TestBrokerStatusUsesOnlyScopedRunEndpoint(t *testing.T) {
	var method, path, auth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method, path, auth = r.Method, r.URL.EscapedPath(), r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"run_id":"run/123","status":"running"}`))
	}))
	defer server.Close()
	broker := &Broker{URL: server.URL + "/v1/launch-profiles/codex-issue-implement/launch", Token: "token", Client: server.Client()}
	result, err := broker.Status(context.Background(), "run/123")
	if err != nil || result.Status != "running" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if method != http.MethodGet || path != "/v1/runs/run%2F123" || auth != "Bearer token" {
		t.Fatalf("method=%s path=%s auth=%s", method, path, auth)
	}
}

func TestDisabledStandbyHealthAndReadiness(t *testing.T) {
	metrics := NewMetrics()
	metrics.SetDisabled()
	for _, tt := range []struct {
		path       string
		statusCode int
		body       string
	}{{"/healthz", 200, "ok"}, {"/readyz", 503, "disabled"}} {
		recorder := httptest.NewRecorder()
		metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, tt.path, nil))
		if recorder.Code != tt.statusCode || !strings.Contains(recorder.Body.String(), tt.body) {
			t.Fatalf("%s code=%d body=%q", tt.path, recorder.Code, recorder.Body.String())
		}
	}
}

func (l *launchSequence) Status(context.Context, string) (RunStatus, error) {
	var result RunStatus
	if l.statusCalls < len(l.statuses) {
		result = l.statuses[l.statusCalls]
	}
	var err error
	if l.statusCalls < len(l.statusErrors) {
		err = l.statusErrors[l.statusCalls]
	}
	l.statusCalls++
	return result, err
}

func TestRetriesAndTerminalErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics()
	for _, tt := range []struct {
		name      string
		launchErr error
		terminal  bool
	}{{"transient", BrokerError{Transport: true, Message: "network"}, false}, {"4xx", BrokerError{Status: 400, Message: "bad config"}, true}} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
			defer s.Close()
			c, _ := Select(validSignal("d", 1))
			_ = s.Record(ctx, "d", "accepted", 1, &c, now)
			launcher := &launchSequence{errors: []error{tt.launchErr}}
			worked, err := RunOne(ctx, logger, metrics, s, launcher, now)
			if err != nil || !worked {
				t.Fatal(err)
			}
			var status string
			if err := s.db.QueryRow(`SELECT status FROM jobs`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := StateLaunchRetry
			if tt.terminal {
				want = StateFailed
			}
			if status != want {
				t.Fatalf("status=%s", status)
			}
		})
	}
}

func TestSuccessfulLaunchRecordsBrokerRunID(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1500, 0)
	s, err := OpenStore(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, _ := Select(validSignal("success", 5))
	if err := s.Record(ctx, "success", "accepted", 1, &c, now); err != nil {
		t.Fatal(err)
	}
	launcher := &launchSequence{errors: []error{nil}}
	if worked, err := RunOne(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(), s, launcher, now); err != nil || !worked {
		t.Fatalf("worked=%v err=%v", worked, err)
	}
	var status, runID string
	if err := s.db.QueryRow(`SELECT status,broker_run_id FROM jobs`).Scan(&status, &runID); err != nil {
		t.Fatal(err)
	}
	if status != StateLaunched || runID != "run-sequence" {
		t.Fatalf("status=%q broker_run_id=%q", status, runID)
	}
}

func TestTransientRetriesStopAtDurableWindow(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2000, 0)
	s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	c, _ := Select(validSignal("bounded", 2))
	_ = s.Record(ctx, "bounded", "accepted", 1, &c, now)
	launcher := &launchSequence{errors: []error{BrokerError{Transport: true, Message: "one"}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics()
	if worked, err := RunOne(ctx, logger, metrics, s, launcher, now); err != nil || !worked {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE jobs SET due_at=?`, now.Add(LaunchRetryWindow).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if worked, err := RunOne(ctx, logger, metrics, s, launcher, now.Add(LaunchRetryWindow)); err != nil || !worked {
		t.Fatal(err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM jobs`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != StateFailed || launcher.calls != 1 {
		t.Fatalf("status=%s calls=%d", status, launcher.calls)
	}
}

type fakeDelivery struct {
	data          []byte
	acked, termed bool
	sequence      uint64
}

func (d *fakeDelivery) Data() []byte                    { return d.data }
func (d *fakeDelivery) StreamSequence() (uint64, error) { return d.sequence, nil }
func (d *fakeDelivery) AckSync() error                  { d.acked = true; return nil }
func (d *fakeDelivery) Term() error                     { d.termed = true; return nil }

func TestIrrelevantValidDeliveryIsMinimallyRecordedAndAcked(t *testing.T) {
	s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	signal := validSignal("irrelevant", 3)
	signal.Meta.SourceAction = "closed"
	data, _ := json.Marshal(signal)
	d := &fakeDelivery{data: data, sequence: 1}
	if !Process(context.Background(), slog.Default(), NewMetrics(), s, d, time.Unix(1, 0)) || !d.acked {
		t.Fatal("not acknowledged")
	}
	deliveries, jobs, _ := s.Counts(context.Background())
	if deliveries != 1 || jobs != 0 {
		t.Fatalf("counts %d %d", deliveries, jobs)
	}
}
