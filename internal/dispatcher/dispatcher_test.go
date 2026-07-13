package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
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
	if err := s.Record(ctx, a.DeliveryID, "accepted", &a, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, a.DeliveryID, "accepted", &a, now); err != nil {
		t.Fatal(err)
	}
	if err := s.Record(ctx, b.DeliveryID, "accepted", &b, now); err != nil {
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
	job, ok, err := s.ClaimDue(ctx, now)
	if err != nil || !ok || job.DeliveryID != "delivery-1" {
		t.Fatalf("job=%+v ok=%v err=%v", job, ok, err)
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
	if header != "github-task-dispatcher:v1:grubbyhacker/apple-jobs-matcher:delivery:abc:codex-issue-implement" || auth != "Bearer token" {
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
		})
	}
}

type launchSequence struct {
	errors []error
	calls  int
}

func (l *launchSequence) Launch(context.Context, Job) (LaunchResult, error) {
	err := l.errors[l.calls]
	l.calls++
	return LaunchResult{RunID: "run-sequence"}, err
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
	}{{"transient", errors.New("network"), false}, {"4xx", BrokerError{Status: 400, Message: "bad config"}, true}} {
		t.Run(tt.name, func(t *testing.T) {
			s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
			defer s.Close()
			c, _ := Select(validSignal("d", 1))
			_ = s.Record(ctx, "d", "accepted", &c, now)
			launcher := &launchSequence{errors: []error{tt.launchErr}}
			worked, err := RunOne(ctx, logger, metrics, s, launcher, now, 5)
			if err != nil || !worked {
				t.Fatal(err)
			}
			var status string
			if err := s.db.QueryRow(`SELECT status FROM jobs`).Scan(&status); err != nil {
				t.Fatal(err)
			}
			want := "retry"
			if tt.terminal {
				want = "terminal"
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
	if err := s.Record(ctx, "success", "accepted", &c, now); err != nil {
		t.Fatal(err)
	}
	launcher := &launchSequence{errors: []error{nil}}
	if worked, err := RunOne(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(), s, launcher, now, 5); err != nil || !worked {
		t.Fatalf("worked=%v err=%v", worked, err)
	}
	var status, runID string
	if err := s.db.QueryRow(`SELECT status,broker_run_id FROM jobs`).Scan(&status, &runID); err != nil {
		t.Fatal(err)
	}
	if status != "succeeded" || runID != "run-sequence" {
		t.Fatalf("status=%q broker_run_id=%q", status, runID)
	}
}

func TestTransientRetriesStopAtBound(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2000, 0)
	s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	c, _ := Select(validSignal("bounded", 2))
	_ = s.Record(ctx, "bounded", "accepted", &c, now)
	launcher := &launchSequence{errors: []error{errors.New("one"), errors.New("two")}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics()
	if worked, err := RunOne(ctx, logger, metrics, s, launcher, now, 2); err != nil || !worked {
		t.Fatal(err)
	}
	if worked, err := RunOne(ctx, logger, metrics, s, launcher, now.Add(time.Second), 2); err != nil || !worked {
		t.Fatal(err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM jobs`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "terminal" || launcher.calls != 2 {
		t.Fatalf("status=%s calls=%d", status, launcher.calls)
	}
}

type fakeDelivery struct {
	data          []byte
	acked, termed bool
}

func (d *fakeDelivery) Data() []byte   { return d.data }
func (d *fakeDelivery) AckSync() error { d.acked = true; return nil }
func (d *fakeDelivery) Term() error    { d.termed = true; return nil }

func TestIrrelevantValidDeliveryIsMinimallyRecordedAndAcked(t *testing.T) {
	s, _ := OpenStore(filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	signal := validSignal("irrelevant", 3)
	signal.Meta.SourceAction = "closed"
	data, _ := json.Marshal(signal)
	d := &fakeDelivery{data: data}
	if !Process(context.Background(), slog.Default(), NewMetrics(), s, d, time.Unix(1, 0)) || !d.acked {
		t.Fatal("not acknowledged")
	}
	deliveries, jobs, _ := s.Counts(context.Background())
	if deliveries != 1 || jobs != 0 {
		t.Fatalf("counts %d %d", deliveries, jobs)
	}
}
