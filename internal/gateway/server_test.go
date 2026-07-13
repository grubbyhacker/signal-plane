package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

type capturePublisher struct {
	subject string
	signal  envelope.Signal
}

type readyPublisher struct {
	capturePublisher
	err error
}

func (publisher *readyPublisher) Ready(context.Context) error { return publisher.err }

func TestReadinessRequiresInspectableBus(t *testing.T) {
	publisher := &readyPublisher{err: errors.New("down")}
	server := New(slog.Default(), []config.Route{}, publisher)
	rec := httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), `"error":"not_ready"`) {
		t.Fatalf("not ready response = %d %s", rec.Code, rec.Body.String())
	}
	publisher.err = nil
	rec = httptest.NewRecorder()
	server.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready response = %d %s", rec.Code, rec.Body.String())
	}
}

func TestMetricsUseBoundedSourceAndReasonLabels(t *testing.T) {
	server := New(slog.Default(), nil, &capturePublisher{})
	server.metrics.add(config.Route{ID: "known-route", Source: "untrusted"}, "surprise", "arbitrary")
	server.metrics.add(config.Route{ID: "github", Source: "github"}, "ignored", "action_filtered")
	metrics, err := server.metrics.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range metrics {
		if metric.GetName() == "signal_gateway_requests_total" {
			foundOther := false
			foundFiltered := false
			for _, sample := range metric.Metric {
				got := map[string]string{}
				for _, label := range sample.Label {
					got[label.GetName()] = label.GetValue()
				}
				if got["source"] == "other" && got["result"] == "other" && got["reason"] == "other" {
					foundOther = true
				}
				if got["source"] == "github" && got["result"] == "ignored" && got["reason"] == "action_filtered" {
					foundFiltered = true
				}
			}
			if !foundOther || !foundFiltered {
				t.Fatalf("expected bounded other and action_filtered metric samples")
			}
			return
		}
	}
	t.Fatal("gateway request metric not found")
}

func (publisher *capturePublisher) Publish(subject string, signal envelope.Signal) error {
	publisher.subject = subject
	publisher.signal = signal
	return nil
}

func TestManualRoutePublishesRawPayload(t *testing.T) {
	t.Setenv("SIGNAL_GATEWAY_MANUAL_TOKEN", "local-token")
	publisher := &capturePublisher{}
	route := config.Route{
		ID:                 "manual-local",
		Path:               "/manual",
		Source:             "manual",
		MaxBodyBytes:       1024,
		PublishSubject:     "signals.manual.local.test",
		ManualAuthTokenEnv: "SIGNAL_GATEWAY_MANUAL_TOKEN",
	}
	server := New(slog.Default(), []config.Route{route}, publisher)
	server.clock = func() time.Time { return time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC) }

	req := httptest.NewRequest(http.MethodPost, "/manual", strings.NewReader(`{"message":"hello","nested":{"n":1}}`))
	req.Header.Set("Authorization", "Bearer local-token")
	req.Header.Set("X-Signal-Event", "manual-test")
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if publisher.subject != "signals.manual.local.test" {
		t.Fatalf("subject = %q", publisher.subject)
	}
	if string(publisher.signal.Payload) != `{"message":"hello","nested":{"n":1}}` {
		t.Fatalf("payload = %s", publisher.signal.Payload)
	}
	if publisher.signal.Meta.SourceEvent != "manual-test" {
		t.Fatalf("event = %q", publisher.signal.Meta.SourceEvent)
	}
}

func TestGitHubRouteRejectsBadSignature(t *testing.T) {
	t.Setenv("SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET", "secret")
	publisher := &capturePublisher{}
	route := config.Route{
		ID:             "github-local",
		Path:           "/webhooks/github",
		Source:         "github",
		MaxBodyBytes:   1024,
		PublishSubject: "signals.github.webhook",
		GitHub: config.GitHubConfig{
			WebhookSecretEnv: "SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET",
		},
	}
	server := New(slog.Default(), []config.Route{route}, publisher)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(`{"zen":"hi"}`))
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestGitHubRoutePublishesAllowedEvent(t *testing.T) {
	t.Setenv("SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET", "secret")
	publisher := &capturePublisher{}
	route := config.Route{
		ID:             "github-local",
		Path:           "/webhooks/github",
		Source:         "github",
		MaxBodyBytes:   1024,
		PublishSubject: "signals.github.webhook",
		GitHub: config.GitHubConfig{
			WebhookSecretEnv: "SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET",
		},
		Admission: config.AdmissionSet{
			Repositories: []string{"grubbyhacker/signal-plane"},
			Events:       []string{"pull_request"},
			Actions:      []string{"opened"},
		},
	}
	server := New(slog.Default(), []config.Route{route}, publisher)
	body := []byte(`{"action":"opened","repository":{"full_name":"grubbyhacker/signal-plane"}}`)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubSignature("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if publisher.signal.Meta.SourceDeliveryID != "delivery-1" {
		t.Fatalf("delivery id = %q", publisher.signal.Meta.SourceDeliveryID)
	}
	var preserved map[string]any
	if err := json.Unmarshal(publisher.signal.Payload, &preserved); err != nil {
		t.Fatal(err)
	}
	if preserved["action"] != "opened" {
		t.Fatalf("payload action = %v", preserved["action"])
	}
}

func TestGitHubPingIsAcknowledgedWithoutPublishByDefault(t *testing.T) {
	t.Setenv("SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET", "secret")
	publisher := &capturePublisher{}
	route := config.Route{
		ID:             "github-local",
		Path:           "/webhooks/github",
		Source:         "github",
		MaxBodyBytes:   1024,
		PublishSubject: "signals.github.webhook",
		GitHub: config.GitHubConfig{
			WebhookSecretEnv: "SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET",
		},
		Admission: config.AdmissionSet{
			Repositories: []string{"grubbyhacker/signal-plane"},
			Events:       []string{"pull_request", "ping"},
			Actions:      []string{"opened"},
		},
	}
	server := New(slog.Default(), []config.Route{route}, publisher)
	body := []byte(`{"repository":{"full_name":"grubbyhacker/signal-plane"}}`)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubSignature("secret", body))
	req.Header.Set("X-GitHub-Event", "ping")
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if publisher.subject != "" {
		t.Fatalf("ping should not publish, got subject %q", publisher.subject)
	}
}

func TestGitHubAllowedEventWithFilteredActionIsAcknowledgedWithoutPublish(t *testing.T) {
	t.Setenv("SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET", "secret")
	publisher := &capturePublisher{}
	route := config.Route{
		ID:             "github-local",
		Path:           "/webhooks/github",
		Source:         "github",
		MaxBodyBytes:   1024,
		PublishSubject: "signals.github.webhook",
		GitHub: config.GitHubConfig{
			WebhookSecretEnv: "SIGNAL_GATEWAY_GITHUB_WEBHOOK_SECRET",
		},
		Admission: config.AdmissionSet{
			Repositories: []string{"grubbyhacker/signal-plane"},
			Events:       []string{"pull_request"},
			Actions:      []string{"opened", "synchronize"},
		},
	}
	server := New(slog.Default(), []config.Route{route}, publisher)
	body := []byte(`{"action":"closed","repository":{"full_name":"grubbyhacker/signal-plane"}}`)

	req := httptest.NewRequest(http.MethodPost, "/webhooks/github", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", githubSignature("secret", body))
	req.Header.Set("X-GitHub-Event", "pull_request")
	req.Header.Set("X-GitHub-Delivery", "delivery-filtered-action")
	rec := httptest.NewRecorder()

	server.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if publisher.subject != "" {
		t.Fatalf("filtered action should not publish, got subject %q", publisher.subject)
	}
	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response["status"] != "ignored" || response["reason"] != "action_filtered" {
		t.Fatalf("response = %#v", response)
	}
}

func githubSignature(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
