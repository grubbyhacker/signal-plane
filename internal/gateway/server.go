package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/buildinfo"
	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/source/githubwebhook"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Publisher interface {
	Publish(subject string, signal envelope.Signal) error
}

type readinessChecker interface{ Ready(context.Context) error }

type Server struct {
	logger    *slog.Logger
	publisher Publisher
	routes    []config.Route
	clock     func() time.Time
	metrics   *metrics
}

func New(logger *slog.Logger, routes []config.Route, publisher Publisher) *Server {
	return &Server{
		logger:    logger,
		publisher: publisher,
		routes:    routes,
		clock:     func() time.Time { return time.Now().UTC() },
		metrics:   newMetrics(),
	}
}

func (server *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", writeOK)
	mux.HandleFunc("GET /readyz", server.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(server.metrics.registry, promhttp.HandlerOpts{}))
	for _, route := range server.routes {
		route := route
		mux.HandleFunc("POST "+route.Path, func(w http.ResponseWriter, r *http.Request) {
			server.handleRoute(w, r, route)
		})
	}
	return mux
}

func (server *Server) ready(w http.ResponseWriter, r *http.Request) {
	checker, ok := server.publisher.(readinessChecker)
	if !ok || checker.Ready(r.Context()) != nil {
		server.metrics.readiness.Set(0)
		server.metrics.dependencyErrors.WithLabelValues("nats", "unavailable").Inc()
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not_ready"})
		return
	}
	server.metrics.readiness.Set(1)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func writeOK(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func (server *Server) handleRoute(w http.ResponseWriter, r *http.Request, route config.Route) {
	body, err := readJSONBody(w, r, route.MaxBodyBytes)
	if err != nil {
		server.reject(w, route, http.StatusBadRequest, "invalid_body", err)
		return
	}

	admission, err := server.admit(r, route, body)
	if err != nil {
		var rejected rejectedRequest
		if errors.As(err, &rejected) {
			server.reject(w, route, rejected.status, rejected.reason, rejected)
			return
		}
		server.reject(w, route, http.StatusForbidden, "rejected", err)
		return
	}
	if admission.ignore {
		server.metrics.add(route, "ignored", admission.ignoreReason)
		writeJSON(w, http.StatusAccepted, map[string]string{
			"status": "ignored",
			"reason": admission.ignoreReason,
		})
		return
	}

	signal := envelope.Signal{
		Meta: envelope.Meta{
			SignalID:         envelope.NewSignalID(),
			Source:           route.Source,
			RouteID:          route.ID,
			ReceivedAt:       server.clock(),
			SourceEvent:      admission.event,
			SourceAction:     admission.action,
			SourceDeliveryID: admission.deliveryID,
			Namespace:        admission.namespace,
			ObjectKind:       admission.objectKind,
			ObjectID:         admission.objectID,
			SourceRevision:   admission.sourceRevision,
			ActorClass:       admission.actorClass,
			Authentication: envelope.Authentication{
				Method:   admission.authMethod,
				Verified: admission.authVerified,
			},
		},
		Payload: append(json.RawMessage(nil), body...),
	}

	started := time.Now()
	if err := server.publisher.Publish(route.Subject(), signal); err != nil {
		server.metrics.publishDuration.Observe(time.Since(started).Seconds())
		server.metrics.add(route, "rejected", "publish_failed")
		server.metrics.dependencyErrors.WithLabelValues("nats", normalizedError(err)).Inc()
		server.logger.Error("publish failed", "route_id", route.ID, "source", route.Source, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "publish_failed"})
		return
	}
	server.metrics.publishDuration.Observe(time.Since(started).Seconds())

	server.metrics.add(route, "accepted", "")
	server.logger.Info(
		"accepted signal",
		"signal_id", signal.Meta.SignalID,
		"source", signal.Meta.Source,
		"route_id", signal.Meta.RouteID,
		"source_event", signal.Meta.SourceEvent,
		"source_action", signal.Meta.SourceAction,
	)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":    "accepted",
		"signal_id": signal.Meta.SignalID,
		"subject":   route.Subject(),
	})
}

func readJSONBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		return nil, err
	}
	if !json.Valid(body) {
		return nil, errors.New("request body must be valid JSON")
	}
	if firstNonSpace(body) != '{' {
		return nil, errors.New("request body must be a JSON object")
	}
	return body, nil
}

func firstNonSpace(body []byte) byte {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return 0
	}
	return body[0]
}

type admissionResult struct {
	event          string
	action         string
	deliveryID     string
	ignore         bool
	ignoreReason   string
	namespace      string
	objectKind     string
	objectID       string
	sourceRevision string
	actorClass     string
	authMethod     string
	authVerified   bool
}

func (server *Server) admit(r *http.Request, route config.Route, body []byte) (admissionResult, error) {
	switch route.Source {
	case "github":
		return admitGitHub(r, route, body)
	case "manual":
		return admitManual(r, route)
	default:
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "unknown_source"}
	}
}

func admitManual(r *http.Request, route config.Route) (admissionResult, error) {
	verified := false
	method := "manual_test"
	if route.ManualAuthTokenEnv != "" {
		want := os.Getenv(route.ManualAuthTokenEnv)
		if want == "" {
			return admissionResult{}, rejectedRequest{status: http.StatusServiceUnavailable, reason: "missing_manual_token"}
		}
		if !bearerOrHeaderMatches(r, want) {
			return admissionResult{}, rejectedRequest{status: http.StatusUnauthorized, reason: "bad_manual_token"}
		}
		verified = true
		method = "manual_bearer"
	}
	return admissionResult{
		event:        headerDefault(r, "X-Signal-Event", "manual"),
		action:       r.Header.Get("X-Signal-Action"),
		deliveryID:   r.Header.Get("X-Signal-Delivery"),
		authMethod:   method,
		authVerified: verified,
	}, nil
}

func admitGitHub(r *http.Request, route config.Route, body []byte) (admissionResult, error) {
	secret := os.Getenv(route.GitHub.WebhookSecretEnv)
	if secret == "" {
		return admissionResult{}, rejectedRequest{status: http.StatusServiceUnavailable, reason: "missing_github_secret"}
	}
	if !githubwebhook.VerifySignature(secret, body, r.Header.Get(githubwebhook.SignatureHeader)) {
		return admissionResult{}, rejectedRequest{status: http.StatusUnauthorized, reason: "bad_github_signature"}
	}

	event := r.Header.Get("X-GitHub-Event")
	deliveryID := r.Header.Get("X-GitHub-Delivery")
	var decoded struct {
		Action     string `json:"action"`
		Repository struct {
			FullName string `json:"full_name"`
		} `json:"repository"`
		Issue *struct {
			Number    int64  `json:"number"`
			UpdatedAt string `json:"updated_at"`
		} `json:"issue"`
		PullRequest *struct {
			Number    int64  `json:"number"`
			UpdatedAt string `json:"updated_at"`
			Head      struct {
				SHA string `json:"sha"`
			} `json:"head"`
		} `json:"pull_request"`
		Release *struct {
			ID        int64  `json:"id"`
			TagName   string `json:"tag_name"`
			UpdatedAt string `json:"updated_at"`
		} `json:"release"`
		Sender struct {
			Type string `json:"type"`
		} `json:"sender"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return admissionResult{}, rejectedRequest{status: http.StatusBadRequest, reason: "invalid_github_payload"}
	}
	if len(route.Admission.Tuples) > 0 {
		result, err := admitGitHubTuple(route, event, deliveryID, decoded.Action, decoded.Repository.FullName)
		return enrichGitHubAdmission(result, event, decoded.Repository.FullName, decoded.Issue, decoded.PullRequest, decoded.Release, decoded.Sender.Type), err
	}
	if !config.ContainsAllowed(route.Admission.Repositories, decoded.Repository.FullName) {
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "repository_not_allowed"}
	}
	if !config.ContainsAllowed(route.Admission.Events, event) {
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "event_not_allowed"}
	}
	if event == "ping" && !route.GitHub.PublishPing {
		return admissionResult{ignore: true, ignoreReason: "ping"}, nil
	}
	if !config.ContainsAllowed(route.Admission.Actions, decoded.Action) {
		return admissionResult{
			event:        event,
			action:       decoded.Action,
			deliveryID:   deliveryID,
			ignore:       true,
			ignoreReason: "action_filtered",
		}, nil
	}
	return enrichGitHubAdmission(admissionResult{event: event, action: decoded.Action, deliveryID: deliveryID}, event, decoded.Repository.FullName, decoded.Issue, decoded.PullRequest, decoded.Release, decoded.Sender.Type), nil
}

func enrichGitHubAdmission(result admissionResult, event, repository string, issue *struct {
	Number    int64  `json:"number"`
	UpdatedAt string `json:"updated_at"`
}, pullRequest *struct {
	Number    int64  `json:"number"`
	UpdatedAt string `json:"updated_at"`
	Head      struct {
		SHA string `json:"sha"`
	} `json:"head"`
}, release *struct {
	ID        int64  `json:"id"`
	TagName   string `json:"tag_name"`
	UpdatedAt string `json:"updated_at"`
}, actorType string) admissionResult {
	result.namespace = repository
	result.objectKind = event
	result.authMethod = "github_hmac_sha256"
	result.authVerified = true
	result.actorClass = strings.ToLower(actorType)
	if issue != nil {
		result.objectKind, result.objectID, result.sourceRevision = "issue", fmt.Sprint(issue.Number), issue.UpdatedAt
	}
	if pullRequest != nil {
		result.objectKind, result.objectID, result.sourceRevision = "pull_request", fmt.Sprint(pullRequest.Number), pullRequest.Head.SHA
		if result.sourceRevision == "" {
			result.sourceRevision = pullRequest.UpdatedAt
		}
	}
	if release != nil {
		result.objectKind, result.objectID, result.sourceRevision = "release", fmt.Sprint(release.ID), release.UpdatedAt
		if result.sourceRevision == "" {
			result.sourceRevision = release.TagName
		}
	}
	return result
}

func admitGitHubTuple(route config.Route, event, deliveryID, action, repository string) (admissionResult, error) {
	repositoryMatched := false
	for _, tuple := range route.Admission.Tuples {
		if tuple.Repository != repository {
			continue
		}
		repositoryMatched = true
		if tuple.Event != event {
			continue
		}
		if event == "ping" && !route.GitHub.PublishPing {
			return admissionResult{ignore: true, ignoreReason: "ping"}, nil
		}
		if !config.ContainsAllowed(tuple.Actions, action) {
			return admissionResult{event: event, action: action, deliveryID: deliveryID, ignore: true, ignoreReason: "action_filtered"}, nil
		}
		return admissionResult{event: event, action: action, deliveryID: deliveryID}, nil
	}
	if !repositoryMatched {
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "repository_not_allowed"}
	}
	return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "event_not_allowed"}
}

func bearerOrHeaderMatches(r *http.Request, want string) bool {
	if got := r.Header.Get("X-Signal-Token"); got == want {
		return true
	}
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, prefix) && strings.TrimPrefix(auth, prefix) == want
}

func headerDefault(r *http.Request, name string, fallback string) string {
	if value := r.Header.Get(name); value != "" {
		return value
	}
	return fallback
}

type rejectedRequest struct {
	status int
	reason string
}

func (err rejectedRequest) Error() string {
	return err.reason
}

func (server *Server) reject(w http.ResponseWriter, route config.Route, status int, reason string, err error) {
	server.metrics.add(route, "rejected", reason)
	server.logger.Warn("rejected signal", "route_id", route.ID, "source", route.Source, "reason", reason, "error", err)
	writeJSON(w, status, map[string]string{"error": reason})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type metrics struct {
	registry         *prometheus.Registry
	requests         *prometheus.CounterVec
	publishDuration  prometheus.Histogram
	readiness        prometheus.Gauge
	dependencyErrors *prometheus.CounterVec
}

func newMetrics() *metrics {
	registry := prometheus.NewRegistry()
	m := &metrics{
		registry:         registry,
		requests:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "signal_gateway_requests_total", Help: "Gateway requests by bounded admission outcome."}, []string{"route_id", "source", "result", "reason"}),
		publishDuration:  prometheus.NewHistogram(prometheus.HistogramOpts{Name: "signal_gateway_publish_duration_seconds", Help: "Synchronous JetStream publish acknowledgement duration.", Buckets: prometheus.DefBuckets}),
		readiness:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "signal_gateway_readiness", Help: "Whether NATS and the configured stream are inspectable."}),
		dependencyErrors: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "signal_gateway_dependency_errors_total", Help: "Normalized dependency errors."}, []string{"dependency", "reason"}),
	}
	registry.MustRegister(m.requests, m.publishDuration, m.readiness, m.dependencyErrors, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}), prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "signal_gateway_build_info", Help: "Build information for signal-gateway.", ConstLabels: prometheus.Labels{"version": buildinfo.Version}}, func() float64 { return 1 }))
	return m
}

func (m *metrics) add(route config.Route, result string, reason string) {
	m.requests.WithLabelValues(boundedRoute(route.ID), boundedSource(route.Source), boundedResult(result), boundedReason(reason)).Inc()
}

func boundedRoute(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
func boundedSource(value string) string {
	switch value {
	case "github", "manual":
		return value
	}
	return "other"
}
func boundedResult(value string) string {
	switch value {
	case "accepted", "ignored", "rejected":
		return value
	}
	return "other"
}
func boundedReason(value string) string {
	switch value {
	case "", "ping", "action_filtered", "invalid_body", "unknown_source", "missing_manual_token", "bad_manual_token", "missing_github_secret", "bad_github_signature", "invalid_github_payload", "repository_not_allowed", "event_not_allowed", "action_not_allowed", "publish_failed", "rejected":
		return value
	}
	return "other"
}
func normalizedError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "other"
}
