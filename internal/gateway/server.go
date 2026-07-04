package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/source/githubwebhook"
)

type Publisher interface {
	Publish(subject string, signal envelope.Signal) error
}

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
	mux.HandleFunc("GET /readyz", writeOK)
	mux.HandleFunc("GET /metrics", server.metrics.handle)
	for _, route := range server.routes {
		route := route
		mux.HandleFunc("POST "+route.Path, func(w http.ResponseWriter, r *http.Request) {
			server.handleRoute(w, r, route)
		})
	}
	return mux
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
		server.metrics.add(route, "ignored", "ping")
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "ignored", "reason": "ping"})
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
		},
		Payload: append(json.RawMessage(nil), body...),
	}

	if err := server.publisher.Publish(route.Subject(), signal); err != nil {
		server.metrics.add(route, "rejected", "publish_failed")
		server.logger.Error("publish failed", "route_id", route.ID, "source", route.Source, "error", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "publish_failed"})
		return
	}

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
	event      string
	action     string
	deliveryID string
	ignore     bool
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
	if route.ManualAuthTokenEnv != "" {
		want := os.Getenv(route.ManualAuthTokenEnv)
		if want == "" {
			return admissionResult{}, rejectedRequest{status: http.StatusServiceUnavailable, reason: "missing_manual_token"}
		}
		if !bearerOrHeaderMatches(r, want) {
			return admissionResult{}, rejectedRequest{status: http.StatusUnauthorized, reason: "bad_manual_token"}
		}
	}
	return admissionResult{
		event:      headerDefault(r, "X-Signal-Event", "manual"),
		action:     r.Header.Get("X-Signal-Action"),
		deliveryID: r.Header.Get("X-Signal-Delivery"),
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
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return admissionResult{}, rejectedRequest{status: http.StatusBadRequest, reason: "invalid_github_payload"}
	}
	if !config.ContainsAllowed(route.Admission.Repositories, decoded.Repository.FullName) {
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "repository_not_allowed"}
	}
	if !config.ContainsAllowed(route.Admission.Events, event) {
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "event_not_allowed"}
	}
	if event == "ping" && !route.GitHub.PublishPing {
		return admissionResult{ignore: true}, nil
	}
	if !config.ContainsAllowed(route.Admission.Actions, decoded.Action) {
		return admissionResult{}, rejectedRequest{status: http.StatusForbidden, reason: "action_not_allowed"}
	}
	return admissionResult{event: event, action: decoded.Action, deliveryID: deliveryID}, nil
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
	mu       sync.Mutex
	counters map[string]int64
}

func newMetrics() *metrics {
	return &metrics{counters: map[string]int64{}}
}

func (m *metrics) add(route config.Route, result string, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("route_id=%q,source=%q,result=%q,reason=%q", route.ID, route.Source, result, reason)
	m.counters[key]++
}

func (m *metrics) handle(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	w.Header().Set("content-type", "text/plain; version=0.0.4")
	for labels, count := range m.counters {
		_, _ = fmt.Fprintf(w, "signal_gateway_requests_total{%s} %d\n", labels, count)
	}
}
