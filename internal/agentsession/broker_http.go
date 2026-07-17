package agentsession

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const brokerCoordinatorVersion = "broker/coordinator/v1"

type HTTPBroker struct {
	baseURL *url.URL
	token   string
	client  *http.Client
}

func NewHTTPBroker(rawURL, token string, client *http.Client) (*HTTPBroker, error) {
	endpoint, err := url.Parse(rawURL)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("broker coordinator URL is invalid")
	}
	if strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("broker coordinator token is required")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	return &HTTPBroker{baseURL: endpoint, token: token, client: client}, nil
}

type brokerLease struct {
	Principal              string `json:"principal"`
	Profile                string `json:"profile"`
	WorkerID               string `json:"worker_id"`
	SessionLineageID       string `json:"session_lineage_id"`
	WorkerStorageLineageID string `json:"worker_storage_lineage_id"`
	WorkerFenceEpoch       int64  `json:"worker_fence_epoch"`
	ProfileVersion         string `json:"profile_version"`
	PolicyDigest           string `json:"policy_digest"`
	BindingDigest          string `json:"session_binding_digest"`
	IdempotencyDigest      string `json:"idempotency_key_digest"`
	CreatedAt              string `json:"created_at"`
	ReleasedAt             string `json:"released_at"`
	Replay                 bool   `json:"replay"`
}

func (lease brokerLease) normalized() (workledger.SessionLease, error) {
	value := workledger.SessionLease{AuthorityProfile: lease.Profile, ProfileVersion: lease.ProfileVersion, PolicyDigest: lease.PolicyDigest, SessionLineageID: lease.SessionLineageID, WorkerID: lease.WorkerID, WorkerStorageLineageID: lease.WorkerStorageLineageID, WorkerFenceEpoch: lease.WorkerFenceEpoch}
	if lease.Principal == "" || lease.BindingDigest == "" || lease.IdempotencyDigest == "" || lease.CreatedAt == "" {
		return workledger.SessionLease{}, errors.New("broker lease identity is incomplete")
	}
	return value, nil
}

func (broker *HTTPBroker) Acquire(ctx context.Context, request AcquireRequest) (workledger.SessionLease, error) {
	var response struct {
		Version   string `json:"version"`
		Admission struct {
			Lease     brokerLease `json:"lease"`
			Workspace struct {
				UID              int    `json:"uid"`
				GID              int    `json:"gid"`
				WorkspacePath    string `json:"workspace_path"`
				SessionLineageID string `json:"session_lineage_id"`
			} `json:"workspace"`
		} `json:"admission"`
	}
	err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/leases", map[string]any{"profile": request.AuthorityProfile, "idempotency_key": request.IdempotencyKey, "session_binding": request.BindingKey}, &response)
	if err != nil {
		return workledger.SessionLease{}, err
	}
	lease, err := response.Admission.Lease.normalized()
	if err != nil || response.Version != brokerCoordinatorVersion || response.Admission.Workspace.UID < 20000 || response.Admission.Workspace.GID < 20000 || response.Admission.Workspace.WorkspacePath == "" || response.Admission.Workspace.SessionLineageID != lease.SessionLineageID {
		return workledger.SessionLease{}, errors.New("broker lease response is inconsistent")
	}
	return lease, nil
}

func (broker *HTTPBroker) CreateSession(ctx context.Context, request CreateSessionRequest) (BrokerSession, error) {
	var response brokerSessionResponse
	if err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/sessions/create", map[string]any{"session_binding": request.BindingKey}, &response); err != nil {
		return BrokerSession{}, err
	}
	lease, err := response.validate()
	if err != nil {
		return BrokerSession{}, err
	}
	var status struct {
		SessionID string `json:"sessionId"`
	}
	if err := decodeStrict(response.Result, &status); err != nil || status.SessionID == "" {
		return BrokerSession{}, errors.New("broker session create result is invalid")
	}
	return BrokerSession{SessionID: status.SessionID, Lease: lease}, nil
}

func (broker *HTTPBroker) SubmitTurn(ctx context.Context, request SubmitTurnRequest) (BrokerTurn, error) {
	var response brokerSessionResponse
	if err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/sessions/submit", map[string]any{"session_binding": request.BindingKey, "prompt": request.Prompt, "idempotency_key": request.IdempotencyKey}, &response); err != nil {
		return BrokerTurn{}, err
	}
	lease, err := response.validate()
	if err != nil {
		return BrokerTurn{}, err
	}
	var turn struct {
		SessionID string `json:"sessionId"`
		TurnID    string `json:"turnId"`
		Phase     string `json:"phase"`
	}
	if err := decodeStrict(response.Result, &turn); err != nil || turn.SessionID == "" || turn.TurnID == "" || turn.Phase == "" {
		return BrokerTurn{}, errors.New("broker turn result is invalid")
	}
	return BrokerTurn{TurnID: turn.TurnID, Lease: lease}, nil
}

func (broker *HTTPBroker) StreamEvents(ctx context.Context, request StreamEventsRequest) (BrokerEvents, error) {
	var response brokerSessionResponse
	if err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/sessions/events", map[string]any{"session_binding": request.BindingKey, "after": request.Cursor}, &response); err != nil {
		return BrokerEvents{}, err
	}
	lease, err := response.validate()
	if err != nil {
		return BrokerEvents{}, err
	}
	var wireEvents []struct {
		Version   string          `json:"version"`
		Cursor    int64           `json:"cursor"`
		Kind      string          `json:"kind"`
		SessionID string          `json:"sessionId"`
		TurnID    string          `json:"turnId,omitempty"`
		AttemptID string          `json:"attemptId,omitempty"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := decodeStrict(response.Result, &wireEvents); err != nil {
		return BrokerEvents{}, errors.New("broker event result is invalid")
	}
	events := make([]Event, 0, len(wireEvents))
	previous := request.Cursor
	for _, wire := range wireEvents {
		if wire.Version != "agentd/v1" || wire.SessionID == "" || wire.Cursor != previous+1 || !knownAgentdEvent(wire.Kind) || len(wire.Payload) == 0 || !json.Valid(wire.Payload) {
			return BrokerEvents{}, errors.New("broker event stream is non-contiguous or malformed")
		}
		usage := workledger.Usage{}
		if wire.Kind == "attempt_completed" {
			var payload struct {
				Conversation json.RawMessage  `json:"conversation"`
				Facts        []string         `json:"facts,omitempty"`
				TokenUsage   workledger.Usage `json:"tokenUsage"`
			}
			if err := decodeStrict(wire.Payload, &payload); err != nil || len(payload.Conversation) == 0 || !payload.TokenUsage.Valid() {
				return BrokerEvents{}, errors.New("attempt completion payload is invalid")
			}
			usage = payload.TokenUsage
		}
		evidenceRef, err := canonicalEvidenceRef(wire)
		if err != nil {
			return BrokerEvents{}, err
		}
		events = append(events, Event{Cursor: wire.Cursor, Kind: wire.Kind, EvidenceRef: evidenceRef, Usage: usage})
		previous = wire.Cursor
	}
	return BrokerEvents{Lease: lease, Events: events}, nil
}

func (broker *HTTPBroker) Reassign(ctx context.Context, request ReassignRequest) (BrokerReassignment, error) {
	var response struct {
		Version      string `json:"version"`
		Reassignment struct {
			Lease               brokerLease `json:"lease"`
			PredecessorWorkerID string      `json:"predecessor_worker_id"`
			ReplacementWorkerID string      `json:"replacement_worker_id"`
			Replay              bool        `json:"replay"`
		} `json:"reassignment"`
	}
	err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/reassign", map[string]any{"session_binding": request.BindingKey, "session_lineage_id": request.SessionLineageID, "predecessor_worker_id": request.PredecessorWorker, "predecessor_worker_fence_epoch": request.PredecessorEpoch, "idempotency_key": request.IdempotencyKey}, &response)
	if err != nil {
		return BrokerReassignment{}, err
	}
	lease, err := response.Reassignment.Lease.normalized()
	if err != nil || response.Version != brokerCoordinatorVersion || response.Reassignment.PredecessorWorkerID != request.PredecessorWorker || response.Reassignment.ReplacementWorkerID != lease.WorkerID {
		return BrokerReassignment{}, errors.New("broker reassignment response is inconsistent")
	}
	return BrokerReassignment{Lease: lease, State: "broker_committed"}, nil
}

func (broker *HTTPBroker) ReassignmentStatus(ctx context.Context, request ReassignmentStatusRequest) (BrokerReassignmentStatus, error) {
	var wire struct {
		Version          string `json:"version"`
		SessionBinding   string `json:"session_binding"`
		SessionLineageID string `json:"session_lineage_id"`
		AuthorityProfile string `json:"authority_profile"`
		ProfileVersion   string `json:"profile_version"`
		PolicyDigest     string `json:"policy_digest"`
		Predecessor      struct {
			WorkerID         string `json:"workerId"`
			StorageLineageID string `json:"storageLineageId"`
			FenceEpoch       int64  `json:"fenceEpoch"`
		} `json:"predecessor"`
		Successor struct {
			WorkerID         string `json:"workerId"`
			StorageLineageID string `json:"storageLineageId"`
			FenceEpoch       int64  `json:"fenceEpoch"`
		} `json:"successor"`
		IdempotencyKey string `json:"rebind_idempotency_key"`
		State          string `json:"state"`
		ErrorCode      string `json:"error_code,omitempty"`
	}
	if err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/reassignments/status", map[string]any{"session_binding": request.BindingKey, "predecessor_fence_epoch": request.PredecessorEpoch}, &wire); err != nil {
		return BrokerReassignmentStatus{}, err
	}
	lease := workledger.SessionLease{AuthorityProfile: wire.AuthorityProfile, ProfileVersion: wire.ProfileVersion, PolicyDigest: wire.PolicyDigest, SessionLineageID: wire.SessionLineageID, WorkerID: wire.Successor.WorkerID, WorkerStorageLineageID: wire.Successor.StorageLineageID, WorkerFenceEpoch: wire.Successor.FenceEpoch}
	if wire.Version != brokerCoordinatorVersion || wire.SessionBinding != request.BindingKey || wire.Predecessor.FenceEpoch != request.PredecessorEpoch || wire.Predecessor.StorageLineageID != wire.Successor.StorageLineageID {
		return BrokerReassignmentStatus{}, errors.New("broker reassignment status is inconsistent")
	}
	return BrokerReassignmentStatus{Lease: lease, PredecessorWorker: wire.Predecessor.WorkerID, PredecessorEpoch: wire.Predecessor.FenceEpoch, RebindIdempotencyKey: wire.IdempotencyKey, State: wire.State, ErrorCode: wire.ErrorCode}, nil
}

type brokerSessionResponse struct {
	Version string          `json:"version"`
	Lease   brokerLease     `json:"lease"`
	Result  json.RawMessage `json:"result"`
}

func (response brokerSessionResponse) validate() (workledger.SessionLease, error) {
	lease, err := response.Lease.normalized()
	if err != nil || response.Version != brokerCoordinatorVersion || len(response.Result) == 0 {
		return workledger.SessionLease{}, errors.New("broker session response is invalid")
	}
	return lease, nil
}

func (broker *HTTPBroker) post(ctx context.Context, path string, input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	endpoint := broker.baseURL.ResolveReference(&url.URL{Path: path})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+broker.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := broker.client.Do(request)
	if err != nil {
		return errors.New("broker coordinator transport unavailable")
	}
	defer response.Body.Close()
	limited, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil || len(limited) > 1024*1024 {
		return errors.New("broker coordinator response exceeds limit")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("broker coordinator rejected request with status %d", response.StatusCode)
	}
	if err := decodeStrict(limited, output); err != nil {
		return errors.New("broker coordinator returned malformed JSON")
	}
	return nil
}

func decodeStrict(data []byte, output any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("JSON contains trailing content")
	}
	return nil
}

func knownAgentdEvent(kind string) bool {
	switch kind {
	case "session_created", "turn_enqueued", "attempt_started", "attempt_completed", "attempt_interrupted", "turn_cancelled", "turn_finished", "session_checkpointed", "session_resumed", "session_terminated", "session_rebound", "continuity_degraded", "verifier_evaluated", "verifier_continuation", "cancellation_failed", "verifier_failed", "verifier_escalated":
		return true
	default:
		return false
	}
}

func canonicalEvidenceRef(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}
