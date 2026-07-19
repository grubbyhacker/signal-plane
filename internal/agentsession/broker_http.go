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

const (
	brokerCoordinatorVersion   = "broker/coordinator/v1"
	brokerCoordinatorV2Version = "broker/coordinator/v2"
)

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
	if err := request.RegisteredTask.Validate(request.BindingKey); err != nil {
		return workledger.SessionLease{}, fmt.Errorf("registered admission is invalid: %w", err)
	}
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
	err := broker.post(ctx, "/v1/authority-workers/coordinator/v2/leases", brokerAcquireV2Request{Version: brokerCoordinatorV2Version, Profile: request.AuthorityProfile, IdempotencyKey: request.IdempotencyKey, SessionBinding: request.BindingKey, RegisteredTaskSource: request.RegisteredTask.Source, RegisteredTask: request.RegisteredTask.Snapshot, AdmissionTaskDigest: request.RegisteredTask.Digest}, &response)
	if err != nil {
		return workledger.SessionLease{}, err
	}
	lease, err := response.Admission.Lease.normalized()
	if err != nil || response.Version != brokerCoordinatorV2Version || response.Admission.Workspace.UID < 20000 || response.Admission.Workspace.GID < 20000 || response.Admission.Workspace.WorkspacePath == "" || response.Admission.Workspace.SessionLineageID != lease.SessionLineageID {
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

type BrokerSessionStatus struct {
	Version, SessionID, CoordinatorBinding, AuthorityBinding string
	WorkerID, StorageLineageID, SessionLineageID, Phase      string
	FenceEpoch, NextCursor                                   int64
	Workspace                                                BrokerSessionWorkspace
	Conversation                                             *BrokerConversation
	ActiveTurnID                                             string
	TurnIDs                                                  []string
}

type BrokerSessionWorkspace struct {
	WorkspaceRef, BranchRef, CheckpointRef string
	UID, GID                               int
}

type BrokerConversation struct {
	AdapterKind, AdapterVersion, BackendThreadRef string
}

func (broker *HTTPBroker) Checkpoint(ctx context.Context, bindingKey, checkpointRef string) (BrokerSessionStatus, error) {
	if !boundedOpaque(checkpointRef) {
		return BrokerSessionStatus{}, errors.New("checkpoint reference is invalid")
	}
	return broker.sessionStatusCommand(ctx, "checkpoint", map[string]any{"session_binding": bindingKey, "checkpoint_ref": checkpointRef})
}

func (broker *HTTPBroker) Resume(ctx context.Context, bindingKey string) (BrokerSessionStatus, error) {
	return broker.sessionStatusCommand(ctx, "resume", map[string]any{"session_binding": bindingKey})
}

func (broker *HTTPBroker) Status(ctx context.Context, bindingKey string) (BrokerSessionStatus, error) {
	return broker.sessionStatusCommand(ctx, "status", map[string]any{"session_binding": bindingKey})
}

func (broker *HTTPBroker) Cancel(ctx context.Context, bindingKey, turnID string) (BrokerTurn, error) {
	if !boundedOpaque(bindingKey) || !boundedID(turnID) {
		return BrokerTurn{}, errors.New("turn identity is invalid")
	}
	var response brokerSessionResponse
	if err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/sessions/cancel", map[string]any{"session_binding": bindingKey, "turn_id": turnID}, &response); err != nil {
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
	if err := decodeStrict(response.Result, &turn); err != nil || !boundedID(turn.SessionID) || turn.TurnID != turnID || turn.Phase == "" {
		return BrokerTurn{}, errors.New("broker cancel result is invalid")
	}
	return BrokerTurn{TurnID: turn.TurnID, Lease: lease}, nil
}

func (broker *HTTPBroker) sessionStatusCommand(ctx context.Context, operation string, input map[string]any) (BrokerSessionStatus, error) {
	bindingKey, _ := input["session_binding"].(string)
	if !boundedOpaque(bindingKey) {
		return BrokerSessionStatus{}, errors.New("session binding is invalid")
	}
	var response brokerSessionResponse
	if err := broker.post(ctx, "/v1/authority-workers/coordinator/v1/sessions/"+operation, input, &response); err != nil {
		return BrokerSessionStatus{}, err
	}
	lease, err := response.validate()
	if err != nil {
		return BrokerSessionStatus{}, err
	}
	// The agentd status wire uses lower camel case outside the workspace object.
	type statusWire struct {
		Version            string `json:"version"`
		SessionID          string `json:"sessionId"`
		CoordinatorBinding string `json:"coordinatorBinding"`
		AuthorityBinding   string `json:"authorityBinding"`
		WorkerID           string `json:"workerId"`
		StorageLineageID   string `json:"storageLineageId"`
		FenceEpoch         int64  `json:"fenceEpoch"`
		SessionLineageID   string `json:"sessionLineageId"`
		Workspace          struct {
			WorkspaceRef  string `json:"workspaceRef"`
			UID           int    `json:"uid"`
			GID           int    `json:"gid"`
			BranchRef     string `json:"branchRef,omitempty"`
			CheckpointRef string `json:"checkpointRef,omitempty"`
		} `json:"workspace"`
		Phase        string `json:"phase"`
		Conversation *struct {
			AdapterKind      string `json:"adapterKind"`
			AdapterVersion   string `json:"adapterVersion"`
			BackendThreadRef string `json:"backendThreadRef"`
		} `json:"conversation,omitempty"`
		ActiveTurnID string   `json:"activeTurnId,omitempty"`
		TurnIDs      []string `json:"turnIds"`
		NextCursor   int64    `json:"nextCursor"`
	}
	var status statusWire
	if err := decodeStrict(response.Result, &status); err != nil || status.Version != "agentd/v1" || !boundedID(status.SessionID) || status.CoordinatorBinding != bindingKey || status.AuthorityBinding != lease.AuthorityProfile || status.WorkerID != lease.WorkerID || status.StorageLineageID != lease.WorkerStorageLineageID || status.FenceEpoch != lease.WorkerFenceEpoch || status.SessionLineageID != lease.SessionLineageID || (status.Phase != "active" && status.Phase != "terminated") || !boundedOpaque(status.Workspace.WorkspaceRef) || status.Workspace.UID < 0 || status.Workspace.GID < 0 || status.TurnIDs == nil || status.NextCursor < 1 {
		return BrokerSessionStatus{}, errors.New("broker session status is inconsistent")
	}
	if (status.Workspace.BranchRef != "" && !boundedOpaque(status.Workspace.BranchRef)) || (status.Workspace.CheckpointRef != "" && !boundedOpaque(status.Workspace.CheckpointRef)) || (status.ActiveTurnID != "" && !boundedID(status.ActiveTurnID)) {
		return BrokerSessionStatus{}, errors.New("broker session status has invalid references")
	}
	for _, turnID := range status.TurnIDs {
		if !boundedID(turnID) {
			return BrokerSessionStatus{}, errors.New("broker session status has invalid turn identity")
		}
	}
	if status.Conversation != nil && (!boundedID(status.Conversation.AdapterKind) || !boundedID(status.Conversation.AdapterVersion) || !boundedOpaque(status.Conversation.BackendThreadRef)) {
		return BrokerSessionStatus{}, errors.New("broker session status has invalid conversation identity")
	}
	value := BrokerSessionStatus{Version: status.Version, SessionID: status.SessionID, CoordinatorBinding: status.CoordinatorBinding, AuthorityBinding: status.AuthorityBinding, WorkerID: status.WorkerID, StorageLineageID: status.StorageLineageID, FenceEpoch: status.FenceEpoch, SessionLineageID: status.SessionLineageID, Phase: status.Phase, Workspace: BrokerSessionWorkspace{WorkspaceRef: status.Workspace.WorkspaceRef, UID: status.Workspace.UID, GID: status.Workspace.GID, BranchRef: status.Workspace.BranchRef, CheckpointRef: status.Workspace.CheckpointRef}, ActiveTurnID: status.ActiveTurnID, TurnIDs: status.TurnIDs, NextCursor: status.NextCursor}
	if status.Conversation != nil {
		value.Conversation = &BrokerConversation{AdapterKind: status.Conversation.AdapterKind, AdapterVersion: status.Conversation.AdapterVersion, BackendThreadRef: status.Conversation.BackendThreadRef}
	}
	if operation == "checkpoint" && value.Workspace.CheckpointRef != input["checkpoint_ref"] {
		return BrokerSessionStatus{}, errors.New("broker checkpoint result is inconsistent")
	}
	return value, nil
}

func boundedID(value string) bool {
	return value != "" && len(value) <= 128 && !strings.ContainsAny(value, "\r\n")
}
func boundedOpaque(value string) bool {
	return value != "" && len(value) <= 512 && !strings.ContainsAny(value, "\r\n")
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
