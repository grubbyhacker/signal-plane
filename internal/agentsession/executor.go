package agentsession

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const ExecutorID = "agent_session_v1"
const authorityProfile = "general-writer-v1"

type Executor struct {
	Store            *workledger.Store
	BrokerURL, Token string
	Client           *http.Client
}

func (e *Executor) Descriptor() workledger.ExecutorDescriptor {
	return workledger.ExecutorDescriptor{ID: ExecutorID, Kind: workledger.ExecutorAgentSession, Version: "v1"}
}
func (e *Executor) Execute(ctx context.Context, request workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	key := "session:" + request.WorkItem.ID
	body, _ := json.Marshal(map[string]string{"profile": authorityProfile, "idempotency_key": request.Attempt.IdempotencyKey, "session_binding": key})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.BrokerURL+"/v1/authority-workers/leases", bytes.NewReader(body))
	if err != nil {
		return workledger.ExecutorResult{}, err
	}
	req.Header.Set("Authorization", "Bearer "+e.Token)
	req.Header.Set("Content-Type", "application/json")
	client := e.Client
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "broker_admission", SanitizedError: "authority admission unavailable"}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "broker_admission", SanitizedError: "authority admission denied"}, nil
	}
	var out struct {
		Lease struct {
			WorkerID string `json:"worker_id"`
		} `json:"lease"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || out.Lease.WorkerID == "" {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "broker_protocol", SanitizedError: "authority admission response invalid"}, nil
	}
	if _, err := e.Store.BindSession(ctx, request.WorkItem.ID, key, authorityProfile, out.Lease.WorkerID, time.Now().UTC()); err != nil {
		return workledger.ExecutorResult{}, err
	}
	create, err := http.NewRequestWithContext(ctx, http.MethodPost, e.BrokerURL+"/v1/authority-workers/leases/"+key+"/sessions", nil)
	if err != nil {
		return workledger.ExecutorResult{}, err
	}
	create.Header.Set("Authorization", "Bearer "+e.Token)
	created, err := client.Do(create)
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_create", SanitizedError: "agent session create unavailable"}, nil
	}
	defer created.Body.Close()
	if created.StatusCode != http.StatusCreated {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_create", SanitizedError: "agent session create denied"}, nil
	}
	var session struct {
		SessionID string `json:"sessionId"`
	}
	if err := json.NewDecoder(created.Body).Decode(&session); err != nil || session.SessionID == "" {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "agentd_protocol", SanitizedError: "agent session create response invalid"}, nil
	}
	return workledger.ExecutorResult{Outcome: workledger.OutcomeWaiting, ExternalCorrelation: session.SessionID}, nil
}
