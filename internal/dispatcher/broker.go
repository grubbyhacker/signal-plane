package dispatcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxBrokerResponseBytes = 1 << 20

type Broker struct {
	URL, Token string
	Client     *http.Client
}

type BrokerError struct {
	Status    int
	Code      string
	Message   string
	Transport bool
	Malformed bool
}

func (e BrokerError) Error() string { return e.Message }
func (e BrokerError) Retryable() bool {
	return e.Transport || e.Status == http.StatusTooManyRequests || e.Status >= 500 || e.Code == "profile_busy"
}

type LaunchResult struct {
	RunID string `json:"run_id"`
}

type RunStatus struct {
	RunID  string `json:"run_id"`
	Status string `json:"status"`
}

func (b *Broker) Launch(ctx context.Context, job Job) (LaunchResult, error) {
	body, err := json.Marshal(struct {
		Parameters struct {
			IssueNumber      int64  `json:"issue_number"`
			SourceDeliveryID string `json:"source_delivery_id"`
		} `json:"parameters"`
	}{Parameters: struct {
		IssueNumber      int64  `json:"issue_number"`
		SourceDeliveryID string `json:"source_delivery_id"`
	}{job.IssueNumber, brokerSourceID(job)}})
	if err != nil {
		return LaunchResult{}, permanentMalformed("encode broker launch request", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL, bytes.NewReader(body))
	if err != nil {
		return LaunchResult{}, permanentMalformed("create broker launch request", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Repository + issue + the fixed profile form the semantic duplicate barrier.
	req.Header.Set("Idempotency-Key", fmt.Sprintf("github-task-dispatcher:v2:%s:issue:%d:%s", job.Repository, job.IssueNumber, Profile))
	b.authorize(req)
	var result LaunchResult
	if err := b.doJSON(req, &result); err != nil {
		return LaunchResult{}, err
	}
	result.RunID = strings.TrimSpace(result.RunID)
	if result.RunID == "" {
		return LaunchResult{}, permanentMalformed("broker launch response is missing run_id", nil)
	}
	return result, nil
}

// brokerSourceID is deliberately semantic rather than a GitHub delivery ID.
// Every field in the broker's idempotency fingerprint must remain identical
// for relabels and restored-database replay of the same repository issue.
func brokerSourceID(job Job) string {
	stable := fmt.Sprintf("%s\x00%d\x00%s", job.Repository, job.IssueNumber, Profile)
	return fmt.Sprintf("github-task-dispatcher-v2-%x", sha256.Sum256([]byte(stable)))
}

func (b *Broker) Status(ctx context.Context, runID string) (RunStatus, error) {
	base, err := url.Parse(b.URL)
	if err != nil {
		return RunStatus{}, permanentMalformed("parse broker URL", err)
	}
	base.Path = "/v1/runs/" + runID
	base.RawPath = "/v1/runs/" + url.PathEscape(runID)
	base.RawQuery = ""
	base.Fragment = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return RunStatus{}, permanentMalformed("create broker status request", err)
	}
	b.authorize(req)
	var result RunStatus
	if err := b.doJSON(req, &result); err != nil {
		return RunStatus{}, err
	}
	result.RunID = strings.TrimSpace(result.RunID)
	result.Status = strings.ToLower(strings.TrimSpace(result.Status))
	if result.RunID == "" || result.RunID != runID || result.Status == "" {
		return RunStatus{}, permanentMalformed("broker status response has invalid run_id or status", nil)
	}
	return result, nil
}

func (b *Broker) authorize(req *http.Request) {
	if b.Token != "" {
		req.Header.Set("Authorization", "Bearer "+b.Token)
	}
}

func (b *Broker) doJSON(req *http.Request, destination any) error {
	client := b.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return BrokerError{Transport: true, Message: "broker transport failure: " + err.Error()}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxBrokerResponseBytes+1))
	if err != nil {
		return BrokerError{Transport: true, Message: "read broker response: " + err.Error()}
	}
	if len(raw) > maxBrokerResponseBytes {
		return permanentMalformed(fmt.Sprintf("broker response exceeds %d bytes", maxBrokerResponseBytes), nil)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code, message := structuredBrokerError(raw)
		if message == "" {
			message = fmt.Sprintf("broker returned HTTP %d", response.StatusCode)
		}
		return BrokerError{Status: response.StatusCode, Code: code, Message: message}
	}
	if err := json.Unmarshal(raw, destination); err != nil {
		return permanentMalformed("decode broker success response", err)
	}
	return nil
}

func structuredBrokerError(raw []byte) (string, string) {
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return "", ""
	}
	if body.Error != nil {
		return strings.ToLower(strings.TrimSpace(body.Error.Code)), strings.TrimSpace(body.Error.Message)
	}
	return strings.ToLower(strings.TrimSpace(body.Code)), strings.TrimSpace(body.Message)
}

func permanentMalformed(message string, cause error) error {
	if cause != nil {
		message += ": " + cause.Error()
	}
	return BrokerError{Message: message, Malformed: true}
}

func IsRetryable(err error) bool {
	var brokerErr BrokerError
	return errors.As(err, &brokerErr) && brokerErr.Retryable()
}
