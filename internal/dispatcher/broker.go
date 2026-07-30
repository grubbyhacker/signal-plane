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
	URL, Token                 string
	ReporterURL, ReporterToken string
	Client                     *http.Client
}

type CommentResult struct {
	ID  int64  `json:"id"`
	URL string `json:"html_url"`
}

func (b *Broker) TerminalResult(ctx context.Context, runID string) (TerminalResult, error) {
	endpoint, err := runEndpoint(b.URL, runID, "terminal-result")
	if err != nil {
		return TerminalResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return TerminalResult{}, permanentMalformed("create terminal result request", err)
	}
	b.authorize(req)
	var out TerminalResult
	if err = b.doJSON(req, &out); err != nil {
		return TerminalResult{}, err
	}
	return out, nil
}
func (b *Broker) Comment(ctx context.Context, job Job, body, key string) (CommentResult, error) {
	base := b.ReporterURL
	if base == "" {
		return CommentResult{}, permanentMalformed("reporter broker URL is required", nil)
	}
	parts := strings.Split(job.Repository, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return CommentResult{}, permanentMalformed("invalid repository", nil)
	}
	u, err := url.Parse(base)
	if err != nil {
		return CommentResult{}, permanentMalformed("parse reporter broker URL", err)
	}
	u.Path = "/v1/repos/" + parts[0] + "/" + parts[1] + fmt.Sprintf("/issues/%d/comments", job.IssueNumber)
	u.RawPath = "/v1/repos/" + url.PathEscape(parts[0]) + "/" + url.PathEscape(parts[1]) + fmt.Sprintf("/issues/%d/comments", job.IssueNumber)
	u.RawQuery = ""
	u.Fragment = ""
	payload, _ := json.Marshal(map[string]any{"body": body, "metadata": map[string]string{"Semantic-Job": fmt.Sprintf("%d", job.ID), "Broker-Run": job.BrokerRunID}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return CommentResult{}, permanentMalformed("create issue comment request", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	if b.ReporterToken != "" {
		req.Header.Set("Authorization", "Bearer "+b.ReporterToken)
	}
	var out CommentResult
	if err = b.doJSON(req, &out); err != nil {
		return CommentResult{}, err
	}
	if out.ID < 1 {
		return CommentResult{}, permanentMalformed("comment response missing id", nil)
	}
	return out, nil
}
func runEndpoint(raw, runID, suffix string) (string, error) {
	base, err := url.Parse(raw)
	if err != nil {
		return "", permanentMalformed("parse broker URL", err)
	}
	base.Path = "/v1/runs/" + runID + "/" + suffix
	base.RawPath = "/v1/runs/" + url.PathEscape(runID) + "/" + suffix
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
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
	endpoint, err := b.launchURL(job.Profile)
	if err != nil {
		return LaunchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return LaunchResult{}, permanentMalformed("create broker launch request", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Repository + issue + the fixed profile form the semantic duplicate barrier.
	req.Header.Set("Idempotency-Key", fmt.Sprintf("repository-task-dispatcher:v1:%s:%s:issue:%d:%s", job.RouteID, job.Repository, job.IssueNumber, job.Profile))
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
	stable := fmt.Sprintf("%s\x00%s\x00%d\x00%s", job.RouteID, job.Repository, job.IssueNumber, job.Profile)
	return fmt.Sprintf("repository-task-dispatcher-v1-%x", sha256.Sum256([]byte(stable)))
}

func (b *Broker) launchURL(profile string) (string, error) {
	if strings.TrimSpace(profile) == "" {
		return "", permanentMalformed("launch profile is required", nil)
	}
	base, err := url.Parse(b.URL)
	if err != nil {
		return "", permanentMalformed("parse broker URL", err)
	}
	base.Path = "/v1/launch-profiles/" + profile + "/launch"
	base.RawPath = "/v1/launch-profiles/" + url.PathEscape(profile) + "/launch"
	base.RawQuery = ""
	base.Fragment = ""
	return base.String(), nil
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
