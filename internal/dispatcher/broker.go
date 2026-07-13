package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const maxBrokerResponseBytes = 1 << 20

type Broker struct {
	URL, Token string
	Client     *http.Client
}

type BrokerError struct {
	Status  int
	Message string
}

func (e BrokerError) Error() string  { return e.Message }
func (e BrokerError) Terminal() bool { return e.Status >= 400 && e.Status < 500 }

type LaunchResult struct {
	RunID string `json:"run_id"`
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
	}{job.IssueNumber, job.DeliveryID}})
	if err != nil {
		return LaunchResult{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.URL, bytes.NewReader(body))
	if err != nil {
		return LaunchResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("github-task-dispatcher:v1:%s:delivery:%s:%s", job.Repository, job.DeliveryID, Profile))
	if b.Token != "" {
		req.Header.Set("Authorization", "Bearer "+b.Token)
	}
	client := b.Client
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return LaunchResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return LaunchResult{}, BrokerError{Status: response.StatusCode, Message: fmt.Sprintf("broker returned HTTP %d", response.StatusCode)}
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxBrokerResponseBytes+1))
	if err != nil {
		return LaunchResult{}, fmt.Errorf("read broker launch response: %w", err)
	}
	if len(raw) > maxBrokerResponseBytes {
		return LaunchResult{}, fmt.Errorf("broker launch response exceeds %d bytes", maxBrokerResponseBytes)
	}
	var result LaunchResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return LaunchResult{}, fmt.Errorf("decode broker launch response: %w", err)
	}
	result.RunID = strings.TrimSpace(result.RunID)
	if result.RunID == "" {
		return LaunchResult{}, errors.New("broker launch response is missing run_id")
	}
	return result, nil
}
