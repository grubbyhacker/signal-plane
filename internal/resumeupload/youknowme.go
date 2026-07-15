package resumeupload

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

type YKMAuthMode string

const (
	YKMAuthCloudflare YKMAuthMode = "cloudflare_access"
	YKMAuthLocal      YKMAuthMode = "local_secret"
)

type YKMConfig struct {
	BaseURL                             string
	AuthMode                            YKMAuthMode
	ClientID, ClientSecret, LocalSecret string
}

func (config YKMConfig) Validate() error {
	parsed, err := url.Parse(config.BaseURL)
	if err != nil || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/mcp" || parsed.Host == "" {
		return errors.New("YouKnowMe base URL must be a fixed MCP endpoint")
	}
	switch config.AuthMode {
	case YKMAuthCloudflare:
		if config.BaseURL != "https://mcp.fleiglabs.cc/mcp" || config.ClientID == "" || config.ClientSecret == "" || config.LocalSecret != "" {
			return errors.New("cloudflare_access requires HTTPS client ID/secret only")
		}
	case YKMAuthLocal:
		if config.BaseURL != "http://youknowme-mcp:8765/mcp" || config.LocalSecret == "" || config.ClientID != "" || config.ClientSecret != "" {
			return errors.New("local_secret requires a loopback/private service MCP URL and local secret only")
		}
	default:
		return errors.New("unsupported YouKnowMe auth mode")
	}
	return nil
}

type YKMClient struct {
	Config YKMConfig
	Client *http.Client
}
type uploadResponse struct {
	UploadID   string   `json:"upload_id"`
	Accepted   bool     `json:"accepted"`
	Status     string   `json:"status"`
	FileCount  int      `json:"file_count"`
	TotalBytes int      `json:"total_bytes"`
	Warnings   []string `json:"warnings"`
	StagedPath string   `json:"staged_path"`
	Replayed   bool     `json:"replayed"`
}

func (client *YKMClient) Upload(ctx context.Context, filename, content, idempotency string) (uploadResponse, error) {
	if err := client.Config.Validate(); err != nil {
		return uploadResponse{}, err
	}
	arguments := map[string]any{"idempotency_key": idempotency, "files": []map[string]string{{"filename": filename, "content": content}}, "purpose": "Import the reviewed Resume Builder structured Markdown release.", "suggested_type": "profile", "suggested_tags": []string{"resume", "career", "profile"}, "suggested_related": []string{}}
	result, err := client.call(ctx, "upload", arguments)
	if err != nil {
		return uploadResponse{}, err
	}
	var response uploadResponse
	decoder := json.NewDecoder(bytes.NewReader(result))
	decoder.DisallowUnknownFields()
	err = decoder.Decode(&response)
	var trailing any
	trailingErr := decoder.Decode(&trailing)
	if err != nil || trailingErr != io.EOF || !response.Accepted || !regexp.MustCompile(`^upl_[A-Za-z0-9_]{1,76}$`).MatchString(response.UploadID) || response.Status != "pending" || response.FileCount != 1 || response.TotalBytes != len([]byte(content)) || response.StagedPath != "uploads/pending/"+response.UploadID || len(response.Warnings) > 20 {
		return uploadResponse{}, errors.New("YouKnowMe upload response is invalid")
	}
	return response, nil
}
func (client *YKMClient) call(ctx context.Context, tool string, args any) (json.RawMessage, error) {
	session, result, err := client.post(ctx, "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{}, "clientInfo": map[string]string{"name": "signal-plane", "version": "v1"}}})
	if err != nil {
		return nil, err
	}
	if session == "" {
		return nil, errors.New("YouKnowMe MCP did not establish a session")
	}
	_, _, err = client.post(ctx, session, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	if err != nil {
		return nil, err
	}
	_, result, err = client.post(ctx, session, map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": tool, "arguments": args}})
	if err != nil {
		return nil, err
	}
	var envelope struct {
		Error  any `json:"error"`
		Result struct {
			IsError bool `json:"isError"`
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
	}
	if err := json.Unmarshal(result, &envelope); err != nil || envelope.Error != nil || envelope.Result.IsError || len(envelope.Result.Content) != 1 {
		return nil, errors.New("YouKnowMe MCP tool result is invalid")
	}
	return json.RawMessage(envelope.Result.Content[0].Text), nil
}
func (client *YKMClient) post(ctx context.Context, session string, payload any) (string, json.RawMessage, error) {
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, client.Config.BaseURL, bytes.NewReader(body))
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "application/json, text/event-stream")
	if session != "" {
		req.Header.Set("Mcp-Session-Id", session)
	}
	if client.Config.AuthMode == YKMAuthCloudflare {
		req.Header.Set("CF-Access-Client-Id", client.Config.ClientID)
		req.Header.Set("CF-Access-Client-Secret", client.Config.ClientSecret)
	} else {
		req.Header.Set("X-YKM-Local-Secret", client.Config.LocalSecret)
	}
	httpClient := http.Client{Timeout: 20 * time.Second}
	if client.Client != nil {
		httpClient = *client.Client
	}
	httpClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return "", nil, fmt.Errorf("YouKnowMe MCP returned %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", nil, err
	}
	if strings.HasPrefix(resp.Header.Get("content-type"), "text/event-stream") {
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "data:") {
				data = []byte(strings.TrimSpace(strings.TrimPrefix(scanner.Text(), "data:")))
				break
			}
		}
	}
	return resp.Header.Get("Mcp-Session-Id"), data, nil
}
