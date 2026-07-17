package pushscan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPBroker struct {
	BaseURL, Token   string
	Client           *http.Client
	MaxResponseBytes int64
}

func NewHTTPBroker(rawURL, token string, client *http.Client, bounds Bounds) (*HTTPBroker, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.EscapedPath() != "/v1/security/push-tripwire" || token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("push tripwire broker configuration is invalid")
	}
	if client == nil {
		client = http.DefaultClient
	}
	limit, err := brokerWireLimit(bounds)
	if err != nil {
		return nil, err
	}
	return &HTTPBroker{BaseURL: strings.TrimSuffix(rawURL, "/"), Token: token, Client: client, MaxResponseBytes: limit}, nil
}

func (b *HTTPBroker) Material(ctx context.Context, request MaterialRequest) (Material, error) {
	var result Material
	err := b.post(ctx, "/material", "", request, &result)
	return result, err
}

func (b *HTTPBroker) Respond(ctx context.Context, idempotencyKey string, request ResponseRequest) (ResponseResult, error) {
	var result ResponseResult
	err := b.post(ctx, "/respond", idempotencyKey, request, &result)
	return result, err
}

func (b *HTTPBroker) post(ctx context.Context, path, idempotencyKey string, input, output any) error {
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.BaseURL+path, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+b.Token)
	req.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := b.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, b.MaxResponseBytes+1))
	if err != nil || int64(len(raw)) > b.MaxResponseBytes {
		return errors.New("bounded broker response read failed")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var wire struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(raw, &wire)
		return MaterialError{Code: safeReason(wire.Code)}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode strict broker response: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("broker response contains trailing content")
	}
	return nil
}

func brokerWireLimit(bounds Bounds) (int64, error) {
	if bounds.MaxTotalBytes <= 0 || bounds.MaxPaths <= 0 || bounds.MaxCommits <= 0 || bounds.MaxTotalBytes > (1<<62) {
		return 0, errors.New("push tripwire broker bounds are invalid")
	}
	encoded := ((bounds.MaxTotalBytes + 2) / 3) * 4
	metadata := int64(1<<20) + int64(bounds.MaxPaths)*2048 + int64(bounds.MaxCommits)*1024
	if metadata < 0 || encoded > (1<<62)-metadata {
		return 0, errors.New("push tripwire broker bounds overflow")
	}
	return encoded + metadata, nil
}
