package pushscan

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoldenFixturesMatchCanonicalBytesAndStrictSchemas(t *testing.T) {
	for name, output := range map[string]any{
		"material-request.json":  &MaterialRequest{},
		"material-response.json": &Material{},
		"response-request.json":  &ResponseRequest{},
		"response-result.json":   &ResponseResult{},
	} {
		t.Run(name, func(t *testing.T) {
			fixture, err := os.ReadFile(filepath.Join("..", "..", "testdata", "push-tripwire", name))
			if err != nil {
				t.Fatal(err)
			}
			decoder := json.NewDecoder(bytes.NewReader(fixture))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(output); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.MarshalIndent(output, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			encoded = append(encoded, '\n')
			if !bytes.Equal(encoded, fixture) {
				t.Fatalf("Signal schema does not round-trip canonical fixture\nwant:\n%s\ngot:\n%s", fixture, encoded)
			}
		})
	}
}

func TestHTTPBrokerWireLimitIncludesBase64ExpansionAtDecodedBoundary(t *testing.T) {
	decoded := bytes.Repeat([]byte("x"), 3<<20)
	material := Material{Version: WireVersion, DeliveryID: "delivery-01", Repository: "owner/repo", Ref: "refs/heads/main", Before: strings.Repeat("a", 40), After: strings.Repeat("b", 40), Files: []File{{CommitSHA: strings.Repeat("b", 40), Path: "large.bin", Side: "after", Status: "modified", BlobSHA: strings.Repeat("c", 40), Size: int64(len(decoded)), ContentBase64: base64.StdEncoding.EncodeToString(decoded)}}, Bounds: MaterialBounds{PathCount: 1, TotalBytes: int64(len(decoded))}, Complete: true}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/security/push-tripwire/material" {
			t.Fatalf("path = %s", request.URL.Path)
		}
		_ = json.NewEncoder(writer).Encode(material)
	}))
	defer server.Close()
	bounds := Bounds{MaxCommits: 100, MaxPaths: 300, MaxTotalBytes: int64(len(decoded))}
	broker, err := NewHTTPBroker(server.URL+"/v1/security/push-tripwire", "broker-token", server.Client(), bounds)
	if err != nil {
		t.Fatal(err)
	}
	result, err := broker.Material(context.Background(), MaterialRequest{Version: WireVersion})
	if err != nil || len(result.Files) != 1 || result.Files[0].Size != int64(len(decoded)) {
		t.Fatalf("result files=%d err=%v", len(result.Files), err)
	}
}
