package pushscan

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRegistryAcceptsOnlyAuthenticatedPrecomputedDigestAndTransitionsState(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key := []byte("0123456789abcdef0123456789abcdef")
	token := "holder-only-raw-token-value-1234567890"
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(token))
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	body, err := json.Marshal(registryRequest{Fingerprint: base64.StdEncoding.EncodeToString(mac.Sum(nil)), FingerprintID: "fingerprint-01", Profile: "general-writer-v1", LogicalSessionID: "logical-01", SessionLineageID: "session-01", WorkerID: "worker-01", WorkerStorageLineage: "storage-01", WorkerFenceEpoch: 1, ProfileGeneration: 1, IssuedAt: now, ExpiresAt: now.Add(time.Hour), State: "active"})
	if err != nil {
		t.Fatal(err)
	}
	handler := (RegistryHandler{Store: store, Token: "holder-auth-token", ForensicRetention: 168 * time.Hour}).Handler()
	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/security/fingerprints", bytes.NewReader(body)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/security/fingerprints", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer holder-auth-token")
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("register status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	replay := httptest.NewRecorder()
	replayRequest := httptest.NewRequest(http.MethodPost, "/v1/security/fingerprints", bytes.NewReader(body))
	replayRequest.Header.Set("Authorization", "Bearer holder-auth-token")
	handler.ServeHTTP(replay, replayRequest)
	if replay.Code != http.StatusCreated {
		t.Fatalf("exact replay status=%d body=%s", replay.Code, replay.Body.String())
	}
	var changed registryRequest
	if err := json.Unmarshal(body, &changed); err != nil {
		t.Fatal(err)
	}
	changed.ProfileGeneration++
	changedBody, _ := json.Marshal(changed)
	conflict := httptest.NewRecorder()
	conflictRequest := httptest.NewRequest(http.MethodPost, "/v1/security/fingerprints", bytes.NewReader(changedBody))
	conflictRequest.Header.Set("Authorization", "Bearer holder-auth-token")
	handler.ServeHTTP(conflict, conflictRequest)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	if strings.Contains(string(body), token) {
		t.Fatal("registration request contains raw token")
	}
	var retained int64
	if err := store.db.QueryRow(`SELECT retained_until FROM push_scan_token_fingerprints WHERE fingerprint_id='fingerprint-01'`).Scan(&retained); err != nil || retained != now.Add(time.Hour+168*time.Hour).UnixMilli() {
		t.Fatalf("retained_until=%d err=%v", retained, err)
	}
	attribution, ok, err := store.MatchCandidate(context.Background(), key, token, now)
	if err != nil || !ok || attribution.State != "active" {
		t.Fatalf("match=%#v ok=%v err=%v", attribution, ok, err)
	}

	transition := httptest.NewRecorder()
	transitionRequest := httptest.NewRequest(http.MethodPost, "/v1/security/fingerprints/fingerprint-01/state", strings.NewReader(`{"state":"revoked"}`))
	transitionRequest.Header.Set("Authorization", "Bearer holder-auth-token")
	handler.ServeHTTP(transition, transitionRequest)
	if transition.Code != http.StatusOK {
		t.Fatalf("transition status=%d body=%s", transition.Code, transition.Body.String())
	}
	attribution, ok, err = store.MatchCandidate(context.Background(), key, token, now)
	if err != nil || !ok || attribution.State != "revoked" {
		t.Fatalf("revoked match=%#v ok=%v err=%v", attribution, ok, err)
	}
}

func TestRegistryRejectsUnknownFieldsAndOversizeBodies(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	handler := (RegistryHandler{Store: store, Token: "holder-auth-token", ForensicRetention: 168 * time.Hour}).Handler()
	for name, body := range map[string]string{"unknown": `{"fingerprint":"x","raw_token":"must-not-exist"}`, "caller retention": `{"fingerprint":"x","retained_until":"2099-01-01T00:00:00Z"}`, "oversize": strings.Repeat("x", maxRegistryRequestBytes+1)} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/security/fingerprints", strings.NewReader(body))
			request.Header.Set("Authorization", "Bearer holder-auth-token")
			handler.ServeHTTP(recorder, request)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}
