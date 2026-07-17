package pushscan

import (
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

const maxRegistryRequestBytes = 16 << 10

var registryIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)

type RegistryHandler struct {
	Store             *Store
	Token             string
	ForensicRetention time.Duration
}

type registryRequest struct {
	Fingerprint          string    `json:"fingerprint"`
	FingerprintID        string    `json:"fingerprint_id"`
	Profile              string    `json:"profile"`
	LogicalSessionID     string    `json:"logical_session_id"`
	SessionLineageID     string    `json:"session_lineage_id"`
	WorkerID             string    `json:"worker_id"`
	WorkerStorageLineage string    `json:"worker_storage_lineage_id"`
	WorkerFenceEpoch     int64     `json:"worker_fence_epoch"`
	ProfileGeneration    int64     `json:"profile_generation"`
	IssuedAt             time.Time `json:"issued_at"`
	ExpiresAt            time.Time `json:"expires_at"`
	State                string    `json:"state"`
}

func (handler RegistryHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(writer http.ResponseWriter, request *http.Request) {
		if handler.Store == nil || handler.Store.Ready(request.Context()) != nil {
			http.Error(writer, `{"error":"not_ready"}`, http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("ok\n"))
	})
	mux.HandleFunc("POST /v1/security/fingerprints", handler.register)
	mux.HandleFunc("POST /v1/security/fingerprints/{fingerprint_id}/state", handler.transition)
	return mux
}

func (handler RegistryHandler) register(writer http.ResponseWriter, request *http.Request) {
	if !handler.authorized(request) {
		http.Error(writer, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	var input registryRequest
	if err := decodeRegistryRequest(request, &input); err != nil || !validRegistryRequest(input) || handler.ForensicRetention <= 0 {
		http.Error(writer, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	digest, err := base64.StdEncoding.Strict().DecodeString(input.Fingerprint)
	if err != nil {
		http.Error(writer, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	registration := FingerprintRegistration{FingerprintID: input.FingerprintID, Profile: input.Profile, LogicalSessionID: input.LogicalSessionID, SessionLineageID: input.SessionLineageID, WorkerID: input.WorkerID, WorkerStorageLineage: input.WorkerStorageLineage, WorkerFenceEpoch: input.WorkerFenceEpoch, ProfileGeneration: input.ProfileGeneration, IssuedAt: input.IssuedAt, ExpiresAt: input.ExpiresAt, RetainedUntil: input.ExpiresAt.Add(handler.ForensicRetention), State: input.State}
	if err := handler.Store.RegisterFingerprint(request.Context(), digest, registration); err != nil {
		if errors.Is(err, ErrFingerprintConflict) {
			http.Error(writer, `{"error":"registration_conflict"}`, http.StatusConflict)
			return
		}
		http.Error(writer, `{"error":"registration_rejected"}`, http.StatusUnprocessableEntity)
		return
	}
	writer.WriteHeader(http.StatusCreated)
	_, _ = writer.Write([]byte(`{"status":"registered"}` + "\n"))
}

func (handler RegistryHandler) transition(writer http.ResponseWriter, request *http.Request) {
	if !handler.authorized(request) {
		http.Error(writer, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	id := request.PathValue("fingerprint_id")
	var input struct {
		State string `json:"state"`
	}
	if !registryIDPattern.MatchString(id) || decodeRegistryRequest(request, &input) != nil || (input.State != "expired" && input.State != "revoked") {
		http.Error(writer, `{"error":"invalid_request"}`, http.StatusBadRequest)
		return
	}
	if err := handler.Store.TransitionFingerprintState(request.Context(), id, input.State); err != nil {
		http.Error(writer, `{"error":"transition_rejected"}`, http.StatusConflict)
		return
	}
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(`{"status":"transitioned"}` + "\n"))
}

func (handler RegistryHandler) authorized(request *http.Request) bool {
	const prefix = "Bearer "
	provided := request.Header.Get("Authorization")
	if handler.Token == "" || !strings.HasPrefix(provided, prefix) {
		return false
	}
	provided = strings.TrimPrefix(provided, prefix)
	return len(provided) == len(handler.Token) && subtle.ConstantTimeCompare([]byte(provided), []byte(handler.Token)) == 1
}

func decodeRegistryRequest(request *http.Request, output any) error {
	raw, err := io.ReadAll(io.LimitReader(request.Body, maxRegistryRequestBytes+1))
	if err != nil || len(raw) > maxRegistryRequestBytes {
		return errors.New("registry request exceeds bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("trailing registry request content")
	}
	return nil
}

func validRegistryRequest(input registryRequest) bool {
	values := []string{input.Profile, input.LogicalSessionID, input.SessionLineageID, input.WorkerID, input.WorkerStorageLineage}
	if !registryIDPattern.MatchString(input.FingerprintID) || input.Fingerprint == "" {
		return false
	}
	for _, value := range values {
		if value == "" || len(value) > 256 {
			return false
		}
	}
	return true
}
