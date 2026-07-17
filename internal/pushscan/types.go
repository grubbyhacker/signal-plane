package pushscan

import (
	"context"
	"time"
)

const (
	WireVersion     = "broker/push-tripwire/v1"
	ConsumerAckWait = 6 * time.Minute
)

type PushIdentity struct {
	DeliveryID     string
	Repository     string
	Ref            string
	Before         string
	After          string
	HeadTime       *time.Time
	SourceTime     *time.Time
	ReceivedAt     time.Time
	StreamSequence uint64
}

type MaterialRequest struct {
	Version    string `json:"version"`
	DeliveryID string `json:"delivery_id"`
	Repository string `json:"repository"`
	Ref        string `json:"ref"`
	Before     string `json:"before"`
	After      string `json:"after"`
}

type Commit struct {
	SHA     string `json:"sha"`
	Message string `json:"message"`
}

type File struct {
	CommitSHA     string `json:"commit_sha"`
	Path          string `json:"path"`
	Side          string `json:"side"`
	Status        string `json:"status"`
	BlobSHA       string `json:"blob_sha"`
	Size          int64  `json:"size"`
	ContentBase64 string `json:"content_base64"`
}

type MaterialBounds struct {
	CommitCount int   `json:"commit_count"`
	PathCount   int   `json:"path_count"`
	TotalBytes  int64 `json:"total_bytes"`
}

type Material struct {
	Version    string         `json:"version"`
	DeliveryID string         `json:"delivery_id"`
	Repository string         `json:"repository"`
	Ref        string         `json:"ref"`
	Before     string         `json:"before"`
	After      string         `json:"after"`
	Complete   bool           `json:"complete"`
	Bounds     MaterialBounds `json:"bounds"`
	Commits    []Commit       `json:"commits"`
	Files      []File         `json:"files"`
	ReasonCode string         `json:"reason_code,omitempty"`
}

type Attribution struct {
	FingerprintID        string
	Profile              string
	LogicalSessionID     string
	SessionLineageID     string
	WorkerID             string
	WorkerStorageLineage string
	WorkerFenceEpoch     int64
	ProfileGeneration    int64
	IssuedAt             time.Time
	ExpiresAt            time.Time
	State                string
}

type Finding struct {
	ID          string
	Severity    string
	ReasonCode  string
	Attribution Attribution
}

type ResponseRequest struct {
	Version           string           `json:"version"`
	FindingID         string           `json:"finding_id"`
	DeliveryID        string           `json:"delivery_id"`
	Repository        string           `json:"repository"`
	Ref               string           `json:"ref"`
	Before            string           `json:"before"`
	After             string           `json:"after"`
	Severity          string           `json:"severity"`
	ReasonCode        string           `json:"reason_code"`
	FingerprintID     string           `json:"fingerprint_id,omitempty"`
	Profile           string           `json:"profile"`
	ProfileGeneration int64            `json:"profile_generation"`
	Binding           *ResponseBinding `json:"binding,omitempty"`
	Actions           []string         `json:"actions"`
}

type ResponseBinding struct {
	LogicalSessionID     string `json:"logical_session_id"`
	SessionLineageID     string `json:"session_lineage_id"`
	WorkerID             string `json:"worker_id"`
	WorkerStorageLineage string `json:"worker_storage_lineage_id"`
	WorkerFenceEpoch     int64  `json:"worker_fence_epoch"`
}

type ActionResult struct {
	Action      string    `json:"action"`
	State       string    `json:"state"`
	CompletedAt time.Time `json:"completed_at"`
}

type ResponseResult struct {
	Version          string         `json:"version"`
	FindingID        string         `json:"finding_id"`
	IdempotentReplay bool           `json:"idempotent_replay"`
	Actions          []ActionResult `json:"actions"`
}

type Broker interface {
	Material(ctx context.Context, request MaterialRequest) (Material, error)
	Respond(ctx context.Context, idempotencyKey string, request ResponseRequest) (ResponseResult, error)
}

type Bounds struct {
	MaxCommits, MaxPaths, MaxCandidates, MaxDecodeDepth int
	MaxBlobBytes, MaxTotalBytes                         int64
}

type FingerprintRegistration struct {
	FingerprintID, Profile, LogicalSessionID, SessionLineageID string
	WorkerID, WorkerStorageLineage                             string
	WorkerFenceEpoch, ProfileGeneration                        int64
	IssuedAt, ExpiresAt, RetainedUntil                         time.Time
	State                                                      string
}

type Result struct {
	DeliveryID, FindingID, Status, Severity, ReasonCode string
	FingerprintID, Profile, LogicalSessionID            string
	WorkerID, SessionLineageID, WorkerStorageLineage    string
	WorkerFenceEpoch, ProfileGeneration                 int64
	ReceiptAt, ScanStartedAt, MaterialCompletedAt       time.Time
	FindingAt, ResponseRequestedAt, HaltedAt            time.Time
	FenceRequestedAt, FencedAt                          time.Time
	FenceState                                          string
	TerminalAt, SLODeadline                             time.Time
	SLOBreached                                         bool
	SLOState                                            string
	AlertState                                          string
	AlertRequestedAt                                    time.Time
}

type SecurityEvent struct {
	Version              string    `json:"version"`
	EventID              string    `json:"event_id"`
	State                string    `json:"state"`
	FindingID            string    `json:"finding_id"`
	DeliveryID           string    `json:"delivery_id"`
	Repository           string    `json:"repository"`
	Ref                  string    `json:"ref"`
	Before               string    `json:"before"`
	After                string    `json:"after"`
	Severity             string    `json:"severity"`
	ReasonCode           string    `json:"reason_code"`
	FingerprintID        string    `json:"fingerprint_id,omitempty"`
	Profile              string    `json:"profile"`
	ProfileGeneration    int64     `json:"profile_generation"`
	LogicalSessionID     string    `json:"logical_session_id,omitempty"`
	SessionLineageID     string    `json:"session_lineage_id,omitempty"`
	WorkerID             string    `json:"worker_id,omitempty"`
	WorkerStorageLineage string    `json:"worker_storage_lineage_id,omitempty"`
	WorkerFenceEpoch     int64     `json:"worker_fence_epoch,omitempty"`
	ReceivedAt           time.Time `json:"received_at"`
	ReceiptAt            time.Time `json:"receipt_at"`
	FindingAt            time.Time `json:"finding_at"`
	AlertRequestedAt     time.Time `json:"alert_requested_at"`
}

type EventSink interface {
	Publish(context.Context, SecurityEvent) error
}
