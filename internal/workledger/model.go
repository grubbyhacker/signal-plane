package workledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const SchemaVersion = 4

type WorkState string

const (
	StateObserved   WorkState = "observed"
	StateAdmitted   WorkState = "admitted"
	StateActive     WorkState = "active"
	StateWaiting    WorkState = "waiting"
	StateCompleted  WorkState = "completed"
	StateFailed     WorkState = "failed"
	StateCancelled  WorkState = "cancelled"
	StateSuperseded WorkState = "superseded"
	StateDeadLetter WorkState = "dead_letter"
)

type ExecutorKind string

const (
	ExecutorDeterministicTool ExecutorKind = "deterministic_tool"
	ExecutorPolicyEvaluator   ExecutorKind = "policy_evaluator"
	ExecutorAgentSession      ExecutorKind = "agent_session"
)

type SerializationScope string

const (
	SerializeObject    SerializationScope = "object"
	SerializeNamespace SerializationScope = "namespace"
	SerializeRoute     SerializationScope = "route"
)

type AdmissionPolicy struct {
	Sources     []string `json:"sources"`
	Namespaces  []string `json:"namespaces"`
	ObjectKinds []string `json:"object_kinds"`
	Events      []string `json:"events"`
	Actions     []string `json:"actions,omitempty"`
}

type ConcurrencyPolicy struct {
	Serialization SerializationScope `json:"serialization"`
	Supersede     bool               `json:"supersede"`
}

type RetryPolicy struct {
	MaxAttempts int             `json:"max_attempts"`
	Backoff     []time.Duration `json:"backoff"`
}

type RouteDefinition struct {
	ID              string            `json:"id"`
	SchemaVersion   int               `json:"schema_version"`
	SemanticVersion string            `json:"semantic_version"`
	ExecutorID      string            `json:"executor_id"`
	Admission       AdmissionPolicy   `json:"admission"`
	Concurrency     ConcurrencyPolicy `json:"concurrency"`
	Retry           RetryPolicy       `json:"retry"`
}

type RouteSnapshot struct {
	ID              string
	RouteID         string
	SchemaVersion   int
	SemanticVersion string
	Digest          string
	ExecutorID      string
	ExecutorKind    ExecutorKind
	ExecutorVersion string
	Admission       AdmissionPolicy
	Concurrency     ConcurrencyPolicy
	Retry           RetryPolicy
	ActivatedAt     time.Time
	RetiredAt       *time.Time
}

type Event struct {
	SignalID           string
	SourceDeliveryID   string
	TransportStream    string
	TransportSequence  uint64
	Source             string
	Namespace          string
	ObjectKind         string
	ObjectID           string
	EventKind          string
	Action             string
	ActorClass         string
	SourceRevision     string
	CorrelationID      string
	CausationID        string
	RootWorkItemID     string
	ParentWorkItemID   string
	OriginatingSession string
	OriginatingTurn    string
	HopCount           int
	ExpiresAt          *time.Time
	PayloadDigest      string
	EvidenceRef        string
	ReceivedAt         time.Time
}

func (event Event) Digest() string {
	canonical := struct {
		Source, Namespace, ObjectKind, ObjectID       string
		EventKind, Action, ActorClass, SourceRevision string
		PayloadDigest                                 string
	}{event.Source, event.Namespace, event.ObjectKind, event.ObjectID, event.EventKind, event.Action, event.ActorClass, event.SourceRevision, event.PayloadDigest}
	encoded, _ := json.Marshal(canonical)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func (event Event) SemanticObjectKey() string {
	return strings.Join([]string{event.Source, event.Namespace, event.ObjectKind, event.ObjectID}, ":")
}

type WorkItem struct {
	ID                        string
	RouteSnapshotID           string
	RouteID                   string
	SemanticObjectKey         string
	Source                    string
	Namespace                 string
	ObjectKind                string
	ObjectID                  string
	SourceRevision            string
	SerializationKey          string
	State                     WorkState
	StateVersion              int
	SupersededByID            string
	LatestExecutorCorrelation string
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
	TerminalAt                *time.Time
	NextAttemptAt             *time.Time
}

type AttemptState string

const (
	AttemptRunning        AttemptState = "running"
	AttemptRecoverable    AttemptState = "recoverable"
	AttemptRetryScheduled AttemptState = "retry_scheduled"
	AttemptSucceeded      AttemptState = "succeeded"
	AttemptFailed         AttemptState = "failed"
	AttemptInterrupted    AttemptState = "interrupted"
	AttemptSuperseded     AttemptState = "superseded"
)

type ExecutorAttempt struct {
	ID                       string
	WorkItemID               string
	AttemptNumber            int
	ExecutorID               string
	ExecutorKind             ExecutorKind
	ExecutorVersion          string
	IdempotencyKey           string
	OperationIdempotencyKey  string
	RequestedOperationDigest string
	State                    AttemptState
	RetryClassification      string
	ExternalCorrelation      string
	ResultDigest             string
	CreatedAt                time.Time
}

type AdmissionResult struct {
	WorkItem  WorkItem
	EventID   string
	Duplicate bool
}

type ReleaseOperation struct {
	RepositoryID, InstallationID, ReleaseID                     int64
	Repository, Tag, PublishedAt, TargetCommitish, CommitSHA    string
	AssetID, AssetSize                                          int64
	AssetName, AssetContentType, ProviderDigest, ComputedDigest string
}

func (operation ReleaseOperation) Validate() error {
	if operation.Repository != "grubbyhacker/resume-builder" || operation.RepositoryID <= 0 || operation.InstallationID != 146625575 || operation.ReleaseID <= 0 || operation.AssetID <= 0 || operation.AssetSize <= 0 || operation.AssetSize > 20*1024 {
		return errors.New("release operation has invalid fixed identity or bounds")
	}
	for name, value := range map[string]string{"tag": operation.Tag, "published_at": operation.PublishedAt, "target_commitish": operation.TargetCommitish, "commit_sha": operation.CommitSHA, "asset_name": operation.AssetName, "asset_content_type": operation.AssetContentType, "provider_digest": operation.ProviderDigest, "computed_digest": operation.ComputedDigest} {
		if value == "" || len(value) > 256 {
			return fmt.Errorf("release operation %s is missing or oversized", name)
		}
	}
	tagPattern := regexp.MustCompile(`^v\d{4}\.\d{2}\.\d{2}-[0-9a-f]{7}$`)
	if !tagPattern.MatchString(operation.Tag) || !regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(operation.CommitSHA) {
		return errors.New("release operation tag or commit contract is invalid")
	}
	date := strings.ReplaceAll(operation.Tag[1:11], ".", "")
	if _, err := time.Parse(time.RFC3339, operation.PublishedAt); err != nil {
		return errors.New("release operation published_at is invalid")
	}
	if operation.TargetCommitish != operation.CommitSHA || operation.AssetContentType != "text/markdown" || operation.Tag[len(operation.Tag)-7:] != operation.CommitSHA[:7] || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*_`+date+`\.structured\.md$`).MatchString(operation.AssetName) {
		return errors.New("release operation tag, commit, or asset contract is invalid")
	}
	if !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(operation.ProviderDigest) || operation.ProviderDigest != operation.ComputedDigest {
		return errors.New("release operation digest contract is invalid")
	}
	return nil
}

func DecodeRouteDefinition(data []byte) (RouteDefinition, error) {
	var definition RouteDefinition
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&definition); err != nil {
		return RouteDefinition{}, fmt.Errorf("decode route definition: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return RouteDefinition{}, errors.New("decode route definition: trailing JSON")
		}
		return RouteDefinition{}, fmt.Errorf("decode route definition trailing content: %w", err)
	}
	if err := definition.Validate(); err != nil {
		return RouteDefinition{}, err
	}
	return definition, nil
}

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func (definition RouteDefinition) Validate() error {
	if !identifierPattern.MatchString(definition.ID) {
		return errors.New("route id must be a bounded lowercase identifier")
	}
	if definition.SchemaVersion <= 0 || strings.TrimSpace(definition.SemanticVersion) == "" {
		return errors.New("route schema and semantic versions are required")
	}
	if !identifierPattern.MatchString(definition.ExecutorID) {
		return errors.New("route executor_id must name a registered executor")
	}
	if len(definition.Admission.Sources) == 0 || len(definition.Admission.ObjectKinds) == 0 || len(definition.Admission.Events) == 0 {
		return errors.New("route admission requires sources, object kinds, and events")
	}
	for name, values := range map[string][]string{"sources": definition.Admission.Sources, "namespaces": definition.Admission.Namespaces, "object_kinds": definition.Admission.ObjectKinds, "events": definition.Admission.Events, "actions": definition.Admission.Actions} {
		if len(values) > 64 {
			return fmt.Errorf("route admission %s exceeds 64 entries", name)
		}
		seen := make(map[string]struct{}, len(values))
		for _, value := range values {
			if strings.TrimSpace(value) == "" || len(value) > 256 {
				return fmt.Errorf("route admission %s contains an empty or oversized value", name)
			}
			if _, duplicate := seen[value]; duplicate {
				return fmt.Errorf("route admission %s contains duplicate %q", name, value)
			}
			seen[value] = struct{}{}
		}
	}
	switch definition.Concurrency.Serialization {
	case SerializeObject, SerializeNamespace, SerializeRoute:
	default:
		return fmt.Errorf("unsupported serialization scope %q", definition.Concurrency.Serialization)
	}
	if definition.Retry.MaxAttempts < 1 || definition.Retry.MaxAttempts > 10 {
		return errors.New("route max_attempts must be between 1 and 10")
	}
	if len(definition.Retry.Backoff) != definition.Retry.MaxAttempts-1 {
		return errors.New("route retry backoff must define one delay per retry")
	}
	for _, delay := range definition.Retry.Backoff {
		if delay <= 0 || delay > 24*time.Hour {
			return errors.New("route retry delays must be positive and at most 24 hours")
		}
	}
	return nil
}

func (definition RouteDefinition) Digest() (string, error) {
	if err := definition.Validate(); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(definition)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (policy AdmissionPolicy) Matches(event Event) bool {
	return contains(policy.Sources, event.Source) && contains(policy.Namespaces, event.Namespace) &&
		contains(policy.ObjectKinds, event.ObjectKind) && contains(policy.Events, event.EventKind) && contains(policy.Actions, event.Action)
}

func contains(allowed []string, value string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if candidate == value {
			return true
		}
	}
	return false
}

func serializationKey(route RouteDefinition, event Event) string {
	switch route.Concurrency.Serialization {
	case SerializeNamespace:
		return route.ID + ":namespace:" + event.Namespace
	case SerializeRoute:
		return route.ID + ":route"
	default:
		return route.ID + ":object:" + event.SemanticObjectKey()
	}
}
