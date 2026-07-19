package agentsession

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"unicode/utf8"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const repositoryContractDigest = "sha256:df72462d2bde6674349b2265d8768c6bba0b3368114cd015195ce66a697fc102"

var sha256Digest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// RegisteredTask is the immutable, broker-mediated durable-admission input.
// It is assembled only from the ledger's single joined work-item/snapshot read.
type RegisteredTask struct {
	Source   RegisteredTaskSource
	Snapshot RegisteredTaskSnapshot
	Digest   string
}

type RegisteredTaskSource struct {
	WorkItemID      string `json:"work_item_id"`
	RouteSnapshotID string `json:"route_snapshot_id"`
}

type RegisteredTaskSnapshot struct {
	TaskKind           string          `json:"taskKind"`
	TaskVersion        string          `json:"taskVersion"`
	CompletionContract string          `json:"completionContract"`
	VerifierID         string          `json:"verifierId"`
	ContractDigest     string          `json:"contractDigest"`
	TaskEvidenceDigest string          `json:"taskEvidenceDigest"`
	Parameters         json.RawMessage `json:"parameters"`
}

type brokerAcquireV2Request struct {
	Version              string                 `json:"version"`
	Profile              string                 `json:"profile"`
	IdempotencyKey       string                 `json:"idempotency_key"`
	SessionBinding       string                 `json:"session_binding"`
	RegisteredTaskSource RegisteredTaskSource   `json:"registered_task_source"`
	RegisteredTask       RegisteredTaskSnapshot `json:"registered_task"`
	AdmissionTaskDigest  string                 `json:"admission_task_digest"`
}

func (e *Executor) registeredTask(ctx context.Context, request workledger.ExecutorRequest) (RegisteredTask, error) {
	if e.Store == nil || request.WorkItem.ID == "" {
		return RegisteredTask{}, errors.New("registered task source is unavailable")
	}
	snapshot, err := e.Store.WorkTaskSnapshot(ctx, request.WorkItem.ID)
	if err != nil {
		return RegisteredTask{}, err
	}
	task := RegisteredTask{
		Source:   RegisteredTaskSource{WorkItemID: snapshot.WorkItemID, RouteSnapshotID: snapshot.RouteSnapshotID},
		Snapshot: RegisteredTaskSnapshot{TaskKind: snapshot.Kind, TaskVersion: snapshot.Version, CompletionContract: snapshot.CompletionContract, VerifierID: snapshot.VerifierID, ContractDigest: snapshot.ContractDigest, TaskEvidenceDigest: snapshot.TaskEvidenceDigest, Parameters: append(json.RawMessage(nil), snapshot.Parameters...)},
	}
	digest, err := admissionTaskDigest(task.Source, task.Snapshot)
	if err != nil {
		return RegisteredTask{}, err
	}
	task.Digest = digest
	if err := task.Validate("session:" + snapshot.WorkItemID); err != nil {
		return RegisteredTask{}, err
	}
	return task, nil
}

func (task RegisteredTask) Validate(binding string) error {
	if !boundedID(task.Source.WorkItemID) || !boundedID(task.Source.RouteSnapshotID) || !utf8.ValidString(task.Source.WorkItemID) || !utf8.ValidString(task.Source.RouteSnapshotID) || binding != "session:"+task.Source.WorkItemID {
		return errors.New("registered task source or session binding is invalid")
	}
	if task.Snapshot.TaskKind != RepositoryChangeTaskKind || task.Snapshot.TaskVersion != "1.0.0" || task.Snapshot.CompletionContract != RepositoryCompletionContract || task.Snapshot.VerifierID != RepositoryCompletionContract || task.Snapshot.ContractDigest != repositoryContractDigest || !sha256Digest.MatchString(task.Snapshot.TaskEvidenceDigest) {
		return errors.New("registered task descriptor is outside the locked contract")
	}
	canonical, err := (RepositoryChangeTask{}).CanonicalizeParameters(task.Snapshot.Parameters)
	if err != nil || string(canonical) != string(task.Snapshot.Parameters) {
		return errors.New("registered task parameters are not canonical")
	}
	digest, err := admissionTaskDigest(task.Source, task.Snapshot)
	if err != nil || task.Digest != digest {
		return errors.New("registered task admission digest is invalid")
	}
	return nil
}

func admissionTaskDigest(source RegisteredTaskSource, task RegisteredTaskSnapshot) (string, error) {
	if !json.Valid(task.Parameters) {
		return "", errors.New("registered task parameters are invalid JSON")
	}
	parameters, err := jcsObjectFromJSON(task.Parameters)
	if err != nil {
		return "", fmt.Errorf("canonicalize registered task parameters: %w", err)
	}
	// RFC 8785 orders object names lexicographically. All values here are strings
	// or the already-validated parameters object, so no floating-point conversion
	// is involved in the digest surface.
	canonical := `{"registered_task":{"completionContract":` + jcsString(task.CompletionContract) + `,"contractDigest":` + jcsString(task.ContractDigest) + `,"parameters":` + parameters + `,"taskEvidenceDigest":` + jcsString(task.TaskEvidenceDigest) + `,"taskKind":` + jcsString(task.TaskKind) + `,"taskVersion":` + jcsString(task.TaskVersion) + `,"verifierId":` + jcsString(task.VerifierID) + `},"registered_task_source":{"route_snapshot_id":` + jcsString(source.RouteSnapshotID) + `,"work_item_id":` + jcsString(source.WorkItemID) + `}}`
	sum := sha256.Sum256([]byte(canonical))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func jcsObjectFromJSON(raw []byte) (string, error) {
	var parameters RepositoryChangeParameters
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parameters); err != nil {
		return "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return "", errors.New("parameters contain trailing content")
	}
	if parameters.RepositoryID == "" || parameters.BaseRevision == "" || parameters.BranchRef == "" || parameters.ValidationSelection == "" {
		return "", errors.New("incomplete parameters")
	}
	return `{"baseRevision":` + jcsString(parameters.BaseRevision) + `,"branchRef":` + jcsString(parameters.BranchRef) + `,"repositoryId":` + jcsString(parameters.RepositoryID) + `,"validationSelection":` + jcsString(parameters.ValidationSelection) + `}`, nil
}

func jcsString(value string) string {
	if !utf8.ValidString(value) {
		return ""
	}
	encoded := make([]byte, 0, len(value)+2)
	encoded = append(encoded, '"')
	for _, r := range value {
		switch r {
		case '"', '\\':
			encoded = append(encoded, '\\', byte(r))
		case '\b':
			encoded = append(encoded, '\\', 'b')
		case '\t':
			encoded = append(encoded, '\\', 't')
		case '\n':
			encoded = append(encoded, '\\', 'n')
		case '\f':
			encoded = append(encoded, '\\', 'f')
		case '\r':
			encoded = append(encoded, '\\', 'r')
		default:
			if r < 0x20 {
				encoded = append(encoded, '\\', 'u', '0', '0', "0123456789abcdef"[r>>4], "0123456789abcdef"[r&0xf])
			} else {
				encoded = append(encoded, string(r)...)
			}
		}
	}
	return string(append(encoded, '"'))
}
