package agentsession

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const (
	RepositoryChangeTaskKind     = "repository_change_v1"
	RepositoryCompletionContract = "repository_state_v1"
	NeutralRepositoryID          = "neutral/pr10-proof"
	repositoryContractDocument   = `{"budget":{"maxContinuations":1,"maxModelTurns":2,"maxRuntimeMs":1200000,"maxTotalTokens":250000,"perTurnTimeoutMs":600000,"wallClockDeadlineMs":1800000},"completionContract":"repository_state_v1","parameterSchema":"neutral_repository_change_v1","reasonCodes":["base_revision_mismatch","branch_mismatch","evidence_ambiguous","forbidden_action","head_not_advanced","head_not_reachable","ignored_state","untracked_state","validation_missing","validation_stale","worktree_dirty"],"taskKind":"repository_change_v1","verifierId":"repository_state_v1","version":"1.0.0"}`
)

type RepositoryChangeParameters struct {
	RepositoryID        string `json:"repositoryId"`
	BaseRevision        string `json:"baseRevision"`
	BranchRef           string `json:"branchRef"`
	ValidationSelection string `json:"validationSelection"`
}

type RepositoryChangeTask struct{}

func (RepositoryChangeTask) Descriptor() workledger.TaskDescriptor {
	digest := sha256.Sum256([]byte(repositoryContractDocument))
	return workledger.TaskDescriptor{
		Kind:               RepositoryChangeTaskKind,
		Version:            "1.0.0",
		CompletionContract: RepositoryCompletionContract,
		VerifierID:         RepositoryCompletionContract,
		ContractDigest:     "sha256:" + hex.EncodeToString(digest[:]),
	}
}

func (RepositoryChangeTask) ValidateAdmission(admission workledger.AdmissionPolicy) error {
	if !reflect.DeepEqual(admission.Sources, []string{"manual"}) ||
		!reflect.DeepEqual(admission.Namespaces, []string{NeutralRepositoryID}) ||
		!reflect.DeepEqual(admission.ObjectKinds, []string{"repository_task"}) ||
		!reflect.DeepEqual(admission.Events, []string{"repository_change"}) ||
		!reflect.DeepEqual(admission.Actions, []string{"requested"}) {
		return errors.New("repository_change_v1 is restricted to the neutral manual staging route")
	}
	return nil
}

func (RepositoryChangeTask) CanonicalizeParameters(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var parameters RepositoryChangeParameters
	if err := decoder.Decode(&parameters); err != nil {
		return nil, fmt.Errorf("decode repository task parameters: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("repository task parameters contain trailing content")
	}
	if parameters.RepositoryID != NeutralRepositoryID ||
		!regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(parameters.BaseRevision) ||
		!regexp.MustCompile(`^agent/pr10-proof/[a-z0-9][a-z0-9-]{0,62}$`).MatchString(parameters.BranchRef) ||
		parameters.ValidationSelection != "required" {
		return nil, errors.New("repository task parameters are outside the reviewed neutral contract")
	}
	canonical, err := json.Marshal(parameters)
	return json.RawMessage(canonical), err
}

func NeutralRepositoryTaskSelection(baseRevision, branchRef string) *workledger.TaskSelection {
	parameters, _ := json.Marshal(RepositoryChangeParameters{
		RepositoryID:        NeutralRepositoryID,
		BaseRevision:        baseRevision,
		BranchRef:           branchRef,
		ValidationSelection: "required",
	})
	return &workledger.TaskSelection{Kind: RepositoryChangeTaskKind, Parameters: parameters}
}
