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
	"reflect"
	"regexp"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const (
	GitHubGreenPRTaskKind   = "github_green_pr_v1"
	GitHubGreenPRContract   = "github_green_pr_v1"
	GitHubGreenPRRepository = "grubbyhacker/repository-worker-lifecycle-test"
	gitHubGreenPRDocument   = `{"budget":{"maxContinuations":1,"maxModelTurns":2,"maxRuntimeMs":1200000,"maxTotalTokens":250000,"perTurnTimeoutMs":600000,"wallClockDeadlineMs":1800000},"completionContract":"github_green_pr_v1","parameterSchema":"github_green_pr_v1","taskKind":"github_green_pr_v1","verifierId":"github_green_pr_v1","version":"1.0.0"}`
)

func init() {
	sum := sha256.Sum256([]byte(gitHubGreenPRDocument))
	if "sha256:"+hex.EncodeToString(sum[:]) != gitHubGreenPRDigest {
		panic("github green PR contract document digest drift")
	}
}

type GitHubGreenPRParameters struct {
	Repository string `json:"repository"`
	BaseBranch string `json:"baseBranch"`
	BranchRef  string `json:"branchRef"`
}

type GitHubGreenPRTask struct{}

func (GitHubGreenPRTask) Descriptor() workledger.TaskDescriptor {
	return workledger.TaskDescriptor{
		Kind:               GitHubGreenPRTaskKind,
		Version:            "1.0.0",
		CompletionContract: GitHubGreenPRContract,
		VerifierID:         GitHubGreenPRContract,
		ContractDigest:     gitHubGreenPRDigest,
	}
}

func (GitHubGreenPRTask) ValidateAdmission(admission workledger.AdmissionPolicy) error {
	if !reflect.DeepEqual(admission.Sources, []string{"manual"}) ||
		!reflect.DeepEqual(admission.Namespaces, []string{GitHubGreenPRRepository}) ||
		!reflect.DeepEqual(admission.ObjectKinds, []string{"repository_task"}) ||
		!reflect.DeepEqual(admission.Events, []string{"repository_change"}) ||
		!reflect.DeepEqual(admission.Actions, []string{"requested"}) {
		return errors.New("github_green_pr_v1 is restricted to the registered manual route")
	}
	return nil
}

func (GitHubGreenPRTask) CanonicalizeParameters(raw json.RawMessage) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var parameters GitHubGreenPRParameters
	if err := decoder.Decode(&parameters); err != nil {
		return nil, fmt.Errorf("decode github green PR task parameters: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("github green PR task parameters contain trailing content")
	}
	if parameters.Repository != GitHubGreenPRRepository ||
		parameters.BaseBranch != "main" ||
		!regexp.MustCompile(`^agent/fleiglabs-repo-agent/[a-z0-9][a-z0-9-]{0,62}$`).MatchString(parameters.BranchRef) {
		return nil, errors.New("github green PR task parameters are outside the registered contract")
	}
	canonical, err := json.Marshal(parameters)
	return json.RawMessage(canonical), err
}

func GitHubGreenPRTaskSelection(branchRef string) *workledger.TaskSelection {
	parameters, _ := json.Marshal(GitHubGreenPRParameters{
		Repository: GitHubGreenPRRepository,
		BaseBranch: "main",
		BranchRef:  branchRef,
	})
	return &workledger.TaskSelection{Kind: GitHubGreenPRTaskKind, Parameters: parameters}
}

func (e *Executor) RecordGitHubGreenPRResult(ctx context.Context, workItemID string, result workledger.VerifierResult) error {
	if e.Store == nil || result.VerifierID != GitHubGreenPRContract || result.CompletionContract != GitHubGreenPRContract {
		return errors.New("registered github green PR observation is required")
	}
	return e.Store.RecordVerifierResult(ctx, workItemID, result, 1, e.now())
}
