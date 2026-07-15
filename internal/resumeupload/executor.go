package resumeupload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

const ExecutorID = "youknowme_upload_v1"

type Executor struct {
	Store  *workledger.Store
	GitHub AssetReader
	YKM    Uploader
	Now    func() time.Time
}
type AssetReader interface {
	DownloadVerified(context.Context, workledger.ReleaseOperation) ([]byte, error)
}
type Uploader interface {
	Upload(context.Context, string, string, string) (uploadResponse, error)
}

func (executor *Executor) Descriptor() workledger.ExecutorDescriptor {
	return workledger.ExecutorDescriptor{ID: ExecutorID, Kind: workledger.ExecutorDeterministicTool, Version: "v1"}
}
func (executor *Executor) Execute(ctx context.Context, request workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	operation, err := executor.Store.ReleaseOperation(ctx, request.WorkItem.ID)
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomePermanentFailure, SanitizedError: "release operation is unavailable"}, nil
	}
	if external, result, ok, err := executor.Store.ContentResult(ctx, operation.ComputedDigest); err != nil {
		return workledger.ExecutorResult{}, err
	} else if ok {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted, ExternalCorrelation: external, ResultDigest: result}, nil
	}
	content, err := executor.GitHub.DownloadVerified(ctx, operation)
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "github_release_read", SanitizedError: "verified release asset retrieval failed"}, nil
	}
	key := "signal-plane:resume:v1:" + strings.TrimPrefix(operation.ComputedDigest, "sha256:")
	if request.Attempt.ID != "" && (request.Attempt.OperationIdempotencyKey != key || request.Attempt.RequestedOperationDigest != operation.ComputedDigest) {
		return workledger.ExecutorResult{Outcome: workledger.OutcomePermanentFailure, SanitizedError: "attempt operation evidence does not match durable release"}, nil
	}
	filename := "resume_" + strings.TrimPrefix(operation.ComputedDigest, "sha256:") + ".structured.md"
	response, err := executor.YKM.Upload(ctx, filename, string(content), key)
	if err != nil {
		return workledger.ExecutorResult{Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "youknowme_ambiguous", SanitizedError: "YouKnowMe upload did not return a durable result"}, nil
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%s", response.UploadID, operation.ComputedDigest)))
	result := "sha256:" + hex.EncodeToString(sum[:])
	now := time.Now().UTC()
	if executor.Now != nil {
		now = executor.Now()
	}
	if err := executor.Store.RecordContentResult(ctx, operation.ComputedDigest, response.UploadID, result, now); err != nil {
		return workledger.ExecutorResult{}, err
	}
	return workledger.ExecutorResult{Outcome: workledger.OutcomeCompleted, ExternalCorrelation: response.UploadID, ResultDigest: result}, nil
}
