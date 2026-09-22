package workitemlaunch

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// Broker is the authenticated control-plane launch seam. The concrete
// dispatcher.Broker supplies the deployment-owned origin and bearer token.
type Broker interface {
	LaunchWorkItem(ctx context.Context, profile, workItemID, idempotencyKey string) (dispatcher.LaunchResult, error)
}

// Executor launches agent-bound WorkItems through deployment-owned profile
// mappings. The WorkItem supplies only its authoritative identity and resolved
// mode; emitters cannot select a broker profile or runtime.
type Executor struct {
	Config config.WorkItemLauncherConfig
	Broker Broker
}

func (executor *Executor) Descriptor() workledger.ExecutorDescriptor {
	kind := executor.Config.ExecutorKind
	if kind == "" {
		kind = string(workledger.ExecutorDeterministicTool)
	}
	return workledger.ExecutorDescriptor{
		ID: executor.Config.ExecutorID, Kind: workledger.ExecutorKind(kind), Version: executor.Config.ExecutorVersion,
	}
}

func (executor *Executor) Execute(ctx context.Context, request workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	if executor.Broker == nil {
		return permanent("launcher broker is unavailable"), nil
	}
	item := request.WorkItem
	if item.AgentType != executor.Config.AgentType {
		return permanent("work item agent type is not authorized by this launcher"), nil
	}
	profile := executor.Config.Profiles[item.AgentMode]
	if profile == "" {
		return permanent("work item mode has no deployment-owned launch profile"), nil
	}
	result, err := executor.Broker.LaunchWorkItem(ctx, profile, item.ID, request.Attempt.IdempotencyKey)
	if err != nil {
		if retryable, ok := err.(interface{ Retryable() bool }); ok && retryable.Retryable() {
			return workledger.ExecutorResult{
				Outcome: workledger.OutcomeRetryableFailure, RetryClassification: "broker_launch_transient",
				SanitizedError: "broker launch is temporarily unavailable",
			}, nil
		}
		return permanent("broker launch was denied"), nil
	}
	digest := sha256.Sum256([]byte(result.RunID))
	return workledger.ExecutorResult{
		Outcome: workledger.OutcomeCompleted, ExternalCorrelation: result.RunID,
		ResultDigest: fmt.Sprintf("sha256:%x", digest),
	}, nil
}

func permanent(message string) workledger.ExecutorResult {
	return workledger.ExecutorResult{Outcome: workledger.OutcomePermanentFailure, SanitizedError: message}
}
