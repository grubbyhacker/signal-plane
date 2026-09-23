package workitemlaunch

import (
	"context"
	"errors"
	"testing"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

type fakeBroker struct {
	profile, workItemID, uploadID, key string
	result                             dispatcher.LaunchResult
	err                                error
}

func (broker *fakeBroker) LaunchWorkItem(_ context.Context, profile, workItemID, uploadID, key string) (dispatcher.LaunchResult, error) {
	broker.profile, broker.workItemID, broker.uploadID, broker.key = profile, workItemID, uploadID, key
	return broker.result, broker.err
}

type retryError struct{ retryable bool }

func (err retryError) Error() string   { return "broker failed" }
func (err retryError) Retryable() bool { return err.retryable }

func testExecutor(broker Broker) *Executor {
	return &Executor{Config: config.WorkItemLauncherConfig{
		Enabled: true, AgentType: "youknowme-curator", ExecutorID: "youknowme.curator",
		ExecutorKind: "deterministic_tool", ExecutorVersion: "v1",
		Profiles: map[string]string{"process_intake": "ykm-curator-workitem-intake"},
	}, Broker: broker}
}

func testRequest() workledger.ExecutorRequest {
	return workledger.ExecutorRequest{
		WorkItem: workledger.WorkItem{ID: "work-123", AgentType: "youknowme-curator", AgentMode: "process_intake", ObjectKind: "upload", ObjectID: "upl_123"},
		Attempt:  workledger.ExecutorAttempt{IdempotencyKey: "executor:work-123:digest:1"},
	}
}

func TestExecutorLaunchesAuthoritativeWorkItem(t *testing.T) {
	broker := &fakeBroker{result: dispatcher.LaunchResult{Version: "broker-run-launch/v1", RunID: "run-123"}}
	result, err := testExecutor(broker).Execute(context.Background(), testRequest())
	if err != nil {
		t.Fatal(err)
	}
	if broker.profile != "ykm-curator-workitem-intake" || broker.workItemID != "work-123" || broker.uploadID != "upl_123" || broker.key != "executor:work-123:digest:1" {
		t.Fatalf("launch coordinates = %#v", broker)
	}
	if result.Outcome != workledger.OutcomeCompleted || result.ExternalCorrelation != "run-123" || result.ResultDigest == "" {
		t.Fatalf("result = %#v", result)
	}
}

func TestExecutorClassifiesBrokerFailuresAndRejectsUnmappedModes(t *testing.T) {
	for name, test := range map[string]struct {
		err  error
		want workledger.ExecutorOutcome
	}{
		"retryable": {retryError{retryable: true}, workledger.OutcomeRetryableFailure},
		"permanent": {retryError{retryable: false}, workledger.OutcomePermanentFailure},
	} {
		t.Run(name, func(t *testing.T) {
			result, err := testExecutor(&fakeBroker{err: test.err}).Execute(context.Background(), testRequest())
			if err != nil || result.Outcome != test.want {
				t.Fatalf("result=%#v err=%v", result, err)
			}
		})
	}
	request := testRequest()
	request.WorkItem.AgentMode = "reconcile"
	result, err := testExecutor(&fakeBroker{err: errors.New("must not be called")}).Execute(context.Background(), request)
	if err != nil || result.Outcome != workledger.OutcomePermanentFailure {
		t.Fatalf("unmapped result=%#v err=%v", result, err)
	}
}
