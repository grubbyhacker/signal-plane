package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/nats-io/nats-server/v2/server"
)

const (
	recoveryStream  = "RECOVERY_PROOF"
	recoverySubject = "signals.github.recovery"
)

func TestDispatcherRecoveryProof(t *testing.T) {
	t.Run("reset replay reconcile evidence and startup gate", proveRecovery)
	t.Run("status failures remain incomplete and block startup", proveRecoveryFailure)
}

func proveRecovery(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_900_000_000, 0).UTC()
	bus := recoveryBus(t)
	for _, signal := range []envelope.Signal{
		recoverySignal("old-1", 1), recoverySignal("old-2", 2),
		recoverySignal("replay-3", 3),
		{Meta: envelope.Meta{Source: "github", SourceEvent: "ping", SourceDeliveryID: "replay-4"}},
	} {
		if err := bus.Publish(recoverySubject, signal); err != nil {
			t.Fatal(err)
		}
	}
	// Prove reset semantics by first creating the durable at the wrong start.
	if _, err := bus.NewConsumer(eventbus.ConsumerConfig{Subject: recoverySubject, Durable: "recovery-proof", AckWait: time.Second, MaxAckPending: 1, MaxDeliver: 3, StartSequence: 1}); err != nil {
		t.Fatal(err)
	}
	store := recoveryStoreWithLaunchedJob(t, now)
	broker := &statusSequence{statuses: map[string]dispatcher.RunStatus{"restored-run": {RunID: "restored-run", Status: "completed"}}}
	runner := Runner{Store: store, Broker: broker, Stream: NATSStream{Bus: bus}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }, Timeout: time.Second}
	report, err := runner.Run(ctx, Options{RecoveryID: "restore-20260713", Durable: "recovery-proof", Subject: recoverySubject, ManifestSequence: 2, Execute: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != dispatcher.RecoveryCompleted || report.StartSequence != 3 || report.ReplayCount != 2 || report.RestoredNonterminalJobs != 1 || len(report.Reconciliations) != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Reconciliations[0].BrokerStatus != "completed" || report.Reconciliations[0].ReconciledStatus != dispatcher.StateCompleted {
		t.Fatalf("unexpected reconciliation: %+v", report.Reconciliations[0])
	}
	if broker.calls != 1 {
		t.Fatalf("status calls=%d want=1", broker.calls)
	}
	deliveries, jobs, err := store.Counts(ctx)
	if err != nil || deliveries != 3 || jobs != 2 {
		t.Fatalf("post-recovery counts deliveries=%d jobs=%d err=%v", deliveries, jobs, err)
	}
	if err := store.AssertRecoveryComplete(ctx, "recovery-proof", 3); err != nil {
		t.Fatalf("completed recovery gate: %v", err)
	}
	if err := store.AssertRecoveryComplete(ctx, "wrong-durable", 3); err == nil {
		t.Fatal("startup accepted recovery evidence for the wrong durable")
	}
}

func proveRecoveryFailure(t *testing.T) {
	for _, test := range []struct {
		name   string
		result dispatcher.RunStatus
		err    error
	}{
		{name: "auth", err: dispatcher.BrokerError{Status: 401, Message: "unauthorized"}},
		{name: "malformed", result: dispatcher.RunStatus{RunID: "restored-run", Status: "invented"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(1_900_000_000, 0).UTC()
			store := recoveryStoreWithLaunchedJob(t, now)
			bus := recoveryBus(t)
			broker := &statusSequence{statuses: map[string]dispatcher.RunStatus{"restored-run": test.result}, err: test.err}
			runner := Runner{Store: store, Broker: broker, Stream: NATSStream{Bus: bus}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }, Timeout: time.Second}
			_, err := runner.Run(context.Background(), Options{RecoveryID: "failed-restore", Durable: "failed-recovery", Subject: recoverySubject, ManifestSequence: 2, Execute: true})
			if err == nil {
				t.Fatal("recovery unexpectedly succeeded")
			}
			if gateErr := store.AssertRecoveryComplete(context.Background(), "failed-recovery", 3); gateErr == nil {
				t.Fatal("normal startup bypassed incomplete recovery")
			}
			evidence, outcomes, evidenceErr := store.RecoveryEvidence(context.Background(), "failed-restore")
			if evidenceErr != nil || evidence.Status != dispatcher.RecoveryIncomplete || evidence.Error == "" || len(outcomes) != 0 {
				t.Fatalf("failure evidence=%+v outcomes=%v err=%v", evidence, outcomes, evidenceErr)
			}
		})
	}
}

type statusSequence struct {
	statuses map[string]dispatcher.RunStatus
	err      error
	calls    int
}

func (s *statusSequence) Status(_ context.Context, runID string) (dispatcher.RunStatus, error) {
	s.calls++
	if s.err != nil {
		return dispatcher.RunStatus{}, s.err
	}
	result, ok := s.statuses[runID]
	if !ok {
		return dispatcher.RunStatus{}, errors.New("unknown fake run")
	}
	return result, nil
}

func recoveryStoreWithLaunchedJob(t *testing.T, now time.Time) *dispatcher.Store {
	t.Helper()
	store, err := dispatcher.OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	candidate := dispatcher.Candidate{Repository: dispatcher.Repository, IssueNumber: 42, DeliveryID: "restored-delivery"}
	if err := store.Record(context.Background(), candidate.DeliveryID, "accepted", 2, &candidate, now); err != nil {
		t.Fatal(err)
	}
	work, ok, err := store.ClaimDue(context.Background(), now)
	if err != nil || !ok {
		t.Fatalf("claim restored job ok=%v err=%v", ok, err)
	}
	if err := store.MarkLaunched(context.Background(), work.Job.ID, "restored-run", now, now); err != nil {
		t.Fatal(err)
	}
	return store
}

func recoverySignal(delivery string, issue int64) envelope.Signal {
	payload, _ := json.Marshal(map[string]any{
		"action": "labeled", "repository": map[string]any{"full_name": dispatcher.Repository},
		"issue": map[string]any{"number": issue, "state": "open"},
		"label": map[string]any{"name": "agent:implement"}, "sender": map[string]any{"login": "proof"},
	})
	return envelope.Signal{Meta: envelope.Meta{Source: "github", SourceEvent: "issues", SourceAction: "labeled", SourceDeliveryID: delivery}, Payload: payload}
}

func recoveryBus(t *testing.T) *eventbus.Bus {
	t.Helper()
	natsServer, err := server.NewServer(&server.Options{JetStream: true, StoreDir: t.TempDir(), Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	natsServer.Start()
	if !natsServer.ReadyForConnections(5 * time.Second) {
		t.Fatal("in-process NATS did not become ready")
	}
	t.Cleanup(natsServer.Shutdown)
	bus, err := eventbus.Connect(natsServer.ClientURL(), recoveryStream, []string{recoverySubject})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(bus.Close)
	return bus
}
