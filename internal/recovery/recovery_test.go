package recovery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
	"github.com/grubbyhacker/signal-plane/internal/envelope"
	"github.com/grubbyhacker/signal-plane/internal/eventbus"
	"github.com/nats-io/nats-server/v2/server"
	_ "modernc.org/sqlite"
)

const (
	recoveryStream  = "RECOVERY_PROOF"
	recoverySubject = "signals.github.recovery"
	recoveryRepo    = "example/automation-target"
)

func recoveryRoutes() []config.RepositoryTaskRoute {
	return []config.RepositoryTaskRoute{{
		ID: "fixture", Repository: recoveryRepo, Event: "issues",
		Action: "labeled", Label: "automation:requested", Profile: "repository-task",
	}}
}

func TestDispatcherRecoveryProof(t *testing.T) {
	t.Run("reset replay reconcile evidence and startup gate", proveRecovery)
	t.Run("permanent status failures become reportable recovery evidence", proveRecoveryFailure)
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
	broker := recoveryBroker("completed")
	runner := Runner{Store: store, Broker: broker, Stream: NATSStream{Bus: bus}, Routes: recoveryRoutes(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }, Timeout: time.Second}
	report, err := runner.Run(ctx, Options{RecoveryID: "restore-20260713", Durable: "recovery-proof", Subject: recoverySubject, ManifestSequence: 2, Execute: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != dispatcher.RecoveryCompleted || report.StartSequence != 3 || report.ReplayCount != 2 || report.RestoredNonterminalJobs != 1 || len(report.Reconciliations) != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Reconciliations[0].BrokerStatus != "completed" || report.Reconciliations[0].ReconciledStatus != dispatcher.StateReportPending {
		t.Fatalf("unexpected reconciliation: %+v", report.Reconciliations[0])
	}
	if broker.calls != 1 || broker.terminalCalls != 1 {
		t.Fatalf("status calls=%d terminal calls=%d want=1 each", broker.calls, broker.terminalCalls)
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
			broker := recoveryBroker(test.result.Status)
			broker.err = test.err
			runner := Runner{Store: store, Broker: broker, Stream: NATSStream{Bus: bus}, Routes: recoveryRoutes(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }, Timeout: time.Second}
			report, err := runner.Run(context.Background(), Options{RecoveryID: "failed-restore", Durable: "failed-recovery", Subject: recoverySubject, ManifestSequence: 2, Execute: true})
			if err != nil || report.Status != dispatcher.RecoveryCompleted {
				t.Fatalf("recovery report=%+v err=%v", report, err)
			}
			if gateErr := store.AssertRecoveryComplete(context.Background(), "failed-recovery", 3); gateErr != nil {
				t.Fatalf("reportable failure did not complete recovery: %v", gateErr)
			}
			evidence, outcomes, evidenceErr := store.RecoveryEvidence(context.Background(), "failed-restore")
			if evidenceErr != nil || evidence.Status != dispatcher.RecoveryCompleted || evidence.Error != "" || len(outcomes) != 1 || outcomes[0].ReconciledStatus != dispatcher.StateReportPending {
				t.Fatalf("failure evidence=%+v outcomes=%v err=%v", evidence, outcomes, evidenceErr)
			}
			reportable, ok, claimErr := store.ClaimReportDue(context.Background(), now)
			if claimErr != nil || !ok || reportable.Job.Status != dispatcher.StateReportPending {
				t.Fatalf("reportable failure ok=%v report=%+v err=%v", ok, reportable, claimErr)
			}
		})
	}
}

type statusSequence struct {
	statuses      map[string]dispatcher.RunStatus
	terminals     map[string]dispatcher.TerminalResult
	err           error
	calls         int
	terminalCalls int
}

func (s *statusSequence) TerminalResult(_ context.Context, runID string) (dispatcher.TerminalResult, error) {
	s.terminalCalls++
	result, ok := s.terminals[runID]
	if !ok {
		return dispatcher.TerminalResult{}, errors.New("unknown fake terminal result")
	}
	return result, nil
}

func recoveryBroker(status string) *statusSequence {
	return &statusSequence{
		statuses: map[string]dispatcher.RunStatus{"restored-run": {RunID: "restored-run", Status: status}},
		terminals: map[string]dispatcher.TerminalResult{"restored-run": {
			Version: "repository-task-terminal-result/v1", RunID: "restored-run",
			Profile: "repository-task", Repo: recoveryRepo, Status: "completed",
			Outcome: "no_change_required", FinalizeReason: "worker_exit", TerminalSource: "exited",
			IdempotencyKeyDigest: "digest", RequestFingerprint: "fingerprint",
			LaunchConfigVersion: "route-config-v1", FinalSummary: "complete",
			Result: map[string]any{
				"version": "repository-task-worker-result/v1", "outcome": "no_change_required",
				"detail": "complete", "stage": "completed", "run_id": "restored-run",
				"repository": recoveryRepo, "base_branch": "main", "branch": "",
				"verify_task": "verify", "verification": map[string]any{"status": "passed"},
			},
		}},
	}
}

func TestRestoredTerminalProjectionIsExactlyOnceAcrossRestart(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1_900_000_000, 0).UTC()
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, job := recoveryStoreAtPath(t, path, now)
	broker := recoveryBroker("completed")
	status := broker.statuses[job.BrokerRunID]
	if state, err := dispatcher.ReconcileStatusResult(ctx, store, broker, job, status, now); err != nil || state != dispatcher.StateReportPending {
		t.Fatalf("first projection state=%q err=%v", state, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := dispatcher.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if state, err := dispatcher.ReconcileStatusResult(ctx, store, broker, job, status, now.Add(time.Second)); err != nil || state != dispatcher.StateReportPending {
		t.Fatalf("replayed projection state=%q err=%v", state, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var terminalRows, outboxRows int
	if err := db.QueryRow(`SELECT count(*) FROM terminal_results`).Scan(&terminalRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM notification_outbox`).Scan(&outboxRows); err != nil {
		t.Fatal(err)
	}
	if terminalRows != 1 || outboxRows != 1 {
		t.Fatalf("terminal rows=%d outbox rows=%d", terminalRows, outboxRows)
	}
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
	store, _ := recoveryStoreAtPath(t, filepath.Join(t.TempDir(), "dispatcher.db"), now)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func recoveryStoreAtPath(t *testing.T, path string, now time.Time) (*dispatcher.Store, dispatcher.Job) {
	t.Helper()
	store, err := dispatcher.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	candidate := dispatcher.Candidate{Repository: recoveryRepo, IssueNumber: 42, DeliveryID: "restored-delivery", RouteID: "fixture", Profile: "repository-task"}
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
	work.Job.BrokerRunID = "restored-run"
	work.Job.Status = dispatcher.StateLaunched
	return store, work.Job
}

func recoverySignal(delivery string, issue int64) envelope.Signal {
	payload, _ := json.Marshal(map[string]any{
		"action": "labeled", "repository": map[string]any{"full_name": recoveryRepo},
		"issue": map[string]any{"number": issue, "state": "open"},
		"label": map[string]any{"name": "automation:requested"}, "sender": map[string]any{"login": "proof"},
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
