package dispatcher

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/envelope"
)

type repositoryWaitBroker struct {
	status      RunStatus
	resumeCalls int
	resumeRun   string
	resumeKey   string
	resumeLimit int
}

func (b *repositoryWaitBroker) Launch(context.Context, Job) (LaunchResult, error) {
	return LaunchResult{}, nil
}

func (b *repositoryWaitBroker) Status(context.Context, string) (RunStatus, error) {
	return b.status, nil
}

func (b *repositoryWaitBroker) ResumeRun(_ context.Context, runID, key string, seconds int) (RunStatus, error) {
	b.resumeCalls++
	b.resumeRun, b.resumeKey, b.resumeLimit = runID, key, seconds
	return RunStatus{RunID: runID, Status: "running"}, nil
}

func TestRepositoryRunExternalWaitRequiresMatchingAuthenticatedIssueWake(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(100_000, 0).UTC()
	store, err := OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.ConfigureCIRepair(CIRepairPolicy{ReconciliationWake: 24 * time.Hour, ActiveTimeout: time.Hour, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	job := terminalTestJob(t, store, now)
	wait := CIExternalWait{Service: "github", Phase: "preparation", Operation: "issue.read", Reason: "unavailable", Generation: 3, Since: now}
	broker := &repositoryWaitBroker{status: RunStatus{RunID: job.BrokerRunID, Status: "waiting_external", ExternalWait: &wait}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if worked, err := runStatus(ctx, logger, NewMetrics(), store, broker, job, now); err != nil || !worked {
		t.Fatalf("record wait worked=%v err=%v", worked, err)
	}
	var state string
	var due int64
	if err := store.db.QueryRow(`SELECT status,due_at FROM jobs WHERE id=?`, job.ID).Scan(&state, &due); err != nil {
		t.Fatal(err)
	}
	if state != StateExternalWaiting || due != now.Add(24*time.Hour).UnixMilli() || broker.resumeCalls != 0 {
		t.Fatalf("state=%q due=%d resume_calls=%d", state, due, broker.resumeCalls)
	}

	base := envelope.Signal{Meta: envelope.Meta{Source: "github", Namespace: job.Repository, ObjectKind: "issue", ObjectID: "25", Authentication: envelope.Authentication{Verified: true}}}
	unauthenticated := base
	unauthenticated.Meta.Authentication.Verified = false
	wrongIssue := base
	wrongIssue.Meta.ObjectID = "26"
	for _, signal := range []envelope.Signal{unauthenticated, wrongIssue} {
		if woke, err := store.RecordRepositoryRunExternalWake(ctx, signal, now.Add(time.Minute)); err != nil || woke {
			t.Fatalf("unrelated wake=%v err=%v", woke, err)
		}
	}
	if woke, err := store.RecordRepositoryRunExternalWake(ctx, base, now.Add(2*time.Minute)); err != nil || !woke {
		t.Fatalf("matching wake=%v err=%v", woke, err)
	}

	job.Status = StateExternalWaiting
	if worked, err := runStatus(ctx, logger, NewMetrics(), store, broker, job, now.Add(2*time.Minute)); err != nil || !worked {
		t.Fatalf("resume worked=%v err=%v", worked, err)
	}
	wantKey := repositoryRunResumeKey(job, wait.Generation)
	if broker.resumeCalls != 1 || broker.resumeRun != job.BrokerRunID || broker.resumeKey != wantKey || broker.resumeLimit != 3600 {
		t.Fatalf("resume calls=%d run=%q key=%q limit=%d", broker.resumeCalls, broker.resumeRun, broker.resumeKey, broker.resumeLimit)
	}
	var waitState string
	if err := store.db.QueryRow(`SELECT status FROM jobs WHERE id=?`, job.ID).Scan(&state); err != nil || state != StateLaunched {
		t.Fatalf("job state=%q err=%v", state, err)
	}
	if err := store.db.QueryRow(`SELECT state FROM repository_run_external_waits WHERE job_id=?`, job.ID).Scan(&waitState); err != nil || waitState != "resumed" {
		t.Fatalf("wait state=%q err=%v", waitState, err)
	}
}

func TestRepositoryRunExternalWaitStatusAdvanceReconcilesLostResumeResponse(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(110_000, 0).UTC()
	store, err := OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.ConfigureCIRepair(CIRepairPolicy{ActiveTimeout: time.Hour, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	job := terminalTestJob(t, store, now)
	wait := CIExternalWait{Service: "github", Phase: "delivery", Operation: "git.push", Reason: "rate_limited", Generation: 1, Since: now}
	if _, err := store.RecordRepositoryRunExternalWait(ctx, job, wait, now); err != nil {
		t.Fatal(err)
	}
	signal := envelope.Signal{Meta: envelope.Meta{Source: "github", Namespace: job.Repository, ObjectKind: "issue", ObjectID: "25", Authentication: envelope.Authentication{Verified: true}}}
	if _, err := store.RecordRepositoryRunExternalWake(ctx, signal, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	job.Status = StateExternalWaiting
	broker := &repositoryWaitBroker{status: RunStatus{RunID: job.BrokerRunID, Status: "running"}}
	if _, err := runStatus(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), NewMetrics(), store, broker, job, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	var waitState string
	if err := store.db.QueryRow(`SELECT state FROM repository_run_external_waits WHERE job_id=?`, job.ID).Scan(&waitState); err != nil || waitState != "resumed" {
		t.Fatalf("wait state=%q err=%v", waitState, err)
	}
}

func TestRecoveryReconciliationPersistsBrokerExternalWait(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(120_000, 0).UTC()
	store, err := OpenStore(filepath.Join(t.TempDir(), "dispatcher.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.ConfigureCIRepair(CIRepairPolicy{ReconciliationWake: 7 * 24 * time.Hour, ActiveTimeout: 45 * time.Minute, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	job := terminalTestJob(t, store, now)
	wait := CIExternalWait{Service: "github", Phase: "preparation", Operation: "issue_comments.read", Reason: "rate_limited", Generation: 2, Since: now}
	state, err := ReconcileStatusResult(ctx, store, nil, job, RunStatus{RunID: job.BrokerRunID, Status: "waiting_external", ExternalWait: &wait}, now)
	if err != nil || state != StateExternalWaiting {
		t.Fatalf("state=%q err=%v", state, err)
	}
	var jobState string
	var maxRuntime int
	if err := store.db.QueryRow(`SELECT j.status,w.max_runtime_seconds FROM jobs j JOIN repository_run_external_waits w ON w.job_id=j.id WHERE j.id=?`, job.ID).Scan(&jobState, &maxRuntime); err != nil {
		t.Fatal(err)
	}
	if jobState != StateExternalWaiting || maxRuntime != 2700 {
		t.Fatalf("job state=%q max runtime=%d", jobState, maxRuntime)
	}
}
