package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/agentsession"
	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

func TestFixtureRouteRegistersExactDisabledCoordinatorContract(t *testing.T) {
	store, err := workledger.Open(filepath.Join(t.TempDir(), "coordinator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := workledger.NewRegistry()
	if err := registry.Register(&agentsession.Executor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterTask(agentsession.GitHubGreenPRTask{}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.ActivateRoute(context.Background(), fixtureRoute(), registry, time.Now().UTC())
	if err != nil || snapshot.TaskKind != agentsession.GitHubGreenPRTaskKind || snapshot.ExecutorID != agentsession.ExecutorID {
		t.Fatalf("route=%+v err=%v", snapshot, err)
	}
}
