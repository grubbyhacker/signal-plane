package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/config"
)

func TestFixtureAdmissionCommandUsesConfiguredCoordinatorDatabase(t *testing.T) {
	cfg := config.Config{Coordinator: config.CoordinatorConfig{Enabled: true, DatabasePath: filepath.Join(t.TempDir(), "fixture.db")}}
	now := time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)
	first, err := admitFixture(context.Background(), cfg, now)
	if err != nil || first.Duplicate || first.WorkItem.ID == "" {
		t.Fatalf("first admission=%+v err=%v", first, err)
	}
	second, err := admitFixture(context.Background(), cfg, now.Add(time.Minute))
	if err != nil || !second.Duplicate || second.WorkItem.ID != first.WorkItem.ID {
		t.Fatalf("second admission=%+v err=%v", second, err)
	}
}
