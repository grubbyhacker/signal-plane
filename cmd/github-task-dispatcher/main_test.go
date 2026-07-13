package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/grubbyhacker/signal-plane/internal/dispatcher"
)

func TestRecoveryMetadataCommandEmptyDatabase(t *testing.T) {
	var output bytes.Buffer
	path := filepath.Join(t.TempDir(), "dispatcher.db")
	store, err := dispatcher.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := runRecoveryMetadata([]string{"--database", path}, &output); err != nil {
		t.Fatal(err)
	}
	var got struct {
		SchemaVersion int    `json:"schema_version"`
		Checkpoint    uint64 `json:"last_persisted_jetstream_sequence"`
		StartSequence uint64 `json:"recovery_start_sequence"`
	}
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != dispatcher.SchemaVersion || got.Checkpoint != 0 || got.StartSequence != 1 {
		t.Fatalf("metadata=%+v", got)
	}
}

func TestRecoveryMetadataCommandRequiresDatabase(t *testing.T) {
	if err := runRecoveryMetadata(nil, &bytes.Buffer{}); err == nil {
		t.Fatal("expected usage error")
	}
}
