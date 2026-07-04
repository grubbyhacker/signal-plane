package envelope

import "testing"

func TestNewSignalIDHasSignalPrefix(t *testing.T) {
	id := NewSignalID()
	if len(id) <= len("signal-") {
		t.Fatalf("signal id too short: %q", id)
	}
	if id[:len("signal-")] != "signal-" {
		t.Fatalf("signal id prefix = %q, want signal-", id)
	}
}
