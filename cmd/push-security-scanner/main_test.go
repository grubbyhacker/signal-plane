package main

import (
	"errors"
	"testing"
	"time"
)

func TestScanRetryDelayKeepsTransientFailuresInsideSLO(t *testing.T) {
	if got := scanRetryDelay(errors.New("transient broker failure")); got != 5*time.Second {
		t.Fatalf("transient delay = %s", got)
	}
}
