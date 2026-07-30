package dispatcher

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func terminalJob() Job {
	return Job{ID: 5, Profile: Profile, Repository: Repository, BrokerRunID: "run-5"}
}
func terminalProjection(outcome string) TerminalResult {
	r := TerminalResult{Version: terminalResultVersion, RunID: "run-5", Profile: Profile, Repo: Repository, Branch: "agent/run-5", Status: "completed", Outcome: outcome, FinalSummary: "Worker final summary."}
	result := map[string]any{"outcome": outcome, "verification": "passed"}
	if outcome == "ready_for_review" {
		result["pull_request"] = map[string]any{"number": 48, "url": "https://github.com/example/automation-target/pull/48"}
	}
	if outcome != "no_change_required" && outcome != "ready_for_review" {
		result["verification"] = "not_run"
		r.Status = outcome
	}
	r.Result, _ = json.Marshal(result)
	return r
}

func TestRenderTerminalCommentDeterministic(t *testing.T) {
	want := map[string]string{
		"no_change_required": "## Repository task terminal result\n\n- Outcome: `no_change_required`\n- Semantic job: `5`\n- Broker run: `run-5`\n- Branch: `agent/run-5`\n- Verification: `passed`\n\n---\n\nWorker final summary.\n",
		"ready_for_review":   "## Repository task terminal result\n\n- Outcome: `ready_for_review`\n- Semantic job: `5`\n- Broker run: `run-5`\n- Branch: `agent/run-5`\n- Verification: `passed`\n- Ready-for-review PR: [#48](https://github.com/example/automation-target/pull/48)\n\n---\n\nWorker final summary.\n",
	}
	for _, outcome := range []string{"no_change_required", "ready_for_review", "failed", "timed_out", "stopped", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			got := RenderTerminalComment(terminalJob(), terminalProjection(outcome))
			if expected, ok := want[outcome]; ok {
				if got != expected {
					t.Fatalf("byte output mismatch\nwant %q\ngot  %q", expected, got)
				}
			} else if !strings.Contains(got, "- Outcome: `"+outcome+"`") || !strings.Contains(got, "- Branch: `agent/run-5`") {
				t.Fatalf("missing required headers: %q", got)
			}
		})
	}
}

func TestHarnessFallbackAndCommentLimit(t *testing.T) {
	job := terminalJob()
	for _, reason := range []string{"result.json is absent", "result.json is not a JSON object", "result.json exceeds terminal_result_byte_limit"} {
		r := HarnessTerminalResult(job, "failed", reason)
		if err := ValidateTerminalResult(job, r); err == nil {
			t.Fatal("harness fallback must remain distinct from worker projection")
		}
		if !strings.Contains(RenderTerminalComment(job, r), reason) {
			t.Fatal("fallback reason missing")
		}
	}
	s, err := OpenStore(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.Exec(`INSERT INTO jobs(id,semantic_key,launch_profile,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,created_at,updated_at) VALUES(5,'k',?,?,1,'d','run-5','launched',0,0,0)`, Profile, Repository); err != nil {
		t.Fatal(err)
	}
	r := terminalProjection("no_change_required")
	r.FinalSummary = strings.Repeat("x", maxTerminalBytes)
	if err := s.QueueTerminalResult(context.Background(), job, r, time.Unix(1, 0)); err == nil {
		t.Fatal("whole comment limit was not enforced")
	}
}

func TestQueueTerminalResultRejectsConflictingImmutableProjection(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	job := terminalJob()
	if _, err = s.db.Exec(`INSERT INTO jobs(id,semantic_key,launch_profile,repository,issue_number,source_delivery_id,broker_run_id,status,due_at,created_at,updated_at) VALUES(5,'k',?,?,1,'d','run-5','launched',0,0,0)`, Profile, Repository); err != nil {
		t.Fatal(err)
	}
	r := terminalProjection("no_change_required")
	if err = s.QueueTerminalResult(ctx, job, r, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	r.FinalSummary = "different"
	if err = s.QueueTerminalResult(ctx, job, r, time.Unix(2, 0)); err == nil {
		t.Fatal("conflicting result accepted")
	}
}
