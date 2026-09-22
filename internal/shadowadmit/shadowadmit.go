// Package shadowadmit implements the Stage-5 "shadow admission" seam of the
// agent-platform-coupling design.
//
// The design migrates the launch path by shadow admission FIRST: a
// source-neutral WorkItem candidate is admitted into internal/workledger — it
// dedupes, persists the merged AgentType/mode/contract-revision binding and the
// source identity, and returns a deterministic admission/dedup result — but it
// NEVER launches an agent, never enqueues to the legacy dispatcher launcher,
// and never mutates the legacy dispatcher tables. It is a dry run of admission.
//
// Two invariants hold structurally here and are enforced by an offline
// validator and tests:
//
//   - No image/generation selection at admission. A candidate carries only the
//     deployment-owned (AgentType, mode, contract revision) selection; any
//     broker-resolved field (release generation, digest, broker run, PR
//     correlation) is refused. This reuses workledger.AgentBinding's own guard.
//
//   - One active launcher. Shadow admission is not a launcher: this package
//     imports no dispatcher/broker/launch machinery and never calls the ledger
//     Claim path, so it can never become a second live launcher running beside
//     the legacy one. Cutover to an active launcher is a later stage.
//
// External ingress transport (the host Unix-domain socket, event envelope, and
// route table) is deferred in the design, so this package defines no listener;
// it is the ledger shadow seam a future ingress calls into.
package shadowadmit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// admitter is the narrow slice of *workledger.Store the shadow seam needs. It
// is deliberately admission-only: there is no Claim, no launch, and no
// dispatcher method on this interface, so the shadow admitter cannot start work
// even by accident. (One-active-launcher invariant, expressed as a type.)
type admitter interface {
	AdmitWithAgent(ctx context.Context, snapshotID string, event workledger.Event, binding workledger.AgentBinding, now time.Time) (workledger.AdmissionResult, error)
}

// Candidate is a source-neutral WorkItem admission request. A webhook, a clock
// tick, and a local enqueue all produce the same shape. It carries the
// already-normalized ledger event plus the deployment-owned agent selection the
// route resolved; it cannot express a release, generation, image, broker run,
// or PR correlation.
type Candidate struct {
	// RouteSnapshotID is the active route snapshot the event matched.
	RouteSnapshotID string
	// Event is the source-neutral, already-authenticated ledger event.
	Event workledger.Event
	// Binding is the deployment-owned (AgentType, mode, contract revision)
	// selection. Its zero value is valid and means "no agent bound yet".
	Binding workledger.AgentBinding
}

// Outcome is the deterministic result of a shadow admission. It reports whether
// the candidate was newly admitted or deduped to an existing work item, and
// echoes the persisted agent binding — never a launch, a generation, or any
// broker-resolved selection.
type Outcome struct {
	WorkItemID           string
	EventID              string
	Duplicate            bool
	AgentType            string
	AgentMode            string
	TypeContractRevision string
	// Launched is always false. It exists so callers (and tests) can assert the
	// shadow seam never launches, and so a future active-launcher stage that
	// forgets to set it is caught rather than silently launching.
	Launched bool
}

// Shadow is the disabled-by-default shadow-admission service. When disabled it
// admits nothing and returns ErrDisabled; when enabled it admits into the
// ledger and returns a deterministic outcome, still never launching.
type Shadow struct {
	store   admitter
	enabled bool
	now     func() time.Time
}

// ErrDisabled is returned by Admit when the shadow seam is not enabled. It lets
// a caller wire the seam into a process and keep it inert by configuration.
var ErrDisabled = errors.New("shadow admission is disabled")

// New constructs a Shadow admitter. enabled=false (the default posture) makes
// every Admit call a no-op that returns ErrDisabled.
func New(store admitter, enabled bool) (*Shadow, error) {
	if store == nil {
		return nil, errors.New("shadow admitter requires a work-ledger store")
	}
	return &Shadow{store: store, enabled: enabled, now: time.Now}, nil
}

// Enabled reports whether the seam will admit.
func (shadow *Shadow) Enabled() bool { return shadow.enabled }

// Admit performs a shadow admission: it validates the candidate, refuses any
// image/generation selection, and admits into the ledger. It NEVER launches,
// enqueues, or mutates legacy dispatcher tables. The result is deterministic:
// the same candidate admitted twice returns the same work item with
// Duplicate=true on the second call.
func (shadow *Shadow) Admit(ctx context.Context, candidate Candidate) (Outcome, error) {
	if !shadow.enabled {
		return Outcome{}, ErrDisabled
	}
	if candidate.RouteSnapshotID == "" {
		return Outcome{}, errors.New("shadow admission requires a route snapshot id")
	}
	// Refuse any caller-supplied release/generation/image/broker/PR selection.
	// AgentType/mode/contract are the only admission-safe binding fields.
	if err := candidate.Binding.RejectAdmissionSelection(); err != nil {
		return Outcome{}, fmt.Errorf("shadow admission: %w", err)
	}
	result, err := shadow.store.AdmitWithAgent(ctx, candidate.RouteSnapshotID, candidate.Event, candidate.Binding, shadow.now().UTC())
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{
		WorkItemID:           result.WorkItem.ID,
		EventID:              result.EventID,
		Duplicate:            result.Duplicate,
		AgentType:            result.WorkItem.AgentType,
		AgentMode:            result.WorkItem.AgentMode,
		TypeContractRevision: result.WorkItem.TypeContractRevision,
		Launched:             false,
	}, nil
}
