// Package routeactivatecmd is the managed, idempotent CLI wiring that installs
// a deployment-owned route snapshot into the work ledger via the existing
// Store.ActivateRoute API and emits the STORE-MINTED route_snapshot_id for the
// resolver table.
//
// It is deliberately narrow:
//
//   - Inputs are a deployment-owned full RouteDefinition (a JSON file) and the
//     Executor descriptor (id/kind/version) that ActivateRoute requires. The
//     route_snapshot_id is NEVER a caller input — it is minted by the store and
//     reported back, which is exactly the id the resolver's RouteEntry
//     references.
//   - Reruns are idempotent: activating the same definition + executor returns
//     the existing active snapshot id (saveRoute de-dups on route_id+digest),
//     creating no new generation.
//   - A DIFFERING definition for the same route_id FAILS CLOSED. saveRoute would
//     silently retire the active snapshot and insert a new one; that implicit
//     supersede is refused unless the operator explicitly opts in with
//     AllowSupersede, which is the explicit, safe activation transition.
//   - It holds no launcher, broker, or dispatcher: it activates a route and
//     stops. It never claims work, resolves a release, or opens a listener.
//   - It never fabricates a database row: the only write path is
//     Store.ActivateRoute; conflict detection is a read via
//     Store.ActiveRouteSnapshot.
package routeactivatecmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/grubbyhacker/signal-plane/internal/workledger"
)

// Options are the deployment-owned activation inputs. The route definition is
// supplied as raw JSON bytes (read from a file by the command entrypoint) and
// the executor descriptor names an existing deterministic/policy executor. No
// field carries a snapshot id — the store mints it.
type Options struct {
	// DatabasePath is the work-ledger SQLite path to activate into.
	DatabasePath string
	// RouteDefinitionJSON is the deployment-owned full RouteDefinition, decoded
	// with DisallowUnknownFields so a caller cannot smuggle an unknown field.
	RouteDefinitionJSON []byte
	// Executor is the descriptor ActivateRoute requires. It is registered as an
	// inert descriptor-only executor: activation records the descriptor and
	// never executes it (the shadow stack does not run executors).
	Executor workledger.ExecutorDescriptor
	// AllowSupersede permits the explicit, safe activation transition when an
	// active snapshot for the same route_id exists with a DIFFERENT digest.
	// Without it, a differing definition fails closed.
	AllowSupersede bool
}

// Outcome reports the store-minted snapshot id and whether this activation
// created a new generation or returned an already-active one.
type Outcome struct {
	// RouteSnapshotID is the STORE-MINTED id for the resolver table. Never a
	// caller input.
	RouteSnapshotID string
	// RouteID is the deployment-owned route id from the definition.
	RouteID string
	// Digest is the activation content digest the snapshot is keyed on.
	Digest string
	// AlreadyActive is true when the exact definition+executor was already the
	// active snapshot — the rerun-safe path, no new generation.
	AlreadyActive bool
	// Superseded is true when an explicitly-allowed transition retired a
	// differing active snapshot and activated this one.
	Superseded bool
	// PreviousRouteSnapshotID is the retired snapshot id when Superseded.
	PreviousRouteSnapshotID string
}

// descriptorExecutor is an inert Executor: it exposes a descriptor so
// ActivateRoute can resolve the registry, and refuses to run. Activation only
// records the descriptor; the shadow stack never executes routes, so an
// executed descriptorExecutor would be a bug, and it says so.
type descriptorExecutor struct {
	descriptor workledger.ExecutorDescriptor
}

func (e descriptorExecutor) Descriptor() workledger.ExecutorDescriptor { return e.descriptor }

func (e descriptorExecutor) Execute(context.Context, workledger.ExecutorRequest) (workledger.ExecutorResult, error) {
	return workledger.ExecutorResult{}, errors.New("route-activate executor is descriptor-only and must never run")
}

// Run performs the managed idempotent activation and returns the outcome. It
// opens the ledger, decodes the definition, pre-flights conflict against the
// active snapshot, then calls Store.ActivateRoute. It writes nothing else.
//
// Run is retained for tooling that legitimately owns its own short-lived ledger
// handle (tests and one-shot fixtures). The production single-writer path is
// RunWithStore, driven by the dispatcher against its own handle — the
// operational database must be opened by exactly one process.
func Run(ctx context.Context, opts Options, now time.Time) (Outcome, error) {
	if opts.DatabasePath == "" {
		return Outcome{}, errors.New("route activation requires a work-ledger database_path")
	}
	store, err := workledger.Open(opts.DatabasePath)
	if err != nil {
		return Outcome{}, fmt.Errorf("open work ledger: %w", err)
	}
	defer store.Close()
	return RunWithStore(ctx, store, opts, now)
}

// RunWithStore performs the managed idempotent activation against a CALLER-OWNED
// work-ledger store, opening nothing. It is the single-writer activation seam:
// the dispatcher, the sole owner of the operational database, calls this with
// its own attached handle at startup so route activation writes through the one
// connection instead of a second process. DatabasePath on opts is ignored here;
// the store the caller passes IS the target.
//
// It decodes the definition, pre-flights conflict against the active snapshot,
// then calls Store.ActivateRoute. It writes nothing else and never launches.
func RunWithStore(ctx context.Context, store *workledger.Store, opts Options, now time.Time) (Outcome, error) {
	if store == nil {
		return Outcome{}, errors.New("route activation requires a work-ledger store")
	}
	definition, err := workledger.DecodeRouteDefinition(opts.RouteDefinitionJSON)
	if err != nil {
		return Outcome{}, fmt.Errorf("decode route definition: %w", err)
	}
	if err := opts.Executor.Validate(); err != nil {
		return Outcome{}, fmt.Errorf("executor descriptor: %w", err)
	}
	if definition.ExecutorID != opts.Executor.ID {
		return Outcome{}, fmt.Errorf("route definition executor_id %q does not match executor descriptor id %q", definition.ExecutorID, opts.Executor.ID)
	}

	// The activation content digest is computed from the SAME inputs saveRoute
	// keys on, without touching the database. It is how we tell "identical
	// rerun" from "differing definition" before mutating anything.
	wantDigest, err := workledger.ActivationDigest(definition, opts.Executor)
	if err != nil {
		return Outcome{}, fmt.Errorf("compute activation digest: %w", err)
	}

	active, hasActive, err := store.ActiveRouteSnapshot(ctx, definition.ID)
	if err != nil {
		return Outcome{}, fmt.Errorf("read active route snapshot: %w", err)
	}
	if hasActive && active.Digest != wantDigest && !opts.AllowSupersede {
		// Fail closed: a different definition is already active. saveRoute would
		// silently supersede it; refuse unless the transition is explicit.
		return Outcome{}, fmt.Errorf("route %q already has a different active snapshot %q (digest %s); refusing to supersede without an explicit transition", definition.ID, active.ID, active.Digest)
	}

	registry := workledger.NewRegistry()
	if err := registry.Register(descriptorExecutor{descriptor: opts.Executor}); err != nil {
		return Outcome{}, fmt.Errorf("register executor descriptor: %w", err)
	}

	snapshot, err := store.ActivateRoute(ctx, definition, registry, now)
	if err != nil {
		return Outcome{}, fmt.Errorf("activate route: %w", err)
	}

	outcome := Outcome{
		RouteSnapshotID: snapshot.ID,
		RouteID:         snapshot.RouteID,
		Digest:          snapshot.Digest,
		AlreadyActive:   hasActive && active.Digest == wantDigest,
	}
	if hasActive && active.Digest != wantDigest {
		outcome.Superseded = true
		outcome.PreviousRouteSnapshotID = active.ID
	}
	return outcome, nil
}

// WriteOutcome renders the outcome as a stable, greppable one-line-per-field
// report. The route_snapshot_id line is what a deployment copies into the
// resolver's RouteEntry.
func WriteOutcome(w io.Writer, outcome Outcome) {
	fmt.Fprintf(w, "route_snapshot_id=%s\n", outcome.RouteSnapshotID)
	fmt.Fprintf(w, "route_id=%s\n", outcome.RouteID)
	fmt.Fprintf(w, "digest=%s\n", outcome.Digest)
	fmt.Fprintf(w, "already_active=%t\n", outcome.AlreadyActive)
	fmt.Fprintf(w, "superseded=%t\n", outcome.Superseded)
	if outcome.Superseded {
		fmt.Fprintf(w, "previous_route_snapshot_id=%s\n", outcome.PreviousRouteSnapshotID)
	}
}
