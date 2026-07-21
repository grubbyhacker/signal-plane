# PR10 coordinator integration seam

This change establishes Signal Plane's credential-independent half of the PR10
coordinator contract. It deliberately does not activate a production route,
choose runtime credentials, or claim a lifecycle proof.

## Broker boundary

`internal/agentsession.HTTPBroker` is the concrete authenticated HTTP adapter
for the broker's coordinator v1 endpoints. Requests use fixed paths and strict
JSON decoding. Session acquisition, creation, turn submission, event streaming,
checkpoint, resume, cancel, status, reassignment, and reassignment-status reads
use only the broker-defined fixed operations and preserve the complete authority
and fencing identity:

- authority profile and profile version
- policy digest
- session lineage
- worker and worker storage lineage
- worker fence epoch

The checked-in files under `testdata/coordinator-wire/` are exact stable wire
fixtures shared with the broker implementation. Adapter tests consume those
fixtures so incompatible wire changes fail locally before a cross-repository
lifecycle attempt.

The schema migration is additive. Bindings created by an older schema retain
their evidence but do not have the new complete identity columns; they fail
closed at routing and must be reacquired rather than being guessed or silently
upgraded.

## Reassignment and routing

Reassignment is a durable saga recorded in SQLite:

1. `requested`
2. `broker_committed`
3. `agentd_adopted`
4. `coordinator_committed`

The broker rebind idempotency key is retained separately from Signal Plane's
request idempotency key. A restart can reconcile the broker status and resume
the saga. Reassignment calls carry the predecessor fence epoch, so replay after
coordinator commit resolves the durable transition instead of deriving a new
generation from the mutable successor binding. Broker conflicts and legacy
unresolved adoptions are persisted as escalated transitions with bounded error
codes. No broker command may route while the newest reassignment is short of
`coordinator_committed`; the routing barrier prevents a stale worker or an
unconfirmed successor from receiving work.

Coordinator events are accepted only from the bound worker and fence epoch.
The durable cursor is contiguous: exact replay is idempotent, while a gap or a
conflicting replay fails without advancing the watermark.

## Verification and continuation

Agent runtime completion is evidence, not task completion. The registered
repository verifier is the only path from `waiting` to a terminal success. Its
wire is exactly the package result plus phase: `phase`, `outcome`,
`contractDigest`, `taskEvidenceDigest`, bounded opaque `headRevision`,
`reasons`, and `evidenceRefs`. Signal validates that package shape and phase
mapping, but does not require legacy work-item, attempt, session, fence,
verifier, completion-contract, or evaluation-revision members on that wire.
`evaluation_revision`, where retained in the ledger, is exactly a
representation projection of the package `headRevision`, not a provider fact.
This permits source-owned local refusal and deadline revisions without
inventing a GitHub SHA.

The repository task permits one durable continuation. Agentd owns that
continuation; Signal maps package `continuation` and `missing_or_stale` to a
durable poll only and never submits a second turn. A second continuation
request exhausts the budget and moves the work item to failed escalation rather
than opening an unbounded agent loop. Workspace cleanup remains an agentd
responsibility and is therefore outside this repository's janitor boundary.
Verifier results carry the exact executor-attempt identity at Signal's
persistence boundary. A canonical receipt for that attempt makes exact replay
idempotent, including after terminal commit, while a different result for the
same attempt fails closed.

## Deliberately deferred

- production credential and packaging decisions
- production route activation
- the real repository-verifier emitter
- agentd workspace janitor behavior
- locked cross-repository staging and production lifecycle proof

Those require the broker and agentd release revisions to be chosen together
under the cross-repository orchestration protocol.
