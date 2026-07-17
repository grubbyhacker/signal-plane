# PR10 coordinator integration seam

This change establishes Signal Plane's credential-independent half of the PR10
coordinator contract. It deliberately does not activate a production route,
choose runtime credentials, or claim a lifecycle proof.

## Broker boundary

`internal/agentsession.HTTPBroker` is the concrete authenticated HTTP adapter
for the broker's coordinator v1 endpoints. Requests use fixed paths and strict
JSON decoding. Session acquisition, creation, turn submission, event streaming,
reassignment, and reassignment-status reads preserve the complete authority and
fencing identity:

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
the saga. No broker command may route while the newest reassignment is short of
`coordinator_committed`; the routing barrier prevents a stale worker or an
unconfirmed successor from receiving work.

Coordinator events are accepted only from the bound worker and fence epoch.
The durable cursor is contiguous: exact replay is idempotent, while a gap or a
conflicting replay fails without advancing the watermark.

## Verification and continuation

Agent runtime completion is evidence, not task completion. The registered
repository verifier is the only path from `waiting` to a terminal success. Its
result must match the immutable task contract digest, task evidence digest, and
named completion contract, identify an exact repository head, and cite durable
evidence. A satisfied result additionally requires an `attempt_completed`
coordinator event.

The repository task permits one durable continuation. A second continuation
request exhausts the budget and moves the work item to failed escalation rather
than opening an unbounded agent loop. Workspace cleanup remains an agentd
responsibility and is therefore outside this repository's janitor boundary.

## Deliberately deferred

- production credential and packaging decisions
- production route activation
- the real repository-verifier emitter
- agentd workspace janitor behavior
- locked cross-repository staging and production lifecycle proof

Those require the broker and agentd release revisions to be chosen together
under the cross-repository orchestration protocol.
