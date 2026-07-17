# PR10 asynchronous push tripwire

This slice is disabled by default. It does not activate a production webhook,
broker authority, credential holder, scanner, or real secret. Deployment owns
those later decisions.

The gateway admits only a configured repository/ref tuple after verifying the
GitHub `sha256` webhook signature. Its durable envelope carries delivery ID,
repository, ref, before/after SHA, provider push timestamp, head timestamp, and
local receipt timestamp. A ref deletion remains admissible evidence; the
read-only broker material endpoint rejects it before any opaque Git operation.

The scanner uses a dedicated JetStream durable with a six-minute acknowledgement
lease and an independent exact repository/ref catalog. SQLite stores the first
receipt, immutable push identity, sanitized result and timing state, digest
registry, and a transactional security-event outbox. A duplicate delivery with
different identity is rejected. A finding and `alert_requested` outbox event
are committed before broker response side effects. Once that durable result
exists, the JetStream delivery can be acknowledged even when alert publication
or broker response is temporarily unavailable.

Startup and periodic maintenance independently flush the event outbox, retry
every pending idempotent broker response, advance overdue live-token
SLO state to `breached`, and prunes fingerprint metadata. Reconciliation runs
at `reconcile_interval` (five seconds by default) indefinitely, including after
the receipt deadline; it is not bounded by JetStream redelivery count.
Fingerprint pruning runs at `fingerprint_prune_interval` (one hour by default)
without requiring a new admitted push.

The broker wire contract is `broker/push-tripwire/v1`:

- `POST /v1/security/push-tripwire/material` accepts only delivery, repository,
  ref, before, and after identity. Returned commits and file sides are bounded;
  the scanner revalidates all identity and bounds.
- `POST /v1/security/push-tripwire/respond` accepts a deterministic
  `Idempotency-Key`, fixed `halt_issuance` and optional
  `fence_worker_session` actions, and a deployment-derived binding. Issuance
  must return `halted`; fencing returns `fence_requested` or `fenced` with a
  per-action `completed_at`.

The future holder registers only a precomputed 32-byte HMAC-SHA-256 digest plus
bounded metadata through the private authenticated scanner endpoint. The
scanner never accepts or persists the token value. The caller supplies expiry;
the scanner derives `retained_until` from its configured forensic retention, so
the holder cannot extend or shorten the reviewed retention. Exact active digest
and configured canary matches are high severity. Expired/revoked exact matches
are high forensic findings but never trigger live halt/fence and are explicitly
marked `not_live_when_scanned`. Generic shape/entropy matches are low and remain
local; they never request account logout or broker response.

The live-token response SLO is measured from the first durable receipt and is
the smaller of five minutes or ten percent of registered token TTL. It requires
both the durable alert request and broker-confirmed issuance halt. Fence request
or completion is tracked separately and cannot turn a met live-token halt SLO
into a breach.

Focused tests cover invalid authentication/catalog, replay and collision,
delayed processing, before mismatch, deletion, non-fast-forward rejection, all
reviewed bounds, reversible exact/canary encodings, generic low severity,
sanitized durable events, and idempotent halt/fence reconciliation.
The lifecycle tests also cover broker outage across restart and deadline,
independent alert publication, late response recovery without duplicate effects,
and fingerprint expiry with no new push traffic.
