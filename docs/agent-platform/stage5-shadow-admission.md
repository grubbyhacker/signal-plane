# Stage 5 (shadow admission): the ledger shadow seam

Status: shadow seam landed, inert. Second slice of Stage 5, on top of the
WorkItem agent fields (#61). No launch, no legacy-launcher enqueue, no legacy
dispatcher-table mutation, and no external ingress transport is added.

Architecture source of truth: `agent-infra-docs/design/agent-platform-coupling.md`
(Sequencing step 5 — "Migrate by shadow admission first: admit and dry-run
without launching, then cut over to one active launcher at a time"). This
document may not contradict it.

## What this stage establishes

A source-neutral WorkItem candidate can be **admitted** into the ledger as a dry
run: it dedupes, persists the merged `(agent_type, agent_mode,
type_contract_revision)` binding plus the source identity, and returns a
deterministic admission/dedup result — but nothing launches.

- `internal/shadowadmit` — the seam.
  - `Candidate` — a source-neutral admission request: the route snapshot id, the
    normalized ledger `Event`, and the admission-safe `AgentBinding`.
  - `Shadow` — disabled-by-default service. `Admit` validates, refuses any
    image/generation selection, and admits via `workledger.AdmitWithAgent`. When
    disabled it returns `ErrDisabled` and persists nothing.
  - `Outcome` — deterministic: `WorkItemID`, `EventID`, `Duplicate`, the
    persisted binding, and `Launched` which is always `false`.
  - The `admitter` interface the seam depends on is admission-only: it exposes
    `AdmitWithAgent` and nothing else — no `Claim`, no launch, no dispatcher
    method — so the seam cannot start work even by mistake.
- `internal/config` — `ShadowAdmissionConfig`, wired into `Config`, **disabled by
  default**. The zero value is inert; the block carries no listener address
  because external ingress transport is design-deferred.

## Invariants this stage encodes

1. **No image / release / generation selection at admission.** `Shadow.Admit`
   calls `AgentBinding.RejectAdmissionSelection` before persisting; a candidate
   may carry only the deployment-owned `(agent_type, mode, contract revision)`
   selection.
2. **One active launcher.** The shadow seam is not a launcher: `internal/shadowadmit`
   imports no dispatcher/broker package, never calls the ledger `Claim` path,
   and never references a legacy dispatcher table. It therefore cannot run as a
   second live launcher beside the legacy one. Cutover to a single active
   launcher is a later stage.

Both are enforced offline by `scripts/validate-shadow-admission.py` (wired into
`mise run check` as `validate:shadow-admission`) and by
`internal/shadowadmit/shadowadmit_test.go`:

- disabled seam is inert (`ErrDisabled`, nothing persisted);
- admit persists the binding and dedupes the same candidate deterministically to
  one work item, never reporting a launch;
- selection fields (generation/digest/broker-run/PR) are refused;
- a candidate without a route snapshot id is refused.

## Deferred (not in this PR)

External ingress transport — the host Unix-domain socket, event envelope, and
route table — is deferred in the design (Deferred #1). This PR is the ledger
shadow seam a future ingress calls into, not the ingress itself. Cutover to an
active launcher (Sequencing step 5, second half) is also later.

## Gate

`mise run check` — fmt, three offline contract validators
(`validate:image-tag-contract`, `validate:workitem-agent-admission`,
`validate:shadow-admission`), `go test ./...`, build.
