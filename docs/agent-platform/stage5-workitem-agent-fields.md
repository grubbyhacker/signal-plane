# Agent platform — signal-plane implementation notes

Per-repo stage notes for the agent-platform-coupling effort. The cross-repo
architecture and its invariants live in
`agent-infra-docs/design/agent-platform-coupling.md` and take precedence; this
document may not contradict it.

## Stage 5 (foundation): WorkItem agent fields on `internal/workledger`

Status: foundation landed, inert. No ingress, launch, routing, production
config, or deploy behavior is added by this stage — only the durable
persistence surface the later stages resolve into.

### What this stage establishes

`internal/workledger` is already source-neutral (a webhook, a clock tick, and a
local enqueue produce the same `WorkItem` shape). This stage extends the
`work_items` table with the agent identity the design requires, keeping the
legacy dispatcher tables (`repository_ci_*`, `jobs`-referencing) as read-only
history — they are untouched.

Additive columns on `work_items` (all zero-value safe; a pre-agent database
migrates in place and existing rows keep working):

| Column | Meaning | Authority |
|--------|---------|-----------|
| `agent_type` | concrete type, kebab-case (`youknowme-curator`) | deployment-owned route selection, carried at admission |
| `agent_mode` | per-type enumerated mode, snake_case (`reconcile`) | deployment-owned route selection, carried at admission |
| `type_contract_revision` | AgentType / container-invocation contract revision the route resolved against | deployment-owned route selection, carried at admission |
| `resolved_release_generation` | broker-assigned monotonic generation of the active release (0 = unresolved) | broker, resolved AFTER admission |
| `resolved_release_digest` | broker-derived immutable image digest of the resolved release | broker, resolved AFTER admission |
| `broker_run_id` | broker-minted run identity the capability was issued for | broker, resolved AFTER admission |
| `authoritative_pr_repository` / `authoritative_pr_number` | repo + PR from the broker's own authenticated `pull.create` | broker, recorded AFTER admission |

### The invariant this stage encodes

**Emitters and runtime callers never select an image, release, or generation**
(design Non-goals). Concretely, at admission a caller may carry only the
deployment-owned `(agent_type, mode, type_contract_revision)` selection; the
release generation, its digest, the broker run, and the PR correlation are all
resolved by the broker after admission and are refused if supplied at admission.

Encoded in two places, both offline:

1. `AgentBinding.RejectAdmissionSelection` (`internal/workledger/agentbinding.go`)
   — the guard the admission path invokes before persisting a work item;
   `AdmissionSelectionFields()` is the single source of truth for the refused
   set.
2. `scripts/validate-workitem-agent-admission.py` — a file-only CI validator
   (the model is `scripts/validate-image-tag-contract.py`) that fails if the
   refusal set drifts from the struct, if admission stops invoking the guard, or
   if the admission `INSERT` ever writes a broker-resolved column. Wired into
   `mise run check` as `validate:workitem-agent-admission`.

### Persistence surface added (inert)

- `Store.AdmitWithAgent` — admit an event carrying an admission-safe
  `(agent_type, mode, contract revision)` route selection. `Admit` /
  `AdmitRelease` are unchanged and admit with a zero binding.
- `Store.ResolveRelease` — broker-authoritative: record generation + digest +
  broker run. Monotonic by generation; a mismatched digest for the same
  generation is refused; enforcement stays digest-only at launch (not in this
  stage).
- `Store.RecordAuthoritativePRCorrelation` — bind repo + PR from the broker's
  own `pull.create`. Idempotent; a conflicting correlation is refused. Does not
  itself observe the broker call.
- `Store.WorkItemByAuthoritativePR` — map a later review webhook's repo + PR
  back to the originating work item. An unknown PR yields `sql.ErrNoRows`, which
  callers must treat as no dispatch (silence is correct, guessing is the flaw).
- `Store.WorkItem` — read a work item by id, including the agent fields.

None of these are wired to the dispatcher, ingress, or any launch path in this
stage. They are the durable foundation stages 4–6 resolve into.

### Migration

Additive, `PRAGMA user_version` 21 → 22, via the existing `ensureMigrationColumn`
pattern. The two agent indexes (`work_items_agent_type`,
`work_items_authoritative_pr`) are created after the columns are ensured so an
upgraded pre-agent table has them present. Round-trip, query, monotonicity, and
migration tests live in `internal/workledger/agentbinding_test.go`.

### Gate

`mise run check` — fmt, both offline contract validators, `go test ./...`, build.
