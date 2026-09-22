# Stage 5 (route resolver): first deployment-owned RouteResolver

Status: landed, wired to shadow admission / inert ingress only. On top of the
WorkItem agent fields (#61), shadow-admission seam (#62), and shadow ingress
(#63). No launcher, broker call, legacy dispatcher mutation, or external network
listener.

Architecture source of truth: `agent-infra-docs/design/agent-platform-coupling.md`
("Selection authority") — emitters name a domain fact only; a deployment-owned
route maps the fact to `(agent_type, mode)`; the broker later resolves the
generation. This document may not contradict it.

## What this stage establishes

`internal/routeresolver` — the first deployment-owned `RouteResolver`
(satisfies `shadowingress.RouteResolver`).

- Route config (`Config`): versioned, deployment-owned DATA mapping a domain
  fact to `(agent_type, mode)` plus the ledger `route_snapshot_id` that admits
  it, and an optional `contract_revision`. It carries **no** image, release, or
  generation; `ParseConfig` decodes YAML with `KnownFields(true)` and `New`
  additionally scans for smuggled selection keys (`forbiddenFields`).
- Facts: `Fact(event)` = `event_kind[.action]`, exactly as the design names
  them. The first two facts:
  - `upload.completed` → `(youknowme-curator, process_intake)`
  - `youknowme.reconciliation_due` → `(youknowme-curator, reconcile)`
  - Verified against existing domain vocabulary: neither string exists in the
    codebase today (the "reconciliation" hits are the unrelated legacy CI-repair
    machinery), so no aliasing was needed.
- Deterministic revision: `Config.Revision()` is a sha256 over the
  version-plus-fact-sorted config → `routecfg:v<version>:<hex>`. Order-
  independent and change-sensitive, so a routing change is attributable.
- Mode validation: `New` validates every selected `(agent_type, mode)` against
  an injected `ModeCatalog` interface (`Declares(agentType, mode) bool`), so the
  resolver depends on the AgentType contract SHAPE, not any implementation. A
  deployment-owned `StaticCatalog` / `YouKnowMeCuratorCatalog` satisfies it.
- Resolution: an unknown fact returns `Matched=false` — no dispatch, no
  guessing. The returned binding carries only `(agent_type, mode, contract
  revision)`.

## Wiring

The resolver is injected into `shadowingress.NewServer` as the required
`RouteResolver`. The ingress admits through `shadowadmit` only, feeding the
resolver's route snapshot and admission-safe binding. This package imports no
dispatcher/broker/launch code and never calls the ledger `Claim` path.

## Tests

- Unit (`resolver_test.go`): strict parse rejects unknown/selection fields;
  deterministic order-independent revision; both curator facts match; unknown
  fact → no match; catalog rejection (undeclared mode / unknown type / nil);
  fact composition.
- Integration (`integration_test.go`): activate real ledger routes, wire the
  resolver → shadowadmit, prove the resolver's binding (not caller input)
  persists; unknown fact yields no match; the resolver satisfies
  `shadowingress.RouteResolver` and an enabled ingress binds with it.

## Config

`configs/route-resolver.example.yaml` — the deployment-owned route table.
`route_snapshot_id` points at an activated ledger route snapshot.

## Deferred (not in this PR)

Cutover to an active launcher, the broker generation resolution, and a real
AgentType registry behind the `ModeCatalog` seam.

## Gate

`mise run check` — fmt, five offline validators (incl. `validate:route-resolver`),
`go test ./...`, build.
