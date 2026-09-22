# Stage 5 (shadow ingress): unprivileged host intake

Status: host ingress landed, inert. Third slice of Stage 5, on top of the
WorkItem agent fields (#61) and the shadow-admission ledger seam (#62). No
launcher, broker call, legacy dispatcher-table mutation, external network
listener, or YouKnowMe container credential path is added.

Architecture source of truth: `agent-infra-docs/design/agent-platform-coupling.md`,
"The intake path" — "For host ingress, use a Unix-domain socket with filesystem
permissions and peer-credential checks (SO_PEERCRED). Plain loopback HTTP proves
locality but not which local process called, and a socket carries no secret to
rotate." This document may not contradict it.

## What this stage establishes

`internal/shadowingress` — the host-side intake that feeds the shadow-admission
seam (#62).

- Transport: a **Unix-domain socket** created with group-readable/writable mode
  (`0660`) in a setgid directory whose group is the YouKnowMe transport GID.
  The directory remains non-group-writable; a group/world-writable parent is
  refused at start. Database permissions are unchanged and never shared with
  the producer.
- Authentication: **SO_PEERCRED** on Linux (`golang.org/x/sys/unix.GetsockoptUcred`)
  reads the connecting process's kernel-reported UID and checks it against an
  allow-list (empty = own UID only). The credential is set by the kernel at
  connect time and cannot be forged by the caller.
- Non-Linux: the package builds and tests on macOS (dev), but the peer
  authorizer **fails closed** — there is no portable unforgeable local
  credential to substitute, and production is Linux.
- Envelope: a **bounded** (`64 KiB`), source-neutral domain-fact message with
  `DisallowUnknownFields`. It carries the event identity (source, namespace,
  object, revision, evidence) ONLY. It has **no** routing (route snapshot),
  agent type, mode, image, release, generation, broker run, or PR field — an
  emitter states a domain fact and names no agent and no route. Any such JSON
  key is an unknown field and is rejected.
- Routing: deployment-owned, via an injected `RouteResolver` that maps the
  validated domain-fact `Event` to `{RouteSnapshotID, AgentBinding, Matched}`.
  An enabled ingress **requires** a resolver. An unmatched fact routes to no
  agent and is deterministically dropped (no admission, `matched:false` reply) —
  silence is correct, guessing is the flaw. The route-table schema is not
  implemented here; only the interface seam.
- Admission: through `shadowadmit.Shadow.Admit` **only**, fed the **resolver's**
  route snapshot and admission-safe binding — never any caller-supplied value.
- Stale socket: cleared only after an `os.Lstat` + `os.ModeSocket` check.
  A regular file, directory, symlink, or any non-socket at the socket path is
  **preserved** and startup fails, so a misconfigured path can never destroy
  real data.

## Invariant: no launch dependency

`internal/shadowingress` imports no dispatcher/broker package, never calls the
ledger `Claim` path, references no legacy dispatcher table, opens no external
(tcp/http) listener, and touches no YouKnowMe credential path. Enforced offline
by `scripts/validate-shadow-ingress.py` (wired into `mise run check` as
`validate:shadow-ingress`), which also requires the unix socket, SO_PEERCRED,
the zero binding, and the disabled-by-default no-op.

## Tests

- Platform-neutral (`server_test.go`): envelope validation + bounds; rejection
  of caller-supplied route/agent/image/release/generation JSON fields (unknown
  fields); `validateSocketPath` (absolute + owner-only parent); enabled ingress
  requires a resolver; disabled no-op; socket round-trip that proves the
  RESOLVER's output (not caller input) feeds shadowadmit and the persisted
  binding is the resolver's, plus deterministic dedup and `0660` mode; an
  unmatched fact is dropped, not admitted; `clearStaleSocket` removes a real
  socket but preserves a regular file, and `Serve` refuses to start (without
  clobbering it) on a non-socket path. The round-trip injects an allow-all
  authorizer so it runs off-Linux.
- Linux-only (`peercred_linux_test.go`): `authorizeUID` (own-uid and allow-list
  membership) and a real SO_PEERCRED handshake that accepts the test's own
  process and rejects an excluded UID.

## Config

`shadow_admission.ingress` — `enabled` (default false), `socket_path`,
`allowed_uids`. Disabled by default; no network address.

## Deferred (not in this PR)

Cutover to an active launcher, the deployment-owned route table that maps a
signal to `(agent_type, mode)`, and any external-network ingress. This PR is the
host intake seam only.

## Gate

`mise run check` — fmt, four offline validators
(`validate:image-tag-contract`, `validate:workitem-agent-admission`,
`validate:shadow-admission`, `validate:shadow-ingress`), `go test ./...`, build.
