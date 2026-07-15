# Signal Plane

Signal Plane receives authenticated external signals and carries them through a
small durable event pipeline before any agent-specific behavior is added.

The event plane currently provides:

```text
GitHub or manual test sender
  -> signal-gateway
  -> NATS JetStream
  -> signal-observer (observation)
  -> github-task-dispatcher (retained, disabled proof implementation)
```

The repository now also contains the generalized durable work-ledger core. It
stores immutable route snapshots, source-neutral work items and events, and
typed executor attempts in the same additive SQLite schema used by the retained
proof dispatcher. The registry accepts only compiled executor implementations;
route JSON cannot select commands, URLs, images, credentials, or authority.
No generalized production route is active in this slice.

The disabled-by-default `resume-release-router` is the first compiled
deterministic executor. When deployment-owned configuration enables it, it
admits only GitHub `release/published` events for
`grubbyhacker/resume-builder`, verifies the numeric release, tag, full commit,
canonical structured-Markdown asset, and SHA-256 digests through the fixed
read-only GitHub App identity, then calls YouKnowMe with a content-derived
idempotency key. Release prose and caller-selected URLs, commands, images,
credentials, authority, or merge choices never enter its operation schema.

The gateway remains generic. Agent-specific selection and durable job state are
isolated in `github-task-dispatcher`.

## Components

- `signal-gateway`: HTTP ingress service. It validates source-specific
  authentication and admission policy, wraps accepted payloads in a thin
  envelope, and publishes to NATS JetStream.
- `signal-observer`: NATS consumer used to watch accepted events flow through
  the stream. It retains a durable pull consumer and acknowledges only after
  successful decode and observer logging.
- `github-task-dispatcher`: disabled by default and retained to preserve the
  proven persistence, idempotency, serialization, status, and recovery
  mechanics while the generalized router is built. Its predicate matches only
  the synthetic `example/automation-target` fixture; no production admission,
  webhook, or broker authorization exists for that repository. It stores only
  delivery/job control data in SQLite WAL and calls the private broker's
  `codex-issue-implement` profile when exercised by tests.
- `internal/workledger`: source-neutral admission, deduplication,
  serialization, supersession, retry, and interrupted-attempt recovery behind
  a SQLite store. GitHub is the first authenticated ingress adapter. The core
  carries namespace/object identity plus correlation and causation without
  assuming that every future source is GitHub.
- `resume-release-router`: inert generalized router and
  `youknowme_upload_v1` executor. It supports only deployment-owned
  `cloudflare_access` or `local_secret` YouKnowMe authentication and loads the
  GitHub App PEM from a mounted file.

## Service endpoints

`signal-gateway` serves `/healthz`, `/readyz`, and `/metrics` on its configured
gateway address. `/healthz` is process liveness; `/readyz` requires a live NATS
connection and an inspectable configured JetStream stream, without publishing a
probe.

`signal-observer` serves private operational endpoints on `:8081` by default
(`SIGNAL_OBSERVER_ADDR` overrides it): `/healthz`, `/readyz`, and `/metrics`.
Observer readiness requires an inspectable NATS connection and durable consumer,
so it remains ready while the stream is idle. Malformed messages are terminally
rejected after metadata-only logging; successful observer work uses synchronous
JetStream acknowledgement.

`github-task-dispatcher` serves the same private operational endpoints on its
configured address (default `:8082`). When disabled it initializes and validates
the configured SQLite schema and checkpoint metadata, then remains alive in
standby: health is healthy and readiness returns `503` with `disabled`. Standby
does not read the broker token or connect to NATS. Its SQLite schema never
stores issue title, body, comments, or the provider payload. The broker request
is `POST dispatcher.broker_url` with this fixed shape:

```json
{"parameters":{"issue_number":123,"source_delivery_id":"..."}}
```

It requires `broker_token_env` and uses `Authorization: Bearer ...`; it
sets the semantic `Idempotency-Key`
`github-task-dispatcher:v2:<repo>:issue:<issue-number>:codex-issue-implement`;
the broker's required `source_delivery_id` parameter is a stable semantic hash
of repository, issue, and profile. The real GitHub delivery ID remains only in
SQLite for audit correlation. Transport
failures, HTTP 429/5xx, and structured `profile_busy` responses use a durable,
deterministic 2s/4s/8s/16s/20s launch retry schedule for at most ten minutes.
Other errors, including `idempotency_conflict` and malformed success responses,
fail immediately. The semantic job key is
`github-issue-implement:v1:<repo>:<issue-number>`. `broker_url` must be the
exact private endpoint
`/v1/launch-profiles/codex-issue-implement/launch`. A 2xx response counts as
successful only when it contains a nonempty JSON `run_id`; fresh and
idempotently replayed responses are validated the same way, and that run ID is
stored immediately. The single worker then polls only scoped
`GET /v1/runs/{run_id}` status and does not launch another issue until the run
reaches `completed`, `failed`, or `timed_out`.

Every recorded delivery includes its JetStream stream sequence. Restores use
the managed, offline `github-task-dispatcher recover` procedure documented in
[Dispatcher recovery](docs/dispatcher-recovery.md). It validates the restored
checkpoint against the backup manifest, resets the configured durable to
checkpoint + 1, replays the bounded backlog, reconciles every restored active
run through the authenticated broker status endpoint, and records JSON plus
SQLite evidence. The command is read-only unless `--execute` is supplied and
never calls the broker launch endpoint. An incomplete recovery marker blocks
normal dispatcher startup and therefore blocks new launches.

## Development

```sh
mise run check
mise run proof:dispatcher-recovery
mise run compose-up
SIGNAL_GATEWAY_MANUAL_TOKEN=local-dev-token mise run gateway
mise run observer
```

Run the local end-to-end smoke path with:

```sh
mise run smoke-local
```

The smoke test starts local NATS JetStream with Docker Compose, starts the
gateway and a one-shot observer, sends an authenticated manual event, and waits
for the observer to log the received signal.

Build the service image with:

```sh
docker build -t signal-plane:local .
```

The image contains all four binaries; the default
entrypoint runs the gateway, and deployment tooling can override the command to
run the observer or dispatcher.

## Dispatcher proof boundary

This repository does not perform deployment. The old repository-specific proof
route has been retired, and the remaining synthetic selector is not a
production deployment target. Do not register a webhook, add gateway admission,
or grant a broker identity access to `example/automation-target`.

Production routing and authority bootstrap follow the settled roadmap in
`vps-ops/docs/repository-agent-automation-roadmap.md`; this proof dispatcher is
not the generalized router, and the generalized ledger has no active route yet.

Pushes to `main` publish the deployment image to
`ghcr.io/grubbyhacker/signal-plane:main`.

## Design

- [Signal plane design](docs/agent-signal-plane-design.md)
- [Near-term phase plan](docs/near-term-phase-plan.md)
