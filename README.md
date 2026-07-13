# Signal Plane

Signal Plane receives authenticated external signals and carries them through a
small durable event pipeline before any agent-specific behavior is added.

The event plane currently provides:

```text
GitHub or manual test sender
  -> signal-gateway
  -> NATS JetStream
  -> signal-observer (observation)
  -> github-task-dispatcher (narrow, disabled-by-default task dispatch)
```

The gateway remains generic. Agent-specific selection and durable job state are
isolated in `github-task-dispatcher`.

## Components

- `signal-gateway`: HTTP ingress service. It validates source-specific
  authentication and admission policy, wraps accepted payloads in a thin
  envelope, and publishes to NATS JetStream.
- `signal-observer`: NATS consumer used to watch accepted events flow through
  the stream. It retains a durable pull consumer and acknowledges only after
  successful decode and observer logging.
- `github-task-dispatcher`: disabled by default. Its own configurable durable
  consumer accepts only open, non-PR `issues/labeled` events for
  `grubbyhacker/apple-jobs-matcher` carrying `agent:implement`. It stores only
  delivery/job control data in SQLite WAL and calls the private broker's
  `codex-issue-implement` profile.

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

When enabled, `github-task-dispatcher` serves the same private operational
endpoints on its configured address (default `:8082`). Its SQLite schema never
stores issue title, body, comments, or the provider payload. The broker request
is `POST dispatcher.broker_url` with this fixed shape:

```json
{"parameters":{"issue_number":123,"source_delivery_id":"..."}}
```

It requires `broker_token_env` and uses `Authorization: Bearer ...`; it
sets `Idempotency-Key` to
`github-task-dispatcher:v1:<repo>:delivery:<delivery-id>:codex-issue-implement`.
Broker 4xx responses are terminal; network and 5xx failures use bounded
exponential retry attempts. The semantic job key is
`github-issue-implement:v1:<repo>:<issue-number>`. `broker_url` must be the
exact private endpoint
`/v1/launch-profiles/codex-issue-implement/launch`. A 2xx response counts as
successful only when it contains a nonempty JSON `run_id`; fresh and
idempotently replayed responses are validated the same way, and that run ID is
stored with the dispatcher job.

## Development

```sh
mise run check
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

The image contains all three binaries; the default
entrypoint runs the gateway, and deployment tooling can override the command to
run the observer or dispatcher.

## Dispatcher deployment prerequisites

This repository does not perform deployment. Before enabling the dispatcher in
a private environment, the following hard prerequisites must be satisfied:

- The broker must guarantee idempotency and scope status lookups to the caller
  and profile; an idempotency key must not expose another caller's job state.
- VPS operations must provide a private control network, broker token delivery,
  and backup/restore coverage for the SQLite state file (including WAL state).
- GitHub hook admission and gateway configuration must permit the target
  repository's `issues` / `labeled` events while retaining signature checks.

Pushes to `main` publish the deployment image to
`ghcr.io/grubbyhacker/signal-plane:main`.

## Design

- [Signal plane design](docs/agent-signal-plane-design.md)
- [Near-term phase plan](docs/near-term-phase-plan.md)
