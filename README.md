# Signal Plane

Signal Plane receives authenticated external signals and carries them through a
small durable event pipeline before any agent-specific behavior is added.

Phase one proves:

```text
GitHub or manual test sender
  -> signal-gateway
  -> NATS JetStream
  -> signal-observer
  -> logs and metrics
```

No Hermes, job controller, or LLM runtime is part of the first milestone.

## Components

- `signal-gateway`: HTTP ingress service. It validates source-specific
  authentication and admission policy, wraps accepted payloads in a thin
  envelope, and publishes to NATS JetStream.
- `signal-observer`: NATS consumer used to watch accepted events flow through
  the stream. It retains a durable pull consumer and acknowledges only after
  successful decode and observer logging.

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

The image contains both `signal-gateway` and `signal-observer`; the default
entrypoint runs the gateway, and deployment tooling can override the command to
run the observer.

Pushes to `main` publish the deployment image to
`ghcr.io/grubbyhacker/signal-plane:main`.

## Design

- [Signal plane design](docs/agent-signal-plane-design.md)
- [Near-term phase plan](docs/near-term-phase-plan.md)
