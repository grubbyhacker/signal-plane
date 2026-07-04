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
  the stream.

## Development

```sh
make check
go run ./cmd/signal-gateway
go run ./cmd/signal-observer
```

Local NATS and end-to-end development wiring will be added in the first
implementation sprint.

## Design

- [Signal plane design](docs/agent-signal-plane-design.md)
- [Near-term phase plan](docs/near-term-phase-plan.md)
