# Agent Signal Plane Design

This repo owns the signal-plane implementation. The broader VPS deployment and
Cloudflare/GitHub control-plane shape live in `vps-ops`.

The first construction milestone ends with real GitHub events flowing through a
secure public ingress into NATS JetStream and an observer log. It intentionally
does not involve Hermes, agents, LLMs, or a semantic job controller.

## Core Shape

```text
GitHub or manual test sender
  -> Cloudflare Tunnel, later
  -> signal-gateway
  -> NATS JetStream
  -> signal-observer
  -> logs and metrics
```

## Principles

- `signal-gateway` is source-aware but not agent-aware.
- NATS JetStream is event transport, not a job controller.
- Preserve provider payloads. Use a thin envelope and carry raw payloads through
  the pipeline.
- Extract fields only for authentication, admission, routing, and observability.
- Do not expose NATS directly to the internet.
- Keep webhook secrets in Doppler or runtime secret paths, never in repo files.

## Initial Signal Envelope

```json
{
  "meta": {
    "signal_id": "signal-...",
    "source": "github",
    "route_id": "github-signal-intake",
    "received_at": "2026-07-04T00:00:00Z",
    "source_event": "pull_request",
    "source_action": "synchronize",
    "source_delivery_id": "..."
  },
  "payload": {}
}
```

The `payload` field contains the original provider payload.

## First Live Milestone

The first live milestone is complete when:

- A public GitHub webhook route reaches `signal-gateway` through Cloudflare.
- GitHub HMAC verification succeeds.
- `ping` is verified and acknowledged.
- Real pull request events publish to JetStream.
- `signal-observer` logs the accepted events.
- Rejected requests are counted by reason.
- No agents or job-controller behavior are involved.
