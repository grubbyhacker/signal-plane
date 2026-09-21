# Agent Signal Plane Design

This repo owns the signal-plane implementation. The broader VPS deployment and
Cloudflare/GitHub control-plane shape live in `vps-ops`.

The first construction milestone ends with real GitHub events flowing through a
secure public ingress into NATS JetStream and an observer log. It intentionally
does not involve Hermes, agents, LLMs, or a semantic job controller.

## Current status

> This section reflects the current implementation. The design narrative below
> it predates the dispatcher and describes the original ingress-and-observer
> milestone; read that part as history, not as the current scope. The following
> is configured state against the vps-ops production inventory (managed
> configuration, not SSH-observed runtime).

The repo now does more than ingress and observation. Implemented and enabled in
production:

- **github-task-dispatcher (enabled):** consumes accepted signals and decides
  whether to launch work, keeping its decisions in a durable work ledger. It
  EMITS only — it POSTs authenticated launch requests to an external broker and
  never runs a model or spawns a process itself. No `os/exec` or process spawn
  exists anywhere in this repo, and signal-plane holds no GitHub credential.
- **Failed-CI repair lifecycle (enabled, bounded):** reconciliation wake 168h,
  active timeout 60m, max 2 attempts.
- **resume-release-router (enabled):** routes resume-builder `release` /
  `published` events to the YouKnowMe MCP.

Implemented but **disabled** in production:

- **push-security-scanner.**

The dispatcher's production route is the fixture only
(`repository-agent-fixture` → `grubbyhacker/repository-agent-fixture`); it is not
wired to a broader set of repositories. Inbound events are GitHub webhooks
authenticated by HMAC-SHA-256; there is no CI polling — `check_run` /
`check_suite` / `status` / `pull_request` are wake-ups, not the CI verdict.

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
