# Agent Instructions

This repository contains the signal-plane services used to receive authenticated
external events, publish accepted signals to NATS JetStream, and observe event
flow before any agent or job-controller behavior is introduced.

## Project Boundaries

- Keep `signal-gateway` source-aware, but not agent-aware.
- Keep NATS JetStream as event transport, not a semantic job controller.
- Preserve provider payloads. Use a thin envelope and avoid provider JSON
  reshaping unless a downstream boundary explicitly requires it.
- Do not expose NATS or future job APIs directly to the internet.
- Do not store webhook secrets, provider tokens, or generated credentials in the
  repository.

## Development

- Language: Go.
- Use `make check` before handoff.
- Keep the first implementation boring: standard library first, small packages,
  narrow interfaces, and explicit tests.
- If external dependencies are added, keep them justified and run `go mod tidy`.

## Delivery

Work on feature branches and open ready-for-review PRs. Do not open draft PRs
unless Roger explicitly requests draft mode.
