# Agent Instructions

This repository contains the signal-plane services. They receive authenticated
external events, publish accepted signals to NATS JetStream, observe event flow,
and act on that flow: they make dispatch decisions, keep a durable work ledger,
run a bounded failed-CI repair lifecycle, and route releases. The signal-gateway
remains a source-aware ingress and NATS remains the event transport; the
decision-making services consume that transport, they do not turn it into a
semantic job controller.

## Current status

Configured state (against the vps-ops production inventory; managed
configuration, not SSH-observed runtime):

- **github-task-dispatcher — enabled.** Consumes accepted signals and decides
  whether to launch work. It only EMITS: it POSTs authenticated launch requests
  to an external broker (`internal/dispatcher/broker.go`) and never runs a model
  or spawns a process itself. There is no `os/exec` or process spawn anywhere in
  this repo, and signal-plane holds no GitHub credential.
- **Failed-CI repair — enabled and bounded.** Reconciliation wake 168h, active
  timeout 60m, max 2 attempts.
- **Production route is the fixture only.** The dispatcher routes the
  `repository-agent-fixture` to `grubbyhacker/repository-agent-fixture`. It is
  not wired to a broader set of repositories.
- **resume-release-router — enabled.** Routes resume-builder `release`/`published`
  events to the YouKnowMe MCP.
- **push-security-scanner — implemented but disabled** in production.

Inbound events are GitHub webhooks authenticated by HMAC-SHA-256 over
`X-Hub-Signature-256` (`internal/source/githubwebhook/`). There is no CI polling:
`check_run` / `check_suite` / `status` / `pull_request` are wake-ups, never the
CI verdict itself. Production NATS subjects are clean domain names
(`signals.github.webhook`, etc.); milestone strings appear only in test
constants and doc filenames.

The `docs/agent-signal-plane-design.md` narrative predates the dispatcher and
describes the first ingress-and-observer milestone; read it as history, not as
the current scope.

## Project Boundaries

- Keep `signal-gateway` source-aware, but not agent-aware.
- Keep NATS JetStream as event transport, not a semantic job controller.
- Preserve provider payloads. Use a thin envelope and avoid provider JSON
  reshaping unless a downstream boundary explicitly requires it.
- Do not expose NATS or job APIs directly to the internet.
- Do not store webhook secrets, provider tokens, or generated credentials in the
  repository.

## Development

- Language: Go.
- Use `mise run check` before handoff.
- Keep the first implementation boring: standard library first, small packages,
  narrow interfaces, and explicit tests.
- If external dependencies are added, keep them justified and run `go mod tidy`.

## Delivery

Work on feature branches and open ready-for-review PRs. Do not open draft PRs
unless Roger explicitly requests draft mode.
