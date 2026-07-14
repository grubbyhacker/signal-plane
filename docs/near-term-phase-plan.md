# Near-Term Phase Plan

## Phase 1: local event plane

Prove local flow without GitHub, Cloudflare, Hermes, or a job controller.

- Scaffold `signal-gateway` and `signal-observer`.
- Run local NATS JetStream.
- Publish a thin envelope with raw payload.
- Subscribe with an observer and log accepted signals.
- Expose health, readiness, and metrics.

Definition of done:

- `curl -> signal-gateway -> JetStream -> signal-observer` is visible locally.
- `mise run check` passes.

## Phase 2: GitHub-compatible receiver

Prove GitHub webhook admission locally.

- Verify `X-Hub-Signature-256` against the raw body.
- Use constant-time comparison.
- Handle `ping` without publishing by default.
- Filter by repository, event, and action.
- Count rejects by reason.

Definition of done:

- Valid GitHub fixtures are accepted.
- Invalid signatures and disallowed actions are rejected.
- The raw provider payload is preserved inside the signal envelope.

## Phase 3: local staging through vps-ops

Deploy the event plane through the local staging path.

- Add the combined `signal-plane` role in `vps-ops`.
- Keep NATS private.
- Decide metrics scrape topology.
- Stimulate the gateway from the host and watch observer logs.

Definition of done:

- `mise run deploy:staging -- signal-plane` deploys gateway, observer, and NATS.

## Phase 4: secure public ingress

Expose only the webhook path through Cloudflare Tunnel.

- Add Cloudflare hostname/path routing.
- Add WAF/rate-limit/source-range controls where practical.
- Wire Doppler-provided webhook secret into the deployed gateway.
- Prove a public request reaches NATS and the observer.

Definition of done:

- GitHub `ping` reaches the gateway and is verified.
- Real pull request events are accepted, carried through JetStream, and logged.
- No agent, Hermes, or job controller behavior is involved.

## Phase 5: narrow GitHub issue dispatch (historical proof, route retired)

- Run the disabled-by-default `github-task-dispatcher` as a separate binary.
- Consume GitHub envelopes with an independent durable JetStream consumer.
- Transactionally deduplicate deliveries and semantic issue jobs in SQLite WAL
  before acknowledging transport delivery.
- POST bounded parameters only to the broker's fixed
  `/v1/launch-profiles/codex-issue-implement/launch` endpoint, validate fresh
  and replayed launch responses, persist the returned run ID immediately, and
  use a durable ten-minute launch retry window.
- Serialize launches and durably poll scoped run status through explicit
  pending, retry, launched, and terminal lifecycle states.
- Store the accepted JetStream sequence for restore/replay recovery without
  storing provider payload or prose.
- Execute restore recovery through a dry-run-first CLI that resets the durable
  at checkpoint + 1, records bounded replay evidence, reconciles restored runs,
  and keeps normal startup blocked until the recovery marker is complete.
- Keep all provider prose and payloads out of dispatcher persistence.
- Retain a neutral `example/automation-target` fixture so the dispatcher,
  persistence, idempotency, serialization, status, and recovery tests remain
  runnable without a production repository route.

Retirement state:

- No production GitHub admission or webhook registration targets the synthetic
  repository.
- No production broker identity is authorized for the synthetic repository.
- The proof dispatcher remains disabled and is not the future generalized
  Signal Plane router.

The settled generalized implementation sequence lives in
`vps-ops/docs/repository-agent-automation-roadmap.md`.
