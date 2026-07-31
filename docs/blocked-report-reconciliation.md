# Blocked report reconciliation

`github-task-dispatcher reconcile-report` is the supported operator path for
retrying a permanent repository-task comment failure after its cause has been
corrected. It is deliberately narrower than dispatcher recovery and does not
launch or reconcile a broker run.

The command requires the durable semantic job ID, broker run ID, and existing
notification idempotency key. It fails unless all three values match one
terminal result and one undelivered `blocked` outbox row with complete failure
evidence and no comment identity.

Run the read-only plan first:

```sh
github-task-dispatcher reconcile-report \
  --config /etc/signal-plane/dispatcher.yaml \
  --job-id 5 \
  --broker-run-id 20260730T174325Z-955b6e98f4da20df \
  --idempotency-key repository-task-terminal:v1:<digest>
```

The JSON result reports `mode: plan` and `status: validated`, along with the
semantic key, terminal-result version, outbox ID, prior attempt count, and
sanitized prior error. The plan opens SQLite read-only.

After reviewing that evidence, repeat the exact command with `--execute`. The
transaction changes only the matching job from `report_blocked` to
`report_retry` and its outbox row from `blocked` to `retry`. It preserves the
terminal result, rendered body, idempotency key, attempt count, and prior error.
The normal dispatcher worker then claims the durable row and posts through the
dedicated least-privilege broker principal.

The command refuses rows that are not blocked, have incomplete or conflicting
correlation, or already contain a comment ID, URL, or delivery timestamp.
