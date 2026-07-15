# Dispatcher recovery contract

`github-task-dispatcher recover` is the only supported executable workflow for
a validated restored dispatcher SQLite database. It is designed to run while
the normal dispatcher service is stopped. It never launches broker work.

## Caller contract

The caller must provide a dispatcher configuration whose `database_path` is the
validated restored regular file, whose `durable` is the durable to reset, and
whose `recovery_start_sequence` is exactly the manifest
`last_persisted_jetstream_sequence + 1`. The configuration must retain the
normal private NATS and exact broker profile URL settings. For execution, the
configured `broker_token_env` must contain the caller/profile-scoped status
credential.

Run the read-only validation first:

```sh
github-task-dispatcher recover \
  --config /etc/signal-plane/dispatcher.yaml \
  --manifest-last-sequence 1234 \
  --recovery-id restore-2026-07-13T120000Z
```

The result has `"mode":"dry-run"` and `"status":"validated"`. This form opens
SQLite read-only, does not connect to NATS or the broker, does not reset a
consumer, and does not write recovery evidence.

After an explicitly approved recovery action, run the same command with
`--execute`:

```sh
github-task-dispatcher recover \
  --config /etc/signal-plane/dispatcher.yaml \
  --manifest-last-sequence 1234 \
  --recovery-id restore-2026-07-13T120000Z \
  --execute
```

Execution performs these steps in order:

1. Revalidates that the restored SQLite checkpoint equals `1234`, migrates the
   database to the current supported schema if necessary, and writes an
   `incomplete` recovery marker with start sequence `1235`.
2. Freezes the set of restored nonterminal jobs. Every one must be `launched`
   with a nonempty persisted broker run ID; pending/retry state cannot be
   proven through the read-only status API and fails closed.
3. Deletes and recreates the configured durable consumer at sequence `1235`,
   snapshots its pending count, and processes exactly that bounded backlog.
   Each unique replayed stream sequence is recorded atomically with dispatcher
   state before synchronous acknowledgement.
4. Calls only authenticated `GET /v1/runs/{run_id}` for every frozen restored
   job. All responses are validated before reconciliation writes begin.
5. Records each prior status, broker status, reconciled status, replay count,
   durable, manifest sequence, and start sequence, then marks recovery
   `completed` and emits the evidence as JSON.

Auth/authz/validation errors, HTTP conflicts (including an idempotency
conflict), malformed JSON, mismatched run IDs, unknown broker states, NATS
errors, and SQLite errors leave the marker incomplete. Normal dispatcher
startup checks the database before creating its consumer or worker: it refuses
any incomplete marker, and a nonzero configured `recovery_start_sequence` also
requires matching completed evidence for that durable and start sequence.

An interrupted execution may be rerun with exactly the same recovery ID and
parameters. The frozen restored-job set and replay sequence evidence are
idempotent. A completed ID cannot be reused or retargeted. Keep the emitted JSON
with the restore evidence; SQLite retains the same evidence in
`recovery_runs`, `recovery_replayed_messages`, and
`recovery_reconciliations`.

## Local deterministic proof

```sh
mise run proof:dispatcher-recovery
```

The proof uses an in-process file-backed NATS JetStream server, SQLite, and a
fake status broker. It covers consumer reset, checkpoint + 1 replay, replay
counting, terminal reconciliation, evidence, and startup blocking on auth or
malformed status failures. It has no production or GitHub side effects.
