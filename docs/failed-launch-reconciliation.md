# Failed launch reconciliation

`github-task-dispatcher reconcile-failed-launch` is the supported operator path
for an older semantic job that exhausted launch retries even though the sandbox
broker durably accepted and terminalized the idempotent launch intent.

The command defaults to a read-only plan. It fetches the bounded terminal result
for the exact broker run, verifies its profile and repository against the
semantic job, and refuses jobs that are not `failed` with an empty broker run.
It also refuses any job that already has terminal-result or outbox state.

```sh
github-task-dispatcher reconcile-failed-launch \
  --config /etc/signal-plane/dispatcher.yaml \
  --job-id 6 \
  --broker-run-id 20260731T005719Z-c854bf423735ec8f
```

After reviewing the JSON plan, repeat the exact command with `--execute`.
Execution only binds the broker run and moves the job to normal status polling.
The dispatcher then retrieves the terminal result and creates and delivers the
normal durable notification outbox item. The reconciliation command never
launches a worker or posts directly to GitHub.
