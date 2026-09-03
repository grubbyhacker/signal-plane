# Repository CI repair lifecycle

The repository task dispatcher treats a broker `ready_for_review` result as a
durable transition into `waiting_ci`, not as a terminal success. It records the
originating issue, pull request, branch, delivered head, reviewed profile, and
one absolute repair deadline before acknowledging further work.

Admitted `check_run`, `check_suite`, commit `status`, and `pull_request` events
are wake-ups. Their payloads never decide CI. Each distinct delivery and
semantic payload identity coalesces into an idempotent reconciliation against
the broker's authoritative GitHub observation. The observation is usable only
when `requested_head_sha` and `pull.head_sha` both equal the durable requested
head. There is no periodic CI polling; the absolute deadline adds one final
authoritative reconciliation when events are missing or stuck.

An authoritative success completes silently. An infrastructure failure creates
one issue-escalation outbox operation without launching or charging coding
work. A code failure authorizes at most two model executions for the reviewed
profile. Every attempt has a stable identity derived from the durable task,
pull, admitted head, profile, and ordinal. Broker launch replay uses that exact
identity, and an attempt is charged only when the terminal projection says
`model_execution_started: true`.

A successful repair binds `expected_old_head_sha`, `candidate_head_sha`,
`validated_tree_sha`, `delivered_head_sha`, and `delivered_tree_sha`; the two
tree identities must match. Broker-internal stale-lease recovery remains the
same running attempt, so the final expected-old head may be an intervening
winner. Signal Plane then waits for authoritative CI on the exact delivered
head. Deadline expiry or attempt exhaustion leaves the pull request open and
delivers one terse, idempotent escalation to the originating issue.

The SQLite schema stores event, reconciliation, attempt, charge, delivery, and
reporting identities. Process restart therefore resumes the same broker run or
outbox operation instead of duplicating model execution, pushes, or comments.
