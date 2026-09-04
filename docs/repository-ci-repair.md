# Repository CI repair lifecycle

The repository task dispatcher treats a broker `ready_for_review` result as a
durable transition into `waiting_ci`, not as a terminal success. It records the
originating issue, pull request, branch, delivered head, and reviewed profile
before acknowledging further work. New configuration separates
`repair_active_timeout`, which bounds one active broker phase, from
`repair_reconciliation_wake`, which covers missing events. Neither is an
end-to-end completion cutoff. The old `repair_deadline` remains a rolling-upgrade
alias; its active value is capped to the reviewed 60-minute broker template.

Admitted `check_run`, `check_suite`, commit `status`, and `pull_request` events
are wake-ups. Their payloads never decide CI. Each distinct delivery and
semantic payload identity coalesces into an idempotent reconciliation against
the broker's authoritative GitHub observation. The observation is usable only
when `requested_head_sha` and `pull.head_sha` both equal the durable requested
head. There is no periodic CI polling. A durable wake requests another
authoritative reconciliation when events are missing or stuck. Pending CI
renews that wake. GitHub unavailability and failed observation transport remain
durably retryable without terminating the task or consuming a coding attempt.

An authoritative success completes silently. A terminal infrastructure
aggregate creates one issue-escalation operation without launching or charging
coding work. A code failure authorizes at most two model executions for the
reviewed profile. Every attempt has a stable identity derived from the durable
task, pull, admitted head, profile, and ordinal. Broker launch replay uses that
exact identity and the exact durably recorded `max_runtime_seconds`; an attempt
is charged only when the terminal projection says
`model_execution_started: true`.

A successful repair binds `expected_old_head_sha`, `candidate_head_sha`,
`validated_tree_sha`, `delivered_head_sha`, and `delivered_tree_sha`; the two
tree identities must match. Broker-internal stale-lease recovery remains the
same running attempt, so the final expected-old head may be an intervening
winner. Signal Plane then waits for authoritative CI on the exact delivered
head. Only coding-attempt exhaustion or a nonretryable semantic failure leaves
the pull request open and delivers one terse, idempotent escalation to the
originating issue. Time spent waiting for GitHub, CI, or a durable broker result
does not make the lifecycle terminal.

Broker status `waiting_external` is a nonterminal, structured GitHub outage
state from the reviewed broker contract. Signal Plane durably records its
service, phase, operation, reason, generation, timestamp, and semantic resume
key before acting. For an admitted repair, a newly admitted GitHub event or a
bounded durable wake triggers the exact pull/head observation; Signal resumes
only after that authoritative endpoint responds (including a stale-head
response). For an initial preparation wait, a newly admitted authenticated
GitHub issue event for the same repository and originating issue is a wake, not
proof of availability; the resumed broker preparation performs the definitive
issue read and may safely enter the next external-wait generation. Signal posts
the generation-bound resume key and persisted active runtime to the same broker
run. Preparation restarts before model issuance. Delivery reuses the broker's
sealed candidate and never starts Codex again.

The SQLite schema stores event, reconciliation, attempt, charge, delivery, and
reporting identities. Process restart therefore resumes the same broker run or
outbox operation instead of duplicating model execution, pushes, or comments.

The `waiting_external` and resume DTO is supplied by gh-agent-broker PR #169 at
head `5fc55a6d935b885e8cba0bdccc5b4b33616ac6fe`.
