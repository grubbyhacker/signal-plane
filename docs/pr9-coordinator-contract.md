# PR 9 coordinator contract

The `agent_session_v1` executor is a fixed coordinator for the registered
`general-writer-v1` authority profile and `codex_adapter_v1` runtime adapter.
Route data cannot select a command, image, credential, policy, task kind, or
worker. It records normalized evidence and token use, and reports only runtime
success or waiting; verification and autonomous continuation remain PR 10.

The broker and agentd implementations are intentionally represented by narrow,
typed Go interfaces until `gh-agent-broker` PR #111 and the agentd protocol are
independently reviewed. There is no HTTP fallback or production activation.
Missing implementations fail closed.

Each durable binding records authority policy version, storage lineage, worker,
fence epoch, agentd session identity, and event cursor. Reassignment accepts a
broker-derived successor only with the same profile, policy version, and lineage
and commits through a predecessor worker/epoch CAS. Events from the previous
fence are rejected after cutover; a duplicate cursor is accepted only when its
entire normalized record is identical.

Reassignment requests carry the deterministic idempotency key
`signal-plane:agent-session:reassign:v1:<binding-key>:<predecessor-epoch>`.
The broker must replay the same successor for a repeated key and deny reuse of
that key with a different successor. This permits a coordinator crash after
broker CAS to replay the exact broker operation.

The cursor policy intentionally permits forward gaps: the durable cursor is the
highest accepted cursor, and a gap is not treated as evidence of a missing
event. Replays at or below that cursor must match the complete normalized event;
conflicts are rejected. A broker adapter maps agentd v1 `attempt_completed` and
its `tokenUsage` (`inputTokens`, `cachedInputTokens`, `outputTokens`, and
`reasoningOutputTokens`) into the normalized Signal event model. Token totals
are `inputTokens + outputTokens`; cached and reasoning values remain bounded
breakdowns. Completion is recorded as runtime evidence only—Signal Plane does
not invent task verification or mark work complete.
