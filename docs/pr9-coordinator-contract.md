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
