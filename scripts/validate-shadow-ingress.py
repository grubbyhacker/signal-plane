#!/usr/bin/env python3
"""Offline validator for the Stage-5 shadow-INGRESS invariants.

The host ingress (agent-platform-coupling.md, "The intake path") is an
unprivileged Unix-domain socket that authenticates the local process with
filesystem permissions + SO_PEERCRED, accepts a bounded source-neutral
domain-fact envelope, and calls the shadowadmit service ONLY. It must not
launch, call a broker, mutate legacy dispatcher tables, open an external network
listener, or touch a YouKnowMe container credential path.

This check runs offline against the package source (no live system, runnable
from either side of the seam in CI). It fails if the ingress grows a launch or
network dependency, so a regression is caught before it ships rather than after
a deploy.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "internal" / "shadowingress"

GO_SOURCES = sorted(p for p in PKG.glob("*.go") if not p.name.endswith("_test.go"))

# References that would make the ingress a launcher, a network listener, or a
# legacy-table mutator. Kept literal for auditability.
FORBIDDEN = (
    "internal/dispatcher",     # legacy launcher / broker package
    ".Claim(",                 # ledger claim path = starting work
    "repository_ci_tasks",     # legacy dispatcher tables
    "repository_ci_attempts",
    "repository_run_external_waits",
    "os/exec",                 # process spawn
    'net.Listen("tcp',         # external network listener
    "net/http",                # no HTTP listener; unix socket only
    "ykm",                     # no YouKnowMe container credential path
    "youknowme",
)


def main() -> int:
    if not GO_SOURCES:
        print(f"ERROR: no Go sources under {PKG}")
        return 1

    errors: list[str] = []
    joined = ""
    for path in GO_SOURCES:
        text = path.read_text(encoding="utf-8")
        joined += text
        for token in FORBIDDEN:
            if token in text:
                errors.append(
                    "shadow ingress must not launch, listen externally, or touch "
                    f"legacy/YKM paths: forbidden reference {token!r} in {path.name}"
                )

    # It must admit through shadowadmit and via a Unix socket.
    if "internal/shadowadmit" not in joined:
        errors.append("shadow ingress must call the shadowadmit service")
    if 'net.Listen("unix"' not in joined:
        errors.append("shadow ingress must listen on a unix-domain socket")
    if "SO_PEERCRED" not in joined:
        errors.append("shadow ingress must authenticate peers with SO_PEERCRED")
    # It must admit with a zero agent binding (emitter names no agent) — the
    # server constructs the candidate with an explicit empty AgentBinding.
    if "workledger.AgentBinding{}" not in joined:
        errors.append(
            "shadow ingress must admit with a zero AgentBinding (an emitter names "
            "no agent, image, release, or generation)"
        )
    # Disabled-by-default: the server no-ops when not enabled.
    if "if !server.cfg.Enabled" not in joined:
        errors.append("shadow ingress must be disabled by default (no-op when not enabled)")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Shadow-ingress invariants (unix socket + SO_PEERCRED, no launch/network/legacy dependency) hold.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
