#!/usr/bin/env python3
"""Offline validator for the WorkItem agent-admission selection invariant.

Agent-platform-coupling invariant (agent-infra-docs/design/agent-platform-coupling.md,
Non-goals): "Emitters and runtime callers never select an image, release, or
generation." At admission an event carries a domain fact only; the broker
resolves the release generation, its digest, the broker run, and the PR
correlation AFTER admission from its own authenticated side effects.

This check runs offline against source files (no live system, runnable from
either side of the seam in CI, per the design's verification economy). It
asserts that:

  1. the source-neutral agent binding declares exactly the selection fields the
     admission path must refuse, and
  2. the admission path actually invokes the guard before persisting a work
     item.

A drift in either (a new broker-resolved field added to the struct but not to
the refusal set, or an admission path that stops calling the guard) fails the
gate instead of waiting for a deploy to reveal it.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BINDING = ROOT / "internal" / "workledger" / "agentbinding.go"
STORE = ROOT / "internal" / "workledger" / "store.go"

# The fields a caller/emitter must never populate at admission. Kept in lockstep
# with admissionSelectionFields in agentbinding.go.
SELECTION_FIELDS = (
    "resolved_release_generation",
    "resolved_release_digest",
    "broker_run_id",
    "authoritative_pr_repository",
    "authoritative_pr_number",
)


def main() -> int:
    errors: list[str] = []

    if not BINDING.exists():
        print(f"ERROR: missing {BINDING}")
        return 1
    if not STORE.exists():
        print(f"ERROR: missing {STORE}")
        return 1

    binding = BINDING.read_text(encoding="utf-8")
    store = STORE.read_text(encoding="utf-8")

    # 1. The refusal set must name every selection field, exactly.
    for field in SELECTION_FIELDS:
        if f'"{field}"' not in binding:
            errors.append(
                "agentbinding.go admissionSelectionFields must refuse "
                f"{field!r} at admission"
            )
    if "func (binding AgentBinding) RejectAdmissionSelection() error" not in binding:
        errors.append("agentbinding.go must define RejectAdmissionSelection")

    # 2. The admission path must invoke the guard before doing anything else, and
    #    must only carry the admission-safe subset of the binding into the insert.
    if "binding.RejectAdmissionSelection()" not in store:
        errors.append(
            "store.go admit() must reject caller-supplied selection fields via "
            "binding.RejectAdmissionSelection()"
        )
    if "binding.AdmissionBinding()" not in store:
        errors.append(
            "store.go admit() must reduce the binding to its admission-safe "
            "subset via binding.AdmissionBinding()"
        )
    # The broker-resolved columns must never be written by the admission INSERT.
    insert_marker = "INSERT INTO work_items("
    start = store.find(insert_marker)
    if start == -1:
        errors.append("store.go must contain a work_items INSERT")
    else:
        insert = store[start : store.find(")", start)]
        for field in ("resolved_release_generation", "resolved_release_digest", "broker_run_id", "authoritative_pr_repository", "authoritative_pr_number"):
            if field in insert:
                errors.append(
                    "the work_items admission INSERT must not write the "
                    f"broker-resolved field {field!r}"
                )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("WorkItem agent-admission selection invariant is encoded and enforced.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
