#!/usr/bin/env python3
"""Offline validator for the Stage-5 shadow-admission invariants.

Shadow admission (agent-platform-coupling.md, Sequencing step 5) admits a
source-neutral WorkItem candidate into the ledger as a DRY RUN: it dedupes and
persists, but never launches, never enqueues to the legacy launcher, and never
mutates legacy dispatcher tables. Two invariants must hold, and this check
enforces them offline against source (runnable from either side of the seam in
CI, no live system):

  1. No image / release / generation selection at admission. The shadow admit
     path must invoke AgentBinding.RejectAdmissionSelection before persisting.

  2. One active launcher. The shadow seam must not itself be a launcher: its
     package must not import the dispatcher/broker launch machinery, must not
     call the ledger Claim path, and must not touch legacy dispatcher tables.
     If it did any of these, it would be a second live launcher beside the
     legacy one — exactly what the sequenced cutover forbids until a single
     active launcher is chosen.

A drift in either (a shadow path that stops refusing selection, or a shadow
package that grows a launch/claim/dispatcher-table dependency) fails the gate
instead of shipping a second launcher.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SHADOW = ROOT / "internal" / "shadowadmit" / "shadowadmit.go"

# Substrings that would make the shadow seam a launcher or a legacy-table mutator.
# Kept literal so the check is obvious and easy to audit.
FORBIDDEN = (
    ".Claim(",                 # ledger claim path = starting work
    "internal/dispatcher",     # legacy launcher / broker package
    "repository_ci_tasks",     # legacy dispatcher tables (write or read)
    "repository_ci_attempts",
    "repository_run_external_waits",
    "os/exec",                 # process spawn
    "AdmitRelease",            # release path is not the shadow seam's business
)


def main() -> int:
    if not SHADOW.exists():
        print(f"ERROR: missing {SHADOW}")
        return 1
    src = SHADOW.read_text(encoding="utf-8")
    errors: list[str] = []

    # Invariant 1: no image/generation selection at admission.
    if "RejectAdmissionSelection()" not in src:
        errors.append(
            "shadowadmit must call AgentBinding.RejectAdmissionSelection() so a "
            "caller cannot supply image/release/generation selection at admission"
        )

    # Invariant 2: one active launcher — the shadow seam is not a launcher.
    for token in FORBIDDEN:
        if token in src:
            errors.append(
                "shadow admission must never launch, enqueue, or touch legacy "
                f"dispatcher tables: forbidden reference {token!r} found in "
                "internal/shadowadmit/shadowadmit.go"
            )

    # The seam must be admission-only and inert by default: it admits through
    # AdmitWithAgent and defaults to disabled.
    if "AdmitWithAgent(" not in src:
        errors.append(
            "shadowadmit must admit through workledger.AdmitWithAgent (the "
            "dry-run admission path)"
        )
    if "ErrDisabled" not in src:
        errors.append(
            "shadowadmit must be disabled-by-default and return ErrDisabled when "
            "the seam is not enabled"
        )
    # The Outcome must never report a launch from the shadow seam.
    if "Launched:             false" not in src and "Launched: false" not in src:
        errors.append(
            "shadowadmit Outcome must always report Launched=false; the shadow "
            "seam never launches"
        )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Shadow-admission invariants (no selection at admission; one active launcher) hold.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
