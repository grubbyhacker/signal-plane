#!/usr/bin/env python3
"""Deterministic single-writer validator for the operational work ledger.

The redesign makes github-task-dispatcher the SOLE process that opens the
operational work-ledger SQLite database for WRITING. Exactly one process opening
the database is the invariant that removes the multi-writer hazard on the shared
0640 SQLite files (the failure the standalone-ingress / route-activation-writer
topology hit: several processes writing one database).

This check is offline and deterministic (source token scan over the compiled
tree; no network, no Docker, no database). It proves the property two ways:

  1. WRITABLE-OPEN CALL SITES. The only functions that open the work ledger for
     writing are `workledger.Open(` (owns a fresh writable *sql.DB) and
     `dispatcher.OpenStore(` (opens + migrates the same database writable).
     Their read-only siblings (`workledger`/`dispatcher.OpenStoreReadOnly`, and
     `workledger.Attach`, which wraps an already-open handle and opens nothing)
     are permitted anywhere. Every writable-open call site in a cmd/ package
     must live in the ONE allowed command: cmd/github-task-dispatcher.

  2. NO OTHER COMMAND OPENS IT. Every other cmd/*/main.go (and its command
     package) must contain zero writable-open call sites against the work
     ledger. A command that needs work-ledger state must receive a handle from
     the dispatcher (Attach), not open the database itself.

If a future command legitimately owns a DIFFERENT database (e.g.
push-security-scanner opens its own push-security-scanner.db via
pushscan.OpenStore), that is not a work-ledger open and is not matched here:
this validator matches only workledger.Open / dispatcher.OpenStore.
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
CMD_DIR = ROOT / "cmd"

# The single command permitted to open the work-ledger database writable.
SOLE_WRITER_COMMAND = "github-task-dispatcher"

# Writable-open call sites for the OPERATIONAL work-ledger database. Read-only
# openers and Attach (which opens nothing) are deliberately excluded.
WRITABLE_OPEN_TOKENS = (
    "workledger.Open(",
    "dispatcher.OpenStore(",
)

# Tokens that look similar but are NOT writable work-ledger opens, so a naive
# substring match must not count them. (OpenStoreReadOnly contains OpenStore.)
BENIGN_SUBSTRINGS = (
    "OpenStoreReadOnly(",
    "workledger.Attach(",
)


def go_sources(directory: Path) -> list[Path]:
    return sorted(
        p
        for p in directory.rglob("*.go")
        if not p.name.endswith("_test.go")
    )


def writable_open_hits(path: Path) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if any(benign in line for benign in BENIGN_SUBSTRINGS):
            # A line may still contain a real Open( alongside a benign token; be
            # strict and only skip when the ONLY matches are benign.
            stripped = line
            for benign in BENIGN_SUBSTRINGS:
                stripped = stripped.replace(benign, "")
            if not any(token in stripped for token in WRITABLE_OPEN_TOKENS):
                continue
            line = stripped
        for token in WRITABLE_OPEN_TOKENS:
            if token in line:
                hits.append((lineno, line.strip()))
                break
    return hits


def main() -> int:
    if not CMD_DIR.is_dir():
        print("ERROR: cmd/ directory not found")
        return 1

    errors: list[str] = []
    sole_writer_has_open = False

    for cmd_path in sorted(p for p in CMD_DIR.iterdir() if p.is_dir()):
        command = cmd_path.name
        for source in go_sources(cmd_path):
            hits = writable_open_hits(source)
            if not hits:
                continue
            rel = source.relative_to(ROOT)
            if command == SOLE_WRITER_COMMAND:
                sole_writer_has_open = True
                continue
            for lineno, text in hits:
                errors.append(
                    f"{rel}:{lineno} opens the work ledger writable "
                    f"({text!r}); only cmd/{SOLE_WRITER_COMMAND} may. Receive a "
                    f"handle from the dispatcher (workledger.Attach) instead."
                )

    if not sole_writer_has_open:
        errors.append(
            f"cmd/{SOLE_WRITER_COMMAND} must open the work ledger writable "
            "(expected a workledger.Open or dispatcher.OpenStore call site); "
            "the single-writer owner opened nothing."
        )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(
        "Single-writer contract holds: cmd/"
        f"{SOLE_WRITER_COMMAND} is the sole command that opens the work-ledger "
        "database writable; every other command opens no writable handle."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
