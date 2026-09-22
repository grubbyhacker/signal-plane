#!/usr/bin/env python3
"""Offline validator for the shadow-ingress command's import closure.

The command (cmd/workitem-shadow-ingress + internal/shadowingresscmd) is process
glue for the inert shadow stack. It must NOT be able to reach a launcher, the
legacy dispatcher, or a broker — not even transitively — or the "no launch"
guarantee of the whole shadow stack would leak in through the executable.

This check computes the full transitive import set with `go list -deps` (a
deterministic, offline compile-graph query — no network) and fails if any
forbidden package appears. It is the strongest form of the invariant: it proves
the property over the entire dependency closure, not just the files in one
package.
"""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Packages the shadow-ingress command must never import, transitively.
FORBIDDEN_SUBSTRINGS = (
    "internal/dispatcher",  # legacy launcher + broker client live here
    "internal/recovery",    # dispatcher recovery/reconciliation
    "internal/pushscan",    # unrelated scanner
)

TARGETS = (
    "./cmd/workitem-shadow-ingress",
    "./internal/shadowingresscmd",
)


def deps(target: str) -> list[str]:
    out = subprocess.run(
        ["go", "list", "-deps", "-json", target],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**__import__("os").environ, "GOCACHE": str(ROOT / ".gocache"), "GOMODCACHE": str(ROOT / ".gomodcache")},
    )
    if out.returncode != 0:
        print(f"ERROR: go list failed for {target}:\n{out.stderr}")
        sys.exit(1)
    pkgs = []
    decoder = json.JSONDecoder()
    text = out.stdout.strip()
    idx = 0
    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        obj, end = decoder.raw_decode(text, idx)
        idx = end
        if "ImportPath" in obj:
            pkgs.append(obj["ImportPath"])
    return pkgs


def main() -> int:
    errors: list[str] = []
    for target in TARGETS:
        for pkg in deps(target):
            for forbidden in FORBIDDEN_SUBSTRINGS:
                if forbidden in pkg:
                    errors.append(f"{target} transitively imports forbidden package {pkg}")
    # It MUST import the shadow stack it wires.
    closure = set(deps("./internal/shadowingresscmd"))
    for required in ("internal/shadowadmit", "internal/shadowingress", "internal/routeresolver", "internal/workledger"):
        if not any(required in p for p in closure):
            errors.append(f"shadow-ingress command must import {required}")

    if errors:
        for error in sorted(set(errors)):
            print(f"ERROR: {error}")
        return 1
    print("Shadow-ingress command import closure holds (no launcher/dispatcher/broker; wires the shadow stack).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
