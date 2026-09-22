#!/usr/bin/env python3
"""Offline validator for the managed route-activation command.

workitem-route-activate installs a deployment-owned route snapshot through the
existing Store.ActivateRoute API and emits the STORE-MINTED route_snapshot_id.
The invariants it must hold:

  - It must NOT reach a launcher, the legacy dispatcher, a broker, or open a
    listener/network client -- not even transitively -- or the "no launch"
    guarantee would leak in through this executable.
  - It must go through Store.ActivateRoute and must NOT fabricate a route
    snapshot row with raw SQL (no INSERT/UPDATE against route_snapshots in the
    command package).
  - It must never accept a caller-supplied route_snapshot_id.

The import-closure half is proven with `go list -deps` (deterministic, offline);
the source half is a token scan over the command package.
"""

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "internal" / "routeactivatecmd"
GO_SOURCES = sorted(p for p in PKG.glob("*.go") if not p.name.endswith("_test.go"))

FORBIDDEN_IMPORT_SUBSTRINGS = (
    "internal/dispatcher",  # legacy launcher + broker client
    "internal/recovery",    # dispatcher recovery/reconciliation
    "internal/pushscan",    # unrelated scanner
    "internal/shadowingress",  # network/socket ingress; activation opens none
)

# Tokens that would mean the command fabricated a row or grew a launch/network
# dependency instead of going through the store API.
FORBIDDEN_TOKENS = (
    "INSERT INTO route_snapshots",
    "UPDATE route_snapshots",
    "os/exec",
    "net/http",
    "net.Listen",
    ".Claim(",
    ".ResolveRelease(",
)

TARGETS = (
    "./cmd/workitem-route-activate",
    "./internal/routeactivatecmd",
)


def deps(target: str) -> list[str]:
    out = subprocess.run(
        ["go", "list", "-deps", "-json", target],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "GOCACHE": str(ROOT / ".gocache"), "GOMODCACHE": str(ROOT / ".gomodcache")},
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
    if not GO_SOURCES:
        print(f"ERROR: no Go sources under {PKG}")
        return 1
    errors: list[str] = []

    # Import-closure invariants.
    for target in TARGETS:
        for pkg in deps(target):
            for forbidden in FORBIDDEN_IMPORT_SUBSTRINGS:
                if forbidden in pkg:
                    errors.append(f"{target} transitively imports forbidden package {pkg}")
    closure = set(deps("./internal/routeactivatecmd"))
    if not any("internal/workledger" in p for p in closure):
        errors.append("route-activate command must import internal/workledger")

    # Source invariants over the command package.
    joined = ""
    for path in GO_SOURCES:
        text = path.read_text(encoding="utf-8")
        joined += text
        for token in FORBIDDEN_TOKENS:
            if token in text:
                errors.append(f"route-activate command must not contain {token!r} (found in {path.name})")

    if "store.ActivateRoute(" not in joined:
        errors.append("route-activate command must activate through Store.ActivateRoute")
    # The store, not the caller, mints the id: the command reports snapshot.ID
    # and must never read a snapshot id from its own inputs.
    if "RouteSnapshotID string" in joined and "opts.RouteSnapshotID" in joined:
        errors.append("route-activate command must NEVER accept a caller-supplied route_snapshot_id")

    if errors:
        for error in sorted(set(errors)):
            print(f"ERROR: {error}")
        return 1
    print("Route-activate command invariants hold (Store.ActivateRoute only, store-minted id, no launch/dispatcher/broker/network, no fabricated rows).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
