#!/usr/bin/env python3
"""Offline validator for the deployment-owned route resolver.

Per agent-platform-coupling ("Selection authority"): a deployment-owned route
maps a domain FACT to (agent_type, mode) only. The resolver must not select an
image, release, or generation; must return no match for an unknown fact; and
must wire to shadow admission / inert ingress ONLY — never a launcher, broker,
or legacy dispatcher mutation.

This check runs offline against the package source (no live system, runnable
from either side of the seam in CI). It fails if the resolver grows a launch or
selection dependency, so a regression is caught before it ships.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PKG = ROOT / "internal" / "routeresolver"
GO_SOURCES = sorted(p for p in PKG.glob("*.go") if not p.name.endswith("_test.go"))

# References that would make the resolver a launcher, a broker client, or a
# legacy-table mutator, or let it select an image/release/generation.
FORBIDDEN = (
    "internal/dispatcher",   # legacy launcher / broker package
    ".Claim(",               # ledger claim path = starting work
    "repository_ci_tasks",   # legacy dispatcher tables
    "os/exec",               # process spawn
    "net/http",              # no network client/listener
    'net.Listen',            # no listener of its own
    "ResolvedReleaseGeneration",  # resolver never sets a generation
    "ResolvedReleaseDigest",      # resolver never sets a digest
    "BrokerRunID",                # resolver never sets a broker run
    "AuthoritativePR",            # resolver never sets PR correlation
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
                    "route resolver must map facts to agent_type/mode only and never "
                    f"launch or select image/release/generation: forbidden {token!r} in {path.name}"
                )

    # It must implement the ingress seam and build the admission-safe binding
    # from route data (agent_type, mode, contract revision) only.
    if "shadowingress.Resolution" not in joined:
        errors.append("route resolver must produce a shadowingress.Resolution (the ingress seam)")
    if "Matched: false" not in joined:
        errors.append("route resolver must return no match (Matched=false) for an unknown fact")
    # The route config must carry only fact -> (agent_type, mode) + snapshot id.
    for field in ("AgentType", "Mode", "RouteSnapshotID"):
        if field not in joined:
            errors.append(f"route config must carry {field}")
    # It must validate modes against an injected catalog interface, not import
    # an AgentType implementation.
    if "ModeCatalog" not in joined or "Declares(" not in joined:
        errors.append("route resolver must validate modes against an injected ModeCatalog interface")
    # A stable, deployment-owned revision must be computed.
    if "func (config Config) Revision()" not in joined:
        errors.append("route resolver must compute a stable config revision")
    # It must guard against a smuggled selection field in config.
    if "forbiddenFields" not in joined:
        errors.append("route resolver must reject config selection fields (forbiddenFields guard)")

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Route-resolver invariants (facts-only, no launch/broker/dispatcher, catalog-validated, stable revision) hold.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
