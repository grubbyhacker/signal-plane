#!/usr/bin/env python3
"""Validate that the image packages every command entrypoint.

Regression guard for the Stage-5 gap where cmd/workitem-shadow-ingress existed,
compiled under `mise run build`, and even merged to main, yet the Dockerfile
never built or shipped it — so the published image (and its vps-ops digest pin)
could never run the binary. `go build` passing is not evidence the image ships
the binary; only the Dockerfile is.

This check is offline and deterministic: it enumerates ./cmd/* on disk and
asserts each command is (a) compiled in the build stage and (b) copied to
/usr/local/bin in the runtime stage. It does not touch the network or Docker.
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"
CMD_DIR = ROOT / "cmd"


def commands() -> list[str]:
    return sorted(
        p.name
        for p in CMD_DIR.iterdir()
        if p.is_dir() and (p / "main.go").is_file()
    )


def main() -> int:
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")
    errors: list[str] = []

    cmds = commands()
    if not cmds:
        print("ERROR: no ./cmd/*/main.go entrypoints found")
        return 1

    for name in cmds:
        build_marker = f"-o /out/{name} ./cmd/{name}"
        copy_marker = f"/out/{name} /usr/local/bin/{name}"
        if build_marker not in dockerfile:
            errors.append(
                f"Dockerfile build stage must compile ./cmd/{name} "
                f"(missing `{build_marker}`)"
            )
        if copy_marker not in dockerfile:
            errors.append(
                f"Dockerfile runtime stage must ship {name} "
                f"(missing `COPY --from=build {copy_marker}`)"
            )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print(
        "Dockerfile packaging contract holds: "
        f"all {len(cmds)} command(s) are built and shipped ({', '.join(cmds)})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
