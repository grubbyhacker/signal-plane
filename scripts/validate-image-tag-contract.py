#!/usr/bin/env python3
"""Validate that image publication and deployment use the same immutable tag."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLISH = ROOT / ".github" / "workflows" / "publish-image.yml"
DEPLOY = ROOT / ".github" / "workflows" / "deploy-production.yml"


def main() -> int:
    publish = PUBLISH.read_text(encoding="utf-8")
    deploy = DEPLOY.read_text(encoding="utf-8")

    required_publish = "type=sha,prefix=sha-,format=long"
    required_deploy = (
        "ghcr.io/grubbyhacker/signal-plane:sha-"
        "${{ steps.deploy_inputs.outputs.deploy_sha }}"
    )

    errors: list[str] = []
    if required_publish not in publish:
        errors.append(
            "publish-image.yml must publish sha-<full 40-character commit> "
            "with docker metadata format=long"
        )
    if required_deploy not in deploy:
        errors.append(
            "deploy-production.yml must deploy sha-${{ steps.deploy_inputs.outputs.deploy_sha }}"
        )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Image publish/deploy tag contract is aligned on the full commit SHA.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
