#!/usr/bin/env python3
"""Validate immutable image deployment and retired proof-route boundaries."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLISH = ROOT / ".github" / "workflows" / "publish-image.yml"
DEPLOY = ROOT / ".github" / "workflows" / "deploy-production.yml"


def main() -> int:
    publish = PUBLISH.read_text(encoding="utf-8")
    deploy = DEPLOY.read_text(encoding="utf-8")

    required_publish = "type=sha,prefix=sha-,format=long"
    required_deploy_fragments = (
        "ghcr.io/grubbyhacker/signal-plane:sha-"
        "${{ steps.deploy_inputs.outputs.deploy_sha }}",
        "docker buildx imagetools inspect \"$IMAGE_TAG\" --format '{{.Manifest.Digest}}'",
        "image=ghcr.io/grubbyhacker/signal-plane@${digest}",
        'signal_plane_image=${{ steps.deploy_image.outputs.image }}',
        "VPS_OPS_GH_BROKER_FLEIGLABS_RELEASE_READER_APP_PEM: ${{ secrets.VPS_OPS_GH_BROKER_FLEIGLABS_RELEASE_READER_APP_PEM }}",
        "VPS_OPS_GH_BROKER_FLEIGLABS_RELEASE_READER_WEBHOOK_SECRET: ${{ secrets.VPS_OPS_GH_BROKER_FLEIGLABS_RELEASE_READER_WEBHOOK_SECRET }}",
        "VPS_OPS_SIGNAL_PLANE_YKM_CF_ACCESS_CLIENT_SECRET: ${{ secrets.VPS_OPS_SIGNAL_PLANE_YKM_CF_ACCESS_CLIENT_SECRET }}",
    )
    forbidden_deploy_fragments = (
        "VPS_OPS_SIGNAL_PLANE_DISPATCHER_BROKER_TOKEN",
    )

    errors: list[str] = []
    if required_publish not in publish:
        errors.append(
            "publish-image.yml must publish sha-<full 40-character commit> "
            "with docker metadata format=long"
        )
    for fragment in required_deploy_fragments:
        if fragment not in deploy:
            errors.append(
                "deploy-production.yml must resolve the full-SHA tag and pass its immutable digest: "
                f"missing {fragment!r}"
            )
    for fragment in forbidden_deploy_fragments:
        if fragment in deploy:
            errors.append(
                "deploy-production.yml must not export retired proof-route secrets: "
                f"found {fragment!r}"
            )

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 1

    print("Image deployment and retired proof-route secret contracts are aligned.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
