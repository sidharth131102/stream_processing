#!/usr/bin/env python3

import yaml
import subprocess
import sys
from pathlib import Path
import re


PIPELINE_YAML = "config/pipeline.yaml"


def run(cmd: list):
    print("\n▶", " ".join(cmd))
    subprocess.run(cmd, check=True)


def load_pipeline_yaml():
    path = Path(PIPELINE_YAML)
    if not path.exists():
        raise FileNotFoundError(f"❌ pipeline.yaml not found at {path.resolve()}")

    with path.open("r") as f:
        return yaml.safe_load(f)


def save_pipeline_yaml(cfg):
    with open(PIPELINE_YAML, "w") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def bump_version(tag: str) -> str:
    """
    v17 → v18
    """
    match = re.match(r"v(\d+)", tag)
    if not match:
        raise ValueError(
            f"Invalid image tag format '{tag}'. Expected 'v<number>'"
        )
    return f"v{int(match.group(1)) + 1}"


def main():
    try:
        cfg = load_pipeline_yaml()

        # --------------------------------------------------
        # Extract config
        # --------------------------------------------------
        project_id = cfg["project"]["id"]
        project_region = cfg["project"]["region"]

        docker_cfg = cfg["docker"]
        registry = docker_cfg["registry"]
        image_cfg = docker_cfg["image"]

        registry_region = registry["location"]
        repository = registry["repository"]

        image_name = image_cfg["name"]
        old_tag = image_cfg["tag"]
        new_tag = bump_version(old_tag)

        dockerfile = image_cfg["dockerfile"]
        context = image_cfg["context"]

        image_uri = (
            f"{registry_region}-docker.pkg.dev/"
            f"{project_id}/{repository}/{image_name}:{new_tag}"
        )

        template_cfg = cfg["dataflow"]["template"]
        template_path = template_cfg["storage_path"]
        metadata_file = template_cfg["metadata_file"]
        sdk_language = template_cfg["sdk_language"]

        print(f"\n🔁 Bumping image version: {old_tag} → {new_tag}")

        # --------------------------------------------------
        # Update pipeline.yaml with new tag
        # --------------------------------------------------
        cfg["docker"]["image"]["tag"] = new_tag
        save_pipeline_yaml(cfg)

        # --------------------------------------------------
        # 1️⃣ Build Docker image
        # --------------------------------------------------
        run([
            "docker", "build",
            "-f", dockerfile,
            "-t", image_uri,
            context,
        ])

        # --------------------------------------------------
        # 2️⃣ Push Docker image
        # --------------------------------------------------
        run([
            "docker", "push",
            image_uri,
        ])

        # --------------------------------------------------
        # 3️⃣ Build Flex Template
        # --------------------------------------------------
        run([
            "gcloud", "dataflow", "flex-template", "build",
            template_path,
            "--image", image_uri,
            "--sdk-language", sdk_language,
            "--metadata-file", metadata_file,
        ])

        print("\n✅ Flex Template build completed")
        print(f"   Image tag : {new_tag}")
        print(f"   Image URI : {image_uri}")
        print(f"   Template  : {template_path}\n")

    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
