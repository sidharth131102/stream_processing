#!/usr/bin/env python3

import re
import subprocess
import sys
from pathlib import Path

import yaml


PIPELINE_YAML = "config/pipeline.yaml"


def run(cmd: list):
    print("\n>", " ".join(cmd))
    subprocess.run(cmd, check=True)


def run_optional(cmd: list) -> bool:
    print("\n>", " ".join(cmd))
    result = subprocess.run(cmd, check=False)
    return result.returncode == 0


def load_pipeline_yaml():
    path = Path(PIPELINE_YAML)
    if not path.exists():
        raise FileNotFoundError(f"pipeline.yaml not found at {path.resolve()}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_pipeline_yaml(cfg):
    with open(PIPELINE_YAML, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, sort_keys=False)


def bump_version(tag: str) -> str:
    """
    v17 -> v18
    """
    match = re.fullmatch(r"v(\d+)", tag)
    if not match:
        raise ValueError(f"Invalid image tag '{tag}'. Expected format v<number>.")
    return f"v{int(match.group(1)) + 1}"


def versioned_template_path(latest_path: str, tag: str) -> str:
    """
    gs://.../json-streaming-templ-latest.json -> gs://.../json-streaming-templ-v123.json
    """
    if latest_path.endswith("-latest.json"):
        return latest_path.replace("-latest.json", f"-{tag}.json")

    if latest_path.endswith(".json"):
        return latest_path[:-5] + f"-{tag}.json"

    return latest_path + f"-{tag}.json"


def safe_template_path(latest_path: str) -> str:
    """
    gs://.../json-streaming-templ-latest.json -> gs://.../json-streaming-templ-safe.json
    """
    if latest_path.endswith("-latest.json"):
        return latest_path.replace("-latest.json", "-safe.json")

    if latest_path.endswith(".json"):
        return latest_path[:-5] + "-safe.json"

    return latest_path + "-safe.json"


def main():
    try:
        cfg = load_pipeline_yaml()

        project_id = cfg["project"]["id"]
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
        latest_template = template_cfg["storage_path"]
        metadata_file = template_cfg["metadata_file"]
        sdk_language = template_cfg["sdk_language"]

        new_versioned_template = versioned_template_path(latest_template, new_tag)
        old_versioned_template = versioned_template_path(latest_template, old_tag)
        safe_template = template_cfg.get("safe_storage_path") or safe_template_path(latest_template)

        print(f"\nBumping image version: {old_tag} -> {new_tag}")

        # Preserve current latest as rollback-safe template (best effort).
        copied_latest_to_safe = run_optional(
            ["gcloud", "storage", "cp", latest_template, safe_template]
        )

        # Build and push image.
        run(
            [
                "docker",
                "build",
                "-f",
                dockerfile,
                "-t",
                image_uri,
                context,
            ]
        )
        run(["docker", "push", image_uri])

        # Build versioned template.
        run(
            [
                "gcloud",
                "dataflow",
                "flex-template",
                "build",
                new_versioned_template,
                "--image",
                image_uri,
                "--sdk-language",
                sdk_language,
                "--metadata-file",
                metadata_file,
            ]
        )

        # Move versioned template to latest pointer used by streaming/backfill defaults.
        run(["gcloud", "storage", "cp", new_versioned_template, latest_template])

        # If latest->safe copy failed (e.g., first run), use previous known version as safe (best effort).
        if not copied_latest_to_safe:
            run_optional(["gcloud", "storage", "cp", old_versioned_template, safe_template])

        # Persist template metadata for DAGs.
        cfg["docker"]["image"]["tag"] = new_tag
        cfg["dataflow"]["template"]["safe_storage_path"] = safe_template
        cfg["dataflow"]["template"]["latest_storage_path"] = latest_template
        cfg["dataflow"]["template"]["latest_versioned_path"] = new_versioned_template
        cfg["dataflow"]["template"]["previous_versioned_path"] = old_versioned_template
        save_pipeline_yaml(cfg)

        print("\nFlex Template build completed")
        print(f"  Image tag               : {new_tag}")
        print(f"  Image URI               : {image_uri}")
        print(f"  Template latest pointer : {latest_template}")
        print(f"  Template safe pointer   : {safe_template}")
        print(f"  Template versioned      : {new_versioned_template}\n")

    except Exception as e:
        print(f"\nERROR: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
