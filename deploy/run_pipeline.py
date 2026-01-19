import yaml
import subprocess
import sys
from pathlib import Path
from datetime import datetime

PIPELINE_YAML = "config/pipeline.yaml"


def load_pipeline_config(path: str):
    if not Path(path).exists():
        raise FileNotFoundError(f"❌ pipeline.yaml not found at: {path}")

    with open(path, "r") as f:
        return yaml.safe_load(f)


def require(cfg, path):
    """Helper to enforce required config keys"""
    node = cfg
    for key in path.split("."):
        if key not in node:
            raise KeyError(f"❌ Missing required config key: {path}")
        node = node[key]
    return node


def build_gcloud_command(cfg):
    project_id = require(cfg, "project.id")
    region = require(cfg, "project.region")

    sa_name = require(cfg, "service_account.name")
    service_account_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"

    df = require(cfg, "dataflow")
    job = require(df, "job")
    template = require(df, "template")

    job_name = f"{job['name_prefix']}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    cmd = [
        "gcloud", "dataflow", "flex-template", "run",
        job_name,
        "--project", project_id,
        "--region", job.get("worker_region"),
        "--template-file-gcs-location", template["storage_path"],
        "--staging-location", job["staging_location"],
        "--temp-location", job["temp_location"],
        "--service-account-email", service_account_email,
    ]

    # -----------------------------
    # Runtime parameters (MANDATORY)
    # -----------------------------
    parameters = job.get("parameters", {})
    if not parameters:
        raise ValueError("❌ dataflow.job.parameters cannot be empty")

    if "job_mode" not in parameters:
        raise ValueError("❌ job_mode must be provided for Flex Template")

    param_string = ",".join(f"{k}={v}" for k, v in parameters.items())
    cmd.extend(["--parameters", param_string])

    # -----------------------------
    # Streaming Engine (ONLY for streaming jobs)

    # -----------------------------
    # if "worker_region" in job:
    #     cmd.extend(["--worker-region", job["worker_region"]])
    # if "worker_zone" in job:
    #     cmd.extend(["--worker-zone", job["worker_zone"]])
    # if "worker_machine_type" in job:
    #     cmd.extend(["--worker-machine-type", job["worker_machine_type"]])

    if (
        job.get("enable_streaming_engine", False)
        and parameters.get("job_mode") == "streaming"
    ):
        cmd.append("--enable-streaming-engine")

    return cmd


def main():
    try:
        cfg = load_pipeline_config(PIPELINE_YAML)
        cmd = build_gcloud_command(cfg)

        print("\n🚀 Launching Dataflow Flex Template job:\n")
        print(" ".join(cmd))
        print("\n----------------------------------------\n")

        subprocess.run(cmd, check=True)

        print("\n✅ Dataflow job launched successfully\n")

    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
