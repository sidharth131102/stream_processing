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
    parameters = job.get("parameters", {})

    job_name = f"{job['name_prefix']}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    # Base command
    cmd = [
        "gcloud", "dataflow", "flex-template", "run",
        job_name,
        "--project", project_id,
        "--region", region,
        "--template-file-gcs-location", template["storage_path"],
        "--staging-location", job["staging_location"],
        "--temp-location", job["temp_location"],
        "--service-account-email", service_account_email,
    ]

    # -----------------------------
    # Performance & Worker Scaling (UNCOMMENTED & FIXED)
    # -----------------------------
    if job.get("num_workers"):
        cmd.extend(["--num-workers", str(job["num_workers"])])
    
    if job.get("max_workers"):
        cmd.extend(["--max-workers", str(job["max_workers"])])

    if job.get("disk_size_gb"):
        cmd.extend(["--disk-size-gb", str(job["disk_size_gb"])])

    if job.get("worker_machine_type"):
        cmd.extend(["--worker-machine-type", job["worker_machine_type"]])

    # High Performance Engine for Stateful Dedup
    if job.get("enable_streaming_engine", False) and parameters.get("job_mode") == "streaming":
        cmd.append("--enable-streaming-engine")

    # -----------------------------
    # Runtime Parameters
    # -----------------------------
    param_string = ",".join(f"{k}={v}" for k, v in parameters.items())
    cmd.extend(["--parameters", param_string])

    return cmd

def main():
    try:
        cfg = load_pipeline_config(PIPELINE_YAML)
        cmd = build_gcloud_command(cfg)
        print("\n🚀 Launching Optimized Dataflow Flex Template:\n")
        print(" ".join(cmd))
        subprocess.run(cmd, check=True)
    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()