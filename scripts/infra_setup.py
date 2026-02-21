#!/usr/bin/env python3
"""
infra_setup.py - CMEK Enabled Version
"""

import subprocess
import sys
import yaml
import json
import argparse
from pathlib import Path
from typing import Dict, List, Optional
from google.cloud import storage

# ... [Utilities: run, run_capture, load_yaml, set_project, enable_apis remain unchanged] ...

def run(cmd: list, check=True):
    print("\n▶", " ".join(cmd))
    subprocess.run(cmd, check=check)

def run_capture(cmd: list) -> str:
    print("\n▶", " ".join(cmd))
    return subprocess.check_output(cmd, text=True).strip()

def load_yaml(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(path)
    return yaml.safe_load(path.read_text())

def set_project(project_id: str):
    run(["gcloud", "config", "set", "project", project_id])

def enable_apis(apis: list):
    run(["gcloud", "services", "enable", *apis])


# --------------------------------------------------
# External network/perimeter validation (no creation)
# --------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Infrastructure setup with optional external network/perimeter validation."
    )
    parser.add_argument(
        "--network",
        help="Existing VPC network name to use for Dataflow workers.",
    )
    parser.add_argument(
        "--subnetwork",
        help=(
            "Existing subnet to use for Dataflow workers. "
            "Accepts full self link or short form regions/<region>/subnetworks/<name>."
        ),
    )
    parser.add_argument(
        "--subnetwork-region",
        help="Region for --subnetwork when only subnet name is provided.",
    )
    parser.add_argument(
        "--vpc-perimeter",
        help="Existing VPC-SC perimeter name to validate (not created by this script).",
    )
    parser.add_argument(
        "--access-policy-id",
        help="Access Context Manager policy id used with --vpc-perimeter.",
    )
    return parser.parse_args()


def extract_subnet_name(subnetwork: str) -> str:
    value = subnetwork.strip()
    if "/" in value:
        return value.split("/")[-1]
    return value


def validate_existing_subnetwork(project_id: str, subnet_name: str, subnet_region: str):
    run_capture(
        [
            "gcloud",
            "compute",
            "networks",
            "subnets",
            "describe",
            subnet_name,
            "--project",
            project_id,
            "--region",
            subnet_region,
        ]
    )
    print(f"✔ Validated existing subnet: {subnet_name} ({subnet_region})")


def validate_existing_perimeter(perimeter_name: str, policy_id: str):
    run_capture(
        [
            "gcloud",
            "access-context-manager",
            "perimeters",
            "describe",
            perimeter_name,
            "--policy",
            policy_id,
        ]
    )
    print(f"✔ Validated existing VPC-SC perimeter: {perimeter_name}")

# --------------------------------------------------
# IAM
# --------------------------------------------------
def ensure_service_account(project_id: str, name: str, display_name: str) -> str:
    email = f"{name}@{project_id}.iam.gserviceaccount.com"
    try:
        run_capture(["gcloud", "iam", "service-accounts", "describe", email, "--project", project_id])
        print(f"✔ SA exists: {email}")
    except subprocess.CalledProcessError:
        run(["gcloud", "iam", "service-accounts", "create", name, "--display-name", display_name, "--project", project_id])
    return email

def grant_roles(project_id: str, member: str, roles: list):
    for role in roles:
        run(["gcloud", "projects", "add-iam-policy-binding", project_id, "--member", member, "--role", role], check=False)

# --------------------------------------------------
# GCS (Updated to propagate key)
# --------------------------------------------------
def ensure_bucket(project_id, bucket, location, cmek_key=None):
    cmd = ["gsutil", "mb", "-p", project_id, "-l", location]
    if cmek_key:
        cmd.extend(["-k", cmek_key])
    cmd.append(f"gs://{bucket}")
    run(cmd, check=False)

# ... [upload_files, ensure_folders, upload_schema_files remain unchanged] ...

def upload_files(bucket: str, files: list):
    for f in files:
        run(["gsutil", "cp", f["source"], f"gs://{bucket}/{f['destination']}"])

def ensure_folders(bucket: str, folders: list):
    for folder in folders:
        run(["gsutil", "mkdir", f"gs://{bucket}/{folder}"], check=False)

def upload_schema_files(config_bucket: str, schemas: list):
    for schema in schemas:
        src = schema.get("payload_schema_file")
        if not src: continue
        dest = f"gs://{config_bucket}/schemas/{Path(src).name}"
        run(["gsutil", "cp", src, dest], check=False)

# --------------------------------------------------
# Pub/Sub
# --------------------------------------------------
def create_topic(name: str, cmek_key: str | None = None):
    cmd = ["gcloud", "pubsub", "topics", "create", name]
    if cmek_key:
        cmd.extend(["--topic-encryption-key", cmek_key])
    run(cmd, check=False)

# ... [create_subscription, attach_schema_to_events_topic, register_schema remain unchanged] ...

def create_subscription(cfg: dict):
    cmd = ["gcloud", "pubsub", "subscriptions", "create", cfg["name"], "--topic", cfg["topic"], "--ack-deadline", str(cfg.get("ack_deadline", 60))]
    if "dead_letter_topic" in cfg:
        cmd.extend(["--dead-letter-topic", cfg["dead_letter_topic"], "--max-delivery-attempts", str(cfg.get("max_delivery_attempts", 5))])
    run(cmd, check=False)

def attach_schema_to_events_topic(topic_name: str, schema_name: str):
    run(["gcloud", "pubsub", "topics", "update", topic_name, "--schema", schema_name, "--message-encoding", "json"], check=False)

def register_schema(schema: dict):
    run(["gcloud", "pubsub", "schemas", "create", schema["name"], "--type", schema["type"], "--definition-file", schema["definition_file"]], check=False)

# --------------------------------------------------
# Artifact Registry (Updated to accept key)
# --------------------------------------------------
def ensure_artifact_registry(project_id, location, repository, cmek_key=None):
    try:
        # We use a simple subprocess here to avoid the traceback on 404
        subprocess.check_output(["gcloud", "artifacts", "repositories", "describe", 
                                 repository, "--project", project_id, "--location", location], 
                                stderr=subprocess.STDOUT)
        print(f"✔ Artifact Registry exists: {repository}")
    except subprocess.CalledProcessError:
        cmd = ["gcloud", "artifacts", "repositories", "create", repository, 
               "--project", project_id, "--repository-format", "docker", "--location", location]
        if cmek_key:
            cmd.extend(["--kms-key", cmek_key])
        # Added --quiet to avoid interactive "Grant permission?" prompts
        cmd.append("--quiet") 
        run(cmd)
# --------------------------------------------------
# BigQuery (Updated to accept key)
# --------------------------------------------------
def ensure_dataset(project_id: str, name: str, location: str, cmek_key: str = None):
    try:
        run_capture(["bq", "show", f"{project_id}:{name}"])
        print(f"✔ Dataset exists: {name}")
    except subprocess.CalledProcessError:
        cmd = ["bq", "mk", "--dataset", "--location", location]
        if cmek_key:
            # Sets the default key for all tables created in this dataset
            cmd.append(f"--default_kms_key={cmek_key}")
        cmd.append(f"{project_id}:{name}")
        run(cmd)

# ... [Monitoring helpers remain unchanged] ...

def create_monitoring_dashboard(dashboard_file: Path):
    if not dashboard_file.exists(): return
    dashboard_data = json.loads(dashboard_file.read_text())
    display_name = dashboard_data.get("displayName", "My Dashboard")
    existing = run_capture(["gcloud", "monitoring", "dashboards", "list", f'--filter=displayName="{display_name}"', "--format=value(name)"])
    if existing:
        run(["gcloud", "monitoring", "dashboards", "update", existing, "--config-from-file", str(dashboard_file)])
    else:
        run(["gcloud", "monitoring", "dashboards", "create", "--config-from-file", str(dashboard_file)])

# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    args = parse_args()
    cfg = load_yaml(Path("config/pipeline.yaml"))
    project_id = cfg["project"]["id"]
    region = cfg["project"]["region"]

    set_project(project_id)
    enable_apis(["dataflow.googleapis.com", "pubsub.googleapis.com", "bigquery.googleapis.com", "storage.googleapis.com", 
                 "logging.googleapis.com", "composer.googleapis.com", "monitoring.googleapis.com", "artifactregistry.googleapis.com", "iam.googleapis.com","cloudresourcemanager.googleapis.com","datalineage.googleapis.com","datacatalog.googleapis.com","dataplex.googleapis.com","cloudkms.googleapis.com","compute.googleapis.com","accesscontextmanager.googleapis.com"])

    # Optional validation for externally managed network/perimeter.
    network_cfg = cfg.get("network", {})
    subnet_cfg = network_cfg.get("subnetwork", {})
    selected_network = args.network or network_cfg.get("name")
    selected_subnetwork = args.subnetwork or subnet_cfg.get("self_link") or subnet_cfg.get("name")
    selected_subnet_region = (
        args.subnetwork_region
        or subnet_cfg.get("region")
        or region
    )
    if selected_subnetwork:
        validate_existing_subnetwork(
            project_id=project_id,
            subnet_name=extract_subnet_name(selected_subnetwork),
            subnet_region=selected_subnet_region,
        )

    vpc_sc_cfg = cfg.get("vpc_sc", {})
    perimeter_name = args.vpc_perimeter or vpc_sc_cfg.get("perimeter_name")
    policy_id = args.access_policy_id or vpc_sc_cfg.get("access_context", {}).get("policy_id")
    if perimeter_name:
        if not policy_id:
            raise ValueError(
                "Perimeter validation needs a policy id. Set --access-policy-id or vpc_sc.access_context.policy_id."
            )
        validate_existing_perimeter(
            perimeter_name=perimeter_name,
            policy_id=str(policy_id),
        )
        print("ℹ Pub/Sub and BigQuery are protected via VPC-SC perimeter, not via subnet flags.")
    if selected_network:
        print(f"ℹ Dataflow will use network: {selected_network}")
    if selected_subnetwork:
        print(f"ℹ Dataflow will use subnetwork: {selected_subnetwork}")

    # 1. Extract CMEK Config
    cmek_cfg = cfg.get("security", {}).get("cmek", {})
    cmek_key = cmek_cfg.get("key_name") if cmek_cfg.get("enabled") else None

    # 2. Service Accounts & Roles
    sa_cfg = cfg["service_account"]
    sa_email = ensure_service_account(project_id, sa_cfg["name"], sa_cfg["display_name"])
    grant_roles(project_id, f"serviceAccount:{sa_email}", sa_cfg["roles"])

    # 3. Buckets (Now with CMEK)
    for bucket in cfg["storage"]["buckets"]:
        ensure_bucket(project_id, bucket["name"], region, cmek_key=cmek_key)
        if "files" in bucket: upload_files(bucket["name"], bucket["files"])
        if "folders" in bucket: ensure_folders(bucket["name"], bucket["folders"])
    
    config_bucket = cfg["pipeline_configs"]["bucket"]
    upload_schema_files(config_bucket=config_bucket, schemas=cfg["pubsub"].get("schemas", []))

    # 4. Pub/Sub (CMEK handled in helpers)
    for schema in cfg["pubsub"].get("schemas", []):
        register_schema(schema)

    for topic in cfg["pubsub"]["topics"]:
        create_topic(topic["name"], cmek_key=cmek_key)

    for sub in cfg["pubsub"]["subscriptions"]:
        create_subscription(sub)

    event_topic_name = cfg["pubsub"]["topics"][0]["name"]
    registry_schema_name = cfg["pubsub"]["schemas"][0]["name"]
    attach_schema_to_events_topic(topic_name=event_topic_name, schema_name=registry_schema_name)

    # 5. Artifact Registry (Now with CMEK)
    docker_cfg = cfg.get("docker", {}).get("registry", {})
    if docker_cfg:
        ensure_artifact_registry(project_id=project_id, location=docker_cfg["location"], 
                                 repository=docker_cfg["repository"], cmek_key=cmek_key)

    # 6. BigQuery (Now with CMEK)
    for ds in cfg["bigquery"]["datasets"]:
        ensure_dataset(project_id, ds["name"], ds["location"], cmek_key=cmek_key)

    # 7. Monitoring
    create_monitoring_dashboard(Path("monitoring/dashboard/dataflow_observability.json"))

    print("\n🎉 Infrastructure setup completed successfully (CMEK: {})".format("Enabled" if cmek_key else "Disabled"))

if __name__ == "__main__":
    main()
