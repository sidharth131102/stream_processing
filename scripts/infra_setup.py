#!/usr/bin/env python3
"""
infra_setup.py

Single entrypoint for full GCP infrastructure bootstrap
for Stream Accelerator.

Replaces:
- enable apis.sh
- create service account.sh
- create_bucket_and_upload_configs.sh
- register_schema.sh
- create_pubsub.sh
- create_bigquery_dataset.sh
- bootstrap_composer.py
"""

import subprocess
import sys
import yaml
from pathlib import Path
from typing import Dict
from google.cloud import storage


# --------------------------------------------------
# Utilities
# --------------------------------------------------
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


# --------------------------------------------------
# GCP Core
# --------------------------------------------------
def set_project(project_id: str):
    run(["gcloud", "config", "set", "project", project_id])


def enable_apis(apis: list):
    run(["gcloud", "services", "enable", *apis])


# --------------------------------------------------
# IAM
# --------------------------------------------------
def ensure_service_account(project_id: str, name: str, display_name: str) -> str:
    email = f"{name}@{project_id}.iam.gserviceaccount.com"
    try:
        run_capture([
            "gcloud", "iam", "service-accounts", "describe",
            email, "--project", project_id
        ])
        print(f"✔ SA exists: {email}")
    except subprocess.CalledProcessError:
        run([
            "gcloud", "iam", "service-accounts", "create",
            name,
            "--display-name", display_name,
            "--project", project_id
        ])
    return email


def grant_roles(project_id: str, member: str, roles: list):
    for role in roles:
        run([
            "gcloud", "projects", "add-iam-policy-binding",
            project_id,
            "--member", member,
            "--role", role
        ], check=False)


# --------------------------------------------------
# GCS
# --------------------------------------------------
def ensure_bucket(project_id, bucket, location, cmek_key=None):
    cmd = ["gsutil", "mb", "-p", project_id, "-l", location]
    if cmek_key:
        cmd.extend(["-k", cmek_key])
    cmd.append(f"gs://{bucket}")
    run(cmd, check=False)


def upload_files(bucket: str, files: list):
    for f in files:
        run(["gsutil", "cp", f["source"], f"gs://{bucket}/{f['destination']}"])


def ensure_folders(bucket: str, folders: list):
    for folder in folders:
        run(["gsutil", "mkdir", f"gs://{bucket}/{folder}"], check=False)

def upload_schema_files(config_bucket: str, schemas: list):
    """
    Upload schema files to gs://<config-bucket>/schemas/
    """
    for schema in schemas:
        src = schema.get("payload_schema_file")
        if not src:
            continue

        dest = f"gs://{config_bucket}/schemas/{Path(src).name}"

        print(f"📦 Uploading schema file: {src} → {dest}")
        run(["gsutil", "cp", src, dest], check=False)

# --------------------------------------------------
# Pub/Sub
# --------------------------------------------------
def create_topic(name: str, cmek_key: str | None = None):
    cmd = ["gcloud", "pubsub", "topics", "create", name]
    if cmek_key:
        cmd.extend(["--kms-key-name", cmek_key])
    run(cmd, check=False)

def create_subscription(cfg: dict):
    cmd = [
        "gcloud", "pubsub", "subscriptions", "create", cfg["name"],
        "--topic", cfg["topic"],
        "--ack-deadline", str(cfg.get("ack_deadline", 60)),
    ]

    if "dead_letter_topic" in cfg:
        cmd.extend([
            "--dead-letter-topic", cfg["dead_letter_topic"],
            "--max-delivery-attempts", str(cfg.get("max_delivery_attempts", 5))
        ])

    run(cmd, check=False)

def attach_schema_to_events_topic(topic_name: str, schema_name: str):
    """
    Specifically updates the events topic with the required schema and encoding.
    """
    run([
        "gcloud", "pubsub", "topics", "update", topic_name,
        "--schema", schema_name,
        "--message-encoding", "json"
    ], check=False)

def register_schema(schema: dict):
    run([
        "gcloud", "pubsub", "schemas", "create",
        schema["name"],
        "--type", schema["type"],
        "--definition-file", schema["definition_file"]
    ], check=False)


def ensure_artifact_registry(
    project_id: str,
    location: str,
    repository: str,
    description: str = "Dataflow accelerator images"
):
    """
    Ensure Artifact Registry Docker repository exists.
    Idempotent: safe to re-run.
    """
    try:
        # Check if repo exists
        run_capture([
            "gcloud", "artifacts", "repositories", "describe",
            repository,
            "--project", project_id,
            "--location", location,
        ])
        print(f"✔ Artifact Registry exists: {repository}")
    except subprocess.CalledProcessError:
        run([
            "gcloud", "artifacts", "repositories", "create", repository,
            "--project", project_id,
            "--repository-format", "docker",
            "--location", location,
            "--description", description,
        ])
        print(f"✔ Created Artifact Registry: {repository}")

# --------------------------------------------------
# BigQuery
# --------------------------------------------------
def ensure_dataset(project_id: str, name: str, location: str):
    try:
        run_capture(["bq", "show", f"{project_id}:{name}"])
        print(f"✔ Dataset exists: {name}")
    except subprocess.CalledProcessError:
        run([
            "bq", "mk",
            "--dataset",
            "--location", location,
            f"{project_id}:{name}"
        ])


# --------------------------------------------------
# Monitoring (Dashboards & Alerts)
# --------------------------------------------------
def create_monitoring_dashboard(dashboard_file: Path):
    if not dashboard_file.exists():
        print(f"⚠ Dashboard file not found, skipping: {dashboard_file}")
        return

    run([
        "gcloud", "monitoring", "dashboards", "create",
        "--config-from-file", str(dashboard_file)
    ], check=False)


def create_alert_policies(alerts_dir: Path):
    if not alerts_dir.exists():
        print(f"⚠ Alerts directory not found, skipping: {alerts_dir}")
        return

    alert_files = sorted(alerts_dir.glob("*.json"))

    if not alert_files:
        print(f"⚠ No alert policy files found in {alerts_dir}")
        return

    for alert_file in alert_files:
        print(f"📣 Creating alert policy: {alert_file.name}")
        run([
            "gcloud", "monitoring", "policies", "create",
            "--policy-from-file", str(alert_file)
        ], check=False)

def create_email_channel(project_id: str, email: str) -> str:
    """
    Creates (or reuses) an email notification channel.
    Returns the channel resource ID.
    """
    # Check if channel already exists
    existing = run_capture([
        "gcloud", "monitoring", "channels", "list",
        "--filter", f'type="email" AND labels.email_address="{email}"',
        "--format", "value(name)"
    ])

    if existing:
        print(f"✔ Email channel exists: {existing}")
        return existing

    channel_id = run_capture([
        "gcloud", "beta", "monitoring", "channels", "create",
        "--display-name", f"Alerts Email ({email})",
        "--type", "email",
        "--channel-labels", f"email_address={email}",
        "--format", "value(name)"
    ])

    print(f"✔ Created email channel: {channel_id}")
    return channel_id


def update_pipeline_yaml_with_channels(
    pipeline_yaml: Path,
    channels: Dict[str, str]
):
    cfg = yaml.safe_load(pipeline_yaml.read_text())

    cfg.setdefault("observability", {})
    cfg["observability"].setdefault("alerts", {})
    cfg["observability"]["alerts"].setdefault("notification_channels", {})

    for name, channel_id in channels.items():
        cfg["observability"]["alerts"]["notification_channels"][name] = {
            "id": channel_id
        }

    pipeline_yaml.write_text(yaml.safe_dump(cfg, sort_keys=False))
    print("✔ Updated pipeline.yaml with notification channel IDs")


# --------------------------------------------------
# Composer (reuse your logic)
# --------------------------------------------------
# from bootstrap_composer import main as bootstrap_composer


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    cfg = load_yaml(Path("config/pipeline.yaml"))

    project_id = cfg["project"]["id"]
    region = cfg["project"]["region"]

    set_project(project_id)

    enable_apis([
        "dataflow.googleapis.com",
        "pubsub.googleapis.com",
        "bigquery.googleapis.com",
        "storage.googleapis.com",
        "logging.googleapis.com",
        "composer.googleapis.com",
        "monitoring.googleapis.com",
        "artifactregistry.googleapis.com",
        "iam.googleapis.com",
    ])

    # --------------------------------------------------
    # Service Accounts
    # --------------------------------------------------
    sa_cfg = cfg["service_account"]
    sa_email = ensure_service_account(
        project_id,
        sa_cfg["name"],
        sa_cfg["display_name"]
    )
    grant_roles(project_id, f"serviceAccount:{sa_email}", sa_cfg["roles"])
    cmek_cfg = cfg.get("security", {}).get("cmek", {})
    cmek_key = cmek_cfg.get("key_name") if cmek_cfg.get("enabled") else None
    event_topic_name = cfg["pubsub"]["topics"][0]["name"]
    registry_schema_name = cfg["pubsub"]["schemas"][0]["name"]
    # --------------------------------------------------
    # Buckets
    # --------------------------------------------------
    for bucket in cfg["storage"]["buckets"]:
        ensure_bucket(project_id, bucket["name"], region)
        if "files" in bucket:
            upload_files(bucket["name"], bucket["files"])
        if "folders" in bucket:
            ensure_folders(bucket["name"], bucket["folders"])
    
    config_bucket = cfg["pipeline_configs"]["bucket"]

    upload_schema_files(
        config_bucket=config_bucket,
        schemas=cfg["pubsub"].get("schemas", [])
    )

    # --------------------------------------------------
    # Pub/Sub
    # --------------------------------------------------
    for schema in cfg["pubsub"].get("schemas", []):
        register_schema(schema)

    for topic in cfg["pubsub"]["topics"]:
        create_topic(topic["name"], cmek_key)

    for sub in cfg["pubsub"]["subscriptions"]:
        create_subscription(sub)

    attach_schema_to_events_topic(
        topic_name=event_topic_name, 
        schema_name=registry_schema_name
    )

    docker_cfg = cfg.get("docker", {}).get("registry", {})

    if docker_cfg:
        ensure_artifact_registry(
            project_id=project_id,
            location=docker_cfg["location"],
            repository=docker_cfg["repository"],
        )



    # # --------------------------------------------------
    # # BigQuery
    # # --------------------------------------------------
    for ds in cfg["bigquery"]["datasets"]:
        ensure_dataset(project_id, ds["name"], ds["location"])

    # --------------------------------------------------
    # Monitoring
    # --------------------------------------------------
    create_monitoring_dashboard(
        Path("monitoring/dashboard/dataflow_observability.json")
    )

    # # --------------------------------------------------
    # # Composer
    # # --------------------------------------------------
    # bootstrap_composer()

    # print("\n🎉 Infrastructure setup completed successfully")


if __name__ == "__main__":
    main()
