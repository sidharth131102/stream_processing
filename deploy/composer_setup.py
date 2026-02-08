#!/usr/bin/env python3
"""
bootstrap_composer.py

Bootstraps a Cloud Composer v3 environment using composer.yaml.

Responsibilities:
1. Load composer.yaml from config bucket
2. Ensure Composer service agent IAM
3. Create Composer service account
4. Grant IAM roles
5. Create Composer environment (if missing)
6. Discover Composer DAG bucket
7. Sync DAGs
8. Set Airflow variables
9. Validate setup
"""

import subprocess
import sys
import yaml
from pathlib import Path
from typing import Dict
from google.cloud import storage


# --------------------------------------------------
# Helpers
# --------------------------------------------------
def run(cmd: list):
    print("\n▶", " ".join(cmd))
    subprocess.run(cmd, check=True)


def run_capture(cmd: list) -> str:
    print("\n▶", " ".join(cmd))
    return subprocess.check_output(cmd, text=True).strip()

def load_yaml(path: Path) -> Dict:
    if not path.exists():
        raise FileNotFoundError(path)
    return yaml.safe_load(path.read_text())

# def load_yaml_from_gcs(bucket: str, path: str) -> Dict:
#     client = storage.Client()
#     blob = client.bucket(bucket).blob(path)

#     if not blob.exists():
#         raise FileNotFoundError(
#             f"composer.yaml not found at gs://{bucket}/{path}"
#         )

    return yaml.safe_load(blob.download_as_text())


def ensure_service_account(project_id: str, sa_name: str, display_name: str):
    email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"

    try:
        run_capture([
            "gcloud", "iam", "service-accounts", "describe",
            email, "--project", project_id
        ])
        print(f"✔ Service account exists: {email}")
    except subprocess.CalledProcessError:
        run([
            "gcloud", "iam", "service-accounts", "create",
            sa_name,
            "--project", project_id,
            "--display-name", display_name
        ])

    return email

def ensure_composer_service_agent(project_id: str, project_number: str):
    """
    Ensure Composer Service Agent has required IAM role.
    """
    agent_email = (
        f"service-{project_number}"
        "@cloudcomposer-accounts.iam.gserviceaccount.com"
    )

    print(
        f"\n🔐 Ensuring Composer Service Agent IAM: {agent_email}"
    )

    run([
        "gcloud", "projects", "add-iam-policy-binding",
        project_id,
        "--member", f"serviceAccount:{agent_email}",
        "--role", "roles/composer.ServiceAgentV2Ext",
    ])


def grant_roles(project_id: str, member: str, roles: list):
    for role in roles:
        run([
            "gcloud", "projects", "add-iam-policy-binding",
            project_id,
            "--member", member,
            "--role", role
        ])


def composer_env_exists(env_name: str, region: str) -> bool:
    try:
        run_capture([
            "gcloud", "composer", "environments", "describe",
            env_name,
            "--location", region
        ])
        return True
    except subprocess.CalledProcessError:
        return False


def get_dag_bucket(env_name: str, region: str) -> str:
    return run_capture([
        "gcloud", "composer", "environments", "describe",
        env_name,
        "--location", region,
        "--format", "value(config.dagGcsPrefix)"
    ])


def set_airflow_variable(env_name: str, region: str, key: str, value: str):
    run([
        "gcloud", "composer", "environments", "run",
        env_name,        # 1. Environment Name
        "variables",     # 2. SUBCOMMAND must be here
        "--location",
        region,          # 3. Gcloud flags
        "--",            # 4. Separator
        "set",
        key,
        value            # 5. Airflow CLI arguments
    ])


# --------------------------------------------------
# Main
# --------------------------------------------------
def main():
    try:
        # --------------------------------------------------
        # Load composer.yaml
        # --------------------------------------------------

        cfg = load_yaml(Path("config/composer.yaml"))

        project_id = cfg["project"]["id"]
        project_number = cfg["project"]["number"]

        composer_cfg = cfg["composer"]
        env_cfg = composer_cfg["environment"]
        sa_cfg = composer_cfg["service_account"]
        agent_cfg = composer_cfg["service_agent"]

        env_name = env_cfg["name"]
        region = env_cfg["region"]

        print(f"\n🚀 Bootstrapping Composer environment: {env_name}")

        # --------------------------------------------------
        # Ensure Composer service agent IAM
        # --------------------------------------------------
        ensure_composer_service_agent(
            project_id=project_id,
            project_number=project_number,
        )

        # --------------------------------------------------
        # Ensure Composer service account
        # --------------------------------------------------
        print("\n🔐 Ensuring Composer service account")

        sa_email = ensure_service_account(
            project_id,
            sa_cfg["name"],
            sa_cfg["display_name"]
        )

        grant_roles(
            project_id,
            f"serviceAccount:{sa_email}",
            sa_cfg["roles"],
        )

        # --------------------------------------------------
        # Create Composer environment (if needed)
        # --------------------------------------------------
        if not composer_env_exists(env_name, region):
            print("\n🏗 Creating Composer environment")

            run([
                "gcloud", "composer", "environments", "create",
                env_name,
                "--location", region,
                "--image-version", env_cfg["image_version"],
                "--environment-size", env_cfg["environment_size"],
                "--service-account", sa_email,
            ])
        else:
            print("✔ Composer environment already exists")

        # --------------------------------------------------
        # Discover DAG bucket
        # --------------------------------------------------
        dag_bucket = get_dag_bucket(env_name, region)
        print(f"\n📦 Composer DAG bucket: {dag_bucket}")

        # --------------------------------------------------
        # Sync DAGs
        # --------------------------------------------------
        dags_cfg = composer_cfg["dags"]
        local_dags = Path(dags_cfg["local_path"]).resolve()

        if not local_dags.exists():
            raise FileNotFoundError(
                f"DAG folder not found: {local_dags}"
            )

        print("\n📤 Syncing DAGs")

        run([
            "gsutil", "-m", "rsync", "-r",
            str(local_dags),
            dag_bucket
        ])

        # --------------------------------------------------
        # Set Airflow variables
        # --------------------------------------------------
        print("\n⚙ Setting Airflow variables")

        for key, value in composer_cfg["airflow"]["variables"].items():
            set_airflow_variable(env_name, region, key, value)

        print("\n✅ Composer bootstrap completed successfully")

    except Exception as e:
        print(f"\n❌ ERROR: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
