#!/usr/bin/env python3
"""
ci_cd_bootstrap.py

Bootstraps GitHub Actions → GCP CI/CD using Workload Identity Federation.
"""

import subprocess
import yaml
import sys
from pathlib import Path


def run(cmd: list[str]):
    print("▶", " ".join(cmd))
    subprocess.run(cmd, check=True)


def load_cfg(path: str):
    with open(path) as f:
        return yaml.safe_load(f)


def enable_apis(project_id, apis):
    for api in apis:
        run([
            "gcloud", "services", "enable", api,
            "--project", project_id
        ])


def create_service_account(project_id, sa_name):
    run([
        "gcloud", "iam", "service-accounts", "create", sa_name,
        "--project", project_id,
        "--display-name", "GitHub CI/CD"
    ])


def grant_roles(project_id, sa_email):
    roles = [
        "roles/dataflow.developer",
        "roles/dataflow.worker",
        "roles/artifactregistry.writer",
        "roles/storage.admin",
        "roles/iam.serviceAccountUser",
    ]

    for role in roles:
        run([
            "gcloud", "projects", "add-iam-policy-binding", project_id,
            "--member", f"serviceAccount:{sa_email}",
            "--role", role
        ])


def create_wif_pool(project_id, pool):
    run([
        "gcloud", "iam", "workload-identity-pools", "create", pool,
        "--project", project_id,
        "--location", "global",
        "--display-name", "GitHub Actions Pool"
    ])


def create_github_provider(project_id, pool, provider, org, repo):
    run([
        "gcloud", "iam", "workload-identity-pools", "providers", "create-oidc",
        provider,
        "--project", project_id,
        "--location", "global",
        "--workload-identity-pool", pool,
        "--display-name", "GitHub Provider",
        "--issuer-uri", "https://token.actions.githubusercontent.com",
        "--attribute-mapping",
        "google.subject=assertion.sub,"
        "attribute.repository=assertion.repository,"
        "attribute.ref=assertion.ref",
        "--attribute-condition",
        f"assertion.repository == '{org}/{repo}'"
    ])


def allow_github_impersonation(project_number, pool, sa_email, org, repo):
    member = (
        f"principalSet://iam.googleapis.com/projects/{project_number}"
        f"/locations/global/workloadIdentityPools/{pool}"
        f"/attribute.repository/{org}/{repo}"
    )

    run([
        "gcloud", "iam", "service-accounts", "add-iam-policy-binding",
        sa_email,
        "--role", "roles/iam.workloadIdentityUser",
        "--member", member
    ])



def main():
    if len(sys.argv) != 2:
        print("Usage: python ci_cd_bootstrap.py ci_cd.yaml")
        sys.exit(1)

    cfg = load_cfg(sys.argv[1])

    project = cfg["project"]
    github = cfg["github"]
    cicd = cfg["ci_cd"]

    project_id = project["id"]
    project_number = project["number"]

    sa_name = cicd["service_account_name"]
    sa_email = f"{sa_name}@{project_id}.iam.gserviceaccount.com"

    print("\n🚀 Bootstrapping CI/CD for project:", project_id)

    enable_apis(project_id, cfg.get("apis", []))
    create_service_account(project_id, sa_name)
    grant_roles(project_id, sa_email)
    create_wif_pool(project_id, cicd["workload_identity_pool"])
    create_github_provider(
        project_id,
        cicd["workload_identity_pool"],
        cicd["workload_identity_provider"],
        github["org"],
        github["repo"],
    )
    allow_github_impersonation(
        project_number,
        cicd["workload_identity_pool"],
        sa_email,
        github["org"],
        github["repo"],
    )

    print("\n✅ CI/CD bootstrap completed successfully")


if __name__ == "__main__":
    main()
