#!/usr/bin/env python3
import json
import subprocess
import yaml
from pathlib import Path
from typing import Dict
from scripts.infra_setup import set_project

def run(cmd: list, check=True):
    print("\n▶", " ".join(cmd))
    subprocess.run(cmd, check=check)

def run_capture(cmd):
    return subprocess.check_output(cmd, text=True).strip()
from typing import Dict
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


def create_alert_policies(alerts_dir: Path, pipeline_cfg: dict):
    if not alerts_dir.exists():
        print(f"⚠ Alerts directory not found: {alerts_dir}")
        return

    # Extract windowing values for SLA calculation
    w_cfg = pipeline_cfg.get("windowing", {})
    # Default to 7 mins total if config is missing
    sla_threshold = (
        w_cfg.get("window_size_sec", 60) + 
        w_cfg.get("allowed_lateness_sec", 300) + 
        w_cfg.get("sla_buffer_sec", 60)
    )

    alert_files = sorted(alerts_dir.glob("*.json"))

    for alert_file in alert_files:
        print(f"📣 Processing alert policy: {alert_file.name}")
        
        # Load the JSON to modify it in memory
        policy_data = json.loads(alert_file.read_text())

        # Logic: If this is the watermark alert, inject our dynamic threshold
        if "watermark_lag" in alert_file.name:
            print(f"  ⚙ Injecting dynamic threshold: {sla_threshold}s")
            # Path in your JSON: conditions[0] -> conditionThreshold -> thresholdValue
            policy_data["conditions"][0]["conditionThreshold"]["thresholdValue"] = sla_threshold
            
            # Save to a temporary file because gcloud needs a file path
            temp_path = alert_file.with_suffix(".tmp.json")
            temp_path.write_text(json.dumps(policy_data))
            upload_path = str(temp_path)
        else:
            upload_path = str(alert_file)

        # Create/Update the policy
        run([
            "gcloud", "monitoring", "policies", "create",
            "--policy-from-file", upload_path
        ], check=False)

        # Cleanup temp file
        if "watermark_lag" in alert_file.name:
            temp_path.unlink()
def main():
    cfg = yaml.safe_load(Path("config/pipeline.yaml").read_text())
    project_id = cfg["project"]["id"]

    set_project(project_id)
    create_alert_policies(Path("monitoring/alerts"), cfg)

    alerts_cfg = cfg.get("observability", {}).get("alerts", {})

    if alerts_cfg.get("enabled", False):
        channels = {}

        email_cfg = alerts_cfg.get("email")
        if email_cfg and "address" in email_cfg:
            channels["email"] = create_email_channel(
                project_id,
                email_cfg["address"]
            )

        if channels:
            update_pipeline_yaml_with_channels(
                Path("config/pipeline.yaml"),
                channels
            )
    channels = cfg.get("observability", {}) \
                  .get("alerts", {}) \
                  .get("notification_channels", {})

    if not channels:
        print("⚠ No notification channels found in pipeline.yaml")
        return

    channel_ids = ",".join(c["id"] for c in channels.values())

    policies = run_capture([
        "gcloud", "monitoring", "policies", "list",
        "--format", "value(name)"
    ]).splitlines()

    for policy in policies:
        print(f"🔔 Attaching channels to {policy}")
        subprocess.run([
            "gcloud", "monitoring", "policies", "update", policy,
            "--notification-channels", channel_ids
        ], check=False)

    print("✔ Alert policies updated with notification channels")


if __name__ == "__main__":
    main()
