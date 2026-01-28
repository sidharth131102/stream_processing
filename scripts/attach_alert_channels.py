#!/usr/bin/env python3

import subprocess
import yaml
from pathlib import Path


def run_capture(cmd):
    return subprocess.check_output(cmd, text=True).strip()


def main():
    cfg = yaml.safe_load(Path("config/pipeline.yaml").read_text())

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
