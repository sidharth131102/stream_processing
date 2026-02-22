#!/usr/bin/env python3

import yaml
from pathlib import Path
from typing import List

from google.cloud import monitoring_v3
from google.protobuf import duration_pb2


CONFIG_PATH = Path("config/pipeline.yaml")


# ---------------------------------------------------------
# Load YAML Configuration
# ---------------------------------------------------------
def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"{CONFIG_PATH} not found")

    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# Extract Notification Channels (Matches Your YAML)
# ---------------------------------------------------------
def get_notification_channels(cfg: dict) -> List[str]:
    channels_cfg = (
        cfg.get("observability", {})
        .get("notification_channels", {})
    )

    channel_ids = [c["id"] for c in channels_cfg.values() if "id" in c]

    if not channel_ids:
        print("⚠ No notification channels found in pipeline.yaml")

    return channel_ids


# ---------------------------------------------------------
# Create or Update Alert Policy (Idempotent)
# ---------------------------------------------------------
def create_or_update_threshold_alert(
    project_id: str,
    display_name: str,
    metric_filter: str,
    threshold: float,
    duration_sec: int,
    alignment_sec: int,
    per_series_aligner,
    channel_ids: List[str],
):
    client = monitoring_v3.AlertPolicyServiceClient()
    project_name = f"projects/{project_id}"

    duration = duration_pb2.Duration(seconds=int(duration_sec))
    alignment = duration_pb2.Duration(seconds=int(alignment_sec))

    condition = monitoring_v3.AlertPolicy.Condition(
        display_name=f"{display_name} condition",
        condition_threshold=monitoring_v3.AlertPolicy.Condition.MetricThreshold(
            filter=metric_filter,
            comparison=monitoring_v3.ComparisonType.COMPARISON_GT,
            threshold_value=float(threshold),
            duration=duration,
            aggregations=[
                monitoring_v3.Aggregation(
                    alignment_period=alignment,
                    per_series_aligner=per_series_aligner,
                )
            ],
        ),
    )

    policy = monitoring_v3.AlertPolicy(
        display_name=display_name,
        combiner=monitoring_v3.AlertPolicy.ConditionCombinerType.OR,
        conditions=[condition],
        notification_channels=channel_ids,
        enabled=True,
    )

    # Check if policy exists
    existing_policies = client.list_alert_policies(
        request={"name": project_name}
    )

    for existing in existing_policies:
        if existing.display_name == display_name:
            policy.name = existing.name
            client.update_alert_policy(alert_policy=policy)
            print(f"✔ Updated alert policy: {display_name}")
            return existing.name

    # Create if not found
    created = client.create_alert_policy(
        request={"name": project_name, "alert_policy": policy}
    )

    print(f"✔ Created alert policy: {display_name}")
    return created.name


# ---------------------------------------------------------
# Setup All Dataflow Alerts From YAML
# ---------------------------------------------------------
def setup_dataflow_alerts(cfg: dict):
    project_id = cfg["project"]["id"]
    alerts_cfg = cfg.get("observability", {}).get("alerts", {})

    if not alerts_cfg.get("enabled", False):
        print("⚠ Alerts are disabled in pipeline.yaml")
        return

    channel_ids = get_notification_channels(cfg)

    # -------------------------------------------------
    # 1️⃣ Watermark Lag
    # -------------------------------------------------
    wm_cfg = alerts_cfg.get("watermark_lag", {})
    if wm_cfg.get("enabled"):
        create_or_update_threshold_alert(
            project_id=project_id,
            display_name="Dataflow – Watermark Lag",
            metric_filter=(
                'resource.type="dataflow_job" AND '
                'metric.type="dataflow.googleapis.com/job/data_watermark_age"'
            ),
            threshold=wm_cfg["threshold"],
            duration_sec=wm_cfg["duration_sec"],
            alignment_sec=wm_cfg["alignment_sec"],
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            channel_ids=channel_ids,
        )

    # -------------------------------------------------
    # 2️⃣ Worker Saturation
    # -------------------------------------------------
    ws_cfg = alerts_cfg.get("worker_saturation", {})
    if ws_cfg.get("enabled"):
        create_or_update_threshold_alert(
            project_id=project_id,
            display_name="Dataflow – Worker Saturation",
            metric_filter=(
                'resource.type="dataflow_job" AND '
                'metric.type="dataflow.googleapis.com/job/aggregated_worker_utilization"'
            ),
            threshold=ws_cfg["threshold"],
            duration_sec=ws_cfg["duration_sec"],
            alignment_sec=ws_cfg["alignment_sec"],
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN,
            channel_ids=channel_ids,
        )

    # -------------------------------------------------
    # 3️⃣ Stage Error Rate
    # -------------------------------------------------
    se_cfg = alerts_cfg.get("stage_error_rate", {})
    if se_cfg.get("enabled"):
        create_or_update_threshold_alert(
            project_id=project_id,
            display_name="Dataflow – High Stage Error Rate",
            metric_filter=(
                'resource.type="dataflow_job" AND '
                'metric.type="dataflow.googleapis.com/job/user_counter" AND '
                '(metric.labels.metric_name="validation" OR '
                'metric.labels.metric_name="parse" OR '
                'metric.labels.metric_name="preprocess" OR '
                'metric.labels.metric_name="transform" OR '
                'metric.labels.metric_name="schema" OR '
                'metric.labels.metric_name="unknown")'
            ),
            threshold=se_cfg["threshold"],
            duration_sec=se_cfg["duration_sec"],
            alignment_sec=se_cfg["alignment_sec"],
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            channel_ids=channel_ids,
        )

    # -------------------------------------------------
    # 4️⃣ High DLQ Flow
    # -------------------------------------------------
    dlq_cfg = alerts_cfg.get("high_dlq_flow", {})
    if dlq_cfg.get("enabled"):
        create_or_update_threshold_alert(
            project_id=project_id,
            display_name="Dataflow – High DLQ Flow",
            metric_filter=(
                'resource.type="dataflow_job" AND '
                'metric.type="dataflow.googleapis.com/job/user_counter" AND '
                'metric.labels.metric_name="events_total"'
            ),
            threshold=dlq_cfg["threshold"],
            duration_sec=dlq_cfg["duration_sec"],
            alignment_sec=dlq_cfg["alignment_sec"],
            per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_SUM,
            channel_ids=channel_ids,
        )


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------
def main():
    cfg = load_config()
    setup_dataflow_alerts(cfg)
    print("✔ Monitoring setup completed successfully.")


if __name__ == "__main__":
    main()