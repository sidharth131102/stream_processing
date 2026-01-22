from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
    WorkerOptions,
)
import logging
from pipeline.options import CustomOptions
from pipeline.config_loader import load_all_configs
from pipeline.graph_builder import build_pipeline




def normalize_backfill_config(cfg: dict) -> dict:
    backfill_yaml = cfg.get("backfill_yaml")
    if not backfill_yaml:
        raise ValueError("backfill.yaml is required for backfill mode")

    source_cfg = backfill_yaml["source"]

    start_ts = cfg["backfill"]["start_ts"]
    end_ts = cfg["backfill"]["end_ts"]

    start_dt = datetime.fromisoformat(start_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    end_dt = datetime.fromisoformat(end_ts.replace("Z", "+00:00")).astimezone(timezone.utc)

    if start_dt >= end_dt:
        raise ValueError("Invalid backfill window: start_ts >= end_ts")

    return {
        "path_pattern": source_cfg["path_pattern"],
        "start_ts": start_ts,
        "end_ts": end_ts,
        "event_time_field": source_cfg.get("event_time_field", "event_ts"),
        "timezone": source_cfg.get("timezone", "UTC"),
        "run_id": cfg["backfill"]["run_id"],
    }

def run():
    pipeline_options = PipelineOptions()

    custom = pipeline_options.view_as(CustomOptions)
    job_mode = custom.job_mode

    # ----------------------------
    # Inject job_mode EARLY
    # ----------------------------
    cfg = {"job_mode": job_mode}

    # ----------------------------
    # Validate backfill args EARLY
    # ----------------------------
    if job_mode == "backfill":
        if not custom.backfill_start_ts or not custom.backfill_end_ts:
            raise ValueError(
                "Backfill requires --backfill_start_ts and --backfill_end_ts"
            )

        cfg["backfill"] = {
            "start_ts": custom.backfill_start_ts,
            "end_ts": custom.backfill_end_ts,
        }

    # ----------------------------
    # Load configs (GCS only)
    # ----------------------------
    if not custom.config_bucket:
        raise ValueError("--config_bucket is required")

    loaded_cfg = load_all_configs(bucket=custom.config_bucket)

    # Merge loaded config INTO existing cfg
    cfg.update(loaded_cfg)
    if job_mode == "backfill":
        cfg["backfill"]["run_id"] = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        cfg["backfill"] = normalize_backfill_config(cfg)
    # ----------------------------
    # Add backfill run_id (STEP 7.1)
    # ----------------------------
    

    logging.info(f"Final backfill config: {cfg.get('backfill')}")

    # ----------------------------
    # Pipeline options
    # ---------------------------

    setup = pipeline_options.view_as(SetupOptions)
    setup.save_main_session = True



    # ----------------------------
    # Build pipeline
    # ----------------------------
    with beam.Pipeline(options=pipeline_options) as p:
        build_pipeline(
            p,
            cfg,
            subscription=custom.subscription,
        )
