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





def run():
    pipeline_options = PipelineOptions()
    custom = pipeline_options.view_as(CustomOptions)

    cfg = {
        "job_mode": custom.job_mode
    }

    # ----------------------------
    # Backfill runtime injection
    # ----------------------------
    if custom.job_mode == "backfill":
        if not custom.backfill_start_ts or not custom.backfill_end_ts:
            raise ValueError("Backfill requires start and end timestamps")

        if not custom.path_pattern:
            raise ValueError("Backfill requires --path_pattern")

        cfg["backfill"] = {
            "start_ts": custom.backfill_start_ts,
            "end_ts": custom.backfill_end_ts,
            "path_pattern": custom.path_pattern,
        }

    # ----------------------------
    # Load static configs
    # ----------------------------
    loaded_cfg = load_all_configs(custom.config_bucket)
    cfg.update(loaded_cfg)

    logging.info(f"Final backfill config: {cfg.get('backfill')}")

    setup = pipeline_options.view_as(SetupOptions)
    setup.save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        build_pipeline(
            p,
            cfg,
            subscription=custom.subscription,
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()