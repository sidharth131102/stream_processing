import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
)
from apache_beam.options.pipeline_options import WorkerOptions
import logging
from pipeline.options import CustomOptions
from pipeline.config_loader import load_all_configs
from pipeline.graph_builder import build_pipeline



def run():
    pipeline_options = PipelineOptions(flags=None)


    custom = pipeline_options.view_as(CustomOptions)
    job_mode = custom.job_mode

    standard = pipeline_options.view_as(StandardOptions)

    if job_mode == "streaming":
        standard.runner = "DataflowRunner"
        standard.streaming = True
    elif job_mode == "backfill":
        standard.runner = "DataflowRunner"
        standard.streaming = False
    else:
        raise ValueError(f"Unsupported job_mode: {job_mode}")

    setup = pipeline_options.view_as(SetupOptions)
    setup.save_main_session = True

    # ----------------------------
    # Load configs (GCS only)
    # ----------------------------
    if not custom.config_bucket:
        raise ValueError("--config_bucket is required")

    cfg = load_all_configs(bucket=custom.config_bucket)
    job_cfg = cfg.get("dataflow", {}).get("job", {})
    experiments = job_cfg.get("experiments", [])
    if experiments:
        standard.experiments = experiments

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
    # APPLY DATAFLOW JOB OPTIONS (FIX)
    # ----------------------------
    
    if job_cfg.get("enable_streaming_engine"):
        logging.info("Streaming Engine ENABLED via pipeline.yaml")

    worker = pipeline_options.view_as(WorkerOptions)
    worker.num_workers = job_cfg.get("num_workers")
    worker.max_num_workers = job_cfg.get("max_workers")
    worker.machine_type = job_cfg.get("worker_machine_type")
    worker.disk_size_gb = job_cfg.get("disk_size_gb")
    worker.autoscaling_algorithm = job_cfg.get(
        "autoscaling_algorithm", "THROUGHPUT_BASED"
    )

    logging.info(f"Applied Dataflow job config: {job_cfg}")

    # ----------------------------
    # Build pipeline
    # ----------------------------
    with beam.Pipeline(options=pipeline_options) as p:
        build_pipeline(
            p,
            cfg,
            subscription=custom.subscription,
        )

if __name__ == "__main__":
    run()
