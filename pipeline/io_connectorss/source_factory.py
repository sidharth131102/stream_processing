from pipeline.io_connectorss.pubsub_reader import read_pubsub
from pipeline.io_connectorss.gcs_reader import ReadFromGCSArchive


def read_source(p, cfg, subscription=None):
    job_mode = cfg.get("job_mode")

    if job_mode == "streaming":
        if not subscription:
            raise ValueError("subscription is required for streaming mode")
        return read_pubsub(p, subscription)

    if job_mode == "backfill":
        backfill_cfg = cfg.get("backfill")
        if not backfill_cfg:
            raise ValueError("Normalized backfill config missing")

        path_pattern = backfill_cfg["path_pattern"]
        return (
            p
            | "ReadFromGCSArchive"
            >> ReadFromGCSArchive(path_pattern)
        )

    raise ValueError(f"Unsupported job_mode: {job_mode}")
