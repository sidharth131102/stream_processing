from pipeline.io_connectorss.pubsub_reader import read_pubsub
from pipeline.io_connectorss.bq_backfill_reader import ReadFromRawBQ

def read_source(p, cfg, subscription=None):
    job_mode = cfg.get("job_mode")

    if job_mode == "streaming":
        if not subscription:
            raise ValueError("subscription is required for streaming")
        return read_pubsub(p, subscription)

    if job_mode == "backfill":
        backfill_cfg = cfg["backfill"]
        raw_cfg = cfg["raw_events"]

        return (
            p
            | "ReadFromRawEventsBQ"
            >> ReadFromRawBQ({
                "table": raw_cfg["table"],
                "start_ts": backfill_cfg["start_ts"],
                "end_ts": backfill_cfg["end_ts"],
                "project": cfg["project"]["id"],
                "location": cfg["project"]["region"],
            })
        )


    raise ValueError(f"Unsupported job_mode: {job_mode}")
