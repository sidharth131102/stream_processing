import apache_beam as beam
import json
from datetime import datetime, timezone
from apache_beam.io.fileio import WriteToFiles, default_file_naming


def _extract_date_partition(event: dict) -> str:
    """
    Partition by event_ts (event time).
    Expected ISO-8601 string, e.g. 2026-01-14T06:31:10Z
    """
    event_ts = event.get("event_ts")

    if not event_ts:
        return "unknown/unknown/unknown/part"

    try:
        dt = datetime.fromisoformat(
            event_ts.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except Exception:
        return "invalid/invalid/invalid/part"

    # IMPORTANT: return a PATH, not just a key
    return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/part"




class WriteRawArchive(beam.PTransform):
    def __init__(self, archive_cfg: dict):
        self.bucket = archive_cfg["bucket"]
        self.prefix = archive_cfg.get("path_prefix", "events")

    def expand(self, pcoll):
        return (
            pcoll
            | "ArchiveToJson" >> beam.Map(json.dumps)
            | "WriteArchivePartitioned"
            >> WriteToFiles(
                path=f"gs://{self.bucket}/{self.prefix}",
                destination=lambda json_str: _extract_date_partition(
                    json.loads(json_str)
                ),
                file_naming=default_file_naming(".json"),
            )
        )