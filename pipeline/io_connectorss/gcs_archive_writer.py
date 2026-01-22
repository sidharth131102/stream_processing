import apache_beam as beam
import json
from datetime import datetime, timezone
from apache_beam.io.fileio import WriteToFiles


def _extract_date_partition(event: dict) -> str:
    """
    Returns partition path like: yyyy/MM/dd
    Based on event_ts (ISO-8601).
    """
    ts = event.get("event_ts")
    if not ts:
        # Fallback: put in unknown bucket (never crash pipeline)
        return "unknown/unknown/unknown"

    dt = datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"


class WriteRawArchive(beam.PTransform):
    """
    Writes raw events to GCS in time-partitioned folders:
    gs://bucket/events/yyyy/MM/dd/*.json
    """

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
                file_naming=WriteToFiles.default_file_naming(".json"),
            )
        )
