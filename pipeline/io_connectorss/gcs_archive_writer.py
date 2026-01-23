import apache_beam as beam
import json
from datetime import datetime, timezone
from apache_beam.io.fileio import WriteToFiles, default_file_naming


def _extract_date_partition(event: dict) -> str:
    """
    Partition by Pub/Sub publish time (RAW archive).
    """
    pubsub = event.get("_pubsub", {})
    publish_time = pubsub.get("publish_time")

    if not publish_time:
        return "unknown/unknown/unknown"

    dt = datetime.fromisoformat(
        publish_time.replace("Z", "+00:00")
    ).astimezone(timezone.utc)

    return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"



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
