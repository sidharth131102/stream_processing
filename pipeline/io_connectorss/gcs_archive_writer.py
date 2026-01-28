import apache_beam as beam
import json
from datetime import datetime, timezone
from apache_beam.io.fileio import WriteToFiles, FileNaming
from apache_beam.options.value_provider import StaticValueProvider


def _sanitize_for_json(event: dict) -> dict:
    event = dict(event)
    event.pop("beam_event_time", None)
    event.pop("beam_processing_time", None)
    return event


def _extract_date_partition(json_str: str) -> str:
    """
    Returns date-only partition: YYYY/MM/DD
    """
    try:
        event = json.loads(json_str)
        event_ts = event.get("event_ts")

        if not isinstance(event_ts, str):
            return "unknown/unknown/unknown"

        dt = datetime.fromisoformat(
            event_ts.replace("Z", "+00:00")
        ).astimezone(timezone.utc)

        return f"{dt.year:04d}/{dt.month:02d}/{dt.day:02d}"

    except Exception:
        return "invalid/invalid/invalid"


class DatePartitionedJsonNaming(FileNaming):
    def __call__(self, destination, shard_index, total_shards, window, pane, *args, **kwargs):
        shard = shard_index if shard_index is not None else 0
        return f"{destination}/part-{shard:05d}.json"


class WriteRawArchive(beam.PTransform):
    def __init__(self, archive_cfg: dict):
        # ✅ events lives in PATH, not destination
        self.base_path = StaticValueProvider(
            str,
            f"gs://{archive_cfg['bucket']}/{archive_cfg.get('path_prefix', 'events')}"
        )

        self.temp_dir = StaticValueProvider(
            str,
            f"gs://{archive_cfg['bucket']}/_tmp/archive"
        )

    def expand(self, pcoll):
        return (
            pcoll
            | "PrepareJson"
            >> beam.Map(lambda e: json.dumps(_sanitize_for_json(e)))
            | "WriteArchivePartitioned"
            >> WriteToFiles(
                path=self.base_path,
                destination=_extract_date_partition,
                file_naming=DatePartitionedJsonNaming(),
                sink=beam.io.fileio.TextSink(),
                shards=1,
                temp_directory=self.temp_dir,
            )
        )
