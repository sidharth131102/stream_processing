import apache_beam as beam
from apache_beam.utils.timestamp import Timestamp
from datetime import datetime, timezone
import json


class WriteRawEventsBQ(beam.PTransform):
    def __init__(self, cfg: dict):
        self.table = cfg["table"]  # project:dataset.raw_events

    def expand(self, pcoll):
        return (
            pcoll
            | "ToRawBQRow" >> beam.Map(self._to_row)
            | "WriteRawBQ" >> beam.io.WriteToBigQuery(
                table=self.table,
                schema={
                    "fields": [
                        {"name": "ingest_ts", "type": "TIMESTAMP"},
                        {"name": "event_ts", "type": "TIMESTAMP"},
                        {"name": "event_ts_raw", "type": "STRING"},
                        {"name": "event_id", "type": "STRING"},
                        {"name": "event_type", "type": "STRING"},
                        {"name": "event_source", "type": "STRING"},
                        {"name": "schema_name", "type": "STRING"},
                        {"name": "schema_version", "type": "STRING"},
                        {"name": "raw_payload", "type": "STRING"},
                        {"name": "pubsub_metadata", "type": "STRING"},
                    ]
                },

                # ✅ PARTITIONING (CRITICAL FOR BACKFILL COST)
                additional_bq_parameters={
                    "timePartitioning": {
                        "type": "DAY",
                        "field": "event_ts"
                    }
                },

                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
            )
        )

    @staticmethod
    def _to_row(event: dict):
        now_ts = Timestamp.from_utc_datetime(
            datetime.now(timezone.utc)
        )

        event_ts = event.get("event_ts")

        if isinstance(event_ts, datetime):
            event_ts_beam = Timestamp.from_utc_datetime(event_ts)
            event_ts_raw = event_ts.isoformat()

        elif isinstance(event_ts, str):
            event_ts_beam = Timestamp.from_rfc3339(event_ts)
            event_ts_raw = event_ts

        else:
            event_ts_beam = None
            event_ts_raw = None

        payload = event.get("payload")
        if isinstance(payload, str):
            raw_payload = payload
        else:
            raw_payload = json.dumps(payload)

        return {
            "ingest_ts": now_ts,
            "event_ts": event_ts_beam,
            "event_ts_raw": event_ts_raw,
            "event_id": event.get("event_id"),
            "event_type": event.get("event_type"),
            "event_source": event.get("event_source"),
            "schema_name": "json_event",
            "schema_version": "v2",
            "raw_payload": raw_payload,
            "pubsub_metadata": json.dumps(event.get("_pubsub")),
        }
